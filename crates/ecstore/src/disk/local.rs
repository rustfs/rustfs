// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::config::storageclass::DEFAULT_INLINE_BLOCK;
use crate::data_usage::local_snapshot::ensure_data_usage_layout;
use crate::disk::disk_store::get_object_disk_read_timeout;
use crate::disk::{
    BUCKET_META_PREFIX, CHECK_PART_FILE_CORRUPT, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS, CHECK_PART_UNKNOWN,
    CHECK_PART_VOLUME_NOT_FOUND, CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskLocation, DiskMetrics,
    FileInfoVersions, FileReader, FileWriter, MmapCopyStageMetrics, RUSTFS_META_BUCKET, RUSTFS_META_TMP_BUCKET,
    RUSTFS_META_TMP_DELETED_BUCKET, ReadMultipleReq, ReadMultipleResp, ReadOptions, RenameDataResp, STORAGE_FORMAT_FILE,
    STORAGE_FORMAT_FILE_BACKUP, UpdateMetadataOpts, VolumeInfo, WalkDirOptions, conv_part_err_to_int,
    endpoint::Endpoint,
    error::{DiskError, Error, FileAccessDeniedWithContext, Result},
    error_conv::{to_access_error, to_file_error, to_unformatted_disk_error, to_volume_error},
    format::FormatV3,
    fs::{
        O_APPEND, O_CREATE, O_RDONLY, O_TRUNC, O_WRONLY, access, access_std, lstat, lstat_std, remove, remove_all_std,
        remove_std, rename,
    },
    os,
    os::{check_path_length, is_empty_dir, is_root_disk, rename_all, rename_all_ignore_missing_source},
};
use crate::erasure::coding::bitrot_verify;
use crate::runtime::sources as runtime_sources;
use bytes::Bytes;
use metrics::counter;
use parking_lot::RwLock as ParkingLotRwLock;
use rustfs_filemeta::{
    Cache, FileInfo, FileInfoOpts, FileMeta, MetaCacheEntry, MetacacheWriter, ObjectPartInfo, Opts, RawFileInfo, UpdateFn,
    get_file_info, read_xl_meta_no_data,
};
use rustfs_utils::HashAlgorithm;
use rustfs_utils::os::get_info;
use rustfs_utils::path::{
    GLOBAL_DIR_SUFFIX, GLOBAL_DIR_SUFFIX_WITH_SLASH, SLASH_SEPARATOR, clean, decode_dir_object, encode_dir_object, has_suffix,
    path_join, path_join_buf,
};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::io::{Error as IoError, SeekFrom};
#[cfg(target_os = "linux")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use std::{
    fs::Metadata,
    path::{Path, PathBuf},
};
use time::OffsetDateTime;
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ErrorKind, ReadBuf};
use tokio::sync::{Notify, RwLock};
use tokio::time::{Instant, Sleep, interval_at, timeout};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const DELETED_OBJECTS_CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 5);
const STALE_TMP_OBJECT_EXPIRY: Duration = Duration::from_secs(24 * 60 * 60);
const RUSTFS_META_TMP_OLD_BUCKET: &str = ".rustfs.sys/tmp-old";
const STARTUP_CLEANUP_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const ENV_BITROT_SIZE_MISMATCH_RETRY_COUNT: &str = "RUSTFS_BITROT_SIZE_MISMATCH_RETRY_COUNT";
const ENV_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS: &str = "RUSTFS_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS";
const DEFAULT_BITROT_SIZE_MISMATCH_RETRY_COUNT: u64 = 2;
const DEFAULT_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS: u64 = 100;
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_DISK_LOCAL: &str = "disk_local";
const EVENT_DISK_LOCAL_STARTUP_CLEANUP: &str = "disk_local_startup_cleanup";
const EVENT_DISK_LOCAL_BACKGROUND_CLEANUP: &str = "disk_local_background_cleanup";
const EVENT_DISK_LOCAL_SCAN_FAILED: &str = "disk_local_scan_failed";
const EVENT_DISK_LOCAL_RENAME_REJECTED: &str = "disk_local_rename_rejected";
const EVENT_DISK_LOCAL_READ_VERSION_FALLBACK: &str = "disk_local_read_version_fallback";
#[cfg(target_os = "linux")]
const EVENT_DISK_LOCAL_DIRECT_IO_FALLBACK: &str = "disk_local_direct_io_fallback";
const EVENT_DISK_LOCAL_DELETE_FAILED: &str = "disk_local_delete_failed";
const EVENT_DISK_LOCAL_CHECK_PARTS: &str = "disk_local_check_parts";
const EVENT_DISK_LOCAL_ACCESS_FAILED: &str = "disk_local_access_failed";
const EVENT_DISK_LOCAL_VOLUME_SETUP_FAILED: &str = "disk_local_volume_setup_failed";
const EVENT_DISK_LOCAL_FORMAT_DECODE_FAILED: &str = "disk_local_format_decode_failed";
const METRIC_GET_OBJECT_MMAP_PAGE_FAULTS_TOTAL: &str = "rustfs_io_get_object_mmap_page_faults_total";
const METRIC_GET_OBJECT_DIRECT_READ_PAGE_FAULTS_TOTAL: &str = "rustfs_io_get_object_direct_read_page_faults_total";

#[inline(always)]
fn record_mmap_copy_stage(metrics: MmapCopyStageMetrics, stage: &'static str, started_at: Option<std::time::Instant>) {
    if let Some(started_at) = started_at {
        rustfs_io_metrics::record_get_object_stage_duration(metrics.path, stage, started_at.elapsed().as_secs_f64());
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MmapPageFaultCounts {
    minor: libc::c_long,
    major: libc::c_long,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MmapPageFaultDelta {
    minor: u64,
    major: u64,
}

#[cfg(all(unix, any(target_os = "linux", target_os = "android")))]
fn mmap_rusage_who() -> libc::c_int {
    libc::RUSAGE_THREAD
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
fn mmap_rusage_who() -> libc::c_int {
    libc::RUSAGE_SELF
}

#[cfg(unix)]
// SAFETY: this allowance is limited to reading kernel-provided rusage data via
// libc; each unsafe operation below documents pointer validity and initialization.
#[allow(unsafe_code)]
fn read_mmap_page_fault_counts(enabled: bool) -> Option<MmapPageFaultCounts> {
    if !enabled {
        return None;
    }

    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` writes to the provided `rusage` pointer when it
    // returns 0. The pointer is valid for writes and initialized only on success.
    let rc = unsafe { libc::getrusage(mmap_rusage_who(), usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }

    // SAFETY: `getrusage` returned success, so `usage` has been initialized.
    let usage = unsafe { usage.assume_init() };
    Some(MmapPageFaultCounts {
        minor: usage.ru_minflt,
        major: usage.ru_majflt,
    })
}

#[cfg(unix)]
fn non_negative_fault_delta(before: libc::c_long, after: libc::c_long) -> u64 {
    if after <= before {
        return 0;
    }

    u64::try_from(after - before).unwrap_or(u64::MAX)
}

#[cfg(unix)]
fn mmap_page_fault_delta(before: Option<MmapPageFaultCounts>, after: Option<MmapPageFaultCounts>) -> MmapPageFaultDelta {
    match (before, after) {
        (Some(before), Some(after)) => MmapPageFaultDelta {
            minor: non_negative_fault_delta(before.minor, after.minor),
            major: non_negative_fault_delta(before.major, after.major),
        },
        _ => MmapPageFaultDelta::default(),
    }
}

#[cfg(unix)]
fn record_mmap_page_fault_delta(path: &'static str, stage: &'static str, delta: MmapPageFaultDelta) {
    if delta.minor > 0 {
        counter!(
            METRIC_GET_OBJECT_MMAP_PAGE_FAULTS_TOTAL,
            "path" => path,
            "stage" => stage,
            "kind" => "minor",
        )
        .increment(delta.minor);
    }

    if delta.major > 0 {
        counter!(
            METRIC_GET_OBJECT_MMAP_PAGE_FAULTS_TOTAL,
            "path" => path,
            "stage" => stage,
            "kind" => "major",
        )
        .increment(delta.major);
    }
}

#[cfg(unix)]
fn record_direct_read_page_fault_delta(path: &'static str, stage: &'static str, delta: MmapPageFaultDelta) {
    if delta.minor > 0 {
        counter!(
            METRIC_GET_OBJECT_DIRECT_READ_PAGE_FAULTS_TOTAL,
            "path" => path,
            "stage" => stage,
            "kind" => "minor",
        )
        .increment(delta.minor);
    }

    if delta.major > 0 {
        counter!(
            METRIC_GET_OBJECT_DIRECT_READ_PAGE_FAULTS_TOTAL,
            "path" => path,
            "stage" => stage,
            "kind" => "major",
        )
        .increment(delta.major);
    }
}

/// Enable O_DIRECT for large sequential reads.
/// When enabled, shard reads bypass the page cache using O_DIRECT flag.
/// Requires aligned buffers (typically 512 bytes or 4096 bytes).
/// Default: false (uses page cache via mmap/pread).
const ENV_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE: &str = "RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE";
const DEFAULT_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE: bool = false;

/// Minimum shard size threshold for O_DIRECT reads.
/// Only shards larger than this threshold will use O_DIRECT.
/// Default: 4MB.
const ENV_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD: &str = "RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD";
const DEFAULT_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD: usize = 4 * 1024 * 1024;
const ENV_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE: &str = "RUSTFS_OBJECT_MMAP_POPULATE_ENABLE";
const DEFAULT_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE: bool = false;
const ENV_RUSTFS_OBJECT_MMAP_READ_METHOD: &str = "RUSTFS_OBJECT_MMAP_READ_METHOD";
const RUSTFS_OBJECT_MMAP_READ_METHOD_MMAP_COPY: &str = "mmap_copy";
const RUSTFS_OBJECT_MMAP_READ_METHOD_DIRECT_READ_COPY: &str = "direct_read_copy";

/// Fsync writes and commit-point renames so acknowledged data survives power loss.
/// Disabling trades durability for latency: data acknowledged with 200 OK may only
/// live in the page cache and be lost on a whole-node power failure.
/// Default: true.
const ENV_RUSTFS_DRIVE_SYNC_ENABLE: &str = "RUSTFS_DRIVE_SYNC_ENABLE";
const DEFAULT_RUSTFS_DRIVE_SYNC_ENABLE: bool = true;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalReadCopyMethod {
    MmapCopy,
    DirectReadCopy,
}

/// Check if O_DIRECT reads are enabled.
fn is_direct_io_read_enabled() -> bool {
    rustfs_utils::get_env_bool(ENV_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE, DEFAULT_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE)
}

/// Check if durable (fsync) writes are enabled.
pub(crate) fn drive_sync_enabled() -> bool {
    rustfs_utils::get_env_bool(ENV_RUSTFS_DRIVE_SYNC_ENABLE, DEFAULT_RUSTFS_DRIVE_SYNC_ENABLE)
}

/// Get the O_DIRECT read threshold size.
fn get_direct_io_read_threshold() -> usize {
    rustfs_utils::get_env_usize(ENV_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD, DEFAULT_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD)
}

fn should_populate_mmap_read(length: usize) -> bool {
    length > 0 && rustfs_utils::get_env_bool(ENV_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE, DEFAULT_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE)
}

fn local_read_copy_method() -> LocalReadCopyMethod {
    let method = rustfs_utils::get_env_str(ENV_RUSTFS_OBJECT_MMAP_READ_METHOD, RUSTFS_OBJECT_MMAP_READ_METHOD_MMAP_COPY);
    match method.as_str() {
        RUSTFS_OBJECT_MMAP_READ_METHOD_DIRECT_READ_COPY => LocalReadCopyMethod::DirectReadCopy,
        _ => LocalReadCopyMethod::MmapCopy,
    }
}

/// Runtime state for the true O_DIRECT read path (Linux only).
///
/// `supported` starts true and latches false on the first EINVAL/EOPNOTSUPP
/// from an O_DIRECT open/read (tmpfs, overlayfs, and 9p commonly reject the
/// flag); the path then permanently falls back to the buffered read methods
/// for this disk. `align` caches the DIO alignment probed from the backing
/// filesystem. O_DIRECT errors must never surface to callers: EINVAL maps to
/// `FileNotFound` in `to_file_error`, which would masquerade as a missing
/// shard and trigger spurious EC rebuilds.
#[cfg(target_os = "linux")]
#[derive(Debug)]
struct DirectIoReadState {
    supported: AtomicBool,
    align: OnceLock<usize>,
    fallback_logged: AtomicBool,
}

#[cfg(target_os = "linux")]
impl DirectIoReadState {
    fn new() -> Self {
        Self {
            supported: AtomicBool::new(true),
            align: OnceLock::new(),
            fallback_logged: AtomicBool::new(false),
        }
    }
}

#[cfg(target_os = "linux")]
const DEFAULT_DIRECT_IO_ALIGN: usize = 4096;

/// Probe the DIO alignment requirement for the file's filesystem via
/// statx STATX_DIOALIGN (kernel >= 6.1). Falls back to 4096, a safe upper
/// bound for 512e/4Kn devices, when the kernel or filesystem does not
/// report it.
#[cfg(target_os = "linux")]
fn probe_direct_io_align(file: &std::fs::File) -> usize {
    use rustix::fs::{AtFlags, StatxFlags};

    match rustix::fs::statx(file, "", AtFlags::EMPTY_PATH, StatxFlags::DIOALIGN) {
        Ok(stx) => {
            if StatxFlags::from_bits_retain(stx.stx_mask).contains(StatxFlags::DIOALIGN) {
                let align = stx.stx_dio_mem_align.max(stx.stx_dio_offset_align) as usize;
                if align.is_power_of_two() && align >= 512 {
                    return align;
                }
            }
            DEFAULT_DIRECT_IO_ALIGN
        }
        Err(_) => DEFAULT_DIRECT_IO_ALIGN,
    }
}

/// Heap buffer with explicit alignment for O_DIRECT reads.
#[cfg(target_os = "linux")]
struct AlignedBuf {
    ptr: std::ptr::NonNull<u8>,
    len: usize,
    layout: std::alloc::Layout,
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
impl AlignedBuf {
    fn new(len: usize, align: usize) -> std::io::Result<Self> {
        debug_assert!(len > 0, "AlignedBuf must not be zero-sized");
        let layout = std::alloc::Layout::from_size_align(len, align)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        // SAFETY: `layout` has non-zero size (callers guarantee len > 0) and a
        // valid power-of-two alignment enforced by Layout::from_size_align.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = std::ptr::NonNull::new(ptr).ok_or(std::io::ErrorKind::OutOfMemory)?;
        Ok(Self { ptr, len, layout })
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` is a live allocation of exactly `len` bytes owned by
        // self, initialized to zero at allocation and only written via
        // `as_mut_slice`.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: as in `as_slice`, plus `&mut self` guarantees exclusivity.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`layout` come from the successful alloc_zeroed in new().
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) }
    }
}

#[cfg(target_os = "linux")]
fn is_direct_io_unsupported(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc::EINVAL) | Some(libc::EOPNOTSUPP))
}

/// True O_DIRECT positioned read: open with O_DIRECT, read the aligned
/// superset range into an aligned bounce buffer, then slice out the exact
/// logical range. Alignment padding never leaks to callers — BitrotReader
/// reads exact shard_size and would flag padded output as corruption.
///
/// Short reads are legal for O_DIRECT; the loop stops at EOF (res == 0).
/// A read that ends before covering the logical range is an error (the
/// caller has already validated `offset + length <= file size`, so this
/// only happens on concurrent truncation) and makes the caller fall back
/// to the buffered path.
#[cfg(target_os = "linux")]
fn pread_direct_aligned(file_path: &Path, offset: u64, length: usize, state: &DirectIoReadState) -> std::io::Result<Bytes> {
    use std::os::unix::fs::{FileExt, OpenOptionsExt};

    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::DIRECT.bits() as i32)
        .open(file_path)?;

    let align = *state.align.get_or_init(|| probe_direct_io_align(&file));
    let align_u64 = align as u64;

    let aligned_offset = offset - (offset % align_u64);
    let logical_start =
        usize::try_from(offset - aligned_offset).map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let logical_end = logical_start.checked_add(length).ok_or(std::io::ErrorKind::InvalidInput)?;
    let aligned_len = logical_end.checked_add(align - 1).ok_or(std::io::ErrorKind::InvalidInput)? / align * align;

    let mut buf = AlignedBuf::new(aligned_len, align)?;

    let mut filled = 0usize;
    while filled < aligned_len {
        // `filled` stays a multiple of `align` except possibly at EOF, so
        // both the buffer address and the file offset remain aligned.
        let n = file.read_at(&mut buf.as_mut_slice()[filled..], aligned_offset + filled as u64)?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    if filled < logical_end {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short O_DIRECT read"));
    }

    Ok(Bytes::copy_from_slice(&buf.as_slice()[logical_start..logical_end]))
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn mmap_page_size() -> Result<u64> {
    static PAGE_SIZE: OnceLock<Option<u64>> = OnceLock::new();

    PAGE_SIZE
        .get_or_init(|| {
            // SAFETY: `sysconf(_SC_PAGESIZE)` has no pointer arguments and only
            // queries process-global OS configuration.
            let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            if page_size <= 0 {
                return None;
            }
            u64::try_from(page_size).ok()
        })
        .ok_or_else(|| DiskError::other("failed to determine system page size"))
}

#[cfg(test)]
static RENAME_DATA_FAIL_BEFORE_OLD_METADATA_BACKUP: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

#[cfg(test)]
fn set_rename_data_fail_before_old_metadata_backup(dst_path: &str) {
    *RENAME_DATA_FAIL_BEFORE_OLD_METADATA_BACKUP
        .lock()
        .expect("test failpoint lock should not be poisoned") = Some(dst_path.to_string());
}

#[cfg(test)]
fn should_fail_before_old_metadata_backup(dst_path: &str) -> bool {
    let mut target = RENAME_DATA_FAIL_BEFORE_OLD_METADATA_BACKUP
        .lock()
        .expect("test failpoint lock should not be poisoned");
    if target.as_deref() == Some(dst_path) {
        target.take();
        true
    } else {
        false
    }
}

#[cfg(not(test))]
fn should_fail_before_old_metadata_backup(_dst_path: &str) -> bool {
    false
}

fn log_startup_disk_io_error(stage: &str, path: &Path, err: &IoError) {
    warn!(
        event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
        stage,
        path = %path.display(),
        error_kind = ?err.kind(),
        raw_os_error = ?err.raw_os_error(),
        error = ?err,
        state = "io_failed",
        "Disk local startup filesystem operation failed"
    );
}

fn log_startup_disk_error(stage: &str, path: &Path, err: &DiskError) {
    if let DiskError::Io(io_err) = err {
        log_startup_disk_io_error(stage, path, io_err);
        return;
    }

    warn!(
        event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
        stage,
        path = %path.display(),
        error = ?err,
        state = "failed",
        "Disk local startup operation failed"
    );
}

#[derive(Debug, Clone)]
pub struct FormatInfo {
    pub id: Option<Uuid>,
    pub data: Bytes,
    pub file_info: Option<Metadata>,
    pub last_check: Option<OffsetDateTime>,
}

/// A helper enum to handle internal buffer types for writing data.
pub enum InternalBuf<'a> {
    Ref(&'a [u8]),
    Owned(Bytes),
}

struct FileCacheReclaimWriter {
    inner: File,
    reclaim_len: usize,
    reclaim_on_shutdown: bool,
    reclaimed: bool,
}

struct FileCacheReclaimReader {
    inner: File,
    reclaim_offset: u64,
    reclaim_len: usize,
    reclaim_on_drop: bool,
    reclaimed: bool,
}

struct StallTimeoutReader<R> {
    inner: R,
    timeout: Duration,
    timer: Option<std::pin::Pin<Box<Sleep>>>,
}

impl<R> StallTimeoutReader<R> {
    fn new(inner: R, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            timer: None,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for StallTimeoutReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let filled_before = buf.filled().len();
        match std::pin::Pin::new(&mut self.inner).poll_read(cx, buf) {
            std::task::Poll::Ready(result) => {
                self.timer = None;
                std::task::Poll::Ready(result)
            }
            std::task::Poll::Pending => {
                if self.timeout.is_zero() {
                    return std::task::Poll::Pending;
                }

                if self.timer.is_none() {
                    self.timer = Some(Box::pin(tokio::time::sleep(self.timeout)));
                }

                if let Some(timer) = self.timer.as_mut()
                    && std::future::Future::poll(timer.as_mut(), cx).is_ready()
                {
                    self.timer = None;
                    return std::task::Poll::Ready(Err(std::io::Error::new(
                        ErrorKind::TimedOut,
                        "local disk read stall timeout",
                    )));
                }

                if buf.filled().len() > filled_before {
                    self.timer = None;
                }

                std::task::Poll::Pending
            }
        }
    }
}

fn record_file_cache_reclaim_success(kind: &'static str, reclaim_len: usize, started: std::time::Instant) {
    counter!("rustfs_page_cache_reclaim_requests_total", "kind" => kind.to_string(), "result" => "ok".to_string()).increment(1);
    counter!("rustfs_page_cache_reclaim_bytes_total", "kind" => kind.to_string()).increment(reclaim_len as u64);
    metrics::histogram!("rustfs_page_cache_reclaim_duration_seconds", "kind" => kind.to_string())
        .record(started.elapsed().as_secs_f64());
}

fn record_file_cache_reclaim_error(kind: &'static str) {
    counter!("rustfs_page_cache_reclaim_requests_total", "kind" => kind.to_string(), "result" => "err".to_string()).increment(1);
}

fn bitrot_size_mismatch_retry_count() -> usize {
    rustfs_utils::get_env_u64(ENV_BITROT_SIZE_MISMATCH_RETRY_COUNT, DEFAULT_BITROT_SIZE_MISMATCH_RETRY_COUNT) as usize
}

fn bitrot_size_mismatch_retry_delay() -> Duration {
    Duration::from_millis(rustfs_utils::get_env_u64(
        ENV_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS,
        DEFAULT_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS,
    ))
}

fn is_bitrot_size_mismatch_error(err: &std::io::Error) -> bool {
    err.to_string().contains("bitrot shard file size mismatch")
}

impl FileCacheReclaimReader {
    fn new(inner: File, reclaim_offset: u64, reclaim_len: usize, reclaim_on_drop: bool) -> Self {
        #[cfg(target_os = "macos")]
        if reclaim_on_drop {
            let _ = set_fd_nocache(&inner);
        }

        Self {
            inner,
            reclaim_offset,
            reclaim_len,
            reclaim_on_drop,
            reclaimed: false,
        }
    }

    #[cfg(target_os = "linux")]
    fn reclaim_file_cache(&mut self) -> std::io::Result<()> {
        use core::num::NonZeroU64;
        use rustix::fs::{Advice, fadvise};

        if !self.reclaim_on_drop || self.reclaimed || self.reclaim_len == 0 {
            return Ok(());
        }

        let started = std::time::Instant::now();
        let reclaim_len =
            NonZeroU64::new(self.reclaim_len as u64).expect("reclaim_len is guaranteed non-zero by the early return");
        fadvise(&self.inner, self.reclaim_offset, Some(reclaim_len), Advice::DontNeed).map_err(std::io::Error::from)?;

        self.reclaimed = true;
        record_file_cache_reclaim_success("read", self.reclaim_len, started);
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn reclaim_file_cache(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn set_fd_nocache(file: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `fcntl` is called on a valid file descriptor owned by `file`.
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    if ret == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn set_std_fd_nocache(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `fcntl` is called on a valid file descriptor owned by `file`.
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    if ret == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

impl Drop for FileCacheReclaimReader {
    fn drop(&mut self) {
        if let Err(err) = self.reclaim_file_cache() {
            record_file_cache_reclaim_error("read");
            debug!(error = ?err, reclaim_offset = self.reclaim_offset, reclaim_len = self.reclaim_len, "failed to reclaim file cache after read");
        }
    }
}

impl AsyncRead for FileCacheReclaimReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl FileCacheReclaimWriter {
    fn new(inner: File, reclaim_len: usize, reclaim_on_shutdown: bool) -> Self {
        #[cfg(target_os = "macos")]
        if reclaim_on_shutdown {
            let _ = set_fd_nocache(&inner);
        }

        Self {
            inner,
            reclaim_len,
            reclaim_on_shutdown,
            reclaimed: false,
        }
    }

    #[cfg(target_os = "linux")]
    fn reclaim_file_cache(&mut self) -> std::io::Result<()> {
        use core::num::NonZeroU64;
        use rustix::fs::{Advice, fadvise};

        if !self.reclaim_on_shutdown || self.reclaimed || self.reclaim_len == 0 {
            return Ok(());
        }

        let started = std::time::Instant::now();
        let reclaim_len =
            NonZeroU64::new(self.reclaim_len as u64).expect("reclaim_len is guaranteed non-zero by the early return");
        fadvise(&self.inner, 0, Some(reclaim_len), Advice::DontNeed).map_err(std::io::Error::from)?;

        self.reclaimed = true;
        record_file_cache_reclaim_success("write", self.reclaim_len, started);
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn reclaim_file_cache(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl AsyncWrite for FileCacheReclaimWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match std::pin::Pin::new(&mut self.inner).poll_shutdown(cx) {
            std::task::Poll::Ready(Ok(())) => {
                if let Err(err) = self.reclaim_file_cache() {
                    record_file_cache_reclaim_error("write");
                    debug!(error = ?err, reclaim_len = self.reclaim_len, "failed to reclaim file cache after write");
                }
                std::task::Poll::Ready(Ok(()))
            }
            other => other,
        }
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

fn should_reclaim_file_cache_after_write(file_size: i64) -> bool {
    if file_size <= 0 {
        return false;
    }

    if !rustfs_utils::get_env_bool(
        rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE,
        rustfs_config::DEFAULT_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE,
    ) {
        return false;
    }

    let threshold = rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD,
        rustfs_config::DEFAULT_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD,
    );
    file_size as usize >= threshold
}

fn should_reclaim_file_cache_after_read(length: usize) -> bool {
    if length == 0 {
        return false;
    }

    if !rustfs_utils::get_env_bool(
        rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE,
        rustfs_config::DEFAULT_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE,
    ) {
        return false;
    }

    let threshold = rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD,
        rustfs_config::DEFAULT_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD,
    );
    length >= threshold
}

/// Write-open semantics for [`LocalIoBackend::open_write`].
///
/// `Truncate` mirrors `DiskAPI::create_file` (O_CREATE|O_WRONLY|O_TRUNC, no
/// volume access check, cache-reclaim writer); `Append` mirrors
/// `DiskAPI::append_file` (O_CREATE|O_APPEND|O_WRONLY, volume access check).
/// The access-check asymmetry is preserved historical behavior.
#[derive(Clone, Copy, Debug)]
pub(crate) enum WriteMode {
    Truncate { size_hint: i64 },
    Append,
}

/// Local-disk file I/O backend behind [`LocalDisk`].
///
/// Models the real per-file operations of the `DiskAPI` hot path so an
/// alternative backend (e.g. a runtime-probed io_uring implementation) can be
/// swapped in without touching callers. The default [`StdBackend`] preserves
/// the pre-trait behavior byte-for-byte. Commit-point durability
/// (fdatasync -> rename -> fsync-dir in `rename_data`) is deliberately NOT
/// part of this trait.
#[async_trait::async_trait]
pub(crate) trait LocalIoBackend: Send + Sync + Debug + 'static {
    /// Positioned whole-range read returning owned bytes
    /// (mirrors `read_file_mmap_copy_with_metrics`).
    async fn pread_bytes(
        &self,
        volume: &str,
        path: &str,
        offset: usize,
        length: usize,
        metrics: Option<MmapCopyStageMetrics>,
    ) -> Result<Bytes>;

    /// Open a bounded streaming reader over `offset..offset+length`
    /// (mirrors `read_file_stream`).
    async fn open_read_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader>;

    /// Open a whole-file streaming reader (mirrors `read_file`).
    async fn open_full_read(&self, volume: &str, path: &str) -> Result<FileReader>;

    /// Open a writer (mirrors `create_file`/`append_file` per [`WriteMode`]).
    async fn open_write(&self, volume: &str, path: &str, mode: WriteMode) -> Result<FileWriter>;
}

/// Default [`LocalIoBackend`]: tokio blocking-pool file I/O plus the
/// mmap-copy / direct-read-copy positioned read, moved verbatim from the
/// former `DiskAPI` method bodies on `LocalDisk`.
#[derive(Debug)]
pub(crate) struct StdBackend {
    root: PathBuf,
    #[cfg(target_os = "linux")]
    direct_io: Arc<DirectIoReadState>,
}

impl StdBackend {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            #[cfg(target_os = "linux")]
            direct_io: Arc::new(DirectIoReadState::new()),
        }
    }

    async fn open_file(&self, path: impl AsRef<Path>, mode: usize, skip_parent: impl AsRef<Path>) -> Result<File> {
        let mut skip_parent = skip_parent.as_ref();
        if skip_parent.as_os_str().is_empty() {
            skip_parent = self.root.as_path();
        }

        if let Some(parent) = path.as_ref().parent()
            && parent != skip_parent
        {
            os::make_dir_all(parent, skip_parent).await?;
        }

        let f = super::fs::open_file(path.as_ref(), mode).await.map_err(to_file_error)?;

        Ok(f)
    }

    async fn open_file_read_only(&self, path: impl AsRef<Path>) -> Result<File> {
        let f = super::fs::open_file(path.as_ref(), O_RDONLY).await.map_err(to_file_error)?;
        Ok(f)
    }
}

#[async_trait::async_trait]
impl LocalIoBackend for StdBackend {
    /// File read using mmap-then-copy on Unix or efficient read on non-Unix.
    // SAFETY: Unix unsafe calls in this function only query page size and mmap
    // a read-only file region after bounds and alignment are validated.
    #[allow(unsafe_code)]
    async fn pread_bytes(
        &self,
        volume: &str,
        path: &str,
        offset: usize,
        length: usize,
        metrics: Option<MmapCopyStageMetrics>,
    ) -> Result<Bytes> {
        let metrics = metrics.filter(|_| rustfs_io_metrics::get_stage_metrics_enabled());
        let metrics_enabled = metrics.is_some();

        let metadata_validate_start = metrics_enabled.then(std::time::Instant::now);
        let Some(end_offset) = offset.checked_add(length) else {
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.metadata_validate_stage, metadata_validate_start);
            }
            return Err(DiskError::FileCorrupt);
        };

        // Unix: use mmap to read the data (copies into Bytes for safe ownership)
        // Non-Unix: fall back to efficient read
        #[cfg(unix)]
        {
            use memmap2::MmapOptions;
            use std::time::{Duration as StdDuration, Instant as StdInstant};

            struct MmapCopyReadResult {
                bytes: Bytes,
                access_check_duration: StdDuration,
                path_resolve_duration: StdDuration,
                metadata_lookup_duration: StdDuration,
                metadata_validate_duration: StdDuration,
                file_open_duration: StdDuration,
                mmap_map_duration: StdDuration,
                mmap_copy_duration: StdDuration,
                direct_read_copy_duration: StdDuration,
                mmap_map_fault_delta: MmapPageFaultDelta,
                mmap_copy_fault_delta: MmapPageFaultDelta,
                direct_read_copy_fault_delta: MmapPageFaultDelta,
                blocking_task_duration: StdDuration,
                used_direct_io: bool,
            }

            enum MmapCopyReadError {
                Disk(DiskError),
                OutOfBounds { actual_size: u64 },
            }

            impl From<DiskError> for MmapCopyReadError {
                fn from(err: DiskError) -> Self {
                    Self::Disk(err)
                }
            }

            let start = StdInstant::now();
            let root = self.root.clone();
            let volume_owned = volume.to_owned();
            let path_owned = path.to_owned();

            let should_reclaim_after_read = should_reclaim_file_cache_after_read(length);
            let should_populate_mmap_read = should_populate_mmap_read(length);
            let read_copy_method = local_read_copy_method();
            #[cfg(target_os = "linux")]
            let direct_io_eligible = is_direct_io_read_enabled() && length > 0 && length >= get_direct_io_read_threshold();
            #[cfg(target_os = "linux")]
            let direct_io_state = self.direct_io.clone();
            let offset_u64 = u64::try_from(offset).map_err(|_| DiskError::FileCorrupt)?;
            let end_offset_u64 = u64::try_from(end_offset).map_err(|_| DiskError::FileCorrupt)?;
            let blocking_wait_start = metrics_enabled.then(std::time::Instant::now);
            let read_result = tokio::task::spawn_blocking(move || {
                let blocking_task_start = metrics_enabled.then(StdInstant::now);

                let access_check_start = metrics_enabled.then(StdInstant::now);
                let volume_dir = local_disk_bucket_path(&root, &volume_owned)?;
                if !skip_access_checks(&volume_owned) {
                    access_std(&volume_dir).map_err(|e| DiskError::from(to_access_error(e, DiskError::VolumeAccessDenied)))?;
                }
                let access_check_duration = access_check_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());

                let path_resolve_start = metrics_enabled.then(StdInstant::now);
                let file_path = local_disk_object_path(&root, &volume_owned, &path_owned)?;
                check_path_length(file_path.to_string_lossy().as_ref())?;
                let path_resolve_duration = path_resolve_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());

                let file_open_start = metrics_enabled.then(StdInstant::now);
                let mut file = std::fs::File::open(&file_path).map_err(DiskError::from)?;
                let file_open_duration = file_open_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());

                let metadata_lookup_start = metrics_enabled.then(StdInstant::now);
                let meta = file.metadata().map_err(DiskError::from)?;
                let metadata_lookup_duration = metadata_lookup_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());

                let metadata_validate_start = metrics_enabled.then(StdInstant::now);
                if meta.len() < end_offset_u64 {
                    return Err(MmapCopyReadError::OutOfBounds { actual_size: meta.len() });
                }
                let metadata_validate_duration =
                    metadata_validate_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());

                #[cfg(target_os = "macos")]
                if should_reclaim_after_read {
                    let _ = set_std_fd_nocache(&file);
                }

                let mut mmap_map_duration = StdDuration::ZERO;
                let mut mmap_copy_duration = StdDuration::ZERO;
                let mut direct_read_copy_duration = StdDuration::ZERO;
                let mut mmap_map_fault_delta = MmapPageFaultDelta::default();
                let mut mmap_copy_fault_delta = MmapPageFaultDelta::default();
                let mut direct_read_copy_fault_delta = MmapPageFaultDelta::default();
                let mut _reclaim_offset = offset_u64;
                let mut _reclaim_len = length;

                #[cfg(target_os = "linux")]
                let mut direct_io_bytes: Option<Bytes> = None;
                #[cfg(not(target_os = "linux"))]
                let direct_io_bytes: Option<Bytes> = None;
                #[cfg(target_os = "linux")]
                if direct_io_eligible && direct_io_state.supported.load(Ordering::Relaxed) {
                    let direct_start = metrics_enabled.then(StdInstant::now);
                    let direct_faults_before = read_mmap_page_fault_counts(metrics_enabled);
                    match pread_direct_aligned(&file_path, offset_u64, length, &direct_io_state) {
                        Ok(bytes) => {
                            let direct_faults_after = read_mmap_page_fault_counts(metrics_enabled);
                            direct_read_copy_duration = direct_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());
                            direct_read_copy_fault_delta = mmap_page_fault_delta(direct_faults_before, direct_faults_after);
                            direct_io_bytes = Some(bytes);
                        }
                        Err(err) => {
                            // Never surface O_DIRECT errors: EINVAL maps to
                            // FileNotFound in to_file_error and would trigger a
                            // spurious EC rebuild. Latch off on unsupported
                            // filesystems; otherwise retry buffered this once.
                            if is_direct_io_unsupported(&err) {
                                direct_io_state.supported.store(false, Ordering::Relaxed);
                            }
                            if !direct_io_state.fallback_logged.swap(true, Ordering::Relaxed) {
                                warn!(
                                    event = EVENT_DISK_LOCAL_DIRECT_IO_FALLBACK,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                                    path = %file_path.display(),
                                    error = ?err,
                                    "O_DIRECT read unavailable; falling back to buffered reads"
                                );
                            }
                        }
                    }
                }

                let used_direct_io = direct_io_bytes.is_some();
                let bytes = if let Some(bytes) = direct_io_bytes {
                    bytes
                } else {
                    match read_copy_method {
                        LocalReadCopyMethod::MmapCopy => {
                            // mmap offsets on Unix must be page-size aligned. Align the
                            // mapping down to the nearest page boundary, then slice out the
                            // originally requested logical range.
                            let page_size = mmap_page_size()?;
                            let aligned_offset = offset_u64 - (offset_u64 % page_size);
                            let logical_offset = usize::try_from(offset_u64 - aligned_offset)
                                .map_err(|_| DiskError::other("mmap offset overflow"))?;
                            let map_len = logical_offset
                                .checked_add(length)
                                .ok_or_else(|| DiskError::other("mmap length overflow"))?;
                            _reclaim_offset = aligned_offset;
                            _reclaim_len = map_len;

                            // SAFETY: The file is opened as read-only, and we're mapping a region
                            // that we've already verified exists and is within file bounds. The
                            // file offset passed to mmap is page-size aligned as required on Unix.
                            let mmap_map_start = metrics_enabled.then(StdInstant::now);
                            let mmap_map_faults_before = read_mmap_page_fault_counts(metrics_enabled);
                            let mut mmap_options = MmapOptions::new();
                            mmap_options.offset(aligned_offset).len(map_len);
                            if should_populate_mmap_read {
                                mmap_options.populate();
                            }
                            let mmap = unsafe { mmap_options.map(&file) }.map_err(DiskError::other)?;
                            let mmap_map_faults_after = read_mmap_page_fault_counts(metrics_enabled);
                            mmap_map_duration = mmap_map_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());
                            mmap_map_fault_delta = mmap_page_fault_delta(mmap_map_faults_before, mmap_map_faults_after);

                            // Copy only the requested logical range into a Bytes buffer. This
                            // avoids undefined behavior from treating OS-managed mmap memory as
                            // allocator-managed Vec storage, at the cost of an extra copy.
                            let end = logical_offset
                                .checked_add(length)
                                .ok_or_else(|| DiskError::other("mmap slice length overflow"))?;
                            let mmap_copy_start = metrics_enabled.then(StdInstant::now);
                            let mmap_copy_faults_before = read_mmap_page_fault_counts(metrics_enabled);
                            let bytes = Bytes::copy_from_slice(&mmap[logical_offset..end]);
                            let mmap_copy_faults_after = read_mmap_page_fault_counts(metrics_enabled);
                            mmap_copy_duration = mmap_copy_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());
                            mmap_copy_fault_delta = mmap_page_fault_delta(mmap_copy_faults_before, mmap_copy_faults_after);
                            bytes
                        }
                        LocalReadCopyMethod::DirectReadCopy => {
                            use std::io::{Read as _, Seek as _};

                            let direct_read_copy_start = metrics_enabled.then(StdInstant::now);
                            let direct_read_copy_faults_before = read_mmap_page_fault_counts(metrics_enabled);
                            file.seek(SeekFrom::Start(offset_u64)).map_err(DiskError::from)?;
                            let mut buffer = vec![0; length];
                            file.read_exact(&mut buffer).map_err(DiskError::from)?;
                            let direct_read_copy_faults_after = read_mmap_page_fault_counts(metrics_enabled);
                            direct_read_copy_duration =
                                direct_read_copy_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());
                            direct_read_copy_fault_delta =
                                mmap_page_fault_delta(direct_read_copy_faults_before, direct_read_copy_faults_after);
                            Bytes::from(buffer)
                        }
                    }
                };

                #[cfg(target_os = "linux")]
                if should_reclaim_after_read && _reclaim_len > 0 {
                    use core::num::NonZeroU64;
                    use rustix::fs::{Advice, fadvise};

                    let reclaim_len = NonZeroU64::new(
                        u64::try_from(_reclaim_len).map_err(|_| DiskError::other("read reclaim length overflow"))?,
                    )
                    .ok_or_else(|| DiskError::other("read reclaim length overflow"))?;
                    fadvise(&file, _reclaim_offset, Some(reclaim_len), Advice::DontNeed)
                        .map_err(std::io::Error::from)
                        .map_err(DiskError::from)?;
                }

                let blocking_task_duration = blocking_task_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());

                Ok::<MmapCopyReadResult, MmapCopyReadError>(MmapCopyReadResult {
                    bytes,
                    access_check_duration,
                    path_resolve_duration,
                    metadata_lookup_duration,
                    metadata_validate_duration,
                    file_open_duration,
                    mmap_map_duration,
                    mmap_copy_duration,
                    direct_read_copy_duration,
                    mmap_map_fault_delta,
                    mmap_copy_fault_delta,
                    direct_read_copy_fault_delta,
                    blocking_task_duration,
                    used_direct_io,
                })
            })
            .await
            .map_err(DiskError::from)
            .map_err(MmapCopyReadError::Disk)
            .and_then(|result| result);
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.blocking_wait_stage, blocking_wait_start);
            }
            let read_result = match read_result {
                Ok(read_result) => read_result,
                Err(MmapCopyReadError::Disk(err)) => return Err(err),
                Err(MmapCopyReadError::OutOfBounds { actual_size }) => {
                    error!(
                        event = EVENT_DISK_LOCAL_READ_VERSION_FALLBACK,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        volume,
                        path,
                        offset,
                        length,
                        actual_size,
                        reason = "read_file_mmap_copy_out_of_bounds",
                        "Disk local read fallback failed"
                    );
                    return Err(DiskError::FileCorrupt);
                }
            };
            if metrics_enabled && let Some(metrics) = metrics {
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.blocking_task_stage,
                    read_result.blocking_task_duration.as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.access_check_stage,
                    read_result.access_check_duration.as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.path_resolve_stage,
                    read_result.path_resolve_duration.as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.metadata_lookup_stage,
                    read_result.metadata_lookup_duration.as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.metadata_validate_stage,
                    read_result.metadata_validate_duration.as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.file_open_stage,
                    read_result.file_open_duration.as_secs_f64(),
                );
                if read_result.used_direct_io {
                    rustfs_io_metrics::record_get_object_stage_duration(
                        metrics.path,
                        metrics.direct_read_copy_stage,
                        read_result.direct_read_copy_duration.as_secs_f64(),
                    );
                    record_direct_read_page_fault_delta(
                        metrics.path,
                        metrics.direct_read_copy_stage,
                        read_result.direct_read_copy_fault_delta,
                    );
                } else {
                    match read_copy_method {
                        LocalReadCopyMethod::MmapCopy => {
                            rustfs_io_metrics::record_get_object_stage_duration(
                                metrics.path,
                                metrics.mmap_map_stage,
                                read_result.mmap_map_duration.as_secs_f64(),
                            );
                            rustfs_io_metrics::record_get_object_stage_duration(
                                metrics.path,
                                metrics.mmap_copy_stage,
                                read_result.mmap_copy_duration.as_secs_f64(),
                            );
                            record_mmap_page_fault_delta(metrics.path, metrics.mmap_map_stage, read_result.mmap_map_fault_delta);
                            record_mmap_page_fault_delta(
                                metrics.path,
                                metrics.mmap_copy_stage,
                                read_result.mmap_copy_fault_delta,
                            );
                        }
                        LocalReadCopyMethod::DirectReadCopy => {
                            rustfs_io_metrics::record_get_object_stage_duration(
                                metrics.path,
                                metrics.direct_read_copy_stage,
                                read_result.direct_read_copy_duration.as_secs_f64(),
                            );
                            record_direct_read_page_fault_delta(
                                metrics.path,
                                metrics.direct_read_copy_stage,
                                read_result.direct_read_copy_fault_delta,
                            );
                        }
                    }
                }
            }
            let bytes = read_result.bytes;

            // Log successful mmap read metrics
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

            // Record mmap read metrics
            rustfs_io_metrics::record_zero_copy_read(length, duration_ms);

            debug!(
                size = length,
                duration_ms = duration_ms,
                mmap_populate = should_populate_mmap_read,
                read_copy_method = ?read_copy_method,
                "mmap_read_success"
            );

            return Ok(bytes);
        }

        // Non-Unix fallback: efficient read into Bytes
        #[cfg(not(unix))]
        {
            // Record zero-copy fallback
            rustfs_io_metrics::record_zero_copy_fallback("non_unix_platform");

            debug!(reason = "non_unix_platform", "zero_copy_fallback");

            let access_check_start = metrics_enabled.then(std::time::Instant::now);
            let volume_dir = local_disk_bucket_path(&self.root, volume)?;
            if !skip_access_checks(volume) {
                access(&volume_dir)
                    .await
                    .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
            }
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.access_check_stage, access_check_start);
            }

            let path_resolve_start = metrics_enabled.then(std::time::Instant::now);
            let file_path = local_disk_object_path(&self.root, volume, path)?;
            check_path_length(file_path.to_string_lossy().as_ref())?;
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.path_resolve_stage, path_resolve_start);
            }

            let file_path_clone = file_path.clone();
            let metadata_lookup_start = metrics_enabled.then(std::time::Instant::now);
            let meta_result = tokio::task::spawn_blocking(move || std::fs::metadata(&file_path_clone).map_err(DiskError::from))
                .await
                .map_err(DiskError::from)
                .and_then(|result| result);
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.metadata_lookup_stage, metadata_lookup_start);
            }
            let meta = meta_result?;

            let metadata_validate_start = metrics_enabled.then(std::time::Instant::now);
            if meta.len() < end_offset as u64 {
                if let Some(metrics) = metrics {
                    record_mmap_copy_stage(metrics, metrics.metadata_validate_stage, metadata_validate_start);
                }
                error!(
                    event = EVENT_DISK_LOCAL_READ_VERSION_FALLBACK,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    volume,
                    path,
                    offset,
                    length,
                    actual_size = meta.len(),
                    reason = "read_file_mmap_copy_out_of_bounds",
                    "Disk local read fallback failed"
                );
                return Err(DiskError::FileCorrupt);
            }
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.metadata_validate_stage, metadata_validate_start);
            }

            let mut f = self.open_file(file_path, O_RDONLY, volume_dir).await?;

            if offset > 0 {
                f.seek(SeekFrom::Start(offset as u64)).await?;
            }

            let mut buffer = vec![0; length];
            f.read_exact(&mut buffer).await?;

            Ok(Bytes::from(buffer))
        }
    }

    async fn open_read_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader> {
        let volume_dir = local_disk_bucket_path(&self.root, volume)?;
        if !skip_access_checks(volume) {
            access(&volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        let file_path = local_disk_object_path(&self.root, volume, path)?;
        check_path_length(file_path.to_string_lossy().as_ref())?;

        let mut f = self.open_file_read_only(file_path).await?;

        let meta = f.metadata().await?;
        let end_offset = offset.checked_add(length).ok_or(DiskError::FileCorrupt)?;
        if meta.len() < end_offset as u64 {
            error!(
                event = EVENT_DISK_LOCAL_READ_VERSION_FALLBACK,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                volume,
                path,
                offset,
                length,
                actual_size = meta.len(),
                reason = "read_file_stream_out_of_bounds",
                "Disk local read fallback failed"
            );
            return Err(DiskError::FileCorrupt);
        }

        if offset > 0 {
            f.seek(SeekFrom::Start(offset as u64)).await?;
        }

        let reclaim_on_drop = should_reclaim_file_cache_after_read(length);
        let reader = FileCacheReclaimReader::new(f, offset as u64, length, reclaim_on_drop);
        Ok(Box::new(StallTimeoutReader::new(reader, get_object_disk_read_timeout())))
    }

    async fn open_full_read(&self, volume: &str, path: &str) -> Result<FileReader> {
        let volume_dir = local_disk_bucket_path(&self.root, volume)?;
        if !skip_access_checks(volume) {
            access(&volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        let file_path = local_disk_object_path(&self.root, volume, path)?;
        check_path_length(file_path.to_string_lossy().as_ref())?;

        let f = self.open_file_read_only(file_path).await?;

        Ok(Box::new(f))
    }

    async fn open_write(&self, volume: &str, path: &str, mode: WriteMode) -> Result<FileWriter> {
        match mode {
            WriteMode::Truncate { size_hint } => {
                let volume_dir = local_disk_bucket_path(&self.root, volume)?;
                let file_path = local_disk_object_path(&self.root, volume, path)?;
                check_path_length(file_path.to_string_lossy().as_ref())?;

                if let Some(parent) = file_path.parent() {
                    os::make_dir_all(parent, &volume_dir).await?;
                }
                // O_TRUNC: if a file already exists at this path, stale trailing bytes past
                // the new content would otherwise survive and mismatch the metadata size.
                let f = super::fs::open_file(&file_path, O_CREATE | O_WRONLY | O_TRUNC)
                    .await
                    .map_err(to_file_error)?;
                let reclaim_on_shutdown = should_reclaim_file_cache_after_write(size_hint);

                Ok(Box::new(FileCacheReclaimWriter::new(f, size_hint.max(0) as usize, reclaim_on_shutdown)))
            }
            WriteMode::Append => {
                let volume_dir = local_disk_bucket_path(&self.root, volume)?;
                if !skip_access_checks(volume) {
                    access(&volume_dir)
                        .await
                        .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
                }

                let file_path = local_disk_object_path(&self.root, volume, path)?;
                check_path_length(file_path.to_string_lossy().as_ref())?;

                let f = self.open_file(file_path, O_CREATE | O_APPEND | O_WRONLY, volume_dir).await?;

                Ok(Box::new(f))
            }
        }
    }
}

pub struct LocalDisk {
    pub root: PathBuf,
    pub format_path: PathBuf,
    pub format_info: RwLock<FormatInfo>,
    pub endpoint: Endpoint,
    pub disk_info_cache: Arc<Cache<DiskInfo>>,
    pub scanning: Arc<AtomicU32>,
    pub rotational: bool,
    pub fstype: String,
    pub major: u64,
    pub minor: u64,
    pub nrrequests: u64,
    // Performance optimization fields
    path_cache: Arc<ParkingLotRwLock<HashMap<String, PathBuf>>>,
    current_dir: Arc<OnceLock<PathBuf>>,
    // pub id: Mutex<Option<Uuid>>,
    // pub format_data: Mutex<Vec<u8>>,
    // pub format_file_info: Mutex<Option<Metadata>>,
    // pub format_last_check: Mutex<Option<OffsetDateTime>>,
    startup_cleanup_ready: Arc<AtomicU32>,
    startup_cleanup_notify: Arc<Notify>,
    exit_signal: Option<tokio::sync::broadcast::Sender<()>>,
    io_backend: Arc<dyn LocalIoBackend>,
}

impl Drop for LocalDisk {
    fn drop(&mut self) {
        if let Some(exit_signal) = self.exit_signal.take() {
            let _ = exit_signal.send(());
        }
    }
}

impl Debug for LocalDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalDisk")
            .field("root", &self.root)
            .field("format_path", &self.format_path)
            .field("format_info", &self.format_info)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

/// Resolve the local disk root path from an endpoint path.
///
/// Tries `canonicalize` first (fast path). On Windows, if canonicalization reports
/// `NotFound` for paths that may still be valid mount roots, falls back to
/// `absolutize` + metadata check to accept valid local directory roots that
/// don't support full canonicalization.
fn resolve_local_disk_root(ep_path: &str) -> Result<PathBuf> {
    match rustfs_utils::canonicalize(ep_path) {
        Ok(path) => Ok(path),
        Err(err) => {
            if err.kind() != ErrorKind::NotFound {
                return Err(to_file_error(err).into());
            }

            #[cfg(windows)]
            {
                // On Windows, canonicalize can fail for ZFS volumes, junction points,
                // subst drives, and other non-standard filesystem mounts. Try a fallback
                // path resolution using absolutize + metadata check.
                let absolute = match crate::disk::endpoint::windows_fallback_local_path(ep_path, &err, "local disk root") {
                    Ok(path) => path,
                    Err(_) => {
                        return Err(DiskError::VolumeNotFound);
                    }
                };

                match std::fs::metadata(&absolute) {
                    Ok(metadata) => {
                        if !metadata.is_dir() {
                            return Err(DiskError::DiskNotDir);
                        }
                        return Ok(absolute);
                    }
                    Err(meta_err) => {
                        if meta_err.kind() == ErrorKind::NotFound {
                            return Err(DiskError::VolumeNotFound);
                        }
                        return Err(to_file_error(meta_err).into());
                    }
                }
            }

            #[cfg(not(windows))]
            {
                Err(DiskError::VolumeNotFound)
            }
        }
    }
}

impl LocalDisk {
    pub async fn new(ep: &Endpoint, cleanup: bool) -> Result<Self> {
        debug!(
            event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
            endpoint = %ep,
            state = "create_started",
            cleanup,
            "Local disk creation started"
        );
        let endpoint_path = ep.get_file_path();
        let root = resolve_local_disk_root(&endpoint_path).inspect_err(|err| {
            log_startup_disk_error("resolve_local_disk_root", Path::new(&endpoint_path), err);
        })?;

        ensure_data_usage_layout(&root)
            .await
            .map_err(DiskError::from)
            .inspect_err(|err| {
                log_startup_disk_error("ensure_data_usage_layout", &root, err);
            })?;

        let startup_cleanup_ready = Arc::new(AtomicU32::new(u32::from(!cleanup)));
        let startup_cleanup_notify = Arc::new(Notify::new());

        if cleanup
            && let Err(err) =
                Self::cleanup_tmp_on_startup(&root, startup_cleanup_ready.clone(), startup_cleanup_notify.clone()).await
        {
            startup_cleanup_ready.store(1, Ordering::Release);
            startup_cleanup_notify.notify_waiters();
            warn!(
                event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                root = ?root,
                state = "failed",
                error = ?err,
                "Local disk startup cleanup failed"
            );
        }

        // Use optimized path resolution instead of absolutize_virtually
        let format_path = root.join(RUSTFS_META_BUCKET).join(super::FORMAT_CONFIG_FILE);
        debug!(
            event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
            root = ?root,
            format_path = ?format_path,
            state = "format_path_resolved",
            "Local disk format path resolved"
        );
        let (format_data, format_meta) = read_file_exists(&format_path).await.inspect_err(|err| {
            log_startup_disk_error("read_format_json", &format_path, err);
        })?;

        let mut id = None;
        // let mut format_legacy = false;
        let mut format_last_check = None;

        if !format_data.is_empty() {
            let s = format_data.as_ref();
            let fm = FormatV3::try_from(s).map_err(Error::other)?;
            let (set_idx, disk_idx) = fm.find_disk_index_by_disk_id(fm.erasure.this)?;

            if set_idx as i32 != ep.set_idx || disk_idx as i32 != ep.disk_idx {
                return Err(DiskError::InconsistentDisk);
            }

            id = Some(fm.erasure.this);
            // format_legacy = fm.erasure.distribution_algo == DistributionAlgoVersion::V1;
            format_last_check = Some(OffsetDateTime::now_utc());
        }

        let format_info = FormatInfo {
            id,
            data: format_data,
            file_info: format_meta,
            last_check: format_last_check,
        };
        let root_clone = root.clone();
        let update_fn: UpdateFn<DiskInfo> = Box::new(move || {
            let disk_id = id;
            let root = root_clone.clone();
            Box::pin(async move {
                match get_disk_info(root.clone()).await {
                    Ok((info, is_root_disk)) => {
                        let physical_device_ids = match rustfs_utils::os::get_physical_device_ids(root.to_string_lossy().as_ref())
                        {
                            Ok(ids) => ids,
                            Err(err) => {
                                warn!(
                                    event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                                    root = ?root,
                                    state = "physical_device_id_lookup_failed",
                                    error = ?err,
                                    "Disk local startup metadata lookup failed"
                                );
                                Vec::new()
                            }
                        };
                        // An erasure-set heal drops a marker on the disks it is
                        // rebuilding (see rustfs-heal); surface it so scanner
                        // coordination, lock selection and admin/metrics see
                        // the rebuild. Refreshed with this cache (~1s).
                        let healing =
                            tokio::fs::try_exists(root.join(super::RUSTFS_META_BUCKET).join(super::HEALING_MARKER_PATH))
                                .await
                                .unwrap_or(false);
                        let disk_info = DiskInfo {
                            total: info.total,
                            free: info.free,
                            used: info.used,
                            used_inodes: info.files.saturating_sub(info.ffree),
                            free_inodes: info.ffree,
                            major: info.major,
                            minor: info.minor,
                            fs_type: info.fstype,
                            root_disk: is_root_disk,
                            physical_device_ids,
                            id: disk_id,
                            healing,
                            ..Default::default()
                        };
                        // if root {
                        //     return Err(Error::new(DiskError::DriveIsRoot));
                        // }

                        Ok(disk_info)
                    }
                    Err(err) => Err(err.into()),
                }
            })
        });

        let cache = Cache::new(update_fn, Duration::from_secs(1), Opts::default());

        // TODO: DIRECT support
        // TODD: DiskInfo
        let mut disk = Self {
            root: root.clone(),
            endpoint: ep.clone(),
            format_path,
            format_info: RwLock::new(format_info),
            disk_info_cache: Arc::new(cache),
            scanning: Arc::new(AtomicU32::new(0)),
            rotational: Default::default(),
            fstype: Default::default(),
            minor: Default::default(),
            major: Default::default(),
            nrrequests: Default::default(),
            // // format_legacy,
            // format_file_info: Mutex::new(format_meta),
            // format_data: Mutex::new(format_data),
            // format_last_check: Mutex::new(format_last_check),
            path_cache: Arc::new(ParkingLotRwLock::new(HashMap::with_capacity(2048))),
            current_dir: Arc::new(OnceLock::new()),
            startup_cleanup_ready,
            startup_cleanup_notify,
            exit_signal: None,
            io_backend: Arc::new(StdBackend::new(root.clone())),
        };
        let (info, _root) = get_disk_info(root.clone()).await.inspect_err(|err| {
            log_startup_disk_error("get_disk_info", &root, err);
        })?;
        disk.major = info.major;
        disk.minor = info.minor;
        disk.fstype = info.fstype;

        // if root {
        //     return Err(Error::new(DiskError::DriveIsRoot));
        // }

        if info.nrrequests > 0 {
            disk.nrrequests = info.nrrequests;
        }

        if info.rotational {
            disk.rotational = true;
        }

        disk.make_meta_volumes().await.inspect_err(|err| {
            log_startup_disk_error("make_meta_volumes", &disk.root, err);
        })?;

        let (exit_tx, exit_rx) = tokio::sync::broadcast::channel(1);
        disk.exit_signal = Some(exit_tx);

        let root = disk.root.clone();
        tokio::spawn(Self::cleanup_deleted_objects_loop(root, exit_rx));
        debug!(
            event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
            endpoint = %disk.endpoint,
            root = ?disk.root,
            state = "created",
            "Local disk created"
        );
        Ok(disk)
    }

    async fn cleanup_deleted_objects_loop(root: PathBuf, mut exit_rx: tokio::sync::broadcast::Receiver<()>) {
        let start_at = Instant::now() + DELETED_OBJECTS_CLEANUP_INTERVAL;
        let mut interval = interval_at(start_at, DELETED_OBJECTS_CLEANUP_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(err) = Self::cleanup_deleted_objects(root.clone()).await {
                        error!(
                            event = EVENT_DISK_LOCAL_BACKGROUND_CLEANUP,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                            task = "deleted_objects",
                            state = "failed",
                            error = ?err,
                            "Disk local background cleanup failed"
                        );
                    }
                    if let Err(err) = Self::cleanup_stale_tmp_objects(root.clone()).await {
                        error!(
                            event = EVENT_DISK_LOCAL_BACKGROUND_CLEANUP,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                            task = "stale_tmp_objects",
                            state = "failed",
                            error = ?err,
                            "Disk local background cleanup failed"
                        );
                    }
                }
                _ = exit_rx.recv() => {
                    info!(
                        event = EVENT_DISK_LOCAL_BACKGROUND_CLEANUP,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        task = "deleted_objects_loop",
                        state = "stopped",
                        "Disk local background cleanup loop stopped"
                    );
                    break;
                }
            }
        }
    }

    fn meta_path(root: &Path, meta_path: &str) -> PathBuf {
        #[cfg(windows)]
        let meta_path = meta_path.replace('/', "\\");
        #[cfg(not(windows))]
        let meta_path = meta_path.to_string();

        root.join(meta_path)
    }

    async fn cleanup_tmp_on_startup(
        root: &Path,
        startup_cleanup_ready: Arc<AtomicU32>,
        startup_cleanup_notify: Arc<Notify>,
    ) -> Result<()> {
        let tmp_path = Self::meta_path(root, RUSTFS_META_TMP_BUCKET);
        let tmp_old_path = Self::meta_path(root, RUSTFS_META_TMP_OLD_BUCKET).join(Uuid::new_v4().to_string());

        rename_all(&tmp_path, &tmp_old_path, root).await.inspect_err(|err| {
            log_startup_disk_error("cleanup_tmp_rename_all", &tmp_path, err);
        })?;

        let tmp_deleted_path = Self::meta_path(root, RUSTFS_META_TMP_DELETED_BUCKET);
        tokio::fs::create_dir_all(&tmp_deleted_path).await.inspect_err(|err| {
            log_startup_disk_io_error("cleanup_tmp_create_deleted_dir", &tmp_deleted_path, err);
        })?;

        let tmp_old_root = Self::meta_path(root, RUSTFS_META_TMP_OLD_BUCKET);
        tokio::spawn(async move {
            if let Err(err) = tokio::fs::remove_dir_all(&tmp_old_root).await
                && err.kind() != ErrorKind::NotFound
            {
                log_startup_disk_io_error("cleanup_tmp_remove_old_dir", &tmp_old_root, &err);
            }
            startup_cleanup_ready.store(1, Ordering::Release);
            startup_cleanup_notify.notify_waiters();
        });

        Ok(())
    }

    async fn wait_for_startup_cleanup(&self) {
        if self.startup_cleanup_ready.load(Ordering::Acquire) != 0 {
            return;
        }

        if wait_for_startup_cleanup_signal(
            self.startup_cleanup_ready.as_ref(),
            self.startup_cleanup_notify.as_ref(),
            STARTUP_CLEANUP_WAIT_TIMEOUT,
        )
        .await
        {
            debug!(disk = %self.endpoint, "startup cleanup barrier released before walk_dir");
        } else {
            warn!(
                event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                disk = %self.endpoint,
                timeout_ms = STARTUP_CLEANUP_WAIT_TIMEOUT.as_millis(),
                state = "timed_out",
                "Disk local startup cleanup barrier timed out"
            );
        }
    }

    async fn cleanup_stale_tmp_objects(root: PathBuf) -> Result<()> {
        Self::cleanup_stale_tmp_objects_with_expiry(root, STALE_TMP_OBJECT_EXPIRY).await
    }

    async fn cleanup_stale_tmp_objects_with_expiry(root: PathBuf, expiry: Duration) -> Result<()> {
        let tmp_path = Self::meta_path(&root, RUSTFS_META_TMP_BUCKET);
        let mut entries = match fs::read_dir(&tmp_path).await {
            Ok(entries) => entries,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Ok(());
                }
                return Err(e.into());
            }
        };

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.is_empty() || name == "." || name == ".." || name == ".trash" {
                continue;
            }

            let file_type = entry.file_type().await?;
            if !file_type.is_dir() {
                continue;
            }

            let Some(age) = entry
                .metadata()
                .await?
                .modified()
                .ok()
                .and_then(|modified| modified.elapsed().ok())
            else {
                continue;
            };
            if age <= expiry {
                continue;
            }

            let target_path = Self::meta_path(&root, RUSTFS_META_TMP_DELETED_BUCKET).join(Uuid::new_v4().to_string());
            rename_all(entry.path(), target_path, Self::meta_path(&root, RUSTFS_META_BUCKET)).await?;
        }

        Ok(())
    }

    async fn cleanup_deleted_objects(root: PathBuf) -> Result<()> {
        let trash = Self::meta_path(&root, RUSTFS_META_TMP_DELETED_BUCKET);
        let mut entries = match fs::read_dir(&trash).await {
            Ok(entries) => entries,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Ok(());
                }
                return Err(e.into());
            }
        };

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.is_empty() || name == "." || name == ".." {
                continue;
            }

            let file_type = entry.file_type().await?;

            let path = trash.join(name);

            if file_type.is_dir() {
                if let Err(e) = tokio::fs::remove_dir_all(path).await
                    && e.kind() != ErrorKind::NotFound
                {
                    return Err(e.into());
                }
            } else if let Err(e) = tokio::fs::remove_file(path).await
                && e.kind() != ErrorKind::NotFound
            {
                return Err(e.into());
            }
        }

        Ok(())
    }

    fn is_valid_volname(volname: &str) -> bool {
        if volname.len() < 3 {
            return false;
        }

        if cfg!(target_os = "windows") {
            // Windows volume names must not include reserved characters.
            // This regular expression matches disallowed characters.
            if volname.contains('|')
                || volname.contains('<')
                || volname.contains('>')
                || volname.contains('?')
                || volname.contains('*')
                || volname.contains(':')
                || volname.contains('"')
                || volname.contains('\\')
            {
                return false;
            }
        } else {
            // Non-Windows systems may require additional validation rules.
        }

        true
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn check_format_json(&self) -> Result<Metadata> {
        let md = fs::metadata(&self.format_path).await.map_err(to_unformatted_disk_error)?;
        Ok(md)
    }
    async fn make_meta_volumes(&self) -> Result<()> {
        let buckets = format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}");
        let multipart = format!("{}/{}", RUSTFS_META_BUCKET, "multipart");
        let config = format!("{}/{}", RUSTFS_META_BUCKET, "config");
        let tmp = format!("{}/{}", RUSTFS_META_BUCKET, "tmp");

        let defaults = vec![
            buckets.as_str(),
            multipart.as_str(),
            config.as_str(),
            tmp.as_str(),
            RUSTFS_META_TMP_DELETED_BUCKET,
        ];

        self.make_volumes(defaults).await
    }

    // Optimized path resolution with caching
    pub fn resolve_abs_path(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy();

        // Fast cache read
        {
            let cache = self.path_cache.read();
            if let Some(cached_path) = cache.get(path_str.as_ref()) {
                return Ok(cached_path.clone());
            }
        }

        // Calculate absolute path without using path_absolutize for better performance
        let abs_path = if path_ref.is_absolute() {
            path_ref.to_path_buf()
        } else {
            #[cfg(windows)]
            {
                self.root.join(path_str.replace('/', "\\"))
            }
            #[cfg(not(windows))]
            {
                self.root.join(path_ref)
            }
        };

        // Normalize path components to avoid filesystem calls
        let normalized = normalize_path_components(abs_path.as_path());

        // Cache the result
        {
            let mut cache = self.path_cache.write();

            // Simple cache size control
            if cache.len() >= 4096 {
                // Clear half the cache - simple eviction strategy
                let keys_to_remove: Vec<_> = cache.keys().take(cache.len() / 2).cloned().collect();
                for key in keys_to_remove {
                    cache.remove(&key);
                }
            }

            cache.insert(path_str.into_owned(), normalized.clone());
        }

        Ok(normalized)
    }

    // Get the absolute path of an object
    pub fn get_object_path(&self, bucket: &str, key: &str) -> Result<PathBuf> {
        local_disk_object_path(&self.root, bucket, key)
    }

    // Get the absolute path of a bucket
    pub fn get_bucket_path(&self, bucket: &str) -> Result<PathBuf> {
        local_disk_bucket_path(&self.root, bucket)
    }

    // Check if a path is valid
    fn check_valid_path<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        check_local_disk_valid_path(&self.root, path)
    }

    fn reject_symlink_components(&self, path: &Path) -> Result<()> {
        reject_local_disk_symlink_components(&self.root, path)
    }

    // Batch path generation with single lock acquisition
    fn get_object_paths_batch(&self, requests: &[(String, String)]) -> Result<Vec<PathBuf>> {
        let mut results = Vec::with_capacity(requests.len());
        let mut cache_misses = Vec::new();

        // First attempt to get all paths from cache
        {
            let cache = self.path_cache.read();
            for (i, (bucket, key)) in requests.iter().enumerate() {
                let cache_key = path_join_buf(&[bucket, key]);
                if let Some(cached_path) = cache.get(&cache_key) {
                    results.push((i, cached_path.clone()));
                } else {
                    cache_misses.push((i, bucket, key, cache_key));
                }
            }
        }

        // Handle cache misses
        if !cache_misses.is_empty() {
            let mut new_entries = Vec::new();
            for (i, _bucket, _key, cache_key) in cache_misses {
                #[cfg(windows)]
                let path = self.root.join(cache_key.replace('/', "\\"));
                #[cfg(not(windows))]
                let path = self.root.join(&cache_key);

                results.push((i, path.clone()));
                new_entries.push((cache_key, path));
            }

            // Batch update cache
            {
                let mut cache = self.path_cache.write();
                for (key, path) in new_entries {
                    cache.insert(key, path);
                }
            }
        }

        // Sort results back to original order
        results.sort_by_key(|(i, _)| *i);
        Ok(results.into_iter().map(|(_, path)| path).collect())
    }

    // /// Write to the filesystem atomically.
    // /// This is done by first writing to a temporary location and then moving the file.
    // pub(crate) async fn prepare_file_write<'a>(&self, path: &'a PathBuf) -> Result<FileWriter<'a>> {
    //     let tmp_path = self.get_object_path(RUSTFS_META_TMP_BUCKET, Uuid::new_v4().to_string().as_str())?;

    //     debug!("prepare_file_write tmp_path:{:?}, path:{:?}", &tmp_path, &path);

    //     let file = File::create(&tmp_path).await?;
    //     let writer = BufWriter::new(file);
    //     Ok(FileWriter {
    //         tmp_path,
    //         dest_path: path,
    //         writer,
    //         clean_tmp: true,
    //     })
    // }

    async fn move_to_trash(&self, delete_path: &PathBuf, recursive: bool, immediate_purge: bool) -> Result<()> {
        // if recursive {
        //     remove_all_std(delete_path).map_err(to_volume_error)?;
        // } else {
        //     remove_std(delete_path).map_err(to_file_error)?;
        // }

        // return Ok(());

        // TODO: async notifications for disk space checks and trash cleanup

        let trash_path = self.get_object_path(RUSTFS_META_TMP_DELETED_BUCKET, Uuid::new_v4().to_string().as_str())?;
        // if let Some(parent) = trash_path.parent() {
        //     if !parent.exists() {
        //         fs::create_dir_all(parent).await?;
        //     }
        // }

        let err = if recursive {
            rename_all_ignore_missing_source(delete_path, trash_path, self.get_bucket_path(RUSTFS_META_TMP_DELETED_BUCKET)?)
                .await
                .err()
        } else {
            match rename(&delete_path, &trash_path).await {
                Ok(()) => None,
                Err(err) if err.kind() == ErrorKind::NotFound => None,
                Err(err) => Some(to_file_error(err).into()),
            }
        };

        if immediate_purge || delete_path.to_string_lossy().ends_with(SLASH_SEPARATOR) {
            let trash_path2 = self.get_object_path(RUSTFS_META_TMP_DELETED_BUCKET, Uuid::new_v4().to_string().as_str())?;
            let _ = rename_all_ignore_missing_source(
                encode_dir_object(delete_path.to_string_lossy().as_ref()),
                trash_path2,
                self.get_bucket_path(RUSTFS_META_TMP_DELETED_BUCKET)?,
            )
            .await;
        }

        if let Some(err) = err {
            if err == Error::DiskFull {
                if recursive {
                    remove_all_std(delete_path).map_err(to_volume_error)?;
                } else {
                    remove_std(delete_path).map_err(to_file_error)?;
                }
            }

            return Ok(());
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    #[async_recursion::async_recursion]
    async fn delete_file(
        &self,
        base_path: &PathBuf,
        delete_path: &PathBuf,
        recursive: bool,
        immediate_purge: bool,
    ) -> Result<()> {
        // debug!("delete_file {:?}\n base_path:{:?}", &delete_path, &base_path);

        if is_root_path(base_path) || is_root_path(delete_path) {
            // debug!("delete_file skip {:?}", &delete_path);
            return Ok(());
        }

        if !delete_path.starts_with(base_path) || base_path == delete_path {
            // debug!("delete_file skip {:?}", &delete_path);
            return Ok(());
        }

        if recursive {
            self.move_to_trash(delete_path, recursive, immediate_purge).await?;
        } else if delete_path.is_dir() {
            // debug!("delete_file remove_dir {:?}", &delete_path);
            if let Err(err) = fs::remove_dir(&delete_path).await {
                // debug!("remove_dir err {:?} when {:?}", &err, &delete_path);
                match err.kind() {
                    ErrorKind::NotFound => (),
                    ErrorKind::DirectoryNotEmpty => (),
                    kind => {
                        warn!(
                            event = EVENT_DISK_LOCAL_DELETE_FAILED,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                            path = ?delete_path,
                            operation = "remove_dir",
                            error_kind = %kind,
                            "Disk local delete failed"
                        );
                        return Err(Error::other(FileAccessDeniedWithContext {
                            path: delete_path.clone(),
                            source: err,
                        }));
                    }
                }
            }
            // debug!("delete_file remove_dir done {:?}", &delete_path);
        } else if let Err(err) = fs::remove_file(&delete_path).await {
            // debug!("remove_file err {:?} when {:?}", &err, &delete_path);
            match err.kind() {
                ErrorKind::NotFound => (),
                _ => {
                    warn!(
                        event = EVENT_DISK_LOCAL_DELETE_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = ?delete_path,
                        operation = "remove_file",
                        error = ?err,
                        "Disk local delete failed"
                    );
                    return Err(Error::other(FileAccessDeniedWithContext {
                        path: delete_path.clone(),
                        source: err,
                    }));
                }
            }
        }

        if let Some(dir_path) = delete_path.parent() {
            Box::pin(self.delete_file(base_path, &PathBuf::from(dir_path), false, false)).await?;
        }

        // debug!("delete_file done {:?}", &delete_path);
        Ok(())
    }

    /// read xl.meta raw data
    #[tracing::instrument(level = "debug", skip(self, volume_dir, file_path))]
    async fn read_raw(
        &self,
        bucket: &str,
        volume_dir: impl AsRef<Path>,
        file_path: impl AsRef<Path>,
        read_data: bool,
    ) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        if file_path.as_ref().as_os_str().is_empty() {
            return Err(DiskError::FileNotFound);
        }

        let meta_path = path_join(&[file_path.as_ref(), Path::new(STORAGE_FORMAT_FILE)]);

        let res = {
            if read_data {
                self.read_all_data_with_dmtime(bucket, volume_dir, meta_path).await
            } else {
                match self.read_metadata_with_dmtime(meta_path).await {
                    Ok(res) => Ok(res),
                    Err(err) => {
                        if err == Error::FileNotFound
                            && !skip_access_checks(volume_dir.as_ref().to_string_lossy().to_string().as_str())
                            && let Err(e) = access(volume_dir.as_ref()).await
                            && e.kind() == ErrorKind::NotFound
                        {
                            // warn!("read_metadata_with_dmtime os err {:?}", &aerr);
                            return Err(DiskError::VolumeNotFound);
                        }

                        Err(err)
                    }
                }
            }
        };

        let (buf, mtime) = res?;
        if buf.is_empty() {
            return Err(DiskError::FileNotFound);
        }

        Ok((buf, mtime))
    }

    async fn read_metadata_with_dmtime(&self, file_path: impl AsRef<Path>) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        check_path_length(file_path.as_ref().to_string_lossy().as_ref())?;

        let mut f = super::fs::open_file(file_path.as_ref(), O_RDONLY)
            .await
            .map_err(to_file_error)?;

        let meta = f.metadata().await.map_err(to_file_error)?;

        if meta.is_dir() {
            // fix use io::Error
            return Err(Error::FileNotFound);
        }

        let size = meta.len() as usize;

        let data = read_xl_meta_no_data(&mut f, size).await?;

        let modtime = match meta.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => None,
        };

        Ok((data, modtime))
    }

    async fn read_all_data(&self, volume: &str, volume_dir: impl AsRef<Path>, file_path: impl AsRef<Path>) -> Result<Vec<u8>> {
        // TODO: timeout support
        let (data, _) = self.read_all_data_with_dmtime(volume, volume_dir, file_path).await?;
        Ok(data)
    }

    #[tracing::instrument(level = "debug", skip(self, volume_dir, file_path))]
    async fn read_all_data_with_dmtime(
        &self,
        volume: &str,
        volume_dir: impl AsRef<Path>,
        file_path: impl AsRef<Path>,
    ) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        let mut f = match super::fs::open_file(file_path.as_ref(), O_RDONLY).await {
            Ok(f) => f,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound
                    && !skip_access_checks(volume)
                    && let Err(er) = access(volume_dir.as_ref()).await
                    && er.kind() == ErrorKind::NotFound
                {
                    warn!(
                        event = EVENT_DISK_LOCAL_READ_VERSION_FALLBACK,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        reason = "read_all_data_with_dmtime_volume_not_found",
                        error = ?er,
                        "Disk local read fallback failed"
                    );
                    return Err(DiskError::VolumeNotFound);
                }

                return Err(to_file_error(e).into());
            }
        };

        let meta = f.metadata().await.map_err(to_file_error)?;

        if meta.is_dir() {
            return Err(DiskError::FileNotFound);
        }

        let size = meta.len() as usize;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size).map_err(Error::other)?;

        f.read_to_end(&mut bytes).await.map_err(to_file_error)?;

        let modtime = match meta.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => None,
        };

        Ok((bytes, modtime))
    }

    async fn delete_versions_internal(&self, volume: &str, path: &str, fis: &[FileInfo]) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        let xlpath = self.get_object_path(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str())?;

        let (data, _) = self.read_all_data_with_dmtime(volume, volume_dir.as_path(), &xlpath).await?;

        if data.is_empty() {
            return Err(DiskError::FileNotFound);
        }

        let mut fm = FileMeta::default();

        fm.unmarshal_msg(&data)?;

        for fi in fis.iter() {
            let data_dir = match fm.delete_version(fi) {
                Ok(res) => res,
                Err(err) => {
                    let err: DiskError = err.into();
                    if !fi.deleted && (err == DiskError::FileNotFound || err == DiskError::FileVersionNotFound) {
                        continue;
                    }

                    return Err(err);
                }
            };

            if let Some(dir) = data_dir {
                let vid = fi.version_id.unwrap_or_default();
                let _ = fm.data.remove(vec![vid, dir]);

                let dir_path = self.get_object_path(volume, format!("{path}/{dir}").as_str())?;
                if let Err(err) = self.move_to_trash(&dir_path, true, false).await
                    && !(err == DiskError::FileNotFound || err == DiskError::VolumeNotFound)
                {
                    return Err(err);
                };
            }
        }

        // Remove xl.meta when no versions remain
        if fm.versions.is_empty() {
            self.delete_file(&volume_dir, &xlpath, true, false).await?;
            return Ok(());
        }

        // Update xl.meta atomically: a concurrent reader or crash mid-write must
        // never observe a truncated xl.meta for versions that were not deleted.
        let buf = fm.marshal_msg()?;

        self.write_all_meta(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str(), &buf, true)
            .await?;

        Ok(())
    }

    async fn write_all_meta(&self, volume: &str, path: &str, buf: &[u8], sync: bool) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        let file_path = self.get_object_path(volume, path)?;
        check_path_length(file_path.to_string_lossy().as_ref())?;

        let tmp_volume_dir = self.get_bucket_path(super::RUSTFS_META_TMP_BUCKET)?;
        let tmp_file_path = self.get_object_path(super::RUSTFS_META_TMP_BUCKET, Uuid::new_v4().to_string().as_str())?;

        self.write_all_internal(&tmp_file_path, InternalBuf::Ref(buf), sync, &tmp_volume_dir)
            .await?;

        rename_all(tmp_file_path, &file_path, volume_dir).await?;

        if sync
            && drive_sync_enabled()
            && let Some(parent) = file_path.parent()
        {
            os::fsync_dir(parent).await.map_err(to_file_error)?;
        }

        Ok(())
    }

    // write_all_public for trail
    async fn write_all_public(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        if volume == RUSTFS_META_BUCKET && path == super::FORMAT_CONFIG_FILE {
            let mut format_info = self.format_info.write().await;
            format_info.data.clone_from(&data);
        }

        let volume_dir = self.get_bucket_path(volume)?;

        self.write_all_private(volume, path, data, true, &volume_dir).await?;

        Ok(())
    }

    // write_all_private with check_path_length
    #[tracing::instrument(level = "debug", skip_all)]
    async fn write_all_private(&self, volume: &str, path: &str, buf: Bytes, sync: bool, skip_parent: &Path) -> Result<()> {
        let file_path = self.get_object_path(volume, path)?;
        check_path_length(file_path.to_string_lossy().as_ref())?;

        self.write_all_internal(&file_path, InternalBuf::Owned(buf), sync, skip_parent)
            .await?;

        Ok(())
    }
    // write_all_internal do write file
    async fn write_all_internal(&self, file_path: &Path, data: InternalBuf<'_>, sync: bool, skip_parent: &Path) -> Result<()> {
        let skip_parent = if skip_parent.as_os_str().is_empty() {
            self.root.as_path()
        } else {
            skip_parent
        };

        let sync = sync && drive_sync_enabled();

        match data {
            InternalBuf::Ref(buf) => {
                let mut f = self.open_file(file_path, O_CREATE | O_WRONLY | O_TRUNC, skip_parent).await?;
                f.write_all(buf).await.map_err(to_file_error)?;
                if sync {
                    f.sync_data().await.map_err(to_file_error)?;
                    // Persist the directory entry too, so a freshly created file
                    // survives power loss along with its contents.
                    if let Some(parent) = file_path.parent() {
                        os::fsync_dir(parent).await.map_err(to_file_error)?;
                    }
                }
            }
            InternalBuf::Owned(buf) => {
                let path = file_path.to_path_buf();
                if let Some(parent) = path.parent()
                    && parent != skip_parent
                {
                    os::make_dir_all(parent, skip_parent).await?;
                }

                tokio::task::spawn_blocking(move || {
                    let mut f = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&path)
                        .map_err(to_file_error)?;

                    std::io::Write::write_all(&mut f, buf.as_ref()).map_err(to_file_error)?;
                    if sync {
                        f.sync_data().map_err(to_file_error)?;
                        if let Some(parent) = path.parent() {
                            os::fsync_dir_std(parent).map_err(to_file_error)?;
                        }
                    }

                    Ok::<(), std::io::Error>(())
                })
                .await
                .map_err(DiskError::from)??;
            }
        }

        Ok(())
    }

    async fn open_file(&self, path: impl AsRef<Path>, mode: usize, skip_parent: impl AsRef<Path>) -> Result<File> {
        let mut skip_parent = skip_parent.as_ref();
        if skip_parent.as_os_str().is_empty() {
            skip_parent = self.root.as_path();
        }

        if let Some(parent) = path.as_ref().parent()
            && parent != skip_parent
        {
            os::make_dir_all(parent, skip_parent).await?;
        }

        let f = super::fs::open_file(path.as_ref(), mode).await.map_err(to_file_error)?;

        Ok(f)
    }

    async fn open_file_read_only(&self, path: impl AsRef<Path>) -> Result<File> {
        let f = super::fs::open_file(path.as_ref(), O_RDONLY).await.map_err(to_file_error)?;
        Ok(f)
    }

    #[allow(dead_code)]
    fn get_metrics(&self) -> DiskMetrics {
        DiskMetrics::default()
    }

    async fn bitrot_verify(&self, part_path: &PathBuf, part_size: usize, algo: HashAlgorithm, shard_size: usize) -> Result<()> {
        let retry_count = bitrot_size_mismatch_retry_count();
        let retry_delay = bitrot_size_mismatch_retry_delay();

        for attempt in 0..=retry_count {
            let file = super::fs::open_file(part_path, O_RDONLY).await.map_err(to_file_error)?;
            let meta = file.metadata().await.map_err(to_file_error)?;
            let file_size = meta.len() as usize;

            match bitrot_verify(Box::new(file), file_size, part_size, algo.clone(), shard_size).await {
                Ok(()) => return Ok(()),
                Err(err) if attempt < retry_count && is_bitrot_size_mismatch_error(&err) => {
                    info!(
                        event = EVENT_DISK_LOCAL_CHECK_PARTS,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = %part_path.display(),
                        expected_size = part_size,
                        actual_size = file_size,
                        retry_attempt = attempt + 1,
                        retry_count,
                        retry_delay_ms = retry_delay.as_millis(),
                        state = "bitrot_retry",
                        "Disk local check_parts state changed"
                    );
                    tokio::time::sleep(retry_delay).await;
                }
                Err(err) => return Err(to_file_error(err).into()),
            }
        }

        Err(DiskError::other("bitrot size mismatch retry loop exhausted"))
    }

    #[async_recursion::async_recursion]
    #[allow(clippy::too_many_arguments)]
    async fn scan_dir<W>(
        &self,
        mut current: String,
        mut prefix: String,
        opts: &WalkDirOptions,
        out: &mut MetacacheWriter<W>,
        objs_returned: &mut i32,
        skip_current_dir_object: bool,
        multipart_dir_to_skip: Option<HashSet<String>>,
    ) -> Result<()>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let forward = {
            opts.forward_to
                .as_ref()
                .and_then(|v| v.strip_prefix(&current))
                .map(|forward| {
                    if let Some(idx) = forward.find('/') {
                        forward[..idx].to_owned()
                    } else {
                        forward.to_owned()
                    }
                })
        };

        if opts.limit > 0 && *objs_returned >= opts.limit {
            return Ok(());
        }

        // TODO: add lock

        let mut entries = match self.list_dir("", &opts.bucket, &current, -1).await {
            Ok(res) => res,
            Err(e) => {
                if e != DiskError::VolumeNotFound && e != Error::FileNotFound {
                    error!(
                        event = EVENT_DISK_LOCAL_SCAN_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = %current,
                        operation = "list_dir",
                        error = ?e,
                        "Disk local scan failed"
                    );
                    return Err(e);
                }

                if opts.report_notfound && e == Error::FileNotFound && current == opts.base_dir {
                    return Err(DiskError::FileNotFound);
                }

                return Ok(());
            }
        };

        if entries.is_empty() {
            return Ok(());
        }

        current = current.trim_matches('/').to_owned();

        let bucket = opts.bucket.as_str();

        let mut dir_objes = HashSet::new();

        // First-level filtering
        for item in entries.iter_mut() {
            let entry = item.clone();
            // check limit
            if opts.limit > 0 && *objs_returned >= opts.limit {
                return Ok(());
            }
            // check multipart dir
            if skip_current_dir_object
                && let Some(ref dir_to_skip) = multipart_dir_to_skip
                && dir_to_skip.contains(entry.trim_end_matches(SLASH_SEPARATOR))
            {
                *item = "".to_owned();
                continue;
            }
            // check prefix
            if !prefix.is_empty() && !entry.starts_with(prefix.as_str()) {
                *item = "".to_owned();
                continue;
            }

            if let Some(forward) = &forward
                && &entry < forward
            {
                *item = "".to_owned();
                continue;
            }

            if entry.ends_with(SLASH_SEPARATOR) {
                if entry.ends_with(GLOBAL_DIR_SUFFIX_WITH_SLASH) {
                    let entry = format!("{}{}", entry.as_str().trim_end_matches(GLOBAL_DIR_SUFFIX_WITH_SLASH), SLASH_SEPARATOR);
                    dir_objes.insert(entry.clone());
                    *item = entry;
                    continue;
                }

                *item = entry.trim_end_matches(SLASH_SEPARATOR).to_owned();
                continue;
            }

            *item = "".to_owned();

            if entry.ends_with(STORAGE_FORMAT_FILE) {
                if skip_current_dir_object {
                    continue;
                }

                let metadata = self
                    .read_metadata(bucket, format!("{}/{}", &current, &entry).as_str())
                    .await?;

                let entry = entry.strip_suffix(STORAGE_FORMAT_FILE).unwrap_or_default().to_owned();
                let name = entry.trim_end_matches(SLASH_SEPARATOR);
                let name = decode_dir_object(format!("{}/{}", &current, &name).as_str());

                if opts.limit <= 0 || metadata_counts_toward_limit(&metadata) {
                    *objs_returned += 1;
                }

                out.write_obj(&MetaCacheEntry {
                    name: name.clone(),
                    metadata: metadata.to_vec(),
                    ..Default::default()
                })
                .await?;

                continue;
            }
        }

        entries.sort();

        if let Some(forward) = &forward {
            for (i, entry) in entries.iter().enumerate() {
                if entry >= forward || forward.starts_with(entry.as_str()) {
                    entries.drain(..i);
                    break;
                }
            }
        }

        let mut dir_stack: Vec<(String, bool, Option<HashSet<String>>)> = Vec::with_capacity(5);
        // Explicit directory markers and real directories can resolve to the same logical path.
        let schedule_dir = |dir_stack: &mut Vec<(String, bool, Option<HashSet<String>>)>,
                            dir_name: String,
                            skip_object: bool,
                            dir_to_skip: Option<HashSet<String>>| {
            if let Some((last_dir_name, existing_skip_object, existing_dir_to_skip)) = dir_stack.last_mut()
                && *last_dir_name == dir_name
            {
                *existing_skip_object |= skip_object;
                if let Some(existing_dir_to_skip) = existing_dir_to_skip {
                    if let Some(new_dir_to_skip) = &dir_to_skip {
                        existing_dir_to_skip.extend(new_dir_to_skip.iter().cloned());
                    }
                } else {
                    *existing_dir_to_skip = dir_to_skip;
                }
            } else {
                dir_stack.push((dir_name, skip_object, dir_to_skip));
            }
        };
        prefix = "".to_owned();

        for entry in entries.iter() {
            if opts.limit > 0 && *objs_returned >= opts.limit {
                return Ok(());
            }

            if entry.is_empty() {
                continue;
            }

            let name = path_join_buf(&[current.as_str(), entry.as_str()]);

            while let Some((last_name, _, _)) = dir_stack.last()
                && *last_name < name
            {
                let (pop, skip_object, dir_to_skip) = dir_stack.pop().expect("operation should succeed");
                out.write_obj(&MetaCacheEntry {
                    name: pop.clone(),
                    ..Default::default()
                })
                .await?;

                let scan_path = pop.clone();
                if opts.recursive
                    && let Err(er) =
                        Box::pin(self.scan_dir(pop, prefix.clone(), opts, out, objs_returned, skip_object, dir_to_skip)).await
                {
                    error!(
                        event = EVENT_DISK_LOCAL_SCAN_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = %scan_path,
                        operation = "scan_dir",
                        error = ?er,
                        "Disk local scan failed"
                    );
                    return Err(er);
                }
            }

            let mut meta = MetaCacheEntry {
                name,
                ..Default::default()
            };

            let mut is_dir_obj = false;

            if let Some(_dir) = dir_objes.get(entry) {
                is_dir_obj = true;
                meta.name
                    .truncate(meta.name.len() - meta.name.chars().last().expect("operation should succeed").len_utf8());
                meta.name.push_str(GLOBAL_DIR_SUFFIX_WITH_SLASH);
            }

            let fname = format!("{}/{}", &meta.name, STORAGE_FORMAT_FILE);

            match self.read_metadata(&opts.bucket, fname.as_str()).await {
                Ok(res) => {
                    if is_dir_obj {
                        meta.name = meta.name.trim_end_matches(GLOBAL_DIR_SUFFIX_WITH_SLASH).to_owned();
                        meta.name.push_str(SLASH_SEPARATOR);
                    }

                    meta.metadata = res.to_vec();

                    out.write_obj(&meta).await?;

                    let file_meta = if opts.limit > 0 || opts.recursive {
                        FileMeta::load(&res).ok()
                    } else {
                        None
                    };

                    if opts.limit <= 0 || file_meta.as_ref().is_none_or(file_meta_counts_toward_limit) {
                        *objs_returned += 1;
                    }

                    if opts.recursive {
                        let mut dir_to_skip = HashSet::new();
                        if let Some(file_meta) = file_meta.as_ref()
                            && let Ok(data_dirs) = file_meta.get_data_dirs()
                        {
                            for data_dir in data_dirs.iter().flatten() {
                                dir_to_skip.insert(data_dir.to_string());
                            }
                        }
                        let mut dir_name = meta.name.clone();
                        if !dir_name.ends_with(SLASH_SEPARATOR) {
                            dir_name.push_str(SLASH_SEPARATOR);
                        }
                        schedule_dir(
                            &mut dir_stack,
                            dir_name,
                            true,
                            if dir_to_skip.is_empty() { None } else { Some(dir_to_skip) },
                        );
                    }
                }
                Err(err) => {
                    if err == Error::FileNotFound || err == Error::IsNotRegular {
                        // NOT an object, append to stack (with slash)
                        // If dirObject, but no metadata (which is unexpected) we skip it.
                        if !is_dir_obj && !is_empty_dir(self.get_object_path(&opts.bucket, &meta.name)?).await {
                            meta.name.push_str(SLASH_SEPARATOR);
                            if opts.recursive
                                || opts.incl_deleted
                                || self.directory_has_visible_listing_entry(&opts.bucket, &meta.name).await?
                            {
                                schedule_dir(&mut dir_stack, meta.name, false, None);
                            }
                        }

                        continue;
                    }

                    error!(
                        event = EVENT_DISK_LOCAL_SCAN_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = %fname,
                        operation = "read_metadata",
                        error = ?err,
                        "Disk local scan failed"
                    );
                    return Err(err);
                }
            };
        }

        while let Some((dir, skip_object, dir_to_skip)) = dir_stack.pop() {
            if opts.limit > 0 && *objs_returned >= opts.limit {
                return Ok(());
            }

            out.write_obj(&MetaCacheEntry {
                name: dir.clone(),
                ..Default::default()
            })
            .await?;

            let scan_path = dir.clone();
            if opts.recursive
                && let Err(er) =
                    Box::pin(self.scan_dir(dir, prefix.clone(), opts, out, objs_returned, skip_object, dir_to_skip)).await
            {
                error!(
                    event = EVENT_DISK_LOCAL_SCAN_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    path = %scan_path,
                    operation = "scan_dir",
                    error = ?er,
                    "Disk local recursive scan failed"
                );
                return Err(er);
            }
        }

        Ok(())
    }

    async fn directory_has_visible_listing_entry(&self, bucket: &str, dir_name: &str) -> Result<bool> {
        let mut stack = vec![dir_name.trim_matches('/').to_owned()];

        while let Some(current) = stack.pop() {
            if current.is_empty() {
                continue;
            }

            let entries = match self.list_dir("", bucket, &current, -1).await {
                Ok(entries) => entries,
                Err(err) => {
                    if err == DiskError::VolumeNotFound || err == Error::FileNotFound {
                        continue;
                    }

                    return Err(err);
                }
            };

            let mut data_dirs_to_skip = HashSet::new();
            let mut child_dirs = Vec::new();

            for entry in entries {
                if entry == STORAGE_FORMAT_FILE {
                    let metadata_path = path_join_buf(&[current.as_str(), STORAGE_FORMAT_FILE]);
                    match self.read_metadata(bucket, metadata_path.as_str()).await {
                        Ok(metadata) => {
                            let file_meta = match FileMeta::load(&metadata) {
                                Ok(file_meta) => file_meta,
                                Err(_) => return Ok(true),
                            };

                            if file_meta_counts_toward_limit(&file_meta) {
                                return Ok(true);
                            }

                            if let Ok(data_dirs) = file_meta.get_data_dirs() {
                                for data_dir in data_dirs.iter().flatten() {
                                    data_dirs_to_skip.insert(data_dir.to_string());
                                }
                            }
                        }
                        Err(err) => {
                            if err != Error::FileNotFound && err != Error::IsNotRegular {
                                return Err(err);
                            }
                        }
                    }

                    continue;
                }

                if entry.ends_with(SLASH_SEPARATOR) {
                    let child = entry.trim_end_matches(SLASH_SEPARATOR);
                    if !child.is_empty() {
                        child_dirs.push(child.to_owned());
                    }
                }
            }

            for child in child_dirs {
                if !data_dirs_to_skip.contains(&child) {
                    stack.push(path_join_buf(&[current.as_str(), child.as_str()]));
                }
            }
        }

        Ok(false)
    }
}

pub struct ScanGuard(pub Arc<AtomicU32>);

impl Drop for ScanGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

fn is_root_path(path: impl AsRef<Path>) -> bool {
    path.as_ref().components().count() == 1 && path.as_ref().has_root()
}

fn metadata_counts_toward_limit(metadata: &[u8]) -> bool {
    FileMeta::load(metadata).map_or(true, |meta| file_meta_counts_toward_limit(&meta))
}

fn file_meta_counts_toward_limit(meta: &FileMeta) -> bool {
    meta.into_fileinfo("", "", "", false, true, false)
        .map_or_else(|_| !meta.all_hidden(true), |latest| !latest.deleted && !latest.tier_free_version())
}

// Filter std::io::ErrorKind::NotFound
async fn read_file_exists(path: impl AsRef<Path>) -> Result<(Bytes, Option<Metadata>)> {
    let p = path.as_ref();
    let (data, meta) = match read_file_all(&p).await {
        Ok((data, meta)) => (data, Some(meta)),
        Err(e) => {
            if e == Error::FileNotFound {
                (Bytes::new(), None)
            } else {
                return Err(e);
            }
        }
    };

    // let mut data = Vec::new();
    // if meta.is_some() {
    //     data = fs::read(&p).await?;
    // }

    Ok((data, meta))
}

async fn read_file_all(path: impl AsRef<Path>) -> Result<(Bytes, Metadata)> {
    let p = path.as_ref();
    let meta = read_file_metadata(&path).await?;

    let data = fs::read(&p)
        .await
        .inspect_err(|err| {
            log_startup_disk_io_error("read_file_all", p, err);
        })
        .map_err(to_file_error)?;

    Ok((data.into(), meta))
}

async fn read_file_metadata(p: impl AsRef<Path>) -> Result<Metadata> {
    let path = p.as_ref();
    let meta = fs::metadata(path)
        .await
        .inspect_err(|err| {
            if err.kind() != ErrorKind::NotFound {
                log_startup_disk_io_error("read_file_metadata", path, err);
            }
        })
        .map_err(to_file_error)?;

    Ok(meta)
}

fn skip_access_checks(p: impl AsRef<str>) -> bool {
    let vols = [
        RUSTFS_META_TMP_DELETED_BUCKET,
        super::RUSTFS_META_TMP_BUCKET,
        super::RUSTFS_META_MULTIPART_BUCKET,
        RUSTFS_META_BUCKET,
    ];

    for v in vols.iter() {
        if p.as_ref().starts_with(v) {
            return true;
        }
    }

    false
}

fn local_disk_object_path(root: &Path, bucket: &str, key: &str) -> Result<PathBuf> {
    let cache_key = if key.is_empty() {
        bucket.to_string()
    } else {
        path_join_buf(&[bucket, key])
    };

    #[cfg(windows)]
    let path = root.join(cache_key.replace('/', "\\"));
    #[cfg(not(windows))]
    let path = root.join(cache_key);

    check_local_disk_valid_path(root, &path)?;
    Ok(path)
}

fn local_disk_bucket_path(root: &Path, bucket: &str) -> Result<PathBuf> {
    #[cfg(windows)]
    let bucket_path = root.join(bucket.replace('/', "\\"));
    #[cfg(not(windows))]
    let bucket_path = root.join(bucket);

    check_local_disk_valid_path(root, &bucket_path)?;
    Ok(bucket_path)
}

fn check_local_disk_valid_path(root: &Path, path: impl AsRef<Path>) -> Result<()> {
    let path = normalize_path_components(path);
    if !path.starts_with(root) {
        return Err(DiskError::InvalidPath);
    }

    reject_local_disk_symlink_components(root, &path)
}

fn reject_local_disk_symlink_components(root: &Path, path: &Path) -> Result<()> {
    let relative = path.strip_prefix(root).map_err(|_| DiskError::InvalidPath)?;
    let mut current = root.to_path_buf();

    for component in relative.components() {
        current.push(component.as_os_str());

        match lstat_std(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(DiskError::InvalidPath);
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => break,
            Err(err) => return Err(to_file_error(err).into()),
        }
    }

    Ok(())
}

// Lightweight path normalization without filesystem calls
fn normalize_path_components(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    let mut result = PathBuf::new();

    for component in path.components() {
        match component {
            std::path::Component::Normal(name) => {
                result.push(name);
            }
            std::path::Component::ParentDir => {
                result.pop();
            }
            std::path::Component::CurDir => {
                // Ignore current directory components
            }
            std::path::Component::RootDir => {
                result.push(component);
            }
            std::path::Component::Prefix(_prefix) => {
                result.push(component);
            }
        }
    }

    result
}

#[async_trait::async_trait]
impl DiskAPI for LocalDisk {
    fn to_string(&self) -> String {
        self.root.to_string_lossy().to_string()
    }

    fn is_local(&self) -> bool {
        true
    }

    fn host_name(&self) -> String {
        self.endpoint.host_port()
    }

    async fn is_online(&self) -> bool {
        true
    }

    fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn path(&self) -> PathBuf {
        self.root.clone()
    }

    fn get_disk_location(&self) -> DiskLocation {
        DiskLocation {
            pool_idx: {
                if self.endpoint.pool_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.pool_idx as usize)
                }
            },
            set_idx: {
                if self.endpoint.set_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.set_idx as usize)
                }
            },
            disk_idx: {
                if self.endpoint.disk_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.disk_idx as usize)
                }
            },
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        let format_info = {
            let format_info = self.format_info.read().await;
            format_info.clone()
        };

        let id = format_info.id;

        if format_info.file_info.is_some() && id.is_some() {
            // Reuse the cached disk id only when the cached format check is fresh.
            if let Some(last_check) = format_info.last_check
                && last_check.unix_timestamp() + 1 >= OffsetDateTime::now_utc().unix_timestamp()
            {
                return Ok(id);
            }
        }

        let file_meta = match self.check_format_json().await {
            Ok(meta) => meta,
            Err(err) => {
                if matches!(err, DiskError::UnformattedDisk | DiskError::DiskNotFound) {
                    let mut format_info = self.format_info.write().await;
                    format_info.id = None;
                    format_info.data = Bytes::new();
                    format_info.file_info = None;
                    format_info.last_check = None;
                }
                return Err(err);
            }
        };

        if let Some(file_info) = &format_info.file_info
            && super::fs::same_file(&file_meta, file_info)
        {
            let mut format_info = self.format_info.write().await;
            format_info.last_check = Some(OffsetDateTime::now_utc());
            drop(format_info);

            return Ok(id);
        }

        debug!("get_disk_id: read format.json");

        let b = fs::read(&self.format_path).await.map_err(to_unformatted_disk_error)?;

        let fm = FormatV3::try_from(b.as_slice()).map_err(|e| {
            warn!(
                event = EVENT_DISK_LOCAL_FORMAT_DECODE_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                error = ?e,
                "Disk local format decode failed"
            );
            DiskError::CorruptedBackend
        })?;

        let (m, n) = fm.find_disk_index_by_disk_id(fm.erasure.this)?;

        let disk_id = fm.erasure.this;

        if m as i32 != self.endpoint.set_idx || n as i32 != self.endpoint.disk_idx {
            return Err(DiskError::InconsistentDisk);
        }

        let mut format_info = self.format_info.write().await;
        format_info.id = Some(disk_id);
        format_info.file_info = Some(file_meta);
        format_info.data = b.into();
        format_info.last_check = Some(OffsetDateTime::now_utc());
        drop(format_info);

        Ok(Some(disk_id))
    }

    async fn set_disk_id(&self, _id: Option<Uuid>) -> Result<()> {
        // No setup is required locally
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        if volume == RUSTFS_META_BUCKET && path == super::FORMAT_CONFIG_FILE {
            let format_info = self.format_info.read().await;
            if !format_info.data.is_empty() {
                return Ok(format_info.data.clone());
            }
        }

        let p = self.get_object_path(volume, path)?;

        let (data, _) = read_file_all(&p).await?;

        Ok(data)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        self.write_all_public(volume, path, data).await
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume)
            && let Err(e) = access(&volume_dir).await
        {
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        let file_path = self.get_object_path(volume, path)?;

        check_path_length(file_path.to_string_lossy().to_string().as_str())?;

        self.delete_file(&volume_dir, &file_path, opt.recursive, opt.immediate)
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume)
            && let Err(e) = access(&volume_dir).await
        {
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        let mut resp = CheckPartsResp {
            results: vec![0; fi.parts.len()],
        };

        let erasure = &fi.erasure;
        for (i, part) in fi.parts.iter().enumerate() {
            let checksum_info = erasure.get_checksum_info(part.number);
            let checksum_algo =
                if fi.uses_legacy_checksum && checksum_info.algorithm == rustfs_utils::HashAlgorithm::HighwayHash256S {
                    rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
                } else {
                    checksum_info.algorithm
                };
            let part_path = self.get_object_path(
                volume,
                path_join_buf(&[
                    path,
                    &fi.data_dir.map_or_else(|| "".to_string(), |dir| dir.to_string()),
                    &format!("part.{}", part.number),
                ])
                .as_str(),
            )?;
            let err = self
                .bitrot_verify(
                    &part_path,
                    erasure.shard_file_size(part.size as i64) as usize,
                    checksum_algo,
                    erasure.shard_size(),
                )
                .await
                .err();
            resp.results[i] = conv_part_err_to_int(&err);
            if resp.results[i] == CHECK_PART_UNKNOWN
                && let Some(err) = err
            {
                error!(
                    event = EVENT_DISK_LOCAL_CHECK_PARTS,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    path = ?part_path,
                    part_number = part.number,
                    state = "bitrot_verify_failed",
                    error = ?err,
                    "Disk local check_parts state changed"
                );
                if err == DiskError::FileAccessDenied {
                    continue;
                }
                info!(
                    event = EVENT_DISK_LOCAL_CHECK_PARTS,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    endpoint = %self.endpoint,
                    path = ?part_path,
                    part_number = part.number,
                    state = "unknown",
                    "Disk local check_parts state changed"
                );
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(skip(self))]
    async fn read_parts(&self, bucket: &str, paths: &[String]) -> Result<Vec<ObjectPartInfo>> {
        let volume_dir = self.get_bucket_path(bucket)?;

        let mut ret = vec![ObjectPartInfo::default(); paths.len()];

        for (i, path_str) in paths.iter().enumerate() {
            let path = Path::new(path_str);
            let file_name = path.file_name().and_then(|v| v.to_str()).unwrap_or_default();
            let num = file_name
                .strip_prefix("part.")
                .and_then(|v| v.strip_suffix(".meta"))
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or_default();

            if let Err(err) = access(
                self.get_object_path(
                    bucket,
                    path_join_buf(&[
                        path.parent().unwrap_or_else(|| Path::new("")).to_string_lossy().as_ref(),
                        &format!("part.{num}"),
                    ])
                    .as_str(),
                )?,
            )
            .await
            {
                ret[i] = ObjectPartInfo {
                    number: num,
                    error: Some(err.to_string()),
                    ..Default::default()
                };
                continue;
            }

            let data = match self
                .read_all_data(bucket, volume_dir.clone(), self.get_object_path(bucket, path.to_string_lossy().as_ref())?)
                .await
            {
                Ok(data) => data,
                Err(err) => {
                    ret[i] = ObjectPartInfo {
                        number: num,
                        error: Some(err.to_string()),
                        ..Default::default()
                    };
                    continue;
                }
            };

            match ObjectPartInfo::unmarshal(&data) {
                Ok(meta) => {
                    ret[i] = meta;
                }
                Err(err) => {
                    ret[i] = ObjectPartInfo {
                        number: num,
                        error: Some(err.to_string()),
                        ..Default::default()
                    };
                }
            };
        }

        Ok(ret)
    }
    #[tracing::instrument(skip(self))]
    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        let volume_dir = self.get_bucket_path(volume)?;
        let file_path = self.get_object_path(volume, path)?;
        check_path_length(file_path.to_string_lossy().as_ref())?;
        let mut resp = CheckPartsResp {
            results: vec![0; fi.parts.len()],
        };

        for (i, part) in fi.parts.iter().enumerate() {
            let part_path = self.get_object_path(
                volume,
                path_join_buf(&[
                    path,
                    &fi.data_dir.map_or_else(|| "".to_string(), |dir| dir.to_string()),
                    &format!("part.{}", part.number),
                ])
                .as_str(),
            )?;

            debug!(
                event = EVENT_DISK_LOCAL_CHECK_PARTS,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                path = ?part_path,
                part_number = part.number,
                state = "checking",
                "Disk local check_parts state changed"
            );

            match lstat(&part_path).await {
                Ok(st) => {
                    if st.is_dir() {
                        resp.results[i] = CHECK_PART_FILE_NOT_FOUND;
                        continue;
                    }
                    if (st.len() as i64) < fi.erasure.shard_file_size(part.size as i64) {
                        resp.results[i] = CHECK_PART_FILE_CORRUPT;
                        continue;
                    }

                    resp.results[i] = CHECK_PART_SUCCESS;
                }
                Err(err) => {
                    debug!(
                        event = EVENT_DISK_LOCAL_CHECK_PARTS,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = ?part_path,
                        part_number = part.number,
                        state = "part_stat_failed",
                        error = ?err,
                        "Disk local check_parts state changed"
                    );

                    let e: DiskError = to_file_error(err).into();

                    if e == DiskError::FileNotFound {
                        if !skip_access_checks(volume)
                            && let Err(err) = access(&volume_dir).await
                            && err.kind() == ErrorKind::NotFound
                        {
                            resp.results[i] = CHECK_PART_VOLUME_NOT_FOUND;
                            continue;
                        }
                        resp.results[i] = CHECK_PART_FILE_NOT_FOUND;
                    } else {
                        error!(
                            event = EVENT_DISK_LOCAL_CHECK_PARTS,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                            path = ?file_path,
                            part_number = part.number,
                            state = "file_stat_failed",
                            error = ?e,
                            "Disk local check_parts state changed"
                        );
                    }
                    continue;
                }
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Bytes) -> Result<()> {
        let src_volume_dir = self.get_bucket_path(src_volume)?;
        let dst_volume_dir = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(src_volume) {
            super::fs::access_std(&src_volume_dir).map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?
        }
        if !skip_access_checks(dst_volume) {
            super::fs::access_std(&dst_volume_dir).map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?
        }

        let src_is_dir = has_suffix(src_path, SLASH_SEPARATOR);
        let dst_is_dir = has_suffix(dst_path, SLASH_SEPARATOR);

        if !src_is_dir && dst_is_dir || src_is_dir && !dst_is_dir {
            warn!(
                event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                reason = "src_dst_type_mismatch",
                src_is_dir,
                dst_is_dir,
                "Disk local rename rejected"
            );
            return Err(DiskError::FileAccessDenied);
        }

        let src_file_path = self.get_object_path(src_volume, src_path)?;
        let dst_file_path = self.get_object_path(dst_volume, dst_path)?;

        // warn!("rename_part src_file_path:{:?}, dst_file_path:{:?}", &src_file_path, &dst_file_path);

        check_path_length(src_file_path.to_string_lossy().as_ref())?;
        check_path_length(dst_file_path.to_string_lossy().as_ref())?;

        if src_is_dir {
            let meta_op = match lstat_std(&src_file_path).map_err(|e| to_file_error(e).into()) {
                Ok(meta) => Some(meta),
                Err(e) => {
                    return Err(e);
                }
            };

            if let Some(meta) = meta_op
                && !meta.is_dir()
            {
                warn!(
                    event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    reason = "src_expected_dir_missing",
                    path = ?src_file_path,
                    "Disk local rename rejected"
                );
                return Err(DiskError::FileAccessDenied);
            }

            remove_std(&dst_file_path).map_err(to_file_error)?;
        } else {
            let meta = lstat_std(&src_file_path).map_err(|e| -> DiskError { to_file_error(e).into() })?;
            if meta.is_dir() {
                warn!(
                    event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    reason = "src_unexpected_dir",
                    path = ?src_file_path,
                    "Disk local rename rejected"
                );
                return Err(DiskError::FileAccessDenied);
            }
        }

        // UploadPart is acknowledged once this rename lands, so the part data and
        // its directory entry must be durable before we return.
        let sync = drive_sync_enabled();
        if sync && !src_is_dir {
            let src = src_file_path.clone();
            tokio::task::spawn_blocking(move || std::fs::File::open(&src)?.sync_data())
                .await
                .map_err(DiskError::from)?
                .map_err(to_file_error)?;
        }

        rename_all(&src_file_path, &dst_file_path, &dst_volume_dir).await?;

        if sync && let Some(parent) = dst_file_path.parent() {
            os::fsync_dir(parent).await.map_err(to_file_error)?;
        }

        let dst_meta = lstat_std(&dst_file_path).map_err(|e| -> DiskError { to_file_error(e).into() })?;
        if src_is_dir != dst_meta.is_dir() {
            warn!(
                event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                reason = "dst_type_changed_after_rename",
                path = ?dst_file_path,
                "Disk local rename rejected"
            );
            return Err(DiskError::FileAccessDenied);
        }

        self.write_all(dst_volume, format!("{dst_path}.meta").as_str(), meta).await?;

        if let Some(parent) = src_file_path.parent() {
            self.delete_file(&src_volume_dir, &parent.to_path_buf(), false, false).await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        let src_volume_dir = self.get_bucket_path(src_volume)?;
        let dst_volume_dir = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(src_volume) {
            access(&src_volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }
        if !skip_access_checks(dst_volume) {
            access(&dst_volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        let src_is_dir = has_suffix(src_path, SLASH_SEPARATOR);
        let dst_is_dir = has_suffix(dst_path, SLASH_SEPARATOR);
        if (dst_is_dir || src_is_dir) && (!dst_is_dir || !src_is_dir) {
            return Err(Error::from(DiskError::FileAccessDenied));
        }

        let src_file_path = self.get_object_path(src_volume, src_path)?;
        check_path_length(src_file_path.to_string_lossy().as_ref())?;

        let dst_file_path = self.get_object_path(dst_volume, dst_path)?;
        check_path_length(dst_file_path.to_string_lossy().as_ref())?;

        if src_is_dir {
            let meta_op = match lstat(&src_file_path).await {
                Ok(meta) => Some(meta),
                Err(e) => {
                    let e: DiskError = to_file_error(e).into();
                    if e != DiskError::FileNotFound {
                        return Err(e);
                    } else {
                        None
                    }
                }
            };

            if let Some(meta) = meta_op
                && !meta.is_dir()
            {
                return Err(DiskError::FileAccessDenied);
            }

            remove(&dst_file_path).await.map_err(to_file_error)?;
        }

        rename_all(&src_file_path, &dst_file_path, &dst_volume_dir).await?;

        if let Some(parent) = src_file_path.parent() {
            let _ = self.delete_file(&src_volume_dir, &parent.to_path_buf(), false, false).await;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, _file_size: i64) -> Result<FileWriter> {
        if !origvolume.is_empty() {
            let origvolume_dir = self.get_bucket_path(origvolume)?;
            if !skip_access_checks(origvolume) {
                access(origvolume_dir)
                    .await
                    .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
            }
        }

        self.io_backend
            .open_write(volume, path, WriteMode::Truncate { size_hint: _file_size })
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    // async fn append_file(&self, volume: &str, path: &str, mut r: DuplexStream) -> Result<File> {
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        self.io_backend.open_write(volume, path, WriteMode::Append).await
    }

    // TODO: io verifier
    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        self.io_backend.open_full_read(volume, path).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader> {
        self.io_backend.open_read_stream(volume, path, offset, length).await
    }

    /// File read using mmap-then-copy on Unix or efficient read on non-Unix.
    // SAFETY: Unix unsafe calls in this function only query page size and mmap
    // a read-only file region after bounds and alignment are validated.
    #[allow(unsafe_code)]
    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file_mmap_copy(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<Bytes> {
        self.read_file_mmap_copy_with_metrics(volume, path, offset, length, None)
            .await
    }

    /// File read using mmap-then-copy on Unix or efficient read on non-Unix.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file_mmap_copy_with_metrics(
        &self,
        volume: &str,
        path: &str,
        offset: usize,
        length: usize,
        metrics: Option<MmapCopyStageMetrics>,
    ) -> Result<Bytes> {
        self.io_backend.pread_bytes(volume, path, offset, length, metrics).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>> {
        if !origvolume.is_empty() {
            let origvolume_dir = self.get_bucket_path(origvolume)?;
            if !skip_access_checks(origvolume)
                && let Err(e) = access(origvolume_dir).await
            {
                return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
            }
        }

        let volume_dir = self.get_bucket_path(volume)?;
        let dir_path_abs = self.get_object_path(volume, dir_path.trim_start_matches(SLASH_SEPARATOR))?;

        let entries = match os::read_dir(&dir_path_abs, count).await {
            Ok(res) => res,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound
                    && !skip_access_checks(volume)
                    && let Err(e) = access(&volume_dir).await
                {
                    return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
                }

                return Err(to_file_error(e).into());
            }
        };

        Ok(entries)
    }

    // FIXME: TODO: io.writer TODO cancel
    #[tracing::instrument(level = "debug", skip(self, wr))]
    async fn walk_dir<W: AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        self.wait_for_startup_cleanup().await;

        let volume_dir = self.get_bucket_path(&opts.bucket)?;

        if !skip_access_checks(&opts.bucket)
            && let Err(e) = access(&volume_dir).await
        {
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        let mut wr = wr;

        let mut out = MetacacheWriter::new(&mut wr);

        let mut objs_returned = 0;

        let mut skip_current_dir_object = false;
        let mut multipart_dir_to_skip: HashSet<String> = HashSet::new();
        if opts.base_dir.ends_with(SLASH_SEPARATOR) {
            if let Ok(data) = self
                .read_metadata(
                    &opts.bucket,
                    path_join_buf(&[
                        format!("{}{}", opts.base_dir.trim_end_matches(SLASH_SEPARATOR), GLOBAL_DIR_SUFFIX).as_str(),
                        STORAGE_FORMAT_FILE,
                    ])
                    .as_str(),
                )
                .await
            {
                let meta = MetaCacheEntry {
                    name: opts.base_dir.clone(),
                    metadata: data.to_vec(),
                    ..Default::default()
                };
                out.write_obj(&meta).await?;
                objs_returned += 1;
            } else {
                let fpath =
                    self.get_object_path(&opts.bucket, path_join_buf(&[opts.base_dir.as_str(), STORAGE_FORMAT_FILE]).as_str())?;

                if let Ok(meta) = tokio::fs::metadata(&fpath).await
                    && meta.is_file()
                {
                    skip_current_dir_object = true;
                    if let Ok(meta_bytes) = self
                        .read_metadata(
                            opts.bucket.as_str(),
                            path_join_buf(&[opts.base_dir.as_str(), STORAGE_FORMAT_FILE]).as_str(),
                        )
                        .await
                        && let Ok(file_meta) = FileMeta::load(&meta_bytes)
                        && let Ok(data_dirs) = file_meta.get_data_dirs()
                    {
                        for data_dir in data_dirs.iter().flatten() {
                            multipart_dir_to_skip.insert(data_dir.to_string());
                        }
                    }
                }
            }
        }

        self.scan_dir(
            opts.base_dir.clone(),
            opts.filter_prefix.clone().unwrap_or_default(),
            &opts,
            &mut out,
            &mut objs_returned,
            skip_current_dir_object,
            if multipart_dir_to_skip.is_empty() {
                None
            } else {
                Some(multipart_dir_to_skip)
            },
        )
        .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, fi))]
    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        let src_volume_dir = self.get_bucket_path(src_volume)?;
        if !skip_access_checks(src_volume)
            && let Err(e) = super::fs::access_std(&src_volume_dir)
        {
            info!(
                event = EVENT_DISK_LOCAL_ACCESS_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                path = ?src_volume_dir,
                operation = "rename_data_src_access",
                error = %e,
                "Disk local access check failed"
            );
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        let dst_volume_dir = self.get_bucket_path(dst_volume)?;
        if !skip_access_checks(dst_volume)
            && let Err(e) = super::fs::access_std(&dst_volume_dir)
        {
            info!(
                event = EVENT_DISK_LOCAL_ACCESS_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                path = ?dst_volume_dir,
                operation = "rename_data_dst_access",
                error = %e,
                "Disk local access check failed"
            );
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        // xl.meta path
        let src_file_path = self.get_object_path(src_volume, format!("{}/{}", &src_path, STORAGE_FORMAT_FILE).as_str())?;
        let dst_file_path = self.get_object_path(dst_volume, format!("{}/{}", &dst_path, STORAGE_FORMAT_FILE).as_str())?;

        // data_dir path
        let has_data_dir_path = {
            let has_data_dir = {
                if !fi.is_remote() {
                    fi.data_dir
                        .map(|dir| rustfs_utils::path::retain_slash(dir.to_string().as_str()))
                } else {
                    None
                }
            };

            if let Some(data_dir) = has_data_dir {
                let src_data_path = self.get_object_path(
                    src_volume,
                    rustfs_utils::path::retain_slash(format!("{}/{}", &src_path, data_dir).as_str()).as_str(),
                )?;
                let dst_data_path = self.get_object_path(
                    dst_volume,
                    rustfs_utils::path::retain_slash(format!("{}/{}", &dst_path, data_dir).as_str()).as_str(),
                )?;

                Some((src_data_path, dst_data_path))
            } else {
                None
            }
        };

        check_path_length(src_file_path.to_string_lossy().to_string().as_str())?;
        check_path_length(dst_file_path.to_string_lossy().to_string().as_str())?;

        let no_inline = fi.data.is_none() && fi.size > 0;

        if no_inline {
            // Non-inline: read xl.meta, parse, write, rename data dir, rename xl.meta
            let has_dst_buf = match super::fs::read_file(&dst_file_path).await {
                Ok(res) => Some(res),
                Err(e) => {
                    let e: DiskError = to_file_error(e).into();
                    if e != DiskError::FileNotFound {
                        return Err(e);
                    }
                    None
                }
            };

            let mut xlmeta = FileMeta::new();
            if let Some(dst_buf) = has_dst_buf.as_ref()
                && FileMeta::is_xl2_v1_format(dst_buf)
                && let Ok(nmeta) = FileMeta::load(dst_buf)
            {
                xlmeta = nmeta
            }

            let mut skip_parent = dst_volume_dir.clone();
            if has_dst_buf.as_ref().is_some()
                && let Some(parent) = dst_file_path.parent()
            {
                skip_parent = parent.to_path_buf();
            }

            let version_id = fi.version_id.unwrap_or_default();
            let has_old_data_dir = xlmeta.find_unshared_data_dir_for_version(Some(version_id));
            if let Some(old_data_dir) = has_old_data_dir.as_ref() {
                let _ = xlmeta.data.remove_two(version_id, *old_data_dir);
            }
            xlmeta.add_version(fi)?;
            let new_dst_buf = xlmeta.marshal_msg()?;

            let src_file_parent = src_file_path.parent().unwrap_or(src_volume_dir.as_path());
            self.write_all_private(
                src_volume,
                &format!("{}/{}", &src_path, STORAGE_FORMAT_FILE),
                new_dst_buf.into(),
                true,
                src_file_parent,
            )
            .await?;

            // Make shard files durable before the commit rename: once rename_data
            // succeeds the write is acknowledged, so data must not live only in the
            // page cache. Multipart parts were already synced during rename_part, so
            // their fdatasync here is a cheap no-op. A missing source dir is left for
            // the rename below to report through the existing rollback path.
            if drive_sync_enabled()
                && let Some((src_data_path, _)) = has_data_dir_path.as_ref()
                && let Err(err) = os::sync_dir_files(src_data_path).await
                && err.kind() != ErrorKind::NotFound
            {
                return Err(to_file_error(err).into());
            }

            if let Some((src_data_path, dst_data_path)) = has_data_dir_path.as_ref()
                && let Err(err) = rename_all(src_data_path, dst_data_path, &skip_parent).await
            {
                let _ = self.delete_file(&dst_volume_dir, dst_data_path, false, false).await;
                info!(
                    event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    reason = "rename_all_data_path_failed",
                    src_path = ?src_data_path,
                    dst_path = ?dst_data_path,
                    error = ?err,
                    "Disk local rename flow failed"
                );
                return Err(err);
            }

            if should_fail_before_old_metadata_backup(dst_path) {
                if let Some((_, dst_data_path)) = has_data_dir_path.as_ref() {
                    let _ = self.delete_file(&dst_volume_dir, dst_data_path, false, false).await;
                }
                info!(
                    event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    reason = "test_fail_before_old_metadata_backup",
                    "Disk local rename flow failed before metadata commit"
                );
                return Err(DiskError::Unexpected);
            }

            if let Some(old_data_dir) = has_old_data_dir
                && let Some(dst_buf) = has_dst_buf.as_ref()
                && let Err(err) = self
                    .write_all_private(
                        dst_volume,
                        &format!("{}/{}/{}", &dst_path, &old_data_dir, STORAGE_FORMAT_FILE_BACKUP),
                        dst_buf.clone().into(),
                        true,
                        &skip_parent,
                    )
                    .await
            {
                if let Some((_, dst_data_path)) = has_data_dir_path.as_ref() {
                    let _ = self.delete_file(&dst_volume_dir, dst_data_path, false, false).await;
                }
                info!(
                    event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    reason = "write_old_metadata_backup_failed",
                    error = ?err,
                    "Disk local rename flow failed"
                );
                return Err(err);
            }

            if let Err(err) = rename_all(&src_file_path, &dst_file_path, &skip_parent).await {
                if let Some((_, dst_data_path)) = has_data_dir_path.as_ref() {
                    let _ = self.delete_file(&dst_volume_dir, dst_data_path, false, false).await;
                }
                info!(
                    event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    reason = "rename_all_metadata_failed",
                    src_path = ?src_file_path,
                    dst_path = ?dst_file_path,
                    error = ?err,
                    "Disk local rename flow failed"
                );
                return Err(err);
            }

            // Persist the directory entries for both the data dir and xl.meta renames;
            // without this the commit itself can vanish on power loss.
            if drive_sync_enabled()
                && let Some(parent) = dst_file_path.parent()
                && let Err(err) = os::fsync_dir(parent).await
            {
                return Err(to_file_error(err).into());
            }

            if let Some(src_file_path_parent) = src_file_path.parent() {
                if src_volume != super::RUSTFS_META_MULTIPART_BUCKET {
                    let _ = remove_std(src_file_path_parent);
                } else {
                    let _ = self
                        .delete_file(&dst_volume_dir, &src_file_path_parent.to_path_buf(), true, false)
                        .await;
                }
            }

            Ok(RenameDataResp {
                old_data_dir: has_old_data_dir,
                sign: None,
            })
        } else {
            // Inline: merge read + parse + write + rename into single spawn_blocking
            let src = src_file_path.clone();
            let dst = dst_file_path.clone();
            let cleanup_path = if src_volume == super::RUSTFS_META_MULTIPART_BUCKET {
                src_file_path.parent().map(|p| p.to_path_buf())
            } else {
                None
            };

            let (old_data_dir, _dst_buf) = tokio::task::spawn_blocking(move || {
                // Read existing xl.meta
                let has_dst_buf = match std::fs::read(&dst) {
                    Ok(buf) => Some(Bytes::from(buf)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => return Err(to_file_error(e)),
                };

                let mut xlmeta = FileMeta::new();
                if let Some(ref buf) = has_dst_buf
                    && FileMeta::is_xl2_v1_format(buf)
                    && let Ok(nmeta) = FileMeta::load(buf)
                {
                    xlmeta = nmeta
                }

                let version_id = fi.version_id.unwrap_or_default();
                let old_data_dir = xlmeta.find_unshared_data_dir_for_version(Some(version_id));
                if let Some(d) = old_data_dir.as_ref() {
                    let _ = xlmeta.data.remove_two(version_id, *d);
                }
                xlmeta.add_version(fi)?;
                let new_buf = xlmeta.marshal_msg()?;

                // Write new xl.meta + rename
                if let Some(parent) = src.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let sync = drive_sync_enabled();
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&src)?;
                std::io::Write::write_all(&mut f, &new_buf)?;
                if sync {
                    f.sync_data()?;
                }
                if let Some(old_dir) = old_data_dir.as_ref()
                    && let Some(ref buf) = has_dst_buf
                    && let Some(dst_parent) = dst.parent()
                {
                    let old_path = dst_parent.join(old_dir.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);
                    let old_parent = old_path.parent().map(|p| p.to_path_buf());
                    if let Some(ref old_parent) = old_parent {
                        std::fs::create_dir_all(old_parent)?;
                    }
                    // This rollback backup is the sole restore source for a later
                    // undo_write when the set-level write quorum fails. Persist it as
                    // durably as the new xl.meta written above (and as the non-inline
                    // branch does): a bare std::fs::write leaves both the bytes and the
                    // new directory entry in the page cache, so a crash before a
                    // rollback could restore a lost or truncated backup.
                    let mut backup = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&old_path)
                        .map_err(to_file_error)?;
                    std::io::Write::write_all(&mut backup, buf).map_err(to_file_error)?;
                    if sync {
                        backup.sync_data().map_err(to_file_error)?;
                        if let Some(ref old_parent) = old_parent {
                            os::fsync_dir_std(old_parent).map_err(to_file_error)?;
                        }
                    }
                }

                match std::fs::rename(&src, &dst) {
                    Ok(()) => Ok(()),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound && !src.exists() => Ok(()),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        if let Some(parent) = dst.parent() {
                            std::fs::create_dir_all(parent)?;
                        }
                        std::fs::rename(&src, &dst).map_err(to_file_error)?;
                        Ok(())
                    }
                    Err(err) => Err(to_file_error(err)),
                }?;

                // Persist the commit rename's directory entry across power loss.
                if sync && let Some(dst_parent) = dst.parent() {
                    os::fsync_dir_std(dst_parent)?;
                }

                Ok::<(Option<uuid::Uuid>, Option<Bytes>), std::io::Error>((old_data_dir, has_dst_buf))
            })
            .await
            .map_err(DiskError::from)??;

            // Cleanup
            if let Some(ref cleanup) = cleanup_path {
                let _ = self.delete_file(&dst_volume_dir, cleanup, true, false).await;
            } else if let Some(parent) = src_file_path.parent() {
                let _ = remove_std(parent);
            }

            Ok(RenameDataResp {
                old_data_dir,
                sign: None,
            })
        }
    }

    #[tracing::instrument(skip(self))]
    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        for vol in volumes {
            if let Err(e) = self.make_volume(vol).await
                && e != DiskError::VolumeExists
            {
                error!(
                    event = EVENT_DISK_LOCAL_VOLUME_SETUP_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    volume = vol,
                    operation = "make_volumes",
                    error = %e,
                    "Disk local volume setup failed"
                );
                return Err(e);
            }
            // TODO: health check
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn make_volume(&self, volume: &str) -> Result<()> {
        if !Self::is_valid_volname(volume) {
            return Err(Error::other("Invalid arguments specified"));
        }

        let volume_dir = self.get_bucket_path(volume)?;

        if let Err(e) = access(&volume_dir).await {
            if e.kind() == ErrorKind::NotFound {
                os::make_dir_all(&volume_dir, self.root.as_path()).await?;
                return Ok(());
            }
            error!(
                event = EVENT_DISK_LOCAL_VOLUME_SETUP_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                volume,
                operation = "make_volume",
                error = %e,
                "Disk local volume setup failed"
            );
            return Err(to_volume_error(e).into());
        }

        Err(DiskError::VolumeExists)
    }

    #[tracing::instrument(skip(self))]
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let mut volumes = Vec::new();

        let entries = os::read_dir(&self.root, -1).await.map_err(to_volume_error)?;

        for entry in entries {
            if !has_suffix(&entry, SLASH_SEPARATOR) || !Self::is_valid_volname(clean(&entry).as_str()) {
                continue;
            }

            volumes.push(VolumeInfo {
                name: clean(&entry),
                created: None,
            });
        }

        Ok(volumes)
    }

    #[tracing::instrument(skip(self))]
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        let volume_dir = self.get_bucket_path(volume)?;
        let meta = lstat(&volume_dir).await.map_err(to_volume_error)?;

        let modtime = match meta.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => None,
        };

        Ok(VolumeInfo {
            name: volume.to_string(),
            created: modtime,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn delete_paths(&self, volume: &str, paths: &[String]) -> Result<()> {
        let volume_dir = self.get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            access(&volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        for path in paths.iter() {
            let file_path = self.get_object_path(volume, path)?;

            check_path_length(file_path.to_string_lossy().as_ref())?;

            self.move_to_trash(&file_path, false, false).await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        if !fi.metadata.is_empty() {
            let file_path = self.get_object_path(volume, path)?;

            check_path_length(file_path.to_string_lossy().as_ref())?;

            let buf = self
                .read_all(volume, format!("{}/{}", &path, STORAGE_FORMAT_FILE).as_str())
                .await
                .map_err(|e| {
                    if e == DiskError::FileNotFound && fi.version_id.is_some() {
                        DiskError::FileVersionNotFound
                    } else {
                        e
                    }
                })?;

            if !FileMeta::is_xl2_v1_format(buf.as_ref()) {
                return Err(DiskError::FileVersionNotFound);
            }

            let mut xl_meta = FileMeta::load(buf.as_ref())?;

            xl_meta.update_object_version_with_opts(fi, opts.replace_user_metadata)?;

            let wbuf = xl_meta.marshal_msg()?;

            return self
                .write_all_meta(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str(), &wbuf, !opts.no_persistence)
                .await;
        }

        Err(Error::other("Invalid Argument"))
    }

    #[tracing::instrument(skip(self))]
    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        let p = self.get_object_path(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str())?;

        let mut meta = FileMeta::new();
        if !fi.fresh {
            let (buf, _) = read_file_exists(&p).await?;
            if !buf.is_empty() {
                let _ = meta.unmarshal_msg(&buf).map_err(|_| {
                    meta = FileMeta::new();
                });
            }
        }

        meta.add_version(fi)?;

        let fm_data = meta.marshal_msg()?;

        // Atomic temp+rename: this path also rewrites live xl.meta (delete markers,
        // decommission), where an in-place truncate would expose torn metadata.
        self.write_all_meta(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str(), &fm_data, true)
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_version(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<FileInfo> {
        if !org_volume.is_empty() {
            let org_volume_path = self.get_bucket_path(org_volume)?;
            if !skip_access_checks(org_volume) {
                access(&org_volume_path)
                    .await
                    .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
            }
        }

        let file_path = self.get_object_path(volume, path)?;
        let volume_dir = self.get_bucket_path(volume)?;

        check_path_length(file_path.to_string_lossy().as_ref())?;

        let read_data = opts.read_data;

        let (data, _) = self
            .read_raw(volume, volume_dir.clone(), file_path, read_data)
            .await
            .map_err(|e| {
                if e == DiskError::FileNotFound && !version_id.is_empty() {
                    DiskError::FileVersionNotFound
                } else {
                    e
                }
            })?;

        let mut fi = get_file_info(
            &data,
            volume,
            path,
            version_id,
            FileInfoOpts {
                data: read_data,
                include_free_versions: opts.incl_free_versions,
            },
        )?;

        if opts.read_data {
            if fi.data.as_ref().is_some_and(|d| !d.is_empty()) || fi.size == 0 {
                if fi.inline_data() {
                    return Ok(fi);
                }

                if fi.size == 0 || fi.version_id.is_none_or(|v| v.is_nil()) {
                    fi.set_inline_data();
                    return Ok(fi);
                };
                if let Some(part) = fi.parts.first() {
                    let part_path = format!("part.{}", part.number);
                    let part_path = path_join_buf(&[
                        path,
                        fi.data_dir.map_or_else(|| "".to_string(), |dir| dir.to_string()).as_str(),
                        part_path.as_str(),
                    ]);
                    let part_path = self.get_object_path(volume, part_path.as_str())?;
                    if lstat(&part_path).await.is_err() {
                        fi.set_inline_data();
                        return Ok(fi);
                    }
                }

                fi.data = None;
            }

            let inline = fi.transition_status.is_empty() && fi.data_dir.is_some() && fi.parts.len() == 1;
            if inline && fi.shard_file_size(fi.parts[0].actual_size) < DEFAULT_INLINE_BLOCK as i64 {
                let part_path = path_join_buf(&[
                    path,
                    fi.data_dir.map_or_else(|| "".to_string(), |dir| dir.to_string()).as_str(),
                    format!("part.{}", fi.parts[0].number).as_str(),
                ]);
                let part_path = self.get_object_path(volume, part_path.as_str())?;

                let data = self.read_all_data(volume, volume_dir, part_path.clone()).await.map_err(|e| {
                    warn!(
                        event = EVENT_DISK_LOCAL_READ_VERSION_FALLBACK,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = ?part_path,
                        reason = "inline_data_read_failed",
                        error = %e,
                        "Disk local read_version fallback failed"
                    );
                    e
                })?;
                fi.data = Some(Bytes::from(data));
            }
        }

        Ok(fi)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        let file_path = self.get_object_path(volume, path)?;
        let file_dir = self.get_bucket_path(volume)?;

        let (buf, _) = self.read_raw(volume, file_dir, file_path, read_data).await?;

        Ok(RawFileInfo { buf })
    }

    #[tracing::instrument(skip(self))]
    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<()> {
        if path.starts_with(SLASH_SEPARATOR) {
            return self
                .delete(
                    volume,
                    path,
                    DeleteOptions {
                        recursive: false,
                        immediate: false,
                        ..Default::default()
                    },
                )
                .await;
        }

        let volume_dir = self.get_bucket_path(volume)?;

        let file_path = self.get_object_path(volume, path)?;

        check_path_length(file_path.to_string_lossy().as_ref())?;

        let xl_path = path_join(&[file_path.as_path(), Path::new(STORAGE_FORMAT_FILE)]);
        let buf = match self.read_all_data(volume, &volume_dir, &xl_path).await {
            Ok(res) => res,
            Err(err) => {
                if err != DiskError::FileNotFound {
                    return Err(err);
                }

                if fi.deleted && force_del_marker {
                    return self.write_metadata("", volume, path, fi).await;
                }

                return if fi.version_id.is_some() {
                    Err(DiskError::FileVersionNotFound)
                } else {
                    Err(DiskError::FileNotFound)
                };
            }
        };

        let mut meta = FileMeta::load(&buf)?;
        let old_dir = meta.delete_version(&fi)?;

        if let Some(uuid) = old_dir {
            let vid = fi.version_id.unwrap_or_default();
            let _ = meta.data.remove(vec![vid, uuid])?;

            let old_path = path_join(&[file_path.as_path(), Path::new(uuid.to_string().as_str())]);
            check_path_length(old_path.to_string_lossy().as_ref())?;

            if let Err(err) = self.move_to_trash(&old_path, true, false).await
                && err != DiskError::FileNotFound
                && err != DiskError::VolumeNotFound
            {
                return Err(err);
            }
        }

        if let Some(old_data_dir) = opts.old_data_dir
            && opts.undo_write
        {
            let src_path = path_join(&[
                file_path.as_path(),
                Path::new(format!("{old_data_dir}{SLASH_SEPARATOR}{STORAGE_FORMAT_FILE_BACKUP}").as_str()),
            ]);
            let dst_path = path_join(&[file_path.as_path(), Path::new(STORAGE_FORMAT_FILE)]);
            return rename_all(&src_path, &dst_path, file_path).await;
        }

        if !meta.versions.is_empty() {
            let buf = meta.marshal_msg()?;
            return self
                .write_all_meta(volume, format!("{path}{SLASH_SEPARATOR}{STORAGE_FORMAT_FILE}").as_str(), &buf, true)
                .await;
        }

        self.delete_file(&volume_dir, &xl_path, true, false).await
    }
    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_versions(&self, volume: &str, versions: Vec<FileInfoVersions>, _opts: DeleteOptions) -> Vec<Option<Error>> {
        let mut errs = Vec::with_capacity(versions.len());
        for _ in 0..versions.len() {
            errs.push(None);
        }

        for (i, ver) in versions.iter().enumerate() {
            if let Err(e) = self.delete_versions_internal(volume, ver.name.as_str(), &ver.versions).await {
                errs[i] = Some(e);
            } else {
                errs[i] = None;
            }
        }

        errs
    }

    #[tracing::instrument(skip(self))]
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        let mut results = Vec::new();
        let mut found = 0;

        for v in req.files.iter() {
            let fpath = self.get_object_path(&req.bucket, format!("{}/{}", &req.prefix, v).as_str())?;
            let mut res = ReadMultipleResp {
                bucket: req.bucket.clone(),
                prefix: req.prefix.clone(),
                file: v.clone(),
                ..Default::default()
            };

            // if req.metadata_only {}
            match read_file_all(&fpath).await {
                Ok((data, meta)) => {
                    found += 1;

                    if req.max_size > 0 && data.len() > req.max_size {
                        res.exists = true;
                        res.error = format!("max size ({}) exceeded: {}", req.max_size, data.len());
                        results.push(res);
                        break;
                    }

                    res.exists = true;
                    res.data = data.into();
                    res.mod_time = match meta.modified() {
                        Ok(md) => Some(OffsetDateTime::from(md)),
                        Err(_) => {
                            warn!(
                                event = EVENT_DISK_LOCAL_FORMAT_DECODE_FAILED,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                                reason = "modified_time_unsupported",
                                "Disk local modified time is unsupported on this platform"
                            );
                            None
                        }
                    };
                    results.push(res);

                    if req.max_results > 0 && found >= req.max_results {
                        break;
                    }
                }
                Err(e) => {
                    if e != DiskError::FileNotFound && e != DiskError::VolumeNotFound {
                        res.exists = true;
                        res.error = e.to_string();
                    }

                    if req.abort404 && !res.exists {
                        results.push(res);
                        break;
                    }

                    results.push(res);
                }
            }
        }

        Ok(results)
    }

    #[tracing::instrument(skip(self))]
    async fn delete_volume(&self, volume: &str, force_delete: bool) -> Result<()> {
        let p = self.get_bucket_path(volume)?;

        // Non-force is non-recursive: `remove_dir` (rmdir) fails atomically with
        // `DirectoryNotEmpty` -> VolumeNotEmpty if the bucket still holds any
        // object data, so a misclassified "dangling" bucket on the heal path
        // (or a non-force S3 DeleteBucket on a populated bucket) can never be
        // recursively wiped. Only an explicit `force_delete` (e.g. S3 force
        // bucket delete) removes recursively. Mirrors MinIO's
        // xlStorage.DeleteVol (Remove vs RemoveAll). (backlog#799 B1)
        let res = if force_delete {
            fs::remove_dir_all(&p).await
        } else {
            fs::remove_dir(&p).await
        };

        if let Err(err) = res {
            let e: DiskError = to_volume_error(err).into();
            if e != DiskError::VolumeNotFound {
                return Err(e);
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn disk_info(&self, _: &DiskInfoOptions) -> Result<DiskInfo> {
        let mut info = Cache::get(self.disk_info_cache.clone()).await?;
        info.nr_requests = self.nrrequests;
        info.rotational = self.rotational;
        info.mount_path = self.path().to_str().expect("operation should succeed").to_string();
        info.endpoint = self.endpoint.to_string();
        info.scanning = self.scanning.load(Ordering::Acquire) == 1;

        if info.id.is_none() {
            info.id = self.get_disk_id().await.unwrap_or(None);
        }

        Ok(info)
    }
    #[tracing::instrument(skip(self))]
    fn start_scan(&self) -> ScanGuard {
        self.scanning.fetch_add(1, Ordering::Release);
        ScanGuard(Arc::clone(&self.scanning))
    }

    #[tracing::instrument(skip(self))]
    async fn read_metadata(&self, volume: &str, path: &str) -> Result<Bytes> {
        let file_path = self.get_object_path(volume, path)?;
        let volume_dir = self.get_bucket_path(volume)?;
        let (data, _) = self.read_all_data_with_dmtime(volume, volume_dir, file_path).await?;
        Ok(data.into())
    }
}

async fn wait_for_startup_cleanup_signal(
    startup_cleanup_ready: &AtomicU32,
    startup_cleanup_notify: &Notify,
    wait_timeout: Duration,
) -> bool {
    if startup_cleanup_ready.load(Ordering::Acquire) != 0 {
        return true;
    }

    timeout(wait_timeout, async {
        loop {
            if startup_cleanup_ready.load(Ordering::Acquire) != 0 {
                return;
            }
            let notified = startup_cleanup_notify.notified();
            if startup_cleanup_ready.load(Ordering::Acquire) != 0 {
                return;
            }
            notified.await;
        }
    })
    .await
    .is_ok()
}

#[tracing::instrument]
async fn get_disk_info(drive_path: PathBuf) -> Result<(rustfs_utils::os::DiskInfo, bool)> {
    let drive_path = drive_path.to_string_lossy().to_string();
    check_path_length(&drive_path)?;

    let disk_info = get_info(&drive_path).inspect_err(|err| {
        log_startup_disk_io_error("get_disk_info_stat", Path::new(&drive_path), err);
    })?;
    let root_drive = if let Some(root_disk_threshold) = runtime_sources::root_disk_threshold_for_erasure_disk().await {
        if root_disk_threshold > 0 {
            disk_info.total <= root_disk_threshold
        } else {
            is_root_disk(&drive_path, SLASH_SEPARATOR).unwrap_or_default()
        }
    } else {
        false
    };

    Ok((disk_info, root_drive))
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncReadExt, ReadBuf};

    fn test_file_info(name: &str, version_id: Uuid, data_dir: Option<Uuid>, data: Option<Bytes>) -> FileInfo {
        let size = data
            .as_ref()
            .map(|data| i64::try_from(data.len()).expect("test data length should fit i64"))
            .unwrap_or(1);
        FileInfo {
            name: name.to_string(),
            version_id: Some(version_id),
            data_dir,
            data,
            size,
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        }
    }

    fn test_meta(fi: FileInfo) -> Vec<u8> {
        let mut meta = FileMeta::default();
        meta.add_version(fi).expect("test metadata should accept file info");
        meta.marshal_msg().expect("test metadata should encode")
    }

    async fn ensure_test_volume(disk: &LocalDisk, volume: &str) {
        match disk.make_volume(volume).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("test volume should be available: {err:?}"),
        }
    }

    #[tokio::test]
    async fn test_rename_data_writes_old_metadata_backup_before_non_inline_undo() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/object";
        let tmp_object = "tmp-write";
        let version_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("22222222-2222-2222-2222-222222222222").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("33333333-3333-3333-3333-333333333333").expect("new data dir should parse");

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let dst_object_dir = dir.path().join(bucket).join("dir/object");
        fs::create_dir_all(dst_object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old data dir should be created");
        fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), test_meta(old_fi))
            .await
            .expect("old metadata should be written");

        let tmp_data_dir = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(new_data_dir.to_string());
        fs::create_dir_all(&tmp_data_dir)
            .await
            .expect("new tmp data dir should be created");
        fs::write(tmp_data_dir.join("part.1"), b"new-data")
            .await
            .expect("new tmp data should be written");

        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        let resp = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("rename_data should commit");

        assert_eq!(resp.old_data_dir, Some(old_data_dir));
        assert!(
            dst_object_dir
                .join(old_data_dir.to_string())
                .join(STORAGE_FORMAT_FILE_BACKUP)
                .exists()
        );
        assert!(
            !dst_object_dir
                .join(old_data_dir.to_string())
                .join(STORAGE_FORMAT_FILE)
                .exists()
        );
    }

    #[tokio::test]
    async fn test_rename_data_writes_old_metadata_backup_for_inline_overwrite() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "inline-object";
        let tmp_object = "tmp-inline-write";
        let version_id = Uuid::parse_str("12121212-1212-1212-1212-121212121212").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("34343434-3434-3434-3434-343434343434").expect("old data dir should parse");

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let old_meta = test_meta(old_fi);
        let dst_object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(dst_object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old data dir should be created");
        fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), &old_meta)
            .await
            .expect("old metadata should be written");

        let tmp_object_dir = dir.path().join(RUSTFS_META_TMP_BUCKET).join(tmp_object);
        fs::create_dir_all(&tmp_object_dir)
            .await
            .expect("tmp object dir should be created");

        let new_fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"inline-new")));
        let resp = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("inline rename_data should commit");

        assert_eq!(resp.old_data_dir, Some(old_data_dir));
        let backup_path = dst_object_dir.join(old_data_dir.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);
        assert!(backup_path.exists());
        // The rollback backup must contain the previous metadata bytes verbatim so
        // that undo_write can restore the prior committed object; guards the inline
        // backup write against truncation/corruption regressions.
        assert_eq!(
            fs::read(&backup_path).await.expect("backup should be readable"),
            old_meta,
            "inline rollback backup must contain the previous metadata bytes verbatim"
        );
        assert!(
            !dst_object_dir
                .join(old_data_dir.to_string())
                .join(STORAGE_FORMAT_FILE)
                .exists()
        );
    }

    #[tokio::test]
    async fn test_delete_version_undo_restores_backup_to_object_root() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/object";
        let version_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("55555555-5555-5555-5555-555555555555").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("66666666-6666-6666-6666-666666666666").expect("new data dir should parse");

        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join("dir/object");
        fs::create_dir_all(object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old backup dir should be created");
        fs::create_dir_all(object_dir.join(new_data_dir.to_string()))
            .await
            .expect("new data dir should be created");

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let old_meta = test_meta(old_fi);
        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        fs::write(
            object_dir.join(old_data_dir.to_string()).join(STORAGE_FORMAT_FILE_BACKUP),
            old_meta.clone(),
        )
        .await
        .expect("old metadata backup should be written");
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), test_meta(new_fi.clone()))
            .await
            .expect("new metadata should be written");

        disk.delete_version(
            bucket,
            object,
            new_fi,
            false,
            DeleteOptions {
                undo_write: true,
                old_data_dir: Some(old_data_dir),
                ..Default::default()
            },
        )
        .await
        .expect("undo should restore old metadata");

        let restored_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("restored metadata should be readable");
        assert_eq!(restored_meta, old_meta);
        assert!(!object_dir.join("dir/object").join(STORAGE_FORMAT_FILE).exists());
    }

    #[tokio::test]
    async fn test_delete_version_undo_restores_backup_when_other_versions_remain() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/object";
        let version_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").expect("version id should parse");
        let other_version_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("55555555-5555-5555-5555-555555555555").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("66666666-6666-6666-6666-666666666666").expect("new data dir should parse");
        let other_data_dir = Uuid::parse_str("88888888-8888-8888-8888-888888888888").expect("other data dir should parse");

        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join("dir/object");
        fs::create_dir_all(object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old backup dir should be created");
        fs::create_dir_all(object_dir.join(new_data_dir.to_string()))
            .await
            .expect("new data dir should be created");

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let other_fi = test_file_info(object, other_version_id, Some(other_data_dir), None);
        let mut old_meta = FileMeta::default();
        old_meta
            .add_version(old_fi)
            .expect("old metadata should accept old file info");
        old_meta
            .add_version(other_fi.clone())
            .expect("old metadata should accept other file info");
        let old_meta = old_meta.marshal_msg().expect("old metadata should encode");

        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        let mut new_meta = FileMeta::default();
        new_meta
            .add_version(new_fi.clone())
            .expect("new metadata should accept new file info");
        new_meta
            .add_version(other_fi)
            .expect("new metadata should accept other file info");

        fs::write(
            object_dir.join(old_data_dir.to_string()).join(STORAGE_FORMAT_FILE_BACKUP),
            old_meta.clone(),
        )
        .await
        .expect("old metadata backup should be written");
        fs::write(
            object_dir.join(STORAGE_FORMAT_FILE),
            new_meta.marshal_msg().expect("new metadata should encode"),
        )
        .await
        .expect("new metadata should be written");

        disk.delete_version(
            bucket,
            object,
            new_fi,
            false,
            DeleteOptions {
                undo_write: true,
                old_data_dir: Some(old_data_dir),
                ..Default::default()
            },
        )
        .await
        .expect("undo should restore old metadata");

        let restored_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("restored metadata should be readable");
        assert_eq!(restored_meta, old_meta);
    }

    #[tokio::test]
    async fn test_rename_data_failure_before_metadata_commit_preserves_old_metadata() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "failpoint-object";
        let tmp_object = "tmp-object";
        let version_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").expect("version id should parse");

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let old_meta = test_meta(old_fi);
        let object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old data dir should be created");
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), old_meta.clone())
            .await
            .expect("old metadata should be written");

        let tmp_data_dir = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(new_data_dir.to_string());
        fs::create_dir_all(&tmp_data_dir)
            .await
            .expect("new tmp data dir should be created");
        fs::write(tmp_data_dir.join("part.1"), b"new-data")
            .await
            .expect("new tmp data should be written");

        set_rename_data_fail_before_old_metadata_backup(object);
        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        let result = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await;

        assert!(result.is_err());
        let current_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("old metadata should still be readable");
        assert_eq!(current_meta, old_meta);
        assert!(!object_dir.join("object").join(STORAGE_FORMAT_FILE).exists());
    }

    #[tokio::test]
    async fn test_skip_access_checks() {
        // let arr = Vec::new();

        let vols = [
            RUSTFS_META_TMP_DELETED_BUCKET,
            super::super::RUSTFS_META_TMP_BUCKET,
            super::super::RUSTFS_META_MULTIPART_BUCKET,
            RUSTFS_META_BUCKET,
        ];

        let paths: Vec<_> = vols.iter().map(|v| path_join(&[Path::new(v), Path::new("test")])).collect();

        for p in paths.iter() {
            assert!(skip_access_checks(p.to_str().expect("operation should succeed")));
        }
    }

    #[derive(Debug, Default)]
    struct PendingTestReader;

    impl AsyncRead for PendingTestReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    #[tokio::test(start_paused = true)]
    async fn local_read_timeout_reader_times_out_when_inner_stalls() {
        let mut reader = StallTimeoutReader::new(PendingTestReader, Duration::from_secs(10));
        let mut buf = [0; 1];

        let err = reader
            .read(&mut buf)
            .await
            .expect_err("stalled local reader should return a timeout error");

        assert_eq!(err.kind(), ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn test_get_disk_id_invalidates_cache_after_format_removal() {
        use crate::disk::FORMAT_CONFIG_FILE;
        use crate::disk::format::FormatV3;
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let mut endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        let meta_dir = dir.path().join(RUSTFS_META_BUCKET);
        fs::create_dir_all(&meta_dir).await.expect("meta dir should be creatable");
        let mut format = FormatV3::new(1, 1);
        format.erasure.this = format.erasure.sets[0][0];
        let format_json = format.to_json().expect("format should serialize");
        fs::write(meta_dir.join(FORMAT_CONFIG_FILE), format_json)
            .await
            .expect("format.json should be writable");

        let disk = LocalDisk::new(&endpoint, false)
            .await
            .expect("local disk should open after seeding format");

        let initial_id = disk.get_disk_id().await.expect("disk id lookup should succeed");
        assert!(initial_id.is_some(), "new disk should expose a disk id");

        fs::remove_file(&disk.format_path)
            .await
            .expect("format.json should be removable");

        tokio::time::sleep(Duration::from_secs(2)).await;

        let err = disk
            .get_disk_id()
            .await
            .expect_err("removed format.json should invalidate the cached disk id");
        assert!(matches!(err, DiskError::UnformattedDisk));

        let format_info = disk.format_info.read().await.clone();
        assert!(format_info.id.is_none(), "cached disk id should be cleared");
        assert!(format_info.data.is_empty(), "cached format bytes should be cleared");
        assert!(format_info.file_info.is_none(), "cached file metadata should be cleared");
        assert!(format_info.last_check.is_none(), "cached format timestamp should be cleared");
    }

    #[tokio::test]
    async fn cleanup_tmp_on_startup_moves_existing_tmp_and_recreates_trash() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let tmp = LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_BUCKET);
        let leftover = tmp.join("leftover").join("data");
        fs::create_dir_all(leftover.parent().expect("operation should succeed"))
            .await
            .expect("operation should succeed");
        fs::write(&leftover, b"temporary").await.expect("operation should succeed");

        LocalDisk::cleanup_tmp_on_startup(dir.path(), Arc::new(AtomicU32::new(0)), Arc::new(Notify::new()))
            .await
            .expect("operation should succeed");

        assert!(!tmp.join("leftover").exists());
        assert!(LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_DELETED_BUCKET).exists());
    }

    #[tokio::test]
    async fn cleanup_stale_tmp_objects_moves_expired_tmp_dirs_to_trash() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let tmp = LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_BUCKET);
        let stale = tmp.join("stale").join("data");
        let trash = LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_DELETED_BUCKET);
        fs::create_dir_all(stale.parent().expect("operation should succeed"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(&trash).await.expect("operation should succeed");
        fs::write(&stale, b"temporary").await.expect("operation should succeed");

        tokio::time::sleep(Duration::from_millis(2)).await;
        LocalDisk::cleanup_stale_tmp_objects_with_expiry(dir.path().to_path_buf(), Duration::ZERO)
            .await
            .expect("operation should succeed");

        assert!(!tmp.join("stale").exists());
        assert!(trash.exists());

        let mut entries = fs::read_dir(&trash).await.expect("operation should succeed");
        assert!(entries.next_entry().await.expect("operation should succeed").is_some());
    }

    #[tokio::test]
    async fn cleanup_stale_tmp_objects_keeps_fresh_dirs_and_regular_files() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let tmp = LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_BUCKET);
        let fresh_dir = tmp.join("fresh").join("data");
        let regular_file = tmp.join("note.txt");
        let trash = LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_DELETED_BUCKET);

        fs::create_dir_all(fresh_dir.parent().expect("operation should succeed"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(&trash).await.expect("operation should succeed");
        fs::write(&fresh_dir, b"temporary").await.expect("operation should succeed");
        fs::write(&regular_file, b"keep").await.expect("operation should succeed");

        LocalDisk::cleanup_stale_tmp_objects_with_expiry(dir.path().to_path_buf(), Duration::from_secs(60))
            .await
            .expect("operation should succeed");

        assert!(tmp.join("fresh").exists());
        assert!(regular_file.exists());

        let mut entries = fs::read_dir(&trash).await.expect("operation should succeed");
        assert!(entries.next_entry().await.expect("operation should succeed").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_loop_interval_does_not_tick_immediately() {
        let start_at = tokio::time::Instant::now() + DELETED_OBJECTS_CLEANUP_INTERVAL;
        let mut interval = interval_at(start_at, DELETED_OBJECTS_CLEANUP_INTERVAL);

        assert!(tokio::time::timeout(Duration::from_secs(1), interval.tick()).await.is_err());

        tokio::time::advance(DELETED_OBJECTS_CLEANUP_INTERVAL).await;
        interval.tick().await;
    }

    #[tokio::test(start_paused = true)]
    async fn startup_cleanup_barrier_waits_for_notification() {
        let ready = Arc::new(AtomicU32::new(0));
        let notify = Arc::new(Notify::new());

        let wait = tokio::spawn({
            let ready = ready.clone();
            let notify = notify.clone();
            async move { wait_for_startup_cleanup_signal(ready.as_ref(), notify.as_ref(), Duration::from_secs(2)).await }
        });

        tokio::task::yield_now().await;
        assert!(!wait.is_finished());

        ready.store(1, Ordering::Release);
        notify.notify_waiters();

        assert!(wait.await.expect("operation should succeed"));
    }

    #[tokio::test(start_paused = true)]
    async fn startup_cleanup_barrier_times_out() {
        let ready = Arc::new(AtomicU32::new(0));
        let notify = Arc::new(Notify::new());

        let wait = tokio::spawn({
            let ready = ready.clone();
            let notify = notify.clone();
            async move { wait_for_startup_cleanup_signal(ready.as_ref(), notify.as_ref(), Duration::from_secs(2)).await }
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;

        assert!(!wait.await.expect("operation should succeed"));
    }

    #[tokio::test]
    async fn test_scan_dir_includes_nested_object_dirs() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        fs::create_dir_all(bucket_dir.join("foo/bar/xyzzy"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(bucket_dir.join("quux/thud"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(bucket_dir.join("asdf"))
            .await
            .expect("operation should succeed");

        fs::write(bucket_dir.join("foo/bar/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");
        fs::write(bucket_dir.join("foo/bar/xyzzy/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");
        fs::write(bucket_dir.join("quux/thud/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");
        fs::write(bucket_dir.join("asdf/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let (reader, mut writer) = tokio::io::duplex(4096);
        let mut out = MetacacheWriter::new(&mut writer);
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: "".to_string(),
            recursive: true,
            ..Default::default()
        };
        let mut objs_returned = 0;

        disk.scan_dir("".to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
            .await
            .expect("operation should succeed");
        out.close().await.expect("operation should succeed");

        let mut reader = MetacacheReader::new(reader);
        let entries = reader.read_all().await.expect("operation should succeed");
        let names: Vec<String> = entries
            .into_iter()
            .filter(|entry| !entry.metadata.is_empty())
            .map(|entry| entry.name)
            .collect();

        assert!(names.contains(&"asdf".to_string()));
        assert!(names.contains(&"foo/bar".to_string()));
        assert!(names.contains(&"foo/bar/xyzzy".to_string()));
        assert!(names.contains(&"quux/thud".to_string()));
    }

    #[tokio::test]
    async fn test_scan_dir_deduplicates_explicit_dir_marker_recursion() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        fs::create_dir_all(bucket_dir.join("marker/file.txt"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(bucket_dir.join("marker/subdir/file.txt"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(bucket_dir.join(format!("marker/subdir{GLOBAL_DIR_SUFFIX}")))
            .await
            .expect("operation should succeed");

        fs::write(bucket_dir.join("marker/file.txt/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");
        fs::write(bucket_dir.join("marker/subdir/file.txt/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");
        fs::write(bucket_dir.join(format!("marker/subdir{GLOBAL_DIR_SUFFIX}/xl.meta")), b"meta")
            .await
            .expect("operation should succeed");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let (reader, mut writer) = tokio::io::duplex(4096);
        let mut out = MetacacheWriter::new(&mut writer);
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: "marker/".to_string(),
            recursive: true,
            ..Default::default()
        };
        let mut objs_returned = 0;

        disk.scan_dir("marker/".to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
            .await
            .expect("operation should succeed");
        out.close().await.expect("operation should succeed");

        let mut reader = MetacacheReader::new(reader);
        let entries = reader.read_all().await.expect("operation should succeed");
        let names: Vec<String> = entries
            .into_iter()
            .filter(|entry| !entry.metadata.is_empty())
            .map(|entry| entry.name)
            .collect();

        assert_eq!(names.iter().filter(|name| *name == "marker/subdir/file.txt").count(), 1);
        assert_eq!(names.iter().filter(|name| *name == "marker/subdir/").count(), 1);
        assert_eq!(names.iter().filter(|name| *name == "marker/file.txt").count(), 1);
    }

    #[tokio::test]
    async fn test_scan_dir_forward_to_repeated_prefix_component() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        for name in [
            "different/prefix/prefix/repo-0000",
            "different/prefix/prefix/repo-0001",
            "different/prefix/prefix/repo-0002",
            "engineering/alpha-0000",
            "engineering/engineering/engineering/repo-0000",
            "engineering/engineering/engineering/repo-0001",
            "engineering/engineering/repo-0000",
            "engineering/engineering/repo-0001",
            "engineering/engineering/repo-0002",
            "engineering/zulu-0000",
            "unrelated/engineering/repo-0000",
        ] {
            let object_dir = bucket_dir.join(name);
            fs::create_dir_all(&object_dir).await.expect("operation should succeed");
            fs::write(object_dir.join(STORAGE_FORMAT_FILE), b"meta")
                .await
                .expect("operation should succeed");
        }

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        async fn scan_names(disk: &LocalDisk, bucket: &str, base_dir: &str, forward_to: &str) -> (Vec<String>, i32) {
            let (reader, mut writer) = tokio::io::duplex(4096);
            let mut out = MetacacheWriter::new(&mut writer);
            let opts = WalkDirOptions {
                bucket: bucket.to_string(),
                base_dir: base_dir.to_string(),
                recursive: true,
                forward_to: Some(forward_to.to_string()),
                ..Default::default()
            };
            let mut objs_returned = 0;

            disk.scan_dir(base_dir.to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
                .await
                .expect("operation should succeed");
            out.close().await.expect("operation should succeed");
            drop(out);
            drop(writer);

            let mut reader = MetacacheReader::new(reader);
            let entries = reader.read_all().await.expect("operation should succeed");
            let names: Vec<String> = entries
                .into_iter()
                .filter(|entry| !entry.metadata.is_empty())
                .map(|entry| entry.name)
                .collect();

            (names, objs_returned)
        }

        let (engineering_names, engineering_count) =
            scan_names(&disk, bucket, "engineering/", "engineering/engineering/engineering/repo-0001").await;

        assert_eq!(
            engineering_names,
            vec![
                "engineering/engineering/engineering/repo-0001".to_string(),
                "engineering/engineering/repo-0000".to_string(),
                "engineering/engineering/repo-0001".to_string(),
                "engineering/engineering/repo-0002".to_string(),
                "engineering/zulu-0000".to_string(),
            ],
            "forward_to must resume at the requested triply repeated prefix and preserve lexicographic order"
        );
        assert_eq!(engineering_count as usize, engineering_names.len());

        let (different_names, different_count) =
            scan_names(&disk, bucket, "different/", "different/prefix/prefix/repo-0001").await;

        assert_eq!(
            different_names,
            vec![
                "different/prefix/prefix/repo-0001".to_string(),
                "different/prefix/prefix/repo-0002".to_string(),
            ],
            "forward_to must also work for repeated components unrelated to the engineering prefix"
        );
        assert_eq!(different_count as usize, different_names.len());

        let (double_names, double_count) = scan_names(&disk, bucket, "engineering/", "engineering/engineering/repo-0001").await;

        assert_eq!(
            double_names,
            vec![
                "engineering/engineering/repo-0001".to_string(),
                "engineering/engineering/repo-0002".to_string(),
                "engineering/zulu-0000".to_string(),
            ],
            "forward_to must not skip a child directory whose name repeats the base prefix"
        );
        assert_eq!(double_count as usize, double_names.len());
    }

    #[tokio::test]
    async fn test_scan_dir_hidden_delete_markers_do_not_exhaust_limit() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        fn delete_marker_metadata(version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            fm.add_version(FileInfo {
                deleted: true,
                version_id: Some(Uuid::parse_str(version_id).expect("test version id should parse")),
                mod_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            })
            .expect("delete marker metadata should be valid");
            fm.marshal_msg().expect("delete marker metadata should encode")
        }

        fn delete_marker_with_old_object_metadata(delete_version_id: &str, object_version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            fm.add_version({
                let mut fi = FileInfo::new("hidden", 1, 1);
                fi.version_id = Some(Uuid::parse_str(object_version_id).expect("test version id should parse"));
                fi.mod_time = Some(OffsetDateTime::now_utc() - time::Duration::seconds(1));
                fi
            })
            .expect("object metadata should be valid");
            fm.add_version(FileInfo {
                deleted: true,
                version_id: Some(Uuid::parse_str(delete_version_id).expect("test version id should parse")),
                mod_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            })
            .expect("delete marker metadata should be valid");
            fm.marshal_msg().expect("delete marker metadata should encode")
        }

        fn object_metadata(version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            let mut fi = FileInfo::new("visible", 1, 1);
            fi.version_id = Some(Uuid::parse_str(version_id).expect("test version id should parse"));
            fi.mod_time = Some(OffsetDateTime::now_utc());
            fm.add_version(fi).expect("object metadata should be valid");
            fm.marshal_msg().expect("object metadata should encode")
        }

        let dir = tempdir().expect("operation should succeed");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        for (name, version_id) in [
            ("shard/aaa-trash-0000", "11111111-1111-1111-1111-111111111111"),
            ("shard/aaa-trash-0001", "22222222-2222-2222-2222-222222222222"),
            ("shard/aaa-trash-0002", "33333333-3333-3333-3333-333333333333"),
        ] {
            let object_dir = bucket_dir.join(name);
            fs::create_dir_all(&object_dir).await.expect("operation should succeed");
            fs::write(object_dir.join(STORAGE_FORMAT_FILE), delete_marker_metadata(version_id))
                .await
                .expect("operation should succeed");
        }

        let hidden_versioned_dir = bucket_dir.join("shard/aaa-trash-0003");
        fs::create_dir_all(&hidden_versioned_dir)
            .await
            .expect("operation should succeed");
        fs::write(
            hidden_versioned_dir.join(STORAGE_FORMAT_FILE),
            delete_marker_with_old_object_metadata(
                "44444444-4444-4444-4444-444444444444",
                "55555555-5555-5555-5555-555555555555",
            ),
        )
        .await
        .expect("operation should succeed");

        let visible_dir = bucket_dir.join("shard/bbb-visible-0000");
        fs::create_dir_all(&visible_dir).await.expect("operation should succeed");
        fs::write(
            visible_dir.join(STORAGE_FORMAT_FILE),
            object_metadata("66666666-6666-6666-6666-666666666666"),
        )
        .await
        .expect("operation should succeed");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let (reader, mut writer) = tokio::io::duplex(4096);
        let mut out = MetacacheWriter::new(&mut writer);
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: "".to_string(),
            recursive: true,
            limit: 1,
            ..Default::default()
        };
        let mut objs_returned = 0;

        disk.scan_dir("".to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
            .await
            .expect("operation should succeed");
        out.close().await.expect("operation should succeed");
        drop(out);
        drop(writer);

        let mut reader = MetacacheReader::new(reader);
        let has_visible_object = reader
            .read_all()
            .await
            .expect("operation should succeed")
            .into_iter()
            .any(|entry| !entry.metadata.is_empty() && entry.name == "shard/bbb-visible-0000");

        assert!(has_visible_object);
        assert_eq!(objs_returned, 1);
    }

    #[tokio::test]
    async fn test_scan_dir_nonrecursive_skips_dirs_with_only_hidden_delete_markers() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        fn hidden_versioned_object_metadata(name: &str, delete_version_id: &str, object_version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            fm.add_version({
                let mut fi = FileInfo::new(name, 1, 1);
                fi.version_id = Some(Uuid::parse_str(object_version_id).expect("test version id should parse"));
                fi.mod_time = Some(OffsetDateTime::now_utc() - time::Duration::seconds(1));
                fi
            })
            .expect("object metadata should be valid");
            fm.add_version(FileInfo {
                name: name.to_owned(),
                deleted: true,
                version_id: Some(Uuid::parse_str(delete_version_id).expect("test version id should parse")),
                mod_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            })
            .expect("delete marker metadata should be valid");
            fm.marshal_msg().expect("hidden metadata should encode")
        }

        fn visible_object_metadata(name: &str, version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            let mut fi = FileInfo::new(name, 1, 1);
            fi.version_id = Some(Uuid::parse_str(version_id).expect("test version id should parse"));
            fi.mod_time = Some(OffsetDateTime::now_utc());
            fm.add_version(fi).expect("object metadata should be valid");
            fm.marshal_msg().expect("visible metadata should encode")
        }

        async fn scan_names(disk: &LocalDisk, bucket: &str, base_dir: &str, incl_deleted: bool) -> Vec<String> {
            let (reader, mut writer) = tokio::io::duplex(4096);
            let mut out = MetacacheWriter::new(&mut writer);
            let opts = WalkDirOptions {
                bucket: bucket.to_string(),
                base_dir: base_dir.to_string(),
                recursive: false,
                incl_deleted,
                ..Default::default()
            };
            let mut objs_returned = 0;

            disk.scan_dir(base_dir.to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
                .await
                .expect("scan_dir should succeed");
            out.close().await.expect("metacache writer should close");
            drop(out);
            drop(writer);

            let mut reader = MetacacheReader::new(reader);
            reader
                .read_all()
                .await
                .expect("scan output should decode")
                .into_iter()
                .map(|entry| entry.name)
                .collect()
        }

        let dir = tempdir().expect("tempdir should be created");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        let hidden_object = bucket_dir.join("hidden/deleted.txt");
        fs::create_dir_all(&hidden_object)
            .await
            .expect("hidden object dir should be created");
        fs::write(
            hidden_object.join(STORAGE_FORMAT_FILE),
            hidden_versioned_object_metadata(
                "hidden/deleted.txt",
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222",
            ),
        )
        .await
        .expect("hidden object metadata should be written");

        let nested_hidden_object = bucket_dir.join("hidden/nested/deleted.txt");
        fs::create_dir_all(&nested_hidden_object)
            .await
            .expect("nested hidden object dir should be created");
        fs::write(
            nested_hidden_object.join(STORAGE_FORMAT_FILE),
            hidden_versioned_object_metadata(
                "hidden/nested/deleted.txt",
                "33333333-3333-3333-3333-333333333333",
                "44444444-4444-4444-4444-444444444444",
            ),
        )
        .await
        .expect("nested hidden object metadata should be written");

        let visible_object = bucket_dir.join("visible/nested/object.txt");
        fs::create_dir_all(&visible_object)
            .await
            .expect("visible object dir should be created");
        fs::write(
            visible_object.join(STORAGE_FORMAT_FILE),
            visible_object_metadata("visible/nested/object.txt", "55555555-5555-5555-5555-555555555555"),
        )
        .await
        .expect("visible object metadata should be written");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let root_names = scan_names(&disk, bucket, "", false).await;
        assert!(!root_names.contains(&"hidden/".to_string()));
        assert!(root_names.contains(&"visible/".to_string()));

        let hidden_names = scan_names(&disk, bucket, "hidden/", false).await;
        assert!(!hidden_names.contains(&"hidden/nested/".to_string()));

        let visible_names = scan_names(&disk, bucket, "visible/", false).await;
        assert!(visible_names.contains(&"visible/nested/".to_string()));

        let versioned_root_names = scan_names(&disk, bucket, "", true).await;
        assert!(versioned_root_names.contains(&"hidden/".to_string()));

        let versioned_hidden_names = scan_names(&disk, bucket, "hidden/", true).await;
        assert!(versioned_hidden_names.contains(&"hidden/nested/".to_string()));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_scan_dir_propagates_metadata_read_errors() {
        use std::fs::Permissions;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);
        let object_dir = bucket_dir.join("broken");
        let meta_path = object_dir.join(STORAGE_FORMAT_FILE);

        fs::create_dir_all(&object_dir).await.expect("operation should succeed");
        fs::write(&meta_path, b"meta").await.expect("operation should succeed");

        let original_permissions = fs::metadata(&meta_path)
            .await
            .expect("operation should succeed")
            .permissions();
        fs::set_permissions(&meta_path, Permissions::from_mode(0o000))
            .await
            .expect("operation should succeed");
        if fs::File::open(&meta_path).await.is_ok() {
            fs::set_permissions(&meta_path, original_permissions)
                .await
                .expect("operation should succeed");
            return;
        }

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let (_reader, mut writer) = tokio::io::duplex(4096);
        let mut out = MetacacheWriter::new(&mut writer);
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: "".to_string(),
            recursive: true,
            ..Default::default()
        };
        let mut objs_returned = 0;

        let result = disk
            .scan_dir("".to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
            .await;

        fs::set_permissions(&meta_path, original_permissions)
            .await
            .expect("operation should succeed");

        assert!(matches!(result, Err(DiskError::FileAccessDenied)));
    }

    #[tokio::test]
    async fn test_walk_dir_ignore_multipart_dirs() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        const UUID_MULTIPART_1: &str = "8b262d24-fcf9-473d-a4cd-f9b27f24f60e";
        const UUID_MULTIPART_2: &str = "fbf3183c-63be-45cc-b3bf-424ddb7f95f8";
        const UUID_OBJ: &str = "db8b9b74-9016-4f9e-83e9-82a772947d28";
        const VER_ID_1: &str = "c683f9f8-c0a1-4bc5-8a67-0faafa839a1a";
        const VER_ID_2: &str = "a4b84f6e-c8ba-461b-8f9d-43feb0893efb";
        const VER_ID_3: &str = "892c9ae7-2bb3-44ee-9a71-bc7ddf08d765";
        const BASE_DIR: &str = "dir1/obj/";
        const MULTIPART_DIR: &str = "multipart-file";
        const DIR_IN_MULTIPART_DIR: &str = "dir-in-multipart";
        const EMPTY_STR: &str = "";

        let parse_uuid = |s: &str| Uuid::parse_str(s).expect("operation should succeed");
        let create_file_info = |version_id: &str, data_dir: &str| FileInfo {
            version_id: Some(parse_uuid(version_id)),
            data_dir: Some(parse_uuid(data_dir)),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };

        let dir = tempdir().expect("operation should succeed");
        let obj_base = dir.path().join("test-bucket").join(BASE_DIR);
        let multipart_base = obj_base.join(MULTIPART_DIR);
        let dir_in_multipart_base = multipart_base.join(DIR_IN_MULTIPART_DIR);

        fs::create_dir_all(&multipart_base).await.expect("operation should succeed");
        for uuid in &[UUID_MULTIPART_1, UUID_MULTIPART_2] {
            fs::create_dir_all(multipart_base.join(uuid))
                .await
                .expect("operation should succeed");
            fs::write(multipart_base.join(uuid).join("part.1"), b"part")
                .await
                .expect("operation should succeed");
        }
        fs::create_dir_all(obj_base.join(UUID_OBJ))
            .await
            .expect("operation should succeed");
        fs::write(obj_base.join(UUID_OBJ).join("part.1"), b"part")
            .await
            .expect("operation should succeed");

        fs::create_dir_all(&dir_in_multipart_base)
            .await
            .expect("operation should succeed");
        fs::write(dir_in_multipart_base.join(STORAGE_FORMAT_FILE), b"meta")
            .await
            .expect("operation should succeed");

        let mut fm = FileMeta::default();
        fm.add_version(create_file_info(VER_ID_1, UUID_MULTIPART_1))
            .expect("operation should succeed");
        fm.add_version(create_file_info(VER_ID_2, UUID_MULTIPART_2))
            .expect("operation should succeed");
        fs::write(
            multipart_base.join(STORAGE_FORMAT_FILE),
            fm.marshal_msg().expect("operation should succeed"),
        )
        .await
        .expect("operation should succeed");

        let mut fm = FileMeta::default();
        fm.add_version(create_file_info(VER_ID_3, UUID_OBJ))
            .expect("operation should succeed");
        fs::write(obj_base.join(STORAGE_FORMAT_FILE), fm.marshal_msg().expect("operation should succeed"))
            .await
            .expect("operation should succeed");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let (reader, mut writer) = tokio::io::duplex(4096);
        disk.walk_dir(
            WalkDirOptions {
                bucket: "test-bucket".to_string(),
                base_dir: BASE_DIR.to_string(),
                recursive: true,
                filter_prefix: Some(EMPTY_STR.to_string()),
                ..Default::default()
            },
            &mut writer,
        )
        .await
        .expect("operation should succeed");
        MetacacheWriter::new(&mut writer)
            .close()
            .await
            .expect("operation should succeed");

        let mut reader = MetacacheReader::new(reader);
        let entries = reader.read_all().await.expect("operation should succeed");
        let names: Vec<String> = entries.into_iter().map(|entry| entry.name).collect();

        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}", BASE_DIR, MULTIPART_DIR))
                .count(),
            1
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/", BASE_DIR, MULTIPART_DIR))
                .count(),
            1
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}", BASE_DIR, MULTIPART_DIR, DIR_IN_MULTIPART_DIR))
                .count(),
            1
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}/", BASE_DIR, MULTIPART_DIR, DIR_IN_MULTIPART_DIR))
                .count(),
            1
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}", BASE_DIR, MULTIPART_DIR, UUID_MULTIPART_1))
                .count(),
            0
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}", BASE_DIR, MULTIPART_DIR, UUID_MULTIPART_2))
                .count(),
            0
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}", BASE_DIR, UUID_OBJ))
                .count(),
            0
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}/", BASE_DIR, MULTIPART_DIR, UUID_MULTIPART_1))
                .count(),
            0
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}/", BASE_DIR, MULTIPART_DIR, UUID_MULTIPART_2))
                .count(),
            0
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/", BASE_DIR, UUID_OBJ))
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn test_make_volume() {
        let p = "./testv0";
        fs::create_dir_all(&p).await.expect("operation should succeed");

        let ep = match Endpoint::try_from(p) {
            Ok(e) => e,
            Err(e) => {
                println!("{e}");
                return;
            }
        };

        let disk = LocalDisk::new(&ep, false).await.expect("operation should succeed");

        let tmpp = disk
            .resolve_abs_path(Path::new(RUSTFS_META_TMP_DELETED_BUCKET))
            .expect("operation should succeed");

        println!("ppp :{:?}", &tmpp);

        let volumes = vec!["a123", "b123", "c123"];

        disk.make_volumes(volumes.clone()).await.expect("operation should succeed");

        disk.make_volumes(volumes.clone()).await.expect("operation should succeed");

        let _ = fs::remove_dir_all(&p).await;
    }

    #[tokio::test]
    async fn test_delete_volume() {
        let p = "./testv1";
        fs::create_dir_all(&p).await.expect("operation should succeed");

        let ep = match Endpoint::try_from(p) {
            Ok(e) => e,
            Err(e) => {
                println!("{e}");
                return;
            }
        };

        let disk = LocalDisk::new(&ep, false).await.expect("operation should succeed");

        let tmpp = disk
            .resolve_abs_path(Path::new(RUSTFS_META_TMP_DELETED_BUCKET))
            .expect("operation should succeed");

        println!("ppp :{:?}", &tmpp);

        let volumes = vec!["a123", "b123", "c123"];

        disk.make_volumes(volumes.clone()).await.expect("operation should succeed");

        disk.delete_volume("a", true).await.expect("operation should succeed");

        let _ = fs::remove_dir_all(&p).await;
    }

    #[tokio::test]
    async fn test_local_disk_basic_operations() {
        let test_dir = "./test_local_disk_basic";
        fs::create_dir_all(&test_dir).await.expect("operation should succeed");

        let endpoint = Endpoint::try_from(test_dir).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        // Test basic properties
        assert!(disk.is_local());
        // Note: host_name() for local disks might be empty or contain localhost/hostname
        // assert!(!disk.host_name().is_empty());
        assert!(!disk.to_string().is_empty());

        // Test path resolution
        let abs_path = disk.resolve_abs_path("test/path").expect("operation should succeed");
        assert!(abs_path.is_absolute());

        // Test bucket path
        let bucket_path = disk.get_bucket_path("test-bucket").expect("operation should succeed");
        assert!(bucket_path.to_string_lossy().contains("test-bucket"));

        // Test object path
        let object_path = disk
            .get_object_path("test-bucket", "test-object")
            .expect("operation should succeed");
        assert!(object_path.to_string_lossy().contains("test-bucket"));
        assert!(object_path.to_string_lossy().contains("test-object"));

        // Clean up the test directory
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_get_bucket_path_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        let outside_dir = tempdir().expect("operation should succeed");
        let link_path = root_dir.path().join("escape-bucket");
        symlink(outside_dir.path(), &link_path).expect("operation should succeed");

        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        assert!(matches!(disk.get_bucket_path("escape-bucket"), Err(DiskError::InvalidPath)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_get_object_path_rejects_symlink_component_escape() {
        use std::os::unix::fs::symlink;
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        let outside_dir = tempdir().expect("operation should succeed");
        let bucket_dir = root_dir.path().join("bucket");
        fs::create_dir_all(&bucket_dir).await.expect("operation should succeed");
        let link_path = bucket_dir.join("escape");
        symlink(outside_dir.path(), &link_path).expect("operation should succeed");

        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        assert!(matches!(disk.get_object_path("bucket", "escape/object.txt"), Err(DiskError::InvalidPath)));
    }

    #[tokio::test]
    async fn test_local_disk_file_operations() {
        let test_dir = "./test_local_disk_file_ops";
        fs::create_dir_all(&test_dir).await.expect("operation should succeed");

        let endpoint = Endpoint::try_from(test_dir).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        // Create test volume
        disk.make_volume("test-volume").await.expect("operation should succeed");

        // Test write and read operations
        let test_data: Vec<u8> = vec![1, 2, 3, 4, 5];
        disk.write_all("test-volume", "test-file.txt", test_data.clone().into())
            .await
            .expect("operation should succeed");

        let read_data = disk
            .read_all("test-volume", "test-file.txt")
            .await
            .expect("operation should succeed");
        assert_eq!(read_data, test_data);

        // Test file deletion
        let delete_opts = DeleteOptions {
            recursive: false,
            immediate: true,
            undo_write: false,
            old_data_dir: None,
        };
        disk.delete("test-volume", "test-file.txt", delete_opts)
            .await
            .expect("operation should succeed");

        // Clean up
        disk.delete_volume("test-volume", true)
            .await
            .expect("operation should succeed");
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn delete_volume_non_force_refuses_non_empty_bucket() {
        // backlog#799 B1: a non-force delete_volume must refuse a bucket that
        // still holds object data (VolumeNotEmpty) and leave it intact, so a
        // misclassified "dangling" bucket cannot be recursively wiped. Only an
        // explicit force delete removes it recursively.
        let test_dir = "./test_b1_delete_volume_guard";
        let _ = fs::remove_dir_all(&test_dir).await;
        fs::create_dir_all(&test_dir).await.expect("operation should succeed");
        let endpoint = Endpoint::try_from(test_dir).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        disk.make_volume("b1-bucket").await.expect("operation should succeed");
        let data: Vec<u8> = vec![1, 2, 3];
        disk.write_all("b1-bucket", "obj.dat", data.clone().into())
            .await
            .expect("operation should succeed");

        // Non-force must refuse and preserve the data.
        let err = disk
            .delete_volume("b1-bucket", false)
            .await
            .expect_err("non-empty bucket must be refused");
        assert!(matches!(err, DiskError::VolumeNotEmpty), "expected VolumeNotEmpty, got {err:?}");
        assert!(
            disk.stat_volume("b1-bucket").await.is_ok(),
            "bucket must still exist after a refused non-force delete"
        );
        assert_eq!(disk.read_all("b1-bucket", "obj.dat").await.expect("data preserved"), data);

        // Force removes it recursively.
        disk.delete_volume("b1-bucket", true)
            .await
            .expect("force delete removes non-empty");
        assert!(disk.stat_volume("b1-bucket").await.is_err(), "bucket must be gone after force delete");

        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn test_local_disk_volume_operations() {
        let test_dir = "./test_local_disk_volumes";
        fs::create_dir_all(&test_dir).await.expect("operation should succeed");

        let endpoint = Endpoint::try_from(test_dir).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        // Test creating multiple volumes
        let volumes = vec!["vol1", "vol2", "vol3"];
        disk.make_volumes(volumes.clone()).await.expect("operation should succeed");

        // Test listing volumes
        let volume_list = disk.list_volumes().await.expect("operation should succeed");
        assert!(!volume_list.is_empty());

        // Test volume stats
        for vol in &volumes {
            let vol_info = disk.stat_volume(vol).await.expect("operation should succeed");
            assert_eq!(vol_info.name, *vol);
        }

        // Test deleting volumes
        for vol in &volumes {
            disk.delete_volume(vol, true).await.expect("operation should succeed");
        }

        // Clean up the test directory
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn test_local_disk_disk_info() {
        let test_dir = "./test_local_disk_info";
        fs::create_dir_all(&test_dir).await.expect("operation should succeed");

        let endpoint = Endpoint::try_from(test_dir).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let disk_info_opts = DiskInfoOptions {
            disk_id: "test-disk".to_string(),
            metrics: true,
            noop: false,
        };

        let disk_info = disk.disk_info(&disk_info_opts).await.expect("operation should succeed");

        // Basic checks on disk info
        // Note: On macOS, Windows, and some other systems, fs_type may be empty
        // because statvfs does not provide filesystem type information.
        // This is a platform limitation, not a bug.
        #[cfg(not(any(target_os = "macos", windows)))]
        assert!(!disk_info.fs_type.is_empty(), "fs_type should not be empty on this platform");
        assert!(disk_info.total > 0);
        assert!(disk_info.free <= disk_info.total);
        assert_eq!(disk_info.nr_requests, disk.nrrequests);
        assert_eq!(disk_info.rotational, disk.rotational);
        assert!(!disk_info.mount_path.is_empty());
        assert!(!disk_info.endpoint.is_empty());

        // Clean up the test directory
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn test_read_file_stream_rejects_offset_length_overflow() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        disk.make_volume("test-volume").await.expect("operation should succeed");
        disk.write_all("test-volume", "test-file.txt", Bytes::from_static(b"test"))
            .await
            .expect("operation should succeed");

        let result = disk.read_file_stream("test-volume", "test-file.txt", usize::MAX, 1).await;
        assert!(matches!(result, Err(DiskError::FileCorrupt)));
    }

    #[tokio::test]
    async fn test_read_file_mmap_copy_rejects_offset_length_overflow() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        disk.make_volume("test-volume").await.expect("operation should succeed");
        disk.write_all("test-volume", "test-file.txt", Bytes::from_static(b"test"))
            .await
            .expect("operation should succeed");

        let result = disk.read_file_mmap_copy("test-volume", "test-file.txt", usize::MAX, 1).await;
        assert!(matches!(result, Err(DiskError::FileCorrupt)));
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_read_file_zero_copy_legacy_alias_rejects_offset_length_overflow() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        disk.make_volume("test-volume").await.expect("operation should succeed");
        disk.write_all("test-volume", "test-file.txt", Bytes::from_static(b"test"))
            .await
            .expect("operation should succeed");

        let result = disk.read_file_zero_copy("test-volume", "test-file.txt", usize::MAX, 1).await;
        assert!(matches!(result, Err(DiskError::FileCorrupt)));
    }

    #[test]
    fn test_is_valid_volname() {
        // Valid volume names (length >= 3)
        assert!(LocalDisk::is_valid_volname("valid-name"));
        assert!(LocalDisk::is_valid_volname("test123"));
        assert!(LocalDisk::is_valid_volname("my-bucket"));

        // Test minimum length requirement
        assert!(!LocalDisk::is_valid_volname(""));
        assert!(!LocalDisk::is_valid_volname("a"));
        assert!(!LocalDisk::is_valid_volname("ab"));
        assert!(LocalDisk::is_valid_volname("abc"));

        // Note: The current implementation doesn't check for system volume names
        // It only checks length and platform-specific special characters
        // System volume names are valid according to the current implementation
        assert!(LocalDisk::is_valid_volname(RUSTFS_META_BUCKET));
        assert!(LocalDisk::is_valid_volname(super::super::RUSTFS_META_TMP_BUCKET));

        // Testing platform-specific behavior for special characters
        #[cfg(windows)]
        {
            // On Windows systems, these should be invalid
            assert!(!LocalDisk::is_valid_volname("invalid\\name"));
            assert!(!LocalDisk::is_valid_volname("invalid:name"));
            assert!(!LocalDisk::is_valid_volname("invalid|name"));
            assert!(!LocalDisk::is_valid_volname("invalid<name"));
            assert!(!LocalDisk::is_valid_volname("invalid>name"));
            assert!(!LocalDisk::is_valid_volname("invalid?name"));
            assert!(!LocalDisk::is_valid_volname("invalid*name"));
            assert!(!LocalDisk::is_valid_volname("invalid\"name"));
        }

        #[cfg(not(windows))]
        {
            // On non-Windows systems, the current implementation doesn't check special characters
            // So these would be considered valid
            assert!(LocalDisk::is_valid_volname("valid/name"));
            assert!(LocalDisk::is_valid_volname("valid:name"));
        }
    }

    #[tokio::test]
    async fn test_read_file_exists() {
        let test_file = "./test_read_exists.txt";

        // Test non-existent file
        let (data, metadata) = read_file_exists(test_file).await.expect("operation should succeed");
        assert!(data.is_empty());
        assert!(metadata.is_none());

        // Create test file
        fs::write(test_file, b"test content").await.expect("operation should succeed");

        // Test existing file
        let (data, metadata) = read_file_exists(test_file).await.expect("operation should succeed");
        assert_eq!(data.as_ref(), b"test content");
        assert!(metadata.is_some());

        // Clean up
        let _ = fs::remove_file(test_file).await;
    }

    #[tokio::test]
    async fn test_read_file_all() {
        let test_file = "./test_read_all.txt";
        let test_content = b"test content for read_all";

        // Create test file
        fs::write(test_file, test_content).await.expect("operation should succeed");

        // Test reading file
        let (data, metadata) = read_file_all(test_file).await.expect("operation should succeed");
        assert_eq!(data.as_ref(), test_content);
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), test_content.len() as u64);

        // Clean up
        let _ = fs::remove_file(test_file).await;
    }

    #[tokio::test]
    async fn test_read_file_metadata() {
        let test_file = "./test_metadata.txt";

        // Create test file
        fs::write(test_file, b"test").await.expect("operation should succeed");

        // Test reading metadata
        let metadata = read_file_metadata(test_file).await.expect("operation should succeed");
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), 4); // "test" is 4 bytes

        // Clean up
        let _ = fs::remove_file(test_file).await;
    }

    #[test]
    fn test_is_root_path() {
        // Unix root path
        assert!(is_root_path("/"));

        // Windows root path (only on Windows)
        #[cfg(windows)]
        assert!(is_root_path("\\"));

        // Non-root paths
        assert!(!is_root_path("/home"));
        assert!(!is_root_path("/tmp"));
        assert!(!is_root_path("relative/path"));

        // On non-Windows systems, backslash is not a root path
        #[cfg(not(windows))]
        assert!(!is_root_path("\\"));
    }

    #[test]
    fn test_normalize_path_components() {
        // Test basic relative path
        assert_eq!(normalize_path_components("a/b/c"), PathBuf::from("a/b/c"));

        // Test path with current directory components (should be ignored)
        assert_eq!(normalize_path_components("a/./b/./c"), PathBuf::from("a/b/c"));

        // Test path with parent directory components
        assert_eq!(normalize_path_components("a/b/../c"), PathBuf::from("a/c"));

        // Test path with multiple parent directory components
        assert_eq!(normalize_path_components("a/b/c/../../d"), PathBuf::from("a/d"));

        // Test path that goes beyond root
        assert_eq!(normalize_path_components("a/../../../b"), PathBuf::from("b"));

        // Test absolute path
        assert_eq!(normalize_path_components("/a/b/c"), PathBuf::from("/a/b/c"));

        // Test absolute path with parent components
        assert_eq!(normalize_path_components("/a/b/../c"), PathBuf::from("/a/c"));

        // Test complex path with mixed components
        assert_eq!(normalize_path_components("a/./b/../c/./d/../e"), PathBuf::from("a/c/e"));

        // Test path with only current directory
        assert_eq!(normalize_path_components("."), PathBuf::from(""));

        // Test path with only parent directory
        assert_eq!(normalize_path_components(".."), PathBuf::from(""));

        // Test path with multiple current directories
        assert_eq!(normalize_path_components("./././a"), PathBuf::from("a"));

        // Test path with multiple parent directories
        assert_eq!(normalize_path_components("../../a"), PathBuf::from("a"));

        // Test empty path
        assert_eq!(normalize_path_components(""), PathBuf::from(""));

        // Test path starting with current directory
        assert_eq!(normalize_path_components("./a/b"), PathBuf::from("a/b"));

        // Test path starting with parent directory
        assert_eq!(normalize_path_components("../a/b"), PathBuf::from("a/b"));

        // Test complex case with multiple levels of parent navigation
        assert_eq!(normalize_path_components("a/b/c/../../../d/e/f/../../g"), PathBuf::from("d/g"));

        // Test path that completely cancels out
        assert_eq!(normalize_path_components("a/b/../../../c/d/../../.."), PathBuf::from(""));

        // Test Windows-style paths (if applicable)
        #[cfg(windows)]
        {
            assert_eq!(normalize_path_components("C:\\a\\b\\c"), PathBuf::from("C:\\a\\b\\c"));

            assert_eq!(normalize_path_components("C:\\a\\..\\b"), PathBuf::from("C:\\b"));
        }
    }

    #[test]
    fn should_reclaim_file_cache_after_write_respects_env_and_threshold() {
        temp_env::with_var_unset(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE, || {
            assert!(!should_reclaim_file_cache_after_write(8 * 1024 * 1024));
        });

        temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE, Some("true"), || {
            temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD, Some("4194304"), || {
                assert!(should_reclaim_file_cache_after_write(8 * 1024 * 1024));
                assert!(!should_reclaim_file_cache_after_write(1024));
            });
        });
    }

    #[test]
    fn should_reclaim_file_cache_after_read_respects_env_and_threshold() {
        temp_env::with_var_unset(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE, || {
            temp_env::with_var_unset(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD, || {
                assert!(should_reclaim_file_cache_after_read(8 * 1024 * 1024));
                assert!(!should_reclaim_file_cache_after_read(1024));
            });
        });

        temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE, Some("false"), || {
            temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD, Some("4194304"), || {
                assert!(!should_reclaim_file_cache_after_read(8 * 1024 * 1024));
            });
        });

        temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE, Some("true"), || {
            temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD, Some("4194304"), || {
                assert!(should_reclaim_file_cache_after_read(8 * 1024 * 1024));
                assert!(!should_reclaim_file_cache_after_read(1024));
            });
        });
    }

    #[test]
    fn should_populate_mmap_read_respects_env() {
        temp_env::with_var_unset(ENV_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE, || {
            assert!(!should_populate_mmap_read(512 * 1024));
        });

        temp_env::with_var(ENV_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE, Some("true"), || {
            assert!(should_populate_mmap_read(512 * 1024));
            assert!(!should_populate_mmap_read(0));
        });

        temp_env::with_var(ENV_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE, Some("false"), || {
            assert!(!should_populate_mmap_read(512 * 1024));
        });
    }

    #[test]
    fn local_read_copy_method_respects_env() {
        temp_env::with_var_unset(ENV_RUSTFS_OBJECT_MMAP_READ_METHOD, || {
            assert_eq!(local_read_copy_method(), LocalReadCopyMethod::MmapCopy);
        });

        temp_env::with_var(
            ENV_RUSTFS_OBJECT_MMAP_READ_METHOD,
            Some(RUSTFS_OBJECT_MMAP_READ_METHOD_DIRECT_READ_COPY),
            || {
                assert_eq!(local_read_copy_method(), LocalReadCopyMethod::DirectReadCopy);
            },
        );

        temp_env::with_var(ENV_RUSTFS_OBJECT_MMAP_READ_METHOD, Some("unknown"), || {
            assert_eq!(local_read_copy_method(), LocalReadCopyMethod::MmapCopy);
        });
    }

    #[cfg(unix)]
    #[test]
    fn mmap_page_size_is_cached_positive() {
        let first = mmap_page_size().expect("page size should be available");
        let second = mmap_page_size().expect("cached page size should be available");

        assert!(first > 0);
        assert_eq!(first, second);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_file_mmap_copy_supports_direct_read_copy_method() {
        use tempfile::tempdir;

        temp_env::async_with_vars(
            [(ENV_RUSTFS_OBJECT_MMAP_READ_METHOD, Some(RUSTFS_OBJECT_MMAP_READ_METHOD_DIRECT_READ_COPY))],
            async {
                let root_dir = tempdir().expect("operation should succeed");
                let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
                let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
                disk.make_volume("test-volume").await.expect("operation should succeed");
                disk.write_all("test-volume", "test-file.txt", Bytes::from_static(b"0123456789abcdef"))
                    .await
                    .expect("operation should succeed");

                let data = disk
                    .read_file_mmap_copy("test-volume", "test-file.txt", 4, 6)
                    .await
                    .expect("operation should succeed");

                assert_eq!(data, Bytes::from_static(b"456789"));
            },
        )
        .await;
    }

    #[cfg(unix)]
    #[test]
    fn mmap_page_fault_delta_clamps_non_monotonic_counts() {
        let before = Some(MmapPageFaultCounts { minor: 10, major: 4 });
        let after = Some(MmapPageFaultCounts { minor: 7, major: 6 });

        assert_eq!(mmap_page_fault_delta(before, after), MmapPageFaultDelta { minor: 0, major: 2 });
        assert_eq!(mmap_page_fault_delta(before, None), MmapPageFaultDelta::default());
    }

    #[test]
    fn test_is_bitrot_size_mismatch_error_only_matches_target_message() {
        assert!(is_bitrot_size_mismatch_error(&std::io::Error::other("bitrot shard file size mismatch")));
        assert!(!is_bitrot_size_mismatch_error(&std::io::Error::other("bitrot hash mismatch")));
    }

    /// Differential test for the LocalIoBackend refactor: every read shape
    /// (pread via mmap_copy, pread via direct_read_copy, bounded stream, full
    /// read) must return byte-identical data for the same (offset, length),
    /// including ranges straddling page boundaries and the file tail.
    #[cfg(unix)]
    #[tokio::test]
    async fn io_backend_read_shapes_return_identical_bytes() {
        use tempfile::tempdir;
        use tokio::io::AsyncReadExt;

        const FILE_LEN: usize = 64 * 1024;

        // Deterministic pseudo-random content (LCG) so failures are reproducible.
        let mut state = 0x9e3779b97f4a7c15u64;
        let content: Vec<u8> = (0..FILE_LEN)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (state >> 33) as u8
            })
            .collect();
        let content = Bytes::from(content);

        let root_dir = tempdir().expect("operation should succeed");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
        disk.make_volume("test-volume").await.expect("operation should succeed");
        disk.write_all("test-volume", "blob.bin", content.clone())
            .await
            .expect("operation should succeed");

        let page = mmap_page_size().expect("page size should be available") as usize;
        let ranges = [
            (0usize, FILE_LEN),
            (1, 17),
            (page - 1, 2),
            (page, page),
            (2 * page - 1, page + 2),
            (FILE_LEN - 7, 7),
            (0, 0),
        ];

        for (offset, length) in ranges {
            let expected = content.slice(offset..offset + length);

            for method in [
                RUSTFS_OBJECT_MMAP_READ_METHOD_MMAP_COPY,
                RUSTFS_OBJECT_MMAP_READ_METHOD_DIRECT_READ_COPY,
            ] {
                let got = temp_env::async_with_vars([(ENV_RUSTFS_OBJECT_MMAP_READ_METHOD, Some(method))], async {
                    disk.read_file_mmap_copy("test-volume", "blob.bin", offset, length)
                        .await
                        .expect("operation should succeed")
                })
                .await;
                assert_eq!(got, expected, "pread_bytes({method}) mismatch at offset={offset} length={length}");
            }

            let mut stream = disk
                .read_file_stream("test-volume", "blob.bin", offset, length)
                .await
                .expect("operation should succeed");
            let mut streamed = vec![0u8; length];
            stream.read_exact(&mut streamed).await.expect("operation should succeed");
            assert_eq!(
                Bytes::from(streamed),
                expected,
                "open_read_stream mismatch at offset={offset} length={length}"
            );
        }

        let mut full = disk
            .read_file("test-volume", "blob.bin")
            .await
            .expect("operation should succeed");
        let mut all = Vec::new();
        full.read_to_end(&mut all).await.expect("operation should succeed");
        assert_eq!(Bytes::from(all), content, "open_full_read mismatch");
    }

    /// The O_DIRECT read path must return the same bytes as the buffered
    /// path for unaligned shard sizes and ranges, and must silently fall
    /// back (never error) on filesystems that reject O_DIRECT (e.g. tmpfs).
    /// Both legs are covered regardless of which filesystem backs tempdir.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn direct_io_read_matches_buffered_path_or_falls_back() {
        use tempfile::tempdir;

        // Unaligned on purpose: 3 blocks + 7 bytes.
        const FILE_LEN: usize = 4096 * 3 + 7;

        let mut state = 0x2545f4914f6cdd1du64;
        let content: Vec<u8> = (0..FILE_LEN)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (state >> 33) as u8
            })
            .collect();
        let content = Bytes::from(content);

        let root_dir = tempdir().expect("operation should succeed");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
        disk.make_volume("test-volume").await.expect("operation should succeed");
        disk.write_all("test-volume", "shard.bin", content.clone())
            .await
            .expect("operation should succeed");

        let ranges = [
            (0usize, FILE_LEN),
            (0, 4096),
            (4095, 4098),
            (4096 * 2, 4096 + 7),
            (FILE_LEN - 7, 7),
        ];

        for (offset, length) in ranges {
            let expected = content.slice(offset..offset + length);
            // Threshold 1 forces every non-empty read through the O_DIRECT attempt.
            let got = temp_env::async_with_vars(
                [
                    (ENV_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE, Some("true")),
                    (ENV_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD, Some("1")),
                ],
                async {
                    disk.read_file_mmap_copy("test-volume", "shard.bin", offset, length)
                        .await
                        .expect("O_DIRECT-eligible read must succeed (direct or fallback)")
                },
            )
            .await;
            assert_eq!(got, expected, "direct-io read mismatch at offset={offset} length={length}");
        }
    }

    /// pread_direct_aligned must never leak alignment padding: the returned
    /// buffer is exactly the requested logical range.
    #[cfg(target_os = "linux")]
    #[test]
    fn pread_direct_aligned_exact_range_or_unsupported() {
        use std::io::Write;

        let dir = tempfile::tempdir().expect("operation should succeed");
        let file_path = dir.path().join("blob.bin");
        let content: Vec<u8> = (0..4096 * 2 + 13).map(|i| (i % 251) as u8).collect();
        std::fs::File::create(&file_path)
            .and_then(|mut f| f.write_all(&content))
            .expect("operation should succeed");

        let state = DirectIoReadState::new();
        match pread_direct_aligned(&file_path, 4090, 100, &state) {
            Ok(bytes) => {
                assert_eq!(&bytes[..], &content[4090..4190], "padding must not leak");
            }
            Err(err) => {
                assert!(
                    is_direct_io_unsupported(&err),
                    "only unsupported-filesystem errors are acceptable here: {err}"
                );
            }
        }
    }
}
