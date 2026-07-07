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

//! `SetDisks` — one erasure set's worth of disks and the object storage logic
//! over them. Historically a single ~19.7k-line God-Object; split into a Core
//! plus borrow-based operation families during backlog#815 (Epic #728).
//!
//! Module layout after the split:
//!
//! - `mod.rs` — the `SetDisks` core struct, its construction, and the shared
//!   inherent state/helpers the operation families borrow.
//! - `ctx` — `SetDisksCtx`, the borrow context that hands operation units
//!   access to the core without cloning `Arc`s (P0, backlog#816).
//! - `core::io_primitives` — the shared low-level read/write/erasure IO
//!   primitives (metadata-fanout quorum, bitrot readers, rename/delete, quorum
//!   helpers) that the operation families call through the core (P5,
//!   backlog#820).
//! - `ops/` — one module per storage-api contract family, each
//!   `impl <Contract> for SetDisks`: `ops::object` (`ObjectIO` +
//!   `ObjectOperations`, the object read/write hot path, P6/backlog#821),
//!   `ops::heal` (`HealOperations`, P1/backlog#817), `ops::multipart`
//!   (`MultipartOperations`, P2/backlog#818), `ops::list` (`ListOperations`)
//!   and `ops::bucket` (`BucketOperations`, P3+P4/backlog#819), `ops::locking`
//!   (`NamespaceLocking` + lock helpers, P7/backlog#822).
//! - `read.rs` — the object-read operation pipeline (`get_object_*`,
//!   `read_version_optimized`) and its metadata cache, kept separate from the
//!   read primitives it drives.
//! - `metadata.rs`, `replication.rs`, `shard_source.rs` — supporting helpers.

// #730: SetDisks still hosts staged read/heal/write migration helpers.
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;
use crate::bucket::metadata_sys;
use crate::bucket::object_lock::objectlock_sys::{check_object_lock_for_deletion, check_retention_for_modification};
use crate::bucket::replication::{
    ReplicateDecision, ReplicationObjectBridge, ReplicationState, ReplicationStatusType, VersionPurgeStatusType,
    replication_state_to_filemeta,
};
use crate::bucket::versioning::VersioningApi;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::client::{object_api_utils::get_raw_etag, transition_api::ReaderImpl};
use crate::cluster::rpc::heal_bucket_local_on_disks;
use crate::data_usage::record_compression_total_memory;
use crate::diagnostics::get::{
    GET_CODEC_STREAMING_OBJECT_CLASS_PLAIN_SINGLE_PART, GET_OBJECT_PATH_BODY_CACHE, GET_OBJECT_PATH_CODEC_STREAMING,
    GET_OBJECT_PATH_CODEC_STREAMING_LEGACY_ENGINE, GET_OBJECT_PATH_CODEC_STREAMING_RUSTFS_ENGINE, GET_OBJECT_PATH_DIRECT_MEMORY,
    GET_OBJECT_PATH_EMPTY, GET_OBJECT_PATH_INLINE_DIRECT, GET_OBJECT_PATH_LEGACY_DUPLEX, GET_OBJECT_PATH_REMOTE_TRANSITION,
    GET_OBJECT_PATH_SET_DISK, GET_STAGE_DECODE, GET_STAGE_EMIT, GET_STAGE_INLINE_PREPARE, GET_STAGE_LOCK_ACQUIRE,
    GET_STAGE_METADATA, GET_STAGE_OBJECT_INFO, GET_STAGE_PATH_DECISION, GET_STAGE_READER_SETUP, classify_storage_error,
    get_stage_timer_if_enabled, record_get_object_pipeline_failure, record_get_stage_duration_if_enabled,
};
use crate::disk::error_reduce::{
    BUCKET_OP_IGNORED_ERRS, OBJECT_OP_IGNORED_ERRS, build_write_quorum_failure_summary, count_errs, reduce_read_quorum_errs,
    reduce_write_quorum_errs,
};
use crate::disk::{
    self, CHECK_PART_DISK_NOT_FOUND, CHECK_PART_FILE_CORRUPT, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS, CHECK_PART_UNKNOWN,
    conv_part_err_to_int, has_part_err,
};
use crate::disk::{STORAGE_FORMAT_FILE, count_part_not_success};
use crate::erasure::codec::bridge::{
    CodecStreamingDecodeEngine, GET_CODEC_STREAMING_ENGINE_LEGACY, GET_CODEC_STREAMING_ENGINE_RUSTFS,
};
use crate::erasure::coding;
use crate::error::{Error, Result, is_err_version_not_found};
use crate::error::{GenericError, ObjectApiError, is_err_object_not_found};
use crate::io_support::bitrot::{create_bitrot_reader, create_bitrot_reader_from_bytes, create_bitrot_writer};
use crate::object_api::ObjectOptions;
use crate::object_api::get_object_body_cache_hook;
use crate::runtime::sources as runtime_sources;
use crate::services::batch_processor::AsyncBatchProcessor;
use crate::storage_api_contracts::{
    bucket::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions},
    list::{StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageObjectInfoOrErr, StorageWalkOptions},
    multipart::{
        CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartOperations as _, MultipartUploadResult, PartInfo,
    },
    namespace::NamespaceLocking as _,
    object::{DeletedObject, ObjectIO as _, ObjectOperations as _, ObjectToDelete},
    range::HTTPRangeSpec,
};
use crate::store::utils::is_reserved_or_invalid_bucket;
use crate::{
    bucket::lifecycle::bucket_lifecycle_ops::{
        LifecycleOps, gen_transition_objname, get_transitioned_object_reader, put_restore_opts,
    },
    cache_value::metacache_set::{ListPathRawOptions, list_path_raw},
    config::storageclass,
    disk::{
        CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskOption, DiskStore, FileInfoVersions,
        RUSTFS_META_BUCKET, RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_BUCKET, ReadMultipleReq, ReadMultipleResp, ReadOptions,
        UpdateMetadataOpts, endpoint::Endpoint, error::DiskError, format::FormatV3, new_disk,
    },
    error::{StorageError, to_object_err},
    object_api::{GetObjectReader, ObjectInfo, PutObjReader},
    // event::name::EventName,
    services::event_notification::{EventArgs, send_event},
    store::init_format::{get_format_erasure_in_quorum, load_format_erasure, load_format_erasure_all, save_format_file},
};
use bytes::Bytes;
use bytesize::ByteSize;
use chrono::Utc;
use futures::future::join_all;
use glob::Pattern;
use http::HeaderMap;
use md5::{Digest as Md5Digest, Md5};
use rand::{Rng, seq::SliceRandom};
use regex::Regex;
use rustfs_common::heal_channel::{
    DriveState, HealAdmissionResult, HealChannelPriority, HealItemType, HealOpts, HealRequestSource, HealScanMode,
    send_heal_disk, send_heal_request_with_admission,
};
use rustfs_config::MI_B;
use rustfs_filemeta::{
    FileInfo, FileMeta, FileMetaShallowVersion, MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams, ObjectPartInfo,
    RawFileInfo, file_info_from_raw, merge_file_meta_versions,
};
use rustfs_io_metrics::{
    record_object_lock_diag_acquire_duration, record_object_lock_diag_enabled, record_object_lock_diag_hold_duration,
    record_object_lock_diag_slow_acquire, record_object_lock_diag_slow_hold,
};
use rustfs_lock::LockClient;
use rustfs_lock::fast_lock::types::LockResult;
use rustfs_lock::local_lock::LocalLock;
use rustfs_lock::{FastLockGuard, LockManager, NamespaceLock, NamespaceLockGuard, NamespaceLockWrapper, ObjectKey};
use rustfs_madmin::heal_commands::{HealDriveInfo, HealResultItem, Infos};
use rustfs_object_capacity::capacity_scope::{
    CapacityScope, CapacityScopeDisk, record_capacity_scope, record_global_dirty_scope,
};
use rustfs_s3_types::EventName;
use rustfs_utils::http::headers::AMZ_OBJECT_TAGGING;
use rustfs_utils::http::headers::AMZ_STORAGE_CLASS;
use rustfs_utils::http::headers::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_TYPE, EXPIRES, HeaderExt as _,
};
use rustfs_utils::http::{
    SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER, SUFFIX_ACTUAL_OBJECT_SIZE_CAP, SUFFIX_ACTUAL_SIZE,
    SUFFIX_COMPRESSION, SUFFIX_COMPRESSION_SIZE, SUFFIX_REPLICATION_SSEC_CRC, contains_key_str, get_header_map, get_str,
    insert_str, is_encryption_metadata_key, remove_header_map,
};
use rustfs_utils::{
    HashAlgorithm,
    crypto::hex,
    path::{SLASH_SEPARATOR, encode_dir_object, has_suffix, path_join_buf},
};
use s3s::header::{X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, X_AMZ_RESTORE};
use sha2::{Digest, Sha256};
use std::hash::Hash;
use std::mem::{self};
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{
    collections::{HashMap, HashSet},
    io::{Cursor, Write},
    path::Path,
    sync::Arc,
    time::Duration,
};
use time::OffsetDateTime;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf},
    sync::{RwLock, broadcast},
};
use tokio::{
    select,
    sync::mpsc::{self, Sender},
    time::{interval, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::{Instrument, debug, info, warn};
use uuid::Uuid;

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
type WalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;
type InlineBitrotReader = coding::BitrotReader<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>>;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_SET_DISK: &str = "set_disk";
const EVENT_SET_DISK_MULTIPART: &str = "set_disk_multipart";
const COMPLETE_MULTIPART_PART_MISSING: &str = "part_missing";
const COMPLETE_MULTIPART_PART_READ_QUORUM_UNAVAILABLE: &str = "read_quorum_unavailable";
const COMPLETE_MULTIPART_PART_ERROR: &str = "part_error";
const MULTIPART_WRITE_QUORUM_UPLOAD_METADATA: &str = "upload_metadata";
const MULTIPART_WRITE_QUORUM_WRITER_SETUP: &str = "writer_setup";
const MULTIPART_WRITE_QUORUM_RENAME_PART: &str = "rename_part";
const EVENT_SET_DISK_WRITE: &str = "set_disk_write";
const EVENT_SET_DISK_HEAL: &str = "set_disk_heal";
const EVENT_SET_DISK_COMMIT_TAIL_SLOW: &str = "set_disk_commit_tail_slow";
const EVENT_SET_DISK_PUT_OBJECT_STAGE_SUMMARY: &str = "set_disk_put_object_stage_summary";
const SET_DISK_COMMIT_TAIL_WARN_THRESHOLD_MS: u128 = 5_000;
const ENV_RUSTFS_PUT_LARGE_BATCH_MIN_SIZE_BYTES: &str = "RUSTFS_PUT_LARGE_BATCH_MIN_SIZE_BYTES";
const DEFAULT_RUSTFS_PUT_LARGE_BATCH_MIN_SIZE_BYTES: usize = 64 * 1024 * 1024;
static CACHED_PUT_LARGE_BATCH_MIN_SIZE_BYTES: OnceLock<usize> = OnceLock::new();
const ENV_RUSTFS_MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES: &str = "RUSTFS_MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES";
const DEFAULT_RUSTFS_MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES: usize = 128 * 1024 * 1024;
static CACHED_MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES: OnceLock<usize> = OnceLock::new();

use crate::io_support::rio::{EtagResolvable, HashReader, HashReaderMut, TryGetIndex as _};

pub const DEFAULT_READ_BUFFER_SIZE: usize = MI_B; // 1 MiB = 1024 * 1024;
pub const MAX_PARTS_COUNT: usize = 10000;
pub(crate) const RUSTFS_MULTIPART_BUCKET_KEY: &str = "x-rustfs-internal-multipart-bucket";
pub(crate) const RUSTFS_MULTIPART_OBJECT_KEY: &str = "x-rustfs-internal-multipart-object";
const ENV_ISSUE3031_DIAG_ENABLE: &str = "RUSTFS_ISSUE3031_DIAG_ENABLE";

struct ObjectLockDiagGuard {
    guard: NamespaceLockGuard,
    enabled: bool,
    op: &'static str,
    bucket: Option<String>,
    object: Option<String>,
    owner: Option<String>,
    mode: &'static str,
    acquired_at: Instant,
}

impl ObjectLockDiagGuard {
    fn new(
        guard: NamespaceLockGuard,
        enabled: bool,
        op: &'static str,
        bucket: Option<String>,
        object: Option<String>,
        owner: Option<String>,
        mode: &'static str,
    ) -> Self {
        Self {
            guard,
            enabled,
            op,
            bucket,
            object,
            owner,
            mode,
            acquired_at: Instant::now(),
        }
    }
}

impl Drop for ObjectLockDiagGuard {
    fn drop(&mut self) {
        if !self.enabled || self.guard.is_released() {
            return;
        }

        let hold = self.acquired_at.elapsed();
        record_object_lock_diag_hold_duration(self.op, self.mode, hold);
        let threshold = get_object_lock_diag_slow_hold_threshold();
        if hold >= threshold {
            record_object_lock_diag_slow_hold(self.op, self.mode);
            warn!(
                target: "rustfs_ecstore::object_lock_diag",
                op = self.op,
                bucket = %self.bucket.as_deref().unwrap_or_default(),
                object = %self.object.as_deref().unwrap_or_default(),
                mode = self.mode,
                owner = %self.owner.as_deref().unwrap_or_default(),
                hold_ms = hold.as_millis(),
                threshold_ms = threshold.as_millis(),
                "object namespace lock held longer than threshold"
            );
        }
    }
}

struct SetDiskLockGuardedReader {
    inner: Box<dyn AsyncRead + Unpin + Send + Sync>,
    guard: Option<ObjectLockDiagGuard>,
}

impl AsyncRead for SetDiskLockGuardedReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let had_capacity = buf.remaining() > 0;
        let filled_before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if had_capacity && matches!(poll, Poll::Ready(Ok(()))) && buf.filled().len() == filled_before {
            self.guard.take();
        }
        poll
    }
}

fn finish_set_disk_read_lock(
    mut reader: GetObjectReader,
    read_lock_guard: Option<ObjectLockDiagGuard>,
    lock_optimization_enabled: bool,
    bucket: &str,
    object: &str,
) -> GetObjectReader {
    if lock_optimization_enabled || reader.buffered_body.is_some() {
        release_materialized_read_lock(bucket, object, read_lock_guard);
        return reader;
    }

    if let Some(guard) = read_lock_guard {
        reader.stream = Box::new(SetDiskLockGuardedReader {
            inner: reader.stream,
            guard: Some(guard),
        });
    }
    reader
}

fn release_materialized_read_lock(bucket: &str, object: &str, read_lock_guard: Option<ObjectLockDiagGuard>) {
    if read_lock_guard.is_some() {
        let lock_id = format!("{}:{}", bucket, object);
        record_lock_release(bucket, object, &lock_id, "read");
        metrics::counter!("rustfs.lock.release.early.total", "type" => "read").increment(1);
    }
    drop(read_lock_guard);
}

pub(crate) fn strip_internal_multipart_metadata(metadata: &mut HashMap<String, String>) {
    metadata.remove(RUSTFS_MULTIPART_BUCKET_KEY);
    metadata.remove(RUSTFS_MULTIPART_OBJECT_KEY);
}

fn should_persist_encryption_original_size(metadata: &HashMap<String, String>) -> bool {
    metadata.keys().any(|key| is_encryption_metadata_key(key))
        || metadata.contains_key(SSEC_ALGORITHM_HEADER)
        || metadata.contains_key(SSEC_KEY_HEADER)
        || metadata.contains_key(SSEC_KEY_MD5_HEADER)
}

fn capacity_scope_from_disks(disks: &[Option<DiskStore>]) -> CapacityScope {
    let mut unique = HashSet::with_capacity(disks.len());
    let mut scoped_disks = Vec::with_capacity(disks.len());

    for disk in disks.iter().flatten() {
        let scope_disk = CapacityScopeDisk {
            endpoint: disk.endpoint().to_string(),
            drive_path: disk.to_string(),
        };
        if unique.insert(scope_disk.clone()) {
            scoped_disks.push(scope_disk);
        }
    }

    CapacityScope { disks: scoped_disks }
}

fn record_capacity_scope_if_needed(scope_token: Option<Uuid>, disks: &[Option<DiskStore>]) {
    let scope = capacity_scope_from_disks(disks);
    if scope.disks.is_empty() {
        return;
    }

    record_global_dirty_scope(scope.clone());

    if let Some(token) = scope_token {
        record_capacity_scope(token, scope);
    }
}

/// Get the duplex buffer size from environment variable or use default.
///
/// This function reads `RUSTFS_DUPLEX_BUFFER_SIZE` environment variable
/// to allow runtime configuration of the duplex pipe buffer size.
/// A larger buffer (e.g., 4MB) helps prevent backpressure-related hangs
/// when reading large objects (20-26MB) under high concurrency.
///
/// Default: 4MB (4 * 1024 * 1024 bytes)
/// Get duplex buffer size from environment variable.
///
/// **Deprecated**: Use `adaptive_duplex_buffer_size()` for object-size-aware sizing.
pub fn get_duplex_buffer_size() -> usize {
    rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_DUPLEX_BUFFER_SIZE,
        rustfs_config::DEFAULT_OBJECT_DUPLEX_BUFFER_SIZE,
    )
}

/// Get adaptive duplex buffer size based on object size.
///
/// Smaller objects get smaller buffers to reduce memory waste.
/// Larger objects get larger buffers to prevent backpressure.
fn adaptive_duplex_buffer_size(object_size: i64) -> usize {
    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;
    match object_size {
        0..=1_048_576 => 64 * KB,           // <= 1MB: 64KB
        1_048_577..=16_777_216 => MB,       // <= 16MB: 1MB
        16_777_217..=268_435_456 => 4 * MB, // <= 256MB: 4MB
        _ => 8 * MB,                        // > 256MB: 8MB
    }
}

// ============================================================================
// GET Optimization Configuration
//
// All GET performance optimization flags are consolidated here.
// Each flag uses `OnceLock` for caching — env var changes require process restart.
// Each flag has a corresponding `*_ROLLOUT_PCT` for percentage-based gradual rollout.
// ============================================================================

const DISK_ONLINE_TIMEOUT: Duration = Duration::from_secs(1);
const DISK_HEALTH_CACHE_TTL: Duration = Duration::from_millis(750);
const GET_OBJECT_METADATA_CACHE_TTL: Duration = Duration::from_secs(2); // Increased from 250ms to 2s
const DEFAULT_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES: usize = 4096; // Increased from 1024 to 4096
const ENV_RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES: &str = "RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES";

// --- Codec Streaming Configuration ---

const ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE: &str = "RUSTFS_GET_CODEC_STREAMING_ENABLE";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_ENABLE: bool = false; // Disabled until rollout gates are ready

const ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE: &str = "RUSTFS_GET_CODEC_STREAMING_MIN_SIZE";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE: usize = MI_B;
const ENV_RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE: &str = "RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE: usize = MI_B;

const ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE: &str = "RUSTFS_GET_CODEC_STREAMING_ENGINE";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_ENGINE: &str = GET_CODEC_STREAMING_ENGINE_LEGACY;

const ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT: &str = "RUSTFS_GET_CODEC_STREAMING_ROLLOUT";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_ROLLOUT: &str = "off";

const ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED: &str = "RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED";
const ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED: &str = "RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED";

const ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT: &str = "RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT: u32 = 100;

const ENV_RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE: &str = "RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE: bool = false;

const ENV_RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS: &str = "RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS: usize = 256;

const ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE: &str = "RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE: bool = false;
const ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_MAX_SIZE: &str = "RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_MAX_SIZE";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_MAX_SIZE: usize = 512 * 1024;

const ENV_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY: &str = "RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY";
const DEFAULT_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY: bool = false;
const ENV_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD: &str = "RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD";
const DEFAULT_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD: usize = 128 * 1024;

// --- Metadata Early-Stop Configuration ---

const ENV_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE: &str = "RUSTFS_GET_METADATA_EARLY_STOP_ENABLE";
// Enabled by default (backlog#872): the early-stop path only engages for
// requests `should_allow_metadata_early_stop` classifies as safe (metadata-only
// reads without version_id / healing / free-version needs) and still requires
// a full read-quorum agreement before stopping. Set the env var to `false` to
// fall back to full-wait metadata fanout.
const DEFAULT_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE: bool = true;

const ENV_RUSTFS_GET_METADATA_EARLY_STOP_ROLLOUT_PCT: &str = "RUSTFS_GET_METADATA_EARLY_STOP_ROLLOUT_PCT";
const DEFAULT_RUSTFS_GET_METADATA_EARLY_STOP_ROLLOUT_PCT: u32 = 100;

const ENV_RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE: &str = "RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE";
const DEFAULT_RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE: bool = false;

// --- Multipart Reader-Setup Prefetch Configuration (backlog#870) ---

const ENV_RUSTFS_GET_MULTIPART_READER_SETUP_PREFETCH: &str = "RUSTFS_GET_MULTIPART_READER_SETUP_PREFETCH";
const DEFAULT_RUSTFS_GET_MULTIPART_READER_SETUP_PREFETCH: bool = true;

static OBJECT_LOCK_DIAG_ENABLED: OnceLock<bool> = OnceLock::new();

mod core;
mod ctx;
mod metadata;
mod ops;
mod read;
mod replication;
pub(crate) mod shard_source;

/// Get lock acquire timeout from environment variable RUSTFS_LOCK_ACQUIRE_TIMEOUT (in seconds)
/// Defaults to 30 seconds if not set or invalid
/// Lock acquisition timeout. Cached: this is consulted on every object
/// lock acquisition and `std::env::var` takes a process-global lock. In test
/// builds the env var is read directly so `temp_env` overrides take effect.
pub fn get_lock_acquire_timeout() -> Duration {
    #[cfg(test)]
    {
        Duration::from_secs(rustfs_utils::get_env_u64(
            rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT,
            rustfs_config::DEFAULT_OBJECT_LOCK_ACQUIRE_TIMEOUT,
        ))
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<Duration> = OnceLock::new();
        *CACHED.get_or_init(|| {
            Duration::from_secs(rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT,
                rustfs_config::DEFAULT_OBJECT_LOCK_ACQUIRE_TIMEOUT,
            ))
        })
    }
}

pub fn is_object_lock_diag_enabled() -> bool {
    *OBJECT_LOCK_DIAG_ENABLED.get_or_init(|| {
        let enabled = rustfs_utils::get_env_bool(
            rustfs_config::ENV_OBJECT_LOCK_DIAG_ENABLE,
            rustfs_config::DEFAULT_OBJECT_LOCK_DIAG_ENABLE,
        );
        record_object_lock_diag_enabled(enabled);
        enabled
    })
}

pub fn get_object_lock_diag_slow_acquire_threshold() -> Duration {
    Duration::from_millis(rustfs_utils::get_env_u64(
        rustfs_config::ENV_OBJECT_LOCK_DIAG_SLOW_ACQUIRE_MS,
        rustfs_config::DEFAULT_OBJECT_LOCK_DIAG_SLOW_ACQUIRE_MS,
    ))
}

pub fn get_object_lock_diag_slow_hold_threshold() -> Duration {
    Duration::from_millis(rustfs_utils::get_env_u64(
        rustfs_config::ENV_OBJECT_LOCK_DIAG_SLOW_HOLD_MS,
        rustfs_config::DEFAULT_OBJECT_LOCK_DIAG_SLOW_HOLD_MS,
    ))
}

/// Check if lock optimization is enabled.
/// When enabled, fully materialized reads may release the read lock before
/// returning to the caller. Streaming reads keep the lock until EOF or drop.
///
/// **Note**: Cached via `OnceLock` in production — env var changes require
/// process restart. In test builds the env var is read directly so that
/// `temp_env` overrides take effect.
pub fn is_lock_optimization_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE,
            rustfs_config::DEFAULT_OBJECT_LOCK_OPTIMIZATION_ENABLE,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<bool> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(
                rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE,
                rustfs_config::DEFAULT_OBJECT_LOCK_OPTIMIZATION_ENABLE,
            )
        })
    }
}

/// Check if deadlock detection is enabled.
/// When enabled, lock operations are recorded for deadlock analysis.
///
/// **Note**: Cached via `OnceLock` — env var changes require process restart.
pub fn is_deadlock_detection_enabled() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| {
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_OBJECT_DEADLOCK_DETECTION_ENABLE,
            rustfs_config::DEFAULT_OBJECT_DEADLOCK_DETECTION_ENABLE,
        )
    })
}

// ============================================================================
// GET Optimization Flag Functions
//
// All functions use `OnceLock` for caching. Environment variable changes
// require process restart to take effect.
// ============================================================================

/// Check if codec streaming is enabled (base flag).
///
/// **Note**: Cached via `OnceLock` — env var changes require process restart.
/// In test mode, bypasses cache to allow per-test env var overrides.
fn is_get_codec_streaming_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, DEFAULT_RUSTFS_GET_CODEC_STREAMING_ENABLE)
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<bool> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, DEFAULT_RUSTFS_GET_CODEC_STREAMING_ENABLE)
        })
    }
}

/// Check if multipart codec streaming is enabled.
///
/// When enabled, multipart objects use per-part codec streaming
/// instead of falling back to the legacy duplex path.
fn is_codec_streaming_multipart_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(
            ENV_RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE,
            DEFAULT_RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<bool> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(
                ENV_RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE,
                DEFAULT_RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE,
            )
        })
    }
}

fn get_codec_streaming_multipart_max_parts() -> usize {
    #[cfg(test)]
    {
        rustfs_utils::get_env_usize(
            ENV_RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS,
            DEFAULT_RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<usize> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_usize(
                ENV_RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS,
                DEFAULT_RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS,
            )
        })
    }
}

/// Check if metadata early-stop is enabled (base flag).
///
/// **Note**: Cached via `OnceLock` in production. In test builds the env var
/// is read directly so that `temp_env` overrides take effect.
fn is_get_metadata_early_stop_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(ENV_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE, DEFAULT_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE)
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<bool> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(ENV_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE, DEFAULT_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE)
        })
    }
}

/// Check if version-aware early-stop is enabled.
///
/// When enabled, versioned requests can early-stop when the requested
/// version_id reaches quorum across disks.
///
/// **Note**: Cached via `OnceLock` in production. In test builds the env var
/// is read directly so that `temp_env` overrides take effect.
fn is_version_early_stop_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(
            ENV_RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE,
            DEFAULT_RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<bool> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(
                ENV_RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE,
                DEFAULT_RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE,
            )
        })
    }
}

/// Check if multipart reads prefetch the next part's bitrot reader setup
/// while the current part decodes (backlog#870).
///
/// **Note**: Cached via `OnceLock` in production. In test builds the env var
/// is read directly so that `temp_env` overrides take effect.
fn is_multipart_reader_setup_prefetch_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(
            ENV_RUSTFS_GET_MULTIPART_READER_SETUP_PREFETCH,
            DEFAULT_RUSTFS_GET_MULTIPART_READER_SETUP_PREFETCH,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<bool> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(
                ENV_RUSTFS_GET_MULTIPART_READER_SETUP_PREFETCH,
                DEFAULT_RUSTFS_GET_MULTIPART_READER_SETUP_PREFETCH,
            )
        })
    }
}

// --- Rollout Percentage Functions ---

fn get_codec_streaming_rollout_pct() -> u32 {
    #[cfg(test)]
    {
        rustfs_utils::get_env_u32(ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT, DEFAULT_RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT)
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<u32> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_u32(ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT, DEFAULT_RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT)
        })
    }
}

fn get_metadata_early_stop_rollout_pct() -> u32 {
    static CACHED: OnceLock<u32> = OnceLock::new();
    *CACHED.get_or_init(|| {
        rustfs_utils::get_env_u32(
            ENV_RUSTFS_GET_METADATA_EARLY_STOP_ROLLOUT_PCT,
            DEFAULT_RUSTFS_GET_METADATA_EARLY_STOP_ROLLOUT_PCT,
        )
    })
}

// --- Request-Level Decision Functions ---

/// Determine if an optimization should be enabled for a specific request.
///
/// Uses a stable hash of `(bucket, object)` to ensure the same object
/// always gets consistent behavior. This enables percentage-based gradual rollout.
fn is_optimization_enabled_for_request(base_enabled: bool, rollout_pct: u32, bucket: &str, object: &str) -> bool {
    if !base_enabled || rollout_pct == 0 {
        return false;
    }
    if rollout_pct >= 100 {
        return true;
    }

    // Stable hash: same (bucket, object) always produces the same result
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bucket.hash(&mut hasher);
    object.hash(&mut hasher);
    let hash = hasher.finish() % 100;

    (hash as u32) < rollout_pct
}
/// Should this specific request use codec streaming?
pub fn should_use_codec_streaming(bucket: &str, object: &str) -> bool {
    let base = is_get_codec_streaming_enabled();
    let pct = get_codec_streaming_rollout_pct();
    is_optimization_enabled_for_request(base, pct, bucket, object)
}

/// Should this specific request use metadata early-stop?
pub fn should_use_metadata_early_stop(bucket: &str, object: &str) -> bool {
    let base = is_get_metadata_early_stop_enabled();
    let pct = get_metadata_early_stop_rollout_pct();
    is_optimization_enabled_for_request(base, pct, bucket, object)
}

fn get_codec_streaming_min_size() -> usize {
    if std::env::var_os(ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE).is_some() {
        return rustfs_utils::get_env_usize(ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, DEFAULT_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE);
    }

    match get_codec_streaming_engine() {
        GetCodecStreamingEngine::Rustfs => rustfs_utils::get_env_usize(
            ENV_RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE,
            DEFAULT_RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE,
        ),
        GetCodecStreamingEngine::Legacy => DEFAULT_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE,
    }
}

fn is_get_codec_streaming_data_blocks_first_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(
            ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE,
            DEFAULT_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<bool> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(
                ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE,
                DEFAULT_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE,
            )
        })
    }
}

fn get_codec_streaming_data_blocks_first_max_size() -> usize {
    #[cfg(test)]
    {
        rustfs_utils::get_env_usize(
            ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_MAX_SIZE,
            DEFAULT_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_MAX_SIZE,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<usize> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_usize(
                ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_MAX_SIZE,
                DEFAULT_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_MAX_SIZE,
            )
        })
    }
}

fn get_object_metadata_cache_max_entries() -> usize {
    #[cfg(test)]
    {
        rustfs_utils::get_env_usize(
            ENV_RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES,
            DEFAULT_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES,
        )
        .max(1)
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<usize> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_usize(
                ENV_RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES,
                DEFAULT_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES,
            )
            .max(1)
        })
    }
}

fn is_get_small_object_direct_memory_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(ENV_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY, DEFAULT_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY)
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<bool> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(ENV_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY, DEFAULT_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY)
        })
    }
}

fn get_small_object_direct_memory_threshold() -> usize {
    #[cfg(test)]
    {
        rustfs_utils::get_env_usize(
            ENV_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD,
            DEFAULT_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<usize> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_usize(
                ENV_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD,
                DEFAULT_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD,
            )
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetCodecStreamingEngine {
    Legacy,
    Rustfs,
}

fn get_codec_streaming_engine() -> GetCodecStreamingEngine {
    let engine = rustfs_utils::get_env_str(ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, DEFAULT_RUSTFS_GET_CODEC_STREAMING_ENGINE);
    match engine.trim() {
        value if value.eq_ignore_ascii_case(GET_CODEC_STREAMING_ENGINE_RUSTFS) => GetCodecStreamingEngine::Rustfs,
        value if value.eq_ignore_ascii_case(GET_CODEC_STREAMING_ENGINE_LEGACY) => GetCodecStreamingEngine::Legacy,
        _ => GetCodecStreamingEngine::Legacy,
    }
}

fn get_codec_streaming_rollout() -> GetCodecStreamingRollout {
    let rollout = rustfs_utils::get_env_str(ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, DEFAULT_RUSTFS_GET_CODEC_STREAMING_ROLLOUT);
    match rollout.trim() {
        value if value.eq_ignore_ascii_case("internal") => GetCodecStreamingRollout::Internal,
        value if value.eq_ignore_ascii_case("benchmark") => GetCodecStreamingRollout::Benchmark,
        _ => GetCodecStreamingRollout::Off,
    }
}

fn is_get_codec_streaming_body_compat_confirmed() -> bool {
    rustfs_utils::get_env_bool(ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, false)
}

fn is_get_codec_streaming_header_compat_confirmed() -> bool {
    rustfs_utils::get_env_bool(ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, false)
}

fn build_get_codec_streaming_decode_engine(erasure: coding::Erasure) -> std::io::Result<CodecStreamingDecodeEngine> {
    match get_codec_streaming_engine() {
        GetCodecStreamingEngine::Legacy => Ok(CodecStreamingDecodeEngine::legacy(erasure)),
        GetCodecStreamingEngine::Rustfs => CodecStreamingDecodeEngine::rustfs(&erasure),
    }
}

fn get_codec_streaming_metrics_path() -> &'static str {
    match get_codec_streaming_engine() {
        GetCodecStreamingEngine::Legacy => GET_OBJECT_PATH_CODEC_STREAMING_LEGACY_ENGINE,
        GetCodecStreamingEngine::Rustfs => GET_OBJECT_PATH_CODEC_STREAMING_RUSTFS_ENGINE,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetCodecStreamingDecision {
    Use,
    Fallback(GetCodecStreamingFallbackReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetCodecStreamingRollout {
    Off,
    Internal,
    Benchmark,
}

impl GetCodecStreamingRollout {
    const fn is_opted_in(self) -> bool {
        !matches!(self, Self::Off)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetCodecStreamingFallbackReason {
    Disabled,
    RolloutNotOptedIn,
    RolloutPctNotSelected,
    BodyCompatibilityUnconfirmed,
    HeaderCompatibilityUnconfirmed,
    LockOptimizationDisabled,
    Range,
    BelowMinSize,
    Encrypted,
    Compressed,
    Remote,
    Multipart,
    InvalidMinSize,
    ReadQuorumNotSafe,
    MultipartPartLimit,
}

impl GetCodecStreamingFallbackReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::RolloutNotOptedIn => "rollout_not_opted_in",
            Self::RolloutPctNotSelected => "rollout_pct_not_selected",
            Self::BodyCompatibilityUnconfirmed => "body_compatibility_unconfirmed",
            Self::HeaderCompatibilityUnconfirmed => "header_compatibility_unconfirmed",
            Self::LockOptimizationDisabled => "lock_optimization_disabled",
            Self::Range => "range",
            Self::BelowMinSize => "below_min_size",
            Self::Encrypted => "encrypted",
            Self::Compressed => "compressed",
            Self::Remote => "remote",
            Self::Multipart => "multipart",
            Self::InvalidMinSize => "invalid_min_size",
            Self::ReadQuorumNotSafe => "read_quorum_not_safe",
            Self::MultipartPartLimit => "multipart_part_limit",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetCodecStreamingObjectClass {
    PlainSinglePart,
    Range,
    Encrypted,
    Compressed,
    Remote,
    Multipart,
}

impl GetCodecStreamingObjectClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PlainSinglePart => crate::diagnostics::get::GET_CODEC_STREAMING_OBJECT_CLASS_PLAIN_SINGLE_PART,
            Self::Range => crate::diagnostics::get::GET_CODEC_STREAMING_OBJECT_CLASS_RANGE,
            Self::Encrypted => crate::diagnostics::get::GET_CODEC_STREAMING_OBJECT_CLASS_ENCRYPTED,
            Self::Compressed => crate::diagnostics::get::GET_CODEC_STREAMING_OBJECT_CLASS_COMPRESSED,
            Self::Remote => crate::diagnostics::get::GET_CODEC_STREAMING_OBJECT_CLASS_REMOTE,
            Self::Multipart => crate::diagnostics::get::GET_CODEC_STREAMING_OBJECT_CLASS_MULTIPART,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GetCodecStreamingGate {
    object_class: GetCodecStreamingObjectClass,
    decision: GetCodecStreamingDecision,
    prefer_data_blocks_first_reader_setup: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetDirectMemoryDecision {
    Use { object_size: usize },
    Fallback(GetDirectMemoryFallbackReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetDirectMemoryFallbackReason {
    Disabled,
    ThresholdZero,
    Range,
    PartNumber,
    VersionId,
    Versioned,
    VersionSuspended,
    InclFreeVersions,
    SkipFreeVersion,
    DataMovement,
    RawDataMovementRead,
    DeleteMarker,
    MetadataOnly,
    VersionOnly,
    Encrypted,
    Compressed,
    Remote,
    ObjectInfoMultipart,
    FileInfoMultipart,
    InvalidSize,
    SizeMismatch,
    AboveThreshold,
}

impl GetDirectMemoryFallbackReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::ThresholdZero => "threshold_zero",
            Self::Range => "range",
            Self::PartNumber => "part_number",
            Self::VersionId => "version_id",
            Self::Versioned => "versioned",
            Self::VersionSuspended => "version_suspended",
            Self::InclFreeVersions => "incl_free_versions",
            Self::SkipFreeVersion => "skip_free_version",
            Self::DataMovement => "data_movement",
            Self::RawDataMovementRead => "raw_data_movement_read",
            Self::DeleteMarker => "delete_marker",
            Self::MetadataOnly => "metadata_only",
            Self::VersionOnly => "version_only",
            Self::Encrypted => "encrypted",
            Self::Compressed => "compressed",
            Self::Remote => "remote",
            Self::ObjectInfoMultipart => "object_info_multipart",
            Self::FileInfoMultipart => "file_info_multipart",
            Self::InvalidSize => "invalid_size",
            Self::SizeMismatch => "size_mismatch",
            Self::AboveThreshold => "above_threshold",
        }
    }
}

fn record_get_codec_streaming_gate_decision(
    object_class: GetCodecStreamingObjectClass,
    decision: GetCodecStreamingDecision,
    size_bucket: &'static str,
) {
    let (outcome, reason) = match decision {
        GetCodecStreamingDecision::Use => (
            crate::diagnostics::get::GET_CODEC_STREAMING_DECISION_USE,
            crate::diagnostics::get::GET_CODEC_STREAMING_REASON_NONE,
        ),
        GetCodecStreamingDecision::Fallback(reason) => {
            (crate::diagnostics::get::GET_CODEC_STREAMING_DECISION_FALLBACK, reason.as_str())
        }
    };
    let object_class = object_class.as_str();
    rustfs_io_metrics::record_get_object_codec_streaming_decision(outcome, object_class, reason);
    rustfs_io_metrics::record_get_object_codec_streaming_decision_by_size(outcome, object_class, reason, size_bucket);
}

fn record_get_direct_memory_decision(
    object_class: GetCodecStreamingObjectClass,
    decision: GetDirectMemoryDecision,
    size_bucket: &'static str,
) {
    let (outcome, reason) = match decision {
        GetDirectMemoryDecision::Use { .. } => (
            crate::diagnostics::get::GET_DIRECT_MEMORY_DECISION_USE,
            crate::diagnostics::get::GET_DIRECT_MEMORY_REASON_NONE,
        ),
        GetDirectMemoryDecision::Fallback(reason) => {
            (crate::diagnostics::get::GET_DIRECT_MEMORY_DECISION_FALLBACK, reason.as_str())
        }
    };
    rustfs_io_metrics::record_get_object_direct_memory_decision(outcome, object_class.as_str(), reason, size_bucket);
}

fn record_get_object_reader_path_observation(
    path: &'static str,
    object_class: GetCodecStreamingObjectClass,
    size_bucket: &'static str,
) {
    rustfs_io_metrics::record_get_object_reader_path(path);
    rustfs_io_metrics::record_get_object_reader_path_by_size(path, object_class.as_str(), size_bucket);
}

fn classify_get_codec_streaming_object_class(
    range: &Option<HTTPRangeSpec>,
    object_info: &ObjectInfo,
    fi: &FileInfo,
) -> GetCodecStreamingObjectClass {
    if range.is_some() {
        return GetCodecStreamingObjectClass::Range;
    }
    if object_info.is_encrypted() {
        return GetCodecStreamingObjectClass::Encrypted;
    }
    if object_info.is_compressed() {
        return GetCodecStreamingObjectClass::Compressed;
    }
    if object_info.is_remote() {
        return GetCodecStreamingObjectClass::Remote;
    }
    if fi.parts.len() != 1 {
        return GetCodecStreamingObjectClass::Multipart;
    }
    GetCodecStreamingObjectClass::PlainSinglePart
}

fn is_get_small_object_direct_memory_eligible_with_threshold(
    range: &Option<HTTPRangeSpec>,
    object_info: &ObjectInfo,
    fi: &FileInfo,
    opts: &ObjectOptions,
    threshold: usize,
) -> bool {
    matches!(
        get_small_object_direct_memory_decision_with_threshold(range, object_info, fi, opts, true, threshold),
        GetDirectMemoryDecision::Use { .. }
    )
}

fn get_small_object_direct_memory_decision_with_threshold(
    range: &Option<HTTPRangeSpec>,
    object_info: &ObjectInfo,
    fi: &FileInfo,
    opts: &ObjectOptions,
    enabled: bool,
    threshold: usize,
) -> GetDirectMemoryDecision {
    if !enabled {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::Disabled);
    }
    if threshold == 0 {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::ThresholdZero);
    }
    if range.is_some() {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::Range);
    }
    if opts.part_number.is_some() {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::PartNumber);
    }
    if opts.version_id.is_some() {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::VersionId);
    }
    if opts.versioned {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::Versioned);
    }
    if opts.version_suspended {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::VersionSuspended);
    }
    if opts.incl_free_versions {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::InclFreeVersions);
    }
    if opts.skip_free_version {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::SkipFreeVersion);
    }
    if opts.data_movement {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::DataMovement);
    }
    if opts.raw_data_movement_read {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::RawDataMovementRead);
    }
    if object_info.delete_marker {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::DeleteMarker);
    }
    if object_info.metadata_only {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::MetadataOnly);
    }
    if object_info.version_only {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::VersionOnly);
    }
    if object_info.is_encrypted() {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::Encrypted);
    }
    if object_info.is_compressed() {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::Compressed);
    }
    if object_info.is_remote() {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::Remote);
    }
    if object_info.parts.len() != 1 {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::ObjectInfoMultipart);
    }
    if fi.parts.len() != 1 {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::FileInfoMultipart);
    }

    let Ok(object_size) = usize::try_from(fi.size) else {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::InvalidSize);
    };
    if object_size == 0 {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::InvalidSize);
    }
    if object_info.size != fi.size {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::SizeMismatch);
    }
    if object_size > threshold {
        return GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::AboveThreshold);
    }

    GetDirectMemoryDecision::Use { object_size }
}

fn get_small_object_direct_memory_decision(
    range: &Option<HTTPRangeSpec>,
    object_info: &ObjectInfo,
    fi: &FileInfo,
    opts: &ObjectOptions,
) -> GetDirectMemoryDecision {
    get_small_object_direct_memory_decision_with_threshold(
        range,
        object_info,
        fi,
        opts,
        is_get_small_object_direct_memory_enabled(),
        get_small_object_direct_memory_threshold(),
    )
}

fn should_prefer_codec_streaming_data_blocks_first_reader_setup(
    object_class: GetCodecStreamingObjectClass,
    object_size: i64,
) -> bool {
    if !is_get_codec_streaming_data_blocks_first_enabled()
        || object_class != GetCodecStreamingObjectClass::PlainSinglePart
        || object_size <= 0
    {
        return false;
    }

    let Ok(object_size) = usize::try_from(object_size) else {
        return false;
    };
    let max_size = get_codec_streaming_data_blocks_first_max_size();
    max_size > 0 && object_size <= max_size
}

fn get_codec_streaming_reader_gate(
    bucket: &str,
    object: &str,
    range: &Option<HTTPRangeSpec>,
    object_info: &ObjectInfo,
    fi: &FileInfo,
    lock_optimization_enabled: bool,
) -> GetCodecStreamingGate {
    let object_class = classify_get_codec_streaming_object_class(range, object_info, fi);

    if !is_get_codec_streaming_enabled() {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Disabled),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if !get_codec_streaming_rollout().is_opted_in() {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::RolloutNotOptedIn),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if !should_use_codec_streaming(bucket, object) {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::RolloutPctNotSelected),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if !is_get_codec_streaming_body_compat_confirmed() {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::BodyCompatibilityUnconfirmed),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if !is_get_codec_streaming_header_compat_confirmed() {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::HeaderCompatibilityUnconfirmed),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if object_class == GetCodecStreamingObjectClass::Range {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Range),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if !lock_optimization_enabled {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::LockOptimizationDisabled),
            prefer_data_blocks_first_reader_setup: false,
        };
    }

    let Ok(min_size) = i64::try_from(get_codec_streaming_min_size()) else {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::InvalidMinSize),
            prefer_data_blocks_first_reader_setup: false,
        };
    };
    if object_info.size < min_size {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::BelowMinSize),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if object_class == GetCodecStreamingObjectClass::Encrypted {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Encrypted),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if object_class == GetCodecStreamingObjectClass::Compressed {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Compressed),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if object_class == GetCodecStreamingObjectClass::Remote {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Remote),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if object_class == GetCodecStreamingObjectClass::Multipart {
        if !is_codec_streaming_multipart_enabled() {
            return GetCodecStreamingGate {
                object_class,
                decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Multipart),
                prefer_data_blocks_first_reader_setup: false,
            };
        }
        if fi.parts.len() > get_codec_streaming_multipart_max_parts() {
            return GetCodecStreamingGate {
                object_class,
                decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::MultipartPartLimit),
                prefer_data_blocks_first_reader_setup: false,
            };
        }
    }

    GetCodecStreamingGate {
        object_class,
        decision: GetCodecStreamingDecision::Use,
        prefer_data_blocks_first_reader_setup: should_prefer_codec_streaming_data_blocks_first_reader_setup(
            object_class,
            object_info.size,
        ),
    }
}

fn is_confirmed_complete_part_missing(err: &str) -> bool {
    err.contains("file not found")
        || err.contains("Specified part could not be found")
        || (err.starts_with("part.") && err.ends_with(" not found"))
}

fn complete_multipart_part_error(part_number: usize, err: &str, bucket: &str, object: &str) -> Error {
    if is_confirmed_complete_part_missing(err) {
        return Error::InvalidPart(part_number, bucket.to_owned(), object.to_owned());
    }

    to_object_err(Error::ErasureReadQuorum, vec![bucket, object])
}

fn complete_multipart_part_error_result(err: &Error) -> &'static str {
    match err {
        Error::InvalidPart(_, _, _) => COMPLETE_MULTIPART_PART_MISSING,
        Error::ErasureReadQuorum | Error::InsufficientReadQuorum(_, _) => COMPLETE_MULTIPART_PART_READ_QUORUM_UNAVAILABLE,
        _ => COMPLETE_MULTIPART_PART_ERROR,
    }
}

/// Record a lock acquisition for deadlock detection.
/// This records detailed lock information for deadlock analysis.
/// Returns the lock_id for later release tracking.
#[inline]
fn record_lock_acquire(bucket: &str, object: &str, lock_type: &str) -> String {
    let lock_id = format!("{}:{}", bucket, object);

    if !is_deadlock_detection_enabled() {
        return lock_id;
    }

    let request_id = format!("get-{}-{}", bucket, object);
    let resource = format!("{}/{}", bucket, object);

    // Log with structured fields for analysis
    debug!(
        request_id = %request_id,
        lock_id = %lock_id,
        lock_type = %lock_type,
        resource = %resource,
        "Lock acquired for deadlock tracking"
    );

    lock_id
}

/// Record a lock release for deadlock detection.
#[inline]
fn record_lock_release(bucket: &str, object: &str, lock_id: &str, lock_type: &str) {
    if !is_deadlock_detection_enabled() {
        return;
    }

    let request_id = format!("get-{}-{}", bucket, object);

    debug!(
        request_id = %request_id,
        lock_id = %lock_id,
        lock_type = %lock_type,
        "Lock released for deadlock tracking"
    );
}

#[derive(Clone, Copy, Debug)]
pub(super) struct MultipartWriteQuorumContext<'a> {
    stage: &'static str,
    bucket: &'a str,
    object: &'a str,
    upload_id: &'a str,
    part_number: Option<usize>,
}

fn log_multipart_write_quorum_failure(
    context: MultipartWriteQuorumContext<'_>,
    errs: &[Option<DiskError>],
    write_quorum: usize,
    returned_error: &DiskError,
) {
    let summary = build_write_quorum_failure_summary(errs, OBJECT_OP_IGNORED_ERRS, write_quorum);
    runtime_sources::record_erasure_write_quorum_failure(context.stage, summary.dominant_error_label);
    warn!(
        target: "rustfs_ecstore::set_disk",
        event = EVENT_SET_DISK_MULTIPART,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_SET_DISK,
        op = "upload_part",
        state = "write_quorum_unavailable",
        stage = context.stage,
        bucket = %context.bucket,
        object = %context.object,
        upload_id = %context.upload_id,
        part_number = context.part_number,
        required = summary.required,
        achieved = summary.achieved,
        failed = summary.failed,
        total = summary.total,
        offline_disks = summary.offline_disks,
        retryable_failures = summary.retryable_failures,
        dominant_error = summary.dominant_error_label,
        returned_error = %returned_error,
        "Set disk multipart write quorum unavailable"
    );
}

fn issue3031_diag_enabled() -> bool {
    rustfs_utils::get_env_bool(ENV_ISSUE3031_DIAG_ENABLE, false)
}

fn build_tiered_decommission_file_info(
    bucket: &str,
    object: &str,
    fi: &FileInfo,
    disk_count: usize,
    default_parity_count: usize,
    storage_class: Option<&str>,
) -> (FileInfo, usize) {
    let parity_drives = runtime_sources::storage_class_parity(storage_class).unwrap_or(default_parity_count);
    let data_drives = disk_count - parity_drives;
    let mut write_quorum = data_drives;
    if data_drives == parity_drives {
        write_quorum += 1;
    }

    let mut updated = fi.clone();
    updated.erasure = FileInfo::new([bucket, object].join("/").as_str(), data_drives, parity_drives).erasure;

    (updated, write_quorum)
}

fn resolve_tiered_decommission_write_quorum_result(
    errs: &[Option<DiskError>],
    write_quorum: usize,
    bucket: &str,
    object: &str,
) -> Result<()> {
    if let Some(err) = reduce_write_quorum_errs(errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
        return Err(to_object_err(err.into(), vec![bucket, object]));
    }

    Ok(())
}

#[derive(Clone, Debug)]
pub struct SetDisks {
    pub locker_owner: String,
    pub disks: Arc<RwLock<Vec<Option<DiskStore>>>>,
    pub set_endpoints: Vec<Endpoint>,
    pub set_drive_count: usize,
    pub default_parity_count: usize,
    pub set_index: usize,
    pub pool_index: usize,
    pub format: FormatV3,
    disk_health_cache: Arc<RwLock<Vec<Option<DiskHealthEntry>>>>,
    get_object_metadata_cache: moka::future::Cache<GetObjectMetadataCacheKey, Arc<GetObjectMetadataCacheEntry>>,
    pub lockers: Vec<Arc<dyn LockClient>>,
    local_lock_manager: Arc<rustfs_lock::GlobalLockManager>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GetObjectMetadataCacheKey {
    bucket: String,
    object: String,
}

impl GetObjectMetadataCacheKey {
    fn new(bucket: &str, object: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
struct GetObjectMetadataCacheEntry {
    #[allow(dead_code)] // Kept for debugging; moka handles TTL internally
    created_at: Instant,
    fi: FileInfo,
    parts_metadata: Vec<FileInfo>,
    online_disks: Vec<Option<DiskStore>>,
    read_quorum: usize,
}

#[derive(Clone, Debug)]
struct DiskHealthEntry {
    last_check: Instant,
    online: bool,
}

impl DiskHealthEntry {
    fn cached_value(&self) -> Option<bool> {
        if self.last_check.elapsed() <= DISK_HEALTH_CACHE_TTL {
            Some(self.online)
        } else {
            None
        }
    }
}

impl SetDisks {
    async fn invalidate_get_object_metadata_cache(&self, bucket: &str, object: &str) {
        self.get_object_metadata_cache
            .invalidate(&GetObjectMetadataCacheKey::new(bucket, object))
            .await;
    }

    async fn acquire_read_lock_diag(&self, op: &'static str, bucket: &str, object: &str) -> Result<ObjectLockDiagGuard> {
        let diag_enabled = is_object_lock_diag_enabled();
        let ns_lock = self.new_ns_lock(bucket, object).await?;
        let acquire_start = Instant::now();
        let guard = ns_lock
            .get_read_lock(get_lock_acquire_timeout())
            .await
            .map_err(|e| self.map_namespace_lock_error(bucket, object, "read", e))?;
        let owner = diag_enabled.then(|| ns_lock.owner().to_string());
        self.log_object_lock_acquire_if_slow(op, bucket, object, "read", owner.as_deref(), acquire_start.elapsed(), diag_enabled);
        Ok(ObjectLockDiagGuard::new(
            guard,
            diag_enabled,
            op,
            diag_enabled.then(|| bucket.to_string()),
            diag_enabled.then(|| object.to_string()),
            owner,
            "read",
        ))
    }

    async fn acquire_write_lock_diag(&self, op: &'static str, bucket: &str, object: &str) -> Result<ObjectLockDiagGuard> {
        let diag_enabled = is_object_lock_diag_enabled();
        let ns_lock = self.new_ns_lock(bucket, object).await?;
        let acquire_start = Instant::now();
        let guard = ns_lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(|e| self.map_namespace_lock_error(bucket, object, "write", e))?;
        let owner = diag_enabled.then(|| ns_lock.owner().to_string());
        self.log_object_lock_acquire_if_slow(
            op,
            bucket,
            object,
            "write",
            owner.as_deref(),
            acquire_start.elapsed(),
            diag_enabled,
        );
        Ok(ObjectLockDiagGuard::new(
            guard,
            diag_enabled,
            op,
            diag_enabled.then(|| bucket.to_string()),
            diag_enabled.then(|| object.to_string()),
            owner,
            "write",
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn log_object_lock_acquire_if_slow(
        &self,
        op: &'static str,
        bucket: &str,
        object: &str,
        mode: &'static str,
        owner: Option<&str>,
        elapsed: Duration,
        diag_enabled: bool,
    ) {
        if !diag_enabled {
            return;
        }

        let threshold = get_object_lock_diag_slow_acquire_threshold();
        record_object_lock_diag_acquire_duration(op, mode, elapsed);
        if elapsed >= threshold {
            record_object_lock_diag_slow_acquire(op, mode);
            warn!(
                target: "rustfs_ecstore::object_lock_diag",
                op,
                bucket,
                object,
                mode,
                owner = owner.unwrap_or_default(),
                acquire_ms = elapsed.as_millis(),
                threshold_ms = threshold.as_millis(),
                "object namespace lock acquisition exceeded threshold"
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        locker_owner: String,
        disks: Arc<RwLock<Vec<Option<DiskStore>>>>,
        set_drive_count: usize,
        default_parity_count: usize,
        set_index: usize,
        pool_index: usize,
        set_endpoints: Vec<Endpoint>,
        format: FormatV3,
        lockers: Vec<Arc<dyn LockClient>>,
    ) -> Arc<Self> {
        Arc::new(SetDisks {
            locker_owner,
            disks,
            set_drive_count,
            default_parity_count,
            set_index,
            pool_index,
            format,
            set_endpoints,
            disk_health_cache: Arc::new(RwLock::new(Vec::new())),
            get_object_metadata_cache: moka::future::Cache::builder()
                .max_capacity(get_object_metadata_cache_max_entries() as u64)
                .time_to_live(GET_OBJECT_METADATA_CACHE_TTL)
                .build(),
            lockers,
            local_lock_manager: runtime_sources::global_lock_manager(),
        })
    }

    // async fn cached_disk_health(&self, index: usize) -> Option<bool> {
    //     let cache = self.disk_health_cache.read().await;
    //     cache
    //         .get(index)
    //         .and_then(|entry| entry.as_ref().and_then(|state| state.cached_value()))
    // }

    // async fn update_disk_health(&self, index: usize, online: bool) {
    //     let mut cache = self.disk_health_cache.write().await;
    //     if cache.len() <= index {
    //         cache.resize(index + 1, None);
    //     }
    //     cache[index] = Some(DiskHealthEntry {
    //         last_check: Instant::now(),
    //         online,
    //     });
    // }

    // async fn is_disk_online_cached(&self, index: usize, disk: &DiskStore) -> bool {
    //     if let Some(online) = self.cached_disk_health(index).await {
    //         return online;
    //     }

    //     let disk_clone = disk.clone();
    //     let online = timeout(DISK_ONLINE_TIMEOUT, async move { disk_clone.is_online().await })
    //         .await
    //         .unwrap_or(false);
    //     self.update_disk_health(index, online).await;
    //     online
    // }

    // async fn filter_online_disks(&self, disks: Vec<Option<DiskStore>>) -> (Vec<Option<DiskStore>>, usize) {
    //     let mut filtered = Vec::with_capacity(disks.len());
    //     let mut online_count = 0;

    //     for (idx, disk) in disks.into_iter().enumerate() {
    //         if let Some(disk_store) = disk {
    //             if self.is_disk_online_cached(idx, &disk_store).await {
    //                 filtered.push(Some(disk_store));
    //                 online_count += 1;
    //             } else {
    //                 filtered.push(None);
    //             }
    //         } else {
    //             filtered.push(None);
    //         }
    //     }

    //     (filtered, online_count)
    // }

    // async fn write_all(disks: &[Option<DiskStore>], bucket: &str, object: &str, buff: Vec<u8>) -> Vec<Option<Error>> {
    //     let mut futures = Vec::with_capacity(disks.len());

    //     let mut errors = Vec::with_capacity(disks.len());

    //     for disk in disks.iter() {
    //         if disk.is_none() {
    //             errors.push(Some(Error::new(DiskError::DiskNotFound)));
    //             continue;
    //         }
    //         let disk = disk.as_ref().unwrap();
    //         futures.push(disk.write_all(bucket, object, buff.clone()));
    //     }

    //     let results = join_all(futures).await;
    //     for result in results {
    //         match result {
    //             Ok(_) => {
    //                 errors.push(None);
    //             }
    //             Err(e) => {
    //                 errors.push(Some(e));
    //             }
    //         }
    //     }
    //     errors
    // }

    // Returns per object readQuorum and writeQuorum
    // readQuorum is the min required disks to read data.
    // writeQuorum is the min required disks to write data.

    // Optimized version using batch processor with quorum support

    // pub async fn walk_dir(&self, opts: &WalkDirOptions) -> (Vec<Option<Vec<MetaCacheEntry>>>, Vec<Option<Error>>) {
    //     let disks = self.disks.read().await;

    //     let disks = disks.clone();
    //     let mut futures = Vec::new();
    //     let mut errs = Vec::new();
    //     let mut ress = Vec::new();

    //     for disk in disks.iter() {
    //         let opts = opts.clone();
    //         futures.push(async move {
    //             if let Some(disk) = disk {
    //                 disk.walk_dir(opts, &mut Writer::NotUse).await
    //             } else {
    //                 Err(DiskError::DiskNotFound)
    //             }
    //         });
    //     }

    //     let results = join_all(futures).await;

    //     for res in results {
    //         match res {
    //             Ok(entries) => {
    //                 ress.push(Some(entries));
    //                 errs.push(None);
    //             }
    //             Err(e) => {
    //                 ress.push(None);
    //                 errs.push(Some(e));
    //             }
    //         }
    //     }

    //     (ress, errs)
    // }

    // async fn remove_object_part(
    //     &self,
    //     bucket: &str,
    //     object: &str,
    //     upload_id: &str,
    //     data_dir: &str,
    //     part_num: usize,
    // ) -> Result<()> {
    //     let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);
    //     let disks = self.disks.read().await;

    //     let disks = disks.clone();

    //     let file_path = format!("{}/{}/part.{}", upload_id_path, data_dir, part_num);

    //     let mut futures = Vec::with_capacity(disks.len());
    //     let mut errors = Vec::with_capacity(disks.len());

    //     for disk in disks.iter() {
    //         let file_path = file_path.clone();
    //         let meta_file_path = format!("{}.meta", file_path);

    //         futures.push(async move {
    //             if let Some(disk) = disk {
    //                 disk.delete(RUSTFS_META_MULTIPART_BUCKET, &file_path, DeleteOptions::default())
    //                     .await?;
    //                 disk.delete(RUSTFS_META_MULTIPART_BUCKET, &meta_file_path, DeleteOptions::default())
    //                     .await
    //             } else {
    //                 Err(DiskError::DiskNotFound)
    //             }
    //         });
    //     }

    //     let results = join_all(futures).await;
    //     for result in results {
    //         match result {
    //             Ok(_) => {
    //                 errors.push(None);
    //             }
    //             Err(e) => {
    //                 errors.push(Some(e));
    //             }
    //         }
    //     }

    //     Ok(())
    // }
    // async fn remove_part_meta(&self, bucket: &str, object: &str, upload_id: &str, data_dir: &str, part_num: usize) -> Result<()> {
    //     let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);
    //     let disks = self.disks.read().await;

    //     let disks = disks.clone();
    //     // let disks = Self::shuffle_disks(&disks, &fi.erasure.distribution);

    //     let file_path = format!("{}/{}/part.{}.meta", upload_id_path, data_dir, part_num);

    //     let mut futures = Vec::with_capacity(disks.len());
    //     let mut errors = Vec::with_capacity(disks.len());

    //     for disk in disks.iter() {
    //         let file_path = file_path.clone();
    //         futures.push(async move {
    //             if let Some(disk) = disk {
    //                 disk.delete(RUSTFS_META_MULTIPART_BUCKET, &file_path, DeleteOptions::default())
    //                     .await
    //             } else {
    //                 Err(DiskError::DiskNotFound)
    //             }
    //         });
    //     }

    //     let results = join_all(futures).await;
    //     for result in results {
    //         match result {
    //             Ok(_) => {
    //                 errors.push(None);
    //             }
    //             Err(e) => {
    //                 errors.push(Some(e));
    //             }
    //         }
    //     }

    //     Ok(())
    // }
}

fn is_explicit_null_version(version_id: Option<Uuid>) -> bool {
    version_id == Some(Uuid::nil())
}

fn delete_file_info_version_id(version_id: Option<Uuid>) -> Option<Uuid> {
    if is_explicit_null_version(version_id) {
        None
    } else {
        version_id
    }
}

fn object_fits_single_block(object_size: i64, block_size: usize) -> bool {
    match usize::try_from(object_size) {
        Ok(size) => size > 0 && size <= block_size,
        Err(_) => false,
    }
}

fn should_use_inline_small_fast_path(is_inline_buffer: bool, object_size: i64, block_size: usize) -> bool {
    is_inline_buffer && object_fits_single_block(object_size, block_size)
}

fn should_use_single_block_non_inline_fast_path(is_inline_buffer: bool, object_size: i64, block_size: usize) -> bool {
    !is_inline_buffer && object_fits_single_block(object_size, block_size)
}

fn should_use_inline_fast_path(
    range: &Option<HTTPRangeSpec>,
    object_info: &ObjectInfo,
    fi: &FileInfo,
    opts: &ObjectOptions,
) -> bool {
    object_info.is_inline_fast_path_eligible() && fi.data.is_some() && range.is_none() && opts.part_number.is_none()
}

enum SmallWritePath {
    Inline,
    SingleBlockNonInline,
    Pipeline,
    PipelineBatchedLarge,
}

impl SmallWritePath {
    fn metric_label(&self) -> &'static str {
        match self {
            SmallWritePath::Inline => "write_inline",
            SmallWritePath::SingleBlockNonInline => "write_single_block_non_inline",
            SmallWritePath::Pipeline => "write_pipeline",
            SmallWritePath::PipelineBatchedLarge => "write_pipeline_batched_large",
        }
    }

    fn multipart_metric_label(&self) -> &'static str {
        match self {
            SmallWritePath::Inline => "multipart_write_inline",
            SmallWritePath::SingleBlockNonInline => "multipart_write_single_block_non_inline",
            SmallWritePath::Pipeline => "multipart_write_pipeline",
            SmallWritePath::PipelineBatchedLarge => "multipart_write_pipeline_batched_large",
        }
    }
}

fn put_large_batch_min_size_bytes() -> usize {
    *CACHED_PUT_LARGE_BATCH_MIN_SIZE_BYTES.get_or_init(|| {
        rustfs_utils::get_env_usize(ENV_RUSTFS_PUT_LARGE_BATCH_MIN_SIZE_BYTES, DEFAULT_RUSTFS_PUT_LARGE_BATCH_MIN_SIZE_BYTES)
    })
}

fn multipart_put_large_batch_min_size_bytes() -> usize {
    *CACHED_MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES.get_or_init(|| {
        rustfs_utils::get_env_usize(
            ENV_RUSTFS_MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES,
            DEFAULT_RUSTFS_MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES,
        )
    })
}

fn classify_small_write_path(is_inline_buffer: bool, object_size: i64, block_size: usize) -> SmallWritePath {
    if should_use_inline_small_fast_path(is_inline_buffer, object_size, block_size) {
        SmallWritePath::Inline
    } else if should_use_single_block_non_inline_fast_path(is_inline_buffer, object_size, block_size) {
        SmallWritePath::SingleBlockNonInline
    } else {
        SmallWritePath::Pipeline
    }
}

fn classify_put_write_path(is_inline_buffer: bool, object_size: i64, block_size: usize) -> SmallWritePath {
    if should_use_inline_small_fast_path(is_inline_buffer, object_size, block_size) {
        return SmallWritePath::Inline;
    }
    if should_use_single_block_non_inline_fast_path(is_inline_buffer, object_size, block_size) {
        return SmallWritePath::SingleBlockNonInline;
    }

    match usize::try_from(object_size) {
        Ok(size) if !is_inline_buffer && size >= put_large_batch_min_size_bytes() => SmallWritePath::PipelineBatchedLarge,
        _ => SmallWritePath::Pipeline,
    }
}

fn classify_multipart_part_write_path(object_size: i64, block_size: usize) -> SmallWritePath {
    if should_use_single_block_non_inline_fast_path(false, object_size, block_size) {
        return SmallWritePath::SingleBlockNonInline;
    }

    match usize::try_from(object_size) {
        Ok(size) if size >= multipart_put_large_batch_min_size_bytes() => SmallWritePath::PipelineBatchedLarge,
        _ => SmallWritePath::Pipeline,
    }
}

fn known_put_object_storage_size(data_size: i64) -> i64 {
    if data_size >= 0 {
        data_size
    } else {
        HashReader::SIZE_PRESERVE_LAYER
    }
}

#[allow(clippy::too_many_arguments)]
async fn build_inline_bitrot_readers(
    files: &[FileInfo],
    total_shards: usize,
    bucket: &str,
    object: &str,
    read_length: usize,
    shard_size: usize,
    checksum_algo: &HashAlgorithm,
    skip_verify_bitrot: bool,
) -> disk::error::Result<Vec<Option<InlineBitrotReader>>> {
    let mut readers = Vec::with_capacity(total_shards);
    for file in files.iter().take(total_shards) {
        let reader = if let Some(data) = &file.data {
            create_bitrot_reader_from_bytes(
                Some(data.clone()),
                None,
                bucket,
                object,
                0,
                read_length,
                shard_size,
                checksum_algo.clone(),
                skip_verify_bitrot,
                false,
            )
            .await?
        } else {
            None
        };
        readers.push(reader);
    }
    Ok(readers)
}

#[allow(clippy::too_many_arguments)]
async fn build_inline_bitrot_readers_from_refs(
    files: &[&FileInfo],
    bucket: &str,
    object: &str,
    read_length: usize,
    shard_size: usize,
    checksum_algo: &HashAlgorithm,
    skip_verify_bitrot: bool,
) -> disk::error::Result<Vec<Option<InlineBitrotReader>>> {
    let mut readers = Vec::with_capacity(files.len());
    for file in files {
        let reader = if let Some(data) = &file.data {
            create_bitrot_reader_from_bytes(
                Some(data.clone()),
                None,
                bucket,
                object,
                0,
                read_length,
                shard_size,
                checksum_algo.clone(),
                skip_verify_bitrot,
                false,
            )
            .await?
        } else {
            None
        };
        readers.push(reader);
    }
    Ok(readers)
}

async fn try_read_inline_data_shards_direct(
    readers: &mut [Option<InlineBitrotReader>],
    data_shards: usize,
    read_length: usize,
    object_size: usize,
) -> Option<Bytes> {
    if object_size == 0 || read_length == 0 || readers.len() < data_shards {
        return None;
    }

    let mut body = Vec::with_capacity(object_size);
    let mut remaining = object_size;
    for reader in readers.iter_mut().take(data_shards) {
        let reader = reader.as_mut()?;
        let mut shard = vec![0u8; read_length];
        let Ok(read) = reader.read(&mut shard).await else {
            return None;
        };
        if read != read_length {
            return None;
        }

        let take = remaining.min(shard.len());
        body.extend_from_slice(&shard[..take]);
        remaining -= take;
        if remaining == 0 {
            return Some(Bytes::from(body));
        }
    }

    None
}

fn can_try_inline_data_shards_direct(object_size: usize, block_size: usize) -> bool {
    object_size > 0 && object_size <= block_size
}

fn inline_erasure_shard_size(block_size: usize, data_shards: usize, uses_legacy: bool) -> usize {
    if block_size == 0 || data_shards == 0 {
        return 0;
    }
    if uses_legacy {
        coding::calc_shard_size_legacy(block_size, data_shards)
    } else {
        coding::calc_shard_size(block_size, data_shards)
    }
}

fn inline_erasure_shard_file_size(total_length: usize, block_size: usize, data_shards: usize, uses_legacy: bool) -> usize {
    if total_length == 0 || block_size == 0 || data_shards == 0 {
        return 0;
    }

    let shard_size = inline_erasure_shard_size(block_size, data_shards, uses_legacy);
    let shard_size_fn = if uses_legacy {
        coding::calc_shard_size_legacy
    } else {
        coding::calc_shard_size
    };
    let num_shards = total_length / block_size;
    let last_block_size = total_length % block_size;
    let last_shard_size = shard_size_fn(last_block_size, data_shards);
    num_shards * shard_size + last_shard_size
}

fn inline_erasure_shard_file_offset(
    start_offset: usize,
    length: usize,
    total_length: usize,
    block_size: usize,
    data_shards: usize,
    uses_legacy: bool,
) -> usize {
    if block_size == 0 || data_shards == 0 {
        return 0;
    }

    let shard_size = inline_erasure_shard_size(block_size, data_shards, uses_legacy);
    let shard_file_size = inline_erasure_shard_file_size(total_length, block_size, data_shards, uses_legacy);
    let end_shard = (start_offset + length) / block_size;
    let till_offset = end_shard * shard_size + shard_size;
    till_offset.min(shard_file_size)
}

fn collect_inline_data_shard_fileinfos_by_index<'a>(
    parts_metadata: &'a [FileInfo],
    fi: &FileInfo,
    data_shards: usize,
    mut disk_is_online: impl FnMut(usize) -> bool,
) -> Option<Vec<&'a FileInfo>> {
    let distribution = &fi.erasure.distribution;
    let mut data_files = vec![None; data_shards];

    for (disk_index, file_info) in parts_metadata.iter().enumerate() {
        if !disk_is_online(disk_index) {
            continue;
        }
        let block_index = *distribution.get(disk_index)?;
        if block_index == 0 || block_index > data_shards {
            continue;
        }
        if !file_info.is_valid() {
            continue;
        }
        if file_info.data.as_ref().is_none_or(|data| data.is_empty()) {
            continue;
        }

        data_files[block_index - 1] = Some(file_info);
    }

    data_files.into_iter().collect()
}

impl SetDisks {
    async fn acquire_dist_delete_object_locks_batch(
        &self,
        batch: &rustfs_lock::BatchLockRequest,
    ) -> (HashMap<(String, String), String>, HashSet<String>, Vec<Vec<rustfs_lock::LockId>>) {
        let requests: Vec<rustfs_lock::LockRequest> = batch
            .requests
            .iter()
            .map(|req| {
                rustfs_lock::LockRequest::new(req.key.clone(), rustfs_lock::LockType::Exclusive, self.locker_owner.clone())
                    .with_acquire_timeout(get_lock_acquire_timeout())
                    .with_ttl(rustfs_lock::fast_lock::DEFAULT_LOCK_TIMEOUT)
            })
            .collect();

        let write_quorum = if self.lockers.len() > 1 {
            (self.lockers.len() / 2) + 1
        } else {
            1
        };

        let mut lock_ids_by_object: Vec<Vec<(usize, rustfs_lock::LockId)>> = vec![Vec::new(); requests.len()];
        let mut errors_by_object: Vec<Option<String>> = vec![None; requests.len()];
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum ObjectLockResolution {
            Pending,
            Succeeded,
            Failed,
        }

        let mut resolution_by_object = vec![ObjectLockResolution::Pending; requests.len()];
        let mut pending_clients = self.lockers.len();
        let mut unresolved_objects = requests.len();
        let mut cleanup_lock_ids_by_client = vec![Vec::new(); self.lockers.len()];

        let mut pending = tokio::task::JoinSet::new();
        for (client_idx, client) in self.lockers.iter().cloned().enumerate() {
            let requests = requests.clone();
            pending.spawn(async move { (client_idx, client.acquire_locks_batch(&requests).await) });
        }

        while unresolved_objects > 0 {
            let Some(join_result) = pending.join_next().await else {
                break;
            };
            pending_clients = pending_clients.saturating_sub(1);

            match join_result {
                Ok((client_idx, Ok(responses))) => {
                    for (req_idx, request) in requests.iter().enumerate() {
                        let response = responses.get(req_idx);
                        match resolution_by_object[req_idx] {
                            ObjectLockResolution::Pending => match response {
                                Some(response) if response.success => {
                                    let lock_id = response
                                        .lock_info
                                        .as_ref()
                                        .map(|lock_info| lock_info.id.clone())
                                        .unwrap_or_else(|| request.lock_id.clone());
                                    lock_ids_by_object[req_idx].push((client_idx, lock_id));
                                }
                                Some(response) => {
                                    if errors_by_object[req_idx].is_none() {
                                        errors_by_object[req_idx] = Some(
                                            response
                                                .error
                                                .clone()
                                                .unwrap_or_else(|| "distributed lock acquisition failed".to_string()),
                                        );
                                    }
                                }
                                None => {
                                    if errors_by_object[req_idx].is_none() {
                                        errors_by_object[req_idx] =
                                            Some(format!("client {client_idx} returned incomplete batch lock response"));
                                    }
                                }
                            },
                            ObjectLockResolution::Succeeded | ObjectLockResolution::Failed => {
                                if let Some(response) = response
                                    && response.success
                                {
                                    let lock_id = response
                                        .lock_info
                                        .as_ref()
                                        .map(|lock_info| lock_info.id.clone())
                                        .unwrap_or_else(|| request.lock_id.clone());
                                    cleanup_lock_ids_by_client[client_idx].push(lock_id);
                                }
                            }
                        }
                    }
                }
                Ok((client_idx, Err(err))) => {
                    for (req_idx, error) in errors_by_object.iter_mut().enumerate().take(requests.len()) {
                        if resolution_by_object[req_idx] == ObjectLockResolution::Pending && error.is_none() {
                            *error = Some(format!("client {client_idx} batch lock request failed: {err}"));
                        }
                    }
                }
                Err(err) => {
                    for (req_idx, error) in errors_by_object.iter_mut().enumerate().take(requests.len()) {
                        if resolution_by_object[req_idx] == ObjectLockResolution::Pending && error.is_none() {
                            *error = Some(format!("batch lock task join failed: {err}"));
                        }
                    }
                }
            }

            for req_idx in 0..requests.len() {
                if resolution_by_object[req_idx] != ObjectLockResolution::Pending {
                    continue;
                }

                let success_count = lock_ids_by_object[req_idx].len();
                if success_count >= write_quorum {
                    resolution_by_object[req_idx] = ObjectLockResolution::Succeeded;
                    unresolved_objects -= 1;
                } else if success_count + pending_clients < write_quorum {
                    resolution_by_object[req_idx] = ObjectLockResolution::Failed;
                    unresolved_objects -= 1;
                }
            }
        }

        if issue3031_diag_enabled() {
            let succeeded_count = resolution_by_object
                .iter()
                .filter(|resolution| matches!(resolution, ObjectLockResolution::Succeeded))
                .count();
            let failed_count = resolution_by_object
                .iter()
                .filter(|resolution| matches!(resolution, ObjectLockResolution::Failed))
                .count();
            let pending_count = resolution_by_object
                .iter()
                .filter(|resolution| matches!(resolution, ObjectLockResolution::Pending))
                .count();
            warn!(
                target: "rustfs_ecstore::set_disk",
                request_count = requests.len(),
                locker_count = self.lockers.len(),
                write_quorum,
                succeeded_count,
                failed_count,
                pending_count,
                pending_clients,
                errors_by_object = ?errors_by_object,
                "issue3031_delete_objects_dist_batch_lock_summary"
            );
        }

        if !pending.is_empty() {
            let cleanup_requests = requests.clone();
            let lockers = self.lockers.clone();
            let handle = tokio::spawn(
                async move {
                    let mut late_lock_ids_by_client = vec![Vec::new(); lockers.len()];
                    let mut pending = pending;
                    while let Some(join_result) = pending.join_next().await {
                        match join_result {
                            Ok((client_idx, Ok(responses))) => {
                                for (req_idx, request) in cleanup_requests.iter().enumerate() {
                                    if let Some(response) = responses.get(req_idx)
                                        && response.success
                                    {
                                        let lock_id = response
                                            .lock_info
                                            .as_ref()
                                            .map(|lock_info| lock_info.id.clone())
                                            .unwrap_or_else(|| request.lock_id.clone());
                                        if let Some(client_locks) = late_lock_ids_by_client.get_mut(client_idx) {
                                            client_locks.push(lock_id);
                                        }
                                    }
                                }
                            }
                            Ok((_client_idx, Err(err))) => {
                                warn!("late distributed delete lock batch request failed: {}", err);
                            }
                            Err(err) => {
                                warn!("late distributed delete lock batch task join failed: {}", err);
                            }
                        }
                    }

                    join_all(lockers.iter().cloned().enumerate().filter_map(|(client_idx, client)| {
                        let lock_ids = late_lock_ids_by_client.get(client_idx).cloned().unwrap_or_default();
                        if lock_ids.is_empty() {
                            None
                        } else {
                            Some(async move {
                                if let Err(err) = client.release_locks_batch(&lock_ids).await {
                                    warn!(
                                        client_idx,
                                        lock_count = lock_ids.len(),
                                        "failed to cleanup late distributed delete locks in batch: {}",
                                        err
                                    );
                                }
                            })
                        }
                    }))
                    .await;
                }
                .instrument(tracing::Span::current()),
            );
            drop(handle);
        }

        let mut failed_map = HashMap::new();
        let mut locked_objects = HashSet::new();
        let mut held_lock_ids_by_client = vec![Vec::new(); self.lockers.len()];
        let mut rollback_lock_ids_by_client = vec![Vec::new(); self.lockers.len()];

        for (req_idx, req) in batch.requests.iter().enumerate() {
            let success_count = lock_ids_by_object[req_idx].len();
            match resolution_by_object[req_idx] {
                ObjectLockResolution::Succeeded => {
                    for (client_idx, lock_id) in lock_ids_by_object[req_idx].drain(..) {
                        held_lock_ids_by_client[client_idx].push(lock_id);
                    }
                    locked_objects.insert(req.key.object.as_ref().to_string());
                }
                ObjectLockResolution::Pending | ObjectLockResolution::Failed => {
                    for (client_idx, lock_id) in lock_ids_by_object[req_idx].drain(..) {
                        rollback_lock_ids_by_client[client_idx].push(lock_id);
                    }
                    failed_map.insert(
                        (req.key.bucket.as_ref().to_string(), req.key.object.as_ref().to_string()),
                        errors_by_object[req_idx].clone().unwrap_or_else(|| {
                            format!("failed to acquire distributed delete lock quorum: {success_count}/{write_quorum}")
                        }),
                    );
                }
            }
        }

        for (client_idx, cleanup_ids) in cleanup_lock_ids_by_client.into_iter().enumerate() {
            rollback_lock_ids_by_client[client_idx].extend(cleanup_ids);
        }

        self.release_dist_delete_object_locks_batch(rollback_lock_ids_by_client).await;

        (failed_map, locked_objects, held_lock_ids_by_client)
    }

    async fn release_dist_delete_object_locks_batch(&self, lock_ids_by_client: Vec<Vec<rustfs_lock::LockId>>) {
        join_all(self.lockers.iter().cloned().enumerate().filter_map(|(client_idx, client)| {
            let lock_ids = lock_ids_by_client.get(client_idx).cloned().unwrap_or_default();
            if lock_ids.is_empty() {
                None
            } else {
                Some(async move {
                    if let Err(err) = client.release_locks_batch(&lock_ids).await {
                        warn!(
                            client_idx,
                            lock_count = lock_ids.len(),
                            "failed to release distributed delete locks in batch: {}",
                            err
                        );
                    }
                })
            }
        }))
        .await;
    }
}

impl SetDisks {
    pub(crate) async fn storage_info_snapshot(&self) -> rustfs_madmin::StorageInfo {
        let disks = self.get_disks_internal().await;

        get_storage_info(&disks, &self.set_endpoints).await
    }

    pub(crate) async fn local_storage_info_snapshot(&self) -> rustfs_madmin::StorageInfo {
        let disks = self.get_disks_internal().await;

        let mut local_disks: Vec<Option<DiskStore>> = Vec::new();
        let mut local_endpoints = Vec::new();

        for (i, ep) in self.set_endpoints.iter().enumerate() {
            if ep.is_local {
                local_disks.push(disks[i].clone());
                local_endpoints.push(ep.clone());
            }
        }

        get_storage_info(&local_disks, &local_endpoints).await
    }

    pub(crate) async fn disk_inventory(&self) -> Vec<Option<DiskStore>> {
        self.get_disks_internal().await
    }
}

fn check_object_lock_retention_update(bucket: &str, object: &str, obj_info: &ObjectInfo, opts: &ObjectOptions) -> Result<()> {
    if let Some(retention) = &opts.object_lock_retention
        && check_retention_for_modification(
            &obj_info.user_defined,
            retention.mode.as_deref(),
            retention.retain_until,
            retention.bypass_governance,
        )
        .is_some()
    {
        return Err(StorageError::PrefixAccessDenied(bucket.to_string(), object.to_string()));
    }

    Ok(())
}

async fn check_object_lock_delete(bucket: &str, object: &str, obj_info: &ObjectInfo, opts: &ObjectOptions) -> Result<()> {
    if set_disk_delete_creates_delete_marker(opts) {
        return Ok(());
    }

    let bypass_governance = opts
        .object_lock_delete
        .as_ref()
        .is_some_and(|delete_opts| delete_opts.bypass_governance);
    if check_object_lock_for_deletion(bucket, obj_info, bypass_governance)
        .await
        .is_some()
    {
        return Err(StorageError::PrefixAccessDenied(bucket.to_string(), object.to_string()));
    }

    Ok(())
}

fn set_disk_delete_creates_delete_marker(opts: &ObjectOptions) -> bool {
    opts.version_id.is_none() && opts.versioned && !opts.version_suspended
}

fn should_preserve_delete_replication_state(opts: &ObjectOptions) -> bool {
    opts.delete_replication.as_ref().is_some_and(|state| {
        state.replica_status == ReplicationStatusType::Replica
            || (!state.replicate_decision_str.is_empty()
                && (!state.composite_replication_status().is_empty() || !state.composite_version_purge_status().is_empty()))
    }) || opts.version_purge_status() == VersionPurgeStatusType::Complete
}

fn should_force_delete_marker_for_missing_version(opts: &ObjectOptions) -> bool {
    opts.delete_marker || (opts.versioned && opts.version_id.is_none() && !opts.data_movement)
}

fn resolve_delete_version_state(opts: &ObjectOptions, goi: &ObjectInfo, version_found: bool) -> (bool, bool) {
    let mut mark_delete = goi.version_id.is_some() || (opts.versioned && opts.version_id.is_none());
    let mut delete_marker = opts.versioned;

    if opts.version_id.is_some() {
        // Decommission/rebalance may recreate a delete marker on a new pool before that
        // exact version exists there, so we must still treat it as a mark-delete write.
        if opts.data_movement && opts.delete_marker && !version_found {
            mark_delete = true;
        }

        let delete_marker_version_purge = version_found && goi.delete_marker && !opts.version_purge_status().is_empty();

        if version_found && opts.delete_marker_replication_status() == ReplicationStatusType::Replica {
            mark_delete = false;
        }

        if opts.version_purge_status().is_empty() && opts.delete_marker_replication_status().is_empty() {
            mark_delete = false;
        }

        if opts.version_purge_status() == VersionPurgeStatusType::Complete {
            mark_delete = false;
        }

        let replica_delete_marker_version_purge =
            version_found && goi.delete_marker && opts.delete_marker_replication_status() == ReplicationStatusType::Replica;

        if delete_marker_version_purge {
            mark_delete = false;
        }

        if !version_found && !opts.delete_marker && opts.delete_marker_replication_status() == ReplicationStatusType::Replica {
            delete_marker = false;
        }

        if version_found
            && (!goi.version_purge_status.is_empty()
                || !goi.delete_marker
                || replica_delete_marker_version_purge
                || delete_marker_version_purge)
        {
            delete_marker = false;
        }
    }

    (mark_delete, delete_marker)
}

impl SetDisks {
    #[tracing::instrument(skip(self, fi, opts))]
    pub(crate) async fn decommission_tiered_object(
        &self,
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        opts: &ObjectOptions,
    ) -> Result<()> {
        let _lock_guard = if !opts.no_lock {
            Some(
                self.new_ns_lock(bucket, object)
                    .await?
                    .get_write_lock(get_lock_acquire_timeout())
                    .await
                    .map_err(|e| self.map_namespace_lock_error(bucket, object, "write", e))?,
            )
        } else {
            None
        };

        let disks = self.disks.read().await.clone();
        let storage_class = opts.user_defined.get(AMZ_STORAGE_CLASS).map(String::as_str);
        let (fi, write_quorum) =
            build_tiered_decommission_file_info(bucket, object, fi, disks.len(), self.default_parity_count, storage_class);
        let parts_metadata = vec![fi.clone(); disks.len()];
        let (shuffle_disks, parts_metadata) = Self::shuffle_disks_and_parts_metadata(&disks, &parts_metadata, &fi);

        let mut errs = Vec::with_capacity(shuffle_disks.len());
        let mut futures = Vec::with_capacity(shuffle_disks.len());
        for (index, disk) in shuffle_disks.iter().enumerate() {
            let mut file_info = parts_metadata[index].clone();
            file_info.erasure.index = index + 1;
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.write_metadata("", bucket, object, file_info).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        for result in join_all(futures).await {
            match result {
                Ok(_) => errs.push(None),
                Err(err) => errs.push(Some(err)),
            }
        }

        resolve_tiered_decommission_write_quorum_result(&errs, write_quorum, bucket, object)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ObjProps {
    successor_mod_time: Option<OffsetDateTime>,
    num_versions: usize,
}

impl Hash for ObjProps {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.successor_mod_time.hash(state);
        self.num_versions.hash(state);
    }
}

#[derive(Default, Clone, Debug)]
pub struct HealEntryResult {
    pub bytes: usize,
    pub success: bool,
    pub skipped: bool,
    pub entry_done: bool,
    pub name: String,
}

fn is_object_dangling(
    meta_arr: &[FileInfo],
    errs: &[Option<DiskError>],
    data_errs_by_part: &HashMap<usize, Vec<usize>>,
) -> (FileInfo, bool) {
    let (not_found_meta_errs, non_actionable_meta_errs) = dangling_meta_errs_count(errs);

    let (mut not_found_parts_errs, mut non_actionable_parts_errs) = (0, 0);

    data_errs_by_part.iter().for_each(|(_, v)| {
        let (nf, na) = dangling_part_errs_count(v);
        if nf > not_found_parts_errs {
            (not_found_parts_errs, non_actionable_parts_errs) = (nf, na);
        }
    });

    let mut valid_meta = FileInfo::default();

    for fi in meta_arr.iter() {
        if fi.is_valid() {
            valid_meta = fi.clone();
            break;
        }
    }

    if !valid_meta.is_valid() {
        let data_blocks = meta_arr.len().div_ceil(2);
        if not_found_parts_errs > data_blocks {
            return (valid_meta, true);
        }

        return (valid_meta, false);
    }

    if non_actionable_meta_errs > 0 || non_actionable_parts_errs > 0 {
        return (valid_meta, false);
    }

    if valid_meta.deleted {
        let data_blocks = errs.len().div_ceil(2);
        return (valid_meta, not_found_meta_errs > data_blocks);
    }

    if not_found_meta_errs > 0 && not_found_meta_errs > valid_meta.erasure.parity_blocks {
        return (valid_meta, true);
    }

    if !valid_meta.is_remote() && not_found_parts_errs > 0 && not_found_parts_errs > valid_meta.erasure.parity_blocks {
        return (valid_meta, true);
    }

    (valid_meta, false)
}

fn dangling_meta_errs_count(cerrs: &[Option<DiskError>]) -> (usize, usize) {
    let (mut not_found_count, mut non_actionable_count) = (0, 0);
    cerrs.iter().for_each(|err| {
        if let Some(err) = err {
            if err == &DiskError::FileNotFound || err == &DiskError::FileVersionNotFound {
                not_found_count += 1;
            } else {
                non_actionable_count += 1;
            }
        }
    });

    (not_found_count, non_actionable_count)
}

fn dangling_part_errs_count(results: &[usize]) -> (usize, usize) {
    let (mut not_found_count, mut non_actionable_count) = (0, 0);
    results.iter().for_each(|result| {
        if *result == CHECK_PART_SUCCESS {
            // skip
        } else if *result == CHECK_PART_FILE_NOT_FOUND {
            not_found_count += 1;
        } else {
            non_actionable_count += 1;
        }
    });

    (not_found_count, non_actionable_count)
}

fn is_object_dir_dangling(errs: &[Option<DiskError>]) -> bool {
    let mut found = 0;
    let mut not_found = 0;
    let mut found_not_empty = 0;
    let mut other_found = 0;
    errs.iter().for_each(|err| {
        if err.is_none() {
            found += 1;
        } else if let Some(err) = err {
            if err == &DiskError::FileNotFound || err == &DiskError::VolumeNotFound {
                not_found += 1;
            } else if err == &DiskError::VolumeNotEmpty {
                found_not_empty += 1;
            } else {
                other_found += 1;
            }
        }
    });

    found = found + found_not_empty + other_found;
    found < not_found && found > 0
}

fn join_errs(errs: &[Option<DiskError>]) -> String {
    let errs = errs
        .iter()
        .map(|err| {
            if let Some(err) = err {
                return err.to_string();
            }
            "<nil>".to_string()
        })
        .collect::<Vec<_>>();

    errs.join(", ")
}

/// disks_with_all_partsv2 is a corrected version based on Go implementation.
/// It sets partsMetadata and onlineDisks when xl.meta is inexistant/corrupted or outdated.
/// It also checks if the status of each part (corrupted, missing, ok) in each drive.
/// Returns (availableDisks, dataErrsByDisk, dataErrsByPart).
#[allow(clippy::too_many_arguments)]
async fn disks_with_all_parts(
    online_disks: &mut [Option<DiskStore>],
    parts_metadata: &mut [FileInfo],
    errs: &[Option<DiskError>],
    latest_meta: &FileInfo,
    filter_by_etag: bool,
    bucket: &str,
    object: &str,
    scan_mode: HealScanMode,
) -> disk::error::Result<(HashMap<usize, Vec<usize>>, HashMap<usize, Vec<usize>>)> {
    let object_name = latest_meta.name.clone();

    // Initialize dataErrsByDisk and dataErrsByPart with 0 (CHECK_PART_UNKNOWN) to match Go
    let mut data_errs_by_disk: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..online_disks.len() {
        data_errs_by_disk.insert(i, vec![CHECK_PART_UNKNOWN; latest_meta.parts.len()]);
    }
    let mut data_errs_by_part: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..latest_meta.parts.len() {
        data_errs_by_part.insert(i, vec![CHECK_PART_UNKNOWN; online_disks.len()]);
    }

    // Check for inconsistent erasure distribution
    let mut inconsistent = 0;
    for (index, meta) in parts_metadata.iter().enumerate() {
        if !meta.is_valid() {
            // Since for majority of the cases erasure.Index matches with erasure.Distribution we can
            // consider the offline disks as consistent.
            continue;
        }
        if !meta.deleted {
            if meta.erasure.distribution.len() != online_disks.len() {
                // Erasure distribution seems to have lesser
                // number of items than number of online disks.
                inconsistent += 1;
                continue;
            }
            if !meta.erasure.distribution.is_empty()
                && index < meta.erasure.distribution.len()
                && meta.erasure.distribution[index] != meta.erasure.index
            {
                // Mismatch indexes with distribution order
                inconsistent += 1;
            }
        }
    }

    let erasure_distribution_reliable = inconsistent <= parts_metadata.len() / 2;

    // Initialize metaErrs
    let mut meta_errs = Vec::with_capacity(errs.len());
    for _ in 0..errs.len() {
        meta_errs.push(None);
    }

    let online_disks_len = online_disks.len();

    // Process meta errors
    for (index, disk_op) in online_disks.iter_mut().enumerate() {
        if let Some(err) = &errs[index] {
            meta_errs[index] = Some(err.clone());
            continue;
        }

        if disk_op.is_none() {
            meta_errs[index] = Some(DiskError::DiskNotFound);
            continue;
        }

        let meta = &parts_metadata[index];

        let corrupted = if filter_by_etag {
            latest_meta.get_etag() != meta.get_etag()
        } else {
            !meta.mod_time.eq(&latest_meta.mod_time) || !meta.data_dir.eq(&latest_meta.data_dir)
        };

        if corrupted {
            info!(
                "disks_with_all_partsv2: metadata is corrupted, object_name={}, index: {index}",
                object_name
            );
            meta_errs[index] = Some(DiskError::FileCorrupt);
            parts_metadata[index] = FileInfo::default();
            *disk_op = None;

            continue;
        }

        if erasure_distribution_reliable {
            if !meta.is_valid() {
                info!(
                    "disks_with_all_partsv2: metadata is not valid, object_name={}, index: {index}",
                    object_name
                );
                parts_metadata[index] = FileInfo::default();
                meta_errs[index] = Some(DiskError::FileCorrupt);
                *disk_op = None;
                continue;
            }

            if !meta.deleted && meta.erasure.distribution.len() != online_disks_len {
                // Erasure distribution is not the same as onlineDisks
                // attempt a fix if possible, assuming other entries
                // might have the right erasure distribution.
                info!(
                    "disks_with_all_partsv2: erasure distribution is not the same as onlineDisks, object_name={}, index: {index}",
                    object_name
                );
                parts_metadata[index] = FileInfo::default();
                meta_errs[index] = Some(DiskError::FileCorrupt);
                *disk_op = None;
                continue;
            }
        }
    }

    // Copy meta errors to part errors
    for (index, err) in meta_errs.iter().enumerate() {
        if err.is_some() {
            let part_err = conv_part_err_to_int(err);
            for p in 0..latest_meta.parts.len() {
                if let Some(vec) = data_errs_by_part.get_mut(&p)
                    && index < vec.len()
                {
                    vec[index] = part_err;
                }
            }
        }
    }

    // Check data for each disk
    for (index, disk) in online_disks.iter().enumerate() {
        if meta_errs[index].is_some() {
            continue;
        }

        let disk = if let Some(disk) = disk {
            disk
        } else {
            continue;
        };

        let meta = &mut parts_metadata[index];
        if meta.deleted || meta.is_remote() {
            continue;
        }

        // Inline data is stored inside xl.meta, so there is no separate part file to
        // verify here. Treat the shard as present once metadata was read successfully;
        // object reads/heal will validate the inline shard through the normal bitrot
        // reader path. Running bitrot_verify directly here can falsely mark small
        // inline shards corrupt when older metadata has no per-part checksum entries.
        if (meta.data.is_some() || meta.size == 0) && !meta.parts.is_empty() {
            if let Some(vec) = data_errs_by_part.get_mut(&0)
                && index < vec.len()
            {
                vec[index] = CHECK_PART_SUCCESS;
            }
            continue;
        }

        // Verify file or check parts
        let mut verify_resp = CheckPartsResp::default();
        let mut verify_err = None;
        meta.data_dir = latest_meta.data_dir;

        if scan_mode == HealScanMode::Deep {
            // disk has a valid xl.meta but may not have all the
            // parts. This is considered an outdated disk, since
            // it needs healing too.
            match disk.verify_file(bucket, object, meta).await {
                Ok(v) => {
                    verify_resp = v;
                }
                Err(err) => {
                    debug!(
                        event = EVENT_SET_DISK_HEAL,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        object = %object_name,
                        disk_index = index,
                        state = "verify_failed",
                        error = ?err,
                        "Set disk verify_file failed"
                    );
                    verify_err = Some(err);
                }
            }
        } else {
            match disk.check_parts(bucket, object, meta).await {
                Ok(v) => {
                    verify_resp = v;
                }
                Err(err) => {
                    debug!(
                        event = EVENT_SET_DISK_HEAL,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        object = %object_name,
                        disk_index = index,
                        state = "check_parts_failed",
                        error = ?err,
                        "Set disk check_parts failed"
                    );
                    verify_err = Some(err);
                }
            }
        }

        // Update dataErrsByPart for all parts
        for p in 0..latest_meta.parts.len() {
            if let Some(vec) = data_errs_by_part.get_mut(&p)
                && index < vec.len()
            {
                if verify_err.is_some() {
                    vec[index] = conv_part_err_to_int(&verify_err.clone());
                } else {
                    // Fix: verify_resp.results length is based on meta.parts, not latest_meta.parts
                    // We need to check bounds to avoid panic
                    if p < verify_resp.results.len() {
                        vec[index] = verify_resp.results[p];
                    } else {
                        vec[index] = CHECK_PART_SUCCESS;
                    }
                }
            }
        }
    }

    populate_data_errs_by_disk(&mut data_errs_by_disk, &data_errs_by_part);

    Ok((data_errs_by_disk, data_errs_by_part))
}

fn populate_data_errs_by_disk(
    data_errs_by_disk: &mut HashMap<usize, Vec<usize>>,
    data_errs_by_part: &HashMap<usize, Vec<usize>>,
) {
    for (part_index, part_errs) in data_errs_by_part {
        for (disk_index, part_err) in part_errs.iter().enumerate() {
            if let Some(disk_errs) = data_errs_by_disk.get_mut(&disk_index)
                && *part_index < disk_errs.len()
            {
                disk_errs[*part_index] = *part_err;
            }
        }
    }
}

pub fn should_heal_object_on_disk(
    err: &Option<DiskError>,
    parts_errs: &[usize],
    meta: &FileInfo,
    latest_meta: &FileInfo,
) -> (bool, bool, Option<DiskError>) {
    if let Some(err) = err
        && (err == &DiskError::FileNotFound || err == &DiskError::FileVersionNotFound || err == &DiskError::FileCorrupt)
    {
        return (true, true, Some(err.clone()));
    }

    if err.is_some() {
        return (false, false, err.clone());
    }

    if !meta.equals(latest_meta) {
        warn!(
            "should_heal_object_on_disk: metadata is outdated, object_name={}, meta: {:?}, latest_meta: {:?}",
            meta.name, meta, latest_meta
        );
        return (true, true, Some(DiskError::OutdatedXLMeta));
    }

    if !meta.deleted && !meta.is_remote() {
        let err_vec = [CHECK_PART_FILE_NOT_FOUND, CHECK_PART_FILE_CORRUPT];
        for part_err in parts_errs.iter() {
            if err_vec.contains(part_err) {
                return (true, false, Some(DiskError::PartMissingOrCorrupt));
            }
        }
    }
    (false, false, None)
}

async fn get_disks_info(disks: &[Option<DiskStore>], eps: &[Endpoint]) -> Vec<rustfs_madmin::Disk> {
    let mut ret = Vec::new();

    for (i, pool) in disks.iter().enumerate() {
        if let Some(disk) = pool {
            let runtime_state = disk.runtime_state();
            let offline_duration_seconds = disk.offline_duration_secs();
            let capacity_snapshot = disk.last_capacity_snapshot();
            if runtime_state.should_probe_for_admin() || runtime_state == disk::health_state::RuntimeDriveHealthState::Suspect {
                match disk.disk_info(&DiskInfoOptions::default()).await {
                    Ok(res) => {
                        disk.record_capacity_probe(res.total, res.used, res.free);
                        ret.push(rustfs_madmin::Disk {
                            endpoint: eps[i].to_string(),
                            local: eps[i].is_local,
                            pool_index: eps[i].pool_idx,
                            set_index: eps[i].set_idx,
                            disk_index: eps[i].disk_idx,
                            state: "ok".to_owned(),

                            root_disk: res.root_disk,
                            drive_path: res.mount_path.clone(),
                            healing: res.healing,
                            scanning: res.scanning,
                            runtime_state: Some(runtime_state.as_str().to_string()),
                            offline_duration_seconds,
                            capacity_observation_source: Some("live_probe".to_owned()),
                            capacity_observation_age_seconds: Some(0),

                            uuid: res.id.map_or_else(|| "".to_string(), |id| id.to_string()),
                            major: res.major as u32,
                            minor: res.minor as u32,
                            model: None,
                            total_space: res.total,
                            used_space: res.used,
                            available_space: res.free,
                            physical_device_ids: (!res.physical_device_ids.is_empty()).then_some(res.physical_device_ids.clone()),
                            utilization: utilization_percent(res.total, res.used),
                            used_inodes: res.used_inodes,
                            free_inodes: res.free_inodes,
                            ..Default::default()
                        });
                    }
                    Err(err) => {
                        let mut disk_info = rustfs_madmin::Disk {
                            state: err.to_string(),
                            endpoint: eps[i].to_string(),
                            local: eps[i].is_local,
                            pool_index: eps[i].pool_idx,
                            set_index: eps[i].set_idx,
                            disk_index: eps[i].disk_idx,
                            runtime_state: Some(runtime_state.as_str().to_string()),
                            offline_duration_seconds,
                            ..Default::default()
                        };
                        if let Some((total, used, free, _)) = capacity_snapshot {
                            disk_info.total_space = total;
                            disk_info.used_space = used;
                            disk_info.available_space = free;
                            disk_info.utilization = utilization_percent(total, used);
                            disk_info.capacity_observation_source = Some("snapshot".to_owned());
                            disk_info.capacity_observation_age_seconds = capacity_snapshot
                                .map(|(_, _, _, probe_unix_secs)| capacity_snapshot_age_seconds(probe_unix_secs));
                        } else {
                            disk_info.capacity_observation_source = Some("missing".to_owned());
                            disk_info.capacity_observation_age_seconds = Some(0);
                        }
                        ret.push(disk_info);
                    }
                }
            } else {
                ret.push(build_runtime_snapshot_disk(
                    &eps[i],
                    runtime_state,
                    offline_duration_seconds,
                    capacity_snapshot,
                ));
            }
        } else {
            ret.push(rustfs_madmin::Disk {
                endpoint: eps[i].to_string(),
                local: eps[i].is_local,
                pool_index: eps[i].pool_idx,
                set_index: eps[i].set_idx,
                disk_index: eps[i].disk_idx,
                runtime_state: None,
                offline_duration_seconds: None,
                state: DiskError::DiskNotFound.to_string(),
                capacity_observation_source: Some("missing".to_owned()),
                capacity_observation_age_seconds: Some(0),
                ..Default::default()
            })
        }
    }

    ret
}

fn build_runtime_snapshot_disk(
    endpoint: &Endpoint,
    runtime_state: disk::health_state::RuntimeDriveHealthState,
    offline_duration_seconds: Option<u64>,
    capacity_snapshot: Option<(u64, u64, u64, u64)>,
) -> rustfs_madmin::Disk {
    let mut disk = rustfs_madmin::Disk {
        endpoint: endpoint.to_string(),
        local: endpoint.is_local,
        pool_index: endpoint.pool_idx,
        set_index: endpoint.set_idx,
        disk_index: endpoint.disk_idx,
        state: runtime_state.as_str().to_string(),
        runtime_state: Some(runtime_state.as_str().to_string()),
        offline_duration_seconds,
        ..Default::default()
    };

    if let Some((total, used, free, _)) = capacity_snapshot {
        disk.total_space = total;
        disk.used_space = used;
        disk.available_space = free;
        disk.utilization = utilization_percent(total, used);
        disk.capacity_observation_source = Some("snapshot".to_owned());
        disk.capacity_observation_age_seconds =
            capacity_snapshot.map(|(_, _, _, probe_unix_secs)| capacity_snapshot_age_seconds(probe_unix_secs));
    } else {
        disk.capacity_observation_source = Some("missing".to_owned());
        disk.capacity_observation_age_seconds = Some(0);
    }

    disk
}

fn utilization_percent(total: u64, used: u64) -> f64 {
    if total > 0 {
        used as f64 / total as f64 * 100_f64
    } else {
        0_f64
    }
}

fn capacity_snapshot_age_seconds(probe_unix_secs: u64) -> u64 {
    let now_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_secs())
        .unwrap_or(probe_unix_secs);
    now_unix_secs.saturating_sub(probe_unix_secs)
}
async fn get_storage_info(disks: &[Option<DiskStore>], eps: &[Endpoint]) -> rustfs_madmin::StorageInfo {
    // let mut disks = get_disks_info(disks, eps).await;
    // disks.sort_by(|a, b| a.total_space.cmp(&b.total_space));
    //
    // rustfs_madmin::StorageInfo {
    //     disks,
    //     backend: rustfs_madmin::BackendInfo {
    //         backend_type: rustfs_madmin::BackendByte::Erasure,
    //         ..Default::default()
    //     },
    // }
    let mut disks = get_disks_info(disks, eps).await;
    disks.sort_by_key(|a| a.total_space);

    // Provide minimal backend shape for callers. Do NOT guess parity here since it belongs to higher-level config.
    // Missing/empty standard_sc_data will be handled by capacity fallback logic.
    let drives_per_set = vec![eps.len()];
    let total_sets = vec![1];

    rustfs_madmin::StorageInfo {
        disks,
        backend: rustfs_madmin::BackendInfo {
            backend_type: rustfs_madmin::BackendByte::Erasure,
            drives_per_set,
            total_sets,
            ..Default::default()
        },
    }
}
pub async fn stat_all_dirs(disks: &[Option<DiskStore>], bucket: &str, prefix: &str) -> Vec<Option<DiskError>> {
    let mut futures = Vec::with_capacity(disks.len());
    // Spawn one future per disk slot so the returned vector stays index-aligned with `disks`
    // (and therefore with `set_endpoints`). Offline/None disks must yield DiskNotFound in-place
    // rather than being skipped, otherwise callers that zip `errs` against the full disks array
    // (heal_object_dir) would pair every error with the wrong disk/endpoint whenever any disk is
    // offline — and could `make_volume` on the wrong disk.
    for disk in disks.iter() {
        let disk = disk.clone();
        let bucket = bucket.to_string();
        let prefix = prefix.to_string();
        futures.push(tokio::spawn(async move {
            let Some(disk) = disk else {
                return Some(DiskError::DiskNotFound);
            };
            match disk.list_dir("", &bucket, &prefix, 1).await {
                Ok(entries) => {
                    if !entries.is_empty() {
                        return Some(DiskError::VolumeNotEmpty);
                    }
                    None
                }
                Err(err) => Some(err),
            }
        }));
    }

    let results = join_all(futures).await;

    // Preserve length/index alignment: a panicked probe becomes a corrupt-state error instead of
    // a silently-dropped slot that would re-shift every subsequent index.
    let mut errs = Vec::with_capacity(disks.len());
    for res in results.into_iter() {
        match res {
            Ok(err) => errs.push(err),
            Err(join_err) => errs.push(Some(DiskError::other(join_err.to_string()))),
        }
    }
    errs
}

const GLOBAL_MIN_PART_SIZE: ByteSize = ByteSize::mib(5);
fn is_min_allowed_part_size(size: i64) -> bool {
    size >= GLOBAL_MIN_PART_SIZE.as_u64() as i64
}

fn get_complete_multipart_md5(parts: &[CompletePart]) -> String {
    let mut buf = Vec::new();

    for part in parts.iter() {
        if let Some(etag) = &part.etag {
            if let Ok(etag_bytes) = hex_simd::decode_to_vec(etag.as_bytes()) {
                buf.extend(etag_bytes);
            } else {
                buf.extend(etag.bytes());
            }
        }
    }

    let mut hasher = Md5::new();
    hasher.update(&buf);

    let digest = hasher.finalize();
    let etag_hex = faster_hex::hex_string(digest.as_slice());
    format!("{}-{}", etag_hex, parts.len())
}

fn completed_multipart_object_part(part_num: usize, ext_part: &ObjectPartInfo) -> ObjectPartInfo {
    ObjectPartInfo {
        etag: ext_part.etag.clone(),
        number: part_num,
        size: ext_part.size,
        mod_time: ext_part.mod_time,
        actual_size: ext_part.actual_size,
        index: ext_part.index.clone(),
        checksums: ext_part.checksums.clone(),
        ..Default::default()
    }
}

fn complete_part_checksum(part: &CompletePart, checksum_type: rustfs_rio::ChecksumType) -> Option<Option<String>> {
    match checksum_type.base() {
        rustfs_rio::ChecksumType::SHA256 => Some(part.checksum_sha256.clone()),
        rustfs_rio::ChecksumType::SHA1 => Some(part.checksum_sha1.clone()),
        rustfs_rio::ChecksumType::CRC32 => Some(part.checksum_crc32.clone()),
        rustfs_rio::ChecksumType::CRC32C => Some(part.checksum_crc32c.clone()),
        rustfs_rio::ChecksumType::CRC64_NVME => Some(part.checksum_crc64nvme.clone()),
        _ => None,
    }
}

fn parts_after_marker(part_numbers: &[usize], part_number_marker: usize) -> Option<&[usize]> {
    if part_number_marker == 0 {
        return Some(part_numbers);
    }

    part_numbers
        .iter()
        .position(|&part_number| part_number != 0 && part_number == part_number_marker)
        .map(|index| &part_numbers[index + 1..])
}

pub fn canonicalize_etag(etag: &str) -> String {
    let re = Regex::new("\"*?([^\"]*?)\"*?$").unwrap();
    re.replace_all(etag, "$1").to_string()
}

pub fn e_tag_matches(etag: &str, condition: &str) -> bool {
    if condition.trim() == "*" {
        return true;
    }
    canonicalize_etag(etag) == canonicalize_etag(condition)
}

pub fn should_prevent_write(oi: &ObjectInfo, if_none_match: Option<String>, if_match: Option<String>) -> bool {
    let if_none_match = if_none_match
        .as_deref()
        .map(str::trim)
        .filter(|condition| !condition.is_empty());
    let if_match = if_match.as_deref().map(str::trim).filter(|condition| !condition.is_empty());

    match &oi.etag {
        Some(etag) => {
            if let Some(if_none_match) = if_none_match
                && e_tag_matches(etag, if_none_match)
            {
                return true;
            }
            if let Some(if_match) = if_match
                && !e_tag_matches(etag, if_match)
            {
                return true;
            }
            false
        }
        // If we can't obtain the etag of the object, perevent the write only when we have at least one condition
        None => if_none_match.is_some() || if_match.is_some(),
    }
}

/// Validates if the given storage class is supported
pub fn is_valid_storage_class(storage_class: &str) -> bool {
    matches!(
        storage_class,
        storageclass::STANDARD
            | storageclass::RRS
            | storageclass::DEEP_ARCHIVE
            | storageclass::EXPRESS_ONEZONE
            | storageclass::GLACIER
            | storageclass::GLACIER_IR
            | storageclass::INTELLIGENT_TIERING
            | storageclass::ONEZONE_IA
            | storageclass::OUTPOSTS
            | storageclass::SNOW
            | storageclass::STANDARD_IA
    )
}

/// Returns true if the storage class is a cold storage tier that requires special handling
pub fn is_cold_storage_class(storage_class: &str) -> bool {
    matches!(
        storage_class,
        storageclass::DEEP_ARCHIVE | storageclass::GLACIER | storageclass::GLACIER_IR
    )
}

/// Returns true if the storage class is an infrequent access tier
pub fn is_infrequent_access_class(storage_class: &str) -> bool {
    matches!(
        storage_class,
        storageclass::ONEZONE_IA | storageclass::STANDARD_IA | storageclass::INTELLIGENT_TIERING
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::replication::{replication_statuses_map, version_purge_statuses_map};
    use crate::disk::CHECK_PART_UNKNOWN;
    use crate::disk::CHECK_PART_VOLUME_NOT_FOUND;
    use crate::disk::DiskOption;
    use crate::disk::RUSTFS_META_BUCKET;
    use crate::disk::RUSTFS_META_TMP_BUCKET;
    use crate::disk::STORAGE_FORMAT_FILE;
    use crate::disk::STORAGE_FORMAT_FILE_BACKUP;
    use crate::disk::WalkDirOptions;
    use crate::disk::endpoint::Endpoint;
    use crate::disk::error::DiskError;
    use crate::disk::health_state::RuntimeDriveHealthState;
    use crate::disk::new_disk;
    use crate::layout::endpoints::SetupType;
    use crate::object_api::ObjectInfo;
    use crate::storage_api_contracts::{
        heal::HealOperations as _, lifecycle::TransitionedObject, list::ListOperations as _, multipart::CompletePart,
        namespace::NamespaceLocking as _, object::ObjectOperations as _,
    };
    use crate::store::init_format::save_format_file;
    use crate::store::list_objects::ListPathOptions;
    use rustfs_filemeta::ErasureInfo;
    use rustfs_filemeta::FileMeta;
    use rustfs_filemeta::MetaCacheEntry;
    use rustfs_lock::client::local::LocalClient;
    use rustfs_lock::{LockError, LockInfo, LockResponse, LockStats};
    use serial_test::serial;
    use std::collections::HashMap;
    use tempfile::TempDir;
    use time::OffsetDateTime;
    use tokio::fs;

    #[test]
    fn complete_part_error_maps_confirmed_missing_to_invalid_part() {
        for err in ["file not found", "Specified part could not be found", "part.7 not found"] {
            let mapped = complete_multipart_part_error(7, err, "bucket", "object");

            assert!(matches!(
                mapped,
                Error::InvalidPart(7, ref bucket, ref object) if bucket == "bucket" && object == "object"
            ));
            assert_eq!(complete_multipart_part_error_result(&mapped), COMPLETE_MULTIPART_PART_MISSING);
        }
    }

    #[test]
    fn complete_part_error_maps_read_quorum_to_retryable_server_error() {
        let mapped = complete_multipart_part_error(1, "erasure read quorum", "bucket", "object");

        assert!(matches!(
            mapped,
            Error::InsufficientReadQuorum(ref bucket, ref object) if bucket == "bucket" && object == "object"
        ));
        assert_eq!(
            complete_multipart_part_error_result(&mapped),
            COMPLETE_MULTIPART_PART_READ_QUORUM_UNAVAILABLE
        );
    }

    #[test]
    fn complete_part_error_maps_unknown_part_error_to_retryable_server_error() {
        let mapped = complete_multipart_part_error(1, "metadata decode failed", "bucket", "object");

        assert!(matches!(mapped, Error::InsufficientReadQuorum(_, _)));
        assert_ne!(complete_multipart_part_error_result(&mapped), COMPLETE_MULTIPART_PART_MISSING);
    }

    #[derive(Debug, Default)]
    struct FailingClient;

    #[async_trait::async_trait]
    impl LockClient for FailingClient {
        async fn acquire_lock(&self, _request: &rustfs_lock::LockRequest) -> rustfs_lock::Result<LockResponse> {
            Err(LockError::internal("simulated offline client"))
        }

        async fn release(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            Ok(false)
        }

        async fn refresh(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            Ok(false)
        }

        async fn force_release(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            Ok(false)
        }

        async fn check_status(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<Option<LockInfo>> {
            Ok(None)
        }

        async fn get_stats(&self) -> rustfs_lock::Result<LockStats> {
            Ok(LockStats::default())
        }

        async fn close(&self) -> rustfs_lock::Result<()> {
            Ok(())
        }

        async fn is_online(&self) -> bool {
            false
        }

        async fn is_local(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct DelayedBatchClient {
        inner: Arc<dyn LockClient>,
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl LockClient for DelayedBatchClient {
        async fn acquire_lock(&self, request: &rustfs_lock::LockRequest) -> rustfs_lock::Result<LockResponse> {
            self.inner.acquire_lock(request).await
        }

        async fn acquire_locks_batch(&self, requests: &[rustfs_lock::LockRequest]) -> rustfs_lock::Result<Vec<LockResponse>> {
            tokio::time::sleep(self.delay).await;
            self.inner.acquire_locks_batch(requests).await
        }

        async fn release(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            self.inner.release(lock_id).await
        }

        async fn release_locks_batch(&self, lock_ids: &[rustfs_lock::LockId]) -> rustfs_lock::Result<Vec<bool>> {
            self.inner.release_locks_batch(lock_ids).await
        }

        async fn refresh(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            self.inner.refresh(lock_id).await
        }

        async fn force_release(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            self.inner.force_release(lock_id).await
        }

        async fn check_status(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<Option<LockInfo>> {
            self.inner.check_status(lock_id).await
        }

        async fn get_stats(&self) -> rustfs_lock::Result<LockStats> {
            self.inner.get_stats().await
        }

        async fn close(&self) -> rustfs_lock::Result<()> {
            self.inner.close().await
        }

        async fn is_online(&self) -> bool {
            self.inner.is_online().await
        }

        async fn is_local(&self) -> bool {
            self.inner.is_local().await
        }
    }

    async fn make_test_set_disks(lockers: Vec<Arc<dyn LockClient>>) -> Arc<SetDisks> {
        let endpoints = vec![
            Endpoint::try_from("http://127.0.0.1:9000/data").expect("first endpoint should parse"),
            Endpoint::try_from("http://127.0.0.1:9001/data").expect("second endpoint should parse"),
        ];

        SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(vec![None, None])),
            2,
            1,
            0,
            0,
            endpoints,
            FormatV3::new(1, 2),
            lockers,
        )
        .await
    }

    struct SetupTypeGuard {
        previous: SetupType,
    }

    impl SetupTypeGuard {
        async fn switch_to(next: SetupType) -> Self {
            let previous = current_setup_type().await;
            runtime_sources::set_setup_type(next).await;
            Self { previous }
        }
    }

    impl Drop for SetupTypeGuard {
        fn drop(&mut self) {
            let previous = self.previous.clone();
            let handle = tokio::runtime::Handle::current();
            tokio::task::block_in_place(|| {
                handle.block_on(async move {
                    runtime_sources::set_setup_type(previous).await;
                });
            });
        }
    }

    async fn current_setup_type() -> SetupType {
        runtime_sources::current_setup_type().await
    }

    async fn make_formatted_local_disk_for_info_test(disk_idx: usize, format: &FormatV3) -> (TempDir, Endpoint, DiskStore) {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let mut endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(disk_idx);

        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");

        let mut disk_format = format.clone();
        disk_format.erasure.this = format.erasure.sets[0][disk_idx];
        save_format_file(&Some(disk.clone()), &Some(disk_format))
            .await
            .expect("format should be saved");

        (dir, endpoint, disk)
    }

    #[tokio::test]
    async fn test_rename_data_quorum_failure_rolls_back_destination_object() {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let disk_root = dir.path().join("disk0");
        fs::create_dir_all(&disk_root).await.expect("disk root should be created");
        let endpoint = Endpoint::try_from(disk_root.to_str().expect("disk path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");

        let bucket = "bucket";
        let object = "object";
        let tmp_object = "tmp-object";
        let version_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("88888888-8888-8888-8888-888888888888").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("99999999-9999-9999-9999-999999999999").expect("new data dir should parse");

        match disk.make_volume(bucket).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("bucket should be available: {err:?}"),
        }
        match disk.make_volume(RUSTFS_META_TMP_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("tmp bucket should be available: {err:?}"),
        }

        let object_dir = disk_root.join(bucket).join(object);
        fs::create_dir_all(object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old data dir should be created");
        let mut old_fi = FileInfo::new(&format!("{bucket}/{object}"), 1, 1);
        old_fi.name = object.to_string();
        old_fi.version_id = Some(version_id);
        old_fi.data_dir = Some(old_data_dir);
        old_fi.size = 1;
        old_fi.mod_time = Some(OffsetDateTime::now_utc());
        let mut old_meta = FileMeta::default();
        old_meta.add_version(old_fi).expect("old metadata should accept file info");
        let old_meta_buf = old_meta.marshal_msg().expect("old metadata should encode");
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), old_meta_buf.clone())
            .await
            .expect("old metadata should be written");

        let tmp_data_dir = disk_root
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(new_data_dir.to_string());
        fs::create_dir_all(&tmp_data_dir)
            .await
            .expect("new tmp data dir should be created");
        fs::write(tmp_data_dir.join("part.1"), b"new")
            .await
            .expect("new tmp part should be written");

        let mut new_fi = FileInfo::new(&format!("{bucket}/{object}"), 1, 1);
        new_fi.name = object.to_string();
        new_fi.version_id = Some(version_id);
        new_fi.data_dir = Some(new_data_dir);
        new_fi.size = 1;
        new_fi.mod_time = Some(OffsetDateTime::now_utc());

        let disks = vec![Some(disk), None];
        let file_infos = vec![new_fi.clone(), new_fi];
        let result = SetDisks::rename_data(&disks, RUSTFS_META_TMP_BUCKET, tmp_object, &file_infos, bucket, object, 2).await;

        assert!(result.is_err());
        let restored_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("destination metadata should remain readable");
        assert_eq!(restored_meta, old_meta_buf);
        assert!(!object_dir.join(object).join(STORAGE_FORMAT_FILE).exists());
        assert!(!object_dir.join(new_data_dir.to_string()).exists());
        assert!(
            !object_dir
                .join(old_data_dir.to_string())
                .join(STORAGE_FORMAT_FILE_BACKUP)
                .exists()
        );
    }

    #[test]
    fn disk_health_entry_returns_cached_value_within_ttl() {
        let entry = DiskHealthEntry {
            last_check: Instant::now(),
            online: true,
        };

        assert_eq!(entry.cached_value(), Some(true));
    }

    #[test]
    fn disk_health_entry_expires_after_ttl() {
        let entry = DiskHealthEntry {
            last_check: Instant::now() - (DISK_HEALTH_CACHE_TTL + Duration::from_millis(100)),
            online: true,
        };

        assert!(entry.cached_value().is_none());
    }

    #[test]
    fn test_check_part_constants() {
        // Test that all CHECK_PART constants have expected values
        assert_eq!(CHECK_PART_UNKNOWN, 0);
        assert_eq!(CHECK_PART_SUCCESS, 1);
        assert_eq!(CHECK_PART_FILE_NOT_FOUND, 4); // The actual value is 4, not 2
        assert_eq!(CHECK_PART_VOLUME_NOT_FOUND, 3);
        assert_eq!(CHECK_PART_FILE_CORRUPT, 5);
    }

    #[test]
    fn test_is_min_allowed_part_size() {
        // Test minimum part size validation
        assert!(!is_min_allowed_part_size(0));
        assert!(!is_min_allowed_part_size(1024)); // 1KB - too small
        assert!(!is_min_allowed_part_size(1024 * 1024)); // 1MB - too small
        assert!(is_min_allowed_part_size(5 * 1024 * 1024)); // 5MB - minimum allowed
        assert!(is_min_allowed_part_size(10 * 1024 * 1024)); // 10MB - allowed
        assert!(is_min_allowed_part_size(100 * 1024 * 1024)); // 100MB - allowed
    }

    #[test]
    fn resolve_delete_version_state_clears_delete_marker_for_replica_marker_version_purge() {
        let opts = ObjectOptions {
            versioned: true,
            version_id: Some(Uuid::new_v4().to_string()),
            delete_replication: Some(ReplicationState {
                replica_status: ReplicationStatusType::Replica,
                ..Default::default()
            }),
            ..Default::default()
        };
        let current = ObjectInfo {
            version_id: Some(Uuid::new_v4()),
            delete_marker: true,
            ..Default::default()
        };

        let (mark_delete, delete_marker) = resolve_delete_version_state(&opts, &current, true);

        assert!(!mark_delete);
        assert!(
            !delete_marker,
            "replica purge of an existing delete marker version must remove that version, not preserve delete-marker semantics"
        );
    }

    #[test]
    fn resolve_delete_version_state_keeps_delete_marker_for_replica_marker_creation() {
        let opts = ObjectOptions {
            versioned: true,
            version_id: Some(Uuid::new_v4().to_string()),
            delete_marker: true,
            delete_replication: Some(ReplicationState {
                replica_status: ReplicationStatusType::Replica,
                ..Default::default()
            }),
            ..Default::default()
        };

        let (mark_delete, delete_marker) = resolve_delete_version_state(&opts, &ObjectInfo::default(), false);

        assert!(!mark_delete);
        assert!(delete_marker);
    }

    #[test]
    fn resolve_delete_version_state_creates_marker_for_missing_latest_versioned_delete() {
        let opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };

        let (mark_delete, delete_marker) = resolve_delete_version_state(&opts, &ObjectInfo::default(), false);

        assert!(mark_delete);
        assert!(delete_marker);
    }

    #[test]
    fn should_force_delete_marker_for_missing_version_rejects_data_movement_latest_delete() {
        let opts = ObjectOptions {
            versioned: true,
            data_movement: true,
            ..Default::default()
        };

        assert!(!should_force_delete_marker_for_missing_version(&opts));
    }

    #[test]
    fn should_force_delete_marker_for_missing_version_allows_explicit_marker_creation() {
        let opts = ObjectOptions {
            versioned: true,
            data_movement: true,
            delete_marker: true,
            ..Default::default()
        };

        assert!(should_force_delete_marker_for_missing_version(&opts));
    }

    #[test]
    fn resolve_delete_version_state_skips_marker_creation_for_replica_purge_when_version_missing() {
        let opts = ObjectOptions {
            versioned: true,
            version_id: Some(Uuid::new_v4().to_string()),
            delete_replication: Some(ReplicationState {
                replica_status: ReplicationStatusType::Replica,
                ..Default::default()
            }),
            ..Default::default()
        };

        let (mark_delete, delete_marker) = resolve_delete_version_state(&opts, &ObjectInfo::default(), false);

        assert!(
            !mark_delete,
            "replica delete-marker purges should not schedule mark-delete writes when the target version is absent"
        );
        assert!(
            !delete_marker,
            "replica delete-marker purges must become no-ops when the marker version has not arrived on the target yet"
        );
    }

    #[test]
    fn should_preserve_delete_replication_state_for_completed_delete_marker_replication_update() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            delete_replication: Some(ReplicationState {
                replicate_decision_str: "target=true;false;target;".to_string(),
                replication_status_internal: Some("target=COMPLETED;".to_string()),
                targets: replication_statuses_map("target=COMPLETED;"),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(
            should_preserve_delete_replication_state(&opts),
            "source delete-marker replication status updates must not be re-evaluated as fresh delete replication requests"
        );
    }

    #[test]
    fn should_not_preserve_delete_replication_state_for_new_version_delete_request() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        };

        assert!(
            !should_preserve_delete_replication_state(&opts),
            "fresh versioned deletes still need replication eligibility checks"
        );
    }

    #[test]
    fn resolve_delete_version_state_removes_source_delete_marker_version_during_purge_replication() {
        let opts = ObjectOptions {
            versioned: true,
            version_id: Some(Uuid::new_v4().to_string()),
            delete_replication: Some(ReplicationState {
                version_purge_status_internal: Some("target=PENDING;".to_string()),
                purge_targets: version_purge_statuses_map("target=PENDING;"),
                ..Default::default()
            }),
            ..Default::default()
        };
        let current = ObjectInfo {
            version_id: Some(Uuid::new_v4()),
            delete_marker: true,
            ..Default::default()
        };

        let (mark_delete, delete_marker) = resolve_delete_version_state(&opts, &current, true);

        assert!(
            !mark_delete,
            "source delete-marker version purge should delete the local marker instead of rewriting it with purge metadata"
        );
        assert!(
            !delete_marker,
            "source delete-marker version purge should not leave delete-marker semantics behind locally"
        );
    }

    #[test]
    fn test_get_complete_multipart_md5() {
        // Test MD5 calculation for multipart upload
        let parts = vec![
            CompletePart {
                part_num: 1,
                etag: Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
                checksum_crc32: None,
                checksum_crc32c: None,
                checksum_sha1: None,
                checksum_sha256: None,
                checksum_crc64nvme: None,
            },
            CompletePart {
                part_num: 2,
                etag: Some("098f6bcd4621d373cade4e832627b4f6".to_string()),
                checksum_crc32: None,
                checksum_crc32c: None,
                checksum_sha1: None,
                checksum_sha256: None,
                checksum_crc64nvme: None,
            },
        ];

        let md5 = get_complete_multipart_md5(&parts);
        assert!(md5.ends_with("-2")); // Should end with part count
        assert!(md5.len() > 10); // Should have reasonable length

        // Test with empty parts
        let empty_parts = vec![];
        let empty_result = get_complete_multipart_md5(&empty_parts);
        assert!(empty_result.ends_with("-0"));

        // Test with single part
        let single_part = vec![CompletePart {
            part_num: 1,
            etag: Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
            checksum_crc32: None,
            checksum_crc32c: None,
            checksum_sha1: None,
            checksum_sha256: None,
            checksum_crc64nvme: None,
        }];
        let single_result = get_complete_multipart_md5(&single_part);
        assert!(single_result.ends_with("-1"));
    }

    #[test]
    fn test_completed_multipart_object_part_preserves_checksums() {
        let checksums = HashMap::from([
            (rustfs_rio::ChecksumType::CRC32.to_string(), "crc32-value".to_string()),
            (rustfs_rio::ChecksumType::CRC32C.to_string(), "crc32c-value".to_string()),
        ]);
        let ext_part = ObjectPartInfo {
            number: 7,
            etag: "etag-7".to_string(),
            size: 123,
            actual_size: 456,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            index: Some(Bytes::from_static(&[1, 2, 3])),
            checksums: Some(checksums.clone()),
            ..Default::default()
        };

        let completed = completed_multipart_object_part(7, &ext_part);

        assert_eq!(completed.number, 7);
        assert_eq!(completed.etag, ext_part.etag);
        assert_eq!(completed.size, ext_part.size);
        assert_eq!(completed.actual_size, ext_part.actual_size);
        assert_eq!(completed.index, ext_part.index);
        assert_eq!(completed.checksums, Some(checksums));
    }

    #[test]
    fn test_get_upload_id_dir() {
        // Test upload ID directory path generation
        let dir = SetDisks::get_upload_id_dir("bucket", "object", "upload-id");
        // The function returns SHA256 hash of bucket/object + upload_id processing
        assert!(dir.len() > 64); // Should be longer than just SHA256 hash
        assert!(dir.contains("/")); // Should contain path separator

        // Test with base64 encoded upload ID
        let result2 = SetDisks::get_upload_id_dir("bucket", "object", "dXBsb2FkLWlk"); // base64 for "upload-id"
        assert!(!result2.is_empty());
        assert!(result2.len() > 10);
    }

    #[test]
    fn test_get_multipart_sha_dir() {
        // Test multipart SHA directory path generation
        let dir = SetDisks::get_multipart_sha_dir("bucket", "object");
        // The function returns SHA256 hash of "bucket/object"
        assert_eq!(dir.len(), 64); // SHA256 hash length
        assert!(!dir.contains("bucket")); // Should be hash, not original text
        assert!(!dir.contains("object")); // Should be hash, not original text

        // Test with empty strings
        let result2 = SetDisks::get_multipart_sha_dir("", "");
        assert!(!result2.is_empty());
        assert_eq!(result2.len(), 64); // SHA256 hex string length

        // Test that different inputs produce different hashes
        let result3 = SetDisks::get_multipart_sha_dir("bucket1", "object1");
        let result4 = SetDisks::get_multipart_sha_dir("bucket2", "object2");
        assert_ne!(result3, result4);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_new_ns_lock_distributed_read_succeeds_with_two_lockers_one_offline() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;

        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let healthy_client: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager));
        let failing_client: Arc<dyn LockClient> = Arc::new(FailingClient);
        let set_disks = make_test_set_disks(vec![healthy_client, failing_client]).await;

        let guard = set_disks
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_read_lock(Duration::from_millis(100))
            .await
            .expect("read lock should succeed with one healthy locker");

        match guard {
            NamespaceLockGuard::Standard(_) => {}
            NamespaceLockGuard::Fast(_) => panic!("Expected distributed guard for dist-erasure"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_new_ns_lock_distributed_write_fails_with_two_lockers_one_offline() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;

        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let healthy_client: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager));
        let failing_client: Arc<dyn LockClient> = Arc::new(FailingClient);
        let set_disks = make_test_set_disks(vec![healthy_client, failing_client]).await;

        let err = set_disks
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_millis(100))
            .await
            .expect_err("write lock should fail with one healthy locker");

        let err_str = err.to_string().to_lowercase();
        assert!(
            err_str.contains("quorum") || err_str.contains("not reached"),
            "expected quorum error, got: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn copy_object_honors_no_lock_when_outer_write_lock_is_held() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::Erasure).await;
        let set_disks = make_test_set_disks(vec![Arc::new(LocalClient::with_manager(Arc::new(
            rustfs_lock::GlobalLockManager::new(),
        )))])
        .await;

        let _outer_guard = set_disks
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("outer write lock should be acquired");

        let mut src_info = ObjectInfo {
            metadata_only: true,
            ..Default::default()
        };
        let dst_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        let result = timeout(
            Duration::from_secs(1),
            set_disks.copy_object(
                "bucket",
                "object",
                "bucket",
                "object",
                &mut src_info,
                &ObjectOptions::default(),
                &dst_opts,
            ),
        )
        .await
        .expect("no_lock copy path must not wait for the outer lock");

        let err = result.expect_err("empty test disks should fail after bypassing the inner lock");
        assert!(
            !err.to_string().to_ascii_lowercase().contains("lock"),
            "copy_object returned a lock error despite no_lock=true: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn copy_object_rejects_metadata_only_cross_key() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::Erasure).await;
        let set_disks = make_test_set_disks(vec![Arc::new(LocalClient::with_manager(Arc::new(
            rustfs_lock::GlobalLockManager::new(),
        )))])
        .await;

        let mut src_info = ObjectInfo {
            metadata_only: true,
            ..Default::default()
        };

        let err = set_disks
            .copy_object(
                "bucket",
                "source",
                "bucket",
                "dest",
                &mut src_info,
                &ObjectOptions::default(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("metadata-only lower copy is only valid for self-copy updates");

        assert!(matches!(err, StorageError::NotImplemented));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn delete_object_honors_no_lock_when_outer_write_lock_is_held() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::Erasure).await;
        let set_disks = make_test_set_disks(vec![Arc::new(LocalClient::with_manager(Arc::new(
            rustfs_lock::GlobalLockManager::new(),
        )))])
        .await;

        let _outer_guard = set_disks
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("outer write lock should be acquired");

        let result = timeout(
            Duration::from_secs(1),
            set_disks.delete_object(
                "bucket",
                "object",
                ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            ),
        )
        .await
        .expect("no_lock delete path must not wait for the outer lock");

        let err = result.expect_err("empty test disks should fail after bypassing the inner lock");
        assert!(
            !err.to_string().to_ascii_lowercase().contains("lock"),
            "delete_object returned a lock error despite no_lock=true: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn delete_prefix_does_not_lock_literal_prefix_key() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::Erasure).await;
        let set_disks = make_test_set_disks(vec![Arc::new(LocalClient::with_manager(Arc::new(
            rustfs_lock::GlobalLockManager::new(),
        )))])
        .await;

        let _outer_guard = set_disks
            .new_ns_lock("bucket", "prefix")
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("outer write lock should be acquired");

        timeout(
            Duration::from_secs(1),
            set_disks.delete_object(
                "bucket",
                "prefix",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            ),
        )
        .await
        .expect("broad prefix delete must not wait on a literal prefix namespace lock")
        .expect("empty test disks should allow broad prefix cleanup");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn delete_prefix_object_honors_no_lock_when_outer_write_lock_is_held() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::Erasure).await;
        let set_disks = make_test_set_disks(vec![Arc::new(LocalClient::with_manager(Arc::new(
            rustfs_lock::GlobalLockManager::new(),
        )))])
        .await;

        let _outer_guard = set_disks
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("outer write lock should be acquired");

        timeout(
            Duration::from_secs(1),
            set_disks.delete_object(
                "bucket",
                "object",
                ObjectOptions {
                    delete_prefix: true,
                    delete_prefix_object: true,
                    no_lock: true,
                    ..Default::default()
                },
            ),
        )
        .await
        .expect("no_lock exact prefix delete path must not wait for the outer lock")
        .expect("empty test disks should allow exact prefix cleanup");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn delete_prefix_object_locks_real_object_key() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::Erasure).await;
        let set_disks = make_test_set_disks(vec![Arc::new(LocalClient::with_manager(Arc::new(
            rustfs_lock::GlobalLockManager::new(),
        )))])
        .await;

        let _outer_guard = set_disks
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("outer write lock should be acquired");

        let result = timeout(
            Duration::from_millis(50),
            set_disks.delete_object(
                "bucket",
                "object",
                ObjectOptions {
                    delete_prefix: true,
                    delete_prefix_object: true,
                    ..Default::default()
                },
            ),
        )
        .await;

        assert!(result.is_err(), "exact prefix delete should wait on the real object namespace lock");
    }

    async fn make_single_local_disk() -> (TempDir, DiskStore) {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");
        (dir, disk)
    }

    async fn make_set_disks_with(disks: Vec<Option<DiskStore>>) -> Arc<SetDisks> {
        let drive_count = disks.len();
        let endpoints = (0..drive_count)
            .map(|i| Endpoint::try_from(format!("http://127.0.0.1:{}/data", 9000 + i).as_str()).expect("endpoint should parse"))
            .collect::<Vec<_>>();

        SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            drive_count,
            0,
            0,
            0,
            endpoints,
            FormatV3::new(1, drive_count),
            vec![Arc::new(LocalClient::with_manager(Arc::new(
                rustfs_lock::GlobalLockManager::new(),
            )))],
        )
        .await
    }

    // issue #4189: an orphan directory tree (empty dirs, no xl.meta) must be purged.
    #[tokio::test]
    async fn purge_orphan_dir_object_removes_empty_tree() {
        let (dir, disk) = make_single_local_disk().await;
        let root = dir.path();
        fs::create_dir_all(root.join("bucket").join("pfx").join("a").join("b"))
            .await
            .expect("nested empty dir should be created");
        fs::create_dir_all(root.join("bucket").join("pfx").join("c"))
            .await
            .expect("sibling empty dir should be created");

        let set = make_set_disks_with(vec![Some(disk)]).await;
        let purged = set
            .purge_orphan_dir_object("bucket", "pfx/")
            .await
            .expect("purge should succeed");

        assert!(purged, "orphan empty tree should be purged");
        assert!(!root.join("bucket").join("pfx").exists(), "prefix directory should be gone");
        assert!(root.join("bucket").exists(), "bucket volume should remain");
    }

    // issue #4189: a prefix that still anchors a real object must be left intact.
    #[tokio::test]
    async fn purge_orphan_dir_object_preserves_prefix_with_object() {
        let (dir, disk) = make_single_local_disk().await;
        let root = dir.path();
        let obj_dir = root.join("bucket").join("pfx").join("obj");
        fs::create_dir_all(&obj_dir).await.expect("object dir should be created");
        fs::write(obj_dir.join(STORAGE_FORMAT_FILE), b"meta")
            .await
            .expect("object metadata should be written");

        let set = make_set_disks_with(vec![Some(disk)]).await;
        let purged = set
            .purge_orphan_dir_object("bucket", "pfx/")
            .await
            .expect("scan should succeed");

        assert!(!purged, "prefix containing an object must not be purged");
        assert!(obj_dir.join(STORAGE_FORMAT_FILE).exists(), "object metadata must be preserved");
    }

    #[tokio::test]
    async fn purge_orphan_dir_object_missing_returns_false() {
        let (dir, disk) = make_single_local_disk().await;
        fs::create_dir_all(dir.path().join("bucket"))
            .await
            .expect("bucket volume should be created");

        let set = make_set_disks_with(vec![Some(disk)]).await;
        let purged = set
            .purge_orphan_dir_object("bucket", "does-not-exist/")
            .await
            .expect("scan should succeed");

        assert!(!purged, "a missing prefix should report nothing to purge");
    }

    // Cross-disk safety: if any drive still holds object data under the prefix, refuse
    // to purge on every drive so a degraded/healable object is never destroyed.
    #[tokio::test]
    async fn purge_orphan_dir_object_refuses_when_any_disk_has_data() {
        let (dir0, disk0) = make_single_local_disk().await;
        let (dir1, disk1) = make_single_local_disk().await;

        fs::create_dir_all(dir0.path().join("bucket").join("pfx").join("a"))
            .await
            .expect("disk0 empty tree should be created");

        let obj_dir = dir1.path().join("bucket").join("pfx").join("a");
        fs::create_dir_all(&obj_dir)
            .await
            .expect("disk1 object dir should be created");
        fs::write(obj_dir.join(STORAGE_FORMAT_FILE), b"meta")
            .await
            .expect("disk1 object metadata should be written");

        let set = make_set_disks_with(vec![Some(disk0), Some(disk1)]).await;
        let purged = set
            .purge_orphan_dir_object("bucket", "pfx/")
            .await
            .expect("scan should succeed");

        assert!(!purged, "must not purge when any disk holds object data");
        assert!(
            obj_dir.join(STORAGE_FORMAT_FILE).exists(),
            "object metadata on the healthy disk must be preserved"
        );
        assert!(
            dir0.path().join("bucket").join("pfx").join("a").exists(),
            "empty tree must be left untouched when the purge is aborted"
        );
    }

    // Build an `xl.meta` under `object_dir` whose versions reference `data_dirs`
    // (one Object version per data dir, each with its own version id).
    async fn write_object_meta_with_data_dirs(object_dir: &std::path::Path, bucket: &str, object: &str, data_dirs: &[Uuid]) {
        fs::create_dir_all(object_dir).await.expect("object dir should be created");
        let mut meta = FileMeta::default();
        for data_dir in data_dirs {
            let mut fi = FileInfo::new(&format!("{bucket}/{object}"), 1, 1);
            fi.name = object.to_string();
            fi.version_id = Some(Uuid::new_v4());
            fi.data_dir = Some(*data_dir);
            fi.size = 1;
            fi.mod_time = Some(OffsetDateTime::now_utc());
            meta.add_version(fi).expect("metadata should accept file info");
        }
        let buf = meta.marshal_msg().expect("metadata should encode");
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), buf)
            .await
            .expect("metadata should be written");
    }

    // #3231/#3191: a data dir on disk that no version references (a pre-#3510
    // unversioned-overwrite leak) must be reclaimed, leaving the live one intact.
    #[tokio::test]
    async fn reclaim_orphan_data_dirs_removes_unreferenced_dir() {
        let (dir, disk) = make_single_local_disk().await;
        let root = dir.path();
        let live = Uuid::new_v4();
        let orphan = Uuid::new_v4();

        let object_dir = root.join("bucket").join("obj");
        write_object_meta_with_data_dirs(&object_dir, "bucket", "obj", &[live]).await;
        fs::create_dir_all(object_dir.join(live.to_string()))
            .await
            .expect("live data dir should be created");
        fs::write(object_dir.join(live.to_string()).join("part.1"), b"live")
            .await
            .expect("live part should be written");
        fs::create_dir_all(object_dir.join(orphan.to_string()))
            .await
            .expect("orphan data dir should be created");
        fs::write(object_dir.join(orphan.to_string()).join("part.1"), b"stale")
            .await
            .expect("orphan part should be written");

        let set = make_set_disks_with(vec![Some(disk)]).await;
        let removed = set
            .reclaim_orphan_data_dirs("bucket", "obj")
            .await
            .expect("reclaim should succeed");

        assert_eq!(removed, 1, "exactly the unreferenced data dir should be removed");
        assert!(object_dir.join(live.to_string()).exists(), "referenced data dir must be preserved");
        assert!(!object_dir.join(orphan.to_string()).exists(), "orphaned data dir must be removed");
        assert!(object_dir.join(STORAGE_FORMAT_FILE).exists(), "metadata must be preserved");
    }

    // Nothing to reclaim when every physical data dir is still referenced.
    #[tokio::test]
    async fn reclaim_orphan_data_dirs_keeps_referenced_dir() {
        let (dir, disk) = make_single_local_disk().await;
        let root = dir.path();
        let live = Uuid::new_v4();

        let object_dir = root.join("bucket").join("obj");
        write_object_meta_with_data_dirs(&object_dir, "bucket", "obj", &[live]).await;
        fs::create_dir_all(object_dir.join(live.to_string()))
            .await
            .expect("live data dir should be created");

        let set = make_set_disks_with(vec![Some(disk)]).await;
        let removed = set
            .reclaim_orphan_data_dirs("bucket", "obj")
            .await
            .expect("reclaim should succeed");

        assert_eq!(removed, 0, "no data dir should be removed");
        assert!(object_dir.join(live.to_string()).exists(), "referenced data dir must be preserved");
    }

    // Fail closed: a data dir present without a readable xl.meta is degraded, and
    // must never be removed (a heal has to run first).
    #[tokio::test]
    async fn reclaim_orphan_data_dirs_aborts_when_meta_missing() {
        let (dir, disk) = make_single_local_disk().await;
        let root = dir.path();
        let stray = Uuid::new_v4();

        let object_dir = root.join("bucket").join("obj");
        fs::create_dir_all(object_dir.join(stray.to_string()))
            .await
            .expect("data dir should be created");
        fs::write(object_dir.join(stray.to_string()).join("part.1"), b"data")
            .await
            .expect("part should be written");

        let set = make_set_disks_with(vec![Some(disk)]).await;
        let removed = set
            .reclaim_orphan_data_dirs("bucket", "obj")
            .await
            .expect("reclaim should succeed");

        assert_eq!(removed, 0, "must not remove data dirs when metadata is absent");
        assert!(
            object_dir.join(stray.to_string()).exists(),
            "degraded object's data dir must be preserved"
        );
    }

    // Cross-replica union: a data dir referenced by ANOTHER disk's xl.meta must be
    // kept even where the local replica does not name it.
    #[tokio::test]
    async fn reclaim_orphan_data_dirs_keeps_dir_referenced_by_other_replica() {
        let (dir0, disk0) = make_single_local_disk().await;
        let (dir1, disk1) = make_single_local_disk().await;
        let shared = Uuid::new_v4();

        // disk0: meta references only a different dir, but physically holds `shared`.
        let other = Uuid::new_v4();
        let obj0 = dir0.path().join("bucket").join("obj");
        write_object_meta_with_data_dirs(&obj0, "bucket", "obj", &[other]).await;
        fs::create_dir_all(obj0.join(other.to_string()))
            .await
            .expect("dir should be created");
        fs::create_dir_all(obj0.join(shared.to_string()))
            .await
            .expect("dir should be created");

        // disk1: meta references `shared`.
        let obj1 = dir1.path().join("bucket").join("obj");
        write_object_meta_with_data_dirs(&obj1, "bucket", "obj", &[shared]).await;
        fs::create_dir_all(obj1.join(shared.to_string()))
            .await
            .expect("dir should be created");

        let set = make_set_disks_with(vec![Some(disk0), Some(disk1)]).await;
        let removed = set
            .reclaim_orphan_data_dirs("bucket", "obj")
            .await
            .expect("reclaim should succeed");

        assert_eq!(removed, 0, "a dir referenced by any replica must be kept");
        assert!(obj0.join(shared.to_string()).exists(), "cross-referenced data dir must survive");
        assert!(obj0.join(other.to_string()).exists(), "locally referenced data dir must survive");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_acquire_dist_delete_object_locks_batch_succeeds_with_two_healthy_lockers() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;

        let manager1 = Arc::new(rustfs_lock::GlobalLockManager::new());
        let manager2 = Arc::new(rustfs_lock::GlobalLockManager::new());
        let client1: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager1.clone()));
        let client2: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager2.clone()));
        let set_disks = make_test_set_disks(vec![client1, client2]).await;

        let batch = rustfs_lock::BatchLockRequest::new(set_disks.locker_owner.as_str())
            .with_all_or_nothing(false)
            .add_write_lock(ObjectKey::new("bucket", "object-a"))
            .add_write_lock(ObjectKey::new("bucket", "object-b"));

        let (failed_map, locked_objects, held_lock_ids_by_client) =
            set_disks.acquire_dist_delete_object_locks_batch(&batch).await;

        assert!(failed_map.is_empty());
        assert_eq!(locked_objects.len(), 2);
        assert!(locked_objects.contains("object-a"));
        assert!(locked_objects.contains("object-b"));
        assert_eq!(held_lock_ids_by_client.iter().map(Vec::len).sum::<usize>(), batch.requests.len() * 2);

        set_disks
            .release_dist_delete_object_locks_batch(held_lock_ids_by_client)
            .await;

        let local_lock_1 = NamespaceLock::with_local_manager("node-1".to_string(), manager1);
        let local_lock_2 = NamespaceLock::with_local_manager("node-2".to_string(), manager2);

        let guard_1 = local_lock_1
            .get_write_lock(ObjectKey::new("bucket", "object-a"), "owner-b", Duration::from_millis(100))
            .await
            .expect("released batch lock should free node 1");
        let guard_2 = local_lock_2
            .get_write_lock(ObjectKey::new("bucket", "object-b"), "owner-b", Duration::from_millis(100))
            .await
            .expect("released batch lock should free node 2");

        drop(guard_1);
        drop(guard_2);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_acquire_dist_delete_object_locks_batch_rolls_back_when_quorum_not_reached() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;

        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let healthy_client: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager.clone()));
        let failing_client: Arc<dyn LockClient> = Arc::new(FailingClient);
        let set_disks = make_test_set_disks(vec![healthy_client, failing_client]).await;

        let batch = rustfs_lock::BatchLockRequest::new(set_disks.locker_owner.as_str())
            .with_all_or_nothing(false)
            .add_write_lock(ObjectKey::new("bucket", "object-a"));

        let (failed_map, locked_objects, held_lock_ids_by_client) =
            set_disks.acquire_dist_delete_object_locks_batch(&batch).await;

        assert!(locked_objects.is_empty());
        assert!(failed_map.contains_key(&("bucket".to_string(), "object-a".to_string())));
        assert_eq!(held_lock_ids_by_client.iter().map(Vec::len).sum::<usize>(), 0);

        let local_lock = NamespaceLock::with_local_manager("node-1".to_string(), manager);
        let guard = local_lock
            .get_write_lock(ObjectKey::new("bucket", "object-a"), "owner-b", Duration::from_millis(100))
            .await
            .expect("quorum rollback should release the healthy node lock");

        drop(guard);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_acquire_dist_delete_object_locks_batch_returns_after_quorum_without_waiting_for_slow_lockers() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;

        let manager_fast_1 = Arc::new(rustfs_lock::GlobalLockManager::new());
        let manager_fast_2 = Arc::new(rustfs_lock::GlobalLockManager::new());
        let manager_fast_3 = Arc::new(rustfs_lock::GlobalLockManager::new());
        let manager_slow = Arc::new(rustfs_lock::GlobalLockManager::new());

        let client_fast_1: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager_fast_1));
        let client_fast_2: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager_fast_2));
        let client_fast_3: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager_fast_3));
        let client_slow: Arc<dyn LockClient> = Arc::new(DelayedBatchClient {
            inner: Arc::new(LocalClient::with_manager(manager_slow.clone())),
            delay: Duration::from_millis(250),
        });

        let set_disks = make_test_set_disks(vec![client_fast_1, client_fast_2, client_fast_3, client_slow]).await;

        let batch = rustfs_lock::BatchLockRequest::new(set_disks.locker_owner.as_str())
            .with_all_or_nothing(false)
            .add_write_lock(ObjectKey::new("bucket", "object-a"))
            .add_write_lock(ObjectKey::new("bucket", "object-b"));

        let started = Instant::now();
        let (failed_map, locked_objects, held_lock_ids_by_client) =
            set_disks.acquire_dist_delete_object_locks_batch(&batch).await;

        assert!(
            started.elapsed() < Duration::from_millis(150),
            "batch distributed delete locks should return once quorum is satisfied"
        );
        assert!(failed_map.is_empty());
        assert_eq!(locked_objects.len(), 2);

        set_disks
            .release_dist_delete_object_locks_batch(held_lock_ids_by_client)
            .await;

        tokio::time::sleep(Duration::from_millis(350)).await;

        let slow_lock = NamespaceLock::with_local_manager("slow-node".to_string(), manager_slow);
        let guard_a = slow_lock
            .get_write_lock(ObjectKey::new("bucket", "object-a"), "owner-b", Duration::from_millis(100))
            .await
            .expect("late successful batch lock should be cleaned up for object-a");
        let guard_b = slow_lock
            .get_write_lock(ObjectKey::new("bucket", "object-b"), "owner-b", Duration::from_millis(100))
            .await
            .expect("late successful batch lock should be cleaned up for object-b");

        drop(guard_a);
        drop(guard_b);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_acquire_dist_delete_object_locks_batch_fails_early_and_cleans_up_late_successes() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;

        let manager_fast = Arc::new(rustfs_lock::GlobalLockManager::new());
        let manager_slow = Arc::new(rustfs_lock::GlobalLockManager::new());

        let client_fast: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager_fast));
        let client_fail_1: Arc<dyn LockClient> = Arc::new(FailingClient);
        let client_fail_2: Arc<dyn LockClient> = Arc::new(FailingClient);
        let client_slow: Arc<dyn LockClient> = Arc::new(DelayedBatchClient {
            inner: Arc::new(LocalClient::with_manager(manager_slow.clone())),
            delay: Duration::from_millis(250),
        });

        let set_disks = make_test_set_disks(vec![client_fast, client_fail_1, client_fail_2, client_slow]).await;
        let batch = rustfs_lock::BatchLockRequest::new(set_disks.locker_owner.as_str())
            .with_all_or_nothing(false)
            .add_write_lock(ObjectKey::new("bucket", "object-a"))
            .add_write_lock(ObjectKey::new("bucket", "object-b"));

        let started = Instant::now();
        let (failed_map, locked_objects, held_lock_ids_by_client) =
            set_disks.acquire_dist_delete_object_locks_batch(&batch).await;

        assert!(
            started.elapsed() < Duration::from_millis(150),
            "batch distributed delete locks should fail as soon as quorum becomes impossible"
        );
        assert!(locked_objects.is_empty());
        assert!(failed_map.contains_key(&("bucket".to_string(), "object-a".to_string())));
        assert!(failed_map.contains_key(&("bucket".to_string(), "object-b".to_string())));
        assert_eq!(held_lock_ids_by_client.iter().map(Vec::len).sum::<usize>(), 0);

        tokio::time::sleep(Duration::from_millis(350)).await;

        let slow_lock = NamespaceLock::with_local_manager("slow-node".to_string(), manager_slow);
        let guard_a = slow_lock
            .get_write_lock(ObjectKey::new("bucket", "object-a"), "owner-b", Duration::from_millis(100))
            .await
            .expect("late successful batch failure cleanup should release object-a");
        let guard_b = slow_lock
            .get_write_lock(ObjectKey::new("bucket", "object-b"), "owner-b", Duration::from_millis(100))
            .await
            .expect("late successful batch failure cleanup should release object-b");

        drop(guard_a);
        drop(guard_b);
    }

    #[test]
    fn test_common_parity() {
        // Test common parity calculation
        // For parities [2, 2, 2, 3] with n=4, default_parity_count=1:
        // - parity=2: read_quorum = 4-2 = 2, occ=3 >= 2, so valid
        // - parity=3: read_quorum = 4-3 = 1, occ=1 >= 1, so valid
        // - max_occ=3 for parity=2, so returns 2
        let parities = vec![2, 2, 2, 3];
        assert_eq!(SetDisks::common_parity(&parities, 1), 2);

        // For parities [1, 2, 3] with n=3, default_parity_count=2:
        // - parity=1: read_quorum = 3-1 = 2, occ=1 < 2, so invalid
        // - parity=2: read_quorum = 3-2 = 1, occ=1 >= 1, so valid
        // - parity=3: read_quorum = 3-3 = 0, occ=1 >= 0, so valid
        // - max_occ=1, both parity=2 and parity=3 have same occurrence
        // - HashMap iteration order is not guaranteed, so result could be either 2 or 3
        let parities = vec![1, 2, 3];
        let result = SetDisks::common_parity(&parities, 2);
        assert!(result == 2 || result == 3); // Either 2 or 3 is valid

        let empty_parities = vec![];
        assert_eq!(SetDisks::common_parity(&empty_parities, 3), -1); // Empty returns -1

        let invalid_parities = vec![-1, -1, -1];
        assert_eq!(SetDisks::common_parity(&invalid_parities, 2), -1); // all invalid

        let single_parity = vec![4];
        assert_eq!(SetDisks::common_parity(&single_parity, 1), 4);

        // Test with -1 values (ignored)
        let parities_with_invalid = vec![-1, 2, 2, -1];
        assert_eq!(SetDisks::common_parity(&parities_with_invalid, 1), 2);
    }

    #[test]
    fn test_common_time() {
        // Test common time calculation
        let now = OffsetDateTime::now_utc();
        let later = now + Duration::from_secs(60);

        let times = vec![Some(now), Some(now), Some(later)];
        assert_eq!(SetDisks::common_time(&times, 2), Some(now));

        let times2 = vec![Some(now), Some(later), Some(later)];
        assert_eq!(SetDisks::common_time(&times2, 2), Some(later));

        let times_with_none = vec![Some(now), None, Some(now)];
        assert_eq!(SetDisks::common_time(&times_with_none, 2), Some(now));

        let times = vec![None, None, None];
        assert_eq!(SetDisks::common_time(&times, 2), None);

        let empty_times = vec![];
        assert_eq!(SetDisks::common_time(&empty_times, 1), None);
    }

    #[test]
    fn test_common_time_and_occurrence() {
        // Test common time with occurrence count
        let now = OffsetDateTime::now_utc();
        let times = vec![Some(now), Some(now), None];
        let (time, count) = SetDisks::common_time_and_occurrence(&times);
        assert_eq!(time, Some(now));
        assert_eq!(count, 2);

        let times = vec![None, None, None];
        let (time, count) = SetDisks::common_time_and_occurrence(&times);
        assert_eq!(time, None);
        assert_eq!(count, 0); // No valid times, so count is 0
    }

    #[test]
    fn test_common_etag() {
        // Test common etag calculation
        let etags = vec![Some("etag1".to_string()), Some("etag1".to_string()), None];
        assert_eq!(SetDisks::common_etag(&etags, 2), Some("etag1".to_string()));

        let etags = vec![None, None, None];
        assert_eq!(SetDisks::common_etag(&etags, 2), None);
    }

    #[test]
    fn test_common_etags() {
        // Test common etags with occurrence count
        let etags = vec![Some("etag1".to_string()), Some("etag1".to_string()), None];
        let (etag, count) = SetDisks::common_etags(&etags);
        assert_eq!(etag, Some("etag1".to_string()));
        assert_eq!(count, 2);
    }

    #[test]
    fn test_list_object_modtimes() {
        // Test extracting modification times from file info
        let now = OffsetDateTime::now_utc();
        let file_info = FileInfo {
            mod_time: Some(now),
            ..Default::default()
        };
        let parts_metadata = vec![file_info];
        let errs = vec![None];

        let modtimes = SetDisks::list_object_modtimes(&parts_metadata, &errs);
        assert_eq!(modtimes.len(), 1);
        assert_eq!(modtimes[0], Some(now));
    }

    #[test]
    fn test_list_object_etags() {
        // Test extracting etags from file info metadata
        let mut metadata = HashMap::new();
        metadata.insert("etag".to_string(), "test-etag".to_string());

        let file_info = FileInfo {
            metadata,
            ..Default::default()
        };
        let parts_metadata = vec![file_info];
        let errs = vec![None];

        let etags = SetDisks::list_object_etags(&parts_metadata, &errs);
        assert_eq!(etags.len(), 1);
        assert_eq!(etags[0], Some("test-etag".to_string()));
    }

    fn quorum_test_fileinfo(mod_time: OffsetDateTime, data_dir: Uuid, part_etag: &str, erasure_index: usize) -> FileInfo {
        let mut metadata = HashMap::new();
        metadata.insert("etag".to_string(), "object-etag".to_string());

        FileInfo {
            name: "bucket/object".to_string(),
            size: 8 * 1024 * 1024,
            mod_time: Some(mod_time),
            data_dir: Some(data_dir),
            metadata,
            parts: vec![ObjectPartInfo {
                etag: part_etag.to_string(),
                number: 1,
                size: 8 * 1024 * 1024,
                actual_size: 8 * 1024 * 1024,
                mod_time: Some(mod_time),
                ..Default::default()
            }],
            erasure: ErasureInfo {
                data_blocks: 2,
                parity_blocks: 2,
                block_size: 4 * 1024 * 1024,
                index: erasure_index,
                distribution: vec![1, 2, 3, 4],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn decoded_quorum_test_fileinfo_with_metadata(
        mod_time: OffsetDateTime,
        data_dir: Uuid,
        part_etag: &str,
        erasure_index: usize,
        extra_metadata: &[(&str, &str)],
    ) -> FileInfo {
        let mut fi = quorum_test_fileinfo(mod_time, data_dir, part_etag, erasure_index);
        for (name, value) in extra_metadata {
            fi.metadata.insert((*name).to_string(), (*value).to_string());
        }

        let mut meta = FileMeta::new();
        meta.add_version(fi).expect("test file metadata should accept object version");
        let encoded = meta.marshal_msg().expect("test file metadata should marshal");
        rustfs_filemeta::get_file_info(
            &encoded,
            "bucket",
            "object",
            "",
            rustfs_filemeta::FileInfoOpts {
                data: false,
                include_free_versions: false,
            },
        )
        .expect("test file metadata should decode as file info")
    }

    #[test]
    fn test_find_file_info_in_quorum_uses_part_identity() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let metas = vec![
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 1),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 2),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 3),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-b", 4),
        ];

        let fi = SetDisks::find_file_info_in_quorum(&metas, &Some(mod_time), &None, 3)
            .expect("three matching part identities should reach quorum");

        assert_eq!(fi.parts[0].etag, "part-etag-a");
    }

    #[test]
    fn test_find_file_info_in_quorum_rejects_split_part_identity() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let metas = vec![
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 1),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 2),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-b", 3),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-b", 4),
        ];

        let err = SetDisks::find_file_info_in_quorum(&metas, &Some(mod_time), &None, 3)
            .expect_err("split part identities must not reach write quorum");

        assert_eq!(err, DiskError::ErasureReadQuorum);
    }

    #[test]
    fn test_latest_fileinfo_selection_quorum_requires_write_quorum_when_full_metadata_is_available() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let metas = vec![
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 1),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 2),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-b", 3),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-b", 4),
        ];
        let errs = vec![None, None, None, None];

        let quorum = SetDisks::latest_fileinfo_selection_quorum("", &metas, &errs, 2, 3);

        assert_eq!(quorum, 3);
    }

    #[test]
    fn test_latest_fileinfo_selection_quorum_preserves_read_quorum_for_version_or_degraded_reads() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let metas = vec![
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 1),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 2),
            FileInfo::default(),
            FileInfo::default(),
        ];
        let degraded_errs = vec![None, None, Some(DiskError::DiskNotFound), Some(DiskError::DiskNotFound)];
        let clean_errs = vec![None, None, None, None];

        assert_eq!(SetDisks::latest_fileinfo_selection_quorum("", &metas, &degraded_errs, 2, 3), 2);
        assert_eq!(SetDisks::latest_fileinfo_selection_quorum("version-id", &metas, &clean_errs, 2, 3), 2);
    }

    #[test]
    fn test_latest_fileinfo_selection_quorum_keeps_read_quorum_for_partial_overwrite_with_read_error() {
        let old_mod_time = OffsetDateTime::now_utc();
        let new_mod_time = old_mod_time + time::Duration::seconds(1);
        let old_data_dir = Uuid::new_v4();
        let new_data_dir = Uuid::new_v4();
        let metas = vec![
            quorum_test_fileinfo(old_mod_time, old_data_dir, "part-etag-old", 1),
            quorum_test_fileinfo(old_mod_time, old_data_dir, "part-etag-old", 2),
            quorum_test_fileinfo(new_mod_time, new_data_dir, "part-etag-new", 3),
            FileInfo::default(),
        ];
        let errs = vec![None, None, None, Some(DiskError::DiskNotFound)];

        let quorum = SetDisks::latest_fileinfo_selection_quorum("", &metas, &errs, 2, 3);
        let (online_disks, mod_time, etag) = SetDisks::list_online_disks(&vec![None; metas.len()], &metas, &errs, quorum);
        let fi = SetDisks::pick_valid_fileinfo(&metas, mod_time, etag, quorum)
            .expect("old metadata should remain readable with read quorum");

        assert_eq!(quorum, 2);
        assert_eq!(online_disks.len(), metas.len());
        assert_eq!(fi.data_dir, Some(old_data_dir));
        assert_eq!(fi.parts[0].etag, "part-etag-old");

        let (_, selected, selected_quorum) = SetDisks::select_valid_fileinfo(&vec![None; metas.len()], &metas, &errs, "", 2, 3)
            .expect("old metadata should remain selectable with read quorum");
        assert_eq!(selected_quorum, 2);
        assert_eq!(selected.data_dir, Some(old_data_dir));
        assert_eq!(selected.parts[0].etag, "part-etag-old");
        assert!(selected.is_latest);
    }

    #[test]
    fn test_latest_fileinfo_selection_rejects_partial_latest_read_quorum_with_read_error() {
        let old_mod_time = OffsetDateTime::now_utc();
        let new_mod_time = old_mod_time + time::Duration::seconds(1);
        let old_data_dir = Uuid::new_v4();
        let new_data_dir = Uuid::new_v4();
        let metas = vec![
            quorum_test_fileinfo(new_mod_time, new_data_dir, "part-etag-new", 1),
            quorum_test_fileinfo(new_mod_time, new_data_dir, "part-etag-new", 2),
            quorum_test_fileinfo(old_mod_time, old_data_dir, "part-etag-old", 3),
            FileInfo::default(),
        ];
        let errs = vec![None, None, None, Some(DiskError::DiskNotFound)];

        let result = SetDisks::select_valid_fileinfo(&vec![None; metas.len()], &metas, &errs, "", 2, 3);

        assert!(matches!(result, Err(DiskError::ErasureReadQuorum)));
    }

    #[test]
    fn test_latest_fileinfo_selection_preserves_degraded_read_quorum_without_competing_latest() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let metas = vec![
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-old", 1),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-old", 2),
            FileInfo::default(),
            FileInfo::default(),
        ];
        let errs = vec![None, None, Some(DiskError::DiskNotFound), Some(DiskError::DiskNotFound)];

        let (_, selected, selected_quorum) = SetDisks::select_valid_fileinfo(&vec![None; metas.len()], &metas, &errs, "", 2, 3)
            .expect("read quorum should remain enough when no competing latest is visible");

        assert_eq!(selected_quorum, 2);
        assert_eq!(selected.data_dir, Some(data_dir));
        assert_eq!(selected.parts[0].etag, "part-etag-old");
    }

    #[test]
    fn test_latest_fileinfo_selection_ignores_derived_version_stack_drift() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let mut latest_meta = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 1);
        latest_meta.is_latest = true;
        latest_meta.num_versions = 1;

        let mut stale_stack_meta = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 2);
        stale_stack_meta.is_latest = false;
        stale_stack_meta.successor_mod_time = Some(mod_time + time::Duration::seconds(1));
        stale_stack_meta.num_versions = 2;

        let mut newer_stack_meta = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 3);
        newer_stack_meta.is_latest = false;
        newer_stack_meta.successor_mod_time = Some(mod_time + time::Duration::seconds(2));
        newer_stack_meta.num_versions = 3;

        let metas = vec![latest_meta, stale_stack_meta, newer_stack_meta, FileInfo::default()];
        let errs = vec![None, None, None, Some(DiskError::DiskNotFound)];

        let (_, selected, selected_quorum) = SetDisks::select_valid_fileinfo(&vec![None; metas.len()], &metas, &errs, "", 2, 3)
            .expect("same object version should stay readable despite derived version stack drift");

        assert_eq!(selected_quorum, 3);
        assert_eq!(selected.data_dir, Some(data_dir));
        assert_eq!(selected.parts[0].etag, "part-etag");
        assert_eq!(selected.mod_time, Some(mod_time));
    }

    #[test]
    fn test_latest_fileinfo_selection_uses_successor_mod_time_quorum_for_latest_flag() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let mut stale_stack_meta = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 1);
        stale_stack_meta.is_latest = false;
        stale_stack_meta.successor_mod_time = Some(mod_time + time::Duration::seconds(1));
        stale_stack_meta.num_versions = 2;

        let mut latest_meta_a = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 2);
        latest_meta_a.is_latest = true;
        latest_meta_a.num_versions = 1;
        let mut latest_meta_b = latest_meta_a.clone();
        latest_meta_b.erasure.index = 3;

        let metas = vec![stale_stack_meta, latest_meta_a, latest_meta_b];

        let selected = SetDisks::find_file_info_in_quorum(&metas, &Some(mod_time), &None, 2)
            .expect("latest flag should be derived from successor mod time quorum");

        assert!(selected.is_latest);
        assert_eq!(selected.successor_mod_time, None);
        assert_eq!(selected.num_versions, 1);
        assert_eq!(selected.mod_time, Some(mod_time));
    }

    #[test]
    fn test_latest_fileinfo_selection_ignores_replication_state_drift() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let replication_status_key = format!(
            "{}{}",
            rustfs_utils::http::RUSTFS_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_REPLICATION_STATUS
        );
        let replication_timestamp_key = format!(
            "{}{}",
            rustfs_utils::http::RUSTFS_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP
        );
        let replication_reset_key = format!(
            "{}{}target-a",
            rustfs_utils::http::RUSTFS_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_REPLICATION_RESET_ARN_PREFIX
        );
        let meta_a = decoded_quorum_test_fileinfo_with_metadata(
            mod_time,
            data_dir,
            "part-etag",
            1,
            &[
                (&replication_status_key, "target-a=COMPLETED;"),
                (&replication_timestamp_key, "2024-01-01T00:00:00Z"),
                (&replication_reset_key, "COMPLETED"),
            ],
        );
        let meta_b = decoded_quorum_test_fileinfo_with_metadata(
            mod_time,
            data_dir,
            "part-etag",
            2,
            &[
                (&replication_status_key, "target-a=PENDING;"),
                (&replication_timestamp_key, "2024-01-01T00:00:01Z"),
                (&replication_reset_key, "PENDING"),
            ],
        );
        let meta_c = decoded_quorum_test_fileinfo_with_metadata(
            mod_time,
            data_dir,
            "part-etag",
            3,
            &[
                (&replication_status_key, "target-a=FAILED;"),
                (&replication_timestamp_key, "2024-01-01T00:00:02Z"),
                (&replication_reset_key, "FAILED"),
            ],
        );
        assert!(meta_a.replication_state_internal.is_some());
        assert_eq!(
            meta_a
                .metadata
                .get(rustfs_utils::http::AMZ_BUCKET_REPLICATION_STATUS)
                .map(String::as_str),
            Some("COMPLETED")
        );

        let metas = vec![meta_a, meta_b, meta_c, FileInfo::default()];
        let errs = vec![None, None, None, Some(DiskError::DiskNotFound)];

        let (_, selected, selected_quorum) = SetDisks::select_valid_fileinfo(&vec![None; metas.len()], &metas, &errs, "", 2, 3)
            .expect("replication status drift should not split readable object identity");

        assert_eq!(selected_quorum, 3);
        assert_eq!(selected.data_dir, Some(data_dir));
        assert_eq!(selected.parts[0].etag, "part-etag");
    }

    #[test]
    fn test_latest_fileinfo_selection_rejects_same_modtime_metadata_split_without_write_quorum() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let mut old_meta_a = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 1);
        let mut old_meta_b = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 2);
        let mut partial_meta = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 3);
        old_meta_a.metadata.insert("x-amz-meta-color".to_string(), "blue".to_string());
        old_meta_b.metadata.insert("x-amz-meta-color".to_string(), "blue".to_string());
        partial_meta
            .metadata
            .insert("x-amz-meta-color".to_string(), "red".to_string());
        let metas = vec![old_meta_a, old_meta_b, partial_meta, FileInfo::default()];
        let errs = vec![None, None, None, Some(DiskError::DiskNotFound)];

        let quorum = SetDisks::latest_fileinfo_selection_quorum("", &metas, &errs, 2, 3);
        let result = SetDisks::select_valid_fileinfo(&vec![None; metas.len()], &metas, &errs, "", 2, 3);

        assert_eq!(quorum, 2);
        assert!(matches!(result, Err(DiskError::ErasureReadQuorum)));
    }

    #[test]
    fn test_latest_fileinfo_selection_rejects_same_modtime_partial_metadata_read_quorum() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let mut old_meta = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 1);
        let mut partial_meta_a = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 2);
        let mut partial_meta_b = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 3);
        old_meta.metadata.insert("x-amz-meta-color".to_string(), "blue".to_string());
        partial_meta_a
            .metadata
            .insert("x-amz-meta-color".to_string(), "red".to_string());
        partial_meta_b
            .metadata
            .insert("x-amz-meta-color".to_string(), "red".to_string());
        let metas = vec![old_meta, partial_meta_a, partial_meta_b, FileInfo::default()];
        let errs = vec![None, None, None, Some(DiskError::DiskNotFound)];

        let result = SetDisks::select_valid_fileinfo(&vec![None; metas.len()], &metas, &errs, "", 2, 3);

        assert!(matches!(result, Err(DiskError::ErasureReadQuorum)));
    }

    #[test]
    fn test_latest_fileinfo_selection_rejects_same_modtime_transition_split_without_write_quorum() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let old_meta_a = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 1);
        let old_meta_b = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 2);
        let mut partial_meta = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 3);
        partial_meta.transition_status = TRANSITION_COMPLETE.to_string();
        partial_meta.transition_tier = "WARM".to_string();
        partial_meta.transitioned_objname = "remote/object".to_string();
        partial_meta.transition_version_id = Some(Uuid::new_v4());
        let metas = vec![old_meta_a, old_meta_b, partial_meta, FileInfo::default()];
        let errs = vec![None, None, None, Some(DiskError::DiskNotFound)];

        let quorum = SetDisks::latest_fileinfo_selection_quorum("", &metas, &errs, 2, 3);
        let result = SetDisks::select_valid_fileinfo(&vec![None; metas.len()], &metas, &errs, "", 2, 3);

        assert_eq!(quorum, 2);
        assert!(matches!(result, Err(DiskError::ErasureReadQuorum)));
    }

    #[test]
    fn test_latest_fileinfo_selection_quorum_uses_write_quorum_for_degraded_committed_identity() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let metas = vec![
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 1),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 2),
            quorum_test_fileinfo(mod_time, data_dir, "part-etag-a", 3),
            FileInfo::default(),
        ];
        let errs = vec![None, None, None, Some(DiskError::DiskNotFound)];

        assert_eq!(SetDisks::latest_fileinfo_selection_quorum("", &metas, &errs, 2, 3), 3);
    }

    #[test]
    fn test_list_object_parities() {
        // Test extracting parity counts from file info
        let file_info1 = FileInfo {
            erasure: ErasureInfo {
                data_blocks: 4,
                parity_blocks: 2,
                index: 1,                             // Must be > 0 for is_valid() to return true
                distribution: vec![1, 2, 3, 4, 5, 6], // Must match data_blocks + parity_blocks
                ..Default::default()
            },
            size: 100, // Non-zero size
            deleted: false,
            ..Default::default()
        };
        let file_info2 = FileInfo {
            erasure: ErasureInfo {
                data_blocks: 6,
                parity_blocks: 3,
                index: 1,                                      // Must be > 0 for is_valid() to return true
                distribution: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], // Must match data_blocks + parity_blocks
                ..Default::default()
            },
            size: 200, // Non-zero size
            deleted: false,
            ..Default::default()
        };
        let file_info3 = FileInfo {
            erasure: ErasureInfo {
                data_blocks: 2,
                parity_blocks: 1,
                index: 1,                    // Must be > 0 for is_valid() to return true
                distribution: vec![1, 2, 3], // Must match data_blocks + parity_blocks
                ..Default::default()
            },
            size: 0, // Zero size - function returns half of total shards
            deleted: false,
            ..Default::default()
        };

        let parts_metadata = vec![file_info1, file_info2, file_info3];
        let errs = vec![None, None, None];

        let parities = SetDisks::list_object_parities(&parts_metadata, &errs);
        assert_eq!(parities.len(), 3);
        assert_eq!(parities[0], 2); // parity_blocks from first file
        assert_eq!(parities[1], 3); // parity_blocks from second file
        assert_eq!(parities[2], 1); // half of total shards (3/2 = 1) for zero size file
    }

    #[test]
    fn test_conv_part_err_to_int() {
        // Test error conversion to integer codes
        assert_eq!(conv_part_err_to_int(&None), CHECK_PART_SUCCESS);

        let disk_err = DiskError::FileNotFound;
        assert_eq!(conv_part_err_to_int(&Some(disk_err)), CHECK_PART_FILE_NOT_FOUND);

        let other_err = DiskError::other("other error");
        assert_eq!(conv_part_err_to_int(&Some(other_err)), CHECK_PART_UNKNOWN); // Other errors should return UNKNOWN, not SUCCESS
    }

    #[test]
    fn test_has_part_err() {
        // Test checking for part errors
        let no_errors = vec![CHECK_PART_SUCCESS, CHECK_PART_SUCCESS];
        assert!(!has_part_err(&no_errors));

        let with_errors = vec![CHECK_PART_SUCCESS, CHECK_PART_FILE_NOT_FOUND];
        assert!(has_part_err(&with_errors));

        let unknown_errors = vec![CHECK_PART_UNKNOWN, CHECK_PART_SUCCESS];
        assert!(has_part_err(&unknown_errors));
    }

    #[test]
    fn test_populate_data_errs_by_disk_uses_disk_index_not_error_code() {
        let mut data_errs_by_disk = HashMap::from([
            (0, vec![CHECK_PART_UNKNOWN, CHECK_PART_UNKNOWN]),
            (1, vec![CHECK_PART_UNKNOWN, CHECK_PART_UNKNOWN]),
            (2, vec![CHECK_PART_UNKNOWN, CHECK_PART_UNKNOWN]),
        ]);
        let data_errs_by_part = HashMap::from([
            (0, vec![CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS, CHECK_PART_SUCCESS]),
            (1, vec![CHECK_PART_SUCCESS, CHECK_PART_FILE_CORRUPT, CHECK_PART_SUCCESS]),
        ]);

        populate_data_errs_by_disk(&mut data_errs_by_disk, &data_errs_by_part);

        assert_eq!(data_errs_by_disk.get(&0).unwrap(), &vec![CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS]);
        assert_eq!(data_errs_by_disk.get(&1).unwrap(), &vec![CHECK_PART_SUCCESS, CHECK_PART_FILE_CORRUPT]);
        assert_eq!(data_errs_by_disk.get(&2).unwrap(), &vec![CHECK_PART_SUCCESS, CHECK_PART_SUCCESS]);

        let mut data_errs_by_disk = HashMap::from([
            (0, vec![CHECK_PART_UNKNOWN, CHECK_PART_UNKNOWN]),
            (1, vec![CHECK_PART_UNKNOWN, CHECK_PART_UNKNOWN]),
            (2, vec![CHECK_PART_UNKNOWN, CHECK_PART_UNKNOWN]),
            (3, vec![CHECK_PART_UNKNOWN, CHECK_PART_UNKNOWN]),
        ]);
        let data_errs_by_part = HashMap::from([
            (
                0,
                vec![
                    CHECK_PART_FILE_NOT_FOUND,
                    CHECK_PART_SUCCESS,
                    CHECK_PART_SUCCESS,
                    CHECK_PART_SUCCESS,
                ],
            ),
            (
                1,
                vec![
                    CHECK_PART_FILE_CORRUPT,
                    CHECK_PART_SUCCESS,
                    CHECK_PART_SUCCESS,
                    CHECK_PART_SUCCESS,
                ],
            ),
        ]);

        populate_data_errs_by_disk(&mut data_errs_by_disk, &data_errs_by_part);

        assert_eq!(
            data_errs_by_disk.get(&0).unwrap(),
            &vec![CHECK_PART_FILE_NOT_FOUND, CHECK_PART_FILE_CORRUPT]
        );
        assert_eq!(data_errs_by_disk.get(&1).unwrap(), &vec![CHECK_PART_SUCCESS, CHECK_PART_SUCCESS]);
        assert_eq!(data_errs_by_disk.get(&2).unwrap(), &vec![CHECK_PART_SUCCESS, CHECK_PART_SUCCESS]);
        assert_eq!(data_errs_by_disk.get(&3).unwrap(), &vec![CHECK_PART_SUCCESS, CHECK_PART_SUCCESS]);
    }

    #[test]
    fn test_should_heal_object_on_disk() {
        // Test healing decision logic
        let meta = FileInfo::default();
        let latest_meta = FileInfo::default();

        // Test with file not found error
        let err = Some(DiskError::FileNotFound);
        let (should_heal, _, _) = should_heal_object_on_disk(&err, &[], &meta, &latest_meta);
        assert!(should_heal);

        // Test with no error and no part errors
        let (should_heal, _, _) = should_heal_object_on_disk(&None, &[CHECK_PART_SUCCESS], &meta, &latest_meta);
        assert!(!should_heal);

        // Test with part corruption
        let (should_heal, _, _) = should_heal_object_on_disk(&None, &[CHECK_PART_FILE_CORRUPT], &meta, &latest_meta);
        assert!(should_heal);
    }

    #[tokio::test]
    async fn test_get_disks_info_preserves_runtime_state_for_suspect_and_offline_disks() {
        let format = FormatV3::new(1, 3);
        let mut temp_dirs = Vec::new();
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();

        for disk_idx in 0..3 {
            let (dir, endpoint, disk) = make_formatted_local_disk_for_info_test(disk_idx, &format).await;
            temp_dirs.push(dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        disks[1]
            .as_ref()
            .expect("disk 1 should exist")
            .force_runtime_state_for_test(RuntimeDriveHealthState::Suspect);
        disks[2]
            .as_ref()
            .expect("disk 2 should exist")
            .force_runtime_state_for_test(RuntimeDriveHealthState::Offline);

        let info = get_disks_info(&disks, &endpoints).await;
        assert_eq!(info.len(), 3);

        assert_eq!(info[0].state, "ok");
        assert_eq!(info[0].runtime_state.as_deref(), Some("online"));
        assert!(!info[0].drive_path.is_empty(), "online disk should keep immediate disk_info probe");

        assert_eq!(info[1].state, "ok");
        assert_eq!(info[1].runtime_state.as_deref(), Some("suspect"));
        assert!(!info[1].drive_path.is_empty(), "suspect disk should still probe for fresher disk info");

        assert_eq!(info[2].state, "offline");
        assert_eq!(info[2].runtime_state.as_deref(), Some("offline"));
        assert!(info[2].drive_path.is_empty(), "offline disk should use runtime snapshot fallback");
    }

    #[tokio::test]
    async fn test_get_disks_info_uses_capacity_snapshot_for_offline_disk() {
        let format = FormatV3::new(1, 1);
        let (temp_dir, endpoint, disk) = make_formatted_local_disk_for_info_test(0, &format).await;
        disk.record_capacity_probe(100, 40, 60);
        disk.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);

        let info = get_disks_info(&[Some(disk)], &[endpoint]).await;
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].state, "offline");
        assert_eq!(info[0].runtime_state.as_deref(), Some("offline"));
        assert_eq!(info[0].capacity_observation_source.as_deref(), Some("snapshot"));
        assert!(info[0].capacity_observation_age_seconds.unwrap_or(u64::MAX) <= 60);
        assert_eq!(info[0].total_space, 100);
        assert_eq!(info[0].used_space, 40);
        assert_eq!(info[0].available_space, 60);
        assert_eq!(info[0].utilization, 40.0);

        drop(temp_dir);
    }

    #[tokio::test]
    async fn list_path_returns_read_quorum_when_runtime_candidates_are_empty() {
        let disk_count = 4;
        let format = FormatV3::new(1, disk_count);
        let mut temp_dirs = Vec::with_capacity(disk_count);
        let mut endpoints = Vec::with_capacity(disk_count);
        let mut disks = Vec::with_capacity(disk_count);

        for disk_idx in 0..disk_count {
            let (dir, endpoint, disk) = make_formatted_local_disk_for_info_test(disk_idx, &format).await;
            temp_dirs.push(dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        let set_disks = SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            disk_count,
            disk_count / 2,
            0,
            0,
            endpoints,
            format,
            Vec::new(),
        )
        .await;

        for disk in set_disks.get_disks_internal().await.iter().flatten() {
            disk.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);
        }

        let (tx, _rx) = mpsc::channel(1);
        let err = set_disks
            .list_path(
                CancellationToken::new(),
                ListPathOptions {
                    bucket: "bucket".to_string(),
                    recursive: true,
                    ..Default::default()
                },
                tx,
            )
            .await
            .expect_err("empty runtime candidate set should fail before list_path_raw");

        assert_eq!(err, StorageError::ErasureReadQuorum);

        drop(temp_dirs);
    }

    #[tokio::test]
    async fn load_file_info_versions_exact_returns_none_for_explicit_not_found() {
        let format = FormatV3::new(1, 1);
        let (temp_dir, endpoint, disk) = make_formatted_local_disk_for_info_test(0, &format).await;
        let bucket = "bucket";

        disk.make_volume(bucket).await.expect("bucket should be created");

        let set_disks = SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(vec![Some(disk)])),
            1,
            0,
            0,
            0,
            vec![endpoint],
            format,
            Vec::new(),
        )
        .await;

        let versions = set_disks
            .load_file_info_versions_exact(bucket, "missing-object")
            .await
            .expect("explicit object not found should be accepted");

        assert!(versions.is_none());
        drop(temp_dir);
    }

    #[tokio::test]
    async fn load_file_info_versions_exact_rejects_corrupt_metadata() {
        let format = FormatV3::new(1, 1);
        let (temp_dir, endpoint, disk) = make_formatted_local_disk_for_info_test(0, &format).await;
        let bucket = "bucket";
        let object = "object.txt";

        disk.make_volume(bucket).await.expect("bucket should be created");
        let metadata_path = format!("{object}/{STORAGE_FORMAT_FILE}");
        disk.write_all(bucket, &metadata_path, Bytes::from_static(b"not-xl-meta"))
            .await
            .expect("corrupt metadata file should be written");

        let set_disks = SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(vec![Some(disk)])),
            1,
            0,
            0,
            0,
            vec![endpoint],
            format,
            Vec::new(),
        )
        .await;

        let err = set_disks
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect_err("corrupt exact metadata must fail closed");

        assert!(!is_err_object_not_found(&err), "corrupt metadata must not be treated as not found: {err}");
        drop(temp_dir);
    }

    #[tokio::test]
    async fn list_path_still_uses_disk_after_prior_walk_timeout() {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::AsyncWrite;

        struct PendingWriter;

        impl AsyncWrite for PendingWriter {
            fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<std::io::Result<usize>> {
                Poll::Pending
            }

            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        let format = FormatV3::new(1, 1);
        let (temp_dir, endpoint, disk) = make_formatted_local_disk_for_info_test(0, &format).await;
        let bucket = "bucket";
        let object = "obj";

        disk.make_volume(bucket).await.expect("bucket should be created");
        let metadata_path = format!("{object}/{STORAGE_FORMAT_FILE}");
        disk.write_all(bucket, &metadata_path, Bytes::from_static(b"not-an-xl-meta"))
            .await
            .expect("metadata file should be created");

        let set_disks = SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(vec![Some(disk.clone())])),
            1,
            0,
            0,
            0,
            vec![endpoint],
            format,
            Vec::new(),
        )
        .await;

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS, Some("1")),
                (rustfs_config::ENV_DRIVE_WALKDIR_STALL_TIMEOUT_SECS, Some("1")),
            ],
            async {
                let mut writer = PendingWriter;
                let walk_err = disk
                    .walk_dir(
                        WalkDirOptions {
                            bucket: bucket.to_string(),
                            recursive: true,
                            ..Default::default()
                        },
                        &mut writer,
                    )
                    .await
                    .expect_err("walk_dir should time out");
                assert_eq!(walk_err, DiskError::Timeout);
                assert_eq!(disk.runtime_state(), RuntimeDriveHealthState::Online);

                let (tx, mut rx) = mpsc::channel::<MetaCacheEntry>(4);
                set_disks
                    .list_path(
                        CancellationToken::new(),
                        ListPathOptions {
                            bucket: bucket.to_string(),
                            recursive: true,
                            ..Default::default()
                        },
                        tx,
                    )
                    .await
                    .expect("list_path should still succeed after prior walk timeout");

                let entry = rx.recv().await.expect("listing should yield the object entry");
                assert_eq!(entry.name, object);
                assert_eq!(disk.runtime_state(), RuntimeDriveHealthState::Online);
            },
        )
        .await;

        drop(temp_dir);
    }

    #[tokio::test]
    async fn list_path_system_prefix_survives_prior_walk_timeout() {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::AsyncWrite;

        struct PendingWriter;

        impl AsyncWrite for PendingWriter {
            fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<std::io::Result<usize>> {
                Poll::Pending
            }

            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        let format = FormatV3::new(1, 1);
        let (temp_dir, endpoint, disk) = make_formatted_local_disk_for_info_test(0, &format).await;
        let object = "config/iam/sts/test/identity.json";

        let metadata_path = format!("{object}/{STORAGE_FORMAT_FILE}");
        disk.write_all(RUSTFS_META_BUCKET, &metadata_path, Bytes::from_static(b"not-an-xl-meta"))
            .await
            .expect("system path metadata file should be created");

        let set_disks = SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(vec![Some(disk.clone())])),
            1,
            0,
            0,
            0,
            vec![endpoint],
            format,
            Vec::new(),
        )
        .await;

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_DRIVE_WALKDIR_TIMEOUT_SECS, Some("1")),
                (rustfs_config::ENV_DRIVE_WALKDIR_STALL_TIMEOUT_SECS, Some("1")),
            ],
            async {
                let mut writer = PendingWriter;
                let walk_err = disk
                    .walk_dir(
                        WalkDirOptions {
                            bucket: RUSTFS_META_BUCKET.to_string(),
                            base_dir: "config/iam/".to_string(),
                            recursive: true,
                            ..Default::default()
                        },
                        &mut writer,
                    )
                    .await
                    .expect_err("walk_dir should time out");
                assert_eq!(walk_err, DiskError::Timeout);
                assert_eq!(disk.runtime_state(), RuntimeDriveHealthState::Online);

                let (tx, mut rx) = mpsc::channel::<MetaCacheEntry>(4);
                set_disks
                    .list_path(
                        CancellationToken::new(),
                        ListPathOptions {
                            bucket: RUSTFS_META_BUCKET.to_string(),
                            base_dir: "config/iam/".to_string(),
                            recursive: true,
                            ..Default::default()
                        },
                        tx,
                    )
                    .await
                    .expect("system prefix list_path should still succeed after prior walk timeout");

                let entry = rx.recv().await.expect("listing should yield the system-path entry");
                assert_eq!(entry.name, "config/iam/sts/");
                assert!(
                    entry.is_dir(),
                    "system prefix listing should still yield a directory entry after timeout recovery"
                );
                assert_eq!(disk.runtime_state(), RuntimeDriveHealthState::Online);
            },
        )
        .await;

        drop(temp_dir);
    }

    #[test]
    fn test_dangling_meta_errs_count() {
        // Test counting dangling metadata errors
        let errs = vec![None, Some(DiskError::FileNotFound), None];
        let (not_found_count, non_actionable_count) = dangling_meta_errs_count(&errs);
        assert_eq!(not_found_count, 1); // One FileNotFound error
        assert_eq!(non_actionable_count, 0); // No other errors
    }

    #[test]
    fn test_dangling_part_errs_count() {
        // Test counting dangling part errors
        let results = vec![CHECK_PART_SUCCESS, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS];
        let (not_found_count, non_actionable_count) = dangling_part_errs_count(&results);
        assert_eq!(not_found_count, 1); // One FILE_NOT_FOUND error
        assert_eq!(non_actionable_count, 0); // No other errors
    }

    #[test]
    fn test_is_object_dir_dangling() {
        // Test object directory dangling detection
        let errs = vec![Some(DiskError::FileNotFound), Some(DiskError::FileNotFound), None];
        assert!(is_object_dir_dangling(&errs));
        let errs2 = vec![None, None, None];
        assert!(!is_object_dir_dangling(&errs2));

        let errs3 = vec![Some(DiskError::FileCorrupt), Some(DiskError::FileNotFound)];
        assert!(!is_object_dir_dangling(&errs3)); // Mixed errors, not all not found
    }

    #[test]
    fn test_join_errs() {
        // Test joining error messages
        let errs = vec![None, Some(DiskError::other("error1")), Some(DiskError::other("error2"))];
        let joined = join_errs(&errs);
        assert!(joined.contains("<nil>"));
        assert!(joined.contains("io error")); // DiskError::other is rendered as "io error"

        // Test with different error types
        let errs2 = vec![None, Some(DiskError::FileNotFound), Some(DiskError::FileCorrupt)];
        let joined2 = join_errs(&errs2);
        assert!(joined2.contains("<nil>"));
        assert!(joined2.contains("file not found"));
        assert!(joined2.contains("file is corrupted"));
    }

    #[test]
    fn test_reduce_common_data_dir() {
        // Test reducing common data directory
        use uuid::Uuid;

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        let data_dirs = vec![Some(uuid1), Some(uuid1), Some(uuid2)];
        let result = SetDisks::reduce_common_data_dir(&data_dirs, 2);
        assert_eq!(result, Some(uuid1)); // uuid1 appears twice, meets quorum

        let data_dirs = vec![Some(uuid1), Some(uuid2), None];
        let result = SetDisks::reduce_common_data_dir(&data_dirs, 2);
        assert_eq!(result, None); // No UUID meets quorum of 2

        let data_dirs = vec![Some(uuid1), Some(uuid1), None, None];
        let result = SetDisks::reduce_common_data_dir(&data_dirs, 2);
        assert_eq!(result, Some(uuid1)); // Ignore None votes; uuid1 should still meet quorum
    }

    #[test]
    fn test_object_quorum_from_meta_returns_not_found_when_all_metadata_is_missing() {
        let errs = vec![
            Some(DiskError::FileNotFound),
            Some(DiskError::VolumeNotFound),
            Some(DiskError::DiskNotFound),
            Some(DiskError::FileNotFound),
        ];

        let err = SetDisks::object_quorum_from_meta(&vec![FileInfo::default(); errs.len()], &errs, 2)
            .expect_err("missing metadata should map to FileNotFound");

        assert_eq!(err, DiskError::FileNotFound);
    }

    #[test]
    fn test_object_quorum_from_meta_preserves_read_quorum_for_mixed_failures() {
        let errs = vec![
            Some(DiskError::FileNotFound),
            Some(DiskError::VolumeNotFound),
            Some(DiskError::FileCorrupt),
            Some(DiskError::DiskNotFound),
        ];

        let err = SetDisks::object_quorum_from_meta(&vec![FileInfo::default(); errs.len()], &errs, 2)
            .expect_err("mixed metadata failures should keep quorum semantics");

        assert_eq!(err, DiskError::ErasureReadQuorum);
    }

    #[test]
    fn test_shuffle_parts_metadata() {
        // Test metadata shuffling
        let metadata = vec![
            FileInfo {
                name: "file1".to_string(),
                ..Default::default()
            },
            FileInfo {
                name: "file2".to_string(),
                ..Default::default()
            },
            FileInfo {
                name: "file3".to_string(),
                ..Default::default()
            },
        ];

        // Distribution uses 1-based indexing
        let distribution = vec![3, 1, 2]; // 1-based shuffle order
        let result = SetDisks::shuffle_parts_metadata(&metadata, &distribution);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].name, "file2"); // distribution[1] = 1, so metadata[1] goes to index 0
        assert_eq!(result[1].name, "file3"); // distribution[2] = 2, so metadata[2] goes to index 1
        assert_eq!(result[2].name, "file1"); // distribution[0] = 3, so metadata[0] goes to index 2

        // Test with empty distribution
        let empty_distribution = vec![];
        let result2 = SetDisks::shuffle_parts_metadata(&metadata, &empty_distribution);
        assert_eq!(result2.len(), 3);
        assert_eq!(result2[0].name, "file1"); // Should return original order
    }

    #[test]
    fn test_shuffle_disks() {
        // Test disk shuffling
        let disks = vec![None, None, None]; // Mock disks
        let distribution = vec![3, 1, 2]; // 1-based indexing

        let result = SetDisks::shuffle_disks(&disks, &distribution);
        assert_eq!(result.len(), 3);
        // All disks are None, so result should be all None
        assert!(result.iter().all(|d| d.is_none()));

        // Test with empty distribution
        let empty_distribution = vec![];
        let result2 = SetDisks::shuffle_disks(&disks, &empty_distribution);
        assert_eq!(result2.len(), 3);
        assert!(result2.iter().all(|d| d.is_none()));
    }

    #[test]
    fn test_etag_matches() {
        assert!(e_tag_matches("abc", "abc"));
        assert!(e_tag_matches("\"abc\"", "abc"));
        assert!(e_tag_matches("\"abc\"", "*"));
    }

    #[test]
    fn test_build_tiered_decommission_file_info_preserves_transition_metadata() {
        let version_id = Uuid::new_v4();
        let transition_version_id = Uuid::new_v4();
        let original = FileInfo {
            version_id: Some(version_id),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_tier: "WARM-TIER".to_string(),
            transition_version_id: Some(transition_version_id),
            erasure: FileInfo::new("old-bucket/old-object", 8, 8).erasure,
            ..Default::default()
        };

        let (updated, write_quorum) = build_tiered_decommission_file_info("bucket", "object", &original, 16, 4, None);

        assert_eq!(updated.version_id, original.version_id);
        assert_eq!(updated.transition_status, original.transition_status);
        assert_eq!(updated.transitioned_objname, original.transitioned_objname);
        assert_eq!(updated.transition_tier, original.transition_tier);
        assert_eq!(updated.transition_version_id, original.transition_version_id);
        assert_eq!(updated.erasure.data_blocks, 12);
        assert_eq!(updated.erasure.parity_blocks, 4);
        assert_eq!(write_quorum, 12);
        assert_ne!(updated.erasure.distribution, original.erasure.distribution);
    }

    #[test]
    fn test_resolve_tiered_decommission_write_quorum_result_allows_successful_quorum() {
        let errs = vec![None, None, Some(DiskError::DiskNotFound), None];

        let result = resolve_tiered_decommission_write_quorum_result(&errs, 3, "bucket", "object");

        assert!(result.is_ok());
    }

    #[test]
    fn test_resolve_tiered_decommission_write_quorum_result_wraps_object_context() {
        let errs = vec![
            Some(DiskError::DiskNotFound),
            Some(DiskError::DiskNotFound),
            Some(DiskError::DiskNotFound),
            Some(DiskError::DiskNotFound),
        ];

        let err = resolve_tiered_decommission_write_quorum_result(&errs, 3, "bucket", "object").expect_err("expected error");
        let rendered = err.to_string();

        assert!(rendered.contains("bucket"), "{rendered}");
        assert!(rendered.contains("object"), "{rendered}");
    }

    #[test]
    fn test_check_object_lock_retention_update_blocks_compliance_shorten() {
        let now = OffsetDateTime::now_utc();
        let existing_until = now + Duration::from_secs(60 * 60 * 24 * 60);
        let requested_until = now + Duration::from_secs(60 * 60 * 24);

        let mut user_defined = HashMap::new();
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
            s3s::dto::ObjectLockRetentionMode::COMPLIANCE.to_string(),
        );
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
            existing_until.format(&time::format_description::well_known::Rfc3339).unwrap(),
        );

        let obj_info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };
        let opts = ObjectOptions {
            object_lock_retention: Some(crate::storage_api_contracts::object::ObjectLockRetentionOptions {
                mode: Some(s3s::dto::ObjectLockRetentionMode::COMPLIANCE.to_string()),
                retain_until: Some(requested_until),
                bypass_governance: true,
            }),
            ..Default::default()
        };

        let err = check_object_lock_retention_update("bucket", "object", &obj_info, &opts)
            .expect_err("COMPLIANCE shortening must be blocked");

        assert!(matches!(err, StorageError::PrefixAccessDenied(_, _)));
    }

    #[test]
    fn test_check_object_lock_retention_update_allows_governance_shorten_with_bypass() {
        let now = OffsetDateTime::now_utc();
        let existing_until = now + Duration::from_secs(60 * 60 * 24 * 60);
        let requested_until = now + Duration::from_secs(60 * 60 * 24);

        let mut user_defined = HashMap::new();
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
            s3s::dto::ObjectLockRetentionMode::GOVERNANCE.to_string(),
        );
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
            existing_until.format(&time::format_description::well_known::Rfc3339).unwrap(),
        );

        let obj_info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };
        let opts = ObjectOptions {
            object_lock_retention: Some(crate::storage_api_contracts::object::ObjectLockRetentionOptions {
                mode: Some(s3s::dto::ObjectLockRetentionMode::GOVERNANCE.to_string()),
                retain_until: Some(requested_until),
                bypass_governance: true,
            }),
            ..Default::default()
        };

        check_object_lock_retention_update("bucket", "object", &obj_info, &opts)
            .expect("GOVERNANCE shortening with bypass should remain allowed");
    }

    #[tokio::test]
    async fn test_check_object_lock_delete_blocks_compliance_version_delete() {
        let retain_until = OffsetDateTime::now_utc() + Duration::from_secs(60 * 60 * 24 * 60);
        let mut user_defined = HashMap::new();
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
            s3s::dto::ObjectLockRetentionMode::COMPLIANCE.to_string(),
        );
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
            retain_until.format(&time::format_description::well_known::Rfc3339).unwrap(),
        );

        let obj_info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            versioned: true,
            ..Default::default()
        };

        let err = check_object_lock_delete("bucket", "object", &obj_info, &opts)
            .await
            .expect_err("COMPLIANCE retention must block explicit version deletion");

        assert!(matches!(err, StorageError::PrefixAccessDenied(_, _)));
    }

    #[tokio::test]
    async fn test_check_object_lock_delete_allows_versioned_delete_marker_creation() {
        let retain_until = OffsetDateTime::now_utc() + Duration::from_secs(60 * 60 * 24 * 60);
        let mut user_defined = HashMap::new();
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
            s3s::dto::ObjectLockRetentionMode::COMPLIANCE.to_string(),
        );
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
            retain_until.format(&time::format_description::well_known::Rfc3339).unwrap(),
        );

        let obj_info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: None,
            versioned: true,
            version_suspended: false,
            ..Default::default()
        };

        check_object_lock_delete("bucket", "object", &obj_info, &opts)
            .await
            .expect("versioned delete marker creation should not delete the locked version");
    }

    #[test]
    fn test_should_persist_encryption_original_size_rejects_plain_metadata() {
        let metadata = HashMap::from([("content-type".to_string(), "application/octet-stream".to_string())]);

        assert!(!should_persist_encryption_original_size(&metadata));
    }

    #[test]
    fn test_should_persist_encryption_original_size_accepts_sse_c_metadata() {
        let metadata = HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]);

        assert!(should_persist_encryption_original_size(&metadata));
    }

    #[test]
    fn test_should_prevent_write() {
        let oi = ObjectInfo {
            etag: Some("abc".to_string()),
            ..Default::default()
        };
        let if_none_match = Some("abc".to_string());
        let if_match = None;
        assert!(should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = Some("*".to_string());
        let if_match = None;
        assert!(should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = None;
        let if_match = Some("def".to_string());
        assert!(should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = None;
        let if_match = Some("*".to_string());
        assert!(!should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = Some("def".to_string());
        let if_match = None;
        assert!(!should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = Some("def".to_string());
        let if_match = Some("*".to_string());
        assert!(!should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = Some("def".to_string());
        let if_match = Some("\"abc\"".to_string());
        assert!(!should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = Some("*".to_string());
        let if_match = Some("\"abc\"".to_string());
        assert!(should_prevent_write(&oi, if_none_match, if_match));

        let oi = ObjectInfo {
            etag: None,
            ..Default::default()
        };
        let if_none_match = Some("*".to_string());
        let if_match = Some("\"abc\"".to_string());
        assert!(should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = None;
        let if_match = None;
        assert!(!should_prevent_write(&oi, if_none_match, if_match));

        let if_none_match = Some(String::new());
        let if_match = Some(" ".to_string());
        assert!(!should_prevent_write(&oi, if_none_match, if_match));
    }

    #[test]
    fn test_is_valid_storage_class() {
        // Test valid storage classes
        assert!(is_valid_storage_class(storageclass::STANDARD));
        assert!(is_valid_storage_class(storageclass::RRS));
        assert!(is_valid_storage_class(storageclass::DEEP_ARCHIVE));
        assert!(is_valid_storage_class(storageclass::EXPRESS_ONEZONE));
        assert!(is_valid_storage_class(storageclass::GLACIER));
        assert!(is_valid_storage_class(storageclass::GLACIER_IR));
        assert!(is_valid_storage_class(storageclass::INTELLIGENT_TIERING));
        assert!(is_valid_storage_class(storageclass::ONEZONE_IA));
        assert!(is_valid_storage_class(storageclass::OUTPOSTS));
        assert!(is_valid_storage_class(storageclass::SNOW));
        assert!(is_valid_storage_class(storageclass::STANDARD_IA));

        // Test invalid storage classes
        assert!(!is_valid_storage_class("INVALID"));
        assert!(!is_valid_storage_class(""));
        assert!(!is_valid_storage_class("standard")); // lowercase
    }

    #[test]
    fn complete_part_checksum_accepts_missing_value_and_uses_base_type() {
        let missing_checksum_part = CompletePart::default();
        assert_eq!(
            complete_part_checksum(&missing_checksum_part, rustfs_rio::ChecksumType::CRC64_NVME),
            Some(None)
        );

        let full_object_crc32 =
            rustfs_rio::ChecksumType(rustfs_rio::ChecksumType::CRC32.0 | rustfs_rio::ChecksumType::FULL_OBJECT.0);
        let part = CompletePart {
            checksum_crc32: Some("AAAAAA==".to_string()),
            ..Default::default()
        };
        assert_eq!(complete_part_checksum(&part, full_object_crc32), Some(Some("AAAAAA==".to_string())));
    }

    fn direct_memory_test_metadata(size: i64) -> (ObjectInfo, FileInfo, ObjectOptions) {
        let part_size = usize::try_from(size).expect("test size should fit usize");
        let part = ObjectPartInfo {
            number: 1,
            size: part_size,
            actual_size: size,
            ..Default::default()
        };
        let object_info = ObjectInfo {
            size,
            actual_size: size,
            parts: Arc::new(vec![part]),
            etag: Some("0123456789abcdef0123456789abcdef".to_string()),
            ..Default::default()
        };
        let mut fi = FileInfo::new("bucket/object", 1, 0);
        fi.size = size;
        fi.add_object_part(1, String::new(), part_size, None, size, None, None);
        (object_info, fi, ObjectOptions::default())
    }

    #[test]
    fn small_object_direct_memory_eligibility_is_conservative() {
        let (object_info, fi, opts) = direct_memory_test_metadata(1024);
        assert!(is_get_small_object_direct_memory_eligible_with_threshold(
            &None,
            &object_info,
            &fi,
            &opts,
            128 * 1024
        ));

        assert!(!is_get_small_object_direct_memory_eligible_with_threshold(
            &Some(HTTPRangeSpec {
                start: 0,
                end: 10,
                is_suffix_length: false,
            }),
            &object_info,
            &fi,
            &opts,
            128 * 1024
        ));

        let mut part_opts = opts.clone();
        part_opts.part_number = Some(1);
        assert!(!is_get_small_object_direct_memory_eligible_with_threshold(
            &None,
            &object_info,
            &fi,
            &part_opts,
            128 * 1024
        ));

        let mut versioned_opts = opts.clone();
        versioned_opts.versioned = true;
        assert!(!is_get_small_object_direct_memory_eligible_with_threshold(
            &None,
            &object_info,
            &fi,
            &versioned_opts,
            128 * 1024
        ));

        let mut remote = object_info;
        remote.transitioned_object.status = TRANSITION_COMPLETE.to_string();
        remote.transitioned_object.tier = "remote-tier".to_string();
        assert!(!is_get_small_object_direct_memory_eligible_with_threshold(
            &None,
            &remote,
            &fi,
            &opts,
            128 * 1024
        ));
    }

    #[test]
    fn inline_fast_path_rejects_part_number_requests() {
        let (mut object_info, mut fi, opts) = direct_memory_test_metadata(1024);
        object_info.inlined = true;
        fi.data = Some(Bytes::from(vec![0; 1024]));

        assert!(should_use_inline_fast_path(&None, &object_info, &fi, &opts));

        let mut part_opts = opts;
        part_opts.part_number = Some(1);
        assert!(!should_use_inline_fast_path(&None, &object_info, &fi, &part_opts));
    }

    #[test]
    fn small_object_direct_memory_decision_reports_bounded_reasons() {
        let (object_info, fi, opts) = direct_memory_test_metadata(1024);

        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(&None, &object_info, &fi, &opts, false, 128 * 1024),
            GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::Disabled)
        );
        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(&None, &object_info, &fi, &opts, true, 0),
            GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::ThresholdZero)
        );
        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(
                &Some(HTTPRangeSpec {
                    start: 0,
                    end: 10,
                    is_suffix_length: false,
                }),
                &object_info,
                &fi,
                &opts,
                true,
                128 * 1024
            ),
            GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::Range)
        );

        let mut versioned_opts = opts.clone();
        versioned_opts.versioned = true;
        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(&None, &object_info, &fi, &versioned_opts, true, 128 * 1024),
            GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::Versioned)
        );

        let mut encrypted = object_info.clone();
        Arc::make_mut(&mut encrypted.user_defined).insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());
        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(&None, &encrypted, &fi, &opts, true, 128 * 1024),
            GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::Encrypted)
        );

        let mut remote = object_info.clone();
        remote.transitioned_object.status = TRANSITION_COMPLETE.to_string();
        remote.transitioned_object.tier = "remote-tier".to_string();
        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(&None, &remote, &fi, &opts, true, 128 * 1024),
            GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::Remote)
        );

        let mut multipart = object_info.clone();
        multipart.parts = Arc::new(vec![ObjectPartInfo::default(), ObjectPartInfo::default()]);
        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(&None, &multipart, &fi, &opts, true, 128 * 1024),
            GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::ObjectInfoMultipart)
        );
        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(&None, &object_info, &fi, &opts, true, 512),
            GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::AboveThreshold)
        );

        let mut size_mismatch = object_info.clone();
        size_mismatch.size += 1;
        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(&None, &size_mismatch, &fi, &opts, true, 128 * 1024),
            GetDirectMemoryDecision::Fallback(GetDirectMemoryFallbackReason::SizeMismatch)
        );
        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(&None, &object_info, &fi, &opts, true, 128 * 1024),
            GetDirectMemoryDecision::Use { object_size: 1024 }
        );
    }

    #[test]
    fn direct_memory_fallback_metric_labels_are_stable() {
        assert_eq!(GetDirectMemoryFallbackReason::Disabled.as_str(), "disabled");
        assert_eq!(GetDirectMemoryFallbackReason::ThresholdZero.as_str(), "threshold_zero");
        assert_eq!(GetDirectMemoryFallbackReason::Range.as_str(), "range");
        assert_eq!(GetDirectMemoryFallbackReason::PartNumber.as_str(), "part_number");
        assert_eq!(GetDirectMemoryFallbackReason::VersionId.as_str(), "version_id");
        assert_eq!(GetDirectMemoryFallbackReason::Versioned.as_str(), "versioned");
        assert_eq!(GetDirectMemoryFallbackReason::VersionSuspended.as_str(), "version_suspended");
        assert_eq!(GetDirectMemoryFallbackReason::InclFreeVersions.as_str(), "incl_free_versions");
        assert_eq!(GetDirectMemoryFallbackReason::SkipFreeVersion.as_str(), "skip_free_version");
        assert_eq!(GetDirectMemoryFallbackReason::DataMovement.as_str(), "data_movement");
        assert_eq!(GetDirectMemoryFallbackReason::RawDataMovementRead.as_str(), "raw_data_movement_read");
        assert_eq!(GetDirectMemoryFallbackReason::DeleteMarker.as_str(), "delete_marker");
        assert_eq!(GetDirectMemoryFallbackReason::MetadataOnly.as_str(), "metadata_only");
        assert_eq!(GetDirectMemoryFallbackReason::VersionOnly.as_str(), "version_only");
        assert_eq!(GetDirectMemoryFallbackReason::Encrypted.as_str(), "encrypted");
        assert_eq!(GetDirectMemoryFallbackReason::Compressed.as_str(), "compressed");
        assert_eq!(GetDirectMemoryFallbackReason::Remote.as_str(), "remote");
        assert_eq!(GetDirectMemoryFallbackReason::ObjectInfoMultipart.as_str(), "object_info_multipart");
        assert_eq!(GetDirectMemoryFallbackReason::FileInfoMultipart.as_str(), "file_info_multipart");
        assert_eq!(GetDirectMemoryFallbackReason::InvalidSize.as_str(), "invalid_size");
        assert_eq!(GetDirectMemoryFallbackReason::SizeMismatch.as_str(), "size_mismatch");
        assert_eq!(GetDirectMemoryFallbackReason::AboveThreshold.as_str(), "above_threshold");
    }

    #[test]
    fn small_object_direct_memory_eligibility_respects_threshold_and_shape() {
        let (object_info, fi, opts) = direct_memory_test_metadata(128 * 1024);
        assert!(is_get_small_object_direct_memory_eligible_with_threshold(
            &None,
            &object_info,
            &fi,
            &opts,
            128 * 1024
        ));
        assert!(!is_get_small_object_direct_memory_eligible_with_threshold(
            &None,
            &object_info,
            &fi,
            &opts,
            (128 * 1024) - 1
        ));

        let mut multipart = object_info;
        multipart.parts = Arc::new(vec![ObjectPartInfo::default(), ObjectPartInfo::default()]);
        assert!(!is_get_small_object_direct_memory_eligible_with_threshold(
            &None,
            &multipart,
            &fi,
            &opts,
            128 * 1024
        ));
    }

    async fn inline_bitrot_files_for_payload(payload: &[u8]) -> (coding::Erasure, Vec<FileInfo>, usize, HashAlgorithm) {
        let erasure = coding::Erasure::new(4, 2, 1024 * 1024);
        let read_length = erasure.shard_file_offset(0, payload.len(), payload.len());
        let checksum_algo = HashAlgorithm::HighwayHash256S;
        let shards = erasure.encode_data(payload).expect("payload should encode");
        let mut files = Vec::with_capacity(shards.len());

        for shard in shards {
            let mut writer = coding::BitrotWriterWrapper::new(
                coding::CustomWriter::new_inline_buffer(),
                erasure.shard_size(),
                checksum_algo.clone(),
            );
            writer.write(&shard).await.expect("inline shard should write");
            writer.shutdown().await.expect("inline writer should shutdown");
            let data = writer.into_inline_data().expect("inline data should be retained");
            let mut file = FileInfo::new("bucket/object", erasure.data_shards, erasure.parity_shards);
            file.erasure.index = files.len() + 1;
            file.data = Some(Bytes::from(data));
            files.push(file);
        }

        (erasure, files, read_length, checksum_algo)
    }

    fn inline_data_shard_fileinfo(
        name: &str,
        data_blocks: usize,
        parity_blocks: usize,
        erasure_index: usize,
        distribution: &[usize],
        data: Option<&'static [u8]>,
    ) -> FileInfo {
        let mut fi = FileInfo::new(name, data_blocks, parity_blocks);
        fi.name = name.to_string();
        fi.erasure.index = erasure_index;
        fi.erasure.distribution = distribution.to_vec();
        fi.data = data.map(Bytes::from_static);
        fi
    }

    #[test]
    fn collect_inline_data_shards_by_index_uses_distribution_order() {
        let distribution = vec![3, 1, 5, 2, 4, 6];
        let mut fi = FileInfo::new("object", 4, 2);
        fi.erasure.distribution = distribution.clone();
        let files = vec![
            inline_data_shard_fileinfo("block-3", 4, 2, 3, &distribution, Some(b"c")),
            inline_data_shard_fileinfo("block-1", 4, 2, 1, &distribution, Some(b"a")),
            inline_data_shard_fileinfo("parity-5", 4, 2, 5, &distribution, Some(b"p")),
            inline_data_shard_fileinfo("block-2", 4, 2, 2, &distribution, Some(b"b")),
            inline_data_shard_fileinfo("block-4", 4, 2, 4, &distribution, Some(b"d")),
            inline_data_shard_fileinfo("parity-6", 4, 2, 6, &distribution, Some(b"q")),
        ];

        let data_files =
            collect_inline_data_shard_fileinfos_by_index(&files, &fi, 4, |_| true).expect("all data shards should be collected");

        assert_eq!(
            data_files.iter().map(|file| file.name.as_str()).collect::<Vec<_>>(),
            ["block-1", "block-2", "block-3", "block-4"]
        );
    }

    #[test]
    fn collect_inline_data_shards_by_index_rejects_missing_data_shard() {
        let distribution = vec![1, 2, 3, 4];
        let mut fi = FileInfo::new("object", 2, 2);
        fi.erasure.distribution = distribution.clone();
        let files = vec![
            inline_data_shard_fileinfo("block-1", 2, 2, 1, &distribution, Some(b"a")),
            inline_data_shard_fileinfo("block-2", 2, 2, 2, &distribution, None),
            inline_data_shard_fileinfo("parity-3", 2, 2, 3, &distribution, Some(b"p")),
            inline_data_shard_fileinfo("parity-4", 2, 2, 4, &distribution, Some(b"q")),
        ];

        assert!(collect_inline_data_shard_fileinfos_by_index(&files, &fi, 2, |_| true).is_none());
    }

    #[tokio::test]
    async fn inline_data_shards_direct_read_reassembles_payload() {
        let payload = b"small inline object payload that spans data shards";
        let (erasure, files, read_length, checksum_algo) = inline_bitrot_files_for_payload(payload).await;
        let mut readers = build_inline_bitrot_readers(
            &files,
            erasure.data_shards,
            "bucket",
            "object",
            read_length,
            erasure.shard_size(),
            &checksum_algo,
            false,
        )
        .await
        .expect("inline bitrot readers should build");
        assert_eq!(readers.len(), erasure.data_shards);

        let body = try_read_inline_data_shards_direct(&mut readers, erasure.data_shards, read_length, payload.len())
            .await
            .expect("data shard direct read should succeed");

        assert_eq!(body.as_ref(), payload);
    }

    #[tokio::test]
    async fn inline_data_shards_direct_read_rejects_corrupt_shard() {
        let payload = b"small inline object payload that will be corrupted";
        let (erasure, mut files, read_length, checksum_algo) = inline_bitrot_files_for_payload(payload).await;
        let first = files[0].data.as_mut().expect("first shard should exist");
        let mut corrupted = first.to_vec();
        let last = corrupted.last_mut().expect("encoded shard should not be empty");
        *last ^= 0xff;
        *first = Bytes::from(corrupted);

        let mut readers = build_inline_bitrot_readers(
            &files,
            erasure.total_shard_count(),
            "bucket",
            "object",
            read_length,
            erasure.shard_size(),
            &checksum_algo,
            false,
        )
        .await
        .expect("inline bitrot readers should build");

        let body = try_read_inline_data_shards_direct(&mut readers, 4, read_length, payload.len()).await;

        assert!(body.is_none());
    }

    #[test]
    fn inline_data_shards_direct_read_requires_single_block() {
        assert!(can_try_inline_data_shards_direct(1024, 1024));
        assert!(!can_try_inline_data_shards_direct(0, 1024));
        assert!(!can_try_inline_data_shards_direct(1025, 1024));
    }

    #[test]
    fn inline_erasure_offset_helpers_match_erasure_methods() {
        for uses_legacy in [false, true] {
            let erasure = coding::Erasure::new_with_options(4, 2, 1024 * 1024, uses_legacy);
            for object_size in [1usize, 1024, 100 * 1024, 1024 * 1024] {
                assert_eq!(
                    inline_erasure_shard_size(erasure.block_size, erasure.data_shards, uses_legacy),
                    erasure.shard_size()
                );
                assert_eq!(
                    inline_erasure_shard_file_offset(
                        0,
                        object_size,
                        object_size,
                        erasure.block_size,
                        erasure.data_shards,
                        uses_legacy,
                    ),
                    erasure.shard_file_offset(0, object_size, object_size)
                );
            }
        }
    }

    #[tokio::test]
    async fn direct_memory_inline_data_shards_direct_read_reassembles_single_block_payload() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let endpoint =
            Endpoint::try_from(tempdir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");

        let payload = vec![b'i'; 192 * 1024];
        let (erasure, files, _read_length, _checksum_algo) = inline_bitrot_files_for_payload(&payload).await;
        let mut fi = FileInfo::new("bucket/object", erasure.data_shards, erasure.parity_shards);
        fi.size = payload.len() as i64;
        fi.data = files[0].data.clone();
        fi.add_object_part(1, String::new(), payload.len(), None, payload.len() as i64, None, None);

        let disks = vec![Some(disk); erasure.total_shard_count()];
        let metrics_size_bucket = rustfs_io_metrics::get_object_size_bucket(fi.size);

        let body = SetDisks::try_get_object_direct_data_shards_with_fileinfo(
            "bucket",
            "object",
            &fi,
            &files,
            &disks,
            true,
            GET_CODEC_STREAMING_OBJECT_CLASS_PLAIN_SINGLE_PART,
            metrics_size_bucket,
        )
        .await
        .expect("direct-memory inline data shard read should not fail")
        .expect("inline data shard path should be used");

        assert_eq!(body.as_ref(), payload);
    }

    #[tokio::test]
    async fn direct_memory_data_shards_direct_read_reassembles_single_block_payload() {
        use uuid::Uuid;

        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let endpoint =
            Endpoint::try_from(tempdir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");

        let bucket = "bucket";
        let object = "object";
        let payload = vec![b'd'; 192 * 1024];

        disk.make_volume(bucket).await.expect("bucket should be created");

        let mut fi = FileInfo::new(&format!("{bucket}/{object}"), 1, 0);
        let data_dir = Uuid::new_v4();
        fi.data_dir = Some(data_dir);
        fi.size = payload.len() as i64;
        fi.add_object_part(1, String::new(), payload.len(), None, payload.len() as i64, None, None);

        let erasure = coding::Erasure::new_with_options(
            fi.erasure.data_blocks,
            fi.erasure.parity_blocks,
            fi.erasure.block_size,
            fi.uses_legacy_checksum,
        );
        let shard_path = format!("{object}/{data_dir}/part.1");
        let checksum_info = fi.erasure.get_checksum_info(1);

        let mut bitrot_writer = create_bitrot_writer(
            true,
            None,
            bucket,
            &shard_path,
            payload.len() as i64,
            erasure.shard_size(),
            checksum_info.algorithm.clone(),
        )
        .await
        .expect("bitrot writer should be created");

        for chunk in payload.chunks(erasure.shard_size()) {
            bitrot_writer.write(chunk).await.expect("payload chunk should be written");
        }

        let encoded = bitrot_writer.into_inline_data().expect("bitrot encoded data should exist");
        disk.write_all(bucket, &shard_path, Bytes::from(encoded))
            .await
            .expect("encoded shard should be stored");

        let files = vec![fi.clone()];
        let disks = vec![Some(disk)];
        let metrics_size_bucket = rustfs_io_metrics::get_object_size_bucket(fi.size);

        let body = SetDisks::try_get_object_direct_data_shards_with_fileinfo(
            bucket,
            object,
            &fi,
            &files,
            &disks,
            true,
            GET_CODEC_STREAMING_OBJECT_CLASS_PLAIN_SINGLE_PART,
            metrics_size_bucket,
        )
        .await
        .expect("direct-memory data shard read should not fail")
        .expect("single-block data shard path should be used");

        assert_eq!(body.as_ref(), payload);
    }

    #[tokio::test]
    async fn range_reads_use_shard_span_length_for_non_zero_offsets() {
        use tokio::io::AsyncReadExt;
        use uuid::Uuid;

        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let endpoint =
            Endpoint::try_from(tempdir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");

        let bucket = "bucket";
        let object = "object";
        let payload = vec![b'x'; 3 * 1024 * 1024 + 1234];
        let range_offset = 2 * 1024 * 1024 + 17;
        let range_length = 512 * 1024;

        disk.make_volume(bucket).await.expect("bucket should be created");

        let mut fi = FileInfo::new(&format!("{bucket}/{object}"), 1, 0);
        let data_dir = Uuid::new_v4();
        fi.data_dir = Some(data_dir);
        fi.size = payload.len() as i64;
        fi.add_object_part(1, String::new(), payload.len(), None, payload.len() as i64, None, None);

        let erasure = coding::Erasure::new_with_options(
            fi.erasure.data_blocks,
            fi.erasure.parity_blocks,
            fi.erasure.block_size,
            fi.uses_legacy_checksum,
        );
        let shard_path = format!("{object}/{data_dir}/part.1");
        let checksum_info = fi.erasure.get_checksum_info(1);

        let mut bitrot_writer = create_bitrot_writer(
            true,
            None,
            bucket,
            &shard_path,
            payload.len() as i64,
            erasure.shard_size(),
            checksum_info.algorithm.clone(),
        )
        .await
        .expect("bitrot writer should be created");

        for chunk in payload.chunks(erasure.shard_size()) {
            bitrot_writer.write(chunk).await.expect("payload chunk should be written");
        }

        let encoded = bitrot_writer.into_inline_data().expect("bitrot encoded data should exist");
        disk.write_all(bucket, &shard_path, Bytes::from(encoded))
            .await
            .expect("encoded shard should be stored");

        let files = vec![fi.clone()];
        let disks = vec![Some(disk.clone())];
        let (mut reader, mut writer) = tokio::io::duplex(range_length * 2);
        let metrics_size_bucket = rustfs_io_metrics::get_object_size_bucket(fi.size);

        let read_task = tokio::spawn(async move {
            SetDisks::get_object_with_fileinfo(
                bucket,
                object,
                range_offset,
                range_length as i64,
                &mut writer,
                fi,
                files,
                &disks,
                0,
                0,
                true,
                false,
                GET_OBJECT_PATH_LEGACY_DUPLEX,
                GET_CODEC_STREAMING_OBJECT_CLASS_PLAIN_SINGLE_PART,
                metrics_size_bucket,
            )
            .await
        });

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.expect("range bytes should be readable");

        read_task
            .await
            .expect("read task should complete")
            .expect("range read should succeed");

        assert_eq!(out, payload[range_offset..range_offset + range_length]);
    }

    #[tokio::test]
    async fn multipart_reads_stream_all_parts_with_setup_prefetch() {
        use tokio::io::AsyncReadExt;
        use uuid::Uuid;

        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let endpoint =
            Endpoint::try_from(tempdir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");

        let bucket = "bucket";
        let object = "object";
        // Three parts with distinct fill bytes so cross-part ordering bugs and
        // prefetch boundary mistakes surface as content mismatches
        // (backlog#870 exercises the prefetch hit path for parts 2 and 3).
        let parts: Vec<Vec<u8>> = vec![
            vec![b'a'; 2 * 1024 * 1024 + 111],
            vec![b'b'; 1024 * 1024 + 17],
            vec![b'c'; 3 * 1024 * 1024 + 923],
        ];
        let total_size: usize = parts.iter().map(|part| part.len()).sum();

        disk.make_volume(bucket).await.expect("bucket should be created");

        let mut fi = FileInfo::new(&format!("{bucket}/{object}"), 1, 0);
        let data_dir = Uuid::new_v4();
        fi.data_dir = Some(data_dir);
        fi.size = total_size as i64;
        for (index, part) in parts.iter().enumerate() {
            fi.add_object_part(index + 1, String::new(), part.len(), None, part.len() as i64, None, None);
        }

        let erasure = coding::Erasure::new_with_options(
            fi.erasure.data_blocks,
            fi.erasure.parity_blocks,
            fi.erasure.block_size,
            fi.uses_legacy_checksum,
        );

        for (index, payload) in parts.iter().enumerate() {
            let part_number = index + 1;
            let shard_path = format!("{object}/{data_dir}/part.{part_number}");
            let checksum_info = fi.erasure.get_checksum_info(part_number);

            let mut bitrot_writer = create_bitrot_writer(
                true,
                None,
                bucket,
                &shard_path,
                payload.len() as i64,
                erasure.shard_size(),
                checksum_info.algorithm.clone(),
            )
            .await
            .expect("bitrot writer should be created");

            for chunk in payload.chunks(erasure.shard_size()) {
                bitrot_writer.write(chunk).await.expect("payload chunk should be written");
            }

            let encoded = bitrot_writer.into_inline_data().expect("bitrot encoded data should exist");
            disk.write_all(bucket, &shard_path, Bytes::from(encoded))
                .await
                .expect("encoded shard should be stored");
        }

        let files = vec![fi.clone()];
        let disks = vec![Some(disk.clone())];
        let (mut reader, mut writer) = tokio::io::duplex(64 * 1024);
        let metrics_size_bucket = rustfs_io_metrics::get_object_size_bucket(fi.size);

        let read_task = tokio::spawn(async move {
            SetDisks::get_object_with_fileinfo(
                bucket,
                object,
                0,
                total_size as i64,
                &mut writer,
                fi,
                files,
                &disks,
                0,
                0,
                true,
                false,
                GET_OBJECT_PATH_LEGACY_DUPLEX,
                GET_CODEC_STREAMING_OBJECT_CLASS_PLAIN_SINGLE_PART,
                metrics_size_bucket,
            )
            .await
        });

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.expect("all part bytes should be readable");

        read_task
            .await
            .expect("read task should complete")
            .expect("multipart read should succeed");

        let expected: Vec<u8> = parts.concat();
        assert_eq!(out.len(), expected.len(), "all parts should be streamed");
        assert_eq!(out, expected, "part contents and ordering must survive setup prefetch");
    }

    #[test]
    fn parts_after_marker_uses_marker_position() {
        let part_numbers = (1..=1002).collect::<Vec<_>>();

        let remaining = parts_after_marker(&part_numbers, 1000).expect("marker should exist");

        assert_eq!(remaining, &[1001, 1002]);
    }

    #[test]
    fn parts_after_marker_returns_none_for_missing_marker() {
        let part_numbers = vec![1, 2, 3];

        assert!(parts_after_marker(&part_numbers, 4).is_none());
    }

    #[test]
    fn delete_file_info_version_id_maps_explicit_null_version_to_stored_null() {
        assert_eq!(delete_file_info_version_id(Some(Uuid::nil())), None);

        let version_id = Uuid::new_v4();
        assert_eq!(delete_file_info_version_id(Some(version_id)), Some(version_id));
        assert_eq!(delete_file_info_version_id(None), None);
    }

    #[test]
    fn put_object_fast_path_selection_prefers_inline_only_when_inline_buffer_and_single_block() {
        assert!(should_use_inline_small_fast_path(true, 1024, 4096));
        assert!(!should_use_single_block_non_inline_fast_path(true, 1024, 4096));
        assert!(matches!(classify_small_write_path(true, 1024, 4096), SmallWritePath::Inline));

        assert!(!should_use_inline_small_fast_path(false, 1024, 4096));
        assert!(should_use_single_block_non_inline_fast_path(false, 1024, 4096));
        assert!(matches!(
            classify_small_write_path(false, 1024, 4096),
            SmallWritePath::SingleBlockNonInline
        ));
    }

    #[test]
    fn put_object_fast_path_selection_rejects_zero_and_multi_block_payloads() {
        assert!(!should_use_inline_small_fast_path(true, 0, 4096));
        assert!(!should_use_single_block_non_inline_fast_path(false, 0, 4096));
        assert!(matches!(classify_small_write_path(true, 0, 4096), SmallWritePath::Pipeline));

        assert!(!should_use_inline_small_fast_path(true, -1, 4096));
        assert!(!should_use_single_block_non_inline_fast_path(false, -1, 4096));
        assert!(matches!(classify_small_write_path(false, -1, 4096), SmallWritePath::Pipeline));

        assert!(!should_use_inline_small_fast_path(true, 8192, 4096));
        assert!(!should_use_single_block_non_inline_fast_path(false, 8192, 4096));
        assert!(matches!(classify_small_write_path(false, 8192, 4096), SmallWritePath::Pipeline));
    }

    #[test]
    fn put_object_large_batch_path_only_applies_to_large_ordinary_puts() {
        assert!(matches!(
            classify_put_write_path(false, 64 * 1024 * 1024, 1024 * 1024),
            SmallWritePath::PipelineBatchedLarge
        ));
        assert!(matches!(
            classify_put_write_path(false, 32 * 1024 * 1024, 1024 * 1024),
            SmallWritePath::Pipeline
        ));
        assert!(matches!(
            classify_put_write_path(false, 31 * 1024 * 1024, 1024 * 1024),
            SmallWritePath::Pipeline
        ));
        assert!(matches!(
            classify_put_write_path(true, 64 * 1024 * 1024, 1024 * 1024),
            SmallWritePath::Pipeline
        ));
    }

    #[test]
    fn put_object_classification_uses_only_known_storage_size() {
        assert_eq!(known_put_object_storage_size(42), 42);
        assert_eq!(
            known_put_object_storage_size(HashReader::SIZE_PRESERVE_LAYER),
            HashReader::SIZE_PRESERVE_LAYER
        );
        assert!(matches!(
            classify_put_write_path(false, known_put_object_storage_size(HashReader::SIZE_PRESERVE_LAYER), 1024 * 1024),
            SmallWritePath::Pipeline
        ));
        assert!(matches!(
            classify_put_write_path(false, known_put_object_storage_size(1024 * 1024), 1024 * 1024),
            SmallWritePath::SingleBlockNonInline
        ));
    }

    #[test]
    fn put_object_part_fast_path_selection_matches_single_block_non_inline_rules() {
        assert!(should_use_single_block_non_inline_fast_path(false, 4096, 4096));
        assert!(should_use_single_block_non_inline_fast_path(false, 2048, 4096));
        assert!(!should_use_single_block_non_inline_fast_path(false, 4097, 4096));
        assert!(!should_use_single_block_non_inline_fast_path(false, 0, 4096));
        assert!(matches!(
            classify_small_write_path(false, 4096, 4096),
            SmallWritePath::SingleBlockNonInline
        ));
    }

    #[test]
    fn multipart_put_large_batch_path_only_applies_at_128m_and_above() {
        assert!(matches!(
            classify_multipart_part_write_path(128 * 1024 * 1024, 1024 * 1024),
            SmallWritePath::PipelineBatchedLarge
        ));
        assert!(matches!(
            classify_multipart_part_write_path(64 * 1024 * 1024, 1024 * 1024),
            SmallWritePath::Pipeline
        ));
        assert!(matches!(
            classify_multipart_part_write_path(1024 * 1024, 1024 * 1024),
            SmallWritePath::SingleBlockNonInline
        ));
    }

    #[test]
    fn multipart_write_paths_use_distinct_metric_labels() {
        assert_eq!(SmallWritePath::Pipeline.multipart_metric_label(), "multipart_write_pipeline");
        assert_eq!(
            SmallWritePath::PipelineBatchedLarge.multipart_metric_label(),
            "multipart_write_pipeline_batched_large"
        );
        assert_eq!(
            SmallWritePath::SingleBlockNonInline.multipart_metric_label(),
            "multipart_write_single_block_non_inline"
        );
    }

    #[test]
    fn test_is_cold_storage_class() {
        // Test cold storage classes
        assert!(is_cold_storage_class(storageclass::DEEP_ARCHIVE));
        assert!(is_cold_storage_class(storageclass::GLACIER));
        assert!(is_cold_storage_class(storageclass::GLACIER_IR));

        // Test non-cold storage classes
        assert!(!is_cold_storage_class(storageclass::STANDARD));
        assert!(!is_cold_storage_class(storageclass::RRS));
        assert!(!is_cold_storage_class(storageclass::STANDARD_IA));
        assert!(!is_cold_storage_class(storageclass::EXPRESS_ONEZONE));
    }

    #[test]
    fn test_is_infrequent_access_class() {
        // Test infrequent access classes
        assert!(is_infrequent_access_class(storageclass::ONEZONE_IA));
        assert!(is_infrequent_access_class(storageclass::STANDARD_IA));
        assert!(is_infrequent_access_class(storageclass::INTELLIGENT_TIERING));

        // Test frequent access classes
        assert!(!is_infrequent_access_class(storageclass::STANDARD));
        assert!(!is_infrequent_access_class(storageclass::RRS));
        assert!(!is_infrequent_access_class(storageclass::DEEP_ARCHIVE));
        assert!(!is_infrequent_access_class(storageclass::EXPRESS_ONEZONE));
    }

    // Regression test: `mc cp --storage-class STANDARD` on a tiered object (self-copy) must not
    // return NotImplemented.  When the source object is tiered (transitioned_object.tier is
    // non-empty) the usecase layer in object_usecase.rs intentionally leaves metadata_only=false
    // so that the full copy path is taken.  SetDisks::copy_object must therefore accept a
    // same-bucket/same-key call even when metadata_only=false.
    //
    // Currently this test FAILS because the guard at set_disk.rs:1579 unconditionally rejects
    // !metadata_only with StorageError::NotImplemented.  Once the fix is applied the test will
    // pass (or progress further through the copy path before failing on missing disk data).
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn copy_object_tiered_self_copy_does_not_return_not_implemented() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::Erasure).await;
        let set_disks = make_test_set_disks(vec![Arc::new(LocalClient::with_manager(Arc::new(
            rustfs_lock::GlobalLockManager::new(),
        )))])
        .await;

        // Simulate a tiered object: metadata_only is false (set_disk must handle the full copy),
        // and transitioned_object.tier is non-empty (the object lives on a remote tier).
        let mut src_info = ObjectInfo {
            metadata_only: false,
            transitioned_object: TransitionedObject {
                tier: "NEXTCLOUD".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = set_disks
            .copy_object(
                "bucket",
                "object",
                "bucket",
                "object",
                &mut src_info,
                &ObjectOptions::default(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await;

        // The copy must not be rejected with NotImplemented.  Any other outcome (Ok or a
        // different error such as missing-disk / quorum) is acceptable here.
        if let Err(ref err) = result {
            assert!(
                !matches!(err, StorageError::NotImplemented),
                "tiered self-copy returned NotImplemented — copy_object must handle \
                 metadata_only=false for same-key copies of tiered objects, got: {err}"
            );
        }
    }

    async fn make_local_bucket_test_set_disks() -> Arc<SetDisks> {
        let format = FormatV3::new(1, 2);
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();

        for disk_idx in 0..2 {
            let dir = tempfile::tempdir().expect("tempdir should be created");
            let mut endpoint =
                Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_idx);

            let disk = new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("disk should be created");

            let mut disk_format = format.clone();
            disk_format.erasure.this = format.erasure.sets[0][disk_idx];
            save_format_file(&Some(disk.clone()), &Some(disk_format))
                .await
                .expect("format should be saved");

            mem::forget(dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            2,
            1,
            0,
            0,
            endpoints,
            format,
            Vec::new(),
        )
        .await
    }

    async fn make_local_bucket_test_set_disks_with_missing_format() -> Arc<SetDisks> {
        let format = FormatV3::new(1, 2);
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();

        for disk_idx in 0..2 {
            let dir = tempfile::tempdir().expect("tempdir should be created");
            let mut endpoint =
                Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_idx);

            let disk = new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("disk should be created");

            if disk_idx == 0 {
                let mut disk_format = format.clone();
                disk_format.erasure.this = format.erasure.sets[0][disk_idx];
                save_format_file(&Some(disk.clone()), &Some(disk_format))
                    .await
                    .expect("format should be saved");
            }

            mem::forget(dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            2,
            1,
            0,
            0,
            endpoints,
            format,
            Vec::new(),
        )
        .await
    }

    #[tokio::test]
    async fn bucket_operations_round_trip_without_panicking() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-roundtrip";

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let info = set_disks
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .expect("bucket info should be available");
        assert_eq!(info.name, bucket);

        let buckets = set_disks
            .list_bucket(&BucketOptions::default())
            .await
            .expect("bucket listing should succeed");
        assert!(buckets.iter().any(|entry| entry.name == bucket));

        set_disks
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("bucket should be deleted");
    }

    #[tokio::test]
    async fn set_level_listing_trait_methods_use_existing_listing_implementation() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-listing";

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let mut reader = PutObjReader::from_vec(b"hello".to_vec());
        set_disks
            .put_object(
                bucket,
                "object",
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    ..ObjectOptions::default()
                },
            )
            .await
            .expect("object should be written");

        let list_result = set_disks
            .clone()
            .list_objects_v2(bucket, "", None, None, 1000, false, None, false)
            .await
            .expect("set-level list_objects_v2 should succeed");
        assert_eq!(list_result.objects.len(), 1);
        assert_eq!(list_result.objects[0].name, "object");

        let versions_result = set_disks
            .clone()
            .list_object_versions(bucket, "", None, None, None, 1000)
            .await
            .expect("set-level list_object_versions should succeed");
        assert_eq!(versions_result.objects.len(), 1);
        assert_eq!(versions_result.objects[0].name, "object");

        let (tx, mut rx) = mpsc::channel(4);
        set_disks
            .clone()
            .walk(CancellationToken::new(), bucket, "", tx, WalkOptions::default())
            .await
            .expect("set-level walk should succeed");

        let mut walked_names = Vec::new();
        while let Some(item) = rx.recv().await {
            if let Some(object) = item.item {
                walked_names.push(object.name);
            }
        }
        assert!(walked_names.iter().any(|name| name == "object"));
    }

    #[tokio::test]
    async fn set_level_heal_format_repairs_unformatted_disk() {
        let set_disks = make_local_bucket_test_set_disks_with_missing_format().await;
        let disk = {
            let disks = set_disks.disks.read().await;
            disks[1].clone().expect("second disk should exist")
        };

        let before = load_format_erasure(&disk, true)
            .await
            .expect_err("second disk should start unformatted");
        assert_eq!(before, DiskError::UnformattedDisk);

        let (heal_result, heal_err) = set_disks.heal_format(false).await.expect("heal_format should complete");
        assert!(heal_err.is_none(), "heal_format should repair the local unformatted disk");
        assert_eq!(heal_result.disk_count, 2);
        assert_eq!(heal_result.set_count, 1);
        assert_eq!(heal_result.after.drives[1].state, DriveState::Ok.to_string());

        let repaired = load_format_erasure(&disk, true)
            .await
            .expect("second disk should contain a healed format");
        assert_eq!(repaired.erasure.this, set_disks.format.erasure.sets[0][1]);
    }

    #[tokio::test]
    async fn remaining_unsupported_trait_stubs_return_typed_errors() {
        let set_disks = make_test_set_disks(Vec::new()).await;

        let (heal_result, heal_err) = make_local_bucket_test_set_disks()
            .await
            .heal_format(false)
            .await
            .expect("heal_format should be callable on formatted disks");
        assert!(matches!(heal_err, Some(StorageError::NoHealRequired)));
        assert_eq!(heal_result.disk_count, 2);

        let copy_part_err = set_disks
            .copy_object_part(
                "bucket",
                "src",
                "bucket",
                "dst",
                "upload-id",
                1,
                0,
                1,
                &ObjectInfo::default(),
                &ObjectOptions::default(),
                &ObjectOptions::default(),
            )
            .await
            .expect_err("unsupported copy_object_part should return a typed error");
        assert!(matches!(copy_part_err, StorageError::NotImplemented));

        let abandoned_err = set_disks
            .check_abandoned_parts("bucket", "object", &HealOpts::default())
            .await
            .expect_err("abandoned-parts check should stay in the upper reconciliation layer");
        assert!(matches!(abandoned_err, StorageError::NotImplemented));
    }

    #[tokio::test]
    async fn stat_all_dirs_returns_index_aligned_vector_for_offline_disks() {
        // All-offline set: no real disk I/O needed. Isolates the length/index-alignment contract
        // that heal_object_dir depends on when it zips `errs` against the full `disks` array.
        let disks: Vec<Option<DiskStore>> = vec![None, None, None, None];

        let errs = stat_all_dirs(&disks, "bucket", "object").await;

        // Before the fix, offline disks contributed no future and the collected vector had length
        // 0, so any zip against `disks` paired errors with the wrong disk. After the fix each slot
        // is DiskNotFound, index-aligned with `disks`.
        assert_eq!(
            errs.len(),
            disks.len(),
            "stat_all_dirs must return one entry per disk slot to stay index-aligned"
        );
        for err in &errs {
            assert!(
                matches!(err, Some(DiskError::DiskNotFound)),
                "offline (None) disk slot must map to DiskNotFound in-place, got {err:?}"
            );
        }
    }
}
