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
#![allow(unused_imports)]
#![allow(unused_variables)]

use crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;
use crate::bucket::metadata_sys;
use crate::bucket::metadata_sys::ObjectLockConfigState;
use crate::bucket::object_lock::objectlock_sys::{
    check_object_lock_for_deletion_with_config, check_object_lock_for_deletion_with_state, check_retention_for_modification,
};
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
    GET_CODEC_STREAMING_OBJECT_CLASS_PLAIN_SINGLE_PART, GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_GEOMETRY,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_IDENTITY_MISMATCH,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_MISSING_PAYLOAD,
    GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_MISSING_SHARD, GET_OBJECT_PATH_BODY_CACHE, GET_OBJECT_PATH_CODEC_STREAMING,
    GET_OBJECT_PATH_CODEC_STREAMING_LEGACY_ENGINE, GET_OBJECT_PATH_CODEC_STREAMING_RUSTFS_ENGINE, GET_OBJECT_PATH_DIRECT_MEMORY,
    GET_OBJECT_PATH_EMPTY, GET_OBJECT_PATH_INLINE_DIRECT, GET_OBJECT_PATH_INTERNAL_META, GET_OBJECT_PATH_LEGACY_DUPLEX,
    GET_OBJECT_PATH_REMOTE_TRANSITION, GET_OBJECT_PATH_SET_DISK, GET_STAGE_DECODE, GET_STAGE_EMIT, GET_STAGE_INLINE_PREPARE,
    GET_STAGE_LOCK_ACQUIRE, GET_STAGE_METADATA, GET_STAGE_OBJECT_INFO, GET_STAGE_PATH_DECISION, GET_STAGE_READER_SETUP,
    classify_storage_error, get_stage_timer_if_enabled, record_get_object_pipeline_failure,
    record_get_object_pipeline_failure_for_path, record_get_stage_duration_if_enabled,
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
use crate::runtime::instance::{InstanceContext, bootstrap_ctx};
use crate::runtime::sources as runtime_sources;
use crate::services::batch_processor::AsyncBatchProcessor;
use crate::storage_api_contracts::{
    bucket::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions},
    list::{StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageObjectInfoOrErr, StorageWalkOptions},
    multipart::{
        CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartOperations as _, MultipartUploadResult, PartInfo,
    },
    namespace::NamespaceLocking as _,
    object::{DeleteAccounting, DeletedObject, HTTPPreconditions, ObjectIO as _, ObjectOperations as _, ObjectToDelete},
    range::HTTPRangeSpec,
};
use crate::store::utils::is_reserved_or_invalid_bucket;
use crate::{
    bucket::lifecycle::bucket_lifecycle_ops::{LifecycleOps, get_transitioned_object_reader_with_tier_manager, put_restore_opts},
    cache_value::metacache_set::{ListPathRawOptions, list_path_raw},
    config::storageclass,
    disk::{
        CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskOption, DiskStore, FileInfoVersions,
        RUSTFS_META_BUCKET, RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_BUCKET, ReadMultipleReq, ReadMultipleResp, ReadOptions,
        SnapshotLeaseToken, UpdateMetadataOpts, endpoint::Endpoint, error::DiskError, format::FormatV3, new_disk,
    },
    error::{StorageError, to_object_err},
    object_api::{GetObjectReader, NamespaceLockFence, ObjectInfo, ObjectLockConfigSnapshot, PutObjReader},
    // event::name::EventName,
    services::event_notification::{EventArgs, send_event},
    store::init_format::{
        formats_match_reference_slots, get_format_erasure_in_quorum, load_format_erasure, load_format_erasure_all,
        save_format_file,
    },
};
use bytes::Bytes;
use bytesize::ByteSize;
use chrono::Utc;
use futures::future::join_all;
use futures::task::AtomicWaker;
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
    CapacityScope, CapacityScopeDisk, current_dirty_generation, record_capacity_scope, record_global_dirty_scope,
};
use rustfs_s3_types::EventName;
#[cfg(test)]
use rustfs_utils::http::SSEC_ALGORITHM_HEADER;
use rustfs_utils::http::headers::AMZ_OBJECT_TAGGING;
use rustfs_utils::http::headers::AMZ_STORAGE_CLASS;
use rustfs_utils::http::headers::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_TYPE, EXPIRES, HeaderExt as _,
};
use rustfs_utils::http::{
    SUFFIX_ACTUAL_OBJECT_SIZE_CAP, SUFFIX_ACTUAL_SIZE, SUFFIX_BUCKET_INCARNATION_ID, SUFFIX_COMPRESSION, SUFFIX_COMPRESSION_SIZE,
    SUFFIX_REPLICATION_SSEC_CRC, SUFFIX_RESTORE_OPERATION_ID, contains_key_str, get_header_map, get_str, insert_str,
    is_object_encryption_marker, remove_header_map,
};
use rustfs_utils::{
    HashAlgorithm,
    crypto::hex,
    path::{SLASH_SEPARATOR, encode_dir_object, has_suffix, path_join_buf},
};
use s3s::header::{X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, X_AMZ_RESTORE};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::{self};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{
    collections::{HashMap, HashSet},
    io::{Cursor, Write},
    path::Path,
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

pub(super) fn restore_operation_id_from_metadata(metadata: &HashMap<String, String>) -> Result<Option<Uuid>> {
    let Some(value) = rustfs_utils::http::metadata_compat::get_consistent_str(metadata, SUFFIX_RESTORE_OPERATION_ID) else {
        if rustfs_utils::http::metadata_compat::contains_key_str(metadata, SUFFIX_RESTORE_OPERATION_ID) {
            return Err(Error::other("invalid restore operation id metadata".to_string()));
        }
        return Ok(None);
    };
    let id = Uuid::parse_str(value).map_err(|_| Error::other("invalid restore operation id metadata".to_string()))?;
    if id.is_nil() {
        return Err(Error::other("invalid restore operation id metadata".to_string()));
    }
    Ok(Some(id))
}

pub(super) fn require_restore_operation_id(metadata: &HashMap<String, String>, expected: Uuid) -> Result<()> {
    match restore_operation_id_from_metadata(metadata)? {
        Some(actual) if actual == expected => Ok(()),
        _ => Err(Error::other("restore operation id changed before copy-back".to_string())),
    }
}

pub(super) fn restore_commit_operation_id_from_metadata(metadata: &HashMap<String, String>) -> Result<Option<Uuid>> {
    if !metadata.contains_key(X_AMZ_RESTORE.as_str()) {
        return Ok(None);
    }
    restore_operation_id_from_metadata(metadata)
}

impl SetDisks {
    pub(super) async fn require_current_restore_operation_id(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
        expected: Option<Uuid>,
        mode: &str,
    ) -> Result<()> {
        let Some(expected) = expected else {
            return Ok(());
        };
        let read_opts = ObjectOptions {
            version_id: opts.version_id.clone(),
            versioned: opts.versioned,
            version_suspended: opts.version_suspended,
            no_lock: true,
            ..Default::default()
        };
        let current = self.get_object_fileinfo(bucket, object, &read_opts, true, false).await?;
        restore_operation_id_from_metadata(&current.fi().metadata)?
            .filter(|actual| *actual == expected)
            .ok_or_else(|| Error::other(format!("restore operation id changed before {mode}: expected {expected}")))?;
        Ok(())
    }
}

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
type WalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;
type InlineBitrotReader = coding::BitrotReader<crate::io_support::bitrot::ShardReader>;

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
const EVENT_SET_DISK_RENAME_TAIL_DRAIN_FAILED: &str = "set_disk_rename_tail_drain_failed";
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
pub(crate) const DATA_MOVEMENT_MULTIPART_PREFIX: &str = "data-movement";
const ENV_ISSUE3031_DIAG_ENABLE: &str = "RUSTFS_ISSUE3031_DIAG_ENABLE";

/// Validate disk metadata at a boundary that may legitimately return a delete
/// marker. Disk/RPC decode boundaries perform the full collection validation
/// once; repeated quorum passes use the cheap erasure-geometry predicate for
/// payload entries and the canonical marker predicate for pure delete markers.
pub(in crate::set_disk) fn file_info_is_valid_for_metadata(file_info: &FileInfo) -> bool {
    file_info.has_valid_metadata_shape()
}

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

    /// Whether the underlying namespace lock's heartbeat has observed a
    /// refresh-quorum loss (backlog#899 Phase 2). Callers fence their commit
    /// point on this so a stale lock holder does not race a double-write.
    fn is_lock_lost(&self) -> bool {
        self.guard.is_lock_lost()
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
    bucket: &str,
    object: &str,
) -> GetObjectReader {
    if reader.buffered_body.is_some() {
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
    rustfs_utils::http::metadata_compat::remove_str(metadata, SUFFIX_BUCKET_INCARNATION_ID);
}

fn should_persist_encryption_original_size(metadata: &HashMap<String, String>) -> bool {
    metadata.keys().any(|key| is_object_encryption_marker(key))
}

/// Per-set memoized capacity dirty scope.
///
/// The disk endpoint/path identity of each slot in a set is immutable for the
/// set's lifetime (heal replaces the [`DiskStore`] instance but keeps the same
/// endpoint and root), so the dirty scope only needs to be built once instead
/// of allocating a `String` per online disk on every successful write
/// (backlog#1315). Slots are filled lazily as disks are observed online; once
/// every slot has contributed, `complete` latches and the hot path returns the
/// shared `Arc` without any scan.
#[derive(Default, Debug)]
struct CapacityScopeCache {
    /// Per-slot resolved disk identity; `None` slots have not been seen online.
    slots: Vec<Option<CapacityScopeDisk>>,
    /// Deduplicated scope covering every slot resolved so far.
    scope: Option<Arc<CapacityScope>>,
    /// Latches once every slot is resolved; the scope is then stable.
    complete: bool,
}

impl CapacityScopeCache {
    /// Whether `disks` contains an online disk whose slot has not been recorded
    /// yet. Read-only, allocation-free (used under the read lock).
    fn has_unresolved_slot(&self, disks: &[Option<DiskStore>]) -> bool {
        disks
            .iter()
            .enumerate()
            .any(|(idx, disk)| disk.is_some() && self.slots.get(idx).is_none_or(|slot| slot.is_none()))
    }
}

impl SetDisks {
    /// Return the memoized dirty scope for this set, resolving any newly-online
    /// slots first. Steady-state writes hit the fast path (a read lock and an
    /// `Arc` clone) with no per-disk `String` allocation.
    fn capacity_scope(&self, disks: &[Option<DiskStore>]) -> Arc<CapacityScope> {
        {
            let cache = self.capacity_scope_cache.read().unwrap_or_else(|p| p.into_inner());
            if let Some(scope) = cache.scope.as_ref()
                && (cache.complete || !cache.has_unresolved_slot(disks))
            {
                return scope.clone();
            }
        }
        self.resolve_capacity_scope(disks)
    }

    /// Slow path: fill newly-observed slots and rebuild the deduplicated scope.
    fn resolve_capacity_scope(&self, disks: &[Option<DiskStore>]) -> Arc<CapacityScope> {
        let mut cache = self.capacity_scope_cache.write().unwrap_or_else(|p| p.into_inner());
        if cache.slots.len() < self.set_drive_count {
            cache.slots.resize(self.set_drive_count, None);
        }

        let mut changed = false;
        for (idx, disk) in disks.iter().enumerate() {
            let Some(slot) = cache.slots.get_mut(idx) else {
                break;
            };
            if slot.is_some() {
                continue;
            }
            if let Some(disk) = disk {
                *slot = Some(CapacityScopeDisk {
                    endpoint: disk.endpoint().to_string(),
                    drive_path: disk.to_string(),
                });
                changed = true;
            }
        }

        if changed || cache.scope.is_none() {
            let mut unique = HashSet::with_capacity(cache.slots.len());
            let mut scoped_disks = Vec::with_capacity(cache.slots.len());
            for slot in cache.slots.iter().flatten() {
                if unique.insert(slot.clone()) {
                    scoped_disks.push(slot.clone());
                }
            }
            cache.complete = !cache.slots.is_empty() && cache.slots.iter().all(|slot| slot.is_some());
            cache.scope = Some(Arc::new(CapacityScope { disks: scoped_disks }));
        }

        cache.scope.clone().unwrap_or_else(|| Arc::new(CapacityScope::default()))
    }

    /// Record the set's dirty scope after a successful write.
    ///
    /// The global dirty registry is only upgraded on the first write of each
    /// registry generation: once this set has marked its disks, subsequent
    /// writes skip the global mutex until a refresh drain advances the
    /// generation, forcing a re-mark (backlog#1315). The scope-token registry
    /// (multipart completion) still records per token so the app-side write
    /// settle can consume it.
    fn record_capacity_scope_if_needed(&self, scope_token: Option<Uuid>, disks: &[Option<DiskStore>]) {
        let scope = self.capacity_scope(disks);
        if scope.disks.is_empty() {
            return;
        }

        let generation = current_dirty_generation();
        if self.capacity_dirty_generation.load(Ordering::Acquire) != generation {
            // First write of this generation (or after a drain): upgrade the
            // global registry and cache the generation observed under its lock.
            let observed = record_global_dirty_scope((*scope).clone());
            self.capacity_dirty_generation.store(observed, Ordering::Release);
        }

        if let Some(token) = scope_token {
            record_capacity_scope(token, (*scope).clone());
        }
    }

    /// Mark healed disks dirty from the (infrequent) heal path.
    ///
    /// Heal passes disks in erasure-distribution order (`shuffle_disks`), not
    /// physical-slot order, so they must not seed the slot-indexed memo — doing
    /// so could record a disk under the wrong slot and drop another from the
    /// steady-state scope. Heal therefore builds an ad-hoc scope from the disks
    /// it actually rewrote and marks the global registry directly. This runs at
    /// heal frequency, so it does not use the per-generation skip fast-path
    /// (backlog#1315).
    fn record_healed_capacity_scope(&self, disks: &[Option<DiskStore>]) {
        let scope = capacity_scope_from_disks(disks);
        if scope.disks.is_empty() {
            return;
        }
        // Do not advance the set's generation marker here: this ad-hoc scope is
        // only the subset of disks heal rewrote, whereas the marker asserts the
        // full set was marked. Leaving the marker untouched keeps the next write
        // free to upgrade the full-set scope if it has not been marked yet.
        let _ = record_global_dirty_scope(scope);
    }
}

/// Build an ad-hoc, deduplicated dirty scope from `disks`. Used by the heal
/// path where disks are not in physical-slot order (backlog#1315).
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
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_DUPLEX_BUFFER_SIZE,
            rustfs_config::DEFAULT_OBJECT_DUPLEX_BUFFER_SIZE,
        )
        .max(1)
    })
}

/// Get adaptive duplex buffer size based on object size.
///
/// Smaller objects get smaller buffers to reduce memory waste.
/// Larger objects get larger buffers to prevent backpressure.
fn adaptive_duplex_buffer_size(object_size: i64) -> usize {
    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;
    let target = match object_size {
        0..=131_072 => 64 * KB,             // <= 128KB: 64KB
        131_073..=1_048_576 => 512 * KB,    // <= 1MB: reduce duplex backpressure without a 1MB pipe per request
        1_048_577..=16_777_216 => MB,       // <= 16MB: 1MB
        16_777_217..=268_435_456 => 4 * MB, // <= 256MB: 4MB
        _ => 8 * MB,                        // > 256MB: 8MB
    };
    let object_cap = usize::try_from(object_size).ok().filter(|size| *size > 0).unwrap_or(target);
    target.min(object_cap.max(64 * KB)).min(get_duplex_buffer_size())
}

// ============================================================================
// GET Optimization Configuration
//
// All GET performance optimization flags are consolidated here.
// Each flag uses `OnceLock` for caching — env var changes require process restart.
// Each flag has a corresponding `*_ROLLOUT_PCT` for percentage-based gradual rollout.
// ============================================================================

#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
const DISK_ONLINE_TIMEOUT: Duration = Duration::from_secs(1);
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
const DISK_HEALTH_CACHE_TTL: Duration = Duration::from_millis(750);
const GET_OBJECT_METADATA_CACHE_TTL: Duration = Duration::from_secs(2); // Increased from 250ms to 2s
const DEFAULT_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES: usize = 4096; // Increased from 1024 to 4096
const ENV_RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES: &str = "RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES";
const GET_OBJECT_METADATA_CACHE_FENCE_SHARDS: u16 = 4096;

// --- Codec Streaming Configuration ---

const ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE: &str = "RUSTFS_GET_CODEC_STREAMING_ENABLE";
// Emergency kill-switch, not the primary enablement knob. The single switch that
// turns codec streaming on/off is `RUSTFS_GET_CODEC_STREAMING_ROLLOUT` (default
// `off`). This flag defaults to `true` and only exists to force-disable the fast
// path regardless of rollout (set to `false`). Body/header compatibility is
// confirmed by the GET codec-streaming parity e2e net + bench A/B (backlog#1183),
// so it no longer gates enablement. See `get_codec_streaming_reader_gate`.
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_ENABLE: bool = true;

const ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE: &str = "RUSTFS_GET_CODEC_STREAMING_MIN_SIZE";
// Meet the direct-memory path at its default ceiling. Codec streaming remains
// rollout-gated and starts where the eager small-object path ends.
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE: usize = DEFAULT_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD;
const ENV_RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE: &str = "RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE: usize = DEFAULT_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE;

const ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE: &str = "RUSTFS_GET_CODEC_STREAMING_ENGINE";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_ENGINE: &str = GET_CODEC_STREAMING_ENGINE_LEGACY;

// Primary single switch for the codec-streaming GET fast path. `off` (default)
// keeps GET on the legacy duplex path; `on` (aliases `full`/`production`, plus
// the legacy `internal`/`benchmark`) opts it in. Combine with
// `..._ROLLOUT_PCT` for a partial (percentage-based) rollout.
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
// On by default (rustfs/backlog#1802): a small object whose data shards are
// inlined in xl.meta is reassembled straight from the already-resolved
// metadata, skipping the Erasure reconstruct pipeline. The path has a complete
// fallback — if the inline reassembly returns None, the GET proceeds through
// the normal shard-read pipeline, so a miss is correctness-neutral. Set to
// `false` to force the legacy path (kill switch).
const DEFAULT_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY: bool = true;
const ENV_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD: &str = "RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD";
const DEFAULT_RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD: usize = 128 * 1024;

// --- Metadata Early-Stop Configuration ---

const ENV_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE: &str = "RUSTFS_GET_METADATA_EARLY_STOP_ENABLE";
// Enabled by default (backlog#872): the early-stop path only engages for
// requests `should_allow_metadata_early_stop` classifies as safe (latest-version
// reads by default, without version_id / healing / free-version needs) and still
// requires a full read-quorum agreement before stopping. Data-read requests add
// a separate inline-shard verifier before cancelling the remaining fanout. Set
// the env var to `false` to fall back to full-wait metadata fanout.
const DEFAULT_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE: bool = true;

#[allow(
    dead_code,
    reason = "percentage-rollout facet of the metadata early-stop switch; its predicate has no caller while the sibling enable flag is live (backlog#1823)"
)]
const ENV_RUSTFS_GET_METADATA_EARLY_STOP_ROLLOUT_PCT: &str = "RUSTFS_GET_METADATA_EARLY_STOP_ROLLOUT_PCT";
#[allow(
    dead_code,
    reason = "percentage-rollout facet of the metadata early-stop switch; its predicate has no caller while the sibling enable flag is live (backlog#1823)"
)]
const DEFAULT_RUSTFS_GET_METADATA_EARLY_STOP_ROLLOUT_PCT: u32 = 100;

const ENV_RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE: &str = "RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE";
const DEFAULT_RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE: bool = false;

const ENV_RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE: &str = "RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE";
const DEFAULT_RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE: bool = true;

const ENV_RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT: &str = "RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT";
const DEFAULT_RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT: bool = true;

const ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS: &str = "RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS";
const ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DISKS: &str = "RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DISKS";
const ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_BUCKET: &str = "RUSTFS_GET_METADATA_SLOWTAIL_FAULT_BUCKET";
const ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_OBJECT_PREFIX: &str = "RUSTFS_GET_METADATA_SLOWTAIL_FAULT_OBJECT_PREFIX";

// --- Multipart Reader-Setup Prefetch Configuration (backlog#870) ---

const ENV_RUSTFS_GET_MULTIPART_READER_SETUP_PREFETCH: &str = "RUSTFS_GET_MULTIPART_READER_SETUP_PREFETCH";
const DEFAULT_RUSTFS_GET_MULTIPART_READER_SETUP_PREFETCH: bool = true;

static OBJECT_LOCK_DIAG_ENABLED: OnceLock<bool> = OnceLock::new();

mod core;
#[cfg(test)]
pub(crate) use core::io_primitives::disk_call_counters;
mod ctx;
mod metadata;
mod ops;
#[cfg(test)]
pub(crate) use ops::multipart::NewMultipartUploadCommitObservation;
#[cfg(any(test, feature = "test-util"))]
pub use ops::multipart::{MultipartCommitBarrier, MultipartCommitPause};
#[cfg(test)]
pub(crate) use ops::object::DeleteObjectCommitBarrier;
#[cfg(feature = "test-util")]
pub(crate) use ops::object::TransitionCleanupStoreBarrier as SetDiskTransitionCleanupStoreBarrier;
pub(crate) use ops::object::body_cache_plaintext_len;
#[cfg(test)]
pub(crate) use ops::object::cleanup_rejected_transition_upload_durably;
#[cfg(any(test, feature = "test-util"))]
pub use ops::object::{PutObjectCommitBarrier, PutObjectCommitPause};
mod read;
mod replication;
pub(crate) mod shard_source;
#[cfg(all(test, feature = "test-util"))]
mod transition_matrix_tests;

pub use ops::heal_walk::HealWalkVersion;

pub(in crate::set_disk) struct GetObjectFileInfo {
    owned: Option<OwnedGetObjectFileInfo>,
    shared: Option<Arc<GetObjectMetadataCacheEntry>>,
}

struct OwnedGetObjectFileInfo {
    fi: FileInfo,
    parts_metadata: Vec<FileInfo>,
    online_disks: Vec<Option<DiskStore>>,
}

impl GetObjectFileInfo {
    fn owned(fi: FileInfo, parts_metadata: Vec<FileInfo>, online_disks: Vec<Option<DiskStore>>) -> Self {
        Self {
            owned: Some(OwnedGetObjectFileInfo {
                fi,
                parts_metadata,
                online_disks,
            }),
            shared: None,
        }
    }

    fn shared(entry: Arc<GetObjectMetadataCacheEntry>) -> Self {
        Self {
            owned: None,
            shared: Some(entry),
        }
    }

    fn fi(&self) -> &FileInfo {
        match (&self.owned, &self.shared) {
            (Some(snapshot), None) => &snapshot.fi,
            (None, Some(entry)) => &entry.fi,
            _ => unreachable!("GET metadata snapshot representation must be exclusive"),
        }
    }

    fn parts_metadata(&self) -> &[FileInfo] {
        match (&self.owned, &self.shared) {
            (Some(snapshot), None) => &snapshot.parts_metadata,
            (None, Some(entry)) => &entry.parts_metadata,
            _ => unreachable!("GET metadata snapshot representation must be exclusive"),
        }
    }

    fn online_disks(&self) -> &[Option<DiskStore>] {
        match (&self.owned, &self.shared) {
            (Some(snapshot), None) => &snapshot.online_disks,
            (None, Some(entry)) => &entry.online_disks,
            _ => unreachable!("GET metadata snapshot representation must be exclusive"),
        }
    }

    fn into_owned(self) -> (FileInfo, Vec<FileInfo>, Vec<Option<DiskStore>>) {
        match (self.owned, self.shared) {
            (Some(snapshot), None) => {
                let OwnedGetObjectFileInfo {
                    fi,
                    parts_metadata,
                    online_disks,
                } = snapshot;
                (fi, parts_metadata, online_disks)
            }
            (None, Some(entry)) => match Arc::try_unwrap(entry) {
                Ok(entry) => (entry.fi, entry.parts_metadata, entry.online_disks),
                Err(entry) => (entry.fi.clone(), entry.parts_metadata.clone(), entry.online_disks.clone()),
            },
            _ => unreachable!("GET metadata snapshot representation must be exclusive"),
        }
    }

    #[cfg(test)]
    fn has_valid_representation(&self) -> bool {
        self.owned.is_some() ^ self.shared.is_some()
    }

    #[cfg(test)]
    fn shared_entry(&self) -> Option<&Arc<GetObjectMetadataCacheEntry>> {
        self.shared.as_ref()
    }
}

pub(crate) struct PreparedGetObjectMetadata {
    snapshot: GetObjectFileInfo,
    object_info: Option<ObjectInfo>,
}

impl PreparedGetObjectMetadata {
    pub(crate) fn object_info(&self) -> &ObjectInfo {
        self.object_info
            .as_ref()
            .expect("prepared GET metadata must retain its ObjectInfo until consumed")
    }

    pub(crate) fn take_object_info(&mut self) -> ObjectInfo {
        self.object_info
            .take()
            .expect("prepared GET metadata ObjectInfo must be consumed exactly once")
    }

    pub(crate) fn read_semantics_identity(&self) -> [u8; 32] {
        SetDisks::file_info_quorum_hash(self.snapshot.fi())
    }
}

tokio::task_local! {
    static PREPARED_GET_OBJECT_METADATA: std::cell::RefCell<Option<PreparedGetObjectMetadata>>;
}

#[cfg(test)]
tokio::task_local! {
    static GET_OBJECT_INFO_CONVERSIONS: Arc<AtomicU64>;
}

fn build_get_object_info(fi: &FileInfo, bucket: &str, object: &str, versioned: bool) -> ObjectInfo {
    #[cfg(test)]
    let _ = GET_OBJECT_INFO_CONVERSIONS.try_with(|conversions| {
        conversions.fetch_add(1, Ordering::Relaxed);
    });
    ObjectInfo::from_file_info(fi, bucket, object, versioned)
}

fn take_prepared_get_object_metadata() -> Option<PreparedGetObjectMetadata> {
    PREPARED_GET_OBJECT_METADATA
        .try_with(|prepared| prepared.borrow_mut().take())
        .ok()
        .flatten()
}

async fn with_prepared_get_object_metadata<F>(metadata: PreparedGetObjectMetadata, future: F) -> F::Output
where
    F: std::future::Future,
{
    PREPARED_GET_OBJECT_METADATA
        .scope(std::cell::RefCell::new(Some(metadata)), future)
        .await
}

#[cfg(test)]
mod prepared_get_object_metadata_tests {
    use super::*;
    use crate::ecstore_validation_blackbox::make_local_set_disks;
    use crate::object_api::{BLOCK_SIZE_V2, PutObjReader};
    use crate::set_disk::core::io_primitives::{bounded_metadata_fanout_order, disk_call_counters, rename_fanout_barrier};
    use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
    use crate::test_metrics::CapturingRecorder;
    use http::HeaderMap;
    use tokio::io::AsyncReadExt;

    const READ_VERSION_BARRIER_GUARD: std::time::Duration = std::time::Duration::from_secs(10);

    fn object_with_initial_data_shards(bucket: &str, prefix: &str) -> String {
        (0..1000)
            .map(|index| format!("{prefix}-{index}.bin"))
            .find(|name| {
                let order = bounded_metadata_fanout_order(bucket, name, 4, 2);
                let distribution = FileInfo::new(&[bucket, name].join("/"), 2, 2).erasure.distribution;
                let mut seen = [false; 2];
                for disk_index in order.into_iter().take(3) {
                    if let Some(block_index @ 1..=2) = distribution.get(disk_index).copied() {
                        seen[block_index - 1] = true;
                    }
                }
                seen.into_iter().all(|seen| seen)
            })
            .expect("test should find an object whose initial fanout covers both data shards")
    }

    fn bounded_initial_parity_disk_index(bucket: &str, object: &str) -> usize {
        *bounded_metadata_fanout_order(bucket, object, 4, 2)
            .get(2)
            .expect("4-disk test geometry should schedule one parity disk initially")
    }

    #[tokio::test]
    async fn prepared_metadata_is_consumed_exactly_once() {
        let snapshot = GetObjectFileInfo::owned(FileInfo::default(), Vec::new(), Vec::new());
        assert!(snapshot.has_valid_representation());
        assert!(snapshot.shared_entry().is_none());
        let metadata = PreparedGetObjectMetadata {
            snapshot,
            object_info: None,
        };

        with_prepared_get_object_metadata(metadata, async {
            assert!(take_prepared_get_object_metadata().is_some());
            assert!(take_prepared_get_object_metadata().is_none());
        })
        .await;
        assert!(take_prepared_get_object_metadata().is_none());
    }

    #[test]
    fn cache_hit_consumers_release_snapshot_at_legacy_ownership() {
        let fi = FileInfo {
            name: "object".to_owned(),
            ..Default::default()
        };
        let cached = Arc::new(GetObjectMetadataCacheEntry {
            created_at: Instant::now(),
            parts_metadata: vec![fi.clone()],
            fi,
            online_disks: vec![None],
            read_quorum: 0,
        });
        let snapshot = GetObjectFileInfo::shared(Arc::clone(&cached));

        assert!(snapshot.has_valid_representation());
        assert!(std::mem::size_of::<GetObjectFileInfo>() >= std::mem::size_of::<OwnedGetObjectFileInfo>());
        assert!(
            std::mem::size_of::<GetObjectFileInfo>()
                <= std::mem::size_of::<OwnedGetObjectFileInfo>() + 2 * std::mem::size_of::<usize>()
        );
        assert_eq!(Arc::strong_count(&cached), 2, "a cache hit must add one snapshot reference");
        assert_eq!(snapshot.fi().name, "object");
        assert_eq!(snapshot.parts_metadata().len(), 1);
        assert_eq!(snapshot.online_disks().len(), 1);
        assert_eq!(Arc::strong_count(&cached), 2, "borrowing consumers must not clone the snapshot");

        let (owned_fi, owned_parts, disks) = snapshot.into_owned();
        assert_eq!(owned_fi.name, "object");
        assert_eq!(owned_parts.len(), 1);
        assert_eq!(disks.len(), 1);
        assert_eq!(
            Arc::strong_count(&cached),
            1,
            "legacy ownership must release the cache snapshot after cloning its owned inputs"
        );
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn prepared_reader_reuses_metadata_fanout_exactly_once() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "prepared-metadata-fanout";
        let object = "prepared-metadata-fanout-object.bin";
        let payload = b"prepared-metadata-fanout-payload-".repeat(40_000);
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut put_reader = PutObjReader::from_vec(payload.clone());
        set_disks
            .put_object(bucket, object, &mut put_reader, &opts)
            .await
            .expect("object should be written");

        let calls = disk_call_counters::observe(object);
        let conversions = Arc::new(AtomicU64::new(0));
        let restored = GET_OBJECT_INFO_CONVERSIONS
            .scope(Arc::clone(&conversions), async {
                let metadata = set_disks
                    .prepare_get_object_metadata(bucket, object, &opts)
                    .await
                    .expect("prepared metadata should resolve");
                let prepared_calls = calls.total(disk_call_counters::KIND_READ_VERSION);
                assert_eq!(prepared_calls, 4, "default prepared GET metadata should keep full data-read fanout");

                let mut reader = set_disks
                    .get_object_reader_with_prepared_metadata(bucket, object, None, HeaderMap::new(), &opts, metadata)
                    .await
                    .expect("prepared body reader should open");
                let mut restored = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut restored)
                    .await
                    .expect("prepared body should stream");
                restored
            })
            .await;

        assert_eq!(restored, payload);
        assert_eq!(
            conversions.load(Ordering::Relaxed),
            1,
            "prepared reader must consume the ObjectInfo built during metadata preparation"
        );
        assert_eq!(
            calls.total(disk_call_counters::KIND_READ_VERSION),
            4,
            "reader construction must consume prepared metadata instead of repeating the fanout"
        );
    }

    #[test]
    #[serial_test::serial(body_cache_hook)]
    fn inline_data_read_early_stop_defaults_return_exact_body() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime should build");
        let bucket = "inline-data-read-early-stop-reader";
        let object = object_with_initial_data_shards(bucket, "inline-data-read-early-stop-reader-object");
        let payload = b"inline early-stop reader payload".repeat(256);
        let recorder = CapturingRecorder::default();
        let previous_gate = rustfs_io_metrics::get_stage_metrics_enabled();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);

        let (restored, object_size, calls_total) = metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
                let opts = ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                };

                set_disks
                    .make_bucket(bucket, &MakeBucketOptions::default())
                    .await
                    .expect("bucket should be created");
                let mut put_reader = PutObjReader::from_vec(payload.clone());
                set_disks
                    .put_object(bucket, &object, &mut put_reader, &opts)
                    .await
                    .expect("inline object should be written");

                temp_env::async_with_vars(
                    [
                        ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", None::<&str>),
                        ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", None::<&str>),
                        ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", None::<&str>),
                    ],
                    async {
                        let slow_parity_disk = bounded_initial_parity_disk_index(bucket, &object);
                        let barrier =
                            rename_fanout_barrier::arm(&object, slow_parity_disk, rename_fanout_barrier::PHASE_READ_VERSION);
                        let calls = disk_call_counters::observe(&object);
                        let set_disks_for_read = Arc::clone(&set_disks);
                        let opts_for_read = opts.clone();
                        let object_for_read = object.clone();
                        let mut open_reader = tokio::spawn(async move {
                            set_disks_for_read
                                .get_object_reader(bucket, &object_for_read, None, HeaderMap::new(), &opts_for_read)
                                .await
                        });

                        tokio::time::timeout(READ_VERSION_BARRIER_GUARD, barrier.wait_until_paused())
                            .await
                            .expect("default inline GET should pause a slow parity metadata read");
                        let mut reader = tokio::time::timeout(READ_VERSION_BARRIER_GUARD, &mut open_reader)
                            .await
                            .expect("default production inline GET should return before the paused parity metadata response")
                            .expect("inline GET reader task should not panic")
                            .expect("inline GET reader should open");
                        let object_size = reader.object_info.size;
                        let mut restored = Vec::new();
                        reader
                            .stream
                            .read_to_end(&mut restored)
                            .await
                            .expect("inline GET body should stream");

                        (restored, object_size, calls.total(disk_call_counters::KIND_READ_VERSION))
                    },
                )
                .await
            })
        });
        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_gate);

        assert_eq!(object_size, payload.len() as i64);
        assert_eq!(restored, payload);
        assert_eq!(
            calls_total, 4,
            "default production inline GET should schedule the initial bounded quorum plus one hedge"
        );
        assert_eq!(
            recorder.histogram_values(
                "rustfs_io_get_object_metadata_fanout_scheduled",
                &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)]
            ),
            vec![4.0],
            "default production GET should record all scheduled metadata tasks"
        );
        assert_eq!(
            recorder.histogram_values(
                "rustfs_io_get_object_metadata_fanout_completed",
                &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)]
            ),
            vec![3.0],
            "default production GET should record only observed metadata responses as completed"
        );
        assert_eq!(
            recorder.histogram_values(
                "rustfs_io_get_object_metadata_fanout_cancelled",
                &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)]
            ),
            vec![1.0],
            "default production GET should record the aborted slow parity metadata task"
        );
    }

    #[test]
    #[serial_test::serial(body_cache_hook)]
    fn prepared_metadata_uses_full_fanout_even_when_data_read_early_stop_is_enabled() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime should build");
        let bucket = "prepared-metadata-early-stop-enabled";
        let object = object_with_initial_data_shards(bucket, "prepared-metadata-early-stop-enabled-object");
        let payload = b"prepared metadata early-stop enabled payload".repeat(16);
        let recorder = CapturingRecorder::default();
        let previous_gate = rustfs_io_metrics::get_stage_metrics_enabled();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);

        let (restored, calls_total) = metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
                let opts = ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                };

                set_disks
                    .make_bucket(bucket, &MakeBucketOptions::default())
                    .await
                    .expect("bucket should be created");
                let mut put_reader = PutObjReader::from_vec(payload.clone());
                set_disks
                    .put_object(bucket, &object, &mut put_reader, &opts)
                    .await
                    .expect("object should be written");

                temp_env::async_with_vars(
                    [
                        ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                        ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", Some("true")),
                        ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", Some("true")),
                    ],
                    async {
                        let calls = disk_call_counters::observe(&object);
                        let metadata = set_disks
                            .prepare_get_object_metadata(bucket, &object, &opts)
                            .await
                            .expect("prepared metadata should resolve");
                        let calls_total = calls.total(disk_call_counters::KIND_READ_VERSION);

                        let mut reader = set_disks
                            .get_object_reader_with_prepared_metadata(bucket, &object, None, HeaderMap::new(), &opts, metadata)
                            .await
                            .expect("prepared body reader should open");
                        let mut restored = Vec::new();
                        reader
                            .stream
                            .read_to_end(&mut restored)
                            .await
                            .expect("prepared body should stream");
                        (restored, calls_total)
                    },
                )
                .await
            })
        });
        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_gate);

        assert_eq!(restored, payload);
        assert_eq!(
            calls_total, 4,
            "prepared metadata must opt out of data-read early-stop until the read shape is known"
        );
        assert_eq!(
            recorder.histogram_values(
                "rustfs_io_get_object_metadata_fanout_scheduled",
                &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)]
            ),
            vec![4.0],
            "prepared metadata should schedule the full metadata fanout"
        );
        assert_eq!(
            recorder.histogram_values(
                "rustfs_io_get_object_metadata_fanout_completed",
                &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)]
            ),
            vec![4.0],
            "prepared metadata must wait for every scheduled metadata response"
        );
        assert_eq!(
            recorder.histogram_values(
                "rustfs_io_get_object_metadata_fanout_cancelled",
                &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)]
            ),
            vec![0.0],
            "prepared metadata must not cancel metadata responses"
        );
    }

    #[test]
    #[serial_test::serial(body_cache_hook)]
    fn data_read_early_stop_request_shapes_full_wait_in_production_reader() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime should build");
        let bucket = "data-read-early-stop-shape-reader";
        let payload = b"shape-gated inline reader payload".repeat(256);

        for (object_prefix, range, configure_opts, expected_body) in [
            (
                "data-read-early-stop-range-reader-object",
                Some(HTTPRangeSpec {
                    start: 0,
                    end: 3,
                    is_suffix_length: false,
                }),
                None,
                payload[..4].to_vec(),
            ),
            ("data-read-early-stop-part-reader-object", None, Some(1), payload.clone()),
        ] {
            let recorder = CapturingRecorder::default();
            let previous_gate = rustfs_io_metrics::get_stage_metrics_enabled();
            rustfs_io_metrics::set_get_stage_metrics_enabled(true);
            let (restored, calls_total) = metrics::with_local_recorder(&recorder, || {
                runtime.block_on(async {
                    let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
                    let object = object_with_initial_data_shards(bucket, object_prefix);
                    let mut opts = ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    };
                    opts.part_number = configure_opts;

                    set_disks
                        .make_bucket(bucket, &MakeBucketOptions::default())
                        .await
                        .expect("bucket should be created");
                    let mut put_reader = PutObjReader::from_vec(payload.clone());
                    set_disks
                        .put_object(bucket, &object, &mut put_reader, &opts)
                        .await
                        .expect("inline object should be written");

                    temp_env::async_with_vars(
                        [
                            ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", None::<&str>),
                            ("RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE", None::<&str>),
                            ("RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT", None::<&str>),
                        ],
                        async {
                            let calls = disk_call_counters::observe(&object);
                            let mut reader = set_disks
                                .get_object_reader(bucket, &object, range, HeaderMap::new(), &opts)
                                .await
                                .expect("shape-gated GET reader should open");
                            let mut restored = Vec::new();
                            reader
                                .stream
                                .read_to_end(&mut restored)
                                .await
                                .expect("shape-gated GET body should stream");
                            (restored, calls.total(disk_call_counters::KIND_READ_VERSION))
                        },
                    )
                    .await
                })
            });
            rustfs_io_metrics::set_get_stage_metrics_enabled(previous_gate);

            assert_eq!(restored, expected_body);
            assert_eq!(calls_total, 4, "shape-gated production GET should keep full metadata fanout");
            assert_eq!(
                recorder.histogram_values(
                    "rustfs_io_get_object_metadata_fanout_scheduled",
                    &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)]
                ),
                vec![4.0],
                "shape-gated production GET should schedule the full metadata fanout"
            );
            assert_eq!(
                recorder.histogram_values(
                    "rustfs_io_get_object_metadata_fanout_completed",
                    &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)]
                ),
                vec![4.0],
                "shape-gated production GET must wait for every scheduled metadata response"
            );
            assert_eq!(
                recorder.histogram_values(
                    "rustfs_io_get_object_metadata_fanout_cancelled",
                    &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)]
                ),
                vec![0.0],
                "shape-gated production GET must not cancel metadata responses"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn prepared_reader_rebuilds_object_info_when_precomputed_value_is_absent() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "prepared-object-info-fallback";
        let object = "prepared-object-info-fallback.bin";
        let payload = b"prepared-object-info-fallback-payload".repeat(4_000);
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut put_reader = PutObjReader::from_vec(payload.clone());
        set_disks
            .put_object(bucket, object, &mut put_reader, &opts)
            .await
            .expect("object should be written");

        let conversions = Arc::new(AtomicU64::new(0));
        let restored = GET_OBJECT_INFO_CONVERSIONS
            .scope(Arc::clone(&conversions), async {
                let mut metadata = set_disks
                    .prepare_get_object_metadata(bucket, object, &opts)
                    .await
                    .expect("prepared metadata should resolve");
                metadata.object_info = None;
                let mut reader = set_disks
                    .get_object_reader_with_prepared_metadata(bucket, object, None, HeaderMap::new(), &opts, metadata)
                    .await
                    .expect("reader should rebuild missing prepared ObjectInfo");
                let mut restored = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut restored)
                    .await
                    .expect("fallback reader should stream");
                restored
            })
            .await;

        assert_eq!(restored, payload);
        assert_eq!(
            conversions.load(Ordering::Relaxed),
            2,
            "missing precomputed ObjectInfo must trigger exactly one structural fallback rebuild"
        );
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn prepared_reader_restores_full_body_with_one_offline_shard() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "prepared-reader-offline-shard";
        let object = "prepared-reader-offline-shard-object.bin";
        let payload = (0..(BLOCK_SIZE_V2 + 321))
            .map(|idx| ((idx * 19) % 251) as u8)
            .collect::<Vec<_>>();
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut put_reader = PutObjReader::from_vec(payload.clone());
        set_disks
            .put_object(bucket, object, &mut put_reader, &opts)
            .await
            .expect("object should be written");
        set_disks.disks.write().await[0] = None;

        let metadata = set_disks
            .prepare_get_object_metadata(bucket, object, &opts)
            .await
            .expect("prepared metadata should tolerate one offline shard");
        let mut reader = set_disks
            .get_object_reader_with_prepared_metadata(bucket, object, None, HeaderMap::new(), &opts, metadata)
            .await
            .expect("prepared body reader should open with one offline shard");
        let mut restored = Vec::new();
        reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("degraded prepared body should stream");

        assert_eq!(restored, payload);
    }
}

impl SetDisks {
    pub(crate) async fn prepare_get_object_metadata(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<PreparedGetObjectMetadata> {
        let snapshot = self.get_object_fileinfo(bucket, object, opts, true, false).await?;
        let object_info = build_get_object_info(snapshot.fi(), bucket, object, opts.versioned || opts.version_suspended);
        Ok(PreparedGetObjectMetadata {
            snapshot,
            object_info: Some(object_info),
        })
    }

    pub(crate) async fn get_object_reader_with_prepared_metadata(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        headers: HeaderMap,
        opts: &ObjectOptions,
        metadata: PreparedGetObjectMetadata,
    ) -> Result<GetObjectReader> {
        with_prepared_get_object_metadata(metadata, self.get_object_reader(bucket, object, range, headers, opts)).await
    }
}

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

fn get_put_object_commit_lock_acquire_timeout_override_ms() -> u64 {
    #[cfg(test)]
    {
        rustfs_utils::get_env_u64(
            rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS,
            rustfs_config::DEFAULT_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<u64> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_u64(
                rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS,
                rustfs_config::DEFAULT_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS,
            )
        })
    }
}

fn get_put_object_commit_lock_acquire_timeout(op: &'static str) -> Duration {
    let default_timeout = get_lock_acquire_timeout();
    if op != "put_object_commit" {
        return default_timeout;
    }

    let timeout_ms = get_put_object_commit_lock_acquire_timeout_override_ms();
    if timeout_ms == 0 {
        default_timeout
    } else {
        Duration::from_millis(timeout_ms)
    }
}

fn put_object_commit_lock_timeout_override_enabled(op: &'static str) -> bool {
    op == "put_object_commit" && get_put_object_commit_lock_acquire_timeout_override_ms() != 0
}

fn put_object_commit_lock_admission_budget_label() -> &'static str {
    match get_put_object_commit_lock_acquire_timeout_override_ms() {
        0 => rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_DISABLED,
        1..=250 => rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_250MS,
        251..=500 => rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_500MS,
        501..=1000 => rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_1000MS,
        _ => rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_GT_1000MS,
    }
}

fn record_put_object_commit_lock_admission(op: &'static str, outcome: &'static str) {
    if op != "put_object_commit" || !rustfs_io_metrics::put_stage_metrics_enabled() {
        return;
    }
    rustfs_io_metrics::record_put_object_commit_lock_admission(put_object_commit_lock_admission_budget_label(), outcome);
}

fn put_object_commit_lock_acquire_error_outcome(op: &'static str, err: &rustfs_lock::error::LockError) -> &'static str {
    if put_object_commit_lock_timeout_override_enabled(op) && matches!(err, rustfs_lock::error::LockError::Timeout { .. }) {
        rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_TIMEOUT_SLOWDOWN
    } else {
        rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_LOCK_ERROR
    }
}

fn resolve_put_object_commit_lock_acquire_result(
    set: &SetDisks,
    op: &'static str,
    bucket: &str,
    object: &str,
    result: std::result::Result<rustfs_lock::namespace::NamespaceLockGuard, rustfs_lock::error::LockError>,
) -> Result<rustfs_lock::namespace::NamespaceLockGuard> {
    match result {
        Ok(guard) => {
            record_put_object_commit_lock_admission(op, rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_ACQUIRED);
            Ok(guard)
        }
        Err(err) => {
            record_put_object_commit_lock_admission(op, put_object_commit_lock_acquire_error_outcome(op, &err));
            Err(map_put_object_commit_lock_acquire_error(set, op, bucket, object, err))
        }
    }
}

fn map_put_object_commit_lock_acquire_error(
    set: &SetDisks,
    op: &'static str,
    bucket: &str,
    object: &str,
    err: rustfs_lock::error::LockError,
) -> StorageError {
    if put_object_commit_lock_timeout_override_enabled(op) && matches!(err, rustfs_lock::error::LockError::Timeout { .. }) {
        StorageError::SlowDown
    } else {
        set.map_namespace_lock_error(bucket, object, "write", err)
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
/// Fully materialized reads release the read lock before returning. Streaming
/// reads retain it until the response body completes or is dropped.
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

fn is_get_metadata_data_read_early_stop_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(
            ENV_RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE,
            DEFAULT_RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<bool> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(
                ENV_RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE,
                DEFAULT_RUSTFS_GET_METADATA_DATA_READ_EARLY_STOP_ENABLE,
            )
        })
    }
}

fn is_get_metadata_early_stop_bounded_fanout_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(
            ENV_RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT,
            DEFAULT_RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<bool> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(
                ENV_RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT,
                DEFAULT_RUSTFS_GET_METADATA_EARLY_STOP_BOUNDED_FANOUT,
            )
        })
    }
}

#[derive(Debug)]
struct GetMetadataSlowtailFaultConfig {
    delay: Duration,
    disks: Arc<[usize]>,
    bucket: Option<String>,
    object_prefix: Option<String>,
}

#[derive(Clone, Debug)]
struct GetMetadataSlowtailFaultRequest {
    delay: Duration,
    disks: Arc<[usize]>,
}

impl GetMetadataSlowtailFaultRequest {
    fn delay_for_disk(&self, disk_index: usize) -> Option<Duration> {
        self.disks.contains(&disk_index).then_some(self.delay)
    }
}

fn parse_get_metadata_slowtail_fault_disks(raw: &str) -> Option<Vec<usize>> {
    let mut disks = Vec::new();
    for item in raw.split(',').map(str::trim).filter(|item| !item.is_empty()) {
        let Ok(index) = item.parse::<usize>() else {
            return None;
        };
        if !disks.contains(&index) {
            disks.push(index);
        }
    }
    (!disks.is_empty()).then_some(disks)
}

fn load_get_metadata_slowtail_fault_config() -> Option<GetMetadataSlowtailFaultConfig> {
    let delay_ms = rustfs_utils::get_env_u64(ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS, 0);
    if delay_ms == 0 {
        return None;
    }
    let disks = parse_get_metadata_slowtail_fault_disks(&std::env::var(ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DISKS).ok()?)?;
    let bucket = std::env::var(ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_BUCKET)
        .ok()
        .filter(|value| !value.is_empty());
    let object_prefix = std::env::var(ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_OBJECT_PREFIX)
        .ok()
        .filter(|value| !value.is_empty());
    Some(GetMetadataSlowtailFaultConfig {
        delay: Duration::from_millis(delay_ms),
        disks: Arc::from(disks.into_boxed_slice()),
        bucket,
        object_prefix,
    })
}

fn get_metadata_slowtail_fault_request(bucket: &str, object: &str, read_data: bool) -> Option<GetMetadataSlowtailFaultRequest> {
    if !read_data {
        return None;
    }

    #[cfg(test)]
    let config = load_get_metadata_slowtail_fault_config();
    #[cfg(test)]
    let config = config.as_ref()?;
    #[cfg(not(test))]
    let config = ({
        static CACHED: OnceLock<Option<GetMetadataSlowtailFaultConfig>> = OnceLock::new();
        CACHED.get_or_init(load_get_metadata_slowtail_fault_config).as_ref()
    })?;

    if let Some(expected_bucket) = &config.bucket
        && expected_bucket != bucket
    {
        return None;
    }
    if let Some(expected_prefix) = &config.object_prefix
        && !object.starts_with(expected_prefix)
    {
        return None;
    }
    Some(GetMetadataSlowtailFaultRequest {
        delay: config.delay,
        disks: config.disks.clone(),
    })
}

#[cfg(test)]
fn get_metadata_slowtail_fault_delay(bucket: &str, object: &str, disk_index: usize, read_data: bool) -> Option<Duration> {
    get_metadata_slowtail_fault_request(bucket, object, read_data)?.delay_for_disk(disk_index)
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

#[allow(
    dead_code,
    reason = "percentage-rollout facet of the metadata early-stop switch; its predicate has no caller while the sibling enable flag is live (backlog#1823)"
)]
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
fn should_use_codec_streaming(config: GetCodecStreamingConfig, bucket: &str, object: &str) -> bool {
    is_optimization_enabled_for_request(config.enabled, config.rollout_pct, bucket, object)
}

/// Should this specific request use metadata early-stop?
#[allow(
    dead_code,
    reason = "percentage-rollout facet of the metadata early-stop switch; its predicate has no caller while the sibling enable flag is live (backlog#1823)"
)]
pub fn should_use_metadata_early_stop(bucket: &str, object: &str) -> bool {
    let base = is_get_metadata_early_stop_enabled();
    let pct = get_metadata_early_stop_rollout_pct();
    is_optimization_enabled_for_request(base, pct, bucket, object)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GetCodecStreamingConfig {
    enabled: bool,
    rollout: GetCodecStreamingRollout,
    rollout_pct: u32,
    body_compat_confirmed: bool,
    header_compat_confirmed: bool,
    engine: GetCodecStreamingEngine,
    min_size: usize,
}

fn parse_get_codec_streaming_engine(engine: &str) -> GetCodecStreamingEngine {
    match engine.trim() {
        value if value.eq_ignore_ascii_case(GET_CODEC_STREAMING_ENGINE_RUSTFS) => GetCodecStreamingEngine::Rustfs,
        value if value.eq_ignore_ascii_case(GET_CODEC_STREAMING_ENGINE_LEGACY) => GetCodecStreamingEngine::Legacy,
        _ => GetCodecStreamingEngine::Legacy,
    }
}

fn parse_get_codec_streaming_rollout(rollout: &str) -> GetCodecStreamingRollout {
    match rollout.trim() {
        // Clean production token. `internal`/`benchmark` remain accepted aliases
        // for backward compatibility; all three opt the fast path in.
        value
            if value.eq_ignore_ascii_case("on")
                || value.eq_ignore_ascii_case("full")
                || value.eq_ignore_ascii_case("production") =>
        {
            GetCodecStreamingRollout::On
        }
        value if value.eq_ignore_ascii_case("internal") => GetCodecStreamingRollout::Internal,
        value if value.eq_ignore_ascii_case("benchmark") => GetCodecStreamingRollout::Benchmark,
        _ => GetCodecStreamingRollout::Off,
    }
}

fn load_get_codec_streaming_config() -> GetCodecStreamingConfig {
    let engine = parse_get_codec_streaming_engine(&rustfs_utils::get_env_str(
        ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE,
        DEFAULT_RUSTFS_GET_CODEC_STREAMING_ENGINE,
    ));
    let min_size = if std::env::var_os(ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE).is_some() {
        rustfs_utils::get_env_usize(ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, DEFAULT_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE)
    } else {
        match engine {
            GetCodecStreamingEngine::Rustfs => rustfs_utils::get_env_usize(
                ENV_RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE,
                DEFAULT_RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE,
            ),
            GetCodecStreamingEngine::Legacy => DEFAULT_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE,
        }
    };

    GetCodecStreamingConfig {
        enabled: rustfs_utils::get_env_bool(ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, DEFAULT_RUSTFS_GET_CODEC_STREAMING_ENABLE),
        rollout: parse_get_codec_streaming_rollout(&rustfs_utils::get_env_str(
            ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT,
            DEFAULT_RUSTFS_GET_CODEC_STREAMING_ROLLOUT,
        )),
        rollout_pct: rustfs_utils::get_env_u32(
            ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT,
            DEFAULT_RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT,
        ),
        body_compat_confirmed: rustfs_utils::get_env_bool(ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, true),
        header_compat_confirmed: rustfs_utils::get_env_bool(ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, true),
        engine,
        min_size,
    }
}

fn get_codec_streaming_config_cached_core(load: impl FnOnce() -> GetCodecStreamingConfig) -> GetCodecStreamingConfig {
    static CACHED: OnceLock<GetCodecStreamingConfig> = OnceLock::new();
    *CACHED.get_or_init(load)
}

fn get_codec_streaming_config() -> GetCodecStreamingConfig {
    #[cfg(test)]
    {
        load_get_codec_streaming_config()
    }
    #[cfg(not(test))]
    {
        get_codec_streaming_config_cached_core(load_get_codec_streaming_config)
    }
}

fn get_codec_streaming_engine() -> GetCodecStreamingEngine {
    get_codec_streaming_config().engine
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
    /// Default. Codec streaming disabled; GET uses the legacy duplex path.
    Off,
    /// Production enablement token (`on`/`full`/`production`).
    On,
    /// Backward-compatible aliases that also opt the fast path in.
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
    PartNumber,
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
            Self::PartNumber => "part_number",
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
    if object_info.is_compressed() || fi.is_compressed() {
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

#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
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
    // Bucket-level versioning no longer blocks the inline path (rustfs/backlog#1802):
    // `fi` here is the already-resolved target version, so reassembling its inlined
    // data shards is correct whether the bucket is versioned or not. This direct-memory
    // decision still falls back for an explicit versionId (the `version_id` check above);
    // a delete-marker latest is rejected below.
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
    part_number: Option<usize>,
    object_class: GetCodecStreamingObjectClass,
    object_info: &ObjectInfo,
    fi: &FileInfo,
    lock_optimization_enabled: bool,
) -> GetCodecStreamingGate {
    let config = get_codec_streaming_config();

    if !config.enabled {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Disabled),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if !config.rollout.is_opted_in() {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::RolloutNotOptedIn),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if !should_use_codec_streaming(config, bucket, object) {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::RolloutPctNotSelected),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if !config.body_compat_confirmed {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::BodyCompatibilityUnconfirmed),
            prefer_data_blocks_first_reader_setup: false,
        };
    }
    if !config.header_compat_confirmed {
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
    // A partNumber GET arrives with `range == None`, so it is not caught by the
    // Range class above, yet it still requires a non-zero storage offset/length
    // (synthesized from the part size). The codec-streaming path builds a
    // full-object reader and drops the offset/length returned by
    // `GetObjectReader::new`, so partNumber >= 2 would stream the whole object.
    // Mirror the direct-memory part_number fallback and route these requests back
    // to the legacy duplex path, which applies the offset/length correctly.
    if part_number.is_some() {
        return GetCodecStreamingGate {
            object_class,
            decision: GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::PartNumber),
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
    let Ok(min_size) = i64::try_from(config.min_size) else {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct WriteLayout {
    data_drives: usize,
    parity_drives: usize,
    write_quorum: usize,
}

impl WriteLayout {
    fn from_parity(drive_count: usize, parity_drives: usize) -> Result<Self> {
        let max_parity = drive_count / 2;
        if parity_drives > max_parity {
            return Err(Error::other(format!(
                "write parity {parity_drives} exceeds the maximum {max_parity} for {drive_count} drives"
            )));
        }

        let data_drives = drive_count
            .checked_sub(parity_drives)
            .filter(|&data_drives| data_drives > 0 && parity_drives <= data_drives)
            .ok_or_else(|| Error::other(format!("invalid write layout with {drive_count} drives and parity {parity_drives}")))?;
        let write_quorum = data_drives
            .checked_add(usize::from(data_drives == parity_drives))
            .filter(|&write_quorum| write_quorum <= drive_count)
            .ok_or_else(|| Error::other(format!("invalid write quorum for {drive_count} drives and parity {parity_drives}")))?;

        Ok(Self {
            data_drives,
            parity_drives,
            write_quorum,
        })
    }
}

pub(super) fn resolve_write_layout(
    config: &storageclass::Config,
    pool_index: usize,
    drive_count: usize,
    fallback_parity: usize,
    storage_class: Option<&str>,
    max_parity: bool,
) -> Result<WriteLayout> {
    let configured_parity = if config.is_initialized() {
        config
            .parity_for_pool(storage_class.unwrap_or_default(), pool_index, drive_count)
            .ok_or_else(|| {
                Error::other(format!("storage class layout does not match pool {pool_index} with {drive_count} drives"))
            })?
    } else {
        fallback_parity
    };
    let parity_drives = if max_parity { drive_count / 2 } else { configured_parity };

    WriteLayout::from_parity(drive_count, parity_drives)
}

#[cfg(test)]
mod write_layout_tests {
    use super::{WriteLayout, resolve_write_layout};
    use crate::config::storageclass::{
        CLASS_RRS, CLASS_STANDARD, INLINE_BLOCK_ENV, OPTIMIZE_ENV, RRS, RRS_ENV, STANDARD_ENV, lookup_config_for_pools,
        lookup_config_for_pools_without_env,
    };
    use arc_swap::ArcSwap;
    use rustfs_config::server_config::KVS;
    use std::sync::Arc;

    #[test]
    fn automatic_standard_layout_is_resolved_per_pool() {
        let config = lookup_config_for_pools_without_env(&KVS::new(), &[4, 2])
            .expect("automatic storage class should resolve for both pools");

        assert_eq!(
            resolve_write_layout(&config, 0, 4, 2, None, false).expect("first pool should resolve"),
            WriteLayout {
                data_drives: 2,
                parity_drives: 2,
                write_quorum: 3,
            }
        );
        assert_eq!(
            resolve_write_layout(&config, 1, 2, 1, None, false).expect("second pool should resolve"),
            WriteLayout {
                data_drives: 1,
                parity_drives: 1,
                write_quorum: 2,
            }
        );
    }

    #[test]
    fn reduced_redundancy_layout_allows_single_disk_zero_parity() {
        let config =
            lookup_config_for_pools_without_env(&KVS::new(), &[4, 1]).expect("reduced redundancy should resolve for both pools");

        assert_eq!(
            resolve_write_layout(&config, 0, 4, 2, Some(RRS), false).expect("four-drive RRS pool should resolve"),
            WriteLayout {
                data_drives: 3,
                parity_drives: 1,
                write_quorum: 3,
            }
        );
        assert_eq!(
            resolve_write_layout(&config, 1, 1, 0, Some(RRS), false).expect("single-drive RRS pool should resolve"),
            WriteLayout {
                data_drives: 1,
                parity_drives: 0,
                write_quorum: 1,
            }
        );
    }

    #[test]
    fn write_layout_rejects_unknown_topology_and_invalid_parity() {
        let config = lookup_config_for_pools_without_env(&KVS::new(), &[4, 2]).expect("test storage class should resolve");

        assert!(resolve_write_layout(&config, 2, 2, 1, None, false).is_err());
        assert!(resolve_write_layout(&config, 1, 4, 2, None, false).is_err());
        assert!(WriteLayout::from_parity(4, 3).is_err());
        assert!(WriteLayout::from_parity(0, 0).is_err());

        let mut zero_parity_kvs = KVS::new();
        zero_parity_kvs.insert(CLASS_STANDARD.to_string(), "EC:0".to_string());
        zero_parity_kvs.insert(CLASS_RRS.to_string(), "EC:0".to_string());
        let zero_parity = lookup_config_for_pools_without_env(&zero_parity_kvs, &[4]).expect("zero-parity config should resolve");
        assert_eq!(
            resolve_write_layout(&zero_parity, 0, 4, 2, None, true).expect("max parity should override configured parity"),
            WriteLayout {
                data_drives: 2,
                parity_drives: 2,
                write_quorum: 3,
            }
        );
    }

    #[test]
    fn only_uninitialized_config_falls_back_to_pool_startup_parity() {
        let uninitialized = crate::config::storageclass::Config::default();
        assert_eq!(
            resolve_write_layout(&uninitialized, 99, 2, 0, None, false)
                .expect("uninitialized config should preserve the pool's startup fallback"),
            WriteLayout {
                data_drives: 2,
                parity_drives: 0,
                write_quorum: 2,
            }
        );

        let initialized = lookup_config_for_pools_without_env(&KVS::new(), &[4, 2]).expect("initialized config should resolve");
        assert!(resolve_write_layout(&initialized, 99, 2, 1, None, false).is_err());
        assert!(resolve_write_layout(&initialized, 1, 4, 2, None, false).is_err());
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn held_snapshot_keeps_parity_and_inline_policy_consistent_across_reload() {
        let old = temp_env::with_vars(
            [
                (STANDARD_ENV, Some("")),
                (RRS_ENV, Some("")),
                (OPTIMIZE_ENV, None),
                (INLINE_BLOCK_ENV, Some("1KiB")),
            ],
            || lookup_config_for_pools(&KVS::new(), &[4, 2]),
        )
        .expect("old config should resolve");
        let new = temp_env::with_vars(
            [
                (STANDARD_ENV, Some("EC:1")),
                (RRS_ENV, Some("EC:1")),
                (OPTIMIZE_ENV, None),
                (INLINE_BLOCK_ENV, Some("0B")),
            ],
            || lookup_config_for_pools(&KVS::new(), &[4, 2]),
        )
        .expect("new config should resolve");

        let published = ArcSwap::from_pointee(old);
        let held = published.load_full();
        published.store(Arc::new(new));

        let held_layout = resolve_write_layout(&held, 0, 4, 2, None, false).expect("held snapshot should remain valid");
        assert_eq!(held_layout.parity_drives, 2);
        assert!(held.should_inline(512, held_layout.data_drives, false));

        let current = published.load_full();
        let current_layout = resolve_write_layout(&current, 0, 4, 2, None, false).expect("new snapshot should resolve");
        assert_eq!(current_layout.parity_drives, 1);
        assert!(!current.should_inline(512, current_layout.data_drives, false));
    }
}

fn build_tiered_decommission_file_info(bucket: &str, object: &str, fi: &FileInfo, layout: WriteLayout) -> FileInfo {
    let WriteLayout {
        data_drives,
        parity_drives,
        ..
    } = layout;

    let mut updated = fi.clone();
    updated.erasure = FileInfo::new([bucket, object].join("/").as_str(), data_drives, parity_drives).erasure;

    updated
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
    /// Stable namespace shared by every object lock created for this set.
    set_lock_namespace: Arc<str>,
    pub format: FormatV3,
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    disk_health_cache: Arc<RwLock<Vec<Option<DiskHealthEntry>>>>,
    get_object_metadata_cache: moka::future::Cache<GetObjectMetadataCacheKey, Arc<GetObjectMetadataCacheEntry>>,
    get_object_metadata_cache_hash_builder: std::collections::hash_map::RandomState,
    get_object_metadata_cache_generations: Arc<[AtomicU64]>,
    /// GET codecs keyed by every persisted layout dimension that affects
    /// decoding. Clones of a set share the memoized shells.
    erasure_cache: Arc<ErasureCache>,
    pub lockers: Vec<Arc<dyn LockClient>>,
    shared_lockers: Arc<[Arc<dyn LockClient>]>,
    local_lock_manager: Arc<rustfs_lock::GlobalLockManager>,
    /// Per-instance runtime context (Phase 5, backlog#939).
    ///
    /// The leaf of the object-graph ctx plumbing (ECStore → Sets → SetDisks).
    /// Slice 3 sources `local_lock_manager` from this context to give each
    /// instance its own lock namespace.
    ctx: Arc<InstanceContext>,
    /// Memoized capacity dirty scope so successful writes reuse a prebuilt
    /// `Arc` instead of allocating a `String` per online disk (backlog#1315).
    /// `Arc` so clones of a set share one memo.
    capacity_scope_cache: Arc<std::sync::RwLock<CapacityScopeCache>>,
    /// Last global dirty-registry generation this set marked. `u64::MAX` until
    /// the first write; equality with the current generation lets steady-state
    /// writes skip the global registry mutex (backlog#1315). `Arc` so clones of
    /// a set share one generation marker.
    capacity_dirty_generation: Arc<AtomicU64>,
    #[cfg(test)]
    storage_class_config_override: Arc<std::sync::RwLock<Option<Arc<storageclass::Config>>>>,
}

// DistributedLock sends the raw ObjectKey to its clients; LockRegistry clones
// each endpoint's canonical Arc, so an exact Arc set identifies the lock domain.
pub(crate) fn same_distributed_lock_domain(left: &[Arc<dyn LockClient>], right: &[Arc<dyn LockClient>]) -> bool {
    left.iter()
        .all(|left_client| right.iter().any(|right_client| Arc::ptr_eq(left_client, right_client)))
        && right
            .iter()
            .all(|right_client| left.iter().any(|left_client| Arc::ptr_eq(left_client, right_client)))
}

const ERASURE_CACHE_MAX_ENTRIES: usize = 32;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ErasureCacheKey {
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
    uses_legacy: bool,
}

struct ErasureCache {
    entries: parking_lot::RwLock<HashMap<ErasureCacheKey, Arc<coding::Erasure>>>,
}

impl std::fmt::Debug for ErasureCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ErasureCache")
            .field("entries", &self.entries.read().len())
            .finish()
    }
}

impl ErasureCache {
    fn new() -> Self {
        Self {
            entries: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    fn get_or_try_insert(
        &self,
        key: ErasureCacheKey,
    ) -> std::result::Result<Arc<coding::Erasure>, coding::ErasureConstructionError> {
        if let Some(erasure) = self.entries.read().get(&key) {
            return Ok(Arc::clone(erasure));
        }

        // Serialize first construction for a key so concurrent cold GETs still
        // create exactly one shell. Codec construction never awaits.
        let mut entries = self.entries.write();
        if let Some(erasure) = entries.get(&key) {
            return Ok(Arc::clone(erasure));
        }
        let erasure = Arc::new(coding::Erasure::try_new_with_options(
            key.data_shards,
            key.parity_shards,
            key.block_size,
            key.uses_legacy,
        )?);
        if entries.len() < ERASURE_CACHE_MAX_ENTRIES {
            entries.insert(key, Arc::clone(&erasure));
        }
        Ok(erasure)
    }

    fn get_for_file_info(&self, fi: &FileInfo) -> Result<Arc<coding::Erasure>> {
        self.get_or_try_insert(ErasureCacheKey {
            data_shards: fi.erasure.data_blocks,
            parity_shards: fi.erasure.parity_blocks,
            block_size: fi.erasure.block_size,
            uses_legacy: fi.uses_legacy_checksum,
        })
        .map_err(Error::from)
    }
}

#[cfg(test)]
mod erasure_cache_tests {
    use super::*;

    #[test]
    fn reuses_shells_and_keeps_every_layout_dimension_in_the_key() {
        let cache = ErasureCache::new();
        let base = ErasureCacheKey {
            data_shards: 4,
            parity_shards: 2,
            block_size: 1_048_576,
            uses_legacy: false,
        };
        let first = cache.get_or_try_insert(base).expect("modern shell should construct");
        let reused = cache.get_or_try_insert(base).expect("same modern shell should be cached");
        assert!(Arc::ptr_eq(&first, &reused));

        for distinct in [
            ErasureCacheKey { data_shards: 3, ..base },
            ErasureCacheKey {
                parity_shards: 1,
                ..base
            },
            ErasureCacheKey {
                block_size: 524_288,
                ..base
            },
            ErasureCacheKey {
                uses_legacy: true,
                ..base
            },
        ] {
            let shell = cache.get_or_try_insert(distinct).expect("distinct shell should construct");
            assert!(!Arc::ptr_eq(&first, &shell));
        }
        assert_eq!(cache.entries.read().len(), 5);
    }

    #[test]
    fn does_not_cache_invalid_layouts_or_grow_past_the_bound() {
        let cache = ErasureCache::new();
        let invalid = ErasureCacheKey {
            data_shards: 4,
            parity_shards: 2,
            block_size: 0,
            uses_legacy: false,
        };
        assert!(cache.get_or_try_insert(invalid).is_err());
        assert!(cache.entries.read().is_empty());

        for block_size in 1..=(ERASURE_CACHE_MAX_ENTRIES + 1) {
            cache
                .get_or_try_insert(ErasureCacheKey {
                    data_shards: 4,
                    parity_shards: 2,
                    block_size,
                    uses_legacy: false,
                })
                .expect("bounded cache fixture should construct");
        }
        assert_eq!(cache.entries.read().len(), ERASURE_CACHE_MAX_ENTRIES);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GetObjectMetadataCacheKey {
    bucket: Arc<str>,
    object: Arc<str>,
    generation: u64,
    hash: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GetObjectMetadataCacheGeneration {
    index: usize,
    value: u64,
    hash: u64,
}

#[cfg(test)]
struct MetadataCacheInvalidationProbeState {
    bucket: String,
    object: String,
    count: AtomicU64,
}

#[cfg(test)]
struct MetadataCacheInvalidationProbe {
    state: Arc<MetadataCacheInvalidationProbeState>,
}

#[cfg(test)]
static METADATA_CACHE_INVALIDATION_PROBE: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<MetadataCacheInvalidationProbeState>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
impl MetadataCacheInvalidationProbe {
    fn install(bucket: &str, object: &str) -> Self {
        let state = Arc::new(MetadataCacheInvalidationProbeState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            count: AtomicU64::new(0),
        });
        let mut slot = METADATA_CACHE_INVALIDATION_PROBE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("metadata cache invalidation probe mutex should not poison");
        assert!(
            slot.is_none(),
            "metadata cache invalidation probe must be installed by one test at a time"
        );
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    fn count(&self) -> u64 {
        self.state.count.load(Ordering::Acquire)
    }
}

#[cfg(test)]
impl Drop for MetadataCacheInvalidationProbe {
    fn drop(&mut self) {
        let mut slot = METADATA_CACHE_INVALIDATION_PROBE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("metadata cache invalidation probe mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
fn record_metadata_cache_invalidation(bucket: &str, object: &str) {
    let probe = METADATA_CACHE_INVALIDATION_PROBE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("metadata cache invalidation probe mutex should not poison")
        .as_ref()
        .filter(|probe| probe.bucket == bucket && probe.object == object)
        .cloned();
    if let Some(probe) = probe {
        probe.count.fetch_add(1, Ordering::AcqRel);
    }
}

impl GetObjectMetadataCacheKey {
    fn new(bucket: &str, object: &str, generation: GetObjectMetadataCacheGeneration) -> Self {
        Self {
            bucket: Arc::from(bucket),
            object: Arc::from(object),
            generation: generation.value,
            hash: generation.hash,
        }
    }
}

impl Hash for GetObjectMetadataCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
        self.generation.hash(state);
    }
}

#[derive(Debug)]
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
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    last_check: Instant,
    online: bool,
}

impl DiskHealthEntry {
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    fn cached_value(&self) -> Option<bool> {
        if self.last_check.elapsed() <= DISK_HEALTH_CACHE_TTL {
            Some(self.online)
        } else {
            None
        }
    }
}

impl SetDisks {
    fn storage_class_config_snapshot(&self) -> Arc<storageclass::Config> {
        #[cfg(test)]
        if let Some(config) = self
            .storage_class_config_override
            .read()
            .expect("test storage class override lock should not be poisoned")
            .as_ref()
        {
            return config.clone();
        }

        runtime_sources::storage_class_config_snapshot()
    }

    #[cfg(test)]
    pub(crate) fn set_test_storage_class_config(&self, config: storageclass::Config) {
        *self
            .storage_class_config_override
            .write()
            .expect("test storage class override lock should not be poisoned") = Some(Arc::new(config));
    }

    fn get_object_metadata_cache_hash(&self, bucket: &str, object: &str) -> u64 {
        let mut hasher = self.get_object_metadata_cache_hash_builder.build_hasher();
        bucket.hash(&mut hasher);
        object.hash(&mut hasher);
        hasher.finish()
    }

    fn get_object_metadata_cache_generation(&self, bucket: &str, object: &str) -> Option<GetObjectMetadataCacheGeneration> {
        let hash = self.get_object_metadata_cache_hash(bucket, object);
        let hash_bytes = hash.to_le_bytes();
        let index = usize::from(u16::from_le_bytes([hash_bytes[0], hash_bytes[1]]) % GET_OBJECT_METADATA_CACHE_FENCE_SHARDS);
        let value = self.get_object_metadata_cache_generations[index].load(Ordering::Acquire);
        (value != u64::MAX).then_some(GetObjectMetadataCacheGeneration { index, value, hash })
    }

    fn is_get_object_metadata_cache_generation_current(&self, generation: GetObjectMetadataCacheGeneration) -> bool {
        self.get_object_metadata_cache_generations[generation.index].load(Ordering::Acquire) == generation.value
    }

    async fn invalidate_get_object_metadata_cache(&self, bucket: &str, object: &str) {
        let hash = self.get_object_metadata_cache_hash(bucket, object);
        let hash_bytes = hash.to_le_bytes();
        let index = usize::from(u16::from_le_bytes([hash_bytes[0], hash_bytes[1]]) % GET_OBJECT_METADATA_CACHE_FENCE_SHARDS);
        let generation = &self.get_object_metadata_cache_generations[index];
        let previous = match generation.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| current.checked_add(1)) {
            Ok(previous) | Err(previous) => previous,
        };
        let previous = GetObjectMetadataCacheGeneration {
            index,
            value: previous,
            hash,
        };
        self.get_object_metadata_cache
            .invalidate(&GetObjectMetadataCacheKey::new(bucket, object, previous))
            .await;
        #[cfg(test)]
        record_metadata_cache_invalidation(bucket, object);
    }

    fn invalidate_all_get_object_metadata_cache(&self) {
        for generation in self.get_object_metadata_cache_generations.iter() {
            let _ = generation.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| current.checked_add(1));
        }
        self.get_object_metadata_cache.invalidate_all();
    }

    #[inline(always)]
    fn record_put_object_commit_namespace_lock_wait(op: &'static str, acquire_start: Instant) {
        if op != "put_object_commit" || !rustfs_io_metrics::put_stage_metrics_enabled() {
            return;
        }
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_PUT_OBJECT_COMMIT_NAMESPACE_LOCK_WAIT,
            Some(acquire_start),
        );
    }

    async fn acquire_read_lock_diag(&self, op: &'static str, bucket: &str, object: &str) -> Result<ObjectLockDiagGuard> {
        crate::hp_guard!("SetDisks::acquire_read_lock");
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
        crate::hp_guard!("SetDisks::acquire_write_lock");
        let diag_enabled = is_object_lock_diag_enabled();
        let ns_lock = self.new_ns_lock(bucket, object).await?;
        let acquire_start = Instant::now();
        let acquire_timeout = get_put_object_commit_lock_acquire_timeout(op);
        let guard = resolve_put_object_commit_lock_acquire_result(
            self,
            op,
            bucket,
            object,
            ns_lock.get_write_lock(acquire_timeout).await,
        )?;
        Self::record_put_object_commit_namespace_lock_wait(op, acquire_start);
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

    #[cfg(any(test, feature = "test-util"))]
    async fn acquire_write_lock_diag_with_pending_hook(
        &self,
        op: &'static str,
        bucket: &str,
        object: &str,
        on_pending: impl FnOnce(),
    ) -> Result<ObjectLockDiagGuard> {
        crate::hp_guard!("SetDisks::acquire_write_lock");
        let diag_enabled = is_object_lock_diag_enabled();
        let ns_lock = self.new_ns_lock(bucket, object).await?;
        let acquire_start = Instant::now();
        let acquire_timeout = get_put_object_commit_lock_acquire_timeout(op);
        let acquire = ns_lock.get_write_lock(acquire_timeout);
        tokio::pin!(acquire);
        let mut on_pending = Some(on_pending);
        let guard = resolve_put_object_commit_lock_acquire_result(
            self,
            op,
            bucket,
            object,
            futures::future::poll_fn(|cx| match std::future::Future::poll(acquire.as_mut(), cx) {
                std::task::Poll::Pending => {
                    if let Some(on_pending) = on_pending.take() {
                        on_pending();
                    }
                    std::task::Poll::Pending
                }
                std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
            })
            .await,
        )?;
        Self::record_put_object_commit_namespace_lock_wait(op, acquire_start);
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
        Self::new_with_instance_ctx(
            locker_owner,
            disks,
            set_drive_count,
            default_parity_count,
            set_index,
            pool_index,
            set_endpoints,
            format,
            lockers,
            bootstrap_ctx(),
        )
        .await
    }

    /// Build a set bound to an explicit instance context (Phase 5 follow-up,
    /// backlog#1052). The legacy [`SetDisks::new`] entry adopts the process
    /// bootstrap context; a store constructed around its own context threads it
    /// down here so the whole object graph shares one cell.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_instance_ctx(
        locker_owner: String,
        disks: Arc<RwLock<Vec<Option<DiskStore>>>>,
        set_drive_count: usize,
        default_parity_count: usize,
        set_index: usize,
        pool_index: usize,
        set_endpoints: Vec<Endpoint>,
        format: FormatV3,
        lockers: Vec<Arc<dyn LockClient>>,
        instance_ctx: Arc<InstanceContext>,
    ) -> Arc<Self> {
        let ctx = instance_ctx;
        let set_lock_namespace: Arc<str> = format!("set-{pool_index}-{set_index}").into();
        let shared_lockers = Arc::from(lockers.to_vec());
        Arc::new(SetDisks {
            locker_owner,
            disks,
            set_drive_count,
            default_parity_count,
            set_index,
            pool_index,
            set_lock_namespace,
            format,
            set_endpoints,
            disk_health_cache: Arc::new(RwLock::new(Vec::new())),
            get_object_metadata_cache: moka::future::Cache::builder()
                .max_capacity(get_object_metadata_cache_max_entries() as u64)
                .time_to_live(GET_OBJECT_METADATA_CACHE_TTL)
                .build(),
            get_object_metadata_cache_hash_builder: std::collections::hash_map::RandomState::new(),
            get_object_metadata_cache_generations: Arc::from(
                (0..usize::from(GET_OBJECT_METADATA_CACHE_FENCE_SHARDS))
                    .map(|_| AtomicU64::new(0))
                    .collect::<Vec<_>>(),
            ),
            erasure_cache: Arc::new(ErasureCache::new()),
            lockers,
            shared_lockers,
            // Sourced from the instance context so each instance owns its lock
            // namespace (Phase 5 Slice 3). Single-instance: ctx aliases the
            // process lock-manager singleton, so this is unchanged.
            local_lock_manager: ctx.lock_manager(),
            ctx,
            capacity_scope_cache: Arc::new(std::sync::RwLock::new(CapacityScopeCache::default())),
            capacity_dirty_generation: Arc::new(AtomicU64::new(u64::MAX)),
            #[cfg(test)]
            storage_class_config_override: Arc::new(std::sync::RwLock::new(None)),
        })
    }

    /// This set's per-instance runtime context (Phase 5, backlog#939).
    #[allow(dead_code)] // Read by tests; consumed by later slices.
    pub(crate) fn instance_ctx(&self) -> &Arc<InstanceContext> {
        &self.ctx
    }

    /// Admit one short scanner cache publication under this set's instance
    /// movement fence. The caller must hold the returned guard through its
    /// final conditional cache write; no scan-round work belongs under it.
    pub async fn scanner_data_usage_publication_admission_guard(&self) -> Option<(tokio::sync::OwnedRwLockReadGuard<()>, u64)> {
        let operation_gate = self.ctx.data_movement_operation_gate();
        let operation_guard = operation_gate.read_owned().await;
        if self.ctx.scanner_publication_state_allowed() {
            let epoch = self.ctx.data_movement_operation_epoch();
            return Some((operation_guard, epoch));
        }

        // The owner deliberately marks the cached state UNKNOWN after every
        // movement epoch advance. Do not strand remote scanner writers in that
        // state: release this guard before asking the storage owner to refresh
        // its durable movement snapshot, since the owner uses the same gate.
        drop(operation_guard);
        let owner = runtime_sources::object_store_handle().filter(|owner| Arc::ptr_eq(&owner.ctx, &self.ctx))?;
        owner.scanner_data_usage_publication_admission_guard().await
    }

    /// Whether both sets' namespace-lock implementations cover the same object key.
    pub(crate) async fn shares_namespace_lock_domain(&self, other: &Self) -> bool {
        match (self.ctx.is_dist_erasure().await, other.ctx.is_dist_erasure().await) {
            (false, false) => Arc::ptr_eq(&self.local_lock_manager, &other.local_lock_manager),
            (true, true) => same_distributed_lock_domain(&self.lockers, &other.lockers),
            _ => false,
        }
    }

    /// The lock manager this set actually uses (test-only; Phase 5 Slice 3).
    #[cfg(test)]
    pub(crate) fn local_lock_manager_for_test(&self) -> &Arc<rustfs_lock::GlobalLockManager> {
        &self.local_lock_manager
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

#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
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

    let shards_needed = object_size.div_ceil(read_length);
    if shards_needed > data_shards {
        return None;
    }
    let encoded_capacity = read_length.checked_mul(shards_needed)?;
    let mut body = Vec::with_capacity(encoded_capacity);
    for reader in readers.iter_mut().take(shards_needed) {
        let reader = reader.as_mut()?;
        let Ok(read) = reader.read_appending(&mut body, read_length).await else {
            return None;
        };
        if read != read_length {
            return None;
        }

        if body.len() >= object_size {
            let body = Bytes::from(body);
            return Some(if body.len() == object_size {
                body
            } else {
                body.slice(..object_size)
            });
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
    disk_is_online: impl FnMut(usize) -> bool,
) -> Option<Vec<&'a FileInfo>> {
    collect_inline_data_shard_fileinfos_by_index_or_reason(parts_metadata, fi, data_shards, disk_is_online).ok()
}

fn collect_inline_data_shard_fileinfos_by_index_or_reason<'a>(
    parts_metadata: &'a [FileInfo],
    fi: &FileInfo,
    data_shards: usize,
    mut disk_is_online: impl FnMut(usize) -> bool,
) -> std::result::Result<Vec<&'a FileInfo>, &'static str> {
    let distribution = &fi.erasure.distribution;
    let mut data_files = vec![None; data_shards];

    for (disk_index, file_info) in parts_metadata.iter().enumerate() {
        if !disk_is_online(disk_index) {
            continue;
        }
        let Some(&block_index) = distribution.get(disk_index) else {
            return Err(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_GEOMETRY);
        };
        if block_index == 0 || block_index > data_shards {
            continue;
        }
        if file_info.name.is_empty() {
            return Err(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_MISSING_SHARD);
        }
        if file_info.erasure.index != block_index {
            return Err(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_IDENTITY_MISMATCH);
        }
        if !file_info.has_valid_erasure_geometry() {
            return Err(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_GEOMETRY);
        }
        if !core::io_primitives::metadata_early_stop_candidate_matches(file_info, fi) {
            return Err(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_IDENTITY_MISMATCH);
        }
        if file_info.data.as_ref().is_none_or(|data| data.is_empty()) {
            return Err(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_MISSING_PAYLOAD);
        }

        data_files[block_index - 1] = Some(file_info);
    }

    data_files
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(GET_METADATA_EARLY_STOP_REASON_DATA_READ_INLINE_MISSING_SHARD)
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

/// Whether the batch-delete path must stat each object under the held lock to
/// run [`check_object_lock_delete`] (the #4297 protection).
///
/// Under S3 semantics retention/legal-hold metadata can only be written to
/// buckets created with Object Lock enabled (`validate_bucket_object_lock_enabled`
/// guards every write surface), and default retention only exists with a bucket
/// lock configuration, so for buckets without Object Lock the per-object stat in
/// `delete_objects` has no consumer and can be skipped (backlog#929 / HP-8).
///
/// Fail closed: when bucket metadata cannot be resolved the check stays on, so
/// object-lock protection is never skipped because of a metadata lookup miss.
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub(crate) fn object_lock_delete_check_required(bucket_meta: Option<&crate::bucket::metadata::BucketMetadata>) -> bool {
    bucket_meta.is_none_or(|meta| meta.object_locking())
}

fn restore_expiry_snapshot_matches(obj_info: &ObjectInfo, opts: &ObjectOptions) -> bool {
    let expected = &opts.transition;
    expected.expire_restored
        && expected.status == TRANSITION_COMPLETE
        && obj_info.transitioned_object.status == TRANSITION_COMPLETE
        && !obj_info.transitioned_object.name.is_empty()
        && !obj_info.transitioned_object.tier.is_empty()
        && expected.tier == obj_info.transitioned_object.tier
        && expected.expected_remote_name == obj_info.transitioned_object.name
        && expected.expected_remote_version_id == obj_info.transitioned_object.version_id
        && !expected.etag.is_empty()
        && obj_info.etag.as_deref() == Some(expected.etag.as_str())
        && expected.expected_data_dir.is_some()
        && expected.expected_data_dir == obj_info.data_dir
        && obj_info.restore_expires == Some(expected.restore_expiry)
        && !obj_info.restore_ongoing
        // Deliberately no `restore_expiry <= now` clause. Whether the restored
        // copy is due to expire is the ILM evaluator's decision, already made
        // when it emitted DeleteRestoredAction; re-deriving it here only adds a
        // way for a legitimate action to be rejected. The stale-event risk it
        // looks like it covers is already covered above: a re-restore rewrites
        // `restore_expires`, so a replayed event fails the equality check.
        && match obj_info.version_id {
            Some(version_id) => opts.version_id.as_deref().and_then(|value| Uuid::parse_str(value).ok()) == Some(version_id),
            None => opts.version_id.is_none(),
        }
}

async fn check_object_lock_delete(
    ctx: &InstanceContext,
    bucket: &str,
    object: &str,
    obj_info: &ObjectInfo,
    opts: &ObjectOptions,
) -> Result<()> {
    if crate::bucket::utils::is_meta_bucketname(bucket) {
        return Ok(());
    }
    if opts.transition.expire_restored {
        return restore_expiry_snapshot_matches(obj_info, opts)
            .then_some(())
            .ok_or(StorageError::PreconditionFailed);
    }
    if set_disk_delete_creates_delete_marker(opts) {
        return Ok(());
    }

    let bypass_governance = opts
        .object_lock_delete
        .as_ref()
        .is_some_and(|delete_opts| delete_opts.bypass_governance);
    let blocked = match opts.object_lock_config_snapshot.as_deref() {
        Some(snapshot) => check_object_lock_for_deletion_with_state(snapshot.state(), obj_info, bypass_governance)?.is_some(),
        None => {
            let state = metadata_sys::get_object_lock_config_state_in(ctx, bucket).await?;
            check_object_lock_for_deletion_with_state(&state, obj_info, bypass_governance)?.is_some()
        }
    };
    if blocked {
        return Err(StorageError::PrefixAccessDenied(bucket.to_string(), object.to_string()));
    }

    Ok(())
}

fn ensure_delete_commit_locks_held(
    lock_guard: Option<&ObjectLockDiagGuard>,
    bucket: &str,
    object: &str,
    opts: &ObjectOptions,
) -> Result<()> {
    if lock_guard.is_some_and(ObjectLockDiagGuard::is_lock_lost)
        || opts
            .namespace_lock_fence
            .as_ref()
            .is_some_and(NamespaceLockFence::is_lock_lost)
        || opts
            .bucket_lifecycle_lock_fence
            .as_ref()
            .is_some_and(NamespaceLockFence::is_lock_lost)
    {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "delete_object_commit",
            bucket: bucket.to_string(),
            object: object.to_string(),
            required: 1,
            achieved: 0,
        });
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
    opts.delete_marker || ((opts.versioned || opts.version_suspended) && opts.version_id.is_none() && !opts.data_movement)
}

fn resolve_delete_version_state(opts: &ObjectOptions, goi: &ObjectInfo, version_found: bool) -> (bool, bool) {
    let mut mark_delete = goi.version_id.is_some() || ((opts.versioned || opts.version_suspended) && opts.version_id.is_none());
    let mut delete_marker = opts.versioned;

    if opts.version_id.is_some() {
        // Decommission/rebalance may recreate a delete marker on a new pool before that
        // exact version exists there, so we must still treat it as a mark-delete write.
        let data_movement_missing_delete_marker = opts.data_movement && opts.delete_marker && !version_found;
        if data_movement_missing_delete_marker {
            mark_delete = true;
            delete_marker = true;
        }

        let delete_marker_version_purge = version_found && goi.delete_marker && !opts.version_purge_status().is_empty();

        if version_found && opts.delete_marker_replication_status() == ReplicationStatusType::Replica {
            mark_delete = false;
        }

        if !data_movement_missing_delete_marker
            && opts.version_purge_status().is_empty()
            && opts.delete_marker_replication_status().is_empty()
        {
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
        let storage_class_config = self.storage_class_config_snapshot();
        let bucket_lifecycle_guard = if let Some(expected_incarnation_id) = opts.expected_bucket_incarnation_id
            && opts.bucket_lifecycle_lock_fence.is_none()
            && !crate::bucket::utils::is_meta_bucketname(bucket)
        {
            Some(
                metadata_sys::object_store_in(&self.ctx)
                    .await?
                    .acquire_bucket_incarnation_fence(bucket, expected_incarnation_id)
                    .await?,
            )
        } else {
            None
        };
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

        if opts.http_preconditions.is_some()
            && let Some(err) = self.check_write_precondition(bucket, object, opts).await
        {
            return Err(err);
        }

        let disks = self.disks.read().await.clone();
        let storage_class = opts.user_defined.get(AMZ_STORAGE_CLASS).map(String::as_str);
        let layout = resolve_write_layout(
            &storage_class_config,
            self.pool_index,
            disks.len(),
            self.default_parity_count,
            storage_class,
            opts.max_parity,
        )?;
        let fi = build_tiered_decommission_file_info(bucket, object, fi, layout);
        let write_quorum = layout.write_quorum;
        if _lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
            || opts
                .namespace_lock_fence
                .as_ref()
                .is_some_and(NamespaceLockFence::is_lock_lost)
            || opts
                .bucket_lifecycle_lock_fence
                .as_ref()
                .is_some_and(NamespaceLockFence::is_lock_lost)
            || bucket_lifecycle_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
        {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "decommission_tiered_object_commit",
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }
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
        if file_info_is_valid_for_metadata(fi) {
            valid_meta = fi.clone();
            break;
        }
    }

    if !file_info_is_valid_for_metadata(&valid_meta) {
        let data_blocks = meta_arr.len().div_ceil(2);
        if not_found_parts_errs > data_blocks {
            return (valid_meta, true);
        }

        return (valid_meta, false);
    }

    if non_actionable_meta_errs > 0 || non_actionable_parts_errs > 0 {
        return (valid_meta, false);
    }

    if valid_meta.is_canonical_delete_marker() {
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
        if !file_info_is_valid_for_metadata(meta) {
            // Since for majority of the cases erasure.Index matches with erasure.Distribution we can
            // consider the offline disks as consistent.
            continue;
        }
        if !meta.is_canonical_delete_marker() {
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
            debug!(
                event = EVENT_SET_DISK_HEAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                bucket,
                object = %object_name,
                disk_index = index,
                state = "metadata_corrupt",
                "Set disk object metadata is corrupt"
            );
            meta_errs[index] = Some(DiskError::FileCorrupt);
            parts_metadata[index] = FileInfo::default();
            *disk_op = None;

            continue;
        }

        if erasure_distribution_reliable {
            if !file_info_is_valid_for_metadata(meta) {
                debug!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object = %object_name,
                    disk_index = index,
                    state = "metadata_invalid",
                    "Set disk object metadata is invalid"
                );
                parts_metadata[index] = FileInfo::default();
                meta_errs[index] = Some(DiskError::FileCorrupt);
                *disk_op = None;
                continue;
            }

            if !meta.is_canonical_delete_marker() && meta.erasure.distribution.len() != online_disks_len {
                // Erasure distribution is not the same as onlineDisks
                // attempt a fix if possible, assuming other entries
                // might have the right erasure distribution.
                debug!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object = %object_name,
                    disk_index = index,
                    state = "erasure_distribution_mismatch",
                    "Set disk erasure distribution mismatched online disks"
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
        if meta.is_canonical_delete_marker() || meta.is_remote() {
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
                        bucket,
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
                        bucket,
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
        debug!(
            event = EVENT_SET_DISK_HEAL,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_SET_DISK,
            object = %meta.name,
            state = "metadata_outdated",
            "Set disk object metadata is outdated"
        );
        return (true, true, Some(DiskError::OutdatedXLMeta));
    }

    if !meta.is_canonical_delete_marker() && !meta.is_remote() {
        if parts_errs.contains(&CHECK_PART_FILE_CORRUPT) {
            return (true, false, Some(DiskError::FileCorrupt));
        }
        if parts_errs.contains(&CHECK_PART_FILE_NOT_FOUND) {
            return (true, false, Some(DiskError::PartMissingOrCorrupt));
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
            let cached_disk_id = disk.cached_disk_id().await;
            if runtime_state.should_probe_for_admin() || runtime_state == disk::health_state::RuntimeDriveHealthState::Suspect {
                match disk
                    .disk_info(&DiskInfoOptions {
                        metrics: true,
                        ..Default::default()
                    })
                    .await
                {
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
                            metrics: Some(res.metrics),
                            ..Default::default()
                        });
                    }
                    Err(err) => {
                        let mut disk_info = rustfs_madmin::Disk {
                            state: err.to_string(),
                            endpoint: eps[i].to_string(),
                            drive_path: eps[i].get_file_path(),
                            local: eps[i].is_local,
                            pool_index: eps[i].pool_idx,
                            set_index: eps[i].set_idx,
                            disk_index: eps[i].disk_idx,
                            runtime_state: Some(runtime_state.as_str().to_string()),
                            offline_duration_seconds,
                            metrics: disk.metrics_snapshot(),
                            uuid: cached_disk_id.map_or_else(String::new, |id| id.to_string()),
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
                let mut disk_info =
                    build_runtime_snapshot_disk(&eps[i], runtime_state, offline_duration_seconds, capacity_snapshot);
                disk_info.metrics = disk.metrics_snapshot();
                disk_info.uuid = cached_disk_id.map_or_else(String::new, |id| id.to_string());
                ret.push(disk_info);
            }
        } else {
            ret.push(rustfs_madmin::Disk {
                endpoint: eps[i].to_string(),
                drive_path: eps[i].get_file_path(),
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
        drive_path: endpoint.get_file_path(),
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
        // XXHash3/64/128 and SHA-512 (AWS 2026-04): s3s CompletePart has no typed
        // field to carry a client-supplied per-part value in the
        // CompleteMultipartUpload request, so accept the type with no double-check
        // value (the part was already verified server-side at UploadPart). This
        // mirrors the missing-value path of the five typed algorithms. Reject only
        // genuinely unset/invalid types.
        base if base.is_set() => Some(None),
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
    storageclass::is_supported_write_class(storage_class)
}

/// Returns true if the storage class is a cold storage tier that requires special handling
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub fn is_cold_storage_class(storage_class: &str) -> bool {
    matches!(
        storage_class,
        storageclass::DEEP_ARCHIVE | storageclass::GLACIER | storageclass::GLACIER_IR
    )
}

/// Returns true if the storage class is an infrequent access tier
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
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
    use crate::cluster::rpc::{RemoteDisk, TcpHttpInternodeDataTransport};
    use crate::disk::CHECK_PART_UNKNOWN;
    use crate::disk::CHECK_PART_VOLUME_NOT_FOUND;
    use crate::disk::DataDirDeleteStatus;
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
    use crate::object_api::BLOCK_SIZE_V2;
    use crate::object_api::ObjectInfo;
    use crate::set_disk::core::io_primitives::rename_fanout_barrier;
    use crate::storage_api_contracts::{
        heal::HealOperations as _, lifecycle::TransitionedObject, list::ListOperations as _, multipart::CompletePart,
        namespace::NamespaceLocking as _, object::ObjectIO as _, object::ObjectOperations as _,
    };
    use crate::store::init_format::save_format_file;
    use crate::store::list_objects::ListPathOptions;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
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
    use tokio::io::AsyncReadExt;

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
        make_test_set_disks_with_ctx(lockers, bootstrap_ctx()).await
    }

    async fn make_test_set_disks_with_ctx(
        lockers: Vec<Arc<dyn LockClient>>,
        instance_ctx: Arc<InstanceContext>,
    ) -> Arc<SetDisks> {
        let endpoints = vec![
            Endpoint::try_from("http://127.0.0.1:9000/data").expect("first endpoint should parse"),
            Endpoint::try_from("http://127.0.0.1:9001/data").expect("second endpoint should parse"),
        ];

        SetDisks::new_with_instance_ctx(
            "test-owner".to_string(),
            Arc::new(RwLock::new(vec![None, None])),
            2,
            1,
            0,
            0,
            endpoints,
            FormatV3::new(1, 2),
            lockers,
            instance_ctx,
        )
        .await
    }

    /// Pins the dist-erasure resolution SOURCE for `new_ns_lock` (adversarial
    /// review): the lock strategy must come from the set's own instance
    /// context, never the ambient facade — otherwise another in-process
    /// instance (or a concurrent test's ambient DistErasure window) reroutes
    /// this set's locking onto the wrong strategy.
    #[tokio::test(flavor = "multi_thread")]
    async fn new_ns_lock_resolves_dist_from_set_instance_context() {
        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let locker: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager));

        let dist_ctx = Arc::new(InstanceContext::new());
        dist_ctx.update_erasure_type(SetupType::DistErasure).await;
        let dist_set = make_test_set_disks_with_ctx(vec![locker.clone()], dist_ctx).await;
        let dist_guard = dist_set
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_read_lock(Duration::from_millis(500))
            .await
            .expect("dist read lock should succeed with one healthy locker");
        assert!(
            matches!(dist_guard, NamespaceLockGuard::Standard(_)),
            "a DistErasure instance context must select the distributed lock strategy"
        );
        drop(dist_guard);

        let local_ctx = Arc::new(InstanceContext::new());
        local_ctx.update_erasure_type(SetupType::Erasure).await;
        let local_set = make_test_set_disks_with_ctx(vec![locker], local_ctx).await;
        let local_guard = local_set
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_read_lock(Duration::from_millis(500))
            .await
            .expect("local read lock should succeed");
        assert!(
            matches!(local_guard, NamespaceLockGuard::Fast(_)),
            "a plain-erasure instance context must select the local lock strategy"
        );
    }

    #[tokio::test]
    async fn new_ns_lock_reuses_the_set_namespace_allocation() {
        let ctx = Arc::new(InstanceContext::new());
        ctx.update_erasure_type(SetupType::Erasure).await;
        let set = make_test_set_disks_with_ctx(Vec::new(), ctx).await;

        assert_eq!(&*set.set_lock_namespace, "set-0-0");
        let before = Arc::strong_count(&set.set_lock_namespace);
        let lock = set
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created");

        assert_eq!(
            Arc::strong_count(&set.set_lock_namespace),
            before + 1,
            "each lock should share the set namespace instead of formatting a new String"
        );
        drop(lock);
        assert_eq!(Arc::strong_count(&set.set_lock_namespace), before);
    }

    fn put_object_commit_namespace_lock_wait_sample_count(snapshotter: &metrics_util::debugging::Snapshotter) -> usize {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _, _, _)| {
                composite.key().name() == "rustfs_s3_put_object_stage_duration_ms"
                    && composite.key().labels().any(|label| {
                        label.key() == "stage"
                            && label.value() == rustfs_io_metrics::PUT_STAGE_PUT_OBJECT_COMMIT_NAMESPACE_LOCK_WAIT
                    })
            })
            .map(|(_, _, _, value)| match value {
                DebugValue::Histogram(samples) => samples.len(),
                _ => 0,
            })
            .sum()
    }

    fn put_object_commit_lock_admission_count(
        rows: &[(
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        )],
        budget: &'static str,
        outcome: &'static str,
    ) -> u64 {
        rows.iter()
            .filter(|(composite, _, _, _)| {
                composite.key().name() == "rustfs_s3_put_object_commit_namespace_lock_admission_total"
                    && composite
                        .key()
                        .labels()
                        .any(|label| label.key() == "budget" && label.value() == budget)
                    && composite
                        .key()
                        .labels()
                        .any(|label| label.key() == "outcome" && label.value() == outcome)
            })
            .map(|(_, _, _, value)| match value {
                DebugValue::Counter(count) => *count,
                _ => 0,
            })
            .sum()
    }

    #[test]
    #[serial]
    fn put_object_commit_lock_admission_budget_labels_are_bounded() {
        let cases = [
            ("0", rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_DISABLED),
            ("250", rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_250MS),
            ("251", rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_500MS),
            ("500", rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_500MS),
            ("501", rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_1000MS),
            ("1000", rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_1000MS),
            ("1001", rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_GT_1000MS),
        ];
        for (timeout_ms, expected) in cases {
            temp_env::with_vars(
                [(rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some(timeout_ms))],
                || {
                    assert_eq!(put_object_commit_lock_admission_budget_label(), expected);
                },
            );
        }
    }

    #[test]
    #[serial]
    fn put_object_commit_lock_admission_error_outcomes_are_bounded() {
        let timeout = LockError::timeout("bucket/object", Duration::from_millis(1));
        temp_env::with_vars([(rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some("1"))], || {
            assert_eq!(
                put_object_commit_lock_acquire_error_outcome("put_object_commit", &timeout),
                rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_TIMEOUT_SLOWDOWN
            );
            assert_eq!(
                put_object_commit_lock_acquire_error_outcome("complete_multipart_upload_commit", &timeout),
                rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_LOCK_ERROR
            );
        });

        let internal = LockError::internal("simulated lock manager error");
        temp_env::with_vars([(rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some("1"))], || {
            assert_eq!(
                put_object_commit_lock_acquire_error_outcome("put_object_commit", &internal),
                rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_LOCK_ERROR
            );
        });
    }

    #[test]
    #[serial]
    fn put_object_commit_namespace_lock_wait_metric_is_wired_to_both_write_lock_paths() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should start");

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let ctx = Arc::new(InstanceContext::new());
                ctx.update_erasure_type(SetupType::Erasure).await;
                let set = make_test_set_disks_with_ctx(Vec::new(), ctx).await;
                let bucket = "bucket";
                let object = "object";

                rustfs_io_metrics::set_put_stage_metrics_enabled(false);
                let guard = set
                    .acquire_write_lock_diag("put_object_commit", bucket, object)
                    .await
                    .expect("disabled metrics acquire should succeed");
                drop(guard);
                assert_eq!(put_object_commit_namespace_lock_wait_sample_count(&snapshotter), 0);

                rustfs_io_metrics::set_put_stage_metrics_enabled(true);
                let guard = set
                    .acquire_write_lock_diag("put_object_commit", bucket, object)
                    .await
                    .expect("normal PUT commit acquire should succeed");
                drop(guard);
                assert_eq!(put_object_commit_namespace_lock_wait_sample_count(&snapshotter), 1);

                let guard = set
                    .acquire_write_lock_diag("complete_multipart_upload_commit", bucket, object)
                    .await
                    .expect("non-PUT commit acquire should succeed");
                drop(guard);
                assert_eq!(put_object_commit_namespace_lock_wait_sample_count(&snapshotter), 0);

                let held_guard = set
                    .acquire_write_lock_diag("put_object_commit", bucket, object)
                    .await
                    .expect("holder acquire should succeed");
                assert_eq!(put_object_commit_namespace_lock_wait_sample_count(&snapshotter), 1);

                let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
                let pending_acquire =
                    set.acquire_write_lock_diag_with_pending_hook("put_object_commit", bucket, object, move || {
                        let _ = pending_tx.send(());
                    });
                let release_holder = async {
                    pending_rx.await.expect("pending hook should fire");
                    drop(held_guard);
                };
                let (pending_guard, ()) = tokio::join!(pending_acquire, release_holder);
                drop(pending_guard.expect("pending-hook PUT commit acquire should succeed"));
                assert_eq!(put_object_commit_namespace_lock_wait_sample_count(&snapshotter), 1);

                rustfs_io_metrics::set_put_stage_metrics_enabled(false);
            });
        });
    }

    #[test]
    #[serial]
    fn put_object_commit_lock_timeout_override_only_applies_to_put_commit() {
        temp_env::with_vars([(rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some("17"))], || {
            assert_eq!(get_put_object_commit_lock_acquire_timeout("put_object_commit"), Duration::from_millis(17));
            assert_eq!(
                get_put_object_commit_lock_acquire_timeout("complete_multipart_upload_commit"),
                get_lock_acquire_timeout()
            );
        });

        temp_env::with_vars([(rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some("0"))], || {
            assert_eq!(
                get_put_object_commit_lock_acquire_timeout("put_object_commit"),
                get_lock_acquire_timeout()
            );
        });
    }

    #[test]
    #[serial]
    fn put_object_commit_lock_timeout_override_bounds_contention_wait() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should start");

        temp_env::with_vars([(rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some("1"))], || {
            runtime.block_on(async {
                let ctx = Arc::new(InstanceContext::new());
                ctx.update_erasure_type(SetupType::Erasure).await;
                let set = make_test_set_disks_with_ctx(Vec::new(), ctx).await;
                let bucket = "bucket";
                let object = "object";

                let held_guard = set
                    .acquire_write_lock_diag("put_object_commit", bucket, object)
                    .await
                    .expect("holder acquire should succeed");
                let started = Instant::now();
                let err = match set.acquire_write_lock_diag("put_object_commit", bucket, object).await {
                    Ok(_) => panic!("contended PUT commit lock should honor the short timeout"),
                    Err(err) => err,
                };
                assert!(
                    started.elapsed() < Duration::from_secs(1),
                    "short PUT commit lock timeout should not wait for the global timeout"
                );
                assert!(matches!(err, StorageError::SlowDown));

                drop(held_guard);
                set.acquire_write_lock_diag("put_object_commit", bucket, object)
                    .await
                    .expect("permit should not leak after timeout");
            });
        });
    }

    #[test]
    #[serial]
    fn put_object_commit_lock_admission_records_acquired_and_timeout() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should start");

        temp_env::with_vars([(rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some("1"))], || {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            metrics::with_local_recorder(&recorder, || {
                rustfs_io_metrics::set_put_stage_metrics_enabled(true);
                runtime.block_on(async {
                    let ctx = Arc::new(InstanceContext::new());
                    ctx.update_erasure_type(SetupType::Erasure).await;
                    let set = make_test_set_disks_with_ctx(Vec::new(), ctx).await;
                    let held_guard = set
                        .acquire_write_lock_diag("put_object_commit", "bucket", "object")
                        .await
                        .expect("holder acquire should succeed");
                    let err = match set.acquire_write_lock_diag("put_object_commit", "bucket", "object").await {
                        Ok(_) => panic!("contended PUT commit acquire should return SlowDown"),
                        Err(err) => err,
                    };
                    assert!(matches!(err, StorageError::SlowDown));
                    drop(held_guard);
                    rustfs_io_metrics::set_put_stage_metrics_enabled(false);
                });
            });

            let rows = snapshotter.snapshot().into_vec();
            assert_eq!(
                put_object_commit_lock_admission_count(
                    &rows,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_250MS,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_ACQUIRED,
                ),
                1
            );
            assert_eq!(
                put_object_commit_lock_admission_count(
                    &rows,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_250MS,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_TIMEOUT_SLOWDOWN,
                ),
                1
            );
        });
    }

    #[test]
    #[serial]
    fn put_object_commit_lock_admission_records_disabled_budget_acquired() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should start");

        temp_env::with_vars([(rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some("0"))], || {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            metrics::with_local_recorder(&recorder, || {
                rustfs_io_metrics::set_put_stage_metrics_enabled(true);
                runtime.block_on(async {
                    let ctx = Arc::new(InstanceContext::new());
                    ctx.update_erasure_type(SetupType::Erasure).await;
                    let set = make_test_set_disks_with_ctx(Vec::new(), ctx).await;
                    let guard = set
                        .acquire_write_lock_diag("put_object_commit", "bucket", "object")
                        .await
                        .expect("PUT commit acquire should succeed with default timeout");
                    drop(guard);
                    rustfs_io_metrics::set_put_stage_metrics_enabled(false);
                });
            });

            let rows = snapshotter.snapshot().into_vec();
            assert_eq!(
                put_object_commit_lock_admission_count(
                    &rows,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_DISABLED,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_ACQUIRED,
                ),
                1
            );
        });
    }

    #[test]
    #[serial]
    fn put_object_commit_lock_admission_skips_non_put_commit_ops() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should start");

        temp_env::with_vars([(rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some("250"))], || {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            metrics::with_local_recorder(&recorder, || {
                rustfs_io_metrics::set_put_stage_metrics_enabled(true);
                runtime.block_on(async {
                    let ctx = Arc::new(InstanceContext::new());
                    ctx.update_erasure_type(SetupType::Erasure).await;
                    let set = make_test_set_disks_with_ctx(Vec::new(), ctx).await;
                    let guard = set
                        .acquire_write_lock_diag("complete_multipart_upload_commit", "bucket", "object")
                        .await
                        .expect("non-PUT commit acquire should succeed");
                    drop(guard);
                    rustfs_io_metrics::set_put_stage_metrics_enabled(false);
                });
            });

            let rows = snapshotter.snapshot().into_vec();
            assert_eq!(
                rows.iter()
                    .filter(|(composite, _, _, _)| {
                        composite.key().name() == "rustfs_s3_put_object_commit_namespace_lock_admission_total"
                    })
                    .count(),
                0
            );
        });
    }

    #[test]
    #[serial]
    fn put_object_commit_lock_admission_records_lock_error() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should start");

        temp_env::with_vars([(rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some("250"))], || {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            metrics::with_local_recorder(&recorder, || {
                rustfs_io_metrics::set_put_stage_metrics_enabled(true);
                runtime.block_on(async {
                    let healthy: Arc<dyn LockClient> =
                        Arc::new(LocalClient::with_manager(Arc::new(rustfs_lock::GlobalLockManager::new())));
                    let failing: Arc<dyn LockClient> = Arc::new(FailingClient);
                    let ctx = Arc::new(InstanceContext::new());
                    ctx.update_erasure_type(SetupType::DistErasure).await;
                    let set = make_test_set_disks_with_ctx(vec![healthy, failing], ctx).await;
                    assert!(
                        set.acquire_write_lock_diag("put_object_commit", "bucket", "object")
                            .await
                            .is_err(),
                        "one healthy locker must not satisfy the PUT commit write quorum"
                    );
                    rustfs_io_metrics::set_put_stage_metrics_enabled(false);
                });
            });

            let rows = snapshotter.snapshot().into_vec();
            assert_eq!(
                put_object_commit_lock_admission_count(
                    &rows,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_250MS,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_LOCK_ERROR,
                ),
                1
            );
            assert_eq!(
                put_object_commit_lock_admission_count(
                    &rows,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_250MS,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_TIMEOUT_SLOWDOWN,
                ),
                0
            );
        });
    }

    #[test]
    #[serial]
    fn put_object_commit_lock_admission_records_pending_hook_acquired() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should start");

        temp_env::with_vars([(rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some("500"))], || {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            metrics::with_local_recorder(&recorder, || {
                rustfs_io_metrics::set_put_stage_metrics_enabled(true);
                runtime.block_on(async {
                    let ctx = Arc::new(InstanceContext::new());
                    ctx.update_erasure_type(SetupType::Erasure).await;
                    let set = make_test_set_disks_with_ctx(Vec::new(), ctx).await;
                    let held_guard = set
                        .acquire_write_lock_diag("put_object_commit", "bucket", "object")
                        .await
                        .expect("holder acquire should succeed");
                    let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
                    let pending_acquire =
                        set.acquire_write_lock_diag_with_pending_hook("put_object_commit", "bucket", "object", move || {
                            let _ = pending_tx.send(());
                        });
                    let release_holder = async {
                        pending_rx.await.expect("pending hook should fire");
                        drop(held_guard);
                    };
                    let (pending_guard, ()) = tokio::join!(pending_acquire, release_holder);
                    drop(pending_guard.expect("pending-hook PUT commit acquire should succeed"));
                    rustfs_io_metrics::set_put_stage_metrics_enabled(false);
                });
            });

            let rows = snapshotter.snapshot().into_vec();
            assert_eq!(
                put_object_commit_lock_admission_count(
                    &rows,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_500MS,
                    rustfs_io_metrics::PUT_COMMIT_LOCK_ADMISSION_OUTCOME_ACQUIRED,
                ),
                2
            );
        });
    }

    #[tokio::test]
    async fn new_ns_lock_shares_clients_without_changing_quorum() {
        let healthy: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(Arc::new(rustfs_lock::GlobalLockManager::new())));
        let failing: Arc<dyn LockClient> = Arc::new(FailingClient);
        let ctx = Arc::new(InstanceContext::new());
        ctx.update_erasure_type(SetupType::DistErasure).await;
        let set = make_test_set_disks_with_ctx(vec![healthy.clone(), failing.clone()], ctx).await;

        assert!(Arc::ptr_eq(&set.lockers[0], &healthy));
        assert!(Arc::ptr_eq(&set.lockers[1], &failing));
        let clients_before = Arc::strong_count(&set.shared_lockers);
        let healthy_before = Arc::strong_count(&healthy);
        let failing_before = Arc::strong_count(&failing);
        let write_lock = set
            .new_ns_lock("bucket", "write-object")
            .await
            .expect("namespace lock should be created");

        assert_eq!(
            Arc::strong_count(&set.shared_lockers),
            clients_before + 1,
            "each object lock should share one client slice allocation"
        );
        assert_eq!(
            Arc::strong_count(&healthy),
            healthy_before,
            "constructing an object lock must not clone each client Arc"
        );
        assert_eq!(
            Arc::strong_count(&failing),
            failing_before,
            "constructing an object lock must not clone each client Arc"
        );

        let write_error = write_lock
            .get_write_lock(Duration::from_millis(500))
            .await
            .expect_err("one healthy client must not satisfy the two-client write quorum");
        assert!(
            matches!(
                write_error,
                LockError::QuorumNotReached {
                    required: 2,
                    achieved: 1
                }
            ),
            "the shared client representation must preserve the exact write quorum result: {write_error}"
        );
        let read_lock = set
            .new_ns_lock("bucket", "read-object")
            .await
            .expect("second namespace lock should be created");
        assert_eq!(Arc::strong_count(&set.shared_lockers), clients_before + 2);
        let read_guard = read_lock
            .get_read_lock(Duration::from_millis(500))
            .await
            .expect("one healthy client should satisfy the two-client read quorum");
        assert!(matches!(read_guard, NamespaceLockGuard::Standard(_)));
    }

    #[tokio::test]
    async fn new_ns_lock_uses_the_current_public_client_domain() {
        let stale_a: Arc<dyn LockClient> = Arc::new(FailingClient);
        let stale_b: Arc<dyn LockClient> = Arc::new(FailingClient);
        let healthy_a: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(Arc::new(rustfs_lock::GlobalLockManager::new())));
        let healthy_b: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(Arc::new(rustfs_lock::GlobalLockManager::new())));
        let ctx = Arc::new(InstanceContext::new());
        ctx.update_erasure_type(SetupType::DistErasure).await;
        let set = make_test_set_disks_with_ctx(vec![stale_a, stale_b], ctx).await;
        let mut set = (*set).clone();
        set.lockers = vec![healthy_a, healthy_b];

        let lock = set
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should use the current public clients");
        let guard = lock
            .get_write_lock(Duration::from_millis(500))
            .await
            .expect("the current healthy clients should satisfy the two-client quorum");
        assert!(matches!(guard, NamespaceLockGuard::Standard(_)));
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

    async fn make_remote_disk_for_info_test(disk_idx: usize) -> (Endpoint, DiskStore) {
        let endpoint_url = format!("http://remote-server:9000/data{disk_idx}");
        let mut endpoint = Endpoint::try_from(endpoint_url.as_str()).expect("remote endpoint should parse");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(disk_idx);
        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("remote disk should be created");

        (endpoint, Arc::new(disk::Disk::Remote(Box::new(remote_disk))))
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

    #[tokio::test]
    async fn test_rename_data_inline_quorum_failure_rolls_back_destination_object() {
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
        let object = "inline-object";
        let tmp_object = "tmp-inline-object";
        let version_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("version id should parse");

        match disk.make_volume(bucket).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("bucket should be available: {err:?}"),
        }
        match disk.make_volume(RUSTFS_META_TMP_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("tmp bucket should be available: {err:?}"),
        }

        let object_dir = disk_root.join(bucket).join(object);
        fs::create_dir_all(&object_dir).await.expect("object dir should be created");
        let mut old_fi = FileInfo::new(&format!("{bucket}/{object}"), 1, 1);
        old_fi.name = object.to_string();
        old_fi.version_id = Some(version_id);
        old_fi.data = Some(Bytes::from_static(b"old-inline"));
        old_fi.size = 10;
        old_fi.mod_time = Some(OffsetDateTime::now_utc());
        let mut old_meta = FileMeta::default();
        old_meta.add_version(old_fi).expect("old metadata should accept file info");
        let old_meta_buf = old_meta.marshal_msg().expect("old metadata should encode");
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), old_meta_buf.clone())
            .await
            .expect("old metadata should be written");

        let mut new_fi = FileInfo::new(&format!("{bucket}/{object}"), 1, 1);
        new_fi.name = object.to_string();
        new_fi.version_id = Some(version_id);
        new_fi.data = Some(Bytes::from_static(b"new-inline"));
        new_fi.size = 10;
        new_fi.mod_time = Some(OffsetDateTime::now_utc());

        let disks = vec![Some(disk), None];
        let file_infos = vec![new_fi.clone(), new_fi];
        let result = SetDisks::rename_data(&disks, RUSTFS_META_TMP_BUCKET, tmp_object, &file_infos, bucket, object, 2).await;

        assert!(result.is_err());
        let restored_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("destination metadata should remain readable");
        assert_eq!(restored_meta, old_meta_buf);
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
    fn resolve_delete_version_state_creates_missing_suspended_data_movement_marker() {
        let opts = ObjectOptions {
            version_suspended: true,
            version_id: Some(Uuid::nil().to_string()),
            data_movement: true,
            delete_marker: true,
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
    async fn streaming_reader_holds_read_lock_until_eof() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::Erasure).await;
        let set_disks = make_test_set_disks(vec![Arc::new(LocalClient::with_manager(Arc::new(
            rustfs_lock::GlobalLockManager::new(),
        )))])
        .await;

        let read_guard = set_disks
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_read_lock(Duration::from_secs(1))
            .await
            .expect("read lock should be acquired");
        let read_guard = ObjectLockDiagGuard::new(
            read_guard,
            false,
            "GetObject",
            Some("bucket".to_owned()),
            Some("object".to_owned()),
            None,
            "read",
        );
        let mut reader = finish_set_disk_read_lock(
            GetObjectReader {
                stream: Box::new(Cursor::new(b"body")),
                object_info: ObjectInfo::default(),
                buffered_body: None,
                body_source: Default::default(),
            },
            Some(read_guard),
            "bucket",
            "object",
        );

        let blocked_write = set_disks
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_millis(100))
            .await;
        assert!(blocked_write.is_err(), "a stalled response must block an overwrite");

        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("stream should reach EOF");
        assert_eq!(body, b"body");

        let write_guard = set_disks
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_secs(1))
            .await;
        assert!(write_guard.is_ok(), "the overwrite should proceed after EOF releases the read lock");
        drop(write_guard);

        let read_guard = set_disks
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_read_lock(Duration::from_secs(1))
            .await
            .expect("second read lock should be acquired");
        let reader = finish_set_disk_read_lock(
            GetObjectReader {
                stream: Box::new(tokio::io::empty()),
                object_info: ObjectInfo::default(),
                buffered_body: None,
                body_source: Default::default(),
            },
            Some(ObjectLockDiagGuard::new(
                read_guard,
                false,
                "GetObject",
                Some("bucket".to_owned()),
                Some("object".to_owned()),
                None,
                "read",
            )),
            "bucket",
            "object",
        );
        drop(reader);

        let write_after_drop = set_disks
            .new_ns_lock("bucket", "object")
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_secs(1))
            .await;
        assert!(write_after_drop.is_ok(), "dropping the response must release the read lock");
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

        let result = timeout(
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
        .expect("broad prefix delete must not wait on a literal prefix namespace lock");

        if let Err(err) = result {
            assert!(
                !err.to_string().to_ascii_lowercase().contains("lock"),
                "broad prefix delete returned a lock error: {err}"
            );
        }
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

        let result = timeout(
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
        .expect("no_lock exact prefix delete path must not wait for the outer lock");

        if let Err(err) = result {
            assert!(
                !err.to_string().to_ascii_lowercase().contains("lock"),
                "no_lock exact prefix delete returned a lock error: {err}"
            );
        }
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

    // backlog#1315: the memoized dirty scope must be byte-for-byte identical to
    // the ad-hoc scope the previous per-write construction produced, otherwise
    // dirty-disk keys diverge from the disk-cache keys and capacity counts drift.
    #[tokio::test]
    #[serial]
    async fn capacity_scope_memo_matches_adhoc_and_is_reused() {
        use rustfs_object_capacity::capacity_scope::drain_global_dirty_scopes;

        let (dir_a, disk_a) = make_single_local_disk().await;
        let (dir_b, disk_b) = make_single_local_disk().await;
        let disks = vec![Some(disk_a), Some(disk_b)];
        let set = make_set_disks_with(disks.clone()).await;

        let expected = capacity_scope_from_disks(&disks);
        let first = set.capacity_scope(&disks);
        assert_eq!(*first, expected, "memoized scope must equal the ad-hoc per-disk construction bit-for-bit");

        // Second call on a fully-resolved set returns the very same Arc (no new
        // allocation): proving steady-state writes do not rebuild String/HashSet.
        let second = set.capacity_scope(&disks);
        assert!(Arc::ptr_eq(&first, &second), "steady-state scope must reuse the cached Arc");

        let _ = drain_global_dirty_scopes();
        drop((dir_a, dir_b));
    }

    // backlog#1315: the global registry mutex must be upgraded only on the first
    // write of each generation; steady-state writes skip it. Reverting the
    // generation skip makes the upgrade count grow per write and fails this test.
    #[tokio::test]
    #[serial]
    async fn record_capacity_scope_upgrades_registry_once_per_generation() {
        use rustfs_object_capacity::capacity_scope::{drain_global_dirty_scopes, global_dirty_upgrade_count};

        let (dir_a, disk_a) = make_single_local_disk().await;
        let (dir_b, disk_b) = make_single_local_disk().await;
        let disks = vec![Some(disk_a), Some(disk_b)];
        let set = make_set_disks_with(disks.clone()).await;

        // Start from a clean registry generation.
        let _ = drain_global_dirty_scopes();
        let before = global_dirty_upgrade_count();

        // First write of this generation upgrades the registry exactly once.
        set.record_capacity_scope_if_needed(None, &disks);
        assert_eq!(
            global_dirty_upgrade_count(),
            before + 1,
            "first write of a generation must upgrade the global registry"
        );

        // Subsequent writes in the same generation must not touch the mutex.
        for _ in 0..16 {
            set.record_capacity_scope_if_needed(None, &disks);
        }
        assert_eq!(
            global_dirty_upgrade_count(),
            before + 1,
            "steady-state writes must reuse the generation mark, not re-upgrade"
        );

        // A drain advances the generation; the next write must re-mark so the
        // disks it wrote are captured by the following refresh (no lost update).
        let drained = drain_global_dirty_scopes();
        assert_eq!(drained.len(), 2, "both set disks must have been recorded dirty");
        set.record_capacity_scope_if_needed(None, &disks);
        assert_eq!(
            global_dirty_upgrade_count(),
            before + 2,
            "the first write after a drain must re-upgrade the registry"
        );

        let _ = drain_global_dirty_scopes();
        drop((dir_a, dir_b));
    }

    // backlog#1315: an offline slot must not force the per-write slow path, and
    // the resolved scope must still cover every online disk.
    #[tokio::test]
    #[serial]
    async fn capacity_scope_tolerates_offline_slot_without_reallocating() {
        use rustfs_object_capacity::capacity_scope::drain_global_dirty_scopes;

        let (dir_a, disk_a) = make_single_local_disk().await;
        // Slot 1 is permanently offline (None); the set never resolves it.
        let disks = vec![Some(disk_a), None];
        let set = make_set_disks_with(disks.clone()).await;

        let first = set.capacity_scope(&disks);
        assert_eq!(first.disks.len(), 1, "only the online disk contributes to the scope");
        // Even though the set is not "complete" (slot 1 unresolved), repeated
        // writes with the same online set reuse the cached Arc via the
        // no-unresolved-slot fast path.
        let second = set.capacity_scope(&disks);
        assert!(
            Arc::ptr_eq(&first, &second),
            "a stable online subset must reuse the cached Arc even with an offline slot"
        );

        let _ = drain_global_dirty_scopes();
        drop(dir_a);
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

    async fn recv_abandoned_parts_trace(
        trace: &mut rustfs_common::trace_bus::TraceSubscription,
        bucket: &str,
        object: &str,
        state: &str,
    ) -> rustfs_common::trace_bus::TraceEvent {
        for _ in 0..32 {
            let event = tokio::time::timeout(std::time::Duration::from_secs(1), trace.recv())
                .await
                .expect("abandoned-parts trace event should arrive")
                .expect("trace bus should stay open");
            if event.kind == rustfs_common::trace_bus::TraceKind::Heal
                && event.func == rustfs_common::trace_bus::TraceFunc::HealCheckAbandonedParts
                && event.bucket.as_deref() == Some(bucket)
                && event.object.as_deref() == Some(object)
                && trace_attr_string(&event, "state").as_deref() == Some(state)
            {
                return (*event).clone();
            }
        }

        panic!("expected abandoned-parts trace state {state} for {bucket}/{object}");
    }

    fn trace_attr_string(event: &rustfs_common::trace_bus::TraceEvent, key: &str) -> Option<String> {
        event.attrs.iter().find_map(|attr| {
            if attr.key != key {
                return None;
            }
            Some(match &attr.value {
                rustfs_common::trace_bus::TraceVal::Bool(value) => value.to_string(),
                rustfs_common::trace_bus::TraceVal::U64(value) => value.to_string(),
                rustfs_common::trace_bus::TraceVal::I64(value) => value.to_string(),
                rustfs_common::trace_bus::TraceVal::Str(value) => value.to_string(),
            })
        })
    }

    #[tokio::test]
    async fn check_abandoned_parts_dry_run_counts_without_deleting() {
        let mut trace = rustfs_common::trace_bus::subscribe_trace_events();
        let (dir, disk) = make_single_local_disk().await;
        let live = Uuid::new_v4();
        let orphan = Uuid::new_v4();

        let object_dir = dir.path().join("bucket").join("obj");
        write_object_meta_with_data_dirs(&object_dir, "bucket", "obj", &[live]).await;
        fs::create_dir_all(object_dir.join(live.to_string()))
            .await
            .expect("live data dir should be created");
        fs::create_dir_all(object_dir.join(orphan.to_string()))
            .await
            .expect("orphan data dir should be created");

        let set = make_set_disks_with(vec![Some(disk)]).await;
        set.check_abandoned_parts(
            "bucket",
            "obj",
            &HealOpts {
                dry_run: true,
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("dry-run abandoned-parts check should succeed");
        let dry_run_trace = recv_abandoned_parts_trace(&mut trace, "bucket", "obj", "dry_run_matched").await;
        assert_eq!(trace_attr_string(&dry_run_trace, "dry_run").as_deref(), Some("true"));
        assert_eq!(trace_attr_string(&dry_run_trace, "data_dirs").as_deref(), Some("1"));

        assert!(object_dir.join(live.to_string()).exists(), "referenced data dir must be preserved");
        assert!(object_dir.join(orphan.to_string()).exists(), "dry-run must not remove orphaned data dir");

        set.check_abandoned_parts(
            "bucket",
            "obj",
            &HealOpts {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("abandoned-parts check should reclaim stale data dir");
        let reclaim_trace = recv_abandoned_parts_trace(&mut trace, "bucket", "obj", "reclaimed").await;
        assert_eq!(trace_attr_string(&reclaim_trace, "dry_run").as_deref(), Some("false"));
        assert_eq!(trace_attr_string(&reclaim_trace, "data_dirs").as_deref(), Some("1"));

        assert!(
            object_dir.join(live.to_string()).exists(),
            "referenced data dir must remain after reclaim"
        );
        assert!(!object_dir.join(orphan.to_string()).exists(), "orphaned data dir must be removed");
    }

    #[tokio::test]
    async fn reclaim_orphan_data_dirs_recovers_deferred_cleanup_after_restart() {
        let (dir, disk) = make_single_local_disk().await;
        let live = Uuid::new_v4();
        let orphan = Uuid::new_v4();
        let object_dir = dir.path().join("bucket").join("obj");
        write_object_meta_with_data_dirs(&object_dir, "bucket", "obj", &[live]).await;
        fs::create_dir_all(object_dir.join(live.to_string()))
            .await
            .expect("live data dir should be created");
        let orphan_path = format!("obj/{orphan}");
        disk.write_all("bucket", &format!("{orphan_path}/part.1"), Bytes::from_static(b"stale"))
            .await
            .expect("orphan part should be written");

        let _token = disk
            .acquire_snapshot_lease("bucket", &orphan_path)
            .await
            .expect("snapshot lease should be acquired");
        assert_eq!(
            disk.delete_data_dir(
                "bucket",
                &orphan_path,
                DeleteOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .await
            .expect("cleanup should be deferred"),
            DataDirDeleteStatus::Deferred
        );
        drop(disk);

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let restarted = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should restart");
        let set = make_set_disks_with(vec![Some(restarted)]).await;
        let removed = set
            .reclaim_orphan_data_dirs("bucket", "obj")
            .await
            .expect("restart reclaim should succeed");

        assert_eq!(removed, 1, "the deferred orphan should be reclaimed after restart");
        assert!(object_dir.join(live.to_string()).exists(), "referenced data dir must be preserved");
        assert!(!object_dir.join(orphan.to_string()).exists(), "deferred orphan must be removed");
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
        fs::write(
            object_dir.join(stray.to_string()).join(format!(
                "{}{}",
                crate::disk::local::RESERVED_DELETE_DATA_DIR_MARKER_PREFIX,
                Uuid::new_v4()
            )),
            [],
        )
        .await
        .expect("pre-commit delete reservation should be written");

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

    #[tokio::test]
    async fn reclaim_orphan_data_dirs_recovers_committed_delete_marker_without_meta() {
        let (dir, disk) = make_single_local_disk().await;
        let stale = Uuid::new_v4();
        let transaction = Uuid::new_v4();
        let object_dir = dir.path().join("bucket").join("obj");
        let stale_dir = object_dir.join(stale.to_string());
        fs::create_dir_all(&stale_dir)
            .await
            .expect("committed stale data dir should be created");
        fs::write(stale_dir.join("part.1"), b"stale")
            .await
            .expect("stale part should be written");
        fs::write(
            stale_dir.join(format!("{}{}", crate::disk::local::DELETE_DATA_DIR_MARKER_PREFIX, transaction)),
            [],
        )
        .await
        .expect("committed delete marker should be written");

        let set = make_set_disks_with(vec![Some(disk)]).await;
        let removed = set
            .reclaim_orphan_data_dirs("bucket", "obj")
            .await
            .expect("upgrade reclaim should succeed");

        assert_eq!(removed, 1, "the committed delete residue should be reclaimed");
        assert!(!stale_dir.exists(), "the committed stale data dir should be removed");
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

    // backlog#898 A5: old == committed data dir => anti-misdelete guard skips the
    // whole cleanup; the live (just-committed) dir must be left untouched.
    #[tokio::test]
    async fn commit_rename_data_dir_skips_delete_when_old_equals_committed_dir() {
        let (dir, disk) = make_single_local_disk().await;
        let root = dir.path();
        let bucket = "bucket";
        let object = "object";
        fs::create_dir_all(root.join(bucket))
            .await
            .expect("bucket volume should be created");

        let same_dir = Uuid::parse_str("55555555-5555-5555-5555-555555555555").expect("dir should parse");
        let data_path = root.join(bucket).join(object).join(same_dir.to_string());
        fs::create_dir_all(&data_path).await.expect("data dir should be created");
        fs::write(data_path.join("part.1"), b"live")
            .await
            .expect("live part should be written");

        let set = make_set_disks_with(vec![Some(disk.clone())]).await;
        let cleanup = set
            .commit_rename_data_dir(&[Some(disk.clone())], bucket, object, &same_dir.to_string(), &same_dir.to_string(), 1)
            .await;

        assert_eq!(cleanup.attempted, 0, "guard must skip: no delete may be issued");
        assert!(cleanup.unreclaimed_disks.is_empty());
        assert!(!cleanup.has_residue());
        assert!(data_path.join("part.1").exists(), "committed data dir must NOT be deleted");
    }

    // backlog#898 A5b: old != committed and old dir exists => normal reclaim; the
    // dereferenced old dir is physically removed and the receipt reports success.
    #[tokio::test]
    async fn commit_rename_data_dir_reclaims_distinct_old_dir() {
        let (dir, disk) = make_single_local_disk().await;
        let root = dir.path();
        let bucket = "bucket";
        let object = "object";
        fs::create_dir_all(root.join(bucket))
            .await
            .expect("bucket volume should be created");

        let old_dir = Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("old dir should parse");
        let new_dir = Uuid::parse_str("22222222-2222-2222-2222-222222222222").expect("new dir should parse");
        let old_path = root.join(bucket).join(object).join(old_dir.to_string());
        fs::create_dir_all(&old_path).await.expect("old data dir should be created");
        fs::write(old_path.join("part.1"), b"stale")
            .await
            .expect("stale part should be written");

        let set = make_set_disks_with(vec![Some(disk.clone())]).await;
        let cleanup = set
            .commit_rename_data_dir(&[Some(disk.clone())], bucket, object, &old_dir.to_string(), &new_dir.to_string(), 1)
            .await;

        assert_eq!(cleanup.attempted, 1);
        assert_eq!(cleanup.reclaimed, 1);
        assert!(!cleanup.has_residue());
        assert!(!cleanup.below_quorum);
        assert!(!old_path.exists(), "dereferenced old data dir must be physically removed");
    }

    // backlog#898 group B (end-to-end): a real overwrite whose post-commit old
    // data dir cleanup is forced to fail (via the test-only fault seam) must
    // still return 200 with the new ObjectInfo — no false-negative ACK.
    #[tokio::test]
    async fn put_object_overwrite_returns_ok_when_old_data_dir_cleanup_fails() {
        use crate::set_disk::core::io_primitives::cleanup_fault_injection;

        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-cleanup-fault";
        let object = "object-below-quorum-cleanup";

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        // v1: establishes an old data dir that the overwrite will try to reclaim.
        let mut reader = PutObjReader::from_vec(b"hello".to_vec());
        set_disks
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    ..ObjectOptions::default()
                },
            )
            .await
            .expect("first write should succeed");

        // Force the old-data-dir cleanup delete to fail on disk 0 during the
        // overwrite. rename_data still commits; only the GC of the dereferenced
        // old dir fails, which must NOT turn the PUT into a 503.
        let _fault = cleanup_fault_injection::fail_cleanup_on(object, &[0]);

        let mut reader = PutObjReader::from_vec(b"goodbye!!".to_vec());
        let oi = set_disks
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    ..ObjectOptions::default()
                },
            )
            .await
            .expect("overwrite must return Ok even when old-data-dir cleanup fails");

        assert_eq!(oi.size, 9, "returned ObjectInfo must reflect the new committed write");

        // The committed new version must be readable with the new size.
        let read_back = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("committed object must be readable after a failed cleanup");
        assert_eq!(read_back.size, 9, "HEAD must observe the new version, not stale metadata");
    }

    // Regression for the inline-overwrite rollback backup leak: #5703 stopped
    // reporting the synthetic rollback dir for recursive post-commit cleanup,
    // which stranded `object/<synthetic>/xl.meta.bkp` after every inline
    // overwrite. The object dir then never emptied, so the s3-tests teardown
    // sequence (delete object, delete bucket) failed with BucketNotEmpty
    // forever. After a committed overwrite the backup must be reclaimed and a
    // subsequent delete must leave nothing behind.
    #[tokio::test]
    async fn inline_overwrite_reclaims_synthetic_rollback_backup() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-inline-rollback-leak";
        let object = "obj";

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        for body in [b"hello".to_vec(), b"goodbye".to_vec()] {
            let mut reader = PutObjReader::from_vec(body);
            set_disks
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        no_lock: true,
                        ..ObjectOptions::default()
                    },
                )
                .await
                .expect("inline write should succeed");
        }

        // The committed overwrite must leave only xl.meta in the object dir on
        // every disk; a stranded rollback dir keeps the bucket undeletable.
        for endpoint in &set_disks.set_endpoints {
            let object_dir = std::path::PathBuf::from(endpoint.get_file_path()).join(bucket).join(object);
            let mut entries: Vec<String> = std::fs::read_dir(&object_dir)
                .expect("object dir should exist")
                .map(|entry| entry.expect("entry should read").file_name().to_string_lossy().into_owned())
                .collect();
            entries.sort();
            assert_eq!(
                entries,
                vec![STORAGE_FORMAT_FILE.to_string()],
                "only xl.meta may remain after an inline overwrite in {object_dir:?}"
            );
        }

        // With only xl.meta left, the s3-tests teardown (delete object, delete
        // bucket) empties the dir; the delete paths themselves are covered by
        // their own tests. This harness has no bucket metadata sys, so the
        // full delete_object flow cannot run here.
    }

    // #5703's security property must survive the backup reclamation: the
    // synthetic rollback dir of key K maps to the directory `K/<uuid>`, which
    // can simultaneously be a legitimate child key. Reclaiming the backup must
    // remove exactly the backup file — never the child key's metadata.
    #[tokio::test]
    async fn inline_overwrite_backup_reclaim_spares_child_key_dir() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-inline-rollback-child";
        let object = "obj";
        let synthetic = crate::disk::local::inline_metadata_rollback_dir(Uuid::nil(), &FileMeta::new());
        let child_object = format!("{object}/{synthetic}");

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let mut reader = PutObjReader::from_vec(b"child".to_vec());
        set_disks
            .put_object(
                bucket,
                &child_object,
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    ..ObjectOptions::default()
                },
            )
            .await
            .expect("child write should succeed");

        // Create then overwrite the parent key: the overwrite writes its
        // rollback backup into the child's directory and must afterwards
        // reclaim only that file.
        for body in [b"first".to_vec(), b"second".to_vec()] {
            let mut reader = PutObjReader::from_vec(body);
            set_disks
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        no_lock: true,
                        ..ObjectOptions::default()
                    },
                )
                .await
                .expect("parent write should succeed");
        }

        let child_info = set_disks
            .get_object_info(bucket, &child_object, &ObjectOptions::default())
            .await
            .expect("child key must survive the parent's rollback backup reclamation");
        assert_eq!(child_info.size, 5, "child key content must be untouched");

        for endpoint in &set_disks.set_endpoints {
            let child_dir = std::path::PathBuf::from(endpoint.get_file_path())
                .join(bucket)
                .join(object)
                .join(synthetic.to_string());
            let mut entries: Vec<String> = std::fs::read_dir(&child_dir)
                .expect("child object dir should exist")
                .map(|entry| entry.expect("entry should read").file_name().to_string_lossy().into_owned())
                .collect();
            entries.sort();
            assert_eq!(
                entries,
                vec![STORAGE_FORMAT_FILE.to_string()],
                "the child dir must keep its xl.meta and lose only the stray backup in {child_dir:?}"
            );
        }
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
                include_part_checksums: true,
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
        let mut first = quorum_test_fileinfo(mod_time, data_dir, "part-etag-old", 1);
        rustfs_utils::http::insert_str(
            &mut first.metadata,
            rustfs_utils::http::SUFFIX_PART_CHECKSUMS,
            r#"[[1,[["CRC32C","AAAAAA=="]]]]"#.to_string(),
        );
        let mut second = first.clone();
        second.erasure.index = 2;
        let metas = vec![first, second, FileInfo::default(), FileInfo::default()];
        let errs = vec![None, None, Some(DiskError::DiskNotFound), Some(DiskError::DiskNotFound)];

        let (_, mut selected, selected_quorum) =
            SetDisks::select_valid_fileinfo(&vec![None; metas.len()], &metas, &errs, "", 2, 3)
                .expect("read quorum should remain enough when no competing latest is visible");

        assert_eq!(selected_quorum, 2);
        assert_eq!(selected.data_dir, Some(data_dir));
        assert_eq!(selected.parts[0].etag, "part-etag-old");
        assert!(selected.parts[0].checksums.is_none());
        SetDisks::hydrate_selected_fileinfo_part_checksums(&mut selected)
            .expect("requested part checksums should hydrate after winner selection");
        assert_eq!(
            selected.parts[0]
                .checksums
                .as_ref()
                .and_then(|checksums| checksums.get("CRC32C"))
                .map(String::as_str),
            Some("AAAAAA==")
        );
    }

    #[test]
    fn test_degraded_fileinfo_selection_rejects_malformed_part_checksum_metadata() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let mut first = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 1);
        rustfs_utils::http::insert_str(&mut first.metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS, "not-json".to_string());
        let mut second = first.clone();
        second.erasure.index = 2;
        let metas = vec![first, second, FileInfo::default(), FileInfo::default()];
        let errs = vec![None, None, Some(DiskError::DiskNotFound), Some(DiskError::DiskNotFound)];

        let (_, mut selected, _) = SetDisks::select_valid_fileinfo(&vec![None; metas.len()], &metas, &errs, "", 2, 3)
            .expect("winner selection should defer sidecar decoding");
        let err = SetDisks::hydrate_selected_fileinfo_part_checksums(&mut selected)
            .expect_err("a malformed degraded winner must fail closed when checksums are requested");

        assert_eq!(err, DiskError::FileCorrupt);
    }

    #[test]
    fn test_pick_valid_fileinfo_rejects_malformed_part_checksum_metadata() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        let mut meta = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 1);
        rustfs_utils::http::insert_str(&mut meta.metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS, "not-json".to_string());
        let mut second = meta.clone();
        second.erasure.index = 2;

        let mut selected = SetDisks::pick_valid_fileinfo(&[meta, second], Some(mod_time), None, 2)
            .expect("winner selection should defer sidecar decoding");
        let err = SetDisks::hydrate_selected_fileinfo_part_checksums(&mut selected)
            .expect_err("a malformed winning part-checksum sidecar must fail closed when checksums are requested");

        assert_eq!(err, DiskError::FileCorrupt);
    }

    #[test]
    fn test_part_checksum_hydration_rejects_invalid_algorithm_and_value() {
        let mod_time = OffsetDateTime::now_utc();
        let data_dir = Uuid::new_v4();
        for encoded in [
            r#"[[1,[["UNKNOWN","AAAAAA=="]]]]"#,
            r#"[[1,[["CRC32C","not-base64"]]]]"#,
            r#"[[1,[["CRC32C","AA=="]]]]"#,
            r#"[[1,[["CRC32C","AAAAAA==-0"]]]]"#,
            r#"[[1,[["CRC32C","AAAAAA==-1"]]]]"#,
            r#"[[1,[["CRC32C","AAAAAA=="],["crc32c","BBBBBB=="]]]]"#,
        ] {
            let mut meta = quorum_test_fileinfo(mod_time, data_dir, "part-etag", 1);
            rustfs_utils::http::insert_str(&mut meta.metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS, encoded.to_string());

            let err = SetDisks::hydrate_selected_fileinfo_part_checksums(&mut meta)
                .expect_err("invalid persisted part checksum metadata must fail closed");
            assert_eq!(err, DiskError::FileCorrupt);
        }
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
                block_size: 4,
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
                block_size: 4,
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
                block_size: 4,
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
    fn delete_markers_participate_in_four_disk_metadata_quorum_without_erasure_geometry() {
        let marker = FileInfo {
            name: "bucket/deleted".to_string(),
            deleted: true,
            version_id: Some(Uuid::new_v4()),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        let parts_metadata = vec![marker; 4];
        let errs = vec![None; 4];

        assert!(parts_metadata.iter().all(file_info_is_valid_for_metadata));
        assert!(parts_metadata.iter().all(|metadata| !metadata.is_valid()));
        assert_eq!(SetDisks::list_object_parities(&parts_metadata, &errs), vec![2; 4]);
        assert_eq!(
            SetDisks::object_quorum_from_meta(&parts_metadata, &errs, 2)
                .expect("four matching delete markers must reach metadata quorum"),
            (2, 3)
        );
    }

    #[test]
    fn metadata_boundary_does_not_relax_non_delete_or_malformed_delete_metadata() {
        assert!(!file_info_is_valid_for_metadata(&FileInfo::default()));

        let mut transitioned = FileInfo::new("bucket/transitioned", 2, 2);
        transitioned.erasure.index = 1;
        transitioned.transition_status = TRANSITION_COMPLETE.to_string();
        assert!(file_info_is_valid_for_metadata(&transitioned));

        transitioned.erasure = ErasureInfo::default();
        assert!(
            !file_info_is_valid_for_metadata(&transitioned),
            "transition state must not relax local erasure validation"
        );

        let mut purge_pending = FileInfo::new("bucket/purge-pending", 2, 2);
        purge_pending.erasure.index = 1;
        purge_pending.deleted = true;
        purge_pending.parts.push(ObjectPartInfo {
            number: 1,
            ..Default::default()
        });
        assert!(
            file_info_is_valid_for_metadata(&purge_pending),
            "purge-pending payload metadata must retain its valid erasure vote"
        );
        assert!(!purge_pending.is_canonical_delete_marker());

        let mut malformed_marker = FileInfo {
            deleted: true,
            ..Default::default()
        };
        malformed_marker.parts = vec![
            ObjectPartInfo {
                number: 1,
                ..Default::default()
            },
            ObjectPartInfo {
                number: 1,
                ..Default::default()
            },
        ];
        assert!(!file_info_is_valid_for_metadata(&malformed_marker));
    }

    #[test]
    fn purge_pending_payload_uses_its_erasure_parity_for_metadata_quorum() {
        let version_id = Uuid::new_v4();
        let mod_time = OffsetDateTime::now_utc();
        let parts_metadata = (1..=6)
            .map(|disk_index| {
                let mut purge_pending = FileInfo::new("bucket/purge-pending", 5, 1);
                purge_pending.name = "bucket/purge-pending".to_string();
                purge_pending.version_id = Some(version_id);
                purge_pending.mod_time = Some(mod_time);
                purge_pending.size = 1;
                purge_pending.deleted = true;
                purge_pending.erasure.index = disk_index;
                purge_pending.add_object_part(1, "part-etag-1".to_string(), 1, None, 1, None, None);
                purge_pending
            })
            .collect::<Vec<_>>();
        let errs = vec![None; 6];

        assert!(parts_metadata.iter().all(file_info_is_valid_for_metadata));
        assert!(parts_metadata.iter().all(|metadata| !metadata.is_canonical_delete_marker()));
        assert_eq!(SetDisks::list_object_parities(&parts_metadata, &errs), vec![1; 6]);
        assert_eq!(
            SetDisks::object_quorum_from_meta(&parts_metadata, &errs, 3)
                .expect("purge-pending payload should retain its EC:1 quorum"),
            (5, 5)
        );
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

        let err = Some(DiskError::FileCorrupt);
        let (should_heal, is_meta, reason) = should_heal_object_on_disk(&err, &[], &meta, &latest_meta);
        assert!(should_heal);
        assert!(is_meta);
        assert_eq!(reason, Some(DiskError::FileCorrupt));

        // Test with no error and no part errors
        let (should_heal, _, _) = should_heal_object_on_disk(&None, &[CHECK_PART_SUCCESS], &meta, &latest_meta);
        assert!(!should_heal);

        // Test with part corruption
        let (should_heal, _, reason) = should_heal_object_on_disk(&None, &[CHECK_PART_FILE_CORRUPT], &meta, &latest_meta);
        assert!(should_heal);
        assert_eq!(reason, Some(DiskError::FileCorrupt));
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
        let offline_disk_id = Uuid::new_v4();
        disks[2]
            .as_ref()
            .expect("disk 2 should exist")
            .set_disk_id_state(Some(offline_disk_id))
            .await
            .expect("offline disk id should be cached");
        disks[2]
            .as_ref()
            .expect("disk 2 should exist")
            .force_runtime_state_for_test(RuntimeDriveHealthState::Offline);

        let info = get_disks_info(&disks, &endpoints).await;
        assert_eq!(info.len(), 3);

        assert_eq!(info[0].state, "ok");
        assert_eq!(info[0].runtime_state.as_deref(), Some("online"));
        assert!(!info[0].drive_path.is_empty(), "online disk should keep immediate disk_info probe");
        assert!(
            info[0]
                .metrics
                .as_ref()
                .and_then(|metrics| metrics.api_calls.get("disk_info"))
                .copied()
                .unwrap_or_default()
                > 0,
            "online disk should expose disk_info operation metrics"
        );

        assert_eq!(info[1].state, "ok");
        assert_eq!(info[1].runtime_state.as_deref(), Some("suspect"));
        assert!(!info[1].drive_path.is_empty(), "suspect disk should still probe for fresher disk info");
        assert!(
            info[1]
                .metrics
                .as_ref()
                .and_then(|metrics| metrics.last_minute.get("disk_info"))
                .map(|action| action.count)
                .unwrap_or_default()
                > 0,
            "suspect disk should expose last-minute disk_info latency"
        );

        assert_eq!(info[2].state, "offline");
        assert_eq!(info[2].runtime_state.as_deref(), Some("offline"));
        assert_eq!(
            info[2].drive_path,
            endpoints[2].get_file_path(),
            "offline disk should keep stable endpoint path"
        );
        assert_eq!(info[2].uuid, offline_disk_id.to_string());
        assert!(
            info[2].metrics.is_some(),
            "offline runtime fallback should preserve disk metrics snapshot"
        );
    }

    #[tokio::test]
    async fn test_get_disks_info_preserves_remote_cached_disk_id_when_offline() {
        let (endpoint, disk) = make_remote_disk_for_info_test(0).await;
        let remote_disk_id = Uuid::new_v4();
        disk.set_disk_id_state(Some(remote_disk_id))
            .await
            .expect("remote disk id should be cached");
        disk.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);

        let info = get_disks_info(&[Some(disk)], &[endpoint]).await;

        assert_eq!(info.len(), 1);
        assert_eq!(info[0].state, "offline");
        assert_eq!(info[0].runtime_state.as_deref(), Some("offline"));
        assert_eq!(info[0].uuid, remote_disk_id.to_string());
    }

    #[tokio::test]
    async fn test_get_disks_info_preserves_cached_disk_id_after_failed_live_probe() {
        let format = FormatV3::new(1, 1);
        let (temp_dir, endpoint, disk) = make_formatted_local_disk_for_info_test(0, &format).await;
        let cached_disk_id = Uuid::new_v4();
        disk.set_disk_id_state(Some(cached_disk_id))
            .await
            .expect("disk id should be cached before the failed probe");
        disk.force_runtime_state_for_test(RuntimeDriveHealthState::Suspect);

        let info = get_disks_info(&[Some(disk)], &[endpoint]).await;

        assert_eq!(info.len(), 1);
        assert_eq!(info[0].runtime_state.as_deref(), Some("suspect"));
        assert_eq!(info[0].uuid, cached_disk_id.to_string());
        assert_eq!(
            info[0].drive_path,
            temp_dir.path().to_string_lossy(),
            "failed live probe should still keep the endpoint path"
        );
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

    fn rename_versions_signature(first_key: u8, version_count: usize) -> Vec<u8> {
        let mut signature = vec![0; version_count * 16];
        signature[7] = first_key;
        signature
    }

    // backlog#1321: `classify_rename_convergence` is the single decision that
    // replaced the old `Option<Vec<u8>>::is_some()` heal gate. These are the
    // revert-fails white-box cases behind the issue acceptance matrix — if the
    // decision regressed to "a signature exists => needs heal", the healthy
    // 4/4 and 8/8 cases below would flip from `AllSuccessIdentical` to a
    // heal-worthy variant and fail.
    #[test]
    fn test_classify_rename_convergence_healthy_identical_needs_no_heal() {
        use crate::set_disk::core::io_primitives::RenameConvergence;
        let sig = rename_versions_signature(1, 3);

        // Healthy 4/4: every disk committed with an identical, known signature.
        let disk_versions = vec![Some(sig.clone()), Some(sig.clone()), Some(sig.clone()), Some(sig.clone())];
        let errs = vec![None, None, None, None];
        let convergence = SetDisks::classify_rename_convergence(&disk_versions, &errs);
        assert_eq!(convergence, RenameConvergence::AllSuccessIdentical);
        assert!(!convergence.needs_heal(), "a fully converged healthy MPU must not enqueue heal");

        // Healthy 8/8: same property at a wider set width.
        let disk_versions = vec![Some(sig); 8];
        let errs = vec![None; 8];
        let convergence = SetDisks::classify_rename_convergence(&disk_versions, &errs);
        assert_eq!(convergence, RenameConvergence::AllSuccessIdentical);
        assert!(!convergence.needs_heal());
    }

    #[test]
    fn test_classify_rename_convergence_three_agree_one_diverges_heals() {
        use crate::set_disk::core::io_primitives::RenameConvergence;
        let common = rename_versions_signature(1, 2);
        let odd = rename_versions_signature(2, 2);

        // 3-same, 1-divergent, all committed: reconcile the odd replica.
        let disk_versions = vec![Some(common.clone()), Some(common.clone()), Some(common), Some(odd)];
        let errs = vec![None, None, None, None];
        let convergence = SetDisks::classify_rename_convergence(&disk_versions, &errs);
        assert_eq!(convergence, RenameConvergence::SignatureDivergent);
        assert!(convergence.needs_heal());
    }

    #[test]
    fn test_classify_rename_convergence_failed_or_offline_disk_heals() {
        use crate::set_disk::core::io_primitives::RenameConvergence;
        let sig = rename_versions_signature(1, 2);

        // 1 disk failed/offline while the rest committed identically: past the
        // write-quorum gate this is a `PartialCommit` — a replica is missing.
        let disk_versions = vec![Some(sig.clone()), Some(sig.clone()), Some(sig), None];
        let errs = vec![None, None, None, Some(DiskError::DiskNotFound)];
        let convergence = SetDisks::classify_rename_convergence(&disk_versions, &errs);
        assert_eq!(convergence, RenameConvergence::PartialCommit);
        assert!(convergence.needs_heal());
    }

    #[test]
    fn test_classify_rename_convergence_no_common_quorum_heals() {
        use crate::set_disk::core::io_primitives::RenameConvergence;
        // No signature holds a majority (2/2 split), every disk committed:
        // divergence with no common quorum still reconciles via heal.
        let a = rename_versions_signature(1, 1);
        let b = rename_versions_signature(2, 1);
        let disk_versions = vec![Some(a.clone()), Some(a), Some(b.clone()), Some(b)];
        let errs = vec![None, None, None, None];
        let convergence = SetDisks::classify_rename_convergence(&disk_versions, &errs);
        assert_eq!(convergence, RenameConvergence::SignatureDivergent);
        assert!(convergence.needs_heal());
    }

    #[test]
    fn test_classify_rename_convergence_over_ten_versions_by_success() {
        use crate::set_disk::core::io_primitives::RenameConvergence;
        // >10 versions: every disk deliberately omits the signature (`None`).
        // All committed => `Unknown` => scanner-backstopped, no self-enqueue.
        let disk_versions = vec![None, None, None, None];
        let errs = vec![None, None, None, None];
        let convergence = SetDisks::classify_rename_convergence(&disk_versions, &errs);
        assert_eq!(convergence, RenameConvergence::Unknown);
        assert!(
            !convergence.needs_heal(),
            ">10-version healthy commit relies on the scanner, not self-enqueue"
        );

        // Same >10-version shape but with a failed disk: a failure is
        // conservative-heal regardless of signatures being unavailable.
        let errs = vec![None, None, None, Some(DiskError::FileCorrupt)];
        let convergence = SetDisks::classify_rename_convergence(&disk_versions, &errs);
        assert_eq!(convergence, RenameConvergence::PartialCommit);
        assert!(convergence.needs_heal());
    }

    #[test]
    fn test_classify_rename_convergence_mixed_signed_unsigned_heals() {
        use crate::set_disk::core::io_primitives::RenameConvergence;
        // A committed replica with <=10 versions (signed) alongside one with
        // >10 versions (unsigned) is itself a version-count divergence.
        let sig = rename_versions_signature(1, 2);
        let disk_versions = vec![Some(sig.clone()), Some(sig.clone()), Some(sig), None];
        let errs = vec![None, None, None, None];
        let convergence = SetDisks::classify_rename_convergence(&disk_versions, &errs);
        assert_eq!(convergence, RenameConvergence::SignatureDivergent);
        assert!(convergence.needs_heal());
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

        let layout = WriteLayout::from_parity(16, 4).expect("tiered write layout should be valid");
        let updated = build_tiered_decommission_file_info("bucket", "object", &original, layout);

        assert_eq!(updated.version_id, original.version_id);
        assert_eq!(updated.transition_status, original.transition_status);
        assert_eq!(updated.transitioned_objname, original.transitioned_objname);
        assert_eq!(updated.transition_tier, original.transition_tier);
        assert_eq!(updated.transition_version_id, original.transition_version_id);
        assert_eq!(updated.erasure.data_blocks, 12);
        assert_eq!(updated.erasure.parity_blocks, 4);
        assert_eq!(layout.write_quorum, 12);
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
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))),
            ..Default::default()
        };

        let err = check_object_lock_delete(&bootstrap_ctx(), "bucket", "object", &obj_info, &opts)
            .await
            .expect_err("COMPLIANCE retention must block explicit version deletion");

        assert!(matches!(err, StorageError::PrefixAccessDenied(_, _)));
    }

    #[tokio::test]
    async fn test_check_object_lock_delete_allows_retained_restored_copy_expiry() {
        let retain_until = OffsetDateTime::now_utc() + Duration::from_secs(60 * 60 * 24 * 60);
        let restore_expiry = OffsetDateTime::now_utc() - Duration::from_secs(1);
        let version_id = Uuid::new_v4();
        let data_dir = Uuid::new_v4();
        let obj_info = ObjectInfo {
            version_id: Some(version_id),
            data_dir: Some(data_dir),
            etag: Some("etag".to_string()),
            transitioned_object: TransitionedObject {
                name: "remote-object".to_string(),
                tier: "tier".to_string(),
                status: TRANSITION_COMPLETE.to_string(),
                ..Default::default()
            },
            restore_expires: Some(restore_expiry),
            user_defined: Arc::new(HashMap::from([
                (
                    X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
                    s3s::dto::ObjectLockRetentionMode::COMPLIANCE.to_string(),
                ),
                (
                    X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
                    retain_until.format(&time::format_description::well_known::Rfc3339).unwrap(),
                ),
            ])),
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some(version_id.to_string()),
            versioned: true,
            transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                status: TRANSITION_COMPLETE.to_string(),
                tier: "tier".to_string(),
                etag: "etag".to_string(),
                expected_data_dir: Some(data_dir),
                expected_remote_name: "remote-object".to_string(),
                restore_expiry,
                expire_restored: true,
                ..Default::default()
            },
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))),
            ..Default::default()
        };

        check_object_lock_delete(&bootstrap_ctx(), "bucket", "object", &obj_info, &opts)
            .await
            .expect("restore expiry only strips the local copy and must preserve the retained logical version");
    }

    #[tokio::test]
    async fn test_check_object_lock_delete_rejects_stale_restored_copy_expiry() {
        let restore_expiry = OffsetDateTime::now_utc() - Duration::from_secs(1);
        let data_dir = Uuid::new_v4();
        let obj_info = ObjectInfo {
            data_dir: Some(data_dir),
            etag: Some("etag".to_string()),
            transitioned_object: TransitionedObject {
                name: "remote-object".to_string(),
                tier: "tier".to_string(),
                status: TRANSITION_COMPLETE.to_string(),
                ..Default::default()
            },
            restore_expires: Some(restore_expiry + Duration::from_secs(60)),
            ..Default::default()
        };
        let opts = ObjectOptions {
            versioned: true,
            transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                status: TRANSITION_COMPLETE.to_string(),
                tier: "tier".to_string(),
                etag: "etag".to_string(),
                expected_data_dir: Some(data_dir),
                expected_remote_name: "remote-object".to_string(),
                restore_expiry,
                expire_restored: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let err = check_object_lock_delete(&bootstrap_ctx(), "bucket", "object", &obj_info, &opts)
            .await
            .expect_err("a renewed restored copy must reject the stale expiry task");
        assert!(matches!(err, StorageError::PreconditionFailed));
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

        check_object_lock_delete(&bootstrap_ctx(), "bucket", "object", &obj_info, &opts)
            .await
            .expect("versioned delete marker creation should not delete the locked version");
    }

    // backlog#929 (HP-8): the delete_objects per-object stat is gated on the
    // bucket object-lock configuration. Lock-enabled buckets (either legacy
    // lock_enabled flag or an enabled ObjectLockConfiguration) and unknown
    // metadata must keep the #4297 locked-stat path; only buckets that
    // provably have no Object Lock may skip it.
    #[test]
    fn test_object_lock_delete_check_required_skips_plain_buckets() {
        let bm = crate::bucket::metadata::BucketMetadata::new("plain-bucket");
        assert!(!object_lock_delete_check_required(Some(&bm)));
    }

    #[test]
    fn test_object_lock_delete_check_required_keeps_legacy_lock_enabled_buckets() {
        let mut bm = crate::bucket::metadata::BucketMetadata::new("legacy-lock-bucket");
        bm.lock_enabled = true;
        assert!(object_lock_delete_check_required(Some(&bm)));
    }

    #[test]
    fn test_object_lock_delete_check_required_keeps_object_lock_config_buckets() {
        use s3s::dto::{ObjectLockConfiguration, ObjectLockEnabled};

        let mut bm = crate::bucket::metadata::BucketMetadata::new("lock-config-bucket");
        bm.object_lock_config = Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            ..Default::default()
        });
        assert!(object_lock_delete_check_required(Some(&bm)));
    }

    #[test]
    fn test_object_lock_delete_check_required_fails_closed_without_metadata() {
        assert!(object_lock_delete_check_required(None));
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
        assert!(is_valid_storage_class(storageclass::STANDARD));
        assert!(is_valid_storage_class(storageclass::RRS));
        assert!(!is_valid_storage_class(storageclass::STANDARD_IA));
        assert!(!is_valid_storage_class("INVALID"));
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

        // AWS 2026-04 additional algorithms have no CompletePart field, so they are
        // accepted with no double-check value (verified at UploadPart) — Some(None),
        // NOT the outer None that would fail the multipart completion (#1261).
        for ct in [
            rustfs_rio::ChecksumType::XXHASH3,
            rustfs_rio::ChecksumType::XXHASH64,
            rustfs_rio::ChecksumType::XXHASH128,
            rustfs_rio::ChecksumType::SHA512,
            rustfs_rio::ChecksumType::MD5,
        ] {
            assert_eq!(complete_part_checksum(&CompletePart::default(), ct), Some(None), "{ct:?}");
        }

        // Genuinely unset / invalid types must still be rejected (outer None).
        assert_eq!(complete_part_checksum(&CompletePart::default(), rustfs_rio::ChecksumType::NONE), None);
        assert_eq!(complete_part_checksum(&CompletePart::default(), rustfs_rio::ChecksumType::INVALID), None);
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

        // Bucket-level versioning no longer blocks the inline path (rustfs/backlog#1802):
        // a latest-version read on a versioned bucket is eligible.
        let mut versioned_opts = opts.clone();
        versioned_opts.versioned = true;
        assert!(is_get_small_object_direct_memory_eligible_with_threshold(
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

        // Bucket-level versioning no longer falls back (rustfs/backlog#1802): the
        // latest version on a versioned bucket is served inline like any other.
        let mut versioned_opts = opts.clone();
        versioned_opts.versioned = true;
        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(&None, &object_info, &fi, &versioned_opts, true, 128 * 1024),
            GetDirectMemoryDecision::Use { object_size: 1024 }
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

    async fn inline_bitrot_files_for_payload_with_mode(
        payload: &[u8],
        uses_legacy: bool,
    ) -> (coding::Erasure, Vec<FileInfo>, usize, HashAlgorithm) {
        let erasure = coding::Erasure::new_with_options(4, 2, 1024 * 1024, uses_legacy);
        let read_length = erasure.shard_file_offset(0, payload.len(), payload.len());
        let checksum_algo = if uses_legacy {
            HashAlgorithm::HighwayHash256SLegacy
        } else {
            HashAlgorithm::HighwayHash256S
        };
        let shards = erasure.encode_data(payload).expect("payload should encode");
        let version_id = Some(Uuid::new_v4());
        let data_dir = Some(Uuid::new_v4());
        let mod_time = Some(OffsetDateTime::now_utc());
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
            file.volume = "bucket".to_string();
            file.name = "object".to_string();
            file.size = i64::try_from(payload.len()).expect("test payload should fit i64");
            file.is_latest = true;
            file.version_id = version_id;
            file.data_dir = data_dir;
            file.mod_time = mod_time;
            file.metadata.insert("etag".to_string(), "etag-inline".to_string());
            file.add_object_part(1, "part-etag-inline".to_string(), payload.len(), file.mod_time, file.size, None, None);
            file.set_inline_data();
            file.erasure.index = files.len() + 1;
            file.data = Some(Bytes::from(data));
            files.push(file);
        }

        (erasure, files, read_length, checksum_algo)
    }

    async fn inline_bitrot_files_for_payload(payload: &[u8]) -> (coding::Erasure, Vec<FileInfo>, usize, HashAlgorithm) {
        inline_bitrot_files_for_payload_with_mode(payload, false).await
    }

    fn disk_ordered_fileinfos(files: &[FileInfo]) -> Vec<FileInfo> {
        let distribution = &files
            .first()
            .expect("inline data shard fixture should include metadata")
            .erasure
            .distribution;
        distribution
            .iter()
            .map(|block_index| {
                files
                    .get(block_index.checked_sub(1).expect("erasure block indexes are one-based"))
                    .expect("inline data shard fixture should include every distributed shard")
                    .clone()
            })
            .collect()
    }

    fn inline_data_shard_fileinfo(
        data_blocks: usize,
        parity_blocks: usize,
        erasure_index: usize,
        distribution: &[usize],
        data: Option<&'static [u8]>,
    ) -> FileInfo {
        let mut fi = FileInfo::new("object", data_blocks, parity_blocks);
        fi.name = "object".to_string();
        fi.volume = "bucket".to_string();
        fi.size = 4;
        fi.is_latest = true;
        fi.data_dir = Some(Uuid::nil());
        fi.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
        fi.metadata.insert("etag".to_string(), "etag-inline".to_string());
        fi.add_object_part(1, "part-etag-inline".to_string(), 4, fi.mod_time, 4, None, None);
        fi.set_inline_data();
        fi.erasure.index = erasure_index;
        fi.erasure.distribution = distribution.to_vec();
        fi.data = data.map(Bytes::from_static);
        fi
    }

    #[test]
    fn collect_inline_data_shards_by_index_uses_distribution_order() {
        let distribution = vec![3, 1, 5, 2, 4, 6];
        let mut fi = inline_data_shard_fileinfo(4, 2, 1, &distribution, Some(b"x"));
        fi.erasure.index = 1;
        fi.erasure.distribution = distribution.clone();
        let files = vec![
            inline_data_shard_fileinfo(4, 2, 3, &distribution, Some(b"c")),
            inline_data_shard_fileinfo(4, 2, 1, &distribution, Some(b"a")),
            inline_data_shard_fileinfo(4, 2, 5, &distribution, Some(b"p")),
            inline_data_shard_fileinfo(4, 2, 2, &distribution, Some(b"b")),
            inline_data_shard_fileinfo(4, 2, 4, &distribution, Some(b"d")),
            inline_data_shard_fileinfo(4, 2, 6, &distribution, Some(b"q")),
        ];

        let data_files =
            collect_inline_data_shard_fileinfos_by_index(&files, &fi, 4, |_| true).expect("all data shards should be collected");

        assert_eq!(
            data_files
                .iter()
                .map(|file| file.data.as_deref().expect("fixture carries inline bytes"))
                .collect::<Vec<_>>(),
            [b"a".as_slice(), b"b".as_slice(), b"c".as_slice(), b"d".as_slice()]
        );
    }

    #[test]
    fn collect_inline_data_shards_by_index_rejects_missing_data_shard() {
        let distribution = vec![1, 2, 3, 4];
        let mut fi = inline_data_shard_fileinfo(2, 2, 1, &distribution, Some(b"x"));
        fi.erasure.index = 1;
        fi.erasure.distribution = distribution.clone();
        let files = vec![
            inline_data_shard_fileinfo(2, 2, 1, &distribution, Some(b"a")),
            inline_data_shard_fileinfo(2, 2, 2, &distribution, None),
            inline_data_shard_fileinfo(2, 2, 3, &distribution, Some(b"p")),
            inline_data_shard_fileinfo(2, 2, 4, &distribution, Some(b"q")),
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
    async fn inline_data_shards_direct_read_reassembles_legacy_payload_with_padding() {
        let payload = b"legacy inline payload whose size is not divisible by the data shard count";
        let (erasure, files, read_length, checksum_algo) = inline_bitrot_files_for_payload_with_mode(payload, true).await;
        assert_ne!(payload.len() % erasure.data_shards, 0, "test payload must exercise EC padding");
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
        .expect("legacy inline bitrot readers should build");

        let body = try_read_inline_data_shards_direct(&mut readers, erasure.data_shards, read_length, payload.len())
            .await
            .expect("legacy data shard direct read should succeed");

        assert_eq!(body.len(), payload.len());
        assert_eq!(body.as_ref(), payload);
    }

    #[tokio::test]
    async fn inline_data_shards_direct_read_rejects_corrupt_shard() {
        let payload = b"small inline object payload that will be corrupted";
        let (erasure, mut files, read_length, checksum_algo) = inline_bitrot_files_for_payload(payload).await;
        let second = files[1].data.as_mut().expect("second shard should exist");
        let mut corrupted = second.to_vec();
        let last = corrupted.last_mut().expect("encoded shard should not be empty");
        *last ^= 0xff;
        *second = Bytes::from(corrupted);

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

        assert!(body.is_none(), "a later corrupt shard must discard the already-appended body prefix");
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
        let fi = files[0].clone();
        let disk_files = disk_ordered_fileinfos(&files);

        let disks = vec![Some(disk); erasure.total_shard_count()];
        let metrics_size_bucket = rustfs_io_metrics::get_object_size_bucket(fi.size);

        let body = SetDisks::try_get_object_direct_data_shards_with_fileinfo(
            "bucket",
            "object",
            Arc::new(ErasureCache::new()),
            &fi,
            &disk_files,
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
    async fn direct_memory_versioned_bucket_uses_inline_data_shards_for_latest() {
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

        let payload = vec![b'v'; 64 * 1024];
        let payload_size = i64::try_from(payload.len()).expect("test payload size should fit i64");
        let (erasure, files, _read_length, _checksum_algo) = inline_bitrot_files_for_payload(&payload).await;
        let fi = files[0].clone();
        let disk_files = disk_ordered_fileinfos(&files);

        let mut object_info = ObjectInfo {
            size: payload_size,
            actual_size: payload_size,
            parts: Arc::new(vec![ObjectPartInfo {
                number: 1,
                size: payload.len(),
                actual_size: payload_size,
                ..Default::default()
            }]),
            ..Default::default()
        };
        object_info.inlined = true;
        let opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };
        let metrics_size_bucket = rustfs_io_metrics::get_object_size_bucket(fi.size);

        assert_eq!(
            get_small_object_direct_memory_decision_with_threshold(&None, &object_info, &fi, &opts, true, 128 * 1024),
            GetDirectMemoryDecision::Use {
                object_size: payload.len()
            }
        );

        let body = SetDisks::try_get_object_direct_data_shards_with_fileinfo(
            "bucket",
            "object",
            Arc::new(ErasureCache::new()),
            &fi,
            &disk_files,
            &vec![Some(disk); erasure.total_shard_count()],
            true,
            GET_CODEC_STREAMING_OBJECT_CLASS_PLAIN_SINGLE_PART,
            metrics_size_bucket,
        )
        .await
        .expect("versioned latest direct-memory read should not fail")
        .expect("versioned latest should use inline data shards");

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
            Arc::new(ErasureCache::new()),
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
                Arc::new(ErasureCache::new()),
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
                Arc::new(ErasureCache::new()),
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
        make_local_bucket_test_set_disks_with_drive_count(2).await
    }

    fn assert_exclusive_object_lock_held(set_disks: &SetDisks, bucket: &str, object: &str) {
        let lock = set_disks
            .local_lock_manager_for_test()
            .get_lock_info(&ObjectKey::new(bucket, object))
            .expect("object lock should be visible while rename is paused");
        assert!(matches!(lock.mode, rustfs_lock::LockMode::Exclusive));
        assert_eq!(lock.owner.as_ref(), set_disks.locker_owner.as_str());
    }

    async fn make_local_bucket_test_set_disks_with_drive_count(drive_count: usize) -> Arc<SetDisks> {
        let format = FormatV3::new(1, drive_count);
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();

        for disk_idx in 0..drive_count {
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

        let instance_ctx = Arc::new(InstanceContext::new());
        instance_ctx.update_erasure_type(SetupType::Erasure).await;
        let set_disks = SetDisks::new_with_instance_ctx(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            drive_count,
            drive_count / 2,
            0,
            0,
            endpoints,
            format,
            Vec::new(),
            instance_ctx,
        )
        .await;
        set_disks.set_test_storage_class_config(
            storageclass::lookup_config_for_pools_without_env(&rustfs_config::server_config::KVS::new(), &[drive_count])
                .expect("test storage class should resolve for the local drive count"),
        );
        set_disks
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
    async fn set_level_put_get_delete_restores_large_object_and_fails_closed_after_delete() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-object-roundtrip";
        let object = "nested/object.bin";
        let payload = (0..(BLOCK_SIZE_V2 + 17)).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let opts = ObjectOptions {
            no_lock: true,
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))),
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(payload.clone());
        let written = set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("large object should be written");

        assert_eq!(written.size, payload.len() as i64);
        let mut get_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("large object reader should open");
        let mut restored = Vec::new();
        get_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("large object should stream");
        assert_eq!(restored, payload);

        let deleted = set_disks
            .delete_object(bucket, object, opts.clone())
            .await
            .expect("object delete should succeed");
        assert_eq!(deleted.name, object);

        let err = match set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
        {
            Ok(_) => panic!("deleted object must not be readable"),
            Err(err) => err,
        };
        assert!(
            is_err_object_not_found(&err),
            "deleted object read must fail closed with object-not-found, got {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn streaming_get_blocks_concurrent_overwrite_until_eof() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("true"))], async {
            let set_disks = make_local_bucket_test_set_disks().await;
            let bucket = "snapshot-streaming-overwrite";
            let object = "object";
            let old_body = vec![0x41; 2 * 1024 * 1024];
            let new_body = vec![0x42; old_body.len()];
            let opts = ObjectOptions::default();

            set_disks
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created");
            let mut old_reader = PutObjReader::from_vec(old_body.clone());
            set_disks
                .put_object(bucket, object, &mut old_reader, &opts)
                .await
                .expect("old object should be written");

            let mut snapshot = set_disks
                .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
                .await
                .expect("snapshot reader should open");
            let overwrite_set = Arc::clone(&set_disks);
            let overwrite_body = new_body.clone();
            let overwrite_opts = opts.clone();
            let mut overwrite = tokio::spawn(async move {
                let mut reader = PutObjReader::from_vec(overwrite_body);
                overwrite_set.put_object(bucket, object, &mut reader, &overwrite_opts).await
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut overwrite)
                    .await
                    .is_err(),
                "overwrite must wait while the response body retains the read lock"
            );

            let mut restored = Vec::new();
            snapshot
                .stream
                .read_to_end(&mut restored)
                .await
                .expect("stream should remain readable");
            assert_eq!(restored, old_body);
            tokio::time::timeout(Duration::from_secs(5), overwrite)
                .await
                .expect("overwrite should proceed after EOF")
                .expect("overwrite task should join")
                .expect("overwrite should succeed");

            let mut latest = set_disks
                .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
                .await
                .expect("latest reader should open");
            let mut latest_body = Vec::new();
            latest
                .stream
                .read_to_end(&mut latest_body)
                .await
                .expect("latest object should remain readable");
            assert_eq!(latest_body, new_body);

            let cancelled = set_disks
                .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
                .await
                .expect("cancelled reader should open");
            drop(cancelled);
            let mut replacement = PutObjReader::from_vec(vec![0x43; 2 * 1024 * 1024]);
            tokio::time::timeout(Duration::from_secs(5), set_disks.put_object(bucket, object, &mut replacement, &opts))
                .await
                .expect("reader drop must release its read lock")
                .expect("replacement after cancellation should succeed");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn streaming_get_blocks_concurrent_delete_until_eof() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("true"))], async {
            let set_disks = make_local_bucket_test_set_disks().await;
            let bucket = "snapshot-streaming-delete";
            let object = "object";
            let body = vec![0x41; 2 * 1024 * 1024];
            let opts = ObjectOptions {
                object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                    ObjectLockConfigState::ConfirmedAbsent,
                ))),
                ..Default::default()
            };

            set_disks
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created");
            let mut reader = PutObjReader::from_vec(body.clone());
            set_disks
                .put_object(bucket, object, &mut reader, &opts)
                .await
                .expect("object should be written");

            let mut snapshot = set_disks
                .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
                .await
                .expect("snapshot reader should open");
            let delete_set = Arc::clone(&set_disks);
            let delete_opts = opts.clone();
            let mut delete = tokio::spawn(async move { delete_set.delete_object(bucket, object, delete_opts).await });
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut delete).await.is_err(),
                "delete must wait while the response body retains the read lock"
            );

            let mut restored = Vec::new();
            snapshot
                .stream
                .read_to_end(&mut restored)
                .await
                .expect("stream should remain readable before delete");
            assert_eq!(restored, body);
            tokio::time::timeout(Duration::from_secs(30), delete)
                .await
                .expect("delete should proceed after EOF")
                .expect("delete task should join")
                .expect("delete should succeed");
            let err = match set_disks
                .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
                .await
            {
                Ok(_) => panic!("a new read must not observe the deleted object"),
                Err(err) => err,
            };
            assert!(is_err_object_not_found(&err));
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn streaming_get_blocks_concurrent_delete_objects_until_eof() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("true"))], async {
            let set_disks = make_local_bucket_test_set_disks().await;
            let bucket = "snapshot-streaming-delete-objects";
            let object = "object";
            let body = vec![0x41; 2 * 1024 * 1024];
            let opts = ObjectOptions {
                object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                    ObjectLockConfigState::ConfirmedAbsent,
                ))),
                ..Default::default()
            };

            set_disks
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created");
            let mut reader = PutObjReader::from_vec(body.clone());
            set_disks
                .put_object(bucket, object, &mut reader, &opts)
                .await
                .expect("object should be written");

            let mut snapshot = set_disks
                .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
                .await
                .expect("snapshot reader should open");
            let delete_set = Arc::clone(&set_disks);
            let delete_opts = opts.clone();
            let mut delete = tokio::spawn(async move {
                delete_set
                    .delete_objects(
                        bucket,
                        vec![ObjectToDelete {
                            object_name: object.to_string(),
                            ..Default::default()
                        }],
                        delete_opts,
                    )
                    .await
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut delete).await.is_err(),
                "batch delete must wait while the response body retains the read lock"
            );

            let mut restored = Vec::new();
            snapshot
                .stream
                .read_to_end(&mut restored)
                .await
                .expect("stream should remain readable before batch delete");
            assert_eq!(restored, body);
            let (_, errors) = tokio::time::timeout(Duration::from_secs(30), delete)
                .await
                .expect("batch delete should proceed after EOF")
                .expect("batch delete task should join");
            assert!(errors.iter().all(Option::is_none));
        })
        .await;
    }

    #[tokio::test]
    async fn set_level_batched_large_put_get_restores_body() {
        const BATCHED_LARGE_SIZE: usize = 64 * 1024 * 1024;

        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-batched-large-object";
        let object = "large/batched.bin";
        let payload = (0..BATCHED_LARGE_SIZE)
            .map(|idx| ((idx as u64).wrapping_mul(31).wrapping_add(17) % 251) as u8)
            .collect::<Vec<_>>();
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(payload.clone());
        let written = set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("batched-large object should be written");
        assert_eq!(written.size, payload.len() as i64);

        let mut get_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("batched-large object reader should open");
        let mut restored = Vec::with_capacity(payload.len());
        get_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("batched-large object should stream");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    async fn set_level_put_object_fails_closed_when_writer_quorum_is_unavailable() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-put-writer-quorum";
        let object = "object.txt";
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created before disk loss");
        {
            let mut disks = set_disks.disks.write().await;
            disks[1] = None;
        }

        let mut reader = PutObjReader::from_vec(b"quorum guarded body".to_vec());
        let err = set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect_err("missing writer quorum must fail the put");
        assert!(
            matches!(err, Error::ErasureWriteQuorum | Error::InsufficientWriteQuorum(_, _)),
            "expected write quorum failure, got {err:?}"
        );

        let read_err = match set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
        {
            Ok(_) => panic!("failed put must not leave a readable object"),
            Err(err) => err,
        };
        assert!(
            is_err_object_not_found(&read_err) || matches!(read_err, Error::ErasureReadQuorum),
            "failed put must fail closed on read, got {read_err:?}"
        );
    }

    #[tokio::test]
    async fn set_level_put_object_short_reader_fails_closed_across_write_paths() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-put-short-reader";
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let cases = [
            ("single-block", b"short body".to_vec(), 32),
            ("pipeline", b"pipeline short body".to_vec(), BLOCK_SIZE_V2 as i64 + 1),
            ("batched-large", b"batched short body".to_vec(), 64 * 1024 * 1024),
        ];

        for (name, payload, declared_size) in cases {
            let object = format!("{name}/object.txt");
            let hash_reader = HashReader::from_stream(Cursor::new(payload), declared_size, declared_size, None, None, false)
                .expect("test reader should be constructed");
            let mut reader = PutObjReader::new(hash_reader);
            let err = set_disks
                .put_object(bucket, &object, &mut reader, &opts)
                .await
                .expect_err("short reader must fail the put");
            let err_text = format!("{err:?}");
            assert!(
                err_text.contains("UnexpectedEof") || err_text.contains("IncompleteBody"),
                "{name} short reader should fail with an EOF/incomplete-body error, got {err:?}"
            );

            let read_err = match set_disks
                .get_object_reader(bucket, &object, None, HeaderMap::new(), &opts)
                .await
            {
                Ok(_) => panic!("{name} short failed put must not leave a readable object"),
                Err(err) => err,
            };
            assert!(
                is_err_object_not_found(&read_err) || matches!(read_err, Error::ErasureReadQuorum),
                "{name} short failed put must fail closed on read, got {read_err:?}"
            );
        }
    }

    #[tokio::test]
    async fn set_level_get_restores_body_when_one_shard_is_missing_after_write() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-read-repair-missing-shard";
        let object = "object.bin";
        let payload = (0..(BLOCK_SIZE_V2 + 211))
            .map(|idx| ((idx * 13) % 251) as u8)
            .collect::<Vec<_>>();
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(payload.clone());
        set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("object should be written before shard loss");

        {
            let mut disks = set_disks.disks.write().await;
            disks[1] = None;
        }

        let mut get_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("object should remain readable with one missing shard");
        let mut restored = Vec::new();
        get_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("degraded read should stream");

        assert_eq!(restored, payload);
    }

    #[tokio::test]
    async fn set_level_overwrite_restores_new_body_and_cleans_old_data_dir() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-object-overwrite";
        let object = "nested/object.bin";
        let first_payload = (0..(BLOCK_SIZE_V2 + 31)).map(|idx| (idx % 239) as u8).collect::<Vec<_>>();
        let second_payload = (0..(BLOCK_SIZE_V2 + 97))
            .map(|idx| ((idx * 7) % 251) as u8)
            .collect::<Vec<_>>();
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut first_reader = PutObjReader::from_vec(first_payload);
        set_disks
            .put_object(bucket, object, &mut first_reader, &opts)
            .await
            .expect("first object body should be written");

        let mut second_reader = PutObjReader::from_vec(second_payload.clone());
        let second = set_disks
            .put_object(bucket, object, &mut second_reader, &opts)
            .await
            .expect("overwrite body should commit and clean the old data dir");

        assert_eq!(second.size, second_payload.len() as i64);
        let mut get_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("overwritten object reader should open");
        let mut restored = Vec::new();
        get_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("overwritten object should stream");
        assert_eq!(restored, second_payload);
    }

    #[tokio::test]
    async fn set_level_write_preconditions_fail_closed_and_allow_matching_etags() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-write-preconditions";
        let object = "object.txt";
        let write_opts = ObjectOptions {
            no_lock: true,
            preserve_etag: Some("conditional-etag".to_string()),
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(b"conditional body".to_vec());
        set_disks
            .put_object(bucket, object, &mut reader, &write_opts)
            .await
            .expect("object should be written with deterministic etag");

        let reject_existing = ObjectOptions {
            http_preconditions: Some(HTTPPreconditions {
                if_none_match: Some("conditional-etag".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(matches!(
            set_disks.check_write_precondition(bucket, object, &reject_existing).await,
            Some(StorageError::PreconditionFailed)
        ));

        let allow_matching = ObjectOptions {
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some("\"conditional-etag\"".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            set_disks
                .check_write_precondition(bucket, object, &allow_matching)
                .await
                .is_none()
        );

        let missing_if_match = ObjectOptions {
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some("missing-etag".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(matches!(
            set_disks
                .check_write_precondition(bucket, "missing-object.txt", &missing_if_match)
                .await,
            Some(StorageError::ObjectNotFound(_, _))
        ));
    }

    #[tokio::test]
    async fn conditional_replace_holds_object_lock_through_rename() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-conditional-replace-fence";
        let object = "config/conditional-replace.json";
        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let mut initial_reader = PutObjReader::from_vec(b"initial config".to_vec());
        let initial = set_disks
            .put_object(
                bucket,
                object,
                &mut initial_reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("initial config should be written");
        let initial_etag = initial.etag.expect("initial config should have an ETag");

        let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
        let writer_store = set_disks.clone();
        let expected_etag = initial_etag.clone();
        let writer = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(b"replacement config".to_vec());
            writer_store
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        preserve_etag: Some("replacement-etag".to_string()),
                        http_preconditions: Some(HTTPPreconditions {
                            if_match: Some(expected_etag),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(30), barrier.wait_until_paused())
            .await
            .expect("conditional replace should reach the rename barrier");
        assert_exclusive_object_lock_held(&set_disks, bucket, object);
        barrier.release();
        writer
            .await
            .expect("conditional writer task should finish")
            .expect("matching conditional replace should commit");
        assert!(
            set_disks
                .local_lock_manager_for_test()
                .get_lock_info(&ObjectKey::new(bucket, object))
                .is_none(),
            "conditional replace should release the object lock after commit"
        );
        let contender = set_disks
            .new_ns_lock(bucket, object)
            .await
            .expect("contender namespace lock should be created");
        let contender_guard = contender
            .get_write_lock(std::time::Duration::from_secs(30))
            .await
            .expect("contender should acquire after conditional replace commits");
        drop(contender_guard);

        let mut stale_reader = PutObjReader::from_vec(b"stale config".to_vec());
        let err = set_disks
            .put_object(
                bucket,
                object,
                &mut stale_reader,
                &ObjectOptions {
                    http_preconditions: Some(HTTPPreconditions {
                        if_match: Some(initial_etag),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect_err("the old ETag must fail after the fenced replacement commits");
        assert_eq!(err, StorageError::PreconditionFailed);
    }

    #[tokio::test]
    async fn repeated_body_write_keeps_etag_but_changes_data_dir_generation() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-write-generation";
        let object = "config/write-generation.json";
        let body = b"identical config body".to_vec();
        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let mut first_reader = PutObjReader::from_vec(body.clone());
        let first = set_disks
            .put_object(
                bucket,
                object,
                &mut first_reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("first config body should be written");
        let mut second_reader = PutObjReader::from_vec(body);
        let second = set_disks
            .put_object(
                bucket,
                object,
                &mut second_reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("identical config body should be rewritten");

        assert_eq!(first.etag, second.etag, "content ETag should expose the ABA collision");
        assert_ne!(first.data_dir, second.data_dir, "each committed body write needs a unique generation");
        assert!(first.data_dir.is_some() && second.data_dir.is_some());
    }

    #[tokio::test]
    async fn conditional_create_holds_object_lock_through_rename() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-conditional-create-fence";
        let object = "config/conditional-create.json";
        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
        let writer_store = set_disks.clone();
        let writer = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(b"created config".to_vec());
            writer_store
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        http_preconditions: Some(HTTPPreconditions {
                            if_none_match: Some("*".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(30), barrier.wait_until_paused())
            .await
            .expect("conditional create should reach the rename barrier");
        assert_exclusive_object_lock_held(&set_disks, bucket, object);
        barrier.release();
        writer
            .await
            .expect("conditional writer task should finish")
            .expect("first conditional create should commit");
        assert!(
            set_disks
                .local_lock_manager_for_test()
                .get_lock_info(&ObjectKey::new(bucket, object))
                .is_none(),
            "conditional create should release the object lock after commit"
        );
        let contender = set_disks
            .new_ns_lock(bucket, object)
            .await
            .expect("contender namespace lock should be created");
        let contender_guard = contender
            .get_write_lock(std::time::Duration::from_secs(30))
            .await
            .expect("contender should acquire after conditional create commits");
        drop(contender_guard);

        let mut duplicate_reader = PutObjReader::from_vec(b"duplicate config".to_vec());
        let err = set_disks
            .put_object(
                bucket,
                object,
                &mut duplicate_reader,
                &ObjectOptions {
                    http_preconditions: Some(HTTPPreconditions {
                        if_none_match: Some("*".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a second create-only write must not replace the committed config");
        assert_eq!(err, StorageError::PreconditionFailed);
    }

    #[tokio::test]
    async fn set_level_if_none_match_fails_closed_without_read_quorum() {
        let set_disks = make_local_bucket_test_set_disks_with_drive_count(4).await;
        let bucket = "bucket-write-precondition-quorum";
        let object = "existing-object.txt";

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created before disk loss");
        let mut reader = PutObjReader::from_vec(b"existing object body".to_vec());
        set_disks
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("object should be written before disk loss");
        {
            let mut disks = set_disks.disks.write().await;
            disks[1..].fill(None);
        }

        let create_only = ObjectOptions {
            http_preconditions: Some(HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let result = set_disks.check_write_precondition(bucket, object, &create_only).await;
        assert!(
            matches!(result, Some(StorageError::ErasureReadQuorum | StorageError::InsufficientReadQuorum(_, _))),
            "expected read-quorum failure, got {result:?}"
        );
    }

    #[tokio::test]
    async fn set_level_versioned_delete_marker_hides_object_without_corrupting_version_metadata() {
        let set_disks = make_local_bucket_test_set_disks_with_drive_count(4).await;
        let bucket = "bucket-versioned-delete";
        let object = "object.txt";
        let opts = ObjectOptions {
            no_lock: true,
            versioned: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(b"versioned object body".to_vec());
        let written = set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("versioned object should be written");
        assert!(written.version_id.is_some());

        let marker = set_disks
            .delete_object(bucket, object, opts.clone())
            .await
            .expect("versioned delete should create a marker");

        assert!(marker.delete_marker);
        let marker_version = marker.version_id.expect("versioned delete marker should carry a version id");
        let create_only = ObjectOptions {
            versioned: true,
            data_movement: true,
            http_preconditions: Some(HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            set_disks.check_write_precondition(bucket, object, &create_only).await,
            Some(StorageError::PreconditionFailed),
            "data movement must not replace a target delete marker"
        );
        let err = match set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
        {
            Ok(_) => panic!("latest delete marker must hide object body"),
            Err(err) => err,
        };
        assert!(
            is_err_object_not_found(&err),
            "latest delete marker read must map to ObjectNotFound, got {err:?}"
        );

        let versioned_opts = ObjectOptions {
            version_id: Some(marker_version.to_string()),
            ..opts.clone()
        };
        let err = match set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &versioned_opts)
            .await
        {
            Ok(_) => panic!("explicit delete marker version must not expose a body"),
            Err(err) => err,
        };
        assert!(
            matches!(err, Error::MethodNotAllowed),
            "explicit delete marker version read must map to MethodNotAllowed, got {err:?}"
        );
    }

    #[tokio::test]
    async fn set_level_metadata_self_copy_preserves_body_and_updates_metadata() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-metadata-copy";
        let object = "object.txt";
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let payload = b"metadata copy must not lose committed bytes".to_vec();

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(payload.clone());
        set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("object should be written");

        let mut source = set_disks
            .get_object_info(bucket, object, &opts)
            .await
            .expect("object info should be readable");
        let mut metadata = (*source.user_defined).clone();
        metadata.insert("x-amz-meta-copy-check".to_string(), "present".to_string());
        source.user_defined = Arc::new(metadata);
        source.metadata_only = true;
        source.etag = Some("metadata-copy-etag".to_string());

        let copied = set_disks
            .copy_object(bucket, object, bucket, object, &mut source, &opts, &opts)
            .await
            .expect("metadata self-copy should succeed");
        assert_eq!(copied.user_defined.get("x-amz-meta-copy-check").map(String::as_str), Some("present"));

        let mut get_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("copied object reader should open");
        let mut restored = Vec::new();
        get_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("copied object should stream");
        assert_eq!(restored, payload);

        let reread = set_disks
            .get_object_info(bucket, object, &opts)
            .await
            .expect("copied object info should be readable");
        assert_eq!(reread.user_defined.get("x-amz-meta-copy-check").map(String::as_str), Some("present"));
    }

    #[tokio::test]
    async fn set_level_empty_object_read_uses_buffered_empty_body_with_locks() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-empty-object";
        let object = "empty.bin";
        let opts = ObjectOptions::default();

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(Vec::new());
        let written = set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("empty object should be written");
        assert_eq!(written.size, 0);

        let mut get_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("empty object reader should open");
        assert_eq!(get_reader.object_info.size, 0);
        assert_eq!(get_reader.buffered_body.as_ref().map(Bytes::len), Some(0));
        let mut restored = Vec::new();
        get_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("empty object should stream");
        assert!(restored.is_empty());

        let info = set_disks
            .get_object_info(bucket, object, &opts)
            .await
            .expect("empty object info should be readable");
        assert_eq!(info.size, 0);
    }

    #[tokio::test]
    async fn set_level_put_object_options_preserve_etag_and_normalize_standard_storage_class() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-put-options";
        let object = "object.txt";
        let mod_time = OffsetDateTime::from_unix_timestamp(1_717_171_717).expect("fixed timestamp should parse");
        let mut user_defined = HashMap::new();
        user_defined.insert(AMZ_STORAGE_CLASS.to_string(), storageclass::STANDARD.to_string());
        user_defined.insert(SUFFIX_COMPRESSION.to_string(), "zstd".to_string());
        let mut eval_metadata = HashMap::new();
        eval_metadata.insert("x-amz-meta-evaluated".to_string(), "yes".to_string());
        let opts = ObjectOptions {
            mod_time: Some(mod_time),
            preserve_etag: Some("preserved-etag".to_string()),
            user_defined,
            eval_metadata: Some(eval_metadata),
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(b"option matrix body".to_vec());
        let written = set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("object should be written with option matrix");
        assert_eq!(written.etag.as_deref(), Some("preserved-etag"));
        assert_eq!(written.mod_time, Some(mod_time));
        assert_eq!(written.user_defined.get("x-amz-meta-evaluated").map(String::as_str), Some("yes"));
        assert!(!written.user_defined.contains_key(AMZ_STORAGE_CLASS));

        let info = set_disks
            .get_object_info(bucket, object, &opts)
            .await
            .expect("object info should be readable");
        assert_eq!(info.etag.as_deref(), Some("preserved-etag"));
        assert_eq!(info.mod_time, Some(mod_time));
        assert_eq!(info.user_defined.get("x-amz-meta-evaluated").map(String::as_str), Some("yes"));
        assert!(!info.user_defined.contains_key(AMZ_STORAGE_CLASS));
    }

    #[tokio::test]
    async fn set_level_put_object_metadata_updates_headers_without_rewriting_body() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-put-metadata";
        let object = "object.txt";
        let payload = b"metadata update must preserve bytes".to_vec();
        let write_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(payload.clone());
        set_disks
            .put_object(bucket, object, &mut reader, &write_opts)
            .await
            .expect("object should be written");

        let mut eval_metadata = HashMap::new();
        eval_metadata.insert("x-amz-meta-updated".to_string(), "true".to_string());
        let update_time = OffsetDateTime::from_unix_timestamp(1_717_181_818).expect("fixed timestamp should parse");
        let update_opts = ObjectOptions {
            eval_metadata: Some(eval_metadata),
            mod_time: Some(update_time),
            ..Default::default()
        };
        let updated = set_disks
            .put_object_metadata(bucket, object, &update_opts)
            .await
            .expect("metadata update should succeed");
        assert_eq!(updated.mod_time, Some(update_time));
        assert_eq!(updated.user_defined.get("x-amz-meta-updated").map(String::as_str), Some("true"));

        let mut get_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &write_opts)
            .await
            .expect("updated object reader should open");
        let mut restored = Vec::new();
        get_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("updated object should stream");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    async fn set_level_copy_object_with_prefetched_reader_restores_body() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-copy-reader";
        let object = "object.txt";
        let payload = b"copy reader body".to_vec();
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut source = ObjectInfo {
            metadata_only: false,
            put_object_reader: Some(PutObjReader::from_vec(payload.clone())),
            ..Default::default()
        };
        let copied = set_disks
            .copy_object(bucket, object, bucket, object, &mut source, &opts, &opts)
            .await
            .expect("copy with prefetched reader should write object data");
        assert_eq!(copied.size, payload.len() as i64);

        let mut get_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("copied reader should open");
        let mut restored = Vec::new();
        get_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("copied object should stream");
        assert_eq!(restored, payload);
    }

    /// The other half of the suspended-versioning delete contract: a client
    /// that drains such a bucket lists the null delete marker and then purges
    /// it as `?versionId=null`, which is what `nuke_bucket` does before
    /// `DeleteBucket`. That purge must succeed — if it is rejected the marker's
    /// `xl.meta` survives, and `DeleteBucket`'s raw disk scan then reports
    /// `BucketNotEmpty` for a bucket the client has already emptied.
    #[tokio::test]
    async fn set_level_explicit_null_version_delete_purges_the_null_delete_marker() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-null-marker-purge";
        let object = "object.txt";
        // The delete path reads versioned/suspended from the bucket-config
        // snapshot, not from `opts`, so inject a real Suspended config —
        // otherwise `from_file_info` never synthesizes the null version id and
        // the branch under test is not reached.
        let suspended = crate::bucket::replication::DeleteReplicationConfigSnapshot::from_configs_for_test(
            s3s::dto::VersioningConfiguration {
                status: Some(s3s::dto::BucketVersioningStatus::from_static(s3s::dto::BucketVersioningStatus::SUSPENDED)),
                ..Default::default()
            },
            None,
        );
        let opts = ObjectOptions {
            no_lock: true,
            version_suspended: true,
            delete_replication_config_snapshot: Some(Arc::new(suspended)),
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))),
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(b"suspended version body".to_vec());
        set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("suspended-version object should be written");

        let marker = set_disks
            .delete_object(bucket, object, opts.clone())
            .await
            .expect("version-suspended delete should create a null marker");
        assert!(marker.delete_marker);
        assert_eq!(marker.version_id, Some(Uuid::nil()));

        let (_deleted, errs) = set_disks
            .delete_objects(
                bucket,
                vec![ObjectToDelete {
                    object_name: object.to_string(),
                    version_id: Some(Uuid::nil()),
                    ..Default::default()
                }],
                opts.clone(),
            )
            .await;

        assert!(
            errs.iter().all(Option::is_none),
            "explicit null-version purge of the null delete marker must succeed, got {errs:?}"
        );
    }

    #[tokio::test]
    async fn set_level_version_suspended_delete_creates_null_delete_marker() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-version-suspended-delete";
        let object = "object.txt";
        let opts = ObjectOptions {
            no_lock: true,
            version_suspended: true,
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))),
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(b"suspended version body".to_vec());
        set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("suspended-version object should be written");

        let marker = set_disks
            .delete_object(bucket, object, opts.clone())
            .await
            .expect("version-suspended delete should create a null marker");
        assert!(marker.delete_marker);
        assert_eq!(marker.version_id, Some(Uuid::nil()));

        let err = match set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
        {
            Ok(_) => panic!("null delete marker must hide object body"),
            Err(err) => err,
        };
        assert!(
            is_err_object_not_found(&err),
            "null delete marker read must map to ObjectNotFound, got {err:?}"
        );
    }

    #[tokio::test]
    async fn set_level_delete_prefix_removes_nested_objects() {
        let set_disks = make_local_bucket_test_set_disks().await;
        let bucket = "bucket-delete-prefix";
        let object = "prefix/object.txt";
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(b"prefix body".to_vec());
        set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("prefix object should be written");

        set_disks
            .delete_object(
                bucket,
                "prefix/",
                ObjectOptions {
                    delete_prefix: true,
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("prefix delete should succeed");

        let err = match set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
        {
            Ok(_) => panic!("object under deleted prefix must not be readable"),
            Err(err) => err,
        };
        assert!(
            is_err_object_not_found(&err),
            "prefix-deleted object must fail closed with object-not-found, got {err:?}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn set_level_delete_objects_batch_removes_live_data_and_tolerates_missing_keys() {
        temp_env::async_with_vars([("RUSTFS_ISSUE3031_DIAG_ENABLE", Some("true"))], async {
            let set_disks = make_local_bucket_test_set_disks().await;
            let bucket = "bucket-delete-objects";
            let existing = "existing.txt";
            let missing = "missing.txt";
            let opts = ObjectOptions {
                no_lock: true,
                object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                    ObjectLockConfigState::ConfirmedAbsent,
                ))),
                ..Default::default()
            };

            set_disks
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created");
            let mut reader = PutObjReader::from_vec(b"batch delete body".to_vec());
            set_disks
                .put_object(bucket, existing, &mut reader, &opts)
                .await
                .expect("object should be written before batch delete");

            let (deleted, errors) = set_disks
                .delete_objects(
                    bucket,
                    vec![
                        ObjectToDelete {
                            object_name: existing.to_string(),
                            ..Default::default()
                        },
                        ObjectToDelete {
                            object_name: missing.to_string(),
                            ..Default::default()
                        },
                    ],
                    opts.clone(),
                )
                .await;

            assert_eq!(deleted.len(), 2);
            assert_eq!(errors.len(), 2);
            assert!(errors.iter().all(Option::is_none));
            assert_eq!(deleted[0].object_name, existing);
            assert!(deleted[0].found);
            assert!(!deleted[0].delete_marker);
            assert_eq!(deleted[1].object_name, missing);
            assert!(!deleted[1].found);
            assert!(!deleted[1].delete_marker);

            let err = match set_disks
                .get_object_reader(bucket, existing, None, HeaderMap::new(), &opts)
                .await
            {
                Ok(_) => panic!("batch-deleted object must not be readable as latest"),
                Err(err) => err,
            };
            assert!(
                is_err_object_not_found(&err),
                "batch-deleted object must fail closed with object-not-found, got {err:?}"
            );
        })
        .await;
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

        set_disks
            .check_abandoned_parts(
                "bucket",
                "object",
                &HealOpts {
                    dry_run: true,
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("abandoned-parts check should be callable on empty disk sets");
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

    #[test]
    fn adaptive_duplex_buffer_size_raises_mid_sized_gets_without_penalizing_tiny_objects() {
        assert_eq!(adaptive_duplex_buffer_size(64 * 1024), 64 * 1024);
        assert_eq!(adaptive_duplex_buffer_size(128 * 1024), 64 * 1024);
        assert_eq!(adaptive_duplex_buffer_size(256 * 1024), 256 * 1024);
        assert_eq!(adaptive_duplex_buffer_size(1024 * 1024), 512 * 1024);
        assert_eq!(adaptive_duplex_buffer_size(2 * 1024 * 1024), 1024 * 1024);
    }
}
