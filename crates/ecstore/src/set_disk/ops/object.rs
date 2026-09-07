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

//! `ObjectIO` and `ObjectOperations` storage-api contract impls for `SetDisks`
//! — the core object read/write hot path (P6 of the God-Object split, tracking
//! backlog#815, issue backlog#821). Relocated verbatim from set_disk/mod.rs;
//! the contracts stay implemented `for SetDisks`, so their associated-type
//! bounds are unchanged, and the impls reach shared primitives through the
//! SetDisks core (io_primitives) via inherent calls.

#[cfg(test)]
use super::super::MetadataCacheInvalidationProbe;
use super::super::{
    AMZ_OBJECT_TAGGING, AMZ_STORAGE_CLASS, Arc, AsyncWrite, AtomicU64, BufReader, Bytes, CACHE_CONTROL, CONTENT_DISPOSITION,
    CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_TYPE, CompletePart, Cursor, DeleteAccounting, DeleteOptions, DeletedObject,
    DiskError, DiskStore, EVENT_SET_DISK_COMMIT_TAIL_SLOW, EVENT_SET_DISK_PUT_OBJECT_STAGE_SUMMARY, EVENT_SET_DISK_WRITE,
    EXPIRES, Error, EventArgs, EventName, FastLockGuard, FileInfo, FileInfoVersions,
    GET_CODEC_STREAMING_OBJECT_CLASS_PLAIN_SINGLE_PART, GET_OBJECT_PATH_BODY_CACHE, GET_OBJECT_PATH_CODEC_STREAMING,
    GET_OBJECT_PATH_DIRECT_MEMORY, GET_OBJECT_PATH_EMPTY, GET_OBJECT_PATH_INLINE_DIRECT, GET_OBJECT_PATH_INTERNAL_META,
    GET_OBJECT_PATH_LEGACY_DUPLEX, GET_OBJECT_PATH_REMOTE_TRANSITION, GET_OBJECT_PATH_SET_DISK, GET_STAGE_DECODE, GET_STAGE_EMIT,
    GET_STAGE_INLINE_PREPARE, GET_STAGE_LOCK_ACQUIRE, GET_STAGE_METADATA, GET_STAGE_OBJECT_INFO, GET_STAGE_PATH_DECISION,
    GET_STAGE_READER_SETUP, GenericError, GetCodecStreamingDecision, GetCodecStreamingFallbackReason, GetDirectMemoryDecision,
    GetObjectReader, HTTPRangeSpec, HashAlgorithm, HashMap, HashReader, HashSet, HeaderMap, HealChannelPriority, InstanceContext,
    Instant, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_SET_DISK, OBJECT_OP_IGNORED_ERRS, ObjectApiError, ObjectInfo, ObjectKey,
    ObjectLockConfigSnapshot, ObjectLockConfigState, ObjectOptions, ObjectReader, ObjectToDelete, OffsetDateTime, Ordering, Pin,
    PutObjReader, RUSTFS_META_BUCKET, RUSTFS_META_TMP_BUCKET, ReadPathPlan, ReaderImpl, ReplicateDecision,
    ReplicationObjectBridge, Result, SET_DISK_COMMIT_TAIL_WARN_THRESHOLD_MS, SLASH_SEPARATOR, SUFFIX_ACTUAL_SIZE,
    SUFFIX_COMPRESSION, SUFFIX_COMPRESSION_SIZE, SUFFIX_RESTORE_OPERATION_ID, SUFFIX_RESTORE_WORKER_LOCK, SetDisks,
    SmallWritePath, StorageError, TRANSITION_COMPLETE, UpdateMetadataOpts, Uuid, WriteLayout, X_AMZ_OBJECT_LOCK_LEGAL_HOLD,
    X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, X_AMZ_RESTORE, adaptive_duplex_buffer_size,
    build_get_object_info, build_inline_bitrot_readers, build_inline_bitrot_readers_from_refs, can_try_inline_data_shards_direct,
    check_object_lock_delete, check_object_lock_for_deletion_with_state, check_object_lock_retention_update,
    classify_get_codec_streaming_object_class, classify_put_write_path, classify_storage_error,
    collect_inline_data_shard_fileinfos_by_index, contains_key_str, create_bitrot_writer, debug, delete_file_info_version_id,
    disk, ensure_delete_commit_locks_held, error, explicit_delete_removed_marker, finish_set_disk_read_lock,
    get_codec_streaming_reader_gate_with_plan, get_object_body_cache_hook, get_raw_etag,
    get_small_object_direct_memory_decision_with_threshold_and_plan, get_small_object_direct_memory_threshold,
    get_stage_timer_if_enabled, get_str, get_transitioned_object_reader_with_tier_manager, inline_erasure_shard_file_offset,
    inline_erasure_shard_size, insert_str, is_deadlock_detection_enabled, is_err_object_not_found, is_err_version_not_found,
    is_explicit_null_version, is_get_codec_streaming_base_enabled, is_get_small_object_direct_memory_enabled,
    is_lock_optimization_enabled, issue3031_diag_enabled, join_all, known_put_object_storage_size, path_join_buf,
    put_restore_opts, record_compression_total_memory, record_get_codec_streaming_gate_decision,
    record_get_direct_memory_decision, record_get_object_pipeline_failure, record_get_object_pipeline_failure_for_path,
    record_get_object_reader_path_observation, record_get_stage_duration_if_enabled, record_lock_acquire,
    reduce_write_quorum_errs, release_materialized_read_lock, replication_write_may_pass_worm_gate, require_restore_operation_id,
    resolve_delete_version_state, resolve_tiered_decommission_write_quorum_result, resolve_write_layout,
    restore_commit_operation_id_from_metadata, restore_operation_id_from_metadata, send_event,
    set_disk_delete_creates_delete_marker, should_force_delete_marker_for_missing_version,
    should_persist_encryption_original_size, should_preserve_delete_replication_state, should_use_inline_fast_path_with_plan,
    take_prepared_get_object_metadata, to_object_err, try_read_inline_data_shards_direct, warn,
};
use super::bitrot_self_verify::{BitrotSelfVerifyTarget, drop_failed_writer_disks, verify_written_bitrot_shards};
use crate::api::config::storageclass;
use crate::bucket::lifecycle::bucket_lifecycle_ops::LifecycleOps;
use crate::bucket::utils::is_meta_bucketname;
use crate::bucket::versioning::VersioningApi;
use crate::disk::DiskAPI;
use crate::object_api::ScannerPublicationCommitScopeGuard;
use crate::set_disk::coding;
use crate::set_disk::core::io_primitives::GetCodecStreamingReaderBuildOutcome;
use crate::set_disk::mem;
use crate::set_disk::metadata_sys;
use crate::set_disk::read::{GET_OBJECT_PATH_MID_SIZE_STREAMING, GetObjectDownstreamWriter};
use crate::set_disk::runtime_sources;
use crate::storage_api_contracts::multipart::MultipartOperations;
use crate::storage_api_contracts::object::ObjectIO;
use crate::storage_api_contracts::object::ObjectOperations;
use rustfs_lock::LockManager;
use rustfs_rio::EtagResolvable;
use rustfs_rio::HashReaderMut;
use rustfs_rio::TryGetIndex;
use rustfs_utils::http::HeaderExt;
use tokio::io::AsyncWriteExt;

// Keep the mid-size reader separate from the codec-streaming rollout. The
// latter is intentionally opt-in because its per-stripe worker can regress
// tiny objects; this bounded range is the gap between direct-memory GETs and
// the legacy duplex reader.
const ENV_RUSTFS_GET_MID_SIZE_STREAMING_ENABLE: &str = "RUSTFS_GET_MID_SIZE_STREAMING_ENABLE";
const DEFAULT_RUSTFS_GET_MID_SIZE_STREAMING_ENABLE: bool = true;
const GET_MID_SIZE_STREAMING_MIN_SIZE: usize = 128 * 1024 + 1;
// Exclude 1 MiB from the bounded mid-size reader until it has a demonstrated
// high-concurrency performance envelope; existing codec/legacy gates decide
// which established reader handles the object.
const GET_MID_SIZE_STREAMING_MAX_SIZE: usize = 512 * 1024;
const EVENT_LIFECYCLE_TRANSITION_CLEANUP: &str = "lifecycle_transition_cleanup";
const EVENT_LIFECYCLE_TRANSITIONED_DELETE_CLEANUP_OWNER: &str = "lifecycle_transitioned_delete_cleanup_owner";
const EVENT_LIFECYCLE_RESTORE_CLEANUP: &str = "lifecycle_restore_cleanup";

fn replication_status_writeback_is_current(object_info: &ObjectInfo, condition: &ReplicationStatusWritebackCondition) -> bool {
    let current = object_info.replication_generation_snapshot();
    !condition.expected_generation.invalid && !current.invalid && current == condition.expected_generation
}

/// Merge storage-authored evaluated metadata without leaving compatibility
/// aliases from an older writer behind. Existing objects can contain a single
/// mixed-case MinIO key; blindly inserting lowercase dual aliases would make
/// strict generation reads observe conflicting values forever.
fn merge_evaluated_metadata(metadata: &mut HashMap<String, String>, evaluated: &HashMap<String, String>) -> Result<bool> {
    let mut internal_updates = std::collections::BTreeMap::<String, (String, String, bool)>::new();
    let mut ordinary_updates = Vec::new();

    for (key, value) in evaluated {
        let Some(suffix) = rustfs_utils::http::strip_internal_prefix_preserving_case(key) else {
            ordinary_updates.push((key, value));
            continue;
        };
        let normalized_suffix = suffix.to_ascii_lowercase();
        let rustfs_preferred = rustfs_utils::http::starts_with_ignore_ascii_case(key, rustfs_utils::http::RUSTFS_INTERNAL_PREFIX);
        match internal_updates.entry(normalized_suffix) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((suffix.to_string(), value.clone(), rustfs_preferred));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if entry.get().1 != *value {
                    return Err(Error::other("conflicting evaluated internal metadata aliases"));
                }
                if rustfs_preferred && !entry.get().2 {
                    entry.insert((suffix.to_string(), value.clone(), true));
                }
            }
        }
    }

    for (key, value) in ordinary_updates {
        metadata.insert(key.clone(), value.clone());
    }
    for (suffix, value, _) in internal_updates.values() {
        rustfs_utils::http::remove_str(metadata, suffix);
        // `remove_str` is optimized for canonical lowercase suffixes. Retain
        // this exact fallback for dynamic suffixes whose identifier casing is
        // significant (for example replication-reset target ARNs).
        metadata.retain(|key, _| {
            !rustfs_utils::http::strip_internal_prefix_preserving_case(key)
                .is_some_and(|existing| existing.eq_ignore_ascii_case(suffix))
        });
        rustfs_utils::http::insert_str(metadata, suffix, value.clone());
    }

    Ok(!internal_updates.is_empty())
}

fn rebuild_file_info_replication_state(fi: &mut FileInfo) {
    fi.replication_state_internal = rustfs_filemeta::get_internal_replication_state(&fi.metadata);
}

fn is_get_mid_size_streaming_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(ENV_RUSTFS_GET_MID_SIZE_STREAMING_ENABLE, DEFAULT_RUSTFS_GET_MID_SIZE_STREAMING_ENABLE)
    }
    #[cfg(not(test))]
    {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ENABLED.get_or_init(|| {
            rustfs_utils::get_env_bool(ENV_RUSTFS_GET_MID_SIZE_STREAMING_ENABLE, DEFAULT_RUSTFS_GET_MID_SIZE_STREAMING_ENABLE)
        })
    }
}

/// Return the object size when the bounded non-duplex reader is safe.
///
/// This predicate deliberately has a narrower contract than the general
/// codec-streaming gate: only a whole, plain, single-part object is eligible.
/// Ranges, transforms, remote objects, multipart reads, copy-source reads and
/// special movement/version requests retain their existing legacy semantics.
#[allow(dead_code, reason = "asserted by this file's gate tests")]
fn get_mid_size_streaming_object_size(
    range: &Option<HTTPRangeSpec>,
    object_info: &ObjectInfo,
    fi: &FileInfo,
    opts: &ObjectOptions,
    lock_optimization_enabled: bool,
) -> Option<usize> {
    get_mid_size_streaming_object_size_with_flags(
        range,
        object_info,
        fi,
        opts,
        lock_optimization_enabled,
        is_get_mid_size_streaming_enabled(),
        is_get_codec_streaming_base_enabled(),
    )
}

#[allow(dead_code, reason = "asserted by this file's gate tests")]
fn get_mid_size_streaming_object_size_with_flags(
    range: &Option<HTTPRangeSpec>,
    object_info: &ObjectInfo,
    fi: &FileInfo,
    opts: &ObjectOptions,
    lock_optimization_enabled: bool,
    mid_size_enabled: bool,
    codec_base_enabled: bool,
) -> Option<usize> {
    get_mid_size_streaming_object_size_with_flags_and_plan(
        range,
        object_info,
        opts,
        lock_optimization_enabled,
        mid_size_enabled,
        codec_base_enabled,
        super::super::ReadPathPlan::new(object_info, fi),
    )
}

fn get_mid_size_streaming_object_size_with_flags_and_plan(
    range: &Option<HTTPRangeSpec>,
    object_info: &ObjectInfo,
    opts: &ObjectOptions,
    lock_optimization_enabled: bool,
    mid_size_enabled: bool,
    codec_base_enabled: bool,
    plan: super::super::ReadPathPlan,
) -> Option<usize> {
    if !mid_size_enabled
        || !codec_base_enabled
        || !lock_optimization_enabled
        || range.is_some()
        || opts.part_number.is_some()
        || opts.version_id.is_some()
        || opts.incl_free_versions
        || opts.skip_free_version
        || opts.data_movement
        || opts.raw_data_movement_read
        || object_info.delete_marker
        || object_info.metadata_only
        || object_info.version_only
        || crate::set_disk::get_object_read_policy() != super::super::GetObjectReadPolicy::Default
    {
        return None;
    }

    let shape = plan.shape()?;
    if !plan.is_plain() {
        return None;
    }
    (GET_MID_SIZE_STREAMING_MIN_SIZE..=GET_MID_SIZE_STREAMING_MAX_SIZE)
        .contains(&shape.object_size)
        .then_some(shape.object_size)
}

#[cfg(all(test, feature = "test-util"))]
use super::super::GetObjectMetadataCacheEntry;
#[cfg(test)]
use super::super::GetObjectMetadataCacheKey;
#[cfg(test)]
use super::super::capacity_scope_from_disks;
#[cfg(all(test, feature = "test-util"))]
use super::super::get_lock_acquire_timeout;
use crate::bucket::lifecycle::{
    tier_delete_journal::{TierDeleteDispatchAuthorization, record_tier_delete_journal_backend_identity},
    tier_sweeper::{
        Jentry, RemoteTierDeleteOutcome, attach_tier_delete_source,
        delete_confirmed_transition_candidate_exact_with_lease_idempotent, delete_object_from_remote_tier_with_lease_idempotent,
        transitioned_force_delete_journal_entry,
    },
    transition_transaction::{
        TransitionCleanupDecision, TransitionRemoteVersion, TransitionSourceIdentity, TransitionSourceVersionMode,
        TransitionTransaction, TransitionTransactionInit, TransitionTransactionState, delete_transition_transaction_record,
        save_transition_transaction_record, save_transition_transaction_record_if_current,
    },
};
use crate::bucket::quota::reservation;
use crate::bucket::replication::{
    DeleteReplicationConfigSnapshot, ReplicationLifecycleBridge, ReplicationStatusType, VersionPurgeStatusType,
    replication_state_to_filemeta, replication_status_from_filemeta, version_purge_status_from_filemeta,
    version_purge_status_to_filemeta,
};
use crate::data_usage::quota_object_size;
use crate::diagnostics::get::GetObjectFailureReason;
#[cfg(test)]
use crate::disk::DiskOption;
#[cfg(all(test, feature = "test-util"))]
use crate::disk::RUSTFS_META_MULTIPART_BUCKET;
#[cfg(test)]
use crate::disk::ReadOptions;
#[cfg(test)]
use crate::disk::STORAGE_FORMAT_FILE;
#[cfg(test)]
use crate::disk::endpoint::Endpoint;
#[cfg(test)]
use crate::disk::format::FormatV3;
#[cfg(test)]
use crate::disk::new_disk;
use crate::disk::{DataDirDeleteStatus, OldCurrentSize};
use crate::error::is_err_invalid_upload_id;
use crate::object_api::{GetObjectBodySource, get_object_body_cache_hook_suppressed};
use crate::object_api::{
    NamespaceLockFence, ReplicationStatusWritebackCondition, ReplicationStatusWritebackMode,
    SCANNER_PUBLICATION_LEASE_FENCE_METADATA_KEY, WriteCompletion,
};
use crate::services::notification_sys::RemoteVersionStateFleetProofToken;
use crate::services::tier::tier::{TierConfigMgr, TierDestinationId, TierOperationLease, tier_destination_id_from_metadata};
use crate::set_disk::core::io_primitives::{RenameRollbackReceipt, RenameTailCleanup, finish_rename_tail_heal};
#[cfg(test)]
use crate::storage_api_contracts::namespace::NamespaceLocking;
#[cfg(test)]
use crate::storage_api_contracts::object::HTTPPreconditions;
use crate::store::ECStore;
use crate::store::utils::clean_metadata;
use crate::store::{RemoteTuplePublicationCommitGuard, RemoteTuplePublicationFence};
use futures::FutureExt as _;
use http::HeaderValue;
#[cfg(test)]
use rustfs_filemeta::FileMeta;
#[cfg(all(test, feature = "test-util"))]
use rustfs_filemeta::ObjectPartInfo;
use rustfs_utils::path::decode_dir_object;
#[cfg(test)]
use rustfs_utils::path::encode_dir_object;
use std::future::Future;
use std::task::{Context, Poll};
use std::time::Duration;
#[cfg(test)]
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, ReadBuf};
#[cfg(all(test, feature = "test-util"))]
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

fn record_transitioned_delete_cleanup_owner(bucket: &str, object: &str, batch: bool) {
    metrics::counter!("rustfs_ilm_transitioned_delete_cleanup_owners_total", "owner" => "tier_free_version").increment(1);
    debug!(
        event = EVENT_LIFECYCLE_TRANSITIONED_DELETE_CLEANUP_OWNER,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_SET_DISK,
        bucket,
        object,
        cleanup_owner = "tier_free_version",
        batch,
        "Selected the durable cleanup owner for a transitioned source delete"
    );
}

/// A causal free-version receipt is only valid when the delete request really
/// removes the locked transitioned source. In particular, replication may turn
/// an otherwise successful delete into a metadata-only purge-state update, and
/// a versioned delete without a version ID writes a new delete marker instead
/// of removing the source selected by `goi`.
fn transitioned_delete_publishes_free_version(source: &ObjectInfo, delete_request: &FileInfo, skip_free_version: bool) -> bool {
    if source.delete_marker
        || source.transitioned_object.status != TRANSITION_COMPLETE
        || skip_free_version
        || delete_request.skip_tier_free_version()
        || delete_request.expire_restored
        || delete_request.transition_status == TRANSITION_COMPLETE
        || delete_file_info_version_id(source.version_id) != delete_request.version_id
    {
        return false;
    }

    // Keep this predicate aligned with FileMeta::delete_version's Object
    // branch: a non-delete-marker request with a nonterminal purge status (or
    // mark_deleted with no purge status) updates replication metadata in place
    // and never calls MetaObject::init_free_version.
    let purge_status = version_purge_status_from_filemeta(delete_request.version_purge_status());
    let metadata_only = !delete_request.deleted
        && ((purge_status.is_empty() && delete_request.mark_deleted)
            || (!purge_status.is_empty() && purge_status != VersionPurgeStatusType::Complete));

    !metadata_only
}

fn record_committed_tier_free_version_receipt(
    opts: &ObjectOptions,
    bucket: &str,
    object: &str,
    source: &ObjectInfo,
    free_version_id: Uuid,
    batch: bool,
) {
    if let Some(sink) = opts.tier_free_version_receipt_sink.as_ref()
        && let Err(err) = sink.record(source, free_version_id)
    {
        warn!(
            event = EVENT_LIFECYCLE_TRANSITIONED_DELETE_CLEANUP_OWNER,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_SET_DISK,
            bucket,
            object,
            batch,
            error = ?err,
            "Failed to retain the in-memory tier free-version scheduling receipt"
        );
    }
    record_transitioned_delete_cleanup_owner(bucket, object, batch);
}

struct TierFreeVersionReceiptCandidate {
    source: ObjectInfo,
    free_version_id: Uuid,
}

fn committed_tier_free_version_receipt_indices(
    versions: &[FileInfoVersions],
    delete_errors: &[Option<Error>],
    candidates: &HashMap<usize, TierFreeVersionReceiptCandidate>,
) -> Vec<usize> {
    if candidates.is_empty() {
        return Vec::new();
    }

    let mut committed = Vec::with_capacity(candidates.len());
    for group in versions {
        let should_rollback = group
            .versions
            .iter()
            .any(|version| delete_errors.get(version.idx).is_none_or(|error| error.is_some()));
        if should_rollback {
            continue;
        }
        committed.extend(
            group
                .versions
                .iter()
                .map(|version| version.idx)
                .filter(|idx| candidates.contains_key(idx)),
        );
    }
    committed
}

async fn acquire_single_tier_delete_lease(opts: &ObjectOptions, source: &ObjectInfo) -> Result<Option<TierOperationLease>> {
    let Some(api) = opts.tier_delete_journal_api.as_ref() else {
        return Ok(None);
    };
    if source.transitioned_object.status != TRANSITION_COMPLETE || set_disk_delete_creates_delete_marker(opts) {
        return Ok(None);
    }
    let backend_identity = tier_destination_id_from_metadata(&source.user_defined).map_err(Error::other)?;
    let lease = match backend_identity {
        Some(backend_identity) => {
            TierConfigMgr::acquire_operation_lease_for_backend_identity(
                &api.tier_config_mgr(),
                &source.transitioned_object.tier,
                backend_identity,
            )
            .await
        }
        None => TierConfigMgr::acquire_operation_lease(&api.tier_config_mgr(), &source.transitioned_object.tier).await,
    }
    .map_err(Error::other)?;
    Ok(Some(lease))
}

const OLD_DATA_CLEANUP_RECEIPT_FILE: &str = ".rustfs-old-data-cleanup-receipt.json";
const SCANNER_PUBLICATION_LEASE_FENCE_MAX_BYTES: usize = 64 * 1024;
const SCANNER_PUBLICATION_LEASE_FENCE_MAX_ENTRIES: usize = 256;

fn begin_scanner_publication_delete_mutation(scope: Option<&crate::object_api::ScannerPublicationCommitScope>) -> Result<()> {
    let Some(scope) = scope else {
        return Ok(());
    };
    if scope.state() == crate::object_api::ScannerPublicationCommitState::Admitted {
        scope
            .try_begin()
            .map_err(|_| Error::other("scanner publication delete scope cannot start"))?;
    }
    if !scope.can_commit() {
        let _ = scope.mark_indeterminate();
        return Err(StorageError::OperationCanceled);
    }
    Ok(())
}

fn take_scanner_publication_lease_tokens(user_defined: &mut HashMap<String, String>) -> Result<Option<HashMap<String, Uuid>>> {
    let Some(encoded) = user_defined.remove(SCANNER_PUBLICATION_LEASE_FENCE_METADATA_KEY) else {
        return Ok(None);
    };
    if encoded.len() > SCANNER_PUBLICATION_LEASE_FENCE_MAX_BYTES {
        return Err(Error::other("scanner publication lease fence is too large"));
    }
    let encoded_tokens = serde_json::from_str::<HashMap<String, String>>(&encoded)
        .map_err(|err| Error::other(format!("invalid scanner publication lease fence: {err}")))?;
    if encoded_tokens.len() > SCANNER_PUBLICATION_LEASE_FENCE_MAX_ENTRIES {
        return Err(Error::other("scanner publication lease fence has too many entries"));
    }
    let mut tokens = HashMap::with_capacity(encoded_tokens.len());
    for (host, token) in encoded_tokens {
        if host.is_empty() || host.len() > 1024 {
            return Err(Error::other("invalid scanner publication lease fence host"));
        }
        let token = Uuid::parse_str(&token)
            .map_err(|err| Error::other(format!("invalid scanner publication lease fence token: {err}")))?;
        if token.is_nil() {
            return Err(Error::other("invalid scanner publication lease fence token"));
        }
        tokens.insert(host, token);
    }
    Ok(Some(tokens))
}

#[cfg(test)]
mod replication_status_writeback_tests {
    use super::*;

    #[test]
    fn replication_status_writeback_matches_exact_raw_generation() {
        let mut metadata = HashMap::new();
        let generation = "2026-09-02T12:34:56.123456789+08:00[Asia/Shanghai]";
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP, generation.to_string());
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_REPLICATION_GENERATION,
            Uuid::from_u128(1).to_string(),
        );
        let object_info = ObjectInfo {
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        let expected_generation = object_info.replication_generation_snapshot();

        assert!(replication_status_writeback_is_current(
            &object_info,
            &ReplicationStatusWritebackCondition {
                expected_generation: expected_generation.clone(),
                ..Default::default()
            }
        ));
        let mut stale_generation = expected_generation;
        stale_generation.timestamp = Some("2026-09-02T04:34:56.123456789Z".to_string());
        assert!(!replication_status_writeback_is_current(
            &object_info,
            &ReplicationStatusWritebackCondition {
                expected_generation: stale_generation,
                ..Default::default()
            }
        ));
    }

    #[test]
    fn replication_status_writeback_distinguishes_legacy_absence() {
        let object_info = ObjectInfo::default();
        let expected_generation = object_info.replication_generation_snapshot();
        assert!(replication_status_writeback_is_current(
            &object_info,
            &ReplicationStatusWritebackCondition {
                expected_generation: expected_generation.clone(),
                ..Default::default()
            }
        ));
        let mut newer = expected_generation;
        newer.timestamp = Some("newer".to_string());
        assert!(!replication_status_writeback_is_current(
            &object_info,
            &ReplicationStatusWritebackCondition {
                expected_generation: newer,
                ..Default::default()
            }
        ));
    }

    #[test]
    fn replication_status_writeback_normalizes_suspended_null_version_identity() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,
            "opaque-null-version-time".to_string(),
        );
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_REPLICATION_GENERATION,
            Uuid::from_u128(1).to_string(),
        );
        let queued = ObjectInfo {
            version_id: Some(Uuid::nil()),
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        let expected_generation = queued.replication_generation_snapshot();

        let mut reread = queued;
        reread.version_id = None;
        assert!(replication_status_writeback_is_current(
            &reread,
            &ReplicationStatusWritebackCondition {
                expected_generation: expected_generation.clone(),
                ..Default::default()
            }
        ));

        reread.version_id = Some(Uuid::from_u128(2));
        assert!(!replication_status_writeback_is_current(
            &reread,
            &ReplicationStatusWritebackCondition {
                expected_generation,
                ..Default::default()
            }
        ));
    }

    #[test]
    fn replication_status_writeback_rejects_conflicting_or_invalid_generation_aliases() {
        let timestamp_key = format!(
            "{}{}",
            rustfs_utils::http::RUSTFS_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP
        );
        let timestamp_alias = format!(
            "{}{}",
            rustfs_utils::http::MINIO_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP
        );
        let generation_key = format!(
            "{}{}",
            rustfs_utils::http::RUSTFS_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_REPLICATION_GENERATION
        );
        let generation_alias = format!(
            "{}{}",
            rustfs_utils::http::MINIO_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_REPLICATION_GENERATION
        );
        let object_info = ObjectInfo {
            user_defined: Arc::new(HashMap::from([
                (timestamp_key, "same".to_string()),
                (timestamp_alias, "different".to_string()),
                (generation_key, Uuid::from_u128(1).to_string()),
                (generation_alias, "not-a-uuid".to_string()),
            ])),
            ..Default::default()
        };
        let invalid = object_info.replication_generation_snapshot();
        assert!(invalid.invalid);
        assert!(!replication_status_writeback_is_current(
            &object_info,
            &ReplicationStatusWritebackCondition {
                expected_generation: invalid,
                ..Default::default()
            }
        ));
    }

    #[test]
    fn replication_status_writeback_accepts_single_compatibility_alias_and_rejects_empty_values() {
        let timestamp_alias = format!(
            "{}{}",
            rustfs_utils::http::MINIO_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP
        );
        let generation_alias = format!(
            "{}{}",
            rustfs_utils::http::MINIO_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_REPLICATION_GENERATION
        );
        let object_info = ObjectInfo {
            user_defined: Arc::new(HashMap::from([
                (timestamp_alias, "raw-minio-time".to_string()),
                (generation_alias.clone(), Uuid::from_u128(1).to_string()),
            ])),
            ..Default::default()
        };
        let expected_generation = object_info.replication_generation_snapshot();
        assert!(!expected_generation.invalid);
        assert!(replication_status_writeback_is_current(
            &object_info,
            &ReplicationStatusWritebackCondition {
                expected_generation,
                ..Default::default()
            }
        ));

        let invalid_object_info = ObjectInfo {
            user_defined: Arc::new(HashMap::from([(generation_alias, String::new())])),
            ..Default::default()
        };
        let invalid = invalid_object_info.replication_generation_snapshot();
        assert!(invalid.invalid);
        assert!(!replication_status_writeback_is_current(
            &invalid_object_info,
            &ReplicationStatusWritebackCondition {
                expected_generation: invalid,
                ..Default::default()
            }
        ));

        for suffix in [
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,
            rustfs_utils::http::SUFFIX_TAGGING_TIMESTAMP,
            rustfs_utils::http::SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP,
            rustfs_utils::http::SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP,
        ] {
            let mut metadata = HashMap::new();
            rustfs_utils::http::insert_str(&mut metadata, suffix, String::new());
            let invalid_object_info = ObjectInfo {
                user_defined: Arc::new(metadata),
                ..Default::default()
            };
            let invalid = invalid_object_info.replication_generation_snapshot();
            assert!(invalid.invalid, "empty {suffix} must fail closed");
            assert!(!replication_status_writeback_is_current(
                &invalid_object_info,
                &ReplicationStatusWritebackCondition {
                    expected_generation: invalid,
                    ..Default::default()
                }
            ));
        }
    }

    #[test]
    fn replication_payload_fingerprint_tracks_target_admission_not_status_value() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP, "same".to_string());
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_REPLICATION_GENERATION,
            Uuid::from_u128(1).to_string(),
        );
        let pending = ObjectInfo {
            user_defined: Arc::new(metadata),
            replication_status_internal: Some("arn:a=PENDING;".to_string()),
            ..Default::default()
        };
        let expected_generation = pending.replication_generation_snapshot();

        let mut same_targets = pending.clone();
        same_targets.replication_status_internal = Some("arn:a=FAILED;".to_string());
        assert!(replication_status_writeback_is_current(
            &same_targets,
            &ReplicationStatusWritebackCondition {
                expected_generation: expected_generation.clone(),
                ..Default::default()
            }
        ));

        let mut different_targets = pending;
        different_targets.replication_status_internal = Some("arn:b=PENDING;".to_string());
        assert!(!replication_status_writeback_is_current(
            &different_targets,
            &ReplicationStatusWritebackCondition {
                expected_generation,
                ..Default::default()
            }
        ));
    }
}

#[cfg(test)]
mod scanner_publication_lease_fence_tests {
    use super::*;

    #[test]
    fn scanner_publication_lease_fence_is_transient_and_validated() {
        let token = Uuid::new_v4();
        let mut metadata = HashMap::from([(
            SCANNER_PUBLICATION_LEASE_FENCE_METADATA_KEY.to_string(),
            serde_json::json!({"http://node-a:9000": token.to_string()}).to_string(),
        )]);
        let parsed = take_scanner_publication_lease_tokens(&mut metadata)
            .expect("valid scanner publication lease fence should parse")
            .expect("scanner publication lease fence should be present");
        assert_eq!(parsed.get("http://node-a:9000"), Some(&token));
        assert!(!metadata.contains_key(SCANNER_PUBLICATION_LEASE_FENCE_METADATA_KEY));

        let mut malformed = HashMap::from([(
            SCANNER_PUBLICATION_LEASE_FENCE_METADATA_KEY.to_string(),
            r#"{"http://node-a:9000":"not-a-uuid"}"#.to_string(),
        )]);
        assert!(take_scanner_publication_lease_tokens(&mut malformed).is_err());
    }
}

#[cfg(test)]
mod tier_free_version_receipt_eligibility_tests {
    use super::*;
    use crate::bucket::replication::ReplicationState;

    fn transitioned_source(version_id: Option<Uuid>) -> ObjectInfo {
        let mut source = ObjectInfo {
            version_id,
            ..Default::default()
        };
        source.transitioned_object.status = TRANSITION_COMPLETE.to_string();
        source.transitioned_object.tier = "WARM".to_string();
        source.transitioned_object.name = "remote/object".to_string();
        source
    }

    fn delete_request(version_id: Option<Uuid>) -> FileInfo {
        let mut request = FileInfo {
            version_id,
            ..Default::default()
        };
        request.set_tier_free_version_id(&Uuid::new_v4().to_string());
        request
    }

    #[test]
    fn accepts_exact_transitioned_source_removal_and_suspended_null_replacement() {
        let version_id = Uuid::new_v4();
        assert!(transitioned_delete_publishes_free_version(
            &transitioned_source(Some(version_id)),
            &delete_request(Some(version_id)),
            false,
        ));

        let mut suspended_null_delete = delete_request(None);
        suspended_null_delete.deleted = true;
        suspended_null_delete.mark_deleted = true;
        assert!(transitioned_delete_publishes_free_version(
            &transitioned_source(Some(Uuid::nil())),
            &suspended_null_delete,
            false,
        ));
    }

    #[test]
    fn rejects_new_marker_version_and_non_transitioned_or_delete_marker_sources() {
        let source_id = Uuid::new_v4();
        assert!(!transitioned_delete_publishes_free_version(
            &transitioned_source(Some(source_id)),
            &delete_request(Some(Uuid::new_v4())),
            false,
        ));

        let mut ordinary = transitioned_source(Some(source_id));
        ordinary.transitioned_object.status.clear();
        assert!(!transitioned_delete_publishes_free_version(
            &ordinary,
            &delete_request(Some(source_id)),
            false,
        ));

        let mut delete_marker = transitioned_source(Some(source_id));
        delete_marker.delete_marker = true;
        assert!(!transitioned_delete_publishes_free_version(
            &delete_marker,
            &delete_request(Some(source_id)),
            false,
        ));
    }

    #[test]
    fn rejects_skip_restore_and_transition_metadata_updates() {
        let version_id = Uuid::new_v4();
        let source = transitioned_source(Some(version_id));

        assert!(!transitioned_delete_publishes_free_version(
            &source,
            &delete_request(Some(version_id)),
            true,
        ));

        let mut skip_request = delete_request(Some(version_id));
        skip_request.set_skip_tier_free_version();
        assert!(!transitioned_delete_publishes_free_version(&source, &skip_request, false));

        let mut restore_request = delete_request(Some(version_id));
        restore_request.expire_restored = true;
        assert!(!transitioned_delete_publishes_free_version(&source, &restore_request, false));

        let mut transition_update = delete_request(Some(version_id));
        transition_update.transition_status = TRANSITION_COMPLETE.to_string();
        assert!(!transitioned_delete_publishes_free_version(&source, &transition_update, false));
    }

    #[test]
    fn rejects_nonterminal_replication_metadata_only_update_but_accepts_complete_purge() {
        let version_id = Uuid::new_v4();
        let source = transitioned_source(Some(version_id));

        let mut pending = delete_request(Some(version_id));
        pending.replication_state_internal = Some(replication_state_to_filemeta(&ReplicationState {
            version_purge_status_internal: Some("PENDING".to_string()),
            ..Default::default()
        }));
        assert!(!transitioned_delete_publishes_free_version(&source, &pending, false));

        let mut mark_deleted = delete_request(Some(version_id));
        mark_deleted.mark_deleted = true;
        assert!(!transitioned_delete_publishes_free_version(&source, &mark_deleted, false));

        let mut complete = delete_request(Some(version_id));
        complete.replication_state_internal = Some(replication_state_to_filemeta(&ReplicationState {
            version_purge_status_internal: Some("COMPLETE".to_string()),
            ..Default::default()
        }));
        assert!(transitioned_delete_publishes_free_version(&source, &complete, false));
    }

    #[test]
    fn whole_physical_object_group_must_commit_before_any_receipt_is_retained() {
        let candidate = || TierFreeVersionReceiptCandidate {
            source: transitioned_source(Some(Uuid::new_v4())),
            free_version_id: Uuid::new_v4(),
        };
        let candidates = HashMap::from([(0, candidate()), (2, candidate())]);
        let versions = vec![
            FileInfoVersions {
                versions: vec![
                    FileInfo {
                        idx: 0,
                        ..Default::default()
                    },
                    FileInfo {
                        idx: 1,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            FileInfoVersions {
                versions: vec![FileInfo {
                    idx: 2,
                    ..Default::default()
                }],
                ..Default::default()
            },
        ];
        let one_sibling_failed = vec![None, Some(Error::other("injected quorum failure")), None];

        assert_eq!(
            committed_tier_free_version_receipt_indices(&versions, &one_sibling_failed, &candidates),
            vec![2],
            "a sibling failure must suppress every receipt from the rolled-back xl.meta group"
        );
        assert_eq!(
            committed_tier_free_version_receipt_indices(&versions, &[None, None, None], &candidates),
            vec![0, 2],
            "independent fully committed groups should retain their sparse receipts"
        );
    }
}

struct PutObjectCommitCancellation {
    token: CancellationToken,
    armed: bool,
}

impl PutObjectCommitCancellation {
    fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            armed: true,
        }
    }

    fn child_token(&self) -> CancellationToken {
        self.token.clone()
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PutObjectCommitCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.token.cancel();
        }
    }
}

async fn wait_for_put_object_commit_cancellation(
    owner_cancellation: Option<&CancellationToken>,
    request_cancellation: Option<&CancellationToken>,
) {
    match (owner_cancellation, request_cancellation) {
        (Some(owner), Some(request)) => {
            tokio::select! {
                _ = owner.cancelled() => {}
                _ = request.cancelled() => {}
            }
        }
        (Some(owner), None) => owner.cancelled().await,
        (None, Some(request)) => request.cancelled().await,
        (None, None) => std::future::pending().await,
    }
}

#[inline]
fn duration_millis_f64(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

struct LifecycleDeleteAllPlan<'a> {
    history: Vec<&'a FileInfo>,
    trigger: Option<&'a FileInfo>,
}

impl<'a> LifecycleDeleteAllPlan<'a> {
    fn trigger_only(&self) -> Result<Option<&'a FileInfo>> {
        if !self.history.is_empty() {
            return Err(StorageError::PreconditionFailed);
        }
        Ok(self.trigger)
    }
}

fn lifecycle_delete_all_plan<'a>(
    versions: &'a rustfs_filemeta::FileInfoVersions,
    trigger: &crate::object_api::LifecycleDeleteAllRequest,
) -> Result<LifecycleDeleteAllPlan<'a>> {
    let normalized_version_id = |version: &FileInfo| version.version_id.filter(|version_id| !version_id.is_nil());
    let mut history = Vec::with_capacity(versions.versions.len());
    let mut trigger_version = None;
    for (ordinary_index, version) in versions
        .versions
        .iter()
        .filter(|version| !version.tier_free_version())
        .enumerate()
    {
        if matches!(
            replication_status_from_filemeta(version.replication_status()),
            ReplicationStatusType::Pending | ReplicationStatusType::Failed
        ) || version.version_purge_status().is_pending()
        {
            return Err(StorageError::PreconditionFailed);
        }
        if normalized_version_id(version) == trigger.version_id {
            if trigger_version.is_some() || ordinary_index != 0 || version.deleted != trigger.delete_marker {
                return Err(StorageError::PreconditionFailed);
            }
            trigger_version = Some(version);
        } else {
            history.push(version);
        }
    }
    Ok(LifecycleDeleteAllPlan {
        history,
        trigger: trigger_version,
    })
}

fn lifecycle_delete_all_tier_journal_entry(
    bucket: &str,
    object: &str,
    version: &FileInfo,
    opts: &ObjectOptions,
) -> Result<Option<Jentry>> {
    if version.transition_status != rustfs_filemeta::TRANSITION_COMPLETE {
        return Ok(None);
    }

    let logical_object = decode_dir_object(object);
    let mut source = ObjectInfo::from_file_info(version, bucket, object, true);
    source.version_id = source.version_id.filter(|version_id| !version_id.is_nil());
    let Some(mut entry) = transitioned_force_delete_journal_entry(&source.transitioned_object, source.transition_version_state)
    else {
        return Ok(None);
    };
    attach_tier_delete_source(&mut entry, bucket, &logical_object, &source, opts.versioned, opts.version_suspended);
    record_tier_delete_journal_backend_identity(&mut entry, &source.user_defined).map_err(Error::other)?;
    if !entry.can_replace_tier_free_version() {
        return Ok(None);
    }
    Ok(Some(entry))
}

async fn acquire_lifecycle_delete_all_tier_lease(version: &FileInfo, opts: &ObjectOptions) -> Result<Option<TierOperationLease>> {
    if version.transition_status != rustfs_filemeta::TRANSITION_COMPLETE {
        return Ok(None);
    }
    let Some(api) = opts.tier_delete_journal_api.as_ref() else {
        return Ok(None);
    };
    let backend_identity = tier_destination_id_from_metadata(&version.metadata).map_err(Error::other)?;
    let lease = match backend_identity {
        Some(backend_identity) => {
            TierConfigMgr::acquire_operation_lease_for_backend_identity(
                &api.tier_config_mgr(),
                &version.transition_tier,
                backend_identity,
            )
            .await
        }
        None => TierConfigMgr::acquire_operation_lease(&api.tier_config_mgr(), &version.transition_tier).await,
    }
    .map_err(Error::other)?;
    Ok(Some(lease))
}

fn lifecycle_delete_all_replication_delete(
    bucket: &str,
    object: &str,
    version: &FileInfo,
    opts: &ObjectOptions,
) -> Result<Option<(crate::bucket::replication::ReplicationState, DeletedObject)>> {
    let snapshot = opts
        .delete_replication_config_snapshot
        .as_deref()
        .ok_or(StorageError::PreconditionFailed)?;
    let logical_object = decode_dir_object(object);
    if !snapshot.has_active_rule(&logical_object) {
        return Ok(None);
    }

    let versioned = opts.versioned || opts.version_suspended;
    let source = ObjectInfo::from_file_info(version, bucket, object, versioned);
    let Some(version_id) = source.version_id else {
        return Err(StorageError::PreconditionFailed);
    };
    let delete_opts = ObjectOptions {
        version_id: Some(version_id.to_string()),
        versioned: opts.versioned,
        version_suspended: opts.version_suspended,
        replication_request: opts.replication_request,
        no_lock: true,
        ..Default::default()
    };
    let object_to_delete = ObjectToDelete {
        object_name: logical_object.clone(),
        version_id: Some(version_id),
        ..Default::default()
    };
    let decision = ReplicationObjectBridge::check_delete_with_snapshot(&object_to_delete, &source, &delete_opts, false, snapshot);
    if !decision.replicate_any() {
        return Ok(None);
    }

    let replication_state = ReplicationLifecycleBridge::version_delete_replication_state(&decision);
    let deleted_object = if source.delete_marker {
        DeletedObject {
            delete_marker: true,
            delete_marker_version_id: Some(version_id),
            delete_marker_mtime: source.mod_time,
            object_name: logical_object,
            replication_state: Some(replication_state_to_filemeta(&replication_state)),
            ..Default::default()
        }
    } else {
        DeletedObject {
            object_name: logical_object,
            version_id: Some(version_id),
            replication_state: Some(replication_state_to_filemeta(&replication_state)),
            ..Default::default()
        }
    };
    Ok(Some((replication_state, deleted_object)))
}

#[cfg(test)]
mod lifecycle_delete_all_plan_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks_isolated as hermetic_set_disks;
    use super::*;
    use crate::bucket::replication::ReplicationState;

    fn delete_opts(request: crate::object_api::LifecycleDeleteAllRequest) -> ObjectOptions {
        let mut opts = ObjectOptions {
            delete_prefix: true,
            delete_prefix_object: true,
            versioned: true,
            lifecycle_delete_all: Some(request),
            delete_replication_config_snapshot: Some(Arc::new(DeleteReplicationConfigSnapshot::default())),
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))),
            ..Default::default()
        };
        opts.ensure_lifecycle_delete_all_journal();
        opts
    }

    fn trigger(version_id: Uuid) -> crate::object_api::LifecycleDeleteAllRequest {
        crate::object_api::LifecycleDeleteAllRequest {
            version_id: Some(version_id),
            delete_marker: true,
            action: rustfs_scanner_metrics::metrics::IlmAction::DelMarkerDeleteAllVersionsAction,
            rule_id: "rule".to_string(),
            phase: crate::object_api::LifecycleDeleteAllPhase::Preflight,
        }
    }

    #[test]
    fn orders_history_before_trigger_and_excludes_tier_free_versions() {
        let trigger_id = Uuid::new_v4();
        let old_id = Uuid::new_v4();
        let mut free = FileInfo::default();
        free.set_tier_free_version();
        free.version_id = Some(Uuid::new_v4());
        let versions = rustfs_filemeta::FileInfoVersions {
            versions: vec![
                FileInfo {
                    version_id: Some(trigger_id),
                    deleted: true,
                    ..Default::default()
                },
                free,
                FileInfo {
                    version_id: Some(old_id),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let trigger = trigger(trigger_id);

        let plan = lifecycle_delete_all_plan(&versions, &trigger).expect("valid lifecycle plan");
        assert_eq!(
            plan.history.iter().map(|version| version.version_id).collect::<Vec<_>>(),
            vec![Some(old_id)]
        );
        assert_eq!(plan.trigger.and_then(|version| version.version_id), Some(trigger_id));
        assert!(plan.history.iter().all(|version| !version.tier_free_version()));
    }

    #[test]
    fn rejects_a_noncurrent_trigger_copy() {
        let trigger_id = Uuid::new_v4();
        let versions = rustfs_filemeta::FileInfoVersions {
            versions: vec![
                FileInfo {
                    version_id: Some(Uuid::new_v4()),
                    deleted: false,
                    ..Default::default()
                },
                FileInfo {
                    version_id: Some(trigger_id),
                    deleted: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let trigger = trigger(trigger_id);

        assert!(matches!(
            lifecycle_delete_all_plan(&versions, &trigger),
            Err(StorageError::PreconditionFailed)
        ));
    }

    #[test]
    fn allows_a_history_only_pool_without_the_trigger() {
        let old_id = Uuid::new_v4();
        let versions = rustfs_filemeta::FileInfoVersions {
            versions: vec![FileInfo {
                version_id: Some(old_id),
                ..Default::default()
            }],
            ..Default::default()
        };

        let plan = lifecycle_delete_all_plan(&versions, &trigger(Uuid::new_v4())).expect("history-only pool should plan");
        assert_eq!(
            plan.history.iter().map(|version| version.version_id).collect::<Vec<_>>(),
            vec![Some(old_id)]
        );
        assert!(plan.trigger.is_none());
    }

    #[test]
    fn rejects_duplicate_trigger_versions() {
        let trigger_id = Uuid::new_v4();
        let versions = rustfs_filemeta::FileInfoVersions {
            versions: vec![
                FileInfo {
                    version_id: Some(trigger_id),
                    deleted: true,
                    ..Default::default()
                },
                FileInfo {
                    version_id: Some(trigger_id),
                    deleted: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert!(matches!(
            lifecycle_delete_all_plan(&versions, &trigger(trigger_id)),
            Err(StorageError::PreconditionFailed)
        ));
    }

    #[test]
    fn rejects_nonterminal_replication_and_purge_states_before_building_a_delete_plan() {
        let trigger_id = Uuid::new_v4();
        for (replication_status, purge_status) in [
            (Some("PENDING"), None),
            (Some("FAILED"), None),
            (None, Some("PENDING")),
            (None, Some("FAILED")),
        ] {
            let versions = rustfs_filemeta::FileInfoVersions {
                versions: vec![
                    FileInfo {
                        version_id: Some(trigger_id),
                        deleted: true,
                        ..Default::default()
                    },
                    FileInfo {
                        version_id: Some(Uuid::new_v4()),
                        replication_state_internal: Some(replication_state_to_filemeta(&ReplicationState {
                            replication_status_internal: replication_status.map(str::to_string),
                            version_purge_status_internal: purge_status.map(str::to_string),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };

            assert!(matches!(
                lifecycle_delete_all_plan(&versions, &trigger(trigger_id)),
                Err(StorageError::PreconditionFailed)
            ));
        }
    }

    #[test]
    fn final_preflight_and_trigger_reject_remaining_history() {
        let trigger_id = Uuid::new_v4();
        let current = FileInfo {
            version_id: Some(trigger_id),
            deleted: true,
            ..Default::default()
        };
        let history = FileInfo {
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        let request = trigger(trigger_id);
        let with_history = rustfs_filemeta::FileInfoVersions {
            versions: vec![current.clone(), history.clone()],
            ..Default::default()
        };
        assert!(matches!(
            lifecycle_delete_all_plan(&with_history, &request).and_then(|plan| plan.trigger_only()),
            Err(StorageError::PreconditionFailed)
        ));

        let trigger_only_versions = rustfs_filemeta::FileInfoVersions {
            versions: vec![current],
            ..Default::default()
        };
        let trigger_only = lifecycle_delete_all_plan(&trigger_only_versions, &request)
            .and_then(|plan| plan.trigger_only())
            .expect("trigger-only phase should proceed")
            .expect("trigger should remain");
        assert_eq!(trigger_only.version_id, Some(trigger_id));

        let history_only_versions = rustfs_filemeta::FileInfoVersions {
            versions: vec![history],
            ..Default::default()
        };
        assert!(matches!(
            lifecycle_delete_all_plan(&history_only_versions, &request).and_then(|plan| plan.trigger_only()),
            Err(StorageError::PreconditionFailed)
        ));
    }

    #[test]
    fn null_trigger_matches_none_and_nil_version_ids() {
        let request = crate::object_api::LifecycleDeleteAllRequest {
            version_id: None,
            delete_marker: false,
            action: rustfs_scanner_metrics::metrics::IlmAction::DeleteAllVersionsAction,
            rule_id: "rule".to_string(),
            phase: crate::object_api::LifecycleDeleteAllPhase::Preflight,
        };
        for version_id in [None, Some(Uuid::nil())] {
            let versions = rustfs_filemeta::FileInfoVersions {
                versions: vec![FileInfo {
                    version_id,
                    ..Default::default()
                }],
                ..Default::default()
            };

            let plan = lifecycle_delete_all_plan(&versions, &request).expect("null trigger should match its persisted form");
            assert!(plan.history.is_empty());
            assert!(plan.trigger.is_some());
        }
    }

    #[test]
    fn tier_journal_coverage_is_source_exact_and_falls_back_for_legacy_metadata() {
        let identity = [7_u8; 32];
        let version_id = Uuid::from_u128(1);
        let mut metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(identity),
        );
        let transitioned = |data_dir| FileInfo {
            version_id: Some(version_id),
            data_dir: Some(data_dir),
            metadata: metadata.clone(),
            transition_status: rustfs_filemeta::TRANSITION_COMPLETE.to_string(),
            transition_tier: "WARM".to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_version: Some("remote-version".to_string()),
            transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
            ..Default::default()
        };
        let opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };

        let first = lifecycle_delete_all_tier_journal_entry("bucket", "object", &transitioned(Uuid::from_u128(2)), &opts)
            .expect("exact transitioned source should be journalable")
            .expect("completed transition should require a journal");
        let second = lifecycle_delete_all_tier_journal_entry("bucket", "object", &transitioned(Uuid::from_u128(3)), &opts)
            .expect("second pool source should be journalable")
            .expect("completed transition should require a journal");
        assert_ne!(first.source, second.source, "each pool-local source needs independent coverage");

        let mut unknown = transitioned(Uuid::from_u128(4));
        unknown.transition_version_state = rustfs_filemeta::TransitionVersionState::Unknown;
        assert!(
            lifecycle_delete_all_tier_journal_entry("bucket", "object", &unknown, &opts)
                .expect("legacy Unknown metadata should use the free-version fallback")
                .is_none()
        );

        let mut missing_identity = transitioned(Uuid::from_u128(5));
        missing_identity.metadata.clear();
        assert!(
            lifecycle_delete_all_tier_journal_entry("bucket", "object", &missing_identity, &opts)
                .expect("legacy metadata without destination identity should use the free-version fallback")
                .is_none()
        );
    }

    #[cfg(not(feature = "rio-v2"))]
    #[tokio::test]
    async fn put_object_stamps_the_v2_frame_layout_marker_only_when_enabled() {
        use rustfs_utils::http::headers::SSEC_ALGORITHM_HEADER;

        async fn put_encrypted_and_reread(marker_expected: bool) {
            let (_temp_dirs, disks, set) = hermetic_set_disks(4).await;
            let bucket = "frame-layout-marker-bucket";
            for disk in &disks {
                disk.make_volume(bucket).await.expect("bucket volume should be created");
            }

            let opts = ObjectOptions {
                user_defined: HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]),
                ..Default::default()
            };
            let mut reader = PutObjReader::from_vec(vec![0x42; 4096]);
            set.put_object(bucket, "encrypted", &mut reader, &opts)
                .await
                .expect("encrypted object should be stored");

            let info = set
                .get_object_info(bucket, "encrypted", &ObjectOptions::default())
                .await
                .expect("stored object should be readable");
            let marker = rustfs_utils::http::get_consistent_str(
                &info.user_defined,
                crate::object_api::ENCRYPTED_FRAME_LAYOUT_FIXED8K_SUFFIX,
            )
            .map(str::to_string);
            if marker_expected {
                let data_dir = info.data_dir.expect("stored object should have a data dir").to_string();
                assert_eq!(marker.as_deref(), Some(data_dir.as_str()), "marker must bind the object's data_dir");
            } else {
                assert_eq!(marker, None, "marker must not be stamped");
            }

            // A plaintext object never carries the marker, whatever the switch.
            let mut reader = PutObjReader::from_vec(vec![0x43; 128]);
            set.put_object(bucket, "plaintext", &mut reader, &ObjectOptions::default())
                .await
                .expect("plaintext object should be stored");
            let info = set
                .get_object_info(bucket, "plaintext", &ObjectOptions::default())
                .await
                .expect("plaintext object should be readable");
            assert_eq!(
                rustfs_utils::http::get_consistent_str(
                    &info.user_defined,
                    crate::object_api::ENCRYPTED_FRAME_LAYOUT_FIXED8K_SUFFIX
                ),
                None,
                "plaintext objects never carry a frame-layout marker"
            );
        }

        temp_env::async_with_vars([(crate::io_support::rio::ENV_RUSTFS_ENCRYPTION_FRAME_V2, Some("true"))], async {
            put_encrypted_and_reread(true).await;
        })
        .await;
        temp_env::async_with_vars([(crate::io_support::rio::ENV_RUSTFS_ENCRYPTION_FRAME_V2, None::<&str>)], async {
            put_encrypted_and_reread(false).await;
        })
        .await;
    }

    #[tokio::test]
    async fn staged_delete_removes_history_before_the_trigger() {
        let (temp_dirs, disks, set) = hermetic_set_disks(4).await;
        let bucket = "lifecycle-delete-all-staged";
        let object = "object";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        for body in [b"old".as_slice(), b"new".as_slice()] {
            let mut reader = PutObjReader::from_vec(body.to_vec());
            set.put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("versioned object should be stored");
        }
        let marker = set
            .delete_object(
                bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("delete marker should be created");
        let marker_id = marker.version_id.expect("delete marker should have a version id");
        let transitioned_id = Uuid::new_v4();
        let remote_version_id = Uuid::new_v4();
        let free_version_id = Uuid::new_v4();
        for temp_dir in &temp_dirs {
            let meta_path = temp_dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE);
            let encoded = tokio::fs::read(&meta_path).await.expect("xl.meta should be readable");
            let mut metadata = FileMeta::load(&encoded).expect("xl.meta should decode");
            metadata
                .add_version(FileInfo {
                    volume: bucket.to_string(),
                    name: object.to_string(),
                    version_id: Some(transitioned_id),
                    transition_status: crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE.to_string(),
                    transitioned_objname: "remote/lifecycle-delete-all-staged/object".to_string(),
                    transition_version_id: Some(remote_version_id),
                    transition_version: Some(remote_version_id.to_string()),
                    transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
                    transition_tier: "WARM".to_string(),
                    mod_time: Some(OffsetDateTime::now_utc() - time::Duration::days(10)),
                    ..Default::default()
                })
                .expect("transitioned version should be added");
            let mut transition_delete = FileInfo {
                volume: bucket.to_string(),
                name: object.to_string(),
                version_id: Some(transitioned_id),
                mod_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            };
            transition_delete.set_tier_free_version_id(&free_version_id.to_string());
            metadata
                .delete_version(&transition_delete)
                .expect("transitioned version should become a free-version record");
            tokio::fs::write(&meta_path, metadata.marshal_msg().expect("xl.meta should encode"))
                .await
                .expect("xl.meta should be rewritten");
        }

        let mut request = trigger(marker_id);
        request.phase = crate::object_api::LifecycleDeleteAllPhase::Trigger;
        let err = set
            .delete_object(bucket, object, delete_opts(request.clone()))
            .await
            .expect_err("trigger must remain while historical versions exist");
        assert_eq!(err, StorageError::PreconditionFailed);

        request.phase = crate::object_api::LifecycleDeleteAllPhase::Preflight;
        set.delete_object(bucket, object, delete_opts(request.clone()))
            .await
            .expect("preflight should pass");
        request.phase = crate::object_api::LifecycleDeleteAllPhase::History;
        set.delete_object(bucket, object, delete_opts(request.clone()))
            .await
            .expect("history phase should delete old versions");
        let after_history = set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("metadata should load")
            .expect("trigger metadata should remain");
        let ordinary: Vec<_> = after_history
            .versions
            .iter()
            .filter(|version| !version.tier_free_version())
            .collect();
        assert_eq!(ordinary.len(), 1);
        assert_eq!(ordinary[0].version_id, Some(marker_id));

        request.phase = crate::object_api::LifecycleDeleteAllPhase::Trigger;
        set.delete_object(bucket, object, delete_opts(request))
            .await
            .expect("trigger phase should delete the final marker");
        for temp_dir in &temp_dirs {
            let meta_path = temp_dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE);
            let encoded = tokio::fs::read(&meta_path).await.expect("free-version xl.meta should remain");
            let remaining = FileMeta::load(&encoded)
                .expect("free-version xl.meta should decode")
                .get_all_file_info_versions(bucket, object, true)
                .expect("free-version metadata should remain readable");
            let free_versions: Vec<_> = remaining
                .versions
                .iter()
                .filter(|version| version.tier_free_version())
                .collect();
            assert_eq!(free_versions.len(), 1);
            assert_eq!(free_versions[0].version_id, Some(free_version_id));
            assert_eq!(free_versions[0].transition_tier, "WARM");
            assert_eq!(free_versions[0].transitioned_objname, "remote/lifecycle-delete-all-staged/object");
            assert_eq!(free_versions[0].transition_version_id, Some(remote_version_id));
        }
    }

    #[tokio::test]
    async fn stale_trigger_after_a_new_put_is_rejected_without_writes() {
        let (_temp_dirs, disks, set) = hermetic_set_disks(4).await;
        let bucket = "lifecycle-delete-all-stale-trigger";
        let object = "object";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut reader = PutObjReader::from_vec(b"old".to_vec());
        set.put_object(
            bucket,
            object,
            &mut reader,
            &ObjectOptions {
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect("old version should be stored");
        let marker = set
            .delete_object(
                bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("delete marker should be created");
        let marker_id = marker.version_id.expect("delete marker should have a version id");
        let mut replacement = PutObjReader::from_vec(b"replacement".to_vec());
        set.put_object(
            bucket,
            object,
            &mut replacement,
            &ObjectOptions {
                versioned: true,
                ..Default::default()
            },
        )
        .await
        .expect("new current version should be stored");
        let before = set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("metadata should load")
            .expect("versions should exist");

        let err = set
            .delete_object(bucket, object, delete_opts(trigger(marker_id)))
            .await
            .expect_err("stale marker must not authorize a purge");
        assert_eq!(err, StorageError::PreconditionFailed);
        let after = set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("metadata should load after rejection")
            .expect("versions should remain");
        assert_eq!(
            after.versions.iter().map(|version| version.version_id).collect::<Vec<_>>(),
            before.versions.iter().map(|version| version.version_id).collect::<Vec<_>>()
        );
    }
}

pub(in crate::set_disk::ops) fn assign_object_transaction_epoch(
    shuffle_disks: &[Option<DiskStore>],
    parts_metadatas: &mut [FileInfo],
) -> Uuid {
    let epoch = Uuid::new_v4();
    for (disk, file_info) in shuffle_disks.iter().zip(parts_metadatas.iter_mut()) {
        if disk.is_some() {
            file_info.set_object_transaction_epoch(epoch);
        }
    }
    epoch
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OldDataCleanupReceiptRecord {
    epoch: String,
    old_data_dir: String,
    committed_data_dir: Option<String>,
}

#[derive(Clone, Copy)]
struct OldDataCleanupReceipt {
    epoch: Uuid,
    old_data_dir: Uuid,
    committed_data_dir: Option<Uuid>,
}

impl OldDataCleanupReceipt {
    fn new(epoch: Uuid, old_data_dir: Uuid, committed_data_dir: Option<Uuid>) -> Self {
        Self {
            epoch,
            old_data_dir,
            committed_data_dir,
        }
    }

    fn encode(self) -> disk::error::Result<Bytes> {
        let record = OldDataCleanupReceiptRecord {
            epoch: self.epoch.to_string(),
            old_data_dir: self.old_data_dir.to_string(),
            committed_data_dir: self.committed_data_dir.map(|dir| dir.to_string()),
        };
        Ok(Bytes::from(serde_json::to_vec(&record)?))
    }

    fn decode(data: &[u8]) -> disk::error::Result<Self> {
        let record: OldDataCleanupReceiptRecord = serde_json::from_slice(data)?;
        let epoch = Uuid::parse_str(&record.epoch).map_err(DiskError::other)?;
        let old_data_dir = Uuid::parse_str(&record.old_data_dir).map_err(DiskError::other)?;
        let committed_data_dir = record
            .committed_data_dir
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(DiskError::other)?;
        if epoch.is_nil() || old_data_dir.is_nil() || committed_data_dir.is_some_and(|dir| dir.is_nil()) {
            return Err(DiskError::FileCorrupt);
        }
        Ok(Self::new(epoch, old_data_dir, committed_data_dir))
    }
}

pub(in crate::set_disk::ops) fn old_data_cleanup_receipt_path(object: &str, old_data_dir: Uuid) -> String {
    path_join_buf(&[object, &old_data_dir.to_string(), OLD_DATA_CLEANUP_RECEIPT_FILE])
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::set_disk::ops) enum ObjectTransactionEpochFence {
    Absent,
    Present(Uuid),
}

impl ObjectTransactionEpochFence {
    fn from_file_info(file_info: &FileInfo) -> Result<Self> {
        match file_info.object_transaction_epoch() {
            Ok(Some(epoch)) => Ok(Self::Present(epoch)),
            Ok(None) => Ok(Self::Absent),
            Err(_) => Err(StorageError::FileCorrupt),
        }
    }
}

pub(in crate::set_disk::ops) async fn read_object_transaction_epoch_fence(
    set: &SetDisks,
    bucket: &str,
    object: &str,
) -> Result<ObjectTransactionEpochFence> {
    let current = set
        .get_object_fileinfo(
            bucket,
            object,
            &ObjectOptions {
                no_lock: true,
                metadata_cache_safe: false,
                versioned: true,
                ..Default::default()
            },
            false,
            false,
        )
        .await;
    match current {
        Ok(snapshot) => ObjectTransactionEpochFence::from_file_info(snapshot.fi()),
        Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => Ok(ObjectTransactionEpochFence::Absent),
        Err(err) => Err(err),
    }
}

pub(in crate::set_disk::ops) async fn verify_object_transaction_epoch_fence(
    set: &SetDisks,
    bucket: &str,
    object: &str,
    expected: ObjectTransactionEpochFence,
) -> Result<()> {
    let current = read_object_transaction_epoch_fence(set, bucket, object).await?;
    if current == expected {
        Ok(())
    } else {
        Err(StorageError::PreconditionFailed)
    }
}

fn old_data_cleanup_receipt_epoch_matches_current(receipt: OldDataCleanupReceipt, current: ObjectTransactionEpochFence) -> bool {
    matches!(current, ObjectTransactionEpochFence::Present(epoch) if epoch == receipt.epoch)
}

#[cfg(test)]
mod duration_metrics_tests {
    use super::duration_millis_f64;
    use std::time::Duration;

    #[test]
    fn duration_millis_preserves_sub_millisecond_precision() {
        assert_eq!(duration_millis_f64(Duration::from_micros(125)), 0.125);
    }
}

fn is_restore_control_metadata(key: &str) -> bool {
    key.eq_ignore_ascii_case(X_AMZ_RESTORE.as_str())
        || key.eq_ignore_ascii_case(rustfs_utils::http::headers::AMZ_RESTORE_EXPIRY_DAYS)
        || key.eq_ignore_ascii_case(rustfs_utils::http::headers::AMZ_RESTORE_REQUEST_DATE)
        || rustfs_utils::http::internal_key_strip_suffix_prefix(key, SUFFIX_RESTORE_OPERATION_ID)
            .is_some_and(|remainder| remainder.is_empty())
        || rustfs_utils::http::internal_key_strip_suffix_prefix(key, SUFFIX_RESTORE_WORKER_LOCK)
            .is_some_and(|remainder| remainder.is_empty())
}

fn restore_metadata_update_preserves_protected_metadata(
    existing: &HashMap<String, String>,
    replacement: &HashMap<String, String>,
) -> bool {
    let mut existing = existing.clone();
    clean_metadata(&mut existing);
    let mut replacement = replacement.clone();
    clean_metadata(&mut replacement);
    let existing_count = existing.keys().filter(|key| !is_restore_control_metadata(key)).count();
    let replacement_count = replacement.keys().filter(|key| !is_restore_control_metadata(key)).count();
    existing_count == replacement_count
        && existing
            .iter()
            .filter(|(key, _)| !is_restore_control_metadata(key))
            .all(|(key, value)| replacement.get(key) == Some(value))
}

#[cfg(test)]
mod restore_metadata_update_tests {
    use super::*;

    #[test]
    fn restore_metadata_update_cannot_change_retention_or_user_metadata() {
        let mut existing = HashMap::from([
            ("etag".to_string(), "etag-value".to_string()),
            ("x-amz-meta-owner".to_string(), "alice".to_string()),
            ("x-amz-object-lock-mode".to_string(), "COMPLIANCE".to_string()),
        ]);
        let mut replacement = existing.clone();
        replacement.insert(X_AMZ_RESTORE.as_str().to_string(), "ongoing-request=\"true\"".to_string());
        rustfs_utils::http::metadata_compat::insert_str(
            &mut replacement,
            SUFFIX_RESTORE_OPERATION_ID,
            Uuid::new_v4().to_string(),
        );
        rustfs_utils::http::metadata_compat::insert_str(
            &mut replacement,
            SUFFIX_RESTORE_WORKER_LOCK,
            rustfs_utils::http::metadata_compat::RESTORE_WORKER_LOCK_PROTOCOL_V1.to_string(),
        );
        assert!(restore_metadata_update_preserves_protected_metadata(&existing, &replacement));

        replacement.insert("x-amz-object-lock-mode".to_string(), "GOVERNANCE".to_string());
        assert!(!restore_metadata_update_preserves_protected_metadata(&existing, &replacement));

        replacement.clone_from(&existing);
        replacement.insert("x-amz-meta-owner".to_string(), "mallory".to_string());
        assert!(!restore_metadata_update_preserves_protected_metadata(&existing, &replacement));

        existing.insert(X_AMZ_RESTORE.as_str().to_string(), "ongoing-request=\"false\"".to_string());
        replacement.clone_from(&existing);
        replacement.remove(X_AMZ_RESTORE.as_str());
        assert!(restore_metadata_update_preserves_protected_metadata(&existing, &replacement));
    }
}

fn erasure_from_file_info(fi: &FileInfo, uses_legacy: bool) -> Result<coding::Erasure> {
    coding::Erasure::try_new_with_options(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size, uses_legacy)
        .map_err(Error::from)
}

async fn get_object_reader_with_context(
    ctx: &InstanceContext,
    reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
    range: Option<HTTPRangeSpec>,
    object_info: &ObjectInfo,
    opts: &ObjectOptions,
    headers: &HeaderMap<HeaderValue>,
) -> Result<(GetObjectReader, usize, i64)> {
    GetObjectReader::new_with_resolver(reader, range, object_info, opts, headers, ctx.object_encryption_resolver()).await
}

async fn get_legacy_object_reader_with_context<R>(
    ctx: &InstanceContext,
    reader: R,
    terminal: tokio::sync::oneshot::Receiver<Result<()>>,
    range: Option<HTTPRangeSpec>,
    object_info: &ObjectInfo,
    opts: &ObjectOptions,
    headers: &HeaderMap<HeaderValue>,
) -> Result<(GetObjectReader, usize, i64)>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    // ReadPlan validates this size below; failure here only keeps the terminal
    // guard inside the transform until that validation returns its typed error.
    let full_plaintext_size = object_info.get_actual_size().ok();
    let whole_object = opts.part_number.is_none()
        && match (&range, full_plaintext_size) {
            (None, _) => true,
            (Some(range), Some(size)) => range
                .get_offset_length(size)
                .is_ok_and(|(offset, length)| offset == 0 && length == size),
            (Some(_), None) => false,
        };
    let (source, terminal): (Box<dyn AsyncRead + Unpin + Send + Sync>, _) = if whole_object {
        (Box::new(reader), Some(terminal))
    } else {
        (Box::new(LegacyDuplexProducerReader::new(reader, terminal)), None)
    };
    let (mut reader, offset, length) = get_object_reader_with_context(ctx, source, range, object_info, opts, headers).await?;
    if let Some(terminal) = terminal {
        reader.stream = Box::new(LegacyDuplexProducerReader::new(reader.stream, terminal));
    }
    Ok((reader, offset, length))
}

/// Cancels a detached legacy GET producer when its consumer is dropped.
///
/// The producer owns the shard readers and the object read lock, while the
/// consumer owns only the duplex read half. Closing that half eventually
/// unblocks a writer, but can leave a producer stuck in reader setup or remote
/// recovery until a lower-level timeout fires. This small boundary wrapper
/// provides an explicit cancellation signal without changing the public
/// `GetObjectReader` shape.
struct ProducerCancellationReader<R> {
    inner: R,
    cancellation: CancellationToken,
}

impl<R> ProducerCancellationReader<R> {
    fn new(inner: R, cancellation: CancellationToken) -> Self {
        Self { inner, cancellation }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ProducerCancellationReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<R> Drop for ProducerCancellationReader<R> {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

fn data_read_metadata_early_stop_request_shape_allowed(range: &Option<HTTPRangeSpec>, opts: &ObjectOptions) -> bool {
    range.is_none()
        && opts.part_number.is_none()
        && opts.version_id.is_none()
        && !opts.incl_free_versions
        && !opts.skip_free_version
        && !opts.raw_data_movement_read
        && !opts.data_movement
        && !crate::object_api::restore_request_active(opts)
}

fn prepare_late_materialized_retry(initial_result: &Result<()>, output: &mut Vec<u8>, expected_size: usize) -> bool {
    if initial_result.is_ok() && output.len() == expected_size {
        return false;
    }
    output.clear();
    true
}

#[cfg(test)]
mod data_read_metadata_early_stop_request_shape_tests {
    use super::*;

    #[test]
    fn data_read_metadata_early_stop_only_allows_whole_latest_plain_get_shape() {
        assert!(data_read_metadata_early_stop_request_shape_allowed(&None, &ObjectOptions::default()));

        let range = Some(HTTPRangeSpec {
            is_suffix_length: false,
            start: 0,
            end: 0,
        });
        assert!(!data_read_metadata_early_stop_request_shape_allowed(&range, &ObjectOptions::default()));

        let part_opts = ObjectOptions {
            part_number: Some(1),
            ..Default::default()
        };
        assert!(!data_read_metadata_early_stop_request_shape_allowed(&None, &part_opts));

        let version_opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        };
        assert!(!data_read_metadata_early_stop_request_shape_allowed(&None, &version_opts));

        let incl_free_opts = ObjectOptions {
            incl_free_versions: true,
            ..Default::default()
        };
        assert!(!data_read_metadata_early_stop_request_shape_allowed(&None, &incl_free_opts));

        let skip_free_opts = ObjectOptions {
            skip_free_version: true,
            ..Default::default()
        };
        assert!(!data_read_metadata_early_stop_request_shape_allowed(&None, &skip_free_opts));

        let data_movement_opts = ObjectOptions {
            data_movement: true,
            ..Default::default()
        };
        assert!(!data_read_metadata_early_stop_request_shape_allowed(&None, &data_movement_opts));

        let raw_data_movement_opts = ObjectOptions {
            raw_data_movement_read: true,
            ..Default::default()
        };
        assert!(!data_read_metadata_early_stop_request_shape_allowed(&None, &raw_data_movement_opts));

        let mut restore_opts = ObjectOptions::default();
        restore_opts.transition.restore_request.days = Some(1);
        assert!(!data_read_metadata_early_stop_request_shape_allowed(&None, &restore_opts));
    }

    #[test]
    fn late_materialized_retry_clears_partial_buffer_after_error() {
        let mut output = b"partial-prefix".to_vec();
        let result = Err(Error::FileCorrupt);
        assert!(prepare_late_materialized_retry(&result, &mut output, 1024));
        assert!(output.is_empty());
    }
}

/// Length of the full plaintext body when — and only when — this read's output
/// is exactly the object's complete plaintext, so the app-layer body cache may
/// serve it in place of the erasure read.
///
/// A hook hit bypasses `ReadPlan`/`ReadTransform` entirely, so it is sound only
/// where the normal read path would produce that same plaintext byte-for-byte
/// AND expose the same `object_info.size`. This is a fail-closed allow-list, not
/// a deny-list: every read whose `ReadPlan` applies some other transform returns
/// `None`, so a newly added `ReadPlan` branch bypasses the cache by default
/// instead of silently serving bytes in the wrong representation.
///
/// Refused reads, each mapping to a `ReadPlan::build` branch:
/// - ranged / part-number reads — the cache only holds whole objects;
/// - `raw_data_movement_read` / `data_movement` — yields the STORED
///   representation, e.g. compressed bytes (backlog#1108);
/// - restore reads — `restore_request_active` forces the `Plain` branch, so a
///   compressed object yields STORED bytes under its compressed `size`;
/// - encrypted objects — `ReadTransform::Encrypted` rewrites size and the cache
///   must never hold their plaintext;
/// - remote (transitioned) objects — served from the warm tier.
///
/// Compressed objects ARE eligible: `ReadTransform::Compressed` returns the full
/// plaintext and rewrites `object_info.size` to the decompressed length, which
/// the caller must replicate with the returned length (backlog#1109).
///
/// The fail-closed shape here is enforced by `scripts/check_body_cache_whitelist.sh`
/// (backlog#1146): the guard requires every exclusion predicate and a `return
/// None` to precede the first `Some(..)`, so a refactor to a deny-list fails CI.
/// If you rename or move this function, update that script.
fn full_object_plaintext_len(range: &Option<HTTPRangeSpec>, opts: &ObjectOptions, object_info: &ObjectInfo) -> Option<i64> {
    if range.is_some()
        || opts.part_number.is_some()
        || opts.raw_data_movement_read
        || opts.data_movement
        || crate::object_api::restore_request_active(opts)
        || object_info.is_encrypted()
        || object_info.is_remote()
        || object_info.delete_marker
        || object_info.size == 0
        || object_info.version_only
        || object_info.metadata_only
        || object_info.is_inline_fast_path_eligible()
    {
        return None;
    }

    if object_info.is_compressed() {
        return object_info.get_actual_size().ok();
    }

    Some(object_info.size)
}

const RESTORE_MULTIPART_ABORT_FAILURES_TOTAL: &str = "rustfs_restore_multipart_abort_failures_total";
const RESTORE_TIER_MUTATION_RETRY_BUDGET: Duration = Duration::from_secs(30);
const RESTORE_TIER_MUTATION_RETRY_BASE: Duration = Duration::from_millis(250);
const RESTORE_TIER_MUTATION_RETRY_CAP: Duration = Duration::from_secs(5);

fn restore_tier_mutation_retry_delay(attempt: u32) -> Duration {
    RESTORE_TIER_MUTATION_RETRY_BASE
        .saturating_mul(1_u32 << attempt.min(5))
        .min(RESTORE_TIER_MUTATION_RETRY_CAP)
}

async fn acquire_restore_tier_lease(
    manager: &Arc<tokio::sync::RwLock<TierConfigMgr>>,
    tier_name: &str,
    expected_backend_identity: Option<TierDestinationId>,
) -> Result<TierOperationLease> {
    let deadline = Instant::now() + RESTORE_TIER_MUTATION_RETRY_BUDGET;
    let mut attempt = 0_u32;
    loop {
        let result = match expected_backend_identity {
            Some(identity) => TierConfigMgr::acquire_operation_lease_for_backend_identity(manager, tier_name, identity).await,
            None => TierConfigMgr::acquire_operation_lease(manager, tier_name).await,
        };
        match result {
            Ok(lease) => return Ok(lease),
            Err(err) if TierConfigMgr::operation_lease_blocked_by_mutation(&err) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(Error::other(err));
                }
                tokio::time::sleep(restore_tier_mutation_retry_delay(attempt).min(remaining)).await;
                attempt = attempt.saturating_add(1);
            }
            Err(err) => return Err(Error::other(err)),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RestoreMultipartFailurePoint {
    InvalidPartSize,
    RangeOverflow,
    TierGet,
    HashReader,
    PutPart,
    SizeMismatch,
    Complete,
}

#[cfg(test)]
static RESTORE_MULTIPART_FAILURE_POINT: std::sync::Mutex<Option<RestoreMultipartFailurePoint>> = std::sync::Mutex::new(None);
#[cfg(test)]
static RESTORE_MULTIPART_UPLOAD_ID: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
#[cfg(test)]
static RESTORE_MULTIPART_ABORT_ATTEMPTS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
fn restore_multipart_failure_is(point: RestoreMultipartFailurePoint) -> bool {
    *RESTORE_MULTIPART_FAILURE_POINT
        .lock()
        .expect("restore multipart failure-point lock must not be poisoned")
        == Some(point)
}

#[cfg(test)]
fn fail_restore_multipart_at(point: RestoreMultipartFailurePoint) -> Result<()> {
    if restore_multipart_failure_is(point) {
        return Err(StorageError::Unexpected);
    }
    Ok(())
}

struct RestoreMultipartUploadCleanup {
    store: Arc<SetDisks>,
    bucket: String,
    object: String,
    upload_id: String,
    armed: bool,
}

impl RestoreMultipartUploadCleanup {
    fn new(store: Arc<SetDisks>, bucket: &str, object: &str, upload_id: &str) -> Self {
        Self {
            store,
            bucket: bucket.to_string(),
            object: object.to_string(),
            upload_id: upload_id.to_string(),
            armed: true,
        }
    }

    async fn abort(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        #[cfg(test)]
        RESTORE_MULTIPART_ABORT_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
        if let Err(err) = self
            .store
            .abort_multipart_upload(&self.bucket, &self.object, &self.upload_id, &ObjectOptions::default())
            .await
            && !is_err_invalid_upload_id(&err)
        {
            metrics::counter!(RESTORE_MULTIPART_ABORT_FAILURES_TOTAL).increment(1);
            warn!(
                bucket = self.bucket,
                object = self.object,
                upload_id = self.upload_id,
                error = ?err,
                "failed to abort incomplete multipart restore"
            );
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RestoreMultipartUploadCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let store = Arc::clone(&self.store);
        let bucket = self.bucket.clone();
        let object = self.object.clone();
        let upload_id = self.upload_id.clone();
        #[cfg(test)]
        RESTORE_MULTIPART_ABORT_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
        // Cancellation while the restore runtime is alive is cleaned
        // asynchronously. Runtime teardown itself is only best-effort because
        // Drop cannot await storage IO; normal error exits use abort() above.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(err) = store
                    .abort_multipart_upload(&bucket, &object, &upload_id, &ObjectOptions::default())
                    .await
                    && !is_err_invalid_upload_id(&err)
                {
                    metrics::counter!(RESTORE_MULTIPART_ABORT_FAILURES_TOTAL).increment(1);
                    warn!(
                        bucket,
                        object,
                        upload_id,
                        error = ?err,
                        "failed to abort cancelled multipart restore"
                    );
                }
            });
        }
    }
}

pub(crate) fn body_cache_plaintext_len(
    range: &Option<HTTPRangeSpec>,
    opts: &ObjectOptions,
    object_info: &ObjectInfo,
) -> Option<i64> {
    full_object_plaintext_len(range, opts, object_info)
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::object::ObjectIO for SetDisks {
    type Error = Error;
    type RangeSpec = HTTPRangeSpec;
    type HeaderMap = HeaderMap;
    type ObjectOptions = ObjectOptions;
    type ObjectInfo = ObjectInfo;
    type GetObjectReader = GetObjectReader;
    type PutObjectReader = PutObjReader;

    #[tracing::instrument(level = "debug", skip(self, h))]
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        crate::hp_guard!("SetDisks::get_object_reader");
        let stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        // Check if lock optimization is enabled for reads that are fully materialized in memory.
        let lock_optimization_enabled = is_lock_optimization_enabled();

        // Acquire a shared read-lock early to protect read consistency
        let mut read_lock_guard = if !opts.no_lock {
            let acquire_start = stage_metrics_enabled.then(Instant::now);
            let lock_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);

            // Record lock wait for deadlock detection
            if is_deadlock_detection_enabled() {
                debug!(
                    lock_id = format!("{}:{}", bucket, object),
                    lock_type = "read",
                    resource = format!("{}/{}", bucket, object),
                    "Waiting for read lock"
                );
            }

            let guard = self.acquire_read_lock_diag("get_object", bucket, object).await?;

            // Record lock acquisition for deadlock detection
            let _lock_id = record_lock_acquire(bucket, object, "read");

            // Record lock statistics only when GET stage metrics are enabled,
            // matching the adjacent stage timer. Avoids a per-GET clock read and
            // two global-recorder lookups when observability/stage metrics are off.
            if let Some(acquire_start) = acquire_start {
                metrics::counter!("rustfs.lock.acquire.total", "type" => "read").increment(1);
                metrics::histogram!("rustfs.lock.acquire.duration.seconds").record(acquire_start.elapsed().as_secs_f64());
            }
            record_get_stage_duration_if_enabled(GET_OBJECT_PATH_SET_DISK, GET_STAGE_LOCK_ACQUIRE, lock_stage_start);

            Some(guard)
        } else {
            None
        };

        let metadata_stage_start = stage_metrics_enabled.then(Instant::now);
        let (snapshot, prepared_object_info) = if let Some(prepared) = take_prepared_get_object_metadata() {
            (prepared.snapshot, prepared.object_info)
        } else {
            match self
                .get_object_fileinfo_for_get_object_reader(
                    bucket,
                    object,
                    opts,
                    true,
                    data_read_metadata_early_stop_request_shape_allowed(&range, opts),
                )
                .await
            {
                Ok(snapshot) => (snapshot, None),
                Err(err) => {
                    if let Some(metadata_stage_start) = metadata_stage_start {
                        rustfs_io_metrics::record_get_object_metadata_phase_duration(
                            metadata_stage_start.elapsed().as_secs_f64(),
                        );
                    }
                    let failure_path = if is_meta_bucketname(bucket) {
                        GET_OBJECT_PATH_INTERNAL_META
                    } else {
                        GET_OBJECT_PATH_LEGACY_DUPLEX
                    };
                    record_get_object_pipeline_failure_for_path(failure_path, GET_STAGE_METADATA, classify_storage_error(&err));
                    return Err(to_object_err(err, vec![bucket, object]));
                }
            }
        };
        let fi = snapshot.fi();
        let files = snapshot.parts_metadata();
        let disks = snapshot.online_disks();
        let object_info_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let object_info = prepared_object_info
            .unwrap_or_else(|| build_get_object_info(fi, bucket, object, opts.versioned || opts.version_suspended));
        let object_class = classify_get_codec_streaming_object_class(&range, &object_info, fi);
        let metrics_size = if stage_metrics_enabled {
            object_info.get_actual_size().unwrap_or(object_info.size)
        } else {
            object_info.size
        };
        let size_bucket = rustfs_io_metrics::get_object_size_bucket(metrics_size);
        record_get_stage_duration_if_enabled(GET_OBJECT_PATH_SET_DISK, GET_STAGE_OBJECT_INFO, object_info_stage_start);
        if let Some(metadata_stage_start) = metadata_stage_start {
            let metadata_elapsed = metadata_stage_start.elapsed().as_secs_f64();
            rustfs_io_metrics::record_get_object_metadata_phase_duration(metadata_elapsed);
            rustfs_io_metrics::record_get_object_stage_duration_by_size(
                GET_OBJECT_PATH_SET_DISK,
                GET_STAGE_METADATA,
                object_class.as_str(),
                size_bucket,
                metadata_elapsed,
            );
        }

        if object_info.delete_marker {
            if opts.version_id.is_none() {
                return Err(to_object_err(Error::FileNotFound, vec![bucket, object]));
            }
            return Err(to_object_err(Error::MethodNotAllowed, vec![bucket, object]));
        }

        // if object_info.size == 0 {
        //     let empty_rd: Box<dyn AsyncRead> = Box::new(Bytes::new());

        //     return Ok(GetObjectReader {
        //         stream: empty_rd,
        //         object_info,
        //     });
        // }

        if object_info.size == 0 {
            record_get_object_reader_path_observation(GET_OBJECT_PATH_EMPTY, object_class, size_bucket);
            // if let Some(rs) = range {
            //     let _ = rs.get_offset_length(object_info.size)?;
            // }

            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(Vec::new())),
                object_info,
                buffered_body: Some(Bytes::new()),
                body_source: GetObjectBodySource::Unprobed,
            };
            return Ok(reader);
        }

        // All remaining local fast paths share this immutable, fail-closed
        // metadata decision. Build it once after empty/remote exits so those
        // requests do not pay for part and transform scans they cannot use.
        let read_path_plan = ReadPathPlan::new(&object_info, fi);

        // Inline data fast path: skip duplex pipe for small inline objects.
        // Uses the shared predicate from ObjectInfo; additionally checks that
        // inline data is actually present and neither range nor partNumber is
        // in flight.
        let use_inline_fast_path = object_info.is_inline_fast_path_eligible()
            && fi.data.is_some()
            && range.is_none()
            && opts.part_number.is_none()
            && should_use_inline_fast_path_with_plan(&range, &object_info, fi, opts, read_path_plan);
        if use_inline_fast_path {
            let mut inline_prepare_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
            let data_shards = fi.erasure.data_blocks;

            let object_size = usize::try_from(fi.size)
                .map_err(|_| to_object_err(Error::other("inline fast path object size is invalid"), vec![bucket, object]))?;

            let checksum_info = fi.erasure.get_checksum_info(fi.parts[0].number);
            let checksum_algo =
                if fi.uses_legacy_checksum && checksum_info.algorithm == rustfs_utils::HashAlgorithm::HighwayHash256S {
                    rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
                } else {
                    checksum_info.algorithm
                };

            if can_try_inline_data_shards_direct(object_size, fi.erasure.block_size)
                && let Some(data_files) = collect_inline_data_shard_fileinfos_by_index(files, fi, data_shards, |index| {
                    disks.get(index).is_some_and(Option::is_some)
                })
            {
                let read_length = inline_erasure_shard_file_offset(
                    0,
                    object_size,
                    object_size,
                    fi.erasure.block_size,
                    data_shards,
                    fi.uses_legacy_checksum,
                );
                let shard_size = inline_erasure_shard_size(fi.erasure.block_size, data_shards, fi.uses_legacy_checksum);
                if let Some(inline_prepare_stage_start) = inline_prepare_stage_start.take() {
                    rustfs_io_metrics::record_get_object_stage_duration_by_size(
                        GET_OBJECT_PATH_INLINE_DIRECT,
                        GET_STAGE_INLINE_PREPARE,
                        object_class.as_str(),
                        size_bucket,
                        inline_prepare_stage_start.elapsed().as_secs_f64(),
                    );
                }
                let reader_setup_stage_start = rustfs_io_metrics::get_stage_metrics_enabled().then(Instant::now);
                let mut readers = build_inline_bitrot_readers_from_refs(
                    &data_files,
                    bucket,
                    object,
                    read_length,
                    shard_size,
                    &checksum_algo,
                    opts.skip_verify_bitrot,
                )
                .await?;
                if let Some(reader_setup_stage_start) = reader_setup_stage_start {
                    rustfs_io_metrics::record_get_object_stage_duration_by_size(
                        GET_OBJECT_PATH_INLINE_DIRECT,
                        GET_STAGE_READER_SETUP,
                        object_class.as_str(),
                        size_bucket,
                        reader_setup_stage_start.elapsed().as_secs_f64(),
                    );
                }

                // Decode directly
                let decode_stage_start = rustfs_io_metrics::get_stage_metrics_enabled().then(Instant::now);
                if let Some(body) = try_read_inline_data_shards_direct(&mut readers, data_shards, read_length, object_size).await
                {
                    if let Some(decode_stage_start) = decode_stage_start {
                        rustfs_io_metrics::record_get_object_stage_duration_by_size(
                            GET_OBJECT_PATH_INLINE_DIRECT,
                            GET_STAGE_DECODE,
                            object_class.as_str(),
                            size_bucket,
                            decode_stage_start.elapsed().as_secs_f64(),
                        );
                    }

                    record_get_object_reader_path_observation(GET_OBJECT_PATH_INLINE_DIRECT, object_class, size_bucket);
                    let reader = GetObjectReader {
                        stream: Box::new(Cursor::new(body.clone())),
                        object_info,
                        buffered_body: Some(body),
                        body_source: GetObjectBodySource::Unprobed,
                    };
                    return Ok(reader);
                }
            }

            let erasure = self.erasure_cache.get_for_file_info(fi)?;
            let read_length = erasure.shard_file_offset(0, object_size, object_size);
            let total_shards = data_shards + fi.erasure.parity_blocks;
            let (_disks, files) = Self::shuffle_disks_and_parts_metadata_by_index(disks, files, fi);

            // Check if we have enough inline data shards
            let inline_count = files
                .iter()
                .take(data_shards)
                .filter(|f| f.data.as_ref().is_some_and(|d| !d.is_empty()))
                .count();

            if inline_count >= data_shards {
                if let Some(inline_prepare_stage_start) = inline_prepare_stage_start.take() {
                    rustfs_io_metrics::record_get_object_stage_duration_by_size(
                        GET_OBJECT_PATH_INLINE_DIRECT,
                        GET_STAGE_INLINE_PREPARE,
                        object_class.as_str(),
                        size_bucket,
                        inline_prepare_stage_start.elapsed().as_secs_f64(),
                    );
                }
                let reader_setup_stage_start = rustfs_io_metrics::get_stage_metrics_enabled().then(Instant::now);
                let readers = build_inline_bitrot_readers(
                    &files,
                    total_shards,
                    bucket,
                    object,
                    read_length,
                    erasure.shard_size(),
                    &checksum_algo,
                    opts.skip_verify_bitrot,
                )
                .await?;
                if let Some(reader_setup_stage_start) = reader_setup_stage_start {
                    rustfs_io_metrics::record_get_object_stage_duration_by_size(
                        GET_OBJECT_PATH_INLINE_DIRECT,
                        GET_STAGE_READER_SETUP,
                        object_class.as_str(),
                        size_bucket,
                        reader_setup_stage_start.elapsed().as_secs_f64(),
                    );
                }

                let decode_stage_start = rustfs_io_metrics::get_stage_metrics_enabled().then(Instant::now);
                let mut output = Cursor::new(Vec::with_capacity(object_size));
                let (written, err) = erasure.decode(&mut output, readers, 0, object_size, object_size).await;
                if let Some(e) = err {
                    return Err(to_object_err(e.into(), vec![bucket, object]));
                }
                if written == 0 && fi.size > 0 {
                    return Err(to_object_err(
                        Error::other("inline fast path: erasure decode returned 0 bytes"),
                        vec![bucket, object],
                    ));
                }
                let body = Bytes::from(output.into_inner());
                if let Some(decode_stage_start) = decode_stage_start {
                    rustfs_io_metrics::record_get_object_stage_duration_by_size(
                        GET_OBJECT_PATH_INLINE_DIRECT,
                        GET_STAGE_DECODE,
                        object_class.as_str(),
                        size_bucket,
                        decode_stage_start.elapsed().as_secs_f64(),
                    );
                }

                record_get_object_reader_path_observation(GET_OBJECT_PATH_INLINE_DIRECT, object_class, size_bucket);
                let reader = GetObjectReader {
                    stream: Box::new(Cursor::new(body.clone())),
                    object_info,
                    buffered_body: Some(body),
                    body_source: GetObjectBodySource::Unprobed,
                };
                return Ok(reader);
            }
        }

        if object_info.is_remote() {
            let decision = GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Remote);
            record_get_codec_streaming_gate_decision(object_class, decision, size_bucket);
            rustfs_io_metrics::record_get_object_codec_streaming_fallback(GetCodecStreamingFallbackReason::Remote.as_str());
            record_get_object_reader_path_observation(GET_OBJECT_PATH_REMOTE_TRANSITION, object_class, size_bucket);
            let mut opts = opts.clone();
            if object_info.parts.len() == 1 {
                opts.part_number = Some(1);
            }
            let gr = get_transitioned_object_reader_with_tier_manager(
                bucket,
                object,
                &range,
                &h,
                &object_info,
                &opts,
                &self.ctx.tier_config_mgr(),
                self.ctx.object_encryption_resolver(),
            )
            .await?;
            return Ok(finish_set_disk_read_lock(gr, read_lock_guard.take(), bucket, object));
        }

        // Metadata resolution and the remote-tier branch are complete here.
        // Keep the rollout/configuration gate deferred until the request
        // really needs codec streaming so an opted-out codec path cannot add
        // fixed cost to the inline/direct-memory/mid-size hot paths.
        // App-layer object data cache probe: metadata (etag/size) is resolved
        // but no data shards have been read yet, so a hit skips the erasure
        // read, bitrot verify and decode entirely. The hook validates object
        // identity and rejects anything it cannot serve byte-identically.
        //
        // `plaintext_len` carries the size the normal `ReadPlan` would have
        // published, which the reader below must reproduce: for a compressed
        // object `ReadTransform::Compressed` sets `object_info.size` to the
        // decompressed length, and consumers such as UploadPartCopy read the
        // copy length straight off that field (backlog#1109).
        // Records whether the app-layer cache probe ran for this read, so the
        // app layer does not repeat the lookup it already performed after fresh
        // metadata resolution (backlog#1121 / ODC-16). It stays `Unprobed` when
        // the read is ineligible under the allow-list or the hook is not
        // registered; the direct-memory and streaming readers built below carry
        // it forward.
        let mut body_source = GetObjectBodySource::Unprobed;
        if let Some(plaintext_len) = full_object_plaintext_len(&range, opts, &object_info)
            && !get_object_body_cache_hook_suppressed()
            && let Some(hook) = get_object_body_cache_hook()
        {
            match hook.lookup(bucket, object, &object_info).await {
                Some(body) if i64::try_from(body.len()).is_ok_and(|len| len == plaintext_len) => {
                    record_get_object_reader_path_observation(GET_OBJECT_PATH_BODY_CACHE, object_class, size_bucket);
                    let mut object_info = object_info;
                    object_info.size = plaintext_len;
                    let reader = GetObjectReader {
                        stream: Box::new(Cursor::new(body.clone())),
                        object_info,
                        buffered_body: Some(body),
                        body_source: GetObjectBodySource::HookServed,
                    };
                    if lock_optimization_enabled {
                        release_materialized_read_lock(bucket, object, read_lock_guard.take());
                    }
                    return Ok(reader);
                }
                // Probed after fresh metadata resolution but no usable body: a
                // genuine miss, or a length-defensive rejection. The miss is
                // authoritative, so the app layer must not look up again.
                _ => {
                    body_source = GetObjectBodySource::HookMissed;
                }
            }
        }

        if snapshot.has_late_metadata_fanout() {
            // Keep refresh plus the second decode off the default GET poll stack.
            // The allocation is limited to the opt-in late-materialization path.
            return Box::pin(async move {
                let object_size = usize::try_from(object_info.size)
                    .map_err(|_| to_object_err(Error::other("two-phase GET object size is invalid"), vec![bucket, object]))?;
                let mut output = Vec::with_capacity(object_size);
                let (fi, files, disks, late_metadata_fanout_disks) = snapshot.into_owned_with_late_metadata_fanout();
                let expected_identity = super::super::read::LateMetadataIdentity::from_file_info(&fi);
                let late_metadata_fanout_disks = late_metadata_fanout_disks.ok_or_else(|| {
                    to_object_err(Error::other("two-phase GET fallback context is missing"), vec![bucket, object])
                })?;
                let initial_result = Self::get_object_with_fileinfo(
                    bucket,
                    object,
                    Arc::clone(&self.erasure_cache),
                    0,
                    object_info.size,
                    &mut output,
                    fi,
                    files,
                    &disks,
                    self.set_index,
                    self.pool_index,
                    opts.skip_verify_bitrot,
                    true,
                    true,
                    GET_OBJECT_PATH_LEGACY_DUPLEX,
                    object_class.as_str(),
                    size_bucket,
                )
                .await;
                if prepare_late_materialized_retry(&initial_result, &mut output, object_size) {
                    let (full_fi, full_parts_metadata, full_online_disks) = Self::refresh_late_metadata_fanout(
                        &late_metadata_fanout_disks,
                        bucket,
                        object,
                        &expected_identity,
                        GET_OBJECT_PATH_LEGACY_DUPLEX,
                    )
                    .await?;
                    Self::get_object_with_fileinfo(
                        bucket,
                        object,
                        Arc::clone(&self.erasure_cache),
                        0,
                        object_info.size,
                        &mut output,
                        full_fi,
                        full_parts_metadata,
                        &full_online_disks,
                        self.set_index,
                        self.pool_index,
                        opts.skip_verify_bitrot,
                        true,
                        false,
                        GET_OBJECT_PATH_LEGACY_DUPLEX,
                        object_class.as_str(),
                        size_bucket,
                    )
                    .await?;
                }
                if output.len() != object_size {
                    return Err(to_object_err(Error::other("two-phase GET decoded length mismatch"), vec![bucket, object]));
                }

                record_get_object_reader_path_observation(GET_OBJECT_PATH_LEGACY_DUPLEX, object_class, size_bucket);
                let body = Bytes::from(output);
                let reader = GetObjectReader {
                    stream: Box::new(Cursor::new(body.clone())),
                    object_info,
                    buffered_body: Some(body),
                    body_source,
                };
                if lock_optimization_enabled {
                    release_materialized_read_lock(bucket, object, read_lock_guard.take());
                }
                Ok(reader)
            })
            .await;
        }

        let direct_memory_decision = get_small_object_direct_memory_decision_with_threshold_and_plan(
            &range,
            &object_info,
            fi,
            opts,
            is_get_small_object_direct_memory_enabled(),
            get_small_object_direct_memory_threshold(),
            read_path_plan,
        );
        record_get_direct_memory_decision(object_class, direct_memory_decision, size_bucket);
        if let GetDirectMemoryDecision::Use { object_size } = direct_memory_decision {
            if let Some(body) = Self::try_get_object_direct_data_shards_with_fileinfo(
                bucket,
                object,
                Arc::clone(&self.erasure_cache),
                fi,
                files,
                disks,
                opts.skip_verify_bitrot,
                object_class.as_str(),
                size_bucket,
            )
            .await?
            {
                if body.len() != object_size {
                    return Err(to_object_err(
                        Error::other("direct-memory GET decoded length mismatch"),
                        vec![bucket, object],
                    ));
                }

                record_get_object_reader_path_observation(GET_OBJECT_PATH_DIRECT_MEMORY, object_class, size_bucket);
                let reader = GetObjectReader {
                    stream: Box::new(Cursor::new(body.clone())),
                    object_info,
                    buffered_body: Some(body),
                    body_source,
                };
                if lock_optimization_enabled {
                    release_materialized_read_lock(bucket, object, read_lock_guard.take());
                    debug!(bucket, object, "Lock optimization: released read lock after direct-memory read");
                }
                return Ok(reader);
            }

            let mut output = Vec::with_capacity(object_size);
            let (fi, files, disks) = snapshot.into_owned();
            Self::get_object_with_fileinfo(
                bucket,
                object,
                Arc::clone(&self.erasure_cache),
                0,
                object_info.size,
                &mut output,
                fi,
                files,
                &disks,
                self.set_index,
                self.pool_index,
                opts.skip_verify_bitrot,
                true,
                false,
                GET_OBJECT_PATH_DIRECT_MEMORY,
                object_class.as_str(),
                size_bucket,
            )
            .await?;

            if output.len() != object_size {
                return Err(to_object_err(
                    Error::other("direct-memory GET decoded length mismatch"),
                    vec![bucket, object],
                ));
            }

            record_get_object_reader_path_observation(GET_OBJECT_PATH_DIRECT_MEMORY, object_class, size_bucket);
            let body = Bytes::from(output);
            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(body.clone())),
                object_info,
                buffered_body: Some(body),
                body_source,
            };
            if lock_optimization_enabled {
                release_materialized_read_lock(bucket, object, read_lock_guard.take());
                debug!(bucket, object, "Lock optimization: released read lock after direct-memory read");
            }
            return Ok(reader);
        }

        // Mid-size plain objects use a bounded, non-duplex reader. Keep this
        // path independent from the general codec-streaming rollout: that
        // rollout remains off by default because its worker overhead is not a
        // win for tiny objects. A failed setup degrades to the existing codec
        // gate/legacy path before any response bytes are returned.
        if get_mid_size_streaming_object_size_with_flags_and_plan(
            &range,
            &object_info,
            opts,
            lock_optimization_enabled,
            is_get_mid_size_streaming_enabled(),
            is_get_codec_streaming_base_enabled(),
            read_path_plan,
        )
        .is_some()
        {
            match Self::get_object_mid_size_reader_with_fileinfo(
                bucket,
                object,
                Arc::clone(&self.erasure_cache),
                fi,
                files,
                disks,
                self.set_index,
                self.pool_index,
                opts.skip_verify_bitrot,
                object_class.as_str(),
                size_bucket,
                false,
            )
            .await?
            {
                GetCodecStreamingReaderBuildOutcome::Reader(stream) => {
                    record_get_object_reader_path_observation(GET_OBJECT_PATH_MID_SIZE_STREAMING, object_class, size_bucket);
                    let (mut reader, _offset, _length) =
                        get_object_reader_with_context(&self.ctx, stream, range, &object_info, opts, &h).await?;
                    reader.body_source = body_source;
                    return Ok(finish_set_disk_read_lock(reader, read_lock_guard.take(), bucket, object));
                }
                GetCodecStreamingReaderBuildOutcome::Fallback(_) => {
                    // The setup found a degraded-but-readable layout. Let the
                    // established codec gate and then legacy path handle it;
                    // this preserves whole-request fallback semantics.
                }
            }
        }

        let path_decision_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let codec_streaming_gate = get_codec_streaming_reader_gate_with_plan(
            bucket,
            object,
            opts.part_number,
            object_class,
            &object_info,
            fi,
            lock_optimization_enabled,
            read_path_plan,
        );
        record_get_stage_duration_if_enabled(GET_OBJECT_PATH_SET_DISK, GET_STAGE_PATH_DECISION, path_decision_stage_start);

        match codec_streaming_gate.decision {
            GetCodecStreamingDecision::Use => {
                match Self::get_object_decode_reader_with_fileinfo(
                    bucket,
                    object,
                    Arc::clone(&self.erasure_cache),
                    fi,
                    files,
                    disks,
                    self.set_index,
                    self.pool_index,
                    opts.skip_verify_bitrot,
                    object_class.as_str(),
                    size_bucket,
                    codec_streaming_gate.prefer_data_blocks_first_reader_setup,
                )
                .await?
                {
                    GetCodecStreamingReaderBuildOutcome::Reader(stream) => {
                        record_get_codec_streaming_gate_decision(
                            codec_streaming_gate.object_class,
                            GetCodecStreamingDecision::Use,
                            size_bucket,
                        );
                        record_get_object_reader_path_observation(GET_OBJECT_PATH_CODEC_STREAMING, object_class, size_bucket);
                        let (mut reader, _offset, _length) =
                            get_object_reader_with_context(&self.ctx, stream, range, &object_info, opts, &h).await?;
                        // Carry the hook probe result so the app layer skips its
                        // now-redundant lookup on the streaming miss path (ODC-16).
                        reader.body_source = body_source;
                        return Ok(finish_set_disk_read_lock(reader, read_lock_guard.take(), bucket, object));
                    }
                    GetCodecStreamingReaderBuildOutcome::Fallback(reason) => {
                        record_get_codec_streaming_gate_decision(
                            codec_streaming_gate.object_class,
                            GetCodecStreamingDecision::Fallback(reason),
                            size_bucket,
                        );
                        rustfs_io_metrics::record_get_object_codec_streaming_fallback(reason.as_str());
                    }
                }
            }
            GetCodecStreamingDecision::Fallback(reason) => {
                record_get_codec_streaming_gate_decision(
                    codec_streaming_gate.object_class,
                    codec_streaming_gate.decision,
                    size_bucket,
                );
                rustfs_io_metrics::record_get_object_codec_streaming_fallback(reason.as_str());
            }
        }

        record_get_object_reader_path_observation(GET_OBJECT_PATH_LEGACY_DUPLEX, object_class, size_bucket);

        let duplex_buffer_size = adaptive_duplex_buffer_size(object_info.size);
        let (rd, wd) = tokio::io::duplex(duplex_buffer_size);
        debug!(bucket, object, duplex_buffer_size, "Created duplex pipe for object data transfer");

        let (producer_terminal_tx, producer_terminal_rx) = tokio::sync::oneshot::channel();
        let (mut reader, offset, length) =
            get_legacy_object_reader_with_context(&self.ctx, rd, producer_terminal_rx, range, &object_info, opts, &h).await?;
        // Carry the hook probe result so the app layer skips its now-redundant
        // lookup on the streaming miss path (ODC-16).
        reader.body_source = body_source;

        // The producer is otherwise detached from the returned reader. Tie its
        // lifetime to the source stream so a cancelled copy (or an abandoned
        // GET) releases in-flight shard opens, response bodies, and the read
        // lock immediately instead of waiting for a disk timeout.
        let producer_cancellation = crate::set_disk::get_object_read_cancellation();
        if let Some(cancellation) = producer_cancellation.as_ref() {
            reader.stream = Box::new(ProducerCancellationReader::new(reader.stream, cancellation.clone()));
        }

        // let disks = disks.clone();
        let bucket = bucket.to_owned();
        let object = object.to_owned();
        let set_index = self.set_index;
        let pool_index = self.pool_index;
        let skip_verify = opts.skip_verify_bitrot;
        // The producer runs in a separate Tokio task, so carry the caller's
        // read policy across the task boundary explicitly. Tokio task-local
        // values are not inherited by spawned tasks.
        let read_policy = crate::set_disk::get_object_read_policy();
        let erasure_cache = Arc::clone(&self.erasure_cache);
        let (fi, files, disks) = snapshot.into_owned();
        tokio::spawn(async move {
            let _guard = read_lock_guard;
            let mut writer = GetObjectDownstreamWriter::new(wd);
            // Do not wrap the entire read+write pipeline in `disk_read_timeout`.
            // `get_object_with_fileinfo` also waits on `writer`, so an outer timeout
            // would incorrectly treat downstream backpressure as disk-read latency.
            // Disk read timeouts must be enforced at the actual disk I/O operations.
            let producer_result = tokio::select! {
                biased;
                result = crate::set_disk::with_get_object_read_policy(
                    read_policy,
                    Self::get_object_with_fileinfo(
                        &bucket,
                        &object,
                        erasure_cache,
                        offset,
                        length,
                        &mut writer,
                        fi,
                        files,
                        &disks,
                        set_index,
                        pool_index,
                        skip_verify,
                        false,
                        false,
                        GET_OBJECT_PATH_LEGACY_DUPLEX,
                        object_class.as_str(),
                        size_bucket,
                    ),
                ) => result,
                _ = async {
                    if let Some(cancellation) = producer_cancellation.as_ref() {
                        cancellation.cancelled().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => Err(Error::OperationCanceled),
            };
            if let Err(e) = &producer_result
                && !matches!(e, Error::OperationCanceled)
            {
                let reason = classify_storage_error(e);
                if reason == GetObjectFailureReason::DownstreamClosed {
                    debug!(
                        event = EVENT_SET_DISK_WRITE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        bucket,
                        object,
                        pool_index,
                        set_index,
                        offset,
                        requested_length = length,
                        state = "downstream_closed",
                        stage = GET_STAGE_EMIT,
                        reason = reason.as_str(),
                        error = ?e,
                        "Set disk object read pipeline stopped after downstream closed"
                    );
                } else {
                    record_get_object_pipeline_failure(GET_STAGE_EMIT, reason);
                    error!(
                        event = EVENT_SET_DISK_WRITE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        bucket,
                        object,
                        pool_index,
                        set_index,
                        offset,
                        requested_length = length,
                        skip_verify_bitrot = skip_verify,
                        state = "read_pipeline_failed",
                        stage = GET_STAGE_EMIT,
                        reason = reason.as_str(),
                        error = ?e,
                        "Set disk object read pipeline failed"
                    );
                }
            };
            let _ = producer_terminal_tx.send(producer_result.map(|_| ()));
        });

        Ok(reader)
    }

    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.put_object_with_old_current_size(bucket, object, data, opts)
            .await
            .map(|(object_info, _)| object_info)
    }
}

/// `ReplicationState::target_delete_marker_version_ids` is skipped by the
/// positional `FileInfo` wire form, so a remote disk would otherwise receive a
/// delete with an empty map and lose the exact per-target version. Copy it into
/// the object's internal metadata — the durable carrier both sides already
/// agree on — before the delete is dispatched. Bounds mirror
/// `persist_target_delete_marker_versions`; anything outside them is dropped
/// rather than forwarded.
fn delete_file_info_with_replication_transport_metadata(fi: &FileInfo) -> FileInfo {
    let mut transported = fi.clone();
    let Some(state) = transported.replication_state_internal.as_ref() else {
        return transported;
    };
    if state.target_delete_marker_version_ids.len() > 1_000 {
        return transported;
    }
    for (arn, version_id) in &state.target_delete_marker_version_ids {
        if !arn.starts_with("arn:") || arn.len() > 1_024 || version_id.is_empty() || version_id.len() > 1_024 {
            continue;
        }
        let suffix = format!("{}{}", rustfs_utils::http::SUFFIX_REPLICATION_DELETE_MARKER_VERSION_ARN_PREFIX, arn);
        rustfs_utils::http::insert_str(&mut transported.metadata, &suffix, version_id.clone());
    }
    transported
}

/// True when an authorized replication write carries at least one per-category
/// source timestamp, i.e. receiver-side LWW has something to judge.
pub(in crate::set_disk) fn replication_lww_applicable(opts: &ObjectOptions) -> bool {
    opts.replication_request
        && (opts.replication_tagging_timestamp.is_some()
            || opts.replication_retention_timestamp.is_some()
            || opts.replication_legalhold_timestamp.is_some())
}

/// The stored per-category state of a destination version, as compared by
/// [`merge_replication_metadata_lww`]. `ObjectInfo::from_file_info`
/// externalizes tags into `user_tags` (stripping the metadata key), so the
/// tag value is folded back into map form here.
pub(in crate::set_disk) fn stored_replication_category_metadata(existing: &ObjectInfo) -> HashMap<String, String> {
    let mut stored = (*existing.user_defined).clone();
    if !existing.user_tags.is_empty() {
        stored.insert(rustfs_utils::http::headers::AMZ_OBJECT_TAGGING.to_string(), (*existing.user_tags).clone());
    }
    stored
}

/// Receiver-side last-writer-wins for authorized replication writes
/// (rustfs/backlog#1953, audit A4/P1-6). Metadata-only replication reuses the
/// whole-object transports, so in active-active topologies an inbound write
/// carries the source's tags / retention / legal hold verbatim and would
/// otherwise overwrite a category the destination modified more recently —
/// both sites end up permanently diverged while reporting COMPLETED.
///
/// Judged per category, only when the inbound request carries that category's
/// source timestamp (`ObjectOptions::replication_*_timestamp`):
/// - stored timestamp newer than inbound: the local category values and
///   timestamp are kept; the rest of the write proceeds per the inbound
///   metadata and the object-level result stays successful (failing the write
///   instead would loop through MRF, re-delivering the stale value forever);
/// - otherwise the inbound category wins and its internal timestamp key is
///   pinned to the source-authored time — the PUT path re-stamps the
///   object-lock timestamps with the receiver's clock
///   (`parse_object_lock_retention` / `parse_object_lock_legal_hold` insert
///   `now()` via `eval_metadata`), which would make the replica's clock the
///   LWW authority and wedge later convergence;
/// - no stored timestamp (pre-P1-6 data) or no inbound timestamp: the current
///   overwrite behavior is preserved.
///
/// Returns whether `inbound` was modified. Callers must hold the object write
/// lock so the stored values compared here are the ones being replaced.
pub(in crate::set_disk) fn merge_replication_metadata_lww(
    inbound: &mut HashMap<String, String>,
    existing: &HashMap<String, String>,
    opts: &ObjectOptions,
) -> bool {
    use rustfs_utils::http::headers::{
        AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, AMZ_OBJECT_TAGGING,
    };
    use rustfs_utils::http::metadata_compat::{
        SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, SUFFIX_TAGGING_TIMESTAMP, get_str,
        remove_str,
    };
    use time::format_description::well_known::Rfc3339;

    let categories: [(Option<OffsetDateTime>, &str, &[&str]); 3] = [
        (opts.replication_tagging_timestamp, SUFFIX_TAGGING_TIMESTAMP, &[AMZ_OBJECT_TAGGING]),
        (
            opts.replication_retention_timestamp,
            SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP,
            &[AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER],
        ),
        (
            opts.replication_legalhold_timestamp,
            SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP,
            &[AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER],
        ),
    ];

    let mut changed = false;
    for (inbound_timestamp, timestamp_suffix, value_keys) in categories {
        let Some(inbound_timestamp) = inbound_timestamp else { continue };
        let is_category_value_key = |key: &str| value_keys.iter().any(|value_key| key.eq_ignore_ascii_case(value_key));
        let stored_timestamp = get_str(existing, timestamp_suffix).and_then(|value| OffsetDateTime::parse(&value, &Rfc3339).ok());
        if stored_timestamp.is_some_and(|stored| stored > inbound_timestamp) {
            inbound.retain(|key, _| !is_category_value_key(key));
            remove_str(inbound, timestamp_suffix);
            for (key, value) in existing {
                if is_category_value_key(key) {
                    inbound.insert(key.clone(), value.clone());
                }
            }
            // Restore the winning timestamp via insert_str, not a verbatim key
            // copy: a MinIO-written version may carry only the
            // x-minio-internal- key, and the dual-key invariant requires every
            // write to produce both keys.
            if let Some(stored_value) = get_str(existing, timestamp_suffix) {
                rustfs_utils::http::insert_str(inbound, timestamp_suffix, stored_value);
            }
            changed = true;
        } else if let Ok(source_authored) = inbound_timestamp.format(&Rfc3339)
            && get_str(inbound, timestamp_suffix).as_deref() != Some(source_authored.as_str())
        {
            rustfs_utils::http::insert_str(inbound, timestamp_suffix, source_authored);
            changed = true;
        }
    }
    changed
}

impl SetDisks {
    pub(in crate::set_disk) async fn cleanup_rename_tail(
        &self,
        targets: Vec<RenameTailCleanup>,
        bucket: &str,
        object: &str,
        committed_data_dir: Option<Uuid>,
        epoch: Option<Uuid>,
    ) {
        for target in targets {
            let mut disks = vec![None; self.set_drive_count];
            disks[target.disk_index] = Some(target.disk);
            self.persist_old_data_cleanup_receipts(&disks, bucket, object, target.old_data_dir, committed_data_dir, epoch)
                .await;
            let cleanup = self
                .commit_rename_data_dir_and_mark_capacity(
                    &disks,
                    bucket,
                    object,
                    &target.old_data_dir.to_string(),
                    &committed_data_dir.unwrap_or_default().to_string(),
                    1,
                )
                .await;
            self.report_old_data_dir_cleanup(bucket, object, &target.old_data_dir.to_string(), &cleanup)
                .await;
        }
    }

    pub(in crate::set_disk) async fn persist_old_data_cleanup_receipts(
        &self,
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        old_data_dir: Uuid,
        committed_data_dir: Option<Uuid>,
        epoch: Option<Uuid>,
    ) {
        let Some(epoch) = epoch else { return };
        if committed_data_dir == Some(old_data_dir) {
            return;
        }
        let receipt = OldDataCleanupReceipt::new(epoch, old_data_dir, committed_data_dir);
        let Ok(encoded) = receipt.encode() else {
            return;
        };
        let path = old_data_cleanup_receipt_path(object, old_data_dir);
        let futures = disks.iter().filter_map(|disk| {
            disk.as_ref().map(|disk| {
                let disk = disk.clone();
                let encoded = encoded.clone();
                let bucket = bucket.to_owned();
                let path = path.clone();
                async move { disk.write_all(&bucket, &path, encoded).await }
            })
        });
        for result in join_all(futures).await {
            if let Err(err) = result {
                debug!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    old_dir = %old_data_dir,
                    error = %err,
                    state = "cleanup_receipt_persist_failed",
                    "SetDisk old-data cleanup receipt persist failed"
                );
            }
        }
    }

    pub(in crate::set_disk) async fn reconcile_old_data_cleanup_receipts(
        &self,
        bucket: &str,
        object: &str,
    ) -> disk::error::Result<usize> {
        if object_transaction_fencing_fleet_proof().is_none() {
            return Ok(0);
        }
        let current = read_object_transaction_epoch_fence(self, bucket, object)
            .await
            .map_err(|e| e.narrow_to_disk().unwrap_or_else(DiskError::other))?;
        let disks = self.get_disks_internal().await;
        let mut removed = 0usize;

        for disk in disks.iter().flatten() {
            let entries = match disk.list_dir("", bucket, object, 0).await {
                Ok(entries) => entries,
                Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => continue,
                Err(err) => return Err(err),
            };
            for entry in entries {
                let Some(name) = entry.strip_suffix(SLASH_SEPARATOR) else { continue };
                let Ok(data_dir) = Uuid::parse_str(name) else { continue };
                if data_dir.is_nil() {
                    continue;
                }

                let receipt_path = old_data_cleanup_receipt_path(object, data_dir);
                let receipt_bytes = match disk.read_all(bucket, &receipt_path).await {
                    Ok(bytes) => bytes,
                    Err(DiskError::FileNotFound | DiskError::FileVersionNotFound | DiskError::VolumeNotFound) => continue,
                    Err(err) => return Err(err),
                };
                let receipt = OldDataCleanupReceipt::decode(&receipt_bytes)?;
                if receipt.old_data_dir != data_dir
                    || receipt.committed_data_dir == Some(receipt.old_data_dir)
                    || !old_data_cleanup_receipt_epoch_matches_current(receipt, current)
                {
                    continue;
                }

                let old_path = format!("{object}/{}", receipt.old_data_dir);
                match disk
                    .delete_data_dir(
                        bucket,
                        &old_path,
                        DeleteOptions {
                            recursive: true,
                            immediate: true,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(DataDirDeleteStatus::Deleted) => removed += 1,
                    Ok(DataDirDeleteStatus::Deferred) => {}
                    Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => {}
                    Err(err) => return Err(err),
                }
            }
        }

        Ok(removed)
    }

    async fn validate_bucket_incarnation(&self, bucket: &str, expected: Uuid) -> Result<()> {
        let current = metadata_sys::get_bucket_incarnation_id_in(&self.ctx, bucket).await?;
        if current != expected {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        Ok(())
    }

    /// `put_object` plus the destination key's previous current-version size,
    /// quorum-reduced from the dst `xl.meta` copies `rename_data` reads while
    /// committing (rustfs/backlog#1009). `None` means unknown (mixed-version
    /// peers, unparsable metadata, or sub-quorum divergence) — callers must
    /// fall back to degraded accounting, never assume "absent". The extra
    /// value is deliberately *not* part of `ObjectInfo`, which feeds S3
    /// responses, event payloads, replication, and ILM verbatim.
    #[tracing::instrument(skip(self, data,))]
    pub async fn put_object_with_old_current_size(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<(ObjectInfo, Option<OldCurrentSize>)> {
        self.put_object_with_old_current_size_boxed(bucket, object, data, opts, None)
            .await
    }

    pub(crate) async fn put_object_with_old_current_size_for_data_movement(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
        publication_fence: RemoteTuplePublicationFence,
    ) -> Result<(ObjectInfo, Option<OldCurrentSize>)> {
        if !opts.data_movement {
            return Err(Error::other("publication-fenced PUT requires data_movement options"));
        }
        let mut opts = opts.clone();
        // The owned publication capability supplies the exact target write
        // domain at commit time; the generic PUT path must not reacquire it.
        opts.no_lock = true;
        self.put_object_with_old_current_size_boxed(bucket, object, data, &opts, Some(publication_fence))
            .await
    }

    fn put_object_with_old_current_size_boxed<'a>(
        &'a self,
        bucket: &'a str,
        object: &'a str,
        data: &'a mut PutObjReader,
        opts: &'a ObjectOptions,
        publication_fence: Option<RemoteTuplePublicationFence>,
    ) -> impl Future<Output = Result<(ObjectInfo, Option<OldCurrentSize>)>> + Send + 'a {
        Box::pin(self.put_object_with_old_current_size_inner(bucket, object, data, opts, publication_fence))
    }

    async fn put_object_with_old_current_size_inner(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
        mut publication_fence: Option<RemoteTuplePublicationFence>,
    ) -> Result<(ObjectInfo, Option<OldCurrentSize>)> {
        if publication_fence.is_none()
            && opts.data_movement
            && rustfs_utils::http::metadata_compat::contains_key_str(
                &opts.user_defined,
                rustfs_utils::http::SUFFIX_TRANSITION_STATUS,
            )
        {
            return Err(Error::other(
                "data movement cannot publish transition ownership without a publication capability",
            ));
        }
        crate::hp_guard!("SetDisks::put_object");
        let mut scope_outcome_guard = opts
            .scanner_publication_commit_scope
            .clone()
            .map(ScannerPublicationCommitScopeGuard::new);
        let storage_class_config = self.storage_class_config_snapshot();
        self.invalidate_get_object_metadata_cache(bucket, object).await;

        let disks = self.get_disks_internal().await;

        let mut object_lock_guard = None;
        let mut decommission_object_lock_guard = None;
        let mut decommission_target_lock_covered = false;
        let mut decommission_capacity_guard = None;
        let mut bucket_lifecycle_guard = None;
        let mut publication_commit_guard: Option<RemoteTuplePublicationCommitGuard> = None;

        // This pre-body check is advisory fast-fail only: the authoritative
        // precondition evaluation happens under the commit namespace lock
        // below, so the namespace write lock must NOT be taken here — holding
        // it across client-paced body ingestion starves concurrent reads of
        // the same object into lock-timeout 503s (rustfs/backlog#2074).
        // Data movement skips the advisory read: its staleness predicate is
        // only meaningful at commit time.
        if opts.http_preconditions.is_some()
            && !opts.data_movement
            && let Some(err) = self.check_write_precondition(bucket, object, opts).await
        {
            return Err(err);
        }

        let expected_restore_operation_id = restore_commit_operation_id_from_metadata(&opts.user_defined)?;
        let mut user_defined = opts.user_defined.clone();
        if let Some(eval_metadata) = &opts.eval_metadata {
            merge_evaluated_metadata(&mut user_defined, eval_metadata)?;
        }
        let scanner_publication_lease_tokens = take_scanner_publication_lease_tokens(&mut user_defined)?;
        if replication_lww_applicable(opts) {
            // Object Lock evaluation stamps category timestamps with this
            // receiver's clock. Pin them back to the source-authored times
            // before the first copy of a version is committed; the existing-
            // version branch below may still replace them with newer local
            // state.
            merge_replication_metadata_lww(&mut user_defined, &HashMap::new(), opts);
        }
        if expected_restore_operation_id.is_some() {
            rustfs_utils::http::metadata_compat::remove_str(&mut user_defined, SUFFIX_RESTORE_OPERATION_ID);
            rustfs_utils::http::metadata_compat::remove_str(&mut user_defined, SUFFIX_RESTORE_WORKER_LOCK);
        }
        let WriteLayout {
            data_drives,
            parity_drives,
            write_quorum,
        } = resolve_write_layout(
            &storage_class_config,
            self.pool_index,
            disks.len(),
            self.default_parity_count,
            user_defined.get(AMZ_STORAGE_CLASS).map(String::as_str),
            opts.max_parity,
        )?;

        // if filtered_online < write_quorum {
        //     warn!(
        //         "online disk snapshot {} below write quorum {} for {}/{}; returning erasure write quorum error",
        //         filtered_online, write_quorum, bucket, object
        //     );
        //     return Err(to_object_err(Error::ErasureWriteQuorum, vec![bucket, object]));
        // }

        let mut fi = FileInfo::new([bucket, object].join("/").as_str(), data_drives, parity_drives);

        fi.version_id = {
            if let Some(ref vid) = opts.version_id {
                Some(Uuid::parse_str(vid.as_str()).map_err(Error::other)?)
            } else {
                None
            }
        };

        if opts.versioned && fi.version_id.is_none() {
            fi.version_id = Some(Uuid::new_v4());
        }

        fi.data_dir = Some(Uuid::new_v4());

        // Never inherit a frame-layout marker: an incoming one (replication
        // passthrough, data movement) describes some other object identity and
        // this write mints a fresh data_dir. Stamp only when this put actually
        // encrypts its stream locally, which is when the v2 write switch is on.
        rustfs_utils::http::metadata_compat::remove_str(
            &mut user_defined,
            crate::object_api::ENCRYPTED_FRAME_LAYOUT_FIXED8K_SUFFIX,
        );
        #[cfg(not(feature = "rio-v2"))]
        if crate::io_support::rio::encryption_frame_v2_enabled()
            && !opts.preserve_ciphertext
            && !opts.data_movement
            && should_persist_encryption_original_size(&user_defined)
            && let Some(data_dir) = fi.data_dir.filter(|data_dir| !data_dir.is_nil())
        {
            insert_str(
                &mut user_defined,
                crate::object_api::ENCRYPTED_FRAME_LAYOUT_FIXED8K_SUFFIX,
                data_dir.to_string(),
            );
        }

        let mut shuffle_disks = Self::shuffle_disks_owned(disks, &fi.erasure.distribution);

        let tmp_dir = Uuid::new_v4().to_string();

        let tmp_object = format!("{}/{}/part.1", tmp_dir, fi.data_dir.unwrap());

        let (operation_cancellation, mut commit_cancellation_handoff_tx, cancellation_wait) =
            if let Some(cancellation) = opts.put_object_cancellation.clone() {
                let (handoff_tx, handoff_rx) = tokio::sync::oneshot::channel();
                (Some(cancellation.clone()), Some(handoff_tx), Some((cancellation, handoff_rx)))
            } else {
                (None, None, None)
            };
        let mut tmp_cleanup_owned = false;
        let rollback_receipt = RenameRollbackReceipt::default();
        let operation = async {
            let erasure = Arc::new(erasure_from_file_info(&fi, false)?);

            let put_object_size = known_put_object_storage_size(data.size());
            let shard_file_size_raw = erasure.shard_file_size(put_object_size);
            let is_inline_buffer = storage_class_config.should_inline(shard_file_size_raw, erasure.data_shards, opts.versioned);

            let collect_stage_timing = rustfs_io_metrics::put_stage_metrics_enabled() || issue3031_diag_enabled();
            let shard_file_size = shard_file_size_raw;
            let shard_size = erasure.shard_size();
            let write_path = classify_put_write_path(is_inline_buffer, put_object_size, fi.erasure.block_size);
            let direct_inline_commit = matches!(write_path, SmallWritePath::Inline);
            rustfs_io_metrics::record_put_object_path(write_path.metric_label());
            let writer_setup_stage_start = collect_stage_timing.then(Instant::now);
            let (mut writers, errors) = if direct_inline_commit {
                let online = join_all(shuffle_disks.iter().map(|disk| async move {
                    if let Some(disk) = disk {
                        disk.is_online().await
                    } else {
                        false
                    }
                }))
                .await;
                let mut errors = Vec::with_capacity(online.len());
                for (disk, is_online) in shuffle_disks.iter_mut().zip(online) {
                    if is_online {
                        errors.push(None);
                    } else {
                        *disk = None;
                        errors.push(Some(DiskError::DiskNotFound));
                    }
                }
                (std::iter::repeat_with(|| None).take(shuffle_disks.len()).collect(), errors)
            } else {
                let writer_futs: Vec<_> = shuffle_disks
                    .iter()
                    .map(|disk_op| {
                        let tmp_obj = tmp_object.clone();
                        async move {
                            if let Some(disk) = disk_op
                                && disk.is_online().await
                            {
                                match create_bitrot_writer(
                                    is_inline_buffer,
                                    Some(disk),
                                    RUSTFS_META_TMP_BUCKET,
                                    &tmp_obj,
                                    shard_file_size,
                                    shard_size,
                                    HashAlgorithm::HighwayHash256S,
                                )
                                .await
                                {
                                    Ok(writer) => (Some(writer), None),
                                    Err(err) => {
                                        warn!(
                                            event = EVENT_SET_DISK_WRITE,
                                            component = LOG_COMPONENT_ECSTORE,
                                            subsystem = LOG_SUBSYSTEM_SET_DISK,
                                            disk = ?disk,
                                            state = "bitrot_writer_skipped",
                                            error = ?err,
                                            "Set disk bitrot writer skipped"
                                        );
                                        (None, Some(err))
                                    }
                                }
                            } else {
                                (None, Some(DiskError::DiskNotFound))
                            }
                        }
                    })
                    .collect();
                let writer_results = join_all(writer_futs).await;
                let mut writers = Vec::with_capacity(writer_results.len());
                let mut errors = Vec::with_capacity(writer_results.len());
                for (writer, error) in writer_results {
                    writers.push(writer);
                    errors.push(error);
                }
                (writers, errors)
            };
            let writer_setup_elapsed = writer_setup_stage_start.map(|stage_start| stage_start.elapsed());
            let writer_setup_ms = writer_setup_elapsed
                .map(|elapsed| elapsed.as_millis() as u64)
                .unwrap_or_default();
            if let Some(writer_setup_elapsed) = writer_setup_elapsed {
                rustfs_io_metrics::record_put_object_stage_duration(
                    "set_disk_writer_setup",
                    duration_millis_f64(writer_setup_elapsed),
                );
            }

            let nil_count = errors.iter().filter(|&e| e.is_none()).count();
            if nil_count < write_quorum {
                error!(
                    event = EVENT_SET_DISK_WRITE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    write_quorum,
                    available_writers = nil_count,
                    state = "write_quorum_unavailable",
                    error = ?errors,
                    "Set disk write quorum unavailable"
                );
                if let Some(write_err) = reduce_write_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, write_quorum) {
                    return Err(to_object_err(write_err.into(), vec![bucket, object]));
                }

                return Err(Error::other(format!("not enough disks to write: {errors:?}")));
            }

            let stream = mem::replace(
                &mut data.stream,
                HashReader::from_stream(Cursor::new(Vec::new()), 0, 0, None, None, false)?,
            );

            let small_size_hint = if matches!(write_path, SmallWritePath::Inline | SmallWritePath::SingleBlockNonInline) {
                usize::try_from(put_object_size).map_err(Error::other)?
            } else {
                0
            };

            let encode_stage_start = collect_stage_timing.then(Instant::now);
            let mut inline_shards = None;
            let (reader, w_size) = match write_path {
                SmallWritePath::Inline => match Arc::clone(&erasure)
                    .encode_inline_shards_with_size_hint(stream, small_size_hint)
                    .await
                {
                    Ok((r, w, shards)) => {
                        inline_shards = Some(shards);
                        (r, w)
                    }
                    Err(e) => {
                        error!("encode_inline_small err {:?}", e);
                        return Err(e.into());
                    }
                },
                SmallWritePath::SingleBlockNonInline => match Arc::clone(&erasure)
                    .encode_single_block_non_inline_with_size_hint(stream, &mut writers, write_quorum, small_size_hint)
                    .await
                {
                    Ok((r, w)) => (r, w),
                    Err(e) => {
                        error!("encode_single_block_non_inline err {:?}", e);
                        return Err(e.into());
                    }
                },
                SmallWritePath::PipelineBatchedLarge => {
                    match Arc::clone(&erasure).encode_batched(stream, &mut writers, write_quorum).await {
                        Ok((r, w)) => (r, w),
                        Err(e) => {
                            error!("encode_batched err {:?}", e);
                            return Err(e.into());
                        }
                    }
                }
                SmallWritePath::Pipeline => match Arc::clone(&erasure).encode(stream, &mut writers, write_quorum).await {
                    Ok((r, w)) => (r, w),
                    Err(e) => {
                        error!("encode err {:?}", e);
                        return Err(e.into());
                    }
                },
            };
            let encode_elapsed = encode_stage_start.map(|stage_start| stage_start.elapsed());
            let encode_ms = encode_elapsed.map(|elapsed| elapsed.as_millis() as u64).unwrap_or_default();
            if let Some(encode_elapsed) = encode_elapsed {
                rustfs_io_metrics::record_put_object_stage_duration("set_disk_encode", duration_millis_f64(encode_elapsed));
            }

            let _ = mem::replace(&mut data.stream, reader);
            // if let Err(err) = close_bitrot_writers(&mut writers).await {
            //     error!("close_bitrot_writers err {:?}", err);
            // }

            if (w_size as i64) < data.size() {
                warn!(
                    event = EVENT_SET_DISK_WRITE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    written_size = w_size,
                    expected_size = data.size(),
                    state = "short_write",
                    "Set disk write produced fewer bytes than expected"
                );
                return Err(Error::other(format!(
                    "put_object write size < data.size(), w_size={}, data.size={}",
                    w_size,
                    data.size()
                )));
            }

            if contains_key_str(&user_defined, SUFFIX_COMPRESSION) {
                insert_str(&mut user_defined, SUFFIX_COMPRESSION_SIZE, w_size.to_string());
            }

            let index_op = data
                .stream
                .try_get_index()
                .map(crate::io_support::rio::compression_index_storage_bytes);

            let mut etag = data.stream.try_resolve_etag().unwrap_or_default();
            if let Some(ref tag) = opts.preserve_etag {
                etag = tag.clone();
            }

            user_defined.insert("etag".to_owned(), etag.clone());

            if !user_defined.contains_key("content-type") {
                //  get content-type
            }

            let mut actual_size = data.actual_size();
            if actual_size < 0 {
                let is_compressed = fi.is_compressed();
                if !is_compressed {
                    actual_size = w_size as i64;
                }
            }

            // SSE-C replication carries the source object's sealed checksum
            // out of band; store it verbatim like the multipart path does.
            if let Some(cssum) =
                rustfs_utils::http::get_header_map(&user_defined, rustfs_utils::http::SUFFIX_REPLICATION_SSEC_CRC)
                && !cssum.is_empty()
            {
                fi.checksum = base64_simd::STANDARD.decode_to_vec(&cssum).ok().map(bytes::Bytes::from);
                rustfs_utils::http::remove_header_map(&mut user_defined, rustfs_utils::http::SUFFIX_REPLICATION_SSEC_CRC);
            }

            if fi.checksum.is_none()
                && let Some(content_hash) = data.as_hash_reader().content_hash()
            {
                fi.checksum = Some(content_hash.to_bytes(&[]));
            }

            if let Some(sc) = user_defined.get(AMZ_STORAGE_CLASS)
                && sc == storageclass::STANDARD
            {
                let _ = user_defined.remove(AMZ_STORAGE_CLASS);
            }

            let mod_time = opts.mod_time;

            // Drop any disk whose shard did not fully commit (offline at writer
            // setup, short write, or a write/shutdown error) so its truncated or
            // absent shard is not renamed into place and counted toward write
            // quorum. Otherwise redundancy is inflated: the object claims N good
            // shards but one is short/corrupt, so a single later disk failure can
            // drop it below reconstructable quorum (backlog#852 / #799 B3).
            // `rename_data` re-checks write quorum over the surviving disks and
            // rolls back if too few remain.
            let committed_shards = if matches!(write_path, SmallWritePath::Inline) {
                shuffle_disks.iter().filter(|disk| disk.is_some()).count()
            } else {
                drop_failed_writer_disks(&mut shuffle_disks, &writers)
            };
            if committed_shards < write_quorum {
                return Err(Error::other(format!(
                    "put_object write quorum unavailable after encode: {committed_shards} shard(s) committed, need {write_quorum}"
                )));
            }

            fi.metadata = user_defined;
            fi.mod_time = mod_time;
            fi.size = w_size as i64;
            fi.versioned = opts.versioned || opts.version_suspended;
            fi.add_object_part(1, etag, w_size, mod_time, actual_size, index_op, None);
            if opts.data_movement {
                fi.set_data_moved();
            }
            let parity_blocks = fi.erasure.parity_blocks;

            let response_metadata_slot = shuffle_disks
                .iter()
                .rposition(Option::is_some)
                .ok_or_else(|| Error::other("put_object write quorum unavailable after encode"))?;
            let mut base_file_info = fi;
            let mut parts_metadatas = Vec::with_capacity(shuffle_disks.len());
            for (i, disk) in shuffle_disks.iter().enumerate() {
                if disk.is_none() {
                    parts_metadatas.push(FileInfo::default());
                    continue;
                }

                let mut pfi = if i == response_metadata_slot {
                    std::mem::take(&mut base_file_info)
                } else {
                    base_file_info.clone()
                };
                if is_inline_buffer {
                    if let Some(shards) = inline_shards.as_ref() {
                        pfi.data = Some(
                            shards
                                .get(i)
                                .cloned()
                                .ok_or_else(|| Error::other(format!("inline encoder omitted disk shard {i}")))?,
                        );
                    } else if let Some(writer) = writers[i].take() {
                        pfi.data = Some(writer.into_inline_data().map(Bytes::from).unwrap_or_default());
                    }

                    pfi.set_inline_data();
                }
                parts_metadatas.push(pfi);
            }
            let committed_version_id = parts_metadatas[response_metadata_slot].version_id;
            let committed_data_dir = parts_metadatas[response_metadata_slot].data_dir;
            let is_compressed = parts_metadatas[response_metadata_slot].is_compressed();

            drop(writers); // drop writers to close all files, this is to prevent FileAccessDenied errors when renaming data

            if parity_blocks == 0 {
                let written_size = i64::try_from(w_size).map_err(|_| Error::other("put_object written size overflows i64"))?;
                let logical_shard_size = usize::try_from(erasure.shard_file_size(written_size))
                    .map_err(|_| Error::other("put_object shard size overflows usize"))?;
                verify_written_bitrot_shards(
                    &shuffle_disks,
                    if is_inline_buffer {
                        Some(parts_metadatas.as_slice())
                    } else {
                        None
                    },
                    BitrotSelfVerifyTarget {
                        operation: "put_object",
                        bucket,
                        object,
                        part_number: None,
                        volume: RUSTFS_META_TMP_BUCKET,
                        path: &tmp_object,
                        logical_shard_size,
                        shard_size,
                        write_quorum,
                    },
                )
                .await?;
            }

            if let Some(store) = opts.decommission_capacity_admission.as_ref() {
                #[cfg(test)]
                {
                    crate::core::pools::notify_decommission_external_object_commit_phase_started(store.id);
                    crate::core::pools::wait_for_decommission_external_object_commit_phase_release(store.id).await;
                }
                let (object_guard, target_lock_covered, capacity_guard) = store
                    .acquire_external_decommission_commit_guards(
                        self.pool_index,
                        bucket,
                        object,
                        opts.no_lock || object_lock_guard.is_some(),
                    )
                    .await?;
                decommission_object_lock_guard = object_guard;
                decommission_target_lock_covered = target_lock_covered;
                decommission_capacity_guard = capacity_guard;
            }
            if publication_fence.is_some() || (!opts.no_lock && object_lock_guard.is_none() && !decommission_target_lock_covered)
            {
                #[cfg(any(test, feature = "test-util"))]
                pause_put_object_commit(bucket, object, PutObjectCommitPause::BeforeNamespace).await;
                if let Some(expected_incarnation_id) = opts.expected_bucket_incarnation_id
                    && opts.bucket_lifecycle_lock_fence.is_none()
                {
                    bucket_lifecycle_guard = Some(
                        metadata_sys::object_store_in(&self.ctx)
                            .await?
                            .acquire_bucket_incarnation_fence(bucket, expected_incarnation_id)
                            .await?,
                    );
                }
                if let Some(fence) = publication_fence.take() {
                    publication_commit_guard = Some(fence.into_commit_guard(self.pool_index, bucket, object).await?);
                } else {
                    #[cfg(any(test, feature = "test-util"))]
                    {
                        object_lock_guard = Some(
                            self.acquire_write_lock_diag_with_pending_hook("put_object_commit", bucket, object, || {
                                notify_put_object_commit_namespace_pending(bucket, object);
                            })
                            .await?,
                        );
                        notify_put_object_commit_namespace_acquired(bucket, object);
                    }
                    #[cfg(not(any(test, feature = "test-util")))]
                    {
                        object_lock_guard = Some(self.acquire_write_lock_diag("put_object_commit", bucket, object).await?);
                    }
                }
            }
            #[cfg(any(test, feature = "test-util"))]
            pause_put_object_commit(bucket, object, PutObjectCommitPause::AfterNamespace).await;

            if opts.http_preconditions.is_some()
                && let Some(err) = self.check_write_precondition(bucket, object, opts).await
            {
                return Err(err);
            }

            // Generate ordinary PUT timestamps under the commit lock so version
            // ordering follows durable commit ordering when writers queued on
            // the same object. Internal callers with an explicit timestamp keep
            // their supplied value.
            if opts.mod_time.is_none() {
                let commit_time = Some(OffsetDateTime::now_utc());
                for pfi in &mut parts_metadatas {
                    pfi.mod_time = commit_time;
                    for part in &mut pfi.parts {
                        part.mod_time = commit_time;
                    }
                }
            }

            if let Some(expected) = opts.expected_current_version_id.as_deref() {
                let current = self
                    .get_object_info(
                        bucket,
                        object,
                        &ObjectOptions {
                            no_lock: true,
                            metadata_cache_safe: false,
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|err| {
                        if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                            StorageError::PreconditionFailed
                        } else {
                            err
                        }
                    })?;
                if current.version_id.map(|version| version.to_string()).as_deref() != Some(expected) {
                    return Err(StorageError::PreconditionFailed);
                }
            }

            if let Some(version_id) = opts.version_id.as_deref()
                && !is_meta_bucketname(bucket)
            {
                let current = self
                    .get_object_info(
                        bucket,
                        object,
                        &ObjectOptions {
                            version_id: Some(version_id.to_string()),
                            no_lock: true,
                            metadata_cache_safe: false,
                            versioned: opts.versioned,
                            version_suspended: opts.version_suspended,
                            ..Default::default()
                        },
                    )
                    .await;
                match current {
                    Ok(existing) => {
                        let object_lock_config = opts.object_lock_config_snapshot.as_deref().ok_or_else(|| {
                            Error::other("explicit-version PUT is missing its Object Lock configuration snapshot")
                        })?;
                        // The WORM gate protects the locked version from local
                        // overwrites; an authorized replication write passes it
                        // only when the LWW merge below will judge every
                        // locking category (see
                        // `replication_write_may_pass_worm_gate`, which judges
                        // the same authoritative lock state as the gate,
                        // bucket default retention included). Gate first so
                        // malformed lock metadata still fails closed.
                        if check_object_lock_for_deletion_with_state(object_lock_config.state(), &existing, false)?.is_some()
                            && !replication_write_may_pass_worm_gate(object_lock_config.state(), &existing, opts)?
                        {
                            return Err(StorageError::PrefixAccessDenied(bucket.to_string(), object.to_string()));
                        }
                        // Receiver-side LWW (rustfs/backlog#1953): reuse this
                        // commit-lock read of the destination version so a
                        // category (tags / retention / legal hold) modified
                        // more recently on this site is kept instead of being
                        // overwritten by the inbound replication metadata.
                        if replication_lww_applicable(opts) {
                            let stored = stored_replication_category_metadata(&existing);
                            let mut merged = parts_metadatas[response_metadata_slot].metadata.clone();
                            if merge_replication_metadata_lww(&mut merged, &stored, opts) {
                                for (pfi, disk) in parts_metadatas.iter_mut().zip(shuffle_disks.iter()) {
                                    if disk.is_some() {
                                        pfi.metadata = merged.clone();
                                    }
                                }
                            }
                        }
                    }
                    Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => {}
                    Err(err) => return Err(err),
                }
            }

            self.require_current_restore_operation_id(bucket, object, opts, expected_restore_operation_id, "put_object_commit")
                .await?;

            // Fence every commit-time read before entering rename_data. Once
            // rename_data returns Ok the write is durable and must not be aborted.
            if object_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                || publication_commit_guard
                    .as_ref()
                    .is_some_and(RemoteTuplePublicationCommitGuard::is_lock_lost)
            {
                return Err(StorageError::NamespaceLockQuorumUnavailable {
                    mode: "put_object_commit",
                    bucket: bucket.to_string(),
                    object: object.to_string(),
                    required: 1,
                    achieved: 0,
                });
            }

            if opts
                .namespace_lock_fence
                .as_ref()
                .is_some_and(NamespaceLockFence::is_lock_lost)
            {
                return Err(StorageError::NamespaceLockQuorumUnavailable {
                    mode: "put_object_outer_lock",
                    bucket: bucket.to_string(),
                    object: object.to_string(),
                    required: 1,
                    achieved: 0,
                });
            }

            if opts
                .bucket_lifecycle_lock_fence
                .as_ref()
                .is_some_and(NamespaceLockFence::is_lock_lost)
                || bucket_lifecycle_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
            {
                return Err(StorageError::NamespaceLockQuorumUnavailable {
                    mode: "put_object_bucket_generation",
                    bucket: bucket.to_string(),
                    object: object.to_string(),
                    required: 1,
                    achieved: 0,
                });
            }

            if decommission_capacity_guard.is_none()
                && let Some(store) = opts.decommission_capacity_admission.as_ref()
            {
                decommission_capacity_guard = Some(
                    store
                        .acquire_external_decommission_capacity_fence(&[self.pool_index], "mutation")
                        .await?,
                );
            }

            // The object namespace is acquired above, after the input stream
            // has been fully staged. Only then admit the local publication
            // against the decommission capacity ledger; holding this guard
            // through rename_data keeps the namespace -> capacity order.
            if decommission_object_lock_guard
                .as_ref()
                .is_some_and(|guard| guard.is_lock_lost())
            {
                return Err(StorageError::NamespaceLockQuorumUnavailable {
                    mode: "put_object_external_namespace",
                    bucket: bucket.to_string(),
                    object: object.to_string(),
                    required: 1,
                    achieved: 0,
                });
            }

            let transaction_fencing_proof = object_transaction_fencing_fleet_proof();
            if object_transaction_fencing_requested() && transaction_fencing_proof.is_none() {
                return Err(Error::other("object transaction fencing requires a live fleet capability proof"));
            }
            let transaction_epoch_fence = if transaction_fencing_proof.is_some() {
                Some(read_object_transaction_epoch_fence(self, bucket, object).await?)
            } else {
                None
            };

            let quota_context = reservation::begin(
                &self.ctx,
                bucket,
                object,
                opts.quota_admission,
                opts.data_movement,
                self.pool_index,
                self.set_index,
            )
            .await?;
            let quota_mutation_fence = quota_context.is_enforced() || opts.quota_admission.is_some();
            let mut replication_quota_size = None;

            if opts.replication_request {
                if quota_context.is_enforced() && opts.preserve_ciphertext {
                    return Err(Error::PartMissingOrCorrupt);
                }
                if quota_context.is_enforced() {
                    let persisted_metadata = &parts_metadatas[response_metadata_slot].metadata;
                    let observed_size = u64::try_from(actual_size).map_err(|_| Error::PartMissingOrCorrupt)?;
                    let physical_size = u64::try_from(w_size).map_err(|_| Error::PartMissingOrCorrupt)?;
                    let transformed = contains_key_str(persisted_metadata, SUFFIX_COMPRESSION)
                        || should_persist_encryption_original_size(persisted_metadata);
                    let declared_size = get_str(persisted_metadata, SUFFIX_ACTUAL_SIZE)
                        .map(|value| value.parse::<u64>().map_err(|_| Error::PartMissingOrCorrupt))
                        .transpose()?
                        .unwrap_or(0);
                    let declared_encryption_size = rustfs_utils::http::get_object_encryption_original_size(persisted_metadata)
                        .map_err(Error::other)?
                        .map(u64::try_from)
                        .transpose()
                        .map_err(|_| Error::PartMissingOrCorrupt)?
                        .unwrap_or(0);
                    let logical_size = observed_size.max(declared_size).max(declared_encryption_size);
                    let persisted_size = if transformed {
                        logical_size
                    } else {
                        logical_size.max(physical_size)
                    };
                    replication_quota_size = Some(logical_size.max(physical_size));
                    actual_size = i64::try_from(persisted_size).map_err(|_| Error::PartMissingOrCorrupt)?;
                    for metadata in &mut parts_metadatas {
                        insert_str(&mut metadata.metadata, SUFFIX_ACTUAL_SIZE, persisted_size.to_string());
                        if should_persist_encryption_original_size(&metadata.metadata) {
                            metadata
                                .metadata
                                .insert("x-rustfs-encryption-original-size".to_string(), persisted_size.to_string());
                        }
                        if let Some(part) = metadata.parts.first_mut() {
                            part.actual_size = actual_size;
                        }
                    }
                }
            } else if actual_size >= 0 {
                let observed_size = u64::try_from(actual_size).map_err(|_| Error::PartMissingOrCorrupt)?;
                let persisted_metadata = &parts_metadatas[response_metadata_slot].metadata;
                let transformed = contains_key_str(persisted_metadata, SUFFIX_COMPRESSION)
                    || should_persist_encryption_original_size(persisted_metadata);
                let server_observed_size = if transformed {
                    observed_size
                } else {
                    observed_size.max(u64::try_from(w_size).map_err(|_| Error::PartMissingOrCorrupt)?)
                };
                actual_size = i64::try_from(server_observed_size).map_err(|_| Error::PartMissingOrCorrupt)?;
                for metadata in &mut parts_metadatas {
                    insert_str(&mut metadata.metadata, SUFFIX_ACTUAL_SIZE, server_observed_size.to_string());
                    if should_persist_encryption_original_size(&metadata.metadata) {
                        metadata
                            .metadata
                            .insert("x-rustfs-encryption-original-size".to_string(), server_observed_size.to_string());
                    }
                    if let Some(part) = metadata.parts.first_mut() {
                        part.actual_size = actual_size;
                    }
                }
            }

            let (quota_old_size, quota_new_size) = if quota_context.is_enforced() {
                let new_size = match replication_quota_size {
                    Some(size) => size,
                    None => u64::try_from(actual_size)
                        .map_err(|_| Error::PartMissingOrCorrupt)?
                        .max(u64::try_from(w_size).map_err(|_| Error::PartMissingOrCorrupt)?),
                };
                let old_size = if opts.data_movement {
                    new_size
                } else {
                    reservation::replaced_logical_size(self, bucket, object, opts).await?
                };
                (old_size, new_size)
            } else {
                (0, 0)
            };
            let quota_reservation = quota_context.reserve(quota_old_size, quota_new_size).await?;
            let (commit_disks, quota_fence_tokens) = if quota_mutation_fence {
                match Self::prepare_quota_mutation_fences(&shuffle_disks, bucket, object, write_quorum).await {
                    Ok((disks, tokens)) => {
                        for (metadata, token) in parts_metadatas.iter_mut().zip(tokens.iter().copied()) {
                            if let Some(token) = token {
                                insert_str(
                                    &mut metadata.metadata,
                                    crate::disk::QUOTA_MUTATION_FENCE_METADATA_SUFFIX,
                                    token.as_uuid().to_string(),
                                );
                            }
                        }
                        (disks, tokens)
                    }
                    Err(err) => {
                        quota_reservation.abort().await;
                        return Err(err);
                    }
                }
            } else {
                (shuffle_disks.clone(), vec![None; shuffle_disks.len()])
            };
            let transaction_epoch =
                transaction_epoch_fence.map(|_| assign_object_transaction_epoch(&commit_disks, &mut parts_metadatas));

            let commit_set = self.clone();
            let commit_bucket = bucket.to_owned();
            let commit_object = object.to_owned();
            let commit_tmp_dir = tmp_dir.clone();
            let commit_rollback_receipt = rollback_receipt.clone();
            let commit_object_lock_guard = object_lock_guard.take();
            let commit_decommission_object_lock_guard = decommission_object_lock_guard.take();
            let commit_publication_guard = publication_commit_guard.take();
            let commit_bucket_lifecycle_guard = bucket_lifecycle_guard.take();
            let commit_decommission_capacity_guard = decommission_capacity_guard.take();
            let commit_scanner_publication_scope = opts.scanner_publication_commit_scope.clone();
            // A scanner publication scope owns the movement permit until the
            // complete rename fan-out drains. Keep this path synchronous so
            // its terminal state is known before the coordinator releases
            // remote leases.
            let commit_owns_namespace_guard = commit_object_lock_guard.is_some()
                || commit_decommission_object_lock_guard.is_some()
                || commit_publication_guard.is_some();
            let commit_allows_early_ack = opts.write_completion == WriteCompletion::Quorum
                && !(opts.data_movement && opts.has_decommission_capacity_reservation())
                && commit_owns_namespace_guard
                && commit_scanner_publication_scope.is_none();
            // Full-tail callers also transfer owned guards to the coordinator:
            // cancelling their ACK waiter must not cancel an in-flight rename.
            let detach_commit_owner = commit_scanner_publication_scope.is_some()
                || commit_owns_namespace_guard
                || commit_bucket_lifecycle_guard.is_some()
                || quota_mutation_fence;
            let commit_write_path_label = write_path.metric_label();
            let commit_is_versioned = opts.versioned || opts.version_suspended;
            let commit_versioned = opts.versioned;
            let commit_version_suspended = opts.version_suspended;
            let commit_version_id = opts.version_id.clone();
            let commit_namespace_lock_fence = opts.namespace_lock_fence.clone();
            let commit_bucket_lifecycle_lock_fence = opts.bucket_lifecycle_lock_fence.clone();
            let commit_capacity_scope_token = opts.capacity_scope_token;
            let commit_replication_state = replication_state_to_filemeta(&opts.put_replication_state());
            let commit_scanner_publication_lease_tokens = scanner_publication_lease_tokens;
            let request_cancellation = operation_cancellation.clone();
            tmp_cleanup_owned = true;

            let commit = move |cancellation: Option<CancellationToken>| async move {
                let mut _object_lock_guard = commit_object_lock_guard;
                let mut _decommission_object_lock_guard = commit_decommission_object_lock_guard;
                let mut _publication_guard = commit_publication_guard;
                let mut _bucket_lifecycle_guard = commit_bucket_lifecycle_guard;
                let mut _decommission_capacity_guard = commit_decommission_capacity_guard;
                let mut quota_reservation = quota_reservation;
                let rename_stage_start = Instant::now();
                let pre_rename = async {
                    #[cfg(any(test, feature = "test-util"))]
                    pause_put_object_commit(&commit_bucket, &commit_object, PutObjectCommitPause::AfterQuotaReservation).await;
                    quota_reservation.mark_commit_started().await?;
                    #[cfg(any(test, feature = "test-util"))]
                    pause_put_object_commit(&commit_bucket, &commit_object, PutObjectCommitPause::BeforeQuotaRename).await;
                    if quota_reservation.is_lock_lost()
                        || !quota_reservation.capability_proof_matches()
                        || _object_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                        || _decommission_object_lock_guard
                            .as_ref()
                            .is_some_and(|guard| guard.is_lock_lost())
                        || _publication_guard
                            .as_ref()
                            .is_some_and(RemoteTuplePublicationCommitGuard::is_lock_lost)
                        || commit_namespace_lock_fence
                            .as_ref()
                            .is_some_and(NamespaceLockFence::is_lock_lost)
                        || commit_bucket_lifecycle_lock_fence
                            .as_ref()
                            .is_some_and(NamespaceLockFence::is_lock_lost)
                        || _bucket_lifecycle_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                        || _decommission_capacity_guard
                            .as_ref()
                            .is_some_and(|guard| guard.is_lock_lost())
                    {
                        return Err(StorageError::NamespaceLockQuorumUnavailable {
                            mode: "quota_reservation",
                            bucket: commit_bucket.clone(),
                            object: commit_object.clone(),
                            required: 1,
                            achieved: 0,
                        });
                    }
                    let restore_opts = ObjectOptions {
                        version_id: commit_version_id.clone(),
                        versioned: commit_versioned,
                        version_suspended: commit_version_suspended,
                        no_lock: true,
                        ..Default::default()
                    };
                    commit_set
                        .require_current_restore_operation_id(
                            &commit_bucket,
                            &commit_object,
                            &restore_opts,
                            expected_restore_operation_id,
                            "put_object_quota_reservation",
                        )
                        .await?;
                    if let Some(proof) = transaction_fencing_proof.as_ref()
                        && !object_transaction_fencing_fleet_proof_matches(proof)
                    {
                        return Err(Error::other("object transaction fencing fleet capability changed during put_object"));
                    }
                    if let Some(expected) = transaction_epoch_fence {
                        #[cfg(any(test, feature = "test-util"))]
                        pause_put_object_commit(
                            &commit_bucket,
                            &commit_object,
                            PutObjectCommitPause::BeforeTransactionEpochVerify,
                        )
                        .await;
                        verify_object_transaction_epoch_fence(&commit_set, &commit_bucket, &commit_object, expected).await?;
                    }
                    if quota_reservation.is_lock_lost()
                        || !quota_reservation.capability_proof_matches()
                        || _object_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                        || _decommission_object_lock_guard
                            .as_ref()
                            .is_some_and(|guard| guard.is_lock_lost())
                        || _publication_guard
                            .as_ref()
                            .is_some_and(RemoteTuplePublicationCommitGuard::is_lock_lost)
                        || commit_namespace_lock_fence
                            .as_ref()
                            .is_some_and(NamespaceLockFence::is_lock_lost)
                        || commit_bucket_lifecycle_lock_fence
                            .as_ref()
                            .is_some_and(NamespaceLockFence::is_lock_lost)
                        || _bucket_lifecycle_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                        || _decommission_capacity_guard
                            .as_ref()
                            .is_some_and(|guard| guard.is_lock_lost())
                    {
                        return Err(StorageError::NamespaceLockQuorumUnavailable {
                            mode: "quota_reservation",
                            bucket: commit_bucket.clone(),
                            object: commit_object.clone(),
                            required: 1,
                            achieved: 0,
                        });
                    }
                    Ok(())
                };
                let mut pre_rename_result = if cancellation.is_some() || request_cancellation.is_some() {
                    tokio::select! {
                        biased;
                        _ = wait_for_put_object_commit_cancellation(cancellation.as_ref(), request_cancellation.as_ref()) => {
                            Err(StorageError::OperationCanceled)
                        },
                        result = pre_rename => result,
                    }
                } else {
                    pre_rename.await
                };
                if pre_rename_result.is_ok()
                    && let Some(scope) = commit_scanner_publication_scope.as_ref()
                    && let Err(err) = scope.try_begin()
                {
                    let _ = scope.mark_aborted_before_commit();
                    pre_rename_result = Err(Error::other(format!("scanner publication commit scope cannot start: {err:?}")));
                }
                if pre_rename_result.is_ok()
                    && let Some(scope) = commit_scanner_publication_scope.as_ref()
                    && !scope.can_commit()
                {
                    let _ = scope.mark_indeterminate();
                    pre_rename_result = Err(StorageError::OperationCanceled);
                }
                if let Err(err) = pre_rename_result {
                    SetDisks::abort_quota_reservation_after_fence(
                        quota_reservation,
                        &commit_disks,
                        &quota_fence_tokens,
                        &commit_bucket,
                        &commit_object,
                        write_quorum,
                        quota_mutation_fence,
                    )
                    .await;
                    if let Err(cleanup_err) = commit_set.delete_all(RUSTFS_META_TMP_BUCKET, &commit_tmp_dir).await {
                        warn!(tmp_dir = %commit_tmp_dir, error = ?cleanup_err, "failed to cleanup put_object temporary data");
                    } else if issue3031_diag_enabled() {
                        warn!(
                            target: "rustfs_ecstore::set_disk",
                            bucket = %commit_bucket,
                            object = %commit_object,
                            tmp_dir = %commit_tmp_dir,
                            "issue3031_put_object_tmp_cleanup_done"
                        );
                    }
                    return Err(err);
                }

                Self::assign_rename_data_indexes(&mut parts_metadatas);
                let mut rename_result = SetDisks::rename_data_owned_with_fence(
                    &commit_disks,
                    (RUSTFS_META_TMP_BUCKET, commit_tmp_dir.as_str()),
                    parts_metadatas,
                    (&commit_bucket, &commit_object),
                    commit_allows_early_ack,
                    crate::set_disk::core::io_primitives::RenameDataFenceOptions::new(
                        write_quorum,
                        commit_scanner_publication_lease_tokens.as_ref(),
                    )
                    .with_publication_scope(commit_scanner_publication_scope.clone())
                    .with_rollback_receipt(commit_rollback_receipt.clone())
                    .with_namespace_commit_guard(
                        (!is_meta_bucketname(&commit_bucket)).then(|| commit_set.ctx.begin_namespace_commit()),
                    ),
                )
                .await;
                if let Some(scope) = commit_scanner_publication_scope.as_ref() {
                    if rename_result.is_ok() {
                        let _ = scope.mark_committed();
                    } else {
                        let _ = scope.mark_indeterminate();
                    }
                }
                #[cfg(any(test, feature = "test-util"))]
                if rename_result.is_ok() {
                    pause_put_object_commit(&commit_bucket, &commit_object, PutObjectCommitPause::AfterRenameQuorum).await;
                }
                let mut rename_guard_release = None;
                let mut needs_immediate_heal = false;
                let mut tail_owns_tmp_cleanup = false;
                if let Ok(rename_commit) = rename_result.as_mut() {
                    commit_set.record_capacity_scope_if_needed(commit_capacity_scope_token, &rename_commit.capacity_disks);
                    // Install the tail watcher before any post-commit await. The
                    // latch keeps namespace guards through their prior handoff point.
                    needs_immediate_heal = rename_commit.needs_immediate_heal();
                    if let Some(rename_tail_drain) = rename_commit.tail_drain.take() {
                        tail_owns_tmp_cleanup = true;
                        let mut request = rustfs_heal_contracts::heal_channel::create_heal_request_with_options(
                            commit_bucket.clone(),
                            Some(commit_object.clone()),
                            false,
                            Some(HealChannelPriority::Normal),
                            Some(commit_set.pool_index),
                            Some(commit_set.set_index),
                        );
                        request.object_version_id = committed_version_id
                            .or_else(|| commit_version_suspended.then(Uuid::nil))
                            .map(|version_id| version_id.to_string());
                        let object_lock_guard = _object_lock_guard.take();
                        let publication_guard = _publication_guard.take();
                        let bucket_lifecycle_guard = _bucket_lifecycle_guard.take();
                        let decommission_object_lock_guard = _decommission_object_lock_guard.take();
                        let decommission_capacity_guard = _decommission_capacity_guard.take();
                        let cleanup_bucket = commit_bucket.clone();
                        let cleanup_object = commit_object.clone();
                        let heal_set = commit_set.clone();
                        let cleanup_set = commit_set.clone();
                        let cleanup_tmp_dir = commit_tmp_dir.clone();
                        let fence_disks = commit_disks.clone();
                        let fence_tokens = quota_fence_tokens.clone();
                        let fence_bucket = commit_bucket.clone();
                        let fence_object = commit_object.clone();
                        let (guard_release_tx, guard_release_rx) = tokio::sync::oneshot::channel();
                        rename_guard_release = Some(guard_release_tx);
                        tokio::spawn(finish_rename_tail_heal(
                            rename_tail_drain,
                            guard_release_rx,
                            (
                                object_lock_guard,
                                publication_guard,
                                bucket_lifecycle_guard,
                                decommission_object_lock_guard,
                                decommission_capacity_guard,
                            ),
                            request,
                            move || async move {
                                if quota_mutation_fence {
                                    let _ = SetDisks::release_quota_mutation_fences(
                                        &fence_disks,
                                        &fence_tokens,
                                        &fence_bucket,
                                        &fence_object,
                                        write_quorum,
                                    )
                                    .await;
                                }
                            },
                            move |(
                                object_lock_guard,
                                publication_guard,
                                bucket_lifecycle_guard,
                                decommission_object_lock_guard,
                                decommission_capacity_guard,
                            ),
                                  targets| async move {
                                drop(object_lock_guard);
                                drop(publication_guard);
                                drop(bucket_lifecycle_guard);
                                cleanup_set
                                    .cleanup_rename_tail(
                                        targets,
                                        &cleanup_bucket,
                                        &cleanup_object,
                                        committed_data_dir,
                                        transaction_epoch,
                                    )
                                    .await;
                                if let Err(err) = cleanup_set.delete_all(RUSTFS_META_TMP_BUCKET, &cleanup_tmp_dir).await {
                                    warn!(tmp_dir = %cleanup_tmp_dir, error = ?err, "failed to cleanup put_object temporary data");
                                } else if issue3031_diag_enabled() {
                                    warn!(
                                        target: "rustfs_ecstore::set_disk",
                                        tmp_dir = %cleanup_tmp_dir,
                                        "issue3031_put_object_tmp_cleanup_done"
                                    );
                                }
                                drop(decommission_object_lock_guard);
                                drop(decommission_capacity_guard);
                            },
                            |request| async move { heal_set.submit_rename_tail_heal(request).await },
                        ));
                    }
                }
                if !tail_owns_tmp_cleanup {
                    drop(_decommission_capacity_guard.take());
                }
                #[cfg(any(test, feature = "test-util"))]
                if rename_result.is_ok() {
                    pause_put_object_commit(&commit_bucket, &commit_object, PutObjectCommitPause::AfterRenameHandoff).await;
                }
                if quota_mutation_fence && !tail_owns_tmp_cleanup {
                    let _ = SetDisks::release_quota_mutation_fences(
                        &commit_disks,
                        &quota_fence_tokens,
                        &commit_bucket,
                        &commit_object,
                        write_quorum,
                    )
                    .await;
                }
                if rename_result.is_ok() {
                    quota_reservation.commit().await;
                }
                let rename_commit = match rename_result {
                    Ok(commit) => commit,
                    Err(err) => {
                        if commit_rollback_receipt.is_incomplete() {
                            // Incomplete undo retains the staging source and
                            // rollback backup for recovery; cleanup is unsafe.
                            return Err(err.into());
                        }
                        if let Err(cleanup_err) = commit_set.delete_all(RUSTFS_META_TMP_BUCKET, &commit_tmp_dir).await {
                            warn!(tmp_dir = %commit_tmp_dir, error = ?cleanup_err, "failed to cleanup put_object temporary data");
                        } else if issue3031_diag_enabled() {
                            warn!(
                                target: "rustfs_ecstore::set_disk",
                                bucket = %commit_bucket,
                                object = %commit_object,
                                tmp_dir = %commit_tmp_dir,
                                "issue3031_put_object_tmp_cleanup_done"
                            );
                        }
                        return Err(err.into());
                    }
                };
                let online_disks = rename_commit.online_disks;
                let op_old_dir = rename_commit.data_dir;
                let cleanup_disks = rename_commit.cleanup_disks;
                let old_current_size = rename_commit.old_current_size;
                let mut fi = rename_commit.committed_file_info;

                if needs_immediate_heal {
                    let mut request = rustfs_heal_contracts::heal_channel::create_heal_request_with_options(
                        commit_bucket.clone(),
                        Some(commit_object.clone()),
                        false,
                        Some(HealChannelPriority::Normal),
                        Some(commit_set.pool_index),
                        Some(commit_set.set_index),
                    );
                    request.object_version_id = committed_version_id
                        .or_else(|| commit_version_suspended.then(Uuid::nil))
                        .map(|version_id| version_id.to_string());
                    let heal_set = commit_set.clone();
                    tokio::spawn(async move { heal_set.submit_rename_tail_heal(request).await });
                }

                let rename_stage_elapsed = rename_stage_start.elapsed();
                let rename_stage_ms = rename_stage_elapsed.as_millis() as u64;

                if let Some(old_dir) = op_old_dir {
                    commit_set
                        .persist_old_data_cleanup_receipts(
                            &cleanup_disks,
                            &commit_bucket,
                            &commit_object,
                            old_dir,
                            committed_data_dir,
                            transaction_epoch,
                        )
                        .await;
                }

                commit_set
                    .invalidate_get_object_metadata_cache(&commit_bucket, &commit_object)
                    .await;

                if let Some(release) = rename_guard_release.take() {
                    let _ = release.send(true);
                }
                // The exact old-data-dir reclamation below is best-effort space
                // cleanup; it must not serialize the next operation on this object.
                drop(_object_lock_guard.take());
                drop(_publication_guard.take());
                drop(_bucket_lifecycle_guard.take());

                rustfs_io_metrics::record_put_object_stage_duration("set_disk_rename", duration_millis_f64(rename_stage_elapsed));
                if (rename_stage_ms as u128) >= SET_DISK_COMMIT_TAIL_WARN_THRESHOLD_MS {
                    warn!(
                        event = EVENT_SET_DISK_COMMIT_TAIL_SLOW,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        stage = "rename_data",
                        bucket = %commit_bucket,
                        object = %commit_object,
                        tmp_dir = %commit_tmp_dir,
                        duration_ms = { rename_stage_ms },
                        write_quorum,
                        state = "slow",
                        "SetDisk commit tail stage is slow"
                    );
                }

                let mut cleanup_stage_ms: Option<u64> = None;
                if let Some(old_dir) = op_old_dir {
                    let committed_dir = committed_data_dir.unwrap_or_default().to_string();
                    let cleanup_stage_start = Instant::now();
                    // backlog#898: reclaiming the dereferenced old data dir is
                    // best-effort and returns a receipt (never `Err`). A failed GC
                    // here must not negate an already-committed, durable write, so we
                    // deliberately do NOT `?`-propagate it into a 503. On residue the
                    // report path emits the leak metric and enqueues a heal.
                    let cleanup = commit_set
                        .commit_rename_data_dir_and_mark_capacity(
                            &cleanup_disks,
                            &commit_bucket,
                            &commit_object,
                            &old_dir.to_string(),
                            &committed_dir,
                            write_quorum,
                        )
                        .await;
                    let cleanup_elapsed = cleanup_stage_start.elapsed();
                    let cleanup_ms = cleanup_elapsed.as_millis() as u64;
                    cleanup_stage_ms = Some(cleanup_ms);
                    rustfs_io_metrics::record_put_object_stage_duration(
                        "set_disk_old_data_cleanup",
                        duration_millis_f64(cleanup_elapsed),
                    );
                    commit_set
                        .report_old_data_dir_cleanup(&commit_bucket, &commit_object, &old_dir.to_string(), &cleanup)
                        .await;
                    if (cleanup_ms as u128) >= SET_DISK_COMMIT_TAIL_WARN_THRESHOLD_MS {
                        warn!(
                            event = EVENT_SET_DISK_COMMIT_TAIL_SLOW,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_SET_DISK,
                            stage = "commit_rename_data_dir",
                            bucket = %commit_bucket,
                            object = %commit_object,
                            tmp_dir = %commit_tmp_dir,
                            old_dir = %old_dir,
                            duration_ms = cleanup_ms,
                            write_quorum,
                            state = "slow",
                            "SetDisk commit tail stage is slow"
                        );
                    }
                }
                if is_compressed {
                    record_compression_total_memory(actual_size as u64, w_size as u64).await;
                }
                fi.replication_state_internal = Some(commit_replication_state);

                fi.is_latest = true;

                if issue3031_diag_enabled() {
                    let online_success_count = online_disks.iter().filter(|disk| disk.is_some()).count();
                    warn!(
                        target: "rustfs_ecstore::set_disk",
                        bucket = %commit_bucket,
                        object = %commit_object,
                        tmp_dir = %commit_tmp_dir,
                        data_dir = ?fi.data_dir,
                        write_quorum,
                        online_success_count,
                        op_old_dir = ?op_old_dir,
                        "issue3031_put_object_commit_succeeded"
                    );
                }

                let total_commit_tail_ms = rename_stage_start.elapsed().as_millis();
                if total_commit_tail_ms >= SET_DISK_COMMIT_TAIL_WARN_THRESHOLD_MS {
                    warn!(
                        event = EVENT_SET_DISK_COMMIT_TAIL_SLOW,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        stage = "put_object_commit_tail",
                        bucket = %commit_bucket,
                        object = %commit_object,
                        tmp_dir = %commit_tmp_dir,
                        duration_ms = total_commit_tail_ms as u64,
                        write_quorum,
                        state = "slow",
                        "SetDisk commit tail is slow"
                    );
                }

                if issue3031_diag_enabled() {
                    warn!(
                        event = EVENT_SET_DISK_PUT_OBJECT_STAGE_SUMMARY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        bucket = %commit_bucket,
                        object = %commit_object,
                        write_quorum,
                        write_path = commit_write_path_label,
                        writer_setup_ms,
                        encode_ms,
                        rename_ms = rename_stage_ms,
                        cleanup_ms = cleanup_stage_ms.unwrap_or_default(),
                        cleanup_present = cleanup_stage_ms.is_some(),
                        commit_tail_ms = total_commit_tail_ms as u64,
                        result = "success",
                        "SetDisk put_object stage summary"
                    );
                }

                if !tail_owns_tmp_cleanup {
                    let cleanup_set = commit_set.clone();
                    let cleanup_tmp_dir = commit_tmp_dir.clone();
                    tokio::spawn(async move {
                        if let Err(err) = cleanup_set.delete_all(RUSTFS_META_TMP_BUCKET, &cleanup_tmp_dir).await {
                            warn!(tmp_dir = %cleanup_tmp_dir, error = ?err, "failed to cleanup put_object temporary data");
                        } else if issue3031_diag_enabled() {
                            warn!(
                                target: "rustfs_ecstore::set_disk",
                                tmp_dir = %cleanup_tmp_dir,
                                "issue3031_put_object_tmp_cleanup_done"
                            );
                        }
                    });
                }

                Ok((
                    ObjectInfo::from_file_info(&fi, &commit_bucket, &commit_object, commit_is_versioned),
                    old_current_size,
                ))
            };

            if let Some(handoff) = commit_cancellation_handoff_tx.take() {
                let _ = handoff.send(());
            }
            if detach_commit_owner {
                if let Some(scope_outcome_guard) = scope_outcome_guard.as_mut() {
                    // The spawned commit closure owns the scope clone and is
                    // now responsible for its terminal outcome.
                    scope_outcome_guard.disarm();
                }
                let mut cancellation = PutObjectCommitCancellation::new();
                let child_token = cancellation.child_token();
                let result = tokio::spawn(async move { Box::pin(commit(Some(child_token))).await })
                    .await
                    .map_err(|err| Error::other(format!("put_object commit task failed: {err}")))?;
                cancellation.disarm();
                result
            } else {
                Box::pin(commit(None)).await
            }
        };
        let result: Result<(ObjectInfo, Option<OldCurrentSize>)> = if let Some((cancellation, mut handoff_rx)) = cancellation_wait
        {
            tokio::pin!(operation);
            tokio::select! {
                biased;
                _ = &mut handoff_rx => operation.await,
                result = &mut operation => result,
                _ = cancellation.cancelled() => Err(StorageError::OperationCanceled),
            }
        } else {
            operation.await
        };

        if issue3031_diag_enabled()
            && let Err(err) = &result
        {
            let stage_hint = if err.to_string().contains("not enough disks to write") {
                "writer_setup_or_quorum"
            } else {
                "unknown"
            };
            warn!(
                event = EVENT_SET_DISK_PUT_OBJECT_STAGE_SUMMARY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                bucket = %bucket,
                object = %object,
                result = "error",
                stage_hint,
                error = %err,
                "SetDisk put_object stage summary"
            );
        }

        if issue3031_diag_enabled() {
            warn!(
                target: "rustfs_ecstore::set_disk",
                bucket = %bucket,
                object = %object,
                tmp_dir = %tmp_dir,
                result = ?result.as_ref().map(|_| ()).map_err(|err| err.to_string()),
                "issue3031_put_object_tmp_cleanup_start"
            );
        }

        if tmp_cleanup_owned && result.is_ok() {
        } else if result.is_ok() {
            // Success path: `rename_data` has already moved the data dir out of
            // the tmp workspace and removed the (empty) tmp dir where it could,
            // so this delete_all is a speculative safety net that normally hits
            // a missing path. It still must run — `rename_data`'s `remove_std`
            // only removes empty directories and silently ignores failures —
            // but it has no reason to run on the response path: under
            // fsync-heavy load the same-disk queueing behind it was measured to
            // add ~9ms average (p99 77ms) to PUT latency (backlog#924 / HP-3).
            // If the process dies before the spawned task runs, the stale tmp
            // entry is reclaimed by cleanup_stale_tmp_objects (24h expiry,
            // 5-minute background loop).
            let set_disks = self.clone();
            tokio::spawn(async move {
                if let Err(err) = set_disks.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_dir).await {
                    warn!(tmp_dir = %tmp_dir, error = ?err, "failed to cleanup put_object temporary data");
                } else if issue3031_diag_enabled() {
                    warn!(
                        target: "rustfs_ecstore::set_disk",
                        tmp_dir = %tmp_dir,
                        "issue3031_put_object_tmp_cleanup_done"
                    );
                }
            });
        } else if !rollback_receipt.is_incomplete() {
            // Failure path (quorum loss / rollback): keep the cleanup inline so
            // a failed PUT never returns while its tmp shards are still on disk
            // (state-residue hardening tracked by backlog#864 / backlog#898).
            if let Err(err) = self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_dir).await {
                warn!(tmp_dir = %tmp_dir, error = ?err, "failed to cleanup put_object temporary data");
            } else if issue3031_diag_enabled() {
                warn!(
                    target: "rustfs_ecstore::set_disk",
                    bucket = %bucket,
                    object = %object,
                    tmp_dir = %tmp_dir,
                    "issue3031_put_object_tmp_cleanup_done"
                );
            }
        }

        result
    }
}

struct TransitionUploadReader<R> {
    inner: R,
    consumed: Arc<AtomicU64>,
}

impl<R> TransitionUploadReader<R> {
    fn new(inner: R, consumed: Arc<AtomicU64>) -> Self {
        Self { inner, consumed }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TransitionUploadReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len() - before;
                let read =
                    u64::try_from(read).map_err(|_| std::io::Error::other("transition upload read count exceeds u64::MAX"))?;
                self.consumed
                    .try_update(Ordering::Release, Ordering::Relaxed, |consumed| consumed.checked_add(read))
                    .map_err(|_| std::io::Error::other("transition upload read count overflow"))?;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

struct LegacyDuplexProducerReader<R> {
    inner: Option<R>,
    terminal: Option<tokio::sync::oneshot::Receiver<Result<()>>>,
    inner_eof: bool,
}

impl<R> LegacyDuplexProducerReader<R> {
    fn new(inner: R, terminal: tokio::sync::oneshot::Receiver<Result<()>>) -> Self {
        Self {
            inner: Some(inner),
            terminal: Some(terminal),
            inner_eof: false,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for LegacyDuplexProducerReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if !self.inner_eof {
            let before = buf.filled().len();
            if let Some(inner) = self.inner.as_mut() {
                match Pin::new(inner).poll_read(cx, buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Ready(Ok(())) if buf.filled().len() > before => return Poll::Ready(Ok(())),
                    Poll::Ready(Ok(())) => {
                        self.inner_eof = true;
                        self.inner = None;
                    }
                }
            } else {
                self.inner_eof = true;
            }
        }

        let Some(terminal) = self.terminal.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        match Pin::new(terminal).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Ok(()))) => {
                self.terminal = None;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(Err(err))) => {
                self.terminal = None;
                Poll::Ready(Err(std::io::Error::other(err)))
            }
            Poll::Ready(Err(_)) => {
                self.terminal = None;
                Poll::Ready(Err(std::io::Error::other(StorageError::Unexpected)))
            }
        }
    }
}

#[cfg(test)]
mod legacy_duplex_producer_reader_tests {
    use super::*;
    use crate::object_api::{EncryptionResolutionError, ObjectEncryptionResolver, ReadEncryptionMaterial, ReadEncryptionMode};
    use rustfs_utils::CompressionAlgorithm;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const TEST_DUPLEX_CAPACITY: usize = 64 * 1024;

    fn storage_error_source(error: &std::io::Error) -> &StorageError {
        error
            .get_ref()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("legacy duplex terminal error should retain StorageError source")
    }

    async fn compressed_fixture(plaintext: Vec<u8>, recorded_size: usize) -> (Vec<u8>, ObjectInfo) {
        let mut compressor = rustfs_rio::CompressReader::new(std::io::Cursor::new(plaintext), CompressionAlgorithm::default());
        let mut compressed = Vec::new();
        compressor
            .read_to_end(&mut compressed)
            .await
            .expect("compress test plaintext");

        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            CompressionAlgorithm::default().to_string(),
        );
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, recorded_size.to_string());
        let object_info = ObjectInfo {
            size: i64::try_from(compressed.len()).expect("compressed fixture length should fit in i64"),
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        (compressed, object_info)
    }

    #[tokio::test]
    async fn legacy_duplex_reader_allows_clean_completion() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
        writer
            .write_all(b"complete")
            .await
            .expect("duplex write should fit in buffer");
        drop(writer);
        terminal_tx.send(Ok(())).expect("terminal receiver should remain installed");

        let mut reader = LegacyDuplexProducerReader::new(reader, terminal_rx);
        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect("clean producer completion should surface clean EOF");

        assert_eq!(out, b"complete");
    }

    #[tokio::test]
    async fn producer_cancellation_reader_cancels_pending_producer_on_drop() {
        let cancellation = CancellationToken::new();
        let producer_cancellation = cancellation.clone();
        let producer = tokio::spawn(async move {
            tokio::select! {
                _ = producer_cancellation.cancelled() => true,
                _ = std::future::pending::<()>() => false,
            }
        });

        let reader = ProducerCancellationReader::new(tokio::io::empty(), cancellation);
        drop(reader);

        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), producer)
                .await
                .expect("dropping the consumer should cancel the producer promptly")
                .expect("producer task should not panic")
        );
    }

    #[tokio::test]
    async fn legacy_duplex_reader_ignores_zero_capacity_read_buf() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
        writer.write_all(b"body").await.expect("duplex write should fit in buffer");
        drop(writer);
        terminal_tx
            .send(Err(StorageError::FileCorrupt))
            .expect("terminal receiver should remain installed");

        let mut reader = LegacyDuplexProducerReader::new(reader, terminal_rx);
        let mut empty = [];
        std::future::poll_fn(|cx| {
            let mut read_buf = ReadBuf::new(&mut empty);
            Pin::new(&mut reader).poll_read(cx, &mut read_buf)
        })
        .await
        .expect("zero-capacity reads should complete without observing EOF or terminal state");
        assert!(!reader.inner_eof);
        assert!(reader.terminal.is_some());

        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("subsequent reads must still receive data and the terminal error");
        assert_eq!(out, b"body");
        assert!(matches!(storage_error_source(&err), StorageError::FileCorrupt));
    }

    #[tokio::test]
    async fn legacy_duplex_reader_surfaces_terminal_error_after_partial_data() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
        writer.write_all(b"partial").await.expect("duplex write should fit in buffer");
        drop(writer);
        terminal_tx
            .send(Err(StorageError::FileCorrupt))
            .expect("terminal receiver should remain installed");

        let mut reader = LegacyDuplexProducerReader::new(reader, terminal_rx);
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("terminal producer error must not become clean EOF");

        assert_eq!(out, b"partial");
        assert!(matches!(storage_error_source(&err), StorageError::FileCorrupt));
    }

    #[tokio::test]
    async fn legacy_duplex_reader_surfaces_terminal_error_after_declared_length() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
        writer.write_all(b"exact").await.expect("duplex write should fit in buffer");
        drop(writer);
        terminal_tx
            .send(Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "remote body reset after final byte",
            ))))
            .expect("terminal receiver should remain installed");

        let reader = LegacyDuplexProducerReader::new(reader, terminal_rx);
        let mut reader =
            HashReader::from_stream(reader, 5, 5, None, None, false).expect("hash reader should accept exact declared length");
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("producer terminal error after the declared length must still fail");

        assert_eq!(out, b"exact");
        assert!(
            matches!(storage_error_source(&err), StorageError::Io(io_error) if io_error.kind() == std::io::ErrorKind::ConnectionReset)
        );
    }

    #[tokio::test]
    async fn legacy_compressed_reader_surfaces_terminal_error_after_complete_plaintext() {
        let plaintext = b"compressed terminal result must survive the plaintext limit".repeat(16);
        let (compressed, object_info) = compressed_fixture(plaintext.clone(), plaintext.len()).await;
        let full_range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 0,
            end: i64::try_from(plaintext.len()).expect("plaintext fixture length should fit in i64") - 1,
        };
        for range in [None, Some(full_range)] {
            let (mut writer, reader) = tokio::io::duplex(compressed.len().max(1));
            writer
                .write_all(&compressed)
                .await
                .expect("compressed body should fit in duplex buffer");
            drop(writer);
            let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
            terminal_tx
                .send(Err(StorageError::FileCorrupt))
                .expect("terminal receiver should remain installed");

            let (mut reader, _, _) = get_legacy_object_reader_with_context(
                &InstanceContext::new(),
                reader,
                terminal_rx,
                range,
                &object_info,
                &ObjectOptions::default(),
                &HeaderMap::new(),
            )
            .await
            .expect("compressed read plan should build");
            let mut out = Vec::new();
            let err = reader
                .read_to_end(&mut out)
                .await
                .expect_err("terminal error after complete decompression must not become clean EOF");

            assert_eq!(out, plaintext);
            assert!(matches!(storage_error_source(&err), StorageError::FileCorrupt));
        }
    }

    #[tokio::test]
    async fn legacy_exact_reader_rejects_extra_data_without_backpressure_deadlock() {
        let payload = vec![0x5a; TEST_DUPLEX_CAPACITY * 2];
        let (mut writer, reader) = tokio::io::duplex(TEST_DUPLEX_CAPACITY);
        let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
        let producer = tokio::spawn(async move {
            let result = writer.write_all(&payload).await;
            drop(writer);
            let terminal_result = result
                .as_ref()
                .map(|_| ())
                .map_err(|err| StorageError::Io(std::io::Error::new(err.kind(), err.to_string())));
            let _ = terminal_tx.send(terminal_result);
            result
        });
        let reader = crate::io_support::rio::HardLimitReader::new(reader, 1);
        let mut reader = LegacyDuplexProducerReader::new(reader, terminal_rx);

        let mut out = Vec::new();
        tokio::time::timeout(std::time::Duration::from_secs(1), reader.read_to_end(&mut out))
            .await
            .expect("extra data beyond the declared size must not deadlock")
            .expect_err("extra data beyond the declared size must fail closed");
        assert_eq!(out, [0x5a]);
        drop(reader);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), producer)
            .await
            .expect("producer must unblock after the read fails")
            .expect("producer task should not panic");
    }

    #[tokio::test]
    async fn legacy_terminal_reader_releases_unconsumed_source_before_waiting() {
        let payload = vec![0x5a; TEST_DUPLEX_CAPACITY * 2];
        let (mut writer, reader) = tokio::io::duplex(TEST_DUPLEX_CAPACITY);
        let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
        let producer = tokio::spawn(async move {
            let result = writer.write_all(&payload).await;
            drop(writer);
            let terminal_result = result
                .as_ref()
                .map(|_| ())
                .map_err(|err| StorageError::Io(std::io::Error::new(err.kind(), err.to_string())));
            let _ = terminal_tx.send(terminal_result);
            result
        });
        let reader = rustfs_rio::LimitReader::new(reader, 1);
        let mut reader = LegacyDuplexProducerReader::new(reader, terminal_rx);

        let mut out = Vec::new();
        let err = tokio::time::timeout(std::time::Duration::from_secs(1), reader.read_to_end(&mut out))
            .await
            .expect("terminal wait must not deadlock behind unconsumed source data")
            .expect_err("unconsumed source data must fail the producer terminal result");
        assert_eq!(out, [0x5a]);
        assert!(
            matches!(storage_error_source(&err), StorageError::Io(io_error) if io_error.kind() == std::io::ErrorKind::BrokenPipe)
        );
        producer
            .await
            .expect("producer task should not panic")
            .expect_err("source should close early");
    }

    struct FixedEncryptionResolver {
        key_bytes: [u8; 32],
        base_nonce: [u8; 12],
    }

    #[async_trait::async_trait]
    impl ObjectEncryptionResolver for FixedEncryptionResolver {
        async fn resolve_read_material(
            &self,
            _request: crate::object_api::ReadEncryptionRequest<'_>,
        ) -> std::result::Result<Option<ReadEncryptionMaterial>, EncryptionResolutionError> {
            Ok(Some(ReadEncryptionMaterial {
                key_bytes: self.key_bytes,
                mode: ReadEncryptionMode::Direct {
                    base_nonce: self.base_nonce,
                },
            }))
        }
    }

    #[tokio::test]
    async fn legacy_encrypted_reader_surfaces_terminal_error_after_complete_plaintext() {
        let plaintext = b"encrypted terminal result must survive the plaintext limit".repeat(16);
        let key_bytes = [0x31; 32];
        let base_nonce = [0x42; 12];
        let mut encryptor = rustfs_rio::EncryptReader::new(std::io::Cursor::new(plaintext.clone()), key_bytes, base_nonce);
        let mut encrypted = Vec::new();
        encryptor.read_to_end(&mut encrypted).await.expect("encrypt test plaintext");

        let object_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "encrypted-object".to_string(),
            size: i64::try_from(encrypted.len()).expect("encrypted fixture length should fit in i64"),
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
            ])),
            ..Default::default()
        };
        let ctx = InstanceContext::new();
        assert!(
            ctx.set_object_encryption_resolver(Arc::new(FixedEncryptionResolver { key_bytes, base_nonce }))
                .is_ok(),
            "fresh context should accept resolver"
        );
        let full_range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 0,
            end: i64::try_from(plaintext.len()).expect("plaintext fixture length should fit in i64") - 1,
        };
        for range in [None, Some(full_range)] {
            let (mut writer, reader) = tokio::io::duplex(encrypted.len().max(1));
            writer
                .write_all(&encrypted)
                .await
                .expect("encrypted body should fit in duplex buffer");
            drop(writer);
            let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
            terminal_tx
                .send(Err(StorageError::FileCorrupt))
                .expect("terminal receiver should remain installed");

            let (mut reader, _, _) = get_legacy_object_reader_with_context(
                &ctx,
                reader,
                terminal_rx,
                range,
                &object_info,
                &ObjectOptions::default(),
                &HeaderMap::new(),
            )
            .await
            .expect("encrypted read plan should build");
            let mut out = Vec::new();
            let err = reader
                .read_to_end(&mut out)
                .await
                .expect_err("terminal error after complete decryption must not become clean EOF");

            assert_eq!(out, plaintext);
            assert!(matches!(storage_error_source(&err), StorageError::FileCorrupt));
        }
    }

    #[tokio::test]
    async fn legacy_duplex_reader_fails_closed_when_terminal_channel_closes() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel::<Result<()>>();
        writer.write_all(b"body").await.expect("duplex write should fit in buffer");
        drop(writer);
        drop(terminal_tx);

        let mut reader = LegacyDuplexProducerReader::new(reader, terminal_rx);
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("producer disappearance must fail closed");

        assert_eq!(out, b"body");
        assert!(matches!(storage_error_source(&err), StorageError::Unexpected));
    }
}

struct TransitionUploadWriter<W> {
    inner: W,
    produced: u64,
}

impl<W> TransitionUploadWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, produced: 0 }
    }

    fn produced(&self) -> u64 {
        self.produced
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for TransitionUploadWriter<W> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                let written_u64 = u64::try_from(written)
                    .map_err(|_| std::io::Error::other("transition upload write count exceeds u64::MAX"))?;
                self.produced = self
                    .produced
                    .checked_add(written_u64)
                    .ok_or_else(|| std::io::Error::other("transition upload write count overflow"))?;
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub(crate) struct TransitionUploadFailure {
    pub(crate) error: StorageError,
    pub(crate) candidate: Option<TransitionUploadCandidate>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TransitionUploadCompletion {
    pub(crate) candidate: TransitionUploadCandidate,
    pub(crate) produced: u64,
    pub(crate) consumed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TransitionUploadCandidate {
    remote_version: TransitionUploadRemoteVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TransitionUploadRemoteVersion {
    KnownExact(String),
    KnownUnversioned(String),
}

impl TransitionUploadCandidate {
    pub(crate) fn from_put_response(remote_version: String) -> Self {
        let remote_version = if remote_version.is_empty() {
            TransitionUploadRemoteVersion::KnownUnversioned(remote_version)
        } else {
            TransitionUploadRemoteVersion::KnownExact(remote_version)
        };
        Self { remote_version }
    }

    pub(crate) fn remote_version(&self) -> &str {
        match &self.remote_version {
            TransitionUploadRemoteVersion::KnownExact(remote_version)
            | TransitionUploadRemoteVersion::KnownUnversioned(remote_version) => remote_version,
        }
    }

    pub(crate) fn cleanup_version(&self) -> &str {
        match &self.remote_version {
            TransitionUploadRemoteVersion::KnownExact(remote_version) => remote_version,
            TransitionUploadRemoteVersion::KnownUnversioned(_) => "",
        }
    }

    fn cleanup_version_is_exact(&self) -> bool {
        matches!(&self.remote_version, TransitionUploadRemoteVersion::KnownExact(_))
    }
}

pub(crate) async fn complete_transition_upload<Remote, Producer>(
    remote_upload: Remote,
    producer: Producer,
    expected_size: u64,
    consumed: Arc<AtomicU64>,
) -> std::result::Result<TransitionUploadCompletion, Box<TransitionUploadFailure>>
where
    Remote: Future<Output = std::result::Result<String, std::io::Error>>,
    Producer: Future<Output = Result<u64>>,
{
    let producer = std::panic::AssertUnwindSafe(producer).catch_unwind();
    let (remote_result, producer_result) = tokio::join!(remote_upload, producer);
    let remote_version = match remote_result {
        Ok(remote_version) => remote_version,
        Err(remote_error) => {
            let error = match producer_result {
                Ok(Err(StorageError::Io(producer_error))) if producer_error.kind() == std::io::ErrorKind::BrokenPipe => {
                    StorageError::Io(remote_error)
                }
                Ok(Err(producer_error)) => producer_error,
                Err(_) => StorageError::Unexpected,
                Ok(Ok(_)) => StorageError::Io(remote_error),
            };
            return Err(Box::new(TransitionUploadFailure { error, candidate: None }));
        }
    };
    let candidate = TransitionUploadCandidate::from_put_response(remote_version);
    let produced = match producer_result {
        Ok(Ok(produced)) => produced,
        Ok(Err(error)) => {
            return Err(Box::new(TransitionUploadFailure {
                error,
                candidate: Some(candidate),
            }));
        }
        Err(_) => {
            return Err(Box::new(TransitionUploadFailure {
                error: StorageError::Unexpected,
                candidate: Some(candidate),
            }));
        }
    };
    let consumed = consumed.load(Ordering::Acquire);
    if produced != expected_size || consumed != expected_size {
        let error = if produced < expected_size || consumed < expected_size {
            StorageError::LessData
        } else {
            StorageError::MoreData
        };
        return Err(Box::new(TransitionUploadFailure {
            error,
            candidate: Some(candidate),
        }));
    }
    Ok(TransitionUploadCompletion {
        candidate,
        produced,
        consumed,
    })
}

pub(crate) async fn cleanup_uncommitted_transition_upload(
    lease: &TierOperationLease,
    object: &str,
    cleanup_version: &str,
    version_id_exact: bool,
) -> std::io::Result<RemoteTierDeleteOutcome> {
    if version_id_exact {
        delete_confirmed_transition_candidate_exact_with_lease_idempotent(object, cleanup_version, lease).await
    } else {
        delete_object_from_remote_tier_with_lease_idempotent(object, cleanup_version, lease, false).await
    }
}

fn log_transition_upload_cleanup_failure(lease: &TierOperationLease, object: &str, cleanup_version: &str, err: &std::io::Error) {
    warn!(
        event = EVENT_LIFECYCLE_TRANSITION_CLEANUP,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_SET_DISK,
        tier = lease.tier_name(),
        tier_generation = lease.generation(),
        object,
        remote_version = cleanup_version,
        error = ?err,
        "Transition upload cleanup failed"
    );
}

pub(crate) struct TransitionUploadCleanup {
    lease: TierOperationLease,
    object: String,
    candidate: Option<TransitionUploadCandidate>,
    cleanup_api: Option<Arc<ECStore>>,
    cleanup_transaction: Option<TransitionTransaction>,
    armed: bool,
}

impl TransitionUploadCleanup {
    pub(crate) fn new(lease: TierOperationLease, object: &str) -> Self {
        Self {
            lease,
            object: object.to_string(),
            candidate: None,
            cleanup_api: None,
            cleanup_transaction: None,
            armed: true,
        }
    }

    fn set_cleanup_owner(&mut self, api: Option<Arc<ECStore>>, transaction: &TransitionTransaction) {
        self.cleanup_api = api;
        self.cleanup_transaction = Some(transaction.clone());
    }

    fn update_cleanup_transaction(&mut self, transaction: &TransitionTransaction) {
        self.cleanup_transaction = Some(transaction.clone());
    }

    fn cleanup_candidate(&self) -> std::io::Result<&TransitionUploadCandidate> {
        self.candidate
            .as_ref()
            .ok_or_else(|| std::io::Error::other("transition upload cleanup has no confirmed remote candidate"))
    }

    pub(crate) async fn cleanup(&mut self) -> std::io::Result<RemoteTierDeleteOutcome> {
        let candidate = self.cleanup_candidate()?;
        let result = cleanup_uncommitted_transition_upload(
            &self.lease,
            &self.object,
            candidate.cleanup_version(),
            candidate.cleanup_version_is_exact(),
        )
        .await;
        match result {
            Ok(outcome) => {
                self.armed = false;
                Ok(outcome)
            }
            Err(err) => {
                log_transition_upload_cleanup_failure(&self.lease, &self.object, candidate.cleanup_version(), &err);
                Err(err)
            }
        }
    }

    async fn cleanup_rejected_upload(
        &mut self,
        api: Option<Arc<ECStore>>,
        transaction: &mut TransitionTransaction,
    ) -> std::io::Result<()> {
        let api = api.or_else(|| self.cleanup_api.clone());
        self.cleanup_api = api.clone();
        let candidate = self.cleanup_candidate()?.clone();
        let owner_error = persist_rejected_transition_cleanup_owner(api.as_ref(), transaction, &candidate)
            .await
            .err();
        self.update_cleanup_transaction(transaction);
        let cleanup = cleanup_rejected_transition_upload_durably(
            &self.lease,
            &self.object,
            candidate.cleanup_version(),
            candidate.cleanup_version_is_exact(),
            api,
        )
        .await;
        match (owner_error, cleanup) {
            (_, Ok(())) => {
                self.armed = false;
                Ok(())
            }
            (None, Err(cleanup_error)) => Err(cleanup_error),
            (Some(owner_error), Err(cleanup_error)) => Err(crate::error::stable_io_error(
                "rejected transition upload cleanup error followed a transaction owner update failure",
                format!("owner error: {owner_error}; cleanup error: {cleanup_error}"),
            )),
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TransitionUploadCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        #[cfg(all(test, feature = "test-util"))]
        if transition_transaction_kill_point_is_active(self.cleanup_transaction.as_ref()) {
            return;
        }
        let Some(candidate) = self.candidate.as_ref() else {
            return;
        };
        let lease = match self.lease.try_clone() {
            Ok(lease) => lease,
            Err(err) => {
                warn!(
                    event = EVENT_LIFECYCLE_TRANSITION_CLEANUP,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    tier = self.lease.tier_name(),
                    tier_generation = self.lease.generation(),
                    object = self.object,
                    error = ?err,
                    "Cancelled transition cleanup could not retain its tier lease"
                );
                return;
            }
        };
        let object = self.object.clone();
        let candidate = candidate.clone();
        let cleanup_api = self.cleanup_api.clone();
        let mut cleanup_transaction = self.cleanup_transaction.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let owner_error = match (cleanup_api.as_ref(), cleanup_transaction.as_mut()) {
                    (Some(api), Some(transaction)) => {
                        persist_rejected_transition_cleanup_owner(Some(api), transaction, &candidate)
                            .await
                            .err()
                    }
                    _ => None,
                };
                let cleanup_version = candidate.cleanup_version().to_string();
                let cleanup = cleanup_rejected_transition_upload_durably(
                    &lease,
                    &object,
                    &cleanup_version,
                    candidate.cleanup_version_is_exact(),
                    cleanup_api,
                )
                .await;
                let result = match (owner_error, cleanup) {
                    (_, Ok(())) => Ok(()),
                    (None, Err(cleanup_error)) => Err(cleanup_error),
                    (Some(owner_error), Err(cleanup_error)) => Err(crate::error::stable_io_error(
                        "cancelled transition upload cleanup error followed a transaction owner update failure",
                        format!("owner error: {owner_error}; cleanup error: {cleanup_error}"),
                    )),
                };
                if let Err(err) = result {
                    warn!(
                        event = EVENT_LIFECYCLE_TRANSITION_CLEANUP,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        tier = lease.tier_name(),
                        tier_generation = lease.generation(),
                        object,
                        remote_version = cleanup_version,
                        error = ?err,
                        "Cancelled transition upload remains transaction-owned for recovery"
                    );
                }
            });
        }
    }
}

async fn persist_rejected_transition_cleanup_owner(
    api: Option<&Arc<ECStore>>,
    transaction: &mut TransitionTransaction,
    candidate: &TransitionUploadCandidate,
) -> Result<()> {
    let expected = transaction.clone();
    let remote_version = TransitionRemoteVersion::known_from_put_response(candidate.remote_version().to_string());
    let decision = match transaction.state {
        TransitionTransactionState::UploadOutcomeUnknown => TransitionCleanupDecision::RemoteVersionRecoveredAfterCancellation,
        TransitionTransactionState::Uploaded => TransitionCleanupDecision::UploadAbortedBeforeLocalCommit,
        TransitionTransactionState::CleanupPending if transaction.remote_version == remote_version => return Ok(()),
        state => {
            return Err(Error::other_with_context(
                "transition transaction state cannot own a rejected upload",
                format!("state {state:?}"),
            ));
        }
    };
    let mut cleanup = expected.clone();
    cleanup
        .mark_cleanup_pending(
            expected.fence(),
            crate::bucket::lifecycle::transition_transaction::TransitionCleanupProof {
                transaction_id: expected.transaction_id,
                write_id: expected.write_id,
                remote_object: expected.remote_object.clone(),
                remote_version,
                backend_fingerprint: expected.backend_fingerprint,
                decision,
            },
        )
        .map_err(Error::other)?;
    compare_and_save_transition_transaction_if_available(api, &expected, &cleanup).await?;
    *transaction = cleanup;
    Ok(())
}

pub(crate) async fn cleanup_rejected_transition_upload_durably(
    lease: &TierOperationLease,
    object: &str,
    cleanup_version: &str,
    version_id_exact: bool,
    _api: Option<Arc<ECStore>>,
) -> std::io::Result<()> {
    cleanup_uncommitted_transition_upload(lease, object, cleanup_version, version_id_exact)
        .await
        .map(|_| ())
        .inspect_err(|err| log_transition_upload_cleanup_failure(lease, object, cleanup_version, err))
}

async fn transition_cleanup_store(ctx: &Arc<crate::runtime::instance::InstanceContext>) -> Option<Arc<ECStore>> {
    #[cfg(feature = "test-util")]
    pause_transition_cleanup_store().await;

    transition_object_store(ctx).await
}

async fn transition_object_store(ctx: &Arc<crate::runtime::instance::InstanceContext>) -> Option<Arc<ECStore>> {
    if let Some(api) = runtime_sources::object_store_handle().filter(|api| Arc::ptr_eq(&api.ctx, ctx)) {
        return Some(api);
    }
    let metadata_sys = ctx.bucket_metadata_sys()?;
    let api = metadata_sys.read().await.object_store();
    Arc::ptr_eq(&api.ctx, ctx).then_some(api)
}

fn transition_deployment_id(ctx: &crate::runtime::instance::InstanceContext) -> Result<Uuid> {
    if let Some(deployment_id) = ctx.deployment_id() {
        return Ok(deployment_id);
    }
    #[cfg(test)]
    {
        Ok(Uuid::new_v4())
    }
    #[cfg(not(test))]
    {
        Err(Error::other("transition transaction requires initialized deployment id"))
    }
}

fn transition_transaction_not_after_unix_nanos() -> Result<i64> {
    let not_after = (OffsetDateTime::now_utc() + time::Duration::days(7)).unix_timestamp_nanos();
    i64::try_from(not_after).map_err(|_| Error::other("transition transaction deadline timestamp overflow"))
}

fn transition_source_version_mode(opts: &ObjectOptions, fi: &FileInfo) -> TransitionSourceVersionMode {
    let requested_null_version = opts
        .version_id
        .as_deref()
        .and_then(|version_id| Uuid::parse_str(version_id).ok())
        .is_some_and(|version_id| version_id.is_nil());
    let source_version_id = fi.version_id.filter(|version_id| !version_id.is_nil());
    if opts.versioned && (source_version_id.is_some() || (fi.versioned && !requested_null_version)) {
        TransitionSourceVersionMode::Versioned
    } else if opts.version_suspended || opts.versioned {
        TransitionSourceVersionMode::VersionSuspended
    } else {
        TransitionSourceVersionMode::Unversioned
    }
}

fn transition_source_identity(
    bucket: &str,
    object: &str,
    fi: &FileInfo,
    opts: &ObjectOptions,
    stored_etag: &str,
) -> Result<TransitionSourceIdentity> {
    let version_mode = transition_source_version_mode(opts, fi);
    let mod_time = fi
        .mod_time
        .ok_or_else(|| Error::other("transition source identity requires mod_time"))?
        .unix_timestamp_nanos();
    let mod_time_unix_nanos =
        i64::try_from(mod_time).map_err(|_| Error::other("transition source mod_time timestamp overflow"))?;
    let version_id = if version_mode == TransitionSourceVersionMode::Versioned {
        fi.version_id.filter(|version_id| !version_id.is_nil())
    } else {
        None
    };
    let data_dir = fi
        .data_dir
        .ok_or_else(|| Error::other("transition source identity requires data_dir"))?;
    Ok(TransitionSourceIdentity {
        bucket: bucket.to_string(),
        object: object.to_string(),
        version_id,
        data_dir,
        mod_time_unix_nanos,
        size: fi.size,
        etag: stored_etag.to_string(),
        version_mode,
    })
}

async fn save_transition_transaction_if_available(api: Option<&Arc<ECStore>>, transaction: &TransitionTransaction) -> Result<()> {
    #[cfg(test)]
    let started = std::time::Instant::now();
    let result = if let Some(api) = api {
        save_transition_transaction_record(api.clone(), transaction).await
    } else {
        #[cfg(test)]
        {
            Ok(())
        }
        #[cfg(not(test))]
        {
            Err(Error::other("transition transaction store is unavailable"))
        }
    };
    #[cfg(test)]
    record_transition_transaction_mutation(
        transaction,
        TransitionTransactionMutationKind::Create,
        None,
        started.elapsed(),
        result.is_ok(),
    );
    result
}

async fn compare_and_save_transition_transaction_if_available(
    api: Option<&Arc<ECStore>>,
    expected: &TransitionTransaction,
    next: &TransitionTransaction,
) -> Result<()> {
    #[cfg(test)]
    let started = std::time::Instant::now();
    let result = if let Some(api) = api {
        // The transition worker already has a deep poll chain. Keep the CAS
        // read/write/receipt future off Tokio's default worker stack.
        Box::pin(save_transition_transaction_record_if_current(api.clone(), expected, next)).await
    } else {
        #[cfg(test)]
        {
            Ok(())
        }
        #[cfg(not(test))]
        {
            Err(Error::other("transition transaction store is unavailable"))
        }
    };
    #[cfg(test)]
    record_transition_transaction_mutation(
        next,
        TransitionTransactionMutationKind::CompareAndSave,
        Some(expected.state),
        started.elapsed(),
        result.is_ok(),
    );
    result
}

async fn advance_and_save_transition_transaction(
    api: Option<&Arc<ECStore>>,
    transaction: &mut TransitionTransaction,
    next: TransitionTransactionState,
    remote_version: Option<TransitionRemoteVersion>,
) -> Result<()> {
    let expected = transaction.clone();
    let mut advanced = expected.clone();
    advanced
        .advance(expected.fence(), next, remote_version)
        .map_err(Error::other)?;
    compare_and_save_transition_transaction_if_available(api, &expected, &advanced).await?;
    *transaction = advanced;
    Ok(())
}

#[cfg(test)]
struct TransitionTransactionMutationProbeState {
    bucket: String,
    object: String,
    observations: std::sync::Mutex<Vec<TransitionTransactionMutationObservation>>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransitionTransactionMutationKind {
    Create,
    CompareAndSave,
    Delete,
}

#[cfg(test)]
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "full mutation measurements are consumed by tests behind `--features test-util`"
)]
pub(crate) struct TransitionTransactionMutationObservation {
    pub(crate) kind: TransitionTransactionMutationKind,
    pub(crate) previous_state: Option<TransitionTransactionState>,
    pub(crate) state: TransitionTransactionState,
    pub(crate) encoded_bytes: usize,
    pub(crate) elapsed: std::time::Duration,
    pub(crate) succeeded: bool,
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "installed by set_disk tests behind `--features test-util` (backlog#1823)"
)]
pub(crate) struct TransitionTransactionMutationProbe {
    state: Arc<TransitionTransactionMutationProbeState>,
}

#[cfg(test)]
static TRANSITION_TRANSACTION_MUTATION_PROBE: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<TransitionTransactionMutationProbeState>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
impl TransitionTransactionMutationProbe {
    #[allow(
        dead_code,
        reason = "installed by set_disk tests behind `--features test-util` (backlog#1823)"
    )]
    pub(crate) fn install(bucket: &str, object: &str) -> Self {
        let state = Arc::new(TransitionTransactionMutationProbeState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            observations: std::sync::Mutex::new(Vec::new()),
        });
        let mut slot = TRANSITION_TRANSACTION_MUTATION_PROBE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition transaction mutation probe mutex should not poison");
        assert!(
            slot.is_none(),
            "transition transaction mutation probe must be installed by one test at a time"
        );
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    #[allow(
        dead_code,
        reason = "installed by set_disk tests behind `--features test-util` (backlog#1823)"
    )]
    fn attempts(&self) -> usize {
        self.observations()
            .into_iter()
            .filter(|observation| {
                observation.kind == TransitionTransactionMutationKind::CompareAndSave
                    && observation.state == TransitionTransactionState::Uploaded
            })
            .count()
    }

    pub(crate) fn observations(&self) -> Vec<TransitionTransactionMutationObservation> {
        self.state
            .observations
            .lock()
            .expect("transition transaction mutation observations mutex should not poison")
            .clone()
    }
}

#[cfg(test)]
impl Drop for TransitionTransactionMutationProbe {
    fn drop(&mut self) {
        let mut slot = TRANSITION_TRANSACTION_MUTATION_PROBE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition transaction mutation probe mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
fn record_transition_transaction_mutation(
    transaction: &TransitionTransaction,
    kind: TransitionTransactionMutationKind,
    previous_state: Option<TransitionTransactionState>,
    elapsed: std::time::Duration,
    succeeded: bool,
) {
    let state = TRANSITION_TRANSACTION_MUTATION_PROBE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("transition transaction mutation probe mutex should not poison")
        .as_ref()
        .filter(|state| state.bucket == transaction.source.bucket && state.object == transaction.source.object)
        .cloned();
    if let Some(state) = state {
        let encoded_bytes = transaction.encode().map_or(0, |encoded| encoded.len());
        state
            .observations
            .lock()
            .expect("transition transaction mutation observations mutex should not poison")
            .push(TransitionTransactionMutationObservation {
                kind,
                previous_state,
                state: transaction.state,
                encoded_bytes,
                elapsed,
                succeeded,
            });
    }
}

async fn delete_transition_transaction_if_available(
    api: Option<&Arc<ECStore>>,
    transaction: &TransitionTransaction,
) -> Result<()> {
    #[cfg(test)]
    let started = std::time::Instant::now();
    let result = if let Some(api) = api {
        // Conditional delete now includes a read and terminal receipt; box it
        // for the same transition-worker stack bound as the CAS path above.
        Box::pin(delete_transition_transaction_record(api.clone(), transaction)).await
    } else {
        Ok(())
    };
    #[cfg(test)]
    record_transition_transaction_mutation(
        transaction,
        TransitionTransactionMutationKind::Delete,
        Some(transaction.state),
        started.elapsed(),
        result.is_ok(),
    );
    result
}

async fn delete_transition_transaction_after_remote_cleanup(
    api: Option<&Arc<ECStore>>,
    transaction: &TransitionTransaction,
    bucket: &str,
    object: &str,
) {
    if let Err(err) = delete_transition_transaction_if_available(api, transaction).await {
        warn!(
            bucket = bucket,
            object = object,
            transaction_id = %transaction.transaction_id,
            error = ?err,
            "transition remote candidate was cleaned but transaction record cleanup failed"
        );
    }
}

#[cfg(feature = "test-util")]
#[derive(Default)]
struct TransitionCleanupStoreBarrierState {
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(feature = "test-util")]
/// One-shot test barrier placed before transition cleanup resolves its ECStore.
pub(crate) struct TransitionCleanupStoreBarrier {
    state: Arc<TransitionCleanupStoreBarrierState>,
}

#[cfg(feature = "test-util")]
static TRANSITION_CLEANUP_STORE_BARRIER: std::sync::OnceLock<std::sync::Mutex<Option<Arc<TransitionCleanupStoreBarrierState>>>> =
    std::sync::OnceLock::new();

#[cfg(feature = "test-util")]
impl TransitionCleanupStoreBarrier {
    /// Install the process-local barrier for the next cleanup-store resolution.
    pub(crate) fn install() -> Self {
        let state = Arc::new(TransitionCleanupStoreBarrierState::default());
        let mut slot = TRANSITION_CLEANUP_STORE_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition cleanup store barrier mutex should not poison");
        assert!(slot.is_none(), "transition cleanup store barrier must be installed by one test at a time");
        *slot = Some(state.clone());
        drop(slot);
        Self { state }
    }

    /// Wait until a transition reaches the cleanup-store resolution boundary.
    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("transition should reach the cleanup store barrier");
    }
}

#[cfg(feature = "test-util")]
impl Drop for TransitionCleanupStoreBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = TRANSITION_CLEANUP_STORE_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition cleanup store barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(feature = "test-util")]
async fn pause_transition_cleanup_store() {
    let barrier = TRANSITION_CLEANUP_STORE_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("transition cleanup store barrier mutex should not poison")
        .take();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(all(test, feature = "test-util"))]
struct TransitionUploadCandidateBarrier {
    state: Arc<TransitionCleanupStoreBarrierState>,
}

#[cfg(all(test, feature = "test-util"))]
static TRANSITION_UPLOAD_CANDIDATE_BARRIER: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<TransitionCleanupStoreBarrierState>>>,
> = std::sync::OnceLock::new();

#[cfg(all(test, feature = "test-util"))]
impl TransitionUploadCandidateBarrier {
    fn install() -> Self {
        let state = Arc::new(TransitionCleanupStoreBarrierState::default());
        let mut slot = TRANSITION_UPLOAD_CANDIDATE_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition upload candidate barrier mutex should not poison");
        assert!(
            slot.is_none(),
            "transition upload candidate barrier must be installed by one test at a time"
        );
        *slot = Some(state.clone());
        drop(slot);
        Self { state }
    }

    async fn wait_until_paused(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("transition should record its remote upload candidate");
    }
}

#[cfg(all(test, feature = "test-util"))]
impl Drop for TransitionUploadCandidateBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = TRANSITION_UPLOAD_CANDIDATE_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition upload candidate barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(all(test, feature = "test-util"))]
async fn pause_after_transition_upload_candidate_recorded() {
    let barrier = TRANSITION_UPLOAD_CANDIDATE_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("transition upload candidate barrier mutex should not poison")
        .take();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(all(test, feature = "test-util"))]
struct TransitionUploadedCommitBarrierState {
    bucket: String,
    object: String,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) struct TransitionUploadedCommitBarrier {
    state: Arc<TransitionUploadedCommitBarrierState>,
}

#[cfg(all(test, feature = "test-util"))]
static TRANSITION_UPLOADED_COMMIT_BARRIER: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<TransitionUploadedCommitBarrierState>>>,
> = std::sync::OnceLock::new();

#[cfg(all(test, feature = "test-util"))]
impl TransitionUploadedCommitBarrier {
    pub(crate) fn install(bucket: &str, object: &str) -> Self {
        let state = Arc::new(TransitionUploadedCommitBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut slot = TRANSITION_UPLOADED_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition uploaded commit barrier mutex should not poison");
        assert!(
            slot.is_none(),
            "transition uploaded commit barrier must be installed by one test at a time"
        );
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("transition should persist Uploaded before acquiring its commit lock");
    }

    pub(crate) fn release(&self) {
        self.state.release.notify_one();
    }
}

#[cfg(all(test, feature = "test-util"))]
impl Drop for TransitionUploadedCommitBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = TRANSITION_UPLOADED_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition uploaded commit barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(all(test, feature = "test-util"))]
async fn pause_after_transition_uploaded_persisted(bucket: &str, object: &str) {
    let barrier = TRANSITION_UPLOADED_COMMIT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("transition uploaded commit barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.bucket == bucket && barrier.object == object)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(all(test, feature = "test-util"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransitionTransactionKillPoint {
    PrePutFence,
    UploadBeforeCommitFence,
    CommitFenceBeforeLocalCommit,
    LocalCommitBeforeDelete,
}

#[cfg(all(test, feature = "test-util"))]
struct TransitionTransactionKillPointBarrierState {
    bucket: String,
    object: String,
    point: TransitionTransactionKillPoint,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) struct TransitionTransactionKillPointBarrier {
    state: Arc<TransitionTransactionKillPointBarrierState>,
}

#[cfg(all(test, feature = "test-util"))]
static TRANSITION_TRANSACTION_KILL_POINT_BARRIER: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<TransitionTransactionKillPointBarrierState>>>,
> = std::sync::OnceLock::new();

#[cfg(all(test, feature = "test-util"))]
impl TransitionTransactionKillPointBarrier {
    pub(crate) fn install(bucket: &str, object: &str, point: TransitionTransactionKillPoint) -> Self {
        let state = Arc::new(TransitionTransactionKillPointBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            point,
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut slot = TRANSITION_TRANSACTION_KILL_POINT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition transaction kill-point barrier mutex should not poison");
        assert!(slot.is_none(), "one transition transaction kill-point may be installed at a time");
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("transition should reach the requested transaction kill-point");
    }
}

#[cfg(all(test, feature = "test-util"))]
impl Drop for TransitionTransactionKillPointBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = TRANSITION_TRANSACTION_KILL_POINT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition transaction kill-point barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(all(test, feature = "test-util"))]
async fn pause_transition_transaction_at(bucket: &str, object: &str, point: TransitionTransactionKillPoint) {
    let barrier = TRANSITION_TRANSACTION_KILL_POINT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("transition transaction kill-point barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.bucket == bucket && barrier.object == object && barrier.point == point)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(all(test, feature = "test-util"))]
fn transition_transaction_kill_point_is_active(transaction: Option<&TransitionTransaction>) -> bool {
    let Some(transaction) = transaction else {
        return false;
    };
    TRANSITION_TRANSACTION_KILL_POINT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("transition transaction kill-point barrier mutex should not poison")
        .as_ref()
        .is_some_and(|barrier| barrier.bucket == transaction.source.bucket && barrier.object == transaction.source.object)
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TransitionCommitPause {
    BeforeLockLost,
    BeforeLeaseValidation,
    AfterLeaseValidation,
}

#[cfg(test)]
struct TransitionCommitBarrierState {
    bucket: String,
    object: String,
    pause: TransitionCommitPause,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "installed by set_disk tests behind `--features test-util` (backlog#1823)"
)]
struct TransitionCommitBarrier {
    state: Arc<TransitionCommitBarrierState>,
}

#[cfg(test)]
static TRANSITION_COMMIT_BARRIER: std::sync::OnceLock<std::sync::Mutex<Option<Arc<TransitionCommitBarrierState>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
impl TransitionCommitBarrier {
    #[allow(
        dead_code,
        reason = "installed by set_disk tests behind `--features test-util` (backlog#1823)"
    )]
    fn install_before_lock_lost_check(bucket: &str, object: &str) -> Self {
        Self::install_at(bucket, object, TransitionCommitPause::BeforeLockLost)
    }

    #[allow(
        dead_code,
        reason = "installed by set_disk tests behind `--features test-util` (backlog#1823)"
    )]
    fn install(bucket: &str, object: &str) -> Self {
        Self::install_at(bucket, object, TransitionCommitPause::BeforeLeaseValidation)
    }

    #[allow(
        dead_code,
        reason = "installed by set_disk tests behind `--features test-util` (backlog#1823)"
    )]
    fn install_after_lease_check(bucket: &str, object: &str) -> Self {
        Self::install_at(bucket, object, TransitionCommitPause::AfterLeaseValidation)
    }

    fn install_at(bucket: &str, object: &str, pause: TransitionCommitPause) -> Self {
        let state = Arc::new(TransitionCommitBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            pause,
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut slot = TRANSITION_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition commit barrier mutex should not poison");
        assert!(slot.is_none(), "transition commit barrier must be installed by one test at a time");
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    #[allow(
        dead_code,
        reason = "installed by set_disk tests behind `--features test-util` (backlog#1823)"
    )]
    async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("transition should reach the deterministic commit barrier");
    }

    #[allow(
        dead_code,
        reason = "installed by set_disk tests behind `--features test-util` (backlog#1823)"
    )]
    fn release(&self) {
        self.state.release.notify_one();
    }
}

#[cfg(test)]
impl Drop for TransitionCommitBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = TRANSITION_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition commit barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
async fn pause_transition_commit(bucket: &str, object: &str, pause: TransitionCommitPause) {
    let barrier = TRANSITION_COMMIT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("transition commit barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.bucket == bucket && barrier.object == object && barrier.pause == pause)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(test)]
fn persisted_transition_version(
    remote_version: &str,
) -> std::io::Result<(Option<String>, rustfs_filemeta::TransitionVersionState)> {
    persisted_transition_version_with_gate(remote_version, remote_version_state_writer_enabled())
}

#[cfg(test)]
fn remote_version_state_writer_enabled() -> bool {
    remote_version_state_writer_fleet_proof().is_some()
}

fn remote_version_state_writer_fleet_proof() -> Option<RemoteVersionStateFleetProofToken> {
    transaction_fencing_fleet_proof(remote_version_state_writer_requested())
}

fn remote_version_state_writer_requested() -> bool {
    transaction_fencing_gate_requested_for(
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_TIER_REMOTE_VERSION_STATE_WRITE,
            rustfs_config::DEFAULT_TIER_REMOTE_VERSION_STATE_WRITE,
        ),
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_TIER_REMOTE_VERSION_STATE_FLEET_CONFIRMED,
            rustfs_config::DEFAULT_TIER_REMOTE_VERSION_STATE_FLEET_CONFIRMED,
        ),
        true,
    )
}

fn remote_version_state_writer_fleet_proof_matches(proof: &RemoteVersionStateFleetProofToken) -> bool {
    transaction_fencing_fleet_proof_matches_for(
        remote_version_state_writer_requested(),
        crate::services::notification_sys::remote_version_state_fleet_proof_matches(proof),
    )
}

pub(in crate::set_disk::ops) fn object_transaction_fencing_fleet_proof() -> Option<RemoteVersionStateFleetProofToken> {
    transaction_fencing_fleet_proof(object_transaction_fencing_requested())
}

pub(in crate::set_disk::ops) fn object_transaction_fencing_requested() -> bool {
    object_transaction_fencing_requested_cached()
}

#[cfg(not(test))]
fn object_transaction_fencing_requested_cached() -> bool {
    static REQUESTED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *REQUESTED.get_or_init(load_object_transaction_fencing_requested)
}

#[cfg(test)]
fn object_transaction_fencing_requested_cached() -> bool {
    load_object_transaction_fencing_requested()
}

fn load_object_transaction_fencing_requested() -> bool {
    transaction_fencing_gate_requested_for(
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_WRITE,
            rustfs_config::DEFAULT_OBJECT_TRANSACTION_FENCING_WRITE,
        ),
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED,
            rustfs_config::DEFAULT_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED,
        ),
        true,
    )
}

pub(in crate::set_disk::ops) fn object_transaction_fencing_fleet_proof_matches(
    proof: &RemoteVersionStateFleetProofToken,
) -> bool {
    transaction_fencing_fleet_proof_matches_for(
        object_transaction_fencing_requested(),
        crate::services::notification_sys::remote_version_state_fleet_proof_matches(proof),
    )
}

fn transaction_fencing_fleet_proof(requested: bool) -> Option<RemoteVersionStateFleetProofToken> {
    requested
        .then(crate::services::notification_sys::acquire_remote_version_state_fleet_proof)
        .flatten()
}

fn transaction_fencing_fleet_proof_matches_for(requested: bool, fleet_proof_matches: bool) -> bool {
    requested && fleet_proof_matches
}

fn transaction_fencing_gate_requested_for(requested: bool, fleet_confirmed: bool, fleet_proof_valid: bool) -> bool {
    requested && fleet_confirmed && fleet_proof_valid
}

#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutObjectCommitPause {
    BeforeNamespace,
    AfterNamespace,
    AfterQuotaReservation,
    BeforeQuotaRename,
    BeforeMetadata,
    BeforeTransactionEpochVerify,
    AfterRenameQuorum,
    AfterRenameHandoff,
}

#[cfg(any(test, feature = "test-util"))]
struct PutObjectCommitBarrierState {
    bucket: String,
    object: String,
    pause: PutObjectCommitPause,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
    namespace_pending: tokio::sync::Notify,
    namespace_acquired: std::sync::atomic::AtomicBool,
}

#[cfg(any(test, feature = "test-util"))]
pub struct PutObjectCommitBarrier {
    state: Arc<PutObjectCommitBarrierState>,
}

#[cfg(any(test, feature = "test-util"))]
static PUT_OBJECT_COMMIT_BARRIER: std::sync::OnceLock<std::sync::Mutex<Vec<Arc<PutObjectCommitBarrierState>>>> =
    std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-util"))]
impl PutObjectCommitBarrier {
    pub fn install(bucket: &str, object: &str, pause: PutObjectCommitPause) -> Self {
        let state = Arc::new(PutObjectCommitBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            pause,
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            namespace_pending: tokio::sync::Notify::new(),
            namespace_acquired: std::sync::atomic::AtomicBool::new(false),
        });
        let mut slot = PUT_OBJECT_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("put object commit barrier mutex should not poison");
        assert!(
            !slot.iter().any(|current| {
                current.bucket == state.bucket && current.object == state.object && current.pause == state.pause
            }),
            "put object commit barrier must be unique for a bucket, object, and pause"
        );
        slot.push(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    pub async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("put object should reach the deterministic commit barrier");
    }

    pub fn release(&self) {
        self.state.release.notify_one();
    }

    pub async fn release_and_wait_until_namespace_pending(&self) {
        assert_eq!(self.state.pause, PutObjectCommitPause::BeforeNamespace);
        let namespace_pending = self.state.namespace_pending.notified();
        self.release();
        tokio::time::timeout(Duration::from_secs(5), namespace_pending)
            .await
            .expect("put object should wait for the namespace lock after leaving the commit barrier");
    }

    pub fn namespace_acquired(&self) -> bool {
        self.state.namespace_acquired.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[cfg(any(test, feature = "test-util"))]
impl Drop for PutObjectCommitBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = PUT_OBJECT_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("put object commit barrier mutex should not poison");
        slot.retain(|state| !Arc::ptr_eq(state, &self.state));
    }
}

#[cfg(any(test, feature = "test-util"))]
async fn pause_put_object_commit(bucket: &str, object: &str, pause: PutObjectCommitPause) {
    let barrier = {
        let mut slot = PUT_OBJECT_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("put object commit barrier mutex should not poison");
        if let Some(index) = slot
            .iter()
            .position(|barrier| barrier.bucket == bucket && barrier.object == object && barrier.pause == pause)
        {
            if pause == PutObjectCommitPause::BeforeTransactionEpochVerify {
                Some(slot.remove(index))
            } else {
                Some(Arc::clone(&slot[index]))
            }
        } else {
            None
        }
    };
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(any(test, feature = "test-util"))]
fn notify_put_object_commit_namespace_pending(bucket: &str, object: &str) {
    let barrier = PUT_OBJECT_COMMIT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
        .expect("put object commit barrier mutex should not poison")
        .iter()
        .find(|barrier| {
            barrier.bucket == bucket && barrier.object == object && barrier.pause == PutObjectCommitPause::BeforeNamespace
        })
        .cloned();
    if let Some(barrier) = barrier {
        barrier.namespace_pending.notify_one();
    }
}

#[cfg(any(test, feature = "test-util"))]
fn notify_put_object_commit_namespace_acquired(bucket: &str, object: &str) {
    let barrier = PUT_OBJECT_COMMIT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
        .expect("put object commit barrier mutex should not poison")
        .iter()
        .find(|barrier| {
            barrier.bucket == bucket && barrier.object == object && barrier.pause == PutObjectCommitPause::BeforeNamespace
        })
        .cloned();
    if let Some(barrier) = barrier {
        barrier.namespace_acquired.store(true, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
struct DeleteObjectCommitBarrierState {
    bucket: String,
    object: String,
    pause_after_publish: bool,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
pub(crate) struct DeleteObjectCommitBarrier {
    state: Arc<DeleteObjectCommitBarrierState>,
}

#[cfg(test)]
static DELETE_OBJECT_COMMIT_BARRIER: std::sync::OnceLock<std::sync::Mutex<Option<Arc<DeleteObjectCommitBarrierState>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
impl DeleteObjectCommitBarrier {
    pub(crate) fn install(bucket: &str, object: &str) -> Self {
        Self::install_with_mode(bucket, object, false)
    }

    #[cfg(feature = "test-util")]
    pub(crate) fn install_after_publish(bucket: &str, object: &str) -> Self {
        Self::install_with_mode(bucket, object, true)
    }

    fn install_with_mode(bucket: &str, object: &str, pause_after_publish: bool) -> Self {
        let state = Arc::new(DeleteObjectCommitBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            pause_after_publish,
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut slot = DELETE_OBJECT_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("delete object commit barrier mutex should not poison");
        assert!(slot.is_none(), "delete object commit barrier must be unique");
        *slot = Some(Arc::clone(&state));
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("delete object should reach the deterministic commit barrier");
    }

    pub(crate) fn release(&self) {
        self.state.release.notify_one();
    }
}

#[cfg(test)]
impl Drop for DeleteObjectCommitBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = DELETE_OBJECT_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("delete object commit barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
async fn pause_delete_object_commit(bucket: &str, object: &str) {
    let barrier = DELETE_OBJECT_COMMIT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("delete object commit barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.bucket == bucket && barrier.object == object && !barrier.pause_after_publish)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(test)]
async fn pause_delete_object_commit_after_publish(bucket: &str, object: &str) {
    let barrier = DELETE_OBJECT_COMMIT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("delete object commit barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.bucket == bucket && barrier.object == object && barrier.pause_after_publish)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

fn persisted_transition_version_with_gate(
    remote_version: &str,
    remote_version_state_writer_enabled: bool,
) -> std::io::Result<(Option<String>, rustfs_filemeta::TransitionVersionState)> {
    if remote_version.is_empty() {
        return Ok((None, rustfs_filemeta::TransitionVersionState::KnownDisabled));
    }

    match Uuid::parse_str(remote_version) {
        Ok(version_id) if version_id.is_nil() => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "remote tier returned a nil object version ID",
        )),
        Ok(_) => Ok((Some(remote_version.to_string()), rustfs_filemeta::TransitionVersionState::Exact)),
        Err(_) if !remote_version_state_writer_enabled => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "opaque remote tier versions require the operator-attested live fleet capability gate",
        )),
        Err(_) if remote_version == "null" => {
            Ok((Some(remote_version.to_string()), rustfs_filemeta::TransitionVersionState::SuspendedNull))
        }
        Err(_) => Ok((Some(remote_version.to_string()), rustfs_filemeta::TransitionVersionState::Exact)),
    }
}

#[cfg(test)]
#[derive(Default)]
struct ObjectTaggingCommitBarrierState {
    bucket: String,
    object: String,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
struct ObjectTaggingCommitBarrier {
    state: Arc<ObjectTaggingCommitBarrierState>,
}

#[cfg(test)]
static OBJECT_TAGGING_COMMIT_BARRIER: std::sync::OnceLock<std::sync::Mutex<Option<Arc<ObjectTaggingCommitBarrierState>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
impl ObjectTaggingCommitBarrier {
    fn install(bucket: &str, object: &str) -> Self {
        let state = Arc::new(ObjectTaggingCommitBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            ..Default::default()
        });
        let mut slot = OBJECT_TAGGING_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("object tagging commit barrier mutex should not poison");
        assert!(slot.is_none(), "object tagging commit barrier must be installed by one test at a time");
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("object tagging should reach the deterministic commit barrier");
    }

    fn release(&self) {
        self.state.release.notify_one();
    }
}

#[cfg(test)]
impl Drop for ObjectTaggingCommitBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = OBJECT_TAGGING_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("object tagging commit barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
async fn pause_object_tagging_commit(bucket: &str, object: &str) {
    let barrier = OBJECT_TAGGING_COMMIT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("object tagging commit barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.bucket == bucket && barrier.object == object)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

#[cfg(test)]
mod transition_upload_completion_tests {
    use super::*;

    fn consumed(bytes: u64) -> Arc<AtomicU64> {
        Arc::new(AtomicU64::new(bytes))
    }

    #[tokio::test]
    async fn rejects_source_errors_at_first_middle_and_last_chunk() {
        let remote_version = Uuid::nil().to_string();
        for consumed_bytes in [0, 512, 1023] {
            let result = complete_transition_upload(
                std::future::ready(Ok(remote_version.clone())),
                std::future::ready(Err(StorageError::FileCorrupt)),
                1024,
                consumed(consumed_bytes),
            )
            .await;
            let failure = result.expect_err("a source read error must fail the upload completion protocol");
            assert!(matches!(failure.error, StorageError::FileCorrupt));
            assert_eq!(
                failure.candidate.as_ref().map(TransitionUploadCandidate::remote_version),
                Some(remote_version.as_str())
            );
        }
    }

    #[tokio::test]
    async fn rejects_partial_body_accepted_by_remote() {
        let failure = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            std::future::ready(Ok(1024)),
            1024,
            consumed(511),
        )
        .await
        .expect_err("remote success must not hide a partially consumed body");
        assert!(matches!(failure.error, StorageError::LessData));
        assert!(failure.candidate.is_some());
    }

    #[tokio::test]
    async fn maps_source_panic_cancel_and_early_close_to_failures() {
        let panic_failure = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            async {
                panic!("injected transition producer panic");
                #[allow(unreachable_code)]
                Ok(0)
            },
            1,
            consumed(0),
        )
        .await
        .expect_err("a producer panic must not enter local commit");
        assert!(matches!(panic_failure.error, StorageError::Unexpected));

        let cancelled = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            std::future::ready(Err(StorageError::OperationCanceled)),
            1,
            consumed(0),
        )
        .await
        .expect_err("a cancelled producer must not enter local commit");
        assert!(matches!(cancelled.error, StorageError::OperationCanceled));

        let early_close = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            std::future::ready(Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "remote reader closed early",
            )))),
            1024,
            consumed(16),
        )
        .await
        .expect_err("an early remote close must not enter local commit");
        assert!(matches!(early_close.error, StorageError::Io(ref err) if err.kind() == std::io::ErrorKind::BrokenPipe));
    }

    #[tokio::test]
    async fn rejects_declared_size_mismatches_and_accepts_zero_size() {
        let shorter = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            std::future::ready(Ok(511)),
            512,
            consumed(511),
        )
        .await
        .expect_err("a short source must fail the declared-size check");
        assert!(matches!(shorter.error, StorageError::LessData));

        let longer = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            std::future::ready(Ok(513)),
            512,
            consumed(513),
        )
        .await
        .expect_err("an oversized source must fail the declared-size check");
        assert!(matches!(longer.error, StorageError::MoreData));

        let exact_remote_version = Uuid::new_v4().to_string();
        let exact = complete_transition_upload(
            std::future::ready(Ok(exact_remote_version.clone())),
            std::future::ready(Ok(512)),
            512,
            consumed(512),
        )
        .await
        .expect("an exact producer and consumer byte count must complete");
        assert_eq!(exact.candidate.remote_version(), exact_remote_version);
        assert_eq!((exact.produced, exact.consumed), (512, 512));

        let remote_version = Uuid::nil().to_string();
        let result =
            complete_transition_upload(std::future::ready(Ok(remote_version.clone())), std::future::ready(Ok(0)), 0, consumed(0))
                .await
                .expect("an empty source and empty remote body must complete");
        assert_eq!(result.candidate.remote_version(), remote_version);
        assert_eq!((result.produced, result.consumed), (0, 0));
    }

    #[tokio::test]
    async fn preserves_remote_error_when_commit_status_is_unknown() {
        let failure = complete_transition_upload(
            std::future::ready(Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "remote response was lost"))),
            std::future::ready(Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "consumer disappeared",
            )))),
            1024,
            consumed(0),
        )
        .await
        .expect_err("an unknown remote commit result must fail closed");
        assert!(matches!(failure.error, StorageError::Io(ref err) if err.kind() == std::io::ErrorKind::ConnectionReset));
        assert!(failure.candidate.is_none(), "unknown remote versions must never enter precise cleanup");

        let source_failure = complete_transition_upload(
            std::future::ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "remote rejected the truncated body",
            ))),
            std::future::ready(Err(StorageError::FileCorrupt)),
            1024,
            consumed(128),
        )
        .await
        .expect_err("a source integrity error must survive a concurrent remote failure");
        assert!(matches!(source_failure.error, StorageError::FileCorrupt));
        assert!(source_failure.candidate.is_none());
    }
}

#[cfg(test)]
mod transition_version_id_tests {
    use super::{
        TransitionUploadCandidate, persisted_transition_version, persisted_transition_version_with_gate,
        transaction_fencing_fleet_proof_matches_for, transaction_fencing_gate_requested_for,
    };
    use rustfs_filemeta::TransitionVersionState;
    use uuid::Uuid;

    #[test]
    fn normalizes_persisted_unversioned_ids_and_preserves_put_constraints() {
        assert_eq!(
            persisted_transition_version("").expect("empty remote version identifies an unversioned tier"),
            (None, TransitionVersionState::KnownDisabled)
        );
        assert!(persisted_transition_version(&Uuid::nil().to_string()).is_err());
        let nil_put_response = Uuid::nil().to_string();
        let nil_candidate = TransitionUploadCandidate::from_put_response(nil_put_response.clone());
        assert_eq!(nil_candidate.cleanup_version(), nil_put_response);
        assert!(nil_candidate.cleanup_version_is_exact());

        let empty_candidate = TransitionUploadCandidate::from_put_response(String::new());
        assert_eq!(empty_candidate.cleanup_version(), "");
        assert!(!empty_candidate.cleanup_version_is_exact());
    }

    #[test]
    fn preserves_uuid_and_gates_opaque_remote_ids() {
        let version_id = Uuid::new_v4();
        assert_eq!(
            persisted_transition_version(&version_id.to_string()).expect("UUID remote version"),
            (Some(version_id.to_string()), TransitionVersionState::Exact)
        );
        assert!(persisted_transition_version("null").is_err());
        assert!(persisted_transition_version("opaque-version-token").is_err());
        assert_eq!(
            TransitionUploadCandidate::from_put_response(version_id.to_string()).cleanup_version(),
            version_id.to_string()
        );
        assert_eq!(
            TransitionUploadCandidate::from_put_response("opaque-version-token".to_string()).cleanup_version(),
            "opaque-version-token"
        );
    }

    #[test]
    fn remote_version_state_writer_requires_request_and_fleet_confirmation() {
        for (case, requested, fleet_confirmed, fleet_proof_valid, expected) in [
            ("old defaults", false, false, false, false),
            ("missing fleet confirmation", true, false, true, false),
            ("missing local opt-in", false, true, true, false),
            ("missing fleet proof", true, true, false, false),
            ("explicitly unconfirmed fleet", true, false, true, false),
            ("rolled-back writer", false, true, true, false),
            ("fully upgraded fleet", true, true, true, true),
        ] {
            assert_eq!(
                transaction_fencing_gate_requested_for(requested, fleet_confirmed, fleet_proof_valid),
                expected,
                "{case}"
            );
        }
    }

    #[test]
    fn object_transaction_fencing_gate_requires_request_confirmation_and_live_proof() {
        for (case, requested, fleet_confirmed, fleet_proof_valid, expected) in [
            ("old defaults", false, false, false, false),
            ("missing fleet confirmation", true, false, true, false),
            ("missing local opt-in", false, true, true, false),
            ("missing fleet proof", true, true, false, false),
            ("fully upgraded fleet", true, true, true, true),
        ] {
            assert_eq!(
                transaction_fencing_gate_requested_for(requested, fleet_confirmed, fleet_proof_valid),
                expected,
                "{case}"
            );
        }
    }

    #[test]
    fn remote_version_state_commit_rechecks_operator_gate_and_live_proof() {
        for (case, requested, fleet_proof_matches, expected) in [
            ("operator gate closed", false, true, false),
            ("fleet proof changed", true, false, false),
            ("current authorization", true, true, true),
        ] {
            assert_eq!(
                transaction_fencing_fleet_proof_matches_for(requested, fleet_proof_matches),
                expected,
                "{case}"
            );
        }
    }

    #[test]
    fn fleet_gate_enables_null_and_opaque_remote_version_states() {
        for (remote_version, expected) in [
            ("null", (Some("null".to_string()), TransitionVersionState::SuspendedNull)),
            (
                "opaque-version-token",
                (Some("opaque-version-token".to_string()), TransitionVersionState::Exact),
            ),
        ] {
            assert!(
                persisted_transition_version_with_gate(remote_version, false).is_err(),
                "missing fleet confirmation must reject {remote_version:?}"
            );
            assert_eq!(
                persisted_transition_version_with_gate(remote_version, true).expect("fleet-confirmed state must be persisted"),
                expected
            );
        }
        assert_eq!(
            persisted_transition_version_with_gate("", true).expect("empty remote version identifies an unversioned tier"),
            (None, TransitionVersionState::KnownDisabled)
        );
    }
}

impl SetDisks {
    pub(crate) async fn replay_authorized_tier_delete_sources(
        &self,
        bucket: &str,
        object: &str,
        authorization: &TierDeleteDispatchAuthorization,
        opts: &ObjectOptions,
    ) -> Result<usize> {
        let versions = match self.load_file_info_versions_exact(bucket, object).await {
            Ok(Some(versions)) => versions,
            Ok(None) => return Ok(0),
            Err(err) if crate::error::is_err_strict_volume_not_found(&err) => return Ok(0),
            Err(err) => return Err(err),
        };
        let lock_state = opts
            .object_lock_config_snapshot
            .as_deref()
            .ok_or_else(|| Error::other("authorized tier delete replay is missing its Object Lock snapshot"))?
            .state();
        let bypass_governance = opts
            .object_lock_delete
            .as_ref()
            .is_some_and(|delete_opts| delete_opts.bypass_governance);
        let encoded_object = rustfs_utils::path::encode_dir_object(object);
        let mut deleted = 0;

        for version in versions.versions {
            let Some(candidate) = lifecycle_delete_all_tier_journal_entry(bucket, &encoded_object, &version, opts)? else {
                continue;
            };
            if !authorization.authorizes_journal_entry(&candidate)? {
                continue;
            }
            let object_info =
                ObjectInfo::from_file_info(&version, bucket, &encoded_object, opts.versioned || opts.version_suspended);
            if check_object_lock_for_deletion_with_state(lock_state, &object_info, bypass_governance)?.is_some() {
                return Err(StorageError::PrefixAccessDenied(bucket.to_string(), object.to_string()));
            }
            let replication_delete = if opts.lifecycle_delete_all.is_some() {
                lifecycle_delete_all_replication_delete(bucket, &encoded_object, &version, opts)?
            } else {
                None
            };
            let mut delete_request = FileInfo {
                name: encoded_object.clone(),
                version_id: version.version_id,
                replication_state_internal: replication_delete
                    .as_ref()
                    .map(|(state, _)| replication_state_to_filemeta(state)),
                ..Default::default()
            };
            delete_request.set_tier_free_version_id(&Uuid::new_v4().to_string());
            delete_request.set_skip_tier_free_version();
            ensure_delete_commit_locks_held(None, bucket, &encoded_object, opts)?;
            authorization.authorized_journal_name(&candidate)?;
            begin_scanner_publication_delete_mutation(opts.scanner_publication_commit_scope.as_ref())?;
            self.delete_object_version(bucket, &encoded_object, &delete_request, false)
                .await?;
            if let Some((_, deleted_object)) = replication_delete {
                ReplicationLifecycleBridge::schedule_delete(bucket.to_string(), deleted_object).await;
            }
            deleted += 1;
        }

        ensure_delete_commit_locks_held(None, bucket, &encoded_object, opts)?;
        if let Some(remaining) = self.load_file_info_versions_exact(bucket, object).await? {
            for version in remaining.versions {
                let Some(candidate) = lifecycle_delete_all_tier_journal_entry(bucket, &encoded_object, &version, opts)? else {
                    continue;
                };
                if authorization.authorizes_journal_entry(&candidate)? {
                    return Err(Error::other("authorized tier delete source remained after exact replay"));
                }
            }
        }
        if deleted > 0 {
            self.invalidate_get_object_metadata_cache(bucket, &encoded_object).await;
        }
        Ok(deleted)
    }

    async fn update_object_tags_locked(
        &self,
        operation: &'static str,
        bucket: &str,
        object: &str,
        tags: &str,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let object_lock_guard = if opts.no_lock {
            None
        } else {
            Some(self.acquire_write_lock_diag(operation, bucket, object).await?)
        };
        // Force the full quorum fanout (allow_early_stop=false): `disks` is the
        // write target below, and an early-stop subset would only carry read
        // quorum, failing write quorum on update_object_meta (backlog#872).
        let read_opts = opts.as_read_opts();
        let (mut fi, _, disks) = self
            .get_object_fileinfo_gated(bucket, object, &read_opts, false, false)
            .await?
            .into_owned();

        fi.metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags.to_owned());
        if let Some(eval_metadata) = &opts.eval_metadata
            && merge_evaluated_metadata(&mut fi.metadata, eval_metadata)?
        {
            rebuild_file_info_replication_state(&mut fi);
        }
        fi.acknowledge_data_movement();

        #[cfg(test)]
        pause_object_tagging_commit(bucket, object).await;
        // Fence the read-modify-write before any disk can merge metadata derived
        // from this read after another writer has reacquired the same key.
        if object_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: operation,
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }

        self.update_object_meta(bucket, object, fi.clone(), &disks).await?;

        Ok(ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended))
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::object::ObjectOperations for SetDisks {
    type Error = Error;
    type ObjectInfo = ObjectInfo;
    type ObjectOptions = ObjectOptions;
    type FileInfo = FileInfo;
    type ObjectToDelete = ObjectToDelete;
    type DeletedObject = DeletedObject;

    #[tracing::instrument(skip(self))]
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        src_info: &mut ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        if !src_info.metadata_only {
            if path_join_buf(&[src_bucket, src_object]) != path_join_buf(&[dst_bucket, dst_object]) {
                return Err(StorageError::NotImplemented);
            }
            // Self-copy with a data reader: write tier data back locally (de-tiering).
            // Handles `mc cp --storage-class STANDARD obj obj` on a transitioned object.
            if let Some(mut put_reader) = src_info.put_object_reader.take() {
                return self.put_object(dst_bucket, dst_object, &mut put_reader, dst_opts).await;
            }
            // Same-key tiered copy without a pre-fetched reader: fall through to the metadata
            // path so the caller gets a disk/quorum error rather than NotImplemented.
        }

        if path_join_buf(&[src_bucket, src_object]) != path_join_buf(&[dst_bucket, dst_object]) {
            return Err(StorageError::NotImplemented);
        }

        let _lock_guard = if dst_opts.no_lock {
            None
        } else {
            Some(
                self.acquire_write_lock_diag("copy_object_metadata", dst_bucket, dst_object)
                    .await?,
            )
        };

        if let Some(expected) = dst_opts.expected_current_version_id.as_deref() {
            let current = self
                .get_object_info(
                    dst_bucket,
                    dst_object,
                    &ObjectOptions {
                        no_lock: true,
                        metadata_cache_safe: false,
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|err| {
                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                        StorageError::PreconditionFailed
                    } else {
                        err
                    }
                })?;
            if current.version_id.map(|version| version.to_string()).as_deref() != Some(expected) {
                return Err(StorageError::PreconditionFailed);
            }
        }

        self.invalidate_get_object_metadata_cache(dst_bucket, dst_object).await;

        if dst_opts.http_preconditions.is_some()
            && let Some(err) = self.check_write_precondition(dst_bucket, dst_object, dst_opts).await
        {
            return Err(err);
        }

        let disks = self.get_disks_internal().await;

        let (mut metas, errs) = {
            if let Some(vid) = &src_opts.version_id {
                Self::read_all_fileinfo(&disks, "", src_bucket, src_object, vid, true, false, false).await?
            } else {
                Self::read_all_xl(&disks, src_bucket, src_object, true, false).await
            }
        };

        let (read_quorum, write_quorum) = match Self::object_quorum_from_meta(&metas, &errs, self.default_parity_count) {
            Ok((r, w)) => (
                usize::try_from(r)
                    .map_err(|_| to_object_err(DiskError::ErasureReadQuorum.into(), vec![src_bucket, src_object]))?,
                usize::try_from(w)
                    .map_err(|_| to_object_err(DiskError::ErasureWriteQuorum.into(), vec![src_bucket, src_object]))?,
            ),
            Err(mut err) => {
                if err == DiskError::ErasureReadQuorum
                    && !src_bucket.starts_with(RUSTFS_META_BUCKET)
                    && self
                        .delete_if_dangling(src_bucket, src_object, &metas, &errs, &HashMap::new(), src_opts.clone())
                        .await
                        .is_ok()
                {
                    if src_opts.version_id.is_some() {
                        err = DiskError::FileVersionNotFound
                    } else {
                        err = DiskError::FileNotFound
                    }
                }
                return Err(to_object_err(err.into(), vec![src_bucket, src_object]));
            }
        };

        let src_version_id = src_opts.version_id.as_deref().unwrap_or_default();
        let (online_disks, mut fi, _) =
            Self::select_valid_fileinfo(&disks, &metas, &errs, src_version_id, read_quorum, write_quorum)
                .map_err(|e| to_object_err(e.into(), vec![src_bucket, src_object]))?;

        if fi.deleted {
            if src_opts.version_id.is_none() {
                return Err(to_object_err(Error::FileNotFound, vec![src_bucket, src_object]));
            }
            return Err(to_object_err(Error::MethodNotAllowed, vec![src_bucket, src_object]));
        }

        let restore_metadata_update = src_info.metadata_only
            && src_bucket == dst_bucket
            && src_object == dst_object
            && src_opts.version_id == dst_opts.version_id
            && src_info
                .user_defined
                .keys()
                .any(|key| key.eq_ignore_ascii_case(X_AMZ_RESTORE.as_str()))
            && restore_metadata_update_preserves_protected_metadata(&fi.metadata, src_info.user_defined.as_ref());
        if let Some(dst_version_id) = dst_opts.version_id.as_deref()
            && !is_meta_bucketname(dst_bucket)
            && !restore_metadata_update
        {
            let object_lock_config = dst_opts
                .object_lock_config_snapshot
                .as_deref()
                .ok_or_else(|| Error::other("explicit-version copy is missing its Object Lock configuration snapshot"))?;
            let current = self
                .get_object_info(
                    dst_bucket,
                    dst_object,
                    &ObjectOptions {
                        version_id: Some(dst_version_id.to_string()),
                        no_lock: true,
                        metadata_cache_safe: false,
                        versioned: dst_opts.versioned,
                        version_suspended: dst_opts.version_suspended,
                        ..Default::default()
                    },
                )
                .await;
            match current {
                Ok(existing)
                    if check_object_lock_for_deletion_with_state(object_lock_config.state(), &existing, false)?.is_some() =>
                {
                    return Err(StorageError::PrefixAccessDenied(dst_bucket.to_string(), dst_object.to_string()));
                }
                Ok(_) => {}
                Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => {}
                Err(err) => return Err(err),
            }
        }

        let version_id = {
            if src_info.version_only {
                if let Some(vid) = &dst_opts.version_id {
                    Some(Uuid::parse_str(vid)?)
                } else {
                    Some(Uuid::new_v4())
                }
            } else {
                src_info.version_id
            }
        };

        let preserved_part_checksums = if (src_info.metadata_only || src_info.version_only)
            && rustfs_utils::http::contains_key_str(&fi.metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS)
        {
            Self::hydrate_selected_fileinfo_part_checksums(&mut fi).map_err(|_| Error::FileCorrupt)?;
            Some(
                rustfs_utils::http::get_consistent_str(&fi.metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS)
                    .ok_or(Error::FileCorrupt)?
                    .to_string(),
            )
        } else {
            None
        };
        let mut replacement_metadata = (*src_info.user_defined).clone();
        if let Some(part_checksums) = preserved_part_checksums {
            rustfs_utils::http::insert_str(&mut replacement_metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS, part_checksums);
        }
        if let Some(etag) = &src_info.etag {
            replacement_metadata.insert("etag".to_owned(), etag.clone());
        }
        fi.metadata = replacement_metadata.clone();

        let mod_time = OffsetDateTime::now_utc();
        fi.mod_time = Some(mod_time);
        fi.version_id = version_id;
        fi.versioned = src_opts.versioned || src_opts.version_suspended;

        if _lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
            || dst_opts
                .namespace_lock_fence
                .as_ref()
                .is_some_and(NamespaceLockFence::is_lock_lost)
            || dst_opts
                .bucket_lifecycle_lock_fence
                .as_ref()
                .is_some_and(NamespaceLockFence::is_lock_lost)
        {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "copy_object_commit",
                bucket: dst_bucket.to_string(),
                object: dst_object.to_string(),
                required: 1,
                achieved: 0,
            });
        }

        if src_info.version_only {
            let inline_data = fi.inline_data();

            for fi in metas.iter_mut() {
                if fi.has_valid_erasure_geometry() {
                    fi.metadata.clone_from(&replacement_metadata);
                    fi.mod_time = Some(mod_time);
                    fi.version_id = version_id;
                    fi.versioned = src_opts.versioned || src_opts.version_suspended;

                    if !fi.inline_data() {
                        fi.data = None;
                    }

                    if inline_data {
                        fi.set_inline_data();
                    }
                }
            }

            Self::write_unique_file_info(&online_disks, "", src_bucket, src_object, &metas, write_quorum)
                .await
                .map_err(|e| to_object_err(e.into(), vec![src_bucket, src_object]))?;
        } else {
            self.update_object_meta_with_opts(
                src_bucket,
                src_object,
                fi.clone(),
                &online_disks,
                &UpdateMetadataOpts {
                    replace_user_metadata: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| to_object_err(e.into(), vec![src_bucket, src_object]))?;
        }

        self.invalidate_get_object_metadata_cache(src_bucket, src_object).await;

        Ok(ObjectInfo::from_file_info(
            &fi,
            src_bucket,
            src_object,
            src_opts.versioned || src_opts.version_suspended,
        ))
    }
    #[tracing::instrument(skip(self))]
    async fn delete_object_version(&self, bucket: &str, object: &str, fi: &FileInfo, force_del_marker: bool) -> Result<()> {
        let transported = delete_file_info_with_replication_transport_metadata(fi);
        let fi = &transported;
        let disks = self.disk_inventory().await;
        let namespace_owner = (!is_meta_bucketname(bucket)).then(|| self.ctx.begin_namespace_commit());
        let write_quorum = disks.len() / 2 + 1;
        let rollback_dir = Uuid::new_v4();

        let mut futures = Vec::with_capacity(disks.len());
        let mut errs = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let disk_namespace_owner = namespace_owner.clone().map(|owner| owner as Arc<dyn Send + Sync>);
            futures.push(async move {
                if let Some(disk) = disk {
                    match disk
                        .delete_version_with_namespace_owner(
                            bucket,
                            object,
                            fi.clone(),
                            force_del_marker,
                            DeleteOptions {
                                old_data_dir: Some(rollback_dir),
                                ..Default::default()
                            },
                            disk_namespace_owner,
                        )
                        .await
                    {
                        Ok(r) => Ok(r),
                        Err(e) => Err(e),
                    }
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(_) => {
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e));
                }
            }
        }

        let quorum_result = resolve_tiered_decommission_write_quorum_result(&errs, write_quorum, bucket, object);
        let should_rollback = quorum_result.is_err();
        let mut rollback_futures = Vec::new();
        for (index, err) in errs.iter().enumerate() {
            // backlog#1158: when rolling back, fan the idempotent undo out to every
            // online disk (each self-decides from its staged backup: restore if the
            // rollback dir is present, no-op otherwise). This covers a disk that
            // staged + applied the delete and *then* errored, which the plain
            // `err.is_some()` skip would leave deleted while its peers were restored.
            // On success only the successful disks' backup dirs need cleaning; errored
            // disks' residue is reclaimed by heal/scanner.
            if !should_rollback && err.is_some() {
                continue;
            }

            let Some(disk) = disks[index].as_ref() else {
                continue;
            };

            let disk = disk.clone();
            let bucket = bucket.to_string();
            let object = object.to_string();
            let fi = fi.clone();
            let disk_namespace_owner = namespace_owner.clone().map(|owner| owner as Arc<dyn Send + Sync>);
            rollback_futures.push(async move {
                if should_rollback {
                    if let Err(err) = disk
                        .delete_version_with_namespace_owner(
                            &bucket,
                            &object,
                            fi,
                            force_del_marker,
                            DeleteOptions {
                                undo_write: true,
                                undo_delete: true,
                                old_data_dir: Some(rollback_dir),
                                ..Default::default()
                            },
                            disk_namespace_owner,
                        )
                        .await
                    {
                        warn!(
                            bucket = %bucket,
                            object = %object,
                            rollback_dir = %rollback_dir,
                            error = ?err,
                            "failed to roll back delete after write quorum failure"
                        );
                    }
                } else {
                    let rollback_path = format!("{object}/{rollback_dir}");
                    if let Err(err) = disk
                        .delete_with_namespace_owner(
                            &bucket,
                            &rollback_path,
                            DeleteOptions {
                                recursive: true,
                                immediate: true,
                                ..Default::default()
                            },
                            disk_namespace_owner,
                        )
                        .await
                        && err != DiskError::FileNotFound
                        && err != DiskError::VolumeNotFound
                    {
                        warn!(
                            bucket = %bucket,
                            object = %object,
                            rollback_dir = %rollback_dir,
                            error = ?err,
                            "failed to clean delete rollback state after quorum success"
                        );
                    }
                }
            });
        }

        join_all(rollback_futures).await;
        drop(namespace_owner);
        quorum_result
    }

    #[tracing::instrument(skip(self, objects, opts))]
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        let (deleted, errors, _) = self.delete_objects_with_accounting(bucket, objects, opts).await;
        (deleted, errors)
    }

    async fn delete_objects_with_accounting(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>, Vec<Option<DeleteAccounting>>) {
        let mut del_objects = vec![DeletedObject::default(); objects.len()];
        let mut accounting = vec![None; objects.len()];
        let delete_config_snapshot = opts
            .delete_replication_config_snapshot
            .clone()
            .unwrap_or_else(|| Arc::new(DeleteReplicationConfigSnapshot::default()));

        for object in &objects {
            self.invalidate_get_object_metadata_cache(bucket, &object.object_name).await;
        }

        let mut del_errs = (0..objects.len()).map(|_| None).collect::<Vec<_>>();

        // Acquire locks in batch mode (best effort, matching previous behavior)
        let mut batch = rustfs_lock::BatchLockRequest::new(self.locker_owner.as_str()).with_all_or_nothing(false);
        let mut unique_objects: HashSet<String> = HashSet::new();
        for dobj in &objects {
            if unique_objects.insert(dobj.object_name.clone()) {
                batch = batch.add_write_lock(ObjectKey::new(bucket, dobj.object_name.clone()));
            }
        }
        let unique_lock_count = batch.requests.len();

        let mut failed_map = HashMap::new();
        let mut _local_batch_guards: Vec<FastLockGuard> = Vec::with_capacity(batch.requests.len());
        let mut locked_objects = HashSet::new();

        // Instance-scoped, not the ambient facade (backlog#1052) — see
        // new_ns_lock. The same applies to the versioning and bucket-metadata
        // lookups below: resolving them through the first published
        // instance's context would let a second in-process store delete with
        // the wrong versioning semantics or skip the object-lock gate.
        let dist_erasure = self.ctx.is_dist_erasure().await;
        let mut dist_batch_lock_ids = vec![Vec::new(); self.lockers.len()];

        if opts.no_lock {
            locked_objects = unique_objects;
        } else if dist_erasure {
            (failed_map, locked_objects, dist_batch_lock_ids) = self.acquire_dist_delete_object_locks_batch(&batch).await;
        } else {
            let batch_result = self.local_lock_manager.acquire_locks_batch(batch).await;
            _local_batch_guards = batch_result.guards;

            for key in batch_result.successful_locks {
                locked_objects.insert(key.object.as_ref().to_string());
            }

            for (key, err) in batch_result.failed_locks {
                failed_map.insert((key.bucket.as_ref().to_string(), key.object.as_ref().to_string()), format!("{err:?}"));
            }
        }

        if issue3031_diag_enabled() {
            let failed_lock_count = failed_map.len();
            let locked_object_count = locked_objects.len();
            let dist_lock_id_count = dist_batch_lock_ids.iter().map(Vec::len).sum::<usize>();
            warn!(
                target: "rustfs_ecstore::set_disk",
                bucket = %bucket,
                requested_object_count = objects.len(),
                unique_lock_count,
                locked_object_count,
                failed_lock_count,
                dist_erasure,
                dist_lock_id_count,
                failed_objects = ?failed_map.keys().collect::<Vec<_>>(),
                "issue3031_delete_objects_lock_batch_context"
            );
        }

        // Mark failures for objects that could not be locked
        for (i, dobj) in objects.iter().enumerate() {
            if let Some(err) = failed_map.get(&(bucket.to_string(), dobj.object_name.clone())) {
                del_errs[i] = Some(Error::other(err.to_string()));
            }
        }

        let object_lock_config = if is_meta_bucketname(bucket) {
            Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))
        } else {
            match opts.object_lock_config_snapshot.clone() {
                Some(snapshot) => snapshot,
                None => match metadata_sys::get_object_lock_config_state_in(&self.ctx, bucket).await {
                    Ok(state) => Arc::new(ObjectLockConfigSnapshot::new(state)),
                    Err(err) => {
                        let message = err.to_string();
                        for (index, item) in del_errs.iter_mut().enumerate() {
                            if locked_objects.contains(&objects[index].object_name) {
                                *item = Some(Error::other(message.clone()));
                            }
                        }
                        return (del_objects, del_errs, accounting);
                    }
                },
            }
        };
        let mut vers_map: HashMap<&String, FileInfoVersions> = HashMap::new();
        let mut tier_reference_leases: Vec<(usize, String, Option<TierDestinationId>)> = Vec::new();
        let mut tier_free_version_receipt_candidates: HashMap<usize, TierFreeVersionReceiptCandidate> = HashMap::new();

        for (i, dobj) in objects.iter().enumerate() {
            if del_errs[i].is_some() {
                continue;
            }

            let replication_object_name = decode_dir_object(&dobj.object_name);
            let explicit_null_version = is_explicit_null_version(dobj.version_id);
            let version_id = delete_file_info_version_id(dobj.version_id);
            let (versioned, version_suspended) = delete_config_snapshot
                .versioning_config()
                .delete_state(replication_object_name.as_str());
            let check_opts = ObjectOptions {
                version_id: dobj.version_id.map(|version_id| version_id.to_string()),
                versioned,
                version_suspended,
                object_lock_delete: opts.object_lock_delete.clone(),
                object_lock_config_snapshot: Some(Arc::clone(&object_lock_config)),
                no_lock: true,
                ..Default::default()
            };
            // A missing bucket Object Lock configuration does not prove that
            // persisted object metadata is clean: migrated or corrupt data may
            // still carry an explicit retention or legal hold. Only a real
            // delete-marker creation can skip the object-level WORM check.
            let object_lock_check_required = !is_meta_bucketname(bucket) && !set_disk_delete_creates_delete_marker(&check_opts);
            let replicate_delete = delete_config_snapshot.has_active_rule(&replication_object_name);
            let marker_delete = dobj.version_id.is_none() || dobj.synthetic_version_id;
            let replication_needs_source = replicate_delete
                && (!marker_delete || delete_config_snapshot.active_delete_marker_rules_require_tags(&replication_object_name));
            let (goi, gerr) = if object_lock_check_required
                || replication_needs_source
                || opts.tier_delete_journal_api.is_some()
                || dobj.expected_identity.is_some()
            {
                let (goi, _write_quorum, gerr) = self.get_object_info_and_quorum(bucket, &dobj.object_name, &check_opts).await;
                (goi, gerr)
            } else {
                (ObjectInfo::default(), None)
            };
            let source_missing = gerr
                .as_ref()
                .is_some_and(|err| is_err_object_not_found(err) || is_err_version_not_found(err));
            // A pool-local miss keeps the existing idempotent batch-delete
            // semantics. Exact-version deletes fan out to every pool, and a
            // retry may also arrive after the intended generation is gone.
            // Only a concrete generation with a different identity is stale.
            if let Some(expected) = dobj.expected_identity
                && (gerr.is_none() || matches!(gerr.as_ref(), Some(StorageError::MethodNotAllowed)))
                && (goi.data_dir != expected.data_dir
                    || goi.mod_time != expected.mod_time
                    || goi.delete_marker != expected.delete_marker)
            {
                del_errs[i] = Some(StorageError::PreconditionFailed);
                continue;
            }
            // Resolve accounting from the generation selected under this
            // object's write lock. A request-layer pre-stat is only an
            // optimization and cannot identify a concurrent overwrite.
            let (accounting_size, accounting_version_id, removed_current_object) = if source_missing
                || dobj.synthetic_version_id
                || set_disk_delete_creates_delete_marker(&check_opts)
                || goi.delete_marker
            {
                (None, None, false)
            } else {
                (
                    quota_object_size(&goi).ok(),
                    goi.version_id.filter(|version_id| !version_id.is_nil()),
                    (dobj.version_id.is_none() || is_explicit_null_version(dobj.version_id)) && !dobj.synthetic_version_id,
                )
            };
            // Normalize both sides before comparing. `goi.version_id` is the
            // client-facing identity, where `from_file_info` synthesizes
            // `Some(Uuid::nil())` for a null version on a versioned or
            // versioning-suspended bucket; `version_id` is the storage identity,
            // where an explicit `?versionId=null` maps to `None`. Comparing them
            // raw makes an explicit null-version delete of a null delete marker
            // look like a version mismatch, so the `MethodNotAllowed` from the
            // lookup below is recorded as a delete failure and the marker is
            // never purged — the bucket then stays non-empty on disk forever.
            let explicit_delete_marker = dobj.version_id.is_some()
                && goi.delete_marker
                && delete_file_info_version_id(goi.version_id) == version_id
                && matches!(gerr.as_ref(), Some(StorageError::MethodNotAllowed));
            if let Some(err) = gerr.as_ref()
                && !source_missing
                && !explicit_delete_marker
            {
                del_errs[i] = Some(err.clone());
                continue;
            }
            if object_lock_check_required
                && !source_missing
                && let Err(err) = check_object_lock_delete(&self.ctx, bucket, &dobj.object_name, &goi, &check_opts).await
            {
                del_errs[i] = Some(err);
                continue;
            }

            if opts.tier_delete_journal_api.is_some()
                && !source_missing
                && goi.transitioned_object.status == TRANSITION_COMPLETE
                && !set_disk_delete_creates_delete_marker(&check_opts)
            {
                match tier_destination_id_from_metadata(&goi.user_defined) {
                    Ok(identity) => {
                        tier_reference_leases.push((i, goi.transitioned_object.tier.clone(), identity));
                    }
                    Err(err) => {
                        del_errs[i] = Some(Error::other(err));
                        continue;
                    }
                }
            }

            let mut admitted = dobj.clone();
            admitted.object_name = replication_object_name;
            if admitted.synthetic_version_id {
                admitted.version_id = None;
            }
            if replicate_delete {
                let dsc = ReplicationObjectBridge::check_delete_with_snapshot(
                    &admitted,
                    &goi,
                    &check_opts,
                    source_missing,
                    &delete_config_snapshot,
                );
                if dsc.replicate_any() {
                    if admitted.version_id.is_some() {
                        admitted.version_purge_status = Some(version_purge_status_to_filemeta(VersionPurgeStatusType::Pending));
                        admitted.version_purge_statuses = dsc.pending_status();
                    } else {
                        admitted.delete_marker_replication_status = dsc.pending_status();
                    }
                    admitted.replicate_decision_str = Some(dsc.to_string());
                }
            }
            let mut vr = FileInfo {
                name: dobj.object_name.clone(),
                version_id,
                idx: i,
                replication_state_internal: Some(admitted.replication_state()),
                ..Default::default()
            };

            let tier_free_version_id = Uuid::new_v4();
            vr.set_tier_free_version_id(&tier_free_version_id.to_string());

            // Delete
            // del_objects[i].object_name.clone_from(&vr.name);
            // del_objects[i].version_id = vr.version_id.map(|v| v.to_string());

            if dobj.version_id.is_none() && (version_suspended || versioned) {
                vr.mod_time = Some(OffsetDateTime::now_utc());
                vr.deleted = true;
                vr.mark_deleted = true;
                if versioned {
                    vr.version_id = Some(Uuid::new_v4());
                }
            }

            // Same normalization as `explicit_delete_marker` above: `goi.version_id`
            // is the client-facing identity (`Some(Uuid::nil())` for a null
            // version) while `version_id` is the storage identity (`None` for an
            // explicit null). Comparing them raw made a null delete marker's
            // removal take the non-marker branch below, so the response lost
            // `DeleteMarker`/`DeleteMarkerVersionId` and the removal was
            // accounted as an object deletion (issue #6745).
            let removed_delete_marker =
                goi.delete_marker && dobj.version_id.is_some() && delete_file_info_version_id(goi.version_id) == version_id;
            // Response-side only for the null identity: a delete request marked
            // `deleted` with `version_id == None` makes `FileMeta::delete_version`
            // re-create the marker it just removed (the suspended-bucket
            // "delete mints a marker" write path), so the write shape must stay
            // untouched for explicit null-marker removals.
            if removed_delete_marker && version_id.is_some() {
                vr.deleted = true;
                vr.mod_time = goi.mod_time;
            }

            let v = {
                if vers_map.contains_key(&dobj.object_name) {
                    let val = vers_map.get_mut(&dobj.object_name).unwrap();
                    val.versions.push(vr.clone());
                    val.clone()
                } else {
                    FileInfoVersions {
                        name: vr.name.clone(),
                        versions: vec![vr.clone()],
                        ..Default::default()
                    }
                }
            };

            if vr.deleted || removed_delete_marker {
                del_objects[i] = DeletedObject {
                    delete_marker: true,
                    // `vr.version_id` holds the storage identity, which is
                    // `None` for an explicit null-marker removal; report the
                    // client-facing null identity so the response can carry
                    // `DeleteMarkerVersionId` for the marker that was removed.
                    delete_marker_version_id: if explicit_null_version {
                        Some(Uuid::nil())
                    } else {
                        vr.version_id
                    },
                    // For a null-marker removal `vr` stays undeleted (write
                    // shape), so take the marker's mtime from the source.
                    delete_marker_mtime: vr.mod_time.or(goi.mod_time),
                    object_name: vr.name.clone(),
                    replication_state: vr.replication_state_internal.clone(),
                    source_missing,
                    ..Default::default()
                }
            } else {
                del_objects[i] = DeletedObject {
                    object_name: vr.name.clone(),
                    version_id: if explicit_null_version {
                        Some(Uuid::nil())
                    } else {
                        vr.version_id
                    },
                    replication_state: vr.replication_state_internal.clone(),
                    source_missing,
                    ..Default::default()
                };
                accounting[i] = Some(DeleteAccounting {
                    size: accounting_size,
                    version_id: accounting_version_id,
                    removed_current_object,
                });
            }

            if opts.tier_free_version_receipt_sink.is_some()
                && !dobj.synthetic_version_id
                && transitioned_delete_publishes_free_version(&goi, &vr, opts.skip_free_version)
            {
                tier_free_version_receipt_candidates.insert(
                    i,
                    TierFreeVersionReceiptCandidate {
                        source: goi,
                        free_version_id: tier_free_version_id,
                    },
                );
            }

            // Only add to vers_map if we hold the lock
            if locked_objects.contains(&dobj.object_name) {
                vers_map.insert(&dobj.object_name, v);
            }
        }

        let mut vers = Vec::with_capacity(vers_map.len());

        for (_, mut fi_vers) in vers_map {
            fi_vers.versions.sort_by_key(|a| a.deleted);

            if let Some(index) = fi_vers.versions.iter().position(|fi| fi.deleted) {
                fi_vers.versions.truncate(index + 1);
            }

            vers.push(fi_vers);
        }

        let rollback_dir = Uuid::new_v4();

        let disks = self.disks.read().await;

        let disks = disks.clone();

        if opts
            .namespace_lock_fence
            .as_ref()
            .is_some_and(NamespaceLockFence::is_lock_lost)
        {
            if dist_erasure {
                self.release_dist_delete_object_locks_batch(dist_batch_lock_ids).await;
            }
            for (index, object) in objects.iter().enumerate() {
                if del_errs[index].is_none() {
                    del_errs[index] = Some(Error::NamespaceLockQuorumUnavailable {
                        mode: "delete_objects_commit",
                        bucket: bucket.to_string(),
                        object: decode_dir_object(&object.object_name),
                        required: 1,
                        achieved: 0,
                    });
                }
            }
            return (del_objects, del_errs, accounting);
        }

        let mut tier_leases: Vec<TierOperationLease> = Vec::new();
        if let Some(api) = opts.tier_delete_journal_api.as_ref() {
            let mut reference_groups: HashMap<(String, Option<TierDestinationId>), Vec<usize>> = HashMap::new();
            for (idx, tier_name, backend_identity) in tier_reference_leases {
                reference_groups.entry((tier_name, backend_identity)).or_default().push(idx);
            }
            for ((tier_name, backend_identity), indices) in reference_groups {
                let lease = match backend_identity {
                    Some(backend_identity) => {
                        TierConfigMgr::acquire_operation_lease_for_backend_identity(
                            &api.tier_config_mgr(),
                            &tier_name,
                            backend_identity,
                        )
                        .await
                    }
                    None => TierConfigMgr::acquire_operation_lease(&api.tier_config_mgr(), &tier_name).await,
                };
                match lease {
                    Ok(lease) => tier_leases.push(lease),
                    Err(err) => {
                        let message = err.to_string();
                        for idx in indices {
                            del_errs[idx] = Some(Error::other(message.clone()));
                        }
                    }
                }
            }
        }

        // Keep backend generations pinned through the source mutation, its
        // free-version write quorum, and any local rollback. Ordinary
        // single/batch deletes never transfer cleanup ownership to a journal.
        let _tier_leases = tier_leases;

        for fi_vers in &mut vers {
            fi_vers.versions.retain(|fi| del_errs[fi.idx].is_none());
        }
        vers.retain(|fi_vers| !fi_vers.versions.is_empty());
        let mut futures = Vec::with_capacity(disks.len());
        let lock_lost_during_commit = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let vers = vers.clone();
            let namespace_lock_fence = opts.namespace_lock_fence.clone();
            let lock_lost_during_commit = Arc::clone(&lock_lost_during_commit);
            futures.push(async move {
                if namespace_lock_fence.as_ref().is_some_and(NamespaceLockFence::is_lock_lost) {
                    lock_lost_during_commit.store(true, std::sync::atomic::Ordering::Release);
                    return (0..vers.len()).map(|_| Some(DiskError::DiskOngoingReq)).collect();
                }
                if let Some(disk) = disk {
                    disk.delete_versions(
                        bucket,
                        vers,
                        DeleteOptions {
                            old_data_dir: Some(rollback_dir),
                            ..Default::default()
                        },
                    )
                    .await
                } else {
                    let mut errs = Vec::with_capacity(vers.len());
                    for _ in 0..vers.len() {
                        errs.push(Some(DiskError::DiskNotFound));
                    }
                    errs
                }
            });
        }

        let results = join_all(futures).await;

        let mut del_obj_errs: Vec<Vec<Option<DiskError>>> = vec![vec![None; objects.len()]; disks.len()];

        // For each disk delete all objects
        for (disk_idx, errors) in results.into_iter().enumerate() {
            // Deletion results for all objects
            for idx in 0..vers.len() {
                if errors[idx].is_some() {
                    for fi in vers[idx].versions.iter() {
                        del_obj_errs[disk_idx][fi.idx] = errors[idx].clone();
                    }
                }
            }
        }

        for obj_idx in 0..objects.len() {
            let mut disk_err = vec![None; disks.len()];

            for disk_idx in 0..disks.len() {
                if del_obj_errs[disk_idx][obj_idx].is_some() {
                    disk_err[disk_idx] = del_obj_errs[disk_idx][obj_idx].clone();
                }
            }

            let mut has_err = reduce_write_quorum_errs(&disk_err, OBJECT_OP_IGNORED_ERRS, disks.len() / 2 + 1);
            if let Some(err) = has_err.clone() {
                let er = err.into();
                if (is_err_object_not_found(&er) || is_err_version_not_found(&er)) && !del_objects[obj_idx].delete_marker {
                    has_err = None;
                }
            } else {
                del_objects[obj_idx].found = true;
            }

            if let Some(err) = has_err {
                if del_objects[obj_idx].version_id.is_some() {
                    del_errs[obj_idx] = Some(to_object_err(
                        err.into(),
                        vec![
                            bucket,
                            &objects[obj_idx].object_name.clone(),
                            &objects[obj_idx].version_id.unwrap_or_default().to_string(),
                        ],
                    ));
                } else {
                    del_errs[obj_idx] = Some(to_object_err(err.into(), vec![bucket, &objects[obj_idx].object_name.clone()]));
                }
            }
        }

        if lock_lost_during_commit.load(std::sync::atomic::Ordering::Acquire) {
            for (index, object) in objects.iter().enumerate() {
                if del_errs[index].is_none() {
                    del_errs[index] = Some(Error::NamespaceLockQuorumUnavailable {
                        mode: "delete_objects_commit",
                        bucket: bucket.to_string(),
                        object: decode_dir_object(&object.object_name),
                        required: 1,
                        achieved: 0,
                    });
                }
            }
        }

        self.record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);

        let mut rollback_futures = Vec::new();
        let committed_receipt_indices =
            committed_tier_free_version_receipt_indices(&vers, &del_errs, &tier_free_version_receipt_candidates);
        for fi_vers in &vers {
            // delete_versions commits one xl.meta per object group, so rollback must use the same boundary.
            let should_rollback = fi_vers.versions.iter().any(|fi| del_errs[fi.idx].is_some());
            for (disk_idx, disk) in disks.iter().enumerate() {
                // backlog#1158: on rollback, include every online disk so a disk that
                // staged + applied the delete and then errored is still restored (the
                // disk-side undo is idempotent, no-op when nothing was staged). On
                // success, skip the errored disks and only clean up successful ones.
                if !should_rollback && fi_vers.versions.iter().any(|fi| del_obj_errs[disk_idx][fi.idx].is_some()) {
                    continue;
                }

                let Some(disk) = disk.as_ref() else {
                    continue;
                };

                let disk = disk.clone();
                let bucket = bucket.to_string();
                let object = fi_vers.name.clone();
                let versions = fi_vers.clone();
                rollback_futures.push(async move {
                    if should_rollback {
                        let errs = disk
                            .delete_versions(
                                &bucket,
                                vec![versions],
                                DeleteOptions {
                                    undo_write: true,
                                    undo_delete: true,
                                    old_data_dir: Some(rollback_dir),
                                    ..Default::default()
                                },
                            )
                            .await;
                        if let Some(err) = errs.into_iter().flatten().next() {
                            warn!(
                                bucket = %bucket,
                                object = %object,
                                rollback_dir = %rollback_dir,
                                error = ?err,
                                "failed to roll back batch delete after write quorum failure"
                            );
                        }
                    } else {
                        let rollback_path = format!("{object}/{rollback_dir}");
                        if let Err(err) = disk
                            .delete(
                                &bucket,
                                &rollback_path,
                                DeleteOptions {
                                    recursive: true,
                                    immediate: true,
                                    ..Default::default()
                                },
                            )
                            .await
                            && err != DiskError::FileNotFound
                            && err != DiskError::VolumeNotFound
                        {
                            warn!(
                                bucket = %bucket,
                                object = %object,
                                rollback_dir = %rollback_dir,
                                error = ?err,
                                "failed to clean batch delete rollback state after quorum success"
                            );
                        }
                    }
                });
            }
        }

        join_all(rollback_futures).await;

        for idx in committed_receipt_indices {
            let Some(candidate) = tier_free_version_receipt_candidates.remove(&idx) else {
                continue;
            };
            record_committed_tier_free_version_receipt(
                &opts,
                bucket,
                &decode_dir_object(&objects[idx].object_name),
                &candidate.source,
                candidate.free_version_id,
                true,
            );
        }

        // TODO(backlog): support partial object deletion for multi-part objects

        if dist_erasure {
            self.release_dist_delete_object_locks_batch(dist_batch_lock_ids).await;
        }

        for (object, err) in objects.iter().zip(del_errs.iter()) {
            if err.is_none() {
                self.invalidate_get_object_metadata_cache(bucket, &object.object_name).await;
            }
        }

        // An accounting identity is actionable only when the delete result is
        // successful. Never let a failed commit (including a partial quorum
        // failure) reach the request-layer fast delta path.
        for (index, err) in del_errs.iter().enumerate() {
            if err.is_some() {
                accounting[index] = None;
            }
        }

        (del_objects, del_errs, accounting)
    }

    #[tracing::instrument(skip(self, opts))]
    async fn delete_object(&self, bucket: &str, object: &str, mut opts: ObjectOptions) -> Result<ObjectInfo> {
        let _scope_outcome_guard = opts
            .scanner_publication_commit_scope
            .clone()
            .map(ScannerPublicationCommitScopeGuard::new);
        let scanner_publication_commit_scope = opts.scanner_publication_commit_scope.clone();
        // Scanner cleanup carries the per-peer lease fence as transient
        // request metadata. Consume it before any delete-prefix fanout so it
        // cannot be persisted or treated as user metadata.
        let scanner_publication_lease_tokens = take_scanner_publication_lease_tokens(&mut opts.user_defined)?;
        let preserve_delete_replication_state = should_preserve_delete_replication_state(&opts);
        let delete_config_snapshot = if opts.delete_prefix || opts.transition.expire_restored || preserve_delete_replication_state
        {
            None
        } else if let Some(snapshot) = opts.delete_replication_config_snapshot.clone() {
            Some(snapshot)
        } else {
            Some(Arc::new(DeleteReplicationConfigSnapshot::default()))
        };

        self.invalidate_get_object_metadata_cache(bucket, object).await;

        // Guard lock for single object delete
        let _lock_guard = if (!opts.delete_prefix || opts.delete_prefix_object) && !opts.no_lock {
            Some(self.acquire_write_lock_diag("delete_object", bucket, object).await?)
        } else {
            None
        };
        if opts.delete_prefix {
            let destructive_dispatch_phase = opts.lifecycle_delete_all.as_ref().is_none_or(|request| {
                matches!(
                    request.phase,
                    crate::object_api::LifecycleDeleteAllPhase::History | crate::object_api::LifecycleDeleteAllPhase::Trigger
                )
            });
            if destructive_dispatch_phase && opts.tier_delete_journal_api.is_some() {
                let authorization = opts
                    .tier_delete_dispatch_authorization
                    .as_ref()
                    .ok_or(StorageError::PreconditionFailed)?;
                let incarnation = opts.expected_bucket_incarnation_id.ok_or(StorageError::PreconditionFailed)?;
                authorization.ensure_current(bucket, incarnation, object)?;
                if !authorization.mutation_started() {
                    return Err(StorageError::PreconditionFailed);
                }
            }
            if opts.delete_prefix_object && !is_meta_bucketname(bucket) {
                let object_lock_config = if opts.data_movement {
                    None
                } else {
                    Some(match opts.object_lock_config_snapshot.as_deref() {
                        Some(snapshot) => snapshot.state().clone(),
                        None => metadata_sys::get_object_lock_config_state_in(&self.ctx, bucket).await?,
                    })
                };
                if let Some(versions) = self.load_file_info_versions_exact(bucket, object).await? {
                    let bypass_governance = opts
                        .object_lock_delete
                        .as_ref()
                        .is_some_and(|delete_opts| delete_opts.bypass_governance);
                    if let Some(object_lock_config) = object_lock_config.as_ref() {
                        for version in versions
                            .versions
                            .iter()
                            .chain(versions.free_versions.iter().filter(|_| opts.lifecycle_delete_all.is_none()))
                        {
                            let object_info = ObjectInfo::from_file_info(version, bucket, object, true);
                            if check_object_lock_for_deletion_with_state(object_lock_config, &object_info, bypass_governance)?
                                .is_some()
                            {
                                return Err(StorageError::PrefixAccessDenied(bucket.to_string(), object.to_string()));
                            }
                        }
                    }
                    if let Some(trigger) = opts.lifecycle_delete_all.as_ref() {
                        let plan = lifecycle_delete_all_plan(&versions, trigger)?;
                        if trigger.phase == crate::object_api::LifecycleDeleteAllPhase::Preflight {
                            return Ok(ObjectInfo::default());
                        }
                        if trigger.phase == crate::object_api::LifecycleDeleteAllPhase::FinalPreflight {
                            return Ok(plan
                                .trigger_only()?
                                .map(|version| ObjectInfo::from_file_info(version, bucket, object, true))
                                .unwrap_or_default());
                        }
                        let plan = match trigger.phase {
                            crate::object_api::LifecycleDeleteAllPhase::History => plan.history,
                            crate::object_api::LifecycleDeleteAllPhase::Trigger => plan.trigger_only()?.into_iter().collect(),
                            crate::object_api::LifecycleDeleteAllPhase::Preflight
                            | crate::object_api::LifecycleDeleteAllPhase::FinalPreflight => {
                                return Err(StorageError::PreconditionFailed);
                            }
                        };
                        for version in plan {
                            ensure_delete_commit_locks_held(_lock_guard.as_ref(), bucket, object, &opts)?;
                            // The lease closes the gap between the durable
                            // reference proof and publishing either a committed
                            // v6 journal or the legacy free-version fallback.
                            let _tier_lease = acquire_lifecycle_delete_all_tier_lease(version, &opts).await?;
                            let replication_delete = lifecycle_delete_all_replication_delete(bucket, object, version, &opts)?;
                            let mut delete_request = FileInfo {
                                name: object.to_string(),
                                version_id: version.version_id,
                                replication_state_internal: replication_delete
                                    .as_ref()
                                    .map(|(state, _)| replication_state_to_filemeta(state)),
                                ..Default::default()
                            };
                            delete_request.set_tier_free_version_id(&Uuid::new_v4().to_string());
                            if version.transition_status == rustfs_filemeta::TRANSITION_COMPLETE
                                && let Some(entry) = lifecycle_delete_all_tier_journal_entry(bucket, object, version, &opts)?
                            {
                                let authorization = opts
                                    .tier_delete_dispatch_authorization
                                    .as_ref()
                                    .ok_or(StorageError::PreconditionFailed)?;
                                let incarnation = opts.expected_bucket_incarnation_id.ok_or(StorageError::PreconditionFailed)?;
                                authorization.ensure_current(bucket, incarnation, object)?;
                                authorization.authorized_journal_name(&entry)?;
                                delete_request.set_skip_tier_free_version();
                            }
                            begin_scanner_publication_delete_mutation(scanner_publication_commit_scope.as_ref())?;
                            self.delete_object_version(bucket, object, &delete_request, false).await?;
                            if let Some((_, deleted_object)) = replication_delete {
                                ReplicationLifecycleBridge::schedule_delete(bucket.to_string(), deleted_object).await;
                            }
                        }
                    } else {
                        for version in &versions.versions {
                            ensure_delete_commit_locks_held(_lock_guard.as_ref(), bucket, object, &opts)?;
                            let mut delete_request = FileInfo {
                                name: object.to_string(),
                                version_id: version.version_id,
                                ..Default::default()
                            };
                            delete_request.set_tier_free_version_id(&Uuid::new_v4().to_string());
                            begin_scanner_publication_delete_mutation(scanner_publication_commit_scope.as_ref())?;
                            self.delete_object_version(bucket, object, &delete_request, false).await?;
                        }
                        for version in &versions.free_versions {
                            ensure_delete_commit_locks_held(_lock_guard.as_ref(), bucket, object, &opts)?;
                            let mut delete_request = FileInfo {
                                name: object.to_string(),
                                version_id: version.version_id,
                                deleted: true,
                                ..Default::default()
                            };
                            delete_request.set_tier_free_version();
                            begin_scanner_publication_delete_mutation(scanner_publication_commit_scope.as_ref())?;
                            self.delete_object_version(bucket, object, &delete_request, false).await?;
                        }
                    }
                }
                if let Some(scope) = scanner_publication_commit_scope.as_ref() {
                    let _ = scope.mark_committed();
                }
                self.invalidate_get_object_metadata_cache(bucket, object).await;
                return Ok(ObjectInfo::default());
            }
            if let Some(expected_incarnation_id) = opts.expected_bucket_incarnation_id {
                self.validate_bucket_incarnation(bucket, expected_incarnation_id).await?;
            }
            ensure_delete_commit_locks_held(_lock_guard.as_ref(), bucket, object, &opts)?;
            begin_scanner_publication_delete_mutation(scanner_publication_commit_scope.as_ref())?;
            self.delete_prefix_with_scanner_publication_lease(
                bucket,
                object,
                scanner_publication_lease_tokens.as_ref(),
                scanner_publication_commit_scope.clone(),
            )
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;

            if let Some(scope) = scanner_publication_commit_scope.as_ref() {
                let _ = scope.mark_committed();
            }
            self.invalidate_all_get_object_metadata_cache();
            return Ok(ObjectInfo::default());
        }

        if let Some(expected) = opts.expected_current_version_id.as_deref() {
            let current = self
                .get_object_info(
                    bucket,
                    object,
                    &ObjectOptions {
                        no_lock: true,
                        metadata_cache_safe: false,
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|err| {
                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                        StorageError::PreconditionFailed
                    } else {
                        err
                    }
                })?;
            if !current.delete_marker
                || current.version_id.map(|version| version.to_string()).as_deref() != Some(expected)
                || opts.version_id.as_deref() != Some(expected)
            {
                return Err(StorageError::PreconditionFailed);
            }
        }

        // TODO(backlog): integrate lifecycle evaluation before object deletion

        let mut version_found = true;
        // delete_object_version below derives its own majority quorum from the
        // disk array, so the object-derived quorum here is unused.
        let (mut goi, _write_quorum, gerr) = self.get_object_info_and_quorum(bucket, object, &opts).await;
        if let Some(err) = &gerr
            && goi.name.is_empty()
        {
            if should_force_delete_marker_for_missing_version(&opts) {
                version_found = false;
            } else {
                return Err(err.clone());
            }
        }

        if version_found {
            opts.precondition_check(&goi)?;
            check_object_lock_delete(&self.ctx, bucket, object, &goi, &opts).await?;
        }

        if opts.transition.expire_restored {
            // Restore-expiry (DeleteRestoredAction / DeleteRestoredVersionAction)
            // must only drop the local restored copy and strip the x-amz-restore
            // headers; the version itself stays transitioned (status=complete)
            // and keeps serving GETs from the tier. Route it before delete-marker
            // resolution and replication dispatch: a delete marker would hide the
            // version, a replicated delete would remove it on the target, and a
            // free-version record would schedule remote tier cleanup.
            if !version_found {
                return Err(gerr.unwrap_or_else(|| StorageError::ObjectNotFound(bucket.to_string(), object.to_string())));
            }
            let dfi = FileInfo {
                name: object.to_string(),
                version_id: goi.version_id,
                mod_time: Some(opts.mod_time.unwrap_or_else(OffsetDateTime::now_utc)),
                expire_restored: true,
                ..Default::default()
            };
            ensure_delete_commit_locks_held(_lock_guard.as_ref(), bucket, object, &opts)?;
            begin_scanner_publication_delete_mutation(scanner_publication_commit_scope.as_ref())?;
            self.delete_object_version(bucket, object, &dfi, false)
                .await
                .map_err(|e| to_object_err(e, vec![bucket, object]))?;
            self.invalidate_get_object_metadata_cache(bucket, object).await;
            if let Some(scope) = scanner_publication_commit_scope.as_ref() {
                let _ = scope.mark_committed();
            }
            return Ok(ObjectInfo::from_file_info(&dfi, bucket, object, opts.versioned || opts.version_suspended));
        }

        let otd = ObjectToDelete {
            object_name: decode_dir_object(object),
            version_id: if opts.synthetic_version_id {
                None
            } else {
                opts.version_id.as_deref().map(Uuid::parse_str).transpose()?
            },
            synthetic_version_id: opts.synthetic_version_id,
            ..Default::default()
        };

        let dsc = if let Some(snapshot) = delete_config_snapshot {
            ReplicationObjectBridge::check_delete_with_snapshot(&otd, &goi, &opts, gerr.is_some(), &snapshot)
        } else {
            ReplicateDecision::default()
        };

        if dsc.replicate_any() {
            opts.set_delete_replication_state(dsc);
            goi.replication_decision = opts
                .delete_replication
                .as_ref()
                .map(|v| v.replicate_decision_str.clone())
                .unwrap_or_default();
        }

        let (mark_delete, mut delete_marker) = resolve_delete_version_state(&opts, &goi, version_found);

        let mod_time = if let Some(mt) = opts.mod_time {
            mt
        } else {
            OffsetDateTime::now_utc()
        };

        let find_vid = Uuid::new_v4();
        #[cfg(test)]
        pause_delete_object_commit(bucket, object).await;

        if mark_delete && (opts.versioned || opts.version_suspended) {
            if !delete_marker {
                delete_marker = opts.version_suspended && opts.version_id.is_none();
            }

            let mut fi = FileInfo {
                name: object.to_string(),
                deleted: delete_marker,
                mark_deleted: mark_delete,
                mod_time: Some(mod_time),
                replication_state_internal: opts.delete_replication.as_ref().map(replication_state_to_filemeta),
                ..Default::default() // TODO(backlog): populate transition state on delete markers
            };

            fi.set_tier_free_version_id(&find_vid.to_string());

            fi.version_id = if let Some(vid) = opts.version_id.as_ref() {
                let vid = Uuid::parse_str(vid.as_str())?;
                (!opts.version_suspended || !vid.is_nil()).then_some(vid)
            } else if opts.version_suspended {
                None
            } else if opts.versioned {
                Some(Uuid::new_v4())
            } else {
                None
            };

            ensure_delete_commit_locks_held(_lock_guard.as_ref(), bucket, object, &opts)?;
            begin_scanner_publication_delete_mutation(scanner_publication_commit_scope.as_ref())?;
            let _tier_delete_lease = acquire_single_tier_delete_lease(&opts, &goi).await?;
            if opts.skip_free_version {
                fi.set_skip_tier_free_version();
            }
            self.delete_object_version(bucket, object, &fi, should_force_delete_marker_for_missing_version(&opts))
                .await
                .map_err(|e| to_object_err(e, vec![bucket, object]))?;
            #[cfg(test)]
            pause_delete_object_commit_after_publish(bucket, object).await;

            if opts.tier_free_version_receipt_sink.is_some()
                && transitioned_delete_publishes_free_version(&goi, &fi, opts.skip_free_version)
            {
                record_committed_tier_free_version_receipt(&opts, bucket, object, &goi, find_vid, false);
            }

            let disks = self.disk_inventory().await;
            self.record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);

            let mut oi = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);
            oi.user_tags = Arc::clone(&goi.user_tags);
            oi.replication_decision = goi.replication_decision;
            self.invalidate_get_object_metadata_cache(bucket, object).await;
            if let Some(scope) = scanner_publication_commit_scope.as_ref() {
                let _ = scope.mark_committed();
            }
            return Ok(oi);
        }

        // Create a single object deletion request
        let mut dfi = FileInfo {
            name: object.to_string(),
            version_id: opts
                .version_id
                .as_ref()
                .and_then(|v| Uuid::parse_str(v).ok())
                .filter(|vid| !opts.version_suspended || !vid.is_nil()),
            mark_deleted: mark_delete,
            deleted: delete_marker,
            mod_time: Some(mod_time),
            replication_state_internal: opts.delete_replication.as_ref().map(replication_state_to_filemeta),
            ..Default::default()
        };

        dfi.set_tier_free_version_id(&find_vid.to_string());

        ensure_delete_commit_locks_held(_lock_guard.as_ref(), bucket, object, &opts)?;
        begin_scanner_publication_delete_mutation(scanner_publication_commit_scope.as_ref())?;
        let _tier_delete_lease = acquire_single_tier_delete_lease(&opts, &goi).await?;
        if opts.skip_free_version {
            dfi.set_skip_tier_free_version();
        }
        self.delete_object_version(bucket, object, &dfi, opts.delete_marker)
            .await
            .map_err(|e| to_object_err(e, vec![bucket, object]))?;
        #[cfg(test)]
        pause_delete_object_commit_after_publish(bucket, object).await;

        if opts.tier_free_version_receipt_sink.is_some()
            && transitioned_delete_publishes_free_version(&goi, &dfi, opts.skip_free_version)
        {
            record_committed_tier_free_version_receipt(&opts, bucket, object, &goi, find_vid, false);
        }

        let disks = self.disk_inventory().await;
        self.record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);

        let mut obj_info = ObjectInfo::from_file_info(&dfi, bucket, object, opts.versioned || opts.version_suspended);
        obj_info.size = goi.size;
        // Keep the committed source metadata on the internal delete result so
        // the request layer can derive canonical accounting for this exact
        // generation. Delete responses do not expose these fields.
        obj_info.actual_size = goi.actual_size;
        obj_info.user_defined = Arc::clone(&goi.user_defined);
        obj_info.parts = Arc::clone(&goi.parts);
        obj_info.user_tags = Arc::clone(&goi.user_tags);
        // Report delete-marker semantics for an explicit-version delete whose
        // target was a delete marker. On versioning-suspended buckets
        // `resolve_delete_version_state` cannot mark the write request itself
        // (a `deleted` write with the null identity re-creates the marker), so
        // the marker-ness is restored on the response here (issue #6745).
        if explicit_delete_removed_marker(&opts, &goi, version_found) {
            obj_info.delete_marker = true;
        }
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        if let Some(scope) = scanner_publication_commit_scope.as_ref() {
            let _ = scope.mark_committed();
        }
        Ok(obj_info)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        crate::hp_guard!("SetDisks::get_object_info");
        // Acquire a shared read-lock to protect consistency during info fetch
        let _read_lock_guard = if !opts.no_lock {
            Some(self.acquire_read_lock_diag("get_object_info", bucket, object).await?)
        } else {
            None
        };

        // Use the same full xl.meta read path as GetObject metadata resolution.
        // This avoids HEAD/GetObject metadata visibility skew immediately after
        // PutObject/CompleteMultipartUpload.
        let snapshot = self
            .get_object_fileinfo(bucket, object, opts, true, false)
            .await
            .map_err(|e| to_object_err(e, vec![bucket, object]))?;

        let oi = ObjectInfo::from_file_info(snapshot.fi(), bucket, object, opts.versioned || opts.version_suspended);

        Ok(oi)
    }

    #[tracing::instrument(skip(self))]
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()> {
        // MRF journal intent: partial-write recovery must survive a restart
        // (HS-01); the heal request below remains the in-memory fast path.
        let version_uuid = if version_id.is_empty() {
            Some(None)
        } else {
            uuid::Uuid::try_parse(version_id).ok().map(Some)
        };
        if let Some(version_uuid) = version_uuid
            && let (Ok(pool_index), Ok(set_index)) = (u32::try_from(self.pool_index), u32::try_from(self.set_index))
        {
            let scope = rustfs_common::mrf_channel::MrfScope { pool_index, set_index };
            let _ = rustfs_common::mrf_channel::try_send_mrf_intent_typed(
                rustfs_common::mrf_channel::MrfKind::PartialWrite,
                bucket,
                object,
                version_uuid,
                Some(scope),
            );
        }
        let mut request = rustfs_heal_contracts::heal_channel::create_heal_request_with_options(
            bucket.to_string(),
            Some(object.to_string()),
            false,
            Some(HealChannelPriority::Normal),
            Some(self.pool_index),
            Some(self.set_index),
        );
        request.object_version_id = (!version_id.is_empty()).then(|| version_id.to_string());
        if let Err(e) = rustfs_heal_contracts::heal_channel::send_heal_request(request).await {
            warn!(
                bucket,
                object,
                version_id,
                error = %e,
                "Failed to enqueue heal request for partial object"
            );
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn put_object_metadata(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let replication_validate_only = opts
            .replication_status_writeback
            .as_ref()
            .is_some_and(|condition| condition.mode == ReplicationStatusWritebackMode::ValidateOnly);
        if !replication_validate_only {
            self.invalidate_get_object_metadata_cache(bucket, object).await;
        }

        // Guard lock for metadata update
        #[cfg(any(test, feature = "test-util"))]
        pause_put_object_commit(bucket, object, PutObjectCommitPause::BeforeMetadata).await;
        let _lock_guard = if !opts.no_lock || opts.replication_status_writeback.is_some() {
            Some(self.acquire_write_lock_diag("put_object_metadata", bucket, object).await?)
        } else {
            None
        };

        let disks = self.get_disks_internal().await;

        let (metas, errs) = {
            if let Some(version_id) = &opts.version_id {
                Self::read_all_fileinfo(&disks, "", bucket, object, version_id.to_string().as_str(), false, false, false).await?
            } else {
                Self::read_all_xl(&disks, bucket, object, false, false).await
            }
        };

        let (read_quorum, write_quorum) = match Self::object_quorum_from_meta(&metas, &errs, self.default_parity_count) {
            Ok((read_quorum, write_quorum)) => (read_quorum, write_quorum),
            Err(mut err) => {
                // A replication terminal CAS is a validation/write-back
                // attempt, never a heal operation. In particular,
                // ValidateOnly must remain zero-write, and a worker whose
                // outer replication lease is already lost must not delete the
                // surviving xl.meta copies before the namespace fence below
                // can reject it. Leave dangling cleanup to heal/scanner.
                if opts.replication_status_writeback.is_some() {
                    return Err(to_object_err(err.into(), vec![bucket, object]));
                }
                if err == DiskError::ErasureReadQuorum
                    && !bucket.starts_with(RUSTFS_META_BUCKET)
                    && self
                        .delete_if_dangling(bucket, object, &metas, &errs, &HashMap::new(), opts.clone())
                        .await
                        .is_ok()
                {
                    if opts.version_id.is_some() {
                        err = DiskError::FileVersionNotFound
                    } else {
                        err = DiskError::FileNotFound
                    }
                }
                return Err(to_object_err(err.into(), vec![bucket, object]));
            }
        };

        let read_quorum =
            usize::try_from(read_quorum).map_err(|_| to_object_err(DiskError::ErasureReadQuorum.into(), vec![bucket, object]))?;
        let write_quorum = usize::try_from(write_quorum)
            .map_err(|_| to_object_err(DiskError::ErasureWriteQuorum.into(), vec![bucket, object]))?;

        let version_id = opts.version_id.as_deref().unwrap_or_default();
        let (online_disks, mut fi, _) = Self::select_valid_fileinfo(&disks, &metas, &errs, version_id, read_quorum, write_quorum)
            .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;

        if fi.deleted {
            return Err(to_object_err(Error::MethodNotAllowed, vec![bucket, object]));
        }

        let obj_info = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);

        if opts
            .replication_status_writeback
            .as_ref()
            .is_some_and(|condition| !replication_status_writeback_is_current(&obj_info, condition))
        {
            return Err(Error::PreconditionFailed);
        }

        check_object_lock_retention_update(bucket, object, &obj_info, opts)?;

        if let Some(expected_incarnation_id) = opts.expected_bucket_incarnation_id {
            self.validate_bucket_incarnation(bucket, expected_incarnation_id).await?;
        }
        if _lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
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
                mode: "put_object_metadata_commit",
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }

        if replication_validate_only {
            return Ok(obj_info);
        }

        for (k, v) in obj_info.user_defined.iter() {
            fi.metadata.insert(k.clone(), v.clone());
        }

        if let Some(mt) = &opts.eval_metadata
            && merge_evaluated_metadata(&mut fi.metadata, mt)?
        {
            rebuild_file_info_replication_state(&mut fi);
        }

        fi.acknowledge_data_movement();

        if opts.mod_time.is_some() {
            fi.mod_time = opts.mod_time;
        }
        if let Some(ref version_id) = opts.version_id {
            fi.version_id = Uuid::parse_str(version_id).ok();
        }

        self.update_object_meta(bucket, object, fi.clone(), &online_disks)
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;

        self.invalidate_get_object_metadata_cache(bucket, object).await;

        Ok(ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended))
    }

    #[tracing::instrument(skip(self))]
    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        let oi = self.get_object_info(bucket, object, opts).await?;
        Ok((*oi.user_tags).clone())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let tier_config_mgr = self.ctx.tier_config_mgr();
        let tgt_client = match TierConfigMgr::acquire_operation_lease(&tier_config_mgr, &opts.transition.tier).await {
            Ok(client) => client,
            Err(err) => {
                return Err(Error::other(format!("remote tier error: {err}")));
            }
        };

        // Acquire write-lock early; hold for the whole transition operation scope
        // if !opts.no_lock {
        //     let guard_opt = self
        //         .namespace_lock
        //         .lock_guard(object, &self.locker_owner, Duration::from_secs(5), Duration::from_secs(10))
        //         .await?;
        //     if guard_opt.is_none() {
        //         return Err(Error::other("can not get lock. please retry".to_string()));
        //     }
        //     _lock_guard = guard_opt;
        // }

        let mut transition_read_opts = opts.clone();
        transition_read_opts.include_part_checksums = true;
        let (mut fi, meta_arr, online_disks) = self
            .get_object_fileinfo(bucket, object, &transition_read_opts, true, false)
            .await?
            .into_owned();
        /*if err != nil {
            return Err(to_object_err(err, vec![bucket, object]));
        }*/
        /*if fi.deleted {
            if opts.version_id.is_none() {
                return Err(to_object_err(DiskError::FileNotFound, vec![bucket, object]));
            }
            return Err(to_object_err(ERR_METHOD_NOT_ALLOWED, vec![bucket, object]));
        }*/
        // Normalize ETags by removing quotes before comparison (PR #592 compatibility)
        let transition_etag = rustfs_utils::path::trim_etag(&opts.transition.etag);
        let stored_etag = rustfs_utils::path::trim_etag(&get_raw_etag(&fi.metadata));
        if let Some(mod_time1) = opts.mod_time {
            if let Some(mod_time2) = fi.mod_time.as_ref() {
                if mod_time1.unix_timestamp() != mod_time2.unix_timestamp()
                    || (!transition_etag.is_empty() && transition_etag != stored_etag)
                {
                    return Err(to_object_err(Error::other(DiskError::FileNotFound), vec![bucket, object]));
                }
            } else {
                return Err(Error::other("mod_time 2 error.".to_string()));
            }
        } else {
            return Err(Error::other("mod_time 1 error.".to_string()));
        }
        if fi.transition_status == TRANSITION_COMPLETE {
            return Ok(());
        }

        /*if fi.xlv1 {
            if let Err(err) = self.heal_object(bucket, object, "", &HealOpts {no_lock: true, ..Default::default()}) {
                return err.expect("err");
            }
            (fi, meta_arr, online_disks) = self.get_object_fileinfo(&bucket, &object, &opts, true, false);
            if err != nil {
                return to_object_err(err, vec![bucket, object]);
            }
        }*/

        let oi = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);
        let transaction_api = transition_object_store(&self.ctx).await;
        let transition_compaction_fleet_proof =
            crate::services::notification_sys::acquire_transition_transaction_compaction_fleet_proof();
        let compact_transition_transaction = transition_compaction_fleet_proof
            .as_ref()
            .is_some_and(crate::services::notification_sys::transition_transaction_compaction_fleet_proof_matches);
        let transaction_init = TransitionTransactionInit {
            deployment_id: transition_deployment_id(&self.ctx)?,
            transaction_id: Uuid::new_v4(),
            owner_epoch: Uuid::new_v4(),
            write_id: Uuid::new_v4(),
            source: transition_source_identity(bucket, object, &fi, opts, &stored_etag)?,
            tier_name: opts.transition.tier.clone(),
            backend_fingerprint: tgt_client.backend_identity(),
            not_after_unix_nanos: transition_transaction_not_after_unix_nanos()?,
        };
        let mut transaction = if compact_transition_transaction {
            TransitionTransaction::new_compact(transaction_init)
        } else {
            TransitionTransaction::new(transaction_init)
        }
        .map_err(Error::other)?;
        save_transition_transaction_if_available(transaction_api.as_ref(), &transaction).await?;
        #[cfg(all(test, feature = "test-util"))]
        pause_transition_transaction_at(bucket, object, TransitionTransactionKillPoint::PrePutFence).await;
        let transaction_id = transaction.transaction_id;
        let dest_obj = transaction.remote_object.clone();
        let mut transition_meta = (*oi.user_defined).clone();
        rustfs_utils::http::remove_str(&mut transition_meta, rustfs_utils::http::SUFFIX_PART_CHECKSUMS);
        transition_meta.insert("name".to_string(), object.to_string());
        rustfs_utils::http::metadata_compat::insert_str(
            &mut transition_meta,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TRANSACTION_ID,
            transaction.transaction_id.to_string(),
        );
        rustfs_utils::http::metadata_compat::insert_str(
            &mut transition_meta,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(transaction.backend_fingerprint),
        );

        if let Some(content_type) = oi.content_type.as_ref().filter(|value| !value.is_empty()) {
            transition_meta.insert(CONTENT_TYPE.to_ascii_lowercase(), content_type.clone());
        }

        for header in [
            CONTENT_ENCODING,
            CONTENT_LANGUAGE,
            CONTENT_DISPOSITION,
            CACHE_CONTROL,
            EXPIRES,
            X_AMZ_OBJECT_LOCK_MODE.as_str(),
            X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str(),
            X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str(),
        ] {
            if let Some(value) = fi.metadata.lookup(header).filter(|value| !value.is_empty()) {
                transition_meta.insert(header.to_ascii_lowercase(), value.to_string());
            }
        }

        let expected_size = u64::try_from(fi.size).map_err(|_| StorageError::FileCorrupt)?;
        let (pr, pw) = tokio::io::duplex(fi.erasure.block_size);
        let consumed = Arc::new(AtomicU64::new(0));
        let reader = ReaderImpl::ObjectBody(ObjectReader::new(TransitionUploadReader::new(pr, Arc::clone(&consumed))));

        let cloned_bucket = bucket.to_string();
        let cloned_object = object.to_string();
        let cloned_fi = fi.clone();
        let set_index = self.set_index;
        let pool_index = self.pool_index;
        let skip_verify = opts.skip_verify_bitrot;
        let metrics_size_bucket = rustfs_io_metrics::get_object_size_bucket(cloned_fi.size);
        let erasure_cache = Arc::clone(&self.erasure_cache);
        let producer = async move {
            let mut writer = TransitionUploadWriter::new(pw);
            Self::get_object_with_fileinfo(
                &cloned_bucket,
                &cloned_object,
                erasure_cache,
                0,
                cloned_fi.size,
                &mut writer,
                cloned_fi,
                meta_arr,
                &online_disks,
                set_index,
                pool_index,
                skip_verify,
                false,
                false,
                GET_OBJECT_PATH_LEGACY_DUPLEX,
                GET_CODEC_STREAMING_OBJECT_CLASS_PLAIN_SINGLE_PART,
                metrics_size_bucket,
            )
            .await?;
            writer.shutdown().await?;
            Ok(writer.produced())
        };

        let mut upload_cleanup = TransitionUploadCleanup::new(tgt_client, &dest_obj);
        upload_cleanup.set_cleanup_owner(transaction_api.clone(), &transaction);
        if !compact_transition_transaction {
            advance_and_save_transition_transaction(
                transaction_api.as_ref(),
                &mut transaction,
                TransitionTransactionState::UploadOutcomeUnknown,
                None,
            )
            .await?;
            upload_cleanup.update_cleanup_transaction(&transaction);
        }
        let remote_upload = {
            let lease = &upload_cleanup.lease;
            let recorded_candidate = &mut upload_cleanup.candidate;
            let remote_object = &dest_obj;
            async move {
                let remote_version = lease.put_with_meta(remote_object, reader, fi.size, transition_meta).await?;
                *recorded_candidate = Some(TransitionUploadCandidate::from_put_response(remote_version.clone()));
                #[cfg(all(test, feature = "test-util"))]
                pause_after_transition_upload_candidate_recorded().await;
                Ok(remote_version)
            }
        };
        let rv = complete_transition_upload(remote_upload, producer, expected_size, consumed).await;
        let candidate = match rv {
            Ok(completion) => completion.candidate,
            Err(failure) => {
                if failure.candidate.is_some() {
                    let cleanup_api = transition_cleanup_store(&self.ctx).await;
                    if let Err(cleanup_err) = upload_cleanup.cleanup_rejected_upload(cleanup_api, &mut transaction).await {
                        return Err(StorageError::Io(std::io::Error::other(format!(
                            "{}; rejected remote upload cleanup failed: {cleanup_err}",
                            failure.error
                        ))));
                    }
                    delete_transition_transaction_after_remote_cleanup(transaction_api.as_ref(), &transaction, bucket, object)
                        .await;
                }
                return Err(failure.error);
            }
        };

        if let Err(err) = upload_cleanup.lease.validate_remote_version_id(candidate.remote_version()) {
            let cleanup_api = transition_cleanup_store(&self.ctx).await;
            if let Err(cleanup_err) = upload_cleanup.cleanup_rejected_upload(cleanup_api, &mut transaction).await {
                return Err(StorageError::Io(std::io::Error::other(format!(
                    "{err}; rejected remote upload cleanup failed: {cleanup_err}"
                ))));
            }
            delete_transition_transaction_after_remote_cleanup(transaction_api.as_ref(), &transaction, bucket, object).await;
            return Err(err.into());
        }
        let fleet_proof = remote_version_state_writer_fleet_proof();
        let remote_version_requires_fleet_proof =
            !candidate.remote_version().is_empty() && Uuid::parse_str(candidate.remote_version()).is_err();
        let (transition_version_id, transition_version_state) =
            match persisted_transition_version_with_gate(candidate.remote_version(), fleet_proof.is_some()) {
                Ok(version) => version,
                Err(err) => {
                    let cleanup_api = transition_cleanup_store(&self.ctx).await;
                    if let Err(cleanup_err) = upload_cleanup.cleanup_rejected_upload(cleanup_api, &mut transaction).await {
                        return Err(StorageError::Io(std::io::Error::other(format!(
                            "{err}; rejected remote upload cleanup failed: {cleanup_err}"
                        ))));
                    }
                    delete_transition_transaction_after_remote_cleanup(transaction_api.as_ref(), &transaction, bucket, object)
                        .await;
                    return Err(err.into());
                }
            };
        if !compact_transition_transaction {
            if let Err(err) = advance_and_save_transition_transaction(
                transaction_api.as_ref(),
                &mut transaction,
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::known_from_put_response(candidate.remote_version().to_string())),
            )
            .await
            {
                let cleanup_api = transition_cleanup_store(&self.ctx).await;
                if let Err(cleanup_err) = upload_cleanup.cleanup_rejected_upload(cleanup_api, &mut transaction).await {
                    return Err(StorageError::Io(std::io::Error::other(format!(
                        "{err}; uploaded transition transaction persist failed and cleanup failed: {cleanup_err}"
                    ))));
                }
                delete_transition_transaction_after_remote_cleanup(transaction_api.as_ref(), &transaction, bucket, object).await;
                return Err(err);
            }
            upload_cleanup.update_cleanup_transaction(&transaction);
        }

        #[cfg(all(test, feature = "test-util"))]
        pause_after_transition_uploaded_persisted(bucket, object).await;
        #[cfg(all(test, feature = "test-util"))]
        pause_transition_transaction_at(bucket, object, TransitionTransactionKillPoint::UploadBeforeCommitFence).await;

        let commit_opts = opts.as_commit_opts();
        // Note: Using clone() here is necessary because ObjectOptions has 124 fields.
        // Future optimization: Consider using Cow<ObjectOptions> or a builder pattern.
        let transition_lock_guard = if opts.no_lock {
            None
        } else {
            match self.acquire_write_lock_diag("transition_object_commit", bucket, object).await {
                Ok(guard) => Some(guard),
                Err(err) => {
                    if upload_cleanup.cleanup().await.is_ok() {
                        delete_transition_transaction_after_remote_cleanup(
                            transaction_api.as_ref(),
                            &transaction,
                            bucket,
                            object,
                        )
                        .await;
                    }
                    return Err(err);
                }
            }
        };
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        let current = self.get_object_fileinfo(bucket, object, &commit_opts, true, false).await;
        let current = match current {
            Ok(current) => current,
            Err(err) => {
                drop(transition_lock_guard);
                if upload_cleanup.cleanup().await.is_ok() {
                    delete_transition_transaction_after_remote_cleanup(transaction_api.as_ref(), &transaction, bucket, object)
                        .await;
                }
                return Err(err);
            }
        };
        let (mut current_fi, _, _) = current.into_owned();
        let source_matches = current_fi.version_id == fi.version_id
            && current_fi.data_dir == fi.data_dir
            && current_fi.mod_time == fi.mod_time
            && current_fi.size == fi.size
            && rustfs_utils::path::trim_etag(&get_raw_etag(&current_fi.metadata)) == stored_etag;
        if current_fi.transition_status == TRANSITION_COMPLETE || !source_matches {
            let already_transitioned = current_fi.transition_status == TRANSITION_COMPLETE;
            drop(transition_lock_guard);
            if upload_cleanup.cleanup().await.is_ok() {
                delete_transition_transaction_after_remote_cleanup(transaction_api.as_ref(), &transaction, bucket, object).await;
            }
            if already_transitioned {
                return Ok(());
            }
            return Err(to_object_err(Error::other(DiskError::FileNotFound), vec![bucket, object]));
        }

        current_fi.transition_status = TRANSITION_COMPLETE.to_string();
        current_fi.transitioned_objname = dest_obj;
        current_fi.transition_tier = opts.transition.tier.clone();
        current_fi.transition_version_id = transition_version_id
            .as_deref()
            .and_then(|version_id| Uuid::parse_str(version_id).ok());
        current_fi.transition_version = transition_version_id;
        current_fi.transition_version_state = transition_version_state;
        rustfs_utils::http::metadata_compat::insert_str(
            &mut current_fi.metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(upload_cleanup.lease.backend_identity()),
        );
        fi = current_fi;
        let event_name = EventName::LifecycleTransition.as_str();

        #[cfg(test)]
        pause_transition_commit(bucket, object, TransitionCommitPause::BeforeLockLost).await;
        if transition_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            drop(transition_lock_guard);
            if upload_cleanup.cleanup().await.is_ok() {
                delete_transition_transaction_after_remote_cleanup(transaction_api.as_ref(), &transaction, bucket, object).await;
            }
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "transition_object_commit",
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }
        #[cfg(test)]
        pause_transition_commit(bucket, object, TransitionCommitPause::BeforeLeaseValidation).await;
        if !upload_cleanup.lease.is_current_generation() {
            drop(transition_lock_guard);
            if upload_cleanup.cleanup().await.is_ok() {
                delete_transition_transaction_after_remote_cleanup(transaction_api.as_ref(), &transaction, bucket, object).await;
            }
            return Err(Error::other("remote tier configuration changed during transition"));
        }
        #[cfg(test)]
        pause_transition_commit(bucket, object, TransitionCommitPause::AfterLeaseValidation).await;
        // This check is the fleet-proof lease linearization point. Revocation
        // blocks later commits; an already-authorized local quorum commit is
        // allowed to finish without holding a synchronous lock across I/O.
        if remote_version_requires_fleet_proof
            && !fleet_proof
                .as_ref()
                .is_some_and(remote_version_state_writer_fleet_proof_matches)
        {
            drop(transition_lock_guard);
            if upload_cleanup.cleanup().await.is_ok() {
                delete_transition_transaction_after_remote_cleanup(transaction_api.as_ref(), &transaction, bucket, object).await;
            }
            return Err(Error::other("remote version state fleet capability changed during transition"));
        }
        if compact_transition_transaction
            && !transition_compaction_fleet_proof
                .as_ref()
                .is_some_and(crate::services::notification_sys::transition_transaction_compaction_fleet_proof_matches)
        {
            drop(transition_lock_guard);
            if upload_cleanup.cleanup().await.is_ok() {
                delete_transition_transaction_after_remote_cleanup(transaction_api.as_ref(), &transaction, bucket, object).await;
            }
            return Err(Error::other(
                "transition transaction compaction fleet capability changed during transition",
            ));
        }
        if let Err(err) = advance_and_save_transition_transaction(
            transaction_api.as_ref(),
            &mut transaction,
            TransitionTransactionState::LocalCommitStarted,
            compact_transition_transaction
                .then(|| TransitionRemoteVersion::known_from_put_response(candidate.remote_version().to_string())),
        )
        .await
        {
            drop(transition_lock_guard);
            if upload_cleanup.cleanup().await.is_ok() {
                delete_transition_transaction_after_remote_cleanup(transaction_api.as_ref(), &transaction, bucket, object).await;
            }
            return Err(err);
        }
        upload_cleanup.update_cleanup_transaction(&transaction);
        #[cfg(all(test, feature = "test-util"))]
        pause_transition_transaction_at(bucket, object, TransitionTransactionKillPoint::CommitFenceBeforeLocalCommit).await;
        upload_cleanup.disarm();
        if let Err(err) = self.delete_object_version(bucket, object, &fi, false).await {
            warn!(
                bucket = bucket,
                object = object,
                error = ?err,
                "transition remote upload completed but local commit failed"
            );
            self.invalidate_get_object_metadata_cache(bucket, object).await;
            drop(transition_lock_guard);
            return Err(err);
        }
        #[cfg(all(test, feature = "test-util"))]
        pause_transition_transaction_at(bucket, object, TransitionTransactionKillPoint::LocalCommitBeforeDelete).await;
        if compact_transition_transaction {
            if let Err(err) = delete_transition_transaction_if_available(transaction_api.as_ref(), &transaction).await {
                warn!(
                    bucket = bucket,
                    object = object,
                    transaction_id = %transaction_id,
                    error = ?err,
                    "transition committed locally but compact transaction cleanup failed"
                );
            }
        } else {
            match advance_and_save_transition_transaction(
                transaction_api.as_ref(),
                &mut transaction,
                TransitionTransactionState::Committed,
                None,
            )
            .await
            {
                Ok(()) => {
                    if let Err(err) = delete_transition_transaction_if_available(transaction_api.as_ref(), &transaction).await {
                        warn!(
                            bucket = bucket,
                            object = object,
                            transaction_id = %transaction_id,
                            error = ?err,
                            "transition committed locally but transaction cleanup failed"
                        );
                    }
                }
                Err(err) => {
                    warn!(
                        bucket = bucket,
                        object = object,
                        transaction_id = %transaction_id,
                        error = ?err,
                        "transition committed locally but transaction committed-state advance failed"
                    );
                }
            }
        }

        // delete_object_version persisted transition_status=complete and freed the
        // local data, but does not touch the GET metadata cache. Drop any cached
        // pre-transition entry so a late duplicate transition task (or a plain GET)
        // re-reads the fresh state; a stale hit here defeats the TRANSITION_COMPLETE
        // early-return above and streams the already-deleted local data to the
        // remote tier again (rustfs/rustfs#4827).
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        drop(transition_lock_guard);
        let disks = self.disk_inventory().await;
        self.record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);

        for disk in disks.iter() {
            if disk.is_some() {
                continue;
            }
            let _ = self
                .add_partial(bucket, object, opts.version_id.as_deref().unwrap_or_default())
                .await;
            break;
        }

        let obj_info = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);
        send_event(EventArgs {
            event_name: event_name.to_string(),
            bucket_name: bucket.to_string(),
            object: obj_info,
            user_agent: "Internal: [ILM-Transition]".to_string(),
            host: runtime_sources::default_local_node_name(),
            ..Default::default()
        });
        //let tags = opts.lifecycle_audit_event.tags();
        //auditLogLifecycle(ctx, objInfo, ILMTransition, tags, traceFn)
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn restore_transitioned_object(self: Arc<Self>, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        // Acquire write-lock early for the restore operation
        // if !opts.no_lock {
        //     let guard_opt = self
        //         .namespace_lock
        //         .lock_guard(object, &self.locker_owner, Duration::from_secs(5), Duration::from_secs(10))
        //         .await?;
        //     if guard_opt.is_none() {
        //         return Err(Error::other("can not get lock. please retry".to_string()));
        //     }
        //     _lock_guard = guard_opt;
        // }
        let self_ = self.clone();
        let bucket_lifecycle_guard = if let Some(expected_incarnation_id) = opts.expected_bucket_incarnation_id
            && opts.bucket_lifecycle_lock_fence.is_none()
        {
            let guard = metadata_sys::object_store_in(&self.ctx)
                .await?
                .acquire_bucket_lifecycle_read_lock(bucket)
                .await?;
            self.validate_bucket_incarnation(bucket, expected_incarnation_id).await?;
            Some(guard)
        } else {
            None
        };
        if opts
            .bucket_lifecycle_lock_fence
            .as_ref()
            .is_some_and(NamespaceLockFence::is_lock_lost)
        {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "restore_object_bucket_generation",
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }
        let mut restore_read_opts = opts.clone();
        restore_read_opts.include_part_checksums = true;
        let actual = self
            .clone()
            .get_object_fileinfo(bucket, object, &restore_read_opts, true, false)
            .await
            .map_err(|err| to_object_err(err, vec![bucket, object]))?;
        drop(bucket_lifecycle_guard);
        let actual_fi = actual.fi();

        let oi = ObjectInfo::from_file_info(actual_fi, bucket, object, opts.versioned || opts.version_suspended);
        let restore_result: Result<()> = async {
            // The tier reader releases its own lease at EOF, before PUT or
            // CompleteMultipartUpload necessarily reaches metadata quorum. Keep a
            // second exact-generation lease for the whole restore so the final
            // same-key write lock and this lease together fence remote-tuple
            // publication through commit.
            let expected_backend_identity = tier_destination_id_from_metadata(&oi.user_defined).map_err(Error::other)?;
            let tier_config_mgr = self.ctx.tier_config_mgr();
            let _remote_tuple_publication_lease =
                acquire_restore_tier_lease(&tier_config_mgr, &oi.transitioned_object.tier, expected_backend_identity).await?;
            let expected_operation_id = restore_operation_id_from_metadata(&opts.user_defined)?;
            if let Some(expected_operation_id) = expected_operation_id {
                require_restore_operation_id(oi.user_defined.as_ref(), expected_operation_id)?;
            }
            let mut ropts = put_restore_opts(bucket, object, &opts.transition.restore_request, &oi).await?;
            if let Some(expected_operation_id) = expected_operation_id {
                rustfs_utils::http::metadata_compat::insert_str(
                    &mut ropts.user_defined,
                    SUFFIX_RESTORE_OPERATION_ID,
                    expected_operation_id.to_string(),
                );
            }
            let mut restore_commit_metadata = if let Some(expected_operation_id) = expected_operation_id {
                let mut metadata = HashMap::new();
                metadata.insert(X_AMZ_RESTORE.as_str().to_string(), "ongoing-request=\"false\"".to_string());
                rustfs_utils::http::metadata_compat::insert_str(
                    &mut metadata,
                    SUFFIX_RESTORE_OPERATION_ID,
                    expected_operation_id.to_string(),
                );
                metadata
            } else {
                HashMap::new()
            };
            if let Some(part_checksums) =
                rustfs_utils::http::get_consistent_str(&actual_fi.metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS)
            {
                rustfs_utils::http::insert_str(
                    &mut restore_commit_metadata,
                    rustfs_utils::http::SUFFIX_PART_CHECKSUMS,
                    part_checksums.to_string(),
                );
            }
            // Keep the public ECStore capacity admission attached to each local
            // commit. The tier reads below must remain outside the object write
            // lock so HEAD/GET do not wait for a slow remote copy-back. Restore
            // does not hold an outer object write lock while copying bytes, so a
            // caller-supplied boolean is not transferable lock authority: PUT and
            // Complete must acquire their own commit-late write locks.
            ropts.no_lock = false;
            ropts.expected_bucket_incarnation_id = opts.expected_bucket_incarnation_id;
            ropts.bucket_lifecycle_lock_fence = opts.bucket_lifecycle_lock_fence.clone();
            ropts.namespace_lock_fence = opts.namespace_lock_fence.clone();
            ropts.object_lock_config_snapshot = opts.object_lock_config_snapshot.clone();
            ropts.decommission_capacity_admission = opts.decommission_capacity_admission.clone();
            if oi.parts.len() == 1 {
                let mut opts = opts.clone();
                opts.part_number = Some(1);
                let rs: Option<HTTPRangeSpec> = None;
                let gr = get_transitioned_object_reader_with_tier_manager(
                    bucket,
                    object,
                    &rs,
                    &HeaderMap::new(),
                    &oi,
                    &opts,
                    &self_.ctx.tier_config_mgr(),
                    self_.ctx.object_encryption_resolver(),
                )
                .await
                .map_err(|err| to_object_err(err.into(), vec![bucket, object]))?;
                let reader = BufReader::new(gr.stream);
                let hash_reader = HashReader::from_stream(reader, gr.object_info.size, oi.get_actual_size()?, None, None, false)?;
                let mut p_reader = PutObjReader::new(hash_reader);
                let restored_info = self_
                    .clone()
                    .put_object(bucket, object, &mut p_reader, &ropts)
                    .await
                    .map_err(|err| to_object_err(err, vec![bucket, object]))?;
                let restored_info = self_.finalize_restore_metadata(bucket, object, &restored_info, &opts).await?;
                send_event(EventArgs {
                    event_name: EventName::ObjectRestoreCompleted.as_str().to_string(),
                    bucket_name: bucket.to_string(),
                    object: restored_info,
                    user_agent: "Internal: [Restore-Completed]".to_string(),
                    host: runtime_sources::default_local_node_name(),
                    ..Default::default()
                });
                return Ok(());
            }

            let res = self_.clone().new_multipart_upload(bucket, object, &ropts).await?;
            #[cfg(test)]
            {
                *RESTORE_MULTIPART_UPLOAD_ID
                    .lock()
                    .expect("restore multipart upload-id lock must not be poisoned") = Some(res.upload_id.clone());
            }
            let mut upload_cleanup = RestoreMultipartUploadCleanup::new(self_.clone(), bucket, object, &res.upload_id);
            let restore_result: Result<ObjectInfo> = async {
                let mut uploaded_parts: Vec<CompletePart> = vec![];
                let parts = Arc::clone(&oi.parts);
                let mut part_offset: i64 = 0;
                for part_info in parts.iter() {
                    let mut part_opts = opts.clone();
                    part_opts.part_number = Some(part_info.number);
                    #[cfg(test)]
                    fail_restore_multipart_at(RestoreMultipartFailurePoint::InvalidPartSize)?;
                    if part_info.actual_size <= 0 {
                        return Err(Error::other(format!("invalid multipart restore part size {}", part_info.actual_size)));
                    }
                    #[cfg(test)]
                    fail_restore_multipart_at(RestoreMultipartFailurePoint::RangeOverflow)?;
                    let part_end = part_offset
                        .checked_add(part_info.actual_size - 1)
                        .ok_or_else(|| Error::other("multipart restore part range overflow".to_string()))?;
                    let rs = Some(HTTPRangeSpec {
                        is_suffix_length: false,
                        start: part_offset,
                        end: part_end,
                    });
                    part_offset = part_end
                        .checked_add(1)
                        .ok_or_else(|| Error::other("multipart restore part offset overflow".to_string()))?;
                    #[cfg(test)]
                    fail_restore_multipart_at(RestoreMultipartFailurePoint::TierGet)?;
                    let gr = get_transitioned_object_reader_with_tier_manager(
                        bucket,
                        object,
                        &rs,
                        &HeaderMap::new(),
                        &oi,
                        &part_opts,
                        &self_.ctx.tier_config_mgr(),
                        self_.ctx.object_encryption_resolver(),
                    )
                    .await
                    .map_err(StorageError::Io)?;
                    let reader = BufReader::new(gr.stream);
                    #[cfg(test)]
                    fail_restore_multipart_at(RestoreMultipartFailurePoint::HashReader)?;
                    let hash_reader =
                        HashReader::from_stream(reader, part_info.actual_size, part_info.actual_size, None, None, false)?;
                    let mut p_reader = PutObjReader::new(hash_reader);
                    #[cfg(test)]
                    fail_restore_multipart_at(RestoreMultipartFailurePoint::PutPart)?;
                    let p_info = self_
                        .clone()
                        .put_object_part(bucket, object, &res.upload_id, part_info.number, &mut p_reader, &ropts)
                        .await?;
                    #[cfg(test)]
                    let p_info = if restore_multipart_failure_is(RestoreMultipartFailurePoint::SizeMismatch) {
                        let mut injected = p_info;
                        injected.size = 0;
                        injected
                    } else {
                        p_info
                    };
                    if p_info.size as i64 != part_info.actual_size {
                        return Err(Error::other(ObjectApiError::InvalidObjectState(GenericError {
                            bucket: bucket.to_string(),
                            object: object.to_string(),
                            ..Default::default()
                        })));
                    }
                    uploaded_parts.push(CompletePart {
                        part_num: p_info.part_num,
                        etag: p_info.etag,
                        checksum_crc32: None,
                        checksum_crc32c: None,
                        checksum_sha1: None,
                        checksum_sha256: None,
                        checksum_crc64nvme: None,
                    });
                }
                #[cfg(test)]
                if restore_multipart_failure_is(RestoreMultipartFailurePoint::Complete) {
                    uploaded_parts
                        .first_mut()
                        .expect("multipart restore must contain at least one uploaded part")
                        .etag = Some("injected-invalid-complete-etag".to_string());
                }
                let complete_opts = ObjectOptions {
                    mod_time: oi.mod_time,
                    version_id: oi.version_id.map(|version| version.to_string()),
                    expected_bucket_incarnation_id: opts.expected_bucket_incarnation_id,
                    bucket_lifecycle_lock_fence: opts.bucket_lifecycle_lock_fence.clone(),
                    user_defined: restore_commit_metadata,
                    no_lock: false,
                    decommission_capacity_admission: opts.decommission_capacity_admission.clone(),
                    ..Default::default()
                };
                self_
                    .clone()
                    .complete_multipart_upload(bucket, object, &res.upload_id, uploaded_parts, &complete_opts)
                    .await
            }
            .await;
            let restored_info = match restore_result {
                Ok(info) => {
                    upload_cleanup.disarm();
                    info
                }
                Err(err) => {
                    upload_cleanup.abort().await;
                    return Err(err);
                }
            };
            let restored_info = self_.finalize_restore_metadata(bucket, object, &restored_info, opts).await?;
            send_event(EventArgs {
                event_name: EventName::ObjectRestoreCompleted.as_str().to_string(),
                bucket_name: bucket.to_string(),
                object: restored_info,
                user_agent: "Internal: [Restore-Completed]".to_string(),
                host: runtime_sources::default_local_node_name(),
                ..Default::default()
            });
            Ok(())
        }
        .await;
        if let Err(primary_error) = restore_result {
            if let Err(cleanup_error) = self_.update_restore_metadata(bucket, object, &oi, opts).await {
                warn!(
                    event = EVENT_LIFECYCLE_RESTORE_CLEANUP,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    state = "failed",
                    bucket,
                    object,
                    primary_error = %primary_error,
                    cleanup_error = %cleanup_error,
                    "restore metadata cleanup failed"
                );
            }
            return Err(primary_error);
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn put_object_tags(&self, bucket: &str, object: &str, tags: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.update_object_tags_locked("put_object_tags", bucket, object, tags, opts)
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.update_object_tags_locked("delete_object_tags", bucket, object, "", opts)
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let get_object_reader = <Self as crate::storage_api_contracts::object::ObjectIO>::get_object_reader(
            self,
            bucket,
            object,
            None,
            HeaderMap::new(),
            opts,
        )
        .await?;
        // Stream to sink to avoid loading entire object into memory during verification
        let mut reader = get_object_reader.stream;
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod erasure_construction_tests {
    use super::*;
    use crate::erasure::coding::ErasureConstructionError;
    use std::error::Error as _;

    #[test]
    fn object_file_info_mapping_preserves_construction_error() {
        let mut fi = FileInfo::new("object", 2, 2);
        fi.erasure.block_size = 0;

        let error = match erasure_from_file_info(&fi, false) {
            Ok(_) => panic!("invalid object erasure metadata must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("block_size must be greater than zero"));
        let io_source = error.source().expect("StorageError::Io must expose its io::Error source");
        let construction_source = io_source
            .source()
            .expect("io::Error must expose the erasure construction error");
        assert!(construction_source.is::<ErasureConstructionError>());
    }
}

#[cfg(test)]
mod mid_size_streaming_gate_tests {
    use super::*;
    use rustfs_filemeta::ObjectPartInfo;
    use serial_test::serial;

    fn plain_metadata(size: usize) -> (ObjectInfo, FileInfo) {
        let size_i64 = i64::try_from(size).expect("test size fits i64");
        let mut fi = FileInfo::new("object", 2, 2);
        fi.size = size_i64;
        fi.add_object_part(1, "etag".to_string(), size, fi.mod_time, size_i64, None, None);
        let object_info = ObjectInfo {
            size: size_i64,
            parts: Arc::new(vec![ObjectPartInfo {
                number: 1,
                size,
                actual_size: size_i64,
                ..Default::default()
            }]),
            ..Default::default()
        };
        (object_info, fi)
    }

    #[test]
    #[serial]
    fn direct_memory_boundary_stays_out_of_mid_size_streaming() {
        let (object_info, fi) = plain_metadata(128 * 1024);
        assert_eq!(
            get_mid_size_streaming_object_size_with_flags(&None, &object_info, &fi, &ObjectOptions::default(), true, true, true),
            None
        );
    }

    #[test]
    #[serial]
    fn mid_size_streaming_accepts_object_just_above_direct_memory_ceiling() {
        let (object_info, fi) = plain_metadata(128 * 1024 + 1);
        assert_eq!(
            get_mid_size_streaming_object_size_with_flags(&None, &object_info, &fi, &ObjectOptions::default(), true, true, true),
            Some(128 * 1024 + 1)
        );
    }

    #[test]
    #[serial]
    fn mid_size_streaming_stops_at_512kib_and_rejects_one_mib() {
        let (object_info, fi) = plain_metadata(512 * 1024);
        assert_eq!(
            get_mid_size_streaming_object_size_with_flags(&None, &object_info, &fi, &ObjectOptions::default(), true, true, true),
            Some(512 * 1024)
        );

        let (large_info, large_fi) = plain_metadata(512 * 1024 + 1);
        assert_eq!(
            get_mid_size_streaming_object_size_with_flags(
                &None,
                &large_info,
                &large_fi,
                &ObjectOptions::default(),
                true,
                true,
                true
            ),
            None
        );

        let (one_mib_info, one_mib_fi) = plain_metadata(1024 * 1024);
        assert_eq!(
            get_mid_size_streaming_object_size_with_flags(
                &None,
                &one_mib_info,
                &one_mib_fi,
                &ObjectOptions::default(),
                true,
                true,
                true
            ),
            None
        );
    }

    #[test]
    #[serial]
    fn mid_size_streaming_rejects_ranges_and_transformed_objects() {
        let (object_info, fi) = plain_metadata(256 * 1024);
        let range = Some(HTTPRangeSpec {
            is_suffix_length: false,
            start: 0,
            end: 1,
        });
        assert_eq!(
            get_mid_size_streaming_object_size_with_flags(&range, &object_info, &fi, &ObjectOptions::default(), true, true, true),
            None
        );

        let mut encrypted = object_info;
        encrypted.user_defined = Arc::new(HashMap::from([("x-minio-encryption-key".to_string(), "opaque".to_string())]));
        assert_eq!(
            get_mid_size_streaming_object_size_with_flags(&None, &encrypted, &fi, &ObjectOptions::default(), true, true, true),
            None
        );
    }

    #[test]
    #[serial]
    fn mid_size_streaming_rejects_inconsistent_part_geometry() {
        let (object_info, mut fi) = plain_metadata(256 * 1024);
        fi.parts[0].size += 1;
        assert_eq!(
            get_mid_size_streaming_object_size_with_flags(&None, &object_info, &fi, &ObjectOptions::default(), true, true, true),
            None
        );

        let (mut object_info, fi) = plain_metadata(256 * 1024);
        object_info.parts = Arc::new(vec![ObjectPartInfo {
            number: 2,
            size: fi.parts[0].size,
            actual_size: fi.parts[0].actual_size,
            ..Default::default()
        }]);
        assert_eq!(
            get_mid_size_streaming_object_size_with_flags(&None, &object_info, &fi, &ObjectOptions::default(), true, true, true),
            None
        );
    }

    #[test]
    #[serial]
    fn mid_size_streaming_can_be_disabled_without_affecting_direct_memory_gate() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_MID_SIZE_STREAMING_ENABLE, Some("false")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
            ],
            || {
                let (object_info, fi) = plain_metadata(256 * 1024);
                assert_eq!(
                    get_mid_size_streaming_object_size(&None, &object_info, &fi, &ObjectOptions::default(), true),
                    None
                );
            },
        );
    }

    #[test]
    #[serial]
    fn mid_size_streaming_respects_codec_compatibility_kill_switches() {
        let (object_info, fi) = plain_metadata(256 * 1024);
        for (name, value) in [
            (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, "false"),
            (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, "false"),
            (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, "false"),
        ] {
            temp_env::with_vars(
                [
                    (ENV_RUSTFS_GET_MID_SIZE_STREAMING_ENABLE, Some("true")),
                    (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                    (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                    (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                    (name, Some(value)),
                ],
                || {
                    assert_eq!(
                        get_mid_size_streaming_object_size(&None, &object_info, &fi, &ObjectOptions::default(), true),
                        None
                    );
                },
            );
        }
    }

    #[test]
    #[serial]
    fn mid_size_streaming_rejects_copy_source_policy() {
        let (object_info, fi) = plain_metadata(256 * 1024);
        let result = tokio::runtime::Runtime::new()
            .expect("test runtime should initialize")
            .block_on(crate::set_disk::with_get_object_read_policy(
                crate::set_disk::GetObjectReadPolicy::CopySource,
                async {
                    get_mid_size_streaming_object_size_with_flags(
                        &None,
                        &object_info,
                        &fi,
                        &ObjectOptions::default(),
                        true,
                        true,
                        true,
                    )
                },
            ));
        assert_eq!(result, None);
    }
}

#[cfg(test)]
mod object_encryption_resolver_wiring_tests {
    use super::*;
    use crate::object_api::{EncryptionResolutionError, ObjectEncryptionResolver, ReadEncryptionMaterial, ReadEncryptionRequest};
    use std::io::Cursor;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl CapturedLogs {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().expect("captured logs mutex should not poison").clone())
                .expect("captured logs should be valid UTF-8")
        }
    }

    impl std::io::Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut captured = self.0.lock().expect("captured logs mutex should not poison");
            std::io::Write::write(&mut *captured, buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CapturedLogWriter(Arc::clone(&self.0))
        }
    }

    struct CountingResolver {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ObjectEncryptionResolver for CountingResolver {
        async fn resolve_read_material(
            &self,
            _request: ReadEncryptionRequest<'_>,
        ) -> std::result::Result<Option<ReadEncryptionMaterial>, EncryptionResolutionError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
    }

    #[tokio::test]
    async fn get_object_reader_forwards_instance_resolver() {
        let resolver = Arc::new(CountingResolver {
            calls: AtomicUsize::new(0),
        });
        let ctx = InstanceContext::new();
        assert!(
            ctx.set_object_encryption_resolver(resolver.clone()).is_ok(),
            "fresh context should accept resolver"
        );
        let object_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            size: 1,
            user_defined: Arc::new(HashMap::from([("x-amz-server-side-encryption".to_string(), "AES256".to_string())])),
            ..Default::default()
        };

        let result = get_object_reader_with_context(
            &ctx,
            Box::new(Cursor::new(Vec::<u8>::new())),
            None,
            &object_info,
            &ObjectOptions::default(),
            &HeaderMap::new(),
        )
        .await;

        assert!(result.is_err(), "resolver returning no material must fail closed");
        assert_eq!(resolver.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_object_reader_span_never_records_transport_headers() {
        use super::hermetic_set_disks_support::hermetic_set_disks_isolated;
        use crate::storage_api_contracts::object::ObjectIO as _;
        use rustfs_utils::http::headers::SSEC_KEY_HEADER;

        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(logs.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let (_temp_dirs, _disks, set_disks) = hermetic_set_disks_isolated(4).await;
        let mut headers = HeaderMap::new();
        headers.insert(http::header::AUTHORIZATION, HeaderValue::from_static("credential-must-not-be-logged"));
        headers.insert(SSEC_KEY_HEADER, HeaderValue::from_static("customer-key-must-not-be-logged"));

        let _ = set_disks
            .get_object_reader("missing-bucket", "missing-object", None, headers, &ObjectOptions::default())
            .await;

        let captured = logs.contents();
        assert!(!captured.contains("credential-must-not-be-logged"));
        assert!(!captured.contains("customer-key-must-not-be-logged"));
    }
}

#[cfg(test)]
pub(in crate::set_disk::ops) mod hermetic_set_disks_support {
    //! Shared hermetic `SetDisks` construction for the ops tests below: the
    //! `SetDisks` under test is built directly on formatted local disks (same
    //! pattern as the `ops/locking.rs` tests) so the tests stay hermetic — no
    //! global local-disk registry, lock clients, or ECStore instance is
    //! touched, and each test owns its temp workspace.

    use super::*;
    use crate::store::init_format::save_format_file;
    use rustfs_lock::client::LockClient;
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    async fn make_formatted_local_disk_for_pool(
        disk_idx: usize,
        pool_index: usize,
        format: &FormatV3,
    ) -> (TempDir, Endpoint, DiskStore) {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let mut endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        endpoint.set_pool_index(pool_index);
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
        .expect("local disk should be created");

        let mut disk_format = format.clone();
        disk_format.erasure.this = format.erasure.sets[0][disk_idx];
        save_format_file(&Some(disk.clone()), &Some(disk_format))
            .await
            .expect("format should be saved");

        (dir, endpoint, disk)
    }

    pub(in crate::set_disk::ops) async fn hermetic_set_disks(disk_count: usize) -> (Vec<TempDir>, Vec<DiskStore>, Arc<SetDisks>) {
        hermetic_set_disks_for_pool_with_default_parity(disk_count, 0, disk_count / 2).await
    }

    pub(in crate::set_disk::ops) async fn hermetic_set_disks_for_pool_with_default_parity(
        disk_count: usize,
        pool_index: usize,
        default_parity_count: usize,
    ) -> (Vec<TempDir>, Vec<DiskStore>, Arc<SetDisks>) {
        hermetic_set_disks_with_lockers(disk_count, pool_index, default_parity_count, Vec::new()).await
    }

    /// Like [`hermetic_set_disks`], but binds the set to an isolated instance
    /// context pinned to plain erasure. `#[serial]` tests elsewhere flip the
    /// shared bootstrap context to DistErasure (`SetupTypeGuard`) while
    /// non-serial hermetic tests run; on the shared context such a window
    /// reroutes locking onto the empty dist locker list ("No lock clients
    /// available") and the batch-delete gate onto the dist path. Only suitable
    /// for tests that never touch context-resolved services registered on the
    /// ambient context (tier config manager, expiry state, ...), because the
    /// isolated context starts every one of those cells fresh.
    pub(crate) async fn hermetic_set_disks_isolated(disk_count: usize) -> (Vec<TempDir>, Vec<DiskStore>, Arc<SetDisks>) {
        hermetic_set_disks_for_pool_with_default_parity_isolated(disk_count, 0, disk_count / 2).await
    }

    /// Pool-parameterized variant of [`hermetic_set_disks_isolated`] with the
    /// same isolation contract.
    pub(in crate::set_disk::ops) async fn hermetic_set_disks_for_pool_with_default_parity_isolated(
        disk_count: usize,
        pool_index: usize,
        default_parity_count: usize,
    ) -> (Vec<TempDir>, Vec<DiskStore>, Arc<SetDisks>) {
        let isolated_ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        isolated_ctx
            .update_erasure_type(crate::layout::endpoints::SetupType::Erasure)
            .await;
        hermetic_set_disks_with_lockers_and_ctx(disk_count, pool_index, default_parity_count, Vec::new(), isolated_ctx).await
    }

    pub(in crate::set_disk::ops) async fn hermetic_set_disks_with_lockers(
        disk_count: usize,
        pool_index: usize,
        default_parity_count: usize,
        lockers: Vec<Arc<dyn LockClient>>,
    ) -> (Vec<TempDir>, Vec<DiskStore>, Arc<SetDisks>) {
        hermetic_set_disks_with_lockers_and_ctx(
            disk_count,
            pool_index,
            default_parity_count,
            lockers,
            crate::runtime::instance::bootstrap_ctx(),
        )
        .await
    }

    pub(in crate::set_disk::ops) async fn hermetic_set_disks_with_lockers_and_ctx(
        disk_count: usize,
        pool_index: usize,
        default_parity_count: usize,
        lockers: Vec<Arc<dyn LockClient>>,
        instance_ctx: Arc<crate::runtime::instance::InstanceContext>,
    ) -> (Vec<TempDir>, Vec<DiskStore>, Arc<SetDisks>) {
        let format = FormatV3::new(1, disk_count);

        let mut temp_dirs = Vec::with_capacity(disk_count);
        let mut endpoints = Vec::with_capacity(disk_count);
        let mut disk_stores = Vec::with_capacity(disk_count);
        let mut disks = Vec::with_capacity(disk_count);

        for disk_idx in 0..disk_count {
            let (temp_dir, endpoint, disk) = make_formatted_local_disk_for_pool(disk_idx, pool_index, &format).await;
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disk_stores.push(disk.clone());
            disks.push(Some(disk));
        }

        let set_disks = SetDisks::new_with_instance_ctx(
            "hermetic-ops-test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            disk_count,
            default_parity_count,
            0,
            pool_index,
            endpoints,
            format,
            lockers,
            instance_ctx,
        )
        .await;

        (temp_dirs, disk_stores, set_disks)
    }
}

#[cfg(test)]
mod replication_quota_safety_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks;
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn quota_put_future_keeps_commit_state_off_the_caller_stack() {
        let (_temp_dirs, _disks, set_disks) = hermetic_set_disks(4).await;
        let mut reader = PutObjReader::from_vec(Vec::new());
        let opts = ObjectOptions::default();

        let future = set_disks.put_object_with_old_current_size("bucket", "object", &mut reader, &opts);
        let future_size = std::mem::size_of_val(&future);

        assert!(
            future_size <= 1024,
            "put_object_with_old_current_size future must stay stack-bounded, got {future_size} bytes"
        );
    }

    #[tokio::test]
    async fn replication_put_quota_uses_physical_bytes_as_a_safety_floor() {
        let (_temp_dirs, disks, set_disks) = hermetic_set_disks(4).await;
        let bucket = "replication-put-quota-safety";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut user_defined = HashMap::new();
        insert_str(
            &mut user_defined,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        insert_str(&mut user_defined, SUFFIX_ACTUAL_SIZE, "1".to_string());
        let payload = vec![0x61; 4096];

        let mut denied_opts = ObjectOptions {
            replication_request: true,
            user_defined: user_defined.clone(),
            ..Default::default()
        };
        assert!(denied_opts.set_quota_admission(0, 4095));
        let mut denied_reader = PutObjReader::new(
            HashReader::from_stream(Cursor::new(payload.clone()), 4096, 1, None, None, false)
                .expect("construct forged replication reader"),
        );
        let err = set_disks
            .put_object(bucket, "object", &mut denied_reader, &denied_opts)
            .await
            .expect_err("server-observed bytes must prevent a tiny replication quota claim");
        assert!(matches!(err, StorageError::QuotaExceeded { current: 0, limit: 4095 }));

        let mut allowed_opts = ObjectOptions {
            replication_request: true,
            user_defined,
            ..Default::default()
        };
        assert!(allowed_opts.set_quota_admission(0, 4096));
        let mut allowed_reader = PutObjReader::new(
            HashReader::from_stream(Cursor::new(payload), 4096, 1, None, None, false)
                .expect("construct exact-boundary replication reader"),
        );
        let stored = set_disks
            .put_object(bucket, "object", &mut allowed_reader, &allowed_opts)
            .await
            .expect("server-observed exact quota boundary should succeed");
        assert_eq!(stored.get_actual_size().expect("stored logical size should parse"), 1);
    }

    #[tokio::test]
    async fn delete_returns_canonical_compressed_accounting_size() {
        let (_temp_dirs, disks, set_disks) = hermetic_set_disks(4).await;
        let bucket = "compressed-delete-accounting";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut user_defined = HashMap::new();
        insert_str(
            &mut user_defined,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        insert_str(&mut user_defined, SUFFIX_ACTUAL_SIZE, "1000".to_string());
        let mut reader = PutObjReader::new(
            HashReader::from_stream(Cursor::new(vec![0x5a; 400]), 400, 1000, None, None, false)
                .expect("compressed fixture reader should be valid"),
        );
        set_disks
            .put_object(
                bucket,
                "object",
                &mut reader,
                &ObjectOptions {
                    user_defined,
                    ..Default::default()
                },
            )
            .await
            .expect("compressed object should be written");

        let (deleted, errors, accounting) = set_disks
            .delete_objects_with_accounting(
                bucket,
                vec![ObjectToDelete {
                    object_name: "object".to_string(),
                    ..Default::default()
                }],
                ObjectOptions {
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                },
            )
            .await;

        assert!(errors[0].is_none(), "compressed delete should succeed: {:?}", errors[0]);
        assert!(deleted[0].found, "the committed object must be reported as found");
        assert_eq!(accounting[0].as_ref().and_then(|value| value.size), Some(1000));
        assert!(accounting[0].as_ref().is_some_and(|value| value.version_id.is_none()));
        assert!(accounting[0].as_ref().is_some_and(|value| value.removed_current_object));
    }

    #[tokio::test]
    async fn suspended_delete_marker_does_not_return_body_accounting() {
        let (_temp_dirs, disks, set_disks) = hermetic_set_disks(4).await;
        let bucket = "suspended-delete-accounting";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut user_defined = HashMap::new();
        insert_str(
            &mut user_defined,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        insert_str(&mut user_defined, SUFFIX_ACTUAL_SIZE, "1000".to_string());
        let mut reader = PutObjReader::new(
            HashReader::from_stream(Cursor::new(vec![0x5a; 400]), 400, 1000, None, None, false)
                .expect("compressed fixture reader should be valid"),
        );
        let suspended_opts = ObjectOptions {
            version_suspended: true,
            delete_replication_config_snapshot: Some(Arc::new(DeleteReplicationConfigSnapshot::from_configs_for_test(
                s3s::dto::VersioningConfiguration {
                    status: Some(s3s::dto::BucketVersioningStatus::from_static(s3s::dto::BucketVersioningStatus::SUSPENDED)),
                    ..Default::default()
                },
                None,
            ))),
            user_defined,
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))),
            ..Default::default()
        };
        set_disks
            .put_object(bucket, "object", &mut reader, &suspended_opts)
            .await
            .expect("compressed object should be written");

        let (deleted, errors, accounting) = set_disks
            .delete_objects_with_accounting(
                bucket,
                vec![ObjectToDelete {
                    object_name: "object".to_string(),
                    ..Default::default()
                }],
                suspended_opts,
            )
            .await;
        assert!(errors[0].is_none(), "suspended delete should create a marker: {:?}", errors[0]);
        assert!(deleted[0].delete_marker);
        assert!(accounting[0].is_none(), "a delete marker must not carry body accounting");
    }

    #[tokio::test]
    async fn direct_put_cannot_persist_a_tiny_logical_size() {
        let (_temp_dirs, disks, set_disks) = hermetic_set_disks(4).await;
        let bucket = "direct-put-quota-safety";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let payload = vec![0x62; 4096];
        let mut denied_opts = ObjectOptions::default();
        assert!(denied_opts.set_quota_admission(0, 4095));
        let mut denied_reader = PutObjReader::new(
            HashReader::from_stream(Cursor::new(payload.clone()), 4096, 1, None, None, false)
                .expect("construct forged direct reader"),
        );
        let err = set_disks
            .put_object(bucket, "object", &mut denied_reader, &denied_opts)
            .await
            .expect_err("server-observed bytes must prevent a tiny direct quota claim");
        assert!(matches!(err, StorageError::QuotaExceeded { current: 0, limit: 4095 }));

        let mut allowed_opts = ObjectOptions::default();
        assert!(allowed_opts.set_quota_admission(0, 4096));
        let mut allowed_reader = PutObjReader::new(
            HashReader::from_stream(Cursor::new(payload), 4096, 1, None, None, false)
                .expect("construct exact-boundary direct reader"),
        );
        let stored = set_disks
            .put_object(bucket, "object", &mut allowed_reader, &allowed_opts)
            .await
            .expect("server-observed exact quota boundary should succeed");
        assert_eq!(stored.get_actual_size().expect("stored logical size should parse"), 4096);
    }

    #[tokio::test]
    async fn quota_rejects_ciphertext_replication_without_a_server_observed_logical_size() {
        let (_temp_dirs, disks, set_disks) = hermetic_set_disks(4).await;
        let bucket = "ciphertext-replication-quota-safety";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut user_defined = HashMap::new();
        user_defined.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
        insert_str(&mut user_defined, SUFFIX_ACTUAL_SIZE, "1".to_string());
        let mut opts = ObjectOptions {
            replication_request: true,
            preserve_ciphertext: true,
            user_defined,
            ..Default::default()
        };
        assert!(opts.set_quota_admission(0, u64::MAX));
        let payload = vec![0x63; 4096];
        let mut reader = PutObjReader::new(
            HashReader::from_stream(Cursor::new(payload), 4096, 4096, None, None, false)
                .expect("construct ciphertext replication reader"),
        );
        let err = set_disks
            .put_object(bucket, "object", &mut reader, &opts)
            .await
            .expect_err("ciphertext replication without a server-observed logical size must fail closed");
        assert!(matches!(err, StorageError::PartMissingOrCorrupt));
    }
}

#[cfg(test)]
mod replication_lww_tests {
    //! Receiver-side LWW for authorized replication writes (rustfs/backlog#1953,
    //! audit A4/P1-6): an inbound replication PUT whose per-category timestamp
    //! (tags / retention / legal hold) is older than the destination version's
    //! stored timestamp must keep the local category values instead of
    //! overwriting them; categories are judged independently and the write
    //! itself still succeeds.

    use super::hermetic_set_disks_support::hermetic_set_disks_isolated as hermetic_set_disks;
    use super::*;
    use rustfs_utils::http::headers::{
        AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, AMZ_OBJECT_TAGGING,
    };
    use rustfs_utils::http::{
        SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, SUFFIX_TAGGING_TIMESTAMP, get_str,
        insert_str,
    };
    use time::format_description::well_known::Rfc3339;

    const T_OLD: &str = "2026-01-01T00:00:00Z";
    const T_LOCAL: &str = "2026-02-01T00:00:00Z";
    const T_NEW: &str = "2026-03-01T00:00:00Z";

    fn parse_ts(value: &str) -> OffsetDateTime {
        OffsetDateTime::parse(value, &Rfc3339).expect("test timestamp should parse")
    }

    async fn make_bucket(disks: &[DiskStore], bucket: &str) {
        for disk in disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
    }

    async fn put_version(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, version_id: &str, opts: &ObjectOptions) {
        let mut reader = PutObjReader::from_vec(b"lww-body".to_vec());
        set_disks
            .put_object(bucket, object, &mut reader, opts)
            .await
            .expect("versioned put should commit");
        assert_eq!(opts.version_id.as_deref(), Some(version_id));
    }

    fn versioned_opts(version_id: &str, user_defined: HashMap<String, String>) -> ObjectOptions {
        ObjectOptions {
            versioned: true,
            version_id: Some(version_id.to_string()),
            user_defined,
            // Explicit-version PUTs require the bucket Object Lock snapshot.
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                crate::bucket::metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
            ))),
            ..Default::default()
        }
    }

    /// Local state: version `version_id` with tags "site=local" stamped `T_LOCAL`.
    async fn seed_local_tagged_version(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, version_id: &str) {
        let mut user_defined = HashMap::new();
        user_defined.insert(AMZ_OBJECT_TAGGING.to_string(), "site=local".to_string());
        insert_str(&mut user_defined, SUFFIX_TAGGING_TIMESTAMP, T_LOCAL.to_string());
        put_version(set_disks, bucket, object, version_id, &versioned_opts(version_id, user_defined)).await;
    }

    fn inbound_tagging_opts(version_id: &str, tags: &str, timestamp: &str) -> ObjectOptions {
        let mut user_defined = HashMap::new();
        user_defined.insert(AMZ_OBJECT_TAGGING.to_string(), tags.to_string());
        insert_str(&mut user_defined, SUFFIX_TAGGING_TIMESTAMP, timestamp.to_string());
        ObjectOptions {
            replication_request: true,
            replication_tagging_timestamp: Some(parse_ts(timestamp)),
            ..versioned_opts(version_id, user_defined)
        }
    }

    async fn version_info(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, version_id: &str) -> ObjectInfo {
        set_disks
            .get_object_info(bucket, object, &versioned_opts(version_id, HashMap::new()))
            .await
            .expect("version should be readable")
    }

    #[tokio::test]
    async fn inbound_stale_tagging_keeps_newer_local_tags() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-tagging-stale";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;
        seed_local_tagged_version(&set_disks, bucket, object, &version_id).await;

        put_version(
            &set_disks,
            bucket,
            object,
            &version_id,
            &inbound_tagging_opts(&version_id, "site=remote", T_OLD),
        )
        .await;

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(
            info.user_tags.as_str(),
            "site=local",
            "older inbound tags must not overwrite newer local tags"
        );
        assert_eq!(
            get_str(&info.user_defined, SUFFIX_TAGGING_TIMESTAMP).as_deref(),
            Some(T_LOCAL),
            "the winning local tagging timestamp must be preserved"
        );
    }

    #[tokio::test]
    async fn inbound_newer_tagging_overwrites_local_tags() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-tagging-newer";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;
        seed_local_tagged_version(&set_disks, bucket, object, &version_id).await;

        put_version(
            &set_disks,
            bucket,
            object,
            &version_id,
            &inbound_tagging_opts(&version_id, "site=remote", T_NEW),
        )
        .await;

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(
            info.user_tags.as_str(),
            "site=remote",
            "newer inbound tags must overwrite older local tags"
        );
        assert_eq!(get_str(&info.user_defined, SUFFIX_TAGGING_TIMESTAMP).as_deref(), Some(T_NEW));
    }

    #[tokio::test]
    async fn inbound_wins_when_local_has_no_tagging_timestamp() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-tagging-no-local-ts";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;
        // Pre-P1-6 data: local tags without a stored tagging timestamp.
        let mut user_defined = HashMap::new();
        user_defined.insert(AMZ_OBJECT_TAGGING.to_string(), "site=local".to_string());
        put_version(&set_disks, bucket, object, &version_id, &versioned_opts(&version_id, user_defined)).await;

        put_version(
            &set_disks,
            bucket,
            object,
            &version_id,
            &inbound_tagging_opts(&version_id, "site=remote", T_OLD),
        )
        .await;

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(
            info.user_tags.as_str(),
            "site=remote",
            "without a local timestamp the inbound category must win (pre-LWW data compatibility)"
        );
    }

    #[tokio::test]
    async fn categories_are_judged_independently() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-category-independent";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;

        // Local: newer tags (T_LOCAL), older *cleared* retention (T_OLD) —
        // timestamp key only, the shape a replicated retention clear stores.
        // (An active local retention would already block the overwrite at the
        // WORM gate; the LWW-reachable retention states are cleared/expired.)
        let mut local = HashMap::new();
        local.insert(AMZ_OBJECT_TAGGING.to_string(), "site=local".to_string());
        insert_str(&mut local, SUFFIX_TAGGING_TIMESTAMP, T_LOCAL.to_string());
        insert_str(&mut local, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, T_OLD.to_string());
        put_version(&set_disks, bucket, object, &version_id, &versioned_opts(&version_id, local)).await;

        // Inbound: older tags (T_OLD), newer retention (T_NEW).
        let mut inbound = HashMap::new();
        inbound.insert(AMZ_OBJECT_TAGGING.to_string(), "site=remote".to_string());
        insert_str(&mut inbound, SUFFIX_TAGGING_TIMESTAMP, T_OLD.to_string());
        inbound.insert(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), "COMPLIANCE".to_string());
        inbound.insert(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), "2028-01-01T00:00:00Z".to_string());
        insert_str(&mut inbound, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, T_NEW.to_string());
        let opts = ObjectOptions {
            replication_request: true,
            replication_tagging_timestamp: Some(parse_ts(T_OLD)),
            replication_retention_timestamp: Some(parse_ts(T_NEW)),
            ..versioned_opts(&version_id, inbound)
        };
        put_version(&set_disks, bucket, object, &version_id, &opts).await;

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(info.user_tags.as_str(), "site=local", "the stale tagging category must keep local values");
        assert_eq!(
            info.user_defined.get(AMZ_OBJECT_LOCK_MODE_LOWER).map(String::as_str),
            Some("COMPLIANCE"),
            "the newer retention category must be applied in the same write"
        );
        assert_eq!(get_str(&info.user_defined, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP).as_deref(), Some(T_NEW));
    }

    #[tokio::test]
    async fn inbound_stale_legal_hold_keeps_local_value() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-legalhold-stale";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;

        // Local: legal hold released (OFF) at T_LOCAL. (A local hold that is
        // still ON already blocks the overwrite at the WORM gate; the
        // LWW-reachable divergence is a stale inbound ON resurrecting a hold
        // that was released more recently on this site.)
        let mut local = HashMap::new();
        local.insert(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.to_string(), "OFF".to_string());
        insert_str(&mut local, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, T_LOCAL.to_string());
        put_version(&set_disks, bucket, object, &version_id, &versioned_opts(&version_id, local)).await;

        let mut inbound = HashMap::new();
        inbound.insert(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.to_string(), "ON".to_string());
        insert_str(&mut inbound, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, T_OLD.to_string());
        let opts = ObjectOptions {
            replication_request: true,
            replication_legalhold_timestamp: Some(parse_ts(T_OLD)),
            ..versioned_opts(&version_id, inbound)
        };
        put_version(&set_disks, bucket, object, &version_id, &opts).await;

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(
            info.user_defined.get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER).map(String::as_str),
            Some("OFF"),
            "a stale inbound legal hold must not resurrect a hold released more recently"
        );
        assert_eq!(
            get_str(&info.user_defined, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP).as_deref(),
            Some(T_LOCAL)
        );
    }

    /// Dual-key invariant under LWW: a MinIO-written destination version may
    /// carry only the x-minio-internal timestamp key; when the local category
    /// wins, the restored map must still hold BOTH compatibility keys.
    #[test]
    fn local_win_restores_both_internal_timestamp_keys_for_minio_only_metadata() {
        let mut inbound = HashMap::new();
        inbound.insert(AMZ_OBJECT_TAGGING.to_string(), "site=remote".to_string());
        insert_str(&mut inbound, SUFFIX_TAGGING_TIMESTAMP, T_OLD.to_string());
        let existing = HashMap::from([
            (AMZ_OBJECT_TAGGING.to_string(), "site=local".to_string()),
            ("X-Minio-Internal-Tagging-Timestamp".to_string(), T_LOCAL.to_string()),
        ]);
        let opts = ObjectOptions {
            replication_request: true,
            replication_tagging_timestamp: Some(parse_ts(T_OLD)),
            ..Default::default()
        };

        assert!(merge_replication_metadata_lww(&mut inbound, &existing, &opts));
        assert_eq!(inbound.get(AMZ_OBJECT_TAGGING).map(String::as_str), Some("site=local"));
        assert_eq!(
            inbound.get("x-rustfs-internal-tagging-timestamp").map(String::as_str),
            Some(T_LOCAL),
            "the RustFS twin key must be materialized even when the source version only had the MinIO key"
        );
        assert_eq!(inbound.get("x-minio-internal-tagging-timestamp").map(String::as_str), Some(T_LOCAL));
    }

    /// When the inbound category wins, the stored timestamp must be the
    /// source-authored one: the PUT path's eval_metadata stamps the
    /// object-lock timestamps with the receiver's clock
    /// (`parse_object_lock_retention`), which would otherwise make this
    /// replica's clock the LWW authority and wedge later convergence.
    #[tokio::test]
    async fn inbound_win_pins_stored_timestamp_to_source_authored_value() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-retention-ts-pinned";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;

        // Local cleared retention at T_OLD.
        let mut local = HashMap::new();
        insert_str(&mut local, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, T_OLD.to_string());
        put_version(&set_disks, bucket, object, &version_id, &versioned_opts(&version_id, local)).await;

        // Inbound newer retention: the source authored T_LOCAL, but the PUT
        // path's eval_metadata stomped the metadata key with receiver-now
        // (simulated by T_NEW here).
        let mut inbound = HashMap::new();
        inbound.insert(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), "GOVERNANCE".to_string());
        inbound.insert(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), "2028-01-01T00:00:00Z".to_string());
        insert_str(&mut inbound, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, T_NEW.to_string());
        let opts = ObjectOptions {
            replication_request: true,
            replication_retention_timestamp: Some(parse_ts(T_LOCAL)),
            ..versioned_opts(&version_id, inbound)
        };
        put_version(&set_disks, bucket, object, &version_id, &opts).await;

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(
            get_str(&info.user_defined, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP).as_deref(),
            Some(T_LOCAL),
            "the stored category timestamp must be the source-authored time, not the receiver's clock"
        );
        assert_eq!(info.user_defined.get(AMZ_OBJECT_LOCK_MODE_LOWER).map(String::as_str), Some("GOVERNANCE"));
    }

    #[tokio::test]
    async fn first_inbound_version_pins_source_authored_timestamp() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-first-version-ts-pinned";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;

        let mut inbound = HashMap::new();
        inbound.insert(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.to_string(), "OFF".to_string());
        insert_str(&mut inbound, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, T_OLD.to_string());
        let mut evaluated = inbound.clone();
        insert_str(&mut evaluated, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, T_NEW.to_string());
        let opts = ObjectOptions {
            replication_request: true,
            replication_legalhold_timestamp: Some(parse_ts(T_OLD)),
            eval_metadata: Some(evaluated),
            ..versioned_opts(&version_id, inbound)
        };

        put_version(&set_disks, bucket, object, &version_id, &opts).await;

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(
            get_str(&info.user_defined, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP).as_deref(),
            Some(T_OLD),
            "the first copy must store the source timestamp, not the receiver evaluation time"
        );
    }

    #[tokio::test]
    async fn newer_local_tag_deletion_survives_stale_inbound_tags() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-tagging-deleted";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;
        // Local DeleteObjectTagging state: no tags, but a newer tagging timestamp.
        let mut local = HashMap::new();
        insert_str(&mut local, SUFFIX_TAGGING_TIMESTAMP, T_LOCAL.to_string());
        put_version(&set_disks, bucket, object, &version_id, &versioned_opts(&version_id, local)).await;

        put_version(
            &set_disks,
            bucket,
            object,
            &version_id,
            &inbound_tagging_opts(&version_id, "site=remote", T_OLD),
        )
        .await;

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert!(
            info.user_tags.is_empty(),
            "a newer local tag deletion must not be resurrected by older inbound tags"
        );
        assert_eq!(get_str(&info.user_defined, SUFFIX_TAGGING_TIMESTAMP).as_deref(), Some(T_LOCAL));
    }

    /// Destination version under an active legal hold at `hold_timestamp`,
    /// plus an active COMPLIANCE retention (no retention timestamp).
    async fn seed_locked_version(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, version_id: &str, hold_timestamp: &str) {
        let mut local = HashMap::new();
        local.insert(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.to_string(), "ON".to_string());
        insert_str(&mut local, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, hold_timestamp.to_string());
        local.insert(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), "COMPLIANCE".to_string());
        local.insert(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), "2099-01-01T00:00:00Z".to_string());
        put_version(set_disks, bucket, object, version_id, &versioned_opts(version_id, local)).await;
    }

    /// Inbound legal-hold release from a source that also carries the (same)
    /// COMPLIANCE retention; the sender stamps a source timestamp for every
    /// category the source version has.
    fn inbound_legal_hold_release_opts(version_id: &str, timestamp: &str) -> ObjectOptions {
        let mut inbound = HashMap::new();
        inbound.insert(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.to_string(), "OFF".to_string());
        insert_str(&mut inbound, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, timestamp.to_string());
        inbound.insert(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), "COMPLIANCE".to_string());
        inbound.insert(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), "2099-01-01T00:00:00Z".to_string());
        insert_str(&mut inbound, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, T_OLD.to_string());
        ObjectOptions {
            replication_request: true,
            replication_legalhold_timestamp: Some(parse_ts(timestamp)),
            replication_retention_timestamp: Some(parse_ts(T_OLD)),
            ..versioned_opts(version_id, inbound)
        }
    }

    /// The source's lock state governs the replica: a legal-hold release (or a
    /// retention change) can only reach this site through the authorized
    /// replication write, so the commit-time WORM gate must not reject it
    /// because the destination version is currently locked.
    #[tokio::test]
    async fn inbound_newer_legal_hold_release_updates_locked_version() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-locked-release-newer";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;
        seed_locked_version(&set_disks, bucket, object, &version_id, T_OLD).await;

        put_version(
            &set_disks,
            bucket,
            object,
            &version_id,
            &inbound_legal_hold_release_opts(&version_id, T_NEW),
        )
        .await;

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(
            info.user_defined.get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER).map(String::as_str),
            Some("OFF"),
            "a newer source-side legal hold release must be applied to the locked replica"
        );
        assert_eq!(get_str(&info.user_defined, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP).as_deref(), Some(T_NEW));
        assert_eq!(
            info.user_defined.get(AMZ_OBJECT_LOCK_MODE_LOWER).map(String::as_str),
            Some("COMPLIANCE"),
            "the untouched retention category must survive the write"
        );
    }

    /// Skipping the WORM gate for replication writes must not weaken LWW: a
    /// stale inbound release still loses to a hold applied more recently here.
    #[tokio::test]
    async fn inbound_stale_legal_hold_release_keeps_newer_local_hold() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-locked-release-stale";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;
        seed_locked_version(&set_disks, bucket, object, &version_id, T_LOCAL).await;

        put_version(
            &set_disks,
            bucket,
            object,
            &version_id,
            &inbound_legal_hold_release_opts(&version_id, T_OLD),
        )
        .await;

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(
            info.user_defined.get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER).map(String::as_str),
            Some("ON"),
            "a stale inbound release must not lift a hold applied more recently on this site"
        );
        assert_eq!(
            get_str(&info.user_defined, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP).as_deref(),
            Some(T_LOCAL)
        );
    }

    /// A replication write that carries no source decision for a locking
    /// category (here: tags changed at a source that never held the object)
    /// must not lift the destination's hold by replacing the metadata
    /// unjudged; it stays WORM-rejected like a local overwrite.
    #[tokio::test]
    async fn inbound_without_legal_hold_timestamp_stays_rejected_on_held_version() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-locked-unjudged-category";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;
        seed_locked_version(&set_disks, bucket, object, &version_id, T_OLD).await;

        let mut inbound = HashMap::new();
        inbound.insert(AMZ_OBJECT_TAGGING.to_string(), "k=v".to_string());
        insert_str(&mut inbound, SUFFIX_TAGGING_TIMESTAMP, T_NEW.to_string());
        inbound.insert(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), "COMPLIANCE".to_string());
        inbound.insert(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), "2099-01-01T00:00:00Z".to_string());
        let opts = ObjectOptions {
            replication_request: true,
            replication_tagging_timestamp: Some(parse_ts(T_NEW)),
            replication_retention_timestamp: Some(parse_ts(T_NEW)),
            replication_legalhold_timestamp: None,
            ..versioned_opts(&version_id, inbound)
        };
        let mut reader = PutObjReader::from_vec(b"lww-body".to_vec());
        let err = set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect_err("a replication write without the legal-hold source timestamp must stay rejected");
        assert!(matches!(err, StorageError::PrefixAccessDenied(_, _)), "unexpected error: {err}");

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(info.user_defined.get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER).map(String::as_str), Some("ON"));
    }

    fn default_retention_snapshot(mode: &'static str) -> Arc<ObjectLockConfigSnapshot> {
        Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::Configured {
            config: s3s::dto::ObjectLockConfiguration {
                object_lock_enabled: Some(s3s::dto::ObjectLockEnabled::from_static(s3s::dto::ObjectLockEnabled::ENABLED)),
                rule: Some(s3s::dto::ObjectLockRule {
                    default_retention: Some(s3s::dto::DefaultRetention {
                        mode: Some(s3s::dto::ObjectLockRetentionMode::from_static(mode)),
                        days: Some(1),
                        years: None,
                    }),
                }),
            },
            updated_at: OffsetDateTime::now_utc(),
        }))
    }

    /// The bucket default retention locks a version that carries no explicit
    /// retention keys. A tagging-only authorized replication write carries no
    /// source retention decision, so it must stay WORM-rejected exactly like
    /// it does for an explicitly retained version; with the retention source
    /// timestamp the write passes and LWW judges it.
    #[tokio::test]
    async fn inbound_without_retention_timestamp_stays_rejected_under_bucket_default_retention() {
        for mode in [
            s3s::dto::ObjectLockRetentionMode::COMPLIANCE,
            s3s::dto::ObjectLockRetentionMode::GOVERNANCE,
        ] {
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "lww-locked-default-retention";
            let object = "object";
            let version_id = Uuid::new_v4().to_string();
            make_bucket(&disk_stores, bucket).await;
            seed_local_tagged_version(&set_disks, bucket, object, &version_id).await;
            let seeded = version_info(&set_disks, bucket, object, &version_id).await;
            assert!(
                !seeded.user_defined.contains_key(AMZ_OBJECT_LOCK_MODE_LOWER),
                "the seeded version must be protected by the bucket default only"
            );

            let tagging_only = ObjectOptions {
                object_lock_config_snapshot: Some(default_retention_snapshot(mode)),
                ..inbound_tagging_opts(&version_id, "site=remote", T_NEW)
            };
            let mut reader = PutObjReader::from_vec(b"lww-body".to_vec());
            let err = set_disks
                .put_object(bucket, object, &mut reader, &tagging_only)
                .await
                .expect_err("{mode}: a tagging-only replication write must not pass the bucket default retention lock");
            assert!(matches!(err, StorageError::PrefixAccessDenied(_, _)), "{mode}: unexpected error: {err}");
            let info = version_info(&set_disks, bucket, object, &version_id).await;
            assert_eq!(
                info.user_tags.as_str(),
                "site=local",
                "{mode}: the default-protected version must be untouched"
            );

            let with_retention_decision = ObjectOptions {
                replication_retention_timestamp: Some(parse_ts(T_NEW)),
                ..tagging_only
            };
            put_version(&set_disks, bucket, object, &version_id, &with_retention_decision).await;
            let info = version_info(&set_disks, bucket, object, &version_id).await;
            assert_eq!(
                info.user_tags.as_str(),
                "site=remote",
                "{mode}: with the retention source timestamp the newer inbound tags win"
            );
        }
    }

    /// The gate runs before the replication bypass, so malformed persisted
    /// lock metadata still fails closed for an authorized replication write.
    #[tokio::test]
    async fn replication_write_on_malformed_lock_metadata_still_fails_closed() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-locked-malformed";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;
        let mut local = HashMap::new();
        local.insert(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.to_string(), "MAYBE".to_string());
        put_version(&set_disks, bucket, object, &version_id, &versioned_opts(&version_id, local)).await;

        let mut reader = PutObjReader::from_vec(b"lww-body".to_vec());
        let err = set_disks
            .put_object(bucket, object, &mut reader, &inbound_legal_hold_release_opts(&version_id, T_NEW))
            .await
            .expect_err("malformed persisted lock metadata must fail the replication write closed");
        assert!(!matches!(err, StorageError::PrefixAccessDenied(_, _)), "unexpected error: {err}");

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(info.user_defined.get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER).map(String::as_str), Some("MAYBE"));
    }

    /// The bypass is scoped to authorized replication writes: the same
    /// explicit-version PUT without `replication_request` stays WORM-rejected.
    #[tokio::test]
    async fn non_replication_overwrite_of_locked_version_is_still_rejected() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lww-locked-plain-put";
        let object = "object";
        let version_id = Uuid::new_v4().to_string();
        make_bucket(&disk_stores, bucket).await;
        seed_locked_version(&set_disks, bucket, object, &version_id, T_OLD).await;

        let opts = ObjectOptions {
            replication_request: false,
            ..inbound_legal_hold_release_opts(&version_id, T_NEW)
        };
        let mut reader = PutObjReader::from_vec(b"lww-body".to_vec());
        let err = set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect_err("a non-replication overwrite of a locked version must stay rejected");
        assert!(matches!(err, StorageError::PrefixAccessDenied(_, _)), "unexpected error: {err}");

        let info = version_info(&set_disks, bucket, object, &version_id).await;
        assert_eq!(info.user_defined.get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER).map(String::as_str), Some("ON"));
    }
}

#[cfg(test)]
mod inline_put_commit_path_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks_isolated as hermetic_set_disks;
    use super::*;
    use crate::config::storageclass::{INLINE_BLOCK_ENV, lookup_config_for_pools, lookup_config_for_pools_without_env};
    use crate::disk::ReadOptions;
    use crate::ecstore_validation_blackbox::make_local_set_disks;
    use crate::set_disk::disk_call_counters;
    use crate::storage_api_contracts::bucket::{BucketOperations, MakeBucketOptions};
    use rustfs_config::server_config::KVS;
    use serial_test::serial;
    use tokio::io::AsyncReadExt;

    async fn make_bucket(disks: &[DiskStore], bucket: &str) {
        for disk in disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
    }

    #[tokio::test]
    async fn inline_put_direct_commit_round_trips_verified_bitrot_shards() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "inline-direct-commit";
        let object = "object.bin";
        let payload: Vec<u8> = (0..16 * 1024).map(|index| (index % 251) as u8).collect();
        make_bucket(&disk_stores, bucket).await;

        let mut reader = PutObjReader::from_vec(payload.clone());
        // This test asserts the committed inline shard on every individual
        // disk. A lock-owning PUT may quorum-ack before its rename tail
        // drains, so keep the setup on the full-fanout commit path.
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
            .expect("inline PUT should commit");

        let read_data = ReadOptions {
            read_data: true,
            ..Default::default()
        };
        for (disk_index, disk) in disk_stores.iter().enumerate() {
            let file_info = disk
                .read_version("", bucket, object, "", &read_data)
                .await
                .unwrap_or_else(|err| panic!("disk {disk_index} should persist inline metadata: {err}"));
            assert!(file_info.inline_data(), "disk {disk_index} should mark the shard inline");
            let inline_data = file_info
                .data
                .as_ref()
                .unwrap_or_else(|| panic!("disk {disk_index} should persist inline bitrot bytes"));
            let erasure = erasure_from_file_info(&file_info, false).expect("persisted erasure layout should be valid");
            let logical_shard_size =
                usize::try_from(erasure.shard_file_size(payload.len() as i64)).expect("logical shard size should fit usize");
            coding::bitrot_verify(
                Cursor::new(inline_data.clone()),
                inline_data.len(),
                logical_shard_size,
                HashAlgorithm::HighwayHash256S,
                erasure.shard_size(),
            )
            .await
            .unwrap_or_else(|err| panic!("disk {disk_index} inline shard should pass bitrot verification: {err}"));
        }

        let mut object_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("committed inline object should be readable");
        let mut restored = Vec::new();
        object_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("inline object should stream");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    #[serial]
    async fn get_object_reader_wires_mid_size_to_single_inflight() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "mid-size-reader-wiring";
        let object = "object.bin";
        let payload: Vec<u8> = (0..256 * 1024).map(|index| (index % 251) as u8).collect();
        make_bucket(&disk_stores, bucket).await;
        let storage_class = temp_env::with_var(INLINE_BLOCK_ENV, Some("1KiB"), || lookup_config_for_pools(&KVS::new(), &[4]))
            .expect("test storage class should resolve");
        set_disks.set_test_storage_class_config(storage_class);

        let mut writer = PutObjReader::from_vec(payload.clone());
        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_GET_MID_SIZE_STREAMING_ENABLE, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("on")),
                (rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("true")),
            ],
            async {
                set_disks
                    .put_object(bucket, object, &mut writer, &ObjectOptions::default())
                    .await
                    .expect("mid-size wiring fixture should commit");

                crate::set_disk::reset_test_get_object_reader_path();
                let single_inflight_before = crate::set_disk::coding::decode_reader::test_single_inflight_construction_count();
                let mut reader = set_disks
                    .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                    .await
                    .expect("mid-size wiring GET should succeed");
                let mut restored = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut restored)
                    .await
                    .expect("mid-size wiring reader should stream");

                assert_eq!(restored, payload);
                assert!(
                    crate::set_disk::test_get_object_reader_selected_mid_size(),
                    "full get_object_reader path must select mid-size streaming (path id {})",
                    crate::set_disk::test_get_object_reader_path_id()
                );
                assert!(
                    crate::set_disk::coding::decode_reader::test_single_inflight_construction_count() > single_inflight_before,
                    "mid-size get_object_reader wiring must construct SingleInFlight"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn get_object_reader_routes_one_mib_away_from_mid_size_reader() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "one-mib-legacy-reader";
        let object = "object.bin";
        let payload = vec![0x5a; 1024 * 1024];
        make_bucket(&disk_stores, bucket).await;
        let storage_class = temp_env::with_var(INLINE_BLOCK_ENV, Some("1KiB"), || lookup_config_for_pools(&KVS::new(), &[4]))
            .expect("test storage class should resolve");
        set_disks.set_test_storage_class_config(storage_class);

        let mut writer = PutObjReader::from_vec(payload.clone());
        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_GET_MID_SIZE_STREAMING_ENABLE, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("off")),
                (rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("true")),
            ],
            async {
                set_disks
                    .put_object(bucket, object, &mut writer, &ObjectOptions::default())
                    .await
                    .expect("1 MiB fixture should commit");

                crate::set_disk::reset_test_get_object_reader_path();
                let mut reader = set_disks
                    .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                    .await
                    .expect("1 MiB legacy GET should succeed");
                let mut restored = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut restored)
                    .await
                    .expect("1 MiB legacy reader should stream");

                assert_eq!(restored, payload);
                assert_eq!(
                    crate::set_disk::test_get_object_reader_path_id(),
                    8,
                    "1 MiB must bypass mid-size and use legacy duplex when codec rollout is off"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn get_object_reader_codec_rollout_excludes_late_metadata_refresh() {
        let (_temp_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "one-mib-codec-reader";
        let object = "object.bin";
        let payload = vec![0x6b; 1024 * 1024];
        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("codec bucket should be created");
        let storage_class = temp_env::with_var(INLINE_BLOCK_ENV, Some("1KiB"), || lookup_config_for_pools(&KVS::new(), &[4]))
            .expect("test storage class should resolve");
        set_disks.set_test_storage_class_config(storage_class);

        let mut writer = PutObjReader::from_vec(payload.clone());
        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_TWO_PHASE_READ_PLAN_ENABLE", Some("true")),
                (ENV_RUSTFS_GET_MID_SIZE_STREAMING_ENABLE, Some("false")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some("legacy")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE, Some("false")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (crate::set_disk::ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("on")),
                (rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("true")),
            ],
            async {
                set_disks
                    .put_object(bucket, object, &mut writer, &ObjectOptions::default())
                    .await
                    .expect("codec fixture should commit");
                crate::set_disk::reset_test_get_object_reader_path();
                let calls = disk_call_counters::observe(object);
                let mut reader = set_disks
                    .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                    .await
                    .expect("codec GET should succeed");
                let mut restored = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut restored)
                    .await
                    .expect("codec GET should stream");
                assert_eq!(restored, payload);
                assert_eq!(
                    crate::set_disk::test_get_object_reader_path_id(),
                    5,
                    "codec path must win over late refresh"
                );
                assert_eq!(
                    calls.total(disk_call_counters::KIND_READ_VERSION),
                    4,
                    "codec path must use full metadata fanout and must not trigger a second late refresh"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn repeated_gets_reuse_the_set_erasure_shell() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "get-erasure-shell-cache";
        let object = "object.bin";
        let payload = vec![0x4d; 1024 * 1024];
        make_bucket(&disk_stores, bucket).await;

        let mut reader = PutObjReader::from_vec(payload.clone());
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("non-inline object should commit");
        assert!(set_disks.erasure_cache.entries.read().is_empty());

        for _ in 0..2 {
            let mut object_reader = set_disks
                .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("cached-shell GET should succeed");
            let mut restored = Vec::new();
            object_reader
                .stream
                .read_to_end(&mut restored)
                .await
                .expect("cached-shell GET should stream");
            assert_eq!(restored, payload);
            assert_eq!(set_disks.erasure_cache.entries.read().len(), 1);
        }
    }

    #[tokio::test]
    #[serial]
    async fn ec_8_4_default_budget_keeps_large_inline_candidate_out_of_xl_meta() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(12).await;
        set_disks.set_test_storage_class_config(
            lookup_config_for_pools_without_env(&KVS::new(), &[12]).expect("EC8+4 storage class should resolve"),
        );
        let bucket = "ec-8-4-inline-budget";
        let object = "object.bin";
        let payload = vec![0x5c; 300 * 1024];
        make_bucket(&disk_stores, bucket).await;

        let mut reader = PutObjReader::from_vec(payload.clone());
        temp_env::async_with_vars(
            [(
                crate::set_disk::core::io_primitives::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE,
                Some("false"),
            )],
            set_disks.put_object(bucket, object, &mut reader, &ObjectOptions::default()),
        )
        .await
        .expect("EC8+4 PUT should commit through the non-inline path");

        for (disk_index, disk) in disk_stores.iter().enumerate() {
            let file_info = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .unwrap_or_else(|err| panic!("disk {disk_index} should persist EC8+4 metadata: {err}"));
            assert_eq!(file_info.erasure.data_blocks, 8);
            assert_eq!(file_info.erasure.parity_blocks, 4);
            assert!(!file_info.inline_data(), "disk {disk_index} must keep the shard outside xl.meta");
        }

        let mut object_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("non-inline EC8+4 object should remain readable");
        let mut restored = Vec::new();
        object_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("non-inline EC8+4 object should stream");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    #[serial]
    async fn ec_8_4_versioned_budget_reaches_put_placement_decision() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(12).await;
        set_disks.set_test_storage_class_config(
            lookup_config_for_pools_without_env(&KVS::new(), &[12]).expect("EC8+4 storage class should resolve"),
        );
        let bucket = "ec-8-4-versioned-inline-budget";
        let object = "object.bin";
        let payload = vec![0x73; 64 * 1024];
        make_bucket(&disk_stores, bucket).await;

        let options = ObjectOptions {
            versioned: true,
            ..Default::default()
        };
        let mut reader = PutObjReader::from_vec(payload.clone());
        temp_env::async_with_vars(
            [(
                crate::set_disk::core::io_primitives::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE,
                Some("false"),
            )],
            set_disks.put_object(bucket, object, &mut reader, &options),
        )
        .await
        .expect("versioned EC8+4 PUT should use the reduced inline budget");

        for (disk_index, disk) in disk_stores.iter().enumerate() {
            let file_info = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .unwrap_or_else(|err| panic!("disk {disk_index} should persist versioned EC8+4 metadata: {err}"));
            assert!(
                !file_info.inline_data(),
                "disk {disk_index} must keep the versioned shard outside xl.meta"
            );
        }

        let mut object_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &options)
            .await
            .expect("versioned non-inline EC8+4 object should remain readable");
        let mut restored = Vec::new();
        object_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("versioned non-inline EC8+4 object should stream");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    async fn inline_put_direct_commit_accepts_exact_quorum_and_rejects_quorum_minus_one() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "inline-direct-quorum";
        let exact_quorum_object = "exact-quorum.bin";
        let below_quorum_object = "below-quorum.bin";
        make_bucket(&disk_stores, bucket).await;
        {
            let mut disks = set_disks.disks.write().await;
            disks[3] = None;
        }

        let mut reader = PutObjReader::from_vec(vec![0x5a; 4 * 1024]);
        set_disks
            .put_object(bucket, exact_quorum_object, &mut reader, &ObjectOptions::default())
            .await
            .expect("three online disks should satisfy the four-disk write quorum");
        for (disk_index, disk) in disk_stores.iter().enumerate() {
            let persisted = disk
                .read_version("", bucket, exact_quorum_object, "", &ReadOptions::default())
                .await;
            assert_eq!(
                persisted.is_ok(),
                disk_index < 3,
                "exact-quorum commit should publish only on the three online disks"
            );
        }

        set_disks.disks.write().await[2] = None;
        let mut reader = PutObjReader::from_vec(vec![0xa5; 4 * 1024]);
        set_disks
            .put_object(bucket, below_quorum_object, &mut reader, &ObjectOptions::default())
            .await
            .expect_err("two online disks are one below the four-disk write quorum");

        for (disk_index, disk) in disk_stores.iter().enumerate() {
            assert!(
                disk.read_version("", bucket, below_quorum_object, "", &ReadOptions::default())
                    .await
                    .is_err(),
                "disk {disk_index} must not expose an object after pre-commit quorum failure"
            );
        }
    }

    #[tokio::test]
    async fn inline_put_direct_commit_handles_post_encode_rename_failures() {
        use crate::disk::health_state::RuntimeDriveHealthState;

        let payload = vec![0x5a; 4 * 1024];

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "inline-direct-post-encode-quorum";
        let object = "exact-quorum.bin";
        make_bucket(&disk_stores, bucket).await;
        let barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterNamespace);
        let put = {
            let set_disks = Arc::clone(&set_disks);
            let payload = payload.clone();
            tokio::spawn(async move {
                let mut reader = PutObjReader::from_vec(payload);
                set_disks
                    .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                    .await
            })
        };
        barrier.wait_until_paused().await;
        disk_stores[3].force_runtime_state_for_test(RuntimeDriveHealthState::Offline);
        barrier.release();
        put.await
            .expect("exact-quorum PUT task should complete")
            .expect("one post-encode rename failure should preserve write quorum");
        disk_stores[3].force_runtime_state_for_test(RuntimeDriveHealthState::Online);
        for (disk_index, disk) in disk_stores.iter().enumerate() {
            let persisted = disk.read_version("", bucket, object, "", &ReadOptions::default()).await;
            assert_eq!(
                persisted.is_ok(),
                disk_index < 3,
                "only disks that completed rename_data may publish the exact-quorum object"
            );
        }

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "inline-direct-post-encode-rollback";
        let object = "rollback.bin";
        let old_payload = vec![0x31; 4 * 1024];
        make_bucket(&disk_stores, bucket).await;
        let mut old_reader = PutObjReader::from_vec(old_payload.clone());
        set_disks
            .put_object(bucket, object, &mut old_reader, &ObjectOptions::default())
            .await
            .expect("old inline object should commit");
        let read_data = ReadOptions {
            read_data: true,
            ..Default::default()
        };
        let mut old_disk_data = Vec::with_capacity(disk_stores.len());
        for disk in &disk_stores {
            old_disk_data.push(
                disk.read_version("", bucket, object, "", &read_data)
                    .await
                    .expect("old inline shard should be readable before overwrite")
                    .data,
            );
        }

        let barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterNamespace);
        let put = {
            let set_disks = Arc::clone(&set_disks);
            let payload = payload.clone();
            tokio::spawn(async move {
                let mut reader = PutObjReader::from_vec(payload);
                set_disks
                    .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                    .await
            })
        };
        barrier.wait_until_paused().await;
        for disk in &disk_stores[2..] {
            disk.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);
        }
        barrier.release();
        put.await
            .expect("quorum-minus-one PUT task should complete")
            .expect_err("two post-encode rename failures must fail write quorum");
        for disk in &disk_stores[2..] {
            disk.force_runtime_state_for_test(RuntimeDriveHealthState::Online);
        }

        for (disk_index, disk) in disk_stores.iter().enumerate() {
            let restored = disk
                .read_version("", bucket, object, "", &read_data)
                .await
                .unwrap_or_else(|err| panic!("disk {disk_index} should retain the old inline object: {err}"));
            assert_eq!(restored.data, old_disk_data[disk_index]);
        }
        let mut object_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("old object should remain readable after quorum rollback");
        let mut restored = Vec::new();
        object_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("old object should stream after quorum rollback");
        assert_eq!(restored, old_payload);
    }

    #[tokio::test]
    async fn zero_length_put_keeps_existing_pipeline_layout_and_round_trips() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "zero-length-put";
        let object = "empty.bin";
        make_bucket(&disk_stores, bucket).await;

        let mut reader = PutObjReader::from_vec(Vec::new());
        // This test asserts the committed layout on every individual disk. A
        // lock-owning PUT may quorum-ack before its rename tail drains, so
        // keep the setup on the full-fanout commit path.
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
            .expect("zero-length PUT should commit through the existing pipeline");

        let read_data = ReadOptions {
            read_data: true,
            ..Default::default()
        };
        for (disk_index, disk) in disk_stores.iter().enumerate() {
            let file_info = disk
                .read_version("", bucket, object, "", &read_data)
                .await
                .unwrap_or_else(|err| panic!("disk {disk_index} should persist empty-object metadata: {err}"));
            assert_eq!(file_info.size, 0);
            assert_eq!(file_info.data.as_deref(), Some(&[][..]));
        }

        let mut object_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("empty object should be readable");
        let mut restored = Vec::new();
        object_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("empty object should stream");
        assert!(restored.is_empty());
    }
}

#[cfg(test)]
mod get_object_downstream_close_accounting_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks;
    use super::*;
    use crate::diagnostics::get::{
        GET_METADATA_EARLY_STOP_REASON_NOT_FOUND, GET_OBJECT_PATH_INTERNAL_META, GET_STAGE_DECODE, GET_STAGE_EMIT,
        GetObjectFailureReason,
    };
    use crate::disk::RUSTFS_META_BUCKET;
    use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::test_metrics::CapturingRecorder;
    use std::time::Duration;

    #[test]
    #[serial_test::serial]
    fn dropped_legacy_reader_does_not_record_emit_downstream_closed() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime should build");
        let recorder = CapturingRecorder::default();
        let previous_gate = rustfs_io_metrics::get_stage_metrics_enabled();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);

        let (decode_failures, emit_failures, legacy_fanout, internal_fanout) = metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let (_temp_dirs, _disk_stores, set_disks) = hermetic_set_disks(4).await;
                let bucket = "get-downstream-close-accounting";
                let object = "object.bin";
                let payload = b"downstream-close-accounting-".repeat(10_000);
                let options = ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                };

                set_disks
                    .make_bucket(bucket, &MakeBucketOptions::default())
                    .await
                    .expect("bucket should be created");
                let mut put_reader = PutObjReader::from_vec(payload.clone());
                set_disks
                    .put_object(bucket, object, &mut put_reader, &options)
                    .await
                    .expect("object should be written");

                let range = Some(HTTPRangeSpec {
                    is_suffix_length: false,
                    start: 0,
                    end: payload.len() as i64 - 1,
                });
                let reader = set_disks
                    .get_object_reader(bucket, object, range, HeaderMap::new(), &options)
                    .await
                    .expect("legacy reader should open");
                drop(reader);

                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let decode_failures = recorder.counter_value(
                            "rustfs_io_get_object_pipeline_failures_total",
                            &[
                                ("path", "legacy_duplex"),
                                ("stage", GET_STAGE_DECODE),
                                ("reason", GetObjectFailureReason::DownstreamClosed.as_str()),
                            ],
                        );
                        if decode_failures > 0 {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("dropped reader should reach the downstream-close producer path");

                (
                    recorder.counter_value(
                        "rustfs_io_get_object_pipeline_failures_total",
                        &[
                            ("path", "legacy_duplex"),
                            ("stage", GET_STAGE_DECODE),
                            ("reason", GetObjectFailureReason::DownstreamClosed.as_str()),
                        ],
                    ),
                    recorder.counter_value(
                        "rustfs_io_get_object_pipeline_failures_total",
                        &[
                            ("path", "legacy_duplex"),
                            ("stage", GET_STAGE_EMIT),
                            ("reason", GetObjectFailureReason::DownstreamClosed.as_str()),
                        ],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_fanout_total_responses",
                        &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_fanout_total_responses",
                        &[("path", GET_OBJECT_PATH_INTERNAL_META)],
                    ),
                )
            })
        });
        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_gate);

        assert!(decode_failures > 0, "the producer must expose the downstream close at decode");
        assert_eq!(emit_failures, 0, "downstream closure must not be counted as an emit failure");
        assert_eq!(legacy_fanout, vec![4.0], "ordinary object fanout must retain the legacy_duplex path");
        assert!(
            internal_fanout.is_empty(),
            "ordinary object fanout must not be attributed to internal_meta"
        );
    }

    #[test]
    #[serial_test::serial]
    fn missing_internal_meta_reader_records_internal_meta_path() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime should build");
        let recorder = CapturingRecorder::default();
        let previous_gate = rustfs_io_metrics::get_stage_metrics_enabled();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);

        let (
            internal_missing,
            legacy_unknown,
            internal_fanout,
            legacy_fanout,
            internal_scheduled,
            legacy_scheduled,
            internal_completed,
            legacy_completed,
            internal_cancelled,
            legacy_cancelled,
            internal_not_found_miss,
            legacy_not_found_miss,
            internal_saved,
            legacy_saved,
        ) = metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let (_temp_dirs, _disk_stores, set_disks) = hermetic_set_disks(4).await;
                let options = ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                };

                let result = set_disks
                    .get_object_reader(
                        RUSTFS_META_BUCKET,
                        "buckets/.usage-cache/nonexistent.bin",
                        None,
                        HeaderMap::new(),
                        &options,
                    )
                    .await;
                assert!(result.is_err(), "missing internal metadata must still return an error");

                (
                    recorder.counter_value(
                        "rustfs_io_get_object_pipeline_failures_total",
                        &[
                            ("path", GET_OBJECT_PATH_INTERNAL_META),
                            ("stage", GET_STAGE_METADATA),
                            ("reason", GetObjectFailureReason::MetadataMissing.as_str()),
                        ],
                    ),
                    recorder.counter_value(
                        "rustfs_io_get_object_pipeline_failures_total",
                        &[
                            ("path", GET_OBJECT_PATH_LEGACY_DUPLEX),
                            ("stage", GET_STAGE_METADATA),
                            ("reason", GetObjectFailureReason::Unknown.as_str()),
                        ],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_fanout_error_responses",
                        &[("path", GET_OBJECT_PATH_INTERNAL_META)],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_fanout_error_responses",
                        &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_fanout_scheduled",
                        &[("path", GET_OBJECT_PATH_INTERNAL_META)],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_fanout_scheduled",
                        &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_fanout_completed",
                        &[("path", GET_OBJECT_PATH_INTERNAL_META)],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_fanout_completed",
                        &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_fanout_cancelled",
                        &[("path", GET_OBJECT_PATH_INTERNAL_META)],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_fanout_cancelled",
                        &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)],
                    ),
                    recorder.counter_value(
                        "rustfs_io_get_object_metadata_early_stop_total",
                        &[
                            ("path", GET_OBJECT_PATH_INTERNAL_META),
                            ("decision", "miss"),
                            ("reason", GET_METADATA_EARLY_STOP_REASON_NOT_FOUND),
                        ],
                    ),
                    recorder.counter_value(
                        "rustfs_io_get_object_metadata_early_stop_total",
                        &[
                            ("path", GET_OBJECT_PATH_LEGACY_DUPLEX),
                            ("decision", "miss"),
                            ("reason", GET_METADATA_EARLY_STOP_REASON_NOT_FOUND),
                        ],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_early_stop_saved_responses",
                        &[("path", GET_OBJECT_PATH_INTERNAL_META)],
                    ),
                    recorder.histogram_values(
                        "rustfs_io_get_object_metadata_early_stop_saved_responses",
                        &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX)],
                    ),
                )
            })
        });
        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_gate);

        assert!(internal_missing > 0, "internal metadata miss must use the internal_meta path");
        assert_eq!(
            legacy_unknown, 0,
            "internal metadata miss must not be attributed to legacy_duplex/unknown"
        );
        assert_eq!(internal_fanout, vec![4.0], "internal metadata fanout must retain its path label");
        assert!(legacy_fanout.is_empty(), "internal metadata fanout must not leak into legacy_duplex");
        assert_eq!(
            internal_scheduled,
            vec![4.0],
            "internal metadata lifecycle scheduled count must retain its path label"
        );
        assert!(
            legacy_scheduled.is_empty(),
            "internal metadata lifecycle scheduled count must not leak into legacy_duplex"
        );
        assert_eq!(
            internal_completed,
            vec![4.0],
            "internal metadata lifecycle completed count must retain its path label"
        );
        assert!(
            legacy_completed.is_empty(),
            "internal metadata lifecycle completed count must not leak into legacy_duplex"
        );
        assert_eq!(
            internal_cancelled,
            vec![0.0],
            "internal metadata full-wait lifecycle must record zero cancellations"
        );
        assert!(
            legacy_cancelled.is_empty(),
            "internal metadata lifecycle cancelled count must not leak into legacy_duplex"
        );
        assert_eq!(
            internal_not_found_miss, 1,
            "internal metadata not-found early-stop miss must retain its path label"
        );
        assert_eq!(
            legacy_not_found_miss, 0,
            "internal metadata not-found early-stop miss must not leak into legacy_duplex"
        );
        assert_eq!(
            internal_saved,
            vec![0.0],
            "internal metadata not-found miss must record zero saved responses on internal_meta"
        );
        assert!(
            legacy_saved.is_empty(),
            "internal metadata not-found miss saved responses must not leak into legacy_duplex"
        );
    }
}

#[cfg(test)]
mod metadata_mutation_generation_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks_isolated as hermetic_set_disks;
    use super::*;
    use crate::disk::ReadOptions;

    async fn put_and_prime(
        set_disks: &Arc<SetDisks>,
        bucket: &str,
        object: &str,
        payload: &[u8],
    ) -> (ObjectInfo, GetObjectMetadataCacheKey) {
        let mut reader = PutObjReader::from_vec(payload.to_vec());
        // A lock-owning PUT may quorum-ack before its rename tail drains, and
        // cache priming refuses to publish while a straggler disk still reads
        // as an error; keep the setup on the full-fanout commit path.
        let info = set_disks
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
            .expect("test object should be written");
        // The publish is also bounded by the cache TTL, so re-prime until the
        // current generation is observably cached instead of asserting on a
        // single read that a loaded host can stall past expiry.
        let key = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                set_disks
                    .get_object_fileinfo(bucket, object, &ObjectOptions::default(), true, false)
                    .await
                    .expect("test object metadata should resolve");
                let generation = set_disks
                    .get_object_metadata_cache_generation(bucket, object)
                    .expect("metadata cache generation should be active");
                let key = GetObjectMetadataCacheKey::new(bucket, object, generation);
                if set_disks.get_object_metadata_cache.get(&key).await.is_some() {
                    return key;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("metadata priming should publish the current generation");
        (info, key)
    }

    async fn assert_retired(set_disks: &SetDisks, key: &GetObjectMetadataCacheKey) {
        set_disks.get_object_metadata_cache.run_pending_tasks().await;
        assert!(
            set_disks.get_object_metadata_cache.get(key).await.is_none(),
            "the mutation must physically retire the prior metadata generation"
        );
    }

    fn replication_metadata(timestamp: Option<&str>, mutation_id: Option<&str>, status: &str) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        if let Some(timestamp) = timestamp {
            rustfs_utils::http::insert_str(
                &mut metadata,
                rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,
                timestamp.to_string(),
            );
        }
        if let Some(mutation_id) = mutation_id {
            rustfs_utils::http::insert_str(
                &mut metadata,
                rustfs_utils::http::SUFFIX_REPLICATION_GENERATION,
                mutation_id.to_string(),
            );
        }
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_REPLICATION_STATUS, status.to_string());
        metadata
    }

    async fn seed_single_mixed_case_minio_generation(
        set_disks: &Arc<SetDisks>,
        bucket: &str,
        object: &str,
        category_suffix: &str,
        category_key: &str,
    ) {
        let mut reader = PutObjReader::from_vec(format!("payload for {object}").into_bytes());
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let (mut fi, _, disks) = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("source file metadata should be readable")
            .into_owned();
        for suffix in [
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,
            rustfs_utils::http::SUFFIX_REPLICATION_STATUS,
            category_suffix,
        ] {
            rustfs_utils::http::remove_str(&mut fi.metadata, suffix);
        }
        fi.metadata
            .insert("X-Minio-Internal-Replication-Timestamp".to_string(), "2026-09-03T00:00:00Z".to_string());
        fi.metadata
            .insert("X-Minio-Internal-Replication-Status".to_string(), "arn:test=PENDING;".to_string());
        fi.metadata
            .insert(category_key.to_string(), "2026-09-03T00:00:01Z".to_string());
        rebuild_file_info_replication_state(&mut fi);
        set_disks
            .update_object_meta(bucket, object, fi, &disks)
            .await
            .expect("mixed-case legacy metadata should be persisted");
        set_disks.invalidate_get_object_metadata_cache(bucket, object).await;

        let seeded = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("mixed-case legacy object should remain readable");
        assert!(!seeded.replication_generation_snapshot().invalid);
    }

    fn assert_dual_alias_value(metadata: &HashMap<String, String>, suffix: &str, expected: &str) {
        let aliases = metadata
            .iter()
            .filter(|(key, _)| rustfs_utils::http::has_internal_suffix(key, suffix))
            .collect::<Vec<_>>();
        assert_eq!(aliases.len(), 2, "{suffix} must be rewritten as exactly two canonical aliases");
        assert!(aliases.iter().all(|(_, value)| value.as_str() == expected));
        assert!(metadata.contains_key(&format!("x-rustfs-internal-{suffix}")));
        assert!(metadata.contains_key(&format!("x-minio-internal-{suffix}")));
    }

    #[tokio::test]
    async fn replication_status_writeback_cas_accepts_suspended_null_version_snapshot() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "replication-suspended-null-version-cas";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let timestamp = "opaque-suspended-null-version-time";
        let mutation_id = Uuid::from_u128(700).to_string();
        let mut reader = PutObjReader::from_vec(b"suspended null version".to_vec());
        let queued = set_disks
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    version_suspended: true,
                    user_defined: replication_metadata(Some(timestamp), Some(&mutation_id), "arn:test=PENDING;"),
                    ..Default::default()
                },
            )
            .await
            .expect("suspended-bucket null version should be written");
        assert_eq!(queued.version_id, Some(Uuid::nil()));
        let expected_generation = queued.replication_generation_snapshot();

        let reread = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("the same xl.meta should be readable without versioning flags");
        assert_eq!(reread.version_id, None);
        assert_eq!(reread.replication_generation_snapshot(), expected_generation);

        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(Uuid::nil().to_string()),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation: expected_generation.clone(),
                        mode: ReplicationStatusWritebackMode::ValidateOnly,
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect("ValidateOnly must accept the same suspended null-version identity");

        let completed = set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(Uuid::nil().to_string()),
                    eval_metadata: Some(replication_metadata(None, None, "arn:test=COMPLETED;")),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect("the queued null-version snapshot must match the storage reread");
        assert_eq!(completed.replication_status_internal.as_deref(), Some("arn:test=COMPLETED;"));
    }

    #[tokio::test]
    async fn replication_status_writeback_cas_applies_current_rejects_stale_and_supports_legacy_absence() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "replication-status-writeback-cas";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let object = "generation.txt";
        let generation_a = "2026-09-02T12:34:56.123456789+08:00[Asia/Shanghai]";
        let generation_b = generation_a;
        let mutation_a = Uuid::from_u128(1).to_string();
        let mutation_b = Uuid::from_u128(2).to_string();
        let mut reader = PutObjReader::from_vec(b"generation-fenced".to_vec());
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(Some(generation_a), Some(&mutation_a), "arn:test=PENDING;")),
                    ..Default::default()
                },
            )
            .await
            .expect("generation A should be installed");
        let generation_a_snapshot = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("generation A snapshot should be readable")
            .replication_generation_snapshot();

        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(None, None, "arn:test=COMPLETED;")),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation: generation_a_snapshot.clone(),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect("the worker that observed generation A should publish its terminal status");
        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("generation A terminal status should be readable");
        assert_eq!(current.replication_status_internal.as_deref(), Some("arn:test=COMPLETED;"));
        assert_eq!(
            rustfs_utils::http::get_str(current.user_defined.as_ref(), rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,)
                .as_deref(),
            Some(generation_a)
        );

        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(Some(generation_b), Some(&mutation_b), "arn:test=PENDING;")),
                    ..Default::default()
                },
            )
            .await
            .expect("a newer metadata mutation should install generation B");
        let generation_b_snapshot = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("generation B snapshot should be readable")
            .replication_generation_snapshot();
        let stale_error = set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(None, None, "arn:test=COMPLETED;")),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation: generation_a_snapshot,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect_err("generation A must not overwrite generation B's PENDING status");
        assert!(matches!(stale_error, Error::PreconditionFailed));
        let after_stale = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("generation B should remain readable");
        assert_eq!(after_stale.replication_status_internal.as_deref(), Some("arn:test=PENDING;"));
        assert_eq!(
            rustfs_utils::http::get_str(after_stale.user_defined.as_ref(), rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,)
                .as_deref(),
            Some(generation_b)
        );

        set_disks
            .put_object_tags(
                bucket,
                object,
                "stage=legacy-writer",
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(Some(generation_b), None, "arn:test=PENDING;")),
                    ..Default::default()
                },
            )
            .await
            .expect("a legacy metadata writer should preserve the unknown mutation id");
        let legacy_writer_error = set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(None, None, "arn:test=COMPLETED;")),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation: generation_b_snapshot,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a preserved UUID and timestamp must not hide a legacy writer's payload change");
        assert!(matches!(legacy_writer_error, Error::PreconditionFailed));

        let legacy_object = "legacy-without-generation.txt";
        let mut legacy_reader = PutObjReader::from_vec(b"legacy".to_vec());
        set_disks
            .put_object(bucket, legacy_object, &mut legacy_reader, &ObjectOptions::default())
            .await
            .expect("legacy source object should be written");
        let legacy_snapshot = set_disks
            .get_object_info(bucket, legacy_object, &ObjectOptions::default())
            .await
            .expect("legacy snapshot should be readable")
            .replication_generation_snapshot();
        set_disks
            .put_object_metadata(
                bucket,
                legacy_object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(None, None, "arn:test=COMPLETED;")),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation: legacy_snapshot,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect("legacy absence must compare as an exact generation value");
        let legacy = set_disks
            .get_object_info(bucket, legacy_object, &ObjectOptions::default())
            .await
            .expect("legacy terminal status should be readable");
        assert_eq!(legacy.replication_status_internal.as_deref(), Some("arn:test=COMPLETED;"));
        assert!(
            rustfs_utils::http::get_str(legacy.user_defined.as_ref(), rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,)
                .is_none()
        );

        let timestamp_only_object = "legacy-timestamp-only.txt";
        let mut timestamp_only_reader = PutObjReader::from_vec(b"timestamp only".to_vec());
        set_disks
            .put_object(bucket, timestamp_only_object, &mut timestamp_only_reader, &ObjectOptions::default())
            .await
            .expect("timestamp-only source object should be written");
        set_disks
            .put_object_metadata(
                bucket,
                timestamp_only_object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(Some(generation_a), None, "arn:test=PENDING;")),
                    ..Default::default()
                },
            )
            .await
            .expect("legacy timestamp-only generation should be installed");
        let timestamp_only_snapshot = set_disks
            .get_object_info(bucket, timestamp_only_object, &ObjectOptions::default())
            .await
            .expect("legacy timestamp-only snapshot should be readable")
            .replication_generation_snapshot();
        assert_eq!(timestamp_only_snapshot.timestamp.as_deref(), Some(generation_a));
        assert_eq!(timestamp_only_snapshot.mutation_id, None);
        set_disks
            .put_object_metadata(
                bucket,
                timestamp_only_object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(None, None, "arn:test=COMPLETED;")),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation: timestamp_only_snapshot,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect("legacy timestamp-only generation should remain writable through exact CAS");
    }

    #[tokio::test]
    async fn metadata_mutations_canonicalize_mixed_case_minio_generation_aliases() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "replication-mixed-case-generation";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let tag_object = "tagging.txt";
        seed_single_mixed_case_minio_generation(
            &set_disks,
            bucket,
            tag_object,
            rustfs_utils::http::SUFFIX_TAGGING_TIMESTAMP,
            "X-Minio-Internal-Tagging-Timestamp",
        )
        .await;
        let tag_timestamp = "2026-09-03T01:00:00Z";
        let tag_mutation = Uuid::from_u128(101).to_string();
        let mut tag_eval = replication_metadata(Some(tag_timestamp), Some(&tag_mutation), "arn:test=PENDING;");
        rustfs_utils::http::insert_str(
            &mut tag_eval,
            rustfs_utils::http::SUFFIX_TAGGING_TIMESTAMP,
            "2026-09-03T01:00:01Z".to_string(),
        );
        let tagged = set_disks
            .put_object_tags(
                bucket,
                tag_object,
                "stage=canonical",
                &ObjectOptions {
                    eval_metadata: Some(tag_eval),
                    ..Default::default()
                },
            )
            .await
            .expect("tag mutation should canonicalize legacy aliases");
        assert_dual_alias_value(
            tagged.user_defined.as_ref(),
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,
            tag_timestamp,
        );
        assert_dual_alias_value(
            tagged.user_defined.as_ref(),
            rustfs_utils::http::SUFFIX_REPLICATION_STATUS,
            "arn:test=PENDING;",
        );
        assert_dual_alias_value(
            tagged.user_defined.as_ref(),
            rustfs_utils::http::SUFFIX_TAGGING_TIMESTAMP,
            "2026-09-03T01:00:01Z",
        );
        let tag_snapshot = tagged.replication_generation_snapshot();
        assert!(!tag_snapshot.invalid);
        let persisted_tag_snapshot = set_disks
            .get_object_info(bucket, tag_object, &ObjectOptions::default())
            .await
            .expect("canonicalized tag metadata should be reread")
            .replication_generation_snapshot();
        assert_eq!(tag_snapshot, persisted_tag_snapshot);
        set_disks
            .put_object_metadata(
                bucket,
                tag_object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(None, None, "arn:test=COMPLETED;")),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation: tag_snapshot,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect("the canonicalized tag generation should accept terminal CAS");

        let metadata_object = "retention-metadata.txt";
        seed_single_mixed_case_minio_generation(
            &set_disks,
            bucket,
            metadata_object,
            rustfs_utils::http::SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP,
            "X-Minio-Internal-Objectlock-Retention-Timestamp",
        )
        .await;
        let metadata_timestamp = "2026-09-03T02:00:00Z";
        let metadata_mutation = Uuid::from_u128(102).to_string();
        let mut metadata_eval = replication_metadata(Some(metadata_timestamp), Some(&metadata_mutation), "arn:test=PENDING;");
        rustfs_utils::http::insert_str(
            &mut metadata_eval,
            rustfs_utils::http::SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP,
            "2026-09-03T02:00:01Z".to_string(),
        );
        metadata_eval.insert("x-amz-meta-proof".to_string(), "updated".to_string());
        let updated = set_disks
            .put_object_metadata(
                bucket,
                metadata_object,
                &ObjectOptions {
                    eval_metadata: Some(metadata_eval),
                    ..Default::default()
                },
            )
            .await
            .expect("generic metadata mutation should canonicalize legacy aliases");
        assert_dual_alias_value(
            updated.user_defined.as_ref(),
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,
            metadata_timestamp,
        );
        assert_dual_alias_value(
            updated.user_defined.as_ref(),
            rustfs_utils::http::SUFFIX_REPLICATION_STATUS,
            "arn:test=PENDING;",
        );
        assert_dual_alias_value(
            updated.user_defined.as_ref(),
            rustfs_utils::http::SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP,
            "2026-09-03T02:00:01Z",
        );
        let metadata_snapshot = updated.replication_generation_snapshot();
        assert!(!metadata_snapshot.invalid);
        let failed = set_disks
            .put_object_metadata(
                bucket,
                metadata_object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(None, None, "arn:test=FAILED;")),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation: metadata_snapshot,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect("the canonicalized generic metadata generation should accept terminal CAS");
        assert_eq!(failed.replication_status_internal.as_deref(), Some("arn:test=FAILED;"));
    }

    #[tokio::test]
    #[serial_test::serial(metadata_cache_invalidation_probe)]
    async fn replication_status_writeback_validate_only_is_zero_write_and_checks_fences() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "replication-status-validate-only";
        let object = "failed-retry.txt";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(b"failed retry".to_vec());
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let timestamp = "2026-09-02T12:34:56.123456789+08:00[Asia/Shanghai]";
        let mutation_id = Uuid::from_u128(10).to_string();
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(Some(timestamp), Some(&mutation_id), "arn:test=FAILED;")),
                    ..Default::default()
                },
            )
            .await
            .expect("failed generation should be installed");
        let snapshot = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("failed generation should be readable")
            .replication_generation_snapshot();

        let invalidations = MetadataCacheInvalidationProbe::install(bucket, object);
        let validated = set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    eval_metadata: Some(replication_metadata(None, None, "arn:test=COMPLETED;")),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation: snapshot.clone(),
                        mode: ReplicationStatusWritebackMode::ValidateOnly,
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect("the current FAILED generation should validate without a rewrite");
        assert_eq!(validated.replication_status_internal.as_deref(), Some("arn:test=FAILED;"));
        assert_eq!(
            invalidations.count(),
            0,
            "validate-only must not invalidate metadata cache or enter a write path"
        );

        let mut stale = snapshot.clone();
        stale.mutation_id = Some(Uuid::from_u128(11).to_string());
        let stale_error = set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(replication_metadata(None, None, "arn:test=COMPLETED;")),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation: stale,
                        mode: ReplicationStatusWritebackMode::ValidateOnly,
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a stale FAILED retry must be superseded during validate-only");
        assert!(matches!(stale_error, Error::PreconditionFailed));
        assert_eq!(invalidations.count(), 0, "a rejected validate-only CAS must not invalidate cache");

        let lost_fence_error = set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    namespace_lock_fence: Some(NamespaceLockFence::lost_for_test()),
                    replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                        expected_generation: snapshot,
                        mode: ReplicationStatusWritebackMode::ValidateOnly,
                    })),
                    ..Default::default()
                },
            )
            .await
            .expect_err("validate-only must not report success after its namespace fence is lost");
        assert!(matches!(lost_fence_error, StorageError::NamespaceLockQuorumUnavailable { .. }));
        assert_eq!(invalidations.count(), 0, "a fenced validate-only path must remain side-effect free");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn replication_terminal_cas_quorum_failure_never_deletes_surviving_metadata() {
        temp_env::async_with_vars([("RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS", Some("0"))], async {
            let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "replication-terminal-quorum-failure";
            let object = "survivor.txt";
            for disk in &disk_stores {
                disk.make_volume(bucket).await.expect("bucket volume should be created");
            }

            let mut reader = PutObjReader::from_vec(b"surviving metadata".to_vec());
            set_disks
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
                .expect("source object should be written");
            let expected_generation = set_disks
                .get_object_info(bucket, object, &ObjectOptions::default())
                .await
                .expect("source generation should be readable")
                .replication_generation_snapshot();

            let meta_paths = temp_dirs
                .iter()
                .map(|temp_dir| temp_dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE))
                .collect::<Vec<_>>();
            let surviving_before = tokio::fs::read(&meta_paths[3])
                .await
                .expect("the surviving xl.meta should exist before the terminal attempt");
            for path in &meta_paths[..3] {
                tokio::fs::remove_file(path)
                    .await
                    .expect("three xl.meta copies should be removed to force read-quorum failure");
            }

            set_disks
                .put_object_metadata(
                    bucket,
                    object,
                    &ObjectOptions {
                        replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                            expected_generation: expected_generation.clone(),
                            mode: ReplicationStatusWritebackMode::ValidateOnly,
                        })),
                        ..Default::default()
                    },
                )
                .await
                .expect_err("terminal CAS must retry when current metadata lacks read quorum");

            let surviving_after_validate = tokio::fs::read(&meta_paths[3])
                .await
                .expect("terminal CAS must not delete the final xl.meta copy");
            assert_eq!(
                surviving_after_validate, surviving_before,
                "ValidateOnly must remain zero-write on quorum failure"
            );

            set_disks
                .put_object_metadata(
                    bucket,
                    object,
                    &ObjectOptions {
                        namespace_lock_fence: Some(NamespaceLockFence::lost_for_test()),
                        replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
                            expected_generation,
                            mode: ReplicationStatusWritebackMode::Update,
                        })),
                        ..Default::default()
                    },
                )
                .await
                .expect_err("a lost-fence terminal update must retry when current metadata lacks read quorum");

            let surviving_after_lost_fence = tokio::fs::read(&meta_paths[3])
                .await
                .expect("a lost-fence terminal update must not delete the final xl.meta copy");
            assert_eq!(
                surviving_after_lost_fence, surviving_before,
                "a lost outer fence must remain zero-write on quorum failure"
            );
        })
        .await;
    }

    async fn persist_part_checksum_sidecar(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, value: &str) {
        let (fi, _, disks) = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("object metadata should be readable before adding the checksum sidecar")
            .into_owned();
        let mut fi = fi;
        rustfs_utils::http::insert_str(&mut fi.metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS, value.to_string());
        set_disks
            .update_object_meta(bucket, object, fi, &disks)
            .await
            .expect("checksum sidecar should be persisted");
        set_disks.invalidate_get_object_metadata_cache(bucket, object).await;
    }

    #[tokio::test]
    #[serial_test::serial(metadata_cache_invalidation_probe)]
    async fn segment_observation_equal_size_mutations_retire_metadata_generation() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "segment-observation-bucket";
        let object = "hot/object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("create segment fixture bucket");
        }
        let (before, old_key) = put_and_prime(&set_disks, bucket, object, b"before").await;
        let probe = MetadataCacheInvalidationProbe::install(bucket, object);
        let mut replacement = PutObjReader::from_vec(b"after!".to_vec());
        set_disks
            .put_object(bucket, object, &mut replacement, &ObjectOptions::default())
            .await
            .expect("commit same-length replacement with normal owner locking");
        assert_eq!(probe.count(), 2, "same-length PUT must retire its metadata generation");
        assert_retired(&set_disks, &old_key).await;
        drop(probe);
        let after = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("read replacement metadata");
        assert_eq!(before.size, after.size);
        assert_ne!(before.etag, after.etag, "equal size is not equal content");
        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("read replacement body through the owner");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("drain replacement body");
        assert_eq!(body, b"after!");
        drop(reader);

        let (before, old_key) = put_and_prime(&set_disks, bucket, object, b"after!").await;
        let probe = MetadataCacheInvalidationProbe::install(bucket, object);
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(HashMap::from([("x-amz-meta-segment".to_string(), "changed".to_string())])),
                    ..Default::default()
                },
            )
            .await
            .expect("commit metadata-only mutation with normal owner locking");
        assert_eq!(probe.count(), 4, "metadata-only mutation must retire both owner fences");
        assert_retired(&set_disks, &old_key).await;
        let after = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("read committed metadata-only mutation");
        assert_eq!(before.size, after.size);
        assert_eq!(before.etag, after.etag);
        assert!(!before.user_defined.contains_key("x-amz-meta-segment"));
        assert_eq!(after.user_defined.get("x-amz-meta-segment").map(String::as_str), Some("changed"));
    }

    #[tokio::test]
    #[serial_test::serial(metadata_cache_invalidation_probe)]
    async fn metadata_semantic_mutation_generation_matrix_retires_cached_snapshot() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "metadata-mutation-generation-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let put_object = "put-object";
        let (_, put_key) = put_and_prime(&set_disks, bucket, put_object, b"initial PUT body").await;
        let put_probe = MetadataCacheInvalidationProbe::install(bucket, put_object);
        let mut replacement = PutObjReader::from_vec(b"replacement PUT body".to_vec());
        set_disks
            .put_object(bucket, put_object, &mut replacement, &ObjectOptions::default())
            .await
            .expect("replacement PUT should succeed");
        assert_eq!(put_probe.count(), 2, "PUT must invalidate before mutation and after commit");
        assert_retired(&set_disks, &put_key).await;
        drop(put_probe);

        let delete_object = "delete-object";
        let (_, delete_key) = put_and_prime(&set_disks, bucket, delete_object, b"DELETE body").await;
        let delete_probe = MetadataCacheInvalidationProbe::install(bucket, delete_object);
        set_disks
            .delete_object(
                bucket,
                delete_object,
                ObjectOptions {
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                },
            )
            .await
            .expect("DELETE should succeed");
        assert_eq!(delete_probe.count(), 2, "DELETE must invalidate before mutation and after commit");
        assert_retired(&set_disks, &delete_key).await;
        drop(delete_probe);

        let copy_object = "copy-object";
        let (mut copy_info, copy_key) = put_and_prime(&set_disks, bucket, copy_object, b"COPY body").await;
        copy_info.metadata_only = true;
        Arc::make_mut(&mut copy_info.user_defined).insert("x-amz-meta-copy".to_string(), "updated".to_string());
        let copy_probe = MetadataCacheInvalidationProbe::install(bucket, copy_object);
        set_disks
            .copy_object(
                bucket,
                copy_object,
                bucket,
                copy_object,
                &mut copy_info,
                &ObjectOptions::default(),
                &ObjectOptions::default(),
            )
            .await
            .expect("metadata COPY should succeed");
        assert_eq!(
            copy_probe.count(),
            4,
            "metadata COPY must retain both outer and update-object-meta fences"
        );
        assert_retired(&set_disks, &copy_key).await;
        drop(copy_probe);

        let metadata_object = "metadata-object";
        let (_, metadata_key) = put_and_prime(&set_disks, bucket, metadata_object, b"metadata body").await;
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-meta-updated".to_string(), "true".to_string());
        let metadata_opts = ObjectOptions {
            eval_metadata: Some(metadata),
            ..Default::default()
        };
        let metadata_probe = MetadataCacheInvalidationProbe::install(bucket, metadata_object);
        set_disks
            .put_object_metadata(bucket, metadata_object, &metadata_opts)
            .await
            .expect("metadata PUT should succeed");
        assert_eq!(
            metadata_probe.count(),
            4,
            "metadata PUT must retain both outer and update-object-meta fences"
        );
        assert_retired(&set_disks, &metadata_key).await;
    }

    #[tokio::test]
    async fn metadata_only_copy_preserves_valid_part_checksums_and_rejects_conflicting_aliases() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "metadata-copy-part-checksums-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let valid_object = "valid-sidecar";
        let (mut valid_source, _) = put_and_prime(&set_disks, bucket, valid_object, b"valid sidecar body").await;
        persist_part_checksum_sidecar(&set_disks, bucket, valid_object, r#"[[1,[["CRC32C","AAAAAA=="]]]]"#).await;
        valid_source.metadata_only = true;
        Arc::make_mut(&mut valid_source.user_defined).insert("x-amz-meta-copy".to_string(), "updated".to_string());
        set_disks
            .copy_object(
                bucket,
                valid_object,
                bucket,
                valid_object,
                &mut valid_source,
                &ObjectOptions::default(),
                &ObjectOptions::default(),
            )
            .await
            .expect("metadata-only copy should preserve a valid checksum sidecar");
        let copied = set_disks
            .get_object_info(
                bucket,
                valid_object,
                &ObjectOptions {
                    include_part_checksums: true,
                    ..Default::default()
                },
            )
            .await
            .expect("copied object should retain readable part checksums");
        assert_eq!(
            copied.parts[0]
                .checksums
                .as_ref()
                .and_then(|checksums| checksums.get("CRC32C"))
                .map(String::as_str),
            Some("AAAAAA==")
        );

        let conflicting_object = "conflicting-sidecar";
        let (mut conflicting_source, _) =
            put_and_prime(&set_disks, bucket, conflicting_object, b"conflicting sidecar body").await;
        let (fi, _, disks) = set_disks
            .get_object_fileinfo(
                bucket,
                conflicting_object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("conflicting object metadata should be readable before corruption is injected")
            .into_owned();
        let mut fi = fi;
        let rustfs_key = format!(
            "{}{}",
            rustfs_utils::http::RUSTFS_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_PART_CHECKSUMS
        );
        let minio_key = format!(
            "{}{}",
            rustfs_utils::http::MINIO_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_PART_CHECKSUMS
        );
        fi.metadata
            .insert(rustfs_key.clone(), r#"[[1,[["CRC32C","AAAAAA=="]]]]"#.to_string());
        fi.metadata
            .insert(minio_key.clone(), r#"[[1,[["CRC32C","AQAAAA=="]]]]"#.to_string());
        set_disks
            .update_object_meta(bucket, conflicting_object, fi, &disks)
            .await
            .expect("conflicting aliases should be persisted for the fail-closed regression");
        set_disks
            .invalidate_get_object_metadata_cache(bucket, conflicting_object)
            .await;
        conflicting_source.metadata_only = true;
        let err = set_disks
            .copy_object(
                bucket,
                conflicting_object,
                bucket,
                conflicting_object,
                &mut conflicting_source,
                &ObjectOptions::default(),
                &ObjectOptions::default(),
            )
            .await
            .expect_err("metadata-only copy must reject conflicting checksum aliases");
        assert!(matches!(err, Error::FileCorrupt));
        let raw_err = disk_stores[0]
            .read_version("", bucket, conflicting_object, "", &ReadOptions::default())
            .await
            .expect_err("the rejected copy must leave the conflicting persisted aliases fail-closed");
        assert!(matches!(raw_err, crate::disk::error::DiskError::FileCorrupt));
    }
}

#[cfg(all(test, feature = "test-util"))]
mod transition_commit_failure_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks;
    use super::*;
    use crate::bucket::lifecycle::lifecycle::{TRANSITION_COMPLETE, TRANSITION_PENDING, TransitionOptions};
    use crate::services::tier::test_util::{MockWarmBackend, register_mock_tier};
    use crate::services::tier::tier::TierConfigMgr;
    use http::HeaderMap;
    use rustfs_filemeta::{RestoreStatusOps as _, parse_restore_obj_status};
    use s3s::dto::RestoreRequest;
    use tokio::io::AsyncReadExt;

    pub(super) fn restore_operation_id_metadata(operation_id: Uuid) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_id.to_string(),
        );
        metadata
    }

    pub(super) fn restore_metadata(operation_id: Uuid, ongoing: bool) -> HashMap<String, String> {
        let mut metadata = restore_operation_id_metadata(operation_id);
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_WORKER_LOCK,
            rustfs_utils::http::metadata_compat::RESTORE_WORKER_LOCK_PROTOCOL_V1.to_string(),
        );
        metadata.insert(s3s::header::X_AMZ_RESTORE.as_str().to_string(), format!("ongoing-request=\"{ongoing}\""));
        metadata
    }

    #[test]
    fn restore_tier_mutation_retry_backoff_is_bounded() {
        assert_eq!(restore_tier_mutation_retry_delay(0), Duration::from_millis(250));
        assert_eq!(restore_tier_mutation_retry_delay(1), Duration::from_millis(500));
        assert_eq!(restore_tier_mutation_retry_delay(4), Duration::from_secs(4));
        assert_eq!(restore_tier_mutation_retry_delay(5), RESTORE_TIER_MUTATION_RETRY_CAP);
        assert_eq!(restore_tier_mutation_retry_delay(u32::MAX), RESTORE_TIER_MUTATION_RETRY_CAP);
    }

    #[tokio::test]
    async fn rejected_unsupported_remote_versions_are_cleaned_up() {
        for remote_version in ["null", "opaque-version-token"] {
            let manager = TierConfigMgr::new();
            let backend = register_mock_tier(&manager, "WARM").await;
            let lease = TierConfigMgr::acquire_operation_lease(&manager, "WARM")
                .await
                .expect("mock tier lease should be available");
            let candidate = TransitionUploadCandidate::from_put_response(remote_version.to_string());

            persisted_transition_version(candidate.remote_version()).expect_err("unsupported writer version must fail closed");
            cleanup_rejected_transition_upload_durably(
                &lease,
                "remote/object",
                candidate.cleanup_version(),
                candidate.cleanup_version_is_exact(),
                None,
            )
            .await
            .expect("rejected remote upload must be cleaned up");

            assert_eq!(
                backend.remove_versions().await,
                vec![("remote/object".to_string(), candidate.cleanup_version().to_string())]
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial(restore_multipart_failure_point)]
    async fn multipart_restore_aborts_every_post_create_failure() {
        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        RESTORE_MULTIPART_ABORT_ATTEMPTS.store(0, Ordering::Relaxed);
        let bucket = "restore-multipart-failure-cleanup-bucket";
        let object = "object.bin";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let source_upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("source multipart upload should be created");
        let mut first_reader = PutObjReader::from_vec(vec![b'a'; 5 * 1024 * 1024]);
        let first = set_disks
            .put_object_part(bucket, object, &source_upload.upload_id, 1, &mut first_reader, &ObjectOptions::default())
            .await
            .expect("first source part should be staged");
        let mut second_reader = PutObjReader::from_vec(vec![b'b'; 1024 * 1024]);
        let second = set_disks
            .put_object_part(bucket, object, &source_upload.upload_id, 2, &mut second_reader, &ObjectOptions::default())
            .await
            .expect("second source part should be staged");
        let original = set_disks
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &source_upload.upload_id,
                vec![
                    CompletePart {
                        part_num: first.part_num,
                        etag: first.etag,
                        ..Default::default()
                    },
                    CompletePart {
                        part_num: second.part_num,
                        etag: second.etag,
                        ..Default::default()
                    },
                ],
                &ObjectOptions::default(),
            )
            .await
            .expect("source multipart upload should complete");
        let (source_fi, _, online_disks) = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("source metadata should be readable before adding the checksum sidecar")
            .into_owned();
        let mut source_fi = source_fi;
        rustfs_utils::http::insert_str(
            &mut source_fi.metadata,
            rustfs_utils::http::SUFFIX_PART_CHECKSUMS,
            r#"[[1,[["CRC32C","AAAAAA=="]]],[2,[["CRC32C","AQAAAA=="]]]]"#.to_string(),
        );
        set_disks
            .update_object_meta(bucket, object, source_fi, &online_disks)
            .await
            .expect("source checksum sidecar should be persisted before transition");
        set_disks.invalidate_get_object_metadata_cache(bucket, object).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        set_disks
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name,
                        etag: original.etag.clone().unwrap_or_default(),
                        ..Default::default()
                    },
                    version_id: original.version_id.map(|version| version.to_string()),
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("multipart source should transition before restore");

        for point in [
            RestoreMultipartFailurePoint::InvalidPartSize,
            RestoreMultipartFailurePoint::RangeOverflow,
            RestoreMultipartFailurePoint::TierGet,
            RestoreMultipartFailurePoint::HashReader,
            RestoreMultipartFailurePoint::PutPart,
            RestoreMultipartFailurePoint::SizeMismatch,
            RestoreMultipartFailurePoint::Complete,
        ] {
            *RESTORE_MULTIPART_FAILURE_POINT
                .lock()
                .expect("restore multipart failure-point lock must not be poisoned") = Some(point);
            *RESTORE_MULTIPART_UPLOAD_ID
                .lock()
                .expect("restore multipart upload-id lock must not be poisoned") = None;
            let operation_id = Uuid::new_v4();
            set_disks
                .put_object_metadata(
                    bucket,
                    object,
                    &ObjectOptions {
                        eval_metadata: Some(restore_metadata(operation_id, true)),
                        ..Default::default()
                    },
                )
                .await
                .expect("the injected restore generation should be installed");
            let mut opts = ObjectOptions::default();
            opts.transition.restore_request.days = Some(1);
            opts.user_defined = restore_operation_id_metadata(operation_id);
            set_disks
                .clone()
                .restore_transitioned_object(bucket, object, &opts)
                .await
                .expect_err("injected post-create restore failure must surface");
            let upload_id = RESTORE_MULTIPART_UPLOAD_ID
                .lock()
                .expect("restore multipart upload-id lock must not be poisoned")
                .clone()
                .expect("restore must create an upload before the injected failure");
            let err = set_disks
                .get_multipart_info(bucket, object, &upload_id, &ObjectOptions::default())
                .await
                .expect_err("failed restore upload must immediately disappear");
            assert!(is_err_invalid_upload_id(&err), "{point:?}: unexpected upload lookup error: {err:?}");
            let upload_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
            for temp_dir in &temp_dirs {
                assert!(
                    !temp_dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&upload_path).exists(),
                    "{point:?}: failed restore must remove staged multipart data"
                );
            }
            let cleaned = set_disks
                .get_object_info(bucket, object, &ObjectOptions::default())
                .await
                .expect("failed restore cleanup should leave the transitioned object readable");
            assert!(
                !cleaned.user_defined.contains_key(s3s::header::X_AMZ_RESTORE.as_str()),
                "{point:?}: every post-snapshot failure must clean the public ongoing marker"
            );
            assert!(
                rustfs_utils::http::get_str(cleaned.user_defined.as_ref(), rustfs_utils::http::SUFFIX_RESTORE_OPERATION_ID,)
                    .is_none(),
                "{point:?}: every post-snapshot failure must clean its operation generation"
            );
            assert!(
                rustfs_utils::http::get_str(cleaned.user_defined.as_ref(), rustfs_utils::http::SUFFIX_RESTORE_WORKER_LOCK,)
                    .is_none(),
                "{point:?}: every post-snapshot failure must clean its liveness marker"
            );
        }
        assert_eq!(
            RESTORE_MULTIPART_ABORT_ATTEMPTS.load(Ordering::Relaxed),
            7,
            "every injected post-create failure must attempt an abort"
        );
        *RESTORE_MULTIPART_FAILURE_POINT
            .lock()
            .expect("restore multipart failure-point lock must not be poisoned") = None;
        let operation_id = Uuid::new_v4();
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(restore_metadata(operation_id, true)),
                    ..Default::default()
                },
            )
            .await
            .expect("the successful restore generation should be installed");
        let mut opts = ObjectOptions::default();
        opts.transition.restore_request.days = Some(1);
        opts.user_defined = restore_operation_id_metadata(operation_id);
        set_disks
            .clone()
            .restore_transitioned_object(bucket, object, &opts)
            .await
            .expect("multipart restore should complete after failure injection is cleared");
        assert_eq!(
            RESTORE_MULTIPART_ABORT_ATTEMPTS.load(Ordering::Relaxed),
            7,
            "successful multipart completion must disarm cleanup without aborting"
        );
        let restored = set_disks
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    include_part_checksums: true,
                    ..Default::default()
                },
            )
            .await
            .expect("successful multipart restore must leave the committed object intact");
        assert_eq!(
            restored.parts[0]
                .checksums
                .as_ref()
                .and_then(|checksums| checksums.get("CRC32C"))
                .map(String::as_str),
            Some("AAAAAA==")
        );
        assert_eq!(
            restored.parts[1]
                .checksums
                .as_ref()
                .and_then(|checksums| checksums.get("CRC32C"))
                .map(String::as_str),
            Some("AQAAAA==")
        );
        let restore_header = restored
            .user_defined
            .get(s3s::header::X_AMZ_RESTORE.as_str())
            .expect("successful multipart restore must persist restore status");
        let restore_status = parse_restore_obj_status(restore_header).expect("successful restore status must be valid");
        assert!(!restore_status.on_going(), "successful multipart restore must not remain in progress");
        assert!(
            restore_status.expiry().is_some(),
            "successful multipart restore must retain its expiry date"
        );
        assert!(
            rustfs_utils::http::get_str(restored.user_defined.as_ref(), rustfs_utils::http::SUFFIX_RESTORE_WORKER_LOCK,)
                .is_none(),
            "successful multipart restore must consume the worker-liveness marker"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn restore_failure_after_snapshot_cleans_exact_generation_and_returns_primary_error() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-post-snapshot-cleanup-bucket";
        let object = "object.bin";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(b"post-snapshot cleanup source".repeat(1024));
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        set_disks
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name,
                        etag: original.etag.clone().unwrap_or_default(),
                        ..Default::default()
                    },
                    version_id: original.version_id.map(|version| version.to_string()),
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("source object should transition before restore");

        let operation_id = Uuid::new_v4();
        let (mut source_fi, _, online_disks) = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("transitioned metadata should be readable")
            .into_owned();
        source_fi.metadata.extend(restore_metadata(operation_id, true));
        rustfs_utils::http::insert_str(
            &mut source_fi.metadata,
            rustfs_utils::http::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            "invalid".to_string(),
        );
        set_disks
            .update_object_meta(bucket, object, source_fi, &online_disks)
            .await
            .expect("invalid backend identity fixture should be persisted");
        set_disks.invalidate_get_object_metadata_cache(bucket, object).await;

        let mut opts = ObjectOptions::default();
        opts.transition.restore_request.days = Some(1);
        opts.user_defined = restore_operation_id_metadata(operation_id);
        let error = set_disks
            .clone()
            .restore_transitioned_object(bucket, object, &opts)
            .await
            .expect_err("invalid backend identity must fail before the tier read");
        assert!(
            error
                .to_string()
                .contains("transition tier backend identity has an invalid length"),
            "cleanup must preserve the primary validation error: {error}"
        );

        let cleaned = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("cleanup should leave the transitioned object readable");
        assert_eq!(cleaned.transitioned_object.status, TRANSITION_COMPLETE);
        assert!(!cleaned.user_defined.contains_key(s3s::header::X_AMZ_RESTORE.as_str()));
        assert!(
            rustfs_utils::http::get_str(cleaned.user_defined.as_ref(), rustfs_utils::http::SUFFIX_RESTORE_OPERATION_ID,)
                .is_none()
        );
        assert!(
            rustfs_utils::http::get_str(cleaned.user_defined.as_ref(), rustfs_utils::http::SUFFIX_RESTORE_WORKER_LOCK,).is_none()
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn single_part_restore_keeps_object_lock_snapshot_for_versioned_and_suspended_objects() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-single-part-versioned-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        for (case, versioned, version_suspended) in [("versioned", true, false), ("suspended", false, true)] {
            let object = format!("{case}.bin");
            let payload = format!("single-part restore must preserve the {case} object identity")
                .repeat(1024)
                .into_bytes();
            let mut reader = PutObjReader::from_vec(payload.clone());
            let original = set_disks
                .put_object(
                    bucket,
                    &object,
                    &mut reader,
                    &ObjectOptions {
                        versioned,
                        version_suspended,
                        ..Default::default()
                    },
                )
                .await
                .expect("source object should be written");
            let version_id = original
                .version_id
                .expect("versioned and suspended objects should resolve an explicit version");
            let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
            let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
            set_disks
                .transition_object(
                    bucket,
                    &object,
                    &ObjectOptions {
                        no_lock: true,
                        transition: TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name,
                            etag: original.etag.clone().unwrap_or_default(),
                            ..Default::default()
                        },
                        version_id: Some(version_id.to_string()),
                        versioned,
                        version_suspended,
                        mod_time: original.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("source object should transition before restore");

            set_disks
                .clone()
                .restore_transitioned_object(
                    bucket,
                    &object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            restore_request: RestoreRequest {
                                days: Some(1),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        version_id: Some(version_id.to_string()),
                        versioned,
                        version_suspended,
                        object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                            ObjectLockConfigState::ConfirmedAbsent,
                        ))),
                        ..Default::default()
                    },
                )
                .await
                .expect("single-part restore should commit the selected version");

            let restored = set_disks
                .get_object_info(
                    bucket,
                    &object,
                    &ObjectOptions {
                        version_id: Some(version_id.to_string()),
                        versioned,
                        version_suspended,
                        ..Default::default()
                    },
                )
                .await
                .expect("restored version should remain readable");
            assert_eq!(restored.version_id, Some(version_id), "restore must preserve the selected {case} version");
            let restore_header = restored
                .user_defined
                .get(s3s::header::X_AMZ_RESTORE.as_str())
                .expect("restored version must carry its completed restore status");
            let restore_status = parse_restore_obj_status(restore_header).expect("restore status must be valid");
            assert!(!restore_status.on_going(), "restored {case} version must not remain in progress");

            backend.external_remove(&restored.transitioned_object.name).await;
            let mut restored_body = Vec::new();
            set_disks
                .get_object_reader(
                    bucket,
                    &object,
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        version_id: Some(version_id.to_string()),
                        versioned,
                        version_suspended,
                        ..Default::default()
                    },
                )
                .await
                .expect("restored local version should remain readable after the remote copy is removed")
                .stream
                .read_to_end(&mut restored_body)
                .await
                .expect("restored local body should drain");
            assert_eq!(restored_body, payload, "restored {case} body must come from the committed local copy");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn local_commit_failure_returns_error_and_preserves_remote_candidate() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-commit-failure-bucket";
        let object = "object.bin";
        let payload = b"transition commit failure must preserve the uploaded candidate".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let snapshot = set_disks
            .get_object_fileinfo(bucket, object, &ObjectOptions::default(), true, false)
            .await
            .expect("source metadata should resolve");
        let (fi, parts_metadata, online_disks) = snapshot.into_owned();
        let generation = set_disks
            .get_object_metadata_cache_generation(bucket, object)
            .expect("metadata cache generation should be active");
        let cache_key = GetObjectMetadataCacheKey::new(bucket, object, generation);
        set_disks
            .get_object_metadata_cache
            .insert(
                cache_key.clone(),
                Arc::new(GetObjectMetadataCacheEntry {
                    created_at: Instant::now(),
                    fi,
                    parts_metadata,
                    online_disks,
                    read_quorum: 2,
                }),
            )
            .await;
        assert!(set_disks.get_object_metadata_cache.get(&cache_key).await.is_some());

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name,
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let barrier = TransitionCommitBarrier::install(bucket, object);
        let invalidations = MetadataCacheInvalidationProbe::install(bucket, object);
        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        assert_eq!(invalidations.count(), 1, "precommit revalidation must fence the old metadata once");
        let saved_disks = {
            let mut disks = set_disks.disks.write().await;
            let saved = std::mem::take(&mut *disks);
            *disks = vec![None; saved.len()];
            saved
        };
        barrier.release();
        let result = transition.await.expect("transition task should not panic");
        *set_disks.disks.write().await = saved_disks;

        result.expect_err("local write quorum failure must be returned to the transition worker");
        assert_eq!(
            invalidations.count(),
            2,
            "local commit failure must fence any partially committed metadata again"
        );
        assert_eq!(backend.put_count().await, 1);
        assert_eq!(
            backend.remove_count().await,
            0,
            "ambiguous local commit failure must retain the remote candidate"
        );
        assert_eq!(backend.object_count().await, 1);
        assert!(
            set_disks.get_object_metadata_cache.get(&cache_key).await.is_none(),
            "local commit failure must invalidate pre-transition metadata"
        );

        let mut restored = Vec::new();
        set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the local source must remain readable after a fail-before-commit result")
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("the local source body should drain");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn partial_local_commit_failure_rolls_back_applied_disks_and_preserves_remote_candidate() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-partial-rollback-bucket";
        let object = "object.bin";
        let payload = b"partial local commit failure must roll back applied disks".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name,
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let barrier = TransitionCommitBarrier::install_after_lease_check(bucket, object);
        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        assert_eq!(backend.put_count().await, 1, "remote candidate must exist before local commit");

        let saved_disks = {
            let mut disks = set_disks.disks.write().await;
            let saved = disks.clone();
            for disk in disks.iter_mut().skip(2) {
                *disk = None;
            }
            saved
        };
        barrier.release();
        let result = transition.await.expect("transition task should not panic");

        result.expect_err("partial local commit must fail when only two of four disks are writable");
        let snapshot = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("rollback should keep the source metadata readable on applied disks");
        assert_ne!(
            snapshot.fi().transition_status,
            TRANSITION_COMPLETE,
            "rollback must not leave the applied disks marked as transitioned"
        );
        let mut restored = Vec::new();
        set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("rollback should keep the source object readable on applied disks")
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("the local source body should drain after rollback");
        assert_eq!(restored, payload);

        *set_disks.disks.write().await = saved_disks;
        assert_eq!(
            backend.remove_count().await,
            0,
            "ambiguous local commit failure must retain the remote candidate for scanner reconciliation"
        );
        assert_eq!(backend.object_count().await, 1);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn local_commit_post_apply_error_rolls_back_the_errored_disk() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-post-apply-rollback-bucket";
        let object = "object.bin";
        let payload = b"post-apply delete errors must still roll back the changed disk".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name,
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let barrier = TransitionCommitBarrier::install_after_lease_check(bucket, object);
        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        assert_eq!(backend.put_count().await, 1, "remote candidate must exist before local commit");

        crate::disk::local::set_delete_version_fail_after_commit(disk_stores[0].path().as_path(), object);
        let saved_disk = {
            let mut disks = set_disks.disks.write().await;
            let saved = disks[1].take();
            assert!(saved.is_some(), "test setup should start with the second disk online");
            saved
        };
        barrier.release();
        let result = transition.await.expect("transition task should not panic");

        result.expect_err("post-apply disk error plus one offline disk must fail write quorum");
        let errored_disk_fi = disk_stores[0]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("rollback must restore metadata on the disk that returned the post-apply error");
        assert_ne!(
            errored_disk_fi.transition_status, TRANSITION_COMPLETE,
            "rollback must not skip the disk that applied delete_version but returned an error"
        );
        disk_stores[0]
            .check_parts(bucket, object, &errored_disk_fi)
            .await
            .expect("rollback must restore the errored disk's staged data directory");

        set_disks.disks.write().await[1] = saved_disk;
        assert_eq!(
            backend.remove_count().await,
            0,
            "ambiguous local commit failure must retain the remote candidate for reconciliation"
        );
        assert_eq!(backend.object_count().await, 1);

        let mut restored = Vec::new();
        set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source must remain readable after rolling back the post-apply error")
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("the local source body should drain");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn production_transition_replacement_revokes_commit_and_cleans_with_old_driver() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-generation-fence-bucket";
        let object = "object.bin";
        let payload = b"generation replacement must fence the local transition commit".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut reader = PutObjReader::from_vec(payload);
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let manager = runtime_sources::global_tier_config_mgr();
        let old_backend = register_mock_tier(&manager, &tier_name).await;
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name.clone(),
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let barrier = TransitionCommitBarrier::install(bucket, object);
        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        assert_eq!(old_backend.put_count().await, 1, "remote candidate must exist before replacement");

        let replacement_backend = MockWarmBackend::new();
        let replacement_manager = Arc::new(RwLock::new(TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: manager.read().await.tiers.clone(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        }));
        {
            let mut replacement = replacement_manager.write().await;
            replacement
                .tiers
                .get_mut(&tier_name)
                .and_then(|tier| tier.minio.as_mut())
                .expect("replacement tier should exist")
                .prefix = "replacement/".to_string();
            replacement
                .install_test_driver(&tier_name, Box::new(replacement_backend))
                .expect("replacement driver should install");
        }
        let replacement = match Arc::try_unwrap(replacement_manager) {
            Ok(manager) => manager.into_inner(),
            Err(_) => panic!("replacement manager should have one owner"),
        };
        let publish_handle = manager.clone();
        let publish_tier = tier_name.clone();
        let publish =
            tokio::spawn(
                async move { TierConfigMgr::publish_candidate(&publish_handle, replacement, Some(&publish_tier)).await },
            );

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match TierConfigMgr::acquire_operation_lease(&manager, &tier_name).await {
                    Err(err) if err.message.contains("being replaced") => break,
                    Ok(lease) => drop(lease),
                    Err(err) => panic!("unexpected lease error while replacement drains: {err}"),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement should revoke the in-flight generation");
        assert!(!publish.is_finished(), "replacement must wait for the production transition lease");

        barrier.release();
        transition
            .await
            .expect("transition task should join")
            .expect_err("revoked generation must not commit tier-name metadata");
        publish
            .await
            .expect("replacement task should join")
            .expect("replacement should finish after old cleanup releases its lease");
        assert_eq!(
            old_backend.remove_count().await,
            1,
            "cancelled transition must clean up with the old driver"
        );
        assert_eq!(old_backend.object_count().await, 0);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn restore_keeps_remote_tuple_lease_after_reader_eof_until_local_commit() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-remote-tuple-publication-lease-bucket";
        let object = "object.bin";
        let payload = b"restore must retain its tier generation through metadata quorum".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(payload);
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let manager = runtime_sources::global_tier_config_mgr();
        let _backend = register_mock_tier(&manager, &tier_name).await;
        set_disks
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.clone(),
                        etag: original.etag.clone().unwrap_or_default(),
                        ..Default::default()
                    },
                    version_id: original.version_id.map(|version| version.to_string()),
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("source object should transition before restore");
        assert_eq!(
            TierConfigMgr::active_operation_lease_count(&manager, &tier_name).await,
            0,
            "transition should release its operation lease before restore"
        );

        // BeforeNamespace is reached after the restore reader has drained but
        // before the destination metadata write lock and quorum rename.
        let barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::BeforeNamespace);
        let restore_set = Arc::clone(&set_disks);
        let restore = tokio::spawn(async move {
            let mut opts = ObjectOptions::default();
            opts.transition.restore_request.days = Some(1);
            restore_set.restore_transitioned_object(bucket, object, &opts).await
        });
        barrier.wait_until_paused().await;
        assert_eq!(
            TierConfigMgr::active_operation_lease_count(&manager, &tier_name).await,
            1,
            "the publication lease must outlive the EOF-scoped tier reader lease"
        );

        barrier.release();
        restore
            .await
            .expect("restore task should join")
            .expect("restore should commit after the barrier releases");
        assert_eq!(
            TierConfigMgr::active_operation_lease_count(&manager, &tier_name).await,
            0,
            "restore completion should release the publication lease"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn failed_restore_cleanup_does_not_overwrite_concurrent_unversioned_put() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-cleanup-cas-bucket";
        let object = "object.bin";
        let original_payload = b"old transitioned body".repeat(1024);
        let replacement_payload = b"new visible unversioned body".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(original_payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        set_disks
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name,
                        etag: original.etag.clone().unwrap_or_default(),
                        ..Default::default()
                    },
                    version_id: original.version_id.map(|version| version.to_string()),
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("source object should transition before restore");

        let get_barrier = backend.arm_failing_get_barrier().await;
        let restore_set = Arc::clone(&set_disks);
        let restore = tokio::spawn(async move {
            let mut opts = ObjectOptions::default();
            opts.transition.restore_request.days = Some(1);
            restore_set.restore_transitioned_object(bucket, object, &opts).await
        });
        get_barrier.wait_until_paused().await;

        let mut replacement_reader = PutObjReader::from_vec(replacement_payload.clone());
        let replacement = set_disks
            .put_object(bucket, object, &mut replacement_reader, &ObjectOptions::default())
            .await
            .expect("concurrent unversioned PUT should commit while restore GET is paused");
        get_barrier.release();
        restore
            .await
            .expect("restore task should join")
            .expect_err("injected tier GET failure should surface");

        let visible = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("replacement metadata should remain visible");
        assert_eq!(visible.etag, replacement.etag, "stale restore cleanup must not republish the old ETag");
        assert_ne!(
            visible.transitioned_object.status, TRANSITION_COMPLETE,
            "replacement object must not regain stale transition metadata"
        );
        let mut body = Vec::new();
        set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("replacement object should be readable")
            .stream
            .read_to_end(&mut body)
            .await
            .expect("replacement body should drain");
        assert_eq!(
            body, replacement_payload,
            "stale restore cleanup must not make the old remote body current again"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn failed_restore_cleanup_ignores_replaced_operation_id_with_same_identity() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-cleanup-operation-id-bucket";
        let object = "object.bin";
        let payload = b"same identity restore cleanup must respect operation id".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let operation_a = Uuid::new_v4();
        let operation_b = Uuid::new_v4();
        let metadata = restore_metadata(operation_a, true);

        let mut reader = PutObjReader::from_vec(payload);
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(metadata),
                    ..Default::default()
                },
            )
            .await
            .expect("restore operation A metadata should be installed");
        let stale_operation_a = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("operation A metadata should resolve");

        let mut operation_b_metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut operation_b_metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_b.to_string(),
        );
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(operation_b_metadata),
                    ..Default::default()
                },
            )
            .await
            .expect("same-identity restore operation B should replace A");

        let mut expected_operation_a = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut expected_operation_a,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_a.to_string(),
        );
        set_disks
            .update_restore_metadata(
                bucket,
                object,
                &stale_operation_a,
                &ObjectOptions {
                    user_defined: expected_operation_a,
                    ..Default::default()
                },
            )
            .await
            .expect("stale operation A cleanup should no-op, not fail");
        let current_operation_b = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("operation B metadata should remain readable");
        assert!(
            current_operation_b
                .user_defined
                .contains_key(s3s::header::X_AMZ_RESTORE.as_str()),
            "stale cleanup for operation A must not remove operation B's restore header"
        );
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                current_operation_b.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            ),
            Some(operation_b.to_string().as_str()),
            "stale cleanup for operation A must not remove or rewrite operation B"
        );

        let mut expected_operation_b = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut expected_operation_b,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_b.to_string(),
        );
        set_disks
            .update_restore_metadata(
                bucket,
                object,
                &current_operation_b,
                &ObjectOptions {
                    user_defined: expected_operation_b,
                    ..Default::default()
                },
            )
            .await
            .expect("matching operation B cleanup should remove restore markers");
        let cleaned = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("cleaned object metadata should remain readable");
        assert!(
            !cleaned.user_defined.contains_key(s3s::header::X_AMZ_RESTORE.as_str()),
            "matching cleanup must remove the restore header"
        );
        assert!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                cleaned.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            )
            .is_none(),
            "matching cleanup must remove the restore operation id"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn no_lock_restore_finalize_requires_live_namespace_fence() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-finalize-fence-bucket";
        let object = "object.bin";
        let payload = b"restore finalize no_lock fence must fail closed".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let operation_id = Uuid::new_v4();
        let mut reader = PutObjReader::from_vec(payload);
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(restore_metadata(operation_id, true)),
                    ..Default::default()
                },
            )
            .await
            .expect("restore metadata should be installed");
        let restoring = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("restore metadata should be readable");

        let err = set_disks
            .finalize_restore_metadata(
                bucket,
                object,
                &restoring,
                &ObjectOptions {
                    no_lock: true,
                    namespace_lock_fence: Some(NamespaceLockFence::lost_for_test()),
                    user_defined: restore_operation_id_metadata(operation_id),
                    ..Default::default()
                },
            )
            .await
            .expect_err("lost outer namespace fence must reject no_lock restore finalization");
        assert!(matches!(err, Error::NamespaceLockQuorumUnavailable { .. }));

        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("restore metadata should remain readable");
        let restore_status = parse_restore_obj_status(
            current
                .user_defined
                .get(s3s::header::X_AMZ_RESTORE.as_str())
                .expect("restore header must remain pending"),
        )
        .expect("restore header should remain parseable");
        assert!(
            restore_status.on_going(),
            "lost no_lock finalization must not publish restored completion metadata"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn no_lock_restore_cleanup_requires_live_namespace_fence() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-cleanup-fence-bucket";
        let object = "object.bin";
        let payload = b"restore cleanup no_lock fence must fail closed".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let operation_id = Uuid::new_v4();
        let mut reader = PutObjReader::from_vec(payload);
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(restore_metadata(operation_id, true)),
                    ..Default::default()
                },
            )
            .await
            .expect("restore metadata should be installed");
        let restoring = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("restore metadata should be readable");

        let err = set_disks
            .update_restore_metadata(
                bucket,
                object,
                &restoring,
                &ObjectOptions {
                    no_lock: true,
                    namespace_lock_fence: Some(NamespaceLockFence::lost_for_test()),
                    user_defined: restore_operation_id_metadata(operation_id),
                    ..Default::default()
                },
            )
            .await
            .expect_err("lost outer namespace fence must reject no_lock restore cleanup");
        assert!(matches!(err, Error::NamespaceLockQuorumUnavailable { .. }));

        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("restore metadata should remain readable");
        assert!(
            current.user_defined.contains_key(s3s::header::X_AMZ_RESTORE.as_str()),
            "lost no_lock cleanup must not remove the restore header"
        );
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                current.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            ),
            Some(operation_id.to_string().as_str()),
            "lost no_lock cleanup must not remove the restore operation id"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn restore_worker_propagates_operation_id_to_final_put_commit() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-worker-commit-operation-id-bucket";
        let object = "object.bin";
        let payload = b"restore worker must carry operation id to final commit".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        set_disks
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name,
                        etag: original.etag.clone().unwrap_or_default(),
                        ..Default::default()
                    },
                    version_id: original.version_id.map(|version| version.to_string()),
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("source object should transition before restore");

        let operation_a = Uuid::new_v4();
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(restore_metadata(operation_a, true)),
                    ..Default::default()
                },
            )
            .await
            .expect("restore operation A metadata should be installed");

        let get_barrier = backend.arm_get_barrier().await;
        let restore_set = Arc::clone(&set_disks);
        let restore = tokio::spawn(async move {
            restore_set
                .restore_transitioned_object(
                    bucket,
                    object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            restore_request: RestoreRequest {
                                days: Some(1),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        user_defined: restore_operation_id_metadata(operation_a),
                        ..Default::default()
                    },
                )
                .await
        });
        get_barrier.wait_until_paused().await;

        let operation_b = Uuid::new_v4();
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(restore_operation_id_metadata(operation_b)),
                    ..Default::default()
                },
            )
            .await
            .expect("operation B should replace operation A after worker starts tier GET");
        get_barrier.release();
        restore
            .await
            .expect("restore task should join")
            .expect_err("stale operation A must fail at final PUT commit");

        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("operation B metadata should remain visible after stale worker is rejected");
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                current.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            ),
            Some(operation_b.to_string().as_str()),
            "stale worker must not remove or replace operation B"
        );
        assert_eq!(
            current.transitioned_object.status, TRANSITION_COMPLETE,
            "stale worker must not publish its restored local body after operation id replacement"
        );
        let mut body = Vec::new();
        set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("original transitioned object should remain readable")
            .stream
            .read_to_end(&mut body)
            .await
            .expect("original remote body should drain");
        assert_eq!(body, payload);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn restore_put_commit_rechecks_operation_id_and_strips_internal_marker() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-put-commit-operation-id-bucket";
        let object = "object.bin";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let operation_a = Uuid::new_v4();
        let operation_a_metadata = restore_metadata(operation_a, true);
        let mut reader = PutObjReader::from_vec(b"restore source body".repeat(1024));
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(operation_a_metadata.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("ongoing restore operation A should be installed");

        let operation_b = Uuid::new_v4();
        let mut operation_b_metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut operation_b_metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_b.to_string(),
        );
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(operation_b_metadata),
                    ..Default::default()
                },
            )
            .await
            .expect("operation B should replace operation A before final commit");

        let mismatch = set_disks
            .finalize_restore_metadata(
                bucket,
                object,
                &set_disks
                    .get_object_info(bucket, object, &ObjectOptions::default())
                    .await
                    .expect("operation B metadata should be readable"),
                &ObjectOptions {
                    user_defined: restore_operation_id_metadata(operation_a),
                    ..Default::default()
                },
            )
            .await
            .expect_err("operation A must not finalize operation B metadata");
        assert!(matches!(
            mismatch,
            Error::Io(ref error)
                if error.kind() == std::io::ErrorKind::Other
                    && error.to_string() == "restore operation id changed before metadata finalization"
        ));
        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("operation B metadata should remain after mismatched finalization");
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                current.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            ),
            Some(operation_b.to_string().as_str()),
            "mismatched finalization must not remove operation B"
        );
        assert!(
            parse_restore_obj_status(
                current
                    .user_defined
                    .get(s3s::header::X_AMZ_RESTORE.as_str())
                    .expect("operation B restore header should remain pending"),
            )
            .expect("operation B restore header should parse")
            .on_going(),
            "mismatched finalization must not publish restore completion"
        );

        let mut stale_restore_reader = PutObjReader::from_vec(b"stale A restored body".repeat(1024));
        let result = set_disks
            .put_object(
                bucket,
                object,
                &mut stale_restore_reader,
                &ObjectOptions {
                    user_defined: operation_a_metadata,
                    ..Default::default()
                },
            )
            .await;
        result.expect_err("stale operation A must not commit after operation B replaces it");

        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("operation B metadata should remain current after stale commit is rejected");
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                current.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            ),
            Some(operation_b.to_string().as_str()),
            "stale commit must not remove or replace operation B"
        );

        let mut matching_restore_reader = PutObjReader::from_vec(b"matching B restored body".repeat(1024));
        let operation_b_restore_metadata = restore_metadata(operation_b, false);
        let restored = set_disks
            .put_object(
                bucket,
                object,
                &mut matching_restore_reader,
                &ObjectOptions {
                    user_defined: operation_b_restore_metadata.clone(),
                    ..Default::default()
                },
            )
            .await
            .expect("matching operation B should be allowed to commit");
        set_disks
            .finalize_restore_metadata(
                bucket,
                object,
                &restored,
                &ObjectOptions {
                    user_defined: restore_operation_id_metadata(operation_b),
                    transition: TransitionOptions {
                        restore_request: RestoreRequest {
                            days: Some(1),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("matching operation B should finalize after its commit consumes the operation id");
        let restored = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("restored object should remain readable");
        assert!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                restored.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            )
            .is_none(),
            "completed restore PUT must not persist the internal operation id"
        );
        assert!(
            rustfs_utils::http::get_str(restored.user_defined.as_ref(), rustfs_utils::http::SUFFIX_RESTORE_WORKER_LOCK,)
                .is_none(),
            "completed restore PUT must not persist the worker-liveness marker"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn restore_multipart_complete_rechecks_operation_id_and_strips_internal_marker() {
        use crate::storage_api_contracts::multipart::MultipartOperations as _;

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-multipart-commit-operation-id-bucket";
        let object = "object.bin";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let operation_a = Uuid::new_v4();
        let operation_a_metadata = restore_metadata(operation_a, true);
        let mut initial_reader = PutObjReader::from_vec(b"multipart restore source body".repeat(1024));
        set_disks
            .put_object(bucket, object, &mut initial_reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(operation_a_metadata.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("ongoing multipart restore operation A should be installed");

        let upload = set_disks
            .new_multipart_upload(
                bucket,
                object,
                &ObjectOptions {
                    user_defined: operation_a_metadata.clone(),
                    ..Default::default()
                },
            )
            .await
            .expect("restore multipart upload should be created");
        let mut part_reader = PutObjReader::from_vec(b"stale multipart A restored body".repeat(1024));
        let part = set_disks
            .put_object_part(bucket, object, &upload.upload_id, 1, &mut part_reader, &ObjectOptions::default())
            .await
            .expect("restore multipart part should be written");

        let operation_b = Uuid::new_v4();
        let mut operation_b_metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut operation_b_metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_b.to_string(),
        );
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(operation_b_metadata),
                    ..Default::default()
                },
            )
            .await
            .expect("operation B should replace operation A before multipart complete");

        let complete_result = set_disks
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &upload.upload_id,
                vec![CompletePart {
                    part_num: part.part_num,
                    etag: part.etag.clone(),
                    ..Default::default()
                }],
                &ObjectOptions {
                    user_defined: operation_a_metadata,
                    ..Default::default()
                },
            )
            .await;
        complete_result.expect_err("stale operation A must not complete after operation B replaces it");

        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("operation B metadata should remain current after stale multipart completion");
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                current.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            ),
            Some(operation_b.to_string().as_str()),
            "stale multipart completion must not remove or replace operation B"
        );

        let operation_b_restore_metadata = restore_metadata(operation_b, false);
        let upload = set_disks
            .new_multipart_upload(
                bucket,
                object,
                &ObjectOptions {
                    user_defined: operation_b_restore_metadata.clone(),
                    ..Default::default()
                },
            )
            .await
            .expect("matching operation B multipart upload should be created");
        let mut part_reader = PutObjReader::from_vec(b"matching multipart B restored body".repeat(1024));
        let part = set_disks
            .put_object_part(bucket, object, &upload.upload_id, 1, &mut part_reader, &ObjectOptions::default())
            .await
            .expect("matching restore multipart part should be written");
        set_disks
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &upload.upload_id,
                vec![CompletePart {
                    part_num: part.part_num,
                    etag: part.etag,
                    ..Default::default()
                }],
                &ObjectOptions {
                    user_defined: operation_b_restore_metadata,
                    ..Default::default()
                },
            )
            .await
            .expect("matching operation B should complete");
        let restored = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("completed multipart restore should remain readable");
        assert!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                restored.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            )
            .is_none(),
            "completed multipart restore must not persist the internal operation id"
        );
        assert!(
            rustfs_utils::http::get_str(restored.user_defined.as_ref(), rustfs_utils::http::SUFFIX_RESTORE_WORKER_LOCK,)
                .is_none(),
            "completed multipart restore must not persist the worker-liveness marker"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_reload_rejects_route_change_after_local_transition_commit() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-post-check-fence-bucket";
        let object = "object.bin";
        let payload = b"the operation lease must cover the local transition commit".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut reader = PutObjReader::from_vec(payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let manager = runtime_sources::global_tier_config_mgr();
        let old_backend = register_mock_tier(&manager, &tier_name).await;
        let old_prefix = manager.read().await.tiers[&tier_name]
            .minio
            .as_ref()
            .expect("old tier route should exist")
            .prefix
            .clone();
        let old_identity = TierConfigMgr::acquire_operation_lease(&manager, &tier_name)
            .await
            .expect("old tier identity should be available")
            .backend_identity();
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name.clone(),
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let barrier = TransitionCommitBarrier::install_after_lease_check(bucket, object);
        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        assert_eq!(old_backend.put_count().await, 1, "remote candidate must exist before replacement");

        let replacement_manager = Arc::new(RwLock::new(TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: manager.read().await.tiers.clone(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        }));
        {
            let mut replacement = replacement_manager.write().await;
            replacement
                .tiers
                .get_mut(&tier_name)
                .and_then(|tier| tier.minio.as_mut())
                .expect("replacement tier should exist")
                .prefix = "replacement/".to_string();
        }
        let replacement = match Arc::try_unwrap(replacement_manager) {
            Ok(manager) => manager.into_inner(),
            Err(_) => panic!("replacement manager should have one owner"),
        };
        let publish_handle = manager.clone();
        let publish = tokio::spawn(async move { publish_handle.write().await.publish_legacy_reload(replacement).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while manager.try_read().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("legacy reload should hold the manager guard while the checked generation drains");
        assert!(
            !publish.is_finished(),
            "replacement must wait until the local transition commit releases its lease"
        );

        barrier.release();
        transition
            .await
            .expect("transition task should join")
            .expect("a transition linearized before replacement should commit");
        publish
            .await
            .expect("replacement task should join")
            .expect_err("replacement must not rebind a tier name referenced by the committed transition");
        assert_eq!(old_backend.remove_count().await, 0, "committed transition data must not be cleaned up");
        assert_eq!(old_backend.object_count().await, 1);
        let transitioned = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("transitioned metadata should resolve");
        let expected_identity = rustfs_utils::crypto::hex(old_identity);
        let rustfs_identity_key = format!(
            "{}{}",
            rustfs_utils::http::metadata_compat::RUSTFS_INTERNAL_PREFIX,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID
        );
        let minio_identity_key = format!(
            "{}{}",
            rustfs_utils::http::metadata_compat::MINIO_INTERNAL_PREFIX,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID
        );
        assert_eq!(
            transitioned.user_defined.get(&rustfs_identity_key),
            Some(&expected_identity),
            "transition commits must write the RustFS destination identity key"
        );
        assert_eq!(
            transitioned.user_defined.get(&minio_identity_key),
            Some(&expected_identity),
            "transition commits must write the MinIO-compatible destination identity key"
        );
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_str(
                &transitioned.user_defined,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            )
            .as_deref(),
            Some(expected_identity.as_str())
        );
        assert_eq!(
            manager
                .read()
                .await
                .tiers
                .get(&tier_name)
                .and_then(|tier| tier.minio.as_ref())
                .expect("old tier route should remain configured")
                .prefix,
            old_prefix
        );

        let mut restored = Vec::new();
        set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the transitioned object must remain routed through the old generation")
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("the transitioned object body should drain");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn object_transaction_fencing_requires_live_fleet_proof_before_put_commit() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transaction-fencing-no-proof";
        let object = "object.bin";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let err = temp_env::async_with_vars(
            [
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_WRITE, Some("true")),
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED, Some("true")),
            ],
            async {
                let mut reader = PutObjReader::from_vec(b"must-not-commit-without-proof".to_vec());
                set_disks
                    .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                    .await
            },
        )
        .await
        .expect_err("object fencing must fail closed without a live fleet proof");

        assert!(
            err.to_string()
                .contains("object transaction fencing requires a live fleet capability proof"),
            "unexpected error: {err:?}"
        );
        for disk in &disk_stores {
            let missing = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect_err("failed fenced PUT must not publish object metadata");
            assert!(
                matches!(missing, DiskError::FileNotFound | DiskError::FileVersionNotFound),
                "failed fenced PUT left unexpected disk state: {missing:?}"
            );
        }
    }
}

#[cfg(all(test, feature = "test-util"))]
mod transition_upload_integrity_tests {
    use super::hermetic_set_disks_support::{hermetic_set_disks, hermetic_set_disks_with_lockers};
    use super::transition_commit_failure_tests::{restore_metadata, restore_operation_id_metadata};
    use super::*;
    use crate::bucket::lifecycle::lifecycle::{TRANSITION_PENDING, TransitionOptions};
    use crate::layout::endpoints::SetupType;
    use crate::services::notification_sys::install_transition_transaction_compaction_fleet_proof_for_test;
    use crate::services::tier::test_util::register_mock_tier;
    use crate::set_disk::replication::RestoreFinalizeBarrier;
    use http::HeaderMap;
    use rustfs_filemeta::RestoreStatusOps as _;
    use rustfs_lock::client::local::LocalClient;
    use rustfs_lock::{LockClient, LockError, LockId, LockInfo, LockRequest, LockResponse, LockStats};
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;

    struct SetupTypeGuard {
        previous: SetupType,
    }

    impl SetupTypeGuard {
        async fn switch_to(next: SetupType) -> Self {
            let previous = runtime_sources::current_setup_type().await;
            runtime_sources::set_setup_type(next).await;
            Self { previous }
        }
    }

    impl Drop for SetupTypeGuard {
        fn drop(&mut self) {
            let previous = self.previous.clone();
            let handle = tokio::runtime::Handle::current();
            std::thread::spawn(move || {
                handle.block_on(async {
                    runtime_sources::set_setup_type(previous).await;
                });
            })
            .join()
            .expect("setup type restore thread should not panic");
        }
    }

    #[derive(Debug)]
    struct FailingLockClient;

    #[async_trait::async_trait]
    impl LockClient for FailingLockClient {
        async fn acquire_lock(&self, _request: &rustfs_lock::LockRequest) -> rustfs_lock::Result<LockResponse> {
            Err(LockError::internal("simulated transition commit lock client failure"))
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
    struct LockLostRefreshClient {
        refresh_calls: Arc<AtomicUsize>,
        active: tokio::sync::Mutex<HashSet<LockId>>,
    }

    impl LockLostRefreshClient {
        fn new(refresh_calls: Arc<AtomicUsize>) -> Self {
            Self {
                refresh_calls,
                active: tokio::sync::Mutex::new(HashSet::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl LockClient for LockLostRefreshClient {
        async fn acquire_lock(&self, request: &LockRequest) -> rustfs_lock::Result<LockResponse> {
            self.active.lock().await.insert(request.lock_id.clone());
            Ok(LockResponse::success(
                LockInfo {
                    id: request.lock_id.clone(),
                    resource: request.resource.clone(),
                    lock_type: request.lock_type,
                    status: rustfs_lock::LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + request.ttl,
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                },
                Duration::ZERO,
            ))
        }

        async fn release(&self, lock_id: &LockId) -> rustfs_lock::Result<bool> {
            Ok(self.active.lock().await.remove(lock_id))
        }

        async fn refresh(&self, _lock_id: &LockId) -> rustfs_lock::Result<bool> {
            self.refresh_calls.fetch_add(1, Ordering::SeqCst);
            Ok(false)
        }

        async fn force_release(&self, lock_id: &LockId) -> rustfs_lock::Result<bool> {
            self.release(lock_id).await
        }

        async fn check_status(&self, _lock_id: &LockId) -> rustfs_lock::Result<Option<LockInfo>> {
            Ok(None)
        }

        async fn get_stats(&self) -> rustfs_lock::Result<LockStats> {
            Ok(LockStats::default())
        }

        async fn close(&self) -> rustfs_lock::Result<()> {
            Ok(())
        }

        async fn is_online(&self) -> bool {
            true
        }

        async fn is_local(&self) -> bool {
            false
        }
    }

    async fn write_committed_restore(
        set_disks: &Arc<SetDisks>,
        disk_stores: &[DiskStore],
        bucket: &str,
        object: &str,
        operation_id: Uuid,
    ) -> ObjectInfo {
        for disk in disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut source = PutObjReader::from_vec(b"restore source body".repeat(1024));
        set_disks
            .put_object(bucket, object, &mut source, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(restore_metadata(operation_id, true)),
                    ..Default::default()
                },
            )
            .await
            .expect("pending restore metadata should be installed");

        let mut restored_reader = PutObjReader::from_vec(b"restored body".repeat(1024));
        set_disks
            .put_object(
                bucket,
                object,
                &mut restored_reader,
                &ObjectOptions {
                    user_defined: restore_metadata(operation_id, true),
                    ..Default::default()
                },
            )
            .await
            .expect("matching restore commit should consume its operation id")
    }

    fn restore_finalize_options(operation_id: Uuid) -> ObjectOptions {
        ObjectOptions {
            user_defined: restore_operation_id_metadata(operation_id),
            transition: TransitionOptions {
                restore_request: s3s::dto::RestoreRequest {
                    days: Some(1),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        }
    }

    async fn assert_committed_restore_remains_pending(set_disks: &Arc<SetDisks>, bucket: &str, object: &str) {
        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("pending restore metadata should remain readable");
        assert!(
            restore_operation_id_from_metadata(current.user_defined.as_ref())
                .expect("operation id metadata should parse")
                .is_none(),
            "successful restore commit must have consumed the operation id"
        );
        assert!(
            rustfs_filemeta::parse_restore_obj_status(
                current
                    .user_defined
                    .get(s3s::header::X_AMZ_RESTORE.as_str())
                    .expect("pending restore header should remain"),
            )
            .expect("restore header should parse")
            .on_going(),
            "failed finalization must not publish completion metadata"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial]
    async fn restore_finalize_rejects_acquired_lock_loss_after_commit() {
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let lockers: Vec<Arc<dyn LockClient>> = (0..4)
            .map(|_| Arc::new(LockLostRefreshClient::new(Arc::clone(&refresh_calls))) as Arc<dyn LockClient>)
            .collect();
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "restore-finalize-acquired-lock-lost-bucket";
        let object = "object.bin";
        let operation_id = Uuid::new_v4();
        let restored = temp_env::async_with_vars(
            [(
                crate::set_disk::core::io_primitives::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE,
                Some("false"),
            )],
            async { write_committed_restore(&set_disks, &disk_stores, bucket, object, operation_id).await },
        )
        .await;
        tokio::time::pause();
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let barrier = RestoreFinalizeBarrier::install(bucket, object);
        let finalize_set = Arc::clone(&set_disks);
        let finalize = tokio::spawn(async move {
            finalize_set
                .finalize_restore_metadata(bucket, object, &restored, &restore_finalize_options(operation_id))
                .await
        });
        barrier.wait_until_paused().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(refresh_calls.load(Ordering::SeqCst) > 0, "restore finalization lock must attempt renewal");
        barrier.release();

        let error = finalize
            .await
            .expect("restore finalization task should join")
            .expect_err("lost acquired lock must reject restore finalization");
        assert!(matches!(
            error,
            Error::Io(ref error)
                if error.kind() == std::io::ErrorKind::Other
                    && error.to_string() == "restore finalization lock lost before metadata update"
        ));
        assert_committed_restore_remains_pending(&set_disks, bucket, object).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn restore_finalize_rejects_outer_fence_loss_after_metadata_read() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-finalize-outer-fence-lost-bucket";
        let object = "object.bin";
        let operation_id = Uuid::new_v4();
        let restored = write_committed_restore(&set_disks, &disk_stores, bucket, object, operation_id).await;
        let (fence, loss_handle) = NamespaceLockFence::loss_handle_for_test();
        let barrier = RestoreFinalizeBarrier::install(bucket, object);
        let finalize_set = Arc::clone(&set_disks);
        let finalize = tokio::spawn(async move {
            let mut opts = restore_finalize_options(operation_id);
            opts.no_lock = true;
            opts.namespace_lock_fence = Some(fence);
            finalize_set.finalize_restore_metadata(bucket, object, &restored, &opts).await
        });
        barrier.wait_until_paused().await;
        loss_handle.store(true, std::sync::atomic::Ordering::Release);
        barrier.release();

        let error = finalize
            .await
            .expect("restore finalization task should join")
            .expect_err("lost outer fence must reject restore finalization");
        assert!(matches!(
            error,
            Error::NamespaceLockQuorumUnavailable {
                mode: "restore_finalize_metadata",
                required: 1,
                achieved: 0,
                ..
            }
        ));
        assert_committed_restore_remains_pending(&set_disks, bucket, object).await;
    }

    async fn assert_local_source_intact(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, payload: &[u8]) {
        let mut restored = Vec::new();
        set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("failed transition must leave the local source readable")
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("local source should drain after failed transition");
        assert_eq!(restored, payload);
        let snapshot = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("local source metadata should remain available");
        assert_ne!(snapshot.fi().transition_status, TRANSITION_COMPLETE);
    }

    async fn write_source(
        set_disks: &Arc<SetDisks>,
        disk_stores: &[DiskStore],
        bucket: &str,
        object: &str,
        payload: &[u8],
    ) -> ObjectInfo {
        for disk in disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut reader = PutObjReader::from_vec(payload.to_vec());
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written")
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_tiered_metadata_is_create_only_under_object_lock() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "tiered-data-movement-create-only";
        let object = "object.bin";
        let version_id = Uuid::new_v4();
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut reader = PutObjReader::from_vec(b"existing target".to_vec());
        set_disks
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("existing target should be written");
        let conflicting = FileInfo {
            volume: bucket.to_string(),
            name: object.to_string(),
            version_id: Some(version_id),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND),
            size: 12,
            parts: vec![ObjectPartInfo {
                number: 1,
                size: 12,
                actual_size: 12,
                etag: "part-etag".to_string(),
                ..Default::default()
            }],
            transition_status: TRANSITION_COMPLETE.to_string(),
            transition_tier: "WARM".to_string(),
            transitioned_objname: "remote/object-a".to_string(),
            transition_version: Some("remote-version-a".to_string()),
            transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
            fresh: true,
            ..Default::default()
        };
        let err = set_disks
            .decommission_tiered_object(
                bucket,
                object,
                &conflicting,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id.to_string()),
                    mod_time: conflicting.mod_time,
                    data_movement: true,
                    http_preconditions: Some(crate::data_movement::data_movement_target_precondition()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("data movement must not overwrite an existing tiered version");
        assert!(matches!(err, StorageError::PreconditionFailed));

        let (stored, _, _) = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(version_id.to_string()),
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("the existing target should remain readable")
            .into_owned();
        assert_ne!(stored.transition_status, TRANSITION_COMPLETE);
        assert!(stored.transition_version.is_none());
    }

    #[tokio::test]
    async fn data_movement_tiered_metadata_rejects_lost_outer_namespace_fence() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "tiered-data-movement-lost-outer-fence";
        let object = "object.bin";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let version_id = Uuid::new_v4();
        let source = FileInfo {
            volume: bucket.to_string(),
            name: object.to_string(),
            version_id: Some(version_id),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND),
            size: 12,
            parts: vec![ObjectPartInfo {
                number: 1,
                size: 12,
                actual_size: 12,
                etag: "part-etag".to_string(),
                ..Default::default()
            }],
            transition_status: TRANSITION_COMPLETE.to_string(),
            transition_tier: "WARM".to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_version: Some("remote-version".to_string()),
            transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
            fresh: true,
            ..Default::default()
        };
        let err = set_disks
            .decommission_tiered_object(
                bucket,
                object,
                &source,
                &ObjectOptions {
                    no_lock: true,
                    namespace_lock_fence: Some(NamespaceLockFence::lost_for_test()),
                    versioned: true,
                    version_id: Some(version_id.to_string()),
                    mod_time: source.mod_time,
                    data_movement: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("a lost outer object fence must block target metadata publication");

        assert!(matches!(err, StorageError::NamespaceLockQuorumUnavailable { .. }));
        assert!(
            set_disks
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("target metadata lookup should succeed")
                .is_none(),
            "lost outer fence must not publish a target version"
        );
    }

    fn transition_options(original: &ObjectInfo, tier_name: String) -> ObjectOptions {
        ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name,
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        }
    }

    async fn corrupt_shard_at(path: PathBuf, position: ShardCorruptionPosition) {
        let mut bytes = tokio::fs::read(&path).await.expect("committed shard should be readable");
        assert!(!bytes.is_empty(), "committed shard should not be empty");
        let offset = match position {
            ShardCorruptionPosition::First => 0,
            ShardCorruptionPosition::Middle => bytes.len() / 2,
            ShardCorruptionPosition::Last => bytes.len() - 1,
        };
        bytes[offset] ^= 0xff;
        tokio::fs::write(&path, bytes)
            .await
            .expect("committed shard corruption should be written");
    }

    #[derive(Clone, Copy, Debug)]
    enum ShardCorruptionPosition {
        First,
        Middle,
        Last,
    }

    impl ShardCorruptionPosition {
        fn label(self) -> &'static str {
            match self {
                ShardCorruptionPosition::First => "first",
                ShardCorruptionPosition::Middle => "middle",
                ShardCorruptionPosition::Last => "last",
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn transition_uses_transaction_canonical_remote_object_name() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-transaction-remote-object-bucket";
        let object = "object.bin";
        let payload = b"transition remote object must be bound to its transaction id".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let (source_fi, _, online_disks) = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("source metadata should be readable")
            .into_owned();
        let mut source_fi = source_fi;
        rustfs_utils::http::insert_str(
            &mut source_fi.metadata,
            rustfs_utils::http::SUFFIX_PART_CHECKSUMS,
            r#"[[1,[["CRC32C","AAAAAA=="]]]]"#.to_string(),
        );
        set_disks
            .update_object_meta(bucket, object, source_fi, &online_disks)
            .await
            .expect("source checksum sidecar should be persisted");
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let remote_version = Uuid::new_v4().to_string();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_put_remote_version(Some(remote_version.clone())).await;

        set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect("transition should commit");

        let put_versions = backend.put_versions().await;
        assert_eq!(put_versions.len(), 1, "transition should upload one remote candidate");
        let remote_object = &put_versions[0].0;
        assert!(
            remote_object.starts_with(crate::bucket::lifecycle::transition_transaction::TRANSITION_TRANSACTION_PREFIX),
            "remote object should be transaction-scoped: {remote_object}"
        );
        let snapshot = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    include_part_checksums: true,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("committed transition metadata should be readable");
        let fi = snapshot.fi();
        assert_eq!(fi.transition_status, TRANSITION_COMPLETE);
        assert_eq!(fi.transitioned_objname, *remote_object);
        assert_eq!(
            fi.transition_version_id,
            Some(Uuid::parse_str(&remote_version).expect("test version id should parse"))
        );
        assert_eq!(
            fi.parts[0]
                .checksums
                .as_ref()
                .and_then(|checksums| checksums.get("CRC32C"))
                .map(String::as_str),
            Some("AAAAAA==")
        );
        let remote_metadata = backend
            .metadata(remote_object)
            .await
            .expect("remote metadata should be stored");
        assert!(
            !rustfs_utils::http::contains_key_str(&remote_metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS),
            "the internal checksum sidecar must not be uploaded as remote user metadata"
        );
        assert!(backend.contains(remote_object).await, "committed remote object should remain available");
    }

    /// Compresses `plaintext` with the codec the PUT path uses, so the stored
    /// bytes round-trip through the read path's decompressor.
    async fn compress_for_storage(plaintext: &[u8]) -> Vec<u8> {
        let mut reader = crate::io_support::rio::compression_reader(
            Cursor::new(plaintext.to_vec()),
            rustfs_utils::CompressionAlgorithm::default(),
            false,
        );
        let mut compressed = Vec::new();
        reader.read_to_end(&mut compressed).await.expect("plaintext should compress");
        assert!(compressed.len() < plaintext.len(), "test payload must actually compress");
        compressed
    }

    /// Writes a genuinely compressed object: stored data is `compressed`, and the
    /// metadata marks it compressed with the plaintext length as its actual size,
    /// exactly as the app-layer compress path records it.
    async fn write_compressed_source(
        set_disks: &Arc<SetDisks>,
        disk_stores: &[DiskStore],
        bucket: &str,
        object: &str,
        plaintext: &[u8],
        compressed: &[u8],
    ) -> ObjectInfo {
        for disk in disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut user_defined = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut user_defined,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            crate::io_support::rio::compression_metadata_value(rustfs_utils::CompressionAlgorithm::default()),
        );
        rustfs_utils::http::insert_str(&mut user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, plaintext.len().to_string());
        let stream = crate::io_support::rio::HashReader::from_stream(
            Cursor::new(compressed.to_vec()),
            compressed.len() as i64,
            plaintext.len() as i64,
            None,
            None,
            false,
        )
        .expect("hash reader over compressed bytes");
        let mut reader = PutObjReader::new(stream);
        set_disks
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    user_defined,
                    ..Default::default()
                },
            )
            .await
            .expect("compressed object should be written")
    }

    async fn read_transitioned(
        set_disks: &Arc<SetDisks>,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        opts: &ObjectOptions,
    ) -> (Vec<u8>, i64) {
        let mut reader = set_disks
            .get_object_reader(bucket, object, range, HeaderMap::new(), opts)
            .await
            .expect("transitioned object reader should open");
        let published_size = reader.object_info.size;
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("transitioned body should drain");
        (body, published_size)
    }

    /// Transition uploads the object's STORED bytes, so a tiered read has to
    /// apply the same transform an erasure read would. #6107 routed this path
    /// through `ReadPlan` to stop serving an encrypted object's ciphertext;
    /// compression rides the same plan, and nothing pinned it (backlog#1851).
    /// Without the transform this GET returns the compressed bytes under the
    /// compressed size — silent corruption for every client of a compressed
    /// object that ILM has moved to a warm tier.
    #[tokio::test]
    #[serial_test::serial]
    async fn transitioned_compressed_object_get_returns_plaintext() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transitioned-compressed-get-bucket";
        let object = "object.txt";
        let plaintext = b"transitioned compressed objects must decompress on read ".repeat(20_000);
        let compressed = compress_for_storage(&plaintext).await;
        let original = write_compressed_source(&set_disks, &disk_stores, bucket, object, &plaintext, &compressed).await;

        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let (local_body, local_size) = read_transitioned(&set_disks, bucket, object, None, &opts).await;
        assert_eq!(local_body, plaintext, "control: the pre-transition read must decompress");
        assert_eq!(
            local_size,
            plaintext.len() as i64,
            "control: the pre-transition read publishes the plaintext size"
        );

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect("transition should commit");

        let put_versions = backend.put_versions().await;
        assert_eq!(put_versions.len(), 1, "transition should upload one remote candidate");
        let remote_bytes = backend
            .bytes(&put_versions[0].0)
            .await
            .expect("remote candidate should be stored");
        assert_eq!(
            remote_bytes, compressed,
            "transition uploads the stored representation; the read side is what has to decode it"
        );

        let (body, published_size) = read_transitioned(&set_disks, bucket, object, None, &opts).await;
        assert_eq!(body, plaintext, "a tiered read must return the object's content, not its stored bytes");
        assert_eq!(
            published_size,
            plaintext.len() as i64,
            "a tiered read must publish the plaintext size, not the compressed one"
        );
    }

    /// A ranged tiered read is expressed in plaintext coordinates, so the plan
    /// has to translate it into the remote copy's compressed extent and skip
    /// into the decompressed stream — the same translation the erasure path does.
    #[tokio::test]
    #[serial_test::serial]
    async fn transitioned_compressed_object_range_get_returns_plaintext_slice() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transitioned-compressed-range-bucket";
        let object = "object.txt";
        let plaintext = b"ranged reads of transitioned compressed objects must land in plaintext ".repeat(20_000);
        let compressed = compress_for_storage(&plaintext).await;
        let original = write_compressed_source(&set_disks, &disk_stores, bucket, object, &plaintext, &compressed).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect("transition should commit");

        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        // Deliberately past the compressed size, so a range still measured in
        // stored coordinates could not produce this slice.
        let start = compressed.len() as i64 + 4096;
        let end = start + 511;
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start,
            end,
        };
        let (body, published_size) = read_transitioned(&set_disks, bucket, object, Some(range), &opts).await;

        let expected = &plaintext[start as usize..=end as usize];
        assert_eq!(body, expected, "a ranged tiered read must return that plaintext slice");
        assert_eq!(published_size, expected.len() as i64, "a ranged tiered read publishes the slice length");
    }

    /// The restore copy-back re-writes the object under its original metadata,
    /// which still says "compressed". It therefore has to keep receiving the
    /// STORED bytes: `restore_request_active` holds it on the plan's `Plain`
    /// branch, and decompressing there would write plaintext under compressed
    /// metadata.
    #[tokio::test]
    #[serial_test::serial]
    async fn restore_read_of_transitioned_compressed_object_keeps_stored_bytes() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transitioned-compressed-restore-bucket";
        let object = "object.txt";
        let plaintext = b"restore copy-back must keep the stored representation intact ".repeat(20_000);
        let compressed = compress_for_storage(&plaintext).await;
        let original = write_compressed_source(&set_disks, &disk_stores, bucket, object, &plaintext, &compressed).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect("transition should commit");

        let oi = set_disks
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("transitioned metadata should resolve");
        let restore_opts = ObjectOptions {
            no_lock: true,
            part_number: Some(1),
            transition: TransitionOptions {
                restore_request: s3s::dto::RestoreRequest {
                    days: Some(1),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };
        let mut reader = get_transitioned_object_reader_with_tier_manager(
            bucket,
            object,
            &None,
            &HeaderMap::new(),
            &oi,
            &restore_opts,
            &set_disks.ctx.tier_config_mgr(),
            set_disks.ctx.object_encryption_resolver(),
        )
        .await
        .expect("restore read of the tiered copy should open");
        let published_size = reader.object_info.size;
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("restore body should drain");

        assert_eq!(body, compressed, "a restore read must copy the stored bytes back verbatim");
        assert_eq!(
            published_size,
            compressed.len() as i64,
            "a restore read must keep publishing the stored size"
        );
    }

    /// Plain objects must keep streaming the remote bytes through untouched:
    /// their plan is `Plain`, so the tiered read stays byte-identical.
    #[tokio::test]
    #[serial_test::serial]
    async fn transitioned_plain_object_get_is_unchanged() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transitioned-plain-get-bucket";
        let object = "object.bin";
        let payload = b"plain transitioned objects must keep reading back byte-identical ".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect("transition should commit");

        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let (body, published_size) = read_transitioned(&set_disks, bucket, object, None, &opts).await;
        assert_eq!(body, payload);
        assert_eq!(published_size, payload.len() as i64);

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 100,
            end: 611,
        };
        let (ranged_body, ranged_size) = read_transitioned(&set_disks, bucket, object, Some(range), &opts).await;
        assert_eq!(ranged_body, &payload[100..=611]);
        assert_eq!(ranged_size, payload.len() as i64, "a plain ranged read keeps publishing the object size");
    }

    async fn corrupt_beyond_read_quorum(
        temp_dirs: &[tempfile::TempDir],
        bucket: &str,
        object: &str,
        data_dir: Uuid,
        parity_blocks: usize,
        position: ShardCorruptionPosition,
    ) {
        for temp_dir in temp_dirs.iter().take(parity_blocks + 1) {
            let shard = temp_dir
                .path()
                .join(bucket)
                .join(object)
                .join(data_dir.to_string())
                .join("part.1");
            corrupt_shard_at(shard, position).await;
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn remote_put_failure_preserves_error_without_cleanup_candidate() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-remote-put-failure-bucket";
        let object = "object.bin";
        let payload = b"a failed remote PUT must not manufacture a cleanup candidate".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_unreachable(true).await;

        let error = set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect_err("an unreachable tier must fail the remote PUT");
        assert!(
            matches!(error, StorageError::Io(ref err) if err.kind() == std::io::ErrorKind::ConnectionRefused),
            "the original remote PUT error must be preserved: {error:?}"
        );
        assert_eq!(backend.remove_count().await, 0, "an unconfirmed candidate must not be cleaned up");
        assert_eq!(backend.exact_remove_count(), 0, "an unconfirmed candidate must not reach exact cleanup");
        assert_eq!(backend.object_count().await, 0);
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial]
    async fn data_movement_cleanup_aborts_after_outer_lock_loss() {
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let lockers: Vec<Arc<dyn LockClient>> = (0..4)
            .map(|_| Arc::new(LockLostRefreshClient::new(Arc::clone(&refresh_calls))) as Arc<dyn LockClient>)
            .collect();
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "data-movement-cleanup-lock-lost";
        let object = "object.bin";
        let payload = b"lost data movement cleanup lock must preserve the source".repeat(1024);
        temp_env::async_with_vars(
            [(
                crate::set_disk::core::io_primitives::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE,
                Some("false"),
            )],
            async { write_source(&set_disks, &disk_stores, bucket, object, &payload).await },
        )
        .await;
        tokio::time::pause();
        let expected = set_disks
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("source versions should be readable")
            .expect("source versions should exist");
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let barrier = crate::data_movement::SourceCleanupDeleteBarrier::install(bucket, object);

        let cleanup_set = Arc::clone(&set_disks);
        let cleanup = tokio::spawn(async move {
            crate::data_movement::cleanup_source_entry_if_unchanged(
                cleanup_set,
                bucket,
                object,
                &expected,
                &[],
                crate::data_movement::SourceCleanupBucketFence::default(),
                "test_data_movement",
            )
            .await
        });
        barrier.wait_until_paused().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::SeqCst) > 0,
            "test must drive the real distributed-lock heartbeat before cleanup commit"
        );
        barrier.release();

        let error = cleanup
            .await
            .expect("cleanup task should not panic")
            .expect_err("cleanup must fail after its outer namespace lock loses refresh quorum");
        assert!(matches!(
            error,
            crate::data_movement::SourceCleanupError::Storage(StorageError::NamespaceLockQuorumUnavailable { .. })
        ));
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial]
    async fn data_movement_cleanup_aborts_after_bucket_fence_loss() {
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let lockers: Vec<Arc<dyn LockClient>> = (0..4)
            .map(|_| Arc::new(LockLostRefreshClient::new(Arc::clone(&refresh_calls))) as Arc<dyn LockClient>)
            .collect();
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "data-movement-cleanup-bucket-fence-lost";
        let object = "object.bin";
        let payload = b"lost bucket fence must preserve the source".repeat(1024);
        temp_env::async_with_vars(
            [(
                crate::set_disk::core::io_primitives::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE,
                Some("false"),
            )],
            async { write_source(&set_disks, &disk_stores, bucket, object, &payload).await },
        )
        .await;
        tokio::time::pause();
        let expected = set_disks
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("source versions should be readable")
            .expect("source versions should exist");

        let distributed_setup = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let bucket_guard = set_disks
            .new_ns_lock(bucket, crate::storage_api_contracts::bucket::BUCKET_LIFECYCLE_LOCK_OBJECT)
            .await
            .expect("create bucket lifecycle lock")
            .get_read_lock(get_lock_acquire_timeout())
            .await
            .expect("acquire bucket lifecycle read lock");
        let local_cleanup_setup = SetupTypeGuard::switch_to(SetupType::Erasure).await;
        let barrier = crate::data_movement::SourceCleanupDeleteBarrier::install(bucket, object);

        let cleanup_set = Arc::clone(&set_disks);
        let cleanup = tokio::spawn(async move {
            let result = crate::data_movement::cleanup_source_entry_if_unchanged(
                cleanup_set,
                bucket,
                object,
                &expected,
                &[],
                crate::data_movement::SourceCleanupBucketFence {
                    expected_incarnation_id: None,
                    lifecycle_guard: Some(&bucket_guard),
                    namespace_lock_lost_signal: None,
                    ..Default::default()
                },
                "test_data_movement",
            )
            .await;
            drop(bucket_guard);
            result
        });
        barrier.wait_until_paused().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(refresh_calls.load(Ordering::SeqCst) > 0, "bucket guard refresh must run before commit");
        barrier.release();

        let error = cleanup
            .await
            .expect("cleanup task should not panic")
            .expect_err("cleanup must fail after its bucket lifecycle fence loses refresh quorum");
        assert!(matches!(
            error,
            crate::data_movement::SourceCleanupError::Storage(StorageError::NamespaceLockQuorumUnavailable { .. })
        ));
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
        drop(local_cleanup_setup);
        drop(distributed_setup);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn partial_remote_acceptance_cleans_exact_candidate_and_preserves_source() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-partial-accept-bucket";
        let object = "object.bin";
        let payload = vec![0x5a; 2 * 1024 * 1024];
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let remote_version = Uuid::nil().to_string();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_put_read_limit(Some(4096)).await;
        backend.set_put_remote_version(Some(remote_version.clone())).await;

        let error = set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect_err("accepting only a prefix must fail transition completion");
        assert!(matches!(error, StorageError::Io(_) | StorageError::LessData));
        let removed_versions = backend.remove_versions().await;
        assert_eq!(removed_versions.len(), 1);
        assert_eq!(
            removed_versions[0].1, remote_version,
            "a non-empty fresh PUT response must be used as the exact cleanup constraint"
        );
        assert_eq!(backend.object_count().await, 0);
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn commit_lock_acquire_failure_cleans_remote_candidate_and_preserves_source() {
        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let lockers: Vec<Arc<dyn LockClient>> = vec![Arc::new(LocalClient::with_manager(manager)), Arc::new(FailingLockClient)];
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "transition-lock-acquire-failure-bucket";
        let object = "object.bin";
        let payload = b"transition commit lock failure must clean the remote candidate".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let mut opts = transition_options(&original, tier_name);
        opts.no_lock = false;

        let error = set_disks
            .transition_object(bucket, object, &opts)
            .await
            .expect_err("transition must fail when the commit namespace write lock cannot reach quorum");
        assert!(
            matches!(
                error,
                StorageError::NamespaceLockQuorumUnavailable { .. }
                    | StorageError::Lock(LockError::QuorumNotReached { .. })
                    | StorageError::Lock(LockError::Internal { .. })
            ),
            "unexpected transition lock-acquire error: {error:?}"
        );
        assert_eq!(
            backend.put_count().await,
            1,
            "remote candidate must be uploaded before commit lock failure"
        );
        assert_eq!(
            backend.remove_count().await,
            1,
            "known remote candidate must be removed when commit lock acquisition fails"
        );
        assert_eq!(
            backend.remove_versions().await,
            backend.put_versions().await,
            "lock-acquire cleanup must target the exact remote version returned by PUT"
        );
        assert_eq!(
            backend.object_count().await,
            0,
            "commit lock failure must not leave an orphan remote candidate"
        );
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial]
    async fn commit_lock_lost_after_upload_cleans_remote_candidate_and_preserves_source() {
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let lockers: Vec<Arc<dyn LockClient>> = (0..4)
            .map(|_| Arc::new(LockLostRefreshClient::new(Arc::clone(&refresh_calls))) as Arc<dyn LockClient>)
            .collect();
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "transition-lock-lost-bucket";
        let object = "object.bin";
        let payload = b"lost transition commit lock must clean the remote candidate".repeat(1024);
        let original = temp_env::async_with_vars(
            [(
                crate::set_disk::core::io_primitives::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE,
                Some("false"),
            )],
            async { write_source(&set_disks, &disk_stores, bucket, object, &payload).await },
        )
        .await;
        tokio::time::pause();
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let mut opts = transition_options(&original, tier_name);
        opts.no_lock = false;
        let barrier = TransitionCommitBarrier::install_before_lock_lost_check(bucket, object);

        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::SeqCst) > 0,
            "test must drive the real distributed-lock heartbeat before the commit fence"
        );
        barrier.release();

        let error = transition
            .await
            .expect("transition task should not panic")
            .expect_err("transition must fail after the commit namespace write lock loses refresh quorum");
        assert!(
            matches!(error, StorageError::NamespaceLockQuorumUnavailable { .. }),
            "unexpected transition lock-lost error: {error:?}"
        );
        assert_eq!(backend.put_count().await, 1);
        assert_eq!(
            backend.remove_count().await,
            1,
            "lock-lost commit fence must remove the uploaded remote candidate"
        );
        assert_eq!(
            backend.remove_versions().await,
            backend.put_versions().await,
            "lock-lost cleanup must target the exact remote version returned by PUT"
        );
        assert_eq!(
            backend.object_count().await,
            0,
            "lock-lost commit fence must not leave an orphan remote candidate"
        );
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn cancelled_after_remote_upload_cleans_candidate_and_preserves_source() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-cancel-after-upload-bucket";
        let object = "object.bin";
        let payload = b"cancelled transition after upload must clean remote candidate".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let _compaction_proof = install_transition_transaction_compaction_fleet_proof_for_test("object-transaction-fencing-test");
        let barrier = TransitionCommitBarrier::install(bucket, object);

        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move {
            transition_set
                .transition_object(bucket, object, &transition_options(&original, tier_name))
                .await
        });
        barrier.wait_until_paused().await;
        assert_eq!(
            backend.put_count().await,
            1,
            "transition must upload a remote candidate before the cancellation point"
        );
        let put_versions = backend.put_versions().await;
        assert_eq!(put_versions.len(), 1, "transition should expose one remote candidate/version");
        assert_eq!(backend.object_count().await, 1, "remote candidate should be visible before cancellation");

        transition.abort();
        let join = transition
            .await
            .expect_err("aborted transition task should report cancellation");
        assert!(join.is_cancelled(), "transition task should be cancelled, not panic");
        drop(barrier);

        assert!(
            backend
                .wait_for_remote_absence(&put_versions[0].0, Duration::from_secs(5))
                .await,
            "cancellation cleanup must remove the uploaded remote candidate"
        );
        assert_eq!(
            backend.remove_versions().await,
            put_versions,
            "cancellation cleanup must target the exact remote version returned by PUT"
        );
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn cancelled_after_put_response_before_upload_join_cleans_candidate() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-cancel-before-upload-join-bucket";
        let object = "object.bin";
        let payload = b"a confirmed remote upload must survive cancellation until cleanup owns it".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let barrier = TransitionUploadCandidateBarrier::install();

        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move {
            transition_set
                .transition_object(bucket, object, &transition_options(&original, tier_name))
                .await
        });
        barrier.wait_until_paused().await;
        let put_versions = backend.put_versions().await;
        assert_eq!(put_versions.len(), 1, "the remote PUT response must identify one cleanup candidate");
        assert_eq!(backend.object_count().await, 1, "the candidate must exist at the cancellation point");

        transition.abort();
        assert!(
            transition
                .await
                .expect_err("aborted transition task should report cancellation")
                .is_cancelled()
        );
        drop(barrier);

        assert!(
            backend
                .wait_for_remote_absence(&put_versions[0].0, Duration::from_secs(5))
                .await,
            "the pre-created cleanup guard must remove a candidate recorded before upload finalization completes"
        );
        assert_eq!(backend.remove_versions().await, put_versions);
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_bitrot_producer_failures_do_not_commit_transition() {
        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;

        for position in [
            ShardCorruptionPosition::First,
            ShardCorruptionPosition::Middle,
            ShardCorruptionPosition::Last,
        ] {
            let bucket = format!("transition-real-bitrot-{}", position.label());
            let object = format!("{}-corrupt.bin", position.label());
            let payload = vec![0x41; 2 * 1024 * 1024];
            // Corruption edits physical shards, so every healthy rename must finish first.
            for disk in &disk_stores {
                disk.make_volume(&bucket).await.expect("bucket volume should be created");
            }
            let mut reader = PutObjReader::from_vec(payload.to_vec());
            let original = set_disks
                .put_object(
                    &bucket,
                    &object,
                    &mut reader,
                    &ObjectOptions {
                        write_completion: WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("source object should be written");
            let source = set_disks
                .get_object_fileinfo(
                    &bucket,
                    &object,
                    &ObjectOptions {
                        no_lock: true,
                        metadata_cache_safe: false,
                        ..Default::default()
                    },
                    true,
                    false,
                )
                .await
                .expect("source metadata should be available before shard corruption");
            let source = source.fi();
            let data_dir = source.data_dir.expect("source object should have a data directory");

            corrupt_beyond_read_quorum(&temp_dirs, &bucket, &object, data_dir, source.erasure.parity_blocks, position).await;

            let error = set_disks
                .transition_object(&bucket, &object, &transition_options(&original, tier_name.clone()))
                .await
                .expect_err("producer bitrot failure must not commit transition metadata");
            assert!(
                matches!(
                    error,
                    StorageError::FileCorrupt
                        | StorageError::ErasureReadQuorum
                        | StorageError::InsufficientReadQuorum(_, _)
                        | StorageError::LessData
                        | StorageError::Io(_)
                ),
                "{position:?}: unexpected transition producer error: {error:?}"
            );
            let after = set_disks
                .get_object_fileinfo(
                    &bucket,
                    &object,
                    &ObjectOptions {
                        no_lock: true,
                        metadata_cache_safe: false,
                        ..Default::default()
                    },
                    true,
                    false,
                )
                .await
                .expect("failed transition must leave metadata readable");
            let after = after.fi();
            assert_eq!(
                after.data_dir,
                Some(data_dir),
                "{position:?}: transition must not release the source data dir"
            );
            assert_ne!(
                after.transition_status, TRANSITION_COMPLETE,
                "{position:?}: transition status must remain incomplete after producer bitrot failure"
            );
        }

        assert_eq!(
            backend.put_count().await,
            3,
            "each bitrot case should create one remote cleanup candidate"
        );
        assert_eq!(backend.remove_count().await, 3, "each bitrot candidate must be removed before returning");
        assert_eq!(
            backend.remove_versions().await,
            backend.put_versions().await,
            "bitrot cleanup must target the exact remote version returned by PUT"
        );
        assert_eq!(
            backend.object_count().await,
            0,
            "bitrot producer failures must not leave remote candidates"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn unversioned_remote_version_is_persisted_without_version_id() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-unversioned-tier-bucket";
        let object = "object.bin";
        let payload = b"unversioned remote tier must commit without a version id".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_put_remote_version(Some(String::new())).await;
        let save_probe = TransitionTransactionMutationProbe::install(bucket, object);

        set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect("an unversioned remote version must commit");
        let snapshot = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("committed unversioned transition metadata should be readable");
        let fi = snapshot.fi();
        assert_eq!(fi.transition_version_id, None);
        assert_eq!(fi.transition_version, None);
        assert_eq!(fi.transition_version_state, rustfs_filemeta::TransitionVersionState::KnownDisabled);
        assert_eq!(save_probe.attempts(), 1);
        assert_eq!(backend.remove_count().await, 0);
        assert_eq!(backend.object_count().await, 1);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn opaque_remote_version_is_cleaned_before_parse_failure() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-unknown-version-bucket";
        let object = "object.bin";
        let payload = b"unknown remote version must retain local data".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_put_remote_version(Some("opaque-version-token".to_string())).await;

        set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect_err("an opaque remote version must fail closed until the capability gate is active");
        let removed_versions = backend.remove_versions().await;
        assert_eq!(removed_versions.len(), 1);
        assert_eq!(removed_versions[0].1, "opaque-version-token");
        assert_eq!(backend.object_count().await, 0);
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn nil_remote_version_is_cleaned_exactly_before_transaction_persistence() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-nil-version-bucket";
        let object = "object.bin";
        let payload = b"nil remote version must retain local data".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let remote_version = Uuid::nil().to_string();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_put_remote_version(Some(remote_version.clone())).await;
        let save_probe = TransitionTransactionMutationProbe::install(bucket, object);

        set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect_err("a nil remote version must fail closed before transaction persistence");
        let put_versions = backend.put_versions().await;
        let removed_versions = backend.remove_versions().await;
        assert_eq!(removed_versions, put_versions);
        assert_eq!(removed_versions.len(), 1);
        assert_eq!(
            removed_versions.first().map(|(_, version)| version.as_str()),
            Some(remote_version.as_str())
        );
        assert_eq!(save_probe.attempts(), 0, "nil remote version must be rejected before saving Uploaded");
        assert_eq!(backend.exact_remove_count(), 1);
        assert_eq!(backend.object_count().await, 0);
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn authoritative_read_failure_after_upload_cleans_exact_candidate_and_preserves_source() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-authoritative-read-failure-bucket";
        let object = "object.bin";
        let payload = b"authoritative metadata read failure must clean remote candidate".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let put_barrier = backend.arm_put_barrier().await;

        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move {
            transition_set
                .transition_object(bucket, object, &transition_options(&original, tier_name))
                .await
        });
        put_barrier.wait_until_paused().await;
        let saved_disks = {
            let mut disks = set_disks.disks.write().await;
            let saved = std::mem::take(&mut *disks);
            *disks = vec![None; saved.len()];
            saved
        };
        put_barrier.release();

        let err = transition
            .await
            .expect("transition task should not panic")
            .expect_err("authoritative metadata read failure must fail the transition");
        *set_disks.disks.write().await = saved_disks;
        assert!(
            matches!(
                err,
                StorageError::ErasureReadQuorum | StorageError::InsufficientReadQuorum(_, _) | StorageError::Io(_)
            ),
            "unexpected authoritative read failure: {err:?}"
        );
        assert_eq!(backend.put_count().await, 1);
        assert_eq!(backend.remove_count().await, 1);
        assert_eq!(
            backend.remove_versions().await,
            backend.put_versions().await,
            "authoritative read failure must remove the exact uploaded remote version"
        );
        assert_eq!(backend.object_count().await, 0);
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn backend_version_constraint_rejects_uuid_candidate_before_commit() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-version-constraint-bucket";
        let object = "object.bin";
        let payload = b"version-constrained backend candidate must retain local data".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let remote_version = Uuid::new_v4().to_string();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_put_remote_version(Some(remote_version.clone())).await;
        backend.reject_next_non_empty_remote_version_validation();

        set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect_err("a backend requiring unversioned objects must reject a UUID-shaped version");

        let put_versions = backend.put_versions().await;
        let removed_versions = backend.remove_versions().await;
        assert_eq!(removed_versions, put_versions);
        assert_eq!(removed_versions.len(), 1);
        assert_eq!(removed_versions[0].1, remote_version);
        assert_eq!(backend.object_count().await, 0);
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn backend_version_constraint_cleans_nil_uuid_with_exact_version() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-nil-version-constraint-bucket";
        let object = "object.bin";
        let payload = b"a fresh nil UUID response remains an exact cleanup constraint".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let remote_version = Uuid::nil().to_string();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_put_remote_version(Some(remote_version.clone())).await;
        backend.reject_next_non_empty_remote_version_validation();

        set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect_err("a backend requiring an empty version must reject a nil UUID string");

        let put_versions = backend.put_versions().await;
        let removed_versions = backend.remove_versions().await;
        assert_eq!(removed_versions.len(), 1);
        assert_eq!(removed_versions[0].0, put_versions[0].0);
        assert_eq!(removed_versions[0].1, remote_version);
        assert_eq!(backend.exact_remove_count(), 1);
        assert_eq!(backend.object_count().await, 0);
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial]
    async fn tagging_lock_lost_before_metadata_write_fails_closed() {
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let lockers: Vec<Arc<dyn LockClient>> = (0..4)
            .map(|_| Arc::new(LockLostRefreshClient::new(Arc::clone(&refresh_calls))) as Arc<dyn LockClient>)
            .collect();
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "tagging-lock-lost-bucket";
        let object = "object.bin";
        temp_env::async_with_vars(
            [(
                crate::set_disk::core::io_primitives::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE,
                Some("false"),
            )],
            async { write_source(&set_disks, &disk_stores, bucket, object, b"tagging source").await },
        )
        .await;
        tokio::time::pause();

        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let barrier = ObjectTaggingCommitBarrier::install(bucket, object);
        let tagging_set = Arc::clone(&set_disks);
        let tagging = tokio::spawn(async move {
            tagging_set
                .put_object_tags(bucket, object, "must=not-commit", &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::SeqCst) > 0,
            "test must drive the real distributed-lock heartbeat before the tagging commit fence"
        );
        barrier.release();

        let error = tagging
            .await
            .expect("tagging task should not panic")
            .expect_err("tagging must fail after its namespace lock loses refresh quorum");
        assert!(
            matches!(error, StorageError::NamespaceLockQuorumUnavailable { .. }),
            "unexpected tagging lock-lost error: {error:?}"
        );
        let snapshot = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
                false,
                false,
            )
            .await
            .expect("source metadata should remain readable");
        assert!(
            !snapshot.fi().metadata.contains_key(AMZ_OBJECT_TAGGING),
            "a stale tagging writer must not write metadata after refresh-quorum loss"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn remote_cleanup_failure_after_version_rejection_preserves_source_and_candidate() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-cleanup-failure-bucket";
        let object = "object.bin";
        let payload = b"cleanup failure must keep source and orphan evidence".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_put_remote_version(Some("opaque-version-token".to_string())).await;
        let put_barrier = backend.arm_put_barrier().await;

        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move {
            transition_set
                .transition_object(bucket, object, &transition_options(&original, tier_name))
                .await
        });
        put_barrier.wait_until_paused().await;
        backend.set_server_error(true).await;
        put_barrier.release();

        transition
            .await
            .expect("transition task should not panic")
            .expect_err("opaque remote version must fail closed even if cleanup also fails");
        assert_eq!(backend.put_count().await, 1);
        assert_eq!(
            backend.remove_count().await,
            0,
            "cleanup failure before backend remove must not be reported as a successful exact delete"
        );
        assert_eq!(
            backend.object_count().await,
            1,
            "failed cleanup must leave the remote candidate visible for durable reconciliation"
        );
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }
}

#[cfg(all(test, feature = "test-util"))]
mod transition_source_identity_matrix_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks;
    use super::*;
    use crate::bucket::lifecycle::lifecycle::{TRANSITION_PENDING, TransitionOptions};
    use crate::services::tier::test_util::register_mock_tier;

    #[test]
    fn transition_source_identity_treats_nil_version_as_null_source() {
        let fi = FileInfo {
            version_id: Some(Uuid::nil()),
            data_dir: Some(Uuid::new_v4()),
            mod_time: Some(OffsetDateTime::now_utc()),
            size: 1,
            ..Default::default()
        };
        let opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };

        let source = transition_source_identity("bucket", "object", &fi, &opts, "etag")
            .expect("nil source version should build a null-version identity");

        assert_eq!(source.version_mode, TransitionSourceVersionMode::VersionSuspended);
        assert_eq!(source.version_id, None);
        source.validate().expect("null-version source identity should validate");
    }

    #[test]
    fn transition_source_identity_treats_requested_nil_version_as_null_source() {
        let fi = FileInfo {
            version_id: None,
            data_dir: Some(Uuid::new_v4()),
            mod_time: Some(OffsetDateTime::now_utc()),
            size: 1,
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some(Uuid::nil().to_string()),
            versioned: true,
            ..Default::default()
        };

        let source = transition_source_identity("bucket", "object", &fi, &opts, "etag")
            .expect("requested nil source version should build a null-version identity");

        assert_eq!(source.version_mode, TransitionSourceVersionMode::VersionSuspended);
        assert_eq!(source.version_id, None);
        source
            .validate()
            .expect("requested null-version source identity should validate");
    }

    #[test]
    fn transition_source_identity_treats_unversioned_fileinfo_as_null_source() {
        let fi = FileInfo {
            version_id: None,
            versioned: false,
            data_dir: Some(Uuid::new_v4()),
            mod_time: Some(OffsetDateTime::now_utc()),
            size: 1,
            ..Default::default()
        };
        let opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };

        let source = transition_source_identity("bucket", "object", &fi, &opts, "etag")
            .expect("unversioned fileinfo should build a null-version identity");

        assert_eq!(source.version_mode, TransitionSourceVersionMode::VersionSuspended);
        assert_eq!(source.version_id, None);
        source
            .validate()
            .expect("unversioned fileinfo null-version source identity should validate");
    }

    #[test]
    fn transition_source_identity_still_rejects_missing_versioned_source_id() {
        let fi = FileInfo {
            version_id: None,
            versioned: true,
            data_dir: Some(Uuid::new_v4()),
            mod_time: Some(OffsetDateTime::now_utc()),
            size: 1,
            ..Default::default()
        };
        let opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };

        let source = transition_source_identity("bucket", "object", &fi, &opts, "etag")
            .expect("source identity construction should preserve missing version evidence");

        assert_eq!(source.version_mode, TransitionSourceVersionMode::Versioned);
        assert!(matches!(
            source.validate(),
            Err(crate::bucket::lifecycle::transition_transaction::TransitionTransactionError::Corrupt(
                "versioned source is missing version_id"
            ))
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn transition_source_identity_field_matrix_rejects_single_field_drift() {
        #[derive(Clone, Copy, Debug)]
        enum IdentityField {
            DataDir,
            ModTime,
            Size,
            Etag,
        }

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-identity-matrix-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;

        for (index, field) in [
            IdentityField::DataDir,
            IdentityField::ModTime,
            IdentityField::Size,
            IdentityField::Etag,
        ]
        .into_iter()
        .enumerate()
        {
            let object = format!("identity-{index}.bin");
            let payload = vec![u8::try_from(index + 1).expect("matrix index should fit u8"); 1024 * 1024];
            let mut reader = PutObjReader::from_vec(payload);
            let source_version_id = Uuid::new_v4();
            let source_opts = ObjectOptions {
                version_id: Some(source_version_id.to_string()),
                versioned: true,
                ..Default::default()
            };
            let original = set_disks
                .put_object(bucket, &object, &mut reader, &source_opts)
                .await
                .expect("source object should be written");
            let source = set_disks
                .get_object_fileinfo(bucket, &object, &source_opts, true, false)
                .await
                .expect("source metadata should resolve");
            let source = source.fi();
            assert_eq!(source.version_id, Some(source_version_id));
            assert_eq!(
                transition_source_identity(bucket, &object, source, &source_opts, &get_raw_etag(&source.metadata))
                    .expect("persisted versioned source identity should build")
                    .version_mode,
                TransitionSourceVersionMode::Versioned
            );
            let opts = ObjectOptions {
                no_lock: true,
                versioned: true,
                transition: TransitionOptions {
                    status: TRANSITION_PENDING.to_string(),
                    tier: tier_name.clone(),
                    etag: original.etag.clone().unwrap_or_default(),
                    ..Default::default()
                },
                mod_time: original.mod_time,
                ..Default::default()
            };
            let put_barrier = backend.arm_put_barrier().await;
            let transition_set = Arc::clone(&set_disks);
            let transition_object = object.clone();
            let transition =
                tokio::spawn(async move { transition_set.transition_object(bucket, &transition_object, &opts).await });
            put_barrier.wait_until_paused().await;

            let mut changed = source.clone();
            match field {
                IdentityField::DataDir => changed.data_dir = Some(Uuid::new_v4()),
                IdentityField::ModTime => {
                    changed.mod_time = changed.mod_time.map(|value| value + time::Duration::nanoseconds(1));
                }
                IdentityField::Size => changed.size += 1,
                IdentityField::Etag => {
                    changed.metadata.insert("etag".to_string(), format!("changed-{index}"));
                }
            }
            // Replace the object version list so VersionId drift removes the
            // accepted source version instead of appending a second version.
            changed.fresh = true;
            for disk in &disk_stores {
                disk.write_metadata("", bucket, &object, changed.clone())
                    .await
                    .expect("single-field metadata drift should be written");
            }
            let persisted_opts = ObjectOptions {
                version_id: changed.version_id.map(|version_id| version_id.to_string()),
                versioned: true,
                ..Default::default()
            };
            let persisted = set_disks
                .get_object_fileinfo(bucket, &object, &persisted_opts, true, false)
                .await
                .expect("drifted source metadata should resolve");
            let persisted = persisted.fi();
            put_barrier.release();

            let result = transition.await.expect("transition task should not panic");
            assert!(result.is_err(), "transition must reject {field:?} drift");
            let expected_attempts = index + 1;
            assert_eq!(backend.put_count().await, expected_attempts);
            assert_eq!(backend.remove_count().await, expected_attempts);
            assert_eq!(
                backend.remove_versions().await,
                backend.put_versions().await,
                "rejected identity drift must remove the exact uploaded version"
            );

            match field {
                IdentityField::DataDir => assert_ne!(source.data_dir, persisted.data_dir),
                IdentityField::ModTime => assert_ne!(source.mod_time, persisted.mod_time),
                IdentityField::Size => assert_ne!(source.size, persisted.size),
                IdentityField::Etag => assert_ne!(get_raw_etag(&source.metadata), get_raw_etag(&persisted.metadata)),
            }
            assert_eq!(source.version_id, persisted.version_id);
            if !matches!(field, IdentityField::DataDir) {
                assert_eq!(source.data_dir, persisted.data_dir);
            }
            if !matches!(field, IdentityField::ModTime) {
                assert_eq!(source.mod_time, persisted.mod_time);
            }
            if !matches!(field, IdentityField::Size) {
                assert_eq!(source.size, persisted.size);
            }
            if !matches!(field, IdentityField::Etag) {
                assert_eq!(get_raw_etag(&source.metadata), get_raw_etag(&persisted.metadata));
            }
        }
    }
}

#[cfg(test)]
mod heterogeneous_pool_put_tests {
    use super::hermetic_set_disks_support::{
        hermetic_set_disks_for_pool_with_default_parity_isolated as hermetic_set_disks_for_pool_with_default_parity,
        hermetic_set_disks_isolated as hermetic_set_disks,
    };
    use super::*;
    use crate::config::storageclass::lookup_config_for_pools_without_env;
    use crate::disk::ReadOptions;
    use crate::services::notification_sys::install_remote_version_state_fleet_proof_for_test;
    use rustfs_config::server_config::KVS;
    use serial_test::serial;
    use tokio::io::AsyncReadExt;

    async fn make_bucket(disks: &[DiskStore], bucket: &str) {
        for disk in disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
    }

    async fn object_transaction_epochs(disks: &[DiskStore], bucket: &str, object: &str) -> Vec<Option<Uuid>> {
        let mut epochs = Vec::with_capacity(disks.len());
        for (disk_index, disk) in disks.iter().enumerate() {
            let file_info = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .unwrap_or_else(|err| panic!("disk {disk_index} should persist object metadata: {err}"));
            epochs.push(
                file_info
                    .object_transaction_epoch()
                    .unwrap_or_else(|err| panic!("disk {disk_index} transaction epoch should decode: {err}")),
            );
        }
        epochs
    }

    async fn current_data_dir(disk: &DiskStore, bucket: &str, object: &str) -> Uuid {
        disk.read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("current object metadata should read")
            .data_dir
            .expect("test object should be stored out-of-line")
    }

    async fn data_dir_exists(disk: &DiskStore, bucket: &str, object: &str, data_dir: Uuid) -> bool {
        disk.read_all(bucket, &format!("{object}/{data_dir}/part.1")).await.is_ok()
    }

    async fn cleanup_receipt_exists(disk: &DiskStore, bucket: &str, object: &str, data_dir: Uuid) -> bool {
        disk.read_all(bucket, &old_data_cleanup_receipt_path(object, data_dir))
            .await
            .is_ok()
    }

    fn large_payload(fill: u8) -> Vec<u8> {
        vec![fill; 1024 * 1024]
    }

    #[tokio::test]
    async fn second_pool_regular_put_uses_its_own_layout_and_round_trips() {
        // Deliberately inject the first pool's invalid scalar fallback. The
        // test can pass only if the production PUT uses the held [4, 2]
        // storage-class snapshot and resolves pool 1 to parity 1.
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_for_pool_with_default_parity(2, 1, 2).await;
        set_disks.set_test_storage_class_config(
            lookup_config_for_pools_without_env(&KVS::new(), &[4, 2]).expect("heterogeneous pool storage class should resolve"),
        );

        let bucket = "regular-put-second-pool-bucket";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let payload = vec![0x3c; 4096];
        let mut reader = PutObjReader::from_vec(payload.clone());
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("second-pool regular PUT should encode without zero data shards");

        for (disk_index, disk) in disk_stores.iter().enumerate() {
            let file_info = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .unwrap_or_else(|err| panic!("disk {disk_index} should persist valid second-pool metadata: {err}"));
            assert_eq!(file_info.erasure.data_blocks, 1);
            assert_eq!(file_info.erasure.parity_blocks, 1);
        }

        let mut object_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("second-pool regular PUT should be readable");
        let mut restored = Vec::new();
        object_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("second-pool regular PUT should stream");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    #[serial(storage_class_env)]
    async fn object_transaction_fencing_persists_epoch_only_when_gate_is_enabled() {
        let _proof = install_remote_version_state_fleet_proof_for_test("object-transaction-fencing-test");
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-object-transaction-epoch";
        make_bucket(&disk_stores, bucket).await;

        let mut default_reader = PutObjReader::from_vec(b"default gate stays epoch-free".to_vec());
        // Both epoch readbacks below inspect every disk right after PUT. A
        // lock-owning PUT may quorum-ack before its rename tail drains, so keep
        // these commits on the full-fanout path; the fencing gate itself is
        // driven by the fleet proof and env vars, never by the lock option.
        set_disks
            .put_object(
                bucket,
                "default.bin",
                &mut default_reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("default PUT should commit");
        assert_eq!(
            object_transaction_epochs(&disk_stores, bucket, "default.bin").await,
            vec![None, None, None, None],
            "live proof alone must not write epoch metadata while the opt-in gate is disabled"
        );

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_WRITE, Some("true")),
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED, Some("true")),
            ],
            async {
                let mut fenced_reader = PutObjReader::from_vec(b"fenced epoch commit".to_vec());
                set_disks
                    .put_object(
                        bucket,
                        "fenced.bin",
                        &mut fenced_reader,
                        &ObjectOptions {
                            no_lock: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("fenced PUT should commit with a live proof");
            },
        )
        .await;

        let epochs = object_transaction_epochs(&disk_stores, bucket, "fenced.bin").await;
        let first = epochs[0].expect("fenced PUT should persist an epoch");
        assert!(!first.is_nil());
        assert!(epochs.into_iter().all(|epoch| epoch == Some(first)));
    }

    #[tokio::test]
    #[serial(storage_class_env)]
    async fn old_data_cleanup_receipt_reconciles_failed_put_cleanup_idempotently() {
        use crate::set_disk::core::io_primitives::cleanup_fault_injection;

        let _proof = install_remote_version_state_fleet_proof_for_test("object-transaction-fencing-test");
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-cleanup-receipt-reconcile";
        let object = "object.bin";
        make_bucket(&disk_stores, bucket).await;

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_WRITE, Some("true")),
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED, Some("true")),
                (
                    crate::set_disk::core::io_primitives::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE,
                    Some("false"),
                ),
            ],
            async {
                let mut first_reader = PutObjReader::from_vec(large_payload(0x11));
                set_disks
                    .put_object(bucket, object, &mut first_reader, &ObjectOptions::default())
                    .await
                    .expect("first fenced PUT should commit");
                let old_dir = current_data_dir(&disk_stores[0], bucket, object).await;

                let fault = cleanup_fault_injection::fail_cleanup_on(object, &[0, 1, 2, 3]);
                let mut overwrite_reader = PutObjReader::from_vec(large_payload(0x22));
                set_disks
                    .put_object(bucket, object, &mut overwrite_reader, &ObjectOptions::default())
                    .await
                    .expect("overwrite should commit even when old-data cleanup fails");
                for disk in &disk_stores {
                    assert!(
                        cleanup_receipt_exists(disk, bucket, object, old_dir).await,
                        "fenced failed cleanup should leave a durable receipt"
                    );
                    assert!(
                        data_dir_exists(disk, bucket, object, old_dir).await,
                        "injected cleanup failure should leave the old data dir for reconciliation"
                    );
                }
                drop(fault);

                let removed = set_disks
                    .reconcile_old_data_cleanup_receipts(bucket, object)
                    .await
                    .expect("receipt reconciliation should succeed");
                assert_eq!(removed, 4, "all receipt-targeted old dirs should be reclaimed");
                let repeated = set_disks
                    .reconcile_old_data_cleanup_receipts(bucket, object)
                    .await
                    .expect("receipt reconciliation should be idempotent");
                assert_eq!(repeated, 0, "a drained receipt must not be counted again");
                for disk in &disk_stores {
                    assert!(
                        !data_dir_exists(disk, bucket, object, old_dir).await,
                        "reconciled old data dir must be gone"
                    );
                }
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial(storage_class_env)]
    async fn old_data_cleanup_receipt_noops_after_epoch_mismatch() {
        use crate::set_disk::core::io_primitives::cleanup_fault_injection;

        let _proof = install_remote_version_state_fleet_proof_for_test("object-transaction-fencing-test");
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-cleanup-receipt-epoch-mismatch";
        let object = "object.bin";
        make_bucket(&disk_stores, bucket).await;

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_WRITE, Some("true")),
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED, Some("true")),
                (
                    crate::set_disk::core::io_primitives::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE,
                    Some("false"),
                ),
            ],
            async {
                let mut first_reader = PutObjReader::from_vec(large_payload(0x31));
                set_disks
                    .put_object(bucket, object, &mut first_reader, &ObjectOptions::default())
                    .await
                    .expect("first fenced PUT should commit");
                let old_dir = current_data_dir(&disk_stores[0], bucket, object).await;

                let fault = cleanup_fault_injection::fail_cleanup_on(object, &[0, 1, 2, 3]);
                let mut second_reader = PutObjReader::from_vec(large_payload(0x32));
                set_disks
                    .put_object(bucket, object, &mut second_reader, &ObjectOptions::default())
                    .await
                    .expect("second fenced PUT should commit and leave a receipt");
                drop(fault);

                let mut third_reader = PutObjReader::from_vec(large_payload(0x33));
                set_disks
                    .put_object(bucket, object, &mut third_reader, &ObjectOptions::default())
                    .await
                    .expect("third fenced PUT should advance the current epoch");

                let removed = set_disks
                    .reconcile_old_data_cleanup_receipts(bucket, object)
                    .await
                    .expect("stale receipt reconciliation should succeed");
                assert_eq!(removed, 0, "stale receipt epoch must not reclaim after a newer commit");
                for disk in &disk_stores {
                    assert!(
                        cleanup_receipt_exists(disk, bucket, object, old_dir).await,
                        "stale receipt should remain as no-op evidence"
                    );
                    assert!(
                        data_dir_exists(disk, bucket, object, old_dir).await,
                        "epoch mismatch must preserve the old receipt target"
                    );
                }
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial(storage_class_env)]
    async fn old_data_cleanup_receipt_is_not_persisted_without_epoch_gate() {
        use crate::set_disk::core::io_primitives::cleanup_fault_injection;

        let _proof = install_remote_version_state_fleet_proof_for_test("object-transaction-fencing-test");
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-cleanup-receipt-gate-off";
        let object = "object.bin";
        make_bucket(&disk_stores, bucket).await;

        let mut first_reader = PutObjReader::from_vec(large_payload(0x41));
        set_disks
            .put_object(bucket, object, &mut first_reader, &ObjectOptions::default())
            .await
            .expect("default-gate first PUT should commit");
        let old_dir = current_data_dir(&disk_stores[0], bucket, object).await;

        let _fault = cleanup_fault_injection::fail_cleanup_on(object, &[0, 1, 2, 3]);
        let mut second_reader = PutObjReader::from_vec(large_payload(0x42));
        set_disks
            .put_object(bucket, object, &mut second_reader, &ObjectOptions::default())
            .await
            .expect("default-gate overwrite should commit");

        let removed = set_disks
            .reconcile_old_data_cleanup_receipts(bucket, object)
            .await
            .expect("gate-off receipt reconciliation should be a no-op");
        assert_eq!(removed, 0, "mixed-version/default gate path must not consume epoch-dependent receipts");
        for disk in &disk_stores {
            assert!(
                !cleanup_receipt_exists(disk, bucket, object, old_dir).await,
                "mixed-version/default gate path must not write epoch-dependent receipts"
            );
            assert!(
                data_dir_exists(disk, bucket, object, old_dir).await,
                "cleanup fault should leave old dir without receipt"
            );
        }
    }

    #[tokio::test]
    #[serial(storage_class_env)]
    async fn object_transaction_fencing_rejects_stale_no_lock_put_epoch() {
        let _proof = install_remote_version_state_fleet_proof_for_test("object-transaction-fencing-test");
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-object-transaction-stale-epoch";
        let object = "object.bin";
        make_bucket(&disk_stores, bucket).await;

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_WRITE, Some("true")),
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED, Some("true")),
            ],
            async {
                let mut initial_reader = PutObjReader::from_vec(b"initial fenced body".to_vec());
                set_disks
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
                    .expect("initial fenced PUT should commit");
                let initial_epoch = object_transaction_epochs(&disk_stores, bucket, object)
                    .await
                    .into_iter()
                    .next()
                    .flatten()
                    .expect("initial fenced PUT should persist an epoch");

                let barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::BeforeTransactionEpochVerify);
                let stale_set = Arc::clone(&set_disks);
                let stale = tokio::spawn(async move {
                    let mut stale_reader = PutObjReader::from_vec(b"stale writer body".to_vec());
                    stale_set
                        .put_object(
                            bucket,
                            object,
                            &mut stale_reader,
                            &ObjectOptions {
                                no_lock: true,
                                ..Default::default()
                            },
                        )
                        .await
                });
                barrier.wait_until_paused().await;

                let mut winner_reader = PutObjReader::from_vec(b"winning writer body".to_vec());
                set_disks
                    .put_object(
                        bucket,
                        object,
                        &mut winner_reader,
                        &ObjectOptions {
                            no_lock: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("concurrent fenced PUT should advance the epoch");
                let winning_epoch = object_transaction_epochs(&disk_stores, bucket, object)
                    .await
                    .into_iter()
                    .next()
                    .flatten()
                    .expect("winning fenced PUT should persist an epoch");
                assert_ne!(winning_epoch, initial_epoch);

                barrier.release();
                let err = stale
                    .await
                    .expect("stale PUT task should not panic")
                    .expect_err("stale epoch PUT must be rejected");
                assert_eq!(err, StorageError::PreconditionFailed);

                let final_epochs = object_transaction_epochs(&disk_stores, bucket, object).await;
                assert!(final_epochs.into_iter().all(|epoch| epoch == Some(winning_epoch)));
                let mut reader = set_disks
                    .get_object_reader(
                        bucket,
                        object,
                        None,
                        HeaderMap::new(),
                        &ObjectOptions {
                            no_lock: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("winning object should remain readable");
                let mut restored = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut restored)
                    .await
                    .expect("winning body should stream");
                assert_eq!(restored, b"winning writer body");
            },
        )
        .await;
    }
}

#[cfg(test)]
mod put_object_tmp_cleanup_tests {
    //! Regression coverage for backlog#924 (HP-3): the speculative tmp-dir
    //! cleanup at the end of a successful PUT runs on a spawned task (off the
    //! response path), while a failed PUT must still clean its tmp shards
    //! inline before returning.

    use super::hermetic_set_disks_support::hermetic_set_disks_isolated as hermetic_set_disks;
    use super::*;
    use crate::set_disk::core::io_primitives::{
        ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, rename_fanout_barrier, rename_fault_injection,
    };
    use std::{collections::HashSet, time::Duration};
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    /// Large enough that the erasure shards are written as real tmp files
    /// (never inlined into xl.meta), so the cleanup tests exercise actual cleanup.
    const TEST_OBJECT_SIZE: usize = 1 << 20;

    /// Entries under `.rustfs.sys/tmp` on every disk, excluding the `.trash`
    /// staging directory (trash reclamation is a background concern).
    async fn non_trash_tmp_entries(temp_dirs: &[TempDir]) -> Vec<String> {
        let mut leftovers = Vec::new();
        for temp_dir in temp_dirs {
            let tmp_path = temp_dir.path().join(RUSTFS_META_TMP_BUCKET);
            let mut read_dir = match tokio::fs::read_dir(&tmp_path).await {
                Ok(read_dir) => read_dir,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => panic!("tmp dir {tmp_path:?} should be listable: {err}"),
            };
            while let Some(entry) = read_dir.next_entry().await.expect("tmp dir entry should be readable") {
                let name = entry.file_name().to_string_lossy().to_string();
                if name != ".trash" {
                    leftovers.push(format!("{}/{name}", tmp_path.display()));
                }
            }
        }
        leftovers
    }

    async fn wait_for_tmp_workspace_to_drain(temp_dirs: &[TempDir], failure_context: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let leftovers = non_trash_tmp_entries(temp_dirs).await;
            if leftovers.is_empty() {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "{failure_context}, leftovers: {leftovers:?}");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn recovered_disk_tmp_cleanup_marks_the_snapshot_it_mutates() {
        use rustfs_object_capacity::capacity_scope::drain_global_dirty_scopes;

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let recovered = disk_stores[0].clone();
        let tmp_path = "recovered-disk-cleanup/part.1";
        recovered
            .write_all(RUSTFS_META_TMP_BUCKET, tmp_path, Bytes::from_static(b"stale tmp shard"))
            .await
            .expect("recovered disk should contain staged tmp data");

        set_disks.disks.write().await[0] = None;
        let stale_snapshot = set_disks.get_disks_internal().await;
        assert!(stale_snapshot[0].is_none(), "the pre-cleanup snapshot must exclude the disk");
        set_disks.disks.write().await[0] = Some(recovered.clone());
        let _ = drain_global_dirty_scopes();

        set_disks
            .delete_all(RUSTFS_META_TMP_BUCKET, "recovered-disk-cleanup")
            .await
            .expect("tmp cleanup should use the recovered disk");

        assert!(matches!(
            recovered.read_all(RUSTFS_META_TMP_BUCKET, tmp_path).await,
            Err(DiskError::FileNotFound)
        ));
        let expected = capacity_scope_from_disks(&[Some(recovered)])
            .disks
            .into_iter()
            .collect::<HashSet<_>>();
        let marked = drain_global_dirty_scopes().into_iter().collect::<HashSet<_>>();
        assert!(
            expected.is_subset(&marked),
            "tmp cleanup must mark the recovered disk it actually mutated"
        );
    }

    #[tokio::test]
    async fn put_object_success_eventually_cleans_tmp_workspace() {
        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;

        let bucket = "tmp-clean-ok-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(vec![7u8; TEST_OBJECT_SIZE]);
        set_disks
            .put_object(bucket, "hot-path-object", &mut reader, &ObjectOptions::default())
            .await
            .expect("put_object should succeed");

        wait_for_tmp_workspace_to_drain(&temp_dirs, "tmp workspace should drain after a successful PUT").await;

        drop(temp_dirs);
    }

    #[tokio::test]
    async fn cancelled_put_before_rename_cleans_tmp_workspace() {
        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;

        let bucket = "tmp-clean-cancelled-bucket";
        let object = "cancelled-object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterQuotaReservation);
        let cancelled_set = set_disks.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![8u8; TEST_OBJECT_SIZE]);
            cancelled_set
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;
        put.abort();
        let join_error = put.await.expect_err("the paused PUT task must be cancelled");
        assert!(join_error.is_cancelled(), "the paused PUT task must not panic");

        // Keep the barrier armed so a detached child cannot proceed and hide
        // missing cancellation cleanup.
        wait_for_tmp_workspace_to_drain(&temp_dirs, "cancelling before rename should drain the tmp workspace").await;

        drop(barrier);
        drop(temp_dirs);
    }

    #[tokio::test]
    async fn cooperative_cancellation_while_waiting_for_namespace_lock_cleans_tmp_workspace() {
        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "tmp-clean-namespace-cancel-bucket";
        let object = "contended-object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let first_barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterNamespace);
        let first_set = set_disks.clone();
        let first = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
            first_set
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
        });
        first_barrier.wait_until_paused().await;
        let first_workspace = non_trash_tmp_entries(&temp_dirs).await.into_iter().collect::<HashSet<_>>();
        assert!(!first_workspace.is_empty(), "the lock holder should own a staged tmp workspace");

        let second_barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::BeforeNamespace);
        let cancellation = CancellationToken::new();
        let second_cancellation = cancellation.clone();
        let second_set = set_disks.clone();
        let second = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![b'2'; TEST_OBJECT_SIZE]);
            let opts = ObjectOptions {
                put_object_cancellation: Some(second_cancellation),
                ..Default::default()
            };
            second_set.put_object(bucket, object, &mut reader, &opts).await
        });
        second_barrier.release_and_wait_until_namespace_pending().await;
        let both_workspaces = non_trash_tmp_entries(&temp_dirs).await.into_iter().collect::<HashSet<_>>();
        assert!(
            both_workspaces.len() > first_workspace.len() && first_workspace.is_subset(&both_workspaces),
            "the contending PUT should stage its own tmp workspace before cancellation"
        );

        cancellation.cancel();
        let error = tokio::time::timeout(Duration::from_secs(10), second)
            .await
            .expect("cooperative cancellation should finish storage cleanup")
            .expect("the cancelled PUT task should join")
            .expect_err("the cancelled PUT must not commit");
        assert!(matches!(error, StorageError::OperationCanceled));
        let remaining = non_trash_tmp_entries(&temp_dirs).await.into_iter().collect::<HashSet<_>>();
        assert_eq!(
            remaining, first_workspace,
            "the cancelled contender must clean only its own tmp workspace"
        );

        drop(second_barrier);
        first_barrier.release();
        first
            .await
            .expect("the lock-holding PUT task should join")
            .expect("the lock-holding PUT should commit");
        wait_for_tmp_workspace_to_drain(&temp_dirs, "the committed lock holder should eventually drain its tmp workspace").await;

        drop(first_barrier);
        drop(temp_dirs);
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn put_object_failure_cleans_tmp_workspace_inline() {
        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            for write_completion in [WriteCompletion::Quorum, WriteCompletion::TailDrained] {
                let (temp_dirs, _disk_stores, set_disks) = hermetic_set_disks(4).await;
                let bucket = "tmp-clean-missing-bucket";
                let object = "orphan-object";
                let barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::BeforeNamespace);
                let writer = Arc::clone(&set_disks);
                let put = tokio::spawn(async move {
                    let mut reader = PutObjReader::from_vec(vec![9u8; TEST_OBJECT_SIZE]);
                    writer
                        .put_object(
                            bucket,
                            object,
                            &mut reader,
                            &ObjectOptions {
                                write_completion,
                                ..Default::default()
                            },
                        )
                        .await
                });
                tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_paused())
                    .await
                    .expect("missing-bucket PUT must stage before rename");
                let staged = non_trash_tmp_entries(&temp_dirs).await;
                assert_eq!(staged.len(), 4, "every disk must have a staged workspace before rejection");
                for workspace in staged {
                    let mut entries = tokio::fs::read_dir(&workspace)
                        .await
                        .expect("staged workspace should be readable");
                    let mut shards = 0;
                    while let Some(entry) = entries.next_entry().await.expect("staged data directory should be readable") {
                        if entry.file_type().await.expect("staged entry type").is_dir() {
                            let part = tokio::fs::metadata(entry.path().join("part.1"))
                                .await
                                .expect("staging must contain an actual erasure shard");
                            assert!(part.len() > 0, "the shard must be written before the missing-bucket failure");
                            shards += 1;
                        }
                    }
                    assert_eq!(shards, 1);
                }
                assert!(temp_dirs.iter().all(|dir| !dir.path().join(bucket).exists()));
                barrier.release();
                let err = tokio::time::timeout(Duration::from_secs(30), put)
                    .await
                    .expect("missing-bucket PUT must finish")
                    .expect("PUT task should join")
                    .expect_err("put_object into a missing bucket volume must fail");
                assert!(matches!(err, StorageError::VolumeNotFound), "original disk error expected: {err}");

                // No polling: known pre-publication rejection must clean staging
                // inline, before PUT returns (backlog#864 / backlog#898).
                let leftovers = non_trash_tmp_entries(&temp_dirs).await;
                assert!(
                    leftovers.is_empty(),
                    "failed PUT must not leave tmp shards behind, leftovers: {leftovers:?}, err: {err}"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    async fn committed_put_releases_namespace_lock_before_old_data_cleanup() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-commit-lock-window";
        let object = "commit-lock-window-object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut initial_reader = PutObjReader::from_vec(vec![b'0'; TEST_OBJECT_SIZE]);
        set_disks
            .put_object(bucket, object, &mut initial_reader, &ObjectOptions::default())
            .await
            .expect("initial object should be committed");
        let mut initial = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("initial object should populate the metadata cache");
        let mut initial_body = Vec::new();
        initial
            .stream
            .read_to_end(&mut initial_body)
            .await
            .expect("initial body should drain");
        assert_eq!(initial_body, vec![b'0'; TEST_OBJECT_SIZE]);

        let cleanup_barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_CLEANUP);
        let first_store = Arc::clone(&set_disks);
        let first = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
            first_store
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), cleanup_barrier.wait_until_paused())
            .await
            .expect("first overwrite should reach old-data cleanup");

        let mut committed = tokio::time::timeout(
            Duration::from_secs(30),
            set_disks.get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default()),
        )
        .await
        .expect("GET should not wait for old-data cleanup")
        .expect("committed overwrite should be readable during old-data cleanup");
        let mut committed_body = Vec::new();
        committed
            .stream
            .read_to_end(&mut committed_body)
            .await
            .expect("committed overwrite body should drain");
        assert_eq!(committed_body, vec![b'1'; TEST_OBJECT_SIZE]);

        let second_commit_barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterNamespace);
        let second_store = Arc::clone(&set_disks);
        let second = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![b'2'; TEST_OBJECT_SIZE]);
            second_store
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), second_commit_barrier.wait_until_paused())
            .await
            .expect("second overwrite should acquire the namespace lock during cleanup");

        cleanup_barrier.release();
        first
            .await
            .expect("first overwrite task should join")
            .expect("first overwrite should remain successful after cleanup");
        drop(cleanup_barrier);
        second_commit_barrier.release();
        second
            .await
            .expect("second overwrite task should join")
            .expect("second overwrite should commit after acquiring the released namespace lock");

        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("the latest overwrite should be readable");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("latest body should drain");
        assert_eq!(body, vec![b'2'; TEST_OBJECT_SIZE]);
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn cancelled_post_commit_cleanup_does_not_retain_namespace_lock() {
        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("false"))], async {
            assert_cancelled_post_commit_cleanup_does_not_retain_namespace_lock().await;
        })
        .await;
    }

    async fn assert_cancelled_post_commit_cleanup_does_not_retain_namespace_lock() {
        use rustfs_object_capacity::capacity_scope::drain_global_dirty_scopes;

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-commit-lock-cancelled-cleanup";
        let object = "commit-lock-cancelled-cleanup-object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let candidate_disks = disk_stores.iter().cloned().map(Some).collect::<Vec<_>>();
        let expected_scope = capacity_scope_from_disks(&candidate_disks)
            .disks
            .into_iter()
            .collect::<HashSet<_>>();

        let mut initial_reader = PutObjReader::from_vec(vec![b'0'; TEST_OBJECT_SIZE]);
        set_disks
            .put_object(bucket, object, &mut initial_reader, &ObjectOptions::default())
            .await
            .expect("initial object should be committed");
        let _ = drain_global_dirty_scopes();

        let cleanup_tasks = rename_fanout_barrier::observe_tasks(object);
        let cleanup_barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_CLEANUP);
        let first_store = Arc::clone(&set_disks);
        let first = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
            first_store
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), cleanup_barrier.wait_until_paused())
            .await
            .expect("first overwrite should reach old-data cleanup");

        let second_commit_barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterNamespace);
        let second_store = Arc::clone(&set_disks);
        let second = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![b'2'; TEST_OBJECT_SIZE]);
            second_store
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), second_commit_barrier.wait_until_paused())
            .await
            .expect("second overwrite should acquire the namespace lock before cancellation");

        first.abort();
        assert!(
            first
                .await
                .expect_err("the first request should be cancelled during cleanup")
                .is_cancelled()
        );
        let committed_scope = drain_global_dirty_scopes().into_iter().collect::<HashSet<_>>();
        assert!(
            expected_scope.is_subset(&committed_scope),
            "a committed overwrite must mark every candidate disk before caller cancellation can interrupt post-commit awaits"
        );
        assert!(
            cleanup_tasks.running() >= 1,
            "cancelled cleanup must remain observable until its disk task drains"
        );
        cleanup_barrier.release();
        tokio::time::timeout(Duration::from_secs(30), async {
            while cleanup_tasks.running() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled cleanup disk tasks should drain");
        drop(cleanup_barrier);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let after_cleanup = drain_global_dirty_scopes().into_iter().collect::<HashSet<_>>();
                if expected_scope.is_subset(&after_cleanup) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the completed old-data cleanup must re-mark every candidate disk after a refresh drain");

        second_commit_barrier.release();
        second
            .await
            .expect("second overwrite task should join")
            .expect("second overwrite should survive the earlier request cancellation");

        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("the latest overwrite should be readable");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("latest body should drain");
        assert_eq!(body, vec![b'2'; TEST_OBJECT_SIZE]);
        let _ = drain_global_dirty_scopes();
    }

    #[tokio::test]
    async fn cancelled_rename_keeps_namespace_lock_until_publication() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-commit-lock-cancelled-rename";
        let object = "commit-lock-cancelled-rename-object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let rename_tasks = rename_fanout_barrier::observe_tasks(object);
        let rename_barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
        let first_store = Arc::clone(&set_disks);
        let first = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
            first_store
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), rename_barrier.wait_until_paused())
            .await
            .expect("first PUT should pause during the authoritative rename");

        let second_namespace_barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::BeforeNamespace);
        let second_store = Arc::clone(&set_disks);
        let second = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![b'2'; TEST_OBJECT_SIZE]);
            second_store
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
        });
        second_namespace_barrier.release_and_wait_until_namespace_pending().await;

        first.abort();
        assert!(
            first
                .await
                .expect_err("the first request should be cancelled while rename is parked")
                .is_cancelled()
        );
        tokio::task::yield_now().await;
        assert!(
            !second.is_finished(),
            "the second writer must remain blocked by the cancelled commit owner"
        );

        rename_barrier.release();
        drop(rename_barrier);
        tokio::time::timeout(Duration::from_secs(30), async {
            while rename_tasks.running() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the cancelled owner's rename fanout should drain");
        second
            .await
            .expect("second overwrite task should join")
            .expect("second overwrite should commit after the cancelled owner reaches publication");

        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("the latest overwrite should be readable");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("latest body should drain");
        assert_eq!(body, vec![b'2'; TEST_OBJECT_SIZE]);
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn early_ack_tail_drain_retains_namespace_lock_until_background_rename_finishes() {
        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "put-early-ack-tail-lock";
            let object = "early-ack-tail-lock-object";
            for disk in &disk_stores {
                disk.make_volume(bucket).await.expect("bucket volume should be created");
            }

            let rename_tasks = rename_fanout_barrier::observe_tasks(object);
            let rename_barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
            let first_store = Arc::clone(&set_disks);
            let first = tokio::spawn(async move {
                let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
                first_store
                    .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                    .await
            });
            tokio::time::timeout(Duration::from_secs(30), rename_barrier.wait_until_paused())
                .await
                .expect("first PUT should pause one tail disk during rename");
            first
                .await
                .expect("first early-ACK PUT task should join before tail release")
                .expect("first early-ACK PUT should return after write quorum");
            assert!(
                rename_tasks.running() >= 1,
                "tail rename must still be draining after the first PUT returns"
            );

            let second_namespace_barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::BeforeNamespace);
            let second_store = Arc::clone(&set_disks);
            let second = tokio::spawn(async move {
                let mut reader = PutObjReader::from_vec(vec![b'2'; TEST_OBJECT_SIZE]);
                second_store
                    .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                    .await
            });
            second_namespace_barrier.release_and_wait_until_namespace_pending().await;
            tokio::task::yield_now().await;
            assert!(
                !second.is_finished(),
                "second writer must remain blocked until the first early-ACK tail drain releases the namespace lock"
            );

            rename_barrier.release();
            drop(rename_barrier);
            tokio::time::timeout(Duration::from_secs(30), async {
                while rename_tasks.running() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("early-ACK tail rename should drain after release");
            second
                .await
                .expect("second overwrite task should join")
                .expect("second overwrite should commit after the tail drain releases the namespace lock");

            let mut reader = set_disks
                .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("the latest overwrite should be readable");
            let mut body = Vec::new();
            reader.stream.read_to_end(&mut body).await.expect("latest body should drain");
            assert_eq!(body, vec![b'2'; TEST_OBJECT_SIZE]);
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn early_ack_successful_tail_reclaims_its_old_non_inline_data_dir() {
        use rustfs_object_capacity::capacity_scope::drain_global_dirty_scopes;

        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "put-early-ack-tail-cleanup";
            let object = "early-ack-tail-cleanup-object";
            for disk in &disk_stores {
                disk.make_volume(bucket).await.expect("bucket volume should be created");
            }
            let expected_scope = capacity_scope_from_disks(&disk_stores.iter().cloned().map(Some).collect::<Vec<_>>())
                .disks
                .into_iter()
                .collect::<HashSet<_>>();

            let mut initial = PutObjReader::from_vec(vec![b'0'; TEST_OBJECT_SIZE]);
            set_disks
                .put_object(bucket, object, &mut initial, &ObjectOptions::default())
                .await
                .expect("initial non-inline object should commit");
            drop(
                set_disks
                    .acquire_write_lock_diag("tail_cleanup_fixture_ready", bucket, object)
                    .await
                    .expect("initial PUT tail should drain before inspecting one disk"),
            );
            let old_data_dir = disk_stores[0]
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("initial object metadata should be readable")
                .data_dir
                .expect("one-megabyte test object should have a data directory");
            let old_part = format!("{object}/{old_data_dir}/part.1");

            let mut heal_requests = set_disks.capture_test_rename_tail_heals();
            let tail_tasks = rename_fanout_barrier::observe_tasks(object);
            let rename_barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
            let overwrite_set = Arc::clone(&set_disks);
            let overwrite = tokio::spawn(async move {
                let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
                overwrite_set
                    .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                    .await
            });

            tokio::time::timeout(Duration::from_secs(30), rename_barrier.wait_until_paused())
                .await
                .expect("one overwrite tail should pause during rename");
            let cleanup_barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_CLEANUP);
            overwrite
                .await
                .expect("early-ACK overwrite task should join")
                .expect("overwrite should return after write quorum");
            let retained_before_tail = futures::future::join_all(disk_stores.iter().map(|disk| disk.read_all(bucket, &old_part)))
                .await
                .into_iter()
                .filter(|result| result.is_ok())
                .count();
            assert_eq!(
                retained_before_tail, 1,
                "only the paused tail disk should still retain the referenced old body"
            );
            let _ = drain_global_dirty_scopes();

            rename_barrier.release();
            tokio::time::timeout(Duration::from_secs(30), cleanup_barrier.wait_until_paused())
                .await
                .expect("the successful tail should pause before reclaiming its old body");
            let after_tail = drain_global_dirty_scopes().into_iter().collect::<HashSet<_>>();
            assert!(
                expected_scope.is_subset(&after_tail),
                "the completed rename tail must re-mark capacity after the first scope was drained"
            );
            cleanup_barrier.release();
            let cleanup_wait = tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let retained = futures::future::join_all(disk_stores.iter().map(|disk| disk.read_all(bucket, &old_part)))
                        .await
                        .into_iter()
                        .filter(|result| result.is_ok())
                        .count();
                    if retained == 0 && tail_tasks.running() == 0 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await;
            if cleanup_wait.is_err() {
                let retained = futures::future::join_all(disk_stores.iter().map(|disk| disk.read_all(bucket, &old_part)))
                    .await
                    .into_iter()
                    .filter(|result| result.is_ok())
                    .count();
                panic!(
                    "the successful tail must reclaim its old body and drain: retained={retained}, running={}",
                    tail_tasks.running()
                );
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let after_cleanup = drain_global_dirty_scopes().into_iter().collect::<HashSet<_>>();
                    if expected_scope.is_subset(&after_cleanup) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the completed tail cleanup must re-mark capacity after its preceding scope was drained");

            for (idx, disk) in disk_stores.iter().enumerate() {
                assert!(
                    matches!(disk.read_all(bucket, &old_part).await, Err(DiskError::FileNotFound)),
                    "disk {idx} must reclaim the dereferenced old body"
                );
            }
            let mut reader = set_disks
                .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("the overwrite should remain readable after tail cleanup");
            let mut body = Vec::new();
            reader
                .stream
                .read_to_end(&mut body)
                .await
                .expect("overwritten body should drain");
            assert_eq!(body, vec![b'1'; TEST_OBJECT_SIZE]);
            assert!(
                matches!(heal_requests.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
                "successful tail cleanup must not submit heal"
            );
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn early_ack_put_holds_quota_fences_and_re_marks_capacity_after_tail_drain() {
        use rustfs_object_capacity::capacity_scope::drain_global_dirty_scopes;

        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "put-early-ack-capacity-scope";
            let object = "early-ack-capacity-scope-object";
            for disk in &disk_stores {
                disk.make_volume(bucket).await.expect("bucket volume should be created");
            }
            let candidate_disks = disk_stores.iter().cloned().map(Some).collect::<Vec<_>>();
            let expected = capacity_scope_from_disks(&candidate_disks)
                .disks
                .into_iter()
                .collect::<HashSet<_>>();
            let _ = drain_global_dirty_scopes();
            let mut heal_requests = set_disks.capture_test_rename_tail_heals();

            let rename_tasks = rename_fanout_barrier::observe_tasks(object);
            let rename_barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
            let put_store = Arc::clone(&set_disks);
            let put = tokio::spawn(async move {
                let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
                let mut opts = ObjectOptions::default();
                assert!(opts.set_quota_admission(0, u64::MAX));
                put_store.put_object(bucket, object, &mut reader, &opts).await
            });
            tokio::time::timeout(Duration::from_secs(30), rename_barrier.wait_until_paused())
                .await
                .expect("PUT should pause one tail disk during rename");
            put.await
                .expect("early-ACK PUT task should join before tail release")
                .expect("early-ACK PUT should return after write quorum");
            assert!(rename_tasks.running() >= 1, "the paused tail disk must remain in flight after quorum ACK");

            let initial = drain_global_dirty_scopes().into_iter().collect::<HashSet<_>>();
            assert!(expected.is_subset(&initial), "the quorum ACK must mark every candidate disk dirty");

            rename_barrier.release();
            let tail_done = tokio::time::timeout(
                Duration::from_secs(30),
                set_disks.acquire_write_lock_diag("capacity_scope_tail_probe", bucket, object),
            )
            .await
            .expect("the object lock should become available after the tail drain")
            .expect("the tail completion probe should acquire the object lock");
            drop(tail_done);

            let after_tail = drain_global_dirty_scopes().into_iter().collect::<HashSet<_>>();
            assert!(
                expected.is_subset(&after_tail),
                "a tail completing after refresh drained the first mark must re-mark every candidate disk"
            );
            for (disk_index, disk) in disk_stores.iter().enumerate() {
                disk.read_version("", bucket, object, "", &ReadOptions::default())
                    .await
                    .unwrap_or_else(|err| panic!("disk {disk_index} must claim its quota fence and finish the rename: {err}"));
            }
            assert!(
                matches!(heal_requests.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
                "a successful rename tail must not submit heal"
            );
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn detached_early_ack_handoff_survives_caller_cancellation() {
        use rustfs_object_capacity::capacity_scope::drain_global_dirty_scopes;

        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "put-early-ack-cancelled-handoff";
            let object = "early-ack-cancelled-handoff-object";
            for disk in &disk_stores {
                disk.make_volume(bucket).await.expect("bucket volume should be created");
            }
            let candidate_disks = disk_stores.iter().cloned().map(Some).collect::<Vec<_>>();
            let expected = capacity_scope_from_disks(&candidate_disks)
                .disks
                .into_iter()
                .collect::<HashSet<_>>();
            let _ = drain_global_dirty_scopes();
            let mut heal_requests = set_disks.capture_test_rename_tail_heals();
            let expected_version_id = Uuid::nil().to_string();

            let tail_tasks = rename_fanout_barrier::observe_tasks(object);
            let tail_barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
            let _fault = rename_fault_injection::fail_rename_on(object, &[0]);
            let quorum_barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterRenameQuorum);
            let handoff_barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterRenameHandoff);
            let put_store = Arc::clone(&set_disks);
            let put = tokio::spawn(async move {
                let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
                put_store
                    .put_object(
                        bucket,
                        object,
                        &mut reader,
                        &ObjectOptions {
                            version_suspended: true,
                            ..Default::default()
                        },
                    )
                    .await
            });

            tokio::time::timeout(Duration::from_secs(30), tail_barrier.wait_until_paused())
                .await
                .expect("one rename tail should pause while the other disks publish quorum");
            quorum_barrier.wait_until_paused().await;
            put.abort();
            assert!(
                put.await.expect_err("the request task should be cancelled").is_cancelled(),
                "the caller should be cancelled after quorum publication"
            );

            quorum_barrier.release();
            handoff_barrier.wait_until_paused().await;
            assert!(tail_tasks.running() >= 1, "the detached owner must retain the paused tail");
            let initial = drain_global_dirty_scopes().into_iter().collect::<HashSet<_>>();
            assert!(
                expected.is_subset(&initial),
                "the detached continuation must install the full capacity scope"
            );
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(50),
                    set_disks.acquire_write_lock_diag("cancelled_handoff_probe", bucket, object),
                )
                .await
                .is_err(),
                "the watcher must own the namespace guard before the detached continuation awaits"
            );

            handoff_barrier.release();
            tail_barrier.release();
            let tail_done = tokio::time::timeout(
                Duration::from_secs(30),
                set_disks.acquire_write_lock_diag("cancelled_handoff_tail_probe", bucket, object),
            )
            .await
            .expect("the namespace guard should release after the detached tail drains")
            .expect("the post-tail probe should acquire the namespace guard");
            drop(tail_done);

            let after_tail = drain_global_dirty_scopes().into_iter().collect::<HashSet<_>>();
            assert!(
                expected.is_subset(&after_tail),
                "the detached tail must re-mark capacity after the first scope was drained"
            );
            let request = tokio::time::timeout(Duration::from_secs(30), heal_requests.recv())
                .await
                .expect("a failed tail should submit heal")
                .expect("the per-set heal capture should stay connected");
            assert_eq!(request.bucket, bucket);
            assert_eq!(request.object_prefix.as_deref(), Some(object));
            assert_eq!(request.object_version_id.as_deref(), Some(expected_version_id.as_str()));
            assert_eq!(request.pool_index, Some(set_disks.pool_index));
            assert_eq!(request.set_index, Some(set_disks.set_index));
        })
        .await;
    }

    async fn make_completion_test_bucket(disks: &[DiskStore], bucket: &str) {
        for disk in disks {
            disk.make_volume(bucket)
                .await
                .expect("completion test bucket should be created");
        }
    }

    /// Observe the actual metadata quorum while the remaining rename is parked.
    /// A completed task count alone can race tasks that have not started yet.
    async fn wait_for_paused_tail_metadata_quorum(disks: &[DiskStore], bucket: &str, object: &str) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let mut committed = 0;
                for disk in disks {
                    match disk.read_version("", bucket, object, "", &ReadOptions::default()).await {
                        Ok(_) => committed += 1,
                        Err(DiskError::FileNotFound | DiskError::FileVersionNotFound) => {}
                        Err(err) => panic!("unexpected metadata error while observing {bucket}/{object}: {err}"),
                    }
                }
                if committed == 3 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("three disks must publish metadata while the fourth rename remains paused");
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn tail_drained_put_waits_for_tail_and_allows_immediate_cas() {
        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            for size in [4096, 1024 * 1024] {
                let (_dirs, disks, set) = hermetic_set_disks(4).await;
                let bucket = "put-full-tail-cas";
                let object = "full-tail-cas-object";
                make_completion_test_bucket(&disks, bucket).await;
                let tasks = rename_fanout_barrier::observe_tasks(object);
                let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
                let writer = Arc::clone(&set);
                let put = tokio::spawn(async move {
                    let mut reader = PutObjReader::from_vec(vec![b'1'; size]);
                    writer
                        .put_object(
                            bucket,
                            object,
                            &mut reader,
                            &ObjectOptions {
                                write_completion: WriteCompletion::TailDrained,
                                ..Default::default()
                            },
                        )
                        .await
                });
                tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_paused())
                    .await
                    .expect("full-tail PUT must reach the rename barrier");
                wait_for_paused_tail_metadata_quorum(&disks, bucket, object).await;
                assert!(!put.is_finished(), "full-tail PUT must remain pending after metadata quorum");
                let mut lock_probe = Box::pin(set.acquire_write_lock_diag("full_tail_probe", bucket, object));
                assert!(
                    futures::poll!(lock_probe.as_mut()).is_pending(),
                    "the owned namespace guard must remain held"
                );
                barrier.release();
                let written = tokio::time::timeout(Duration::from_secs(30), put)
                    .await
                    .expect("full-tail PUT should finish after release")
                    .expect("full-tail PUT task should join")
                    .expect("full-tail PUT must commit");
                assert_eq!(tasks.running(), 0, "full-tail response must follow every rename task");
                drop(
                    tokio::time::timeout(Duration::from_secs(5), lock_probe)
                        .await
                        .expect("same-key lock should be available on return")
                        .expect("same-key lock probe should succeed"),
                );
                for disk in &disks {
                    disk.read_version("", bucket, object, "", &ReadOptions::default())
                        .await
                        .expect("successful full-tail PUT must publish on every healthy disk");
                }
                drop(barrier);
                let mut replacement = PutObjReader::from_vec(b"cas successor".to_vec());
                set.put_object(
                    bucket,
                    object,
                    &mut replacement,
                    &ObjectOptions {
                        write_completion: WriteCompletion::TailDrained,
                        http_preconditions: Some(HTTPPreconditions {
                            if_match: written.etag,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await
                .expect("immediate same-key CAS must acquire the namespace guard");
                let mut read = set
                    .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                    .await
                    .expect("CAS successor must be immediately readable");
                let mut body = Vec::new();
                read.stream.read_to_end(&mut body).await.expect("successor body must drain");
                assert_eq!(body, b"cas successor");
            }
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn tail_drained_put_preserves_quorum_success_and_heals_failed_tail() {
        let (_dirs, disks, set) = hermetic_set_disks(4).await;
        let bucket = "put-full-tail-heal";
        let object = "full-tail-heal-object";
        make_completion_test_bucket(&disks, bucket).await;
        let mut heals = set.capture_test_rename_tail_heals();
        let tasks = rename_fanout_barrier::observe_tasks(object);
        let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
        let _fault = rename_fault_injection::fail_rename_on(object, &[0]);
        let writer = Arc::clone(&set);
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
            writer
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        write_completion: WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_paused())
            .await
            .expect("failed tail must first reach the rename barrier");
        wait_for_paused_tail_metadata_quorum(&disks, bucket, object).await;
        assert!(!put.is_finished(), "committed quorum must still wait for the failing tail");
        barrier.release();
        tokio::time::timeout(Duration::from_secs(30), put)
            .await
            .expect("failed tail should drain")
            .expect("PUT task should join")
            .expect("a minority tail error must not negate committed quorum");
        assert_eq!(tasks.running(), 0);
        let heal = tokio::time::timeout(Duration::from_secs(30), heals.recv())
            .await
            .expect("failed tail must schedule heal")
            .expect("heal capture must remain connected");
        assert_eq!(heal.bucket, bucket);
        assert_eq!(heal.object_prefix.as_deref(), Some(object));
        let info = set
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("committed object must remain readable despite the failed tail");
        assert_eq!(info.size, TEST_OBJECT_SIZE as i64);
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn tail_drained_put_rejects_quorum_minus_one() {
        let (_dirs, disks, set) = hermetic_set_disks(4).await;
        let bucket = "put-full-tail-no-quorum";
        let object = "full-tail-no-quorum-object";
        make_completion_test_bucket(&disks, bucket).await;
        let _fault = rename_fault_injection::fail_rename_on(object, &[0, 1]);
        let tasks = rename_fanout_barrier::observe_tasks(object);
        let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
        let err = set
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    write_completion: WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect_err("draining two successful disks cannot satisfy write quorum three");
        assert!(
            matches!(err, Error::ErasureWriteQuorum | Error::InsufficientWriteQuorum(_, _)),
            "original quorum error expected: {err}"
        );
        assert_eq!(tasks.running(), 0, "failed fan-out and rollback must complete before return");
        assert!(
            set.get_object_info(bucket, object, &ObjectOptions::default()).await.is_err(),
            "failed fresh write must not become visible"
        );
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn put_incomplete_rollback_preserves_staging_and_old_version_backup() {
        use crate::set_disk::core::io_primitives::rollback_fault_injection;

        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            for write_completion in [WriteCompletion::Quorum, WriteCompletion::TailDrained] {
                for fault in [
                    rollback_fault_injection::Fault::Io,
                    rollback_fault_injection::Fault::VolumeNotFoundAfterRename,
                ] {
                    let (dirs, disks, set) = hermetic_set_disks(4).await;
                    let bucket = "put-incomplete-undo";
                    let object = "incomplete-undo-object";
                    make_completion_test_bucket(&disks, bucket).await;
                    let mut old_reader = PutObjReader::from_vec(vec![b'0'; TEST_OBJECT_SIZE]);
                    set.put_object(
                        bucket,
                        object,
                        &mut old_reader,
                        &ObjectOptions {
                            write_completion: WriteCompletion::TailDrained,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("old generation should be completely committed");
                    wait_for_tmp_workspace_to_drain(&dirs, "old PUT must leave no unrelated staging").await;
                    let old = disks[0]
                        .read_version("", bucket, object, "", &ReadOptions::default())
                        .await
                        .expect("old metadata must be readable");
                    let old_data_dir = old.data_dir.expect("non-inline old version needs a data directory");
                    let tasks = rename_fanout_barrier::observe_tasks(object);
                    let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
                    let _rename_fault = rename_fault_injection::fail_rename_on(object, &[2, 3]);
                    let _undo_fault = rollback_fault_injection::arm(object, 0, fault);
                    let writer = Arc::clone(&set);
                    let put = tokio::spawn(async move {
                        let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
                        writer
                            .put_object(
                                bucket,
                                object,
                                &mut reader,
                                &ObjectOptions {
                                    write_completion,
                                    ..Default::default()
                                },
                            )
                            .await
                    });
                    tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_paused())
                        .await
                        .expect("overwrite must enter the actual rename fan-out before failure injection");
                    barrier.release();
                    let err = tokio::time::timeout(Duration::from_secs(30), put)
                        .await
                        .expect("incomplete undo must return without hanging")
                        .expect("PUT task should join")
                        .expect_err("two renamed disks cannot satisfy write quorum three");
                    assert!(
                        matches!(err, Error::ErasureWriteQuorum | Error::InsufficientWriteQuorum(_, _)),
                        "original quorum error expected: {err}"
                    );
                    assert_eq!(tasks.running(), 0, "every rename and undo task must be reaped before return");
                    let leftovers = non_trash_tmp_entries(&dirs).await;
                    assert!(!leftovers.is_empty(), "incomplete undo must retain the new staging source for recovery");
                    let backups = dirs
                        .iter()
                        .filter(|dir| {
                            dir.path()
                                .join(bucket)
                                .join(object)
                                .join(old_data_dir.to_string())
                                .join(crate::disk::STORAGE_FORMAT_FILE_BACKUP)
                                .exists()
                        })
                        .count();
                    assert_eq!(backups, 1, "exactly the failed undo disk must retain its old-version backup");
                    // The remaining three disks still serve the old generation;
                    // the failed minority must never become an acknowledged write.
                    let mut read = set
                        .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                        .await
                        .expect("old generation must remain readable after incomplete rollback");
                    let mut body = Vec::new();
                    read.stream
                        .read_to_end(&mut body)
                        .await
                        .expect("old generation should stream");
                    assert_eq!(body, vec![b'0'; TEST_OBJECT_SIZE]);
                }
            }
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn tail_drained_put_owned_commit_survives_waiter_cancellation() {
        let (dirs, disks, set) = hermetic_set_disks(4).await;
        let bucket = RUSTFS_META_BUCKET;
        let object = "full-tail-cancelled-receipt";
        // Internal config writes do not own a bucket lifecycle guard. The object
        // guard alone must keep the full-tail coordinator alive after cancellation.
        let tasks = rename_fanout_barrier::observe_tasks(object);
        let barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
        let writer = Arc::clone(&set);
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
            writer
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        write_completion: WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_paused())
            .await
            .expect("cancelled receipt must first reach the rename barrier");
        wait_for_paused_tail_metadata_quorum(&disks, bucket, object).await;
        put.abort();
        assert!(put.await.expect_err("ACK waiter should cancel").is_cancelled());
        let mut lock_probe = Box::pin(set.acquire_write_lock_diag("cancelled_full_tail_probe", bucket, object));
        assert!(
            futures::poll!(lock_probe.as_mut()).is_pending(),
            "owned coordinator must retain the namespace guard after waiter cancellation"
        );
        barrier.release();
        drop(
            tokio::time::timeout(Duration::from_secs(30), lock_probe)
                .await
                .expect("cancelled coordinator must eventually release its guard")
                .expect("post-commit lock probe should succeed"),
        );
        assert_eq!(tasks.running(), 0, "cancelled coordinator must reap every rename task");
        for disk in &disks {
            disk.read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("caller cancellation must not interrupt committed receipt materialization");
        }
        wait_for_tmp_workspace_to_drain(&dirs, "cancelled full-tail commit should release staging ownership").await;
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn no_lock_put_waits_for_rename_tail_under_outer_guard() {
        temp_env::async_with_vars([(ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "put-no-lock-serial-rename";
            let object = "put-no-lock-serial-rename-object";
            for disk in &disk_stores {
                disk.make_volume(bucket).await.expect("bucket volume should be created");
            }

            let outer_guard = set_disks
                .acquire_write_lock_diag("outer_no_lock_put", bucket, object)
                .await
                .expect("the outer caller should hold the namespace guard");
            let rename_tasks = rename_fanout_barrier::observe_tasks(object);
            let rename_barrier = rename_fanout_barrier::arm(object, 0, rename_fanout_barrier::PHASE_RENAME);
            let put_store = Arc::clone(&set_disks);
            let put = tokio::spawn(async move {
                let mut reader = PutObjReader::from_vec(vec![b'1'; TEST_OBJECT_SIZE]);
                put_store
                    .put_object(
                        bucket,
                        object,
                        &mut reader,
                        &ObjectOptions {
                            no_lock: true,
                            write_completion: WriteCompletion::TailDrained,
                            ..Default::default()
                        },
                    )
                    .await
            });

            tokio::time::timeout(Duration::from_secs(30), rename_barrier.wait_until_paused())
                .await
                .expect("no-lock PUT should reach the paused rename disk");
            tokio::time::timeout(Duration::from_secs(30), async {
                while rename_tasks.running() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the other rename disks should finish while one disk remains paused");
            assert!(
                !put.is_finished(),
                "a no-lock caller that cannot hand off its outer guard must await the full rename fanout"
            );

            rename_barrier.release();
            put.await
                .expect("no-lock PUT task should join")
                .expect("no-lock PUT should commit after the rename tail releases");
            let mut lock_probe = Box::pin(set_disks.acquire_write_lock_diag("borrowed_full_tail_probe", bucket, object));
            assert!(
                futures::poll!(lock_probe.as_mut()).is_pending(),
                "full-tail PUT must not release the caller's outer guard"
            );
            drop(outer_guard);
            drop(
                tokio::time::timeout(Duration::from_secs(5), lock_probe)
                    .await
                    .expect("outer owner releasing its guard should unblock the probe")
                    .expect("post-outer-guard probe should succeed"),
            );
        })
        .await;
    }

    #[tokio::test]
    async fn put_object_no_lock_aborts_after_outer_namespace_lock_loss() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-lost-outer-lock";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(vec![3u8; 4096]);
        let err = set_disks
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    namespace_lock_fence: Some(NamespaceLockFence::lost_for_test()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a lost outer namespace lock must abort before rename");

        assert!(matches!(err, Error::NamespaceLockQuorumUnavailable { .. }));
        assert!(
            set_disks
                .get_object_info(bucket, object, &ObjectOptions::default())
                .await
                .is_err(),
            "the object must not become visible after the outer lock is lost"
        );
    }

    #[tokio::test]
    async fn put_object_aborts_when_bucket_lifecycle_fence_is_lost_after_commit_lock() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-lost-bucket-lifecycle-lock";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterNamespace);
        let (fence, loss_handle) = NamespaceLockFence::loss_handle_for_test();
        let put_store = Arc::clone(&set_disks);
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(b"must-not-commit".to_vec());
            put_store
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        versioned: true,
                        bucket_lifecycle_lock_fence: Some(fence),
                        ..Default::default()
                    },
                )
                .await
        });

        barrier.wait_until_paused().await;
        loss_handle.store(true, std::sync::atomic::Ordering::Release);
        barrier.release();
        let err = put
            .await
            .expect("PUT task should join")
            .expect_err("bucket lifecycle lock loss at commit must abort the PUT");
        assert!(matches!(err, Error::NamespaceLockQuorumUnavailable { .. }));
        assert!(
            set_disks
                .get_object_info(bucket, object, &ObjectOptions::default())
                .await
                .is_err(),
            "object must remain absent after bucket lifecycle lock loss"
        );
    }

    #[tokio::test]
    async fn data_movement_precondition_is_rechecked_at_commit() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "data-movement-commit-precondition";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let migration_body = vec![b'm'; 64 * 1024];
        let split = migration_body.len() / 2;
        let (mut source, stream) = tokio::io::duplex(64);
        let hash_reader = HashReader::from_stream(
            stream,
            i64::try_from(migration_body.len()).expect("migration body length should fit i64"),
            i64::try_from(migration_body.len()).expect("migration body length should fit i64"),
            None,
            None,
            false,
        )
        .expect("migration hash reader should be created");
        let migration_store = Arc::clone(&set_disks);
        let migration = tokio::spawn(async move {
            let mut reader = PutObjReader::new(hash_reader);
            migration_store
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        data_movement: true,
                        http_preconditions: Some(HTTPPreconditions {
                            if_none_match: Some("*".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await
        });

        source
            .write_all(&migration_body[..split])
            .await
            .expect("migration should consume the first half before commit");
        let mut client_reader = PutObjReader::from_vec(b"new client body".to_vec());
        tokio::time::timeout(
            Duration::from_secs(5),
            set_disks.put_object(bucket, object, &mut client_reader, &ObjectOptions::default()),
        )
        .await
        .expect("client write must not wait for the migration body")
        .expect("client write should commit while migration waits for the remaining source body");
        let barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterNamespace);
        source
            .write_all(&migration_body[split..])
            .await
            .expect("migration should consume the remaining source body");
        drop(source);
        barrier.wait_until_paused().await;
        barrier.release();

        let err = migration
            .await
            .expect("migration task should join")
            .expect_err("migration must recheck the target after acquiring its commit lock");
        assert_eq!(err, StorageError::PreconditionFailed);

        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("client object should remain readable");
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("client object should drain");
        assert_eq!(body, b"new client body");
    }

    #[tokio::test]
    async fn conditional_put_does_not_block_reads_during_body_ingestion() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "conditional-put-nonblocking-read";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut initial_reader = PutObjReader::from_vec(b"old body".to_vec());
        let initial = set_disks
            .put_object(bucket, object, &mut initial_reader, &ObjectOptions::default())
            .await
            .expect("initial object should be written");
        let initial_etag = initial.etag.clone().expect("initial object should have an etag");

        let body = vec![b'c'; 64 * 1024];
        let split = body.len() / 2;
        let (mut source, stream) = tokio::io::duplex(64);
        let hash_reader = HashReader::from_stream(
            stream,
            i64::try_from(body.len()).expect("body length should fit i64"),
            i64::try_from(body.len()).expect("body length should fit i64"),
            None,
            None,
            false,
        )
        .expect("conditional hash reader should be created");
        let writer_store = Arc::clone(&set_disks);
        let etag_for_put = initial_etag.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::new(hash_reader);
            writer_store
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        http_preconditions: Some(HTTPPreconditions {
                            if_match: Some(etag_for_put),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await
        });

        source
            .write_all(&body[..split])
            .await
            .expect("conditional PUT should consume the first half of the body");
        let info = tokio::time::timeout(
            Duration::from_secs(5),
            set_disks.get_object_info(bucket, object, &ObjectOptions::default()),
        )
        .await
        .expect("reads must not wait for the conditional PUT body")
        .expect("the old version must stay readable during body ingestion");
        assert_eq!(info.etag.as_deref(), Some(initial_etag.as_str()));

        source
            .write_all(&body[split..])
            .await
            .expect("conditional PUT should consume the remaining body");
        drop(source);
        put.await
            .expect("conditional PUT task should join")
            .expect("conditional PUT should commit after the body completes");
    }

    #[tokio::test]
    async fn conditional_put_precondition_is_rechecked_at_commit() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "conditional-put-commit-recheck";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut initial_reader = PutObjReader::from_vec(b"old body".to_vec());
        let initial = set_disks
            .put_object(bucket, object, &mut initial_reader, &ObjectOptions::default())
            .await
            .expect("initial object should be written");
        let initial_etag = initial.etag.clone().expect("initial object should have an etag");

        let body = vec![b'c'; 64 * 1024];
        let split = body.len() / 2;
        let (mut source, stream) = tokio::io::duplex(64);
        let hash_reader = HashReader::from_stream(
            stream,
            i64::try_from(body.len()).expect("body length should fit i64"),
            i64::try_from(body.len()).expect("body length should fit i64"),
            None,
            None,
            false,
        )
        .expect("conditional hash reader should be created");
        let writer_store = Arc::clone(&set_disks);
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::new(hash_reader);
            writer_store
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        http_preconditions: Some(HTTPPreconditions {
                            if_match: Some(initial_etag),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await
        });

        source
            .write_all(&body[..split])
            .await
            .expect("conditional PUT should consume the first half of the body");
        let mut interloper_reader = PutObjReader::from_vec(b"interloper body".to_vec());
        tokio::time::timeout(
            Duration::from_secs(5),
            set_disks.put_object(bucket, object, &mut interloper_reader, &ObjectOptions::default()),
        )
        .await
        .expect("the interloper write must not wait for the conditional PUT body")
        .expect("the interloper write should commit while the conditional PUT streams");

        source
            .write_all(&body[split..])
            .await
            .expect("conditional PUT should consume the remaining body");
        drop(source);
        let err = put
            .await
            .expect("conditional PUT task should join")
            .expect_err("the conditional PUT must recheck its precondition under the commit lock");
        assert_eq!(err, StorageError::PreconditionFailed);

        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("the interloper object should remain readable");
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("the interloper object should drain");
        assert_eq!(body, b"interloper body");
    }

    #[tokio::test]
    async fn metadata_copy_no_lock_aborts_after_outer_namespace_lock_loss() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "copy-lost-outer-lock";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut initial_reader = PutObjReader::from_vec(b"original".to_vec());
        set_disks
            .put_object(bucket, object, &mut initial_reader, &ObjectOptions::default())
            .await
            .expect("initial object should be written");
        let mut copy_info = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("initial metadata should be readable");
        copy_info.metadata_only = true;
        Arc::make_mut(&mut copy_info.user_defined).insert("x-amz-meta-proof".to_string(), "mutated".to_string());

        let err = set_disks
            .copy_object(
                bucket,
                object,
                bucket,
                object,
                &mut copy_info,
                &ObjectOptions::default(),
                &ObjectOptions {
                    no_lock: true,
                    namespace_lock_fence: Some(NamespaceLockFence::lost_for_test()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("metadata copy must not commit after its outer namespace lock is lost");
        assert!(matches!(err, Error::NamespaceLockQuorumUnavailable { .. }));

        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("original metadata should remain readable");
        assert!(!current.user_defined.contains_key("x-amz-meta-proof"));
    }

    #[tokio::test]
    async fn explicit_version_overwrite_rechecks_object_lock_under_commit_lock() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-explicit-version-object-lock";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let retain_until = (OffsetDateTime::now_utc() + time::Duration::days(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        let cases = [
            (
                "compliance",
                HashMap::from([
                    (
                        X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
                        s3s::dto::ObjectLockRetentionMode::COMPLIANCE.to_string(),
                    ),
                    (X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(), retain_until.clone()),
                ]),
            ),
            (
                "governance",
                HashMap::from([
                    (
                        X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
                        s3s::dto::ObjectLockRetentionMode::GOVERNANCE.to_string(),
                    ),
                    (X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(), retain_until),
                ]),
            ),
            (
                "legal-hold",
                HashMap::from([(
                    X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str().to_string(),
                    s3s::dto::ObjectLockLegalHoldStatus::ON.to_string(),
                )]),
            ),
        ];

        for (case, lock_metadata) in cases {
            let object = format!("{case}-object");
            let original_body = format!("original-{case}").into_bytes();
            let mut initial_reader = PutObjReader::from_vec(original_body.clone());
            let initial = set_disks
                .put_object(
                    bucket,
                    &object,
                    &mut initial_reader,
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("initial version should be written");
            let version_id = initial
                .version_id
                .expect("versioned PUT should return a version ID")
                .to_string();

            let barrier = PutObjectCommitBarrier::install(bucket, &object, PutObjectCommitPause::BeforeNamespace);
            let overwrite_store = Arc::clone(&set_disks);
            let overwrite_object = object.clone();
            let overwrite_version = version_id.clone();
            let overwrite = tokio::spawn(async move {
                let mut reader = PutObjReader::from_vec(format!("replacement-{case}").into_bytes());
                overwrite_store
                    .put_object(
                        bucket,
                        &overwrite_object,
                        &mut reader,
                        &ObjectOptions {
                            version_id: Some(overwrite_version),
                            versioned: true,
                            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                                ObjectLockConfigState::ConfirmedAbsent,
                            ))),
                            ..Default::default()
                        },
                    )
                    .await
            });

            barrier.wait_until_paused().await;
            set_disks
                .put_object_metadata(
                    bucket,
                    &object,
                    &ObjectOptions {
                        version_id: Some(version_id.clone()),
                        versioned: true,
                        eval_metadata: Some(lock_metadata),
                        ..Default::default()
                    },
                )
                .await
                .expect("Object Lock metadata should win the race before the overwrite commit lock");
            barrier.release();

            let err = overwrite
                .await
                .expect("overwrite task should not panic")
                .expect_err("the commit-time Object Lock check must reject the overwrite");
            assert!(matches!(err, StorageError::PrefixAccessDenied(_, _)), "unexpected {case} error: {err}");

            let mut reader = set_disks
                .get_object_reader(
                    bucket,
                    &object,
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        version_id: Some(version_id),
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("the protected version should remain readable");
            let mut body = Vec::new();
            reader
                .stream
                .read_to_end(&mut body)
                .await
                .expect("protected body should drain");
            assert_eq!(body, original_body, "{case} overwrite must not change the protected body");
            drop(barrier);
        }

        let object = "commit-lock-proof";
        let mut initial_reader = PutObjReader::from_vec(b"original".to_vec());
        let initial = set_disks
            .put_object(
                bucket,
                object,
                &mut initial_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("initial lock-proof version should be written");
        let version_id = initial
            .version_id
            .expect("versioned PUT should return a version ID")
            .to_string();
        let barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterNamespace);
        let overwrite_store = Arc::clone(&set_disks);
        let overwrite_version = version_id.clone();
        let overwrite = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(b"replacement".to_vec());
            overwrite_store
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        version_id: Some(overwrite_version),
                        versioned: true,
                        object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                            ObjectLockConfigState::ConfirmedAbsent,
                        ))),
                        ..Default::default()
                    },
                )
                .await
        });

        barrier.wait_until_paused().await;
        let metadata_barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::BeforeMetadata);
        let metadata_store = Arc::clone(&set_disks);
        let metadata_version = version_id.clone();
        let mut metadata_update = tokio::spawn(async move {
            metadata_store
                .put_object_metadata(
                    bucket,
                    object,
                    &ObjectOptions {
                        version_id: Some(metadata_version),
                        versioned: true,
                        eval_metadata: Some(HashMap::from([("proof".to_string(), "blocked".to_string())])),
                        ..Default::default()
                    },
                )
                .await
        });
        metadata_barrier.wait_until_paused().await;
        metadata_barrier.release();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut metadata_update)
                .await
                .is_err(),
            "metadata update must block while PUT holds the commit lock"
        );
        drop(metadata_barrier);
        barrier.release();
        overwrite
            .await
            .expect("overwrite task should not panic")
            .expect("unlocked exact-version overwrite should commit");
        metadata_update
            .await
            .expect("metadata task should not panic")
            .expect("metadata update should proceed after PUT releases the commit lock");
        drop(barrier);
    }

    #[tokio::test]
    async fn explicit_version_overwrite_honors_bucket_default_compliance() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-default-compliance-object-lock";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let original_body = b"default-compliance-original".to_vec();
        let mut initial_reader = PutObjReader::from_vec(original_body.clone());
        let initial = set_disks
            .put_object(
                bucket,
                object,
                &mut initial_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("initial version should be written");
        let version_id = initial
            .version_id
            .expect("versioned PUT should return a version ID")
            .to_string();
        let default_compliance = ObjectLockConfigState::Configured {
            config: s3s::dto::ObjectLockConfiguration {
                object_lock_enabled: Some(s3s::dto::ObjectLockEnabled::from_static(s3s::dto::ObjectLockEnabled::ENABLED)),
                rule: Some(s3s::dto::ObjectLockRule {
                    default_retention: Some(s3s::dto::DefaultRetention {
                        mode: Some(s3s::dto::ObjectLockRetentionMode::from_static(
                            s3s::dto::ObjectLockRetentionMode::COMPLIANCE,
                        )),
                        days: Some(1),
                        years: None,
                    }),
                }),
            },
            updated_at: OffsetDateTime::now_utc(),
        };
        let mut replacement = PutObjReader::from_vec(b"replacement".to_vec());
        let err = set_disks
            .put_object(
                bucket,
                object,
                &mut replacement,
                &ObjectOptions {
                    version_id: Some(version_id.clone()),
                    versioned: true,
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(default_compliance))),
                    ..Default::default()
                },
            )
            .await
            .expect_err("bucket default COMPLIANCE must block an exact-version overwrite");
        assert!(matches!(err, StorageError::PrefixAccessDenied(_, _)));

        let mut reader = set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    version_id: Some(version_id),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the default-protected version should remain readable");
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("protected body should drain");
        assert_eq!(body, original_body);
    }

    /// A local PutObjectRetention / PutObjectLegalHold clear persists empty
    /// lock keys (`parse_object_lock_retention`). The commit-time WORM gate
    /// must read that as unlocked: an explicit-version PUT (the inbound
    /// replication transport) and a version delete both have to succeed
    /// (rustfs/backlog#1953).
    #[tokio::test]
    async fn explicit_version_overwrite_and_delete_succeed_after_local_lock_clear() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "put-explicit-version-cleared-lock";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut initial_reader = PutObjReader::from_vec(b"original".to_vec());
        let initial = set_disks
            .put_object(
                bucket,
                object,
                &mut initial_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("initial version should be written");
        let version_id = initial
            .version_id
            .expect("versioned PUT should return a version ID")
            .to_string();
        let version_opts = ObjectOptions {
            versioned: true,
            version_id: Some(version_id.clone()),
            delete_replication_config_snapshot: Some(Arc::new(DeleteReplicationConfigSnapshot::default())),
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))),
            ..Default::default()
        };
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(HashMap::from([
                        (X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(), String::new()),
                        (X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(), String::new()),
                        (X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str().to_string(), String::new()),
                    ])),
                    ..version_opts.clone()
                },
            )
            .await
            .expect("cleared lock metadata should be written");

        let mut replacement = PutObjReader::from_vec(b"replacement".to_vec());
        set_disks
            .put_object(bucket, object, &mut replacement, &version_opts)
            .await
            .expect("explicit-version PUT must not be wedged by cleared lock metadata");

        set_disks
            .delete_object(bucket, object, version_opts)
            .await
            .expect("version delete must not be wedged by cleared lock metadata");
    }

    #[tokio::test]
    async fn version_only_copy_checks_the_destination_version_object_lock() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "copy-destination-object-lock";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut source_reader = PutObjReader::from_vec(b"source-version".to_vec());
        let source = set_disks
            .put_object(
                bucket,
                object,
                &mut source_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source version should be written");
        let source_version = source.version_id.expect("source version ID").to_string();

        let destination_body = b"protected-destination".to_vec();
        let mut destination_reader = PutObjReader::from_vec(destination_body.clone());
        let destination = set_disks
            .put_object(
                bucket,
                object,
                &mut destination_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("destination version should be written");
        let destination_version = destination.version_id.expect("destination version ID").to_string();
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(destination_version.clone()),
                    versioned: true,
                    eval_metadata: Some(HashMap::from([(
                        X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str().to_string(),
                        s3s::dto::ObjectLockLegalHoldStatus::ON.to_string(),
                    )])),
                    ..Default::default()
                },
            )
            .await
            .expect("destination legal hold should be written");

        let mut source_info = set_disks
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(source_version.clone()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source version should be readable");
        source_info.version_only = true;
        let err = set_disks
            .copy_object(
                bucket,
                object,
                bucket,
                object,
                &mut source_info,
                &ObjectOptions {
                    version_id: Some(source_version),
                    versioned: true,
                    ..Default::default()
                },
                &ObjectOptions {
                    version_id: Some(destination_version.clone()),
                    versioned: true,
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                },
            )
            .await
            .expect_err("source metadata must not overwrite a locked destination version");
        assert!(matches!(err, StorageError::PrefixAccessDenied(_, _)));

        let mut reader = set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    version_id: Some(destination_version),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("protected destination should remain readable");
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("destination body should drain");
        assert_eq!(body, destination_body);
    }
}

#[cfg(test)]
mod put_object_tags_early_stop_regression_tests {
    //! Regression coverage for backlog#881: read-before-write tagging under GET
    //! metadata early-stop. backlog#872 enabled metadata early-stop by default;
    //! an early-stopped metadata fanout returns only a read-quorum subset of
    //! disks, and `put_object_tags` reuses that disk set as its write target, so
    //! it shrank the write set and failed write quorum with SlowDown. The fix
    //! forces a full-quorum fanout (allow_early_stop=false) inside
    //! `put_object_tags`. This multi-disk integration test pins that end to end:
    //! with early-stop enabled, the tag must land on EVERY online disk's xl.meta,
    //! not a read-quorum subset.

    use super::hermetic_set_disks_support::hermetic_set_disks_isolated as hermetic_set_disks;
    use super::*;
    use crate::disk::ReadOptions;

    #[tokio::test]
    async fn put_object_tags_writes_all_online_disks_under_early_stop() {
        // Early-stop defaults to on; pin it explicitly (enabled + full rollout)
        // so the regression scenario holds regardless of default/rollout drift.
        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_EARLY_STOP_ROLLOUT_PCT", Some("100")),
            ],
            async {
                let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
                let bucket = "backlog881-early-stop-bucket";
                let object = "read-before-write-tagged-object";
                for disk in &disk_stores {
                    disk.make_volume(bucket).await.expect("bucket volume should be created");
                }

                let mut reader = PutObjReader::from_vec(vec![5u8; 1024]);
                set_disks
                    .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                    .await
                    .expect("put_object should succeed");

                let (fi, _, disks) = set_disks
                    .get_object_fileinfo(
                        bucket,
                        object,
                        &ObjectOptions {
                            no_lock: true,
                            metadata_cache_safe: false,
                            ..Default::default()
                        },
                        true,
                        false,
                    )
                    .await
                    .expect("object metadata should be readable before adding the checksum sidecar")
                    .into_owned();
                let mut fi = fi;
                rustfs_utils::http::insert_str(
                    &mut fi.metadata,
                    rustfs_utils::http::SUFFIX_PART_CHECKSUMS,
                    r#"[[1,[["CRC32C","AAAAAA=="]]]]"#.to_string(),
                );
                set_disks
                    .update_object_meta(bucket, object, fi, &disks)
                    .await
                    .expect("checksum sidecar should be persisted before tagging");
                set_disks.invalidate_get_object_metadata_cache(bucket, object).await;

                let tags = "unit=backlog881&stage=regression";
                set_disks
                    .put_object_tags(bucket, object, tags, &ObjectOptions::default())
                    .await
                    .expect("put_object_tags must not fail write quorum (SlowDown) under early-stop");

                // Core assertion: the tag must be present on EVERY online disk,
                // proving the write target was the full set and not an
                // early-stopped read-quorum subset.
                for (idx, disk) in disk_stores.iter().enumerate() {
                    let fi = disk
                        .read_version("", bucket, object, "", &ReadOptions::default())
                        .await
                        .unwrap_or_else(|e| panic!("disk {idx} must hold xl.meta for the tagged object: {e}"));
                    assert_eq!(
                        fi.metadata.get(AMZ_OBJECT_TAGGING).map(String::as_str),
                        Some(tags),
                        "disk {idx} must carry the tag written under early-stop (write set not shrunk to a read-quorum subset)"
                    );
                    assert_eq!(
                        rustfs_utils::http::get_consistent_str(&fi.metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS),
                        Some(r#"[[1,[["CRC32C","AAAAAA=="]]]]"#),
                        "disk {idx} must retain the checksum sidecar across the tag metadata update"
                    );
                }
            },
        )
        .await;
    }
}

#[cfg(test)]
mod object_tagging_namespace_lock_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks_isolated as hermetic_set_disks;
    use super::*;
    use crate::disk::ReadOptions;

    #[derive(Clone, Copy, Debug)]
    enum CompetingMutation {
        Put,
        Delete,
    }

    async fn read_body(set_disks: &SetDisks, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), opts)
            .await?
            .stream
            .read_to_end(&mut body)
            .await?;
        Ok(body)
    }

    #[tokio::test]
    async fn tagging_honors_an_inherited_namespace_write_lock() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "tag-lock-inherited";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut reader = PutObjReader::from_vec(b"body".to_vec());
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("object should be written");

        let outer_lock = set_disks
            .new_ns_lock(bucket, object)
            .await
            .expect("outer namespace lock should be created")
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("outer namespace write lock should be acquired");
        set_disks
            .put_object_tags(
                bucket,
                object,
                "lock=inherited",
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("tagging should not reacquire an inherited namespace lock");
        drop(outer_lock);

        assert_eq!(
            set_disks
                .get_object_tags(bucket, object, &ObjectOptions::default())
                .await
                .expect("persisted tags should remain readable"),
            "lock=inherited"
        );
    }

    #[tokio::test]
    async fn version_delete_returns_tags_read_under_the_delete_lock() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "delete-locked-tags";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut reader = PutObjReader::from_vec(b"body".to_vec());
        let uploaded = set_disks
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("versioned object should be written");
        let version_id = uploaded.version_id.expect("versioned PUT should return an ID").to_string();
        let version_opts = ObjectOptions {
            versioned: true,
            version_id: Some(version_id),
            delete_replication_config_snapshot: Some(Arc::new(DeleteReplicationConfigSnapshot::default())),
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))),
            ..Default::default()
        };
        set_disks
            .put_object_tags(bucket, object, "generation=stale", &version_opts)
            .await
            .expect("initial tags should be written");
        let advisory = set_disks
            .get_object_info(bucket, object, &version_opts)
            .await
            .expect("advisory pre-read should succeed");
        set_disks
            .put_object_tags(bucket, object, "generation=locked", &version_opts)
            .await
            .expect("concurrent tag update should commit before delete");

        let deleted = set_disks
            .delete_object(bucket, object, version_opts)
            .await
            .expect("version delete should succeed");

        assert_eq!(advisory.user_tags.as_str(), "generation=stale");
        assert_eq!(deleted.user_tags.as_str(), "generation=locked");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn tagging_serializes_with_put_and_delete_for_versioned_and_unversioned_objects() {
        for versioned in [false, true] {
            for mutation in [CompetingMutation::Put, CompetingMutation::Delete] {
                let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
                let bucket = format!("tag-lock-{}-{mutation:?}", if versioned { "versioned" } else { "plain" }).to_lowercase();
                let object = "object";
                for disk in &disk_stores {
                    disk.make_volume(&bucket).await.expect("bucket volume should be created");
                }

                let object_opts = ObjectOptions {
                    versioned,
                    delete_replication_config_snapshot: Some(Arc::new(DeleteReplicationConfigSnapshot::default())),
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                };
                let original_body = b"original body".to_vec();
                let mut original_reader = PutObjReader::from_vec(original_body.clone());
                let original = set_disks
                    .put_object(&bucket, object, &mut original_reader, &object_opts)
                    .await
                    .expect("original object should be written");
                let original_version = original.version_id.map(|version| version.to_string());
                set_disks
                    .put_object_tags(&bucket, object, "stage=initial", &object_opts)
                    .await
                    .expect("initial tags should be written");

                let barrier = ObjectTaggingCommitBarrier::install(&bucket, object);
                let tagging_set = Arc::clone(&set_disks);
                let tagging_bucket = bucket.clone();
                let tagging_opts = object_opts.clone();
                let tagging = tokio::spawn(async move {
                    match mutation {
                        CompetingMutation::Put => {
                            tagging_set
                                .put_object_tags(&tagging_bucket, object, "stage=before-mutation", &tagging_opts)
                                .await
                        }
                        CompetingMutation::Delete => tagging_set.delete_object_tags(&tagging_bucket, object, &tagging_opts).await,
                    }
                });
                barrier.wait_until_paused().await;

                let mutation_set = Arc::clone(&set_disks);
                let mutation_bucket = bucket.clone();
                let mutation_opts = object_opts.clone();
                let (mutation_started_tx, mutation_started_rx) = tokio::sync::oneshot::channel();
                let competing = tokio::spawn(async move {
                    mutation_started_tx
                        .send(())
                        .expect("tagging test should wait for the competing mutation");
                    match mutation {
                        CompetingMutation::Put => {
                            let mut replacement = PutObjReader::from_vec(b"replacement body".to_vec());
                            mutation_set
                                .put_object(&mutation_bucket, object, &mut replacement, &mutation_opts)
                                .await
                                .map(Some)
                        }
                        CompetingMutation::Delete => mutation_set
                            .delete_object(&mutation_bucket, object, mutation_opts)
                            .await
                            .map(|_| None),
                    }
                });
                mutation_started_rx
                    .await
                    .expect("competing mutation should reach the namespace operation while tagging is paused");

                barrier.release();
                tagging
                    .await
                    .expect("tagging task should not panic")
                    .expect("tagging should commit before the queued mutation");
                let competing_result = competing
                    .await
                    .expect("competing mutation task should not panic")
                    .expect("competing mutation should commit after tagging releases the lock");

                match mutation {
                    CompetingMutation::Put => {
                        let replacement = competing_result.expect("put mutation should return the replacement object");
                        let current = set_disks
                            .get_object_info(&bucket, object, &object_opts)
                            .await
                            .expect("replacement metadata should remain readable");
                        assert_eq!(
                            read_body(&set_disks, &bucket, object, &object_opts)
                                .await
                                .expect("replacement version should remain readable"),
                            b"replacement body"
                        );
                        assert_eq!(current.etag, replacement.etag, "tagging must not restore the previous version's ETag");
                        assert!(
                            current.user_tags.is_empty(),
                            "a tag update ordered before the replacement must not leak onto the replacement version"
                        );
                    }
                    CompetingMutation::Delete if versioned => {
                        let old_version_opts = ObjectOptions {
                            versioned: true,
                            version_id: original_version,
                            ..Default::default()
                        };
                        assert_eq!(
                            read_body(&set_disks, &bucket, object, &old_version_opts)
                                .await
                                .expect("the version hidden by the delete marker should remain readable"),
                            original_body
                        );
                        let old_version = set_disks
                            .get_object_info(&bucket, object, &old_version_opts)
                            .await
                            .expect("the historical version metadata should remain readable");
                        assert!(
                            old_version.user_tags.is_empty(),
                            "delete-tagging ordered before the delete marker must persist on the historical version"
                        );
                        for disk in &disk_stores {
                            let current = disk
                                .read_version("", &bucket, object, "", &ReadOptions::default())
                                .await
                                .expect("the current delete marker should remain visible on every disk");
                            assert!(current.deleted);
                        }
                    }
                    CompetingMutation::Delete => {
                        let error = read_body(&set_disks, &bucket, object, &object_opts)
                            .await
                            .expect_err("unversioned deletion must not be undone by a stale tagging write");
                        assert!(
                            is_err_object_not_found(&error) || is_err_version_not_found(&error),
                            "unexpected error after unversioned delete: {error:?}"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod delete_objects_lock_gating_tests {
    //! Regression coverage for backlog#929 (HP-8): the batch-delete per-object
    //! stat is gated on the bucket object-lock configuration. For buckets whose
    //! metadata is unknown the gate fails closed, so these hermetic tests (their
    //! buckets are never registered with the metadata sys) exercise the
    //! locked-stat path and prove the #4297 delete protection is intact end to
    //! end, while per-key result mapping of mixed batches stays stable.

    use super::hermetic_set_disks_support::hermetic_set_disks_isolated as hermetic_set_disks;
    use super::hermetic_set_disks_support::hermetic_set_disks_with_lockers_and_ctx;
    use super::*;
    use serial_test::serial;

    async fn put_plain_object(set_disks: &Arc<SetDisks>, bucket: &str, object: &str) {
        let mut reader = PutObjReader::from_vec(vec![3u8; 1024]);
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("plain object should be written");
    }

    #[tokio::test]
    async fn delete_objects_reports_mixed_results_per_key() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "hp8-mixed-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        put_plain_object(&set_disks, bucket, "obj-a").await;
        put_plain_object(&set_disks, bucket, "obj-c").await;

        let objects = vec![
            ObjectToDelete {
                object_name: "obj-a".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "missing-b".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "obj-c".to_string(),
                ..Default::default()
            },
        ];

        let (deleted, errs) = set_disks
            .delete_objects(
                bucket,
                objects,
                ObjectOptions {
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                },
            )
            .await;

        assert_eq!(deleted.len(), 3);
        assert_eq!(errs.len(), 3);
        assert!(
            errs.iter().all(Option::is_none),
            "S3 batch delete reports missing keys as deleted, not as errors: {errs:?}"
        );
        assert_eq!(deleted[0].object_name, "obj-a");
        assert_eq!(deleted[1].object_name, "missing-b");
        assert_eq!(deleted[2].object_name, "obj-c");
        assert!(deleted[0].found, "existing key must be reported as found");
        assert!(!deleted[1].found, "missing key must be reported as not found");
        assert!(deleted[2].found, "existing key must be reported as found");

        for object in ["obj-a", "obj-c"] {
            set_disks
                .get_object_info(bucket, object, &ObjectOptions::default())
                .await
                .expect_err("deleted object must be gone");
        }
    }

    #[tokio::test]
    async fn delete_objects_derives_per_object_versioning_from_the_request_snapshot() {
        use s3s::dto::{BucketVersioningStatus, ExcludedPrefix, VersioningConfiguration};

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "batch-versioning-snapshot-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        put_plain_object(&set_disks, bucket, "marker-object").await;
        put_plain_object(&set_disks, bucket, "archive/unversioned-object").await;
        let snapshot = Arc::new(DeleteReplicationConfigSnapshot::from_configs_for_test(
            VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                excluded_prefixes: Some(vec![ExcludedPrefix {
                    prefix: Some("archive/".to_string()),
                }]),
                ..Default::default()
            },
            None,
        ));
        let objects = vec![
            ObjectToDelete {
                object_name: "marker-object".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "archive/unversioned-object".to_string(),
                ..Default::default()
            },
        ];

        let (deleted, errors) = set_disks
            .delete_objects(
                bucket,
                objects,
                ObjectOptions {
                    versioned: true,
                    delete_replication_config_snapshot: Some(snapshot),
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                },
            )
            .await;

        assert!(
            errors.iter().all(Option::is_none),
            "snapshot-backed batch delete should succeed: {errors:?}"
        );
        assert!(deleted[0].delete_marker);
        assert!(deleted[0].delete_marker_version_id.is_some());
        assert!(!deleted[1].delete_marker);
        set_disks
            .get_object_info(bucket, "archive/unversioned-object", &ObjectOptions::default())
            .await
            .expect_err("the snapshot-excluded object should be removed without a delete marker");
    }

    #[tokio::test]
    async fn batch_version_delete_uses_tags_read_under_the_delete_lock() {
        use rustfs_utils::http::headers::AMZ_OBJECT_TAGGING;
        use s3s::dto::{
            BucketVersioningStatus, DeleteReplication, DeleteReplicationStatus, Destination, ReplicationConfiguration,
            ReplicationRule, ReplicationRuleFilter, ReplicationRuleStatus, Tag, VersioningConfiguration,
        };

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "batch-delete-locked-tags";
        let object = "object";
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut reader = PutObjReader::from_vec(b"body".to_vec());
        let uploaded = set_disks
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    user_defined: HashMap::from([(AMZ_OBJECT_TAGGING.to_string(), "generation=stale".to_string())]),
                    ..Default::default()
                },
            )
            .await
            .expect("versioned object should be written");
        let version_id = uploaded.version_id.expect("versioned PUT should return an ID");
        let version_opts = ObjectOptions {
            versioned: true,
            version_id: Some(version_id.to_string()),
            ..Default::default()
        };
        set_disks
            .put_object_tags(bucket, object, "generation=locked", &version_opts)
            .await
            .expect("tag update should commit before the batch delete");

        let snapshot = Arc::new(DeleteReplicationConfigSnapshot::from_configs_for_test(
            VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                ..Default::default()
            },
            Some(ReplicationConfiguration {
                role: String::new(),
                rules: vec![ReplicationRule {
                    delete_marker_replication: None,
                    delete_replication: Some(DeleteReplication {
                        status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
                    }),
                    destination: Destination {
                        bucket: arn.to_string(),
                        ..Default::default()
                    },
                    existing_object_replication: None,
                    filter: Some(ReplicationRuleFilter {
                        tag: Some(Tag {
                            key: Some("generation".to_string()),
                            value: Some("locked".to_string()),
                        }),
                        ..Default::default()
                    }),
                    id: Some("delete".to_string()),
                    prefix: Some(String::new()),
                    priority: Some(1),
                    source_selection_criteria: None,
                    status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
                }],
            }),
        ));
        let (deleted, errors) = set_disks
            .delete_objects(
                bucket,
                vec![ObjectToDelete {
                    object_name: object.to_string(),
                    version_id: Some(version_id),
                    ..Default::default()
                }],
                ObjectOptions {
                    versioned: true,
                    delete_replication_config_snapshot: Some(snapshot),
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                },
            )
            .await;

        assert!(errors.iter().all(Option::is_none), "batch version delete should succeed: {errors:?}");
        let state = deleted[0]
            .replication_state
            .as_ref()
            .expect("locked decision should be persisted");
        assert_eq!(
            state.version_purge_status_internal.as_deref(),
            Some("arn:rustfs:replication:us-east-1:target:bucket=PENDING;")
        );
        assert!(state.replicate_decision_str.contains(arn));
    }

    #[tokio::test]
    #[serial]
    async fn lifecycle_delete_all_history_records_exact_replication_purge() {
        use s3s::dto::{
            BucketVersioningStatus, DeleteReplication, DeleteReplicationStatus, Destination, ReplicationConfiguration,
            ReplicationRule, ReplicationRuleStatus, VersioningConfiguration,
        };

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "lifecycle-delete-all-replication";
        let object = "object";
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut first_reader = PutObjReader::from_vec(b"first".to_vec());
        let first = set_disks
            .put_object(
                bucket,
                object,
                &mut first_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("first version should be written");
        let first_version_id = first.version_id.expect("first PUT should return a version id");
        let mut trigger_reader = PutObjReader::from_vec(b"trigger".to_vec());
        let trigger = set_disks
            .put_object(
                bucket,
                object,
                &mut trigger_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("trigger version should be written");
        let trigger_version_id = trigger.version_id.expect("trigger PUT should return a version id");

        let snapshot = Arc::new(DeleteReplicationConfigSnapshot::from_configs_for_test(
            VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                ..Default::default()
            },
            Some(ReplicationConfiguration {
                role: String::new(),
                rules: vec![ReplicationRule {
                    delete_marker_replication: None,
                    delete_replication: Some(DeleteReplication {
                        status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
                    }),
                    destination: Destination {
                        bucket: arn.to_string(),
                        ..Default::default()
                    },
                    existing_object_replication: None,
                    filter: None,
                    id: Some("delete-all-purge".to_string()),
                    prefix: Some(String::new()),
                    priority: Some(1),
                    source_selection_criteria: None,
                    status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
                }],
            }),
        ));
        let mut opts = ObjectOptions {
            delete_prefix: true,
            delete_prefix_object: true,
            versioned: true,
            lifecycle_delete_all: Some(crate::object_api::LifecycleDeleteAllRequest {
                version_id: Some(trigger_version_id),
                delete_marker: false,
                action: rustfs_scanner_metrics::metrics::IlmAction::DeleteAllVersionsAction,
                rule_id: "rule".to_string(),
                phase: crate::object_api::LifecycleDeleteAllPhase::History,
            }),
            delete_replication_config_snapshot: Some(snapshot),
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))),
            ..Default::default()
        };
        opts.ensure_lifecycle_delete_all_journal();
        let _ = crate::bucket::replication::ReplicationLifecycleBridge::take_scheduled_deletes_for_test();

        set_disks
            .delete_object(bucket, object, opts.clone())
            .await
            .expect("history phase should delete the old version");

        let scheduled = crate::bucket::replication::ReplicationLifecycleBridge::take_scheduled_deletes_for_test();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].object_name, object);
        assert_eq!(scheduled[0].version_id, Some(first_version_id));
        assert!(!scheduled[0].delete_marker);
        let state = scheduled[0]
            .replication_state
            .as_ref()
            .expect("delete-all history purge should carry replication state");
        assert_eq!(state.version_purge_status_internal.as_deref(), Some(format!("{arn}=PENDING;").as_str()));
        assert!(state.replicate_decision_str.contains(arn));
    }

    #[tokio::test]
    async fn synthetic_directory_delete_uses_decoded_prefix_and_marker_switch() {
        use s3s::dto::{
            BucketVersioningStatus, DeleteMarkerReplication, DeleteMarkerReplicationStatus, DeleteReplication,
            DeleteReplicationStatus, Destination, ReplicationConfiguration, ReplicationRule, ReplicationRuleFilter,
            ReplicationRuleStatus, VersioningConfiguration,
        };

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "batch-directory-replication";
        let object = encode_dir_object("photos/");
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        put_plain_object(&set_disks, bucket, &object).await;

        let snapshot = Arc::new(DeleteReplicationConfigSnapshot::from_configs_for_test(
            VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                ..Default::default()
            },
            Some(ReplicationConfiguration {
                role: String::new(),
                rules: vec![ReplicationRule {
                    delete_marker_replication: Some(DeleteMarkerReplication {
                        status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
                    }),
                    delete_replication: Some(DeleteReplication {
                        status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::DISABLED),
                    }),
                    destination: Destination {
                        bucket: arn.to_string(),
                        ..Default::default()
                    },
                    existing_object_replication: None,
                    filter: Some(ReplicationRuleFilter {
                        prefix: Some("photos/".to_string()),
                        ..Default::default()
                    }),
                    id: Some("directory-marker".to_string()),
                    prefix: None,
                    priority: Some(1),
                    source_selection_criteria: None,
                    status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
                }],
            }),
        ));

        let (deleted, errors) = set_disks
            .delete_objects(
                bucket,
                vec![ObjectToDelete {
                    object_name: object,
                    version_id: Some(Uuid::nil()),
                    synthetic_version_id: true,
                    ..Default::default()
                }],
                ObjectOptions {
                    versioned: true,
                    delete_replication_config_snapshot: Some(snapshot),
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                },
            )
            .await;

        assert!(errors[0].is_none(), "directory delete should succeed: {:?}", errors[0]);
        let state = deleted[0]
            .replication_state
            .as_ref()
            .expect("marker replication decision should be persisted");
        assert_eq!(state.replication_status_internal.as_deref(), Some(format!("{arn}=PENDING;").as_str()));
        assert!(state.version_purge_status_internal.is_none());
    }

    #[tokio::test]
    async fn delete_objects_aborts_before_disk_mutation_after_outer_lock_loss() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "batch-lost-outer-lock";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        put_plain_object(&set_disks, bucket, object).await;

        let (_deleted, errors) = set_disks
            .delete_objects(
                bucket,
                vec![ObjectToDelete {
                    object_name: object.to_string(),
                    ..Default::default()
                }],
                ObjectOptions {
                    no_lock: true,
                    namespace_lock_fence: Some(NamespaceLockFence::lost_for_test()),
                    delete_replication_config_snapshot: Some(Arc::new(DeleteReplicationConfigSnapshot::default())),
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                },
            )
            .await;

        assert!(
            matches!(errors[0], Some(Error::NamespaceLockQuorumUnavailable { .. })),
            "lost outer lock must fail the batch before disk mutation: {:?}",
            errors[0]
        );
        set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("object must survive a lost-lock batch");
    }

    #[tokio::test]
    async fn delete_object_aborts_when_outer_lock_is_lost_at_commit() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "single-lost-outer-lock";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        put_plain_object(&set_disks, bucket, object).await;

        let barrier = DeleteObjectCommitBarrier::install(bucket, object);
        let (fence, loss_handle) = NamespaceLockFence::loss_handle_for_test();
        let delete_store = Arc::clone(&set_disks);
        let delete = tokio::spawn(async move {
            delete_store
                .delete_object(
                    bucket,
                    object,
                    ObjectOptions {
                        no_lock: true,
                        namespace_lock_fence: Some(fence),
                        object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                            ObjectLockConfigState::ConfirmedAbsent,
                        ))),
                        ..Default::default()
                    },
                )
                .await
        });
        barrier.wait_until_paused().await;
        loss_handle.store(true, std::sync::atomic::Ordering::Release);
        barrier.release();

        let err = delete
            .await
            .expect("delete task should join")
            .expect_err("lock loss at commit must abort the delete");
        assert!(matches!(err, Error::NamespaceLockQuorumUnavailable { .. }));
        set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("object must survive commit-time lock loss");
    }

    #[tokio::test]
    async fn delete_objects_blocks_locked_object_and_deletes_the_rest() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "hp8-locked-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        // COMPLIANCE retention metadata on the object; this bucket is unknown
        // to the metadata sys, so the fail-closed gate must keep the held-lock
        // stat and the #4297 rejection.
        let retain_until = OffsetDateTime::now_utc() + Duration::from_secs(60 * 60 * 24 * 30);
        let mut user_defined = HashMap::new();
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
            s3s::dto::ObjectLockRetentionMode::COMPLIANCE.to_string(),
        );
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
            retain_until
                .format(&time::format_description::well_known::Rfc3339)
                .expect("retain-until date should format"),
        );
        let mut reader = PutObjReader::from_vec(vec![7u8; 512]);
        set_disks
            .put_object(
                bucket,
                "locked",
                &mut reader,
                &ObjectOptions {
                    user_defined,
                    ..Default::default()
                },
            )
            .await
            .expect("locked object should be written");

        put_plain_object(&set_disks, bucket, "plain").await;

        let objects = vec![
            ObjectToDelete {
                object_name: "locked".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "plain".to_string(),
                ..Default::default()
            },
        ];

        let (_deleted, errs) = set_disks
            .delete_objects(
                bucket,
                objects,
                ObjectOptions {
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                },
            )
            .await;

        let lock_err = errs[0]
            .as_ref()
            .expect("COMPLIANCE retention must block the batch delete entry");
        assert!(
            matches!(lock_err, Error::PrefixAccessDenied(_, _)),
            "locked entry must fail with access denied, got: {lock_err:?}"
        );
        assert!(errs[1].is_none(), "unlocked entry must still be deleted: {:?}", errs[1]);

        set_disks
            .get_object_info(bucket, "locked", &ObjectOptions::default())
            .await
            .expect("locked object must survive the batch delete");
        set_disks
            .get_object_info(bucket, "plain", &ObjectOptions::default())
            .await
            .expect_err("plain object must be deleted");
    }

    /// Pins the dist-erasure resolution SOURCE for the batch-delete lock gate
    /// (adversarial review): the decision must come from the set's own
    /// instance context. With a DistErasure context and no dist lockers the
    /// batch must fail closed on lock acquisition; ambient resolution
    /// (non-dist in this process) would take local locks and let the delete
    /// through.
    #[tokio::test]
    async fn delete_objects_dist_gate_uses_set_instance_context() {
        let dist_ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        dist_ctx
            .update_erasure_type(crate::layout::endpoints::SetupType::DistErasure)
            .await;
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers_and_ctx(4, 0, 2, Vec::new(), dist_ctx).await;
        let bucket = "dist-gate-ctx-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        // The put must bypass locking: with a DistErasure context and no
        // lockers, every namespace lock acquisition fails closed by design.
        let mut reader = PutObjReader::from_vec(vec![5u8; 256]);
        set_disks
            .put_object(
                bucket,
                "obj",
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("object should be written without locks");

        let objects = vec![ObjectToDelete {
            object_name: "obj".to_string(),
            ..Default::default()
        }];
        let (_deleted, errs) = set_disks
            .delete_objects(
                bucket,
                objects,
                ObjectOptions {
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                },
            )
            .await;

        assert!(
            errs[0].is_some(),
            "the dist gate resolved from the set's own context must fail closed on an empty locker list"
        );
        set_disks
            .get_object_info(
                bucket,
                "obj",
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("object must survive the failed batch delete");
    }

    #[tokio::test]
    async fn delete_objects_honors_no_lock_when_outer_write_lock_is_held() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "batch-no-lock-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        put_plain_object(&set_disks, bucket, "obj-a").await;

        let _outer_guard = set_disks
            .new_ns_lock(bucket, "obj-a")
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("outer write lock should be acquired");

        let objects = vec![ObjectToDelete {
            object_name: "obj-a".to_string(),
            ..Default::default()
        }];

        let (deleted, errs) = tokio::time::timeout(
            Duration::from_secs(1),
            set_disks.delete_objects(
                bucket,
                objects,
                ObjectOptions {
                    no_lock: true,
                    object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                        ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                },
            ),
        )
        .await
        .expect("no_lock batch delete path must not wait for the outer lock");

        assert!(errs[0].is_none(), "no_lock batch delete should not fail with a lock error: {:?}", errs[0]);
        assert!(deleted[0].found, "existing object must still be deleted");
    }
}

#[cfg(test)]
mod body_cache_hook_gate_tests {
    //! Regression coverage for backlog#1108 (raw data-movement reads) and
    //! backlog#1109 (compressed objects): the app-layer body-cache hook serves
    //! cached full-object plaintext directly, bypassing `ReadPlan`. It must not
    //! be probed when the normal read path would return a different byte stream.

    use super::full_object_plaintext_len;
    use crate::object_api::{ObjectInfo, ObjectOptions};
    use std::collections::HashMap;
    use std::sync::Arc;

    const STORED_SIZE: i64 = 1024;
    const PLAINTEXT_SIZE: i64 = 4096;

    fn plain_object_info() -> ObjectInfo {
        ObjectInfo {
            size: STORED_SIZE,
            ..Default::default()
        }
    }

    fn compressed_object_info() -> ObjectInfo {
        let mut user_defined = HashMap::new();
        // is_compressed() checks for the internal compression suffix key.
        user_defined.insert("x-rustfs-internal-compression".to_string(), "klauspost/compress/s2".to_string());
        user_defined.insert("x-rustfs-internal-actual-size".to_string(), PLAINTEXT_SIZE.to_string());
        ObjectInfo {
            size: STORED_SIZE,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        }
    }

    fn encrypted_object_info() -> ObjectInfo {
        let mut user_defined = HashMap::new();
        user_defined.insert("x-minio-encryption-key".to_string(), "opaque".to_string());
        ObjectInfo {
            size: STORED_SIZE,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        }
    }

    fn restore_opts() -> ObjectOptions {
        let mut opts = ObjectOptions::default();
        opts.transition.restore_request.days = Some(1);
        opts
    }

    #[test]
    fn plain_full_object_read_yields_its_stored_size() {
        let len = full_object_plaintext_len(&None, &ObjectOptions::default(), &plain_object_info());
        assert_eq!(len, Some(STORED_SIZE));
    }

    #[test]
    fn compressed_object_yields_decompressed_size() {
        // ReadTransform::Compressed publishes the decompressed length as
        // object_info.size; the hit site must reproduce exactly this value or
        // UploadPartCopy truncates the copy (backlog#1109).
        let len = full_object_plaintext_len(&None, &ObjectOptions::default(), &compressed_object_info());
        assert_eq!(len, Some(PLAINTEXT_SIZE));
    }

    #[test]
    fn raw_data_movement_read_is_refused() {
        // Decommission reads set raw_data_movement_read: ReadPlan returns the
        // STORED bytes, so the cached decompressed body must not be served
        // (backlog#1108 — silent data corruption on the destination pool).
        let opts = ObjectOptions {
            raw_data_movement_read: true,
            ..Default::default()
        };
        assert_eq!(full_object_plaintext_len(&None, &opts, &plain_object_info()), None);
    }

    #[test]
    fn data_movement_read_is_refused() {
        let opts = ObjectOptions {
            data_movement: true,
            ..Default::default()
        };
        assert_eq!(full_object_plaintext_len(&None, &opts, &plain_object_info()), None);
    }

    #[test]
    fn restore_read_of_compressed_object_is_refused() {
        // restore_request_active forces ReadPlan down the Plain branch, so the
        // read yields STORED (compressed) bytes under the compressed size.
        assert_eq!(full_object_plaintext_len(&None, &restore_opts(), &compressed_object_info()), None);
    }

    #[test]
    fn restore_read_of_plain_object_is_refused() {
        assert_eq!(full_object_plaintext_len(&None, &restore_opts(), &plain_object_info()), None);
    }

    #[test]
    fn encrypted_object_is_refused() {
        assert_eq!(
            full_object_plaintext_len(&None, &ObjectOptions::default(), &encrypted_object_info()),
            None
        );
    }

    #[test]
    fn compressed_object_without_actual_size_is_refused() {
        // A compressed object whose actual size cannot be resolved must not be
        // served from cache: there is no length to publish as object_info.size.
        let mut user_defined = HashMap::new();
        user_defined.insert("x-rustfs-internal-compression".to_string(), "klauspost/compress/s2".to_string());
        let info = ObjectInfo {
            size: STORED_SIZE,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };
        assert_eq!(full_object_plaintext_len(&None, &ObjectOptions::default(), &info), None);
    }
}

#[cfg(test)]
mod body_cache_hook_e2e_tests {
    //! End-to-end regression coverage for the two merged P0 data-corruption
    //! fixes, driving the *real* `get_object_reader` gate + probe + reader
    //! construction rather than the `full_object_plaintext_len` predicate in
    //! isolation. The predicate-only tests above cannot catch a caller that
    //! opens a new shortcut serving the cached body directly — which was the
    //! original form of both P0s (backlog#1108 / #1109) and the reason ODC-21
    //! (backlog#1126) made the hook re-registrable so these tests can install a
    //! deterministic hook.
    //!
    //! A stand-in `GetObjectBodyCacheHook` plays the app-layer cache: the
    //! adapter itself lives above ecstore and cannot be reached from here, but
    //! the hook trait is exactly the production injection point, so exercising
    //! it through `get_object_reader` is a true end-to-end test of the ecstore
    //! side (gate decision -> probe -> served bytes and published size).
    //!
    //! The tests share one process-global hook slot, so they serialize under a
    //! single key and clear the slot on the way out; each uses a unique
    //! bucket/object and the hook returns `None` for anything else, so a stray
    //! concurrent GET in another test is unaffected.

    use crate::ecstore_validation_blackbox::make_local_set_disks;
    use crate::io_support::rio::{HashReader, compression_metadata_value, compression_reader};
    use crate::object_api::{
        GetObjectBodyCacheHook, GetObjectBodySource, ObjectInfo, ObjectOptions, PutObjReader, clear_get_object_body_cache_hook,
        register_get_object_body_cache_hook,
    };
    use crate::set_disk::SetDisks;
    use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::storage_api_contracts::object::ObjectIO as _;
    use bytes::Bytes;
    use http::HeaderMap;
    use rustfs_utils::CompressionAlgorithm;
    use rustfs_utils::http::{SUFFIX_ACTUAL_SIZE, SUFFIX_COMPRESSION, insert_str};
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt as _;

    /// A stand-in for the app-layer object-data cache: returns `body` only for
    /// the one primed key, mirroring how the real adapter answers a probe.
    struct PrimedBodyHook {
        bucket: String,
        object: String,
        body: Bytes,
    }

    #[async_trait::async_trait]
    impl GetObjectBodyCacheHook for PrimedBodyHook {
        async fn lookup(&self, bucket: &str, object: &str, _info: &ObjectInfo) -> Option<Bytes> {
            (bucket == self.bucket && object == self.object).then(|| self.body.clone())
        }
    }

    /// Registers a primed hook and clears it on drop so no hook leaks into an
    /// unrelated test sharing the process-global slot.
    struct HookGuard;
    impl HookGuard {
        fn install(bucket: &str, object: &str, body: Bytes) -> Self {
            register_get_object_body_cache_hook(Arc::new(PrimedBodyHook {
                bucket: bucket.to_string(),
                object: object.to_string(),
                body,
            }));
            HookGuard
        }
    }
    impl Drop for HookGuard {
        fn drop(&mut self) {
            clear_get_object_body_cache_hook();
        }
    }

    /// Compresses `plaintext` with the same codec the real PUT path uses, so
    /// the stored bytes round-trip through `get_object_reader`'s decompressor.
    async fn compress(plaintext: &[u8]) -> Vec<u8> {
        let mut reader = compression_reader(Cursor::new(plaintext.to_vec()), CompressionAlgorithm::default(), false);
        let mut compressed = Vec::new();
        reader.read_to_end(&mut compressed).await.expect("compress plaintext");
        compressed
    }

    /// Writes a genuinely compressed object: the stored data is `compressed`
    /// and the metadata marks it compressed with `plaintext.len()` as the
    /// decompressed length, exactly as the app-layer compress path records it.
    async fn put_compressed_object(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, plaintext: &[u8], compressed: &[u8]) {
        let mut user_defined = HashMap::new();
        insert_str(
            &mut user_defined,
            SUFFIX_COMPRESSION,
            compression_metadata_value(CompressionAlgorithm::default()),
        );
        insert_str(&mut user_defined, SUFFIX_ACTUAL_SIZE, plaintext.len().to_string());

        let opts = ObjectOptions {
            no_lock: true,
            user_defined,
            ..Default::default()
        };
        let stream = HashReader::from_stream(
            Cursor::new(compressed.to_vec()),
            compressed.len() as i64, // stored (compressed) size
            plaintext.len() as i64,  // actual (decompressed) size
            None,
            None,
            false,
        )
        .expect("hash reader over compressed bytes");
        let mut reader = PutObjReader::new(stream);
        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("compressed object should be written");
    }

    async fn read_to_vec(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, opts: &ObjectOptions) -> (Vec<u8>, i64) {
        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), opts)
            .await
            .expect("object reader should open");
        let published_size = reader.object_info.size;
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("object should stream");
        (body, published_size)
    }

    /// A compressible payload large enough to guarantee stored != plaintext and
    /// to keep the object off the inline / direct-memory fast paths.
    fn compressible_plaintext() -> Vec<u8> {
        b"rustfs-body-cache-e2e-regression-".repeat(20_000)
    }

    /// Writes a plain (uncompressed, unencrypted) object of `data`, large enough
    /// to keep it off the inline fast path so the cache hook is actually probed.
    async fn put_plain_object(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, data: &[u8]) {
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let stream = HashReader::from_stream(Cursor::new(data.to_vec()), data.len() as i64, data.len() as i64, None, None, false)
            .expect("hash reader over plain bytes");
        let mut reader = PutObjReader::new(stream);
        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("plain object should be written");
    }

    /// ODC-16: a cache-hook hit must mark the reader `HookServed` so the app
    /// layer serves the buffered body without a second lookup. A large plain
    /// object stays off the inline fast path, so the hook is genuinely probed.
    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn plain_cache_hit_marks_reader_hook_served() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "e2e-body-cache-hook-served";
        let object = "plain.bin";
        let payload = b"rustfs-hook-served-payload-".repeat(40_000);
        put_plain_object(&set_disks, bucket, object, &payload).await;

        let _guard = HookGuard::install(bucket, object, Bytes::from(payload.clone()));
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("object reader should open");

        assert_eq!(
            reader.body_source,
            GetObjectBodySource::HookServed,
            "a hook hit must mark the reader HookServed"
        );
        assert!(reader.buffered_body.is_some(), "a hook-served reader carries the cache body");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("object should stream");
        assert_eq!(body, payload, "the hook-served body must be the primed plaintext");
    }

    /// ODC-16: when the hook is registered but misses this object, the reader
    /// must be marked `HookMissed` so the app layer skips its now-redundant
    /// lookup (the hook's miss ran after fresh metadata resolution).
    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn plain_cache_miss_marks_reader_hook_missed() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "e2e-body-cache-hook-missed";
        let object = "plain.bin";
        let payload = b"rustfs-hook-missed-payload-".repeat(40_000);
        put_plain_object(&set_disks, bucket, object, &payload).await;

        // Register a hook primed for a DIFFERENT object, so this read is probed
        // (hook registered + eligible) but the probe misses.
        let _guard = HookGuard::install(bucket, "other-object", Bytes::from_static(b"unrelated"));
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("object reader should open");

        assert_eq!(
            reader.body_source,
            GetObjectBodySource::HookMissed,
            "a probed miss must mark the reader HookMissed"
        );
    }

    /// backlog#1108: a raw data-movement read (decommission/rebalance copy) must
    /// yield the STORED (compressed) representation, never the cached plaintext.
    /// Serving the cache here writes decompressed bytes into the destination
    /// pool under the compressed object's metadata — silent corruption.
    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn raw_data_movement_read_serves_stored_bytes_not_cached_plaintext() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "e2e-body-cache-raw-move";
        let object = "compressed.bin";
        let plaintext = compressible_plaintext();
        let compressed = compress(&plaintext).await;
        assert_ne!(compressed, plaintext, "fixture must be genuinely compressed");
        put_compressed_object(&set_disks, bucket, object, &plaintext, &compressed).await;

        // Prime the cache with the DECOMPRESSED plaintext.
        let _guard = HookGuard::install(bucket, object, Bytes::from(plaintext.clone()));

        // decommission_object_migration_read_opts sets both raw_data_movement_read
        // and data_movement; ReadPlan keys the stored-bytes path on the former.
        let opts = ObjectOptions {
            no_lock: true,
            raw_data_movement_read: true,
            ..Default::default()
        };
        let (body, published_size) = read_to_vec(&set_disks, bucket, object, &opts).await;

        assert_eq!(body, compressed, "raw data-movement read must return the stored compressed bytes");
        assert_ne!(body, plaintext, "raw data-movement read must NOT return the cached plaintext");
        assert_eq!(published_size, compressed.len() as i64, "raw read must publish the stored size");
    }

    /// backlog#1109: on a cache hit for a compressed object the served
    /// `object_info.size` must be the DECOMPRESSED length (what
    /// ReadTransform::Compressed publishes), and the streamed body length must
    /// match it. UploadPartCopy reads the copy length straight off this field.
    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn compressed_cache_hit_publishes_decompressed_size() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "e2e-body-cache-compressed";
        let object = "compressed.bin";
        let plaintext = compressible_plaintext();
        let compressed = compress(&plaintext).await;
        assert_ne!(compressed, plaintext, "fixture must be genuinely compressed");
        put_compressed_object(&set_disks, bucket, object, &plaintext, &compressed).await;

        let normal_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        // Control: with no hook the real decode path must decompress to the
        // plaintext and publish the decompressed length. This also proves the
        // fixture round-trips (stored compressed bytes -> plaintext).
        clear_get_object_body_cache_hook();
        let (control_body, control_size) = read_to_vec(&set_disks, bucket, object, &normal_opts).await;
        assert_eq!(control_body, plaintext, "real read must decompress to the plaintext");
        assert_eq!(control_size, plaintext.len() as i64, "real read must publish the decompressed size");

        // Hit: prime with the plaintext and read normally. The probe must serve
        // it AND republish the decompressed length, not the stored size.
        let _guard = HookGuard::install(bucket, object, Bytes::from(plaintext.clone()));
        let (hit_body, hit_size) = read_to_vec(&set_disks, bucket, object, &normal_opts).await;
        assert_eq!(
            hit_size,
            plaintext.len() as i64,
            "cache hit must publish the decompressed size (backlog#1109)"
        );
        assert_ne!(hit_size, compressed.len() as i64, "cache hit must not publish the stored compressed size");
        assert_eq!(hit_body.len() as i64, hit_size, "streamed length must equal the published size");
        assert_eq!(hit_body, plaintext, "cache hit must stream the plaintext");
    }

    /// backlog#1146: a restore read forces ReadPlan down the Plain branch, so a
    /// compressed object yields its STORED bytes under the compressed size. The
    /// cache (holding plaintext) must not be served.
    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn restore_read_serves_stored_bytes_not_cached_plaintext() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "e2e-body-cache-restore";
        let object = "compressed.bin";
        let plaintext = compressible_plaintext();
        let compressed = compress(&plaintext).await;
        assert_ne!(compressed, plaintext, "fixture must be genuinely compressed");
        put_compressed_object(&set_disks, bucket, object, &plaintext, &compressed).await;

        let _guard = HookGuard::install(bucket, object, Bytes::from(plaintext.clone()));

        let mut opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        opts.transition.restore_request.days = Some(1);
        let (body, published_size) = read_to_vec(&set_disks, bucket, object, &opts).await;

        assert_eq!(body, compressed, "restore read must return the stored compressed bytes");
        assert_ne!(body, plaintext, "restore read must NOT return the cached plaintext");
        assert_eq!(
            published_size,
            compressed.len() as i64,
            "restore read must publish the stored compressed size"
        );
    }
}

#[cfg(test)]
mod single_delete_namespace_owner_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks_isolated;
    use super::*;
    use crate::disk::ReadOptions;
    #[cfg(not(windows))]
    use crate::disk::STORAGE_FORMAT_FILE;
    use crate::object_api::WriteCompletion;
    #[cfg(not(windows))]
    use tokio::io::AsyncReadExt;

    async fn seed_version(set: &Arc<SetDisks>, bucket: &str, object: &str, version: Uuid, body: &[u8]) {
        set.put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(body.to_vec()),
            &ObjectOptions {
                versioned: true,
                version_id: Some(version.to_string()),
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("seed a complete real object version");
    }

    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn single_delete_advances_namespace_generation_through_cleanup() {
        let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "single-delete-namespace";
        let object = "last-version";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("fixture bucket");
        }
        let version = Uuid::new_v4();
        seed_version(&set, bucket, object, version, &vec![0x41; 256 * 1024]).await;
        let before = set.ctx.namespace_commit_generation();
        assert!(!set.ctx.namespace_commits_pending());
        let request = FileInfo {
            name: object.to_string(),
            version_id: Some(version),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        let result =
            tokio::time::timeout(Duration::from_secs(10), set.delete_object_version(bucket, object, &request, false)).await;
        if !matches!(result, Ok(Ok(()))) {
            let retained = dirs.into_iter().map(tempfile::TempDir::keep).collect::<Vec<_>>();
            panic!("single delete and cleanup did not finish: {result:?}; retained roots: {retained:?}");
        }
        for (disk, dir) in disks.iter().zip(&dirs) {
            let result = disk
                .read_version("", bucket, object, &version.to_string(), &ReadOptions::default())
                .await;
            assert!(matches!(result, Err(DiskError::FileNotFound | DiskError::FileVersionNotFound)));
            assert!(
                !dir.path().join(bucket).join(object).exists(),
                "immediate cleanup must remove the rollback object tree"
            );
        }
        assert!(!set.ctx.namespace_commits_pending());
        assert_eq!(
            set.ctx.namespace_commit_generation(),
            before + 2,
            "single delete must count one complete root lifetime"
        );
    }

    #[cfg(not(windows))]
    async fn assert_single_delete_physical_owner(case: &'static str) {
        use crate::disk::os::prepared_publication_test_hooks as hooks;
        use futures::FutureExt;

        temp_env::async_with_vars([(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("60"))], async {
            let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
            let bucket = "single-delete-physical-owner";
            let first = Uuid::new_v4();
            let second = Uuid::new_v4();
            let first_body = vec![0x51; 256 * 1024];
            let second_body = vec![0x62; 4096];
            let missing = case == "missing-marker";
            let rollback = case == "rollback";
            let last = case == "last-version";
            let cleanup = case == "immediate-cleanup";
            for disk in &disks {
                disk.make_volume(bucket).await.expect("fixture bucket");
            }
            if !missing {
                seed_version(&set, bucket, case, first, &first_body).await;
                if !last && !cleanup {
                    seed_version(&set, bucket, case, second, &second_body).await;
                }
            }
            let request = FileInfo {
                name: case.to_string(),
                version_id: Some(first),
                deleted: missing,
                mark_deleted: missing,
                mod_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            };
            let before = set.ctx.namespace_commit_generation();
            assert!(!set.ctx.namespace_commits_pending());
            let mut metadata_paths = Vec::new();
            let mut originals = Vec::new();
            let mut cleanup_sources = Vec::new();
            let mut cleanup_parts = Vec::new();
            for disk in &disks {
                let crate::disk::Disk::Local(local) = disk.as_ref() else {
                    panic!("local fixture required");
                };
                let path = local
                    .get_disk()
                    .get_object_path_for_io(bucket, case)
                    .expect("leased IO path")
                    .join(STORAGE_FORMAT_FILE);
                originals.push(if missing {
                    None
                } else {
                    Some(std::fs::read(&path).expect("seeded raw metadata"))
                });
                if cleanup {
                    let fi = disk.read_version("", bucket, case, &first.to_string(), &ReadOptions::default())
                        .await.expect("real non-inline data directory");
                    assert!(!fi.inline_data(), "cleanup fixture must have real shard files");
                    let data = path.parent().expect("object parent").join(fi.data_dir.expect("data directory").to_string());
                    cleanup_parts.push(std::fs::read(data.join("part.1")).expect("real pre-delete shard"));
                    cleanup_sources.push(data);
                }
                metadata_paths.push(path);
            }
            if rollback {
                // Two disks apply the real deletion and then error. Undo must
                // restore all four disks, including these post-apply failures.
                for disk in disks.iter().take(2) {
                    crate::disk::local::set_delete_version_fail_after_commit(disk.path().as_path(), case);
                }
            }
            let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
            let mut guards = Vec::new();
            let mut destination_guards = Vec::new();
            let later_guards = Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut releases = Vec::new();
            for (index, path) in metadata_paths.iter().enumerate() {
                let tx = entered_tx.clone();
                let (release, rx) = std::sync::mpsc::channel::<()>();
                if last || cleanup {
                    let source = if cleanup { &cleanup_sources[index] } else { path };
                    destination_guards.push(hooks::observe_rename_destination(source, move |destination| {
                        let _ = tx.send((index, destination.to_path_buf()));
                        let _ = rx.recv();
                    }));
                } else {
                    let hook_path = path.clone();
                    let path = path.clone();
                    let pause_path = path.clone();
                    let later_guards = Arc::clone(&later_guards);
                    guards.push(hooks::install_at(hooks::Stage::Rename, &hook_path, move || {
                        let pause = move || {
                            let _ = tx.send((index, pause_path));
                            let _ = rx.recv();
                        };
                        if rollback {
                            // This first callback precedes forward metadata publication.
                            // Arm only the subsequent real backup-restore rename.
                            let next = hooks::install_at(hooks::Stage::Rename, &path, pause);
                            later_guards.lock().expect("fixture hook guards").push(next);
                        } else {
                            pause();
                        }
                    }));
                }
                releases.push(release);
            }
            drop(entered_tx);
            let deleting_set = Arc::clone(&set);
            let mut delete =
                tokio::spawn(async move { deleting_set.delete_object_version(bucket, case, &request, missing).await });
            let mut joined = false;
            let mut counts = None;
            let mut physical_keys = std::collections::BTreeMap::new();
            let observations = std::panic::AssertUnwindSafe(async {
                tokio::time::timeout(Duration::from_secs(10), async {
                    while physical_keys.len() < 4 {
                        tokio::select! {
                            entry = entered_rx.recv() => {
                                let (index, key) = entry.expect("actual physical delete entry");
                                assert!(physical_keys.insert(index, key).is_none());
                            }
                            result = &mut delete => {
                                joined = true;
                                panic!("delete returned before physical entry: {result:?}");
                            }
                        }
                    }
                })
                .await
                .expect("all four physical mutations must enter");
                let pending_at_entry = set.ctx.namespace_commits_pending();
                let generation_at_entry = set.ctx.namespace_commit_generation();
                for (path, original) in metadata_paths.iter().zip(&originals) {
                    if missing {
                        assert!(!path.exists(), "missing marker must still be unpublished at entry");
                    } else if cleanup {
                        assert!(!path.exists(), "cleanup must follow the actual last-version deletion");
                    } else {
                        let bytes = std::fs::read(path).expect("paused metadata is readable");
                        let metadata = rustfs_filemeta::FileMeta::load(&bytes).expect("real metadata must parse");
                        if !last && !cleanup {
                            assert!(metadata.find_version(Some(second)).is_ok());
                        }
                        assert_eq!(
                            metadata.find_version(Some(first)).is_err(),
                            rollback,
                            "undo entry must follow actual deletion"
                        );
                        if !rollback {
                            assert_eq!(Some(&bytes), original.as_ref());
                        }
                    }
                }
                if rollback {
                    tokio::time::pause();
                    tokio::time::advance(Duration::from_secs(61)).await;
                    tokio::time::resume();
                    let result = tokio::time::timeout(Duration::from_secs(5), &mut delete).await;
                    joined = result.is_ok();
                    let result = result
                        .expect("ordinary undo deadlines must return")
                        .expect("delete coordinator must not panic");
                    assert!(
                        matches!(&result, Err(StorageError::InsufficientWriteQuorum(error_bucket, error_object)) if error_bucket == bucket && error_object == case),
                        "keep the original failed delete quorum: {result:?}"
                    );
                } else {
                    delete.abort();
                    let result = tokio::time::timeout(Duration::from_secs(5), &mut delete).await;
                    joined = result.is_ok();
                    assert!(
                        result
                            .expect("cancelled caller must join")
                            .expect_err("the caller must be cancelled")
                            .is_cancelled()
                    );
                }
                counts = Some((
                    pending_at_entry,
                    generation_at_entry,
                    set.ctx.namespace_commits_pending(),
                    set.ctx.namespace_commit_generation(),
                ));
                for path in physical_keys.values() {
                    assert!(
                        hooks::drain_namespace_key(path)
                            .now_or_never()
                            .is_none(),
                        "the physical metadata executor must still own its exact key"
                    );
                }
            })
            .catch_unwind()
            .await;

            drop(releases);
            drop(guards);
            drop(destination_guards);
            let coordinator_drained = joined || tokio::time::timeout(Duration::from_secs(10), &mut delete).await.is_ok();
            if !coordinator_drained {
                delete.abort();
                let _ = tokio::time::timeout(Duration::from_secs(5), &mut delete).await;
            }
            later_guards.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clear();
            let drains = futures::future::join_all(physical_keys.values().map(|path| {
                tokio::time::timeout(Duration::from_secs(5), hooks::drain_namespace_key(path))
            })).await;
            let owner_drained = tokio::time::timeout(Duration::from_secs(5), async {
                while set.ctx.namespace_commits_pending() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok();
            if physical_keys.len() != 4 || !coordinator_drained || !owner_drained || drains.iter().any(|result| result.is_err()) {
                let retained = dirs.into_iter().map(tempfile::TempDir::keep).collect::<Vec<_>>();
                eprintln!("single delete cleanup incomplete; retained roots: {retained:?}");
                if let Err(panic) = observations {
                    std::panic::resume_unwind(panic);
                }
                panic!("single delete physical cleanup did not drain");
            }
            if let Err(panic) = observations {
                std::panic::resume_unwind(panic);
            }
            for (index, (disk, (path, original))) in disks.iter().zip(metadata_paths.iter().zip(&originals)).enumerate() {
                if cleanup {
                    assert!(!path.exists(), "metadata must remain deleted after cleanup cancellation");
                    assert!(!cleanup_sources[index].exists(), "physical cleanup must remove the shard directory");
                    assert_eq!(
                        std::fs::read(physical_keys[&index].join("part.1")).expect("actual trashed shard"),
                        cleanup_parts[index],
                        "trash must contain the exact original shard"
                    );
                    continue;
                }
                if last {
                    assert!(!path.exists(), "late trash rename must remove the last metadata");
                    assert_eq!(
                        Some(std::fs::read(&physical_keys[&index]).expect("actual trash destination")),
                        *original,
                        "last-version trash must contain the exact old metadata"
                    );
                    continue;
                }
                let bytes = std::fs::read(path).expect("late metadata publication must finish");
                let metadata = rustfs_filemeta::FileMeta::load(&bytes).expect("final metadata must parse");
                if missing {
                    assert!(
                        metadata
                            .find_version(Some(first))
                            .expect("the marker must be published")
                            .1
                            .delete_marker
                            .is_some()
                    );
                } else {
                    assert!(metadata.find_version(Some(second)).is_ok());
                    assert_eq!(metadata.find_version(Some(first)).is_ok(), rollback);
                    if rollback {
                        assert_eq!(Some(&bytes), original.as_ref(), "physical undo must restore exact old metadata");
                    }
                    let fi = disk
                        .read_version("", bucket, case, &second.to_string(), &ReadOptions { read_data: true, ..Default::default() })
                        .await
                        .expect("remaining version");
                    if fi.inline_data() {
                        assert!(fi.data.as_ref().is_some_and(|data| !data.is_empty()), "remaining inline shard must survive");
                    } else {
                        let parts = disk.check_parts(bucket, case, &fi).await.expect("remaining shard check");
                        assert_eq!(parts.results, vec![crate::disk::CHECK_PART_SUCCESS; fi.parts.len()]);
                    }
                }
            }
            if !missing && !last && !cleanup {
                let mut actual = Vec::new();
                let read_opts = ObjectOptions {
                    version_id: Some(if rollback { first } else { second }.to_string()),
                    versioned: true,
                    ..Default::default()
                };
                let mut reader = tokio::time::timeout(
                    Duration::from_secs(5),
                    set.get_object_reader(bucket, case, None, HeaderMap::new(), &read_opts),
                )
                .await
                .expect("final GET must finish")
                .expect("the surviving version must be readable");
                tokio::time::timeout(Duration::from_secs(5), reader.stream.read_to_end(&mut actual))
                    .await
                    .expect("body must drain")
                    .expect("read surviving body");
                assert_eq!(actual, if rollback { first_body } else { second_body });
            }
            let (pending_at_entry, generation_at_entry, pending_after_return, generation_after_return) =
                counts.expect("complete observations");
            assert!(
                pending_at_entry && pending_after_return,
                "physical single delete outlived namespace accounting: {case}"
            );
            assert_eq!(generation_at_entry, before + 1);
            assert_eq!(generation_after_return, generation_at_entry);
            assert_eq!(set.ctx.namespace_commit_generation(), before + 2);
            assert!(!set.ctx.namespace_commits_pending());
        })
        .await;
    }

    #[cfg(not(windows))]
    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn single_delete_cancel_keeps_owner_until_immediate_data_cleanup() {
        assert_single_delete_physical_owner("immediate-cleanup").await;
    }

    #[cfg(not(windows))]
    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn single_delete_cancel_keeps_owner_until_last_version_trash() {
        assert_single_delete_physical_owner("last-version").await;
    }

    #[cfg(not(windows))]
    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn single_delete_cancel_keeps_owner_until_metadata_rewrite() {
        assert_single_delete_physical_owner("remaining-version").await;
    }

    #[cfg(not(windows))]
    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn single_delete_cancel_keeps_owner_until_missing_marker_publication() {
        assert_single_delete_physical_owner("missing-marker").await;
    }

    #[cfg(not(windows))]
    #[tokio::test]
    #[serial_test::serial(capacity_dirty_scope)]
    async fn single_delete_failed_quorum_keeps_owner_until_physical_undo() {
        assert_single_delete_physical_owner("rollback").await;
    }
}
