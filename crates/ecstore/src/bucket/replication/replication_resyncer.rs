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

use super::replication_bandwidth_boundary;
use super::replication_config_boundary::{ObjectOpts, ReplicationConfigurationExt as _};
use super::replication_config_store::ReplicationConfigStore;
use super::replication_error_boundary::{Error, Result, is_err_object_not_found, is_err_version_not_found};
use super::replication_event_sink::{EventArgs, send_event, send_local_event};
#[cfg(test)]
use super::replication_filemeta_boundary::ReplicationGenerationSnapshot;
use super::replication_filemeta_boundary::{
    REPLICATE_EXISTING, ReplicateDecision, ReplicateObjectInfo, ReplicatedInfos, ReplicatedTargetInfo, ReplicationAction,
    ReplicationState, ReplicationStatusType, ReplicationType, VersionPurgeStatusType, get_replication_state,
    parse_replicate_decision, replication_statuses_map, target_reset_header, version_purge_statuses_map,
};
use super::replication_lock_boundary::ReplicationLockTiming;
use super::replication_logging::{EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REPLICATION_RESYNC};
use super::replication_metadata_boundary::ReplicationMetadataStore;
#[cfg(test)]
use super::replication_msgp_boundary::ReplicationMsgpCodec;
use super::replication_object_config::{ReplicationConfig, get_replication_config, must_replicate};
use super::replication_object_decision_boundary::{
    MustReplicateOptions, ReplicationMultipartPartInput, delete_marker_purge_mrf_entry, delete_marker_purge_version_id,
    delete_replication_creates_marker, delete_replication_target_version_id, heal_uses_delete_replication_path,
    is_object_lock_denied_delete, is_retryable_delete_replication_head_error, is_version_delete_replication,
    replicate_delete_outcome, replication_etags_match, replication_multipart_complete_actual_size,
    replication_multipart_part_plan, replication_single_put_size_error, resync_existing_delete_replication_info,
    should_retry_delete_marker_purge, single_part_replica_etag_mismatch,
};
use super::replication_queue_boundary::{DeletedObjectReplicationInfo, ReplicationQueueAdmission};
use super::replication_resync_boundary::ResyncStatusType;
#[cfg(test)]
use super::replication_resync_boundary::should_count_head_proxy_failure;
use super::replication_resync_boundary::{
    BucketReplicationResyncStatus, ResyncOpts, TargetReplicationResyncStatus, decode_resync_file, encode_resync_file,
    is_version_id_mismatch, resync_state_accepts_update, resync_status_duration, sanitize_resync_error_detail,
    should_auto_resume_resync,
};
#[cfg(test)]
use super::replication_resync_boundary::{RESYNC_META_FORMAT, RESYNC_META_VERSION, WIRE_ZERO_TIME_UNIX};
use super::replication_storage_boundary::{
    AdvancedGetOptions, EcstoreObjectOperations, GetObjectReader, HTTPPreconditions, HTTPRangeSpec, ObjectInfo, ObjectOptions,
    ObjectToDelete, ReplicationObjectIO, ReplicationStatusWritebackCondition, ReplicationStatusWritebackMode, ReplicationStorage,
    StatObjectOptions, StorageObjectInfoOrErr, WalkOptions,
};
#[cfg(test)]
use super::replication_storage_boundary::{NamespaceLockFence, NamespaceLockSignalTestFence, ReplicationDeletedObject};
#[cfg(test)]
use super::replication_target_boundary::VersionIdentityCapability;
use super::replication_target_boundary::{
    ERR_REPLICATION_SSEC_PASSTHROUGH_UNSUPPORTED, HeadObjectSdkError, PutObjectOptions, PutObjectPartOptions,
    RemotePutObjectResponse, ReplicationTargetStore, S3ClientError, SsecPassthroughCapability, SsecPassthroughGate, TargetClient,
    is_replication_target_offline_error, replication_action_for_target_head, replication_complete_multipart_options,
    replication_delete_marker_purge_remove_options, replication_delete_remove_options, replication_force_delete_remove_options,
    replication_object_is_ssec_encrypted, replication_put_object_header_size, replication_put_object_options,
    replication_target_head_is_newer_null_version, resolve_read_api_version_id, ssec_passthrough_evidence_present,
    ssec_passthrough_gate, version_identity_capability_from_put, version_identity_drifted,
};
use super::replication_versioning_boundary::ReplicationVersioningStore;
use super::runtime_boundary as runtime_sources;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedPart;
use aws_smithy_types::body::SdkBody;
use futures::future::join_all;
use futures::stream::StreamExt;
use http::HeaderMap;
use http_body::Frame;
use http_body_util::StreamBody;
use metrics::counter;
#[cfg(test)]
use rmp_serde;
use rustfs_s3_types::EventName;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_TAGGING_DIRECTIVE, SUFFIX_REPLICATION_RESET, SUFFIX_REPLICATION_STATUS,
    has_internal_suffix, insert_str,
};
use rustfs_utils::{DEFAULT_SIP_HASH_KEY, get_env_usize, sip_hash};
#[cfg(test)]
use s3s::dto::ReplicationConfiguration;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::Instant;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncRead;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Duration as TokioDuration;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn};

const BACKGROUND_WALKDIR_TIMEOUT: TokioDuration = TokioDuration::from_secs(60);
const ENV_REPL_RESYNC_MAX_JOBS: &str = "RUSTFS_REPL_RESYNC_MAX_JOBS";
const DEFAULT_REPL_RESYNC_MAX_JOBS: usize = 2;
const MAX_REPL_RESYNC_MAX_JOBS: usize = 32;
const TARGET_CLIENT_UNAVAILABLE_ERROR: &str = "replication target client is unavailable";
use uuid::Uuid;

const EVENT_RESYNC_STATUS_UPDATE_SKIPPED: &str = "replication_resync_status_update_skipped";
const EVENT_RESYNC_OBJECT_PROCESSED: &str = "replication_resync_object_processed";
const EVENT_RESYNC_RUNTIME_SKIPPED: &str = "replication_resync_runtime_skipped";
const EVENT_REPLICATION_DELETE_SKIPPED: &str = "replication_delete_skipped";
const EVENT_REPLICATION_FORCE_DELETE_SKIPPED: &str = "replication_force_delete_skipped";
const EVENT_RESYNC_TASK_FAILED: &str = "replication_resync_task_failed";
const EVENT_RESYNC_TARGET_OPERATION_FAILED: &str = "replication_resync_target_operation_failed";
const EVENT_REPLICATION_ABORT_RETRY_RESOLVED: &str = "replication_abort_retry_resolved";
const EVENT_RESYNC_RUNTIME_CHANNEL_FAILED: &str = "replication_resync_runtime_channel_failed";
const EVENT_DELETE_MARKER_PURGE_FAILED: &str = "replication_delete_marker_purge_failed";
const EVENT_DELETE_MARKER_PURGE_MRF: &str = "replication_delete_marker_purge_mrf";
const METRIC_DELETE_MARKER_PURGE_TOTAL: &str = "rustfs_replication_delete_marker_purge_total";
const EVENT_REPLICATION_VERSION_IDENTITY_DRIFT: &str = "replication_version_identity_drift";
const EVENT_REPLICATION_DRIFTED_REPLICA_LOCATED: &str = "replication_drifted_replica_located";
const EVENT_REPLICATION_OBJECT_FAILED: &str = "replication_object_failed";
const EVENT_REPLICATION_PURGE_OBJECT_LOCK_DENIED: &str = "replication_purge_object_lock_denied";

#[allow(
    dead_code,
    reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
)]
const RESYNC_TIME_INTERVAL: TokioDuration = TokioDuration::from_secs(60);

static WARNED_MONITOR_UNINIT: std::sync::Once = std::sync::Once::new();

fn resync_target_error_detail<E, R>(error: &SdkError<E, R>) -> Option<String>
where
    E: ProvideErrorMetadata,
{
    sanitize_resync_error_detail(error.code().unwrap_or(match error {
        SdkError::ConstructionFailure(_) => "failed to construct target request",
        SdkError::TimeoutError(_) => "target request timed out",
        SdkError::DispatchFailure(_) => "target dispatch failed",
        SdkError::ResponseError(_) => "invalid target response",
        SdkError::ServiceError(_) => "target service error",
        _ => "target request failed",
    }))
}

async fn finish_resync_workers(
    worker_txs: Vec<tokio::sync::mpsc::Sender<ReplicateObjectInfo>>,
    results_tx: tokio::sync::mpsc::Sender<TargetReplicationResyncStatus>,
    futures: Vec<JoinHandle<()>>,
    abort: bool,
) -> bool {
    drop(worker_txs);
    drop(results_tx);

    if abort {
        for future in &futures {
            future.abort();
        }
    }

    let mut failed = false;
    for result in join_all(futures).await {
        if let Err(err) = result
            && !(abort && err.is_cancelled())
        {
            failed = true;
            error!(
                event = EVENT_RESYNC_TASK_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                error = %err,
                "Replication resync task failed"
            );
        }
    }

    failed
}

fn has_raw_status(err: &SdkError<HeadObjectError>, status: u16) -> bool {
    err.raw_response().is_some_and(|r| r.status().as_u16() == status)
}

fn metadata_requires_existing_target(op_type: ReplicationType, object_info: &ObjectInfo) -> bool {
    op_type == ReplicationType::Metadata
        && object_info
            .user_defined
            .get(AMZ_BUCKET_REPLICATION_STATUS)
            .is_some_and(|status| status.eq_ignore_ascii_case(ReplicationStatusType::Replica.as_str()))
}

const METRIC_VERSION_IDENTITY_DRIFT_TOTAL: &str = "rustfs_replication_version_identity_drift_total";

/// How long a target stays quiet after reporting version-identity drift.
///
/// This used to be a plain "once per ARN per process": one line ever, which on
/// a long-lived server meant the single most important diagnostic for a
/// non-converging generic S3 target scrolled away hours before anyone looked
/// (rustfs#6822). Re-arming on an interval keeps the log bounded while leaving
/// the condition discoverable in any recent window.
const VERSION_IDENTITY_DRIFT_LOG_INTERVAL: TokioDuration = TokioDuration::from_secs(600);

/// When each target last reported version-identity drift, by ARN. Throttling is
/// advisory only — the metric still counts every drifting PUT.
static VERSION_IDENTITY_WARNED_ARNS: LazyLock<StdMutex<HashMap<String, Instant>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

/// Version purges the peer denied under object lock (#6850). A RustFS peer
/// with the replicated-purge GOVERNANCE exemption
/// (`replication_delete_may_bypass_governance`) no longer produces this for
/// governance retention, but COMPLIANCE retention, legal hold, and targets
/// without the exemption (older RustFS, MinIO, generic S3) still deny — and
/// such a purge cannot succeed until the lock on the replica lapses, so
/// retrying every heal cycle only burns bandwidth and failure counters.
/// Entries suppress heal requeues for the backoff window; after it expires
/// one probe runs again, so the purge still converges on its own once
/// retention ends. In-process only: a restart costs at most one extra probe
/// per entry.
const OBJECT_LOCK_DENIED_PURGE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(60 * 60);
const OBJECT_LOCK_DENIED_PURGE_CACHE_MAX: usize = 4096;
type ObjectLockDeniedPurgeKey = (String, String, String);

struct ObjectLockDeniedPurge {
    denied_at: std::time::Instant,
    denied_arns: HashSet<String>,
}

static OBJECT_LOCK_DENIED_PURGES: LazyLock<StdMutex<HashMap<ObjectLockDeniedPurgeKey, ObjectLockDeniedPurge>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

fn object_lock_denied_purge_key(dobj: &DeletedObjectReplicationInfo) -> ObjectLockDeniedPurgeKey {
    let version_id = dobj
        .delete_object
        .delete_marker_version_id
        .or(dobj.delete_object.version_id)
        .unwrap_or_default();
    (dobj.bucket.clone(), dobj.delete_object.object_name.clone(), version_id.to_string())
}

fn record_object_lock_denied_purge(dobj: &DeletedObjectReplicationInfo, arn: &str) {
    let mut denied = OBJECT_LOCK_DENIED_PURGES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if denied.len() >= OBJECT_LOCK_DENIED_PURGE_CACHE_MAX {
        denied.retain(|_, entry| entry.denied_at.elapsed() < OBJECT_LOCK_DENIED_PURGE_BACKOFF);
    }
    let key = object_lock_denied_purge_key(dobj);
    if denied.len() < OBJECT_LOCK_DENIED_PURGE_CACHE_MAX || denied.contains_key(&key) {
        let entry = denied.entry(key).or_insert_with(|| ObjectLockDeniedPurge {
            denied_at: std::time::Instant::now(),
            denied_arns: HashSet::new(),
        });
        entry.denied_at = std::time::Instant::now();
        entry.denied_arns.insert(arn.to_string());
    }
    // Still full after dropping expired entries: skip recording — the purge
    // then simply keeps retrying, which is the pre-#6850 behavior.
}

/// Whether a heal requeue of this delete can only reach targets that denied
/// it under object lock within the backoff window. A target the entry does
/// not cover (another peer, or one whose denial expired) keeps the requeue
/// flowing — suppressing it would delay a purge that could succeed there.
pub(crate) fn object_lock_denied_purge_backoff_active(dobj: &DeletedObjectReplicationInfo) -> bool {
    let key = object_lock_denied_purge_key(dobj);
    let mut denied = OBJECT_LOCK_DENIED_PURGES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match denied.get(&key) {
        Some(entry) if entry.denied_at.elapsed() < OBJECT_LOCK_DENIED_PURGE_BACKOFF => {
            let admitted = dobj.admitted_target_arns();
            !admitted.is_empty() && admitted.iter().all(|arn| entry.denied_arns.contains(arn))
        }
        Some(_) => {
            denied.remove(&key);
            false
        }
        None => false,
    }
}

const REPLICA_ETAG_VERIFY_ENV: &str = "RUSTFS_REPLICATION_REPLICA_ETAG_VERIFY";

/// Escape hatch for a target whose 32-hex ETags are legitimately not the
/// content MD5 (e.g. a gateway hashing its own ciphertext without announcing
/// SSE in the response) — such a target would otherwise fail every object.
fn replica_etag_verification_enabled() -> bool {
    std::env::var(REPLICA_ETAG_VERIFY_ENV)
        .map(|v| !(v.eq_ignore_ascii_case("false") || v == "0"))
        .unwrap_or(true)
}

/// A 200 from the target is not proof the replica holds the source bytes: a
/// target that stores a transformed payload (e.g. undecoded `aws-chunked`
/// frames, #6853) returns the ETag of what it actually wrote. Reporting
/// COMPLETED over such a replica is silent corruption, so a decidable
/// mismatch fails the replication instead. An SSE-C ciphertext passthrough
/// transfer is exempt: the wire bytes are ciphertext while the source ETag is
/// the plaintext MD5, and that path has its own HEAD-back audit.
fn verify_single_part_replica(
    object_info: &ObjectInfo,
    response: &RemotePutObjectResponse,
    ciphertext_passthrough: bool,
) -> std::result::Result<(), std::io::Error> {
    if ciphertext_passthrough || !replica_etag_verification_enabled() {
        return Ok(());
    }
    if single_part_replica_etag_mismatch(object_info.etag.as_deref(), response.etag.as_deref()) {
        // The differing ETags go into the structured log; the error message
        // stays constant so same-cause failures bucket together downstream.
        warn!(
            event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            bucket = %object_info.bucket,
            object = %object_info.name,
            source_etag = ?object_info.etag,
            replica_etag = ?response.etag,
            operation = "verify_replica_etag",
            "Replication target operation failed"
        );
        return Err(std::io::Error::other(REPLICA_ETAG_MISMATCH_ERROR));
    }
    Ok(())
}

const REPLICA_ETAG_MISMATCH_ERROR: &str = "replica etag mismatch: the target persisted different bytes than were sent";

fn audit_target_version_identity(tgt_client: &TargetClient, source_version_id: &str, assigned_version_id: Option<&str>) {
    // Every write refreshes the cached verdict, so the convergence fallback
    // below (`replica_head_fallback`) knows whether a 404 on a
    // version-addressed HEAD can mean "replica missing" on this target.
    if let Some(capability) = version_identity_capability_from_put(source_version_id, assigned_version_id) {
        ReplicationTargetStore::record_version_identity_capability(&tgt_client.arn, capability);
    }
    if !version_identity_drifted(source_version_id, assigned_version_id) {
        return;
    }
    counter!(METRIC_VERSION_IDENTITY_DRIFT_TOTAL).increment(1);
    if !version_identity_drift_log_due(&tgt_client.arn, Instant::now()) {
        return;
    }
    // `error`, not `warn`: the target silently refuses the addressing scheme
    // every version-addressed delete and heal on it depends on, so replication
    // to it can never converge. At `warn` this sat below `DEFAULT_LOG_LEVEL`
    // and no default deployment ever saw the one line that explains why a
    // purged version is still on the target (rustfs#6822).
    error!(
        event = EVENT_REPLICATION_VERSION_IDENTITY_DRIFT,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
        arn = %tgt_client.arn,
        endpoint = %tgt_client.endpoint,
        sent_version_id = %source_version_id,
        assigned_version_id = assigned_version_id.unwrap_or("<none>"),
        "Replication target does not adopt source version ids; version-addressed replication cannot converge (run ?replication-check for details)"
    );
}

/// Whether this ARN's version-identity drift is due to be logged again at
/// `now`, re-arming the throttle when it is. Split out from the audit so the
/// interval policy is testable without a target client.
fn version_identity_drift_log_due(arn: &str, now: Instant) -> bool {
    let mut warned = VERSION_IDENTITY_WARNED_ARNS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match warned.get(arn) {
        Some(last) if now.duration_since(*last) < VERSION_IDENTITY_DRIFT_LOG_INTERVAL => false,
        _ => {
            warned.insert(arn.to_string(), now);
            true
        }
    }
}

/// HEAD against a replication target on behalf of the replication worker
/// (resync/heal/delete convergence checks). This is NOT a client read proxy:
/// it must not touch the proxy metrics — those count only real GET/HEAD/
/// Tagging requests proxied for clients (see `replication_proxy.rs` /
/// `TargetClient::head_object_for_proxy`).
async fn head_object_for_worker(
    target_client: &TargetClient,
    target_bucket: &str,
    object: &str,
    version_id: Option<String>,
) -> std::result::Result<HeadObjectOutput, HeadObjectSdkError> {
    target_client.head_object(target_bucket, object, version_id).await
}

fn is_version_id_format_mismatch(err: &SdkError<HeadObjectError>) -> bool {
    let code = err.as_service_error().and_then(|se| se.code());
    let raw_status = err.raw_response().map(|r| r.status().as_u16());
    is_version_id_mismatch(code, raw_status)
}

async fn mark_replication_target_offline_if_needed(target_client: &Arc<TargetClient>, err: &(impl Display + ?Sized)) {
    if is_replication_target_offline_error(err) {
        ReplicationTargetStore::mark_target_offline(target_client).await;
    }
}

async fn head_object_fallback(
    tgt_client: &TargetClient,
    object: &str,
) -> std::result::Result<Option<HeadObjectOutput>, HeadObjectSdkError> {
    match head_object_for_worker(tgt_client, &tgt_client.bucket, object, None).await {
        Ok(oi) => Ok(Some(oi)),
        Err(e) if head_object_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

fn head_object_not_found(err: &HeadObjectSdkError) -> bool {
    err.as_service_error().is_some_and(|se| se.is_not_found()) || has_raw_status(err, 404)
}

/// Second look at a replica whose version-addressed HEAD failed, for the two
/// target shapes where that failure is not a verdict on the replica:
///
/// - AWS-style 400/403 (the RustFS uuid is rejected as malformed): HEAD the
///   current version without a version id; callers compare ETags.
/// - 404 on a target known to mint its own version ids (the Wasabi shape,
///   rustfs/backlog#2340): the source id never existed there, so locate the
///   replica by exact key and ETag through ListObjectVersions and HEAD the id
///   the target assigned. Without this, every heal, MRF retry and
///   existing-object resync re-drive PUTs the object again and mints one
///   more target version.
///
/// `None` when the error stands as-is: a real miss on an adopting target, or
/// a target whose identity contract is still unknown. A failed lookup is
/// returned as a HEAD-shaped error so callers keep their "target operation
/// failed" handling (retry later) instead of re-driving the PUT.
async fn replica_head_fallback(
    tgt_client: &TargetClient,
    object: &str,
    source_etag: Option<&str>,
    err: &HeadObjectSdkError,
) -> Option<std::result::Result<Option<HeadObjectOutput>, HeadObjectSdkError>> {
    if is_version_id_format_mismatch(err) {
        return Some(head_object_fallback(tgt_client, object).await);
    }
    if !head_object_not_found(err)
        || !ReplicationTargetStore::version_identity_capability(&tgt_client.arn).version_addressing_unreliable()
    {
        return None;
    }
    let etag = source_etag.filter(|etag| !etag.trim().is_empty())?;
    Some(match tgt_client.find_version_by_etag(&tgt_client.bucket, object, etag).await {
        Ok(Some(assigned_version_id)) => {
            debug!(
                event = EVENT_REPLICATION_DRIFTED_REPLICA_LOCATED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %tgt_client.bucket,
                object = %object,
                arn = %tgt_client.arn,
                assigned_version_id = %assigned_version_id,
                "Located replica by content identity on a target that mints its own version ids"
            );
            match head_object_for_worker(tgt_client, &tgt_client.bucket, object, Some(assigned_version_id)).await {
                Ok(oi) => Ok(Some(oi)),
                // The located version disappeared between LIST and HEAD.
                Err(e) if head_object_not_found(&e) => Ok(None),
                Err(e) => Err(e),
            }
        }
        Ok(None) => Ok(None),
        Err(list_err) => Err(Box::new(SdkError::construction_failure(*list_err))),
    })
}

/// Resolve the N2 fail-closed gate for an SSE-C passthrough attempt against
/// this target. Returns `Some(audit_required)` when replication may proceed;
/// on a freshly-flagged header-dropping target it settles `rinfo` as FAILED
/// (no PUT is ever sent — the object stays on the normal MRF retry channel
/// and re-audits once the verdict's TTL expires or replication-check
/// re-probes the target) and returns `None`.
async fn resolve_ssec_passthrough_gate(
    ssec: bool,
    tgt_client: &TargetClient,
    bucket: &str,
    object: &str,
    rinfo: &mut ReplicatedTargetInfo,
) -> Option<bool> {
    let (capability, expired) = ReplicationTargetStore::ssec_passthrough_capability(&tgt_client.arn).await;
    match ssec_passthrough_gate(ssec, capability, expired) {
        SsecPassthroughGate::Proceed => Some(false),
        SsecPassthroughGate::ProceedWithAudit => Some(true),
        SsecPassthroughGate::FailClosed => {
            rinfo.replication_status = ReplicationStatusType::Failed;
            rinfo.error = Some(ERR_REPLICATION_SSEC_PASSTHROUGH_UNSUPPORTED.to_string());
            warn!(
                event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object,
                arn = %tgt_client.arn,
                operation = "ssec_passthrough_gate",
                error = ERR_REPLICATION_SSEC_PASSTHROUGH_UNSUPPORTED,
                "Replication target operation failed"
            );
            None
        }
    }
}

/// Judge SSE-C passthrough evidence on a HEAD of the replica and record the
/// capability verdict for the target. Returns true when the SSE-C material
/// provably survived; otherwise records `Unsupported` and settles `rinfo` as
/// FAILED so the attempt never reports a silently unreadable COMPLETED.
async fn settle_ssec_passthrough_evidence(
    head: &HeadObjectOutput,
    tgt_client: &TargetClient,
    bucket: &str,
    object: &str,
    rinfo: &mut ReplicatedTargetInfo,
) -> bool {
    if ssec_passthrough_evidence_present(head) {
        ReplicationTargetStore::record_ssec_passthrough_capability(&tgt_client.arn, SsecPassthroughCapability::Supported).await;
        return true;
    }
    ReplicationTargetStore::record_ssec_passthrough_capability(&tgt_client.arn, SsecPassthroughCapability::Unsupported).await;
    rinfo.replication_status = ReplicationStatusType::Failed;
    rinfo.error = Some(ERR_REPLICATION_SSEC_PASSTHROUGH_UNSUPPORTED.to_string());
    warn!(
        event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
        bucket = %bucket,
        object = %object,
        arn = %tgt_client.arn,
        endpoint = %tgt_client.endpoint,
        operation = "ssec_passthrough_audit",
        error = ERR_REPLICATION_SSEC_PASSTHROUGH_UNSUPPORTED,
        "Replication target operation failed"
    );
    false
}

/// Post-PUT HEAD-back audit for an SSE-C passthrough replica, over the worker
/// HEAD channel (replication-check exemption plus the `source-proxy-request:
/// false` suppression header, so the target answers locally without a
/// customer key). A HEAD transport failure leaves the capability `Unknown`
/// but still fails this attempt: an unverifiable SSE-C replica must not
/// report COMPLETED.
async fn audit_ssec_passthrough_replica(
    tgt_client: &Arc<TargetClient>,
    bucket: &str,
    object: &str,
    version_id: Option<String>,
    rinfo: &mut ReplicatedTargetInfo,
) -> bool {
    // Address the replica the way the PUT named it: a nil source version id
    // (versioning-suspended / null-version objects) maps to the "null"
    // version, so the audit HEAD does not 4xx-loop on those objects.
    let version_id = resolve_read_api_version_id(version_id);
    match head_object_for_worker(tgt_client.as_ref(), &tgt_client.bucket, object, version_id).await {
        Ok(head) => settle_ssec_passthrough_evidence(&head, tgt_client, bucket, object, rinfo).await,
        Err(e) => {
            rinfo.replication_status = ReplicationStatusType::Failed;
            rinfo.error = Some(format!("SSE-C passthrough audit HEAD failed: {e}"));
            warn!(
                event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object,
                arn = %tgt_client.arn,
                operation = "ssec_passthrough_audit_head",
                error = %e,
                "Replication target operation failed"
            );
            mark_replication_target_offline_if_needed(tgt_client, &e).await;
            false
        }
    }
}

static RESYNC_WORKER_COUNT: usize = 10;

type ResyncCancelKey = (String, String, String);

fn configured_resync_max_jobs() -> usize {
    bounded_resync_max_jobs(get_env_usize(ENV_REPL_RESYNC_MAX_JOBS, DEFAULT_REPL_RESYNC_MAX_JOBS))
}

fn bounded_resync_max_jobs(value: usize) -> usize {
    value.clamp(1, MAX_REPL_RESYNC_MAX_JOBS)
}

#[derive(Debug)]
pub struct ReplicationResyncer {
    pub status_map: Arc<RwLock<HashMap<String, BucketReplicationResyncStatus>>>,
    #[allow(
        dead_code,
        reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
    )]
    pub worker_size: usize,
    pub(crate) cancel_tokens: Arc<RwLock<HashMap<ResyncCancelKey, CancellationToken>>>,
    resync_admission: Arc<Semaphore>,
}

impl ReplicationResyncer {
    pub async fn new() -> Self {
        Self {
            status_map: Arc::new(RwLock::new(HashMap::new())),
            worker_size: RESYNC_WORKER_COUNT,
            cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
            resync_admission: Arc::new(Semaphore::new(configured_resync_max_jobs())),
        }
    }

    async fn acquire_resync_admission(&self, cancellation_token: &CancellationToken) -> Option<OwnedSemaphorePermit> {
        tokio::select! {
            permit = self.resync_admission.clone().acquire_owned() => permit.ok(),
            _ = cancellation_token.cancelled() => None,
        }
    }

    fn cancel_key(opts: &ResyncOpts) -> ResyncCancelKey {
        (opts.bucket.clone(), opts.arn.clone(), opts.resync_id.clone())
    }

    pub async fn register_cancel_token(&self, opts: &ResyncOpts, token: CancellationToken) -> bool {
        let mut cancel_tokens = self.cancel_tokens.write().await;
        match cancel_tokens.entry(Self::cancel_key(opts)) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(token);
                true
            }
            std::collections::hash_map::Entry::Occupied(_) => false,
        }
    }

    pub async fn clear_cancel_token(&self, opts: &ResyncOpts) {
        self.cancel_tokens.write().await.remove(&Self::cancel_key(opts));
    }

    pub async fn cancel(&self, opts: &ResyncOpts) {
        if let Some(token) = self.cancel_tokens.write().await.remove(&Self::cancel_key(opts)) {
            token.cancel();
        }
    }

    pub async fn mark_status<S>(&self, status: ResyncStatusType, opts: ResyncOpts, obj_layer: Arc<S>) -> Result<()>
    where
        S: ReplicationObjectIO,
    {
        let (updated_target, status_duration) = {
            let mut status_map = self.status_map.write().await;
            let now = OffsetDateTime::now_utc();

            let bucket_status = if let Some(bucket_status) = status_map.get_mut(&opts.bucket) {
                bucket_status
            } else {
                let mut bucket_status = BucketReplicationResyncStatus::new();
                bucket_status.id = 0;
                status_map.insert(opts.bucket.clone(), bucket_status);
                status_map.get_mut(&opts.bucket).expect("bucket should be in status map")
            };

            let state = if let Some(state) = bucket_status.targets_map.get_mut(&opts.arn) {
                state
            } else {
                let state = TargetReplicationResyncStatus::new();
                bucket_status.targets_map.insert(opts.arn.clone(), state);
                bucket_status
                    .targets_map
                    .get_mut(&opts.arn)
                    .expect("ARN should be in targets map")
            };

            if !resync_state_accepts_update(state, &opts) {
                debug!(
                    event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %opts.bucket,
                    arn = %opts.arn,
                    incoming_resync_id = %opts.resync_id,
                    current_resync_id = %state.resync_id,
                    reason = "stale_status_update",
                    "Skipped stale resync status update"
                );
                return Ok(());
            }

            if state.resync_status == ResyncStatusType::ResyncCanceled && status != ResyncStatusType::ResyncCanceled {
                debug!(
                    event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %opts.bucket,
                    arn = %opts.arn,
                    incoming_status = %status,
                    reason = "canceled_status_is_terminal",
                    "Skipped resync status update after cancellation"
                );
                return Ok(());
            }

            if state.resync_id.is_empty() {
                state.resync_id = opts.resync_id.clone();
            }
            if state.resync_before_date.is_none() {
                state.resync_before_date = opts.resync_before;
            }
            if state.bucket.is_empty() {
                state.bucket = opts.bucket.clone();
            }
            if status == ResyncStatusType::ResyncStarted && state.start_time.is_none() {
                state.start_time = Some(now);
            }
            state.resync_status = status;
            state.last_update = Some(now);
            let status_duration = resync_status_duration(status, state.start_time, now);

            bucket_status.last_update = Some(now);

            (state.clone(), status_duration)
        };

        // Persist through the CAS so a stale cached map can never clobber
        // states other nodes finalized for other targets; re-run the staleness
        // and canceled-is-terminal guards against the freshest persisted entry.
        let updated_last_update = updated_target.last_update;
        let (final_map, saved) = update_resync_status_cas(&opts.bucket, obj_layer, |persisted| {
            if let Some(current) = persisted.targets_map.get(&opts.arn) {
                if !resync_state_accepts_update(current, &opts) {
                    debug!(
                        event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        bucket = %opts.bucket,
                        arn = %opts.arn,
                        incoming_resync_id = %opts.resync_id,
                        current_resync_id = %current.resync_id,
                        reason = "stale_status_update",
                        "Skipped persisting stale resync status update"
                    );
                    return Ok(false);
                }
                if current.resync_status == ResyncStatusType::ResyncCanceled && status != ResyncStatusType::ResyncCanceled {
                    debug!(
                        event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        bucket = %opts.bucket,
                        arn = %opts.arn,
                        incoming_status = %status,
                        reason = "canceled_status_is_terminal",
                        "Skipped resync status update after cancellation"
                    );
                    return Ok(false);
                }
            }
            persisted.targets_map.insert(opts.arn.clone(), updated_target.clone());
            persisted.last_update = updated_last_update;
            Ok(true)
        })
        .await?;

        // Converge this target's cached entry with what the persisted document
        // decided (our update, or the newer/terminal state that outranked it).
        {
            let mut status_map = self.status_map.write().await;
            if let Some(cached) = status_map.get_mut(&opts.bucket)
                && let Some(final_target) = final_map.targets_map.get(&opts.arn)
            {
                cached.targets_map.insert(opts.arn.clone(), final_target.clone());
                cached.last_update = final_map.last_update.or(cached.last_update);
            }
        }

        if saved && let Some(stats) = runtime_sources::replication_stats() {
            stats.record_resync_status(&opts.bucket, status, status_duration).await;
        }

        Ok(())
    }

    pub async fn inc_stats(&self, status: &TargetReplicationResyncStatus, opts: ResyncOpts) {
        let mut status_map = self.status_map.write().await;
        let now = OffsetDateTime::now_utc();

        let bucket_status = if let Some(bucket_status) = status_map.get_mut(&opts.bucket) {
            bucket_status
        } else {
            let mut bucket_status = BucketReplicationResyncStatus::new();
            bucket_status.id = 0;
            status_map.insert(opts.bucket.clone(), bucket_status);
            status_map.get_mut(&opts.bucket).expect("bucket should be in status map")
        };

        let state = if let Some(state) = bucket_status.targets_map.get_mut(&opts.arn) {
            state
        } else {
            let state = TargetReplicationResyncStatus::new();
            bucket_status.targets_map.insert(opts.arn.clone(), state);
            bucket_status
                .targets_map
                .get_mut(&opts.arn)
                .expect("ARN should be in targets map")
        };

        if !resync_state_accepts_update(state, &opts) {
            debug!(
                event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %opts.bucket,
                arn = %opts.arn,
                incoming_resync_id = %opts.resync_id,
                current_resync_id = %state.resync_id,
                reason = "stale_stats_update",
                "Skipped stale resync stats update"
            );
            return;
        }

        if state.resync_id.is_empty() {
            state.resync_id = opts.resync_id.clone();
        }
        if state.bucket.is_empty() {
            state.bucket = opts.bucket.clone();
        }
        state.object = status.object.clone();
        state.replicated_count += status.replicated_count;
        state.replicated_size += status.replicated_size;
        state.failed_count += status.failed_count;
        state.failed_size += status.failed_size;
        if state.error.is_none() && status.failed_count > 0 {
            state.error = status.error.as_deref().and_then(sanitize_resync_error_detail);
        }
        state.last_update = Some(now);
        bucket_status.last_update = Some(now);
    }

    async fn target_has_resync_failures(&self, opts: &ResyncOpts) -> bool {
        self.status_map
            .read()
            .await
            .get(&opts.bucket)
            .and_then(|status| status.targets_map.get(&opts.arn))
            .is_some_and(|status| status.failed_count > 0)
    }

    #[allow(
        dead_code,
        reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
    )]
    pub async fn persist_to_disk<S>(&self, cancel_token: CancellationToken, api: Arc<S>)
    where
        S: ReplicationObjectIO,
    {
        let mut interval = tokio::time::interval(RESYNC_TIME_INTERVAL);

        let mut last_update_times = HashMap::new();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return;
                }
                _ = interval.tick() => {

                    let snapshot: Vec<(String, BucketReplicationResyncStatus)> = self
                        .status_map
                        .read()
                        .await
                        .iter()
                        .map(|(bucket, status)| (bucket.clone(), status.clone()))
                        .collect();

                    let mut update = false;
                    for (bucket, status) in &snapshot {
                        for target in status.targets_map.values() {
                            if target.last_update.is_none() {
                                update = true;
                                break;
                            }
                        }



                        if let Some(last_update) = status.last_update
                            && last_update > *last_update_times.get(bucket).unwrap_or(&OffsetDateTime::UNIX_EPOCH) {
                                update = true;
                            }

                        if update {
                            // CAS-merge instead of a blind whole-map save: this
                            // cache may lag other nodes' admissions and
                            // cancellations, which must not be overwritten.
                            let result = update_resync_status_cas(bucket, api.clone(), |persisted| {
                                Ok(merge_local_resync_into_persisted(persisted, status))
                            })
                            .await;
                            if let Err(err) = result {
                                error!(
                                    event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                                    bucket = %bucket,
                                    reason = "persist_failed",
                                    error = %err,
                                    "Failed to persist resync status"
                                );
                            } else if let Some(last_update) = status.last_update {
                                last_update_times.insert(bucket.clone(), last_update);
                            }
                        }
                    }

                   interval.reset();
                }
            }
        }
    }

    async fn resync_bucket_mark_status<S: ReplicationObjectIO>(
        &self,
        status: ResyncStatusType,
        opts: ResyncOpts,
        storage: Arc<S>,
    ) {
        if let Err(err) = self.mark_status(status, opts.clone(), storage.clone()).await {
            error!(
                event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %opts.bucket,
                arn = %opts.arn,
                reason = "mark_status_failed",
                error = %err,
                "Failed to update resync status"
            );
        }
    }

    /// Acquire a cluster-wide leader lock for this (bucket, ARN) pair so that only
    /// one node runs the resync scan at a time. Without this, every cluster node would
    /// scan and replicate every object independently, causing N-fold duplicate traffic.
    async fn acquire_resync_leader_lock<S: ReplicationStorage>(
        storage: &Arc<S>,
        opts: &ResyncOpts,
    ) -> Option<rustfs_lock::NamespaceLockGuard> {
        let resync_lock_key = ReplicationMetadataStore::resync_lock_key(&opts.bucket, &opts.arn);
        let resync_ns_lock = match storage
            .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), &resync_lock_key)
            .await
        {
            Ok(l) => l,
            Err(e) => {
                warn!(
                    event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %opts.bucket,
                    arn = %opts.arn,
                    error = %e,
                    reason = "leader_lock_create_failed",
                    "Failed to create resync leader lock — skipping resync"
                );
                return None;
            }
        };
        match resync_ns_lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
            Ok(g) => Some(g),
            Err(_) => {
                debug!(
                    event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %opts.bucket,
                    arn = %opts.arn,
                    reason = "leader_lock_held_by_another_node",
                    "Another node is already running resync for this bucket/ARN — skipping"
                );
                None
            }
        }
    }

    /// Resolve and validate the replication config plus the single remote target
    /// client this resync run replicates to, marking the resync failed (and
    /// returning `None`) when any lookup or validation step does not hold.
    async fn resolve_resync_target<S: ReplicationObjectIO>(
        &self,
        opts: &ResyncOpts,
        storage: &Arc<S>,
    ) -> Option<(ReplicationConfig, Arc<TargetClient>)> {
        let cfg = match get_replication_config(&opts.bucket).await {
            Ok(cfg) => cfg,
            Err(err) => {
                error!(
                    event = EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %opts.bucket,
                    arn = %opts.arn,
                    reason = "replication_config_lookup_failed",
                    error = %err,
                    "Failed to look up replication config during resync"
                );
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return None;
            }
        };

        let targets = match ReplicationTargetStore::list_bucket_targets(&opts.bucket).await {
            Ok(targets) => targets,
            Err(err) => {
                debug!(
                    event = EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %opts.bucket,
                    error = %err,
                    reason = "target_list_failed",
                    "Failed to list bucket targets during resync"
                );
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return None;
            }
        };

        let rcfg = ReplicationConfig::new(cfg.clone(), Some(targets));
        if let Err(err) = rcfg.validate() {
            error!(
                event = EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %opts.bucket,
                arn = %opts.arn,
                error = %err,
                reason = "replication_config_invalid",
                "Replication resync config is invalid"
            );
            self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                .await;
            return None;
        }

        let target_arns = if let Some(cfg) = cfg {
            cfg.filter_target_arns(&ObjectOpts {
                op_type: ReplicationType::Resync,
                target_arn: opts.arn.clone(),
                ..Default::default()
            })
        } else {
            vec![]
        };

        if target_arns.len() != 1 {
            error!(
                event = EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %opts.bucket,
                arn = %opts.arn,
                reason = "target_arn_missing_from_replication_config",
                "Replication resync target ARN missing from replication config"
            );
            self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                .await;
            return None;
        }

        let Some(target_client) = ReplicationTargetStore::remote_target_client(&opts.bucket, &target_arns[0]).await else {
            error!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %opts.bucket,
                arn = %opts.arn,
                reason = "target_client_missing",
                "Replication resync target client missing from bucket targets"
            );
            self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                .await;
            return None;
        };

        Some((rcfg, target_client))
    }

    /// Persist the `ResyncStarted` status for non-heal runs, logging (without
    /// aborting the resync) when the status update fails.
    async fn mark_resync_started<S: ReplicationObjectIO>(&self, heal: bool, opts: &ResyncOpts, storage: &Arc<S>) {
        if !heal
            && let Err(e) = self
                .mark_status(ResyncStatusType::ResyncStarted, opts.clone(), storage.clone())
                .await
        {
            error!(
                event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %opts.bucket,
                arn = %opts.arn,
                reason = "mark_started_failed",
                error = %e,
                "Failed to update resync status"
            );
        }
    }

    /// Drain and join the resync worker tasks after a fatal dispatch error,
    /// logging any observed task failure and persisting the failed status.
    async fn finish_resync_failed<S: ReplicationObjectIO>(
        &self,
        worker_txs: Vec<tokio::sync::mpsc::Sender<ReplicateObjectInfo>>,
        results_tx: tokio::sync::mpsc::Sender<TargetReplicationResyncStatus>,
        futures: Vec<JoinHandle<()>>,
        join_failure_reason: &str,
        opts: &ResyncOpts,
        storage: &Arc<S>,
    ) {
        let worker_failed = finish_resync_workers(worker_txs, results_tx, futures, false).await;
        if worker_failed {
            error!(
                event = EVENT_RESYNC_TASK_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %opts.bucket,
                arn = %opts.arn,
                reason = join_failure_reason,
                "Replication resync worker cleanup observed task failure"
            );
        }
        self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
            .await;
    }

    /// Abort the resync worker tasks after cancellation and persist the
    /// canceled status.
    async fn finish_resync_canceled<S: ReplicationObjectIO>(
        &self,
        worker_txs: Vec<tokio::sync::mpsc::Sender<ReplicateObjectInfo>>,
        results_tx: tokio::sync::mpsc::Sender<TargetReplicationResyncStatus>,
        futures: Vec<JoinHandle<()>>,
        opts: &ResyncOpts,
        storage: &Arc<S>,
    ) {
        finish_resync_workers(worker_txs, results_tx, futures, true).await;
        self.resync_bucket_mark_status(ResyncStatusType::ResyncCanceled, opts.clone(), storage.clone())
            .await;
    }

    /// Spawn the collector task that folds per-object resync results into the
    /// aggregated resync stats.
    fn spawn_resync_results_collector(
        resyncer: Arc<Self>,
        opts: &ResyncOpts,
    ) -> (tokio::sync::mpsc::Sender<TargetReplicationResyncStatus>, JoinHandle<()>) {
        // mpsc, not broadcast: a lagging broadcast receiver returns Err(Lagged) which
        // would end the collector and silently drop every subsequent worker result.
        let (results_tx, mut results_rx) = tokio::sync::mpsc::channel::<TargetReplicationResyncStatus>(RESYNC_WORKER_COUNT * 4);

        let opts_clone = opts.clone();

        let results_fut = tokio::spawn(async move {
            while let Some(st) = results_rx.recv().await {
                resyncer.inc_stats(&st, opts_clone.clone()).await;
            }
        });

        (results_tx, results_fut)
    }

    #[instrument(skip(cancellation_token, storage))]
    pub async fn resync_bucket<S: ReplicationStorage>(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
        storage: Arc<S>,
        heal: bool,
        opts: ResyncOpts,
    ) {
        // Check cancellation before starting the scan.
        // NOTE: the previous design waited here on `worker_rx.resubscribe().recv()` to
        // throttle concurrent resyncs, but `resubscribe()` positions the new receiver at
        // the current write-head of the broadcast ring buffer, so all pre-sent bootstrap
        // signals (written in `ReplicationResyncer::new`) are invisible to it.  Every
        // spawned task therefore blocked forever, which is why `resync start` reported
        // "started" yet objects never moved.  Throttling at this level is also incorrect
        // for broadcast channels (one send unblocks ALL receivers).  The inner
        // per-object worker pool (mpsc channels, `spawn_resync_object_workers`) already
        // provides the right concurrency limit.
        if cancellation_token.is_cancelled() {
            return;
        }

        let Some(_resync_leader_guard) = Self::acquire_resync_leader_lock(&storage, &opts).await else {
            return;
        };

        let Some(_resync_admission_permit) = self.acquire_resync_admission(&cancellation_token).await else {
            return;
        };

        let Some((rcfg, target_client)) = self.resolve_resync_target(&opts, &storage).await else {
            return;
        };

        self.mark_resync_started(heal, &opts, &storage).await;

        let (rx, walk_failed, walk_task) = spawn_resync_walk_task(&storage, &cancellation_token, &opts);

        let mut futures = vec![walk_task];

        let (results_tx, results_fut) = Self::spawn_resync_results_collector(self.clone(), &opts);

        futures.push(results_fut);

        let worker_txs =
            spawn_resync_object_workers(&cancellation_token, &target_client, &storage, &opts, &results_tx, &mut futures);

        self.drive_resync_dispatch(
            &cancellation_token,
            rx,
            &rcfg,
            ResyncRunState {
                worker_txs,
                results_tx,
                futures,
                walk_failed,
            },
            &opts,
            &storage,
        )
        .await;
    }

    /// Pump walked objects through classification into the hashed worker
    /// queues, finalizing the resync status on dispatch error, cancellation,
    /// or completion of the walk.
    async fn drive_resync_dispatch<S: ReplicationStorage>(
        &self,
        cancellation_token: &CancellationToken,
        mut rx: tokio::sync::mpsc::Receiver<StorageObjectInfoOrErr<ObjectInfo, Error>>,
        rcfg: &ReplicationConfig,
        state: ResyncRunState,
        opts: &ResyncOpts,
        storage: &Arc<S>,
    ) {
        let ResyncRunState {
            worker_txs,
            results_tx,
            futures,
            walk_failed,
        } = state;

        while let Some(res) = rx.recv().await {
            if let Some(err) = res.err {
                error!(
                    event = EVENT_RESYNC_RUNTIME_CHANNEL_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %opts.bucket,
                    arn = %opts.arn,
                    reason = "object_info_failed",
                    error = %err,
                    "Failed to receive resync object info"
                );
                cancellation_token.cancel();
                drop(rx);
                self.finish_resync_failed(
                    worker_txs,
                    results_tx,
                    futures,
                    "worker_join_failed_after_object_info_error",
                    opts,
                    storage,
                )
                .await;
                return;
            }

            if cancellation_token.is_cancelled() {
                drop(rx);
                self.finish_resync_canceled(worker_txs, results_tx, futures, opts, storage)
                    .await;
                return;
            }

            let Some(object) = res.item else {
                continue;
            };

            let roi = match get_heal_replicate_object_info(&object, rcfg).await {
                Ok(roi) => roi,
                Err(err) => {
                    error!(
                        event = EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        bucket = %opts.bucket,
                        arn = %opts.arn,
                        object = %object.name,
                        error = %err,
                        "Failed to classify object for replication resync"
                    );
                    cancellation_token.cancel();
                    drop(rx);
                    self.finish_resync_failed(
                        worker_txs,
                        results_tx,
                        futures,
                        "worker_join_failed_after_classification_error",
                        opts,
                        storage,
                    )
                    .await;
                    return;
                }
            };
            if !roi.existing_obj_resync.must_resync() {
                continue;
            }

            if cancellation_token.is_cancelled() {
                drop(rx);
                self.finish_resync_canceled(worker_txs, results_tx, futures, opts, storage)
                    .await;
                return;
            }

            let worker_idx = sip_hash(&roi.name, RESYNC_WORKER_COUNT, &DEFAULT_SIP_HASH_KEY);

            if let Err(err) = worker_txs[worker_idx].send(roi).await {
                error!(
                    event = EVENT_RESYNC_RUNTIME_CHANNEL_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %opts.bucket,
                    arn = %opts.arn,
                    reason = "worker_queue_send_failed",
                    error = %err,
                    "Failed to send resync object to worker"
                );
                cancellation_token.cancel();
                drop(rx);
                self.finish_resync_failed(
                    worker_txs,
                    results_tx,
                    futures,
                    "worker_join_failed_after_queue_send_error",
                    opts,
                    storage,
                )
                .await;
                return;
            }
        }

        let worker_failed = finish_resync_workers(worker_txs, results_tx, futures, false).await;
        let target_failed = self.target_has_resync_failures(opts).await;
        let status = if walk_failed.load(Ordering::Relaxed) || worker_failed || target_failed {
            ResyncStatusType::ResyncFailed
        } else {
            ResyncStatusType::ResyncCompleted
        };

        self.resync_bucket_mark_status(status, opts.clone(), storage.clone()).await;
    }
}

/// Worker-pool channel and task state for one resync run, handed from setup to
/// the dispatch loop.
struct ResyncRunState {
    worker_txs: Vec<tokio::sync::mpsc::Sender<ReplicateObjectInfo>>,
    results_tx: tokio::sync::mpsc::Sender<TargetReplicationResyncStatus>,
    futures: Vec<JoinHandle<()>>,
    walk_failed: Arc<AtomicBool>,
}

/// Spawn the bucket walk task that feeds object listings into the resync
/// dispatch loop, surfacing walk failures through the returned flag.
fn spawn_resync_walk_task<S: ReplicationStorage>(
    storage: &Arc<S>,
    cancellation_token: &CancellationToken,
    opts: &ResyncOpts,
) -> (
    tokio::sync::mpsc::Receiver<StorageObjectInfoOrErr<ObjectInfo, Error>>,
    Arc<AtomicBool>,
    JoinHandle<()>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let walk_failed = Arc::new(AtomicBool::new(false));
    let walk_failed_task = walk_failed.clone();
    let walk_storage = storage.clone();
    let walk_cancellation = cancellation_token.clone();
    let walk_bucket = opts.bucket.clone();
    let walk_arn = opts.arn.clone();
    let walk_task = tokio::spawn(async move {
        if let Err(err) = walk_storage
            .walk(
                walk_cancellation,
                &walk_bucket,
                "",
                tx,
                WalkOptions::default().with_walkdir_timeouts(BACKGROUND_WALKDIR_TIMEOUT),
            )
            .await
        {
            walk_failed_task.store(true, Ordering::Relaxed);
            error!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %walk_bucket,
                arn = %walk_arn,
                reason = "walk_failed",
                error = %err,
                "Replication resync bucket walk failed"
            );
        }
    });
    (rx, walk_failed, walk_task)
}

/// Classify the target HEAD verification result for one resynced object,
/// updating the per-object status counters and returning the accounted size
/// together with any verification error.
async fn verify_resync_head_result(
    head_result: std::result::Result<HeadObjectOutput, HeadObjectSdkError>,
    roi: &ReplicateObjectInfo,
    st: &mut TargetReplicationResyncStatus,
    target_client: &Arc<TargetClient>,
) -> (i64, Option<HeadObjectSdkError>) {
    match head_result {
        Ok(_) => {
            st.replicated_count += 1;
            st.replicated_size += roi.size;
            (roi.size, None)
        }
        Err(err) if roi.delete_marker => {
            // Verifying a replicated delete marker: only a
            // definitive 404/NoSuchKey or 405/MethodNotAllowed
            // confirms the marker propagated. Any other
            // (retryable/ambiguous) HEAD error leaves the outcome
            // unverified, so it must count as failed — not as a
            // blanket success (backlog#862 / #799 B13).
            let retryable = {
                let (is_not_found, code) = err
                    .as_service_error()
                    .map(|se| (se.is_not_found(), se.code()))
                    .unwrap_or((false, None));
                is_retryable_delete_replication_head_error(is_not_found, code)
            };
            if retryable {
                st.failed_count += 1;
                (0, Some(err))
            } else {
                st.replicated_count += 1;
                (0, None)
            }
        }
        Err(err) => {
            // A version-addressed HEAD is not the last word on every target:
            // re-verify through the fallback before counting a well-replicated
            // object as failed (see `replica_head_fallback`).
            match replica_head_fallback(target_client.as_ref(), &roi.name, roi.etag.as_deref(), &err).await {
                Some(Ok(Some(_))) => {
                    st.replicated_count += 1;
                    st.replicated_size += roi.size;
                    (roi.size, None)
                }
                Some(Ok(None)) | None => {
                    st.failed_count += 1;
                    (0, Some(err))
                }
                Some(Err(e2)) => {
                    st.failed_count += 1;
                    (0, Some(e2))
                }
            }
        }
    }
}

/// Replicate one existing object (or delete marker / version purge) to the
/// resync target, verify the outcome via a target HEAD, and produce the
/// per-object resync status update.
async fn resync_worker_process_object<S: ReplicationStorage>(
    mut roi: ReplicateObjectInfo,
    storage: &Arc<S>,
    target_client: &Arc<TargetClient>,
    bucket_name: &str,
    target_arn: &str,
) -> TargetReplicationResyncStatus {
    if roi.delete_marker || !roi.version_purge_status.is_empty() {
        let doi = resync_existing_delete_replication_info(&roi, target_arn);
        replicate_delete(doi, storage.clone()).await;
    } else {
        roi.op_type = ReplicationType::ExistingObject;
        roi.event_type = REPLICATE_EXISTING.to_string();
        replicate_object(roi.clone(), storage.clone()).await;
    }

    let mut st = TargetReplicationResyncStatus {
        object: roi.name.clone(),
        bucket: roi.bucket.clone(),
        ..Default::default()
    };

    let reset_id = target_client.reset_id.clone();

    let head_result = head_object_for_worker(
        target_client.as_ref(),
        &target_client.bucket,
        &roi.name,
        roi.version_id.map(|v| v.to_string()),
    )
    .await;
    let (size, err) = verify_resync_head_result(head_result, &roi, &mut st, target_client).await;

    if err.is_some() {
        debug!(
            event = EVENT_RESYNC_OBJECT_PROCESSED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            reset_id = %reset_id,
            bucket = %bucket_name,
            object = %roi.name,
            version_id = %roi.version_id.unwrap_or_default(),
            size,
            error = ?err,
            "Processed resync object with verification error"
        );
    } else {
        trace!(
            event = EVENT_RESYNC_OBJECT_PROCESSED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            reset_id = %reset_id,
            bucket = %bucket_name,
            object = %roi.name,
            version_id = %roi.version_id.unwrap_or_default(),
            size,
            "Processed resync object"
        );
    }
    st.error = err.as_ref().and_then(|err| resync_target_error_detail(err.as_ref()));

    st
}

/// Spawn the per-object resync worker pool, wiring every worker to the shared
/// results channel and registering its task handle for cleanup.
fn spawn_resync_object_workers<S: ReplicationStorage>(
    cancellation_token: &CancellationToken,
    target_client: &Arc<TargetClient>,
    storage: &Arc<S>,
    opts: &ResyncOpts,
    results_tx: &tokio::sync::mpsc::Sender<TargetReplicationResyncStatus>,
    futures: &mut Vec<JoinHandle<()>>,
) -> Vec<tokio::sync::mpsc::Sender<ReplicateObjectInfo>> {
    let mut worker_txs = Vec::new();

    for _ in 0..RESYNC_WORKER_COUNT {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ReplicateObjectInfo>(100);
        worker_txs.push(tx);

        let cancel_token = cancellation_token.clone();
        let target_client = target_client.clone();
        let storage = storage.clone();
        let results_tx = results_tx.clone();
        let bucket_name = opts.bucket.clone();
        let target_arn = opts.arn.clone();

        let f = tokio::spawn(async move {
            while let Some(roi) = rx.recv().await {
                if cancel_token.is_cancelled() {
                    return;
                }

                let st = resync_worker_process_object(roi, &storage, &target_client, &bucket_name, &target_arn).await;

                if cancel_token.is_cancelled() {
                    return;
                }

                if let Err(err) = results_tx.send(st).await {
                    error!(
                        event = EVENT_RESYNC_RUNTIME_CHANNEL_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        bucket = %bucket_name,
                        reason = "status_channel_send_failed",
                        error = %err,
                        "Failed to send resync status"
                    );
                }
            }
        });

        futures.push(f);
    }

    worker_txs
}

pub async fn get_heal_replicate_object_info(oi: &ObjectInfo, rcfg: &ReplicationConfig) -> Result<ReplicateObjectInfo> {
    let mut oi = oi.clone();
    let mut user_defined = (*oi.user_defined).clone();
    let delete_path = heal_uses_delete_replication_path(oi.delete_marker, &oi.version_purge_status);
    let stored_delete_decision = if delete_path && !oi.replication_decision.is_empty() {
        Some(parse_replicate_decision(&oi.bucket, &oi.replication_decision)?)
    } else {
        None
    };
    let has_stored_delete_decision = stored_delete_decision.is_some();

    if let Some(rc) = rcfg.config.as_ref()
        && !rc.role.is_empty()
    {
        if oi.version_purge_status_internal.is_none() && !oi.version_purge_status.is_empty() {
            oi.version_purge_status_internal = Some(format!("{}={};", rc.role, oi.version_purge_status.as_str()));
        }

        if oi.replication_status_internal.is_none() && !oi.replication_status.is_empty() {
            oi.replication_status_internal = Some(format!("{}={};", rc.role, oi.replication_status.as_str()));
        }

        let keys_to_update: Vec<_> = user_defined
            .iter()
            .filter(|(k, _)| has_internal_suffix(k, SUFFIX_REPLICATION_RESET))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (k, v) in keys_to_update {
            user_defined.remove(&k);
            user_defined.insert(target_reset_header(rc.role.as_str()), v);
        }
    }

    let delete_state = if delete_path && !has_stored_delete_decision {
        ReplicationVersioningStore::prefix_state(&oi.bucket, &oi.name).await?
    } else {
        (false, false)
    };
    let dsc = if let Some(decision) = stored_delete_decision {
        decision
    } else if delete_path {
        if !delete_state.0 && !delete_state.1 {
            ReplicateDecision::default()
        } else {
            rcfg.check_delete_for_heal(
                &ObjectToDelete {
                    object_name: oi.name.clone(),
                    version_id: oi.version_id,
                    ..Default::default()
                },
                &oi,
                &ObjectOptions {
                    versioned: delete_state.0,
                    version_suspended: delete_state.1,
                    ..Default::default()
                },
            )
        }
    } else {
        must_replicate(
            oi.bucket.as_str(),
            &oi.name,
            MustReplicateOptions::new(&user_defined, (*oi.user_tags).clone(), ReplicationType::Heal, false),
        )
        .await
    };

    let target_statuses = replication_statuses_map(&oi.replication_status_internal.clone().unwrap_or_default());
    let target_purge_statuses = version_purge_statuses_map(&oi.version_purge_status_internal.clone().unwrap_or_default());
    let existing_obj_resync = if delete_path && !has_stored_delete_decision && !delete_state.0 && !delete_state.1 {
        Default::default()
    } else {
        rcfg.resync(oi.clone(), dsc.clone(), &target_statuses).await
    };
    let mut replication_state = oi.replication_state();
    replication_state.replicate_decision_str = dsc.to_string();
    let actual_size = oi.get_actual_size_or_physical();
    let replication_generation = oi.replication_generation_snapshot();

    Ok(ReplicateObjectInfo {
        name: oi.name.clone(),
        size: oi.size,
        actual_size,
        bucket: oi.bucket.clone(),
        version_id: oi.version_id,
        etag: oi.etag.clone(),
        mod_time: oi.mod_time,
        replication_status: oi.replication_status,
        replication_status_internal: oi.replication_status_internal.clone(),
        delete_marker: oi.delete_marker,
        version_purge_status_internal: oi.version_purge_status_internal.clone(),
        version_purge_status: oi.version_purge_status,
        replication_state: Some(replication_state),
        op_type: ReplicationType::Heal,
        event_type: "".to_string(),
        dsc,
        existing_obj_resync,
        target_statuses,
        target_purge_statuses,
        replication_timestamp: None,
        replication_generation,
        ssec: replication_object_is_ssec_encrypted(&user_defined),
        user_tags: (*oi.user_tags).clone(),
        checksum: oi.checksum.clone(),
        retry_count: 0,
    })
}

/// Upper bound on optimistic retries for a `resync.bin` compare-and-swap
/// update before giving up; contention on one bucket's status is a handful of
/// writers (status transitions, the periodic saver, admissions), not a crowd.
const RESYNC_STATUS_CAS_MAX_ATTEMPTS: usize = 32;

/// Read-merge-write `resync.bin` under an ETag compare-and-swap.
///
/// Every writer used to persist its node's cached whole-bucket map, so one
/// node's stale cache could silently resurrect a state another node had
/// already finalized (e.g. flip a just-canceled intent back to `Pending`).
/// `apply` receives the freshest persisted map and mutates it in place,
/// returning `Ok(false)` to skip the write. On a concurrent write the load +
/// apply + save cycle is retried against the new document. Returns the final
/// map and whether this call wrote it.
pub(crate) async fn update_resync_status_cas<S, F>(
    bucket: &str,
    api: Arc<S>,
    mut apply: F,
) -> Result<(BucketReplicationResyncStatus, bool)>
where
    S: ReplicationObjectIO,
    F: FnMut(&mut BucketReplicationResyncStatus) -> Result<bool>,
{
    let config_file = ReplicationMetadataStore::bucket_resync_file_path(bucket);
    for _ in 0..RESYNC_STATUS_CAS_MAX_ATTEMPTS {
        let (mut status, preconditions) =
            match ReplicationConfigStore::read_no_lock_with_metadata(api.clone(), &config_file).await {
                Ok((data, object_info)) => {
                    let etag = object_info
                        .etag
                        .filter(|etag| !etag.trim().is_empty())
                        .ok_or_else(|| Error::other("replication resync status has no ETag for conditional update"))?;
                    let status = if data.is_empty() {
                        BucketReplicationResyncStatus::new()
                    } else {
                        decode_resync_file(&data)?
                    };
                    (
                        status,
                        HTTPPreconditions {
                            if_match: Some(etag),
                            ..Default::default()
                        },
                    )
                }
                Err(Error::ConfigNotFound) => (
                    BucketReplicationResyncStatus::new(),
                    HTTPPreconditions {
                        if_none_match: Some("*".to_string()),
                        ..Default::default()
                    },
                ),
                Err(err) => return Err(err),
            };
        if !apply(&mut status)? {
            return Ok((status, false));
        }
        match ReplicationConfigStore::save_conditional(api.clone(), &config_file, encode_resync_file(&status)?, preconditions)
            .await
        {
            Ok(()) => return Ok((status, true)),
            Err(Error::PreconditionFailed) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(Error::other("replication resync status conditional update did not converge"))
}

/// Merge this node's cached bucket resync map into the persisted map for the
/// periodic saver. Per target: same run id overlays the fresher local state
/// unless the persisted state is already terminal and the local one is not
/// (a cancel/completion recorded by another node must stick); a different
/// persisted run id means a newer admission elsewhere and is kept; targets
/// unknown to disk are added. Returns whether `persisted` changed.
pub(crate) fn merge_local_resync_into_persisted(
    persisted: &mut BucketReplicationResyncStatus,
    local: &BucketReplicationResyncStatus,
) -> bool {
    let mut changed = false;
    for (arn, local_state) in &local.targets_map {
        match persisted.targets_map.get(arn) {
            Some(current) if current.resync_id == local_state.resync_id => {
                let persisted_terminal = !should_auto_resume_resync(current.resync_status);
                let local_terminal = !should_auto_resume_resync(local_state.resync_status);
                if persisted_terminal && !local_terminal {
                    continue;
                }
                if current != local_state {
                    persisted.targets_map.insert(arn.clone(), local_state.clone());
                    changed = true;
                }
            }
            Some(_) => {}
            None => {
                persisted.targets_map.insert(arn.clone(), local_state.clone());
                changed = true;
            }
        }
    }
    if changed && local.last_update.is_some() {
        persisted.last_update = local.last_update;
    }
    changed
}

pub async fn replicate_delete<S: ReplicationStorage>(dobj: DeletedObjectReplicationInfo, storage: Arc<S>) {
    let _ = replicate_delete_with_outcome(dobj, storage).await;
}

pub(crate) async fn replicate_delete_with_outcome<S: ReplicationStorage>(
    dobj: DeletedObjectReplicationInfo,
    storage: Arc<S>,
) -> bool {
    if dobj.delete_object.force_delete {
        return replicate_force_delete_to_targets(&dobj, storage).await;
    }

    let bucket = dobj.bucket.clone();
    let version_id = if let Some(version_id) = &dobj.delete_object.delete_marker_version_id {
        Some(version_id.to_owned())
    } else {
        dobj.delete_object.version_id
    };

    if dobj.delete_object.delete_marker
        && let Some(delete_marker_version_id) = dobj.delete_object.delete_marker_version_id
    {
        let source_marker_state = storage
            .get_object_info(
                &bucket,
                &dobj.delete_object.object_name,
                &ObjectOptions {
                    version_id: Some(delete_marker_version_id.to_string()),
                    versioned: ReplicationVersioningStore::prefix_enabled(&bucket, &dobj.delete_object.object_name).await,
                    version_suspended: ReplicationVersioningStore::prefix_suspended(&bucket, &dobj.delete_object.object_name)
                        .await,
                    ..Default::default()
                },
            )
            .await;

        match source_marker_state {
            Ok(info) if info.delete_marker && info.version_id == Some(delete_marker_version_id) => {}
            Ok(_) => {
                debug!(
                    event = EVENT_REPLICATION_DELETE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket,
                    object = dobj.delete_object.object_name,
                    version_id = %delete_marker_version_id,
                    reason = "source_not_delete_marker",
                    "Skipping stale delete-marker replication"
                );
                return true;
            }
            Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => {
                debug!(
                    event = EVENT_REPLICATION_DELETE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket,
                    object = dobj.delete_object.object_name,
                    version_id = %delete_marker_version_id,
                    reason = "source_version_missing",
                    "Skipping stale delete-marker replication"
                );
                // The marker is gone at the source, but a replica of it may
                // already exist on the targets (a live race, or an MRF
                // purge-intent replay landing here on purpose). Purge instead
                // of just skipping; the result decides whether an MRF replay
                // may acknowledge the entry.
                return purge_stale_delete_marker_targets(&bucket, &dobj).await;
            }
            Err(err) => {
                // A transient source error (lock timeout, IO error) must not
                // fall through to the marker-creation send below: that DELETE
                // omits the versionId, so every such retry lets a generic S3
                // target mint one more delete marker (rustfs#6823). Fail the
                // entry without touching the target; the MRF replay / heal
                // scanner retries once the source is readable again.
                debug!(
                    event = EVENT_REPLICATION_DELETE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket,
                    object = dobj.delete_object.object_name,
                    version_id = %delete_marker_version_id,
                    error = %err,
                    reason = "source_state_verification_failed",
                    "Failed to verify source delete-marker state before replication"
                );
                send_local_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: ObjectInfo {
                        bucket: bucket.clone(),
                        name: dobj.delete_object.object_name.clone(),
                        version_id,
                        delete_marker: dobj.delete_object.delete_marker,
                        ..Default::default()
                    },
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                return false;
            }
        }
    }

    let dsc = match parse_replicate_decision(
        &bucket,
        &dobj
            .delete_object
            .replication_state
            .as_ref()
            .map(|v| v.replicate_decision_str.clone())
            .unwrap_or_default(),
    ) {
        Ok(dsc) => dsc,
        Err(err) => {
            debug!(
                event = EVENT_REPLICATION_DELETE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                arn = %dobj.target_arn,
                error = %err,
                reason = "replicate_decision_parse_failed",
                "Failed to parse replicate decision"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return false;
        }
    };
    let ns_lock = match storage
        .new_ns_lock(&bucket, format!("/[replicate]/{}", dobj.delete_object.object_name).as_str())
        .await
    {
        Ok(ns_lock) => ns_lock,
        Err(e) => {
            debug!(
                event = EVENT_REPLICATION_DELETE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %dobj.delete_object.object_name,
                error = %e,
                reason = "ns_lock_unavailable",
                "Skipping replication delete"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return false;
        }
    };

    let _lock_guard = match ns_lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
        Ok(lock_guard) => lock_guard,
        Err(e) => {
            debug!(
                event = EVENT_REPLICATION_DELETE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %dobj.delete_object.object_name,
                error = %e,
                reason = "write_lock_unavailable",
                "Skipping replication delete"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return false;
        }
    };

    // Initialize replicated infos
    let mut rinfos = ReplicatedInfos {
        replication_timestamp: Some(OffsetDateTime::now_utc()),
        targets: Vec::with_capacity(dsc.targets_map.len()),
    };

    let mut join_set = JoinSet::new();

    // Process each target
    let target_arns = dobj.admitted_target_arns();
    let expected_targets = dsc
        .targets_map
        .values()
        .filter(|target| target.replicate && (target_arns.is_empty() || target_arns.iter().any(|arn| arn == &target.arn)))
        .count();
    for tgt_entry in dsc.targets_map.values() {
        // Skip targets that should not be replicated
        if !tgt_entry.replicate {
            continue;
        }

        // If dobj.TargetArn is not empty string, this is a case of specific target being re-synced.
        if !target_arns.is_empty() && !target_arns.iter().any(|arn| arn == &tgt_entry.arn) {
            continue;
        }

        // Get the remote target client
        let Some(tgt_client) = ReplicationTargetStore::remote_target_client(&bucket, &tgt_entry.arn).await else {
            debug!(
                event = EVENT_REPLICATION_DELETE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                arn = %tgt_entry.arn,
                reason = "target_client_missing",
                "Skipping replication delete because target client is unavailable"
            );
            rinfos.targets.push(unavailable_delete_target_info(&dobj, &tgt_entry.arn));
            continue;
        };

        let dobj_clone = dobj.clone();

        // Spawn task in the join set
        join_set.spawn(async move { replicate_delete_to_target(&dobj_clone, tgt_client.clone()).await });
    }

    // Collect all results
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(tgt_info) => {
                rinfos.targets.push(tgt_info);
            }
            Err(e) => {
                error!(
                    event = EVENT_RESYNC_TASK_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %bucket,
                    object = %dobj.delete_object.object_name,
                    operation = "replicate_delete",
                    error = %e,
                    "Replication resync task failed"
                );
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: ObjectInfo {
                        bucket: bucket.clone(),
                        name: dobj.delete_object.object_name.clone(),
                        version_id,
                        delete_marker: dobj.delete_object.delete_marker,
                        ..Default::default()
                    },
                    ..Default::default()
                });
            }
        }
    }

    let is_version_purge = is_version_delete_replication(&dobj.delete_object);

    // The watcher exists to purge a replicated marker once the SOURCE marker
    // vanishes. A version purge is that purge already (its failures reach the
    // journal as a purge entry), so it must not spawn a second watcher that
    // journals a duplicate intent (backlog#2290).
    let requires_delayed_purge = should_retry_delete_marker_purge(&dobj.delete_object) && !is_version_purge;

    let (replication_status, prev_status) = if !is_version_purge {
        (
            rinfos.replication_status(),
            dobj.delete_object
                .replication_state
                .as_ref()
                .map(|v| v.composite_replication_status())
                .unwrap_or(ReplicationStatusType::Empty),
        )
    } else {
        (
            ReplicationStatusType::from(rinfos.version_purge_status()),
            ReplicationStatusType::from(
                dobj.delete_object
                    .replication_state
                    .as_ref()
                    .map(|v| v.composite_version_purge_status())
                    .unwrap_or(VersionPurgeStatusType::Empty),
            ),
        )
    };

    if let Some(stats) = runtime_sources::replication_stats() {
        for tgt in rinfos.targets.iter() {
            if tgt.replication_status != tgt.prev_replication_status {
                stats
                    .update(&bucket, tgt, tgt.replication_status.clone(), tgt.prev_replication_status.clone())
                    .await;
            }
        }
    }

    let delete_version_id = dobj.delete_object.version_id.map(|v| v.to_string());
    note_replication_terminal_failure(&bucket, &dobj.delete_object.object_name, delete_version_id.as_deref(), &rinfos);

    let mut drs = get_replication_state(
        &rinfos,
        &dobj.delete_object.replication_state.clone().unwrap_or_default(),
        delete_version_id,
    );
    if replication_status != prev_status {
        drs.replication_timestamp = Some(OffsetDateTime::now_utc());
    }

    if requires_delayed_purge {
        // Hand the watcher the MERGED replication state: `drs` folds this
        // round's per-target results into the previous state, including the
        // version ids the targets assigned to the markers they just created.
        // Spawning with the pre-merge `dobj` made the purge fall back to a
        // source-derived id, which a target that mints its own ids answers
        // with an idempotent 204 — the intent was then dropped while the
        // real marker stayed behind.
        let bucket_clone = bucket.clone();
        let mut dobj_clone = dobj.clone();
        dobj_clone.delete_object.replication_state = Some(drs.clone());
        let dsc_clone = dsc.clone();
        let storage_clone = storage.clone();
        tokio::spawn(async move {
            watch_and_purge_source_delete_marker(bucket_clone, dobj_clone, dsc_clone, storage_clone).await;
        });
    }

    let event_name = if replication_status == ReplicationStatusType::Completed {
        EventName::ObjectReplicationComplete.to_string()
    } else {
        EventName::ObjectReplicationFailed.to_string()
    };

    let state_persisted = match storage
        .delete_object(
            &bucket,
            &dobj.delete_object.object_name,
            ObjectOptions {
                version_id: version_id.map(|v| v.to_string()),
                mod_time: dobj.delete_object.delete_marker_mtime,
                delete_replication: Some(drs),
                versioned: ReplicationVersioningStore::prefix_enabled(&bucket, &dobj.delete_object.object_name).await,
                version_suspended: ReplicationVersioningStore::prefix_suspended(&bucket, &dobj.delete_object.object_name).await,
                ..Default::default()
            },
        )
        .await
    {
        Ok(object) => {
            send_event(EventArgs {
                event_name,
                bucket_name: bucket.clone(),
                object,
                ..Default::default()
            });
            true
        }
        Err(e) => {
            error!(
                event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                arn = %dobj.target_arn,
                object = %dobj.delete_object.object_name,
                operation = "apply_replication_delete_state",
                error = %e,
                "Replication target operation failed"
            );
            send_event(EventArgs {
                event_name,
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                ..Default::default()
            });
            false
        }
    };

    replicate_delete_outcome(
        expected_targets,
        rinfos.targets.len(),
        state_persisted,
        // Source state is verified by construction here: a verification
        // error returns early above instead of replicating unverified.
        true,
        &replication_status,
    )
}

async fn source_delete_marker_missing<S: EcstoreObjectOperations>(
    storage: &S,
    bucket: &str,
    object_name: &str,
    delete_marker_version_id: Uuid,
) -> bool {
    match storage
        .get_object_info(
            bucket,
            object_name,
            &ObjectOptions {
                version_id: Some(delete_marker_version_id.to_string()),
                versioned: ReplicationVersioningStore::prefix_enabled(bucket, object_name).await,
                version_suspended: ReplicationVersioningStore::prefix_suspended(bucket, object_name).await,
                ..Default::default()
            },
        )
        .await
    {
        Ok(info) => !info.delete_marker || info.version_id != Some(delete_marker_version_id),
        Err(err) => is_err_object_not_found(&err) || is_err_version_not_found(&err),
    }
}

/// One purge pass over the eligible targets. Returns the ARNs that must be
/// retried: the remote DELETE failed, or the target client was unavailable
/// (e.g. a runtime cache miss). Inconsistent recorded version mappings are a
/// deliberate refusal — retrying cannot make guessing a version id safe — so
/// they are logged and excluded from the retry set.
async fn replicate_delete_marker_purge_to_targets(
    bucket: &str,
    dobj: &DeletedObjectReplicationInfo,
    dsc: &ReplicateDecision,
    retry_arns: Option<&[String]>,
) -> Vec<String> {
    let Some(delete_marker_version_id) = dobj.delete_object.delete_marker_version_id else {
        return Vec::new();
    };

    let target_arns = dobj.admitted_target_arns();
    let mut failed_arns = Vec::new();
    for tgt_entry in dsc.targets_map.values() {
        if !tgt_entry.replicate {
            continue;
        }
        if !target_arns.is_empty() && !target_arns.iter().any(|arn| arn == &tgt_entry.arn) {
            continue;
        }
        if let Some(retry_arns) = retry_arns
            && !retry_arns.iter().any(|arn| arn == &tgt_entry.arn)
        {
            continue;
        }
        // Decide the version first: refusing to guess is a per-target
        // FAILURE, not a silent skip. Reporting it as success would let the
        // watcher and the MRF replay drop the purge intent while the marker
        // is still on the target — the leak stays visible instead (the
        // entry is retained and keeps warning) until an operator repairs
        // the metadata.
        let Some(purge_version_id) = delete_marker_purge_version_id(
            dobj.delete_object.replication_state.as_ref(),
            &tgt_entry.arn,
            delete_marker_version_id,
        ) else {
            warn!(
                event = EVENT_DELETE_MARKER_PURGE_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket,
                object = dobj.delete_object.object_name,
                arn = tgt_entry.arn,
                reason = "recorded_target_version_inconsistent",
                "Delete-marker purge refused: recorded target version metadata is inconsistent"
            );
            counter!(METRIC_DELETE_MARKER_PURGE_TOTAL, "state" => "refused").increment(1);
            failed_arns.push(tgt_entry.arn.clone());
            continue;
        };

        let Some(tgt_client) = ReplicationTargetStore::remote_target_client(bucket, &tgt_entry.arn).await else {
            warn!(
                event = EVENT_DELETE_MARKER_PURGE_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket,
                object = dobj.delete_object.object_name,
                arn = tgt_entry.arn,
                reason = "target_client_missing",
                "Delete-marker purge attempt failed"
            );
            counter!(METRIC_DELETE_MARKER_PURGE_TOTAL, "state" => "failed").increment(1);
            failed_arns.push(tgt_entry.arn.clone());
            continue;
        };

        match tgt_client
            .remove_object(
                &tgt_client.bucket,
                &dobj.delete_object.object_name,
                purge_version_id,
                replication_delete_marker_purge_remove_options(dobj.delete_object.delete_marker_mtime),
            )
            .await
        {
            Ok(_) => {
                counter!(METRIC_DELETE_MARKER_PURGE_TOTAL, "state" => "purged").increment(1);
            }
            // The marker version is already gone on the target: the purge goal
            // is met. Strict S3 targets 404 here (RustFS/MinIO answer 204);
            // treating it as a failure would retain the intent entry forever.
            Err(error) if matches!(error.code.as_deref(), Some("NoSuchKey" | "NoSuchVersion")) => {
                counter!(METRIC_DELETE_MARKER_PURGE_TOTAL, "state" => "purged").increment(1);
            }
            Err(error) => {
                warn!(
                    event = EVENT_DELETE_MARKER_PURGE_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket,
                    object = dobj.delete_object.object_name,
                    arn = tgt_entry.arn,
                    error = %error,
                    reason = "target_delete_failed",
                    "Delete-marker purge attempt failed"
                );
                counter!(METRIC_DELETE_MARKER_PURGE_TOTAL, "state" => "failed").increment(1);
                mark_replication_target_offline_if_needed(&tgt_client, &error).await;
                failed_arns.push(tgt_entry.arn.clone());
            }
        }
    }
    failed_arns
}

const DELETE_MARKER_PURGE_WATCH_ROUNDS: usize = 5;
const DELETE_MARKER_PURGE_WATCH_INTERVAL: TokioDuration = TokioDuration::from_secs(1);

/// Watch the source delete marker for a short window after its replication.
///
/// KNOWN NON-DURABLE WINDOW: this task is detached, so a process exit inside
/// the watch window loses an intent that has not been persisted yet. The
/// window predates this code (the previous implementation had no durable
/// channel at all, and no replay half either), so nothing regresses — closing
/// it needs a write-ahead intent recorded before the parent delete is
/// acknowledged, which is tracked as follow-up rather than done here: every
/// delete-marker replication would pay a journal write for a purge that
/// almost never happens.
///
/// If the marker disappears (deleted before or while the replica landed),
/// purge the replicated marker from the targets, retrying failed targets on
/// later rounds. When the window drains with targets still dirty, persist the
/// purge intent as a durable MRF entry so the next startup replays it through
/// `purge_stale_delete_marker_targets`.
async fn watch_and_purge_source_delete_marker<S: ReplicationStorage>(
    bucket: String,
    dobj: DeletedObjectReplicationInfo,
    dsc: ReplicateDecision,
    storage: Arc<S>,
) {
    let Some(delete_marker_version_id) = dobj.delete_object.delete_marker_version_id else {
        return;
    };

    // `pending` is None until the source marker is observed missing; after the
    // first purge pass it holds the targets that still need a successful purge.
    let mut pending: Option<Vec<String>> = None;
    for round in 0..DELETE_MARKER_PURGE_WATCH_ROUNDS {
        pending = match pending.take() {
            None => {
                if source_delete_marker_missing(&*storage, &bucket, &dobj.delete_object.object_name, delete_marker_version_id)
                    .await
                {
                    Some(replicate_delete_marker_purge_to_targets(&bucket, &dobj, &dsc, None).await)
                } else {
                    None
                }
            }
            Some(failed_arns) => Some(replicate_delete_marker_purge_to_targets(&bucket, &dobj, &dsc, Some(&failed_arns)).await),
        };
        if matches!(pending.as_deref(), Some([])) {
            return;
        }
        if round + 1 < DELETE_MARKER_PURGE_WATCH_ROUNDS {
            tokio::time::sleep(DELETE_MARKER_PURGE_WATCH_INTERVAL).await;
        }
    }
    if let Some(failed_arns) = pending.filter(|failed_arns| !failed_arns.is_empty()) {
        enqueue_delete_marker_purge_mrf(&dobj, failed_arns).await;
    }
}

async fn enqueue_delete_marker_purge_mrf(dobj: &DeletedObjectReplicationInfo, failed_arns: Vec<String>) {
    let arns = failed_arns.join(",");
    let miss_reason = match runtime_sources::replication_pool() {
        None => Some("replication_pool_unavailable"),
        Some(pool) => match pool.persist_mrf_entry(delete_marker_purge_mrf_entry(dobj, failed_arns)).await {
            ReplicationQueueAdmission::Queued => None,
            _ => Some("mrf_save_unavailable"),
        },
    };
    match miss_reason {
        None => {
            warn!(
                event = EVENT_DELETE_MARKER_PURGE_MRF,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = dobj.bucket,
                object = dobj.delete_object.object_name,
                arns,
                state = "queued",
                "Delete-marker purge exhausted its watch window; intent persisted to the MRF journal"
            );
            counter!(METRIC_DELETE_MARKER_PURGE_TOTAL, "state" => "mrf_queued").increment(1);
        }
        Some(reason) => {
            warn!(
                event = EVENT_DELETE_MARKER_PURGE_MRF,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = dobj.bucket,
                object = dobj.delete_object.object_name,
                arns,
                state = "missed",
                reason,
                "Delete-marker purge intent could not be persisted for retry"
            );
            counter!(METRIC_DELETE_MARKER_PURGE_TOTAL, "state" => "mrf_missed").increment(1);
        }
    }
}

/// The marker vanished at the source while its replication was still pending
/// (a live race), or this is an MRF purge-intent replay. Any marker already
/// replicated to a target must still be purged; run bounded retry passes and
/// report the result so an MRF replay only acknowledges the entry once every
/// target is clean. Live callers persist a fresh purge intent on failure;
/// replay callers (`ReplicationType::Heal`) rely on Missed retention instead,
/// so the journal does not accumulate duplicate entries.
///
/// Heal callers retry for the full watch window because the startup MRF
/// processor runs before bucket metadata (and thus target clients) finishes
/// initializing — the first pass can see `target_client_missing` and a later
/// round resolves the client; the replay loop is serial and startup-only, so
/// blocking it for up to the window per dirty entry is acceptable. Live
/// callers run on replication workers where a down target would pin a worker
/// for the whole window, so they attempt once and lean on the durable intent
/// entry instead.
async fn purge_stale_delete_marker_targets(bucket: &str, dobj: &DeletedObjectReplicationInfo) -> bool {
    let decision_str = dobj
        .delete_object
        .replication_state
        .as_ref()
        .map(|state| state.replicate_decision_str.clone())
        .unwrap_or_default();
    let dsc = match parse_replicate_decision(bucket, &decision_str) {
        Ok(dsc) => dsc,
        Err(error) => {
            warn!(
                event = EVENT_DELETE_MARKER_PURGE_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket,
                object = dobj.delete_object.object_name,
                error = %error,
                reason = "replicate_decision_parse_failed",
                "Delete-marker purge attempt failed"
            );
            return false;
        }
    };
    let rounds = if dobj.op_type == ReplicationType::Heal {
        DELETE_MARKER_PURGE_WATCH_ROUNDS
    } else {
        1
    };
    let mut failed_arns = replicate_delete_marker_purge_to_targets(bucket, dobj, &dsc, None).await;
    for _ in 1..rounds {
        if failed_arns.is_empty() {
            break;
        }
        tokio::time::sleep(DELETE_MARKER_PURGE_WATCH_INTERVAL).await;
        failed_arns = replicate_delete_marker_purge_to_targets(bucket, dobj, &dsc, Some(&failed_arns)).await;
    }
    if failed_arns.is_empty() {
        return true;
    }
    if dobj.op_type != ReplicationType::Heal {
        enqueue_delete_marker_purge_mrf(dobj, failed_arns).await;
    }
    false
}

async fn replicate_force_delete_to_targets<S: ReplicationStorage>(dobj: &DeletedObjectReplicationInfo, storage: Arc<S>) -> bool {
    let bucket = &dobj.bucket;
    let object_name = &dobj.delete_object.object_name;
    let admitted_target_arns = dobj.admitted_target_arns();

    let legacy_target_arns = if admitted_target_arns.is_empty() {
        match get_replication_config(bucket).await {
            Ok(Some(config)) => config.filter_target_arns(&ObjectOpts {
                name: object_name.clone(),
                ..Default::default()
            }),
            Ok(None) => {
                debug!(
                    event = EVENT_REPLICATION_FORCE_DELETE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %bucket,
                    reason = "replication_config_missing",
                    "Skipping replication force-delete because replication config is missing"
                );
                send_local_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: ObjectInfo {
                        bucket: bucket.clone(),
                        name: object_name.clone(),
                        ..Default::default()
                    },
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                Vec::new()
            }
            Err(err) => {
                debug!(
                    event = EVENT_REPLICATION_FORCE_DELETE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %bucket,
                    error = %err,
                    reason = "replication_config_lookup_failed",
                    "Skipping replication force-delete because replication config lookup failed"
                );
                send_local_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: ObjectInfo {
                        bucket: bucket.clone(),
                        name: object_name.clone(),
                        ..Default::default()
                    },
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let ns_lock = match storage
        .new_ns_lock(bucket, format!("/[replicate]/{}", object_name).as_str())
        .await
    {
        Ok(ns_lock) => ns_lock,
        Err(e) => {
            warn!(
                event = EVENT_REPLICATION_FORCE_DELETE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object_name,
                reason = "ns_lock_create_failed",
                error = %e,
                "Skipping replication force-delete"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return false;
        }
    };

    let _lock_guard = match ns_lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
        Ok(guard) => guard,
        Err(e) => {
            warn!(
                event = EVENT_REPLICATION_FORCE_DELETE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object_name,
                reason = "write_lock_failed",
                error = %e,
                "Skipping replication force-delete"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return false;
        }
    };

    let tgt_arns = if admitted_target_arns.is_empty() {
        legacy_target_arns
    } else {
        admitted_target_arns
    };
    if tgt_arns.is_empty() {
        return false;
    }

    let mut join_set = JoinSet::new();
    let mut all_succeeded = true;

    for arn in tgt_arns {
        let Some(tgt_client) = ReplicationTargetStore::remote_target_client(bucket, &arn).await else {
            all_succeeded = false;
            debug!(
                event = EVENT_REPLICATION_FORCE_DELETE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                arn = %arn,
                reason = "target_client_missing",
                "Skipping replication force-delete because target client is unavailable"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            continue;
        };

        let bucket = bucket.clone();
        let object_name = object_name.clone();

        join_set.spawn(async move {
            if ReplicationTargetStore::target_is_offline(&tgt_client).await {
                error!(
                    event = EVENT_REPLICATION_FORCE_DELETE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %bucket,
                    arn = %tgt_client.arn,
                    reason = "target_offline",
                    endpoint = %tgt_client.to_url(),
                    "Skipping replication force-delete"
                );
                send_local_event(EventArgs {
                    event_name: EventName::ObjectReplicationFailed.to_string(),
                    bucket_name: bucket.clone(),
                    object: ObjectInfo {
                        bucket: bucket.clone(),
                        name: object_name.clone(),
                        ..Default::default()
                    },
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                return false;
            }

            if let Err(e) = tgt_client
                .remove_object(&tgt_client.bucket, &object_name, None, replication_force_delete_remove_options())
                .await
            {
                error!(
                    event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %bucket,
                    object = %object_name,
                    arn = %tgt_client.arn,
                    operation = "force_delete_remove_object",
                    error = %e,
                    "Replication target operation failed"
                );
                send_local_event(EventArgs {
                    event_name: EventName::ObjectReplicationFailed.to_string(),
                    bucket_name: bucket.clone(),
                    object: ObjectInfo {
                        bucket: bucket.clone(),
                        name: object_name.clone(),
                        ..Default::default()
                    },
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                return false;
            }

            true
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(success) => all_succeeded &= success,
            Err(error) => {
                all_succeeded = false;
                error!(
                    event = EVENT_RESYNC_TASK_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %bucket,
                    object = %object_name,
                    operation = "force_delete",
                    error = %error,
                    "Replication resync task failed"
                );
            }
        }
    }

    if all_succeeded
        && let Some(operation_id) = dobj.delete_object.force_delete_id
        && let Err(error) = super::replication_pool::complete_force_delete_intent(storage, operation_id).await
    {
        warn!(
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            bucket = %bucket,
            object = %object_name,
            operation_id = %operation_id,
            error = %error,
            "Force-delete replication completed but durable intent cleanup failed"
        );
        return false;
    }

    all_succeeded
}

fn unavailable_delete_target_info(dobj: &DeletedObjectReplicationInfo, arn: &str) -> ReplicatedTargetInfo {
    let mut rinfo = dobj
        .delete_object
        .replication_state
        .as_ref()
        .map(|state| state.target_state(arn))
        .unwrap_or_else(|| ReplicatedTargetInfo {
            arn: arn.to_string(),
            ..Default::default()
        });
    rinfo.op_type = dobj.op_type;
    if is_version_delete_replication(&dobj.delete_object) {
        if rinfo.version_purge_status != VersionPurgeStatusType::Complete {
            rinfo.version_purge_status = VersionPurgeStatusType::Failed;
            rinfo.error = Some(TARGET_CLIENT_UNAVAILABLE_ERROR.to_string());
        }
    } else if rinfo.prev_replication_status == ReplicationStatusType::Completed && dobj.op_type != ReplicationType::ExistingObject
    {
        rinfo.replication_status = ReplicationStatusType::Completed;
    } else {
        rinfo.replication_status = ReplicationStatusType::Failed;
        rinfo.error = Some(TARGET_CLIENT_UNAVAILABLE_ERROR.to_string());
    }
    rinfo
}

async fn replicate_delete_to_target(dobj: &DeletedObjectReplicationInfo, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo {
    let mut rinfo = dobj
        .delete_object
        .replication_state
        .clone()
        .unwrap_or_default()
        .target_state(&tgt_client.arn);
    rinfo.op_type = dobj.op_type;
    rinfo.endpoint = tgt_client.endpoint.clone();
    rinfo.secure = tgt_client.secure;

    let is_version_purge = is_version_delete_replication(&dobj.delete_object);
    if !is_version_purge
        && rinfo.prev_replication_status == ReplicationStatusType::Completed
        && dobj.op_type != ReplicationType::ExistingObject
    {
        rinfo.replication_status = rinfo.prev_replication_status.clone();
        return rinfo;
    }

    if is_version_purge && rinfo.version_purge_status == VersionPurgeStatusType::Complete {
        return rinfo;
    }

    if ReplicationTargetStore::target_is_offline(&tgt_client).await {
        if !is_version_purge {
            rinfo.replication_status = ReplicationStatusType::Failed;
        } else {
            rinfo.version_purge_status = VersionPurgeStatusType::Failed;
        }
        return rinfo;
    }

    // Purging a replicated delete marker addresses the version the target
    // assigned (recorded when the marker was created there); see
    // `delete_replication_target_version_id`. A corrupt record is a failure,
    // not a guess: the entry stays visible until the metadata is repaired.
    let Some(version_id) = delete_replication_target_version_id(&dobj.delete_object, &tgt_client.arn) else {
        warn!(
            event = EVENT_DELETE_MARKER_PURGE_FAILED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            bucket = tgt_client.bucket,
            object = dobj.delete_object.object_name,
            arn = %tgt_client.arn,
            reason = "recorded_target_version_inconsistent",
            "Replicated version purge refused: recorded target delete-marker version metadata is inconsistent"
        );
        rinfo.version_purge_status = VersionPurgeStatusType::Failed;
        rinfo.error = Some("recorded target delete-marker version metadata is inconsistent".to_string());
        return rinfo;
    };

    if dobj.delete_object.delete_marker && dobj.delete_object.delete_marker_version_id.is_some() {
        match head_object_for_worker(
            tgt_client.as_ref(),
            &tgt_client.bucket,
            &dobj.delete_object.object_name,
            version_id.clone(),
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                let non_retryable = matches!(
                    e.as_ref(),
                    SdkError::ServiceError(service_err)
                        if is_retryable_delete_replication_head_error(
                            service_err.err().is_not_found(),
                            service_err.err().code(),
                        )
                );
                if non_retryable {
                    rinfo.replication_status = ReplicationStatusType::Failed;
                    rinfo.error = Some(e.to_string());
                    return rinfo;
                }
            }
        }
    }

    match tgt_client
        .remove_object(
            &tgt_client.bucket,
            &dobj.delete_object.object_name,
            version_id.clone(),
            // A version purge must keep the versionId on the DELETE even when
            // the purged version is a delete marker: marker-creation semantics
            // would drop it and a generic S3 target would mint a fresh marker
            // on every retry (rustfs#6823).
            replication_delete_remove_options(
                delete_replication_creates_marker(&dobj.delete_object),
                dobj.delete_object.delete_marker_mtime,
            ),
        )
        .await
    {
        Ok(assigned_version_id) => {
            debug!(
                bucket = tgt_client.bucket,
                object = dobj.delete_object.object_name,
                version_id = ?version_id,
                assigned_version_id = ?assigned_version_id,
                delete_marker = dobj.delete_object.delete_marker,
                is_version_purge,
                "replicate_delete_to_target succeeded"
            );
            if !is_version_purge {
                // Record the version the target actually assigned to the marker it
                // just created. A later purge addresses that id directly instead of
                // deriving one from the source uuid, which only holds when the
                // target mirrors source version ids.
                if dobj.delete_object.delete_marker {
                    rinfo.target_delete_marker_version_id = assigned_version_id.filter(|version_id| !version_id.is_empty());
                }
                rinfo.replication_status = ReplicationStatusType::Completed;
            } else {
                rinfo.version_purge_status = VersionPurgeStatusType::Complete;
            }
        }
        Err(e) => {
            let object_lock_denied = is_version_purge && is_object_lock_denied_delete(e.code.as_deref(), e.message.as_deref());
            if object_lock_denied {
                // Terminal for as long as the lock holds: the peer retains
                // this version under COMPLIANCE retention or legal hold, or
                // is a target without the replicated-purge GOVERNANCE
                // exemption (#6850), so the sites stay diverged until the
                // lock on the replica lapses. Surface it loudly instead of
                // letting a silent failed counter and a hot heal-retry loop
                // stand in for the divergence.
                record_object_lock_denied_purge(dobj, &tgt_client.arn);
                error!(
                    event = EVENT_REPLICATION_PURGE_OBJECT_LOCK_DENIED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = tgt_client.bucket,
                    object = dobj.delete_object.object_name,
                    version_id = ?version_id,
                    arn = %tgt_client.arn,
                    error = %e,
                    operation = "replicate_delete_to_target",
                    "Replicated version purge denied by object lock on the target; the sites stay diverged until the lock lapses"
                );
            } else {
                warn!(
                    event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = tgt_client.bucket,
                    object = dobj.delete_object.object_name,
                    version_id = ?version_id,
                    delete_marker = dobj.delete_object.delete_marker,
                    is_version_purge,
                    error = %e,
                    operation = "replicate_delete_to_target",
                    "Replication target operation failed"
                );
            }
            rinfo.error = Some(e.to_string());
            if !is_version_purge {
                rinfo.replication_status = ReplicationStatusType::Failed;
            } else {
                rinfo.version_purge_status = VersionPurgeStatusType::Failed;
            }
            mark_replication_target_offline_if_needed(&tgt_client, &e).await;
        }
    }

    if rinfo.replication_status == ReplicationStatusType::Completed
        && !tgt_client.reset_id.is_empty()
        && dobj.op_type == ReplicationType::ExistingObject
    {
        rinfo.resync_timestamp = format!(
            "{};{}",
            OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "invalid-time".to_string()),
            tgt_client.reset_id
        );
    }

    rinfo
}

pub async fn replicate_object<S: ReplicationStorage>(roi: ReplicateObjectInfo, storage: Arc<S>) -> ReplicationState {
    replicate_object_with_outcome(roi, storage).await.0
}

enum ReplicationStatePersistOutcome {
    Updated,
    Superseded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplicationAttemptDisposition {
    Persisted,
    Superseded,
    Retry,
}

impl ReplicationAttemptDisposition {
    pub(crate) fn consumes_mrf_entry(self) -> bool {
        matches!(self, Self::Persisted | Self::Superseded)
    }
}

fn replication_attempt_preflight(roi: &ReplicateObjectInfo) -> Option<(ReplicationState, ReplicationAttemptDisposition)> {
    roi.replication_generation
        .invalid
        .then(|| (roi.replication_state.clone().unwrap_or_default(), ReplicationAttemptDisposition::Retry))
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ReplicationTerminalPublication {
    emit_terminal_failure: bool,
    emit_event: bool,
    update_transition_stats: bool,
    update_same_state_failure_stats: bool,
}

fn replication_terminal_publication(
    suppress_terminal_publication: bool,
    state_update_needed: bool,
    attempt_status: ReplicationStatusType,
    previous_internal_matches_attempt: bool,
) -> ReplicationTerminalPublication {
    if suppress_terminal_publication {
        return ReplicationTerminalPublication::default();
    }
    ReplicationTerminalPublication {
        emit_terminal_failure: true,
        emit_event: true,
        update_transition_stats: state_update_needed,
        update_same_state_failure_stats: attempt_status != ReplicationStatusType::Completed && previous_internal_matches_attempt,
    }
}

fn replication_status_writeback_mode(state_update_needed: bool) -> ReplicationStatusWritebackMode {
    if state_update_needed {
        ReplicationStatusWritebackMode::Update
    } else {
        ReplicationStatusWritebackMode::ValidateOnly
    }
}

/// Publish a worker's status with a storage-enforced compare-and-set token.
/// `put_object_metadata` checks the token while it owns the object write lock,
/// avoiding both a read/write race and any need to hold a hot object lock
/// across network I/O.
fn replication_status_writeback_options(
    roi: &ReplicateObjectInfo,
    replication_lock_guard: &rustfs_lock::NamespaceLockGuard,
    new_replication_internal: Option<&String>,
    mode: ReplicationStatusWritebackMode,
) -> ObjectOptions {
    let mut eval_metadata = HashMap::new();
    if let Some(status) = new_replication_internal {
        insert_str(&mut eval_metadata, SUFFIX_REPLICATION_STATUS, status.clone());
    }
    let mut write_opts = ObjectOptions {
        version_id: roi.version_id.map(|version_id| version_id.to_string()),
        eval_metadata: Some(eval_metadata),
        replication_status_writeback: Some(Box::new(ReplicationStatusWritebackCondition {
            expected_generation: roi.replication_generation.clone(),
            mode,
        })),
        ..Default::default()
    };
    // The remote transfer runs under a renewable replication namespace lock.
    // Carry that guard's loss signal into the storage commit so a worker whose
    // lease expired while it was doing remote I/O cannot publish over a newer
    // worker for the same generation.
    write_opts.add_namespace_lock_guard(replication_lock_guard);
    write_opts
}

async fn persist_replication_state_if_current<S: ReplicationStorage>(
    roi: &ReplicateObjectInfo,
    storage: &Arc<S>,
    replication_lock_guard: &rustfs_lock::NamespaceLockGuard,
    new_replication_internal: Option<&String>,
    mode: ReplicationStatusWritebackMode,
    object_info: &mut ObjectInfo,
) -> Result<ReplicationStatePersistOutcome> {
    let write_opts = replication_status_writeback_options(roi, replication_lock_guard, new_replication_internal, mode);
    match storage.put_object_metadata(&roi.bucket, &roi.name, &write_opts).await {
        Ok(updated) => {
            *object_info = updated;
            Ok(ReplicationStatePersistOutcome::Updated)
        }
        Err(Error::PreconditionFailed) => Ok(ReplicationStatePersistOutcome::Superseded),
        Err(error) => Err(error),
    }
}

pub(crate) async fn replicate_object_with_outcome<S: ReplicationStorage>(
    roi: ReplicateObjectInfo,
    storage: Arc<S>,
) -> (ReplicationState, ReplicationAttemptDisposition) {
    // Conflicting compatibility aliases, empty opaque timestamps, and invalid
    // mutation UUIDs are corruption, not evidence that another generation
    // superseded this task. Fail before any target I/O and keep the MRF entry
    // retryable instead of repeatedly transmitting and then acknowledging it.
    if let Some(outcome) = replication_attempt_preflight(&roi) {
        return outcome;
    }

    let bucket = roi.bucket.clone();
    let object = roi.name.clone();

    let tgt_arns = roi.admitted_target_arns();

    // Acquire a per-object namespace lock so that at most one worker (across all cluster
    // nodes and MRF retry goroutines) replicates this object version at a time.
    let obj_lock_key = format!("/[replicate]/{}", object);
    let obj_ns_lock = match storage.new_ns_lock(&bucket, &obj_lock_key).await {
        Ok(l) => l,
        Err(e) => {
            debug!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object,
                error = %e,
                reason = "ns_lock_create_failed",
                "Skipping replication object"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: roi.to_object_info(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return (roi.replication_state.unwrap_or_default(), ReplicationAttemptDisposition::Retry);
        }
    };
    let obj_lock_guard = match obj_ns_lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
        Ok(g) => g,
        Err(e) => {
            debug!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object,
                error = %e,
                reason = "ns_lock_write_lock_failed",
                "Skipping replication object"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: roi.to_object_info(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return (roi.replication_state.unwrap_or_default(), ReplicationAttemptDisposition::Retry);
        }
    };

    let mut join_set = JoinSet::new();
    let mut rinfos = ReplicatedInfos {
        replication_timestamp: Some(OffsetDateTime::now_utc()),
        targets: Vec::with_capacity(tgt_arns.len()),
    };

    for arn in tgt_arns {
        let Some(tgt_client) = ReplicationTargetStore::remote_target_client(&bucket, &arn).await else {
            // Deliberately debug: this fires once per object per ARN, so a target that
            // stays unreachable would flood the log from the replication hot path. The
            // condition is reported once per pass by the site-replication reconciler and
            // once per rebuild by `update_all_targets`, which is where an operator can act
            // on it; the FAILED state below preserves retry visibility and the
            // aggregate result emits the user-visible failure event once.
            debug!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object,
                arn = %arn,
                reason = "target_client_missing",
                "Replication target client unavailable"
            );
            rinfos.targets.push(unavailable_object_target_info(&roi, &arn));
            continue;
        };

        let roi_clone = roi.clone();
        let storage_clone = storage.clone();
        join_set.spawn(async move {
            if roi.op_type == ReplicationType::Object {
                roi_clone.replicate_object(storage_clone, tgt_client).await
            } else {
                roi_clone.replicate_all(storage_clone, tgt_client).await
            }
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(tgt_info) => {
                rinfos.targets.push(tgt_info);
            }
            Err(e) => {
                error!(
                    event = EVENT_RESYNC_TASK_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %bucket,
                    object = %object,
                    operation = "replicate_object",
                    error = %e,
                    "Replication resync task failed"
                );
                send_local_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: roi.to_object_info(),
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
            }
        }
    }

    let version_id = roi.version_id.map(|v| v.to_string());

    let previous_state = roi.replication_state.clone().unwrap_or_default();
    let mut merged_state = get_replication_state(&rinfos, &previous_state, version_id.clone());
    let mut replication_status = merged_state.composite_replication_status();
    let new_replication_internal = merged_state.replication_status_internal.clone();
    let mut object_info = roi.to_object_info();
    let mut disposition = ReplicationAttemptDisposition::Persisted;
    let mut suppress_terminal_publication = false;
    let state_update_needed = roi.replication_status_internal != new_replication_internal || rinfos.replication_resynced();
    let writeback_mode = replication_status_writeback_mode(state_update_needed);

    match persist_replication_state_if_current(
        &roi,
        &storage,
        &obj_lock_guard,
        new_replication_internal.as_ref(),
        writeback_mode,
        &mut object_info,
    )
    .await
    {
        Ok(ReplicationStatePersistOutcome::Updated) => {}
        Ok(ReplicationStatePersistOutcome::Superseded) => {
            // A tag/retention/legal-hold mutation committed while this
            // worker was in flight. Its PENDING state is authoritative and
            // must remain discoverable by the queue/MRF/scanner after a
            // crash or missed admission. Return that newer state to sync
            // callers and leave its worker to publish the terminal status.
            suppress_terminal_publication = true;
            disposition = ReplicationAttemptDisposition::Superseded;
            let read_opts = ObjectOptions {
                version_id: roi.version_id.map(|version_id| version_id.to_string()),
                ..Default::default()
            };
            match storage.get_object_info(&bucket, &object, &read_opts).await {
                Ok(current) => {
                    object_info = current;
                    merged_state = object_info.replication_state();
                    replication_status = merged_state.composite_replication_status();
                }
                Err(error) => {
                    // The CAS result is authoritative: a best-effort refetch
                    // failure must not turn a superseded task back into a
                    // retry that can publish the stale generation later.
                    debug!(
                        event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        bucket = %bucket,
                        object = %object,
                        error = %error,
                        reason = "source_snapshot_refetch_failed_after_superseded",
                        "Could not refresh source state after skipping stale replication status update"
                    );
                }
            }
            debug!(
                event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object,
                reason = "source_replication_snapshot_superseded",
                "Skipped stale replication status update"
            );
        }
        Err(e) => {
            disposition = ReplicationAttemptDisposition::Retry;
            suppress_terminal_publication = true;
            // Persisting the resynced replication status failed. Don't swallow
            // it silently — the object's on-disk status now disagrees with the
            // resync result and needs operator visibility (backlog#799 B23).
            warn!(
                event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object,
                error = %e,
                "Failed to persist resynced replication status metadata"
            );
        }
    }

    let publication = replication_terminal_publication(
        suppress_terminal_publication,
        state_update_needed,
        rinfos.replication_status(),
        roi.replication_status_internal == rinfos.replication_status_internal(),
    );

    if publication.update_transition_stats
        && let Some(stats) = runtime_sources::replication_stats()
    {
        for tgt in &rinfos.targets {
            if tgt.replication_status != tgt.prev_replication_status {
                stats
                    .update(&bucket, tgt, tgt.replication_status.clone(), tgt.prev_replication_status.clone())
                    .await;
            }
        }
    }

    if publication.emit_terminal_failure {
        note_replication_terminal_failure(&bucket, &object, version_id.as_deref(), &rinfos);
    }

    let event_name = if replication_status == ReplicationStatusType::Completed {
        EventName::ObjectReplicationComplete.to_string()
    } else {
        EventName::ObjectReplicationFailed.to_string()
    };

    if publication.emit_event {
        send_local_event(EventArgs {
            event_name,
            bucket_name: bucket.clone(),
            object: object_info,
            user_agent: "Internal: [Replication]".to_string(),
            ..Default::default()
        });
    }

    if publication.update_same_state_failure_stats
        && let Some(stats) = runtime_sources::replication_stats()
    {
        for tgt in &rinfos.targets {
            if tgt.replication_status != tgt.prev_replication_status {
                stats
                    .update(&bucket, tgt, tgt.replication_status.clone(), tgt.prev_replication_status.clone())
                    .await;
            }
        }
    }

    (merged_state, disposition)
}

/// Emit the operator-visible record of a replication attempt that ended FAILED.
///
/// Every per-branch failure log in this module is deliberately quieter than
/// `error`: most of them sit on the replication hot path and fire once per
/// object *per ARN*, so a target that stays unreachable would flood the log
/// from inside the transfer loop. That left a hole customers fell into
/// (rustfs#6825): `DEFAULT_LOG_LEVEL` is `error`, so on a stock deployment a
/// failed object produced no line at all, and an operator staring at a replica
/// that never arrived had nothing to correlate — the same trap already
/// documented for the GET path in
/// `crates/e2e_test/src/get_stream_failure_observability_test.rs`.
///
/// This is the one place that knows an object reached a *terminal* FAILED state
/// for a target, so this is where the guaranteed-visible line belongs. It is
/// bounded by the number of objects that actually fail rather than by attempts
/// inside a transfer, and it carries the target's own error so a remote
/// rejection is diagnosable without the operator first having to lower the
/// global log level and reproduce.
fn note_replication_terminal_failure(bucket: &str, object: &str, version_id: Option<&str>, rinfos: &ReplicatedInfos) {
    for target in rinfos.targets.iter() {
        if target.is_empty() {
            continue;
        }
        let replication_failed = target.replication_status == ReplicationStatusType::Failed;
        let purge_failed = target.version_purge_status == VersionPurgeStatusType::Failed;
        if !replication_failed && !purge_failed {
            continue;
        }

        error!(
            event = EVENT_REPLICATION_OBJECT_FAILED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            bucket = %bucket,
            object = %object,
            version_id = version_id.unwrap_or("-"),
            arn = %target.arn,
            endpoint = %target.endpoint,
            op_type = %target.op_type,
            size = target.size,
            replication_status = %target.replication_status.as_str(),
            version_purge_status = %target.version_purge_status.as_str(),
            // The target's error can carry a signed URL or an echoed auth
            // header, so it goes through the same redaction as the persisted
            // resync detail rather than straight into the log.
            error = %target
                .error
                .as_deref()
                .and_then(sanitize_resync_error_detail)
                .unwrap_or_else(|| "<none>".to_string()),
            "Replication failed for object"
        );
    }
}

fn unavailable_object_target_info(roi: &ReplicateObjectInfo, arn: &str) -> ReplicatedTargetInfo {
    ReplicatedTargetInfo {
        arn: arn.to_string(),
        size: roi.actual_size,
        replication_action: if roi.op_type == ReplicationType::Object {
            ReplicationAction::All
        } else {
            ReplicationAction::Metadata
        },
        op_type: roi.op_type,
        replication_status: ReplicationStatusType::Failed,
        prev_replication_status: roi.target_replication_status(arn),
        error: Some(TARGET_CLIENT_UNAVAILABLE_ERROR.to_string()),
        ..Default::default()
    }
}

trait ReplicateObjectInfoExt {
    async fn replicate_object<S: ReplicationObjectIO>(
        &self,
        storage: Arc<S>,
        tgt_client: Arc<TargetClient>,
    ) -> ReplicatedTargetInfo;
    async fn replicate_all<S: ReplicationObjectIO>(&self, storage: Arc<S>, tgt_client: Arc<TargetClient>)
    -> ReplicatedTargetInfo;
    fn to_object_info(&self) -> ObjectInfo;
}

impl ReplicateObjectInfoExt for ReplicateObjectInfo {
    async fn replicate_object<S: ReplicationObjectIO>(
        &self,
        storage: Arc<S>,
        tgt_client: Arc<TargetClient>,
    ) -> ReplicatedTargetInfo {
        let bucket = self.bucket.clone();
        let object = self.name.clone();

        let replication_action = ReplicationAction::All;
        let mut rinfo = ReplicatedTargetInfo {
            arn: tgt_client.arn.clone(),
            size: self.actual_size,
            replication_action,
            op_type: self.op_type,
            replication_status: ReplicationStatusType::Failed,
            prev_replication_status: self.target_replication_status(&tgt_client.arn),
            endpoint: tgt_client.endpoint.clone(),
            secure: tgt_client.secure,
            ..Default::default()
        };

        if ReplicationTargetStore::target_is_offline(&tgt_client).await {
            // The object is reported FAILED here, so this must be as loud as a
            // per-object put_object failure or the key never reaches the logs.
            warn!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object,
                arn = %tgt_client.arn,
                reason = "target_offline",
                endpoint = %tgt_client.to_url(),
                "Skipping replication object target"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: self.to_object_info(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

        // N2 fail-closed: never PUT SSE-C ciphertext at a target known to drop
        // the passthrough transport headers, and never trust a convergence HEAD
        // against such a target — a previous broken replica matches by ETag.
        let Some(ssec_audit_required) = resolve_ssec_passthrough_gate(self.ssec, &tgt_client, &bucket, &object, &mut rinfo).await
        else {
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: self.to_object_info(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        };

        let versioned = ReplicationVersioningStore::prefix_enabled(&bucket, &object).await;
        let version_suspended = ReplicationVersioningStore::prefix_suspended(&bucket, &object).await;

        let obj_opts = ObjectOptions {
            version_id: self.version_id.map(|v| v.to_string()),
            version_suspended,
            versioned,
            replication_request: true,
            // SSE-C passthrough reads the stored ciphertext verbatim; the
            // decrypting reader cannot serve it (no customer key server-side).
            raw_data_movement_read: self.ssec,
            ..Default::default()
        };

        let mut gr = match storage
            .get_object_reader(&bucket, &object, None, HeaderMap::new(), &obj_opts)
            .await
        {
            Ok(gr) => gr,
            Err(e) => {
                if !(is_err_object_not_found(&e) || is_err_version_not_found(&e)) {
                    debug!(
                        event = EVENT_RESYNC_RUNTIME_SKIPPED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        bucket = %bucket,
                        object = %object,
                        arn = %tgt_client.arn,
                        error = %e,
                        reason = "object_reader_unavailable",
                        "Skipping replication object target"
                    );

                    send_local_event(EventArgs {
                        event_name: EventName::ObjectReplicationNotTracked.to_string(),
                        bucket_name: bucket.clone(),
                        object: self.to_object_info(),
                        user_agent: "Internal: [Replication]".to_string(),
                        ..Default::default()
                    });
                }

                return rinfo;
            }
        };

        let object_info = gr.object_info.clone();

        rinfo.prev_replication_status = object_info.target_replication_status(&tgt_client.arn);

        let size = match object_info.get_actual_size() {
            Ok(size) => size,
            Err(e) => {
                debug!(
                    event = EVENT_RESYNC_RUNTIME_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %bucket,
                    object = %object,
                    arn = %tgt_client.arn,
                    error = %e,
                    reason = "actual_size_unavailable",
                    "Skipping replication object target"
                );
                send_local_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: object_info,
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                return rinfo;
            }
        };
        // SSE-C passthrough sends the stored ciphertext; the wire length is
        // the stored size while rinfo keeps the logical size for metering.
        let transfer_size = if self.ssec { object_info.size } else { size };

        if tgt_client.bucket.is_empty() {
            debug!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object,
                arn = %tgt_client.arn,
                reason = "target_bucket_empty",
                "Skipping replication object target"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: object_info,
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

        let mut replication_action = replication_action;
        match head_object_for_worker(tgt_client.as_ref(), &tgt_client.bucket, &object, self.version_id.map(|v| v.to_string()))
            .await
        {
            Ok(oi) => {
                replication_action = replication_action_for_target_head(&object_info, &oi, self.op_type);
                if replication_action == ReplicationAction::None {
                    // An SSE-C replica only counts as converged when the same
                    // HEAD proves its decryption material survived; a broken
                    // ciphertext copy from an earlier attempt matches by ETag.
                    if ssec_audit_required
                        && !settle_ssec_passthrough_evidence(&oi, &tgt_client, &bucket, &object, &mut rinfo).await
                    {
                        return rinfo;
                    }
                    rinfo.replication_status = ReplicationStatusType::Completed;
                    rinfo.replication_resynced = true;
                    rinfo.replication_action = ReplicationAction::None;
                    rinfo.size = size;
                    return rinfo;
                }
            }
            Err(e) => {
                if let Some(fallback) = replica_head_fallback(&tgt_client, &object, object_info.etag.as_deref(), &e).await {
                    match fallback {
                        Ok(Some(oi)) if replication_etags_match(object_info.etag.as_deref(), oi.e_tag.as_deref()) => {
                            if ssec_audit_required
                                && !settle_ssec_passthrough_evidence(&oi, &tgt_client, &bucket, &object, &mut rinfo).await
                            {
                                return rinfo;
                            }
                            rinfo.replication_status = ReplicationStatusType::Completed;
                            rinfo.replication_resynced = true;
                            rinfo.replication_action = ReplicationAction::None;
                            rinfo.size = size;
                            return rinfo;
                        }
                        Ok(_) => {}
                        Err(e2) => {
                            rinfo.error = Some(e2.to_string());
                            warn!(
                                event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                                bucket = %bucket,
                                object = %object,
                                arn = %tgt_client.arn,
                                operation = "head_object_fallback",
                                error = %e2,
                                "Replication target operation failed"
                            );
                            return rinfo;
                        }
                    }
                } else if head_object_not_found(&e) {
                    // Object not on target yet → fall through to PUT.
                } else {
                    rinfo.error = Some(e.to_string());
                    warn!(
                        event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        bucket = %bucket,
                        object = %object,
                        arn = %tgt_client.arn,
                        operation = "head_object",
                        error = %e,
                        "Replication target operation failed"
                    );
                    return rinfo;
                }
            }
        }

        rinfo.replication_status = ReplicationStatusType::Completed;
        rinfo.replication_resynced = true;
        rinfo.size = size;
        rinfo.replication_action = replication_action;

        let (put_opts, is_multipart) = match replication_put_object_options(&tgt_client.storage_class, &object_info) {
            Ok((put_opts, is_mp)) => (put_opts, is_mp),
            Err(e) => {
                // Unsupported source metadata (e.g. managed SSE) is a fail-closed
                // condition: report FAILED so the composite status and the
                // OperationFailedReplication event reflect that nothing reached
                // the target, instead of leaking the optimistic Completed above.
                rinfo.replication_status = ReplicationStatusType::Failed;
                rinfo.error = Some(e.to_string());
                warn!(
                    event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %bucket,
                    object = %object,
                    arn = %tgt_client.arn,
                    operation = "build_put_options",
                    error = %e,
                    "Replication target operation failed"
                );
                send_local_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: object_info,
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                return rinfo;
            }
        };

        if let Some(reason) = replication_single_put_size_error(is_multipart, transfer_size, object_info.etag.as_deref()) {
            drop(gr);
            rinfo.replication_status = ReplicationStatusType::Failed;
            rinfo.error = Some(reason.clone());
            warn!(
                event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                target_bucket = %tgt_client.bucket,
                arn = %tgt_client.arn,
                object = %object,
                operation = "put_object",
                transfer_size = transfer_size,
                error = %reason,
                "Replication target operation failed"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: object_info,
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

        if let Some(err) = if is_multipart {
            drop(gr);
            let result = replicate_object_with_multipart(MultipartReplicationContext {
                storage: storage.clone(),
                cli: tgt_client.clone(),
                src_bucket: &bucket,
                dst_bucket: &tgt_client.bucket,
                object: &object,
                object_info: &object_info,
                obj_opts: &obj_opts,
                arn: &rinfo.arn,
                put_opts,
            })
            .await;
            result.err()
        } else {
            gr.stream = wrap_with_bandwidth_monitor(gr.stream, &put_opts, &bucket, &rinfo.arn);
            let byte_stream = async_read_to_bytestream(gr.stream);
            let result = tgt_client
                .put_object(&tgt_client.bucket, &object, transfer_size, byte_stream, &put_opts)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
                .and_then(|response| {
                    audit_target_version_identity(
                        &tgt_client,
                        &put_opts.internal.source_version_id,
                        response.version_id.as_deref(),
                    );
                    verify_single_part_replica(&object_info, &response, obj_opts.raw_data_movement_read)
                });
            result.err()
        } {
            rinfo.replication_status = ReplicationStatusType::Failed;
            rinfo.error = Some(err.to_string());
            warn!(
                event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                target_bucket = %tgt_client.bucket,
                arn = %tgt_client.arn,
                object = %object,
                operation = "put_object",
                error = ?err,
                "Replication target operation failed"
            );

            mark_replication_target_offline_if_needed(&tgt_client, &err).await;
            return rinfo;
        }

        // First SSE-C passthrough PUT against this target: verify the replica
        // kept its decryption material before reporting COMPLETED.
        if ssec_audit_required
            && !audit_ssec_passthrough_replica(&tgt_client, &bucket, &object, self.version_id.map(|v| v.to_string()), &mut rinfo)
                .await
        {
            return rinfo;
        }

        rinfo.replication_status = ReplicationStatusType::Completed;

        rinfo
    }

    async fn replicate_all<S: ReplicationObjectIO>(
        &self,
        storage: Arc<S>,
        tgt_client: Arc<TargetClient>,
    ) -> ReplicatedTargetInfo {
        let start_time = OffsetDateTime::now_utc();

        let bucket = self.bucket.clone();
        let object = self.name.clone();

        let mut rinfo = replicate_all_target_info(self, &tgt_client);

        if ReplicationTargetStore::target_is_offline(&tgt_client).await {
            note_replicate_all_target_offline(self, &bucket, &tgt_client);
            return rinfo;
        }

        // N2 fail-closed: see the gate in `replicate_object` — the same policy
        // applies to the metadata/existing-object transport.
        let Some(ssec_audit_required) = resolve_ssec_passthrough_gate(self.ssec, &tgt_client, &bucket, &object, &mut rinfo).await
        else {
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: self.to_object_info(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
            return rinfo;
        };

        let versioned = ReplicationVersioningStore::prefix_enabled(&bucket, &object).await;
        let version_suspended = ReplicationVersioningStore::prefix_suspended(&bucket, &object).await;

        let obj_opts = replicate_all_read_options(self, versioned, version_suspended);

        let gr = match storage
            .get_object_reader(&bucket, &object, None, HeaderMap::new(), &obj_opts)
            .await
        {
            Ok(gr) => gr,
            Err(e) => {
                note_replicate_all_reader_unavailable(self, &bucket, &tgt_client, &e);
                return rinfo;
            }
        };

        let object_info = gr.object_info.clone();

        rinfo.prev_replication_status = object_info.target_replication_status(&tgt_client.arn);

        let size = match object_info.get_actual_size() {
            Ok(size) => size,
            Err(e) => {
                note_replicate_all_size_unavailable(&bucket, &tgt_client, object_info, &e);
                return rinfo;
            }
        };
        // SSE-C passthrough sends the stored ciphertext; the wire length is
        // the stored size while rinfo keeps the logical size for metering.
        let transfer_size = if self.ssec { object_info.size } else { size };

        if tgt_client.bucket.is_empty() {
            note_replicate_all_target_bucket_empty(&bucket, &tgt_client, object_info);
            return rinfo;
        }

        let _sopts = replicate_all_stat_options(&object_info, &bucket, &tgt_client);

        let Some((replication_action, object_info)) = resolve_replicate_all_action(
            ReplicateAllActionContext {
                roi: self,
                tgt_client: &tgt_client,
                bucket: &bucket,
                object: &object,
                start_time,
                ssec_audit_required,
            },
            object_info,
            &mut rinfo,
        )
        .await
        else {
            return rinfo;
        };

        rinfo.replication_status = ReplicationStatusType::Completed;
        rinfo.size = size;
        rinfo.replication_action = replication_action;

        if replication_action == ReplicationAction::None {
            // The target already holds a matching object (reached here only via
            // the version-id fallback ETag match above) — there is nothing to
            // copy. Record it as synced and return, instead of falling into the
            // metadata propagation path below, which previously left
            // AWS-style targets permanently FAILED and never converging
            // (backlog#860 / #799 B11).
            if self.op_type == ReplicationType::ExistingObject && !tgt_client.reset_id.is_empty() {
                apply_replication_resync_timestamp(&mut rinfo, &tgt_client.reset_id);
            }
            rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
            return rinfo;
        }

        // The target client has no metadata-only operation. Reuse the existing
        // object transport so metadata changes carry tags and object-lock state
        // atomically with the source version.
        let (put_opts, is_multipart) = match replication_put_object_options(&tgt_client.storage_class, &object_info) {
            Ok((put_opts, is_mp)) => (put_opts, is_mp),
            Err(e) => {
                fail_replicate_all_put_options(&mut rinfo, &tgt_client, &bucket, object_info, &e, start_time);
                return rinfo;
            }
        };

        if let Some(err) = replicate_all_payload_to_target(
            ReplicateAllPayloadContext {
                storage: &storage,
                tgt_client: &tgt_client,
                bucket: &bucket,
                object: &object,
                object_info: &object_info,
                obj_opts: &obj_opts,
                arn: &rinfo.arn,
                transfer_size,
                is_multipart,
                put_opts,
            },
            gr,
        )
        .await
        {
            fail_replicate_all_put_object(&mut rinfo, &tgt_client, &bucket, &object, &err, start_time).await;
            return rinfo;
        }

        // First SSE-C passthrough PUT against this target: verify the replica
        // kept its decryption material before reporting COMPLETED.
        if ssec_audit_required
            && !audit_ssec_passthrough_replica(&tgt_client, &bucket, &object, self.version_id.map(|v| v.to_string()), &mut rinfo)
                .await
        {
            rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
            return rinfo;
        }

        rinfo
    }

    fn to_object_info(&self) -> ObjectInfo {
        ObjectInfo {
            bucket: self.bucket.clone(),
            name: self.name.clone(),
            mod_time: self.mod_time,
            version_id: self.version_id,
            size: self.size,
            user_tags: Arc::new(self.user_tags.clone()),
            actual_size: self.actual_size,
            replication_status_internal: self.replication_status_internal.clone(),
            replication_status: self.replication_status.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            version_purge_status: self.version_purge_status.clone(),
            delete_marker: self.delete_marker,
            checksum: self.checksum.clone(),
            ..Default::default()
        }
    }
}

/// Build the initial replication outcome DTO for `replicate_all`, seeded with
/// the metadata-only action and a failed status until the target confirms
/// otherwise.
fn replicate_all_target_info(roi: &ReplicateObjectInfo, tgt_client: &TargetClient) -> ReplicatedTargetInfo {
    ReplicatedTargetInfo {
        arn: tgt_client.arn.clone(),
        size: roi.actual_size,
        replication_action: ReplicationAction::Metadata,
        op_type: roi.op_type,
        replication_status: ReplicationStatusType::Failed,
        prev_replication_status: roi.target_replication_status(&tgt_client.arn),
        endpoint: tgt_client.endpoint.clone(),
        secure: tgt_client.secure,
        ..Default::default()
    }
}

/// Log and notify that replication was skipped because the target is offline.
fn note_replicate_all_target_offline(roi: &ReplicateObjectInfo, bucket: &str, tgt_client: &TargetClient) {
    // The object is reported FAILED here, so this must be as loud as a
    // per-object put_object failure or the key never reaches the logs.
    warn!(
        event = EVENT_RESYNC_RUNTIME_SKIPPED,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
        bucket = %bucket,
        object = %roi.name,
        arn = %tgt_client.arn,
        target = %tgt_client.to_url(),
        reason = "target_offline",
        "Skipped replication because target is offline"
    );
    send_local_event(EventArgs {
        event_name: EventName::ObjectReplicationNotTracked.to_string(),
        bucket_name: bucket.to_string(),
        object: roi.to_object_info(),
        user_agent: "Internal: [Replication]".to_string(),
        ..Default::default()
    });
}

/// Build the source-side read options for `replicate_all`.
fn replicate_all_read_options(roi: &ReplicateObjectInfo, versioned: bool, version_suspended: bool) -> ObjectOptions {
    ObjectOptions {
        version_id: roi.version_id.map(|v| v.to_string()),
        version_suspended,
        versioned,
        replication_request: true,
        // SSE-C passthrough reads the stored ciphertext verbatim; the
        // decrypting reader cannot serve it (no customer key server-side).
        raw_data_movement_read: roi.ssec,
        ..Default::default()
    }
}

/// Log and notify that replication was skipped because the source object
/// reader is unavailable; missing objects/versions stay silent.
fn note_replicate_all_reader_unavailable(roi: &ReplicateObjectInfo, bucket: &str, tgt_client: &TargetClient, e: &Error) {
    if !(is_err_object_not_found(e) || is_err_version_not_found(e)) {
        debug!(
            event = EVENT_RESYNC_RUNTIME_SKIPPED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            bucket = %bucket,
            object = %roi.name,
            arn = %tgt_client.arn,
            error = %e,
            reason = "object_reader_unavailable",
            "Skipped replication because object reader is unavailable"
        );
        send_local_event(EventArgs {
            event_name: EventName::ObjectReplicationNotTracked.to_string(),
            bucket_name: bucket.to_string(),
            object: roi.to_object_info(),
            user_agent: "Internal: [Replication]".to_string(),
            ..Default::default()
        });
    }
}

/// Log and notify that replication was skipped because the actual object size
/// is unavailable.
fn note_replicate_all_size_unavailable(bucket: &str, tgt_client: &TargetClient, object_info: ObjectInfo, e: &std::io::Error) {
    debug!(
        event = EVENT_RESYNC_RUNTIME_SKIPPED,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
        bucket = %bucket,
        object = %object_info.name,
        arn = %tgt_client.arn,
        error = %e,
        reason = "actual_size_unavailable",
        "Skipped replication because actual object size is unavailable"
    );
    send_local_event(EventArgs {
        event_name: EventName::ObjectReplicationNotTracked.to_string(),
        bucket_name: bucket.to_string(),
        object: object_info,
        user_agent: "Internal: [Replication]".to_string(),
        ..Default::default()
    });
}

/// Log and notify that replication was skipped because the target bucket is
/// empty.
fn note_replicate_all_target_bucket_empty(bucket: &str, tgt_client: &TargetClient, object_info: ObjectInfo) {
    debug!(
        event = EVENT_RESYNC_RUNTIME_SKIPPED,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
        bucket = %bucket,
        object = %object_info.name,
        arn = %tgt_client.arn,
        reason = "target_bucket_empty",
        "Skipped replication because target bucket is empty"
    );
    send_local_event(EventArgs {
        event_name: EventName::ObjectReplicationNotTracked.to_string(),
        bucket_name: bucket.to_string(),
        object: object_info,
        user_agent: "Internal: [Replication]".to_string(),
        ..Default::default()
    });
}

/// Build the stat options for the target metadata comparison, logging (without
/// failing) when the tagging directive header cannot be set.
fn replicate_all_stat_options(object_info: &ObjectInfo, bucket: &str, tgt_client: &TargetClient) -> StatObjectOptions {
    let mut sopts = StatObjectOptions {
        version_id: object_info.version_id.map(|v| v.to_string()).unwrap_or_default(),
        internal: AdvancedGetOptions {
            replication_proxy_request: "false".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    if let Err(err) = sopts.set(AMZ_TAGGING_DIRECTIVE, "ACCESS") {
        debug!(
            event = EVENT_RESYNC_RUNTIME_SKIPPED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            bucket = %bucket,
            arn = %tgt_client.arn,
            error = %err,
            reason = "tagging_directive_header_invalid",
            "Skipped replication tagging directive header detail"
        );
    }

    sopts
}

/// Record a failed payload transfer: mark the outcome FAILED, log the target
/// operation failure, and take the target offline when the error is a network
/// failure.
async fn fail_replicate_all_put_object(
    rinfo: &mut ReplicatedTargetInfo,
    tgt_client: &Arc<TargetClient>,
    bucket: &str,
    object: &str,
    err: &std::io::Error,
    start_time: OffsetDateTime,
) {
    rinfo.replication_status = ReplicationStatusType::Failed;
    rinfo.error = Some(err.to_string());
    warn!(
        event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
        bucket = %bucket,
        arn = %tgt_client.arn,
        object = %object,
        operation = "put_object",
        error = ?err,
        "Replication target operation failed"
    );
    rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();

    mark_replication_target_offline_if_needed(tgt_client, err).await;
}

/// Stamp the replication outcome as resynced against the target's current
/// reset id.
fn apply_replication_resync_timestamp(rinfo: &mut ReplicatedTargetInfo, reset_id: &str) {
    rinfo.resync_timestamp = format!(
        "{};{}",
        OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "invalid-time".to_string()),
        reset_id
    );
    rinfo.replication_resynced = true;
}

/// Borrowed inputs for [`resolve_replicate_all_action`].
struct ReplicateAllActionContext<'a> {
    roi: &'a ReplicateObjectInfo,
    tgt_client: &'a Arc<TargetClient>,
    bucket: &'a str,
    object: &'a str,
    start_time: OffsetDateTime,
    /// N2: the target's SSE-C passthrough capability is still `Unknown`, so a
    /// converged-looking replica must additionally prove its SSE-C material
    /// survived before the comparison may settle COMPLETED.
    ssec_audit_required: bool,
}

/// Compare the source object against the target via HEAD and decide which
/// replication action is still required. Returns `None` after fully settling
/// `rinfo` when replication must stop here — either because the target already
/// matches or because the comparison failed.
async fn resolve_replicate_all_action(
    ctx: ReplicateAllActionContext<'_>,
    object_info: ObjectInfo,
    rinfo: &mut ReplicatedTargetInfo,
) -> Option<(ReplicationAction, ObjectInfo)> {
    let ReplicateAllActionContext {
        roi,
        tgt_client,
        bucket,
        object,
        start_time,
        ssec_audit_required,
    } = ctx;
    let require_existing_target = metadata_requires_existing_target(roi.op_type, &object_info);
    let replication_action;
    match head_object_for_worker(tgt_client.as_ref(), &tgt_client.bucket, object, roi.version_id.map(|v| v.to_string())).await {
        Ok(oi) => {
            replication_action = replication_action_for_target_head(&object_info, &oi, roi.op_type);
            rinfo.replication_status = ReplicationStatusType::Completed;
            if replication_action == ReplicationAction::None {
                // An SSE-C replica only counts as converged when the same HEAD
                // proves its decryption material survived; a broken ciphertext
                // copy from an earlier attempt matches by ETag.
                if ssec_audit_required && !settle_ssec_passthrough_evidence(&oi, tgt_client, bucket, object, rinfo).await {
                    rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                    return None;
                }
                if roi.op_type == ReplicationType::ExistingObject
                    && replication_target_head_is_newer_null_version(&object_info, &oi)
                {
                    warn!(
                        event = EVENT_RESYNC_RUNTIME_SKIPPED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        bucket = %bucket,
                        object = %object,
                        arn = %tgt_client.arn,
                        endpoint = %tgt_client.to_url(),
                        reason = "target_newer_than_source_null_version",
                        "Skipping replication because newer target version exists"
                    );
                    send_local_event(EventArgs {
                        event_name: EventName::ObjectReplicationNotTracked.to_string(),
                        bucket_name: bucket.to_string(),
                        object: object_info.clone(),
                        user_agent: "Internal: [Replication]".to_string(),
                        ..Default::default()
                    });
                }

                if object_info.target_replication_status(&tgt_client.arn) == ReplicationStatusType::Pending
                    || object_info.target_replication_status(&tgt_client.arn) == ReplicationStatusType::Failed
                    || roi.op_type == ReplicationType::ExistingObject
                {
                    rinfo.replication_action = replication_action;
                    rinfo.replication_status = ReplicationStatusType::Completed;
                }

                if rinfo.replication_status == ReplicationStatusType::Completed
                    && roi.op_type == ReplicationType::ExistingObject
                    && !tgt_client.reset_id.is_empty()
                {
                    apply_replication_resync_timestamp(rinfo, &tgt_client.reset_id);
                }

                rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();

                return None;
            }
        }
        Err(e) => {
            if let Some(fallback) = replica_head_fallback(tgt_client, object, object_info.etag.as_deref(), &e).await {
                match fallback {
                    Ok(Some(oi)) => {
                        let etags_match = replication_etags_match(object_info.etag.as_deref(), oi.e_tag.as_deref());
                        if require_existing_target && !etags_match {
                            rinfo.error = Some("replica metadata target does not contain matching object data".to_string());
                            rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                            return None;
                        }
                        replication_action = if etags_match {
                            if ssec_audit_required
                                && !settle_ssec_passthrough_evidence(&oi, tgt_client, bucket, object, rinfo).await
                            {
                                rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                                return None;
                            }
                            ReplicationAction::None
                        } else {
                            ReplicationAction::All
                        };
                    }
                    Ok(None) => {
                        if require_existing_target {
                            rinfo.error = Some("replica metadata target does not contain this object version".to_string());
                            rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                            return None;
                        }
                        replication_action = ReplicationAction::All;
                    }
                    Err(e2) => {
                        rinfo.error = Some(e2.to_string());
                        debug!(
                            event = EVENT_RESYNC_RUNTIME_SKIPPED,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                            bucket = %bucket,
                            object = %object,
                            arn = %tgt_client.arn,
                            error = %e2,
                            reason = "head_object_fallback_failed",
                            "Failed replication head-object fallback"
                        );
                        send_local_event(EventArgs {
                            event_name: EventName::ObjectReplicationNotTracked.to_string(),
                            bucket_name: bucket.to_string(),
                            object: object_info,
                            user_agent: "Internal: [Replication]".to_string(),
                            ..Default::default()
                        });
                        rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                        return None;
                    }
                }
            } else if head_object_not_found(&e) {
                if require_existing_target {
                    rinfo.error = Some("replica metadata target does not contain this object version".to_string());
                    rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                    return None;
                }
                replication_action = ReplicationAction::All;
            } else {
                rinfo.error = Some(e.to_string());
                debug!(
                    event = EVENT_RESYNC_RUNTIME_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %bucket,
                    object = %object,
                    arn = %tgt_client.arn,
                    error = %e,
                    reason = "head_object_failed",
                    "Skipped replication because head-object failed"
                );

                send_local_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.to_string(),
                    object: object_info,
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });

                rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                return None;
            }
        }
    };

    Some((replication_action, object_info))
}

/// Record a fail-closed put-options failure.
/// Unsupported source metadata (e.g. managed SSE) is a fail-closed
/// condition: report FAILED so the composite status and the
/// OperationFailedReplication event reflect that nothing reached
/// the target, instead of leaking the optimistic Completed set earlier.
fn fail_replicate_all_put_options(
    rinfo: &mut ReplicatedTargetInfo,
    tgt_client: &TargetClient,
    bucket: &str,
    object_info: ObjectInfo,
    e: &Error,
    start_time: OffsetDateTime,
) {
    rinfo.replication_status = ReplicationStatusType::Failed;
    rinfo.error = Some(e.to_string());
    warn!(
        event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
        bucket = %bucket,
        object = %object_info.name,
        arn = %tgt_client.arn,
        operation = "build_put_options",
        error = %e,
        "Replication target operation failed"
    );
    send_local_event(EventArgs {
        event_name: EventName::ObjectReplicationNotTracked.to_string(),
        bucket_name: bucket.to_string(),
        object: object_info,
        user_agent: "Internal: [Replication]".to_string(),
        ..Default::default()
    });

    rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
}

/// Borrowed inputs shared by both transports of the `replicate_all` payload
/// transfer step.
struct ReplicateAllPayloadContext<'a, S: ReplicationObjectIO> {
    storage: &'a Arc<S>,
    tgt_client: &'a Arc<TargetClient>,
    bucket: &'a str,
    object: &'a str,
    object_info: &'a ObjectInfo,
    obj_opts: &'a ObjectOptions,
    arn: &'a str,
    transfer_size: i64,
    is_multipart: bool,
    put_opts: PutObjectOptions,
}

/// Ship the object payload to the replication target over the multipart or
/// single-put transport, returning the transport error when the upload fails.
async fn replicate_all_payload_to_target<S: ReplicationObjectIO>(
    ctx: ReplicateAllPayloadContext<'_, S>,
    mut gr: GetObjectReader,
) -> Option<std::io::Error> {
    // Fail before streaming a body the target is required to reject: an S3
    // PutObject caps at 5 GiB, and this route is chosen by the source object's
    // storage shape rather than its size (rustfs#6825).
    if let Some(reason) = replication_single_put_size_error(ctx.is_multipart, ctx.transfer_size, ctx.object_info.etag.as_deref())
    {
        drop(gr);
        return Some(std::io::Error::other(reason));
    }

    if ctx.is_multipart {
        drop(gr);
        let result = replicate_object_with_multipart(MultipartReplicationContext {
            storage: ctx.storage.clone(),
            cli: ctx.tgt_client.clone(),
            src_bucket: ctx.bucket,
            dst_bucket: &ctx.tgt_client.bucket,
            object: ctx.object,
            object_info: ctx.object_info,
            obj_opts: ctx.obj_opts,
            arn: ctx.arn,
            put_opts: ctx.put_opts,
        })
        .await;
        result.err()
    } else {
        gr.stream = wrap_with_bandwidth_monitor(gr.stream, &ctx.put_opts, ctx.bucket, ctx.arn);
        let byte_stream = async_read_to_bytestream(gr.stream);
        let result = ctx
            .tgt_client
            .put_object(&ctx.tgt_client.bucket, ctx.object, ctx.transfer_size, byte_stream, &ctx.put_opts)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
            .and_then(|response| {
                audit_target_version_identity(
                    ctx.tgt_client,
                    &ctx.put_opts.internal.source_version_id,
                    response.version_id.as_deref(),
                );
                verify_single_part_replica(ctx.object_info, &response, ctx.obj_opts.raw_data_movement_read)
            });
        result.err()
    }
}

fn wrap_with_bandwidth_monitor_with_header(
    stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    bucket: &str,
    arn: &str,
    header_size: usize,
) -> Box<dyn AsyncRead + Unpin + Send + Sync> {
    if let Some(monitor) = runtime_sources::bucket_monitor() {
        replication_bandwidth_boundary::wrap_reader(stream, monitor, bucket, arn, header_size)
    } else {
        WARNED_MONITOR_UNINIT.call_once(|| {
            warn!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                reason = "bucket_monitor_uninitialized",
                "Skipping replication bandwidth monitor because global bucket monitor is uninitialized"
            )
        });
        stream
    }
}

fn wrap_with_bandwidth_monitor(
    stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    put_opts: &PutObjectOptions,
    bucket: &str,
    arn: &str,
) -> Box<dyn AsyncRead + Unpin + Send + Sync> {
    let header_size = replication_put_object_header_size(put_opts);
    wrap_with_bandwidth_monitor_with_header(stream, bucket, arn, header_size)
}

fn async_read_to_bytestream(reader: impl AsyncRead + Send + Sync + Unpin + 'static) -> ByteStream {
    // Non-retryable: SDK-level retries are not supported for streaming bodies.
    // Replication-level retry handles failures at a higher layer.
    let stream = ReaderStream::new(reader);
    let body = StreamBody::new(stream.map(|r| r.map(Frame::data)));
    ByteStream::new(SdkBody::from_body_1_x(body))
}

struct MultipartReplicationContext<'a, S: ReplicationObjectIO> {
    storage: Arc<S>,
    cli: Arc<TargetClient>,
    src_bucket: &'a str,
    dst_bucket: &'a str,
    object: &'a str,
    object_info: &'a ObjectInfo,
    obj_opts: &'a ObjectOptions,
    arn: &'a str,
    put_opts: PutObjectOptions,
}

async fn replicate_object_with_multipart<S: ReplicationObjectIO>(ctx: MultipartReplicationContext<'_, S>) -> std::io::Result<()> {
    let mut attempts = 1;
    let upload_id = loop {
        match ctx
            .cli
            .create_multipart_upload(ctx.dst_bucket, ctx.object, &ctx.put_opts)
            .await
        {
            Ok(id) => {
                break id;
            }
            Err(e) => {
                attempts += 1;
                if attempts > 3 {
                    return Err(std::io::Error::other(e.to_string()));
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                continue;
            }
        }
    };

    let cli = ctx.cli.clone();
    let dst_bucket = ctx.dst_bucket;
    let object = ctx.object;
    let arn = ctx.arn;

    let result = replicate_multipart_parts_and_complete(ctx, &upload_id).await;
    abort_multipart_on_failure(
        result,
        dst_bucket,
        object,
        &upload_id,
        arn,
        || async { cli.abort_multipart_upload(dst_bucket, object, &upload_id).await },
        || {
            schedule_replication_abort_retry(
                cli.clone(),
                dst_bucket.to_string(),
                object.to_string(),
                upload_id.clone(),
                arn.to_string(),
            )
        },
    )
    .await
}

const REPLICATION_ABORT_RETRY_ATTEMPTS: u32 = 5;
const REPLICATION_ABORT_RETRY_INITIAL_DELAY_SECS: u64 = 30;

/// The immediate abort usually fails for the same reason the transfer did —
/// the target is unreachable — and MRF only retries the *object*: every replay
/// mints a fresh upload id, so a failed abort would leak its upload on the
/// target forever (#6854). Retry the abort on a detached, bounded backoff
/// (~30s..8m) so it lands once the target comes back; an upload the target no
/// longer knows counts as cleaned up.
fn schedule_replication_abort_retry(cli: Arc<TargetClient>, dst_bucket: String, object: String, upload_id: String, arn: String) {
    tokio::spawn(async move {
        let mut delay_secs = REPLICATION_ABORT_RETRY_INITIAL_DELAY_SECS;
        for attempt in 1..=REPLICATION_ABORT_RETRY_ATTEMPTS {
            tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
            delay_secs = delay_secs.saturating_mul(2);

            match cli.abort_multipart_upload(&dst_bucket, &object, &upload_id).await {
                Ok(()) => {
                    info!(
                        event = EVENT_REPLICATION_ABORT_RETRY_RESOLVED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        target_bucket = %dst_bucket,
                        object = %object,
                        arn = %arn,
                        upload_id = %upload_id,
                        operation = "abort_multipart_upload_retry",
                        attempt,
                        "Replication abort retry cleaned up the orphaned upload"
                    );
                    return;
                }
                Err(err) if target_upload_already_removed(&err) => {
                    info!(
                        event = EVENT_REPLICATION_ABORT_RETRY_RESOLVED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        target_bucket = %dst_bucket,
                        object = %object,
                        arn = %arn,
                        upload_id = %upload_id,
                        operation = "abort_multipart_upload_retry",
                        attempt,
                        "Replication abort retry found the upload already removed"
                    );
                    return;
                }
                Err(err) => {
                    warn!(
                        event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        target_bucket = %dst_bucket,
                        object = %object,
                        arn = %arn,
                        upload_id = %upload_id,
                        operation = "abort_multipart_upload_retry",
                        attempt,
                        error = %err,
                        "Replication target operation failed"
                    );
                }
            }
        }

        // Terminal: the upload id stays in the log so an operator can reap it
        // with list-multipart-uploads/abort by hand (the #6840 contract).
        warn!(
            event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            target_bucket = %dst_bucket,
            object = %object,
            arn = %arn,
            upload_id = %upload_id,
            operation = "abort_multipart_upload_retry",
            result = "gave_up",
            "Replication abort retries exhausted; the incomplete upload remains on the target"
        );
    });
}

/// AWS answers an abort for an unknown upload with `NoSuchUpload`; that means
/// the orphan is gone (aborted elsewhere or expired), which is the goal state.
fn target_upload_already_removed(err: &S3ClientError) -> bool {
    err.code.as_deref() == Some("NoSuchUpload")
}

/// Best-effort abort of the target-side multipart upload once the transfer has
/// failed past CreateMultipartUpload; without it every failed attempt leaves an
/// invisible incomplete upload on the target that keeps billing for its parts.
/// The abort outcome never replaces the transfer error: an abort failure is
/// only logged and `result` is returned as-is.
async fn abort_multipart_on_failure<F, Fut, R>(
    result: std::io::Result<()>,
    dst_bucket: &str,
    object: &str,
    upload_id: &str,
    arn: &str,
    abort: F,
    schedule_abort_retry: R,
) -> std::io::Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), S3ClientError>>,
    R: FnOnce(),
{
    if result.is_ok() {
        return result;
    }
    if let Err(abort_err) = abort().await {
        warn!(
            event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            target_bucket = %dst_bucket,
            object = %object,
            arn = %arn,
            upload_id = %upload_id,
            operation = "abort_multipart_upload",
            error = %abort_err,
            "Replication target operation failed"
        );
        if !target_upload_already_removed(&abort_err) {
            schedule_abort_retry();
        }
    }
    result
}

#[derive(Debug)]
struct MultipartReplicationReadPlan {
    part_number: i32,
    part_size: i64,
    range: Option<HTTPRangeSpec>,
    next_offset: i64,
}

fn multipart_replication_read_plan(
    object_info: &ObjectInfo,
    obj_opts: &ObjectOptions,
    mut input: ReplicationMultipartPartInput,
    stored_size: usize,
    is_last: bool,
) -> std::io::Result<MultipartReplicationReadPlan> {
    let empty_last_part = is_last && input.part_size == 0 && stored_size == 0;
    // Raw reads address stored bytes. Only untransformed legacy parts may
    // substitute their stored size for a missing logical size.
    if obj_opts.raw_data_movement_read || (input.part_size == 0 && !object_info.is_compressed() && !object_info.is_encrypted()) {
        input.part_size = i64::try_from(stored_size).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "multipart replication stored part size exceeds i64")
        })?;
    }
    if empty_last_part {
        if input.offset < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "empty multipart replication part has a negative offset",
            ));
        }
        let part_number = i32::try_from(input.part_number)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "multipart replication part number exceeds i32"))?;
        return Ok(MultipartReplicationReadPlan {
            part_number,
            part_size: 0,
            range: None,
            next_offset: input.offset,
        });
    }
    let plan = replication_multipart_part_plan(input).map_err(std::io::Error::other)?;
    Ok(MultipartReplicationReadPlan {
        part_number: plan.part_number,
        part_size: plan.part_size,
        range: Some(HTTPRangeSpec {
            is_suffix_length: false,
            start: plan.range.start,
            end: plan.range.end,
        }),
        next_offset: plan.next_offset,
    })
}

async fn replicate_multipart_parts_and_complete<S: ReplicationObjectIO>(
    ctx: MultipartReplicationContext<'_, S>,
    upload_id: &str,
) -> std::io::Result<()> {
    let MultipartReplicationContext {
        storage,
        cli,
        src_bucket,
        dst_bucket,
        object,
        object_info,
        obj_opts,
        arn,
        put_opts,
    } = ctx;

    let mut uploaded_parts: Vec<CompletedPart> = Vec::new();

    let mut header_size = replication_put_object_header_size(&put_opts);
    let mut offset: i64 = 0;
    for (index, part_info) in object_info.parts.iter().enumerate() {
        let part_plan = multipart_replication_read_plan(
            object_info,
            obj_opts,
            ReplicationMultipartPartInput {
                offset,
                part_number: part_info.number,
                part_size: part_info.actual_size,
            },
            part_info.size,
            index + 1 == object_info.parts.len(),
        )?;
        offset = part_plan.next_offset;

        let byte_stream = if let Some(range_spec) = part_plan.range {
            let part_reader = storage
                .get_object_reader(src_bucket, object, Some(range_spec), HeaderMap::new(), obj_opts)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let part_stream = wrap_with_bandwidth_monitor_with_header(part_reader.stream, src_bucket, arn, header_size);
            async_read_to_bytestream(part_stream)
        } else {
            ByteStream::from_static(b"")
        };
        header_size = 0;

        let object_part = cli
            .put_object_part(
                dst_bucket,
                object,
                upload_id,
                part_plan.part_number,
                part_plan.part_size,
                byte_stream,
                &PutObjectPartOptions { ..Default::default() },
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let etag = object_part.e_tag.unwrap_or_default();

        uploaded_parts.push(
            CompletedPart::builder()
                .part_number(part_plan.part_number)
                .e_tag(etag)
                .build(),
        );
    }

    let actual_size = replication_multipart_complete_actual_size(&object_info.user_defined);

    let completed = cli
        .complete_multipart_upload(
            dst_bucket,
            object,
            upload_id,
            uploaded_parts,
            &replication_complete_multipart_options(
                actual_size,
                object_info.etag.clone().unwrap_or_default(),
                object_info.mod_time,
                &put_opts.internal,
            ),
        )
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    // Multipart decides the target version at initiate time and only reveals
    // it on completion, so this is where the identity contract is observable
    // for this path. A target can mirror PutObject version ids and still mint
    // its own here, which would leave multipart deletes and heals addressing
    // a version that never existed.
    audit_target_version_identity(&cli, &put_opts.internal.source_version_id, completed.version_id());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::replication_filemeta_boundary::ReplicateTargetDecision;
    use super::super::replication_object_decision_boundary::ReplicationMultipartPlanError;

    #[test]
    fn multipart_read_plan_preserves_legacy_plain_part_ranges() {
        const MIB: usize = 1024 * 1024;
        let object_info = ObjectInfo {
            etag: Some("0123456789abcdef0123456789abcdef".to_string()),
            size: 6 * 1024 * 1024,
            ..Default::default()
        };
        let mut offset = 0;
        for (part_number, stored_size, start, end) in [
            (1, 5 * MIB, 0, 5 * 1024 * 1024 - 1),
            (2, MIB, 5 * 1024 * 1024, 6 * 1024 * 1024 - 1),
        ] {
            let plan = multipart_replication_read_plan(
                &object_info,
                &ObjectOptions::default(),
                ReplicationMultipartPartInput {
                    offset,
                    part_number,
                    part_size: 0,
                },
                stored_size,
                part_number == 2,
            )
            .expect("legacy plain parts must use their stored sizes");
            assert_eq!(plan.part_number, i32::try_from(part_number).expect("part number fits"));
            assert_eq!(plan.part_size, i64::try_from(stored_size).expect("stored size fits"));
            let range = plan.range.expect("a nonempty part must read a range");
            assert!(!range.is_suffix_length);
            assert_eq!((range.start, range.end), (start, end));
            assert_eq!(plan.next_offset, end + 1);
            offset = plan.next_offset;
        }
        assert_eq!(offset, object_info.size);
    }

    #[test]
    fn multipart_read_plan_distinguishes_transformed_and_raw_sizes() {
        for metadata in [
            HashMap::from([("x-rustfs-internal-compression".to_string(), "klauspost/compress/s2".to_string())]),
            HashMap::from([("x-amz-server-side-encryption".to_string(), "AES256".to_string())]),
        ] {
            let object_info = ObjectInfo {
                user_defined: Arc::new(metadata),
                ..Default::default()
            };
            assert!(object_info.is_compressed() || object_info.is_encrypted());
            for raw in [false, true] {
                for actual_size in [-1, 0, 5] {
                    let result = multipart_replication_read_plan(
                        &object_info,
                        &ObjectOptions {
                            raw_data_movement_read: raw,
                            ..Default::default()
                        },
                        ReplicationMultipartPartInput {
                            offset: 7,
                            part_number: 2,
                            part_size: actual_size,
                        },
                        9,
                        true,
                    );
                    if !raw && actual_size <= 0 {
                        let err = result.expect_err("transformed reads cannot substitute physical bytes for unknown plaintext");
                        assert!(matches!(
                            err.get_ref().and_then(|err| err.downcast_ref::<ReplicationMultipartPlanError>()),
                            Some(ReplicationMultipartPlanError::InvalidPartSize { part_size })
                                if *part_size == actual_size
                        ));
                    } else {
                        let plan = result.expect("the selected representation has a known positive size");
                        let expected_size = if raw { 9 } else { 5 };
                        assert_eq!(plan.part_number, 2);
                        assert_eq!(plan.part_size, expected_size);
                        let range = plan.range.expect("a nonempty part must read a range");
                        assert_eq!((range.start, range.end), (7, 7 + expected_size - 1));
                        assert_eq!(plan.next_offset, 7 + expected_size);
                    }
                }
            }
        }
    }

    #[test]
    fn multipart_read_plan_retains_an_empty_last_part_without_advancing() {
        for offset in [5 * 1024 * 1024, i64::MAX] {
            for raw in [false, true] {
                let plan = multipart_replication_read_plan(
                    &ObjectInfo::default(),
                    &ObjectOptions {
                        raw_data_movement_read: raw,
                        ..Default::default()
                    },
                    ReplicationMultipartPartInput {
                        offset,
                        part_number: 2,
                        part_size: 0,
                    },
                    0,
                    true,
                )
                .expect("an empty final part needs no range read");
                assert_eq!(plan.part_number, 2);
                assert_eq!(plan.part_size, 0);
                assert!(plan.range.is_none());
                assert_eq!(plan.next_offset, offset);
            }
        }
    }

    #[test]
    fn multipart_read_plan_rejects_invalid_empty_parts_and_ranges() {
        for (offset, part_number, actual_size, stored_size, is_last) in [
            (0, 1, 0, 0, false),
            (0, 2, -1, 0, true),
            (0, 2, -1, 9, true),
            (-1, 2, 0, 0, true),
            (0, usize::try_from(i32::MAX).expect("i32 fits usize") + 1, 0, 0, true),
            (i64::MAX, 2, 1, 1, true),
            (i64::MAX, 2, 2, 2, true),
        ] {
            let err = multipart_replication_read_plan(
                &ObjectInfo::default(),
                &ObjectOptions::default(),
                ReplicationMultipartPartInput {
                    offset,
                    part_number,
                    part_size: actual_size,
                },
                stored_size,
                is_last,
            )
            .expect_err("invalid part metadata must not become a successful transport plan");
            assert!(
                err.kind() == std::io::ErrorKind::InvalidData
                    || err.get_ref().is_some_and(|err| { err.is::<ReplicationMultipartPlanError>() }),
                "the failure must preserve a typed metadata or planner error: {err}"
            );
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn multipart_read_plan_rejects_physical_size_overflow() {
        for raw in [false, true] {
            let err = multipart_replication_read_plan(
                &ObjectInfo::default(),
                &ObjectOptions {
                    raw_data_movement_read: raw,
                    ..Default::default()
                },
                ReplicationMultipartPartInput {
                    offset: 0,
                    part_number: 1,
                    part_size: 0,
                },
                usize::MAX,
                true,
            )
            .expect_err("a physical size outside the range API must be rejected before casting");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(err.to_string(), "multipart replication stored part size exceeds i64");
        }
    }

    #[test]
    fn same_state_terminal_retry_uses_validate_only() {
        assert_eq!(replication_status_writeback_mode(false), ReplicationStatusWritebackMode::ValidateOnly);
        assert_eq!(replication_status_writeback_mode(true), ReplicationStatusWritebackMode::Update);
    }

    #[test]
    fn superseded_attempt_has_no_terminal_publication_side_effects() {
        assert!(ReplicationAttemptDisposition::Persisted.consumes_mrf_entry());
        assert!(ReplicationAttemptDisposition::Superseded.consumes_mrf_entry());
        assert!(!ReplicationAttemptDisposition::Retry.consumes_mrf_entry());
        assert_eq!(
            replication_terminal_publication(true, false, ReplicationStatusType::Failed, true),
            ReplicationTerminalPublication::default()
        );
        assert_eq!(
            replication_terminal_publication(true, true, ReplicationStatusType::Completed, false),
            ReplicationTerminalPublication::default()
        );

        let current = replication_terminal_publication(false, false, ReplicationStatusType::Failed, true);
        assert!(current.emit_terminal_failure);
        assert!(current.emit_event);
        assert!(!current.update_transition_stats);
        assert!(current.update_same_state_failure_stats);

        assert_eq!(
            replication_terminal_publication(true, true, ReplicationStatusType::Failed, false),
            ReplicationTerminalPublication::default(),
            "a retryable status-persistence failure must not publish a terminal result"
        );
    }

    #[test]
    fn invalid_generation_retries_before_remote_replication() {
        let mut preserved_state = ReplicationState::default();
        preserved_state
            .targets
            .insert("arn:target".to_string(), ReplicationStatusType::Pending);
        let invalid = ReplicateObjectInfo {
            replication_generation: ReplicationGenerationSnapshot {
                invalid: true,
                ..Default::default()
            },
            replication_state: Some(preserved_state.clone()),
            ..Default::default()
        };

        assert_eq!(
            replication_attempt_preflight(&invalid),
            Some((preserved_state, ReplicationAttemptDisposition::Retry))
        );
        assert!(replication_attempt_preflight(&ReplicateObjectInfo::default()).is_none());
    }

    #[tokio::test]
    async fn terminal_writeback_carries_replication_lock_loss_fence() {
        let lock = rustfs_lock::NamespaceLock::new(
            "replication-status-writeback-fence".to_string(),
            Arc::new(rustfs_lock::LocalClient::new()),
        );
        let guard = lock
            .get_write_lock(
                rustfs_lock::ObjectKey::new("bucket", "/[replicate]/object"),
                "worker-a",
                std::time::Duration::from_secs(2),
            )
            .await
            .expect("replication lock should be acquired");
        let signal = guard
            .lock_lost_signal()
            .expect("distributed guard must expose its loss signal");
        let forced_lost = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let _test_fence = NamespaceLockSignalTestFence::install_with_loss_handle(&signal, Arc::clone(&forced_lost));

        // Build the writeback after remote work has acquired the guard, then
        // lose the lease before storage reaches its commit fence.
        let opts = replication_status_writeback_options(
            &ReplicateObjectInfo::default(),
            &guard,
            None,
            ReplicationStatusWritebackMode::ValidateOnly,
        );
        forced_lost.store(true, std::sync::atomic::Ordering::Release);

        assert!(
            opts.namespace_lock_fence
                .as_ref()
                .is_some_and(NamespaceLockFence::is_lock_lost),
            "terminal CAS must observe a replication lease lost after remote I/O"
        );
        assert!(!ReplicationAttemptDisposition::Retry.consumes_mrf_entry());
        assert_eq!(
            replication_terminal_publication(true, true, ReplicationStatusType::Failed, false),
            ReplicationTerminalPublication::default(),
            "a fenced writeback retry must not publish terminal events or statistics"
        );
    }

    #[test]
    fn unavailable_object_target_is_persisted_as_failed() {
        let arn = "arn:object-target";
        let roi = ReplicateObjectInfo {
            actual_size: 42,
            op_type: ReplicationType::Object,
            replication_status_internal: Some(format!("{arn}=PENDING;")),
            ..Default::default()
        };

        let target_info = unavailable_object_target_info(&roi, arn);
        let merged = get_replication_state(
            &ReplicatedInfos {
                replication_timestamp: Some(OffsetDateTime::now_utc()),
                targets: vec![target_info.clone()],
            },
            &ReplicationState::default(),
            None,
        );

        assert_eq!(target_info.replication_status, ReplicationStatusType::Failed);
        assert_eq!(target_info.prev_replication_status, ReplicationStatusType::Pending);
        assert_eq!(target_info.replication_action, ReplicationAction::All);
        assert_eq!(target_info.error.as_deref(), Some(TARGET_CLIENT_UNAVAILABLE_ERROR));
        assert_eq!(merged.targets.get(arn), Some(&ReplicationStatusType::Failed));
    }

    #[test]
    fn unavailable_delete_target_is_failed_without_overwriting_completed_state() {
        let arn = "arn:delete-target";
        let mut previous_state = ReplicationState::default();
        previous_state.targets.insert(arn.to_string(), ReplicationStatusType::Pending);
        let mut dobj = DeletedObjectReplicationInfo {
            delete_object: ReplicationDeletedObject {
                delete_marker: true,
                replication_state: Some(previous_state),
                ..Default::default()
            },
            op_type: ReplicationType::Delete,
            ..Default::default()
        };

        let failed = unavailable_delete_target_info(&dobj, arn);
        assert_eq!(failed.replication_status, ReplicationStatusType::Failed);
        assert_eq!(failed.prev_replication_status, ReplicationStatusType::Pending);
        assert_eq!(failed.error.as_deref(), Some(TARGET_CLIENT_UNAVAILABLE_ERROR));

        dobj.delete_object
            .replication_state
            .as_mut()
            .expect("previous state should exist")
            .targets
            .insert(arn.to_string(), ReplicationStatusType::Completed);
        let completed = unavailable_delete_target_info(&dobj, arn);
        assert_eq!(completed.replication_status, ReplicationStatusType::Completed);
        assert!(completed.error.is_none());
    }

    #[test]
    fn unavailable_version_purge_target_is_persisted_as_failed() {
        let arn = "arn:purge-target";
        let mut previous_state = ReplicationState::default();
        previous_state
            .purge_targets
            .insert(arn.to_string(), VersionPurgeStatusType::Pending);
        let dobj = DeletedObjectReplicationInfo {
            delete_object: ReplicationDeletedObject {
                version_id: Some(Uuid::new_v4()),
                replication_state: Some(previous_state),
                ..Default::default()
            },
            op_type: ReplicationType::Delete,
            ..Default::default()
        };

        let target_info = unavailable_delete_target_info(&dobj, arn);

        assert_eq!(target_info.version_purge_status, VersionPurgeStatusType::Failed);
        assert_eq!(target_info.error.as_deref(), Some(TARGET_CLIENT_UNAVAILABLE_ERROR));
    }

    fn resync_target_state(resync_id: &str, status: ResyncStatusType, replicated_count: i64) -> TargetReplicationResyncStatus {
        TargetReplicationResyncStatus {
            resync_id: resync_id.to_string(),
            resync_status: status,
            replicated_count,
            ..Default::default()
        }
    }

    /// Periodic-saver merge: fresher local progress overlays the same run,
    /// but a terminal state persisted by another node must stick, a newer
    /// admission elsewhere is kept, and locally-known targets are added.
    #[test]
    fn merge_local_resync_keeps_peer_terminal_and_newer_states() {
        let mut persisted = BucketReplicationResyncStatus::new();
        persisted.targets_map.insert(
            "arn:same-run".to_string(),
            resync_target_state("run-1", ResyncStatusType::ResyncStarted, 1),
        );
        persisted.targets_map.insert(
            "arn:canceled".to_string(),
            resync_target_state("run-1", ResyncStatusType::ResyncCanceled, 0),
        );
        persisted.targets_map.insert(
            "arn:new-run".to_string(),
            resync_target_state("run-2", ResyncStatusType::ResyncPending, 0),
        );

        let mut local = BucketReplicationResyncStatus::new();
        local.targets_map.insert(
            "arn:same-run".to_string(),
            resync_target_state("run-1", ResyncStatusType::ResyncStarted, 9),
        );
        local.targets_map.insert(
            "arn:canceled".to_string(),
            resync_target_state("run-1", ResyncStatusType::ResyncPending, 0),
        );
        local.targets_map.insert(
            "arn:new-run".to_string(),
            resync_target_state("run-1", ResyncStatusType::ResyncStarted, 3),
        );
        local.targets_map.insert(
            "arn:local-only".to_string(),
            resync_target_state("run-1", ResyncStatusType::ResyncPending, 0),
        );
        local.last_update = Some(OffsetDateTime::now_utc());

        assert!(merge_local_resync_into_persisted(&mut persisted, &local));
        assert_eq!(persisted.targets_map["arn:same-run"].replicated_count, 9, "fresher local progress wins");
        assert_eq!(
            persisted.targets_map["arn:canceled"].resync_status,
            ResyncStatusType::ResyncCanceled,
            "peer terminal state must stick"
        );
        assert_eq!(
            persisted.targets_map["arn:new-run"].resync_id, "run-2",
            "newer admission elsewhere is kept"
        );
        assert!(persisted.targets_map.contains_key("arn:local-only"));
        assert_eq!(persisted.last_update, local.last_update);
    }

    /// A terminal local state for the same run (completion/failure recorded by
    /// this node) still overlays a non-terminal persisted state.
    #[test]
    fn merge_local_resync_reports_no_change_when_maps_agree() {
        let mut persisted = BucketReplicationResyncStatus::new();
        persisted
            .targets_map
            .insert("arn:same".to_string(), resync_target_state("run-1", ResyncStatusType::ResyncStarted, 5));
        let local = persisted.clone();
        assert!(!merge_local_resync_into_persisted(&mut persisted, &local));

        let mut local = local.clone();
        local
            .targets_map
            .insert("arn:same".to_string(), resync_target_state("run-1", ResyncStatusType::ResyncCompleted, 5));
        assert!(merge_local_resync_into_persisted(&mut persisted, &local));
        assert_eq!(persisted.targets_map["arn:same"].resync_status, ResyncStatusType::ResyncCompleted);
    }

    use super::super::replication_target_boundary::{BucketTarget, BucketTargets};
    use super::*;
    use s3s::dto::{
        BucketVersioningStatus, DeleteReplication, DeleteReplicationStatus, Destination, ExcludedPrefix, ReplicationRule,
        ReplicationRuleStatus, VersioningConfiguration,
    };
    use std::collections::HashMap;
    use time::OffsetDateTime;
    use uuid::Uuid;

    fn test_target_client(endpoint: String) -> Arc<TargetClient> {
        let config = aws_sdk_s3::Config::builder()
            .endpoint_url(endpoint.clone())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::SharedCredentialsProvider::new(
                aws_credential_types::Credentials::new("access", "secret", None, None, "test"),
            ))
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .build();

        Arc::new(TargetClient {
            endpoint,
            credentials: None,
            bucket: "target-bucket".to_string(),
            storage_class: String::new(),
            disable_proxy: false,
            arn: format!("arn:rustfs:replication:us-east-1:target:{}", Uuid::new_v4()),
            reset_id: String::new(),
            secure: false,
            health_check_duration: std::time::Duration::from_secs(5),
            replicate_sync: false,
            client: Arc::new(aws_sdk_s3::Client::from_conf(config)),
        })
    }

    fn spawn_head_status_server(status: u16) -> (String, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("test HTTP listener should bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("test HTTP listener should have an address"));
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test HTTP client should connect");
            let mut request = [0_u8; 8192];
            let bytes_read = stream.read(&mut request).expect("test HTTP request should be read");
            assert!(bytes_read > 0, "test HTTP request should not be empty");
            assert!(request[..bytes_read].starts_with(b"HEAD "), "replication comparison must use HEAD");
            write!(stream, "HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .expect("test HTTP response should be written");
        });
        (endpoint, handle)
    }

    #[tokio::test]
    async fn replica_metadata_missing_target_stops_before_full_put() {
        let (endpoint, server) = spawn_head_status_server(404);
        let target = test_target_client(endpoint);
        let roi = ReplicateObjectInfo {
            bucket: "source".to_string(),
            name: "object".to_string(),
            version_id: Some(Uuid::new_v4()),
            op_type: ReplicationType::Metadata,
            // Normal metadata writes replace REPLICA with per-target PENDING
            // before constructing the worker request.
            replication_status: ReplicationStatusType::Pending,
            ..Default::default()
        };
        let object_info = ObjectInfo {
            bucket: roi.bucket.clone(),
            name: roi.name.clone(),
            version_id: roi.version_id,
            etag: Some("source-etag".to_string()),
            user_defined: Arc::new(HashMap::from([(
                AMZ_BUCKET_REPLICATION_STATUS.to_string(),
                ReplicationStatusType::Replica.as_str().to_string(),
            )])),
            ..Default::default()
        };
        let mut rinfo = replicate_all_target_info(&roi, &target);

        let action = resolve_replicate_all_action(
            ReplicateAllActionContext {
                roi: &roi,
                tgt_client: &target,
                bucket: &roi.bucket,
                object: &roi.name,
                start_time: OffsetDateTime::now_utc(),
                ssec_audit_required: false,
            },
            object_info,
            &mut rinfo,
        )
        .await;

        assert!(action.is_none(), "missing replica metadata targets must not reach the payload PUT path");
        assert_eq!(rinfo.replication_status, ReplicationStatusType::Failed);
        assert_eq!(
            rinfo.error.as_deref(),
            Some("replica metadata target does not contain this object version")
        );
        server.join().expect("test HTTP server should finish");
    }

    #[tokio::test]
    async fn source_metadata_missing_target_rebuilds_object() {
        let (endpoint, server) = spawn_head_status_server(404);
        let target = test_target_client(endpoint);
        let roi = ReplicateObjectInfo {
            bucket: "source".to_string(),
            name: "object".to_string(),
            version_id: Some(Uuid::new_v4()),
            op_type: ReplicationType::Metadata,
            replication_status: ReplicationStatusType::Pending,
            ..Default::default()
        };
        let object_info = ObjectInfo {
            bucket: roi.bucket.clone(),
            name: roi.name.clone(),
            version_id: roi.version_id,
            etag: Some("source-etag".to_string()),
            ..Default::default()
        };
        let mut rinfo = replicate_all_target_info(&roi, &target);

        let action = resolve_replicate_all_action(
            ReplicateAllActionContext {
                roi: &roi,
                tgt_client: &target,
                bucket: &roi.bucket,
                object: &roi.name,
                start_time: OffsetDateTime::now_utc(),
                ssec_audit_required: false,
            },
            object_info,
            &mut rinfo,
        )
        .await;

        assert!(matches!(action, Some((ReplicationAction::All, _))));
        assert!(rinfo.error.is_none());
        server.join().expect("test HTTP server should finish");
    }

    async fn register_test_target(target: &Arc<TargetClient>) {
        ReplicationTargetStore::register_test_target(target).await;
    }

    const DRIFTED_ASSIGNED_VERSION_ID: &str = "001788697733811332140-fR6j6uXKV-";
    const DRIFTED_ETAG: &str = "9a0364b9e99bb480dd25e1f0284c8555";

    /// The Wasabi shape (rustfs/backlog#2340): a version-addressed HEAD with
    /// the source uuid answers 404 (not the AWS 400), ListObjectVersions shows
    /// the id the target minted, and a HEAD by that id succeeds. Serves exactly
    /// `requests` connections and returns the request lines it saw.
    fn spawn_drifted_target_server(requests: usize) -> (String, std::thread::JoinHandle<Vec<String>>) {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("test HTTP listener should bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("test HTTP listener should have an address"));
        let handle = std::thread::spawn(move || {
            let mut seen = Vec::new();
            for _ in 0..requests {
                let (mut stream, _) = listener.accept().expect("test HTTP client should connect");
                let mut request = [0_u8; 8192];
                let bytes_read = stream.read(&mut request).expect("test HTTP request should be read");
                let text = String::from_utf8_lossy(&request[..bytes_read]).to_string();
                let request_line = text.lines().next().unwrap_or_default().to_string();
                let response = if request_line.starts_with("HEAD ") {
                    if request_line.contains(&format!("versionId={DRIFTED_ASSIGNED_VERSION_ID}")) {
                        format!(
                            "HTTP/1.1 200 OK\r\nETag: \"{DRIFTED_ETAG}\"\r\nContent-Length: 4\r\nLast-Modified: Sun, 06 Sep 2026 10:00:00 GMT\r\nConnection: close\r\n\r\n"
                        )
                    } else {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
                    }
                } else if request_line.starts_with("GET ") && request_line.contains("versions") {
                    let body = format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>target-bucket</Name><Prefix>object</Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Version><Key>object</Key><VersionId>{DRIFTED_ASSIGNED_VERSION_ID}</VersionId><IsLatest>true</IsLatest><LastModified>2026-09-06T10:00:00.000Z</LastModified><ETag>&quot;{DRIFTED_ETAG}&quot;</ETag><Size>4</Size><StorageClass>STANDARD</StorageClass></Version></ListVersionsResult>"
                    );
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                } else {
                    "HTTP/1.1 500 Unexpected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
                };
                stream
                    .write_all(response.as_bytes())
                    .expect("test HTTP response should be written");
                seen.push(request_line);
            }
            seen
        });
        (endpoint, handle)
    }

    fn drifted_roi_and_object() -> (ReplicateObjectInfo, ObjectInfo) {
        let roi = ReplicateObjectInfo {
            bucket: "source".to_string(),
            name: "object".to_string(),
            version_id: Some(Uuid::new_v4()),
            op_type: ReplicationType::Heal,
            replication_status: ReplicationStatusType::Pending,
            etag: Some(DRIFTED_ETAG.to_string()),
            size: 4,
            ..Default::default()
        };
        let object_info = ObjectInfo {
            bucket: roi.bucket.clone(),
            name: roi.name.clone(),
            version_id: roi.version_id,
            etag: Some(DRIFTED_ETAG.to_string()),
            size: 4,
            ..Default::default()
        };
        (roi, object_info)
    }

    #[tokio::test]
    async fn heal_redrive_locates_replica_by_etag_on_target_that_mints_own_version_ids() {
        let (endpoint, server) = spawn_drifted_target_server(3);
        let target = test_target_client(endpoint);
        ReplicationTargetStore::record_version_identity_capability(&target.arn, VersionIdentityCapability::MintsOwn);
        let (roi, object_info) = drifted_roi_and_object();
        let mut rinfo = replicate_all_target_info(&roi, &target);

        let action = resolve_replicate_all_action(
            ReplicateAllActionContext {
                roi: &roi,
                tgt_client: &target,
                bucket: &roi.bucket,
                object: &roi.name,
                start_time: OffsetDateTime::now_utc(),
                ssec_audit_required: false,
            },
            object_info,
            &mut rinfo,
        )
        .await;

        assert!(
            matches!(action, Some((ReplicationAction::None, _))),
            "a replica located by content identity must not be re-driven: {action:?}"
        );
        assert!(rinfo.error.is_none(), "{:?}", rinfo.error);
        let seen = server.join().expect("test HTTP server should finish");
        assert_eq!(seen.len(), 3, "HEAD by source id, ListObjectVersions, HEAD by assigned id: {seen:?}");
        assert!(seen[0].starts_with("HEAD ") && seen[0].contains(&roi.version_id.unwrap().to_string()));
        assert!(seen[1].starts_with("GET ") && seen[1].contains("prefix=object"), "{}", seen[1]);
        assert!(seen[2].starts_with("HEAD ") && seen[2].contains(DRIFTED_ASSIGNED_VERSION_ID));
    }

    #[tokio::test]
    async fn head_not_found_still_replicates_when_identity_contract_is_unknown() {
        // Same 404, but the target never revealed whether it adopts version
        // ids: a 404 keeps meaning "replica missing" (adopting targets, e.g.
        // RustFS/MinIO peers, must not skip a genuinely missing version).
        let (endpoint, server) = spawn_head_status_server(404);
        let target = test_target_client(endpoint);
        let (roi, object_info) = drifted_roi_and_object();
        let mut rinfo = replicate_all_target_info(&roi, &target);

        let action = resolve_replicate_all_action(
            ReplicateAllActionContext {
                roi: &roi,
                tgt_client: &target,
                bucket: &roi.bucket,
                object: &roi.name,
                start_time: OffsetDateTime::now_utc(),
                ssec_audit_required: false,
            },
            object_info,
            &mut rinfo,
        )
        .await;

        assert!(matches!(action, Some((ReplicationAction::All, _))));
        server.join().expect("test HTTP server should finish");
    }

    #[tokio::test]
    async fn resync_verification_counts_drifted_replica_as_replicated() {
        let (endpoint, server) = spawn_drifted_target_server(3);
        let target = test_target_client(endpoint);
        ReplicationTargetStore::record_version_identity_capability(&target.arn, VersionIdentityCapability::MintsOwn);
        let (roi, _) = drifted_roi_and_object();
        let mut st = TargetReplicationResyncStatus::default();

        let head_result =
            head_object_for_worker(target.as_ref(), &target.bucket, &roi.name, roi.version_id.map(|v| v.to_string())).await;
        let (size, err) = verify_resync_head_result(head_result, &roi, &mut st, &target).await;

        assert!(err.is_none(), "{err:?}");
        assert_eq!((size, st.replicated_count, st.failed_count), (4, 1, 0));
        server.join().expect("test HTTP server should finish");
    }

    #[test]
    fn put_response_audit_records_identity_verdict() {
        let target = test_target_client("http://127.0.0.1:1".to_string());
        let source = Uuid::new_v4().to_string();
        audit_target_version_identity(&target, &source, Some(DRIFTED_ASSIGNED_VERSION_ID));
        assert_eq!(
            ReplicationTargetStore::version_identity_capability(&target.arn),
            VersionIdentityCapability::MintsOwn
        );
        audit_target_version_identity(&target, &source, Some(&source));
        assert_eq!(
            ReplicationTargetStore::version_identity_capability(&target.arn),
            VersionIdentityCapability::Adopts
        );
        // An unversioned write carries no contract and must not overwrite it.
        audit_target_version_identity(&target, "null", None);
        assert_eq!(
            ReplicationTargetStore::version_identity_capability(&target.arn),
            VersionIdentityCapability::Adopts
        );
    }

    #[test]
    fn resync_admission_configuration_is_bounded() {
        assert_eq!(ENV_REPL_RESYNC_MAX_JOBS, "RUSTFS_REPL_RESYNC_MAX_JOBS");
        assert_eq!(bounded_resync_max_jobs(0), 1);
        assert_eq!(bounded_resync_max_jobs(DEFAULT_REPL_RESYNC_MAX_JOBS), 2);
        assert_eq!(bounded_resync_max_jobs(1000), MAX_REPL_RESYNC_MAX_JOBS);
    }

    #[tokio::test]
    async fn resync_admission_limits_jobs_and_wait_is_cancelable() {
        let resyncer = ReplicationResyncer {
            resync_admission: Arc::new(Semaphore::new(2)),
            ..ReplicationResyncer::new().await
        };
        let first = resyncer
            .acquire_resync_admission(&CancellationToken::new())
            .await
            .expect("first resync should acquire admission");
        let second = resyncer
            .acquire_resync_admission(&CancellationToken::new())
            .await
            .expect("second resync should acquire admission");
        let cancellation = CancellationToken::new();
        let blocked = resyncer.acquire_resync_admission(&cancellation);
        tokio::pin!(blocked);

        assert!(
            tokio::time::timeout(TokioDuration::from_millis(25), &mut blocked)
                .await
                .is_err()
        );
        cancellation.cancel();
        assert!(
            tokio::time::timeout(TokioDuration::from_secs(1), &mut blocked)
                .await
                .expect("canceled admission wait should finish")
                .is_none()
        );

        drop((first, second));
    }

    #[tokio::test]
    async fn replication_target_network_failure_marks_target_offline() {
        let endpoint = format!("http://network-failure-{}.example:9000", Uuid::new_v4());
        let target_client = test_target_client(endpoint);
        register_test_target(&target_client).await;

        assert!(!ReplicationTargetStore::target_is_offline(&target_client).await);

        let err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        mark_replication_target_offline_if_needed(&target_client, &err).await;

        assert!(ReplicationTargetStore::target_is_offline(&target_client).await);
    }

    #[tokio::test]
    async fn replication_target_service_failure_keeps_target_online() {
        let endpoint = format!("http://service-failure-{}.example:9000", Uuid::new_v4());
        let target_client = test_target_client(endpoint);
        register_test_target(&target_client).await;

        assert!(!ReplicationTargetStore::target_is_offline(&target_client).await);

        mark_replication_target_offline_if_needed(&target_client, &"put_object failed: AccessDenied: denied").await;

        assert!(!ReplicationTargetStore::target_is_offline(&target_client).await);
    }

    #[test]
    fn test_unmarshal_resync_payload() {
        let start = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid ts");
        let last = OffsetDateTime::from_unix_timestamp(1_700_000_123).expect("valid ts");
        let before = OffsetDateTime::from_unix_timestamp(1_699_000_000).expect("valid ts");
        let bucket_last = OffsetDateTime::from_unix_timestamp(1_700_111_111).expect("valid ts");

        let mut payload = Vec::new();
        rmp::encode::write_map_len(&mut payload, 4).expect("write map");
        rmp::encode::write_str(&mut payload, "v").expect("write key");
        rmp::encode::write_i32(&mut payload, 1).expect("write version");
        rmp::encode::write_str(&mut payload, "brs").expect("write key");
        rmp::encode::write_map_len(&mut payload, 1).expect("write target map");
        rmp::encode::write_str(&mut payload, "arn:replication::1:dest").expect("write arn");
        rmp::encode::write_map_len(&mut payload, 11).expect("write target");
        rmp::encode::write_str(&mut payload, "st").expect("write key");
        ReplicationMsgpCodec::write_time(&mut payload, start).expect("write time");
        rmp::encode::write_str(&mut payload, "lst").expect("write key");
        ReplicationMsgpCodec::write_time(&mut payload, last).expect("write time");
        rmp::encode::write_str(&mut payload, "id").expect("write key");
        rmp::encode::write_str(&mut payload, "resync-1").expect("write id");
        rmp::encode::write_str(&mut payload, "rdt").expect("write key");
        ReplicationMsgpCodec::write_time(&mut payload, before).expect("write time");
        rmp::encode::write_str(&mut payload, "rst").expect("write key");
        rmp::encode::write_i32(&mut payload, 3).expect("write status");
        rmp::encode::write_str(&mut payload, "fs").expect("write key");
        rmp::encode::write_i64(&mut payload, 11).expect("write fs");
        rmp::encode::write_str(&mut payload, "frc").expect("write key");
        rmp::encode::write_i64(&mut payload, 2).expect("write frc");
        rmp::encode::write_str(&mut payload, "rs").expect("write key");
        rmp::encode::write_i64(&mut payload, 101).expect("write rs");
        rmp::encode::write_str(&mut payload, "rrc").expect("write key");
        rmp::encode::write_i64(&mut payload, 9).expect("write rrc");
        rmp::encode::write_str(&mut payload, "bkt").expect("write key");
        rmp::encode::write_str(&mut payload, "bucket-a").expect("write bucket");
        rmp::encode::write_str(&mut payload, "obj").expect("write key");
        rmp::encode::write_str(&mut payload, "object-a").expect("write obj");
        rmp::encode::write_str(&mut payload, "id").expect("write key");
        rmp::encode::write_i32(&mut payload, 42).expect("write id");
        rmp::encode::write_str(&mut payload, "lu").expect("write key");
        ReplicationMsgpCodec::write_time(&mut payload, bucket_last).expect("write lu");

        let got = BucketReplicationResyncStatus::unmarshal_msg(&payload).expect("decode");
        assert_eq!(got.version, 1);
        assert_eq!(got.id, 42);
        assert_eq!(got.last_update, Some(bucket_last));
        let tgt = got.targets_map.get("arn:replication::1:dest").expect("target exists");
        assert_eq!(tgt.resync_id, "resync-1");
        assert_eq!(tgt.resync_status, ResyncStatusType::ResyncStarted);
        assert_eq!(tgt.bucket, "bucket-a");
        assert_eq!(tgt.object, "object-a");
        assert_eq!(tgt.start_time, Some(start));
        assert_eq!(tgt.last_update, Some(last));
        assert_eq!(tgt.resync_before_date, Some(before));
        assert_eq!(tgt.error, None);
    }

    #[test]
    fn test_unmarshal_legacy_resync_payload() {
        let mut status = BucketReplicationResyncStatus::new();
        status.id = 7;
        status.version = 1;
        status.last_update = Some(OffsetDateTime::from_unix_timestamp(1_700_222_222).expect("valid ts"));
        status.targets_map = HashMap::from([(
            "legacy-arn".to_string(),
            TargetReplicationResyncStatus {
                resync_id: "legacy-1".to_string(),
                resync_status: ResyncStatusType::ResyncCompleted,
                ..Default::default()
            },
        )]);

        let old_payload = rmp_serde::to_vec(&status).expect("legacy encode");
        let got = BucketReplicationResyncStatus::unmarshal_legacy_msg(&old_payload).expect("legacy decode");
        assert_eq!(got.id, 7);
        assert_eq!(got.version, 1);
        assert_eq!(got.targets_map["legacy-arn"].resync_id, "legacy-1");
        assert_eq!(got.targets_map["legacy-arn"].resync_status, ResyncStatusType::ResyncCompleted);
    }

    #[test]
    fn test_resync_file_roundtrip_wire_format() {
        let mut status = BucketReplicationResyncStatus::new();
        status.id = 19;
        status.last_update = Some(OffsetDateTime::from_unix_timestamp(1_700_333_333).expect("valid ts"));
        status.targets_map = HashMap::from([(
            "arn:replication::1:dest".to_string(),
            TargetReplicationResyncStatus {
                resync_id: "wire-1".to_string(),
                resync_status: ResyncStatusType::ResyncStarted,
                replicated_count: 5,
                ..Default::default()
            },
        )]);

        let bytes = encode_resync_file(&status).expect("encode file");
        assert_eq!(&bytes[0..2], &RESYNC_META_FORMAT.to_le_bytes());
        assert_eq!(&bytes[2..4], &RESYNC_META_VERSION.to_le_bytes());

        let got = decode_resync_file(&bytes).expect("decode file");
        assert_eq!(got.version, RESYNC_META_VERSION);
        assert_eq!(got.id, 19);
        assert_eq!(got.targets_map["arn:replication::1:dest"].resync_id, "wire-1");
        assert_eq!(got.targets_map["arn:replication::1:dest"].replicated_count, 5);
    }

    #[test]
    fn test_resync_file_decodes_legacy_payload() {
        let mut status = BucketReplicationResyncStatus::new();
        status.id = 7;
        status.version = RESYNC_META_VERSION;
        status.targets_map = HashMap::from([(
            "legacy-arn".to_string(),
            TargetReplicationResyncStatus {
                resync_id: "legacy-v1".to_string(),
                resync_status: ResyncStatusType::ResyncCompleted,
                ..Default::default()
            },
        )]);

        let legacy_payload = rmp_serde::to_vec(&status).expect("legacy encode");
        let mut file_bytes = Vec::new();
        file_bytes.extend_from_slice(&RESYNC_META_FORMAT.to_le_bytes());
        file_bytes.extend_from_slice(&RESYNC_META_VERSION.to_le_bytes());
        file_bytes.extend_from_slice(&legacy_payload);

        let got = decode_resync_file(&file_bytes).expect("decode legacy");
        assert_eq!(got.id, 7);
        assert_eq!(got.targets_map["legacy-arn"].resync_id, "legacy-v1");
        assert_eq!(got.targets_map["legacy-arn"].resync_status, ResyncStatusType::ResyncCompleted);
    }

    #[test]
    fn test_resync_none_time_encodes_as_wire_zero_and_decodes_to_none() {
        let wire_zero = OffsetDateTime::from_unix_timestamp(WIRE_ZERO_TIME_UNIX).expect("valid wire zero timestamp");

        let mut with_none = BucketReplicationResyncStatus::new();
        with_none.id = 77;
        with_none.targets_map = HashMap::from([(
            "arn:replication::1:dest".to_string(),
            TargetReplicationResyncStatus {
                resync_id: "wire-none".to_string(),
                resync_status: ResyncStatusType::ResyncStarted,
                replicated_count: 1,
                ..Default::default()
            },
        )]);

        let mut with_zero = with_none.clone();
        with_zero.last_update = Some(wire_zero);
        if let Some(target) = with_zero.targets_map.get_mut("arn:replication::1:dest") {
            target.start_time = Some(wire_zero);
            target.last_update = Some(wire_zero);
            target.resync_before_date = Some(wire_zero);
        }

        let encoded_none = encode_resync_file(&with_none).expect("encode with none");
        let encoded_zero = encode_resync_file(&with_zero).expect("encode with zero");
        assert_eq!(encoded_none, encoded_zero);

        let decoded = decode_resync_file(&encoded_none).expect("decode");
        let target = decoded
            .targets_map
            .get("arn:replication::1:dest")
            .expect("target should exist");
        assert_eq!(decoded.last_update, None);
        assert_eq!(target.start_time, None);
        assert_eq!(target.last_update, None);
        assert_eq!(target.resync_before_date, None);
    }

    #[test]
    fn test_replicate_object_info_to_object_info_preserves_delete_marker_flag() {
        let live = ReplicateObjectInfo {
            bucket: "source".to_string(),
            name: "object".to_string(),
            delete_marker: false,
            ..Default::default()
        };
        let delete_marker = ReplicateObjectInfo {
            bucket: "source".to_string(),
            name: "object".to_string(),
            delete_marker: true,
            ..Default::default()
        };

        assert!(!live.to_object_info().delete_marker);
        assert!(delete_marker.to_object_info().delete_marker);
    }

    #[test]
    fn test_is_version_delete_replication_for_delete_marker_version_purge() {
        let dobj = ReplicationDeletedObject {
            delete_marker: false,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(
            is_version_delete_replication(&dobj),
            "delete-marker version purges must be tracked as version purge replication, not delete-marker creation replication"
        );
    }

    #[test]
    fn test_is_version_delete_replication_for_delete_marker_creation() {
        let dobj = ReplicationDeletedObject {
            delete_marker: true,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(
            !is_version_delete_replication(&dobj),
            "delete-marker creation should remain on the delete-marker replication path"
        );
    }

    #[test]
    fn test_should_retry_delete_marker_purge_for_version_purge() {
        let dobj = ReplicationDeletedObject {
            delete_marker: false,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(
            should_retry_delete_marker_purge(&dobj),
            "delete-marker version purge should schedule delayed target cleanup in case the target marker arrives late"
        );
    }

    #[test]
    fn test_should_retry_delete_marker_purge_for_delete_marker_creation() {
        let dobj = ReplicationDeletedObject {
            delete_marker: true,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(
            should_retry_delete_marker_purge(&dobj),
            "delete-marker creation should keep the late-arrival cleanup path so downstream purges can catch up"
        );
    }

    /// P1-21 review follow-up: a target whose recorded marker version is
    /// inconsistent must be reported as a per-target FAILURE. Treating the
    /// refusal as success let the watcher and the MRF replay drop the purge
    /// intent while the marker was still on the target.
    #[tokio::test]
    async fn test_delete_marker_purge_reports_corrupt_recorded_version_as_failure() {
        let arn = format!("arn:rustfs:replication:us-east-1:corrupt:{}", Uuid::new_v4());
        let mut dsc = ReplicateDecision::new();
        dsc.set(ReplicateTargetDecision::new(arn.clone(), true, false));

        let mut state = ReplicationState {
            target_delete_marker_version_ids_corrupt: true,
            ..Default::default()
        };
        state.targets.insert(arn.clone(), ReplicationStatusType::Completed);

        let dobj = DeletedObjectReplicationInfo {
            delete_object: ReplicationDeletedObject {
                object_name: "doc.txt".to_string(),
                delete_marker: true,
                delete_marker_version_id: Some(Uuid::new_v4()),
                replication_state: Some(state),
                ..Default::default()
            },
            bucket: "bucket-a".to_string(),
            ..Default::default()
        };

        // No target client is registered: the refusal must be decided from
        // the recorded metadata alone, before any remote call is attempted.
        let failed = replicate_delete_marker_purge_to_targets("bucket-a", &dobj, &dsc, None).await;

        assert_eq!(
            failed,
            vec![arn],
            "a refused purge must stay in the failed set so the intent is never acknowledged"
        );
    }

    #[test]
    fn test_is_retryable_delete_replication_head_error_allows_delete_marker_head_responses() {
        assert!(
            !is_retryable_delete_replication_head_error(false, Some("405")),
            "numeric 405 responses should not block delete-marker purge replication"
        );
        assert!(
            !is_retryable_delete_replication_head_error(false, Some("MethodNotAllowed")),
            "MethodNotAllowed responses should not block delete-marker purge replication"
        );
        assert!(
            !is_retryable_delete_replication_head_error(true, Some("NoSuchKey")),
            "not-found responses should not block delete-marker purge replication"
        );
        assert!(
            is_retryable_delete_replication_head_error(false, Some("AccessDenied")),
            "unexpected head errors should still fail fast"
        );
    }

    #[test]
    fn test_should_count_head_proxy_failure_ignores_not_found_and_405() {
        assert!(
            !should_count_head_proxy_failure(true, Some("NoSuchKey"), Some(404)),
            "not-found heads are expected when the object has not reached the target yet"
        );
        assert!(
            !should_count_head_proxy_failure(false, Some("MethodNotAllowed"), Some(405)),
            "405 delete-marker probing responses should not be counted as proxy failures"
        );
        assert!(
            !should_count_head_proxy_failure(false, Some("405"), Some(405)),
            "numeric 405 codes must align with MethodNotAllowed semantics"
        );
    }

    #[test]
    fn test_should_count_head_proxy_failure_ignores_version_id_format_rejections() {
        assert!(
            !should_count_head_proxy_failure(false, Some("InvalidArgument"), Some(400)),
            "InvalidArgument/400 is a version-ID format rejection and must not be counted as a proxy failure"
        );
        assert!(
            !should_count_head_proxy_failure(false, None, Some(400)),
            "raw HTTP 400 without error code must not be counted as a proxy failure"
        );
        assert!(
            !should_count_head_proxy_failure(false, None, Some(403)),
            "raw HTTP 403 without error code must not be counted as a proxy failure (IAM user + invalid versionId)"
        );
    }

    #[test]
    fn test_is_version_id_mismatch_detects_invalid_argument() {
        assert!(
            is_version_id_mismatch(Some("InvalidArgument"), Some(400)),
            "AWS S3 returns InvalidArgument/400 when a UUID versionId is passed to HeadObject"
        );
        assert!(
            !is_version_id_mismatch(Some("AccessDenied"), Some(403)),
            "AccessDenied must not trigger the version-ID fallback path"
        );
        assert!(
            !is_version_id_mismatch(Some("NoSuchKey"), Some(404)),
            "NoSuchKey is an object-not-found response, not a version-ID mismatch"
        );
    }

    #[test]
    fn test_is_version_id_mismatch_raw_status_without_service_code() {
        assert!(
            is_version_id_mismatch(None, Some(400)),
            "no error code + HTTP 400 is treated as version-ID mismatch (HEAD response)"
        );
        assert!(
            is_version_id_mismatch(Some(""), Some(400)),
            "empty error code + HTTP 400 is treated as version-ID mismatch"
        );
        assert!(
            is_version_id_mismatch(None, Some(403)),
            "no error code + HTTP 403 is treated as version-ID mismatch (IAM user + invalid versionId)"
        );
        assert!(
            is_version_id_mismatch(Some(""), Some(403)),
            "empty error code + HTTP 403 is treated as version-ID mismatch"
        );
        assert!(
            !is_version_id_mismatch(None, Some(500)),
            "raw 5xx must not trigger the version-ID fallback path"
        );
        assert!(
            !is_version_id_mismatch(None, Some(404)),
            "raw 404 must not trigger the version-ID fallback path"
        );
    }

    #[test]
    fn test_is_version_id_mismatch_400_with_other_service_code() {
        assert!(
            !is_version_id_mismatch(Some("MalformedXML"), Some(400)),
            "MalformedXML/400 is a real request error and must not trigger version-ID fallback"
        );
        assert!(
            !is_version_id_mismatch(Some("EntityTooLarge"), Some(400)),
            "EntityTooLarge/400 is a real request error and must not trigger version-ID fallback"
        );
    }

    #[test]
    fn test_should_count_head_proxy_failure_counts_unexpected_errors() {
        assert!(
            should_count_head_proxy_failure(false, Some("AccessDenied"), Some(403)),
            "non-NotFound and non-405 service errors should be counted as failures"
        );
        assert!(
            should_count_head_proxy_failure(false, None, Some(500)),
            "raw 5xx head responses should be counted as proxy failures"
        );
    }

    #[tokio::test]
    async fn test_get_heal_replicate_object_info_failed_object_returns_heal_roi() {
        let oi = ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "key".to_string(),
            delete_marker: false,
            replication_status: ReplicationStatusType::Failed,
            version_id: Some(Uuid::nil()),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        let rcfg = ReplicationConfig::new(None, None);
        let roi = get_heal_replicate_object_info(&oi, &rcfg)
            .await
            .expect("non-delete heal classification should succeed");

        assert_eq!(roi.replication_status, ReplicationStatusType::Failed);
        assert_eq!(roi.op_type, ReplicationType::Heal);
        assert!(
            roi.dsc.replicate_any() || roi.dsc.targets_map.is_empty(),
            "With no replication config, dsc may be empty; with config, replicate_any() would be true and queueing would occur"
        );
    }

    #[tokio::test]
    async fn test_get_heal_replicate_object_info_preserves_ssec_checksum() {
        let checksum = bytes::Bytes::from_static(b"ssec-checksum");
        let oi = ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "key".to_string(),
            user_defined: Arc::new(HashMap::from([(
                rustfs_utils::http::SSEC_ALGORITHM_HEADER.to_string(),
                "AES256".to_string(),
            )])),
            checksum: Some(checksum.clone()),
            ..Default::default()
        };
        let rcfg = ReplicationConfig::new(None, None);

        let roi = get_heal_replicate_object_info(&oi, &rcfg)
            .await
            .expect("non-delete heal classification should succeed");

        assert!(roi.ssec);
        assert_eq!(roi.checksum, Some(checksum));
    }

    #[tokio::test]
    async fn test_get_heal_replicate_object_info_maps_version_purge_status_for_role() {
        let role = "arn:rustfs:replication::target:bucket";
        let oi = ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "key".to_string(),
            delete_marker: false,
            version_purge_status: VersionPurgeStatusType::Pending,
            version_id: Some(Uuid::nil()),
            mod_time: Some(OffsetDateTime::now_utc()),
            replication_decision: format!("{role}=true;false;{role};"),
            ..Default::default()
        };
        let rcfg = ReplicationConfig::new(
            Some(ReplicationConfiguration {
                role: role.to_string(),
                rules: vec![],
            }),
            None,
        );
        let roi = get_heal_replicate_object_info(&oi, &rcfg)
            .await
            .expect("stored purge admission should classify without a live versioning lookup");

        assert_eq!(roi.replication_status_internal, None);
        assert_eq!(roi.version_purge_status_internal.as_deref(), Some(format!("{role}=PENDING;").as_str()));
        assert_eq!(roi.target_purge_statuses.get(role), Some(&VersionPurgeStatusType::Pending));
    }

    #[tokio::test]
    async fn heal_pending_purge_reads_one_versioning_generation() {
        let bucket = format!("heal-versioning-snapshot-{}", Uuid::new_v4());
        let object = "archive/object";
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        ReplicationVersioningStore::install_prefix_state_test_config(
            &bucket,
            VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                excluded_prefixes: Some(vec![ExcludedPrefix {
                    prefix: Some("archive/".to_string()),
                }]),
                ..Default::default()
            },
        );
        let rcfg = ReplicationConfig::new(
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
                    id: Some("delete".to_string()),
                    prefix: Some(String::new()),
                    priority: Some(1),
                    source_selection_criteria: None,
                    status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
                }],
            }),
            Some(BucketTargets {
                targets: vec![BucketTarget {
                    arn: arn.to_string(),
                    ..Default::default()
                }],
            }),
        );
        let oi = ObjectInfo {
            bucket,
            name: object.to_string(),
            version_id: Some(Uuid::nil()),
            version_purge_status: VersionPurgeStatusType::Pending,
            ..Default::default()
        };

        let roi = get_heal_replicate_object_info(&oi, &rcfg)
            .await
            .expect("pending null purge classification should succeed");

        assert!(roi.dsc.targets_map.get(arn).is_some_and(|target| target.replicate));
        assert!(
            roi.existing_obj_resync
                .targets
                .get(arn)
                .is_some_and(|target| target.replicate)
        );
    }

    #[tokio::test]
    async fn heal_pending_purge_preserves_the_persisted_admission_decision() {
        let admitted_arn = "arn:rustfs:replication:us-east-1:target:admitted";
        let current_role = "arn:rustfs:replication:us-east-1:target:current";
        let rcfg = ReplicationConfig::new(
            Some(ReplicationConfiguration {
                role: current_role.to_string(),
                rules: vec![ReplicationRule {
                    delete_marker_replication: None,
                    delete_replication: Some(DeleteReplication {
                        status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::DISABLED),
                    }),
                    destination: Destination {
                        bucket: current_role.to_string(),
                        ..Default::default()
                    },
                    existing_object_replication: None,
                    filter: None,
                    id: Some("delete".to_string()),
                    prefix: Some(String::new()),
                    priority: Some(1),
                    source_selection_criteria: None,
                    status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
                }],
            }),
            Some(BucketTargets {
                targets: vec![
                    BucketTarget {
                        arn: admitted_arn.to_string(),
                        ..Default::default()
                    },
                    BucketTarget {
                        arn: current_role.to_string(),
                        ..Default::default()
                    },
                ],
            }),
        );
        let oi = ObjectInfo {
            bucket: "heal-persisted-delete-decision".to_string(),
            name: "object".to_string(),
            version_id: Some(Uuid::new_v4()),
            version_purge_status: VersionPurgeStatusType::Pending,
            version_purge_status_internal: Some(format!("{admitted_arn}=PENDING;")),
            replication_decision: format!("{admitted_arn}=true;false;{admitted_arn};"),
            ..Default::default()
        };

        let roi = get_heal_replicate_object_info(&oi, &rcfg)
            .await
            .expect("persisted delete admission should survive live rule disablement");

        assert_eq!(
            roi.version_purge_status_internal.as_deref(),
            Some(format!("{admitted_arn}=PENDING;").as_str())
        );
        assert!(roi.dsc.targets_map.get(admitted_arn).is_some_and(|target| target.replicate));
        assert!(!roi.dsc.targets_map.contains_key(current_role));
        assert!(
            roi.existing_obj_resync
                .targets
                .get(admitted_arn)
                .is_some_and(|target| target.replicate)
        );
        assert!(!roi.existing_obj_resync.targets.contains_key(current_role));
    }

    #[tokio::test]
    async fn heal_rejects_semantically_invalid_replication_config() {
        let rcfg = ReplicationConfig::new(
            Some(ReplicationConfiguration {
                role: String::new(),
                rules: vec![ReplicationRule {
                    delete_marker_replication: None,
                    delete_replication: None,
                    destination: Destination {
                        bucket: "arn:rustfs:replication:us-east-1:target:bucket".to_string(),
                        ..Default::default()
                    },
                    existing_object_replication: None,
                    filter: None,
                    id: Some("invalid".to_string()),
                    prefix: Some(String::new()),
                    priority: Some(1),
                    source_selection_criteria: None,
                    status: ReplicationRuleStatus::from_static("Enabld"),
                }],
            }),
            Some(BucketTargets::default()),
        );
        let err = rcfg
            .validate()
            .expect_err("invalid string-backed statuses must fail before heal classification loop");

        assert!(err.to_string().contains("Rule.Status"));
    }

    #[tokio::test]
    async fn test_cancel_marks_only_matching_bucket_target_token() {
        let resyncer = ReplicationResyncer::new().await;
        let opts_a = ResyncOpts {
            bucket: "bucket-a".to_string(),
            arn: "arn:replication::a".to_string(),
            resync_id: "rid-a".to_string(),
            resync_before: None,
        };
        let opts_b = ResyncOpts {
            bucket: "bucket-b".to_string(),
            arn: "arn:replication::b".to_string(),
            resync_id: "rid-b".to_string(),
            resync_before: None,
        };
        let token_a = CancellationToken::new();
        let token_b = CancellationToken::new();
        resyncer.register_cancel_token(&opts_a, token_a.clone()).await;
        resyncer.register_cancel_token(&opts_b, token_b.clone()).await;

        resyncer.cancel(&opts_a).await;

        assert!(token_a.is_cancelled());
        assert!(!token_b.is_cancelled());
    }

    #[tokio::test]
    async fn test_finish_resync_workers_closes_result_collector() {
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel::<ReplicateObjectInfo>(1);
        let (results_tx, mut results_rx) = tokio::sync::mpsc::channel::<TargetReplicationResyncStatus>(1);
        let worker = tokio::spawn(async move { while worker_rx.recv().await.is_some() {} });
        let collector = tokio::spawn(async move { while results_rx.recv().await.is_some() {} });

        let failed = tokio::time::timeout(
            TokioDuration::from_secs(1),
            finish_resync_workers(vec![worker_tx], results_tx, vec![worker, collector], false),
        )
        .await
        .expect("resync worker cleanup should not hang after closing senders");

        assert!(!failed);
    }

    #[tokio::test]
    async fn test_finish_resync_workers_reports_join_failure() {
        let (results_tx, _results_rx) = tokio::sync::mpsc::channel::<TargetReplicationResyncStatus>(1);
        let failed_worker = tokio::spawn(async {
            panic!("intentional resync worker failure");
        });

        let failed = finish_resync_workers(Vec::new(), results_tx, vec![failed_worker], false).await;

        assert!(failed);
    }

    #[tokio::test]
    async fn test_target_has_resync_failures_reads_accumulated_stats() {
        let resyncer = ReplicationResyncer::new().await;
        let opts = ResyncOpts {
            bucket: "bucket".to_string(),
            arn: "arn:replication::dest".to_string(),
            resync_id: "run-new".to_string(),
            resync_before: None,
        };
        let status = TargetReplicationResyncStatus {
            failed_count: 1,
            ..Default::default()
        };

        resyncer.inc_stats(&status, opts.clone()).await;

        assert!(resyncer.target_has_resync_failures(&opts).await);
    }

    #[tokio::test]
    async fn test_inc_stats_retains_first_sanitized_error_across_success() {
        let resyncer = ReplicationResyncer::new().await;
        let opts = ResyncOpts {
            bucket: "bucket".to_string(),
            arn: "arn:replication::dest".to_string(),
            resync_id: "run-new".to_string(),
            resync_before: None,
        };
        let failed = TargetReplicationResyncStatus {
            failed_count: 1,
            object: "failed-object".to_string(),
            error: Some("Authorization: Bearer status-secret".to_string()),
            ..Default::default()
        };
        let later_failure = TargetReplicationResyncStatus {
            failed_count: 1,
            object: "later-failed-object".to_string(),
            error: Some("AccessDenied".to_string()),
            ..Default::default()
        };
        let succeeded = TargetReplicationResyncStatus {
            replicated_count: 1,
            object: "successful-object".to_string(),
            ..Default::default()
        };

        resyncer.inc_stats(&failed, opts.clone()).await;
        resyncer.inc_stats(&later_failure, opts.clone()).await;
        resyncer.inc_stats(&succeeded, opts.clone()).await;

        let status_map = resyncer.status_map.read().await;
        let target = &status_map["bucket"].targets_map["arn:replication::dest"];
        assert_eq!(target.failed_count, 2);
        assert_eq!(target.replicated_count, 1);
        assert_eq!(target.object, "successful-object");
        assert_eq!(target.error.as_deref(), Some("[redacted sensitive resync error detail]"));
    }

    #[test]
    fn test_resync_target_error_detail_uses_safe_service_code_and_fallback() {
        let metadata = aws_smithy_types::error::ErrorMetadata::builder()
            .code("AccessDenied")
            .message("Authorization: Bearer status-secret")
            .build();
        let service_error = SdkError::service_error(HeadObjectError::generic(metadata), ());
        let timeout_error =
            SdkError::<HeadObjectError, ()>::timeout_error(std::io::Error::new(std::io::ErrorKind::TimedOut, "status-secret"));

        assert_eq!(resync_target_error_detail(&service_error).as_deref(), Some("AccessDenied"));
        assert_eq!(resync_target_error_detail(&timeout_error).as_deref(), Some("target request timed out"));
    }

    #[test]
    fn test_resync_state_accepts_update_only_for_matching_run() {
        let current = TargetReplicationResyncStatus {
            resync_id: "run-new".to_string(),
            ..Default::default()
        };
        let matching = ResyncOpts {
            bucket: "bucket".to_string(),
            arn: "arn:replication::dest".to_string(),
            resync_id: "run-new".to_string(),
            resync_before: None,
        };
        let stale = ResyncOpts {
            bucket: "bucket".to_string(),
            arn: "arn:replication::dest".to_string(),
            resync_id: "run-old".to_string(),
            resync_before: None,
        };

        assert!(resync_state_accepts_update(&TargetReplicationResyncStatus::default(), &matching));
        assert!(resync_state_accepts_update(&current, &matching));
        assert!(!resync_state_accepts_update(&current, &stale));
    }

    #[test]
    fn object_lock_denied_purge_backoff_tracks_version_and_target() {
        let denied = DeletedObjectReplicationInfo {
            bucket: "worm-backoff-test-bucket".to_string(),
            target_arn: "arn:rustfs:replication::worm-test:t1".to_string(),
            delete_object: ReplicationDeletedObject {
                object_name: "locked-object".to_string(),
                version_id: Some(uuid::Uuid::new_v4()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(!object_lock_denied_purge_backoff_active(&denied));

        record_object_lock_denied_purge(&denied, "arn:rustfs:replication::worm-test:t1");
        assert!(object_lock_denied_purge_backoff_active(&denied));

        // A requeue that can also reach a target this denial does not cover
        // must keep flowing: the purge may succeed there.
        let mut other_target = denied.clone();
        other_target.target_arn = "arn:rustfs:replication::worm-test:t2".to_string();
        assert!(!object_lock_denied_purge_backoff_active(&other_target));

        // A different version of the same object must not be suppressed.
        let mut other_version = denied;
        other_version.delete_object.version_id = Some(uuid::Uuid::new_v4());
        assert!(!object_lock_denied_purge_backoff_active(&other_version));
    }

    #[tokio::test]
    async fn abort_multipart_on_failure_skips_abort_when_transfer_succeeded() {
        let aborted = Arc::new(AtomicBool::new(false));
        let flag = aborted.clone();
        let retry_scheduled = Arc::new(AtomicBool::new(false));
        let retry_flag = retry_scheduled.clone();

        let result = abort_multipart_on_failure(
            Ok(()),
            "dst-bucket",
            "obj",
            "upload-1",
            "arn:dest",
            move || async move {
                flag.store(true, Ordering::SeqCst);
                Ok(())
            },
            move || retry_flag.store(true, Ordering::SeqCst),
        )
        .await;

        assert!(result.is_ok());
        assert!(!aborted.load(Ordering::SeqCst));
        assert!(!retry_scheduled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn abort_multipart_on_failure_aborts_and_keeps_transfer_error() {
        let aborted = Arc::new(AtomicBool::new(false));
        let flag = aborted.clone();
        let retry_scheduled = Arc::new(AtomicBool::new(false));
        let retry_flag = retry_scheduled.clone();

        // The abort itself failing must not mask the transfer error, and a
        // failed abort must hand the upload id to the retry schedule (#6854):
        // the object itself is re-replicated under a fresh upload id, so
        // nothing else will ever abort this one.
        let result = abort_multipart_on_failure(
            Err(std::io::Error::other("transfer failed")),
            "dst-bucket",
            "obj",
            "upload-1",
            "arn:dest",
            move || async move {
                flag.store(true, Ordering::SeqCst);
                Err(S3ClientError::new("abort failed"))
            },
            move || retry_flag.store(true, Ordering::SeqCst),
        )
        .await;

        assert!(aborted.load(Ordering::SeqCst));
        assert!(retry_scheduled.load(Ordering::SeqCst));
        assert_eq!(result.unwrap_err().to_string(), "transfer failed");
    }

    #[tokio::test]
    async fn abort_multipart_on_failure_does_not_retry_a_gone_upload() {
        let retry_scheduled = Arc::new(AtomicBool::new(false));
        let retry_flag = retry_scheduled.clone();

        let result = abort_multipart_on_failure(
            Err(std::io::Error::other("transfer failed")),
            "dst-bucket",
            "obj",
            "upload-1",
            "arn:dest",
            || async { Err(S3ClientError::with_metadata("gone", None, Some("NoSuchUpload".to_string()), None)) },
            move || retry_flag.store(true, Ordering::SeqCst),
        )
        .await;

        // NoSuchUpload means the orphan no longer exists; retrying would only
        // produce noise.
        assert!(!retry_scheduled.load(Ordering::SeqCst));
        assert_eq!(result.unwrap_err().to_string(), "transfer failed");
    }

    /// A replication target's terminal outcome, as the operator sees it.
    fn failed_target(arn: &str, error: &str) -> ReplicatedTargetInfo {
        ReplicatedTargetInfo {
            arn: arn.to_string(),
            size: 6 * 1024 * 1024 * 1024,
            op_type: ReplicationType::Object,
            replication_status: ReplicationStatusType::Failed,
            endpoint: "s3.wasabisys.com".to_string(),
            error: Some(error.to_string()),
            ..Default::default()
        }
    }

    /// Capture the log this module writes, filtered exactly the way a stock
    /// deployment filters it.
    fn logs_at_default_level(emit: impl FnOnce()) -> String {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::EnvFilter;
        use tracing_subscriber::fmt::MakeWriter;
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Clone, Default)]
        struct CapturedLogs {
            buffer: Arc<Mutex<Vec<u8>>>,
        }
        struct CapturedLogWriter {
            buffer: Arc<Mutex<Vec<u8>>>,
        }
        impl std::io::Write for CapturedLogWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.buffer
                    .lock()
                    .expect("captured logs mutex should not be poisoned")
                    .extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> MakeWriter<'a> for CapturedLogs {
            type Writer = CapturedLogWriter;
            fn make_writer(&'a self) -> Self::Writer {
                CapturedLogWriter {
                    buffer: Arc::clone(&self.buffer),
                }
            }
        }

        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::registry()
            // Not a hand-picked level: this is the filter an operator who has
            // changed nothing is actually running.
            .with(EnvFilter::new(rustfs_config::DEFAULT_LOG_LEVEL))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(logs.clone())
                    .with_ansi(false)
                    .without_time(),
            );
        let _guard = tracing::subscriber::set_default(subscriber);
        let _callsite_pin = crate::test_tracing::pin_callsite_interest_for_test();

        emit();

        let buffer = logs
            .buffer
            .lock()
            .expect("captured logs mutex should not be poisoned")
            .clone();
        String::from_utf8(buffer).expect("captured logs should be valid UTF-8")
    }

    /// rustfs#6825: a 6 GiB object never reached the target and the server said
    /// nothing an operator could act on, because every failure line in this
    /// module sat below `DEFAULT_LOG_LEVEL`. The object key, the target, and
    /// the target's own error have to survive the default filter.
    #[test]
    fn failed_replication_names_the_object_at_the_default_log_level() {
        let rinfos = ReplicatedInfos {
            replication_timestamp: Some(OffsetDateTime::now_utc()),
            targets: vec![failed_target("arn:replication::wasabi", "put_object failed: EntityTooLarge")],
        };

        let logs = logs_at_default_level(|| {
            note_replication_terminal_failure("photos", "backups/vm-image.qcow2", Some("v-9"), &rinfos);
        });

        assert!(logs.contains("backups/vm-image.qcow2"), "the failed object must be named: {logs}");
        assert!(logs.contains("arn:replication::wasabi"), "the target must be named: {logs}");
        assert!(logs.contains("EntityTooLarge"), "the target's own error must survive: {logs}");
        assert!(logs.contains("v-9"), "the version must be named: {logs}");
        assert!(logs.contains(EVENT_REPLICATION_OBJECT_FAILED), "the event must be structured: {logs}");
    }

    #[test]
    fn successful_replication_stays_quiet_at_the_default_log_level() {
        let rinfos = ReplicatedInfos {
            replication_timestamp: Some(OffsetDateTime::now_utc()),
            targets: vec![ReplicatedTargetInfo {
                arn: "arn:replication::wasabi".to_string(),
                replication_status: ReplicationStatusType::Completed,
                ..Default::default()
            }],
        };

        let logs = logs_at_default_level(|| {
            note_replication_terminal_failure("photos", "backups/ok.bin", None, &rinfos);
        });

        assert!(logs.is_empty(), "a completed replication must not log an error: {logs}");
    }

    /// A failed version purge is the 6822 symptom (the version stays on the
    /// target); it must be as visible as a failed transfer even though the
    /// replication status itself is not FAILED.
    #[test]
    fn failed_version_purge_is_reported_at_the_default_log_level() {
        let rinfos = ReplicatedInfos {
            replication_timestamp: Some(OffsetDateTime::now_utc()),
            targets: vec![ReplicatedTargetInfo {
                arn: "arn:replication::wasabi".to_string(),
                op_type: ReplicationType::Delete,
                replication_status: ReplicationStatusType::Empty,
                version_purge_status: VersionPurgeStatusType::Failed,
                error: Some("remove_object failed: NoSuchVersion".to_string()),
                ..Default::default()
            }],
        };

        let logs = logs_at_default_level(|| {
            note_replication_terminal_failure("photos", "backups/purged.bin", Some("v-1"), &rinfos);
        });

        assert!(logs.contains("backups/purged.bin"), "the purged object must be named: {logs}");
        assert!(logs.contains("NoSuchVersion"), "the target's own error must survive: {logs}");
    }

    /// The target's error is echoed remote text and can carry a signed URL or
    /// an auth header, so it goes through the persisted-detail redaction rather
    /// than straight into the log.
    #[test]
    fn failed_replication_redacts_a_sensitive_target_error() {
        let rinfos = ReplicatedInfos {
            replication_timestamp: Some(OffsetDateTime::now_utc()),
            targets: vec![failed_target(
                "arn:replication::wasabi",
                "put_object failed: rejected Authorization: Bearer super-secret",
            )],
        };

        let logs = logs_at_default_level(|| {
            note_replication_terminal_failure("photos", "backups/vm-image.qcow2", None, &rinfos);
        });

        assert!(logs.contains("backups/vm-image.qcow2"), "the object must still be named: {logs}");
        assert!(!logs.contains("super-secret"), "the credential must not reach the log: {logs}");
    }

    /// An empty target slot carries no outcome; reporting it would invent a
    /// failure for a target that was never attempted.
    #[test]
    fn empty_target_slots_are_not_reported_as_failures() {
        let rinfos = ReplicatedInfos {
            replication_timestamp: Some(OffsetDateTime::now_utc()),
            targets: vec![ReplicatedTargetInfo::default()],
        };

        let logs = logs_at_default_level(|| {
            note_replication_terminal_failure("photos", "backups/unattempted.bin", None, &rinfos);
        });

        assert!(logs.is_empty(), "an empty target slot must not be reported: {logs}");
    }

    #[test]
    fn version_identity_drift_re_arms_after_the_throttle_interval() {
        let arn = "arn:replication::drift-throttle-test";
        let start = Instant::now();

        assert!(version_identity_drift_log_due(arn, start), "first drift must be reported");
        assert!(
            !version_identity_drift_log_due(arn, start + VERSION_IDENTITY_DRIFT_LOG_INTERVAL / 2),
            "a second drift inside the interval must stay throttled"
        );
        assert!(
            version_identity_drift_log_due(arn, start + VERSION_IDENTITY_DRIFT_LOG_INTERVAL),
            "drift must become visible again once the interval elapses, instead of \
             going silent for the rest of the process lifetime"
        );
    }

    #[test]
    fn version_identity_drift_throttles_each_target_independently() {
        let now = Instant::now();

        assert!(version_identity_drift_log_due("arn:replication::drift-a", now));
        assert!(
            version_identity_drift_log_due("arn:replication::drift-b", now),
            "one target's report must not silence another's"
        );
    }
    mod multipart_transport_tests {
        use super::super::super::replication_filemeta_boundary::ObjectPartInfo;
        use super::super::super::replication_storage_boundary::ObjectIO as _;
        use super::*;
        use bytes::Bytes;
        use http_body_util::{BodyExt, Full};
        use std::convert::Infallible;

        #[derive(Debug)]
        struct Source {
            body: Bytes,
            info: ObjectInfo,
            ranges: StdMutex<Vec<(i64, i64)>>,
            full_reads: std::sync::atomic::AtomicUsize,
        }

        #[async_trait::async_trait]
        impl super::super::super::replication_storage_boundary::ObjectIO for Source {
            type Error = Error;
            type RangeSpec = HTTPRangeSpec;
            type HeaderMap = HeaderMap;
            type ObjectOptions = ObjectOptions;
            type ObjectInfo = ObjectInfo;
            type GetObjectReader = GetObjectReader;
            type PutObjectReader = super::super::super::replication_storage_boundary::PutObjReader;

            async fn get_object_reader(
                &self,
                _bucket: &str,
                _object: &str,
                range: Option<HTTPRangeSpec>,
                _headers: HeaderMap,
                opts: &ObjectOptions,
            ) -> Result<GetObjectReader> {
                assert_eq!(
                    opts.version_id,
                    self.info.version_id.map(|id| id.to_string()),
                    "every read retains the selected source version"
                );
                if range.is_none() {
                    self.full_reads.fetch_add(1, Ordering::Relaxed);
                    return Ok(GetObjectReader {
                        stream: Box::new(std::io::Cursor::new(self.body.clone())),
                        object_info: self.info.clone(),
                        buffered_body: None,
                        body_source: Default::default(),
                    });
                }
                let range = range.expect("multipart transport must request an explicit nonempty range");
                assert!(!range.is_suffix_length);
                assert!(range.start <= range.end, "empty parts must not issue an inverted range");
                self.ranges.lock().expect("range journal lock").push((range.start, range.end));
                let start = usize::try_from(range.start).expect("nonnegative start");
                let end = usize::try_from(range.end).expect("nonnegative end");
                let body = self.body.slice(start..=end);
                Ok(GetObjectReader {
                    stream: Box::new(std::io::Cursor::new(body)),
                    object_info: self.info.clone(),
                    buffered_body: None,
                    body_source: Default::default(),
                })
            }

            async fn put_object(
                &self,
                _bucket: &str,
                _object: &str,
                _data: &mut Self::PutObjectReader,
                _opts: &ObjectOptions,
            ) -> Result<ObjectInfo> {
                panic!("replication must not overwrite its source")
            }
        }

        #[derive(Debug)]
        struct RequestRecord {
            method: http::Method,
            query: HashMap<String, String>,
            headers: HeaderMap,
            body: Bytes,
        }

        #[tokio::test]
        async fn multipart_transport_preserves_legacy_zero_actual_sizes() {
            run_transport(4096, None).await;
        }

        #[tokio::test]
        async fn multipart_transport_uploads_an_empty_last_part_without_reading_a_range() {
            run_transport(0, None).await;
        }

        #[tokio::test]
        async fn multipart_transport_preserves_transformed_unknown_nonempty_parts() {
            for unknown_part in [(0, 0), (1, 0), (0, -1), (1, -1)] {
                run_transport(4096, Some(unknown_part)).await;
            }
        }

        #[tokio::test]
        async fn multipart_transport_preserves_transformed_empty_tail() {
            run_transport(0, Some((1, 0))).await;
        }

        async fn run_transport(tail_size: usize, unknown_part: Option<(usize, i64)>) {
            const FIRST_SIZE: usize = 5 * 1024 * 1024;
            let body = Bytes::from([vec![0x35; FIRST_SIZE], vec![0xa7; tail_size]].concat());
            let etag = faster_hex::hex_string(rustfs_utils::hash::HashAlgorithm::Md5.hash_encode(&body).as_ref());
            let source = Arc::new(Source {
                info: ObjectInfo {
                    size: i64::try_from(body.len() + if unknown_part.is_some() { 16 } else { 0 }).expect("stored size"),
                    actual_size: i64::try_from(body.len()).expect("body size"),
                    etag: Some(etag.clone()),
                    version_id: Some(Uuid::new_v4()),
                    user_defined: Arc::new(if unknown_part.is_some() {
                        HashMap::from([("x-amz-server-side-encryption".to_string(), "AES256".to_string())])
                    } else {
                        HashMap::new()
                    }),
                    parts: Arc::new(vec![
                        ObjectPartInfo {
                            number: 1,
                            size: FIRST_SIZE + if unknown_part.is_some() { 8 } else { 0 },
                            actual_size: if let Some((0, size)) = unknown_part {
                                size
                            } else if unknown_part.is_some() || tail_size == 0 {
                                i64::try_from(FIRST_SIZE).expect("first part size")
                            } else {
                                0
                            },
                            ..Default::default()
                        },
                        ObjectPartInfo {
                            number: 2,
                            size: tail_size + if unknown_part.is_some() { 8 } else { 0 },
                            actual_size: if let Some((1, size)) = unknown_part {
                                size
                            } else if unknown_part.is_some() {
                                i64::try_from(tail_size).expect("tail logical size")
                            } else {
                                0
                            },
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                },
                body: body.clone(),
                ranges: StdMutex::new(Vec::new()),
                full_reads: std::sync::atomic::AtomicUsize::new(0),
            });
            let journal = Arc::new(StdMutex::new(Vec::<RequestRecord>::new()));
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind multipart target");
            let endpoint = format!("http://{}", listener.local_addr().expect("multipart target address"));
            let server_journal = journal.clone();
            let server = tokio::spawn(async move {
                let mut connections = JoinSet::new();
                loop {
                    let (stream, _) = listener.accept().await.expect("accept multipart request");
                    let journal = server_journal.clone();
                    connections.spawn(async move {
                            let service = hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                                let journal = journal.clone();
                                async move {
                                    let (request, body) = request.into_parts();
                                    let query: HashMap<String, String> = url::form_urlencoded::parse(
                                        request.uri.query().unwrap_or_default().as_bytes(),
                                    ).into_owned().collect();
                                    let body = body.collect().await.expect("read complete multipart request body").to_bytes();
                                    let response = if request.method == http::Method::POST && query.contains_key("uploads") {
                                        "<InitiateMultipartUploadResult><Bucket>target-bucket</Bucket><Key>object</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"
                                    } else if request.method == http::Method::PUT {
                                        ""
                                    } else if request.method == http::Method::POST && query.contains_key("uploadId") {
                                        "<CompleteMultipartUploadResult><Location>http://localhost/object</Location><Bucket>target-bucket</Bucket><Key>object</Key><ETag>&quot;target-2&quot;</ETag></CompleteMultipartUploadResult>"
                                    } else if request.method == http::Method::DELETE && query.contains_key("uploadId") {
 ""
 } else {
                                        panic!("unexpected multipart request: {} {}", request.method, request.uri)
                                    };
                                    let response_etag = if request.method == http::Method::PUT && !query.contains_key("partNumber") {
                                        format!("\"{}\"", faster_hex::hex_string(rustfs_utils::hash::HashAlgorithm::Md5.hash_encode(&body).as_ref()))
                                    } else {
                                        "\"uploaded-part\"".to_string()
                                    };
                                    journal.lock().expect("request journal lock").push(RequestRecord {
                                        method: request.method, query, headers: request.headers, body,
                                    });
                                    Ok::<_, Infallible>(hyper::Response::builder()
                                        .header("content-type", "application/xml")
                                        .header("etag", response_etag)
                                        .body(Full::new(Bytes::from_static(response.as_bytes())))
                                        .expect("multipart response"))
                                }
                            });
                            hyper::server::conn::http1::Builder::new()
                                .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                                .await.expect("serve multipart connection");
                        });
                }
            });
            let mut target = test_target_client(endpoint);
            let config = target
                .client
                .config()
                .to_builder()
                .request_checksum_calculation(aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired)
                .force_path_style(true)
                .build();
            Arc::get_mut(&mut target).expect("unshared test target").client = Arc::new(aws_sdk_s3::Client::from_conf(config));
            let (put_opts, is_multipart) = replication_put_object_options("STANDARD", &source.info).expect("replication options");
            let opts = ObjectOptions {
                version_id: source.info.version_id.map(|id| id.to_string()),
                ..Default::default()
            };
            let reader = source
                .get_object_reader("source", "object", None, HeaderMap::new(), &opts)
                .await
                .expect("open the existing full-object stream");
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                replicate_all_payload_to_target(
                    ReplicateAllPayloadContext {
                        storage: &source,
                        tgt_client: &target,
                        bucket: "source",
                        object: "object",
                        object_info: &source.info,
                        obj_opts: &opts,
                        arn: &target.arn,
                        transfer_size: i64::try_from(body.len()).expect("plaintext size"),
                        is_multipart,
                        put_opts,
                    },
                    reader,
                ),
            )
            .await;
            server.abort();
            assert!(server.await.expect_err("fixture server is stopped").is_cancelled());
            if let Some(error) = result.expect("replication must finish") {
                panic!("legacy parts must replicate successfully: {error}");
            }
            assert_eq!(
                source.full_reads.load(Ordering::Relaxed),
                1,
                "reuse the initial full stream without an extra read"
            );
            if unknown_part.is_some() {
                let requests = journal.lock().expect("request journal lock");
                assert_eq!(requests.len(), 1, "unknown transformed boundaries retain one streaming PUT");
                let request = &requests[0];
                assert_eq!(request.method, http::Method::PUT);
                let source_version = source.info.version_id.map(|id| id.to_string()).expect("versioned fixture");
                assert_eq!(
                    request.query,
                    HashMap::from([
                        ("x-id".to_string(), "PutObject".to_string()),
                        ("versionId".to_string(), source_version.clone()),
                    ]),
                    "single PUT carries only the SDK operation query and the source versionId the target must reuse"
                );
                assert_eq!(request.body, body, "single PUT includes every byte of both source parts");
                assert_eq!(
                    request.headers.get("content-length").expect("body length"),
                    body.len().to_string().as_str()
                );
                assert_eq!(
                    rustfs_utils::http::get_header(&request.headers, rustfs_utils::http::SUFFIX_SOURCE_ETAG).as_deref(),
                    Some(etag.as_str())
                );
                assert_eq!(
                    rustfs_utils::http::get_header(&request.headers, rustfs_utils::http::SUFFIX_SOURCE_VERSION_ID)
                        .map(|value| value.into_owned()),
                    Some(source_version),
                    "single PUT preserves the selected source version"
                );
                assert!(
                    source.ranges.lock().expect("range journal lock").is_empty(),
                    "unknown logical boundaries must not issue guessed ranges"
                );
                return;
            }

            let requests = journal.lock().expect("request journal lock");
            assert_eq!(requests.len(), 4, "initiate, two upload parts, and complete without retries");
            assert!(requests[0].query.contains_key("uploads"));
            for (index, expected) in [(1, body.slice(..FIRST_SIZE)), (2, body.slice(FIRST_SIZE..))] {
                assert_eq!(requests[index].method, http::Method::PUT);
                assert_eq!(requests[index].query.get("partNumber"), Some(&index.to_string()));
                assert_eq!(requests[index].body, expected, "upload part contains the exact source range");
                assert_eq!(
                    requests[index].headers.get("content-length").expect("part content length"),
                    expected.len().to_string().as_str()
                );
            }
            let complete = &requests[3];
            assert_eq!(complete.method, http::Method::POST);
            assert_eq!(
                rustfs_utils::http::get_header(&complete.headers, rustfs_utils::http::SUFFIX_SOURCE_ETAG).as_deref(),
                Some(etag.as_str())
            );
            let complete_xml = std::str::from_utf8(&complete.body).expect("complete XML");
            assert_eq!(
                complete_xml.matches("<Part>").count(),
                2,
                "the empty final part must remain in the completion list"
            );
            assert!(complete_xml.contains("<PartNumber>1</PartNumber>"));
            assert!(complete_xml.contains("<PartNumber>2</PartNumber>"));
            let mut expected_ranges = vec![(0, i64::try_from(FIRST_SIZE - 1).expect("first end"))];
            if tail_size > 0 {
                expected_ranges.push((
                    i64::try_from(FIRST_SIZE).expect("tail start"),
                    i64::try_from(body.len() - 1).expect("tail end"),
                ));
            }
            assert_eq!(*source.ranges.lock().expect("range journal lock"), expected_ranges);
        }
    }
}
