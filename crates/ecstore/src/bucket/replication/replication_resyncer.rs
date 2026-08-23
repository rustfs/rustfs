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
    heal_uses_delete_replication_path, is_retryable_delete_replication_head_error, is_version_delete_replication,
    replicate_delete_outcome, replication_etags_match, replication_multipart_complete_actual_size,
    replication_multipart_part_plan, resync_existing_delete_replication_info, should_retry_delete_marker_purge,
    target_delete_version_id,
};
use super::replication_queue_boundary::{DeletedObjectReplicationInfo, ReplicationQueueAdmission};
use super::replication_resync_boundary::ResyncStatusType;
#[cfg(test)]
use super::replication_resync_boundary::should_count_head_proxy_failure;
use super::replication_resync_boundary::{
    BucketReplicationResyncStatus, ResyncOpts, TargetReplicationResyncStatus, encode_resync_file, is_version_id_mismatch,
    resync_state_accepts_update, resync_status_duration, sanitize_resync_error_detail,
};
#[cfg(test)]
use super::replication_resync_boundary::{RESYNC_META_FORMAT, RESYNC_META_VERSION, WIRE_ZERO_TIME_UNIX, decode_resync_file};
#[cfg(test)]
use super::replication_storage_boundary::ReplicationDeletedObject;
use super::replication_storage_boundary::{
    AdvancedGetOptions, EcstoreObjectOperations, GetObjectReader, HTTPRangeSpec, ObjectInfo, ObjectOptions, ObjectToDelete,
    ReplicationObjectIO, ReplicationStorage, StatObjectOptions, StorageObjectInfoOrErr, WalkOptions,
};
use super::replication_target_boundary::{
    ERR_REPLICATION_SSEC_PASSTHROUGH_UNSUPPORTED, HeadObjectSdkError, PutObjectOptions, PutObjectPartOptions,
    ReplicationTargetStore, SsecPassthroughCapability, SsecPassthroughGate, TargetClient, is_replication_target_offline_error,
    replication_action_for_target_head, replication_complete_multipart_options, replication_delete_marker_purge_remove_options,
    replication_delete_remove_options, replication_force_delete_remove_options, replication_object_is_ssec_encrypted,
    replication_put_object_header_size, replication_put_object_options, replication_target_head_is_newer_null_version,
    resolve_read_api_version_id, ssec_passthrough_evidence_present, ssec_passthrough_gate, version_identity_drifted,
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
    AMZ_TAGGING_DIRECTIVE, SUFFIX_REPLICATION_RESET, SUFFIX_REPLICATION_STATUS, has_internal_suffix, insert_str,
};
use rustfs_utils::{DEFAULT_SIP_HASH_KEY, get_env_usize, sip_hash};
#[cfg(test)]
use s3s::dto::ReplicationConfiguration;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncRead;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Duration as TokioDuration;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, trace, warn};

const BACKGROUND_WALKDIR_TIMEOUT: TokioDuration = TokioDuration::from_secs(60);
const ENV_REPL_RESYNC_MAX_JOBS: &str = "RUSTFS_REPL_RESYNC_MAX_JOBS";
const DEFAULT_REPL_RESYNC_MAX_JOBS: usize = 2;
const MAX_REPL_RESYNC_MAX_JOBS: usize = 32;
use uuid::Uuid;

const EVENT_RESYNC_STATUS_UPDATE_SKIPPED: &str = "replication_resync_status_update_skipped";
const EVENT_RESYNC_OBJECT_PROCESSED: &str = "replication_resync_object_processed";
const EVENT_RESYNC_RUNTIME_SKIPPED: &str = "replication_resync_runtime_skipped";
const EVENT_REPLICATION_DELETE_SKIPPED: &str = "replication_delete_skipped";
const EVENT_REPLICATION_FORCE_DELETE_SKIPPED: &str = "replication_force_delete_skipped";
const EVENT_RESYNC_TASK_FAILED: &str = "replication_resync_task_failed";
const EVENT_RESYNC_TARGET_OPERATION_FAILED: &str = "replication_resync_target_operation_failed";
const EVENT_RESYNC_RUNTIME_CHANNEL_FAILED: &str = "replication_resync_runtime_channel_failed";
const EVENT_DELETE_MARKER_PURGE_FAILED: &str = "replication_delete_marker_purge_failed";
const EVENT_DELETE_MARKER_PURGE_MRF: &str = "replication_delete_marker_purge_mrf";
const METRIC_DELETE_MARKER_PURGE_TOTAL: &str = "rustfs_replication_delete_marker_purge_total";
const EVENT_REPLICATION_VERSION_IDENTITY_DRIFT: &str = "replication_version_identity_drift";

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

const METRIC_VERSION_IDENTITY_DRIFT_TOTAL: &str = "rustfs_replication_version_identity_drift_total";

/// Targets that already produced a version-identity-drift warning this
/// process lifetime, by ARN. Deduping is advisory only (the metric still
/// counts every drifting PUT), so a reconfigured target re-warning only
/// after a restart is acceptable.
static VERSION_IDENTITY_WARNED_ARNS: LazyLock<StdMutex<HashSet<String>>> = LazyLock::new(|| StdMutex::new(HashSet::new()));

fn audit_target_version_identity(tgt_client: &TargetClient, source_version_id: &str, assigned_version_id: Option<&str>) {
    if !version_identity_drifted(source_version_id, assigned_version_id) {
        return;
    }
    counter!(METRIC_VERSION_IDENTITY_DRIFT_TOTAL).increment(1);
    let mut warned = VERSION_IDENTITY_WARNED_ARNS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if warned.insert(tgt_client.arn.clone()) {
        warn!(
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
        Err(e) if e.as_service_error().is_some_and(|se| se.is_not_found()) || has_raw_status(&e, 404) => Ok(None),
        Err(e) => Err(e),
    }
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
        let (bucket_status, status_duration) = {
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

            (bucket_status.clone(), status_duration)
        };

        save_resync_status(&opts.bucket, &bucket_status, obj_layer.clone()).await?;
        if status != ResyncStatusType::ResyncCanceled {
            let canceled_status = self
                .status_map
                .read()
                .await
                .get(&opts.bucket)
                .filter(|current| {
                    current.targets_map.get(&opts.arn).is_some_and(|target| {
                        target.resync_id == opts.resync_id && target.resync_status == ResyncStatusType::ResyncCanceled
                    })
                })
                .cloned();
            if let Some(canceled_status) = canceled_status {
                save_resync_status(&opts.bucket, &canceled_status, obj_layer).await?;
                return Ok(());
            }
        }
        if let Some(stats) = runtime_sources::replication_stats() {
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

                    let status_map = self.status_map.read().await;

                    let mut update = false;
                    for (bucket, status) in status_map.iter() {
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
                            if let Err(err) = save_resync_status(bucket, status, api.clone()).await {
                                error!(
                                    event = EVENT_RESYNC_STATUS_UPDATE_SKIPPED,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                                    bucket = %bucket,
                                    reason = "persist_failed",
                                    error = %err,
                                    "Failed to persist resync status"
                                );
                            } else {
                                last_update_times.insert(bucket.clone(), status.last_update.expect("last_update should be set"));
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
        Err(err) if is_version_id_format_mismatch(&err) => {
            // AWS-style target rejects the RustFS UUID versionId
            // (400). Re-verify without the versionId before
            // concluding the object failed to replicate, instead
            // of counting a well-replicated object as failed.
            match head_object_fallback(target_client.as_ref(), &roi.name).await {
                Ok(Some(_)) => {
                    st.replicated_count += 1;
                    st.replicated_size += roi.size;
                    (roi.size, None)
                }
                Ok(None) => {
                    st.failed_count += 1;
                    (0, Some(err))
                }
                Err(e2) => {
                    st.failed_count += 1;
                    (0, Some(e2))
                }
            }
        }
        Err(err) => {
            st.failed_count += 1;
            (0, Some(err))
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
        ssec: replication_object_is_ssec_encrypted(&user_defined),
        user_tags: (*oi.user_tags).clone(),
        checksum: oi.checksum.clone(),
        retry_count: 0,
    })
}

pub(crate) async fn save_resync_status<S: ReplicationObjectIO>(
    bucket: &str,
    status: &BucketReplicationResyncStatus,
    api: Arc<S>,
) -> Result<()> {
    let data = encode_resync_file(status)?;

    let config_file = ReplicationMetadataStore::bucket_resync_file_path(bucket);
    ReplicationConfigStore::save(api, &config_file, data).await?;

    Ok(())
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
    let mut source_state_verified = true;
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
                source_state_verified = false;
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

    let requires_delayed_purge = should_retry_delete_marker_purge(&dobj.delete_object);

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

    let mut drs = get_replication_state(
        &rinfos,
        &dobj.delete_object.replication_state.clone().unwrap_or_default(),
        dobj.delete_object.version_id.map(|v| v.to_string()),
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
        source_state_verified,
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

async fn replicate_delete_to_target(dobj: &DeletedObjectReplicationInfo, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo {
    let version_id = if let Some(version_id) = &dobj.delete_object.delete_marker_version_id {
        version_id.to_owned()
    } else {
        dobj.delete_object.version_id.unwrap_or_default()
    };

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

    let version_id = target_delete_version_id(version_id, is_version_purge);

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
            replication_delete_remove_options(dobj.delete_object.delete_marker, dobj.delete_object.delete_marker_mtime),
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

pub(crate) async fn replicate_object_with_outcome<S: ReplicationStorage>(
    roi: ReplicateObjectInfo,
    storage: Arc<S>,
) -> (ReplicationState, bool) {
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
            return (roi.replication_state.unwrap_or_default(), false);
        }
    };
    let _obj_lock_guard = match obj_ns_lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
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
            return (roi.replication_state.unwrap_or_default(), false);
        }
    };

    let mut join_set = JoinSet::new();

    for arn in tgt_arns {
        let Some(tgt_client) = ReplicationTargetStore::remote_target_client(&bucket, &arn).await else {
            // Deliberately debug: this fires once per object per ARN, so a target that
            // stays unreachable would flood the log from the replication hot path. The
            // condition is reported once per pass by the site-replication reconciler and
            // once per rebuild by `update_all_targets`, which is where an operator can act
            // on it; the per-object event below still records each dropped object.
            debug!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object,
                arn = %arn,
                reason = "target_client_missing",
                "Replication rule has no bucket target for its destination ARN; object not replicated"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: roi.to_object_info(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
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

    let mut rinfos = ReplicatedInfos {
        replication_timestamp: Some(OffsetDateTime::now_utc()),
        targets: Vec::with_capacity(join_set.len()),
    };

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

    let previous_state = roi.replication_state.clone().unwrap_or_default();
    let merged_state = get_replication_state(&rinfos, &previous_state, roi.version_id.map(|v| v.to_string()));
    let replication_status = merged_state.composite_replication_status();
    let new_replication_internal = merged_state.replication_status_internal.clone();
    let mut object_info = roi.to_object_info();
    let mut state_persisted = true;

    if roi.replication_status_internal != new_replication_internal || rinfos.replication_resynced() {
        let mut eval_metadata = HashMap::new();
        if let Some(ref s) = new_replication_internal {
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_STATUS, s.clone());
        }
        let popts = ObjectOptions {
            version_id: roi.version_id.map(|v| v.to_string()),
            eval_metadata: Some(eval_metadata),
            ..Default::default()
        };

        match storage.put_object_metadata(&bucket, &object, &popts).await {
            Ok(u) => object_info = u,
            Err(e) => {
                state_persisted = false;
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

        if let Some(stats) = runtime_sources::replication_stats() {
            for tgt in &rinfos.targets {
                if tgt.replication_status != tgt.prev_replication_status {
                    stats
                        .update(&bucket, tgt, tgt.replication_status.clone(), tgt.prev_replication_status.clone())
                        .await;
                }
            }
        }
    }

    let event_name = if replication_status == ReplicationStatusType::Completed {
        EventName::ObjectReplicationComplete.to_string()
    } else {
        EventName::ObjectReplicationFailed.to_string()
    };

    send_local_event(EventArgs {
        event_name,
        bucket_name: bucket.clone(),
        object: object_info,
        user_agent: "Internal: [Replication]".to_string(),
        ..Default::default()
    });

    if rinfos.replication_status() != ReplicationStatusType::Completed
        && roi.replication_status_internal == rinfos.replication_status_internal()
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

    (merged_state, state_persisted)
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
            debug!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
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
                if e.as_service_error().is_some_and(|se| se.is_not_found()) || has_raw_status(&e, 404) {
                    // Object not on target yet → fall through to PUT.
                } else if is_version_id_format_mismatch(&e) {
                    // Version-ID format mismatch: retry without versionId and compare ETags.
                    match head_object_fallback(&tgt_client, &object).await {
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
                                arn = %tgt_client.arn,
                                operation = "head_object_fallback",
                                error = %e2,
                                "Replication target operation failed"
                            );
                            return rinfo;
                        }
                    }
                } else {
                    rinfo.error = Some(e.to_string());
                    warn!(
                        event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        bucket = %bucket,
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
                .map(|assigned_version_id| {
                    audit_target_version_identity(
                        &tgt_client,
                        &put_opts.internal.source_version_id,
                        assigned_version_id.as_deref(),
                    )
                })
                .map_err(|e| std::io::Error::other(e.to_string()));
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
    debug!(
        event = EVENT_RESYNC_RUNTIME_SKIPPED,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
        bucket = %bucket,
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
            if is_version_id_format_mismatch(&e) {
                // Version-ID format mismatch: retry without versionId and compare ETags.
                match head_object_fallback(tgt_client, object).await {
                    Ok(Some(oi)) => {
                        replication_action = if replication_etags_match(object_info.etag.as_deref(), oi.e_tag.as_deref()) {
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
                        replication_action = ReplicationAction::All;
                    }
                    Err(e2) => {
                        rinfo.error = Some(e2.to_string());
                        debug!(
                            event = EVENT_RESYNC_RUNTIME_SKIPPED,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                            bucket = %bucket,
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
            } else if e.as_service_error().is_some_and(|se| se.is_not_found()) {
                replication_action = ReplicationAction::All;
            } else {
                rinfo.error = Some(e.to_string());
                debug!(
                    event = EVENT_RESYNC_RUNTIME_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket = %bucket,
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
            .map(|assigned_version_id| {
                audit_target_version_identity(
                    ctx.tgt_client,
                    &ctx.put_opts.internal.source_version_id,
                    assigned_version_id.as_deref(),
                )
            })
            .map_err(|e| std::io::Error::other(e.to_string()));
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
    let mut attempts = 1;
    let upload_id = loop {
        match cli.create_multipart_upload(dst_bucket, object, &put_opts).await {
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

    let mut uploaded_parts: Vec<CompletedPart> = Vec::new();

    let mut header_size = replication_put_object_header_size(&put_opts);
    let mut offset: i64 = 0;
    for part_info in object_info.parts.iter() {
        // Ciphertext passthrough (raw read) ranges over the stored part
        // bytes; decrypted reads range over the logical plaintext parts.
        let part_size = if obj_opts.raw_data_movement_read {
            part_info.size as i64
        } else {
            part_info.actual_size
        };
        let part_plan = replication_multipart_part_plan(ReplicationMultipartPartInput {
            offset,
            part_number: part_info.number,
            part_size,
        })
        .map_err(|err| std::io::Error::other(err.to_string()))?;
        let range_spec = HTTPRangeSpec {
            is_suffix_length: false,
            start: part_plan.range.start,
            end: part_plan.range.end,
        };
        offset = part_plan.next_offset;

        let part_reader = storage
            .get_object_reader(src_bucket, object, Some(range_spec), HeaderMap::new(), obj_opts)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let part_stream = wrap_with_bandwidth_monitor_with_header(part_reader.stream, src_bucket, arn, header_size);
        header_size = 0;
        let byte_stream = async_read_to_bytestream(part_stream);

        let object_part = cli
            .put_object_part(
                dst_bucket,
                object,
                &upload_id,
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
            &upload_id,
            uploaded_parts,
            &replication_complete_multipart_options(
                actual_size,
                object_info.etag.clone().unwrap_or_default(),
                object_info.mod_time,
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

    async fn register_test_target(target: &Arc<TargetClient>) {
        ReplicationTargetStore::register_test_target(target).await;
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
}
