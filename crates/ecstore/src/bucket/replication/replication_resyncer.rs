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
#[cfg(test)]
use super::replication_error_boundary::Error;
use super::replication_error_boundary::{Result, is_err_object_not_found, is_err_version_not_found};
use super::replication_event_sink::{EventArgs, send_event, send_local_event};
use super::replication_filemeta_boundary::{
    NULL_VERSION_ID, REPLICATE_EXISTING, REPLICATE_EXISTING_DELETE, ReplicateDecision, ReplicateObjectInfo, ReplicatedInfos,
    ReplicatedTargetInfo, ReplicationAction, ReplicationState, ReplicationStatusType, ReplicationType, VersionPurgeStatusType,
    get_replication_state, parse_replicate_decision, replication_statuses_map, target_reset_header, version_purge_statuses_map,
};
use super::replication_lock_boundary::ReplicationLockTiming;
use super::replication_logging::{EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REPLICATION_RESYNC};
use super::replication_metadata_boundary::ReplicationMetadataStore;
#[cfg(test)]
use super::replication_msgp_boundary::ReplicationMsgpCodec;
use super::replication_object_config::{ReplicationConfig, get_replication_config, must_replicate};
#[cfg(test)]
use super::replication_object_decision_boundary::should_retry_delete_marker_purge;
use super::replication_object_decision_boundary::{
    MustReplicateOptions, ReplicationMultipartPartInput, heal_uses_delete_replication_path,
    is_retryable_delete_replication_head_error, is_version_delete_replication, replication_etags_match,
    replication_multipart_complete_actual_size, replication_multipart_part_plan,
};
use super::replication_pool::get_global_replication_pool;
use super::replication_queue_boundary::{DeletedObjectReplicationInfo, ReplicationQueueAdmission};
use super::replication_resync_boundary::ResyncStatusType;
use super::replication_resync_boundary::{
    BucketReplicationResyncStatus, ResyncOpts, TargetReplicationResyncStatus, encode_resync_file, is_version_id_mismatch,
    resync_state_accepts_update, sanitize_resync_error_detail, should_count_head_proxy_failure,
};
#[cfg(test)]
use super::replication_resync_boundary::{RESYNC_META_FORMAT, RESYNC_META_VERSION, WIRE_ZERO_TIME_UNIX, decode_resync_file};
use super::replication_storage_boundary::{
    AdvancedGetOptions, EcstoreObjectOperations, HTTPRangeSpec, ObjectInfo, ObjectOptions, ObjectToDelete,
    ReplicationDeletedObject, ReplicationObjectIO, ReplicationStorage, StatObjectOptions, WalkOptions,
};
use super::replication_target_boundary::{
    PutObjectOptions, PutObjectPartOptions, ReplicationTargetStore, TargetClient, replication_action_for_target_head,
    replication_complete_multipart_options, replication_delete_marker_purge_remove_options, replication_delete_remove_options,
    replication_force_delete_remove_options, replication_object_is_ssec_encrypted, replication_put_object_header_size,
    replication_put_object_options, replication_target_head_is_newer_null_version,
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
#[cfg(test)]
use rmp_serde;
use rustfs_s3_types::EventName;
use rustfs_utils::http::{
    AMZ_TAGGING_DIRECTIVE, SUFFIX_REPLICATION_RESET, SUFFIX_REPLICATION_STATUS, has_internal_suffix, insert_str,
};
use rustfs_utils::{DEFAULT_SIP_HASH_KEY, sip_hash};
#[cfg(test)]
use s3s::dto::ReplicationConfiguration;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncRead;
use tokio::sync::RwLock;
use tokio::task::{Id as TaskId, JoinError, JoinHandle, JoinSet};
use tokio::time::Duration as TokioDuration;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, trace, warn};

const BACKGROUND_WALKDIR_TIMEOUT: TokioDuration = TokioDuration::from_secs(60);
use uuid::Uuid;

const EVENT_RESYNC_STATUS_UPDATE_SKIPPED: &str = "replication_resync_status_update_skipped";
const EVENT_RESYNC_OBJECT_PROCESSED: &str = "replication_resync_object_processed";
const EVENT_RESYNC_RUNTIME_SKIPPED: &str = "replication_resync_runtime_skipped";
const EVENT_REPLICATION_DELETE_SKIPPED: &str = "replication_delete_skipped";
const EVENT_REPLICATION_FORCE_DELETE_SKIPPED: &str = "replication_force_delete_skipped";
const EVENT_RESYNC_TASK_FAILED: &str = "replication_resync_task_failed";
const EVENT_RESYNC_TARGET_OPERATION_FAILED: &str = "replication_resync_target_operation_failed";
const EVENT_RESYNC_RUNTIME_CHANNEL_FAILED: &str = "replication_resync_runtime_channel_failed";
const REPLICATION_TARGET_OFFLINE_ERROR_MARKERS: &[&str] = &[
    "dispatch failure",
    "timeouterror",
    "timed out",
    "connection refused",
    "connection reset",
    "connection closed",
    "connection aborted",
    "broken pipe",
    "dns error",
    "failed to lookup address",
    "name or service not known",
    "deadline has elapsed",
    "tcp connect error",
];

const RESYNC_TIME_INTERVAL: TokioDuration = TokioDuration::from_secs(60);
const MAX_PARALLEL_DELETE_MARKER_RECONCILIATIONS: usize = 32;

static WARNED_MONITOR_UNINIT: std::sync::Once = std::sync::Once::new();

#[cfg(test)]
struct DeleteReplicationSourceCheckProbeState {
    bucket: String,
    object: String,
    responses: Vec<Option<(bool, Option<ReplicationState>)>>,
    pause_on_call: Option<usize>,
    calls: std::sync::atomic::AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

#[cfg(test)]
pub(super) struct DeleteReplicationSourceCheckProbe {
    state: Arc<DeleteReplicationSourceCheckProbeState>,
    _exclusive: tokio::sync::OwnedMutexGuard<()>,
}

#[cfg(test)]
static DELETE_REPLICATION_SOURCE_CHECK_PROBE: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<DeleteReplicationSourceCheckProbeState>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
static DELETE_REPLICATION_SOURCE_CHECK_EXCLUSIVE: std::sync::OnceLock<Arc<tokio::sync::Mutex<()>>> = std::sync::OnceLock::new();

#[cfg(test)]
struct DeleteRetryCaptureState {
    bucket: String,
    object: String,
    batches: Vec<Vec<DeletedObjectReplicationInfo>>,
}

#[cfg(test)]
struct DeleteRetryCapture {
    bucket: String,
    object: String,
    _exclusive: tokio::sync::OwnedMutexGuard<()>,
}

#[cfg(test)]
static DELETE_RETRY_CAPTURE: std::sync::OnceLock<std::sync::Mutex<Option<DeleteRetryCaptureState>>> = std::sync::OnceLock::new();
#[cfg(test)]
static DELETE_RETRY_CAPTURE_EXCLUSIVE: std::sync::OnceLock<Arc<tokio::sync::Mutex<()>>> = std::sync::OnceLock::new();

#[cfg(test)]
impl DeleteRetryCapture {
    async fn install(bucket: &str, object: &str) -> Self {
        let exclusive = DELETE_RETRY_CAPTURE_EXCLUSIVE
            .get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
            .lock_owned()
            .await;
        let state = DeleteRetryCaptureState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            batches: Vec::new(),
        };
        *DELETE_RETRY_CAPTURE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("delete retry capture mutex should not poison") = Some(state);
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
            _exclusive: exclusive,
        }
    }

    fn batches(&self) -> Vec<Vec<DeletedObjectReplicationInfo>> {
        DELETE_RETRY_CAPTURE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("delete retry capture mutex should not poison")
            .as_ref()
            .filter(|state| state.bucket == self.bucket && state.object == self.object)
            .map(|state| state.batches.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
impl Drop for DeleteRetryCapture {
    fn drop(&mut self) {
        let mut slot = DELETE_RETRY_CAPTURE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("delete retry capture mutex should not poison");
        if slot
            .as_ref()
            .is_some_and(|state| state.bucket == self.bucket && state.object == self.object)
        {
            *slot = None;
        }
    }
}

#[cfg(test)]
fn capture_delete_retries(dobj: &DeletedObjectReplicationInfo, retries: &[DeletedObjectReplicationInfo]) -> bool {
    let mut slot = DELETE_RETRY_CAPTURE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("delete retry capture mutex should not poison");
    let Some(state) = slot
        .as_mut()
        .filter(|state| state.bucket == dobj.bucket && state.object == dobj.delete_object.object_name)
    else {
        return false;
    };
    state.batches.push(retries.to_vec());
    true
}

#[cfg(test)]
impl DeleteReplicationSourceCheckProbe {
    pub(super) async fn install(bucket: &str, object: &str, responses: Vec<bool>, pause_on_call: Option<usize>) -> Self {
        Self::install_states(
            bucket,
            object,
            responses.into_iter().map(|matches| Some((matches, None))).collect(),
            pause_on_call,
        )
        .await
    }

    pub(super) async fn install_results(
        bucket: &str,
        object: &str,
        responses: Vec<Option<bool>>,
        pause_on_call: Option<usize>,
    ) -> Self {
        Self::install_states(
            bucket,
            object,
            responses
                .into_iter()
                .map(|response| response.map(|matches| (matches, None)))
                .collect(),
            pause_on_call,
        )
        .await
    }

    async fn install_states(
        bucket: &str,
        object: &str,
        responses: Vec<Option<(bool, Option<ReplicationState>)>>,
        pause_on_call: Option<usize>,
    ) -> Self {
        assert!(!responses.is_empty(), "source-check probe needs at least one response");
        let exclusive = DELETE_REPLICATION_SOURCE_CHECK_EXCLUSIVE
            .get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
            .lock_owned()
            .await;
        let state = Arc::new(DeleteReplicationSourceCheckProbeState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            responses,
            pause_on_call,
            calls: std::sync::atomic::AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        let mut slot = DELETE_REPLICATION_SOURCE_CHECK_PROBE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("source-check probe mutex should not poison");
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self {
            state,
            _exclusive: exclusive,
        }
    }

    async fn wait_until_paused(&self) {
        let pause_on_call = self.state.pause_on_call.expect("probe should have a paused call");
        tokio::time::timeout(TokioDuration::from_secs(30), async {
            loop {
                if self.state.calls.load(std::sync::atomic::Ordering::Acquire) >= pause_on_call {
                    return;
                }
                self.state.entered.notified().await;
            }
        })
        .await
        .expect("source check should reach the deterministic pause");
    }

    fn release(&self) {
        self.state.release.add_permits(1);
    }
}

#[cfg(test)]
impl Drop for DeleteReplicationSourceCheckProbe {
    fn drop(&mut self) {
        let mut slot = DELETE_REPLICATION_SOURCE_CHECK_PROBE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("source-check probe mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
        drop(slot);
        self.state.release.add_permits(1);
    }
}

#[cfg(test)]
fn delete_replication_source_check_probe(bucket: &str, object: &str) -> Option<Arc<DeleteReplicationSourceCheckProbeState>> {
    DELETE_REPLICATION_SOURCE_CHECK_PROBE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("source-check probe mutex should not poison")
        .as_ref()
        .filter(|state| state.bucket == bucket && state.object == object)
        .cloned()
}

#[cfg(test)]
async fn probe_delete_replication_source_check(
    state: Arc<DeleteReplicationSourceCheckProbeState>,
) -> Result<(bool, Option<ReplicationState>)> {
    let call = state.calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1;
    if state.pause_on_call == Some(call) {
        state.entered.notify_one();
        state
            .release
            .acquire()
            .await
            .expect("source-check probe should remain open")
            .forget();
    }
    state
        .responses
        .get(call - 1)
        .expect("source-check probe should define every response")
        .clone()
        .ok_or_else(|| Error::other("injected source-check failure"))
}

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

fn is_head_proxy_failure(err: &SdkError<HeadObjectError>) -> bool {
    let (is_not_found, code) = err
        .as_service_error()
        .map(|service_err| (service_err.is_not_found(), service_err.code()))
        .unwrap_or((false, None));
    let raw_status = err.raw_response().map(|resp| resp.status().as_u16());
    should_count_head_proxy_failure(is_not_found, code, raw_status)
}

async fn record_proxy_request(bucket: &str, api: &str, is_err: bool) {
    if let Some(stats) = runtime_sources::replication_stats() {
        stats.inc_proxy(bucket, api, is_err).await;
    }
}

async fn head_object_with_proxy_stats(
    source_bucket: &str,
    target_client: &TargetClient,
    target_bucket: &str,
    object: &str,
    version_id: Option<String>,
) -> std::result::Result<HeadObjectOutput, SdkError<HeadObjectError>> {
    let result = target_client.head_object(target_bucket, object, version_id).await;
    let is_err = result.as_ref().err().is_some_and(is_head_proxy_failure);
    record_proxy_request(source_bucket, "HeadObject", is_err).await;
    result
}

fn is_version_id_format_mismatch(err: &SdkError<HeadObjectError>) -> bool {
    let code = err.as_service_error().and_then(|se| se.code());
    let raw_status = err.raw_response().map(|r| r.status().as_u16());
    is_version_id_mismatch(code, raw_status)
}

fn is_replication_target_offline_error(err: &(impl Display + ?Sized)) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    REPLICATION_TARGET_OFFLINE_ERROR_MARKERS
        .iter()
        .any(|marker| message.contains(marker))
}

async fn mark_replication_target_offline_if_needed(target_client: &Arc<TargetClient>, err: &(impl Display + ?Sized)) {
    if is_replication_target_offline_error(err) {
        ReplicationTargetStore::mark_target_offline(target_client).await;
    }
}

async fn head_object_fallback(
    source_bucket: &str,
    tgt_client: &TargetClient,
    object: &str,
) -> std::result::Result<Option<HeadObjectOutput>, SdkError<HeadObjectError>> {
    match head_object_with_proxy_stats(source_bucket, tgt_client, &tgt_client.bucket, object, None).await {
        Ok(oi) => Ok(Some(oi)),
        Err(e) if e.as_service_error().is_some_and(|se| se.is_not_found()) || has_raw_status(&e, 404) => Ok(None),
        Err(e) => Err(e),
    }
}

static RESYNC_WORKER_COUNT: usize = 10;

fn resync_status_duration(
    status: ResyncStatusType,
    start_time: Option<OffsetDateTime>,
    now: OffsetDateTime,
) -> Option<std::time::Duration> {
    if !matches!(
        status,
        ResyncStatusType::ResyncCompleted | ResyncStatusType::ResyncFailed | ResyncStatusType::ResyncCanceled
    ) {
        return None;
    }

    let millis = (now - start_time?).whole_milliseconds();
    if millis < 0 {
        return None;
    }

    let millis = if millis > i128::from(u64::MAX) {
        u64::MAX
    } else {
        u64::try_from(millis).ok()?
    };
    Some(std::time::Duration::from_millis(millis))
}

type ResyncCancelKey = (String, String, String);

#[derive(Debug)]
pub struct ReplicationResyncer {
    pub status_map: Arc<RwLock<HashMap<String, BucketReplicationResyncStatus>>>,
    pub worker_size: usize,
    pub(crate) cancel_tokens: Arc<RwLock<HashMap<ResyncCancelKey, CancellationToken>>>,
}

impl ReplicationResyncer {
    pub async fn new() -> Self {
        Self {
            status_map: Arc::new(RwLock::new(HashMap::new())),
            worker_size: RESYNC_WORKER_COUNT,
            cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
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
        // per-object worker pool (mpsc channels, line ~877) already provides the right
        // concurrency limit.
        if cancellation_token.is_cancelled() {
            return;
        }

        // Acquire a cluster-wide leader lock for this (bucket, ARN) pair so that only
        // one node runs the resync scan at a time. Without this, every cluster node would
        // scan and replicate every object independently, causing N-fold duplicate traffic.
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
                return;
            }
        };
        let _resync_leader_guard = match resync_ns_lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
            Ok(g) => g,
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
                return;
            }
        };

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
                return;
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
                return;
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
            return;
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
            return;
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
            return;
        };

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

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        if let Err(err) = storage
            .clone()
            .walk(
                cancellation_token.clone(),
                &opts.bucket,
                "",
                tx.clone(),
                WalkOptions::default().with_walkdir_timeouts(BACKGROUND_WALKDIR_TIMEOUT),
            )
            .await
        {
            error!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %opts.bucket,
                arn = %opts.arn,
                reason = "walk_failed",
                error = %err,
                "Replication resync bucket walk failed"
            );
            self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                .await;
            return;
        }
        drop(tx);

        let status = {
            self.status_map
                .read()
                .await
                .get(&opts.bucket)
                .and_then(|status| status.targets_map.get(&opts.arn))
                .cloned()
                .unwrap_or_default()
        };

        // An empty checkpoint means no per-object progress was persisted before the
        // interruption: resume from the beginning, otherwise `object.name != checkpoint`
        // below would skip every object and mark the resync completed without work.
        let mut last_checkpoint = if (status.resync_status == ResyncStatusType::ResyncStarted
            || status.resync_status == ResyncStatusType::ResyncFailed)
            && !status.object.is_empty()
        {
            Some(status.object)
        } else {
            None
        };

        let mut worker_txs = Vec::new();
        // mpsc, not broadcast: a lagging broadcast receiver returns Err(Lagged) which
        // would end the collector and silently drop every subsequent worker result.
        let (results_tx, mut results_rx) = tokio::sync::mpsc::channel::<TargetReplicationResyncStatus>(RESYNC_WORKER_COUNT * 4);

        let opts_clone = opts.clone();
        let self_clone = self.clone();

        let mut futures = Vec::new();

        let results_fut = tokio::spawn(async move {
            while let Some(st) = results_rx.recv().await {
                self_clone.inc_stats(&st, opts_clone.clone()).await;
            }
        });

        futures.push(results_fut);

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
                while let Some(mut roi) = rx.recv().await {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    if roi.delete_marker || !roi.version_purge_status.is_empty() {
                        let (version_id, dm_version_id) = if roi.version_purge_status.is_empty() {
                            (None, roi.version_id)
                        } else {
                            (roi.version_id, None)
                        };

                        let doi = DeletedObjectReplicationInfo {
                            delete_object: ReplicationDeletedObject {
                                object_name: roi.name.clone(),
                                delete_marker_version_id: dm_version_id,
                                version_id,
                                replication_state: roi.replication_state.clone(),
                                delete_marker: roi.delete_marker,
                                delete_marker_mtime: roi.mod_time,
                                ..Default::default()
                            },
                            bucket: roi.bucket.clone(),
                            event_type: REPLICATE_EXISTING_DELETE.to_string(),
                            op_type: ReplicationType::ExistingObject,
                            target_arn: target_arn.clone(),
                            ..Default::default()
                        };
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

                    let head_result = head_object_with_proxy_stats(
                        &bucket_name,
                        target_client.as_ref(),
                        &target_client.bucket,
                        &roi.name,
                        roi.version_id.map(|v| v.to_string()),
                    )
                    .await;
                    let (size, err) = match head_result {
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
                            match head_object_fallback(&bucket_name, target_client.as_ref(), &roi.name).await {
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
                    };

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
                    st.error = err.as_ref().and_then(resync_target_error_detail);

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
                let worker_failed = finish_resync_workers(worker_txs, results_tx, futures, false).await;
                if worker_failed {
                    error!(
                        event = EVENT_RESYNC_TASK_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        bucket = %opts.bucket,
                        arn = %opts.arn,
                        reason = "worker_join_failed_after_object_info_error",
                        "Replication resync worker cleanup observed task failure"
                    );
                }
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }

            if cancellation_token.is_cancelled() {
                finish_resync_workers(worker_txs, results_tx, futures, true).await;
                self.resync_bucket_mark_status(ResyncStatusType::ResyncCanceled, opts.clone(), storage.clone())
                    .await;
                return;
            }

            let Some(object) = res.item else {
                continue;
            };

            if heal
                && let Some(checkpoint) = &last_checkpoint
                && &object.name != checkpoint
            {
                continue;
            }
            last_checkpoint = None;

            let roi = match get_heal_replicate_object_info(&object, &rcfg).await {
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
                    let worker_failed = finish_resync_workers(worker_txs, results_tx, futures, false).await;
                    if worker_failed {
                        error!(
                            event = EVENT_RESYNC_TASK_FAILED,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                            bucket = %opts.bucket,
                            arn = %opts.arn,
                            reason = "worker_join_failed_after_classification_error",
                            "Replication resync worker cleanup observed task failure"
                        );
                    }
                    self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                        .await;
                    return;
                }
            };
            if !roi.existing_obj_resync.must_resync() {
                continue;
            }

            if cancellation_token.is_cancelled() {
                finish_resync_workers(worker_txs, results_tx, futures, true).await;
                self.resync_bucket_mark_status(ResyncStatusType::ResyncCanceled, opts.clone(), storage.clone())
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
                let worker_failed = finish_resync_workers(worker_txs, results_tx, futures, false).await;
                if worker_failed {
                    error!(
                        event = EVENT_RESYNC_TASK_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                        bucket = %opts.bucket,
                        arn = %opts.arn,
                        reason = "worker_join_failed_after_queue_send_error",
                        "Replication resync worker cleanup observed task failure"
                    );
                }
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }
        }

        let worker_failed = finish_resync_workers(worker_txs, results_tx, futures, false).await;
        let target_failed = self.target_has_resync_failures(&opts).await;
        let status = if worker_failed || target_failed {
            ResyncStatusType::ResyncFailed
        } else {
            ResyncStatusType::ResyncCompleted
        };

        self.resync_bucket_mark_status(status, opts.clone(), storage.clone()).await;
    }
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
    let actual_size = oi.get_actual_size().unwrap_or_default();

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

pub async fn replicate_delete<S: ReplicationStorage>(mut dobj: DeletedObjectReplicationInfo, storage: Arc<S>) -> bool {
    if dobj.delete_object.force_delete {
        replicate_force_delete_to_targets(&dobj, storage).await;
        return true;
    }

    let bucket = dobj.bucket.clone();
    let version_id = if let Some(version_id) = &dobj.delete_object.delete_marker_version_id {
        Some(version_id.to_owned())
    } else {
        dobj.delete_object.version_id
    };

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

    let replication_guard = match ns_lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
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

    let mut rinfos = ReplicatedInfos {
        replication_timestamp: Some(OffsetDateTime::now_utc()),
        targets: Vec::with_capacity(dsc.targets_map.len()),
    };
    let mut source_marker_was_present = false;
    let mut source_marker_is_absent = false;
    let mut run_target_operations = true;

    if dobj.delete_object.delete_marker
        && let Some(delete_marker_version_id) = dobj.delete_object.delete_marker_version_id
    {
        match source_delete_marker_matches(
            storage.clone(),
            bucket.clone(),
            dobj.delete_object.object_name.clone(),
            delete_marker_version_id,
            &replication_guard,
        )
        .await
        {
            Ok((true, current_state)) => {
                source_marker_was_present = true;
                refresh_delete_replication_state(&mut dobj, current_state);
            }
            Ok((false, _)) => {
                run_target_operations = false;
                source_marker_is_absent = true;
                debug!(
                    event = EVENT_REPLICATION_DELETE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket,
                    object = dobj.delete_object.object_name,
                    version_id = %delete_marker_version_id,
                    reason = "source_version_missing_or_replaced",
                    "Reconciling a stale delete-marker replication task"
                );
                if target_delete_marker_version_conflicts(&dobj) {
                    rinfos.targets = failed_delete_targets_for_decision(
                        &dobj,
                        &dsc,
                        "target delete-marker version metadata conflicts with the retry",
                    );
                } else {
                    let target_versions = delete_marker_target_versions(&dobj, &rinfos);
                    rinfos.targets = replicate_delete_marker_purge_to_targets(
                        bucket.clone(),
                        dobj.clone(),
                        dsc.clone(),
                        target_versions.clone(),
                        true,
                    )
                    .await;
                }
            }
            Err(err) => {
                run_target_operations = false;
                for tgt_entry in dsc.targets_map.values() {
                    if !tgt_entry.replicate || (!dobj.target_arn.is_empty() && dobj.target_arn != tgt_entry.arn) {
                        continue;
                    }
                    let mut rinfo = delete_target_state(&dobj, &tgt_entry.arn);
                    rinfo.replication_status = ReplicationStatusType::Failed;
                    rinfo.error = Some(err.to_string());
                    rinfos.targets.push(rinfo);
                }
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

    if run_target_operations && target_delete_marker_version_conflicts(&dobj) {
        run_target_operations = false;
        rinfos.targets =
            failed_delete_targets_for_decision(&dobj, &dsc, "target delete-marker version metadata conflicts with the retry");
    }

    let mut join_set = JoinSet::new();
    let mut target_tasks = HashMap::new();
    let target_task_delete = Arc::new(dobj.clone());

    // Process each target
    let target_arns = dobj.admitted_target_arns();
    for tgt_entry in dsc.targets_map.values().filter(|_| run_target_operations) {
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
            let mut rinfo = delete_target_state(&dobj, &tgt_entry.arn);
            if is_version_delete_replication(&dobj.delete_object) {
                rinfo.version_purge_status = VersionPurgeStatusType::Failed;
            } else {
                rinfo.replication_status = ReplicationStatusType::Failed;
            }
            rinfo.error = Some("replication target client unavailable".to_string());
            rinfos.targets.push(rinfo);
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

        let dobj_clone = Arc::clone(&target_task_delete);

        // Spawn task in the join set
        let task = join_set.spawn(async move { replicate_delete_to_target(dobj_clone.as_ref(), tgt_client.clone()).await });
        target_tasks.insert(task.id(), tgt_entry.arn.clone());
    }

    // Collect all results
    while let Some(result) = join_set.join_next_with_id().await {
        match delete_target_join_result(result, &mut target_tasks, &dobj) {
            Ok(tgt_info) => rinfos.targets.push(tgt_info),
            Err((e, rinfo)) => {
                rinfos.targets.push(*rinfo);
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

    let mut lock_lost = replication_guard.is_lock_lost();
    if run_target_operations
        && source_marker_was_present
        && !lock_lost
        && let Some(delete_marker_version_id) = dobj.delete_object.delete_marker_version_id
    {
        match source_delete_marker_matches(
            storage.clone(),
            bucket.clone(),
            dobj.delete_object.object_name.clone(),
            delete_marker_version_id,
            &replication_guard,
        )
        .await
        {
            Ok((true, current_state)) => {
                refresh_delete_replication_state(&mut dobj, current_state);
            }
            Ok((false, _)) => {
                source_marker_is_absent = true;
                let target_versions = delete_marker_target_versions(&dobj, &rinfos);
                merge_delete_target_results(
                    &mut rinfos,
                    replicate_delete_marker_purge_to_targets(
                        bucket.clone(),
                        dobj.clone(),
                        dsc.clone(),
                        target_versions.clone(),
                        false,
                    )
                    .await,
                );
            }
            Err(err) => {
                for target in &mut rinfos.targets {
                    target.replication_status = ReplicationStatusType::Failed;
                    if target.error.is_none() {
                        target.error = Some(err.to_string());
                    }
                }
                debug!(
                    event = EVENT_REPLICATION_DELETE_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                    bucket,
                    object = dobj.delete_object.object_name,
                    version_id = %delete_marker_version_id,
                    error = %err,
                    reason = "source_state_post_verification_failed",
                    "Failed to verify source delete-marker state after replication"
                );
            }
        }
    }

    lock_lost |= replication_guard.is_lock_lost();
    if lock_lost {
        for target in &mut rinfos.targets {
            if is_version_delete_replication(&dobj.delete_object) {
                target.version_purge_status = VersionPurgeStatusType::Failed;
            } else {
                target.replication_status = ReplicationStatusType::Failed;
            }
            target.error.get_or_insert_with(|| "replication lock lease lost".to_string());
        }
    }

    let is_version_purge = is_version_delete_replication(&dobj.delete_object);

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

    let mut drs = get_replication_state(
        &rinfos,
        &dobj.delete_object.replication_state.clone().unwrap_or_default(),
        dobj.delete_object.version_id.map(|v| v.to_string()),
    );
    if replication_status != prev_status {
        drs.replication_timestamp = Some(OffsetDateTime::now_utc());
    }
    let target_version_state_conflicts = target_delete_marker_version_conflicts(&dobj);

    let event_name = if replication_status == ReplicationStatusType::Completed {
        EventName::ObjectReplicationComplete.to_string()
    } else {
        EventName::ObjectReplicationFailed.to_string()
    };

    let mut state_applied = source_marker_is_absent;
    let versioned = ReplicationVersioningStore::prefix_enabled(&bucket, &dobj.delete_object.object_name).await;
    let version_suspended = ReplicationVersioningStore::prefix_suspended(&bucket, &dobj.delete_object.object_name).await;
    lock_lost |= replication_guard.is_lock_lost();
    if lock_lost {
        mark_delete_targets_failed(&mut rinfos, is_version_purge, "replication lock lease lost");
    }
    if !source_marker_is_absent && !lock_lost && !target_version_state_conflicts {
        let mut delete_options = ObjectOptions {
            version_id: version_id.map(|v| v.to_string()),
            mod_time: dobj.delete_object.delete_marker_mtime,
            delete_replication: Some(drs),
            versioned,
            version_suspended,
            ..Default::default()
        };
        if let Some(signal) = replication_guard.lock_lost_signal() {
            delete_options.add_namespace_lock_lost_signal(signal);
        }
        match storage
            .delete_object(&bucket, &dobj.delete_object.object_name, delete_options)
            .await
        {
            Ok(object) => {
                if replication_guard.is_lock_lost() {
                    mark_delete_targets_failed(&mut rinfos, is_version_purge, "replication lock lease lost during commit");
                } else {
                    state_applied = true;
                    send_event(EventArgs {
                        event_name: event_name.clone(),
                        bucket_name: bucket.clone(),
                        object,
                        ..Default::default()
                    });
                }
            }
            Err(e) => {
                mark_delete_targets_failed(&mut rinfos, is_version_purge, &e.to_string());
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
                    event_name: event_name.clone(),
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

    drop(replication_guard);
    let has_failed_targets = rinfos.targets.iter().any(|target| {
        target.replication_status == ReplicationStatusType::Failed
            || target.version_purge_status == VersionPurgeStatusType::Failed
    });
    let retry_admission = persist_failed_delete_replications(&dobj, &rinfos).await;
    if let Some(stats) = runtime_sources::replication_stats() {
        for tgt in &rinfos.targets {
            if tgt.replication_status != tgt.prev_replication_status {
                stats
                    .update(&bucket, tgt, tgt.replication_status.clone(), tgt.prev_replication_status.clone())
                    .await;
            }
        }
    }
    if target_version_state_conflicts {
        false
    } else {
        (state_applied && !has_failed_targets) || (has_failed_targets && retry_admission == ReplicationQueueAdmission::Queued)
    }
}

fn delete_target_join_result(
    result: std::result::Result<(TaskId, ReplicatedTargetInfo), JoinError>,
    target_tasks: &mut HashMap<TaskId, String>,
    dobj: &DeletedObjectReplicationInfo,
) -> std::result::Result<ReplicatedTargetInfo, (JoinError, Box<ReplicatedTargetInfo>)> {
    match result {
        Ok((task_id, target)) => {
            target_tasks.remove(&task_id);
            Ok(target)
        }
        Err(error) => {
            let target_arn = target_tasks.remove(&error.id()).unwrap_or_default();
            let mut target = delete_target_state(dobj, &target_arn);
            if is_version_delete_replication(&dobj.delete_object) {
                target.version_purge_status = VersionPurgeStatusType::Failed;
            } else {
                target.replication_status = ReplicationStatusType::Failed;
            }
            target.error = Some(error.to_string());
            Err((error, Box::new(target)))
        }
    }
}

async fn source_delete_marker_matches<S: EcstoreObjectOperations>(
    storage: Arc<S>,
    bucket: String,
    object_name: String,
    delete_marker_version_id: Uuid,
    _replication_guard: &rustfs_lock::NamespaceLockGuard,
) -> Result<(bool, Option<ReplicationState>)> {
    #[cfg(test)]
    if let Some(probe) = delete_replication_source_check_probe(&bucket, &object_name) {
        return probe_delete_replication_source_check(probe).await;
    }

    match storage
        .get_object_info(
            &bucket,
            &object_name,
            &ObjectOptions {
                version_id: Some(delete_marker_version_id.to_string()),
                versioned: ReplicationVersioningStore::prefix_enabled(&bucket, &object_name).await,
                version_suspended: ReplicationVersioningStore::prefix_suspended(&bucket, &object_name).await,
                ..Default::default()
            },
        )
        .await
    {
        Ok(info) if source_delete_marker_matches_id(&info, delete_marker_version_id) => {
            Ok((true, Some(info.replication_state())))
        }
        Ok(_) => Ok((false, None)),
        Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => Ok((false, None)),
        Err(err) => Err(err),
    }
}

fn source_delete_marker_matches_id(info: &ObjectInfo, delete_marker_version_id: Uuid) -> bool {
    info.delete_marker && info.version_id == Some(delete_marker_version_id)
}

fn refresh_delete_replication_state(dobj: &mut DeletedObjectReplicationInfo, current_state: Option<ReplicationState>) {
    if let Some(current_state) = current_state {
        dobj.blocked_delete_marker_version_state = false;
        if !dobj.target_arn.is_empty()
            && let Some(version_id) = current_state.target_delete_marker_version_ids.get(&dobj.target_arn)
        {
            dobj.target_delete_marker_version_id = Some(version_id.clone());
        }
        dobj.delete_object.replication_state = Some(current_state);
    }
}

fn delete_target_state(dobj: &DeletedObjectReplicationInfo, arn: &str) -> ReplicatedTargetInfo {
    let mut rinfo = dobj
        .delete_object
        .replication_state
        .clone()
        .unwrap_or_default()
        .target_state(arn);
    if dobj.target_arn == arn
        && let Some(version_id) = dobj.target_delete_marker_version_id.as_ref()
    {
        rinfo.target_delete_marker_version_id = Some(version_id.clone());
    }
    rinfo.op_type = dobj.op_type;
    rinfo
}

fn target_delete_marker_version_conflicts(dobj: &DeletedObjectReplicationInfo) -> bool {
    if dobj.blocked_delete_marker_version_state {
        return true;
    }
    if target_delete_marker_version_metadata_corrupt(dobj) {
        return true;
    }
    if dobj.target_arn.is_empty() {
        return false;
    }
    let Some(retry_version_id) = dobj.target_delete_marker_version_id.as_deref() else {
        return false;
    };
    dobj.delete_object
        .replication_state
        .as_ref()
        .and_then(|state| state.target_delete_marker_version_ids.get(&dobj.target_arn))
        .is_some_and(|persisted_version_id| persisted_version_id != retry_version_id)
}

fn target_delete_marker_version_metadata_corrupt(dobj: &DeletedObjectReplicationInfo) -> bool {
    dobj.delete_object
        .replication_state
        .as_ref()
        .is_some_and(|state| state.target_delete_marker_version_ids_corrupt)
}

fn failed_delete_targets_for_decision(
    dobj: &DeletedObjectReplicationInfo,
    dsc: &ReplicateDecision,
    error: &str,
) -> Vec<ReplicatedTargetInfo> {
    dsc.targets_map
        .values()
        .filter(|target| target.replicate && (dobj.target_arn.is_empty() || dobj.target_arn == target.arn))
        .map(|target| {
            let mut rinfo = delete_target_state(dobj, &target.arn);
            if is_version_delete_replication(&dobj.delete_object) {
                rinfo.version_purge_status = VersionPurgeStatusType::Failed;
            } else {
                rinfo.replication_status = ReplicationStatusType::Failed;
            }
            rinfo.error = Some(error.to_string());
            rinfo
        })
        .collect()
}

fn mark_delete_targets_failed(rinfos: &mut ReplicatedInfos, is_version_purge: bool, error: &str) {
    for target in &mut rinfos.targets {
        if is_version_purge {
            target.version_purge_status = VersionPurgeStatusType::Failed;
        } else {
            target.replication_status = ReplicationStatusType::Failed;
        }
        target.error.get_or_insert_with(|| error.to_string());
    }
}

fn delete_marker_target_versions(dobj: &DeletedObjectReplicationInfo, rinfos: &ReplicatedInfos) -> HashMap<String, String> {
    let mut versions = dobj
        .delete_object
        .replication_state
        .as_ref()
        .map(|state| state.target_delete_marker_version_ids.clone())
        .unwrap_or_default();
    if !dobj.target_arn.is_empty()
        && let Some(version_id) = dobj.target_delete_marker_version_id.as_ref()
    {
        versions.entry(dobj.target_arn.clone()).or_insert_with(|| version_id.clone());
    }
    for target in &rinfos.targets {
        if let Some(version_id) = target.target_delete_marker_version_id.as_ref() {
            versions.insert(target.arn.clone(), version_id.clone());
        }
    }
    versions
}

fn merge_delete_target_results(rinfos: &mut ReplicatedInfos, results: Vec<ReplicatedTargetInfo>) {
    let mut indexes = rinfos
        .targets
        .iter()
        .enumerate()
        .map(|(index, target)| (target.arn.clone(), index))
        .collect::<HashMap<_, _>>();
    for result in results {
        if let Some(index) = indexes.get(&result.arn).copied() {
            rinfos.targets[index] = result;
        } else {
            indexes.insert(result.arn.clone(), rinfos.targets.len());
            rinfos.targets.push(result);
        }
    }
}

async fn persist_failed_delete_replications(
    dobj: &DeletedObjectReplicationInfo,
    rinfos: &ReplicatedInfos,
) -> ReplicationQueueAdmission {
    let mut retry_base = dobj.clone();
    retry_base.blocked_delete_marker_version_state = target_delete_marker_version_conflicts(dobj);
    retry_base.delete_object.replication_state = None;
    let mut failed = Vec::new();
    for target in rinfos.targets.iter().filter(|target| {
        target.replication_status == ReplicationStatusType::Failed
            || target.version_purge_status == VersionPurgeStatusType::Failed
    }) {
        if target.arn.is_empty()
            || target.arn.len() > 1_024
            || target
                .target_delete_marker_version_id
                .as_ref()
                .is_some_and(|version_id| version_id.is_empty() || version_id.len() > 1_024)
        {
            return ReplicationQueueAdmission::Missed;
        }
        let mut retry = retry_base.clone();
        retry.target_arn = target.arn.clone();
        retry.target_delete_marker_version_id = target.target_delete_marker_version_id.clone();
        failed.push(retry);
    }
    if failed.is_empty() {
        return ReplicationQueueAdmission::Skipped;
    }

    #[cfg(test)]
    if capture_delete_retries(dobj, &failed) {
        return ReplicationQueueAdmission::Queued;
    }

    let Some(pool) = get_global_replication_pool() else {
        error!(
            event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            bucket = %dobj.bucket,
            object = %dobj.delete_object.object_name,
            operation = "persist_failed_replication_delete",
            "Replication pool is unavailable for durable delete retry"
        );
        return ReplicationQueueAdmission::Missed;
    };

    let admission = pool.queue_mrf_delete_tasks(failed).await;
    if admission != ReplicationQueueAdmission::Queued {
        error!(
            event = EVENT_RESYNC_TARGET_OPERATION_FAILED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
            bucket = %dobj.bucket,
            object = %dobj.delete_object.object_name,
            operation = "persist_failed_replication_delete",
            "Failed to durably queue replication delete retries"
        );
    }
    admission
}

async fn replicate_delete_marker_purge_to_targets(
    bucket: String,
    dobj: DeletedObjectReplicationInfo,
    dsc: ReplicateDecision,
    target_versions: HashMap<String, String>,
    allow_legacy_source_version_fallback: bool,
) -> Vec<ReplicatedTargetInfo> {
    let Some(delete_marker_version_id) = dobj.delete_object.delete_marker_version_id else {
        return Vec::new();
    };

    // Materialize the target set before any await so the returned future is Send.
    #[allow(clippy::needless_collect)]
    let target_arns = dsc
        .targets_map
        .values()
        .filter(|target| target.replicate && (dobj.target_arn.is_empty() || dobj.target_arn == target.arn))
        .map(|target| target.arn.clone())
        .collect::<Vec<_>>();
    let target_versions = Arc::new(target_versions);

    futures::stream::iter(target_arns.into_iter().map(|target_arn| {
        let bucket = bucket.clone();
        let dobj = dobj.clone();
        let target_versions = Arc::clone(&target_versions);
        async move {
            let mut rinfo = delete_target_state(&dobj, &target_arn);
            let mapped_target_version = target_versions.get(&target_arn).cloned();
            let Some(tgt_client) = ReplicationTargetStore::remote_target_client(&bucket, &target_arn).await else {
                rinfo.replication_status = ReplicationStatusType::Failed;
                rinfo.error = Some("replication target client unavailable".to_string());
                return rinfo;
            };
            rinfo.endpoint = tgt_client.endpoint.clone();
            rinfo.secure = tgt_client.secure;
            let target_version_id = mapped_target_version.clone().or_else(|| {
                if allow_legacy_source_version_fallback {
                    target_delete_version_id(delete_marker_version_id, true)
                } else {
                    None
                }
            });
            let Some(target_version_id) = target_version_id else {
                rinfo.replication_status = ReplicationStatusType::Failed;
                rinfo.error = Some("target delete-marker version id unavailable".to_string());
                return rinfo;
            };
            if target_version_id.is_empty() || target_version_id.len() > 1_024 {
                rinfo.replication_status = ReplicationStatusType::Failed;
                rinfo.error = Some("target delete-marker version id is invalid".to_string());
                return rinfo;
            }
            rinfo.target_delete_marker_version_id = mapped_target_version.clone();

            match purge_target_delete_marker_version(
                &bucket,
                &dobj.delete_object.object_name,
                &tgt_client,
                &target_version_id,
                dobj.delete_object.delete_marker_mtime,
            )
            .await
            {
                Ok(_) => rinfo.replication_status = ReplicationStatusType::Completed,
                Err(err) => {
                    rinfo.replication_status = ReplicationStatusType::Failed;
                    rinfo.error = Some(err);
                }
            }
            rinfo
        }
    }))
    .buffer_unordered(MAX_PARALLEL_DELETE_MARKER_RECONCILIATIONS)
    .collect::<Vec<_>>()
    .await
}

async fn purge_target_delete_marker_version(
    source_bucket: &str,
    object: &str,
    tgt_client: &Arc<TargetClient>,
    target_version_id: &str,
    replication_mtime: Option<OffsetDateTime>,
) -> std::result::Result<(), String> {
    if target_version_id.is_empty() || target_version_id.len() > 1_024 {
        return Err("target delete-marker version id is invalid".to_string());
    }

    match head_object_with_proxy_stats(source_bucket, tgt_client, &tgt_client.bucket, object, Some(target_version_id.to_string()))
        .await
    {
        Err(SdkError::ServiceError(service_err))
            if (matches!(service_err.err().code(), Some("MethodNotAllowed" | "405"))
                || service_err.raw().status().as_u16() == 405)
                && service_err
                    .raw()
                    .headers()
                    .get("x-amz-delete-marker")
                    .is_some_and(|value| value.eq_ignore_ascii_case("true")) => {}
        Err(SdkError::ServiceError(service_err))
            if matches!(service_err.err().code(), Some("MethodNotAllowed" | "405"))
                || service_err.raw().status().as_u16() == 405 =>
        {
            return Err("target delete-marker validation response omitted x-amz-delete-marker: true".to_string());
        }
        Err(SdkError::ServiceError(service_err))
            if service_err.err().is_not_found() || service_err.raw().status().as_u16() == 404 =>
        {
            return Ok(());
        }
        Err(err) => {
            mark_replication_target_offline_if_needed(tgt_client, &err).await;
            return Err(format!("target delete-marker validation failed: {err}"));
        }
        Ok(_) => return Err("target version is not a delete marker".to_string()),
    }

    match tgt_client
        .remove_object(
            &tgt_client.bucket,
            object,
            Some(target_version_id.to_string()),
            replication_delete_marker_purge_remove_options(replication_mtime),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            mark_replication_target_offline_if_needed(tgt_client, &err).await;
            Err(err.to_string())
        }
    }
}

async fn replicate_force_delete_to_targets<S: ReplicationStorage>(dobj: &DeletedObjectReplicationInfo, storage: Arc<S>) {
    let bucket = &dobj.bucket;
    let object_name = &dobj.delete_object.object_name;

    let rcfg = match get_replication_config(bucket).await {
        Ok(Some(config)) => config,
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
            return;
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
            return;
        }
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
            return;
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
            return;
        }
    };

    let tgt_arns = {
        let admitted = dobj.admitted_target_arns();
        if admitted.is_empty() {
            rcfg.filter_target_arns(&ObjectOpts {
                name: object_name.clone(),
                ..Default::default()
            })
        } else {
            admitted
        }
    };

    let mut join_set = JoinSet::new();

    for arn in tgt_arns {
        let Some(tgt_client) = ReplicationTargetStore::remote_target_client(bucket, &arn).await else {
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
                return;
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
            }
        });
    }

    while let Some(result) = join_set.join_next().await {
        if let Err(e) = result {
            error!(
                event = EVENT_RESYNC_TASK_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                object = %object_name,
                operation = "force_delete",
                error = %e,
                "Replication resync task failed"
            );
        }
    }
}

fn target_delete_version_id(version_id: Uuid, version_purge: bool) -> Option<String> {
    if version_id.is_nil() {
        version_purge.then(|| NULL_VERSION_ID.to_string())
    } else {
        Some(version_id.to_string())
    }
}

fn is_target_delete_marker_creation(delete_object: &ReplicationDeletedObject) -> bool {
    delete_object.delete_marker && !is_version_delete_replication(delete_object)
}

async fn replicate_delete_to_target(dobj: &DeletedObjectReplicationInfo, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo {
    let source_version_id = if let Some(version_id) = &dobj.delete_object.delete_marker_version_id {
        version_id.to_owned()
    } else {
        dobj.delete_object.version_id.unwrap_or_default()
    };

    let mut rinfo = delete_target_state(dobj, &tgt_client.arn);
    rinfo.endpoint = tgt_client.endpoint.clone();
    rinfo.secure = tgt_client.secure;

    let is_version_purge = is_version_delete_replication(&dobj.delete_object);
    let is_marker_creation = is_target_delete_marker_creation(&dobj.delete_object);
    if target_delete_marker_version_conflicts(dobj) {
        if is_version_purge {
            rinfo.version_purge_status = VersionPurgeStatusType::Failed;
        } else {
            rinfo.replication_status = ReplicationStatusType::Failed;
        }
        rinfo.error = Some("target delete-marker version metadata conflicts with the retry".to_string());
        return rinfo;
    }
    if !is_version_purge
        && rinfo.prev_replication_status == ReplicationStatusType::Completed
        && dobj.op_type != ReplicationType::ExistingObject
        && dobj.target_delete_marker_version_id.is_none()
    {
        rinfo.replication_status = rinfo.prev_replication_status.clone();
        return rinfo;
    }

    if is_version_purge
        && rinfo.version_purge_status == VersionPurgeStatusType::Complete
        && dobj.target_delete_marker_version_id.is_none()
    {
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

    if is_version_purge && dobj.delete_object.delete_marker_version_id.is_some() {
        let mapped_target_version = rinfo.target_delete_marker_version_id.clone();
        let target_version_id = mapped_target_version
            .clone()
            .or_else(|| target_delete_version_id(source_version_id, true));
        let Some(target_version_id) = target_version_id else {
            rinfo.version_purge_status = VersionPurgeStatusType::Failed;
            rinfo.error = Some("target delete-marker version id unavailable".to_string());
            return rinfo;
        };
        rinfo.target_delete_marker_version_id = mapped_target_version.clone();
        match purge_target_delete_marker_version(
            &dobj.bucket,
            &dobj.delete_object.object_name,
            &tgt_client,
            &target_version_id,
            dobj.delete_object.delete_marker_mtime,
        )
        .await
        {
            Ok(()) => rinfo.version_purge_status = VersionPurgeStatusType::Complete,
            Err(err) => {
                rinfo.version_purge_status = VersionPurgeStatusType::Failed;
                rinfo.error = Some(err);
            }
        }
        return rinfo;
    }

    if is_marker_creation && let Some(target_version_id) = rinfo.target_delete_marker_version_id.clone() {
        match head_object_with_proxy_stats(
            &dobj.bucket,
            tgt_client.as_ref(),
            &tgt_client.bucket,
            &dobj.delete_object.object_name,
            Some(target_version_id),
        )
        .await
        {
            Err(SdkError::ServiceError(service_err))
                if (matches!(service_err.err().code(), Some("MethodNotAllowed" | "405"))
                    || service_err.raw().status().as_u16() == 405)
                    && service_err
                        .raw()
                        .headers()
                        .get("x-amz-delete-marker")
                        .is_some_and(|value| value.eq_ignore_ascii_case("true")) =>
            {
                rinfo.replication_status = ReplicationStatusType::Completed;
                return rinfo;
            }
            Err(SdkError::ServiceError(service_err))
                if service_err.err().is_not_found() || service_err.raw().status().as_u16() == 404 =>
            {
                rinfo.target_delete_marker_version_id = None;
            }
            Err(SdkError::ServiceError(service_err))
                if matches!(service_err.err().code(), Some("MethodNotAllowed" | "405"))
                    || service_err.raw().status().as_u16() == 405 =>
            {
                rinfo.replication_status = ReplicationStatusType::Failed;
                rinfo.error = Some("target delete-marker validation response omitted x-amz-delete-marker: true".to_string());
                return rinfo;
            }
            Err(err) => {
                mark_replication_target_offline_if_needed(&tgt_client, &err).await;
                rinfo.replication_status = ReplicationStatusType::Failed;
                rinfo.error = Some(format!("target delete-marker validation failed: {err}"));
                return rinfo;
            }
            Ok(_) => {
                rinfo.replication_status = ReplicationStatusType::Failed;
                rinfo.error = Some("target version is not a delete marker".to_string());
                return rinfo;
            }
        }
    }

    let version_id = target_delete_version_id(source_version_id, is_version_purge);

    if is_marker_creation {
        match head_object_with_proxy_stats(
            &dobj.bucket,
            tgt_client.as_ref(),
            &tgt_client.bucket,
            &dobj.delete_object.object_name,
            version_id.clone(),
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                let non_retryable = match &e {
                    SdkError::ServiceError(service_err) => {
                        let status = service_err.raw().status().as_u16();
                        status != 404
                            && status != 405
                            && is_retryable_delete_replication_head_error(
                                service_err.err().is_not_found(),
                                service_err.err().code(),
                            )
                    }
                    _ => true,
                };
                if non_retryable {
                    rinfo.replication_status = ReplicationStatusType::Failed;
                    rinfo.error = Some(e.to_string());
                    return rinfo;
                }
            }
        }
    }

    match tgt_client
        .remove_object_with_output(
            &tgt_client.bucket,
            &dobj.delete_object.object_name,
            version_id.clone(),
            replication_delete_remove_options(is_marker_creation, dobj.delete_object.delete_marker_mtime),
        )
        .await
    {
        Ok(output) => {
            debug!(
                bucket = tgt_client.bucket,
                object = dobj.delete_object.object_name,
                version_id = ?version_id,
                delete_marker = dobj.delete_object.delete_marker,
                is_version_purge,
                "replicate_delete_to_target succeeded"
            );
            if !is_version_purge {
                if is_marker_creation {
                    let Some(target_version_id) = output
                        .version_id
                        .filter(|version_id| !version_id.is_empty() && version_id.len() <= 1_024)
                    else {
                        rinfo.replication_status = ReplicationStatusType::Failed;
                        rinfo.error = Some("target delete-marker response omitted a valid version id".to_string());
                        return rinfo;
                    };
                    rinfo.target_delete_marker_version_id = Some(target_version_id);
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

pub async fn replicate_object<S: ReplicationStorage>(roi: ReplicateObjectInfo, storage: Arc<S>) -> (ReplicationState, bool) {
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
            return (roi.replication_state.unwrap_or_default(), false);
        }
    };

    let mut join_set = JoinSet::new();
    let mut target_missing = false;

    for arn in tgt_arns {
        let Some(tgt_client) = ReplicationTargetStore::remote_target_client(&bucket, &arn).await else {
            target_missing = true;
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
    let mut target_task_failed = false;

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(tgt_info) => {
                rinfos.targets.push(tgt_info);
            }
            Err(e) => {
                target_task_failed = true;
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
    let mut metadata_persisted = true;

    if roi.replication_status_internal != new_replication_internal || rinfos.replication_resynced() {
        let mut eval_metadata = HashMap::new();
        if let Some(ref s) = new_replication_internal {
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_STATUS, s.clone());
        }
        let mut popts = ObjectOptions {
            version_id: roi.version_id.map(|v| v.to_string()),
            eval_metadata: Some(eval_metadata),
            ..Default::default()
        };
        if let Some(signal) = obj_lock_guard.lock_lost_signal() {
            popts.add_namespace_lock_lost_signal(signal);
        }

        match storage.put_object_metadata(&bucket, &object, &popts).await {
            Ok(u) => object_info = u,
            Err(e) => {
                metadata_persisted = false;
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

    if obj_lock_guard.is_lock_lost() {
        metadata_persisted = false;
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

    let acknowledged = replication_status == ReplicationStatusType::Completed
        && !target_missing
        && !target_task_failed
        && metadata_persisted
        && !rinfos.targets.is_empty();
    (merged_state, acknowledged)
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

        if self.target_replication_status(&tgt_client.arn) == ReplicationStatusType::Completed
            && !self.existing_obj_resync.is_empty()
            && self.existing_obj_resync.must_resync_target(&tgt_client.arn)
        {
            rinfo.replication_status = ReplicationStatusType::Completed;
            rinfo.replication_resynced = true;

            return rinfo;
        }

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

        let versioned = ReplicationVersioningStore::prefix_enabled(&bucket, &object).await;
        let version_suspended = ReplicationVersioningStore::prefix_suspended(&bucket, &object).await;

        let obj_opts = ObjectOptions {
            version_id: self.version_id.map(|v| v.to_string()),
            version_suspended,
            versioned,
            replication_request: true,
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
        match head_object_with_proxy_stats(
            &bucket,
            tgt_client.as_ref(),
            &tgt_client.bucket,
            &object,
            self.version_id.map(|v| v.to_string()),
        )
        .await
        {
            Ok(oi) => {
                replication_action = replication_action_for_target_head(&object_info, &oi, self.op_type);
                if replication_action == ReplicationAction::None {
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
                    match head_object_fallback(&bucket, &tgt_client, &object).await {
                        Ok(Some(oi)) if replication_etags_match(object_info.etag.as_deref(), oi.e_tag.as_deref()) => {
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

        let has_tagging_replication = !put_opts.user_tags.is_empty();
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
            record_proxy_request(&bucket, "PutObject", result.is_err()).await;
            if has_tagging_replication {
                record_proxy_request(&bucket, "PutObjectTagging", result.is_err()).await;
            }
            result.err()
        } else {
            gr.stream = wrap_with_bandwidth_monitor(gr.stream, &put_opts, &bucket, &rinfo.arn);
            let byte_stream = async_read_to_bytestream(gr.stream);
            let result = tgt_client
                .put_object(&tgt_client.bucket, &object, size, byte_stream, &put_opts)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()));
            record_proxy_request(&bucket, "PutObject", result.is_err()).await;
            if has_tagging_replication {
                record_proxy_request(&bucket, "PutObjectTagging", result.is_err()).await;
            }
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

        let mut replication_action = ReplicationAction::Metadata;
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
                target = %tgt_client.to_url(),
                reason = "target_offline",
                "Skipped replication because target is offline"
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

        let versioned = ReplicationVersioningStore::prefix_enabled(&bucket, &object).await;
        let version_suspended = ReplicationVersioningStore::prefix_suspended(&bucket, &object).await;

        let obj_opts = ObjectOptions {
            version_id: self.version_id.map(|v| v.to_string()),
            version_suspended,
            versioned,
            replication_request: true,
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
                        "Skipped replication because object reader is unavailable"
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

        if rinfo.prev_replication_status == ReplicationStatusType::Completed
            && !self.existing_obj_resync.is_empty()
            && self.existing_obj_resync.must_resync_target(&tgt_client.arn)
        {
            rinfo.replication_status = ReplicationStatusType::Completed;
            rinfo.replication_resynced = true;
            return rinfo;
        }

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
                    "Skipped replication because actual object size is unavailable"
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

        // TODO: SSE

        if tgt_client.bucket.is_empty() {
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
                bucket_name: bucket.clone(),
                object: object_info,
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

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

        match head_object_with_proxy_stats(
            &bucket,
            tgt_client.as_ref(),
            &tgt_client.bucket,
            &object,
            self.version_id.map(|v| v.to_string()),
        )
        .await
        {
            Ok(oi) => {
                replication_action = replication_action_for_target_head(&object_info, &oi, self.op_type);
                rinfo.replication_status = ReplicationStatusType::Completed;
                if replication_action == ReplicationAction::None {
                    if self.op_type == ReplicationType::ExistingObject
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
                            bucket_name: bucket.clone(),
                            object: object_info.clone(),
                            user_agent: "Internal: [Replication]".to_string(),
                            ..Default::default()
                        });
                    }

                    if object_info.target_replication_status(&tgt_client.arn) == ReplicationStatusType::Pending
                        || object_info.target_replication_status(&tgt_client.arn) == ReplicationStatusType::Failed
                        || self.op_type == ReplicationType::ExistingObject
                    {
                        rinfo.replication_action = replication_action;
                        rinfo.replication_status = ReplicationStatusType::Completed;
                    }

                    if rinfo.replication_status == ReplicationStatusType::Completed
                        && self.op_type == ReplicationType::ExistingObject
                        && !tgt_client.reset_id.is_empty()
                    {
                        rinfo.resync_timestamp = format!(
                            "{};{}",
                            OffsetDateTime::now_utc()
                                .format(&Rfc3339)
                                .unwrap_or_else(|_| "invalid-time".to_string()),
                            tgt_client.reset_id
                        );
                        rinfo.replication_resynced = true;
                    }

                    rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();

                    return rinfo;
                }
            }
            Err(e) => {
                if is_version_id_format_mismatch(&e) {
                    // Version-ID format mismatch: retry without versionId and compare ETags.
                    match head_object_fallback(&bucket, &tgt_client, &object).await {
                        Ok(Some(oi)) => {
                            replication_action = if replication_etags_match(object_info.etag.as_deref(), oi.e_tag.as_deref()) {
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
                                bucket_name: bucket.clone(),
                                object: object_info,
                                user_agent: "Internal: [Replication]".to_string(),
                                ..Default::default()
                            });
                            rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                            return rinfo;
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
                        bucket_name: bucket.clone(),
                        object: object_info,
                        user_agent: "Internal: [Replication]".to_string(),
                        ..Default::default()
                    });

                    rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                    return rinfo;
                }
            }
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
                rinfo.resync_timestamp = format!(
                    "{};{}",
                    OffsetDateTime::now_utc()
                        .format(&Rfc3339)
                        .unwrap_or_else(|_| "invalid-time".to_string()),
                    tgt_client.reset_id
                );
                rinfo.replication_resynced = true;
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

                rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                return rinfo;
            }
        };

        let has_tagging_replication = !put_opts.user_tags.is_empty();
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
            record_proxy_request(&bucket, "PutObject", result.is_err()).await;
            if has_tagging_replication {
                record_proxy_request(&bucket, "PutObjectTagging", result.is_err()).await;
            }
            result.err()
        } else {
            gr.stream = wrap_with_bandwidth_monitor(gr.stream, &put_opts, &bucket, &rinfo.arn);
            let byte_stream = async_read_to_bytestream(gr.stream);
            let result = tgt_client
                .put_object(&tgt_client.bucket, &object, size, byte_stream, &put_opts)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()));
            record_proxy_request(&bucket, "PutObject", result.is_err()).await;
            if has_tagging_replication {
                record_proxy_request(&bucket, "PutObjectTagging", result.is_err()).await;
            }
            result.err()
        } {
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

            mark_replication_target_offline_if_needed(&tgt_client, &err).await;
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
        let part_plan = replication_multipart_part_plan(ReplicationMultipartPartInput {
            offset,
            part_number: part_info.number,
            part_size: part_info.actual_size,
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

    cli.complete_multipart_upload(
        dst_bucket,
        object,
        &upload_id,
        uploaded_parts,
        &replication_complete_multipart_options(actual_size),
    )
    .await
    .map_err(|e| std::io::Error::other(e.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::replication_filemeta_boundary::{ReplicateTargetDecision, ReplicationState, ReplicationWorkerOperation};
    use super::super::replication_storage_boundary::StorageNamespaceLocking as _;
    use super::super::replication_target_boundary::{ArnTarget, BucketTarget, BucketTargets, TargetRegistry};
    use super::*;
    use crate::layout::endpoint::Endpoint;
    use crate::layout::format::FormatV3;
    use crate::set_disk::SetDisks;
    use s3s::dto::{
        BucketVersioningStatus, DeleteReplication, DeleteReplicationStatus, Destination, ExcludedPrefix, ReplicationRule,
        ReplicationRuleStatus, VersioningConfiguration,
    };
    use std::collections::HashMap;
    use time::OffsetDateTime;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use uuid::Uuid;

    fn test_target_client(endpoint: String) -> Arc<TargetClient> {
        let config = aws_sdk_s3::Config::builder()
            .endpoint_url(endpoint.clone())
            .force_path_style(true)
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

    struct TestHttpResponse {
        status: &'static str,
        headers: Vec<(&'static str, String)>,
        body: String,
    }

    async fn test_s3_server(responses: Vec<TestHttpResponse>) -> (String, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("test S3 listener should bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("test listener address should exist"));
        let handle = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(responses.len());
            for response in responses {
                let (mut stream, _) = listener.accept().await.expect("test S3 client should connect");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 2048];
                loop {
                    let read = stream.read(&mut buffer).await.expect("test S3 request should be readable");
                    assert_ne!(read, 0, "test S3 request closed before headers completed");
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                requests.push(String::from_utf8(request).expect("test S3 request should be UTF-8"));

                let mut headers = String::new();
                for (name, value) in response.headers {
                    headers.push_str(name);
                    headers.push_str(": ");
                    headers.push_str(&value);
                    headers.push_str("\r\n");
                }
                let reply = format!(
                    "HTTP/1.1 {}\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response.status,
                    headers,
                    response.body.len(),
                    response.body
                );
                stream
                    .write_all(reply.as_bytes())
                    .await
                    .expect("test S3 response should be writable");
            }
            requests
        });
        (endpoint, handle)
    }

    async fn blocked_marker_creation_server(
        target_version_id: String,
    ) -> (String, JoinHandle<Vec<String>>, Arc<tokio::sync::Notify>, Arc<tokio::sync::Semaphore>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("test S3 listener should bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("test listener address should exist"));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let entered_for_server = Arc::clone(&entered);
        let release_for_server = Arc::clone(&release);
        let handle = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (index, response) in [
                TestHttpResponse {
                    status: "404 Not Found",
                    headers: Vec::new(),
                    body: String::new(),
                },
                TestHttpResponse {
                    status: "204 No Content",
                    headers: vec![("x-amz-version-id", target_version_id)],
                    body: String::new(),
                },
            ]
            .into_iter()
            .enumerate()
            {
                let (mut stream, _) = listener.accept().await.expect("test S3 client should connect");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 2048];
                loop {
                    let read = stream.read(&mut buffer).await.expect("test S3 request should be readable");
                    assert_ne!(read, 0, "test S3 request closed before headers completed");
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                requests.push(String::from_utf8(request).expect("test S3 request should be UTF-8"));
                if index == 1 {
                    entered_for_server.notify_one();
                    release_for_server
                        .acquire()
                        .await
                        .expect("RPC pause should remain open")
                        .forget();
                }
                let mut headers = String::new();
                for (name, value) in response.headers {
                    headers.push_str(name);
                    headers.push_str(": ");
                    headers.push_str(&value);
                    headers.push_str("\r\n");
                }
                let reply = format!(
                    "HTTP/1.1 {}\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response.status,
                    headers,
                    response.body.len(),
                    response.body
                );
                stream
                    .write_all(reply.as_bytes())
                    .await
                    .expect("test S3 response should be writable");
            }
            requests
        });
        (endpoint, handle, entered, release)
    }

    fn marker_replication_task(
        bucket: &str,
        object: &str,
        source_version_id: Uuid,
        target_arn: Option<&str>,
        target_version_id: Option<&str>,
    ) -> DeletedObjectReplicationInfo {
        let mut decision = ReplicateDecision::new();
        if let Some(target_arn) = target_arn {
            decision.set(ReplicateTargetDecision::new(target_arn.to_string(), true, false));
        }
        let targets = target_arn
            .map(|target_arn| HashMap::from([(target_arn.to_string(), ReplicationStatusType::Pending)]))
            .unwrap_or_default();
        let target_delete_marker_version_ids = target_arn
            .zip(target_version_id)
            .map(|(target_arn, target_version_id)| HashMap::from([(target_arn.to_string(), target_version_id.to_string())]))
            .unwrap_or_default();

        DeletedObjectReplicationInfo {
            bucket: bucket.to_string(),
            delete_object: ReplicationDeletedObject {
                object_name: object.to_string(),
                delete_marker: true,
                delete_marker_version_id: Some(source_version_id),
                replication_state: Some(ReplicationState {
                    replicate_decision_str: decision.to_string(),
                    replication_status_internal: target_arn.map(|target_arn| format!("{target_arn}=PENDING;")),
                    targets,
                    target_delete_marker_version_ids,
                    ..Default::default()
                }),
                ..Default::default()
            },
            target_arn: target_arn.unwrap_or_default().to_string(),
            target_delete_marker_version_id: target_version_id.map(str::to_string),
            op_type: ReplicationType::Delete,
            ..Default::default()
        }
    }

    #[test]
    fn refreshed_delete_state_preserves_completed_targets() {
        let target_a = "arn:target-a";
        let target_b = "arn:target-b";
        let mut delete = marker_replication_task("source", "object", Uuid::new_v4(), Some(target_b), None);
        let current_state = ReplicationState {
            targets: HashMap::from([
                (target_a.to_string(), ReplicationStatusType::Completed),
                (target_b.to_string(), ReplicationStatusType::Failed),
            ]),
            ..Default::default()
        };

        refresh_delete_replication_state(&mut delete, Some(current_state));
        assert_eq!(
            delete_target_state(&delete, target_a).prev_replication_status,
            ReplicationStatusType::Completed
        );
        assert_eq!(
            delete_target_state(&delete, target_b).prev_replication_status,
            ReplicationStatusType::Failed
        );

        let merged = get_replication_state(
            &ReplicatedInfos {
                replication_timestamp: None,
                targets: vec![ReplicatedTargetInfo {
                    arn: target_b.to_string(),
                    replication_status: ReplicationStatusType::Completed,
                    ..Default::default()
                }],
            },
            delete
                .delete_object
                .replication_state
                .as_ref()
                .expect("refreshed state should exist"),
            None,
        );
        assert_eq!(merged.targets.get(target_a), Some(&ReplicationStatusType::Completed));
        assert_eq!(merged.targets.get(target_b), Some(&ReplicationStatusType::Completed));
    }

    #[test]
    fn source_delete_marker_match_preserves_historical_versions() {
        let version_id = Uuid::new_v4();
        let marker = ObjectInfo {
            delete_marker: true,
            version_id: Some(version_id),
            is_latest: true,
            ..Default::default()
        };
        assert!(source_delete_marker_matches_id(&marker, version_id));

        let historical = ObjectInfo {
            is_latest: false,
            ..marker.clone()
        };
        assert!(source_delete_marker_matches_id(&historical, version_id));
        assert!(!source_delete_marker_matches_id(&marker, Uuid::new_v4()));
    }

    #[tokio::test]
    async fn panicked_delete_target_worker_is_recorded_as_failed() {
        let target_arn = "arn:target-a";
        let task = marker_replication_task("source", "object", Uuid::new_v4(), Some(target_arn), None);
        let mut join_set = JoinSet::new();
        let handle = join_set.spawn(async {
            panic!("injected target worker panic");
            #[allow(unreachable_code)]
            ReplicatedTargetInfo::default()
        });
        let mut target_tasks = HashMap::from([(handle.id(), target_arn.to_string())]);

        let result = delete_target_join_result(
            join_set.join_next_with_id().await.expect("panic result should be available"),
            &mut target_tasks,
            &task,
        )
        .expect_err("a panicked worker must fail the target");

        assert_eq!(result.1.arn, target_arn);
        assert_eq!(result.1.replication_status, ReplicationStatusType::Failed);
        assert!(result.1.error.as_deref().is_some_and(|error| error.contains("panic")));
        assert!(target_tasks.is_empty());
    }

    #[tokio::test]
    async fn cancelled_delete_target_worker_is_recorded_as_failed() {
        let target_arn = "arn:target-a";
        let task = marker_replication_task("source", "object", Uuid::new_v4(), Some(target_arn), None);
        let mut join_set = JoinSet::new();
        let handle = join_set.spawn(std::future::pending::<ReplicatedTargetInfo>());
        let mut target_tasks = HashMap::from([(handle.id(), target_arn.to_string())]);
        handle.abort();

        let result = delete_target_join_result(
            join_set
                .join_next_with_id()
                .await
                .expect("cancellation result should be available"),
            &mut target_tasks,
            &task,
        )
        .expect_err("a cancelled worker must fail the target");

        assert_eq!(result.1.arn, target_arn);
        assert_eq!(result.1.replication_status, ReplicationStatusType::Failed);
        assert!(result.1.error.as_deref().is_some_and(|error| error.contains("cancel")));
        assert!(target_tasks.is_empty());
    }

    #[tokio::test]
    async fn failed_delete_retries_are_admitted_as_one_validated_batch() {
        let bucket = format!("retry-batch-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let task = marker_replication_task(&bucket, &object, Uuid::new_v4(), None, None);
        let capture = DeleteRetryCapture::install(&bucket, &object).await;
        let infos = ReplicatedInfos {
            replication_timestamp: None,
            targets: vec![
                ReplicatedTargetInfo {
                    arn: "arn:target-a".to_string(),
                    replication_status: ReplicationStatusType::Failed,
                    target_delete_marker_version_id: Some("target-id-a".to_string()),
                    ..Default::default()
                },
                ReplicatedTargetInfo {
                    arn: "arn:target-b".to_string(),
                    replication_status: ReplicationStatusType::Failed,
                    target_delete_marker_version_id: Some("target-id-b".to_string()),
                    ..Default::default()
                },
            ],
        };

        assert_eq!(persist_failed_delete_replications(&task, &infos).await, ReplicationQueueAdmission::Queued);
        let batches = capture.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2);
        assert_eq!(batches[0][0].target_delete_marker_version_id.as_deref(), Some("target-id-a"));
        assert_eq!(batches[0][1].target_delete_marker_version_id.as_deref(), Some("target-id-b"));

        let mut invalid = infos;
        invalid.targets[1].arn.clear();
        assert_eq!(
            persist_failed_delete_replications(&task, &invalid).await,
            ReplicationQueueAdmission::Missed
        );
        assert_eq!(capture.batches().len(), 1, "an invalid member must reject the whole batch");
    }

    async fn lock_only_replication_storage() -> Arc<SetDisks> {
        SetDisks::new(
            "replication-race-test".to_string(),
            Arc::new(RwLock::new(vec![None])),
            1,
            0,
            0,
            0,
            vec![Endpoint::try_from("http://127.0.0.1:9000/data").expect("test endpoint should parse")],
            FormatV3::new(1, 1),
            Vec::new(),
        )
        .await
    }

    #[tokio::test]
    async fn delete_marker_source_check_runs_under_replication_lock() {
        let bucket = format!("source-check-lock-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let source_version_id = Uuid::new_v4();
        let storage = lock_only_replication_storage().await;
        let probe = DeleteReplicationSourceCheckProbe::install(&bucket, &object, vec![true, true], Some(1)).await;
        let task = tokio::spawn(replicate_delete(
            marker_replication_task(&bucket, &object, source_version_id, None, None),
            Arc::clone(&storage),
        ));

        probe.wait_until_paused().await;
        let competing = storage
            .new_ns_lock(&bucket, &format!("/[replicate]/{object}"))
            .await
            .expect("competing replication lock should be constructed");
        let err = competing
            .get_write_lock_quiet(TokioDuration::from_millis(50))
            .await
            .expect_err("source verification must retain the replication write lock");
        assert!(matches!(err, rustfs_lock::error::LockError::Timeout { .. }));

        probe.release();
        tokio::time::timeout(TokioDuration::from_secs(30), task)
            .await
            .expect("replication task should finish after the probe releases")
            .expect("replication task should not panic");
    }

    #[tokio::test]
    async fn delete_marker_postcheck_and_target_rpc_remain_under_replication_lock() {
        let target_version_id = "opaque-target-marker-version";
        let (endpoint, server) = test_s3_server(vec![
            TestHttpResponse {
                status: "404 Not Found",
                headers: Vec::new(),
                body: String::new(),
            },
            TestHttpResponse {
                status: "204 No Content",
                headers: vec![("x-amz-version-id", target_version_id.to_string())],
                body: String::new(),
            },
        ])
        .await;
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        TargetRegistry::get()
            .arn_remotes_map
            .write()
            .await
            .insert(target_arn.clone(), ArnTarget::with_client(Arc::clone(&target_client)));
        let bucket = format!("postcheck-lock-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let storage = lock_only_replication_storage().await;
        let probe = DeleteReplicationSourceCheckProbe::install(&bucket, &object, vec![true, true], Some(2)).await;
        let task = tokio::spawn(replicate_delete(
            marker_replication_task(&bucket, &object, Uuid::new_v4(), Some(&target_arn), None),
            Arc::clone(&storage),
        ));

        probe.wait_until_paused().await;
        let competing = storage
            .new_ns_lock(&bucket, &format!("/[replicate]/{object}"))
            .await
            .expect("competing replication lock should be constructed");
        assert!(matches!(
            competing.get_write_lock_quiet(TokioDuration::from_millis(50)).await,
            Err(rustfs_lock::error::LockError::Timeout { .. })
        ));

        probe.release();
        tokio::time::timeout(TokioDuration::from_secs(30), task)
            .await
            .expect("replication task should finish after the post-check releases")
            .expect("replication task should not panic");
        tokio::time::timeout(TokioDuration::from_secs(30), server)
            .await
            .expect("target RPCs should finish before the post-check pause")
            .expect("test S3 server should not panic");
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
    }

    #[tokio::test]
    async fn delete_marker_target_rpc_remains_under_replication_lock() {
        let (endpoint, server, rpc_entered, rpc_release) =
            blocked_marker_creation_server("opaque-target-version".to_string()).await;
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        TargetRegistry::get()
            .arn_remotes_map
            .write()
            .await
            .insert(target_arn.clone(), ArnTarget::with_client(Arc::clone(&target_client)));
        let bucket = format!("target-rpc-lock-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let storage = lock_only_replication_storage().await;
        let _probe = DeleteReplicationSourceCheckProbe::install(&bucket, &object, vec![true, true], None).await;
        let task = tokio::spawn(replicate_delete(
            marker_replication_task(&bucket, &object, Uuid::new_v4(), Some(&target_arn), None),
            Arc::clone(&storage),
        ));

        tokio::time::timeout(TokioDuration::from_secs(30), rpc_entered.notified())
            .await
            .expect("target RPC should reach its deterministic pause");
        let competing = storage
            .new_ns_lock(&bucket, &format!("/[replicate]/{object}"))
            .await
            .expect("competing replication lock should be constructed");
        assert!(matches!(
            competing.get_write_lock_quiet(TokioDuration::from_millis(50)).await,
            Err(rustfs_lock::error::LockError::Timeout { .. })
        ));

        rpc_release.add_permits(1);
        tokio::time::timeout(TokioDuration::from_secs(30), task)
            .await
            .expect("replication task should finish after the target RPC releases")
            .expect("replication task should not panic");
        tokio::time::timeout(TokioDuration::from_secs(30), server)
            .await
            .expect("test S3 server should finish")
            .expect("test S3 server should not panic");
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
    }

    #[tokio::test]
    async fn postcheck_source_disappearance_purges_generic_target_marker_by_returned_version_id() {
        let target_version_id = "opaque-target-marker-version";
        let (endpoint, server) = test_s3_server(vec![
            TestHttpResponse {
                status: "404 Not Found",
                headers: Vec::new(),
                body: String::new(),
            },
            TestHttpResponse {
                status: "204 No Content",
                headers: vec![
                    ("x-amz-delete-marker", "true".to_string()),
                    ("x-amz-version-id", target_version_id.to_string()),
                ],
                body: String::new(),
            },
            TestHttpResponse {
                status: "405 Method Not Allowed",
                headers: vec![("x-amz-delete-marker", "true".to_string())],
                body: String::new(),
            },
            TestHttpResponse {
                status: "204 No Content",
                headers: Vec::new(),
                body: String::new(),
            },
        ])
        .await;
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        TargetRegistry::get()
            .arn_remotes_map
            .write()
            .await
            .insert(target_arn.clone(), ArnTarget::with_client(Arc::clone(&target_client)));

        let bucket = format!("postcheck-source-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let source_version_id = Uuid::new_v4();
        let storage = lock_only_replication_storage().await;
        let _probe = DeleteReplicationSourceCheckProbe::install(&bucket, &object, vec![true, false], None).await;
        let completed = tokio::time::timeout(
            TokioDuration::from_secs(30),
            replicate_delete(
                marker_replication_task(&bucket, &object, source_version_id, Some(&target_arn), None),
                storage,
            ),
        )
        .await
        .expect("post-check reconciliation should finish");
        assert!(completed, "a source-absent exact purge must be terminally acknowledged");

        let requests = tokio::time::timeout(TokioDuration::from_secs(30), server)
            .await
            .expect("test S3 server should receive all requests")
            .expect("test S3 server should not panic");
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);

        assert_eq!(requests.len(), 4);
        let create = requests[1].lines().next().expect("marker create request line should exist");
        let compensation = requests[3].lines().next().expect("marker purge request line should exist");
        assert!(create.starts_with("DELETE "));
        assert!(!create.contains("versionId="), "generic marker creation must not address the source UUID");
        assert!(compensation.starts_with("DELETE "));
        assert!(compensation.contains(&format!("versionId={target_version_id}")));
        assert!(!compensation.contains(&source_version_id.to_string()));
    }

    #[tokio::test]
    async fn precheck_source_absence_purges_known_exact_target_marker_once() {
        let target_version_id = "opaque-known-target-marker";
        let (endpoint, server) = test_s3_server(vec![
            TestHttpResponse {
                status: "405 Method Not Allowed",
                headers: vec![("x-amz-delete-marker", "true".to_string())],
                body: String::new(),
            },
            TestHttpResponse {
                status: "204 No Content",
                headers: Vec::new(),
                body: String::new(),
            },
        ])
        .await;
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        TargetRegistry::get()
            .arn_remotes_map
            .write()
            .await
            .insert(target_arn.clone(), ArnTarget::with_client(Arc::clone(&target_client)));
        let bucket = format!("precheck-source-absent-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let storage = lock_only_replication_storage().await;
        let _probe = DeleteReplicationSourceCheckProbe::install(&bucket, &object, vec![false], None).await;

        assert!(
            replicate_delete(
                marker_replication_task(&bucket, &object, Uuid::new_v4(), Some(&target_arn), Some(target_version_id),),
                storage,
            )
            .await
        );

        let requests = server.await.expect("test S3 server should not panic");
        assert_eq!(requests.len(), 2);
        for request in requests {
            assert!(
                request
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .contains(&format!("versionId={target_version_id}"))
            );
        }
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
    }

    #[tokio::test]
    async fn postcheck_error_keeps_target_marker_and_records_failure() {
        let (endpoint, server) = test_s3_server(vec![
            TestHttpResponse {
                status: "404 Not Found",
                headers: Vec::new(),
                body: String::new(),
            },
            TestHttpResponse {
                status: "204 No Content",
                headers: vec![("x-amz-version-id", "opaque-target-marker".to_string())],
                body: String::new(),
            },
        ])
        .await;
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        TargetRegistry::get()
            .arn_remotes_map
            .write()
            .await
            .insert(target_arn.clone(), ArnTarget::with_client(Arc::clone(&target_client)));
        let bucket = format!("postcheck-error-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let storage = lock_only_replication_storage().await;
        let _probe = DeleteReplicationSourceCheckProbe::install_results(&bucket, &object, vec![Some(true), None], None).await;
        let retry_capture = DeleteRetryCapture::install(&bucket, &object).await;

        let completed = tokio::time::timeout(
            TokioDuration::from_secs(30),
            replicate_delete(
                marker_replication_task(&bucket, &object, Uuid::new_v4(), Some(&target_arn), None),
                storage,
            ),
        )
        .await
        .expect("post-check error handling should finish");
        assert!(completed, "a post-check error may be acknowledged only after durable retry admission");

        let requests = tokio::time::timeout(TokioDuration::from_secs(30), server)
            .await
            .expect("only the initial target marker creation should run")
            .expect("test S3 server should not panic");
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
        assert_eq!(requests.len(), 2, "source verification errors must not trigger destructive compensation");
        let batches = retry_capture.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[0][0].target_arn, target_arn);
        assert_eq!(batches[0][0].target_delete_marker_version_id.as_deref(), Some("opaque-target-marker"));
    }

    #[tokio::test]
    async fn precheck_error_does_not_mutate_target_or_acknowledge() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("test S3 listener should bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("test listener address should exist"));
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        TargetRegistry::get()
            .arn_remotes_map
            .write()
            .await
            .insert(target_arn.clone(), ArnTarget::with_client(Arc::clone(&target_client)));
        let bucket = format!("precheck-error-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let storage = lock_only_replication_storage().await;
        let _probe = DeleteReplicationSourceCheckProbe::install_results(&bucket, &object, vec![None], None).await;
        let retry_capture = DeleteRetryCapture::install(&bucket, &object).await;

        let completed = replicate_delete(
            marker_replication_task(&bucket, &object, Uuid::new_v4(), Some(&target_arn), None),
            storage,
        )
        .await;

        assert!(
            completed,
            "a source precheck error may be acknowledged only after durable retry admission"
        );
        assert!(
            tokio::time::timeout(TokioDuration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "a source precheck error must not issue a target request"
        );
        let batches = retry_capture.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[0][0].target_arn, target_arn);
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
    }

    #[tokio::test]
    async fn access_denied_compensation_stays_failed_and_same_target_replay_converges() {
        let target_version_id = "opaque-replay-target-version";
        let access_denied = "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>";
        let (endpoint, server) = test_s3_server(vec![
            TestHttpResponse {
                status: "405 Method Not Allowed",
                headers: vec![("x-amz-delete-marker", "true".to_string())],
                body: String::new(),
            },
            TestHttpResponse {
                status: "403 Forbidden",
                headers: vec![("Content-Type", "application/xml".to_string())],
                body: access_denied.to_string(),
            },
            TestHttpResponse {
                status: "405 Method Not Allowed",
                headers: vec![("x-amz-delete-marker", "true".to_string())],
                body: String::new(),
            },
            TestHttpResponse {
                status: "204 No Content",
                headers: Vec::new(),
                body: String::new(),
            },
        ])
        .await;
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        TargetRegistry::get()
            .arn_remotes_map
            .write()
            .await
            .insert(target_arn.clone(), ArnTarget::with_client(Arc::clone(&target_client)));

        let source_version_id = Uuid::new_v4();
        let task =
            marker_replication_task("source-bucket", "object", source_version_id, Some(&target_arn), Some(target_version_id));
        let dsc = parse_replicate_decision(
            &task.bucket,
            &task
                .delete_object
                .replication_state
                .as_ref()
                .expect("test task should have replication state")
                .replicate_decision_str,
        )
        .expect("test decision should parse");
        let versions = HashMap::from([(target_arn.clone(), target_version_id.to_string())]);

        let first =
            replicate_delete_marker_purge_to_targets(task.bucket.clone(), task.clone(), dsc.clone(), versions.clone(), false)
                .await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].arn, target_arn);
        assert_eq!(first[0].replication_status, ReplicationStatusType::Failed);
        assert!(first[0].error.as_deref().is_some_and(|error| error.contains("AccessDenied")));
        assert_eq!(first[0].target_delete_marker_version_id.as_deref(), Some(target_version_id));

        let replay = replicate_delete_marker_purge_to_targets(task.bucket.clone(), task.clone(), dsc, versions, false).await;
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].arn, target_arn);
        assert_eq!(replay[0].replication_status, ReplicationStatusType::Completed);
        assert_eq!(replay[0].target_delete_marker_version_id.as_deref(), Some(target_version_id));

        let requests = tokio::time::timeout(TokioDuration::from_secs(30), server)
            .await
            .expect("test S3 server should receive both purge attempts")
            .expect("test S3 server should not panic");
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
        assert_eq!(requests.len(), 4);
        for request in [&requests[1], &requests[3]] {
            let request_line = request.lines().next().expect("purge request line should exist");
            assert!(request_line.starts_with("DELETE "));
            assert!(request_line.contains(&format!("versionId={target_version_id}")));
            assert!(!request_line.contains(&source_version_id.to_string()));
        }
    }

    #[tokio::test]
    async fn delete_marker_purge_rejects_405_without_delete_marker_header() {
        for headers in [Vec::new(), vec![("x-amz-delete-marker", "false".to_string())]] {
            let (endpoint, server) = test_s3_server(vec![TestHttpResponse {
                status: "405 Method Not Allowed",
                headers,
                body: String::new(),
            }])
            .await;
            let target_client = test_target_client(endpoint);

            let result =
                purge_target_delete_marker_version("source-bucket", "object", &target_client, "candidate-version", None).await;

            assert!(result.is_err());
            let requests = server.await.expect("test S3 server should not panic");
            assert_eq!(requests.len(), 1, "an unverified 405 must not be followed by DELETE");
            TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
        }
    }

    #[tokio::test]
    async fn legacy_source_id_fallback_treats_missing_marker_as_complete() {
        let (endpoint, server) = test_s3_server(vec![TestHttpResponse {
            status: "404 Not Found",
            headers: Vec::new(),
            body: String::new(),
        }])
        .await;
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        TargetRegistry::get()
            .arn_remotes_map
            .write()
            .await
            .insert(target_arn.clone(), ArnTarget::with_client(Arc::clone(&target_client)));
        let task = marker_replication_task("source-bucket", "object", Uuid::new_v4(), Some(&target_arn), None);
        let dsc = parse_replicate_decision(
            &task.bucket,
            &task
                .delete_object
                .replication_state
                .as_ref()
                .expect("test task should have replication state")
                .replicate_decision_str,
        )
        .expect("test decision should parse");

        let result = replicate_delete_marker_purge_to_targets(task.bucket.clone(), task, dsc, HashMap::new(), true).await;
        assert_eq!(result[0].replication_status, ReplicationStatusType::Completed);
        assert!(result[0].target_delete_marker_version_id.is_none());

        let requests = server.await.expect("test S3 server should not panic");
        assert_eq!(requests.len(), 1);
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
    }

    #[tokio::test]
    async fn marker_creation_rejects_missing_empty_and_oversized_target_version_ids() {
        let mut responses = Vec::new();
        for version_id in [None, Some(String::new()), Some("x".repeat(1_025))] {
            responses.push(TestHttpResponse {
                status: "404 Not Found",
                headers: Vec::new(),
                body: String::new(),
            });
            responses.push(TestHttpResponse {
                status: "204 No Content",
                headers: version_id
                    .map(|version_id| vec![("x-amz-version-id", version_id)])
                    .unwrap_or_default(),
                body: String::new(),
            });
        }
        let (endpoint, server) = test_s3_server(responses).await;
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        register_test_target(&target_client).await;

        for _ in 0..3 {
            let task = marker_replication_task("source-bucket", "object", Uuid::new_v4(), Some(&target_arn), None);
            let result = replicate_delete_to_target(&task, Arc::clone(&target_client)).await;
            assert_eq!(result.replication_status, ReplicationStatusType::Failed);
            assert!(result.target_delete_marker_version_id.is_none());
            assert!(
                result
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains("valid version id"))
            );
        }

        let requests = server.await.expect("test S3 server should not panic");
        assert_eq!(requests.len(), 6);
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().target_h_mutex.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
    }

    #[tokio::test]
    async fn targeted_marker_retry_keeps_verified_existing_target_marker() {
        let target_version_id = "opaque-existing-target-marker";
        let (endpoint, server) = test_s3_server(vec![TestHttpResponse {
            status: "405 Method Not Allowed",
            headers: vec![("x-amz-delete-marker", "true".to_string())],
            body: String::new(),
        }])
        .await;
        let target_client = test_target_client(endpoint);
        let target_endpoint = target_client.endpoint.clone();
        let target_arn = target_client.arn.clone();
        register_test_target(&target_client).await;
        let task = marker_replication_task("source-bucket", "object", Uuid::new_v4(), Some(&target_arn), Some(target_version_id));

        let result = replicate_delete_to_target(&task, target_client).await;

        assert_eq!(result.replication_status, ReplicationStatusType::Completed);
        assert_eq!(result.target_delete_marker_version_id.as_deref(), Some(target_version_id));
        let requests = tokio::time::timeout(TokioDuration::from_secs(30), server)
            .await
            .expect("test S3 server should receive the verification request")
            .expect("test S3 server should not panic");
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().target_h_mutex.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_endpoint);
        assert_eq!(requests.len(), 1);
        let request_line = requests[0].lines().next().unwrap_or_default();
        assert!(request_line.starts_with("HEAD "));
        assert!(request_line.contains(&format!("versionId={target_version_id}")));
    }

    #[tokio::test]
    async fn targeted_marker_retry_recreates_only_when_mapped_marker_is_missing() {
        let old_target_version_id = "opaque-missing-target-marker";
        let new_target_version_id = "opaque-new-target-marker";
        let (endpoint, server) = test_s3_server(vec![
            TestHttpResponse {
                status: "404 Not Found",
                headers: Vec::new(),
                body: String::new(),
            },
            TestHttpResponse {
                status: "404 Not Found",
                headers: Vec::new(),
                body: String::new(),
            },
            TestHttpResponse {
                status: "204 No Content",
                headers: vec![("x-amz-version-id", new_target_version_id.to_string())],
                body: String::new(),
            },
        ])
        .await;
        let target_client = test_target_client(endpoint);
        let target_endpoint = target_client.endpoint.clone();
        let target_arn = target_client.arn.clone();
        register_test_target(&target_client).await;
        let task =
            marker_replication_task("source-bucket", "object", Uuid::new_v4(), Some(&target_arn), Some(old_target_version_id));

        let result = replicate_delete_to_target(&task, target_client).await;

        assert_eq!(result.replication_status, ReplicationStatusType::Completed);
        assert_eq!(result.target_delete_marker_version_id.as_deref(), Some(new_target_version_id));
        let requests = server.await.expect("test S3 server should not panic");
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().target_h_mutex.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_endpoint);
        assert_eq!(requests.len(), 3);
        let mapped_probe = requests[0].lines().next().unwrap_or_default();
        assert!(mapped_probe.starts_with("HEAD "));
        assert!(mapped_probe.contains(&format!("versionId={old_target_version_id}")));
        assert!(requests[1].lines().next().unwrap_or_default().starts_with("HEAD "));
        assert!(requests[2].lines().next().unwrap_or_default().starts_with("DELETE "));
        assert!(!requests[2].lines().next().unwrap_or_default().contains("versionId="));
    }

    #[tokio::test]
    async fn targeted_retry_postcheck_purges_verified_existing_marker_id() {
        let target_version_id = "opaque-existing-target-marker";
        let (endpoint, server) = test_s3_server(vec![
            TestHttpResponse {
                status: "405 Method Not Allowed",
                headers: vec![("x-amz-delete-marker", "true".to_string())],
                body: String::new(),
            },
            TestHttpResponse {
                status: "405 Method Not Allowed",
                headers: vec![("x-amz-delete-marker", "true".to_string())],
                body: String::new(),
            },
            TestHttpResponse {
                status: "204 No Content",
                headers: Vec::new(),
                body: String::new(),
            },
        ])
        .await;
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        TargetRegistry::get()
            .arn_remotes_map
            .write()
            .await
            .insert(target_arn.clone(), ArnTarget::with_client(Arc::clone(&target_client)));
        let bucket = format!("targeted-postcheck-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let task = marker_replication_task(&bucket, &object, Uuid::new_v4(), Some(&target_arn), Some(target_version_id));
        let storage = lock_only_replication_storage().await;
        let _probe = DeleteReplicationSourceCheckProbe::install(&bucket, &object, vec![true, false], None).await;

        assert!(replicate_delete(task, storage).await);

        let requests = server.await.expect("test S3 server should not panic");
        assert_eq!(requests.len(), 3);
        let compensation = requests[2].lines().next().expect("compensation request line should exist");
        assert!(compensation.contains(&format!("versionId={target_version_id}")));
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
    }

    #[tokio::test]
    async fn source_absent_conflicting_target_id_is_retained_without_remote_mutation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("test S3 listener should bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("test listener address should exist"));
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        TargetRegistry::get()
            .arn_remotes_map
            .write()
            .await
            .insert(target_arn.clone(), ArnTarget::with_client(Arc::clone(&target_client)));
        let bucket = format!("conflicting-target-id-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let mut task = marker_replication_task(&bucket, &object, Uuid::new_v4(), Some(&target_arn), Some("mrf-target-version"));
        task.delete_object
            .replication_state
            .as_mut()
            .expect("test replication state should exist")
            .target_delete_marker_version_ids
            .insert(target_arn.clone(), "xl-meta-target-version".to_string());
        let storage = lock_only_replication_storage().await;
        let _probe = DeleteReplicationSourceCheckProbe::install(&bucket, &object, vec![false], None).await;

        let completed = replicate_delete(task, storage).await;

        assert!(!completed, "a conflicting task must remain retained even when a retry was durably queued");
        assert!(
            tokio::time::timeout(TokioDuration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "conflicting target IDs must not issue a target request"
        );
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
    }

    #[tokio::test]
    async fn refreshed_target_id_rebases_stale_mrf_without_remote_mutation() {
        let (endpoint, server) = test_s3_server(vec![TestHttpResponse {
            status: "405 Method Not Allowed",
            headers: vec![("x-amz-delete-marker", "true".to_string())],
            body: String::new(),
        }])
        .await;
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        register_test_target(&target_client).await;
        let bucket = format!("refreshed-conflict-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let task = marker_replication_task(&bucket, &object, Uuid::new_v4(), Some(&target_arn), Some("mrf-target-version"));
        let refreshed = ReplicationState {
            target_delete_marker_version_ids: HashMap::from([(target_arn.clone(), "xl-meta-target-version".to_string())]),
            ..Default::default()
        };
        let storage = lock_only_replication_storage().await;
        let _probe = DeleteReplicationSourceCheckProbe::install_states(
            &bucket,
            &object,
            vec![Some((true, Some(refreshed.clone()))), Some((true, Some(refreshed)))],
            None,
        )
        .await;
        let retry_capture = DeleteRetryCapture::install(&bucket, &object).await;

        assert!(replicate_delete(task, storage).await);
        let requests = server.await.expect("test S3 server should not panic");
        assert_eq!(requests.len(), 1);
        let request_line = requests[0].lines().next().unwrap_or_default();
        assert!(request_line.starts_with("HEAD "));
        assert!(request_line.contains("versionId=xl-meta-target-version"));
        let batches = retry_capture.batches();
        assert_eq!(batches.len(), 1, "the injected local commit failure should remain retryable");
        assert_eq!(batches[0].len(), 1);
        assert!(!batches[0][0].blocked_delete_marker_version_state);
        assert_eq!(batches[0][0].target_delete_marker_version_id.as_deref(), Some("xl-meta-target-version"));
        assert!(!batches[0][0].to_mrf_entry().blocked_delete_marker_version_state());
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().target_h_mutex.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
    }

    #[tokio::test]
    async fn corrupt_target_version_metadata_remains_fail_closed_across_retries() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("test S3 listener should bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("test listener address should exist"));
        let target_client = test_target_client(endpoint);
        let target_arn = target_client.arn.clone();
        TargetRegistry::get()
            .arn_remotes_map
            .write()
            .await
            .insert(target_arn.clone(), ArnTarget::with_client(Arc::clone(&target_client)));
        let bucket = format!("corrupt-target-id-{}", Uuid::new_v4());
        let object = format!("object-{}", Uuid::new_v4());
        let mut task = marker_replication_task(&bucket, &object, Uuid::new_v4(), Some(&target_arn), None);
        task.delete_object
            .replication_state
            .as_mut()
            .expect("test replication state should exist")
            .target_delete_marker_version_ids_corrupt = true;
        let storage = lock_only_replication_storage().await;
        let _probe = DeleteReplicationSourceCheckProbe::install(&bucket, &object, vec![false, false], None).await;

        assert!(!replicate_delete(task.clone(), Arc::clone(&storage)).await);
        assert!(!replicate_delete(task, storage).await);
        assert!(
            tokio::time::timeout(TokioDuration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "corrupt target version metadata must remain non-destructive across retries"
        );
        TargetRegistry::get().arn_remotes_map.write().await.remove(&target_arn);
        TargetRegistry::get().h_mutex.write().await.remove(&target_client.endpoint);
    }

    #[test]
    fn replication_target_offline_error_classifier_is_network_scoped() {
        assert!(is_replication_target_offline_error(&"put_object dispatch failure: connector error"));
        assert!(is_replication_target_offline_error(&"request TimeoutError after retry"));
        assert!(is_replication_target_offline_error(&"tcp connect error: connection refused"));
        assert!(!is_replication_target_offline_error(&"put_object failed: AccessDenied: denied"));
        assert!(!is_replication_target_offline_error(&"put_object failed: NoSuchBucket"));
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
    fn test_target_delete_marker_creation_excludes_replayed_version_purges() {
        let marker = ReplicationDeletedObject {
            delete_marker: true,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };
        let replayed_purge = ReplicationDeletedObject {
            delete_marker: true,
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(is_target_delete_marker_creation(&marker));
        assert!(!is_target_delete_marker_creation(&replayed_purge));
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
    fn test_resync_status_duration_only_tracks_terminal_status() {
        let start = match OffsetDateTime::from_unix_timestamp(1_700_000_000) {
            Ok(start) => start,
            Err(err) => panic!("valid test timestamp: {err}"),
        };
        let end = start + time::Duration::seconds(2);

        assert_eq!(
            resync_status_duration(ResyncStatusType::ResyncCompleted, Some(start), end),
            Some(std::time::Duration::from_millis(2000))
        );
        assert_eq!(resync_status_duration(ResyncStatusType::ResyncStarted, Some(start), end), None);
        assert_eq!(resync_status_duration(ResyncStatusType::ResyncFailed, None, end), None);
    }

    #[test]
    fn target_delete_version_id_preserves_explicit_null_purges() {
        let version_id = Uuid::new_v4();

        assert_eq!(target_delete_version_id(version_id, true), Some(version_id.to_string()));
        assert_eq!(target_delete_version_id(Uuid::nil(), true).as_deref(), Some(NULL_VERSION_ID));
        assert_eq!(target_delete_version_id(Uuid::nil(), false), None);
    }
}
