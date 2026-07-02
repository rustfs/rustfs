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

use super::config::{ObjectOpts, ReplicationConfigurationExt as _};
use super::datatypes::ResyncStatusType;
use super::replication_bandwidth_boundary;
use super::replication_config_store::ReplicationConfigStore;
use super::replication_error_boundary::{Error, Result, is_err_object_not_found, is_err_version_not_found};
use super::replication_event_sink::{EventArgs, send_event, send_local_event};
use super::replication_filemeta_boundary::{
    MrfReplicateEntry, REPLICATE_EXISTING, REPLICATE_EXISTING_DELETE, ReplicateDecision, ReplicateObjectInfo, ReplicatedInfos,
    ReplicatedTargetInfo, ReplicationAction, ReplicationStatusType, ReplicationType, VersionPurgeStatusType,
    get_replication_state, parse_replicate_decision, replication_statuses_map, target_reset_header, version_purge_statuses_map,
};
use super::replication_lock_boundary::ReplicationLockTiming;
use super::replication_metadata_boundary::ReplicationMetadataStore;
#[cfg(test)]
use super::replication_msgp_boundary::ReplicationMsgpCodec;
use super::replication_object_config::{ReplicationConfig, check_replicate_delete, get_replication_config, must_replicate};
use super::replication_storage_boundary::{
    AdvancedGetOptions, DeletedObject, EcstoreObjectOperations, HTTPRangeSpec, ObjectInfo, ObjectOptions, ObjectToDelete,
    ReplicationObjectIO, ReplicationStorage, StatObjectOptions, WalkOptions,
};
use super::replication_target_boundary::{
    PutObjectOptions, PutObjectPartOptions, ReplicationTargetStore, TargetClient, replication_complete_multipart_options,
    replication_delete_marker_purge_remove_options, replication_delete_remove_options, replication_force_delete_remove_options,
    replication_put_object_header_size, replication_put_object_options,
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
#[cfg(test)]
use rustfs_replication::content_matches_by_etag;
use rustfs_replication::{
    BucketReplicationResyncStatus, DeletedObjectReplicationInfo, MustReplicateOptions, ReplicationSourceObject,
    ReplicationTargetObject, ResyncOpts, TargetReplicationResyncStatus, heal_uses_delete_replication_path,
    is_retryable_delete_replication_head_error, is_version_delete_replication, is_version_id_mismatch,
    replication_action_for_target, replication_etags_match, resync_state_accepts_update, should_count_head_proxy_failure,
    should_retry_delete_marker_purge, target_is_newer_than_source_null_version,
};
use rustfs_s3_types::EventName;
use rustfs_utils::http::{
    AMZ_TAGGING_DIRECTIVE, SUFFIX_REPLICATION_RESET, SUFFIX_REPLICATION_STATUS, has_internal_suffix, insert_str,
};
use rustfs_utils::{DEFAULT_SIP_HASH_KEY, sip_hash};
#[cfg(test)]
use s3s::dto::ReplicationConfiguration;
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncRead;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::Duration as TokioDuration;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, trace, warn};
use uuid::Uuid;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_REPLICATION_RESYNC: &str = "replication_resync";
const EVENT_RESYNC_STATUS_UPDATE_SKIPPED: &str = "replication_resync_status_update_skipped";
const EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED: &str = "replication_resync_config_lookup_skipped";
const EVENT_RESYNC_OBJECT_PROCESSED: &str = "replication_resync_object_processed";
const EVENT_RESYNC_RUNTIME_SKIPPED: &str = "replication_resync_runtime_skipped";
const EVENT_REPLICATION_DELETE_SKIPPED: &str = "replication_delete_skipped";
const EVENT_REPLICATION_FORCE_DELETE_SKIPPED: &str = "replication_force_delete_skipped";
const EVENT_RESYNC_TASK_FAILED: &str = "replication_resync_task_failed";
const EVENT_RESYNC_TARGET_OPERATION_FAILED: &str = "replication_resync_target_operation_failed";
const EVENT_RESYNC_RUNTIME_CHANNEL_FAILED: &str = "replication_resync_runtime_channel_failed";

pub(crate) const RESYNC_META_FORMAT: u16 = rustfs_replication::resync::RESYNC_META_FORMAT;
pub(crate) const RESYNC_META_VERSION: u16 = rustfs_replication::resync::RESYNC_META_VERSION;
pub(crate) const MRF_META_FORMAT: u16 = rustfs_replication::mrf::MRF_META_FORMAT;
pub(crate) const MRF_META_VERSION: u16 = rustfs_replication::mrf::MRF_META_VERSION;
const RESYNC_TIME_INTERVAL: TokioDuration = TokioDuration::from_secs(60);

static WARNED_MONITOR_UNINIT: std::sync::Once = std::sync::Once::new();

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

fn head_object_last_modified(oi: &HeadObjectOutput) -> Option<OffsetDateTime> {
    oi.last_modified
        .map(|dt| OffsetDateTime::from_unix_timestamp(dt.secs()).unwrap_or(OffsetDateTime::UNIX_EPOCH))
}

fn replication_source_object(oi: &ObjectInfo) -> ReplicationSourceObject<'_> {
    ReplicationSourceObject {
        mod_time: oi.mod_time,
        version_id: oi.version_id.map(|version_id| version_id.to_string()),
        etag: oi.etag.as_deref(),
        actual_size: oi.get_actual_size().unwrap_or_default(),
        delete_marker: oi.delete_marker,
        content_type: oi.content_type.as_deref(),
        content_encoding: oi.content_encoding.as_deref(),
        user_tags: oi.user_tags.as_str(),
        user_defined: oi.user_defined.as_ref(),
    }
}

fn replication_target_object(oi: &HeadObjectOutput) -> ReplicationTargetObject<'_> {
    ReplicationTargetObject {
        last_modified: head_object_last_modified(oi),
        version_id: oi.version_id.as_deref(),
        etag: oi.e_tag.as_deref(),
        content_length: oi.content_length.unwrap_or_default(),
        delete_marker: oi.delete_marker.unwrap_or_default(),
        content_type: oi.content_type.as_deref(),
        metadata: oi.metadata.as_ref(),
        tag_count: oi.tag_count.unwrap_or_default(),
    }
}

fn map_replication_error(err: rustfs_replication::Error) -> Error {
    match err {
        rustfs_replication::Error::CorruptedFormat => Error::CorruptedFormat,
        rustfs_replication::Error::Other(err) => Error::other(err),
    }
}

pub(crate) fn encode_resync_file(status: &BucketReplicationResyncStatus) -> Result<Vec<u8>> {
    rustfs_replication::encode_resync_file(status).map_err(map_replication_error)
}

pub(crate) fn decode_resync_file(data: &[u8]) -> Result<BucketReplicationResyncStatus> {
    rustfs_replication::decode_resync_file(data).map_err(map_replication_error)
}

pub(crate) fn encode_mrf_file(entries: &[MrfReplicateEntry]) -> Result<Vec<u8>> {
    rustfs_replication::encode_mrf_file(entries).map_err(map_replication_error)
}

pub(crate) fn decode_mrf_file(data: &[u8]) -> Result<Vec<MrfReplicateEntry>> {
    rustfs_replication::decode_mrf_file(data).map_err(map_replication_error)
}

static RESYNC_WORKER_COUNT: usize = 10;

#[derive(Debug)]
pub struct ReplicationResyncer {
    pub status_map: Arc<RwLock<HashMap<String, BucketReplicationResyncStatus>>>,
    pub worker_size: usize,
    pub cancel_tokens: Arc<RwLock<HashMap<String, CancellationToken>>>,
}

impl ReplicationResyncer {
    pub async fn new() -> Self {
        Self {
            status_map: Arc::new(RwLock::new(HashMap::new())),
            worker_size: RESYNC_WORKER_COUNT,
            cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn cancel_key(opts: &ResyncOpts) -> String {
        format!("{}:{}", opts.bucket, opts.arn)
    }

    pub async fn register_cancel_token(&self, opts: &ResyncOpts, token: CancellationToken) {
        self.cancel_tokens.write().await.insert(Self::cancel_key(opts), token);
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
        let bucket_status = {
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

            bucket_status.last_update = Some(now);

            bucket_status.clone()
        };

        save_resync_status(&opts.bucket, &bucket_status, obj_layer).await?;

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
        state.last_update = Some(now);
        bucket_status.last_update = Some(now);
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
        // TODO: Metrics
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
            .walk(cancellation_token.clone(), &opts.bucket, "", tx.clone(), WalkOptions::default())
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

        let status = {
            self.status_map
                .read()
                .await
                .get(&opts.bucket)
                .and_then(|status| status.targets_map.get(&opts.arn))
                .cloned()
                .unwrap_or_default()
        };

        let mut last_checkpoint = if status.resync_status == ResyncStatusType::ResyncStarted
            || status.resync_status == ResyncStatusType::ResyncFailed
        {
            Some(status.object)
        } else {
            None
        };

        let mut worker_txs = Vec::new();
        let (results_tx, mut results_rx) = tokio::sync::broadcast::channel::<TargetReplicationResyncStatus>(1);

        let opts_clone = opts.clone();
        let self_clone = self.clone();

        let mut futures = Vec::new();

        let results_fut = tokio::spawn(async move {
            while let Ok(st) = results_rx.recv().await {
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
                            delete_object: DeletedObject {
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
                    let (size, err) = if let Err(err) = head_result {
                        if roi.delete_marker {
                            st.replicated_count += 1;
                        } else {
                            st.failed_count += 1;
                        }
                        (0, Some(err))
                    } else {
                        st.replicated_count += 1;
                        st.replicated_size += roi.size;
                        (roi.size, None)
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

                    if cancel_token.is_cancelled() {
                        return;
                    }

                    if let Err(err) = results_tx.send(st) {
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
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }

            if cancellation_token.is_cancelled() {
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

            let roi = get_heal_replicate_object_info(&object, &rcfg).await;
            if !roi.existing_obj_resync.must_resync() {
                continue;
            }

            if cancellation_token.is_cancelled() {
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
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }
        }

        for worker_tx in worker_txs {
            drop(worker_tx);
        }

        join_all(futures).await;

        self.resync_bucket_mark_status(ResyncStatusType::ResyncCompleted, opts.clone(), storage.clone())
            .await;
    }
}

pub async fn get_heal_replicate_object_info(oi: &ObjectInfo, rcfg: &ReplicationConfig) -> ReplicateObjectInfo {
    let mut oi = oi.clone();
    let mut user_defined = (*oi.user_defined).clone();

    if let Some(rc) = rcfg.config.as_ref()
        && !rc.role.is_empty()
    {
        if !oi.version_purge_status.is_empty() {
            oi.version_purge_status_internal = Some(format!("{}={};", rc.role, oi.version_purge_status.as_str()));
        }

        if !oi.replication_status.is_empty() {
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

    let dsc = if heal_uses_delete_replication_path(oi.delete_marker, &oi.version_purge_status) {
        check_replicate_delete(
            oi.bucket.as_str(),
            &ObjectToDelete {
                object_name: oi.name.clone(),
                version_id: oi.version_id,
                ..Default::default()
            },
            &oi,
            &ObjectOptions {
                versioned: ReplicationVersioningStore::prefix_enabled(&oi.bucket, &oi.name).await,
                version_suspended: ReplicationVersioningStore::prefix_suspended(&oi.bucket, &oi.name).await,
                ..Default::default()
            },
            None,
        )
        .await
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
    let existing_obj_resync = rcfg.resync(oi.clone(), dsc.clone(), &target_statuses).await;
    let mut replication_state = oi.replication_state();
    replication_state.replicate_decision_str = dsc.to_string();
    let actual_size = oi.get_actual_size().unwrap_or_default();

    ReplicateObjectInfo {
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
        ssec: false, // TODO: add ssec support
        user_tags: (*oi.user_tags).clone(),
        checksum: oi.checksum.clone(),
        retry_count: 0,
    }
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
    if dobj.delete_object.force_delete {
        replicate_force_delete_to_targets(&dobj, storage).await;
        return;
    }

    let bucket = dobj.bucket.clone();
    let version_id = if let Some(version_id) = &dobj.delete_object.delete_marker_version_id {
        Some(version_id.to_owned())
    } else {
        dobj.delete_object.version_id
    };

    let _rcfg = match get_replication_config(&bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            debug!(
                event = EVENT_REPLICATION_DELETE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                reason = "replication_config_missing",
                "Skipping replication delete because replication config is missing"
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

            return;
        }
        Err(err) => {
            debug!(
                event = EVENT_REPLICATION_DELETE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                error = %err,
                reason = "replication_config_lookup_failed",
                "Skipping replication delete because replication config lookup failed"
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
            return;
        }
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
                return;
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
                return;
            }
            Err(err) => {
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
            return;
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
            return;
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
            return;
        }
    };

    // Initialize replicated infos
    let mut rinfos = ReplicatedInfos {
        replication_timestamp: Some(OffsetDateTime::now_utc()),
        targets: Vec::with_capacity(dsc.targets_map.len()),
    };

    let mut join_set = JoinSet::new();

    // Process each target
    for (_, tgt_entry) in dsc.targets_map.iter() {
        // Skip targets that should not be replicated
        if !tgt_entry.replicate {
            continue;
        }

        // If dobj.TargetArn is not empty string, this is a case of specific target being re-synced.
        if !dobj.target_arn.is_empty() && dobj.target_arn != tgt_entry.arn {
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

    if should_retry_delete_marker_purge(&dobj.delete_object) {
        let bucket_clone = bucket.clone();
        let dobj_clone = dobj.clone();
        let dsc_clone = dsc.clone();
        let storage_clone = storage.clone();
        tokio::spawn(async move {
            for _ in 0..5 {
                if let Some(delete_marker_version_id) = dobj_clone.delete_object.delete_marker_version_id
                    && source_delete_marker_missing(
                        &*storage_clone,
                        &bucket_clone,
                        &dobj_clone.delete_object.object_name,
                        delete_marker_version_id,
                    )
                    .await
                {
                    replicate_delete_marker_purge_to_targets(&bucket_clone, &dobj_clone, &dsc_clone).await;
                    break;
                }
                tokio::time::sleep(TokioDuration::from_secs(1)).await;
            }
        });
    }

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

    let event_name = if replication_status == ReplicationStatusType::Completed {
        EventName::ObjectReplicationComplete.to_string()
    } else {
        EventName::ObjectReplicationFailed.to_string()
    };

    match storage
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
        }
    }
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

async fn replicate_delete_marker_purge_to_targets(bucket: &str, dobj: &DeletedObjectReplicationInfo, dsc: &ReplicateDecision) {
    let Some(delete_marker_version_id) = dobj.delete_object.delete_marker_version_id else {
        return;
    };

    for tgt_entry in dsc.targets_map.values() {
        if !tgt_entry.replicate {
            continue;
        }
        if !dobj.target_arn.is_empty() && dobj.target_arn != tgt_entry.arn {
            continue;
        }
        let Some(tgt_client) = ReplicationTargetStore::remote_target_client(bucket, &tgt_entry.arn).await else {
            continue;
        };

        let _ = tgt_client
            .remove_object(
                &tgt_client.bucket,
                &dobj.delete_object.object_name,
                Some(delete_marker_version_id.to_string()),
                replication_delete_marker_purge_remove_options(dobj.delete_object.delete_marker_mtime),
            )
            .await;
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

    let tgt_arns = if !dobj.target_arn.is_empty() {
        vec![dobj.target_arn.clone()]
    } else {
        rcfg.filter_target_arns(&ObjectOpts {
            name: object_name.clone(),
            ..Default::default()
        })
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

    let version_id = if version_id.is_nil() {
        None
    } else {
        Some(version_id.to_string())
    };

    if dobj.delete_object.delete_marker && dobj.delete_object.delete_marker_version_id.is_some() {
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
                let non_retryable = matches!(
                    &e,
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
        Ok(_) => {
            debug!(
                bucket = tgt_client.bucket,
                object = dobj.delete_object.object_name,
                version_id = ?version_id,
                delete_marker = dobj.delete_object.delete_marker,
                is_version_purge,
                "replicate_delete_to_target succeeded"
            );
            if !is_version_purge {
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
            // TODO: check offline
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

pub async fn replicate_object<S: ReplicationStorage>(roi: ReplicateObjectInfo, storage: Arc<S>) {
    let bucket = roi.bucket.clone();
    let object = roi.name.clone();

    let cfg = match get_replication_config(&bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            debug!(
                event = EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                reason = "replication_config_missing",
                "Skipping replication object because replication config is missing"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: roi.to_object_info(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return;
        }
        Err(err) => {
            error!(
                event = EVENT_RESYNC_CONFIG_LOOKUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                reason = "replication_config_lookup_failed",
                error = %err,
                "Failed to look up replication config for object replication"
            );
            send_local_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: roi.to_object_info(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return;
        }
    };

    let tgt_arns = cfg.filter_target_arns(&ObjectOpts {
        name: object.clone(),
        user_tags: roi.user_tags.clone(),
        ssec: roi.ssec,
        op_type: roi.op_type,
        // ExistingObject ops must respect per-rule ExistingObjectReplicationStatus.
        // Heal ops intentionally bypass it (repairing a past failure is not an initial sync).
        existing_object: roi.op_type == ReplicationType::ExistingObject,
        ..Default::default()
    });

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
            return;
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
            return;
        }
    };

    let mut join_set = JoinSet::new();

    for arn in tgt_arns {
        let Some(tgt_client) = ReplicationTargetStore::remote_target_client(&bucket, &arn).await else {
            debug!(
                event = EVENT_RESYNC_RUNTIME_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION_RESYNC,
                bucket = %bucket,
                arn = %arn,
                reason = "target_client_missing",
                "Skipping replication object target"
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

    let replication_status = rinfos.replication_status();
    let new_replication_internal = rinfos.replication_status_internal();
    let mut object_info = roi.to_object_info();

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

        if let Ok(u) = storage.put_object_metadata(&bucket, &object, &popts).await {
            object_info = u;
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
                if !is_err_object_not_found(&e) || is_err_version_not_found(&e) {
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
                replication_action = replication_action_for_target(
                    &replication_source_object(&object_info),
                    &replication_target_object(&oi),
                    self.op_type,
                );
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

            // TODO: check offline
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
                if !is_err_object_not_found(&e) || is_err_version_not_found(&e) {
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
                replication_action = replication_action_for_target(
                    &replication_source_object(&object_info),
                    &replication_target_object(&oi),
                    self.op_type,
                );
                rinfo.replication_status = ReplicationStatusType::Completed;
                if replication_action == ReplicationAction::None {
                    if self.op_type == ReplicationType::ExistingObject
                        && target_is_newer_than_source_null_version(
                            &replication_source_object(&object_info),
                            &replication_target_object(&oi),
                        )
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

        if replication_action != ReplicationAction::All {
            // TODO: copy object
        } else {
            let (put_opts, is_multipart) = match replication_put_object_options(&tgt_client.storage_class, &object_info) {
                Ok((put_opts, is_mp)) => (put_opts, is_mp),
                Err(e) => {
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

                // TODO: check offline
                return rinfo;
            }
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

fn part_range_spec_from_actual_size(offset: i64, part_size: i64) -> std::io::Result<(HTTPRangeSpec, i64)> {
    if offset < 0 {
        return Err(std::io::Error::other("invalid part offset"));
    }
    if part_size <= 0 {
        return Err(std::io::Error::other(format!("invalid part size {part_size}")));
    }
    let end = offset
        .checked_add(part_size - 1)
        .ok_or_else(|| std::io::Error::other("part range overflow"))?;
    let next_offset = end
        .checked_add(1)
        .ok_or_else(|| std::io::Error::other("part offset overflow"))?;
    Ok((
        HTTPRangeSpec {
            is_suffix_length: false,
            start: offset,
            end,
        },
        next_offset,
    ))
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
        let part_number = i32::try_from(part_info.number)
            .map_err(|_| std::io::Error::other(format!("part number {} overflows i32", part_info.number)))?;
        let part_size = part_info.actual_size;
        let (range_spec, next_offset) = part_range_spec_from_actual_size(offset, part_size)?;
        offset = next_offset;

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
                part_number,
                part_size,
                byte_stream,
                &PutObjectPartOptions { ..Default::default() },
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let etag = object_part.e_tag.unwrap_or_default();

        uploaded_parts.push(CompletedPart::builder().part_number(part_number).e_tag(etag).build());
    }

    let actual_size =
        rustfs_utils::http::get_str(&object_info.user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE).unwrap_or_default();

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
    use super::*;
    use aws_smithy_types::DateTime;
    use std::collections::HashMap;
    use time::{Duration, OffsetDateTime};
    use uuid::Uuid;

    #[test]
    fn test_part_range_spec_from_actual_size() {
        let (rs, next) = part_range_spec_from_actual_size(0, 10).unwrap();
        assert_eq!(rs.start, 0);
        assert_eq!(rs.end, 9);
        assert_eq!(next, 10);
    }

    #[test]
    fn test_part_range_spec_rejects_non_positive() {
        assert!(part_range_spec_from_actual_size(0, 0).is_err());
        assert!(part_range_spec_from_actual_size(0, -1).is_err());
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
        let wire_zero = OffsetDateTime::from_unix_timestamp(rustfs_replication::resync::WIRE_ZERO_TIME_UNIX)
            .expect("valid wire zero timestamp");

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
    fn test_get_replication_action_existing_object_source_newer_null_version_requires_replication() {
        let source = ObjectInfo {
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(20)),
            version_id: None,
            ..Default::default()
        };
        let target = HeadObjectOutput::builder().last_modified(DateTime::from_secs(10)).build();

        assert_eq!(
            replication_action_for_target(
                &replication_source_object(&source),
                &replication_target_object(&target),
                ReplicationType::ExistingObject,
            ),
            ReplicationAction::All,
            "a newer source null version must not be skipped during existing-object replication"
        );
    }

    #[test]
    fn test_get_replication_action_existing_object_target_newer_null_version_skips() {
        let source = ObjectInfo {
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(10)),
            version_id: None,
            ..Default::default()
        };
        let target = HeadObjectOutput::builder().last_modified(DateTime::from_secs(20)).build();

        assert_eq!(
            replication_action_for_target(
                &replication_source_object(&source),
                &replication_target_object(&target),
                ReplicationType::ExistingObject,
            ),
            ReplicationAction::None,
            "a newer target null-version object should not be overwritten by existing-object replication"
        );
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
        let dobj = DeletedObject {
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
        let dobj = DeletedObject {
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
        let dobj = DeletedObject {
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
        let dobj = DeletedObject {
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
    fn test_content_matches_compares_etag_only() {
        let src = ObjectInfo {
            etag: Some("\"abc123\"".to_string()),
            ..Default::default()
        };

        let tgt_match = HeadObjectOutput::builder().e_tag("\"abc123\"").build();
        assert!(
            content_matches_by_etag(&replication_source_object(&src), &replication_target_object(&tgt_match)),
            "identical ETags must match"
        );

        let tgt_unquoted_match = HeadObjectOutput::builder().e_tag("abc123").build();
        assert!(
            content_matches_by_etag(&replication_source_object(&src), &replication_target_object(&tgt_unquoted_match)),
            "quoted and unquoted ETags with identical values must match"
        );

        // version_id on the target is intentionally ignored
        let tgt_different_version = HeadObjectOutput::builder()
            .e_tag("\"abc123\"")
            .version_id("aws-alphanumeric-id")
            .build();
        assert!(
            content_matches_by_etag(&replication_source_object(&src), &replication_target_object(&tgt_different_version)),
            "matching ETags with different version IDs must still match"
        );

        let tgt_different_content = HeadObjectOutput::builder().e_tag("\"def456\"").build();
        assert!(
            !content_matches_by_etag(&replication_source_object(&src), &replication_target_object(&tgt_different_content)),
            "different ETags must not match"
        );

        let src_no_etag = ObjectInfo {
            etag: None,
            ..Default::default()
        };
        assert!(
            !content_matches_by_etag(&replication_source_object(&src_no_etag), &replication_target_object(&tgt_match)),
            "missing source ETag must not match"
        );

        let tgt_no_etag = HeadObjectOutput::builder().build();
        assert!(
            !content_matches_by_etag(&replication_source_object(&src), &replication_target_object(&tgt_no_etag)),
            "missing target ETag must not match"
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
        let roi = get_heal_replicate_object_info(&oi, &rcfg).await;

        assert_eq!(roi.replication_status, ReplicationStatusType::Failed);
        assert_eq!(roi.op_type, ReplicationType::Heal);
        assert!(
            roi.dsc.replicate_any() || roi.dsc.targets_map.is_empty(),
            "With no replication config, dsc may be empty; with config, replicate_any() would be true and queueing would occur"
        );
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
            ..Default::default()
        };
        let rcfg = ReplicationConfig::new(
            Some(ReplicationConfiguration {
                role: role.to_string(),
                rules: vec![],
            }),
            None,
        );
        let roi = get_heal_replicate_object_info(&oi, &rcfg).await;

        assert_eq!(roi.replication_status_internal, None);
        assert_eq!(roi.version_purge_status_internal.as_deref(), Some(format!("{role}=PENDING;").as_str()));
        assert_eq!(roi.target_purge_statuses.get(role), Some(&VersionPurgeStatusType::Pending));
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
