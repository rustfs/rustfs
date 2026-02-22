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

use crate::bucket::bucket_target_sys::{
    AdvancedPutOptions, BucketTargetSys, PutObjectOptions, PutObjectPartOptions, RemoveObjectOptions, TargetClient,
};
use crate::bucket::metadata_sys;
use crate::bucket::replication::ResyncStatusType;
use crate::bucket::replication::{ObjectOpts, ReplicationConfigurationExt as _};
use crate::bucket::tagging::decode_tags_to_map;
use crate::bucket::target::BucketTargets;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::client::api_get_options::{AdvancedGetOptions, StatObjectOptions};
use crate::config::com::save_config;
use crate::disk::BUCKET_META_PREFIX;
use crate::error::{Error, Result, is_err_object_not_found, is_err_version_not_found};
use crate::event::name::EventName;
use crate::event_notification::{EventArgs, send_event};
use crate::global::GLOBAL_LocalNodeName;
use crate::set_disk::get_lock_acquire_timeout;
use crate::store_api::{DeletedObject, ObjectInfo, ObjectOptions, ObjectToDelete, WalkOptions};
use crate::{StorageAPI, new_object_layer_fn};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedPart, ObjectLockLegalHoldStatus};
use byteorder::ByteOrder;
use futures::future::join_all;
use http::HeaderMap;
use regex::Regex;
use rustfs_filemeta::{
    MrfReplicateEntry, REPLICATE_EXISTING, REPLICATE_EXISTING_DELETE, REPLICATION_RESET, ReplicateDecision, ReplicateObjectInfo,
    ReplicateTargetDecision, ReplicatedInfos, ReplicatedTargetInfo, ReplicationAction, ReplicationState, ReplicationStatusType,
    ReplicationType, ReplicationWorkerOperation, ResyncDecision, ResyncTargetDecision, VersionPurgeStatusType,
    get_replication_state, parse_replicate_decision, replication_statuses_map, target_reset_header, version_purge_statuses_map,
};
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_TAGGING, AMZ_TAGGING_DIRECTIVE, CONTENT_ENCODING, HeaderExt as _,
    RESERVED_METADATA_PREFIX, RESERVED_METADATA_PREFIX_LOWER, RUSTFS_REPLICATION_ACTUAL_OBJECT_SIZE,
    RUSTFS_REPLICATION_RESET_STATUS, SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER, headers,
};
use rustfs_utils::path::path_join_buf;
use rustfs_utils::string::strings_has_prefix_fold;
use rustfs_utils::{DEFAULT_SIP_HASH_KEY, sip_hash};
use s3s::dto::ReplicationConfiguration;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::Duration as TokioDuration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const REPLICATION_DIR: &str = ".replication";
const RESYNC_FILE_NAME: &str = "resync.bin";
const RESYNC_META_FORMAT: u16 = 1;
const RESYNC_META_VERSION: u16 = 1;
const RESYNC_TIME_INTERVAL: TokioDuration = TokioDuration::from_secs(60);

#[derive(Debug, Clone, Default)]
pub struct ResyncOpts {
    pub bucket: String,
    pub arn: String,
    pub resync_id: String,
    pub resync_before: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TargetReplicationResyncStatus {
    pub start_time: Option<OffsetDateTime>,
    pub last_update: Option<OffsetDateTime>,
    pub resync_id: String,
    pub resync_before_date: Option<OffsetDateTime>,
    pub resync_status: ResyncStatusType,
    pub failed_size: i64,
    pub failed_count: i64,
    pub replicated_size: i64,
    pub replicated_count: i64,
    pub bucket: String,
    pub object: String,
    pub error: Option<String>,
}

impl TargetReplicationResyncStatus {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BucketReplicationResyncStatus {
    pub version: u16,
    pub targets_map: HashMap<String, TargetReplicationResyncStatus>,
    pub id: i32,
    pub last_update: Option<OffsetDateTime>,
}

impl BucketReplicationResyncStatus {
    pub fn new() -> Self {
        Self {
            version: RESYNC_META_VERSION,
            ..Default::default()
        }
    }

    pub fn clone_tgt_stats(&self) -> HashMap<String, TargetReplicationResyncStatus> {
        self.targets_map.clone()
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        Ok(rmp_serde::to_vec(&self)?)
    }

    pub fn unmarshal_msg(data: &[u8]) -> Result<Self> {
        Ok(rmp_serde::from_slice(data)?)
    }
}

static RESYNC_WORKER_COUNT: usize = 10;

#[derive(Debug)]
pub struct ReplicationResyncer {
    pub status_map: Arc<RwLock<HashMap<String, BucketReplicationResyncStatus>>>,
    pub worker_size: usize,
    pub resync_cancel_tx: CancellationToken,
    pub resync_cancel_rx: CancellationToken,
    pub worker_tx: tokio::sync::broadcast::Sender<()>,
    pub worker_rx: tokio::sync::broadcast::Receiver<()>,
}

impl ReplicationResyncer {
    pub async fn new() -> Self {
        let resync_cancel_tx = CancellationToken::new();
        let resync_cancel_rx = resync_cancel_tx.clone();
        let (worker_tx, worker_rx) = tokio::sync::broadcast::channel(RESYNC_WORKER_COUNT);

        for _ in 0..RESYNC_WORKER_COUNT {
            if let Err(err) = worker_tx.send(()) {
                error!("Failed to send worker message: {}", err);
            }
        }

        Self {
            status_map: Arc::new(RwLock::new(HashMap::new())),
            worker_size: RESYNC_WORKER_COUNT,
            resync_cancel_tx,
            resync_cancel_rx,
            worker_tx,
            worker_rx,
        }
    }

    pub async fn mark_status<S: StorageAPI>(&self, status: ResyncStatusType, opts: ResyncOpts, obj_layer: Arc<S>) -> Result<()> {
        let bucket_status = {
            let mut status_map = self.status_map.write().await;

            let bucket_status = if let Some(bucket_status) = status_map.get_mut(&opts.bucket) {
                bucket_status
            } else {
                let mut bucket_status = BucketReplicationResyncStatus::new();
                bucket_status.id = 0;
                status_map.insert(opts.bucket.clone(), bucket_status);
                status_map.get_mut(&opts.bucket).unwrap()
            };

            let state = if let Some(state) = bucket_status.targets_map.get_mut(&opts.arn) {
                state
            } else {
                let state = TargetReplicationResyncStatus::new();
                bucket_status.targets_map.insert(opts.arn.clone(), state);
                bucket_status.targets_map.get_mut(&opts.arn).unwrap()
            };

            state.resync_status = status;
            state.last_update = Some(OffsetDateTime::now_utc());

            bucket_status.last_update = Some(OffsetDateTime::now_utc());

            bucket_status.clone()
        };

        save_resync_status(&opts.bucket, &bucket_status, obj_layer).await?;

        Ok(())
    }

    pub async fn inc_stats(&self, status: &TargetReplicationResyncStatus, opts: ResyncOpts) {
        let mut status_map = self.status_map.write().await;

        let bucket_status = if let Some(bucket_status) = status_map.get_mut(&opts.bucket) {
            bucket_status
        } else {
            let mut bucket_status = BucketReplicationResyncStatus::new();
            bucket_status.id = 0;
            status_map.insert(opts.bucket.clone(), bucket_status);
            status_map.get_mut(&opts.bucket).unwrap()
        };

        let state = if let Some(state) = bucket_status.targets_map.get_mut(&opts.arn) {
            state
        } else {
            let state = TargetReplicationResyncStatus::new();
            bucket_status.targets_map.insert(opts.arn.clone(), state);
            bucket_status.targets_map.get_mut(&opts.arn).unwrap()
        };

        state.object = status.object.clone();
        state.replicated_count += status.replicated_count;
        state.replicated_size += status.replicated_size;
        state.failed_count += status.failed_count;
        state.failed_size += status.failed_size;
        state.last_update = Some(OffsetDateTime::now_utc());
        bucket_status.last_update = Some(OffsetDateTime::now_utc());
    }

    pub async fn persist_to_disk<S: StorageAPI>(&self, cancel_token: CancellationToken, api: Arc<S>) {
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
                                error!("Failed to save resync status: {}", err);
                            } else {
                                last_update_times.insert(bucket.clone(), status.last_update.unwrap());
                            }
                        }
                    }

                   interval.reset();
                }
            }
        }
    }

    async fn resync_bucket_mark_status<S: StorageAPI>(&self, status: ResyncStatusType, opts: ResyncOpts, storage: Arc<S>) {
        if let Err(err) = self.mark_status(status, opts.clone(), storage.clone()).await {
            error!("Failed to mark resync status: {}", err);
        }
        if let Err(err) = self.worker_tx.send(()) {
            error!("Failed to send worker message: {}", err);
        }
        // TODO: Metrics
    }

    pub async fn resync_bucket<S: StorageAPI>(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
        storage: Arc<S>,
        heal: bool,
        opts: ResyncOpts,
    ) {
        let mut worker_rx = self.worker_rx.resubscribe();

        tokio::select! {
            _ = cancellation_token.cancelled() => {
                return;
            }

            _ = worker_rx.recv() => {}
        }

        let cfg = match get_replication_config(&opts.bucket).await {
            Ok(cfg) => cfg,
            Err(err) => {
                error!("Failed to get replication config: {}", err);
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }
        };

        let targets = match BucketTargetSys::get().list_bucket_targets(&opts.bucket).await {
            Ok(targets) => targets,
            Err(err) => {
                warn!("Failed to list bucket targets: {}", err);
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
                "replication resync failed for {} - arn specified {} is missing in the replication config",
                opts.bucket, opts.arn
            );
            self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                .await;
            return;
        }

        let Some(target_client) = BucketTargetSys::get()
            .get_remote_target_client(&opts.bucket, &target_arns[0])
            .await
        else {
            error!(
                "replication resync failed for {} - arn specified {} is missing in the bucket targets",
                opts.bucket, opts.arn
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
            error!("Failed to mark resync status: {}", e);
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        if let Err(err) = storage
            .clone()
            .walk(cancellation_token.clone(), &opts.bucket, "", tx.clone(), WalkOptions::default())
            .await
        {
            error!("Failed to walk bucket {}: {}", opts.bucket, err);
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
            let resync_cancel_rx = self.resync_cancel_rx.clone();
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

                    let (size, err) = if let Err(err) = target_client
                        .head_object(&target_client.bucket, &roi.name, roi.version_id.map(|v| v.to_string()))
                        .await
                    {
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

                    info!(
                        "resynced reset_id:{} object: {}/{}-{} size:{} err:{:?}",
                        reset_id,
                        bucket_name,
                        roi.name,
                        roi.version_id.unwrap_or_default(),
                        size,
                        err,
                    );

                    if resync_cancel_rx.is_cancelled() {
                        return;
                    }

                    if cancel_token.is_cancelled() {
                        return;
                    }

                    if let Err(err) = results_tx.send(st) {
                        error!("Failed to send resync status: {}", err);
                    }
                }
            });

            futures.push(f);
        }

        let resync_cancel_rx = self.resync_cancel_rx.clone();

        while let Some(res) = rx.recv().await {
            if let Some(err) = res.err {
                error!("Failed to get object info: {}", err);
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }

            if resync_cancel_rx.is_cancelled() {
                self.resync_bucket_mark_status(ResyncStatusType::ResyncCanceled, opts.clone(), storage.clone())
                    .await;
                return;
            }

            if cancellation_token.is_cancelled() {
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
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

            if resync_cancel_rx.is_cancelled() {
                self.resync_bucket_mark_status(ResyncStatusType::ResyncCanceled, opts.clone(), storage.clone())
                    .await;
                return;
            }

            if cancellation_token.is_cancelled() {
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }

            let worker_idx = sip_hash(&roi.name, RESYNC_WORKER_COUNT, &DEFAULT_SIP_HASH_KEY) as usize;

            if let Err(err) = worker_txs[worker_idx].send(roi).await {
                error!("Failed to send object info to worker: {}", err);
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
    let mut user_defined = oi.user_defined.clone();

    if let Some(rc) = rcfg.config.as_ref()
        && !rc.role.is_empty()
    {
        if !oi.replication_status.is_empty() {
            oi.replication_status_internal = Some(format!("{}={};", rc.role, oi.replication_status.as_str()));
        }

        if !oi.replication_status.is_empty() {
            oi.replication_status_internal = Some(format!("{}={};", rc.role, oi.replication_status.as_str()));
        }

        let keys_to_update: Vec<_> = user_defined
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(format!("{RESERVED_METADATA_PREFIX_LOWER}{REPLICATION_RESET}").as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (k, v) in keys_to_update {
            user_defined.remove(&k);
            user_defined.insert(target_reset_header(rc.role.as_str()), v);
        }
    }

    let dsc = if oi.delete_marker || !oi.replication_status.is_empty() {
        check_replicate_delete(
            oi.bucket.as_str(),
            &ObjectToDelete {
                object_name: oi.name.clone(),
                version_id: oi.version_id,
                ..Default::default()
            },
            &oi,
            &ObjectOptions {
                versioned: BucketVersioningSys::prefix_enabled(&oi.bucket, &oi.name).await,
                version_suspended: BucketVersioningSys::prefix_suspended(&oi.bucket, &oi.name).await,
                ..Default::default()
            },
            None,
        )
        .await
    } else {
        must_replicate(
            oi.bucket.as_str(),
            &oi.name,
            MustReplicateOptions::new(
                &user_defined,
                oi.user_tags.clone(),
                ReplicationStatusType::Empty,
                ReplicationType::Heal,
                ObjectOptions::default(),
            ),
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
        user_tags: oi.user_tags.clone(),
        checksum: oi.checksum.clone(),
        retry_count: 0,
    }
}

async fn save_resync_status<S: StorageAPI>(bucket: &str, status: &BucketReplicationResyncStatus, api: Arc<S>) -> Result<()> {
    let buf = status.marshal_msg()?;

    let mut data = Vec::new();

    let mut major = [0u8; 2];
    byteorder::LittleEndian::write_u16(&mut major, RESYNC_META_FORMAT);
    data.extend_from_slice(&major);

    let mut minor = [0u8; 2];
    byteorder::LittleEndian::write_u16(&mut minor, RESYNC_META_VERSION);
    data.extend_from_slice(&minor);

    data.extend_from_slice(&buf);

    let config_file = path_join_buf(&[BUCKET_META_PREFIX, bucket, REPLICATION_DIR, RESYNC_FILE_NAME]);
    save_config(api, &config_file, data).await?;

    Ok(())
}

async fn get_replication_config(bucket: &str) -> Result<Option<ReplicationConfiguration>> {
    let config = match metadata_sys::get_replication_config(bucket).await {
        Ok((config, _)) => Some(config),
        Err(err) => {
            if err != Error::ConfigNotFound {
                return Err(err);
            }
            None
        }
    };
    Ok(config)
}

#[derive(Debug, Clone, Default)]
pub struct DeletedObjectReplicationInfo {
    pub delete_object: DeletedObject,
    pub bucket: String,
    pub event_type: String,
    pub op_type: ReplicationType,
    pub reset_id: String,
    pub target_arn: String,
}

impl ReplicationWorkerOperation for DeletedObjectReplicationInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_mrf_entry(&self) -> MrfReplicateEntry {
        MrfReplicateEntry {
            bucket: self.bucket.clone(),
            object: self.delete_object.object_name.clone(),
            version_id: None,
            retry_count: 0,
            size: 0,
        }
    }

    fn get_bucket(&self) -> &str {
        &self.bucket
    }

    fn get_object(&self) -> &str {
        &self.delete_object.object_name
    }

    fn get_size(&self) -> i64 {
        0
    }

    fn is_delete_marker(&self) -> bool {
        true
    }

    fn get_op_type(&self) -> ReplicationType {
        self.op_type
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub config: Option<ReplicationConfiguration>,
    pub remotes: Option<BucketTargets>,
}

impl ReplicationConfig {
    pub fn new(config: Option<ReplicationConfiguration>, remotes: Option<BucketTargets>) -> Self {
        Self { config, remotes }
    }

    pub fn is_empty(&self) -> bool {
        self.config.is_none()
    }

    pub fn replicate(&self, obj: &ObjectOpts) -> bool {
        self.config.as_ref().is_some_and(|config| config.replicate(obj))
    }

    pub async fn resync(
        &self,
        oi: ObjectInfo,
        dsc: ReplicateDecision,
        status: &HashMap<String, ReplicationStatusType>,
    ) -> ResyncDecision {
        if self.is_empty() {
            return ResyncDecision::default();
        }

        let mut dsc = dsc;

        if oi.delete_marker {
            let opts = ObjectOpts {
                name: oi.name.clone(),
                version_id: oi.version_id,
                delete_marker: true,
                op_type: ReplicationType::Delete,
                existing_object: true,
                ..Default::default()
            };
            let arns = self
                .config
                .as_ref()
                .map(|config| config.filter_target_arns(&opts))
                .unwrap_or_default();

            if arns.is_empty() {
                return ResyncDecision::default();
            }

            for arn in arns {
                let mut opts = opts.clone();
                opts.target_arn = arn;

                dsc.set(ReplicateTargetDecision::new(opts.target_arn.clone(), self.replicate(&opts), false));
            }

            return self.resync_internal(oi, dsc, status);
        }

        let mut user_defined = oi.user_defined.clone();
        user_defined.remove(AMZ_BUCKET_REPLICATION_STATUS);

        let dsc = must_replicate(
            oi.bucket.as_str(),
            &oi.name,
            MustReplicateOptions::new(
                &user_defined,
                oi.user_tags.clone(),
                ReplicationStatusType::Empty,
                ReplicationType::ExistingObject,
                ObjectOptions::default(),
            ),
        )
        .await;

        self.resync_internal(oi, dsc, status)
    }

    fn resync_internal(
        &self,
        oi: ObjectInfo,
        dsc: ReplicateDecision,
        status: &HashMap<String, ReplicationStatusType>,
    ) -> ResyncDecision {
        let Some(remotes) = self.remotes.as_ref() else {
            return ResyncDecision::default();
        };

        if remotes.is_empty() {
            return ResyncDecision::default();
        }

        let mut resync_decision = ResyncDecision::default();

        for target in remotes.targets.iter() {
            if let Some(decision) = dsc.targets_map.get(&target.arn)
                && decision.replicate
            {
                resync_decision.targets.insert(
                    decision.arn.clone(),
                    resync_target(
                        &oi,
                        &target.arn,
                        &target.reset_id,
                        target.reset_before_date,
                        status.get(&decision.arn).unwrap_or(&ReplicationStatusType::Empty).clone(),
                    ),
                );
            }
        }

        resync_decision
    }
}

pub fn resync_target(
    oi: &ObjectInfo,
    arn: &str,
    reset_id: &str,
    reset_before_date: Option<OffsetDateTime>,
    status: ReplicationStatusType,
) -> ResyncTargetDecision {
    let rs = oi
        .user_defined
        .get(target_reset_header(arn).as_str())
        .or(oi.user_defined.get(RUSTFS_REPLICATION_RESET_STATUS))
        .map(|s| s.to_string());

    let mut dec = ResyncTargetDecision::default();

    let mod_time = oi.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH);

    if rs.is_none() {
        let reset_before_date = reset_before_date.unwrap_or(OffsetDateTime::UNIX_EPOCH);
        if !reset_id.is_empty() && mod_time < reset_before_date {
            dec.replicate = true;
            return dec;
        }

        dec.replicate = status == ReplicationStatusType::Empty;

        return dec;
    }

    if reset_id.is_empty() || reset_before_date.is_none() {
        return dec;
    }

    let rs = rs.unwrap();
    let reset_before_date = reset_before_date.unwrap();

    let parts: Vec<&str> = rs.splitn(2, ';').collect();

    if parts.len() != 2 {
        return dec;
    }

    let new_reset = parts[0] == reset_id;

    if !new_reset && status == ReplicationStatusType::Completed {
        return dec;
    }

    dec.replicate = new_reset && mod_time < reset_before_date;

    dec
}

pub struct MustReplicateOptions {
    meta: HashMap<String, String>,
    status: ReplicationStatusType,
    op_type: ReplicationType,
    replication_request: bool,
}

impl MustReplicateOptions {
    pub fn new(
        meta: &HashMap<String, String>,
        user_tags: String,
        status: ReplicationStatusType,
        op_type: ReplicationType,
        opts: ObjectOptions,
    ) -> Self {
        let mut meta = meta.clone();
        if !user_tags.is_empty() {
            meta.insert(AMZ_OBJECT_TAGGING.to_string(), user_tags);
        }

        Self {
            meta,
            status,
            op_type,
            replication_request: opts.replication_request,
        }
    }

    pub fn from_object_info(oi: &ObjectInfo, op_type: ReplicationType, opts: ObjectOptions) -> Self {
        Self::new(&oi.user_defined, oi.user_tags.clone(), oi.replication_status.clone(), op_type, opts)
    }

    pub fn replication_status(&self) -> ReplicationStatusType {
        if let Some(rs) = self.meta.get(AMZ_BUCKET_REPLICATION_STATUS) {
            return ReplicationStatusType::from(rs.as_str());
        }
        ReplicationStatusType::default()
    }

    pub fn is_existing_object_replication(&self) -> bool {
        self.op_type == ReplicationType::ExistingObject
    }

    pub fn is_metadata_replication(&self) -> bool {
        self.op_type == ReplicationType::Metadata
    }
}

pub fn get_must_replicate_options(
    user_defined: &HashMap<String, String>,
    user_tags: String,
    status: ReplicationStatusType,
    op_type: ReplicationType,
    opts: ObjectOptions,
) -> MustReplicateOptions {
    MustReplicateOptions::new(user_defined, user_tags, status, op_type, opts)
}

/// Returns whether object version is a delete marker and if object qualifies for replication
pub async fn check_replicate_delete(
    bucket: &str,
    dobj: &ObjectToDelete,
    oi: &ObjectInfo,
    del_opts: &ObjectOptions,
    gerr: Option<String>,
) -> ReplicateDecision {
    let rcfg = match get_replication_config(bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            // warn!("No replication config found for bucket: {}", bucket);
            return ReplicateDecision::default();
        }
        Err(err) => {
            error!("Failed to get replication config for bucket {}: {}", bucket, err);
            return ReplicateDecision::default();
        }
    };

    // If incoming request is a replication request, it does not need to be re-replicated.
    if del_opts.replication_request {
        return ReplicateDecision::default();
    }

    // Skip replication if this object's prefix is excluded from being versioned.
    if !del_opts.versioned {
        return ReplicateDecision::default();
    }

    let opts = ObjectOpts {
        name: dobj.object_name.clone(),
        ssec: is_ssec_encrypted(&oi.user_defined),
        user_tags: oi.user_tags.clone(),
        delete_marker: oi.delete_marker,
        version_id: dobj.version_id,
        op_type: ReplicationType::Delete,
        ..Default::default()
    };

    let tgt_arns = rcfg.filter_target_arns(&opts);
    let mut dsc = ReplicateDecision::new();

    if tgt_arns.is_empty() {
        return dsc;
    }

    for tgt_arn in tgt_arns {
        let mut opts = opts.clone();
        opts.target_arn = tgt_arn.clone();
        let replicate = rcfg.replicate(&opts);
        let sync = false; // Default sync value

        // When incoming delete is removal of a delete marker (a.k.a versioned delete),
        // GetObjectInfo returns extra information even though it returns errFileNotFound
        if gerr.is_some() {
            let valid_repl_status = matches!(
                oi.target_replication_status(&tgt_arn),
                ReplicationStatusType::Pending | ReplicationStatusType::Completed | ReplicationStatusType::Failed
            );

            if oi.delete_marker && (valid_repl_status || replicate) {
                dsc.set(ReplicateTargetDecision::new(tgt_arn, replicate, sync));
                continue;
            }

            // Can be the case that other cluster is down and duplicate `mc rm --vid`
            // is issued - this still needs to be replicated back to the other target
            if oi.version_purge_status != VersionPurgeStatusType::default() {
                let replicate = oi.version_purge_status == VersionPurgeStatusType::Pending
                    || oi.version_purge_status == VersionPurgeStatusType::Failed;
                dsc.set(ReplicateTargetDecision::new(tgt_arn, replicate, sync));
            }
            continue;
        }

        let tgt = BucketTargetSys::get().get_remote_target_client(bucket, &tgt_arn).await;
        // The target online status should not be used here while deciding
        // whether to replicate deletes as the target could be temporarily down
        let tgt_dsc = if let Some(tgt) = tgt {
            ReplicateTargetDecision::new(tgt_arn, replicate, tgt.replicate_sync)
        } else {
            ReplicateTargetDecision::new(tgt_arn, false, false)
        };
        dsc.set(tgt_dsc);
    }

    dsc
}

/// Check if the user-defined metadata contains SSEC encryption headers
fn is_ssec_encrypted(user_defined: &std::collections::HashMap<String, String>) -> bool {
    user_defined.contains_key(SSEC_ALGORITHM_HEADER)
        || user_defined.contains_key(SSEC_KEY_HEADER)
        || user_defined.contains_key(SSEC_KEY_MD5_HEADER)
}

/// Extension trait for ObjectInfo to add replication-related methods
pub trait ObjectInfoExt {
    fn target_replication_status(&self, arn: &str) -> ReplicationStatusType;
    fn replication_state(&self) -> ReplicationState;
}

impl ObjectInfoExt for ObjectInfo {
    /// Returns replication status of a target
    fn target_replication_status(&self, arn: &str) -> ReplicationStatusType {
        lazy_static::lazy_static! {
            static ref REPL_STATUS_REGEX: Regex = Regex::new(r"([^=].*?)=([^,].*?);").unwrap();
        }

        let binding = self.replication_status_internal.clone().unwrap_or_default();
        let captures = REPL_STATUS_REGEX.captures_iter(&binding);
        for cap in captures {
            if cap.len() == 3 && &cap[1] == arn {
                return ReplicationStatusType::from(&cap[2]);
            }
        }
        ReplicationStatusType::default()
    }

    fn replication_state(&self) -> ReplicationState {
        ReplicationState {
            replication_status_internal: self.replication_status_internal.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            replicate_decision_str: self.replication_decision.clone(),
            targets: replication_statuses_map(&self.replication_status_internal.clone().unwrap_or_default()),
            purge_targets: version_purge_statuses_map(&self.version_purge_status_internal.clone().unwrap_or_default()),
            reset_statuses_map: self
                .user_defined
                .iter()
                .filter_map(|(k, v)| {
                    if k.starts_with(&format!("{RESERVED_METADATA_PREFIX_LOWER}-{REPLICATION_RESET}")) {
                        Some((
                            k.trim_start_matches(&format!("{RESERVED_METADATA_PREFIX_LOWER}-{REPLICATION_RESET}"))
                                .to_string(),
                            v.clone(),
                        ))
                    } else {
                        None
                    }
                })
                .collect(),
            ..Default::default()
        }
    }
}

pub async fn must_replicate(bucket: &str, object: &str, mopts: MustReplicateOptions) -> ReplicateDecision {
    if new_object_layer_fn().is_none() {
        return ReplicateDecision::default();
    }

    if !BucketVersioningSys::prefix_enabled(bucket, object).await {
        return ReplicateDecision::default();
    }

    let replication_status = mopts.replication_status();

    if replication_status == ReplicationStatusType::Replica && !mopts.is_metadata_replication() {
        return ReplicateDecision::default();
    }

    if mopts.replication_request {
        return ReplicateDecision::default();
    }

    let cfg = match get_replication_config(bucket).await {
        Ok(cfg) => {
            if let Some(cfg) = cfg {
                cfg
            } else {
                return ReplicateDecision::default();
            }
        }
        Err(_err) => {
            return ReplicateDecision::default();
        }
    };

    let opts = ObjectOpts {
        name: object.to_string(),
        replica: replication_status == ReplicationStatusType::Replica,
        existing_object: mopts.is_existing_object_replication(),
        user_tags: mopts.meta.get(AMZ_OBJECT_TAGGING).map(|s| s.to_string()).unwrap_or_default(),
        ..Default::default()
    };

    let arns = cfg.filter_target_arns(&opts);

    if arns.is_empty() {
        return ReplicateDecision::default();
    }

    let mut dsc = ReplicateDecision::default();

    for arn in arns {
        let cli = BucketTargetSys::get().get_remote_target_client(bucket, &arn).await;

        let mut sopts = opts.clone();
        sopts.target_arn = arn.clone();

        let replicate = cfg.replicate(&sopts);
        let synchronous = if let Some(cli) = cli { cli.replicate_sync } else { false };

        dsc.set(ReplicateTargetDecision::new(arn, replicate, synchronous));
    }

    dsc
}

pub async fn replicate_delete<S: StorageAPI>(dobj: DeletedObjectReplicationInfo, storage: Arc<S>) {
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
            warn!("No replication config found for bucket: {}", bucket);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });

            return;
        }
        Err(err) => {
            warn!("replication config for bucket: {} error: {}", bucket, err);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
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
            warn!(
                "failed to parse replicate decision for bucket:{} arn:{} error:{}",
                bucket, dobj.target_arn, err
            );
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
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
            warn!(
                "failed to get ns lock for bucket:{} object:{} error:{}",
                bucket, dobj.delete_object.object_name, e
            );
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
    };

    let _lock_guard = match ns_lock.get_write_lock(get_lock_acquire_timeout()).await {
        Ok(lock_guard) => lock_guard,
        Err(e) => {
            warn!(
                "failed to get write lock for bucket:{} object:{} error:{}",
                bucket, dobj.delete_object.object_name, e
            );
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
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
        let Some(tgt_client) = BucketTargetSys::get().get_remote_target_client(&bucket, &tgt_entry.arn).await else {
            warn!("failed to get target for bucket:{:?}, arn:{:?}", &bucket, &tgt_entry.arn);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
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
                error!("replicate_delete task failed: {}", e);
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
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

    let (replication_status, prev_status) = if dobj.delete_object.version_id.is_none() {
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

    for tgt in rinfos.targets.iter() {
        if tgt.replication_status != tgt.prev_replication_status {
            // TODO: update global replication status
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
        EventName::ObjectReplicationComplete.as_ref().to_string()
    } else {
        EventName::ObjectReplicationFailed.as_ref().to_string()
    };

    match storage
        .delete_object(
            &bucket,
            &dobj.delete_object.object_name,
            ObjectOptions {
                version_id: version_id.map(|v| v.to_string()),
                mod_time: dobj.delete_object.delete_marker_mtime,
                delete_replication: Some(drs),
                versioned: BucketVersioningSys::prefix_enabled(&bucket, &dobj.delete_object.object_name).await,
                version_suspended: BucketVersioningSys::prefix_suspended(&bucket, &dobj.delete_object.object_name).await,
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
            error!("failed to delete object for bucket:{} arn:{} error:{}", bucket, dobj.target_arn, e);
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

async fn replicate_force_delete_to_targets<S: StorageAPI>(dobj: &DeletedObjectReplicationInfo, storage: Arc<S>) {
    let bucket = &dobj.bucket;
    let object_name = &dobj.delete_object.object_name;

    let rcfg = match get_replication_config(bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            warn!("replicate force-delete: no replication config for bucket:{}", bucket);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
        Err(err) => {
            warn!("replicate force-delete: replication config error bucket:{} error:{}", bucket, err);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
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
                "replicate force-delete: failed to get ns lock bucket:{} object:{} error:{}",
                bucket, object_name, e
            );
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
    };

    let _lock_guard = match ns_lock.get_write_lock(get_lock_acquire_timeout()).await {
        Ok(guard) => guard,
        Err(e) => {
            warn!(
                "replicate force-delete: failed to get write lock bucket:{} object:{} error:{}",
                bucket, object_name, e
            );
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
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
        let Some(tgt_client) = BucketTargetSys::get().get_remote_target_client(bucket, &arn).await else {
            warn!("replicate force-delete: failed to get target client bucket:{} arn:{}", bucket, arn);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            continue;
        };

        let bucket = bucket.clone();
        let object_name = object_name.clone();

        join_set.spawn(async move {
            if BucketTargetSys::get().is_offline(&tgt_client.to_url()).await {
                error!("replicate force-delete: target offline bucket:{} arn:{}", bucket, tgt_client.arn);
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationFailed.as_ref().to_string(),
                    bucket_name: bucket.clone(),
                    object: ObjectInfo {
                        bucket: bucket.clone(),
                        name: object_name.clone(),
                        ..Default::default()
                    },
                    user_agent: "Internal: [Replication]".to_string(),
                    host: GLOBAL_LocalNodeName.to_string(),
                    ..Default::default()
                });
                return;
            }

            if let Err(e) = tgt_client
                .remove_object(
                    &tgt_client.bucket,
                    &object_name,
                    None,
                    RemoveObjectOptions {
                        force_delete: true,
                        governance_bypass: false,
                        replication_delete_marker: false,
                        replication_mtime: None,
                        replication_status: ReplicationStatusType::Replica,
                        replication_request: true,
                        replication_validity_check: false,
                    },
                )
                .await
            {
                error!(
                    "replicate force-delete failed bucket:{} object:{} arn:{} error:{}",
                    bucket, object_name, tgt_client.arn, e
                );
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationFailed.as_ref().to_string(),
                    bucket_name: bucket.clone(),
                    object: ObjectInfo {
                        bucket: bucket.clone(),
                        name: object_name.clone(),
                        ..Default::default()
                    },
                    user_agent: "Internal: [Replication]".to_string(),
                    host: GLOBAL_LocalNodeName.to_string(),
                    ..Default::default()
                });
            }
        });
    }

    while let Some(result) = join_set.join_next().await {
        if let Err(e) = result {
            error!("replicate force-delete task panicked: {}", e);
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

    if dobj.delete_object.version_id.is_none()
        && rinfo.prev_replication_status == ReplicationStatusType::Completed
        && dobj.op_type != ReplicationType::ExistingObject
    {
        rinfo.replication_status = rinfo.prev_replication_status.clone();
        return rinfo;
    }

    if dobj.delete_object.version_id.is_some() && rinfo.version_purge_status == VersionPurgeStatusType::Complete {
        return rinfo;
    }

    if BucketTargetSys::get().is_offline(&tgt_client.to_url()).await {
        if dobj.delete_object.version_id.is_none() {
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

    if dobj.delete_object.delete_marker_version_id.is_some()
        && let Err(e) = tgt_client
            .head_object(&tgt_client.bucket, &dobj.delete_object.object_name, version_id.clone())
            .await
        && let SdkError::ServiceError(service_err) = &e
        && !service_err.err().is_not_found()
    {
        rinfo.replication_status = ReplicationStatusType::Failed;
        rinfo.error = Some(e.to_string());

        return rinfo;
    };

    match tgt_client
        .remove_object(
            &tgt_client.bucket,
            &dobj.delete_object.object_name,
            version_id.clone(),
            RemoveObjectOptions {
                force_delete: false,
                governance_bypass: false,
                replication_delete_marker: dobj.delete_object.delete_marker_version_id.is_some(),
                replication_mtime: dobj.delete_object.delete_marker_mtime,
                replication_status: ReplicationStatusType::Replica,
                replication_request: true,
                replication_validity_check: false,
            },
        )
        .await
    {
        Ok(_) => {
            if dobj.delete_object.version_id.is_none() {
                rinfo.replication_status = ReplicationStatusType::Completed;
            } else {
                rinfo.version_purge_status = VersionPurgeStatusType::Complete;
            }
        }
        Err(e) => {
            rinfo.error = Some(e.to_string());
            if dobj.delete_object.version_id.is_none() {
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
        rinfo.resync_timestamp = format!("{};{}", OffsetDateTime::now_utc().format(&Rfc3339).unwrap(), tgt_client.reset_id);
    }

    rinfo
}

pub async fn replicate_object<S: StorageAPI>(roi: ReplicateObjectInfo, storage: Arc<S>) {
    let bucket = roi.bucket.clone();
    let object = roi.name.clone();

    let cfg = match get_replication_config(&bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            warn!("No replication config found for bucket: {}", bucket);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: roi.to_object_info(),
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return;
        }
        Err(err) => {
            error!("Failed to get replication config for bucket {}: {}", bucket, err);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: roi.to_object_info(),
                host: GLOBAL_LocalNodeName.to_string(),
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
        ..Default::default()
    });

    // TODO: NSLOCK

    let mut join_set = JoinSet::new();

    for arn in tgt_arns {
        let Some(tgt_client) = BucketTargetSys::get().get_remote_target_client(&bucket, &arn).await else {
            warn!("failed to get target for bucket:{:?}, arn:{:?}", &bucket, &arn);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: roi.to_object_info(),
                host: GLOBAL_LocalNodeName.to_string(),
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
                error!("replicate_object task failed: {}", e);
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                    bucket_name: bucket.clone(),
                    object: roi.to_object_info(),
                    host: GLOBAL_LocalNodeName.to_string(),
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
            eval_metadata.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}replication-status"), s.clone());
        }
        let popts = ObjectOptions {
            version_id: roi.version_id.map(|v| v.to_string()),
            eval_metadata: Some(eval_metadata),
            ..Default::default()
        };

        if let Ok(u) = storage.put_object_metadata(&bucket, &object, &popts).await {
            object_info = u;
        }

        // TODO: update stats
    }

    let event_name = if replication_status == ReplicationStatusType::Completed {
        EventName::ObjectReplicationComplete.as_ref().to_string()
    } else {
        EventName::ObjectReplicationFailed.as_ref().to_string()
    };

    send_event(EventArgs {
        event_name,
        bucket_name: bucket.clone(),
        object: object_info,
        host: GLOBAL_LocalNodeName.to_string(),
        user_agent: "Internal: [Replication]".to_string(),
        ..Default::default()
    });

    if rinfos.replication_status() != ReplicationStatusType::Completed {
        // TODO: update stats
        // pool
    }
}

trait ReplicateObjectInfoExt {
    async fn replicate_object<S: StorageAPI>(&self, storage: Arc<S>, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo;
    async fn replicate_all<S: StorageAPI>(&self, storage: Arc<S>, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo;
    fn to_object_info(&self) -> ObjectInfo;
}

impl ReplicateObjectInfoExt for ReplicateObjectInfo {
    async fn replicate_object<S: StorageAPI>(&self, storage: Arc<S>, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo {
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

        if BucketTargetSys::get().is_offline(&tgt_client.to_url()).await {
            warn!("target is offline: {}", tgt_client.to_url());
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: self.to_object_info(),
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

        let versioned = BucketVersioningSys::prefix_enabled(&bucket, &object).await;
        let version_suspended = BucketVersioningSys::prefix_suspended(&bucket, &object).await;

        let mut gr = match storage
            .get_object_reader(
                &bucket,
                &object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    version_id: self.version_id.map(|v| v.to_string()),
                    version_suspended,
                    versioned,
                    replication_request: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(gr) => gr,
            Err(e) => {
                if !is_err_object_not_found(&e) || is_err_version_not_found(&e) {
                    warn!("failed to get object reader for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);

                    send_event(EventArgs {
                        event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                        bucket_name: bucket.clone(),
                        object: self.to_object_info(),
                        host: GLOBAL_LocalNodeName.to_string(),
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
                warn!("failed to get actual size for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                    bucket_name: bucket.clone(),
                    object: object_info,
                    host: GLOBAL_LocalNodeName.to_string(),
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                return rinfo;
            }
        };

        if tgt_client.bucket.is_empty() {
            warn!("target bucket is empty: {}", tgt_client.bucket);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: object_info,
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

        let mut replication_action = replication_action;
        match tgt_client
            .head_object(&tgt_client.bucket, &object, self.version_id.map(|v| v.to_string()))
            .await
        {
            Ok(oi) => {
                replication_action = get_replication_action(&object_info, &oi, self.op_type);
                if replication_action == ReplicationAction::None {
                    rinfo.replication_status = ReplicationStatusType::Completed;
                    rinfo.replication_resynced = true;
                    rinfo.replication_action = ReplicationAction::None;
                    rinfo.size = size;
                    return rinfo;
                }
            }
            Err(e) => {
                if let Some(se) = e.as_service_error() {
                    if !se.is_not_found() {
                        rinfo.error = Some(e.to_string());
                        warn!("replication head_object failed bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);
                        return rinfo;
                    }
                } else {
                    rinfo.error = Some(e.to_string());
                    warn!("replication head_object failed bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);
                    return rinfo;
                }
            }
        }

        rinfo.replication_status = ReplicationStatusType::Completed;
        rinfo.replication_resynced = true;
        rinfo.size = size;
        rinfo.replication_action = replication_action;

        let (put_opts, is_multipart) = match put_replication_opts(&tgt_client.storage_class, &object_info) {
            Ok((put_opts, is_mp)) => (put_opts, is_mp),
            Err(e) => {
                warn!(
                    "failed to get put replication opts for bucket:{} arn:{} error:{}",
                    bucket, tgt_client.arn, e
                );
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                    bucket_name: bucket.clone(),
                    object: object_info,
                    host: GLOBAL_LocalNodeName.to_string(),
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                return rinfo;
            }
        };

        // TODO:bandwidth

        if let Some(err) = if is_multipart {
            replicate_object_with_multipart(tgt_client.clone(), &tgt_client.bucket, &object, gr.stream, &object_info, put_opts)
                .await
                .err()
        } else {
            // TODO: use stream
            let body = match gr.read_all().await {
                Ok(body) => body,
                Err(e) => {
                    rinfo.replication_status = ReplicationStatusType::Failed;
                    rinfo.error = Some(e.to_string());
                    warn!("failed to read object for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);
                    send_event(EventArgs {
                        event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                        bucket_name: bucket.clone(),
                        object: object_info.clone(),
                        host: GLOBAL_LocalNodeName.to_string(),
                        user_agent: "Internal: [Replication]".to_string(),
                        ..Default::default()
                    });
                    return rinfo;
                }
            };
            let reader = ByteStream::from(body);
            tgt_client
                .put_object(&tgt_client.bucket, &object, size, reader, &put_opts)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
                .err()
        } {
            rinfo.replication_status = ReplicationStatusType::Failed;
            rinfo.error = Some(err.to_string());
            warn!(
                "replication put_object failed src_bucket={} dest_bucket={} object={} err={:?}",
                bucket, tgt_client.bucket, object, err
            );

            // TODO: check offline
            return rinfo;
        }

        rinfo.replication_status = ReplicationStatusType::Completed;

        rinfo
    }

    async fn replicate_all<S: StorageAPI>(&self, storage: Arc<S>, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo {
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

        if BucketTargetSys::get().is_offline(&tgt_client.to_url()).await {
            warn!("target is offline: {}", tgt_client.to_url());
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: self.to_object_info(),
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

        let versioned = BucketVersioningSys::prefix_enabled(&bucket, &object).await;
        let version_suspended = BucketVersioningSys::prefix_suspended(&bucket, &object).await;

        let mut gr = match storage
            .get_object_reader(
                &bucket,
                &object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    version_id: self.version_id.map(|v| v.to_string()),
                    version_suspended,
                    versioned,
                    replication_request: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(gr) => gr,
            Err(e) => {
                if !is_err_object_not_found(&e) || is_err_version_not_found(&e) {
                    warn!("failed to get object reader for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);
                    send_event(EventArgs {
                        event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                        bucket_name: bucket.clone(),
                        object: self.to_object_info(),
                        host: GLOBAL_LocalNodeName.to_string(),
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
                warn!("failed to get actual size for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                    bucket_name: bucket.clone(),
                    object: object_info,
                    host: GLOBAL_LocalNodeName.to_string(),
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                return rinfo;
            }
        };

        // TODO: SSE

        if tgt_client.bucket.is_empty() {
            warn!("target bucket is empty: {}", tgt_client.bucket);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                bucket_name: bucket.clone(),
                object: object_info,
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

        let sopts = StatObjectOptions {
            version_id: object_info.version_id.map(|v| v.to_string()).unwrap_or_default(),
            internal: AdvancedGetOptions {
                replication_proxy_request: "false".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        sopts.set(AMZ_TAGGING_DIRECTIVE, "ACCESS");

        match tgt_client
            .head_object(&tgt_client.bucket, &object, self.version_id.map(|v| v.to_string()))
            .await
        {
            Ok(oi) => {
                replication_action = get_replication_action(&object_info, &oi, self.op_type);
                rinfo.replication_status = ReplicationStatusType::Completed;
                if replication_action == ReplicationAction::None {
                    if self.op_type == ReplicationType::ExistingObject
                        && object_info.mod_time
                            > oi.last_modified.map(|dt| {
                                time::OffsetDateTime::from_unix_timestamp(dt.secs()).unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
                            })
                        && object_info.version_id.is_none()
                    {
                        warn!(
                            "unable to replicate {}/{} Newer version exists on target {}",
                            bucket,
                            object,
                            tgt_client.to_url()
                        );
                        send_event(EventArgs {
                            event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                            bucket_name: bucket.clone(),
                            object: object_info.clone(),
                            host: GLOBAL_LocalNodeName.to_string(),
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
                        rinfo.resync_timestamp =
                            format!("{};{}", OffsetDateTime::now_utc().format(&Rfc3339).unwrap(), tgt_client.reset_id);
                        rinfo.replication_resynced = true;
                    }

                    rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();

                    return rinfo;
                }
            }
            Err(e) => {
                if let Some(se) = e.as_service_error() {
                    if se.is_not_found() {
                        replication_action = ReplicationAction::All;
                    } else {
                        rinfo.error = Some(e.to_string());
                        warn!("failed to head object for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);

                        send_event(EventArgs {
                            event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                            bucket_name: bucket.clone(),
                            object: object_info,
                            host: GLOBAL_LocalNodeName.to_string(),
                            user_agent: "Internal: [Replication]".to_string(),
                            ..Default::default()
                        });

                        rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                        return rinfo;
                    }
                } else {
                    rinfo.error = Some(e.to_string());
                    warn!("failed to head object for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);

                    send_event(EventArgs {
                        event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                        bucket_name: bucket.clone(),
                        object: object_info,
                        host: GLOBAL_LocalNodeName.to_string(),
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
            let (put_opts, is_multipart) = match put_replication_opts(&tgt_client.storage_class, &object_info) {
                Ok((put_opts, is_mp)) => (put_opts, is_mp),
                Err(e) => {
                    rinfo.error = Some(e.to_string());
                    warn!(
                        "failed to get put replication opts for bucket:{} arn:{} error:{}",
                        bucket, tgt_client.arn, e
                    );
                    send_event(EventArgs {
                        event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                        bucket_name: bucket.clone(),
                        object: object_info,
                        host: GLOBAL_LocalNodeName.to_string(),
                        user_agent: "Internal: [Replication]".to_string(),
                        ..Default::default()
                    });

                    rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                    return rinfo;
                }
            };
            if let Some(err) = if is_multipart {
                replicate_object_with_multipart(
                    tgt_client.clone(),
                    &tgt_client.bucket,
                    &object,
                    gr.stream,
                    &object_info,
                    put_opts,
                )
                .await
                .err()
            } else {
                let body = match gr.read_all().await {
                    Ok(body) => body,
                    Err(e) => {
                        rinfo.replication_status = ReplicationStatusType::Failed;
                        rinfo.error = Some(e.to_string());
                        warn!("failed to read object for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);
                        send_event(EventArgs {
                            event_name: EventName::ObjectReplicationNotTracked.as_ref().to_string(),
                            bucket_name: bucket.clone(),
                            object: object_info,
                            host: GLOBAL_LocalNodeName.to_string(),
                            user_agent: "Internal: [Replication]".to_string(),
                            ..Default::default()
                        });
                        rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                        return rinfo;
                    }
                };
                let reader = ByteStream::from(body);
                tgt_client
                    .put_object(&tgt_client.bucket, &object, size, reader, &put_opts)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))
                    .err()
            } {
                rinfo.replication_status = ReplicationStatusType::Failed;
                rinfo.error = Some(err.to_string());
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
            user_tags: self.user_tags.clone(),
            actual_size: self.actual_size,
            replication_status_internal: self.replication_status_internal.clone(),
            replication_status: self.replication_status.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            version_purge_status: self.version_purge_status.clone(),
            delete_marker: true,
            checksum: self.checksum.clone(),
            ..Default::default()
        }
    }
}

// Standard headers that needs to be extracted from User metadata.
static STANDARD_HEADERS: &[&str] = &[
    headers::CONTENT_TYPE,
    headers::CACHE_CONTROL,
    headers::CONTENT_ENCODING,
    headers::CONTENT_LANGUAGE,
    headers::CONTENT_DISPOSITION,
    headers::AMZ_STORAGE_CLASS,
    headers::AMZ_OBJECT_TAGGING,
    headers::AMZ_BUCKET_REPLICATION_STATUS,
    headers::AMZ_OBJECT_LOCK_MODE,
    headers::AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
    headers::AMZ_OBJECT_LOCK_LEGAL_HOLD,
    headers::AMZ_TAG_COUNT,
    headers::AMZ_SERVER_SIDE_ENCRYPTION,
];

fn is_standard_header(k: &str) -> bool {
    STANDARD_HEADERS.iter().any(|h| h.eq_ignore_ascii_case(k))
}

// Valid SSE replication headers mapping from internal to replication headers
static VALID_SSE_REPLICATION_HEADERS: &[(&str, &str)] = &[
    (
        "X-Rustfs-Internal-Server-Side-Encryption-Sealed-Key",
        "X-Rustfs-Replication-Server-Side-Encryption-Sealed-Key",
    ),
    (
        "X-Rustfs-Internal-Server-Side-Encryption-Seal-Algorithm",
        "X-Rustfs-Replication-Server-Side-Encryption-Seal-Algorithm",
    ),
    (
        "X-Rustfs-Internal-Server-Side-Encryption-Iv",
        "X-Rustfs-Replication-Server-Side-Encryption-Iv",
    ),
    ("X-Rustfs-Internal-Encrypted-Multipart", "X-Rustfs-Replication-Encrypted-Multipart"),
    ("X-Rustfs-Internal-Actual-Object-Size", "X-Rustfs-Replication-Actual-Object-Size"),
];

const REPLICATION_SSEC_CHECKSUM_HEADER: &str = "X-Rustfs-Replication-Ssec-Crc";

fn is_valid_sse_header(k: &str) -> Option<&str> {
    VALID_SSE_REPLICATION_HEADERS
        .iter()
        .find(|(internal, _)| k.eq_ignore_ascii_case(internal))
        .map(|(_, replication)| *replication)
}

fn put_replication_opts(sc: &str, object_info: &ObjectInfo) -> Result<(PutObjectOptions, bool)> {
    use crate::config::storageclass::{RRS, STANDARD};
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use rustfs_utils::http::{
        AMZ_CHECKSUM_TYPE, AMZ_CHECKSUM_TYPE_FULL_OBJECT, AMZ_SERVER_SIDE_ENCRYPTION, AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID,
    };

    let mut meta = HashMap::new();
    let is_ssec = is_ssec_encrypted(&object_info.user_defined);

    // Process user-defined metadata
    for (k, v) in object_info.user_defined.iter() {
        let has_valid_sse_header = is_valid_sse_header(k).is_some();

        // In case of SSE-C objects copy the allowed internal headers as well
        if !is_ssec || !has_valid_sse_header {
            if strings_has_prefix_fold(k, RESERVED_METADATA_PREFIX) {
                continue;
            }
            if is_standard_header(k) {
                continue;
            }
        }

        if let Some(replication_header) = is_valid_sse_header(k) {
            meta.insert(replication_header.to_string(), v.to_string());
        } else {
            meta.insert(k.to_string(), v.to_string());
        }
    }

    let mut is_multipart = object_info.is_multipart();

    // Handle checksum
    if let Some(checksum_data) = &object_info.checksum
        && !checksum_data.is_empty()
    {
        // Add encrypted CRC to metadata for SSE-C objects
        if is_ssec {
            let encoded = BASE64_STANDARD.encode(checksum_data);
            meta.insert(REPLICATION_SSEC_CHECKSUM_HEADER.to_string(), encoded);
        } else {
            // Get checksum metadata for non-SSE-C objects
            let (cs_meta, is_mp) = object_info.decrypt_checksums(0, &http::HeaderMap::new())?;
            is_multipart = is_mp;

            // Set object checksum metadata
            for (k, v) in cs_meta.iter() {
                if k != AMZ_CHECKSUM_TYPE {
                    meta.insert(k.clone(), v.clone());
                }
            }

            // For objects where checksum is full object, use the cheaper PutObject replication
            if !object_info.is_multipart()
                && cs_meta
                    .get(AMZ_CHECKSUM_TYPE)
                    .map(|v| v.as_str() == AMZ_CHECKSUM_TYPE_FULL_OBJECT)
                    .unwrap_or(false)
            {
                is_multipart = false;
            }
        }
    }

    // Handle storage class default
    let storage_class = if sc.is_empty() {
        let obj_sc = object_info.storage_class.as_deref().unwrap_or_default();
        if obj_sc == STANDARD || obj_sc == RRS {
            obj_sc.to_string()
        } else {
            sc.to_string()
        }
    } else {
        sc.to_string()
    };

    let mut put_op = PutObjectOptions {
        user_metadata: meta,
        content_type: object_info.content_type.clone().unwrap_or_default(),
        content_encoding: object_info.content_encoding.clone().unwrap_or_default(),
        expires: object_info.expires.unwrap_or(OffsetDateTime::UNIX_EPOCH),
        storage_class,
        internal: AdvancedPutOptions {
            source_version_id: object_info.version_id.map(|v| v.to_string()).unwrap_or_default(),
            source_etag: object_info.etag.clone().unwrap_or_default(),
            source_mtime: object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH),
            replication_status: ReplicationStatusType::Replica, // Changed from Pending to Replica
            replication_request: true, // always set this to distinguish between replication and normal PUT operation
            ..Default::default()
        },
        ..Default::default()
    };

    if !object_info.user_tags.is_empty() {
        let tags = decode_tags_to_map(&object_info.user_tags);

        if !tags.is_empty() {
            put_op.user_tags = tags;
            // set tag timestamp in opts
            put_op.internal.tagging_timestamp = if let Some(ts) = object_info
                .user_defined
                .get(&format!("{RESERVED_METADATA_PREFIX_LOWER}tagging-timestamp"))
            {
                OffsetDateTime::parse(ts, &Rfc3339)
                    .map_err(|e| Error::other(format!("Failed to parse tagging timestamp: {}", e)))?
            } else {
                object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
            };
        }
    }

    // Use case-insensitive lookup for headers
    let lk_map = object_info.user_defined.clone();

    if let Some(lang) = lk_map.lookup(headers::CONTENT_LANGUAGE) {
        put_op.content_language = lang.to_string();
    }

    if let Some(cd) = lk_map.lookup(headers::CONTENT_DISPOSITION) {
        put_op.content_disposition = cd.to_string();
    }

    if let Some(v) = lk_map.lookup(headers::CACHE_CONTROL) {
        put_op.cache_control = v.to_string();
    }

    if let Some(v) = lk_map.lookup(headers::AMZ_OBJECT_LOCK_MODE) {
        let mode = v.to_string().to_uppercase();
        put_op.mode = Some(aws_sdk_s3::types::ObjectLockRetentionMode::from(mode.as_str()));
    }

    if let Some(v) = lk_map.lookup(headers::AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE) {
        put_op.retain_until_date =
            OffsetDateTime::parse(v, &Rfc3339).map_err(|e| Error::other(format!("Failed to parse retain until date: {}", e)))?;
        // set retention timestamp in opts
        put_op.internal.retention_timestamp = if let Some(v) = object_info
            .user_defined
            .get(&format!("{RESERVED_METADATA_PREFIX_LOWER}objectlock-retention-timestamp"))
        {
            OffsetDateTime::parse(v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH)
        } else {
            object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
        };
    }

    if let Some(v) = lk_map.lookup(headers::AMZ_OBJECT_LOCK_LEGAL_HOLD) {
        let hold = v.to_uppercase();
        put_op.legalhold = Some(ObjectLockLegalHoldStatus::from(hold.as_str()));
        // set legalhold timestamp in opts
        put_op.internal.legalhold_timestamp = if let Some(v) = object_info
            .user_defined
            .get(&format!("{RESERVED_METADATA_PREFIX_LOWER}objectlock-legalhold-timestamp"))
        {
            OffsetDateTime::parse(v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH)
        } else {
            object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
        };
    }

    // Handle SSE-S3 encryption
    if object_info
        .user_defined
        .get(AMZ_SERVER_SIDE_ENCRYPTION)
        .map(|v| v.eq_ignore_ascii_case("AES256"))
        .unwrap_or(false)
    {
        // SSE-S3 detected - set ServerSideEncryption
        // Note: This requires the PutObjectOptions to support SSE
        // TODO: Implement SSE-S3 support in PutObjectOptions if not already present
    }

    // Handle SSE-KMS encryption
    if object_info.user_defined.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID) {
        // SSE-KMS detected
        // If KMS key ID replication is enabled (as by default)
        // we include the object's KMS key ID. In any case, we
        // always set the SSE-KMS header. If no KMS key ID is
        // specified, MinIO is supposed to use whatever default
        // config applies on the site or bucket.
        // TODO: Implement SSE-KMS support with key ID replication
        // let key_id = if kms::replicate_key_id() {
        //     object_info.kms_key_id()
        // } else {
        //     None
        // };
        // TODO: Set SSE-KMS encryption in put_op
    }

    Ok((put_op, is_multipart))
}

async fn replicate_object_with_multipart(
    cli: Arc<TargetClient>,
    bucket: &str,
    object: &str,
    reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
    object_info: &ObjectInfo,
    opts: PutObjectOptions,
) -> std::io::Result<()> {
    let mut attempts = 1;
    let upload_id = loop {
        match cli.create_multipart_upload(bucket, object, &opts).await {
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

    let mut reader = reader;
    for part_info in object_info.parts.iter() {
        let mut chunk = vec![0u8; part_info.actual_size as usize];
        AsyncReadExt::read_exact(&mut *reader, &mut chunk).await?;

        let object_part = cli
            .put_object_part(
                bucket,
                object,
                &upload_id,
                part_info.number as i32,
                part_info.actual_size,
                ByteStream::from(chunk),
                &PutObjectPartOptions { ..Default::default() },
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let etag = object_part.e_tag.unwrap_or_default();

        uploaded_parts.push(
            CompletedPart::builder()
                .part_number(part_info.number as i32)
                .e_tag(etag)
                .build(),
        );
    }

    let mut user_metadata = HashMap::new();

    user_metadata.insert(
        RUSTFS_REPLICATION_ACTUAL_OBJECT_SIZE.to_string(),
        object_info
            .user_defined
            .get(&format!("{RESERVED_METADATA_PREFIX}actual-size"))
            .map(|v| v.to_string())
            .unwrap_or_default(),
    );

    cli.complete_multipart_upload(
        bucket,
        object,
        &upload_id,
        uploaded_parts,
        &PutObjectOptions {
            user_metadata,
            ..Default::default()
        },
    )
    .await
    .map_err(|e| std::io::Error::other(e.to_string()))?;

    Ok(())
}

fn get_replication_action(oi1: &ObjectInfo, oi2: &HeadObjectOutput, op_type: ReplicationType) -> ReplicationAction {
    if op_type == ReplicationType::ExistingObject
        && oi1.mod_time
            > oi2
                .last_modified
                .map(|dt| time::OffsetDateTime::from_unix_timestamp(dt.secs()).unwrap_or(time::OffsetDateTime::UNIX_EPOCH))
        && oi1.version_id.is_none()
    {
        return ReplicationAction::None;
    }

    let size = oi1.get_actual_size().unwrap_or_default();

    // Normalize ETags by removing quotes before comparison (PR #592 compatibility)
    let oi1_etag = oi1.etag.as_ref().map(|e| rustfs_utils::path::trim_etag(e));
    let oi2_etag = oi2.e_tag.as_ref().map(|e| rustfs_utils::path::trim_etag(e));

    if oi1_etag != oi2_etag
        || oi1.version_id.map(|v| v.to_string()) != oi2.version_id
        || size != oi2.content_length.unwrap_or_default()
        || oi1.delete_marker != oi2.delete_marker.unwrap_or_default()
        || oi1.mod_time
            != oi2
                .last_modified
                .map(|dt| time::OffsetDateTime::from_unix_timestamp(dt.secs()).unwrap_or(time::OffsetDateTime::UNIX_EPOCH))
    {
        return ReplicationAction::All;
    }

    if oi1.content_type != oi2.content_type {
        return ReplicationAction::Metadata;
    }

    let empty_metadata = HashMap::new();
    let metadata = oi2.metadata.as_ref().unwrap_or(&empty_metadata);

    if let Some(content_encoding) = &oi1.content_encoding {
        if let Some(enc) = metadata
            .get(CONTENT_ENCODING)
            .or_else(|| metadata.get(&CONTENT_ENCODING.to_lowercase()))
        {
            if enc != content_encoding {
                return ReplicationAction::Metadata;
            }
        } else {
            return ReplicationAction::Metadata;
        }
    }

    let oi1_tags = decode_tags_to_map(&oi1.user_tags);
    let oi2_tags = decode_tags_to_map(metadata.get(AMZ_OBJECT_TAGGING).cloned().unwrap_or_default().as_str());

    if (oi2.tag_count.unwrap_or_default() > 0 && oi1_tags != oi2_tags)
        || oi2.tag_count.unwrap_or_default() != oi1_tags.len() as i32
    {
        return ReplicationAction::Metadata;
    }

    // Compare only necessary headers
    let compare_keys = vec![
        "Expires",
        "Cache-Control",
        "Content-Language",
        "Content-Disposition",
        "X-Amz-Object-Lock-Mode",
        "X-Amz-Object-Lock-Retain-Until-Date",
        "X-Amz-Object-Lock-Legal-Hold",
        "X-Amz-Website-Redirect-Location",
        "X-Amz-Meta-",
    ];

    // compare metadata on both maps to see if meta is identical
    let mut compare_meta1 = HashMap::new();
    for (k, v) in &oi1.user_defined {
        let mut found = false;
        for prefix in &compare_keys {
            if strings_has_prefix_fold(k, prefix) {
                found = true;
                break;
            }
        }
        if found {
            compare_meta1.insert(k.to_lowercase(), v.clone());
        }
    }

    let mut compare_meta2 = HashMap::new();
    for (k, v) in metadata {
        let mut found = false;
        for prefix in &compare_keys {
            if strings_has_prefix_fold(k.to_string().as_str(), prefix) {
                found = true;
                break;
            }
        }
        if found {
            compare_meta2.insert(k.to_lowercase(), v.clone());
        }
    }

    if compare_meta1 != compare_meta2 {
        return ReplicationAction::Metadata;
    }

    ReplicationAction::None
}
