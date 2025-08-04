use crate::bucket::bucket_target_sys::BucketTargetSys;
use crate::bucket::metadata_sys;
use crate::bucket::replication::{
    ObjectOpts, REPLICATE_EXISTING, REPLICATE_EXISTING_DELETE, REPLICATION_RESET, ReplicateObjectInfo,
    ReplicationConfigurationExt as _, ReplicationState, ReplicationType, ResyncTargetDecision, StatusType,
    VersionPurgeStatusType, replication_statuses_map, target_reset_header, version_purge_statuses_map,
};
use crate::bucket::target::BucketTargets;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::config::com::save_config;
use crate::disk::BUCKET_META_PREFIX;
use crate::error::{Error, Result};
use crate::store_api::{DeletedObject, ObjectInfo, ObjectOptions, ObjectToDelete, WalkOptions};
use crate::{StorageAPI, new_object_layer_fn};
use byteorder::ByteOrder;
use futures::future::join_all;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_TAGGING, RESERVED_METADATA_PREFIX_LOWER, SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER,
    SSEC_KEY_MD5_HEADER,
};
use rustfs_utils::path::path_join_buf;
use rustfs_utils::{DEFAULT_SIP_HASH_KEY, sip_hash};
use s3s::dto::ReplicationConfiguration;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tokio::time::Duration as TokioDuration;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use super::replication_type::{ReplicateDecision, ReplicateTargetDecision, ResyncDecision};
use regex::Regex;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ResyncStatusType {
    #[default]
    NoResync,
    ResyncPending,
    ResyncCanceled,
    ResyncStarted,
    ResyncCompleted,
    ResyncFailed,
}

impl ResyncStatusType {
    pub fn is_valid(&self) -> bool {
        *self != ResyncStatusType::NoResync
    }
}

impl fmt::Display for ResyncStatusType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ResyncStatusType::ResyncStarted => "Ongoing",
            ResyncStatusType::ResyncCompleted => "Completed",
            ResyncStatusType::ResyncFailed => "Failed",
            ResyncStatusType::ResyncPending => "Pending",
            ResyncStatusType::ResyncCanceled => "Canceled",
            ResyncStatusType::NoResync => "",
        };
        write!(f, "{s}")
    }
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

pub struct ReplicationResyncer {
    pub status_map: Arc<RwLock<HashMap<String, BucketReplicationResyncStatus>>>,
    pub worker_size: usize,
    pub resync_cancel_tx: tokio::sync::mpsc::Sender<()>,
    pub resync_cancel_rx: tokio::sync::mpsc::Receiver<()>,
    pub worker_tx: tokio::sync::mpsc::Sender<()>,
    pub worker_rx: tokio::sync::mpsc::Receiver<()>,
}

impl ReplicationResyncer {
    pub async fn new() -> Self {
        let (resync_cancel_tx, resync_cancel_rx) = tokio::sync::mpsc::channel(RESYNC_WORKER_COUNT);
        let (worker_tx, worker_rx) = tokio::sync::mpsc::channel(RESYNC_WORKER_COUNT);

        for _ in 0..RESYNC_WORKER_COUNT {
            worker_tx.send(()).await.unwrap();
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



                        if let Some(last_update) = status.last_update {
                            if last_update > *last_update_times.get(bucket).unwrap_or(&OffsetDateTime::UNIX_EPOCH) {
                                update = true;
                            }
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
        if let Err(err) = self.worker_tx.send(()).await {
            error!("Failed to send worker message: {}", err);
        }
        // TODO: Metrics
    }

    async fn resync_bucket<S: StorageAPI>(
        &mut self,
        mut cancel_token: tokio::sync::broadcast::Receiver<bool>,
        storage: Arc<S>,
        heal: bool,
        opts: ResyncOpts,
    ) {
        tokio::select! {
            _ = cancel_token.recv() => {
                return;
            }
            _ = self.worker_rx.recv() => {}
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

        if !heal {
            if let Err(e) = self
                .mark_status(ResyncStatusType::ResyncStarted, opts.clone(), storage.clone())
                .await
            {
                error!("Failed to mark resync status: {}", e);
            }
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        if let Err(err) = storage
            .clone()
            .walk(cancel_token.resubscribe(), &opts.bucket, "", tx.clone(), WalkOptions::default())
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

        let mut futures = Vec::new();

        for _ in 0..RESYNC_WORKER_COUNT {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<ReplicateObjectInfo>(100);
            worker_txs.push(tx);

            let mut cancel_token = cancel_token.resubscribe();
            let target_client = target_client.clone();

            let f = tokio::spawn(async move {
                while let Some(mut roi) = rx.recv().await {
                    if cancel_token.try_recv().is_ok() {
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
                                delete_marker_version_id: dm_version_id.map(|v| v.to_string()),
                                version_id: version_id.map(|v| v.to_string()),
                                replication_state: roi.replication_state,
                                delete_marker: roi.delete_marker,
                                delete_marker_mtime: roi.mod_time,
                            },
                            bucket: roi.bucket.clone(),
                            event_type: REPLICATE_EXISTING_DELETE.to_string(),
                            op_type: ReplicationType::ExistingObject,
                            ..Default::default()
                        };
                        replicate_delete(doi, storage).await;
                    } else {
                        roi.op_type = ReplicationType::ExistingObject;
                        roi.event_type = REPLICATE_EXISTING.to_string();
                        replicate_object(roi.clone(), storage).await;
                    }

                    let st = TargetReplicationResyncStatus {
                        object: roi.name.clone(),
                        bucket: roi.bucket.clone(),
                        ..Default::default()
                    };

                    // target_client.state_object()

                    todo!()
                }
            });

            futures.push(f);
        }

        while let Some(res) = rx.recv().await {
            if let Some(err) = res.err {
                error!("Failed to get object info: {}", err);
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }

            if self.resync_cancel_rx.try_recv().is_ok() {
                self.resync_bucket_mark_status(ResyncStatusType::ResyncCanceled, opts.clone(), storage.clone())
                    .await;
                return;
            }

            if cancel_token.try_recv().is_ok() {
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

            if self.resync_cancel_rx.try_recv().is_ok() {
                self.resync_bucket_mark_status(ResyncStatusType::ResyncCanceled, opts.clone(), storage.clone())
                    .await;
                return;
            }

            if cancel_token.try_recv().is_ok() {
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
            oi.replication_status_internal = format!("{}={};", rc.role, oi.replication_status.as_str());
        }

        if !oi.replication_status.is_empty() {
            oi.replication_status_internal = format!("{}={};", rc.role, oi.replication_status.as_str());
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
                StatusType::Empty,
                ReplicationType::Heal,
                ObjectOptions::default(),
            ),
        )
        .await
    };

    let target_statuses = replication_statuses_map(&oi.replication_status_internal);
    let target_purge_statuses = version_purge_statuses_map(&oi.version_purge_status_internal);
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
        replication_state,
        op_type: ReplicationType::Heal,
        dsc,
        existing_obj_resync,
        target_statuses,
        target_purge_statuses,
        replication_timestamp: None,
        ssec: false, // TODO: add ssec support
        user_tags: oi.user_tags.clone(),
        checksum: None,
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

#[derive(Debug, Clone, Default)]
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

    pub async fn resync(&self, oi: ObjectInfo, dsc: ReplicateDecision, status: &HashMap<String, StatusType>) -> ResyncDecision {
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
                StatusType::Empty,
                ReplicationType::ExistingObject,
                ObjectOptions::default(),
            ),
        )
        .await;

        self.resync_internal(oi, dsc, status)
    }

    fn resync_internal(&self, oi: ObjectInfo, dsc: ReplicateDecision, status: &HashMap<String, StatusType>) -> ResyncDecision {
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
                    ResyncTargetDecision::resync_target(
                        &oi,
                        &target.arn,
                        &target.reset_id,
                        target.reset_before_date,
                        status.get(&decision.arn).unwrap_or(&StatusType::Empty).clone(),
                    ),
                );
            }
        }

        resync_decision
    }
}

pub struct MustReplicateOptions {
    meta: HashMap<String, String>,
    status: StatusType,
    op_type: ReplicationType,
    replication_request: bool,
}

impl MustReplicateOptions {
    pub fn new(
        meta: &HashMap<String, String>,
        user_tags: String,
        status: StatusType,
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

    pub fn replication_status(&self) -> StatusType {
        if let Some(rs) = self.meta.get(AMZ_BUCKET_REPLICATION_STATUS) {
            return StatusType::from(rs.as_str());
        }
        StatusType::default()
    }

    pub fn is_existing_object_replication(&self) -> bool {
        self.op_type == ReplicationType::ExistingObject
    }

    pub fn is_metadata_replication(&self) -> bool {
        self.op_type == ReplicationType::Metadata
    }
}

/// Returns whether object version is a delete marker and if object qualifies for replication
pub async fn check_replicate_delete(
    bucket: &str,
    dobj: &ObjectToDelete,
    oi: &ObjectInfo,
    del_opts: &ObjectOptions,
    gerr: Option<&Error>,
) -> ReplicateDecision {
    let rcfg = match get_replication_config(bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            warn!("No replication config found for bucket: {}", bucket);
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
        if let Some(_gerr) = gerr {
            let valid_repl_status = matches!(
                oi.target_replication_status(&tgt_arn),
                StatusType::Pending | StatusType::Completed | StatusType::Failed
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
    fn target_replication_status(&self, arn: &str) -> StatusType;
    fn replication_state(&self) -> ReplicationState;
}

impl ObjectInfoExt for ObjectInfo {
    /// Returns replication status of a target
    fn target_replication_status(&self, arn: &str) -> StatusType {
        lazy_static::lazy_static! {
            static ref REPL_STATUS_REGEX: Regex = Regex::new(r"([^=].*?)=([^,].*?);").unwrap();
        }

        let captures = REPL_STATUS_REGEX.captures_iter(&self.replication_status_internal);
        for cap in captures {
            if cap.len() == 3 && &cap[1] == arn {
                return StatusType::from(&cap[2]);
            }
        }
        StatusType::default()
    }

    fn replication_state(&self) -> ReplicationState {
        ReplicationState {
            replication_status_internal: self.replication_status_internal.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            replicate_decision_str: self.replication_decision.clone(),
            targets: replication_statuses_map(&self.replication_status_internal),
            purge_targets: version_purge_statuses_map(&self.version_purge_status_internal),
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

    if replication_status == StatusType::Replica && !mopts.is_metadata_replication() {
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
        Err(err) => {
            error!("Failed to get replication config: {}", err);
            return ReplicateDecision::default();
        }
    };

    let opts = ObjectOpts {
        name: object.to_string(),
        replica: replication_status == StatusType::Replica,
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

async fn replicate_delete<S: StorageAPI>(doi: DeletedObjectReplicationInfo, storage: Arc<S>) {
    todo!()
}

async fn replicate_object<S: StorageAPI>(roi: ReplicateObjectInfo, storage: Arc<S>) {
    todo!()
}
