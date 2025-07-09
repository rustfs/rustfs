use crate::bucket::bucket_target_sys::BucketTargetSys;
use crate::bucket::metadata_sys;
use crate::bucket::replication::{ObjectOpts, ReplicationConfigurationExt as _, ReplicationType, StatusType};
use crate::bucket::target::BucketTargets;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::config::com::save_config;
use crate::disk::BUCKET_META_PREFIX;
use crate::error::{Error, Result};
use crate::store_api::ObjectInfo;
use crate::{StorageAPI, new_object_layer_fn};
use byteorder::ByteOrder;
use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_TAGGING};
use rustfs_utils::path::path_join_buf;
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

const REPLICATION_DIR: &str = ".replication";
const RESYNC_FILE_NAME: &str = "resync.bin";
const RESYNC_META_FORMAT: u16 = 1;
const RESYNC_META_VERSION: u16 = 1;
const RESYNC_TIME_INTERVAL: TokioDuration = TokioDuration::from_secs(60);

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

    async fn resync_bucket<S: StorageAPI>(&mut self, cancel_token: CancellationToken, api: Arc<S>, heal: bool, opts: ResyncOpts) {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                return;
            }
            _ = self.worker_rx.recv() => {}
        }

        let cfg = match get_replication_config(&opts.bucket).await {
            Ok(cfg) => cfg,
            Err(err) => {
                error!("Failed to get replication config: {}", err);
                return;
            }
        };

        let targets = match BucketTargetSys::get().list_bucket_targets(&opts.bucket).await {
            Ok(targets) => targets,
            Err(err) => {
                warn!("Failed to list bucket targets: {}", err);
                return;
            }
        };

        todo!()
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

struct ReplicationConfig {
    pub config: Option<ReplicationConfiguration>,
    pub remotes: Option<BucketTargets>,
}

impl ReplicationConfig {
    pub fn new() -> Self {
        Self {
            config: None,
            remotes: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.config.is_none()
    }

    pub fn replicate(&self, obj: &ObjectOpts) -> bool {
        self.config.as_ref().is_some_and(|config| config.replicate(obj))
    }

    pub fn resync(&self, oi: ObjectInfo, dsc: &mut ReplicateDecision, status: HashMap<String, StatusType>) -> ResyncDecision {
        if self.is_empty() {
            return ResyncDecision::default();
        }

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

        let mut opts = ObjectOpts {
            name: oi.name.clone(),
            version_id: oi.version_id,
            ..Default::default()
        };

        todo!()
    }

    fn resync_internal(
        &self,
        oi: ObjectInfo,
        dsc: &mut ReplicateDecision,
        status: HashMap<String, StatusType>,
    ) -> ResyncDecision {
        todo!()
    }
}

struct MustReplicateOptions {
    meta: HashMap<String, String>,
    status: StatusType,
    op_type: ReplicationType,
    replication_request: bool,
}

impl MustReplicateOptions {
    pub fn replication_status(&self) -> StatusType {
        if let Some(rs) = self.meta.get(AMZ_BUCKET_REPLICATION_STATUS) {
            return StatusType::from(rs.as_str());
        }
        StatusType::default()
    }

    pub fn is_existing_object_replication(&self) -> bool {
        self.op_type == ReplicationType::ExistingObjectReplication
    }

    pub fn is_metadata_replication(&self) -> bool {
        self.op_type == ReplicationType::MetadataReplication
    }
}

async fn must_replicate(bucket: &str, object: &str, mopts: MustReplicateOptions) -> ReplicateDecision {
    let Some(store) = new_object_layer_fn() else {
        return ReplicateDecision::default();
    };

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

    for arn in arns {
        BucketTargetSys::get().get_remote_target_client(&arn).await.map(|target| {
    }

    todo!()
}
