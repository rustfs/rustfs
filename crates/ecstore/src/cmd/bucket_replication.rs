#![allow(unused_variables)]
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
#![allow(dead_code)]
// use error::Error;
use crate::StorageAPI;
use crate::bucket::metadata_sys::get_replication_config;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::error::Error;
use crate::new_object_layer_fn;
use crate::rpc::RemotePeerS3Client;
use crate::store;
use crate::store_api::ObjectIO;
use crate::store_api::ObjectInfo;
use crate::store_api::ObjectOptions;
use crate::store_api::ObjectToDelete;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::Config;
use aws_sdk_s3::config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::config::Region;
use bytes::Bytes;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use http::HeaderMap;
use http::Method;
use lazy_static::lazy_static;
// use std::time::SystemTime;
use once_cell::sync::Lazy;
use regex::Regex;
use rustfs_rsc::Minio;
use rustfs_rsc::provider::StaticProvider;
use s3s::dto::DeleteMarkerReplicationStatus;
use s3s::dto::DeleteReplicationStatus;
use s3s::dto::ExistingObjectReplicationStatus;
use s3s::dto::ReplicaModificationsStatus;
use s3s::dto::ReplicationRuleStatus;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::iter::Iterator;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::vec;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;
// use bucket_targets::{self, GLOBAL_Bucket_Target_Sys};
use crate::bucket::lifecycle::bucket_lifecycle_ops::TransitionedObject;

#[derive(Serialize, Deserialize, Debug)]
struct MRFReplicateEntry {
    #[serde(rename = "bucket")]
    bucket: String,

    #[serde(rename = "object")]
    object: String,

    #[serde(skip_serializing, skip_deserializing)]
    version_id: String,

    #[serde(rename = "retryCount")]
    retry_count: i32,

    #[serde(skip_serializing, skip_deserializing)]
    sz: i64,
}

trait ReplicationWorkerOperation: Any + Send + Sync {
    fn to_mrf_entry(&self) -> MRFReplicateEntry;
    fn as_any(&self) -> &dyn Any;
}

// WorkerMaxLimit max number of workers per node for "fast" mode
pub const WORKER_MAX_LIMIT: usize = 50;

// WorkerMinLimit min number of workers per node for "slow" mode
pub const WORKER_MIN_LIMIT: usize = 5;

// WorkerAutoDefault is default number of workers for "auto" mode
pub const WORKER_AUTO_DEFAULT: usize = 10;

// MRFWorkerMaxLimit max number of mrf workers per node for "fast" mode
pub const MRF_WORKER_MAX_LIMIT: usize = 8;

// MRFWorkerMinLimit min number of mrf workers per node for "slow" mode
pub const MRF_WORKER_MIN_LIMIT: usize = 2;

// MRFWorkerAutoDefault is default number of mrf workers for "auto" mode
pub const MRF_WORKER_AUTO_DEFAULT: usize = 4;

// LargeWorkerCount is default number of workers assigned to large uploads ( >= 128MiB)
pub const LARGE_WORKER_COUNT: usize = 2;

pub const MIN_LARGE_OBJSIZE: u64 = 128 * 1024 * 1024;

pub struct ReplicationPool {
    // Atomic operations
    active_workers: Arc<AtomicI32>,
    active_lrg_workers: Arc<AtomicI32>,
    active_mrf_workers: Arc<AtomicI32>,

    // Shared objects
    obj_layer: Arc<store::ECStore>,
    //ctx: Arc<std::sync::Mutex<()>>, // Placeholder for context; replace as needed
    priority: String,
    max_workers: usize,
    max_lworkers: usize,
    //stats: Option<Arc<ReplicationStats>>,

    // Synchronization primitives
    //mu: RwLock<()>,
    //mrf_mu: Mutex<()>,
    //resyncer: Option<Arc<ReplicationResyncer>>,

    // Workers
    workers_sender: Vec<Sender<Box<dyn ReplicationWorkerOperation>>>,
    workers_recever: Vec<Receiver<Box<dyn ReplicationWorkerOperation>>>,
    lrg_workers_sender: Vec<Sender<Box<dyn ReplicationWorkerOperation>>>,
    lrg_workers_receiver: Vec<Receiver<Box<dyn ReplicationWorkerOperation>>>,

    // MRF
    //mrf_worker_kill_ch: Option<Sender<()>>,
    mrf_replica_ch_sender: Sender<Box<dyn ReplicationWorkerOperation>>,
    mrf_replica_ch_receiver: Receiver<Box<dyn ReplicationWorkerOperation>>,
    //mrf_save_ch: Sender<MRFReplicateEntry>,
    //mrf_stop_ch: Sender<()>,
    mrf_worker_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)] // 明确表示底层值为 u8
pub enum ReplicationType {
    #[default]
    UnsetReplicationType = 0,
    ObjectReplicationType = 1,
    DeleteReplicationType = 2,
    MetadataReplicationType = 3,
    HealReplicationType = 4,
    ExistingObjectReplicationType = 5,
    ResyncReplicationType = 6,
    AllReplicationType = 7,
}

impl ReplicationType {
    /// 从 u8 转换为枚举
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::UnsetReplicationType),
            1 => Some(Self::ObjectReplicationType),
            2 => Some(Self::DeleteReplicationType),
            3 => Some(Self::MetadataReplicationType),
            4 => Some(Self::HealReplicationType),
            5 => Some(Self::ExistingObjectReplicationType),
            6 => Some(Self::ResyncReplicationType),
            7 => Some(Self::AllReplicationType),
            _ => None,
        }
    }

    /// 获取枚举对应的 u8 值
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn is_data_replication(self) -> bool {
        matches!(
            self,
            ReplicationType::ObjectReplicationType
                | ReplicationType::HealReplicationType
                | ReplicationType::ExistingObjectReplicationType
        )
    }
}

const SYSTEM_XML_OBJECT: &str = ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml";
const CAPACITY_XML_OBJECT: &str = ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml";
const VEEAM_AGENT_SUBSTR: &str = "APN/1.0 Veeam/1.0";

fn is_veeam_sos_api_object(object: &str) -> bool {
    matches!(object, SYSTEM_XML_OBJECT | CAPACITY_XML_OBJECT)
}

pub async fn queue_replication_heal(
    bucket: &str,
    oi: &ObjectInfo,
    rcfg: &s3s::dto::ReplicationConfiguration,
    _retry_count: u32,
) -> Option<ReplicateObjectInfo> {
    if oi.mod_time.is_none() || is_veeam_sos_api_object(&oi.name) {
        return None;
    }

    if rcfg.rules.is_empty() {
        return None;
    }

    let mut moi = oi.clone();

    let mut roi = get_heal_replicate_object_info(&mut moi, rcfg).await;
    //roi.retry_count = retry_count;

    if !roi.dsc.replicate_any() {
        error!("Replication heal for object {} in bucket {} is not configured", oi.name, bucket);
        return None;
    }

    if oi.replication_status == ReplicationStatusType::Completed && !roi.existing_obj_resync.must_resync() {
        return None;
    }

    // Handle Delete Marker or VersionPurgeStatus cases
    if roi.delete_marker || !roi.version_purge_status.is_empty() {
        let (version_id, dm_version_id) = if roi.version_purge_status.is_empty() {
            (String::new(), roi.version_id.clone())
        } else {
            (roi.version_id.clone(), String::new())
        };

        let dv = DeletedObjectReplicationInfo {
            deleted_object: DeletedObject {
                object_name: Some(roi.name.clone()),
                delete_marker_version_id: Some(dm_version_id),
                version_id: Some(roi.version_id.clone()),
                replication_state: roi.replication_state.clone(),
                delete_marker_mtime: roi.mod_time,
                delete_marker: Some(roi.delete_marker),
            },
            bucket: roi.bucket.clone(),
            op_type: ReplicationType::HealReplicationType,
            //event_type: ReplicationType::HealDeleteType,
            event_type: "".to_string(),
            reset_id: "".to_string(),
            target_arn: "".to_string(),
        };

        if matches!(roi.replication_status, ReplicationStatusType::Pending | ReplicationStatusType::Failed)
            || matches!(roi.version_purge_status, VersionPurgeStatusType::Failed | VersionPurgeStatusType::Pending)
        {
            let mut pool = GLOBAL_REPLICATION_POOL.write().await;
            pool.as_mut().unwrap().queue_replica_task(roi).await;
            //GLOBAL_REPLICATION_POOL().queue_replica_delete_task(dv);
            return None;
        }

        if roi.existing_obj_resync.must_resync()
            && (roi.replication_status == ReplicationStatusType::Completed || roi.replication_status.is_empty())
        {
            //queue_replicate_deletes_wrapper(dv, &roi.existing_obj_resync);
            let mut pool = GLOBAL_REPLICATION_POOL.write().await;
            pool.as_mut().unwrap().queue_replica_task(roi).await;
            return None;
        }

        return None;
    }

    if roi.existing_obj_resync.must_resync() {
        roi.op_type = ReplicationType::ExistingObjectReplicationType as i32;
    }

    let mut pool = GLOBAL_REPLICATION_POOL.write().await;

    match roi.replication_status {
        ReplicationStatusType::Pending | ReplicationStatusType::Failed => {
            //roi.event_type = ReplicateEventType::Heal;
            //roi.event_type = ReplicateEventType::Heal;
            pool.as_mut().unwrap().queue_replica_task(roi.clone()).await;
            return Some(roi);
        }
        _ => {}
    }

    if roi.existing_obj_resync.must_resync() {
        //roi.event_type = ReplicateEventType::Existing;
        pool.as_mut().unwrap().queue_replica_task(roi.clone()).await;
    }

    Some(roi)
}

fn new_replicate_target_decision(arn: String, replicate: bool, sync: bool) -> ReplicateTargetDecision {
    ReplicateTargetDecision {
        id: String::new(), // Using a default value for the 'id' field is acceptable
        replicate,
        synchronous: sync,
        arn,
    }
}

pub async fn check_replicate_delete(
    bucket: &str,
    dobj: &ObjectToDelete,
    oi: &ObjectInfo,
    del_opts: &ObjectOptions,
    gerr: Option<&Error>,
) -> ReplicateDecision {
    error!("check_replicate_delete");
    let mut dsc = ReplicateDecision::default();

    let rcfg = match get_replication_config(bucket).await {
        Ok((cfg, mod_time)) => cfg,
        Err(e) => {
            //repl_log_once_if(ctx, None, bucket); // 你需要实现这个日志函数
            error!("get replication config err:");
            return dsc;
        }
    };

    if del_opts.replication_request {
        return dsc;
    }

    if !del_opts.versioned {
        return dsc;
    }

    let mut opts = ReplicationObjectOpts {
        name: dobj.object_name.clone(),
        ssec: false,
        user_tags: Some(oi.user_tags.clone()),
        delete_marker: oi.delete_marker,
        //version_id: dobj.version_id.clone().map(|v| v.to_string()),
        version_id: oi.version_id.map(|uuid| uuid.to_string()).unwrap_or_default(),
        op_type: ReplicationType::DeleteReplicationType,
        target_arn: None,
        replica: true,
        existing_object: true,
    };

    let tgt_arns = rcfg.filter_target_arns(&opts);
    dsc.targets_map = HashMap::with_capacity(tgt_arns.len());

    if tgt_arns.is_empty() {
        return dsc;
    }

    let sync = false;
    let mut replicate;

    for tgt_arn in tgt_arns {
        //let mut opts = opts.clone();
        opts.target_arn = Some(tgt_arn.clone());
        replicate = rcfg.replicate(&opts);

        if gerr.is_some() {
            let valid_repl_status = matches!(
                oi.target_replication_status(tgt_arn.clone()),
                ReplicationStatusType::Pending | ReplicationStatusType::Completed | ReplicationStatusType::Failed
            );

            if oi.delete_marker && (valid_repl_status || replicate) {
                dsc.set(new_replicate_target_decision(tgt_arn.clone(), replicate, sync));
                continue;
            }

            if !oi.version_purge_status.is_empty() {
                replicate = matches!(oi.version_purge_status, VersionPurgeStatusType::Pending | VersionPurgeStatusType::Failed);
                dsc.set(new_replicate_target_decision(tgt_arn.clone(), replicate, sync));
                continue;
            }
        }

        let tgt = bucket_targets::get_bucket_target_client(bucket, &tgt_arn).await;

        let tgt_dsc = match tgt {
            Ok(tgt) => new_replicate_target_decision(tgt_arn.clone(), replicate, tgt.replicate_sync),
            Err(_) => new_replicate_target_decision(tgt_arn.clone(), false, false),
        };

        // let tgt_dsc = if let Some(tgt) = tgt {
        //     new_replicate_target_decision(tgt_arn.clone(), replicate, tgt.replicate_sync)
        // } else {
        //     new_replicate_target_decision(tgt_arn.clone(), false, false)
        // };

        dsc.set(tgt_dsc);
    }

    dsc
}
// use crate::replication::*;
// use crate::crypto;
// use crate::global::*;

fn target_reset_header(arn: &str) -> String {
    format!("{RESERVED_METADATA_PREFIX_LOWER}{REPLICATION_RESET}-{arn}")
}

pub async fn get_heal_replicate_object_info(
    oi: &mut ObjectInfo,
    rcfg: &s3s::dto::ReplicationConfiguration,
) -> ReplicateObjectInfo {
    let mut user_defined = oi.user_defined.clone();

    if !rcfg.rules.is_empty() {
        if !oi.replication_status.is_empty() {
            oi.replication_status_internal = format!("{}={};", rcfg.role, oi.replication_status.as_str());
        }

        if !oi.version_purge_status.is_empty() {
            oi.version_purge_status_internal = format!("{}={};", rcfg.role, oi.version_purge_status);
        }

        // let to_replace: Vec<(String, String)> = user_defined
        //     .iter()
        //     .filter(|(k, _)| k.eq_ignore_ascii_case(&(RESERVED_METADATA_PREFIX_LOWER.to_owned() + REPLICATION_RESET)))
        //     .map(|(k, v)| (k.clone(), v.clone()))
        //     .collect::<HashMap<String, String>>()
        //     .collect();
        let to_replace: Vec<(String, String)> = user_defined
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(&(RESERVED_METADATA_PREFIX_LOWER.to_owned() + REPLICATION_RESET)))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // 第二步：apply 修改
        for (k, v) in to_replace {
            user_defined.remove(&k);
            user_defined.insert(target_reset_header(&rcfg.role), v);
        }
    }
    //}

    //let dsc = if oi.delete_marker || !oi.version_purge_status.is_empty() {
    let dsc = if oi.delete_marker {
        check_replicate_delete(
            &oi.bucket,
            &ObjectToDelete {
                object_name: oi.name.clone(),
                version_id: oi.version_id,
            },
            oi,
            &ObjectOptions {
                // versioned: global_bucket_versioning_sys::prefix_enabled(&oi.bucket, &oi.name),
                // version_suspended: global_bucket_versioning_sys::prefix_suspended(&oi.bucket, &oi.name),
                versioned: true,
                version_suspended: false,
                ..Default::default()
            },
            None,
        )
        .await
    } else {
        // let opts: ObjectOptions = put_opts(&bucket, &key, version_id, &req.headers, Some(mt))
        // .await
        // .map_err(to_s3_error)?;
        let mt = oi.user_defined.clone();
        let mt2 = oi.user_defined.clone();
        let opts = ObjectOptions {
            user_defined: user_defined.clone(),
            versioned: true,
            version_id: oi.version_id.map(|uuid| uuid.to_string()),
            mod_time: oi.mod_time,
            ..Default::default()
        };
        let repoptions =
            get_must_replicate_options(&mt2, "", ReplicationStatusType::Unknown, ReplicationType::ObjectReplicationType, &opts);

        let decision = must_replicate(&oi.bucket, &oi.name, &repoptions).await;
        error!("decision:");
        decision
    };

    let tgt_statuses = replication_statuses_map(&oi.replication_status_internal);
    let purge_statuses = version_purge_statuses_map(&oi.version_purge_status_internal);
    //let existing_obj_resync = rcfg.resync(&GLOBAL_CONTEXT, oi, &dsc, &tgt_statuses);

    // let tm = user_defined
    //     .get(&(RESERVED_METADATA_PREFIX_LOWER.to_owned() + REPLICATION_TIMESTAMP))
    //     .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
    //     .map(|dt| dt.with_timezone(&Utc));

    let tm = user_defined
        .get(&(RESERVED_METADATA_PREFIX_LOWER.to_owned() + REPLICATION_TIMESTAMP))
        .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
        .map(|dt| dt.with_timezone(&Utc));

    let mut rstate = oi.replication_state();
    rstate.replicate_decision_str = dsc.to_string();

    let asz = oi.get_actual_size().unwrap_or(0);

    let key = format!("{RESERVED_METADATA_PREFIX_LOWER}{REPLICATION_TIMESTAMP}");
    let tm: Option<DateTime<Utc>> = user_defined
        .get(&key)
        .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
        .map(|dt| dt.with_timezone(&Utc));

    let mut result = ReplicateObjectInfo {
        name: oi.name.clone(),
        size: oi.size,
        actual_size: asz,
        bucket: oi.bucket.clone(),
        //version_id: oi.version_id.clone(),
        version_id: oi
            .version_id
            .map(|uuid| uuid.to_string()) // 将 Uuid 转换为 String
            .unwrap_or_default(),
        etag: oi.etag.clone().unwrap(),
        mod_time: convert_offsetdatetime_to_chrono(oi.mod_time).unwrap(),
        replication_status: oi.replication_status.clone(),
        replication_status_internal: oi.replication_status_internal.clone(),
        delete_marker: oi.delete_marker,
        version_purge_status_internal: oi.version_purge_status_internal.clone(),
        version_purge_status: oi.version_purge_status.clone(),
        replication_state: rstate,
        op_type: 1,
        dsc,
        existing_obj_resync: Default::default(),
        target_statuses: tgt_statuses,
        target_purge_statuses: purge_statuses,
        replication_timestamp: tm.unwrap_or_else(Utc::now),
        //ssec: crypto::is_encrypted(&oi.user_defined),
        ssec: false,
        user_tags: oi.user_tags.clone(),
        checksum: oi.checksum.clone(),
        event_type: "".to_string(),
        retry_count: 0,
        reset_id: "".to_string(),
        target_arn: "".to_string(),
    };

    if result.ssec {
        result.checksum = oi.checksum.clone();
    }

    warn!(
        "Replication heal for object {} in bucket {} is configured {:?}",
        oi.name, oi.bucket, oi.version_id
    );

    result
}

#[derive(Debug, Clone)]
pub struct MustReplicateOptions {
    pub meta: HashMap<String, String>,
    pub status: ReplicationStatusType,
    pub op_type: ReplicationType,
    pub replication_request: bool, // Incoming request is a replication request
}

impl MustReplicateOptions {
    /// Get the replication status from metadata, if available.
    pub fn replication_status(&self) -> ReplicationStatusType {
        if let Some(rs) = self.meta.get("x-amz-bucket-replication-status") {
            return match rs.as_str() {
                "Pending" => ReplicationStatusType::Pending,
                "Completed" => ReplicationStatusType::Completed,
                "CompletedLegacy" => ReplicationStatusType::CompletedLegacy,
                "Failed" => ReplicationStatusType::Failed,
                "Replica" => ReplicationStatusType::Replica,
                _ => ReplicationStatusType::Unknown,
            };
        }
        self.status.clone()
    }

    /// Check if the operation type is existing object replication.
    pub fn is_existing_object_replication(&self) -> bool {
        self.op_type == ReplicationType::ExistingObjectReplicationType
    }

    /// Check if the operation type is metadata replication.
    pub fn is_metadata_replication(&self) -> bool {
        self.op_type == ReplicationType::MetadataReplicationType
    }
}

use tokio::sync::mpsc;

use crate::cmd::bucket_targets;

// use super::bucket_targets::Client;
use super::bucket_targets::TargetClient;
//use crate::storage;

// 模拟依赖的类型
pub struct Context; // 用于代替 Go 的 `context.Context`
#[derive(Default)]
pub struct ReplicationStats;

#[derive(Default)]
pub struct ReplicationPoolOpts {
    pub priority: String,
    pub max_workers: usize,
    pub max_l_workers: usize,
}

//pub static GLOBAL_REPLICATION_POOL: OnceLock<RwLock<ReplicationPool>> = OnceLock::new();

pub static GLOBAL_REPLICATION_POOL: Lazy<RwLock<Option<ReplicationPool>>> = Lazy::new(|| {
    RwLock::new(None) // 允许延迟初始化
});

impl ReplicationPool {
    pub async fn init_bucket_replication_pool(
        obj_layer: Arc<store::ECStore>,
        opts: ReplicationPoolOpts,
        stats: Arc<ReplicationStats>,
    ) {
        let mut workers = 0;
        let mut failed_workers = 0;
        let mut priority = "auto".to_string();
        let mut max_workers = WORKER_MAX_LIMIT;
        warn!("init_bucket_replication_pool {} {} {} {}", workers, failed_workers, priority, max_workers);

        let (sender, receiver) = mpsc::channel::<Box<dyn ReplicationWorkerOperation>>(10);

        // Self {
        //     mrf_replica_ch_sender: sender,
        // }

        if !opts.priority.is_empty() {
            priority = opts.priority.clone();
        }
        if opts.max_workers > 0 {
            max_workers = opts.max_workers;
        }

        match priority.as_str() {
            "fast" => {
                workers = WORKER_MAX_LIMIT;
                failed_workers = MRF_WORKER_MAX_LIMIT;
            }
            "slow" => {
                workers = WORKER_MIN_LIMIT;
                failed_workers = MRF_WORKER_MIN_LIMIT;
            }
            _ => {
                workers = WORKER_AUTO_DEFAULT;
                failed_workers = MRF_WORKER_AUTO_DEFAULT;
            }
        }

        if max_workers > 0 && workers > max_workers {
            workers = max_workers;
        }
        if max_workers > 0 && failed_workers > max_workers {
            failed_workers = max_workers;
        }

        let max_l_workers = if opts.max_l_workers > 0 {
            opts.max_l_workers
        } else {
            LARGE_WORKER_COUNT
        };

        // 初始化通道
        let (mrf_replica_tx, _) = mpsc::channel::<u32>(100_000);
        let (mrf_worker_kill_tx, _) = mpsc::channel::<u32>(failed_workers);
        let (mrf_save_tx, _) = mpsc::channel::<u32>(100_000);
        let (mrf_stop_tx, _) = mpsc::channel::<u32>(1);

        let mut pool = Self {
            workers_sender: Vec::with_capacity(workers),
            workers_recever: Vec::with_capacity(workers),
            lrg_workers_sender: Vec::with_capacity(max_l_workers),
            lrg_workers_receiver: Vec::with_capacity(max_l_workers),
            active_workers: Arc::new(AtomicI32::new(0)),
            active_lrg_workers: Arc::new(AtomicI32::new(0)),
            active_mrf_workers: Arc::new(AtomicI32::new(0)),
            max_lworkers: max_l_workers,
            //mrf_worker_kill_ch: None,
            mrf_replica_ch_sender: sender,
            mrf_replica_ch_receiver: receiver,
            mrf_worker_size: workers,
            priority,
            max_workers,
            obj_layer,
        };

        warn!("work size is: {}", workers);
        pool.resize_lrg_workers(max_l_workers, Some(0)).await;
        pool.resize_workers(workers, Some(0)).await;
        pool.resize_failed_workers(failed_workers).await;
        let obj_layer_clone = pool.obj_layer.clone();

        // 启动后台任务
        let resyncer = Arc::new(RwLock::new(ReplicationResyncer::new()));
        let x = Arc::new(RwLock::new(&pool));
        // tokio::spawn(async move {
        //     resyncer.lock().await.persist_to_disk(ctx_clone, obj_layer_clone).await;
        // });

        tokio::spawn(async move {
            //pool4.process_mrf().await
        });
        let pool5 = Arc::clone(&x);
        tokio::spawn(async move {
            //pool5.persist_mrf().await
        });

        let mut global_pool = GLOBAL_REPLICATION_POOL.write().await;
        global_pool.replace(pool);
    }

    pub async fn resize_lrg_workers(&mut self, n: usize, check_old: Option<usize>) {
        //let mut lrg_workers = self.lrg_workers.lock().unwrap();
        if (check_old.is_some() && self.lrg_workers_sender.len() != check_old.unwrap())
            || n == self.lrg_workers_sender.len()
            || n < 1
        {
            // Either already satisfied or worker count changed while waiting for the lock.
            return;
        }
        debug!("Resizing large workers pool");

        let active_workers = Arc::clone(&self.active_lrg_workers);
        let obj_layer = Arc::clone(&self.obj_layer);
        let mut lrg_workers_sender = std::mem::take(&mut self.lrg_workers_sender);

        while lrg_workers_sender.len() < n {
            let (sender, mut receiver) = mpsc::channel::<Box<dyn ReplicationWorkerOperation>>(100);
            lrg_workers_sender.push(sender);

            let active_workers_clone = Arc::clone(&active_workers);
            let obj_layer_clone = Arc::clone(&obj_layer);

            tokio::spawn(async move {
                while let Some(operation) = receiver.recv().await {
                    debug!("Processing replication operation in worker");
                    active_workers_clone.fetch_add(1, Ordering::SeqCst);

                    if let Some(info) = operation.as_any().downcast_ref::<ReplicateObjectInfo>() {
                        replicate_object(info.clone(), obj_layer_clone.clone()).await;
                    } else if let Some(info) = operation.as_any().downcast_ref::<DeletedObjectReplicationInfo>() {
                        replicate_delete(&info.clone(), obj_layer_clone.clone()).await;
                    } else {
                        eprintln!("Unknown replication type");
                    }

                    active_workers_clone.fetch_sub(1, Ordering::SeqCst);
                }
            });
        }

        // Add new workers if needed
        // Remove excess workers if needed
        while lrg_workers_sender.len() > n {
            lrg_workers_sender.pop(); // Dropping the sender will close the channel
        }

        self.lrg_workers_sender = lrg_workers_sender;
    }

    pub async fn resize_workers(&mut self, n: usize, check_old: Option<usize>) {
        debug!("resize worker");
        //let mut lrg_workers = self.lrg_workers.lock().unwrap();
        if (check_old.is_some() && self.workers_sender.len() != check_old.unwrap()) || n == self.workers_sender.len() || n < 1 {
            // Either already satisfied or worker count changed while waiting for the lock.
            return;
        }
        debug!("resize worker");
        // Add new workers if needed
        let active_workers_clone = Arc::clone(&self.active_workers);
        let mut vsender = std::mem::take(&mut self.workers_sender);
        //let mut works_sender = std::mem::take(&mut self.workers_sender);
        let layer = Arc::clone(&self.obj_layer);
        while vsender.len() < n {
            debug!("resize workers");
            let (sender, mut receiver) = mpsc::channel::<Box<dyn ReplicationWorkerOperation>>(100);
            vsender.push(sender);

            let active_workers_clone = Arc::clone(&active_workers_clone);
            // Spawn a new workero
            let layer_clone = Arc::clone(&layer);
            tokio::spawn(async move {
                while let Some(operation) = receiver.recv().await {
                    // Simulate work being processed
                    active_workers_clone.fetch_add(1, Ordering::SeqCst);

                    if let Some(info) = operation.as_any().downcast_ref::<ReplicateObjectInfo>() {
                        //self.stats.inc_q(&info.bucket, info.size, info.delete_marker, &info.op_type);
                        let _layer = Arc::clone(&layer_clone);
                        replicate_object(info.clone(), _layer).await;
                        //self.stats.dec_q(&info.bucket, info.size, info.delete_marker, &info.op_type);
                    } else if let Some(info) = operation.as_any().downcast_ref::<DeletedObjectReplicationInfo>() {
                        let _layer = Arc::clone(&layer_clone);
                        replicate_delete(&info.clone(), _layer).await;
                    } else {
                        eprintln!("Unknown replication type");
                    }

                    active_workers_clone.fetch_sub(1, Ordering::SeqCst);
                }
            });
        }
        // Remove excess workers if needed
        while vsender.len() > n {
            vsender.pop(); // Dropping the sender will close the channel
        }
        self.workers_sender = vsender;
        // warn!("self sender size is {:?}", self.workers_sender.len());
        // warn!("self sender size is {:?}", self.workers_sender.len());
    }

    async fn resize_failed_workers(&self, _count: usize) {
        // 实现失败 worker 的初始化逻辑
    }

    // async fn process_mrf(&self) {
    //     // 实现 MRF 处理逻辑
    // }

    // async fn persist_mrf(&self) {
    //     // 实现 MRF 持久化逻辑
    // }

    fn get_worker_ch(&self, bucket: &str, object: &str, _sz: i64) -> Option<&Sender<Box<dyn ReplicationWorkerOperation>>> {
        let h = xxh3_64(format!("{bucket}{object}").as_bytes()); // 计算哈希值

        // need lock;
        let workers = &self.workers_sender; // 读锁

        if workers.is_empty() {
            warn!("workers is empty");
            return None;
        }

        let index = (h as usize) % workers.len(); // 选择 worker
        Some(&workers[index]) // 返回对应的 Sender
    }

    async fn queue_replica_task(&mut self, ri: ReplicateObjectInfo) {
        if ri.size >= MIN_LARGE_OBJSIZE as i64 {
            let h = xxh3_64(format!("{}{}", ri.bucket, ri.name).as_bytes());
            let workers = &self.lrg_workers_sender;
            let worker_count = workers.len();

            if worker_count > 0 {
                let worker_index = (h as usize) % worker_count;
                let sender = &workers[worker_index];

                match sender.try_send(Box::new(ri.clone())) {
                    Ok(_) => return,
                    Err(_) => {
                        // 任务队列满了，执行 MRF 处理
                        //println!("Queue full, saving to MRF: {}", ri.to_mrf_entry());
                        println!("Queue full, saving to MRF");
                    }
                }
            }

            // 检查是否需要增加 worker
            let existing = worker_count;
            let max_workers = self.max_lworkers.min(LARGE_WORKER_COUNT);

            if self.active_lrg_workers.load(Ordering::SeqCst) < max_workers as i32 {
                let new_worker_count = (existing + 1).min(max_workers);
                self.resize_lrg_workers(new_worker_count, Some(existing)).await;
            }
            return;
        }
        let mut ch: Option<&Sender<Box<dyn ReplicationWorkerOperation>>> = None;
        let mut heal_ch: Option<&Sender<Box<dyn ReplicationWorkerOperation>>> = None;
        warn!("enqueue object:{}", ch.is_none());

        if ri.op_type == ReplicationType::HealReplicationType as i32
            || ri.op_type == ReplicationType::ExistingObjectReplicationType as i32
        {
            ch = Some(&self.mrf_replica_ch_sender);
            heal_ch = self.get_worker_ch(&ri.name, &ri.bucket, ri.size);
        } else {
            info!("get worker channel for replication");
            ch = self.get_worker_ch(&ri.name, &ri.bucket, ri.size);
        }

        if ch.is_none() && heal_ch.is_none() {
            error!("replicste chan empty");
            return;
        }

        let mut sent = false;
        tokio::select! {
            //_ = self.ctx_done.closed() => {},
            Some(h) = async { heal_ch } => {
                //if let Some(h) = h {
                    if h.send(Box::new(ri.clone())).await.is_ok() {
                        warn!("enqueue object");
                        sent = true;
                    }
                //}
            }
            Some(c) = async { ch } => {
                //if let Some(c) = c {
                    if c.send(Box::new(ri.clone())).await.is_ok() {
                        info!("enqueue object");
                        sent = true;
                    }
                //}
            }
        }

        if !sent {
            //todo!
            //self.queue_mrf_save(ri).await;
            let max_workers = self.max_workers;

            match self.priority.as_str() {
                "fast" => {
                    println!("Warning: Unable to keep up with incoming traffic");
                }
                "slow" => {
                    println!("Warning: Incoming traffic is too high. Increase replication priority.");
                }
                _ => {
                    let worker_count = self.active_workers.load(Ordering::SeqCst);
                    let max_workers = max_workers.min(WORKER_MAX_LIMIT);
                    if worker_count < max_workers as i32 {
                        //self.resize_workers((worker_count + 1 as usize).try_into().unwrap(), worker_count).await;
                        self.resize_workers(worker_count as usize + 1_usize, Some(worker_count as usize))
                            .await;
                    }

                    //let max_mrf_workers = max_workers.min(MRFWorkerMaxLimit);
                    let max_mrf_workers = max_workers.min(MRF_WORKER_MAX_LIMIT);
                    if self.mrf_worker_size < max_mrf_workers {
                        self.resize_failed_workers(self.mrf_worker_size + 1).await;
                    }
                }
            }
        }
    }
}

pub struct ReplicationResyncer;

impl Default for ReplicationResyncer {
    fn default() -> Self {
        Self
    }
}

impl ReplicationResyncer {
    pub fn new() -> Self {
        Self
    }

    pub async fn persist_to_disk(&self, _ctx: Arc<Context>, _obj_layer: Arc<store::ECStore>) {
        // 实现持久化到磁盘的逻辑
    }
}

pub async fn init_bucket_replication_pool() {
    if let Some(store) = new_object_layer_fn() {
        let opts = ReplicationPoolOpts::default();
        let stats = ReplicationStats;
        let stat = Arc::new(stats);
        warn!("init bucket replication pool");
        ReplicationPool::init_bucket_replication_pool(store, opts, stat).await;
    } else {
        // TODO: to be added
    }
}

pub struct ReplicationClient {
    pub s3cli: S3Client,
    pub remote_peer_client: RemotePeerS3Client,
    pub arn: String,
}

pub trait RemotePeerS3ClientExt {
    fn putobject(remote_bucket: String, remote_object: String, size: i64);
    fn multipart();
}

impl RemotePeerS3ClientExt for RemotePeerS3Client {
    fn putobject(remote_bucket: String, remote_object: String, size: i64) {}

    fn multipart() {}
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationStatusType {
    #[default]
    Pending,
    Completed,
    CompletedLegacy,
    Failed,
    Replica,
    Unknown,
}

impl ReplicationStatusType {
    // Converts the enum variant to its string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            ReplicationStatusType::Pending => "PENDING",
            ReplicationStatusType::Completed => "COMPLETED",
            ReplicationStatusType::CompletedLegacy => "COMPLETE",
            ReplicationStatusType::Failed => "FAILED",
            ReplicationStatusType::Replica => "REPLICA",
            ReplicationStatusType::Unknown => "",
        }
    }

    // Checks if the status is empty (not set)
    pub fn is_empty(&self) -> bool {
        matches!(self, ReplicationStatusType::Pending) // Adjust logic if needed
    }

    // 从字符串构造 ReplicationStatusType 枚举
    pub fn from(value: &str) -> Self {
        match value.to_uppercase().as_str() {
            "PENDING" => ReplicationStatusType::Pending,
            "COMPLETED" => ReplicationStatusType::Completed,
            "COMPLETE" => ReplicationStatusType::CompletedLegacy,
            "FAILED" => ReplicationStatusType::Failed,
            "REPLICA" => ReplicationStatusType::Replica,
            other => ReplicationStatusType::Unknown,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersionPurgeStatusType {
    Pending,
    Complete,
    Failed,
    Empty,
    #[default]
    Unknown,
}

impl VersionPurgeStatusType {
    // 检查是否是 Empty
    pub fn is_empty(&self) -> bool {
        matches!(self, VersionPurgeStatusType::Empty)
    }

    // 检查是否是 Pending（Pending 或 Failed 都算作 Pending 状态）
    pub fn is_pending(&self) -> bool {
        matches!(self, VersionPurgeStatusType::Pending | VersionPurgeStatusType::Failed)
    }
}

// 从字符串实现转换（类似于 Go 的字符串比较）
impl From<&str> for VersionPurgeStatusType {
    fn from(value: &str) -> Self {
        match value.to_uppercase().as_str() {
            "PENDING" => VersionPurgeStatusType::Pending,
            "COMPLETE" => VersionPurgeStatusType::Complete,
            "FAILED" => VersionPurgeStatusType::Failed,
            _ => VersionPurgeStatusType::Empty,
        }
    }
}

impl fmt::Display for VersionPurgeStatusType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            VersionPurgeStatusType::Pending => "PENDING",
            VersionPurgeStatusType::Complete => "COMPLETE",
            VersionPurgeStatusType::Failed => "FAILED",
            VersionPurgeStatusType::Empty => "",
            VersionPurgeStatusType::Unknown => "UNKNOWN",
        };
        write!(f, "{s}")
    }
}

pub fn get_composite_version_purge_status(status_map: &HashMap<String, VersionPurgeStatusType>) -> VersionPurgeStatusType {
    if status_map.is_empty() {
        return VersionPurgeStatusType::Unknown;
    }

    let mut completed_count = 0;

    for status in status_map.values() {
        match status {
            VersionPurgeStatusType::Failed => return VersionPurgeStatusType::Failed,
            VersionPurgeStatusType::Complete => completed_count += 1,
            _ => {}
        }
    }

    if completed_count == status_map.len() {
        VersionPurgeStatusType::Complete
    } else {
        VersionPurgeStatusType::Pending
    }
}

// 定义 ReplicationAction 枚举
#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum ReplicationAction {
    ReplicateMetadata,
    #[default]
    ReplicateNone,
    ReplicateAll,
}

impl FromStr for ReplicationAction {
    // 工厂方法，根据字符串生成对应的枚举
    type Err = ();
    fn from_str(action: &str) -> Result<Self, Self::Err> {
        match action.to_lowercase().as_str() {
            "metadata" => Ok(ReplicationAction::ReplicateMetadata),
            "none" => Ok(ReplicationAction::ReplicateNone),
            "all" => Ok(ReplicationAction::ReplicateAll),
            _ => Err(()),
        }
    }
}

// 定义 ObjectInfo 结构体
// #[derive(Debug)]
// pub struct ObjectInfo {
//     pub e_tag: String,
//     pub version_id: String,
//     pub actual_size: i64,
//     pub mod_time: DateTime<Utc>,
//     pub delete_marker: bool,
//     pub content_type: String,
//     pub content_encoding: String,
//     pub user_tags: HashMap<String, String>,
//     pub user_defined: HashMap<String, String>,
// }

// impl ObjectInfo {
//     // 获取实际大小
//     pub fn get_actual_size(&self) -> i64 {
//         self.actual_size
//     }
// }

// 忽略大小写比较字符串列表
// fn equals(k1: &str, keys: &[&str]) -> bool {
//     keys.iter().any(|&k2| k1.eq_ignore_ascii_case(k2))
// }

// 比较两个对象的 ReplicationAction
pub fn get_replication_action(oi1: &ObjectInfo, oi2: &ObjectInfo, op_type: &str) -> ReplicationAction {
    let _null_version_id = "null";

    // 如果是现有对象复制，判断是否需要跳过同步
    if op_type == "existing" && oi1.mod_time > oi2.mod_time && oi1.version_id.is_none() {
        return ReplicationAction::ReplicateNone;
    }

    let sz = oi1.get_actual_size();

    // 完整复制的条件
    if oi1.etag != oi2.etag
        || oi1.version_id != oi2.version_id
        || sz.unwrap() != oi2.size
        || oi1.delete_marker != oi2.delete_marker
        || oi1.mod_time != oi2.mod_time
    {
        return ReplicationAction::ReplicateAll;
    }

    // 元数据复制的条件
    if oi1.content_type != oi2.content_type {
        return ReplicationAction::ReplicateMetadata;
    }

    // if oi1.content_encoding.is_some() {
    //     if let Some(enc) = oi2
    //         .metadata
    //         .get("content-encoding")
    //         .or_else(|| oi2.metadata.get("content-encoding".to_lowercase().as_str()))
    //     {
    //         if enc.join(",") != oi1.content_encoding {
    //             return ReplicationAction::ReplicateMetadata;
    //         }
    //     } else {
    //         return ReplicationAction::ReplicateMetadata;
    //     }
    // }

    // if !oi2.user_tags.is_empty() && oi1.user_tags != oi2.user_tags {
    //     return ReplicationAction::ReplicateMetadata;
    // }

    // 需要比较的头部前缀列表
    // let compare_keys = vec![
    //     "expires",
    //     "cache-control",
    //     "content-language",
    //     "content-disposition",
    //     "x-amz-object-lock-mode",
    //     "x-amz-object-lock-retain-until-date",
    //     "x-amz-object-lock-legal-hold",
    //     "x-amz-website-redirect-location",
    //     "x-amz-meta-",
    // ];

    // 提取并比较必要的元数据
    // let compare_meta1: HashMap<String, String> = oi1
    //     .user_defined
    //     .iter()
    //     .filter(|(k, _)| compare_keys.iter().any(|prefix| k.to_lowercase().starts_with(prefix)))
    //     .map(|(k, v)| (k.to_lowercase(), v.clone()))
    //     .collect();

    // let compare_meta2: HashMap<String, String> = oi2
    //     .metadata
    //     .iter()
    //     .filter(|(k, _)| compare_keys.iter().any(|prefix| k.to_lowercase().starts_with(prefix)))
    //     .map(|(k, v)| (k.to_lowercase(), v.join(",")))
    //     .collect();

    // if compare_meta1 != compare_meta2 {
    //     return ReplicationAction::ReplicateMetadata;
    // }

    ReplicationAction::ReplicateNone
}

/// 目标的复制决策结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateTargetDecision {
    pub replicate: bool,   // 是否进行复制
    pub synchronous: bool, // 是否是同步复制
    pub arn: String,       // 复制目标的 ARN
    pub id: String,        // ID
}

impl ReplicateTargetDecision {
    /// 创建一个新的 ReplicateTargetDecision 实例
    pub fn new(arn: &str, replicate: bool, synchronous: bool) -> Self {
        Self {
            id: String::new(),
            replicate,
            synchronous,
            arn: arn.to_string(),
        }
    }
}

impl fmt::Display for ReplicateTargetDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{};{};{};{}", self.replicate, self.synchronous, self.arn, self.id)
    }
}

/// 复制决策结构体，包含多个目标的决策
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ReplicateDecision {
    targets_map: HashMap<String, ReplicateTargetDecision>,
}

impl ReplicateDecision {
    /// 创建一个新的空的 ReplicateDecision
    pub fn new() -> Self {
        Self {
            targets_map: HashMap::new(),
        }
    }

    /// 检查是否有任何目标需要复制
    pub fn replicate_any(&self) -> bool {
        self.targets_map.values().any(|t| t.replicate)
    }

    /// 检查是否有任何目标需要同步复制
    pub fn synchronous(&self) -> bool {
        self.targets_map.values().any(|t| t.synchronous)
    }

    /// 将目标的决策添加到 map 中
    pub fn set(&mut self, decision: ReplicateTargetDecision) {
        self.targets_map.insert(decision.arn.clone(), decision);
    }

    /// 返回所有目标的 Pending 状态字符串
    pub fn pending_status(&self) -> String {
        let mut result = String::new();
        for target in self.targets_map.values() {
            if target.replicate {
                result.push_str(&format!("{}=PENDING;", target.arn));
            }
        }
        result
    }
}

impl fmt::Display for ReplicateDecision {
    /// 将 ReplicateDecision 转换为字符串格式
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut entries = Vec::new();
        for (key, value) in &self.targets_map {
            entries.push(format!("{key}={value}"));
        }
        write!(f, "{}", entries.join(","))
    }
}

/// ResyncTargetDecision 表示重同步决策
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResyncTargetDecision {
    pub replicate: bool,
    pub reset_id: String,
    pub reset_before_date: DateTime<Utc>,
}

/// ResyncDecision 表示所有目标的重同步决策
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct ResyncDecision {
    targets: HashMap<String, ResyncTargetDecision>,
}

impl ResyncDecision {
    /// 创建一个新的 ResyncDecision
    pub fn new() -> Self {
        Self { targets: HashMap::new() }
    }

    /// 检查是否没有任何目标需要重同步
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    /// 检查是否有至少一个目标需要重同步
    pub fn must_resync(&self) -> bool {
        self.targets.values().any(|v| v.replicate)
    }

    /// 检查指定目标是否需要重同步
    pub fn must_resync_target(&self, tgt_arn: &str) -> bool {
        if let Some(target) = self.targets.get(tgt_arn) {
            target.replicate
        } else {
            false
        }
    }
}

/// 解析字符串为 ReplicateDecision 结构
pub fn parse_replicate_decision(input: &str) -> Result<ReplicateDecision, &'static str> {
    let mut decision = ReplicateDecision::new();
    if input.is_empty() {
        return Ok(decision);
    }

    for pair in input.split(',') {
        if pair.is_empty() {
            continue;
        }
        let parts: Vec<&str> = pair.split('=').collect();
        if parts.len() != 2 {
            return Err("Invalid replicate decision format");
        }

        let key = parts[0];
        let value = parts[1].trim_matches('"');
        let values: Vec<&str> = value.split(';').collect();

        if values.len() != 4 {
            return Err("Invalid replicate target decision format");
        }

        let replicate = values[0] == "true";
        let synchronous = values[1] == "true";
        let arn = values[2].to_string();
        let id = values[3].to_string();

        decision.set(ReplicateTargetDecision {
            replicate,
            synchronous,
            arn,
            id,
        });
    }
    Ok(decision)
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ReplicatedTargetInfo {
    pub arn: String,
    pub size: i64,
    pub duration: Duration,
    pub replication_action: ReplicationAction,          // 完整或仅元数据
    pub op_type: i32,                                   // 传输类型
    pub replication_status: ReplicationStatusType,      // 当前复制状态
    pub prev_replication_status: ReplicationStatusType, // 上一个复制状态
    pub version_purge_status: VersionPurgeStatusType,   // 版本清理状态
    pub resync_timestamp: String,                       // 重同步时间戳
    pub replication_resynced: bool,                     // 是否重同步
    pub endpoint: String,                               // 目标端点
    pub secure: bool,                                   // 是否安全连接
    pub err: Option<String>,                            // 错误信息
}

// 实现 ReplicatedTargetInfo 方法
impl ReplicatedTargetInfo {
    /// 检查 arn 是否为空
    pub fn is_empty(&self) -> bool {
        self.arn.is_empty()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeletedObjectReplicationInfo {
    #[serde(flatten)] // 使用 `flatten` 将 `DeletedObject` 的字段展开到当前结构体
    pub deleted_object: DeletedObject,

    pub bucket: String,
    pub event_type: String,
    pub op_type: ReplicationType, // 假设 `replication.Type` 是 `ReplicationType` 枚举
    pub reset_id: String,
    pub target_arn: String,
}

pub fn get_composite_replication_status(m: &HashMap<String, ReplicationStatusType>) -> ReplicationStatusType {
    if m.is_empty() {
        return ReplicationStatusType::Unknown;
    }

    let mut completed_count = 0;

    for status in m.values() {
        match status {
            ReplicationStatusType::Failed => return ReplicationStatusType::Failed,
            ReplicationStatusType::Completed => completed_count += 1,
            _ => {}
        }
    }

    if completed_count == m.len() {
        return ReplicationStatusType::Completed;
    }

    ReplicationStatusType::Pending
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ReplicationState {
    pub replica_timestamp: DateTime<Utc>,
    pub replica_status: ReplicationStatusType,
    pub delete_marker: bool,
    pub replication_timestamp: DateTime<Utc>,
    pub replication_status_internal: String,
    pub version_purge_status_internal: String,
    pub replicate_decision_str: String,
    pub targets: HashMap<String, ReplicationStatusType>,
    pub purge_targets: HashMap<String, VersionPurgeStatusType>,
    pub reset_statuses_map: HashMap<String, String>,
}

// impl Default for ReplicationState {
//     fn default() -> Self {
//         ReplicationState {
//             replica_timestamp: Utc::now(),
//             replica_status: ReplicationStatusType::default(),
//             delete_marker: false,
//             replication_timestamp: Utc::now(),
//             replication_status_internal: String::new(),
//             version_purge_status_internal: String::new(),
//             replicate_decision_str: String::new(),
//             targets: HashMap::new(),
//             purge_targets: HashMap::new(),
//             reset_statuses_map: HashMap::new(),
//         }
//     }
// }

pub struct ReplicationObjectOpts {
    pub name: String,
    pub user_tags: Option<String>,
    pub version_id: String,
    pub delete_marker: bool,
    pub ssec: bool,
    pub op_type: ReplicationType,
    pub replica: bool,
    pub existing_object: bool,
    pub target_arn: Option<String>,
}

pub trait ConfigProcess {
    fn filter_actionable_rules(&self, obj: &ReplicationObjectOpts) -> Vec<s3s::dto::ReplicationRule>;

    fn replicate(&self, obj: &ReplicationObjectOpts) -> bool;
    fn filter_target_arns(&self, obj: &ReplicationObjectOpts) -> Vec<String>;
}

impl ConfigProcess for s3s::dto::ReplicationConfiguration {
    fn filter_target_arns(&self, obj: &ReplicationObjectOpts) -> Vec<String> {
        let mut arns = Vec::new();
        let mut tgts_map = HashSet::new();

        let rules = self.filter_actionable_rules(obj);
        debug!("rule len is {}", rules.len());
        for rule in rules {
            debug!("rule");

            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                debug!("rule");
                continue;
            }

            if !self.role.is_empty() {
                debug!("rule");
                arns.push(self.role.clone()); // use legacy RoleArn if present
                return arns;
            }

            debug!("rule");
            if !tgts_map.contains(&rule.destination.bucket) {
                tgts_map.insert(rule.destination.bucket.clone());
            }
        }

        for arn in tgts_map {
            arns.push(arn);
        }
        arns
    }

    fn replicate(&self, obj: &ReplicationObjectOpts) -> bool {
        for rule in self.filter_actionable_rules(obj) {
            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                warn!("need replicate failed");
                continue;
            }
            if obj.existing_object
                && rule.existing_object_replication.is_some()
                && rule.existing_object_replication.unwrap().status
                    == ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::DISABLED)
            {
                warn!("need replicate failed");
                return false;
            }

            if obj.op_type == ReplicationType::DeleteReplicationType {
                return if !obj.version_id.is_empty() {
                    // 扩展：检查版本化删除
                    if rule.delete_replication.is_none() {
                        warn!("need replicate failed");
                        return false;
                    }
                    rule.delete_replication.unwrap().status
                        == DeleteReplicationStatus::from_static(DeleteReplicationStatus::DISABLED)
                } else {
                    if rule.delete_marker_replication.is_none() {
                        warn!("need replicate failed");
                        return false;
                    }
                    if rule.delete_marker_replication.as_ref().unwrap().status.clone().is_none() {
                        warn!("need replicate failed");
                        return false;
                    }
                    rule.delete_marker_replication.as_ref().unwrap().status.clone().unwrap()
                        == DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::DISABLED)
                };
            }
            // 处理常规对象/元数据复制
            if !obj.replica {
                warn!("not need replicate {} {} ", obj.name, obj.version_id);
                return true;
            }
            return obj.replica
                && rule.source_selection_criteria.is_some()
                && rule.source_selection_criteria.unwrap().replica_modifications.unwrap().status
                    == ReplicaModificationsStatus::from_static(ReplicaModificationsStatus::ENABLED);
        }
        warn!("need replicate failed");
        false
    }

    fn filter_actionable_rules(&self, obj: &ReplicationObjectOpts) -> Vec<s3s::dto::ReplicationRule> {
        if obj.name.is_empty()
            && !matches!(obj.op_type, ReplicationType::ResyncReplicationType | ReplicationType::AllReplicationType)
        {
            warn!("filter");
            return vec![];
        }

        let mut rules: Vec<s3s::dto::ReplicationRule> = Vec::new();
        debug!("rule size is {}", &self.rules.len());

        for rule in &self.rules {
            if rule.status.as_str() == ReplicationRuleStatus::DISABLED {
                debug!("rule size is");
                continue;
            }

            if obj.target_arn.is_some()
                && rule.destination.bucket != obj.target_arn.clone().unwrap()
                && self.role != obj.target_arn.clone().unwrap()
            {
                debug!("rule size is");
                continue;
            }
            debug!("match {:?}", obj.op_type.clone());
            if matches!(obj.op_type, ReplicationType::ResyncReplicationType | ReplicationType::AllReplicationType) {
                //println!("filter");
                rules.push(rule.clone());
                continue;
            }

            if obj.existing_object {
                if rule.existing_object_replication.is_none() {
                    continue;
                }

                if rule.existing_object_replication.clone().unwrap().status.as_str() == ExistingObjectReplicationStatus::DISABLED
                {
                    continue;
                }
            }

            if rule.prefix.is_some() && !obj.name.starts_with(rule.prefix.as_ref().unwrap()) {
                continue;
            }

            //if rule.filter.test_tags(&obj.user_tags) {
            rules.push(rule.clone());
            //}
        }

        rules.sort_by(|a, b| {
            if a.priority == b.priority {
                a.destination.bucket.to_string().cmp(&b.destination.bucket.to_string())
            } else {
                b.priority.cmp(&a.priority)
            }
        });

        rules
    }
}

fn replication_statuses_map(s: &str) -> HashMap<String, ReplicationStatusType> {
    let mut targets = HashMap::new();
    let repl_status_regex = Regex::new(r"(\w+):([\w-]+)").unwrap();

    for cap in repl_status_regex.captures_iter(s) {
        if let (Some(target), Some(status)) = (cap.get(1), cap.get(2)) {
            let tp = ReplicationStatusType::from(status.as_str());
            targets.insert(target.as_str().to_string(), tp);
        }
    }

    targets
}

fn version_purge_statuses_map(s: &str) -> HashMap<String, VersionPurgeStatusType> {
    let mut targets = HashMap::new();
    let repl_status_regex = Regex::new(r"(\w+):([\w-]+)").unwrap();

    for cap in repl_status_regex.captures_iter(s) {
        if let (Some(target), Some(status)) = (cap.get(1), cap.get(2)) {
            let ptp = VersionPurgeStatusType::from(status.as_str());
            targets.insert(target.as_str().to_string(), ptp);
        }
    }

    targets
}

pub trait TraitForObjectInfo {
    fn replication_state(&self) -> ReplicationState;
}

const RESERVED_METADATA_PREFIX: &str = "X-Rustfs-Internal-";
const RESERVED_METADATA_PREFIX_LOWER: &str = "x-rustfs-internal-";
lazy_static! {
    static ref THROTTLE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(3600);
}

// Replication-related string constants
pub const REPLICATION_RESET: &str = "replication-reset";
pub const REPLICATION_STATUS: &str = "replication-status";
pub const REPLICATION_TIMESTAMP: &str = "replication-timestamp";
pub const REPLICA_STATUS: &str = "replica-status";
pub const REPLICA_TIMESTAMP: &str = "replica-timestamp";
pub const TAGGING_TIMESTAMP: &str = "tagging-timestamp";
pub const OBJECT_LOCK_RETENTION_TIMESTAMP: &str = "objectlock-retention-timestamp";
pub const OBJECT_LOCK_LEGAL_HOLD_TIMESTAMP: &str = "objectlock-legalhold-timestamp";
pub const REPLICATION_SSEC_CHECKSUM_HEADER: &str = "X-Rustfs-Replication-Ssec-Crc";

impl TraitForObjectInfo for ObjectInfo {
    fn replication_state(&self) -> ReplicationState {
        let mut rs = ReplicationState {
            replication_status_internal: self.replication_status_internal.clone(),
            //version_purge_status_internal: self.version_purge_status_internal.clone(),
            version_purge_status_internal: "".to_string(),
            replicate_decision_str: self.replication_status_internal.clone(),
            targets: HashMap::new(),
            purge_targets: HashMap::new(),
            reset_statuses_map: HashMap::new(),
            replica_timestamp: Utc::now(),
            replica_status: ReplicationStatusType::Pending,
            delete_marker: false,
            replication_timestamp: Utc::now(),
        };

        // Set targets and purge_targets using respective functions
        rs.targets = replication_statuses_map(&self.replication_status_internal);
        //rs.purge_targets = version_purge_statuses_map(&self.version_purge_status_internal);
        rs.purge_targets = version_purge_statuses_map("");

        // Process reset statuses map

        for (k, v) in self.user_defined.iter() {
            if k.starts_with(&(RESERVED_METADATA_PREFIX_LOWER.to_owned() + REPLICATION_RESET)) {
                let arn = k.trim_start_matches(&(RESERVED_METADATA_PREFIX_LOWER.to_owned() + REPLICATION_RESET));
                rs.reset_statuses_map.insert(arn.to_string(), v.clone());
            }
        }

        rs
    }
}

fn convert_offsetdatetime_to_chrono(offset_dt: Option<OffsetDateTime>) -> Option<DateTime<Utc>> {
    //offset_dt.map(|odt| {
    let tm = offset_dt.unwrap().unix_timestamp();
    //let naive = NaiveDateTime::from_timestamp_opt(tm, 0).expect("Invalid timestamp");
    DateTime::<Utc>::from_timestamp(tm, 0)
    //DateTime::from_naive_utc_and_offset(naive, Utc) // Convert to Utc first
    //})
}

pub async fn schedule_replication(oi: ObjectInfo, o: Arc<store::ECStore>, dsc: ReplicateDecision, op_type: i32) {
    let tgt_statuses = replication_statuses_map(&oi.replication_status_internal);
    // //let purge_statuses = version_purge_statuses_map(&oi.);
    let replication_timestamp = Utc::now(); // Placeholder for timestamp parsing
    let replication_state = oi.replication_state();

    let actual_size = oi.actual_size;
    //let ssec = oi.user_defined.contains_key("ssec");
    let ssec = false;

    let ri = ReplicateObjectInfo {
        name: oi.name,
        size: oi.size,
        bucket: oi.bucket,
        version_id: oi
            .version_id
            .map(|uuid| uuid.to_string()) // 将 Uuid 转换为 String
            .unwrap_or_default(),
        etag: oi.etag.unwrap_or_default(),
        mod_time: convert_offsetdatetime_to_chrono(oi.mod_time).unwrap(),
        replication_status: oi.replication_status,
        replication_status_internal: oi.replication_status_internal,
        delete_marker: oi.delete_marker,
        version_purge_status_internal: oi.version_purge_status_internal,
        version_purge_status: oi.version_purge_status,
        replication_state,
        op_type,
        dsc: dsc.clone(),
        target_statuses: tgt_statuses,
        target_purge_statuses: Default::default(),
        replication_timestamp,
        ssec,
        user_tags: oi.user_tags,
        checksum: if ssec { oi.checksum.clone() } else { Vec::new() },
        event_type: "".to_string(),
        retry_count: 0,
        reset_id: "".to_string(),
        existing_obj_resync: Default::default(),
        target_arn: "".to_string(),
        actual_size: 0,
    };

    if dsc.synchronous() {
        warn!("object sync replication");
        replicate_object(ri, o).await;
    } else {
        warn!("object need async replication");
        //GLOBAL_REPLICATION_POOL.lock().unwrap().queue_replica_task(ri);
        let mut pool = GLOBAL_REPLICATION_POOL.write().await;
        pool.as_mut().unwrap().queue_replica_task(ri).await;
    }
}

pub async fn must_replicate(bucket: &str, object: &str, mopts: &MustReplicateOptions) -> ReplicateDecision {
    let mut decision = ReplicateDecision::default();

    // object layer 未初始化时直接返回
    if new_object_layer_fn().is_none() {
        return decision;
    }

    // 检查是否允许复制（版本化前缀
    if !BucketVersioningSys::prefix_enabled(bucket, object).await {
        return decision;
    }

    let repl_status = mopts.replication_status();
    if repl_status == ReplicationStatusType::Replica && !mopts.is_metadata_replication() {
        return decision;
    }

    if mopts.replication_request {
        return decision;
    }

    let cfg = match get_replication_config(bucket).await {
        Ok((config, timestamp)) => config,
        //Ok(None) => return decision,
        Err(err) => {
            //repl_log_once_if(err, bucket);
            return decision;
        }
    };

    let mut opts = ReplicationObjectOpts {
        name: object.to_string(),
        //ssec: crypto::is_ssec_encrypted(&mopts.meta),
        ssec: false,
        replica: repl_status == ReplicationStatusType::Replica,
        existing_object: mopts.is_existing_object_replication(),
        user_tags: None,
        target_arn: None,
        version_id: "0".to_string(),
        delete_marker: false,
        op_type: mopts.op_type,
    };

    if let Some(tag_str) = mopts.meta.get("x-amz-object-tagging") {
        opts.user_tags = Some(tag_str.clone());
    }

    // let rules = cfg.filter_actionable_rules(&opts);
    let tgt_arns = cfg.filter_target_arns(&opts);
    info!("arn lens:{}", tgt_arns.len());
    for tgt_arn in tgt_arns {
        let tgt = bucket_targets::get_bucket_target_client(bucket, &tgt_arn.clone()).await;
        //let tgt = GLOBAL_Bucket_Target_Sys.get().unwrap().get_remote_target_client(tgt)

        // 不判断在线状态，因为目标可能暂时不可用
        opts.target_arn = Some(tgt_arn.clone());
        let replicate = cfg.replicate(&opts);
        info!("need replicate {}", &replicate);

        let synchronous = tgt.is_ok_and(|t| t.replicate_sync);
        //decision.set(ReplicateTargetDecision::new(replicate,synchronous));
        info!("targe decision arn is:{}", tgt_arn.clone());
        decision.set(ReplicateTargetDecision {
            replicate,
            synchronous,
            arn: tgt_arn.clone(),
            id: 0.to_string(),
        });
    }
    info!("must replicate");
    decision
}

impl ReplicationState {
    // Equal 方法：判断两个状态是否相等
    pub fn equal(&self, other: &ReplicationState) -> bool {
        self.replica_status == other.replica_status
            && self.replication_status_internal == other.replication_status_internal
            && self.version_purge_status_internal == other.version_purge_status_internal
    }

    // CompositeReplicationStatus 方法：返回总体的复制状态
    pub fn composite_replication_status(&self) -> ReplicationStatusType {
        if !self.replication_status_internal.is_empty() {
            let status = ReplicationStatusType::from(self.replication_status_internal.as_str());
            match status {
                ReplicationStatusType::Pending
                | ReplicationStatusType::Completed
                | ReplicationStatusType::Failed
                | ReplicationStatusType::Replica => status,
                _ => {
                    let repl_status = get_composite_replication_status(&self.targets);
                    if self.replica_timestamp == Utc::now() || self.replica_timestamp.timestamp() == 0 {
                        return repl_status;
                    }
                    if repl_status == ReplicationStatusType::Completed && self.replica_timestamp > self.replication_timestamp {
                        return self.replica_status.clone();
                    }
                    repl_status
                }
            }
        } else if !self.replica_status.is_empty() {
            self.replica_status.clone()
        } else {
            ReplicationStatusType::Unknown
        }
    }

    // CompositeVersionPurgeStatus 方法：返回总体的版本清除状态
    pub fn composite_version_purge_status(&self) -> VersionPurgeStatusType {
        let status = VersionPurgeStatusType::from(self.version_purge_status_internal.as_str());
        match status {
            VersionPurgeStatusType::Pending | VersionPurgeStatusType::Complete | VersionPurgeStatusType::Failed => status,
            _ => get_composite_version_purge_status(&self.purge_targets),
        }
    }

    // target_state 方法：返回目标状态
    pub fn target_state(&self, arn: &str) -> ReplicatedTargetInfo {
        ReplicatedTargetInfo {
            arn: arn.to_string(),
            prev_replication_status: self.targets.get(arn).cloned().unwrap_or(ReplicationStatusType::Unknown),
            version_purge_status: self
                .purge_targets
                .get(arn)
                .cloned()
                .unwrap_or(VersionPurgeStatusType::Unknown),
            resync_timestamp: self.reset_statuses_map.get(arn).cloned().unwrap_or_default(),
            size: 0,
            replication_status: self.replica_status.clone(),
            duration: Duration::zero(),
            replication_action: ReplicationAction::ReplicateAll,
            op_type: 0,
            replication_resynced: false,
            endpoint: "".to_string(),
            secure: false,
            err: None,
        }
    }
}

lazy_static! {
    static ref REPL_STATUS_REGEX: Regex = Regex::new(r"([^=].*?)=([^,].*?);").unwrap();
}
pub trait ObjectInfoExt {
    fn target_replication_status(&self, arn: String) -> ReplicationStatusType;
    fn is_multipart(&self) -> bool;
}

impl ObjectInfoExt for ObjectInfo {
    fn target_replication_status(&self, arn: String) -> ReplicationStatusType {
        let rep_stat_matches = REPL_STATUS_REGEX.captures_iter(&self.replication_status_internal);
        for matched in rep_stat_matches {
            if let Some(arn_match) = matched.get(1) {
                if arn_match.as_str() == arn {
                    if let Some(status_match) = matched.get(2) {
                        return ReplicationStatusType::from(status_match.as_str());
                    }
                }
            }
        }
        /* `ReplicationStatusType` value */
        ReplicationStatusType::Unknown
    }
    fn is_multipart(&self) -> bool {
        match &self.etag {
            Some(etgval) => etgval.len() != 32 && !etgval.is_empty(),
            None => false,
        }
    }
}

// Replication type enum (placeholder, as it's not clearly used in the Go code)
//#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateObjectInfo {
    pub name: String,
    pub bucket: String,
    pub version_id: String,
    pub etag: String,
    pub size: i64,
    pub actual_size: i64,
    pub mod_time: DateTime<Utc>,
    pub user_tags: String,
    pub ssec: bool,
    pub replication_status: ReplicationStatusType,
    pub replication_status_internal: String,
    pub version_purge_status_internal: String,
    pub version_purge_status: VersionPurgeStatusType,
    pub replication_state: ReplicationState,
    pub delete_marker: bool,

    pub op_type: i32,
    pub event_type: String,
    pub retry_count: u32,
    pub reset_id: String,
    pub dsc: ReplicateDecision,
    pub existing_obj_resync: ResyncDecision,
    pub target_arn: String,
    pub target_statuses: HashMap<String, ReplicationStatusType>,
    pub target_purge_statuses: HashMap<String, VersionPurgeStatusType>,
    pub replication_timestamp: DateTime<Utc>,
    pub checksum: Vec<u8>,
}
impl ReplicateObjectInfo {
    pub fn to_object_info(&self) -> ObjectInfo {
        ObjectInfo {
            bucket: self.bucket.clone(),
            name: self.name.clone(),
            mod_time: Some(
                OffsetDateTime::from_unix_timestamp(self.mod_time.timestamp()).unwrap_or_else(|_| OffsetDateTime::now_utc()),
            ),
            size: self.size,
            actual_size: self.actual_size,
            is_dir: false,
            user_defined: HashMap::new(), // 可以按需从别处导入
            parity_blocks: 0,
            data_blocks: 0,
            version_id: Uuid::try_parse(&self.version_id).ok(),
            delete_marker: self.delete_marker,
            transitioned_object: TransitionedObject::default(),
            restore_ongoing: false,
            restore_expires: Some(OffsetDateTime::now_utc()),
            user_tags: self.user_tags.clone(),
            parts: Vec::new(),
            is_latest: true,
            content_type: None,
            content_encoding: None,
            num_versions: 0,
            successor_mod_time: None,
            put_object_reader: None,
            etag: Some(self.etag.clone()),
            inlined: false,
            metadata_only: false,
            version_only: false,
            replication_status_internal: self.replication_status_internal.clone(),
            replication_status: self.replication_status.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            version_purge_status: self.version_purge_status.clone(),
            checksum: self.checksum.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeletedObject {
    #[serde(rename = "DeleteMarker")]
    pub delete_marker: Option<bool>, // Go 中的 `bool` 转换为 Rust 中的 `Option<bool>` 以支持 `omitempty`

    #[serde(rename = "DeleteMarkerVersionId")]
    pub delete_marker_version_id: Option<String>, // `omitempty` 转为 `Option<String>`

    #[serde(rename = "Key")]
    pub object_name: Option<String>, // 同样适用 `Option` 包含 `omitempty`

    #[serde(rename = "VersionId")]
    pub version_id: Option<String>, // 同上

    // 以下字段未出现在 XML 序列化中，因此不需要 serde 标注
    #[serde(skip)]
    pub delete_marker_mtime: DateTime<Utc>, // 自定义类型，需定义或引入
    #[serde(skip)]
    pub replication_state: ReplicationState, // 自定义类型，需定义或引入
}

// 假设 `DeleteMarkerMTime` 和 `ReplicationState` 的定义如下：
#[derive(Debug, Default, Clone)]
pub struct DeleteMarkerMTime {
    time: chrono::NaiveDate,
    // 填写具体字段类型
}

impl ReplicationWorkerOperation for ReplicateObjectInfo {
    fn to_mrf_entry(&self) -> MRFReplicateEntry {
        MRFReplicateEntry {
            bucket: self.bucket.clone(),
            object: self.name.clone(),
            version_id: self.version_id.clone(), // 直接使用计算后的 version_id
            retry_count: 0,
            sz: self.size,
        }
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ReplicationWorkerOperation for DeletedObjectReplicationInfo {
    fn to_mrf_entry(&self) -> MRFReplicateEntry {
        MRFReplicateEntry {
            bucket: self.bucket.clone(),
            object: self.deleted_object.object_name.clone().unwrap().clone(),
            version_id: self.deleted_object.delete_marker_version_id.clone().unwrap_or_default(),
            retry_count: 0,
            sz: 0,
        }
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn get_s3client_from_para(ak: &str, sk: &str, url: &str, _region: &str) -> Result<S3Client, Box<dyn std::error::Error>> {
    let credentials = Credentials::new(ak, sk, None, None, "");
    let region = Region::new("us-east-1".to_string());

    let config = Config::builder()
        .region(region)
        .endpoint_url(url.to_string())
        .credentials_provider(credentials)
        .behavior_version(BehaviorVersion::latest()) // Adjust as necessary
        .build();
    Ok(S3Client::from_conf(config))
}

// use hyper::body::Body;
// use s3s::Body;

async fn replicate_object_with_multipart(
    rep_obj: &ReplicateObjectInfo,
    local_obj_info: &ObjectInfo,
    target_info: &ReplicatedTargetInfo,
    tgt_cli: &TargetClient,
) -> Result<(), Error> {
    let store = new_object_layer_fn().unwrap();
    let provider = StaticProvider::new(&tgt_cli.ak, &tgt_cli.sk, None);
    let rustfs_cli = Minio::builder()
        .endpoint(target_info.endpoint.clone())
        .provider(provider)
        .secure(false)
        .build()
        .map_err(|e| Error::other(format!("build rustfs client failed: {e}")))?;

    let ret = rustfs_cli
        .create_multipart_upload_with_versionid(tgt_cli.bucket.clone(), local_obj_info.name.clone(), rep_obj.version_id.clone())
        .await;
    match ret {
        Ok(task) => {
            let parts_len = local_obj_info.parts.len();
            let mut part_results = vec![None; parts_len];
            let version_id = local_obj_info.version_id.expect("missing version_id");
            let task = Arc::new(task); // clone safe
            let store = Arc::new(store);
            let rustfs_cli = Arc::new(rustfs_cli);

            let mut upload_futures = FuturesUnordered::new();

            for (index, _) in local_obj_info.parts.iter().enumerate() {
                let store = Arc::clone(&store);
                let rustfs_cli = Arc::clone(&rustfs_cli);
                let task = Arc::clone(&task);
                let bucket = local_obj_info.bucket.clone();
                let name = local_obj_info.name.clone();

                upload_futures.push(tokio::spawn(async move {
                    let get_opts = ObjectOptions {
                        version_id: Some(version_id.to_string()),
                        versioned: true,
                        part_number: Some(index + 1),
                        version_suspended: false,
                        ..Default::default()
                    };

                    let h = HeaderMap::new();
                    match store.get_object_reader(&bucket, &name, None, h, &get_opts).await {
                        Ok(mut reader) => match reader.read_all().await {
                            Ok(ret) => {
                                debug!("readall suc:");
                                let body = Bytes::from(ret);
                                match rustfs_cli.upload_part(&task, index + 1, body).await {
                                    Ok(part) => {
                                        debug!("multipar upload suc:");
                                        Ok((index, part))
                                    }
                                    Err(err) => {
                                        error!("upload part {} failed: {}", index + 1, err);
                                        Err(Error::other(format!("upload error: {err}")))
                                    }
                                }
                            }
                            Err(err) => {
                                error!("read error for part {}: {}", index + 1, err);
                                Err(err)
                            }
                        },
                        Err(err) => {
                            error!("reader error for part {}: {}", index + 1, err);
                            Err(Error::other(format!("reader error: {err}")))
                        }
                    }
                }));
            }

            while let Some(result) = upload_futures.next().await {
                match result {
                    Ok(Ok((index, part))) => {
                        part_results[index] = Some(part);
                    }
                    Ok(Err(err)) => {
                        error!("upload part failed: {}", err);
                        return Err(err);
                    }
                    Err(join_err) => {
                        error!("tokio join error: {}", join_err);
                        return Err(Error::other(format!("join error: {join_err}")));
                    }
                }
            }

            let parts: Vec<_> = part_results.into_iter().flatten().collect();

            let ret = rustfs_cli.complete_multipart_upload(&task, parts, None).await;
            match ret {
                Ok(res) => {
                    warn!("finish upload suc:{:?} version_id={:?}", res, local_obj_info.version_id);
                }
                Err(err) => {
                    error!("finish upload failed:{}", err);
                    return Err(Error::other(format!("finish upload failed:{err}")));
                }
            }
        }
        Err(err) => {
            return Err(Error::other(format!("finish upload failed:{err}")));
        }
    }
    Ok(())
}

impl ReplicateObjectInfo {
    fn target_replication_status(&self, arn: &str) -> ReplicationStatusType {
        // 定义正则表达式，匹配类似 `arn;status` 格式
        let repl_status_regex = Regex::new(r"(\w+);(\w+)").expect("Invalid regex");

        // 遍历正则表达式的匹配项
        for caps in repl_status_regex.captures_iter(&self.replication_status_internal) {
            if let (Some(matched_arn), Some(matched_status)) = (caps.get(1), caps.get(2)) {
                // 如果 ARN 匹配，返回对应的状态
                if matched_arn.as_str() == arn {
                    return ReplicationStatusType::from(matched_status.as_str());
                }
            }
        }

        // 如果没有匹配到，返回默认的 `Unknown` 状态
        ReplicationStatusType::Unknown
    }

    async fn replicate_object(&self, target: &TargetClient, _arn: String) -> ReplicatedTargetInfo {
        let _start_time = Utc::now();

        // 初始化 ReplicatedTargetInfo
        warn!("replicate is {}", _arn.clone());
        let mut rinfo = ReplicatedTargetInfo {
            size: self.actual_size,
            arn: _arn.clone(),
            prev_replication_status: self.target_replication_status(&_arn.clone()),
            replication_status: ReplicationStatusType::Failed,
            op_type: self.op_type,
            replication_action: ReplicationAction::ReplicateAll,
            endpoint: target.endpoint.clone(),
            secure: target.endpoint.clone().contains("https://"),
            resync_timestamp: Utc::now().to_string(),
            replication_resynced: false,
            duration: Duration::default(),
            err: None,
            version_purge_status: VersionPurgeStatusType::Pending,
        };

        if self.target_replication_status(&_arn) == ReplicationStatusType::Completed
            && !self.existing_obj_resync.is_empty()
            && !self.existing_obj_resync.must_resync_target(&_arn)
        {
            warn!("replication return");
            rinfo.replication_status = ReplicationStatusType::Completed;
            rinfo.replication_resynced = true;
            return rinfo;
        }

        // 模拟远程目标离线的检查
        // if self.is_target_offline(&target.endpoint) {
        //     rinfo.err = Some(format!(
        //         "Target is offline for bucket: {} arn: {} retry: {}",
        //         self.bucket,
        //         _arn.clone(),
        //         self.retry_count
        //     ));
        //     return rinfo;
        // }

        // versioned := globalBucketVersioningSys.PrefixEnabled(bucket, object)
        // versionSuspended := globalBucketVersioningSys.PrefixSuspended(bucket, object)

        // 模拟对象获取和元数据检查
        let opt = ObjectOptions {
            version_id: Some(self.version_id.clone()),
            versioned: true,
            version_suspended: false,
            ..Default::default()
        };

        let object_info = match self.get_object_info(opt).await {
            Ok(info) => info,
            Err(err) => {
                error!("get object info err:{}", err);
                rinfo.err = Some(err.to_string());
                return rinfo;
            }
        };

        rinfo.prev_replication_status = object_info.target_replication_status(_arn);

        // 设置对象大小
        //rinfo.size = object_info.actual_size.unwrap_or(0);
        rinfo.size = object_info.actual_size;
        //rinfo.replication_action = object_info.

        rinfo.replication_status = ReplicationStatusType::Completed;
        rinfo.size = object_info.get_actual_size().unwrap_or(0) as i64;
        rinfo.replication_action = ReplicationAction::ReplicateAll;

        let store = new_object_layer_fn().unwrap();
        //todo!() put replicationopts;
        if object_info.is_multipart() {
            debug!("version is multi part");
            match replicate_object_with_multipart(self, &object_info, &rinfo, target).await {
                Ok(_) => {
                    rinfo.replication_status = ReplicationStatusType::Completed;
                    println!("Object replicated successfully.");
                }
                Err(e) => {
                    rinfo.replication_status = ReplicationStatusType::Failed;
                    error!("Failed to replicate object: {:?}", e);
                    // 你可以根据错误类型进一步分类处理
                }
            }
            //replicate_object_with_multipart(local_obj_info, target_info, tgt_cli)
        } else {
            let get_opts = ObjectOptions {
                version_id: Some(object_info.version_id.expect("REASON").to_string()),
                versioned: true,
                version_suspended: false,
                ..Default::default()
            };
            warn!("version id is:{:?}", get_opts.version_id);
            let h = HeaderMap::new();
            let gr = store
                .get_object_reader(&object_info.bucket, &object_info.name, None, h, &get_opts)
                .await;

            match gr {
                Ok(mut reader) => {
                    warn!("endpoint is: {}", rinfo.endpoint);
                    let provider = StaticProvider::new(&target.ak, &target.sk, None);
                    let res = reader.read_all().await;
                    match res {
                        Ok(ret) => {
                            let body = rustfs_rsc::Data::from(ret);
                            let rustfs_cli = Minio::builder()
                                .endpoint(rinfo.endpoint.clone())
                                .provider(provider)
                                .secure(false)
                                .build()
                                .unwrap();

                            let ex = rustfs_cli.executor(Method::PUT);
                            let ret = ex
                                .bucket_name(target.bucket.clone())
                                .object_name(self.name.clone())
                                .body(body)
                                .query("versionId", get_opts.version_id.clone().unwrap())
                                .send_ok()
                                .await;
                            match ret {
                                Ok(_res) => {
                                    warn!("replicate suc: {} {} {}", self.bucket, self.name, self.version_id);
                                    rinfo.replication_status = ReplicationStatusType::Completed;
                                }
                                Err(err) => {
                                    error!("replicate {} err:{}", target.bucket.clone(), err);
                                    rinfo.replication_status = ReplicationStatusType::Failed;
                                }
                            }
                        }
                        Err(err) => {
                            error!("read_all err {}", err);
                            rinfo.replication_status = ReplicationStatusType::Failed;
                            return rinfo;
                        }
                    }
                }
                Err(err) => {
                    rinfo.replication_status = ReplicationStatusType::Failed;
                    error!("get client error {}", err);
                }
            }
        }
        rinfo
    }

    fn is_target_offline(&self, endpoint: &str) -> bool {
        // 模拟检查目标是否离线
        warn!("Checking if target {} is offline", endpoint);
        false
    }

    async fn get_object_info(&self, opts: ObjectOptions) -> Result<ObjectInfo, Error> {
        let objectlayer = new_object_layer_fn();
        //let opts = ecstore::store_api::ObjectOptions { max_parity: (), mod_time: (), part_number: (), delete_prefix: (), version_id: (), no_lock: (), versioned: (), version_suspended: (), skip_decommissioned: (), skip_rebalancing: (), data_movement: (), src_pool_idx: (), user_defined: (), preserve_etag: (), metadata_chg: (), replication_request: (), delete_marker: () }
        objectlayer.unwrap().get_object_info(&self.bucket, &self.name, &opts).await
    }

    fn perform_replication(&self, target: &RemotePeerS3Client, object_info: &ObjectInfo) -> Result<(), String> {
        // 模拟复制操作
        // println!(
        //     "Replicating object {} to target {}",
        //     //object_info.name, target.arn
        // );
        Ok(())
    }

    fn current_timestamp() -> String {
        // 返回当前时间戳
        "2024-12-18T00:00:00Z".to_string()
    }
}

//pub fn getvalidrule(cfg: ReplicationConfiguration) -> Vec<String> {
// let mut arns = Vec::new();
// let mut tgts_map = std::collections::HashSet::new();
// for rule in cfg.rules {
//     if rule.status.as_str() == "Disable" {
//         continue;
//     }

//     if tgts_map.insert(rule.clone()) {}
// }
// arns
//}

pub async fn replicate_delete(_ri: &DeletedObjectReplicationInfo, object_api: Arc<store::ECStore>) {}

pub fn clone_mss(v: &HashMap<String, String>) -> HashMap<String, String> {
    let mut r = HashMap::with_capacity(v.len());
    for (k, v) in v {
        r.insert(k.clone(), v.clone());
    }
    r
}

pub fn get_must_replicate_options(
    user_defined: &HashMap<String, String>,
    user_tags: &str,
    status: ReplicationStatusType, // 假设 `status` 是字符串类型
    op: ReplicationType,           // 假设 `op` 是字符串类型
    opts: &ObjectOptions,
) -> MustReplicateOptions {
    let mut meta = clone_mss(user_defined);

    if !user_tags.is_empty() {
        meta.insert("xhttp.AmzObjectTagging".to_string(), user_tags.to_string());
    }

    MustReplicateOptions {
        meta,
        status,
        op_type: op,
        replication_request: opts.replication_request,
    }
}

#[derive(Default)]
struct ReplicatedInfos {
    //replication_time_stamp: DateTime<Utc>,
    targets: Vec<ReplicatedTargetInfo>,
}

// #[derive(Clone, Copy, PartialEq)]
// enum ReplicationStatus {
//     Completed,
//     InProgress,
//     Pending,
// }

impl ReplicatedInfos {
    pub fn action(&self) -> ReplicationAction {
        for target in &self.targets {
            if target.is_empty() {
                continue;
            }
            if target.prev_replication_status != ReplicationStatusType::Completed {
                return target.replication_action.clone();
            }
        }
        ReplicationAction::ReplicateNone
    }

    // fn completed_size(&self) -> i64 {
    //     let mut sz = 0;
    //     for t in &self.targets {
    //         if t.empty() {
    //             continue;
    //         }
    //         if t.replication_status == ReplicationStatusType::Completed
    //             && t.prev_replication_status != ReplicationStatusType::Completed
    //         {
    //             sz += t.size;
    //         }
    //     }
    //     sz
    // }

    pub fn replication_resynced(&self) -> bool {
        // 只要存在一个非 empty 且 replication_resynced 为 true 的目标，就返回 true
        self.targets.iter().any(|t| !t.is_empty() && t.replication_resynced)
    }

    /// 对应 Go 的 ReplicationStatusInternal
    pub fn replication_status_internal(&self) -> String {
        let mut buf = String::new();
        for t in &self.targets {
            if t.is_empty() {
                continue;
            }
            // 类似 fmt.Fprintf(b, "%s=%s;", t.Arn, t.ReplicationStatus.String())
            buf.push_str(&format!("{}={};", t.arn, t.replication_status.as_str()));
        }
        buf
    }

    pub fn replication_status(&self) -> ReplicationStatusType {
        // 如果没有任何目标，返回 Unknown（对应 Go 里 StatusType("")）
        if self.targets.is_empty() {
            return ReplicationStatusType::Unknown;
        }

        // 统计已完成的数量
        let mut completed = 0;

        for t in &self.targets {
            match t.replication_status {
                ReplicationStatusType::Failed => {
                    // 只要有一个失败，整体就是 Failed
                    return ReplicationStatusType::Failed;
                }
                ReplicationStatusType::Completed => {
                    completed += 1;
                }
                _ => {}
            }
        }

        // 全部完成，则 Completed，否则 Pending
        if completed == self.targets.len() {
            ReplicationStatusType::Completed
        } else {
            ReplicationStatusType::Pending
        }
    }
}

impl ReplicatedTargetInfo {
    fn empty(&self) -> bool {
        // Implement your logic to check if the target is empty
        self.size == 0
    }
}

pub async fn replicate_object(ri: ReplicateObjectInfo, object_api: Arc<store::ECStore>) {
    let bucket = ri.bucket.clone();
    let obj = ri.name.clone();
    match get_replication_config(&bucket).await {
        Ok((cfg, timestamp)) => {
            info!(
                "replicate object: {} {} and arn is: {}",
                ri.name.clone(),
                timestamp,
                ri.target_arn.clone()
            );
            //let arns = getvalidrule(config);

            //TODO:nslock

            let objectlayer = new_object_layer_fn();

            let opts = ReplicationObjectOpts {
                name: ri.name.clone(),
                //ssec: crypto::is_ssec_encrypted(&mopts.meta),
                ssec: false,
                //replica: repl_status == ReplicationStatusType::Replica,
                replica: ri.replication_status == ReplicationStatusType::Replica,
                existing_object: ri.existing_obj_resync.must_resync(),
                user_tags: None,
                target_arn: Some(ri.target_arn.clone()),
                version_id: ri.version_id.clone(),
                delete_marker: false,
                op_type: ReplicationType::from_u8(ri.op_type as u8).expect("REASON"),
            };

            let tgt_arns = cfg.filter_target_arns(&opts);
            info!("target len:{}", tgt_arns.len());

            let rinfos = Arc::new(Mutex::new(ReplicatedInfos::default()));
            let cri = Arc::new(ri.clone());
            let mut tasks: Vec<task::JoinHandle<()>> = vec![];

            for tgt_arn in tgt_arns {
                let tgt = bucket_targets::get_bucket_target_client(&ri.bucket, &tgt_arn).await;

                if tgt.is_err() {
                    // repl_log_once_if(ctx, format!("failed to get target for bucket: {} arn: {}", bucket, tgt_arn), &tgt_arn).await;
                    // send_event(event_args {
                    //     event_name: "ObjectReplicationNotTracked".to_string(),
                    //     bucket_name: bucket.to_string(),
                    //     object: ri.to_object_info(),
                    //     user_agent: "Internal: [Replication]".to_string(),
                    //     host: global_local_node_name.to_string(),
                    // }).await;
                    continue;
                }

                let tgt = tgt.unwrap();
                let rinfos_clone = Arc::clone(&rinfos);
                let lcri = Arc::clone(&cri);
                let task = task::spawn(async move {
                    warn!("async task");
                    let mut tgt_info: ReplicatedTargetInfo = Default::default();
                    if lcri.op_type as u8 == ReplicationType::ObjectReplicationType.as_u8() {
                        warn!("object replication and arn is {}", tgt.arn.clone());
                        // all incoming calls go through optimized path.`o`

                        tgt_info = lcri.replicate_object(&tgt, tgt.arn.clone()).await;
                    } else {
                        warn!("async task");
                        // tgt_info = ri.replicate_all(object_api, &tgt).await;
                    }

                    let mut rinfos_locked = rinfos_clone.lock().await;
                    rinfos_locked.targets.push(tgt_info);
                });

                tasks.push(task);
            }
            //futures::future::join_all(tasks);
            futures::future::join_all(tasks).await;

            let mut rs = rinfos.lock().await;
            let replication_status = rs.replication_status();
            //rinfos
            let new_repl_status_internal = rs.replication_status_internal();
            // ri.to_object_info() 假设...
            warn!("{} and {}", new_repl_status_internal, ri.replication_status_internal);
            let obj_info = ri.to_object_info();
            if ri.replication_status_internal != new_repl_status_internal || rs.replication_resynced() {
                warn!("save meta");
                let mut eval_metadata = HashMap::new();

                eval_metadata.insert(
                    format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replication-status"),
                    new_repl_status_internal.clone(),
                );
                eval_metadata.insert(
                    format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replication-timestamp"),
                    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                );
                eval_metadata.insert("x-amz-bucket-replication-status".to_string(), replication_status.as_str().to_owned());

                for rinfo in &rs.targets {
                    // if !rinfo.resync_timestamp.is_empty() {
                    //     eval_metadata.insert(
                    //         format!("x-rustfs-replication-reset-status-{}", rinfo.arn),
                    //         rinfo.resync_timestamp.clone(),
                    //     );
                    // }
                }

                if !ri.user_tags.is_empty() {
                    eval_metadata.insert("x-amz-tagging".to_string(), ri.user_tags.clone());
                }

                let popts = ObjectOptions {
                    //mod_time: Some(ri.mod_time),
                    mod_time: None,
                    version_id: Some(ri.version_id.clone()),
                    eval_metadata: Some(eval_metadata),
                    ..Default::default()
                };

                //let uobj_info = ;
                match object_api.put_object_metadata(&ri.bucket, &ri.name, &popts).await {
                    Ok(info) => {
                        info!("Put metadata success: {:?}", info);
                        // 你可以访问 info 字段，例如 info.size, info.last_modified 等
                    }
                    Err(e) => {
                        error!("Failed to put metadata: {}", e);
                        // 根据错误类型做不同处理
                        // if let Some(CustomError::NotFound) = e.downcast_ref::<CustomError>() { ... }
                    }
                }

                // if !uobj_info.name.is_empty() {
                //     obj_info = uobj_info;
                // }

                let mut op_type = ReplicationType::MetadataReplicationType;
                if rs.action() == ReplicationAction::ReplicateAll {
                    op_type = ReplicationType::ObjectReplicationType
                }

                for rinfo in &mut rs.targets {
                    if rinfo.replication_status != rinfo.prev_replication_status {
                        //rinfo.op_type = Some(op_type.clone());
                        //global_replication_stats::update(&bucket, rinfo);
                    }
                }
                debug!("op type: {:?}", op_type);
            }

            // send_event(EventArgs {
            //     event_name: ri.event_name.clone(),
            //     bucket_name: bucket.into(),
            //     object: obj_info.clone(),
            //     user_agent: "Internal: [Replication]".into(),
            //     host: "local-node-name".into(),
            // });

            // 失败重试
            // if rs.replication_status() != ReplicationStatusType::Completed {
            //     //ri.op_type = "HealReplicationType".into();
            //     ri.event_type = "ReplicateMRF".into();
            //     //ri.replication_status_internal = rinfos.replication_status_internal();
            //     ri.retry_count += 1;
            //     // global_replication_pool.get().queue_mrf_save(ri.to_mrf_entry());
            // }
        }
        Err(err) => {
            println!("Failed to get replication config: {err:?}");
        }
    }
}
