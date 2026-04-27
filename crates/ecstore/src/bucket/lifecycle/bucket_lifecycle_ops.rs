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

use crate::bucket::lifecycle::bucket_lifecycle_audit::{LcAuditEvent, LcEventSrc};
use crate::bucket::lifecycle::evaluator::Evaluator;
use crate::bucket::lifecycle::lifecycle::{
    self, ExpirationOptions, Lifecycle, ObjectOpts, TransitionOptions, abort_incomplete_multipart_upload_due,
};
use crate::bucket::lifecycle::tier_last_day_stats::{DailyAllTierStats, LastDayTierStats};
use crate::bucket::lifecycle::tier_sweeper::{Jentry, delete_object_from_remote_tier};
use crate::bucket::object_lock::objectlock_sys::check_object_lock_for_deletion;
use crate::bucket::replication::{
    DeletedObjectReplicationInfo, ReplicationConfig, check_replicate_delete, schedule_replication_delete,
};
use crate::bucket::{metadata_sys, metadata_sys::get_lifecycle_config, versioning_sys::BucketVersioningSys};
use crate::client::object_api_utils::new_getobjectreader;
use crate::disk::error::DiskError;
use crate::disk::{DeleteOptions, Disk, DiskAPI, RUSTFS_META_MULTIPART_BUCKET, STORAGE_FORMAT_FILE};
use crate::error::Error;
use crate::error::StorageError;
use crate::error::{error_resp_to_object_err, is_err_object_not_found, is_err_version_not_found, is_network_or_host_down};
use crate::event_notification::{EventArgs, send_event};
use crate::global::GLOBAL_LocalNodeName;
use crate::global::{GLOBAL_LifecycleSys, GLOBAL_TierConfigMgr, get_global_deployment_id};
use crate::set_disk::{MAX_PARTS_COUNT, RUSTFS_MULTIPART_BUCKET_KEY, RUSTFS_MULTIPART_OBJECT_KEY, SetDisks};
use crate::store::ECStore;
use crate::store_api::{
    GetObjectReader, HTTPRangeSpec, ListOperations, MultipartOperations, ObjectInfo, ObjectOperations, ObjectOptions,
    ObjectToDelete,
};
use crate::tier::warm_backend::WarmBackendGetOpts;
use async_channel::{Receiver as A_Receiver, Sender as A_Sender, bounded};
use futures::Future;
use http::HeaderMap;
use lazy_static::lazy_static;
use rustfs_common::data_usage::TierStats;
use rustfs_common::heal_channel::rep_has_active_rules;
use rustfs_common::metrics::{IlmAction, Metrics};
use rustfs_filemeta::{
    FileInfo, FileInfoOpts, NULL_VERSION_ID, REPLICATE_INCOMING_DELETE, ReplicateDecision, ReplicationState, RestoreStatusOps,
    VersionPurgeStatusType, get_file_info, is_restored_object_on_disk,
};
use rustfs_s3_common::EventName;
use rustfs_utils::{get_env_i64, get_env_usize, path::encode_dir_object, string::strings_has_prefix_fold};
use s3s::dto::{
    BucketLifecycleConfiguration, DefaultRetention, ReplicationConfiguration, RestoreRequest, RestoreRequestType, RestoreStatus,
    Timestamp,
};
use s3s::header::{X_AMZ_RESTORE, X_AMZ_SERVER_SIDE_ENCRYPTION};
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::HashMap;
use std::env;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration as StdDuration;
use time::OffsetDateTime;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use xxhash_rust::xxh64;

pub type TimeFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;
pub type TraceFn =
    Arc<dyn Fn(String, HashMap<String, String>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;
pub type ExpiryOpType = Box<dyn ExpiryOp + Send + Sync + 'static>;

static XXHASH_SEED: u64 = 0;

pub const AMZ_OBJECT_TAGGING: &str = "X-Amz-Tagging";
pub const AMZ_TAG_COUNT: &str = "x-amz-tagging-count";
pub const AMZ_TAG_DIRECTIVE: &str = "X-Amz-Tagging-Directive";
pub const AMZ_ENCRYPTION_AES: &str = "AES256";
pub const AMZ_ENCRYPTION_KMS: &str = "aws:kms";

pub const ERR_INVALID_STORAGECLASS: &str = "invalid tier.";
const ENV_STALE_UPLOADS_EXPIRY: &str = "RUSTFS_API_STALE_UPLOADS_EXPIRY";
const ENV_STALE_UPLOADS_CLEANUP_INTERVAL: &str = "RUSTFS_API_STALE_UPLOADS_CLEANUP_INTERVAL";
const DEFAULT_STALE_UPLOADS_EXPIRY: StdDuration = StdDuration::from_secs(24 * 60 * 60);
const DEFAULT_STALE_UPLOADS_CLEANUP_INTERVAL: StdDuration = StdDuration::from_secs(6 * 60 * 60);

lazy_static! {
    pub static ref GLOBAL_ExpiryState: Arc<RwLock<ExpiryState>> = ExpiryState::new();
    pub static ref GLOBAL_TransitionState: Arc<TransitionState> = TransitionState::new();
}

pub struct LifecycleSys;

impl LifecycleSys {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }

    pub async fn get(&self, bucket: &str) -> Option<BucketLifecycleConfiguration> {
        match get_lifecycle_config(bucket).await {
            Ok((lc, _)) => Some(lc),
            Err(Error::ConfigNotFound) => None,
            Err(err) => {
                warn!(bucket, error = ?err, "failed to load lifecycle config");
                None
            }
        }
    }

    pub fn trace(oi: &ObjectInfo) -> TraceFn {
        let bucket = oi.bucket.clone();
        let name = oi.name.clone();
        let version_id = oi.version_id.map(|v| v.to_string()).unwrap_or_default();
        Arc::new(move |_action: String, _ctx: HashMap<String, String>| {
            let bucket = bucket.clone();
            let name = name.clone();
            let version_id = version_id.clone();
            Box::pin(async move {
                info!(
                    bucket = %bucket,
                    object = %name,
                    version_id = %version_id,
                    action = %_action,
                    "ILM lifecycle trace: {} on {}/{} (version: {})",
                    _action, bucket, name, version_id
                );
            })
        })
    }
}

struct ExpiryTask {
    obj_info: ObjectInfo,
    event: lifecycle::Event,
    src: LcEventSrc,
}

impl ExpiryOp for ExpiryTask {
    fn op_hash(&self) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(self.obj_info.bucket.as_bytes());
        hasher.update(self.obj_info.name.as_bytes());
        xxh64::xxh64(hasher.finalize().as_slice(), XXHASH_SEED)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct ExpiryStats {
    missed_expiry_tasks: AtomicI64,
    missed_freevers_tasks: AtomicI64,
    missed_tier_journal_tasks: AtomicI64,
    workers: AtomicI64,
}

#[allow(dead_code)]
impl ExpiryStats {
    pub fn missed_tasks(&self) -> i64 {
        self.missed_expiry_tasks.load(Ordering::SeqCst)
    }

    fn missed_free_vers_tasks(&self) -> i64 {
        self.missed_freevers_tasks.load(Ordering::SeqCst)
    }

    fn missed_tier_journal_tasks(&self) -> i64 {
        self.missed_tier_journal_tasks.load(Ordering::SeqCst)
    }

    fn num_workers(&self) -> i64 {
        self.workers.load(Ordering::SeqCst)
    }
}

pub trait ExpiryOp: 'static {
    fn op_hash(&self) -> u64;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug, Default, Clone)]
pub struct TransitionedObject {
    pub name: String,
    pub version_id: String,
    pub tier: String,
    pub free_version: bool,
    pub status: String,
}

struct FreeVersionTask(ObjectInfo);

impl ExpiryOp for FreeVersionTask {
    fn op_hash(&self) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(self.0.transitioned_object.tier.as_bytes());
        hasher.update(self.0.transitioned_object.name.as_bytes());
        xxh64::xxh64(hasher.finalize().as_slice(), XXHASH_SEED)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct NewerNoncurrentTask {
    bucket: String,
    versions: Vec<ObjectToDelete>,
    event: lifecycle::Event,
}

impl ExpiryOp for NewerNoncurrentTask {
    fn op_hash(&self) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(self.bucket.as_bytes());
        hasher.update(self.versions[0].object_name.as_bytes());
        xxh64::xxh64(hasher.finalize().as_slice(), XXHASH_SEED)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct ExpiryState {
    tasks_tx: Vec<Sender<Option<ExpiryOpType>>>,
    tasks_rx: Vec<Arc<tokio::sync::Mutex<Receiver<Option<ExpiryOpType>>>>>,
    stats: Option<ExpiryStats>,
}

impl ExpiryState {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            tasks_tx: vec![],
            tasks_rx: vec![],
            stats: Some(ExpiryStats {
                missed_expiry_tasks: AtomicI64::new(0),
                missed_freevers_tasks: AtomicI64::new(0),
                missed_tier_journal_tasks: AtomicI64::new(0),
                workers: AtomicI64::new(0),
            }),
        }))
    }

    pub async fn pending_tasks(&self) -> usize {
        let rxs = &self.tasks_rx;
        if rxs.is_empty() {
            return 0;
        }
        let mut tasks = 0;
        for rx in rxs.iter() {
            tasks += rx.lock().await.len();
        }
        tasks
    }

    pub async fn enqueue_tier_journal_entry(&mut self, je: &Jentry) -> Result<(), std::io::Error> {
        let wrkr = self.get_worker_ch(je.op_hash());
        if wrkr.is_none() {
            *self.stats.as_mut().expect("stats lock").missed_tier_journal_tasks.get_mut() += 1;
            return Ok(());
        }
        let wrkr = wrkr.expect("worker channel should exist after None check");
        select! {
            //_ -> GlobalContext.Done() => ()
            _ = wrkr.send(Some(Box::new(je.clone()))) => (),
            else => {
                *self.stats.as_mut().expect("stats lock").missed_tier_journal_tasks.get_mut() += 1;
            }
        }
        Ok(())
    }

    pub async fn enqueue_free_version(&mut self, oi: ObjectInfo) {
        let task = FreeVersionTask(oi);
        let wrkr = self.get_worker_ch(task.op_hash());
        if wrkr.is_none() {
            *self.stats.as_mut().expect("stats lock").missed_freevers_tasks.get_mut() += 1;
            return;
        }
        let wrkr = wrkr.expect("worker channel should exist after None check");
        select! {
            //_ -> GlobalContext.Done() => {}
            _ = wrkr.send(Some(Box::new(task))) => (),
            else => {
                *self.stats.as_mut().expect("stats lock").missed_freevers_tasks.get_mut() += 1;
            }
        }
    }

    pub async fn enqueue_by_days(&mut self, oi: &ObjectInfo, event: &lifecycle::Event, src: &LcEventSrc) {
        let task = ExpiryTask {
            obj_info: oi.clone(),
            event: event.clone(),
            src: src.clone(),
        };
        let wrkr = self.get_worker_ch(task.op_hash());
        if wrkr.is_none() {
            *self.stats.as_mut().expect("stats lock").missed_expiry_tasks.get_mut() += 1;
            return;
        }
        let wrkr = wrkr.expect("worker channel should exist after None check");
        select! {
            //_ -> GlobalContext.Done() => {}
            _ = wrkr.send(Some(Box::new(task))) => (),
            else => {
                *self.stats.as_mut().expect("stats lock").missed_expiry_tasks.get_mut() += 1;
            }
        }
    }

    pub async fn enqueue_by_newer_noncurrent(&mut self, bucket: &str, versions: Vec<ObjectToDelete>, lc_event: lifecycle::Event) {
        if versions.is_empty() {
            return;
        }

        let task = NewerNoncurrentTask {
            bucket: String::from(bucket),
            versions,
            event: lc_event,
        };
        let wrkr = self.get_worker_ch(task.op_hash());
        if wrkr.is_none() {
            *self.stats.as_mut().expect("stats lock").missed_expiry_tasks.get_mut() += 1;
            return;
        }
        let wrkr = wrkr.expect("worker channel should exist after None check");
        select! {
            //_ -> GlobalContext.Done() => {}
            _ = wrkr.send(Some(Box::new(task))) => (),
            else => {
                *self.stats.as_mut().expect("stats lock").missed_expiry_tasks.get_mut() += 1;
            }
        }
    }

    pub fn get_worker_ch(&self, h: u64) -> Option<Sender<Option<ExpiryOpType>>> {
        if self.tasks_tx.is_empty() {
            return None;
        }
        Some(self.tasks_tx[h as usize % self.tasks_tx.len()].clone())
    }

    pub fn increment_missed_tier_journal_tasks(&mut self) {
        *self.stats.as_mut().expect("stats lock").missed_tier_journal_tasks.get_mut() += 1;
    }

    pub async fn resize_workers(n: usize, api: Arc<ECStore>) {
        if n == GLOBAL_ExpiryState.read().await.tasks_tx.len() || n < 1 {
            return;
        }

        let mut state = GLOBAL_ExpiryState.write().await;

        while state.tasks_tx.len() < n {
            let (tx, rx) = mpsc::channel(1000);
            let api = api.clone();
            let rx = Arc::new(tokio::sync::Mutex::new(rx));
            state.tasks_tx.push(tx);
            state.tasks_rx.push(rx.clone());
            *state.stats.as_mut().expect("stats lock").workers.get_mut() += 1;
            tokio::spawn(async move {
                let mut rx = rx.lock().await;
                //let mut expiry_state = GLOBAL_ExpiryState.read().await;
                ExpiryState::worker(&mut rx, api).await;
            });
        }

        let mut l = state.tasks_tx.len();
        while l > n {
            let worker = state.tasks_tx[l - 1].clone();
            worker.send(None).await.unwrap_or(());
            state.tasks_tx.remove(l - 1);
            state.tasks_rx.remove(l - 1);
            *state.stats.as_mut().expect("stats lock").workers.get_mut() -= 1;
            l -= 1;
        }
    }

    pub async fn worker(rx: &mut Receiver<Option<ExpiryOpType>>, api: Arc<ECStore>) {
        let cancel_token = crate::global::get_background_services_cancel_token().unwrap_or_else(|| {
            static FALLBACK: std::sync::OnceLock<tokio_util::sync::CancellationToken> = std::sync::OnceLock::new();
            FALLBACK.get_or_init(tokio_util::sync::CancellationToken::new)
        });

        loop {
            select! {
                _ = cancel_token.cancelled() => {
                    info!("lifecycle expiry worker received shutdown signal, exiting");
                    break;
                }
                v = rx.recv() => {
                    if v.is_none() {
                        break;
                    }
                    let v = v.expect("channel closed unexpectedly");
                    if v.is_none() {
                        //rx.close();
                        //drop(rx);
                        let _ = rx;
                        return;
                    }
                    let v = v.expect("received None after None check");
                    if v.as_any().is::<ExpiryTask>() {
                        let v = v.as_any().downcast_ref::<ExpiryTask>().expect("ExpiryTask downcast failed");
                        //debug!("lifecycle expiry worker received task: {:?}", v.obj_info);
                        if !v.obj_info.transitioned_object.status.is_empty() {
                            apply_expiry_on_transitioned_object(api.clone(), &v.obj_info, &v.event, &v.src).await;
                        } else {
                            apply_expiry_on_non_transitioned_objects(api.clone(), &v.obj_info, &v.event, &v.src).await;
                        }
                    }
                    else if v.as_any().is::<NewerNoncurrentTask>() {
                        let v = v.as_any().downcast_ref::<NewerNoncurrentTask>().expect("NewerNoncurrentTask downcast failed");
                        crate::client::object_handlers_common::delete_object_versions(&api, &v.bucket, &v.versions, v.event.clone()).await;
                    }
                    else if v.as_any().is::<Jentry>() {
                        let v = v.as_any().downcast_ref::<Jentry>().expect("Jentry downcast failed");
                        if let Err(err) = delete_object_from_remote_tier(&v.obj_name, &v.version_id, &v.tier_name).await {
                            warn!(
                                object = %v.obj_name,
                                version_id = %v.version_id,
                                tier = %v.tier_name,
                                error = ?err,
                                "failed to delete transitioned object from remote tier"
                            );
                        }
                    }
                    else if v.as_any().is::<FreeVersionTask>() {
                        let v = v.as_any().downcast_ref::<FreeVersionTask>().expect("FreeVersionTask downcast failed");
                        let oi = v.0.clone();
                        if let Err(err) = delete_object_from_remote_tier(
                            &oi.transitioned_object.name,
                            &oi.transitioned_object.version_id,
                            &oi.transitioned_object.tier,
                        )
                        .await
                        {
                            warn!(
                                bucket = %oi.bucket,
                                object = %oi.name,
                                remote_object = %oi.transitioned_object.name,
                                remote_version_id = %oi.transitioned_object.version_id,
                                tier = %oi.transitioned_object.tier,
                                error = ?err,
                                "failed to sweep transitioned free version from remote tier"
                            );
                            continue;
                        }

                        let mut fi = FileInfo {
                            name: oi.name.clone(),
                            version_id: oi.version_id,
                            deleted: true,
                            ..Default::default()
                        };
                        fi.set_tier_free_version();

                        let mut deleted_locally = false;
                        for pool in api.pools.iter() {
                            let set = pool.get_disks_by_key(&oi.name);
                            match set.delete_object_version(&oi.bucket, &oi.name, &fi, false).await {
                                Ok(()) => {
                                    deleted_locally = true;
                                    break;
                                }
                                Err(err) if is_err_version_not_found(&err) || is_err_object_not_found(&err) => continue,
                                Err(err) => {
                                    warn!(
                                        bucket = %oi.bucket,
                                        object = %oi.name,
                                        remote_object = %oi.transitioned_object.name,
                                        remote_version_id = %oi.transitioned_object.version_id,
                                        tier = %oi.transitioned_object.tier,
                                        error = ?err,
                                        "failed to delete transitioned free version after remote tier sweep"
                                    );
                                    break;
                                }
                            }
                        }

                        if !deleted_locally {
                            warn!(
                                bucket = %oi.bucket,
                                object = %oi.name,
                                remote_object = %oi.transitioned_object.name,
                                remote_version_id = %oi.transitioned_object.version_id,
                                tier = %oi.transitioned_object.tier,
                                "transitioned free version was not found during local cleanup"
                            );
                        }
                    }
                    else {
                        //info!("Invalid work type - {:?}", v);
                        warn!("lifecycle worker received unsupported operation type");
                    }
                }
            }
        }
    }
}

struct TransitionTask {
    obj_info: ObjectInfo,
    src: LcEventSrc,
    event: lifecycle::Event,
}

impl ExpiryOp for TransitionTask {
    fn op_hash(&self) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(self.obj_info.bucket.as_bytes());
        // hasher.update(format!("{}", self.obj_info.versions[0].object_name).as_bytes());
        xxh64::xxh64(hasher.finalize().as_slice(), XXHASH_SEED)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct TransitionState {
    transition_tx: A_Sender<Option<TransitionTask>>,
    transition_rx: A_Receiver<Option<TransitionTask>>,
    pub num_workers: AtomicI64,
    kill_tx: A_Sender<()>,
    kill_rx: A_Receiver<()>,
    active_tasks: AtomicI64,
    missed_immediate_tasks: AtomicI64,
    last_day_stats: Arc<Mutex<HashMap<String, LastDayTierStats>>>,
}

impl TransitionState {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Arc<Self> {
        let (tx1, rx1) = bounded(1000);
        let (tx2, rx2) = bounded(1);
        Arc::new(Self {
            transition_tx: tx1,
            transition_rx: rx1,
            num_workers: AtomicI64::new(0),
            kill_tx: tx2,
            kill_rx: rx2,
            active_tasks: AtomicI64::new(0),
            missed_immediate_tasks: AtomicI64::new(0),
            last_day_stats: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn queue_transition_task(&self, oi: &ObjectInfo, event: &lifecycle::Event, src: &LcEventSrc) {
        let task = TransitionTask {
            obj_info: oi.clone(),
            src: src.clone(),
            event: event.clone(),
        };
        select! {
            //_ -> t.ctx.Done() => (),
            _ = self.transition_tx.send(Some(task)) => (),
            else => {
                match src {
                    LcEventSrc::S3PutObject | LcEventSrc::S3CopyObject | LcEventSrc::S3CompleteMultipartUpload => {
                        self.missed_immediate_tasks.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => ()
                }
            },
        }
    }

    pub async fn init(api: Arc<ECStore>) {
        let max_workers = get_env_i64("RUSTFS_MAX_TRANSITION_WORKERS", std::cmp::min(num_cpus::get() as i64, 16));
        let mut n = max_workers;
        let tw = 8; //globalILMConfig.getTransitionWorkers();
        if tw > 0 {
            n = tw;
        }

        //let mut transition_state = GLOBAL_TransitionState.write().await;
        //self.objAPI = objAPI
        Self::update_workers(api, n).await;
    }

    pub fn pending_tasks(&self) -> usize {
        //let transition_rx = GLOBAL_TransitionState.transition_rx.lock().unwrap();
        let transition_rx = &GLOBAL_TransitionState.transition_rx;
        transition_rx.len()
    }

    pub fn active_tasks(&self) -> i64 {
        self.active_tasks.load(Ordering::SeqCst)
    }

    pub fn missed_immediate_tasks(&self) -> i64 {
        self.missed_immediate_tasks.load(Ordering::SeqCst)
    }

    pub async fn worker(api: Arc<ECStore>) {
        loop {
            select! {
                _ = GLOBAL_TransitionState.kill_rx.recv() => {
                    return;
                }
                task = GLOBAL_TransitionState.transition_rx.recv() => {
                    if task.is_err() {
                        break;
                    }
                    let task = task.expect("channel recv should succeed after error check");
                    if task.is_none() {
                        //self.transition_rx.close();
                        //drop(self.transition_rx);
                        return;
                    }
                    let task = task.expect("received None after None check");
                    if task.as_any().is::<TransitionTask>() {
                        let task = task.as_any().downcast_ref::<TransitionTask>().expect("TransitionTask downcast failed");

                        GLOBAL_TransitionState.active_tasks.fetch_add(1, Ordering::SeqCst);

                        let obj_info_for_event = ObjectInfo {
                            bucket: task.obj_info.bucket.clone(),
                            name: task.obj_info.name.clone(),
                            size: task.obj_info.size,
                            version_id: task.obj_info.version_id,
                            ..Default::default()
                        };

                        if let Err(err) = transition_object(api.clone(), &task.obj_info, LcAuditEvent::new(task.event.clone(), task.src.clone())).await {
                            if !is_err_version_not_found(&err) && !is_err_object_not_found(&err) && !is_network_or_host_down(&err.to_string(), false) && !err.to_string().contains("use of closed network connection") {
                                error!("Transition to {} failed for {}/{} version:{} with {}",
                                    task.event.storage_class, task.obj_info.bucket, task.obj_info.name, task.obj_info.version_id.map(|v| v.to_string()).unwrap_or_default(), err.to_string());
                            }
                            // Send s3:ObjectTransition:Failed event
                            send_event(EventArgs {
                                event_name: EventName::ObjectTransitionFailed.to_string(),
                                bucket_name: obj_info_for_event.bucket.clone(),
                                object: obj_info_for_event,
                                user_agent: "Internal: [ILM-Transition]".to_string(),
                                host: GLOBAL_LocalNodeName.to_string(),
                                ..Default::default()
                            });
                        } else {
                            let mut ts = TierStats {
                                total_size: task.obj_info.size as u64,
                                num_versions: 1,
                                ..Default::default()
                            };
                            if task.obj_info.is_latest {
                                ts.num_objects = 1;
                            }
                            GLOBAL_TransitionState.add_lastday_stats(&task.event.storage_class, ts);

                            // Send s3:ObjectTransition:Complete event
                            send_event(EventArgs {
                                event_name: EventName::ObjectTransitionComplete.to_string(),
                                bucket_name: obj_info_for_event.bucket.clone(),
                                object: obj_info_for_event,
                                user_agent: "Internal: [ILM-Transition]".to_string(),
                                host: GLOBAL_LocalNodeName.to_string(),
                                ..Default::default()
                            });
                        }
                        GLOBAL_TransitionState.active_tasks.fetch_add(-1, Ordering::SeqCst);
                    }
                }
                else => ()
            }
        }
    }

    pub fn add_lastday_stats(&self, tier: &str, ts: TierStats) {
        let mut tier_stats = self.last_day_stats.lock().unwrap();
        tier_stats
            .entry(tier.to_string())
            .and_modify(|e| e.add_stats(ts))
            .or_default();
    }

    pub fn get_daily_all_tier_stats(&self) -> DailyAllTierStats {
        let tier_stats = self.last_day_stats.lock().unwrap();
        let mut res = DailyAllTierStats::with_capacity(tier_stats.len());
        for (tier, st) in tier_stats.iter() {
            res.insert(tier.clone(), st.clone());
        }
        res
    }

    pub async fn update_workers(api: Arc<ECStore>, n: i64) {
        Self::update_workers_inner(api, n).await;
    }

    pub async fn update_workers_inner(api: Arc<ECStore>, n: i64) {
        let mut n = n;
        if n == 0 {
            let max_workers = get_env_i64("RUSTFS_MAX_TRANSITION_WORKERS", std::cmp::min(num_cpus::get() as i64, 16));
            n = max_workers;
        }
        // Allow environment override of maximum workers
        let absolute_max = get_env_i64("RUSTFS_ABSOLUTE_MAX_WORKERS", 32);
        n = std::cmp::min(n, absolute_max);

        let mut num_workers = GLOBAL_TransitionState.num_workers.load(Ordering::SeqCst);
        while num_workers < n {
            let clone_api = api.clone();
            tokio::spawn(async move {
                TransitionState::worker(clone_api).await;
            });
            num_workers += 1;
            GLOBAL_TransitionState.num_workers.fetch_add(1, Ordering::SeqCst);
        }

        let mut num_workers = GLOBAL_TransitionState.num_workers.load(Ordering::SeqCst);
        while num_workers > n {
            let worker = GLOBAL_TransitionState.kill_tx.clone();
            let _ = worker.send(()).await;
            num_workers -= 1;
            GLOBAL_TransitionState.num_workers.fetch_add(-1, Ordering::SeqCst);
        }
    }
}

pub async fn init_background_expiry(api: Arc<ECStore>) {
    let mut workers = get_env_usize("RUSTFS_MAX_EXPIRY_WORKERS", std::cmp::min(num_cpus::get(), 16));
    //globalILMConfig.getExpirationWorkers()
    if let Ok(env_expiration_workers) = env::var("_RUSTFS_ILM_EXPIRATION_WORKERS")
        && let Ok(num_expirations) = env_expiration_workers.parse::<usize>()
    {
        workers = num_expirations;
    }

    if workers == 0 {
        workers = get_env_usize("RUSTFS_DEFAULT_EXPIRY_WORKERS", 8);
    }

    //let expiry_state = GLOBAL_ExpiryStSate.write().await;
    ExpiryState::resize_workers(workers, api).await;
}

#[derive(Debug, Clone)]
struct StaleMultipartUploadCandidate {
    path: String,
    initiated: OffsetDateTime,
    metadata: Option<HashMap<String, String>>,
}

fn parse_stale_uploads_duration(env_key: &str, default: StdDuration) -> StdDuration {
    env::var(env_key)
        .ok()
        .and_then(|value| rustfs_madmin::utils::parse_duration(&value).ok())
        .filter(|duration| !duration.is_zero())
        .unwrap_or(default)
}

fn stale_uploads_expiry() -> StdDuration {
    parse_stale_uploads_duration(ENV_STALE_UPLOADS_EXPIRY, DEFAULT_STALE_UPLOADS_EXPIRY)
}

fn stale_uploads_cleanup_interval() -> StdDuration {
    parse_stale_uploads_duration(ENV_STALE_UPLOADS_CLEANUP_INTERVAL, DEFAULT_STALE_UPLOADS_CLEANUP_INTERVAL)
}

fn encode_stale_upload_id(upload_uuid: &str) -> String {
    base64_simd::URL_SAFE_NO_PAD
        .encode_to_string(format!("{}.{}", get_global_deployment_id().unwrap_or_default(), upload_uuid).as_bytes())
}

fn initiated_from_upload_dir(upload_dir: &str, fallback: Option<OffsetDateTime>) -> OffsetDateTime {
    upload_dir
        .split_once('x')
        .and_then(|(_, nanos)| nanos.parse::<i128>().ok())
        .and_then(|nanos| OffsetDateTime::from_unix_timestamp_nanos(nanos).ok())
        .or(fallback)
        .unwrap_or_else(OffsetDateTime::now_utc)
}

fn stale_upload_default_due(initiated: OffsetDateTime, default_expiry: StdDuration) -> OffsetDateTime {
    initiated + time::Duration::seconds(default_expiry.as_secs() as i64)
}

async fn stale_upload_current_size(set: &Arc<SetDisks>, metadata: &HashMap<String, String>, upload_dir: &str) -> Option<usize> {
    let bucket = metadata.get(RUSTFS_MULTIPART_BUCKET_KEY)?;
    let object = metadata.get(RUSTFS_MULTIPART_OBJECT_KEY)?;
    let upload_id = encode_stale_upload_id(upload_dir);
    let parts = set
        .list_object_parts(bucket, object, &upload_id, None, MAX_PARTS_COUNT, &ObjectOptions::default())
        .await
        .ok()?;

    Some(
        parts
            .parts
            .iter()
            .map(|part| part.actual_size.max(part.size as i64).max(0) as usize)
            .sum(),
    )
}

async fn stale_upload_lifecycle_due(
    set: &Arc<SetDisks>,
    metadata: &HashMap<String, String>,
    initiated: OffsetDateTime,
    upload_dir: &str,
) -> Option<OffsetDateTime> {
    let bucket = metadata.get(RUSTFS_MULTIPART_BUCKET_KEY)?;
    let object = metadata.get(RUSTFS_MULTIPART_OBJECT_KEY)?;

    let lifecycle = match metadata_sys::get_lifecycle_config(bucket).await {
        Ok((lifecycle, _)) => lifecycle,
        Err(_) => return None,
    };

    let object_opts = ObjectOpts {
        name: object.clone(),
        user_tags: metadata.get(AMZ_OBJECT_TAGGING).cloned().unwrap_or_default(),
        mod_time: Some(initiated),
        size: stale_upload_current_size(set, metadata, upload_dir).await.unwrap_or_default(),
        is_latest: true,
        delete_marker: false,
        user_defined: metadata.clone(),
        ..Default::default()
    };

    abort_incomplete_multipart_upload_due(&lifecycle, &object_opts)
        .await
        .map(|(due, _)| due)
}

async fn read_stale_multipart_candidate(
    disk: &Disk,
    sha_dir: &str,
    upload_dir: &str,
) -> Result<StaleMultipartUploadCandidate, DiskError> {
    let metadata_path = format!("{sha_dir}/{upload_dir}/{STORAGE_FORMAT_FILE}");
    let metadata_bytes = disk.read_metadata(RUSTFS_META_MULTIPART_BUCKET, &metadata_path).await?;

    let (metadata, mod_time) = match get_file_info(
        &metadata_bytes,
        RUSTFS_META_MULTIPART_BUCKET,
        &metadata_path,
        "",
        FileInfoOpts {
            data: false,
            include_free_versions: false,
        },
    ) {
        Ok(file_info) => (Some(file_info.metadata), file_info.mod_time),
        Err(err) => {
            warn!(path = %metadata_path, error = ?err, "failed to parse multipart metadata during stale cleanup");
            (None, None)
        }
    };

    let initiated = initiated_from_upload_dir(upload_dir, mod_time);

    Ok(StaleMultipartUploadCandidate {
        path: format!("{sha_dir}/{upload_dir}"),
        initiated,
        metadata,
    })
}

fn merge_stale_multipart_candidate(
    candidates: &mut HashMap<String, StaleMultipartUploadCandidate>,
    candidate: StaleMultipartUploadCandidate,
) {
    match candidates.get(&candidate.path) {
        Some(existing) if existing.metadata.is_some() => {}
        Some(existing) if existing.metadata.is_none() && candidate.metadata.is_none() => {}
        _ => {
            candidates.insert(candidate.path.clone(), candidate);
        }
    }
}

async fn cleanup_empty_multipart_sha_dirs_on_local_disks(set: &Arc<SetDisks>) {
    for disk in set.get_local_disks().await.into_iter().flatten() {
        if !disk.is_online().await {
            continue;
        }

        let sha_dirs = match disk
            .list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, "", -1)
            .await
        {
            Ok(entries) => entries,
            Err(err) => {
                if err != DiskError::FileNotFound && err != DiskError::VolumeNotFound {
                    warn!(error = ?err, "failed to list multipart root during empty sha cleanup");
                }
                continue;
            }
        };

        for sha_dir in sha_dirs {
            let sha_dir = sha_dir.trim_end_matches('/').to_string();
            let upload_dirs = match disk
                .list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, &sha_dir, -1)
                .await
            {
                Ok(entries) => entries,
                Err(err) => {
                    if err != DiskError::FileNotFound && err != DiskError::VolumeNotFound {
                        warn!(sha_dir = %sha_dir, error = ?err, "failed to list multipart sha dir during empty sha cleanup");
                    }
                    continue;
                }
            };

            if !upload_dirs.is_empty() {
                continue;
            }

            if let Err(err) = disk
                .delete(RUSTFS_META_MULTIPART_BUCKET, &sha_dir, DeleteOptions::default())
                .await
                && err != DiskError::FileNotFound
                && err != DiskError::VolumeNotFound
            {
                warn!(sha_dir = %sha_dir, error = ?err, "failed to remove empty multipart sha dir");
            }
        }
    }
}

async fn cleanup_stale_multipart_uploads_in_set(set: &Arc<SetDisks>, now: OffsetDateTime, default_expiry: StdDuration) -> usize {
    let mut deleted = 0usize;
    let mut candidates = HashMap::new();

    for disk in set.get_local_disks().await.into_iter().flatten() {
        if !disk.is_online().await {
            continue;
        }

        let sha_dirs = match disk
            .list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, "", -1)
            .await
        {
            Ok(entries) => entries,
            Err(err) => {
                if err != DiskError::FileNotFound && err != DiskError::VolumeNotFound {
                    warn!(error = ?err, "failed to list multipart root during stale cleanup");
                }
                continue;
            }
        };

        for sha_dir in sha_dirs {
            let sha_dir = sha_dir.trim_end_matches('/').to_string();
            let upload_dirs = match disk
                .list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, &sha_dir, -1)
                .await
            {
                Ok(entries) => entries,
                Err(err) => {
                    if err != DiskError::FileNotFound && err != DiskError::VolumeNotFound {
                        warn!(sha_dir = %sha_dir, error = ?err, "failed to list multipart sha dir during stale cleanup");
                    }
                    continue;
                }
            };

            for upload_dir in upload_dirs {
                let upload_dir = upload_dir.trim_end_matches('/').to_string();
                let candidate_path = format!("{sha_dir}/{upload_dir}");
                if candidates
                    .get(&candidate_path)
                    .is_some_and(|existing: &StaleMultipartUploadCandidate| existing.metadata.is_some())
                {
                    continue;
                }

                let candidate = match read_stale_multipart_candidate(disk.as_ref(), &sha_dir, &upload_dir).await {
                    Ok(candidate) => candidate,
                    Err(err) => {
                        if err != DiskError::FileNotFound {
                            warn!(path = %candidate_path, error = ?err, "failed to read multipart metadata during stale cleanup");
                        }
                        let initiated = initiated_from_upload_dir(&upload_dir, None);
                        StaleMultipartUploadCandidate {
                            path: candidate_path,
                            initiated,
                            metadata: None,
                        }
                    }
                };
                merge_stale_multipart_candidate(&mut candidates, candidate);
            }
        }
    }

    for candidate in candidates.into_values() {
        let upload_dir = candidate.path.rsplit('/').next().unwrap_or_default().to_string();
        let mut due = stale_upload_default_due(candidate.initiated, default_expiry);
        if let Some(metadata) = candidate.metadata.as_ref()
            && let Some(lifecycle_due) = stale_upload_lifecycle_due(set, metadata, candidate.initiated, &upload_dir).await
            && lifecycle_due < due
        {
            due = lifecycle_due;
        }

        if now < due {
            continue;
        }

        match set.delete_all(RUSTFS_META_MULTIPART_BUCKET, &candidate.path).await {
            Ok(()) => {
                deleted += 1;
                let upload_id = encode_stale_upload_id(&upload_dir);
                if let Some(metadata) = candidate.metadata.as_ref() {
                    info!(
                        bucket = metadata.get(RUSTFS_MULTIPART_BUCKET_KEY).cloned().unwrap_or_default(),
                        object = metadata.get(RUSTFS_MULTIPART_OBJECT_KEY).cloned().unwrap_or_default(),
                        upload_id = %upload_id,
                        due = ?due,
                        "removed stale multipart upload"
                    );
                } else {
                    info!(path = %candidate.path, upload_id = %upload_id, due = ?due, "removed stale multipart upload");
                }
            }
            Err(err) => warn!(path = %candidate.path, error = ?err, "failed to remove stale multipart upload"),
        }
    }

    cleanup_empty_multipart_sha_dirs_on_local_disks(set).await;

    deleted
}

async fn cleanup_stale_multipart_uploads_once_at(api: Arc<ECStore>, now: OffsetDateTime, default_expiry: StdDuration) -> usize {
    let mut deleted = 0usize;
    for pool in &api.pools {
        for set in &pool.disk_set {
            deleted += cleanup_stale_multipart_uploads_in_set(set, now, default_expiry).await;
        }
    }
    deleted
}

pub async fn run_stale_multipart_upload_cleanup_once(api: Arc<ECStore>) -> usize {
    cleanup_stale_multipart_uploads_once_at(api, OffsetDateTime::now_utc(), stale_uploads_expiry()).await
}

pub fn init_background_stale_multipart_upload_cleanup(api: Arc<ECStore>) {
    let cleanup_interval = stale_uploads_cleanup_interval();
    let default_expiry = stale_uploads_expiry();
    let api = Arc::downgrade(&api);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_interval);

        loop {
            interval.tick().await;

            let Some(api) = Weak::upgrade(&api) else {
                return;
            };

            let deleted = cleanup_stale_multipart_uploads_once_at(api, OffsetDateTime::now_utc(), default_expiry).await;
            if deleted > 0 {
                info!(deleted, "completed stale multipart cleanup pass");
            }
        }
    });
}

pub async fn validate_transition_tier(lc: &BucketLifecycleConfiguration) -> Result<(), std::io::Error> {
    for rule in &lc.rules {
        if let Some(transitions) = &rule.transitions {
            for transition in transitions {
                if let Some(storage_class) = &transition.storage_class
                    && storage_class.as_str() != ""
                {
                    let valid = GLOBAL_TierConfigMgr.read().await.is_tier_valid(storage_class.as_str());
                    if !valid {
                        return Err(std::io::Error::other(ERR_INVALID_STORAGECLASS));
                    }
                }
            }
        }
        if let Some(noncurrent_version_transitions) = &rule.noncurrent_version_transitions {
            for noncurrent_version_transition in noncurrent_version_transitions {
                if let Some(storage_class) = &noncurrent_version_transition.storage_class
                    && storage_class.as_str() != ""
                {
                    let valid = GLOBAL_TierConfigMgr.read().await.is_tier_valid(storage_class.as_str());
                    if !valid {
                        return Err(std::io::Error::other(ERR_INVALID_STORAGECLASS));
                    }
                }
            }
        }
    }
    Ok(())
}

fn mark_delete_opts_skip_decommissioned_on_remote_success(opts: &mut ObjectOptions, remote_delete_succeeded: bool) {
    if remote_delete_succeeded {
        opts.skip_decommissioned = true;
    }
}

pub async fn enqueue_transition_immediate(oi: &ObjectInfo, src: LcEventSrc) {
    if let Some(lc) = GLOBAL_LifecycleSys.get(&oi.bucket).await {
        enqueue_transition_with_lifecycle(oi, &lc, &src).await;
    }
}

pub async fn enqueue_immediate_expiry(oi: &ObjectInfo, src: LcEventSrc) {
    let Some(lifecycle) = GLOBAL_LifecycleSys.get(&oi.bucket).await else {
        return;
    };
    let Some(api) = crate::new_object_layer_fn() else {
        return;
    };

    let mut marker = None;
    let mut version_marker = None;
    let mut object_infos = Vec::new();

    loop {
        let Ok(page) = api
            .clone()
            .list_object_versions(&oi.bucket, &oi.name, marker.clone(), version_marker.clone(), None, 1000)
            .await
        else {
            return;
        };

        object_infos.extend(page.objects.into_iter().filter(|object| object.name == oi.name));

        if !page.is_truncated {
            break;
        }

        marker = page.next_marker;
        version_marker = page.next_version_idmarker;
    }

    if object_infos.is_empty() {
        object_infos.push(oi.clone());
    }

    let lock_config = match metadata_sys::get_object_lock_config(&oi.bucket).await {
        Ok((cfg, _)) => Some(Arc::new(cfg)),
        Err(_) => None,
    };
    let replication = match metadata_sys::get_replication_config(&oi.bucket).await {
        Ok((cfg, _)) if !cfg.rules.is_empty() => Some(Arc::new(ReplicationConfig::new(Some(cfg), None))),
        _ => None,
    };

    let object_opts = object_infos
        .iter()
        .map(|object| object.to_lifecycle_opts())
        .collect::<Vec<ObjectOpts>>();
    let Ok(events) = Evaluator::new(Arc::new(lifecycle))
        .with_lock_retention(lock_config)
        .with_replication_config(replication)
        .eval(&object_opts)
        .await
    else {
        return;
    };

    let mut to_delete_objs = Vec::new();
    let mut noncurrent_event = None;

    for (object, event) in object_infos.iter().zip(events.iter()) {
        if event.due != Some(OffsetDateTime::UNIX_EPOCH) {
            continue;
        }

        match event.action {
            IlmAction::DeleteAction
            | IlmAction::DeleteRestoredAction
            | IlmAction::DeleteRestoredVersionAction
            | IlmAction::DeleteAllVersionsAction
            | IlmAction::DelMarkerDeleteAllVersionsAction => {
                apply_expiry_rule(event, &src, object).await;
            }
            IlmAction::DeleteVersionAction => {
                to_delete_objs.push(ObjectToDelete {
                    object_name: object.name.clone(),
                    version_id: object.version_id,
                    ..Default::default()
                });
                if noncurrent_event.is_none() {
                    noncurrent_event = Some(event.clone());
                }
            }
            _ => {}
        }
    }

    if !to_delete_objs.is_empty()
        && let Some(event) = noncurrent_event
    {
        GLOBAL_ExpiryState
            .write()
            .await
            .enqueue_by_newer_noncurrent(&oi.bucket, to_delete_objs, event)
            .await;
    }
}

pub async fn enqueue_transition_for_existing_objects(api: Arc<ECStore>, bucket: &str) -> Result<(), Error> {
    let Some(lc) = GLOBAL_LifecycleSys.get(bucket).await else {
        return Ok(());
    };
    let mut marker = None;
    let mut version_marker = None;
    let src = LcEventSrc::Scanner;

    loop {
        let page = api
            .clone()
            .list_object_versions(bucket, "", marker.clone(), version_marker.clone(), None, 1000)
            .await?;

        for object in &page.objects {
            enqueue_transition_with_lifecycle(object, &lc, &src).await;
        }

        if !page.is_truncated {
            return Ok(());
        }

        marker = page.next_marker;
        version_marker = page.next_version_idmarker;
    }
}

pub async fn enqueue_expiry_for_existing_objects(api: Arc<ECStore>, bucket: &str) -> Result<(), Error> {
    let Ok((lc, _)) = metadata_sys::get_lifecycle_config(bucket).await else {
        return Ok(());
    };
    let lock_retention = metadata_sys::get_object_lock_config(bucket)
        .await
        .ok()
        .and_then(|(cfg, _)| cfg.rule.and_then(|rule| rule.default_retention));
    let replication_config = metadata_sys::get_replication_config(bucket).await.ok();
    let mut marker = None;
    let mut version_marker = None;
    let src = LcEventSrc::Scanner;

    loop {
        let page = api
            .clone()
            .list_object_versions(bucket, "", marker.clone(), version_marker.clone(), None, 1000)
            .await?;

        for object in &page.objects {
            let event = eval_action_from_lifecycle(&lc, lock_retention.clone(), replication_config.clone(), object).await;
            match event.action {
                IlmAction::DeleteAction
                | IlmAction::DeleteVersionAction
                | IlmAction::DeleteRestoredAction
                | IlmAction::DeleteRestoredVersionAction
                | IlmAction::DeleteAllVersionsAction
                | IlmAction::DelMarkerDeleteAllVersionsAction => {
                    if event
                        .due
                        .is_some_and(|due| due.unix_timestamp() <= OffsetDateTime::now_utc().unix_timestamp())
                    {
                        if object.is_remote() {
                            apply_expiry_on_transitioned_object(api.clone(), object, &event, &src).await;
                        } else {
                            apply_expiry_on_non_transitioned_objects(api.clone(), object, &event, &src).await;
                        }
                    } else {
                        apply_expiry_rule(&event, &src, object).await;
                    }
                }
                _ => {}
            }
        }

        if !page.is_truncated {
            return Ok(());
        }

        marker = page.next_marker;
        version_marker = page.next_version_idmarker;
    }
}

async fn enqueue_transition_with_lifecycle(oi: &ObjectInfo, lc: &BucketLifecycleConfiguration, src: &LcEventSrc) {
    let event = lc.eval(&oi.to_lifecycle_opts()).await;
    match event.action {
        IlmAction::TransitionAction | IlmAction::TransitionVersionAction => {
            if oi.delete_marker || oi.is_dir {
                return;
            }
            GLOBAL_TransitionState.queue_transition_task(oi, &event, src).await;
        }
        _ => (),
    }
}

pub async fn expire_transitioned_object(
    api: Arc<ECStore>,
    oi: &ObjectInfo,
    lc_event: &lifecycle::Event,
    _src: &LcEventSrc,
) -> Result<ObjectInfo, std::io::Error> {
    //let traceFn = GLOBAL_LifecycleSys.trace(oi);
    let mut opts = ObjectOptions {
        versioned: BucketVersioningSys::prefix_enabled(&oi.bucket, &oi.name).await,
        expiration: ExpirationOptions { expire: true },
        ..Default::default()
    };
    if lc_event.action == IlmAction::DeleteVersionAction {
        opts.version_id = oi.version_id.map(|id| id.to_string());
    }
    //let tags = LcAuditEvent::new(src, lcEvent).Tags();
    if lc_event.action == IlmAction::DeleteRestoredAction {
        opts.transition.expire_restored = true;
        return match api.delete_object(&oi.bucket, &oi.name, opts).await {
            Ok(dobj) => {
                //audit_log_lifecycle(*oi, ILMExpiry, tags, traceFn);
                Ok(dobj)
            }
            Err(err) => Err(std::io::Error::other(err)),
        };
    }

    let ret = delete_object_from_remote_tier(
        &oi.transitioned_object.name,
        &oi.transitioned_object.version_id,
        &oi.transitioned_object.tier,
    )
    .await;
    if let Err(e) = &ret {
        error!("Failed to delete remote transitioned object {}: {:?}", oi.transitioned_object.name, e);
    }
    mark_delete_opts_skip_decommissioned_on_remote_success(&mut opts, ret.is_ok());

    let dobj = match api.delete_object(&oi.bucket, &oi.name, opts).await {
        Ok(obj) => obj,
        Err(e) => {
            error!("Failed to delete transitioned object {}/{}: {:?}", oi.bucket, oi.name, e);
            // Return the original object info if deletion fails
            oi.clone()
        }
    };

    schedule_lifecycle_replication_delete_if_needed(oi, &dobj).await;

    //audit_log_lifecycle(oi, ILMExpiry, tags);

    let event_name = if oi.delete_marker {
        EventName::LifecycleExpirationDelete
    } else if dobj.delete_marker {
        EventName::LifecycleExpirationDeleteMarkerCreated
    } else {
        EventName::LifecycleExpirationDelete
    };
    let obj_info = ObjectInfo {
        bucket: oi.bucket.clone(),
        name: oi.name.clone(),
        size: oi.size,
        version_id: oi.version_id,
        delete_marker: oi.delete_marker,
        ..Default::default()
    };
    send_event(EventArgs {
        event_name: event_name.to_string(),
        bucket_name: obj_info.bucket.clone(),
        object: obj_info,
        user_agent: "Internal: [ILM-Expiry]".to_string(),
        host: GLOBAL_LocalNodeName.to_string(),
        ..Default::default()
    });
    /*let system = match notification_system() {
        Some(sys) => sys,
        None => {
            let config = Config::new();
            initialize(config).await?;
            notification_system().expect("Failed to initialize notification system")
        }
    };
    let event = Arc::new(Event::new_test_event("my-bucket", "document.pdf", EventName::ObjectCreatedPut));
    system.send_event(event).await;*/

    Ok(dobj)
}

pub fn gen_transition_objname(bucket: &str) -> Result<String, Error> {
    let us = Uuid::new_v4().to_string();
    let mut hasher = Sha256::new();
    hasher.update(format!("{}/{}", get_global_deployment_id().unwrap_or_default(), bucket).as_bytes());
    let hash = rustfs_utils::crypto::hex(hasher.finalize().as_slice());
    let obj = format!("{}/{}/{}/{}", &hash[0..16], &us[0..2], &us[2..4], &us);
    Ok(obj)
}

pub async fn transition_object(api: Arc<ECStore>, oi: &ObjectInfo, lae: LcAuditEvent) -> Result<(), Error> {
    let time_ilm = Metrics::time_ilm(lae.event.action);

    let etag = if let Some(etag) = &oi.etag { etag } else { "" };
    let etag = etag.to_string();

    let opts = ObjectOptions {
        transition: TransitionOptions {
            status: lifecycle::TRANSITION_PENDING.to_string(),
            tier: lae.event.storage_class,
            etag,
            ..Default::default()
        },
        //lifecycle_audit_event: lae,
        version_id: oi.version_id.map(|v| v.to_string()),
        versioned: BucketVersioningSys::prefix_enabled(&oi.bucket, &oi.name).await,
        version_suspended: BucketVersioningSys::prefix_suspended(&oi.bucket, &oi.name).await,
        mod_time: oi.mod_time,
        ..Default::default()
    };
    let result = api.transition_object(&oi.bucket, &oi.name, &opts).await;
    time_ilm(1)();
    result
}

pub fn audit_tier_actions(_tier: &str, bytes: i64) -> TimeFn {
    let tier = _tier.to_string();
    Arc::new(move || {
        let tier = tier.clone();
        Box::pin(async move {
            info!(
                tier = %tier,
                bytes = bytes,
                "ILM tier transition audit: completed transition of {} bytes to tier '{}'",
                bytes, tier
            );
        })
    })
}

pub async fn get_transitioned_object_reader(
    bucket: &str,
    object: &str,
    rs: &Option<HTTPRangeSpec>,
    h: &HeaderMap,
    oi: &ObjectInfo,
    opts: &ObjectOptions,
) -> Result<GetObjectReader, std::io::Error> {
    let mut tier_config_mgr = GLOBAL_TierConfigMgr.write().await;
    let tgt_client = match tier_config_mgr.get_driver(&oi.transitioned_object.tier).await {
        Ok(d) => d,
        Err(err) => return Err(std::io::Error::other(err)),
    };

    let ret = new_getobjectreader(rs, oi, opts, h);
    if let Err(err) = ret {
        return Err(error_resp_to_object_err(err, vec![bucket, object]));
    }
    let (get_fn, off, length) = ret.expect("get_transitioned_object_reader should succeed after error check");
    let mut gopts = WarmBackendGetOpts::default();

    if off >= 0 && length >= 0 {
        gopts.start_offset = off;
        gopts.length = length;
    }

    //return Ok(HttpFileReader::new(rs, &oi, opts, &h));
    //timeTierAction := auditTierActions(oi.transitioned_object.Tier, length)
    let reader = tgt_client
        .get(&oi.transitioned_object.name, &oi.transitioned_object.version_id, gopts)
        .await?;
    Ok(get_fn(reader, h.clone()))
}

pub async fn post_restore_opts(version_id: &str, bucket: &str, object: &str) -> Result<ObjectOptions, std::io::Error> {
    let versioned = BucketVersioningSys::prefix_enabled(bucket, object).await;
    let version_suspended = BucketVersioningSys::prefix_suspended(bucket, object).await;
    let vid = version_id.trim();
    if !vid.is_empty() && vid != NULL_VERSION_ID {
        if let Err(_err) = Uuid::parse_str(vid) {
            return Err(std::io::Error::other(
                StorageError::InvalidVersionID(bucket.to_string(), object.to_string(), vid.to_string()).to_string(),
            ));
        }
        if !versioned && !version_suspended {
            return Err(std::io::Error::other(
                StorageError::InvalidArgument(
                    bucket.to_string(),
                    object.to_string(),
                    format!("version-id specified {} but versioning is not enabled on {}", vid, bucket),
                )
                .to_string(),
            ));
        }
    }
    Ok(ObjectOptions {
        versioned,
        version_suspended,
        version_id: Some(vid.to_string()),
        ..Default::default()
    })
}

pub async fn put_restore_opts(
    bucket: &str,
    object: &str,
    rreq: &RestoreRequest,
    oi: &ObjectInfo,
) -> Result<ObjectOptions, std::io::Error> {
    let mut meta = HashMap::<String, String>::new();
    /*let mut b = false;
    let Some(Some(Some(mut sc))) = rreq.output_location.s3.storage_class else { b = true; };
    if b || sc == "" {
        //sc = oi.storage_class;
        sc = oi.transitioned_object.tier;
    }
    meta.insert(X_AMZ_STORAGE_CLASS.as_str().to_lowercase(), sc);*/

    if let Some(type_) = &rreq.type_
        && type_.as_str() == RestoreRequestType::SELECT
    {
        for v in rreq
            .output_location
            .as_ref()
            .unwrap()
            .s3
            .as_ref()
            .unwrap()
            .user_metadata
            .as_ref()
            .unwrap()
        {
            if !strings_has_prefix_fold(&v.name.clone().unwrap(), "x-amz-meta") {
                meta.insert(
                    format!("x-amz-meta-{}", v.name.as_ref().unwrap()),
                    v.value.clone().unwrap_or_else(|| "".to_string()),
                );
                continue;
            }
            meta.insert(v.name.clone().unwrap(), v.value.clone().unwrap_or_else(|| "".to_string()));
        }
        if let Some(output_location) = rreq.output_location.as_ref()
            && let Some(s3) = &output_location.s3
            && let Some(tags) = &s3.tagging
        {
            meta.insert(
                AMZ_OBJECT_TAGGING.to_string(),
                serde_urlencoded::to_string(tags.tag_set.clone()).unwrap_or_else(|_| "".to_string()),
            );
        }
        if let Some(output_location) = rreq.output_location.as_ref()
            && let Some(s3) = &output_location.s3
            && let Some(encryption) = &s3.encryption
            && encryption.encryption_type.as_str() != ""
        {
            meta.insert(X_AMZ_SERVER_SIDE_ENCRYPTION.as_str().to_string(), AMZ_ENCRYPTION_AES.to_string());
        }
        return Ok(ObjectOptions {
            versioned: BucketVersioningSys::prefix_enabled(bucket, object).await,
            version_suspended: BucketVersioningSys::prefix_suspended(bucket, object).await,
            user_defined: meta,
            ..Default::default()
        });
    }
    for (k, v) in &oi.user_defined {
        meta.insert(k.to_string(), v.clone());
    }
    if !oi.user_tags.is_empty() {
        meta.insert(AMZ_OBJECT_TAGGING.to_string(), oi.user_tags.clone());
    }
    let restore_expiry = lifecycle::expected_expiry_time(OffsetDateTime::now_utc(), rreq.days.unwrap_or(1));
    meta.insert(
        X_AMZ_RESTORE.as_str().to_string(),
        RestoreStatus {
            is_restore_in_progress: Some(false),
            restore_expiry_date: Some(Timestamp::from(restore_expiry)),
        }
        .to_string(),
    );
    Ok(ObjectOptions {
        versioned: BucketVersioningSys::prefix_enabled(bucket, object).await,
        version_suspended: BucketVersioningSys::prefix_suspended(bucket, object).await,
        user_defined: meta,
        version_id: oi.version_id.map(|e| e.to_string()),
        mod_time: oi.mod_time,
        //expires:           oi.expires,
        ..Default::default()
    })
}

pub trait LifecycleOps {
    fn to_lifecycle_opts(&self) -> lifecycle::ObjectOpts;
    fn is_remote(&self) -> bool;
}

impl LifecycleOps for ObjectInfo {
    fn to_lifecycle_opts(&self) -> lifecycle::ObjectOpts {
        lifecycle::ObjectOpts {
            name: self.name.clone(),
            user_tags: self.user_tags.clone(),
            version_id: self.version_id,
            mod_time: self.mod_time,
            size: self.size as usize,
            is_latest: self.is_latest,
            num_versions: self.num_versions,
            delete_marker: self.delete_marker,
            successor_mod_time: self.successor_mod_time,
            restore_ongoing: self.restore_ongoing,
            restore_expires: self.restore_expires,
            transition_status: self.transitioned_object.status.clone(),
            ..Default::default()
        }
    }

    fn is_remote(&self) -> bool {
        if self.transitioned_object.status != lifecycle::TRANSITION_COMPLETE {
            return false;
        }
        !is_restored_object_on_disk(&self.user_defined)
    }
}

pub trait RestoreRequestOps {
    fn validate(&self, api: Arc<ECStore>) -> Result<(), std::io::Error>;
}

impl RestoreRequestOps for RestoreRequest {
    fn validate(&self, _api: Arc<ECStore>) -> Result<(), std::io::Error> {
        // SELECT type requires select_parameters, and vice versa
        if self.type_.as_ref().is_none_or(|t| t.as_str() != RestoreRequestType::SELECT) && self.select_parameters.is_some() {
            return Err(std::io::Error::other("Select parameters can only be specified with SELECT request type"));
        }
        if let Some(type_) = &self.type_
            && type_.as_str() == RestoreRequestType::SELECT
            && self.select_parameters.is_none()
        {
            return Err(std::io::Error::other("SELECT restore request requires select parameters to be specified"));
        }

        // OutputLocation is only valid for SELECT requests
        if self.type_.as_ref().is_none_or(|t| t.as_str() != RestoreRequestType::SELECT) && self.output_location.is_some() {
            return Err(std::io::Error::other("OutputLocation can only be specified with SELECT request type"));
        }
        if let Some(type_) = &self.type_
            && type_.as_str() == RestoreRequestType::SELECT
            && self.output_location.is_none()
        {
            return Err(std::io::Error::other("OutputLocation required for SELECT requests"));
        }

        // Days must not be specified with SELECT requests
        if let Some(type_) = &self.type_
            && type_.as_str() == RestoreRequestType::SELECT
            && self.days.is_some_and(|d| d > 0)
        {
            return Err(std::io::Error::other("Days cannot be specified with SELECT restore request"));
        }

        // For non-SELECT requests, days must be at least 1
        if self.type_.is_none() && self.days.is_none_or(|d| d <= 0) {
            return Err(std::io::Error::other("restoration days should be at least 1"));
        }

        Ok(())
    }
}

const _MAX_RESTORE_OBJECT_REQUEST_SIZE: i64 = 2 << 20;

pub async fn eval_action_from_lifecycle(
    lc: &BucketLifecycleConfiguration,
    lr: Option<DefaultRetention>,
    rcfg: Option<(ReplicationConfiguration, OffsetDateTime)>,
    oi: &ObjectInfo,
) -> lifecycle::Event {
    let event = lc.eval(&oi.to_lifecycle_opts()).await;
    //if serverDebugLog {
    info!("lifecycle: Secondary scan: {}", event.action);
    //}

    let lock_enabled = if let Some(lr) = lr { lr.mode.is_some() } else { false };

    match event.action {
        IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction if lock_enabled => {
            return lifecycle::Event::default();
        }
        IlmAction::DeleteVersionAction | IlmAction::DeleteRestoredVersionAction => {
            if oi.version_id.is_none() {
                return lifecycle::Event::default();
            }
            // Lifecycle operations should never bypass governance retention
            if lock_enabled && check_object_lock_for_deletion(&oi.bucket, oi, false).await.is_some() {
                //if serverDebugLog {
                if oi.version_id.is_some() {
                    info!(
                        "lifecycle: {} v({}) is locked, not deleting",
                        oi.name,
                        oi.version_id.map(|v| v.to_string()).unwrap_or_default()
                    );
                } else {
                    info!("lifecycle: {} is locked, not deleting", oi.name);
                }
                //}
                return lifecycle::Event::default();
            }
            if let Some(rcfg) = rcfg
                && rep_has_active_rules(&rcfg.0, &oi.name, true)
            {
                return lifecycle::Event::default();
            }
        }
        _ => (),
    }

    event
}

pub async fn apply_transition_rule(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    if oi.delete_marker || oi.is_dir {
        return false;
    }
    GLOBAL_TransitionState.queue_transition_task(oi, event, src).await;
    true
}

pub async fn apply_expiry_on_transitioned_object(
    api: Arc<ECStore>,
    oi: &ObjectInfo,
    lc_event: &lifecycle::Event,
    src: &LcEventSrc,
) -> bool {
    let time_ilm = Metrics::time_ilm(lc_event.action);
    if let Err(_err) = expire_transitioned_object(api, oi, lc_event, src).await {
        return false;
    }
    time_ilm(1)();

    true
}

pub async fn apply_expiry_on_non_transitioned_objects(
    api: Arc<ECStore>,
    oi: &ObjectInfo,
    lc_event: &lifecycle::Event,
    _src: &LcEventSrc,
) -> bool {
    let mut opts = ObjectOptions {
        expiration: ExpirationOptions { expire: true },
        ..Default::default()
    };

    if lc_event.action.delete_versioned() {
        opts.version_id = oi.version_id.map(|v| v.to_string());
    }

    opts.versioned = BucketVersioningSys::prefix_enabled(&oi.bucket, &oi.name).await;
    opts.version_suspended = BucketVersioningSys::prefix_suspended(&oi.bucket, &oi.name).await;

    if lc_event.action.delete_all() {
        opts.delete_prefix = true;
        opts.delete_prefix_object = true;
    }

    let time_ilm = Metrics::time_ilm(lc_event.action);

    //debug!("lc_event.action: {:?}", lc_event.action);
    debug!("expiry_on_non_transitioned_objects opts: {:?}", opts);
    let mut dobj = match api.delete_object(&oi.bucket, &encode_dir_object(&oi.name), opts).await {
        Ok(dobj) => dobj,
        Err(e) => {
            error!("delete_object error: {:?}", e);
            return false;
        }
    };
    schedule_lifecycle_replication_delete_if_needed(oi, &dobj).await;
    //debug!("dobj: {:?}", dobj);
    if dobj.name.is_empty() {
        dobj = oi.clone();
    }

    //let tags = LcAuditEvent::new(lc_event.clone(), src.clone()).tags();
    //tags["version-id"] = dobj.version_id;

    let event_name = match lc_event.action {
        IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction => EventName::LifecycleExpirationDelete,
        _ if oi.delete_marker => EventName::LifecycleExpirationDelete,
        _ if dobj.delete_marker => EventName::LifecycleExpirationDeleteMarkerCreated,
        _ => EventName::LifecycleExpirationDelete,
    };
    send_event(EventArgs {
        event_name: event_name.to_string(),
        bucket_name: dobj.bucket.clone(),
        object: dobj,
        user_agent: "Internal: [ILM-Expiry]".to_string(),
        host: GLOBAL_LocalNodeName.to_string(),
        ..Default::default()
    });

    if lc_event.action != IlmAction::NoneAction {
        let mut num_versions = 1_u64;
        if lc_event.action.delete_all() {
            num_versions = oi.num_versions as u64;
        }
        time_ilm(num_versions)();
    }

    true
}

pub async fn apply_expiry_rule(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    let mut expiry_state = GLOBAL_ExpiryState.write().await;
    expiry_state.enqueue_by_days(oi, event, src).await;
    true
}

fn lifecycle_deleted_object(oi: &ObjectInfo, dobj: &ObjectInfo) -> crate::store_api::DeletedObject {
    if dobj.delete_marker {
        return crate::store_api::DeletedObject {
            object_name: oi.name.clone(),
            delete_marker: true,
            delete_marker_version_id: dobj.version_id,
            delete_marker_mtime: dobj.mod_time.or(oi.mod_time),
            ..Default::default()
        };
    }

    if oi.delete_marker && oi.version_id.is_some() {
        return crate::store_api::DeletedObject {
            object_name: oi.name.clone(),
            delete_marker: false,
            delete_marker_version_id: oi.version_id,
            delete_marker_mtime: oi.mod_time,
            ..Default::default()
        };
    }

    crate::store_api::DeletedObject {
        object_name: oi.name.clone(),
        delete_marker: false,
        version_id: oi.version_id,
        delete_marker_mtime: oi.mod_time,
        ..Default::default()
    }
}

async fn schedule_lifecycle_replication_delete_if_needed(oi: &ObjectInfo, dobj: &ObjectInfo) {
    let mut delete_object = lifecycle_deleted_object(oi, dobj);
    let version_id = if delete_object.delete_marker {
        None
    } else if delete_object.delete_marker_version_id.is_some() {
        delete_object.delete_marker_version_id
    } else {
        delete_object.version_id
    };

    let replication_state = lifecycle_delete_replication_state(oi, version_id).await;
    if replication_state.is_none() {
        return;
    }

    delete_object.replication_state = replication_state;

    schedule_replication_delete(DeletedObjectReplicationInfo {
        delete_object,
        bucket: oi.bucket.clone(),
        event_type: REPLICATE_INCOMING_DELETE.to_string(),
        ..Default::default()
    })
    .await;
}

fn should_reuse_lifecycle_delete_replication_state(oi: &ObjectInfo, version_delete: bool) -> bool {
    let state = oi.replication_state();
    if version_delete {
        oi.version_purge_status == VersionPurgeStatusType::Pending && !state.purge_targets.is_empty()
    } else {
        oi.replication_status == rustfs_filemeta::ReplicationStatusType::Pending && !state.targets.is_empty()
    }
}

fn lifecycle_version_purge_state_from_completed_targets(oi: &ObjectInfo) -> Option<ReplicationState> {
    if oi.replication_status != rustfs_filemeta::ReplicationStatusType::Completed {
        return None;
    }

    let targets = oi.replication_state().targets;
    if targets.is_empty() {
        return None;
    }

    let pending_status = targets.keys().map(|arn| format!("{arn}=PENDING;")).collect::<String>();

    Some(ReplicationState {
        replicate_decision_str: oi.replication_decision.clone(),
        version_purge_status_internal: Some(pending_status.clone()),
        purge_targets: rustfs_filemeta::version_purge_statuses_map(&pending_status),
        ..Default::default()
    })
}

async fn lifecycle_delete_replication_state(oi: &ObjectInfo, version_id: Option<Uuid>) -> Option<ReplicationState> {
    if should_reuse_lifecycle_delete_replication_state(oi, version_id.is_some()) {
        return Some(oi.replication_state());
    }

    if version_id.is_some()
        && let Some(state) = lifecycle_version_purge_state_from_completed_targets(oi)
    {
        return Some(state);
    }

    let dsc = check_replicate_delete(
        &oi.bucket,
        &ObjectToDelete {
            object_name: oi.name.clone(),
            version_id,
            ..Default::default()
        },
        oi,
        &ObjectOptions {
            version_id: version_id.map(|v| v.to_string()),
            versioned: BucketVersioningSys::prefix_enabled(&oi.bucket, &oi.name).await,
            ..Default::default()
        },
        None,
    )
    .await;
    if !dsc.replicate_any() {
        return None;
    }

    Some(replication_state_for_delete(dsc, version_id.is_some()))
}

fn replication_state_for_delete(dsc: ReplicateDecision, version_delete: bool) -> ReplicationState {
    let pending_status = dsc.pending_status();
    let mut state = ReplicationState {
        replicate_decision_str: dsc.to_string(),
        ..Default::default()
    };
    if version_delete {
        state.version_purge_status_internal = pending_status.clone();
        state.purge_targets = rustfs_filemeta::version_purge_statuses_map(pending_status.as_deref().unwrap_or_default());
    } else {
        state.replication_status_internal = pending_status.clone();
        state.targets = rustfs_filemeta::replication_statuses_map(pending_status.as_deref().unwrap_or_default());
    }
    state
}

pub async fn apply_lifecycle_action(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    let mut success = false;
    match event.action {
        IlmAction::DeleteVersionAction
        | IlmAction::DeleteAction
        | IlmAction::DeleteRestoredAction
        | IlmAction::DeleteRestoredVersionAction
        | IlmAction::DeleteAllVersionsAction
        | IlmAction::DelMarkerDeleteAllVersionsAction => {
            success = apply_expiry_rule(event, src, oi).await;
        }
        IlmAction::TransitionAction | IlmAction::TransitionVersionAction => {
            success = apply_transition_rule(event, src, oi).await;
        }
        _ => (),
    }
    success
}

#[cfg(test)]
mod tests {
    use super::{
        StaleMultipartUploadCandidate, cleanup_empty_multipart_sha_dirs_on_local_disks, cleanup_stale_multipart_uploads_once_at,
        lifecycle_deleted_object, lifecycle_version_purge_state_from_completed_targets,
        mark_delete_opts_skip_decommissioned_on_remote_success, merge_stale_multipart_candidate, replication_state_for_delete,
        should_reuse_lifecycle_delete_replication_state,
    };
    use crate::bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
    use crate::bucket::metadata_sys;
    use crate::disk::RUSTFS_META_MULTIPART_BUCKET;
    use crate::disk::endpoint::Endpoint;
    use crate::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
    use crate::error::is_err_invalid_upload_id;
    use crate::set_disk::{RUSTFS_MULTIPART_BUCKET_KEY, RUSTFS_MULTIPART_OBJECT_KEY};
    use crate::store::ECStore;
    use crate::store_api::{
        BucketOperations, BucketOptions, MakeBucketOptions, MultipartOperations, ObjectInfo, ObjectOptions, PutObjReader,
    };
    use rustfs_filemeta::{ReplicateDecision, VersionPurgeStatusType};
    use serial_test::serial;
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, OnceLock};
    use std::time::Duration as StdDuration;
    use time::OffsetDateTime;
    use tokio::fs;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    #[test]
    fn mark_delete_opts_skip_decommissioned_on_remote_success_sets_flag_on_success() {
        let mut opts = ObjectOptions::default();

        mark_delete_opts_skip_decommissioned_on_remote_success(&mut opts, true);

        assert!(opts.skip_decommissioned);
    }

    #[test]
    fn mark_delete_opts_skip_decommissioned_on_remote_success_preserves_false_on_failure() {
        let mut opts = ObjectOptions::default();

        mark_delete_opts_skip_decommissioned_on_remote_success(&mut opts, false);

        assert!(!opts.skip_decommissioned);
    }

    #[test]
    fn mark_delete_opts_skip_decommissioned_on_remote_success_preserves_existing_true_on_failure() {
        let mut opts = ObjectOptions {
            skip_decommissioned: true,
            ..ObjectOptions::default()
        };

        mark_delete_opts_skip_decommissioned_on_remote_success(&mut opts, false);

        assert!(opts.skip_decommissioned);
    }

    #[test]
    fn lifecycle_deleted_object_uses_delete_marker_created_by_expiry() {
        let source = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "key".to_string(),
            ..Default::default()
        };
        let delete_result = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "key".to_string(),
            delete_marker: true,
            version_id: Some(Uuid::new_v4()),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };

        let deleted = lifecycle_deleted_object(&source, &delete_result);

        assert!(deleted.delete_marker);
        assert_eq!(deleted.delete_marker_version_id, delete_result.version_id);
        assert_eq!(deleted.version_id, None);
        assert_eq!(deleted.object_name, "key");
    }

    #[test]
    fn lifecycle_deleted_object_uses_version_id_for_noncurrent_version_purge() {
        let version_id = Uuid::new_v4();
        let source = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "key".to_string(),
            version_id: Some(version_id),
            ..Default::default()
        };

        let deleted = lifecycle_deleted_object(&source, &ObjectInfo::default());

        assert!(!deleted.delete_marker);
        assert_eq!(deleted.version_id, Some(version_id));
        assert_eq!(deleted.delete_marker_version_id, None);
    }

    #[test]
    fn lifecycle_deleted_object_uses_delete_marker_version_for_marker_purge() {
        let version_id = Uuid::new_v4();
        let source = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "key".to_string(),
            delete_marker: true,
            version_id: Some(version_id),
            ..Default::default()
        };

        let deleted = lifecycle_deleted_object(&source, &ObjectInfo::default());

        assert!(!deleted.delete_marker);
        assert_eq!(deleted.delete_marker_version_id, Some(version_id));
        assert_eq!(deleted.version_id, None);
    }

    #[test]
    fn replication_state_for_delete_uses_replication_targets_for_current_delete() {
        let arn = "arn:aws:s3:::target-bucket";
        let mut dsc = ReplicateDecision::default();
        dsc.set(rustfs_filemeta::ReplicateTargetDecision::new(arn.to_string(), true, false));

        let state = replication_state_for_delete(dsc, false);

        assert_eq!(state.replication_status_internal.as_deref(), Some(format!("{arn}=PENDING;").as_str()));
        assert!(state.version_purge_status_internal.is_none());
        assert!(state.targets.contains_key(arn));
    }

    #[test]
    fn replication_state_for_delete_uses_purge_targets_for_version_delete() {
        let arn = "arn:aws:s3:::target-bucket";
        let mut dsc = ReplicateDecision::default();
        dsc.set(rustfs_filemeta::ReplicateTargetDecision::new(arn.to_string(), true, false));

        let state = replication_state_for_delete(dsc, true);

        assert_eq!(state.version_purge_status_internal.as_deref(), Some(format!("{arn}=PENDING;").as_str()));
        assert!(state.replication_status_internal.is_none());
        assert!(state.purge_targets.contains_key(arn));
    }

    #[test]
    fn lifecycle_delete_replication_state_reuses_only_pending_version_purge_state() {
        let oi = ObjectInfo {
            version_purge_status: VersionPurgeStatusType::Pending,
            version_purge_status_internal: Some("arn:aws:s3:::target=PENDING;".to_string()),
            replication_decision: "arn:aws:s3:::target=true;false;arn:aws:s3:::target;".to_string(),
            ..Default::default()
        };

        assert!(should_reuse_lifecycle_delete_replication_state(&oi, true));
        assert!(!should_reuse_lifecycle_delete_replication_state(&oi, false));
    }

    #[test]
    fn lifecycle_delete_replication_state_does_not_reuse_put_replication_for_version_delete() {
        let oi = ObjectInfo {
            replication_status: rustfs_filemeta::ReplicationStatusType::Completed,
            replication_status_internal: Some("arn:aws:s3:::target=COMPLETED;".to_string()),
            replication_decision: "arn:aws:s3:::target=true;false;arn:aws:s3:::target;".to_string(),
            ..Default::default()
        };

        assert!(
            !should_reuse_lifecycle_delete_replication_state(&oi, true),
            "version purges must not reuse plain object replication state from prior PUT/delete-marker replication"
        );
    }

    #[test]
    fn lifecycle_version_purge_state_from_completed_targets_derives_pending_purge_targets() {
        let oi = ObjectInfo {
            replication_status: rustfs_filemeta::ReplicationStatusType::Completed,
            replication_status_internal: Some("arn:aws:s3:::target=COMPLETED;".to_string()),
            replication_decision: "arn:aws:s3:::target=true;false;arn:aws:s3:::target;".to_string(),
            ..Default::default()
        };

        let state = lifecycle_version_purge_state_from_completed_targets(&oi)
            .expect("completed replication targets should be convertible into version-purge targets");

        assert_eq!(state.version_purge_status_internal.as_deref(), Some("arn:aws:s3:::target=PENDING;"));
        assert!(state.purge_targets.contains_key("arn:aws:s3:::target"));
        assert_eq!(state.replicate_decision_str, oi.replication_decision);
    }

    static STALE_MULTIPART_TEST_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>)> = OnceLock::new();

    async fn setup_test_env() -> (Vec<PathBuf>, Arc<ECStore>) {
        if let Some((paths, ecstore)) = STALE_MULTIPART_TEST_ENV.get() {
            return (paths.clone(), ecstore.clone());
        }

        let test_base_dir = format!("/tmp/rustfs_stale_multipart_test_{}", Uuid::new_v4());
        let temp_dir = PathBuf::from(&test_base_dir);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir).await.ok();
        }
        fs::create_dir_all(&temp_dir).await.unwrap();

        let disk_paths = vec![
            temp_dir.join("disk1"),
            temp_dir.join("disk2"),
            temp_dir.join("disk3"),
            temp_dir.join("disk4"),
        ];

        for disk_path in &disk_paths {
            fs::create_dir_all(disk_path).await.unwrap();
        }

        let mut endpoints = Vec::new();
        for (i, disk_path) in disk_paths.iter().enumerate() {
            let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(i);
            endpoints.push(endpoint);
        }

        let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "stale-multipart-test".to_string(),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        }]);

        crate::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

        let ecstore = ECStore::new("127.0.0.1:0".parse().unwrap(), endpoint_pools, CancellationToken::new())
            .await
            .unwrap();

        let buckets = ecstore
            .list_bucket(&BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_iter()
            .map(|bucket| bucket.name)
            .collect();
        metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

        let _ = STALE_MULTIPART_TEST_ENV.set((disk_paths.clone(), ecstore.clone()));

        (disk_paths, ecstore)
    }

    async fn create_test_bucket(ecstore: &Arc<ECStore>, bucket: &str) {
        ecstore
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
    }

    async fn set_abort_incomplete_lifecycle(bucket: &str, prefix: &str, days_after_initiation: i32) {
        let lifecycle_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>abort-multipart</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix>{prefix}</Prefix>
    </Filter>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>{days_after_initiation}</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>"#
        );

        metadata_sys::update(bucket, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
            .await
            .expect("lifecycle metadata should be stored");
    }

    async fn set_abort_incomplete_lifecycle_with_size(
        bucket: &str,
        prefix: &str,
        days_after_initiation: i32,
        object_size_greater_than: usize,
    ) {
        let lifecycle_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>abort-multipart-size</ID>
    <Status>Enabled</Status>
    <Filter>
      <And>
        <Prefix>{prefix}</Prefix>
        <ObjectSizeGreaterThan>{object_size_greater_than}</ObjectSizeGreaterThan>
      </And>
    </Filter>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>{days_after_initiation}</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>"#
        );

        metadata_sys::update(bucket, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
            .await
            .expect("lifecycle metadata should be stored");
    }

    fn multipart_sha_dir(bucket: &str, object: &str) -> String {
        hex_simd::encode_to_string(Sha256::digest(format!("{bucket}/{object}").as_bytes()), hex_simd::AsciiCase::Lower)
    }

    #[test]
    fn merge_stale_multipart_candidate_prefers_metadata_over_fallback() {
        let mut candidates = HashMap::new();

        merge_stale_multipart_candidate(
            &mut candidates,
            StaleMultipartUploadCandidate {
                path: "sha/upload".to_string(),
                initiated: OffsetDateTime::UNIX_EPOCH,
                metadata: None,
            },
        );
        merge_stale_multipart_candidate(
            &mut candidates,
            StaleMultipartUploadCandidate {
                path: "sha/upload".to_string(),
                initiated: OffsetDateTime::UNIX_EPOCH,
                metadata: Some(HashMap::from([("k".to_string(), "v".to_string())])),
            },
        );

        assert_eq!(
            candidates
                .get("sha/upload")
                .and_then(|candidate| candidate.metadata.as_ref())
                .and_then(|metadata| metadata.get("k")),
            Some(&"v".to_string())
        );
    }

    #[tokio::test]
    #[serial]
    async fn stale_multipart_cleanup_uses_default_expiry_without_lifecycle() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-default-{}", Uuid::new_v4().simple());
        let object = "default-cleanup/object.txt";
        create_test_bucket(&ecstore, &bucket).await;

        let initiated = OffsetDateTime::now_utc() - time::Duration::hours(30);
        let upload = ecstore
            .new_multipart_upload(
                &bucket,
                object,
                &ObjectOptions {
                    mod_time: Some(initiated),
                    ..Default::default()
                },
            )
            .await
            .expect("multipart upload should be created");

        let deleted = cleanup_stale_multipart_uploads_once_at(
            ecstore.clone(),
            OffsetDateTime::now_utc(),
            StdDuration::from_secs(24 * 60 * 60),
        )
        .await;
        assert!(deleted >= 1, "expected at least one stale multipart upload to be removed");

        let err = ecstore
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await
            .expect_err("stale multipart upload should be removed");
        assert!(is_err_invalid_upload_id(&err));
    }

    #[tokio::test]
    #[serial]
    async fn stale_multipart_cleanup_applies_abort_incomplete_lifecycle_before_default_expiry() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-lifecycle-{}", Uuid::new_v4().simple());
        let object = "logs/prefix/object.txt";
        create_test_bucket(&ecstore, &bucket).await;
        set_abort_incomplete_lifecycle(&bucket, "logs/", 1).await;

        let initiated = OffsetDateTime::now_utc() - time::Duration::hours(48);
        let upload = ecstore
            .new_multipart_upload(
                &bucket,
                object,
                &ObjectOptions {
                    mod_time: Some(initiated),
                    ..Default::default()
                },
            )
            .await
            .expect("multipart upload should be created");

        let deleted = cleanup_stale_multipart_uploads_once_at(
            ecstore.clone(),
            OffsetDateTime::now_utc(),
            StdDuration::from_secs(7 * 24 * 60 * 60),
        )
        .await;
        assert!(deleted >= 1, "expected lifecycle-driven stale multipart cleanup to run");

        let err = ecstore
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await
            .expect_err("multipart upload should be removed by lifecycle abort rule");
        assert!(is_err_invalid_upload_id(&err));
    }

    #[tokio::test]
    #[serial]
    async fn stale_multipart_cleanup_applies_abort_lifecycle_with_size_filter() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-size-{}", Uuid::new_v4().simple());
        let object = "logs/sized/object.txt";
        create_test_bucket(&ecstore, &bucket).await;
        set_abort_incomplete_lifecycle_with_size(&bucket, "logs/", 1, 5).await;

        let initiated = OffsetDateTime::now_utc() - time::Duration::hours(48);
        let upload = ecstore
            .new_multipart_upload(
                &bucket,
                object,
                &ObjectOptions {
                    mod_time: Some(initiated),
                    ..Default::default()
                },
            )
            .await
            .expect("multipart upload should be created");

        let mut data = PutObjReader::from_vec(vec![1, 2, 3, 4, 5, 6]);
        ecstore
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut data, &ObjectOptions::default())
            .await
            .expect("multipart part should be uploaded");

        let deleted = cleanup_stale_multipart_uploads_once_at(
            ecstore.clone(),
            OffsetDateTime::now_utc(),
            StdDuration::from_secs(7 * 24 * 60 * 60),
        )
        .await;
        assert!(deleted >= 1, "expected lifecycle-driven stale multipart cleanup to run");

        let err = ecstore
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await
            .expect_err("multipart upload should be removed by size-qualified lifecycle abort rule");
        assert!(is_err_invalid_upload_id(&err));
    }

    #[tokio::test]
    #[serial]
    async fn multipart_info_and_list_parts_do_not_expose_internal_metadata_keys() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-sanitize-{}", Uuid::new_v4().simple());
        let object = "sanitize/object.txt";
        create_test_bucket(&ecstore, &bucket).await;

        let upload = ecstore
            .new_multipart_upload(&bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");

        let multipart_info = ecstore
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await
            .expect("multipart info should be readable");
        assert!(!multipart_info.user_defined.contains_key(RUSTFS_MULTIPART_BUCKET_KEY));
        assert!(!multipart_info.user_defined.contains_key(RUSTFS_MULTIPART_OBJECT_KEY));

        let parts = ecstore
            .list_object_parts(&bucket, object, &upload.upload_id, None, 0, &ObjectOptions::default())
            .await
            .expect("multipart parts should be readable");
        assert!(!parts.user_defined.contains_key(RUSTFS_MULTIPART_BUCKET_KEY));
        assert!(!parts.user_defined.contains_key(RUSTFS_MULTIPART_OBJECT_KEY));
    }

    #[tokio::test]
    #[serial]
    async fn cleanup_removes_empty_multipart_sha_dirs() {
        let (paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-empty-sha-{}", Uuid::new_v4().simple());
        let object = "empty-sha/object.txt";
        let sha_dir = multipart_sha_dir(&bucket, object);
        for path in &paths {
            fs::create_dir_all(path.join(RUSTFS_META_MULTIPART_BUCKET).join(&sha_dir))
                .await
                .expect("empty multipart sha dir should be created for cleanup");
            assert!(
                path.join(RUSTFS_META_MULTIPART_BUCKET).join(&sha_dir).exists(),
                "empty multipart sha dir should exist before cleanup"
            );
        }

        cleanup_empty_multipart_sha_dirs_on_local_disks(&ecstore.pools[0].disk_set[0]).await;

        for path in &paths {
            assert!(
                !path.join(RUSTFS_META_MULTIPART_BUCKET).join(&sha_dir).exists(),
                "empty multipart sha dir should be removed"
            );
        }
    }
}
