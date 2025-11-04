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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use crate::bucket::lifecycle::bucket_lifecycle_audit::{LcAuditEvent, LcEventSrc};
use crate::bucket::lifecycle::lifecycle::{self, ExpirationOptions, Lifecycle, TransitionOptions};
use crate::bucket::lifecycle::tier_last_day_stats::{DailyAllTierStats, LastDayTierStats};
use crate::bucket::lifecycle::tier_sweeper::{Jentry, delete_object_from_remote_tier};
use crate::bucket::object_lock::objectlock_sys::enforce_retention_for_deletion;
use crate::bucket::{metadata_sys::get_lifecycle_config, versioning_sys::BucketVersioningSys};
use crate::client::object_api_utils::new_getobjectreader;
use crate::error::Error;
use crate::error::StorageError;
use crate::error::{error_resp_to_object_err, is_err_object_not_found, is_err_version_not_found, is_network_or_host_down};
use crate::event::name::EventName;
use crate::event_notification::{EventArgs, send_event};
use crate::global::GLOBAL_LocalNodeName;
use crate::global::{GLOBAL_LifecycleSys, GLOBAL_TierConfigMgr, get_global_deployment_id};
use crate::store::ECStore;
use crate::store_api::StorageAPI;
use crate::store_api::{GetObjectReader, HTTPRangeSpec, ObjectInfo, ObjectOptions, ObjectToDelete};
use crate::tier::warm_backend::WarmBackendGetOpts;
use async_channel::{Receiver as A_Receiver, Sender as A_Sender, bounded};
use bytes::BytesMut;
use futures::Future;
use http::HeaderMap;
use lazy_static::lazy_static;
use rustfs_common::data_usage::TierStats;
use rustfs_common::heal_channel::rep_has_active_rules;
use rustfs_common::metrics::{IlmAction, Metrics};
use rustfs_filemeta::{NULL_VERSION_ID, RestoreStatusOps, is_restored_object_on_disk};
use rustfs_utils::path::encode_dir_object;
use rustfs_utils::string::strings_has_prefix_fold;
use s3s::Body;
use s3s::dto::{
    BucketLifecycleConfiguration, DefaultRetention, ReplicationConfiguration, RestoreRequest, RestoreRequestType, RestoreStatus,
    ServerSideEncryption, Timestamp,
};
use s3s::header::{X_AMZ_RESTORE, X_AMZ_SERVER_SIDE_ENCRYPTION, X_AMZ_STORAGE_CLASS};
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info};
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
        let lc = get_lifecycle_config(bucket).await.expect("get_lifecycle_config err!").0;
        Some(lc)
    }

    pub fn trace(_oi: &ObjectInfo) -> TraceFn {
        todo!();
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
        let _ = hasher.write(format!("{}", self.obj_info.bucket).as_bytes());
        let _ = hasher.write(format!("{}", self.obj_info.name).as_bytes());
        hasher.flush();
        xxh64::xxh64(hasher.clone().finalize().as_slice(), XXHASH_SEED)
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
        let _ = hasher.write(format!("{}", self.0.transitioned_object.tier).as_bytes());
        let _ = hasher.write(format!("{}", self.0.transitioned_object.name).as_bytes());
        hasher.flush();
        xxh64::xxh64(hasher.clone().finalize().as_slice(), XXHASH_SEED)
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
        let _ = hasher.write(format!("{}", self.bucket).as_bytes());
        let _ = hasher.write(format!("{}", self.versions[0].object_name).as_bytes());
        hasher.flush();
        xxh64::xxh64(hasher.clone().finalize().as_slice(), XXHASH_SEED)
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
        if rxs.len() == 0 {
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
            *self.stats.as_mut().expect("err").missed_tier_journal_tasks.get_mut() += 1;
        }
        let wrkr = wrkr.expect("err");
        select! {
            //_ -> GlobalContext.Done() => ()
            _ = wrkr.send(Some(Box::new(je.clone()))) => (),
            else => {
                *self.stats.as_mut().expect("err").missed_tier_journal_tasks.get_mut() += 1;
            }
        }
        return Ok(());
    }

    pub async fn enqueue_free_version(&mut self, oi: ObjectInfo) {
        let task = FreeVersionTask(oi);
        let wrkr = self.get_worker_ch(task.op_hash());
        if wrkr.is_none() {
            *self.stats.as_mut().expect("err").missed_freevers_tasks.get_mut() += 1;
            return;
        }
        let wrkr = wrkr.expect("err!");
        select! {
            //_ -> GlobalContext.Done() => {}
            _ = wrkr.send(Some(Box::new(task))) => (),
            else => {
                *self.stats.as_mut().expect("err").missed_freevers_tasks.get_mut() += 1;
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
            *self.stats.as_mut().expect("err").missed_expiry_tasks.get_mut() += 1;
            return;
        }
        let wrkr = wrkr.expect("err!");
        select! {
            //_ -> GlobalContext.Done() => {}
            _ = wrkr.send(Some(Box::new(task))) => (),
            else => {
                *self.stats.as_mut().expect("err").missed_expiry_tasks.get_mut() += 1;
            }
        }
    }

    pub async fn enqueue_by_newer_noncurrent(&mut self, bucket: &str, versions: Vec<ObjectToDelete>, lc_event: lifecycle::Event) {
        if versions.len() == 0 {
            return;
        }

        let task = NewerNoncurrentTask {
            bucket: String::from(bucket),
            versions,
            event: lc_event,
        };
        let wrkr = self.get_worker_ch(task.op_hash());
        if wrkr.is_none() {
            *self.stats.as_mut().expect("err").missed_expiry_tasks.get_mut() += 1;
            return;
        }
        let wrkr = wrkr.expect("err!");
        select! {
            //_ -> GlobalContext.Done() => {}
            _ = wrkr.send(Some(Box::new(task))) => (),
            else => {
                *self.stats.as_mut().expect("err").missed_expiry_tasks.get_mut() += 1;
            }
        }
    }

    pub fn get_worker_ch(&self, h: u64) -> Option<Sender<Option<ExpiryOpType>>> {
        if self.tasks_tx.len() == 0 {
            return None;
        }
        Some(self.tasks_tx[h as usize % self.tasks_tx.len()].clone())
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
            *state.stats.as_mut().expect("err").workers.get_mut() += 1;
            tokio::spawn(async move {
                let mut rx = rx.lock().await;
                //let mut expiry_state = GLOBAL_ExpiryState.read().await;
                ExpiryState::worker(&mut *rx, api).await;
            });
        }

        let mut l = state.tasks_tx.len();
        while l > n {
            let worker = state.tasks_tx[l - 1].clone();
            worker.send(None).await.unwrap_or(());
            state.tasks_tx.remove(l - 1);
            state.tasks_rx.remove(l - 1);
            *state.stats.as_mut().expect("err").workers.get_mut() -= 1;
            l -= 1;
        }
    }

    pub async fn worker(rx: &mut Receiver<Option<ExpiryOpType>>, api: Arc<ECStore>) {
        //let cancel_token =
        //    get_background_services_cancel_token().ok_or_else(|| Error::other("Background services not initialized"))?;

        loop {
            select! {
                //_ = cancel_token.cancelled() => {
                _ = tokio::signal::ctrl_c() => {
                    info!("got ctrl+c, exits");
                    break;
                }
                v = rx.recv() => {
                    if v.is_none() {
                        break;
                    }
                    let v = v.expect("err!");
                    if v.is_none() {
                        //rx.close();
                        //drop(rx);
                        let _ = rx;
                        return;
                    }
                    let v = v.expect("err!");
                    if v.as_any().is::<ExpiryTask>() {
                        let v = v.as_any().downcast_ref::<ExpiryTask>().expect("err!");
                        if v.obj_info.transitioned_object.status != "" {
                            apply_expiry_on_transitioned_object(api.clone(), &v.obj_info, &v.event, &v.src).await;
                        } else {
                            apply_expiry_on_non_transitioned_objects(api.clone(), &v.obj_info, &v.event, &v.src).await;
                        }
                    }
                    else if v.as_any().is::<NewerNoncurrentTask>() {
                        let _v = v.as_any().downcast_ref::<NewerNoncurrentTask>().expect("err!");
                        //delete_object_versions(api, &v.bucket, &v.versions, v.event).await;
                    }
                    else if v.as_any().is::<Jentry>() {
                        //transitionLogIf(es.ctx, deleteObjectFromRemoteTier(es.ctx, v.ObjName, v.VersionID, v.TierName))
                    }
                    else if v.as_any().is::<FreeVersionTask>() {
                        let v = v.as_any().downcast_ref::<FreeVersionTask>().expect("err!");
                        let _oi = v.0.clone();

                    }
                    else {
                        //info!("Invalid work type - {:?}", v);
                        todo!();
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
        let _ = hasher.write(format!("{}", self.obj_info.bucket).as_bytes());
        //let _ = hasher.write(format!("{}", self.obj_info.versions[0].object_name).as_bytes());
        hasher.flush();
        xxh64::xxh64(hasher.clone().finalize().as_slice(), XXHASH_SEED)
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
        let max_workers = std::env::var("RUSTFS_MAX_TRANSITION_WORKERS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_else(|| std::cmp::min(num_cpus::get() as i64, 16));
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
                    let task = task.expect("err!");
                    if task.is_none() {
                        //self.transition_rx.close();
                        //drop(self.transition_rx);
                        return;
                    }
                    let task = task.expect("err!");
                    if task.as_any().is::<TransitionTask>() {
                        let task = task.as_any().downcast_ref::<TransitionTask>().expect("err!");

                        GLOBAL_TransitionState.active_tasks.fetch_add(1, Ordering::SeqCst);
                        if let Err(err) = transition_object(api.clone(), &task.obj_info, LcAuditEvent::new(task.event.clone(), task.src.clone())).await {
                            if !is_err_version_not_found(&err) && !is_err_object_not_found(&err) && !is_network_or_host_down(&err.to_string(), false) && !err.to_string().contains("use of closed network connection") {
                                error!("Transition to {} failed for {}/{} version:{} with {}",
                                    task.event.storage_class, task.obj_info.bucket, task.obj_info.name, task.obj_info.version_id.map(|v| v.to_string()).unwrap_or_default(), err.to_string());
                            }
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
            .or_insert(LastDayTierStats::default());
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
            let max_workers = std::env::var("RUSTFS_MAX_TRANSITION_WORKERS")
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or_else(|| std::cmp::min(num_cpus::get() as i64, 16));
            n = max_workers;
        }
        // Allow environment override of maximum workers
        let absolute_max = std::env::var("RUSTFS_ABSOLUTE_MAX_WORKERS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(32);
        n = std::cmp::min(n, absolute_max);

        let mut num_workers = GLOBAL_TransitionState.num_workers.load(Ordering::SeqCst);
        while num_workers < n {
            let clone_api = api.clone();
            tokio::spawn(async move {
                TransitionState::worker(clone_api).await;
            });
            num_workers = num_workers + 1;
            GLOBAL_TransitionState.num_workers.fetch_add(1, Ordering::SeqCst);
        }

        let mut num_workers = GLOBAL_TransitionState.num_workers.load(Ordering::SeqCst);
        while num_workers > n {
            let worker = GLOBAL_TransitionState.kill_tx.clone();
            worker.send(()).await;
            num_workers = num_workers - 1;
            GLOBAL_TransitionState.num_workers.fetch_add(-1, Ordering::SeqCst);
        }
    }
}

pub async fn init_background_expiry(api: Arc<ECStore>) {
    let mut workers = std::env::var("RUSTFS_MAX_EXPIRY_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| std::cmp::min(num_cpus::get(), 16));
    //globalILMConfig.getExpirationWorkers()
    if let Ok(env_expiration_workers) = env::var("_RUSTFS_ILM_EXPIRATION_WORKERS") {
        if let Ok(num_expirations) = env_expiration_workers.parse::<usize>() {
            workers = num_expirations;
        }
    }

    if workers == 0 {
        workers = std::env::var("RUSTFS_DEFAULT_EXPIRY_WORKERS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(8);
    }

    //let expiry_state = GLOBAL_ExpiryStSate.write().await;
    ExpiryState::resize_workers(workers, api).await;
}

pub async fn validate_transition_tier(lc: &BucketLifecycleConfiguration) -> Result<(), std::io::Error> {
    for rule in &lc.rules {
        if let Some(transitions) = &rule.transitions {
            for transition in transitions {
                if let Some(storage_class) = &transition.storage_class {
                    if storage_class.as_str() != "" {
                        let valid = GLOBAL_TierConfigMgr.read().await.is_tier_valid(storage_class.as_str());
                        if !valid {
                            return Err(std::io::Error::other(ERR_INVALID_STORAGECLASS));
                        }
                    }
                }
            }
        }
        if let Some(noncurrent_version_transitions) = &rule.noncurrent_version_transitions {
            for noncurrent_version_transition in noncurrent_version_transitions {
                if let Some(storage_class) = &noncurrent_version_transition.storage_class {
                    if storage_class.as_str() != "" {
                        let valid = GLOBAL_TierConfigMgr.read().await.is_tier_valid(storage_class.as_str());
                        if !valid {
                            return Err(std::io::Error::other(ERR_INVALID_STORAGECLASS));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub async fn enqueue_transition_immediate(oi: &ObjectInfo, src: LcEventSrc) {
    let lc = GLOBAL_LifecycleSys.get(&oi.bucket).await;
    if !lc.is_none() {
        let event = lc.expect("err").eval(&oi.to_lifecycle_opts()).await;
        match event.action {
            IlmAction::TransitionAction | IlmAction::TransitionVersionAction => {
                if oi.delete_marker || oi.is_dir {
                    return;
                }
                GLOBAL_TransitionState.queue_transition_task(oi, &event, &src).await;
            }
            _ => (),
        }
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
        match api.delete_object(&oi.bucket, &oi.name, opts).await {
            Ok(dobj) => {
                //audit_log_lifecycle(*oi, ILMExpiry, tags, traceFn);
                return Ok(dobj);
            }
            Err(err) => return Err(std::io::Error::other(err)),
        }
    }

    let ret = delete_object_from_remote_tier(
        &oi.transitioned_object.name,
        &oi.transitioned_object.version_id,
        &oi.transitioned_object.tier,
    )
    .await;
    if ret.is_ok() {
        opts.skip_decommissioned = true;
    } else {
        //transitionLogIf(ctx, err);
    }

    let dobj = match api.delete_object(&oi.bucket, &oi.name, opts).await {
        Ok(obj) => obj,
        Err(e) => {
            error!("Failed to delete transitioned object {}/{}: {:?}", oi.bucket, oi.name, e);
            // Return the original object info if deletion fails
            oi.clone()
        }
    };

    //defer auditLogLifecycle(ctx, *oi, ILMExpiry, tags, traceFn)

    let mut event_name = EventName::ObjectRemovedDelete;
    if oi.delete_marker {
        event_name = EventName::ObjectRemovedDeleteMarkerCreated;
    }
    let obj_info = ObjectInfo {
        name: oi.name.clone(),
        version_id: oi.version_id,
        delete_marker: oi.delete_marker,
        ..Default::default()
    };
    send_event(EventArgs {
        event_name: event_name.as_ref().to_string(),
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
    let _ = hasher.write(format!("{}/{}", get_global_deployment_id().unwrap_or_default(), bucket).as_bytes());
    hasher.flush();
    let hash = rustfs_utils::crypto::hex(hasher.clone().finalize().as_slice());
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
    time_ilm(1);
    api.transition_object(&oi.bucket, &oi.name, &opts).await
}

pub fn audit_tier_actions(_api: ECStore, _tier: &str, _bytes: i64) -> TimeFn {
    todo!();
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

    let ret = new_getobjectreader(rs, &oi, opts, &h);
    if let Err(err) = ret {
        return Err(error_resp_to_object_err(err, vec![bucket, object]));
    }
    let (get_fn, off, length) = ret.expect("err");
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
    if vid != "" && vid != NULL_VERSION_ID {
        if let Err(err) = Uuid::parse_str(vid) {
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
        versioned: versioned,
        version_suspended: version_suspended,
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
                    v.value.clone().unwrap_or("".to_string()),
                );
                continue;
            }
            meta.insert(v.name.clone().unwrap(), v.value.clone().unwrap_or("".to_string()));
        }
        if let Some(output_location) = rreq.output_location.as_ref() {
            if let Some(s3) = &output_location.s3 {
                if let Some(tags) = &s3.tagging {
                    meta.insert(
                        AMZ_OBJECT_TAGGING.to_string(),
                        serde_urlencoded::to_string(tags.tag_set.clone()).unwrap_or("".to_string()),
                    );
                }
            }
        }
        if let Some(output_location) = rreq.output_location.as_ref() {
            if let Some(s3) = &output_location.s3 {
                if let Some(encryption) = &s3.encryption {
                    if encryption.encryption_type.as_str() != "" {
                        meta.insert(X_AMZ_SERVER_SIDE_ENCRYPTION.as_str().to_string(), AMZ_ENCRYPTION_AES.to_string());
                    }
                }
            }
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
    if oi.user_tags.len() != 0 {
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
            version_id: self.version_id.map(|v| v.to_string()).unwrap_or_default(),
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
    fn validate(&self, api: Arc<ECStore>) -> Result<(), std::io::Error> {
        /*if self.type_.is_none() && self.select_parameters.is_some() {
            return Err(std::io::Error::other("Select parameters can only be specified with SELECT request type"));
        }
        if let Some(type_) = self.type_ && type_ == RestoreRequestType::SELECT && self.select_parameters.is_none() {
            return Err(std::io::Error::other("SELECT restore request requires select parameters to be specified"));
        }

        if self.type_.is_none() && self.output_location.is_some() {
            return Err(std::io::Error::other("OutputLocation required only for SELECT request type"));
        }
        if let Some(type_) = self.type_ && type_ == RestoreRequestType::SELECT && self.output_location.is_none() {
            return Err(std::io::Error::other("OutputLocation required for SELECT requests"));
        }

        if let Some(type_) = self.type_ && type_ == RestoreRequestType::SELECT && self.days != 0 {
            return Err(std::io::Error::other("Days cannot be specified with SELECT restore request"));
        }
        if self.days == 0 && self.type_.is_none() {
            return Err(std::io::Error::other("restoration days should be at least 1"));
        }
        if self.output_location.is_some() {
            if _, err := api.get_bucket_info(self.output_location.s3.bucket_name, BucketOptions{}); err != nil {
                return err
            }
            if self.output_location.s3.prefix == "" {
                return Err(std::io::Error::other("Prefix is a required parameter in OutputLocation"));
            }
            if self.output_location.s3.encryption.encryption_type.as_str() != ServerSideEncryption::AES256 {
                return NotImplemented{}
            }
        }*/
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
        lifecycle::IlmAction::DeleteAllVersionsAction | lifecycle::IlmAction::DelMarkerDeleteAllVersionsAction => {
            if lock_enabled {
                return lifecycle::Event::default();
            }
        }
        lifecycle::IlmAction::DeleteVersionAction | lifecycle::IlmAction::DeleteRestoredVersionAction => {
            if oi.version_id.is_none() {
                return lifecycle::Event::default();
            }
            if lock_enabled && enforce_retention_for_deletion(oi) {
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
            if let Some(rcfg) = rcfg {
                if rep_has_active_rules(&rcfg.0, &oi.name, true) {
                    return lifecycle::Event::default();
                }
            }
        }
        _ => (),
    }

    event
}

async fn apply_transition_rule(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
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
    // let time_ilm = ScannerMetrics::time_ilm(lc_event.action.clone());
    if let Err(_err) = expire_transitioned_object(api, oi, lc_event, src).await {
        return false;
    }
    // let _ = time_ilm(1);

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

    // let time_ilm = ScannerMetrics::time_ilm(lc_event.action.clone());

    //debug!("lc_event.action: {:?}", lc_event.action);
    //debug!("opts: {:?}", opts);
    let mut dobj = match api.delete_object(&oi.bucket, &encode_dir_object(&oi.name), opts).await {
        Ok(dobj) => dobj,
        Err(e) => {
            error!("delete_object error: {:?}", e);
            return false;
        }
    };
    //debug!("dobj: {:?}", dobj);
    if dobj.name.is_empty() {
        dobj = oi.clone();
    }

    //let tags = LcAuditEvent::new(lc_event.clone(), src.clone()).tags();
    //tags["version-id"] = dobj.version_id;

    let mut event_name = EventName::ObjectRemovedDelete;
    if oi.delete_marker {
        event_name = EventName::ObjectRemovedDeleteMarkerCreated;
    }
    match lc_event.action {
        lifecycle::IlmAction::DeleteAllVersionsAction => event_name = EventName::ObjectRemovedDeleteAllVersions,
        lifecycle::IlmAction::DelMarkerDeleteAllVersionsAction => event_name = EventName::ILMDelMarkerExpirationDelete,
        _ => (),
    }
    send_event(EventArgs {
        event_name: event_name.as_ref().to_string(),
        bucket_name: dobj.bucket.clone(),
        object: dobj,
        user_agent: "Internal: [ILM-Expiry]".to_string(),
        host: GLOBAL_LocalNodeName.to_string(),
        ..Default::default()
    });

    if lc_event.action != lifecycle::IlmAction::NoneAction {
        // let mut num_versions = 1_u64;
        // if lc_event.action.delete_all() {
        //     num_versions = oi.num_versions as u64;
        // }
        // let _ = time_ilm(num_versions);
    }

    true
}

async fn apply_expiry_rule(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    let mut expiry_state = GLOBAL_ExpiryState.write().await;
    expiry_state.enqueue_by_days(oi, event, src).await;
    true
}

pub async fn apply_lifecycle_action(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    let mut success = false;
    match event.action {
        lifecycle::IlmAction::DeleteVersionAction
        | lifecycle::IlmAction::DeleteAction
        | lifecycle::IlmAction::DeleteRestoredAction
        | lifecycle::IlmAction::DeleteRestoredVersionAction
        | lifecycle::IlmAction::DeleteAllVersionsAction
        | lifecycle::IlmAction::DelMarkerDeleteAllVersionsAction => {
            success = apply_expiry_rule(event, src, oi).await;
        }
        lifecycle::IlmAction::TransitionAction | lifecycle::IlmAction::TransitionVersionAction => {
            success = apply_transition_rule(event, src, oi).await;
        }
        _ => (),
    }
    success
}
