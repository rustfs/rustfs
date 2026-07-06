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

use super::replication_config_store::ReplicationConfigStore;
use super::replication_error_boundary::Error as EcstoreError;
use super::replication_filemeta_boundary::{
    MrfOpKind, MrfReplicateEntry, REPLICATE_HEAL_DELETE, ReplicateDecision, ReplicateObjectInfo, ReplicatedTargetInfo,
    ReplicationStatusType, ReplicationType, ReplicationWorkerOperation, ResyncDecision, replication_statuses_map,
    version_purge_statuses_map,
};
use super::replication_logging::{EVENT_REPLICATION_CONFIG_LOOKUP_SKIPPED, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REPLICATION};
use super::replication_metadata_boundary::ReplicationMetadataStore;
use super::replication_object_config::{ReplicationConfig, check_replicate_delete};
use super::replication_queue_boundary::{
    DeletedObjectReplicationInfo, LARGE_WORKER_COUNT, ReplicationBackpressureRecommendation, ReplicationBackpressureState,
    ReplicationHealQueueAction, ReplicationHealQueueResult, ReplicationHealResyncDeletes, ReplicationOperation,
    ReplicationPoolOpts, ReplicationPriority, ReplicationQueueAdmission, ReplicationWorkerQueue, WORKER_MAX_LIMIT,
    initial_worker_counts, large_worker_backpressure_resize, mrf_worker_size_to_count, replication_backpressure_recommendation,
    replication_heal_queue_action, resized_worker_counts, should_queue_large_object, worker_queue_for_replication_type,
};
use super::replication_resync_boundary::ResyncStatusType;
use super::replication_resync_boundary::{
    BucketReplicationResyncStatus, ResyncOpts, TargetReplicationResyncStatus, decode_mrf_file, decode_resync_file,
    encode_mrf_file, should_auto_resume_resync,
};
use super::replication_resyncer::{
    ReplicationResyncer, get_heal_replicate_object_info, replicate_delete, replicate_object, save_resync_status,
};
use super::replication_state::ReplicationStats;
use super::replication_storage_boundary::{
    ObjectInfo, ObjectOptions, ObjectToDelete, ReplicationDeletedObject, ReplicationObjectIO, ReplicationStorage,
};
use super::replication_target_boundary::ReplicationTargetStore;
use super::replication_versioning_boundary::ReplicationVersioningStore;
use super::runtime_boundary as runtime_sources;
use lazy_static::lazy_static;
use rustfs_utils::http::{SUFFIX_REPLICATION_TIMESTAMP, get_str};
use std::sync::Arc;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

const EVENT_REPLICATION_WORKER_RESIZE_SKIPPED: &str = "replication_worker_resize_skipped";
const EVENT_REPLICATION_WORKER_RESIZED: &str = "replication_worker_resized";
const EVENT_REPLICATION_BACKPRESSURE: &str = "replication_backpressure";
const EVENT_REPLICATION_RESYNC_LOAD_SKIPPED: &str = "replication_resync_load_skipped";
const EVENT_REPLICATION_RESYNC_RECOVERED: &str = "replication_resync_recovered";
const EVENT_REPLICATION_MRF_QUEUE_UNAVAILABLE: &str = "replication_mrf_queue_unavailable";

/// Main replication pool structure
#[derive(Debug)]
pub struct ReplicationPool<S: ReplicationStorage> {
    // Atomic counters for active workers
    active_workers: Arc<AtomicI32>,
    active_lrg_workers: Arc<AtomicI32>,
    active_mrf_workers: Arc<AtomicI32>,

    storage: Arc<S>,

    // Configuration
    priority: RwLock<ReplicationPriority>,
    max_workers: RwLock<usize>,
    max_l_workers: RwLock<usize>,

    // Statistics
    stats: Arc<ReplicationStats>,

    // Worker channels
    workers: RwLock<Vec<Sender<ReplicationOperation>>>,
    lrg_workers: RwLock<Vec<Sender<ReplicationOperation>>>,

    // MRF (Most Recent Failures) channels
    mrf_replica_tx: Sender<ReplicationOperation>,
    // Shared among N MRF workers; Arc allows spawning more than one worker.
    mrf_replica_rx: Arc<Mutex<Receiver<ReplicationOperation>>>,
    mrf_save_tx: Sender<MrfReplicateEntry>,
    mrf_save_rx: Mutex<Option<Receiver<MrfReplicateEntry>>>,

    // Control channels
    mrf_worker_kill_tx: Sender<()>,
    mrf_stop_tx: Sender<()>,

    // Worker size tracking
    mrf_worker_size: AtomicI32,

    // Task handles for cleanup
    task_handles: Mutex<Vec<JoinHandle<()>>>,

    // Replication resyncer for handling bucket resync operations
    resyncer: Arc<ReplicationResyncer>,
}

impl<S: ReplicationStorage> ReplicationPool<S> {
    /// Creates a new replication pool with specified options
    pub async fn new(opts: ReplicationPoolOpts, stats: Arc<ReplicationStats>, storage: Arc<S>) -> Arc<Self> {
        let worker_counts = initial_worker_counts(&opts);
        let max_workers = opts.max_workers.unwrap_or(WORKER_MAX_LIMIT);
        let max_l_workers = opts.max_l_workers.unwrap_or(LARGE_WORKER_COUNT);

        // Create MRF channels
        let (mrf_replica_tx, mrf_replica_rx) = mpsc::channel(100000);
        let (mrf_save_tx, mrf_save_rx) = mpsc::channel(100000);
        let (mrf_worker_kill_tx, _mrf_worker_kill_rx) = mpsc::channel(worker_counts.mrf_workers);
        let (mrf_stop_tx, _mrf_stop_rx) = mpsc::channel(1);

        let pool = Arc::new(Self {
            active_workers: Arc::new(AtomicI32::new(0)),
            active_lrg_workers: Arc::new(AtomicI32::new(0)),
            active_mrf_workers: Arc::new(AtomicI32::new(0)),
            priority: RwLock::new(opts.priority),
            max_workers: RwLock::new(max_workers),
            max_l_workers: RwLock::new(max_l_workers),
            stats,
            storage,
            workers: RwLock::new(Vec::new()),
            lrg_workers: RwLock::new(Vec::new()),
            mrf_replica_tx,
            mrf_replica_rx: Arc::new(Mutex::new(mrf_replica_rx)),
            mrf_save_tx,
            mrf_save_rx: Mutex::new(Some(mrf_save_rx)),
            mrf_worker_kill_tx,
            mrf_stop_tx,
            mrf_worker_size: AtomicI32::new(0),
            task_handles: Mutex::new(Vec::new()),
            resyncer: Arc::new(ReplicationResyncer::new().await),
        });

        // Initialize workers
        pool.resize_lrg_workers(max_l_workers, 0).await;
        pool.resize_workers(worker_counts.workers, 0).await;
        pool.resize_failed_workers(worker_counts.mrf_workers_i32()).await;

        // Start background tasks
        pool.start_mrf_processor().await;
        pool.start_mrf_persister().await;

        pool
    }

    /// Returns the number of active workers handling replication traffic
    pub fn active_workers(&self) -> i32 {
        self.active_workers.load(Ordering::SeqCst)
    }

    /// Returns the number of active workers handling replication failures
    pub fn active_mrf_workers(&self) -> i32 {
        self.active_mrf_workers.load(Ordering::SeqCst)
    }

    /// Returns the number of active workers handling traffic > 128MiB object size
    pub fn active_lrg_workers(&self) -> i32 {
        self.active_lrg_workers.load(Ordering::SeqCst)
    }

    /// Resizes the large workers pool
    pub async fn resize_lrg_workers(&self, n: usize, check_old: usize) {
        let mut lrg_workers = self.lrg_workers.write().await;

        if (check_old > 0 && lrg_workers.len() != check_old) || n == lrg_workers.len() || n < 1 {
            return;
        }

        // Add workers if needed
        while lrg_workers.len() < n {
            let (tx, rx) = mpsc::channel(100000);
            lrg_workers.push(tx);

            let active_counter = self.active_lrg_workers.clone();
            let storage = self.storage.clone();

            let handle = tokio::spawn(async move {
                let mut rx = rx;
                while let Some(operation) = rx.recv().await {
                    active_counter.fetch_add(1, Ordering::SeqCst);

                    match operation {
                        ReplicationOperation::Object(obj_info) => {
                            replicate_object(*obj_info, storage.clone()).await;
                        }
                        ReplicationOperation::Delete(del_info) => {
                            replicate_delete(*del_info, storage.clone()).await;
                        }
                    }

                    active_counter.fetch_sub(1, Ordering::SeqCst);
                }
            });

            self.task_handles.lock().await.push(handle);
        }

        // Remove workers if needed
        while lrg_workers.len() > n {
            if let Some(worker) = lrg_workers.pop() {
                drop(worker); // Closing the channel will terminate the worker
            }
        }
    }

    /// Resizes the regular workers pool
    pub async fn resize_workers(&self, n: usize, check_old: usize) {
        let mut workers = self.workers.write().await;

        if (check_old > 0 && workers.len() != check_old) || n == workers.len() || n < 1 {
            debug!(
                event = EVENT_REPLICATION_WORKER_RESIZE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                check_old_mismatch = check_old > 0 && workers.len() != check_old,
                same_size = n == workers.len(),
                invalid_target_size = n < 1,
                current_workers = workers.len(),
                target_workers = n,
                "Skipped replication worker resize"
            );
            return;
        }

        // Add workers if needed
        if workers.len() < n {
            info!(
                event = EVENT_REPLICATION_WORKER_RESIZED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                action = "increase",
                from_workers = workers.len(),
                to_workers = n,
                "Resized replication workers"
            );
        }

        while workers.len() < n {
            let (tx, rx) = mpsc::channel(10000);
            workers.push(tx);

            let active_counter = self.active_workers.clone();
            let stats = self.stats.clone();
            let storage = self.storage.clone();

            let handle = tokio::spawn(async move {
                let mut rx = rx;
                while let Some(operation) = rx.recv().await {
                    active_counter.fetch_add(1, Ordering::SeqCst);

                    match operation {
                        ReplicationOperation::Object(obj_info) => {
                            stats
                                .inc_q(&obj_info.bucket, obj_info.size, obj_info.delete_marker, obj_info.op_type)
                                .await;

                            // Perform actual replication (placeholder)
                            replicate_object(obj_info.as_ref().clone(), storage.clone()).await;

                            stats
                                .dec_q(&obj_info.bucket, obj_info.size, obj_info.delete_marker, obj_info.op_type)
                                .await;
                        }
                        ReplicationOperation::Delete(del_info) => {
                            stats.inc_q(&del_info.bucket, 0, true, del_info.op_type).await;
                            // Perform actual delete replication (placeholder)
                            replicate_delete(del_info.as_ref().clone(), storage.clone()).await;

                            stats.dec_q(&del_info.bucket, 0, true, del_info.op_type).await;
                        }
                    }

                    active_counter.fetch_sub(1, Ordering::SeqCst);
                }
            });

            self.task_handles.lock().await.push(handle);
        }

        // Remove workers if needed
        if workers.len() > n {
            info!(
                event = EVENT_REPLICATION_WORKER_RESIZED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                action = "decrease",
                from_workers = workers.len(),
                to_workers = n,
                "Resized replication workers"
            );
        }

        while workers.len() > n {
            if let Some(worker) = workers.pop() {
                drop(worker); // Closing the channel will terminate the worker
            }
        }
    }

    /// Resizes the failed workers pool
    pub async fn resize_failed_workers(&self, n: i32) {
        // Spawn workers up to n.  Each worker shares the receiver via Arc<Mutex<...>>.
        // The mutex is held only while calling recv() — released before processing — so
        // all workers process entries concurrently (the dequeue step is serialised but
        // the replication I/O is not).
        while self.mrf_worker_size.load(Ordering::SeqCst) < n {
            self.mrf_worker_size.fetch_add(1, Ordering::SeqCst);

            let active_counter = self.active_mrf_workers.clone();
            let stats = self.stats.clone();
            let storage = self.storage.clone();
            let mrf_rx = Arc::clone(&self.mrf_replica_rx);

            let handle = tokio::spawn(async move {
                loop {
                    let operation = { mrf_rx.lock().await.recv().await };
                    let Some(operation) = operation else { break };

                    active_counter.fetch_add(1, Ordering::SeqCst);
                    match operation {
                        ReplicationOperation::Object(obj_info) => {
                            stats
                                .inc_q(&obj_info.bucket, obj_info.size, obj_info.delete_marker, obj_info.op_type)
                                .await;
                            replicate_object(obj_info.as_ref().clone(), storage.clone()).await;
                            stats
                                .dec_q(&obj_info.bucket, obj_info.size, obj_info.delete_marker, obj_info.op_type)
                                .await;
                        }
                        ReplicationOperation::Delete(del_info) => {
                            replicate_delete(*del_info, storage.clone()).await;
                        }
                    }
                    active_counter.fetch_sub(1, Ordering::SeqCst);
                }
            });
            self.task_handles.lock().await.push(handle);
        }

        // Remove workers if needed
        while self.mrf_worker_size.load(Ordering::SeqCst) > n {
            self.mrf_worker_size.fetch_sub(1, Ordering::SeqCst);
            let _ = self.mrf_worker_kill_tx.try_send(());
        }
    }

    /// Resizes worker priority and counts
    pub async fn resize_worker_priority(
        &self,
        pri: ReplicationPriority,
        max_workers: Option<usize>,
        max_l_workers: Option<usize>,
    ) {
        let current_workers = self.workers.read().await.len();
        let current_mrf = mrf_worker_size_to_count(self.mrf_worker_size.load(Ordering::SeqCst));
        let worker_counts = resized_worker_counts(&pri, max_workers, current_workers, current_mrf);

        if let Some(max_w) = max_workers {
            *self.max_workers.write().await = max_w;
        }

        let max_l_workers_val = max_l_workers.unwrap_or(LARGE_WORKER_COUNT);
        *self.max_l_workers.write().await = max_l_workers_val;
        *self.priority.write().await = pri;

        self.resize_workers(worker_counts.workers, 0).await;
        self.resize_failed_workers(worker_counts.mrf_workers_i32()).await;
        self.resize_lrg_workers(max_l_workers_val, 0).await;
    }

    /// Gets a worker channel deterministically based on bucket and object names
    async fn get_worker_ch(&self, bucket: &str, object: &str, _size: i64) -> Option<Sender<ReplicationOperation>> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        format!("{bucket}{object}").hash(&mut hasher);
        let hash = hasher.finish();

        let workers = self.workers.read().await;
        if workers.is_empty() {
            return None;
        }

        let index = (hash as usize) % workers.len();
        workers.get(index).cloned()
    }

    async fn worker_queue_channel(
        &self,
        op_type: &ReplicationType,
        bucket: &str,
        object: &str,
        size: i64,
    ) -> Option<Sender<ReplicationOperation>> {
        match worker_queue_for_replication_type(op_type) {
            ReplicationWorkerQueue::Mrf => Some(self.mrf_replica_tx.clone()),
            ReplicationWorkerQueue::Regular => self.get_worker_ch(bucket, object, size).await,
        }
    }

    async fn apply_queue_backpressure(&self, queue_type: &'static str, include_mrf_workers: bool, message: &'static str) {
        let priority = self.priority.read().await.clone();
        let max_workers = *self.max_workers.read().await;
        let current_workers = self.workers.read().await.len();
        let current_mrf_workers = self.mrf_worker_size.load(Ordering::SeqCst);
        let recommendation = replication_backpressure_recommendation(
            &priority,
            ReplicationBackpressureState {
                current_workers,
                active_workers: self.active_workers(),
                current_mrf_workers,
                active_mrf_workers: self.active_mrf_workers(),
                max_workers,
                include_mrf_workers,
            },
        );

        match recommendation {
            ReplicationBackpressureRecommendation::KeepFast => {
                debug!(
                    event = EVENT_REPLICATION_BACKPRESSURE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    queue_type,
                    priority = "fast",
                    recommendation = "none",
                    "{message}"
                );
            }
            ReplicationBackpressureRecommendation::SetPriorityAuto => {
                debug!(
                    event = EVENT_REPLICATION_BACKPRESSURE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    queue_type,
                    priority = "slow",
                    recommendation = "set_priority_auto",
                    "{message}"
                );
            }
            ReplicationBackpressureRecommendation::Resize(resize) => {
                if let Some(regular_workers) = resize.regular_workers {
                    self.resize_workers(regular_workers.new_count, regular_workers.existing_count)
                        .await;
                }

                if let Some(mrf_workers) = resize.mrf_workers {
                    self.resize_failed_workers(mrf_workers).await;
                }
            }
            ReplicationBackpressureRecommendation::Noop => {}
        }
    }

    /// Queues a replica task
    pub async fn queue_replica_task(&self, ri: ReplicateObjectInfo) -> ReplicationQueueAdmission {
        // If object is large, queue it to a static set of large workers
        if should_queue_large_object(ri.size) {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let mut hasher = DefaultHasher::new();
            format!("{}{}", ri.bucket, ri.name).hash(&mut hasher);
            let hash = hasher.finish();

            let lrg_workers = self.lrg_workers.read().await;

            if !lrg_workers.is_empty() {
                let index = (hash as usize) % lrg_workers.len();

                if let Some(worker) = lrg_workers.get(index)
                    && worker.try_send(ReplicationOperation::Object(Box::new(ri.clone()))).is_err()
                {
                    // Try to add more workers if possible
                    let max_l_workers = *self.max_l_workers.read().await;
                    let existing = lrg_workers.len();
                    let resize = large_worker_backpressure_resize(existing, self.active_lrg_workers(), max_l_workers);
                    drop(lrg_workers);

                    // Queue to MRF if worker is busy.
                    let admission =
                        queue_mrf_save_admission(&self.mrf_save_tx, ri.to_mrf_entry(), &ri.bucket, &ri.name, "large_object")
                            .await;

                    if let Some(resize) = resize {
                        self.resize_lrg_workers(resize.new_count, resize.existing_count).await;
                    }
                    return admission;
                }

                return ReplicationQueueAdmission::Queued;
            }
            return ReplicationQueueAdmission::Missed;
        }

        // Handle regular sized objects

        let ch = self.worker_queue_channel(&ri.op_type, &ri.bucket, &ri.name, ri.size).await;

        let Some(channel) = ch else {
            return ReplicationQueueAdmission::Missed;
        };

        if channel.try_send(ReplicationOperation::Object(Box::new(ri.clone()))).is_ok() {
            return ReplicationQueueAdmission::Queued;
        }

        // Queue to MRF if all workers are busy.
        let admission = queue_mrf_save_admission(&self.mrf_save_tx, ri.to_mrf_entry(), &ri.bucket, &ri.name, "object").await;

        // Try to scale up workers based on priority
        self.apply_queue_backpressure("object", true, "Replication queue is backpressured")
            .await;

        admission
    }

    /// Queues a replica delete task
    pub async fn queue_replica_delete_task(&self, doi: DeletedObjectReplicationInfo) -> ReplicationQueueAdmission {
        let ch = self
            .worker_queue_channel(&doi.op_type, &doi.bucket, &doi.delete_object.object_name, 0)
            .await;

        let Some(channel) = ch else {
            return ReplicationQueueAdmission::Missed;
        };

        if channel.try_send(ReplicationOperation::Delete(Box::new(doi.clone()))).is_ok() {
            return ReplicationQueueAdmission::Queued;
        }

        let admission = queue_mrf_save_admission(
            &self.mrf_save_tx,
            doi.to_mrf_entry(),
            &doi.bucket,
            &doi.delete_object.object_name,
            "delete",
        )
        .await;

        self.apply_queue_backpressure("delete", false, "Replication delete queue is backpressured")
            .await;

        admission
    }

    /// Queues an MRF save operation
    async fn queue_mrf_save(&self, entry: MrfReplicateEntry) {
        let _ = queue_mrf_save_admission(&self.mrf_save_tx, entry, "", "", "mrf_worker").await;
    }

    /// Starts the MRF processor — one-shot at startup.
    ///
    /// Reads the on-disk MRF file, re-injects every entry into `mrf_replica_tx` as a
    /// Heal operation, then clears the file.  The file is cleared AFTER all entries are
    /// successfully queued so a crash mid-replay results in at-most-twice delivery
    /// (safe — replication is idempotent) rather than entry loss.
    async fn start_mrf_processor(&self) {
        let storage = self.storage.clone();

        let handle = tokio::spawn(async move {
            let data = match ReplicationConfigStore::read(storage.clone(), ReplicationMetadataStore::MRF_REPLICATION_FILE).await {
                Ok(d) => d,
                Err(EcstoreError::ConfigNotFound) => return, // no file yet — normal on first start
                Err(e) => {
                    warn!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION,
                        error = %e,
                        "Failed to load MRF recovery file"
                    );
                    return;
                }
            };

            let entries = match decode_mrf_file(&data) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION,
                        error = %e,
                        "Failed to decode MRF recovery file — discarding corrupt data"
                    );
                    // Overwrite the corrupt file so we don't fail again on next restart.
                    let _ = ReplicationConfigStore::save(
                        storage,
                        ReplicationMetadataStore::MRF_REPLICATION_FILE,
                        encode_mrf_file(&[]).unwrap_or_default(),
                    )
                    .await;
                    return;
                }
            };

            let total = entries.len();
            let mut queued_count = 0usize;

            for entry in entries.iter() {
                match entry.op {
                    MrfOpKind::Delete => {
                        // Reconstruct a heal delete and re-queue it.  We do NOT call
                        // get_object_info here because the delete-marker or version may
                        // already be absent from the local store — that is expected.
                        //
                        // The MRF entry does not persist the replication decision and the
                        // source object is gone, so re-derive the decision from the live
                        // bucket config (mirroring get_heal_replicate_object_info) and set
                        // it on the reconstructed delete. Without this the decision string
                        // is empty and the delete replicates to zero targets — a silent
                        // no-op that leaves replicas diverged (backlog#858 / #799 B9).
                        let versioned = ReplicationVersioningStore::prefix_enabled(&entry.bucket, &entry.object).await;
                        let oi = ObjectInfo {
                            bucket: entry.bucket.clone(),
                            name: entry.object.clone(),
                            version_id: entry.version_id,
                            delete_marker: entry.delete_marker,
                            ..Default::default()
                        };
                        let dsc = check_replicate_delete(
                            &entry.bucket,
                            &ObjectToDelete {
                                object_name: entry.object.clone(),
                                version_id: entry.version_id,
                                ..Default::default()
                            },
                            &oi,
                            &ObjectOptions {
                                versioned,
                                ..Default::default()
                            },
                            None,
                        )
                        .await;
                        let mut rstate = oi.replication_state();
                        rstate.replicate_decision_str = dsc.to_string();

                        let dv = DeletedObjectReplicationInfo {
                            delete_object: ReplicationDeletedObject {
                                object_name: entry.object.clone(),
                                version_id: entry.version_id,
                                delete_marker_version_id: entry.delete_marker_version_id,
                                delete_marker: entry.delete_marker,
                                replication_state: Some(rstate),
                                ..Default::default()
                            },
                            bucket: entry.bucket.clone(),
                            op_type: ReplicationType::Heal,
                            event_type: REPLICATE_HEAL_DELETE.to_string(),
                            ..Default::default()
                        };
                        schedule_replication_delete(dv).await;
                        queued_count += 1;
                    }
                    MrfOpKind::Object => {
                        let opts = ObjectOptions {
                            version_id: entry.version_id.map(|u| u.to_string()),
                            ..Default::default()
                        };
                        let oi = match storage.get_object_info(&entry.bucket, &entry.object, &opts).await {
                            Ok(oi) => oi,
                            Err(e) => {
                                debug!(
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                                    bucket = %entry.bucket,
                                    object = %entry.object,
                                    error = %e,
                                    "MRF recovery: object not found, skipping"
                                );
                                continue;
                            }
                        };
                        // Route through queue_replication_heal so the replication decision (dsc)
                        // is computed from the live config — required for replicate_object.
                        queue_replication_heal(&entry.bucket, oi, entry.retry_count as u32).await;
                        queued_count += 1;
                    }
                }
            }

            // Clear AFTER all entries are processed so a crash mid-replay causes at-most-twice
            // delivery (idempotent) rather than entry loss.
            if let Err(e) = ReplicationConfigStore::save(
                storage,
                ReplicationMetadataStore::MRF_REPLICATION_FILE,
                encode_mrf_file(&[]).unwrap_or_default(),
            )
            .await
            {
                warn!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    error = %e,
                    "Failed to clear MRF recovery file after replay — entries may be replayed again on next restart"
                );
            }

            if queued_count > 0 {
                info!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    recovered = queued_count,
                    total,
                    "Recovered MRF entries from disk and queued for retry"
                );
            }
        });
        self.task_handles.lock().await.push(handle);
    }

    /// Starts the MRF persister — ongoing background task.
    ///
    /// Drains `mrf_save_rx` (entries that overflowed the normal worker channels) and
    /// writes them to the on-disk MRF file every 10 seconds or when 1 000 entries
    /// accumulate.  The file is overwritten (not appended) on each flush so it always
    /// reflects the current pending backlog.
    async fn start_mrf_persister(&self) {
        let Some(mut rx) = self.mrf_save_rx.lock().await.take() else {
            return;
        };
        let storage = self.storage.clone();

        let handle = tokio::spawn(async move {
            let mut pending: Vec<MrfReplicateEntry> = Vec::new();
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    entry = rx.recv() => match entry {
                        Some(e) => {
                            pending.push(e);
                            if pending.len() >= 1000 && flush_mrf_to_disk(&pending, &storage).await {
                                pending.clear();
                            }
                        }
                        None => {
                            // Channel closed (pool shutting down) — final flush.
                            if !pending.is_empty() {
                                flush_mrf_to_disk(&pending, &storage).await;
                            }
                            break;
                        }
                    },
                    _ = interval.tick() => {
                        if !pending.is_empty() && flush_mrf_to_disk(&pending, &storage).await {
                            pending.clear();
                        }
                    }
                }
            }
        });
        self.task_handles.lock().await.push(handle);
    }

    /// Worker function for handling regular replication operations
    async fn add_worker(
        &self,
        mut rx: Receiver<ReplicationOperation>,
        active_counter: Arc<AtomicI32>,
        stats: Arc<ReplicationStats>,
    ) {
        while let Some(operation) = rx.recv().await {
            active_counter.fetch_add(1, Ordering::SeqCst);

            match operation {
                ReplicationOperation::Object(obj_info) => {
                    stats
                        .inc_q(&obj_info.bucket, obj_info.size, obj_info.delete_marker, obj_info.op_type)
                        .await;

                    // Perform actual replication (placeholder)
                    replicate_object(obj_info.as_ref().clone(), self.storage.clone()).await;

                    stats
                        .dec_q(&obj_info.bucket, obj_info.size, obj_info.delete_marker, obj_info.op_type)
                        .await;
                }
                ReplicationOperation::Delete(del_info) => {
                    stats.inc_q(&del_info.bucket, 0, true, del_info.op_type).await;

                    // Perform actual delete replication (placeholder)
                    replicate_delete(del_info.as_ref().clone(), self.storage.clone()).await;

                    stats.dec_q(&del_info.bucket, 0, true, del_info.op_type).await;
                }
            }

            active_counter.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Worker function for handling large object replication operations
    async fn add_large_worker(&self, mut rx: Receiver<ReplicationOperation>, active_counter: Arc<AtomicI32>, storage: Arc<S>) {
        while let Some(operation) = rx.recv().await {
            active_counter.fetch_add(1, Ordering::SeqCst);

            match operation {
                ReplicationOperation::Object(obj_info) => {
                    replicate_object(*obj_info, storage.clone()).await;
                }
                ReplicationOperation::Delete(del_info) => {
                    replicate_delete(*del_info, storage.clone()).await;
                }
            }

            active_counter.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Worker function for handling MRF (Most Recent Failures) operations
    async fn add_mrf_worker(
        &self,
        mut rx: Receiver<ReplicationOperation>,
        active_counter: Arc<AtomicI32>,
        stats: Arc<ReplicationStats>,
    ) {
        while let Some(operation) = rx.recv().await {
            active_counter.fetch_add(1, Ordering::SeqCst);

            match operation {
                ReplicationOperation::Object(obj_info) => {
                    stats
                        .inc_q(&obj_info.bucket, obj_info.size, obj_info.delete_marker, obj_info.op_type)
                        .await;

                    replicate_object(obj_info.as_ref().clone(), self.storage.clone()).await;

                    stats
                        .dec_q(&obj_info.bucket, obj_info.size, obj_info.delete_marker, obj_info.op_type)
                        .await;
                }
                ReplicationOperation::Delete(del_info) => {
                    replicate_delete(*del_info, self.storage.clone()).await;
                }
            }

            active_counter.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Delete resync metadata from replication resync state in memory
    pub async fn delete_resync_metadata(&self, bucket: &str) {
        let mut status_map = self.resyncer.status_map.write().await;
        status_map.remove(bucket);
        // Note: global site resync metrics deletion would be handled here
        // global_site_resync_metrics.delete_bucket(bucket);
    }

    /// Initialize bucket replication resync for all buckets
    pub async fn init_resync_internal(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
        buckets: Vec<String>,
    ) -> Result<(), EcstoreError> {
        // Load bucket metadata system in background
        let pool_clone = self;

        tokio::spawn(async move {
            pool_clone.start_resync_routine(buckets, cancellation_token).await;
        });

        Ok(())
    }

    pub async fn get_bucket_resync_status(&self, bucket: &str) -> Result<BucketReplicationResyncStatus, EcstoreError> {
        if let Some(status) = self.resyncer.status_map.read().await.get(bucket).cloned() {
            return Ok(status);
        }

        let status = load_bucket_resync_metadata(bucket, self.storage.clone()).await?;
        self.resyncer
            .status_map
            .write()
            .await
            .insert(bucket.to_string(), status.clone());
        Ok(status)
    }

    pub async fn cancel_bucket_resync(&self, opts: ResyncOpts) -> Result<(), EcstoreError> {
        self.resyncer.cancel(&opts).await;
        self.resyncer
            .mark_status(ResyncStatusType::ResyncCanceled, opts, self.storage.clone())
            .await?;
        Ok(())
    }

    pub async fn start_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<(), EcstoreError> {
        let now = OffsetDateTime::now_utc();
        let bucket_status = {
            let mut status_map = self.resyncer.status_map.write().await;
            let bucket_status = status_map.entry(opts.bucket.clone()).or_insert_with(|| {
                let mut status = BucketReplicationResyncStatus::new();
                status.id = 0;
                status
            });

            bucket_status.last_update = Some(now);
            bucket_status.targets_map.insert(
                opts.arn.clone(),
                TargetReplicationResyncStatus {
                    start_time: Some(now),
                    last_update: Some(now),
                    resync_id: opts.resync_id.clone(),
                    resync_before_date: opts.resync_before,
                    resync_status: ResyncStatusType::ResyncPending,
                    failed_size: 0,
                    failed_count: 0,
                    replicated_size: 0,
                    replicated_count: 0,
                    bucket: opts.bucket.clone(),
                    object: String::new(),
                    error: None,
                },
            );

            bucket_status.clone()
        };

        save_resync_status(&opts.bucket, &bucket_status, self.storage.clone()).await?;

        let resyncer = self.resyncer.clone();
        let storage = self.storage.clone();
        let cancel_token = CancellationToken::new();
        resyncer.register_cancel_token(&opts, cancel_token.clone()).await;
        tokio::spawn(async move {
            Box::pin(resyncer.clone().resync_bucket(cancel_token, storage, false, opts.clone())).await;
            resyncer.clear_cancel_token(&opts).await;
        });

        Ok(())
    }

    /// Start the resync routine that runs in a loop
    async fn start_resync_routine(self: Arc<Self>, buckets: Vec<String>, cancellation_token: CancellationToken) {
        // Run the replication resync in a loop
        loop {
            let self_clone = self.clone();
            let ctx = cancellation_token.clone();
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    return;
                }
                result = self_clone.load_resync(&buckets, ctx) => {
                    if result.is_ok() {
                        return;
                    }
                }
            }

            // Generate random duration between 0 and 1 minute
            use rand::RngExt;
            let duration_millis = rand::rng().random_range(0..60_000);
            let mut duration = Duration::from_millis(duration_millis);

            // Make sure to sleep at least a second to avoid high CPU ticks
            if duration < Duration::from_secs(1) {
                duration = Duration::from_secs(1);
            }

            tokio::time::sleep(duration).await;
        }
    }

    /// Load bucket replication resync statuses into memory
    #[instrument(skip(_cancellation_token))]
    async fn load_resync(
        self: Arc<Self>,
        buckets: &[String],
        _cancellation_token: CancellationToken,
    ) -> Result<(), EcstoreError> {
        // TODO: add leader_lock
        // Make sure only one node running resync on the cluster
        // Note: Leader lock implementation would be needed here
        // let _lock_guard = global_leader_lock.get_lock().await?;

        let mut recovered_statuses = Vec::new();
        let mut restart_opts = Vec::new();
        let mut recovered_bucket_count = 0usize;
        let mut skipped_failed_target_count = 0usize;

        for bucket in buckets {
            let meta = match load_bucket_resync_metadata(bucket, self.storage.clone()).await {
                Ok(meta) => meta,
                Err(err) => {
                    if !matches!(err, EcstoreError::VolumeNotFound) {
                        debug!(
                            event = EVENT_REPLICATION_RESYNC_LOAD_SKIPPED,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REPLICATION,
                            bucket,
                            error = ?err,
                            reason = "metadata_load_failed",
                            "Skipped replication resync metadata load"
                        );
                    }
                    continue;
                }
            };

            if meta.targets_map.is_empty() {
                continue;
            }

            recovered_bucket_count += 1;
            for (arn, stats) in &meta.targets_map {
                if should_auto_resume_resync(stats.resync_status) {
                    restart_opts.push(ResyncOpts {
                        bucket: bucket.clone(),
                        arn: arn.clone(),
                        resync_id: stats.resync_id.clone(),
                        resync_before: stats.resync_before_date,
                    });
                } else if stats.resync_status == ResyncStatusType::ResyncFailed {
                    skipped_failed_target_count += 1;
                }
            }

            recovered_statuses.push((bucket.clone(), meta));
        }

        if !recovered_statuses.is_empty() {
            let mut status_map = self.resyncer.status_map.write().await;
            status_map.extend(recovered_statuses);
        }

        if !restart_opts.is_empty() || skipped_failed_target_count > 0 {
            info!(
                event = EVENT_REPLICATION_RESYNC_RECOVERED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                recovered_buckets = recovered_bucket_count,
                resumed_targets = restart_opts.len(),
                skipped_failed_targets = skipped_failed_target_count,
                "Recovered replication resync state from persisted metadata; failed targets require manual resync restart"
            );
        }

        for opts in restart_opts {
            let ctx = CancellationToken::new();
            let resync = self.resyncer.clone();
            let storage = self.storage.clone();
            tokio::spawn(async move {
                resync.register_cancel_token(&opts, ctx.clone()).await;
                Box::pin(resync.clone().resync_bucket(ctx, storage, true, opts.clone())).await;
                resync.clear_cancel_token(&opts).await;
            });
        }

        Ok(())
    }
}

async fn queue_mrf_save_admission(
    tx: &Sender<MrfReplicateEntry>,
    entry: MrfReplicateEntry,
    bucket: &str,
    object: &str,
    queue_type: &'static str,
) -> ReplicationQueueAdmission {
    if tx.send(entry).await.is_ok() {
        return ReplicationQueueAdmission::Queued;
    }

    warn!(
        event = EVENT_REPLICATION_MRF_QUEUE_UNAVAILABLE,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_REPLICATION,
        bucket = %bucket,
        object = %object,
        queue_type = queue_type,
        "MRF save channel unavailable — replication failure entry could not be persisted for retry"
    );
    ReplicationQueueAdmission::Missed
}

/// Encodes `entries` and overwrites the MRF persistence file.
/// Returns `true` on success; on failure logs the error and returns `false`.
/// Callers must NOT clear their in-memory buffer on `false` so the next tick
/// can retry — otherwise a transient storage error permanently drops the batch.
async fn flush_mrf_to_disk<S: ReplicationObjectIO>(entries: &[MrfReplicateEntry], storage: &Arc<S>) -> bool {
    match encode_mrf_file(entries) {
        Ok(data) => {
            if let Err(e) =
                ReplicationConfigStore::save(storage.clone(), ReplicationMetadataStore::MRF_REPLICATION_FILE, data).await
            {
                warn!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    count = entries.len(),
                    error = %e,
                    "Failed to flush MRF entries to disk"
                );
                return false;
            }
            true
        }
        Err(e) => {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                count = entries.len(),
                error = %e,
                "Failed to encode MRF entries for disk flush"
            );
            false
        }
    }
}

/// Load bucket resync metadata from disk
async fn load_bucket_resync_metadata<S: ReplicationObjectIO>(
    bucket: &str,
    obj_api: Arc<S>,
) -> Result<BucketReplicationResyncStatus, EcstoreError> {
    let mut brs = BucketReplicationResyncStatus::new();

    let resync_file_path = ReplicationMetadataStore::bucket_resync_file_path(bucket);

    let data = match ReplicationConfigStore::read(obj_api, &resync_file_path).await {
        Ok(data) => data,
        Err(EcstoreError::ConfigNotFound) => return Ok(brs),
        Err(err) => return Err(err),
    };

    if data.is_empty() {
        // Seems to be empty
        return Ok(brs);
    }

    brs = decode_resync_file(&data)?;

    Ok(brs)
}

// Define a trait object type for the replication pool
pub type DynReplicationPool = dyn ReplicationPoolTrait + Send + Sync;

/// Trait that abstracts the replication pool operations
#[async_trait::async_trait]
pub trait ReplicationPoolTrait: std::fmt::Debug {
    fn active_workers(&self) -> i32;
    fn active_mrf_workers(&self) -> i32;
    fn active_lrg_workers(&self) -> i32;
    async fn queue_replica_task(&self, ri: ReplicateObjectInfo) -> ReplicationQueueAdmission;
    async fn queue_replica_delete_task(&self, ri: DeletedObjectReplicationInfo) -> ReplicationQueueAdmission;
    async fn resize(&self, priority: ReplicationPriority, max_workers: usize, max_l_workers: usize);
    async fn get_bucket_resync_status(&self, bucket: &str) -> Result<BucketReplicationResyncStatus, EcstoreError>;
    async fn cancel_bucket_resync(&self, opts: ResyncOpts) -> Result<(), EcstoreError>;
    async fn start_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<(), EcstoreError>;
    async fn init_resync(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
        buckets: Vec<String>,
    ) -> Result<(), EcstoreError>;
}

// Implement the trait for ReplicationPool
#[async_trait::async_trait]
impl<S: ReplicationStorage> ReplicationPoolTrait for ReplicationPool<S> {
    fn active_workers(&self) -> i32 {
        ReplicationPool::<S>::active_workers(self)
    }

    fn active_mrf_workers(&self) -> i32 {
        ReplicationPool::<S>::active_mrf_workers(self)
    }

    fn active_lrg_workers(&self) -> i32 {
        ReplicationPool::<S>::active_lrg_workers(self)
    }

    async fn queue_replica_task(&self, ri: ReplicateObjectInfo) -> ReplicationQueueAdmission {
        self.queue_replica_task(ri).await
    }

    async fn queue_replica_delete_task(&self, ri: DeletedObjectReplicationInfo) -> ReplicationQueueAdmission {
        self.queue_replica_delete_task(ri).await
    }

    async fn resize(&self, priority: ReplicationPriority, max_workers: usize, max_l_workers: usize) {
        self.resize(priority, max_workers, max_l_workers).await;
    }

    async fn get_bucket_resync_status(&self, bucket: &str) -> Result<BucketReplicationResyncStatus, EcstoreError> {
        self.get_bucket_resync_status(bucket).await
    }

    async fn cancel_bucket_resync(&self, opts: ResyncOpts) -> Result<(), EcstoreError> {
        self.cancel_bucket_resync(opts).await
    }

    async fn start_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<(), EcstoreError> {
        self.start_bucket_resync(opts).await
    }

    async fn init_resync(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
        buckets: Vec<String>,
    ) -> Result<(), EcstoreError> {
        self.init_resync_internal(cancellation_token, buckets).await
    }
}

lazy_static! {
    pub static ref GLOBAL_REPLICATION_POOL: tokio::sync::OnceCell<Arc<DynReplicationPool>> = tokio::sync::OnceCell::new();
    pub static ref GLOBAL_REPLICATION_STATS: tokio::sync::OnceCell<Arc<ReplicationStats>> = tokio::sync::OnceCell::new();
}

/// Initializes background replication with the given options
pub async fn init_background_replication<S: ReplicationStorage>(storage: Arc<S>) {
    let stats = GLOBAL_REPLICATION_STATS
        .get_or_init(|| async {
            let stats = Arc::new(ReplicationStats::new());
            stats.start_background_tasks().await;
            stats
        })
        .await;

    let _pool = GLOBAL_REPLICATION_POOL
        .get_or_init(|| async {
            let pool = ReplicationPool::new(ReplicationPoolOpts::default(), stats.clone(), storage).await;
            pool as Arc<DynReplicationPool>
        })
        .await;

    assert!(runtime_sources::replication_runtime_initialized());
}

pub fn get_global_replication_pool() -> Option<Arc<DynReplicationPool>> {
    runtime_sources::replication_pool()
}

pub fn get_global_replication_stats() -> Option<Arc<ReplicationStats>> {
    runtime_sources::replication_stats()
}

pub(crate) async fn schedule_replication<S: ReplicationStorage>(
    oi: ObjectInfo,
    o: Arc<S>,
    dsc: ReplicateDecision,
    op_type: ReplicationType,
) {
    let tgt_statuses = replication_statuses_map(&oi.replication_status_internal.clone().unwrap_or_default());
    let purge_statuses = version_purge_statuses_map(&oi.version_purge_status_internal.clone().unwrap_or_default());
    let tm = get_str(&oi.user_defined, SUFFIX_REPLICATION_TIMESTAMP)
        .map(|v| OffsetDateTime::parse(&v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH));
    let mut rstate = oi.replication_state();
    rstate.replicate_decision_str = dsc.to_string();
    let asz = oi.get_actual_size().unwrap_or_default();

    let mut ri = ReplicateObjectInfo {
        name: oi.name,
        size: oi.size,
        actual_size: asz,
        bucket: oi.bucket,
        version_id: oi.version_id,
        etag: oi.etag,
        mod_time: oi.mod_time,
        replication_status: oi.replication_status,
        replication_status_internal: oi.replication_status_internal,
        delete_marker: oi.delete_marker,
        version_purge_status_internal: oi.version_purge_status_internal,
        version_purge_status: oi.version_purge_status,

        replication_state: Some(rstate),
        op_type,
        dsc: dsc.clone(),
        target_statuses: tgt_statuses,
        target_purge_statuses: purge_statuses,
        replication_timestamp: tm,
        user_tags: (*oi.user_tags).clone(),
        checksum: None,
        retry_count: 0,
        event_type: "".to_string(),
        existing_obj_resync: ResyncDecision::default(),
        ssec: false,
    };

    if ri.ssec {
        ri.checksum = oi.checksum
    }
    if dsc.is_synchronous() {
        replicate_object(ri, o).await
    } else if let Some(pool) = runtime_sources::replication_pool() {
        let _ = pool.queue_replica_task(ri).await;
    }
}

pub(crate) async fn schedule_replication_delete(dv: DeletedObjectReplicationInfo) {
    if let Some(pool) = runtime_sources::replication_pool() {
        let _ = pool.queue_replica_delete_task(dv.clone()).await;
    }

    if let (Some(rs), Some(stats)) = (dv.delete_object.replication_state, runtime_sources::replication_stats()) {
        for (k, _v) in rs.targets.iter() {
            let ri = ReplicatedTargetInfo {
                arn: k.clone(),
                size: 0,
                duration: Duration::default(),
                op_type: ReplicationType::Delete,
                ..Default::default()
            };
            stats
                .update(&dv.bucket, &ri, ReplicationStatusType::Pending, ReplicationStatusType::Empty)
                .await;
        }
    }
}

/// QueueReplicationHeal is a wrapper for queue_replication_heal_internal
pub async fn queue_replication_heal(bucket: &str, oi: ObjectInfo, retry_count: u32) {
    // ignore modtime zero objects
    if oi.mod_time.is_none() || oi.mod_time == Some(OffsetDateTime::UNIX_EPOCH) {
        return;
    }

    let rcfg = match ReplicationMetadataStore::replication_config(bucket).await {
        Ok((config, _)) => config,
        Err(err) => {
            debug!(
                event = EVENT_REPLICATION_CONFIG_LOOKUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                bucket,
                error = %err,
                reason = "config_lookup_failed",
                "Skipped replication heal queue due to missing replication config"
            );

            return;
        }
    };

    let tgts = match ReplicationTargetStore::list_bucket_targets(bucket).await {
        Ok(targets) => Some(targets),
        Err(err) => {
            debug!(
                event = EVENT_REPLICATION_CONFIG_LOOKUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                bucket,
                error = %err,
                reason = "target_list_failed",
                "Skipped bucket target list during replication heal queue setup"
            );
            None
        }
    };

    let rcfg_wrapper = ReplicationConfig::new(Some(rcfg), tgts);
    queue_replication_heal_internal(bucket, oi, rcfg_wrapper, retry_count).await;
}

/// queue_replication_heal_internal enqueues objects that failed replication OR eligible for resyncing through
/// an ongoing resync operation or via existing objects replication configuration setting.
pub(crate) async fn queue_replication_heal_internal(
    _bucket: &str,
    oi: ObjectInfo,
    rcfg: ReplicationConfig,
    retry_count: u32,
) -> ReplicationHealQueueResult {
    let mut roi = ReplicateObjectInfo::default();

    // ignore modtime zero objects
    if oi.mod_time.is_none() || oi.mod_time == Some(OffsetDateTime::UNIX_EPOCH) {
        return ReplicationHealQueueResult {
            object_info: roi,
            admission: ReplicationQueueAdmission::Skipped,
        };
    }

    if rcfg.config.is_none() || rcfg.remotes.is_none() {
        return ReplicationHealQueueResult {
            object_info: roi,
            admission: ReplicationQueueAdmission::Skipped,
        };
    }

    roi = get_heal_replicate_object_info(&oi, &rcfg).await;
    roi.retry_count = retry_count;

    match replication_heal_queue_action(&mut roi) {
        ReplicationHealQueueAction::Skip => ReplicationHealQueueResult {
            object_info: roi,
            admission: ReplicationQueueAdmission::Skipped,
        },
        ReplicationHealQueueAction::QueueObject => {
            let admission = if let Some(pool) = runtime_sources::replication_pool() {
                pool.queue_replica_task(roi.clone()).await
            } else {
                ReplicationQueueAdmission::Missed
            };
            ReplicationHealQueueResult {
                object_info: roi,
                admission,
            }
        }
        ReplicationHealQueueAction::QueueDelete(dv) => {
            let admission = if let Some(pool) = runtime_sources::replication_pool() {
                pool.queue_replica_delete_task(dv).await
            } else {
                ReplicationQueueAdmission::Missed
            };
            ReplicationHealQueueResult {
                object_info: roi,
                admission,
            }
        }
        ReplicationHealQueueAction::QueueResyncDeletes(batch) => {
            let admission = queue_replicate_deletes(batch).await;
            ReplicationHealQueueResult {
                object_info: roi,
                admission,
            }
        }
    }
}

async fn queue_replicate_deletes(batch: ReplicationHealResyncDeletes) -> ReplicationQueueAdmission {
    let mut admission = ReplicationQueueAdmission::Skipped;
    for dv in batch.target_delete_infos() {
        let target_admission = if let Some(pool) = runtime_sources::replication_pool() {
            pool.queue_replica_delete_task(dv).await
        } else {
            ReplicationQueueAdmission::Missed
        };
        admission.merge(target_admission);
    }
    admission
}

#[cfg(test)]
mod tests {
    use super::super::replication_resync_boundary::{decode_mrf_file, encode_mrf_file};
    use super::*;
    use uuid::Uuid;

    #[test]
    fn replication_queue_admission_combines_target_results() {
        let mut admission = ReplicationQueueAdmission::Skipped;

        admission.merge(ReplicationQueueAdmission::Queued);
        assert_eq!(admission, ReplicationQueueAdmission::Queued);

        admission.merge(ReplicationQueueAdmission::Missed);
        assert_eq!(admission, ReplicationQueueAdmission::Missed);
    }

    #[tokio::test]
    async fn mrf_save_admission_waits_for_capacity_instead_of_dropping() {
        let (tx, mut rx) = mpsc::channel(1);
        let first = MrfReplicateEntry {
            bucket: "bucket".to_string(),
            object: "first".to_string(),
            version_id: None,
            retry_count: 1,
            size: 1,
            op: MrfOpKind::Object,
            delete_marker_version_id: None,
            delete_marker: false,
        };
        let second = MrfReplicateEntry {
            object: "second".to_string(),
            ..first.clone()
        };

        tx.try_send(first).expect("first MRF entry should fill the test channel");

        let admission = queue_mrf_save_admission(&tx, second, "bucket", "second", "test");
        tokio::pin!(admission);

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut admission).await.is_err(),
            "full MRF channel should apply backpressure instead of returning Missed"
        );

        let received = rx.recv().await.expect("first MRF entry should still be queued");
        assert_eq!(received.object, "first");

        let admission = tokio::time::timeout(Duration::from_secs(1), &mut admission)
            .await
            .expect("MRF admission should finish once capacity is available");
        assert_eq!(admission, ReplicationQueueAdmission::Queued);

        let received = rx
            .recv()
            .await
            .expect("second MRF entry should be queued after capacity opens");
        assert_eq!(received.object, "second");
    }

    #[test]
    fn auto_resume_resync_only_for_inflight_states() {
        assert!(should_auto_resume_resync(ResyncStatusType::ResyncPending));
        assert!(should_auto_resume_resync(ResyncStatusType::ResyncStarted));
        assert!(!should_auto_resume_resync(ResyncStatusType::NoResync));
        assert!(!should_auto_resume_resync(ResyncStatusType::ResyncCanceled));
        assert!(!should_auto_resume_resync(ResyncStatusType::ResyncCompleted));
        assert!(!should_auto_resume_resync(ResyncStatusType::ResyncFailed));
    }

    // ── MrfReplicateEntry encode/decode roundtrips ────────────────────────────

    #[test]
    fn mrf_entry_object_roundtrip() {
        let vid = Uuid::new_v4();
        let entry = MrfReplicateEntry {
            bucket: "my-bucket".to_string(),
            object: "path/to/obj".to_string(),
            version_id: Some(vid),
            retry_count: 3,
            size: 1024,
            op: MrfOpKind::Object,
            delete_marker_version_id: None,
            delete_marker: false,
        };

        let encoded = encode_mrf_file(std::slice::from_ref(&entry)).expect("encode");
        let decoded = decode_mrf_file(&encoded).expect("decode");

        assert_eq!(decoded.len(), 1);
        let got = &decoded[0];
        assert_eq!(got.bucket, "my-bucket");
        assert_eq!(got.object, "path/to/obj");
        assert_eq!(got.version_id, Some(vid));
        assert_eq!(got.retry_count, 3);
        assert_eq!(got.size, 1024);
        assert_eq!(got.op, MrfOpKind::Object);
        assert_eq!(got.delete_marker_version_id, None);
        assert!(!got.delete_marker);
    }

    #[test]
    fn mrf_entry_delete_marker_roundtrip() {
        let dm_vid = Uuid::new_v4();
        let entry = MrfReplicateEntry {
            bucket: "del-bucket".to_string(),
            object: "key".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            delete_marker_version_id: Some(dm_vid),
            delete_marker: true,
        };

        let encoded = encode_mrf_file(std::slice::from_ref(&entry)).expect("encode");
        let decoded = decode_mrf_file(&encoded).expect("decode");

        assert_eq!(decoded.len(), 1);
        let got = &decoded[0];
        assert_eq!(got.bucket, "del-bucket");
        assert_eq!(got.object, "key");
        assert_eq!(got.version_id, None);
        assert_eq!(got.op, MrfOpKind::Delete);
        assert_eq!(got.delete_marker_version_id, Some(dm_vid));
        assert!(got.delete_marker);
    }

    #[test]
    fn mrf_entry_versioned_delete_roundtrip() {
        let vid = Uuid::new_v4();
        let entry = MrfReplicateEntry {
            bucket: "ver-bucket".to_string(),
            object: "versioned-key".to_string(),
            version_id: Some(vid),
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            delete_marker_version_id: None,
            delete_marker: false,
        };

        let encoded = encode_mrf_file(&[entry]).expect("encode");
        let decoded = decode_mrf_file(&encoded).expect("decode");

        assert_eq!(decoded.len(), 1);
        let got = &decoded[0];
        assert_eq!(got.op, MrfOpKind::Delete);
        assert_eq!(got.version_id, Some(vid));
        assert_eq!(got.delete_marker_version_id, None);
        assert!(!got.delete_marker);
    }

    #[test]
    fn mrf_entry_mixed_batch_roundtrip() {
        let obj_vid = Uuid::new_v4();
        let del_dm_vid = Uuid::new_v4();
        let entries = vec![
            MrfReplicateEntry {
                bucket: "b".to_string(),
                object: "obj".to_string(),
                version_id: Some(obj_vid),
                retry_count: 1,
                size: 512,
                op: MrfOpKind::Object,
                delete_marker_version_id: None,
                delete_marker: false,
            },
            MrfReplicateEntry {
                bucket: "b".to_string(),
                object: "del".to_string(),
                version_id: None,
                retry_count: 0,
                size: 0,
                op: MrfOpKind::Delete,
                delete_marker_version_id: Some(del_dm_vid),
                delete_marker: true,
            },
        ];

        let encoded = encode_mrf_file(&entries).expect("encode");
        let decoded = decode_mrf_file(&encoded).expect("decode");

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].op, MrfOpKind::Object);
        assert_eq!(decoded[0].version_id, Some(obj_vid));
        assert_eq!(decoded[1].op, MrfOpKind::Delete);
        assert_eq!(decoded[1].delete_marker_version_id, Some(del_dm_vid));
        assert!(decoded[1].delete_marker);
    }

    // ── Recovery replay routing ───────────────────────────────────────────────

    #[test]
    fn mrf_entry_op_routes_correctly() {
        // Object entries must have op=Object so the processor calls get_object_info + heal.
        let obj_entry = MrfReplicateEntry {
            bucket: "b".to_string(),
            object: "o".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Object,
            delete_marker_version_id: None,
            delete_marker: false,
        };
        assert_eq!(obj_entry.op, MrfOpKind::Object);

        // Delete entries must have op=Delete so the processor calls schedule_replication_delete.
        let del_entry = MrfReplicateEntry {
            bucket: "b".to_string(),
            object: "o".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            delete_marker_version_id: Some(Uuid::new_v4()),
            delete_marker: true,
        };
        assert_eq!(del_entry.op, MrfOpKind::Delete);

        // Entries written by old code (before the op field existed) must deserialise as Object
        // so existing recovery behaviour is preserved.
        let legacy_entry = MrfReplicateEntry {
            bucket: "b".to_string(),
            object: "o".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::default(),
            delete_marker_version_id: None,
            delete_marker: false,
        };
        assert_eq!(legacy_entry.op, MrfOpKind::Object, "legacy default must be Object");
    }

    #[test]
    fn mrf_legacy_file_without_op_field_decoded_as_object() {
        // Hand-build the exact bytes a pre-MrfOpKind binary would have written to disk.
        // The old MrfReplicateEntry had only 4 persisted keys (versionID is omitted when
        // None due to skip_serializing_if): bucket, object, retryCount, size.
        // There is no "op", "deleteMarker", or "deleteMarkerVersionID" key.
        //
        // This proves that #[serde(default)] on the `op` field carries real weight:
        // if you remove that attribute, rmp_serde will return an error on this payload
        // and the test will fail.
        let mut msgpack = Vec::new();
        // Outer: array of 1 (the Vec<MrfReplicateEntry>)
        rmp::encode::write_array_len(&mut msgpack, 1).unwrap();
        // Inner: named map with the 4 original fields only — no "op", no "deleteMarker*"
        rmp::encode::write_map_len(&mut msgpack, 4).unwrap();
        rmp::encode::write_str(&mut msgpack, "bucket").unwrap();
        rmp::encode::write_str(&mut msgpack, "old-bucket").unwrap();
        rmp::encode::write_str(&mut msgpack, "object").unwrap();
        rmp::encode::write_str(&mut msgpack, "old-key").unwrap();
        rmp::encode::write_str(&mut msgpack, "retryCount").unwrap();
        rmp::encode::write_i32(&mut msgpack, 2).unwrap();
        rmp::encode::write_str(&mut msgpack, "size").unwrap();
        rmp::encode::write_i64(&mut msgpack, 100).unwrap();

        // Prepend the MRF file header: format=1 (LE u16) || version=1 (LE u16)
        let mut data = Vec::with_capacity(4 + msgpack.len());
        data.extend_from_slice(&1u16.to_le_bytes()); // MRF_META_FORMAT
        data.extend_from_slice(&1u16.to_le_bytes()); // MRF_META_VERSION
        data.extend_from_slice(&msgpack);

        let decoded = decode_mrf_file(&data).expect("legacy payload must decode without error");
        assert_eq!(decoded.len(), 1);
        let entry = &decoded[0];
        assert_eq!(entry.bucket, "old-bucket");
        assert_eq!(entry.object, "old-key");
        assert_eq!(entry.retry_count, 2);
        assert_eq!(entry.size, 100);
        assert_eq!(entry.version_id, None);
        // The "op" key was absent — #[serde(default)] must fill in MrfOpKind::Object.
        assert_eq!(entry.op, MrfOpKind::Object, "missing op key must default to Object");
        assert!(!entry.delete_marker);
        assert_eq!(entry.delete_marker_version_id, None);
    }
}
