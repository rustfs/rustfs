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
use super::replication_lock_boundary::ReplicationLockTiming;
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
use super::replication_target_boundary::{ReplicationTargetStore, replication_object_is_ssec_encrypted};
use super::replication_versioning_boundary::ReplicationVersioningStore;
use super::runtime_boundary as runtime_sources;
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

#[derive(Debug, Default)]
pub struct DurableMrfBacklog {
    pub available: bool,
    pub entries: Vec<MrfReplicateEntry>,
}

fn durable_mrf_backlog_from_read(result: Result<Vec<u8>, EcstoreError>) -> DurableMrfBacklog {
    match result {
        Ok(data) => match decode_mrf_file(&data) {
            Ok(entries) if entries.iter().all(|entry| entry.size >= 0) => DurableMrfBacklog {
                available: true,
                entries,
            },
            Ok(_) | Err(_) => DurableMrfBacklog::default(),
        },
        Err(EcstoreError::ConfigNotFound) => DurableMrfBacklog {
            available: true,
            entries: Vec::new(),
        },
        Err(_) => DurableMrfBacklog::default(),
    }
}

pub async fn read_durable_mrf_backlog<S: ReplicationObjectIO>(storage: Arc<S>) -> DurableMrfBacklog {
    durable_mrf_backlog_from_read(ReplicationConfigStore::read(storage, ReplicationMetadataStore::MRF_REPLICATION_FILE).await)
}

#[derive(Debug, thiserror::Error)]
#[error("replication resync {active_resync_id} is already active for {bucket}/{arn}")]
struct ResyncActiveConflictError {
    bucket: String,
    arn: String,
    active_resync_id: String,
}

pub fn resync_start_conflict_id(error: &EcstoreError) -> Option<&str> {
    match error {
        EcstoreError::Io(io_error) => io_error
            .get_ref()?
            .downcast_ref::<ResyncActiveConflictError>()
            .map(|conflict| conflict.active_resync_id.as_str()),
        _ => None,
    }
}

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

                        // Restore the original delete-marker mtime persisted with the entry so
                        // the replica keeps the source timestamp. Old MRF files lack this field
                        // (delete_marker_mtime = None) — fall back to None so the replica is
                        // stamped with the current time, preserving pre-#867 behaviour
                        // (backlog#867).
                        let delete_marker_mtime = entry
                            .delete_marker_mtime
                            .and_then(|nanos| OffsetDateTime::from_unix_timestamp_nanos(nanos as i128).ok());

                        let dv = DeletedObjectReplicationInfo {
                            delete_object: ReplicationDeletedObject {
                                object_name: entry.object.clone(),
                                version_id: entry.version_id,
                                delete_marker_version_id: entry.delete_marker_version_id,
                                delete_marker: entry.delete_marker,
                                delete_marker_mtime,
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
    /// writes them to the on-disk MRF file every flush interval (default 10s,
    /// overridable via `RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS`) or when 1 000 new
    /// entries accumulate.  Each flush rewrites the whole cumulative backlog so no
    /// previously-persisted (and not-yet-replayed) entry is lost; the file is only
    /// consumed and cleared at startup.
    async fn start_mrf_persister(&self) {
        let Some(mut rx) = self.mrf_save_rx.lock().await.take() else {
            return;
        };
        let storage = self.storage.clone();

        let handle = tokio::spawn(async move {
            // The on-disk MRF file is a restart-recovery backstop: entries are
            // only replayed (and the file cleared) at startup, never during the
            // run. So the file must hold the *cumulative* set of overflow entries
            // written this run. `pending` is therefore kept cumulative and the
            // whole set is rewritten on each flush — clearing it after a flush
            // let the next flush overwrite the file and drop everything written
            // earlier (backlog#859 / #799 B10). Bounded by `MRF_PENDING_CAP` so a
            // sustained failure storm can't grow it without limit.
            const MRF_PENDING_CAP: usize = 200_000;
            let mut pending: Vec<MrfReplicateEntry> = Vec::new();
            let mut flushed_len = 0usize;
            let mut dirty = false;
            let mut capped = false;
            // Flush interval: `RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS` (default 10000ms,
            // clamped to >=10ms), read once when the persister task starts.
            let mut interval = tokio::time::interval(super::replication_timing::mrf_flush_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    entry = rx.recv() => match entry {
                        Some(e) => {
                            if pending.len() >= MRF_PENDING_CAP {
                                if !capped {
                                    capped = true;
                                    warn!(
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_REPLICATION,
                                        cap = MRF_PENDING_CAP,
                                        "MRF pending backlog hit cap — dropping further recovery entries for this run"
                                    );
                                }
                                continue;
                            }
                            pending.push(e);
                            dirty = true;
                            // Flush eagerly once enough new entries have accumulated
                            // since the last write (measured against the flushed
                            // set, not the absolute length, so a large backlog is
                            // not rewritten on every single add).
                            if pending.len() - flushed_len >= 1000 && flush_mrf_to_disk(&pending, &storage).await {
                                flushed_len = pending.len();
                                dirty = false;
                            }
                        }
                        None => {
                            // Channel closed (pool shutting down) — final flush.
                            if dirty {
                                flush_mrf_to_disk(&pending, &storage).await;
                            }
                            break;
                        }
                    },
                    _ = interval.tick() => {
                        if dirty && flush_mrf_to_disk(&pending, &storage).await {
                            flushed_len = pending.len();
                            dirty = false;
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
        let new_run = self.clone().admit_bucket_resync(opts.clone()).await?;
        self.activate_bucket_resync(opts, !new_run).await
    }

    pub async fn admit_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<bool, EcstoreError> {
        tokio::spawn(async move { self.admit_bucket_resync_transaction(opts).await })
            .await
            .map_err(|error| EcstoreError::other(format!("replication resync admission task failed: {error}")))?
    }

    async fn admit_bucket_resync_transaction(self: Arc<Self>, opts: ResyncOpts) -> Result<bool, EcstoreError> {
        let admission_lock_key = ReplicationMetadataStore::resync_admission_lock_key(&opts.bucket);
        let admission_lock = self
            .storage
            .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), &admission_lock_key)
            .await?;
        // Lock order: bucket resync admission lock -> resync status config-object lock.
        let _admission_guard = match admission_lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
            Ok(guard) => guard,
            Err(lock_error) => {
                if let Ok(status) = load_bucket_resync_metadata(&opts.bucket, self.storage.clone()).await {
                    self.resyncer.status_map.write().await.insert(opts.bucket.clone(), status);
                }
                return Err(EcstoreError::from(lock_error));
            }
        };

        let mut bucket_status = load_bucket_resync_metadata(&opts.bucket, self.storage.clone()).await?;
        if let Some(active) = bucket_status.targets_map.get(&opts.arn) {
            if active.resync_id == opts.resync_id {
                self.resyncer
                    .status_map
                    .write()
                    .await
                    .insert(opts.bucket.clone(), bucket_status);
                return Ok(false);
            }
            if should_auto_resume_resync(active.resync_status) {
                let active_resync_id = active.resync_id.clone();
                self.resyncer
                    .status_map
                    .write()
                    .await
                    .insert(opts.bucket.clone(), bucket_status);
                return Err(EcstoreError::other(ResyncActiveConflictError {
                    bucket: opts.bucket.clone(),
                    arn: opts.arn.clone(),
                    active_resync_id,
                }));
            }
        }

        let now = OffsetDateTime::now_utc();
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

        save_resync_status(&opts.bucket, &bucket_status, self.storage.clone()).await?;
        self.resyncer
            .status_map
            .write()
            .await
            .insert(opts.bucket.clone(), bucket_status);

        Ok(true)
    }

    pub async fn activate_bucket_resync(self: Arc<Self>, opts: ResyncOpts, recovering: bool) -> Result<(), EcstoreError> {
        let bucket_status = load_bucket_resync_metadata(&opts.bucket, self.storage.clone()).await?;
        let Some(target_status) = bucket_status.targets_map.get(&opts.arn) else {
            return Err(EcstoreError::other("replication resync admission is missing"));
        };
        if target_status.resync_id != opts.resync_id {
            return Err(EcstoreError::other(ResyncActiveConflictError {
                bucket: opts.bucket.clone(),
                arn: opts.arn.clone(),
                active_resync_id: target_status.resync_id.clone(),
            }));
        }
        if !should_auto_resume_resync(target_status.resync_status) {
            return Ok(());
        }
        self.resyncer
            .status_map
            .write()
            .await
            .insert(opts.bucket.clone(), bucket_status);

        let resyncer = self.resyncer.clone();
        let storage = self.storage.clone();
        let cancel_token = CancellationToken::new();
        if resyncer.register_cancel_token(&opts, cancel_token.clone()).await {
            tokio::spawn(async move {
                Box::pin(
                    resyncer
                        .clone()
                        .resync_bucket(cancel_token, storage, recovering, opts.clone()),
                )
                .await;
                resyncer.clear_cancel_token(&opts).await;
            });
        }

        Ok(())
    }

    /// Start the resync routine that runs in a loop
    async fn start_resync_routine(self: Arc<Self>, buckets: Vec<String>, cancellation_token: CancellationToken) {
        // Retry-poll sleep upper bound: `RUSTFS_REPL_RESYNC_POLL_MAX_MS`
        // (default 60000ms, clamped to >=10ms), read once when this routine
        // starts. The anti-busy-spin floor is min(1s, max) so the default
        // keeps the historical "sleep at least one second" behavior while
        // short test overrides stay short.
        let max_sleep = super::replication_timing::resync_poll_max_sleep();
        let max_sleep_ms = u64::try_from(max_sleep.as_millis()).unwrap_or(u64::MAX).max(1);
        let floor_sleep = Duration::from_secs(1).min(max_sleep);
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

            // Generate a random duration between 0 and `max_sleep` (default 1 minute)
            use rand::RngExt;
            let duration_millis = rand::rng().random_range(0..max_sleep_ms);
            let mut duration = Duration::from_millis(duration_millis);

            // Make sure to sleep at least `floor_sleep` to avoid high CPU ticks
            if duration < floor_sleep {
                duration = floor_sleep;
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
        let load_resync_lock = match self
            .storage
            .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), "replication/resync/load-resync.lock")
            .await
        {
            Ok(lock) => lock,
            Err(err) => {
                warn!(
                    event = EVENT_REPLICATION_RESYNC_LOAD_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    error = ?err,
                    reason = "leader_lock_create_failed",
                    "Skipped replication resync metadata load"
                );
                return Ok(());
            }
        };
        let _load_resync_guard = match load_resync_lock
            .get_write_lock(ReplicationLockTiming::acquire_timeout())
            .await
        {
            Ok(guard) => guard,
            Err(_) => {
                debug!(
                    event = EVENT_REPLICATION_RESYNC_LOAD_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    reason = "leader_lock_held_by_another_node",
                    "Another node is already loading replication resync metadata"
                );
                return Ok(());
            }
        };

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
                if resync.register_cancel_token(&opts, ctx.clone()).await {
                    Box::pin(resync.clone().resync_bucket(ctx, storage, true, opts.clone())).await;
                    resync.clear_cancel_token(&opts).await;
                }
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
    async fn admit_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<bool, EcstoreError>;
    async fn activate_bucket_resync(self: Arc<Self>, opts: ResyncOpts, recovering: bool) -> Result<(), EcstoreError>;
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

    async fn admit_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<bool, EcstoreError> {
        self.admit_bucket_resync(opts).await
    }

    async fn activate_bucket_resync(self: Arc<Self>, opts: ResyncOpts, recovering: bool) -> Result<(), EcstoreError> {
        self.activate_bucket_resync(opts, recovering).await
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

/// Initializes background replication with the given options.
///
/// Phase 5 (backlog#939): the replication stats/pool moved into the per-instance
/// `InstanceContext`; this owner initializes the current instance's cells
/// (lazily, once — single-instance behavior is unchanged).
pub async fn init_background_replication<S: ReplicationStorage>(storage: Arc<S>) {
    let ctx = crate::runtime::global::current_ctx();

    let stats = ctx
        .replication_stats_cell()
        .get_or_init(|| async {
            let stats = Arc::new(ReplicationStats::new());
            stats.start_background_tasks().await;
            stats
        })
        .await;

    let _pool = ctx
        .replication_pool_cell()
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
    let synchronous = dsc.is_synchronous();
    let ri = replicate_object_info_from_object_info(oi, dsc, op_type);

    if synchronous {
        replicate_object(ri, o).await
    } else if let Some(pool) = runtime_sources::replication_pool() {
        let _ = pool.queue_replica_task(ri).await;
    }
}

fn replicate_object_info_from_object_info(
    oi: ObjectInfo,
    dsc: ReplicateDecision,
    op_type: ReplicationType,
) -> ReplicateObjectInfo {
    let tgt_statuses = replication_statuses_map(&oi.replication_status_internal.clone().unwrap_or_default());
    let purge_statuses = version_purge_statuses_map(&oi.version_purge_status_internal.clone().unwrap_or_default());
    let tm = get_str(&oi.user_defined, SUFFIX_REPLICATION_TIMESTAMP)
        .map(|v| OffsetDateTime::parse(&v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH));
    let mut rstate = oi.replication_state();
    rstate.replicate_decision_str = dsc.to_string();
    let asz = oi.get_actual_size().unwrap_or_default();
    let ssec = replication_object_is_ssec_encrypted(&oi.user_defined);
    let checksum = if ssec { oi.checksum.clone() } else { None };

    ReplicateObjectInfo {
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
        dsc,
        target_statuses: tgt_statuses,
        target_purge_statuses: purge_statuses,
        replication_timestamp: tm,
        user_tags: (*oi.user_tags).clone(),
        checksum,
        retry_count: 0,
        event_type: "".to_string(),
        existing_obj_resync: ResyncDecision::default(),
        ssec,
    }
}

pub(crate) async fn schedule_replication_delete(dv: DeletedObjectReplicationInfo) {
    if let Some(pool) = runtime_sources::replication_pool() {
        let _ = pool.queue_replica_delete_task(dv.clone()).await;
    }

    if let (Some(rs), Some(stats)) = (dv.delete_object.replication_state, runtime_sources::replication_stats()) {
        for k in rs.targets.keys() {
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
    use super::super::replication_resync_boundary::{decode_mrf_file, encode_mrf_file, encode_resync_file};
    use super::super::replication_storage_boundary::{
        DeletedObject, FileInfo, GetObjectReader, HTTPRangeSpec, ListOperations, ObjectIO, ObjectOperations, PutObjReader,
        StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageNamespaceLocking, StorageObjectInfoOrErr, WalkOptions,
    };
    use super::*;
    use std::collections::HashMap;
    use std::fmt::{Debug, Formatter};
    use std::io::Cursor;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use tokio::io::AsyncReadExt;
    use tokio::sync::Notify;
    use uuid::Uuid;

    type TestListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
    type TestListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
    type TestObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, EcstoreError>;

    struct LoadResyncSharedState {
        data: StdMutex<Vec<u8>>,
        lock_manager: Arc<rustfs_lock::GlobalLockManager>,
        first_read_started: Notify,
        delay_first_read: AtomicBool,
        read_count: AtomicUsize,
        write_count: AtomicUsize,
        fail_next_write: AtomicBool,
        block_next_write: AtomicBool,
        write_started: Notify,
        allow_write: Notify,
    }

    struct LoadResyncNodeStore {
        owner: String,
        shared: Arc<LoadResyncSharedState>,
    }

    impl LoadResyncNodeStore {
        fn new(owner: &str, shared: Arc<LoadResyncSharedState>) -> Self {
            Self {
                owner: owner.to_string(),
                shared,
            }
        }
    }

    impl Debug for LoadResyncNodeStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("LoadResyncNodeStore").field("owner", &self.owner).finish()
        }
    }

    #[async_trait::async_trait]
    impl ObjectIO for LoadResyncNodeStore {
        type Error = EcstoreError;
        type RangeSpec = HTTPRangeSpec;
        type HeaderMap = http::HeaderMap;
        type ObjectOptions = ObjectOptions;
        type ObjectInfo = ObjectInfo;
        type GetObjectReader = GetObjectReader;
        type PutObjectReader = PutObjReader;

        async fn get_object_reader(
            &self,
            _bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::GetObjectReader, Self::Error> {
            if !object.ends_with("/.replication/resync.bin") {
                return Err(EcstoreError::FileNotFound);
            }

            let read_index = self.shared.read_count.fetch_add(1, Ordering::SeqCst);
            if read_index == 0 && self.shared.delay_first_read.load(Ordering::SeqCst) {
                self.shared.first_read_started.notify_waiters();
                tokio::time::sleep(Duration::from_millis(1_500)).await;
            }

            let data = self
                .shared
                .data
                .lock()
                .expect("test data lock should not be poisoned")
                .clone();
            if data.is_empty() {
                return Err(EcstoreError::FileNotFound);
            }
            let size = i64::try_from(data.len()).expect("test metadata length should fit i64");
            Ok(Self::GetObjectReader {
                stream: Box::new(Cursor::new(data)),
                object_info: ObjectInfo {
                    size,
                    actual_size: size,
                    ..Default::default()
                },
                buffered_body: None,
                body_source: Default::default(),
            })
        }

        async fn put_object(
            &self,
            _bucket: &str,
            _object: &str,
            data: &mut Self::PutObjectReader,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            if self.shared.fail_next_write.swap(false, Ordering::SeqCst) {
                return Err(EcstoreError::Unexpected);
            }
            if self.shared.block_next_write.swap(false, Ordering::SeqCst) {
                self.shared.write_started.notify_one();
                self.shared.allow_write.notified().await;
            }
            let mut encoded = Vec::new();
            data.stream.read_to_end(&mut encoded).await.map_err(EcstoreError::from)?;
            *self.shared.data.lock().expect("test data lock should not be poisoned") = encoded;
            self.shared.write_count.fetch_add(1, Ordering::SeqCst);
            Ok(ObjectInfo::default())
        }
    }

    #[async_trait::async_trait]
    impl ObjectOperations for LoadResyncNodeStore {
        type Error = EcstoreError;
        type ObjectInfo = ObjectInfo;
        type ObjectOptions = ObjectOptions;
        type FileInfo = FileInfo;
        type ObjectToDelete = ObjectToDelete;
        type DeletedObject = DeletedObject;

        async fn get_object_info(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn verify_object_integrity(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<(), Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn copy_object(
            &self,
            _src_bucket: &str,
            _src_object: &str,
            _dst_bucket: &str,
            _dst_object: &str,
            _src_info: &mut Self::ObjectInfo,
            _src_opts: &Self::ObjectOptions,
            _dst_opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn delete_object_version(
            &self,
            _bucket: &str,
            _object: &str,
            _fi: &Self::FileInfo,
            _force_del_marker: bool,
        ) -> Result<(), Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn delete_object(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn delete_objects(
            &self,
            _bucket: &str,
            _objects: Vec<Self::ObjectToDelete>,
            _opts: Self::ObjectOptions,
        ) -> (Vec<Self::DeletedObject>, Vec<Option<Self::Error>>) {
            (Vec::new(), vec![Some(EcstoreError::NotImplemented)])
        }

        async fn put_object_metadata(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn get_object_tags(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<String, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn put_object_tags(
            &self,
            _bucket: &str,
            _object: &str,
            _tags: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn delete_object_tags(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn add_partial(&self, _bucket: &str, _object: &str, _version_id: &str) -> Result<(), Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn transition_object(&self, _bucket: &str, _object: &str, _opts: &Self::ObjectOptions) -> Result<(), Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn restore_transitioned_object(
            self: Arc<Self>,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<(), Self::Error> {
            Err(EcstoreError::NotImplemented)
        }
    }

    #[async_trait::async_trait]
    impl ListOperations for LoadResyncNodeStore {
        type Error = EcstoreError;
        type ListObjectsV2Info = TestListObjectsV2Info;
        type ListObjectVersionsInfo = TestListObjectVersionsInfo;
        type ObjectInfoOrErr = TestObjectInfoOrErr;
        type WalkOptions = WalkOptions;
        type WalkCancellation = CancellationToken;
        type WalkResultSender = Sender<TestObjectInfoOrErr>;

        async fn list_objects_v2(
            self: Arc<Self>,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<String>,
            _delimiter: Option<String>,
            _max_keys: i32,
            _fetch_owner: bool,
            _start_after: Option<String>,
            _incl_deleted: bool,
        ) -> Result<Self::ListObjectsV2Info, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn list_object_versions(
            self: Arc<Self>,
            _bucket: &str,
            _prefix: &str,
            _marker: Option<String>,
            _version_marker: Option<String>,
            _delimiter: Option<String>,
            _max_keys: i32,
        ) -> Result<Self::ListObjectVersionsInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn walk(
            self: Arc<Self>,
            _rx: Self::WalkCancellation,
            _bucket: &str,
            _prefix: &str,
            _result: Self::WalkResultSender,
            _opts: Self::WalkOptions,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl StorageNamespaceLocking for LoadResyncNodeStore {
        type Error = EcstoreError;
        type NamespaceLock = rustfs_lock::NamespaceLockWrapper;

        async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<Self::NamespaceLock, Self::Error> {
            let lock =
                rustfs_lock::NamespaceLock::with_local_manager("load-resync-test".to_string(), self.shared.lock_manager.clone());
            Ok(rustfs_lock::NamespaceLockWrapper::new(
                lock,
                rustfs_lock::ObjectKey::new(bucket.to_string(), object.to_string()),
                self.owner.clone(),
            ))
        }
    }

    async fn new_test_replication_pool(storage: Arc<LoadResyncNodeStore>) -> Arc<ReplicationPool<LoadResyncNodeStore>> {
        let (mrf_replica_tx, mrf_replica_rx) = mpsc::channel(1);
        let (mrf_save_tx, mrf_save_rx) = mpsc::channel(1);
        let (mrf_worker_kill_tx, _) = mpsc::channel(1);
        let (mrf_stop_tx, _) = mpsc::channel(1);

        Arc::new(ReplicationPool {
            active_workers: Arc::new(AtomicI32::new(0)),
            active_lrg_workers: Arc::new(AtomicI32::new(0)),
            active_mrf_workers: Arc::new(AtomicI32::new(0)),
            storage,
            priority: RwLock::new(ReplicationPoolOpts::default().priority),
            max_workers: RwLock::new(WORKER_MAX_LIMIT),
            max_l_workers: RwLock::new(LARGE_WORKER_COUNT),
            stats: Arc::new(ReplicationStats::new()),
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
        })
    }

    fn load_resync_test_metadata() -> Vec<u8> {
        let mut status = BucketReplicationResyncStatus::new();
        status.targets_map.insert(
            "arn:test".to_string(),
            TargetReplicationResyncStatus {
                bucket: "load-resync-lock".to_string(),
                resync_status: ResyncStatusType::ResyncCompleted,
                ..Default::default()
            },
        );
        encode_resync_file(&status).expect("test resync metadata should encode")
    }

    fn empty_resync_shared_state() -> Arc<LoadResyncSharedState> {
        Arc::new(LoadResyncSharedState {
            data: StdMutex::new(Vec::new()),
            lock_manager: Arc::new(rustfs_lock::GlobalLockManager::new()),
            first_read_started: Notify::new(),
            delay_first_read: AtomicBool::new(false),
            read_count: AtomicUsize::new(0),
            write_count: AtomicUsize::new(0),
            fail_next_write: AtomicBool::new(false),
            block_next_write: AtomicBool::new(false),
            write_started: Notify::new(),
            allow_write: Notify::new(),
        })
    }

    async fn hold_resync_runtime_lock(
        shared: &Arc<LoadResyncSharedState>,
        bucket: &str,
        arn: &str,
    ) -> rustfs_lock::NamespaceLockGuard {
        let lock =
            rustfs_lock::NamespaceLock::with_local_manager("resync-start-blocker".to_string(), shared.lock_manager.clone());
        let lock = rustfs_lock::NamespaceLockWrapper::new(
            lock,
            rustfs_lock::ObjectKey::new(
                ReplicationMetadataStore::rustfs_meta_bucket().to_string(),
                ReplicationMetadataStore::resync_lock_key(bucket, arn),
            ),
            "blocker".to_string(),
        );
        lock.get_write_lock(Duration::from_secs(1))
            .await
            .expect("test should hold the runtime resync lock")
    }

    fn test_resync_opts(bucket: &str, arn: &str, id: &str) -> ResyncOpts {
        ResyncOpts {
            bucket: bucket.to_string(),
            arn: arn.to_string(),
            resync_id: id.to_string(),
            resync_before: Some(OffsetDateTime::UNIX_EPOCH),
        }
    }

    #[tokio::test]
    async fn concurrent_resync_starts_accept_one_id_and_reject_the_other() {
        let shared = empty_resync_shared_state();
        let first_pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
        let second_pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-b", shared.clone()))).await;
        let _runtime_guard = hold_resync_runtime_lock(&shared, "atomic-start", "arn:test").await;

        let first = first_pool
            .clone()
            .start_bucket_resync(test_resync_opts("atomic-start", "arn:test", "run-a"));
        let second = second_pool
            .clone()
            .start_bucket_resync(test_resync_opts("atomic-start", "arn:test", "run-b"));
        let (first, second) = tokio::join!(first, second);

        let (accepted_id, conflict) = match (first, second) {
            (Ok(()), Err(conflict)) => ("run-a", conflict),
            (Err(conflict), Ok(())) => ("run-b", conflict),
            outcome => panic!("exactly one concurrent start should be accepted: {outcome:?}"),
        };
        assert_eq!(resync_start_conflict_id(&conflict), Some(accepted_id));

        let persisted = decode_resync_file(&shared.data.lock().expect("test data lock should not be poisoned"))
            .expect("accepted status should be persisted");
        assert_eq!(persisted.targets_map["arn:test"].resync_id, accepted_id);
        assert_eq!(persisted.targets_map["arn:test"].resync_status, ResyncStatusType::ResyncPending);
        assert_eq!(
            first_pool.resyncer.status_map.read().await["atomic-start"].targets_map["arn:test"].resync_id,
            accepted_id
        );
        assert_eq!(
            second_pool.resyncer.status_map.read().await["atomic-start"].targets_map["arn:test"].resync_id,
            accepted_id
        );
    }

    #[tokio::test]
    async fn same_resync_id_retry_is_idempotent_without_rewriting_status() {
        let shared = empty_resync_shared_state();
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
        let _runtime_guard = hold_resync_runtime_lock(&shared, "same-id", "arn:test").await;
        let opts = test_resync_opts("same-id", "arn:test", "run-a");

        pool.clone()
            .start_bucket_resync(opts.clone())
            .await
            .expect("first start should be accepted");
        let first_status = pool
            .resyncer
            .status_map
            .read()
            .await
            .get("same-id")
            .expect("accepted status should be published")
            .targets_map["arn:test"]
            .clone();

        pool.clone()
            .start_bucket_resync(opts)
            .await
            .expect("same ID retry should be accepted idempotently");
        let retried_status = pool
            .resyncer
            .status_map
            .read()
            .await
            .get("same-id")
            .expect("retried status should remain published")
            .targets_map["arn:test"]
            .clone();

        assert_eq!(shared.write_count.load(Ordering::SeqCst), 1);
        assert_eq!(retried_status.resync_id, first_status.resync_id);
        assert_eq!(retried_status.start_time, first_status.start_time);
        assert_eq!(retried_status.resync_status, ResyncStatusType::ResyncPending);
        assert_eq!(pool.resyncer.cancel_tokens.read().await.len(), 1);
    }

    #[tokio::test]
    async fn admitted_resync_waits_for_target_metadata_commit_before_activation() {
        let shared = empty_resync_shared_state();
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
        let _runtime_guard = hold_resync_runtime_lock(&shared, "two-phase-start", "arn:test").await;
        let opts = test_resync_opts("two-phase-start", "arn:test", "run-a");

        let new_run = pool
            .clone()
            .admit_bucket_resync(opts.clone())
            .await
            .expect("admission should persist the intent");
        assert!(new_run);
        assert!(pool.resyncer.cancel_tokens.read().await.is_empty());
        assert_eq!(shared.write_count.load(Ordering::SeqCst), 1);

        pool.clone()
            .activate_bucket_resync(opts, false)
            .await
            .expect("activation should start the admitted run");
        assert_eq!(pool.resyncer.cancel_tokens.read().await.len(), 1);
    }

    #[tokio::test]
    async fn same_id_retry_after_restart_recreates_missing_runtime_task() {
        let shared = empty_resync_shared_state();
        let mut persisted = BucketReplicationResyncStatus::new();
        persisted.targets_map.insert(
            "arn:test".to_string(),
            TargetReplicationResyncStatus {
                bucket: "restart-retry".to_string(),
                resync_id: "run-a".to_string(),
                resync_status: ResyncStatusType::ResyncPending,
                ..Default::default()
            },
        );
        *shared.data.lock().expect("test data lock should not be poisoned") =
            encode_resync_file(&persisted).expect("restart status should encode");
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
        let _runtime_guard = hold_resync_runtime_lock(&shared, "restart-retry", "arn:test").await;

        pool.clone()
            .start_bucket_resync(test_resync_opts("restart-retry", "arn:test", "run-a"))
            .await
            .expect("same ID retry should recover an accepted run");

        assert_eq!(shared.write_count.load(Ordering::SeqCst), 0);
        assert_eq!(pool.resyncer.cancel_tokens.read().await.len(), 1);
        assert_eq!(
            pool.resyncer.status_map.read().await["restart-retry"].targets_map["arn:test"].resync_id,
            "run-a"
        );
    }

    #[tokio::test]
    async fn same_completed_resync_id_retry_does_not_restart_work() {
        let shared = empty_resync_shared_state();
        let mut persisted = BucketReplicationResyncStatus::new();
        persisted.targets_map.insert(
            "arn:test".to_string(),
            TargetReplicationResyncStatus {
                bucket: "completed-retry".to_string(),
                resync_id: "run-a".to_string(),
                resync_status: ResyncStatusType::ResyncCompleted,
                ..Default::default()
            },
        );
        *shared.data.lock().expect("test data lock should not be poisoned") =
            encode_resync_file(&persisted).expect("completed status should encode");
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;

        pool.clone()
            .start_bucket_resync(test_resync_opts("completed-retry", "arn:test", "run-a"))
            .await
            .expect("completed same ID retry should remain idempotent");

        assert_eq!(shared.write_count.load(Ordering::SeqCst), 0);
        assert!(pool.resyncer.cancel_tokens.read().await.is_empty());
        assert_eq!(
            pool.resyncer.status_map.read().await["completed-retry"].targets_map["arn:test"].resync_status,
            ResyncStatusType::ResyncCompleted
        );
    }

    #[tokio::test]
    async fn start_failure_does_not_publish_or_persist_requested_id() {
        let shared = empty_resync_shared_state();
        shared.fail_next_write.store(true, Ordering::SeqCst);
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;

        let error = pool
            .clone()
            .start_bucket_resync(test_resync_opts("failed-start", "arn:test", "run-a"))
            .await
            .expect_err("metadata save failure should reject the start");

        assert!(matches!(error, EcstoreError::Unexpected));
        assert!(shared.data.lock().expect("test data lock should not be poisoned").is_empty());
        assert!(!pool.resyncer.status_map.read().await.contains_key("failed-start"));
        assert_eq!(shared.write_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn canceled_start_request_finishes_accepted_transaction() {
        let shared = empty_resync_shared_state();
        shared.block_next_write.store(true, Ordering::SeqCst);
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
        let _runtime_guard = hold_resync_runtime_lock(&shared, "canceled-start", "arn:test").await;

        let start_pool = pool.clone();
        let start = tokio::spawn(async move {
            start_pool
                .start_bucket_resync(test_resync_opts("canceled-start", "arn:test", "run-a"))
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), shared.write_started.notified())
            .await
            .expect("start transaction should reach the durable write");
        start.abort();
        assert!(start.await.expect_err("caller task should be canceled").is_cancelled());
        shared.allow_write.notify_one();

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if pool.resyncer.status_map.read().await.contains_key("canceled-start") {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached admission transaction should finish after caller cancellation");
        assert_eq!(shared.write_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            pool.resyncer.status_map.read().await["canceled-start"].targets_map["arn:test"].resync_id,
            "run-a"
        );
    }

    #[test]
    fn replication_queue_admission_combines_target_results() {
        let mut admission = ReplicationQueueAdmission::Skipped;

        admission.merge(ReplicationQueueAdmission::Queued);
        assert_eq!(admission, ReplicationQueueAdmission::Queued);

        admission.merge(ReplicationQueueAdmission::Missed);
        assert_eq!(admission, ReplicationQueueAdmission::Missed);
    }

    #[test]
    fn replicate_object_info_from_object_info_preserves_ssec_checksum() {
        let checksum = bytes::Bytes::from_static(b"ssec-checksum");
        let oi = ObjectInfo {
            bucket: "source".to_string(),
            name: "object".to_string(),
            user_defined: Arc::new(HashMap::from([(
                rustfs_utils::http::SSEC_ALGORITHM_HEADER.to_string(),
                "AES256".to_string(),
            )])),
            checksum: Some(checksum.clone()),
            ..Default::default()
        };

        let ri = replicate_object_info_from_object_info(oi, ReplicateDecision::default(), ReplicationType::Object);

        assert!(ri.ssec);
        assert_eq!(ri.checksum, Some(checksum));
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
            delete_marker_mtime: None,
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

    #[tokio::test]
    async fn load_resync_leader_lock_allows_only_one_startup_recovery() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
            let shared = Arc::new(LoadResyncSharedState {
                data: StdMutex::new(load_resync_test_metadata()),
                lock_manager: Arc::new(rustfs_lock::GlobalLockManager::new()),
                first_read_started: Notify::new(),
                delay_first_read: AtomicBool::new(true),
                read_count: AtomicUsize::new(0),
                write_count: AtomicUsize::new(0),
                fail_next_write: AtomicBool::new(false),
                block_next_write: AtomicBool::new(false),
                write_started: Notify::new(),
                allow_write: Notify::new(),
            });
            let leader_pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
            let skipped_pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-b", shared.clone()))).await;

            let leader = leader_pool.clone();
            let leader_task = tokio::spawn(async move {
                let buckets = vec!["load-resync-lock".to_string()];
                leader.load_resync(&buckets, CancellationToken::new()).await
            });

            tokio::time::timeout(Duration::from_secs(1), shared.first_read_started.notified())
                .await
                .expect("leader should start reading persisted resync metadata");

            let buckets = vec!["load-resync-lock".to_string()];
            skipped_pool
                .clone()
                .load_resync(&buckets, CancellationToken::new())
                .await
                .expect("contended load_resync should skip without failing startup");

            leader_task
                .await
                .expect("leader load_resync task should not panic")
                .expect("leader load_resync should succeed");

            assert_eq!(
                shared.read_count.load(Ordering::SeqCst),
                1,
                "only the leader node should read persisted resync metadata"
            );
            assert!(
                leader_pool.resyncer.status_map.read().await.contains_key("load-resync-lock"),
                "leader node should recover persisted resync status"
            );
            assert!(
                skipped_pool.resyncer.status_map.read().await.is_empty(),
                "node that does not hold the leader lock must not populate status_map"
            );
        })
        .await;
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
            delete_marker_mtime: None,
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
        // A specific, non-now() nanosecond timestamp: replay must preserve this exact value
        // instead of stamping the replica with the current time (backlog#867).
        let mtime_nanos = 1_705_312_200_123_456_789i64;
        let entry = MrfReplicateEntry {
            bucket: "del-bucket".to_string(),
            object: "key".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            delete_marker_version_id: Some(dm_vid),
            delete_marker: true,
            delete_marker_mtime: Some(mtime_nanos),
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
        assert_eq!(
            got.delete_marker_mtime,
            Some(mtime_nanos),
            "delete-marker mtime must survive the MRF disk round-trip"
        );
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
            delete_marker_mtime: None,
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
                delete_marker_mtime: None,
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
                delete_marker_mtime: None,
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
            delete_marker_mtime: None,
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
            delete_marker_mtime: None,
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
            delete_marker_mtime: None,
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
        // The "deleteMarkerMtime" key was absent in old files — #[serde(default)] must fill in
        // None so replay falls back to the current time (backlog#867 backward compatibility).
        assert_eq!(entry.delete_marker_mtime, None, "missing deleteMarkerMtime key must default to None");
    }

    #[test]
    fn durable_mrf_snapshot_reads_restart_backlog_and_valid_empty_state() {
        let entries = vec![MrfReplicateEntry {
            bucket: "restart-bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
            retry_count: 1,
            size: 512,
            op: MrfOpKind::Object,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
        }];
        let encoded = encode_mrf_file(&entries).expect("durable MRF backlog should encode");

        let recovered = durable_mrf_backlog_from_read(Ok(encoded));
        assert!(recovered.available);
        assert_eq!(recovered.entries.len(), 1);
        assert_eq!(recovered.entries[0].bucket, "restart-bucket");
        assert_eq!(recovered.entries[0].size, 512);

        let missing_file = durable_mrf_backlog_from_read(Err(EcstoreError::ConfigNotFound));
        assert!(missing_file.available);
        assert!(missing_file.entries.is_empty());
    }

    #[test]
    fn durable_mrf_snapshot_marks_corrupt_or_invalid_data_unavailable() {
        let corrupt = durable_mrf_backlog_from_read(Ok(vec![0, 1, 2]));
        assert!(!corrupt.available);
        assert!(corrupt.entries.is_empty());

        let negative = encode_mrf_file(&[MrfReplicateEntry {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
            retry_count: 0,
            size: -1,
            op: MrfOpKind::Object,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
        }])
        .expect("invalid persisted entry should still encode for boundary testing");
        let invalid = durable_mrf_backlog_from_read(Ok(negative));
        assert!(!invalid.available);
        assert!(invalid.entries.is_empty());
    }
}
