use crate::StorageAPI;
use crate::bucket::replication::MrfReplicateEntry;
use crate::bucket::replication::ReplicateDecision;
use crate::bucket::replication::ReplicateObjectInfo;
use crate::bucket::replication::ReplicationWorkerOperation;
use crate::bucket::replication::ResyncDecision;
use crate::bucket::replication::ResyncOpts;
use crate::bucket::replication::ResyncStatusType;
use crate::bucket::replication::replicate_delete;
use crate::bucket::replication::replicate_object;
use crate::disk::BUCKET_META_PREFIX;
use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;

use crate::bucket::replication::replication_resyncer::{
    BucketReplicationResyncStatus, DeletedObjectReplicationInfo, ReplicationResyncer,
};
use crate::bucket::replication::replication_state::ReplicationStats;
use crate::bucket::replication::replication_statuses_map;
use crate::bucket::replication::version_purge_statuses_map;
use crate::config::com::read_config;
use crate::error::Error as EcstoreError;
use crate::store_api::ObjectInfo;

use lazy_static::lazy_static;
use rustfs_filemeta::ReplicatedTargetInfo;
use rustfs_filemeta::ReplicationStatusType;
use rustfs_filemeta::ReplicationType;
use rustfs_utils::http::RESERVED_METADATA_PREFIX_LOWER;
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
use tracing::info;
use tracing::warn;

// Worker limits
pub const WORKER_MAX_LIMIT: usize = 500;
pub const WORKER_MIN_LIMIT: usize = 50;
pub const WORKER_AUTO_DEFAULT: usize = 100;
pub const MRF_WORKER_MAX_LIMIT: usize = 8;
pub const MRF_WORKER_MIN_LIMIT: usize = 2;
pub const MRF_WORKER_AUTO_DEFAULT: usize = 4;
pub const LARGE_WORKER_COUNT: usize = 10;
pub const MIN_LARGE_OBJ_SIZE: i64 = 128 * 1024 * 1024; // 128MiB

/// Priority levels for replication
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationPriority {
    Fast,
    Slow,
    Auto,
}

impl std::str::FromStr for ReplicationPriority {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "fast" => Ok(ReplicationPriority::Fast),
            "slow" => Ok(ReplicationPriority::Slow),
            "auto" => Ok(ReplicationPriority::Auto),
            _ => Ok(ReplicationPriority::Auto), // Default to Auto for unknown values
        }
    }
}

impl ReplicationPriority {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReplicationPriority::Fast => "fast",
            ReplicationPriority::Slow => "slow",
            ReplicationPriority::Auto => "auto",
        }
    }
}

/// Enum for different types of replication operations
#[derive(Debug)]
pub enum ReplicationOperation {
    Object(Box<ReplicateObjectInfo>),
    Delete(Box<DeletedObjectReplicationInfo>),
}

impl ReplicationWorkerOperation for ReplicationOperation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_mrf_entry(&self) -> MrfReplicateEntry {
        match self {
            ReplicationOperation::Object(obj) => obj.to_mrf_entry(),
            ReplicationOperation::Delete(del) => del.to_mrf_entry(),
        }
    }

    fn get_bucket(&self) -> &str {
        match self {
            ReplicationOperation::Object(obj) => obj.get_bucket(),
            ReplicationOperation::Delete(del) => del.get_bucket(),
        }
    }

    fn get_object(&self) -> &str {
        match self {
            ReplicationOperation::Object(obj) => obj.get_object(),
            ReplicationOperation::Delete(del) => del.get_object(),
        }
    }

    fn get_size(&self) -> i64 {
        match self {
            ReplicationOperation::Object(obj) => obj.get_size(),
            ReplicationOperation::Delete(del) => del.get_size(),
        }
    }

    fn is_delete_marker(&self) -> bool {
        match self {
            ReplicationOperation::Object(obj) => obj.is_delete_marker(),
            ReplicationOperation::Delete(del) => del.is_delete_marker(),
        }
    }

    fn get_op_type(&self) -> ReplicationType {
        match self {
            ReplicationOperation::Object(obj) => obj.get_op_type(),
            ReplicationOperation::Delete(del) => del.get_op_type(),
        }
    }
}

/// Replication pool options
#[derive(Debug, Clone)]
pub struct ReplicationPoolOpts {
    pub priority: ReplicationPriority,
    pub max_workers: Option<usize>,
    pub max_l_workers: Option<usize>,
}

impl Default for ReplicationPoolOpts {
    fn default() -> Self {
        Self {
            priority: ReplicationPriority::Auto,
            max_workers: None,
            max_l_workers: None,
        }
    }
}
/// Main replication pool structure
#[derive(Debug)]
pub struct ReplicationPool<S: StorageAPI> {
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
    mrf_replica_rx: Mutex<Option<Receiver<ReplicationOperation>>>,
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

impl<S: StorageAPI> ReplicationPool<S> {
    /// Creates a new replication pool with specified options
    pub async fn new(opts: ReplicationPoolOpts, stats: Arc<ReplicationStats>, storage: Arc<S>) -> Arc<Self> {
        let max_workers = opts.max_workers.unwrap_or(WORKER_MAX_LIMIT);

        let (workers, failed_workers) = match opts.priority {
            ReplicationPriority::Fast => (WORKER_MAX_LIMIT, MRF_WORKER_MAX_LIMIT),
            ReplicationPriority::Slow => (WORKER_MIN_LIMIT, MRF_WORKER_MIN_LIMIT),
            ReplicationPriority::Auto => (WORKER_AUTO_DEFAULT, MRF_WORKER_AUTO_DEFAULT),
        };

        let workers = std::cmp::min(workers, max_workers);
        let failed_workers = std::cmp::min(failed_workers, max_workers);

        let max_l_workers = opts.max_l_workers.unwrap_or(LARGE_WORKER_COUNT);

        // Create MRF channels
        let (mrf_replica_tx, mrf_replica_rx) = mpsc::channel(100000);
        let (mrf_save_tx, mrf_save_rx) = mpsc::channel(100000);
        let (mrf_worker_kill_tx, _mrf_worker_kill_rx) = mpsc::channel(failed_workers);
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
            mrf_replica_rx: Mutex::new(Some(mrf_replica_rx)),
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
        pool.resize_workers(workers, 0).await;
        pool.resize_failed_workers(failed_workers as i32).await;

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
            warn!(
                "resize_workers: skipping resize - check_old_mismatch={}, same_size={}, invalid_n={}",
                check_old > 0 && workers.len() != check_old,
                n == workers.len(),
                n < 1
            );
            return;
        }

        // Add workers if needed
        if workers.len() < n {
            info!("resize_workers: adding workers from {} to {}", workers.len(), n);
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
            warn!("resize_workers: removing workers from {} to {}", workers.len(), n);
        }

        while workers.len() > n {
            if let Some(worker) = workers.pop() {
                drop(worker); // Closing the channel will terminate the worker
            }
        }
    }

    /// Resizes the failed workers pool
    pub async fn resize_failed_workers(&self, n: i32) {
        // Add workers if needed
        while self.mrf_worker_size.load(Ordering::SeqCst) < n {
            self.mrf_worker_size.fetch_add(1, Ordering::SeqCst);

            let active_counter = self.active_mrf_workers.clone();
            let stats = self.stats.clone();
            let storage = self.storage.clone();
            let mrf_rx = self.mrf_replica_rx.lock().await.take();

            if let Some(rx) = mrf_rx {
                let handle = tokio::spawn(async move {
                    let mut rx = rx;
                    while let Some(operation) = rx.recv().await {
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
                break; // Only one receiver can be taken
            }
        }

        // Remove workers if needed
        while self.mrf_worker_size.load(Ordering::SeqCst) > n {
            self.mrf_worker_size.fetch_sub(1, Ordering::SeqCst);
            let _ = self.mrf_worker_kill_tx.try_send(()); // Signal worker to stop
        }
    }

    /// Resizes worker priority and counts
    pub async fn resize_worker_priority(
        &self,
        pri: ReplicationPriority,
        max_workers: Option<usize>,
        max_l_workers: Option<usize>,
    ) {
        let (workers, mrf_workers) = match pri {
            ReplicationPriority::Fast => (WORKER_MAX_LIMIT, MRF_WORKER_MAX_LIMIT),
            ReplicationPriority::Slow => (WORKER_MIN_LIMIT, MRF_WORKER_MIN_LIMIT),
            ReplicationPriority::Auto => {
                let mut workers = WORKER_AUTO_DEFAULT;
                let mut mrf_workers = MRF_WORKER_AUTO_DEFAULT;

                let current_workers = self.workers.read().await.len();
                if current_workers < WORKER_AUTO_DEFAULT {
                    workers = std::cmp::min(current_workers + 1, WORKER_AUTO_DEFAULT);
                }

                let current_mrf = self.mrf_worker_size.load(Ordering::SeqCst) as usize;
                if current_mrf < MRF_WORKER_AUTO_DEFAULT {
                    mrf_workers = std::cmp::min(current_mrf + 1, MRF_WORKER_AUTO_DEFAULT);
                }
                (workers, mrf_workers)
            }
        };

        let (final_workers, final_mrf_workers) = if let Some(max_w) = max_workers {
            *self.max_workers.write().await = max_w;
            (std::cmp::min(workers, max_w), std::cmp::min(mrf_workers, max_w))
        } else {
            (workers, mrf_workers)
        };

        let max_l_workers_val = max_l_workers.unwrap_or(LARGE_WORKER_COUNT);
        *self.max_l_workers.write().await = max_l_workers_val;
        *self.priority.write().await = pri;

        self.resize_workers(final_workers, 0).await;
        self.resize_failed_workers(final_mrf_workers as i32).await;
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

    /// Queues a replica task
    pub async fn queue_replica_task(&self, ri: ReplicateObjectInfo) {
        // If object is large, queue it to a static set of large workers
        if ri.size >= MIN_LARGE_OBJ_SIZE {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let mut hasher = DefaultHasher::new();
            format!("{}{}", ri.bucket, ri.name).hash(&mut hasher);
            let hash = hasher.finish();

            let lrg_workers = self.lrg_workers.read().await;

            if !lrg_workers.is_empty() {
                let index = (hash as usize) % lrg_workers.len();

                if let Some(worker) = lrg_workers.get(index) {
                    if worker.try_send(ReplicationOperation::Object(Box::new(ri.clone()))).is_err() {
                        // Queue to MRF if worker is busy
                        let _ = self.mrf_save_tx.try_send(ri.to_mrf_entry());

                        // Try to add more workers if possible
                        let max_l_workers = *self.max_l_workers.read().await;
                        let existing = lrg_workers.len();
                        if self.active_lrg_workers() < std::cmp::min(max_l_workers, LARGE_WORKER_COUNT) as i32 {
                            let workers = std::cmp::min(existing + 1, max_l_workers);

                            drop(lrg_workers);
                            self.resize_lrg_workers(workers, existing).await;
                        }
                    }
                }
            }
            return;
        }

        // Handle regular sized objects

        let ch = match ri.op_type {
            ReplicationType::Heal | ReplicationType::ExistingObject => Some(self.mrf_replica_tx.clone()),
            _ => self.get_worker_ch(&ri.bucket, &ri.name, ri.size).await,
        };

        if let Some(channel) = ch {
            if channel.try_send(ReplicationOperation::Object(Box::new(ri.clone()))).is_err() {
                // Queue to MRF if all workers are busy
                let _ = self.mrf_save_tx.try_send(ri.to_mrf_entry());

                // Try to scale up workers based on priority
                let priority = self.priority.read().await.clone();
                let max_workers = *self.max_workers.read().await;

                match priority {
                    ReplicationPriority::Fast => {
                        // Log warning about unable to keep up
                        info!("Warning: Unable to keep up with incoming traffic");
                    }
                    ReplicationPriority::Slow => {
                        info!(
                            "Warning: Unable to keep up with incoming traffic - recommend increasing replication priority to auto"
                        );
                    }
                    ReplicationPriority::Auto => {
                        let max_w = std::cmp::min(max_workers, WORKER_MAX_LIMIT);
                        let active_workers = self.active_workers();

                        if active_workers < max_w as i32 {
                            let workers = self.workers.read().await;
                            let new_count = std::cmp::min(workers.len() + 1, max_w);
                            let existing = workers.len();

                            drop(workers);
                            self.resize_workers(new_count, existing).await;
                        }

                        let max_mrf_workers = std::cmp::min(max_workers, MRF_WORKER_MAX_LIMIT);
                        let active_mrf = self.active_mrf_workers();

                        if active_mrf < max_mrf_workers as i32 {
                            let current_mrf = self.mrf_worker_size.load(Ordering::SeqCst);
                            let new_mrf = std::cmp::min(current_mrf + 1, max_mrf_workers as i32);

                            self.resize_failed_workers(new_mrf).await;
                        }
                    }
                }
            }
        }
    }

    /// Queues a replica delete task
    pub async fn queue_replica_delete_task(&self, doi: DeletedObjectReplicationInfo) {
        let ch = match doi.op_type {
            ReplicationType::Heal | ReplicationType::ExistingObject => Some(self.mrf_replica_tx.clone()),
            _ => self.get_worker_ch(&doi.bucket, &doi.delete_object.object_name, 0).await,
        };

        if let Some(channel) = ch {
            if channel.try_send(ReplicationOperation::Delete(Box::new(doi.clone()))).is_err() {
                let _ = self.mrf_save_tx.try_send(doi.to_mrf_entry());

                let priority = self.priority.read().await.clone();
                let max_workers = *self.max_workers.read().await;

                match priority {
                    ReplicationPriority::Fast => {
                        info!("Warning: Unable to keep up with incoming deletes");
                    }
                    ReplicationPriority::Slow => {
                        info!(
                            "Warning: Unable to keep up with incoming deletes - recommend increasing replication priority to auto"
                        );
                    }
                    ReplicationPriority::Auto => {
                        let max_w = std::cmp::min(max_workers, WORKER_MAX_LIMIT);
                        if self.active_workers() < max_w as i32 {
                            let workers = self.workers.read().await;
                            let new_count = std::cmp::min(workers.len() + 1, max_w);
                            let existing = workers.len();
                            drop(workers);
                            self.resize_workers(new_count, existing).await;
                        }
                    }
                }
            }
        }
    }

    /// Queues an MRF save operation
    async fn queue_mrf_save(&self, entry: MrfReplicateEntry) {
        let _ = self.mrf_save_tx.try_send(entry);
    }

    /// Starts the MRF processor background task
    async fn start_mrf_processor(&self) {
        // This would start a background task to process MRF entries
        // Implementation depends on the actual MRF processing logic
    }

    /// Starts the MRF persister background task  
    async fn start_mrf_persister(&self) {
        // This would start a background task to persist MRF entries to disk
        // Implementation depends on the actual persistence logic
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
        let pool_clone = self.clone();

        tokio::spawn(async move {
            pool_clone.start_resync_routine(buckets, cancellation_token).await;
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
            use rand::Rng;
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
    async fn load_resync(self: Arc<Self>, buckets: &[String], cancellation_token: CancellationToken) -> Result<(), EcstoreError> {
        // TODO: add leader_lock
        // Make sure only one node running resync on the cluster
        // Note: Leader lock implementation would be needed here
        // let _lock_guard = global_leader_lock.get_lock().await?;

        for bucket in buckets {
            let meta = match load_bucket_resync_metadata(bucket, self.storage.clone()).await {
                Ok(meta) => meta,
                Err(err) => {
                    if !matches!(err, EcstoreError::VolumeNotFound) {
                        warn!("Error loading resync metadata for bucket {bucket}: {err:?}");
                    }
                    continue;
                }
            };

            // Store metadata in resyncer
            {
                let mut status_map = self.resyncer.status_map.write().await;
                status_map.insert(bucket.clone(), meta.clone());
            }

            // Process target statistics
            let target_stats = meta.clone_tgt_stats();
            for (arn, stats) in target_stats {
                match stats.resync_status {
                    ResyncStatusType::ResyncFailed | ResyncStatusType::ResyncStarted | ResyncStatusType::ResyncPending => {
                        // Note: This would spawn a resync task in a real implementation
                        // For now, we just log the resync request

                        let ctx = cancellation_token.clone();
                        let bucket_clone = bucket.clone();
                        let resync = self.resyncer.clone();
                        let storage = self.storage.clone();
                        tokio::spawn(async move {
                            resync
                                .resync_bucket(
                                    ctx,
                                    storage,
                                    true,
                                    ResyncOpts {
                                        bucket: bucket_clone,
                                        arn,
                                        resync_id: stats.resync_id,
                                        resync_before: stats.resync_before_date,
                                    },
                                )
                                .await;
                        });
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

/// Load bucket resync metadata from disk
async fn load_bucket_resync_metadata<S: StorageAPI>(
    bucket: &str,
    obj_api: Arc<S>,
) -> Result<BucketReplicationResyncStatus, EcstoreError> {
    use std::convert::TryInto;

    let mut brs = BucketReplicationResyncStatus::new();

    // Constants that would be defined elsewhere
    const REPLICATION_DIR: &str = "replication";
    const RESYNC_FILE_NAME: &str = "resync.bin";
    const RESYNC_META_FORMAT: u16 = 1;
    const RESYNC_META_VERSION: u16 = 1;
    const RESYNC_META_VERSION_V1: u16 = 1;

    let resync_dir_path = format!("{BUCKET_META_PREFIX}/{bucket}/{REPLICATION_DIR}");
    let resync_file_path = format!("{resync_dir_path}/{RESYNC_FILE_NAME}");

    let data = match read_config(obj_api, &resync_file_path).await {
        Ok(data) => data,
        Err(EcstoreError::ConfigNotFound) => return Ok(brs),
        Err(err) => return Err(err),
    };

    if data.is_empty() {
        // Seems to be empty
        return Ok(brs);
    }

    if data.len() <= 4 {
        return Err(EcstoreError::CorruptedFormat);
    }

    // Read resync meta header
    let format = u16::from_le_bytes(data[0..2].try_into().unwrap());
    if format != RESYNC_META_FORMAT {
        return Err(EcstoreError::CorruptedFormat);
    }

    let version = u16::from_le_bytes(data[2..4].try_into().unwrap());
    if version != RESYNC_META_VERSION {
        return Err(EcstoreError::CorruptedFormat);
    }

    // Parse data
    brs = BucketReplicationResyncStatus::unmarshal_msg(&data[4..])?;

    if brs.version != RESYNC_META_VERSION_V1 {
        return Err(EcstoreError::CorruptedFormat);
    }

    Ok(brs)
}

// Define a trait object type for the replication pool
pub type DynReplicationPool = dyn ReplicationPoolTrait + Send + Sync;

/// Trait that abstracts the replication pool operations
#[async_trait::async_trait]
pub trait ReplicationPoolTrait: std::fmt::Debug {
    async fn queue_replica_task(&self, ri: ReplicateObjectInfo);
    async fn queue_replica_delete_task(&self, ri: DeletedObjectReplicationInfo);
    async fn resize(&self, priority: ReplicationPriority, max_workers: usize, max_l_workers: usize);
    async fn init_resync(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
        buckets: Vec<String>,
    ) -> Result<(), EcstoreError>;
}

// Implement the trait for ReplicationPool
#[async_trait::async_trait]
impl<S: StorageAPI> ReplicationPoolTrait for ReplicationPool<S> {
    async fn queue_replica_task(&self, ri: ReplicateObjectInfo) {
        self.queue_replica_task(ri).await;
    }

    async fn queue_replica_delete_task(&self, ri: DeletedObjectReplicationInfo) {
        self.queue_replica_delete_task(ri).await;
    }

    async fn resize(&self, priority: ReplicationPriority, max_workers: usize, max_l_workers: usize) {
        self.resize(priority, max_workers, max_l_workers).await;
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
pub async fn init_background_replication<S: StorageAPI>(storage: Arc<S>) {
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

    assert!(GLOBAL_REPLICATION_STATS.get().is_some());
    assert!(GLOBAL_REPLICATION_POOL.get().is_some());
}

pub async fn schedule_replication<S: StorageAPI>(oi: ObjectInfo, o: Arc<S>, dsc: ReplicateDecision, op_type: ReplicationType) {
    let tgt_statuses = replication_statuses_map(&oi.replication_status_internal.clone().unwrap_or_default());
    let purge_statuses = version_purge_statuses_map(&oi.version_purge_status_internal.clone().unwrap_or_default());
    let tm = oi
        .user_defined
        .get(&format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replication-timestamp"))
        .map(|v| OffsetDateTime::parse(v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH));
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
        user_tags: oi.user_tags,
        checksum: vec![],
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
    } else if let Some(pool) = GLOBAL_REPLICATION_POOL.get() {
        pool.queue_replica_task(ri).await;
    }
}

pub async fn schedule_replication_delete(dv: DeletedObjectReplicationInfo) {
    if let Some(pool) = GLOBAL_REPLICATION_POOL.get() {
        pool.queue_replica_delete_task(dv.clone()).await;
    }

    if let (Some(rs), Some(stats)) = (dv.delete_object.replication_state, GLOBAL_REPLICATION_STATS.get()) {
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
