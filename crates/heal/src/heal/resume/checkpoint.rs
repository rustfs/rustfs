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

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use super::super::{BUCKET_META_PREFIX, DiskStore, HealDiskExt as _, RUSTFS_META_BUCKET};
use super::{
    LOG_COMPONENT_HEAL, LOG_SUBSYSTEM_RESUME, PersistThrottle, RESUME_CHECKPOINT_FILE, delete_resume_file, path_to_str,
    validate_resume_task_id,
};

const EVENT_HEAL_CHECKPOINT_STATE: &str = "heal_checkpoint_state";

/// Current on-disk schema version for `ResumeCheckpoint`. Same rationale as
/// `CURRENT_RESUME_SCHEMA`: pre-per-version dedup identities are not comparable
/// to the new `compose_key` identities, so a stale checkpoint is discarded.
pub(super) const CURRENT_CHECKPOINT_SCHEMA: u32 = 5;

#[derive(Debug, Clone, Copy)]
pub enum CheckpointObjectOutcome {
    Processed,
    Failed,
    Skipped,
}

#[derive(Debug)]
pub struct CheckpointObjectOutcomeRecord {
    pub object: String,
    pub outcome: CheckpointObjectOutcome,
    pub successful: u64,
    pub failed: u64,
    pub skipped: u64,
    pub bytes: u64,
    pub skipped_new_versions: u64,
    pub skipped_ilm_expired: u64,
    pub counter_unknown: bool,
}

/// resume checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeCheckpoint {
    /// on-disk schema version; absent in legacy snapshots (defaults to 0)
    #[serde(default)]
    pub schema_version: u32,
    /// task id
    pub task_id: String,
    /// checkpoint time
    pub checkpoint_time: u64,
    /// current bucket index
    pub current_bucket_index: usize,
    /// current object index
    pub current_object_index: usize,
    /// Objects healed since the last completed page. HashSet: with the
    /// previous Vec the per-object `contains` was O(n) and made large-bucket
    /// heals O(N²). Only spans the in-flight page — completed pages are
    /// covered by `current_object_index`, so `complete_page` prunes the sets.
    pub processed_objects: HashSet<String>,
    /// failed objects
    pub failed_objects: HashSet<String>,
    /// skipped objects
    pub skipped_objects: HashSet<String>,
    /// Aggregate object ledger counters restored alongside the dedup sets.
    #[serde(default)]
    pub successful_objects: u64,
    #[serde(default)]
    pub failed_object_count: u64,
    #[serde(default)]
    pub skipped_object_count: u64,
    #[serde(default)]
    pub skipped_new_versions: u64,
    #[serde(default)]
    pub skipped_ilm_expired: u64,
    #[serde(default)]
    pub processed_bytes: u64,
    #[serde(default)]
    pub total_objects: u64,
    #[serde(default)]
    pub total_bytes: u64,
    #[serde(default)]
    pub baseline_generation: Option<u64>,
    #[serde(default)]
    pub baseline_known: bool,
    /// Persistent telemetry fence for counter/byte overflow or corruption.
    #[serde(default)]
    pub counter_unknown: bool,
}

impl ResumeCheckpoint {
    pub fn new(task_id: String) -> Self {
        Self {
            schema_version: CURRENT_CHECKPOINT_SCHEMA,
            task_id,
            checkpoint_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            current_bucket_index: 0,
            current_object_index: 0,
            processed_objects: HashSet::new(),
            failed_objects: HashSet::new(),
            skipped_objects: HashSet::new(),
            successful_objects: 0,
            failed_object_count: 0,
            skipped_object_count: 0,
            skipped_new_versions: 0,
            skipped_ilm_expired: 0,
            processed_bytes: 0,
            total_objects: 0,
            total_bytes: 0,
            baseline_generation: None,
            baseline_known: false,
            counter_unknown: false,
        }
    }

    pub fn update_position(&mut self, bucket_index: usize, object_index: usize) {
        self.current_bucket_index = bucket_index;
        self.current_object_index = object_index;
        self.checkpoint_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn add_processed_object(&mut self, object: String) {
        self.processed_objects.insert(object);
    }

    pub fn add_failed_object(&mut self, object: String) {
        self.failed_objects.insert(object);
    }

    pub fn add_skipped_object(&mut self, object: String) {
        self.skipped_objects.insert(object);
    }

    pub fn update_progress(&mut self, successful: u64, failed: u64, skipped: u64, bytes: u64) {
        self.successful_objects = successful;
        self.failed_object_count = failed;
        self.skipped_object_count = skipped;
        self.processed_bytes = bytes;
    }

    pub fn set_progress_baseline(&mut self, total_objects: u64, total_bytes: u64, generation: Option<u64>) {
        self.total_objects = total_objects;
        self.total_bytes = total_bytes;
        self.baseline_generation = generation;
        // The caller has already validated that this is a complete snapshot;
        // preserve the distinction between a known empty scope and an old
        // checkpoint that omitted all baseline fields.
        self.baseline_known = true;
    }

    pub fn mark_counter_unknown(&mut self) {
        self.counter_unknown = true;
        self.checkpoint_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn set_skipped_version_counts(&mut self, new_versions: u64, ilm_expired: u64) {
        self.skipped_new_versions = new_versions;
        self.skipped_ilm_expired = ilm_expired;
        self.checkpoint_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    /// Advance past a fully-processed page: objects below `object_index` are
    /// skipped by position on resume, so the per-object sets no longer need
    /// their entries and would otherwise grow with the whole bucket.
    pub fn complete_page(&mut self, bucket_index: usize, object_index: usize) {
        self.update_position(bucket_index, object_index);
        self.processed_objects.clear();
        self.skipped_objects.clear();
        self.failed_objects.clear();
    }

    /// Reset the scan to the start and clear the per-object sets so a retry
    /// re-scans the whole set.
    pub fn reset_for_retry(&mut self) {
        self.update_position(0, 0);
        self.processed_objects.clear();
        self.skipped_objects.clear();
        self.successful_objects = 0;
        self.failed_object_count = 0;
        self.skipped_object_count = 0;
        self.skipped_new_versions = 0;
        self.skipped_ilm_expired = 0;
        self.processed_bytes = 0;
        self.total_objects = 0;
        self.total_bytes = 0;
        self.baseline_generation = None;
        self.baseline_known = false;
        self.counter_unknown = false;
        self.failed_objects.clear();
    }
}

/// resume checkpoint manager
pub struct CheckpointManager {
    disk: DiskStore,
    checkpoint: Arc<RwLock<ResumeCheckpoint>>,
    throttle: Mutex<PersistThrottle>,
}

impl CheckpointManager {
    /// create new checkpoint manager
    pub async fn new(disk: DiskStore, task_id: String) -> Result<Self> {
        validate_resume_task_id(&task_id)?;
        let checkpoint = ResumeCheckpoint::new(task_id);
        let manager = Self {
            disk,
            checkpoint: Arc::new(RwLock::new(checkpoint)),
            throttle: Mutex::new(PersistThrottle::new()),
        };

        // save initial checkpoint
        if let Err(e) = manager.save_checkpoint().await {
            warn!(
                target: "rustfs::heal::resume",
                event = EVENT_HEAL_CHECKPOINT_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_RESUME,
                state = "initial_save_failed",
                error = %e,
                "Heal checkpoint persistence failed"
            );
        }
        Ok(manager)
    }

    /// load checkpoint from disk
    pub async fn load_from_disk(disk: DiskStore, task_id: &str) -> Result<Self> {
        validate_resume_task_id(task_id)?;
        let checkpoint_data = Self::read_checkpoint_file(&disk, task_id).await?;
        let mut checkpoint: ResumeCheckpoint =
            serde_json::from_slice(&checkpoint_data).map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to deserialize checkpoint: {e}"),
            })?;
        if checkpoint.task_id != task_id {
            return Err(Error::TaskExecutionFailed {
                message: "Resume checkpoint task id does not match filename".to_string(),
            });
        }

        // A checkpoint from an older schema stored latest-only dedup identities
        // that are not comparable to the new per-version `compose_key`
        // identities. Discard the stale sets and position, then stamp the
        // current schema so the scan restarts cleanly.
        if checkpoint.schema_version > CURRENT_CHECKPOINT_SCHEMA {
            return Err(Error::TaskExecutionFailed {
                message: format!(
                    "Checkpoint schema {} is newer than supported schema {CURRENT_CHECKPOINT_SCHEMA}",
                    checkpoint.schema_version
                ),
            });
        }
        if checkpoint.schema_version < CURRENT_CHECKPOINT_SCHEMA {
            warn!(
                target: "rustfs::heal::resume",
                event = EVENT_HEAL_CHECKPOINT_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_RESUME,
                task_id,
                found_schema = checkpoint.schema_version,
                current_schema = CURRENT_CHECKPOINT_SCHEMA,
                state = "schema_discarded",
                "Heal checkpoint schema is stale; discarding dedup sets and position"
            );
            checkpoint.processed_objects.clear();
            checkpoint.failed_objects.clear();
            checkpoint.skipped_objects.clear();
            checkpoint.successful_objects = 0;
            checkpoint.failed_object_count = 0;
            checkpoint.skipped_object_count = 0;
            checkpoint.skipped_new_versions = 0;
            checkpoint.skipped_ilm_expired = 0;
            checkpoint.processed_bytes = 0;
            checkpoint.total_objects = 0;
            checkpoint.total_bytes = 0;
            checkpoint.baseline_generation = None;
            checkpoint.baseline_known = false;
            checkpoint.counter_unknown = false;
            checkpoint.current_bucket_index = 0;
            checkpoint.current_object_index = 0;
            checkpoint.schema_version = CURRENT_CHECKPOINT_SCHEMA;
        }

        Ok(Self {
            disk,
            checkpoint: Arc::new(RwLock::new(checkpoint)),
            throttle: Mutex::new(PersistThrottle::new()),
        })
    }

    /// check if checkpoint exists
    pub async fn has_checkpoint(disk: &DiskStore, task_id: &str) -> bool {
        if validate_resume_task_id(task_id).is_err() {
            return false;
        }
        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"));
        match path_to_str(&file_path) {
            Ok(path_str) => match disk.read_all(RUSTFS_META_BUCKET, path_str).await {
                Ok(data) => !data.is_empty(),
                Err(_) => false,
            },
            Err(_) => false,
        }
    }

    /// get current checkpoint
    pub async fn get_checkpoint(&self) -> ResumeCheckpoint {
        self.checkpoint.read().await.clone()
    }

    /// update position
    pub async fn update_position(&self, bucket_index: usize, object_index: usize) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.update_position(bucket_index, object_index);
        drop(checkpoint);
        self.save_checkpoint_throttled().await
    }

    /// Persist a completed page position while retaining its identities.
    pub async fn complete_page(&self, bucket_index: usize, object_index: usize) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.complete_page(bucket_index, object_index);
        drop(checkpoint);
        self.save_checkpoint_throttled().await
    }

    /// Persist the page position while retaining identities until the resume
    /// cursor is durable.
    pub async fn advance_page(&self, bucket_index: usize, object_index: usize) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.update_position(bucket_index, object_index);
        drop(checkpoint);
        self.save_checkpoint().await
    }

    /// Remove the previous page's dedup identities only after its resume cursor
    /// has been durably exposed.
    pub async fn prune_completed_page(&self) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.processed_objects.clear();
        checkpoint.skipped_objects.clear();
        checkpoint.failed_objects.clear();
        drop(checkpoint);
        self.save_checkpoint().await
    }

    /// Advance to the next bucket and clear the final page identities after the
    /// resume state has durably recorded the completed bucket.
    pub async fn complete_bucket(&self, next_bucket_index: usize) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.complete_page(next_bucket_index, 0);
        drop(checkpoint);
        self.save_checkpoint().await
    }

    /// Reset the checkpoint to the start of the scan for a retry, then persist.
    pub async fn reset_for_retry(&self) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.reset_for_retry();
        drop(checkpoint);
        self.save_checkpoint_throttled().await
    }

    /// Add a processed object. Called once per healed object, so persistence
    /// is batched (`PERSIST_EVERY_MUTATIONS` / `PERSIST_INTERVAL`); positions
    /// and page boundaries still persist unconditionally.
    pub async fn add_processed_object(&self, object: String) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.add_processed_object(object);
        drop(checkpoint);
        self.save_checkpoint_if_due().await
    }

    /// add failed object (batched, see `add_processed_object`)
    pub async fn add_failed_object(&self, object: String) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.add_failed_object(object);
        drop(checkpoint);
        self.save_checkpoint_if_due().await
    }

    /// add skipped object (batched, see `add_processed_object`)
    pub async fn add_skipped_object(&self, object: String) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.add_skipped_object(object);
        drop(checkpoint);
        self.save_checkpoint_if_due().await
    }

    /// Atomically persist an object's dedup identity with its aggregate result.
    pub async fn record_object_outcome(&self, record: CheckpointObjectOutcomeRecord) -> Result<()> {
        let CheckpointObjectOutcomeRecord {
            object,
            outcome,
            successful,
            failed,
            skipped,
            bytes,
            skipped_new_versions,
            skipped_ilm_expired,
            counter_unknown,
        } = record;
        let mut checkpoint = self.checkpoint.write().await;
        match outcome {
            CheckpointObjectOutcome::Processed => checkpoint.add_processed_object(object),
            CheckpointObjectOutcome::Failed => checkpoint.add_failed_object(object),
            CheckpointObjectOutcome::Skipped => checkpoint.add_skipped_object(object),
        }
        checkpoint.update_progress(successful, failed, skipped, bytes);
        checkpoint.set_skipped_version_counts(skipped_new_versions, skipped_ilm_expired);
        if counter_unknown {
            checkpoint.mark_counter_unknown();
        }
        drop(checkpoint);
        self.save_checkpoint_if_due().await
    }

    pub async fn update_progress(&self, successful: u64, failed: u64, skipped: u64, bytes: u64) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.update_progress(successful, failed, skipped, bytes);
        drop(checkpoint);
        self.save_checkpoint_if_due().await
    }

    pub async fn set_progress_baseline(&self, total_objects: u64, total_bytes: u64, generation: Option<u64>) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.set_progress_baseline(total_objects, total_bytes, generation);
        drop(checkpoint);
        self.save_checkpoint_throttled().await
    }

    pub async fn mark_counter_unknown(&self) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.mark_counter_unknown();
        drop(checkpoint);
        self.save_checkpoint().await
    }

    pub async fn set_skipped_version_counts(&self, new_versions: u64, ilm_expired: u64) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.set_skipped_version_counts(new_versions, ilm_expired);
        drop(checkpoint);
        self.save_checkpoint_throttled().await
    }

    async fn save_checkpoint_if_due(&self) -> Result<()> {
        let should_save = self.throttle.lock().map(|mut throttle| throttle.record()).unwrap_or(true);
        if !should_save {
            return Ok(());
        }
        self.save_checkpoint_throttled().await
    }

    async fn save_checkpoint_throttled(&self) -> Result<()> {
        let result = self.save_checkpoint().await;
        if result.is_ok()
            && let Ok(mut throttle) = self.throttle.lock()
        {
            throttle.mark_saved();
        }
        result
    }

    /// cleanup checkpoint
    pub async fn cleanup(&self) -> Result<()> {
        let task_id = self.checkpoint.read().await.task_id.clone();
        validate_resume_task_id(&task_id)?;

        let checkpoint_file = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"));
        delete_resume_file(&self.disk, &checkpoint_file).await?;

        debug!(
            target: "rustfs::heal::resume",
            event = EVENT_HEAL_CHECKPOINT_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_RESUME,
            task_id,
            state = "cleaned",
            "Heal checkpoint cleaned"
        );
        Ok(())
    }

    /// save checkpoint to disk
    async fn save_checkpoint(&self) -> Result<()> {
        let checkpoint = self.checkpoint.read().await;
        validate_resume_task_id(&checkpoint.task_id)?;
        let checkpoint_data = serde_json::to_vec(&*checkpoint).map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to serialize checkpoint: {e}"),
        })?;

        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{}_{}", checkpoint.task_id, RESUME_CHECKPOINT_FILE));

        let path_str = path_to_str(&file_path)?;
        self.disk
            .write_all(RUSTFS_META_BUCKET, path_str, checkpoint_data.into())
            .await
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to save checkpoint: {e}"),
            })?;

        debug!(
            target: "rustfs::heal::resume",
            event = EVENT_HEAL_CHECKPOINT_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_RESUME,
            task_id = %checkpoint.task_id,
            state = "saved",
            "Heal checkpoint persisted"
        );
        Ok(())
    }

    /// read checkpoint file from disk
    async fn read_checkpoint_file(disk: &DiskStore, task_id: &str) -> Result<Vec<u8>> {
        validate_resume_task_id(task_id)?;
        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"));

        let path_str = path_to_str(&file_path)?;
        disk.read_all(RUSTFS_META_BUCKET, path_str)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to read checkpoint file: {e}"),
            })
    }
}
