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
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tracing::{debug, warn};

use super::super::storage_api::owner::{EcstoreConditionalFileUpdate, EcstoreDiskAPI, EcstoreDiskBytes};
use super::super::{BUCKET_META_PREFIX, DiskStore, HealDiskExt, RUSTFS_META_BUCKET};
use super::{
    LOG_COMPONENT_HEAL, LOG_SUBSYSTEM_RESUME, PersistThrottle, RESUME_CHECKPOINT_BLOCKED_FILE, RESUME_CHECKPOINT_FILE,
    delete_resume_file, path_to_str, validate_resume_task_id,
};

const EVENT_HEAL_CHECKPOINT_STATE: &str = "heal_checkpoint_state";
const RESUME_CHECKPOINT_DIGEST_FILE: &str = "ahm_checkpoint.sha256";
const CHECKPOINT_PER_VERSION_SCHEMA: u32 = 5;

/// Current on-disk schema version for `ResumeCheckpoint`. Schema 5 could
/// persist dedup identities without the aggregate counters needed to restore
/// them safely, so stale checkpoints are discarded and replayed.
pub(super) const CURRENT_CHECKPOINT_SCHEMA: u32 = 6;

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
    /// Integrity digest over the checkpoint with this field set to `None`.
    /// Keeping it in the checkpoint makes the payload and its authentication
    /// record one CAS generation instead of two independently-written files.
    #[serde(default)]
    pub integrity_digest: Option<String>,
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
            integrity_digest: None,
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
    save_lock: AsyncMutex<()>,
    last_saved: Mutex<Option<EcstoreDiskBytes>>,
}

impl CheckpointManager {
    fn blocked_path(task_id: &str) -> std::path::PathBuf {
        Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_BLOCKED_FILE}"))
    }

    /// Return whether a checkpoint was permanently isolated after a malformed
    /// or unsupported snapshot was observed.
    pub(crate) async fn is_blocked(disk: &DiskStore, task_id: &str) -> bool {
        if validate_resume_task_id(task_id).is_err() {
            return false;
        }
        let blocked_path = Self::blocked_path(task_id);
        let Ok(path) = path_to_str(&blocked_path) else {
            return false;
        };
        match HealDiskExt::read_all(disk.as_ref(), RUSTFS_META_BUCKET, path).await {
            Ok(_) => true,
            Err(crate::heal::DiskError::FileNotFound) => false,
            Err(_) => true,
        }
    }

    /// Validate the checkpoint while enumerating resumable state. This reads
    /// the checkpoint once and also isolates malformed or unsupported data.
    pub(crate) async fn is_resumable(disk: &DiskStore, task_id: &str) -> Result<bool> {
        validate_resume_task_id(task_id)?;
        if Self::is_blocked(disk, task_id).await {
            return Err(Error::InvalidCheckpoint(format!("Resume task {task_id} has a blocked checkpoint")));
        }
        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"));
        let Ok(path) = path_to_str(&file_path) else {
            return Err(Error::InvalidCheckpoint("Resume checkpoint path is not valid UTF-8".to_string()));
        };
        match HealDiskExt::read_all(disk.as_ref(), RUSTFS_META_BUCKET, path).await {
            Ok(bytes) if bytes.is_empty() => Ok(true),
            Ok(bytes) => Self::load_from_data(disk.clone(), task_id, bytes.to_vec())
                .await
                .map(|_| true),
            Err(crate::heal::DiskError::FileNotFound) => Ok(true),
            Err(error) => Err(error.into()),
        }
    }

    async fn block_invalid_snapshot(disk: &DiskStore, task_id: &str) {
        // This marker is intentionally version-agnostic: an unsupported reader
        // must stop selector retries until an operator cleans up the snapshot.
        let blocked_path = Self::blocked_path(task_id);
        let Ok(path) = path_to_str(&blocked_path) else {
            return;
        };
        let result = EcstoreDiskAPI::compare_and_update_file(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            path,
            None,
            Some(EcstoreDiskBytes::from_static(b"blocked")),
        )
        .await;
        match result {
            Ok(EcstoreConditionalFileUpdate::Updated | EcstoreConditionalFileUpdate::Mismatch) => {}
            Ok(EcstoreConditionalFileUpdate::Missing) => warn!(
                target: "rustfs::heal::resume",
                event = EVENT_HEAL_CHECKPOINT_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_RESUME,
                task_id,
                state = "blocked_marker_write_failed",
                error = "marker target disappeared",
                "Heal checkpoint could not persist its blocked marker"
            ),
            Err(error) => warn!(
                target: "rustfs::heal::resume",
                event = EVENT_HEAL_CHECKPOINT_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_RESUME,
                task_id,
                state = "blocked_marker_write_failed",
                error = %error,
                "Heal checkpoint could not persist its blocked marker"
            ),
        }
    }

    /// create new checkpoint manager
    pub async fn new(disk: DiskStore, task_id: String) -> Result<Self> {
        validate_resume_task_id(&task_id)?;
        let checkpoint_volume = format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}");
        if let Err(error) = EcstoreDiskAPI::make_volume(disk.as_ref(), &checkpoint_volume).await
            && error != crate::heal::DiskError::VolumeExists
        {
            return Err(Error::TaskExecutionFailed {
                message: format!("Failed to create checkpoint volume: {error}"),
            });
        }
        let checkpoint = ResumeCheckpoint::new(task_id);
        let manager = Self {
            disk,
            checkpoint: Arc::new(RwLock::new(checkpoint)),
            throttle: Mutex::new(PersistThrottle::new()),
            save_lock: AsyncMutex::new(()),
            last_saved: Mutex::new(None),
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
            return Err(e);
        }
        Ok(manager)
    }

    /// load checkpoint from disk
    pub async fn load_from_disk(disk: DiskStore, task_id: &str) -> Result<Self> {
        validate_resume_task_id(task_id)?;
        let checkpoint_data = Self::read_checkpoint_file(&disk, task_id).await?;
        Self::load_from_data(disk, task_id, checkpoint_data).await
    }

    async fn load_from_data(disk: DiskStore, task_id: &str, checkpoint_data: Vec<u8>) -> Result<Self> {
        validate_resume_task_id(task_id)?;
        let mut checkpoint: ResumeCheckpoint = match serde_json::from_slice(&checkpoint_data) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                Self::block_invalid_snapshot(&disk, task_id).await;
                return Err(Error::TaskExecutionFailed {
                    message: format!("Failed to deserialize checkpoint: {error}"),
                });
            }
        };
        if checkpoint.task_id != task_id {
            Self::block_invalid_snapshot(&disk, task_id).await;
            return Err(Error::TaskExecutionFailed {
                message: "Resume checkpoint task id does not match filename".to_string(),
            });
        }

        // Older checkpoints can contain identities that are not comparable to
        // the current keys or lack their corresponding aggregate counters.
        // Discard the stale sets and position so the scan restarts cleanly.
        if checkpoint.schema_version > CURRENT_CHECKPOINT_SCHEMA {
            Self::block_invalid_snapshot(&disk, task_id).await;
            return Err(Error::TaskExecutionFailed {
                message: format!(
                    "Checkpoint schema {} is newer than supported schema {CURRENT_CHECKPOINT_SCHEMA}",
                    checkpoint.schema_version
                ),
            });
        }

        let integrity_verified = if let Some(expected) = checkpoint.integrity_digest.as_deref() {
            let actual = Self::checkpoint_digest(&Self::serialize_without_digest(&checkpoint)?);
            if expected != actual {
                Self::block_invalid_snapshot(&disk, task_id).await;
                return Err(Error::InvalidCheckpoint(format!(
                    "Resume checkpoint digest does not match task {task_id}"
                )));
            }
            true
        } else if checkpoint.schema_version >= CURRENT_CHECKPOINT_SCHEMA {
            Self::block_invalid_snapshot(&disk, task_id).await;
            return Err(Error::InvalidCheckpoint(format!(
                "Resume checkpoint digest is missing for task {task_id}"
            )));
        } else {
            let digest_path = Self::digest_path(task_id);
            let digest_path = path_to_str(&digest_path)?;
            match HealDiskExt::read_all(disk.as_ref(), RUSTFS_META_BUCKET, digest_path).await {
                Ok(expected) => {
                    let actual = Self::checkpoint_digest(&checkpoint_data);
                    if expected.as_ref() != actual.as_bytes() {
                        Self::block_invalid_snapshot(&disk, task_id).await;
                        return Err(Error::InvalidCheckpoint(format!(
                            "Resume checkpoint digest does not match task {task_id}"
                        )));
                    }
                    true
                }
                Err(crate::heal::DiskError::FileNotFound) => false,
                Err(error) => {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to read checkpoint digest: {error}"),
                    });
                }
            }
        };

        if checkpoint.schema_version < CHECKPOINT_PER_VERSION_SCHEMA || !integrity_verified {
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
        }
        checkpoint.schema_version = CURRENT_CHECKPOINT_SCHEMA;

        Ok(Self {
            disk,
            checkpoint: Arc::new(RwLock::new(checkpoint)),
            throttle: Mutex::new(PersistThrottle::new()),
            save_lock: AsyncMutex::new(()),
            last_saved: Mutex::new(Some(EcstoreDiskBytes::from(checkpoint_data))),
        })
    }

    /// check if checkpoint exists
    pub async fn has_checkpoint(disk: &DiskStore, task_id: &str) -> bool {
        if validate_resume_task_id(task_id).is_err() {
            return false;
        }
        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"));
        match path_to_str(&file_path) {
            Ok(path_str) => match HealDiskExt::read_all(disk.as_ref(), RUSTFS_META_BUCKET, path_str).await {
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
        delete_resume_file(&self.disk, &Self::digest_path(&task_id)).await?;
        delete_resume_file(&self.disk, &Self::blocked_path(&task_id)).await?;

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
        // Serialize saves and take the snapshot only after acquiring the lock:
        // a slower writer must not publish a snapshot taken before a newer one.
        let _save_guard = self.save_lock.lock().await;
        let checkpoint = self.checkpoint.read().await.clone();
        validate_resume_task_id(&checkpoint.task_id)?;
        let unsigned_checkpoint_data = Self::serialize_without_digest(&checkpoint)?;
        let digest = Self::checkpoint_digest(&unsigned_checkpoint_data);
        let mut persisted_checkpoint = checkpoint.clone();
        persisted_checkpoint.integrity_digest = Some(digest);
        let checkpoint_data =
            EcstoreDiskBytes::from(serde_json::to_vec(&persisted_checkpoint).map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to serialize checkpoint: {e}"),
            })?);

        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{}_{}", checkpoint.task_id, RESUME_CHECKPOINT_FILE));

        let path_str = path_to_str(&file_path)?;
        let last_saved = self
            .last_saved
            .lock()
            .map_err(|_| Error::TaskExecutionFailed {
                message: "Checkpoint save state lock is poisoned; refusing to save".to_string(),
            })?
            .clone();
        let update = EcstoreDiskAPI::compare_and_update_file(
            self.disk.as_ref(),
            RUSTFS_META_BUCKET,
            path_str,
            last_saved.clone(),
            Some(checkpoint_data.clone()),
        )
        .await
        .map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to save checkpoint: {e}"),
        })?;

        let expected = match update {
            EcstoreConditionalFileUpdate::Updated => None,
            EcstoreConditionalFileUpdate::Missing => {
                return Err(Error::TaskExecutionFailed {
                    message: "Checkpoint was removed after this manager saved it; refusing to recreate it".to_string(),
                });
            }
            EcstoreConditionalFileUpdate::Mismatch => {
                // A healthy manager normally completes the CAS above without
                // another read or JSON parse. Inspect only after a mismatch so
                // corruption and future schemas cannot be overwritten blindly.
                let existing = match HealDiskExt::read_all(self.disk.as_ref(), RUSTFS_META_BUCKET, path_str).await {
                    Ok(existing) => existing,
                    Err(crate::heal::DiskError::FileNotFound) => {
                        return Err(Error::TaskExecutionFailed {
                            message: "Checkpoint was removed after this manager saved it; refusing to recreate it".to_string(),
                        });
                    }
                    Err(error) => {
                        return Err(Error::TaskExecutionFailed {
                            message: format!("Failed to inspect checkpoint after CAS mismatch: {error}"),
                        });
                    }
                };

                if existing.is_empty() && last_saved.is_none() {
                    Some(existing)
                } else {
                    let current: ResumeCheckpoint = match serde_json::from_slice(&existing) {
                        Ok(current) => current,
                        Err(error) => {
                            Self::block_invalid_snapshot(&self.disk, &checkpoint.task_id).await;
                            return Err(Error::TaskExecutionFailed {
                                message: format!("Existing checkpoint is corrupt: {error}"),
                            });
                        }
                    };
                    if current.task_id != checkpoint.task_id {
                        Self::block_invalid_snapshot(&self.disk, &checkpoint.task_id).await;
                        return Err(Error::TaskExecutionFailed {
                            message: "Existing checkpoint task id does not match filename".to_string(),
                        });
                    }
                    if current.schema_version > CURRENT_CHECKPOINT_SCHEMA {
                        Self::block_invalid_snapshot(&self.disk, &checkpoint.task_id).await;
                        return Err(Error::TaskExecutionFailed {
                            message: format!(
                                "Existing checkpoint schema {} is newer than supported schema {CURRENT_CHECKPOINT_SCHEMA}",
                                current.schema_version
                            ),
                        });
                    }
                    if last_saved.as_ref().is_none_or(|saved| saved.as_ref() != existing.as_ref()) {
                        return Err(Error::TaskExecutionFailed {
                            message: "Checkpoint changed since this manager loaded it; refusing to overwrite newer progress"
                                .to_string(),
                        });
                    }
                    Some(existing)
                }
            }
        };

        if let Some(expected) = expected {
            match EcstoreDiskAPI::compare_and_update_file(
                self.disk.as_ref(),
                RUSTFS_META_BUCKET,
                path_str,
                Some(expected),
                Some(checkpoint_data.clone()),
            )
            .await
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to save checkpoint after CAS mismatch: {e}"),
            })? {
                EcstoreConditionalFileUpdate::Updated => {}
                EcstoreConditionalFileUpdate::Missing | EcstoreConditionalFileUpdate::Mismatch => {
                    return Err(Error::TaskExecutionFailed {
                        message: "Checkpoint changed while saving; refusing to overwrite newer progress".to_string(),
                    });
                }
            }
        }

        let mut last_saved = self.last_saved.lock().map_err(|_| Error::TaskExecutionFailed {
            message: "Checkpoint save state lock is poisoned after save".to_string(),
        })?;
        *last_saved = Some(checkpoint_data);

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
        HealDiskExt::read_all(disk.as_ref(), RUSTFS_META_BUCKET, path_str)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to read checkpoint file: {e}"),
            })
    }

    fn serialize_without_digest(checkpoint: &ResumeCheckpoint) -> Result<Vec<u8>> {
        let mut unsigned = checkpoint.clone();
        unsigned.integrity_digest = None;
        let mut value = serde_json::to_value(&unsigned).map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to serialize checkpoint: {e}"),
        })?;
        for field in ["processed_objects", "failed_objects", "skipped_objects"] {
            let Some(values) = value.get_mut(field).and_then(serde_json::Value::as_array_mut) else {
                return Err(Error::TaskExecutionFailed {
                    message: format!("Failed to canonicalize checkpoint field: {field}"),
                });
            };
            values.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        }
        serde_json::to_vec(&value).map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to serialize checkpoint: {e}"),
        })
    }

    fn checkpoint_digest(checkpoint_data: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(Sha256::digest(checkpoint_data))
    }

    fn digest_path(task_id: &str) -> std::path::PathBuf {
        Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_DIGEST_FILE}"))
    }
}
