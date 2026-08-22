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

/// Current on-disk schema version for `ResumeCheckpoint`. Same rationale as
/// `CURRENT_RESUME_SCHEMA`: pre-per-version dedup identities are not comparable
/// to the new `compose_key` identities, so a stale checkpoint is discarded.
pub(super) const CURRENT_CHECKPOINT_SCHEMA: u32 = 5;

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

        // A checkpoint from an older schema stored latest-only dedup identities
        // that are not comparable to the new per-version `compose_key`
        // identities. Discard the stale sets and position, then stamp the
        // current schema so the scan restarts cleanly.
        if checkpoint.schema_version > CURRENT_CHECKPOINT_SCHEMA {
            Self::block_invalid_snapshot(&disk, task_id).await;
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
            checkpoint.current_bucket_index = 0;
            checkpoint.current_object_index = 0;
            checkpoint.schema_version = CURRENT_CHECKPOINT_SCHEMA;
        }

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

    /// Advance past a completed page and prune the per-object sets, then persist.
    pub async fn complete_page(&self, bucket_index: usize, object_index: usize) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.complete_page(bucket_index, object_index);
        drop(checkpoint);
        self.save_checkpoint_throttled().await
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
        let checkpoint_data =
            EcstoreDiskBytes::from(serde_json::to_vec(&checkpoint).map_err(|e| Error::TaskExecutionFailed {
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

        let digest_path = Self::digest_path(&checkpoint.task_id);
        let digest = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(checkpoint_data.as_ref()));
        HealDiskExt::write_all(
            self.disk.as_ref(),
            RUSTFS_META_BUCKET,
            path_to_str(&digest_path)?,
            EcstoreDiskBytes::from(digest.into_bytes()),
        )
        .await
        .map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to save checkpoint digest: {e}"),
        })?;

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
        let checkpoint = HealDiskExt::read_all(disk.as_ref(), RUSTFS_META_BUCKET, path_str)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to read checkpoint file: {e}"),
            })?;
        let digest_path = Self::digest_path(task_id);
        let digest_path = path_to_str(&digest_path)?;
        match HealDiskExt::read_all(disk.as_ref(), RUSTFS_META_BUCKET, digest_path).await {
            Ok(expected) => {
                let actual = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&checkpoint));
                if expected.as_ref() != actual.as_bytes() {
                    Self::block_invalid_snapshot(disk, task_id).await;
                    return Err(Error::InvalidCheckpoint(format!(
                        "Resume checkpoint digest does not match task {task_id}"
                    )));
                }
            }
            Err(crate::heal::DiskError::FileNotFound) => {}
            Err(error) => {
                return Err(Error::TaskExecutionFailed {
                    message: format!("Failed to read checkpoint digest: {error}"),
                });
            }
        }
        Ok(checkpoint)
    }

    fn digest_path(task_id: &str) -> std::path::PathBuf {
        Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_DIGEST_FILE}"))
    }
}
