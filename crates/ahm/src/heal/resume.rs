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
use rustfs_ecstore::disk::{BUCKET_META_PREFIX, DiskAPI, DiskStore, RUSTFS_META_BUCKET};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// resume state file constants
const RESUME_STATE_FILE: &str = "ahm_resume_state.json";
const RESUME_PROGRESS_FILE: &str = "ahm_progress.json";
const RESUME_CHECKPOINT_FILE: &str = "ahm_checkpoint.json";

/// Helper function to convert Path to &str, returning an error if conversion fails
fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| Error::other(format!("Invalid UTF-8 path: {:?}", path)))
}

/// resume state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeState {
    /// task id
    pub task_id: String,
    /// task type
    pub task_type: String,
    /// set disk identifier (for erasure set tasks)
    #[serde(default)]
    pub set_disk_id: String,
    /// start time
    pub start_time: u64,
    /// last update time
    pub last_update: u64,
    /// completed
    pub completed: bool,
    /// total objects
    pub total_objects: u64,
    /// processed objects
    pub processed_objects: u64,
    /// successful objects
    pub successful_objects: u64,
    /// failed objects
    pub failed_objects: u64,
    /// skipped objects
    pub skipped_objects: u64,
    /// current bucket
    pub current_bucket: Option<String>,
    /// current object
    pub current_object: Option<String>,
    /// completed buckets
    pub completed_buckets: Vec<String>,
    /// pending buckets
    pub pending_buckets: Vec<String>,
    /// error message
    pub error_message: Option<String>,
    /// retry count
    pub retry_count: u32,
    /// max retries
    pub max_retries: u32,
}

impl ResumeState {
    pub fn new(task_id: String, task_type: String, set_disk_id: String, buckets: Vec<String>) -> Self {
        Self {
            task_id,
            task_type,
            set_disk_id,
            start_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            last_update: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            completed: false,
            total_objects: 0,
            processed_objects: 0,
            successful_objects: 0,
            failed_objects: 0,
            skipped_objects: 0,
            current_bucket: None,
            current_object: None,
            completed_buckets: Vec::new(),
            pending_buckets: buckets,
            error_message: None,
            retry_count: 0,
            max_retries: 3,
        }
    }

    pub fn update_progress(&mut self, processed: u64, successful: u64, failed: u64, skipped: u64) {
        self.processed_objects = processed;
        self.successful_objects = successful;
        self.failed_objects = failed;
        self.skipped_objects = skipped;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn set_current_item(&mut self, bucket: Option<String>, object: Option<String>) {
        self.current_bucket = bucket;
        self.current_object = object;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn complete_bucket(&mut self, bucket: &str) {
        if !self.completed_buckets.contains(&bucket.to_string()) {
            self.completed_buckets.push(bucket.to_string());
        }
        if let Some(pos) = self.pending_buckets.iter().position(|b| b == bucket) {
            self.pending_buckets.remove(pos);
        }
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn mark_completed(&mut self) {
        self.completed = true;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn set_error(&mut self, error: String) {
        self.error_message = Some(error);
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    pub fn get_progress_percentage(&self) -> f64 {
        if self.total_objects == 0 {
            return 0.0;
        }
        (self.processed_objects as f64 / self.total_objects as f64) * 100.0
    }

    pub fn get_success_rate(&self) -> f64 {
        let total = self.successful_objects + self.failed_objects;
        if total == 0 {
            return 0.0;
        }
        (self.successful_objects as f64 / total as f64) * 100.0
    }
}

/// resume manager
pub struct ResumeManager {
    disk: DiskStore,
    state: Arc<RwLock<ResumeState>>,
}

impl ResumeManager {
    /// create new resume manager
    pub async fn new(
        disk: DiskStore,
        task_id: String,
        task_type: String,
        set_disk_id: String,
        buckets: Vec<String>,
    ) -> Result<Self> {
        let state = ResumeState::new(task_id, task_type, set_disk_id, buckets);
        let manager = Self {
            disk,
            state: Arc::new(RwLock::new(state)),
        };

        // save initial state
        manager.save_state().await?;
        Ok(manager)
    }

    /// load resume state from disk
    pub async fn load_from_disk(disk: DiskStore, task_id: &str) -> Result<Self> {
        let state_data = Self::read_state_file(&disk, task_id).await?;
        let state: ResumeState = serde_json::from_slice(&state_data).map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to deserialize resume state: {e}"),
        })?;

        Ok(Self {
            disk,
            state: Arc::new(RwLock::new(state)),
        })
    }

    /// check if resume state exists
    pub async fn has_resume_state(disk: &DiskStore, task_id: &str) -> bool {
        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_STATE_FILE}"));
        match path_to_str(&file_path) {
            Ok(path_str) => match disk.read_all(RUSTFS_META_BUCKET, path_str).await {
                Ok(data) => !data.is_empty(),
                Err(_) => false,
            },
            Err(_) => false,
        }
    }

    /// get current state
    pub async fn get_state(&self) -> ResumeState {
        self.state.read().await.clone()
    }

    /// update progress
    pub async fn update_progress(&self, processed: u64, successful: u64, failed: u64, skipped: u64) -> Result<()> {
        let mut state = self.state.write().await;
        state.update_progress(processed, successful, failed, skipped);
        drop(state);
        self.save_state().await
    }

    /// set current item
    pub async fn set_current_item(&self, bucket: Option<String>, object: Option<String>) -> Result<()> {
        let mut state = self.state.write().await;
        state.set_current_item(bucket, object);
        drop(state);
        self.save_state().await
    }

    /// complete bucket
    pub async fn complete_bucket(&self, bucket: &str) -> Result<()> {
        let mut state = self.state.write().await;
        state.complete_bucket(bucket);
        drop(state);
        self.save_state().await
    }

    /// mark task completed
    pub async fn mark_completed(&self) -> Result<()> {
        let mut state = self.state.write().await;
        state.mark_completed();
        drop(state);
        self.save_state().await
    }

    /// set error message
    pub async fn set_error(&self, error: String) -> Result<()> {
        let mut state = self.state.write().await;
        state.set_error(error);
        drop(state);
        self.save_state().await
    }

    /// increment retry count
    pub async fn increment_retry(&self) -> Result<()> {
        let mut state = self.state.write().await;
        state.increment_retry();
        drop(state);
        self.save_state().await
    }

    /// cleanup resume state
    pub async fn cleanup(&self) -> Result<()> {
        let state = self.state.read().await;
        let task_id = &state.task_id;

        // delete state files
        let state_file = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_STATE_FILE}"));
        let progress_file = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_PROGRESS_FILE}"));
        let checkpoint_file = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"));

        // ignore delete errors, files may not exist
        if let Ok(path_str) = path_to_str(&state_file) {
            let _ = self.disk.delete(RUSTFS_META_BUCKET, path_str, Default::default()).await;
        }
        if let Ok(path_str) = path_to_str(&progress_file) {
            let _ = self.disk.delete(RUSTFS_META_BUCKET, path_str, Default::default()).await;
        }
        if let Ok(path_str) = path_to_str(&checkpoint_file) {
            let _ = self.disk.delete(RUSTFS_META_BUCKET, path_str, Default::default()).await;
        }

        info!("Cleaned up resume state for task: {}", task_id);
        Ok(())
    }

    /// save state to disk
    async fn save_state(&self) -> Result<()> {
        let state = self.state.read().await;
        let state_data = serde_json::to_vec(&*state).map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to serialize resume state: {e}"),
        })?;

        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{}_{}", state.task_id, RESUME_STATE_FILE));

        let path_str = path_to_str(&file_path)?;
        self.disk
            .write_all(RUSTFS_META_BUCKET, path_str, state_data.into())
            .await
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to save resume state: {e}"),
            })?;

        debug!("Saved resume state for task: {}", state.task_id);
        Ok(())
    }

    /// read state file from disk
    async fn read_state_file(disk: &DiskStore, task_id: &str) -> Result<Vec<u8>> {
        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_STATE_FILE}"));

        let path_str = path_to_str(&file_path)?;
        disk.read_all(RUSTFS_META_BUCKET, path_str)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to read resume state file: {e}"),
            })
    }
}

/// resume checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeCheckpoint {
    /// task id
    pub task_id: String,
    /// checkpoint time
    pub checkpoint_time: u64,
    /// current bucket index
    pub current_bucket_index: usize,
    /// current object index
    pub current_object_index: usize,
    /// processed objects
    pub processed_objects: Vec<String>,
    /// failed objects
    pub failed_objects: Vec<String>,
    /// skipped objects
    pub skipped_objects: Vec<String>,
}

impl ResumeCheckpoint {
    pub fn new(task_id: String) -> Self {
        Self {
            task_id,
            checkpoint_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            current_bucket_index: 0,
            current_object_index: 0,
            processed_objects: Vec::new(),
            failed_objects: Vec::new(),
            skipped_objects: Vec::new(),
        }
    }

    pub fn update_position(&mut self, bucket_index: usize, object_index: usize) {
        self.current_bucket_index = bucket_index;
        self.current_object_index = object_index;
        self.checkpoint_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn add_processed_object(&mut self, object: String) {
        if !self.processed_objects.contains(&object) {
            self.processed_objects.push(object);
        }
    }

    pub fn add_failed_object(&mut self, object: String) {
        if !self.failed_objects.contains(&object) {
            self.failed_objects.push(object);
        }
    }

    pub fn add_skipped_object(&mut self, object: String) {
        if !self.skipped_objects.contains(&object) {
            self.skipped_objects.push(object);
        }
    }
}

/// resume checkpoint manager
pub struct CheckpointManager {
    disk: DiskStore,
    checkpoint: Arc<RwLock<ResumeCheckpoint>>,
}

impl CheckpointManager {
    /// create new checkpoint manager
    pub async fn new(disk: DiskStore, task_id: String) -> Result<Self> {
        let checkpoint = ResumeCheckpoint::new(task_id);
        let manager = Self {
            disk,
            checkpoint: Arc::new(RwLock::new(checkpoint)),
        };

        // save initial checkpoint
        manager.save_checkpoint().await?;
        Ok(manager)
    }

    /// load checkpoint from disk
    pub async fn load_from_disk(disk: DiskStore, task_id: &str) -> Result<Self> {
        let checkpoint_data = Self::read_checkpoint_file(&disk, task_id).await?;
        let checkpoint: ResumeCheckpoint = serde_json::from_slice(&checkpoint_data).map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to deserialize checkpoint: {e}"),
        })?;

        Ok(Self {
            disk,
            checkpoint: Arc::new(RwLock::new(checkpoint)),
        })
    }

    /// check if checkpoint exists
    pub async fn has_checkpoint(disk: &DiskStore, task_id: &str) -> bool {
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
        self.save_checkpoint().await
    }

    /// add processed object
    pub async fn add_processed_object(&self, object: String) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.add_processed_object(object);
        drop(checkpoint);
        self.save_checkpoint().await
    }

    /// add failed object
    pub async fn add_failed_object(&self, object: String) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.add_failed_object(object);
        drop(checkpoint);
        self.save_checkpoint().await
    }

    /// add skipped object
    pub async fn add_skipped_object(&self, object: String) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.add_skipped_object(object);
        drop(checkpoint);
        self.save_checkpoint().await
    }

    /// cleanup checkpoint
    pub async fn cleanup(&self) -> Result<()> {
        let checkpoint = self.checkpoint.read().await;
        let task_id = &checkpoint.task_id;

        let checkpoint_file = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"));
        if let Ok(path_str) = path_to_str(&checkpoint_file) {
            let _ = self.disk.delete(RUSTFS_META_BUCKET, path_str, Default::default()).await;
        }

        info!("Cleaned up checkpoint for task: {}", task_id);
        Ok(())
    }

    /// save checkpoint to disk
    async fn save_checkpoint(&self) -> Result<()> {
        let checkpoint = self.checkpoint.read().await;
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

        debug!("Saved checkpoint for task: {}", checkpoint.task_id);
        Ok(())
    }

    /// read checkpoint file from disk
    async fn read_checkpoint_file(disk: &DiskStore, task_id: &str) -> Result<Vec<u8>> {
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

/// resume utils
pub struct ResumeUtils;

impl ResumeUtils {
    /// generate unique task id
    pub fn generate_task_id() -> String {
        Uuid::new_v4().to_string()
    }

    /// check if task can be resumed
    pub async fn can_resume_task(disk: &DiskStore, task_id: &str) -> bool {
        ResumeManager::has_resume_state(disk, task_id).await
    }

    /// get all resumable task ids
    pub async fn get_resumable_tasks(disk: &DiskStore) -> Result<Vec<String>> {
        // List all files in the buckets metadata directory
        let entries = match disk.list_dir("", RUSTFS_META_BUCKET, BUCKET_META_PREFIX, -1).await {
            Ok(entries) => entries,
            Err(e) => {
                debug!("Failed to list resume state files: {}", e);
                return Ok(Vec::new());
            }
        };

        let mut task_ids = Vec::new();

        // Filter files that end with ahm_resume_state.json and extract task IDs
        for entry in entries {
            if entry.ends_with(&format!("_{RESUME_STATE_FILE}")) {
                // Extract task ID from filename: {task_id}_ahm_resume_state.json
                if let Some(task_id) = entry.strip_suffix(&format!("_{RESUME_STATE_FILE}")) {
                    if !task_id.is_empty() {
                        task_ids.push(task_id.to_string());
                    }
                }
            }
        }

        debug!("Found {} resumable tasks: {:?}", task_ids.len(), task_ids);
        Ok(task_ids)
    }

    /// cleanup expired resume states
    pub async fn cleanup_expired_states(disk: &DiskStore, max_age_hours: u64) -> Result<()> {
        let task_ids = Self::get_resumable_tasks(disk).await?;
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        for task_id in task_ids {
            if let Ok(resume_manager) = ResumeManager::load_from_disk(disk.clone(), &task_id).await {
                let state = resume_manager.get_state().await;
                let age_hours = (current_time - state.last_update) / 3600;

                if age_hours > max_age_hours {
                    info!("Cleaning up expired resume state for task: {} (age: {} hours)", task_id, age_hours);
                    if let Err(e) = resume_manager.cleanup().await {
                        warn!("Failed to cleanup expired resume state for task {}: {}", task_id, e);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resume_state_creation() {
        let task_id = ResumeUtils::generate_task_id();
        let buckets = vec!["bucket1".to_string(), "bucket2".to_string()];
        let state = ResumeState::new(task_id.clone(), "erasure_set".to_string(), "pool_0_set_0".to_string(), buckets);

        assert_eq!(state.task_id, task_id);
        assert_eq!(state.task_type, "erasure_set");
        assert!(!state.completed);
        assert_eq!(state.processed_objects, 0);
        assert_eq!(state.pending_buckets.len(), 2);
    }

    #[tokio::test]
    async fn test_resume_state_progress() {
        let task_id = ResumeUtils::generate_task_id();
        let buckets = vec!["bucket1".to_string()];
        let mut state = ResumeState::new(task_id, "erasure_set".to_string(), "pool_0_set_0".to_string(), buckets);

        state.update_progress(10, 8, 1, 1);
        assert_eq!(state.processed_objects, 10);
        assert_eq!(state.successful_objects, 8);
        assert_eq!(state.failed_objects, 1);
        assert_eq!(state.skipped_objects, 1);

        let progress = state.get_progress_percentage();
        assert_eq!(progress, 0.0); // total_objects is 0

        state.total_objects = 100;
        let progress = state.get_progress_percentage();
        assert_eq!(progress, 10.0);
    }

    #[tokio::test]
    async fn test_resume_state_bucket_completion() {
        let task_id = ResumeUtils::generate_task_id();
        let buckets = vec!["bucket1".to_string(), "bucket2".to_string()];
        let mut state = ResumeState::new(task_id, "erasure_set".to_string(), "pool_0_set_0".to_string(), buckets);

        assert_eq!(state.pending_buckets.len(), 2);
        assert_eq!(state.completed_buckets.len(), 0);

        state.complete_bucket("bucket1");
        assert_eq!(state.pending_buckets.len(), 1);
        assert_eq!(state.completed_buckets.len(), 1);
        assert!(state.completed_buckets.contains(&"bucket1".to_string()));
    }

    #[tokio::test]
    async fn test_resume_utils() {
        let task_id1 = ResumeUtils::generate_task_id();
        let task_id2 = ResumeUtils::generate_task_id();

        assert_ne!(task_id1, task_id2);
        assert_eq!(task_id1.len(), 36); // UUID length
        assert_eq!(task_id2.len(), 36);
    }

    #[tokio::test]
    async fn test_get_resumable_tasks_integration() {
        use rustfs_ecstore::disk::{DiskOption, endpoint::Endpoint, new_disk};
        use tempfile::TempDir;

        // Create a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let disk_path = temp_dir.path().join("test_disk");
        std::fs::create_dir_all(&disk_path).unwrap();

        // Create a local disk for testing
        let endpoint = Endpoint::try_from(disk_path.to_string_lossy().as_ref()).unwrap();
        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };
        let disk = new_disk(&endpoint, &disk_option).await.unwrap();

        // Create necessary directories first (ignore if already exist)
        let _ = disk.make_volume(RUSTFS_META_BUCKET).await;
        let _ = disk.make_volume(&format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}")).await;

        // Create some test resume state files
        let task_ids = vec![
            "test-task-1".to_string(),
            "test-task-2".to_string(),
            "test-task-3".to_string(),
        ];

        // Save resume state files for each task
        for task_id in &task_ids {
            let state = ResumeState::new(
                task_id.clone(),
                "erasure_set".to_string(),
                "pool_0_set_0".to_string(),
                vec!["bucket1".to_string(), "bucket2".to_string()],
            );

            let state_data = serde_json::to_vec(&state).unwrap();
            let file_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");

            disk.write_all(RUSTFS_META_BUCKET, &file_path, state_data.into())
                .await
                .unwrap();
        }

        // Also create some non-resume state files to test filtering
        let non_resume_files = vec![
            "other_file.txt",
            "task4_ahm_checkpoint.json",
            "task5_ahm_progress.json",
            "_ahm_resume_state.json", // Invalid: empty task ID
        ];

        for file_name in non_resume_files {
            let file_path = format!("{BUCKET_META_PREFIX}/{file_name}");
            disk.write_all(RUSTFS_META_BUCKET, &file_path, b"test data".to_vec().into())
                .await
                .unwrap();
        }

        // Now call get_resumable_tasks to see if it finds the correct files
        let found_task_ids = ResumeUtils::get_resumable_tasks(&disk).await.unwrap();

        // Verify that only the valid resume state files are found
        assert_eq!(found_task_ids.len(), 3);
        for task_id in &task_ids {
            assert!(found_task_ids.contains(task_id), "Task ID {task_id} not found");
        }

        // Verify that invalid files are not included
        assert!(!found_task_ids.contains(&"".to_string()));
        assert!(!found_task_ids.contains(&"task4".to_string()));
        assert!(!found_task_ids.contains(&"task5".to_string()));

        // Clean up
        temp_dir.close().unwrap();
    }
}
