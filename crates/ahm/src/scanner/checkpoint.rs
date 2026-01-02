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

use crate::scanner::node_scanner::ScanProgress;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CheckpointData {
    pub version: u32,
    pub timestamp: SystemTime,
    pub progress: ScanProgress,
    pub node_id: String,
    pub checksum: u64,
}

impl CheckpointData {
    pub fn new(progress: ScanProgress, node_id: String) -> Self {
        let mut checkpoint = Self {
            version: 1,
            timestamp: SystemTime::now(),
            progress,
            node_id,
            checksum: 0,
        };

        checkpoint.checksum = checkpoint.calculate_checksum();
        checkpoint
    }

    fn calculate_checksum(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.version.hash(&mut hasher);
        self.node_id.hash(&mut hasher);
        self.progress.current_cycle.hash(&mut hasher);
        self.progress.current_disk_index.hash(&mut hasher);

        if let Some(ref bucket) = self.progress.current_bucket {
            bucket.hash(&mut hasher);
        }

        if let Some(ref key) = self.progress.last_scan_key {
            key.hash(&mut hasher);
        }

        hasher.finish()
    }

    pub fn verify_integrity(&self) -> bool {
        let calculated_checksum = self.calculate_checksum();
        self.checksum == calculated_checksum
    }
}

pub struct CheckpointManager {
    checkpoint_file: PathBuf,
    backup_file: PathBuf,
    temp_file: PathBuf,
    save_interval: Duration,
    last_save: RwLock<SystemTime>,
    node_id: String,
}

impl CheckpointManager {
    pub fn new(node_id: &str, data_dir: &Path) -> Self {
        if !data_dir.exists()
            && let Err(e) = std::fs::create_dir_all(data_dir)
        {
            error!("create data dir failed {:?}: {}", data_dir, e);
        }

        let checkpoint_file = data_dir.join(format!("scanner_checkpoint_{node_id}.json"));
        let backup_file = data_dir.join(format!("scanner_checkpoint_{node_id}.backup"));
        let temp_file = data_dir.join(format!("scanner_checkpoint_{node_id}.tmp"));

        Self {
            checkpoint_file,
            backup_file,
            temp_file,
            save_interval: Duration::from_secs(30), // 30s
            last_save: RwLock::new(SystemTime::UNIX_EPOCH),
            node_id: node_id.to_string(),
        }
    }

    pub async fn save_checkpoint(&self, progress: &ScanProgress) -> Result<()> {
        let now = SystemTime::now();
        let last_save = *self.last_save.read().await;

        if now.duration_since(last_save).unwrap_or(Duration::ZERO) < self.save_interval {
            return Ok(());
        }

        let checkpoint_data = CheckpointData::new(progress.clone(), self.node_id.clone());

        let json_data = serde_json::to_string_pretty(&checkpoint_data)
            .map_err(|e| Error::Serialization(format!("serialize checkpoint failed: {e}")))?;

        tokio::fs::write(&self.temp_file, json_data)
            .await
            .map_err(|e| Error::IO(format!("write temp checkpoint file failed: {e}")))?;

        if self.checkpoint_file.exists() {
            tokio::fs::copy(&self.checkpoint_file, &self.backup_file)
                .await
                .map_err(|e| Error::IO(format!("backup checkpoint file failed: {e}")))?;
        }

        tokio::fs::rename(&self.temp_file, &self.checkpoint_file)
            .await
            .map_err(|e| Error::IO(format!("replace checkpoint file failed: {e}")))?;

        *self.last_save.write().await = now;

        debug!(
            "save checkpoint to {:?}, cycle: {}, disk index: {}",
            self.checkpoint_file, checkpoint_data.progress.current_cycle, checkpoint_data.progress.current_disk_index
        );

        Ok(())
    }

    pub async fn load_checkpoint(&self) -> Result<Option<ScanProgress>> {
        // first try main checkpoint file
        match self.load_checkpoint_from_file(&self.checkpoint_file).await {
            Ok(checkpoint) => {
                info!(
                    "restore scan progress from main checkpoint file: cycle={}, disk index={}, last scan key={:?}",
                    checkpoint.current_cycle, checkpoint.current_disk_index, checkpoint.last_scan_key
                );
                Ok(Some(checkpoint))
            }
            Err(e) => {
                warn!("main checkpoint file is corrupted or not exists: {}", e);

                // try backup file
                match self.load_checkpoint_from_file(&self.backup_file).await {
                    Ok(checkpoint) => {
                        warn!(
                            "restore scan progress from backup file: cycle={}, disk index={}",
                            checkpoint.current_cycle, checkpoint.current_disk_index
                        );

                        // copy backup file to main checkpoint file
                        if let Err(copy_err) = tokio::fs::copy(&self.backup_file, &self.checkpoint_file).await {
                            warn!("restore main checkpoint file failed: {}", copy_err);
                        }

                        Ok(Some(checkpoint))
                    }
                    Err(backup_e) => {
                        warn!("backup file is corrupted or not exists: {}", backup_e);
                        info!("cannot restore scan progress, will start fresh scan");
                        Ok(None)
                    }
                }
            }
        }
    }

    /// load checkpoint from file
    async fn load_checkpoint_from_file(&self, file_path: &Path) -> Result<ScanProgress> {
        if !file_path.exists() {
            return Err(Error::NotFound(format!("checkpoint file not exists: {file_path:?}")));
        }

        // read file content
        let content = tokio::fs::read_to_string(file_path)
            .await
            .map_err(|e| Error::IO(format!("read checkpoint file failed: {e}")))?;

        // deserialize
        let checkpoint_data: CheckpointData =
            serde_json::from_str(&content).map_err(|e| Error::Serialization(format!("deserialize checkpoint failed: {e}")))?;

        // validate checkpoint data
        self.validate_checkpoint(&checkpoint_data)?;

        Ok(checkpoint_data.progress)
    }

    /// validate checkpoint data
    fn validate_checkpoint(&self, checkpoint: &CheckpointData) -> Result<()> {
        // validate data integrity
        if !checkpoint.verify_integrity() {
            return Err(Error::InvalidCheckpoint(
                "checkpoint data verification failed, may be corrupted".to_string(),
            ));
        }

        // validate node id match
        if checkpoint.node_id != self.node_id {
            return Err(Error::InvalidCheckpoint(format!(
                "checkpoint node id not match: expected {}, actual {}",
                self.node_id, checkpoint.node_id
            )));
        }

        let now = SystemTime::now();
        let checkpoint_age = now.duration_since(checkpoint.timestamp).unwrap_or(Duration::MAX);

        // checkpoint is too old (more than 24 hours), may be data expired
        if checkpoint_age > Duration::from_secs(24 * 3600) {
            return Err(Error::InvalidCheckpoint(format!("checkpoint data is too old: {checkpoint_age:?}")));
        }

        // validate version compatibility
        if checkpoint.version > 1 {
            return Err(Error::InvalidCheckpoint(format!(
                "unsupported checkpoint version: {}",
                checkpoint.version
            )));
        }

        Ok(())
    }

    /// clean checkpoint file
    ///
    /// called when scanner stops or resets
    pub async fn cleanup_checkpoint(&self) -> Result<()> {
        // delete main file
        if self.checkpoint_file.exists() {
            tokio::fs::remove_file(&self.checkpoint_file)
                .await
                .map_err(|e| Error::IO(format!("delete main checkpoint file failed: {e}")))?;
        }

        // delete backup file
        if self.backup_file.exists() {
            tokio::fs::remove_file(&self.backup_file)
                .await
                .map_err(|e| Error::IO(format!("delete backup checkpoint file failed: {e}")))?;
        }

        // delete temp file
        if self.temp_file.exists() {
            tokio::fs::remove_file(&self.temp_file)
                .await
                .map_err(|e| Error::IO(format!("delete temp checkpoint file failed: {e}")))?;
        }

        info!("cleaned up all checkpoint files");
        Ok(())
    }

    /// get checkpoint file info
    pub async fn get_checkpoint_info(&self) -> Result<Option<CheckpointInfo>> {
        if !self.checkpoint_file.exists() {
            return Ok(None);
        }

        let metadata = tokio::fs::metadata(&self.checkpoint_file)
            .await
            .map_err(|e| Error::IO(format!("get checkpoint file metadata failed: {e}")))?;

        let content = tokio::fs::read_to_string(&self.checkpoint_file)
            .await
            .map_err(|e| Error::IO(format!("read checkpoint file failed: {e}")))?;

        let checkpoint_data: CheckpointData =
            serde_json::from_str(&content).map_err(|e| Error::Serialization(format!("deserialize checkpoint failed: {e}")))?;

        Ok(Some(CheckpointInfo {
            file_size: metadata.len(),
            last_modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            checkpoint_timestamp: checkpoint_data.timestamp,
            current_cycle: checkpoint_data.progress.current_cycle,
            current_disk_index: checkpoint_data.progress.current_disk_index,
            completed_disks_count: checkpoint_data.progress.completed_disks.len(),
            is_valid: checkpoint_data.verify_integrity(),
        }))
    }

    /// force save checkpoint (ignore time interval limit)
    pub async fn force_save_checkpoint(&self, progress: &ScanProgress) -> Result<()> {
        // temporarily reset last save time, force save
        *self.last_save.write().await = SystemTime::UNIX_EPOCH;
        self.save_checkpoint(progress).await
    }

    /// set save interval
    pub async fn set_save_interval(&mut self, interval: Duration) {
        self.save_interval = interval;
        info!("checkpoint save interval set to: {:?}", interval);
    }
}

/// checkpoint info
#[derive(Debug, Clone)]
pub struct CheckpointInfo {
    /// file size
    pub file_size: u64,
    /// file last modified time
    pub last_modified: SystemTime,
    /// checkpoint creation time
    pub checkpoint_timestamp: SystemTime,
    /// current scan cycle
    pub current_cycle: u64,
    /// current disk index
    pub current_disk_index: usize,
    /// completed disks count
    pub completed_disks_count: usize,
    /// checkpoint is valid
    pub is_valid: bool,
}
