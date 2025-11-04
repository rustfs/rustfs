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

use crate::scanner::node_scanner::{BucketStats, DiskStats, LocalScanStats};
use crate::{Error, Result};
use rustfs_common::data_usage::DataUsageInfo;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// local stats manager
pub struct LocalStatsManager {
    /// node id
    node_id: String,
    /// stats file path
    stats_file: PathBuf,
    /// backup file path
    backup_file: PathBuf,
    /// temp file path
    temp_file: PathBuf,
    /// local stats data
    stats: Arc<RwLock<LocalScanStats>>,
    /// save interval
    save_interval: Duration,
    /// last save time
    last_save: Arc<RwLock<SystemTime>>,
    /// stats counters
    counters: Arc<StatsCounters>,
}

/// stats counters
pub struct StatsCounters {
    /// total scanned objects
    pub total_objects_scanned: AtomicU64,
    /// total healthy objects
    pub total_healthy_objects: AtomicU64,
    /// total corrupted objects  
    pub total_corrupted_objects: AtomicU64,
    /// total scanned bytes
    pub total_bytes_scanned: AtomicU64,
    /// total scan errors
    pub total_scan_errors: AtomicU64,
    /// total heal triggered
    pub total_heal_triggered: AtomicU64,
}

impl Default for StatsCounters {
    fn default() -> Self {
        Self {
            total_objects_scanned: AtomicU64::new(0),
            total_healthy_objects: AtomicU64::new(0),
            total_corrupted_objects: AtomicU64::new(0),
            total_bytes_scanned: AtomicU64::new(0),
            total_scan_errors: AtomicU64::new(0),
            total_heal_triggered: AtomicU64::new(0),
        }
    }
}

/// scan result entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResultEntry {
    /// object path
    pub object_path: String,
    /// bucket name
    pub bucket_name: String,
    /// object size
    pub object_size: u64,
    /// is healthy
    pub is_healthy: bool,
    /// error message (if any)
    pub error_message: Option<String>,
    /// scan time
    pub scan_time: SystemTime,
    /// disk id
    pub disk_id: String,
}

/// batch scan result
#[derive(Debug, Clone)]
pub struct BatchScanResult {
    /// disk id
    pub disk_id: String,
    /// scan result entries
    pub entries: Vec<ScanResultEntry>,
    /// scan start time
    pub scan_start: SystemTime,
    /// scan end time
    pub scan_end: SystemTime,
    /// scan duration
    pub scan_duration: Duration,
}

impl LocalStatsManager {
    /// create new local stats manager
    pub fn new(node_id: &str, data_dir: &Path) -> Self {
        // ensure data directory exists
        if !data_dir.exists() {
            if let Err(e) = std::fs::create_dir_all(data_dir) {
                error!("create stats data directory failed {:?}: {}", data_dir, e);
            }
        }

        let stats_file = data_dir.join(format!("scanner_stats_{node_id}.json"));
        let backup_file = data_dir.join(format!("scanner_stats_{node_id}.backup"));
        let temp_file = data_dir.join(format!("scanner_stats_{node_id}.tmp"));

        Self {
            node_id: node_id.to_string(),
            stats_file,
            backup_file,
            temp_file,
            stats: Arc::new(RwLock::new(LocalScanStats::default())),
            save_interval: Duration::from_secs(60), // 60 seconds save once
            last_save: Arc::new(RwLock::new(SystemTime::UNIX_EPOCH)),
            counters: Arc::new(StatsCounters::default()),
        }
    }

    /// load local stats data
    pub async fn load_stats(&self) -> Result<()> {
        if !self.stats_file.exists() {
            info!("stats data file not exists, will create new stats data");
            return Ok(());
        }

        match self.load_stats_from_file(&self.stats_file).await {
            Ok(stats) => {
                *self.stats.write().await = stats;
                info!("success load local stats data");
                Ok(())
            }
            Err(e) => {
                warn!("load main stats file failed: {}, try backup file", e);

                match self.load_stats_from_file(&self.backup_file).await {
                    Ok(stats) => {
                        *self.stats.write().await = stats;
                        warn!("restore stats data from backup file");
                        Ok(())
                    }
                    Err(backup_e) => {
                        warn!("backup file also cannot load: {}, will use default stats data", backup_e);
                        Ok(())
                    }
                }
            }
        }
    }

    /// load stats data from file
    async fn load_stats_from_file(&self, file_path: &Path) -> Result<LocalScanStats> {
        let content = tokio::fs::read_to_string(file_path)
            .await
            .map_err(|e| Error::IO(format!("read stats file failed: {e}")))?;

        let stats: LocalScanStats =
            serde_json::from_str(&content).map_err(|e| Error::Serialization(format!("deserialize stats data failed: {e}")))?;

        Ok(stats)
    }

    /// save stats data to disk
    pub async fn save_stats(&self) -> Result<()> {
        let now = SystemTime::now();
        let last_save = *self.last_save.read().await;

        // frequency control
        if now.duration_since(last_save).unwrap_or(Duration::ZERO) < self.save_interval {
            return Ok(());
        }

        let stats = self.stats.read().await.clone();

        // serialize
        let json_data = serde_json::to_string_pretty(&stats)
            .map_err(|e| Error::Serialization(format!("serialize stats data failed: {e}")))?;

        // atomic write
        tokio::fs::write(&self.temp_file, json_data)
            .await
            .map_err(|e| Error::IO(format!("write temp stats file failed: {e}")))?;

        // backup existing file
        if self.stats_file.exists() {
            tokio::fs::copy(&self.stats_file, &self.backup_file)
                .await
                .map_err(|e| Error::IO(format!("backup stats file failed: {e}")))?;
        }

        // atomic replace
        tokio::fs::rename(&self.temp_file, &self.stats_file)
            .await
            .map_err(|e| Error::IO(format!("replace stats file failed: {e}")))?;

        *self.last_save.write().await = now;

        debug!("save local stats data to {:?}", self.stats_file);
        Ok(())
    }

    /// force save stats data
    pub async fn force_save_stats(&self) -> Result<()> {
        *self.last_save.write().await = SystemTime::UNIX_EPOCH;
        self.save_stats().await
    }

    /// update disk scan result
    pub async fn update_disk_scan_result(&self, result: &BatchScanResult) -> Result<()> {
        let mut stats = self.stats.write().await;

        // update disk stats
        let disk_stat = stats.disks_stats.entry(result.disk_id.clone()).or_insert_with(|| DiskStats {
            disk_id: result.disk_id.clone(),
            ..Default::default()
        });

        let healthy_count = result.entries.iter().filter(|e| e.is_healthy).count() as u64;
        let error_count = result.entries.iter().filter(|e| !e.is_healthy).count() as u64;

        disk_stat.objects_scanned += result.entries.len() as u64;
        disk_stat.errors_count += error_count;
        disk_stat.last_scan_time = result.scan_end;
        disk_stat.scan_duration = result.scan_duration;
        disk_stat.scan_completed = true;

        // update overall stats
        stats.objects_scanned += result.entries.len() as u64;
        stats.healthy_objects += healthy_count;
        stats.corrupted_objects += error_count;
        stats.last_update = SystemTime::now();

        // update bucket stats
        for entry in &result.entries {
            let _bucket_stat = stats
                .buckets_stats
                .entry(entry.bucket_name.clone())
                .or_insert_with(BucketStats::default);

            // TODO: update BucketStats
        }

        // update atomic counters
        self.counters
            .total_objects_scanned
            .fetch_add(result.entries.len() as u64, Ordering::Relaxed);
        self.counters
            .total_healthy_objects
            .fetch_add(healthy_count, Ordering::Relaxed);
        self.counters
            .total_corrupted_objects
            .fetch_add(error_count, Ordering::Relaxed);

        let total_bytes: u64 = result.entries.iter().map(|e| e.object_size).sum();
        self.counters.total_bytes_scanned.fetch_add(total_bytes, Ordering::Relaxed);

        if error_count > 0 {
            self.counters.total_scan_errors.fetch_add(error_count, Ordering::Relaxed);
        }

        drop(stats);

        debug!(
            "update disk {} scan result: objects {}, healthy {}, error {}",
            result.disk_id,
            result.entries.len(),
            healthy_count,
            error_count
        );

        Ok(())
    }

    /// record single object scan result
    pub async fn record_object_scan(&self, entry: ScanResultEntry) -> Result<()> {
        let result = BatchScanResult {
            disk_id: entry.disk_id.clone(),
            entries: vec![entry],
            scan_start: SystemTime::now(),
            scan_end: SystemTime::now(),
            scan_duration: Duration::from_millis(0),
        };

        self.update_disk_scan_result(&result).await
    }

    /// get local stats data copy
    pub async fn get_stats(&self) -> LocalScanStats {
        self.stats.read().await.clone()
    }

    /// get real-time counters
    pub fn get_counters(&self) -> Arc<StatsCounters> {
        self.counters.clone()
    }

    /// reset stats data
    pub async fn reset_stats(&self) -> Result<()> {
        {
            let mut stats = self.stats.write().await;
            *stats = LocalScanStats::default();
        }

        // reset counters
        self.counters.total_objects_scanned.store(0, Ordering::Relaxed);
        self.counters.total_healthy_objects.store(0, Ordering::Relaxed);
        self.counters.total_corrupted_objects.store(0, Ordering::Relaxed);
        self.counters.total_bytes_scanned.store(0, Ordering::Relaxed);
        self.counters.total_scan_errors.store(0, Ordering::Relaxed);
        self.counters.total_heal_triggered.store(0, Ordering::Relaxed);

        info!("reset local stats data");
        Ok(())
    }

    /// get stats summary
    pub async fn get_stats_summary(&self) -> StatsSummary {
        let stats = self.stats.read().await;

        StatsSummary {
            node_id: self.node_id.clone(),
            total_objects_scanned: self.counters.total_objects_scanned.load(Ordering::Relaxed),
            total_healthy_objects: self.counters.total_healthy_objects.load(Ordering::Relaxed),
            total_corrupted_objects: self.counters.total_corrupted_objects.load(Ordering::Relaxed),
            total_bytes_scanned: self.counters.total_bytes_scanned.load(Ordering::Relaxed),
            total_scan_errors: self.counters.total_scan_errors.load(Ordering::Relaxed),
            total_heal_triggered: self.counters.total_heal_triggered.load(Ordering::Relaxed),
            total_disks: stats.disks_stats.len(),
            total_buckets: stats.buckets_stats.len(),
            last_update: stats.last_update,
            scan_progress: stats.scan_progress.clone(),
            data_usage: stats.data_usage.clone(),
        }
    }

    /// record heal triggered
    pub async fn record_heal_triggered(&self, object_path: &str, error_message: &str) {
        self.counters.total_heal_triggered.fetch_add(1, Ordering::Relaxed);

        info!("record heal triggered: object={}, error={}", object_path, error_message);
    }

    /// update data usage stats
    pub async fn update_data_usage(&self, data_usage: DataUsageInfo) {
        let mut stats = self.stats.write().await;
        stats.data_usage = data_usage;
        stats.last_update = SystemTime::now();

        debug!("update data usage stats");
    }

    /// cleanup stats files
    pub async fn cleanup_stats_files(&self) -> Result<()> {
        // delete main file
        if self.stats_file.exists() {
            tokio::fs::remove_file(&self.stats_file)
                .await
                .map_err(|e| Error::IO(format!("delete stats file failed: {e}")))?;
        }

        // delete backup file
        if self.backup_file.exists() {
            tokio::fs::remove_file(&self.backup_file)
                .await
                .map_err(|e| Error::IO(format!("delete backup stats file failed: {e}")))?;
        }

        // delete temp file
        if self.temp_file.exists() {
            tokio::fs::remove_file(&self.temp_file)
                .await
                .map_err(|e| Error::IO(format!("delete temp stats file failed: {e}")))?;
        }

        info!("cleanup all stats files");
        Ok(())
    }

    /// set save interval
    pub fn set_save_interval(&mut self, interval: Duration) {
        self.save_interval = interval;
        info!("set stats data save interval to {:?}", interval);
    }
}

/// stats summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSummary {
    /// node id
    pub node_id: String,
    /// total scanned objects
    pub total_objects_scanned: u64,
    /// total healthy objects
    pub total_healthy_objects: u64,
    /// total corrupted objects
    pub total_corrupted_objects: u64,
    /// total scanned bytes
    pub total_bytes_scanned: u64,
    /// total scan errors
    pub total_scan_errors: u64,
    /// total heal triggered
    pub total_heal_triggered: u64,
    /// total disks
    pub total_disks: usize,
    /// total buckets
    pub total_buckets: usize,
    /// last update time
    pub last_update: SystemTime,
    /// scan progress
    pub scan_progress: super::node_scanner::ScanProgress,
    /// data usage snapshot for the node
    pub data_usage: DataUsageInfo,
}
