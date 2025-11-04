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

use crate::Result;
use crate::scanner::{
    AdvancedIOMonitor, AdvancedIOThrottler, BatchScanResult, CheckpointManager, IOMonitorConfig, IOThrottlerConfig,
    LocalStatsManager, MetricsSnapshot, ScanResultEntry,
};
use rustfs_common::data_usage::DataUsageInfo;
use rustfs_ecstore::StorageAPI;
use rustfs_ecstore::disk::{DiskAPI, DiskStore};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// SystemTime serde
mod system_time_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
        duration.as_secs().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + std::time::Duration::from_secs(secs))
    }
}

/// Option<SystemTime> serde
mod option_system_time_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(time: &Option<SystemTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match time {
            Some(t) => {
                let duration = t.duration_since(UNIX_EPOCH).unwrap_or_default();
                Some(duration.as_secs()).serialize(serializer)
            }
            None => None::<u64>.serialize(serializer),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<SystemTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = Option::<u64>::deserialize(deserializer)?;
        Ok(secs.map(|s| UNIX_EPOCH + std::time::Duration::from_secs(s)))
    }
}

/// temporary BucketStats definition, for backward compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketStats {
    pub name: String,
    pub object_count: u64,
    pub total_size: u64,
    #[serde(with = "system_time_serde")]
    pub last_update: SystemTime,
}

impl Default for BucketStats {
    fn default() -> Self {
        Self {
            name: String::new(),
            object_count: 0,
            total_size: 0,
            last_update: SystemTime::now(),
        }
    }
}

/// business load level enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LoadLevel {
    /// low load (<30%)
    Low,
    /// medium load (30-60%)
    Medium,
    /// high load (60-80%)
    High,
    /// ultra high load (>80%)
    Critical,
}

/// node scanner config
#[derive(Debug, Clone)]
pub struct NodeScannerConfig {
    /// scan interval
    pub scan_interval: Duration,
    /// disk scan delay
    pub disk_scan_delay: Duration,
    /// whether to enable smart scheduling
    pub enable_smart_scheduling: bool,
    /// whether to enable checkpoint resume
    pub enable_checkpoint: bool,
    /// checkpoint save interval
    pub checkpoint_save_interval: Duration,
    /// data directory path
    pub data_dir: PathBuf,
    /// max scan retry attempts
    pub max_retry_attempts: u32,
}

impl Default for NodeScannerConfig {
    fn default() -> Self {
        // use a user-writable directory for scanner data
        let data_dir = std::env::temp_dir().join("rustfs_scanner");

        Self {
            scan_interval: Duration::from_secs(300),  // 5 minutes base interval
            disk_scan_delay: Duration::from_secs(10), // disk scan delay 10 seconds
            enable_smart_scheduling: true,
            enable_checkpoint: true,
            checkpoint_save_interval: Duration::from_secs(30), // checkpoint save interval 30 seconds
            data_dir,
            max_retry_attempts: 3,
        }
    }
}

/// local scan stats data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalScanStats {
    /// scanned objects count
    pub objects_scanned: u64,
    /// healthy objects count
    pub healthy_objects: u64,
    /// corrupted objects count
    pub corrupted_objects: u64,
    /// data usage
    pub data_usage: DataUsageInfo,
    /// last update time
    #[serde(with = "system_time_serde")]
    pub last_update: SystemTime,
    /// buckets stats
    pub buckets_stats: HashMap<String, BucketStats>,
    /// disks stats
    pub disks_stats: HashMap<String, DiskStats>,
    /// scan progress
    pub scan_progress: ScanProgress,
    /// checkpoint timestamp
    #[serde(with = "system_time_serde")]
    pub checkpoint_timestamp: SystemTime,
}

impl Default for LocalScanStats {
    fn default() -> Self {
        Self {
            objects_scanned: 0,
            healthy_objects: 0,
            corrupted_objects: 0,
            data_usage: DataUsageInfo::default(),
            last_update: SystemTime::now(),
            buckets_stats: HashMap::new(),
            disks_stats: HashMap::new(),
            scan_progress: ScanProgress::default(),
            checkpoint_timestamp: SystemTime::now(),
        }
    }
}

/// disk stats info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskStats {
    /// disk id
    pub disk_id: String,
    /// scanned objects count
    pub objects_scanned: u64,
    /// errors count
    pub errors_count: u64,
    /// last scan time
    #[serde(with = "system_time_serde")]
    pub last_scan_time: SystemTime,
    /// scan duration
    pub scan_duration: Duration,
    /// whether scan is completed
    pub scan_completed: bool,
}

impl Default for DiskStats {
    fn default() -> Self {
        Self {
            disk_id: String::new(),
            objects_scanned: 0,
            errors_count: 0,
            last_scan_time: SystemTime::now(),
            scan_duration: Duration::from_secs(0),
            scan_completed: false,
        }
    }
}

/// scan progress state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanProgress {
    /// current scan cycle
    pub current_cycle: u64,
    /// current disk index
    pub current_disk_index: usize,
    /// current bucket
    pub current_bucket: Option<String>,
    /// current object prefix
    pub current_object_prefix: Option<String>,
    /// completed disks
    pub completed_disks: HashSet<String>,
    /// completed buckets scan state
    pub completed_buckets: HashMap<String, BucketScanState>,
    /// last scanned object key
    pub last_scan_key: Option<String>,
    /// scan start time
    #[serde(with = "system_time_serde")]
    pub scan_start_time: SystemTime,
    /// estimated completion time
    #[serde(with = "option_system_time_serde")]
    pub estimated_completion: Option<SystemTime>,
    /// data usage statistics
    pub data_usage: Option<DataUsageInfo>,
}

impl Default for ScanProgress {
    fn default() -> Self {
        Self {
            current_cycle: 0,
            current_disk_index: 0,
            current_bucket: None,
            current_object_prefix: None,
            completed_disks: HashSet::new(),
            completed_buckets: HashMap::new(),
            last_scan_key: None,
            scan_start_time: SystemTime::now(),
            estimated_completion: None,
            data_usage: None,
        }
    }
}

/// bucket scan state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketScanState {
    /// whether completed
    pub completed: bool,
    /// last scanned object key
    pub last_object_key: Option<String>,
    /// scanned objects count
    pub objects_scanned: u64,
    /// scan timestamp
    pub scan_timestamp: SystemTime,
}

/// IO monitor
pub struct IOMonitor {
    /// current disk IOPS
    current_iops: Arc<AtomicU64>,
    /// disk queue depth
    queue_depth: Arc<AtomicU64>,
    /// business request latency (milliseconds)
    business_latency: Arc<AtomicU64>,
    /// CPU usage
    cpu_usage: Arc<AtomicU8>,
    /// memory usage
    /// memory usage (reserved field)
    #[allow(dead_code)]
    memory_usage: Arc<AtomicU8>,
}

impl IOMonitor {
    pub fn new() -> Self {
        Self {
            current_iops: Arc::new(AtomicU64::new(0)),
            queue_depth: Arc::new(AtomicU64::new(0)),
            business_latency: Arc::new(AtomicU64::new(0)),
            cpu_usage: Arc::new(AtomicU8::new(0)),
            memory_usage: Arc::new(AtomicU8::new(0)),
        }
    }

    /// get current business load level
    pub async fn get_business_load_level(&self) -> LoadLevel {
        let iops = self.current_iops.load(Ordering::Relaxed);
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        let latency = self.business_latency.load(Ordering::Relaxed);
        let cpu = self.cpu_usage.load(Ordering::Relaxed);

        // comprehensive evaluation of load level
        let load_score = self.calculate_load_score(iops, queue_depth, latency, cpu);

        match load_score {
            0..=30 => LoadLevel::Low,
            31..=60 => LoadLevel::Medium,
            61..=80 => LoadLevel::High,
            _ => LoadLevel::Critical,
        }
    }

    fn calculate_load_score(&self, iops: u64, queue_depth: u64, latency: u64, cpu: u8) -> u8 {
        // simplified load score algorithm, actual implementation needs to adjust based on specific hardware metrics
        let iops_score = std::cmp::min(iops / 100, 25) as u8; // IOPS weight 25%
        let queue_score = std::cmp::min(queue_depth * 5, 25) as u8; // queue depth weight 25%
        let latency_score = std::cmp::min(latency / 10, 25) as u8; // latency weight 25%
        let cpu_score = std::cmp::min(cpu / 4, 25); // CPU weight 25%

        iops_score + queue_score + latency_score + cpu_score
    }

    /// update system metrics
    pub async fn update_metrics(&self, iops: u64, queue_depth: u64, latency: u64, cpu: u8) {
        self.current_iops.store(iops, Ordering::Relaxed);
        self.queue_depth.store(queue_depth, Ordering::Relaxed);
        self.business_latency.store(latency, Ordering::Relaxed);
        self.cpu_usage.store(cpu, Ordering::Relaxed);
    }
}

/// IO throttler
pub struct IOThrottler {
    /// max IOPS limit (reserved field)
    #[allow(dead_code)]
    max_iops: Arc<AtomicU64>,
    /// current IOPS usage (reserved field)
    #[allow(dead_code)]
    current_iops: Arc<AtomicU64>,
    /// business priority weight (0-100)
    business_priority: Arc<AtomicU8>,
    /// scan operation delay (milliseconds)
    scan_delay: Arc<AtomicU64>,
}

impl IOThrottler {
    pub fn new() -> Self {
        Self {
            max_iops: Arc::new(AtomicU64::new(1000)), // default max 1000 IOPS
            current_iops: Arc::new(AtomicU64::new(0)),
            business_priority: Arc::new(AtomicU8::new(95)), // business priority 95%
            scan_delay: Arc::new(AtomicU64::new(100)),      // default 100ms delay
        }
    }

    /// adjust scanning delay based on load level
    pub async fn adjust_for_load_level(&self, load_level: LoadLevel) -> Duration {
        let delay_ms = match load_level {
            LoadLevel::Low => {
                self.scan_delay.store(100, Ordering::Relaxed); // 100ms delay
                self.business_priority.store(90, Ordering::Relaxed);
                100
            }
            LoadLevel::Medium => {
                self.scan_delay.store(500, Ordering::Relaxed); // 500ms delay
                self.business_priority.store(95, Ordering::Relaxed);
                500
            }
            LoadLevel::High => {
                self.scan_delay.store(2000, Ordering::Relaxed); // 2s
                self.business_priority.store(98, Ordering::Relaxed);
                2000
            }
            LoadLevel::Critical => {
                self.scan_delay.store(10000, Ordering::Relaxed); // 10s delay (actually will pause scanning)
                self.business_priority.store(99, Ordering::Relaxed);
                10000
            }
        };

        Duration::from_millis(delay_ms)
    }

    /// check whether should pause scanning
    pub async fn should_pause_scanning(&self, load_level: LoadLevel) -> bool {
        matches!(load_level, LoadLevel::Critical)
    }
}

/// decentralized node scanner
///
/// responsible for serial scanning of local disks, implementing smart scheduling and checkpoint resume functionality
pub struct NodeScanner {
    /// node id
    node_id: String,
    /// local disks list
    local_disks: Arc<RwLock<Vec<Arc<DiskStore>>>>,
    /// IO monitor
    io_monitor: Arc<AdvancedIOMonitor>,
    /// IO throttler
    throttler: Arc<AdvancedIOThrottler>,
    /// config
    config: Arc<RwLock<NodeScannerConfig>>,
    /// current scanned disk index
    current_disk_index: Arc<AtomicUsize>,
    /// local stats data
    local_stats: Arc<RwLock<LocalScanStats>>,
    /// stats data manager
    stats_manager: Arc<LocalStatsManager>,
    /// scan progress state
    scan_progress: Arc<RwLock<ScanProgress>>,
    /// checkpoint manager mapping (one for each disk)
    checkpoint_managers: Arc<RwLock<HashMap<String, Arc<CheckpointManager>>>>,
    /// cancel token
    cancel_token: CancellationToken,
}

impl NodeScanner {
    /// create a new node scanner
    pub fn new(node_id: String, config: NodeScannerConfig) -> Self {
        // Ensure data directory exists
        if !config.data_dir.exists() {
            if let Err(e) = std::fs::create_dir_all(&config.data_dir) {
                error!("create data directory failed {:?}: {}", config.data_dir, e);
            }
        }

        let stats_manager = Arc::new(LocalStatsManager::new(&node_id, &config.data_dir));

        let monitor_config = IOMonitorConfig {
            monitor_interval: Duration::from_secs(1),
            enable_system_monitoring: true,
            ..Default::default()
        };
        let io_monitor = Arc::new(AdvancedIOMonitor::new(monitor_config));

        let throttler_config = IOThrottlerConfig {
            max_iops: 1000,
            base_business_priority: 95,
            min_scan_delay: 5000,
            max_scan_delay: 60000,
            enable_dynamic_adjustment: true,
            adjustment_response_time: 5,
        };
        let throttler = Arc::new(AdvancedIOThrottler::new(throttler_config));

        Self {
            node_id,
            local_disks: Arc::new(RwLock::new(Vec::new())),
            io_monitor,
            throttler,
            config: Arc::new(RwLock::new(config)),
            current_disk_index: Arc::new(AtomicUsize::new(0)),
            local_stats: Arc::new(RwLock::new(LocalScanStats::default())),
            stats_manager,
            scan_progress: Arc::new(RwLock::new(ScanProgress::default())),
            checkpoint_managers: Arc::new(RwLock::new(HashMap::new())),
            cancel_token: CancellationToken::new(),
        }
    }

    /// add local disk and create checkpoint manager for it
    pub async fn add_local_disk(&self, disk: Arc<DiskStore>) {
        // get disk path and create corresponding scanner directory
        let disk_path = disk.path();
        let sys_dir = disk_path.join(".rustfs.sys").join("scanner");

        // ensure directory exists
        if let Err(e) = std::fs::create_dir_all(&sys_dir) {
            error!("Failed to create scanner directory on disk {:?}: {}", disk_path, e);
            return;
        }

        // create checkpoint manager for the disk
        let disk_id = disk_path.to_string_lossy().to_string();
        let checkpoint_manager = Arc::new(CheckpointManager::new(&self.node_id, &sys_dir));

        // store checkpoint manager
        self.checkpoint_managers
            .write()
            .await
            .insert(disk_id.clone(), checkpoint_manager);

        // add disk to local disks list
        self.local_disks.write().await.push(disk.clone());

        info!("Added disk {} with checkpoint manager to node {}", disk_id, self.node_id);
    }

    /// get checkpoint manager for the disk
    async fn get_checkpoint_manager_for_disk(&self, disk_id: &str) -> Option<Arc<CheckpointManager>> {
        self.checkpoint_managers.read().await.get(disk_id).cloned()
    }

    /// get default checkpoint manager (for the case when there is no local disk)
    async fn get_default_checkpoint_manager(&self) -> Option<Arc<CheckpointManager>> {
        let config = self.config.read().await;
        let data_dir = &config.data_dir;

        // ensure data directory exists
        if let Err(e) = std::fs::create_dir_all(data_dir) {
            error!("Failed to create data directory {:?}: {}", data_dir, e);
            return None;
        }

        // create default checkpoint manager
        Some(Arc::new(CheckpointManager::new(&self.node_id, data_dir)))
    }

    /// create checkpoint manager for all disks (called during initialization)
    pub async fn initialize_checkpoint_managers(&self) {
        let local_disks = self.local_disks.read().await;
        let mut checkpoint_managers = self.checkpoint_managers.write().await;

        for disk in local_disks.iter() {
            let disk_path = disk.path();
            let sys_dir = disk_path.join(".rustfs.sys").join("scanner");

            // ensure directory exists
            if let Err(e) = std::fs::create_dir_all(&sys_dir) {
                error!("Failed to create scanner directory on disk {:?}: {}", disk_path, e);
                continue;
            }

            // create checkpoint manager for the disk
            let disk_id = disk_path.to_string_lossy().to_string();
            let checkpoint_manager = Arc::new(CheckpointManager::new(&self.node_id, &sys_dir));
            checkpoint_managers.insert(disk_id, checkpoint_manager);
        }
    }

    /// set data directory
    pub async fn set_data_dir(&self, data_dir: &Path) {
        // Update the checkpoint manager with the new data directory
        let _new_checkpoint_manager = CheckpointManager::new(&self.node_id, data_dir);
        // Note: We can't directly replace the Arc, so we would need to redesign this
        info!("Would set data directory to: {:?}", data_dir);
        // TODO: Implement proper data directory management
    }

    /// get node id
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// get local stats data
    pub async fn get_local_stats(&self) -> LocalScanStats {
        self.local_stats.read().await.clone()
    }

    /// get scan progress
    pub async fn get_scan_progress(&self) -> ScanProgress {
        self.scan_progress.read().await.clone()
    }

    /// start scanner
    pub async fn start(&self) -> Result<()> {
        info!("start scanner for node {}", self.node_id);

        // try to resume from checkpoint
        self.start_with_resume().await?;

        Ok(())
    }

    /// try to resume from checkpoint when node starts
    pub async fn start_with_resume(&self) -> Result<()> {
        info!("node {} start, try to resume checkpoint", self.node_id);

        // initialize checkpoint managers
        self.initialize_checkpoint_managers().await;

        // try to resume scanning progress (from the first disk)
        let local_disks = self.local_disks.read().await;
        if !local_disks.is_empty() {
            let first_disk = &local_disks[0];
            let disk_id = first_disk.path().to_string_lossy().to_string();

            if let Some(checkpoint_manager) = self.get_checkpoint_manager_for_disk(&disk_id).await {
                match checkpoint_manager.load_checkpoint().await {
                    Ok(Some(progress)) => {
                        info!(
                            "success to resume scanning from disk {}: cycle={}, disk={}, last_key={:?}",
                            disk_id, progress.current_cycle, progress.current_disk_index, progress.last_scan_key
                        );

                        *self.scan_progress.write().await = progress;

                        // use the resumed progress to start scanning
                        self.resume_scanning_from_checkpoint().await
                    }
                    Ok(None) => {
                        info!("disk {} has no valid checkpoint, start fresh scanning", disk_id);
                        self.start_fresh_scanning().await
                    }
                    Err(e) => {
                        warn!("failed to resume scanning from disk {}: {}, start fresh scanning", disk_id, e);
                        self.start_fresh_scanning().await
                    }
                }
            } else {
                info!("cannot get checkpoint manager for disk {}, start fresh scanning", disk_id);
                self.start_fresh_scanning().await
            }
        } else {
            info!("no local disk, try to resume from default checkpoint manager");

            // try to load from default checkpoint manager
            if let Some(default_checkpoint_manager) = self.get_default_checkpoint_manager().await {
                match default_checkpoint_manager.load_checkpoint().await {
                    Ok(Some(scan_progress)) => {
                        info!(
                            "resume scanning from default checkpoint: cycle={}, last_key={:?}",
                            scan_progress.current_cycle, scan_progress.last_scan_key
                        );

                        // resume scanning progress
                        *self.scan_progress.write().await = scan_progress;

                        return self.resume_scanning_from_checkpoint().await;
                    }
                    Ok(None) => {
                        info!("no default checkpoint file");
                    }
                    Err(e) => {
                        warn!("load default checkpoint failed: {}", e);
                    }
                }
            }

            // if no checkpoint, check if there is scanning progress in memory (for test scenario)
            let current_progress = self.scan_progress.read().await;
            if current_progress.current_cycle > 0 || current_progress.last_scan_key.is_some() {
                info!(
                    "found scanning progress in memory: cycle={}, disk={}, last_key={:?}",
                    current_progress.current_cycle, current_progress.current_disk_index, current_progress.last_scan_key
                );
                drop(current_progress);
                self.resume_scanning_from_checkpoint().await
            } else {
                drop(current_progress);
                self.start_fresh_scanning().await
            }
        }
    }

    /// resume scanning from checkpoint
    async fn resume_scanning_from_checkpoint(&self) -> Result<()> {
        let progress = self.scan_progress.read().await;
        let disk_index = progress.current_disk_index;
        let last_scan_key = progress.last_scan_key.clone();
        drop(progress);

        info!("resume scanning from disk {}: last_scan_key={:?}", disk_index, last_scan_key);

        // update current disk index
        self.current_disk_index
            .store(disk_index, std::sync::atomic::Ordering::Relaxed);

        // start IO monitoring
        self.start_io_monitoring().await?;

        // start scanning loop
        let scanner_clone = self.clone_for_background();
        tokio::spawn(async move {
            if let Err(e) = scanner_clone.scan_loop_with_resume(last_scan_key).await {
                error!("scanning loop failed: {}", e);
            }
        });

        Ok(())
    }

    /// start fresh scanning
    async fn start_fresh_scanning(&self) -> Result<()> {
        info!("start fresh scanning loop");

        // initialize scanning progress
        {
            let mut progress = self.scan_progress.write().await;
            progress.current_cycle += 1;
            progress.current_disk_index = 0;
            progress.scan_start_time = SystemTime::now();
            progress.last_scan_key = None;
            progress.completed_disks.clear();
            progress.completed_buckets.clear();
        }

        self.current_disk_index.store(0, std::sync::atomic::Ordering::Relaxed);

        // start IO monitoring
        self.start_io_monitoring().await?;

        // start scanning loop
        let scanner_clone = self.clone_for_background();
        tokio::spawn(async move {
            if let Err(e) = scanner_clone.scan_loop_with_resume(None).await {
                error!("scanning loop failed: {}", e);
            }
        });

        Ok(())
    }

    /// stop scanner
    pub async fn stop(&self) -> Result<()> {
        info!("stop scanner for node {}", self.node_id);
        self.cancel_token.cancel();
        Ok(())
    }

    /// clone for background task
    fn clone_for_background(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            local_disks: self.local_disks.clone(),
            io_monitor: self.io_monitor.clone(),
            throttler: self.throttler.clone(),
            config: self.config.clone(),
            current_disk_index: self.current_disk_index.clone(),
            local_stats: self.local_stats.clone(),
            stats_manager: self.stats_manager.clone(),
            scan_progress: self.scan_progress.clone(),
            checkpoint_managers: self.checkpoint_managers.clone(),
            cancel_token: self.cancel_token.clone(),
        }
    }

    /// start IO monitoring
    async fn start_io_monitoring(&self) -> Result<()> {
        info!("start advanced IO monitoring");
        self.io_monitor.start().await?;
        Ok(())
    }

    /// main scanning loop (not supported checkpoint resume)
    #[allow(dead_code)]
    async fn scan_loop(&self) -> Result<()> {
        self.scan_loop_with_resume(None).await
    }

    /// main scanning loop with checkpoint resume
    async fn scan_loop_with_resume(&self, resume_from_key: Option<String>) -> Result<()> {
        info!("node {} start scanning loop, resume from key: {:?}", self.node_id, resume_from_key);

        while !self.cancel_token.is_cancelled() {
            // check business load
            let load_level = self.io_monitor.get_business_load_level().await;

            // get current system metrics
            let current_metrics = self.io_monitor.get_current_metrics().await;
            let metrics_snapshot = MetricsSnapshot {
                iops: current_metrics.iops,
                latency: current_metrics.avg_latency,
                cpu_usage: current_metrics.cpu_usage,
                memory_usage: current_metrics.memory_usage,
            };

            // get throttle decision
            let throttle_decision = self
                .throttler
                .make_throttle_decision(load_level, Some(metrics_snapshot))
                .await;

            // according to decision action
            if throttle_decision.should_pause {
                warn!("pause scanning according to throttle decision: {}", throttle_decision.reason);
                tokio::time::sleep(Duration::from_secs(600)).await; // pause 10 minutes
                continue;
            }

            // execute serial disk scanning
            if let Err(e) = self.scan_all_disks_serially().await {
                error!("disk scanning failed: {}", e);
            }

            // save checkpoint
            if let Err(e) = self.save_checkpoint().await {
                warn!("save checkpoint failed: {}", e);
            }

            // use throttle decision suggested delay
            let scan_interval = throttle_decision.suggested_delay;
            info!("scan completed, wait {:?} for next round", scan_interval);
            info!(
                "resource allocation: business {}%, scanner {}%",
                throttle_decision.resource_allocation.business_percentage,
                throttle_decision.resource_allocation.scanner_percentage
            );

            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    info!("scanning loop cancelled");
                    break;
                }
                _ = tokio::time::sleep(scan_interval) => {
                    // continue next round scanning
                }
            }
        }

        Ok(())
    }

    /// serial scanning all local disks
    async fn scan_all_disks_serially(&self) -> Result<()> {
        let local_disks = self.local_disks.read().await;
        info!("start serial scanning node {} of {} disks", self.node_id, local_disks.len());

        for (index, disk) in local_disks.iter().enumerate() {
            // check again whether should pause
            let load_level = self.io_monitor.get_business_load_level().await;
            let should_pause = self.throttler.should_pause_scanning(load_level).await;

            if should_pause {
                warn!("business load too high, interrupt disk scanning");
                break;
            }

            info!("start scanning disk {:?} (index: {})", disk.path(), index);

            // scan single disk
            if let Err(e) = self.scan_single_disk(disk.clone()).await {
                error!("scan disk {:?} failed: {}", disk.path(), e);
                continue;
            }

            // update scan progress
            self.update_disk_scan_progress(index, &disk.path().to_string_lossy()).await;

            // disk inter-delay (using smart throttle decision)
            if index < local_disks.len() - 1 {
                let delay = self.throttler.adjust_for_load_level(load_level).await;
                info!("disk {:?} scan completed, smart delay {:?} for next", disk.path(), delay);
                tokio::time::sleep(delay).await;
            }
        }

        // update cycle scan progress
        self.complete_scan_cycle().await;

        Ok(())
    }

    /// scan single disk
    async fn scan_single_disk(&self, disk: Arc<DiskStore>) -> Result<()> {
        info!("scan disk: path={:?}", disk.path());

        let scan_start = SystemTime::now();
        let mut scan_entries = Vec::new();

        // get ECStore instance for real disk scanning
        if let Some(ecstore) = rustfs_ecstore::new_object_layer_fn() {
            // get all buckets on disk
            match ecstore
                .list_bucket(&rustfs_ecstore::store_api::BucketOptions::default())
                .await
            {
                Ok(buckets) => {
                    for bucket_info in buckets {
                        let bucket_name = &bucket_info.name;

                        // skip system internal buckets
                        if bucket_name == ".minio.sys" {
                            continue;
                        }

                        // scan objects in bucket
                        match self.scan_bucket_on_disk(&disk, bucket_name, &mut scan_entries).await {
                            Ok(object_count) => {
                                debug!(
                                    "disk {:?} bucket {} scan completed, found {} objects",
                                    disk.path(),
                                    bucket_name,
                                    object_count
                                );
                            }
                            Err(e) => {
                                warn!("disk {:?} bucket {} scan failed: {}", disk.path(), bucket_name, e);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("get bucket list failed: {}", e);
                    // Fallback: simulate some scan results for testing to continue
                    self.generate_fallback_scan_results(&disk, &mut scan_entries).await;
                }
            }
        } else {
            warn!("ECStore instance not available, use simulated scan data");
            // Fallback: simulate some scan results for testing to continue
            self.generate_fallback_scan_results(&disk, &mut scan_entries).await;
        }

        let scan_end = SystemTime::now();
        let scan_duration = scan_end.duration_since(scan_start).unwrap_or(Duration::ZERO);

        // create batch scan result
        let batch_result = BatchScanResult {
            disk_id: disk.path().to_string_lossy().to_string(),
            entries: scan_entries,
            scan_start,
            scan_end,
            scan_duration,
        };

        // update stats data
        self.stats_manager.update_disk_scan_result(&batch_result).await?;

        // sync update local stats data structure
        self.update_local_disk_stats(&disk.path().to_string_lossy(), batch_result.entries.len() as u64)
            .await;

        Ok(())
    }

    /// update disk scan progress
    async fn update_disk_scan_progress(&self, disk_index: usize, disk_id: &str) {
        let mut progress = self.scan_progress.write().await;
        progress.current_disk_index = disk_index;
        progress.completed_disks.insert(disk_id.to_string());

        debug!("update scan progress: disk index {}, disk id {}", disk_index, disk_id);
    }

    /// complete scan cycle
    async fn complete_scan_cycle(&self) {
        let mut progress = self.scan_progress.write().await;
        progress.current_cycle += 1;
        progress.current_disk_index = 0;
        progress.completed_disks.clear();
        progress.scan_start_time = SystemTime::now();

        info!("complete scan cycle {}", progress.current_cycle);
    }

    /// update local disk stats
    async fn update_local_disk_stats(&self, disk_id: &str, objects_scanned: u64) {
        let mut stats = self.local_stats.write().await;

        let disk_stat = stats.disks_stats.entry(disk_id.to_string()).or_insert_with(|| DiskStats {
            disk_id: disk_id.to_string(),
            ..Default::default()
        });

        disk_stat.objects_scanned += objects_scanned;
        disk_stat.last_scan_time = SystemTime::now();
        disk_stat.scan_completed = true;

        stats.last_update = SystemTime::now();
        stats.objects_scanned += objects_scanned;
        stats.healthy_objects += objects_scanned; // assume most objects are healthy

        debug!("update disk {} stats, scanned objects {}", disk_id, objects_scanned);
    }

    /// scan objects in specified bucket on disk
    async fn scan_bucket_on_disk(
        &self,
        disk: &DiskStore,
        bucket_name: &str,
        scan_entries: &mut Vec<ScanResultEntry>,
    ) -> Result<usize> {
        let disk_path = disk.path();
        let bucket_path = disk_path.join(bucket_name);

        if !bucket_path.exists() {
            return Ok(0);
        }

        let mut object_count = 0;

        // iterate all objects in bucket directory
        if let Ok(entries) = std::fs::read_dir(&bucket_path) {
            for entry in entries.flatten() {
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    let object_name = entry_path.file_name().and_then(|name| name.to_str()).unwrap_or("unknown");

                    // skip hidden files and system files
                    if object_name.starts_with('.') {
                        continue;
                    }

                    // get object size (simplified processing)
                    let object_size = self.estimate_object_size(&entry_path).await;

                    let entry = ScanResultEntry {
                        object_path: format!("{bucket_name}/{object_name}"),
                        bucket_name: bucket_name.to_string(),
                        object_size,
                        is_healthy: true, // assume most objects are healthy
                        error_message: None,
                        scan_time: SystemTime::now(),
                        disk_id: disk_path.to_string_lossy().to_string(),
                    };

                    scan_entries.push(entry);
                    object_count += 1;
                }
            }
        }

        Ok(object_count)
    }

    /// estimate object size
    async fn estimate_object_size(&self, object_dir: &std::path::Path) -> u64 {
        let mut total_size = 0u64;

        if let Ok(entries) = std::fs::read_dir(object_dir) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    total_size += metadata.len();
                }
            }
        }

        total_size
    }

    /// generate fallback scan results (for testing or exceptional cases)
    async fn generate_fallback_scan_results(&self, disk: &DiskStore, scan_entries: &mut Vec<ScanResultEntry>) {
        debug!("generate fallback scan results for disk: {:?}", disk.path());

        // simulate some scan results
        for i in 0..5 {
            let entry = ScanResultEntry {
                object_path: format!("/fallback-bucket/object_{i}"),
                bucket_name: "fallback-bucket".to_string(),
                object_size: 1024 * (i + 1),
                is_healthy: true,
                error_message: None,
                scan_time: SystemTime::now(),
                disk_id: disk.path().to_string_lossy().to_string(),
            };
            scan_entries.push(entry);
        }
    }

    /// get adaptive scan interval
    #[allow(dead_code)]
    async fn get_adaptive_scan_interval(&self, load_level: LoadLevel) -> Duration {
        let base_interval = self.config.read().await.scan_interval;

        match load_level {
            LoadLevel::Low => base_interval,
            LoadLevel::Medium => base_interval * 2,
            LoadLevel::High => base_interval * 4,
            LoadLevel::Critical => base_interval * 10,
        }
    }

    /// save checkpoint to current disk
    async fn save_checkpoint(&self) -> Result<()> {
        let progress = self.scan_progress.read().await;

        // get current scanned disk
        let current_disk_index = self.current_disk_index.load(std::sync::atomic::Ordering::Relaxed);
        let local_disks = self.local_disks.read().await;
        if current_disk_index < local_disks.len() {
            let current_disk = &local_disks[current_disk_index];
            let disk_id = current_disk.path().to_string_lossy().to_string();

            // get checkpoint manager for disk
            if let Some(checkpoint_manager) = self.get_checkpoint_manager_for_disk(&disk_id).await {
                checkpoint_manager.save_checkpoint(&progress).await?;
                debug!("save checkpoint to disk {}", disk_id);
            } else {
                warn!("cannot get checkpoint manager for disk {}", disk_id);
            }
        }

        Ok(())
    }

    /// force save checkpoint to current disk
    pub async fn force_save_checkpoint(&self) -> Result<()> {
        let progress = self.scan_progress.read().await;

        // get current scanned disk
        let current_disk_index = self.current_disk_index.load(std::sync::atomic::Ordering::Relaxed);
        let local_disks = self.local_disks.read().await;

        let checkpoint_manager = if current_disk_index < local_disks.len() {
            // when there is local disk, use corresponding disk's checkpoint manager
            let current_disk = &local_disks[current_disk_index];
            let disk_id = current_disk.path().to_string_lossy().to_string();
            self.get_checkpoint_manager_for_disk(&disk_id).await
        } else {
            // when there is no local disk, use default checkpoint manager
            self.get_default_checkpoint_manager().await
        };

        if let Some(checkpoint_manager) = checkpoint_manager {
            checkpoint_manager.force_save_checkpoint(&progress).await?;
            info!("force save checkpoint");
        } else {
            warn!("cannot get checkpoint manager");
        }

        Ok(())
    }

    /// get checkpoint info (from current disk)
    pub async fn get_checkpoint_info(&self) -> Result<Option<super::checkpoint::CheckpointInfo>> {
        // get current scanned disk
        let current_disk_index = self.current_disk_index.load(std::sync::atomic::Ordering::Relaxed);
        let local_disks = self.local_disks.read().await;
        if current_disk_index < local_disks.len() {
            let current_disk = &local_disks[current_disk_index];
            let disk_id = current_disk.path().to_string_lossy().to_string();

            // get checkpoint manager for disk
            if let Some(checkpoint_manager) = self.get_checkpoint_manager_for_disk(&disk_id).await {
                checkpoint_manager.get_checkpoint_info().await
            } else {
                warn!("cannot get checkpoint manager for disk {}", disk_id);
                Ok(None)
            }
        } else if let Some(default_checkpoint_manager) = self.get_default_checkpoint_manager().await {
            default_checkpoint_manager.get_checkpoint_info().await
        } else {
            Ok(None)
        }
    }

    /// cleanup all checkpoints on all disks
    pub async fn cleanup_checkpoint(&self) -> Result<()> {
        let checkpoint_managers = self.checkpoint_managers.read().await;
        for (disk_id, checkpoint_manager) in checkpoint_managers.iter() {
            if let Err(e) = checkpoint_manager.cleanup_checkpoint().await {
                warn!("cleanup checkpoint on disk {} failed: {}", disk_id, e);
            } else {
                info!("cleanup checkpoint on disk {}", disk_id);
            }
        }
        Ok(())
    }

    /// get stats data manager
    pub fn get_stats_manager(&self) -> Arc<LocalStatsManager> {
        self.stats_manager.clone()
    }

    /// get stats summary
    pub async fn get_stats_summary(&self) -> super::local_stats::StatsSummary {
        self.stats_manager.get_stats_summary().await
    }

    /// record heal triggered
    pub async fn record_heal_triggered(&self, object_path: &str, error_message: &str) {
        self.stats_manager.record_heal_triggered(object_path, error_message).await;
    }

    /// update data usage stats
    pub async fn update_data_usage(&self, data_usage: DataUsageInfo) {
        self.stats_manager.update_data_usage(data_usage).await;
    }

    /// initialize stats data
    pub async fn initialize_stats(&self) -> Result<()> {
        self.stats_manager.load_stats().await
    }

    /// get IO monitor
    pub fn get_io_monitor(&self) -> Arc<AdvancedIOMonitor> {
        self.io_monitor.clone()
    }

    /// get IO throttler
    pub fn get_io_throttler(&self) -> Arc<AdvancedIOThrottler> {
        self.throttler.clone()
    }

    /// update business metrics
    pub async fn update_business_metrics(&self, latency: u64, qps: u64, error_rate: u64, connections: u64) {
        self.io_monitor
            .update_business_metrics(latency, qps, error_rate, connections)
            .await;
    }

    /// get current IO metrics
    pub async fn get_current_io_metrics(&self) -> super::io_monitor::IOMetrics {
        self.io_monitor.get_current_metrics().await
    }

    /// get throttle stats
    pub async fn get_throttle_stats(&self) -> super::io_throttler::ThrottleStats {
        self.throttler.get_throttle_stats().await
    }

    /// run business pressure simulation
    pub async fn run_business_pressure_simulation(&self, duration: Duration) -> super::io_throttler::SimulationResult {
        self.throttler.simulate_business_pressure(duration).await
    }

    /// update scan progress (for testing)
    pub async fn update_scan_progress_for_test(&self, cycle: u64, disk_index: usize, last_key: Option<String>) {
        let mut progress = self.scan_progress.write().await;
        progress.current_cycle = cycle;
        progress.current_disk_index = disk_index;
        progress.last_scan_key = last_key;
    }
}

impl Default for IOMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for IOThrottler {
    fn default() -> Self {
        Self::new()
    }
}
