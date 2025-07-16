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

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use ecstore::{
    disk::{DiskAPI, DiskStore, WalkDirOptions},
    set_disk::SetDisks,
};
use rustfs_ecstore as ecstore;
use rustfs_filemeta::MetacacheReader;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::{
    data_usage::DataUsageInfo,
    metrics::{BucketMetrics, DiskMetrics, MetricsCollector, ScannerMetrics},
};
use crate::heal::HealManager;
use crate::{
    error::{Error, Result},
    get_ahm_services_cancel_token, HealRequest,
};

use rustfs_ecstore::disk::RUSTFS_META_BUCKET;

/// Custom scan mode enum for AHM scanner
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanMode {
    /// Normal scan - basic object discovery and metadata collection
    #[default]
    Normal,
    /// Deep scan - includes EC verification and integrity checks
    Deep,
}

/// Scanner configuration
#[derive(Debug, Clone)]
pub struct ScannerConfig {
    /// Scan interval between cycles
    pub scan_interval: Duration,
    /// Deep scan interval (how often to perform deep scan)
    pub deep_scan_interval: Duration,
    /// Maximum concurrent scans
    pub max_concurrent_scans: usize,
    /// Whether to enable healing
    pub enable_healing: bool,
    /// Whether to enable metrics collection
    pub enable_metrics: bool,
    /// Current scan mode (normal, deep)
    pub scan_mode: ScanMode,
    /// Whether to enable data usage statistics collection
    pub enable_data_usage_stats: bool,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(60),        // 1 minute
            deep_scan_interval: Duration::from_secs(3600), // 1 hour
            max_concurrent_scans: 20,
            enable_healing: true,
            enable_metrics: true,
            scan_mode: ScanMode::Normal,
            enable_data_usage_stats: true,
        }
    }
}

/// Scanner state
#[derive(Debug, Default)]
pub struct ScannerState {
    /// Whether scanner is running
    pub is_running: bool,
    /// Current scan cycle
    pub current_cycle: u64,
    /// Last scan start time
    pub last_scan_start: Option<SystemTime>,
    /// Last scan end time
    pub last_scan_end: Option<SystemTime>,
    /// Current scan duration
    pub current_scan_duration: Option<Duration>,
    /// Last deep scan time
    pub last_deep_scan_time: Option<SystemTime>,
    /// Buckets being scanned
    pub scanning_buckets: Vec<String>,
    /// Disks being scanned
    pub scanning_disks: Vec<String>,
}

/// AHM Scanner - Automatic Health Management Scanner
///
/// This scanner monitors the health of objects in the RustFS storage system.
/// It integrates with ECStore to perform real data scanning across all EC sets
/// and collects metrics.
///
/// The scanner operates on the entire ECStore, scanning all EC (Erasure Coding) sets,
/// where each set contains multiple disks that store the same objects with different shards.
pub struct Scanner {
    /// Scanner configuration
    config: Arc<RwLock<ScannerConfig>>,
    /// Scanner state
    state: Arc<RwLock<ScannerState>>,
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    /// Bucket metrics cache
    bucket_metrics: Arc<Mutex<HashMap<String, BucketMetrics>>>,
    /// Disk metrics cache
    disk_metrics: Arc<Mutex<HashMap<String, DiskMetrics>>>,
    /// Data usage statistics cache
    data_usage_stats: Arc<Mutex<HashMap<String, DataUsageInfo>>>,
    /// Last data usage statistics collection time
    last_data_usage_collection: Arc<RwLock<Option<SystemTime>>>,
    /// Heal manager for auto-heal integration
    heal_manager: Option<Arc<HealManager>>,
}

impl Scanner {
    /// Create a new scanner
    pub fn new(config: Option<ScannerConfig>, heal_manager: Option<Arc<HealManager>>) -> Self {
        let config = config.unwrap_or_default();
        info!("Creating AHM scanner for all EC sets");
        Self {
            config: Arc::new(RwLock::new(config)),
            state: Arc::new(RwLock::new(ScannerState::default())),
            metrics: Arc::new(MetricsCollector::new()),
            bucket_metrics: Arc::new(Mutex::new(HashMap::new())),
            disk_metrics: Arc::new(Mutex::new(HashMap::new())),
            data_usage_stats: Arc::new(Mutex::new(HashMap::new())),
            last_data_usage_collection: Arc::new(RwLock::new(None)),
            heal_manager,
        }
    }

    /// Set the heal manager after construction
    pub fn set_heal_manager(&mut self, heal_manager: Arc<HealManager>) {
        self.heal_manager = Some(heal_manager);
    }

    /// Start the scanner
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;

        if state.is_running {
            warn!("Scanner is already running");
            return Ok(());
        }

        state.is_running = true;
        state.last_scan_start = Some(SystemTime::now());

        info!("Starting AHM scanner");

        // Start background scan loop
        let scanner = self.clone_for_background();
        tokio::spawn(async move {
            if let Err(e) = scanner.scan_loop().await {
                error!("Scanner loop failed: {}", e);
            }
        });

        Ok(())
    }

    /// Stop the scanner gracefully
    pub async fn stop(&self) -> Result<()> {
        let mut state = self.state.write().await;

        if !state.is_running {
            warn!("Scanner is not running");
            return Ok(());
        }

        info!("Stopping AHM scanner gracefully...");

        // Trigger cancellation using global cancel token
        if let Some(cancel_token) = get_ahm_services_cancel_token() {
            cancel_token.cancel();
        }

        state.is_running = false;
        state.last_scan_end = Some(SystemTime::now());

        if let Some(start_time) = state.last_scan_start {
            state.current_scan_duration = Some(SystemTime::now().duration_since(start_time).unwrap_or(Duration::ZERO));
        }

        info!("AHM scanner stopped");
        Ok(())
    }

    /// Get integrated data usage statistics for DataUsageInfoHandler
    pub async fn get_data_usage_info(&self) -> Result<DataUsageInfo> {
        let mut integrated_info = DataUsageInfo::new();

        // Collect data from all buckets
        {
            let data_usage_guard = self.data_usage_stats.lock().await;
            for (bucket_name, bucket_data) in data_usage_guard.iter() {
                let _bucket_name = bucket_name;

                // Merge bucket data into integrated info
                integrated_info.merge(bucket_data);
            }
        }

        // Update capacity information from storage info
        if let Some(ecstore) = rustfs_ecstore::new_object_layer_fn() {
            let mut total_capacity = 0u64;
            let mut total_used_capacity = 0u64;
            let mut total_free_capacity = 0u64;

            // Collect capacity info from all SetDisks
            for pool in &ecstore.pools {
                for set_disks in &pool.disk_set {
                    let (disks, _) = set_disks.get_online_disks_with_healing(false).await;
                    for disk in disks {
                        if let Ok(disk_info) = disk
                            .disk_info(&ecstore::disk::DiskInfoOptions {
                                disk_id: disk.path().to_string_lossy().to_string(),
                                metrics: true,
                                noop: false,
                            })
                            .await
                        {
                            total_capacity += disk_info.total;
                            total_used_capacity += disk_info.used;
                            total_free_capacity += disk_info.free;
                        }
                    }
                }
            }

            if total_capacity > 0 {
                integrated_info.update_capacity(total_capacity, total_used_capacity, total_free_capacity);
            }
        }

        Ok(integrated_info)
    }

    /// Get current scanner metrics
    pub async fn get_metrics(&self) -> ScannerMetrics {
        let mut metrics = self.metrics.get_metrics();

        // Add bucket metrics
        let bucket_metrics: HashMap<String, BucketMetrics> = {
            let bucket_metrics_guard = self.bucket_metrics.lock().await;
            bucket_metrics_guard
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        };
        metrics.bucket_metrics = bucket_metrics;

        // Add disk metrics
        let disk_metrics: HashMap<String, DiskMetrics> = {
            let disk_metrics_guard = self.disk_metrics.lock().await;
            disk_metrics_guard
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        };
        metrics.disk_metrics = disk_metrics;

        // Add current scan duration
        let state = self.state.read().await;
        metrics.current_scan_duration = state.current_scan_duration;

        metrics
    }

    /// Perform a single scan cycle
    pub async fn scan_cycle(&self) -> Result<()> {
        let start_time = SystemTime::now();

        info!("Starting scan cycle {} for all EC sets", self.metrics.get_metrics().current_cycle + 1);

        // Update state
        {
            let mut state = self.state.write().await;
            state.current_cycle += 1;
            state.last_scan_start = Some(start_time);
            state.scanning_buckets.clear();
            state.scanning_disks.clear();
        }

        self.metrics.set_current_cycle(self.state.read().await.current_cycle);
        self.metrics.increment_total_cycles();

        // Get ECStore and all SetDisks
        let Some(ecstore) = rustfs_ecstore::new_object_layer_fn() else {
            warn!("No ECStore available for scanning");
            return Ok(());
        };

        // Get all SetDisks from all pools
        let mut all_set_disks = Vec::new();
        for pool in &ecstore.pools {
            for set_disks in &pool.disk_set {
                all_set_disks.push(set_disks.clone());
            }
        }

        if all_set_disks.is_empty() {
            warn!("No EC sets available for scanning");
            return Ok(());
        }

        info!("Scanning {} EC sets across {} pools", all_set_disks.len(), ecstore.pools.len());

        // Phase 1: Scan all SetDisks concurrently
        let config = self.config.read().await;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_scans));
        drop(config);
        let mut scan_futures = Vec::new();

        for set_disks in all_set_disks {
            let semaphore = semaphore.clone();
            let scanner = self.clone_for_background();

            let future = async move {
                let _permit = semaphore.acquire().await.unwrap();
                scanner.scan_set_disks(set_disks).await
            };

            scan_futures.push(future);
        }

        // Wait for all scans to complete
        let mut results = Vec::new();
        for future in scan_futures {
            results.push(future.await);
        }

        // Check results and collect object metadata
        let mut successful_scans = 0;
        let mut failed_scans = 0;
        let mut all_disk_objects = Vec::new();

        for result in results {
            match result {
                Ok(disk_objects) => {
                    successful_scans += 1;
                    all_disk_objects.extend(disk_objects);
                }
                Err(e) => {
                    failed_scans += 1;
                    error!("SetDisks scan failed: {}", e);
                }
            }
        }

        // Phase 2: Analyze object distribution and perform EC verification
        if successful_scans > 0 {
            // Get all disks from all SetDisks for analysis
            let mut all_disks = Vec::new();
            for pool in &ecstore.pools {
                for set_disks in &pool.disk_set {
                    let (disks, _) = set_disks.get_online_disks_with_healing(false).await;
                    all_disks.extend(disks);
                }
            }

            if let Err(e) = self.analyze_object_distribution(&all_disk_objects, &all_disks).await {
                error!("Object distribution analysis failed: {}", e);
            }
        }

        // Update scan duration
        let scan_duration = SystemTime::now().duration_since(start_time).unwrap_or(Duration::ZERO);

        {
            let mut state = self.state.write().await;
            state.last_scan_end = Some(SystemTime::now());
            state.current_scan_duration = Some(scan_duration);
        }

        info!(
            "Completed scan cycle in {:?} ({} successful, {} failed)",
            scan_duration, successful_scans, failed_scans
        );
        Ok(())
    }

    /// Scan a single SetDisks (EC set)
    async fn scan_set_disks(
        &self,
        set_disks: Arc<SetDisks>,
    ) -> Result<Vec<HashMap<String, HashMap<String, rustfs_filemeta::FileMeta>>>> {
        let set_index = set_disks.set_index;
        let pool_index = set_disks.pool_index;

        info!("Scanning EC set {} in pool {}", set_index, pool_index);

        // Get online disks from this EC set
        let (disks, _) = set_disks.get_online_disks_with_healing(false).await;

        if disks.is_empty() {
            warn!("No online disks available for EC set {} in pool {}", set_index, pool_index);
            return Ok(Vec::new());
        }

        info!("Scanning {} online disks in EC set {} (pool {})", disks.len(), set_index, pool_index);

        // Scan all disks in this SetDisks concurrently
        let config = self.config.read().await;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_scans));
        drop(config);
        let mut scan_futures = Vec::new();

        for disk in disks {
            let semaphore = semaphore.clone();
            let scanner = self.clone_for_background();

            let future = async move {
                let _permit = semaphore.acquire().await.unwrap();
                scanner.scan_disk(&disk).await
            };

            scan_futures.push(future);
        }

        // Wait for all scans to complete
        let mut results = Vec::new();
        for future in scan_futures {
            results.push(future.await);
        }

        // Check results and collect object metadata
        let mut successful_scans = 0;
        let mut failed_scans = 0;
        let mut all_disk_objects = Vec::new();

        for result in results {
            match result {
                Ok(disk_objects) => {
                    successful_scans += 1;
                    all_disk_objects.push(disk_objects);
                }
                Err(e) => {
                    failed_scans += 1;
                    error!("Disk scan failed in EC set {} (pool {}): {}", set_index, pool_index, e);
                    // Add empty map for failed disk
                    all_disk_objects.push(HashMap::new());
                }
            }
        }

        info!(
            "Completed scanning EC set {} (pool {}): {} successful, {} failed",
            set_index, pool_index, successful_scans, failed_scans
        );

        Ok(all_disk_objects)
    }

    /// Scan a single disk
    async fn scan_disk(&self, disk: &DiskStore) -> Result<HashMap<String, HashMap<String, rustfs_filemeta::FileMeta>>> {
        let disk_path = disk.path().to_string_lossy().to_string();

        info!("Scanning disk: {}", disk_path);

        // Update disk metrics
        {
            let mut disk_metrics_guard = self.disk_metrics.lock().await;
            let metrics = disk_metrics_guard.entry(disk_path.clone()).or_insert_with(|| DiskMetrics {
                disk_path: disk_path.clone(),
                ..Default::default()
            });

            metrics.is_scanning = true;
            metrics.last_scan_time = Some(SystemTime::now());

            // Get disk info using DiskStore's disk_info interface
            if let Ok(disk_info) = disk
                .disk_info(&ecstore::disk::DiskInfoOptions {
                    disk_id: disk_path.clone(),
                    metrics: true,
                    noop: false,
                })
                .await
            {
                metrics.total_space = disk_info.total;
                metrics.used_space = disk_info.used;
                metrics.free_space = disk_info.free;
                metrics.is_online = disk.is_online().await;

                // 检查磁盘状态，如果离线则提交heal任务
                if !metrics.is_online {
                    let enable_healing = self.config.read().await.enable_healing;
                    if enable_healing {
                        if let Some(heal_manager) = &self.heal_manager {
                            let req = HealRequest::disk(disk.endpoint().clone());
                            match heal_manager.submit_heal_request(req).await {
                                Ok(task_id) => {
                                    warn!("disk offline, submit heal task: {} {}", task_id, disk_path);
                                }
                                Err(e) => {
                                    error!("disk offline, submit heal task failed: {} {}", disk_path, e);
                                }
                            }
                        }
                    }
                }

                // Additional disk info for debugging
                debug!(
                    "Disk {}: total={}, used={}, free={}, online={}",
                    disk_path, disk_info.total, disk_info.used, disk_info.free, metrics.is_online
                );
            }
        }

        // Update state
        {
            let mut state = self.state.write().await;
            state.scanning_disks.push(disk_path.clone());
        }

        // List volumes (buckets) on this disk
        let volumes = match disk.list_volumes().await {
            Ok(volumes) => volumes,
            Err(e) => {
                error!("Failed to list volumes on disk {}: {}", disk_path, e);

                // 磁盘访问失败，提交磁盘heal任务
                let enable_healing = self.config.read().await.enable_healing;
                if enable_healing {
                    if let Some(heal_manager) = &self.heal_manager {
                        use crate::heal::{HealPriority, HealRequest};
                        let req = HealRequest::new(
                            crate::heal::HealType::Disk {
                                endpoint: disk.endpoint().clone(),
                            },
                            crate::heal::HealOptions::default(),
                            HealPriority::Urgent,
                        );
                        match heal_manager.submit_heal_request(req).await {
                            Ok(task_id) => {
                                warn!("disk access failed, submit heal task: {} {}", task_id, disk_path);
                            }
                            Err(heal_err) => {
                                error!("disk access failed, submit heal task failed: {} {}", disk_path, heal_err);
                            }
                        }
                    }
                }

                return Err(Error::Storage(e.into()));
            }
        };

        // Scan each volume and collect object metadata
        let mut disk_objects = HashMap::new();
        for volume in volumes {
            // check cancel token
            if let Some(cancel_token) = get_ahm_services_cancel_token() {
                if cancel_token.is_cancelled() {
                    info!("Cancellation requested, stopping disk scan");
                    break;
                }
            }

            match self.scan_volume(disk, &volume.name).await {
                Ok(object_metadata) => {
                    disk_objects.insert(volume.name, object_metadata);
                }
                Err(e) => {
                    error!("Failed to scan volume {} on disk {}: {}", volume.name, disk_path, e);
                    continue;
                }
            }
        }

        // Update disk metrics after scan
        {
            let mut disk_metrics_guard = self.disk_metrics.lock().await;
            if let Some(existing_metrics) = disk_metrics_guard.get(&disk_path) {
                let mut updated_metrics = existing_metrics.clone();
                updated_metrics.is_scanning = false;
                disk_metrics_guard.insert(disk_path.clone(), updated_metrics);
            }
        }

        // Update state
        {
            let mut state = self.state.write().await;
            state.scanning_disks.retain(|d| d != &disk_path);
        }

        Ok(disk_objects)
    }

    /// Scan a single volume (bucket) and collect object information
    ///
    /// This method collects all objects from a disk for a specific bucket.
    /// It returns a map of object names to their metadata for later analysis.
    async fn scan_volume(&self, disk: &DiskStore, bucket: &str) -> Result<HashMap<String, rustfs_filemeta::FileMeta>> {
        info!("Scanning bucket: {} on disk: {}", bucket, disk.to_string());

        // Initialize bucket metrics if not exists
        {
            let mut bucket_metrics_guard = self.bucket_metrics.lock().await;
            bucket_metrics_guard
                .entry(bucket.to_string())
                .or_insert_with(|| BucketMetrics {
                    bucket: bucket.to_string(),
                    ..Default::default()
                });
        }

        // Update state
        {
            let mut state = self.state.write().await;
            state.scanning_buckets.push(bucket.to_string());
        }

        self.metrics.increment_bucket_scans_started(1);

        let scan_start = SystemTime::now();

        // Walk through all objects in the bucket
        let walk_opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: String::new(),
            recursive: true,
            report_notfound: false,
            filter_prefix: None,
            forward_to: None,
            limit: 0,
            disk_id: String::new(),
        };

        // Use a buffer to collect scan results for processing
        let mut scan_buffer = Vec::new();

        if let Err(e) = disk.walk_dir(walk_opts, &mut scan_buffer).await {
            error!("Failed to walk directory for bucket {}: {}", bucket, e);
            return Err(Error::Storage(e.into()));
        }

        // Process the scan results using MetacacheReader
        let mut reader = MetacacheReader::new(std::io::Cursor::new(scan_buffer));
        let mut objects_scanned = 0u64;
        let mut objects_with_issues = 0u64;
        let mut object_metadata = HashMap::new();

        // Process each object entry
        while let Ok(Some(mut entry)) = reader.peek().await {
            objects_scanned += 1;
            // Check if this is an actual object (not just a directory)
            if entry.is_object() {
                debug!("Scanned object: {}", entry.name);

                // Parse object metadata
                if let Ok(file_meta) = entry.xl_meta() {
                    if file_meta.versions.is_empty() {
                        objects_with_issues += 1;
                        warn!("Object {} has no versions", entry.name);

                        // 对象元数据损坏，提交元数据heal任务
                        let enable_healing = self.config.read().await.enable_healing;
                        if enable_healing {
                            if let Some(heal_manager) = &self.heal_manager {
                                let req = HealRequest::metadata(bucket.to_string(), entry.name.clone());
                                match heal_manager.submit_heal_request(req).await {
                                    Ok(task_id) => {
                                        warn!(
                                            "object metadata damaged, submit heal task: {} {} / {}",
                                            task_id, bucket, entry.name
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            "object metadata damaged, submit heal task failed: {} / {} {}",
                                            bucket, entry.name, e
                                        );
                                    }
                                }
                            }
                        }
                    } else {
                        // Store object metadata for later analysis
                        object_metadata.insert(entry.name.clone(), file_meta.clone());
                    }
                } else {
                    objects_with_issues += 1;
                    warn!("Failed to parse metadata for object {}", entry.name);

                    // 对象元数据解析失败，提交元数据heal任务
                    let enable_healing = self.config.read().await.enable_healing;
                    if enable_healing {
                        if let Some(heal_manager) = &self.heal_manager {
                            let req = HealRequest::metadata(bucket.to_string(), entry.name.clone());
                            match heal_manager.submit_heal_request(req).await {
                                Ok(task_id) => {
                                    warn!(
                                        "object metadata parse failed, submit heal task: {} {} / {}",
                                        task_id, bucket, entry.name
                                    );
                                }
                                Err(e) => {
                                    error!(
                                        "object metadata parse failed, submit heal task failed: {} / {} {}",
                                        bucket, entry.name, e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        // Update metrics
        self.metrics.increment_objects_scanned(objects_scanned);
        self.metrics.increment_objects_with_issues(objects_with_issues);
        self.metrics.increment_bucket_scans_finished(1);

        // Update bucket metrics
        {
            let mut bucket_metrics_guard = self.bucket_metrics.lock().await;
            if let Some(existing_metrics) = bucket_metrics_guard.get(bucket) {
                let mut updated_metrics = existing_metrics.clone();
                updated_metrics.total_objects = objects_scanned;
                updated_metrics.objects_with_issues = objects_with_issues;
                updated_metrics.scan_duration = Some(SystemTime::now().duration_since(scan_start).unwrap_or(Duration::ZERO));
                bucket_metrics_guard.insert(bucket.to_string(), updated_metrics);
            }
        }

        // Update state
        {
            let mut state = self.state.write().await;
            state.scanning_buckets.retain(|b| b != bucket);
        }

        debug!(
            "Completed scanning bucket: {} on disk {} ({} objects, {} issues)",
            bucket,
            disk.to_string(),
            objects_scanned,
            objects_with_issues
        );

        Ok(object_metadata)
    }

    /// Analyze object distribution across all disks and perform EC verification
    ///
    /// This method takes the collected object metadata from all disks and:
    /// 1. Creates a union of all objects across all disks
    /// 2. Identifies missing objects on each disk (for healing)
    /// 3. Performs EC decode verification for deep scan mode
    async fn analyze_object_distribution(
        &self,
        all_disk_objects: &[HashMap<String, HashMap<String, rustfs_filemeta::FileMeta>>],
        disks: &[DiskStore],
    ) -> Result<()> {
        info!("Analyzing object distribution across {} disks", disks.len());

        // Step 1: Create union of all objects across all disks
        let mut all_objects = HashMap::new(); // bucket -> Set<object_name>
        let mut object_locations = HashMap::new(); // (bucket, object) -> Vec<disk_index>

        for (disk_idx, disk_objects) in all_disk_objects.iter().enumerate() {
            for (bucket, objects) in disk_objects {
                if bucket == RUSTFS_META_BUCKET {
                    // Skip internal system bucket during analysis to speed up tests
                    continue;
                }
                // Add bucket to all_objects
                let bucket_objects = all_objects
                    .entry(bucket.clone())
                    .or_insert_with(std::collections::HashSet::new);

                for (object_name, _file_meta) in objects.iter() {
                    bucket_objects.insert(object_name.clone());

                    // Record which disk has this object
                    let key = (bucket.clone(), object_name.clone());
                    let locations = object_locations.entry(key).or_insert_with(Vec::new);
                    locations.push(disk_idx);
                }
            }
        }

        info!(
            "Found {} buckets with {} total objects",
            all_objects.len(),
            all_objects.values().map(|s| s.len()).sum::<usize>()
        );

        // Step 2: Identify missing objects and perform EC verification
        let mut objects_needing_heal = 0u64;
        let mut objects_with_ec_issues = 0u64;

        for (bucket, objects) in &all_objects {
            // Skip internal RustFS system bucket to avoid lengthy checks on temporary/trash objects
            if bucket == RUSTFS_META_BUCKET {
                continue;
            }
            for object_name in objects {
                let key = (bucket.clone(), object_name.clone());
                let empty_vec = Vec::new();
                let locations = object_locations.get(&key).unwrap_or(&empty_vec);

                // Check if object is missing from some disks
                if locations.len() < disks.len() {
                    objects_needing_heal += 1;
                    let missing_disks: Vec<usize> = (0..disks.len()).filter(|&i| !locations.contains(&i)).collect();
                    warn!("Object {}/{} missing from disks: {:?}", bucket, object_name, missing_disks);
                    println!("Object {bucket}/{object_name} missing from disks: {missing_disks:?}");

                    // submit heal task
                    let enable_healing = self.config.read().await.enable_healing;
                    if enable_healing {
                        if let Some(heal_manager) = &self.heal_manager {
                            use crate::heal::{HealPriority, HealRequest};
                            let req = HealRequest::new(
                                crate::heal::HealType::Object {
                                    bucket: bucket.clone(),
                                    object: object_name.clone(),
                                    version_id: None,
                                },
                                crate::heal::HealOptions::default(),
                                HealPriority::High,
                            );
                            match heal_manager.submit_heal_request(req).await {
                                Ok(task_id) => {
                                    warn!(
                                        "object missing, submit heal task: {} {} / {} (missing disks: {:?})",
                                        task_id, bucket, object_name, missing_disks
                                    );
                                }
                                Err(e) => {
                                    error!("object missing, submit heal task failed: {} / {} {}", bucket, object_name, e);
                                }
                            }
                        }
                    }
                }

                // Step 3: Deep scan EC verification
                let config = self.config.read().await;
                if config.scan_mode == ScanMode::Deep {
                    // Find the first disk that has this object to get metadata
                    if let Some(&first_disk_idx) = locations.first() {
                        if let Some(file_meta) = all_disk_objects[first_disk_idx]
                            .get(bucket)
                            .and_then(|objects| objects.get(object_name))
                        {
                            if let Err(e) = self
                                .verify_ec_decode_with_locations(bucket, object_name, file_meta, locations, disks)
                                .await
                            {
                                objects_with_ec_issues += 1;
                                warn!("EC decode verification failed for object {}/{}: {}", bucket, object_name, e);
                            }
                        }
                    }
                }
            }
        }

        info!(
            "Analysis complete: {} objects need healing, {} objects have EC issues",
            objects_needing_heal, objects_with_ec_issues
        );

        // Step 4: Collect data usage statistics if enabled
        let config = self.config.read().await;
        if config.enable_data_usage_stats {
            if let Err(e) = self.collect_data_usage_statistics(all_disk_objects).await {
                error!("Failed to collect data usage statistics: {}", e);
            }
        }
        drop(config);

        Ok(())
    }

    /// Verify EC decode capability for an object using known disk locations
    ///
    /// This method is optimized to use the known locations of object copies
    /// instead of scanning all disks.
    async fn verify_ec_decode_with_locations(
        &self,
        bucket: &str,
        object: &str,
        file_meta: &rustfs_filemeta::FileMeta,
        locations: &[usize],
        all_disks: &[DiskStore],
    ) -> Result<()> {
        // Get EC parameters from the latest version
        let (data_blocks, _parity_blocks) = if let Some(latest_version) = file_meta.versions.last() {
            if let Ok(version) = rustfs_filemeta::FileMetaVersion::try_from(latest_version.clone()) {
                if let Some(obj) = version.object {
                    (obj.erasure_m, obj.erasure_n)
                } else {
                    // Not an object version, skip EC verification
                    return Ok(());
                }
            } else {
                // Cannot parse version, skip EC verification
                return Ok(());
            }
        } else {
            // No versions, skip EC verification
            return Ok(());
        };

        let read_quorum = data_blocks; // Need at least data_blocks to decode

        if locations.len() < read_quorum {
            return Err(Error::Scanner(format!(
                "Insufficient object copies for EC decode: need {}, have {}",
                read_quorum,
                locations.len()
            )));
        }

        // Try to read object metadata from the known locations
        let mut successful_reads = 0;
        let mut errors = Vec::new();

        for &disk_idx in locations {
            if successful_reads >= read_quorum {
                break; // We have enough copies for EC decode
            }

            let disk = &all_disks[disk_idx];
            match disk.read_xl(bucket, object, false).await {
                Ok(_) => {
                    successful_reads += 1;
                    debug!(
                        "Successfully read object {}/{} from disk {} (index: {})",
                        bucket,
                        object,
                        disk.to_string(),
                        disk_idx
                    );
                }
                Err(e) => {
                    let error_msg = format!("{e}");
                    errors.push(error_msg);
                    debug!(
                        "Failed to read object {}/{} from disk {} (index: {}): {}",
                        bucket,
                        object,
                        disk.to_string(),
                        disk_idx,
                        e
                    );
                }
            }
        }

        if successful_reads >= read_quorum {
            debug!(
                "EC decode verification passed for object {}/{} ({} successful reads from {} locations)",
                bucket,
                object,
                successful_reads,
                locations.len()
            );
            Ok(())
        } else {
            // submit heal task
            let enable_healing = self.config.read().await.enable_healing;
            if enable_healing {
                if let Some(heal_manager) = &self.heal_manager {
                    use crate::heal::HealRequest;
                    let req = HealRequest::ec_decode(bucket.to_string(), object.to_string(), None);
                    match heal_manager.submit_heal_request(req).await {
                        Ok(task_id) => {
                            warn!("EC decode failed, submit heal task: {} {} / {}", task_id, bucket, object);
                        }
                        Err(e) => {
                            error!("EC decode failed, submit heal task failed: {} / {} {}", bucket, object, e);
                        }
                    }
                }
            }
            Err(Error::Scanner(format!(
                "EC decode verification failed for object {bucket}/{object}: need {read_quorum} reads, got {successful_reads} (errors: {errors:?})"
            )))
        }
    }

    /// Background scan loop with graceful shutdown
    async fn scan_loop(self) -> Result<()> {
        let config = self.config.read().await;
        let mut interval = tokio::time::interval(config.scan_interval);
        let deep_scan_interval = config.deep_scan_interval;
        drop(config);

        // Get global cancel token
        let cancel_token = if let Some(global_token) = get_ahm_services_cancel_token() {
            global_token.clone()
        } else {
            CancellationToken::new()
        };

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Check if scanner should still be running
                    if !self.state.read().await.is_running {
                        break;
                    }

                    // 检查取消信号
                    if cancel_token.is_cancelled() {
                        info!("Cancellation requested, exiting scanner loop");
                        break;
                    }

                    // Determine if it's time for a deep scan
                    let current_time = SystemTime::now();
                    let last_deep_scan_time = self.state.read().await.last_deep_scan_time.unwrap_or(SystemTime::UNIX_EPOCH);

                    if current_time.duration_since(last_deep_scan_time).unwrap_or(Duration::ZERO) >= deep_scan_interval {
                        info!("Deep scan interval reached, switching to deep scan mode");
                        self.config.write().await.scan_mode = ScanMode::Deep;
                        self.state.write().await.last_deep_scan_time = Some(current_time);
                    }

                    // Perform scan cycle
                    if let Err(e) = self.scan_cycle().await {
                        error!("Scan cycle failed: {}", e);
                    }
                }
                _ = cancel_token.cancelled() => {
                    info!("Received cancellation, stopping scanner loop");
                    break;
                }
            }
        }

        info!("Scanner loop stopped");
        Ok(())
    }

    /// Collect data usage statistics from scanned objects
    async fn collect_data_usage_statistics(
        &self,
        all_disk_objects: &[HashMap<String, HashMap<String, rustfs_filemeta::FileMeta>>],
    ) -> Result<()> {
        info!("Collecting data usage statistics from {} disk scans", all_disk_objects.len());

        let mut data_usage = DataUsageInfo::default();

        // Collect objects from all disks (avoid duplicates by using first occurrence)
        let mut processed_objects = std::collections::HashSet::new();

        for disk_objects in all_disk_objects {
            for (bucket_name, objects) in disk_objects {
                if bucket_name == RUSTFS_META_BUCKET {
                    continue; // skip internal bucket from data usage stats
                }
                for object_name in objects.keys() {
                    let object_key = format!("{bucket_name}/{object_name}");

                    // Skip if already processed (avoid duplicates across disks)
                    if !processed_objects.insert(object_key.clone()) {
                        continue;
                    }

                    // Add object to data usage statistics (pass entire FileMeta for accurate version counting)
                    data_usage.add_object_from_file_meta(&object_key, objects.get(object_name).unwrap());
                }
            }
        }

        // Ensure buckets_count is correctly set
        data_usage.buckets_count = data_usage.buckets_usage.len() as u64;

        // Log statistics before storing
        info!(
            "Collected data usage statistics: {} buckets, {} total objects, {} total size",
            data_usage.buckets_count, data_usage.objects_total_count, data_usage.objects_total_size
        );

        // Store in cache and update last collection time
        let current_time = SystemTime::now();
        {
            let mut data_usage_guard = self.data_usage_stats.lock().await;
            data_usage_guard.insert("current".to_string(), data_usage.clone());
        }
        {
            let mut last_collection = self.last_data_usage_collection.write().await;
            *last_collection = Some(current_time);
        }

        // Store to backend if configured (spawned to avoid blocking scan loop)
        let config = self.config.read().await;
        if config.enable_data_usage_stats {
            if let Some(store) = rustfs_ecstore::new_object_layer_fn() {
                // Offload persistence to background task
                let data_clone = data_usage.clone();
                tokio::spawn(async move {
                    if let Err(e) = super::data_usage::store_data_usage_in_backend(data_clone, store).await {
                        error!("Failed to store data usage statistics to backend: {}", e);
                    } else {
                        info!("Successfully stored data usage statistics to backend");
                    }
                });
            } else {
                warn!("Storage not available, skipping backend persistence");
            }
        }

        Ok(())
    }

    /// Clone scanner for background tasks
    fn clone_for_background(&self) -> Self {
        Self {
            config: self.config.clone(),
            state: Arc::clone(&self.state),
            metrics: Arc::clone(&self.metrics),
            bucket_metrics: Arc::clone(&self.bucket_metrics),
            disk_metrics: Arc::clone(&self.disk_metrics),
            data_usage_stats: Arc::clone(&self.data_usage_stats),
            last_data_usage_collection: Arc::clone(&self.last_data_usage_collection),
            heal_manager: self.heal_manager.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_ecstore::disk::endpoint::Endpoint;
    use rustfs_ecstore::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
    use rustfs_ecstore::store::ECStore;
    use rustfs_ecstore::{
        StorageAPI,
        store_api::{MakeBucketOptions, ObjectIO, PutObjReader},
    };
    use serial_test::serial;
    use std::fs;
    use std::net::SocketAddr;
    use std::sync::OnceLock;

    // Global test environment cache to avoid repeated initialization
    static GLOBAL_TEST_ENV: OnceLock<(Vec<std::path::PathBuf>, Arc<ECStore>)> = OnceLock::new();

    async fn prepare_test_env(test_dir: Option<&str>, port: Option<u16>) -> (Vec<std::path::PathBuf>, Arc<ECStore>) {
        // Check if global environment is already initialized
        if let Some((disk_paths, ecstore)) = GLOBAL_TEST_ENV.get() {
            return (disk_paths.clone(), ecstore.clone());
        }

        // create temp dir as 4 disks
        let test_base_dir = test_dir.unwrap_or("/tmp/rustfs_ahm_test");
        let temp_dir = std::path::PathBuf::from(test_base_dir);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir).unwrap();
        }
        fs::create_dir_all(&temp_dir).unwrap();

        // create 4 disk dirs
        let disk_paths = vec![
            temp_dir.join("disk1"),
            temp_dir.join("disk2"),
            temp_dir.join("disk3"),
            temp_dir.join("disk4"),
        ];

        for disk_path in &disk_paths {
            fs::create_dir_all(disk_path).unwrap();
        }

        // create EndpointServerPools
        let mut endpoints = Vec::new();
        for (i, disk_path) in disk_paths.iter().enumerate() {
            let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
            // set correct index
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(i);
            endpoints.push(endpoint);
        }

        let pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "test".to_string(),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        };

        let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);

        // format disks
        rustfs_ecstore::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

        // create ECStore with dynamic port
        let port = port.unwrap_or(9000);
        let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let ecstore = ECStore::new(server_addr, endpoint_pools).await.unwrap();

        // init bucket metadata system
        let buckets_list = ecstore
            .list_bucket(&rustfs_ecstore::store_api::BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let buckets = buckets_list.into_iter().map(|v| v.name).collect();
        rustfs_ecstore::bucket::metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

        // Store in global cache
        let _ = GLOBAL_TEST_ENV.set((disk_paths.clone(), ecstore.clone()));

        (disk_paths, ecstore)
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_scanner_basic_functionality() {
        const TEST_DIR_BASIC: &str = "/tmp/rustfs_ahm_test_basic";
        let (disk_paths, ecstore) = prepare_test_env(Some(TEST_DIR_BASIC), Some(9001)).await;

        // create some test data
        let bucket_name = "test-bucket";
        let object_name = "test-object";
        let test_data = b"Hello, RustFS!";

        // create bucket and verify
        let bucket_opts = MakeBucketOptions::default();
        ecstore
            .make_bucket(bucket_name, &bucket_opts)
            .await
            .expect("make_bucket failed");

        // check bucket really exists
        let buckets = ecstore
            .list_bucket(&rustfs_ecstore::store_api::BucketOptions::default())
            .await
            .unwrap();
        assert!(buckets.iter().any(|b| b.name == bucket_name), "bucket not found after creation");

        // write object
        let mut put_reader = PutObjReader::from_vec(test_data.to_vec());
        let object_opts = rustfs_ecstore::store_api::ObjectOptions::default();
        ecstore
            .put_object(bucket_name, object_name, &mut put_reader, &object_opts)
            .await
            .expect("put_object failed");

        // create Scanner and test basic functionality
        let scanner = Scanner::new(None, None);

        // Test 1: Normal scan - verify object is found
        println!("=== Test 1: Normal scan ===");
        let scan_result = scanner.scan_cycle().await;
        assert!(scan_result.is_ok(), "Normal scan should succeed");
        let metrics = scanner.get_metrics().await;
        assert!(metrics.objects_scanned > 0, "Objects scanned should be positive");
        println!("Normal scan completed successfully");

        // Test 2: Simulate disk corruption - delete object data from disk1
        println!("=== Test 2: Simulate disk corruption ===");
        let disk1_bucket_path = disk_paths[0].join(bucket_name);
        let disk1_object_path = disk1_bucket_path.join(object_name);

        // Try to delete the object file from disk1 (simulate corruption)
        // Note: This might fail if ECStore is actively using the file
        match fs::remove_dir_all(&disk1_object_path) {
            Ok(_) => {
                println!("Successfully deleted object from disk1: {disk1_object_path:?}");

                // Verify deletion by checking if the directory still exists
                if disk1_object_path.exists() {
                    println!("WARNING: Directory still exists after deletion: {disk1_object_path:?}");
                } else {
                    println!("Confirmed: Directory was successfully deleted");
                }
            }
            Err(e) => {
                println!("Could not delete object from disk1 (file may be in use): {disk1_object_path:?} - {e}");
                // This is expected behavior - ECStore might be holding file handles
            }
        }

        // Scan again - should still complete (even with missing data)
        let scan_result_after_corruption = scanner.scan_cycle().await;
        println!("Scan after corruption result: {scan_result_after_corruption:?}");

        // Scanner should handle missing data gracefully
        assert!(scan_result_after_corruption.is_ok(), "Scanner should handle missing data gracefully");

        // Test 3: Verify EC decode capability
        println!("=== Test 3: Verify EC decode ===");
        // Note: EC decode verification is done internally during scan_cycle
        // We can verify that the scanner handles missing data gracefully
        println!("EC decode verification is handled internally during scan cycles");

        // Test 4: Test metrics collection
        println!("=== Test 4: Metrics collection ===");
        let final_metrics = scanner.get_metrics().await;
        println!("Final metrics: {final_metrics:?}");

        // Verify metrics are reasonable
        assert!(final_metrics.total_cycles > 0, "Should have completed scan cycles");
        assert!(final_metrics.last_activity.is_some(), "Should have scan activity");

        // clean up temp dir
        let temp_dir = std::path::PathBuf::from(TEST_DIR_BASIC);
        if let Err(e) = fs::remove_dir_all(&temp_dir) {
            eprintln!("Warning: Failed to clean up temp directory {temp_dir:?}: {e}");
        }
    }

    // test data usage statistics collection and validation
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_scanner_usage_stats() {
        const TEST_DIR_USAGE_STATS: &str = "/tmp/rustfs_ahm_test_usage_stats";
        let (_, ecstore) = prepare_test_env(Some(TEST_DIR_USAGE_STATS), Some(9002)).await;

        // prepare test bucket and object
        let bucket = "test-bucket";
        ecstore.make_bucket(bucket, &Default::default()).await.unwrap();
        let mut pr = PutObjReader::from_vec(b"hello".to_vec());
        ecstore
            .put_object(bucket, "obj1", &mut pr, &Default::default())
            .await
            .unwrap();

        let scanner = Scanner::new(None, None);

        // enable statistics
        {
            let mut cfg = scanner.config.write().await;
            cfg.enable_data_usage_stats = true;
        }

        // first scan and get statistics
        scanner.scan_cycle().await.unwrap();
        let du_initial = scanner.get_data_usage_info().await.unwrap();
        assert!(du_initial.objects_total_count > 0);

        // write 3 more objects and get statistics again
        for size in [1024, 2048, 4096] {
            let name = format!("obj_{size}");
            let mut pr = PutObjReader::from_vec(vec![b'x'; size]);
            ecstore.put_object(bucket, &name, &mut pr, &Default::default()).await.unwrap();
        }

        scanner.scan_cycle().await.unwrap();
        let du_after = scanner.get_data_usage_info().await.unwrap();
        assert!(du_after.objects_total_count >= du_initial.objects_total_count + 3);

        // verify correctness of persisted data
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let persisted = crate::scanner::data_usage::load_data_usage_from_backend(ecstore.clone())
            .await
            .expect("load persisted usage");
        assert_eq!(persisted.objects_total_count, du_after.objects_total_count);
        assert_eq!(persisted.buckets_count, du_after.buckets_count);
        let p_bucket = persisted.buckets_usage.get(bucket).unwrap();
        let m_bucket = du_after.buckets_usage.get(bucket).unwrap();
        assert_eq!(p_bucket.objects_count, m_bucket.objects_count);
        assert_eq!(p_bucket.size, m_bucket.size);

        // consistency - again scan should not change count
        scanner.scan_cycle().await.unwrap();
        let du_cons = scanner.get_data_usage_info().await.unwrap();
        assert_eq!(du_after.objects_total_count, du_cons.objects_total_count);

        // clean up temp dir
        let _ = std::fs::remove_dir_all(std::path::Path::new(TEST_DIR_USAGE_STATS));
    }
}
