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
use crate::scanner::LoadLevel;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// IO monitor config   
#[derive(Debug, Clone)]
pub struct IOMonitorConfig {
    /// monitor interval
    pub monitor_interval: Duration,
    /// history data retention time
    pub history_retention: Duration,
    /// load evaluation window size
    pub load_window_size: usize,
    /// whether to enable actual system monitoring
    pub enable_system_monitoring: bool,
    /// disk path list (for monitoring specific disks)
    pub disk_paths: Vec<String>,
}

impl Default for IOMonitorConfig {
    fn default() -> Self {
        Self {
            monitor_interval: Duration::from_secs(1),    // 1 second monitor interval
            history_retention: Duration::from_secs(300), // keep 5 minutes history
            load_window_size: 30,                        // 30 sample points sliding window
            enable_system_monitoring: false,             // default use simulated data
            disk_paths: Vec::new(),
        }
    }
}

/// IO monitor metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IOMetrics {
    /// timestamp
    pub timestamp: SystemTime,
    /// disk IOPS (read + write)
    pub iops: u64,
    /// read IOPS
    pub read_iops: u64,
    /// write IOPS
    pub write_iops: u64,
    /// disk queue depth
    pub queue_depth: u64,
    /// average latency (milliseconds)
    pub avg_latency: u64,
    /// read latency (milliseconds)
    pub read_latency: u64,
    /// write latency (milliseconds)
    pub write_latency: u64,
    /// CPU usage (0-100)
    pub cpu_usage: u8,
    /// memory usage (0-100)
    pub memory_usage: u8,
    /// disk usage (0-100)
    pub disk_utilization: u8,
    /// network IO (Mbps)
    pub network_io: u64,
}

impl Default for IOMetrics {
    fn default() -> Self {
        Self {
            timestamp: SystemTime::now(),
            iops: 0,
            read_iops: 0,
            write_iops: 0,
            queue_depth: 0,
            avg_latency: 0,
            read_latency: 0,
            write_latency: 0,
            cpu_usage: 0,
            memory_usage: 0,
            disk_utilization: 0,
            network_io: 0,
        }
    }
}

/// load level stats
#[derive(Debug, Clone, Default)]
pub struct LoadLevelStats {
    /// low load duration (seconds)
    pub low_load_duration: u64,
    /// medium load duration (seconds)
    pub medium_load_duration: u64,
    /// high load duration (seconds)
    pub high_load_duration: u64,
    /// critical load duration (seconds)
    pub critical_load_duration: u64,
    /// load transitions
    pub load_transitions: u64,
}

/// advanced IO monitor
pub struct AdvancedIOMonitor {
    /// config
    config: Arc<RwLock<IOMonitorConfig>>,
    /// current metrics
    current_metrics: Arc<RwLock<IOMetrics>>,
    /// history metrics (sliding window)
    history_metrics: Arc<RwLock<VecDeque<IOMetrics>>>,
    /// current load level
    current_load_level: Arc<RwLock<LoadLevel>>,
    /// load level history
    load_level_history: Arc<RwLock<VecDeque<(SystemTime, LoadLevel)>>>,
    /// load level stats
    load_stats: Arc<RwLock<LoadLevelStats>>,
    /// business IO metrics (updated by external)
    business_metrics: Arc<BusinessIOMetrics>,
    /// cancel token
    cancel_token: CancellationToken,
}

/// business IO metrics
pub struct BusinessIOMetrics {
    /// business request latency (milliseconds)
    pub request_latency: AtomicU64,
    /// business request QPS
    pub request_qps: AtomicU64,
    /// business error rate (0-10000, 0.00%-100.00%)
    pub error_rate: AtomicU64,
    /// active connections
    pub active_connections: AtomicU64,
    /// last update time
    pub last_update: Arc<RwLock<SystemTime>>,
}

impl Default for BusinessIOMetrics {
    fn default() -> Self {
        Self {
            request_latency: AtomicU64::new(0),
            request_qps: AtomicU64::new(0),
            error_rate: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            last_update: Arc::new(RwLock::new(SystemTime::UNIX_EPOCH)),
        }
    }
}

impl AdvancedIOMonitor {
    /// create new advanced IO monitor
    pub fn new(config: IOMonitorConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            current_metrics: Arc::new(RwLock::new(IOMetrics::default())),
            history_metrics: Arc::new(RwLock::new(VecDeque::new())),
            current_load_level: Arc::new(RwLock::new(LoadLevel::Low)),
            load_level_history: Arc::new(RwLock::new(VecDeque::new())),
            load_stats: Arc::new(RwLock::new(LoadLevelStats::default())),
            business_metrics: Arc::new(BusinessIOMetrics::default()),
            cancel_token: CancellationToken::new(),
        }
    }

    /// start monitoring
    pub async fn start(&self) -> Result<()> {
        info!("start advanced IO monitor");

        let monitor = self.clone_for_background();
        tokio::spawn(async move {
            if let Err(e) = monitor.monitoring_loop().await {
                error!("IO monitoring loop failed: {}", e);
            }
        });

        Ok(())
    }

    /// stop monitoring
    pub async fn stop(&self) {
        info!("stop IO monitor");
        self.cancel_token.cancel();
    }

    /// monitoring loop
    async fn monitoring_loop(&self) -> Result<()> {
        let mut interval = {
            let config = self.config.read().await;
            tokio::time::interval(config.monitor_interval)
        };

        let mut last_load_level = LoadLevel::Low;
        let mut load_level_start_time = SystemTime::now();

        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    info!("IO monitoring loop cancelled");
                    break;
                }
                _ = interval.tick() => {
                    // collect system metrics
                    let metrics = self.collect_system_metrics().await;

                    // update current metrics
                    *self.current_metrics.write().await = metrics.clone();

                    // update history metrics
                    self.update_metrics_history(metrics.clone()).await;

                    // calculate load level
                    let new_load_level = self.calculate_load_level(&metrics).await;

                    // check if load level changed
                    if new_load_level != last_load_level {
                        self.handle_load_level_change(last_load_level, new_load_level, load_level_start_time).await;
                        last_load_level = new_load_level;
                        load_level_start_time = SystemTime::now();
                    }

                    // update current load level
                    *self.current_load_level.write().await = new_load_level;

                    debug!("IO monitor updated: IOPS={}, queue depth={}, latency={}ms, load level={:?}",
                           metrics.iops, metrics.queue_depth, metrics.avg_latency, new_load_level);
                }
            }
        }

        Ok(())
    }

    /// collect system metrics
    async fn collect_system_metrics(&self) -> IOMetrics {
        let config = self.config.read().await;

        if config.enable_system_monitoring {
            // actual system monitoring implementation
            self.collect_real_system_metrics().await
        } else {
            // simulated data
            self.generate_simulated_metrics().await
        }
    }

    /// collect real system metrics (need to be implemented according to specific system)
    async fn collect_real_system_metrics(&self) -> IOMetrics {
        // TODO: implement actual system metrics collection
        // can use procfs, sysfs or other system API

        let metrics = IOMetrics {
            timestamp: SystemTime::now(),
            ..Default::default()
        };

        // example: read /proc/diskstats
        if let Ok(diskstats) = tokio::fs::read_to_string("/proc/diskstats").await {
            // parse disk stats info
            // here need to implement specific parsing logic
            debug!("read disk stats info: {} bytes", diskstats.len());
        }

        // example: read /proc/stat to get CPU info
        if let Ok(stat) = tokio::fs::read_to_string("/proc/stat").await {
            // parse CPU stats info
            debug!("read CPU stats info: {} bytes", stat.len());
        }

        // example: read /proc/meminfo to get memory info
        if let Ok(meminfo) = tokio::fs::read_to_string("/proc/meminfo").await {
            // parse memory stats info
            debug!("read memory stats info: {} bytes", meminfo.len());
        }

        metrics
    }

    /// generate simulated metrics (for testing and development)
    async fn generate_simulated_metrics(&self) -> IOMetrics {
        use rand::Rng;
        let mut rng = rand::rng();

        // get business metrics impact
        let business_latency = self.business_metrics.request_latency.load(Ordering::Relaxed);
        let business_qps = self.business_metrics.request_qps.load(Ordering::Relaxed);

        // generate simulated system metrics based on business load
        let base_iops = 100 + (business_qps / 10);
        let base_latency = 5 + (business_latency / 10);

        IOMetrics {
            timestamp: SystemTime::now(),
            iops: base_iops + rng.random_range(0..50),
            read_iops: (base_iops * 6 / 10) + rng.random_range(0..20),
            write_iops: (base_iops * 4 / 10) + rng.random_range(0..20),
            queue_depth: rng.random_range(1..20),
            avg_latency: base_latency + rng.random_range(0..10),
            read_latency: base_latency + rng.random_range(0..5),
            write_latency: base_latency + rng.random_range(0..15),
            cpu_usage: rng.random_range(10..70),
            memory_usage: rng.random_range(30..80),
            disk_utilization: rng.random_range(20..90),
            network_io: rng.random_range(10..1000),
        }
    }

    /// update metrics history
    async fn update_metrics_history(&self, metrics: IOMetrics) {
        let mut history = self.history_metrics.write().await;
        let config = self.config.read().await;

        // add new metrics
        history.push_back(metrics);

        // clean expired data
        let retention_cutoff = SystemTime::now() - config.history_retention;
        while let Some(front) = history.front() {
            if front.timestamp < retention_cutoff {
                history.pop_front();
            } else {
                break;
            }
        }

        // limit window size
        while history.len() > config.load_window_size {
            history.pop_front();
        }
    }

    /// calculate load level
    async fn calculate_load_level(&self, metrics: &IOMetrics) -> LoadLevel {
        // multi-dimensional load evaluation algorithm
        let mut load_score = 0u32;

        // IOPS load evaluation (weight: 25%)
        let iops_score = match metrics.iops {
            0..=200 => 0,
            201..=500 => 15,
            501..=1000 => 25,
            _ => 35,
        };
        load_score += iops_score;

        // latency load evaluation (weight: 30%)
        let latency_score = match metrics.avg_latency {
            0..=10 => 0,
            11..=50 => 20,
            51..=100 => 30,
            _ => 40,
        };
        load_score += latency_score;

        // queue depth evaluation (weight: 20%)
        let queue_score = match metrics.queue_depth {
            0..=5 => 0,
            6..=15 => 10,
            16..=30 => 20,
            _ => 25,
        };
        load_score += queue_score;

        // CPU usage evaluation (weight: 15%)
        let cpu_score = match metrics.cpu_usage {
            0..=30 => 0,
            31..=60 => 8,
            61..=80 => 12,
            _ => 15,
        };
        load_score += cpu_score;

        // disk usage evaluation (weight: 10%)
        let disk_score = match metrics.disk_utilization {
            0..=50 => 0,
            51..=75 => 5,
            76..=90 => 8,
            _ => 10,
        };
        load_score += disk_score;

        // business metrics impact
        let business_latency = self.business_metrics.request_latency.load(Ordering::Relaxed);
        let business_error_rate = self.business_metrics.error_rate.load(Ordering::Relaxed);

        if business_latency > 100 {
            load_score += 20; // business latency too high
        }
        if business_error_rate > 100 {
            // > 1%
            load_score += 15; // business error rate too high
        }

        // history trend analysis
        let trend_score = self.calculate_trend_score().await;
        load_score += trend_score;

        // determine load level based on total score
        match load_score {
            0..=30 => LoadLevel::Low,
            31..=60 => LoadLevel::Medium,
            61..=90 => LoadLevel::High,
            _ => LoadLevel::Critical,
        }
    }

    /// calculate trend score
    async fn calculate_trend_score(&self) -> u32 {
        let history = self.history_metrics.read().await;

        if history.len() < 5 {
            return 0; // data insufficient, cannot analyze trend
        }

        // analyze trend of last 5 samples
        let recent: Vec<_> = history.iter().rev().take(5).collect();

        // check IOPS rising trend
        let mut iops_trend = 0;
        for i in 1..recent.len() {
            if recent[i - 1].iops > recent[i].iops {
                iops_trend += 1;
            }
        }

        // check latency rising trend
        let mut latency_trend = 0;
        for i in 1..recent.len() {
            if recent[i - 1].avg_latency > recent[i].avg_latency {
                latency_trend += 1;
            }
        }

        // if IOPS and latency are both rising, increase load score
        if iops_trend >= 3 && latency_trend >= 3 {
            15 // obvious rising trend
        } else if iops_trend >= 2 || latency_trend >= 2 {
            5 // slight rising trend
        } else {
            0 // no obvious trend
        }
    }

    /// handle load level change
    async fn handle_load_level_change(&self, old_level: LoadLevel, new_level: LoadLevel, start_time: SystemTime) {
        let duration = SystemTime::now().duration_since(start_time).unwrap_or(Duration::ZERO);

        // update stats
        {
            let mut stats = self.load_stats.write().await;
            match old_level {
                LoadLevel::Low => stats.low_load_duration += duration.as_secs(),
                LoadLevel::Medium => stats.medium_load_duration += duration.as_secs(),
                LoadLevel::High => stats.high_load_duration += duration.as_secs(),
                LoadLevel::Critical => stats.critical_load_duration += duration.as_secs(),
            }
            stats.load_transitions += 1;
        }

        // update history
        {
            let mut history = self.load_level_history.write().await;
            history.push_back((SystemTime::now(), new_level));

            // keep history record in reasonable range
            while history.len() > 100 {
                history.pop_front();
            }
        }

        info!("load level changed: {:?} -> {:?}, duration: {:?}", old_level, new_level, duration);

        // if enter critical load state, record warning
        if new_level == LoadLevel::Critical {
            warn!("system entered critical load state, Scanner will pause running");
        }
    }

    /// get current load level
    pub async fn get_business_load_level(&self) -> LoadLevel {
        *self.current_load_level.read().await
    }

    /// get current metrics
    pub async fn get_current_metrics(&self) -> IOMetrics {
        self.current_metrics.read().await.clone()
    }

    /// get history metrics
    pub async fn get_history_metrics(&self) -> Vec<IOMetrics> {
        self.history_metrics.read().await.iter().cloned().collect()
    }

    /// get load stats
    pub async fn get_load_stats(&self) -> LoadLevelStats {
        self.load_stats.read().await.clone()
    }

    /// update business IO metrics
    pub async fn update_business_metrics(&self, latency: u64, qps: u64, error_rate: u64, connections: u64) {
        self.business_metrics.request_latency.store(latency, Ordering::Relaxed);
        self.business_metrics.request_qps.store(qps, Ordering::Relaxed);
        self.business_metrics.error_rate.store(error_rate, Ordering::Relaxed);
        self.business_metrics.active_connections.store(connections, Ordering::Relaxed);

        *self.business_metrics.last_update.write().await = SystemTime::now();

        debug!(
            "update business metrics: latency={}ms, QPS={}, error rate={}‰, connections={}",
            latency, qps, error_rate, connections
        );
    }

    /// clone for background task
    fn clone_for_background(&self) -> Self {
        Self {
            config: self.config.clone(),
            current_metrics: self.current_metrics.clone(),
            history_metrics: self.history_metrics.clone(),
            current_load_level: self.current_load_level.clone(),
            load_level_history: self.load_level_history.clone(),
            load_stats: self.load_stats.clone(),
            business_metrics: self.business_metrics.clone(),
            cancel_token: self.cancel_token.clone(),
        }
    }

    /// reset stats
    pub async fn reset_stats(&self) {
        *self.load_stats.write().await = LoadLevelStats::default();
        self.load_level_history.write().await.clear();
        self.history_metrics.write().await.clear();
        info!("IO monitor stats reset");
    }

    /// get load level history
    pub async fn get_load_level_history(&self) -> Vec<(SystemTime, LoadLevel)> {
        self.load_level_history.read().await.iter().cloned().collect()
    }
}
