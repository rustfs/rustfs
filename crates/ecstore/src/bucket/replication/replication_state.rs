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

use crate::bucket::replication::get_global_replication_pool;
use crate::error::Error;
use crate::global::get_global_bucket_monitor;
use rustfs_filemeta::{ReplicatedTargetInfo, ReplicationStatusType, ReplicationType};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;

const ROLLING_WINDOW: Duration = Duration::from_secs(60);
const FAILURE_LAST_HOUR_WINDOW: Duration = Duration::from_secs(60 * 60);

/// Exponential Moving Average with thread-safe interior mutability
#[derive(Debug)]
pub struct ExponentialMovingAverage {
    pub alpha: f64,
    pub value: AtomicU64, // Store f64 as u64 bits
    pub last_update: Arc<Mutex<SystemTime>>,
}

impl ExponentialMovingAverage {
    pub fn new() -> Self {
        let now = SystemTime::now();
        Self {
            alpha: 0.1, // smoothing factor
            value: AtomicU64::new(0_f64.to_bits()),
            last_update: Arc::new(Mutex::new(now)),
        }
    }

    pub fn add_value(&self, value: f64, timestamp: SystemTime) {
        let current_value = f64::from_bits(self.value.load(AtomicOrdering::Relaxed));
        let new_value = if current_value == 0.0 {
            value
        } else {
            self.alpha * value + (1.0 - self.alpha) * current_value
        };
        self.value.store(new_value.to_bits(), AtomicOrdering::Relaxed);

        // Update timestamp (this is async, but we'll use try_lock to avoid blocking)
        if let Ok(mut last_update) = self.last_update.try_lock() {
            *last_update = timestamp;
        }
    }

    pub fn get_current_average(&self) -> f64 {
        f64::from_bits(self.value.load(AtomicOrdering::Relaxed))
    }

    pub fn update_exponential_moving_average(&self, now: SystemTime) {
        if let Ok(mut last_update_guard) = self.last_update.try_lock() {
            let last_update = *last_update_guard;
            if let Ok(duration) = now.duration_since(last_update)
                && duration.as_secs() > 0
            {
                let decay = (-duration.as_secs_f64() / 60.0).exp(); // 1 minute decay
                let current_value = f64::from_bits(self.value.load(AtomicOrdering::Relaxed));
                self.value.store((current_value * decay).to_bits(), AtomicOrdering::Relaxed);
                *last_update_guard = now;
            }
        }
    }

    pub fn merge(&self, other: &ExponentialMovingAverage) -> Self {
        let now = SystemTime::now();
        let self_value = f64::from_bits(self.value.load(AtomicOrdering::Relaxed));
        let other_value = f64::from_bits(other.value.load(AtomicOrdering::Relaxed));
        let merged_value = (self_value + other_value) / 2.0;

        // Get timestamps (use current time as fallback)
        let self_time = self.last_update.try_lock().map(|t| *t).unwrap_or(now);
        let other_time = other.last_update.try_lock().map(|t| *t).unwrap_or(now);
        let merged_time = self_time.max(other_time);

        Self {
            alpha: self.alpha,
            value: AtomicU64::new(merged_value.to_bits()),
            last_update: Arc::new(Mutex::new(merged_time)),
        }
    }
}

impl Clone for ExponentialMovingAverage {
    fn clone(&self) -> Self {
        let now = SystemTime::now();
        let value = self.value.load(AtomicOrdering::Relaxed);
        let last_update = self.last_update.try_lock().map(|t| *t).unwrap_or(now);

        Self {
            alpha: self.alpha,
            value: AtomicU64::new(value),
            last_update: Arc::new(Mutex::new(last_update)),
        }
    }
}

impl Default for ExponentialMovingAverage {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ExponentialMovingAverage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ExponentialMovingAverage", 3)?;
        state.serialize_field("alpha", &self.alpha)?;
        state.serialize_field("value", &f64::from_bits(self.value.load(AtomicOrdering::Relaxed)))?;
        let last_update = self.last_update.try_lock().map(|t| *t).unwrap_or(SystemTime::UNIX_EPOCH);
        state.serialize_field("last_update", &last_update)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for ExponentialMovingAverage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ExponentialMovingAverageData {
            alpha: f64,
            value: f64,
            last_update: SystemTime,
        }

        let data = ExponentialMovingAverageData::deserialize(deserializer)?;
        Ok(Self {
            alpha: data.alpha,
            value: AtomicU64::new(data.value.to_bits()),
            last_update: Arc::new(Mutex::new(data.last_update)),
        })
    }
}

/// Transfer statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XferStats {
    pub avg: f64,
    pub curr: f64,
    pub peak: f64,
    pub measure: ExponentialMovingAverage,
}

impl XferStats {
    pub fn new() -> Self {
        Self {
            avg: 0.0,
            curr: 0.0,
            peak: 0.0,
            measure: ExponentialMovingAverage::new(),
        }
    }

    pub fn add_size(&mut self, size: i64, duration: Duration) {
        if duration.as_nanos() > 0 {
            let rate = (size as f64) / duration.as_secs_f64();
            self.curr = rate;
            if rate > self.peak {
                self.peak = rate;
            }
            self.measure.add_value(rate, SystemTime::now());
            self.avg = self.measure.get_current_average();
        }
    }

    pub fn clone_stats(&self) -> Self {
        Self {
            avg: self.avg,
            curr: self.curr,
            peak: self.peak,
            measure: self.measure.clone(),
        }
    }

    pub fn merge(&self, other: &XferStats) -> Self {
        Self {
            avg: (self.avg + other.avg) / 2.0,
            curr: self.curr + other.curr,
            peak: self.peak.max(other.peak),
            measure: self.measure.merge(&other.measure),
        }
    }

    pub fn update_exponential_moving_average(&mut self, now: SystemTime) {
        self.measure.update_exponential_moving_average(now);
        self.avg = self.measure.get_current_average();
    }
}

impl Default for XferStats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ReplStat {
    pub arn: String,
    pub completed: bool,
    pub pending: bool,
    pub failed: bool,
    pub op_type: ReplicationType,
    pub transfer_size: i64,
    pub transfer_duration: Duration,
    pub endpoint: String,
    pub secure: bool,
    pub err: Option<Error>,
}

impl ReplStat {
    pub fn new() -> Self {
        Self {
            arn: String::new(),
            completed: false,
            pending: false,
            failed: false,
            op_type: ReplicationType::default(),
            transfer_size: 0,
            transfer_duration: Duration::default(),
            endpoint: String::new(),
            secure: false,
            err: None,
        }
    }

    pub fn endpoint(&self) -> String {
        let scheme = if self.secure { "https" } else { "http" };
        format!("{}://{}", scheme, self.endpoint)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn set(
        &mut self,
        arn: String,
        size: i64,
        duration: Duration,
        status: ReplicationStatusType,
        op_type: ReplicationType,
        endpoint: String,
        secure: bool,
        err: Option<Error>,
    ) {
        self.arn = arn;
        self.transfer_size = size;
        self.transfer_duration = duration;
        self.op_type = op_type;
        self.endpoint = endpoint;
        self.secure = secure;
        self.err = err;

        // Reset status
        self.completed = false;
        self.pending = false;
        self.failed = false;

        match status {
            ReplicationStatusType::Completed => self.completed = true,
            ReplicationStatusType::Pending => self.pending = true,
            ReplicationStatusType::Failed => self.failed = true,
            _ => {}
        }
    }
}

impl Default for ReplStat {
    fn default() -> Self {
        Self::new()
    }
}

/// Site replication statistics
#[derive(Debug, Default)]
pub struct SRStats {
    pub replica_size: AtomicI64,
    pub replica_count: AtomicI64,
    // More site replication related statistics fields can be added here
}

impl SRStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&self, rs: &ReplStat, _depl_id: &str) {
        // Update site replication statistics
        // In actual implementation, statistics would be updated based on deployment ID
        if rs.completed {
            self.replica_size.fetch_add(rs.transfer_size, Ordering::Relaxed);
            self.replica_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn get(&self) -> HashMap<String, i64> {
        // Return current statistics
        let mut stats = HashMap::new();
        stats.insert("replica_size".to_string(), self.replica_size.load(Ordering::Relaxed));
        stats.insert("replica_count".to_string(), self.replica_count.load(Ordering::Relaxed));
        stats
    }
}

/// Statistics in queue
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct InQueueStats {
    pub bytes: i64,
    pub count: i64,
    #[serde(skip)]
    pub now_bytes: AtomicI64,
    #[serde(skip)]
    pub now_count: AtomicI64,
}

#[derive(Debug, Clone)]
struct QueueSample {
    observed_at: Instant,
    bytes: i64,
    count: i64,
}

impl Clone for InQueueStats {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes,
            count: self.count,
            now_bytes: AtomicI64::new(self.now_bytes.load(Ordering::Relaxed)),
            now_count: AtomicI64::new(self.now_count.load(Ordering::Relaxed)),
        }
    }
}

impl InQueueStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_current_bytes(&self) -> i64 {
        self.now_bytes.load(Ordering::Relaxed)
    }

    pub fn get_current_count(&self) -> i64 {
        self.now_count.load(Ordering::Relaxed)
    }
}

/// Metrics in queue
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InQueueMetric {
    pub curr: InQueueStats,
    pub avg: InQueueStats,
    pub max: InQueueStats,
    pub last_minute: InQueueStats,
    #[serde(skip)]
    samples: VecDeque<QueueSample>,
}

impl InQueueMetric {
    fn observe(&mut self, observed_at: Instant) {
        let bytes = self.curr.now_bytes.load(Ordering::Relaxed);
        let count = self.curr.now_count.load(Ordering::Relaxed);

        self.curr.bytes = bytes;
        self.curr.count = count;
        self.samples.push_back(QueueSample {
            observed_at,
            bytes,
            count,
        });

        while self
            .samples
            .front()
            .is_some_and(|sample| observed_at.duration_since(sample.observed_at) > ROLLING_WINDOW)
        {
            self.samples.pop_front();
        }

        if self.samples.is_empty() {
            self.avg = InQueueStats::default();
            self.max = InQueueStats::default();
            self.last_minute = InQueueStats::default();
            return;
        }

        let sample_count = self.samples.len() as i64;
        let total_bytes = self.samples.iter().map(|sample| sample.bytes).sum::<i64>();
        let total_count = self.samples.iter().map(|sample| sample.count).sum::<i64>();
        let max_bytes = self.samples.iter().map(|sample| sample.bytes).max().unwrap_or(0);
        let max_count = self.samples.iter().map(|sample| sample.count).max().unwrap_or(0);

        self.avg.bytes = total_bytes / sample_count;
        self.avg.count = total_count / sample_count;
        self.max.bytes = max_bytes;
        self.max.count = max_count;
        self.last_minute.bytes = self.avg.bytes;
        self.last_minute.count = self.avg.count;
    }

    fn snapshot(&self) -> Self {
        let mut snapshot = self.clone();
        snapshot.curr.bytes = snapshot.curr.now_bytes.load(Ordering::Relaxed);
        snapshot.curr.count = snapshot.curr.now_count.load(Ordering::Relaxed);
        snapshot
    }

    pub fn merge(&self, other: &InQueueMetric) -> Self {
        Self {
            curr: InQueueStats {
                bytes: self.curr.bytes + other.curr.bytes,
                count: self.curr.count + other.curr.count,
                now_bytes: AtomicI64::new(
                    self.curr.now_bytes.load(Ordering::Relaxed) + other.curr.now_bytes.load(Ordering::Relaxed),
                ),
                now_count: AtomicI64::new(
                    self.curr.now_count.load(Ordering::Relaxed) + other.curr.now_count.load(Ordering::Relaxed),
                ),
            },
            avg: InQueueStats {
                bytes: (self.avg.bytes + other.avg.bytes) / 2,
                count: (self.avg.count + other.avg.count) / 2,
                ..Default::default()
            },
            max: InQueueStats {
                bytes: self.max.bytes.max(other.max.bytes),
                count: self.max.count.max(other.max.count),
                ..Default::default()
            },
            last_minute: InQueueStats {
                bytes: self.last_minute.bytes + other.last_minute.bytes,
                count: self.last_minute.count + other.last_minute.count,
                ..Default::default()
            },
            samples: VecDeque::new(),
        }
    }
}

/// Queue cache
#[derive(Debug, Default)]
pub struct QueueCache {
    pub bucket_stats: HashMap<String, InQueueMetric>,
    pub sr_queue_stats: InQueueMetric,
}

impl QueueCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self) {
        let observed_at = Instant::now();
        self.sr_queue_stats.observe(observed_at);
        for stats in self.bucket_stats.values_mut() {
            stats.observe(observed_at);
        }
    }

    pub fn get_bucket_stats(&self, bucket: &str) -> InQueueMetric {
        self.bucket_stats.get(bucket).map(InQueueMetric::snapshot).unwrap_or_default()
    }

    pub fn get_site_stats(&self) -> InQueueMetric {
        self.sr_queue_stats.snapshot()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProxyMetric {
    pub get_total: i64,
    pub get_failed: i64,
    pub put_total: i64,
    pub put_failed: i64,
    pub put_tag_total: i64,
    pub put_tag_failed: i64,
    pub head_total: i64,
    pub head_failed: i64,
}

impl ProxyMetric {
    pub fn add(&mut self, other: &ProxyMetric) {
        self.get_total += other.get_total;
        self.get_failed += other.get_failed;
        self.put_total += other.put_total;
        self.put_failed += other.put_failed;
        self.put_tag_total += other.put_tag_total;
        self.put_tag_failed += other.put_tag_failed;
        self.head_total += other.head_total;
        self.head_failed += other.head_failed;
    }
}

/// Proxy statistics cache
#[derive(Debug, Clone, Default)]
pub struct ProxyStatsCache {
    bucket_stats: HashMap<String, ProxyMetric>,
}

impl ProxyStatsCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc(&mut self, bucket: &str, api: &str, is_err: bool) {
        let metric = self.bucket_stats.entry(bucket.to_string()).or_default();

        match api {
            "GetObject" => {
                metric.get_total += 1;
                if is_err {
                    metric.get_failed += 1;
                }
            }
            "PutObject" => {
                metric.put_total += 1;
                if is_err {
                    metric.put_failed += 1;
                }
            }
            "PutObjectTagging" => {
                metric.put_tag_total += 1;
                if is_err {
                    metric.put_tag_failed += 1;
                }
            }
            "HeadObject" => {
                metric.head_total += 1;
                if is_err {
                    metric.head_failed += 1;
                }
            }
            _ => {}
        }
    }

    pub fn get_bucket_stats(&self, bucket: &str) -> ProxyMetric {
        self.bucket_stats.get(bucket).cloned().unwrap_or_default()
    }

    pub fn get_site_stats(&self) -> ProxyMetric {
        let mut total = ProxyMetric::default();
        for metric in self.bucket_stats.values() {
            total.add(metric);
        }
        total
    }
}

#[derive(Debug, Clone)]
struct FailureSample {
    observed_at: Instant,
    size: i64,
}

/// Failure statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FailStats {
    pub count: i64,
    pub size: i64,
    #[serde(skip)]
    recent: VecDeque<FailureSample>,
}

impl FailStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_size(&mut self, size: i64, _err: Option<&Error>) {
        let observed_at = Instant::now();
        self.count += 1;
        self.size += size;
        self.recent.push_back(FailureSample { observed_at, size });
        self.prune(observed_at);
    }

    fn prune(&mut self, observed_at: Instant) {
        while self
            .recent
            .front()
            .is_some_and(|sample| observed_at.duration_since(sample.observed_at) > FAILURE_LAST_HOUR_WINDOW)
        {
            self.recent.pop_front();
        }
    }

    pub fn recent_since(&self, window: Duration) -> FailedMetric {
        let now = Instant::now();
        let mut count = 0i64;
        let mut size = 0i64;
        for sample in self.recent.iter().rev() {
            if now.duration_since(sample.observed_at) > window {
                break;
            }
            count += 1;
            size += sample.size;
        }
        FailedMetric { count, size }
    }

    pub fn merge(&self, other: &FailStats) -> Self {
        Self {
            count: self.count + other.count,
            size: self.size + other.size,
            recent: VecDeque::new(),
        }
    }

    pub fn to_metric(&self) -> FailedMetric {
        FailedMetric {
            count: self.count,
            size: self.size,
        }
    }
}

/// Failed metric
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FailedMetric {
    pub count: i64,
    pub size: i64,
}

/// Latency statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyStats {
    pub avg: f64,
    pub curr: f64,
    pub max: f64,
}

impl LatencyStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, _size: i64, duration: Duration) {
        let latency = duration.as_millis() as f64;
        self.curr = latency;
        if latency > self.max {
            self.max = latency;
        }
        // Simple moving average (simplified implementation)
        self.avg = (self.avg + latency) / 2.0;
    }

    pub fn merge(&self, other: &LatencyStats) -> Self {
        Self {
            avg: (self.avg + other.avg) / 2.0,
            curr: self.curr.max(other.curr),
            max: self.max.max(other.max),
        }
    }
}

/// Bucket replication statistics for a single target
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketReplicationStat {
    pub replicated_size: i64,
    pub replicated_count: i64,
    pub failed: FailedMetric,
    pub fail_stats: FailStats,
    pub latency: LatencyStats,
    pub xfer_rate_lrg: XferStats,
    pub xfer_rate_sml: XferStats,
    pub bandwidth_limit_bytes_per_sec: i64,
    pub current_bandwidth_bytes_per_sec: f64,
}

impl BucketReplicationStat {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_xfer_rate(&mut self, size: i64, duration: Duration) {
        // Classify as large or small transfer based on size
        if size > 1024 * 1024 {
            // > 1MB
            self.xfer_rate_lrg.add_size(size, duration);
        } else {
            self.xfer_rate_sml.add_size(size, duration);
        }
    }
}

/// Queue statistics for nodes
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueStats {
    pub nodes: Vec<QueueNode>,
}

/// Queue node statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueNode {
    pub q_stats: InQueueMetric,
}

/// Bucket replication statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketReplicationStats {
    pub stats: HashMap<String, BucketReplicationStat>,
    pub replica_size: i64,
    pub replica_count: i64,
    pub replicated_size: i64,
    pub replicated_count: i64,
    pub q_stat: InQueueMetric,
}

impl BucketReplicationStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.stats.is_empty() && self.replica_size == 0 && self.replicated_size == 0
    }

    pub fn has_replication_usage(&self) -> bool {
        self.replica_size > 0 || self.replicated_size > 0 || !self.stats.is_empty()
    }

    pub fn clone_stats(&self) -> Self {
        self.clone()
    }
}

/// Bucket statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketStats {
    pub uptime: i64,
    pub replication_stats: BucketReplicationStats,
    pub queue_stats: QueueStats,
    pub proxy_stats: ProxyMetric,
}

/// Site replication metrics summary
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SRMetricsSummary {
    pub uptime: i64,
    pub queued: InQueueMetric,
    pub active_workers: ActiveWorkerStat,
    pub metrics: HashMap<String, i64>,
    pub proxied: ProxyMetric,
    pub replica_size: i64,
    pub replica_count: i64,
}

/// Active worker statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActiveWorkerStat {
    pub curr: i32,
    pub max: i32,
    pub avg: f64,
    #[serde(skip)]
    samples: VecDeque<WorkerSample>,
}

#[derive(Debug, Clone)]
struct WorkerSample {
    observed_at: Instant,
    workers: i32,
}

impl ActiveWorkerStat {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self) -> Self {
        self.clone()
    }

    pub fn update(&mut self, curr: i32) {
        let observed_at = Instant::now();
        self.curr = curr;
        self.samples.push_back(WorkerSample {
            observed_at,
            workers: curr,
        });

        while self
            .samples
            .front()
            .is_some_and(|sample| observed_at.duration_since(sample.observed_at) > ROLLING_WINDOW)
        {
            self.samples.pop_front();
        }

        if self.samples.is_empty() {
            self.max = curr;
            self.avg = curr as f64;
            return;
        }

        self.max = self.samples.iter().map(|sample| sample.workers).max().unwrap_or(curr);
        let total = self.samples.iter().map(|sample| sample.workers as i64).sum::<i64>();
        self.avg = total as f64 / self.samples.len() as f64;
    }
}

/// Global replication statistics
#[derive(Debug)]
pub struct ReplicationStats {
    // Site replication statistics - maintain global level statistics
    pub sr_stats: Arc<SRStats>,
    // Active worker statistics
    pub workers: Arc<Mutex<ActiveWorkerStat>>,
    // Queue statistics cache
    pub q_cache: Arc<Mutex<QueueCache>>,
    // Proxy statistics cache
    pub p_cache: Arc<Mutex<ProxyStatsCache>>,
    // MRF backlog statistics (simplified)
    pub mrf_stats: HashMap<String, i64>,
    // Bucket replication cache
    pub cache: Arc<RwLock<HashMap<String, BucketReplicationStats>>>,
    pub most_recent_stats: Arc<Mutex<HashMap<String, BucketStats>>>,
}

impl ReplicationStats {
    pub fn new() -> Self {
        Self {
            sr_stats: Arc::new(SRStats::new()),
            workers: Arc::new(Mutex::new(ActiveWorkerStat::new())),
            q_cache: Arc::new(Mutex::new(QueueCache::new())),
            p_cache: Arc::new(Mutex::new(ProxyStatsCache::new())),
            mrf_stats: HashMap::new(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            most_recent_stats: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Initialize background tasks
    pub async fn start_background_tasks(&self) {
        // Start moving average calculation task
        let cache_clone = Arc::clone(&self.cache);
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                Self::update_moving_avg_static(&cache_clone).await;
            }
        });

        // Start worker statistics collection task
        let workers_clone = Arc::clone(&self.workers);
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(2));
            loop {
                interval.tick().await;
                let current = get_global_replication_pool()
                    .map(|pool| pool.active_workers() + pool.active_lrg_workers() + pool.active_mrf_workers())
                    .unwrap_or(0);
                let mut workers = workers_clone.lock().await;
                workers.update(current);
            }
        });

        // Start queue statistics collection task
        let q_cache_clone = Arc::clone(&self.q_cache);
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(2));
            loop {
                interval.tick().await;
                let mut cache = q_cache_clone.lock().await;
                cache.update();
            }
        });
    }

    async fn update_moving_avg_static(cache: &Arc<RwLock<HashMap<String, BucketReplicationStats>>>) {
        // This is a simplified implementation
        // In actual implementation, exponential moving averages need to be updated
        let now = SystemTime::now();

        let cache_read = cache.read().await;
        for (_bucket, stats) in cache_read.iter() {
            for stat in stats.stats.values() {
                // Now we can update the moving averages using interior mutability
                stat.xfer_rate_lrg.measure.update_exponential_moving_average(now);
                stat.xfer_rate_sml.measure.update_exponential_moving_average(now);
            }
        }
    }

    /// Check if bucket replication statistics have usage
    pub fn has_replication_usage(&self, bucket: &str) -> bool {
        if let Ok(cache) = self.cache.try_read()
            && let Some(stats) = cache.get(bucket)
        {
            return stats.has_replication_usage();
        }
        false
    }

    /// Get active worker statistics
    pub fn active_workers(&self) -> ActiveWorkerStat {
        // This should be called from an async context
        // For now, use try_lock to avoid blocking
        self.workers.try_lock().map(|w| w.get()).unwrap_or_default()
    }

    /// Delete bucket's memory replication statistics
    pub async fn delete(&self, bucket: &str) {
        let mut cache = self.cache.write().await;
        cache.remove(bucket);
    }

    /// Update replica statistics
    pub async fn update_replica_stat(&self, bucket: &str, size: i64) {
        let mut cache = self.cache.write().await;
        let stats = cache.entry(bucket.to_string()).or_insert_with(BucketReplicationStats::new);

        stats.replica_size += size;
        stats.replica_count += 1;

        // Update site replication statistics
        self.sr_stats.replica_size.fetch_add(size, Ordering::Relaxed);
        self.sr_stats.replica_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Site replication update replica statistics
    fn sr_update_replica_stat(&self, size: i64) {
        self.sr_stats.replica_size.fetch_add(size, Ordering::Relaxed);
        self.sr_stats.replica_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Site replication update
    fn sr_update(&self, rs: &ReplStat) {
        // In actual implementation, deployment ID would be obtained here
        let depl_id = "default"; // simplified implementation
        self.sr_stats.update(rs, depl_id);
    }

    /// Update replication statistics
    pub async fn update(
        &self,
        bucket: &str,
        ri: &ReplicatedTargetInfo,
        status: ReplicationStatusType,
        prev_status: ReplicationStatusType,
    ) {
        let mut rs = ReplStat::new();

        match status {
            ReplicationStatusType::Pending if ri.op_type.is_data_replication() && prev_status != status => {
                rs.set(
                    ri.arn.clone(),
                    ri.size,
                    Duration::default(),
                    status,
                    ri.op_type,
                    ri.endpoint.clone(),
                    ri.secure,
                    ri.error.as_ref().map(|e| crate::error::Error::other(e.clone())),
                );
            }
            ReplicationStatusType::Completed if ri.op_type.is_data_replication() => {
                rs.set(
                    ri.arn.clone(),
                    ri.size,
                    ri.duration,
                    status,
                    ri.op_type,
                    ri.endpoint.clone(),
                    ri.secure,
                    ri.error.as_ref().map(|e| crate::error::Error::other(e.clone())),
                );
            }
            ReplicationStatusType::Failed
                if ri.op_type.is_data_replication() && prev_status == ReplicationStatusType::Pending =>
            {
                rs.set(
                    ri.arn.clone(),
                    ri.size,
                    ri.duration,
                    status,
                    ri.op_type,
                    ri.endpoint.clone(),
                    ri.secure,
                    ri.error.as_ref().map(|e| crate::error::Error::other(e.clone())),
                );
            }
            ReplicationStatusType::Replica if ri.op_type == ReplicationType::Object => {
                rs.set(
                    ri.arn.clone(),
                    ri.size,
                    Duration::default(),
                    status,
                    ri.op_type,
                    String::new(),
                    false,
                    ri.error.as_ref().map(|e| crate::error::Error::other(e.clone())),
                );
            }
            _ => {}
        }

        // Update site replication memory statistics
        if rs.completed || rs.failed {
            self.sr_update(&rs);
        }

        // Update bucket replication memory statistics
        let mut cache = self.cache.write().await;
        let bucket_stats = cache.entry(bucket.to_string()).or_insert_with(BucketReplicationStats::new);

        let stat = bucket_stats
            .stats
            .entry(ri.arn.clone())
            .or_insert_with(|| BucketReplicationStat {
                xfer_rate_lrg: XferStats::new(),
                xfer_rate_sml: XferStats::new(),
                ..Default::default()
            });

        match (rs.completed, rs.failed, rs.pending) {
            (true, false, false) => {
                stat.replicated_size += rs.transfer_size;
                stat.replicated_count += 1;
                if rs.transfer_duration > Duration::default() {
                    stat.latency.update(rs.transfer_size, rs.transfer_duration);
                    stat.update_xfer_rate(rs.transfer_size, rs.transfer_duration);
                }
            }
            (false, true, false) => {
                stat.fail_stats.add_size(rs.transfer_size, rs.err.as_ref());
            }
            (false, false, true) => {
                // Pending status, no processing for now
            }
            _ => {}
        }
    }

    /// Get replication metrics for all buckets
    pub async fn get_all(&self) -> HashMap<String, BucketReplicationStats> {
        let cache = self.cache.read().await;
        let mut result = HashMap::with_capacity(cache.len());

        for (bucket, stats) in cache.iter() {
            result.insert(bucket.clone(), stats.clone_stats());
        }
        drop(cache);

        {
            let q_cache = self.q_cache.lock().await;
            for (bucket, queue_stats) in &q_cache.bucket_stats {
                let bucket_stats = result.entry(bucket.clone()).or_insert_with(BucketReplicationStats::new);
                bucket_stats.q_stat = queue_stats.snapshot();
            }
        }

        {
            let p_cache = self.p_cache.lock().await;
            for bucket in p_cache.bucket_stats.keys() {
                result.entry(bucket.clone()).or_insert_with(BucketReplicationStats::new);
            }
        }

        result
    }

    /// Get replication metrics for a single bucket
    pub async fn get(&self, bucket: &str) -> BucketReplicationStats {
        let cache = self.cache.read().await;
        if let Some(stats) = cache.get(bucket) {
            stats.clone_stats()
        } else {
            BucketReplicationStats::new()
        }
    }

    /// Get metrics summary for site replication node
    pub async fn get_sr_metrics_for_node(&self) -> SRMetricsSummary {
        let boot_time = SystemTime::UNIX_EPOCH; // simplified implementation
        let uptime = SystemTime::now().duration_since(boot_time).unwrap_or_default().as_secs() as i64;

        let q_cache = self.q_cache.lock().await;
        let queued = q_cache.get_site_stats();

        let p_cache = self.p_cache.lock().await;
        let proxied = p_cache.get_site_stats();

        SRMetricsSummary {
            uptime,
            queued,
            active_workers: self.active_workers(),
            metrics: self.sr_stats.get(),
            proxied,
            replica_size: self.sr_stats.replica_size.load(Ordering::Relaxed),
            replica_count: self.sr_stats.replica_count.load(Ordering::Relaxed),
        }
    }

    /// Calculate bucket replication statistics
    pub async fn calculate_bucket_replication_stats(&self, bucket: &str, bucket_stats: Vec<BucketStats>) -> BucketStats {
        if bucket_stats.is_empty() {
            return BucketStats {
                uptime: 0,
                replication_stats: BucketReplicationStats::new(),
                queue_stats: Default::default(),
                proxy_stats: ProxyMetric::default(),
            };
        }

        // Accumulate cluster bucket statistics
        let mut stats = HashMap::new();
        let mut tot_replica_size = 0i64;
        let mut tot_replica_count = 0i64;
        let mut tot_replicated_size = 0i64;
        let mut tot_replicated_count = 0i64;
        let mut tq = InQueueMetric::default();

        for bucket_stat in &bucket_stats {
            tot_replica_size += bucket_stat.replication_stats.replica_size;
            tot_replica_count += bucket_stat.replication_stats.replica_count;

            for q in &bucket_stat.queue_stats.nodes {
                tq = tq.merge(&q.q_stats);
            }

            for (arn, stat) in &bucket_stat.replication_stats.stats {
                let old_stat = stats.entry(arn.clone()).or_insert_with(|| BucketReplicationStat {
                    xfer_rate_lrg: XferStats::new(),
                    xfer_rate_sml: XferStats::new(),
                    ..Default::default()
                });

                let f_stats = stat.fail_stats.merge(&old_stat.fail_stats);
                let lrg = old_stat.xfer_rate_lrg.merge(&stat.xfer_rate_lrg);
                let sml = old_stat.xfer_rate_sml.merge(&stat.xfer_rate_sml);

                *old_stat = BucketReplicationStat {
                    failed: f_stats.to_metric(),
                    fail_stats: f_stats,
                    replicated_size: stat.replicated_size + old_stat.replicated_size,
                    replicated_count: stat.replicated_count + old_stat.replicated_count,
                    latency: stat.latency.merge(&old_stat.latency),
                    xfer_rate_lrg: lrg,
                    xfer_rate_sml: sml,
                    bandwidth_limit_bytes_per_sec: stat.bandwidth_limit_bytes_per_sec,
                    current_bandwidth_bytes_per_sec: stat.current_bandwidth_bytes_per_sec
                        + old_stat.current_bandwidth_bytes_per_sec,
                };

                tot_replicated_size += stat.replicated_size;
                tot_replicated_count += stat.replicated_count;
            }
        }

        let s = BucketReplicationStats {
            stats,
            q_stat: tq,
            replica_size: tot_replica_size,
            replica_count: tot_replica_count,
            replicated_size: tot_replicated_size,
            replicated_count: tot_replicated_count,
        };

        let qs = Default::default();
        let mut ps = ProxyMetric::default();

        for bs in &bucket_stats {
            // qs.nodes.extend(bs.queue_stats.nodes.clone()); // simplified implementation
            ps.add(&bs.proxy_stats);
        }

        let uptime = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let bs = BucketStats {
            uptime,
            replication_stats: s,
            queue_stats: qs,
            proxy_stats: ps,
        };

        // Update recent statistics
        let mut recent_stats = self.most_recent_stats.lock().await;
        if !bs.replication_stats.stats.is_empty() {
            recent_stats.insert(bucket.to_string(), bs.clone());
        }

        bs
    }

    /// Get latest replication statistics
    pub async fn get_latest_replication_stats(&self, bucket: &str) -> BucketStats {
        // In actual implementation, statistics would be obtained from cluster
        // This is simplified to get from local cache
        let cache = self.cache.read().await;
        let mut replication_stats = if let Some(stats) = cache.get(bucket) {
            stats.clone_stats()
        } else {
            BucketReplicationStats::new()
        };
        let uptime = if cache.contains_key(bucket) {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64
        } else {
            0
        };
        drop(cache);

        if let Some(monitor) = get_global_bucket_monitor() {
            let bw_report = monitor.get_report(|name| name == bucket);
            for (opts, bw) in bw_report.bucket_stats {
                let stat = replication_stats
                    .stats
                    .entry(opts.replication_arn)
                    .or_insert_with(|| BucketReplicationStat {
                        xfer_rate_lrg: XferStats::new(),
                        xfer_rate_sml: XferStats::new(),
                        ..Default::default()
                    });
                stat.bandwidth_limit_bytes_per_sec = bw.limit_bytes_per_sec;
                stat.current_bandwidth_bytes_per_sec = bw.current_bandwidth_bytes_per_sec;
            }
        }

        BucketStats {
            uptime,
            replication_stats,
            queue_stats: Default::default(),
            proxy_stats: ProxyMetric::default(),
        }
    }

    /// Increase queue statistics
    pub async fn inc_q(&self, bucket: &str, size: i64, _is_delete_repl: bool, _op_type: ReplicationType) {
        let mut q_cache = self.q_cache.lock().await;
        let stats = q_cache
            .bucket_stats
            .entry(bucket.to_string())
            .or_insert_with(InQueueMetric::default);
        stats.curr.now_bytes.fetch_add(size, Ordering::Relaxed);
        stats.curr.now_count.fetch_add(1, Ordering::Relaxed);

        q_cache.sr_queue_stats.curr.now_bytes.fetch_add(size, Ordering::Relaxed);
        q_cache.sr_queue_stats.curr.now_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrease queue statistics
    pub async fn dec_q(&self, bucket: &str, size: i64, _is_del_marker: bool, _op_type: ReplicationType) {
        let mut q_cache = self.q_cache.lock().await;
        let stats = q_cache
            .bucket_stats
            .entry(bucket.to_string())
            .or_insert_with(InQueueMetric::default);
        stats.curr.now_bytes.fetch_sub(size, Ordering::Relaxed);
        stats.curr.now_count.fetch_sub(1, Ordering::Relaxed);

        q_cache.sr_queue_stats.curr.now_bytes.fetch_sub(size, Ordering::Relaxed);
        q_cache.sr_queue_stats.curr.now_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Increase proxy metrics
    pub async fn inc_proxy(&self, bucket: &str, api: &str, is_err: bool) {
        let mut p_cache = self.p_cache.lock().await;
        p_cache.inc(bucket, api, is_err);
    }

    /// Get proxy statistics
    pub async fn get_proxy_stats(&self, bucket: &str) -> ProxyMetric {
        let p_cache = self.p_cache.lock().await;
        p_cache.get_bucket_stats(bucket)
    }
}

impl Default for ReplicationStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_stats_new() {
        let stats = ReplicationStats::new();
        let workers = stats.active_workers();
        assert_eq!(workers.curr, 0);
    }

    #[test]
    fn test_in_queue_metric_observe_updates_rolling_stats() {
        let mut metric = InQueueMetric::default();
        metric.curr.now_bytes.store(128, Ordering::Relaxed);
        metric.curr.now_count.store(4, Ordering::Relaxed);
        metric.observe(Instant::now());

        metric.curr.now_bytes.store(256, Ordering::Relaxed);
        metric.curr.now_count.store(6, Ordering::Relaxed);
        metric.observe(Instant::now());

        assert_eq!(metric.curr.bytes, 256);
        assert_eq!(metric.curr.count, 6);
        assert_eq!(metric.max.bytes, 256);
        assert_eq!(metric.max.count, 6);
        assert_eq!(metric.last_minute.bytes, 192);
        assert_eq!(metric.last_minute.count, 5);
    }

    #[test]
    fn test_fail_stats_recent_since_tracks_windows() {
        let mut stats = FailStats::default();
        stats.add_size(64, None);
        stats.add_size(32, None);

        let last_minute = stats.recent_since(Duration::from_secs(60));
        let last_hour = stats.recent_since(Duration::from_secs(60 * 60));
        assert_eq!(last_minute.count, 2);
        assert_eq!(last_minute.size, 96);
        assert_eq!(last_hour.count, 2);
        assert_eq!(last_hour.size, 96);
    }

    #[test]
    fn test_active_worker_stat_update_tracks_rolling_avg_and_max() {
        let mut stats = ActiveWorkerStat::default();
        stats.update(2);
        stats.update(6);
        stats.update(4);

        assert_eq!(stats.curr, 4);
        assert_eq!(stats.max, 6);
        assert_eq!(stats.avg, 4.0);
    }

    #[tokio::test]
    async fn test_delete_bucket_stats() {
        let stats = ReplicationStats::new();
        stats.delete("test-bucket").await;

        let bucket_stats = stats.get("test-bucket").await;
        assert!(bucket_stats.is_empty());
    }

    #[tokio::test]
    async fn test_update_replica_stat() {
        let stats = ReplicationStats::new();
        stats.update_replica_stat("test-bucket", 1024).await;

        let bucket_stats = stats.get("test-bucket").await;
        assert_eq!(bucket_stats.replica_size, 1024);
        assert_eq!(bucket_stats.replica_count, 1);
    }

    #[tokio::test]
    async fn test_replication_stats_update() {
        let stats = ReplicationStats::new();

        let target_info = ReplicatedTargetInfo {
            arn: "test-arn".to_string(),
            size: 1024,
            duration: Duration::from_secs(1),
            op_type: ReplicationType::Object,
            endpoint: "test.example.com".to_string(),
            secure: true,
            error: None,
            ..Default::default()
        };

        stats
            .update(
                "test-bucket",
                &target_info,
                ReplicationStatusType::Completed,
                ReplicationStatusType::Pending,
            )
            .await;

        let bucket_stats = stats.get("test-bucket").await;
        assert!(!bucket_stats.is_empty());
        assert!(bucket_stats.stats.contains_key("test-arn"));

        let stat = &bucket_stats.stats["test-arn"];
        assert_eq!(stat.replicated_size, 1024);
        assert_eq!(stat.replicated_count, 1);
    }

    #[tokio::test]
    async fn test_get_all_includes_proxy_only_bucket() {
        let stats = ReplicationStats::new();
        stats.inc_proxy("proxy-only-bucket", "HeadObject", false).await;

        let all = stats.get_all().await;
        assert!(all.contains_key("proxy-only-bucket"));
    }

    #[test]
    fn test_sr_stats() {
        let sr_stats = SRStats::new();
        let initial_size = sr_stats.replica_size.load(Ordering::Relaxed);
        let initial_count = sr_stats.replica_count.load(Ordering::Relaxed);

        assert_eq!(initial_size, 0);
        assert_eq!(initial_count, 0);

        let stats_map = sr_stats.get();
        assert_eq!(stats_map["replica_size"], 0);
        assert_eq!(stats_map["replica_count"], 0);
    }
}
