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

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering, Ordering as AtomicOrdering};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::Mutex;

const ROLLING_WINDOW: Duration = Duration::from_secs(60);
const FAILURE_LAST_HOUR_WINDOW: Duration = Duration::from_secs(60 * 60);

#[derive(Debug)]
pub struct ExponentialMovingAverage {
    pub alpha: f64,
    pub value: AtomicU64,
    pub last_update: Arc<Mutex<SystemTime>>,
}

impl ExponentialMovingAverage {
    pub fn new() -> Self {
        let now = SystemTime::now();
        Self {
            alpha: 0.1,
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
                let decay = (-duration.as_secs_f64() / 60.0).exp();
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

    pub fn snapshot(&self) -> Self {
        let mut snapshot = self.clone();
        snapshot.samples.clear();
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
    pub get_tag_total: i64,
    pub get_tag_failed: i64,
    pub put_total: i64,
    pub put_failed: i64,
    pub put_tag_total: i64,
    pub put_tag_failed: i64,
    pub delete_tag_total: i64,
    pub delete_tag_failed: i64,
    pub head_total: i64,
    pub head_failed: i64,
}

impl ProxyMetric {
    pub fn add(&mut self, other: &ProxyMetric) {
        self.get_total += other.get_total;
        self.get_failed += other.get_failed;
        self.get_tag_total += other.get_tag_total;
        self.get_tag_failed += other.get_tag_failed;
        self.put_total += other.put_total;
        self.put_failed += other.put_failed;
        self.put_tag_total += other.put_tag_total;
        self.put_tag_failed += other.put_tag_failed;
        self.delete_tag_total += other.delete_tag_total;
        self.delete_tag_failed += other.delete_tag_failed;
        self.head_total += other.head_total;
        self.head_failed += other.head_failed;
    }
}

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
            "GetObjectTagging" => {
                metric.get_tag_total += 1;
                if is_err {
                    metric.get_tag_failed += 1;
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
            "DeleteObjectTagging" => {
                metric.delete_tag_total += 1;
                if is_err {
                    metric.delete_tag_failed += 1;
                }
            }
            _ => {}
        }
    }

    pub fn get_bucket_stats(&self, bucket: &str) -> ProxyMetric {
        self.bucket_stats.get(bucket).cloned().unwrap_or_default()
    }

    pub fn bucket_names(&self) -> impl Iterator<Item = &str> {
        self.bucket_stats.keys().map(String::as_str)
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

    pub fn add_size<E>(&mut self, size: i64, _err: Option<&E>) {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FailedMetric {
    pub count: i64,
    pub size: i64,
}

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
        if size > 1024 * 1024 {
            self.xfer_rate_lrg.add_size(size, duration);
        } else {
            self.xfer_rate_sml.add_size(size, duration);
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueStats {
    pub nodes: Vec<QueueNode>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueNode {
    pub q_stats: InQueueMetric,
}

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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketStats {
    pub uptime: i64,
    pub replication_stats: BucketReplicationStats,
    pub queue_stats: QueueStats,
    pub proxy_stats: ProxyMetric,
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_queue_metric_observe_updates_rolling_stats() {
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
    fn fail_stats_recent_since_tracks_windows() {
        let mut stats = FailStats::default();
        stats.add_size(64, None::<&()>);
        stats.add_size(32, None::<&()>);

        let last_minute = stats.recent_since(Duration::from_secs(60));
        let last_hour = stats.recent_since(Duration::from_secs(60 * 60));
        assert_eq!(last_minute.count, 2);
        assert_eq!(last_minute.size, 96);
        assert_eq!(last_hour.count, 2);
        assert_eq!(last_hour.size, 96);
    }

    #[test]
    fn active_worker_stat_update_tracks_rolling_avg_and_max() {
        let mut stats = ActiveWorkerStat::default();
        stats.update(2);
        stats.update(6);
        stats.update(4);

        assert_eq!(stats.curr, 4);
        assert_eq!(stats.max, 6);
        assert_eq!(stats.avg, 4.0);
    }
}
