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

use super::replication_error_boundary::Error;
use super::replication_filemeta_boundary::{ReplicatedTargetInfo, ReplicationStatusType, ReplicationType};
use super::replication_stats_boundary::{
    ActiveWorkerStat, BucketReplicationStat, BucketReplicationStats, BucketStats, InQueueMetric, ProxyMetric, ProxyStatsCache,
    QueueCache, SRMetricsSummary, XferStats,
};
use super::runtime_boundary as runtime_sources;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;

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
                let current = runtime_sources::replication_pool()
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
                    ri.error.as_ref().map(|e| Error::other(e.clone())),
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
                    ri.error.as_ref().map(|e| Error::other(e.clone())),
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
                    ri.error.as_ref().map(|e| Error::other(e.clone())),
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
                    ri.error.as_ref().map(|e| Error::other(e.clone())),
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
            for bucket in p_cache.bucket_names() {
                result.entry(bucket.to_string()).or_insert_with(BucketReplicationStats::new);
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

        if let Some(monitor) = runtime_sources::bucket_monitor() {
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
