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

use crate::scanner::{
    local_stats::StatsSummary,
    node_scanner::{BucketStats, LoadLevel, ScanProgress},
};
use crate::{Error, Result};
use rustfs_common::data_usage::DataUsageInfo;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// node client config
#[derive(Debug, Clone)]
pub struct NodeClientConfig {
    /// connect timeout
    pub connect_timeout: Duration,
    /// request timeout
    pub request_timeout: Duration,
    /// retry times
    pub max_retries: u32,
    /// retry interval
    pub retry_interval: Duration,
}

impl Default for NodeClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            max_retries: 3,
            retry_interval: Duration::from_secs(1),
        }
    }
}

/// node info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// node id
    pub node_id: String,
    /// node address
    pub address: String,
    /// node port
    pub port: u16,
    /// is online
    pub is_online: bool,
    /// last heartbeat time
    pub last_heartbeat: SystemTime,
    /// node version
    pub version: String,
}

/// aggregated stats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedStats {
    /// aggregation timestamp
    pub aggregation_timestamp: SystemTime,
    /// number of nodes participating in aggregation
    pub node_count: usize,
    /// number of online nodes
    pub online_node_count: usize,
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
    /// aggregated data usage
    pub aggregated_data_usage: DataUsageInfo,
    /// node summaries
    pub node_summaries: HashMap<String, StatsSummary>,
    /// aggregated bucket stats
    pub aggregated_bucket_stats: HashMap<String, BucketStats>,
    /// aggregated scan progress
    pub scan_progress_summary: ScanProgressSummary,
    /// load level distribution
    pub load_level_distribution: HashMap<LoadLevel, usize>,
}

impl Default for AggregatedStats {
    fn default() -> Self {
        Self {
            aggregation_timestamp: SystemTime::now(),
            node_count: 0,
            online_node_count: 0,
            total_objects_scanned: 0,
            total_healthy_objects: 0,
            total_corrupted_objects: 0,
            total_bytes_scanned: 0,
            total_scan_errors: 0,
            total_heal_triggered: 0,
            total_disks: 0,
            total_buckets: 0,
            aggregated_data_usage: DataUsageInfo::default(),
            node_summaries: HashMap::new(),
            aggregated_bucket_stats: HashMap::new(),
            scan_progress_summary: ScanProgressSummary::default(),
            load_level_distribution: HashMap::new(),
        }
    }
}

/// scan progress summary
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScanProgressSummary {
    /// average current cycle
    pub average_current_cycle: f64,
    /// total completed disks
    pub total_completed_disks: usize,
    /// total completed buckets
    pub total_completed_buckets: usize,
    /// latest scan start time
    pub earliest_scan_start: Option<SystemTime>,
    /// estimated completion time
    pub estimated_completion: Option<SystemTime>,
    /// node progress
    pub node_progress: HashMap<String, ScanProgress>,
}

/// node client
///
/// responsible for communicating with other nodes, getting stats data
pub struct NodeClient {
    /// node info
    node_info: NodeInfo,
    /// config
    config: NodeClientConfig,
    /// HTTP client
    http_client: reqwest::Client,
}

impl NodeClient {
    /// create new node client
    pub fn new(node_info: NodeInfo, config: NodeClientConfig) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            node_info,
            config,
            http_client,
        }
    }

    /// get node stats summary
    pub async fn get_stats_summary(&self) -> Result<StatsSummary> {
        let url = format!("http://{}:{}/internal/scanner/stats", self.node_info.address, self.node_info.port);

        for attempt in 1..=self.config.max_retries {
            match self.try_get_stats_summary(&url).await {
                Ok(summary) => return Ok(summary),
                Err(e) => {
                    warn!("try to get node {} stats failed: {}", self.node_info.node_id, e);

                    if attempt < self.config.max_retries {
                        tokio::time::sleep(self.config.retry_interval).await;
                    }
                }
            }
        }

        Err(Error::Other(format!("cannot get stats data from node {}", self.node_info.node_id)))
    }

    /// try to get stats summary
    async fn try_get_stats_summary(&self, url: &str) -> Result<StatsSummary> {
        let response = self
            .http_client
            .get(url)
            .send()
            .await
            .map_err(|e| Error::Other(format!("HTTP request failed: {e}")))?;

        if !response.status().is_success() {
            return Err(Error::Other(format!("HTTP status error: {}", response.status())));
        }

        let summary = response
            .json::<StatsSummary>()
            .await
            .map_err(|e| Error::Serialization(format!("deserialize stats data failed: {e}")))?;

        Ok(summary)
    }

    /// check node health status
    pub async fn check_health(&self) -> bool {
        let url = format!("http://{}:{}/internal/health", self.node_info.address, self.node_info.port);

        match self.http_client.get(&url).send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// get node info
    pub fn get_node_info(&self) -> &NodeInfo {
        &self.node_info
    }

    /// update node online status
    pub fn update_online_status(&mut self, is_online: bool) {
        self.node_info.is_online = is_online;
        if is_online {
            self.node_info.last_heartbeat = SystemTime::now();
        }
    }
}

/// decentralized stats aggregator config
#[derive(Debug, Clone)]
pub struct DecentralizedStatsAggregatorConfig {
    /// aggregation interval
    pub aggregation_interval: Duration,
    /// cache ttl
    pub cache_ttl: Duration,
    /// node timeout
    pub node_timeout: Duration,
    /// max concurrent aggregations
    pub max_concurrent_aggregations: usize,
}

impl Default for DecentralizedStatsAggregatorConfig {
    fn default() -> Self {
        Self {
            aggregation_interval: Duration::from_secs(30), // 30 seconds to aggregate
            cache_ttl: Duration::from_secs(3),             // 3 seconds to cache
            node_timeout: Duration::from_secs(5),          // 5 seconds to node timeout
            max_concurrent_aggregations: 10,               // max 10 nodes to aggregate concurrently
        }
    }
}

/// decentralized stats aggregator
///
/// real-time aggregate stats data from all nodes, provide global view
pub struct DecentralizedStatsAggregator {
    /// config
    config: Arc<RwLock<DecentralizedStatsAggregatorConfig>>,
    /// node clients
    node_clients: Arc<RwLock<HashMap<String, Arc<NodeClient>>>>,
    /// cached aggregated stats
    cached_stats: Arc<RwLock<Option<AggregatedStats>>>,
    /// cache timestamp
    cache_timestamp: Arc<RwLock<SystemTime>>,
    /// local node stats summary
    local_stats_summary: Arc<RwLock<Option<StatsSummary>>>,
}

impl DecentralizedStatsAggregator {
    /// create new decentralized stats aggregator
    pub fn new(config: DecentralizedStatsAggregatorConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            node_clients: Arc::new(RwLock::new(HashMap::new())),
            cached_stats: Arc::new(RwLock::new(None)),
            cache_timestamp: Arc::new(RwLock::new(SystemTime::UNIX_EPOCH)),
            local_stats_summary: Arc::new(RwLock::new(None)),
        }
    }

    /// add node client
    pub async fn add_node(&self, node_info: NodeInfo) {
        let client_config = NodeClientConfig::default();
        let client = Arc::new(NodeClient::new(node_info.clone(), client_config));

        self.node_clients.write().await.insert(node_info.node_id.clone(), client);

        info!("add node to aggregator: {}", node_info.node_id);
    }

    /// remove node client
    pub async fn remove_node(&self, node_id: &str) {
        self.node_clients.write().await.remove(node_id);
        info!("remove node from aggregator: {}", node_id);
    }

    /// set local node stats summary
    pub async fn set_local_stats(&self, stats: StatsSummary) {
        *self.local_stats_summary.write().await = Some(stats);
    }

    /// get aggregated stats data (with cache)
    pub async fn get_aggregated_stats(&self) -> Result<AggregatedStats> {
        let config = self.config.read().await;
        let cache_ttl = config.cache_ttl;
        drop(config);

        // check cache validity
        let cache_timestamp = *self.cache_timestamp.read().await;
        let now = SystemTime::now();

        debug!(
            "cache check: cache_timestamp={:?}, now={:?}, cache_ttl={:?}",
            cache_timestamp, now, cache_ttl
        );

        // Check cache validity if timestamp is not initial value (UNIX_EPOCH)
        if cache_timestamp != SystemTime::UNIX_EPOCH {
            if let Ok(elapsed) = now.duration_since(cache_timestamp) {
                if elapsed < cache_ttl {
                    if let Some(cached) = self.cached_stats.read().await.as_ref() {
                        debug!("Returning cached aggregated stats, remaining TTL: {:?}", cache_ttl - elapsed);
                        return Ok(cached.clone());
                    }
                } else {
                    debug!("Cache expired: elapsed={:?} >= ttl={:?}", elapsed, cache_ttl);
                }
            }
        }

        // cache expired, re-aggregate
        info!("cache expired, start re-aggregating stats data");
        let aggregation_timestamp = now;
        let aggregated = self.aggregate_stats_from_all_nodes(aggregation_timestamp).await?;

        // update cache
        *self.cached_stats.write().await = Some(aggregated.clone());
        // Use the time when aggregation completes as cache timestamp to avoid premature expiry during long runs
        *self.cache_timestamp.write().await = SystemTime::now();

        Ok(aggregated)
    }

    /// force refresh aggregated stats (ignore cache)
    pub async fn force_refresh_aggregated_stats(&self) -> Result<AggregatedStats> {
        let now = SystemTime::now();
        let aggregated = self.aggregate_stats_from_all_nodes(now).await?;

        // update cache
        *self.cached_stats.write().await = Some(aggregated.clone());
        // Cache timestamp should reflect completion time rather than aggregation start
        *self.cache_timestamp.write().await = SystemTime::now();

        Ok(aggregated)
    }

    /// aggregate stats data from all nodes
    async fn aggregate_stats_from_all_nodes(&self, aggregation_timestamp: SystemTime) -> Result<AggregatedStats> {
        let node_clients = self.node_clients.read().await;
        let config = self.config.read().await;

        // concurrent get stats data from all nodes
        let mut tasks = Vec::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_aggregations));

        // add local node stats
        let mut node_summaries = HashMap::new();
        if let Some(local_stats) = self.local_stats_summary.read().await.as_ref() {
            node_summaries.insert(local_stats.node_id.clone(), local_stats.clone());
        }

        // get remote node stats
        for (node_id, client) in node_clients.iter() {
            let client = client.clone();
            let semaphore = semaphore.clone();
            let node_id = node_id.clone();

            let task = tokio::spawn(async move {
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(e) => {
                        warn!("Failed to acquire semaphore for node {}: {}", node_id, e);
                        return None;
                    }
                };

                match client.get_stats_summary().await {
                    Ok(summary) => {
                        debug!("successfully get node {} stats data", node_id);
                        Some((node_id, summary))
                    }
                    Err(e) => {
                        warn!("get node {} stats data failed: {}", node_id, e);
                        None
                    }
                }
            });

            tasks.push(task);
        }

        // wait for all tasks to complete
        for task in tasks {
            if let Ok(Some((node_id, summary))) = task.await {
                node_summaries.insert(node_id, summary);
            }
        }

        drop(node_clients);
        drop(config);

        // aggregate stats data
        let aggregated = self.aggregate_node_summaries(node_summaries, aggregation_timestamp).await;

        info!(
            "aggregate stats completed: {} nodes, {} online",
            aggregated.node_count, aggregated.online_node_count
        );

        Ok(aggregated)
    }

    /// aggregate node summaries
    async fn aggregate_node_summaries(
        &self,
        node_summaries: HashMap<String, StatsSummary>,
        aggregation_timestamp: SystemTime,
    ) -> AggregatedStats {
        let mut aggregated = AggregatedStats {
            aggregation_timestamp,
            node_count: node_summaries.len(),
            online_node_count: node_summaries.len(), // assume all nodes with data are online
            node_summaries: node_summaries.clone(),
            ..Default::default()
        };

        // aggregate numeric stats
        for (node_id, summary) in &node_summaries {
            aggregated.total_objects_scanned += summary.total_objects_scanned;
            aggregated.total_healthy_objects += summary.total_healthy_objects;
            aggregated.total_corrupted_objects += summary.total_corrupted_objects;
            aggregated.total_bytes_scanned += summary.total_bytes_scanned;
            aggregated.total_scan_errors += summary.total_scan_errors;
            aggregated.total_heal_triggered += summary.total_heal_triggered;
            aggregated.total_disks += summary.total_disks;
            aggregated.total_buckets += summary.total_buckets;
            aggregated.aggregated_data_usage.merge(&summary.data_usage);

            // aggregate scan progress
            aggregated
                .scan_progress_summary
                .node_progress
                .insert(node_id.clone(), summary.scan_progress.clone());

            aggregated.scan_progress_summary.total_completed_disks += summary.scan_progress.completed_disks.len();
            aggregated.scan_progress_summary.total_completed_buckets += summary.scan_progress.completed_buckets.len();
        }

        // calculate average scan cycle
        if !node_summaries.is_empty() {
            let total_cycles: u64 = node_summaries.values().map(|s| s.scan_progress.current_cycle).sum();
            aggregated.scan_progress_summary.average_current_cycle = total_cycles as f64 / node_summaries.len() as f64;
        }

        // find earliest scan start time
        aggregated.scan_progress_summary.earliest_scan_start =
            node_summaries.values().map(|s| s.scan_progress.scan_start_time).min();

        // TODO: aggregate bucket stats and data usage
        // here we need to implement it based on the specific BucketStats and DataUsageInfo structure

        aggregated
    }

    /// get nodes health status
    pub async fn get_nodes_health(&self) -> HashMap<String, bool> {
        let node_clients = self.node_clients.read().await;
        let mut health_status = HashMap::new();

        // concurrent check all nodes health status
        let mut tasks = Vec::new();

        for (node_id, client) in node_clients.iter() {
            let client = client.clone();
            let node_id = node_id.clone();

            let task = tokio::spawn(async move {
                let is_healthy = client.check_health().await;
                (node_id, is_healthy)
            });

            tasks.push(task);
        }

        // collect results
        for task in tasks {
            if let Ok((node_id, is_healthy)) = task.await {
                health_status.insert(node_id, is_healthy);
            }
        }

        health_status
    }

    /// get online nodes list
    pub async fn get_online_nodes(&self) -> Vec<String> {
        let health_status = self.get_nodes_health().await;

        health_status
            .into_iter()
            .filter_map(|(node_id, is_healthy)| if is_healthy { Some(node_id) } else { None })
            .collect()
    }

    /// clear cache
    pub async fn clear_cache(&self) {
        *self.cached_stats.write().await = None;
        *self.cache_timestamp.write().await = SystemTime::UNIX_EPOCH;
        info!("clear aggregated stats cache");
    }

    /// get cache status
    pub async fn get_cache_status(&self) -> CacheStatus {
        let cached_stats = self.cached_stats.read().await;
        let cache_timestamp = *self.cache_timestamp.read().await;
        let config = self.config.read().await;

        let is_valid = if let Ok(elapsed) = SystemTime::now().duration_since(cache_timestamp) {
            elapsed < config.cache_ttl
        } else {
            false
        };

        CacheStatus {
            has_cached_data: cached_stats.is_some(),
            cache_timestamp,
            is_valid,
            ttl: config.cache_ttl,
        }
    }

    /// update config
    pub async fn update_config(&self, new_config: DecentralizedStatsAggregatorConfig) {
        *self.config.write().await = new_config;
        info!("update aggregator config");
    }
}

/// cache status
#[derive(Debug, Clone)]
pub struct CacheStatus {
    /// has cached data
    pub has_cached_data: bool,
    /// cache timestamp
    pub cache_timestamp: SystemTime,
    /// cache is valid
    pub is_valid: bool,
    /// cache ttl
    pub ttl: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::node_scanner::{BucketScanState, ScanProgress};
    use rustfs_common::data_usage::{BucketUsageInfo, DataUsageInfo};
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;

    #[tokio::test]
    async fn aggregated_stats_merge_data_usage() {
        let aggregator = DecentralizedStatsAggregator::new(DecentralizedStatsAggregatorConfig::default());

        let mut data_usage = DataUsageInfo::default();
        let bucket_usage = BucketUsageInfo {
            objects_count: 5,
            size: 1024,
            ..Default::default()
        };
        data_usage.buckets_usage.insert("bucket".to_string(), bucket_usage);
        data_usage.objects_total_count = 5;
        data_usage.objects_total_size = 1024;

        let summary = StatsSummary {
            node_id: "local-node".to_string(),
            total_objects_scanned: 10,
            total_healthy_objects: 9,
            total_corrupted_objects: 1,
            total_bytes_scanned: 2048,
            total_scan_errors: 0,
            total_heal_triggered: 0,
            total_disks: 2,
            total_buckets: 1,
            last_update: SystemTime::now(),
            scan_progress: ScanProgress::default(),
            data_usage: data_usage.clone(),
        };

        aggregator.set_local_stats(summary).await;

        // Wait briefly to ensure async cache writes settle in high-concurrency environments
        tokio::time::sleep(Duration::from_millis(10)).await;

        let aggregated = aggregator.get_aggregated_stats().await.expect("aggregated stats");

        assert_eq!(aggregated.node_count, 1);
        assert!(aggregated.node_summaries.contains_key("local-node"));
        assert_eq!(aggregated.aggregated_data_usage.objects_total_count, 5);
        assert_eq!(
            aggregated
                .aggregated_data_usage
                .buckets_usage
                .get("bucket")
                .expect("bucket usage present")
                .objects_count,
            5
        );
    }

    #[tokio::test]
    async fn aggregated_stats_merge_multiple_nodes() {
        let aggregator = DecentralizedStatsAggregator::new(DecentralizedStatsAggregatorConfig::default());

        let mut local_usage = DataUsageInfo::default();
        let local_bucket = BucketUsageInfo {
            objects_count: 3,
            versions_count: 3,
            size: 150,
            ..Default::default()
        };
        local_usage.buckets_usage.insert("local-bucket".to_string(), local_bucket);
        local_usage.calculate_totals();
        local_usage.buckets_count = local_usage.buckets_usage.len() as u64;
        local_usage.last_update = Some(SystemTime::now());

        let local_progress = ScanProgress {
            current_cycle: 1,
            completed_disks: {
                let mut set = std::collections::HashSet::new();
                set.insert("disk-local".to_string());
                set
            },
            completed_buckets: {
                let mut map = std::collections::HashMap::new();
                map.insert(
                    "local-bucket".to_string(),
                    BucketScanState {
                        completed: true,
                        last_object_key: Some("obj1".to_string()),
                        objects_scanned: 3,
                        scan_timestamp: SystemTime::now(),
                    },
                );
                map
            },
            ..Default::default()
        };

        let local_summary = StatsSummary {
            node_id: "node-local".to_string(),
            total_objects_scanned: 30,
            total_healthy_objects: 30,
            total_corrupted_objects: 0,
            total_bytes_scanned: 1500,
            total_scan_errors: 0,
            total_heal_triggered: 0,
            total_disks: 1,
            total_buckets: 1,
            last_update: SystemTime::now(),
            scan_progress: local_progress,
            data_usage: local_usage.clone(),
        };

        let mut remote_usage = DataUsageInfo::default();
        let remote_bucket = BucketUsageInfo {
            objects_count: 5,
            versions_count: 5,
            size: 250,
            ..Default::default()
        };
        remote_usage.buckets_usage.insert("remote-bucket".to_string(), remote_bucket);
        remote_usage.calculate_totals();
        remote_usage.buckets_count = remote_usage.buckets_usage.len() as u64;
        remote_usage.last_update = Some(SystemTime::now());

        let remote_progress = ScanProgress {
            current_cycle: 2,
            completed_disks: {
                let mut set = std::collections::HashSet::new();
                set.insert("disk-remote".to_string());
                set
            },
            completed_buckets: {
                let mut map = std::collections::HashMap::new();
                map.insert(
                    "remote-bucket".to_string(),
                    BucketScanState {
                        completed: true,
                        last_object_key: Some("remote-obj".to_string()),
                        objects_scanned: 5,
                        scan_timestamp: SystemTime::now(),
                    },
                );
                map
            },
            ..Default::default()
        };

        let remote_summary = StatsSummary {
            node_id: "node-remote".to_string(),
            total_objects_scanned: 50,
            total_healthy_objects: 48,
            total_corrupted_objects: 2,
            total_bytes_scanned: 2048,
            total_scan_errors: 1,
            total_heal_triggered: 1,
            total_disks: 2,
            total_buckets: 1,
            last_update: SystemTime::now(),
            scan_progress: remote_progress,
            data_usage: remote_usage.clone(),
        };
        let node_summaries: HashMap<_, _> = [
            (local_summary.node_id.clone(), local_summary.clone()),
            (remote_summary.node_id.clone(), remote_summary.clone()),
        ]
        .into_iter()
        .collect();

        let aggregated = aggregator.aggregate_node_summaries(node_summaries, SystemTime::now()).await;

        assert_eq!(aggregated.node_count, 2);
        assert_eq!(aggregated.total_objects_scanned, 80);
        assert_eq!(aggregated.total_corrupted_objects, 2);
        assert_eq!(aggregated.total_disks, 3);
        assert!(aggregated.node_summaries.contains_key("node-local"));
        assert!(aggregated.node_summaries.contains_key("node-remote"));

        assert_eq!(
            aggregated.aggregated_data_usage.objects_total_count,
            local_usage.objects_total_count + remote_usage.objects_total_count
        );
        assert_eq!(
            aggregated.aggregated_data_usage.objects_total_size,
            local_usage.objects_total_size + remote_usage.objects_total_size
        );

        let mut expected_buckets: HashSet<&str> = HashSet::new();
        expected_buckets.insert("local-bucket");
        expected_buckets.insert("remote-bucket");
        let actual_buckets: HashSet<&str> = aggregated
            .aggregated_data_usage
            .buckets_usage
            .keys()
            .map(|s| s.as_str())
            .collect();
        assert_eq!(expected_buckets, actual_buckets);
    }
}
