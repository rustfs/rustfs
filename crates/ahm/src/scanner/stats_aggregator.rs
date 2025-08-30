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

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use rustfs_common::data_usage::DataUsageInfo;

use crate::{error::Result, Error};
use super::{
    local_stats::StatsSummary,
    node_scanner::{LoadLevel, ScanProgress, BucketStats},
};

/// 节点客户端配置
#[derive(Debug, Clone)]
pub struct NodeClientConfig {
    /// 连接超时时间
    pub connect_timeout: Duration,
    /// 请求超时时间
    pub request_timeout: Duration,
    /// 重试次数
    pub max_retries: u32,
    /// 重试间隔
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

/// 节点信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// 节点ID
    pub node_id: String,
    /// 节点地址
    pub address: String,
    /// 节点端口
    pub port: u16,
    /// 是否在线
    pub is_online: bool,
    /// 最后心跳时间
    pub last_heartbeat: SystemTime,
    /// 节点版本
    pub version: String,
}

/// 聚合统计数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedStats {
    /// 聚合时间戳
    pub aggregation_timestamp: SystemTime,
    /// 参与聚合的节点数量
    pub node_count: usize,
    /// 在线节点数量
    pub online_node_count: usize,
    /// 总扫描对象数
    pub total_objects_scanned: u64,
    /// 总健康对象数
    pub total_healthy_objects: u64,
    /// 总损坏对象数
    pub total_corrupted_objects: u64,
    /// 总扫描字节数
    pub total_bytes_scanned: u64,
    /// 总扫描错误数
    pub total_scan_errors: u64,
    /// 总 heal 触发次数
    pub total_heal_triggered: u64,
    /// 总磁盘数
    pub total_disks: usize,
    /// 总存储桶数
    pub total_buckets: usize,
    /// 聚合数据使用情况
    pub aggregated_data_usage: DataUsageInfo,
    /// 各节点统计摘要
    pub node_summaries: HashMap<String, StatsSummary>,
    /// 聚合存储桶统计
    pub aggregated_bucket_stats: HashMap<String, BucketStats>,
    /// 扫描进度聚合
    pub scan_progress_summary: ScanProgressSummary,
    /// 负载级别分布
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

/// 扫描进度摘要
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScanProgressSummary {
    /// 活跃扫描周期的平均值
    pub average_current_cycle: f64,
    /// 总已完成磁盘数
    pub total_completed_disks: usize,
    /// 总已完成存储桶数
    pub total_completed_buckets: usize,
    /// 最新扫描开始时间
    pub earliest_scan_start: Option<SystemTime>,
    /// 预计完成时间
    pub estimated_completion: Option<SystemTime>,
    /// 各节点扫描进度
    pub node_progress: HashMap<String, ScanProgress>,
}

/// 节点客户端
/// 
/// 负责与其他节点通信，获取统计数据
pub struct NodeClient {
    /// 节点信息
    node_info: NodeInfo,
    /// 配置
    config: NodeClientConfig,
    /// HTTP 客户端
    http_client: reqwest::Client,
}

impl NodeClient {
    /// 创建新的节点客户端
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

    /// 获取节点统计摘要
    pub async fn get_stats_summary(&self) -> Result<StatsSummary> {
        let url = format!("http://{}:{}/internal/scanner/stats", 
                         self.node_info.address, self.node_info.port);
        
        for attempt in 1..=self.config.max_retries {
            match self.try_get_stats_summary(&url).await {
                Ok(summary) => return Ok(summary),
                Err(e) => {
                    warn!("尝试 {} 获取节点 {} 统计失败: {}", 
                          attempt, self.node_info.node_id, e);
                    
                    if attempt < self.config.max_retries {
                        tokio::time::sleep(self.config.retry_interval).await;
                    }
                }
            }
        }
        
        Err(Error::Other(format!("无法从节点 {} 获取统计数据", self.node_info.node_id)))
    }

    /// 尝试获取统计摘要
    async fn try_get_stats_summary(&self, url: &str) -> Result<StatsSummary> {
        let response = self.http_client.get(url)
            .send()
            .await
            .map_err(|e| Error::Other(format!("HTTP 请求失败: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::Other(format!("HTTP 状态错误: {}", response.status())));
        }

        let summary = response.json::<StatsSummary>()
            .await
            .map_err(|e| Error::Serialization(format!("反序列化统计数据失败: {}", e)))?;

        Ok(summary)
    }

    /// 检查节点健康状态
    pub async fn check_health(&self) -> bool {
        let url = format!("http://{}:{}/internal/health", 
                         self.node_info.address, self.node_info.port);
        
        match self.http_client.get(&url).send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// 获取节点信息
    pub fn get_node_info(&self) -> &NodeInfo {
        &self.node_info
    }

    /// 更新节点在线状态
    pub fn update_online_status(&mut self, is_online: bool) {
        self.node_info.is_online = is_online;
        if is_online {
            self.node_info.last_heartbeat = SystemTime::now();
        }
    }
}

/// 去中心化统计聚合器配置
#[derive(Debug, Clone)]
pub struct DecentralizedStatsAggregatorConfig {
    /// 聚合间隔
    pub aggregation_interval: Duration,
    /// 缓存过期时间
    pub cache_ttl: Duration,
    /// 节点超时时间
    pub node_timeout: Duration,
    /// 并发聚合数量
    pub max_concurrent_aggregations: usize,
}

impl Default for DecentralizedStatsAggregatorConfig {
    fn default() -> Self {
        Self {
            aggregation_interval: Duration::from_secs(30),   // 30秒聚合一次
            cache_ttl: Duration::from_secs(3),               // 3秒缓存
            node_timeout: Duration::from_secs(5),            // 5秒节点超时
            max_concurrent_aggregations: 10,                 // 最多同时聚合10个节点
        }
    }
}

/// 去中心化统计聚合器
/// 
/// 实时聚合各节点的统计数据，提供全局视图
pub struct DecentralizedStatsAggregator {
    /// 配置
    config: Arc<RwLock<DecentralizedStatsAggregatorConfig>>,
    /// 节点客户端
    node_clients: Arc<RwLock<HashMap<String, Arc<NodeClient>>>>,
    /// 缓存的聚合统计
    cached_stats: Arc<RwLock<Option<AggregatedStats>>>,
    /// 缓存时间戳
    cache_timestamp: Arc<RwLock<SystemTime>>,
    /// 本地节点统计摘要
    local_stats_summary: Arc<RwLock<Option<StatsSummary>>>,
}

impl DecentralizedStatsAggregator {
    /// 创建新的去中心化统计聚合器
    pub fn new(config: DecentralizedStatsAggregatorConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            node_clients: Arc::new(RwLock::new(HashMap::new())),
            cached_stats: Arc::new(RwLock::new(None)),
            cache_timestamp: Arc::new(RwLock::new(SystemTime::UNIX_EPOCH)),
            local_stats_summary: Arc::new(RwLock::new(None)),
        }
    }

    /// 添加节点客户端
    pub async fn add_node(&self, node_info: NodeInfo) {
        let client_config = NodeClientConfig::default();
        let client = Arc::new(NodeClient::new(node_info.clone(), client_config));
        
        self.node_clients.write().await.insert(node_info.node_id.clone(), client);
        
        info!("添加节点到聚合器: {}", node_info.node_id);
    }

    /// 移除节点客户端
    pub async fn remove_node(&self, node_id: &str) {
        self.node_clients.write().await.remove(node_id);
        info!("从聚合器移除节点: {}", node_id);
    }

    /// 设置本地节点统计摘要
    pub async fn set_local_stats(&self, stats: StatsSummary) {
        *self.local_stats_summary.write().await = Some(stats);
    }

    /// 获取聚合统计数据（带缓存）
    pub async fn get_aggregated_stats(&self) -> Result<AggregatedStats> {
        let config = self.config.read().await;
        let cache_ttl = config.cache_ttl;
        drop(config);

        // 检查缓存是否有效
        let cache_timestamp = *self.cache_timestamp.read().await;
        let now = SystemTime::now();
        
        if let Ok(elapsed) = now.duration_since(cache_timestamp) {
            if elapsed < cache_ttl {
                if let Some(cached) = self.cached_stats.read().await.as_ref() {
                    debug!("返回缓存的聚合统计数据，剩余有效时间: {:?}", cache_ttl - elapsed);
                    return Ok(cached.clone());
                }
            }
        }

        // 缓存失效，重新聚合
        info!("缓存失效，开始重新聚合统计数据");
        let aggregated = self.aggregate_stats_from_all_nodes().await?;

        // 更新缓存
        *self.cached_stats.write().await = Some(aggregated.clone());
        *self.cache_timestamp.write().await = now;

        Ok(aggregated)
    }

    /// 强制刷新聚合统计（忽略缓存）
    pub async fn force_refresh_aggregated_stats(&self) -> Result<AggregatedStats> {
        info!("强制刷新聚合统计数据");
        
        let aggregated = self.aggregate_stats_from_all_nodes().await?;

        // 更新缓存
        *self.cached_stats.write().await = Some(aggregated.clone());
        *self.cache_timestamp.write().await = SystemTime::now();

        Ok(aggregated)
    }

    /// 从所有节点聚合统计数据
    async fn aggregate_stats_from_all_nodes(&self) -> Result<AggregatedStats> {
        let node_clients = self.node_clients.read().await;
        let config = self.config.read().await;
        
        // 并发获取所有节点的统计数据
        let mut tasks = Vec::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_aggregations));
        
        // 添加本地节点统计
        let mut node_summaries = HashMap::new();
        if let Some(local_stats) = self.local_stats_summary.read().await.as_ref() {
            node_summaries.insert(local_stats.node_id.clone(), local_stats.clone());
        }

        // 获取远程节点统计
        for (node_id, client) in node_clients.iter() {
            let client = client.clone();
            let semaphore = semaphore.clone();
            let node_id = node_id.clone();
            
            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                
                match client.get_stats_summary().await {
                    Ok(summary) => {
                        debug!("成功获取节点 {} 统计数据", node_id);
                        Some((node_id, summary))
                    }
                    Err(e) => {
                        warn!("获取节点 {} 统计数据失败: {}", node_id, e);
                        None
                    }
                }
            });
            
            tasks.push(task);
        }

        // 等待所有任务完成
        for task in tasks {
            if let Ok(Some((node_id, summary))) = task.await {
                node_summaries.insert(node_id, summary);
            }
        }

        drop(node_clients);
        drop(config);

        // 聚合统计数据
        let aggregated = self.aggregate_node_summaries(node_summaries).await;

        info!("聚合统计完成：{} 个节点，{} 个在线", 
              aggregated.node_count, aggregated.online_node_count);

        Ok(aggregated)
    }

    /// 聚合节点摘要数据
    async fn aggregate_node_summaries(&self, node_summaries: HashMap<String, StatsSummary>) -> AggregatedStats {
        let mut aggregated = AggregatedStats {
            aggregation_timestamp: SystemTime::now(),
            node_count: node_summaries.len(),
            online_node_count: node_summaries.len(), // 假设能获取到数据的节点都在线
            node_summaries: node_summaries.clone(),
            ..Default::default()
        };

        // 聚合数值统计
        for (node_id, summary) in &node_summaries {
            aggregated.total_objects_scanned += summary.total_objects_scanned;
            aggregated.total_healthy_objects += summary.total_healthy_objects;
            aggregated.total_corrupted_objects += summary.total_corrupted_objects;
            aggregated.total_bytes_scanned += summary.total_bytes_scanned;
            aggregated.total_scan_errors += summary.total_scan_errors;
            aggregated.total_heal_triggered += summary.total_heal_triggered;
            aggregated.total_disks += summary.total_disks;
            aggregated.total_buckets += summary.total_buckets;

            // 聚合扫描进度
            aggregated.scan_progress_summary.node_progress.insert(
                node_id.clone(),
                summary.scan_progress.clone(),
            );

            aggregated.scan_progress_summary.total_completed_disks += 
                summary.scan_progress.completed_disks.len();
            aggregated.scan_progress_summary.total_completed_buckets += 
                summary.scan_progress.completed_buckets.len();
        }

        // 计算平均扫描周期
        if !node_summaries.is_empty() {
            let total_cycles: u64 = node_summaries.values()
                .map(|s| s.scan_progress.current_cycle)
                .sum();
            aggregated.scan_progress_summary.average_current_cycle = 
                total_cycles as f64 / node_summaries.len() as f64;
        }

        // 找到最早的扫描开始时间
        aggregated.scan_progress_summary.earliest_scan_start = node_summaries.values()
            .map(|s| s.scan_progress.scan_start_time)
            .min();

        // TODO: 聚合存储桶统计和数据使用情况
        // 这里需要根据具体的 BucketStats 和 DataUsageInfo 结构来实现

        aggregated
    }

    /// 获取节点健康状态
    pub async fn get_nodes_health(&self) -> HashMap<String, bool> {
        let node_clients = self.node_clients.read().await;
        let mut health_status = HashMap::new();

        // 并发检查所有节点健康状态
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

        // 收集结果
        for task in tasks {
            if let Ok((node_id, is_healthy)) = task.await {
                health_status.insert(node_id, is_healthy);
            }
        }

        health_status
    }

    /// 获取在线节点列表
    pub async fn get_online_nodes(&self) -> Vec<String> {
        let health_status = self.get_nodes_health().await;
        
        health_status.into_iter()
            .filter_map(|(node_id, is_healthy)| {
                if is_healthy { Some(node_id) } else { None }
            })
            .collect()
    }

    /// 清除缓存
    pub async fn clear_cache(&self) {
        *self.cached_stats.write().await = None;
        *self.cache_timestamp.write().await = SystemTime::UNIX_EPOCH;
        info!("已清除聚合统计缓存");
    }

    /// 获取缓存状态
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

    /// 更新配置
    pub async fn update_config(&self, new_config: DecentralizedStatsAggregatorConfig) {
        *self.config.write().await = new_config;
        info!("已更新聚合器配置");
    }
}

/// 缓存状态
#[derive(Debug, Clone)]
pub struct CacheStatus {
    /// 是否有缓存数据
    pub has_cached_data: bool,
    /// 缓存时间戳
    pub cache_timestamp: SystemTime,
    /// 缓存是否有效
    pub is_valid: bool,
    /// 缓存 TTL
    pub ttl: Duration,
}