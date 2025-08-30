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
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::node_scanner::LoadLevel;
use crate::error::Result;

/// IO 监控配置
#[derive(Debug, Clone)]
pub struct IOMonitorConfig {
    /// 监控间隔
    pub monitor_interval: Duration,
    /// 历史数据保留时间
    pub history_retention: Duration,
    /// 负载评估窗口大小
    pub load_window_size: usize,
    /// 是否启用实际系统监控
    pub enable_system_monitoring: bool,
    /// 磁盘路径列表（用于监控特定磁盘）
    pub disk_paths: Vec<String>,
}

impl Default for IOMonitorConfig {
    fn default() -> Self {
        Self {
            monitor_interval: Duration::from_secs(1),      // 1秒监控间隔
            history_retention: Duration::from_secs(300),   // 保留5分钟历史
            load_window_size: 30,                          // 30个采样点的滑动窗口
            enable_system_monitoring: false,               // 默认使用模拟数据
            disk_paths: Vec::new(),
        }
    }
}

/// IO 监控指标
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IOMetrics {
    /// 时间戳
    pub timestamp: SystemTime,
    /// 磁盘 IOPS（读 + 写）
    pub iops: u64,
    /// 读 IOPS
    pub read_iops: u64,
    /// 写 IOPS
    pub write_iops: u64,
    /// 磁盘队列深度
    pub queue_depth: u64,
    /// 平均延迟（毫秒）
    pub avg_latency: u64,
    /// 读延迟（毫秒）
    pub read_latency: u64,
    /// 写延迟（毫秒）
    pub write_latency: u64,
    /// CPU 使用率 (0-100)
    pub cpu_usage: u8,
    /// 内存使用率 (0-100)
    pub memory_usage: u8,
    /// 磁盘使用率 (0-100)
    pub disk_utilization: u8,
    /// 网络 IO (Mbps)
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

/// 负载级别统计
#[derive(Debug, Clone, Default)]
pub struct LoadLevelStats {
    /// 低负载时间（秒）
    pub low_load_duration: u64,
    /// 中负载时间（秒）
    pub medium_load_duration: u64,
    /// 高负载时间（秒）
    pub high_load_duration: u64,
    /// 超高负载时间（秒）
    pub critical_load_duration: u64,
    /// 负载切换次数
    pub load_transitions: u64,
}

/// 增强的 IO 监控器
pub struct AdvancedIOMonitor {
    /// 配置
    config: Arc<RwLock<IOMonitorConfig>>,
    /// 当前指标
    current_metrics: Arc<RwLock<IOMetrics>>,
    /// 历史指标（滑动窗口）
    history_metrics: Arc<RwLock<VecDeque<IOMetrics>>>,
    /// 当前负载级别
    current_load_level: Arc<RwLock<LoadLevel>>,
    /// 负载级别历史
    load_level_history: Arc<RwLock<VecDeque<(SystemTime, LoadLevel)>>>,
    /// 负载级别统计
    load_stats: Arc<RwLock<LoadLevelStats>>,
    /// 业务 IO 指标（由外部更新）
    business_metrics: Arc<BusinessIOMetrics>,
    /// 取消令牌
    cancel_token: CancellationToken,
}

/// 业务 IO 指标
pub struct BusinessIOMetrics {
    /// 业务请求延迟（毫秒）
    pub request_latency: AtomicU64,
    /// 业务请求 QPS
    pub request_qps: AtomicU64,
    /// 业务错误率 (0-10000, 表示0.00%-100.00%)
    pub error_rate: AtomicU64,
    /// 活跃连接数
    pub active_connections: AtomicU64,
    /// 最后更新时间
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
    /// 创建新的高级 IO 监控器
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

    /// 启动监控
    pub async fn start(&self) -> Result<()> {
        info!("启动高级 IO 监控器");

        let monitor = self.clone_for_background();
        tokio::spawn(async move {
            if let Err(e) = monitor.monitoring_loop().await {
                error!("IO 监控循环失败: {}", e);
            }
        });

        Ok(())
    }

    /// 停止监控
    pub async fn stop(&self) {
        info!("停止 IO 监控器");
        self.cancel_token.cancel();
    }

    /// 监控循环
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
                    info!("IO 监控循环被取消");
                    break;
                }
                _ = interval.tick() => {
                    // 收集系统指标
                    let metrics = self.collect_system_metrics().await;
                    
                    // 更新当前指标
                    *self.current_metrics.write().await = metrics.clone();

                    // 更新历史指标
                    self.update_metrics_history(metrics.clone()).await;

                    // 计算负载级别
                    let new_load_level = self.calculate_load_level(&metrics).await;
                    
                    // 检查负载级别是否变化
                    if new_load_level != last_load_level {
                        self.handle_load_level_change(last_load_level, new_load_level, load_level_start_time).await;
                        last_load_level = new_load_level;
                        load_level_start_time = SystemTime::now();
                    }

                    // 更新当前负载级别
                    *self.current_load_level.write().await = new_load_level;

                    debug!("IO 监控更新: IOPS={}, 队列深度={}, 延迟={}ms, 负载级别={:?}", 
                           metrics.iops, metrics.queue_depth, metrics.avg_latency, new_load_level);
                }
            }
        }

        Ok(())
    }

    /// 收集系统指标
    async fn collect_system_metrics(&self) -> IOMetrics {
        let config = self.config.read().await;
        
        if config.enable_system_monitoring {
            // 实际系统监控实现
            self.collect_real_system_metrics().await
        } else {
            // 模拟数据
            self.generate_simulated_metrics().await
        }
    }

    /// 收集真实系统指标（需要根据具体系统实现）
    async fn collect_real_system_metrics(&self) -> IOMetrics {
        // TODO: 实现真实的系统指标收集
        // 可以使用 procfs、sysfs 或其他系统 API
        
        let metrics = IOMetrics {
            timestamp: SystemTime::now(),
            ..Default::default()
        };

        // 示例：读取 /proc/diskstats
        if let Ok(diskstats) = tokio::fs::read_to_string("/proc/diskstats").await {
            // 解析磁盘统计信息
            // 这里需要实现具体的解析逻辑
            debug!("读取磁盘统计信息: {} 字节", diskstats.len());
        }

        // 示例：读取 /proc/stat 获取 CPU 信息
        if let Ok(stat) = tokio::fs::read_to_string("/proc/stat").await {
            // 解析 CPU 统计信息
            debug!("读取 CPU 统计信息: {} 字节", stat.len());
        }

        // 示例：读取 /proc/meminfo 获取内存信息
        if let Ok(meminfo) = tokio::fs::read_to_string("/proc/meminfo").await {
            // 解析内存统计信息
            debug!("读取内存统计信息: {} 字节", meminfo.len());
        }

        metrics
    }

    /// 生成模拟指标（用于测试和开发）
    async fn generate_simulated_metrics(&self) -> IOMetrics {
        use rand::Rng;
        let mut rng = rand::rng();

        // 获取业务指标影响
        let business_latency = self.business_metrics.request_latency.load(Ordering::Relaxed);
        let business_qps = self.business_metrics.request_qps.load(Ordering::Relaxed);
        
        // 基于业务负载生成模拟的系统指标
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

    /// 更新指标历史
    async fn update_metrics_history(&self, metrics: IOMetrics) {
        let mut history = self.history_metrics.write().await;
        let config = self.config.read().await;

        // 添加新指标
        history.push_back(metrics);

        // 清理过期数据
        let retention_cutoff = SystemTime::now() - config.history_retention;
        while let Some(front) = history.front() {
            if front.timestamp < retention_cutoff {
                history.pop_front();
            } else {
                break;
            }
        }

        // 限制窗口大小
        while history.len() > config.load_window_size {
            history.pop_front();
        }
    }

    /// 计算负载级别
    async fn calculate_load_level(&self, metrics: &IOMetrics) -> LoadLevel {
        // 多维度负载评估算法
        let mut load_score = 0u32;

        // IOPS 负载评估 (权重: 25%)
        let iops_score = match metrics.iops {
            0..=200 => 0,
            201..=500 => 15,
            501..=1000 => 25,
            _ => 35,
        };
        load_score += iops_score;

        // 延迟负载评估 (权重: 30%)
        let latency_score = match metrics.avg_latency {
            0..=10 => 0,
            11..=50 => 20,
            51..=100 => 30,
            _ => 40,
        };
        load_score += latency_score;

        // 队列深度评估 (权重: 20%)
        let queue_score = match metrics.queue_depth {
            0..=5 => 0,
            6..=15 => 10,
            16..=30 => 20,
            _ => 25,
        };
        load_score += queue_score;

        // CPU 使用率评估 (权重: 15%)
        let cpu_score = match metrics.cpu_usage {
            0..=30 => 0,
            31..=60 => 8,
            61..=80 => 12,
            _ => 15,
        };
        load_score += cpu_score;

        // 磁盘使用率评估 (权重: 10%)
        let disk_score = match metrics.disk_utilization {
            0..=50 => 0,
            51..=75 => 5,
            76..=90 => 8,
            _ => 10,
        };
        load_score += disk_score;

        // 业务指标影响
        let business_latency = self.business_metrics.request_latency.load(Ordering::Relaxed);
        let business_error_rate = self.business_metrics.error_rate.load(Ordering::Relaxed);
        
        if business_latency > 100 {
            load_score += 20; // 业务延迟过高
        }
        if business_error_rate > 100 { // > 1%
            load_score += 15; // 业务错误率过高
        }

        // 历史趋势分析
        let trend_score = self.calculate_trend_score().await;
        load_score += trend_score;

        // 根据总分确定负载级别
        match load_score {
            0..=30 => LoadLevel::Low,
            31..=60 => LoadLevel::Medium,
            61..=90 => LoadLevel::High,
            _ => LoadLevel::Critical,
        }
    }

    /// 计算趋势评分
    async fn calculate_trend_score(&self) -> u32 {
        let history = self.history_metrics.read().await;
        
        if history.len() < 5 {
            return 0; // 数据不足，无法分析趋势
        }

        // 分析最近 5 个样本的趋势
        let recent: Vec<_> = history.iter().rev().take(5).collect();
        
        // 检查 IOPS 上升趋势
        let mut iops_trend = 0;
        for i in 1..recent.len() {
            if recent[i-1].iops > recent[i].iops {
                iops_trend += 1;
            }
        }

        // 检查延迟上升趋势
        let mut latency_trend = 0;
        for i in 1..recent.len() {
            if recent[i-1].avg_latency > recent[i].avg_latency {
                latency_trend += 1;
            }
        }

        // 如果 IOPS 和延迟都在上升，增加负载评分
        if iops_trend >= 3 && latency_trend >= 3 {
            15 // 明显上升趋势
        } else if iops_trend >= 2 || latency_trend >= 2 {
            5  // 轻微上升趋势
        } else {
            0  // 无明显趋势
        }
    }

    /// 处理负载级别变化
    async fn handle_load_level_change(&self, old_level: LoadLevel, new_level: LoadLevel, start_time: SystemTime) {
        let duration = SystemTime::now().duration_since(start_time).unwrap_or(Duration::ZERO);
        
        // 更新统计
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

        // 更新历史
        {
            let mut history = self.load_level_history.write().await;
            history.push_back((SystemTime::now(), new_level));
            
            // 保持历史记录在合理范围内
            while history.len() > 100 {
                history.pop_front();
            }
        }

        info!("负载级别变化: {:?} -> {:?}, 持续时间: {:?}", 
              old_level, new_level, duration);

        // 如果进入关键负载状态，记录警告
        if new_level == LoadLevel::Critical {
            warn!("系统进入关键负载状态，Scanner 将暂停运行");
        }
    }

    /// 获取当前负载级别
    pub async fn get_business_load_level(&self) -> LoadLevel {
        *self.current_load_level.read().await
    }

    /// 获取当前指标
    pub async fn get_current_metrics(&self) -> IOMetrics {
        self.current_metrics.read().await.clone()
    }

    /// 获取历史指标
    pub async fn get_history_metrics(&self) -> Vec<IOMetrics> {
        self.history_metrics.read().await.iter().cloned().collect()
    }

    /// 获取负载统计
    pub async fn get_load_stats(&self) -> LoadLevelStats {
        self.load_stats.read().await.clone()
    }

    /// 更新业务 IO 指标
    pub async fn update_business_metrics(&self, latency: u64, qps: u64, error_rate: u64, connections: u64) {
        self.business_metrics.request_latency.store(latency, Ordering::Relaxed);
        self.business_metrics.request_qps.store(qps, Ordering::Relaxed);
        self.business_metrics.error_rate.store(error_rate, Ordering::Relaxed);
        self.business_metrics.active_connections.store(connections, Ordering::Relaxed);
        
        *self.business_metrics.last_update.write().await = SystemTime::now();
        
        debug!("更新业务指标: 延迟={}ms, QPS={}, 错误率={}‰, 连接数={}", 
               latency, qps, error_rate, connections);
    }

    /// 克隆用于后台任务
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

    /// 重置统计
    pub async fn reset_stats(&self) {
        *self.load_stats.write().await = LoadLevelStats::default();
        self.load_level_history.write().await.clear();
        self.history_metrics.write().await.clear();
        info!("已重置 IO 监控统计");
    }

    /// 获取负载级别历史
    pub async fn get_load_level_history(&self) -> Vec<(SystemTime, LoadLevel)> {
        self.load_level_history.read().await.iter().cloned().collect()
    }
}