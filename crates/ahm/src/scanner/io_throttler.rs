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
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::node_scanner::LoadLevel;

/// IO 限流配置
#[derive(Debug, Clone)]
pub struct IOThrottlerConfig {
    /// 最大 IOPS 限制
    pub max_iops: u64,
    /// 业务优先级基线（百分比）
    pub base_business_priority: u8,
    /// 扫描器最小延迟（毫秒）
    pub min_scan_delay: u64,
    /// 扫描器最大延迟（毫秒）
    pub max_scan_delay: u64,
    /// 是否启用动态调节
    pub enable_dynamic_adjustment: bool,
    /// 调节响应时间（秒）
    pub adjustment_response_time: u64,
}

impl Default for IOThrottlerConfig {
    fn default() -> Self {
        Self {
            max_iops: 1000,             // 默认最大 1000 IOPS
            base_business_priority: 95, // 业务优先级 95%
            min_scan_delay: 5000,       // 最小 5s 延迟
            max_scan_delay: 60000,      // 最大 60s 延迟
            enable_dynamic_adjustment: true,
            adjustment_response_time: 5, // 5秒响应时间
        }
    }
}

/// 资源分配策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceAllocationStrategy {
    /// 业务优先策略
    BusinessFirst,
    /// 平衡策略
    Balanced,
    /// 维护优先策略（仅在特殊情况下使用）
    MaintenanceFirst,
}

/// 限流决策
#[derive(Debug, Clone)]
pub struct ThrottleDecision {
    /// 是否应该暂停扫描
    pub should_pause: bool,
    /// 建议的扫描延迟
    pub suggested_delay: Duration,
    /// 资源分配建议
    pub resource_allocation: ResourceAllocation,
    /// 决策原因
    pub reason: String,
}

/// 资源分配
#[derive(Debug, Clone)]
pub struct ResourceAllocation {
    /// 业务 IO 分配比例 (0-100)
    pub business_percentage: u8,
    /// 扫描器 IO 分配比例 (0-100)
    pub scanner_percentage: u8,
    /// 分配策略
    pub strategy: ResourceAllocationStrategy,
}

/// 增强的 IO 限流器
///
/// 根据实时系统负载和业务需求动态调节扫描器的资源使用，
/// 确保业务 IO 获得优先保障。
pub struct AdvancedIOThrottler {
    /// 配置
    config: Arc<RwLock<IOThrottlerConfig>>,
    /// 当前 IOPS 使用量 (预留字段)
    #[allow(dead_code)]
    current_iops: Arc<AtomicU64>,
    /// 业务优先级权重 (0-100)
    business_priority: Arc<AtomicU8>,
    /// 扫描操作间延迟 (毫秒)
    scan_delay: Arc<AtomicU64>,
    /// 资源分配策略
    allocation_strategy: Arc<RwLock<ResourceAllocationStrategy>>,
    /// 限流历史记录
    throttle_history: Arc<RwLock<Vec<ThrottleRecord>>>,
    /// 最后调节时间 (预留字段)
    #[allow(dead_code)]
    last_adjustment: Arc<RwLock<SystemTime>>,
}

/// 限流记录
#[derive(Debug, Clone)]
pub struct ThrottleRecord {
    /// 时间戳
    pub timestamp: SystemTime,
    /// 负载级别
    pub load_level: LoadLevel,
    /// 决策
    pub decision: ThrottleDecision,
    /// 系统指标快照
    pub metrics_snapshot: MetricsSnapshot,
}

/// 指标快照
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// IOPS
    pub iops: u64,
    /// 延迟
    pub latency: u64,
    /// CPU 使用率
    pub cpu_usage: u8,
    /// 内存使用率
    pub memory_usage: u8,
}

impl AdvancedIOThrottler {
    /// 创建新的高级 IO 限流器
    pub fn new(config: IOThrottlerConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            current_iops: Arc::new(AtomicU64::new(0)),
            business_priority: Arc::new(AtomicU8::new(95)),
            scan_delay: Arc::new(AtomicU64::new(5000)),
            allocation_strategy: Arc::new(RwLock::new(ResourceAllocationStrategy::BusinessFirst)),
            throttle_history: Arc::new(RwLock::new(Vec::new())),
            last_adjustment: Arc::new(RwLock::new(SystemTime::UNIX_EPOCH)),
        }
    }

    /// 根据负载级别调整扫描延迟
    pub async fn adjust_for_load_level(&self, load_level: LoadLevel) -> Duration {
        let config = self.config.read().await;

        let delay_ms = match load_level {
            LoadLevel::Low => {
                // 低负载：使用最小延迟
                self.scan_delay.store(config.min_scan_delay, Ordering::Relaxed);
                self.business_priority
                    .store(config.base_business_priority.saturating_sub(5), Ordering::Relaxed);
                config.min_scan_delay
            }
            LoadLevel::Medium => {
                // 中负载：适度增加延迟
                let delay = config.min_scan_delay * 5; // 500ms
                self.scan_delay.store(delay, Ordering::Relaxed);
                self.business_priority.store(config.base_business_priority, Ordering::Relaxed);
                delay
            }
            LoadLevel::High => {
                // 高负载：显著增加延迟
                let delay = config.min_scan_delay * 10; // 50s
                self.scan_delay.store(delay, Ordering::Relaxed);
                self.business_priority
                    .store(config.base_business_priority.saturating_add(3), Ordering::Relaxed);
                delay
            }
            LoadLevel::Critical => {
                // 关键负载：最大延迟或暂停
                let delay = config.max_scan_delay; // 60s
                self.scan_delay.store(delay, Ordering::Relaxed);
                self.business_priority.store(99, Ordering::Relaxed);
                delay
            }
        };

        let duration = Duration::from_millis(delay_ms);

        debug!("根据负载级别 {:?} 调整扫描延迟: {:?}", load_level, duration);

        duration
    }

    /// 创建限流决策
    pub async fn make_throttle_decision(&self, load_level: LoadLevel, metrics: Option<MetricsSnapshot>) -> ThrottleDecision {
        let _config = self.config.read().await;

        let should_pause = matches!(load_level, LoadLevel::Critical);

        let suggested_delay = self.adjust_for_load_level(load_level).await;

        let resource_allocation = self.calculate_resource_allocation(load_level).await;

        let reason = match load_level {
            LoadLevel::Low => "系统负载较低，扫描器可以正常运行".to_string(),
            LoadLevel::Medium => "系统负载适中，扫描器降速运行".to_string(),
            LoadLevel::High => "系统负载较高，扫描器显著降速".to_string(),
            LoadLevel::Critical => "系统负载过高，暂停扫描器运行".to_string(),
        };

        let decision = ThrottleDecision {
            should_pause,
            suggested_delay,
            resource_allocation,
            reason,
        };

        // 记录决策历史
        if let Some(snapshot) = metrics {
            self.record_throttle_decision(load_level, decision.clone(), snapshot).await;
        }

        decision
    }

    /// 计算资源分配
    async fn calculate_resource_allocation(&self, load_level: LoadLevel) -> ResourceAllocation {
        let strategy = *self.allocation_strategy.read().await;

        let (business_pct, scanner_pct) = match (strategy, load_level) {
            (ResourceAllocationStrategy::BusinessFirst, LoadLevel::Low) => (90, 10),
            (ResourceAllocationStrategy::BusinessFirst, LoadLevel::Medium) => (95, 5),
            (ResourceAllocationStrategy::BusinessFirst, LoadLevel::High) => (98, 2),
            (ResourceAllocationStrategy::BusinessFirst, LoadLevel::Critical) => (99, 1),

            (ResourceAllocationStrategy::Balanced, LoadLevel::Low) => (80, 20),
            (ResourceAllocationStrategy::Balanced, LoadLevel::Medium) => (85, 15),
            (ResourceAllocationStrategy::Balanced, LoadLevel::High) => (90, 10),
            (ResourceAllocationStrategy::Balanced, LoadLevel::Critical) => (95, 5),

            (ResourceAllocationStrategy::MaintenanceFirst, _) => (70, 30), // 特殊维护模式
        };

        ResourceAllocation {
            business_percentage: business_pct,
            scanner_percentage: scanner_pct,
            strategy,
        }
    }

    /// 检查是否应该暂停扫描
    pub async fn should_pause_scanning(&self, load_level: LoadLevel) -> bool {
        match load_level {
            LoadLevel::Critical => {
                warn!("系统负载达到关键级别，暂停扫描器");
                true
            }
            _ => false,
        }
    }

    /// 记录限流决策
    async fn record_throttle_decision(&self, load_level: LoadLevel, decision: ThrottleDecision, metrics: MetricsSnapshot) {
        let record = ThrottleRecord {
            timestamp: SystemTime::now(),
            load_level,
            decision,
            metrics_snapshot: metrics,
        };

        let mut history = self.throttle_history.write().await;
        history.push(record);

        // 保持历史记录在合理范围内（最近 1000 条）
        while history.len() > 1000 {
            history.remove(0);
        }
    }

    /// 设置资源分配策略
    pub async fn set_allocation_strategy(&self, strategy: ResourceAllocationStrategy) {
        *self.allocation_strategy.write().await = strategy;
        info!("设置资源分配策略: {:?}", strategy);
    }

    /// 获取当前资源分配
    pub async fn get_current_allocation(&self) -> ResourceAllocation {
        let current_load = LoadLevel::Low; // 需要从外部获取
        self.calculate_resource_allocation(current_load).await
    }

    /// 获取限流历史
    pub async fn get_throttle_history(&self) -> Vec<ThrottleRecord> {
        self.throttle_history.read().await.clone()
    }

    /// 获取限流统计
    pub async fn get_throttle_stats(&self) -> ThrottleStats {
        let history = self.throttle_history.read().await;

        let total_decisions = history.len();
        let pause_decisions = history.iter().filter(|r| r.decision.should_pause).count();

        let mut delay_sum = Duration::ZERO;
        for record in history.iter() {
            delay_sum += record.decision.suggested_delay;
        }

        let avg_delay = if total_decisions > 0 {
            delay_sum / total_decisions as u32
        } else {
            Duration::ZERO
        };

        // 按负载级别统计
        let low_count = history.iter().filter(|r| r.load_level == LoadLevel::Low).count();
        let medium_count = history.iter().filter(|r| r.load_level == LoadLevel::Medium).count();
        let high_count = history.iter().filter(|r| r.load_level == LoadLevel::High).count();
        let critical_count = history.iter().filter(|r| r.load_level == LoadLevel::Critical).count();

        ThrottleStats {
            total_decisions,
            pause_decisions,
            average_delay: avg_delay,
            load_level_distribution: LoadLevelDistribution {
                low_count,
                medium_count,
                high_count,
                critical_count,
            },
        }
    }

    /// 重置限流历史
    pub async fn reset_history(&self) {
        self.throttle_history.write().await.clear();
        info!("已重置限流历史记录");
    }

    /// 更新配置
    pub async fn update_config(&self, new_config: IOThrottlerConfig) {
        *self.config.write().await = new_config;
        info!("已更新 IO 限流器配置");
    }

    /// 获取当前扫描延迟
    pub fn get_current_scan_delay(&self) -> Duration {
        let delay_ms = self.scan_delay.load(Ordering::Relaxed);
        Duration::from_millis(delay_ms)
    }

    /// 获取当前业务优先级
    pub fn get_current_business_priority(&self) -> u8 {
        self.business_priority.load(Ordering::Relaxed)
    }

    /// 模拟业务负载压力测试
    pub async fn simulate_business_pressure(&self, duration: Duration) -> SimulationResult {
        info!("开始模拟业务负载压力测试，持续时间: {:?}", duration);

        let start_time = SystemTime::now();
        let mut simulation_records = Vec::new();

        // 模拟不同负载级别的变化
        let load_levels = [
            LoadLevel::Low,
            LoadLevel::Medium,
            LoadLevel::High,
            LoadLevel::Critical,
            LoadLevel::High,
            LoadLevel::Medium,
            LoadLevel::Low,
        ];

        let step_duration = duration / load_levels.len() as u32;

        for (i, &load_level) in load_levels.iter().enumerate() {
            let _step_start = SystemTime::now();

            // 模拟该负载级别下的指标
            let metrics = MetricsSnapshot {
                iops: match load_level {
                    LoadLevel::Low => 200,
                    LoadLevel::Medium => 500,
                    LoadLevel::High => 800,
                    LoadLevel::Critical => 1200,
                },
                latency: match load_level {
                    LoadLevel::Low => 10,
                    LoadLevel::Medium => 25,
                    LoadLevel::High => 60,
                    LoadLevel::Critical => 150,
                },
                cpu_usage: match load_level {
                    LoadLevel::Low => 30,
                    LoadLevel::Medium => 50,
                    LoadLevel::High => 75,
                    LoadLevel::Critical => 95,
                },
                memory_usage: match load_level {
                    LoadLevel::Low => 40,
                    LoadLevel::Medium => 60,
                    LoadLevel::High => 80,
                    LoadLevel::Critical => 90,
                },
            };

            let decision = self.make_throttle_decision(load_level, Some(metrics.clone())).await;

            simulation_records.push(SimulationRecord {
                step: i + 1,
                load_level,
                metrics,
                decision: decision.clone(),
                step_duration,
            });

            info!(
                "模拟步骤 {}: 负载={:?}, 延迟={:?}, 暂停={}",
                i + 1,
                load_level,
                decision.suggested_delay,
                decision.should_pause
            );

            // 等待步骤时间
            tokio::time::sleep(step_duration).await;
        }

        let total_duration = SystemTime::now().duration_since(start_time).unwrap_or(Duration::ZERO);

        SimulationResult {
            total_duration,
            simulation_records,
            final_stats: self.get_throttle_stats().await,
        }
    }
}

/// 限流统计
#[derive(Debug, Clone)]
pub struct ThrottleStats {
    /// 总决策数
    pub total_decisions: usize,
    /// 暂停决策数
    pub pause_decisions: usize,
    /// 平均延迟
    pub average_delay: Duration,
    /// 负载级别分布
    pub load_level_distribution: LoadLevelDistribution,
}

/// 负载级别分布
#[derive(Debug, Clone)]
pub struct LoadLevelDistribution {
    /// 低负载次数
    pub low_count: usize,
    /// 中负载次数
    pub medium_count: usize,
    /// 高负载次数
    pub high_count: usize,
    /// 关键负载次数
    pub critical_count: usize,
}

/// 模拟结果
#[derive(Debug, Clone)]
pub struct SimulationResult {
    /// 总持续时间
    pub total_duration: Duration,
    /// 模拟记录
    pub simulation_records: Vec<SimulationRecord>,
    /// 最终统计
    pub final_stats: ThrottleStats,
}

/// 模拟记录
#[derive(Debug, Clone)]
pub struct SimulationRecord {
    /// 步骤编号
    pub step: usize,
    /// 负载级别
    pub load_level: LoadLevel,
    /// 指标快照
    pub metrics: MetricsSnapshot,
    /// 限流决策
    pub decision: ThrottleDecision,
    /// 步骤持续时间
    pub step_duration: Duration,
}

impl Default for AdvancedIOThrottler {
    fn default() -> Self {
        Self::new(IOThrottlerConfig::default())
    }
}
