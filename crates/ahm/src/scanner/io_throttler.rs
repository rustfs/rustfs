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

use crate::scanner::LoadLevel;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// IO throttler config
#[derive(Debug, Clone)]
pub struct IOThrottlerConfig {
    /// max IOPS limit
    pub max_iops: u64,
    /// business priority baseline (percentage)
    pub base_business_priority: u8,
    /// scanner minimum delay (milliseconds)
    pub min_scan_delay: u64,
    /// scanner maximum delay (milliseconds)
    pub max_scan_delay: u64,
    /// whether enable dynamic adjustment
    pub enable_dynamic_adjustment: bool,
    /// adjustment response time (seconds)
    pub adjustment_response_time: u64,
}

impl Default for IOThrottlerConfig {
    fn default() -> Self {
        Self {
            max_iops: 1000,             // default max 1000 IOPS
            base_business_priority: 95, // business priority 95%
            min_scan_delay: 5000,       // minimum 5s delay
            max_scan_delay: 60000,      // maximum 60s delay
            enable_dynamic_adjustment: true,
            adjustment_response_time: 5, // 5 seconds response time
        }
    }
}

/// resource allocation strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceAllocationStrategy {
    /// business priority strategy
    BusinessFirst,
    /// balanced strategy
    Balanced,
    /// maintenance priority strategy (only used in special cases)
    MaintenanceFirst,
}

/// throttle decision
#[derive(Debug, Clone)]
pub struct ThrottleDecision {
    /// whether should pause scanning
    pub should_pause: bool,
    /// suggested scanning delay
    pub suggested_delay: Duration,
    /// resource allocation suggestion
    pub resource_allocation: ResourceAllocation,
    /// decision reason
    pub reason: String,
}

/// resource allocation
#[derive(Debug, Clone)]
pub struct ResourceAllocation {
    /// business IO allocation percentage (0-100)
    pub business_percentage: u8,
    /// scanner IO allocation percentage (0-100)
    pub scanner_percentage: u8,
    /// allocation strategy
    pub strategy: ResourceAllocationStrategy,
}

/// enhanced IO throttler
///
/// dynamically adjust the resource usage of the scanner based on real-time system load and business demand,
/// ensure business IO gets priority protection.
pub struct AdvancedIOThrottler {
    /// config
    config: Arc<RwLock<IOThrottlerConfig>>,
    /// current IOPS usage (reserved field)
    #[allow(dead_code)]
    current_iops: Arc<AtomicU64>,
    /// business priority weight (0-100)
    business_priority: Arc<AtomicU8>,
    /// scanning operation delay (milliseconds)
    scan_delay: Arc<AtomicU64>,
    /// resource allocation strategy
    allocation_strategy: Arc<RwLock<ResourceAllocationStrategy>>,
    /// throttle history record
    throttle_history: Arc<RwLock<Vec<ThrottleRecord>>>,
    /// last adjustment time (reserved field)
    #[allow(dead_code)]
    last_adjustment: Arc<RwLock<SystemTime>>,
}

/// throttle record
#[derive(Debug, Clone)]
pub struct ThrottleRecord {
    /// timestamp
    pub timestamp: SystemTime,
    /// load level
    pub load_level: LoadLevel,
    /// decision
    pub decision: ThrottleDecision,
    /// system metrics snapshot
    pub metrics_snapshot: MetricsSnapshot,
}

/// metrics snapshot
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// IOPS
    pub iops: u64,
    /// latency
    pub latency: u64,
    /// CPU usage
    pub cpu_usage: u8,
    /// memory usage
    pub memory_usage: u8,
}

impl AdvancedIOThrottler {
    /// create new advanced IO throttler
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

    /// adjust scanning delay based on load level
    pub async fn adjust_for_load_level(&self, load_level: LoadLevel) -> Duration {
        let config = self.config.read().await;

        let delay_ms = match load_level {
            LoadLevel::Low => {
                // low load: use minimum delay
                self.scan_delay.store(config.min_scan_delay, Ordering::Relaxed);
                self.business_priority
                    .store(config.base_business_priority.saturating_sub(5), Ordering::Relaxed);
                config.min_scan_delay
            }
            LoadLevel::Medium => {
                // medium load: increase delay moderately
                let delay = config.min_scan_delay * 5; // 500ms
                self.scan_delay.store(delay, Ordering::Relaxed);
                self.business_priority.store(config.base_business_priority, Ordering::Relaxed);
                delay
            }
            LoadLevel::High => {
                // high load: increase delay significantly
                let delay = config.min_scan_delay * 10; // 50s
                self.scan_delay.store(delay, Ordering::Relaxed);
                self.business_priority
                    .store(config.base_business_priority.saturating_add(3), Ordering::Relaxed);
                delay
            }
            LoadLevel::Critical => {
                // critical load: maximum delay or pause
                let delay = config.max_scan_delay; // 60s
                self.scan_delay.store(delay, Ordering::Relaxed);
                self.business_priority.store(99, Ordering::Relaxed);
                delay
            }
        };

        let duration = Duration::from_millis(delay_ms);

        debug!("Adjust scanning delay based on load level {:?}: {:?}", load_level, duration);

        duration
    }

    /// create throttle decision
    pub async fn make_throttle_decision(&self, load_level: LoadLevel, metrics: Option<MetricsSnapshot>) -> ThrottleDecision {
        let _config = self.config.read().await;

        let should_pause = matches!(load_level, LoadLevel::Critical);

        let suggested_delay = self.adjust_for_load_level(load_level).await;

        let resource_allocation = self.calculate_resource_allocation(load_level).await;

        let reason = match load_level {
            LoadLevel::Low => "system load is low, scanner can run normally".to_string(),
            LoadLevel::Medium => "system load is moderate, scanner is running at reduced speed".to_string(),
            LoadLevel::High => "system load is high, scanner is running at significantly reduced speed".to_string(),
            LoadLevel::Critical => "system load is too high, scanner is paused".to_string(),
        };

        let decision = ThrottleDecision {
            should_pause,
            suggested_delay,
            resource_allocation,
            reason,
        };

        // record decision history
        if let Some(snapshot) = metrics {
            self.record_throttle_decision(load_level, decision.clone(), snapshot).await;
        }

        decision
    }

    /// calculate resource allocation
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

            (ResourceAllocationStrategy::MaintenanceFirst, _) => (70, 30), // special maintenance mode
        };

        ResourceAllocation {
            business_percentage: business_pct,
            scanner_percentage: scanner_pct,
            strategy,
        }
    }

    /// check whether should pause scanning
    pub async fn should_pause_scanning(&self, load_level: LoadLevel) -> bool {
        match load_level {
            LoadLevel::Critical => {
                warn!("System load reached critical level, pausing scanner");
                true
            }
            _ => false,
        }
    }

    /// record throttle decision
    async fn record_throttle_decision(&self, load_level: LoadLevel, decision: ThrottleDecision, metrics: MetricsSnapshot) {
        let record = ThrottleRecord {
            timestamp: SystemTime::now(),
            load_level,
            decision,
            metrics_snapshot: metrics,
        };

        let mut history = self.throttle_history.write().await;
        history.push(record);

        // keep history record in reasonable range (last 1000 records)
        while history.len() > 1000 {
            history.remove(0);
        }
    }

    /// set resource allocation strategy
    pub async fn set_allocation_strategy(&self, strategy: ResourceAllocationStrategy) {
        *self.allocation_strategy.write().await = strategy;
        info!("Set resource allocation strategy: {:?}", strategy);
    }

    /// get current resource allocation
    pub async fn get_current_allocation(&self) -> ResourceAllocation {
        let current_load = LoadLevel::Low; // need to get from external
        self.calculate_resource_allocation(current_load).await
    }

    /// get throttle history
    pub async fn get_throttle_history(&self) -> Vec<ThrottleRecord> {
        self.throttle_history.read().await.clone()
    }

    /// get throttle stats
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

        // count by load level
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

    /// reset throttle history
    pub async fn reset_history(&self) {
        self.throttle_history.write().await.clear();
        info!("Reset throttle history");
    }

    /// update config
    pub async fn update_config(&self, new_config: IOThrottlerConfig) {
        *self.config.write().await = new_config;
        info!("Updated IO throttler configuration");
    }

    /// get current scanning delay
    pub fn get_current_scan_delay(&self) -> Duration {
        let delay_ms = self.scan_delay.load(Ordering::Relaxed);
        Duration::from_millis(delay_ms)
    }

    /// get current business priority
    pub fn get_current_business_priority(&self) -> u8 {
        self.business_priority.load(Ordering::Relaxed)
    }

    /// simulate business load pressure test
    pub async fn simulate_business_pressure(&self, duration: Duration) -> SimulationResult {
        info!("Start simulating business load pressure test, duration: {:?}", duration);

        let start_time = SystemTime::now();
        let mut simulation_records = Vec::new();

        // simulate different load level changes
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

            // simulate metrics for this load level
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
                "simulate step {}: load={:?}, delay={:?}, pause={}",
                i + 1,
                load_level,
                decision.suggested_delay,
                decision.should_pause
            );

            // wait for step duration
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

/// throttle stats
#[derive(Debug, Clone)]
pub struct ThrottleStats {
    /// total decisions
    pub total_decisions: usize,
    /// pause decisions
    pub pause_decisions: usize,
    /// average delay
    pub average_delay: Duration,
    /// load level distribution
    pub load_level_distribution: LoadLevelDistribution,
}

/// load level distribution
#[derive(Debug, Clone)]
pub struct LoadLevelDistribution {
    /// low load count
    pub low_count: usize,
    /// medium load count
    pub medium_count: usize,
    /// high load count
    pub high_count: usize,
    /// critical load count
    pub critical_count: usize,
}

/// simulation result
#[derive(Debug, Clone)]
pub struct SimulationResult {
    /// total duration
    pub total_duration: Duration,
    /// simulation records
    pub simulation_records: Vec<SimulationRecord>,
    /// final stats
    pub final_stats: ThrottleStats,
}

/// simulation record
#[derive(Debug, Clone)]
pub struct SimulationRecord {
    /// step number
    pub step: usize,
    /// load level
    pub load_level: LoadLevel,
    /// metrics snapshot
    pub metrics: MetricsSnapshot,
    /// throttle decision
    pub decision: ThrottleDecision,
    /// step duration
    pub step_duration: Duration,
}

impl Default for AdvancedIOThrottler {
    fn default() -> Self {
        Self::new(IOThrottlerConfig::default())
    }
}
