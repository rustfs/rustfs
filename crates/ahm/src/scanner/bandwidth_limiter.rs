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
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::{
    sync::RwLock,
    time::{sleep, sleep_until, Instant as TokioInstant},
};
use tracing::{debug, info, warn};

use crate::error::Result;

/// Configuration for bandwidth limiting
#[derive(Debug, Clone)]
pub struct BandwidthConfig {
    /// Maximum bytes per second
    pub bytes_per_second: u64,
    /// Maximum operations per second
    pub operations_per_second: u64,
    /// Burst allowance multiplier
    pub burst_multiplier: f64,
    /// Whether to enable adaptive throttling
    pub adaptive_throttling: bool,
    /// Minimum sleep duration between operations
    pub min_sleep_duration: Duration,
    /// Maximum sleep duration between operations
    pub max_sleep_duration: Duration,
}

impl Default for BandwidthConfig {
    fn default() -> Self {
        Self {
            bytes_per_second: 100 * 1024 * 1024, // 100 MB/s
            operations_per_second: 1000, // 1000 ops/s
            burst_multiplier: 2.0,
            adaptive_throttling: true,
            min_sleep_duration: Duration::from_micros(100),
            max_sleep_duration: Duration::from_millis(100),
        }
    }
}

/// Bandwidth limiter for controlling scan I/O rates
pub struct BandwidthLimiter {
    config: BandwidthConfig,
    bytes_this_second: Arc<AtomicU64>,
    operations_this_second: Arc<AtomicU64>,
    last_reset: Arc<RwLock<Instant>>,
    adaptive_sleep_duration: Arc<RwLock<Duration>>,
    total_bytes_processed: Arc<AtomicU64>,
    total_operations_processed: Arc<AtomicU64>,
    start_time: Instant,
}

impl BandwidthLimiter {
    /// Create a new bandwidth limiter
    pub fn new(config: BandwidthConfig) -> Self {
        let adaptive_sleep = if config.adaptive_throttling {
            config.min_sleep_duration
        } else {
            Duration::from_micros(1000) // 1ms default
        };

        Self {
            config,
            bytes_this_second: Arc::new(AtomicU64::new(0)),
            operations_this_second: Arc::new(AtomicU64::new(0)),
            last_reset: Arc::new(RwLock::new(Instant::now())),
            adaptive_sleep_duration: Arc::new(RwLock::new(adaptive_sleep)),
            total_bytes_processed: Arc::new(AtomicU64::new(0)),
            total_operations_processed: Arc::new(AtomicU64::new(0)),
            start_time: Instant::now(),
        }
    }

    /// Wait for bandwidth allowance before processing bytes
    pub async fn wait_for_bytes(&self, bytes: u64) -> Result<()> {
        if self.config.bytes_per_second == 0 {
            return Ok(());
        }

        let mut total_wait_time = Duration::ZERO;
        let mut remaining_bytes = bytes;

        while remaining_bytes > 0 {
            // Reset counters if a second has passed
            self.reset_counters_if_needed().await;

            let current_bytes = self.bytes_this_second.load(Ordering::Relaxed);
            let burst_limit = (self.config.bytes_per_second as f64 * self.config.burst_multiplier) as u64;

            if current_bytes >= burst_limit {
                // We're over the burst limit, wait
                let wait_time = self.calculate_wait_time(current_bytes, self.config.bytes_per_second).await;
                sleep(wait_time).await;
                total_wait_time += wait_time;
                continue;
            }

            let bytes_to_process = std::cmp::min(remaining_bytes, burst_limit - current_bytes);
            self.bytes_this_second.fetch_add(bytes_to_process, Ordering::Relaxed);
            self.total_bytes_processed.fetch_add(bytes_to_process, Ordering::Relaxed);
            remaining_bytes -= bytes_to_process;

            // Adaptive throttling
            if self.config.adaptive_throttling {
                self.update_adaptive_sleep(bytes_to_process).await;
            }
        }

        if total_wait_time > Duration::ZERO {
            debug!("Bandwidth limiter waited {:?} for {} bytes", total_wait_time, bytes);
        }

        Ok(())
    }

    /// Wait for bandwidth allowance before processing an operation
    pub async fn wait_for_operation(&self) -> Result<()> {
        if self.config.operations_per_second == 0 {
            return Ok(());
        }

        // Reset counters if a second has passed
        self.reset_counters_if_needed().await;

        let current_ops = self.operations_this_second.load(Ordering::Relaxed);
        let burst_limit = (self.config.operations_per_second as f64 * self.config.burst_multiplier) as u64;

        if current_ops >= burst_limit {
            // We're over the burst limit, wait
            let wait_time = self.calculate_wait_time(current_ops, self.config.operations_per_second).await;
            sleep(wait_time).await;
            debug!("Bandwidth limiter waited {:?} for operation", wait_time);
        }

        self.operations_this_second.fetch_add(1, Ordering::Relaxed);
        self.total_operations_processed.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Wait for bandwidth allowance before processing both bytes and operations
    pub async fn wait_for_bytes_and_operation(&self, bytes: u64) -> Result<()> {
        self.wait_for_bytes(bytes).await?;
        self.wait_for_operation().await?;
        Ok(())
    }

    /// Reset counters if a second has passed
    async fn reset_counters_if_needed(&self) {
        let mut last_reset = self.last_reset.write().await;
        let now = Instant::now();
        
        if now.duration_since(*last_reset) >= Duration::from_secs(1) {
            self.bytes_this_second.store(0, Ordering::Relaxed);
            self.operations_this_second.store(0, Ordering::Relaxed);
            *last_reset = now;
        }
    }

    /// Calculate wait time based on current usage and limit
    async fn calculate_wait_time(&self, current: u64, limit: u64) -> Duration {
        if current == 0 || limit == 0 {
            return self.config.min_sleep_duration;
        }

        let utilization = current as f64 / limit as f64;
        let base_sleep = self.config.min_sleep_duration.as_micros() as f64;
        let max_sleep = self.config.max_sleep_duration.as_micros() as f64;

        // Exponential backoff based on utilization
        let sleep_micros = base_sleep * (utilization * utilization);
        let sleep_micros = sleep_micros.min(max_sleep).max(base_sleep);

        Duration::from_micros(sleep_micros as u64)
    }

    /// Update adaptive sleep duration based on recent activity
    async fn update_adaptive_sleep(&self, bytes_processed: u64) {
        let mut sleep_duration = self.adaptive_sleep_duration.write().await;
        
        // Simple adaptive algorithm: increase sleep if we're processing too much
        let current_rate = bytes_processed as f64 / sleep_duration.as_secs_f64();
        let target_rate = self.config.bytes_per_second as f64;
        
        if current_rate > target_rate * 1.1 {
            // We're going too fast, increase sleep
            *sleep_duration = Duration::from_micros(
                (sleep_duration.as_micros() as f64 * 1.1) as u64
            ).min(self.config.max_sleep_duration);
        } else if current_rate < target_rate * 0.9 {
            // We're going too slow, decrease sleep
            *sleep_duration = Duration::from_micros(
                (sleep_duration.as_micros() as f64 * 0.9) as u64
            ).max(self.config.min_sleep_duration);
        }
    }

    /// Get current bandwidth statistics
    pub async fn statistics(&self) -> BandwidthStatistics {
        let elapsed = self.start_time.elapsed();
        let total_bytes = self.total_bytes_processed.load(Ordering::Relaxed);
        let total_ops = self.total_operations_processed.load(Ordering::Relaxed);
        let current_bytes = self.bytes_this_second.load(Ordering::Relaxed);
        let current_ops = self.operations_this_second.load(Ordering::Relaxed);
        let adaptive_sleep = *self.adaptive_sleep_duration.read().await;

        BandwidthStatistics {
            total_bytes_processed: total_bytes,
            total_operations_processed: total_ops,
            current_bytes_per_second: current_bytes,
            current_operations_per_second: current_ops,
            average_bytes_per_second: if elapsed.as_secs() > 0 {
                total_bytes / elapsed.as_secs()
            } else {
                0
            },
            average_operations_per_second: if elapsed.as_secs() > 0 {
                total_ops / elapsed.as_secs()
            } else {
                0
            },
            adaptive_sleep_duration: adaptive_sleep,
            uptime: elapsed,
        }
    }

    /// Reset all statistics
    pub async fn reset_statistics(&self) {
        self.total_bytes_processed.store(0, Ordering::Relaxed);
        self.total_operations_processed.store(0, Ordering::Relaxed);
        self.bytes_this_second.store(0, Ordering::Relaxed);
        self.operations_this_second.store(0, Ordering::Relaxed);
        *self.last_reset.write().await = Instant::now();
        *self.adaptive_sleep_duration.write().await = self.config.min_sleep_duration;
    }

    /// Update configuration
    pub async fn update_config(&self, new_config: BandwidthConfig) {
        info!("Updating bandwidth limiter config: {:?}", new_config);
        
        // Reset adaptive sleep if adaptive throttling is disabled
        if !new_config.adaptive_throttling {
            *self.adaptive_sleep_duration.write().await = new_config.min_sleep_duration;
        }
        
        // Note: We can't update the config struct itself since it's not wrapped in Arc<RwLock>
        // In a real implementation, you might want to wrap the config in Arc<RwLock> as well
        warn!("Config update not fully implemented - config struct is not mutable");
    }
}

/// Statistics for bandwidth limiting
#[derive(Debug, Clone)]
pub struct BandwidthStatistics {
    pub total_bytes_processed: u64,
    pub total_operations_processed: u64,
    pub current_bytes_per_second: u64,
    pub current_operations_per_second: u64,
    pub average_bytes_per_second: u64,
    pub average_operations_per_second: u64,
    pub adaptive_sleep_duration: Duration,
    pub uptime: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant as TokioInstant;

    #[tokio::test]
    async fn test_bandwidth_limiter_creation() {
        let config = BandwidthConfig::default();
        let limiter = BandwidthLimiter::new(config);
        let stats = limiter.statistics().await;
        assert_eq!(stats.total_bytes_processed, 0);
        assert_eq!(stats.total_operations_processed, 0);
    }

    #[tokio::test]
    async fn test_bytes_limiting() {
        let config = BandwidthConfig {
            bytes_per_second: 1000, // 1KB/s
            operations_per_second: 1000,
            ..Default::default()
        };
        let limiter = BandwidthLimiter::new(config);
        
        let start = TokioInstant::now();
        
        // Process 500 bytes (should not be limited)
        limiter.wait_for_bytes(500).await.unwrap();
        
        // Process another 600 bytes (should be limited)
        limiter.wait_for_bytes(600).await.unwrap();
        
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(100)); // Should take some time due to limiting
    }

    #[tokio::test]
    async fn test_operation_limiting() {
        let config = BandwidthConfig {
            bytes_per_second: 1000000, // 1MB/s
            operations_per_second: 10, // 10 ops/s
            ..Default::default()
        };
        let limiter = BandwidthLimiter::new(config);
        
        let start = TokioInstant::now();
        
        // Process 15 operations (should be limited)
        for _ in 0..15 {
            limiter.wait_for_operation().await.unwrap();
        }
        
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(500)); // Should take some time due to limiting
    }

    #[tokio::test]
    async fn test_statistics() {
        let config = BandwidthConfig::default();
        let limiter = BandwidthLimiter::new(config);
        
        limiter.wait_for_bytes(1000).await.unwrap();
        limiter.wait_for_operation().await.unwrap();
        
        let stats = limiter.statistics().await;
        assert_eq!(stats.total_bytes_processed, 1000);
        assert_eq!(stats.total_operations_processed, 1);
        assert!(stats.uptime > Duration::ZERO);
    }
} 