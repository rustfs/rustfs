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

//! Lock Health Monitoring (P3 - Proactive lock health checking)
//!
//! This module provides proactive monitoring of distributed lock health to detect
//! and prevent deadlocks before they impact operations.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

/// Lock health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockHealth {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Lock statistics for health tracking
#[derive(Debug, Clone)]
struct LockStats {
    total_attempts: u64,
    timeouts: u64,
    successes: u64,
    last_timeout: Option<Instant>,
    consecutive_timeouts: u32,
    last_success: Option<Instant>,
}

impl LockStats {
    fn new() -> Self {
        Self {
            total_attempts: 0,
            timeouts: 0,
            successes: 0,
            last_timeout: None,
            consecutive_timeouts: 0,
            last_success: None,
        }
    }

    /// Calculate lock health based on recent statistics
    fn calculate_health(&self) -> LockHealth {
        // Unhealthy: 3+ consecutive timeouts OR 50%+ timeout rate
        if self.consecutive_timeouts >= 3 {
            return LockHealth::Unhealthy;
        }

        if self.total_attempts >= 10 {
            let timeout_rate = self.timeouts as f64 / self.total_attempts as f64;
            if timeout_rate > 0.5 {
                return LockHealth::Unhealthy;
            }
            if timeout_rate > 0.2 {
                return LockHealth::Degraded;
            }
        }

        // Degraded: Recent timeout (within 30s)
        if let Some(last_timeout) = self.last_timeout {
            if last_timeout.elapsed() < Duration::from_secs(30) {
                return LockHealth::Degraded;
            }
        }

        LockHealth::Healthy
    }
}

/// Global lock health monitor
pub struct LockHealthMonitor {
    stats: Arc<RwLock<HashMap<String, LockStats>>>,
}

impl LockHealthMonitor {
    pub fn new() -> Self {
        Self {
            stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record a lock attempt
    pub async fn record_attempt(&self, lock_path: &str) {
        let mut stats = self.stats.write().await;
        let stat = stats.entry(lock_path.to_string()).or_insert_with(LockStats::new);
        stat.total_attempts += 1;
    }

    /// Record a lock timeout
    pub async fn record_timeout(&self, lock_path: &str) {
        let mut stats = self.stats.write().await;
        let stat = stats.entry(lock_path.to_string()).or_insert_with(LockStats::new);
        stat.timeouts += 1;
        stat.last_timeout = Some(Instant::now());
        stat.consecutive_timeouts += 1;

        let health = stat.calculate_health();
        metrics::counter!("rustfs_lock_health_timeouts_total", "path" => lock_path.to_string()).increment(1);

        if health == LockHealth::Unhealthy {
            error!(
                "Lock {} is UNHEALTHY: {} consecutive timeouts, {}/{} timeout rate",
                lock_path, stat.consecutive_timeouts, stat.timeouts, stat.total_attempts
            );
            metrics::gauge!("rustfs_lock_health_status", "path" => lock_path.to_string()).set(0.0);
        } else if health == LockHealth::Degraded {
            warn!("Lock {} is DEGRADED: recent timeout detected", lock_path);
            metrics::gauge!("rustfs_lock_health_status", "path" => lock_path.to_string()).set(0.5);
        }
    }

    /// Record a lock success
    pub async fn record_success(&self, lock_path: &str) {
        let mut stats = self.stats.write().await;
        let stat = stats.entry(lock_path.to_string()).or_insert_with(LockStats::new);
        stat.successes += 1;
        stat.last_success = Some(Instant::now());
        stat.consecutive_timeouts = 0; // Reset on success

        let health = stat.calculate_health();
        if health == LockHealth::Healthy {
            metrics::gauge!("rustfs_lock_health_status", "path" => lock_path.to_string()).set(1.0);
        }
    }

    /// Get current health of a lock
    pub async fn get_health(&self, lock_path: &str) -> LockHealth {
        let stats = self.stats.read().await;
        match stats.get(lock_path) {
            Some(stat) => stat.calculate_health(),
            None => LockHealth::Healthy, // Unknown locks are assumed healthy
        }
    }

    /// Check if operation should proceed based on lock health
    pub async fn should_attempt_operation(&self, lock_path: &str) -> bool {
        let health = self.get_health(lock_path).await;
        match health {
            LockHealth::Healthy => true,
            LockHealth::Degraded => {
                debug!("Lock {} is degraded but allowing operation", lock_path);
                true
            }
            LockHealth::Unhealthy => {
                warn!("Lock {} is unhealthy, operation may fail", lock_path);
                // Still allow but with warning - operator may need to intervene
                true
            }
        }
    }

    /// Get statistics for all locks (for monitoring dashboard)
    pub async fn get_all_stats(&self) -> HashMap<String, (LockHealth, u64, u64)> {
        let stats = self.stats.read().await;
        stats
            .iter()
            .map(|(path, stat)| {
                let health = stat.calculate_health();
                (path.clone(), (health, stat.total_attempts, stat.timeouts))
            })
            .collect()
    }

    /// Background task to periodically clean old statistics
    pub async fn cleanup_old_stats(&self) {
        const CLEANUP_INTERVAL_SECS: u64 = 300; // 5 minutes
        const STAT_EXPIRY_SECS: u64 = 600; // 10 minutes

        loop {
            tokio::time::sleep(Duration::from_secs(CLEANUP_INTERVAL_SECS)).await;

            let mut stats = self.stats.write().await;
            let now = Instant::now();
            stats.retain(|path, stat| {
                // Keep if there was recent activity
                if let Some(last_success) = stat.last_success {
                    if now.duration_since(last_success) < Duration::from_secs(STAT_EXPIRY_SECS) {
                        return true;
                    }
                }
                if let Some(last_timeout) = stat.last_timeout {
                    if now.duration_since(last_timeout) < Duration::from_secs(STAT_EXPIRY_SECS) {
                        return true;
                    }
                }
                debug!("Cleaning up old lock stats for: {}", path);
                false
            });
        }
    }
}

impl Default for LockHealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

// Global lock health monitor instance
use std::sync::LazyLock;
pub static GLOBAL_LOCK_HEALTH_MONITOR: LazyLock<LockHealthMonitor> = LazyLock::new(LockHealthMonitor::new);

/// Helper function to check lock health before operation
pub async fn check_lock_health(lock_path: &str) -> LockHealth {
    GLOBAL_LOCK_HEALTH_MONITOR.get_health(lock_path).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lock_health_tracking() {
        let monitor = LockHealthMonitor::new();
        let lock_path = "test/lock/path";

        // Initially healthy
        assert_eq!(monitor.get_health(lock_path).await, LockHealth::Healthy);

        // After timeout, becomes degraded
        monitor.record_attempt(lock_path).await;
        monitor.record_timeout(lock_path).await;
        assert_eq!(monitor.get_health(lock_path).await, LockHealth::Degraded);

        // After 3 consecutive timeouts, becomes unhealthy
        monitor.record_attempt(lock_path).await;
        monitor.record_timeout(lock_path).await;
        monitor.record_attempt(lock_path).await;
        monitor.record_timeout(lock_path).await;
        assert_eq!(monitor.get_health(lock_path).await, LockHealth::Unhealthy);

        // After success, resets consecutive timeouts
        monitor.record_attempt(lock_path).await;
        monitor.record_success(lock_path).await;
        let health = monitor.get_health(lock_path).await;
        assert!(health == LockHealth::Healthy || health == LockHealth::Degraded);
    }

    #[tokio::test]
    async fn test_timeout_rate_calculation() {
        let monitor = LockHealthMonitor::new();
        let lock_path = "test/lock/rate";

        // Simulate high timeout rate (60%)
        for _ in 0..6 {
            monitor.record_attempt(lock_path).await;
            monitor.record_timeout(lock_path).await;
        }
        for _ in 0..4 {
            monitor.record_attempt(lock_path).await;
            monitor.record_success(lock_path).await;
        }

        assert_eq!(monitor.get_health(lock_path).await, LockHealth::Unhealthy);
    }
}
