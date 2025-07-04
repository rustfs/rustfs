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
use std::time::Duration;

/// Lock manager configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockConfig {
    /// Lock acquisition timeout
    #[serde(default = "default_timeout")]
    pub timeout: Duration,

    /// Retry interval
    #[serde(default = "default_retry_interval")]
    pub retry_interval: Duration,

    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,

    /// Lock refresh interval
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval: Duration,

    /// Connection pool size
    #[serde(default = "default_connection_pool_size")]
    pub connection_pool_size: usize,

    /// Enable metrics collection
    #[serde(default = "default_enable_metrics")]
    pub enable_metrics: bool,

    /// Enable tracing
    #[serde(default = "default_enable_tracing")]
    pub enable_tracing: bool,

    /// Distributed lock configuration
    #[serde(default)]
    pub distributed: DistributedConfig,

    /// Local lock configuration
    #[serde(default)]
    pub local: LocalConfig,
}

/// Distributed lock configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedConfig {
    /// Quorum ratio (0.0-1.0)
    #[serde(default = "default_quorum_ratio")]
    pub quorum_ratio: f64,

    /// Minimum quorum size
    #[serde(default = "default_min_quorum")]
    pub min_quorum: usize,

    /// Enable auto refresh
    #[serde(default = "default_auto_refresh")]
    pub auto_refresh: bool,

    /// Heartbeat interval
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: Duration,
}

/// Local lock configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalConfig {
    /// Maximum number of locks
    #[serde(default = "default_max_locks")]
    pub max_locks: usize,

    /// Lock cleanup interval
    #[serde(default = "default_cleanup_interval")]
    pub cleanup_interval: Duration,

    /// Lock expiry time
    #[serde(default = "default_lock_expiry")]
    pub lock_expiry: Duration,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            timeout: default_timeout(),
            retry_interval: default_retry_interval(),
            max_retries: default_max_retries(),
            refresh_interval: default_refresh_interval(),
            connection_pool_size: default_connection_pool_size(),
            enable_metrics: default_enable_metrics(),
            enable_tracing: default_enable_tracing(),
            distributed: DistributedConfig::default(),
            local: LocalConfig::default(),
        }
    }
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            quorum_ratio: default_quorum_ratio(),
            min_quorum: default_min_quorum(),
            auto_refresh: default_auto_refresh(),
            heartbeat_interval: default_heartbeat_interval(),
        }
    }
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            max_locks: default_max_locks(),
            cleanup_interval: default_cleanup_interval(),
            lock_expiry: default_lock_expiry(),
        }
    }
}

// Default value functions
fn default_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_retry_interval() -> Duration {
    Duration::from_millis(100)
}

fn default_max_retries() -> usize {
    3
}

fn default_refresh_interval() -> Duration {
    Duration::from_secs(10)
}

fn default_connection_pool_size() -> usize {
    10
}

fn default_enable_metrics() -> bool {
    true
}

fn default_enable_tracing() -> bool {
    true
}

fn default_quorum_ratio() -> f64 {
    0.5
}

fn default_min_quorum() -> usize {
    1
}

fn default_auto_refresh() -> bool {
    true
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_max_locks() -> usize {
    10000
}

fn default_cleanup_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_lock_expiry() -> Duration {
    Duration::from_secs(300)
}

impl LockConfig {
    /// Create minimal configuration
    pub fn minimal() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            retry_interval: Duration::from_millis(50),
            max_retries: 1,
            refresh_interval: Duration::from_secs(5),
            connection_pool_size: 5,
            enable_metrics: false,
            enable_tracing: false,
            distributed: DistributedConfig {
                quorum_ratio: 0.5,
                min_quorum: 1,
                auto_refresh: false,
                heartbeat_interval: Duration::from_secs(10),
            },
            local: LocalConfig {
                max_locks: 1000,
                cleanup_interval: Duration::from_secs(30),
                lock_expiry: Duration::from_secs(60),
            },
        }
    }

    /// Create high performance configuration
    pub fn high_performance() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            retry_interval: Duration::from_millis(10),
            max_retries: 5,
            refresh_interval: Duration::from_secs(30),
            connection_pool_size: 50,
            enable_metrics: true,
            enable_tracing: true,
            distributed: DistributedConfig {
                quorum_ratio: 0.7,
                min_quorum: 3,
                auto_refresh: true,
                heartbeat_interval: Duration::from_secs(2),
            },
            local: LocalConfig {
                max_locks: 100000,
                cleanup_interval: Duration::from_secs(300),
                lock_expiry: Duration::from_secs(1800),
            },
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.timeout.is_zero() {
            return Err(crate::error::LockError::configuration("Timeout must be greater than zero"));
        }

        if self.retry_interval.is_zero() {
            return Err(crate::error::LockError::configuration("Retry interval must be greater than zero"));
        }

        if self.max_retries == 0 {
            return Err(crate::error::LockError::configuration("Max retries must be greater than zero"));
        }

        if self.distributed.quorum_ratio < 0.0 || self.distributed.quorum_ratio > 1.0 {
            return Err(crate::error::LockError::configuration("Quorum ratio must be between 0.0 and 1.0"));
        }

        if self.distributed.min_quorum == 0 {
            return Err(crate::error::LockError::configuration("Minimum quorum must be greater than zero"));
        }

        Ok(())
    }

    /// Calculate quorum size for distributed locks
    pub fn calculate_quorum(&self, total_nodes: usize) -> usize {
        let quorum = (total_nodes as f64 * self.distributed.quorum_ratio).ceil() as usize;
        std::cmp::max(quorum, self.distributed.min_quorum)
    }

    /// Calculate fault tolerance
    pub fn calculate_tolerance(&self, total_nodes: usize) -> usize {
        total_nodes - self.calculate_quorum(total_nodes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = LockConfig::default();
        assert!(!config.timeout.is_zero());
        assert!(!config.retry_interval.is_zero());
        assert!(config.max_retries > 0);
    }

    #[test]
    fn test_minimal_config() {
        let config = LockConfig::minimal();
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert_eq!(config.max_retries, 1);
        assert!(!config.enable_metrics);
    }

    #[test]
    fn test_high_performance_config() {
        let config = LockConfig::high_performance();
        assert_eq!(config.timeout, Duration::from_secs(60));
        assert_eq!(config.max_retries, 5);
        assert!(config.enable_metrics);
    }

    #[test]
    fn test_config_validation() {
        let mut config = LockConfig::default();
        assert!(config.validate().is_ok());

        config.timeout = Duration::ZERO;
        assert!(config.validate().is_err());

        config = LockConfig::default();
        config.distributed.quorum_ratio = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_quorum_calculation() {
        let config = LockConfig::default();
        assert_eq!(config.calculate_quorum(10), 5);
        assert_eq!(config.calculate_quorum(3), 2);
        assert_eq!(config.calculate_tolerance(10), 5);
    }

    #[test]
    fn test_serialization() {
        let config = LockConfig::default();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: LockConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.timeout, deserialized.timeout);
        assert_eq!(config.max_retries, deserialized.max_retries);
    }
}
