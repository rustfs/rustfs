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

/// Lock system configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LockConfig {
    /// Whether distributed locking is enabled
    pub distributed_enabled: bool,
    /// Local lock configuration
    pub local: LocalLockConfig,
    /// Distributed lock configuration
    pub distributed: DistributedLockConfig,
    /// Network configuration
    pub network: NetworkConfig,
}

/// Local lock configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalLockConfig {
    /// Default lock timeout
    pub default_timeout: Duration,
    /// Default lock expiration time
    pub default_expiration: Duration,
    /// Maximum number of locks per resource
    pub max_locks_per_resource: usize,
}

/// Distributed lock configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedLockConfig {
    /// Total number of nodes in the cluster
    pub total_nodes: usize,
    /// Number of nodes that can fail (tolerance)
    pub tolerance: usize,
    /// Lock acquisition timeout
    pub acquisition_timeout: Duration,
    /// Lock refresh interval
    pub refresh_interval: Duration,
    /// Lock expiration time
    pub expiration_time: Duration,
    /// Retry interval for failed operations
    pub retry_interval: Duration,
    /// Maximum number of retry attempts
    pub max_retries: usize,
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Keep-alive interval
    pub keep_alive_interval: Duration,
    /// Maximum connection pool size
    pub max_connections: usize,
}

impl Default for LocalLockConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            default_expiration: Duration::from_secs(60),
            max_locks_per_resource: 1000,
        }
    }
}

impl Default for DistributedLockConfig {
    fn default() -> Self {
        Self {
            total_nodes: 3,
            tolerance: 1,
            acquisition_timeout: Duration::from_secs(30),
            refresh_interval: Duration::from_secs(10),
            expiration_time: Duration::from_secs(60),
            retry_interval: Duration::from_millis(250),
            max_retries: 10,
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            connection_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            keep_alive_interval: Duration::from_secs(30),
            max_connections: 100,
        }
    }
}

impl LockConfig {
    /// Create new lock configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Create distributed lock configuration
    pub fn distributed(total_nodes: usize, tolerance: usize) -> Self {
        Self {
            distributed_enabled: true,
            distributed: DistributedLockConfig {
                total_nodes,
                tolerance,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Create local-only lock configuration
    pub fn local() -> Self {
        Self {
            distributed_enabled: false,
            ..Default::default()
        }
    }

    /// Check if distributed locking is enabled
    pub fn is_distributed(&self) -> bool {
        self.distributed_enabled
    }

    /// Get quorum size for distributed locks
    pub fn get_quorum_size(&self) -> usize {
        self.distributed.total_nodes - self.distributed.tolerance
    }

    /// Check if quorum configuration is valid
    pub fn is_quorum_valid(&self) -> bool {
        self.distributed.tolerance < self.distributed.total_nodes
    }

    /// Get effective timeout
    pub fn get_effective_timeout(&self, timeout: Option<Duration>) -> Duration {
        timeout.unwrap_or(self.local.default_timeout)
    }

    /// Get effective expiration
    pub fn get_effective_expiration(&self, expiration: Option<Duration>) -> Duration {
        expiration.unwrap_or(self.local.default_expiration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_config_default() {
        let config = LockConfig::default();
        assert!(!config.distributed_enabled);
        assert_eq!(config.local.default_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_lock_config_distributed() {
        let config = LockConfig::distributed(5, 2);
        assert!(config.distributed_enabled);
        assert_eq!(config.distributed.total_nodes, 5);
        assert_eq!(config.distributed.tolerance, 2);
        assert_eq!(config.get_quorum_size(), 3);
    }

    #[test]
    fn test_lock_config_local() {
        let config = LockConfig::local();
        assert!(!config.distributed_enabled);
    }

    #[test]
    fn test_effective_timeout() {
        let config = LockConfig::default();
        assert_eq!(config.get_effective_timeout(None), Duration::from_secs(30));
        assert_eq!(config.get_effective_timeout(Some(Duration::from_secs(10))), Duration::from_secs(10));
    }

    #[test]
    fn test_effective_expiration() {
        let config = LockConfig::default();
        assert_eq!(config.get_effective_expiration(None), Duration::from_secs(60));
        assert_eq!(config.get_effective_expiration(Some(Duration::from_secs(30))), Duration::from_secs(30));
    }
}
