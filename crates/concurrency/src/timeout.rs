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

//! Timeout management for operations

use rustfs_io_core::{TimeoutConfig as CoreTimeoutConfig, TimeoutError, calculate_adaptive_timeout};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Facade policy for the concurrency-layer timeout manager.
#[derive(Debug, Clone)]
pub struct TimeoutManagerPolicy {
    /// Default timeout duration
    pub default_timeout: Duration,
    /// Maximum timeout duration
    pub max_timeout: Duration,
    /// Enable dynamic timeout calculation
    pub enable_dynamic: bool,
}

impl Default for TimeoutManagerPolicy {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            max_timeout: Duration::from_secs(300),
            enable_dynamic: true,
        }
    }
}

impl TimeoutManagerPolicy {
    /// Convert the facade policy into the reusable io-core timeout configuration.
    ///
    /// This keeps the concurrency layer explicitly wired to the shared core
    /// timeout primitives without changing the facade's public behavior.
    pub fn to_core_config(&self) -> CoreTimeoutConfig {
        CoreTimeoutConfig {
            base_timeout: self.default_timeout,
            timeout_per_mb: Duration::ZERO,
            max_timeout: self.max_timeout,
            min_timeout: Duration::from_secs(1),
            get_object_timeout: self.default_timeout,
            put_object_timeout: self.max_timeout,
            list_objects_timeout: self.default_timeout,
            enable_dynamic_timeout: self.enable_dynamic,
        }
    }
}

/// Backward-compatible alias for the old timeout facade name.
pub type TimeoutConfig = TimeoutManagerPolicy;

/// Timeout manager
pub struct TimeoutManager {
    config: TimeoutManagerPolicy,
    core_config: CoreTimeoutConfig,
}

impl TimeoutManager {
    /// Create a new timeout manager
    pub fn new(default_timeout: Duration, max_timeout: Duration, enable_dynamic: bool) -> Self {
        Self::from_policy(TimeoutManagerPolicy {
            default_timeout,
            max_timeout,
            enable_dynamic,
        })
    }

    /// Create a new timeout manager from the facade policy type.
    pub fn from_policy(config: TimeoutManagerPolicy) -> Self {
        let core_config = config.to_core_config();
        Self { config, core_config }
    }

    /// Get the configuration
    pub fn config(&self) -> &TimeoutManagerPolicy {
        &self.config
    }

    /// Get the derived io-core timeout configuration.
    pub fn core_config(&self) -> &CoreTimeoutConfig {
        &self.core_config
    }

    /// Calculate timeout for a given size
    pub fn calculate_timeout(&self, size: u64, _history: &[Duration]) -> Duration {
        if !self.config.enable_dynamic {
            return self.config.default_timeout;
        }

        calculate_adaptive_timeout(self.core_config.base_timeout, None, 0, size)
            .clamp(self.core_config.min_timeout, self.core_config.max_timeout)
    }

    /// Wrap an operation with timeout control
    pub async fn wrap_operation<F, T, E>(&self, operation: F, timeout: Option<Duration>) -> Result<T, TimeoutError>
    where
        F: std::future::Future<Output = Result<T, E>>,
        E: Into<TimeoutError>,
    {
        let timeout = timeout.unwrap_or(self.config.default_timeout);

        match tokio::time::timeout(timeout, operation).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Err(TimeoutError::TimedOut(timeout)),
        }
    }

    /// Create a timeout guard for manual timeout control
    pub fn create_guard(&self, timeout: Option<Duration>) -> TimeoutGuard {
        TimeoutGuard::new(timeout.unwrap_or(self.core_config.base_timeout))
    }
}

/// Timeout guard for manual timeout control
pub struct TimeoutGuard {
    timeout: Duration,
    start: Instant,
    cancel_token: CancellationToken,
}

impl TimeoutGuard {
    fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            start: Instant::now(),
            cancel_token: CancellationToken::new(),
        }
    }

    /// Check if timeout has elapsed
    pub fn is_timed_out(&self) -> bool {
        self.start.elapsed() > self.timeout
    }

    /// Get remaining time
    pub fn remaining(&self) -> Duration {
        self.timeout.saturating_sub(self.start.elapsed())
    }

    /// Get the cancellation token
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Cancel the operation
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeout_config() {
        let config = TimeoutManagerPolicy::default();
        assert!(config.default_timeout < config.max_timeout);
    }

    #[test]
    fn test_timeout_policy_to_core_config() {
        let policy = TimeoutManagerPolicy::default();
        let core = policy.to_core_config();
        assert_eq!(core.base_timeout, policy.default_timeout);
        assert_eq!(core.max_timeout, policy.max_timeout);
        assert_eq!(core.get_object_timeout, policy.default_timeout);
        assert!(core.enable_dynamic_timeout);
    }

    #[tokio::test]
    async fn test_wrap_operation_success() {
        let manager = TimeoutManager::new(Duration::from_secs(5), Duration::from_secs(10), true);

        let result = manager.wrap_operation(async { Ok::<_, TimeoutError>(42) }, None).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }
}
