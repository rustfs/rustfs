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

use rustfs_io_core::{TimeoutError, calculate_adaptive_timeout};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Timeout configuration
#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    /// Default timeout duration
    pub default_timeout: Duration,
    /// Maximum timeout duration
    pub max_timeout: Duration,
    /// Enable dynamic timeout calculation
    pub enable_dynamic: bool,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            max_timeout: Duration::from_secs(300),
            enable_dynamic: true,
        }
    }
}

/// Timeout manager
pub struct TimeoutManager {
    config: TimeoutConfig,
}

impl TimeoutManager {
    /// Create a new timeout manager
    pub fn new(default_timeout: Duration, max_timeout: Duration, enable_dynamic: bool) -> Self {
        Self {
            config: TimeoutConfig {
                default_timeout,
                max_timeout,
                enable_dynamic,
            },
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &TimeoutConfig {
        &self.config
    }

    /// Calculate timeout for a given size
    pub fn calculate_timeout(&self, size: u64, _history: &[Duration]) -> Duration {
        if !self.config.enable_dynamic {
            return self.config.default_timeout;
        }

        calculate_adaptive_timeout(self.config.default_timeout, None, 0, size).min(self.config.max_timeout)
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
        TimeoutGuard::new(timeout.unwrap_or(self.config.default_timeout))
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
        let config = TimeoutConfig::default();
        assert!(config.default_timeout < config.max_timeout);
    }

    #[tokio::test]
    async fn test_wrap_operation_success() {
        let manager = TimeoutManager::new(Duration::from_secs(5), Duration::from_secs(10), true);

        let result = manager.wrap_operation(async { Ok::<_, TimeoutError>(42) }, None).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }
}
