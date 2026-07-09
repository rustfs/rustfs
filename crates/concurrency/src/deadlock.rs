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

//! Shared deadlock-monitor policy type.
//!
//! The runtime request-hang / deadlock detection loop lives in
//! `rustfs/src/storage/deadlock_detector.rs`; this module only carries the
//! monitor policy type that implementation shares.

use rustfs_io_core::DeadlockDetectorConfig as CoreDeadlockConfig;
use std::time::Duration;

/// Policy for the request-hang deadlock monitor.
#[derive(Debug, Clone, Copy)]
pub struct DeadlockMonitorPolicy {
    /// Enable deadlock detection
    pub enabled: bool,
    /// Check interval
    pub check_interval: Duration,
    /// Hang threshold
    pub hang_threshold: Duration,
}

impl Default for DeadlockMonitorPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            check_interval: Duration::from_secs(10),
            hang_threshold: Duration::from_secs(60),
        }
    }
}

impl DeadlockMonitorPolicy {
    /// Convert the policy into the reusable io-core deadlock config.
    pub fn to_core_config(&self) -> CoreDeadlockConfig {
        CoreDeadlockConfig {
            enabled: self.enabled,
            detection_interval: self.check_interval,
            max_hold_time: self.hang_threshold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deadlock_policy_defaults_disabled() {
        let policy = DeadlockMonitorPolicy::default();
        assert!(!policy.enabled);
    }

    #[test]
    fn test_deadlock_policy_to_core_config() {
        let policy = DeadlockMonitorPolicy::default();
        let core = policy.to_core_config();
        assert_eq!(core.enabled, policy.enabled);
        assert_eq!(core.detection_interval, policy.check_interval);
        assert_eq!(core.max_hold_time, policy.hang_threshold);
    }
}
