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

//! Deadlock detection management

use rustfs_io_core::{DeadlockDetector as CoreDeadlockDetector, DeadlockDetectorConfig as CoreDeadlockConfig, LockType};
use rustfs_io_metrics::deadlock_metrics;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Facade policy for the concurrency-layer deadlock monitor.
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
    /// Convert the facade policy into the reusable io-core deadlock config.
    pub fn to_core_config(&self) -> CoreDeadlockConfig {
        CoreDeadlockConfig {
            enabled: self.enabled,
            detection_interval: self.check_interval,
            max_hold_time: self.hang_threshold,
        }
    }
}

/// Deadlock manager
pub struct DeadlockManager {
    config: DeadlockMonitorPolicy,
    detector: Arc<CoreDeadlockDetector>,
    running: Arc<tokio::sync::Mutex<bool>>,
}

impl DeadlockManager {
    /// Create a new deadlock manager
    pub fn new(enabled: bool, check_interval: Duration, hang_threshold: Duration) -> Self {
        Self::from_policy(DeadlockMonitorPolicy {
            enabled,
            check_interval,
            hang_threshold,
        })
    }

    /// Create a new deadlock manager from the facade policy type.
    pub fn from_policy(config: DeadlockMonitorPolicy) -> Self {
        let core_config = config.to_core_config();
        Self {
            config,
            detector: Arc::new(CoreDeadlockDetector::new(core_config)),
            running: Arc::new(tokio::sync::Mutex::new(false)),
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &DeadlockMonitorPolicy {
        &self.config
    }

    /// Get the core detector
    pub fn detector(&self) -> Arc<CoreDeadlockDetector> {
        self.detector.clone()
    }

    /// Start the deadlock detection background task
    pub async fn start(&self) {
        if !self.config.enabled {
            return;
        }

        let mut running = self.running.lock().await;
        if *running {
            return;
        }
        *running = true;
        drop(running);

        tracing::info!("Deadlock detection started");
    }

    /// Stop the deadlock detection
    pub async fn stop(&self) {
        let mut running = self.running.lock().await;
        *running = false;

        tracing::info!("Deadlock detection stopped");
    }

    /// Create a request tracker
    pub fn track_request(&self, request_id: String, description: String) -> RequestTracker {
        RequestTracker::new(request_id, description, self.detector.clone())
    }

    /// Register a lock
    pub fn register_lock(&self, lock_type: LockType) -> u64 {
        self.detector.register_lock(lock_type)
    }

    /// Unregister a lock
    pub fn unregister_lock(&self, lock_id: u64) {
        self.detector.unregister_lock(lock_id);
    }

    /// Detect deadlock
    pub fn detect_deadlock(&self) -> Option<Vec<u64>> {
        let result = self.detector.detect_deadlock();
        if let Some(ref cycle) = result {
            deadlock_metrics::record_deadlock_detected(cycle.len());
        }
        result
    }
}

/// Lightweight compatibility wrapper for request-scoped deadlock bookkeeping.
///
/// This type intentionally stays minimal in the concurrency layer. Rich
/// request-level lock/resource diagnostics belong to
/// `rustfs::storage::deadlock_detector::RequestResourceTracker`.
pub struct RequestTracker {
    request_id: String,
    description: String,
    start_time: Instant,
    resources: HashMap<String, Vec<String>>,
    detector: Arc<CoreDeadlockDetector>,
}

impl RequestTracker {
    fn new(request_id: String, description: String, detector: Arc<CoreDeadlockDetector>) -> Self {
        let start_time = Instant::now();
        detector.register_request(&request_id, 1); // Use placeholder thread ID

        Self {
            request_id,
            description,
            start_time,
            resources: HashMap::new(),
            detector,
        }
    }

    /// Get the request ID
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Get the description
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Get the elapsed time
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Record a lock acquisition
    pub fn record_lock_acquire(&mut self, lock_id: u64, resource: String) {
        self.resources.entry("locks".to_string()).or_default().push(resource);
        self.detector.record_acquire(lock_id, 1); // Use placeholder thread ID
        deadlock_metrics::record_lock_acquisition("read");
    }

    /// Return a read-only view of tracked resource names.
    pub fn resources(&self) -> &HashMap<String, Vec<String>> {
        &self.resources
    }

    /// Record a lock release
    pub fn record_lock_release(&mut self, lock_id: u64) {
        self.detector.record_release(lock_id);
    }
}

impl Drop for RequestTracker {
    fn drop(&mut self) {
        self.detector.unregister_request(&self.request_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deadlock_manager_creation() {
        let manager = DeadlockManager::new(false, Duration::from_secs(10), Duration::from_secs(60));
        assert!(!manager.config().enabled);
    }

    #[test]
    fn test_deadlock_policy_to_core_config() {
        let policy = DeadlockMonitorPolicy::default();
        let core = policy.to_core_config();
        assert_eq!(core.enabled, policy.enabled);
        assert_eq!(core.detection_interval, policy.check_interval);
        assert_eq!(core.max_hold_time, policy.hang_threshold);
    }

    #[tokio::test]
    async fn test_request_tracker() {
        let manager = DeadlockManager::new(true, Duration::from_secs(10), Duration::from_secs(60));
        let mut tracker = manager.track_request("req-1".to_string(), "test request".to_string());
        let lock_id = manager.register_lock(LockType::Mutex);
        tracker.record_lock_acquire(lock_id, "bucket/key".to_string());

        assert_eq!(tracker.request_id(), "req-1");
        assert_eq!(tracker.description(), "test request");
        assert_eq!(tracker.resources().get("locks").map(Vec::len), Some(1));
    }
}
