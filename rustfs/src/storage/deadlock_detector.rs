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

//! Deadlock Detection for Concurrent Request Monitoring.
//!
//! This module provides deadlock detection capabilities for diagnosing
//! hanging requests and lock contention issues in production systems.
//!
//! # Migration Note
//!
//! This module extends `rustfs_io_core::DeadlockDetector` with request-level
//! resource tracking (memory, file handles). For basic deadlock detection,
//! consider using the io-core version directly:
//!
//! ```ignore
//! // Basic deadlock detection
//! use rustfs_io_core::DeadlockDetector;
//! let detector = DeadlockDetector::with_defaults();
//! ```
//!
//! # Key Features
//!
//! - Request resource tracking (locks, memory, file handles)
//! - Lock wait graph analysis for cycle detection
//! - Configurable detection interval and hang threshold
//! - Deadlock metrics emitted through the shared metrics pipeline
//! - Detailed diagnostic logging
//!
//! # Usage
//!
//! ```ignore
//! use crate::storage::deadlock_detector::{DeadlockDetector, DeadlockDetectorConfig};
//!
//! let config = DeadlockDetectorConfig::from_env();
//! let detector = DeadlockDetector::new(config);
//! detector.start();
//!
//! // Track a request
//! let request_id = detector.register_request();
//! detector.record_lock_acquire(request_id, lock_info);
//!
//! // ... request processing ...
//!
//! detector.unregister_request(request_id);
//! ```

// Allow dead_code for public API that may be used by external modules or future features
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing::{debug, error, warn};

use metrics::counter;

/// Request identifier type.
pub type RequestId = String;

/// Lock identifier type.
pub type LockId = String;

/// Deadlock detector configuration.
#[derive(Debug, Clone)]
pub struct DeadlockDetectorConfig {
    /// Whether deadlock detection is enabled.
    pub enabled: bool,
    /// Detection check interval.
    pub check_interval: Duration,
    /// Hang threshold - requests running longer than this are considered potentially hung.
    pub hang_threshold: Duration,
    /// Whether to capture backtraces (expensive, only for debugging).
    pub capture_backtrace: bool,
}

impl Default for DeadlockDetectorConfig {
    fn default() -> Self {
        Self {
            enabled: rustfs_config::DEFAULT_OBJECT_DEADLOCK_DETECTION_ENABLE,
            check_interval: Duration::from_secs(rustfs_config::DEFAULT_OBJECT_DEADLOCK_CHECK_INTERVAL),
            hang_threshold: Duration::from_secs(rustfs_config::DEFAULT_OBJECT_DEADLOCK_HANG_THRESHOLD),
            capture_backtrace: false,
        }
    }
}

impl DeadlockDetectorConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let enabled = rustfs_utils::get_env_bool(
            rustfs_config::ENV_OBJECT_DEADLOCK_DETECTION_ENABLE,
            rustfs_config::DEFAULT_OBJECT_DEADLOCK_DETECTION_ENABLE,
        );
        let check_interval = Duration::from_secs(rustfs_utils::get_env_u64(
            rustfs_config::ENV_OBJECT_DEADLOCK_CHECK_INTERVAL,
            rustfs_config::DEFAULT_OBJECT_DEADLOCK_CHECK_INTERVAL,
        ));
        let hang_threshold = Duration::from_secs(rustfs_utils::get_env_u64(
            rustfs_config::ENV_OBJECT_DEADLOCK_HANG_THRESHOLD,
            rustfs_config::DEFAULT_OBJECT_DEADLOCK_HANG_THRESHOLD,
        ));

        Self {
            enabled,
            check_interval,
            hang_threshold,
            capture_backtrace: false,
        }
    }
}

/// Lock information for tracking.
#[derive(Debug, Clone)]
pub struct LockInfo {
    /// Lock identifier.
    pub id: LockId,
    /// Lock type (read/write).
    pub lock_type: LockType,
    /// Resource being locked (bucket/key).
    pub resource: String,
    /// When the lock was acquired.
    pub acquire_time: Instant,
}

/// Lock type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    /// Read lock (shared).
    Read,
    /// Write lock (exclusive).
    Write,
}

impl std::fmt::Display for LockType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockType::Read => write!(f, "read"),
            LockType::Write => write!(f, "write"),
        }
    }
}

/// Resource type being tracked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceType {
    /// Lock resource.
    Lock,
    /// Memory resource.
    Memory,
    /// File handle.
    FileHandle,
    /// I/O permit.
    IoPermit,
}

/// Request resource tracker.
#[derive(Debug, Clone)]
pub struct RequestResourceTracker {
    /// Request ID.
    pub request_id: RequestId,
    /// When the request started.
    pub start_time: Instant,
    /// Locks currently held by this request.
    pub held_locks: Vec<LockInfo>,
    /// Lock this request is waiting for (if any).
    pub waiting_lock: Option<LockInfo>,
    /// Resources held by this request.
    pub resources: HashMap<ResourceType, usize>,
    /// Request description (e.g., "GetObject bucket/key").
    pub description: String,
}

impl RequestResourceTracker {
    /// Create a new tracker for a request.
    pub fn new(request_id: RequestId, description: impl Into<String>) -> Self {
        Self {
            request_id,
            start_time: Instant::now(),
            held_locks: Vec::new(),
            waiting_lock: None,
            resources: HashMap::new(),
            description: description.into(),
        }
    }

    /// Get the elapsed time since the request started.
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Check if this request is potentially hung.
    pub fn is_hung(&self, threshold: Duration) -> bool {
        self.elapsed() > threshold
    }
}

/// Deadlock detection result.
#[derive(Debug, Clone)]
pub struct DeadlockInfo {
    /// Time of detection.
    pub detected_at: Instant,
    /// Requests involved in the deadlock cycle.
    pub cycle: Vec<RequestId>,
    /// Lock wait graph showing the deadlock.
    pub wait_graph: Vec<WaitGraphEdge>,
    /// Resource usage at detection time.
    pub resource_usage: ResourceUsage,
}

/// Edge in the lock wait graph.
#[derive(Debug, Clone)]
pub struct WaitGraphEdge {
    /// Request that is waiting.
    pub from: RequestId,
    /// Request that is blocking (holds the lock).
    pub to: RequestId,
    /// The lock being waited for.
    pub lock_id: LockId,
}

/// Resource usage snapshot.
#[derive(Debug, Clone, Default)]
pub struct ResourceUsage {
    /// Total memory used (bytes).
    pub memory_bytes: usize,
    /// Total file handles open.
    pub open_files: usize,
    /// Total active requests.
    pub active_requests: usize,
    /// Total locks held.
    pub locks_held: usize,
}

/// Deadlock detector.
pub struct DeadlockDetector {
    /// Configuration.
    config: DeadlockDetectorConfig,
    /// Active request trackers.
    requests: Arc<RwLock<HashMap<RequestId, RequestResourceTracker>>>,
    /// Detection task handle.
    detector_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Shutdown signal.
    shutdown_tx: broadcast::Sender<()>,
    /// Total deadlocks detected.
    deadlocks_detected: Arc<AtomicU64>,
    /// Is currently running.
    running: Arc<AtomicBool>,
}

impl DeadlockDetector {
    /// Create a new deadlock detector.
    pub fn new(config: DeadlockDetectorConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            config,
            requests: Arc::new(RwLock::new(HashMap::new())),
            detector_task: Arc::new(Mutex::new(None)),
            shutdown_tx,
            deadlocks_detected: Arc::new(AtomicU64::new(0)),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check if detection is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Start the detection task.
    pub fn start(&self) {
        if !self.config.enabled {
            debug!("Deadlock detection is disabled");
            return;
        }

        if self.running.swap(true, Ordering::Relaxed) {
            debug!("Deadlock detector already running");
            return;
        }

        let requests = self.requests.clone();
        let config = self.config.clone();
        let deadlocks_detected = self.deadlocks_detected.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let running = self.running.clone();

        let handle = tokio::spawn(async move {
            debug!(
                check_interval_secs = config.check_interval.as_secs(),
                hang_threshold_secs = config.hang_threshold.as_secs(),
                "Deadlock detector started"
            );

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(config.check_interval) => {
                        Self::detect_cycle(&requests, &config, &deadlocks_detected);
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Deadlock detector shutting down");
                        break;
                    }
                }
            }

            running.store(false, Ordering::Relaxed);
        });

        *self.detector_task.lock().unwrap() = Some(handle);
    }

    /// Stop the detection task.
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(());
        if let Some(handle) = self.detector_task.lock().unwrap().take() {
            // Don't await the handle as we're in a non-async context
            handle.abort();
        }
        self.running.store(false, Ordering::Relaxed);
    }

    /// Register a new request for tracking.
    pub fn register_request(&self, request_id: impl Into<String>, description: impl Into<String>) {
        if !self.config.enabled {
            return;
        }

        let request_id = request_id.into();
        let tracker = RequestResourceTracker::new(request_id.clone(), description);

        self.requests.write().unwrap().insert(request_id.clone(), tracker);

        debug!(request_id = %request_id, "Request registered for deadlock tracking");
    }

    /// Unregister a request (it completed or was cancelled).
    pub fn unregister_request(&self, request_id: &str) {
        if !self.config.enabled {
            return;
        }

        self.requests.write().unwrap().remove(request_id);

        debug!(request_id = %request_id, "Request unregistered from deadlock tracking");
    }

    /// Record a lock acquisition.
    pub fn record_lock_acquire(&self, request_id: &str, lock: LockInfo) {
        if !self.config.enabled {
            return;
        }

        if let Some(tracker) = self.requests.write().unwrap().get_mut(request_id) {
            tracker.held_locks.push(lock);
        }
    }

    /// Record a lock release.
    pub fn record_lock_release(&self, request_id: &str, lock_id: &LockId) {
        if !self.config.enabled {
            return;
        }

        if let Some(tracker) = self.requests.write().unwrap().get_mut(request_id) {
            tracker.held_locks.retain(|l| &l.id != lock_id);
        }
    }

    /// Record that a request is waiting for a lock.
    pub fn record_lock_wait(&self, request_id: &str, lock: LockInfo) {
        if !self.config.enabled {
            return;
        }

        if let Some(tracker) = self.requests.write().unwrap().get_mut(request_id) {
            tracker.waiting_lock = Some(lock);
        }
    }

    /// Clear the waiting lock (acquired or gave up).
    pub fn clear_lock_wait(&self, request_id: &str) {
        if !self.config.enabled {
            return;
        }

        if let Some(tracker) = self.requests.write().unwrap().get_mut(request_id) {
            tracker.waiting_lock = None;
        }
    }

    /// Record resource usage.
    pub fn record_resource(&self, request_id: &str, resource_type: ResourceType, amount: usize) {
        if !self.config.enabled {
            return;
        }

        if let Some(tracker) = self.requests.write().unwrap().get_mut(request_id) {
            tracker.resources.insert(resource_type, amount);
        }
    }

    /// Get current number of tracked requests.
    pub fn tracked_count(&self) -> usize {
        self.requests.read().unwrap().len()
    }

    /// Get total deadlocks detected.
    pub fn total_detected(&self) -> u64 {
        self.deadlocks_detected.load(Ordering::Relaxed)
    }

    /// Detect deadlock cycles in the lock wait graph.
    fn detect_cycle(
        requests: &Arc<RwLock<HashMap<RequestId, RequestResourceTracker>>>,
        config: &DeadlockDetectorConfig,
        deadlocks_detected: &Arc<AtomicU64>,
    ) {
        let requests_guard = requests.read().unwrap();

        // Find hung requests
        let hung_requests: Vec<_> = requests_guard.values().filter(|r| r.is_hung(config.hang_threshold)).collect();

        if hung_requests.is_empty() {
            return;
        }

        // Build lock wait graph
        // Edge: request A -> request B means A is waiting for a lock that B holds
        let mut wait_graph: Vec<WaitGraphEdge> = Vec::new();

        for waiting in &hung_requests {
            if let Some(waiting_for) = &waiting.waiting_lock {
                // Find who holds this lock
                for holding in &hung_requests {
                    if holding.request_id == waiting.request_id {
                        continue;
                    }
                    if holding.held_locks.iter().any(|l| l.id == waiting_for.id) {
                        wait_graph.push(WaitGraphEdge {
                            from: waiting.request_id.clone(),
                            to: holding.request_id.clone(),
                            lock_id: waiting_for.id.clone(),
                        });
                    }
                }
            }
        }

        // Detect cycles using DFS
        if let Some(cycle) = Self::find_cycle(&wait_graph) {
            deadlocks_detected.fetch_add(1, Ordering::Relaxed);

            counter!("rustfs_deadlock_detected_total").increment(1);

            // Log detailed deadlock information
            error!(
                cycle = ?cycle,
                wait_graph = ?wait_graph,
                hung_requests_count = hung_requests.len(),
                "Deadlock detected: circular lock wait chain found"
            );

            // Log each request in the cycle
            for request_id in &cycle {
                if let Some(tracker) = requests_guard.get(request_id) {
                    warn!(
                        request_id = %request_id,
                        description = %tracker.description,
                        elapsed_secs = tracker.elapsed().as_secs(),
                        held_locks = ?tracker.held_locks.iter().map(|l| &l.id).collect::<Vec<_>>(),
                        waiting_lock = ?tracker.waiting_lock.as_ref().map(|l| &l.id),
                        "Request in deadlock cycle"
                    );
                }
            }
        } else {
            // No cycle, but log hung requests for diagnosis
            debug!(
                hung_requests_count = hung_requests.len(),
                wait_graph_edges = wait_graph.len(),
                "Hung requests detected but no deadlock cycle"
            );
        }
    }

    /// Find a cycle in the wait graph using DFS.
    fn find_cycle(edges: &[WaitGraphEdge]) -> Option<Vec<RequestId>> {
        // Build adjacency list
        let mut graph: HashMap<&RequestId, Vec<&RequestId>> = HashMap::new();
        for edge in edges {
            graph.entry(&edge.from).or_default().push(&edge.to);
        }

        // DFS with path tracking
        let mut visited: HashSet<&RequestId> = HashSet::new();
        let mut path: Vec<&RequestId> = Vec::new();
        let mut path_set: HashSet<&RequestId> = HashSet::new();

        for start in graph.keys() {
            if visited.contains(start) {
                continue;
            }

            if Self::dfs_find_cycle(start, &graph, &mut visited, &mut path, &mut path_set) {
                return Some(path.iter().map(|s| (*s).clone()).collect());
            }
        }

        None
    }

    /// DFS helper for cycle detection.
    fn dfs_find_cycle<'a>(
        node: &'a RequestId,
        graph: &HashMap<&'a RequestId, Vec<&'a RequestId>>,
        visited: &mut HashSet<&'a RequestId>,
        path: &mut Vec<&'a RequestId>,
        path_set: &mut HashSet<&'a RequestId>,
    ) -> bool {
        visited.insert(node);
        path.push(node);
        path_set.insert(node);

        if let Some(neighbors) = graph.get(&node) {
            for neighbor in neighbors {
                if path_set.contains(neighbor) {
                    // Found cycle - trim path to just the cycle
                    let cycle_start = path.iter().position(|n| *n == *neighbor).unwrap();
                    path.drain(0..cycle_start);
                    return true;
                }

                if !visited.contains(neighbor) && Self::dfs_find_cycle(neighbor, graph, visited, path, path_set) {
                    return true;
                }
            }
        }

        path.pop();
        path_set.remove(node);
        false
    }
}

impl Drop for DeadlockDetector {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Global deadlock detector instance.
static DEADLOCK_DETECTOR: std::sync::OnceLock<Arc<DeadlockDetector>> = std::sync::OnceLock::new();

/// Get the global deadlock detector.
pub fn get_deadlock_detector() -> Arc<DeadlockDetector> {
    DEADLOCK_DETECTOR
        .get_or_init(|| {
            let config = DeadlockDetectorConfig::from_env();
            Arc::new(DeadlockDetector::new(config))
        })
        .clone()
}

/// Initialize and start the global deadlock detector.
pub fn init_deadlock_detector() {
    let detector = get_deadlock_detector();
    detector.start();
}

/// Check if deadlock detection is enabled.
pub fn is_deadlock_detection_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_OBJECT_DEADLOCK_DETECTION_ENABLE,
        rustfs_config::DEFAULT_OBJECT_DEADLOCK_DETECTION_ENABLE,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deadlock_detector_config_default() {
        let config = DeadlockDetectorConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.check_interval, Duration::from_secs(5));
        assert_eq!(config.hang_threshold, Duration::from_secs(10));
    }

    #[test]
    fn test_request_resource_tracker() {
        let tracker = RequestResourceTracker::new("req-1".to_string(), "GetObject bucket/key");
        assert!(tracker.elapsed() < Duration::from_secs(1));
        assert!(!tracker.is_hung(Duration::from_secs(10)));
    }

    #[test]
    fn test_lock_info() {
        let lock = LockInfo {
            id: "lock-1".to_string(),
            lock_type: LockType::Read,
            resource: "bucket/key".to_string(),
            acquire_time: Instant::now(),
        };
        assert_eq!(format!("{}", lock.lock_type), "read");
    }

    #[test]
    fn test_deadlock_detector_registration() {
        let config = DeadlockDetectorConfig {
            enabled: true,
            ..Default::default()
        };
        let detector = DeadlockDetector::new(config);

        detector.register_request("req-1", "Test request");
        assert_eq!(detector.tracked_count(), 1);

        detector.unregister_request("req-1");
        assert_eq!(detector.tracked_count(), 0);
    }

    #[test]
    fn test_cycle_detection() {
        // Create a simple cycle: A -> B -> C -> A
        let edges = vec![
            WaitGraphEdge {
                from: "A".to_string(),
                to: "B".to_string(),
                lock_id: "lock-1".to_string(),
            },
            WaitGraphEdge {
                from: "B".to_string(),
                to: "C".to_string(),
                lock_id: "lock-2".to_string(),
            },
            WaitGraphEdge {
                from: "C".to_string(),
                to: "A".to_string(),
                lock_id: "lock-3".to_string(),
            },
        ];

        let cycle = DeadlockDetector::find_cycle(&edges);
        assert!(cycle.is_some());
        let cycle = cycle.unwrap();
        assert!(cycle.len() >= 2);
    }

    #[test]
    fn test_no_cycle() {
        // No cycle: A -> B -> C
        let edges = vec![
            WaitGraphEdge {
                from: "A".to_string(),
                to: "B".to_string(),
                lock_id: "lock-1".to_string(),
            },
            WaitGraphEdge {
                from: "B".to_string(),
                to: "C".to_string(),
                lock_id: "lock-2".to_string(),
            },
        ];

        let cycle = DeadlockDetector::find_cycle(&edges);
        assert!(cycle.is_none());
    }
}
