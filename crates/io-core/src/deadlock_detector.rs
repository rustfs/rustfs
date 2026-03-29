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

//! Deadlock detection for concurrent operations.
//!
//! This module provides deadlock detection mechanisms using wait-for graphs
//! to identify potential circular dependencies between locks.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Lock type identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockType {
    /// Mutex lock.
    Mutex,
    /// RwLock (read).
    RwLockRead,
    /// RwLock (write).
    RwLockWrite,
    /// Semaphore.
    Semaphore,
}

impl LockType {
    /// Get as string.
    pub fn as_str(&self) -> &'static str {
        match self {
            LockType::Mutex => "mutex",
            LockType::RwLockRead => "rwlock_read",
            LockType::RwLockWrite => "rwlock_write",
            LockType::Semaphore => "semaphore",
        }
    }
}

/// Lock information.
#[derive(Debug, Clone)]
pub struct LockInfo {
    /// Lock ID.
    pub id: u64,
    /// Lock type.
    pub lock_type: LockType,
    /// Owner thread ID (if held).
    pub owner: Option<u64>,
    /// Waiters (thread IDs).
    pub waiters: Vec<u64>,
    /// Acquisition time.
    pub acquired_at: Option<Instant>,
}

impl LockInfo {
    /// Create new lock info.
    pub fn new(id: u64, lock_type: LockType) -> Self {
        Self {
            id,
            lock_type,
            owner: None,
            waiters: Vec::new(),
            acquired_at: None,
        }
    }

    /// Check if the lock is held.
    pub fn is_held(&self) -> bool {
        self.owner.is_some()
    }

    /// Get hold duration.
    pub fn hold_duration(&self) -> Option<Duration> {
        self.acquired_at.map(|t| t.elapsed())
    }
}

/// Wait graph edge (thread A waits for thread B).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WaitGraphEdge {
    /// Waiting thread ID.
    pub waiter: u64,
    /// Resource/thread being waited for.
    pub waited_for: u64,
    /// Lock ID involved.
    pub lock_id: u64,
}

/// Deadlock detector configuration.
#[derive(Debug, Clone)]
pub struct DeadlockDetectorConfig {
    /// Detection interval.
    pub detection_interval: Duration,
    /// Maximum lock hold time before warning.
    pub max_hold_time: Duration,
    /// Whether detection is enabled.
    pub enabled: bool,
}

impl Default for DeadlockDetectorConfig {
    fn default() -> Self {
        Self {
            detection_interval: Duration::from_secs(1),
            max_hold_time: Duration::from_secs(30),
            enabled: true,
        }
    }
}

/// Deadlock detector.
pub struct DeadlockDetector {
    /// Configuration.
    config: DeadlockDetectorConfig,
    /// Registered locks.
    locks: Mutex<HashMap<u64, LockInfo>>,
    /// Wait graph edges.
    wait_graph: Mutex<Vec<WaitGraphEdge>>,
    /// Tracked requests (request_id -> thread_id).
    requests: Mutex<HashMap<String, u64>>,
    /// Next lock ID.
    next_lock_id: Mutex<u64>,
}

impl DeadlockDetector {
    /// Create a new deadlock detector.
    pub fn new(config: DeadlockDetectorConfig) -> Self {
        Self {
            config,
            locks: Mutex::new(HashMap::new()),
            wait_graph: Mutex::new(Vec::new()),
            requests: Mutex::new(HashMap::new()),
            next_lock_id: Mutex::new(0),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(DeadlockDetectorConfig::default())
    }

    /// Get the configuration.
    pub fn config(&self) -> &DeadlockDetectorConfig {
        &self.config
    }

    /// Register a new lock.
    pub fn register_lock(&self, lock_type: LockType) -> u64 {
        let id = {
            let mut next = self.next_lock_id.lock().unwrap();
            *next += 1;
            *next
        };

        let info = LockInfo::new(id, lock_type);
        if let Ok(mut locks) = self.locks.lock() {
            locks.insert(id, info);
        }

        id
    }

    /// Unregister a lock.
    pub fn unregister_lock(&self, lock_id: u64) {
        if let Ok(mut locks) = self.locks.lock() {
            locks.remove(&lock_id);
        }
    }

    /// Record lock acquisition.
    pub fn record_acquire(&self, lock_id: u64, thread_id: u64) {
        if !self.config.enabled {
            return;
        }

        if let Ok(mut locks) = self.locks.lock()
            && let Some(info) = locks.get_mut(&lock_id)
        {
            info.owner = Some(thread_id);
            info.acquired_at = Some(Instant::now());
            info.waiters.retain(|&w| w != thread_id);
        }

        // Remove wait edge
        if let Ok(mut graph) = self.wait_graph.lock() {
            graph.retain(|e| !(e.waiter == thread_id && e.lock_id == lock_id));
        }
    }

    /// Record lock release.
    pub fn record_release(&self, lock_id: u64) {
        if !self.config.enabled {
            return;
        }

        if let Ok(mut locks) = self.locks.lock()
            && let Some(info) = locks.get_mut(&lock_id)
        {
            info.owner = None;
            info.acquired_at = None;
        }
    }

    /// Record a wait for lock.
    pub fn record_wait(&self, lock_id: u64, thread_id: u64) {
        if !self.config.enabled {
            return;
        }

        // Add to waiters list
        if let Ok(mut locks) = self.locks.lock()
            && let Some(info) = locks.get_mut(&lock_id)
        {
            if !info.waiters.contains(&thread_id) {
                info.waiters.push(thread_id);
            }

            // Add edge to wait graph
            if let Some(owner) = info.owner
                && owner != thread_id
                && let Ok(mut graph) = self.wait_graph.lock()
            {
                graph.push(WaitGraphEdge {
                    waiter: thread_id,
                    waited_for: owner,
                    lock_id,
                });
            }
        }
    }

    /// Detect deadlocks using cycle detection in wait graph.
    pub fn detect_deadlock(&self) -> Option<Vec<u64>> {
        if !self.config.enabled {
            return None;
        }

        let graph = self.wait_graph.lock().unwrap();

        // Build adjacency list
        let mut adj: HashMap<u64, Vec<u64>> = HashMap::new();
        for edge in graph.iter() {
            adj.entry(edge.waiter).or_default().push(edge.waited_for);
        }

        // DFS for cycle detection
        let mut visited: HashSet<u64> = HashSet::new();
        let mut rec_stack: HashSet<u64> = HashSet::new();
        let mut path: Vec<u64> = Vec::new();

        for &node in adj.keys() {
            if self.dfs_cycle(node, &adj, &mut visited, &mut rec_stack, &mut path) {
                return Some(path);
            }
        }

        None
    }

    /// DFS helper for cycle detection.
    fn dfs_cycle(
        &self,
        node: u64,
        adj: &HashMap<u64, Vec<u64>>,
        visited: &mut HashSet<u64>,
        rec_stack: &mut HashSet<u64>,
        path: &mut Vec<u64>,
    ) -> bool {
        if rec_stack.contains(&node) {
            // Found cycle, extract cycle from path
            if let Some(start) = path.iter().position(|&n| n == node) {
                *path = path[start..].to_vec();
            }
            path.push(node);
            return true;
        }

        if visited.contains(&node) {
            return false;
        }

        visited.insert(node);
        rec_stack.insert(node);
        path.push(node);

        if let Some(neighbors) = adj.get(&node) {
            for &neighbor in neighbors {
                if self.dfs_cycle(neighbor, adj, visited, rec_stack, path) {
                    return true;
                }
            }
        }

        rec_stack.remove(&node);
        path.pop();
        false
    }

    /// Check for long-held locks.
    pub fn check_long_held(&self) -> Vec<(u64, Duration)> {
        if !self.config.enabled {
            return Vec::new();
        }

        let locks = self.locks.lock().unwrap();
        let mut result = Vec::new();

        for (&id, info) in locks.iter() {
            if let Some(duration) = info.hold_duration()
                && duration > self.config.max_hold_time
            {
                result.push((id, duration));
            }
        }

        result
    }

    /// Register a request for tracking.
    pub fn register_request(&self, request_id: &str, thread_id: u64) {
        if let Ok(mut requests) = self.requests.lock() {
            requests.insert(request_id.to_string(), thread_id);
        }
    }

    /// Unregister a request.
    pub fn unregister_request(&self, request_id: &str) {
        if let Ok(mut requests) = self.requests.lock() {
            requests.remove(request_id);
        }
    }

    /// Get number of tracked requests.
    pub fn tracked_count(&self) -> usize {
        if let Ok(requests) = self.requests.lock() {
            requests.len()
        } else {
            0
        }
    }

    /// Get lock info.
    pub fn get_lock_info(&self, lock_id: u64) -> Option<LockInfo> {
        let locks = self.locks.lock().unwrap();
        locks.get(&lock_id).cloned()
    }

    /// Get total number of registered locks.
    pub fn lock_count(&self) -> usize {
        let locks = self.locks.lock().unwrap();
        locks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_info() {
        let info = LockInfo::new(1, LockType::Mutex);
        assert!(!info.is_held());
        assert!(info.hold_duration().is_none());
    }

    #[test]
    fn test_register_lock() {
        let detector = DeadlockDetector::with_defaults();

        let id1 = detector.register_lock(LockType::Mutex);
        let id2 = detector.register_lock(LockType::RwLockWrite);

        assert_ne!(id1, id2);
        assert_eq!(detector.lock_count(), 2);

        detector.unregister_lock(id1);
        assert_eq!(detector.lock_count(), 1);
    }

    #[test]
    fn test_acquire_release() {
        let detector = DeadlockDetector::with_defaults();
        let lock_id = detector.register_lock(LockType::Mutex);

        detector.record_acquire(lock_id, 1);
        let info = detector.get_lock_info(lock_id).unwrap();
        assert!(info.is_held());
        assert_eq!(info.owner, Some(1));

        detector.record_release(lock_id);
        let info = detector.get_lock_info(lock_id).unwrap();
        assert!(!info.is_held());
    }

    #[test]
    fn test_request_tracking() {
        let detector = DeadlockDetector::with_defaults();

        detector.register_request("req-1", 1);
        detector.register_request("req-2", 2);
        assert_eq!(detector.tracked_count(), 2);

        detector.unregister_request("req-1");
        assert_eq!(detector.tracked_count(), 1);
    }

    #[test]
    fn test_no_deadlock() {
        let detector = DeadlockDetector::with_defaults();

        let lock1 = detector.register_lock(LockType::Mutex);
        let lock2 = detector.register_lock(LockType::Mutex);

        // Thread 1 holds lock1, waits for lock2
        detector.record_acquire(lock1, 1);
        detector.record_wait(lock2, 1);

        // Thread 2 holds lock2
        detector.record_acquire(lock2, 2);

        // No deadlock
        assert!(detector.detect_deadlock().is_none());
    }

    #[test]
    fn test_disabled_detector() {
        let config = DeadlockDetectorConfig {
            enabled: false,
            ..Default::default()
        };
        let detector = DeadlockDetector::new(config);

        let lock_id = detector.register_lock(LockType::Mutex);
        detector.record_acquire(lock_id, 1);

        // Should not track when disabled
        assert!(detector.detect_deadlock().is_none());
    }
}
