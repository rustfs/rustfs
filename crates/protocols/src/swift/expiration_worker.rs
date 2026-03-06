// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Background Worker for Automatic Object Expiration Cleanup
//!
//! This module implements a background worker that periodically scans for and
//! deletes expired objects based on their X-Delete-At metadata.
//!
//! # Architecture
//!
//! The worker uses a priority queue to efficiently track objects nearing expiration:
//! - Objects with expiration timestamps are added to a min-heap
//! - Worker periodically checks the heap for expired objects
//! - Expired objects are deleted and removed from the heap
//! - Incremental scanning prevents full table scans on each iteration
//!
//! # Configuration
//!
//! ```rust
//! use rustfs_protocols::swift::expiration_worker::*;
//!
//! let config = ExpirationWorkerConfig {
//!     scan_interval_secs: 300,      // Scan every 5 minutes
//!     batch_size: 100,               // Process 100 objects per batch
//!     max_workers: 4,                // Support distributed scanning
//!     worker_id: 0,                  // This worker's ID (0-3)
//! };
//!
//! let worker = ExpirationWorker::new(config);
//! worker.start().await;
//! ```
//!
//! # Distributed Scanning
//!
//! Multiple workers can scan in parallel using consistent hashing:
//! - Each worker is assigned a worker_id (0 to max_workers-1)
//! - Objects are assigned to workers based on hash(account + container + object) % max_workers
//! - This prevents duplicate deletions and distributes load

use super::SwiftResult;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Configuration for expiration worker
#[derive(Debug, Clone)]
pub struct ExpirationWorkerConfig {
    /// Scan interval in seconds (default: 300 = 5 minutes)
    pub scan_interval_secs: u64,

    /// Batch size for processing objects (default: 100)
    pub batch_size: usize,

    /// Maximum number of distributed workers (default: 1)
    pub max_workers: u32,

    /// This worker's ID (0 to max_workers-1)
    pub worker_id: u32,
}

impl Default for ExpirationWorkerConfig {
    fn default() -> Self {
        Self {
            scan_interval_secs: 300, // 5 minutes
            batch_size: 100,
            max_workers: 1,
            worker_id: 0,
        }
    }
}

/// Object expiration entry in priority queue
#[derive(Debug, Clone, Eq, PartialEq)]
struct ExpirationEntry {
    /// Unix timestamp when object expires
    expires_at: u64,

    /// Object path: "account/container/object"
    path: String,
}

impl Ord for ExpirationEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap: earliest expiration first
        self.expires_at
            .cmp(&other.expires_at)
            .then_with(|| self.path.cmp(&other.path))
    }
}

impl PartialOrd for ExpirationEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Metrics for expiration worker
#[derive(Debug, Clone, Default)]
pub struct ExpirationMetrics {
    /// Total objects scanned
    pub objects_scanned: u64,

    /// Total objects deleted
    pub objects_deleted: u64,

    /// Total scan iterations
    pub scan_iterations: u64,

    /// Last scan duration in milliseconds
    pub last_scan_duration_ms: u64,

    /// Objects currently in priority queue
    pub queue_size: usize,

    /// Errors encountered
    pub error_count: u64,
}

/// Background worker for object expiration cleanup
pub struct ExpirationWorker {
    config: ExpirationWorkerConfig,
    priority_queue: Arc<RwLock<BinaryHeap<Reverse<ExpirationEntry>>>>,
    metrics: Arc<RwLock<ExpirationMetrics>>,
    running: Arc<RwLock<bool>>,
}

impl ExpirationWorker {
    /// Create new expiration worker
    pub fn new(config: ExpirationWorkerConfig) -> Self {
        Self {
            config,
            priority_queue: Arc::new(RwLock::new(BinaryHeap::new())),
            metrics: Arc::new(RwLock::new(ExpirationMetrics::default())),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the background worker
    ///
    /// This spawns a tokio task that runs the cleanup loop
    pub async fn start(&self) {
        let mut running = self.running.write().await;
        if *running {
            warn!("Expiration worker already running");
            return;
        }
        *running = true;
        drop(running);

        info!(
            "Starting expiration worker (scan_interval={}s, worker_id={}/{})",
            self.config.scan_interval_secs, self.config.worker_id, self.config.max_workers
        );

        let config = self.config.clone();
        let priority_queue = Arc::clone(&self.priority_queue);
        let metrics = Arc::clone(&self.metrics);
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(config.scan_interval_secs));

            loop {
                ticker.tick().await;

                // Check if still running
                if !*running.read().await {
                    info!("Expiration worker stopped");
                    break;
                }

                // Run cleanup iteration
                if let Err(e) = Self::cleanup_iteration(&config, &priority_queue, &metrics).await {
                    error!("Expiration cleanup iteration failed: {}", e);
                    metrics.write().await.error_count += 1;
                }
            }
        });
    }

    /// Stop the background worker
    pub async fn stop(&self) {
        let mut running = self.running.write().await;
        *running = false;
        info!("Stopping expiration worker");
    }

    /// Get current metrics
    pub async fn get_metrics(&self) -> ExpirationMetrics {
        self.metrics.read().await.clone()
    }

    /// Add object to expiration tracking
    ///
    /// Called when an object with X-Delete-At is created or updated
    pub async fn track_object(&self, account: &str, container: &str, object: &str, expires_at: u64) {
        let path = format!("{}/{}/{}", account, container, object);

        // Check if this worker should handle this object (distributed hashing)
        if !self.should_handle_object(&path) {
            debug!("Skipping object {} (handled by different worker)", path);
            return;
        }

        let entry = ExpirationEntry { expires_at, path: path.clone() };

        let mut queue = self.priority_queue.write().await;
        queue.push(Reverse(entry));

        debug!("Tracking object {} for expiration at {}", path, expires_at);
    }

    /// Remove object from expiration tracking
    ///
    /// Called when an object is deleted or expiration is removed
    pub async fn untrack_object(&self, account: &str, container: &str, object: &str) {
        let path = format!("{}/{}/{}", account, container, object);

        // Note: We can't efficiently remove from BinaryHeap, so we rely on
        // the cleanup iteration to skip objects that no longer exist.
        // This is acceptable because the queue size is bounded and cleanup is periodic.

        debug!("Untracking object {} from expiration", path);
    }

    /// Check if this worker should handle the given object (consistent hashing)
    fn should_handle_object(&self, path: &str) -> bool {
        if self.config.max_workers == 1 {
            return true; // Single worker handles everything
        }

        // Hash the path and mod by max_workers
        let hash = Self::hash_path(path);
        let assigned_worker = (hash % self.config.max_workers as u64) as u32;

        assigned_worker == self.config.worker_id
    }

    /// Simple hash function for consistent hashing
    fn hash_path(path: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        hasher.finish()
    }

    /// Run one cleanup iteration
    async fn cleanup_iteration(
        config: &ExpirationWorkerConfig,
        priority_queue: &Arc<RwLock<BinaryHeap<Reverse<ExpirationEntry>>>>,
        metrics: &Arc<RwLock<ExpirationMetrics>>,
    ) -> SwiftResult<()> {
        let start_time = SystemTime::now();
        let now = start_time.duration_since(UNIX_EPOCH).unwrap().as_secs();

        info!("Starting expiration cleanup iteration (worker_id={})", config.worker_id);

        let mut deleted_count = 0;
        let mut scanned_count = 0;
        let mut batch = Vec::new();

        // Process expired objects from priority queue
        loop {
            // Check if we have a batch to process
            if batch.len() >= config.batch_size {
                break;
            }

            // Peek at next expired object
            let mut queue = priority_queue.write().await;

            if let Some(Reverse(entry)) = queue.peek() {
                if entry.expires_at > now {
                    // No more expired objects
                    break;
                }

                // Remove from queue and add to batch
                let entry = queue.pop().unwrap().0;
                drop(queue); // Release lock

                batch.push(entry);
            } else {
                // Queue is empty
                break;
            }
        }

        // Process batch
        for entry in batch {
            scanned_count += 1;

            // Parse path: "account/container/object"
            let parts: Vec<&str> = entry.path.splitn(3, '/').collect();
            if parts.len() != 3 {
                warn!("Invalid expiration entry path: {}", entry.path);
                continue;
            }

            let (account, container, object) = (parts[0], parts[1], parts[2]);

            // Attempt to delete object
            match Self::delete_expired_object(account, container, object, entry.expires_at).await {
                Ok(true) => {
                    deleted_count += 1;
                    info!("Deleted expired object: {}", entry.path);
                }
                Ok(false) => {
                    debug!("Object {} no longer exists or expiration removed", entry.path);
                }
                Err(e) => {
                    error!("Failed to delete expired object {}: {}", entry.path, e);
                    metrics.write().await.error_count += 1;
                }
            }
        }

        // Update metrics
        let duration = SystemTime::now().duration_since(start_time).unwrap();
        let mut m = metrics.write().await;
        m.objects_scanned += scanned_count;
        m.objects_deleted += deleted_count;
        m.scan_iterations += 1;
        m.last_scan_duration_ms = duration.as_millis() as u64;
        m.queue_size = priority_queue.read().await.len();

        info!(
            "Expiration cleanup iteration complete: scanned={}, deleted={}, duration={}ms, queue_size={}",
            scanned_count, deleted_count, m.last_scan_duration_ms, m.queue_size
        );

        Ok(())
    }

    /// Delete an expired object
    ///
    /// Returns:
    /// - Ok(true) if object was deleted
    /// - Ok(false) if object doesn't exist or expiration was removed
    /// - Err if deletion failed
    async fn delete_expired_object(
        account: &str,
        container: &str,
        object: &str,
        expected_expires_at: u64,
    ) -> SwiftResult<bool> {
        // Note: This is a placeholder implementation
        // In a real system, this would:
        // 1. HEAD the object to verify it still exists and has X-Delete-At metadata
        // 2. Check that X-Delete-At matches expected_expires_at (not modified)
        // 3. DELETE the object
        // 4. Handle errors (NotFound = Ok(false), others = Err)

        // For now, we'll log the deletion
        debug!(
            "Would delete expired object: {}/{}/{} (expires_at={})",
            account, container, object, expected_expires_at
        );

        // TODO: Integrate with actual object storage
        // let info = object::head_object(account, container, object, &None).await?;
        // if let Some(delete_at_str) = info.metadata.get("x-delete-at") {
        //     let delete_at = delete_at_str.parse::<u64>().unwrap_or(0);
        //     if delete_at == expected_expires_at && expiration::is_expired(delete_at) {
        //         object::delete_object(account, container, object, &None).await?;
        //         return Ok(true);
        //     }
        // }

        Ok(false) // Placeholder: object doesn't exist or expiration removed
    }

    /// Scan all objects and add those with expiration to tracking
    ///
    /// This is used for initial population or recovery after restart.
    /// In production, objects should be tracked incrementally via track_object().
    pub async fn scan_all_objects(&self) -> SwiftResult<()> {
        info!("Starting full scan of objects with expiration (worker_id={})", self.config.worker_id);

        // TODO: This would integrate with the storage layer to list all objects
        // For each object with X-Delete-At metadata, call track_object()

        // Placeholder implementation
        warn!("Full object scan not yet implemented - requires storage layer integration");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expiration_entry_ordering() {
        let entry1 = ExpirationEntry {
            expires_at: 1000,
            path: "account/container/obj1".to_string(),
        };

        let entry2 = ExpirationEntry {
            expires_at: 2000,
            path: "account/container/obj2".to_string(),
        };

        // Earlier expiration should be "less than" for min-heap
        assert!(entry1 < entry2);
    }

    #[test]
    fn test_priority_queue_ordering() {
        let mut heap = BinaryHeap::new();

        heap.push(Reverse(ExpirationEntry {
            expires_at: 2000,
            path: "obj2".to_string(),
        }));

        heap.push(Reverse(ExpirationEntry {
            expires_at: 1000,
            path: "obj1".to_string(),
        }));

        heap.push(Reverse(ExpirationEntry {
            expires_at: 3000,
            path: "obj3".to_string(),
        }));

        // Should pop in order: 1000, 2000, 3000
        assert_eq!(heap.pop().unwrap().0.expires_at, 1000);
        assert_eq!(heap.pop().unwrap().0.expires_at, 2000);
        assert_eq!(heap.pop().unwrap().0.expires_at, 3000);
    }

    #[test]
    fn test_should_handle_object_single_worker() {
        let config = ExpirationWorkerConfig {
            max_workers: 1,
            worker_id: 0,
            ..Default::default()
        };

        let worker = ExpirationWorker::new(config);

        // Single worker handles everything
        assert!(worker.should_handle_object("account/container/obj1"));
        assert!(worker.should_handle_object("account/container/obj2"));
    }

    #[test]
    fn test_should_handle_object_distributed() {
        let config1 = ExpirationWorkerConfig {
            max_workers: 4,
            worker_id: 0,
            ..Default::default()
        };

        let config2 = ExpirationWorkerConfig {
            max_workers: 4,
            worker_id: 1,
            ..Default::default()
        };

        let worker1 = ExpirationWorker::new(config1);
        let worker2 = ExpirationWorker::new(config2);

        // Each worker handles a subset based on consistent hashing
        let path = "account/container/obj1";

        let handled_by_1 = worker1.should_handle_object(path);
        let handled_by_2 = worker2.should_handle_object(path);

        // Exactly one worker should handle this path
        assert!(handled_by_1 ^ handled_by_2); // XOR: one true, one false
    }

    #[test]
    fn test_hash_path_deterministic() {
        let path = "account/container/object";

        let hash1 = ExpirationWorker::hash_path(path);
        let hash2 = ExpirationWorker::hash_path(path);

        // Same path should produce same hash
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_path_distribution() {
        let paths = vec![
            "account/container/obj1",
            "account/container/obj2",
            "account/container/obj3",
            "account/container/obj4",
        ];

        let hashes: Vec<u64> = paths.iter().map(|p| ExpirationWorker::hash_path(p)).collect();

        // Different paths should produce different hashes
        for i in 0..hashes.len() {
            for j in (i + 1)..hashes.len() {
                assert_ne!(hashes[i], hashes[j]);
            }
        }
    }

    #[tokio::test]
    async fn test_worker_lifecycle() {
        let config = ExpirationWorkerConfig {
            scan_interval_secs: 1, // Fast for testing
            ..Default::default()
        };

        let worker = ExpirationWorker::new(config);

        // Start worker
        worker.start().await;

        // Should be running
        assert!(*worker.running.read().await);

        // Stop worker
        worker.stop().await;

        // Should be stopped
        assert!(!*worker.running.read().await);
    }

    #[tokio::test]
    async fn test_track_and_metrics() {
        let worker = ExpirationWorker::new(ExpirationWorkerConfig::default());

        // Track some objects
        worker.track_object("account1", "container1", "obj1", 2000).await;
        worker.track_object("account1", "container1", "obj2", 3000).await;

        // Check queue size directly
        assert_eq!(worker.priority_queue.read().await.len(), 2);

        // Update metrics to reflect current queue size
        {
            let mut m = worker.metrics.write().await;
            m.queue_size = worker.priority_queue.read().await.len();
        }

        // Check metrics
        let metrics = worker.get_metrics().await;
        assert_eq!(metrics.queue_size, 2);
    }
}
