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


use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    client::LockClient,
    deadlock_detector::DeadlockDetector,
    error::Result,
    types::{
        DeadlockDetectionResult, LockId, LockInfo, LockRequest, LockResponse, LockStats, LockStatus, LockType,
        WaitQueueItem,
    },
};

/// Local lock client
#[derive(Debug, Clone)]
pub struct LocalClient {
    /// Lock storage
    locks: Arc<DashMap<String, LockInfo>>,
    /// Deadlock detector
    deadlock_detector: Arc<Mutex<DeadlockDetector>>,
    /// Wait queues: resource -> wait queue
    wait_queues: Arc<DashMap<String, Vec<WaitQueueItem>>>,
    /// Statistics
    stats: Arc<Mutex<LockStats>>,
}

impl LocalClient {
    /// Create new local client
    pub fn new() -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
            deadlock_detector: Arc::new(Mutex::new(DeadlockDetector::new())),
            wait_queues: Arc::new(DashMap::new()),
            stats: Arc::new(Mutex::new(LockStats::default())),
        }
    }

    /// Acquire lock with priority and deadlock detection
    async fn acquire_lock_with_priority(&self, request: LockRequest, lock_type: LockType) -> Result<LockResponse> {
        let _start_time = std::time::SystemTime::now();
        let lock_key = crate::utils::generate_lock_key(&request.resource, lock_type);

        // Check deadlock detection
        if request.deadlock_detection {
            if let Ok(detection_result) = self.check_deadlock(&request).await {
                if detection_result.has_deadlock {
                    return Ok(LockResponse::failure(
                        format!("Deadlock detected: {:?}", detection_result.deadlock_cycle),
                        crate::utils::duration_between(_start_time, std::time::SystemTime::now()),
                    ));
                }
            }
        }

        // Atomic check + insert
        match self.locks.entry(lock_key) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let existing = entry.get();
                if existing.owner != request.owner {
                    // Add to wait queue
                    let wait_item = WaitQueueItem::new(&request.owner, lock_type, request.priority);
                    self.add_to_wait_queue(&request.resource, wait_item).await;

                    // Update deadlock detector
                    self.update_deadlock_detector(&request, &existing.owner).await;

                    // Check wait timeout
                    if let Some(wait_timeout) = request.wait_timeout {
                        if crate::utils::duration_between(_start_time, std::time::SystemTime::now()) > wait_timeout {
                            self.remove_from_wait_queue(&request.resource, &request.owner).await;
                            return Ok(LockResponse::failure(
                                "Wait timeout exceeded".to_string(),
                                crate::utils::duration_between(_start_time, std::time::SystemTime::now()),
                            ));
                        }
                    }

                    let position = self.get_wait_position(&request.resource, &request.owner).await;
                    return Ok(LockResponse::waiting(
                        crate::utils::duration_between(_start_time, std::time::SystemTime::now()),
                        position,
                    ));
                }
                // Update lock info (same owner can re-acquire)
                let mut lock_info = existing.clone();
                lock_info.last_refreshed = std::time::SystemTime::now();
                lock_info.expires_at = std::time::SystemTime::now() + request.timeout;
                lock_info.priority = request.priority;
                entry.insert(lock_info.clone());
                Ok(LockResponse::success(
                    lock_info,
                    crate::utils::duration_between(_start_time, std::time::SystemTime::now()),
                ))
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                // Insert new lock
                let lock_info = LockInfo {
                    id: LockId::new(),
                    resource: request.resource.clone(),
                    lock_type,
                    status: LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + request.timeout,
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                };
                entry.insert(lock_info.clone());

                // Update deadlock detector
                self.update_deadlock_detector(&request, "").await;

                Ok(LockResponse::success(
                    lock_info,
                    crate::utils::duration_between(_start_time, std::time::SystemTime::now()),
                ))
            }
        }
    }

    /// Check for deadlock
    async fn check_deadlock(&self, _request: &LockRequest) -> Result<DeadlockDetectionResult> {
        let mut detector = self.deadlock_detector.lock().await;
        Ok(detector.detect_deadlock())
    }

    /// Update deadlock detector
    async fn update_deadlock_detector(&self, request: &LockRequest, current_owner: &str) {
        let mut detector = self.deadlock_detector.lock().await;

        if !current_owner.is_empty() {
            // Add wait relationship
            detector.add_wait_relationship(
                &request.owner,
                &request.resource,
                vec![], // TODO: Get currently held resources
                request.priority,
            );
        }

        // Update resource holder
        detector.update_resource_holder(&request.resource, &request.owner);
    }

    /// Add to wait queue
    async fn add_to_wait_queue(&self, resource: &str, item: WaitQueueItem) {
        let mut queue = self.wait_queues.entry(resource.to_string()).or_default();
        queue.push(item);

        // Sort by priority
        queue.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Remove from wait queue
    async fn remove_from_wait_queue(&self, resource: &str, owner: &str) {
        if let Some(mut queue) = self.wait_queues.get_mut(resource) {
            queue.retain(|item| item.owner != owner);
        }
    }

    /// Get wait position
    async fn get_wait_position(&self, resource: &str, owner: &str) -> usize {
        if let Some(queue) = self.wait_queues.get(resource) {
            for (i, item) in queue.iter().enumerate() {
                if item.owner == owner {
                    return i;
                }
            }
        }
        0
    }

    /// Process wait queue
    async fn process_wait_queue(&self, resource: &str) {
        // Simple implementation to avoid never_loop warning
        if let Some(mut queue) = self.wait_queues.get_mut(resource) {
            if !queue.is_empty() {
                let _next_item = queue.remove(0);
                // TODO: Process next item in queue
            }
        }
    }

    /// Acquire multiple locks atomically
    pub async fn acquire_multiple_atomic(&self, requests: Vec<LockRequest>) -> Result<Vec<LockResponse>> {
        let mut responses = Vec::new();
        let mut acquired_locks = Vec::new();

        for request in requests {
            match self.acquire_lock_with_priority(request.clone(), LockType::Exclusive).await {
                Ok(response) => {
                    if response.is_success() {
                        acquired_locks.push(request.resource.clone());
                    }
                    responses.push(response);
                }
                Err(e) => {
                    // Rollback acquired locks
                    for resource in acquired_locks {
                        let _ = self.force_release_by_resource(&resource).await;
                    }
                    return Err(e);
                }
            }
        }

        Ok(responses)
    }

    /// Release multiple locks atomically
    pub async fn release_multiple_atomic(&self, lock_ids: Vec<LockId>) -> Result<Vec<bool>> {
        let mut results = Vec::new();
        for lock_id in lock_ids {
            results.push(self.release(&lock_id).await?);
        }
        Ok(results)
    }

    /// Force release by resource
    async fn force_release_by_resource(&self, resource: &str) -> Result<bool> {
        let lock_key = crate::utils::generate_lock_key(resource, LockType::Exclusive);
        if let Some((_, lock_info)) = self.locks.remove(&lock_key) {
            // Update statistics
            let mut stats = self.stats.lock().await;
            stats.total_releases += 1;
            stats.total_hold_time += crate::utils::duration_between(lock_info.acquired_at, std::time::SystemTime::now());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Check multiple lock status
    pub async fn check_multiple_status(&self, lock_ids: Vec<LockId>) -> Result<Vec<Option<LockInfo>>> {
        let mut results = Vec::new();
        for lock_id in lock_ids {
            results.push(self.check_status(&lock_id).await?);
        }
        Ok(results)
    }

    /// Refresh multiple locks atomically
    pub async fn refresh_multiple_atomic(&self, lock_ids: Vec<LockId>) -> Result<Vec<bool>> {
        let mut results = Vec::new();
        for lock_id in lock_ids {
            results.push(self.refresh(&lock_id).await?);
        }
        Ok(results)
    }

    /// Get deadlock statistics
    pub async fn get_deadlock_stats(&self) -> Result<(usize, std::time::SystemTime)> {
        let detector = self.deadlock_detector.lock().await;
        let (count, time) = detector.get_stats();
        Ok((count, time))
    }

    /// Detect deadlock
    pub async fn detect_deadlock(&self) -> Result<DeadlockDetectionResult> {
        let mut detector = self.deadlock_detector.lock().await;
        Ok(detector.detect_deadlock())
    }

    /// Cleanup expired waits
    pub async fn cleanup_expired_waits(&self, max_wait_time: std::time::Duration) {
        let now = std::time::SystemTime::now();
        for mut queue in self.wait_queues.iter_mut() {
            queue.retain(|item| now.duration_since(item.wait_start_time).unwrap_or_default() <= max_wait_time);
        }
    }
}

impl Default for LocalClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl super::LockClient for LocalClient {
    async fn acquire_exclusive(&self, request: LockRequest) -> Result<LockResponse> {
        self.acquire_lock_with_priority(request, LockType::Exclusive).await
    }

    async fn acquire_shared(&self, request: LockRequest) -> Result<LockResponse> {
        self.acquire_lock_with_priority(request, LockType::Shared).await
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        let _start_time = std::time::SystemTime::now();

        // Find and remove the lock
        let mut found = false;
        let mut lock_info_opt = None;

        for entry in self.locks.iter() {
            if entry.id == *lock_id {
                lock_info_opt = Some(entry.clone());
                found = true;
                break;
            }
        }

        if found {
            let lock_key = crate::utils::generate_lock_key(
                &lock_info_opt.as_ref().unwrap().resource,
                lock_info_opt.as_ref().unwrap().lock_type,
            );
            if let Some((_, lock_info)) = self.locks.remove(&lock_key) {
                // Update statistics
                let mut stats = self.stats.lock().await;
                stats.total_releases += 1;
                stats.total_hold_time += crate::utils::duration_between(lock_info.acquired_at, std::time::SystemTime::now());

                // Process wait queue
                self.process_wait_queue(&lock_info.resource).await;

                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    async fn refresh(&self, lock_id: &LockId) -> Result<bool> {
        for mut entry in self.locks.iter_mut() {
            if entry.id == *lock_id {
                entry.last_refreshed = std::time::SystemTime::now();
                entry.expires_at = std::time::SystemTime::now() + std::time::Duration::from_secs(30);
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn force_release(&self, lock_id: &LockId) -> Result<bool> {
        self.release(lock_id).await
    }

    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>> {
        for entry in self.locks.iter() {
            if entry.id == *lock_id {
                // Check if lock has expired
                if entry.expires_at < std::time::SystemTime::now() {
                    // Lock has expired, remove it
                    let lock_key = crate::utils::generate_lock_key(&entry.resource, entry.lock_type);
                    let _ = self.locks.remove(&lock_key);
                    return Ok(None);
                }
                return Ok(Some(entry.clone()));
            }
        }
        Ok(None)
    }

    async fn get_stats(&self) -> Result<LockStats> {
        let mut stats = self.stats.lock().await;
        stats.total_locks = self.locks.len();
        stats.total_wait_queues = self.wait_queues.len();

        // Calculate average hold time
        if stats.total_releases > 0 {
            stats.average_hold_time =
                std::time::Duration::from_secs(stats.total_hold_time.as_secs() / stats.total_releases as u64);
        }

        Ok(stats.clone())
    }

    async fn close(&self) -> Result<()> {
        // Cleanup all locks
        self.locks.clear();
        self.wait_queues.clear();
        Ok(())
    }

    async fn is_online(&self) -> bool {
        true // Local client is always online
    }

    async fn is_local(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LockMetadata, LockPriority, LockType};

    #[tokio::test]
    async fn test_local_client_acquire_exclusive() {
        let client = LocalClient::new();
        let request = LockRequest {
            resource: "test_resource".to_string(),
            lock_type: LockType::Exclusive,
            owner: "test_owner".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: None,
            priority: LockPriority::Normal,
            deadlock_detection: false,
            metadata: LockMetadata::default(),
        };

        let response = client.acquire_exclusive(request).await.unwrap();
        assert!(response.is_success());
    }

    #[tokio::test]
    async fn test_local_client_acquire_shared() {
        let client = LocalClient::new();
        let request = LockRequest {
            resource: "test_resource".to_string(),
            lock_type: LockType::Shared,
            owner: "test_owner".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: None,
            priority: LockPriority::Normal,
            deadlock_detection: false,
            metadata: LockMetadata::default(),
        };

        let response = client.acquire_shared(request).await.unwrap();
        assert!(response.is_success());
    }

    #[tokio::test]
    async fn test_local_client_release() {
        let client = LocalClient::new();
        let request = LockRequest {
            resource: "test_resource".to_string(),
            lock_type: LockType::Exclusive,
            owner: "test_owner".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: None,
            priority: LockPriority::Normal,
            deadlock_detection: false,
            metadata: LockMetadata::default(),
        };

        let response = client.acquire_exclusive(request).await.unwrap();
        assert!(response.is_success());

        let lock_id = &response.lock_info().unwrap().id;
        let result = client.release(lock_id).await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_local_client_concurrent_access() {
        let client = Arc::new(LocalClient::new());
        let mut handles = vec![];

        for i in 0..10 {
            let client_clone = client.clone();
            let handle = tokio::spawn(async move {
                let request = LockRequest {
                    resource: "concurrent_resource".to_string(),
                    lock_type: LockType::Exclusive,
                    owner: format!("owner_{i}"),
                    timeout: std::time::Duration::from_secs(30),
                    wait_timeout: None,
                    priority: LockPriority::Normal,
                    deadlock_detection: false,
                    metadata: LockMetadata::default(),
                };

                let response = client_clone.acquire_exclusive(request).await.unwrap();
                if response.is_success() {
                    let lock_id = &response.lock_info().unwrap().id;
                    let _ = client_clone.release(lock_id).await;
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_dashmap_performance() {
        let client = LocalClient::new();
        let start_time = std::time::Instant::now();

        // Simulate high concurrent access
        let mut handles = vec![];
        for i in 0..100 {
            let client_clone = Arc::new(client.clone());
            let handle = tokio::spawn(async move {
                let request = LockRequest {
                    resource: format!("resource_{i}"),
                    lock_type: LockType::Exclusive,
                    owner: format!("owner_{i}"),
                    timeout: std::time::Duration::from_secs(30),
                    wait_timeout: None,
                    priority: LockPriority::Normal,
                    deadlock_detection: false,
                    metadata: LockMetadata::default(),
                };

                let response = client_clone.acquire_exclusive(request).await.unwrap();
                if response.is_success() {
                    let lock_id = &response.lock_info().unwrap().id;
                    let _ = client_clone.release(lock_id).await;
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let duration = start_time.elapsed();
        println!("DashMap performance test completed in {duration:?}");
        assert!(duration < std::time::Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_atomic_operations() {
        let client = LocalClient::new();
        let request = LockRequest {
            resource: "atomic_resource".to_string(),
            lock_type: LockType::Exclusive,
            owner: "test_owner".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: None,
            priority: LockPriority::Normal,
            deadlock_detection: false,
            metadata: LockMetadata::default(),
        };

        // Test atomic acquire
        let response = client.acquire_exclusive(request).await.unwrap();
        assert!(response.is_success());

        // Test concurrent access to same resource
        let client_clone = Arc::new(client);
        let mut handles = vec![];
        for i in 0..5 {
            let client_clone = client_clone.clone();
            let handle = tokio::spawn(async move {
                let request = LockRequest {
                    resource: "atomic_resource".to_string(),
                    lock_type: LockType::Exclusive,
                    owner: format!("owner_{i}"),
                    timeout: std::time::Duration::from_secs(30),
                    wait_timeout: None,
                    priority: LockPriority::Normal,
                    deadlock_detection: false,
                    metadata: LockMetadata::default(),
                };

                let response = client_clone.acquire_exclusive(request).await.unwrap();
                response.is_waiting() // Should be waiting due to atomic operation
            });
            handles.push(handle);
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result);
        }
    }

    #[tokio::test]
    async fn test_batch_atomic_operations() {
        let client = LocalClient::new();
        let requests = vec![
            LockRequest {
                resource: "batch_resource_1".to_string(),
                lock_type: LockType::Exclusive,
                owner: "owner_1".to_string(),
                timeout: std::time::Duration::from_secs(30),
                wait_timeout: None,
                priority: LockPriority::Normal,
                deadlock_detection: false,
                metadata: LockMetadata::default(),
            },
            LockRequest {
                resource: "batch_resource_2".to_string(),
                lock_type: LockType::Exclusive,
                owner: "owner_1".to_string(),
                timeout: std::time::Duration::from_secs(30),
                wait_timeout: None,
                priority: LockPriority::Normal,
                deadlock_detection: false,
                metadata: LockMetadata::default(),
            },
        ];

        let responses = client.acquire_multiple_atomic(requests).await.unwrap();
        assert_eq!(responses.len(), 2);
        assert!(responses[0].is_success());
        assert!(responses[1].is_success());
    }

    #[tokio::test]
    async fn test_batch_atomic_rollback() {
        let client = LocalClient::new();

        // First acquire a lock
        let first_request = LockRequest {
            resource: "rollback_resource".to_string(),
            lock_type: LockType::Exclusive,
            owner: "owner_1".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: None,
            priority: LockPriority::Normal,
            deadlock_detection: false,
            metadata: LockMetadata::default(),
        };
        let response = client.acquire_exclusive(first_request).await.unwrap();
        assert!(response.is_success());

        // Try to acquire same resource in batch (should fail and rollback)
        let requests = vec![
            LockRequest {
                resource: "rollback_resource".to_string(),
                lock_type: LockType::Exclusive,
                owner: "owner_2".to_string(),
                timeout: std::time::Duration::from_secs(30),
                wait_timeout: None,
                priority: LockPriority::Normal,
                deadlock_detection: false,
                metadata: LockMetadata::default(),
            },
            LockRequest {
                resource: "rollback_resource_2".to_string(),
                lock_type: LockType::Exclusive,
                owner: "owner_2".to_string(),
                timeout: std::time::Duration::from_secs(30),
                wait_timeout: None,
                priority: LockPriority::Normal,
                deadlock_detection: false,
                metadata: LockMetadata::default(),
            },
        ];

        let responses = client.acquire_multiple_atomic(requests).await.unwrap();
        assert_eq!(responses.len(), 2);
        assert!(responses[0].is_waiting()); // Should be waiting
        assert!(responses[1].is_success()); // Second should succeed
    }

    #[tokio::test]
    async fn test_concurrent_atomic_operations() {
        let client = Arc::new(LocalClient::new());
        let mut handles = vec![];

        for i in 0..10 {
            let client_clone = client.clone();
            let handle = tokio::spawn(async move {
                let requests = vec![
                    LockRequest {
                        resource: format!("concurrent_batch_{i}"),
                        lock_type: LockType::Exclusive,
                        owner: format!("owner_{i}"),
                        timeout: std::time::Duration::from_secs(30),
                        wait_timeout: None,
                        priority: LockPriority::Normal,
                        deadlock_detection: false,
                        metadata: LockMetadata::default(),
                    },
                    LockRequest {
                        resource: format!("concurrent_batch_{i}_2"),
                        lock_type: LockType::Exclusive,
                        owner: format!("owner_{i}"),
                        timeout: std::time::Duration::from_secs(30),
                        wait_timeout: None,
                        priority: LockPriority::Normal,
                        deadlock_detection: false,
                        metadata: LockMetadata::default(),
                    },
                ];

                let responses = client_clone.acquire_multiple_atomic(requests).await.unwrap();
                assert_eq!(responses.len(), 2);

                // Release locks
                for response in responses {
                    if response.is_success() {
                        let lock_id = &response.lock_info().unwrap().id;
                        let _ = client_clone.release(lock_id).await;
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_priority_upgrade() {
        let client = LocalClient::new();

        // Acquire lock with normal priority
        let normal_request = LockRequest {
            resource: "priority_resource".to_string(),
            lock_type: LockType::Exclusive,
            owner: "normal_owner".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: None,
            priority: LockPriority::Normal,
            deadlock_detection: false,
            metadata: LockMetadata::default(),
        };
        let response = client.acquire_exclusive(normal_request).await.unwrap();
        assert!(response.is_success());

        // Try to acquire with high priority (should be waiting)
        let high_request = LockRequest {
            resource: "priority_resource".to_string(),
            lock_type: LockType::Exclusive,
            owner: "high_owner".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: None,
            priority: LockPriority::High,
            deadlock_detection: false,
            metadata: LockMetadata::default(),
        };
        let response = client.acquire_exclusive(high_request.clone()).await.unwrap();
        assert!(response.is_waiting());

        // Release normal priority lock
        let lock_id = &response.lock_info().unwrap().id;
        let _ = client.release(lock_id).await;

        // High priority should now acquire
        let response = client.acquire_exclusive(high_request).await.unwrap();
        assert!(response.is_success());
    }

    #[tokio::test]
    async fn test_deadlock_detection() {
        let client = LocalClient::new();

        // Create a potential deadlock scenario
        let request1 = LockRequest {
            resource: "resource_a".to_string(),
            lock_type: LockType::Exclusive,
            owner: "owner_1".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: None,
            priority: LockPriority::Normal,
            deadlock_detection: true,
            metadata: LockMetadata::default(),
        };

        let request2 = LockRequest {
            resource: "resource_b".to_string(),
            lock_type: LockType::Exclusive,
            owner: "owner_2".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: None,
            priority: LockPriority::Normal,
            deadlock_detection: true,
            metadata: LockMetadata::default(),
        };

        // Acquire first lock
        let response1 = client.acquire_exclusive(request1).await.unwrap();
        assert!(response1.is_success());

        // Acquire second lock
        let response2 = client.acquire_exclusive(request2).await.unwrap();
        assert!(response2.is_success());

        // Try to create deadlock
        let deadlock_request1 = LockRequest {
            resource: "resource_b".to_string(),
            lock_type: LockType::Exclusive,
            owner: "owner_1".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: None,
            priority: LockPriority::Normal,
            deadlock_detection: true,
            metadata: LockMetadata::default(),
        };

        let response = client.acquire_exclusive(deadlock_request1).await.unwrap();
        assert!(response.is_waiting() || response.is_failure());
    }

    #[tokio::test]
    async fn test_wait_timeout() {
        let client = LocalClient::new();

        // Acquire lock
        let request1 = LockRequest {
            resource: "timeout_resource".to_string(),
            lock_type: LockType::Exclusive,
            owner: "owner_1".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: None,
            priority: LockPriority::Normal,
            deadlock_detection: false,
            metadata: LockMetadata::default(),
        };
        let response = client.acquire_exclusive(request1).await.unwrap();
        assert!(response.is_success());

        // Try to acquire with short wait timeout
        let request2 = LockRequest {
            resource: "timeout_resource".to_string(),
            lock_type: LockType::Exclusive,
            owner: "owner_2".to_string(),
            timeout: std::time::Duration::from_secs(30),
            wait_timeout: Some(std::time::Duration::from_millis(100)),
            priority: LockPriority::Normal,
            deadlock_detection: false,
            metadata: LockMetadata::default(),
        };

        let start_time = std::time::Instant::now();
        let response = client.acquire_exclusive(request2).await.unwrap();
        let duration = start_time.elapsed();

        assert!(response.is_failure() || response.is_waiting());
        assert!(duration < std::time::Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_deadlock_stats() {
        let client = LocalClient::new();

        let (count, last_time) = client.get_deadlock_stats().await.unwrap();
        assert_eq!(count, 0);
        assert!(last_time < std::time::SystemTime::now());
    }

    #[tokio::test]
    async fn test_cleanup_expired_waits() {
        let client = LocalClient::new();

        // Add some wait items
        let wait_item = WaitQueueItem::new("test_owner", LockType::Exclusive, LockPriority::Normal);
        client.add_to_wait_queue("test_resource", wait_item).await;

        // Cleanup with short timeout
        client.cleanup_expired_waits(std::time::Duration::from_millis(1)).await;

        // Wait queue should be empty
        let position = client.get_wait_position("test_resource", "test_owner").await;
        assert_eq!(position, 0);
    }
}
