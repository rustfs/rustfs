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
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::ObjectKey;

/// Lock type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockType {
    /// Exclusive lock (write lock)
    Exclusive,
    /// Shared lock (read lock)
    Shared,
}

/// Lock status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockStatus {
    /// Acquired
    Acquired,
    /// Waiting
    Waiting,
    /// Released
    Released,
    /// Expired
    Expired,
    /// Force released
    ForceReleased,
}

/// Lock priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub enum LockPriority {
    Low = 1,
    #[default]
    Normal = 2,
    High = 3,
    Critical = 4,
}

/// Lock information structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockInfo {
    /// Unique identifier for the lock
    pub id: LockId,
    /// Resource path
    pub resource: ObjectKey,
    /// Lock type
    pub lock_type: LockType,
    /// Lock status
    pub status: LockStatus,
    /// Lock owner
    pub owner: String,
    /// Acquisition time
    pub acquired_at: SystemTime,
    /// Expiration time
    pub expires_at: SystemTime,
    /// Last refresh time
    pub last_refreshed: SystemTime,
    /// Lock metadata
    pub metadata: LockMetadata,
    /// Lock priority
    pub priority: LockPriority,
    /// Wait start time
    pub wait_start_time: Option<SystemTime>,
}

impl LockInfo {
    /// Check if the lock has expired
    pub fn has_expired(&self) -> bool {
        self.expires_at <= SystemTime::now()
    }

    /// Get remaining time until expiration
    pub fn remaining_time(&self) -> Duration {
        let now = SystemTime::now();
        if self.expires_at > now {
            self.expires_at.duration_since(now).unwrap_or(Duration::ZERO)
        } else {
            Duration::ZERO
        }
    }

    /// Check if the lock is still valid
    pub fn is_valid(&self) -> bool {
        !self.has_expired() && self.status == LockStatus::Acquired
    }
}

/// Lock ID type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LockId {
    pub resource: ObjectKey,
    pub uuid: String,
}

impl LockId {
    /// Generate new lock ID for a resource
    pub fn new(resource: ObjectKey) -> Self {
        Self {
            resource,
            uuid: Uuid::new_v4().to_string(),
        }
    }

    /// Generate unique lock ID for a resource
    /// Each call generates a different ID, even for the same resource
    pub fn new_unique(resource: &ObjectKey) -> Self {
        // Use UUID v4 (random) to ensure uniqueness
        // Each call generates a new unique ID regardless of the resource
        Self {
            resource: resource.clone(),
            uuid: Uuid::new_v4().to_string(),
        }
    }

    /// Get string representation of lock ID ("resource:uuid")
    pub fn as_str(&self) -> String {
        format!("{}:{}", self.resource, self.uuid)
    }
}

impl Default for LockId {
    fn default() -> Self {
        Self::new(ObjectKey::new("default", "default"))
    }
}

impl std::fmt::Display for LockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.resource, self.uuid)
    }
}

/// Lock metadata structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockMetadata {
    /// Client information
    pub client_info: Option<String>,
    /// Operation ID
    pub operation_id: Option<String>,
    /// Priority (lower number = higher priority)
    pub priority: Option<i32>,
    /// Custom tags
    pub tags: std::collections::HashMap<String, String>,
    /// Creation time
    pub created_at: SystemTime,
}

impl Default for LockMetadata {
    fn default() -> Self {
        Self {
            client_info: None,
            operation_id: None,
            priority: None,
            tags: std::collections::HashMap::new(),
            created_at: SystemTime::now(),
        }
    }
}

impl LockMetadata {
    /// Create new lock metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Set client information
    pub fn with_client_info(mut self, client_info: impl Into<String>) -> Self {
        self.client_info = Some(client_info.into());
        self
    }

    /// Set operation ID
    pub fn with_operation_id(mut self, operation_id: impl Into<String>) -> Self {
        self.operation_id = Some(operation_id.into());
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Add tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }
}

/// Lock request structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockRequest {
    /// Lock ID
    pub lock_id: LockId,
    /// Resource path
    pub resource: ObjectKey,
    /// Lock type
    pub lock_type: LockType,
    /// Lock owner
    pub owner: String,
    /// Acquire timeout duration (how long to wait for lock acquisition)
    pub acquire_timeout: Duration,
    /// Lock TTL (Time To Live - how long the lock remains valid after acquisition)
    pub ttl: Duration,
    /// Lock metadata
    pub metadata: LockMetadata,
    /// Lock priority
    pub priority: LockPriority,
    /// Deadlock detection
    pub deadlock_detection: bool,
}

impl LockRequest {
    /// Create new lock request
    pub fn new(resource: ObjectKey, lock_type: LockType, owner: impl Into<String>) -> Self {
        Self {
            lock_id: LockId::new_unique(&resource),
            resource,
            lock_type,
            owner: owner.into(),
            acquire_timeout: Duration::from_secs(10), // Default 10 seconds to acquire
            ttl: Duration::from_secs(30),             // Default 30 seconds lock lifetime
            metadata: LockMetadata::default(),
            priority: LockPriority::default(),
            deadlock_detection: false,
        }
    }

    /// Set acquire timeout (how long to wait for lock acquisition)
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set lock TTL (how long the lock remains valid after acquisition)
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Set metadata
    pub fn with_metadata(mut self, metadata: LockMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: LockPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Set deadlock detection
    pub fn with_deadlock_detection(mut self, enabled: bool) -> Self {
        self.deadlock_detection = enabled;
        self
    }
}

/// Lock response structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockResponse {
    /// Whether lock acquisition was successful
    pub success: bool,
    /// Lock information (if successful)
    pub lock_info: Option<LockInfo>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Wait time
    pub wait_time: Duration,
    /// Position in wait queue
    pub position_in_queue: Option<usize>,
}

impl LockResponse {
    /// Create success response
    pub fn success(lock_info: LockInfo, wait_time: Duration) -> Self {
        Self {
            success: true,
            lock_info: Some(lock_info),
            error: None,
            wait_time,
            position_in_queue: None,
        }
    }

    /// Create failure response
    pub fn failure(error: impl Into<String>, wait_time: Duration) -> Self {
        Self {
            success: false,
            lock_info: None,
            error: Some(error.into()),
            wait_time,
            position_in_queue: None,
        }
    }

    /// Create waiting response
    pub fn waiting(wait_time: Duration, position: usize) -> Self {
        Self {
            success: false,
            lock_info: None,
            error: None,
            wait_time,
            position_in_queue: Some(position),
        }
    }

    /// Check if response indicates success
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Check if response indicates failure
    pub fn is_failure(&self) -> bool {
        !self.success && self.error.is_some()
    }

    /// Check if response indicates waiting
    pub fn is_waiting(&self) -> bool {
        !self.success && self.position_in_queue.is_some()
    }

    /// Get lock info
    pub fn lock_info(&self) -> Option<&LockInfo> {
        self.lock_info.as_ref()
    }
}

/// Lock statistics structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockStats {
    /// Total number of locks
    pub total_locks: usize,
    /// Number of exclusive locks
    pub exclusive_locks: usize,
    /// Number of shared locks
    pub shared_locks: usize,
    /// Number of waiting locks
    pub waiting_locks: usize,
    /// Number of deadlock detections
    pub deadlock_detections: usize,
    /// Number of priority upgrades
    pub priority_upgrades: usize,
    /// Last update time
    pub last_updated: SystemTime,
    /// Total releases
    pub total_releases: usize,
    /// Total hold time
    pub total_hold_time: Duration,
    /// Average hold time
    pub average_hold_time: Duration,
    /// Total wait queues
    pub total_wait_queues: usize,
    /// Queue entries
    pub queue_entries: usize,
    /// Average wait time
    pub avg_wait_time: Duration,
    /// Successful acquires
    pub successful_acquires: usize,
    /// Failed acquires
    pub failed_acquires: usize,
}

impl Default for LockStats {
    fn default() -> Self {
        Self {
            total_locks: 0,
            exclusive_locks: 0,
            shared_locks: 0,
            waiting_locks: 0,
            deadlock_detections: 0,
            priority_upgrades: 0,
            last_updated: SystemTime::now(),
            total_releases: 0,
            total_hold_time: Duration::ZERO,
            average_hold_time: Duration::ZERO,
            total_wait_queues: 0,
            queue_entries: 0,
            avg_wait_time: Duration::ZERO,
            successful_acquires: 0,
            failed_acquires: 0,
        }
    }
}

/// Node information structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID
    pub id: String,
    /// Node address
    pub address: String,
    /// Node status
    pub status: NodeStatus,
    /// Last heartbeat time
    pub last_heartbeat: SystemTime,
    /// Node weight
    pub weight: f64,
}

/// Node status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Online
    #[default]
    Online,
    /// Offline
    Offline,
    /// Degraded
    Degraded,
}

/// Cluster information structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    /// Cluster ID
    pub cluster_id: String,
    /// List of nodes
    pub nodes: Vec<NodeInfo>,
    /// Quorum size
    pub quorum: usize,
    /// Cluster status
    pub status: ClusterStatus,
    /// Last update time
    pub last_updated: SystemTime,
}

/// Cluster status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ClusterStatus {
    /// Healthy
    #[default]
    Healthy,
    /// Degraded
    Degraded,
    /// Unhealthy
    Unhealthy,
}

/// Health check status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Healthy
    Healthy,
    /// Degraded
    Degraded,
    /// Unhealthy
    Unhealthy,
}

/// Health check information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthInfo {
    /// Overall status
    pub status: HealthStatus,
    /// Node ID
    pub node_id: String,
    /// Last heartbeat time
    pub last_heartbeat: SystemTime,
    /// Connected nodes count
    pub connected_nodes: usize,
    /// Total nodes count
    pub total_nodes: usize,
    /// Lock statistics
    pub lock_stats: LockStats,
    /// Error message (if any)
    pub error_message: Option<String>,
}

impl Default for HealthInfo {
    fn default() -> Self {
        Self {
            status: HealthStatus::Healthy,
            node_id: "unknown".to_string(),
            last_heartbeat: SystemTime::now(),
            connected_nodes: 1,
            total_nodes: 1,
            lock_stats: LockStats::default(),
            error_message: None,
        }
    }
}

/// Timestamp type alias
pub type Timestamp = u64;

/// Get current timestamp
pub fn current_timestamp() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

/// Convert timestamp to system time
pub fn timestamp_to_system_time(timestamp: Timestamp) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(timestamp)
}

/// Convert system time to timestamp
pub fn system_time_to_timestamp(time: SystemTime) -> Timestamp {
    time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs()
}

/// Deadlock detection result structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadlockDetectionResult {
    /// Whether deadlock exists
    pub has_deadlock: bool,
    /// Deadlock cycle
    pub deadlock_cycle: Vec<String>,
    /// Suggested resolution
    pub suggested_resolution: Option<String>,
    /// Affected resources
    pub affected_resources: Vec<String>,
    /// Affected owners
    pub affected_owners: Vec<String>,
}

/// Wait graph node structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitGraphNode {
    /// Owner
    pub owner: String,
    /// Resources being waited for
    pub waiting_for: Vec<String>,
    /// Resources currently held
    pub held_resources: Vec<String>,
    /// Priority
    pub priority: LockPriority,
    /// Wait start time
    pub wait_start_time: SystemTime,
}

/// Wait queue item structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitQueueItem {
    /// Owner
    pub owner: String,
    /// Lock type
    pub lock_type: LockType,
    /// Priority
    pub priority: LockPriority,
    /// Wait start time
    pub wait_start_time: SystemTime,
    /// Request time
    pub request_time: SystemTime,
}

impl WaitQueueItem {
    /// Create new wait queue item
    pub fn new(owner: &str, lock_type: LockType, priority: LockPriority) -> Self {
        let now = SystemTime::now();
        Self {
            owner: owner.to_string(),
            lock_type,
            priority,
            wait_start_time: now,
            request_time: now,
        }
    }

    /// Get wait duration
    pub fn wait_duration(&self) -> Duration {
        self.wait_start_time.elapsed().unwrap_or_default()
    }
}
