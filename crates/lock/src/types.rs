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
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

use crate::ObjectKey;

/// Wire version for namespace-lock cache-fence stamps.
pub const CACHE_FENCE_STAMP_VERSION: u8 = 1;
/// Fixed number of mutation-counter shards maintained by each lock voter.
pub const CACHE_FENCE_SHARD_COUNT: usize = 4096;
const CACHE_FENCE_SHARD_COUNT_U16: u16 = 4096;
const CACHE_FENCE_SHARD_MASK: u16 = CACHE_FENCE_SHARD_COUNT_U16 - 1;
const CACHE_FENCE_STAMP_ENCODED_LEN: usize = 27;

/// Stable shard for namespace mutation generations. Hash collisions only cause
/// conservative cache invalidation; this is not an identity or authorization hash.
pub(crate) fn cache_fence_shard_index(resource: &ObjectKey) -> u16 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    fn update(mut hash: u64, bytes: &[u8]) -> u64 {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let mut hash = update(FNV_OFFSET, resource.bucket.as_bytes());
    hash = update(hash, &[0]);
    hash = update(hash, resource.object.as_bytes());
    hash = update(hash, &[1]);
    if let Some(version) = &resource.version {
        hash = update(hash, &[1]);
        hash = update(hash, version.as_bytes());
    } else {
        hash = update(hash, &[0]);
    }
    let bytes = hash.to_le_bytes();
    u16::from_le_bytes([bytes[0], bytes[1]]) & CACHE_FENCE_SHARD_MASK
}

/// One lock voter's proof that identifies its process lifetime and the current
/// mutation generation for a stable object-key shard.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CacheFenceStamp {
    voter_boot_id: Uuid,
    shard_index: u16,
    generation: u64,
}

impl CacheFenceStamp {
    pub(crate) fn new(voter_boot_id: Uuid, shard_index: u16, generation: u64) -> Option<Self> {
        (voter_boot_id != Uuid::nil() && shard_index < CACHE_FENCE_SHARD_COUNT_U16 && generation != u64::MAX).then_some(Self {
            voter_boot_id,
            shard_index,
            generation,
        })
    }

    /// Unique identity for this lock-voter process lifetime.
    pub fn voter_boot_id(self) -> Uuid {
        self.voter_boot_id
    }

    /// Stable object-key shard whose generation this stamp describes.
    pub fn shard_index(self) -> u16 {
        self.shard_index
    }

    /// Monotonic generation observed or advanced by the lock grant.
    pub fn generation(self) -> u64 {
        self.generation
    }

    /// Encode the stamp for the optional lock-response proof field.
    pub fn encode(self) -> [u8; CACHE_FENCE_STAMP_ENCODED_LEN] {
        let mut encoded = [0; CACHE_FENCE_STAMP_ENCODED_LEN];
        encoded[0] = CACHE_FENCE_STAMP_VERSION;
        encoded[1..17].copy_from_slice(self.voter_boot_id.as_bytes());
        encoded[17..19].copy_from_slice(&self.shard_index.to_le_bytes());
        encoded[19..27].copy_from_slice(&self.generation.to_le_bytes());
        encoded
    }

    /// Decode and strictly validate an optional lock-response proof field.
    ///
    /// # Errors
    /// Returns [`CacheFenceStampDecodeError`] for unknown versions, malformed
    /// lengths, invalid voter identities, out-of-range shards, or an exhausted
    /// generation sentinel.
    pub fn decode(encoded: &[u8]) -> Result<Self, CacheFenceStampDecodeError> {
        if encoded.len() != CACHE_FENCE_STAMP_ENCODED_LEN {
            return Err(CacheFenceStampDecodeError::InvalidLength);
        }
        if encoded[0] != CACHE_FENCE_STAMP_VERSION {
            return Err(CacheFenceStampDecodeError::UnsupportedVersion);
        }
        let voter_boot_id = Uuid::from_slice(&encoded[1..17]).map_err(|_| CacheFenceStampDecodeError::InvalidBootId)?;
        if voter_boot_id.is_nil() {
            return Err(CacheFenceStampDecodeError::InvalidBootId);
        }
        let shard_index = u16::from_le_bytes([encoded[17], encoded[18]]);
        if shard_index >= CACHE_FENCE_SHARD_COUNT_U16 {
            return Err(CacheFenceStampDecodeError::InvalidShard);
        }
        let generation = u64::from_le_bytes(
            encoded[19..27]
                .try_into()
                .map_err(|_| CacheFenceStampDecodeError::InvalidLength)?,
        );
        if generation == u64::MAX {
            return Err(CacheFenceStampDecodeError::ExhaustedGeneration);
        }
        Ok(Self {
            voter_boot_id,
            shard_index,
            generation,
        })
    }
}

/// Invalid or unsupported cache-fence proof received from a lock peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CacheFenceStampDecodeError {
    #[error("cache-fence stamp has an invalid length")]
    InvalidLength,
    #[error("cache-fence stamp uses an unsupported protocol version")]
    UnsupportedVersion,
    #[error("cache-fence stamp has an invalid voter boot id")]
    InvalidBootId,
    #[error("cache-fence stamp has an invalid shard")]
    InvalidShard,
    #[error("cache-fence stamp generation is exhausted")]
    ExhaustedGeneration,
}

/// A deterministic read-quorum proof suitable for binding a future metadata
/// cache entry to the namespace-lock cohort that authorized its fill.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheFenceProof {
    stamps: Arc<[CacheFenceStamp]>,
}

impl CacheFenceProof {
    pub(crate) fn from_grants(
        resource: &ObjectKey,
        stamps: impl IntoIterator<Item = CacheFenceStamp>,
        required_quorum: usize,
    ) -> Option<Self> {
        if required_quorum == 0 {
            return None;
        }
        let expected_shard = cache_fence_shard_index(resource);
        let mut stamps = stamps.into_iter().collect::<Vec<_>>();
        if stamps.iter().any(|stamp| stamp.shard_index != expected_shard) {
            return None;
        }
        stamps.sort_unstable_by_key(|stamp| stamp.voter_boot_id);
        if stamps.windows(2).any(|pair| pair[0].voter_boot_id == pair[1].voter_boot_id) || stamps.len() < required_quorum {
            return None;
        }
        stamps.truncate(required_quorum);
        Some(Self { stamps: stamps.into() })
    }

    /// Sorted, distinct voter stamps that form this proof.
    pub fn stamps(&self) -> &[CacheFenceStamp] {
        &self.stamps
    }
}

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

/// Point-in-time lease information exposed by lock clients for diagnostics.
#[derive(Debug, Clone)]
pub struct LockLeaseInfo {
    /// Resource protected by the lock.
    pub resource: ObjectKey,
    /// Shared or exclusive lock mode.
    pub lock_type: LockType,
    /// Lock owner recorded by the local lock backend.
    pub owner: String,
    /// Original acquisition time. Refreshes do not change this value.
    pub acquired_at: SystemTime,
    /// Opaque guard identity used to reject stale diagnostic snapshots.
    pub guard_id: Option<u64>,
    /// Remaining lease duration derived from the monotonic lease deadline.
    pub remaining_ttl: Duration,
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
    /// Suppress warning logs for expected contention failures
    pub suppress_contention_logs: bool,
    /// Optional override for the guard heartbeat/refresh interval.
    ///
    /// `None` means the holding guard derives an interval from `ttl` (ttl/3). Mainly injected
    /// by tests to drive very small intervals; `#[serde(default)]` keeps older RPC payloads
    /// (which never encoded this field) deserializable.
    #[serde(default)]
    pub refresh_interval: Option<Duration>,
    /// Request a cache-fence stamp from shared-lock voters. Exclusive grants
    /// always advance their voter generation, including for legacy requests.
    #[serde(default)]
    pub cache_fence_stamp_requested: bool,
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
            suppress_contention_logs: false,
            refresh_interval: None,
            cache_fence_stamp_requested: false,
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

    /// Suppress warning logs for expected contention failures
    pub fn with_suppress_contention_logs(mut self, enabled: bool) -> Self {
        self.suppress_contention_logs = enabled;
        self
    }

    /// Override the guard heartbeat/refresh interval (otherwise derived from `ttl`).
    pub fn with_refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = Some(interval);
        self
    }

    /// Ask capable shared-lock voters to return their current cache-fence stamp.
    pub fn request_cache_fence_stamp(mut self) -> Self {
        self.cache_fence_stamp_requested = true;
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
    /// Optional cache-fence generation returned by the granting voter.
    #[serde(skip)]
    pub cache_fence_stamp: Option<CacheFenceStamp>,
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
            cache_fence_stamp: None,
        }
    }

    /// Create a success response carrying a validated cache-fence stamp.
    pub fn success_with_cache_fence_stamp(lock_info: LockInfo, wait_time: Duration, cache_fence_stamp: CacheFenceStamp) -> Self {
        Self {
            success: true,
            lock_info: Some(lock_info),
            error: None,
            wait_time,
            position_in_queue: None,
            cache_fence_stamp: Some(cache_fence_stamp),
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
            cache_fence_stamp: None,
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
            cache_fence_stamp: None,
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

#[cfg(test)]
mod cache_fence_tests {
    use super::{
        CACHE_FENCE_SHARD_COUNT_U16, CACHE_FENCE_STAMP_VERSION, CacheFenceProof, CacheFenceStamp, CacheFenceStampDecodeError,
        cache_fence_shard_index,
    };
    use crate::{LockRequest, LockType, ObjectKey};
    use uuid::Uuid;

    fn stamp(boot: u128, shard: u16, generation: u64) -> CacheFenceStamp {
        CacheFenceStamp::new(Uuid::from_u128(boot), shard, generation).expect("test stamp should be valid")
    }

    #[test]
    fn cache_fence_stamp_round_trip_and_strict_validation() {
        let original = stamp(1, 17, 42);
        let encoded = original.encode();
        assert_eq!(CacheFenceStamp::decode(&encoded), Ok(original));

        assert_eq!(CacheFenceStamp::decode(&encoded[..26]), Err(CacheFenceStampDecodeError::InvalidLength));

        let mut invalid = encoded;
        invalid[0] = CACHE_FENCE_STAMP_VERSION + 1;
        assert_eq!(CacheFenceStamp::decode(&invalid), Err(CacheFenceStampDecodeError::UnsupportedVersion));

        let mut invalid = encoded;
        invalid[1..17].fill(0);
        assert_eq!(CacheFenceStamp::decode(&invalid), Err(CacheFenceStampDecodeError::InvalidBootId));

        let mut invalid = encoded;
        invalid[17..19].copy_from_slice(&CACHE_FENCE_SHARD_COUNT_U16.to_le_bytes());
        assert_eq!(CacheFenceStamp::decode(&invalid), Err(CacheFenceStampDecodeError::InvalidShard));

        let mut invalid = encoded;
        invalid[19..27].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(CacheFenceStamp::decode(&invalid), Err(CacheFenceStampDecodeError::ExhaustedGeneration));
    }

    #[test]
    fn cache_fence_proof_requires_distinct_matching_quorum_stamps() {
        let resource = ObjectKey::new("bucket", "object");
        let shard = cache_fence_shard_index(&resource);
        assert_eq!(shard, 451, "stable object-key hash must not drift silently");
        let first = stamp(1, shard, 7);
        let second = stamp(2, shard, 7);

        let proof = CacheFenceProof::from_grants(&resource, [second, first], 2).expect("two distinct voters form quorum proof");
        assert_eq!(proof.stamps(), &[first, second], "proof order is deterministic");
        assert_eq!(CacheFenceProof::from_grants(&resource, [first], 2), None);
        assert_eq!(CacheFenceProof::from_grants(&resource, [first, first], 2), None);

        let other_shard = (shard + 1) % CACHE_FENCE_SHARD_COUNT_U16;
        assert_eq!(CacheFenceProof::from_grants(&resource, [first, stamp(3, other_shard, 7)], 2), None);
    }

    #[test]
    fn read_and_write_quorums_always_intersect_for_supported_fleets() {
        for voters in 1_u32..=16 {
            let write_quorum = voters / 2 + 1;
            let read_quorum = voters - voters / 2;
            let all = (1_u32 << voters) - 1;
            for cohort in 0..=all {
                if cohort.count_ones() == write_quorum {
                    assert!(
                        (all ^ cohort).count_ones() < read_quorum,
                        "a read quorum must not fit outside a successful write quorum: voters={voters}, cohort={cohort:#x}"
                    );
                }
                if cohort.count_ones() == read_quorum {
                    assert!(
                        (all ^ cohort).count_ones() < write_quorum,
                        "a write quorum must not fit outside a successful read quorum: voters={voters}, cohort={cohort:#x}"
                    );
                }
            }
        }
    }

    #[test]
    fn cache_fence_request_flag_defaults_off_for_legacy_json() {
        let request =
            LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Shared, "reader").request_cache_fence_stamp();
        let mut encoded = serde_json::to_value(&request).expect("encode current lock request");
        encoded
            .as_object_mut()
            .expect("lock request encodes as an object")
            .remove("cache_fence_stamp_requested");
        let legacy: LockRequest = serde_json::from_value(encoded).expect("decode request from a legacy peer");
        assert!(!legacy.cache_fence_stamp_requested);

        let current: LockRequest = serde_json::from_str(&serde_json::to_string(&request).expect("encode requested proof"))
            .expect("decode requested proof");
        assert!(current.cache_fence_stamp_requested);
    }
}
