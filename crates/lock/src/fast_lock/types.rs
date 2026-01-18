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

use crate::fast_lock::guard::FastLockGuard;
use serde::{Deserialize, Serialize};
use smartstring::SmartString;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

/// Object key for version-aware locking
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey {
    pub bucket: Arc<str>,
    pub object: Arc<str>,
    pub version: Option<Arc<str>>, // None means latest version
}

impl ObjectKey {
    pub fn new(bucket: impl Into<Arc<str>>, object: impl Into<Arc<str>>) -> Self {
        Self {
            bucket: bucket.into(),
            object: object.into(),
            version: None,
        }
    }

    pub fn with_version(bucket: impl Into<Arc<str>>, object: impl Into<Arc<str>>, version: impl Into<Arc<str>>) -> Self {
        Self {
            bucket: bucket.into(),
            object: object.into(),
            version: Some(version.into()),
        }
    }

    pub fn as_latest(&self) -> Self {
        Self {
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            version: None,
        }
    }

    /// Get shard index from object key hash
    pub fn shard_index(&self, shard_mask: usize) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish() as usize & shard_mask
    }
}

/// Optimized object key using smart strings for better performance
#[derive(Debug, Clone)]
pub struct OptimizedObjectKey {
    /// Bucket name - uses inline storage for small strings
    pub bucket: SmartString<smartstring::LazyCompact>,
    /// Object name - uses inline storage for small strings
    pub object: SmartString<smartstring::LazyCompact>,
    /// Version - optional for latest version semantics
    pub version: Option<SmartString<smartstring::LazyCompact>>,
    /// Cached hash to avoid recomputation
    hash_cache: OnceLock<u64>,
}

// Manual implementations to handle OnceLock properly
impl PartialEq for OptimizedObjectKey {
    fn eq(&self, other: &Self) -> bool {
        self.bucket == other.bucket && self.object == other.object && self.version == other.version
    }
}

impl Eq for OptimizedObjectKey {}

impl Hash for OptimizedObjectKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bucket.hash(state);
        self.object.hash(state);
        self.version.hash(state);
    }
}

impl PartialOrd for OptimizedObjectKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OptimizedObjectKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bucket
            .cmp(&other.bucket)
            .then_with(|| self.object.cmp(&other.object))
            .then_with(|| self.version.cmp(&other.version))
    }
}

impl OptimizedObjectKey {
    pub fn new(
        bucket: impl Into<SmartString<smartstring::LazyCompact>>,
        object: impl Into<SmartString<smartstring::LazyCompact>>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            object: object.into(),
            version: None,
            hash_cache: OnceLock::new(),
        }
    }

    pub fn with_version(
        bucket: impl Into<SmartString<smartstring::LazyCompact>>,
        object: impl Into<SmartString<smartstring::LazyCompact>>,
        version: impl Into<SmartString<smartstring::LazyCompact>>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            object: object.into(),
            version: Some(version.into()),
            hash_cache: OnceLock::new(),
        }
    }

    /// Get shard index with cached hash for better performance
    pub fn shard_index(&self, shard_mask: usize) -> usize {
        let hash = *self.hash_cache.get_or_init(|| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.hash(&mut hasher);
            hasher.finish()
        });
        (hash as usize) & shard_mask
    }

    /// Reset hash cache if key is modified
    pub fn invalidate_cache(&mut self) {
        self.hash_cache = OnceLock::new();
    }

    /// Convert from regular ObjectKey
    pub fn from_object_key(key: &ObjectKey) -> Self {
        Self {
            bucket: SmartString::from(key.bucket.as_ref()),
            object: SmartString::from(key.object.as_ref()),
            version: key.version.as_ref().map(|v| SmartString::from(v.as_ref())),
            hash_cache: OnceLock::new(),
        }
    }

    /// Convert to regular ObjectKey
    pub fn to_object_key(&self) -> ObjectKey {
        ObjectKey {
            bucket: Arc::from(self.bucket.as_str()),
            object: Arc::from(self.object.as_str()),
            version: self.version.as_ref().map(|v| Arc::from(v.as_str())),
        }
    }
}

impl std::fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(version) = &self.version {
            write!(f, "{}/{}@{}", self.bucket, self.object, version)
        } else {
            write!(f, "{}/{}@latest", self.bucket, self.object)
        }
    }
}

/// Lock type for object operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockMode {
    /// Shared lock for read operations
    Shared,
    /// Exclusive lock for write operations  
    Exclusive,
}

/// Lock request for object
#[derive(Debug, Clone)]
pub struct ObjectLockRequest {
    pub key: ObjectKey,
    pub mode: LockMode,
    pub owner: Arc<str>,
    pub acquire_timeout: Duration,
    pub lock_timeout: Duration,
    pub priority: LockPriority,
}

impl ObjectLockRequest {
    pub fn new_read(bucket: impl Into<Arc<str>>, object: impl Into<Arc<str>>, owner: impl Into<Arc<str>>) -> Self {
        Self {
            key: ObjectKey::new(bucket, object),
            mode: LockMode::Shared,
            owner: owner.into(),
            acquire_timeout: crate::fast_lock::DEFAULT_ACQUIRE_TIMEOUT,
            lock_timeout: crate::fast_lock::DEFAULT_LOCK_TIMEOUT,
            priority: LockPriority::Normal,
        }
    }

    pub fn new_write(bucket: impl Into<Arc<str>>, object: impl Into<Arc<str>>, owner: impl Into<Arc<str>>) -> Self {
        Self {
            key: ObjectKey::new(bucket, object),
            mode: LockMode::Exclusive,
            owner: owner.into(),
            acquire_timeout: crate::fast_lock::DEFAULT_ACQUIRE_TIMEOUT,
            lock_timeout: crate::fast_lock::DEFAULT_LOCK_TIMEOUT,
            priority: LockPriority::Normal,
        }
    }

    pub fn with_version(mut self, version: impl Into<Arc<str>>) -> Self {
        self.key.version = Some(version.into());
        self
    }

    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    pub fn with_lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = timeout;
        self
    }

    pub fn with_priority(mut self, priority: LockPriority) -> Self {
        self.priority = priority;
        self
    }
}

/// Lock priority for conflict resolution
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum LockPriority {
    Low = 1,
    #[default]
    Normal = 2,
    High = 3,
    Critical = 4,
}

/// Lock acquisition result
#[derive(Debug)]
pub enum LockResult {
    /// Lock acquired successfully
    Acquired,
    /// Lock acquisition failed due to timeout
    Timeout,
    /// Lock acquisition failed due to conflict
    Conflict {
        current_owner: Arc<str>,
        current_mode: LockMode,
    },
}

/// Configuration for the lock manager
#[derive(Debug, Clone)]
pub struct LockConfig {
    pub shard_count: usize,
    pub default_lock_timeout: Duration,
    pub default_acquire_timeout: Duration,
    pub cleanup_interval: Duration,
    pub max_idle_time: Duration,
    pub enable_metrics: bool,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            shard_count: crate::fast_lock::DEFAULT_SHARD_COUNT,
            default_lock_timeout: crate::fast_lock::DEFAULT_LOCK_TIMEOUT,
            default_acquire_timeout: crate::fast_lock::DEFAULT_ACQUIRE_TIMEOUT,
            cleanup_interval: crate::fast_lock::CLEANUP_INTERVAL,
            max_idle_time: Duration::from_secs(300), // 5 minutes
            enable_metrics: true,
        }
    }
}

/// Lock information for monitoring
#[derive(Debug, Clone)]
pub struct ObjectLockInfo {
    pub key: ObjectKey,
    pub mode: LockMode,
    pub owner: Arc<str>,
    pub acquired_at: SystemTime,
    pub expires_at: SystemTime,
    pub priority: LockPriority,
}

/// Batch lock operation request
#[derive(Debug)]
pub struct BatchLockRequest {
    pub requests: Vec<ObjectLockRequest>,
    pub owner: Arc<str>,
    pub all_or_nothing: bool, // If true, either all locks are acquired or none
}

impl BatchLockRequest {
    pub fn new(owner: impl Into<Arc<str>>) -> Self {
        Self {
            requests: Vec::new(),
            owner: owner.into(),
            all_or_nothing: true,
        }
    }

    pub fn add_read_lock(mut self, bucket: impl Into<Arc<str>>, object: impl Into<Arc<str>>) -> Self {
        self.requests
            .push(ObjectLockRequest::new_read(bucket, object, self.owner.clone()));
        self
    }

    pub fn add_write_lock(mut self, bucket: impl Into<Arc<str>>, object: impl Into<Arc<str>>) -> Self {
        self.requests
            .push(ObjectLockRequest::new_write(bucket, object, self.owner.clone()));
        self
    }

    pub fn with_all_or_nothing(mut self, enable: bool) -> Self {
        self.all_or_nothing = enable;
        self
    }
}

/// Batch lock operation result
#[derive(Debug)]
pub struct BatchLockResult {
    pub successful_locks: Vec<ObjectKey>,
    pub failed_locks: Vec<(ObjectKey, LockResult)>,
    pub all_acquired: bool,
    pub guards: Vec<FastLockGuard>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_key() {
        let key1 = ObjectKey::new("bucket1", "object1");
        let key2 = ObjectKey::with_version("bucket1", "object1", "v1");

        assert_eq!(key1.bucket.as_ref(), "bucket1");
        assert_eq!(key1.object.as_ref(), "object1");
        assert_eq!(key1.version, None);

        assert_eq!(key2.version.as_ref().unwrap().as_ref(), "v1");

        // Test display
        assert_eq!(key1.to_string(), "bucket1/object1@latest");
        assert_eq!(key2.to_string(), "bucket1/object1@v1");
    }

    #[test]
    fn test_lock_request() {
        let req = ObjectLockRequest::new_read("bucket", "object", "owner")
            .with_version("v1")
            .with_priority(LockPriority::High);

        assert_eq!(req.mode, LockMode::Shared);
        assert_eq!(req.priority, LockPriority::High);
        assert_eq!(req.key.version.as_ref().unwrap().as_ref(), "v1");
    }

    #[test]
    fn test_batch_request() {
        let batch = BatchLockRequest::new("owner")
            .add_read_lock("bucket", "obj1")
            .add_write_lock("bucket", "obj2");

        assert_eq!(batch.requests.len(), 2);
        assert_eq!(batch.requests[0].mode, LockMode::Shared);
        assert_eq!(batch.requests[1].mode, LockMode::Exclusive);
    }
}
