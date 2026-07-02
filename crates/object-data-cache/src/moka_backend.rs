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

use crate::cache::{ObjectDataCacheFillResult, ObjectDataCacheGetPlan, ObjectDataCacheInvalidationResult, ObjectDataCacheLookup};
use crate::config::ObjectDataCacheConfig;
use crate::entry::ObjectDataCacheEntry;
use crate::index::{ObjectDataCacheIdentityIndex, ObjectDataCacheIndexInsertResult};
use crate::key::ObjectDataCacheIdentity;
use crate::memory::ObjectDataCacheMemoryGate;
use crate::singleflight::{ObjectDataCacheSingleflight, ObjectDataCacheSingleflightAcquire};
use crate::stats::ObjectDataCacheStats;
use bytes::Bytes;
use moka::future::Cache;
use std::sync::Arc;

/// Weighted Moka backend for reusable object bodies.
#[derive(Debug)]
pub struct MokaBackend {
    cache: Cache<crate::key::ObjectDataCacheKey, Arc<ObjectDataCacheEntry>>,
    index: ObjectDataCacheIdentityIndex,
    singleflight: ObjectDataCacheSingleflight,
    memory_gate: ObjectDataCacheMemoryGate,
}

impl MokaBackend {
    /// Creates a new backend from the validated configuration.
    pub fn new(
        config: &ObjectDataCacheConfig,
        stats: Arc<ObjectDataCacheStats>,
    ) -> Result<Self, crate::error::ObjectDataCacheConfigError> {
        let max_capacity = config.resolved_max_bytes()?;
        let ttl = config.ttl;
        let time_to_idle = config.time_to_idle;
        let cache = Cache::builder()
            .max_capacity(max_capacity)
            .weigher(|key, value: &Arc<ObjectDataCacheEntry>| value.estimated_weight(key))
            .time_to_live(ttl)
            .time_to_idle(time_to_idle)
            .build();

        Ok(Self {
            cache,
            index: ObjectDataCacheIdentityIndex::new(usize::from(config.identity_keys_max)),
            singleflight: ObjectDataCacheSingleflight::new(Arc::clone(&stats)),
            memory_gate: ObjectDataCacheMemoryGate::new(config, stats),
        })
    }

    /// Returns the current cache entry count.
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Returns the approximate weighted size of cached entries.
    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    /// Looks up a cached body for the supplied plan.
    pub async fn lookup_body(&self, plan: &ObjectDataCacheGetPlan) -> ObjectDataCacheLookup {
        let ObjectDataCacheGetPlan::Cacheable { key } = plan else {
            return ObjectDataCacheLookup::SkipNotCacheable;
        };

        match self.cache.get(key).await {
            Some(entry) => ObjectDataCacheLookup::Hit(entry.bytes()),
            None => ObjectDataCacheLookup::Miss,
        }
    }

    /// Inserts a cached body for the supplied plan.
    pub async fn fill_body(&self, plan: &ObjectDataCacheGetPlan, bytes: Bytes) -> ObjectDataCacheFillResult {
        let ObjectDataCacheGetPlan::Cacheable { key } = plan else {
            return ObjectDataCacheFillResult::SkippedNotCacheable;
        };

        let fill_state = self.singleflight.acquire(key.clone()).await;
        let ObjectDataCacheSingleflightAcquire::Leader(leader) = fill_state else {
            let ObjectDataCacheSingleflightAcquire::Waiter(waiter) = fill_state else {
                unreachable!();
            };
            return waiter.wait().await;
        };

        if !self.memory_gate.allows_fill(u64::try_from(bytes.len()).unwrap_or(u64::MAX)) {
            return leader.finish(ObjectDataCacheFillResult::SkippedMemoryPressure).await;
        }

        let identity = ObjectDataCacheIdentity::new(Arc::clone(&key.bucket), Arc::clone(&key.object));
        self.index
            .prune_missing(&identity, |candidate| self.cache.contains_key(candidate))
            .await;

        let entry = Arc::new(ObjectDataCacheEntry::new(bytes, key.size, Arc::clone(&key.etag)));
        self.cache.insert(key.clone(), entry).await;

        let result = match self.index.insert(identity, key.clone()).await {
            ObjectDataCacheIndexInsertResult::Inserted | ObjectDataCacheIndexInsertResult::Duplicate => {
                ObjectDataCacheFillResult::Inserted
            }
            ObjectDataCacheIndexInsertResult::Overflow { mut cleared_keys } => {
                cleared_keys.push(key.clone());
                for stale_key in cleared_keys {
                    self.cache.remove(&stale_key).await;
                }
                ObjectDataCacheFillResult::SkippedIdentityOverflow
            }
        };

        leader.finish(result).await
    }

    /// Conservatively invalidates all cached keys matching the object identity.
    pub async fn invalidate_object(&self, identity: &ObjectDataCacheIdentity) -> ObjectDataCacheInvalidationResult {
        let mut keys_to_remove = self.index.remove_identity(identity).await;
        if keys_to_remove.is_empty() {
            keys_to_remove = self
                .cache
                .iter()
                .filter_map(|(key, _value)| {
                    if key.bucket.as_ref() == identity.bucket.as_ref() && key.object.as_ref() == identity.object.as_ref() {
                        Some(Arc::unwrap_or_clone(key))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
        }

        for key in keys_to_remove {
            self.cache.remove(&key).await;
        }

        ObjectDataCacheInvalidationResult::Success
    }
}

#[cfg(test)]
mod tests {
    use super::MokaBackend;
    use crate::cache::{
        ObjectDataCacheFillResult, ObjectDataCacheGetPlan, ObjectDataCacheInvalidationResult, ObjectDataCacheLookup,
    };
    use crate::config::{ObjectDataCacheConfig, ObjectDataCacheMode};
    use crate::key::{ObjectDataCacheBodyVariant, ObjectDataCacheIdentity, ObjectDataCacheKey};
    use crate::stats::ObjectDataCacheStats;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::Duration;

    fn enabled_config() -> ObjectDataCacheConfig {
        ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::FillMaterializeEnabled,
            max_bytes: 8_388_608,
            max_memory_percent: 5,
            max_entry_bytes: 1_048_576,
            ttl: Duration::from_millis(100),
            time_to_idle: Duration::from_millis(100),
            min_free_memory_percent: 20,
            fill_concurrency_per_cpu: 1,
            fill_concurrency_max: 32,
            identity_keys_max: 16,
        }
    }

    fn cacheable_plan(object: &str, etag: &str) -> ObjectDataCacheGetPlan {
        ObjectDataCacheGetPlan::Cacheable {
            key: ObjectDataCacheKey::new("bucket", object, None, etag, 5, ObjectDataCacheBodyVariant::FullObjectPlainV1),
        }
    }

    #[tokio::test]
    async fn moka_backend_round_trips_cached_body() {
        let backend =
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan = cacheable_plan("object", "etag-a");

        let fill = backend.fill_body(&plan, Bytes::from_static(b"hello")).await;
        let lookup = backend.lookup_body(&plan).await;

        assert!(matches!(fill, ObjectDataCacheFillResult::Inserted));
        assert!(matches!(lookup, ObjectDataCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"hello"));
    }

    #[tokio::test]
    async fn moka_backend_invalidates_matching_identity() {
        let backend =
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan_a = cacheable_plan("object-a", "etag-a");
        let plan_b = cacheable_plan("object-b", "etag-b");

        let _ = backend.fill_body(&plan_a, Bytes::from_static(b"aaaaa")).await;
        let _ = backend.fill_body(&plan_b, Bytes::from_static(b"bbbbb")).await;

        let result = backend
            .invalidate_object(&ObjectDataCacheIdentity::new("bucket", "object-a"))
            .await;
        let lookup_a = backend.lookup_body(&plan_a).await;
        let lookup_b = backend.lookup_body(&plan_b).await;

        assert!(matches!(result, ObjectDataCacheInvalidationResult::Success));
        assert!(matches!(lookup_a, ObjectDataCacheLookup::Miss));
        assert!(matches!(lookup_b, ObjectDataCacheLookup::Hit(_)));
    }

    #[tokio::test]
    async fn moka_backend_expires_entries_by_ttl() {
        let backend =
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan = cacheable_plan("object", "etag-a");

        let _ = backend.fill_body(&plan, Bytes::from_static(b"hello")).await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        let lookup = backend.lookup_body(&plan).await;

        assert!(matches!(lookup, ObjectDataCacheLookup::Miss));
    }

    #[tokio::test]
    async fn moka_backend_expires_entries_by_tti() {
        let mut config = enabled_config();
        config.ttl = Duration::from_secs(30);
        config.time_to_idle = Duration::from_millis(100);
        let backend = MokaBackend::new(&config, Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan = cacheable_plan("object", "etag-a");

        let _ = backend.fill_body(&plan, Bytes::from_static(b"hello")).await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        let lookup = backend.lookup_body(&plan).await;

        assert!(matches!(lookup, ObjectDataCacheLookup::Miss));
    }

    #[tokio::test]
    async fn moka_backend_skips_fill_under_memory_pressure() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let backend = MokaBackend::new(&enabled_config(), Arc::clone(&stats)).expect("moka backend should build");
        backend
            .memory_gate
            .set_test_snapshot(Some(crate::memory::ObjectDataCacheMemorySnapshot {
                total_bytes: 1_000,
                available_bytes: 100,
            }));
        let plan = cacheable_plan("object", "etag-a");

        let result = backend.fill_body(&plan, Bytes::from_static(b"hello")).await;
        let lookup = backend.lookup_body(&plan).await;

        assert_eq!(result, ObjectDataCacheFillResult::SkippedMemoryPressure);
        assert!(matches!(lookup, ObjectDataCacheLookup::Miss));
        assert_eq!(stats.snapshot().memory_pressure_events, 1);
    }

    #[tokio::test]
    async fn moka_backend_singleflight_waiter_observes_leader_result() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let backend = Arc::new(MokaBackend::new(&enabled_config(), Arc::clone(&stats)).expect("moka backend should build"));
        let plan = cacheable_plan("object", "etag-a");
        let leader_plan = plan.clone();
        let plan_clone = plan.clone();
        let first = Arc::clone(&backend);
        let second = Arc::clone(&backend);

        let leader = tokio::spawn(async move { first.fill_body(&leader_plan, Bytes::from_static(b"hello")).await });
        let waiter = tokio::spawn(async move { second.fill_body(&plan_clone, Bytes::from_static(b"hello")).await });

        let leader_result = leader.await.expect("leader task should complete");
        let waiter_result = waiter.await.expect("waiter task should complete");
        let lookup = backend.lookup_body(&plan).await;

        assert_eq!(leader_result, ObjectDataCacheFillResult::Inserted);
        assert_eq!(waiter_result, ObjectDataCacheFillResult::Inserted);
        assert!(matches!(lookup, ObjectDataCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"hello"));
    }
}
