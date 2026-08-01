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

//! Caching layer for KMS operations to improve performance

use crate::types::KeyMetadata;
use moka::future::Cache;
use moka::notification::RemovalCause;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Default lifetime of a cached key metadata entry.
const DEFAULT_METADATA_TTL: Duration = Duration::from_secs(300);

// ---------------------------------------------------------------------------
// Metrics
//
// Lookup outcomes and removals are counted here because moka exposes neither
// hit nor miss counts; the numbers below are the cache's own, not derived.
// Label values are exclusively static strings (lookup result, removal cause) —
// key identifiers and key metadata must never reach a metric label.
// ---------------------------------------------------------------------------

/// Counter: key metadata lookups, by `result` (`hit` or `miss`).
const METRIC_CACHE_LOOKUPS_TOTAL: &str = "rustfs_kms_metadata_cache_lookups_total";
/// Counter: entries dropped from the cache, by `cause` (`expired`, `size`,
/// `explicit`, `replaced`); only `expired` and `size` are true evictions, the
/// rest are invalidations driven by key lifecycle operations.
const METRIC_CACHE_EVICTIONS_TOTAL: &str = "rustfs_kms_metadata_cache_evictions_total";
/// Gauge: entries currently held by the cache.
const METRIC_CACHE_ENTRIES: &str = "rustfs_kms_metadata_cache_entries";

/// Register metric descriptions once per process.
fn describe_metrics() {
    static DESCRIBE: std::sync::Once = std::sync::Once::new();
    DESCRIBE.call_once(|| {
        metrics::describe_counter!(METRIC_CACHE_LOOKUPS_TOTAL, "Total KMS key metadata cache lookups, by hit or miss");
        metrics::describe_counter!(
            METRIC_CACHE_EVICTIONS_TOTAL,
            "Total entries dropped from the KMS key metadata cache, by removal cause"
        );
        metrics::describe_gauge!(METRIC_CACHE_ENTRIES, "Entries currently held by the KMS key metadata cache");
    });
}

fn removal_cause_label(cause: RemovalCause) -> &'static str {
    match cause {
        RemovalCause::Expired => "expired",
        RemovalCause::Explicit => "explicit",
        RemovalCause::Replaced => "replaced",
        RemovalCause::Size => "size",
    }
}

/// Cumulative cache counters. Shared with moka's eviction listener, which runs
/// outside any `&self` borrow, hence the `Arc` and the atomics.
#[derive(Debug, Default)]
struct CacheCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

/// Snapshot of the KMS key metadata cache counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KmsCacheStats {
    /// Entries currently held. Eventually consistent: moka applies pending
    /// maintenance lazily.
    pub entries: u64,
    /// Lookups served from the cache since process start.
    pub hits: u64,
    /// Lookups that fell through to the backend since process start.
    pub misses: u64,
    /// Entries dropped since process start, whatever the cause (expiry,
    /// capacity, invalidation, replacement).
    pub evictions: u64,
}

/// KMS cache for storing frequently accessed keys and metadata
pub struct KmsCache {
    key_metadata_cache: Cache<String, KeyMetadata>,
    counters: Arc<CacheCounters>,
}

impl KmsCache {
    /// Create a new KMS cache with the specified capacity
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of entries in the cache
    ///
    /// # Returns
    /// A new instance of `KmsCache`
    ///
    pub fn new(capacity: u64) -> Self {
        Self::with_ttl(capacity, DEFAULT_METADATA_TTL)
    }

    /// Create a new KMS cache with an explicit metadata time-to-live
    fn with_ttl(capacity: u64, metadata_ttl: Duration) -> Self {
        describe_metrics();

        let counters = Arc::new(CacheCounters::default());
        let eviction_counters = Arc::clone(&counters);

        Self {
            key_metadata_cache: Cache::builder()
                .max_capacity(capacity)
                .time_to_live(metadata_ttl)
                .eviction_listener(move |_key: Arc<String>, _metadata: KeyMetadata, cause: RemovalCause| {
                    eviction_counters.evictions.fetch_add(1, Ordering::Relaxed);
                    metrics::counter!(METRIC_CACHE_EVICTIONS_TOTAL, "cause" => removal_cause_label(cause)).increment(1);
                })
                .build(),
            counters,
        }
    }

    /// Get key metadata from cache
    ///
    /// # Arguments
    /// * `key_id` - The ID of the key to retrieve metadata for
    ///
    /// # Returns
    /// An `Option` containing the `KeyMetadata` if found, or `None` if not found
    ///
    pub async fn get_key_metadata(&self, key_id: &str) -> Option<KeyMetadata> {
        let cached = self.key_metadata_cache.get(key_id).await;
        self.record_lookup(cached.is_some());
        cached
    }

    /// Put key metadata into cache
    ///
    /// # Arguments
    /// * `key_id` - The ID of the key to store metadata for
    /// * `metadata` - The `KeyMetadata` to store in the cache
    ///
    pub async fn put_key_metadata(&mut self, key_id: &str, metadata: &KeyMetadata) {
        self.key_metadata_cache.insert(key_id.to_string(), metadata.clone()).await;
        self.key_metadata_cache.run_pending_tasks().await;
        self.record_entry_count();
    }

    /// Remove key metadata from cache
    ///
    /// # Arguments
    /// * `key_id` - The ID of the key to remove metadata for
    ///
    pub async fn remove_key_metadata(&mut self, key_id: &str) {
        self.key_metadata_cache.remove(key_id).await;

        // Flush maintenance so the removal notification and the entry gauge
        // describe the cache as it is once this call returns.
        self.key_metadata_cache.run_pending_tasks().await;
        self.record_entry_count();
    }

    /// Clear all cached entries
    pub async fn clear(&mut self) {
        self.key_metadata_cache.invalidate_all();

        // Wait for invalidation to complete
        self.key_metadata_cache.run_pending_tasks().await;
        self.record_entry_count();
    }

    /// Get cache statistics
    ///
    /// # Returns
    /// A [`KmsCacheStats`] snapshot of entry count, hits, misses and evictions
    ///
    pub fn stats(&self) -> KmsCacheStats {
        KmsCacheStats {
            entries: self.key_metadata_cache.entry_count(),
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            evictions: self.counters.evictions.load(Ordering::Relaxed),
        }
    }

    fn record_lookup(&self, hit: bool) {
        let (counter, result) = if hit {
            (&self.counters.hits, "hit")
        } else {
            (&self.counters.misses, "miss")
        };
        counter.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(METRIC_CACHE_LOOKUPS_TOTAL, "result" => result).increment(1);
    }

    fn record_entry_count(&self) {
        metrics::gauge!(METRIC_CACHE_ENTRIES).set(self.key_metadata_cache.entry_count() as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{KeyState, KeyUsage};
    use jiff::Zoned;
    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::time::Duration;

    impl KmsCache {
        fn contains_key_metadata_for_tests(&self, key_id: &str) -> bool {
            self.key_metadata_cache.contains_key(key_id)
        }
    }

    fn test_metadata(key_id: &str) -> KeyMetadata {
        KeyMetadata {
            key_id: key_id.to_string(),
            key_state: KeyState::Enabled,
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            creation_date: Zoned::now(),
            deletion_date: None,
            origin: "KMS".to_string(),
            key_manager: "CUSTOMER".to_string(),
            tags: std::collections::HashMap::new(),
        }
    }

    type MetricEntry = (
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    );

    /// Drive `test` on a current-thread runtime under a thread-local debugging
    /// recorder and return its output plus one snapshot of everything emitted.
    ///
    /// A single snapshot per test on purpose: `Snapshotter::snapshot` drains
    /// the recorded state, so taking it per assertion would leave every
    /// assertion after the first with nothing to look at.
    fn record_metrics<F, Fut, T>(test: F) -> (T, Vec<MetricEntry>)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let output = metrics::with_local_recorder(&recorder, || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread runtime must build");
            runtime.block_on(test())
        });
        (output, snapshotter.snapshot().into_vec())
    }

    /// Sum of counters with `name` whose labels include every `(key, value)` pair.
    fn counter_value(snapshot: &[MetricEntry], name: &str, labels: &[(&str, &str)]) -> u64 {
        snapshot
            .iter()
            .filter_map(|(composite, _unit, _description, value)| {
                let key = composite.key();
                let matches = composite.kind() == MetricKind::Counter
                    && key.name() == name
                    && labels
                        .iter()
                        .all(|(label, expected)| key.labels().any(|l| l.key() == *label && l.value() == *expected));
                match (matches, value) {
                    (true, DebugValue::Counter(count)) => Some(*count),
                    _ => None,
                }
            })
            .sum()
    }

    /// Last value recorded for the unlabelled gauge `name`.
    fn gauge_value(snapshot: &[MetricEntry], name: &str) -> Option<f64> {
        snapshot.iter().find_map(|(composite, _unit, _description, value)| {
            let matches = composite.kind() == MetricKind::Gauge && composite.key().name() == name;
            match (matches, value) {
                (true, DebugValue::Gauge(gauge)) => Some(**gauge),
                _ => None,
            }
        })
    }

    #[tokio::test]
    async fn test_cache_operations() {
        let mut cache = KmsCache::new(100);

        // Test key metadata caching
        let metadata = KeyMetadata {
            key_id: "test-key-1".to_string(),
            key_state: KeyState::Enabled,
            key_usage: KeyUsage::EncryptDecrypt,
            description: Some("Test key".to_string()),
            creation_date: Zoned::now(),
            deletion_date: None,
            origin: "KMS".to_string(),
            key_manager: "CUSTOMER".to_string(),
            tags: std::collections::HashMap::new(),
        };

        // Put and get metadata
        cache.put_key_metadata("test-key-1", &metadata).await;
        let retrieved = cache.get_key_metadata("test-key-1").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.expect("metadata should be cached").key_id, "test-key-1");

        // Test cache info
        assert_eq!(cache.stats().entries, 1);

        // Test cache clearing
        cache.clear().await;
        assert_eq!(cache.stats().entries, 0);
    }

    #[tokio::test]
    async fn test_cache_with_custom_ttl() {
        let mut cache = KmsCache::with_ttl(
            100,
            Duration::from_millis(100), // Short TTL for testing
        );

        let metadata = KeyMetadata {
            key_id: "ttl-test-key".to_string(),
            key_state: KeyState::Enabled,
            key_usage: KeyUsage::EncryptDecrypt,
            description: Some("TTL test key".to_string()),
            creation_date: Zoned::now(),
            deletion_date: None,
            origin: "KMS".to_string(),
            key_manager: "CUSTOMER".to_string(),
            tags: std::collections::HashMap::new(),
        };

        cache.put_key_metadata("ttl-test-key", &metadata).await;

        // Should be present immediately
        assert!(cache.get_key_metadata("ttl-test-key").await.is_some());

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should be expired now
        assert!(cache.get_key_metadata("ttl-test-key").await.is_none());
    }

    #[tokio::test]
    async fn test_cache_contains_methods() {
        let mut cache = KmsCache::new(100);

        assert!(!cache.contains_key_metadata_for_tests("nonexistent"));

        let metadata = KeyMetadata {
            key_id: "contains-test".to_string(),
            key_state: KeyState::Enabled,
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            creation_date: Zoned::now(),
            deletion_date: None,
            origin: "KMS".to_string(),
            key_manager: "CUSTOMER".to_string(),
            tags: std::collections::HashMap::new(),
        };

        cache.put_key_metadata("contains-test", &metadata).await;

        assert!(cache.contains_key_metadata_for_tests("contains-test"));
    }

    #[test]
    fn lookups_report_real_hit_and_miss_counts() {
        let (stats, snapshot) = record_metrics(|| async {
            let mut cache = KmsCache::new(100);

            assert!(cache.get_key_metadata("absent").await.is_none());
            cache.put_key_metadata("present", &test_metadata("present")).await;
            assert!(cache.get_key_metadata("present").await.is_some());
            assert!(cache.get_key_metadata("absent").await.is_none());

            cache.stats()
        });

        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 2);
        assert_eq!(counter_value(&snapshot, METRIC_CACHE_LOOKUPS_TOTAL, &[("result", "hit")]), 1);
        assert_eq!(counter_value(&snapshot, METRIC_CACHE_LOOKUPS_TOTAL, &[("result", "miss")]), 2);
    }

    #[test]
    fn removals_report_their_cause_and_the_resulting_entry_count() {
        let (stats, snapshot) = record_metrics(|| async {
            let mut cache = KmsCache::new(100);

            cache.put_key_metadata("key", &test_metadata("key")).await;
            cache.put_key_metadata("key", &test_metadata("key")).await;
            cache.remove_key_metadata("key").await;

            cache.stats()
        });

        assert_eq!(counter_value(&snapshot, METRIC_CACHE_EVICTIONS_TOTAL, &[("cause", "replaced")]), 1);
        assert_eq!(counter_value(&snapshot, METRIC_CACHE_EVICTIONS_TOTAL, &[("cause", "explicit")]), 1);
        assert_eq!(stats.evictions, 2);
        assert_eq!(stats.entries, 0);
        assert_eq!(gauge_value(&snapshot, METRIC_CACHE_ENTRIES), Some(0.0));
    }

    #[test]
    fn capacity_pressure_reports_size_evictions() {
        let (stats, snapshot) = record_metrics(|| async {
            let mut cache = KmsCache::with_ttl(1, DEFAULT_METADATA_TTL);

            cache.put_key_metadata("first", &test_metadata("first")).await;
            cache.put_key_metadata("second", &test_metadata("second")).await;

            cache.stats()
        });

        assert_eq!(counter_value(&snapshot, METRIC_CACHE_EVICTIONS_TOTAL, &[("cause", "size")]), 1);
        assert_eq!(stats.evictions, 1);
        assert_eq!(stats.entries, 1);
        assert_eq!(gauge_value(&snapshot, METRIC_CACHE_ENTRIES), Some(1.0));
    }
}
