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

//! Adaptive TTL metrics and recording functions.
//!
//! This module provides metrics recording for adaptive TTL adjustments
//! and access tracking for cache items.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Record TTL adjustment.
///
/// # Arguments
///
/// * `key` - Cache key
/// * `base_ttl` - Base TTL in seconds
/// * `adjusted_ttl` - Adjusted TTL in seconds
#[inline(always)]
pub fn record_ttl_adjustment(_key: &str, base_ttl: u64, adjusted_ttl: u64) {
    use metrics::{counter, gauge};

    counter!("rustfs.cache.ttl.adjustments").increment(1);
    gauge!("rustfs.cache.ttl.base").set(base_ttl as f64);
    gauge!("rustfs.cache.ttl.adjusted").set(adjusted_ttl as f64);

    if adjusted_ttl > base_ttl {
        counter!("rustfs.cache.ttl.extensions").increment(1);
    } else if adjusted_ttl < base_ttl {
        counter!("rustfs.cache.ttl.reductions").increment(1);
    }
}

/// Record TTL expiration.
#[inline(always)]
pub fn record_ttl_expiration() {
    use metrics::counter;
    counter!("rustfs.cache.ttl.expirations").increment(1);
}

/// Record early eviction.
///
/// # Arguments
///
/// * `reason` - Reason for early eviction
#[inline(always)]
pub fn record_early_eviction(reason: &str) {
    use metrics::counter;
    counter!("rustfs.cache.evictions.early", "reason" => reason.to_string()).increment(1);
}

/// Record access pattern change.
///
/// # Arguments
///
/// * `from` - Previous pattern
/// * `to` - New pattern
#[inline(always)]
pub fn record_access_pattern_change(from: &str, to: &str) {
    use metrics::counter;
    counter!("rustfs.cache.access_pattern.changes", "from" => from.to_string(), "to" => to.to_string()).increment(1);
}

/// Adaptive TTL statistics.
#[derive(Debug, Clone, Default)]
pub struct AdaptiveTTLStats {
    /// Number of TTL adjustments.
    pub adjustments: u64,
    /// Number of TTL extensions.
    pub extensions: u64,
    /// Number of TTL reductions.
    pub reductions: u64,
    /// Number of TTL expirations.
    pub expirations: u64,
    /// Number of early evictions.
    pub early_evictions: u64,
}

impl AdaptiveTTLStats {
    /// Create new statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an adjustment.
    pub fn record_adjustment(&mut self, base_ttl: u64, adjusted_ttl: u64) {
        self.adjustments += 1;
        if adjusted_ttl > base_ttl {
            self.extensions += 1;
        } else if adjusted_ttl < base_ttl {
            self.reductions += 1;
        }
    }

    /// Record an expiration.
    pub fn record_expiration(&mut self) {
        self.expirations += 1;
    }

    /// Record an early eviction.
    pub fn record_early_eviction(&mut self) {
        self.early_evictions += 1;
    }

    /// Get extension rate.
    pub fn extension_rate(&self) -> f64 {
        if self.adjustments == 0 {
            0.0
        } else {
            self.extensions as f64 / self.adjustments as f64
        }
    }

    /// Get reduction rate.
    pub fn reduction_rate(&self) -> f64 {
        if self.adjustments == 0 {
            0.0
        } else {
            self.reductions as f64 / self.adjustments as f64
        }
    }

    /// Reset statistics.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

// ============================================================================
// Access Tracker
// ============================================================================

/// Access record for a cache item.
#[derive(Debug, Clone)]
pub struct AccessRecord {
    /// Number of accesses.
    pub count: u64,
    /// Last access time.
    pub last_access: Instant,
    /// First access time.
    pub first_access: Instant,
    /// Total size of accesses.
    pub total_size: u64,
}

impl AccessRecord {
    /// Create a new access record.
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            count: 1,
            last_access: now,
            first_access: now,
            total_size: 0,
        }
    }

    /// Record an access.
    pub fn record_access(&mut self, size: u64) {
        self.count += 1;
        self.last_access = Instant::now();
        self.total_size += size;
    }

    /// Get access frequency (accesses per second).
    pub fn frequency(&self) -> f64 {
        let elapsed = self.first_access.elapsed().as_secs_f64();
        if elapsed > 0.0 { self.count as f64 / elapsed } else { 0.0 }
    }

    /// Get time since last access.
    pub fn idle_time(&self) -> Duration {
        self.last_access.elapsed()
    }
}

impl Default for AccessRecord {
    fn default() -> Self {
        Self::new()
    }
}

/// Access tracker for cache items.
#[derive(Debug, Clone)]
pub struct AccessTracker {
    /// Access records by key.
    records: HashMap<String, AccessRecord>,
    /// Maximum number of tracked items.
    max_items: usize,
    /// Access window for frequency calculation.
    window: Duration,
}

impl AccessTracker {
    /// Create a new access tracker.
    pub fn new(max_items: usize, window: Duration) -> Self {
        Self {
            records: HashMap::with_capacity(max_items),
            max_items,
            window,
        }
    }

    /// Create with default settings.
    pub fn with_defaults() -> Self {
        Self::new(10_000, Duration::from_secs(60))
    }

    /// Record an access to a key.
    pub fn record_access(&mut self, key: &str, size: u64) {
        if let Some(record) = self.records.get_mut(key) {
            record.record_access(size);
        } else {
            if self.records.len() >= self.max_items {
                // Evict oldest entry
                self.evict_oldest();
            }
            let mut record = AccessRecord::new();
            record.total_size = size;
            self.records.insert(key.to_string(), record);
        }
    }

    /// Get access count for a key.
    pub fn get_access_count(&self, key: &str) -> u64 {
        self.records.get(key).map_or(0, |r| r.count)
    }

    /// Get access record for a key.
    pub fn get_record(&self, key: &str) -> Option<&AccessRecord> {
        self.records.get(key)
    }

    /// Check if a key is "hot" (high access frequency).
    pub fn is_hot(&self, key: &str, threshold: u64) -> bool {
        self.records.get(key).is_some_and(|r| r.count >= threshold)
    }

    /// Check if a key is "cold" (low access frequency).
    pub fn is_cold(&self, key: &str, threshold: u64) -> bool {
        self.records.get(key).is_none_or(|r| r.count <= threshold)
    }

    /// Get keys sorted by access count (descending).
    pub fn top_keys(&self, n: usize) -> Vec<(&String, &AccessRecord)> {
        let mut entries: Vec<_> = self.records.iter().collect();
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.1.count));
        entries.into_iter().take(n).collect()
    }

    /// Remove old entries outside the window.
    pub fn prune(&mut self) {
        let now = Instant::now();
        self.records
            .retain(|_, record| now.duration_since(record.last_access) < self.window);
    }

    /// Evict the oldest entry.
    fn evict_oldest(&mut self) {
        let oldest = self.records.iter().min_by_key(|(_, r)| r.last_access).map(|(k, _)| k.clone());

        if let Some(key) = oldest {
            self.records.remove(&key);
        }
    }

    /// Get total number of tracked items.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Clear all records.
    pub fn clear(&mut self) {
        self.records.clear();
    }

    /// Get total access count across all items.
    pub fn total_accesses(&self) -> u64 {
        self.records.values().map(|r| r.count).sum()
    }

    /// Get average access count.
    pub fn avg_access_count(&self) -> f64 {
        if self.records.is_empty() {
            0.0
        } else {
            self.total_accesses() as f64 / self.records.len() as f64
        }
    }
}

impl Default for AccessTracker {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptive_ttl_stats() {
        let mut stats = AdaptiveTTLStats::new();

        stats.record_adjustment(100, 150); // Extension
        stats.record_adjustment(100, 50); // Reduction
        stats.record_adjustment(100, 100); // No change
        stats.record_expiration();
        stats.record_early_eviction();

        assert_eq!(stats.adjustments, 3);
        assert_eq!(stats.extensions, 1);
        assert_eq!(stats.reductions, 1);
        assert_eq!(stats.expirations, 1);
        assert_eq!(stats.early_evictions, 1);

        assert!((stats.extension_rate() - 0.3333333333333333).abs() < 0.01);
        assert!((stats.reduction_rate() - 0.3333333333333333).abs() < 0.01);
    }

    #[test]
    fn test_record_ttl_adjustment() {
        // This test verifies the function compiles and runs
        record_ttl_adjustment("test-key", 100, 150);
        record_ttl_adjustment("test-key", 100, 50);
    }

    #[test]
    fn test_record_ttl_expiration() {
        record_ttl_expiration();
    }

    #[test]
    fn test_record_early_eviction() {
        record_early_eviction("cold");
        record_early_eviction("low_priority");
    }

    #[test]
    fn test_record_access_pattern_change() {
        record_access_pattern_change("sequential", "random");
        record_access_pattern_change("random", "sequential");
    }

    #[test]
    fn test_access_record() {
        let mut record = AccessRecord::new();
        assert_eq!(record.count, 1);

        record.record_access(1024);
        record.record_access(2048);

        assert_eq!(record.count, 3);
        assert_eq!(record.total_size, 3072);
    }

    #[test]
    fn test_access_tracker() {
        let mut tracker = AccessTracker::new(100, Duration::from_secs(60));

        tracker.record_access("key1", 1024);
        tracker.record_access("key1", 1024);
        tracker.record_access("key2", 2048);

        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.get_access_count("key1"), 2);
        assert_eq!(tracker.get_access_count("key2"), 1);
        assert_eq!(tracker.total_accesses(), 3);
    }

    #[test]
    fn test_access_tracker_hot_cold() {
        let mut tracker = AccessTracker::with_defaults();

        // Make key1 hot
        for _ in 0..10 {
            tracker.record_access("key1", 1024);
        }
        tracker.record_access("key2", 1024);

        assert!(tracker.is_hot("key1", 5));
        assert!(!tracker.is_hot("key2", 5));
        assert!(tracker.is_cold("key2", 1));
    }

    #[test]
    fn test_access_tracker_top_keys() {
        let mut tracker = AccessTracker::with_defaults();

        for _ in 0..10 {
            tracker.record_access("key1", 1024);
        }
        for _ in 0..5 {
            tracker.record_access("key2", 1024);
        }
        tracker.record_access("key3", 1024);

        let top = tracker.top_keys(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, "key1");
        assert_eq!(top[1].0, "key2");
    }

    #[test]
    fn test_access_tracker_clear() {
        let mut tracker = AccessTracker::with_defaults();

        tracker.record_access("key1", 1024);
        tracker.record_access("key2", 1024);

        assert_eq!(tracker.len(), 2);
        tracker.clear();
        assert!(tracker.is_empty());
    }
}
