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

use std::sync::atomic::{AtomicU64, Ordering};

/// Cache statistics holder for the engine skeleton.
#[derive(Debug, Default)]
pub struct ObjectDataCacheStats {
    entries: AtomicU64,
    lookups: AtomicU64,
    hits: AtomicU64,
    fills: AtomicU64,
    invalidations: AtomicU64,
    inflight_fills: AtomicU64,
    singleflight_joins: AtomicU64,
    memory_pressure_events: AtomicU64,
}

/// Immutable snapshot of cache statistics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectDataCacheStatsSnapshot {
    /// Number of cached entries.
    pub entries: u64,
    /// Total lookup attempts.
    pub lookups: u64,
    /// Total cache hits.
    pub hits: u64,
    /// Total fill attempts.
    pub fills: u64,
    /// Total invalidation attempts.
    pub invalidations: u64,
    /// Current number of in-flight fills.
    pub inflight_fills: u64,
    /// Number of singleflight joiners.
    pub singleflight_joins: u64,
    /// Number of times fill was skipped under memory pressure.
    pub memory_pressure_events: u64,
}

impl ObjectDataCacheStats {
    /// Updates the current entry count snapshot.
    pub fn set_entries(&self, entries: u64) {
        self.entries.store(entries, Ordering::Relaxed);
    }

    /// Records a cache lookup attempt.
    pub fn record_lookup(&self, hit: bool) {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a cache fill attempt.
    pub fn record_fill(&self) {
        self.fills.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a cache invalidation attempt.
    pub fn record_invalidation(&self) {
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    /// Sets the current number of in-flight fills.
    pub fn set_inflight_fills(&self, inflight_fills: usize) {
        let value = u64::try_from(inflight_fills).unwrap_or(u64::MAX);
        self.inflight_fills.store(value, Ordering::Relaxed);
    }

    /// Records a singleflight join.
    pub fn record_singleflight_join(&self) {
        self.singleflight_joins.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a memory pressure event.
    pub fn record_memory_pressure(&self) {
        self.memory_pressure_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the current immutable stats snapshot.
    pub fn snapshot(&self) -> ObjectDataCacheStatsSnapshot {
        ObjectDataCacheStatsSnapshot {
            entries: self.entries.load(Ordering::Relaxed),
            lookups: self.lookups.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            fills: self.fills.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            inflight_fills: self.inflight_fills.load(Ordering::Relaxed),
            singleflight_joins: self.singleflight_joins.load(Ordering::Relaxed),
            memory_pressure_events: self.memory_pressure_events.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectDataCacheStats;

    #[test]
    fn snapshot_reflects_recorded_counters() {
        let stats = ObjectDataCacheStats::default();
        stats.set_entries(3);
        stats.record_lookup(true);
        stats.record_lookup(false);
        stats.record_fill();
        stats.record_invalidation();
        stats.set_inflight_fills(2);
        stats.record_singleflight_join();
        stats.record_memory_pressure();

        let snapshot = stats.snapshot();

        assert_eq!(snapshot.entries, 3);
        assert_eq!(snapshot.lookups, 2);
        assert_eq!(snapshot.hits, 1);
        assert_eq!(snapshot.fills, 1);
        assert_eq!(snapshot.invalidations, 1);
        assert_eq!(snapshot.inflight_fills, 2);
        assert_eq!(snapshot.singleflight_joins, 1);
        assert_eq!(snapshot.memory_pressure_events, 1);
    }
}
