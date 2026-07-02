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

use crate::config::ObjectDataCacheConfig;
use crate::metrics::record_memory_pressure;
use crate::stats::ObjectDataCacheStats;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use sysinfo::System;

const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Immutable memory snapshot used by the cache fill gate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectDataCacheMemorySnapshot {
    /// Total system memory in bytes.
    pub total_bytes: u64,
    /// Available system memory in bytes.
    pub available_bytes: u64,
}

impl ObjectDataCacheMemorySnapshot {
    /// Returns the available memory percentage.
    pub fn available_percent(&self) -> u8 {
        if self.total_bytes == 0 {
            return 0;
        }

        let percent = self.available_bytes.saturating_mul(100) / self.total_bytes;
        u8::try_from(percent.min(100)).unwrap_or(100)
    }
}

/// Memory gate that keeps a cheap snapshot for fill-path checks.
#[derive(Debug)]
pub struct ObjectDataCacheMemoryGate {
    system: Mutex<System>,
    last_refresh: Mutex<Instant>,
    snapshot_total_bytes: AtomicU64,
    snapshot_available_bytes: AtomicU64,
    min_free_memory_percent: u8,
    refresh_interval: Duration,
    stats: Arc<ObjectDataCacheStats>,
    #[cfg(test)]
    test_override: Mutex<Option<ObjectDataCacheMemorySnapshot>>,
}

impl ObjectDataCacheMemoryGate {
    /// Creates a new memory gate.
    pub fn new(config: &ObjectDataCacheConfig, stats: Arc<ObjectDataCacheStats>) -> Self {
        let mut system = System::new();
        system.refresh_memory();
        let snapshot = ObjectDataCacheMemorySnapshot {
            total_bytes: system.total_memory(),
            available_bytes: system.available_memory(),
        };

        Self {
            system: Mutex::new(system),
            last_refresh: Mutex::new(Instant::now()),
            snapshot_total_bytes: AtomicU64::new(snapshot.total_bytes),
            snapshot_available_bytes: AtomicU64::new(snapshot.available_bytes),
            min_free_memory_percent: config.min_free_memory_percent,
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
            stats,
            #[cfg(test)]
            test_override: Mutex::new(None),
        }
    }

    /// Returns the current atomic memory snapshot.
    pub fn snapshot(&self) -> ObjectDataCacheMemorySnapshot {
        #[cfg(test)]
        {
            if let Some(snapshot) = *lock_or_recover(&self.test_override) {
                return snapshot;
            }
        }

        ObjectDataCacheMemorySnapshot {
            total_bytes: self.snapshot_total_bytes.load(Ordering::Relaxed),
            available_bytes: self.snapshot_available_bytes.load(Ordering::Relaxed),
        }
    }

    /// Refreshes the snapshot if it is stale.
    pub fn refresh_if_stale(&self) {
        {
            let last_refresh = lock_or_recover(&self.last_refresh);
            if last_refresh.elapsed() < self.refresh_interval {
                return;
            }
        }

        let mut system = lock_or_recover(&self.system);
        system.refresh_memory();
        let snapshot = ObjectDataCacheMemorySnapshot {
            total_bytes: system.total_memory(),
            available_bytes: system.available_memory(),
        };

        self.snapshot_total_bytes.store(snapshot.total_bytes, Ordering::Relaxed);
        self.snapshot_available_bytes
            .store(snapshot.available_bytes, Ordering::Relaxed);
        *lock_or_recover(&self.last_refresh) = Instant::now();
    }

    /// Returns true when the fill path may proceed under current memory pressure.
    pub fn allows_fill(&self, required_bytes: u64) -> bool {
        self.refresh_if_stale();
        let snapshot = self.snapshot();
        if snapshot.total_bytes == 0 {
            return true;
        }

        let min_free = u64::from(self.min_free_memory_percent);
        let has_percent_budget = snapshot.available_bytes.saturating_mul(100) >= snapshot.total_bytes.saturating_mul(min_free);
        let has_entry_budget = snapshot.available_bytes >= required_bytes;
        let allowed = has_percent_budget && has_entry_budget;

        if !allowed {
            record_memory_pressure(&self.stats, "moka");
        }

        allowed
    }

    #[cfg(test)]
    pub fn set_test_snapshot(&self, snapshot: Option<ObjectDataCacheMemorySnapshot>) {
        *lock_or_recover(&self.test_override) = snapshot;
    }
}

fn lock_or_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::{ObjectDataCacheMemoryGate, ObjectDataCacheMemorySnapshot};
    use crate::config::ObjectDataCacheConfig;
    use crate::stats::ObjectDataCacheStats;
    use std::sync::Arc;

    #[test]
    fn allows_fill_when_memory_snapshot_has_headroom() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::clone(&stats));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        }));

        assert!(gate.allows_fill(100));
    }

    #[test]
    fn blocks_fill_under_memory_pressure() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::clone(&stats));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 100,
        }));

        assert!(!gate.allows_fill(128));
        assert_eq!(stats.snapshot().memory_pressure_events, 1);
    }
}
