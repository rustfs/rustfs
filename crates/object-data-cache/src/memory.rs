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
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use sysinfo::System;

const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Source used to resolve the effective memory limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryBasis {
    /// Limits come from host memory reported by sysinfo.
    Host,
    /// Limits come from a constraining cgroup (container) memory limit.
    Cgroup,
}

impl MemoryBasis {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Cgroup => "cgroup",
        }
    }
}

/// Effective memory totals after reconciling host memory with cgroup limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EffectiveMemory {
    /// Effective total memory in bytes.
    pub(crate) total_bytes: u64,
    /// Effective available memory in bytes.
    pub(crate) available_bytes: u64,
    /// Whether the limits are host- or cgroup-derived.
    pub(crate) basis: MemoryBasis,
}

/// Reconciles host memory with an optional cgroup limit.
///
/// `cgroup` carries `(total_memory, free_memory)` as reported for the cgroup
/// hierarchy (sysinfo already caps `total_memory` at the host total). A cgroup
/// only counts when it actually constrains below the host, so an unlimited
/// cgroup transparently falls back to host values.
pub(crate) fn select_effective_memory(host_total: u64, host_available: u64, cgroup: Option<(u64, u64)>) -> EffectiveMemory {
    match cgroup {
        Some((cgroup_total, cgroup_free)) if cgroup_total > 0 && cgroup_total < host_total => EffectiveMemory {
            total_bytes: cgroup_total,
            available_bytes: cgroup_free.min(cgroup_total),
            basis: MemoryBasis::Cgroup,
        },
        _ => EffectiveMemory {
            total_bytes: host_total,
            available_bytes: host_available,
            basis: MemoryBasis::Host,
        },
    }
}

/// Resolves the effective memory from an already-refreshed system handle.
///
/// `cgroup_limits()` is computed fresh on each call and is only implemented on
/// Linux (it returns `None` elsewhere), so non-Linux hosts always use host
/// values.
pub(crate) fn effective_memory_from_system(system: &System) -> EffectiveMemory {
    let cgroup = system.cgroup_limits().map(|limits| (limits.total_memory, limits.free_memory));
    select_effective_memory(system.total_memory(), system.available_memory(), cgroup)
}

/// Resolves the effective memory using a fresh, memory-refreshed system handle.
pub(crate) fn resolve_effective_memory() -> EffectiveMemory {
    let mut system = System::new();
    system.refresh_memory();
    effective_memory_from_system(&system)
}

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

/// Lock-free memory snapshot shared between the gate and its refresher task.
#[derive(Debug)]
struct MemorySnapshotCell {
    total_bytes: AtomicU64,
    available_bytes: AtomicU64,
}

impl MemorySnapshotCell {
    fn new(snapshot: ObjectDataCacheMemorySnapshot) -> Self {
        Self {
            total_bytes: AtomicU64::new(snapshot.total_bytes),
            available_bytes: AtomicU64::new(snapshot.available_bytes),
        }
    }

    fn store(&self, snapshot: ObjectDataCacheMemorySnapshot) {
        self.total_bytes.store(snapshot.total_bytes, Ordering::Relaxed);
        self.available_bytes.store(snapshot.available_bytes, Ordering::Relaxed);
    }

    fn load(&self) -> ObjectDataCacheMemorySnapshot {
        ObjectDataCacheMemorySnapshot {
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            available_bytes: self.available_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Aborts the periodic refresher when the gate (and thus the cache) is dropped.
#[derive(Debug)]
struct RefresherGuard(tokio::task::JoinHandle<()>);

impl Drop for RefresherGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Memory gate that keeps a cheap, lock-free snapshot for fill-path checks.
///
/// The snapshot is sampled off the fill path by a dedicated periodic refresher
/// (the private `spawn_refresher` task) that
/// runs the blocking `sysinfo` read on a `spawn_blocking` thread. `allows_fill`
/// only reads atomics, so it never blocks a tokio worker and concurrent fills
/// never serialize on a refresh.
#[derive(Debug)]
pub struct ObjectDataCacheMemoryGate {
    snapshot: Arc<MemorySnapshotCell>,
    min_free_memory_percent: u8,
    stats: Arc<ObjectDataCacheStats>,
    /// Kept alive so the refresher runs for the gate's lifetime; `None` when the
    /// gate is opted out (`min_free_memory_percent == 0`) or no tokio runtime is
    /// available at construction (e.g. synchronous unit tests).
    _refresher: Option<RefresherGuard>,
    #[cfg(test)]
    test_override: Mutex<Option<ObjectDataCacheMemorySnapshot>>,
}

impl ObjectDataCacheMemoryGate {
    /// Creates a new memory gate.
    pub fn new(config: &ObjectDataCacheConfig, stats: Arc<ObjectDataCacheStats>) -> Self {
        // Seed the snapshot once at construction. This is the only blocking
        // sysinfo read on any caller's stack; all later refreshes run off-path.
        let effective = resolve_effective_memory();
        let snapshot = Arc::new(MemorySnapshotCell::new(ObjectDataCacheMemorySnapshot {
            total_bytes: effective.total_bytes,
            available_bytes: effective.available_bytes,
        }));

        let min_free_memory_percent = config.min_free_memory_percent;
        // A zero floor opts out of the gate entirely, so there is nothing to
        // refresh; skip the background task in that case.
        let refresher = if min_free_memory_percent == 0 {
            None
        } else {
            Self::spawn_refresher(Arc::clone(&snapshot), DEFAULT_REFRESH_INTERVAL)
        };

        Self {
            snapshot,
            min_free_memory_percent,
            stats,
            _refresher: refresher,
            #[cfg(test)]
            test_override: Mutex::new(None),
        }
    }

    /// Spawns the periodic refresher, returning `None` when no tokio runtime is
    /// available (the gate then keeps its construction-time seed snapshot).
    fn spawn_refresher(snapshot: Arc<MemorySnapshotCell>, interval: Duration) -> Option<RefresherGuard> {
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let task = handle.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick fires immediately; the seed snapshot is already in
            // place, so refresh from the second tick onward.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                // Run the blocking /proc/meminfo read off the async worker. A
                // join error only happens if the runtime is shutting down; keep
                // the last snapshot rather than clobbering it in that case.
                if let Ok(effective) = tokio::task::spawn_blocking(resolve_effective_memory).await {
                    snapshot.store(ObjectDataCacheMemorySnapshot {
                        total_bytes: effective.total_bytes,
                        available_bytes: effective.available_bytes,
                    });
                }
            }
        });
        Some(RefresherGuard(task))
    }

    /// Returns the current atomic memory snapshot.
    pub fn snapshot(&self) -> ObjectDataCacheMemorySnapshot {
        #[cfg(test)]
        {
            if let Some(snapshot) = *lock_or_recover(&self.test_override) {
                return snapshot;
            }
        }

        self.snapshot.load()
    }

    /// Returns true when the fill path may proceed under current memory pressure.
    ///
    /// This is lock-free and does no blocking sysinfo read: it only reads the
    /// atomic snapshot maintained by the periodic refresher.
    pub fn allows_fill(&self, required_bytes: u64) -> bool {
        // A zero floor opts out of the gate, so fill admission never depends on
        // a live memory reading — which differs between a host and a container.
        // This must short-circuit before any snapshot read (see 51a97a81c).
        if self.min_free_memory_percent == 0 {
            return true;
        }

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

    /// Writes the atomic snapshot directly, bypassing the `test_override` read
    /// path so a test can observe whether `allows_fill` mutates the snapshot.
    #[cfg(test)]
    fn store_raw_snapshot_for_test(&self, snapshot: ObjectDataCacheMemorySnapshot) {
        self.snapshot.store(snapshot);
    }

    /// Reads the atomic snapshot directly, bypassing `test_override`.
    #[cfg(test)]
    fn raw_snapshot_for_test(&self) -> ObjectDataCacheMemorySnapshot {
        self.snapshot.load()
    }
}

#[cfg(test)]
fn lock_or_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::{MemoryBasis, ObjectDataCacheMemoryGate, ObjectDataCacheMemorySnapshot, select_effective_memory};
    use crate::config::ObjectDataCacheConfig;
    use crate::stats::ObjectDataCacheStats;
    use std::sync::Arc;
    use std::time::Duration;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn select_effective_memory_prefers_constraining_cgroup() {
        let effective = select_effective_memory(64 * GIB, 40 * GIB, Some((2 * GIB, GIB)));

        assert_eq!(effective.basis, MemoryBasis::Cgroup);
        assert_eq!(effective.total_bytes, 2 * GIB);
        assert_eq!(effective.available_bytes, GIB);
    }

    #[test]
    fn select_effective_memory_ignores_non_constraining_cgroup() {
        // A cgroup total equal to (or above) the host total means no real limit.
        let effective = select_effective_memory(64 * GIB, 40 * GIB, Some((64 * GIB, 10 * GIB)));

        assert_eq!(effective.basis, MemoryBasis::Host);
        assert_eq!(effective.total_bytes, 64 * GIB);
        assert_eq!(effective.available_bytes, 40 * GIB);
    }

    #[test]
    fn select_effective_memory_falls_back_to_host_without_cgroup() {
        let effective = select_effective_memory(8 * GIB, 4 * GIB, None);

        assert_eq!(effective.basis, MemoryBasis::Host);
        assert_eq!(effective.total_bytes, 8 * GIB);
        assert_eq!(effective.available_bytes, 4 * GIB);
    }

    #[test]
    fn select_effective_memory_caps_available_at_total() {
        let effective = select_effective_memory(64 * GIB, 40 * GIB, Some((2 * GIB, 3 * GIB)));

        assert_eq!(effective.total_bytes, 2 * GIB);
        assert_eq!(effective.available_bytes, 2 * GIB);
    }

    #[test]
    fn gate_pauses_fill_when_container_memory_is_low() {
        // Simulate a pod-sized snapshot (256 MiB total, 16 MiB free): below the
        // default 20% free-memory floor, so fills must pause even though a node
        // would have plenty of headroom.
        let stats = Arc::new(ObjectDataCacheStats::default());
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::clone(&stats));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 256 * 1024 * 1024,
            available_bytes: 16 * 1024 * 1024,
        }));

        assert!(!gate.allows_fill(1024));
        assert_eq!(stats.snapshot().memory_pressure_events, 1);
    }

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
    fn zero_min_free_percent_disables_the_gate() {
        // A pod-sized snapshot far below any floor: the gate is opted out, so
        // fill admission stays independent of the live memory reading.
        let stats = Arc::new(ObjectDataCacheStats::default());
        let gate = ObjectDataCacheMemoryGate::new(
            &ObjectDataCacheConfig {
                min_free_memory_percent: 0,
                ..ObjectDataCacheConfig::default()
            },
            Arc::clone(&stats),
        );
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 1,
        }));

        assert!(gate.allows_fill(512));
        assert_eq!(stats.snapshot().memory_pressure_events, 0);
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

    // ODC-14: `allows_fill` must read the atomic snapshot without performing an
    // inline (blocking) refresh. A synchronous test has no tokio runtime, so no
    // refresher task exists; if `allows_fill` refreshed inline it would clobber
    // the seeded atomic snapshot with the real host reading. Comparing exact
    // byte values makes the assertion independent of the host's actual memory.
    #[test]
    fn allows_fill_reads_snapshot_without_refreshing_inline() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::clone(&stats));

        let sentinel = ObjectDataCacheMemorySnapshot {
            total_bytes: 4_242,
            available_bytes: 2_121,
        };
        gate.store_raw_snapshot_for_test(sentinel);

        // Exercise the gate on the atomic path (no test_override installed).
        let _ = gate.allows_fill(1);

        let after = gate.raw_snapshot_for_test();
        assert_eq!(
            after, sentinel,
            "allows_fill must not mutate the snapshot; an inline refresh would overwrite it"
        );
    }

    // ODC-14: the periodic refresher samples memory off the fill path and
    // updates the atomic snapshot, so a stale seed is replaced without any
    // fill ever blocking on a refresh.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn periodic_refresher_updates_snapshot_off_path() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let snapshot = Arc::new(super::MemorySnapshotCell::new(ObjectDataCacheMemorySnapshot {
            total_bytes: 1,
            available_bytes: 1,
        }));
        let _guard = ObjectDataCacheMemoryGate::spawn_refresher(Arc::clone(&snapshot), Duration::from_millis(20))
            .expect("a tokio runtime is available in an async test");

        // The seed is a bogus 1/1; wait for the refresher to overwrite it with a
        // real host reading (total memory is always far above 1 byte).
        let mut refreshed = false;
        for _ in 0..200 {
            if snapshot.load().total_bytes > 1 {
                refreshed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(refreshed, "the periodic refresher must replace the seed snapshot off the fill path");
        let _ = stats;
    }
}
