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
use bytes::Bytes;
use std::hint::spin_loop;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use sysinfo::System;

const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_TELEMETRY_STALENESS: Duration = Duration::from_secs(15);
// Each writer holds the sequence for only a few atomic loads/stores. Eight
// attempts cover brief overlap without prolonged spinning on a Tokio worker;
// exhaustion safely skips only the cache fill.
const MAX_STATE_RETRIES: usize = 8;

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
    /// Even values identify a stable state; an odd value means a writer owns
    /// the short atomic update section.
    sequence: AtomicU64,
    total_bytes: AtomicU64,
    available_bytes: AtomicU64,
    /// Monotonic ordering only; unlike the reservation counters this never
    /// decreases or resets at a telemetry boundary.
    issued_sequence: AtomicU64,
    /// Token-bound claims. A refresh never changes this value, so a token from
    /// an old epoch cannot subtract a newer epoch's claim when it is dropped.
    live_reserved: AtomicU64,
    /// Releases that collided with the bounded state writer. Both the releasing
    /// thread and the active writer attempt a bounded drain, so Drop never waits
    /// indefinitely and a release cannot be lost when a writer is preempted.
    pending_release: AtomicU64,
    /// Permanently fails admission closed if release accounting violates its
    /// internal bounds instead of risking an under-count.
    release_accounting_failed: AtomicBool,
    /// Already-materialized buffered allocations without an attachable owner.
    /// A sample absorbs only the baseline captured before its blocking read.
    sampled_reserved: AtomicU64,
    clock_origin: Instant,
    last_refresh_millis: AtomicU64,
    #[cfg(test)]
    test_now_millis: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct MemorySampleBaseline {
    issued_sequence: u64,
    sampled_reserved: u64,
}

impl MemorySnapshotCell {
    fn new(snapshot: ObjectDataCacheMemorySnapshot) -> Self {
        let available_bytes = snapshot.available_bytes.min(snapshot.total_bytes);
        Self {
            sequence: AtomicU64::new(0),
            total_bytes: AtomicU64::new(snapshot.total_bytes),
            available_bytes: AtomicU64::new(available_bytes),
            issued_sequence: AtomicU64::new(0),
            live_reserved: AtomicU64::new(0),
            pending_release: AtomicU64::new(0),
            release_accounting_failed: AtomicBool::new(false),
            sampled_reserved: AtomicU64::new(0),
            clock_origin: Instant::now(),
            last_refresh_millis: AtomicU64::new(1),
            #[cfg(test)]
            test_now_millis: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    fn store(&self, snapshot: ObjectDataCacheMemorySnapshot) {
        let Some(baseline) = self.begin_sample() else {
            return;
        };
        self.publish_sample(baseline, snapshot);
    }

    fn begin_sample(&self) -> Option<MemorySampleBaseline> {
        let writer = self.try_write()?;
        let baseline = MemorySampleBaseline {
            issued_sequence: self.issued_sequence.load(Ordering::Relaxed),
            sampled_reserved: self.sampled_reserved.load(Ordering::Relaxed),
        };
        writer.commit();
        Some(baseline)
    }

    fn publish_sample(&self, baseline: MemorySampleBaseline, snapshot: ObjectDataCacheMemorySnapshot) {
        let Some(writer) = self.try_write() else {
            return;
        };
        let issued_sequence = self.issued_sequence.load(Ordering::Relaxed);
        if issued_sequence < baseline.issued_sequence {
            return;
        }
        let sampled_reserved = self.sampled_reserved.load(Ordering::Relaxed);
        let Some(sampled_after) = sampled_reserved.checked_sub(baseline.sampled_reserved) else {
            return;
        };
        self.total_bytes.store(snapshot.total_bytes, Ordering::Relaxed);
        self.available_bytes
            .store(snapshot.available_bytes.min(snapshot.total_bytes), Ordering::Relaxed);
        self.sampled_reserved.store(sampled_after, Ordering::Relaxed);
        self.last_refresh_millis.store(self.now_millis(), Ordering::Relaxed);
        writer.commit();
    }

    fn load(&self) -> Option<ObjectDataCacheMemorySnapshot> {
        for _ in 0..MAX_STATE_RETRIES {
            let start = self.sequence.load(Ordering::Acquire);
            if start & 1 == 1 {
                spin_loop();
                continue;
            }

            let snapshot = ObjectDataCacheMemorySnapshot {
                total_bytes: self.total_bytes.load(Ordering::Relaxed),
                available_bytes: self.available_bytes.load(Ordering::Relaxed),
            };
            if self.sequence.load(Ordering::Acquire) == start {
                return Some(snapshot);
            }
        }

        None
    }

    #[cfg(test)]
    fn admitted(&self) -> u64 {
        if self.release_accounting_failed.load(Ordering::Acquire) {
            return u64::MAX;
        }
        self.live_reserved
            .load(Ordering::Relaxed)
            .saturating_sub(self.pending_release.load(Ordering::Acquire))
            .saturating_add(self.sampled_reserved.load(Ordering::Relaxed))
    }

    #[cfg(test)]
    fn reserve(&self, bytes: u64) {
        let Some(writer) = self.try_write() else {
            return;
        };
        let claimed = self.sampled_reserved.load(Ordering::Relaxed);
        self.sampled_reserved.store(claimed.saturating_add(bytes), Ordering::Relaxed);
        writer.commit();
    }

    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if self
            .pending_release
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| pending.checked_add(bytes))
            .is_err()
        {
            self.release_accounting_failed.store(true, Ordering::Release);
            return;
        }
        self.try_settle_pending_releases();
    }

    fn try_settle_pending_releases(&self) {
        if self.pending_release.load(Ordering::Acquire) == 0 {
            return;
        }
        let Some(writer) = self.try_write_inner(false) else {
            return;
        };
        writer.commit();
    }

    fn settle_pending_releases_locked(&self) {
        let pending = self.pending_release.swap(0, Ordering::AcqRel);
        if pending == 0 {
            return;
        }
        let claimed = self.live_reserved.load(Ordering::Relaxed);
        if let Some(remaining) = claimed.checked_sub(pending) {
            self.live_reserved.store(remaining, Ordering::Relaxed);
        } else {
            self.live_reserved.store(u64::MAX, Ordering::Relaxed);
            self.release_accounting_failed.store(true, Ordering::Release);
        }
    }

    fn materialize_without_owner(&self, bytes: u64) -> bool {
        let Some(writer) = self.try_write() else {
            return false;
        };
        let live = self.live_reserved.load(Ordering::Relaxed);
        let sampled = self.sampled_reserved.load(Ordering::Relaxed);
        let Some(live_after) = live.checked_sub(bytes) else {
            return false;
        };
        let Some(sampled_after) = sampled.checked_add(bytes) else {
            return false;
        };
        self.live_reserved.store(live_after, Ordering::Relaxed);
        self.sampled_reserved.store(sampled_after, Ordering::Relaxed);
        writer.commit();
        true
    }

    fn telemetry_age(&self) -> Option<Duration> {
        let refreshed = self.last_refresh_millis.load(Ordering::Acquire);
        if refreshed == 0 {
            return None;
        }
        Some(Duration::from_millis(self.now_millis().saturating_sub(refreshed)))
    }

    fn now_millis(&self) -> u64 {
        #[cfg(test)]
        {
            let now = self.test_now_millis.load(Ordering::Relaxed);
            if now != 0 {
                return now;
            }
        }
        u64::try_from(self.clock_origin.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .saturating_add(1)
    }

    #[cfg(test)]
    fn set_test_now(&self, now: Duration) {
        self.test_now_millis
            .store(u64::try_from(now.as_millis()).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    #[cfg(test)]
    fn store_with_hook(&self, snapshot: ObjectDataCacheMemorySnapshot, after_total: impl FnOnce()) {
        let Some(writer) = self.try_write() else {
            return;
        };
        self.total_bytes.store(snapshot.total_bytes, Ordering::Relaxed);
        after_total();
        self.available_bytes
            .store(snapshot.available_bytes.min(snapshot.total_bytes), Ordering::Relaxed);
        self.last_refresh_millis.store(self.now_millis(), Ordering::Relaxed);
        writer.commit();
    }

    fn try_write(&self) -> Option<MemoryStateWriteGuard<'_>> {
        self.try_write_inner(true)
    }

    fn try_write_inner(&self, settle_pending_on_drop: bool) -> Option<MemoryStateWriteGuard<'_>> {
        for _ in 0..MAX_STATE_RETRIES {
            let sequence = self.sequence.load(Ordering::Acquire);
            if sequence & 1 == 1 {
                spin_loop();
                continue;
            }
            let next_sequence = sequence.checked_add(2)?;
            if self
                .sequence
                .compare_exchange_weak(sequence, sequence + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.settle_pending_releases_locked();
                let previous = MemoryState {
                    total_bytes: self.total_bytes.load(Ordering::Relaxed),
                    available_bytes: self.available_bytes.load(Ordering::Relaxed),
                    issued_sequence: self.issued_sequence.load(Ordering::Relaxed),
                    live_reserved: self.live_reserved.load(Ordering::Relaxed),
                    sampled_reserved: self.sampled_reserved.load(Ordering::Relaxed),
                    last_refresh_millis: self.last_refresh_millis.load(Ordering::Relaxed),
                };
                return Some(MemoryStateWriteGuard {
                    cell: self,
                    next_sequence,
                    previous,
                    committed: false,
                    settle_pending_on_drop,
                });
            }
        }

        None
    }
}

struct MemoryState {
    total_bytes: u64,
    available_bytes: u64,
    issued_sequence: u64,
    live_reserved: u64,
    sampled_reserved: u64,
    last_refresh_millis: u64,
}

struct MemoryStateWriteGuard<'a> {
    cell: &'a MemorySnapshotCell,
    next_sequence: u64,
    previous: MemoryState,
    committed: bool,
    settle_pending_on_drop: bool,
}

impl MemoryStateWriteGuard<'_> {
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for MemoryStateWriteGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.cell.total_bytes.store(self.previous.total_bytes, Ordering::Relaxed);
            self.cell
                .available_bytes
                .store(self.previous.available_bytes, Ordering::Relaxed);
            self.cell
                .issued_sequence
                .store(self.previous.issued_sequence, Ordering::Relaxed);
            self.cell.live_reserved.store(self.previous.live_reserved, Ordering::Relaxed);
            self.cell
                .sampled_reserved
                .store(self.previous.sampled_reserved, Ordering::Relaxed);
            self.cell
                .last_refresh_millis
                .store(self.previous.last_refresh_millis, Ordering::Relaxed);
        }
        self.cell.sequence.store(self.next_sequence, Ordering::Release);
        if self.settle_pending_on_drop {
            self.cell.try_settle_pending_releases();
        }
    }
}

/// An exclusive claim on memory admitted for one cache allocation.
///
/// Dropping the token releases the claim. Callers that only use the legacy
/// boolean admission API retain the claim until the next telemetry refresh.
#[derive(Debug)]
#[must_use = "dropping the reservation releases the admitted memory"]
pub struct ObjectDataCacheMemoryReservation {
    snapshot: Option<Arc<MemorySnapshotCell>>,
    bytes: u64,
}

impl ObjectDataCacheMemoryReservation {
    /// Attaches this reservation to a newly allocated body.
    ///
    /// Call this before cloning `bytes`. Every clone of the returned value then
    /// shares one allocation owner, and the reservation is released only after
    /// its last clone is dropped.
    pub fn wrap_bytes(self, bytes: Bytes) -> Bytes {
        if self.snapshot.is_none() {
            return bytes;
        }
        Bytes::from_owner(ReservedBytes {
            bytes,
            _reservation: self,
        })
    }

    pub(crate) fn until_refresh(mut self) -> bool {
        let Some(snapshot) = &self.snapshot else {
            return true;
        };
        if !snapshot.materialize_without_owner(self.bytes) {
            return false;
        }
        self.snapshot = None;
        true
    }
}

impl Drop for ObjectDataCacheMemoryReservation {
    fn drop(&mut self) {
        if let Some(snapshot) = &self.snapshot {
            snapshot.release(self.bytes);
        }
    }
}

struct ReservedBytes {
    bytes: Bytes,
    _reservation: ObjectDataCacheMemoryReservation,
}

impl AsRef<[u8]> for ReservedBytes {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
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

/// Memory gate that keeps a cheap atomic snapshot and reservation state.
///
/// The snapshot is sampled off the fill path by a dedicated periodic refresher
/// (the private `spawn_refresher` task) that
/// runs the blocking `sysinfo` read on a `spawn_blocking` thread. `allows_fill`
/// only performs bounded atomic operations, so it never blocks a tokio worker.
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
                let Some(baseline) = snapshot.begin_sample() else {
                    continue;
                };
                // Run the blocking /proc/meminfo read off the async worker. A
                // join error only happens if the runtime is shutting down; keep
                // the last snapshot rather than clobbering it in that case.
                if let Ok(effective) = tokio::task::spawn_blocking(resolve_effective_memory).await {
                    snapshot.publish_sample(
                        baseline,
                        ObjectDataCacheMemorySnapshot {
                            total_bytes: effective.total_bytes,
                            available_bytes: effective.available_bytes,
                        },
                    );
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

        self.snapshot.load().unwrap_or_default()
    }

    /// Returns the age of the last successfully published telemetry sample.
    pub fn telemetry_age(&self) -> Option<Duration> {
        self.snapshot.telemetry_age()
    }

    /// Atomically claims memory for one cache allocation.
    ///
    /// The returned token owns the claim and releases it on drop. Contention is
    /// retried a bounded number of times; exhaustion fails closed by skipping
    /// this cache fill without affecting the underlying object read.
    pub fn try_claim(&self, required_bytes: u64, cache_growth_headroom: u64) -> Option<ObjectDataCacheMemoryReservation> {
        self.try_claim_after(required_bytes, cache_growth_headroom, || {})
    }

    fn try_claim_after(
        &self,
        required_bytes: u64,
        _cache_growth_headroom: u64,
        before_claim: impl FnOnce(),
    ) -> Option<ObjectDataCacheMemoryReservation> {
        if self.min_free_memory_percent == 0 {
            return Some(ObjectDataCacheMemoryReservation {
                snapshot: None,
                bytes: 0,
            });
        }

        before_claim();
        let Some(writer) = self.snapshot.try_write() else {
            record_memory_pressure(&self.stats, "moka");
            return None;
        };
        if self.snapshot.release_accounting_failed.load(Ordering::Acquire) {
            writer.commit();
            record_memory_pressure(&self.stats, "moka");
            return None;
        }
        if self
            .snapshot
            .telemetry_age()
            .is_none_or(|age| age > DEFAULT_TELEMETRY_STALENESS)
        {
            writer.commit();
            record_memory_pressure(&self.stats, "moka");
            return None;
        }
        let snapshot = {
            #[cfg(test)]
            if let Some(snapshot) = *lock_or_recover(&self.test_override) {
                snapshot
            } else {
                ObjectDataCacheMemorySnapshot {
                    total_bytes: self.snapshot.total_bytes.load(Ordering::Relaxed),
                    available_bytes: self.snapshot.available_bytes.load(Ordering::Relaxed),
                }
            }
            #[cfg(not(test))]
            ObjectDataCacheMemorySnapshot {
                total_bytes: self.snapshot.total_bytes.load(Ordering::Relaxed),
                available_bytes: self.snapshot.available_bytes.load(Ordering::Relaxed),
            }
        };
        if snapshot.total_bytes == 0 {
            writer.commit();
            record_memory_pressure(&self.stats, "moka");
            return None;
        }
        let live_reserved = self.snapshot.live_reserved.load(Ordering::Relaxed);
        let sampled_reserved = self.snapshot.sampled_reserved.load(Ordering::Relaxed);
        let Some(claimed) = live_reserved.checked_add(sampled_reserved) else {
            writer.commit();
            record_memory_pressure(&self.stats, "moka");
            return None;
        };
        let Some(claimed_after) = claimed.checked_add(required_bytes) else {
            writer.commit();
            record_memory_pressure(&self.stats, "moka");
            return None;
        };
        let Some(available_after) = snapshot.available_bytes.checked_sub(claimed_after) else {
            writer.commit();
            record_memory_pressure(&self.stats, "moka");
            return None;
        };
        let min_free = u64::from(self.min_free_memory_percent);
        let has_percent_budget = u128::from(available_after) * 100 >= u128::from(snapshot.total_bytes) * u128::from(min_free);

        if !has_percent_budget {
            writer.commit();
            record_memory_pressure(&self.stats, "moka");
            return None;
        }

        let Some(live_after) = live_reserved.checked_add(required_bytes) else {
            writer.commit();
            record_memory_pressure(&self.stats, "moka");
            return None;
        };
        let issued_sequence = self.snapshot.issued_sequence.load(Ordering::Relaxed);
        let Some(next_issued_sequence) = issued_sequence.checked_add(1) else {
            writer.commit();
            record_memory_pressure(&self.stats, "moka");
            return None;
        };
        self.snapshot.live_reserved.store(live_after, Ordering::Relaxed);
        self.snapshot.issued_sequence.store(next_issued_sequence, Ordering::Relaxed);
        writer.commit();
        Some(ObjectDataCacheMemoryReservation {
            snapshot: Some(Arc::clone(&self.snapshot)),
            bytes: required_bytes,
        })
    }

    /// Returns true when the fill path may proceed under current memory pressure.
    ///
    /// This does no blocking sysinfo read: it uses the atomic snapshot and a
    /// bounded reservation update maintained by the periodic refresher.
    ///
    /// `cache_growth_headroom` remains for source compatibility. Reservations
    /// now follow live allocations, so cache capacity cannot cap the peak memory
    /// held by an evicted entry whose response body is still alive.
    pub fn allows_fill(&self, required_bytes: u64, cache_growth_headroom: u64) -> bool {
        self.try_claim_buffered(required_bytes, cache_growth_headroom)
    }

    pub(crate) fn try_claim_buffered(&self, required_bytes: u64, cache_growth_headroom: u64) -> bool {
        let Some(reservation) = self.try_claim(required_bytes, cache_growth_headroom) else {
            return false;
        };
        if reservation.until_refresh() {
            true
        } else {
            record_memory_pressure(&self.stats, "moka");
            false
        }
    }

    #[cfg(test)]
    pub fn set_test_snapshot(&self, snapshot: Option<ObjectDataCacheMemorySnapshot>) {
        *lock_or_recover(&self.test_override) = snapshot;
    }

    /// Writes the atomic snapshot directly, bypassing the `test_override` read
    /// path so a test can observe whether `allows_fill` mutates the snapshot.
    #[cfg(test)]
    pub(crate) fn store_raw_snapshot_for_test(&self, snapshot: ObjectDataCacheMemorySnapshot) {
        self.snapshot.store(snapshot);
    }

    /// Reads the atomic snapshot directly, bypassing `test_override`.
    #[cfg(test)]
    fn raw_snapshot_for_test(&self) -> ObjectDataCacheMemorySnapshot {
        self.snapshot.load().unwrap_or_default()
    }

    #[cfg(test)]
    fn try_claim_with_hook(
        &self,
        required_bytes: u64,
        cache_growth_headroom: u64,
        before_claim: impl FnOnce(),
    ) -> Option<ObjectDataCacheMemoryReservation> {
        self.try_claim_after(required_bytes, cache_growth_headroom, before_claim)
    }

    #[cfg(test)]
    pub(crate) fn claimed_bytes_for_test(&self) -> u64 {
        self.snapshot.admitted()
    }

    #[cfg(test)]
    fn begin_sample_for_test(&self) -> MemorySampleBaseline {
        self.snapshot
            .begin_sample()
            .expect("test sample should acquire the state writer")
    }

    #[cfg(test)]
    fn publish_sample_for_test(&self, baseline: MemorySampleBaseline, snapshot: ObjectDataCacheMemorySnapshot) {
        self.snapshot.publish_sample(baseline, snapshot);
    }

    #[cfg(test)]
    fn set_test_now(&self, now: Duration) {
        self.snapshot.set_test_now(now);
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
    use super::{
        DEFAULT_TELEMETRY_STALENESS, MemoryBasis, ObjectDataCacheMemoryGate, ObjectDataCacheMemorySnapshot,
        select_effective_memory,
    };
    use crate::config::ObjectDataCacheConfig;
    use crate::stats::ObjectDataCacheStats;
    use bytes::Bytes;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn memory_gate_concurrent_claims_are_linearizable() {
        const CLAIMERS: usize = 8;
        let gate = Arc::new(ObjectDataCacheMemoryGate::new(
            &ObjectDataCacheConfig::default(),
            Arc::new(ObjectDataCacheStats::default()),
        ));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        }));
        let ready = Arc::new(Barrier::new(CLAIMERS));

        let mut handles = Vec::with_capacity(CLAIMERS);
        for _ in 0..CLAIMERS {
            let gate = Arc::clone(&gate);
            let ready = Arc::clone(&ready);
            handles.push(thread::spawn(move || {
                gate.try_claim_with_hook(300, u64::MAX, || {
                    ready.wait();
                })
            }));
        }
        let claims: Vec<_> = handles
            .into_iter()
            .filter_map(|handle| handle.join().expect("claim thread should not panic"))
            .collect();

        assert_eq!(claims.len(), 1, "only one 300-byte claim preserves the shared floor");
        assert_eq!(gate.claimed_bytes_for_test(), 300);
        drop(claims);
        assert_eq!(gate.claimed_bytes_for_test(), 0, "dropping the owner releases its claim");
    }

    #[test]
    fn memory_reservation_token_lifecycle_is_idempotent() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        }));

        let mut claim = Some(gate.try_claim(100, u64::MAX).expect("100-byte claim should fit"));
        assert_eq!(gate.claimed_bytes_for_test(), 100);
        drop(claim.take());
        drop(claim);

        assert_eq!(gate.claimed_bytes_for_test(), 0, "the claim is released exactly once");
    }

    #[test]
    fn memory_release_debt_settles_after_active_writer_exits() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        }));
        let claim = gate
            .try_claim(300, 0)
            .expect("the initial claim should fit at the memory floor");
        let writer = gate.snapshot.try_write().expect("the test must hold the memory epoch writer");

        drop(claim);
        assert_eq!(gate.snapshot.live_reserved.load(Ordering::Relaxed), 300);
        assert_eq!(gate.snapshot.pending_release.load(Ordering::Acquire), 300);
        assert_eq!(gate.claimed_bytes_for_test(), 0, "release debt must immediately offset the dead claim");

        drop(writer);
        assert_eq!(gate.claimed_bytes_for_test(), 0, "writer exit must settle the deferred release");
        assert_eq!(gate.snapshot.pending_release.load(Ordering::Acquire), 0);
        assert!(gate.try_claim(300, 0).is_some(), "settled release debt must restore admission");
    }

    #[test]
    fn memory_release_debt_overflow_fails_admission_closed() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        }));
        gate.snapshot.pending_release.store(u64::MAX, Ordering::Release);

        gate.snapshot.release(1);

        assert!(gate.snapshot.release_accounting_failed.load(Ordering::Acquire));
        assert!(gate.try_claim(1, 0).is_none(), "overflowed release accounting must reject future fills");
    }

    #[test]
    fn memory_release_debt_exceeding_live_claim_fails_admission_closed() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        }));
        gate.snapshot.live_reserved.store(1, Ordering::Relaxed);
        gate.snapshot.pending_release.store(2, Ordering::Release);

        gate.snapshot.try_settle_pending_releases();

        assert!(gate.snapshot.release_accounting_failed.load(Ordering::Acquire));
        assert_eq!(gate.snapshot.live_reserved.load(Ordering::Relaxed), u64::MAX);
        assert!(gate.try_claim(1, 0).is_none(), "underflowed release accounting must reject future fills");
    }

    #[test]
    fn memory_gate_claimed_bytes_never_exceed_epoch_budget() {
        const CLAIMERS: usize = 32;
        let gate = Arc::new(ObjectDataCacheMemoryGate::new(
            &ObjectDataCacheConfig::default(),
            Arc::new(ObjectDataCacheStats::default()),
        ));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        }));
        let ready = Arc::new(Barrier::new(CLAIMERS));

        let mut handles = Vec::with_capacity(CLAIMERS);
        for _ in 0..CLAIMERS {
            let gate = Arc::clone(&gate);
            let ready = Arc::clone(&ready);
            handles.push(thread::spawn(move || {
                gate.try_claim_with_hook(100, u64::MAX, || {
                    ready.wait();
                })
            }));
        }
        let claims: Vec<_> = handles
            .into_iter()
            .filter_map(|handle| handle.join().expect("claim thread should not panic"))
            .collect();

        assert_eq!(gate.claimed_bytes_for_test(), u64::try_from(claims.len()).unwrap_or(u64::MAX) * 100);
        assert!(gate.claimed_bytes_for_test() <= 500);
    }

    #[test]
    fn memory_claim_recomputes_when_epoch_changes() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(None);
        gate.store_raw_snapshot_for_test(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 100,
        });

        let claim = gate.try_claim_with_hook(100, u64::MAX, || {
            gate.store_raw_snapshot_for_test(ObjectDataCacheMemorySnapshot {
                total_bytes: 1_000,
                available_bytes: 500,
            });
        });

        assert!(claim.is_some(), "claim must use the epoch published before linearization");
    }

    #[test]
    fn memory_snapshot_publish_preserves_post_sample_claim() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.store_raw_snapshot_for_test(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        });
        let sample = gate.begin_sample_for_test();
        let claim = gate.try_claim(100, 0).expect("post-sample claim should fit");
        assert!(claim.until_refresh(), "materialized claim should enter the telemetry ledger");

        gate.publish_sample_for_test(
            sample,
            ObjectDataCacheMemorySnapshot {
                total_bytes: 1_000,
                available_bytes: 500,
            },
        );

        assert_eq!(gate.claimed_bytes_for_test(), 100, "post-sample claim must carry into the new epoch");
    }

    #[test]
    fn memory_snapshot_carries_pre_sample_unmaterialized_claim() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        }));
        let claim = gate.try_claim(100, 0).expect("pre-sample claim should fit");
        let sample = gate.begin_sample_for_test();

        gate.publish_sample_for_test(
            sample,
            ObjectDataCacheMemorySnapshot {
                total_bytes: 1_000,
                available_bytes: 400,
            },
        );

        assert_eq!(
            gate.claimed_bytes_for_test(),
            100,
            "unmaterialized claim cannot be absorbed by the sample"
        );
        drop(claim);
        assert_eq!(gate.claimed_bytes_for_test(), 0);
    }

    #[test]
    fn memory_gate_telemetry_expiry_degrades_and_recovers() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(None);
        gate.set_test_now(Duration::from_secs(1));
        gate.store_raw_snapshot_for_test(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        });
        assert!(gate.try_claim(100, 0).is_some());

        gate.set_test_now(DEFAULT_TELEMETRY_STALENESS + Duration::from_secs(2));
        assert!(gate.try_claim(100, 0).is_none(), "expired telemetry must fail closed for cache fills");

        gate.store_raw_snapshot_for_test(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        });
        assert!(gate.try_claim(100, 0).is_some(), "a valid refresh must recover admission");
    }

    #[test]
    fn memory_snapshot_reader_never_observes_mixed_epoch() {
        let gate = Arc::new(ObjectDataCacheMemoryGate::new(
            &ObjectDataCacheConfig::default(),
            Arc::new(ObjectDataCacheStats::default()),
        ));
        gate.set_test_snapshot(None);
        gate.store_raw_snapshot_for_test(ObjectDataCacheMemorySnapshot {
            total_bytes: 64 * GIB,
            available_bytes: 40 * GIB,
        });
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let writer = {
            let gate = Arc::clone(&gate);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            thread::spawn(move || {
                gate.snapshot.store_with_hook(
                    ObjectDataCacheMemorySnapshot {
                        total_bytes: 2 * GIB,
                        available_bytes: 100 * 1024 * 1024,
                    },
                    || {
                        entered.wait();
                        release.wait();
                    },
                );
            })
        };

        entered.wait();
        assert_eq!(
            gate.raw_snapshot_for_test(),
            ObjectDataCacheMemorySnapshot::default(),
            "a bounded reader must fail closed while an epoch is incomplete"
        );
        release.wait();
        writer.join().expect("snapshot writer should not panic");

        assert_eq!(
            gate.raw_snapshot_for_test(),
            ObjectDataCacheMemorySnapshot {
                total_bytes: 2 * GIB,
                available_bytes: 100 * 1024 * 1024,
            }
        );
    }

    #[test]
    fn memory_snapshot_interrupted_writer_rolls_back_partial_epoch() {
        let initial = ObjectDataCacheMemorySnapshot {
            total_bytes: 64 * GIB,
            available_bytes: 40 * GIB,
        };
        let snapshot = super::MemorySnapshotCell::new(initial);

        let interrupted = catch_unwind(AssertUnwindSafe(|| {
            snapshot.store_with_hook(
                ObjectDataCacheMemorySnapshot {
                    total_bytes: 2 * GIB,
                    available_bytes: 100 * 1024 * 1024,
                },
                || panic!("interrupt writer before tuple publication"),
            );
        }));

        assert!(interrupted.is_err());
        assert_eq!(snapshot.load(), Some(initial));
    }

    #[test]
    fn memory_claim_sequence_overflow_fails_closed() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        }));
        gate.snapshot.issued_sequence.store(u64::MAX, Ordering::Relaxed);

        assert!(gate.try_claim(100, 0).is_none());
        assert_eq!(gate.claimed_bytes_for_test(), 0);
    }

    #[test]
    fn memory_live_reserved_overflow_fails_closed() {
        let gate = ObjectDataCacheMemoryGate::new(
            &ObjectDataCacheConfig {
                min_free_memory_percent: 1,
                ..ObjectDataCacheConfig::default()
            },
            Arc::new(ObjectDataCacheStats::default()),
        );
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: u64::MAX,
            available_bytes: u64::MAX,
        }));
        gate.snapshot.live_reserved.store(u64::MAX - 1, Ordering::Relaxed);

        assert!(gate.try_claim(2, 0).is_none());
        assert_eq!(gate.claimed_bytes_for_test(), u64::MAX - 1);
    }

    #[test]
    fn memory_old_epoch_release_preserves_new_epoch_claim() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(None);
        gate.store_raw_snapshot_for_test(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        });
        let old_epoch = gate.try_claim(100, 0).expect("old epoch claim should fit");
        let sample = gate.begin_sample_for_test();
        gate.publish_sample_for_test(
            sample,
            ObjectDataCacheMemorySnapshot {
                total_bytes: 1_000,
                available_bytes: 400,
            },
        );
        let new_epoch = gate.try_claim(100, 0).expect("new epoch claim should fit");

        drop(old_epoch);
        assert_eq!(gate.claimed_bytes_for_test(), 100, "old token must not subtract the new epoch claim");
        drop(new_epoch);
        assert_eq!(gate.claimed_bytes_for_test(), 0);
    }

    #[test]
    fn memory_gate_zero_telemetry_fails_closed_when_enabled() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot::default()));

        assert!(gate.try_claim(1, 0).is_none());
    }

    #[test]
    fn memory_gate_preserves_post_admission_floor_at_exact_boundary() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        }));

        let boundary = gate.try_claim(300, 0);
        assert!(boundary.is_some(), "a claim ending exactly at the 20% floor must fit");
        drop(boundary);
        assert!(gate.try_claim(301, u64::MAX).is_none(), "a claim crossing the floor must fail");
    }

    #[test]
    fn full_cache_replacement_reserves_peak_live_bytes_until_last_body_owner_drops() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 1_000,
        }));

        let old_cached = gate
            .try_claim(300, 0)
            .expect("old allocation should fit")
            .wrap_bytes(Bytes::from(vec![0; 300]));
        let old_response = old_cached.clone();
        let replacement = gate
            .try_claim(300, 0)
            .expect("replacement allocation should fit while the old response is live")
            .wrap_bytes(Bytes::from(vec![0; 300]));
        assert_eq!(gate.claimed_bytes_for_test(), 600, "replacement peak includes both live allocations");

        drop(old_cached);
        assert_eq!(
            gate.claimed_bytes_for_test(),
            600,
            "eviction cannot release the response owner's allocation"
        );
        drop(replacement);
        assert_eq!(gate.claimed_bytes_for_test(), 300);
        drop(old_response);
        assert_eq!(gate.claimed_bytes_for_test(), 0);
    }

    #[test]
    fn eviction_completion_does_not_release_live_response_body_reservation() {
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::new(ObjectDataCacheStats::default()));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        }));

        let cached = gate
            .try_claim(100, 0)
            .expect("allocation should fit")
            .wrap_bytes(Bytes::from(vec![0; 100]));
        let response = cached.clone();
        drop(cached);

        assert_eq!(gate.claimed_bytes_for_test(), 100);
        drop(response);
        assert_eq!(gate.claimed_bytes_for_test(), 0);
    }

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

        assert!(!gate.allows_fill(1024, u64::MAX));
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

        assert!(gate.allows_fill(100, u64::MAX));
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

        assert!(gate.allows_fill(512, u64::MAX));
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

        assert!(!gate.allows_fill(128, u64::MAX));
        assert_eq!(stats.snapshot().memory_pressure_events, 1);
    }

    // backlog#1331 supersedes the cache-capacity deduction from backlog#1212:
    // an evicted cache entry can still have a live response clone, so cache
    // growth headroom is not a safe bound on process memory ownership.
    #[test]
    fn cache_growth_headroom_does_not_hide_outstanding_claims() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::clone(&stats));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000_000,
            available_bytes: 500_000, // 50% free, well above the 20% floor
        }));
        gate.snapshot.reserve(10_000_000);

        assert!(!gate.allows_fill(1_000, u64::MAX));
        assert!(!gate.allows_fill(1_000, 0), "zero cache headroom must not erase a live allocation claim");
    }

    #[test]
    fn reservation_still_bounds_burst_independent_of_growth_headroom() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let gate = ObjectDataCacheMemoryGate::new(&ObjectDataCacheConfig::default(), Arc::clone(&stats));
        gate.set_test_snapshot(Some(ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000_000,
            available_bytes: 300_000, // 30% free; floor is 20% = 200_000
        }));
        gate.snapshot.reserve(150_000);

        // 300_000 available - 150_000 reserved = 150_000 effective, below the
        // 200_000 floor: the reservation must still suppress the fill.
        assert!(
            !gate.allows_fill(1_000, 0),
            "reservation must still bound a burst while the cache can genuinely grow"
        );
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
        let _ = gate.allows_fill(1, u64::MAX);

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
            if snapshot.load().is_some_and(|snapshot| snapshot.total_bytes > 1) {
                refreshed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(refreshed, "the periodic refresher must replace the seed snapshot off the fill path");
        let _ = stats;
    }
}
