// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Mutable admission capacity without replacing or revoking held permits.

use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};
use tokio::time::Instant;

pub mod permit_set;

const MAX_PERMIT_REFERENCES: usize = usize::MAX / 2;

/// A resource owner publishes a capacity; running units retain their permits
/// after a reduction. Lease slots are reserved once, with no per-unit Arc
/// allocation. Tokio wait queues may lazily initialize platform mutex storage
/// on first contention. It does not discover devices, start
/// workers, or decide resource policy. Cancellation drops the acquire future.
///
/// Uncontended admission is atomic. Parked work takes a FIFO admission turn,
/// so repeated fast acquisitions cannot starve an older waiter. Source/age
/// priority still belongs to the caller's scheduler, before joining this queue.
pub struct AdaptiveAdmission {
    // One CAS observes both capacity and active count, so a concurrent shrink
    // cannot admit a new unit using an already superseded capacity.
    state: AtomicU64,
    origin: Instant,
    valid_until_millis: AtomicU64,
    // Only the FIFO turn holder subscribes. A stored notification is harmless:
    // capacity and freshness are always checked again after waking.
    changed: Notify,
    queued: AtomicUsize,
    // FIFO admission turn, not a mutable-state lock. No storage lock or
    // admitted permit may be held while joining. This private semaphore is
    // never closed; release and resize do not acquire it.
    turn: Semaphore,
    maximum: u32,
    slots: Box<[PermitSlots]>,
    next_chunk: AtomicUsize,
    completed: AtomicU64,
}

struct PermitSlots {
    free: AtomicU64,
    references: [AtomicUsize; 64],
}

struct QueuedAdmission<'a>(&'a AdaptiveAdmission);

impl Drop for QueuedAdmission<'_> {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::AcqRel);
    }
}

/// One active unit. Clones share that unit without allocating; the final drop
/// returns it to the same owner, including after resize or waiter cancellation.
#[must_use = "hold the permit for the admitted unit's complete lifetime"]
pub struct AdaptivePermit {
    owner: Arc<AdaptiveAdmission>,
    slot: usize,
}

impl Clone for AdaptivePermit {
    fn clone(&self) -> Self {
        // Subset carriers can retain slot references through a shared owner,
        // so the slot count needs its own bound in addition to Arc's guard.
        let owner = Arc::clone(&self.owner);
        if owner.slots[self.slot / 64].references[self.slot % 64].fetch_add(1, Ordering::Relaxed) >= MAX_PERMIT_REFERENCES {
            std::process::abort();
        }
        Self { owner, slot: self.slot }
    }
}

impl std::fmt::Debug for AdaptivePermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdaptivePermit")
            .field("slot", &self.slot)
            .field("owner", &self.owner.snapshot())
            .finish()
    }
}

/// One coherent view of configured capacity and actual active units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveAdmissionSnapshot {
    /// Current cap on new admissions. Zero pauses all new units.
    pub limit: u32,
    /// Existing units, which may temporarily exceed a reduced limit.
    pub active: u32,
}

/// Node scheduling capacity, separate from physical I/O and buffer ownership.
/// Its non-cloneable units end with the async operation; a native tail must
/// retain its own physical-resource unit, never this scheduling unit.
pub struct NodeAdmission {
    capacity: Arc<AdaptiveAdmission>,
    completed: AtomicU64,
}

/// One async operation's scheduling unit. Dropping a cancelled or failed
/// operation releases capacity without claiming completed work.
#[must_use = "retain the unit until the async operation ends"]
pub struct NodePermit {
    _capacity: AdaptivePermit,
    owner: Arc<NodeAdmission>,
}

impl NodeAdmission {
    /// Construct one node owner with no capacity until a resource sample arrives.
    pub fn new(maximum: NonZeroU16) -> Arc<Self> {
        Arc::new(Self {
            capacity: AdaptiveAdmission::new(maximum, 0),
            completed: AtomicU64::new(0),
        })
    }

    /// Publish feedback with this owner's actual completed-work counter.
    pub fn update(&self, controller: &mut FeedbackController, observation: Option<FeedbackObservation>) -> FeedbackDecision {
        self.update_with_probe(controller, observation, true)
    }

    /// Coordinate growth with other resource controllers in the same window.
    /// Protective reductions never depend on permission to probe.
    pub fn update_with_probe(
        &self,
        controller: &mut FeedbackController,
        observation: Option<FeedbackObservation>,
        allow_probe: bool,
    ) -> FeedbackDecision {
        let observation = observation.map(|mut observation| {
            observation.completed_units = self.completed();
            observation
        });
        controller.update_with_probe(&self.capacity, observation, allow_probe)
    }

    /// Current leased scheduling cap and held async units.
    pub fn snapshot(&self) -> AdaptiveAdmissionSnapshot {
        self.capacity.snapshot()
    }

    /// Completed operations, or unknown after the counter exhausts its range.
    pub fn completed(&self) -> Option<u64> {
        let value = self.completed.load(Ordering::Acquire);
        (value != u64::MAX).then_some(value)
    }

    /// Try without waiting, including when the caller already owns a lock.
    pub fn try_acquire(self: &Arc<Self>) -> Option<NodePermit> {
        self.capacity.try_acquire().map(|capacity| NodePermit {
            _capacity: capacity,
            owner: self.clone(),
        })
    }

    /// Wait in FIFO order. Callers must provide a deadline/cancellation and
    /// acquire before storage locks, not while retaining another node unit.
    pub async fn acquire(self: &Arc<Self>) -> NodePermit {
        NodePermit {
            _capacity: self.capacity.acquire().await,
            owner: self.clone(),
        }
    }
}

impl NodePermit {
    /// Count a completed operation exactly once, then release its node unit.
    /// This counter is scheduling progress, not an authoritative heal receipt.
    pub fn complete(self) {
        let _ = self
            .owner
            .completed
            .try_update(Ordering::AcqRel, Ordering::Acquire, |value| value.checked_add(1));
    }
}

/// Confidence-qualified pressure for one complete sampling window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourcePressure {
    /// Missing, stale, reset, or insufficient observations; never evidence of idle.
    Unknown,
    /// A resource or foreground-protection threshold was reached.
    Busy,
    /// All required observations were fresh and below their low watermarks.
    Stable,
}

/// Resource-owner input to the controller, after applying operator and kernel
/// caps. This describes one owner; callers must not duplicate a device's budget
/// for aliases, pools, or remote requesters.
#[derive(Debug, Clone, Copy)]
pub struct FeedbackObservation {
    /// Identity of the resource/telemetry generation. Replacement resets probes.
    pub generation: u64,
    /// Upper bound imposed by resources and explicit configuration; zero blocks.
    pub max_concurrency: u32,
    /// Complete-window pressure, including the confidence of its sources.
    pub pressure: ResourcePressure,
    /// Cumulative completed work in this generation, not just scheduled work.
    pub completed_units: Option<u64>,
}

/// Why the latest concurrency limit was selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedbackReason {
    /// The resource cap is unavailable; no new unit can be bounded safely.
    Unavailable,
    /// A changed owner or reset work counter requires a new baseline.
    NewBaseline,
    /// The resource or operator cap reduced the permitted concurrency.
    CapReduced,
    /// Pressure caused an immediate multiplicative reduction.
    Pressure,
    /// Incomplete telemetry retains at most one maintenance unit.
    LowConfidence,
    /// Waiting for consecutive stable windows, work progress and cooldown.
    Recovering,
    /// One additional unit is being probed after a stable recovery period.
    Probe,
}

/// One controller decision, suitable for metrics without dynamic labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedbackDecision {
    /// New admissions use this cap; existing permits remain valid.
    pub limit: u32,
    /// Decision cause, distinct from a proof of work completion or storage health.
    pub reason: FeedbackReason,
}

/// Single-owner additive probe/multiplicative decrease controller. It adjusts
/// only concurrency. Every source must be fresh before declaring `Stable`.
///
/// The sampler runs every second. A gap over two seconds resets recovery; eight
/// consecutive stable seconds and observed completed work allow one extra unit.
/// Pressure or lower caps apply immediately. This policy is not a performance
/// claim; its parameters require the same workload/foreground-SLO acceptance.
pub struct FeedbackController {
    previous: Option<(Instant, FeedbackObservation)>,
    stable_since: Option<Instant>,
    last_change: Instant,
    limit: u32,
}

impl Default for FeedbackController {
    fn default() -> Self {
        Self::new()
    }
}

impl FeedbackController {
    /// Start without a resource observation or an admission grant.
    pub fn new() -> Self {
        Self {
            previous: None,
            stable_since: None,
            last_change: Instant::now(),
            limit: 0,
        }
    }

    /// Consume one sample and publish a three-second lease to the same gate.
    /// If this function stops being called, admission expires independently.
    pub fn update(&mut self, gate: &AdaptiveAdmission, observation: Option<FeedbackObservation>) -> FeedbackDecision {
        self.update_with_probe(gate, observation, true)
    }

    /// Allow at most the sampling owner's selected controller to probe upward.
    pub fn update_with_probe(
        &mut self,
        gate: &AdaptiveAdmission,
        observation: Option<FeedbackObservation>,
        allow_probe: bool,
    ) -> FeedbackDecision {
        let observation = observation.map(|mut observation| {
            observation.max_concurrency = observation.max_concurrency.min(gate.maximum);
            observation
        });
        let decision = self.decide_with_probe(Instant::now(), observation, allow_probe);
        gate.set_sampled_limit(decision.limit, Duration::from_secs(3));
        decision
    }

    fn decide_with_probe(
        &mut self,
        now: Instant,
        observation: Option<FeedbackObservation>,
        allow_probe: bool,
    ) -> FeedbackDecision {
        let Some(observation) = observation else {
            self.previous = None;
            self.stable_since = None;
            self.last_change = now;
            self.limit = 0;
            return FeedbackDecision {
                limit: 0,
                reason: FeedbackReason::Unavailable,
            };
        };
        let previous = self.previous.replace((now, observation));
        let contiguous = previous.is_some_and(|(at, old)| {
            now.saturating_duration_since(at) <= Duration::from_secs(2)
                && old.generation == observation.generation
                && matches!((old.completed_units, observation.completed_units), (Some(a), Some(b)) if b >= a)
        });
        let advanced = previous
            .is_some_and(|(_, old)| matches!((old.completed_units, observation.completed_units), (Some(a), Some(b)) if b > a));
        let old_limit = self.limit;
        let reason = if observation.max_concurrency == 0 {
            self.limit = 0;
            self.stable_since = None;
            FeedbackReason::CapReduced
        } else if observation.pressure == ResourcePressure::Busy {
            self.limit = (self.limit / 2).max(1).min(observation.max_concurrency);
            self.stable_since = None;
            // Repeated busy windows restart cooldown, even at the minimum cap.
            self.last_change = now;
            FeedbackReason::Pressure
        } else if observation.max_concurrency < self.limit {
            self.limit = observation.max_concurrency;
            self.stable_since = None;
            FeedbackReason::CapReduced
        } else if observation.pressure == ResourcePressure::Unknown || observation.completed_units.is_none() {
            self.limit = 1.min(observation.max_concurrency);
            self.stable_since = None;
            FeedbackReason::LowConfidence
        } else if !contiguous || self.limit == 0 {
            self.limit = 1.min(observation.max_concurrency);
            self.stable_since = None;
            self.last_change = now;
            FeedbackReason::NewBaseline
        } else {
            let since = *self.stable_since.get_or_insert(now);
            if allow_probe
                && advanced
                && now.saturating_duration_since(since) >= Duration::from_secs(8)
                && now.saturating_duration_since(self.last_change) >= Duration::from_secs(8)
                && self.limit < observation.max_concurrency
            {
                self.limit += 1;
                FeedbackReason::Probe
            } else {
                FeedbackReason::Recovering
            }
        };
        if old_limit != self.limit {
            self.last_change = now;
        }
        FeedbackDecision {
            limit: self.limit,
            reason,
        }
    }
}

impl AdaptiveAdmission {
    /// Successful async operations using this physical owner. Native clones
    /// retain capacity but never count another completion.
    pub fn completed(&self) -> Option<u64> {
        let value = self.completed.load(Ordering::Acquire);
        (value != u64::MAX).then_some(value)
    }

    /// Construct a bounded owner, with zero initial capacity allowed for unknown
    /// input. The immutable maximum reserves at most 65,535 lease slots once;
    /// acquiring or cloning a permit does not allocate a per-unit Arc.
    pub fn new(maximum: NonZeroU16, limit: u32) -> Arc<Self> {
        let maximum_u32 = u32::from(maximum.get());
        let maximum = usize::from(maximum.get());
        let slots = (0..maximum.div_ceil(64))
            .map(|chunk| {
                let count = (maximum - chunk * 64).min(64);
                PermitSlots {
                    free: AtomicU64::new(if count == 64 { u64::MAX } else { (1_u64 << count) - 1 }),
                    references: std::array::from_fn(|_| AtomicUsize::new(0)),
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Arc::new(Self {
            state: AtomicU64::new(u64::from(limit.min(maximum_u32)) << 32),
            origin: Instant::now(),
            valid_until_millis: AtomicU64::new(u64::MAX),
            changed: Notify::new(),
            queued: AtomicUsize::new(0),
            turn: Semaphore::new(1),
            maximum: maximum_u32,
            slots,
            next_chunk: AtomicUsize::new(0),
            completed: AtomicU64::new(0),
        })
    }

    /// Publish a cap bounded by the immutable maximum, without losing held work
    /// or changing owner identity.
    pub fn set_limit(&self, limit: u32) {
        let limit = limit.min(self.maximum);
        let _ = self.state.try_update(Ordering::AcqRel, Ordering::Acquire, |state| {
            Some((u64::from(limit) << 32) | (state & u64::from(u32::MAX)))
        });
        self.changed.notify_one();
    }

    /// Publish a cap with a finite observation lifetime. Expired observations
    /// block new admissions even if the sampling worker has stopped. Existing
    /// permits are retained. A single resource owner must serialize publishers.
    ///
    /// Capacity is published before freshness, so a concurrent reader can
    /// temporarily reject a fresh update but cannot use an increased lifetime
    /// with the previous, potentially larger capacity.
    pub fn set_sampled_limit(&self, limit: u32, valid_for: Duration) {
        self.set_limit(limit);
        let deadline = self
            .elapsed_millis()
            .saturating_add(u64::try_from(valid_for.as_millis()).unwrap_or(u64::MAX));
        self.valid_until_millis.store(deadline, Ordering::Release);
        self.changed.notify_one();
    }

    fn elapsed_millis(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn fresh(&self) -> bool {
        self.elapsed_millis() < self.valid_until_millis.load(Ordering::Acquire)
    }

    /// Read the cap and active count from the same atomic observation.
    pub fn snapshot(&self) -> AdaptiveAdmissionSnapshot {
        // Acquire freshness first: a newly published lease must not be paired
        // with capacity loaded before its preceding reduction.
        let fresh = self.fresh();
        let state = self.state.load(Ordering::Acquire);
        AdaptiveAdmissionSnapshot {
            limit: if fresh {
                u32::try_from(state >> 32).unwrap_or(u32::MAX)
            } else {
                0
            },
            active: u32::try_from(state & u64::from(u32::MAX)).unwrap_or(u32::MAX),
        }
    }

    /// Admit immediately if capacity remains, without a per-unit allocation.
    /// The uncontended path uses atomics; refunding a racing acquisition may
    /// wake the FIFO head through Tokio's notification queue.
    pub fn try_acquire(self: &Arc<Self>) -> Option<AdaptivePermit> {
        if self.queued.load(Ordering::Acquire) != 0 {
            return None;
        }
        let permit = self.acquire_capacity()?;
        // A waiter can register between the first queue check and the capacity
        // CAS. Refund before exposing the permit rather than stealing its turn.
        (self.queued.load(Ordering::Acquire) == 0).then_some(permit)
    }

    fn acquire_capacity(self: &Arc<Self>) -> Option<AdaptivePermit> {
        if !self.fresh() {
            return None;
        }
        let state = self.state.load(Ordering::Acquire);
        if state & u64::from(u32::MAX) >= state >> 32 {
            return None;
        }
        let slot = self.reserve_slot()?;
        if self
            .state
            .try_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                let active = state & u64::from(u32::MAX);
                (active < state >> 32).then(|| state + 1)
            })
            .is_err()
        {
            self.release_slot(slot);
            self.changed.notify_one();
            return None;
        }
        self.slots[slot / 64].references[slot % 64].store(1, Ordering::Release);
        let permit = AdaptivePermit {
            owner: self.clone(),
            slot,
        };
        // The capacity CAS can retry across an observation expiry. Recheck
        // before giving ownership to the caller; dropping refunds this unit.
        self.fresh().then_some(permit)
    }

    fn reserve_slot(&self) -> Option<usize> {
        let first = self.next_chunk.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        for offset in 0..self.slots.len() {
            let chunk_index = (first + offset) % self.slots.len();
            let free = &self.slots[chunk_index].free;
            if let Ok(bits) = free.try_update(Ordering::AcqRel, Ordering::Acquire, |bits| (bits != 0).then(|| bits & (bits - 1)))
            {
                // trailing_zeros is in 0..64 because the zero word is rejected.
                return Some(chunk_index * 64 + bits.trailing_zeros() as usize);
            }
        }
        None
    }

    fn release_slot(&self, slot: usize) {
        self.slots[slot / 64].free.fetch_or(1_u64 << (slot % 64), Ordering::Release);
    }

    fn release_reference(&self, slot: usize) {
        self.release_reference_with(slot, || {});
    }

    fn release_reference_with(&self, slot: usize, before_release: impl FnOnce()) {
        if self.slots[slot / 64].references[slot % 64].fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        before_release();
        // Publish the free slot before exposing another admission capacity.
        self.release_slot(slot);
        self.state.fetch_sub(1, Ordering::AcqRel);
        self.changed.notify_one();
    }

    /// Wait for capacity. The future owns no permit until it returns; dropping
    /// a pending future leaves capacity untouched. Wrap in the caller's existing
    /// cancellation/deadline select before acquiring any storage commit locks.
    pub async fn acquire(self: &Arc<Self>) -> AdaptivePermit {
        if let Some(permit) = self.try_acquire() {
            return permit;
        }
        self.queued.fetch_add(1, Ordering::AcqRel);
        let _queued = QueuedAdmission(self);
        // Retain the successful permit inside its Result. No code can close
        // this private semaphore, so acquisition cannot return an error.
        let _turn = self.turn.acquire().await;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            // Register before checking capacity to avoid a lost release/resize.
            notified.as_mut().enable();
            if let Some(permit) = self.acquire_capacity() {
                return permit;
            }
            notified.await;
        }
    }
}

impl Drop for AdaptivePermit {
    fn drop(&mut self) {
        self.owner.release_reference(self.slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn node_progress_requires_explicit_completion_and_never_wraps() {
        let node = NodeAdmission::new(NonZeroU16::new(2).expect("two slots"));
        node.capacity.set_limit(2);
        drop(node.acquire().await);
        assert_eq!(node.completed(), Some(0), "cancellation is not progress");
        node.acquire().await.complete();
        assert_eq!(node.completed(), Some(1));
        node.completed.store(u64::MAX - 1, Ordering::Release);
        node.acquire().await.complete();
        node.acquire().await.complete();
        assert_eq!(node.completed(), None, "exhausted progress stays unknown");
        assert_eq!(node.snapshot().active, 0);
    }

    #[tokio::test]
    async fn cancelled_node_work_does_not_reserve_capacity_for_unrelated_native_tail() {
        let node = NodeAdmission::new(NonZeroU16::new(1).expect("one slot"));
        node.capacity.set_limit(1);
        let physical = AdaptiveAdmission::new(NonZeroU16::new(1).expect("one disk unit"), 1);
        let scheduling = node.acquire().await;
        let native_tail = physical.acquire().await;
        drop(scheduling);
        assert_eq!(node.completed(), Some(0));
        assert!(node.try_acquire().is_some(), "healthy resources can still be scheduled");
        assert!(physical.try_acquire().is_none(), "the stalled disk remains charged");
        drop(native_tail);
        assert!(physical.try_acquire().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn node_feedback_expires_without_revoking_held_work_or_fabricating_progress() {
        let node = NodeAdmission::new(NonZeroU16::new(2).expect("two slots"));
        let mut controller = FeedbackController::new();
        let input = FeedbackObservation {
            generation: 1,
            max_concurrency: 2,
            pressure: ResourcePressure::Stable,
            completed_units: Some(99),
        };
        node.update(&mut controller, Some(input));
        let held = node.acquire().await;
        assert_eq!(node.completed(), Some(0));
        tokio::time::advance(Duration::from_secs(4)).await;
        assert_eq!(node.snapshot(), AdaptiveAdmissionSnapshot { limit: 0, active: 1 });
        assert!(node.try_acquire().is_none());
        held.complete();
        assert_eq!(node.completed(), Some(1));
        assert_eq!(node.snapshot().active, 0);
    }

    #[tokio::test]
    async fn shared_unit_stays_admitted_until_native_worker_releases_its_last_clone() {
        let gate = AdaptiveAdmission::new(NonZeroU16::new(1).expect("one unit"), 1);
        let outer = gate.acquire().await;
        let native = outer.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            started_tx.send(()).expect("worker started");
            release_rx.blocking_recv().expect("release native worker");
            drop(native);
        });
        started_rx.await.expect("native worker is live");
        drop(outer);
        assert_eq!(gate.snapshot().active, 1);
        assert!(gate.try_acquire().is_none());
        gate.set_limit(0);
        assert_eq!(gate.snapshot().active, 1);
        release_tx.send(()).expect("release tail");
        worker.await.expect("native tail completed");
        assert_eq!(gate.snapshot().active, 0);
        gate.set_limit(1);
        assert!(gate.try_acquire().is_some());
    }

    #[tokio::test]
    async fn bounded_slots_cross_word_boundaries_and_recycle_only_after_final_drop() {
        let maximum = NonZeroU16::new(65).expect("partial second chunk");
        let gate = AdaptiveAdmission::new(maximum, u32::MAX);
        assert_eq!(gate.snapshot().limit, 65);
        for _ in 0..4 {
            let units: Vec<_> = (0..65).map(|_| gate.try_acquire().expect("free slot")).collect();
            let tails = units.clone();
            assert_eq!(gate.snapshot().active, 65);
            assert!(gate.try_acquire().is_none());
            drop(units);
            assert_eq!(gate.snapshot().active, 65);
            gate.set_limit(0);
            for (remaining, tail) in (0..65).rev().zip(tails) {
                drop(tail);
                assert_eq!(gate.snapshot().active, remaining);
            }
            gate.set_limit(u32::MAX);
            assert_eq!(gate.snapshot().limit, 65);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_units_survive_concurrent_clone_drop_reuse_and_resize() {
        let gate = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded owner"), 8);
        let mut workers = Vec::new();
        for _ in 0..16 {
            let gate = gate.clone();
            workers.push(tokio::spawn(async move {
                for _ in 0..64 {
                    let root = gate.acquire().await;
                    let one = root.clone();
                    let two = one.clone();
                    drop(root);
                    tokio::task::yield_now().await;
                    drop(two);
                    tokio::task::yield_now().await;
                    drop(one);
                }
            }));
        }
        for limit in [0, 1, 8, 2, 0, 8] {
            gate.set_limit(limit);
            tokio::task::yield_now().await;
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            for worker in workers {
                worker.await.expect("lease worker");
            }
        })
        .await
        .expect("all lease workers complete");
        assert_eq!(gate.snapshot(), AdaptiveAdmissionSnapshot { limit: 8, active: 0 });
        let full: Vec<_> = (0..8).map(|_| gate.try_acquire().expect("all slots returned")).collect();
        assert!(gate.try_acquire().is_none());
        drop(full);
    }

    #[tokio::test]
    async fn queued_work_cannot_be_starved_by_new_fast_acquisitions() {
        use std::task::{Context, Poll, Waker};
        let gate = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded test owner"), 1);
        let held = gate.acquire().await;
        let mut first = Box::pin(gate.acquire());
        let mut second = Box::pin(gate.acquire());
        let mut context = Context::from_waker(Waker::noop());
        assert!(first.as_mut().poll(&mut context).is_pending());
        assert!(second.as_mut().poll(&mut context).is_pending());
        drop(held);
        assert!(gate.try_acquire().is_none(), "new work must not steal a parked worker's turn");
        assert!(second.as_mut().poll(&mut context).is_pending(), "later work cannot pass the queue head");
        let Poll::Ready(first_permit) = first.as_mut().poll(&mut context) else {
            panic!("queue head must run")
        };
        drop(first_permit);
        let Poll::Ready(second_permit) = second.as_mut().poll(&mut context) else {
            panic!("second waiter must run")
        };
        drop(second_permit);
        assert!(gate.try_acquire().is_some(), "drained queue restores the atomic fast path");
    }

    #[tokio::test]
    async fn cancelling_queued_work_preserves_fifo_capacity_and_fast_path() {
        use std::task::{Context, Poll, Waker};
        let gate = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded test owner"), 0);
        let mut head = Box::pin(gate.acquire());
        let mut middle = Box::pin(gate.acquire());
        let mut tail = Box::pin(gate.acquire());
        let mut context = Context::from_waker(Waker::noop());
        assert!(head.as_mut().poll(&mut context).is_pending());
        assert!(middle.as_mut().poll(&mut context).is_pending());
        assert!(tail.as_mut().poll(&mut context).is_pending());
        drop(middle);
        drop(head);
        gate.set_limit(1);
        assert!(
            gate.try_acquire().is_none(),
            "cancelled head must not expose the surviving waiter's capacity"
        );
        let Poll::Ready(permit) = tail.as_mut().poll(&mut context) else {
            panic!("surviving waiter must run")
        };
        drop(permit);
        assert!(gate.try_acquire().is_some());

        gate.set_limit(0);
        let mut cancelled = Box::pin(gate.acquire());
        assert!(cancelled.as_mut().poll(&mut context).is_pending());
        drop(cancelled);
        gate.set_limit(1);
        assert!(
            gate.try_acquire().is_some(),
            "cancelling the final waiter must not strand an admission turn"
        );
    }

    #[tokio::test]
    async fn cancelling_a_notified_head_hands_capacity_to_the_next_waiter() {
        use std::task::{Context, Poll, Waker};
        let gate = AdaptiveAdmission::new(NonZeroU16::new(2).expect("two units"), 0);
        let mut head = Box::pin(gate.acquire());
        let mut next = Box::pin(gate.acquire());
        let mut context = Context::from_waker(Waker::noop());
        assert!(head.as_mut().poll(&mut context).is_pending());
        assert!(next.as_mut().poll(&mut context).is_pending());
        gate.set_limit(1);
        // Cancel after notification, before the head consumes its capacity.
        drop(head);
        let Poll::Ready(permit) = next.as_mut().poll(&mut context) else {
            panic!("successor must observe capacity without another notification")
        };
        assert_eq!(gate.snapshot(), AdaptiveAdmissionSnapshot { limit: 1, active: 1 });
        drop(permit);
    }

    #[tokio::test(start_paused = true)]
    async fn one_resize_serves_multiple_fifo_turns_but_stored_wakes_cannot_bypass_expiry() {
        use std::task::{Context, Poll, Waker};
        let gate = AdaptiveAdmission::new(NonZeroU16::new(2).expect("two units"), 0);
        let mut first = Box::pin(gate.acquire());
        let mut second = Box::pin(gate.acquire());
        let mut context = Context::from_waker(Waker::noop());
        assert!(first.as_mut().poll(&mut context).is_pending());
        assert!(second.as_mut().poll(&mut context).is_pending());
        gate.set_sampled_limit(2, Duration::from_secs(1));
        let Poll::Ready(one) = first.as_mut().poll(&mut context) else {
            panic!("first turn must observe resize")
        };
        let Poll::Ready(two) = second.as_mut().poll(&mut context) else {
            panic!("second turn must observe resize without releasing the first unit")
        };
        tokio::time::advance(Duration::from_secs(1)).await;
        drop(one);
        drop(two);
        let mut expired = Box::pin(gate.acquire());
        assert!(expired.as_mut().poll(&mut context).is_pending(), "stored wake is not a fresh lease");
        gate.set_sampled_limit(1, Duration::from_secs(1));
        let Poll::Ready(renewed) = expired.as_mut().poll(&mut context) else {
            panic!("fresh observation must wake expired admission")
        };
        drop(renewed);
        assert_eq!(gate.snapshot().active, 0);
    }
    use std::future::Future;
    use std::task::Poll;

    #[tokio::test]
    async fn shrink_preserves_all_held_units_and_blocks_until_below_new_limit() {
        let owner = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded test owner"), 3);
        let first = owner.acquire().await;
        let second = owner.acquire().await;
        let third = owner.acquire().await;
        owner.set_limit(1);
        assert_eq!(owner.snapshot(), AdaptiveAdmissionSnapshot { limit: 1, active: 3 });
        drop(first);
        assert!(owner.try_acquire().is_none());
        drop(second);
        assert!(owner.try_acquire().is_none());
        drop(third);
        let fourth = owner.try_acquire().expect("last old permit returned to original owner");
        assert_eq!(owner.snapshot(), AdaptiveAdmissionSnapshot { limit: 1, active: 1 });
        drop(fourth);
        assert_eq!(owner.snapshot().active, 0);
    }

    #[tokio::test]
    async fn zero_capacity_and_cancelled_waiter_do_not_leak_or_bypass() {
        let owner = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded test owner"), 0);
        let mut waiting = Box::pin(owner.acquire());
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(waiting);
        assert_eq!(owner.snapshot(), AdaptiveAdmissionSnapshot { limit: 0, active: 0 });
        assert!(owner.try_acquire().is_none());
        owner.set_limit(1);
        let permit = owner.acquire().await;
        assert!(owner.try_acquire().is_none());
        drop(permit);
        assert_eq!(owner.snapshot().active, 0);
    }

    #[tokio::test]
    async fn resize_wakes_a_registered_waiter_without_an_unrelated_release() {
        let owner = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded test owner"), 0);
        let mut waiting = Box::pin(owner.acquire());
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        owner.set_limit(1);
        let permit = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("resize notification");
        assert_eq!(owner.snapshot().active, 1);
        drop(permit);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_resize_release_and_acquire_keep_the_owner_count_exact() {
        let owner = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded test owner"), 8);
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let owner = owner.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..100 {
                    let permit = owner.acquire().await;
                    tokio::task::yield_now().await;
                    drop(permit);
                }
            }));
        }
        for limit in [1, 0, 4, 2, 8, 0, u32::MAX, 1] {
            owner.set_limit(limit);
            tokio::task::yield_now().await;
        }
        owner.set_limit(8);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for task in tasks {
                task.await.expect("admission task");
            }
        })
        .await
        .expect("all waiters make progress");
        assert_eq!(owner.snapshot(), AdaptiveAdmissionSnapshot { limit: 8, active: 0 });
    }

    #[tokio::test(start_paused = true)]
    async fn expired_observation_blocks_new_work_without_revoking_held_permits() {
        let owner = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded test owner"), 0);
        owner.set_sampled_limit(2, Duration::from_secs(2));
        let held = owner.acquire().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(owner.try_acquire().is_none());
        assert_eq!(owner.snapshot(), AdaptiveAdmissionSnapshot { limit: 0, active: 1 });
        drop(held);
        assert_eq!(owner.snapshot().active, 0);
        owner.set_sampled_limit(1, Duration::from_secs(2));
        let fresh = owner.try_acquire().expect("fresh observation reopens same owner");
        owner.set_sampled_limit(8, Duration::ZERO);
        assert!(owner.try_acquire().is_none());
        drop(fresh);
        assert_eq!(owner.snapshot().active, 0);
    }

    fn healthy(completed: u64) -> FeedbackObservation {
        FeedbackObservation {
            generation: 1,
            max_concurrency: 8,
            pressure: ResourcePressure::Stable,
            completed_units: Some(completed),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shared_probe_permission_blocks_growth_but_never_protective_cuts() {
        let gate = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bound"), 0);
        let mut controller = FeedbackController::new();
        controller.update_with_probe(&gate, Some(healthy(0)), false);
        for completed in 1..=20 {
            tokio::time::advance(Duration::from_secs(1)).await;
            assert_eq!(controller.update_with_probe(&gate, Some(healthy(completed)), false).limit, 1);
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(controller.update_with_probe(&gate, Some(healthy(21)), true).reason, FeedbackReason::Probe);
        assert_eq!(gate.snapshot().limit, 2);
        let mut busy = healthy(22);
        busy.pressure = ResourcePressure::Busy;
        assert_eq!(controller.update_with_probe(&gate, Some(busy), false).limit, 1);
        assert_eq!(controller.update_with_probe(&gate, None, false).limit, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn controller_grows_one_unit_per_cooldown_and_cuts_pressure_immediately() {
        let gate = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded test owner"), 0);
        let mut controller = FeedbackController::new();
        assert_eq!(controller.update(&gate, Some(healthy(0))).limit, 1);
        for completed in 1..=25 {
            tokio::time::advance(Duration::from_secs(1)).await;
            let limit = controller.update(&gate, Some(healthy(completed))).limit;
            assert_eq!(
                limit,
                if completed < 9 {
                    1
                } else if completed < 17 {
                    2
                } else if completed < 25 {
                    3
                } else {
                    4
                }
            );
        }
        let held = gate.try_acquire().expect("existing work");
        let mut pressure = healthy(26);
        pressure.pressure = ResourcePressure::Busy;
        assert_eq!(
            controller.update(&gate, Some(pressure)),
            FeedbackDecision {
                limit: 2,
                reason: FeedbackReason::Pressure
            }
        );
        pressure.max_concurrency = 0;
        assert_eq!(controller.update(&gate, Some(pressure)).limit, 0);
        assert_eq!(gate.snapshot().active, 1);
        assert!(gate.try_acquire().is_none());
        drop(held);
    }

    #[tokio::test(start_paused = true)]
    async fn controller_missing_low_sample_stalled_or_changed_sources_never_expand() {
        for kind in 0..5 {
            let gate = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded test owner"), 0);
            let mut controller = FeedbackController::new();
            for tick in 0..32 {
                let mut observation = healthy(tick);
                match kind {
                    0 => observation.pressure = ResourcePressure::Unknown,
                    1 => observation.completed_units = None,
                    2 => observation.completed_units = Some(0),
                    3 => observation.generation = tick,
                    _ => {}
                }
                tokio::time::advance(Duration::from_secs(if kind == 4 { 3 } else { 1 })).await;
                assert!(controller.update(&gate, Some(observation)).limit <= 1, "kind {kind}");
            }
            assert_eq!(
                controller.update(&gate, None),
                FeedbackDecision {
                    limit: 0,
                    reason: FeedbackReason::Unavailable
                }
            );
            assert!(gate.try_acquire().is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn controller_resumes_after_zero_cap_without_requiring_impossible_progress() {
        let gate = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded test owner"), 0);
        let mut controller = FeedbackController::new();
        let mut sample = healthy(0);
        sample.max_concurrency = 0;
        assert_eq!(controller.update(&gate, Some(sample)).limit, 0);
        tokio::time::advance(Duration::from_secs(1)).await;
        sample.max_concurrency = 8;
        assert_eq!(controller.update(&gate, Some(sample)).limit, 1);
        assert!(gate.try_acquire().is_some(), "minimum progress restarts inside the restored cap");
    }

    #[tokio::test(start_paused = true)]
    async fn controller_applies_pressure_reduction_and_lower_cap_in_the_same_window() {
        let gate = AdaptiveAdmission::new(NonZeroU16::new(8).expect("bounded test owner"), 0);
        let mut controller = FeedbackController::new();
        for completed in 0..=25 {
            controller.update(&gate, Some(healthy(completed)));
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        assert_eq!(gate.snapshot().limit, 4);
        let mut sample = healthy(26);
        sample.max_concurrency = 3;
        sample.pressure = ResourcePressure::Busy;
        assert_eq!(controller.update(&gate, Some(sample)).limit, 2);
    }
}
