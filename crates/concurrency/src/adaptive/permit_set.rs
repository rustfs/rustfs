// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Bounded admission and ownership of physical resource subsets.

use std::io;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::{AdaptiveAdmission, AdaptivePermit, MAX_PERMIT_REFERENCES, NodeAdmission, NodePermit};

/// Matches the bounded physical-device topology. Construct owners during disk
/// attachment, before object work; this does not acquire or discover resources.
const MAX_OWNERS: usize = 256;
const NO_UNIT: usize = usize::MAX;

#[derive(Clone, Copy, Debug)]
struct ResourceMask([u64; MAX_OWNERS / 64]);

impl ResourceMask {
    fn empty() -> Self {
        Self([0; MAX_OWNERS / 64])
    }

    fn insert(&mut self, index: usize) {
        self.0[index / 64] |= 1 << (index % 64);
    }

    fn contains(self, index: usize) -> bool {
        self.0[index / 64] & (1 << (index % 64)) != 0
    }
}

/// A stable subset of resource owners, such as the physical leaves backing one
/// logical disk. Slots are allocated once and reused after their final clone.
/// Different subsets can retain the same admitted unit without charging it twice.
pub struct AdaptivePermitSetOwner {
    owners: Box<[Arc<AdaptiveAdmission>]>,
    turn_order: Box<[usize]>,
    capacity: Arc<AdaptiveAdmission>,
    units: Box<[AtomicUsize]>,
    extending: Box<[AtomicBool]>,
    all: ResourceMask,
}

/// A selection from one preallocated resource owner. Build selections during
/// attachment; combining them for an operation does not allocate or take locks.
/// Aliases and overlapping disk groups charge each physical resource once.
#[derive(Clone)]
pub struct AdaptiveResourceSelection {
    owner: Arc<AdaptivePermitSetOwner>,
    selected: ResourceMask,
}

impl AdaptiveResourceSelection {
    /// Combine attachments from the same owner generation. A rejected union
    /// leaves this selection unchanged, so callers can defer stale topology.
    pub fn union_with(&mut self, other: &Self) -> Result<(), CaptureError> {
        if !Arc::ptr_eq(&self.owner, &other.owner) {
            return Err(CaptureError::ResourceOwner);
        }
        for (selected, additional) in self.selected.0.iter_mut().zip(other.selected.0) {
            *selected |= additional;
        }
        Ok(())
    }

    /// Attempt only the selected resources without retaining partial grants.
    pub fn try_acquire_with_node(&self, node: &Arc<NodeAdmission>) -> Option<NodeResourcePermit> {
        self.owner.try_acquire_selected(node, self.selected)
    }

    /// Preserve the owner's FIFO order while waiting for the selected leaves.
    /// Call before storage locks; cancellation and deadlines belong to callers.
    pub async fn acquire_with_node(&self, node: &Arc<NodeAdmission>) -> Result<NodeResourcePermit, tokio::sync::AcquireError> {
        self.owner.acquire_selected(node, self.selected).await
    }
}

/// Capturing ownership never waits. Exhaustion must be handled before storage
/// work, using the caller's existing cancellation and retry policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureError {
    /// The caller supplied the wrong number of resource units.
    ResourceCount,
    /// A supplied unit belongs to a different capacity owner or incarnation.
    ResourceOwner,
    /// All preallocated subset slots are still retained.
    Full,
    /// A physical resource has no immediately available capacity.
    Unavailable,
    /// Another owner cannot be retained without overflowing its reference count.
    ReferenceOverflow,
}

impl std::fmt::Display for CaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ResourceCount => "resource unit count does not match the subset",
            Self::ResourceOwner => "resource unit owner does not match the subset",
            Self::Full => "resource subset slots are exhausted",
            Self::Unavailable => "physical resource capacity is unavailable",
            Self::ReferenceOverflow => "resource unit reference count overflow",
        })
    }
}

impl std::error::Error for CaptureError {}

/// Retains only this subset's admitted units. Native work may clone this value;
/// cancellation of its async caller does not return capacity before native I/O
/// finishes. The final drop returns each unit to its original owner.
#[must_use = "retain resource ownership until the admitted work finishes"]
pub struct AdaptivePermitSet {
    owner: Arc<AdaptivePermitSetOwner>,
    slot: usize,
}

/// A whole resource set and its async scheduling unit. Native work may retain
/// resource subsets through `resources`, but cannot retain the node unit.
#[must_use = "retain admission until the async operation finishes"]
pub struct NodeResourcePermit {
    resources: AdaptivePermitSet,
    node: NodePermit,
}

impl NodeResourcePermit {
    /// Admitted physical units, available for per-disk native ownership.
    pub fn resources(&self) -> &AdaptivePermitSet {
        &self.resources
    }

    /// Return physical ownership before recording and releasing node progress.
    pub fn complete(self) {
        let Self { resources, node } = self;
        let first_unit = resources.slot * resources.owner.owners.len();
        for (index, owner) in resources.owner.owners.iter().enumerate() {
            if resources.owner.units[first_unit + index].load(Ordering::Acquire) == NO_UNIT {
                continue;
            }
            let _ = owner
                .completed
                .try_update(Ordering::AcqRel, Ordering::Acquire, |value| value.checked_add(1));
        }
        drop(resources);
        node.complete();
    }
}

struct ResourceExtension<'a>(&'a AtomicBool);

impl Drop for ResourceExtension<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct ResourceTurns<'a> {
    owner: &'a AdaptivePermitSetOwner,
    selected: ResourceMask,
    held: usize,
}

impl Drop for ResourceTurns<'_> {
    fn drop(&mut self) {
        for (index, owner) in self.owner.owners.iter().enumerate() {
            if self.selected.contains(index) {
                owner.queued.fetch_sub(1, Ordering::AcqRel);
            }
        }
        for index in self.owner.turn_order[..self.held].iter().rev() {
            if self.selected.contains(*index) {
                self.owner.owners[*index].turn.add_permits(1);
            }
        }
    }
}

impl AdaptivePermitSetOwner {
    /// Reserve a bounded carrier pool outside object work. `maximum` bounds
    /// concurrent captured subsets, including native tails. Admission must run
    /// before storage locks; capturing already admitted subsets never waits.
    pub fn new(maximum: NonZeroU16, owners: Vec<Arc<AdaptiveAdmission>>) -> io::Result<Arc<Self>> {
        if owners.is_empty()
            || owners.len() > MAX_OWNERS
            || owners
                .iter()
                .enumerate()
                .any(|(index, owner)| owners[..index].iter().any(|previous| Arc::ptr_eq(previous, owner)))
        {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid resource subset owners"));
        }
        // AdaptiveAdmission rotates across complete 64-slot words. Its active
        // cap is exact, but any slot in the final word can be selected.
        let count = usize::from(maximum.get()).div_ceil(64) * 64;
        let mut units = Vec::new();
        units.try_reserve_exact(count * owners.len()).map_err(io::Error::other)?;
        units.extend((0..count * owners.len()).map(|_| AtomicUsize::new(NO_UNIT)));
        let mut all = ResourceMask::empty();
        for index in 0..owners.len() {
            all.insert(index);
        }
        // All overlapping sets take FIFO turns in the same order, irrespective
        // of disk attachment order. Strong references keep these addresses stable.
        let mut turn_order: Vec<_> = (0..owners.len()).collect();
        turn_order.sort_unstable_by_key(|index| Arc::as_ptr(&owners[*index]));
        Ok(Arc::new(Self {
            owners: owners.into_boxed_slice(),
            turn_order: turn_order.into_boxed_slice(),
            capacity: AdaptiveAdmission::new(maximum, u32::from(maximum.get())),
            units: units.into_boxed_slice(),
            extending: (0..count).map(|_| AtomicBool::new(false)).collect(),
            all,
        }))
    }

    /// Start a node-scoped operation with no planned local leaves. Auxiliary
    /// local I/O may still extend it through this immutable resource catalog.
    pub fn empty_selection(self: &Arc<Self>) -> AdaptiveResourceSelection {
        AdaptiveResourceSelection {
            owner: self.clone(),
            selected: ResourceMask::empty(),
        }
    }

    /// Resolve an attachment's leaves against this immutable owner outside the
    /// object path. Repeated aliases select the same unit; foreign owners fail.
    pub fn select(self: &Arc<Self>, owners: &[Arc<AdaptiveAdmission>]) -> Result<AdaptiveResourceSelection, CaptureError> {
        if owners.is_empty() || owners.len() > MAX_OWNERS {
            return Err(CaptureError::ResourceCount);
        }
        let mut selected = ResourceMask::empty();
        for expected in owners {
            let index = self
                .owners
                .iter()
                .position(|owner| Arc::ptr_eq(owner, expected))
                .ok_or(CaptureError::ResourceOwner)?;
            selected.insert(index);
        }
        Ok(AdaptiveResourceSelection {
            owner: self.clone(),
            selected,
        })
    }

    /// Retain exactly these previously admitted units, in attachment order.
    /// No resource admission, heap allocation, mutex or new task occurs here.
    /// Rejected input refunds every reference acquired by this attempt.
    pub fn capture(self: &Arc<Self>, units: &[&AdaptivePermit]) -> Result<AdaptivePermitSet, CaptureError> {
        if units.len() != self.owners.len() {
            return Err(CaptureError::ResourceCount);
        }
        let slot = self.capacity.try_acquire().ok_or(CaptureError::Full)?;
        let index = slot.slot;
        let first_unit = index * self.owners.len();
        let mut retained = 0;
        let mut failure = None;
        for (index, (expected, unit)) in self.owners.iter().zip(units).enumerate() {
            if !Arc::ptr_eq(expected, &unit.owner) {
                failure = Some(CaptureError::ResourceOwner);
                break;
            }
            // The borrowed permit prevents slot reuse during capture. This
            // owner keeps its gates alive after that original permit is dropped.
            if expected.slots[unit.slot / 64].references[unit.slot % 64]
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |references| {
                    (references < MAX_PERMIT_REFERENCES).then(|| references + 1)
                })
                .is_err()
            {
                failure = Some(CaptureError::ReferenceOverflow);
                break;
            }
            self.units[first_unit + index].store(unit.slot, Ordering::Relaxed);
            retained += 1;
        }
        if let Some(error) = failure {
            self.release_units(index, retained);
            return Err(error);
        }
        Ok(self.finish_capture(index))
    }

    /// Retain only this owner's physical leaves from an admitted parent set.
    /// Owner identity also rejects stale device incarnations. No new admission,
    /// allocation, or wait occurs while projecting native I/O ownership.
    pub fn capture_from(self: &Arc<Self>, source: &AdaptivePermitSet) -> Result<AdaptivePermitSet, CaptureError> {
        if Arc::ptr_eq(self, &source.owner) {
            let first_unit = source.slot * self.owners.len();
            if self.units[first_unit..first_unit + self.owners.len()]
                .iter()
                .any(|unit| unit.load(Ordering::Acquire) == NO_UNIT)
            {
                return Err(CaptureError::ResourceOwner);
            }
            // A single-disk plan is already its native subset. Reuse that row
            // so a full carrier pool does not block its own admitted operation.
            return Ok(source.clone());
        }
        let slot = self.capacity.try_acquire().ok_or(CaptureError::Full)?;
        let first_unit = slot.slot * self.owners.len();
        let source_first = source.slot * source.owner.owners.len();
        for (index, expected) in self.owners.iter().enumerate() {
            let Some(source_index) = source.owner.owners.iter().position(|owner| Arc::ptr_eq(owner, expected)) else {
                self.release_units(slot.slot, index);
                return Err(CaptureError::ResourceOwner);
            };
            // The borrowed source keeps every referenced unit alive throughout
            // projection, even if other carriers release their references.
            let unit_slot = source.owner.units[source_first + source_index].load(Ordering::Acquire);
            if unit_slot == NO_UNIT {
                self.release_units(slot.slot, index);
                return Err(CaptureError::ResourceOwner);
            }
            if expected.slots[unit_slot / 64].references[unit_slot % 64]
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |references| {
                    (references < MAX_PERMIT_REFERENCES).then(|| references + 1)
                })
                .is_err()
            {
                self.release_units(slot.slot, index);
                return Err(CaptureError::ReferenceOverflow);
            }
            self.units[first_unit + index].store(unit_slot, Ordering::Relaxed);
        }
        Ok(self.finish_capture(slot.slot))
    }

    /// Project native ownership, adding any missing leaves to the parent with
    /// a nonblocking attempt. Additional leaves must come from the same topology
    /// and attachment selection. Parent completion credits each physical owner
    /// once; its native projections retain only the devices they actually use.
    pub fn try_capture_or_acquire_from(
        self: &Arc<Self>,
        source: &AdaptivePermitSet,
        attachment: &AdaptiveResourceSelection,
    ) -> Result<AdaptivePermitSet, CaptureError> {
        match self.capture_from(source) {
            Ok(native) => return Ok(native),
            Err(CaptureError::ResourceOwner) => {}
            Err(error) => return Err(error),
        }
        if !Arc::ptr_eq(&source.owner, &attachment.owner) {
            return Err(CaptureError::ResourceOwner);
        }
        // Serialize only row extension. Never queue or wait under storage locks.
        // The source borrow keeps the row alive until this guard is released.
        let extending = &source.owner.extending[source.slot];
        extending
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .map_err(|_| CaptureError::Unavailable)?;
        let _extension = ResourceExtension(extending);
        let slot = self.capacity.try_acquire().ok_or(CaptureError::Full)?;
        let first_unit = slot.slot * self.owners.len();
        let source_first = source.slot * source.owner.owners.len();
        let mut acquired = ResourceMask::empty();
        for (index, expected) in self.owners.iter().enumerate() {
            let source_index = source.owner.owners.iter().position(|owner| Arc::ptr_eq(owner, expected));
            let Some(source_index) = source_index.filter(|index| attachment.selected.contains(*index)) else {
                self.release_units(slot.slot, index);
                return Err(CaptureError::ResourceOwner);
            };
            let mut unit_slot = source.owner.units[source_first + source_index].load(Ordering::Acquire);
            let additional = if unit_slot == NO_UNIT {
                let Some(unit) = expected.try_acquire() else {
                    self.release_units(slot.slot, index);
                    return Err(CaptureError::Unavailable);
                };
                unit_slot = unit.slot;
                acquired.insert(index);
                Some(unit)
            } else {
                None
            };
            // Either the source or this private local permit keeps the unit
            // alive until the native carrier owns its reference.
            if expected.slots[unit_slot / 64].references[unit_slot % 64]
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |references| {
                    (references < MAX_PERMIT_REFERENCES).then(|| references + 1)
                })
                .is_err()
            {
                self.release_units(slot.slot, index);
                return Err(CaptureError::ReferenceOverflow);
            }
            self.units[first_unit + index].store(unit_slot, Ordering::Relaxed);
            drop(additional);
        }
        // All fallible acquisition is complete. These new units are private to
        // this row, so retaining one parent reference cannot overflow. Publish
        // only after the whole native subset succeeds; failures above leave the
        // parent untouched and return every partial grant.
        for (index, expected) in self.owners.iter().enumerate() {
            if !acquired.contains(index) {
                continue;
            }
            let source_index = source
                .owner
                .owners
                .iter()
                .position(|owner| Arc::ptr_eq(owner, expected))
                .expect("attachment membership was checked before acquisition");
            let unit_slot = self.units[first_unit + index].load(Ordering::Relaxed);
            expected.slots[unit_slot / 64].references[unit_slot % 64].fetch_add(1, Ordering::Relaxed);
            source.owner.units[source_first + source_index].store(unit_slot, Ordering::Release);
        }
        Ok(self.finish_capture(slot.slot))
    }

    /// Attempt the whole set without waiting or retaining a partial grant.
    pub fn try_acquire_with_node(self: &Arc<Self>, node: &Arc<NodeAdmission>) -> Option<NodeResourcePermit> {
        self.try_acquire_selected(node, self.all)
    }

    fn try_acquire_selected(self: &Arc<Self>, node: &Arc<NodeAdmission>, selected: ResourceMask) -> Option<NodeResourcePermit> {
        let slot = self.capacity.try_acquire()?;
        let node = node.try_acquire()?;
        let resources = self.acquire_units(slot.slot, false, selected).ok()?;
        Some(NodeResourcePermit { resources, node })
    }

    /// Acquire physical FIFO turns in stable owner order, then wait for node
    /// capacity only when all physical leaves are ready. No physical or node
    /// unit is held while waiting for a physical leaf. Callers must impose their
    /// cancellation/deadline and enter before cache or namespace locks.
    pub async fn acquire_with_node(
        self: &Arc<Self>,
        node: &Arc<NodeAdmission>,
    ) -> Result<NodeResourcePermit, tokio::sync::AcquireError> {
        self.acquire_selected(node, self.all).await
    }

    async fn acquire_selected(
        self: &Arc<Self>,
        node: &Arc<NodeAdmission>,
        selected: ResourceMask,
    ) -> Result<NodeResourcePermit, tokio::sync::AcquireError> {
        if let Some(permit) = self.try_acquire_selected(node, selected) {
            return Ok(permit);
        }
        for (index, owner) in self.owners.iter().enumerate() {
            if selected.contains(index) {
                owner.queued.fetch_add(1, Ordering::AcqRel);
            }
        }
        let mut turns = ResourceTurns {
            owner: self,
            selected,
            held: 0,
        };
        for (position, index) in self.turn_order.iter().enumerate() {
            if !selected.contains(*index) {
                continue;
            }
            self.owners[*index].turn.acquire().await?.forget();
            turns.held = position + 1;
        }
        loop {
            for (index, owner) in self.owners.iter().enumerate() {
                if selected.contains(index) {
                    Self::wait_available(owner).await;
                }
            }
            // Waiting for a device must not occupy a shared carrier and block
            // an operation whose selected devices are already available.
            let slot = self.capacity.acquire().await;
            let node = node.acquire().await;
            match self.acquire_units(slot.slot, true, selected) {
                Ok(resources) => return Ok(NodeResourcePermit { resources, node }),
                Err(index) => {
                    // A leaf may shrink or expire during the node wait. Return
                    // node capacity before any further wait on that leaf.
                    drop(node);
                    drop(slot);
                    Self::wait_available(&self.owners[index]).await;
                }
            }
        }
    }

    async fn wait_available(owner: &AdaptiveAdmission) {
        loop {
            let notified = owner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let snapshot = owner.snapshot();
            if snapshot.active < snapshot.limit && owner.slots.iter().any(|slots| slots.free.load(Ordering::Acquire) != 0) {
                return;
            }
            notified.await;
        }
    }

    fn acquire_units(
        self: &Arc<Self>,
        slot: usize,
        owns_turns: bool,
        selected: ResourceMask,
    ) -> Result<AdaptivePermitSet, usize> {
        let first_unit = slot * self.owners.len();
        for (index, owner) in self.owners.iter().enumerate() {
            if !selected.contains(index) {
                continue;
            }
            let unit = if owns_turns {
                owner.acquire_capacity()
            } else {
                owner.try_acquire()
            };
            let Some(unit) = unit else {
                self.release_units(slot, index);
                return Err(index);
            };
            // This newly acquired unit has only its local reference, so adding
            // the carrier reference cannot overflow. Its local drop leaves one.
            owner.slots[unit.slot / 64].references[unit.slot % 64].fetch_add(1, Ordering::Relaxed);
            self.units[first_unit + index].store(unit.slot, Ordering::Relaxed);
        }
        Ok(self.finish_capture(slot))
    }

    fn finish_capture(self: &Arc<Self>, index: usize) -> AdaptivePermitSet {
        // Transfer the private pool lease to the carrier. Only the local
        // permit owns the new slot; its drop below leaves one retained reference.
        self.capacity.slots[index / 64].references[index % 64].fetch_add(1, Ordering::Relaxed);
        AdaptivePermitSet {
            owner: self.clone(),
            slot: index,
        }
    }

    fn release_units(&self, slot: usize, count: usize) {
        let first_unit = slot * self.owners.len();
        for (index, owner) in self.owners[..count].iter().enumerate() {
            let unit = self.units[first_unit + index].load(Ordering::Relaxed);
            if unit != NO_UNIT {
                owner.release_reference(unit);
                self.units[first_unit + index].store(NO_UNIT, Ordering::Relaxed);
            }
        }
    }
}

impl Clone for AdaptivePermitSet {
    fn clone(&self) -> Self {
        // Every carrier has a strong owner reference, so Arc's overflow guard
        // also bounds the per-slot carrier count.
        let owner = self.owner.clone();
        let slot = self.slot;
        owner.capacity.slots[slot / 64].references[slot % 64].fetch_add(1, Ordering::Relaxed);
        Self { owner, slot }
    }
}

impl std::fmt::Debug for AdaptivePermitSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let first_unit = self.slot * self.owner.owners.len();
        let resources = self.owner.units[first_unit..first_unit + self.owner.owners.len()]
            .iter()
            .filter(|unit| unit.load(Ordering::Relaxed) != NO_UNIT)
            .count();
        f.debug_struct("AdaptivePermitSet")
            .field("resources", &resources)
            .field("slot", &self.slot)
            .finish()
    }
}

impl Drop for AdaptivePermitSet {
    fn drop(&mut self) {
        // Return physical ownership before the carrier row can be reused.
        self.owner
            .capacity
            .release_reference_with(self.slot, || self.owner.release_units(self.slot, self.owner.owners.len()));
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Waker};
    use std::time::Duration;

    use super::*;

    fn maximum(value: u16) -> NonZeroU16 {
        NonZeroU16::new(value).expect("positive test capacity")
    }

    fn gate(limit: u16) -> Arc<AdaptiveAdmission> {
        AdaptiveAdmission::new(maximum(limit), u32::from(limit))
    }

    fn node(limit: u16) -> Arc<NodeAdmission> {
        let node = NodeAdmission::new(maximum(limit));
        node.capacity.set_limit(u32::from(limit));
        node
    }

    fn assert_pending<T>(future: Pin<&mut impl Future<Output = T>>) {
        assert!(future.poll(&mut Context::from_waker(Waker::noop())).is_pending());
    }

    #[test]
    fn auxiliary_resources_join_parent_completion_without_double_charging() {
        let a = gate(1);
        let b = gate(1);
        let node = node(1);
        let topology = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("topology");
        let parent = topology
            .select(std::slice::from_ref(&a))
            .expect("object device")
            .try_acquire_with_node(&node)
            .expect("object admission");
        let native = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("layered metadata disk");
        let attachment = topology.select(&[a.clone(), b.clone()]).expect("metadata attachment");
        let acquired = native
            .try_capture_or_acquire_from(parent.resources(), &attachment)
            .expect("nonblocking auxiliary I/O");
        assert_eq!([a.snapshot().active, b.snapshot().active, node.snapshot().active], [1, 1, 1]);
        let nested = native
            .try_capture_or_acquire_from(&acquired, &attachment)
            .expect("reuse full native row");
        assert_eq!([a.completed(), b.completed(), node.completed()], [Some(0), Some(0), Some(0)]);
        let tail = acquired.clone();
        drop((acquired, nested));
        parent.complete();
        assert_eq!([a.completed(), b.completed(), node.completed()], [Some(1), Some(1), Some(1)]);
        assert_eq!(node.snapshot().active, 0, "native work never owns node scheduling capacity");
        assert_eq!([a.snapshot().active, b.snapshot().active], [1, 1]);
        drop(tail);
        assert_eq!([a.snapshot().active, b.snapshot().active], [0, 0]);
    }

    #[test]
    fn auxiliary_resource_failure_rolls_back_partial_acquisition_and_cancellation_is_not_progress() {
        let a = gate(1);
        let b = gate(1);
        let c = gate(1);
        let node = node(1);
        let topology = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone(), c.clone()]).expect("topology");
        let parent = topology
            .select(std::slice::from_ref(&a))
            .expect("object device")
            .try_acquire_with_node(&node)
            .expect("object admission");
        let native = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone(), c.clone()]).expect("native carrier");
        let attachment = topology.select(&[a.clone(), b.clone(), c.clone()]).expect("attachment");
        c.set_limit(0);
        assert_eq!(
            native
                .try_capture_or_acquire_from(parent.resources(), &attachment)
                .expect_err("last leaf unavailable"),
            CaptureError::Unavailable
        );
        assert_eq!([a.snapshot().active, b.snapshot().active, c.snapshot().active], [1, 0, 0]);
        c.set_limit(1);
        let acquired = native
            .try_capture_or_acquire_from(parent.resources(), &attachment)
            .expect("rollback returns the carrier");
        let tail = acquired.clone();
        drop((parent, acquired));
        assert_eq!([a.completed(), b.completed(), c.completed()], [Some(0), Some(0), Some(0)]);
        b.set_limit(0);
        drop(tail);
        assert_eq!([a.snapshot().active, b.snapshot().active, c.snapshot().active], [0, 0, 0]);
        assert!(b.try_acquire().is_none(), "old native completion cannot undo shrink");
    }

    #[test]
    fn auxiliary_resource_acquisition_rejects_foreign_topology_and_wrong_attachment() {
        let a = gate(1);
        let b = gate(1);
        let node = node(1);
        let topology = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("topology");
        let parent = topology
            .select(std::slice::from_ref(&a))
            .expect("object device")
            .try_acquire_with_node(&node)
            .expect("object admission");
        let native = AdaptivePermitSetOwner::new(maximum(1), vec![b.clone()]).expect("auxiliary disk");
        let replacement = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("new topology");
        for attachment in [
            replacement.select(std::slice::from_ref(&b)).expect("new attachment"),
            topology.select(std::slice::from_ref(&a)).expect("wrong leaves"),
        ] {
            assert_eq!(
                native
                    .try_capture_or_acquire_from(parent.resources(), &attachment)
                    .expect_err("foreign source"),
                CaptureError::ResourceOwner
            );
            assert_eq!([a.snapshot().active, b.snapshot().active], [1, 0]);
        }
        let attachment = topology.select(std::slice::from_ref(&b)).expect("correct attachment");
        let acquired = native
            .try_capture_or_acquire_from(parent.resources(), &attachment)
            .expect("new native unit");
        assert_eq!(
            native
                .try_capture_or_acquire_from(parent.resources(), &attachment)
                .expect_err("full carrier"),
            CaptureError::Full
        );
        drop(acquired);
        assert_eq!(b.snapshot().active, 1, "the parent retains its expanded physical plan");
        parent.complete();
        assert_eq!([a.completed(), b.completed()], [Some(1), Some(1)]);
        assert_eq!(b.snapshot().active, 0);
    }

    #[test]
    fn auxiliary_acquisition_can_extend_a_partial_row_from_its_own_carrier() {
        let a = gate(1);
        let b = gate(1);
        let node = node(1);
        let topology = AdaptivePermitSetOwner::new(maximum(2), vec![a.clone(), b.clone()]).expect("topology");
        let parent = topology
            .select(std::slice::from_ref(&a))
            .expect("object device")
            .try_acquire_with_node(&node)
            .expect("object admission");
        let attachment = topology.select(&[a.clone(), b.clone()]).expect("full layered disk");
        let acquired = topology
            .try_capture_or_acquire_from(parent.resources(), &attachment)
            .expect("additional carrier row");
        assert_eq!([a.snapshot().active, b.snapshot().active], [1, 1]);
        parent.complete();
        assert_eq!([a.completed(), b.completed(), node.completed()], [Some(1), Some(1), Some(1)]);
        drop(acquired);
        assert_eq!([a.snapshot().active, b.snapshot().active], [0, 0]);
    }

    #[test]
    fn auxiliary_extension_never_waits_and_concurrent_borrowers_share_one_added_unit() {
        let a = gate(1);
        let b = gate(1);
        let node = node(1);
        let topology = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("topology");
        let parent = topology
            .select(std::slice::from_ref(&a))
            .expect("object device")
            .try_acquire_with_node(&node)
            .expect("object admission");
        let native = AdaptivePermitSetOwner::new(maximum(8), vec![b.clone()]).expect("auxiliary carrier");
        let attachment = topology.select(std::slice::from_ref(&b)).expect("attachment");
        topology.extending[parent.resources().slot].store(true, Ordering::Release);
        assert_eq!(
            native
                .try_capture_or_acquire_from(parent.resources(), &attachment)
                .expect_err("busy row"),
            CaptureError::Unavailable
        );
        assert_eq!(b.snapshot().active, 0);
        topology.extending[parent.resources().slot].store(false, Ordering::Release);
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let source = parent.resources().clone();
                let native = native.clone();
                let attachment = attachment.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    native.try_capture_or_acquire_from(&source, &attachment)
                })
            })
            .collect();
        let mut retained = Vec::new();
        for thread in threads {
            match thread.join().expect("concurrent extension") {
                Ok(unit) => retained.push(unit),
                Err(CaptureError::Unavailable) => {}
                Err(error) => panic!("unexpected extension failure: {error}"),
            }
        }
        assert!(!retained.is_empty());
        assert_eq!([a.snapshot().active, b.snapshot().active], [1, 1]);
        parent.complete();
        assert_eq!([a.completed(), b.completed()], [Some(1), Some(1)]);
        assert_eq!(a.snapshot().active, 0);
        assert_eq!(b.snapshot().active, 1, "only auxiliary native work remains");
        drop(retained);
        assert_eq!(b.snapshot().active, 0);
    }

    #[test]
    fn selected_union_deduplicates_aliases_and_does_not_charge_an_unselected_device() {
        let a = gate(1);
        let b = gate(1);
        let unrelated = gate(1);
        unrelated.set_limit(0);
        let node = node(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), unrelated.clone(), b.clone()]).expect("owner");
        let mut selected = group.select(&[a.clone(), a.clone()]).expect("aliases");
        selected
            .union_with(&group.select(&[a.clone(), b.clone()]).expect("overlap"))
            .expect("same owner");
        let admitted = selected
            .try_acquire_with_node(&node)
            .expect("unselected closed device cannot block this work");
        assert_eq!(a.snapshot().active, 1, "aliases charge one physical unit");
        assert_eq!(b.snapshot().active, 1);
        assert_eq!(unrelated.snapshot().active, 0);
        assert_eq!(
            group
                .capture_from(admitted.resources())
                .expect_err("partial selection is not the whole owner"),
            CaptureError::ResourceOwner
        );
        let native = AdaptivePermitSetOwner::new(maximum(1), vec![b.clone()]).expect("native subset");
        let retained = native.capture_from(admitted.resources()).expect("only the touched device");
        admitted.complete();
        assert_eq!(a.snapshot().active, 0);
        assert_eq!(b.snapshot().active, 1, "native work survives the async scheduling unit");
        assert_eq!(node.snapshot().active, 0);
        assert_eq!(a.completed(), Some(1));
        assert_eq!(b.completed(), Some(1));
        assert_eq!(unrelated.completed(), Some(0), "completion must not credit an unselected device");
        drop(retained);
        assert_eq!(b.snapshot().active, 0);
        assert!(group.units.iter().all(|unit| unit.load(Ordering::Acquire) == NO_UNIT));
    }

    #[test]
    fn selected_projection_rejects_unadmitted_leaves_and_refunds_its_prefix() {
        let a = gate(1);
        let b = gate(1);
        let node = node(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("owner");
        let selected = group.select(std::slice::from_ref(&a)).expect("selection");
        let admitted = selected.try_acquire_with_node(&node).expect("admitted");
        let native = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("native subset");
        assert_eq!(
            native.capture_from(admitted.resources()).expect_err("b was not admitted"),
            CaptureError::ResourceOwner
        );
        assert_eq!(native.capacity.snapshot().active, 0);
        assert_eq!(a.snapshot().active, 1);
        assert_eq!(b.snapshot().active, 0);
        drop(admitted);
        assert_eq!(a.snapshot().active, 0, "rejected projection did not retain a prefix");
        let whole = group.try_acquire_with_node(&node).expect("carrier remains reusable");
        let retained = native
            .capture_from(whole.resources())
            .expect("whole source now covers both leaves");
        drop((whole, retained));
        assert_eq!(a.snapshot().active, 0);
        assert_eq!(b.snapshot().active, 0);
    }

    #[test]
    fn selected_union_rejects_a_different_owner_without_changing_the_selection() {
        let a = gate(1);
        let b = gate(1);
        b.set_limit(0);
        let node = node(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("owner");
        let replacement = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("replacement owner");
        let mut selected = group.select(std::slice::from_ref(&a)).expect("selection");
        let additional = replacement.select(std::slice::from_ref(&b)).expect("other generation");
        assert_eq!(selected.union_with(&additional), Err(CaptureError::ResourceOwner));
        selected
            .try_acquire_with_node(&node)
            .expect("rejected union left b unselected")
            .complete();
        assert_eq!(a.completed(), Some(1));
        assert_eq!(b.completed(), Some(0));
        assert!(matches!(group.select(&[]), Err(CaptureError::ResourceCount)));
        assert!(matches!(group.select(&[gate(1)]), Err(CaptureError::ResourceOwner)));
    }

    #[tokio::test]
    async fn selected_cancel_returns_only_acquired_turns_across_unselected_gaps() {
        let gates = [gate(1), gate(1), gate(1)];
        let node = node(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), gates.to_vec()).expect("owner");
        let first = &gates[group.turn_order[0]];
        let unrelated = &gates[group.turn_order[1]];
        let last = &gates[group.turn_order[2]];
        last.set_limit(0);
        let held_turn = last.turn.acquire().await.expect("hold later turn");
        let selected = group
            .select(&[first.clone(), last.clone()])
            .expect("non-contiguous selection");
        let mut waiting = Box::pin(selected.acquire_with_node(&node));
        assert_pending(waiting.as_mut());
        assert_eq!(first.turn.available_permits(), 0);
        assert_eq!(unrelated.turn.available_permits(), 1);
        assert_eq!(unrelated.queued.load(Ordering::Acquire), 0);
        assert_eq!(node.snapshot().active, 0);
        assert_eq!(first.snapshot().active, 0);
        assert!(unrelated.try_acquire().is_some(), "unrelated work keeps its fast path");
        drop(waiting);
        assert_eq!(first.turn.available_permits(), 1);
        assert_eq!(last.turn.available_permits(), 0, "cancellation does not return somebody else's turn");
        assert_eq!(group.capacity.snapshot().active, 0);
        assert!(gates.iter().all(|gate| gate.queued.load(Ordering::Acquire) == 0));
        drop(held_turn);
        last.set_limit(1);
        selected
            .acquire_with_node(&node)
            .await
            .expect("cancelled reservations were returned")
            .complete();
        assert_eq!(unrelated.completed(), Some(0));
    }

    #[tokio::test]
    async fn selected_blocked_device_does_not_reserve_the_shared_carrier() {
        let stalled = gate(1);
        let healthy = gate(1);
        stalled.set_limit(0);
        let node = node(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![stalled.clone(), healthy.clone()]).expect("one carrier");
        let blocked = group.select(std::slice::from_ref(&stalled)).expect("blocked device");
        let available = group.select(std::slice::from_ref(&healthy)).expect("healthy device");
        let mut waiting = Box::pin(blocked.acquire_with_node(&node));
        assert_pending(waiting.as_mut());
        available
            .try_acquire_with_node(&node)
            .expect("a device wait cannot monopolize bookkeeping needed by a healthy device")
            .complete();
        drop(waiting);
        assert_eq!(group.capacity.snapshot().active, 0);
        assert_eq!(stalled.snapshot().active, 0);
        assert_eq!(healthy.completed(), Some(1));
    }

    #[tokio::test]
    async fn selected_shrink_during_node_wait_leaves_unrelated_work_available() {
        let a = gate(1);
        let b = gate(1);
        let unrelated = gate(1);
        let node = node(1);
        let group = AdaptivePermitSetOwner::new(maximum(2), vec![a.clone(), unrelated.clone(), b.clone()]).expect("owner");
        let selected = group.select(&[a.clone(), b.clone()]).expect("selection");
        let other = group.select(std::slice::from_ref(&unrelated)).expect("unrelated selection");
        let busy = node.try_acquire().expect("hold node");
        let mut waiting = Box::pin(selected.acquire_with_node(&node));
        assert_pending(waiting.as_mut());
        assert_eq!(a.snapshot().active, 0);
        assert_eq!(b.snapshot().active, 0);
        b.set_limit(0);
        drop(busy);
        assert_pending(waiting.as_mut());
        assert_eq!(node.snapshot().active, 0, "shrink refunds node capacity before the next disk wait");
        other
            .try_acquire_with_node(&node)
            .expect("healthy device is independent")
            .complete();
        b.set_limit(1);
        waiting.await.expect("selected devices recovered").complete();
        assert_eq!(node.completed(), Some(2));
        for gate in [&a, &b, &unrelated] {
            assert_eq!(gate.snapshot().active, 0);
            assert_eq!(gate.completed(), Some(1));
        }
    }

    #[test]
    fn selected_boundary_bits_and_reused_rows_never_retain_previous_devices() {
        let gates: Vec<_> = (0..MAX_OWNERS).map(|_| gate(1)).collect();
        let node = node(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), gates.clone()).expect("maximum owner count");
        let boundaries = [0, 63, 64, 127, 128, 191, 192, 255];
        let selected = group
            .select(&boundaries.map(|index| gates[index].clone()))
            .expect("all mask boundaries");
        let other = group.select(std::slice::from_ref(&gates[1])).expect("different selection");
        for _ in 0..128 {
            selected.try_acquire_with_node(&node).expect("boundary resources").complete();
            other.try_acquire_with_node(&node).expect("reused row").complete();
            assert!(gates.iter().all(|gate| gate.snapshot().active == 0));
        }
        for (index, gate) in gates.iter().enumerate() {
            assert_eq!(gate.completed(), Some(if boundaries.contains(&index) || index == 1 { 128 } else { 0 }));
        }
        assert!(group.units.iter().all(|unit| unit.load(Ordering::Acquire) == NO_UNIT));
    }

    #[tokio::test]
    async fn cancelled_group_wait_returns_all_turns_without_holding_partial_capacity() {
        let a = gate(1);
        let b = gate(1);
        b.set_limit(0);
        let node = node(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("group");
        assert!(group.try_acquire_with_node(&node).is_none());
        let mut pending = Box::pin(group.acquire_with_node(&node));
        assert_pending(pending.as_mut());
        assert_eq!(a.snapshot().active, 0, "partial physical capacity is never held across a wait");
        assert_eq!(b.snapshot().active, 0);
        assert_eq!(node.snapshot().active, 0, "a blocked disk cannot reserve node capacity");
        assert_eq!(group.capacity.snapshot().active, 0, "a physical wait reserves no shared carrier");
        drop(pending);
        assert_eq!(group.capacity.snapshot().active, 0);
        for owner in [&a, &b] {
            assert_eq!(owner.queued.load(Ordering::Acquire), 0);
            assert_eq!(owner.turn.available_permits(), 1);
        }
        assert!(a.try_acquire().is_some());
        b.set_limit(1);
        group
            .try_acquire_with_node(&node)
            .expect("cancel returned every reservation")
            .complete();
        assert_eq!(node.completed(), Some(1));
        assert_eq!(node.snapshot().active, 0);
    }

    #[tokio::test]
    async fn shrink_during_node_wait_refunds_node_before_waiting_again() {
        let a = gate(1);
        let b = gate(1);
        let c = gate(1);
        let node = node(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("group");
        let unrelated = AdaptivePermitSetOwner::new(maximum(1), vec![c.clone()]).expect("unrelated group");
        let busy = node.try_acquire().expect("busy node");
        let mut pending = Box::pin(group.acquire_with_node(&node));
        assert_pending(pending.as_mut());
        assert_eq!(a.snapshot().active, 0);
        assert_eq!(b.snapshot().active, 0);
        b.set_limit(0);
        drop(busy);
        assert_pending(pending.as_mut());
        assert_eq!(a.snapshot().active, 0, "failed acquisition returned its prefix");
        assert_eq!(node.snapshot().active, 0, "node is refunded before waiting on the shrunken leaf");
        unrelated
            .try_acquire_with_node(&node)
            .expect("healthy device can progress")
            .complete();
        assert_eq!(node.completed(), Some(1));
        b.set_limit(1);
        let admitted = tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .expect("group resumes after capacity returns")
            .expect("turns stay open");
        assert_eq!(a.snapshot().active, 1);
        assert_eq!(b.snapshot().active, 1);
        drop(admitted);
        assert_eq!(node.completed(), Some(1), "cancellation does not count completed work");
    }

    #[tokio::test]
    async fn reversed_attachment_order_uses_one_turn_order_and_blocks_fast_bargers() {
        let a = gate(1);
        let b = gate(1);
        let node = node(2);
        let ab = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("ab");
        let ba = AdaptivePermitSetOwner::new(maximum(1), vec![b.clone(), a.clone()]).expect("ba");
        for (ab_index, ba_index) in ab.turn_order.iter().zip(ba.turn_order.iter()) {
            assert!(Arc::ptr_eq(&ab.owners[*ab_index], &ba.owners[*ba_index]));
        }
        let held = b.try_acquire().expect("prior native unit");
        let mut first = Box::pin(ab.acquire_with_node(&node));
        let mut second = Box::pin(ba.acquire_with_node(&node));
        assert_pending(first.as_mut());
        assert_pending(second.as_mut());
        for _ in 0..1_000 {
            assert!(a.try_acquire().is_none(), "fast path cannot steal a queued group's leaf");
            assert!(ba.try_acquire_with_node(&node).is_none());
        }
        drop(held);
        let first = tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .expect("first group progresses")
            .expect("open turns");
        assert_pending(second.as_mut());
        first.complete();
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("reverse group progresses")
            .expect("open turns")
            .complete();
        assert_eq!(node.completed(), Some(2));
        for owner in [&a, &b] {
            assert_eq!(owner.snapshot().active, 0);
            assert_eq!(owner.queued.load(Ordering::Acquire), 0);
            assert_eq!(owner.turn.available_permits(), 1);
        }
    }

    #[tokio::test]
    async fn partial_turn_failure_returns_prior_turns_and_bookkeeping() {
        let a = gate(1);
        let b = gate(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("group");
        let node = node(1);
        let held = node.try_acquire().expect("force slow path");
        group.owners[group.turn_order[1]].turn.close();
        assert!(group.acquire_with_node(&node).await.is_err());
        assert_eq!(group.capacity.snapshot().active, 0);
        assert_eq!(group.owners[group.turn_order[0]].turn.available_permits(), 1);
        for owner in [&a, &b] {
            assert_eq!(owner.queued.load(Ordering::Acquire), 0);
            assert_eq!(owner.snapshot().active, 0);
        }
        drop(held);
        assert_eq!(node.snapshot().active, 0);
    }

    #[tokio::test]
    async fn cancellation_while_waiting_for_a_later_turn_refunds_only_owned_turns() {
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![gate(1), gate(1)]).expect("group");
        let node = node(1);
        let node_busy = node.try_acquire().expect("force slow path");
        let second = &group.owners[group.turn_order[1]];
        let second_busy = second.turn.acquire().await.expect("prior turn holder");
        let mut pending = Box::pin(group.acquire_with_node(&node));
        assert_pending(pending.as_mut());
        drop(pending);
        assert_eq!(group.capacity.snapshot().active, 0);
        assert_eq!(group.owners[group.turn_order[0]].turn.available_permits(), 1);
        assert_eq!(second.turn.available_permits(), 0, "another holder's turn must not be refunded");
        for owner in &group.owners {
            assert_eq!(owner.queued.load(Ordering::Acquire), 0);
            assert_eq!(owner.snapshot().active, 0);
        }
        drop((second_busy, node_busy));
        group.acquire_with_node(&node).await.expect("turns recovered").complete();
        assert_eq!(node.completed(), Some(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overlapping_groups_survive_concurrent_shrink_cancellation_and_native_tails() {
        let a = gate(2);
        let b = gate(2);
        let node = node(2);
        let ab = AdaptivePermitSetOwner::new(maximum(8), vec![a.clone(), b.clone()]).expect("ab");
        let ba = AdaptivePermitSetOwner::new(maximum(8), vec![b.clone(), a.clone()]).expect("ba");
        let native = AdaptivePermitSetOwner::new(maximum(8), vec![b.clone()]).expect("native leaf");
        let mut jobs = tokio::task::JoinSet::new();
        for worker in 0..8 {
            let group = if worker % 2 == 0 { ab.clone() } else { ba.clone() };
            let node = node.clone();
            let native = native.clone();
            jobs.spawn(async move {
                for iteration in 0..100 {
                    let parent = group.acquire_with_node(&node).await.expect("open turns");
                    let tail = native.capture_from(parent.resources()).expect("native subset");
                    if iteration % 2 == 0 {
                        parent.complete();
                    } else {
                        drop(parent);
                    }
                    tokio::task::yield_now().await;
                    drop(tail);
                }
            });
        }
        let resized = b.clone();
        jobs.spawn(async move {
            for _ in 0..500 {
                resized.set_limit(0);
                tokio::task::yield_now().await;
                resized.set_limit(2);
                tokio::task::yield_now().await;
            }
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(result) = jobs.join_next().await {
                result.expect("workers finish without panic");
            }
        })
        .await
        .expect("all groups retain progress under finite resizes and tails");
        assert_eq!(node.completed(), Some(400), "cancelled operations never count as completed");
        assert_eq!(node.snapshot().active, 0);
        for owner in [&a, &b] {
            assert_eq!(owner.snapshot().active, 0);
            assert_eq!(owner.queued.load(Ordering::Acquire), 0);
            assert_eq!(owner.turn.available_permits(), 1);
        }
        for group in [&ab, &ba, &native] {
            assert_eq!(group.capacity.snapshot().active, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expired_leaf_after_node_wait_does_not_use_its_old_capacity() {
        let physical = gate(1);
        physical.set_sampled_limit(1, Duration::from_secs(1));
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![physical.clone()]).expect("group");
        let node = node(1);
        let busy = node.try_acquire().expect("busy node");
        let mut pending = Box::pin(group.acquire_with_node(&node));
        assert_pending(pending.as_mut());
        tokio::time::advance(Duration::from_secs(2)).await;
        drop(busy);
        assert_pending(pending.as_mut());
        assert_eq!(physical.snapshot().active, 0);
        assert_eq!(node.snapshot().active, 0);
        physical.set_sampled_limit(1, Duration::from_secs(1));
        pending.await.expect("renewal resumes admission").complete();
        assert_eq!(node.completed(), Some(1));
    }

    #[test]
    fn projected_native_alias_retains_only_its_leaf_after_parent_cancellation() {
        let a = gate(1);
        let b = gate(1);
        let node = node(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("group");
        let healthy = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone()]).expect("healthy");
        let source = AdaptivePermitSetOwner::new(maximum(1), vec![b.clone()]).expect("source");
        let target = AdaptivePermitSetOwner::new(maximum(1), vec![b.clone()]).expect("target alias");
        let parent = group.try_acquire_with_node(&node).expect("whole group");
        let read = source.capture_from(parent.resources()).expect("native source");
        let write = target.capture_from(parent.resources()).expect("target shares physical unit");
        assert_eq!(b.snapshot().active, 1);
        drop(parent);
        assert_eq!(node.snapshot().active, 0);
        assert_eq!(node.completed(), Some(0));
        assert_eq!(a.snapshot().active, 0);
        healthy
            .try_acquire_with_node(&node)
            .expect("healthy device resumes")
            .complete();
        b.set_limit(0);
        drop(read);
        assert_eq!(b.snapshot().active, 1, "target native I/O still owns the original unit");
        drop(write);
        assert_eq!(b.snapshot().active, 0);
        assert!(b.try_acquire().is_none(), "native completion cannot reopen an expired or closed owner");
    }

    #[test]
    fn identical_native_subset_reuses_the_parent_carrier_at_capacity_one() {
        let physical = gate(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![physical.clone()]).expect("single disk plan");
        let node = node(1);
        let parent = group.try_acquire_with_node(&node).expect("parent owns the sole carrier");
        let native = group
            .capture_from(parent.resources())
            .expect("same subset shares its carrier");
        drop(parent);
        assert_eq!(node.snapshot().active, 0);
        assert_eq!(physical.snapshot().active, 1);
        assert!(group.try_acquire_with_node(&node).is_none());
        drop(native);
        assert_eq!(physical.snapshot().active, 0);
        group
            .try_acquire_with_node(&node)
            .expect("native released the shared carrier")
            .complete();
        assert_eq!(node.completed(), Some(1));
    }

    #[test]
    fn projection_rejects_replacement_and_overflow_without_leaking_prefix() {
        let a = gate(1);
        let b = gate(1);
        let replacement = gate(1);
        let node = node(1);
        let group = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("group");
        let parent = group.try_acquire_with_node(&node).expect("parent");
        let wrong = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), replacement]).expect("replacement subset");
        assert_eq!(
            wrong.capture_from(parent.resources()).expect_err("wrong incarnation"),
            CaptureError::ResourceOwner
        );
        assert_eq!(wrong.capacity.snapshot().active, 0);
        let same = AdaptivePermitSetOwner::new(maximum(1), vec![a.clone(), b.clone()]).expect("same subset");
        let b_slot = group.units[parent.resources.slot * 2 + 1].load(Ordering::Relaxed);
        let count = &b.slots[b_slot / 64].references[b_slot % 64];
        count.store(MAX_PERMIT_REFERENCES, Ordering::Relaxed);
        assert_eq!(
            same.capture_from(parent.resources()).expect_err("overflow"),
            CaptureError::ReferenceOverflow
        );
        assert_eq!(count.load(Ordering::Relaxed), MAX_PERMIT_REFERENCES);
        count.store(1, Ordering::Relaxed);
        let captured = same
            .capture_from(parent.resources())
            .expect("rejected projection freed carrier");
        assert_eq!(same.capture_from(parent.resources()).expect_err("live carrier"), CaptureError::Full);
        drop((parent, captured));
        assert_eq!(a.snapshot().active, 0, "both rejected prefixes were refunded");
        assert_eq!(b.snapshot().active, 0);
        assert_eq!(node.snapshot().active, 0);
    }

    #[test]
    fn native_subset_releases_healthy_resources_after_parent_cancellation() {
        let slow = gate(1);
        let healthy = gate(1);
        let walk = AdaptivePermitSetOwner::new(maximum(1), vec![slow.clone(), healthy.clone()]).expect("walk subset");
        let native = AdaptivePermitSetOwner::new(maximum(1), vec![slow.clone()]).expect("native subset");
        let slow_unit = slow.try_acquire().expect("slow unit");
        let healthy_unit = healthy.try_acquire().expect("healthy unit");
        let parent = walk.capture(&[&slow_unit, &healthy_unit]).expect("parent ownership");
        let tail = native.capture(&[&slow_unit]).expect("native ownership");
        drop((slow_unit, healthy_unit, parent, walk, native));
        assert_eq!(healthy.snapshot().active, 0, "native work must retain only devices it touches");
        assert!(healthy.try_acquire().is_some(), "healthy work resumes after caller cancellation");
        assert!(slow.try_acquire().is_none(), "native tail remains charged");
        let last = tail.clone();
        drop(tail);
        assert!(slow.try_acquire().is_none(), "the final native reference owns completion");
        drop(last);
        assert_eq!(slow.snapshot().active, 0);
    }

    #[test]
    fn source_and_target_aliases_share_one_admission_without_waiting_again() {
        let physical = gate(1);
        let source = AdaptivePermitSetOwner::new(maximum(1), vec![physical.clone()]).expect("source subset");
        let target = AdaptivePermitSetOwner::new(maximum(1), vec![physical.clone()]).expect("target subset");
        let admitted = physical.try_acquire().expect("one physical unit");
        let read = source.capture(&[&admitted]).expect("source shares admission");
        let write = target.capture(&[&admitted]).expect("target shares admission at capacity one");
        assert_eq!(physical.snapshot().active, 1, "aliases do not create independent capacity");
        drop((admitted, read));
        assert!(physical.try_acquire().is_none(), "target native work is still active");
        drop(write);
        assert!(physical.try_acquire().is_some());
    }

    #[test]
    fn rejects_wrong_owner_and_count_without_leaking_partial_references_or_carrier_slots() {
        let first = gate(1);
        let second = gate(1);
        let replacement = gate(1);
        let carrier = AdaptivePermitSetOwner::new(maximum(1), vec![first.clone(), second.clone()]).expect("subset");
        let a = first.try_acquire().expect("first unit");
        let b = second.try_acquire().expect("second unit");
        let changed = replacement.try_acquire().expect("replacement unit");
        assert_eq!(carrier.capture(&[&a]).expect_err("wrong count"), CaptureError::ResourceCount);
        assert_eq!(
            carrier.capture(&[&a, &changed]).expect_err("wrong incarnation"),
            CaptureError::ResourceOwner
        );
        let valid = carrier
            .capture(&[&a, &b])
            .expect("rejected attempt returned its carrier slot");
        drop((a, b, changed, valid));
        assert_eq!(first.snapshot().active, 0, "the rejected prefix was fully refunded");
        assert_eq!(second.snapshot().active, 0);
        assert_eq!(replacement.snapshot().active, 0);
    }

    #[test]
    fn carrier_pool_does_not_recycle_a_live_slot_and_shrink_preserves_original_units() {
        let physical = gate(2);
        let carrier = AdaptivePermitSetOwner::new(maximum(1), vec![physical.clone()]).expect("subset");
        let a = physical.try_acquire().expect("first unit");
        let b = physical.try_acquire().expect("second unit");
        let held = carrier.capture(&[&a]).expect("one carrier slot");
        let tail = held.clone();
        drop((a, held));
        physical.set_limit(0);
        assert_eq!(physical.snapshot().active, 2, "shrinking never revokes held units");
        assert_eq!(carrier.capture(&[&b]).expect_err("tail keeps its carrier slot"), CaptureError::Full);
        drop(tail);
        let reused = carrier.capture(&[&b]).expect("last drop recycles carrier slot");
        drop((b, reused));
        assert_eq!(physical.snapshot().active, 0);
        assert!(physical.try_acquire().is_none(), "returning old work does not reopen a closed owner");
    }

    #[test]
    fn reference_overflow_refunds_prior_resources() {
        let first = gate(1);
        let second = gate(1);
        let carrier = AdaptivePermitSetOwner::new(maximum(1), vec![first.clone(), second.clone()]).expect("subset");
        let a = first.try_acquire().expect("first unit");
        let b = second.try_acquire().expect("second unit");
        let count = &second.slots[b.slot / 64].references[b.slot % 64];
        count.store(MAX_PERMIT_REFERENCES, Ordering::Relaxed);
        let error = carrier
            .capture(&[&a, &b])
            .expect_err("reject overflow before changing the count");
        assert_eq!(count.load(Ordering::Relaxed), MAX_PERMIT_REFERENCES);
        count.store(1, Ordering::Relaxed);
        assert_eq!(error, CaptureError::ReferenceOverflow);
        drop((a, b));
        assert_eq!(first.snapshot().active, 0, "first unit was refunded when second overflowed");
        assert_eq!(second.snapshot().active, 0);
    }

    #[test]
    fn final_representable_capture_is_released_without_wrapping_the_count() {
        let physical = gate(1);
        let carrier = AdaptivePermitSetOwner::new(maximum(1), vec![physical.clone()]).expect("subset");
        let unit = physical.try_acquire().expect("original unit");
        let count = &physical.slots[unit.slot / 64].references[unit.slot % 64];
        count.store(MAX_PERMIT_REFERENCES - 1, Ordering::Relaxed);
        let retained = carrier.capture(&[&unit]).expect("one remaining reference");
        assert_eq!(count.load(Ordering::Relaxed), MAX_PERMIT_REFERENCES);
        drop(retained);
        assert_eq!(count.load(Ordering::Relaxed), MAX_PERMIT_REFERENCES - 1);
        count.store(1, Ordering::Relaxed);
        drop(unit);
        assert_eq!(physical.snapshot().active, 0);
    }

    #[test]
    fn rejects_empty_duplicate_and_unbounded_resource_sets() {
        let physical = gate(1);
        assert!(AdaptivePermitSetOwner::new(maximum(1), vec![]).is_err());
        assert!(AdaptivePermitSetOwner::new(maximum(1), vec![physical.clone(), physical]).is_err());
        assert!(AdaptivePermitSetOwner::new(maximum(1), (0..257).map(|_| gate(1)).collect()).is_err());
    }

    #[test]
    fn concurrent_capture_and_native_release_reuse_slots_without_refunding_live_work() {
        let physical = gate(8);
        let carrier = AdaptivePermitSetOwner::new(maximum(65), vec![physical.clone()]).expect("cross-word slots");
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let carrier = carrier.clone();
                let unit = physical.try_acquire().expect("one unit per thread");
                scope.spawn(move || {
                    for _ in 0..1_000 {
                        let lease = carrier.capture(&[&unit]).expect("independent carrier slots");
                        let native = lease.clone();
                        drop(lease);
                        assert_ne!(unit.owner.snapshot().active, 0, "live original cannot be refunded");
                        drop(native);
                    }
                });
            }
        });
        assert_eq!(physical.snapshot().active, 0);
        assert_eq!(carrier.capacity.snapshot().active, 0);
        let units: Vec<_> = (0..8)
            .map(|_| physical.try_acquire().expect("all resource slots returned"))
            .collect();
        assert!(physical.try_acquire().is_none());
        drop(units);
    }
}
