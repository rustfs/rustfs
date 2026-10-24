// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Bounded, allocation-free ownership of already admitted resource subsets.

use std::io;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{AdaptiveAdmission, AdaptivePermit, MAX_PERMIT_REFERENCES};

/// Matches the bounded physical-device topology. Construct owners during disk
/// attachment, before object work; this does not acquire or discover resources.
const MAX_OWNERS: usize = 256;

/// A stable subset of resource owners, such as the physical leaves backing one
/// logical disk. Slots are allocated once and reused after their final clone.
/// Different subsets can retain the same admitted unit without charging it twice.
pub struct AdaptivePermitSetOwner {
    owners: Box<[Arc<AdaptiveAdmission>]>,
    capacity: Arc<AdaptiveAdmission>,
    units: Box<[AtomicUsize]>,
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
    /// Another owner cannot be retained without overflowing its reference count.
    ReferenceOverflow,
}

impl std::fmt::Display for CaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ResourceCount => "resource unit count does not match the subset",
            Self::ResourceOwner => "resource unit owner does not match the subset",
            Self::Full => "resource subset slots are exhausted",
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

impl AdaptivePermitSetOwner {
    /// Reserve a bounded carrier pool outside object work. `maximum` bounds
    /// concurrent captured subsets, including native tails. Resource admission
    /// itself remains the caller's responsibility, before storage locks.
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
        units.extend((0..count * owners.len()).map(|_| AtomicUsize::new(0)));
        Ok(Arc::new(Self {
            owners: owners.into_boxed_slice(),
            capacity: AdaptiveAdmission::new(maximum, u32::from(maximum.get())),
            units: units.into_boxed_slice(),
        }))
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
        // Transfer the private pool lease to the carrier. Only this local
        // permit owns the new slot; its drop below leaves one retained reference.
        self.capacity.slots[index / 64].references[index % 64].fetch_add(1, Ordering::Relaxed);
        Ok(AdaptivePermitSet {
            owner: self.clone(),
            slot: index,
        })
    }

    fn release_units(&self, slot: usize, count: usize) {
        let first_unit = slot * self.owners.len();
        for (index, owner) in self.owners[..count].iter().enumerate() {
            owner.release_reference(self.units[first_unit + index].load(Ordering::Relaxed));
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
        f.debug_struct("AdaptivePermitSet")
            .field("resources", &self.owner.owners.len())
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
    use super::*;

    fn maximum(value: u16) -> NonZeroU16 {
        NonZeroU16::new(value).expect("positive test capacity")
    }

    fn gate(limit: u16) -> Arc<AdaptiveAdmission> {
        AdaptiveAdmission::new(maximum(limit), u32::from(limit))
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
