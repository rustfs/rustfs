// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::num::NonZeroU16;
use std::sync::{Arc, Weak};
#[cfg(test)]
use std::time::Duration;

use rustfs_concurrency::adaptive::AdaptiveAdmission;
#[cfg(test)]
use rustfs_concurrency::adaptive::{AdaptiveAdmissionSnapshot, AdaptivePermit};

use super::physical_identity::PhysicalDeviceIdentity;

const MAX_PHYSICAL_OWNERS: usize = 256;

/// Resolve aliases outside object work. The sampling owner serializes changes;
/// attached handles and native permits never consult this registry.
pub(super) struct PhysicalOwnerRegistry {
    maximum: NonZeroU16,
    owners: BTreeMap<PhysicalDeviceIdentity, Weak<AdaptiveAdmission>>,
}

/// One attachment to an existing physical capacity owner, not a reservation.
/// The gate itself owns permit slots, so native permit clones keep a registry
/// entry alive even after the last logical disk attachment disappears.
pub(super) struct PhysicalDeviceAdmission {
    #[cfg(test)]
    identity: PhysicalDeviceIdentity,
    gate: Arc<AdaptiveAdmission>,
}

impl PhysicalOwnerRegistry {
    pub(super) fn new(maximum: NonZeroU16) -> Self {
        Self {
            maximum,
            owners: BTreeMap::new(),
        }
    }

    pub(super) fn bind(&mut self, identities: &[PhysicalDeviceIdentity]) -> io::Result<Vec<PhysicalDeviceAdmission>> {
        if identities.is_empty() || identities.len() > MAX_PHYSICAL_OWNERS || identities.iter().any(|id| id.generation == 0) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid physical capacity identities"));
        }
        let identities = identities.iter().copied().collect::<BTreeSet<_>>();
        self.owners.retain(|_, owner| owner.strong_count() != 0);
        let new_owners = identities
            .iter()
            .filter(|identity| !self.owners.contains_key(identity))
            .count();
        if new_owners > MAX_PHYSICAL_OWNERS - self.owners.len() {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "physical capacity registry is full"));
        }
        Ok(identities
            .into_iter()
            .map(|identity| {
                let gate = self.owners.get(&identity).and_then(Weak::upgrade).unwrap_or_else(|| {
                    let gate = AdaptiveAdmission::new(self.maximum, 0);
                    self.owners.insert(identity, Arc::downgrade(&gate));
                    gate
                });
                PhysicalDeviceAdmission {
                    #[cfg(test)]
                    identity,
                    gate,
                }
            })
            .collect())
    }

    /// Only the sampling owner publishes capacity. New or expired identities
    /// remain closed until a fresh observation establishes a bounded limit.
    #[cfg(test)]
    pub(super) fn publish(&mut self, identity: PhysicalDeviceIdentity, limit: u32, valid_for: Duration) -> bool {
        let Some(gate) = self.owners.get(&identity).and_then(Weak::upgrade) else {
            return false;
        };
        gate.set_sampled_limit(limit, valid_for);
        true
    }
}

impl PhysicalDeviceAdmission {
    pub(super) fn gate(&self) -> &Arc<AdaptiveAdmission> {
        &self.gate
    }

    #[cfg(test)]
    pub(super) fn identity(&self) -> PhysicalDeviceIdentity {
        self.identity
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> AdaptiveAdmissionSnapshot {
        self.gate.snapshot()
    }

    #[cfg(test)]
    pub(super) fn try_acquire(&self) -> Option<AdaptivePermit> {
        self.gate.try_acquire()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn device(minor: u32, generation: u64) -> PhysicalDeviceIdentity {
        PhysicalDeviceIdentity {
            major: 259,
            minor,
            generation,
        }
    }

    fn registry() -> PhysicalOwnerRegistry {
        PhysicalOwnerRegistry::new(NonZeroU16::new(2).expect("two units"))
    }

    #[test]
    fn aliases_and_overlapping_stacks_share_one_physical_capacity() {
        let mut registry = registry();
        let a = device(1, 11);
        let b = device(2, 12);
        let single = registry.bind(&[a]).expect("single device");
        let stack = registry.bind(&[b, a, a]).expect("overlapping stack");
        assert_eq!(stack.len(), 2, "duplicates cannot reserve additional units");
        assert_eq!(stack[0].identity(), a);
        assert!(single[0].try_acquire().is_none(), "unobserved capacity is closed");
        assert!(registry.publish(a, 1, Duration::from_secs(60)));
        let held = single[0].try_acquire().expect("physical unit");
        assert!(stack[0].try_acquire().is_none(), "the alias cannot double the capacity");
        assert_eq!(stack[0].snapshot().active, 1);
        drop(held);
        assert!(stack[0].try_acquire().is_some());
    }

    #[test]
    fn native_tail_keeps_the_owner_alive_after_all_attachments_are_gone() {
        let mut registry = registry();
        let id = device(1, 11);
        let attachment = registry.bind(&[id]).expect("logical disk");
        assert!(registry.publish(id, 1, Duration::from_secs(60)));
        let waiter = attachment[0].try_acquire().expect("admitted work");
        let native = waiter.clone();
        drop(waiter);
        drop(attachment);
        let reconnected = registry.bind(&[id]).expect("reconnect during native I/O");
        assert_eq!(reconnected[0].snapshot().active, 1);
        assert!(reconnected[0].try_acquire().is_none());
        drop(native);
        assert!(reconnected[0].try_acquire().is_some(), "only the final native release returns capacity");
    }

    #[test]
    fn replacement_generation_does_not_receive_old_native_refunds() {
        let mut registry = registry();
        let old_id = device(1, 11);
        let new_id = device(1, 12);
        let old = registry.bind(&[old_id]).expect("old incarnation");
        assert!(registry.publish(old_id, 1, Duration::from_secs(60)));
        let native = old[0].try_acquire().expect("old native I/O");
        drop(old);
        let new = registry.bind(&[new_id]).expect("replacement incarnation");
        assert_eq!(new[0].snapshot().active, 0);
        assert!(new[0].try_acquire().is_none());
        assert!(registry.publish(new_id, 1, Duration::from_secs(60)));
        let current = new[0].try_acquire().expect("new work");
        drop(native);
        assert_eq!(new[0].snapshot().active, 1, "old completion cannot refund the replacement");
        assert!(new[0].try_acquire().is_none());
        drop(current);
        assert_eq!(new[0].snapshot().active, 0);
    }

    #[test]
    fn shrink_and_expiry_preserve_active_units_and_leave_other_disks_available() {
        let mut registry = registry();
        let a = device(1, 11);
        let b = device(2, 12);
        let disks = registry.bind(&[a, b]).expect("two physical disks");
        assert!(registry.publish(a, 2, Duration::from_secs(60)));
        assert!(registry.publish(b, 1, Duration::from_secs(60)));
        let first = disks[0].try_acquire().expect("first unit");
        let second = disks[0].try_acquire().expect("second unit");
        assert!(registry.publish(a, 1, Duration::from_secs(60)));
        assert_eq!(disks[0].snapshot().active, 2);
        assert!(disks[0].try_acquire().is_none());
        assert!(disks[1].try_acquire().is_some(), "a stalled disk cannot consume another disk's capacity");
        assert!(registry.publish(a, 2, Duration::ZERO));
        assert_eq!(disks[0].snapshot().limit, 0);
        assert_eq!(disks[0].snapshot().active, 2);
        drop((first, second));
        assert!(disks[0].try_acquire().is_none(), "expired observations do not reopen after completion");
    }

    #[test]
    fn registry_bound_preserves_live_native_owners_and_reclaims_only_dead_entries() {
        let mut registry = registry();
        let ids = (0..MAX_PHYSICAL_OWNERS)
            .map(|minor| device(u32::try_from(minor).expect("bounded minor"), 1))
            .collect::<Vec<_>>();
        let attachments = registry.bind(&ids).expect("exact registry bound");
        let extra = device(999, 1);
        assert!(matches!(registry.bind(&[extra]), Err(error) if error.kind() == io::ErrorKind::WouldBlock));
        assert_eq!(registry.owners.len(), MAX_PHYSICAL_OWNERS, "failed binding must not add partial owners");
        assert!(registry.publish(ids[0], 1, Duration::from_secs(60)));
        let native = attachments[0].try_acquire().expect("native tail");
        drop(attachments);
        let replacement = registry.bind(&[extra]).expect("dead owners reclaimed");
        assert_eq!(registry.owners.len(), 2, "the native tail still owns its original entry");
        let original = registry.bind(&[ids[0]]).expect("original native owner");
        assert_eq!(original[0].snapshot().active, 1);
        drop((native, original, replacement));
        assert!(!registry.publish(extra, 1, Duration::from_secs(60)), "a dead owner cannot be refreshed");
        assert!(registry.bind(&[]).is_err());
        assert!(registry.bind(&[device(1, 0)]).is_err());
    }
}
