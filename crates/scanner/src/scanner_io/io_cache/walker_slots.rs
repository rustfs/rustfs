// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::NamespaceScannerWalker;
use sha2::{Digest as _, Sha256};
use std::sync::Mutex;
use uuid::Uuid;

/// All state belonging to one physical walker travels with it, even when a
/// different queue consumer makes the next request on that endpoint.
pub(super) struct WalkerSlot<T> {
    pub(super) walker: NamespaceScannerWalker<T>,
    pub(super) remote_session_id: Uuid,
    pub(super) remote_session_sequence: u64,
    endpoint: String,
}

pub(super) struct AvailableWalkers<T>(Mutex<Vec<WalkerSlot<T>>>);

impl<T> AvailableWalkers<T> {
    pub(super) fn new(walkers: Vec<NamespaceScannerWalker<T>>, endpoint: impl Fn(&T) -> String) -> Self {
        Self(Mutex::new(
            walkers
                .into_iter()
                .map(|walker| WalkerSlot {
                    endpoint: endpoint(&walker.disk),
                    walker,
                    remote_session_id: Uuid::new_v4(),
                    remote_session_sequence: 0,
                })
                .collect(),
        ))
    }

    /// Rank available physical endpoints independently of random inventory
    /// order, waiter wakeups or the number of walker slots on a local disk.
    pub(super) fn take_for_bucket(&self, bucket: &str, parked: impl Fn(&T) -> bool) -> Option<WalkerSlot<T>> {
        let mut idle = self.0.lock().unwrap_or_else(|err| err.into_inner());
        let mut prefix = Sha256::new();
        prefix.update(bucket.as_bytes());
        prefix.update([0]);
        let index = idle
            .iter()
            .enumerate()
            .map(|(index, slot)| {
                let rank = prefix.clone().chain_update(slot.endpoint.as_bytes()).finalize();
                (parked(&slot.walker.disk), rank, slot.endpoint.as_str(), index)
            })
            .max()
            .map(|(_, _, _, index)| index)?;
        Some(idle.swap_remove(index))
    }

    pub(super) fn release(&self, slot: WalkerSlot<T>) {
        self.0.lock().unwrap_or_else(|err| err.into_inner()).push(slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner_io::NamespaceScannerWorkerMode;

    fn pool(endpoints: &[&'static str]) -> AvailableWalkers<&'static str> {
        let walkers = endpoints
            .iter()
            .copied()
            .map(|disk| NamespaceScannerWalker {
                disk,
                mode: NamespaceScannerWorkerMode::Coordinator,
                local_admission: None,
            })
            .collect();
        AvailableWalkers::new(walkers, |disk| (*disk).to_owned())
    }

    #[test]
    fn bucket_resumes_on_same_free_endpoint_despite_reordering_or_borrower_changes() {
        let first = pool(&["disk-c", "disk-a", "disk-b"]);
        let reordered = pool(&["disk-b", "disk-c", "disk-a"]);
        for bucket in ["wide", "documents", "snapshots", "videos"] {
            let mut preferred = first.take_for_bucket(bucket, |_| false).expect("preferred walker");
            let another = reordered
                .take_for_bucket(bucket, |_| false)
                .expect("reordered preferred walker");
            assert_eq!(preferred.endpoint, another.endpoint);
            let next = first
                .take_for_bucket(bucket, |_| false)
                .expect("fallback when primary is busy");
            assert_ne!(preferred.endpoint, next.endpoint);
            let alternative = next.endpoint.clone();
            let id = preferred.remote_session_id;
            preferred.remote_session_sequence += 1;
            let sequence = preferred.remote_session_sequence;
            first.release(preferred);
            let resumed = first.take_for_bucket(bucket, |_| false).expect("released primary");
            assert_eq!((resumed.remote_session_id, resumed.remote_session_sequence), (id, sequence));
            first.release(resumed);
            first.release(next);
            let parked = first
                .take_for_bucket(bucket, |disk| *disk == alternative)
                .expect("parked directory");
            assert_eq!(parked.endpoint, alternative, "follow a parked stream on a previous fallback disk");
            first.release(parked);
            reordered.release(another);
        }
    }

    #[test]
    fn retired_slot_leaves_same_endpoint_alternative_and_does_not_leak() {
        let slots = pool(&["disk-a", "disk-a"]);
        let retiring = slots.take_for_bucket("wide", |_| false).expect("first walker");
        let survivor = slots.take_for_bucket("wide", |_| false).expect("second local walker");
        assert!(slots.take_for_bucket("wide", |_| false).is_none());
        assert_ne!(retiring.remote_session_id, survivor.remote_session_id);
        let id = survivor.remote_session_id;
        drop(retiring);
        slots.release(survivor);
        assert_eq!(
            slots
                .take_for_bucket("wide", |_| false)
                .expect("surviving walker")
                .remote_session_id,
            id
        );
        assert!(slots.take_for_bucket("wide", |_| false).is_none());
    }
}
