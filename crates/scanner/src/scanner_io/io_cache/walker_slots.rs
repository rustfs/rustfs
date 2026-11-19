// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::NamespaceScannerWalker;
use crate::scanner_budget::ScannerCycleBudget;
use crate::scanner_io::current_local_scanner_disk;
use crate::{Disk, SetDisks};
use sha2::{Digest as _, Sha256};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// All state belonging to one physical walker travels with it, even when a
/// different queue consumer makes the next request on that endpoint.
pub(super) struct WalkerSlot<T> {
    pub(super) walker: NamespaceScannerWalker<T>,
    pub(super) remote_session_id: Uuid,
    pub(super) remote_session_sequence: u64,
    endpoint: String,
}

impl WalkerSlot<Arc<Disk>> {
    pub(super) async fn bind_local_disk(
        &mut self,
        set: &SetDisks,
        ctx: &CancellationToken,
        budget: &ScannerCycleBudget,
    ) -> Option<Arc<Disk>> {
        let current = current_local_scanner_disk(set, &self.walker.disk, ctx, budget).await?;
        // Parked readers belong to the verified wrapper, including after a same-disk reopen.
        self.walker.disk = Arc::clone(&current);
        Some(current)
    }
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

    #[tokio::test]
    #[serial_test::serial]
    async fn rebound_local_walker_follows_parked_readers_and_checks_format_identity() {
        use crate::scanner_io::{scanner_set_disk_inventory, tests::setup_local_scanner_store};
        use crate::storage_api::owner::EcstoreDiskAPI as _;
        use crate::{DiskOption, new_disk};

        let (_dir, store) = setup_local_scanner_store(1, 4).await;
        let set = &store.pools[0].disk_set[0];
        let walkers = scanner_set_disk_inventory(set)
            .await
            .into_iter()
            .map(|disk| NamespaceScannerWalker {
                disk,
                mode: NamespaceScannerWorkerMode::Coordinator,
                local_admission: None,
            })
            .collect::<Vec<_>>();
        let slots = AvailableWalkers::new(walkers, |disk| disk.endpoint().to_string());
        let preferred = slots.take_for_bucket("wide", |_| false).expect("preferred endpoint");
        let mut fallback = slots.take_for_bucket("wide", |_| false).expect("fallback endpoint");
        let old = &fallback.walker.disk;
        let index = old.get_disk_location().disk_idx.expect("formatted slot");
        let reopened = new_disk(
            &old.endpoint(),
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("reopen the same physical disk");
        old.close().await.expect("retire previous wrapper");
        set.disks.write().await[index] = Some(reopened.clone());

        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, Default::default());
        let bound = fallback.bind_local_disk(set, &ctx, &budget).await.expect("same UUID");
        assert!(Arc::ptr_eq(&bound, &reopened));
        slots.release(fallback);
        slots.release(preferred);
        let mut parked = slots
            .take_for_bucket("wide", |disk| Arc::ptr_eq(disk, &reopened))
            .expect("available parked walker");
        assert!(Arc::ptr_eq(&parked.walker.disk, &reopened));

        let mut changed_format = set.as_ref().clone();
        changed_format.format.erasure.sets[0][index] = Uuid::new_v4();
        assert!(parked.bind_local_disk(&changed_format, &ctx, &budget).await.is_none());
    }
}
