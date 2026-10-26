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

use super::*;
use crate::heal::storage::HealListItem;
use crate::heal::storage_api::{
    EcstoreEndpointServerPools as EndpointServerPools, EcstoreEndpoints as Endpoints, EcstorePoolEndpoints as PoolEndpoints,
    ecstore_init_local_disks as init_local_disks,
};
use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
};

const SET: &str = "pool_0_set_0";
const SCAN_INTERVAL: Duration = Duration::from_secs(10);

struct ScannerStorage {
    survivor: DiskStore,
    /// Additional survivors exposed through the same set inventory.
    sibling_survivors: Vec<DiskStore>,
    identity: ReplacementTargetIdentity,
    inventory_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl HealStorageAPI for ScannerStorage {
    async fn replacement_intent_disks(&self, set: &str) -> Result<Vec<DiskStore>> {
        assert_eq!(set, SET, "discovery must use the target's erasure set");
        self.inventory_calls.fetch_add(1, Ordering::SeqCst);
        // This disk is deliberately outside the node's local registry. The
        // mock supplies the same inventory boundary as a remote survivor,
        // while real disk I/O exercises durable intent discovery and loading.
        let mut disks = vec![self.survivor.clone()];
        disks.extend(self.sibling_survivors.iter().cloned());
        Ok(disks)
    }

    async fn replacement_target_identities(&self, targets: &[String]) -> Result<Vec<ReplacementTargetIdentity>> {
        assert_eq!(targets, std::slice::from_ref(&self.identity.endpoint));
        Ok(vec![self.identity.clone()])
    }

    async fn get_object_meta(&self, _: &str, _: &str) -> Result<Option<HealObjectInfo>> {
        Err(Error::other("scanner admission must not inspect objects"))
    }

    async fn ec_decode_rebuild(&self, _: &str, _: &str) -> Result<Vec<u8>> {
        Err(Error::other("scanner admission must not rebuild objects"))
    }

    async fn get_bucket_info(&self, _: &str) -> Result<Option<BucketInfo>> {
        Err(Error::other("scanner admission must not inspect buckets"))
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        Err(Error::other("same-incarnation discovery must reuse durable buckets"))
    }

    async fn list_buckets_for_admin_heal(
        &self,
        _: Option<crate::heal::storage_api::storage::DiskSetSelector>,
    ) -> Result<Vec<BucketInfo>> {
        Err(Error::other("scanner admission must not start admin discovery"))
    }

    async fn object_exists(&self, _: &str, _: &str) -> Result<bool> {
        Err(Error::other("scanner admission must not inspect objects"))
    }

    async fn heal_object(&self, _: &str, _: &str, _: Option<&str>, _: &HealOpts) -> Result<(HealResultItem, Option<Error>)> {
        Err(Error::other("scanner admission must not execute object healing"))
    }

    async fn heal_bucket(&self, _: &str, _: &HealOpts) -> Result<HealResultItem> {
        Err(Error::other("scanner admission must not execute bucket healing"))
    }

    async fn heal_format(&self, _: bool) -> Result<(HealResultItem, Option<Error>)> {
        Err(Error::other("scanner admission must not execute format healing"))
    }

    async fn list_objects_for_heal_page(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        Err(Error::other("scanner admission must not traverse objects"))
    }

    async fn get_disk_for_resume(&self, _: &str) -> Result<DiskStore> {
        Err(Error::other("scanner admission must retain the durable survivor"))
    }
}

struct ScannerFixture {
    temp: TempDir,
    manager: HealManager,
    storage: Arc<ScannerStorage>,
    owner: ResumeManager,
    task_id: String,
    local_targets: Vec<String>,
}

impl Drop for ScannerFixture {
    fn drop(&mut self) {
        self.manager.cancel_token.cancel();
    }
}

impl ScannerFixture {
    async fn new(unformatted_siblings: bool) -> Self {
        let temp = TempDir::new().expect("scanner replacement fixture directory");
        let disk_ids = (0..3).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
        let deployment_id = Uuid::new_v4();
        let mut endpoints = Vec::new();
        for slot in 0..3 {
            let path = temp.path().join(format!("target-{slot}"));
            let metadata_path = path.join(crate::heal::RUSTFS_META_BUCKET);
            std::fs::create_dir_all(&metadata_path).expect("scanner local target metadata directory");
            if !unformatted_siblings || slot == 0 {
                let format = serde_json::json!({
                    "version": "1",
                    "format": "xl",
                    "id": deployment_id,
                    "xl": {
                        "version": "3",
                        "this": disk_ids[slot],
                        "sets": [disk_ids],
                        "distributionAlgo": "SIPMOD+PARITY"
                    }
                });
                std::fs::write(
                    metadata_path.join("format.json"),
                    serde_json::to_vec(&format).expect("scanner format fixture serialization"),
                )
                .expect("scanner formatted target fixture");
            }
            let mut endpoint =
                Endpoint::try_from(path.to_str().expect("scanner target path is UTF-8")).expect("scanner local endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(slot);
            endpoints.push(endpoint);
        }
        let local_targets = endpoints.iter().map(ToString::to_string).collect::<Vec<_>>();
        init_local_disks(EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 3,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "replacement-scanner-recovery-test".into(),
            platform: "test".into(),
        }]))
        .await
        .expect("scanner local registry initialization");

        let survivor = make_manager_resume_disk(&temp, "survivor").await;
        let local_disks = crate::heal::local_disk_map_read().await;
        assert_eq!(local_disks.len(), 3);
        assert!(
            !local_disks.contains_key(&survivor.endpoint().to_string()),
            "the durable anchor must be discoverable only through the set inventory"
        );
        for disk in local_disks.values().flatten() {
            let is_owner = disk.endpoint().to_string() == local_targets[0];
            match disk.get_disk_id().await {
                Ok(Some(id)) => assert!(!id.is_nil()),
                Err(DiskError::UnformattedDisk) => assert!(unformatted_siblings && !is_owner),
                result => panic!("unexpected fixture disk identity: {result:?}"),
            }
            assert_eq!(disk.runtime_state().as_str(), "online");
        }
        drop(local_disks);

        let identity = ReplacementTargetIdentity {
            endpoint: local_targets[0].clone(),
            canonical_path: local_targets[0].clone(),
            physical_device_ids: vec!["replacement-target-0".into()],
            filesystem_identity: "replacement-target-0-incarnation".into(),
        };
        let task_id = ResumeUtils::generate_task_id();
        let owner = ResumeManager::new_replacement_intent(
            survivor.clone(),
            task_id.clone(),
            SET.into(),
            vec!["seeded-bucket".into()],
            vec![local_targets[0].clone()],
            vec![identity.clone()],
        )
        .await
        .expect("durable remote-survivor replacement intent");
        owner
            .mark_replacement_rebuilding(vec![identity.clone()])
            .await
            .expect("durable replacement rebuild phase");
        owner
            .record_replacement_failure(&Error::Storage(EcstoreError::FileNotFound), 1)
            .await
            .expect("durable in-budget FileNotFound failure");
        let storage = Arc::new(ScannerStorage {
            survivor,
            sibling_survivors: Vec::new(),
            identity,
            inventory_calls: AtomicUsize::new(0),
        });
        let manager = HealManager::new_without_root_recovery_for_test(
            storage.clone(),
            Some(HealConfig {
                heal_interval: SCAN_INTERVAL,
                ..HealConfig::default()
            }),
        );
        Self {
            temp,
            manager,
            storage,
            owner,
            task_id,
            local_targets,
        }
    }

    async fn queued_request(&self) -> HealRequest {
        // Let the immediate interval tick cross the timer's millisecond boundary
        // before the ready loop keeps virtual time fixed during filesystem I/O.
        tokio::time::sleep(Duration::ZERO).await;
        // Keep the paused clock from advancing while filesystem workers
        // finish. A wall-clock deadline also bounds a broken admission path.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let queue = self.manager.heal_queue.lock().await;
            if let Some(item) = queue.heap.peek() {
                assert_eq!(queue.len(), 1, "one set must have exactly one admitted owner");
                return item.request.clone();
            }
            drop(queue);
            assert!(std::time::Instant::now() < deadline, "scanner tick did not admit replacement recovery");
            tokio::task::yield_now().await;
        }
    }

    async fn assert_original_owner_is_queued(&self) {
        let request = self.queued_request().await;
        assert_eq!(request.id, self.task_id, "scanner must replay the durable UUID");
        assert_eq!(request.heal_endpoints, vec![self.local_targets[0].clone()]);
        assert_eq!(request.source, HealRequestSource::AutoHeal);
        assert!(request.replacement_targets_frozen, "replayed durable targets cannot expand");
        assert!(matches!(
            request.heal_type,
            HealType::ErasureSet { set_disk_id, buckets }
                if set_disk_id == SET && buckets == ["seeded-bucket"]
        ));
        assert_eq!(
            self.manager
                .replacement_recovery_anchors
                .lock()
                .expect("scanner anchor registry")
                .get(&self.task_id),
            Some(&self.storage.survivor.endpoint().to_string())
        );
        assert!(self.manager.replacement_recovery_blocks().is_empty());
        assert_eq!(
            self.storage.inventory_calls.load(Ordering::SeqCst),
            1,
            "three local slots in one set must share one inventory lookup per tick"
        );
        let state = self.owner.get_state().await;
        assert_eq!(state.retry_count, 1, "discovery must not reset the durable retry budget");
        assert_eq!(state.replacement_targets, request.heal_endpoints);
    }
}

async fn finish_without_advancing_clock<T>(future: impl Future<Output = T>) -> T {
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            result = &mut future => return result,
            _ = tokio::task::yield_now() => {}
        }
    }
}

#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn scanner_tick_recovers_remote_survivor_after_all_local_targets_are_formatted() {
    let fixture = ScannerFixture::new(false).await;
    // Durable fixture writes must finish before virtual scanner time begins.
    tokio::time::pause();
    fixture
        .manager
        .start_auto_disk_scanner()
        .await
        .expect("start only the periodic replacement scanner");
    fixture.assert_original_owner_is_queued().await;
}

#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn scanner_tick_finishes_existing_owner_before_admitting_new_unformatted_targets() {
    temp_env::async_with_vars([(rustfs_config::ENV_UNSAFE_BYPASS_DISK_CHECK, Some("true"))], async {
        let fixture = ScannerFixture::new(true).await;
        tokio::time::pause();
        fixture
            .manager
            .start_auto_disk_scanner()
            .await
            .expect("start only the periodic replacement scanner");
        fixture.assert_original_owner_is_queued().await;

        // The real task's verified completion publishes the proof and
        // removes its live intent before sibling admission is allowed.
        let owner_removed = finish_without_advancing_clock(async {
            fixture
                .owner
                .mark_replacement_completed_and_verified()
                .await
                .expect("durable owner completion proof");
            fixture
                .owner
                .mark_replacement_cleanup_pending()
                .await
                .expect("durable owner cleanup phase");
            fixture
                .owner
                .cleanup()
                .await
                .expect("completed owner releases its durable intent");
            !ResumeManager::has_replacement_intent(&fixture.storage.survivor, &fixture.task_id).await
        })
        .await;
        assert!(owner_removed, "completed ownership must be removed before the next generation");
        let finished = fixture
            .manager
            .heal_queue
            .lock()
            .await
            .pop_next()
            .expect("remove the completed queued owner");
        assert_eq!(finished.id, fixture.task_id);
        tokio::time::advance(SCAN_INTERVAL).await;

        let next = fixture.queued_request().await;
        assert_ne!(next.id, fixture.task_id, "the follow-up replacement gets a fresh generation");
        assert_eq!(next.heal_endpoints, fixture.local_targets[1..]);
        assert_eq!(next.source, HealRequestSource::AutoHeal);
        assert!(fixture.manager.replacement_recovery_blocks().is_empty());
        assert_eq!(fixture.storage.inventory_calls.load(Ordering::SeqCst), 2);
    })
    .await;
}

#[tokio::test]
async fn recovery_disk_index_keeps_first_snapshot_and_sorted_unique_endpoints() {
    let temp = TempDir::new().expect("recovery disk index fixture");
    let local = make_manager_resume_disk(&temp, "a-local").await;
    let duplicate = make_manager_resume_disk(&temp, "a-local").await;
    let survivor = make_manager_resume_disk(&temp, "z-survivor").await;
    assert!(!Arc::ptr_eq(&local, &duplicate), "duplicate must be a different disk snapshot");
    let local_endpoint = local.endpoint().to_string();
    let survivor_endpoint = survivor.endpoint().to_string();
    let mut inventory = std::collections::BTreeMap::new();
    super::super::auto_scan::extend_replacement_recovery_disks(&mut inventory, [local.clone()]);
    super::super::auto_scan::extend_replacement_recovery_disks(&mut inventory, [survivor.clone(), duplicate, survivor]);
    assert_eq!(inventory.len(), 2, "each endpoint is discovered once");
    assert!(
        Arc::ptr_eq(inventory.get(&local_endpoint).expect("local endpoint remains indexed"), &local),
        "later set inventories must not replace the local snapshot"
    );
    assert_eq!(
        inventory.keys().collect::<Vec<_>>(),
        vec![&local_endpoint, &survivor_endpoint],
        "discovery order remains lexical regardless of insertion order"
    );
}

#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn scanner_tick_blocks_only_the_conflicting_generation_of_a_set() {
    // backlog#2902 step 3: two survivors hold the same revision of a sibling
    // generation with different bytes. That generation is unverifiable, but
    // the set's other generation must still be replayed.
    let mut fixture = ScannerFixture::new(false).await;
    let sibling_a = make_manager_resume_disk(&fixture.temp, "sibling-survivor-a").await;
    let sibling_b = make_manager_resume_disk(&fixture.temp, "sibling-survivor-b").await;
    let sibling_id = ResumeUtils::generate_task_id();
    let sibling_identity = ReplacementTargetIdentity {
        endpoint: fixture.local_targets[1].clone(),
        canonical_path: fixture.local_targets[1].clone(),
        physical_device_ids: vec!["replacement-target-1".into()],
        filesystem_identity: "replacement-target-1-incarnation".into(),
    };
    for (disk, copy) in [(&sibling_a, "copy-a"), (&sibling_b, "copy-b")] {
        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            sibling_id.clone(),
            SET.into(),
            vec!["seeded-bucket".into()],
            vec![fixture.local_targets[1].clone()],
            vec![sibling_identity.clone()],
        )
        .await
        .expect("durable sibling generation copy");
        manager
            .set_error(format!("divergent {copy}"))
            .await
            .expect("diverge the copies");
    }
    let storage = Arc::new(ScannerStorage {
        survivor: fixture.storage.survivor.clone(),
        sibling_survivors: vec![sibling_a, sibling_b],
        identity: fixture.storage.identity.clone(),
        inventory_calls: AtomicUsize::new(0),
    });
    fixture.storage = storage.clone();
    fixture.manager = HealManager::new_without_root_recovery_for_test(
        storage,
        Some(HealConfig {
            heal_interval: SCAN_INTERVAL,
            ..HealConfig::default()
        }),
    );
    tokio::time::pause();
    fixture
        .manager
        .start_auto_disk_scanner()
        .await
        .expect("start only the periodic replacement scanner");

    let request = fixture.queued_request().await;
    assert_eq!(request.id, fixture.task_id, "the clean generation of the set is replayed");
    assert_eq!(request.heal_endpoints, vec![fixture.local_targets[0].clone()]);
    assert_eq!(fixture.owner.get_state().await.retry_count, 1, "replay must not touch the durable budget");
    assert!(!fixture.manager.replacement_recovery_generation_is_blocked(&fixture.task_id));
    let blocks = fixture.manager.replacement_recovery_blocks();
    assert_eq!(blocks.len(), 1, "only the unverifiable generation is blocked: {blocks:?}");
    assert_eq!(blocks[0].task_id, sibling_id);
    assert_eq!(blocks[0].set_disk_id, SET);
    assert!(blocks[0].reason.contains("diverge"), "{}", blocks[0].reason);
    assert!(blocks[0].reserves_targets, "an active intent still reserves its slot");
    assert!(blocks[0].blocked_since > 0);
}
