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
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

const SET: &str = "pool_0_set_0";
const SCAN_INTERVAL: Duration = Duration::from_secs(10);

struct ScannerStorage {
    survivor: DiskStore,
    /// Additional survivors exposed through the same set inventory.
    sibling_survivors: Vec<DiskStore>,
    identity: ReplacementTargetIdentity,
    inventory_calls: AtomicUsize,
    inventory_defers: AtomicBool,
}

#[async_trait::async_trait]
impl HealStorageAPI for ScannerStorage {
    async fn replacement_intent_disks(&self, set: &str, _cancellation: Option<&CancellationToken>) -> Result<Vec<DiskStore>> {
        assert_eq!(set, SET, "discovery must use the target's erasure set");
        self.inventory_calls.fetch_add(1, Ordering::SeqCst);
        if self.inventory_defers.load(Ordering::SeqCst) {
            return Err(Error::transient_skip("test inventory admission deferred"));
        }
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
            inventory_defers: AtomicBool::new(false),
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
        inventory_defers: AtomicBool::new(false),
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

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn owned_auto_scan_waits_for_actual_native_intent_read() {
    use crate::heal::storage_api::{
        EcstoreLocalPublicationPause as LocalPublicationPause, EcstoreLocalPublicationStage as LocalPublicationStage,
    };

    let fixture = ScannerFixture::new(false).await;
    let path = format!(
        "{}/ahm-replacement/{}_ahm_replacement_intent.json",
        crate::heal::BUCKET_META_PREFIX,
        fixture.task_id
    );
    let before = fixture
        .storage
        .survivor
        .read_all(crate::heal::RUSTFS_META_BUCKET, &path)
        .await
        .expect("nonempty real replacement state");
    assert!(!before.is_empty());
    let mut pause = LocalPublicationPause::install(
        fixture.storage.survivor.as_ref(),
        crate::heal::RUSTFS_META_BUCKET,
        &path,
        LocalPublicationStage::NativeReadOpen,
    )
    .expect("native survivor read pause");
    fixture
        .manager
        .start_auto_disk_scanner()
        .await
        .expect("actual slot-1 producer");
    tokio::time::timeout(Duration::from_secs(5), pause.entered())
        .await
        .expect("scanner native read entry deadline")
        .expect("real scanner read entered");
    assert!(fixture.storage.inventory_calls.load(Ordering::SeqCst) >= 1);
    let mut stop = Box::pin(fixture.manager.stop());
    let first = tokio::time::timeout(Duration::from_millis(30), stop.as_mut()).await;
    let held = first.is_err();
    let unlocked = fixture.manager.admin_start_shutdown.try_lock().is_ok()
        && fixture.manager.active_heals.try_lock().is_ok()
        && fixture.manager.completed_heals.try_lock().is_ok();
    drop(pause); // Release the native callback; its original syscall still runs.
    let result = match first {
        Ok(result) => Ok(result),
        Err(_) => tokio::time::timeout(Duration::from_secs(5), stop).await,
    };
    assert!(held, "stop must own the scanner's in-flight tick branch");
    assert!(unlocked, "join may not retain manager locks");
    result.expect("released actual scanner joins").expect("scanner stop");
    let registration = fixture.manager.workers.registration.lock().unwrap();
    assert!(
        registration.fixed[1]
            .as_ref()
            .expect("actual scanner receipt")
            .peek()
            .is_some()
    );
}

#[cfg(not(windows))]
#[test]
#[serial_test::serial(replacement_auto_scan)]
fn owned_resume_gc_cancels_async_loop_before_native_open_tail() {
    use crate::heal::resume::ResumeState;
    use crate::heal::storage_api::{
        EcstoreLocalPublicationPause as LocalPublicationPause, EcstoreLocalPublicationStage as LocalPublicationStage,
    };

    // This fixture owns its original executor, and drops it outside Tokio.
    // Its receipt is local to this runtime, not the service's other native pools.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("owned GC fixture executor");
    let (fixture, pause, file_path, original) = runtime.block_on(async {
        let fixture = ScannerFixture::new(false).await;
        let disk = crate::heal::local_disk_map_read()
            .await
            .get(&fixture.local_targets[0])
            .and_then(Clone::clone)
            .expect("actual registry disk");
        let prefix = crate::heal::BUCKET_META_PREFIX;
        match disk
            .make_volume(&format!("{}/{prefix}", crate::heal::RUSTFS_META_BUCKET))
            .await
        {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(error) => panic!("resume volume: {error}"),
        }
        let id = Uuid::new_v4().to_string();
        let state = ResumeState::new(id.clone(), SET.into(), "disk".into(), vec![]);
        let original = serde_json::to_vec(&state).expect("real ordinary resume state");
        assert!(!original.is_empty());
        let path = format!("{prefix}/{id}_ahm_resume_state.json");
        disk.write_all(crate::heal::RUSTFS_META_BUCKET, &path, original.clone().into())
            .await
            .expect("seed actual local GC namespace");
        let file_path = std::path::Path::new(&fixture.local_targets[0])
            .join(crate::heal::RUSTFS_META_BUCKET)
            .join(&path);
        let mut pause = LocalPublicationPause::install(
            disk.as_ref(),
            crate::heal::RUSTFS_META_BUCKET,
            &path,
            LocalPublicationStage::NativeReadOpen,
        )
        .expect("native ordinary-state open pause");
        fixture.manager.start_resume_gc().await.expect("actual slot-2 producer");
        tokio::time::timeout(Duration::from_secs(5), pause.entered())
            .await
            .expect("GC native open entry deadline")
            .expect("real bounded read entered");
        tokio::time::timeout(Duration::from_secs(5), fixture.manager.stop())
            .await
            .expect("read-only GC may cancel its async inspection")
            .expect("async loop joined");
        {
            let registration = fixture.manager.workers.registration.lock().unwrap();
            assert!(registration.fixed[2].as_ref().expect("actual GC receipt").peek().is_some());
        }
        (fixture, pause, file_path, original)
    });
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let finisher = std::thread::spawn(move || {
        drop(runtime); // Ordinary Drop must wait for its accepted native open.
        finished_tx.send(()).expect("fixture finisher receiver");
    });
    let held_native = matches!(
        finished_rx.recv_timeout(Duration::from_millis(30)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    );
    drop(pause);
    if held_native {
        finished_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("original executor drops after native release");
    }
    finisher.join().expect("ordinary executor Drop completed");
    assert!(held_native, "async GC join must not masquerade as native-tail completion");
    assert_eq!(
        std::fs::read(file_path).expect("retained state"),
        original,
        "read-only cancellation preserves state"
    );
    drop(fixture);
}

#[cfg(not(windows))]
struct ResumeAdmissionFixture {
    _root: TempDir,
    _store: rustfs_test_utils::TestECStoreEnv,
    manager: HealManager,
    disk: DiskStore,
    path: String,
}

#[cfg(not(windows))]
fn publish_resume_node(node: &rustfs_concurrency::adaptive::NodeAdmission, open: bool) {
    use rustfs_concurrency::adaptive::{FeedbackController, FeedbackObservation, ResourcePressure};
    node.update(
        &mut FeedbackController::new(),
        open.then_some(FeedbackObservation {
            generation: 1,
            max_concurrency: 1,
            pressure: ResourcePressure::Unknown,
            completed_units: None,
        }),
    );
}

#[cfg(not(windows))]
fn resume_node(open: bool) -> Arc<rustfs_concurrency::adaptive::NodeAdmission> {
    let node = rustfs_concurrency::adaptive::NodeAdmission::new(std::num::NonZeroU16::MIN);
    publish_resume_node(&node, open);
    node
}

#[cfg(not(windows))]
async fn resume_admission_fixture(node: Option<Arc<rustfs_concurrency::adaptive::NodeAdmission>>) -> ResumeAdmissionFixture {
    use crate::heal::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET, resume::ResumeState, storage::ECStoreHealStorage};
    let root = TempDir::new().expect("owned resume admission store");
    let store = rustfs_test_utils::TestECStoreEnv::builder()
        .base_dir(root.path())
        .build()
        .await;
    let disk = crate::heal::local_disk_map_read()
        .await
        .values()
        .flatten()
        .next()
        .cloned()
        .expect("real local disk");
    match disk.make_volume(&format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}")).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(error) => panic!("ordinary resume volume: {error}"),
    }
    let id = Uuid::new_v4().to_string();
    let state = ResumeState::new(id.clone(), SET.into(), "disk".into(), vec![]);
    let path = format!("{BUCKET_META_PREFIX}/{id}_ahm_resume_state.json");
    let bytes = serde_json::to_vec(&state).expect("real nonempty resume state");
    assert!(!bytes.is_empty());
    disk.write_all(RUSTFS_META_BUCKET, &path, bytes.into())
        .await
        .expect("seed actual inspector input");
    let storage = Arc::new(ECStoreHealStorage::new(store.ecstore.clone()).with_admission(node.clone()));
    match (node.as_ref(), storage.background_node_admission()) {
        (Some(expected), Some(actual)) => {
            assert!(Arc::ptr_eq(expected, &actual), "getter must return the factory-attached instance")
        }
        (None, None) => {}
        _ => panic!("storage capability changed"),
    }
    let manager = HealManager::new_without_root_recovery_for_test(
        storage,
        Some(HealConfig {
            enable_auto_heal: false,
            ..Default::default()
        }),
    );
    ResumeAdmissionFixture {
        _root: root,
        _store: store,
        manager,
        disk,
        path,
    }
}

#[cfg(not(windows))]
async fn assert_resume_admission(mode: &str) {
    use crate::heal::storage_api::{
        EcstoreLocalPublicationPause as LocalPublicationPause, EcstoreLocalPublicationStage as LocalPublicationStage,
    };
    let node = (mode != "none").then(|| resume_node(mode != "node-closed"));
    let fixture = resume_admission_fixture(node.clone()).await;
    let binding = fixture.disk.physical_admission().expect("existing public local binding");
    let physical =
        rustfs_concurrency::adaptive::AdaptiveAdmission::new(std::num::NonZeroU16::MIN, u32::from(mode != "physical-closed"));
    if mode != "missing-plan" && mode != "none" {
        binding
            .set_test_capacity(vec![physical.clone()])
            .expect("explicit synthetic physical capacity");
    } else {
        assert!(binding.plan().is_none(), "this fixture must not fabricate discovery");
    }
    let mut pause = LocalPublicationPause::install(
        fixture.disk.as_ref(),
        crate::heal::RUSTFS_META_BUCKET,
        &fixture.path,
        LocalPublicationStage::NativeReadOpen,
    )
    .expect("actual nonempty native-open witness");
    fixture
        .manager
        .start_resume_gc()
        .await
        .expect("actual production slot-2 producer");
    let entered = tokio::time::timeout(Duration::from_millis(400), pause.entered()).await;
    if mode == "none" {
        assert!(entered.is_ok_and(|r| r.is_ok()), "None preserves the original native path");
    } else {
        assert!(entered.is_err(), "enabled admission must defer before native entry: {mode}");
        assert_eq!(node.as_ref().unwrap().snapshot().active, 0);
        assert_eq!(physical.snapshot().active, 0);
    }
    drop(pause);
    tokio::time::timeout(Duration::from_secs(5), fixture.manager.stop())
        .await
        .expect("real producer shutdown deadline")
        .expect("real producer joined");
    if let Some(node) = node {
        assert_eq!(node.snapshot().active, 0);
        assert_eq!(node.completed(), Some(0), "read-only inspection cannot invent repair progress");
    }
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn resume_admission_none_keeps_real_native_path() {
    assert_resume_admission("none").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn resume_admission_closed_node_defers_before_native() {
    assert_resume_admission("node-closed").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn resume_admission_closed_physical_defers_before_native() {
    assert_resume_admission("physical-closed").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn resume_admission_missing_plan_defers_before_native() {
    assert_resume_admission("missing-plan").await;
}

#[cfg(not(windows))]
#[test]
#[serial_test::serial(replacement_auto_scan)]
fn resume_admission_cancel_returns_node_but_native_keeps_physical_and_memory() {
    const CHILD: &str = "RUSTFS_RESUME_ADMISSION_NATIVE_CHILD";
    const NAME: &str =
        "heal::manager::tests::auto_scan::resume_admission_cancel_returns_node_but_native_keeps_physical_and_memory";
    if std::env::var(CHILD).ok().as_deref() != Some("1") {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([NAME, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .output()
            .expect("fresh policy-owning test process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed; 0 failed; 0 ignored;"),
            "child failed: {stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        println!("RESUME_ADMISSION_NATIVE_CHILD_OK");
        return;
    }
    use crate::heal::storage_api::{
        EcstoreBackgroundMemoryBudget as BackgroundMemoryBudget, EcstoreLocalPublicationPause as LocalPublicationPause,
        EcstoreLocalPublicationStage as LocalPublicationStage,
    };
    let memory = BackgroundMemoryBudget::new();
    assert!(memory.install(), "fresh child owns installed policy");
    let baseline_memory_owners = Arc::strong_count(&memory);
    let node = resume_node(true);
    let physical = rustfs_concurrency::adaptive::AdaptiveAdmission::new(std::num::NonZeroU16::MIN, 1);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (fixture, pause) = runtime.block_on(async {
        let fixture = resume_admission_fixture(Some(node.clone())).await;
        fixture
            .disk
            .physical_admission()
            .unwrap()
            .set_test_capacity(vec![physical.clone()])
            .expect("explicit native physical capacity");
        let mut pause = LocalPublicationPause::install(
            fixture.disk.as_ref(),
            crate::heal::RUSTFS_META_BUCKET,
            &fixture.path,
            LocalPublicationStage::NativeReadOpen,
        )
        .unwrap();
        fixture.manager.start_resume_gc().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), pause.entered())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node.snapshot().active, 1);
        assert_eq!(physical.snapshot().active, 1);
        tokio::time::timeout(Duration::from_secs(5), fixture.manager.stop())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node.snapshot().active, 0, "cancelled async work returns node capacity");
        assert_eq!(physical.snapshot().active, 1, "accepted native work retains selected physical ownership");
        assert!(
            Arc::strong_count(&memory) > baseline_memory_owners,
            "native unit retains its real memory capability"
        );
        (fixture, pause)
    });
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let finish = std::thread::spawn(move || {
        drop(runtime);
        finished_tx.send(()).unwrap();
    });
    let held = matches!(
        finished_rx.recv_timeout(Duration::from_millis(30)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    );
    drop(pause);
    if held {
        finished_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("original executor completed");
    }
    finish.join().unwrap();
    assert!(held, "ordinary executor Drop waits for its accepted native open");
    assert_eq!(physical.snapshot().active, 0);
    assert_eq!(node.snapshot().active, 0);
    assert_eq!(node.completed(), Some(0));
    assert_eq!(Arc::strong_count(&memory), baseline_memory_owners);
    drop(fixture);
}

#[cfg(not(windows))]
async fn auto_format_fixture(
    node: Option<Arc<rustfs_concurrency::adaptive::NodeAdmission>>,
    physical: Option<Arc<rustfs_concurrency::adaptive::AdaptiveAdmission>>,
    seed_unread_intent: bool,
) -> (ResumeAdmissionFixture, Option<(String, Vec<u8>)>) {
    // Preserve the constructor's explicit open/closed state before any fixture await.
    let node_limit = node.as_ref().map(|node| node.snapshot().limit);
    let fixture = resume_admission_fixture(node.clone()).await;
    let disks = crate::heal::local_disk_map_read()
        .await
        .values()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    let saved = if seed_unread_intent {
        let volume = format!("{}/{}/ahm-replacement", crate::heal::RUSTFS_META_BUCKET, crate::heal::BUCKET_META_PREFIX);
        match fixture.disk.make_volume(&volume).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(error) => panic!("intent fixture volume: {error}"),
        }
        let path = format!(
            "{}/ahm-replacement/{}_ahm_replacement_intent.json",
            crate::heal::BUCKET_META_PREFIX,
            Uuid::new_v4()
        );
        let bytes = b"must not be interpreted as absence or corruption while admission defers".to_vec();
        fixture
            .disk
            .write_all(crate::heal::RUSTFS_META_BUCKET, &path, bytes.clone().into())
            .await
            .unwrap();
        Some((path, bytes))
    } else {
        None
    };
    assert!(
        !fixture
            .disk
            .read_all(crate::heal::RUSTFS_META_BUCKET, "format.json")
            .await
            .unwrap()
            .is_empty()
    );
    // get_disk_id uses wall-clock seconds, not Tokio's paused clock. No cache API
    // or invalidation hook is introduced; age its existing coarse one-second TTL.
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Discovery itself has a separate three-second lease: publish the explicit
    // component capacity only after aging the wall-clock format cache.
    for disk in disks {
        let binding = disk.physical_admission().expect("existing local binding");
        if let Some(physical) = &physical {
            // These directory-backed aliases deliberately share one synthetic leaf.
            binding.set_test_capacity(vec![physical.clone()]).unwrap();
        } else {
            assert!(binding.plan().is_none());
        }
    }
    // Cache aging also expires the node sample. Refresh the same owner's ordinary
    // three-second sample only after aging, just as physical discovery is refreshed.
    if let Some(node) = &node {
        let expected = node_limit.expect("attached node has a recorded constructor limit");
        assert!(expected <= 1, "the fixture constructor has one bounded scheduling slot");
        publish_resume_node(node, expected == 1);
        assert_eq!(node.snapshot().limit, expected, "preserve the intended open/closed sample before startup");
        let attached = fixture
            .manager
            .storage
            .background_node_admission()
            .expect("attached fixture node");
        assert!(Arc::ptr_eq(node, &attached), "refresh the same manager-attached controller");
    } else {
        assert!(fixture.manager.storage.background_node_admission().is_none());
    }
    (fixture, saved)
}

#[cfg(not(windows))]
async fn assert_auto_format_deferral(mode: &str) {
    use crate::heal::storage_api::{
        EcstoreLocalPublicationPause as LocalPublicationPause, EcstoreLocalPublicationStage as LocalPublicationStage,
    };
    let node = (mode != "none").then(|| resume_node(mode != "node-closed"));
    let physical = (mode != "none" && mode != "missing-plan").then(|| {
        rustfs_concurrency::adaptive::AdaptiveAdmission::new(std::num::NonZeroU16::MIN, u32::from(mode != "physical-closed"))
    });
    let (fixture, saved) = auto_format_fixture(node.clone(), physical.clone(), mode != "none").await;
    let mut pause = LocalPublicationPause::install(
        fixture.disk.as_ref(),
        crate::heal::RUSTFS_META_BUCKET,
        "format.json",
        LocalPublicationStage::NativeReadOpen,
    )
    .expect("actual cold format syscall pause");
    fixture.manager.start_auto_disk_scanner().await.unwrap();
    let entered = tokio::time::timeout(Duration::from_millis(400), pause.entered()).await;
    let correct = if mode == "none" {
        entered.is_ok_and(|r| r.is_ok())
    } else {
        entered.is_err()
    };
    let no_candidate = fixture.manager.get_queue_length().await == 0;
    let no_corruption = fixture.manager.replacement_recovery_blocks().is_empty();
    drop(pause);
    tokio::time::timeout(Duration::from_secs(5), fixture.manager.stop())
        .await
        .unwrap()
        .unwrap();
    assert!(correct, "format native admission boundary failed: {mode}");
    if mode != "none" {
        assert!(no_candidate, "unavailable metadata cannot start replacement");
        assert!(no_corruption, "quota deferral cannot interpret an unread intent as corruption");
        let (path, bytes) = saved.unwrap();
        assert_eq!(
            fixture
                .disk
                .read_all(crate::heal::RUSTFS_META_BUCKET, &path)
                .await
                .unwrap()
                .as_ref(),
            bytes.as_slice()
        );
        assert_eq!(node.as_ref().unwrap().snapshot().active, 0);
        assert_eq!(node.as_ref().unwrap().completed(), Some(0));
        if let Some(physical) = physical {
            assert_eq!(physical.snapshot().active, 0);
        }
    }
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_format_admission_none_keeps_cold_native_probe() {
    assert_auto_format_deferral("none").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_format_admission_closed_node_defers_set_before_native() {
    assert_auto_format_deferral("node-closed").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_format_admission_closed_physical_defers_set_before_native() {
    assert_auto_format_deferral("physical-closed").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_format_admission_missing_plan_skips_later_same_set_inventory() {
    assert_auto_format_deferral("missing-plan").await;
}

#[cfg(not(windows))]
#[test]
#[serial_test::serial(replacement_auto_scan)]
fn auto_format_admission_funded_probe_finishes_before_stop_and_releases_owners() {
    const FLAG: &str = "RUSTFS_AUTO_FORMAT_ADMISSION_CHILD";
    const NAME: &str =
        "heal::manager::tests::auto_scan::auto_format_admission_funded_probe_finishes_before_stop_and_releases_owners";
    if std::env::var(FLAG).ok().as_deref() != Some("1") {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([NAME, "--exact", "--nocapture", "--test-threads=1"])
            .env(FLAG, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed; 0 failed; 0 ignored;"),
            "child failed: {stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        println!("AUTO_FORMAT_ADMISSION_CHILD_OK");
        return;
    }
    use crate::heal::storage_api::{
        EcstoreBackgroundMemoryBudget as BackgroundMemoryBudget, EcstoreLocalPublicationPause as LocalPublicationPause,
        EcstoreLocalPublicationStage as LocalPublicationStage,
    };
    let memory = BackgroundMemoryBudget::new();
    assert!(memory.install());
    let memory_owners = Arc::strong_count(&memory);
    let node = resume_node(true);
    let physical = rustfs_concurrency::adaptive::AdaptiveAdmission::new(std::num::NonZeroU16::MIN, 1);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime.block_on(async {
        let (fixture, saved) = auto_format_fixture(Some(node.clone()), Some(physical.clone()), true).await;
        let mut pause = LocalPublicationPause::install(
            fixture.disk.as_ref(),
            crate::heal::RUSTFS_META_BUCKET,
            "format.json",
            LocalPublicationStage::NativeReadOpen,
        )
        .unwrap();
        fixture.manager.start_auto_disk_scanner().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), pause.entered())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node.snapshot().active, 1);
        assert_eq!(physical.snapshot().active, 1);
        assert!(Arc::strong_count(&memory) > memory_owners);
        let mut stop = Box::pin(fixture.manager.stop());
        let first = tokio::time::timeout(Duration::from_millis(30), stop.as_mut()).await;
        let held = first.is_err();
        let yielded = tokio::time::timeout(Duration::from_secs(2), async {
            while node.snapshot().active != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        let cancelled_ownership = (
            node.snapshot().active,
            physical.snapshot().active,
            Arc::strong_count(&memory) > memory_owners,
        );
        drop(pause);
        let result = match first {
            Ok(result) => Ok(result),
            Err(_) => tokio::time::timeout(Duration::from_secs(5), stop.as_mut()).await,
        };
        drop(stop);
        assert!(held, "an admitted native format probe must finish before actual slot1 join");
        result.unwrap().unwrap();
        assert!(yielded, "stop cancellation must return node capacity while native format I/O is paused");
        assert_eq!(cancelled_ownership, (0, 1, true));
        assert_eq!(node.snapshot().active, 0);
        assert_eq!(physical.snapshot().active, 0);
        assert_eq!(node.completed(), Some(0));
        assert!(
            fixture.manager.replacement_recovery_blocks().is_empty(),
            "stop must not begin a later intent phase"
        );
        let (path, bytes) = saved.unwrap();
        assert_eq!(
            fixture
                .disk
                .read_all(crate::heal::RUSTFS_META_BUCKET, &path)
                .await
                .unwrap()
                .as_ref(),
            bytes.as_slice()
        );
        fixture
    });
    drop(runtime); // Outside Tokio: ordinary native executor finalization.
    assert_eq!(physical.snapshot().active, 0);
    assert_eq!(Arc::strong_count(&memory), memory_owners);
    drop(fixture);
}

#[cfg(not(windows))]
async fn auto_inventory_local_disks(fixture: &ResumeAdmissionFixture) -> Vec<DiskStore> {
    use crate::heal::storage_api::storage::{DiskSetSelector, StorageAdminApi};
    let disks = StorageAdminApi::disk_set_inventory(fixture._store.ecstore.as_ref(), DiskSetSelector::new(0, 0))
        .await
        .expect("actual ECStore inventory snapshot");
    let disks = disks
        .into_iter()
        .flatten()
        .filter(|disk| disk.endpoint().is_local)
        .collect::<Vec<_>>();
    assert!(disks.len() >= 2, "native cancellation witness needs a following real local disk");
    disks
}

#[cfg(not(windows))]
async fn assert_auto_inventory_admission(mode: &str) {
    use crate::heal::storage::ECStoreHealStorage;
    use crate::heal::storage_api::{
        EcstoreLocalPublicationPause as LocalPublicationPause, EcstoreLocalPublicationStage as LocalPublicationStage,
    };
    let node = (mode != "none").then(|| resume_node(!matches!(mode, "node-closed" | "startup-none")));
    let physical = (!matches!(mode, "none" | "missing-plan")).then(|| {
        rustfs_concurrency::adaptive::AdaptiveAdmission::new(std::num::NonZeroU16::MIN, u32::from(mode != "physical-closed"))
    });
    let (fixture, saved) = auto_format_fixture(node.clone(), physical.clone(), true).await;
    let disks = auto_inventory_local_disks(&fixture).await;
    if matches!(mode, "unformatted" | "corrupt-format") {
        let format = crate::heal::storage_api::EcstoreDiskAPI::path(disks[0].as_ref())
            .join(crate::heal::RUSTFS_META_BUCKET)
            .join("format.json");
        if mode == "unformatted" {
            std::fs::remove_file(&format).expect("remove only the owned fixture format");
        } else {
            let replacement = format.with_extension("inventory-corrupt");
            std::fs::write(&replacement, b"invalid format json").expect("corrupt owned fixture format");
            std::fs::rename(replacement, format).expect("replace format inode to force actual decode");
        }
    }
    if mode == "stale-plan" {
        // Physical discovery has a three-second lease; the real wall clock must expire it.
        tokio::time::sleep(Duration::from_secs(4)).await;
        // Expire only physical discovery; node funding must remain independently fresh.
        publish_resume_node(node.as_ref().expect("stale physical case has an attached node"), true);
        assert_eq!(node.as_ref().expect("same attached node").snapshot().limit, 1);
        assert!(disks[0].physical_admission().expect("local binding").plan().is_none());
    }
    let storage = Arc::new(ECStoreHealStorage::new(fixture._store.ecstore.clone()).with_admission(node.clone()));
    match (node.as_ref(), storage.background_node_admission()) {
        (Some(expected), Some(actual)) => assert!(Arc::ptr_eq(expected, &actual), "reuse the attached node controller"),
        (None, None) => {}
        _ => panic!("inventory storage changed admission capability"),
    }
    let mut pause = LocalPublicationPause::install(
        disks[0].as_ref(),
        crate::heal::RUSTFS_META_BUCKET,
        "format.json",
        LocalPublicationStage::NativeReadOpen,
    )
    .expect("actual first inventory format syscall pause");
    let token = CancellationToken::new();
    let startup = mode == "startup-none";
    let probe = tokio::spawn(async move {
        storage
            .replacement_intent_disks(SET, if startup { None } else { Some(&token) })
            .await
    });
    let entered = tokio::time::timeout(Duration::from_millis(400), pause.entered()).await;
    let direct = matches!(mode, "none" | "startup-none" | "unformatted" | "corrupt-format");
    let native_boundary = if direct {
        entered.is_ok_and(|result| result.is_ok())
    } else {
        entered.is_err()
    };
    drop(pause);
    let result = tokio::time::timeout(Duration::from_secs(5), probe)
        .await
        .expect("inventory entry joins after releasing native work")
        .expect("inventory task should not panic");
    assert!(native_boundary, "inventory native boundary failed for {mode}");
    if mode == "corrupt-format" {
        assert!(
            matches!(result, Err(Error::TaskExecutionFailed { .. })),
            "backend corruption keeps its original error"
        );
    } else if direct {
        let formatted = result.expect("inventory preserves the admitted backend result");
        let contains_first = formatted
            .iter()
            .any(|disk| disk.endpoint().to_string() == disks[0].endpoint().to_string());
        assert_eq!(
            contains_first,
            mode != "unformatted",
            "preserve formatted/unformatted inventory filtering"
        );
        assert!(!formatted.is_empty(), "other formatted disks remain discoverable");
    } else {
        assert!(
            matches!(result, Err(Error::TransientSkip { .. })),
            "unavailable evidence must return typed deferral"
        );
    }
    if let Some(node) = node {
        assert_eq!(node.snapshot().active, 0);
        assert_eq!(node.completed(), Some(0), "inventory diagnostics earn no repair credit");
    }
    if let Some(physical) = physical {
        assert_eq!(physical.snapshot().active, 0);
    }
    let (path, original) = saved.expect("unread durable fixture");
    let fixture_root = crate::heal::storage_api::EcstoreDiskAPI::path(fixture.disk.as_ref());
    // Format validation can reject the storage API before opening this unchanged intent.
    assert_eq!(
        std::fs::read(fixture_root.join(crate::heal::RUSTFS_META_BUCKET).join(&path))
            .expect("task-owned durable fixture remains")
            .as_slice(),
        original.as_slice()
    );
    let readable = fixture.disk.read_all(crate::heal::RUSTFS_META_BUCKET, &path).await;
    if matches!(mode, "unformatted" | "corrupt-format")
        && fixture_root == crate::heal::storage_api::EcstoreDiskAPI::path(disks[0].as_ref())
    {
        assert!(
            matches!(
                (mode, readable),
                ("unformatted", Err(DiskError::UnformattedDisk)) | ("corrupt-format", Err(DiskError::CorruptedBackend))
            ),
            "the deliberately damaged fixture format must retain its exact backend error"
        );
    } else {
        assert_eq!(readable.expect("healthy fixture remains API-readable").as_ref(), original.as_slice());
    }
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_inventory_admission_none_keeps_actual_format_probe() {
    assert_auto_inventory_admission("none").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_inventory_admission_startup_none_keeps_original_probe() {
    assert_auto_inventory_admission("startup-none").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_inventory_admission_closed_node_defers_before_native() {
    assert_auto_inventory_admission("node-closed").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_inventory_admission_closed_physical_defers_before_native() {
    assert_auto_inventory_admission("physical-closed").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_inventory_admission_missing_plan_defers_before_native() {
    assert_auto_inventory_admission("missing-plan").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_inventory_admission_stale_plan_defers_before_native() {
    assert_auto_inventory_admission("stale-plan").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_inventory_admission_funded_unformatted_keeps_backend_filter() {
    assert_auto_inventory_admission("unformatted").await;
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_inventory_admission_funded_corrupt_format_keeps_backend_error() {
    assert_auto_inventory_admission("corrupt-format").await;
}

#[cfg(not(windows))]
#[test]
#[serial_test::serial(replacement_auto_scan)]
fn auto_inventory_admission_cancel_yields_node_and_retains_only_started_native() {
    const FLAG: &str = "RUSTFS_AUTO_INVENTORY_ADMISSION_CHILD";
    const NAME: &str =
        "heal::manager::tests::auto_scan::auto_inventory_admission_cancel_yields_node_and_retains_only_started_native";
    if std::env::var(FLAG).ok().as_deref() != Some("1") {
        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([NAME, "--exact", "--nocapture", "--test-threads=1"])
            .env(FLAG, "1")
            .output()
            .expect("fresh memory-policy test process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed; 0 failed; 0 ignored;"),
            "child failed: {stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        println!("AUTO_INVENTORY_ADMISSION_CHILD_OK");
        return;
    }
    use crate::heal::storage::ECStoreHealStorage;
    use crate::heal::storage_api::{
        EcstoreBackgroundMemoryBudget as BackgroundMemoryBudget, EcstoreLocalPublicationPause as LocalPublicationPause,
        EcstoreLocalPublicationStage as LocalPublicationStage,
    };
    let memory = BackgroundMemoryBudget::new();
    assert!(memory.install(), "fresh child owns installed policy");
    let memory_owners = Arc::strong_count(&memory);
    let node = resume_node(true);
    let physical = rustfs_concurrency::adaptive::AdaptiveAdmission::new(std::num::NonZeroU16::MIN, 1);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("native test runtime");
    let fixture = runtime.block_on(async {
        let (fixture, saved) = auto_format_fixture(Some(node.clone()), Some(physical.clone()), true).await;
        let disks = auto_inventory_local_disks(&fixture).await;
        let storage = Arc::new(ECStoreHealStorage::new(fixture._store.ecstore.clone()).with_admission(Some(node.clone())));
        let mut first_pause = LocalPublicationPause::install(
            disks[0].as_ref(),
            crate::heal::RUSTFS_META_BUCKET,
            "format.json",
            LocalPublicationStage::NativeReadOpen,
        )
        .expect("actual first native inventory probe");
        let mut next_pause = LocalPublicationPause::install(
            disks[1].as_ref(),
            crate::heal::RUSTFS_META_BUCKET,
            "format.json",
            LocalPublicationStage::NativeReadOpen,
        )
        .expect("actual following native inventory probe");
        let token = CancellationToken::new();
        let probe_token = token.clone();
        let mut probe = tokio::spawn(async move { storage.replacement_intent_disks(SET, Some(&probe_token)).await });
        tokio::time::timeout(Duration::from_secs(5), first_pause.entered())
            .await
            .expect("actual native work must enter")
            .expect("native entry witness");
        let funded = (node.snapshot().active, physical.snapshot().active);
        token.cancel();
        let yielded = tokio::time::timeout(Duration::from_secs(2), async {
            while node.snapshot().active != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        let cancelled_ownership = (
            node.snapshot().active,
            physical.snapshot().active,
            Arc::strong_count(&memory) > memory_owners,
        );
        let first = tokio::time::timeout(Duration::from_millis(30), &mut probe).await;
        let held = first.is_err();
        drop(first_pause);
        let next_entered = tokio::time::timeout(Duration::from_millis(400), next_pause.entered())
            .await
            .is_ok();
        drop(next_pause);
        let result = match first {
            Ok(joined) => joined,
            Err(_) => tokio::time::timeout(Duration::from_secs(5), &mut probe)
                .await
                .expect("same inventory future joins after native release"),
        }
        .expect("inventory task must not panic");
        assert_eq!(funded, (1, 1));
        assert!(yielded, "cancellation returns node scheduling while native work stays held");
        assert!(held, "the original inventory future must await its started native operation");
        assert_eq!(
            cancelled_ownership,
            (0, 1, true),
            "physical and memory capability remain with native work"
        );
        assert!(!next_entered, "cancellation must not start the following real disk probe");
        assert!(
            matches!(result, Err(Error::TransientSkip { .. })),
            "cancelled inventory cannot become empty evidence"
        );
        assert_eq!(node.snapshot().active, 0);
        assert_eq!(physical.snapshot().active, 0);
        assert_eq!(node.completed(), Some(0));
        let (path, original) = saved.expect("unread durable fixture");
        assert_eq!(
            fixture
                .disk
                .read_all(crate::heal::RUSTFS_META_BUCKET, &path)
                .await
                .expect("durable fixture remains")
                .as_ref(),
            original.as_slice()
        );
        fixture
    });
    drop(runtime);
    assert_eq!(physical.snapshot().active, 0);
    assert_eq!(Arc::strong_count(&memory), memory_owners, "native memory capability owners are released");
    drop(fixture);
}

#[cfg(not(windows))]
#[tokio::test]
#[serial_test::serial(replacement_auto_scan)]
async fn auto_inventory_admission_deferral_skips_existing_local_recovery_snapshot() {
    use crate::heal::storage_api::{
        EcstoreLocalPublicationPause as LocalPublicationPause, EcstoreLocalPublicationStage as LocalPublicationStage,
    };
    let fixture = ScannerFixture::new(false).await;
    let disk = crate::heal::local_disk_map_read()
        .await
        .values()
        .flatten()
        .next()
        .cloned()
        .expect("existing local recovery snapshot member");
    let volume = format!("{}/{}/ahm-replacement", crate::heal::RUSTFS_META_BUCKET, crate::heal::BUCKET_META_PREFIX);
    match disk.make_volume(&volume).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(error) => panic!("dedicated local intent fixture directory: {error}"),
    }
    let task = Uuid::new_v4().to_string();
    let path = format!("{}/ahm-replacement/{task}_ahm_replacement_intent.json", crate::heal::BUCKET_META_PREFIX);
    let original = b"unavailable inventory cannot classify an unread intent as corruption".to_vec();
    disk.write_all(crate::heal::RUSTFS_META_BUCKET, &path, original.clone().into())
        .await
        .expect("seed unread local intent");
    let survivor_path = format!(
        "{}/ahm-replacement/{}_ahm_replacement_intent.json",
        crate::heal::BUCKET_META_PREFIX,
        fixture.task_id
    );
    let survivor_before = fixture
        .storage
        .survivor
        .read_all(crate::heal::RUSTFS_META_BUCKET, &survivor_path)
        .await
        .expect("durable survivor intent");
    fixture.storage.inventory_defers.store(true, Ordering::SeqCst);
    let mut pause = LocalPublicationPause::install(
        disk.as_ref(),
        crate::heal::RUSTFS_META_BUCKET,
        &path,
        LocalPublicationStage::NativeReadOpen,
    )
    .expect("actual unread intent load pause");
    fixture
        .manager
        .start_auto_disk_scanner()
        .await
        .expect("actual Auto slot1 starts");
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.storage.inventory_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual slot1 inventory boundary must be reached");
    let entered = tokio::time::timeout(Duration::from_millis(400), pause.entered()).await;
    let no_native_load = entered.is_err();
    drop(pause);
    tokio::time::timeout(Duration::from_secs(5), fixture.manager.stop())
        .await
        .expect("Auto slot1 joins after releasing any native read")
        .expect("manager stop succeeds");
    assert!(
        no_native_load,
        "the already-captured local recovery map must not bypass inventory deferral"
    );
    assert!(
        fixture.manager.replacement_recovery_blocks().is_empty(),
        "unread intent is not durable corruption"
    );
    assert_eq!(
        fixture.manager.get_queue_length().await,
        0,
        "same-set evidence remains deferred for this tick"
    );
    assert_eq!(
        disk.read_all(crate::heal::RUSTFS_META_BUCKET, &path)
            .await
            .expect("local intent remains")
            .as_ref(),
        original.as_slice()
    );
    assert_eq!(
        fixture
            .storage
            .survivor
            .read_all(crate::heal::RUSTFS_META_BUCKET, &survivor_path)
            .await
            .expect("survivor remains"),
        survivor_before
    );
}
