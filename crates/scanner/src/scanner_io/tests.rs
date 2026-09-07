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

use super::dirty_usage::{
    DirtyUsageBucketScope, clear_dirty_usage_buckets_for_tests, dirty_usage_bucket_scopes_for_tests,
    dirty_usage_buckets_for_tests,
};
use super::io_disk::tier_stats_template;
use super::*;
use crate::scanner_budget::ScannerCycleBudgetConfig;
use crate::scanner_folder::ScannerItem;
use crate::storage_api::EcstoreScannerPeerDirtyUsageSnapshot;
use crate::storage_api::owner::{
    EcstorePoolDecommissionInfo, EcstoreRebalStatus, EcstoreRebalanceInfo, EcstoreRebalanceMeta, EcstoreRebalanceStats,
    ecstore_hold_namespace_commit,
};
use crate::storage_api::scan::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions, ObjectIO as _};
use crate::{
    DiskOption, ECStore, Endpoint, EndpointServerPools, Endpoints, InstanceContext, PoolEndpoints, ScannerObjectOptions,
    ScannerPutObjReader, UNKNOWN_TIER, init_bucket_metadata_sys_for_scanner_tests, init_ecstore_config_for_scanner_tests,
    init_local_disks_with_instance_ctx, new_disk, path2_bucket_object_with_base_path,
};
use rustfs_concurrency::{
    AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot, WorkloadAdmissionSnapshotProvider,
    WorkloadClass,
};
use rustfs_filemeta::FileInfo;
use serial_test::serial;
use std::collections::BTreeMap;
use std::sync::Arc;
use temp_env::with_var;
use time::OffsetDateTime;
use uuid::Uuid;

mod scoped_entry_fallback;
mod service_cohort;

#[derive(Clone)]
struct FixedWorkloadProvider {
    snapshot: WorkloadAdmissionRegistrySnapshot,
}

impl WorkloadAdmissionSnapshotProvider for FixedWorkloadProvider {
    fn workload_admission_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot {
        self.snapshot.clone()
    }
}

fn install_scanner_workload_provider(snapshot: WorkloadAdmissionRegistrySnapshot) {
    crate::set_scanner_workload_admission_snapshot_provider(Arc::new(FixedWorkloadProvider { snapshot }));
}

fn bucket_info(name: &str) -> BucketInfo {
    BucketInfo {
        name: name.to_string(),
        created: None,
        deleted: None,
        versioning: false,
        object_locking: false,
    }
}

#[test]
fn scanner_activity_preflight_defers_a_temporarily_offline_peer() {
    let preflight = scanner_activity_preflight(Err("peer rustfs-node3:9000 is temporarily offline".to_string()));

    match preflight {
        ScannerActivityPreflight::ActivityBaselineUnavailable(error) => {
            assert_eq!(error, "peer rustfs-node3:9000 is temporarily offline");
        }
        ScannerActivityPreflight::Ready(_) | ScannerActivityPreflight::DataMovement => {
            panic!("an unavailable activity baseline must defer the scanner cycle");
        }
    }
}

async fn setup_two_pool_scanner_store() -> (tempfile::TempDir, Arc<ECStore>) {
    init_ecstore_config_for_scanner_tests();
    let temp_dir = tempfile::tempdir().expect("multi-pool scanner test directory should be created");
    let mut pools = Vec::new();
    for pool_index in 0..2 {
        let mut endpoints = Vec::new();
        for disk_index in 0..4 {
            let disk_path = temp_dir.path().join(format!("pool{pool_index}-disk{disk_index}"));
            tokio::fs::create_dir_all(&disk_path)
                .await
                .expect("multi-pool scanner test disk should be created");
            let mut endpoint =
                Endpoint::try_from(disk_path.to_str().expect("disk path should be utf8")).expect("endpoint should parse");
            endpoint.set_pool_index(pool_index);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            endpoints.push(endpoint);
        }
        pools.push(PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: format!("scanner-cycle-pool-{pool_index}"),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        });
    }

    let endpoint_pools = EndpointServerPools::from(pools);
    let instance_ctx = Arc::new(InstanceContext::new());
    init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
        .await
        .expect("multi-pool local disks should initialize");
    let store = ECStore::new_with_instance_ctx(
        "127.0.0.1:0".parse().expect("test address should parse"),
        endpoint_pools,
        CancellationToken::new(),
        instance_ctx,
    )
    .await
    .expect("multi-pool ECStore should initialize");
    init_bucket_metadata_sys_for_scanner_tests(store.clone()).await;

    (temp_dir, store)
}

async fn wait_for_namespace_commit_tails(store: &ECStore) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while store.scanner_data_usage_publication_blocked().await {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("namespace commit tails should drain before the scanner fixture runs");
}

#[tokio::test]
#[serial]
async fn checkpoint_fixture_bucket_identity_uses_its_set_instance_owner() {
    let (_first_dir, first) = setup_two_pool_scanner_store().await;
    first
        .make_bucket("checkpoint-identity", &MakeBucketOptions::default())
        .await
        .expect("first instance bucket");
    let first_identity =
        scanner_bucket_checkpoint_identity(&first.pools[0].disk_set[0], "checkpoint-identity", 0, 7, HealScanMode::Normal)
            .await
            .expect("first durable identity");
    let (_second_dir, second) = setup_two_pool_scanner_store().await;
    second
        .make_bucket("checkpoint-identity", &MakeBucketOptions::default())
        .await
        .expect("second instance bucket");
    let second_identity =
        scanner_bucket_checkpoint_identity(&second.pools[0].disk_set[0], "checkpoint-identity", 0, 7, HealScanMode::Normal)
            .await
            .expect("second durable identity");
    assert_ne!(first_identity.bucket_incarnation, second_identity.bucket_incarnation);
    assert_eq!(
        scanner_bucket_checkpoint_identity(&first.pools[0].disk_set[0], "checkpoint-identity", 0, 7, HealScanMode::Normal)
            .await
            .expect("first owner remains bound"),
        first_identity
    );
    assert!(
        scanner_bucket_checkpoint_identity(&first.pools[0].disk_set[0], "missing-checkpoint-bucket", 0, 7, HealScanMode::Normal)
            .await
            .is_err()
    );
}

#[tokio::test]
#[serial]
async fn scanner_cache_locks_block_same_source_workers() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    let set = &store.pools[0].disk_set[0];
    let source = DataUsageCacheSource::new(0, 0);
    let cache_name = "photos/.usage-cache.bin";

    let guards = acquire_scanner_cache_locks(set.as_ref(), cache_name, source)
        .await
        .expect("scanner cache locks should be acquired");
    let scoped_lock = set
        .new_ns_lock(RUSTFS_META_BUCKET, &scanner_cache_lock_resource(cache_name, source))
        .await
        .expect("scoped scanner cache lock should be created");
    let scoped_err = scoped_lock
        .get_write_lock_quiet(Duration::from_millis(100))
        .await
        .expect_err("same-source workers must be blocked while scanner cache lock is held");
    assert!(matches!(scoped_err, LockError::Timeout { .. } | LockError::AlreadyLocked { .. }));

    drop(guards);
    acquire_scanner_cache_locks(set.as_ref(), cache_name, source)
        .await
        .expect("scanner cache locks should be released when guards drop");
}

#[tokio::test]
#[serial]
async fn scanner_cache_locks_allow_cross_source_workers() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    let first_set = &store.pools[0].disk_set[0];
    let second_set = &store.pools[1].disk_set[0];
    let cache_name = "photos/.usage-cache.bin";

    let first = acquire_scanner_cache_locks(first_set.as_ref(), cache_name, DataUsageCacheSource::new(0, 0))
        .await
        .expect("first source scanner cache locks should be acquired");
    let second = acquire_scanner_cache_locks(second_set.as_ref(), cache_name, DataUsageCacheSource::new(1, 0))
        .await
        .expect("different source scanner cache locks should not contend");

    assert!(!first.is_lock_lost());
    assert!(!second.is_lock_lost());
}

#[tokio::test]
async fn scanner_set_cache_admission_tracks_owner_snapshot_and_fails_closed() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    let set = store.pools[0].disk_set[0].clone();

    assert!(
        set.scanner_data_usage_publication_admission_guard().await.is_some(),
        "a set should refresh the idle owner snapshot before its first publication"
    );
    assert!(!store.scanner_data_usage_publication_blocked().await);
    assert!(
        set.scanner_data_usage_publication_admission_guard().await.is_some(),
        "an idle owner snapshot should admit the set cache"
    );

    let mut pool_stats = vec![EcstoreRebalanceStats::default(); store.pools.len()];
    pool_stats[0] = EcstoreRebalanceStats {
        participating: true,
        info: EcstoreRebalanceInfo {
            start_time: Some(OffsetDateTime::now_utc()),
            status: EcstoreRebalStatus::Started,
            ..Default::default()
        },
        ..Default::default()
    };
    *store.rebalance_meta.write().await = Some(EcstoreRebalanceMeta {
        id: Uuid::new_v4().to_string(),
        pool_stats,
        ..Default::default()
    });
    assert!(store.scanner_data_usage_publication_blocked().await);
    assert!(
        set.scanner_data_usage_publication_admission_guard().await.is_none(),
        "active movement must keep set cache publication blocked"
    );

    *store.rebalance_meta.write().await = None;
    assert!(!store.scanner_data_usage_publication_blocked().await);
    assert!(
        set.scanner_data_usage_publication_admission_guard().await.is_some(),
        "an idle owner refresh must make set cache publication live again"
    );
}

#[tokio::test]
#[serial]
async fn scanner_cycle_is_deferred_while_rebalance_is_active() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    let mut pool_stats = vec![EcstoreRebalanceStats::default(); store.pools.len()];
    pool_stats[0] = EcstoreRebalanceStats {
        participating: true,
        info: EcstoreRebalanceInfo {
            start_time: Some(OffsetDateTime::now_utc()),
            status: EcstoreRebalStatus::Started,
            ..Default::default()
        },
        ..Default::default()
    };
    *store.rebalance_meta.write().await = Some(EcstoreRebalanceMeta {
        id: Uuid::new_v4().to_string(),
        pool_stats,
        ..Default::default()
    });
    assert!(store.scanner_data_movement_active().await);

    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    let (updates, mut receiver) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        ScannerIOCycle::nsscanner_with_status(store.as_ref(), ctx, budget, updates, 1, 1, HealScanMode::Normal),
    )
    .await
    .expect("rebalance-deferred scanner cycle should finish")
    .expect("rebalance-deferred scanner cycle should succeed");

    assert_eq!(result.status, ScannerCycleStatus::Deferred(ScannerCycleDeferReason::DataMovement));
    assert!(receiver.recv().await.is_none(), "rebalance-deferred cycle must not publish usage");
}

#[tokio::test]
#[serial]
async fn scanner_cycle_is_deferred_while_terminal_decommission_is_blocked() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    for decommission in [
        EcstorePoolDecommissionInfo {
            failed: true,
            ..Default::default()
        },
        EcstorePoolDecommissionInfo {
            canceled: true,
            ..Default::default()
        },
    ] {
        store.pool_meta.write().await.pools[0].decommission = Some(decommission);
        assert!(store.scanner_data_usage_publication_blocked().await);

        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let (updates, mut receiver) = mpsc::channel(1);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            ScannerIOCycle::nsscanner_with_status(store.as_ref(), ctx, budget, updates, 1, 1, HealScanMode::Normal),
        )
        .await
        .expect("terminal-decommission-deferred scanner cycle should finish")
        .expect("terminal-decommission-deferred scanner cycle should succeed");

        assert_eq!(result.status, ScannerCycleStatus::Deferred(ScannerCycleDeferReason::DataMovement));
        assert!(receiver.recv().await.is_none(), "blocked cycle must not publish usage");
    }
}

#[tokio::test]
#[serial]
async fn scoped_scan_production_entry_preserves_deep_and_full_maintenance_work() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in ["hot-bucket", "cold-bucket"] {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = ScannerPutObjReader::from_vec(b"initial".to_vec());
        store.pools[0].disk_set[0]
            .put_object(bucket, "initial", &mut reader, &ScannerObjectOptions::default())
            .await
            .expect("initial object should persist");
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let mut baseline = None;
    for (index, (scan_mode, requires_full_scan, explicit_scope)) in [
        (HealScanMode::Normal, true, false),
        (HealScanMode::Normal, false, false),
        (HealScanMode::Deep, false, false),
        (HealScanMode::Normal, true, false),
        (HealScanMode::Deep, false, true),
        (HealScanMode::Normal, true, true),
    ]
    .into_iter()
    .enumerate()
    {
        if index > 0 {
            let mut reader = ScannerPutObjReader::from_vec(b"maintenance".to_vec());
            store.pools[0].disk_set[0]
                .put_object("cold-bucket", &format!("added-{index}"), &mut reader, &ScannerObjectOptions::default())
                .await
                .expect("cold bucket mutation should persist");
            wait_for_namespace_commit_tails(store.as_ref()).await;
            // Only the hot bucket is in the usage hint. The cold result must
            // come from this cycle's storage walk, not its previous baseline.
            record_dirty_usage_bucket("hot-bucket");
        }
        let requested_scope = if explicit_scope {
            ScannerBucketScanScope::from_dirty_buckets(
                HashSet::from(["hot-bucket".to_string()]),
                HashMap::new(),
                DataUsageScanPlanDigest([7; 32]),
            )
        } else {
            ScannerBucketScanScope::default()
        };
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let (updates, mut receiver) = mpsc::channel(1);
        let (observer, observed_scope) = tokio::sync::oneshot::channel();
        let cycle = u64::try_from(index + 1).expect("test cycle should fit");
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            nsscanner_with_storage_status_scoped(
                store.as_ref(),
                ScannerCycleRequest {
                    ctx,
                    budget,
                    updates,
                    want_cycle: cycle,
                    leader_epoch: 11,
                    scan_mode,
                    scan_scope: requested_scope,
                    persisted_usage_baseline: baseline,
                    observed_usage_candidate: None,
                    requires_full_scan,
                    resolved_scope_observer: Some(observer),
                    service_cohort: None,
                },
            ),
        )
        .await
        .expect("cycle should finish within the test deadline")
        .expect("cycle should succeed");
        assert_eq!(result.status, ScannerCycleStatus::Complete, "cycle {cycle}");
        let resolved = observed_scope.await.expect("production resolver should report its scope");
        if index == 1 {
            assert_eq!(
                resolved.selected_buckets.as_deref(),
                Some(&HashSet::from(["hot-bucket".to_string()])),
                "ordinary dirty work must retain the existing planner"
            );
        } else {
            assert!(resolved.is_default(), "cycle {cycle} must visit the full maintenance scope");
        }
        let mut snapshot = receiver.recv().await.expect("cycle should publish a snapshot");
        assert!(snapshot.usage_snapshot_complete, "cycle {cycle}");
        assert_eq!(
            snapshot.buckets_usage["cold-bucket"].objects_count,
            u64::try_from(index + 1).expect("count should fit")
        );
        assert_eq!(snapshot.buckets_usage["hot-bucket"].objects_count, 1);
        assert_eq!(snapshot.scanner_cycle, Some(cycle));
        assert_eq!(snapshot.scanner_epoch, Some(11));
        snapshot.usage_snapshot_converged = Some(true);
        baseline = Some(Bytes::from(serde_json::to_vec(&snapshot).expect("complete baseline should encode")));
    }
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn scoped_scan_same_cycle_maintenance_rewalks_after_root_delivery_failure() {
    for (scan_mode, requires_full_scan) in [
        (HealScanMode::Normal, false),
        (HealScanMode::Deep, false),
        (HealScanMode::Normal, true),
    ] {
        let (_temp_dir, store) = setup_two_pool_scanner_store().await;
        clear_dirty_usage_buckets_for_tests();
        for bucket in ["hot-bucket", "cold-bucket"] {
            store
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created");
            let mut reader = ScannerPutObjReader::from_vec(b"initial".to_vec());
            store.pools[0].disk_set[0]
                .put_object(bucket, "initial", &mut reader, &ScannerObjectOptions::default())
                .await
                .expect("initial object should persist");
            let lock = store.pools[0].disk_set[0]
                .new_ns_lock(bucket, "initial")
                .await
                .expect("fixture namespace lock should be created");
            let _settled = lock
                .get_write_lock(Duration::from_secs(30))
                .await
                .expect("fixture rename tail should finish before the usage scan");
        }
        wait_for_namespace_commit_tails(&store).await;
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let (updates, receiver) = mpsc::channel(1);
        drop(receiver);
        let failed = tokio::time::timeout(
            Duration::from_secs(30),
            nsscanner_with_storage_status_scoped(
                store.as_ref(),
                ScannerCycleRequest {
                    ctx,
                    budget,
                    updates,
                    want_cycle: 7,
                    leader_epoch: 11,
                    scan_mode: HealScanMode::Normal,
                    scan_scope: ScannerBucketScanScope::default(),
                    persisted_usage_baseline: None,
                    observed_usage_candidate: None,
                    requires_full_scan: false,
                    resolved_scope_observer: None,
                    service_cohort: None,
                },
            ),
        )
        .await
        .expect("normal scan should finish")
        .expect_err("root delivery must fail after bucket cache persistence");
        assert!(failed.to_string().contains("receiver closed"), "{failed}");
        let cache_name = path_join_buf(&["cold-bucket", DATA_USAGE_CACHE_NAME]);
        let mut cached = DataUsageCache::default();
        cached
            .load(store.pools[0].disk_set[0].clone(), &cache_name)
            .await
            .expect("normal bucket cache should have committed");
        assert!(cached.info.snapshot_complete);
        assert_eq!(cached.info.next_cycle, 7);
        assert_eq!(
            cached
                .checked_flatten("cold-bucket")
                .expect("cached root should be valid")
                .objects,
            1
        );

        let mut reader = ScannerPutObjReader::from_vec(b"maintenance".to_vec());
        store.pools[0].disk_set[0]
            .put_object("cold-bucket", "new", &mut reader, &ScannerObjectOptions::default())
            .await
            .expect("new cold object should persist");
        wait_for_namespace_commit_tails(&store).await;
        record_dirty_usage_bucket("hot-bucket");
        if scan_mode == HealScanMode::Normal && !requires_full_scan {
            record_dirty_usage_bucket("cold-bucket");
        }
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let (updates, mut receiver) = mpsc::channel(1);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            nsscanner_with_storage_status_scoped(
                store.as_ref(),
                ScannerCycleRequest {
                    ctx,
                    budget,
                    updates,
                    want_cycle: 7,
                    leader_epoch: 11,
                    scan_mode,
                    scan_scope: ScannerBucketScanScope::default(),
                    persisted_usage_baseline: None,
                    observed_usage_candidate: None,
                    requires_full_scan,
                    resolved_scope_observer: None,
                    service_cohort: None,
                },
            ),
        )
        .await
        .expect("maintenance scan should finish")
        .expect("maintenance scan should succeed");
        assert_eq!(result.status, ScannerCycleStatus::Complete);
        let snapshot = receiver.recv().await.expect("maintenance snapshot should be published");
        assert_eq!(snapshot.scanner_cycle, Some(7));
        assert_eq!(
            snapshot.buckets_usage["cold-bucket"].objects_count, 2,
            "{scan_mode:?}/full={requires_full_scan} must not replay the same-cycle Normal root"
        );
        clear_dirty_usage_buckets_for_tests();
    }
}

#[tokio::test]
async fn data_usage_publish_fails_when_receiver_is_closed() {
    let (updates, receiver) = mpsc::channel(1);
    drop(receiver);

    let err = send_data_usage_update(&updates, DataUsageInfo::default())
        .await
        .expect_err("closed usage receiver must reject the scanner update");

    assert!(err.to_string().contains("receiver closed"));
}

#[tokio::test]
async fn data_usage_publish_rejects_a_second_terminal_update_without_blocking() {
    for (status, data_usage_info) in [
        (ScannerCycleStatus::Complete, DataUsageInfo::default()),
        (
            ScannerCycleStatus::Superseded,
            DataUsageInfo {
                scanner_cycle: Some(7),
                ..Default::default()
            },
        ),
    ] {
        let (updates, mut receiver) = mpsc::channel(1);
        assert!(
            publish_usage_snapshot(&updates, status, data_usage_info)
                .await
                .expect("first terminal update should be accepted")
        );

        let err =
            tokio::time::timeout(Duration::from_secs(1), publish_usage_snapshot(&updates, status, DataUsageInfo::default()))
                .await
                .expect("a full terminal update must fail without waiting")
                .expect_err("a second terminal update must be rejected");
        assert!(err.to_string().contains("already queued"));
        assert!(receiver.try_recv().is_ok(), "the first terminal update must remain owned by the receiver");
        assert!(receiver.try_recv().is_err(), "the rejected second update must not enter the channel");
    }
}

#[tokio::test]
#[serial]
async fn multi_pool_scanner_cycle_publishes_combined_usage() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    let bucket = format!("scanner-union-{}", Uuid::new_v4().simple());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket should be created across both pools");

    for (pool_index, (object, body)) in [("pool-a", b"first".as_slice()), ("pool-b", b"second".as_slice())]
        .into_iter()
        .enumerate()
    {
        let mut reader = ScannerPutObjReader::from_vec(body.to_vec());
        store.pools[pool_index].disk_set[0]
            .put_object(&bucket, object, &mut reader, &ScannerObjectOptions::default())
            .await
            .expect("object should be written to its selected pool");

        // Quorum ACK can precede tail publication on the disk chosen to scan.
        let lock = store.pools[pool_index].disk_set[0]
            .new_ns_lock(&bucket, object)
            .await
            .expect("fixture namespace lock should be created");
        let _settled = lock
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("fixture rename tail should finish before the usage scan");
    }

    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    let (updates, mut receiver) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        ScannerIOCycle::nsscanner_with_status(store.as_ref(), ctx, budget, updates, 1, 1, HealScanMode::Normal),
    )
    .await
    .expect("multi-pool scanner cycle should finish")
    .expect("multi-pool scanner cycle should succeed");

    assert_eq!(result.status, ScannerCycleStatus::Complete);
    let usage = receiver.recv().await.expect("complete scanner cycle should publish usage");
    let bucket_usage = usage
        .buckets_usage
        .get(&bucket)
        .expect("combined bucket usage should be present");
    assert_eq!(bucket_usage.objects_count, 2, "{usage:?}");
    assert_eq!(bucket_usage.size, 11);
    assert_eq!(usage.objects_total_count, 2);
    assert_eq!(usage.objects_total_size, 11);
    assert!(
        receiver.recv().await.is_none(),
        "a scanner cycle must publish at most one terminal usage snapshot"
    );
}

#[tokio::test]
#[serial]
async fn pending_put_commit_keeps_scanner_walk_live_without_authoritative_usage() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    let bucket = format!("scanner-pending-put-{}", Uuid::new_v4().simple());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket should be created across both pools");
    for (pool_index, (object, body)) in [("pool-a", b"first".as_slice()), ("pool-b", b"second".as_slice())]
        .into_iter()
        .enumerate()
    {
        let mut reader = ScannerPutObjReader::from_vec(body.to_vec());
        store.pools[pool_index].disk_set[0]
            .put_object(
                &bucket,
                object,
                &mut reader,
                &ScannerObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("fixture objects must finish their rename fanouts before scanning");
    }

    let mut pending = Some(ecstore_hold_namespace_commit(store.as_ref()));
    let mut previous_activity_digest = None;
    let mut structural_plan_digest = None;
    for (cycle, converged) in [(1, false), (2, true)] {
        if converged {
            drop(pending.take());
        }
        assert_eq!(store.scanner_data_usage_publication_blocked().await, !converged);
        assert!(!store.scanner_data_movement_pause_status().await.paused);
        let activity = crate::scanner::probe_scanner_activity(store.as_ref(), false)
            .await
            .expect("the fixture activity should be observable");
        let activity_digest = crate::scanner::scanner_activity_snapshot_digest(&activity);
        if let Some(previous) = previous_activity_digest.replace(activity_digest) {
            assert_ne!(previous, activity_digest, "draining a namespace commit must change the publication proof");
        }
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, ScannerCycleBudgetConfig::default());
        let (updates, mut receiver) = mpsc::channel(1);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            ScannerIOCycle::nsscanner_with_status(
                store.as_ref(),
                ctx,
                Arc::clone(&budget),
                updates,
                cycle,
                1,
                HealScanMode::Normal,
            ),
        )
        .await
        .expect("namespace scanning must finish while a PUT commit is pending")
        .expect("namespace scanning must remain available during a pending PUT commit");
        assert_eq!(result.activity_digest(), Some(activity_digest));
        if !converged {
            assert_eq!(budget.progress().0, 2, "the pending commit must not suppress actual object traversal");
        }
        assert_eq!(
            result.status,
            if converged {
                ScannerCycleStatus::Complete
            } else {
                ScannerCycleStatus::Superseded
            }
        );
        let usage = receiver
            .recv()
            .await
            .expect("the completed walk should produce a usage candidate");
        assert_eq!(usage.usage_snapshot_converged, Some(converged));
        assert_eq!(usage.scanner_cycle, Some(cycle));
        assert_eq!(usage.objects_total_count, 2);
        assert_eq!(usage.objects_total_size, 11);
        assert_eq!(usage.usage_snapshot_set_states.len(), 2);
        for state in &usage.usage_snapshot_set_states {
            let digest = state
                .scan_plan_digest
                .expect("each set must retain its structural cache identity");
            assert_eq!(*structural_plan_digest.get_or_insert(digest), digest);
        }
        let bucket_usage = usage.buckets_usage.get(&bucket).expect("the walked bucket must be present");
        assert_eq!(bucket_usage.objects_count, 2);
        assert_eq!(bucket_usage.size, 11);
        assert!(receiver.recv().await.is_none(), "each walk must emit exactly one terminal candidate");
    }
}

#[tokio::test]
#[serial]
async fn multi_pool_scanner_cycle_zero_fills_bucket_absent_from_first_pool() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    let bucket = format!("scanner-second-pool-{}", Uuid::new_v4().simple());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket and its authoritative metadata should be created");
    let body = b"second-only";
    let mut reader = ScannerPutObjReader::from_vec(body.to_vec());
    store.pools[1]
        .put_object(&bucket, "pool-b", &mut reader, &ScannerObjectOptions::default())
        .await
        .expect("object should be written only to the second pool");
    {
        let lock = store.pools[1].disk_set[0]
            .new_ns_lock(&bucket, "pool-b")
            .await
            .expect("fixture namespace lock should be created");
        let _settled = lock
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("fixture rename tail should finish before the usage scan");
    }
    store.pools[0]
        .delete_bucket(&bucket, &DeleteBucketOptions::default())
        .await
        .expect("bucket should be removed from the first pool only");
    init_bucket_metadata_sys_for_scanner_tests(store.clone()).await;

    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    let (updates, mut receiver) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        ScannerIOCycle::nsscanner_with_status(store.as_ref(), ctx, budget, updates, 1, 1, HealScanMode::Normal),
    )
    .await
    .expect("second-pool-only scanner cycle should finish")
    .expect("second-pool-only scanner cycle should succeed");

    assert_eq!(result.status, ScannerCycleStatus::Complete);
    let usage = receiver.recv().await.expect("complete scanner cycle should publish usage");
    let bucket_usage = usage
        .buckets_usage
        .get(&bucket)
        .expect("second-pool-only bucket usage should be present");
    assert_eq!(bucket_usage.objects_count, 1);
    assert_eq!(bucket_usage.size, u64::try_from(body.len()).expect("test body length should fit u64"));
    assert_eq!(usage.objects_total_count, 1);
    assert_eq!(
        usage.objects_total_size,
        u64::try_from(body.len()).expect("test body length should fit u64")
    );
}

#[tokio::test]
async fn scanner_item_object_lock_uses_cached_config() {
    let temp_dir = std::env::temp_dir();
    let cached = Arc::new(ObjectLockConfiguration {
        object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
        ..Default::default()
    });
    let item = ScannerItem {
        path: temp_dir.join("object").to_string_lossy().to_string(),
        bucket: "bucket".to_string(),
        prefix: String::new(),
        object_name: "object".to_string(),
        file_type: std::fs::metadata(&temp_dir)
            .expect("temp dir metadata should be readable")
            .file_type(),
        lifecycle: None,
        object_lock: Some(cached.clone()),
        replication: None,
        heal_enabled: false,
        heal_bitrot: false,
        debug: false,
    };

    let resolved = object_lock_config_for_scanner_item(&item)
        .await
        .expect("cached object-lock config should resolve");

    assert!(Arc::ptr_eq(&resolved, &cached));
}

#[test]
fn object_lock_config_enabled_accepts_enabled_only() {
    let enabled = ObjectLockConfiguration {
        object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
        ..Default::default()
    };

    assert!(object_lock_config_enabled(&enabled));
    assert!(!object_lock_config_enabled(&ObjectLockConfiguration::default()));
}

#[test]
#[serial]
fn dirty_usage_snapshot_clear_preserves_newer_generation() {
    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket("photos");
    let buckets = vec![bucket_info("photos")];
    let snapshot = snapshot_dirty_usage_buckets(&buckets, dirty_usage_generation());

    record_dirty_usage_bucket("photos");
    clear_dirty_usage_buckets(&snapshot.buckets);

    assert_eq!(dirty_usage_bucket_count(), 1);
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn dirty_usage_generation_acknowledgement_preserves_newer_mutations() {
    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket("photos");
    let acknowledged_generation = scanner_dirty_usage_state().generation;
    record_dirty_usage_bucket("videos");

    acknowledge_dirty_usage_generation(scanner_activity_epoch(), acknowledged_generation)
        .expect("a matching process and prior generation should be acknowledged");
    acknowledge_dirty_usage_generation(scanner_activity_epoch(), acknowledged_generation)
        .expect("replaying an acknowledged generation should be idempotent");

    let pending = dirty_usage_buckets_for_tests();
    assert!(!pending.contains_key("photos"));
    assert!(pending.contains_key("videos"));
    assert!(scanner_dirty_usage_state().pending);
    drop(pending);

    let remaining_generation = scanner_dirty_usage_state().generation;
    acknowledge_dirty_usage_generation(scanner_activity_epoch(), remaining_generation)
        .expect("the remaining generation should be acknowledged");
    assert!(!scanner_dirty_usage_state().pending);
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn dirty_usage_snapshot_is_sorted_and_reports_its_cutoff() {
    clear_dirty_usage_buckets_for_tests();
    let empty = scanner_dirty_usage_snapshot(0);
    assert_eq!(empty.pending_bucket_count, 0);
    assert!(empty.complete);
    assert!(empty.buckets.is_empty());

    record_dirty_usage_bucket("videos");
    record_dirty_usage_bucket("photos");
    let expected_generation = scanner_dirty_usage_state().generation;

    let snapshot = scanner_dirty_usage_snapshot(2);

    assert_eq!(snapshot.generation, expected_generation);
    assert_eq!(snapshot.pending_bucket_count, 2);
    assert!(snapshot.complete);
    assert_eq!(
        snapshot
            .buckets
            .iter()
            .map(|bucket| bucket.bucket.as_str())
            .collect::<Vec<_>>(),
        vec!["photos", "videos"]
    );
    assert!(snapshot.buckets.iter().all(|bucket| bucket.generation <= snapshot.generation));
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn dirty_usage_object_marks_only_its_top_level_entry_until_the_scope_becomes_ambiguous() {
    clear_dirty_usage_buckets_for_tests();

    record_dirty_usage_object("photos", "2026/january/object-a");
    record_dirty_usage_object("photos", "archive/object-b");
    let scopes = dirty_usage_bucket_scopes_for_tests();
    assert_eq!(
        scopes.get("photos"),
        Some(&DirtyUsageBucketScope::TopLevelEntries(HashSet::from([
            "2026".to_string(),
            "archive".to_string(),
        ])))
    );
    drop(scopes);

    record_dirty_usage_object("photos", "../ambiguous");
    assert_eq!(
        dirty_usage_bucket_scopes_for_tests().get("photos"),
        Some(&DirtyUsageBucketScope::WholeBucket)
    );
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn dirty_usage_object_expands_an_overfull_prefix_journal_to_the_whole_bucket() {
    clear_dirty_usage_buckets_for_tests();

    for index in 0..129 {
        record_dirty_usage_object("photos", &format!("prefix-{index}/object"));
    }

    assert_eq!(
        dirty_usage_bucket_scopes_for_tests().get("photos"),
        Some(&DirtyUsageBucketScope::WholeBucket)
    );
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn dirty_usage_snapshot_marks_truncated_results_incomplete() {
    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket("archive");
    record_dirty_usage_bucket("photos");

    let snapshot = scanner_dirty_usage_snapshot(1);

    assert_eq!(snapshot.pending_bucket_count, 2);
    assert!(!snapshot.complete);
    assert!(snapshot.buckets.is_empty(), "incomplete snapshots must not expose a partial bucket list");
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn dirty_usage_generation_acknowledgement_rejects_stale_process_and_future_generation() {
    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket("photos");
    let generation = scanner_dirty_usage_state().generation;

    assert_eq!(
        acknowledge_dirty_usage_generation("stale-process", generation),
        Err(ScannerDirtyUsageAckError::ProcessChanged)
    );
    assert_eq!(
        acknowledge_dirty_usage_generation(scanner_activity_epoch(), 0),
        Err(ScannerDirtyUsageAckError::InvalidGeneration)
    );
    assert_eq!(
        acknowledge_dirty_usage_generation(scanner_activity_epoch(), u64::MAX),
        Err(ScannerDirtyUsageAckError::InvalidGeneration)
    );
    assert_eq!(
        acknowledge_dirty_usage_generation(
            scanner_activity_epoch(),
            generation.checked_add(1).expect("test generation should not be exhausted")
        ),
        Err(ScannerDirtyUsageAckError::InvalidGeneration)
    );
    assert!(dirty_usage_buckets_for_tests().contains_key("photos"));
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn dirty_usage_snapshot_detects_uncovered_generation() {
    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket("photos");
    let buckets = vec![bucket_info("photos")];
    let snapshot = snapshot_dirty_usage_buckets(&buckets, dirty_usage_generation());

    assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Current);

    record_dirty_usage_bucket("photos");

    assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Changed);
    clear_dirty_usage_buckets_for_tests();
}

#[test]
fn generation_saturates_instead_of_wrapping() {
    let generation = AtomicU64::new(u64::MAX - 1);

    assert_eq!(advance_generation(&generation), u64::MAX);
    assert_eq!(advance_generation(&generation), u64::MAX);
    assert_eq!(generation.load(Ordering::Acquire), u64::MAX);
}

#[test]
#[serial]
fn dirty_usage_snapshot_clears_a_stably_absent_bucket_after_durable_save() {
    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket("photos");
    record_dirty_usage_bucket("temporarily-omitted");
    let generation_before_bucket_list = dirty_usage_generation();

    let snapshot = snapshot_dirty_usage_buckets(&[bucket_info("photos")], generation_before_bucket_list);

    assert!(snapshot.buckets.contains_key("photos"));
    assert!(snapshot.buckets.contains_key("temporarily-omitted"));
    assert!(dirty_usage_buckets().contains_key("temporarily-omitted"));
    assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Current);

    let acknowledgements =
        ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(snapshot.buckets.as_ref().clone())).clear_verified_usage();
    assert!(acknowledgements.is_empty());
    assert!(!dirty_usage_buckets().contains_key("temporarily-omitted"));
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn dirty_usage_snapshot_preserves_an_absent_bucket_recorded_after_listing_started() {
    clear_dirty_usage_buckets_for_tests();
    let generation_before_bucket_list = dirty_usage_generation();
    record_dirty_usage_bucket("new-or-racing-bucket");

    let snapshot = snapshot_dirty_usage_buckets(&[], generation_before_bucket_list);

    assert!(!snapshot.buckets.contains_key("new-or-racing-bucket"));
    assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Changed);
    assert!(dirty_usage_buckets().contains_key("new-or-racing-bucket"));
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn deleting_a_clean_bucket_invalidates_an_inflight_usage_snapshot() {
    clear_dirty_usage_buckets_for_tests();
    let snapshot = snapshot_dirty_usage_buckets(&[bucket_info("photos")], dirty_usage_generation());
    assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Current);

    record_dirty_usage_bucket("photos");

    assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Changed);
    assert!(dirty_usage_buckets().contains_key("photos"));
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn deleting_a_bucket_during_listing_invalidates_the_resulting_usage_snapshot() {
    clear_dirty_usage_buckets_for_tests();
    let generation_before_bucket_list = dirty_usage_generation();

    record_dirty_usage_bucket("photos");
    let snapshot = snapshot_dirty_usage_buckets(&[bucket_info("photos")], generation_before_bucket_list);

    assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Changed);
    assert!(dirty_usage_buckets().contains_key("photos"));
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn scanner_maintenance_change_advances_generation_and_marks_usage_dirty() {
    clear_dirty_usage_buckets_for_tests();
    let generation = scanner_maintenance_generation();

    record_scanner_maintenance_change("photos");

    assert!(scanner_maintenance_generation() > generation);
    assert!(dirty_usage_buckets().contains_key("photos"));
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn dirty_usage_clear_excludes_failed_buckets() {
    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket("photos");
    record_dirty_usage_bucket("videos");
    let buckets = vec![bucket_info("photos"), bucket_info("videos")];
    let snapshot = snapshot_dirty_usage_buckets(&buckets, dirty_usage_generation());
    let failed_buckets = HashSet::from(["videos".to_string()]);
    let clear_snapshot = dirty_usage_buckets_excluding_failed(&snapshot.buckets, &failed_buckets);

    clear_dirty_usage_buckets(&clear_snapshot);

    let dirty_buckets = dirty_usage_buckets();
    assert!(!dirty_buckets.contains_key("photos"));
    assert!(dirty_buckets.contains_key("videos"));
    drop(dirty_buckets);
    clear_dirty_usage_buckets_for_tests();
}

#[test]
fn dirty_usage_clear_plan_excludes_cache_save_failures() {
    let snapshot = DirtyUsageBuckets::from([("photos".to_string(), 1), ("videos".to_string(), 2)]);
    let failed_buckets = HashSet::from(["videos".to_string()]);

    let clear_snapshot = should_clear_dirty_usage_snapshot(true, true, false, true, &snapshot, &failed_buckets)
        .expect("successful completed cycle should produce a clear snapshot");

    assert!(clear_snapshot.contains_key("photos"));
    assert!(!clear_snapshot.contains_key("videos"));
}

#[test]
#[serial]
fn dirty_usage_is_acknowledged_only_after_durable_usage_confirmation() {
    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket("photos");
    let snapshot = snapshot_dirty_usage_buckets(&[bucket_info("photos")], dirty_usage_generation());

    let unconfirmed = ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(snapshot.buckets.as_ref().clone()));
    drop(unconfirmed);
    assert!(dirty_usage_buckets().contains_key("photos"));

    let confirmed = ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(snapshot.buckets.as_ref().clone()));
    let acknowledgements = confirmed.clear_verified_usage();
    assert!(acknowledgements.is_empty());
    assert!(!dirty_usage_buckets().contains_key("photos"));
    clear_dirty_usage_buckets_for_tests();
}

#[test]
#[serial]
fn clear_dirty_usage_bucket_removes_deleted_bucket_marker() {
    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket("photos");
    record_dirty_usage_bucket("videos");

    clear_dirty_usage_bucket("photos");

    let buckets = vec![bucket_info("photos"), bucket_info("videos")];
    let snapshot = snapshot_dirty_usage_buckets(&buckets, dirty_usage_generation());
    assert!(!snapshot.buckets.contains_key("photos"));
    assert!(snapshot.buckets.contains_key("videos"));
    assert_eq!(dirty_usage_bucket_count(), 1);
    clear_dirty_usage_buckets_for_tests();
}

#[test]
fn bucket_usage_scan_order_prioritizes_dirty_buckets() {
    let buckets = vec![bucket_info("missing"), bucket_info("cached"), bucket_info("dirty")];
    let mut old_cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: DATA_USAGE_ROOT.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    old_cache.replace("cached", DATA_USAGE_ROOT, DataUsageEntry::default());
    old_cache.replace("dirty", DATA_USAGE_ROOT, DataUsageEntry::default());

    let dirty_buckets = HashMap::from([("dirty".to_string(), 1)]);
    let ordered = bucket_usage_scan_order(&buckets, &old_cache, &dirty_buckets);
    let names = ordered.iter().map(|bucket| bucket.name.as_str()).collect::<Vec<_>>();

    assert_eq!(names, vec!["dirty", "missing", "cached"]);
}

fn complete_set_usage_cache(buckets: &[(&str, usize)], scan_plan_digest: DataUsageScanPlanDigest) -> DataUsageCache {
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: DATA_USAGE_ROOT.to_string(),
            next_cycle: 7,
            last_update: Some(SystemTime::now()),
            leader_epoch: 11,
            source: Some(DataUsageCacheSource::new(1, 2)),
            snapshot_complete: true,
            scan_plan_digest: Some(scan_plan_digest),
            scan_coverage_digest: Some(scan_plan_digest),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            tier_registry_generation: Some(13),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(DATA_USAGE_ROOT, "", DataUsageEntry::default());
    for (bucket, size) in buckets {
        cache.replace(
            bucket,
            DATA_USAGE_ROOT,
            DataUsageEntry {
                size: *size,
                objects: 1,
                ..Default::default()
            },
        );
    }
    cache
}

#[tokio::test]
#[serial]
async fn set_snapshot_reuse_requires_execution_identity_and_fences_stale_writers() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    let set = Arc::clone(&store.pools[0].disk_set[0]);
    let epoch = scanner_publication_epoch(Arc::clone(&set)).await.expect("idle set admission");
    let mut legacy = complete_set_usage_cache(&[("photos", 5)], DataUsageScanPlanDigest([1; 32]));
    legacy.info.source = Some(DataUsageCacheSource::new(0, 0));
    legacy
        .save(Arc::clone(&set), DATA_USAGE_CACHE_NAME)
        .await
        .expect("seed legacy set cache");
    let mut persisted = DataUsageCache::default();
    let initial = persisted
        .load_with_revisions(Arc::clone(&set), DATA_USAGE_CACHE_NAME)
        .await
        .expect("capture the shared starting revision");
    let mut fresh = legacy.clone();
    fresh.info.scan_execution_digest = Some(DataUsageScanPlanDigest([2; 32]));
    fresh.replace(
        "photos",
        DATA_USAGE_ROOT,
        DataUsageEntry {
            size: 20,
            objects: 1,
            ..Default::default()
        },
    );
    let cycle_floor = AtomicU64::new(fresh.info.next_cycle);
    let (tx, mut rx) = mpsc::channel(1);
    assert!(
        persist_and_publish_cache_snapshot(Arc::clone(&set), &tx, fresh.clone(), Some(&initial), &cycle_floor, epoch)
            .await
            .is_some(),
        "a legacy cache without execution identity must be refreshed"
    );
    let published = rx.try_recv().expect("fresh snapshot should be forwarded");
    assert_eq!(published.find("photos").expect("published bucket").size, 20);
    assert_eq!(published.info.scan_execution_digest, fresh.info.scan_execution_digest);
    let current = persisted
        .load_with_revisions(Arc::clone(&set), DATA_USAGE_CACHE_NAME)
        .await
        .expect("capture the current revision for the unidentified execution");

    let mut stale = legacy.clone();
    stale.info.scan_execution_digest = Some(DataUsageScanPlanDigest([3; 32]));
    for (candidate, revisions) in [(stale, &initial), (legacy, &current)] {
        assert!(
            persist_and_publish_cache_snapshot(Arc::clone(&set), &tx, candidate, Some(revisions), &cycle_floor, epoch)
                .await
                .is_none(),
            "a stale or unidentified execution must not replace the newer snapshot"
        );
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }
    fresh.info.scan_execution_digest = Some(DataUsageScanPlanDigest([4; 32]));
    assert!(
        persist_and_publish_cache_snapshot(Arc::clone(&set), &tx, fresh.clone(), None, &cycle_floor, epoch)
            .await
            .is_none(),
        "an unreadable starting revision must not authorize an overwrite"
    );

    fresh.info.scan_execution_digest = published.info.scan_execution_digest;
    fresh.replace("photos", DATA_USAGE_ROOT, DataUsageEntry::default());
    assert!(
        persist_and_publish_cache_snapshot(Arc::clone(&set), &tx, fresh, Some(&initial), &cycle_floor, epoch)
            .await
            .is_some(),
        "an overlapping identical execution must reuse the completed snapshot"
    );
    assert_eq!(
        rx.try_recv()
            .expect("reused snapshot")
            .find("photos")
            .expect("reused bucket")
            .size,
        20
    );
    persisted
        .load(Arc::clone(&set), DATA_USAGE_CACHE_NAME)
        .await
        .expect("read the final durable set cache");
    assert_eq!(persisted.find("photos").expect("durable bucket").size, 20);
    assert_eq!(persisted.info.scan_execution_digest, published.info.scan_execution_digest);

    let ctx = CancellationToken::new();
    let empty_execution = DataUsageScanPlanDigest([5; 32]);
    set.nsscanner_cache(
        ctx.clone(),
        ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default()),
        ScannerBucketScanPlan {
            service_cohort: None,
            buckets: Vec::new(),
            all_buckets: Arc::new(Vec::new()),
            scope: ScannerBucketScanScope::default(),
            digest: DataUsageScanPlanDigest([6; 32]),
            bucket_coverage_digest: DataUsageScanPlanDigest([6; 32]),
            requires_full_scan: false,
            execution_digest: empty_execution,
            leader_epoch: 11,
            tier_registry_generation: 13,
            publication_epoch: Some(epoch),
            dirty_usage_buckets: Arc::new(HashMap::new()),
            bucket_failures: ScannerBucketFailureState::default(),
            pending_maintenance_work: Arc::new(AtomicBool::new(false)),
            cache_cycle_floor: Arc::new(AtomicU64::new(8)),
        },
        tx,
        8,
        HealScanMode::Normal,
    )
    .await
    .expect("empty set scope should replace its prior nonempty cache");
    let empty = rx.try_recv().expect("empty set snapshot should be published");
    assert_eq!(empty.info.scan_execution_digest, Some(empty_execution));
    assert!(empty.info.snapshot_complete);
    let root = empty.checked_flatten(DATA_USAGE_ROOT).expect("complete empty root");
    assert_eq!((root.size, root.objects), (0, 0));
}

fn complete_usage_baseline(
    source: DataUsageCacheSource,
    scan_plan_digest: DataUsageScanPlanDigest,
    scanner_cycle: u64,
    scanner_epoch: u64,
) -> bytes::Bytes {
    let baseline = DataUsageInfo {
        last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(10)),
        scanner_cycle: Some(scanner_cycle),
        scanner_epoch: Some(scanner_epoch),
        buckets_count: 1,
        buckets_usage: HashMap::from([("photos".to_string(), Default::default())]),
        usage_snapshot_complete: true,
        usage_snapshot_converged: Some(true),
        usage_snapshot_set_states: vec![DataUsageSnapshotSetState {
            pool_index: u64::try_from(source.pool_index).expect("test pool index should fit"),
            set_index: u64::try_from(source.set_index).expect("test set index should fit"),
            scanner_cycle: Some(scanner_cycle),
            scanner_epoch: Some(scanner_epoch),
            scan_plan_digest: Some(scan_plan_digest.0),
            complete: true,
            tombstone: false,
        }],
        ..Default::default()
    };
    bytes::Bytes::from(serde_json::to_vec(&baseline).expect("test baseline should encode"))
}

#[test]
fn scoped_scan_requires_a_converged_complete_baseline_with_exact_set_provenance() {
    let source = DataUsageCacheSource::new(1, 2);
    let expected_sources = HashSet::from([source]);
    let scan_plan_digest = DataUsageScanPlanDigest([9; 32]);
    let baseline = complete_usage_baseline(source, scan_plan_digest, 7, 11);

    assert_eq!(
        complete_scanner_cache_baseline_plan_digest(ScannerCacheBaselineProof {
            authoritative_data: Some(&baseline),
            observed_candidate_data: None,
            expected_sources: &expected_sources,
            leader_epoch: 11,
            want_cycle: 8,
            scan_plan_digest,
        }),
        Some(scan_plan_digest)
    );

    let mut incomplete = serde_json::from_slice::<DataUsageInfo>(&baseline).expect("test baseline should decode");
    incomplete.usage_snapshot_converged = Some(false);
    let incomplete = bytes::Bytes::from(serde_json::to_vec(&incomplete).expect("test baseline should encode"));
    assert_eq!(
        complete_scanner_cache_baseline_plan_digest(ScannerCacheBaselineProof {
            authoritative_data: Some(&incomplete),
            observed_candidate_data: None,
            expected_sources: &expected_sources,
            leader_epoch: 11,
            want_cycle: 8,
            scan_plan_digest,
        }),
        None
    );

    let mut wrong_provenance = serde_json::from_slice::<DataUsageInfo>(&baseline).expect("test baseline should decode");
    wrong_provenance.usage_snapshot_set_states[0].scan_plan_digest = Some([8; 32]);
    let wrong_provenance = bytes::Bytes::from(serde_json::to_vec(&wrong_provenance).expect("test baseline should encode"));
    assert_eq!(
        complete_scanner_cache_baseline_plan_digest(ScannerCacheBaselineProof {
            authoritative_data: Some(&wrong_provenance),
            observed_candidate_data: None,
            expected_sources: &expected_sources,
            leader_epoch: 11,
            want_cycle: 8,
            scan_plan_digest,
        }),
        None
    );
}

#[test]
fn scoped_scan_accepts_only_a_complete_observation_tied_to_the_authoritative_baseline() {
    let source = DataUsageCacheSource::new(1, 2);
    let expected_sources = HashSet::from([source]);
    let scan_plan_digest = DataUsageScanPlanDigest([9; 32]);
    let authoritative = complete_usage_baseline(source, scan_plan_digest, 7, 11);
    let authoritative_info =
        serde_json::from_slice::<DataUsageInfo>(&authoritative).expect("authoritative baseline should decode");
    let bootstrap_authoritative_info =
        crate::scanner::scanner_usage_bootstrap_marker(SystemTime::UNIX_EPOCH + Duration::from_secs(9), Some(11));
    let bootstrap_authoritative =
        bytes::Bytes::from(serde_json::to_vec(&bootstrap_authoritative_info).expect("bootstrap baseline should encode"));
    let mut observed_info = authoritative_info.clone();
    observed_info.last_update = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(11));
    observed_info.scanner_cycle = Some(8);
    observed_info.usage_snapshot_converged = Some(false);
    observed_info.usage_snapshot_authoritative_baseline = Some(bootstrap_authoritative_info.snapshot_identity());
    observed_info.usage_snapshot_set_states[0].scanner_cycle = Some(8);
    let observed = bytes::Bytes::from(serde_json::to_vec(&observed_info).expect("observation should encode"));

    macro_rules! proof {
        ($authoritative:expr, $candidate:expr) => {
            ScannerCacheBaselineProof {
                authoritative_data: Some($authoritative),
                observed_candidate_data: $candidate,
                expected_sources: &expected_sources,
                leader_epoch: 11,
                want_cycle: 9,
                scan_plan_digest,
            }
        };
    }
    assert_eq!(
        complete_scanner_cache_baseline_plan_digest(proof!(&bootstrap_authoritative, Some(&observed))),
        Some(scan_plan_digest)
    );

    observed_info.usage_snapshot_authoritative_baseline = Some(DataUsageInfo::default().snapshot_identity());
    let mismatched_baseline = bytes::Bytes::from(serde_json::to_vec(&observed_info).expect("observation should encode"));
    assert_eq!(
        complete_scanner_cache_baseline_plan_digest(proof!(&bootstrap_authoritative, Some(&mismatched_baseline))),
        None
    );

    observed_info = serde_json::from_slice(&observed).expect("observation should decode");
    observed_info.usage_snapshot_partial = true;
    let partial = bytes::Bytes::from(serde_json::to_vec(&observed_info).expect("partial observation should encode"));
    assert_eq!(
        complete_scanner_cache_baseline_plan_digest(proof!(&bootstrap_authoritative, Some(&partial))),
        None
    );

    observed_info = serde_json::from_slice(&observed).expect("observation should decode");
    observed_info.usage_snapshot_converged = Some(true);
    let converged = bytes::Bytes::from(serde_json::to_vec(&observed_info).expect("converged observation should encode"));
    assert_eq!(
        complete_scanner_cache_baseline_plan_digest(proof!(&bootstrap_authoritative, Some(&converged))),
        None
    );

    let mut legacy_authoritative = authoritative_info.clone();
    legacy_authoritative.usage_snapshot_converged = None;
    let mut stale_info = serde_json::from_slice::<DataUsageInfo>(&observed).expect("observation should decode");
    stale_info.scanner_cycle = Some(7);
    stale_info.usage_snapshot_set_states[0].scanner_cycle = Some(7);
    stale_info.usage_snapshot_authoritative_baseline = Some(legacy_authoritative.snapshot_identity());
    let legacy_authoritative =
        bytes::Bytes::from(serde_json::to_vec(&legacy_authoritative).expect("legacy baseline should encode"));
    let stale = bytes::Bytes::from(serde_json::to_vec(&stale_info).expect("stale observation should encode"));
    assert_eq!(
        complete_scanner_cache_baseline_plan_digest(proof!(&legacy_authoritative, Some(&stale))),
        None
    );

    let malformed = bytes::Bytes::from_static(b"not data usage json");
    assert_eq!(
        complete_scanner_cache_baseline_plan_digest(proof!(&bootstrap_authoritative, Some(&malformed))),
        None
    );

    let mut nonconverged_authoritative = authoritative_info;
    nonconverged_authoritative.usage_snapshot_converged = Some(false);
    let mut observation_of_nonconverged_authoritative = nonconverged_authoritative.clone();
    observation_of_nonconverged_authoritative.last_update = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(12));
    observation_of_nonconverged_authoritative.scanner_cycle = Some(8);
    observation_of_nonconverged_authoritative.usage_snapshot_set_states[0].scanner_cycle = Some(8);
    observation_of_nonconverged_authoritative.usage_snapshot_authoritative_baseline =
        Some(nonconverged_authoritative.snapshot_identity());
    let nonconverged_authoritative =
        bytes::Bytes::from(serde_json::to_vec(&nonconverged_authoritative).expect("nonconverged baseline should encode"));
    let observation_of_nonconverged_authoritative =
        bytes::Bytes::from(serde_json::to_vec(&observation_of_nonconverged_authoritative).expect("observation should encode"));
    assert_eq!(
        complete_scanner_cache_baseline_plan_digest(ScannerCacheBaselineProof {
            authoritative_data: Some(&nonconverged_authoritative),
            observed_candidate_data: Some(&observation_of_nonconverged_authoritative),
            expected_sources: &expected_sources,
            leader_epoch: 11,
            want_cycle: 9,
            scan_plan_digest,
        }),
        None
    );
}

#[test]
fn scoped_scan_selects_only_current_dirty_buckets_after_baseline_validation() {
    let source = DataUsageCacheSource::new(1, 2);
    let expected_sources = HashSet::from([source]);
    let baseline_scan_plan_digest = DataUsageScanPlanDigest([4; 32]);
    let current_scan_plan_digest = DataUsageScanPlanDigest([5; 32]);
    let baseline = complete_usage_baseline(source, current_scan_plan_digest, 7, 11);
    let scope = scoped_scan_scope_from_dirty_buckets(
        ScannerBucketScanScope::default(),
        HashSet::from(["photos".to_string(), "deleted".to_string()]),
        None,
        true,
        &[bucket_info("photos")],
        ScannerCacheBaselineProof {
            authoritative_data: Some(&baseline),
            observed_candidate_data: None,
            expected_sources: &expected_sources,
            leader_epoch: 11,
            want_cycle: 8,
            scan_plan_digest: current_scan_plan_digest,
        },
    );

    assert_eq!(scope.baseline_scan_plan_digest, Some(current_scan_plan_digest));
    assert_eq!(
        scope
            .selected_buckets
            .as_deref()
            .expect("validated scope should select a bucket"),
        &HashSet::from(["photos".to_string()])
    );
    assert_ne!(scope.baseline_scan_plan_digest, Some(baseline_scan_plan_digest));
}

#[test]
fn scoped_scan_baseline_work_proof_requires_uniform_known_set_identity() {
    let source = DataUsageCacheSource::new(1, 2);
    let second_source = DataUsageCacheSource::new(1, 3);
    let sources = HashSet::from([source, second_source]);
    let structural = DataUsageScanPlanDigest([9; 32]);
    let full = scanner_bucket_work_digest(structural, HealScanMode::Normal, true);
    let deep = scanner_bucket_work_digest(structural, HealScanMode::Deep, true);
    let encoded = complete_usage_baseline(source, full, 7, 11);
    let baseline: DataUsageInfo = serde_json::from_slice(&encoded).expect("baseline should decode");
    for (second_plan, expected) in [(full, Some(full)), (deep, None), (DataUsageScanPlanDigest([8; 32]), None)] {
        let mut candidate = baseline.clone();
        let mut second = candidate.usage_snapshot_set_states[0].clone();
        second.set_index = 3;
        second.scan_plan_digest = Some(second_plan.0);
        candidate.usage_snapshot_set_states.push(second);
        let data = Bytes::from(serde_json::to_vec(&candidate).expect("candidate should encode"));
        assert_eq!(
            complete_scanner_cache_baseline_plan_digest(ScannerCacheBaselineProof {
                authoritative_data: Some(&data),
                observed_candidate_data: None,
                expected_sources: &sources,
                leader_epoch: 11,
                want_cycle: 8,
                scan_plan_digest: structural,
            }),
            expected
        );
    }
}

#[test]
fn scoped_scan_uses_only_locally_verified_prefix_hints() {
    let source = DataUsageCacheSource::new(1, 2);
    let expected_sources = HashSet::from([source]);
    let scan_plan_digest = DataUsageScanPlanDigest([6; 32]);
    let baseline = complete_usage_baseline(source, scan_plan_digest, 7, 11);
    let dirty_scopes = HashMap::from([
        (
            "photos".to_string(),
            DirtyUsageBucketScope::TopLevelEntries(HashSet::from(["2026".to_string()])),
        ),
        ("videos".to_string(), DirtyUsageBucketScope::WholeBucket),
    ]);

    let locally_scoped = scoped_scan_scope_from_dirty_buckets(
        ScannerBucketScanScope::default(),
        HashSet::from(["photos".to_string(), "videos".to_string()]),
        Some(&dirty_scopes),
        true,
        &[bucket_info("photos"), bucket_info("videos")],
        ScannerCacheBaselineProof {
            authoritative_data: Some(&baseline),
            observed_candidate_data: None,
            expected_sources: &expected_sources,
            leader_epoch: 11,
            want_cycle: 8,
            scan_plan_digest,
        },
    );
    assert!(locally_scoped.prefix_scope_for("photos").is_some());
    assert!(locally_scoped.prefix_scope_for("videos").is_none());

    let distributed_scope = scoped_scan_scope_from_dirty_buckets(
        ScannerBucketScanScope::default(),
        HashSet::from(["photos".to_string(), "videos".to_string()]),
        None,
        true,
        &[bucket_info("photos"), bucket_info("videos")],
        ScannerCacheBaselineProof {
            authoritative_data: Some(&baseline),
            observed_candidate_data: None,
            expected_sources: &expected_sources,
            leader_epoch: 11,
            want_cycle: 8,
            scan_plan_digest,
        },
    );
    assert!(distributed_scope.prefix_scope_for("photos").is_none());
}

fn peer_dirty_usage_snapshot(
    instance_id: &str,
    generation: u64,
    complete: bool,
    buckets: &[(&str, u64)],
) -> EcstoreScannerPeerDirtyUsageSnapshot {
    EcstoreScannerPeerDirtyUsageSnapshot {
        owner_id: uuid::Uuid::from_u128(0x11111111111111111111111111111111).to_string(),
        instance_id: instance_id.to_string(),
        generation,
        pending_bucket_count: u64::try_from(buckets.len()).expect("test bucket count should fit"),
        protocol_version: crate::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION,
        complete,
        buckets: buckets
            .iter()
            .map(|(bucket, generation)| {
                (
                    (*bucket).to_string(),
                    crate::storage_api::EcstoreScannerPeerDirtyUsageBucket {
                        bucket_incarnation: uuid::Uuid::from_u128(0x22222222222222222222222222222222),
                        generation: *generation,
                    },
                )
            })
            .collect(),
    }
}

#[test]
fn verified_remote_dirty_usage_buckets_merges_only_complete_current_snapshots() {
    let expected_peers = HashMap::from([
        (
            "node-a:9000".to_string(),
            ScannerPeerDirtyUsageExpectation {
                instance_id: "instance-a".to_string(),
                generation: 7,
                pending: true,
            },
        ),
        (
            "node-b:9000".to_string(),
            ScannerPeerDirtyUsageExpectation {
                instance_id: "instance-b".to_string(),
                generation: 3,
                pending: false,
            },
        ),
    ]);

    assert_eq!(
        verified_remote_dirty_usage(
            &expected_peers,
            vec![
                (
                    "node-a:9000".to_string(),
                    peer_dirty_usage_snapshot("instance-a", 7, true, &[("photos", 7)]),
                ),
                (
                    "node-b:9000".to_string(),
                    peer_dirty_usage_snapshot("instance-b", 3, true, &[("archive", 3)]),
                ),
            ],
        ),
        Some(VerifiedRemoteDirtyUsage {
            dirty_buckets: HashSet::from(["photos".to_string(), "archive".to_string()]),
            acknowledgements: vec![
                crate::scanner::ScannerDirtyUsageAcknowledgement {
                    host: "node-a:9000".to_string(),
                    instance_id: "instance-a".to_string(),
                    kind: crate::scanner::ScannerDirtyUsageAcknowledgementKind::Scoped {
                        owner_id: uuid::Uuid::from_u128(0x11111111111111111111111111111111).to_string(),
                        entries: vec![crate::storage_api::EcstoreScannerScopedDirtyUsageAckEntry {
                            bucket: "photos".to_string(),
                            bucket_incarnation: uuid::Uuid::from_u128(0x22222222222222222222222222222222),
                            generation: 7,
                        }],
                    },
                },
                crate::scanner::ScannerDirtyUsageAcknowledgement {
                    host: "node-b:9000".to_string(),
                    instance_id: "instance-b".to_string(),
                    kind: crate::scanner::ScannerDirtyUsageAcknowledgementKind::Scoped {
                        owner_id: uuid::Uuid::from_u128(0x11111111111111111111111111111111).to_string(),
                        entries: vec![crate::storage_api::EcstoreScannerScopedDirtyUsageAckEntry {
                            bucket: "archive".to_string(),
                            bucket_incarnation: uuid::Uuid::from_u128(0x22222222222222222222222222222222),
                            generation: 3,
                        }],
                    },
                },
            ],
        })
    );
}

#[test]
fn scanner_scoped_dirty_usage_ack_cost_threshold_is_single_protocol_batch() {
    let acknowledgement = |entry_count: usize| crate::scanner::ScannerDirtyUsageAcknowledgement {
        host: "node-a:9000".to_string(),
        instance_id: "instance-a".to_string(),
        kind: crate::scanner::ScannerDirtyUsageAcknowledgementKind::Scoped {
            owner_id: uuid::Uuid::from_u128(0x11111111111111111111111111111111).to_string(),
            entries: (0..entry_count)
                .map(|index| crate::storage_api::EcstoreScannerScopedDirtyUsageAckEntry {
                    bucket: format!("bucket-{index:02}"),
                    bucket_incarnation: uuid::Uuid::from_u128(0x22222222222222222222222222222222),
                    generation: 7,
                })
                .collect(),
        },
    };

    assert!(!scanner_scoped_dirty_usage_ack_exceeds_cost_threshold(&[acknowledgement(
        crate::SCANNER_SCOPED_DIRTY_USAGE_ACK_MAX_ENTRIES
    )]));
    assert!(scanner_scoped_dirty_usage_ack_exceeds_cost_threshold(&[acknowledgement(
        crate::SCANNER_SCOPED_DIRTY_USAGE_ACK_MAX_ENTRIES + 1
    )]));
}

#[test]
fn remote_dirty_usage_scope_resolution_falls_back_when_ack_batch_exceeds_threshold() {
    let source = DataUsageCacheSource::new(1, 2);
    let expected_sources = HashSet::from([source]);
    let scan_plan_digest = DataUsageScanPlanDigest([7; 32]);
    let baseline = complete_usage_baseline(source, scan_plan_digest, 7, 11);
    let bucket_names = (0..=crate::SCANNER_SCOPED_DIRTY_USAGE_ACK_MAX_ENTRIES)
        .map(|index| format!("remote-{index:02}"))
        .collect::<Vec<_>>();
    let bucket_refs = bucket_names.iter().map(|bucket| (bucket.as_str(), 7)).collect::<Vec<_>>();
    let all_buckets = bucket_names.iter().map(|bucket| bucket_info(bucket)).collect::<Vec<_>>();
    let expected_peers = HashMap::from([(
        "node-a:9000".to_string(),
        ScannerPeerDirtyUsageExpectation {
            instance_id: "instance-a".to_string(),
            generation: 7,
            pending: true,
        },
    )]);
    let remote_dirty_usage = verified_remote_dirty_usage(
        &expected_peers,
        vec![("node-a:9000".to_string(), peer_dirty_usage_snapshot("instance-a", 7, true, &bucket_refs))],
    )
    .expect("fixture peer state should verify before the resolver cost gate");

    let result = resolve_remote_dirty_usage_scope(
        ScannerBucketScanScope::default(),
        HashSet::new(),
        remote_dirty_usage,
        &all_buckets,
        ScannerCacheBaselineProof {
            authoritative_data: Some(&baseline),
            observed_candidate_data: None,
            expected_sources: &expected_sources,
            leader_epoch: 11,
            want_cycle: 8,
            scan_plan_digest,
        },
    );

    assert!(
        result.scope.is_default(),
        "oversized scoped ACK batches must force the production resolver back to a full scan"
    );
    assert!(
        result.remote_dirty_usage_acknowledgements.is_empty(),
        "full-scan fallback must not send a scoped ACK that peers would reject or split"
    );
}

#[test]
fn verified_remote_dirty_usage_buckets_rejects_incomplete_or_stale_peer_state() {
    let expected_peers = HashMap::from([(
        "node-a:9000".to_string(),
        ScannerPeerDirtyUsageExpectation {
            instance_id: "instance-a".to_string(),
            generation: 7,
            pending: true,
        },
    )]);

    for snapshot in [
        peer_dirty_usage_snapshot("instance-a", 7, false, &[("photos", 7)]),
        peer_dirty_usage_snapshot("instance-a", 6, true, &[("photos", 6)]),
        peer_dirty_usage_snapshot("instance-b", 7, true, &[("photos", 7)]),
        peer_dirty_usage_snapshot("instance-a", 7, true, &[]),
    ] {
        assert!(
            verified_remote_dirty_usage(&expected_peers, vec![("node-a:9000".to_string(), snapshot)]).is_none(),
            "incomplete, stale, mismatched, or empty pending peer state must fall back to a full scan"
        );
    }
}

#[tokio::test]
#[serial]
async fn distributed_scoped_scan_falls_back_when_remote_ack_exceeds_protocol_batch() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket("local-dirty");
    let local_generation = dirty_usage_generation();
    let remote_dirty_buckets = (0..=crate::SCANNER_SCOPED_DIRTY_USAGE_ACK_MAX_ENTRIES)
        .map(|index| (format!("remote-{index:02}"), 7))
        .collect::<Vec<_>>();
    let mut all_buckets = vec![bucket_info_with_created_time("local-dirty")];
    all_buckets.extend(
        remote_dirty_buckets
            .iter()
            .map(|(bucket, _)| bucket_info_with_created_time(bucket)),
    );
    let snapshot_buckets = remote_dirty_buckets
        .iter()
        .map(|(bucket, generation)| (bucket.as_str(), *generation))
        .collect::<Vec<_>>();
    let baseline_digest = DataUsageScanPlanDigest([8; 32]);
    let baseline = complete_usage_baseline(DataUsageCacheSource::new(1, 2), baseline_digest, 7, 11);
    let expected_sources = HashSet::from([DataUsageCacheSource::new(1, 2)]);
    let dirty_usage_snapshot = snapshot_dirty_usage_buckets(&all_buckets, local_generation);
    let activity_before = BTreeMap::from([(
        "node-a:9000".to_string(),
        crate::scanner::scanner_node_activity_for_tests("instance-a", 5, 7, true),
    )]);

    let result = super::io_cycle::resolve_scanner_bucket_scan_scope_for_tests(
        store.as_ref(),
        true,
        super::io_cycle::ScannerBucketScopeResolution {
            requested_scope: ScannerBucketScanScope::default(),
            baseline_proof: ScannerCacheBaselineProof {
                authoritative_data: Some(&baseline),
                observed_candidate_data: None,
                expected_sources: &expected_sources,
                leader_epoch: 11,
                want_cycle: 8,
                scan_plan_digest: baseline_digest,
            },
            activity_before: &activity_before,
            dirty_usage_snapshot: &dirty_usage_snapshot,
            all_buckets: &all_buckets,
            requires_full_scan: false,
            test_peer_snapshots: Some(vec![(
                "node-a:9000".to_string(),
                peer_dirty_usage_snapshot("instance-a", 7, true, &snapshot_buckets),
            )]),
            test_scoped_dirty_usage_capability: Some(true),
        },
    )
    .await;

    assert!(
        result.scope.is_default(),
        "remote scoped acknowledgements above one protocol batch must force a full scan"
    );
    assert!(
        result.remote_dirty_usage_acknowledgements.is_empty(),
        "full-scan fallback must not send scoped remote acknowledgements"
    );
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
async fn distributed_scoped_scan_falls_back_when_remote_scoped_ack_capability_is_rejected() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    let source = DataUsageCacheSource::new(1, 2);
    let expected_sources = HashSet::from([source]);
    let scan_plan_digest = DataUsageScanPlanDigest([7; 32]);
    let baseline = complete_usage_baseline(source, scan_plan_digest, 7, 11);
    let dirty_usage_snapshot = DirtyUsageSnapshot {
        buckets: Arc::new(HashMap::new()),
        scopes: Arc::new(HashMap::new()),
        generation: 7,
        covers_all_pending: true,
    };
    let activity_before = BTreeMap::from([(
        "node-a:9000".to_string(),
        crate::scanner::scanner_node_activity_for_tests("instance-a", 5, 7, true),
    )]);

    for (capability, expected_buckets, expected_ack_count) in
        [(true, Some(HashSet::from(["photos".to_string()])), 1), (false, None, 0)]
    {
        let result = super::io_cycle::resolve_scanner_bucket_scan_scope_for_tests(
            store.as_ref(),
            true,
            super::io_cycle::ScannerBucketScopeResolution {
                requested_scope: ScannerBucketScanScope::default(),
                baseline_proof: ScannerCacheBaselineProof {
                    authoritative_data: Some(&baseline),
                    observed_candidate_data: None,
                    expected_sources: &expected_sources,
                    leader_epoch: 11,
                    want_cycle: 8,
                    scan_plan_digest,
                },
                activity_before: &activity_before,
                dirty_usage_snapshot: &dirty_usage_snapshot,
                all_buckets: &[bucket_info_with_created_time("photos")],
                requires_full_scan: false,
                test_peer_snapshots: Some(vec![(
                    "node-a:9000".to_string(),
                    peer_dirty_usage_snapshot("instance-a", 7, true, &[("photos", 7)]),
                )]),
                test_scoped_dirty_usage_capability: Some(capability),
            },
        )
        .await;

        assert_eq!(result.scope.selected_buckets.as_deref(), expected_buckets.as_ref());
        assert_eq!(result.remote_dirty_usage_acknowledgements.len(), expected_ack_count);
    }
}

fn bucket_info_with_created_time(name: &str) -> BucketInfo {
    BucketInfo {
        created: Some(time::OffsetDateTime::UNIX_EPOCH),
        ..bucket_info(name)
    }
}

#[test]
fn scoped_set_scan_rebuilds_selected_buckets_and_drops_deleted_buckets() {
    let baseline_digest = DataUsageScanPlanDigest([1; 32]);
    let current_digest = DataUsageScanPlanDigest([2; 32]);
    let mut old_cache = complete_set_usage_cache(&[("stable", 10), ("dirty", 20), ("deleted", 30)], baseline_digest);
    old_cache.replace(
        "stable/prefix",
        "stable",
        DataUsageEntry {
            size: 5,
            objects: 1,
            ..Default::default()
        },
    );
    let all_buckets = vec![
        bucket_info_with_created_time("stable"),
        bucket_info_with_created_time("dirty"),
    ];
    let selected_buckets = Arc::new(HashSet::from(["stable".to_string(), "dirty".to_string(), "deleted".to_string()]));

    let prepared = prepare_scoped_set_scan(
        &old_cache,
        &all_buckets,
        &all_buckets,
        &ScannerBucketScanScope {
            selected_buckets: Some(selected_buckets),
            selected_bucket_prefixes: None,
            baseline_scan_plan_digest: Some(baseline_digest),
        },
        ScannerSetCacheGeneration {
            want_cycle: 8,
            leader_epoch: 11,
            tier_registry_generation: 13,
            source: DataUsageCacheSource::new(1, 2),
            scan_plan_digest: current_digest,
        },
    )
    .expect("complete matching set cache should support a scoped scan");

    assert_eq!(
        prepared.buckets.iter().map(|bucket| bucket.name.as_str()).collect::<Vec<_>>(),
        ["stable", "dirty"]
    );
    let stable = prepared
        .cache
        .checked_flatten("stable")
        .expect("selected bucket placeholder should exist");
    assert_eq!((stable.size, stable.objects), (0, 0));
    assert!(prepared.cache.find("stable/prefix").is_none());
    assert_eq!(prepared.cache.find("dirty").map(|entry| (entry.size, entry.objects)), Some((0, 0)));
    assert!(prepared.cache.find("deleted").is_none());
    assert_eq!(prepared.cache.info.scan_plan_digest, Some(current_digest));
    assert_eq!(prepared.cache.info.next_cycle, 8);
    assert!(!prepared.cache.info.snapshot_complete);
    assert!(prepared.cache.info.lkg_snapshot_complete);
    assert_eq!(prepared.cache.info.lkg_next_cycle, Some(7));
    assert_eq!(prepared.cache.info.lkg_scan_plan_digest, Some(baseline_digest));
}

#[test]
fn scoped_set_scan_rejects_unbound_bucket_incarnations() {
    let baseline_digest = DataUsageScanPlanDigest([1; 32]);
    let old_cache = complete_set_usage_cache(&[("stable", 10), ("dirty", 20)], baseline_digest);
    let scope = ScannerBucketScanScope {
        selected_buckets: Some(Arc::new(HashSet::from(["dirty".to_string()]))),
        selected_bucket_prefixes: None,
        baseline_scan_plan_digest: Some(baseline_digest),
    };
    let generation = ScannerSetCacheGeneration {
        want_cycle: 8,
        leader_epoch: 11,
        tier_registry_generation: 13,
        source: DataUsageCacheSource::new(1, 2),
        scan_plan_digest: DataUsageScanPlanDigest([2; 32]),
    };
    for created in [
        None,
        Some(OffsetDateTime::UNIX_EPOCH),
        Some(OffsetDateTime::UNIX_EPOCH + time::Duration::days(1)),
    ] {
        let mut stable = bucket_info("stable");
        stable.created = created;
        let buckets = vec![stable, bucket_info_with_created_time("dirty")];
        assert!(
            prepare_scoped_set_scan(&old_cache, &buckets, &buckets, &scope, generation).is_none(),
            "missing identity, volume timestamps and same-name recreation must all rebuild"
        );
    }
}

#[test]
fn scoped_set_scan_falls_back_when_an_unselected_bucket_has_no_baseline() {
    let baseline_digest = DataUsageScanPlanDigest([3; 32]);
    let old_cache = complete_set_usage_cache(&[("stable", 10)], baseline_digest);
    let all_buckets = vec![bucket_info_with_created_time("stable"), bucket_info_with_created_time("new")];

    assert!(
        prepare_scoped_set_scan(
            &old_cache,
            &all_buckets,
            &all_buckets,
            &ScannerBucketScanScope {
                selected_buckets: Some(Arc::new(HashSet::from(["dirty".to_string()]))),
                selected_bucket_prefixes: None,
                baseline_scan_plan_digest: Some(baseline_digest),
            },
            ScannerSetCacheGeneration {
                want_cycle: 8,
                leader_epoch: 11,
                tier_registry_generation: 13,
                source: DataUsageCacheSource::new(1, 2),
                scan_plan_digest: DataUsageScanPlanDigest([4; 32]),
            },
        )
        .is_none()
    );
}

#[test]
fn scoped_set_scan_requires_an_exact_complete_baseline() {
    let baseline_digest = DataUsageScanPlanDigest([5; 32]);
    let all_buckets = vec![bucket_info_with_created_time("dirty")];
    let scope = ScannerBucketScanScope {
        selected_buckets: Some(Arc::new(HashSet::from(["dirty".to_string()]))),
        selected_bucket_prefixes: None,
        baseline_scan_plan_digest: Some(baseline_digest),
    };
    let generation = ScannerSetCacheGeneration {
        want_cycle: 8,
        leader_epoch: 11,
        tier_registry_generation: 13,
        source: DataUsageCacheSource::new(1, 2),
        scan_plan_digest: DataUsageScanPlanDigest([6; 32]),
    };

    let mut incomplete = complete_set_usage_cache(&[("dirty", 10)], baseline_digest);
    incomplete.info.snapshot_complete = false;
    assert!(prepare_scoped_set_scan(&incomplete, &all_buckets, &all_buckets, &scope, generation).is_none());

    let mut not_durable = complete_set_usage_cache(&[("dirty", 10)], baseline_digest);
    not_durable.info.last_update = None;
    assert!(prepare_scoped_set_scan(&not_durable, &all_buckets, &all_buckets, &scope, generation).is_none());

    let mut unscoped_usage = complete_set_usage_cache(&[("dirty", 10)], baseline_digest);
    unscoped_usage.cache.get_mut(DATA_USAGE_ROOT).expect("set root").objects = 1;
    assert!(prepare_scoped_set_scan(&unscoped_usage, &all_buckets, &all_buckets, &scope, generation).is_none());

    let mut wrong_digest = complete_set_usage_cache(&[("dirty", 10)], baseline_digest);
    wrong_digest.info.scan_plan_digest = Some(DataUsageScanPlanDigest([7; 32]));
    assert!(prepare_scoped_set_scan(&wrong_digest, &all_buckets, &all_buckets, &scope, generation).is_none());

    let empty_scope = ScannerBucketScanScope {
        selected_buckets: Some(Arc::new(HashSet::new())),
        selected_bucket_prefixes: None,
        baseline_scan_plan_digest: Some(baseline_digest),
    };
    let complete = complete_set_usage_cache(&[("dirty", 10)], baseline_digest);
    assert!(prepare_scoped_set_scan(&complete, &all_buckets, &all_buckets, &empty_scope, generation).is_none());
    assert!(prepare_scoped_set_scan(&complete, &all_buckets, &all_buckets, &scope, generation).is_some());

    let unidentified_buckets = vec![bucket_info("dirty")];
    assert!(
        prepare_scoped_set_scan(&complete, &unidentified_buckets, &unidentified_buckets, &scope, generation).is_some(),
        "fully selected buckets are rebuilt without reusing an unproven incarnation"
    );

    let mut future_cache = complete_set_usage_cache(&[("dirty", 10)], baseline_digest);
    future_cache.info.next_cycle = generation.want_cycle.saturating_add(1);
    assert!(prepare_scoped_set_scan(&future_cache, &all_buckets, &all_buckets, &scope, generation).is_none());
}

#[test]
fn record_set_scan_failure_preserves_first_error() {
    let mut first = None;
    record_set_scan_failure(&mut first, Error::other("first"));
    record_set_scan_failure(&mut first, Error::other("second"));

    let first = first.expect("first error should be recorded");
    assert!(first.to_string().contains("first"));
}

#[tokio::test]
async fn scanner_task_join_error_includes_stage() {
    let handle = tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    handle.abort();

    let join_err = handle.await.expect_err("aborted task should return a join error");
    let err = scanner_task_join_error("scanner set", join_err);

    assert!(err.to_string().contains("scanner set task join failed"));
}

#[test]
fn finalize_nsscanner_result_returns_ok_when_any_set_succeeds() {
    let mut results = vec![DataUsageCache::default(), DataUsageCache::default()];
    results[1].info.last_update = Some(SystemTime::now());

    let result = finalize_nsscanner_result(&results, Some(Error::other("set failed")));
    assert!(result.is_ok());
}

#[test]
fn finalize_nsscanner_result_returns_first_error_when_all_sets_fail() {
    let results = vec![DataUsageCache::default(), DataUsageCache::default()];

    let err = finalize_nsscanner_result(&results, Some(Error::other("set failed")))
        .expect_err("all failed sets should bubble first error");
    assert!(err.to_string().contains("set failed"));
}

#[test]
fn scanner_cycle_status_requires_a_clean_complete_snapshot() {
    assert_eq!(
        classify_nsscanner_cycle(
            true,
            false,
            false,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Current,
            ScannerCycleActivityStatus::Unchanged,
        ),
        ScannerCycleStatus::Complete
    );
    assert_eq!(
        classify_nsscanner_cycle(
            true,
            false,
            false,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Changed,
            ScannerCycleActivityStatus::Unchanged,
        ),
        ScannerCycleStatus::Superseded
    );
    assert_eq!(
        classify_nsscanner_cycle(
            true,
            false,
            false,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Current,
            ScannerCycleActivityStatus::Changed,
        ),
        ScannerCycleStatus::Superseded
    );
    assert_eq!(
        classify_nsscanner_cycle(
            true,
            false,
            false,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Current,
            ScannerCycleActivityStatus::Unverified,
        ),
        ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable)
    );

    for status in [
        classify_nsscanner_cycle(
            false,
            false,
            false,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Current,
            ScannerCycleActivityStatus::Unchanged,
        ),
        classify_nsscanner_cycle(
            true,
            true,
            false,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Current,
            ScannerCycleActivityStatus::Unchanged,
        ),
        classify_nsscanner_cycle(
            true,
            false,
            true,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Current,
            ScannerCycleActivityStatus::Unchanged,
        ),
        classify_nsscanner_cycle(
            true,
            false,
            false,
            ScannerBucketScanStatus::Failed,
            DirtyUsageSnapshotStatus::Current,
            ScannerCycleActivityStatus::Changed,
        ),
        classify_nsscanner_cycle(
            false,
            false,
            false,
            ScannerBucketScanStatus::Partial,
            DirtyUsageSnapshotStatus::Changed,
            ScannerCycleActivityStatus::Changed,
        ),
    ] {
        assert_eq!(status, ScannerCycleStatus::Incomplete);
    }
}

#[test]
fn checkpoint_fixture_superseded_is_distinct_from_partial_and_cancel() {
    for (budget, cancelled, bucket, expected) in [
        (false, false, ScannerBucketScanStatus::Complete, ScannerCycleStatus::Superseded),
        (true, false, ScannerBucketScanStatus::Partial, ScannerCycleStatus::Incomplete),
        (false, true, ScannerBucketScanStatus::Partial, ScannerCycleStatus::Incomplete),
    ] {
        assert_eq!(
            classify_nsscanner_cycle(
                true,
                budget,
                cancelled,
                bucket,
                DirtyUsageSnapshotStatus::Changed,
                ScannerCycleActivityStatus::Unchanged
            ),
            expected,
        );
    }
}

#[test]
fn unverified_activity_defers_partial_and_floor_cycles() {
    let expected = ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable);

    assert_eq!(
        classify_nsscanner_cycle(
            true,
            false,
            false,
            ScannerBucketScanStatus::Partial,
            DirtyUsageSnapshotStatus::Current,
            ScannerCycleActivityStatus::Unverified,
        ),
        expected
    );
    assert_eq!(
        classify_nsscanner_cycle(
            false,
            false,
            false,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Current,
            ScannerCycleActivityStatus::Unverified,
        ),
        expected
    );
}

#[tokio::test]
async fn structurally_complete_superseded_cycles_publish_without_claiming_convergence() {
    let (updates, mut receiver) = mpsc::channel(2);

    assert!(
        publish_usage_snapshot(&updates, ScannerCycleStatus::Complete, DataUsageInfo::default())
            .await
            .expect("complete snapshot publication should succeed")
    );
    assert!(
        publish_usage_snapshot(&updates, ScannerCycleStatus::Superseded, DataUsageInfo::default())
            .await
            .expect("superseded snapshot publication should succeed")
    );
    assert!(
        !publish_usage_snapshot(&updates, ScannerCycleStatus::Incomplete, DataUsageInfo::default())
            .await
            .expect("incomplete snapshot suppression should succeed")
    );
    assert!(
        !publish_usage_snapshot(
            &updates,
            ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable),
            DataUsageInfo::default(),
        )
        .await
        .expect("unverified activity suppression should succeed")
    );

    assert_eq!(
        receiver
            .recv()
            .await
            .expect("complete update should be sent")
            .usage_snapshot_converged,
        Some(true)
    );
    assert_eq!(
        receiver
            .recv()
            .await
            .expect("superseded update should be sent")
            .usage_snapshot_converged,
        Some(false)
    );
}

#[tokio::test]
async fn post_scan_activity_failure_retains_complete_usage_as_observation() {
    let (updates, mut receiver) = mpsc::channel(1);
    let status = ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable);

    assert!(should_publish_observational_snapshot(status));
    assert!(
        publish_observational_snapshot(
            &updates,
            DataUsageInfo {
                last_update: Some(SystemTime::now()),
                scanner_cycle: Some(7),
                objects_total_count: 3,
                objects_total_size: 12,
                usage_snapshot_complete: true,
                ..Default::default()
            },
        )
        .await
        .expect("post-scan activity failure should retain an observation")
    );

    let observed = receiver.recv().await.expect("observational update should be queued");
    assert!(!observed.usage_snapshot_complete);
    assert!(observed.usage_snapshot_partial);
    assert_eq!(observed.usage_snapshot_converged, Some(false));
    assert_eq!(observed.objects_total_count, 3);
    assert_eq!(observed.objects_total_size, 12);
}

#[test]
fn only_unverified_activity_allows_post_scan_observation() {
    assert!(should_publish_observational_snapshot(ScannerCycleStatus::Deferred(
        ScannerCycleDeferReason::ActivityBaselineUnavailable
    )));
    assert!(!should_publish_observational_snapshot(ScannerCycleStatus::Deferred(
        ScannerCycleDeferReason::DataMovement
    )));
    assert!(!should_publish_observational_snapshot(ScannerCycleStatus::Incomplete));
}

#[test]
fn scanner_cycle_fails_closed_for_namespace_disappearance() {
    for activity_status in [ScannerCycleActivityStatus::Changed, ScannerCycleActivityStatus::Unchanged] {
        assert_eq!(
            classify_nsscanner_cycle(
                false,
                false,
                false,
                ScannerBucketScanStatus::NamespaceNotFound,
                DirtyUsageSnapshotStatus::Changed,
                activity_status,
            ),
            ScannerCycleStatus::Incomplete
        );
    }
    assert_eq!(
        classify_nsscanner_cycle(
            false,
            false,
            false,
            ScannerBucketScanStatus::NamespaceNotFound,
            DirtyUsageSnapshotStatus::Changed,
            ScannerCycleActivityStatus::Unverified,
        ),
        ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable)
    );
    assert_eq!(
        classify_nsscanner_cycle(
            true,
            true,
            false,
            ScannerBucketScanStatus::NamespaceNotFound,
            DirtyUsageSnapshotStatus::Changed,
            ScannerCycleActivityStatus::Changed,
        ),
        ScannerCycleStatus::Incomplete
    );
}

#[test]
fn scanner_cycle_fails_closed_when_dirty_generation_is_unverified() {
    assert_eq!(
        classify_nsscanner_cycle(
            true,
            false,
            false,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Unverified,
            ScannerCycleActivityStatus::Unchanged,
        ),
        ScannerCycleStatus::Incomplete
    );
}

#[test]
fn scanner_bucket_failure_status_preserves_the_strongest_failure() {
    assert_eq!(scanner_bucket_scan_status(false, false, false), ScannerBucketScanStatus::Complete);
    assert_eq!(scanner_bucket_scan_status(false, false, true), ScannerBucketScanStatus::NamespaceNotFound);
    assert_eq!(scanner_bucket_scan_status(false, true, true), ScannerBucketScanStatus::Partial);
    assert_eq!(scanner_bucket_scan_status(true, true, true), ScannerBucketScanStatus::Failed);
}

#[test]
fn scanner_cycle_surfaces_persisted_pending_heal_work() {
    let clean = DataUsageCache::default();
    assert!(!scanner_results_have_pending_maintenance_work(std::slice::from_ref(&clean)));

    let mut pending = clean;
    pending.info.pending_heals.push(crate::PendingScannerHeal {
        kind: crate::PendingScannerHealKind::Object,
        bucket: "photos".to_string(),
        object: Some("image.jpg".to_string()),
        version_id: None,
        scan_mode: HealScanMode::Normal,
        first_seen: 1,
        last_attempt: 1,
        attempts: 1,
        last_admission_result: "queue_full".to_string(),
        last_admission_reason: "capacity".to_string(),
    });

    assert!(scanner_results_have_pending_maintenance_work(&[pending]));
}

#[tokio::test]
async fn bucket_cache_pending_heal_reaches_cycle_maintenance_state() {
    let pending_maintenance_work = Arc::new(AtomicBool::new(false));
    let mut bucket_cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "photos".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    bucket_cache.replace("photos", DATA_USAGE_ROOT, DataUsageEntry::default());
    bucket_cache.info.pending_heals.push(crate::PendingScannerHeal {
        kind: crate::PendingScannerHealKind::Object,
        bucket: "photos".to_string(),
        object: Some("image.jpg".to_string()),
        version_id: None,
        scan_mode: HealScanMode::Normal,
        first_seen: 1,
        last_attempt: 1,
        attempts: 1,
        last_admission_result: "queue_full".to_string(),
        last_admission_reason: "capacity".to_string(),
    });
    let (sender, mut receiver) = mpsc::channel(1);

    send_cache_root_entry_info(&sender, &bucket_cache, &pending_maintenance_work)
        .await
        .expect("bucket result should send");

    let cycle_pending = pending_maintenance_work_for_cycle(&pending_maintenance_work, &[]);
    assert!(cycle_pending);
    assert_eq!(
        crate::scanner::scanner_cycle_outcome_with_pending_maintenance(
            crate::scanner::ScannerCycleOutcome::Completed,
            cycle_pending,
        ),
        crate::scanner::ScannerCycleOutcome::CompletedWithPendingMaintenance
    );
    assert!(receiver.recv().await.is_some());
}

#[test]
#[serial]
fn scanner_concurrency_limit_preserves_available_when_unconfigured() {
    crate::reset_foreground_read_activity_for_test();
    crate::workload_admission::clear_scanner_workload_admission_snapshot_provider_for_test();
    assert_eq!(scanner_concurrency_limit(0, 4), 4);
}

#[test]
#[serial]
fn scanner_concurrency_limit_caps_to_configured_value() {
    crate::reset_foreground_read_activity_for_test();
    crate::workload_admission::clear_scanner_workload_admission_snapshot_provider_for_test();
    assert_eq!(scanner_concurrency_limit(2, 4), 2);
}

#[test]
#[serial]
fn scanner_concurrency_limit_never_exceeds_available_work() {
    crate::reset_foreground_read_activity_for_test();
    crate::workload_admission::clear_scanner_workload_admission_snapshot_provider_for_test();
    assert_eq!(scanner_concurrency_limit(8, 4), 4);
}

#[test]
#[serial]
fn scanner_concurrency_limit_handles_no_available_work() {
    crate::reset_foreground_read_activity_for_test();
    crate::workload_admission::clear_scanner_workload_admission_snapshot_provider_for_test();
    assert_eq!(scanner_concurrency_limit(2, 0), 0);
}

#[test]
#[serial]
fn scanner_concurrency_limit_yields_to_foreground_reads() {
    crate::reset_foreground_read_activity_for_test();
    crate::workload_admission::clear_scanner_workload_admission_snapshot_provider_for_test();
    crate::set_foreground_read_activity(8);
    assert_eq!(scanner_concurrency_limit(0, 4), 1);
    assert_eq!(scanner_concurrency_limit(3, 4), 1);
    crate::reset_foreground_read_activity_for_test();
}

#[test]
#[serial]
fn scanner_concurrency_limit_yields_to_shared_foreground_pressure() {
    crate::reset_foreground_read_activity_for_test();
    crate::workload_admission::clear_scanner_workload_admission_snapshot_provider_for_test();
    install_scanner_workload_provider(WorkloadAdmissionRegistrySnapshot::new(vec![
        WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundWrite, AdmissionState::Open).with_counts(Some(2), None, Some(16)),
    ]));

    assert_eq!(scanner_concurrency_limit(0, 4), 1);
    assert_eq!(scanner_concurrency_limit(3, 4), 1);

    crate::workload_admission::clear_scanner_workload_admission_snapshot_provider_for_test();
}

#[test]
#[serial]
fn scanner_concurrency_limit_yields_to_streaming_reads() {
    crate::reset_foreground_read_activity_for_test();
    crate::workload_admission::clear_scanner_workload_admission_snapshot_provider_for_test();
    let _guard = crate::ForegroundReadGuard::new();

    assert_eq!(scanner_concurrency_limit(0, 4), 1);
    assert_eq!(scanner_concurrency_limit(3, 4), 1);
}

#[test]
fn decrement_atomic_usize_saturates_at_zero() {
    let counter = AtomicUsize::new(1);
    assert_eq!(decrement_atomic_usize(&counter), 0);
    assert_eq!(decrement_atomic_usize(&counter), 0);
}

#[test]
fn increment_atomic_usize_saturates_at_max() {
    let counter = AtomicUsize::new(usize::MAX);
    assert_eq!(increment_atomic_usize(&counter), usize::MAX);
    assert_eq!(counter.load(Ordering::Relaxed), usize::MAX);
}

#[test]
#[serial]
fn scanner_max_concurrent_set_scans_uses_env_cap() {
    with_var(ENV_SCANNER_MAX_CONCURRENT_SET_SCANS, Some("2"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(scanner_max_concurrent_set_scans(4), 2);
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
#[serial]
fn scanner_max_concurrent_disk_scans_uses_env_cap() {
    with_var(ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS, Some("1"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(scanner_max_concurrent_disk_scans(4), 1);
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
#[cfg(windows)]
fn is_xl_meta_path_accepts_windows_separator() {
    assert!(is_xl_meta_path("D:\\data\\bucket\\object\\xl.meta"));
}

#[test]
fn is_xl_meta_path_accepts_forward_separator() {
    assert!(is_xl_meta_path("/data/bucket/object/xl.meta"));
}

#[test]
fn tier_stats_template_seeds_tiers_and_standard_classes() {
    let template = tier_stats_template(&["WARM".to_string(), "COLD".to_string()]);

    assert_eq!(template.len(), 5);
    for tier in ["WARM", "COLD", storageclass::STANDARD, storageclass::RRS, UNKNOWN_TIER] {
        assert_eq!(template.get(tier), Some(&TierStats::default()), "missing seed for tier {tier}");
    }
}

#[test]
fn tier_stats_template_stays_empty_without_tiers() {
    let template = tier_stats_template(&[]);

    assert!(template.is_empty());
}

#[tokio::test]
async fn get_size_treats_missing_metadata_as_skip_file() {
    let temp_dir = std::env::temp_dir().join(format!("rustfs-scanner-missing-meta-{}", Uuid::new_v4()));
    let bucket = "bucket";
    let object = "object";
    let object_dir = temp_dir.join(bucket).join(object);
    let metadata_path = object_dir.join(STORAGE_FORMAT_FILE);

    tokio::fs::create_dir_all(&object_dir)
        .await
        .expect("failed to create object directory");
    tokio::fs::write(&metadata_path, [])
        .await
        .expect("failed to create metadata placeholder");

    let endpoint = Endpoint::try_from(temp_dir.to_string_lossy().as_ref()).expect("failed to create endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("failed to open local disk");

    let relative_path = metadata_path.to_string_lossy().to_string();
    let (_, scanner_path) = path2_bucket_object_with_base_path(temp_dir.to_string_lossy().as_ref(), relative_path.as_str());
    let file_type = tokio::fs::metadata(&metadata_path)
        .await
        .expect("failed to stat metadata placeholder")
        .file_type();

    tokio::fs::remove_dir_all(&object_dir)
        .await
        .expect("failed to remove object directory");

    let item = ScannerItem {
        path: scanner_path,
        bucket: bucket.to_string(),
        prefix: object.to_string(),
        object_name: STORAGE_FORMAT_FILE.to_string(),
        file_type,
        lifecycle: None,
        object_lock: None,
        replication: None,
        heal_enabled: false,
        heal_bitrot: false,
        debug: false,
    };

    let err = disk
        .get_size(item)
        .await
        .expect_err("missing metadata should be skipped instead of reported as a scanner failure");
    assert!(matches!(err, StorageError::Io(ref io) if io.to_string() == SCANNER_SKIP_FILE_ERROR));

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn get_size_marks_corrupt_metadata_for_heal() {
    let temp_dir = std::env::temp_dir().join(format!("rustfs-scanner-corrupt-meta-{}", Uuid::new_v4()));
    let bucket = "bucket";
    let object = "object";
    let object_dir = temp_dir.join(bucket).join(object);
    let metadata_path = object_dir.join(STORAGE_FORMAT_FILE);

    tokio::fs::create_dir_all(&object_dir)
        .await
        .expect("failed to create object directory");
    tokio::fs::write(&metadata_path, b"not-valid-filemeta")
        .await
        .expect("failed to write corrupt metadata");

    let endpoint = Endpoint::try_from(temp_dir.to_string_lossy().as_ref()).expect("failed to create endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("failed to open local disk");

    let relative_path = metadata_path.to_string_lossy().to_string();
    let (_, scanner_path) = path2_bucket_object_with_base_path(temp_dir.to_string_lossy().as_ref(), relative_path.as_str());
    let file_type = tokio::fs::metadata(&metadata_path)
        .await
        .expect("failed to stat metadata")
        .file_type();

    let item = ScannerItem {
        path: scanner_path,
        bucket: bucket.to_string(),
        prefix: object.to_string(),
        object_name: STORAGE_FORMAT_FILE.to_string(),
        file_type,
        lifecycle: None,
        object_lock: None,
        replication: None,
        heal_enabled: false,
        heal_bitrot: false,
        debug: false,
    };

    let err = disk
        .get_size(item)
        .await
        .expect_err("corrupt metadata should be surfaced as scanner-heal work");
    assert!(is_scanner_metadata_corrupt_error(&err));

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn get_size_counts_delete_markers_separately_from_versions() {
    let temp_dir = std::env::temp_dir().join(format!("rustfs-scanner-versioned-usage-{}", Uuid::new_v4()));
    let bucket = "bucket";
    let object = "object";
    let object_dir = temp_dir.join(bucket).join(object);
    let metadata_path = object_dir.join(STORAGE_FORMAT_FILE);

    tokio::fs::create_dir_all(&object_dir)
        .await
        .expect("failed to create object directory");

    let mut meta = FileMeta::new();
    for (size, timestamp) in [(10, 10), (20, 20)] {
        let mut fi = FileInfo::new(object, 1, 1);
        fi.version_id = Some(Uuid::new_v4());
        fi.mod_time = Some(OffsetDateTime::from_unix_timestamp(timestamp).expect("timestamp should be valid"));
        fi.size = size;
        meta.add_version(fi).expect("object version should be added");
    }

    // A real delete marker carries no erasure geometry (delete paths build it as
    // `FileInfo { deleted: true, .. }`). Construct it that way so it classifies as a
    // storage delete marker rather than a purge-pending payload object.
    let delete_marker = FileInfo {
        name: object.to_string(),
        version_id: Some(Uuid::new_v4()),
        mod_time: Some(OffsetDateTime::from_unix_timestamp(30).expect("timestamp should be valid")),
        deleted: true,
        ..Default::default()
    };
    meta.add_version(delete_marker).expect("delete marker should be added");

    tokio::fs::write(&metadata_path, meta.marshal_msg().expect("metadata should marshal"))
        .await
        .expect("failed to write metadata");

    let endpoint = Endpoint::try_from(temp_dir.to_string_lossy().as_ref()).expect("failed to create endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("failed to open local disk");

    let relative_path = metadata_path.to_string_lossy().to_string();
    let (_, scanner_path) = path2_bucket_object_with_base_path(temp_dir.to_string_lossy().as_ref(), relative_path.as_str());
    let file_type = tokio::fs::metadata(&metadata_path)
        .await
        .expect("failed to stat metadata")
        .file_type();
    let item = ScannerItem {
        path: scanner_path,
        bucket: bucket.to_string(),
        prefix: object.to_string(),
        object_name: STORAGE_FORMAT_FILE.to_string(),
        file_type,
        lifecycle: None,
        object_lock: None,
        replication: None,
        heal_enabled: false,
        heal_bitrot: false,
        debug: false,
    };

    let summary = disk.get_size(item).await.expect("scanner should read versioned metadata");

    assert_eq!(summary.versions, 2);
    assert_eq!(summary.delete_markers, 1);
    assert_eq!(summary.total_size, 30);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[test]
fn cache_root_entry_info_flattens_bucket_children() {
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "bucket",
        DATA_USAGE_ROOT,
        DataUsageEntry {
            size: 10,
            objects: 1,
            ..Default::default()
        },
    );
    cache.replace(
        "bucket/prefix",
        "bucket",
        DataUsageEntry {
            size: 20,
            objects: 2,
            ..Default::default()
        },
    );

    let info = cache_root_entry_info(&cache).expect("valid cache should flatten");

    assert_eq!(info.name, "bucket");
    assert_eq!(info.parent, DATA_USAGE_ROOT);
    assert_eq!(info.entry.size, 30);
    assert_eq!(info.entry.objects, 3);
    assert!(info.entry.children.is_empty());
}

#[test]
fn cache_root_entry_info_rejects_missing_or_dangling_roots() {
    let missing_root = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(cache_root_entry_info(&missing_root).is_err());

    let mut dangling = missing_root;
    let mut root = DataUsageEntry::default();
    root.add_child(&crate::hash_path("bucket/missing"));
    dangling.replace("bucket", DATA_USAGE_ROOT, root);
    assert!(cache_root_entry_info(&dangling).is_err());

    let mut detached = DataUsageCache {
        info: DataUsageCacheInfo {
            name: DATA_USAGE_ROOT.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    detached.replace("bucket", DATA_USAGE_ROOT, DataUsageEntry::default());
    detached.replace(
        "bucket/detached",
        "",
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    assert!(cache_root_entry_info(&detached).is_err());

    let mut detached_bucket = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    detached_bucket.replace(
        "bucket",
        DATA_USAGE_ROOT,
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    detached_bucket.replace(
        "bucket/detached",
        "",
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    assert!(cache_root_entry_info(&detached_bucket).is_err());

    let mut compacted_with_child = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    compacted_with_child.replace(
        "bucket",
        DATA_USAGE_ROOT,
        DataUsageEntry {
            compacted: true,
            ..Default::default()
        },
    );
    compacted_with_child.replace("bucket/prefix", "bucket", DataUsageEntry::default());
    assert!(cache_root_entry_info(&compacted_with_child).is_err());
}

#[test]
fn apply_bucket_result_to_cache_updates_bucket_entry() {
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: DATA_USAGE_ROOT.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "bucket",
        DATA_USAGE_ROOT,
        DataUsageEntry {
            size: 5,
            objects: 1,
            ..Default::default()
        },
    );

    let update_time = SystemTime::now();
    assert!(apply_bucket_result_to_cache(
        &mut cache,
        DataUsageEntryInfo {
            name: "bucket".to_string(),
            parent: DATA_USAGE_ROOT.to_string(),
            entry: DataUsageEntry {
                size: 10,
                objects: 2,
                ..Default::default()
            },
            tier_registry_generation: None,
        },
        update_time,
    ));

    assert_eq!(cache.info.last_update, Some(update_time));
    let entry = cache.find("bucket").expect("bucket entry should remain present");
    assert_eq!(entry.size, 10);
    assert_eq!(entry.objects, 2);
}

#[test]
fn apply_bucket_result_to_cache_rejects_a_different_tier_generation() {
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: DATA_USAGE_ROOT.to_string(),
            tier_registry_generation: Some(7),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "bucket",
        DATA_USAGE_ROOT,
        DataUsageEntry {
            size: 3,
            ..Default::default()
        },
    );

    let applied = apply_bucket_result_to_cache(
        &mut cache,
        DataUsageEntryInfo {
            name: "bucket".to_string(),
            parent: DATA_USAGE_ROOT.to_string(),
            entry: DataUsageEntry {
                size: 11,
                ..Default::default()
            },
            tier_registry_generation: Some(8),
        },
        SystemTime::now(),
    );

    assert!(!applied);
    assert_eq!(cache.find("bucket").map(|entry| entry.size), Some(3));
    assert!(cache.info.last_update.is_none());
}
