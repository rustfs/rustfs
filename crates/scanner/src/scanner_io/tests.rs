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

use super::dirty_usage::{clear_dirty_usage_buckets_for_tests, dirty_usage_buckets_for_tests};
use super::io_disk::tier_stats_template;
use super::*;
use crate::scanner_budget::ScannerCycleBudgetConfig;
use crate::scanner_folder::ScannerItem;
use crate::storage_api::owner::{
    EcstorePoolDecommissionInfo, EcstoreRebalStatus, EcstoreRebalanceInfo, EcstoreRebalanceMeta, EcstoreRebalanceStats,
};
use crate::storage_api::scan::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions, ObjectIO as _};
use crate::{
    DiskOption, ECStore, Endpoint, EndpointServerPools, Endpoints, InstanceContext, PoolEndpoints, ScannerObjectOptions,
    ScannerPutObjReader, init_bucket_metadata_sys_for_scanner_tests, init_ecstore_config_for_scanner_tests,
    init_local_disks_with_instance_ctx, new_disk, path2_bucket_object_with_base_path,
};
use rustfs_filemeta::FileInfo;
use temp_env::with_var;
use time::OffsetDateTime;
use uuid::Uuid;

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

#[tokio::test]
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
        set.scanner_data_usage_publication_admission_guard().await.is_none(),
        "a set must not publish before the owner has refreshed its movement snapshot"
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
async fn data_usage_publish_fails_when_receiver_is_closed() {
    let (updates, receiver) = mpsc::channel(1);
    drop(receiver);

    let err = send_data_usage_update(&updates, DataUsageInfo::default())
        .await
        .expect_err("closed usage receiver must reject the scanner update");

    assert!(err.to_string().contains("receiver closed"));
}

#[tokio::test]
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
    assert_eq!(bucket_usage.objects_count, 2);
    assert_eq!(bucket_usage.size, 11);
    assert_eq!(usage.objects_total_count, 2);
    assert_eq!(usage.objects_total_size, 11);
    assert!(
        receiver.recv().await.is_none(),
        "a scanner cycle must publish at most one terminal usage snapshot"
    );
}

#[tokio::test]
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

    let acknowledgements = ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(snapshot.buckets.as_ref().clone()))
        .acknowledge_durable_usage();
    assert!(acknowledgements.is_empty());
    assert!(!dirty_usage_buckets().contains_key("temporarily-omitted"));
    clear_dirty_usage_buckets_for_tests();
}

#[test]
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
fn scanner_maintenance_change_advances_generation_and_marks_usage_dirty() {
    clear_dirty_usage_buckets_for_tests();
    let generation = scanner_maintenance_generation();

    record_scanner_maintenance_change("photos");

    assert!(scanner_maintenance_generation() > generation);
    assert!(dirty_usage_buckets().contains_key("photos"));
    clear_dirty_usage_buckets_for_tests();
}

#[test]
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
fn dirty_usage_is_acknowledged_only_after_durable_usage_confirmation() {
    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket("photos");
    let snapshot = snapshot_dirty_usage_buckets(&[bucket_info("photos")], dirty_usage_generation());

    let unconfirmed = ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(snapshot.buckets.as_ref().clone()));
    drop(unconfirmed);
    assert!(dirty_usage_buckets().contains_key("photos"));

    let confirmed = ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(snapshot.buckets.as_ref().clone()));
    let acknowledgements = confirmed.acknowledge_durable_usage();
    assert!(acknowledgements.is_empty());
    assert!(!dirty_usage_buckets().contains_key("photos"));
    clear_dirty_usage_buckets_for_tests();
}

#[test]
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
        ScannerCycleStatus::Incomplete
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

#[test]
fn scanner_cycle_fails_closed_for_namespace_disappearance() {
    for activity_status in [
        ScannerCycleActivityStatus::Changed,
        ScannerCycleActivityStatus::Unchanged,
        ScannerCycleActivityStatus::Unverified,
    ] {
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
fn scanner_concurrency_limit_preserves_available_when_unconfigured() {
    crate::reset_foreground_read_activity_for_test();
    assert_eq!(scanner_concurrency_limit(0, 4), 4);
}

#[test]
fn scanner_concurrency_limit_caps_to_configured_value() {
    crate::reset_foreground_read_activity_for_test();
    assert_eq!(scanner_concurrency_limit(2, 4), 2);
}

#[test]
fn scanner_concurrency_limit_never_exceeds_available_work() {
    crate::reset_foreground_read_activity_for_test();
    assert_eq!(scanner_concurrency_limit(8, 4), 4);
}

#[test]
fn scanner_concurrency_limit_handles_no_available_work() {
    crate::reset_foreground_read_activity_for_test();
    assert_eq!(scanner_concurrency_limit(2, 0), 0);
}

#[test]
fn scanner_concurrency_limit_yields_to_foreground_reads() {
    crate::reset_foreground_read_activity_for_test();
    crate::set_foreground_read_activity(8);
    assert_eq!(scanner_concurrency_limit(0, 4), 1);
    assert_eq!(scanner_concurrency_limit(3, 4), 1);
    crate::reset_foreground_read_activity_for_test();
}

#[test]
fn scanner_concurrency_limit_yields_to_streaming_reads() {
    crate::reset_foreground_read_activity_for_test();
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
fn scanner_max_concurrent_set_scans_uses_env_cap() {
    with_var(ENV_SCANNER_MAX_CONCURRENT_SET_SCANS, Some("2"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(scanner_max_concurrent_set_scans(4), 2);
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
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

    assert_eq!(template.len(), 4);
    for tier in ["WARM", "COLD", storageclass::STANDARD, storageclass::RRS] {
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
    apply_bucket_result_to_cache(
        &mut cache,
        DataUsageEntryInfo {
            name: "bucket".to_string(),
            parent: DATA_USAGE_ROOT.to_string(),
            entry: DataUsageEntry {
                size: 10,
                objects: 2,
                ..Default::default()
            },
        },
        update_time,
    );

    assert_eq!(cache.info.last_update, Some(update_time));
    let entry = cache.find("bucket").expect("bucket entry should remain present");
    assert_eq!(entry.size, 10);
    assert_eq!(entry.objects, 2);
}
