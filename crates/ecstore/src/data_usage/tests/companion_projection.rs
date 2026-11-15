// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use rustfs_data_usage::{
    BucketUsageCertification, BucketUsageValueState, DATA_USAGE_BUCKET_OBSERVATION_VERSION, DataUsageBucketObservation,
    DataUsageBucketObservationSource, DataUsageSnapshotSetState,
};

mod native_compatibility;

fn companion_snapshot(bucket: &str, incarnation: uuid::Uuid) -> DataUsageInfo {
    let completed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(40);
    let mut snapshot = data_usage_info_for_test(bucket, 10, 100, completed_at + Duration::from_secs(10));
    snapshot.scanner_epoch = Some(2);
    snapshot.scanner_cycle = Some(9);
    snapshot.usage_snapshot_converged = Some(false);
    snapshot.usage_snapshot_pending_buckets = vec![bucket.to_string()];
    snapshot.usage_snapshot_pending_bucket_count = 1;
    let canonical = snapshot.buckets_usage.get_mut(bucket).expect("canonical bucket");
    canonical.bucket_incarnation = Some(incarnation.to_string());
    canonical.certify(BucketUsageCertification {
        epoch: 2,
        cycle: 3,
        certified_at: SystemTime::UNIX_EPOCH + Duration::from_secs(30),
    });
    canonical.mark_carried();
    snapshot.usage_snapshot_set_states = vec![DataUsageSnapshotSetState {
        pool_index: 0,
        set_index: 0,
        scanner_epoch: Some(2),
        scanner_cycle: Some(9),
        scan_plan_digest: Some([1; 32]),
        complete: true,
        ..Default::default()
    }];
    let mut observed = BucketUsageInfo {
        size: 250,
        objects_count: 25,
        versions_count: 25,
        bucket_incarnation: Some(incarnation.to_string()),
        ..Default::default()
    };
    observed.mark_observed();
    snapshot.usage_snapshot_bucket_observations.insert(
        bucket.to_string(),
        DataUsageBucketObservation {
            version: DATA_USAGE_BUCKET_OBSERVATION_VERSION,
            usage: observed,
            snapshot_identity: snapshot.snapshot_identity(),
            completed_at,
            sources: vec![DataUsageBucketObservationSource {
                pool_index: 0,
                set_index: 0,
                bucket_incarnation: incarnation.to_string(),
                scanner_epoch: 2,
                scanner_cycle: 4,
                scan_plan_digest: [1; 32],
                started_plan: [2; 32],
                requested_plan: [3; 32],
                completed_at,
            }],
        },
    );
    snapshot
}

#[test]
fn companion_cannot_backfill_quota_and_projection_is_rejected_at_storage_boundaries() {
    let incarnation = uuid::Uuid::from_u128(31);
    let mut snapshot = companion_snapshot("bucket", incarnation);
    let mut canonical_quota = HashMap::new();
    backfill_degraded_baseline_from_observed(&mut canonical_quota, &snapshot);
    assert_eq!(canonical_quota.get("bucket"), Some(&100), "quota reads only the unchanged main map");
    assert_eq!(
        snapshot.project_bucket_observations_for_admin(&HashMap::from([("bucket".to_string(), incarnation.to_string())])),
        1
    );
    let mut accidental_quota = HashMap::new();
    backfill_degraded_baseline_from_observed(&mut accidental_quota, &snapshot);
    assert!(accidental_quota.is_empty());
    assert!(reject_admin_usage_projection(&snapshot).is_err());
    assert!(certified_bucket_usage(&snapshot, "bucket", incarnation).is_none());
    let wire = serde_json::to_vec(&snapshot).expect("admin response remains serializable");
    assert!(parse_usage_snapshot(&wire).is_err(), "a serialized response is not a persisted snapshot");
}

#[test]
fn companion_only_observed_snapshot_does_not_create_a_legacy_quota_baseline() {
    let incarnation = uuid::Uuid::from_u128(31);
    let mut snapshot = companion_snapshot("bucket", incarnation);
    snapshot.buckets_usage.clear();
    snapshot.bucket_sizes.clear();
    snapshot.buckets_count = 0;
    snapshot.calculate_totals();
    snapshot.usage_snapshot_complete = false;
    snapshot.usage_snapshot_partial = true;
    snapshot.usage_snapshot_observational = true;
    snapshot.usage_snapshot_set_states[0].complete = false;
    let decoded = parse_usage_snapshot(&serde_json::to_vec(&snapshot).expect("encode companion-only snapshot"))
        .expect("read valid partial snapshot");
    assert!(decoded.is_valid_partial_snapshot());
    let mut baseline = HashMap::new();
    backfill_degraded_baseline_from_observed(&mut baseline, &decoded);
    assert!(baseline.is_empty(), "unknown usage must not become either zero or mixed quota usage");
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-08", "assertion_id": "USAGE-08-A02", "status": "PASS",
            "evidence": {"primary_buckets": decoded.buckets_usage.len(), "legacy_bucket_sizes": decoded.bucket_sizes.len(), "quota_baseline_buckets": baseline.len(), "companion_buckets": decoded.usage_snapshot_bucket_observations.len(), "partial": decoded.is_valid_partial_snapshot()}
        })
    );
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-08", "status": "PASS", "scope": "process-local"})
    );
}

#[test]
fn bucket_removal_discards_companions_even_without_a_canonical_entry() {
    let mut snapshot = companion_snapshot("bucket", uuid::Uuid::from_u128(31));
    snapshot.buckets_usage.clear();
    snapshot.bucket_sizes.clear();
    assert!(data_usage_contains_bucket(&snapshot, "bucket"));
    assert!(remove_bucket_usage_from_info(&mut snapshot, "bucket"));
    assert!(!data_usage_contains_bucket(&snapshot, "bucket"));
    assert!(snapshot.usage_snapshot_bucket_observations.is_empty());
}

#[test]
fn partial_selection_replaces_value_provenance_together() {
    let incarnation = uuid::Uuid::from_u128(31);
    let authoritative = companion_snapshot("bucket", incarnation);
    let mut observed = authoritative.clone();
    observed.usage_snapshot_authoritative_baseline = Some(authoritative.snapshot_identity());
    observed.scanner_cycle = Some(10);
    observed.last_update = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
    observed.usage_snapshot_complete = false;
    observed.usage_snapshot_partial = true;
    observed.usage_snapshot_observational = true;
    observed.usage_snapshot_bucket_observations.clear();
    let usage = observed.buckets_usage.get_mut("bucket").expect("observed main bucket");
    usage.size = 175;
    usage.mark_observed();
    let (selected, _) = select_admin_data_usage_snapshot(authoritative, true, Some(observed));
    assert_eq!(selected.buckets_usage["bucket"].size, 175);
    assert_eq!(selected.buckets_usage["bucket"].usage_value_state, Some(BucketUsageValueState::Observed));
    assert_eq!(selected.buckets_usage["bucket"].usage_certification, None);
    assert!(
        selected.usage_snapshot_bucket_observations.is_empty(),
        "old root companions cannot cross a baseline change"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn companion_admin_cache_isolated_from_quota_memory_and_persistence() {
    use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
    use crate::config::com::save_config;
    use crate::storage_api_contracts::bucket::MakeBucketOptions;

    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let bucket = "usage-companion";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket for incarnation validation");
    let incarnation = store
        .bucket_incarnation_id_from_disk(bucket)
        .await
        .expect("trusted incarnation");
    let canonical = companion_snapshot(bucket, incarnation);
    let bytes = serde_json::to_vec(&canonical).expect("encode canonical root with companion");
    save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), bytes.clone())
        .await
        .expect("save canonical root");
    clear_usage_memory_cache_for_test().await;
    invalidate_data_usage_snapshot_cache().await;
    let mut samples = Vec::new();
    let mut rejected_writes = 0;
    for _ in 0..2 {
        let response = load_admin_data_usage_from_backend_cached(store.clone())
            .await
            .expect("read admin cache");
        assert_eq!(response.buckets_usage[bucket].size, 250);
        assert!(response.usage_snapshot_admin_projection);
        replace_bucket_usage_memory_from_info(&response).await;
        assert!(memory_cache().read().await.is_empty(), "response cannot seed quota memory");
        let admin_bytes = response.buckets_usage[bucket].size;
        let rejected = store_data_usage_in_backend(response, store.clone()).await.is_err();
        assert!(rejected);
        rejected_writes += usize::from(rejected);
        let quota = load_data_usage_from_backend_cached(store.clone())
            .await
            .expect("read independent quota cache");
        assert_eq!(quota.buckets_usage[bucket].size, 100);
        assert!(!quota.usage_snapshot_admin_projection);
        samples.push((admin_bytes, quota.buckets_usage[bucket].size));
        invalidate_data_usage_snapshot_cache().await;
    }
    let persisted = read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("read unchanged canonical root");
    assert_eq!(persisted, bytes);
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-18", "assertion_id": "USAGE-18-A03", "status": "PASS",
            "evidence": {"admin_and_quota_byte_samples": samples, "rejected_projected_writes": rejected_writes, "quota_memory_entries": memory_cache().read().await.len(), "persisted_canonical_unchanged": persisted == bytes}
        })
    );
    let paths = _dirs.iter().map(|dir| dir.path().to_path_buf()).collect::<Vec<_>>();
    drop(_dirs);
    assert!(paths.iter().all(|path| !path.exists()));
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-18", "status": "PASS", "fixtures_removed": paths.iter().all(|path| !path.exists())})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn blocked_companion_identity_stays_unknown_without_delaying_healthy_buckets() {
    use crate::bucket::metadata_sys::{
        acquire_metadata_transaction_guard, init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks,
    };
    use crate::config::com::save_config;
    use crate::storage_api_contracts::bucket::MakeBucketOptions;

    // Admin reads also feed spawned metrics collectors across crate boundaries.
    fn assert_send<T: Send>(future: T) -> T {
        future
    }

    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let blocked = "usage-blocked-identity";
    let healthy = "usage-healthy-identity";
    let mut snapshots = Vec::new();
    for bucket in [blocked, healthy] {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create companion identity fixture");
        let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.expect("real incarnation");
        snapshots.push(companion_snapshot(bucket, incarnation));
    }
    let mut canonical = snapshots.remove(0);
    let other = snapshots.remove(0);
    canonical.buckets_usage.extend(other.buckets_usage);
    canonical.bucket_sizes.extend(other.bucket_sizes);
    canonical
        .usage_snapshot_bucket_observations
        .extend(other.usage_snapshot_bucket_observations);
    canonical.usage_snapshot_pending_buckets.push(healthy.to_string());
    canonical.usage_snapshot_pending_bucket_count = 2;
    canonical.buckets_count = 2;
    canonical.calculate_totals();
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&canonical).expect("encode both canonical values and companions"),
    )
    .await
    .expect("persist usage fixture");
    invalidate_data_usage_snapshot_cache().await;
    let blocked_guard = acquire_metadata_transaction_guard(
        store.clone(),
        blocked,
        "admin_identity_timeout_test",
        rustfs_lock::LockType::Exclusive,
    )
    .await
    .expect("hold the real bucket metadata writer lock");
    let response = tokio::time::timeout(
        ADMIN_USAGE_INCARNATION_TIMEOUT + Duration::from_secs(3),
        assert_send(load_admin_data_usage_from_backend_cached(store.clone())),
    )
    .await
    .expect("optional projection must return before the blocked metadata lock's long timeout")
    .expect("canonical admin response stays available");
    assert!(
        !response.buckets_usage.contains_key(blocked),
        "an unavailable current identity cannot bind the canonical value"
    );
    assert!(!response.bucket_sizes.contains_key(blocked));
    assert_eq!(response.buckets_usage[healthy].size, 250);
    assert_eq!(response.buckets_usage[healthy].usage_value_state, Some(BucketUsageValueState::Observed));
    assert!(response.usage_snapshot_admin_projection);
    assert!(
        ADMIN_DATA_USAGE_SNAPSHOT_REFRESH
            .get()
            .expect("admin refresh mutex")
            .try_lock()
            .is_ok()
    );
    drop(blocked_guard);
    let recovered = load_admin_data_usage_from_backend_cached(store.clone())
        .await
        .expect("refresh after lock recovery");
    assert_eq!(
        recovered.buckets_usage[blocked].size, 250,
        "unavailable identity can recover on a cache hit"
    );
    assert_eq!(recovered.buckets_usage[healthy].size, 250);
    let quota = load_data_usage_from_backend_cached(store)
        .await
        .expect("separate canonical quota read");
    assert_eq!(quota.buckets_usage[blocked].size, 100);
    assert_eq!(quota.buckets_usage[healthy].size, 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn cached_canonical_admin_usage_rechecks_current_identity_without_peer_invalidation() {
    use crate::bucket::metadata::{load_bucket_incarnation, save_bucket_incarnation};
    use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
    use crate::config::com::save_config;
    use crate::storage_api_contracts::bucket::MakeBucketOptions;

    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let bucket = "usage-recreated-cache";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create identity fixture");
    let old_identity = store
        .bucket_incarnation_id_from_disk(bucket)
        .await
        .expect("original identity");
    let mut canonical = companion_snapshot(bucket, old_identity);
    canonical.usage_snapshot_bucket_observations.clear();
    canonical.usage_snapshot_converged = Some(true);
    canonical.usage_snapshot_pending_buckets.clear();
    canonical.usage_snapshot_pending_bucket_count = 0;
    canonical
        .buckets_usage
        .get_mut(bucket)
        .expect("canonical bucket")
        .objects_count = 107;
    canonical.calculate_totals();
    let original_bytes = serde_json::to_vec(&canonical).expect("encode canonical old bucket");
    save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), original_bytes.clone())
        .await
        .expect("persist canonical root");
    invalidate_data_usage_snapshot_cache().await;
    let first = load_admin_data_usage_from_backend_cached(store.clone())
        .await
        .expect("prime raw admin cache");
    assert_eq!(first.buckets_usage[bucket].objects_count, 107);
    assert!(first.is_complete_bucket_usage_snapshot());
    let cache_generation = admin_data_usage_snapshot_generation();
    let new_identity = uuid::Uuid::new_v4();
    // Simulate the storage-owned identity changing on another node while this
    // process has not received its invalidation notice. Do not clear the cache.
    save_bucket_incarnation(store.clone(), bucket, new_identity)
        .await
        .expect("publish changed disk identity");
    assert_eq!(
        load_bucket_incarnation(store.clone(), bucket)
            .await
            .expect("read new sidecar"),
        Some(new_identity)
    );
    let second = load_admin_data_usage_from_backend_cached(store.clone())
        .await
        .expect("read raw cache with fresh identity");
    assert_eq!(admin_data_usage_snapshot_generation(), cache_generation);
    assert!(
        !second.buckets_usage.contains_key(bucket),
        "same name cannot retain the predecessor's 107"
    );
    assert!(!second.bucket_sizes.contains_key(bucket));
    assert_eq!(second.usage_snapshot_converged, Some(false));
    assert!(second.usage_snapshot_admin_projection);
    assert!(reject_admin_usage_projection(&second).is_err());
    assert!(certified_bucket_usage(&second, bucket, new_identity).is_none());
    assert!(parse_usage_snapshot(&serde_json::to_vec(&second).expect("encode display response")).is_err());
    let quota = load_data_usage_from_backend_cached(store.clone())
        .await
        .expect("independent quota baseline");
    assert_eq!(quota.buckets_usage[bucket].objects_count, 107);
    assert_eq!(
        read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("unchanged persisted root"),
        original_bytes
    );
    // Rebinding a cached raw baseline is transient: recovery does not need to
    // reload or permanently blank a healthy bucket's statistics.
    save_bucket_incarnation(store.clone(), bucket, old_identity)
        .await
        .expect("restore fixture identity");
    let recovered = load_admin_data_usage_from_backend_cached(store)
        .await
        .expect("recover on another cache hit");
    assert_eq!(recovered.buckets_usage[bucket].objects_count, 107);
    assert!(recovered.is_complete_bucket_usage_snapshot());
    assert_eq!(admin_data_usage_snapshot_generation(), cache_generation);
    invalidate_data_usage_snapshot_cache().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn admin_memory_overlay_cannot_reintroduce_a_predecessor_identity() {
    use crate::bucket::metadata::save_bucket_incarnation;
    use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
    use crate::config::com::save_config;
    use crate::storage_api_contracts::bucket::MakeBucketOptions;

    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let bucket = "usage-recreated-overlay";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create overlay identity fixture");
    let old_identity = store
        .bucket_incarnation_id_from_disk(bucket)
        .await
        .expect("original identity");
    let mut canonical = companion_snapshot(bucket, old_identity);
    canonical.usage_snapshot_bucket_observations.clear();
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&canonical).expect("encode canonical root"),
    )
    .await
    .expect("persist overlay fixture");
    invalidate_data_usage_snapshot_cache().await;
    let cached_usage = canonical.buckets_usage[bucket].clone();
    replace_bucket_usage_memory_from_authoritative(bucket, cached_usage, SystemTime::now()).await;
    let new_identity = uuid::Uuid::new_v4();
    save_bucket_incarnation(store.clone(), bucket, new_identity)
        .await
        .expect("publish a replacement identity");
    let response = load_admin_data_usage_with_memory_overlay(store.clone())
        .await
        .expect("project after memory overlay");
    assert!(!response.buckets_usage.contains_key(bucket));
    assert_eq!(response.usage_snapshot_converged, Some(false));
    assert!(response.usage_snapshot_admin_projection);
    clear_usage_memory_cache_for_test().await;
    invalidate_data_usage_snapshot_cache().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn admin_overlay_flags_only_change_keeps_identity_bound_cold_usage() {
    use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
    use crate::config::com::save_config;
    use crate::storage_api_contracts::bucket::MakeBucketOptions;

    let (_dirs, store) = isolated_store_over_temp_disks().await;
    init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    assert!(
        !runtime_sources::setup_is_dist_erasure().await,
        "exercise the production single-process overlay"
    );
    let cold = "usage-overlay-cold";
    let unverified = "usage-overlay-unverified";
    for bucket in [cold, unverified] {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create overlay fixture bucket");
    }
    let identity = store
        .bucket_incarnation_id_from_disk(cold)
        .await
        .expect("cold bucket identity");
    let mut canonical = companion_snapshot(cold, identity);
    canonical.usage_snapshot_bucket_observations.clear();
    canonical.usage_snapshot_converged = Some(true);
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&canonical).expect("encode cold canonical value"),
    )
    .await
    .expect("persist cold baseline");
    clear_usage_memory_cache_for_test().await;
    invalidate_data_usage_snapshot_cache().await;
    memory_cache().write().await.insert(
        unverified.to_string(),
        cached_bucket_usage_from_backend(BucketUsageInfo::default(), SystemTime::UNIX_EPOCH, false),
    );
    record_bucket_object_write_memory(unverified, None, 42).await;
    {
        let cache = memory_cache().read().await;
        assert!(cache[unverified].dirty);
        assert!(!cache[unverified].authoritative);
    }
    let response = load_admin_data_usage_with_memory_overlay(store)
        .await
        .expect("identity-filtered flags-only overlay");
    assert_eq!(response.buckets_usage[cold].size, 100);
    assert_eq!(response.objects_total_size, 100);
    assert_eq!(response.bucket_sizes[cold], 100);
    assert!(!response.buckets_usage.contains_key(unverified));
    assert!(response.usage_snapshot_admin_projection);
    assert_eq!(response.usage_snapshot_converged, Some(false));
    assert!(!response.is_complete_bucket_usage_snapshot());
    assert!(!response.is_valid_partial_snapshot());
    clear_usage_memory_cache_for_test().await;
    invalidate_data_usage_snapshot_cache().await;
}
