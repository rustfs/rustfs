// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
use rustfs_data_usage::{
    DATA_USAGE_BUCKET_OBSERVATION_VERSION, DataUsageBucketObservation, DataUsageBucketObservationSource,
    DataUsageSnapshotSetState,
};

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn usage_quota_modes_preserve_actual_admission_decisions() {
    let (dirs, store, sys) = isolated_durable_quota_store().await;
    let bucket = format!("usage-no-quota-{}", Uuid::new_v4().simple());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("unconfigured quota bucket");
    let check = QuotaChecker::new(sys)
        .check_quota(&bucket, QuotaOperation::PutObject, u64::MAX)
        .await
        .expect("no quota");
    assert!(check.allowed && check.quota_limit.is_none());
    store
        .put_object(
            &bucket,
            "allowed",
            &mut PutObjReader::from_vec(vec![0x5a; 512]),
            &ObjectOptions::default(),
        )
        .await
        .expect("real no-quota write");
    stop_reporting_fixture(&store).await;
    let paths = dirs.iter().map(|dir| dir.path().to_path_buf()).collect::<Vec<_>>();
    drop(store);
    drop(dirs);
    assert!(paths.iter().all(|path| !path.exists()));

    assert_unlimited_quota_preserves_fast_path().await;
    assert_snapshot_quota_preserves_degraded_floors().await;
    assert_durable_quota_reporting_preserves_admission().await;
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-19", "assertion_id": "USAGE-19-A01", "status": "PASS",
            "evidence": {
                "quota_modes": ["unconfigured", "unlimited", "legacy-snapshot", "durable-v1", "durable-v2"],
                "durable_snapshot_sources": ["missing", "legacy", "incomplete", "observed", "complete"],
                "durable_storage_decisions_per_fixture": ["reject-8192", "accept-512", "accept-3584", "reject-1"],
                "legacy_missing_baseline_fails_closed": true,
                "legacy_grounded_baselines": ["legacy", "incomplete", "observed"]
            }
        })
    );
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-19", "status": "PASS", "fixtures_removed": true})
    );
}

async fn persist_companion_fixture(
    store: Arc<crate::store::ECStore>,
    bucket: &str,
    primary: bool,
    companion_size: u64,
) -> (String, Vec<u8>) {
    let incarnation = store
        .bucket_incarnation_id_from_disk(bucket)
        .await
        .expect("real companion bucket identity");
    let completed_at = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(20);
    let mut snapshot = DataUsageInfo {
        last_update: Some(completed_at),
        scanner_epoch: Some(2),
        scanner_cycle: Some(3),
        usage_snapshot_complete: primary,
        usage_snapshot_partial: !primary,
        usage_snapshot_observational: true,
        usage_snapshot_converged: Some(false),
        usage_snapshot_authoritative_baseline: Some(DataUsageInfo::default().snapshot_identity()),
        usage_snapshot_pending_buckets: vec![bucket.to_owned()],
        usage_snapshot_pending_bucket_count: 1,
        usage_snapshot_set_states: vec![DataUsageSnapshotSetState {
            pool_index: 0,
            set_index: 0,
            scanner_epoch: Some(2),
            scanner_cycle: Some(3),
            scan_plan_digest: Some([1; 32]),
            complete: primary,
            ..Default::default()
        }],
        ..Default::default()
    };
    if primary {
        snapshot.buckets_count = 1;
        snapshot.buckets_usage.insert(
            bucket.into(),
            BucketUsageInfo {
                size: 1000,
                objects_count: 1,
                versions_count: 1,
                bucket_incarnation: Some(incarnation.to_string()),
                ..Default::default()
            },
        );
        snapshot.bucket_sizes.insert(bucket.into(), 1000);
        snapshot.calculate_totals();
    }
    let mut usage = BucketUsageInfo {
        size: companion_size,
        objects_count: 1,
        versions_count: 1,
        bucket_incarnation: Some(incarnation.to_string()),
        ..Default::default()
    };
    usage.mark_observed();
    snapshot.usage_snapshot_bucket_observations.insert(
        bucket.into(),
        DataUsageBucketObservation {
            version: DATA_USAGE_BUCKET_OBSERVATION_VERSION,
            usage,
            snapshot_identity: snapshot.snapshot_identity(),
            completed_at,
            sources: vec![DataUsageBucketObservationSource {
                pool_index: 0,
                set_index: 0,
                bucket_incarnation: incarnation.to_string(),
                scanner_epoch: 2,
                scanner_cycle: 3,
                scan_plan_digest: [1; 32],
                started_plan: [2; 32],
                requested_plan: [3; 32],
                completed_at,
            }],
        },
    );
    let mut admin = snapshot.clone();
    assert_eq!(
        admin.project_bucket_observations_for_admin(&HashMap::from([(bucket.to_owned(), incarnation.to_string())])),
        1,
        "the companion must be valid, not discarded as malformed"
    );
    assert_eq!(admin.buckets_usage[bucket].size, companion_size);
    let path = format!(
        "{}/{}",
        crate::disk::BUCKET_META_PREFIX,
        rustfs_data_usage::DATA_USAGE_OBSERVED_OBJECT_NAME
    );
    let bytes = serde_json::to_vec(&snapshot).expect("encode valid companion snapshot");
    crate::config::com::save_config(store, &path, bytes.clone())
        .await
        .expect("persist companion beside canonical fields");
    crate::data_usage::invalidate_data_usage_snapshot_cache().await;
    (path, bytes)
}

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn usage_companions_do_not_expand_legacy_fallback_or_durable_admission() {
    let mut decisions = Vec::new();
    for protocol in [0, 1, 2] {
        for (primary, companion_size) in [(true, 0), (true, 1_000_000), (false, 1_000_000)] {
            let (dirs, store, sys) = isolated_durable_quota_store().await;
            let bucket = format!("usage-companion-quota-{}", Uuid::new_v4().simple());
            let quota = match protocol {
                0 => legacy_quota_json(4096),
                1 => serde_json::to_vec(&BucketQuota::new(Some(4096))).unwrap(),
                _ => serde_json::to_vec(&BucketQuota::new_sharded(4096)).unwrap(),
            };
            create_bucket_with_quota_json(&store, &bucket, quota).await;
            let (path, bytes) = persist_companion_fixture(store.clone(), &bucket, primary, companion_size).await;
            let checker = if protocol == 0 {
                QuotaChecker::new(Arc::new(RwLock::new(BucketMetadataSys::new(store.clone()))))
            } else {
                QuotaChecker::new(sys)
            };
            if protocol == 0 {
                let small = checker.check_quota(&bucket, QuotaOperation::PutObject, 512).await;
                if primary {
                    let small = small.expect("grounded primary fallback");
                    assert!(small.allowed && !small.uses_durable_reservations);
                    assert_eq!(small.current_usage, Some(1000));
                    assert!(
                        !checker
                            .check_quota(&bucket, QuotaOperation::PutObject, 4096)
                            .await
                            .expect("legacy limit")
                            .allowed
                    );
                } else {
                    assert!(
                        matches!(small, Err(QuotaError::UsageUnavailable { .. })),
                        "a companion cannot invent a legacy baseline"
                    );
                }
            } else {
                assert!(
                    checker
                        .check_quota(&bucket, QuotaOperation::PutObject, 512)
                        .await
                        .expect("durable checker")
                        .uses_durable_reservations
                );
                if protocol == 1 {
                    assert!(matches!(
                        store
                            .put_object(
                                &bucket,
                                "bootstrap",
                                &mut PutObjReader::from_vec(vec![0x5a; 512]),
                                &ObjectOptions::default()
                            )
                            .await,
                        Err(StorageError::SlowDown)
                    ));
                    crate::bucket::quota::reservation::retry_quota_reconciliation_for_test(store.clone(), &bucket)
                        .await
                        .expect("real v1 ledger bootstrap");
                }
                store
                    .put_object(&bucket, "fits", &mut PutObjReader::from_vec(vec![0x5a; 4096]), &ObjectOptions::default())
                    .await
                    .expect("ledger admits exactly the physical quota regardless of companion");
                assert!(
                    matches!(
                        store
                            .put_object(&bucket, "exceeds", &mut PutObjReader::from_vec(vec![0x5a; 1]), &ObjectOptions::default())
                            .await,
                        Err(StorageError::QuotaExceeded { .. })
                    ),
                    "a low companion cannot expand the ledger"
                );
            }
            assert_eq!(
                crate::config::com::read_config(store.clone(), &path)
                    .await
                    .expect("retained scanner evidence"),
                bytes
            );
            decisions
                .push(serde_json::json!({"protocol": protocol, "primary_baseline": primary, "companion_size": companion_size}));
            crate::data_usage::prepare_bucket_usage_for_namespace_change(&bucket, None)
                .await
                .expect("clear fixture caches");
            crate::data_usage::invalidate_data_usage_snapshot_cache().await;
            stop_reporting_fixture(&store).await;
            let paths = dirs.iter().map(|dir| dir.path().to_path_buf()).collect::<Vec<_>>();
            drop(store);
            drop(dirs);
            assert!(paths.iter().all(|path| !path.exists()));
        }
    }
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-19", "assertion_id": "USAGE-19-A02", "status": "PASS",
            "evidence": {"validated_companion_fixtures": decisions, "legacy_primary_bytes": 1000, "durable_limit_bytes": 4096, "companion_only_legacy_admission": "UsageUnavailable"}
        })
    );
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-19", "status": "PASS", "fixtures_removed": true})
    );
}
