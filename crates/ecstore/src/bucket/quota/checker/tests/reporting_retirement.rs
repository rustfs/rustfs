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
use crate::disk::DiskAPI as _;
use crate::object_api::{ObjectOptions, PutObjReader};
use crate::storage_api_contracts::object::ObjectIO as _;
use crate::test_metrics::CapturingRecorder;
use rustfs_data_usage::{BucketUsageInfo, DataUsageInfo};
use std::time::SystemTime;

async fn stop_reporting_fixture(store: &crate::store::ECStore) {
    if let Some(cancel) = store.ctx.background_cancel_token() {
        cancel.cancel();
    }
    for pool in &store.pools {
        for set in &pool.disk_set {
            let disks = set.disks.read().await.clone();
            for disk in disks.iter().flatten() {
                disk.close().await.expect("stop fixture disk monitoring");
            }
        }
    }
}

async fn install_reporting_snapshot(
    store: Arc<crate::store::ECStore>,
    bucket: &str,
    source: &str,
    size: u64,
) -> Option<(String, Vec<u8>)> {
    if source == "missing" {
        return None;
    }
    let mut info = DataUsageInfo {
        last_update: Some(SystemTime::now()),
        scanner_epoch: Some(1),
        scanner_cycle: Some(1),
        buckets_count: 1,
        ..Default::default()
    };
    info.buckets_usage.insert(
        bucket.to_owned(),
        BucketUsageInfo {
            size,
            objects_count: 1,
            versions_count: 1,
            ..Default::default()
        },
    );
    info.bucket_sizes.insert(bucket.to_owned(), size);
    info.calculate_totals();
    let object = match source {
        "legacy" => rustfs_data_usage::LEGACY_DATA_USAGE_OBJECT_NAME,
        "incomplete" => rustfs_data_usage::DATA_USAGE_OBJECT_NAME,
        "complete" => {
            info.usage_snapshot_complete = true;
            rustfs_data_usage::DATA_USAGE_OBJECT_NAME
        }
        "observed" => {
            info.usage_snapshot_complete = true;
            info.usage_snapshot_converged = Some(false);
            info.usage_snapshot_authoritative_baseline = Some(DataUsageInfo::default().snapshot_identity());
            rustfs_data_usage::DATA_USAGE_OBSERVED_OBJECT_NAME
        }
        _ => panic!("unknown scanner fixture source"),
    };
    let path = format!("{}/{object}", crate::disk::BUCKET_META_PREFIX);
    let bytes = serde_json::to_vec(&info).expect("scanner fixture encoding");
    crate::config::com::save_config(store, &path, bytes.clone())
        .await
        .expect("persist scanner fixture");
    if source == "complete" {
        crate::data_usage::replace_bucket_usage_memory_from_info(&info).await;
    }
    Some((path, bytes))
}

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn durable_quota_reporting_retires_degraded_floors_without_changing_ledger_admission() {
    let recorder = CapturingRecorder::default();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    for protocol in [1, 2] {
        for source in ["missing", "legacy", "incomplete", "observed", "complete"] {
            let (_dirs, store, sys) = isolated_durable_quota_store().await;
            let checker = QuotaChecker::new(sys);
            let bucket = format!("quota-report-{protocol}-{}", Uuid::new_v4().simple());
            let quota = if protocol == 1 {
                BucketQuota::new(Some(4096))
            } else {
                BucketQuota::new_sharded(4096)
            };
            create_bucket_with_quota_json(&store, &bucket, serde_json::to_vec(&quota).expect("quota fixture encoding")).await;
            let snapshot = install_reporting_snapshot(store.clone(), &bucket, source, 1_000_000).await;
            crate::data_usage::invalidate_data_usage_snapshot_cache().await;
            let unavailable_before = recorder.counter_value(METRIC_QUOTA_USAGE_REPORT_UNAVAILABLE_TOTAL, &[]);
            let check = checker
                .check_quota(&bucket, QuotaOperation::PutObject, 512)
                .await
                .expect("reporting cannot fail ledger admission");
            let (_, usage) = checker.get_quota_stats(&bucket).await.expect("best-effort quota statistics");
            let preview = checker
                .check_quota_with_usage_reporting(&bucket, QuotaOperation::PutObject, 512, true)
                .await
                .expect("admin preview uses the same optional report");

            // Exercise the real storage entry point before testing the new
            // reporting semantics: the same ledger must admit and refuse
            // these writes even when a scanner floor claims a million bytes.
            let mut too_large = PutObjReader::from_vec(vec![0x5a; 8192]);
            assert!(
                matches!(
                    store
                        .put_object(&bucket, "too-large", &mut too_large, &ObjectOptions::default())
                        .await,
                    Err(StorageError::QuotaExceeded { .. })
                ),
                "protocol={protocol}, source={source}"
            );
            let mut fits = PutObjReader::from_vec(vec![0x5a; 512]);
            store
                .put_object(&bucket, "fits", &mut fits, &ObjectOptions::default())
                .await
                .expect("ledger admits 512 bytes");
            let mut fills = PutObjReader::from_vec(vec![0x5a; 3584]);
            store
                .put_object(&bucket, "fills", &mut fills, &ObjectOptions::default())
                .await
                .expect("ledger admits exactly the remaining quota");
            let mut exceeds = PutObjReader::from_vec(vec![0x5a; 1]);
            assert!(
                matches!(
                    store
                        .put_object(&bucket, "exceeds", &mut exceeds, &ObjectOptions::default())
                        .await,
                    Err(StorageError::QuotaExceeded { .. })
                ),
                "ledger cannot undercount protocol={protocol}, source={source}"
            );

            assert!(check.allowed && check.uses_durable_reservations);
            let expected_usage = (source == "complete").then_some(1_000_000);
            assert_eq!(
                check.current_usage, expected_usage,
                "protocol={protocol}, source={source}: a floor is not a current report"
            );
            assert_eq!(check.remaining, expected_usage.map(|_| 0));
            assert_eq!(usage, expected_usage, "statistics use the same best-effort reporting contract");
            assert_eq!(preview.current_usage, expected_usage);
            assert_eq!(preview.remaining, expected_usage.map(|_| 0));
            assert_eq!(preview.allowed, source != "complete", "unknown preview is advisory, not a reservation");
            assert_eq!(
                recorder.counter_value(METRIC_QUOTA_USAGE_REPORT_UNAVAILABLE_TOTAL, &[]) - unavailable_before,
                if source == "complete" { 0 } else { 3 },
                "each unknown check/statistics/preview report is counted"
            );
            assert_eq!(crate::data_usage::reload_unknown_bucket_usage_calls_for_test(&bucket), 0);
            assert_eq!(persisted_quota(&store, &bucket).await.reservation_protocol, Some(protocol));
            if let Some((path, original)) = snapshot {
                assert_eq!(
                    crate::config::com::read_config(store.clone(), &path)
                        .await
                        .expect("retained scanner evidence"),
                    original
                );
            }
            crate::data_usage::prepare_bucket_usage_for_namespace_change(&bucket, None)
                .await
                .expect("fixture cache cleanup");
            crate::data_usage::invalidate_data_usage_snapshot_cache().await;
            stop_reporting_fixture(&store).await;
        }
    }
}

#[tokio::test]
#[serial]
async fn snapshot_quota_admission_retains_degraded_floors_until_migration() {
    for source in ["missing", "legacy", "incomplete", "observed"] {
        let (_dirs, store, _) = isolated_durable_quota_store().await;
        let bucket = format!("quota-compat-{}", Uuid::new_v4().simple());
        create_bucket_with_quota_json(&store, &bucket, legacy_quota_json(4096)).await;
        // An unregistered checker models a snapshot-protocol reader without
        // starting the optional background migration during this assertion.
        let checker = QuotaChecker::new(Arc::new(RwLock::new(BucketMetadataSys::new(store.clone()))));
        install_reporting_snapshot(store.clone(), &bucket, source, 1000).await;
        crate::data_usage::invalidate_data_usage_snapshot_cache().await;
        let check = checker.check_quota(&bucket, QuotaOperation::PutObject, 512).await;
        if source == "missing" {
            assert!(matches!(check, Err(QuotaError::UsageUnavailable { .. })));
        } else {
            let check = check.expect("legacy admission retains its grounded compatibility fallback");
            assert_eq!(check.current_usage, Some(1000));
            assert!(check.allowed && !check.uses_durable_reservations);
            assert!(
                !checker
                    .check_quota(&bucket, QuotaOperation::PutObject, 4096)
                    .await
                    .expect("legacy quota decision")
                    .allowed
            );
        }
        assert_eq!(persisted_quota(&store, &bucket).await.reservation_protocol, None);
        crate::data_usage::prepare_bucket_usage_for_namespace_change(&bucket, None)
            .await
            .expect("fixture cache cleanup");
        crate::data_usage::invalidate_data_usage_snapshot_cache().await;
        stop_reporting_fixture(&store).await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn quota_without_limit_keeps_the_zero_usage_io_fast_path() {
    let (_dirs, store, sys) = isolated_durable_quota_store().await;
    let bucket = format!("quota-unlimited-{}", Uuid::new_v4().simple());
    create_bucket_with_quota_json(
        &store,
        &bucket,
        serde_json::to_vec(&BucketQuota::new(None)).expect("unlimited quota encoding"),
    )
    .await;
    let recorder = CapturingRecorder::default();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let check = QuotaChecker::new(sys)
        .check_quota(&bucket, QuotaOperation::PutObject, u64::MAX)
        .await
        .expect("unlimited quota fast path");
    assert!(check.allowed);
    assert_eq!(check.current_usage, None);
    assert_eq!(check.quota_limit, None);
    assert_eq!(recorder.counter_value(METRIC_QUOTA_USAGE_REPORT_UNAVAILABLE_TOTAL, &[]), 0);
    assert_eq!(crate::data_usage::reload_unknown_bucket_usage_calls_for_test(&bucket), 0);
    stop_reporting_fixture(&store).await;
}

#[tokio::test]
#[serial]
async fn quota_statistics_remain_available_without_a_complete_scanner_snapshot() {
    use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};

    for configuration in ["unconfigured", "unlimited", "snapshot"] {
        for source in ["missing", "legacy", "incomplete", "observed", "complete", "empty"] {
            let (_dirs, store, _) = isolated_durable_quota_store().await;
            let bucket = format!("quota-statistics-{}", Uuid::new_v4().simple());
            match configuration {
                "unconfigured" => store
                    .make_bucket(&bucket, &MakeBucketOptions::default())
                    .await
                    .expect("create bucket without a quota configuration"),
                "unlimited" => {
                    create_bucket_with_quota_json(
                        &store,
                        &bucket,
                        serde_json::to_vec(&BucketQuota::new(None)).expect("unlimited quota encoding"),
                    )
                    .await;
                }
                "snapshot" => create_bucket_with_quota_json(&store, &bucket, legacy_quota_json(4096)).await,
                _ => unreachable!(),
            }
            // An unregistered checker keeps legacy migration out of the
            // assertion: reporting must work before a protocol upgrade.
            let metadata_sys = BucketMetadataSys::new(store.clone());
            let (metadata, _) = metadata_sys.get_config(&bucket).await.expect("load existing bucket metadata");
            metadata_sys.set(bucket.clone(), metadata).await;
            let checker = QuotaChecker::new(Arc::new(RwLock::new(metadata_sys)));
            let (snapshot_source, size) = if source == "empty" { ("complete", 0) } else { (source, 1000) };
            install_reporting_snapshot(store.clone(), &bucket, snapshot_source, size).await;
            crate::data_usage::invalidate_data_usage_snapshot_cache().await;

            let statistics = checker.get_quota_stats(&bucket).await;
            let early_reloads = crate::data_usage::reload_unknown_bucket_usage_calls_for_test(&bucket);
            let missing_bucket = format!("missing-{}", Uuid::new_v4().simple());
            let missing_statistics = checker.get_quota_stats(&missing_bucket).await;
            crate::data_usage::prepare_bucket_usage_for_namespace_change(&bucket, None)
                .await
                .expect("fixture cache cleanup");
            crate::data_usage::invalidate_data_usage_snapshot_cache().await;
            stop_reporting_fixture(&store).await;

            let (quota, usage) = statistics.unwrap_or_else(|error| {
                panic!("configuration={configuration}, source={source}: quota statistics must remain available: {error}")
            });
            assert_eq!(quota.quota, (configuration == "snapshot").then_some(4096));
            assert_eq!(
                usage,
                (snapshot_source == "complete").then_some(size),
                "configuration={configuration}, source={source}: only known usage is numeric"
            );
            assert_eq!(early_reloads, 0, "read-only statistics must not force an early snapshot reload");
            assert!(
                matches!(missing_statistics, Err(QuotaError::ConfigNotFound { bucket }) if bucket == missing_bucket),
                "reporting must still reject a missing bucket"
            );
        }
    }
}

#[tokio::test]
async fn quota_statistics_preserve_configuration_errors() {
    let (_dirs, store, _) = isolated_durable_quota_store().await;
    let bucket = format!("quota-invalid-{}", Uuid::new_v4().simple());
    create_bucket_with_quota_json(&store, &bucket, legacy_quota_json(4096)).await;
    let metadata_sys = BucketMetadataSys::new(store.clone());
    let (mut metadata, _) = metadata_sys.get_config(&bucket).await.expect("load existing bucket metadata");
    Arc::make_mut(&mut metadata).quota_config_json = b"{not-json".to_vec();
    metadata_sys.set(bucket.clone(), metadata).await;
    let checker = QuotaChecker::new(Arc::new(RwLock::new(metadata_sys)));

    let statistics = checker.get_quota_stats(&bucket).await;
    stop_reporting_fixture(&store).await;

    assert!(
        matches!(statistics, Err(QuotaError::InvalidConfig { .. })),
        "unavailable reporting must not hide malformed quota configuration"
    );
}
