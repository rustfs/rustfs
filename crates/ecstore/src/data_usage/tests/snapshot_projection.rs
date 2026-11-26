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

#[test]
fn scalar_projection_borrows_snapshot_and_preserves_unknown_and_zero() {
    let loaded_at = tokio::time::Instant::now();
    let mut info = data_usage_info_for_test("known", 1, 42, SystemTime::UNIX_EPOCH);
    info.buckets_usage.insert("empty".to_string(), BucketUsageInfo::default());
    let cache = Some(CachedDataUsageSnapshot {
        snapshot: Some(UsageSnapshot::new(info, false)),
        last_load_failed: false,
        loaded_at,
        degraded_baseline: HashMap::new(),
    });
    let stored = cache
        .as_ref()
        .expect("cached snapshot")
        .snapshot
        .as_ref()
        .expect("known snapshot");
    let ttl = Duration::from_secs(30);

    for (bucket, expected) in [("known", Some(42)), ("empty", Some(0)), ("missing", None)] {
        let value = fresh_cached_data_usage_snapshot(&cache, loaded_at, ttl, |snapshot| {
            assert!(std::ptr::eq(snapshot, stored), "projection must borrow the cached snapshot");
            snapshot.info.buckets_usage.get(bucket).map(|usage| usage.size)
        })
        .expect("fresh snapshot")
        .expect("successful snapshot");
        let owned = fresh_cached_data_usage_snapshot(&cache, loaded_at, ttl, UsageSnapshot::clone_info)
            .expect("fresh snapshot")
            .expect("successful snapshot");
        assert_eq!(value, expected);
        assert_eq!(value, owned.buckets_usage.get(bucket).map(|usage| usage.size));
    }
}

#[test]
fn projection_is_skipped_for_missing_failed_expired_or_invalidated_snapshots() {
    let loaded_at = tokio::time::Instant::now();
    let ttl = Duration::from_secs(30);
    let never_project = |_: &UsageSnapshot| -> u64 { panic!("unusable snapshots must not be projected") };
    assert!(fresh_cached_data_usage_snapshot(&None, loaded_at, ttl, never_project).is_none());

    let mut cache = None;
    let failure = cache_data_usage_snapshot_result(&mut cache, Err(Error::ErasureReadQuorum), loaded_at, 1, 1, never_project)
        .expect("matching generation");
    assert!(matches!(failure, Err(Error::ErasureReadQuorum)));
    assert!(
        fresh_cached_data_usage_snapshot(&cache, loaded_at, ttl, never_project)
            .expect("negative cache remains fresh")
            .is_err()
    );

    cache = Some(CachedDataUsageSnapshot {
        snapshot: Some(UsageSnapshot::new(
            data_usage_info_for_test("known", 1, 42, SystemTime::UNIX_EPOCH),
            false,
        )),
        last_load_failed: false,
        loaded_at,
        degraded_baseline: HashMap::new(),
    });
    assert!(fresh_cached_data_usage_snapshot(&cache, loaded_at + ttl, ttl, never_project).is_none());
    cache = None;
    let rejected = cache_data_usage_snapshot_result(
        &mut cache,
        Ok(LoadedUsageBaseline {
            info: data_usage_info_for_test("known", 1, 42, SystemTime::UNIX_EPOCH),
            degraded_baseline: HashMap::new(),
            invalid_certification_claim: false,
            observed_unavailable: false,
        }),
        loaded_at,
        1,
        2,
        never_project,
    );
    assert!(rejected.is_none());
    assert!(cache.is_none(), "rejected generation must not republish stale data");
}

#[test]
fn projected_publication_moves_bucket_maps_into_cache() {
    let info = data_usage_info_for_test("known", 1, 42, SystemTime::UNIX_EPOCH);
    let original_key = info.buckets_usage.get_key_value("known").expect("source bucket").0.as_ptr();
    let mut cache = None;
    let usage = cache_data_usage_snapshot_result(
        &mut cache,
        Ok(LoadedUsageBaseline {
            info,
            degraded_baseline: HashMap::new(),
            invalid_certification_claim: false,
            observed_unavailable: false,
        }),
        tokio::time::Instant::now(),
        1,
        1,
        |snapshot| {
            let info = &snapshot.info;
            assert_eq!(
                info.buckets_usage
                    .get_key_value("known")
                    .expect("projected bucket")
                    .0
                    .as_ptr(),
                original_key
            );
            info.buckets_usage.get("known").map(|usage| usage.size)
        },
    )
    .expect("matching generation")
    .expect("successful publication");
    assert_eq!(usage, Some(42));
    let cached = cache
        .as_ref()
        .expect("published cache")
        .snapshot
        .as_ref()
        .expect("published snapshot");
    assert_eq!(
        cached
            .info
            .buckets_usage
            .get_key_value("known")
            .expect("cached bucket")
            .0
            .as_ptr(),
        original_key
    );

    let failure = cache_data_usage_snapshot_result(
        &mut cache,
        Err(Error::ErasureReadQuorum),
        tokio::time::Instant::now(),
        1,
        1,
        |_: &UsageSnapshot| -> u64 { panic!("a failed load must not project its retained snapshot") },
    )
    .expect("matching refresh generation");
    assert!(matches!(failure, Err(Error::ErasureReadQuorum)));
    let retained = cache
        .as_ref()
        .expect("negative cache")
        .snapshot
        .as_ref()
        .expect("last good ordering proof");
    assert_eq!(
        retained
            .info
            .buckets_usage
            .get_key_value("known")
            .expect("retained bucket")
            .0
            .as_ptr(),
        original_key,
        "a failed load must retain the snapshot without copying its bucket maps"
    );
}

#[test]
fn bucket_usage_certification_distinguishes_pending_and_unavailable_proof() {
    use BucketUsagePendingReason::{BucketPending, MissingBucketProof, PreviousIncarnation, SnapshotNotReady};
    use BucketUsageStatus::{Certified, Pending, Unavailable};
    use BucketUsageUnavailableReason::{InvalidBucketIncarnation, InvalidSnapshotProof};

    let id = uuid::Uuid::from_u128(7);
    let mut info = data_usage_info_for_test("known", 1, 42, SystemTime::UNIX_EPOCH);
    info.usage_snapshot_converged = Some(true);
    info.scanner_cycle = Some(1);
    info.scanner_epoch = Some(1);
    info.buckets_usage
        .get_mut("known")
        .expect("fixture bucket")
        .bucket_incarnation = Some(id.to_string());
    let certified = UsageSnapshot::new(info.clone(), false);
    assert_eq!(certified.bucket_usage_certification("known", id), Certified(42));
    assert_eq!(certified.bucket_usage_certification("missing", id), Pending(MissingBucketProof));
    assert_eq!(
        certified.bucket_usage_certification("known", uuid::Uuid::from_u128(8)),
        Pending(PreviousIncarnation)
    );
    assert_eq!(
        certified.bucket_usage_certification("known", uuid::Uuid::nil()),
        Unavailable(InvalidBucketIncarnation)
    );
    for (proof, expected) in [
        (None, Pending(MissingBucketProof)),
        (Some("corrupt".into()), Unavailable(InvalidBucketIncarnation)),
        (Some(uuid::Uuid::nil().to_string()), Unavailable(InvalidBucketIncarnation)),
    ] {
        let mut sample = info.clone();
        sample
            .buckets_usage
            .get_mut("known")
            .expect("fixture bucket")
            .bucket_incarnation = proof;
        assert_eq!(UsageSnapshot::new(sample, false).bucket_usage_certification("known", id), expected);
    }

    for (names, count, expected) in [
        (vec!["other"], 1, Certified(42)),
        (vec!["known"], 1, Pending(BucketPending)),
        (vec!["known"], 0, Unavailable(InvalidSnapshotProof)),
        (vec!["z", "a"], 2, Unavailable(InvalidSnapshotProof)),
        (vec!["other", "other"], 2, Unavailable(InvalidSnapshotProof)),
        (vec![], 0, Unavailable(InvalidSnapshotProof)),
    ] {
        let mut sample = info.clone();
        sample.usage_snapshot_converged = Some(false);
        sample.usage_snapshot_pending_buckets = names.into_iter().map(str::to_owned).collect();
        sample.usage_snapshot_pending_bucket_count = count;
        assert_eq!(UsageSnapshot::new(sample, false).bucket_usage_certification("known", id), expected);
    }

    for sample in [
        DataUsageInfo {
            usage_snapshot_partial: true,
            ..info.clone()
        },
        DataUsageInfo {
            scanner_epoch: None,
            usage_snapshot_converged: Some(false),
            usage_snapshot_pending_buckets: vec!["known".into()],
            ..info.clone()
        },
    ] {
        assert_eq!(
            UsageSnapshot::new(sample, false).bucket_usage_certification("known", id),
            Unavailable(InvalidSnapshotProof)
        );
    }

    let bootstrap = DataUsageInfo {
        last_update: Some(SystemTime::UNIX_EPOCH),
        scanner_epoch: Some(1),
        usage_snapshot_converged: Some(false),
        usage_snapshot_bootstrap_pending: true,
        ..Default::default()
    };
    for sample in [
        DataUsageInfo::default(),
        bootstrap,
        DataUsageInfo {
            usage_snapshot_observational: true,
            ..info.clone()
        },
        DataUsageInfo {
            scanner_epoch: None,
            ..info.clone()
        },
        DataUsageInfo {
            usage_snapshot_converged: None,
            ..info
        },
    ] {
        assert_eq!(
            UsageSnapshot::new(sample, false).bucket_usage_certification("known", id),
            Pending(SnapshotNotReady)
        );
    }
}

#[tokio::test]
#[serial]
async fn persisted_scalar_projection_shares_cache_refresh_and_invalidation() {
    use crate::bucket::metadata_sys::test_support::isolated_store_over_temp_disks;
    use crate::config::com::save_config;

    let (_dirs, store) = isolated_store_over_temp_disks().await;
    let mut info = data_usage_info_for_test("known", 1, 42, SystemTime::UNIX_EPOCH);
    info.buckets_usage.insert("empty".to_string(), BucketUsageInfo::default());
    info.bucket_sizes.insert("empty".to_string(), 0);
    info.buckets_count = 2;
    info.usage_snapshot_converged = Some(true);
    info.scanner_cycle = Some(1);
    info.scanner_epoch = Some(1);
    let incarnation = uuid::Uuid::from_u128(7);
    for usage in info.buckets_usage.values_mut() {
        usage.bucket_incarnation = Some(incarnation.to_string());
    }
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&info).expect("encode snapshot"),
    )
    .await
    .expect("persist snapshot");
    invalidate_data_usage_snapshot_cache().await;

    for (bucket, expected) in [("known", Some(42)), ("empty", Some(0)), ("missing", None)] {
        assert_eq!(persisted_bucket_usage(store.clone(), bucket).await, expected);
        assert_eq!(
            load_bucket_usage_from_backend_cached(store.clone(), bucket, incarnation)
                .await
                .expect("project bucket usage"),
            expected
        );
        let owned = load_data_usage_from_backend_cached(store.clone())
            .await
            .expect("owned cached snapshot");
        assert_eq!(expected, owned.buckets_usage.get(bucket).map(|usage| usage.size));
    }
    assert_eq!(
        load_bucket_usage_from_backend_cached(store.clone(), "known", uuid::Uuid::from_u128(8))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        load_bucket_usage_from_backend_cached(store.clone(), "known", uuid::Uuid::nil())
            .await
            .unwrap(),
        None
    );
    info.buckets_usage.get_mut("known").expect("known bucket").size = 84;
    info.bucket_sizes.insert("known".to_string(), 84);
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&info).expect("encode new snapshot"),
    )
    .await
    .expect("persist new snapshot without invalidating cache");
    assert_eq!(
        persisted_bucket_usage(store.clone(), "known").await,
        Some(42),
        "warm reads reuse the TTL cache"
    );
    invalidate_data_usage_snapshot_cache().await;
    assert_eq!(
        persisted_bucket_usage(store.clone(), "known").await,
        Some(84),
        "invalidation exposes the new snapshot"
    );
    assert_eq!(
        load_bucket_usage_from_backend_cached(store.clone(), "known", incarnation)
            .await
            .expect("project new certified snapshot"),
        Some(84)
    );

    info.scanner_cycle = Some(2);
    info.usage_snapshot_converged = Some(false);
    info.usage_snapshot_pending_bucket_count = 2;
    info.usage_snapshot_pending_buckets = vec!["other".into(), "other".into()];
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&info).expect("encode duplicate pending entries"),
    )
    .await
    .expect("persist malformed certification evidence");
    invalidate_data_usage_snapshot_cache().await;
    assert_eq!(
        load_bucket_usage_certification_from_backend_cached(store.clone(), "known", incarnation)
            .await
            .expect("read snapshot without accepting malformed certification"),
        BucketUsageStatus::Unavailable(BucketUsageUnavailableReason::InvalidSnapshotProof)
    );
    assert_eq!(persisted_bucket_usage(store.clone(), "known").await, Some(84));

    info.scanner_cycle = Some(3);
    info.usage_snapshot_converged = Some(true);
    info.usage_snapshot_pending_bucket_count = 0;
    info.usage_snapshot_pending_buckets.clear();
    info.buckets_count += 1;
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&info).expect("encode invalid bucket count"),
    )
    .await
    .expect("persist malformed completeness claim");
    invalidate_data_usage_snapshot_cache().await;
    assert_eq!(
        load_bucket_usage_certification_from_backend_cached(store.clone(), "known", incarnation)
            .await
            .expect("classify demoted completeness claim"),
        BucketUsageStatus::Unavailable(BucketUsageUnavailableReason::InvalidSnapshotProof)
    );
    let raw = load_data_usage_from_backend_cached(store.clone())
        .await
        .expect("read normalized usage");
    assert!(!raw.usage_snapshot_complete);
    assert!(raw.buckets_usage.is_empty());

    info.scanner_cycle = Some(4);
    info.buckets_count -= 1;
    info.usage_snapshot_observational = true;
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&info).expect("encode observational sample in primary slot"),
    )
    .await
    .expect("persist wrong snapshot kind");
    invalidate_data_usage_snapshot_cache().await;
    assert_eq!(
        load_bucket_usage_certification_from_backend_cached(store.clone(), "known", incarnation)
            .await
            .expect("classify wrong snapshot kind"),
        BucketUsageStatus::Unavailable(BucketUsageUnavailableReason::InvalidSnapshotProof)
    );
    invalidate_data_usage_snapshot_cache().await;
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
