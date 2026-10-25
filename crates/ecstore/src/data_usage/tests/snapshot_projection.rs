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
        info: Some(info),
        loaded_at,
        degraded_baseline: HashMap::new(),
    });
    let stored = cache
        .as_ref()
        .expect("cached snapshot")
        .info
        .as_ref()
        .expect("known snapshot");
    let ttl = Duration::from_secs(30);

    for (bucket, expected) in [("known", Some(42)), ("empty", Some(0)), ("missing", None)] {
        let value = fresh_cached_data_usage_snapshot(&cache, loaded_at, ttl, |info| {
            assert!(std::ptr::eq(info, stored), "projection must borrow the cached snapshot");
            info.buckets_usage.get(bucket).map(|usage| usage.size)
        })
        .expect("fresh snapshot")
        .expect("successful snapshot");
        let owned = fresh_cached_data_usage_snapshot(&cache, loaded_at, ttl, DataUsageInfo::clone)
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
    let never_project = |_: &DataUsageInfo| -> u64 { panic!("unusable snapshots must not be projected") };
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
        info: Some(data_usage_info_for_test("known", 1, 42, SystemTime::UNIX_EPOCH)),
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
            observed_unavailable: false,
        }),
        tokio::time::Instant::now(),
        1,
        1,
        |info| {
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
        .info
        .as_ref()
        .expect("published snapshot");
    assert_eq!(
        cached.buckets_usage.get_key_value("known").expect("cached bucket").0.as_ptr(),
        original_key
    );
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
        let owned = load_data_usage_from_backend_cached(store.clone())
            .await
            .expect("owned cached snapshot");
        assert_eq!(expected, owned.buckets_usage.get(bucket).map(|usage| usage.size));
    }
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
