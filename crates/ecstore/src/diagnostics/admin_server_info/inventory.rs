// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::ScannerBucketListing;
use rustfs_data_usage::DataUsageInfo;
use rustfs_madmin::ClusterInventory;
use std::collections::HashSet;

use crate::data_usage::current_admin_bucket_incarnations;
use crate::store::ECStore;
use std::collections::HashMap;
use std::sync::Arc;

pub(super) async fn collect(
    store: &Arc<ECStore>,
    namespace: Option<&ScannerBucketListing>,
    snapshot: Option<&DataUsageInfo>,
    deadline: tokio::time::Instant,
) -> ClusterInventory {
    let mut current_ids = HashMap::new();
    if let (Some(namespace), Some(snapshot)) = (namespace.filter(|listing| listing.topology_complete), snapshot) {
        let names = namespace
            .buckets
            .iter()
            .filter(|bucket| {
                !bucket.name.starts_with('.')
                    && snapshot
                        .buckets_usage
                        .get(&bucket.name)
                        .is_some_and(|usage| usage.bucket_incarnation.is_some())
            })
            .map(|bucket| bucket.name.clone())
            .collect();
        current_ids = current_admin_bucket_incarnations(store.clone(), names, deadline).await;
    }
    summarize(namespace, snapshot, &current_ids)
}

pub(super) async fn bounded_namespace(
    deadline: tokio::time::Instant,
    read: impl Future<Output = crate::error::Result<ScannerBucketListing>>,
) -> crate::error::Result<ScannerBucketListing> {
    tokio::time::timeout_at(deadline, read)
        .await
        .map_err(|_| crate::error::Error::other("inventory namespace read timed out"))?
}

pub(super) async fn namespace_or_fallback(
    previous: Option<crate::error::Result<ScannerBucketListing>>,
    read: impl Future<Output = crate::error::Result<ScannerBucketListing>>,
) -> crate::error::Result<ScannerBucketListing> {
    // A failed or timed-out optional listing is already an observation.
    // Retrying it without the deadline would stall the independent health response.
    match previous {
        Some(result) => result,
        None => read.await,
    }
}

pub(super) fn summarize(
    namespace: Option<&ScannerBucketListing>,
    snapshot: Option<&DataUsageInfo>,
    current_ids: &HashMap<String, String>,
) -> ClusterInventory {
    // An admin projection is a validated display-only view produced by the loader.
    let snapshot = snapshot.filter(|info| {
        info.usage_snapshot_admin_projection || info.is_complete_bucket_usage_snapshot() || info.is_valid_partial_snapshot()
    });
    let mut inventory = ClusterInventory {
        snapshot_at: snapshot
            .and_then(|info| info.last_update)
            .and_then(|time| time.try_into().ok()),
        ..Default::default()
    };
    let Some(namespace) = namespace.filter(|listing| listing.topology_complete) else {
        return inventory;
    };
    let names: HashSet<_> = namespace
        .buckets
        .iter()
        .map(|bucket| bucket.name.as_str())
        .filter(|name| !name.starts_with('.'))
        .collect();
    inventory.current_buckets = u64::try_from(names.len()).ok();
    let Some(snapshot) = snapshot else {
        return inventory;
    };

    let mut measured = 0_u64;
    let mut objects = Some(0_u64);
    let mut bytes = Some(0_u64);
    for &name in &names {
        let Some(usage) = snapshot.buckets_usage.get(name) else { continue };
        if usage
            .bucket_incarnation
            .as_ref()
            .is_none_or(|id| current_ids.get(name) != Some(id))
        {
            continue;
        }
        measured += 1;
        objects = objects.and_then(|total| total.checked_add(usage.objects_count));
        bytes = bytes.and_then(|total| total.checked_add(usage.size));
    }
    inventory.measured_buckets = Some(measured);
    if measured != 0 || names.is_empty() {
        inventory.measured_objects = objects;
        inventory.measured_bytes = bytes;
    }
    inventory.converged = if objects.is_none() || bytes.is_none() {
        None
    } else if inventory.current_buckets != Some(measured)
        || snapshot.buckets_usage.len() != names.len()
        || snapshot.usage_snapshot_pending_bucket_count > 0
        || !snapshot.usage_snapshot_pending_buckets.is_empty()
        || !snapshot.is_complete_bucket_usage_snapshot()
    {
        Some(false)
    } else {
        snapshot.usage_snapshot_converged
    };
    inventory
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_data_usage::BucketUsageInfo;
    use std::time::{Duration, SystemTime};

    fn namespace(names: &[&str]) -> ScannerBucketListing {
        ScannerBucketListing {
            buckets: names
                .iter()
                .map(|name| crate::storage_api_contracts::bucket::BucketInfo {
                    name: name.to_string(),
                    ..Default::default()
                })
                .collect(),
            set_buckets: vec![],
            topology_complete: true,
        }
    }

    fn snapshot(names: &[(&str, u64)]) -> DataUsageInfo {
        DataUsageInfo {
            last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
            usage_snapshot_complete: true,
            usage_snapshot_converged: Some(true),
            buckets_count: u64::try_from(names.len()).unwrap(),
            buckets_usage: names
                .iter()
                .map(|&(name, count)| {
                    (
                        name.to_owned(),
                        BucketUsageInfo {
                            objects_count: count,
                            size: count,
                            bucket_incarnation: Some("11111111-1111-4111-8111-111111111111".into()),
                            ..Default::default()
                        },
                    )
                })
                .collect(),
            ..Default::default()
        }
    }

    fn view(namespace: Option<&ScannerBucketListing>, snapshot: Option<&DataUsageInfo>) -> ClusterInventory {
        let ids = namespace
            .into_iter()
            .flat_map(|ns| &ns.buckets)
            .map(|bucket| (bucket.name.clone(), "11111111-1111-4111-8111-111111111111".to_string()))
            .collect();
        summarize(namespace, snapshot, &ids)
    }

    #[tokio::test(start_paused = true)]
    async fn inventory_namespace_timeout_is_reused_by_the_legacy_fallback() {
        use tokio::time::{Duration, Instant};
        let deadline = Instant::now() + Duration::from_secs(2);
        let result = bounded_namespace(deadline, std::future::pending()).await;
        assert!(result.is_err());
        assert_eq!(Instant::now(), deadline);
        let result = namespace_or_fallback(Some(result), async { panic!("must not retry a failed namespace read") }).await;
        assert!(result.is_err());
        let inventory = view(result.as_ref().ok(), None);
        assert_eq!(inventory.current_buckets, None);
        assert_eq!(inventory.measured_objects, None);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn inventory_excludes_stale_bucket_incarnations_from_a_stored_snapshot() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::config::com::save_config;
        use crate::data_usage::{DATA_USAGE_OBJ_NAME_PATH, invalidate_data_usage_snapshot_cache};
        use crate::storage_api_contracts::bucket::{BucketOperations as _, BucketOptions, MakeBucketOptions};
        let (_dirs, store) = isolated_store_over_temp_disks().await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        for bucket in ["unchanged", "recreated", "new"] {
            store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
        }
        let mut scanned = snapshot(&[("unchanged", 4), ("recreated", 99), ("deleted", 6)]);
        let current = store.bucket_incarnation_id_from_disk("unchanged").await.unwrap();
        scanned.buckets_usage.get_mut("unchanged").unwrap().bucket_incarnation = Some(current.to_string());
        scanned.usage_snapshot_converged = None;
        scanned.calculate_totals();
        save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), serde_json::to_vec(&scanned).unwrap())
            .await
            .unwrap();
        invalidate_data_usage_snapshot_cache().await;
        let listing = store
            .list_bucket_for_scanner(&BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let loaded = super::super::load_server_info_usage_snapshot(store.clone()).await.unwrap();
        assert_eq!(loaded.usage_snapshot_converged, Some(true));
        let result = collect(
            &store,
            Some(&listing),
            Some(&loaded),
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await;
        assert_eq!(result.current_buckets, Some(3));
        assert_eq!(result.measured_buckets, Some(1));
        assert_eq!(result.measured_objects, Some(4));
        assert_eq!(result.converged, Some(false));
        // A complete canonical snapshot from before the convergence field was
        // added inherits the admin loader's existing convergence interpretation.
        let mut legacy = snapshot(&[("unchanged", 4), ("recreated", 0), ("new", 0)]);
        legacy.usage_snapshot_converged = None;
        for (name, usage) in &mut legacy.buckets_usage {
            usage.bucket_incarnation = Some(store.bucket_incarnation_id_from_disk(name).await.unwrap().to_string());
        }
        legacy.calculate_totals();
        save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), serde_json::to_vec(&legacy).unwrap())
            .await
            .unwrap();
        invalidate_data_usage_snapshot_cache().await;
        let loaded = super::super::load_server_info_usage_snapshot(store.clone()).await.unwrap();
        let result = collect(
            &store,
            Some(&listing),
            Some(&loaded),
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await;
        assert_eq!(result.measured_objects, Some(4));
        assert_eq!(result.converged, Some(true));

        invalidate_data_usage_snapshot_cache().await;
    }

    #[test]
    fn inventory_reports_only_the_intersection_of_current_and_measured_buckets() {
        let scope = namespace(&["existing", "new", ".rustfs.sys"]);
        let before = snapshot(&[("existing", 300_000), ("deleted", 7)]);
        let result = view(Some(&scope), Some(&before));
        assert_eq!(result.current_buckets, Some(2));
        assert_eq!(result.measured_buckets, Some(1));
        assert_eq!(result.measured_objects, Some(300_000));
        assert_eq!(result.measured_bytes, Some(300_000));
        assert_eq!(result.converged, Some(false));
        assert_eq!(result.snapshot_at.unwrap().as_second(), 1);
    }

    #[test]
    fn inventory_distinguishes_unavailable_and_confirmed_empty_measurements() {
        let empty = namespace(&[]);
        let missing = view(Some(&empty), Some(&DataUsageInfo::default()));
        assert_eq!(missing.current_buckets, Some(0));
        assert_eq!(missing.measured_objects, None);
        assert_eq!(missing.measured_buckets, None);
        let available = view(Some(&empty), Some(&snapshot(&[])));
        assert_eq!(available.measured_objects, Some(0));
        assert_eq!(available.converged, Some(true));
        let scope = namespace(&["new"]);
        let no_overlap = view(Some(&scope), Some(&snapshot(&[("deleted", 7)])));
        assert_eq!(no_overlap.measured_buckets, Some(0));
        assert_eq!(no_overlap.measured_objects, None);
        let known_empty = view(Some(&scope), Some(&snapshot(&[("new", 0)])));
        assert_eq!(known_empty.measured_objects, Some(0));
        assert_eq!(known_empty.measured_buckets, Some(1));
    }

    #[test]
    fn inventory_keeps_the_recorded_snapshot_convergence_and_age() {
        let scope = namespace(&["a"]);
        let mut scanned = snapshot(&[("a", 12)]);
        scanned.usage_snapshot_converged = None;
        assert_eq!(view(Some(&scope), Some(&scanned)).converged, None);
        scanned.usage_snapshot_converged = Some(true);
        scanned.usage_snapshot_pending_buckets = vec!["a".into()];
        assert_eq!(view(Some(&scope), Some(&scanned)).converged, Some(false));
        scanned.usage_snapshot_pending_buckets.clear();
        scanned.usage_snapshot_converged = Some(false);
        scanned.usage_snapshot_complete = false;
        scanned.usage_snapshot_admin_projection = true;
        let result = view(Some(&scope), Some(&scanned));
        assert_eq!(result.measured_objects, Some(12));
        assert_eq!(result.converged, Some(false));
        assert_eq!(result.snapshot_at.unwrap().as_second(), 1);
    }

    #[test]
    fn inventory_requires_complete_namespace_and_checks_each_sum() {
        let mut scope = namespace(&["a", "b"]);
        let mut scanned = snapshot(&[("a", u64::MAX), ("b", 1)]);
        scanned.buckets_usage.get_mut("a").unwrap().size = 1;
        scope.topology_complete = false;
        let missing = view(Some(&scope), Some(&scanned));
        assert_eq!(missing.current_buckets, None);
        assert_eq!(missing.measured_buckets, None);
        assert_eq!(missing.converged, None);
        scope.topology_complete = true;
        let overflow = view(Some(&scope), Some(&scanned));
        assert_eq!(overflow.measured_buckets, Some(2));
        assert_eq!(overflow.measured_objects, None);
        assert_eq!(overflow.measured_bytes, Some(2));
        assert_eq!(overflow.converged, None);
    }

    #[test]
    fn inventory_checks_even_cached_canonical_incarnations_on_main() {
        let scope = namespace(&["recreated", "unreadable", "legacy"]);
        let mut scanned = snapshot(&[("recreated", 5), ("unreadable", 6), ("legacy", 7)]);
        scanned.buckets_usage.get_mut("legacy").unwrap().bucket_incarnation = None;
        let ids = HashMap::from([
            ("recreated".to_string(), "22222222-2222-4222-8222-222222222222".to_string()),
            ("legacy".to_string(), "33333333-3333-4333-8333-333333333333".to_string()),
        ]);
        let result = summarize(Some(&scope), Some(&scanned), &ids);
        assert_eq!(result.current_buckets, Some(3));
        assert_eq!(result.measured_buckets, Some(0));
        assert_eq!(result.measured_objects, None);
    }
}
