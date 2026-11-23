// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

fn two_source_checkpoint(v2_receipt: bool) -> (DataUsageCache, HashSet<String>) {
    let mut cache = DataUsageCache::default();
    let identity = DataUsageScanIdentity {
        version: 1,
        bucket_incarnation: uuid::Uuid::from_u128(1),
        set_layout: DataUsageScanPlanDigest([2; 32]),
        publication_epoch: 1,
        tier_registry_generation: 3,
        scan_mode: HealScanMode::Normal,
    };
    let plan = DataUsageScanPlanDigest([4; 32]);
    cache.prepare_bucket_checkpoint("bucket", 1, 1, DataUsageCacheSource::new(0, 0), plan, identity);
    let endpoints = HashSet::from(["a".into(), "b".into()]);
    for (source, size) in [("disk-a|a", 7), ("disk-b|b", 11)] {
        cache.activate_directory_source(source.into(), &endpoints);
        cache.replace("bucket", "", DataUsageEntry::default());
        cache.replace(
            "bucket/object",
            "bucket",
            DataUsageEntry {
                objects: 1,
                versions: 2,
                size,
                ..Default::default()
            },
        );
        cache.info.scan_resume_after = Some("bucket/object".into());
        cache.info.scan_checkpoint = Some(DataUsageScanCheckpoint::new(
            "bucket/object".into(),
            DataUsageScanCheckpointReason::Objects,
        ));
        cache
            .seal_scan_frontier(Some("bucket/object"))
            .expect("seal measured source coverage");
        if v2_receipt {
            let digest = cache
                .v2_coverage_prefix_digest("bucket/object")
                .expect("encode v2 source coverage");
            cache.info.scan_coverage_receipt.as_mut().expect("sealed receipt").digest = digest;
        }
    }
    let next_identity = DataUsageScanIdentity {
        publication_epoch: 9,
        ..identity
    };
    assert_eq!(
        cache.prepare_bucket_checkpoint("bucket", 2, 2, DataUsageCacheSource::new(0, 0), plan, next_identity),
        DataUsageCachePrepareOutcome::Reused
    );
    (
        DataUsageCache::unmarshal(&cache.marshal_msg().expect("persist source trees")).expect("cold source tree reload"),
        endpoints,
    )
}

#[test]
fn dormant_source_restores_and_resigns_coverage_after_epoch_change() {
    for v2_receipt in [false, true] {
        let (mut cache, endpoints) = two_source_checkpoint(v2_receipt);
        cache.activate_directory_source("disk-a|a".into(), &endpoints);
        assert_eq!(cache.validated_scan_frontier(), Some("bucket/object"));
        let measured = cache.checked_flatten("bucket").expect("restored connected source tree");
        assert_eq!((measured.objects, measured.versions, measured.size), (1, 2, 7));
        assert_eq!(cache.info.scan_identity.expect("current identity").publication_epoch, 9);
        assert!(!cache.info.snapshot_complete, "a restored prefix is not a certified bucket");
        cache.activate_directory_source("disk-b|b".into(), &endpoints);
        assert_eq!(cache.validated_scan_frontier(), Some("bucket/object"));
        assert_eq!(cache.checked_flatten("bucket").expect("B's own coverage").size, 11);
    }
}

#[test]
fn dormant_source_rejects_corrupt_receipt_after_epoch_change() {
    for field in 0..3 {
        let (mut cache, endpoints) = two_source_checkpoint(false);
        let tree = Arc::make_mut(cache.info.scan_source_trees.get_mut("disk-a|a").expect("dormant source"));
        match field {
            0 => tree.coverage.as_mut().expect("source receipt").digest[0] ^= 1,
            1 => {
                tree.entries
                    .get_mut(&hash_path("bucket/object").0)
                    .expect("covered entry")
                    .size += 1
            }
            2 => tree.checkpoint.as_mut().expect("source checkpoint").resume_after = "bucket/another".into(),
            _ => unreachable!(),
        }
        cache.activate_directory_source("disk-a|a".into(), &endpoints);
        assert!(cache.cache.is_empty(), "corrupt source field {field} must be rejected before re-signing");
        assert!(!cache.has_validated_scan_position());
        assert!(!cache.info.snapshot_complete);
    }
}

#[test]
fn dormant_source_rejects_changed_bucket_scope() {
    for field in 0..5 {
        let (mut cache, endpoints) = two_source_checkpoint(false);
        let tree = Arc::make_mut(cache.info.scan_source_trees.get_mut("disk-a|a").expect("dormant source"));
        match field {
            0 => tree.identity.bucket_incarnation = uuid::Uuid::from_u128(99),
            1 => tree.identity.set_layout = DataUsageScanPlanDigest([99; 32]),
            2 => tree.identity.tier_registry_generation += 1,
            3 => tree.identity.scan_mode = HealScanMode::Deep,
            4 => tree.identity.version += 1,
            _ => unreachable!(),
        }
        cache.activate_directory_source("disk-a|a".into(), &endpoints);
        assert!(
            cache.cache.is_empty(),
            "changed scope field {field} cannot import another source's accounting"
        );
        assert!(!cache.has_validated_scan_position());
    }
}
