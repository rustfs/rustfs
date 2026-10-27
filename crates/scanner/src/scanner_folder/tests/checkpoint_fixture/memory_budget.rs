// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

const OBJECTS_PER_PREFIX: usize = 16;
const FROZEN_FRONTIER: &str = "bucket/prefix-0000/0000";

fn cache_shape(cache: &DataUsageCache, wire_bytes: usize) -> serde_json::Value {
    let mut key_capacity_bytes = 0;
    let mut child_links = 0;
    let mut child_set_capacity = 0;
    let mut child_key_capacity_bytes = 0;
    for (key, entry) in &cache.cache {
        key_capacity_bytes += key.capacity();
        child_links += entry.children.len();
        child_set_capacity += entry.children.capacity();
        child_key_capacity_bytes += entry.children.iter().map(String::capacity).sum::<usize>();
    }
    // The canonical histograms own eleven and seven u64 counters. Map control
    // bytes, allocator overhead, optional statistics and transient work are excluded.
    let histogram_payload_lower_bound_bytes = cache.cache.len() * (11 + 7) * std::mem::size_of::<u64>();
    let map_slot_size = std::mem::size_of::<(String, DataUsageEntry)>();
    let retained_lower_bound_bytes = cache.cache.capacity() * map_slot_size
        + key_capacity_bytes
        + child_set_capacity * std::mem::size_of::<String>()
        + child_key_capacity_bytes
        + histogram_payload_lower_bound_bytes;
    serde_json::json!({
        "entries": cache.cache.len(),
        "entry_capacity": cache.cache.capacity(),
        "entry_size_bytes": std::mem::size_of::<DataUsageEntry>(),
        "map_slot_size_bytes": map_slot_size,
        "map_key_capacity_bytes": key_capacity_bytes,
        "child_links": child_links,
        "child_set_capacity": child_set_capacity,
        "child_key_capacity_bytes": child_key_capacity_bytes,
        "histogram_payload_lower_bound_bytes": histogram_payload_lower_bound_bytes,
        "retained_lower_bound_bytes": retained_lower_bound_bytes,
        "wire_bytes": wire_bytes,
    })
}

async fn characterize_memory_budget(prefixes: usize, coverage_gap: bool) {
    let (mut scanner, root) = build_test_scanner().await;
    let _cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for prefix in 0..prefixes {
        for object in 0..OBJECTS_PER_PREFIX {
            write_checkpoint_object(&root, &format!("prefix-{prefix:04}/{object:04}"), &[(None, 1)]).await;
        }
    }
    let identity = crate::DataUsageScanIdentity {
        tier_registry_generation: crate::runtime_tier_registry_for_cycle(11, 7).await.generation,
        ..bound_checkpoint().1
    };
    scanner
        .old_cache
        .prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
    scanner.new_cache.info = scanner.old_cache.info.clone();
    scanner.is_erasure_mode = true;
    scanner.skip_heal.store(true, Ordering::SeqCst);
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    // Isolate the already-established gap state from filesystem fault injection.
    // Every object below is healthy; this control does not reproduce the fault
    // or durable debt that caused a real scan's coverage gap.
    scanner.coverage_gap = coverage_gap;
    if coverage_gap {
        scanner.coverage_frontier = Some(FROZEN_FRONTIER.to_string());
    }
    scanner
        .scan_folder(
            CancellationToken::new(),
            CachedFolder {
                name: "bucket".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut DataUsageEntry::default(),
        )
        .await
        .expect("walk the real wide-root metadata fixture");

    let expected_objects = prefixes * OBJECTS_PER_PREFIX;
    let totals = scanner
        .new_cache
        .checked_flatten_complete_scope("bucket")
        .expect("the measured cache must be connected and contain no orphan entries");
    assert_eq!((totals.objects, totals.size, totals.versions), (expected_objects, expected_objects, 0));
    assert_eq!(totals.failed_objects, 0, "the gap control must not fabricate object failure debt");
    assert!(
        scanner.usage_updates.is_none(),
        "production-style scans must not retain a progress mirror"
    );
    let bucket = scanner.new_cache.find("bucket").expect("retained root inventory");
    assert!(!bucket.compacted, "these root fanouts preserve prefix statistics");
    assert_eq!(bucket.children.len(), prefixes);
    if coverage_gap {
        assert_eq!(scanner.coverage_frontier.as_deref(), Some(FROZEN_FRONTIER));
        assert!(
            scanner.new_cache.find(FROZEN_FRONTIER).is_some(),
            "the frozen endpoint must remain addressable"
        );
    } else {
        for prefix in 0..prefixes {
            let path = format!("bucket/prefix-{prefix:04}");
            let entry = scanner.new_cache.find(&path).expect("completed root prefix");
            assert!(entry.compacted);
            assert!(entry.children.is_empty());
            assert_eq!((entry.objects, entry.size), (OBJECTS_PER_PREFIX, OBJECTS_PER_PREFIX));
        }
        assert_eq!(scanner.new_cache.cache.len(), prefixes + 1, "healthy completed leaves must be released");
    }

    scanner
        .new_cache
        .seal_scan_frontier(scanner.coverage_frontier.as_deref())
        .expect("seal the measured coverage endpoint");
    assert!(scanner.new_cache.validated_scan_frontier().is_some());
    let encoded = scanner
        .new_cache
        .marshal_msg()
        .expect("encode the measured cache with the production codec");
    let restored = DataUsageCache::unmarshal(&encoded).expect("reload the measured coverage graph");
    assert_eq!(restored.validated_scan_frontier(), scanner.new_cache.validated_scan_frontier());
    assert_eq!(restored.info.scan_coverage_receipt, scanner.new_cache.info.scan_coverage_receipt);
    let restored_totals = restored
        .checked_flatten_complete_scope("bucket")
        .expect("reloaded accounting graph");
    assert_eq!((restored_totals.objects, restored_totals.size), (expected_objects, expected_objects));
    if coverage_gap {
        assert_eq!(restored.validated_scan_frontier(), Some(FROZEN_FRONTIER));
    }

    println!(
        "SCANNER_MEMORY_BUDGET {}",
        serde_json::json!({
            "schema": 1,
            "measurement_point": "folder_walk_return_before_bucket_finalization",
            "scenario": if coverage_gap { "seeded_coverage_gap" } else { "healthy_forward_sweep" },
            "synthetic_gap_control": coverage_gap,
            "prefixes": prefixes,
            "objects_per_prefix": OBJECTS_PER_PREFIX,
            "objects": expected_objects,
            "object_bytes": expected_objects,
            "validated_frontier": restored.validated_scan_frontier(),
            "cache": cache_shape(&scanner.new_cache, encoded.len()),
        })
    );
}

#[tokio::test]
#[serial]
async fn memory_budget_characterizes_wide_root_and_seeded_coverage_gap() {
    let prefixes: &[usize] = match std::env::var("RUSTFS_SCANNER_MEMORY_BUDGET_PROFILE") {
        Err(std::env::VarError::NotPresent) => &[64],
        Ok(value) if value == "1" => &[64, 128, 256],
        _ => panic!("RUSTFS_SCANNER_MEMORY_BUDGET_PROFILE must be absent or 1"),
    };
    for &prefixes in prefixes {
        assert!(prefixes * (OBJECTS_PER_PREFIX + 1) < DATA_SCANNER_COMPACT_AT_CHILDREN);
        for coverage_gap in [false, true] {
            characterize_memory_budget(prefixes, coverage_gap).await;
        }
    }
}
