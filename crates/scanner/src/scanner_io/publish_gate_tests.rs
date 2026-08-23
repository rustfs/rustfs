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
use rustfs_data_usage::{ReplicationAllStats, ReplicationTargetUsage};

const TEST_PLAN_DIGEST: DataUsageScanPlanDigest = DataUsageScanPlanDigest([7; 32]);

#[test]
fn should_publish_completed_snapshot_requires_full_clean_cycle() {
    assert!(should_publish_completed_snapshot(3, 3, false, false));
    assert!(!should_publish_completed_snapshot(2, 3, false, false));
    assert!(!should_publish_completed_snapshot(3, 3, true, false));
    assert!(!should_publish_completed_snapshot(3, 3, false, true));
    assert!(
        should_publish_completed_snapshot(0, 0, false, false),
        "a completed empty namespace is an authoritative zero snapshot"
    );
}

fn incomplete_scope_cache(source: DataUsageCacheSource) -> DataUsageCache {
    DataUsageCache {
        info: DataUsageCacheInfo {
            name: DATA_USAGE_ROOT.to_string(),
            source: Some(source),
            snapshot_complete: false,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[test]
fn incomplete_scan_scope_requires_every_expected_set_marker() {
    let first_source = DataUsageCacheSource::new(0, 0);
    let second_source = DataUsageCacheSource::new(1, 0);
    let expected_sources = HashSet::from([first_source, second_source]);
    let first = incomplete_scope_cache(first_source);
    let second = incomplete_scope_cache(second_source);

    assert!(scanner_results_match_scan_scope(&[first.clone(), second], &expected_sources));
    assert!(!scanner_results_form_complete_snapshot(
        &[first.clone(), incomplete_scope_cache(second_source)],
        &expected_sources
    ));
    assert!(!scanner_results_match_scan_scope(
        &[first.clone(), DataUsageCache::default()],
        &expected_sources
    ));
    assert!(!scanner_results_match_scan_scope(
        &[first.clone(), incomplete_scope_cache(first_source)],
        &expected_sources
    ));

    let mut mismatched_plan = incomplete_scope_cache(second_source);
    mismatched_plan.info.scan_plan_digest = Some(DataUsageScanPlanDigest([8; 32]));
    assert!(!scanner_results_match_scan_scope(&[first, mismatched_plan], &expected_sources));
}

fn completed_root_cache(bucket: &str, objects: usize, update_secs: u64, source: DataUsageCacheSource) -> DataUsageCache {
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: DATA_USAGE_ROOT.to_string(),
            last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(update_secs)),
            source: Some(source),
            snapshot_complete: true,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        bucket,
        DATA_USAGE_ROOT,
        DataUsageEntry {
            objects,
            size: objects.saturating_mul(10),
            ..Default::default()
        },
    );
    cache
}

fn completed_data_usage_info_for_test(
    results: &[DataUsageCache],
    all_buckets: &[String],
    budget_elapsed: bool,
    cancelled: bool,
) -> Option<(DataUsageInfo, SystemTime)> {
    let expected_sources = results.iter().filter_map(|result| result.info.source).collect::<HashSet<_>>();
    completed_data_usage_info(results, &expected_sources, all_buckets, true, budget_elapsed, cancelled)
}

fn lkg_root_cache(bucket: &str, objects: usize, source: DataUsageCacheSource) -> DataUsageCache {
    let mut cache = completed_root_cache(bucket, objects, 10, source);
    cache.info.snapshot_complete = false;
    cache.info.next_cycle = 8;
    cache.info.leader_epoch = 3;
    cache.info.lkg_snapshot_complete = true;
    cache.info.lkg_next_cycle = Some(7);
    cache.info.lkg_last_update = cache.info.last_update;
    cache.info.lkg_leader_epoch = Some(3);
    cache.info.lkg_scan_plan_digest = Some(TEST_PLAN_DIGEST);
    cache
}

#[test]
fn partial_usage_is_observational_not_authoritative_for_quota() {
    let all_buckets = vec!["bucket".to_string()];
    let current_source = DataUsageCacheSource::new(0, 0);
    let stalled_source = DataUsageCacheSource::new(1, 0);
    let mut current = completed_root_cache("bucket", 2, 20, current_source);
    current.info.next_cycle = 8;
    current.info.leader_epoch = 3;
    let stalled = lkg_root_cache("bucket", 1, stalled_source);
    let expected = HashSet::from([current_source, stalled_source]);

    assert!(
        completed_data_usage_info(&[current.clone(), stalled.clone()], &expected, &all_buckets, true, false, false).is_none()
    );
    let (observed, _) = observational_data_usage_info(&[current, stalled], &expected, &all_buckets, TEST_PLAN_DIGEST, 8, 3)
        .expect("a completed set should produce an observational view");
    assert!(observed.usage_snapshot_partial);
    assert!(!observed.usage_snapshot_complete);
    assert_eq!(observed.usage_snapshot_converged, Some(false));
    assert_eq!(observed.usage_snapshot_set_states.len(), 2);
}

#[test]
fn lkg_scope_does_not_count_as_current_cycle_completion() {
    let source = DataUsageCacheSource::new(0, 0);
    let mut lkg = lkg_root_cache("bucket", 1, source);
    lkg.info.last_update = None;
    let expected = HashSet::from([source]);
    assert!(!scanner_results_form_complete_snapshot(&[lkg], &expected));
}

#[test]
fn stale_quota_uses_complete_baseline_plus_positive_deltas() {
    let all_buckets = vec!["bucket".to_string()];
    let source = DataUsageCacheSource::new(0, 0);
    let mut current = completed_root_cache("bucket", 3, 20, source);
    current.info.next_cycle = 8;
    current.info.leader_epoch = 3;
    let expected = HashSet::from([source]);
    let (observed, _) = observational_data_usage_info(&[current], &expected, &all_buckets, TEST_PLAN_DIGEST, 8, 3)
        .expect("complete set data is a valid observational baseline");
    assert_eq!(observed.objects_total_size, 30);
    assert_eq!(observed.usage_snapshot_set_states[0].complete, true);
}

#[test]
fn negative_delta_waits_for_set_reconciliation() {
    let all_buckets = vec!["bucket".to_string()];
    let source = DataUsageCacheSource::new(0, 0);
    let mut stalled = lkg_root_cache("bucket", 4, source);
    stalled.info.lkg_scan_plan_digest = Some(DataUsageScanPlanDigest([9; 32]));
    let expected = HashSet::from([source]);
    assert!(observational_data_usage_info(&[stalled], &expected, &all_buckets, TEST_PLAN_DIGEST, 8, 3).is_none());
}

#[test]
fn set_membership_add_remove_uses_generation_and_tombstone() {
    let state = DataUsageSnapshotSetState {
        pool_index: 1,
        set_index: 2,
        scanner_cycle: Some(9),
        scanner_epoch: Some(4),
        scan_plan_digest: Some(TEST_PLAN_DIGEST.0),
        complete: false,
        tombstone: true,
    };
    let encoded = serde_json::to_vec(&state).expect("set state should serialize");
    let decoded: DataUsageSnapshotSetState = serde_json::from_slice(&encoded).expect("set state should deserialize");
    assert_eq!(decoded, state);

    let snapshot = DataUsageInfo {
        last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(10)),
        scanner_cycle: Some(9),
        scanner_epoch: Some(4),
        buckets_count: 0,
        usage_snapshot_converged: Some(false),
        usage_snapshot_partial: true,
        usage_snapshot_set_states: vec![
            DataUsageSnapshotSetState {
                pool_index: 0,
                set_index: 0,
                scanner_cycle: Some(9),
                scanner_epoch: Some(4),
                scan_plan_digest: Some(TEST_PLAN_DIGEST.0),
                complete: true,
                tombstone: false,
            },
            state,
        ],
        ..Default::default()
    };
    assert!(snapshot.is_valid_partial_snapshot());
}

#[test]
fn old_set_completion_cannot_overwrite_new_aggregate() {
    let all_buckets = vec!["bucket".to_string()];
    let source = DataUsageCacheSource::new(0, 0);
    let mut old = completed_root_cache("bucket", 1, 20, source);
    old.info.next_cycle = 7;
    old.info.leader_epoch = 2;
    let expected = HashSet::from([source]);
    assert!(observational_data_usage_info(&[old], &expected, &all_buckets, TEST_PLAN_DIGEST, 8, 3).is_none());
}

#[test]
fn usage_aggregate_survives_restart_and_leader_failover() {
    let all_buckets = vec!["bucket".to_string()];
    let source = DataUsageCacheSource::new(0, 0);
    let mut lkg = lkg_root_cache("bucket", 5, source);
    lkg.info.lkg_leader_epoch = Some(4);
    lkg.info.lkg_next_cycle = Some(9);
    let expected = HashSet::from([source]);
    let (observed, _) = observational_data_usage_info(&[lkg], &expected, &all_buckets, TEST_PLAN_DIGEST, 10, 5)
        .expect("compatible LKG should survive a leader change");
    assert_eq!(observed.usage_snapshot_set_states[0].scanner_epoch, Some(4));
    assert_eq!(observed.objects_total_size, 50);
}

#[test]
fn usage_aggregate_cost_is_linear_in_set_count() {
    let all_buckets = vec!["bucket".to_string()];
    let mut results = Vec::new();
    let mut expected = HashSet::new();
    for index in 0..32 {
        let source = DataUsageCacheSource::new(index, 0);
        expected.insert(source);
        let mut cache = completed_root_cache("bucket", 1, 20, source);
        cache.info.next_cycle = 8;
        cache.info.leader_epoch = 3;
        results.push(cache);
    }
    let (observed, _) = observational_data_usage_info(&results, &expected, &all_buckets, TEST_PLAN_DIGEST, 8, 3)
        .expect("all set snapshots should aggregate");
    assert_eq!(observed.objects_total_count, 32);
    let reversed = results.iter().rev().cloned().collect::<Vec<_>>();
    let (reversed_observed, _) = observational_data_usage_info(&reversed, &expected, &all_buckets, TEST_PLAN_DIGEST, 8, 3)
        .expect("reordered set snapshots should aggregate");
    assert_eq!(observed.usage_snapshot_set_states, reversed_observed.usage_snapshot_set_states);
}

#[test]
fn completed_data_usage_info_publishes_tier_stats_across_sets() {
    let all_buckets = vec!["bucket-a".to_string(), "bucket-b".to_string()];
    let warm = |total_size, num_versions, num_objects| {
        HashMap::from([(
            "WARM".to_string(),
            TierStats {
                total_size,
                num_versions,
                num_objects,
            },
        )])
    };

    let mut first_set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    let mut tiered = DataUsageEntry::default();
    tiered.add_tier_sizes(&warm(100, 2, 1));
    first_set.replace("bucket-b", DATA_USAGE_ROOT, tiered);

    let mut second_set = completed_root_cache("bucket-b", 2, 20, DataUsageCacheSource::new(1, 0));
    let mut tiered = DataUsageEntry::default();
    tiered.add_tier_sizes(&warm(50, 1, 1));
    second_set.replace("bucket-a", DATA_USAGE_ROOT, tiered);

    let (data_usage_info, _) = completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false)
        .expect("completed sets should publish a snapshot");

    assert_eq!(
        data_usage_info
            .tier_stats
            .expect("tier usage should reach the snapshot")
            .tiers["WARM"],
        TierStats {
            total_size: 150,
            num_versions: 3,
            num_objects: 2,
        }
    );
}

#[test]
fn completed_data_usage_info_omits_tier_stats_without_tiered_objects() {
    let all_buckets = vec!["bucket-a".to_string()];
    let set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));

    let (data_usage_info, _) =
        completed_data_usage_info_for_test(&[set], &all_buckets, false, false).expect("completed set should publish a snapshot");

    assert!(data_usage_info.tier_stats.is_none());
}

#[test]
fn completed_data_usage_info_requires_every_set_before_publish() {
    let all_buckets = vec!["bucket-a".to_string(), "bucket-b".to_string(), "bucket-empty".to_string()];
    let mut first_set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    first_set.replace("bucket-b", DATA_USAGE_ROOT, DataUsageEntry::default());
    first_set.replace("bucket-empty", DATA_USAGE_ROOT, DataUsageEntry::default());
    let mut second_set = completed_root_cache("bucket-b", 2, 20, DataUsageCacheSource::new(1, 0));
    second_set.replace("bucket-a", DATA_USAGE_ROOT, DataUsageEntry::default());
    second_set.replace("bucket-empty", DATA_USAGE_ROOT, DataUsageEntry::default());

    assert!(
        completed_data_usage_info_for_test(&[first_set.clone(), DataUsageCache::default()], &all_buckets, false, false).is_none()
    );
    assert!(completed_data_usage_info_for_test(&[first_set.clone(), second_set.clone()], &all_buckets, true, false).is_none());
    assert!(completed_data_usage_info_for_test(&[first_set.clone(), second_set.clone()], &all_buckets, false, true).is_none());

    let (data_usage_info, last_update) = completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false)
        .expect("all completed sets should produce a publishable data usage snapshot");
    assert_eq!(last_update, SystemTime::UNIX_EPOCH + Duration::from_secs(20));
    assert_eq!(data_usage_info.scanner_cycle, Some(0));
    assert_eq!(data_usage_info.objects_total_count, 3);
    assert_eq!(data_usage_info.buckets_usage.len(), 3);
    assert!(data_usage_info.usage_snapshot_complete);
    assert_eq!(
        data_usage_info
            .buckets_usage
            .get("bucket-empty")
            .map(|usage| (usage.objects_count, usage.size)),
        Some((0, 0))
    );
}

#[test]
fn completed_data_usage_info_publishes_confirmed_empty_namespace() {
    let mut completed_set = DataUsageCache {
        info: DataUsageCacheInfo {
            name: DATA_USAGE_ROOT.to_string(),
            last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(10)),
            source: Some(DataUsageCacheSource::new(0, 0)),
            snapshot_complete: true,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            ..Default::default()
        },
        ..Default::default()
    };
    completed_set.replace(DATA_USAGE_ROOT, "", DataUsageEntry::default());

    let (data_usage_info, _) = completed_data_usage_info_for_test(&[completed_set], &[], false, false)
        .expect("a completed empty namespace should produce an authoritative snapshot");

    assert!(data_usage_info.is_complete_bucket_usage_snapshot());
    assert_eq!(data_usage_info.buckets_count, 0);
    assert!(data_usage_info.buckets_usage.is_empty());
}

#[test]
fn complete_usage_candidate_with_changed_generation_is_superseded() {
    let all_buckets = vec!["bucket".to_string()];
    let completed_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let completed_usage = completed_data_usage_info_for_test(&[completed_set], &all_buckets, false, false);

    assert!(completed_usage.is_some());
    assert_eq!(
        classify_nsscanner_cycle(
            completed_usage.is_some(),
            false,
            false,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Changed,
            ScannerCycleActivityStatus::Unchanged,
        ),
        ScannerCycleStatus::Superseded
    );
}

#[test]
fn completed_data_usage_info_adds_same_bucket_across_unique_sets() {
    let all_buckets = vec!["bucket".to_string()];
    let first_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let mut second_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));
    let second_entry = second_set.find("bucket").cloned().expect("second set bucket entry");
    second_set.replace(
        "bucket",
        DATA_USAGE_ROOT,
        DataUsageEntry {
            versions: 4,
            delete_markers: 1,
            ..second_entry
        },
    );

    let (data_usage_info, last_update) = completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false)
        .expect("unique completed set snapshots should be aggregated");
    let bucket = data_usage_info.buckets_usage.get("bucket").expect("merged bucket usage");

    assert_eq!(last_update, SystemTime::UNIX_EPOCH + Duration::from_secs(20));
    assert_eq!(data_usage_info.objects_total_count, 5);
    assert_eq!(data_usage_info.objects_total_size, 50);
    assert_eq!(bucket.objects_count, 5);
    assert_eq!(bucket.size, 50);
    assert_eq!(bucket.versions_count, 4);
    assert_eq!(bucket.delete_markers_count, 1);
}

#[test]
fn completed_data_usage_info_flattens_nested_bucket_entries() {
    let all_buckets = vec!["bucket".to_string()];
    let mut first_set = completed_root_cache("bucket", 1, 10, DataUsageCacheSource::new(0, 0));
    let mut nested = DataUsageEntry {
        objects: 2,
        versions: 3,
        size: 2048,
        replication_stats: Some(ReplicationAllStats {
            targets: HashMap::from([(
                "arn:target".to_string(),
                ReplicationTargetUsage {
                    replicated_size: 2048,
                    replicated_count: 2,
                    ..Default::default()
                },
            )]),
            replica_size: 2048,
            replica_count: 2,
        }),
        ..Default::default()
    };
    nested.obj_sizes.add(2048);
    nested.obj_versions.add(3);
    first_set.replace("bucket/prefix", "bucket", nested);
    let second_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));

    let (data_usage_info, _) = completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false)
        .expect("nested bucket entries should be flattened before aggregation");
    let bucket = data_usage_info.buckets_usage.get("bucket").expect("merged bucket usage");

    assert_eq!(data_usage_info.objects_total_count, 6);
    assert_eq!(data_usage_info.objects_total_size, 2088);
    assert_eq!(bucket.objects_count, 6);
    assert_eq!(bucket.versions_count, 3);
    assert_eq!(bucket.object_size_histogram["BETWEEN_1024_B_AND_64_KB"], 1);
    assert_eq!(bucket.object_versions_histogram["BETWEEN_2_AND_10"], 1);
    assert_eq!(bucket.replica_size, 2048);
    assert_eq!(bucket.replica_count, 2);
    assert_eq!(bucket.replication_info["arn:target"].replicated_size, 2048);
    assert_eq!(bucket.replication_info["arn:target"].replicated_count, 2);
}

#[test]
fn completed_data_usage_info_rejects_cyclic_bucket_entries() {
    let all_buckets = vec!["bucket".to_string()];
    let mut cache = completed_root_cache("bucket", 1, 10, DataUsageCacheSource::new(0, 0));
    cache.replace(
        "bucket/prefix",
        "bucket",
        DataUsageEntry {
            objects: 1,
            size: 10,
            ..Default::default()
        },
    );
    cache
        .cache
        .get_mut(&crate::hash_path("bucket/prefix").key())
        .expect("nested entry")
        .add_child(&crate::hash_path("bucket"));

    assert!(completed_data_usage_info_for_test(&[cache], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_duplicate_or_incomplete_set_sources() {
    let all_buckets = vec!["bucket".to_string()];
    let source = DataUsageCacheSource::new(0, 0);
    let first_set = completed_root_cache("bucket", 2, 10, source);
    let duplicate_set = completed_root_cache("bucket", 3, 20, source);

    assert!(completed_data_usage_info_for_test(&[first_set.clone(), duplicate_set], &all_buckets, false, false).is_none());

    let mut incomplete_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));
    incomplete_set.info.snapshot_complete = false;
    assert!(completed_data_usage_info_for_test(&[first_set, incomplete_set], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_requires_exact_topology_sources() {
    let all_buckets = vec!["bucket".to_string()];
    let first_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let unexpected_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(99, 99));
    let expected_sources = HashSet::from([DataUsageCacheSource::new(0, 0), DataUsageCacheSource::new(1, 0)]);

    assert!(
        completed_data_usage_info(&[first_set, unexpected_set], &expected_sources, &all_buckets, true, false, false).is_none()
    );
}

#[test]
fn completed_data_usage_info_rejects_incomplete_bucket_plan() {
    let all_buckets = vec!["bucket".to_string()];
    let set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let expected_sources = HashSet::from([DataUsageCacheSource::new(0, 0)]);

    assert!(completed_data_usage_info(&[set], &expected_sources, &all_buckets, false, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_missing_bucket_from_any_set() {
    let all_buckets = vec!["bucket-a".to_string(), "bucket-b".to_string()];
    let mut complete_set = completed_root_cache("bucket-a", 2, 10, DataUsageCacheSource::new(0, 0));
    complete_set.replace("bucket-b", DATA_USAGE_ROOT, DataUsageEntry::default());
    let missing_bucket_set = completed_root_cache("bucket-a", 3, 20, DataUsageCacheSource::new(1, 0));

    assert!(completed_data_usage_info_for_test(&[complete_set, missing_bucket_set], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_mixed_scan_plans() {
    let all_buckets = vec!["bucket".to_string()];
    let first_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let mut second_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));
    second_set.info.scan_plan_digest = Some(DataUsageScanPlanDigest([8; 32]));

    assert!(completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_mixed_scanner_cycles() {
    let all_buckets = vec!["bucket".to_string()];
    let mut first_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let mut second_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));
    first_set.info.next_cycle = 12;
    second_set.info.next_cycle = 13;

    assert!(completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_mixed_leader_epochs() {
    let all_buckets = vec!["bucket".to_string()];
    let mut first_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let mut second_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));
    first_set.info.leader_epoch = 11;
    second_set.info.leader_epoch = 12;

    assert!(completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_counter_overflow() {
    let all_buckets = vec!["bucket".to_string()];
    let first_set = completed_root_cache("bucket", usize::MAX, 10, DataUsageCacheSource::new(0, 0));
    let second_set = completed_root_cache("bucket", 1, 20, DataUsageCacheSource::new(1, 0));

    assert!(completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false).is_none());
}

#[test]
fn current_cache_snapshot_requires_matching_complete_source_and_cycle() {
    let source = DataUsageCacheSource::new(1, 2);
    let mut cache = completed_root_cache("bucket", 1, 10, source);
    cache.info.next_cycle = 10;

    assert!(cache_snapshot_is_current(&cache, DATA_USAGE_ROOT, source, 10, 0, TEST_PLAN_DIGEST));
    assert!(!cache_snapshot_is_current(&cache, "bucket", source, 10, 0, TEST_PLAN_DIGEST));
    assert!(!cache_snapshot_is_current(
        &cache,
        DATA_USAGE_ROOT,
        DataUsageCacheSource::new(2, 1),
        10,
        0,
        TEST_PLAN_DIGEST
    ));
    assert!(!cache_snapshot_is_current(&cache, DATA_USAGE_ROOT, source, 11, 0, TEST_PLAN_DIGEST));
    assert!(!cache_snapshot_is_current(
        &cache,
        DATA_USAGE_ROOT,
        source,
        10,
        0,
        DataUsageScanPlanDigest([8; 32])
    ));
    cache.info.leader_epoch = 2;
    assert!(!cache_snapshot_is_current(&cache, DATA_USAGE_ROOT, source, 10, 1, TEST_PLAN_DIGEST));
    assert!(cache_snapshot_is_current(&cache, DATA_USAGE_ROOT, source, 10, 2, TEST_PLAN_DIGEST));
    cache.info.next_cycle = 11;
    assert!(!cache_snapshot_is_current(&cache, DATA_USAGE_ROOT, source, 10, 2, TEST_PLAN_DIGEST));
}

#[test]
fn current_cache_snapshot_rejects_persisted_windows_key_mismatch() {
    let source = DataUsageCacheSource::new(1, 2);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 10,
            last_update: Some(SystemTime::UNIX_EPOCH),
            source: Some(source),
            snapshot_complete: true,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.cache.insert(
        "bucket".to_string(),
        DataUsageEntry {
            children: HashSet::from(["bucket/prefix".to_string()]),
            ..Default::default()
        },
    );
    cache.cache.insert(
        "bucket\\prefix".to_string(),
        DataUsageEntry {
            objects: 3,
            ..Default::default()
        },
    );

    assert!(!cache_snapshot_is_current(&cache, "bucket", source, 10, 0, TEST_PLAN_DIGEST));
    match current_cache_root_or_prepare(&mut cache, "bucket", source, 10, 0, TEST_PLAN_DIGEST, true) {
        DataUsageCacheScanState::Prepared {
            outcome: DataUsageCachePrepareOutcome::Reset,
            invalid_current: Some(_),
        } => {}
        _ => panic!("an invalid current cache must enter the rebuild path"),
    }
    assert!(cache.cache.is_empty());
    assert_eq!(cache.info.cache_key_format, DATA_USAGE_CACHE_KEY_FORMAT);
}

#[test]
fn current_cache_snapshot_rejects_structurally_valid_legacy_key_format() {
    let source = DataUsageCacheSource::new(1, 2);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 10,
            last_update: Some(SystemTime::UNIX_EPOCH),
            source: Some(source),
            snapshot_complete: true,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.cache.insert(
        "bucket".to_string(),
        DataUsageEntry {
            children: HashSet::from(["bucket\\prefix".to_string()]),
            ..Default::default()
        },
    );
    cache.cache.insert(
        "bucket\\prefix".to_string(),
        DataUsageEntry {
            objects: 3,
            ..Default::default()
        },
    );
    assert_eq!(cache.checked_flatten("bucket").map(|entry| entry.objects), Some(3));

    match current_cache_root_or_prepare(&mut cache, "bucket", source, 10, 0, TEST_PLAN_DIGEST, true) {
        DataUsageCacheScanState::Prepared {
            outcome: DataUsageCachePrepareOutcome::Reset,
            invalid_current: None,
        } => {}
        _ => panic!("a legacy key format must enter the rebuild path"),
    }
    assert!(cache.cache.is_empty());
    assert_eq!(cache.info.cache_key_format, DATA_USAGE_CACHE_KEY_FORMAT);
}

#[test]
fn current_cache_snapshot_rejects_current_bucket_cache_with_detached_entry() {
    let source = DataUsageCacheSource::new(1, 2);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 10,
            last_update: Some(SystemTime::UNIX_EPOCH),
            source: Some(source),
            snapshot_complete: true,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.cache.insert(
        "bucket".to_string(),
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    cache.cache.insert(
        "bucket/detached".to_string(),
        DataUsageEntry {
            objects: 2,
            ..Default::default()
        },
    );
    assert_eq!(cache.checked_flatten("bucket").map(|entry| entry.objects), Some(1));

    match current_cache_root_or_prepare(&mut cache, "bucket", source, 10, 0, TEST_PLAN_DIGEST, true) {
        DataUsageCacheScanState::Prepared {
            outcome: DataUsageCachePrepareOutcome::Reset,
            invalid_current: Some(_),
        } => {}
        _ => panic!("a detached complete bucket cache must enter the rebuild path"),
    }
    assert!(cache.cache.is_empty());
    assert_eq!(cache.info.cache_key_format, DATA_USAGE_CACHE_KEY_FORMAT);
}

#[test]
fn namespace_scanner_worker_selection_keeps_coordinator_fallback_disks() {
    let server_epoch = uuid::Uuid::new_v4();
    let workers = namespace_scanner_workers(vec!["local", "legacy-remote"], vec![("v4", server_epoch)]);

    assert_eq!(
        workers,
        vec![
            ("local", NamespaceScannerWorkerMode::Coordinator),
            ("legacy-remote", NamespaceScannerWorkerMode::Coordinator),
            ("v4", NamespaceScannerWorkerMode::RemoteV4(server_epoch)),
        ]
    );
    assert!(namespace_scanner_workers::<()>(Vec::new(), Vec::new()).is_empty());
}

#[test]
fn remote_scanner_capability_probes_are_grouped_by_peer() {
    let mut groups = group_remote_disks_by_peer(vec![("node-a", 0), ("node-b", 0), ("node-a", 1), ("node-b", 1)], |disk| {
        disk.0.to_string()
    });
    groups.sort_by_key(|group| group[0].0);

    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0], vec![("node-a", 0), ("node-a", 1)]);
    assert_eq!(groups[1], vec![("node-b", 0), ("node-b", 1)]);
}

#[test]
fn scanner_bucket_plan_digest_is_order_independent_and_membership_sensitive() {
    let activity_digest = [4; 32];
    let buckets = vec![
        BucketInfo {
            name: "photos".to_string(),
            ..Default::default()
        },
        BucketInfo {
            name: "videos".to_string(),
            ..Default::default()
        },
    ];
    let reversed = vec![buckets[1].clone(), buckets[0].clone()];
    let changed = vec![
        buckets[0].clone(),
        BucketInfo {
            name: "archives".to_string(),
            ..Default::default()
        },
    ];
    let mut regenerated = buckets.clone();
    regenerated[0].created = Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1));

    assert_eq!(
        scanner_bucket_plan_digest(&buckets, activity_digest),
        scanner_bucket_plan_digest(&reversed, activity_digest)
    );
    assert_ne!(
        scanner_bucket_plan_digest(&buckets, activity_digest),
        scanner_bucket_plan_digest(&changed, activity_digest)
    );
    assert_ne!(
        scanner_bucket_plan_digest(&buckets, activity_digest),
        scanner_bucket_plan_digest(&regenerated, activity_digest)
    );
    assert_ne!(
        scanner_bucket_plan_digest(&buckets, activity_digest),
        scanner_bucket_plan_digest(&buckets, [5; 32])
    );
}

#[test]
fn dirty_bucket_cache_digest_changes_with_generation() {
    let source = DataUsageCacheSource::new(0, 0);
    let plan = DataUsageScanPlanDigest([9; 32]);
    let first = scanner_bucket_cache_digest(plan, Some(7));
    let second = scanner_bucket_cache_digest(plan, Some(8));
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "photos".to_string(),
            next_cycle: 11,
            last_update: Some(SystemTime::now()),
            source: Some(source),
            snapshot_complete: true,
            scan_plan_digest: Some(first),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace("photos", "", DataUsageEntry::default());

    assert_eq!(scanner_bucket_cache_digest(plan, None), plan);
    assert!(cache_snapshot_is_current(&cache, "photos", source, 11, 0, first));
    assert!(!cache_snapshot_is_current(&cache, "photos", source, 11, 0, second));
}

#[test]
fn scanner_cache_lock_resource_is_scoped_to_cache_source() {
    let cache_name = "photos/.usage-cache.bin";
    let first_source = DataUsageCacheSource::new(0, 1);
    let same_source = DataUsageCacheSource::new(0, 1);
    let other_source = DataUsageCacheSource::new(1, 0);

    let first = scanner_cache_lock_resource(cache_name, first_source);
    assert_eq!(first, scanner_cache_lock_resource(cache_name, same_source));
    assert_ne!(first, scanner_cache_lock_resource(cache_name, other_source));
    assert!(first.ends_with(".scanner-cycle.lock.pool-0.set-1"));
}

#[test]
fn count_budget_serializes_set_and_disk_work() {
    assert_eq!(scanner_budgeted_concurrency_limit(8, true), 1);
    assert_eq!(scanner_budgeted_concurrency_limit(8, false), 8);
}

#[test]
fn requeued_bucket_work_is_only_completed_after_retry() {
    let remaining = Arc::new(AtomicUsize::new(1));
    let complete = CancellationToken::new();
    let mut first = BucketWorkGuard::new(remaining.clone(), complete.clone());
    first.mark_requeued();
    drop(first);
    assert_eq!(remaining.load(Ordering::Acquire), 1);
    assert!(!complete.is_cancelled());

    drop(BucketWorkGuard::new(remaining.clone(), complete.clone()));
    assert_eq!(remaining.load(Ordering::Acquire), 0);
    assert!(complete.is_cancelled());
}

#[tokio::test]
async fn exhausted_workers_mark_all_queued_bucket_work_failed() {
    let (tx, rx) = mpsc::channel(2);
    tx.send(BucketInfo {
        name: "photos".to_string(),
        ..Default::default()
    })
    .await
    .expect("queue photos");
    tx.send(BucketInfo {
        name: "videos".to_string(),
        ..Default::default()
    })
    .await
    .expect("queue videos");
    drop(tx);

    let receiver = Mutex::new(rx);
    let remaining = Arc::new(AtomicUsize::new(2));
    let complete = CancellationToken::new();
    let failed = Arc::new(Mutex::new(HashSet::new()));

    let failed_count = mark_unprocessed_bucket_work_failed(&receiver, &remaining, &complete, &failed).await;

    assert_eq!(failed_count, 2);
    assert_eq!(remaining.load(Ordering::Acquire), 0);
    assert!(complete.is_cancelled());
    assert_eq!(*failed.lock().await, HashSet::from(["photos".to_string(), "videos".to_string()]));
}

#[tokio::test]
async fn requeued_bucket_remains_pending_for_another_worker() {
    let (tx, mut rx) = mpsc::channel(1);
    let remaining = Arc::new(AtomicUsize::new(1));
    let complete = CancellationToken::new();
    let mut guard = BucketWorkGuard::new(remaining.clone(), complete.clone());
    let bucket = BucketInfo {
        name: "photos".to_string(),
        ..Default::default()
    };

    assert!(requeue_bucket_work(&tx, &bucket, &mut guard).await);
    drop(guard);

    assert_eq!(remaining.load(Ordering::Acquire), 1);
    assert!(!complete.is_cancelled());
    assert_eq!(rx.recv().await.expect("requeued bucket").name, "photos");
}
