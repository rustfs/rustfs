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
use crate::data_usage_define::{DataUsageSegmentInvalidationProof, UNKNOWN_TIER, UnknownTierStats, hash_path};
use rustfs_data_usage::{ReplicationAllStats, ReplicationTargetUsage, TierAccountingProof};

const TEST_PLAN_DIGEST: DataUsageScanPlanDigest = DataUsageScanPlanDigest([7; 32]);
const TEST_COVERAGE_DIGEST: DataUsageScanPlanDigest = DataUsageScanPlanDigest([6; 32]);

#[test]
fn partial_progress_requires_complete_scope_and_no_hard_failure() {
    for plan_complete in [false, true] {
        for scope_matches in [false, true] {
            for set_failed in [false, true] {
                for missing_namespace in [false, true] {
                    for status in [
                        ScannerBucketScanStatus::Complete,
                        ScannerBucketScanStatus::Partial,
                        ScannerBucketScanStatus::Failed,
                        ScannerBucketScanStatus::NamespaceNotFound,
                    ] {
                        let expected = plan_complete
                            && scope_matches
                            && !set_failed
                            && !missing_namespace
                            && status == ScannerBucketScanStatus::Partial;
                        assert_eq!(
                            scanner_partial_progress_is_usable(
                                plan_complete,
                                scope_matches,
                                status,
                                set_failed,
                                missing_namespace
                            ),
                            expected,
                            "plan={plan_complete}, scope={scope_matches}, status={status:?}, set_failed={set_failed}, missing_namespace={missing_namespace}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn scanner_bucket_inventory_requires_exact_unique_set_union() {
    let first = BucketInfo {
        name: "first".to_string(),
        ..Default::default()
    };
    let second = BucketInfo {
        name: "second".to_string(),
        ..Default::default()
    };
    let source = DataUsageCacheSource::new(0, 0);
    let mut sets = HashMap::from([(source, vec![first.clone()])]);
    assert!(scanner_bucket_inventory_is_complete(std::slice::from_ref(&first), &sets));
    assert!(!scanner_bucket_inventory_is_complete(&[first.clone(), second.clone()], &sets));
    assert!(!scanner_bucket_inventory_is_complete(&[], &sets));
    assert!(!scanner_bucket_inventory_is_complete(&[first.clone(), first.clone()], &sets));
    sets.insert(source, vec![first.clone(), first.clone()]);
    assert!(!scanner_bucket_inventory_is_complete(std::slice::from_ref(&first), &sets));
    sets.insert(source, vec![second]);
    assert!(!scanner_bucket_inventory_is_complete(std::slice::from_ref(&first), &sets));
    let mut recreated = first.clone();
    recreated.created = Some(OffsetDateTime::UNIX_EPOCH);
    sets.insert(source, vec![recreated]);
    assert!(!scanner_bucket_inventory_is_complete(&[first], &sets));
}

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
            scan_coverage_digest: Some(TEST_COVERAGE_DIGEST),
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
            tier_accounting_proof: Some(TierAccountingProof {
                logical_total: u64::try_from(objects.saturating_mul(10)).unwrap_or(u64::MAX),
                logical_known: u64::try_from(objects.saturating_mul(10)).unwrap_or(u64::MAX),
                ..Default::default()
            }),
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
    completed_usage_for_scope(results, &expected_sources, all_buckets, &[], true, budget_elapsed, cancelled)
}

fn completed_usage_for_scope(
    results: &[DataUsageCache],
    expected_sources: &HashSet<DataUsageCacheSource>,
    all_buckets: &[String],
    tier_registry_names: &[String],
    bucket_plan_complete: bool,
    budget_elapsed: bool,
    cancelled: bool,
) -> Option<(DataUsageInfo, SystemTime)> {
    let first = results.first()?;
    completed_data_usage_info(
        results,
        &ScannerSnapshotScope {
            sources: expected_sources,
            buckets: all_buckets,
            identity: ScannerSnapshotIdentity {
                cycle: first.info.next_cycle,
                leader_epoch: first.info.leader_epoch,
                plan_digest: TEST_PLAN_DIGEST,
                coverage_digest: TEST_COVERAGE_DIGEST,
                tier_registry_generation: first.info.tier_registry_generation,
            },
        },
        tier_registry_names,
        bucket_plan_complete,
        budget_elapsed,
        cancelled,
    )
}

#[test]
fn completed_data_usage_info_rejects_duplicate_bucket_inventory() {
    let set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let buckets = vec!["bucket".to_string(), "bucket".to_string()];
    assert!(completed_data_usage_info_for_test(&[set], &buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_carries_segment_invalidation_proof_to_set_state() {
    let source = DataUsageCacheSource::new(0, 0);
    let proof = DataUsageSegmentInvalidationProof {
        process_epoch: "scanner-process".to_string(),
        generation_start: 5,
        generation_end: 8,
        producer_identity_coverage_complete: true,
        cold_zero_walk_oracle: true,
    };
    let mut set = completed_root_cache("bucket", 2, 10, source);
    set.info.segment_invalidation_proof = Some(proof.clone());

    let (usage, _) =
        completed_usage_for_scope(&[set], &HashSet::from([source]), &["bucket".to_string()], &[], true, false, false)
            .expect("complete set should publish root usage");

    assert_eq!(usage.usage_snapshot_set_states.len(), 1);
    assert_eq!(usage.usage_snapshot_set_states[0].segment_invalidation_proof, Some(proof));
}

#[test]
fn completed_data_usage_info_rejects_extra_or_detached_bucket_data() {
    let buckets = vec!["bucket".to_string()];
    let mut set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    set.replace(
        "unlisted",
        DATA_USAGE_ROOT,
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    assert!(completed_data_usage_info_for_test(&[set.clone()], &buckets, false, false).is_none());
    set.cache
        .get_mut(DATA_USAGE_ROOT)
        .expect("set root")
        .children
        .remove(&hash_path("unlisted").key());
    assert!(
        completed_data_usage_info_for_test(&[set], &buckets, false, false).is_none(),
        "orphaned data must not disappear from authoritative accounting"
    );
}

#[test]
fn completed_data_usage_info_rejects_disconnected_expected_bucket() {
    let buckets = vec!["bucket".to_string()];
    let mut set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    set.cache.get_mut(DATA_USAGE_ROOT).expect("set root").children.clear();
    assert!(completed_data_usage_info_for_test(&[set], &buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_root_scalar_data_and_unknown_key_format() {
    let buckets = vec!["bucket".to_string()];
    let set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let mut scalar_root = set.clone();
    scalar_root.cache.get_mut(DATA_USAGE_ROOT).expect("set root").size = 10;
    assert!(completed_data_usage_info_for_test(&[scalar_root], &buckets, false, false).is_none());
    let mut future_format = set;
    future_format.info.cache_key_format = DATA_USAGE_CACHE_KEY_FORMAT + 1;
    assert!(completed_data_usage_info_for_test(&[future_format], &buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_binds_all_results_to_requested_identity() {
    let buckets = vec!["bucket".to_string()];
    let source = DataUsageCacheSource::new(0, 0);
    let sources = HashSet::from([source]);
    let set = completed_root_cache("bucket", 2, 10, source);
    let identity = ScannerSnapshotIdentity {
        cycle: 0,
        leader_epoch: 0,
        plan_digest: TEST_PLAN_DIGEST,
        coverage_digest: TEST_COVERAGE_DIGEST,
        tier_registry_generation: None,
    };
    let results = [set];
    for expected in [
        ScannerSnapshotIdentity { cycle: 1, ..identity },
        ScannerSnapshotIdentity {
            leader_epoch: 1,
            ..identity
        },
        ScannerSnapshotIdentity {
            plan_digest: DataUsageScanPlanDigest([9; 32]),
            ..identity
        },
        ScannerSnapshotIdentity {
            tier_registry_generation: Some(1),
            ..identity
        },
        ScannerSnapshotIdentity {
            coverage_digest: DataUsageScanPlanDigest([4; 32]),
            ..identity
        },
    ] {
        let scope = ScannerSnapshotScope {
            sources: &sources,
            buckets: &buckets,
            identity: expected,
        };
        assert!(completed_data_usage_info(&results, &scope, &[], true, false, false).is_none());
    }
    let scope = ScannerSnapshotScope {
        sources: &sources,
        buckets: &buckets,
        identity,
    };
    let (usage, _) = completed_data_usage_info(&results, &scope, &[], true, false, false)
        .expect("the requested complete scope remains publishable");
    assert_eq!(usage.objects_total_count, 2);
    assert!(usage.is_complete_bucket_usage_snapshot());
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
        completed_usage_for_scope(&[current.clone(), stalled.clone()], &expected, &all_buckets, &[], true, false, false)
            .is_none()
    );
    let (observed, _) = observational_data_usage_info(&[current, stalled], &expected, &all_buckets, &[], TEST_PLAN_DIGEST, 8, 3)
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
    let (observed, _) = observational_data_usage_info(&[current], &expected, &all_buckets, &[], TEST_PLAN_DIGEST, 8, 3)
        .expect("complete set data is a valid observational baseline");
    assert_eq!(observed.objects_total_size, 30);
    assert!(observed.usage_snapshot_set_states[0].complete);
}

#[test]
fn negative_delta_waits_for_set_reconciliation() {
    let all_buckets = vec!["bucket".to_string()];
    let source = DataUsageCacheSource::new(0, 0);
    let mut stalled = lkg_root_cache("bucket", 4, source);
    stalled.info.lkg_scan_plan_digest = Some(DataUsageScanPlanDigest([9; 32]));
    let expected = HashSet::from([source]);
    assert!(observational_data_usage_info(&[stalled], &expected, &all_buckets, &[], TEST_PLAN_DIGEST, 8, 3).is_none());
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
        segment_invalidation_proof: None,
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
                segment_invalidation_proof: None,
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
    assert!(observational_data_usage_info(&[old], &expected, &all_buckets, &[], TEST_PLAN_DIGEST, 8, 3).is_none());
}

#[test]
fn usage_aggregate_survives_restart_and_leader_failover() {
    let all_buckets = vec!["bucket".to_string()];
    let source = DataUsageCacheSource::new(0, 0);
    let mut lkg = lkg_root_cache("bucket", 5, source);
    lkg.info.lkg_leader_epoch = Some(4);
    lkg.info.lkg_next_cycle = Some(9);
    let expected = HashSet::from([source]);
    let (observed, _) = observational_data_usage_info(&[lkg], &expected, &all_buckets, &[], TEST_PLAN_DIGEST, 10, 5)
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
    let (observed, _) = observational_data_usage_info(&results, &expected, &all_buckets, &[], TEST_PLAN_DIGEST, 8, 3)
        .expect("all set snapshots should aggregate");
    assert_eq!(observed.objects_total_count, 32);
    let reversed = results.iter().rev().cloned().collect::<Vec<_>>();
    let (reversed_observed, _) = observational_data_usage_info(&reversed, &expected, &all_buckets, &[], TEST_PLAN_DIGEST, 8, 3)
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
    tiered.tier_accounting_proof = Some(TierAccountingProof {
        physical_total: 100,
        physical_known: 100,
        ..Default::default()
    });
    first_set.replace("bucket-b", DATA_USAGE_ROOT, tiered);

    let mut second_set = completed_root_cache("bucket-b", 2, 20, DataUsageCacheSource::new(1, 0));
    let mut tiered = DataUsageEntry::default();
    tiered.add_tier_sizes(&warm(50, 1, 1));
    tiered.tier_accounting_proof = Some(TierAccountingProof {
        physical_total: 50,
        physical_known: 50,
        ..Default::default()
    });
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
fn completed_data_usage_info_rejects_logical_proof_mismatch() {
    let all_buckets = vec!["bucket-a".to_string()];
    let mut set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    let entry = set.cache.get_mut(&hash_path("bucket-a").key()).expect("bucket entry");
    entry.add_tier_sizes(&HashMap::from([(
        "WARM".to_string(),
        TierStats {
            total_size: 10,
            num_versions: 1,
            num_objects: 1,
        },
    )]));
    entry.tier_accounting_proof = Some(TierAccountingProof {
        logical_total: 10,
        logical_known: 9,
        physical_total: 10,
        physical_known: 10,
        ..Default::default()
    });

    assert!(completed_data_usage_info_for_test(&[set], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_logical_total_size_mismatch() {
    let all_buckets = vec!["bucket-a".to_string()];
    let mut set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    let entry = set.cache.get_mut(&hash_path("bucket-a").key()).expect("bucket entry");
    entry.size = 11;
    entry.add_tier_sizes(&HashMap::from([(
        "WARM".to_string(),
        TierStats {
            total_size: 10,
            num_versions: 1,
            num_objects: 1,
        },
    )]));
    entry.tier_accounting_proof = Some(TierAccountingProof {
        logical_total: 10,
        logical_known: 10,
        physical_total: 10,
        physical_known: 10,
        ..Default::default()
    });

    assert!(completed_data_usage_info_for_test(&[set], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_physical_proof_mismatch() {
    let all_buckets = vec!["bucket-a".to_string()];
    let mut set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    let entry = set.cache.get_mut(&hash_path("bucket-a").key()).expect("bucket entry");
    entry.add_tier_sizes(&HashMap::from([(
        "WARM".to_string(),
        TierStats {
            total_size: 10,
            num_versions: 1,
            num_objects: 1,
        },
    )]));
    entry.tier_accounting_proof = Some(TierAccountingProof {
        logical_total: 10,
        logical_known: 10,
        physical_total: 9,
        physical_known: 9,
        ..Default::default()
    });

    assert!(completed_data_usage_info_for_test(&[set], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_unknown_physical_double_accounting() {
    let all_buckets = vec!["bucket-a".to_string()];
    let mut set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    let entry = set.cache.get_mut(&hash_path("bucket-a").key()).expect("bucket entry");
    entry.add_tier_sizes(&HashMap::from([(
        UNKNOWN_TIER.to_string(),
        TierStats {
            total_size: 10,
            num_versions: 1,
            num_objects: 1,
        },
    )]));
    entry.add_unknown_tier_stats(&UnknownTierStats {
        unknown_physical_bytes: 9,
        ..Default::default()
    });
    entry.tier_accounting_proof = Some(TierAccountingProof {
        logical_total: 10,
        logical_known: 10,
        physical_total: 10,
        physical_known: 10,
        ..Default::default()
    });

    assert!(completed_data_usage_info_for_test(&[set], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_unknown_counter_overflow() {
    let all_buckets = vec!["bucket-a".to_string()];
    let mut set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    let entry = set.cache.get_mut(&hash_path("bucket-a").key()).expect("bucket entry");
    entry.add_unknown_tier_stats(&UnknownTierStats {
        counter_overflowed: true,
        ..Default::default()
    });

    assert!(completed_data_usage_info_for_test(&[set], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_accepts_no_tier_standard_empty_map_with_proof() {
    let all_buckets = vec!["bucket-a".to_string()];
    let set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));

    let (info, _) = completed_data_usage_info_for_test(&[set], &all_buckets, false, false)
        .expect("no-tier STANDARD/RRS usage does not require a tier map");
    assert!(info.tier_stats.is_none());
}

#[test]
fn completed_data_usage_info_accepts_no_tier_unknown_and_standard_shape() {
    let all_buckets = vec!["bucket-a".to_string()];
    let mut set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    let entry = set.cache.get_mut(&hash_path("bucket-a").key()).expect("bucket entry");
    entry.size = 13;
    entry.add_tier_sizes(&HashMap::from([(
        UNKNOWN_TIER.to_string(),
        TierStats {
            total_size: 3,
            num_versions: 1,
            num_objects: 1,
        },
    )]));
    entry.add_unknown_tier_stats(&UnknownTierStats {
        unknown_bytes: 9,
        unknown_physical_bytes: 3,
        unknown_objects: 1,
        unknown_versions: 1,
        ..Default::default()
    });
    entry.tier_accounting_proof = Some(TierAccountingProof {
        logical_total: 13,
        logical_known: 4,
        physical_total: 7,
        physical_known: 4,
        ..Default::default()
    });

    let (info, _) = completed_data_usage_info_for_test(&[set], &all_buckets, false, false)
        .expect("no-tier STANDARD plus UNKNOWN should remain publishable");
    assert_eq!(
        info.tier_stats.expect("unknown bucket should be retained").tiers[UNKNOWN_TIER].total_size,
        3
    );
}

#[test]
fn completed_data_usage_info_accepts_unknown_only_with_current_registry_generation() {
    let all_buckets = vec!["bucket-a".to_string()];
    let mut set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    set.info.tier_registry_generation = Some(7);
    let entry = set.cache.get_mut(&hash_path("bucket-a").key()).expect("bucket entry");
    entry.size = 13;
    entry.add_tier_sizes(&HashMap::from([(
        UNKNOWN_TIER.to_string(),
        TierStats {
            total_size: 3,
            num_versions: 1,
            num_objects: 1,
        },
    )]));
    entry.add_unknown_tier_stats(&UnknownTierStats {
        unknown_bytes: 9,
        unknown_physical_bytes: 3,
        unknown_objects: 1,
        unknown_versions: 1,
        ..Default::default()
    });
    entry.tier_accounting_proof = Some(TierAccountingProof {
        logical_total: 13,
        logical_known: 4,
        physical_total: 7,
        physical_known: 4,
        ..Default::default()
    });

    let expected_sources = HashSet::from([DataUsageCacheSource::new(0, 0)]);
    assert!(
        completed_usage_for_scope(&[set], &expected_sources, &all_buckets, &["WARM".to_string()], true, false, false,).is_some()
    );
}

#[test]
fn completed_data_usage_info_rejects_non_registry_tier_in_current_generation() {
    let all_buckets = vec!["bucket-a".to_string()];
    let mut set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    set.info.tier_registry_generation = Some(7);
    let entry = set.cache.get_mut(&hash_path("bucket-a").key()).expect("bucket entry");
    entry.add_tier_sizes(&HashMap::from([
        (
            "WARM".to_string(),
            TierStats {
                total_size: 4,
                num_versions: 1,
                num_objects: 1,
            },
        ),
        (
            "RETIRED".to_string(),
            TierStats {
                total_size: 6,
                num_versions: 1,
                num_objects: 1,
            },
        ),
    ]));
    entry.tier_accounting_proof = Some(TierAccountingProof {
        logical_total: 10,
        logical_known: 10,
        physical_total: 10,
        physical_known: 10,
        ..Default::default()
    });

    let expected_sources = HashSet::from([DataUsageCacheSource::new(0, 0)]);
    assert!(
        completed_usage_for_scope(&[set], &expected_sources, &all_buckets, &["WARM".to_string()], true, false, false,).is_none()
    );
}

#[test]
fn completed_data_usage_info_rejects_legacy_proof_missing_when_tier_accounted() {
    let all_buckets = vec!["bucket-a".to_string()];
    let mut set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    let entry = set.cache.get_mut(&hash_path("bucket-a").key()).expect("bucket entry");
    entry.add_tier_sizes(&HashMap::from([(
        "WARM".to_string(),
        TierStats {
            total_size: 10,
            num_versions: 1,
            num_objects: 1,
        },
    )]));
    entry.tier_accounting_proof = None;

    assert!(completed_data_usage_info_for_test(&[set], &all_buckets, false, false).is_none());
}

#[test]
fn completed_data_usage_info_rejects_legacy_proof_missing_for_scalar_usage() {
    let all_buckets = vec!["bucket-a".to_string()];
    let mut set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    let entry = set.cache.get_mut(&hash_path("bucket-a").key()).expect("bucket entry");
    entry.tier_accounting_proof = None;

    assert!(completed_data_usage_info_for_test(&[set], &all_buckets, false, false).is_none());
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
fn completed_data_usage_info_rejects_legacy_and_new_tier_generations_mixed() {
    let all_buckets = vec!["bucket-a".to_string()];
    let legacy = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
    let mut current = completed_root_cache("bucket-a", 1, 20, DataUsageCacheSource::new(1, 0));
    current.info.tier_registry_generation = Some(42);

    assert!(
        completed_data_usage_info_for_test(&[legacy, current], &all_buckets, false, false).is_none(),
        "legacy and generation-tagged sets must not publish a mixed snapshot"
    );
}

#[test]
fn current_cache_root_with_new_tier_generation_resets_old_cache() {
    let source = DataUsageCacheSource::new(0, 0);
    let mut cache = completed_root_cache("bucket-a", 1, 10, source);
    cache.info.tier_registry_generation = Some(1);

    let state = current_cache_root_or_prepare_with_generation(
        &mut cache,
        DATA_USAGE_ROOT,
        source,
        0,
        0,
        TEST_PLAN_DIGEST,
        DataUsageCacheReuseOptions {
            require_source: false,
            tier_registry_generation: Some(2),
            checkpoint_identity: None,
            checkpoint_plan_digest: None,
        },
    );

    assert!(matches!(
        state,
        DataUsageCacheScanState::Prepared {
            outcome: DataUsageCachePrepareOutcome::Reset,
            ..
        }
    ));
    assert!(cache.cache.is_empty(), "old-generation entries must not be reused");
    assert_eq!(cache.info.tier_registry_generation, None);
    assert_eq!(cache.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
    assert!(!cache.info.snapshot_complete);
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
    assert_eq!(data_usage_info.scanner_epoch, Some(0));
    assert_eq!(data_usage_info.objects_total_count, 3);
    assert_eq!(data_usage_info.buckets_usage.len(), 3);
    assert!(data_usage_info.usage_snapshot_complete);
    assert_eq!(
        data_usage_info
            .usage_snapshot_set_states
            .iter()
            .map(|state| {
                (
                    state.pool_index,
                    state.set_index,
                    state.scanner_cycle,
                    state.scanner_epoch,
                    state.scan_plan_digest,
                    state.complete,
                    state.tombstone,
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (0, 0, Some(0), Some(0), Some(TEST_PLAN_DIGEST.0), true, false),
            (1, 0, Some(0), Some(0), Some(TEST_PLAN_DIGEST.0), true, false),
        ]
    );
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
            scan_coverage_digest: Some(TEST_COVERAGE_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
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
fn complete_usage_candidate_with_changed_generation_is_certified_per_bucket() {
    let all_buckets = vec!["bucket".to_string()];
    let completed_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let completed_usage = completed_data_usage_info_for_test(&[completed_set], &all_buckets, false, false);

    assert!(completed_usage.is_some());
    // A moved dirty generation shades buckets for the next cycle; only a
    // guardrail veto supersedes a structurally complete candidate.
    assert_eq!(
        classify_nsscanner_cycle(
            completed_usage.is_some(),
            false,
            false,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Changed,
            ScannerCycleActivityStatus::Unchanged,
        ),
        ScannerCycleStatus::Complete
    );
    assert_eq!(
        classify_nsscanner_cycle(
            completed_usage.is_some(),
            false,
            false,
            ScannerBucketScanStatus::Complete,
            DirtyUsageSnapshotStatus::Changed,
            ScannerCycleActivityStatus::Changed,
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
fn bucket_usage_incarnation_is_optional_and_old_readers_ignore_it() {
    #[derive(serde::Deserialize)]
    struct LegacyBucketUsage {
        size: u64,
    }
    let legacy = BucketUsageInfo {
        size: 42,
        ..Default::default()
    };
    let without = serde_json::to_value(&legacy).expect("encode legacy usage");
    assert!(without.get("bucket_incarnation").is_none());
    let restored: BucketUsageInfo = serde_json::from_value(without).expect("decode old usage");
    assert_eq!(restored.bucket_incarnation, None);
    let certified = BucketUsageInfo {
        size: 42,
        bucket_incarnation: Some(uuid::Uuid::from_u128(7).to_string()),
        ..Default::default()
    };
    let encoded = serde_json::to_vec(&certified).expect("encode certified usage");
    let old: LegacyBucketUsage = serde_json::from_slice(&encoded).expect("old schema ignores additive field");
    assert_eq!(old.size, 42);
    let named = rmp_serde::to_vec_named(&certified).expect("encode certified named msgpack");
    let old: LegacyBucketUsage = rmp_serde::from_slice(&named).expect("old named reader ignores additive field");
    assert_eq!(old.size, 42);
    let old_bytes = rmp_serde::to_vec_named(&legacy).expect("encode old shape");
    let new: BucketUsageInfo = rmp_serde::from_slice(&old_bytes).expect("new reader accepts old shape");
    assert_eq!(new.bucket_incarnation, None);
}

#[test]
fn completed_usage_certifies_only_matching_non_nil_bucket_incarnations() {
    let buckets = vec!["bucket".to_string()];
    let id = uuid::Uuid::from_u128(7);
    let mut a = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let mut b = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));
    a.info.scan_bucket_incarnations.insert("bucket".to_string(), id);
    b.info.scan_bucket_incarnations.insert("bucket".to_string(), id);
    let published = completed_data_usage_info_for_test(&[a.clone(), b.clone()], &buckets, false, false)
        .unwrap()
        .0;
    assert_eq!(
        published.buckets_usage["bucket"].bucket_incarnation.as_deref(),
        Some(id.to_string().as_str())
    );
    let encoded = serde_json::to_vec(&published).expect("serialize certified usage");
    let decoded: DataUsageInfo = serde_json::from_slice(&encoded).expect("read certified usage");
    assert_eq!(decoded.buckets_usage["bucket"].bucket_incarnation, Some(id.to_string()));
    b.info
        .scan_bucket_incarnations
        .insert("bucket".to_string(), uuid::Uuid::from_u128(8));
    let mismatch = completed_data_usage_info_for_test(&[a.clone(), b.clone()], &buckets, false, false)
        .unwrap()
        .0;
    assert_eq!(mismatch.buckets_usage["bucket"].bucket_incarnation, None);
    b.info.scan_bucket_incarnations.remove("bucket");
    let missing = completed_data_usage_info_for_test(&[a.clone(), b.clone()], &buckets, false, false)
        .unwrap()
        .0;
    assert_eq!(missing.buckets_usage["bucket"].bucket_incarnation, None);
    b.info
        .scan_bucket_incarnations
        .insert("bucket".to_string(), uuid::Uuid::nil());
    let nil = completed_data_usage_info_for_test(&[a, b], &buckets, false, false).unwrap().0;
    assert_eq!(nil.buckets_usage["bucket"].bucket_incarnation, None);
}

#[test]
fn checked_bucket_usage_info_moves_replication_keys_and_preserves_the_snapshot() {
    let first_target = "arn:rustfs:replication:local::target-a".to_string();
    let zero_target = "arn:rustfs:replication:local::zero-counters".to_string();
    let mut entry = DataUsageEntry {
        size: 32768,
        objects: 9,
        versions: 14,
        delete_markers: 2,
        replication_stats: Some(ReplicationAllStats {
            replica_size: 777,
            replica_count: 3,
            targets: HashMap::from([
                (
                    first_target.clone(),
                    ReplicationTargetUsage {
                        pending_size: 101,
                        replicated_size: 102,
                        failed_size: 103,
                        failed_count: 104,
                        pending_count: 105,
                        missed_threshold_size: 106,
                        after_threshold_size: 107,
                        missed_threshold_count: 108,
                        after_threshold_count: 109,
                        replicated_count: 110,
                    },
                ),
                (zero_target.clone(), ReplicationTargetUsage::default()),
            ]),
        }),
        ..Default::default()
    };
    entry.obj_sizes.add(2048);
    entry.obj_sizes.add(32768);
    entry.obj_versions.add(0);
    entry.obj_versions.add(3);
    let target_allocations = entry
        .replication_stats
        .as_ref()
        .expect("replication fixture")
        .targets
        .keys()
        .map(|target| (target.clone(), target.as_ptr()))
        .collect::<Vec<_>>();

    let usage = checked_bucket_usage_info(entry).expect("owned usage entry remains representable");
    for (target, allocation) in target_allocations {
        assert_eq!(
            usage
                .replication_info
                .get_key_value(&target)
                .expect("retained replication target")
                .0
                .as_ptr(),
            allocation,
            "the target key must move from the consumed entry without cloning its allocation"
        );
    }
    let expected = BucketUsageInfo {
        size: 32768,
        bucket_incarnation: None,
        replication_pending_size_v1: 0,
        replication_failed_size_v1: 0,
        replicated_size_v1: 0,
        replication_pending_count_v1: 0,
        replication_failed_count_v1: 0,
        objects_count: 9,
        object_size_histogram: HashMap::from([
            ("LESS_THAN_1024_B".to_string(), 0),
            ("BETWEEN_1024_B_AND_64_KB".to_string(), 2),
            ("BETWEEN_64_KB_AND_256_KB".to_string(), 0),
            ("BETWEEN_256_KB_AND_512_KB".to_string(), 0),
            ("BETWEEN_512_KB_AND_1_MB".to_string(), 0),
            ("BETWEEN_1024B_AND_1_MB".to_string(), 2),
            ("BETWEEN_1_MB_AND_10_MB".to_string(), 0),
            ("BETWEEN_10_MB_AND_64_MB".to_string(), 0),
            ("BETWEEN_64_MB_AND_128_MB".to_string(), 0),
            ("BETWEEN_128_MB_AND_512_MB".to_string(), 0),
            ("GREATER_THAN_512_MB".to_string(), 0),
        ]),
        object_versions_histogram: HashMap::from([
            ("UNVERSIONED".to_string(), 1),
            ("SINGLE_VERSION".to_string(), 0),
            ("BETWEEN_2_AND_10".to_string(), 1),
            ("BETWEEN_10_AND_100".to_string(), 0),
            ("BETWEEN_100_AND_1000".to_string(), 0),
            ("BETWEEN_1000_AND_10000".to_string(), 0),
            ("GREATER_THAN_10000".to_string(), 0),
        ]),
        versions_count: 14,
        delete_markers_count: 2,
        replica_size: 777,
        replica_count: 3,
        replication_info: HashMap::from([
            (
                first_target,
                BucketTargetUsageInfo {
                    replication_pending_size: 101,
                    replication_failed_size: 103,
                    replicated_size: 102,
                    replica_size: 0,
                    replication_pending_count: 105,
                    replication_failed_count: 104,
                    replicated_count: 110,
                },
            ),
            (zero_target, BucketTargetUsageInfo::default()),
        ]),
    };
    assert_eq!(usage, expected);
    assert_eq!(
        serde_json::to_value(&usage).expect("serialize owned usage"),
        serde_json::to_value(&expected).expect("serialize expected snapshot")
    );
}

#[test]
fn checked_bucket_usage_info_keeps_missing_empty_and_zero_replication_values() {
    for replication_stats in [
        None,
        Some(ReplicationAllStats::default()),
        Some(ReplicationAllStats {
            targets: HashMap::from([(String::new(), ReplicationTargetUsage::default())]),
            ..Default::default()
        }),
    ] {
        let has_empty_target = replication_stats.as_ref().is_some_and(|stats| !stats.targets.is_empty());
        let usage = checked_bucket_usage_info(DataUsageEntry {
            replication_stats,
            ..Default::default()
        })
        .expect("zero usage remains representable");
        assert_eq!(usage.object_size_histogram.len(), 11);
        assert!(usage.object_size_histogram.values().all(|value| *value == 0));
        assert_eq!(usage.object_versions_histogram.len(), 7);
        assert!(usage.object_versions_histogram.values().all(|value| *value == 0));
        let expected = BucketUsageInfo {
            object_size_histogram: usage.object_size_histogram.clone(),
            object_versions_histogram: usage.object_versions_histogram.clone(),
            replication_info: if has_empty_target {
                HashMap::from([(String::new(), BucketTargetUsageInfo::default())])
            } else {
                HashMap::new()
            },
            ..Default::default()
        };
        assert_eq!(usage, expected);
        assert_eq!(
            serde_json::to_value(&usage).expect("serialize zero usage"),
            serde_json::to_value(&expected).expect("serialize expected zero snapshot")
        );
    }
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
        tier_accounting_proof: Some(TierAccountingProof {
            logical_total: 2048,
            logical_known: 2048,
            physical_total: 2048,
            physical_known: 2048,
            ..Default::default()
        }),
        ..Default::default()
    };
    nested.obj_sizes.add(2048);
    nested.obj_versions.add(3);
    first_set.replace("bucket/prefix", "bucket", nested);
    let second_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));

    let mut results = [first_set, second_set];
    let (data_usage_info, _) = completed_data_usage_info_for_test(&results, &all_buckets, false, false)
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

    let sources = results.iter().filter_map(|result| result.info.source).collect::<HashSet<_>>();
    let (observed, _) = observational_data_usage_info(&results, &sources, &all_buckets, &[], TEST_PLAN_DIGEST, 0, 0)
        .expect("current snapshots retain the same bucket usage on the observational path");
    assert_eq!(observed.buckets_usage, data_usage_info.buckets_usage);
    assert_eq!(observed.bucket_sizes, data_usage_info.bucket_sizes);
    assert!(!observed.usage_snapshot_complete);
    assert_eq!(observed.usage_snapshot_converged, Some(false));

    for result in &mut results {
        result.info.snapshot_complete = false;
        result.info.lkg_snapshot_complete = true;
        result.info.lkg_next_cycle = Some(0);
        result.info.lkg_leader_epoch = Some(0);
        result.info.lkg_last_update = result.info.last_update;
        result.info.lkg_scan_plan_digest = Some(TEST_PLAN_DIGEST);
    }
    let (observed_lkg, _) = observational_data_usage_info(&results, &sources, &all_buckets, &[], TEST_PLAN_DIGEST, 1, 1)
        .expect("LKG snapshots retain the same bucket usage after leader change");
    assert_eq!(observed_lkg.buckets_usage, data_usage_info.buckets_usage);
    assert_eq!(observed_lkg.bucket_sizes, data_usage_info.bucket_sizes);
    assert!(!observed_lkg.usage_snapshot_complete);
    assert_eq!(observed_lkg.usage_snapshot_converged, Some(false));
    assert!(observed_lkg.usage_snapshot_set_states.iter().all(|state| !state.complete));
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
        completed_usage_for_scope(&[first_set, unexpected_set], &expected_sources, &all_buckets, &[], true, false, false)
            .is_none()
    );
}

#[test]
fn completed_data_usage_info_rejects_incomplete_bucket_plan() {
    let all_buckets = vec!["bucket".to_string()];
    let set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
    let expected_sources = HashSet::from([DataUsageCacheSource::new(0, 0)]);

    assert!(completed_usage_for_scope(&[set], &expected_sources, &all_buckets, &[], false, false, false).is_none());
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
fn scoped_scan_bucket_work_proof_fences_same_cycle_cache() {
    let source = DataUsageCacheSource::new(0, 0);
    let structural_plan = DataUsageScanPlanDigest([9; 32]);
    let normal_plan = scanner_bucket_work_digest(structural_plan, HealScanMode::Normal, false);
    assert_eq!(normal_plan, structural_plan, "ordinary work keeps the existing digest contract");
    for (scan_mode, full) in [(HealScanMode::Deep, false), (HealScanMode::Normal, true)] {
        let requested_plan = scanner_bucket_work_digest(structural_plan, scan_mode, full);
        assert_ne!(requested_plan, normal_plan);
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "cold".to_string(),
                next_cycle: 7,
                leader_epoch: 11,
                last_update: Some(SystemTime::UNIX_EPOCH),
                source: Some(source),
                snapshot_complete: true,
                scan_plan_digest: Some(normal_plan),
                cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace("cold", "", DataUsageEntry::default());
        assert!(cache_snapshot_is_current(&cache, "cold", source, 7, 11, normal_plan));
        assert!(matches!(
            current_cache_root_or_prepare(&mut cache, "cold", source, 7, 11, requested_plan, true),
            DataUsageCacheScanState::Prepared {
                outcome: DataUsageCachePrepareOutcome::Reset,
                ..
            }
        ));
        assert!(cache.cache.is_empty(), "different work requirements must enter a fresh walk");
        cache.replace("cold", "", DataUsageEntry::default());
        cache.info.snapshot_complete = true;
        cache.info.last_update = Some(SystemTime::UNIX_EPOCH);
        assert!(
            matches!(
                current_cache_root_or_prepare(&mut cache, "cold", source, 7, 11, requested_plan, true),
                DataUsageCacheScanState::Current(_)
            ),
            "completed matching work may satisfy the same intent retry"
        );
    }
    assert_eq!(
        scanner_bucket_work_digest(structural_plan, HealScanMode::Deep, false),
        scanner_bucket_work_digest(structural_plan, HealScanMode::Deep, true)
    );
}

#[test]
fn scoped_scan_complete_root_requires_current_coverage_from_every_set() {
    let sources = HashSet::from([DataUsageCacheSource::new(0, 0), DataUsageCacheSource::new(1, 0)]);
    let buckets = vec!["bucket".to_string()];
    let coverage = DataUsageScanPlanDigest([4; 32]);
    let scope = ScannerSnapshotScope {
        sources: &sources,
        buckets: &buckets,
        identity: ScannerSnapshotIdentity {
            cycle: 0,
            leader_epoch: 0,
            plan_digest: TEST_PLAN_DIGEST,
            coverage_digest: coverage,
            tier_registry_generation: None,
        },
    };
    for (first_coverage, second_coverage, valid) in [
        (Some(coverage), Some(coverage), true),
        (None, Some(coverage), false),
        (Some(coverage), None, false),
        (None, None, false),
        (Some(coverage), Some(DataUsageScanPlanDigest([5; 32])), false),
    ] {
        let mut first = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
        let mut second = completed_root_cache("bucket", 3, 10, DataUsageCacheSource::new(1, 0));
        first.info.scan_coverage_digest = first_coverage;
        second.info.scan_coverage_digest = second_coverage;
        assert_eq!(
            completed_data_usage_info(&[first, second], &scope, &[], true, false, false).is_some(),
            valid
        );
    }
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

#[test]
fn replication_execution_digest_is_canonical_and_tracks_statistics() {
    use rustfs_data_usage::{ReplicationAllStats, ReplicationTargetUsage};
    let execution = DataUsageScanPlanDigest([9; 32]);
    let mut cache = DataUsageCache::default();
    assert_eq!(super::cache::scanner_replication_execution_digest(execution, &cache), execution);
    let first = ReplicationTargetUsage {
        replicated_size: 7,
        replicated_count: 1,
        ..Default::default()
    };
    let second = ReplicationTargetUsage {
        pending_size: 3,
        pending_count: 1,
        ..Default::default()
    };
    cache.cache.insert(
        "photos".to_string(),
        DataUsageEntry {
            replication_stats: Some(ReplicationAllStats {
                replica_size: 0,
                replica_count: 0,
                targets: HashMap::from([("arn:a".to_string(), first.clone()), ("arn:b".to_string(), second.clone())]),
            }),
            ..Default::default()
        },
    );
    let digest = super::cache::scanner_replication_execution_digest(execution, &cache);
    assert_ne!(digest, execution);
    let stats = cache
        .cache
        .get_mut("photos")
        .expect("cached bucket")
        .replication_stats
        .as_mut()
        .expect("replication stats");
    stats.targets = HashMap::from([("arn:b".to_string(), second), ("arn:a".to_string(), first)]);
    assert_eq!(super::cache::scanner_replication_execution_digest(execution, &cache), digest);
    cache
        .cache
        .get_mut("photos")
        .expect("cached bucket")
        .replication_stats
        .as_mut()
        .expect("replication stats")
        .targets
        .get_mut("arn:a")
        .expect("target stats")
        .failed_count = 1;
    assert_ne!(super::cache::scanner_replication_execution_digest(execution, &cache), digest);
}

#[path = "usage_candidate_metrics_tests.rs"]
mod usage_candidate_metrics_tests;
