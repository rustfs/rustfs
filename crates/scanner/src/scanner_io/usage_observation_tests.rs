// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{DataUsageBucketObservation, DataUsageBucketValueOrigin, DataUsageScanIdentity, DataUsageScanProgress};

fn identity() -> DataUsageScanIdentity {
    DataUsageScanIdentity {
        version: 1,
        bucket_incarnation: uuid::Uuid::from_u128(7),
        set_layout: TEST_PLAN_DIGEST,
        publication_epoch: 2,
        tier_registry_generation: 0,
        scan_mode: HealScanMode::Normal,
    }
}

fn observed_source(source: DataUsageCacheSource, objects: usize) -> DataUsageCache {
    let mut cache = completed_root_cache("bucket", objects, 20, source);
    let entry = cache.checked_flatten("bucket").expect("fixture bucket");
    cache.info.snapshot_complete = false;
    cache.info.last_update = None;
    cache.info.next_cycle = 8;
    cache.info.leader_epoch = 3;
    cache.info.tier_registry_generation = Some(0);
    cache
        .info
        .scan_bucket_incarnations
        .insert("bucket".to_string(), identity().bucket_incarnation);
    cache.info.scan_bucket_observations.insert(
        "bucket".to_string(),
        DataUsageBucketObservation {
            version: 1,
            source,
            identity: identity(),
            started_plan: DataUsageScanPlanDigest([1; 32]),
            requested_plan: DataUsageScanPlanDigest([2; 32]),
            completed_epoch: 3,
            completed_cycle: 7,
            completed_at: SystemTime::UNIX_EPOCH + Duration::from_secs(17),
            entry,
        },
    );
    cache
}

#[test]
fn companion_only_snapshot_never_exposes_mixed_values_in_primary_maps() {
    let sources = HashSet::from([DataUsageCacheSource::new(0, 0), DataUsageCacheSource::new(0, 1)]);
    let results = sources.iter().map(|source| observed_source(*source, 2)).collect::<Vec<_>>();
    let buckets = vec!["bucket".to_string()];
    let (data, _) =
        observational_data_usage_info(&results, &sources, &buckets, &[], TEST_PLAN_DIGEST, 8, 3).expect("full-source companion");
    assert!(data.is_valid_partial_snapshot());
    assert!(data.buckets_usage.is_empty());
    assert!(data.bucket_sizes.is_empty());
    assert_eq!(data.objects_total_count, 0);
    assert!(!data.usage_snapshot_complete);
    assert!(data.usage_snapshot_observational);
    let observed = &data.usage_snapshot_bucket_observations["bucket"];
    assert_eq!(observed.usage.objects_count, 4);
    assert_eq!(observed.completed_at, SystemTime::UNIX_EPOCH + Duration::from_secs(17));
    assert!(observed.usage.usage_certification.is_none());
    assert!(
        observational_data_usage_info(&results[..1], &sources, &buckets, &[], TEST_PLAN_DIGEST, 8, 3).is_none(),
        "missing source cannot become zero"
    );
    let mut mismatch = results.clone();
    mismatch[0]
        .info
        .scan_bucket_incarnations
        .insert("bucket".to_string(), uuid::Uuid::from_u128(8));
    assert!(observational_data_usage_info(&mismatch, &sources, &buckets, &[], TEST_PLAN_DIGEST, 8, 3).is_none());
}

#[test]
fn complete_and_mixed_sources_keep_the_primary_counter_unchanged() {
    let first = DataUsageCacheSource::new(0, 0);
    let second = DataUsageCacheSource::new(0, 1);
    let mut current = completed_root_cache("bucket", 3, 23, first);
    current.info.next_cycle = 8;
    current.info.leader_epoch = 3;
    current.info.tier_registry_generation = Some(0);
    current
        .info
        .scan_bucket_incarnations
        .insert("bucket".to_string(), identity().bucket_incarnation);
    current.info.scan_bucket_value_origins.insert(
        "bucket".to_string(),
        DataUsageBucketValueOrigin {
            epoch: 3,
            cycle: 8,
            completed_at: SystemTime::UNIX_EPOCH + Duration::from_secs(23),
            carried: false,
        },
    );
    let sources = HashSet::from([first, second]);
    let (data, _) = observational_data_usage_info(
        &[current, observed_source(second, 2)],
        &sources,
        &["bucket".to_string()],
        &[],
        TEST_PLAN_DIGEST,
        8,
        3,
    )
    .expect("mixed and fresh contributions");
    assert_eq!(data.buckets_usage["bucket"].objects_count, 3);
    assert_eq!(data.usage_snapshot_bucket_observations["bucket"].usage.objects_count, 5);
}

#[test]
fn mixed_observation_survives_fresh_partial_without_refreshing_its_timestamp() {
    let source = DataUsageCacheSource::new(0, 0);
    let mut cache = observed_source(source, 2);
    cache.info.name = "bucket".to_string();
    cache.info.scan_identity = Some(identity());
    cache.info.scan_progress = Some(DataUsageScanProgress {
        started_plan: TEST_PLAN_DIGEST,
        requested_plan: TEST_PLAN_DIGEST,
    });
    let completed_at = cache
        .completed_bucket_observation()
        .expect("fixture observation")
        .completed_at;
    cache.prepare_bucket_checkpoint("bucket", 9, 3, source, TEST_PLAN_DIGEST, identity());
    cache.info.last_update = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(90));
    cache.record_completed_bucket_observation();
    assert_eq!(
        cache
            .completed_bucket_observation()
            .expect("retained observation")
            .completed_at,
        completed_at
    );
    let mut recreated = identity();
    recreated.bucket_incarnation = uuid::Uuid::from_u128(99);
    cache.prepare_bucket_checkpoint("bucket", 10, 3, source, TEST_PLAN_DIGEST, recreated);
    assert!(cache.info.scan_bucket_observations.is_empty());
}

#[test]
fn later_source_turn_preserves_new_mixed_observations_and_does_not_resurrect_completed_ones() {
    let source = DataUsageCacheSource::new(0, 0);
    let old_cache = observed_source(source, 1);
    let mut prior_turn = observed_source(source, 5);
    let observation = prior_turn.info.scan_bucket_observations.get_mut("bucket").unwrap();
    observation.completed_cycle = 8;
    observation.completed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(80);
    let mut continuation = ScannerSourceTurnContinuation {
        cache: prior_turn,
        completed_buckets: HashSet::new(),
    };
    let incarnations = HashMap::from([("bucket".to_string(), identity().bucket_incarnation)]);
    // The next source turn can exhaust its budget without touching this
    // bucket. The retained map must come from the earlier turn of this cycle.
    let retained = io_cache::source_turn_bucket_observations(&old_cache, Some(&continuation), source, 2, 0, Some(&incarnations));
    assert_eq!(retained["bucket"].entry.objects, 5);
    assert_eq!(retained["bucket"].completed_cycle, 8);
    assert_eq!(retained["bucket"].completed_at, SystemTime::UNIX_EPOCH + Duration::from_secs(80));
    let recreated = HashMap::from([("bucket".to_string(), uuid::Uuid::from_u128(99))]);
    assert!(
        io_cache::source_turn_bucket_observations(&old_cache, Some(&continuation), source, 2, 0, Some(&recreated)).is_empty(),
        "continuation observations must still match the current incarnation"
    );
    continuation.cache.info.scan_bucket_observations.clear();
    continuation.completed_buckets.insert("bucket".to_string());
    assert!(
        io_cache::source_turn_bucket_observations(&old_cache, Some(&continuation), source, 2, 0, Some(&incarnations)).is_empty(),
        "a fresh completion that removed the companion must not restore the old persisted one"
    );
}

#[test]
fn bucket_coverage_tracks_only_this_bucket_and_structural_identity() {
    let bucket = BucketInfo {
        name: "bucket".to_string(),
        ..Default::default()
    };
    let source = DataUsageCacheSource::new(0, 0);
    let original = scanner_single_bucket_coverage_digest(&bucket, source, Some(identity()), TEST_PLAN_DIGEST);
    let unrelated = BucketInfo {
        name: "unrelated".to_string(),
        ..Default::default()
    };
    assert_ne!(
        scanner_bucket_plan_digest(std::slice::from_ref(&bucket), TEST_PLAN_DIGEST.0),
        scanner_bucket_plan_digest(&[bucket.clone(), unrelated], TEST_PLAN_DIGEST.0)
    );
    assert_eq!(
        original,
        scanner_single_bucket_coverage_digest(&bucket, source, Some(identity()), TEST_PLAN_DIGEST)
    );
    let mut changed = identity();
    changed.publication_epoch += 1;
    assert_ne!(
        original,
        scanner_single_bucket_coverage_digest(&bucket, source, Some(changed), TEST_PLAN_DIGEST)
    );
    assert_ne!(
        original,
        scanner_single_bucket_coverage_digest(&bucket, DataUsageCacheSource::new(0, 1), Some(identity()), TEST_PLAN_DIGEST)
    );
}

#[test]
fn only_all_fresh_unshaded_bucket_values_receive_a_new_certificate() {
    use rustfs_data_usage::BucketUsageValueState;
    let source = DataUsageCacheSource::new(0, 0);
    let sources = HashSet::from([source]);
    let buckets = vec!["bucket".to_string()];
    let mut cache = completed_root_cache("bucket", 2, 20, source);
    cache
        .info
        .scan_bucket_incarnations
        .insert("bucket".to_string(), identity().bucket_incarnation);
    cache.info.scan_bucket_value_origins.insert(
        "bucket".to_string(),
        DataUsageBucketValueOrigin {
            epoch: 0,
            cycle: 0,
            completed_at: SystemTime::UNIX_EPOCH + Duration::from_secs(20),
            carried: false,
        },
    );
    let scope = ScannerSnapshotScope {
        sources: &sources,
        buckets: &buckets,
        identity: ScannerSnapshotIdentity {
            cycle: 0,
            leader_epoch: 0,
            plan_digest: TEST_PLAN_DIGEST,
            coverage_digest: TEST_COVERAGE_DIGEST,
            tier_registry_generation: None,
        },
    };
    let build =
        |value: DataUsageCache| completed_usage_candidate(&[value], &scope, &[], true, false).expect("complete candidate");
    let (certified, _) = build(cache.clone()).prepare(ScannerCycleStatus::Complete, &ScannerCyclePendingBuckets::new());
    let proof = certified.buckets_usage["bucket"]
        .usage_certification
        .expect("fresh certificate");
    assert_eq!(
        certified.buckets_usage["bucket"].usage_value_state,
        Some(BucketUsageValueState::Certified)
    );
    let (pending, _) =
        build(cache.clone()).prepare(ScannerCycleStatus::Complete, &ScannerCyclePendingBuckets::from(["bucket".to_string()]));
    assert_eq!(pending.buckets_usage["bucket"].usage_value_state, Some(BucketUsageValueState::Observed));
    assert!(pending.buckets_usage["bucket"].usage_certification.is_none());
    assert_eq!(
        pending.buckets_usage["bucket"].objects_count, 2,
        "ordinary complete-pending keeps its canonical counter"
    );
    cache
        .info
        .scan_bucket_value_origins
        .get_mut("bucket")
        .expect("origin")
        .carried = true;
    let mut carried = build(cache.clone());
    carried.retain_carried_certifications(&certified);
    let (carried, _) = carried.prepare(ScannerCycleStatus::Complete, &ScannerCyclePendingBuckets::from(["bucket".to_string()]));
    assert_eq!(carried.buckets_usage["bucket"].usage_value_state, Some(BucketUsageValueState::Carried));
    assert_eq!(carried.buckets_usage["bucket"].usage_certification, Some(proof));
    cache.info.scan_bucket_value_origins.clear();
    let (legacy, _) = build(cache).prepare(ScannerCycleStatus::Complete, &ScannerCyclePendingBuckets::new());
    assert!(
        legacy.buckets_usage["bucket"].usage_certification.is_none(),
        "legacy data remains readable without acquiring a certificate"
    );
}

#[test]
fn usage_repeated_carried_and_mixed_rounds_preserve_value_provenance() {
    use rustfs_data_usage::BucketUsageValueState;

    let first = DataUsageCacheSource::new(0, 0);
    let second = DataUsageCacheSource::new(0, 1);
    let sources = HashSet::from([first, second]);
    let buckets = vec!["bucket".to_string()];
    let mut caches = [first, second]
        .into_iter()
        .map(|source| {
            let mut cache = completed_root_cache("bucket", 2, 20, source);
            cache.info.next_cycle = 8;
            cache.info.leader_epoch = 2;
            cache.info.tier_registry_generation = Some(0);
            cache
                .info
                .scan_bucket_incarnations
                .insert("bucket".into(), identity().bucket_incarnation);
            cache.info.scan_bucket_value_origins.insert(
                "bucket".into(),
                DataUsageBucketValueOrigin {
                    epoch: 2,
                    cycle: 8,
                    completed_at: SystemTime::UNIX_EPOCH + Duration::from_secs(20),
                    carried: false,
                },
            );
            cache
        })
        .collect::<Vec<_>>();
    let build = |values: &[DataUsageCache], cycle| {
        completed_usage_candidate(
            values,
            &ScannerSnapshotScope {
                sources: &sources,
                buckets: &buckets,
                identity: ScannerSnapshotIdentity {
                    cycle,
                    leader_epoch: 2,
                    plan_digest: TEST_PLAN_DIGEST,
                    coverage_digest: TEST_COVERAGE_DIGEST,
                    tier_registry_generation: Some(0),
                },
            },
            &[],
            true,
            false,
        )
        .expect("all physical source identities are present")
    };
    let (mut previous, _) = build(&caches, 8).prepare(ScannerCycleStatus::Complete, &ScannerCyclePendingBuckets::new());
    let certificate = previous.buckets_usage["bucket"]
        .usage_certification
        .expect("fresh whole-bucket certificate");
    let mut carried_cycles = Vec::new();
    for cycle in 9..=11 {
        assert_eq!(
            scanner_bucket_scan_status(false, false, false, true),
            ScannerBucketScanStatus::PartialCarried
        );
        for cache in &mut caches {
            cache.info.next_cycle = cycle;
            cache.info.last_update = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(cycle * 10));
            cache.info.scan_bucket_value_origins.get_mut("bucket").unwrap().carried = true;
        }
        let mut candidate = build(&caches, cycle);
        candidate.retain_carried_certifications(&previous);
        let (next, _) =
            candidate.prepare(ScannerCycleStatus::Complete, &ScannerCyclePendingBuckets::from(["bucket".to_string()]));
        assert_eq!(next.buckets_usage["bucket"].usage_value_state, Some(BucketUsageValueState::Carried));
        assert_eq!(next.buckets_usage["bucket"].usage_certification, Some(certificate));
        assert_ne!(next.last_update, previous.last_update, "aggregation time advances independently");
        carried_cycles.push(next.scanner_cycle.unwrap());
        previous = next;
    }

    let mut observed = observed_source(second, 2);
    let observed_at = observed.info.scan_bucket_observations["bucket"].completed_at;
    let mut observed_times = Vec::new();
    let mut rejected_certificates = 0;
    for cycle in 12..=14 {
        for cache in &mut caches {
            cache.info.next_cycle = cycle;
        }
        let current_origin = caches[0].info.scan_bucket_value_origins.get_mut("bucket").unwrap();
        current_origin.carried = false;
        current_origin.cycle = cycle;
        current_origin.completed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(cycle * 10);
        // The numerical aggregate deliberately still equals the old certified
        // value: equality alone must not certify mixed source generations.
        let mut mixed = build(&caches, cycle);
        mixed.retain_carried_certifications(&previous);
        let (mixed, _) = mixed.prepare(ScannerCycleStatus::Complete, &ScannerCyclePendingBuckets::from(["bucket".to_string()]));
        assert_eq!(
            mixed.buckets_usage["bucket"].objects_count,
            previous.buckets_usage["bucket"].objects_count
        );
        assert!(mixed.buckets_usage["bucket"].usage_certification.is_none());
        assert_ne!(mixed.buckets_usage["bucket"].usage_value_state, Some(BucketUsageValueState::Certified));
        rejected_certificates += 1;

        observed.info.name = "bucket".into();
        observed.info.scan_identity = Some(identity());
        observed.info.scan_progress = Some(DataUsageScanProgress {
            started_plan: TEST_PLAN_DIGEST,
            requested_plan: TEST_PLAN_DIGEST,
        });
        observed.prepare_bucket_checkpoint("bucket", cycle, 3, second, TEST_PLAN_DIGEST, identity());
        observed.info.last_update = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(cycle * 10));
        observed.record_completed_bucket_observation();
        assert_eq!(observed.completed_bucket_observation().unwrap().completed_at, observed_at);
        assert!(observed.info.scan_plan_digest.is_none(), "a bucket checkpoint is not a source root");
        let mut observed_root = observed_source(second, 2);
        observed_root.info.next_cycle = cycle;
        observed_root.info.scan_bucket_observations = crate::scanner_io::io_cache::source_turn_bucket_observations(
            &observed,
            None,
            second,
            identity().publication_epoch,
            identity().tier_registry_generation,
            Some(&observed_root.info.scan_bucket_incarnations),
        );
        let mut current = caches[0].clone();
        current.info.leader_epoch = 3;
        let (aggregate, _) =
            observational_data_usage_info(&[current, observed_root], &sources, &buckets, &[], TEST_PLAN_DIGEST, cycle, 3)
                .expect("complete mixed observation from both sources");
        let observation = aggregate
            .usage_snapshot_bucket_observations
            .get("bucket")
            .expect("validated source roots should retain the mixed observation");
        assert_eq!(observation.completed_at, observed_at);
        assert!(observation.usage.usage_certification.is_none());
        observed_times.push(
            observation
                .completed_at
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
    }
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-09", "assertion_id": "USAGE-09-A02", "status": "PASS",
            "evidence": {"carried_cycles": carried_cycles, "retained_certificate": certificate, "mixed_completed_times": observed_times}
        })
    );
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-09", "assertion_id": "USAGE-09-A03", "status": "PASS",
            "evidence": {"source_count": sources.len(), "numerically_equal_mixed_values_without_inherited_certificate": rejected_certificates}
        })
    );
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-09", "status": "PASS", "scope": "process-local"})
    );
}
