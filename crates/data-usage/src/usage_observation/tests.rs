// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::DataUsageSnapshotSetState;
use std::time::Duration;

const INCARNATION: &str = "11111111-1111-4111-8111-111111111111";

fn time(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

fn usage(size: u64) -> BucketUsageInfo {
    BucketUsageInfo {
        size,
        objects_count: size,
        versions_count: size,
        bucket_incarnation: Some(INCARNATION.to_string()),
        ..Default::default()
    }
}

fn snapshot() -> DataUsageInfo {
    let mut main = usage(10);
    main.certify(BucketUsageCertification {
        epoch: 2,
        cycle: 3,
        certified_at: time(30),
    });
    main.mark_carried();
    let mut snapshot = DataUsageInfo {
        last_update: Some(time(90)),
        scanner_epoch: Some(2),
        scanner_cycle: Some(9),
        buckets_count: 1,
        buckets_usage: HashMap::from([("bucket".to_string(), main)]),
        bucket_sizes: HashMap::from([("bucket".to_string(), 10)]),
        usage_snapshot_complete: true,
        usage_snapshot_converged: Some(false),
        usage_snapshot_pending_buckets: vec!["bucket".to_string()],
        usage_snapshot_pending_bucket_count: 1,
        usage_snapshot_set_states: (0..2)
            .map(|set_index| DataUsageSnapshotSetState {
                pool_index: 0,
                set_index,
                scanner_epoch: Some(2),
                scanner_cycle: Some(9),
                scan_plan_digest: Some([1; 32]),
                complete: true,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    snapshot.calculate_totals();
    let mut observed = usage(25);
    observed.mark_observed();
    let observation = DataUsageBucketObservation {
        version: DATA_USAGE_BUCKET_OBSERVATION_VERSION,
        usage: observed,
        snapshot_identity: snapshot.snapshot_identity(),
        completed_at: time(40),
        sources: (0..2)
            .map(|set_index| DataUsageBucketObservationSource {
                pool_index: 0,
                set_index,
                bucket_incarnation: INCARNATION.to_string(),
                scanner_epoch: 2,
                scanner_cycle: 4 + set_index,
                scan_plan_digest: [1; 32],
                started_plan: [2; 32],
                requested_plan: [3; 32],
                completed_at: time(40 + set_index),
            })
            .collect(),
    };
    snapshot
        .usage_snapshot_bucket_observations
        .insert("bucket".to_string(), observation);
    snapshot
}

fn incarnations() -> HashMap<String, String> {
    HashMap::from([("bucket".to_string(), INCARNATION.to_string())])
}

#[test]
fn provenance_is_additive_and_unknown_states_do_not_keep_certificates() {
    let legacy = usage(10);
    let bytes = serde_json::to_vec(&legacy).expect("encode a legacy bucket without provenance");
    let text = std::str::from_utf8(&bytes).expect("JSON is UTF-8");
    assert!(!text.contains("usage_value_state"));
    assert!(!text.contains("usage_certification"));
    let decoded: BucketUsageInfo = serde_json::from_slice(&bytes).expect("read legacy bucket");
    assert_eq!(decoded.usage_value_state, None);
    assert_eq!(decoded.usage_certification, None);

    let mut wire = serde_json::to_value(legacy).expect("encode future bucket fixture");
    wire["usage_value_state"] = serde_json::json!("future_state");
    wire["usage_certification"] =
        serde_json::json!({"epoch":2,"cycle":3,"certified_at":{"secs_since_epoch":30,"nanos_since_epoch":0}});
    let mut future: BucketUsageInfo = serde_json::from_value(wire).expect("unknown diagnostic enum remains readable");
    future.normalize_usage_provenance();
    assert_eq!(future.usage_value_state, Some(BucketUsageValueState::Unknown));
    assert_eq!(future.usage_certification, None);
}

#[test]
fn carrying_preserves_time_but_adding_a_source_invalidates_the_certificate() {
    let mut bucket = snapshot().buckets_usage.remove("bucket").expect("canonical bucket");
    bucket.mark_carried();
    assert_eq!(bucket.usage_certification.map(|certification| certification.certified_at), Some(time(30)));
    bucket.merge(&usage(1));
    assert_eq!(bucket.size, 11);
    assert_eq!(bucket.usage_value_state, Some(BucketUsageValueState::Observed));
    assert_eq!(bucket.usage_certification, None);
}

#[test]
fn direct_object_accumulation_does_not_reuse_the_old_certificate() {
    let mut simple = snapshot();
    simple.add_object("bucket/object", &rustfs_filemeta::MetaObject::default());
    assert_eq!(simple.buckets_usage["bucket"].usage_value_state, Some(BucketUsageValueState::Observed));
    assert_eq!(simple.buckets_usage["bucket"].usage_certification, None);
    assert!(simple.usage_snapshot_bucket_observations.is_empty());

    let mut versioned = snapshot();
    versioned.add_object_from_file_meta("bucket/object", &rustfs_filemeta::FileMeta::default());
    assert_eq!(versioned.buckets_usage["bucket"].usage_value_state, Some(BucketUsageValueState::Observed));
    assert_eq!(versioned.buckets_usage["bucket"].usage_certification, None);
    assert!(versioned.usage_snapshot_bucket_observations.is_empty());
}

#[test]
fn future_or_malformed_companions_do_not_break_the_canonical_reader() {
    let canonical = snapshot();
    for entry in [
        serde_json::json!({"version": 2, "future_payload": {"new": true}}),
        serde_json::json!({"version": 1, "usage": "broken"}),
    ] {
        let mut wire = serde_json::to_value(&canonical).expect("canonical JSON");
        wire["usage_snapshot_bucket_observations"]["bucket"] = entry;
        let decoded: DataUsageInfo = serde_json::from_value(wire).expect("diagnostics cannot break canonical data");
        assert_eq!(decoded.buckets_usage, canonical.buckets_usage);
        assert!(decoded.usage_snapshot_bucket_observations.is_empty());
        assert!(decoded.is_complete_bucket_usage_snapshot());
    }
}

#[test]
fn complete_companion_projects_only_the_admin_copy_and_keeps_measurement_times() {
    let canonical = snapshot();
    let canonical_bytes = serde_json::to_vec(&canonical).expect("encode canonical candidate");
    let mut response = canonical.clone();
    assert_eq!(response.project_bucket_observations_for_admin(&incarnations()), 1);
    assert_eq!(response.buckets_usage["bucket"].size, 25);
    assert_eq!(response.buckets_usage["bucket"].usage_value_state, Some(BucketUsageValueState::Observed));
    assert_eq!(response.buckets_usage["bucket"].usage_certification, None);
    assert_eq!(response.objects_total_size, 25);
    assert_eq!(response.bucket_sizes["bucket"], 25);
    assert_eq!(response.last_update, canonical.last_update);
    assert_eq!(response.usage_snapshot_bucket_observations["bucket"].completed_at, time(40));
    assert!(response.usage_snapshot_admin_projection);
    assert!(response.usage_snapshot_partial);
    assert!(response.usage_snapshot_observational);
    assert_eq!(response.usage_snapshot_converged, Some(false));
    assert!(!response.is_complete_bucket_usage_snapshot());
    assert!(
        !response.is_valid_partial_snapshot(),
        "response-only values cannot be a storage candidate"
    );
    assert_eq!(serde_json::to_vec(&canonical).expect("reencode original"), canonical_bytes);
    assert_eq!(canonical.buckets_usage["bucket"].size, 10);
    assert_eq!(
        canonical.buckets_usage["bucket"]
            .usage_certification
            .map(|certificate| certificate.certified_at),
        Some(time(30))
    );
    assert_eq!(response.project_bucket_observations_for_admin(&incarnations()), 0);
}

#[test]
fn invalid_companions_preserve_the_canonical_bucket() {
    for invalid in 0..11 {
        let mut candidate = snapshot();
        let observation = candidate
            .usage_snapshot_bucket_observations
            .get_mut("bucket")
            .expect("companion");
        match invalid {
            0 => observation.version += 1,
            1 => observation.snapshot_identity.scanner_cycle = Some(8),
            2 => {
                observation.sources.pop();
            }
            3 => observation.sources[1].set_index = 0,
            4 => observation.sources[1].scanner_cycle = 10,
            5 => observation.sources[1].scan_plan_digest = [9; 32],
            6 => observation.sources[1].bucket_incarnation = "other".to_string(),
            7 => observation.completed_at = time(41),
            8 => observation.usage.certify(BucketUsageCertification {
                epoch: 2,
                cycle: 9,
                certified_at: time(90),
            }),
            9 => candidate.usage_snapshot_set_states[1].scanner_cycle = None,
            10 => {
                candidate
                    .buckets_usage
                    .get_mut("bucket")
                    .expect("canonical")
                    .bucket_incarnation = Some("recreated".to_string())
            }
            _ => unreachable!("bounded fixture variants"),
        }
        let before = candidate.clone();
        assert_eq!(
            candidate.project_bucket_observations_for_admin(&incarnations()),
            0,
            "invalid case {invalid}"
        );
        assert_eq!(candidate, before, "invalid case {invalid} changed canonical data");
    }
}

#[test]
fn a_fresh_set_can_contribute_without_refreshing_an_older_mixed_set() {
    let mut candidate = snapshot();
    let observation = candidate
        .usage_snapshot_bucket_observations
        .get_mut("bucket")
        .expect("companion");
    observation.sources[1].started_plan = observation.sources[1].requested_plan;
    observation.sources[1].completed_at = time(90);
    assert_eq!(candidate.project_bucket_observations_for_admin(&incarnations()), 1);
    assert_eq!(candidate.usage_snapshot_bucket_observations["bucket"].completed_at, time(40));
}

#[test]
fn companion_only_partial_keeps_legacy_maps_empty_until_admin_projection() {
    let mut candidate = snapshot();
    candidate.buckets_usage.clear();
    candidate.bucket_sizes.clear();
    candidate.buckets_count = 0;
    candidate.calculate_totals();
    candidate.usage_snapshot_complete = false;
    candidate.usage_snapshot_partial = true;
    candidate.usage_snapshot_observational = true;
    for state in &mut candidate.usage_snapshot_set_states {
        state.complete = false;
    }
    assert!(candidate.is_valid_partial_snapshot());
    let wire = serde_json::to_value(&candidate).expect("encode companion-only observation");
    assert_eq!(wire["buckets_usage"], serde_json::json!({}));
    assert_eq!(wire["bucket_sizes"], serde_json::json!({}));
    assert_eq!(wire["objects_total_size"], 0);
    assert_eq!(candidate.project_bucket_observations_for_admin(&incarnations()), 1);
    assert_eq!(candidate.objects_total_size, 25);
    assert!(candidate.usage_snapshot_partial);
    assert!(!candidate.usage_snapshot_complete);
}

#[test]
fn projection_rejects_counter_overflow_without_partial_mutation() {
    let mut candidate = snapshot();
    candidate.buckets_usage.insert("cold".to_string(), usage(u64::MAX));
    candidate.buckets_count = 2;
    let before = candidate.clone();
    assert_eq!(candidate.project_bucket_observations_for_admin(&incarnations()), 0);
    assert_eq!(candidate, before);
}

#[test]
fn a_newer_canonical_certificate_and_missing_pending_prevent_old_overlay() {
    for newer_certificate in [false, true] {
        let mut candidate = snapshot();
        if newer_certificate {
            candidate
                .buckets_usage
                .get_mut("bucket")
                .expect("canonical")
                .certify(BucketUsageCertification {
                    epoch: 2,
                    cycle: 9,
                    certified_at: time(90),
                });
        } else {
            candidate.usage_snapshot_pending_buckets.clear();
            candidate.usage_snapshot_pending_bucket_count = 0;
        }
        assert_eq!(candidate.project_bucket_observations_for_admin(&incarnations()), 0);
        assert_eq!(candidate.buckets_usage["bucket"].size, 10);
    }
}

#[test]
fn provenance_and_companions_change_candidate_bytes_and_round_trip_as_maps() {
    let canonical = snapshot();
    let original = serde_json::to_vec(&canonical).expect("encode candidate");
    let mut changed = canonical.clone();
    changed
        .buckets_usage
        .get_mut("bucket")
        .expect("bucket")
        .usage_certification
        .as_mut()
        .expect("certificate")
        .certified_at = time(31);
    assert_ne!(serde_json::to_vec(&changed).expect("encode changed certificate"), original);
    let mut changed = canonical.clone();
    changed
        .usage_snapshot_bucket_observations
        .get_mut("bucket")
        .expect("observation")
        .sources[0]
        .started_plan = [8; 32];
    assert_ne!(serde_json::to_vec(&changed).expect("encode changed source"), original);
    let named = rmp_serde::to_vec_named(&canonical).expect("encode named map");
    let restored: DataUsageInfo = rmp_serde::from_slice(&named).expect("read named map with additive fields");
    assert_eq!(restored, canonical);
}
