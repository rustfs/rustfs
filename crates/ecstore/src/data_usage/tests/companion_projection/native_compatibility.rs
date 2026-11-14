// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Freeze the pre-companion reader's primary fields. Serde's unknown-field
// behavior models the existing JSON reader without importing the new schema.
#[derive(serde::Deserialize)]
struct LegacyPrimarySnapshot {
    buckets_usage: HashMap<String, LegacyPrimaryBucket>,
    bucket_sizes: HashMap<String, u64>,
    objects_total_count: u64,
    objects_total_size: u64,
    #[serde(default)]
    usage_snapshot_complete: bool,
}

#[derive(serde::Deserialize)]
struct LegacyPrimaryBucket {
    size: u64,
    objects_count: u64,
}

#[test]
fn usage_legacy_reader_never_promotes_companion_values_into_primary_lkg() {
    let incarnation = uuid::Uuid::from_u128(31);
    let mut snapshot = companion_snapshot("bucket", incarnation);
    let original_certificate = snapshot.buckets_usage["bucket"].usage_certification;
    let mut decoded_sizes = Vec::new();
    for cycle in 10..=12 {
        snapshot.scanner_cycle = Some(cycle);
        let identity = snapshot.snapshot_identity();
        snapshot
            .usage_snapshot_bucket_observations
            .get_mut("bucket")
            .unwrap()
            .snapshot_identity = identity;
        let bytes = serde_json::to_vec(&snapshot).expect("production persisted snapshot encoding");
        let old: LegacyPrimarySnapshot = serde_json::from_slice(&bytes).expect("old reader tolerates additive fields");
        assert!(old.usage_snapshot_complete);
        assert_eq!(old.buckets_usage["bucket"].size, 100);
        assert_eq!(old.buckets_usage["bucket"].objects_count, 10);
        assert_eq!(old.bucket_sizes["bucket"], 100);
        assert_eq!(old.objects_total_size, 100);
        assert_eq!(old.objects_total_count, 10);
        let current = parse_usage_snapshot(&bytes).expect("current reader accepts persisted primary plus companion");
        assert_eq!(current.buckets_usage["bucket"].usage_certification, original_certificate);
        assert_eq!(current.usage_snapshot_bucket_observations["bucket"].usage.size, 250);
        decoded_sizes.push(old.buckets_usage["bucket"].size);
    }

    snapshot.buckets_usage.clear();
    snapshot.bucket_sizes.clear();
    snapshot.buckets_count = 0;
    snapshot.calculate_totals();
    snapshot.usage_snapshot_complete = false;
    snapshot.usage_snapshot_partial = true;
    snapshot.usage_snapshot_observational = true;
    snapshot.usage_snapshot_set_states[0].complete = false;
    let bytes = serde_json::to_vec(&snapshot).expect("persisted companion-only encoding");
    let old: LegacyPrimarySnapshot = serde_json::from_slice(&bytes).expect("old reader handles the absent baseline");
    assert!(old.buckets_usage.is_empty() && old.bucket_sizes.is_empty());
    assert!(!old.usage_snapshot_complete);
    assert_eq!((old.objects_total_count, old.objects_total_size), (0, 0));
    let current = parse_usage_snapshot(&bytes).expect("valid current observational snapshot");
    assert!(current.is_valid_partial_snapshot());
    assert_eq!(current.usage_snapshot_bucket_observations["bucket"].usage.size, 250);

    assert_eq!(
        snapshot.project_bucket_observations_for_admin(&HashMap::from([("bucket".into(), incarnation.to_string())])),
        1
    );
    let response_bytes = serde_json::to_vec(&snapshot).expect("admin response encoding");
    assert!(
        parse_usage_snapshot(&response_bytes).is_err(),
        "a projected response cannot be written as a baseline"
    );
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-16", "assertion_id": "USAGE-16-A01", "status": "PASS",
            "evidence": {
                "wire_format": "production JSON, frozen legacy named primary fields",
                "legacy_primary_sizes_across_rounds": decoded_sizes,
                "new_companion_size": 250,
                "companion_only_legacy_primary_count": old.buckets_usage.len(),
                "companion_only_legacy_complete": old.usage_snapshot_complete,
                "projected_response_rejected_by_persistence_parser": true
            }
        })
    );
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-16", "status": "PASS", "scope": "process-local"})
    );
}
