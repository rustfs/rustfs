// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::scanner_io::{acknowledge_scoped_dirty_usage, scanner_activity_epoch};
use crate::storage_api::owner::ObjectOperations as _;
use crate::storage_api::scan::{DeleteBucketOptions, MakeBucketOptions, ecstore_scanner_ack_fence};
use rustfs_data_usage::{DATA_USAGE_BUCKET_OBSERVATION_VERSION, DataUsageBucketObservation, DataUsageBucketObservationSource};

#[tokio::test]
#[serial]
async fn usage_recreated_bucket_rejects_old_carry_overlay_certificate_and_ack() {
    let (directory, store) = candidate_store().await;
    let old_incarnation = store
        .bucket_incarnation_id_from_disk(PROOF_BUCKET)
        .await
        .expect("original physical identity");
    let old_generation = crate::scanner_io::dirty_usage_buckets_for_tests()[PROOF_BUCKET];
    let (old_scan, mut old_candidate) = complete_candidate(&store, PROOF_CYCLE).await;
    let old_certificate = old_candidate.buckets_usage[PROOF_BUCKET]
        .usage_certification
        .expect("original certificate");
    assert_eq!(
        publish_candidate(&store, &old_scan, old_candidate.clone(), None)
            .await
            .outcome(),
        DataUsagePersistOutcome::Saved
    );
    let completed_at = old_candidate.last_update.unwrap() + Duration::from_secs(1);
    old_candidate.scanner_cycle = Some(PROOF_CYCLE + 1);
    old_candidate.last_update = Some(completed_at);
    for source in &mut old_candidate.usage_snapshot_set_states {
        source.scanner_cycle = Some(PROOF_CYCLE + 1);
    }
    old_candidate.buckets_usage.get_mut(PROOF_BUCKET).unwrap().mark_carried();
    let mut observed = old_candidate.buckets_usage[PROOF_BUCKET].clone();
    observed.size = 999;
    observed.mark_observed();
    old_candidate.usage_snapshot_converged = Some(false);
    old_candidate.usage_snapshot_pending_buckets = vec![PROOF_BUCKET.into()];
    old_candidate.usage_snapshot_pending_bucket_count = 1;
    old_candidate.usage_snapshot_bucket_observations.insert(
        PROOF_BUCKET.into(),
        DataUsageBucketObservation {
            version: DATA_USAGE_BUCKET_OBSERVATION_VERSION,
            usage: observed,
            snapshot_identity: old_candidate.snapshot_identity(),
            completed_at,
            sources: old_candidate
                .usage_snapshot_set_states
                .iter()
                .map(|source| DataUsageBucketObservationSource {
                    pool_index: source.pool_index,
                    set_index: source.set_index,
                    bucket_incarnation: old_incarnation.to_string(),
                    scanner_epoch: source.scanner_epoch.unwrap(),
                    scanner_cycle: source.scanner_cycle.unwrap(),
                    scan_plan_digest: source.scan_plan_digest.unwrap(),
                    started_plan: [1; 32],
                    requested_plan: [2; 32],
                    completed_at,
                })
                .collect(),
        },
    );
    let mut old_admin = old_candidate.clone();
    assert_eq!(
        old_admin.project_bucket_observations_for_admin(&HashMap::from([(PROOF_BUCKET.into(), old_incarnation.to_string())])),
        1,
        "the old companion is valid before recreation"
    );

    store
        .delete_object(
            PROOF_BUCKET,
            "initial",
            ObjectOptions {
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("delete the original physical object");
    settle_namespace_commits(&store).await;
    store
        .delete_bucket(
            PROOF_BUCKET,
            &DeleteBucketOptions {
                force_if_empty: true,
                ..Default::default()
            },
        )
        .await
        .expect("remove original bucket metadata");
    store
        .make_bucket(PROOF_BUCKET, &MakeBucketOptions::default())
        .await
        .expect("recreate the same name");
    store
        .put_object(
            PROOF_BUCKET,
            "successor",
            &mut PutObjReader::from_vec(b"fresh".to_vec()),
            &ObjectOptions {
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("write only successor contents");
    settle_namespace_commits(&store).await;
    crate::scanner_io::record_dirty_usage_bucket(PROOF_BUCKET);
    let current_incarnation = store
        .bucket_incarnation_id_from_disk(PROOF_BUCKET)
        .await
        .expect("successor physical identity");
    let current_generation = crate::scanner_io::dirty_usage_buckets_for_tests()[PROOF_BUCKET];
    assert_ne!(old_incarnation, current_incarnation);
    assert!(current_generation > old_generation);

    crate::scanner_io::tests::assert_incarnation_change_rejects_carry_and_old_certificate(old_incarnation, current_incarnation);
    let mut current_admin = old_candidate;
    assert_eq!(
        current_admin
            .project_bucket_observations_for_admin(&HashMap::from([(PROOF_BUCKET.into(), current_incarnation.to_string())])),
        0,
        "old companion cannot overlay the recreated bucket"
    );
    assert!(!current_admin.usage_snapshot_admin_projection);
    assert!(
        ecstore_scanner_ack_fence(PROOF_BUCKET, old_incarnation, store.id)
            .await
            .is_err(),
        "old ACK cannot acquire the current incarnation guard"
    );
    {
        let guard = ecstore_scanner_ack_fence(PROOF_BUCKET, current_incarnation, store.id)
            .await
            .expect("successor ACK fence");
        assert_eq!(
            acknowledge_scoped_dirty_usage(scanner_activity_epoch(), &[(&guard, old_generation)], false),
            Ok(0)
        );
        assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests()[PROOF_BUCKET], current_generation);
    }
    let (new_scan, new_candidate) = complete_candidate(&store, PROOF_CYCLE + 1).await;
    let successor = &new_candidate.buckets_usage[PROOF_BUCKET];
    assert_eq!(successor.bucket_incarnation.as_deref(), Some(current_incarnation.to_string().as_str()));
    assert_eq!((successor.objects_count, successor.size), (1, 5));
    assert_ne!(successor.usage_certification, Some(old_certificate));
    let evidence = serde_json::json!({
        "old_incarnation": old_incarnation, "current_incarnation": current_incarnation,
        "old_generation": old_generation, "retained_successor_generation": current_generation,
        "rejected_boundaries": ["carry", "admin-overlay", "certificate-inheritance", "ACK"],
        "successor_objects": successor.objects_count, "successor_bytes": successor.size,
        "successor_certificate": successor.usage_certification
    });
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    drop(new_scan);
    drop(old_scan);
    drop(store);
    let path = directory.path().to_path_buf();
    directory.close().expect("remove recreation fixture");
    assert!(!path.exists());
    println!(
        "usage_assertion: {}",
        serde_json::json!({"case_id": "USAGE-11", "assertion_id": "USAGE-11-A02", "status": "PASS", "evidence": evidence})
    );
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-11", "status": "PASS", "fixtures_removed": !path.exists()})
    );
}
