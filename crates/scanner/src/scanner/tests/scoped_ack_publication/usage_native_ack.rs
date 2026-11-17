// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::scanner_io::{acknowledge_scoped_dirty_usage, scanner_activity_epoch};
use crate::storage_api::scan::ecstore_scanner_ack_fence;
use crate::storage_api::scanner_io::ScannerUsageWatchRequest as EcstoreUsageWatchRequest;

async fn scoped_ack_after_real_post_walk_mutations() -> serde_json::Value {
    let (directory, store) = candidate_store().await;
    let incarnation = store
        .bucket_incarnation_id_from_disk(PROOF_BUCKET)
        .await
        .expect("physical bucket identity");
    let mut request = EcstoreUsageWatchRequest {
        bucket: PROOF_BUCKET.into(),
        bucket_incarnation: incarnation,
        watch_id: None,
        release: false,
        session_id: None,
    };
    let original = store
        .scanner_usage_watch(std::slice::from_ref(&request))
        .await
        .remove(0)
        .known
        .expect("initial clean watch");
    assert_eq!(original.inflight, 0);
    request.watch_id = Some(original.watch_id);
    request.session_id = Some(original.session_id);
    let first_generation = crate::scanner_io::dirty_usage_buckets_for_tests()[PROOF_BUCKET];
    let (first_scan, first_candidate) = complete_candidate(&store, PROOF_CYCLE).await;
    assert_eq!(first_candidate.buckets_usage[PROOF_BUCKET].objects_count, 1);
    let first_bytes = serde_json::to_vec(&first_candidate).unwrap();
    assert_eq!(
        publish_candidate(&store, &first_scan, first_candidate, None).await.outcome(),
        DataUsagePersistOutcome::Saved
    );
    assert_eq!(read_root(&store).await.0.as_deref(), Some(first_bytes.as_slice()));
    // A lost delivery has no receiver-side effect, even with an exact root.
    assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests()[PROOF_BUCKET], first_generation);
    let mut retained = Vec::new();
    let mut revisions = vec![original.revision];
    let mut prior_generations = vec![first_generation];
    let mut stale_deliveries = 0;
    for round in 0..2 {
        store
            .put_object(
                PROOF_BUCKET,
                &format!("post-walk-{round}"),
                &mut PutObjReader::from_vec(b"later".to_vec()),
                &ObjectOptions {
                    write_completion: WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect("actual producer mutation after a sealed scan");
        settle_namespace_commits(&store).await;
        crate::scanner_io::record_dirty_usage_bucket(PROOF_BUCKET);
        let current_generation = crate::scanner_io::dirty_usage_buckets_for_tests()[PROOF_BUCKET];
        assert!(prior_generations.iter().all(|old| *old < current_generation));
        let changed = store
            .scanner_usage_watch(std::slice::from_ref(&request))
            .await
            .remove(0)
            .known
            .expect("same live token");
        assert_eq!(changed.watch_id, original.watch_id);
        assert!(changed.revision > *revisions.last().unwrap());
        assert!(!changed.unchanged_since(&original));
        revisions.push(changed.revision);
        {
            let guard = ecstore_scanner_ack_fence(PROOF_BUCKET, incarnation, store.id)
                .await
                .expect("receiver incarnation fence");
            // Deliver the old ACKs in reverse order, then duplicate each one.
            for generation in prior_generations
                .iter()
                .rev()
                .flat_map(|generation| [*generation, *generation])
            {
                stale_deliveries += 1;
                assert_eq!(
                    acknowledge_scoped_dirty_usage(scanner_activity_epoch(), &[(&guard, generation)], false),
                    Ok(0)
                );
                assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests()[PROOF_BUCKET], current_generation);
            }
        }
        retained.push(current_generation);
        if round == 0 {
            let (scan, candidate) = complete_candidate(&store, PROOF_CYCLE + 1).await;
            assert_eq!(candidate.buckets_usage[PROOF_BUCKET].objects_count, 2);
            assert_eq!(
                publish_candidate(&store, &scan, candidate, None).await.outcome(),
                DataUsagePersistOutcome::Saved
            );
            let guard = ecstore_scanner_ack_fence(PROOF_BUCKET, incarnation, store.id)
                .await
                .expect("fresh root ACK fence");
            assert_eq!(
                acknowledge_scoped_dirty_usage(scanner_activity_epoch(), &[(&guard, current_generation)], false),
                Ok(1)
            );
            assert_eq!(
                acknowledge_scoped_dirty_usage(scanner_activity_epoch(), &[(&guard, current_generation)], false),
                Ok(0)
            );
            assert!(!crate::scanner_io::dirty_usage_buckets_pending());
            prior_generations.push(current_generation);
        }
        let after_ack = store
            .scanner_usage_watch(std::slice::from_ref(&request))
            .await
            .remove(0)
            .known
            .expect("ACK retains token");
        assert!(
            after_ack.unchanged_since(&changed),
            "ACK cannot reset a mutation revision into the old clean interval"
        );
    }
    assert!(crate::scanner_io::dirty_usage_buckets_pending());
    let evidence = serde_json::json!({
        "durable_roots": 2, "post_walk_physical_writes": 2, "original_watch_revision": original.revision,
        "watch_revisions": revisions, "retained_new_generations": retained,
        "duplicate_and_reverse_order_deliveries": stale_deliveries, "lost_first_delivery_retained": true,
    });
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    drop(first_scan);
    drop(store);
    let path = directory.path().to_path_buf();
    directory.close().expect("remove scoped ACK physical fixture");
    assert!(!path.exists());
    evidence
}

#[tokio::test]
#[serial]
async fn usage_ack_aba_does_not_restore_an_old_clean_watch() {
    let evidence = scoped_ack_after_real_post_walk_mutations().await;
    println!(
        "usage_assertion: {}",
        serde_json::json!({"case_id": "USAGE-12", "assertion_id": "USAGE-12-A08", "status": "PASS", "evidence": evidence})
    );
    println!("usage_cleanup: {}", serde_json::json!({"case_id": "USAGE-12", "status": "PASS"}));
}

#[tokio::test]
#[serial]
async fn usage_duplicate_lost_and_reordered_ack_retains_post_walk_mutations() {
    let evidence = scoped_ack_after_real_post_walk_mutations().await;
    println!(
        "usage_assertion: {}",
        serde_json::json!({"case_id": "USAGE-14", "assertion_id": "USAGE-14-A01", "status": "PASS", "evidence": evidence})
    );
    println!("usage_cleanup: {}", serde_json::json!({"case_id": "USAGE-14", "status": "PASS"}));
}
