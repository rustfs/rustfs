// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
#[serial]
async fn usage_drain_lease_and_epoch_loss_reject_real_root_without_restarting_deadline() {
    // The real disk transition uses the production grace, not the 50 ms
    // test-only coordinator shortcut. Both phases retain this one deadline.
    let drain_grace = Duration::from_secs(30);
    let mut observations = Vec::new();
    for loss in ["lease", "epoch"] {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let (directory, store) = setup_scanner_cycle_store_with_pool_count(false, 2).await;
        store
            .make_bucket(PROOF_BUCKET, &crate::storage_api::scan::MakeBucketOptions::default())
            .await
            .expect("create publication fence bucket");
        store
            .put_object(
                PROOF_BUCKET,
                "initial",
                &mut PutObjReader::from_vec(b"proof".to_vec()),
                &ObjectOptions::default(),
            )
            .await
            .expect("write publication fence object");
        crate::scanner_io::record_dirty_usage_bucket(PROOF_BUCKET);
        let (scan, candidate) = complete_candidate(&store, PROOF_CYCLE).await;
        let root_before = read_root(&store).await;
        let dirty_before = crate::scanner_io::dirty_usage_buckets_for_tests();
        let epoch_before = store.scanner_data_movement_pause_status().await.operation_epoch;
        assert_eq!(scan.publication_epoch(), Some(epoch_before));
        let owner = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &owner,
            ScannerCycleBudgetConfig {
                max_objects: Some(1),
                ..Default::default()
            },
        );
        budget.record_object_scanned();
        let original_deadline = budget.drain_deadline(drain_grace).expect("one active drain");
        let lease_deadline = if loss == "lease" {
            Some(std::time::Instant::now())
        } else {
            tokio::time::timeout_at(original_deadline, store.init_rebalance_start(vec![PROOF_BUCKET.to_string()]))
                .await
                .expect("the real movement transition must fit within the original drain")
                .expect("initialize a replacement rebalance through the storage owner");
            None
        };
        let epoch_after = store.scanner_data_movement_pause_status().await.operation_epoch;
        if loss == "epoch" {
            assert!(epoch_after > epoch_before, "a real storage transition must revoke the captured epoch");
        }
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(candidate)
            .await
            .expect("enqueue the actual sealed scan candidate");
        drop(sender);
        let publication = store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch_and_lease_fence(
            owner.clone(), store.clone(), receiver, Some(PROOF_EPOCH), None,
            ScannerPublicationFence::new(scan.publication_epoch(), lease_deadline, None)
                .with_ack_expectation(scan.publication_expectation()),
            || async { None },
        ).await;
        let expected_reason = if loss == "lease" {
            ScannerCycleDeferReason::PublicationLeaseDeadlineExceeded
        } else {
            ScannerCycleDeferReason::DataMovement
        };
        assert_eq!(publication.outcome(), DataUsagePersistOutcome::Deferred(expected_reason));
        let (_, _, acknowledgements) = finalize_scanner_cycle_result(scan, publication);
        assert!(acknowledgements.is_empty());
        assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);
        assert_eq!(read_root(&store).await, root_before, "a lost fence must reject the real root write");
        budget.cancel_for_runtime();
        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
        assert_eq!(budget.drain_deadline(drain_grace), Some(original_deadline));
        assert!(
            Instant::now() <= original_deadline,
            "a rejected publication cannot consume a new grace window"
        );
        observations.push(serde_json::json!({
            "lost_fence": loss, "publication_outcome": format!("{expected_reason:?}"),
            "epoch_before": epoch_before, "epoch_after": epoch_after,
            "acknowledgements": acknowledgements.len(), "retained_dirty_buckets": dirty_before.len(),
            "remaining_original_drain_milliseconds": original_deadline.saturating_duration_since(Instant::now()).as_millis(),
        }));
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        drop(store);
        let fixture_path = directory.path().to_path_buf();
        directory.close().expect("remove publication fence physical disk fixture");
        assert!(!fixture_path.exists());
    }
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-13", "assertion_id": "USAGE-13-A04", "status": "PASS",
            "evidence": {"observed_fence_losses": observations},
        })
    );
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-13", "status": "PASS", "fixture_count_removed": 2})
    );
}
