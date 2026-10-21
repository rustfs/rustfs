// Copyright 2026 RustFS Team
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
use rustfs_utils::path::path_join_buf;

#[tokio::test]
#[serial]
async fn exhausted_budget_still_proves_peer_movement_without_namespace_authority() {
    let (_temp_dir, store) = setup_scanner_cycle_store_with_pool_count(true, 2).await;
    let pending = ecstore_hold_namespace_commit(store.as_ref());
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    budget.cancel_for_runtime();
    let generation = store.scanner_data_movement_generation();
    let result = ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None).with_remote_publication_lease_targets(vec![(
        "peer".into(),
        "session".into(),
        generation,
    )]);
    let plan = ScannerCyclePublicationPlan::for_result(&result);
    let calls = AtomicUsize::new(0);

    let proof = plan
        .await_leases(&ctx, &budget, 352, |observational| {
            calls.fetch_add(1, Ordering::Relaxed);
            store.acquire_scanner_publication_lease_with_purpose(generation, Duration::from_secs(60), observational)
        })
        .await;
    let ScannerPublicationProofWait::Ready((token, admitted_generation)) = proof else {
        panic!("a stopped walk must still prove peer movement while the leader context is alive");
    };
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(!ctx.is_cancelled());
    store
        .validate_scanner_publication_lease_with_purpose(token, admitted_generation, true)
        .await
        .expect("progress must retain a movement-only lease");
    assert!(
        store
            .validate_scanner_publication_lease_with_purpose(token, admitted_generation, false)
            .await
            .is_err(),
        "the lease must not authorize an authoritative snapshot"
    );
    assert!(store.release_scanner_publication_lease(token).await);

    let stale = plan
        .await_leases(&ctx, &budget, 352, |observational| {
            store.acquire_scanner_publication_lease_with_purpose(generation + 1, Duration::from_secs(60), observational)
        })
        .await;
    assert!(matches!(stale, ScannerPublicationProofWait::Rejected(_)));
    drop(pending);

    store.pool_meta.write().await.pools[0].decommission = Some(crate::storage_api::owner::EcstorePoolDecommissionInfo {
        failed: true,
        ..Default::default()
    });
    assert!(store.scanner_data_movement_pause_status().await.paused);
    let current_generation = store.scanner_data_movement_generation();
    let moving = plan
        .await_leases(&ctx, &budget, 352, |observational| {
            store.acquire_scanner_publication_lease_with_purpose(current_generation, Duration::from_secs(60), observational)
        })
        .await;
    assert!(
        matches!(moving, ScannerPublicationProofWait::Rejected(error) if error.to_string().contains("blocked by data movement"))
    );
}

#[tokio::test]
async fn emitted_updates_keep_budget_cancellation_and_publication_purpose() {
    for (status, expected_plan) in [
        (ScannerCycleStatus::Complete, ScannerCyclePublicationPlan::Authoritative),
        (ScannerCycleStatus::Incomplete, ScannerCyclePublicationPlan::Observational),
        (ScannerCycleStatus::Superseded, ScannerCyclePublicationPlan::Observational),
        (
            ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable),
            ScannerCyclePublicationPlan::Observational,
        ),
        (
            ScannerCycleStatus::Deferred(ScannerCycleDeferReason::DataMovement),
            ScannerCyclePublicationPlan::Authoritative,
        ),
    ] {
        let result = ScannerCycleResult::new(status, None).with_usage_update_emitted(true);
        let plan = ScannerCyclePublicationPlan::for_result(&result);
        assert_eq!(plan, expected_plan);
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let ready = plan
            .await_leases(&ctx, &budget, 352, |observational| async move { Ok::<_, String>(observational) })
            .await;
        assert!(matches!(ready, ScannerPublicationProofWait::Ready(observational) if observational == plan.is_observational()));
        budget.cancel_for_runtime();
        let calls = AtomicUsize::new(0);
        let cancelled = plan
            .await_leases(&ctx, &budget, 352, |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                async { Ok::<(), String>(()) }
            })
            .await;
        assert!(matches!(cancelled, ScannerPublicationProofWait::Cancelled));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn progress_proof_honors_parent_cancellation() {
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    ctx.cancel();
    let calls = AtomicUsize::new(0);
    let proof = ScannerCyclePublicationPlan::ProgressOnly
        .await_leases(&ctx, &budget, 352, |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Ok::<_, String>(()) }
        })
        .await;
    assert!(matches!(proof, ScannerPublicationProofWait::Cancelled));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn progress_proof_is_bounded_without_a_runtime_budget() {
    for stalled in [false, true] {
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &ctx,
            ScannerCycleBudgetConfig {
                max_objects: Some(1),
                ..Default::default()
            },
        );
        budget.record_object_scanned();
        assert!(budget.budget_elapsed());
        let calls = AtomicUsize::new(0);
        let started = Instant::now();
        let proof = ScannerCyclePublicationPlan::ProgressOnly
            .await_leases(&ctx, &budget, 352, |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                async move {
                    if stalled {
                        std::future::pending::<()>().await;
                    }
                    Err::<(), _>("scanner publication lease capacity is exhausted")
                }
            })
            .await;
        assert!(matches!(proof, ScannerPublicationProofWait::Cancelled));
        assert!(calls.load(Ordering::Relaxed) > 0);
        assert_eq!(started.elapsed(), SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT);
        assert!(!ctx.is_cancelled());
    }
}

#[tokio::test]
#[serial]
async fn coordinator_persists_budget_progress_without_usage_or_dirty_acknowledgement() {
    assert_coordinator_partial_progress(false, None, false).await;
}

#[tokio::test]
#[serial]
async fn coordinator_preserves_checkpoint_when_bucket_identity_is_unavailable() {
    assert_coordinator_partial_progress(false, None, true).await;
}

#[tokio::test]
#[serial]
async fn coordinator_persists_mixed_plan_tail_without_usage_or_dirty_acknowledgement() {
    assert_coordinator_partial_progress(true, None, false).await;
}

#[tokio::test]
#[serial]
async fn coordinator_partial_progress_requires_successful_cycle_state_save() {
    for after_commit in [false, true] {
        assert_coordinator_partial_progress(true, Some(after_commit), false).await;
    }
}

async fn assert_coordinator_partial_progress(resume_mixed_plan: bool, fail_cycle_save: Option<bool>, block_identity_read: bool) {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let bucket = "scanner-budget-progress";
    store
        .make_bucket(bucket, &crate::storage_api::scan::MakeBucketOptions::default())
        .await
        .expect("fixture bucket");
    for object in ["first", "second", "third"] {
        store.pools[0].disk_set[0]
            .put_object(
                bucket,
                object,
                &mut PutObjReader::from_vec(b"value".to_vec()),
                &ObjectOptions {
                    write_completion: WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect("fixture object");
    }
    crate::scanner_io::record_dirty_usage_bucket(bucket);
    let dirty_before = crate::scanner_io::dirty_usage_buckets_for_tests();
    let baseline = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("usage baseline");
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &ctx,
        ScannerCycleBudgetConfig {
            max_objects: Some(1),
            ..Default::default()
        },
    );
    let mut cycle = CurrentCycle {
        next: 352,
        ..Default::default()
    };
    let mut revision = DataUsageCacheRevision::Missing;
    let outcome = run_data_scanner_cycle_with_budget(
        &ctx,
        &store,
        &mut cycle,
        &mut revision,
        1,
        Arc::clone(&budget),
        ScannerCycleScheduling {
            requires_full_scan: true,
            service_cohort: None,
            scope_fallback: None,
        },
    )
    .await;
    assert!(matches!(outcome, ScannerCycleOutcome::Partial));
    assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
    assert!(budget.progress().0 > 0);
    assert!(budget.cycle_state_persisted());
    assert_eq!(cycle.next, 353);
    assert!(cycle.cycle_completed.is_empty());
    assert!(matches!(revision, DataUsageCacheRevision::Etag(_)));
    let state = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("durable cycle progress");
    let (persisted, epoch) = decode_scanner_cycle_state(&state).expect("valid cycle state");
    assert_eq!(persisted.next, 353);
    assert_eq!(epoch, 1);
    assert_eq!(
        read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("unchanged root"),
        baseline
    );
    assert!(
        read_config(store.clone(), DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str())
            .await
            .is_err()
    );
    assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);

    if block_identity_read {
        let set = store.pools[0].disk_set[0].clone();
        let cache_name = path_join_buf(&[bucket, DATA_USAGE_CACHE_NAME]);
        let mut checkpoint = DataUsageCache::default();
        let revisions = checkpoint
            .load_with_revisions(set.clone(), &cache_name)
            .await
            .expect("load the saved checkpoint before blocking its identity read");
        assert!(checkpoint.info.scan_identity.is_some());
        assert!(checkpoint.has_scan_position());
        let before = serde_json::to_value(&checkpoint).expect("serialize the existing checkpoint");
        let identity_key = path_join_buf(&[crate::BUCKET_META_PREFIX, bucket, ".bucket-incarnation"]);
        let lock = store
            .new_ns_lock(RUSTFS_META_BUCKET, &identity_key)
            .await
            .expect("bucket identity object lock");
        let guard = lock
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("hold the identity sidecar unavailable to scanner reads");
        let blocked_budget = ScannerCycleBudget::new_with_progress_tracking(
            &ctx,
            ScannerCycleBudgetConfig {
                max_objects: Some(1),
                ..Default::default()
            },
        );
        let blocked_outcome = tokio::time::timeout(
            Duration::from_secs(60),
            run_data_scanner_cycle_with_budget(
                &ctx,
                &store,
                &mut cycle,
                &mut revision,
                1,
                Arc::clone(&blocked_budget),
                ScannerCycleScheduling {
                    requires_full_scan: true,
                    service_cohort: None,
                    scope_fallback: None,
                },
            ),
        )
        .await
        .expect("blocked identity reads must finish within the existing acquisition bounds");
        assert_eq!(
            blocked_outcome,
            ScannerCycleOutcome::Failed,
            "no verified bucket work must not publish a cycle"
        );
        assert_eq!(blocked_budget.progress().0, 0, "unverified identity must not start a legacy rebuild");
        let after_revisions = checkpoint
            .load_with_revisions(set, &cache_name)
            .await
            .expect("reload the retained checkpoint while only the identity object is locked");
        assert_eq!(after_revisions, revisions, "unavailable identity must not overwrite saved progress");
        assert_eq!(serde_json::to_value(&checkpoint).expect("serialize retained checkpoint"), before);
        assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);
        drop(guard);

        let recovery_budget = ScannerCycleBudget::new_with_progress_tracking(
            &ctx,
            ScannerCycleBudgetConfig {
                max_objects: Some(1),
                ..Default::default()
            },
        );
        let recovered = run_data_scanner_cycle_with_budget(
            &ctx,
            &store,
            &mut cycle,
            &mut revision,
            1,
            Arc::clone(&recovery_budget),
            ScannerCycleScheduling {
                requires_full_scan: true,
                service_cohort: None,
                scope_fallback: None,
            },
        )
        .await;
        assert_eq!(
            recovered,
            ScannerCycleOutcome::Partial,
            "the same checkpoint must resume after identity reads recover"
        );
        assert_eq!(recovery_budget.progress().0, 1);
        checkpoint
            .load(store.pools[0].disk_set[0].clone(), &cache_name)
            .await
            .expect("reload resumed progress");
        assert!(checkpoint.info.scan_identity.is_some());
        assert!(checkpoint.has_scan_position());
        assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);
    }

    if resume_mixed_plan {
        let set = store.pools[0].disk_set[0].clone();
        let cache_name = path_join_buf(&[bucket, DATA_USAGE_CACHE_NAME]);
        let mut checkpoint = DataUsageCache::default();
        let revisions = checkpoint
            .load_with_revisions(set.clone(), &cache_name)
            .await
            .expect("load the bounded sweep's durable checkpoint");
        assert!(
            checkpoint.info.scan_checkpoint.is_some()
                || checkpoint.info.scan_resume_after.is_some()
                || checkpoint.info.scan_raw_enumeration_cursor.is_some()
                || checkpoint.info.scan_raw_enumeration_page_index.is_some()
                || checkpoint.info.scan_coverage_receipt.is_some(),
            "bounded sweep must retain a durable position: {:?}",
            checkpoint.info
        );
        let progress = checkpoint.info.scan_progress.as_mut().expect("bounded sweep has a plan");
        // A restarted process requests a new plan while retaining the old prefix.
        progress.started_plan.0[0] ^= 1;
        let epoch = checkpoint.info.scan_identity.expect("checkpoint identity").publication_epoch;
        checkpoint
            .save_with_revisions_for_epoch(set.clone(), &cache_name, &revisions, epoch)
            .await
            .expect("persist a prefix from a different process plan");

        let tail_budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, ScannerCycleBudgetConfig::default());
        // A committed-then-failed write is confirmed from durable state by the
        // save itself, so only an uncommitted fault that outlives every bounded
        // retry makes the cycle-state save fail.
        let failure_guard = fail_cycle_save.map(|after_commit| {
            if after_commit {
                super::cycle_persist_failure::fail_next_save(1, true)
            } else {
                super::cycle_persist_failure::fail_saves(1, false, SCANNER_PERSIST_CAS_RETRIES + 1)
            }
        });
        let tail_outcome = run_data_scanner_cycle_with_budget(
            &ctx,
            &store,
            &mut cycle,
            &mut revision,
            1,
            Arc::clone(&tail_budget),
            ScannerCycleScheduling {
                requires_full_scan: true,
                service_cohort: None,
                scope_fallback: None,
            },
        )
        .await;
        assert!(!tail_budget.budget_elapsed(), "this is not the budget-exhaustion fast path");
        checkpoint.load(set, &cache_name).await.expect("reload the saved mixed tail");
        assert!(!checkpoint.info.snapshot_complete);
        assert!(checkpoint.info.scan_checkpoint.is_none());
        assert!(checkpoint.info.scan_resume_after.is_none());
        let progress = checkpoint
            .info
            .scan_progress
            .expect("a fresh verification sweep is still required");
        assert_eq!(progress.started_plan, progress.requested_plan);
        assert_eq!(
            read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
                .await
                .expect("root retained"),
            baseline
        );
        assert!(
            read_config(store.clone(), DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str())
                .await
                .is_err()
        );
        assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);
        if let Some(after_commit) = fail_cycle_save {
            assert!(super::cycle_persist_failure::take_failure(1).is_none(), "injected fault must have fired");
            let state = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
                .await
                .expect("last durable cycle");
            let durable_next = decode_scanner_cycle_state(&state)
                .expect("cycle after failed acknowledgement")
                .0
                .next;
            if after_commit {
                // The write committed before the error surfaced; the save
                // confirms it from durable state under the same epoch.
                assert_eq!(tail_outcome, ScannerCycleOutcome::Partial);
                assert!(tail_budget.cycle_state_persisted());
                assert_eq!(durable_next, 354);
            } else {
                // Every bounded retry failed: progress stays unacknowledged
                // and the durable cycle does not move.
                assert_eq!(tail_outcome, ScannerCycleOutcome::StatePersistenceFailed);
                assert!(!tail_budget.cycle_state_persisted());
                assert_eq!(durable_next, 353);
            }
            drop(failure_guard);
            global_metrics().set_cycle(None).await;
            crate::scanner_io::clear_dirty_usage_buckets_for_tests();
            return;
        }
        assert_eq!(
            tail_outcome,
            ScannerCycleOutcome::Partial,
            "saved recovery progress is not a usage-save failure"
        );
        assert_eq!(cycle.next, 354);
        assert!(cycle.cycle_completed.is_empty());
        assert!(tail_budget.cycle_state_persisted());
        let state = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("durable tail cycle");
        assert_eq!(decode_scanner_cycle_state(&state).expect("tail cycle state").0.next, 354);

        let verification = run_data_scanner_cycle_with_budget(
            &ctx,
            &store,
            &mut cycle,
            &mut revision,
            1,
            ScannerCycleBudget::new_with_progress_tracking(&ctx, ScannerCycleBudgetConfig::default()),
            ScannerCycleScheduling {
                requires_full_scan: true,
                service_cohort: None,
                scope_fallback: None,
            },
        )
        .await;
        assert_eq!(verification, ScannerCycleOutcome::Completed, "only the fresh sweep certifies usage");
        let usage: DataUsageInfo = serde_json::from_slice(
            &read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
                .await
                .expect("verified root"),
        )
        .expect("verified usage JSON");
        assert_eq!((usage.objects_total_count, usage.objects_total_size), (3, 15));
        assert!(!crate::scanner_io::dirty_usage_buckets_pending());
    }
    global_metrics().set_cycle(None).await;
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[test]
fn partial_progress_does_not_mask_missing_updates_or_persistence_failure() {
    for (persisted, expected) in [
        (DataUsagePersistOutcome::NoUpdate, ScannerCycleOutcome::Partial),
        (DataUsagePersistOutcome::Failed, ScannerCycleOutcome::Failed),
        (DataUsagePersistOutcome::Current, ScannerCycleOutcome::Failed),
        (
            DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement),
            ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement),
        ),
    ] {
        let result = ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None).with_partial_bucket_progress(true);
        assert_eq!(scanner_cycle_completion_outcome_for_result(&result, persisted), expected);
    }
    for result in [
        ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None),
        ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None)
            .with_partial_bucket_progress(true)
            .with_usage_update_emitted(true),
        ScannerCycleResult::new(ScannerCycleStatus::Complete, None).with_partial_bucket_progress(true),
    ] {
        assert_eq!(
            scanner_cycle_completion_outcome_for_result(&result, DataUsagePersistOutcome::NoUpdate),
            ScannerCycleOutcome::Failed,
            "partial progress cannot stand in for an expected root update: {result:?}"
        );
    }
}
