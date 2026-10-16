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
    assert!(read_config(store, DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str()).await.is_err());
    assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);
    global_metrics().set_cycle(None).await;
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}
