// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn progress_identity() -> crate::DataUsageScanIdentity {
    crate::DataUsageScanIdentity {
        version: 1,
        bucket_incarnation: Uuid::from_u128(7),
        set_layout: TEST_PLAN_DIGEST,
        publication_epoch: 2,
        tier_registry_generation: 0,
        scan_mode: HealScanMode::Normal,
    }
}

fn durable_receipt() -> RemoteScannerDurableProgress {
    RemoteScannerDurableProgress {
        source: TEST_SOURCE,
        identity: progress_identity(),
        scan_plan_digest: TEST_PLAN_DIGEST,
        checkpoint_plan_digest: TEST_PLAN_DIGEST,
        leader_epoch: 9,
        next_cycle: TEST_NEXT_CYCLE,
        advanced: true,
        resource_deferred: false,
        observation: Some(Box::new(crate::DataUsageBucketObservation {
            version: 1,
            source: TEST_SOURCE,
            identity: progress_identity(),
            started_plan: DataUsageScanPlanDigest([10; 32]),
            requested_plan: TEST_PLAN_DIGEST,
            completed_epoch: 9,
            completed_cycle: TEST_NEXT_CYCLE,
            completed_at: std::time::SystemTime::UNIX_EPOCH,
            entry: crate::DataUsageEntry {
                objects: 5,
                ..Default::default()
            },
        })),
    }
}

#[tokio::test]
async fn durable_progress_requires_negotiation_exact_scope_mac_and_eof() {
    for case in [
        "valid",
        "legacy",
        "source",
        "identity",
        "epoch",
        "plan",
        "future_observation",
        "mac",
        "trailing",
    ] {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(if case == "mac" { Uuid::new_v4() } else { request_id });
        let mut receipt = durable_receipt();
        match case {
            "source" => receipt.source = DataUsageCacheSource::new(99, 99),
            "identity" => receipt.identity.bucket_incarnation = Uuid::from_u128(99),
            "epoch" => receipt.leader_epoch += 1,
            "plan" => receipt.checkpoint_plan_digest = DataUsageScanPlanDigest([99; 32]),
            "future_observation" => receipt.observation.as_mut().expect("fixture observation").completed_cycle += 1,
            _ => {}
        }
        let mut bytes = Vec::new();
        write_frame(
            &mut bytes,
            &writer_auth,
            &mut 0,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::DurableProgress(Box::new(receipt)),
            ),
        )
        .await
        .expect("signed terminal");
        if case == "trailing" {
            bytes.push(1);
        }
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, Default::default());
        let result = consume_remote_scanner_stream_until(
            std::io::Cursor::new(bytes),
            parent,
            budget,
            RemoteScannerResponseExpectation {
                bucket: "bucket",
                source: TEST_SOURCE,
                next_cycle: TEST_NEXT_CYCLE,
                leader_epoch: 9,
                scan_plan_digest: TEST_PLAN_DIGEST,
                checkpoint_plan_digest: TEST_PLAN_DIGEST,
                tier_registry_generation: 0,
                supports_durable_progress: case != "legacy",
                supports_complete_observation: case != "legacy",
                checkpoint_identity: Some(progress_identity()),
            },
            reader_auth,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
        if case == "valid" {
            let RemoteScannerOutcome::DurableProgress {
                advanced, observation, ..
            } = result.expect("validated progress")
            else {
                panic!("progress cannot become Complete")
            };
            assert!(advanced);
            assert_eq!(observation.expect("preserved companion").entry.objects, 5);
        } else {
            assert!(result.is_err(), "invalid progress case {case}");
        }
    }
}

#[test]
fn durable_progress_request_marker_is_absent_for_legacy_peers() {
    let mut request = test_request(Uuid::new_v4());
    let legacy = serde_json::to_value(&request).expect("legacy request");
    assert!(legacy.get("supports_durable_progress").is_none());
    request.supports_durable_progress = true;
    request.checkpoint_plan_digest = Some(TEST_PLAN_DIGEST);
    let bytes = rmp_serde::to_vec_named(&request).expect("extended request");
    assert!(
        decode_remote_scanner_request(&bytes)
            .expect("new request")
            .0
            .supports_durable_progress
    );
}

#[tokio::test]
async fn negotiated_progress_waits_for_the_persisted_terminal_after_budget_exhaustion() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let progress = RemoteScannerProgress {
        objects_scanned: 1,
        directories_started: 0,
        entries_visited: 1,
    };
    let mut bytes = Vec::new();
    let mut sequence = 0;
    write_frame(&mut bytes, &writer_auth, &mut sequence, &RemoteScannerFrame::progress(progress))
        .await
        .expect("progress frame");
    write_frame(
        &mut bytes,
        &writer_auth,
        &mut sequence,
        &RemoteScannerFrame::terminal(progress, RemoteScannerFrameResult::DurableProgress(Box::new(durable_receipt()))),
    )
    .await
    .expect("terminal frame");
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(1),
            ..Default::default()
        },
    );
    let outcome = consume_remote_scanner_stream_until(
        std::io::Cursor::new(bytes),
        parent,
        budget.clone(),
        RemoteScannerResponseExpectation {
            bucket: "bucket",
            source: TEST_SOURCE,
            next_cycle: TEST_NEXT_CYCLE,
            leader_epoch: 9,
            scan_plan_digest: TEST_PLAN_DIGEST,
            checkpoint_plan_digest: TEST_PLAN_DIGEST,
            tier_registry_generation: 0,
            supports_durable_progress: true,
            supports_complete_observation: true,
            checkpoint_identity: Some(progress_identity()),
        },
        reader_auth,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("the persisted terminal is drained after reported work exhausts the budget");
    assert!(budget.budget_elapsed());
    assert!(matches!(outcome, RemoteScannerOutcome::DurableProgress { advanced: true, .. }));
}

#[tokio::test]
async fn observation_only_receipt_cannot_authorize_progress_requeue() {
    for (progress, observation) in [(true, false), (false, true), (true, true)] {
        let id = Uuid::new_v4();
        let auth = FrameAuthenticator::for_test(id);
        let mut receipt = durable_receipt();
        receipt.advanced = progress;
        if !observation {
            receipt.observation = None;
        }
        let mut bytes = Vec::new();
        write_frame(
            &mut bytes,
            &auth,
            &mut 0,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::DurableProgress(Box::new(receipt)),
            ),
        )
        .await
        .expect("terminal");
        let owner = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&owner, Default::default());
        let outcome = consume_remote_scanner_stream_until(
            std::io::Cursor::new(bytes),
            owner,
            budget,
            RemoteScannerResponseExpectation {
                bucket: "bucket",
                source: TEST_SOURCE,
                next_cycle: TEST_NEXT_CYCLE,
                leader_epoch: 9,
                scan_plan_digest: TEST_PLAN_DIGEST,
                checkpoint_plan_digest: TEST_PLAN_DIGEST,
                tier_registry_generation: 0,
                supports_durable_progress: progress,
                supports_complete_observation: observation,
                checkpoint_identity: Some(progress_identity()),
            },
            FrameAuthenticator::for_test(id),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("independently negotiated receipt");
        let RemoteScannerOutcome::DurableProgress {
            advanced,
            observation: value,
            ..
        } = outcome
        else {
            panic!("only partial receipt")
        };
        assert_eq!(advanced, progress);
        assert_eq!(value.is_some(), observation);
    }
}

#[tokio::test]
async fn owner_cancellation_overrides_budget_drain() {
    let owner = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&owner, Default::default());
    budget.cancel_for_runtime();
    owner.cancel();
    let error = await_remote_scanner_read::<u8, _>(
        std::future::pending(),
        &owner,
        &budget,
        true,
        RemoteScannerPhase::Persisting,
        Instant::now() + Duration::from_secs(60),
        Instant::now() + Duration::from_secs(60),
    )
    .await
    .expect_err("owner cancellation wins even after a budget stop");
    assert!(error.error.to_string().contains("cancelled"));
}

#[tokio::test(start_paused = true)]
async fn budget_stop_does_not_restart_a_partly_decoded_frame() {
    let id = Uuid::new_v4();
    let mut bytes = Vec::new();
    write_frame(
        &mut bytes,
        &FrameAuthenticator::for_test(id),
        &mut 0,
        &RemoteScannerFrame::terminal(
            RemoteScannerProgress::default(),
            RemoteScannerFrameResult::DurableProgress(Box::new(durable_receipt())),
        ),
    )
    .await
    .expect("terminal bytes");
    let owner = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&owner, Default::default());
    let (mut writer, reader) = tokio::io::duplex(4);
    let writer_budget = budget.clone();
    let send = tokio::spawn(async move {
        writer.write_all(&bytes[..5]).await.expect("frame prefix");
        writer_budget.cancel_for_runtime();
        writer.write_all(&bytes[5..]).await.expect("remaining frame");
        writer.shutdown().await.expect("clean EOF");
    });
    let outcome = consume_remote_scanner_stream_until(
        reader,
        owner,
        budget,
        RemoteScannerResponseExpectation {
            bucket: "bucket",
            source: TEST_SOURCE,
            next_cycle: TEST_NEXT_CYCLE,
            leader_epoch: 9,
            scan_plan_digest: TEST_PLAN_DIGEST,
            checkpoint_plan_digest: TEST_PLAN_DIGEST,
            tier_registry_generation: 0,
            supports_durable_progress: true,
            supports_complete_observation: true,
            checkpoint_identity: Some(progress_identity()),
        },
        FrameAuthenticator::for_test(id),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("resume the same decoder after budget wakeup");
    assert!(matches!(outcome, RemoteScannerOutcome::DurableProgress { advanced: true, .. }));
    send.await.expect("frame writer");
}
