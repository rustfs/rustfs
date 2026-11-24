// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
#[serial_test::serial]
async fn preflight_refuses_the_frozen_generation_before_acquiring_the_bucket_cache() {
    let (_directory, store) = crate::scanner_io::tests::setup_local_scanner_store(1, 4).await;
    let set = &store.pools[0].disk_set[0];
    let disk = scanner_set_disk_inventory(set).await[0].clone();
    let mut request = test_request(Uuid::new_v4());
    // Hold the same frozen view through the request, independent of global registry TTLs.
    let _cycle = crate::begin_tier_registry_cycle(request.next_cycle, request.leader_epoch);
    let actual = crate::runtime_tier_registry_for_cycle(request.next_cycle, request.leader_epoch)
        .await
        .generation;
    let expected = actual ^ 1;
    request.expected_tier_registry_generation = Some(expected);
    crate::save_config(
        store.clone(),
        crate::DATA_USAGE_BLOOM_NAME_PATH.as_str(),
        crate::scanner::encode_scanner_cycle_fence_for_test(request.next_cycle, request.leader_epoch),
    )
    .await
    .expect("persist the worker's cycle fence");
    let cache_name = path_join_buf(&[&request.bucket, DATA_USAGE_CACHE_NAME]);
    let source = cache_source_for_disk(&disk).expect("local cache source");
    let _cache_lock = acquire_scanner_cache_locks(set, &cache_name, source)
        .await
        .expect("hold the bucket cache lock throughout admission");
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, Default::default());
    let phase = Arc::new(AtomicU8::new(RemoteScannerPhase::Scanning as u8));
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        scan_and_persist_local_bucket_with_store(
            disk,
            ctx,
            budget.clone(),
            phase.clone(),
            Arc::new(Notify::new()),
            request,
            store,
        ),
    )
    .await
    .expect("registry preflight must not wait for the cache lock")
    .expect("authenticated admission can report a generation mismatch");
    let RemoteScannerFrameResult::TierRegistryMismatch(mismatch) = result else {
        panic!("expected preflight rejection, got {result:?}");
    };
    assert_eq!((mismatch.expected, mismatch.actual), (expected, actual));
    assert_eq!(budget_progress(&budget), RemoteScannerProgress::default());
    assert_eq!(remote_scanner_phase(&phase), RemoteScannerPhase::Scanning);
}

async fn read_tier_terminal(
    frame: RemoteScannerFrame,
    preflight: bool,
    valid_mac: bool,
    trailing: bool,
) -> RemoteScannerStreamResult<RemoteScannerOutcome> {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(if valid_mac { request_id } else { Uuid::new_v4() });
    let mut bytes = Vec::new();
    write_frame(&mut bytes, &writer_auth, &mut 0, &frame)
        .await
        .expect("signed terminal");
    if trailing {
        bytes.push(1);
    }
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, Default::default());
    consume_remote_scanner_stream_until(
        Cursor::new(bytes),
        ctx,
        budget,
        RemoteScannerResponseExpectation {
            supports_tier_registry_preflight: preflight,
            supports_durable_progress: false,
            supports_complete_observation: false,
            checkpoint_identity: Some(crate::DataUsageScanIdentity {
                version: 1,
                bucket_incarnation: Uuid::from_u128(7),
                set_layout: TEST_PLAN_DIGEST,
                publication_epoch: 1,
                tier_registry_generation: 0,
                scan_mode: HealScanMode::Normal,
            }),
            checkpoint_plan_digest: TEST_PLAN_DIGEST,
            leader_epoch: 9,
            bucket: "bucket",
            source: TEST_SOURCE,
            next_cycle: TEST_NEXT_CYCLE,
            scan_plan_digest: TEST_PLAN_DIGEST,
            tier_registry_generation: 0,
        },
        reader_auth,
        Instant::now() + Duration::from_secs(5),
    )
    .await
}

#[tokio::test]
async fn tier_preflight_requires_negotiation_matching_request_zero_work_and_authenticated_eof() {
    for case in [
        "valid",
        "unnegotiated",
        "echo",
        "equal",
        "started",
        "persisting",
        "mac",
        "trailing",
    ] {
        let phase = if case == "persisting" {
            RemoteScannerPhase::Persisting
        } else {
            RemoteScannerPhase::Scanning
        };
        let frame = RemoteScannerFrame::with_phase(
            RemoteScannerProgress {
                entries_visited: u64::from(case == "started"),
                ..Default::default()
            },
            phase,
            RemoteScannerFrameResult::TierRegistryMismatch(RemoteScannerTierRegistryMismatch {
                expected: if case == "echo" { 2 } else { 0 },
                actual: u64::from(case != "equal"),
            }),
        );
        let result = read_tier_terminal(frame, case != "unnegotiated", case != "mac", case == "trailing").await;
        if case == "valid" {
            assert!(matches!(
                result,
                Ok(RemoteScannerOutcome::TierRegistryMismatch { expected: 0, actual: 1 })
            ));
        } else {
            assert!(result.is_err(), "invalid preflight case {case}: {result:?}");
        }
    }
}

#[tokio::test]
async fn legacy_complete_requests_a_new_cycle_only_for_an_isolated_generation_difference() {
    for case in [
        "valid",
        "after_preflight",
        "missing",
        "scope",
        "incarnation",
        "non_flattened",
        "mac",
        "trailing",
    ] {
        let mut usage = test_usage("bucket", 1);
        usage.tier_registry_generation = (case != "missing").then_some(1);
        if case == "incarnation" {
            usage.bucket_incarnation = Some(Uuid::from_u128(99));
        }
        if case == "non_flattened" {
            usage.entry.children.insert("child".to_owned());
        }
        let plan = if case == "scope" {
            DataUsageScanPlanDigest([99; 32])
        } else {
            TEST_PLAN_DIGEST
        };
        let frame = RemoteScannerFrame::terminal(
            RemoteScannerProgress::default(),
            RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                source: TEST_SOURCE,
                scan_plan_digest: plan,
                usage,
                pending_maintenance_work: true,
            })),
        );
        let result = read_tier_terminal(frame, case == "after_preflight", case != "mac", case == "trailing").await;
        if case == "valid" {
            assert!(matches!(
                result,
                Ok(RemoteScannerOutcome::TierRegistryMismatch { expected: 0, actual: 1 })
            ));
        } else {
            assert!(result.is_err(), "invalid completion case {case}: {result:?}");
        }
    }
}
