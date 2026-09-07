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

use super::super::cycle_state::cleanup_io_fault;
use super::*;
use crate::storage_api::owner::{EcstoreRebalStatus, EcstoreRebalanceInfo, EcstoreRebalanceMeta, EcstoreRebalanceStats};

async fn seed_cleanup(store: &Arc<ECStore>, state: &str) -> ScannerCycleRecoveryMarker {
    let cycle = CurrentCycle {
        current: 3,
        next: 42,
        ..Default::default()
    };
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_NAME_PATH.as_str(),
        encode_scanner_cycle_state(&cycle, 7).expect("cycle encoding"),
    )
    .await
    .expect("persist cycle");
    let usage = DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(41),
        ..complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0)
    };
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&usage).expect("usage encoding"),
    )
    .await
    .expect("persist usage floor");
    let marker = ScannerCycleRecoveryMarker {
        schema_version: 1,
        primary_revision: "previous-primary".to_string(),
        generation: 41,
        leader_epoch: 7,
        classification: "corrupt".to_string(),
        first_detected_at_unix_secs: 1,
        last_attempt_at_unix_secs: 2,
        retry_count: 1,
        reason: "operator reset in progress".to_string(),
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: DATA_USAGE_BLOOM_RECOVERY_PATH.clone(),
        state: state.to_string(),
    };
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
        serde_json::to_vec(&marker).expect("marker encoding"),
    )
    .await
    .expect("persist operator marker");
    marker
}

async fn persisted_state(store: &Arc<ECStore>) -> Vec<(Option<Vec<u8>>, DataUsageCacheRevision)> {
    let mut state = Vec::new();
    for path in [
        DATA_USAGE_BLOOM_NAME_PATH.as_str(),
        DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
    ] {
        state.push(
            read_config_with_revision(store.clone(), path)
                .await
                .expect("read exact metadata revision"),
        );
    }
    state
}

async fn run_disabled_startup(ctx: CancellationToken, store: Arc<ECStore>) {
    let initialized_before = crate::scanner_runtime_initialized();
    let cleanup = init_scanner_with_recovery(ctx, store, false).await;
    if let Some(cleanup) = cleanup {
        tokio::time::timeout(Duration::from_secs(15), cleanup)
            .await
            .expect("finite disabled cleanup attempt")
            .expect("cleanup task should not panic");
    }
    assert_eq!(
        crate::scanner_runtime_initialized(),
        initialized_before,
        "disabled recovery must not start the normal scanner runtime"
    );
}

fn recovery_intent_request(key: &str, actor: &str) -> ScannerRecoveryIntentRequest {
    ScannerRecoveryIntentRequest {
        action: SCANNER_RECOVERY_INTENT_ACTION_USAGE_FULL_REBUILD.to_string(),
        mode: "full-rebuild".to_string(),
        idempotency_key: key.to_string(),
        actor_sha256: scanner_recovery_actor_sha256(actor),
    }
}

async fn assert_reset_fences(store: &Arc<ECStore>) {
    let data = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("cycle remains durable");
    let (cycle, epoch) = decode_scanner_cycle_state(&data).expect("valid preserved cycle");
    assert_eq!((cycle.current, cycle.next, epoch), (3, 42, 8));
    let usage: DataUsageInfo = serde_json::from_slice(
        &read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("durable usage fence"),
    )
    .expect("valid usage");
    assert_eq!(usage.scanner_epoch, Some(8));
    assert_eq!(usage.scanner_cycle, Some(41));
    assert!(matches!(
        read_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

async fn assert_rebuilt_reset_fences(store: &Arc<ECStore>, expected_epoch: u64) {
    let data = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("rebuilt cycle remains durable");
    let (cycle, epoch) = decode_scanner_cycle_state(&data).expect("valid rebuilt cycle");
    assert_eq!((cycle.current, cycle.next, epoch), (0, 42, expected_epoch));
    let usage: DataUsageInfo = serde_json::from_slice(
        &read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("durable rebuilt usage fence"),
    )
    .expect("valid usage");
    assert_eq!(usage.scanner_epoch, Some(expected_epoch));
    assert_eq!(usage.scanner_cycle, Some(41));
    assert!(matches!(
        read_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[test]
fn scanner_reset_crash_child_process_fixture() {
    let Ok(root) = std::env::var("RUSTFS_SCANNER_RESET_CRASH_ROOT") else {
        return;
    };
    let stage = match std::env::var("RUSTFS_SCANNER_RESET_CRASH_STAGE").as_deref() {
        Ok("primary-read") => cleanup_io_fault::Stage::PrimaryRead,
        Ok("primary-write") => cleanup_io_fault::Stage::PrimaryWrite,
        Ok("usage-fence") => cleanup_io_fault::Stage::UsageFence,
        other => panic!("unexpected reset crash stage: {other:?}"),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime should build");
    runtime.block_on(async {
        let store = setup_scanner_cycle_store_at_path(std::path::Path::new(&root), false, 1).await;
        if stage == cleanup_io_fault::Stage::UsageFence {
            save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), b"corrupt-cycle".to_vec())
                .await
                .expect("force child through reconstruction branch");
        }
        let injection = cleanup_io_fault::install(&store, stage, false);
        let error = resume_scanner_cycle_cleanup(CancellationToken::new(), store)
            .await
            .expect_err("child should stop at the injected owned I/O boundary");
        assert!(injection.fired_while_owned());
        let expected = match stage {
            cleanup_io_fault::Stage::PrimaryRead => "injected primary read failure",
            cleanup_io_fault::Stage::PrimaryWrite => "injected primary write failure",
            cleanup_io_fault::Stage::UsageFence => "injected usage fence failure",
        };
        assert!(error.to_string().contains(expected), "{error}");
    });
    std::process::exit(77);
}

#[tokio::test]
#[serial]
async fn disabled_cleanup_recovers_after_child_process_crash_boundaries() {
    for (name, expected_rebuilt_epoch) in [("primary-read", None), ("primary-write", None), ("usage-fence", Some(9))] {
        let temp_dir = tempfile::tempdir().expect("crash fixture directory");
        let store = setup_scanner_cycle_store_at_path(temp_dir.path(), false, 1).await;
        seed_cleanup(&store, "cleanup-pending").await;
        drop(store);

        let status = std::process::Command::new(std::env::current_exe().expect("test binary path"))
            .arg("scanner::tests::recovery_control::scanner_reset_crash_child_process_fixture")
            .arg("--exact")
            .arg("--nocapture")
            .env("RUSTFS_SCANNER_RESET_CRASH_ROOT", temp_dir.path())
            .env("RUSTFS_SCANNER_RESET_CRASH_STAGE", name)
            .status()
            .expect("child crash fixture should start");
        assert_eq!(status.code(), Some(77), "{name} child did not reach the owned crash boundary");

        let restarted = setup_scanner_cycle_store_at_path(temp_dir.path(), false, 1).await;
        run_disabled_startup(CancellationToken::new(), restarted.clone()).await;
        if let Some(epoch) = expected_rebuilt_epoch {
            assert_rebuilt_reset_fences(&restarted, epoch).await;
        } else {
            assert_reset_fences(&restarted).await;
        }
    }
}

#[tokio::test]
#[serial]
async fn scanner_recovery_intent_accept_is_durable_and_idempotent() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    let request = recovery_intent_request("intent-key-0001", "operator-a");

    let first = accept_scanner_usage_recovery_intent(store.clone(), request.clone())
        .await
        .expect("first intent should persist");
    let record = match first {
        ScannerRecoveryIntentAcceptResult::Accepted { record } => record,
        other => panic!("first request must create a durable intent: {other:?}"),
    };
    assert_eq!(record.state, "accepted");
    assert_eq!(record.action, SCANNER_RECOVERY_INTENT_ACTION_USAGE_FULL_REBUILD);
    assert_ne!(record.idempotency_key_sha256, request.idempotency_key);

    let restarted = restart_scanner_cycle_store_from(&store).await;
    let queried = get_scanner_usage_recovery_intent(restarted.clone(), &record.intent_id)
        .await
        .expect("persisted intent should read after restart")
        .expect("persisted intent should exist");
    assert_eq!(queried, record);

    let replay = accept_scanner_usage_recovery_intent(restarted, request)
        .await
        .expect("lost response retry should be idempotent");
    assert_eq!(replay, ScannerRecoveryIntentAcceptResult::Replayed { record });
}

#[tokio::test]
#[serial]
async fn scanner_recovery_intent_executor_persists_completed_progress() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    let request = recovery_intent_request("intent-key-0001-exec", "operator-a");

    let record = match accept_scanner_usage_recovery_intent(store.clone(), request.clone())
        .await
        .expect("intent should persist")
    {
        ScannerRecoveryIntentAcceptResult::Accepted { record } => record,
        other => panic!("first request must create a durable intent: {other:?}"),
    };

    let reset = run_scanner_usage_recovery_intent(CancellationToken::new(), store.clone(), record.intent_id.clone())
        .await
        .expect("accepted intent should execute")
        .expect("accepted intent should produce a reset");
    assert_eq!(reset.status, "reset");
    assert_eq!(reset.mode, "full-rebuild");

    let completed = get_scanner_usage_recovery_intent(store.clone(), &record.intent_id)
        .await
        .expect("completed intent should read")
        .expect("completed intent should remain durable");
    assert_eq!(completed.state, "completed");
    assert_eq!(completed.intent_id, record.intent_id);

    let restarted = restart_scanner_cycle_store_from(&store).await;
    let replay = accept_scanner_usage_recovery_intent(restarted.clone(), request)
        .await
        .expect("lost response retry should return the durable completed intent");
    assert_eq!(replay, ScannerRecoveryIntentAcceptResult::Replayed { record: completed });
    let rerun = run_scanner_usage_recovery_intent(CancellationToken::new(), restarted, record.intent_id)
        .await
        .expect("terminal intent should not be an execution error");
    assert!(rerun.is_none(), "completed intent must not start a second reset");
}

#[tokio::test]
#[serial]
async fn scanner_recovery_intent_executor_persists_failed_progress() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    let record = match accept_scanner_usage_recovery_intent(
        store.clone(),
        recovery_intent_request("intent-key-0001-failed", "operator-a"),
    )
    .await
    .expect("intent should persist")
    {
        ScannerRecoveryIntentAcceptResult::Accepted { record } => record,
        other => panic!("first request must create a durable intent: {other:?}"),
    };

    let ctx = CancellationToken::new();
    ctx.cancel();
    let error = run_scanner_usage_recovery_intent(ctx, store.clone(), record.intent_id.clone())
        .await
        .expect_err("cancelled intent execution should fail");
    assert!(error.to_string().contains("cancelled"), "{error}");

    let failed = get_scanner_usage_recovery_intent(store, &record.intent_id)
        .await
        .expect("failed intent should read")
        .expect("failed intent should remain durable");
    assert_eq!(failed.state, "failed");
    assert_eq!(failed.intent_id, record.intent_id);
}

#[tokio::test]
#[serial]
async fn scanner_recovery_intent_rejects_same_namespace_conflict() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    let request = recovery_intent_request("intent-key-0002", "operator-a");
    let record = match accept_scanner_usage_recovery_intent(store.clone(), request.clone())
        .await
        .expect("first intent should persist")
    {
        ScannerRecoveryIntentAcceptResult::Accepted { record } => record,
        other => panic!("first request must create a durable intent: {other:?}"),
    };

    let mut unsupported = request.clone();
    unsupported.mode = "future-mode".to_string();
    let error = accept_scanner_usage_recovery_intent(store.clone(), unsupported)
        .await
        .expect_err("unsupported mode must fail before it can collide with a durable record");
    assert!(error.to_string().contains("mode is unsupported"));

    let same_key_other_actor = recovery_intent_request("intent-key-0002", "operator-b");
    let accepted_other_actor = accept_scanner_usage_recovery_intent(store.clone(), same_key_other_actor)
        .await
        .expect("another actor owns an independent idempotency namespace");
    assert!(matches!(accepted_other_actor, ScannerRecoveryIntentAcceptResult::Accepted { .. }));

    let path = format!(".usage.v2.recovery-intents/{}.json", record.intent_id);
    let mut existing = record.clone();
    existing.request_sha256 = scanner_recovery_actor_sha256("different-request");
    save_config(store.clone(), &path, serde_json::to_vec(&existing).expect("mutated record should encode"))
        .await
        .expect("mutate durable record");
    let conflict = accept_scanner_usage_recovery_intent(store, request)
        .await
        .expect("same namespace collision should be reported");
    assert_eq!(
        conflict,
        ScannerRecoveryIntentAcceptResult::Conflict {
            existing: ScannerRecoveryIntentConflict {
                intent_id: record.intent_id,
                state: "accepted".to_string(),
            },
        }
    );
}

#[tokio::test]
#[serial]
async fn scanner_recovery_intent_query_rejects_corrupt_or_unknown_records() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    let request = recovery_intent_request("intent-key-0003", "operator-a");
    let record = match accept_scanner_usage_recovery_intent(store.clone(), request)
        .await
        .expect("first intent should persist")
    {
        ScannerRecoveryIntentAcceptResult::Accepted { record } => record,
        other => panic!("first request must create a durable intent: {other:?}"),
    };
    let path = format!(".usage.v2.recovery-intents/{}.json", record.intent_id);
    save_config(store.clone(), &path, b"{corrupt".to_vec())
        .await
        .expect("corrupt durable record");
    let error = get_scanner_usage_recovery_intent(store.clone(), &record.intent_id)
        .await
        .expect_err("corrupt intent must not decode as absent");
    assert!(error.to_string().contains("scanner recovery intent is invalid"));
    let unknown = get_scanner_usage_recovery_intent(store, &scanner_recovery_actor_sha256("missing"))
        .await
        .expect("missing intent should read as absent");
    assert!(unknown.is_none());
}

#[tokio::test]
#[serial]
async fn scanner_recovery_intent_query_rejects_oversized_records() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    let intent_id = scanner_recovery_actor_sha256("oversized-intent");
    let path = format!(".usage.v2.recovery-intents/{intent_id}.json");
    save_config(store.clone(), &path, vec![b'a'; 16 * 1024 + 1])
        .await
        .expect("oversized durable record");

    let error = get_scanner_usage_recovery_intent(store, &intent_id)
        .await
        .expect_err("oversized intent must not be materialized");
    assert!(error.to_string().contains("bounded object size"), "{error}");
}

#[tokio::test]
#[serial]
async fn disabled_cleanup_reopens_persisted_intent_without_starting_scanner() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    seed_cleanup(&store, "cleanup-pending").await;
    let restarted = restart_scanner_cycle_store_from(&store).await;
    run_disabled_startup(CancellationToken::new(), restarted.clone()).await;
    assert_reset_fences(&restarted).await;
    let completed = persisted_state(&restarted).await;
    run_disabled_startup(CancellationToken::new(), restarted.clone()).await;
    assert_eq!(
        persisted_state(&restarted).await,
        completed,
        "a later startup without an intent must not reset again"
    );
}

#[tokio::test]
#[serial]
async fn disabled_cleanup_does_not_authorize_blocked_unknown_or_corrupt_markers() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    for kind in ["blocked", "unknown-phase", "future-version", "unknown-field", "corrupt"] {
        let marker = seed_cleanup(&store, "blocked").await;
        let mut value = serde_json::to_value(marker).expect("marker value");
        match kind {
            "unknown-phase" => value["state"] = "future-phase".into(),
            "future-version" => value["schema_version"] = 99.into(),
            "unknown-field" => value["future_hint"] = true.into(),
            _ => {}
        }
        let bytes = if kind == "corrupt" {
            b"{broken".to_vec()
        } else {
            serde_json::to_vec(&value).expect("marker JSON")
        };
        save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), bytes)
            .await
            .expect("persist rejected marker");
        let before = persisted_state(&store).await;
        run_disabled_startup(CancellationToken::new(), store.clone()).await;
        assert_eq!(persisted_state(&store).await, before, "{kind} must not become an automatic full rescan");
    }
}

#[tokio::test]
#[serial]
async fn disabled_cleanup_rechecks_revision_after_waiting_for_leader_lock() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    let mut marker = seed_cleanup(&store, "cleanup-pending").await;
    let expected = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
        .await
        .expect("intent revision")
        .1;
    let lock = store
        .new_ns_lock(RUSTFS_META_BUCKET, "leader.lock")
        .await
        .expect("leader lock");
    let guard = lock
        .get_write_lock_quiet(Duration::from_secs(1))
        .await
        .expect("hold leader ownership");
    let mut recovery = Box::pin(reset_scanner_cycle_recovery_for_intent(
        CancellationToken::new(),
        store.clone(),
        Some(expected),
        None,
    ));
    assert!(matches!(futures::poll!(&mut recovery), Poll::Pending));
    marker.state = "blocked".to_string();
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
        serde_json::to_vec(&marker).expect("replacement marker"),
    )
    .await
    .expect("replace intent while the fixture owns leader lock");
    let replaced = persisted_state(&store).await;
    drop(guard);
    let error = recovery.await.expect_err("old preflight cannot authorize replacement marker");
    assert!(error.to_string().contains("changed before recovery acquired ownership"));
    assert_eq!(persisted_state(&store).await, replaced);
}

#[tokio::test]
#[serial]
async fn disabled_cleanup_requires_phase_even_when_revision_matches() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    seed_cleanup(&store, "blocked").await;
    let before = persisted_state(&store).await;
    let error = reset_scanner_cycle_recovery_for_intent(CancellationToken::new(), store.clone(), Some(before[1].1.clone()), None)
        .await
        .expect_err("a matching ETag alone is not operator cleanup authorization");
    assert!(error.to_string().contains("unchanged cleanup-pending"));
    assert_eq!(persisted_state(&store).await, before);
    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("explicit v3 core retains full reset authorization");
    assert_reset_fences(&store).await;
}

#[tokio::test]
#[serial]
async fn disabled_cleanup_lock_busy_preserves_intent_without_force_unlock() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    seed_cleanup(&store, "cleanup-pending").await;
    let before = persisted_state(&store).await;
    let lock = store
        .new_ns_lock(RUSTFS_META_BUCKET, "leader.lock")
        .await
        .expect("leader lock");
    let guard = lock
        .get_write_lock_quiet(Duration::from_secs(1))
        .await
        .expect("hold live leader");
    let error = resume_scanner_cycle_cleanup(CancellationToken::new(), store.clone())
        .await
        .expect_err("busy leader must block recovery");
    assert!(error.to_string().contains("leader lock is busy"));
    let status = scanner_cycle_recovery_status();
    assert_eq!(status.state, "cleanup-pending");
    assert!(
        status
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("leader lock is busy"))
    );
    assert!(!status.retryable, "disabled startup makes one attempt, not an automatic retry loop");
    assert!(!guard.is_lock_lost(), "recovery must not revoke the live owner");
    assert_eq!(persisted_state(&store).await, before);
    drop(guard);
    run_disabled_startup(CancellationToken::new(), store.clone()).await;
    assert_reset_fences(&store).await;
}

#[tokio::test]
#[serial]
async fn concurrent_full_rescan_requests_converge_to_one_recovery_fence() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    seed_cleanup(&store, "blocked").await;
    let before = persisted_state(&store).await;
    let lock = store
        .new_ns_lock(RUSTFS_META_BUCKET, "leader.lock")
        .await
        .expect("leader lock");
    let guard = lock
        .get_write_lock_quiet(Duration::from_secs(1))
        .await
        .expect("hold live leader before concurrent admin resets");

    let first = tokio::spawn(reset_scanner_cycle_recovery(CancellationToken::new(), store.clone()));
    let second = tokio::spawn(reset_scanner_cycle_recovery(CancellationToken::new(), store.clone()));
    tokio::task::yield_now().await;
    assert_eq!(
        persisted_state(&store).await,
        before,
        "waiting admin reset requests must not mutate state before leader ownership"
    );

    drop(guard);
    let (first, second) = tokio::time::timeout(Duration::from_secs(15), async { tokio::join!(first, second) })
        .await
        .expect("both admin reset requests should finish after the live owner releases the lock");
    let first = first.expect("first admin reset task should not panic");
    let second = second.expect("second admin reset task should not panic");
    assert!(first.is_ok(), "first admin reset failed: {first:?}");
    assert!(second.is_ok(), "second admin reset failed: {second:?}");

    assert_reset_fences(&store).await;
    let completed = persisted_state(&store).await;
    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("lost-reply retry after a completed reset should be idempotent");
    assert_eq!(
        persisted_state(&store).await,
        completed,
        "a later retry must not create a second reset identity or rewrite durable fences"
    );
    assert_eq!(scanner_cycle_recovery_status().state, "healthy");
}

#[tokio::test]
#[serial]
async fn disabled_cleanup_movement_pause_preserves_intent_for_later_startup() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    seed_cleanup(&store, "cleanup-pending").await;
    let before = persisted_state(&store).await;
    *store.rebalance_meta.write().await = Some(EcstoreRebalanceMeta {
        id: "cleanup-movement".to_string(),
        pool_stats: vec![EcstoreRebalanceStats {
            participating: true,
            info: EcstoreRebalanceInfo {
                start_time: Some(time::OffsetDateTime::now_utc()),
                status: EcstoreRebalStatus::Started,
                ..Default::default()
            },
            ..Default::default()
        }],
        ..Default::default()
    });
    let error = resume_scanner_cycle_cleanup(CancellationToken::new(), store.clone())
        .await
        .expect_err("movement must block reset publication");
    assert!(error.to_string().contains("blocked by data movement"));
    let status = scanner_cycle_recovery_status();
    assert_eq!(status.state, "cleanup-pending");
    assert!(
        status
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("blocked by data movement"))
    );
    assert_eq!(persisted_state(&store).await, before);
    *store.rebalance_meta.write().await = None;
    run_disabled_startup(CancellationToken::new(), store.clone()).await;
    assert_reset_fences(&store).await;
}

#[tokio::test]
#[serial]
async fn disabled_cleanup_cancelled_startup_preserves_persisted_work() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    seed_cleanup(&store, "cleanup-pending").await;
    let before = persisted_state(&store).await;
    let ctx = CancellationToken::new();
    ctx.cancel();
    run_disabled_startup(ctx, store.clone()).await;
    assert_eq!(persisted_state(&store).await, before);
}

#[tokio::test(start_paused = true)]
#[serial]
async fn disabled_cleanup_probe_obeys_cancellation_and_existing_io_deadline() {
    for cancel in [false, true] {
        let store = Arc::new(MemoryConfigStore::default());
        store.delayed_gets.lock().await.insert(
            memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()),
            data_usage_persist_timeout().saturating_add(Duration::from_secs(60)),
        );
        let ctx = CancellationToken::new();
        let mut probe = Box::pin(read_scanner_cleanup_marker(store.clone(), &ctx));
        assert!(matches!(futures::poll!(&mut probe), Poll::Pending));
        if cancel {
            ctx.cancel();
        } else {
            advance(data_usage_persist_timeout()).await;
        }
        let error = probe.await.expect_err("pending read must be bounded");
        assert!(error.to_string().contains(if cancel { "cancelled" } else { "timed out" }));
        assert!(store.put_counts.lock().await.is_empty(), "probe must remain read-only");
    }
}

#[test]
#[serial]
fn disabled_cleanup_old_observation_cannot_overwrite_a_new_completion() {
    let original = scanner_cycle_recovery_status();
    let healthy = ScannerCycleRecoveryStatus {
        state: "healthy".to_string(),
        ..Default::default()
    };
    publish_scanner_cleanup_status(healthy.clone(), None).expect("first status version");
    let old = scanner_cleanup_status_version();
    publish_scanner_cleanup_status(healthy, None).expect("a newer completion may have identical fields");
    assert!(
        publish_scanner_cleanup_status(
            ScannerCycleRecoveryStatus {
                state: "cleanup-pending".to_string(),
                reason: Some("old lock wait failed".to_string()),
                ..Default::default()
            },
            Some(old)
        )
        .is_none()
    );
    assert_eq!(scanner_cycle_recovery_status().state, "healthy");
    publish_scanner_cleanup_status(original, None).expect("restore prior observation");
}

#[tokio::test]
#[serial]
async fn disabled_cleanup_owned_read_and_write_failures_keep_specific_status() {
    for (stage, newer_completion) in [
        (cleanup_io_fault::Stage::PrimaryRead, false),
        (cleanup_io_fault::Stage::PrimaryWrite, false),
        (cleanup_io_fault::Stage::PrimaryWrite, true),
    ] {
        let (_dir, store) = setup_scanner_cycle_store().await;
        seed_cleanup(&store, "cleanup-pending").await;
        let before = persisted_state(&store).await;
        let injection = cleanup_io_fault::install(&store, stage, newer_completion);
        let error = resume_scanner_cycle_cleanup(CancellationToken::new(), store.clone())
            .await
            .expect_err("injected owned I/O boundary");
        assert!(
            injection.fired_while_owned(),
            "fault must occur after real leader ownership and marker validation"
        );
        let expected_error = match stage {
            cleanup_io_fault::Stage::PrimaryRead => "injected primary read failure",
            cleanup_io_fault::Stage::PrimaryWrite => "injected primary write failure",
            cleanup_io_fault::Stage::UsageFence => "injected usage fence failure",
        };
        assert!(error.to_string().contains(expected_error));
        let status = scanner_cycle_recovery_status();
        if newer_completion {
            assert_eq!(status.state, "healthy", "old error cannot overwrite a newer completion observation");
            assert!(status.reason.is_none());
        } else {
            assert_eq!(status.state, "cleanup-pending");
            assert!(
                status.reason.as_deref().is_some_and(|reason| reason.contains(expected_error)),
                "{status:?}"
            );
        }
        let after = persisted_state(&store).await;
        assert_eq!(after[0], before[0], "failed primary I/O must preserve its prior revision");
        assert_eq!(after[2], before[2], "failed primary I/O must not advance the usage fence");
        let marker: ScannerCycleRecoveryMarker =
            serde_json::from_slice(after[1].0.as_deref().expect("durable marker retained")).expect("valid cleanup marker");
        assert_eq!(marker.state, "cleanup-pending");
        drop(injection);
        run_disabled_startup(CancellationToken::new(), store.clone()).await;
        assert_reset_fences(&store).await;
    }
}

#[tokio::test]
#[serial]
async fn disabled_cleanup_invalidated_observation_never_becomes_unconditional() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    seed_cleanup(&store, "cleanup-pending").await;
    let revision = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
        .await
        .expect("marker revision")
        .1;
    let newer = ScannerCycleRecoveryStatus {
        state: "healthy".to_string(),
        reason: Some("newer completion owner".to_string()),
        ..Default::default()
    };
    publish_scanner_cleanup_status(newer.clone(), None).expect("newer observation");
    let mut invalidated = None;
    reset_scanner_cycle_recovery_for_intent(CancellationToken::new(), store.clone(), Some(revision), Some(&mut invalidated))
        .await
        .expect("metadata cleanup may complete without owning the newest status observation");
    assert!(invalidated.is_none());
    assert_eq!(
        serde_json::to_value(scanner_cycle_recovery_status()).expect("observed status"),
        serde_json::to_value(newer).expect("newer status")
    );
    assert_reset_fences(&store).await;
}

#[tokio::test]
#[serial]
async fn disabled_cleanup_later_failure_preserves_rebuilt_primary_status_identity() {
    for newer_completion in [false, true] {
        let (_dir, store) = setup_scanner_cycle_store().await;
        seed_cleanup(&store, "cleanup-pending").await;
        save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), b"corrupt-cycle".to_vec())
            .await
            .expect("force the full reconstruction branch");
        let before = persisted_state(&store).await;
        let injection = cleanup_io_fault::install(&store, cleanup_io_fault::Stage::UsageFence, newer_completion);
        let error = resume_scanner_cycle_cleanup(CancellationToken::new(), store.clone())
            .await
            .expect_err("fail after primary publication");
        assert!(injection.fired_while_owned());
        assert!(error.to_string().contains("injected usage fence failure"));
        let after = persisted_state(&store).await;
        assert_ne!(after[0].1, before[0].1, "the primary write must actually commit before this failure");
        let (cycle, epoch) =
            decode_scanner_cycle_state(after[0].0.as_deref().expect("rebuilt primary")).expect("valid durable reconstruction");
        assert_eq!((cycle.current, cycle.next, epoch), (0, 42, 8));
        assert_eq!(after[2], before[2], "usage fence publication was rejected");
        let marker: ScannerCycleRecoveryMarker =
            serde_json::from_slice(after[1].0.as_deref().expect("cleanup marker retained")).expect("valid cleanup marker");
        assert_eq!(marker.state, "cleanup-pending");
        let status = scanner_cycle_recovery_status();
        if newer_completion {
            assert_eq!(status.state, "healthy");
            assert!(
                status.reason.is_none(),
                "old core and outer failure must both retain invalidated ownership"
            );
        } else {
            let DataUsageCacheRevision::Etag(etag) = &after[0].1 else {
                panic!("rebuilt primary must have a revision");
            };
            assert_eq!(status.state, "cleanup-pending");
            assert_eq!(status.primary_revision.as_deref(), Some(etag.as_str()));
            assert_eq!(status.generation, Some(cycle.next));
            assert_eq!(status.leader_epoch, Some(epoch));
            assert!(
                status
                    .reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("injected usage fence failure"))
            );
        }
    }
}
