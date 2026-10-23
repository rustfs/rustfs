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

//! Startup-gate self-healing (rustfs/backlog#2901): every fail-closed scanner
//! state must have an automatic exit with a bounded dwell time, without
//! deleting quarantine evidence or relaxing the leadership, CAS, movement, or
//! usage-floor fences.

use super::*;
use crate::storage_api::owner::{EcstoreRebalStatus, EcstoreRebalanceInfo, EcstoreRebalanceMeta, EcstoreRebalanceStats};

const SUPERVISOR_ENV: [(&str, Option<&str>); 3] = [
    ("RUSTFS_LOCK_ACQUIRE_TIMEOUT", Some("1")),
    (ENV_SCANNER_START_DELAY_SECS, Some("0")),
    (ENV_SCANNER_CYCLE, Some("1")),
];
const WAIT_BUDGET: Duration = Duration::from_secs(30);

fn reset_recovery_status() {
    publish_scanner_cleanup_status(
        ScannerCycleRecoveryStatus {
            path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
            quarantine_path: Some(DATA_USAGE_BLOOM_RECOVERY_PATH.clone()),
            state: "healthy".to_string(),
            max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
            ..Default::default()
        },
        None,
    )
    .expect("test must be able to reset the local recovery observation");
}

async fn wait_for_recovery_attempt(attempt: u32) -> ScannerCycleRecoveryStatus {
    tokio::time::timeout(WAIT_BUDGET, async {
        loop {
            let status = scanner_cycle_recovery_status();
            if status.retry_count >= u64::from(attempt) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("recovery attempt {attempt} was never recorded: {:?}", scanner_cycle_recovery_status()))
}

async fn wait_for_recovery_state(state: &str) -> ScannerCycleRecoveryStatus {
    tokio::time::timeout(WAIT_BUDGET, async {
        loop {
            let status = scanner_cycle_recovery_status();
            if status.state == state {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("recovery never reached {state}: {:?}", scanner_cycle_recovery_status()))
}

async fn persisted_cycle(store: &Arc<ECStore>) -> Option<(CurrentCycle, u64)> {
    let data = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH).await.ok()?;
    decode_scanner_cycle_state(&data).ok()
}

async fn wait_for_persisted_cycle(store: &Arc<ECStore>, accept: impl Fn(&CurrentCycle, u64) -> bool) -> (CurrentCycle, u64) {
    tokio::time::timeout(WAIT_BUDGET, async {
        loop {
            if let Some((cycle, epoch)) = persisted_cycle(store).await
                && accept(&cycle, epoch)
            {
                return (cycle, epoch);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "persisted cycle state never reached the expected shape: {:?}",
            scanner_cycle_recovery_status()
        )
    })
}

async fn stop_supervisor(ctx: CancellationToken) {
    ctx.cancel();
    tokio::time::timeout(Duration::from_secs(15), async {
        while crate::scanner_runtime_initialized() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled scanner supervisor must stop");
}

fn valid_usage_floor(epoch: u64, cycle: u64) -> Vec<u8> {
    serde_json::to_vec(&DataUsageInfo {
        scanner_epoch: Some(epoch),
        scanner_cycle: Some(cycle),
        ..complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0)
    })
    .expect("usage floor should encode")
}

fn started_rebalance() -> EcstoreRebalanceMeta {
    EcstoreRebalanceMeta {
        id: "gate-self-heal".to_string(),
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
    }
}

/// rustfs/rustfs#8670: a usage floor that is present but untrusted must not
/// drive the supervisor into a terminal `paused, retryable=false` observation.
/// Every attempt stays retryable with a bounded backoff, and repairing the
/// floor recovers the scanner without a process restart.
#[tokio::test]
#[serial]
async fn usage_floor_load_failure_backs_off_and_recovers_without_restart() {
    temp_env::async_with_vars(SUPERVISOR_ENV, async {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        reset_recovery_status();
        let (_dir, store) = setup_scanner_cycle_store().await;
        // Present but without a baseline identity: the shape #8670 reported,
        // whose error points the operator at the usage-state reset.
        save_config(
            store.clone(),
            DATA_USAGE_OBJ_NAME_PATH.as_str(),
            serde_json::to_vec(&DataUsageInfo::default()).expect("identity-less usage should encode"),
        )
        .await
        .expect("untrusted usage floor should persist");
        assert!(!crate::scanner_runtime_initialized(), "test must own the scanner runtime");
        let ctx = CancellationToken::new();
        let _cancel_on_drop = ctx.clone().drop_guard();
        init_data_scanner_with_storage(ctx.clone(), store.clone()).await;

        let attempts = MAX_SCANNER_CYCLE_RECOVERY_RETRIES + 2;
        let mut first_detected = None;
        for attempt in 1..=attempts {
            let status = wait_for_recovery_attempt(attempt).await;
            assert_eq!(status.classification.as_deref(), Some("usage_floor_load_failed"), "attempt {attempt}");
            assert!(status.retryable, "attempt {attempt} must stay retryable: {status:?}");
            assert_ne!(status.state, "paused", "attempt {attempt} must not report a terminal pause");
            if attempt >= MAX_SCANNER_CYCLE_RECOVERY_RETRIES {
                assert_eq!(status.state, "backoff", "attempt {attempt}: {status:?}");
            } else {
                assert_eq!(status.state, "usage_floor_load_failed", "attempt {attempt}: {status:?}");
            }
            assert!(
                status
                    .reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("usage-state/reset") && reason.contains("next automatic attempt")),
                "the operator hint and the next attempt must both be visible: {status:?}"
            );
            assert!(status.next_attempt_at_unix_secs.is_some(), "attempt {attempt}: {status:?}");
            match first_detected {
                None => first_detected = status.first_detected_at_unix_secs,
                Some(first) => assert_eq!(status.first_detected_at_unix_secs, Some(first), "attempt {attempt}"),
            }
            if attempt == attempts {
                save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), valid_usage_floor(0, 0))
                    .await
                    .expect("repaired usage floor should persist");
            }
            notify_scanner_cycle_recovery_wake();
        }

        let healthy = wait_for_recovery_state("healthy").await;
        assert_eq!(healthy.retry_count, 0, "a successful attempt clears the retry budget: {healthy:?}");
        assert!(!healthy.retryable);
        wait_for_persisted_cycle(&store, |cycle, _| cycle.next >= 1).await;
        assert_eq!(scanner_cycle_schedule_status().execution_role, "leader");
        stop_supervisor(ctx).await;
        global_metrics().set_cycle(None).await;
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    })
    .await;
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

async fn seed_transient_cycle_read_failure() -> ScannerCycleRecoveryStatus {
    let store = Arc::new(MemoryConfigStore::default());
    store.read_errors.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str()),
        EcstoreError::InsufficientReadQuorum(RUSTFS_META_BUCKET.to_string(), DATA_USAGE_BLOOM_NAME_PATH.clone()),
    );
    assert!(matches!(
        load_scanner_cycle_state_for_startup(store).await,
        ScannerCycleStateStartup::Transient(_)
    ));
    let status = scanner_cycle_recovery_status();
    assert_eq!(status.state, "transient");
    assert!(status.retryable);
    status
}

/// rustfs/rustfs#8297: a peer outage longer than the short retry budget
/// (five attempts, about 75 s, inside a 180 s planned restart) must leave the
/// observation retryable with a bounded backoff instead of a terminal pause.
#[tokio::test]
#[serial]
async fn transient_cycle_state_outage_exhausting_the_budget_stays_retryable() {
    {
        reset_recovery_status();
        for attempt in 1..=MAX_SCANNER_CYCLE_RECOVERY_RETRIES + 3 {
            seed_transient_cycle_read_failure().await;
            let retry = record_scanner_cycle_recovery_retry(attempt, Duration::from_secs(1));
            assert_eq!(retry.attempt, attempt);
            assert!(retry.delay <= Duration::from_secs(rustfs_config::DEFAULT_SCANNER_RECOVERY_BACKOFF_CAP_SECS));
            let status = scanner_cycle_recovery_status();
            assert!(status.retryable, "attempt {attempt} must stay retryable: {status:?}");
            assert!(status.next_attempt_at_unix_secs.is_some(), "attempt {attempt}: {status:?}");
            assert_ne!(status.state, "paused", "attempt {attempt} must not report a terminal pause");
            assert_eq!(status.retry_count, u64::from(attempt));
            if attempt >= MAX_SCANNER_CYCLE_RECOVERY_RETRIES {
                assert_eq!(status.state, "backoff", "attempt {attempt}: {status:?}");
            } else {
                assert_eq!(status.state, "transient", "attempt {attempt}: {status:?}");
            }
        }
        // The peer returns: a clean load clears the observation entirely.
        let store = Arc::new(MemoryConfigStore::default());
        assert!(matches!(
            load_scanner_cycle_state_for_startup(store).await,
            ScannerCycleStateStartup::Ready { .. }
        ));
        let healthy = scanner_cycle_recovery_status();
        assert_eq!(healthy.state, "healthy");
        assert_eq!(healthy.retry_count, 0);
    }
}

async fn poison_pause_backlog_replica(store: &Arc<ECStore>) {
    Arc::clone(store)
        .save_scanner_pause_backlog_replica(0, 0, b"{not-json".to_vec(), crate::HTTPPreconditions::default())
        .await
        .expect("poisoned pause backlog replica should persist");
    assert!(
        ScannerPauseBacklogController::claim(store.clone(), scanner_pause_backlog_now())
            .await
            .is_err(),
        "an invalid replica must keep the pause backlog unclaimable"
    );
}

/// rustfs/rustfs#8251: a pause backlog that can never be claimed must not
/// skip every cycle. The leader runs degraded, the status says so, and the
/// movement fence still decides whether a cycle may start.
#[tokio::test]
#[serial]
async fn unclaimable_pause_backlog_runs_cycles_degraded_behind_the_movement_fence() {
    temp_env::async_with_vars(SUPERVISOR_ENV, async {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        reset_recovery_status();
        let (_dir, store) = setup_scanner_cycle_store().await;
        poison_pause_backlog_replica(&store).await;
        *store.rebalance_meta.write().await = Some(started_rebalance());
        assert!(store.scanner_data_movement_pause_status().await.paused);

        let ctx = CancellationToken::new();
        let _cancel_on_drop = ctx.clone().drop_guard();
        let mut leader = tokio::spawn({
            let ctx = ctx.clone();
            let store = store.clone();
            async move { run_data_scanner(ctx, store).await }
        });
        // Movement is active: a degraded ledger must not admit a cycle.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            persisted_cycle(&store).await.is_none(),
            "no cycle may run while data movement pauses the scanner"
        );
        assert!(!global_metrics().report().await.current_cycle_active);
        assert!(!leader.is_finished(), "a paused leader waits for movement to end");

        // Movement ends: the degraded leader runs cycles immediately.
        *store.rebalance_meta.write().await = None;
        store.scanner_data_movement_changed().notify_one();
        let (cycle, epoch) = tokio::time::timeout(WAIT_BUDGET, async {
            loop {
                tokio::select! {
                    result = &mut leader => panic!("degraded leader iteration stopped early: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                if let Some((cycle, epoch)) = persisted_cycle(&store).await
                    && cycle.next >= 1
                {
                    break (cycle, epoch);
                }
            }
        })
        .await
        .expect("a degraded pause backlog must not prevent scanner cycles");
        assert!(epoch >= 1, "the degraded cycle still ran under a claimed epoch: {cycle:?}");

        let backlog = scanner_pause_backlog_status(store.clone()).await;
        assert_eq!(backlog.persistence_state, "degraded", "{backlog:?}");
        assert!(!backlog.durable);
        assert!(
            backlog
                .error
                .as_deref()
                .is_some_and(|error| error.contains("scanner pause backlog"))
        );
        assert!(backlog.alerting);

        ctx.cancel();
        leader
            .await
            .expect("degraded leader task should not panic")
            .expect("degraded leader should stop cleanly");
        global_metrics().set_cycle(None).await;
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    })
    .await;
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

/// A corrupt `.bloomcycle.bin` is a 24-byte counter. After the quarantine
/// marker is durable, the next leader iteration rebuilds the counter from the
/// usage floor through the ordinary fenced leadership claim. The marker stays
/// behind as evidence and the supported operator reset can still remove it.
#[tokio::test]
#[serial]
async fn quarantined_corrupt_cycle_state_is_rebuilt_on_the_next_iteration() {
    temp_env::async_with_vars(SUPERVISOR_ENV, async {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        reset_recovery_status();
        let (_dir, store) = setup_scanner_cycle_store().await;
        save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), valid_usage_floor(3, 41))
            .await
            .expect("usage floor should persist");
        save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), vec![0xff, 0x00, 0x01])
            .await
            .expect("corrupt cycle state should persist");
        assert!(!crate::scanner_runtime_initialized(), "test must own the scanner runtime");
        let ctx = CancellationToken::new();
        let _cancel_on_drop = ctx.clone().drop_guard();
        init_data_scanner_with_storage(ctx.clone(), store.clone()).await;

        let marker = tokio::time::timeout(WAIT_BUDGET, async {
            loop {
                if let Ok(marker) = read_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await {
                    break marker;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the first iteration must quarantine the corrupt cycle state");
        let marker: ScannerCycleRecoveryMarker = serde_json::from_slice(&marker).expect("quarantine marker should decode");
        assert_eq!(marker.classification, "corrupt");
        assert_eq!(marker.state, "blocked");
        let quarantined = tokio::time::timeout(WAIT_BUDGET, async {
            loop {
                let status = scanner_cycle_recovery_status();
                if status.primary_revision.as_deref() == Some(marker.primary_revision.as_str()) {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("quarantine must be visible in the local recovery status");
        assert!(quarantined.retryable, "a corrupt counter must schedule its own rebuild: {quarantined:?}");
        notify_scanner_cycle_recovery_wake();

        let (rebuilt, epoch) = wait_for_persisted_cycle(&store, |_, epoch| epoch >= 1).await;
        assert!(rebuilt.next >= 42, "rebuilt counter must honor the usage floor cycle: {rebuilt:?}");
        assert!(epoch >= 4, "rebuilt epoch must exceed the usage floor epoch: {epoch}");
        let healthy = wait_for_recovery_state("healthy").await;
        assert!(!healthy.retryable);
        assert!(
            healthy.rebuilt_from_quarantine_at_unix_secs.is_some(),
            "the rebuild must be recorded in the recovery status: {healthy:?}"
        );
        assert_eq!(healthy.leader_epoch, Some(epoch));
        let retained = read_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
            .await
            .expect("quarantine evidence must be retained after the automatic rebuild");
        let retained: ScannerCycleRecoveryMarker = serde_json::from_slice(&retained).expect("retained marker should decode");
        assert_eq!(
            retained.primary_revision, marker.primary_revision,
            "evidence must still name the corrupt revision"
        );
        stop_supervisor(ctx).await;

        // A restart with the stale marker starts normally, and the supported
        // operator reset is still the way to remove the evidence.
        assert!(matches!(
            load_scanner_cycle_state_for_startup(store.clone()).await,
            ScannerCycleStateStartup::Ready { .. }
        ));
        reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
            .await
            .expect("operator reset clears a stale quarantine marker");
        assert!(matches!(
            read_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
            Err(EcstoreError::ConfigNotFound)
        ));
        global_metrics().set_cycle(None).await;
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    })
    .await;
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

/// A future-schema counter was written by a newer binary. An older reader
/// must keep failing closed instead of rebuilding over it.
#[tokio::test]
#[serial]
async fn future_schema_cycle_state_stays_blocked_after_quarantine() {
    reset_recovery_status();
    let store = Arc::new(MemoryConfigStore::default());
    let state_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    let mut future = 17_u64.to_le_bytes().to_vec();
    future.extend_from_slice(b"RSCYC999");
    future.extend_from_slice(&4_u64.to_le_bytes());
    store.objects.lock().await.insert(state_key.clone(), future);
    store.revisions.lock().await.insert(state_key.clone(), 13);

    for load in 0..2 {
        assert!(
            matches!(
                load_scanner_cycle_state_for_startup(store.clone()).await,
                ScannerCycleStateStartup::Blocked
            ),
            "load {load}"
        );
        let status = scanner_cycle_recovery_status();
        assert_eq!(status.classification.as_deref(), Some("future_schema"));
        assert!(!status.retryable, "load {load}: {status:?}");
        assert_eq!(status.state, "blocked", "load {load}: {status:?}");
    }
    assert!(store.objects.lock().await.contains_key(&state_key), "future state must not be rewritten");
}

/// rustfs/rustfs#8670: every usage slot is corrupt and the cycle state is
/// missing. The full-rebuild reset starts from an empty baseline instead of
/// refusing with "no trusted cycle or usage floor".
#[tokio::test]
#[serial]
async fn full_rebuild_reset_starts_from_empty_baseline_without_trusted_floor_or_cycle() {
    reset_recovery_status();
    let (_dir, store) = setup_scanner_cycle_store_with_usage_baseline(false).await;
    for path in [
        DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
        format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
    ] {
        save_config(store.clone(), &path, b"not-json".to_vec())
            .await
            .expect("corrupt usage slot should persist");
    }
    assert!(matches!(
        read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH).await,
        Err(EcstoreError::ConfigNotFound)
    ));
    assert!(
        persisted_usage_floor_for_startup(store.clone(), true).await.is_err(),
        "corrupt usage slots must keep startup fail-closed until the operator resets"
    );

    let result = reset_scanner_usage_state_for_full_rebuild(CancellationToken::new(), store.clone())
        .await
        .expect("full-rebuild reset must recover a cluster whose floor and cycle are both untrusted");
    assert_eq!(result.leader_epoch, 1);
    assert_eq!(result.next_cycle, 0);
    assert_eq!(result.usage_state, "bootstrap-pending");

    let (cycle, epoch) = persisted_cycle(&store).await.expect("reset must fence a fresh cycle state");
    assert_eq!((cycle.next, epoch), (0, 1));
    let (floor, startup) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("startup must accept the rebuilt bootstrap marker");
    assert_eq!(startup, PersistedUsageFloorStartup::BootstrapPending);
    assert_eq!(floor.leader_epoch, 1);
    assert_eq!(scanner_cycle_recovery_status().state, "healthy");
}

async fn seeded_memory_cycle(epoch: u64) -> (Arc<MemoryConfigStore>, CurrentCycle, DataUsageCacheRevision) {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut cycle = CurrentCycle {
        next: 12,
        ..Default::default()
    };
    let mut revision = DataUsageCacheRevision::Missing;
    assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, epoch).await);
    (store, cycle, revision)
}

async fn put_count(store: &MemoryConfigStore) -> usize {
    store
        .put_counts
        .lock()
        .await
        .get(&memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str()))
        .copied()
        .unwrap_or(0)
}

/// rustfs/rustfs#8141: a transient write failure is retried under the same
/// epoch inside the same save, so the leader never has to reclaim and fence
/// in-flight remote scans with a new epoch.
#[tokio::test]
async fn cycle_state_save_retries_a_transient_write_failure_under_the_same_epoch() {
    let (store, mut cycle, mut revision) = seeded_memory_cycle(7).await;
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    store.fail_put_number.lock().await.insert(key, 2);
    cycle.next = 13;

    let ctx = CancellationToken::new();
    assert!(
        persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 7).await,
        "one failed write must be retried under the same epoch"
    );
    assert_eq!(put_count(&store).await, 3);
    let (durable, epoch) = decode_scanner_cycle_state(
        &read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("durable cycle state"),
    )
    .expect("durable cycle state should decode");
    assert_eq!((durable.next, epoch), (13, 7));
    assert_eq!(revision, DataUsageCacheRevision::Etag("memory-2".to_string()));
}

/// A write that committed but reported an error is confirmed by reading the
/// durable state back; no second write and no epoch change are needed.
#[tokio::test]
async fn cycle_state_save_confirms_a_post_commit_failure_from_durable_state() {
    let (store, mut cycle, mut revision) = seeded_memory_cycle(7).await;
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    store.error_after_commit_put_number.lock().await.insert(key, 2);
    cycle.next = 13;

    let ctx = CancellationToken::new();
    assert!(
        persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 7).await,
        "a committed write must be confirmed from durable state"
    );
    assert_eq!(put_count(&store).await, 2, "confirmation must not rewrite the committed state");
    assert_eq!(cycle.next, 13);
    assert_eq!(revision, DataUsageCacheRevision::Etag("memory-2".to_string()));
}

/// A CAS conflict that reveals a newer leadership epoch is a real fence: the
/// save yields immediately and never retries against the newer owner.
#[tokio::test]
async fn cycle_state_save_yields_to_a_newer_epoch_without_retrying() {
    let (store, mut cycle, mut revision) = seeded_memory_cycle(7).await;
    let mut newer = cycle.clone();
    newer.next = 20;
    let mut newer_revision = revision.clone();
    let ctx = CancellationToken::new();
    assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut newer, &mut newer_revision, 8).await);
    let puts_before = put_count(&store).await;
    cycle.next = 13;

    assert!(
        !persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 7).await,
        "a newer epoch owns the cycle state"
    );
    assert_eq!(put_count(&store).await, puts_before + 1, "a fenced save must not retry");
    let (durable, epoch) = decode_scanner_cycle_state(
        &read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("durable cycle state"),
    )
    .expect("durable cycle state should decode");
    assert_eq!((durable.next, epoch), (20, 8));
}

/// The retry schedule is bounded: it grows from the transient base (or the
/// shorter configured interval) and never exceeds the ten-minute cap.
#[test]
#[serial]
fn recovery_backoff_delay_is_capped() {
    temp_env::with_var_unset(rustfs_config::ENV_SCANNER_RECOVERY_BACKOFF_CAP_SECS, || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(scanner_cycle_recovery_backoff_delay(1, Duration::from_secs(60)), Duration::from_secs(5));
        assert_eq!(scanner_cycle_recovery_backoff_delay(2, Duration::from_secs(60)), Duration::from_secs(10));
        assert_eq!(scanner_cycle_recovery_backoff_delay(5, Duration::from_secs(60)), Duration::from_secs(80));
        assert_eq!(
            scanner_cycle_recovery_backoff_delay(8, Duration::from_secs(60)),
            Duration::from_secs(rustfs_config::DEFAULT_SCANNER_RECOVERY_BACKOFF_CAP_SECS)
        );
        assert_eq!(
            scanner_cycle_recovery_backoff_delay(u32::MAX, Duration::from_secs(60)),
            Duration::from_secs(rustfs_config::DEFAULT_SCANNER_RECOVERY_BACKOFF_CAP_SECS)
        );
        assert_eq!(scanner_cycle_recovery_backoff_delay(1, Duration::from_secs(1)), Duration::from_secs(1));
        assert_eq!(scanner_cycle_recovery_backoff_delay(0, Duration::ZERO), Duration::from_secs(1));
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
#[serial]
fn recovery_backoff_cap_configuration_controls_the_retry_schedule() {
    let (retry, status, before, after) = temp_env::with_var("RUSTFS_SCANNER_RECOVERY_BACKOFF_CAP_SECS", Some("17"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        reset_recovery_status();
        record_scanner_usage_floor_failure("retryable fixture outage".to_string());
        let before = unix_now_for_test();
        let retry = record_scanner_cycle_recovery_retry(12, Duration::from_secs(60));
        let status = scanner_cycle_recovery_status();
        (retry, status, before, unix_now_for_test())
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    reset_recovery_status();

    assert_eq!(
        retry.delay,
        Duration::from_secs(17),
        "the configured cap must reach the production retry schedule"
    );
    assert_eq!(retry.attempt, 12);
    assert_eq!(status.state, "backoff");
    assert!(status.retryable, "a configured cap cannot make an outage terminal");
    let next = status.next_attempt_at_unix_secs.expect("retry deadline is observable");
    assert!((before + 17..=after + 17).contains(&next));
    assert!(status.reason.expect("retry reason").contains("(in 17s)"));
}

#[test]
#[serial]
fn recovery_backoff_cap_hot_update_changes_the_next_retry_and_status() {
    use rustfs_config::server_config::{Config, KVS};
    use rustfs_config::{
        DEFAULT_DELIMITER, ENV_SCANNER_RECOVERY_BACKOFF_CAP_SECS, SCANNER_RECOVERY_BACKOFF_CAP, SCANNER_SUB_SYS,
    };

    crate::init_ecstore_config_for_scanner_tests();
    let observations = temp_env::with_var_unset(ENV_SCANNER_RECOVERY_BACKOFF_CAP_SECS, || {
        reset_recovery_status();
        record_scanner_usage_floor_failure("retryable fixture outage".to_string());
        [17_u64, 1200, 1]
            .into_iter()
            .enumerate()
            .map(|(index, cap)| {
                let mut config = Config::new();
                let mut kvs = KVS::new();
                kvs.insert(SCANNER_RECOVERY_BACKOFF_CAP.to_string(), cap.to_string());
                config.0.insert(
                    SCANNER_SUB_SYS.to_string(),
                    std::collections::HashMap::from([(DEFAULT_DELIMITER.to_string(), kvs)]),
                );
                let generation = crate::runtime_config::scanner_runtime_config_generation();
                crate::runtime_config::apply_scanner_runtime_config(&config).expect("valid hot update");
                assert!(crate::runtime_config::scanner_runtime_config_generation() > generation);
                let before = unix_now_for_test();
                let attempt = 12 + u32::try_from(index).expect("small fixture index");
                let retry = record_scanner_cycle_recovery_retry(attempt, Duration::from_secs(60));
                (
                    cap,
                    attempt,
                    retry,
                    scanner_cycle_recovery_status(),
                    crate::runtime_config::scanner_runtime_config_status(),
                    before,
                    unix_now_for_test(),
                )
            })
            .collect::<Vec<_>>()
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    reset_recovery_status();

    for (cap, attempt, retry, recovery, runtime, before, after) in observations {
        assert_eq!(retry.delay, Duration::from_secs(cap));
        assert_eq!(retry.attempt, attempt);
        assert_eq!(recovery.state, "backoff");
        assert!(recovery.retryable);
        assert_eq!(recovery.retry_count, attempt);
        let next = recovery.next_attempt_at_unix_secs.expect("next retry is observable");
        assert!((before + cap..=after + cap).contains(&next));
        assert!(recovery.reason.expect("retry reason").contains(&format!("(in {cap}s)")));
        assert_eq!(runtime.recovery_backoff_cap_seconds.value, cap);
        assert_eq!(
            runtime.recovery_backoff_cap_seconds.source,
            crate::runtime_config::ScannerRuntimeConfigSource::Config
        );
    }
}

/// An observation older than the daily window restarts its retry budget so
/// the visible count and the backoff do not grow without bound.
#[test]
#[serial]
fn recovery_retry_budget_restarts_after_the_daily_window() {
    reset_recovery_status();
    let stale_first_detected = unix_now_for_test().saturating_sub(24 * 60 * 60 + 1);
    publish_scanner_cleanup_status(
        ScannerCycleRecoveryStatus {
            path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
            state: "transient".to_string(),
            first_detected_at_unix_secs: Some(stale_first_detected),
            retry_count: 9,
            max_retries: MAX_SCANNER_CYCLE_RECOVERY_RETRIES,
            retryable: true,
            reason: Some("cycle state could not be inspected".to_string()),
            ..Default::default()
        },
        None,
    )
    .expect("seed stale observation");

    let retry = record_scanner_cycle_recovery_retry(10, Duration::from_secs(60));
    assert_eq!(retry.attempt, 1, "the budget restarts from the first attempt");
    assert_eq!(retry.delay, Duration::from_secs(5));
    let status = scanner_cycle_recovery_status();
    assert_eq!(status.retry_count, 1);
    assert_eq!(status.state, "transient");
    assert!(status.retryable);
    assert!(
        status
            .first_detected_at_unix_secs
            .is_some_and(|first| first > stale_first_detected)
    );
    reset_recovery_status();
}

fn unix_now_for_test() -> u64 {
    u64::try_from(Utc::now().timestamp()).expect("current time fits")
}

/// Unit view of the automatic rebuild: the first load quarantines and leaves
/// the observation retryable, the second load hands the quarantined revision
/// to the leadership claim instead of blocking.
#[tokio::test]
#[serial]
async fn corrupt_cycle_state_schedules_a_rebuild_after_its_marker_is_durable() {
    reset_recovery_status();
    let store = Arc::new(MemoryConfigStore::default());
    let state_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    let marker_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str());
    store.objects.lock().await.insert(state_key.clone(), vec![1]);
    store.revisions.lock().await.insert(state_key.clone(), 9);

    assert!(matches!(
        load_scanner_cycle_state_for_startup(store.clone()).await,
        ScannerCycleStateStartup::Blocked
    ));
    let quarantined = scanner_cycle_recovery_status();
    assert_eq!(quarantined.state, "blocked");
    assert_eq!(quarantined.classification.as_deref(), Some("corrupt"));
    assert!(quarantined.retryable, "{quarantined:?}");
    assert!(
        quarantined
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("next leader iteration rebuilds")),
        "{quarantined:?}"
    );
    let marker = store
        .objects
        .lock()
        .await
        .get(&marker_key)
        .cloned()
        .expect("quarantine marker must be durable before any rebuild");

    let ScannerCycleStateStartup::RebuildFromQuarantine { revision } = load_scanner_cycle_state_for_startup(store.clone()).await
    else {
        panic!(
            "a durable corrupt quarantine must schedule a rebuild: {:?}",
            scanner_cycle_recovery_status()
        );
    };
    assert_eq!(revision, DataUsageCacheRevision::Etag("memory-9".to_string()));
    let rebuilding = scanner_cycle_recovery_status();
    assert_eq!(rebuilding.state, "rebuilding");
    assert!(rebuilding.retryable);
    assert_eq!(rebuilding.primary_revision.as_deref(), Some("memory-9"));
    assert_eq!(
        store.objects.lock().await.get(&marker_key),
        Some(&marker),
        "scheduling must not touch the evidence"
    );
    assert_eq!(
        store.objects.lock().await.get(&state_key),
        Some(&vec![1]),
        "scheduling must not touch the primary"
    );

    // The claim CAS-replaces the quarantined revision under a higher epoch.
    seed_usage_snapshot_for_leadership_claim(&store).await;
    let mut cycle = CurrentCycle::default();
    let mut revision = revision;
    let mut leader_epoch = 0;
    assert!(
        claim_scanner_leadership(
            &CancellationToken::new(),
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut leader_epoch,
            false,
            ScannerCycleResetPolicy::None,
        )
        .await
    );
    assert_eq!(leader_epoch, 1);
    let (durable, epoch) = decode_scanner_cycle_state(store.objects.lock().await.get(&state_key).expect("rebuilt primary"))
        .expect("rebuilt primary should decode");
    assert_eq!((durable.next, epoch), (0, 1));
    assert_eq!(
        store.objects.lock().await.get(&marker_key),
        Some(&marker),
        "the rebuild keeps the evidence"
    );
    assert!(matches!(
        load_scanner_cycle_state_for_startup(store.clone()).await,
        ScannerCycleStateStartup::Ready { leader_epoch: 1, .. }
    ));
    reset_recovery_status();
}

fn backlog_observation(now: u64, paused: bool, movement_generation: u64) -> ScannerPauseBacklogObservation {
    ScannerPauseBacklogObservation {
        now_unix_secs: now,
        paused,
        movement_generation,
        movement_work_items: 0,
        pause_started_at_unix_secs: 0,
        dirty_usage_buckets: 0,
        discovered_expiry_items: 0,
        discovered_transition_items: 0,
    }
}

/// A degraded controller admits cycles, keeps the ordinary cadence, and once
/// the claim succeeds again the durable ledger resumes its catch-up phase.
#[tokio::test]
#[serial]
async fn degraded_pause_backlog_resumes_durable_catch_up_after_the_claim_recovers() {
    let (_dir, store) = setup_scanner_cycle_store().await;
    let now = scanner_pause_backlog_now();
    let mut durable = ScannerPauseBacklogController::claim(store.clone(), now)
        .await
        .expect("fresh ledger claim");
    durable.observe(backlog_observation(now.saturating_add(1), true, 7)).await;
    durable.observe(backlog_observation(now.saturating_add(2), false, 7)).await;
    let persisted = scanner_pause_backlog_status(store.clone()).await;
    assert_eq!(persisted.phase, ScannerPauseBacklogPhase::CatchingUp);
    assert!(persisted.pending_full_scan);
    drop(durable);

    let degraded_at = now.saturating_add(10);
    let mut controller =
        ScannerPauseBacklogController::unavailable(store.clone(), "simulated claim failure".to_string(), degraded_at);
    assert_eq!(controller.begin_attempt(degraded_at).await, ScannerPauseBacklogAttemptDecision::Degraded);
    assert_eq!(
        controller.begin_usage_bootstrap_rebuild_attempt(degraded_at).await,
        ScannerPauseBacklogAttemptDecision::Degraded
    );
    assert_eq!(
        controller.scheduling_delay(degraded_at, false),
        None,
        "degraded mode keeps the ordinary cadence"
    );
    let status = scanner_pause_backlog_status(store.clone()).await;
    assert_eq!(status.persistence_state, "degraded");
    assert!(
        status
            .error
            .as_deref()
            .is_some_and(|error| error.contains("simulated claim failure"))
    );

    // Before the refresh interval the observation does not re-claim.
    controller
        .observe(backlog_observation(degraded_at.saturating_add(1), false, 7))
        .await;
    assert_eq!(
        controller.begin_attempt(degraded_at.saturating_add(1)).await,
        ScannerPauseBacklogAttemptDecision::Degraded
    );

    // After it, the claim succeeds and the persisted catch-up phase is back.
    let recovered_at = degraded_at.saturating_add(5 * 60);
    controller.observe(backlog_observation(recovered_at, false, 7)).await;
    assert!(matches!(
        controller.begin_attempt(recovered_at).await,
        ScannerPauseBacklogAttemptDecision::Tracked(_)
    ));
    let recovered = scanner_pause_backlog_status(store.clone()).await;
    assert_eq!(recovered.persistence_state, "healthy", "{recovered:?}");
    assert!(recovered.durable);
    assert_eq!(recovered.phase, ScannerPauseBacklogPhase::CatchingUp);
    assert!(recovered.pending_full_scan, "catch-up debt survives the degraded window");
    assert!(recovered.error.is_none());
}

/// Exhausting the bounded in-save retries is reported as unconfirmed, not as
/// a fence: the caller reloads durable state under the same epoch.
#[tokio::test]
async fn cycle_state_save_reports_exhausted_retries_as_unconfirmed() {
    let (store, mut cycle, mut revision) = seeded_memory_cycle(7).await;
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    store.fail_puts_from.lock().await.insert(key, 2);
    cycle.next = 13;
    let ctx = CancellationToken::new();
    assert_eq!(
        persist_scanner_cycle_state_for_epoch(&ctx, store.clone(), &mut cycle, &mut revision, 7, None).await,
        ScannerCycleStatePersistOutcome::Unconfirmed
    );
    assert_eq!(put_count(&store).await, 1 + SCANNER_PERSIST_CAS_RETRIES + 1);
    assert_eq!(
        revision,
        DataUsageCacheRevision::Etag("memory-1".to_string()),
        "the durable revision is kept"
    );

    let fenced_store = Arc::new(MemoryConfigStore::default());
    let mut newer = CurrentCycle {
        next: 20,
        ..Default::default()
    };
    let mut newer_revision = DataUsageCacheRevision::Missing;
    assert!(persist_scanner_cycle_state(&ctx, fenced_store.clone(), &mut newer, &mut newer_revision, 8).await);
    let mut stale_revision = DataUsageCacheRevision::Etag("memory-0".to_string());
    assert_eq!(
        persist_scanner_cycle_state_for_epoch(&ctx, fenced_store.clone(), &mut cycle, &mut stale_revision, 7, None).await,
        ScannerCycleStatePersistOutcome::Fenced
    );
}
