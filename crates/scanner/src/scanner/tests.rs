// Copyright 2024 RustFS Team
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

use super::heal_info::{classify_background_heal_read_error, decode_background_heal_info};
use super::*;
use crate::EcstoreResult;
use crate::storage_api::owner::ecstore_hold_namespace_commit;
use crate::storage_api::scan::{BucketOperations as _, ObjectIO as _};
use crate::{
    DATA_USAGE_BLOOM_RECOVERY_PATH, DATA_USAGE_CACHE_KEY_FORMAT, DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT,
    DataUsageCachePrepareOutcome, DataUsageCacheSource, DataUsageEntry, DataUsageScanPlanDigest, Endpoint, EndpointServerPools,
    Endpoints, InstanceContext, PoolEndpoints, ScannerGetObjectReader as GetObjectReader, ScannerObjectInfo as ObjectInfo,
    ScannerObjectOptions as ObjectOptions, ScannerPutObjReader as PutObjReader, init_bucket_metadata_sys_for_scanner_tests,
    init_ecstore_config_for_scanner_tests, init_local_disks_with_instance_ctx,
};
use serial_test::serial;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::task::Poll;
use temp_env::{with_var, with_var_unset};
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use tokio::time::{Duration, advance};

const TEST_DEFAULT_SCANNER_CYCLE_SECS: u64 = 24 * 60 * 60;

mod quota_reset_preservation;

mod recovery_control;
mod scoped_ack_publication;

async fn setup_scanner_cycle_store() -> (tempfile::TempDir, Arc<ECStore>) {
    setup_scanner_cycle_store_with_usage_baseline(true).await
}

async fn setup_scanner_cycle_store_with_usage_baseline(seed_usage_baseline: bool) -> (tempfile::TempDir, Arc<ECStore>) {
    setup_scanner_cycle_store_with_pool_count(seed_usage_baseline, 1).await
}

async fn setup_scanner_cycle_store_with_pool_count(
    seed_usage_baseline: bool,
    pool_count: usize,
) -> (tempfile::TempDir, Arc<ECStore>) {
    init_ecstore_config_for_scanner_tests();
    let temp_dir = tempfile::tempdir().expect("scanner cycle test directory should be created");
    let mut pools = Vec::with_capacity(pool_count);
    for pool_index in 0..pool_count {
        let mut endpoints = Vec::new();
        for disk_index in 0..4 {
            let disk_path = temp_dir.path().join(format!("pool{pool_index}/disk{disk_index}"));
            tokio::fs::create_dir_all(&disk_path)
                .await
                .expect("scanner cycle test disk should be created");
            let mut endpoint =
                Endpoint::try_from(disk_path.to_str().expect("disk path should be utf8")).expect("endpoint should parse");
            endpoint.set_pool_index(pool_index);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            endpoints.push(endpoint);
        }
        pools.push(PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: if pool_count == 1 {
                "scanner-cycle-metrics".to_string()
            } else {
                format!("scanner-cycle-metrics-pool-{pool_index}")
            },
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        });
    }
    let endpoint_pools = EndpointServerPools::from(pools);
    let instance_ctx = Arc::new(InstanceContext::new());
    instance_ctx.set_endpoints(endpoint_pools.clone());
    init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
        .await
        .expect("scanner cycle test disks should initialize");
    let store = ECStore::new_with_instance_ctx(
        "127.0.0.1:0".parse().expect("test address should parse"),
        endpoint_pools,
        CancellationToken::new(),
        instance_ctx,
    )
    .await
    .expect("scanner cycle test ECStore should initialize");
    init_bucket_metadata_sys_for_scanner_tests(store.clone()).await;
    if seed_usage_baseline {
        save_config(
            store.clone(),
            DATA_USAGE_OBJ_NAME_PATH.as_str(),
            serde_json::to_vec(&complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0))
                .expect("scanner cycle usage baseline should encode"),
        )
        .await
        .expect("scanner cycle usage baseline should persist");
    }

    (temp_dir, store)
}

async fn restart_scanner_cycle_store_from(store: &Arc<ECStore>) -> Arc<ECStore> {
    let endpoint_pools = store
        .instance_endpoints()
        .expect("scanner restart test store should retain its endpoint topology");
    let instance_ctx = Arc::new(InstanceContext::new());
    instance_ctx.set_endpoints(endpoint_pools.clone());
    init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
        .await
        .expect("scanner restart test disks should reinitialize");
    let restarted = ECStore::new_with_instance_ctx(
        "127.0.0.1:0".parse().expect("test address should parse"),
        endpoint_pools,
        CancellationToken::new(),
        instance_ctx,
    )
    .await
    .expect("restarted scanner cycle test ECStore should initialize");
    init_bucket_metadata_sys_for_scanner_tests(restarted.clone()).await;
    restarted
}

fn assert_run_data_scanner_signature<F, Fut>(_run: F)
where
    F: Fn(CancellationToken, Arc<ECStore>) -> Fut,
    Fut: Future<Output = Result<(), ScannerError>>,
{
}

#[test]
fn run_data_scanner_keeps_its_two_argument_api() {
    assert_run_data_scanner_signature(run_data_scanner);
}

#[tokio::test]
async fn restarted_main_loop_completes_durable_pause_backlog_catch_up() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    global_metrics().set_cycle(None).await;
    let (_temp_dir, store) = setup_scanner_cycle_store().await;

    let paused_at = scanner_pause_backlog_now();
    let mut seeded = ScannerPauseBacklogController::claim(store.clone(), paused_at)
        .await
        .expect("seed writer should claim the durable pause backlog");
    seeded
        .observe(ScannerPauseBacklogObservation {
            now_unix_secs: paused_at.saturating_add(1),
            paused: true,
            movement_generation: store.scanner_data_movement_generation().saturating_add(1),
            movement_work_items: 1,
            pause_started_at_unix_secs: paused_at.saturating_add(1),
            dirty_usage_buckets: 0,
            discovered_expiry_items: 0,
            discovered_transition_items: 0,
        })
        .await;
    drop(seeded);

    let seeded_status = scanner_pause_backlog_status(store.clone()).await;
    assert!(seeded_status.durable, "seeded pause backlog must be set-backed");
    assert_eq!(seeded_status.phase, ScannerPauseBacklogPhase::Paused);
    assert!(seeded_status.pending_full_scan);
    assert_eq!(seeded_status.catch_up_attempts, 0);

    let restarted = restart_scanner_cycle_store_from(&store).await;
    assert!(
        restarted.instance_endpoints().is_some(),
        "restarted scanner store must retain instance endpoints"
    );
    let restarted_status = scanner_pause_backlog_status(restarted.clone()).await;
    assert_eq!(restarted_status.phase, ScannerPauseBacklogPhase::Paused);
    assert_eq!(restarted_status.generation, seeded_status.generation);

    let ctx = CancellationToken::new();
    let scanner_ctx = ctx.clone();
    let scanner_store = restarted.clone();
    let scanner_task = tokio::spawn(async move { run_data_scanner(scanner_ctx, scanner_store).await });

    let final_status = match tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = scanner_pause_backlog_status(restarted.clone()).await;
            if status.phase == ScannerPauseBacklogPhase::Idle
                && status.writer_epoch > seeded_status.writer_epoch
                && status.catch_up_attempts > seeded_status.catch_up_attempts
            {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    {
        Ok(status) => status,
        Err(err) => {
            ctx.cancel();
            scanner_task.abort();
            panic!("restarted scanner did not complete durable catch-up through the main loop: {err}");
        }
    };

    ctx.cancel();
    tokio::time::timeout(Duration::from_secs(5), scanner_task)
        .await
        .expect("scanner loop should stop after cancellation")
        .expect("scanner task should not panic")
        .expect("scanner loop should exit cleanly");

    assert!(final_status.durable);
    assert_eq!(final_status.phase, ScannerPauseBacklogPhase::Idle);
    assert!(!final_status.pending_full_scan);
    assert_eq!(final_status.pending_work_items, 0);
    assert_eq!(final_status.consecutive_failures, 0);
    assert!(final_status.pause_ended_at_unix_secs >= final_status.pause_started_at_unix_secs);

    let usage = read_config(restarted.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("the catch-up scanner cycle should leave an authoritative usage snapshot readable");
    let usage = serde_json::from_slice::<DataUsageInfo>(&usage).expect("authoritative usage snapshot should decode");
    assert!(
        usage.is_complete_bucket_usage_snapshot(),
        "durable catch-up must run a complete scanner cycle before clearing the backlog"
    );

    global_metrics().set_cycle(None).await;
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn running_main_loop_catches_up_pause_cleared_after_startup_observe() {
    temp_env::async_with_vars([(ENV_SCANNER_CYCLE, Some("1")), (ENV_SCANNER_START_DELAY_SECS, Some("0"))], async {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        global_metrics().set_cycle(None).await;
        let (_temp_dir, store) = setup_scanner_cycle_store_with_pool_count(true, 2).await;

        let ctx = CancellationToken::new();
        let scanner_ctx = ctx.clone();
        let scanner_store = store.clone();
        let startup_probe = ScannerStartupObservedProbe::install();
        let scanner_task = tokio::spawn(async move { run_data_scanner(scanner_ctx, scanner_store).await });
        startup_probe.wait().await;
        let ready_probe = ScannerRuntimeObservedProbe::install(&store, false);
        startup_probe.resume();
        drop(startup_probe);
        ready_probe.wait().await;
        drop(ready_probe);

        let paused_probe = ScannerRuntimeObservedProbe::install(&store, true);
        let paused_at = time::OffsetDateTime::now_utc();
        {
            let mut pool_meta = store.pool_meta.write().await;
            pool_meta.pools[0].last_update = paused_at;
            pool_meta.pools[0].decommission = Some(crate::storage_api::owner::EcstorePoolDecommissionInfo {
                failed: true,
                ..Default::default()
            });
        }
        let pause_status = store.scanner_data_movement_pause_status().await;
        assert!(pause_status.paused);
        assert_eq!(
            scanner_local_publication_defer_reason(store.as_ref()).await,
            Some(ScannerCycleDeferReason::DataMovement),
            "an actual data-movement pause must retain durable catch-up tracking"
        );
        paused_probe.wait().await;
        drop(paused_probe);

        let paused_backlog = scanner_pause_backlog_status(store.clone()).await;
        assert_eq!(paused_backlog.phase, ScannerPauseBacklogPhase::Paused);
        assert!(paused_backlog.pending_full_scan);

        let resumed_probe = ScannerRuntimeObservedProbe::install(&store, false);
        store
            .clear_decommission(0)
            .await
            .expect("terminal decommission clear should publish a movement generation");
        resumed_probe.wait().await;
        drop(resumed_probe);

        let final_status = match tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let status = scanner_pause_backlog_status(store.clone()).await;
                if status.phase == ScannerPauseBacklogPhase::Idle
                    && status.writer_epoch == paused_backlog.writer_epoch
                    && status.catch_up_attempts > paused_backlog.catch_up_attempts
                {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        {
            Ok(status) => status,
            Err(err) => {
                ctx.cancel();
                scanner_task.abort();
                panic!("running scanner did not complete durable catch-up after a runtime movement clear: {err}");
            }
        };

        ctx.cancel();
        tokio::time::timeout(Duration::from_secs(5), scanner_task)
            .await
            .expect("scanner loop should stop after cancellation")
            .expect("scanner task should not panic")
            .expect("scanner loop should exit cleanly");

        assert!(final_status.durable);
        assert_eq!(final_status.phase, ScannerPauseBacklogPhase::Idle);
        assert_eq!(final_status.writer_epoch, paused_backlog.writer_epoch);
        assert!(!final_status.pending_full_scan);
        assert_eq!(final_status.pending_work_items, 0);
        assert_eq!(final_status.consecutive_failures, 0);

        global_metrics().set_cycle(None).await;
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    })
    .await;
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[tokio::test]
async fn scanner_cycle_lock_fence_cancels_cycle_context() {
    let cycle_ctx = CancellationToken::new();
    let observed_ctx = cycle_ctx.clone();
    let output = await_scanner_cycle_with_lock_fence(
        &cycle_ctx,
        async move {
            observed_ctx.cancelled().await;
            observed_ctx.is_cancelled()
        },
        std::future::ready(()),
    )
    .await;

    assert_eq!(output, Some(true));
    assert!(cycle_ctx.is_cancelled());
}

#[tokio::test]
async fn scanner_cycle_lock_fence_preserves_completed_cycle() {
    let cycle_ctx = CancellationToken::new();
    let output = await_scanner_cycle_with_lock_fence(&cycle_ctx, std::future::ready(7_u8), std::future::pending()).await;

    assert_eq!(output, Some(7));
    assert!(!cycle_ctx.is_cancelled());
}

#[tokio::test]
async fn scanner_cycle_lock_fence_bounds_uncooperative_shutdown() {
    let cycle_ctx = CancellationToken::new();
    let output = await_scanner_cycle_with_lock_fence(&cycle_ctx, std::future::pending::<()>(), std::future::ready(())).await;

    assert_eq!(output, None);
    assert!(cycle_ctx.is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn cycle_budget_fences_late_writer_after_timeout() {
    let cycle_ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &cycle_ctx,
        ScannerCycleBudgetConfig {
            max_duration: Some(Duration::from_secs(5)),
            ..Default::default()
        },
    );
    let outcome = {
        let cycle = std::future::pending::<()>();
        let lock_lost = std::future::pending::<()>();
        let waiter = await_scanner_cycle_with_budget_fence(&cycle_ctx, &budget, cycle, lock_lost);
        tokio::pin!(waiter);
        tokio::task::yield_now().await;
        advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        advance(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT).await;
        waiter.await
    };
    assert_eq!(outcome, ScannerCycleWaitOutcome::Deadline { worker_stopped: false });
    assert!(cycle_ctx.is_cancelled());
    assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Runtime));

    // A newer leadership epoch is the durable fence that rejects a late
    // writer after the timed-out future has been dropped.
    let store = Arc::new(MemoryConfigStore::default());
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        current: 0,
        next: 12,
        ..Default::default()
    };
    let persist_ctx = CancellationToken::new();
    assert!(persist_scanner_cycle_state(&persist_ctx, store.clone(), &mut cycle, &mut revision, 1).await);
    let newer = encode_scanner_cycle_state(&cycle, 2).expect("new epoch fence should encode");
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    store.interleaving_puts.lock().await.insert(key, (2, newer));
    let mut late_cycle = CurrentCycle { next: 13, ..cycle };
    assert!(!persist_scanner_cycle_state(&persist_ctx, store, &mut late_cycle, &mut revision, 1).await);
}

#[tokio::test(start_paused = true)]
async fn cycle_budget_parent_cancellation_is_not_reported_as_timeout() {
    let cycle_ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &cycle_ctx,
        ScannerCycleBudgetConfig {
            max_duration: Some(Duration::from_secs(5)),
            ..Default::default()
        },
    );
    let waiter = await_scanner_cycle_with_budget_fence(&cycle_ctx, &budget, std::future::pending::<()>(), std::future::pending());
    tokio::pin!(waiter);
    tokio::task::yield_now().await;
    cycle_ctx.cancel();
    tokio::task::yield_now().await;
    advance(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT).await;
    assert_eq!(waiter.await, ScannerCycleWaitOutcome::Cancelled);
}

#[tokio::test(start_paused = true)]
async fn cycle_budget_deadline_wins_same_tick_as_parent_cancellation() {
    let cycle_ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &cycle_ctx,
        ScannerCycleBudgetConfig {
            max_duration: Some(Duration::from_secs(5)),
            ..Default::default()
        },
    );
    let waiter = await_scanner_cycle_with_budget_fence(&cycle_ctx, &budget, std::future::pending::<()>(), std::future::pending());
    tokio::pin!(waiter);
    tokio::task::yield_now().await;
    advance(Duration::from_secs(5)).await;
    cycle_ctx.cancel();
    tokio::task::yield_now().await;
    advance(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT).await;

    assert_eq!(waiter.await, ScannerCycleWaitOutcome::Deadline { worker_stopped: false });
    assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Runtime));
}

#[tokio::test]
async fn cycle_budget_persist_cursor_failure_is_recovery_required() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    store.fail_put_number.lock().await.insert(key, 1);

    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        current: 12,
        next: 12,
        ..Default::default()
    };
    let mut leader_epoch = 1;
    let fenced = fence_scanner_epoch_after_cycle_timeout(
        &ctx,
        store,
        &mut cycle,
        &mut revision,
        &mut leader_epoch,
        false,
        std::future::pending(),
    )
    .await;
    assert!(!fenced, "a failed cursor/generation write must require recovery");
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    assert!(cycle_timeout_requires_recovery(true, budget.cycle_state_persisted(), fenced));

    let metrics = Metrics::new();
    metrics.record_scanner_cycle_timeout(!fenced, Duration::from_secs(17));
    let report = metrics.report().await;
    assert_eq!(report.cycle_timeout_total, 1);
    assert_eq!(report.cycle_recovery_required_total, 1);
    assert_eq!(report.cycle_last_progress_age, 17);
    assert!(report.leader_lease_without_progress);
}

#[tokio::test]
async fn cycle_budget_fence_accepts_bootstrap_pending_usage_marker() {
    let store = Arc::new(MemoryConfigStore::default());
    initialize_usage_baseline_bootstrap(store.clone())
        .await
        .expect("usage reset should publish a bootstrap marker");

    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        current: 12,
        next: 12,
        ..Default::default()
    };
    let mut leader_epoch = 0;

    let fenced = fence_scanner_epoch_after_cycle_timeout(
        &ctx,
        store.clone(),
        &mut cycle,
        &mut revision,
        &mut leader_epoch,
        true,
        std::future::pending(),
    )
    .await;

    assert!(fenced, "a valid reset bootstrap marker must not force cycle recovery after budget expiry");
    assert!(!cycle_timeout_requires_recovery(true, true, fenced));
    assert_eq!(leader_epoch, 1);

    let persisted_cycle = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("timeout fence should persist the next leader epoch");
    let (_, persisted_epoch) = decode_scanner_cycle_state(&persisted_cycle).expect("persisted epoch fence should decode");
    assert_eq!(persisted_epoch, 1);

    let usage = read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("timeout fence should keep the bootstrap usage marker");
    let usage = serde_json::from_slice::<DataUsageInfo>(&usage).expect("bootstrap marker should decode");
    assert!(data_usage_info_is_bootstrap_pending(&usage));
    assert_eq!(usage.scanner_epoch, Some(1));
    assert!(!data_usage_info_has_persisted_baseline_identity(&usage));
}

#[tokio::test]
async fn cycle_budget_deadline_handler_fences_and_releases_guard() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let lock = store
        .new_ns_lock(RUSTFS_META_BUCKET, "leader.lock")
        .await
        .expect("scanner leader lock should be created");
    let mut guard = lock
        .get_write_lock(Duration::from_secs(1))
        .await
        .expect("scanner leader lock should be acquired");

    let ctx = CancellationToken::new();
    let mut cycle_info = CurrentCycle {
        current: 12,
        next: 12,
        ..Default::default()
    };
    let mut cycle_revision = DataUsageCacheRevision::Missing;
    let mut leader_epoch = 1;
    let budget = ScannerCycleBudget::new(
        &ctx,
        ScannerCycleBudgetConfig {
            max_duration: Some(Duration::from_secs(60)),
            ..Default::default()
        },
    );
    budget.mark_cycle_state_persisted();

    handle_scanner_cycle_deadline(
        &ctx,
        store.clone(),
        ScannerCycleDeadlineState {
            cycle_info: &mut cycle_info,
            cycle_revision: &mut cycle_revision,
            leader_epoch: &mut leader_epoch,
            cycle_budget: &budget,
            allow_bootstrap_pending: false,
        },
        true,
        &mut guard,
    )
    .await;

    assert!(guard.is_released());
    let persisted = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("deadline handler should persist a fenced cursor");
    let (_, persisted_epoch) = decode_scanner_cycle_state(&persisted).expect("fenced cursor should decode");
    assert_eq!(persisted_epoch, 2);
    global_metrics().set_cycle(None).await;
}

#[tokio::test]
async fn scanner_cycle_recovery_wake_survives_wait_registration_race() {
    notify_scanner_cycle_recovery_wake();

    tokio::time::timeout(Duration::from_secs(1), SCANNER_CYCLE_RECOVERY_WAKE.notified())
        .await
        .expect("recovery wake should retain a permit until the waiter registers");
}

struct ScannerDefaultSpeedGuard;

impl ScannerDefaultSpeedGuard {
    fn set(speed: ScannerSpeed) -> Self {
        set_scanner_default_speed(speed);
        Self
    }
}

impl Drop for ScannerDefaultSpeedGuard {
    fn drop(&mut self) {
        set_scanner_default_speed(ScannerSpeed::Default);
    }
}

struct ScannerDefaultCycleGuard;

impl ScannerDefaultCycleGuard {
    fn set(secs: u64) -> Self {
        set_scanner_default_cycle_secs(Some(secs));
        Self
    }
}

impl Drop for ScannerDefaultCycleGuard {
    fn drop(&mut self) {
        set_scanner_default_cycle_secs(None);
    }
}

#[derive(Debug, Default)]
struct MemoryConfigStore {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    revisions: Mutex<HashMap<String, u64>>,
    insert_after_gets: Mutex<HashMap<String, Vec<u8>>>,
    read_errors: Mutex<HashMap<String, EcstoreError>>,
    delayed_gets: Mutex<HashMap<String, Duration>>,
    non_regular_objects: Mutex<HashSet<String>>,
    fail_put_number: Mutex<HashMap<String, usize>>,
    object_not_found_put_number: Mutex<HashMap<String, usize>>,
    error_after_commit_put_number: Mutex<HashMap<String, usize>>,
    interleaving_puts: Mutex<HashMap<String, (usize, Vec<u8>)>>,
    cancel_after_interleaving_puts: Mutex<HashMap<String, CancellationToken>>,
    cancel_after_successful_puts: Mutex<HashMap<String, (usize, CancellationToken)>>,
    replace_after_successful_puts: Mutex<HashMap<String, (usize, Vec<u8>)>>,
    error_after_commit_deletes: Mutex<HashSet<String>>,
    cancel_after_deletes: Mutex<HashMap<String, CancellationToken>>,
    pause_next_publication_admission: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    put_counts: Mutex<HashMap<String, usize>>,
    publication_admission_blocked: AtomicBool,
    block_publication_after_admissions: AtomicUsize,
}

fn memory_config_key(bucket: &str, object: &str) -> String {
    format!("{bucket}/{object}")
}

async fn insert_usage_after_first_legacy_backup_read(store: &MemoryConfigStore) {
    let legacy_backup = format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str());
    let mut usage = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    usage.scanner_epoch = Some(7);
    usage.scanner_cycle = Some(11);
    store.insert_after_gets.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, &legacy_backup),
        serde_json::to_vec(&usage).expect("usage snapshot should encode"),
    );
}

#[async_trait::async_trait]
impl crate::storage_api::scanner_io::ObjectIO for MemoryConfigStore {
    type Error = EcstoreError;
    type RangeSpec = crate::storage_api::scanner_io::HTTPRangeSpec;
    type HeaderMap = http::HeaderMap;
    type ObjectOptions = ObjectOptions;
    type ObjectInfo = ObjectInfo;
    type GetObjectReader = GetObjectReader;
    type PutObjectReader = PutObjReader;

    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        _range: Option<crate::storage_api::scanner_io::HTTPRangeSpec>,
        _h: http::HeaderMap,
        _opts: &ObjectOptions,
    ) -> EcstoreResult<GetObjectReader> {
        let key = memory_config_key(bucket, object);
        if let Some(error) = self.read_errors.lock().await.get(&key).cloned() {
            return Err(error);
        }
        if let Some(delay) = self.delayed_gets.lock().await.remove(&key) {
            tokio::time::sleep(delay).await;
        }
        let inserted_data = self.insert_after_gets.lock().await.remove(&key);
        let data = {
            let mut objects = self.objects.lock().await;
            let data = objects.get(&key).cloned();
            if let Some(inserted_data) = inserted_data.as_ref() {
                objects.insert(key.clone(), inserted_data.clone());
            }
            data
        };
        if inserted_data.is_some() {
            let mut revisions = self.revisions.lock().await;
            let revision = revisions.get(&key).copied().unwrap_or(0) + 1;
            revisions.insert(key.clone(), revision);
        }
        let data = data.ok_or(EcstoreError::FileNotFound)?;
        let data_len = i64::try_from(data.len()).expect("memory test object length should fit in i64");
        let revision = *self.revisions.lock().await.entry(key.clone()).or_insert(1);
        let is_dir = self.non_regular_objects.lock().await.contains(&key);

        Ok(GetObjectReader {
            stream: Box::new(Cursor::new(data)),
            object_info: ObjectInfo {
                etag: Some(format!("memory-{revision}")),
                size: data_len,
                is_dir,
                ..Default::default()
            },
            buffered_body: None,
            body_source: Default::default(),
        })
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> EcstoreResult<ObjectInfo> {
        let mut buf = Vec::new();
        data.stream.read_to_end(&mut buf).await?;
        let key = memory_config_key(bucket, object);
        let put_count = {
            let mut put_counts = self.put_counts.lock().await;
            let put_count = put_counts.entry(key.clone()).or_insert(0);
            *put_count += 1;
            *put_count
        };

        if self.fail_put_number.lock().await.get(&key) == Some(&put_count) {
            return Err(EcstoreError::other("injected put failure"));
        }
        if self.object_not_found_put_number.lock().await.get(&key) == Some(&put_count) {
            return Err(EcstoreError::ObjectNotFound(bucket.to_string(), object.to_string()));
        }

        let interleaving_data = {
            let mut interleaving_puts = self.interleaving_puts.lock().await;
            if interleaving_puts
                .get(&key)
                .is_some_and(|(expected_put, _)| *expected_put == put_count)
            {
                interleaving_puts.remove(&key).map(|(_, data)| data)
            } else {
                None
            }
        };
        let cancel_after_interleaving = if interleaving_data.is_some() {
            self.cancel_after_interleaving_puts.lock().await.remove(&key)
        } else {
            None
        };
        let replacement = {
            let mut replacements = self.replace_after_successful_puts.lock().await;
            if replacements
                .get(&key)
                .is_some_and(|(expected_put, _)| *expected_put == put_count)
            {
                replacements.remove(&key).map(|(_, replacement)| replacement)
            } else {
                None
            }
        };
        let mut objects = self.objects.lock().await;
        let mut revisions = self.revisions.lock().await;
        if let Some(interleaving_data) = interleaving_data {
            let revision = revisions.get(&key).copied().unwrap_or(0) + 1;
            objects.insert(key.clone(), interleaving_data);
            revisions.insert(key.clone(), revision);
            if let Some(cancel) = cancel_after_interleaving {
                cancel.cancel();
            }
        }
        let current_revision = objects.contains_key(&key).then(|| revisions.get(&key).copied().unwrap_or(1));
        if let Some(preconditions) = &opts.http_preconditions {
            if preconditions
                .if_none_match
                .as_deref()
                .is_some_and(|condition| !condition.trim().is_empty())
                && current_revision.is_some()
            {
                return Err(EcstoreError::PreconditionFailed);
            }
            if let Some(expected) = preconditions
                .if_match
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                let actual = current_revision.map(|revision| format!("memory-{revision}"));
                if actual.as_deref() != Some(expected.trim_matches('"')) {
                    return Err(EcstoreError::PreconditionFailed);
                }
            }
        }

        let revision = current_revision.unwrap_or(0) + 1;
        objects.insert(key.clone(), buf);
        revisions.insert(key.clone(), revision);
        if let Some(replacement) = replacement {
            objects.insert(key.clone(), replacement);
            revisions.insert(key.clone(), revision + 1);
        }
        drop(revisions);
        drop(objects);
        let cancel_after_success = {
            let mut cancellations = self.cancel_after_successful_puts.lock().await;
            if cancellations
                .get(&key)
                .is_some_and(|(expected_put, _)| *expected_put == put_count)
            {
                cancellations.remove(&key).map(|(_, cancel)| cancel)
            } else {
                None
            }
        };
        if let Some(cancel) = cancel_after_success {
            cancel.cancel();
        }
        if self.error_after_commit_put_number.lock().await.get(&key) == Some(&put_count) {
            return Err(EcstoreError::other("injected post-commit put failure"));
        }
        Ok(ObjectInfo {
            etag: Some(format!("memory-{revision}")),
            ..Default::default()
        })
    }
}

fn with_unset_scanner_timing_env(f: impl FnOnce()) {
    with_var_unset(ENV_SCANNER_SPEED, || {
        with_var_unset("MINIO_SCANNER_SPEED", || {
            with_var_unset(ENV_SCANNER_CYCLE, || {
                with_var_unset("MINIO_SCANNER_CYCLE", || {
                    with_var_unset(ENV_SCANNER_START_DELAY_SECS, || {
                        with_var_unset(ENV_SCANNER_START_DELAY_SECS_DEPRECATED, f);
                    });
                });
            });
        });
    });
}

#[test]
fn test_randomized_cycle_delay_keeps_configured_start_delay() {
    // 120s with ±10% jitter should stay clearly above the historic 30s cap.
    let delay = randomized_cycle_delay_for(Duration::from_secs(120));
    assert!(delay > Duration::from_secs(30), "expected delay > 30s, got {delay:?}");
    // Jitter window should stay within configured bounds.
    assert!(delay >= Duration::from_secs(108));
    assert!(delay <= Duration::from_secs(132));
}

#[test]
fn test_randomized_cycle_delay_bounds_extreme_interval() {
    let delay = randomized_cycle_delay_for(Duration::MAX);

    assert!(delay >= MAX_SCANNER_SCHEDULE_DELAY.mul_f64(0.9));
    assert!(delay <= MAX_SCANNER_SCHEDULE_DELAY);
}

#[test]
fn test_initial_scanner_delay_uses_configured_start_delay() {
    let delay = initial_scanner_delay_for(Some(120));
    assert!(delay >= Duration::from_secs(108));
    assert!(delay <= Duration::from_secs(132));
}

#[test]
#[serial]
fn test_initial_scanner_delay_uses_cycle_without_explicit_start_delay() {
    with_var(ENV_SCANNER_CYCLE, Some("120"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        let delay = initial_scanner_delay_for(None);
        assert!(delay >= Duration::from_secs(108));
        assert!(delay <= Duration::from_secs(132));
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
fn test_initial_scanner_delay_skips_for_cold_usage_cache_with_buckets() {
    let delay = initial_scanner_delay_for_startup(Some(120), true, true, false);
    assert_eq!(delay, Duration::ZERO);
}

#[test]
fn test_initial_scanner_delay_keeps_configured_delay_for_warm_usage_cache_no_replication() {
    let delay = initial_scanner_delay_for_startup(Some(120), false, true, false);
    assert!(delay >= Duration::from_secs(108));
    assert!(delay <= Duration::from_secs(132));
}

#[test]
fn test_initial_scanner_delay_skips_for_cold_usage_cache_without_buckets() {
    let delay = initial_scanner_delay_for_startup(Some(120), true, false, false);
    assert_eq!(delay, Duration::ZERO);
}

#[test]
fn test_initial_scanner_delay_skips_for_active_replication_warm_cache() {
    // Warm cache + active replication rules → skip startup delay so that FAILED-status objects
    // from a crash are healed on the first cycle, not after a 27-33 min sleep.
    let delay = initial_scanner_delay_for_startup(Some(120), false, true, true);
    assert_eq!(delay, Duration::ZERO);
}

#[test]
fn test_initial_scanner_delay_keeps_delay_for_replication_without_buckets() {
    // Active replication but no buckets → no objects to scan, keep normal delay.
    let delay = initial_scanner_delay_for_startup(Some(120), false, false, true);
    assert!(delay >= Duration::from_secs(108));
    assert!(delay <= Duration::from_secs(132));
}

#[test]
#[serial]
fn test_scanner_cycle_max_duration_uses_env() {
    with_var(ENV_SCANNER_CYCLE_MAX_DURATION_SECS, Some("42"), || {
        assert_eq!(scanner_cycle_max_duration(), Some(Duration::from_secs(42)));
    });
}

#[tokio::test]
async fn test_scanner_cycle_budget_cancels_after_duration() {
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        ScannerCycleBudgetConfig {
            max_duration: Some(Duration::from_millis(1)),
            ..Default::default()
        },
    );

    tokio::time::timeout(Duration::from_secs(5), budget.token().cancelled())
        .await
        .expect("scanner cycle budget should cancel after max duration");

    assert!(budget.budget_elapsed());
    assert!(budget.token().is_cancelled());
}

#[tokio::test]
async fn test_scanner_cycle_budget_drop_cancels_child_without_elapsed() {
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        ScannerCycleBudgetConfig {
            max_duration: Some(Duration::from_secs(60)),
            ..Default::default()
        },
    );
    let token = budget.token();

    drop(budget);

    assert!(token.is_cancelled());
}

#[test]
#[serial]
fn test_scanner_cycle_budget_config_uses_work_budget_env() {
    with_var(ENV_SCANNER_CYCLE_MAX_OBJECTS, Some("100"), || {
        with_var(ENV_SCANNER_CYCLE_MAX_DIRECTORIES, Some("25"), || {
            let config = scanner_cycle_budget_config();
            assert_eq!(config.max_objects, Some(100));
            assert_eq!(config.max_directories, Some(25));
        });
    });
}

#[test]
#[serial]
fn test_scanner_cycle_budget_config_disables_zero_work_budgets() {
    with_var(ENV_SCANNER_CYCLE_MAX_OBJECTS, Some("0"), || {
        with_var(ENV_SCANNER_CYCLE_MAX_DIRECTORIES, Some("0"), || {
            let config = scanner_cycle_budget_config();
            assert_eq!(config.max_objects, None);
            assert_eq!(config.max_directories, None);
        });
    });
}

#[test]
fn test_scan_cycle_partial_reason_maps_budget_reason() {
    assert_eq!(
        scan_cycle_partial_reason(Some(ScannerCycleBudgetReason::Runtime)),
        ScanCyclePartialReason::Runtime
    );
    assert_eq!(
        scan_cycle_partial_reason(Some(ScannerCycleBudgetReason::Objects)),
        ScanCyclePartialReason::Objects
    );
    assert_eq!(
        scan_cycle_partial_reason(Some(ScannerCycleBudgetReason::Directories)),
        ScanCyclePartialReason::Directories
    );
    assert_eq!(scan_cycle_partial_reason(None), ScanCyclePartialReason::Unknown);
}

#[test]
fn test_scan_cycle_partial_source_maps_budget_reason() {
    assert_eq!(scan_cycle_partial_source(Some(ScannerCycleBudgetReason::Runtime)), None);
    assert_eq!(
        scan_cycle_partial_source(Some(ScannerCycleBudgetReason::Objects)),
        Some(ScannerWorkSource::Usage)
    );
    assert_eq!(
        scan_cycle_partial_source(Some(ScannerCycleBudgetReason::Directories)),
        Some(ScannerWorkSource::Usage)
    );
    assert_eq!(scan_cycle_partial_source(None), None);
}

#[tokio::test]
#[serial]
async fn test_mark_scan_cycle_idle_clears_published_cycle_state() {
    let mut cycle_info = CurrentCycle {
        current: 12,
        next: 13,
        cycle_completed: vec![Utc::now()],
        started: Utc::now(),
    };

    global_metrics().set_current_scan_mode(HealScanMode::Deep);
    let mut cycle_metrics_guard = ScannerCycleMetricsGuard::new(cycle_info.clone()).await;

    mark_scan_cycle_idle(&mut cycle_info, &mut cycle_metrics_guard).await;

    let published = global_metrics()
        .get_cycle()
        .await
        .expect("scanner cycle state should remain published");

    assert_eq!(cycle_info.current, 0);
    assert_eq!(cycle_info.next, 13);
    assert_eq!(published.current, 0);
    assert_eq!(published.next, 13);
    assert_eq!(global_metrics().current_scan_mode(), HealScanMode::Unknown);

    global_metrics().set_cycle(None).await;
}

#[tokio::test]
#[serial]
async fn scanner_cycle_metrics_guard_covers_published_first_cycle_lifetime() {
    let cycle_started = Utc::now() - chrono::Duration::seconds(5);
    let mut cycle_info = CurrentCycle {
        current: 0,
        next: 1,
        started: cycle_started,
        ..Default::default()
    };
    let mut guard = ScannerCycleMetricsGuard::new(cycle_info.clone()).await;
    let setup_report = global_metrics().report().await;
    assert!(setup_report.current_cycle_active);
    assert_eq!(setup_report.current_cycle, 0);
    assert_eq!(setup_report.current_started.as_second(), cycle_started.timestamp());
    assert_eq!(
        setup_report.current_started.subsec_nanosecond(),
        i32::try_from(cycle_started.timestamp_subsec_nanos()).expect("chrono nanoseconds fit in i32")
    );

    mark_scan_cycle_idle(&mut cycle_info, &mut guard).await;
    let idle_report = global_metrics().report().await;
    assert!(!idle_report.current_cycle_active);

    global_metrics().set_cycle(None).await;
}

#[tokio::test]
#[serial]
async fn scanner_cycle_metrics_guard_keeps_active_cycle_published_during_finalization() {
    let mut cycle_info = CurrentCycle {
        current: 12,
        next: 13,
        started: Utc::now(),
        ..Default::default()
    };
    let mut guard = ScannerCycleMetricsGuard::new(cycle_info.clone()).await;

    cycle_info.current = 0;
    tokio::task::yield_now().await;
    let finalizing_report = global_metrics().report().await;
    assert!(finalizing_report.current_cycle_active);
    assert_eq!(finalizing_report.current_cycle, 12);

    guard.finish(cycle_info).await;
    let idle_report = global_metrics().report().await;
    assert!(!idle_report.current_cycle_active);
    assert_eq!(idle_report.current_cycle, 0);

    global_metrics().set_cycle(None).await;
}

#[tokio::test]
#[serial]
async fn scanner_cycle_metrics_guard_drop_clears_activity() {
    let guard = ScannerCycleMetricsGuard::new(CurrentCycle {
        current: 12,
        next: 13,
        started: Utc::now(),
        ..Default::default()
    })
    .await;
    assert!(global_metrics().report().await.current_cycle_active);

    drop(guard);

    assert!(!global_metrics().report().await.current_cycle_active);
    global_metrics().set_cycle(None).await;
}

#[tokio::test]
#[serial]
async fn run_data_scanner_cycle_publishes_activity_for_owner_lifetime() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let ctx = CancellationToken::new();
    let mut cycle_info = CurrentCycle::default();
    let mut revision = DataUsageCacheRevision::Missing;
    let leader_epoch = u64::MAX - 1;
    let state_persist_reached = Arc::new(Notify::new());
    let _state_persist_hook = set_scanner_cycle_state_persist_test_hook(leader_epoch, state_persist_reached.clone());
    let state_lock = store
        .new_ns_lock(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("scanner cycle state lock should be created");
    let state_guard = state_lock
        .get_write_lock(Duration::from_secs(1))
        .await
        .expect("scanner cycle state lock should be acquired");
    let mut cycle = Box::pin(run_data_scanner_cycle(&ctx, &store, &mut cycle_info, &mut revision, leader_epoch));
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);

    assert!(cycle.as_mut().poll(&mut context).is_pending());
    let active = global_metrics().report().await;
    assert!(active.current_cycle_active);
    assert_eq!(active.current_cycle, 0);

    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            outcome = &mut cycle => panic!("scanner cycle finished before state persistence was released: {outcome:?}"),
            _ = state_persist_reached.notified() => {}
        }
    })
    .await
    .expect("scanner cycle should reach state persistence");
    let finalizing = global_metrics().report().await;
    assert!(finalizing.current_cycle_active);

    drop(state_guard);
    let outcome = tokio::time::timeout(Duration::from_secs(30), cycle)
        .await
        .expect("scanner cycle should finish");
    assert!(matches!(
        outcome,
        ScannerCycleOutcome::Completed | ScannerCycleOutcome::CompletedWithPendingMaintenance
    ));
    assert!(!global_metrics().report().await.current_cycle_active);

    global_metrics().set_cycle(None).await;
}

#[tokio::test]
#[serial]
async fn coordinator_walks_during_pending_put_without_persisting_or_acknowledging_usage() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let mut pause_backlog = ScannerPauseBacklogController::claim(store.clone(), scanner_pause_backlog_now())
        .await
        .expect("scanner pause backlog should be available");
    let bucket = format!("scanner-coordinator-pending-{}", Uuid::new_v4().simple());
    store
        .make_bucket(&bucket, &crate::storage_api::scan::MakeBucketOptions::default())
        .await
        .expect("fixture bucket should be created");
    let mut reader = PutObjReader::from_vec(b"first".to_vec());
    store.pools[0].disk_set[0]
        .put_object(
            &bucket,
            "object",
            &mut reader,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("fixture object should finish its rename fanout");
    crate::scanner_io::record_dirty_usage_bucket(&bucket);
    let dirty_before = crate::scanner_io::dirty_usage_buckets_for_tests();
    let baseline = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("fixture usage baseline should be readable");
    let pending = ecstore_hold_namespace_commit(store.as_ref());
    assert_eq!(
        scanner_local_publication_defer_reason(store.as_ref()).await,
        Some(ScannerCycleDeferReason::ActivityBaselineUnavailable),
        "an ordinary namespace commit must not be classified as data movement"
    );
    let pause_backlog_attempt = pause_backlog.begin_attempt(scanner_pause_backlog_now()).await;
    assert_eq!(pause_backlog_attempt, ScannerPauseBacklogAttemptDecision::Untracked);
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, ScannerCycleBudgetConfig::default());
    let mut cycle_info = CurrentCycle {
        next: 1,
        ..Default::default()
    };
    let mut revision = DataUsageCacheRevision::Missing;
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        run_data_scanner_cycle_with_budget(
            &ctx,
            &store,
            &mut cycle_info,
            &mut revision,
            1,
            Arc::clone(&budget),
            ScannerCycleScheduling {
                requires_full_scan: true,
                service_cohort: None,
            },
        ),
    )
    .await
    .expect("the coordinator must finish its namespace walk while a PUT is pending");
    assert_eq!(budget.progress().0, 1, "the coordinator must reach actual object traversal");
    assert_eq!(
        outcome,
        ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable)
    );
    finish_scanner_pause_backlog_cycle(&mut pause_backlog, &store, pause_backlog_attempt, outcome).await;
    let pause_backlog_status = scanner_pause_backlog_status(store.clone()).await;
    assert_eq!(pause_backlog_status.phase, ScannerPauseBacklogPhase::Idle);
    assert!(!pause_backlog_status.pending_full_scan);
    assert_eq!(pause_backlog_status.catch_up_attempts, 0);
    assert_eq!(cycle_info.next, 1, "a rejected publication must not advance the cycle");
    assert_eq!(revision, DataUsageCacheRevision::Missing);
    assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);
    assert_eq!(
        read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("the prior authoritative usage must remain readable"),
        baseline,
        "the pending candidate must not replace the authoritative baseline"
    );

    let committed_body = b"committed-after-walk";
    let mut reader = PutObjReader::from_vec(committed_body.to_vec());
    store.pools[0].disk_set[0]
        .put_object(
            &bucket,
            "object",
            &mut reader,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("the pending tail must change the physical object before it drains");
    assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty_before);
    drop(pending);
    let retry_budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, ScannerCycleBudgetConfig::default());
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        run_data_scanner_cycle_with_budget(
            &ctx,
            &store,
            &mut cycle_info,
            &mut revision,
            1,
            Arc::clone(&retry_budget),
            ScannerCycleScheduling {
                requires_full_scan: true,
                service_cohort: None,
            },
        ),
    )
    .await
    .expect("the same cycle must converge after the pending PUT drains");
    assert_eq!(
        retry_budget.progress().0,
        1,
        "the same-cycle retry must not reuse the pre-tail bucket cache"
    );
    assert!(matches!(
        outcome,
        ScannerCycleOutcome::Completed | ScannerCycleOutcome::CompletedWithPendingMaintenance
    ));
    assert_eq!(cycle_info.next, 2);
    assert!(!crate::scanner_io::dirty_usage_buckets_for_tests().contains_key(&bucket));
    let usage = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("the converged usage should be persisted");
    let usage: DataUsageInfo = serde_json::from_slice(&usage).expect("the persisted usage should decode");
    assert_eq!(usage.usage_snapshot_converged, Some(true));
    assert_eq!(usage.scanner_cycle, Some(1));
    assert_eq!(usage.objects_total_count, 1);
    assert_eq!(
        usage.objects_total_size,
        u64::try_from(committed_body.len()).expect("fixture body length")
    );
    let bucket_usage = usage
        .buckets_usage
        .get(&bucket)
        .expect("the scanned bucket should be published");
    assert_eq!(bucket_usage.objects_count, 1);
    assert_eq!(bucket_usage.size, u64::try_from(committed_body.len()).expect("fixture body length"));
    global_metrics().set_cycle(None).await;
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn test_finalize_partial_scan_cycle_advances_and_persists_counter() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle_info = CurrentCycle {
        current: 12,
        next: 12,
        cycle_completed: vec![],
        started: Utc::now(),
    };
    let mut cycle_metrics_guard = ScannerCycleMetricsGuard::new(cycle_info.clone()).await;

    assert!(finalize_partial_scan_cycle(&ctx, store.clone(), &mut cycle_info, &mut revision, 1, &mut cycle_metrics_guard,).await);

    assert_eq!(cycle_info.next, 13);
    assert_eq!(cycle_info.current, 0);
    assert!(cycle_info.cycle_completed.is_empty());
    assert!(matches!(revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-1"));

    let buf = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("cycle state should be persisted after a partial cycle");
    assert_eq!(
        u64::from_le_bytes(buf[0..8].try_into().expect("persisted state should start with the counter")),
        13
    );
    let (decoded, epoch) = decode_scanner_cycle_state(&buf).expect("persisted cycle info should decode");
    assert_eq!(decoded.next, 13);
    assert_eq!(decoded.current, 0);
    assert_eq!(epoch, 1);

    global_metrics().set_cycle(None).await;
}

#[tokio::test]
#[serial]
async fn scanner_cycle_recovers_to_newer_durable_cache_floor() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle_info = CurrentCycle {
        current: 12,
        next: 12,
        cycle_completed: vec![],
        started: Utc::now(),
    };
    let mut cycle_metrics_guard = ScannerCycleMetricsGuard::new(cycle_info.clone()).await;

    assert!(
        persist_required_scanner_cycle_floor(
            &ctx,
            store.clone(),
            &mut cycle_info,
            &mut revision,
            7,
            19,
            &mut cycle_metrics_guard,
        )
        .await
    );
    assert_eq!(cycle_info.current, 0);
    assert_eq!(cycle_info.next, 19);

    let buf = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("recovered cycle floor should be persisted");
    let (decoded, epoch) = decode_scanner_cycle_state(&buf).expect("recovered cycle state should decode");
    assert_eq!(decoded.current, 0);
    assert_eq!(decoded.next, 19);
    assert_eq!(epoch, 7);

    global_metrics().set_cycle(None).await;
}

#[tokio::test]
#[serial]
async fn scanner_cycle_rejects_invalid_cache_floor() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle_info = CurrentCycle {
        current: 12,
        next: 12,
        cycle_completed: vec![],
        started: Utc::now(),
    };
    let mut cycle_metrics_guard = ScannerCycleMetricsGuard::new(cycle_info.clone()).await;

    assert!(
        !persist_required_scanner_cycle_floor(
            &ctx,
            store.clone(),
            &mut cycle_info,
            &mut revision,
            7,
            12,
            &mut cycle_metrics_guard,
        )
        .await
    );
    assert_eq!(cycle_info.next, 12);
    assert_eq!(revision, DataUsageCacheRevision::Missing);
    let mut max_cycle_info = CurrentCycle {
        current: 12,
        next: 12,
        ..Default::default()
    };
    let mut max_cycle_metrics_guard = ScannerCycleMetricsGuard::new(max_cycle_info.clone()).await;
    assert!(
        !persist_required_scanner_cycle_floor(
            &ctx,
            store.clone(),
            &mut max_cycle_info,
            &mut revision,
            7,
            u64::MAX,
            &mut max_cycle_metrics_guard,
        )
        .await
    );
    assert!(read_config(store, &DATA_USAGE_BLOOM_NAME_PATH).await.is_err());

    global_metrics().set_cycle(None).await;
}

#[test]
fn scanner_cycle_state_decodes_legacy_and_fenced_formats() {
    let cycle = CurrentCycle {
        current: 12,
        next: 13,
        cycle_completed: vec![],
        started: Utc::now(),
    };
    let mut legacy = cycle.next.to_le_bytes().to_vec();
    legacy.extend(cycle.marshal().expect("legacy cycle state should encode"));

    let (legacy_cycle, legacy_epoch) = decode_scanner_cycle_state(&legacy).expect("legacy cycle state should remain readable");
    assert_eq!(legacy_cycle.next, 13);
    assert_eq!(legacy_epoch, 0);

    let fenced = encode_scanner_cycle_state(&cycle, 7).expect("fenced cycle state should encode");
    let (fenced_cycle, fenced_epoch) = decode_scanner_cycle_state(&fenced).expect("fenced cycle state should decode");
    assert_eq!(fenced_cycle.next, 13);
    assert_eq!(fenced_epoch, 7);

    let mut trailing = fenced;
    trailing.push(0);
    assert!(decode_scanner_cycle_state(&trailing).is_err());
}

#[test]
fn scanner_startup_fails_closed_on_nonempty_corrupt_cycle_state() {
    assert_eq!(
        decode_scanner_cycle_state_for_startup(&[])
            .expect("missing cycle state should use defaults")
            .1,
        0
    );
    assert!(decode_scanner_cycle_state_for_startup(&[1]).is_err());

    let mut corrupt_fenced = 13_u64.to_le_bytes().to_vec();
    corrupt_fenced.extend_from_slice(SCANNER_CYCLE_STATE_MAGIC);
    corrupt_fenced.extend_from_slice(&7_u64.to_le_bytes());
    corrupt_fenced.extend_from_slice(b"not-msgpack");
    assert!(decode_scanner_cycle_state_for_startup(&corrupt_fenced).is_err());
    assert!(decode_scanner_cycle_state_for_startup(&u64::MAX.to_le_bytes()).is_err());

    let exhausted = CurrentCycle {
        next: u64::MAX,
        ..Default::default()
    };
    assert!(encode_scanner_cycle_state(&exhausted, 7).is_err());
}

#[tokio::test]
#[serial]
async fn corrupt_cycle_state_is_quarantined_once() {
    let store = Arc::new(MemoryConfigStore::default());
    let state_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    store.objects.lock().await.insert(state_key.clone(), vec![1]);
    store.revisions.lock().await.insert(state_key.clone(), 7);

    assert!(matches!(
        load_scanner_cycle_state_for_startup(store.clone()).await,
        ScannerCycleStateStartup::Blocked
    ));
    let marker_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str());
    let marker_data = store
        .objects
        .lock()
        .await
        .get(&marker_key)
        .cloned()
        .expect("corrupt state must leave a durable recovery marker");
    let marker: ScannerCycleRecoveryMarker = serde_json::from_slice(&marker_data).expect("marker should be valid JSON");
    assert_eq!(marker.primary_revision, "memory-7");
    assert_eq!(marker.path, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    assert_eq!(marker.quarantine_path, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str());
    assert_eq!(marker.classification, "corrupt");

    // A second startup sees the matching marker before consuming the poison body.
    assert!(matches!(
        load_scanner_cycle_state_for_startup(store.clone()).await,
        ScannerCycleStateStartup::Blocked
    ));

    // Replacing the primary object advances its revision; the stale marker must
    // not quarantine the newer, valid state.
    let cycle = CurrentCycle {
        next: 9,
        ..Default::default()
    };
    let encoded = encode_scanner_cycle_state(&cycle, 3).expect("valid state should encode");
    store.objects.lock().await.insert(state_key.clone(), encoded);
    store.revisions.lock().await.insert(state_key, 8);
    assert!(matches!(
        load_scanner_cycle_state_for_startup(store).await,
        ScannerCycleStateStartup::Ready {
            cycle: CurrentCycle { next: 9, .. },
            leader_epoch: 3,
            ..
        }
    ));
}

#[tokio::test]
#[serial]
async fn empty_cycle_state_object_is_quarantined_as_corrupt() {
    let store = Arc::new(MemoryConfigStore::default());
    let state_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    store.objects.lock().await.insert(state_key.clone(), Vec::new());
    store.revisions.lock().await.insert(state_key, 6);

    assert!(matches!(
        load_scanner_cycle_state_for_startup(store).await,
        ScannerCycleStateStartup::Blocked
    ));
    assert_eq!(scanner_cycle_recovery_status().classification.as_deref(), Some("corrupt"));
    assert!(
        scanner_cycle_recovery_status()
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("empty"))
    );
}

#[tokio::test]
#[serial]
async fn future_cycle_state_schema_is_recovery_required() {
    let store = Arc::new(MemoryConfigStore::default());
    let state_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    let mut future = 17_u64.to_le_bytes().to_vec();
    future.extend_from_slice(b"RSCYC999");
    future.extend_from_slice(&4_u64.to_le_bytes());
    future.extend_from_slice(&[0x90]);
    store.objects.lock().await.insert(state_key.clone(), future);
    store.revisions.lock().await.insert(state_key, 13);

    assert!(matches!(
        load_scanner_cycle_state_for_startup(store).await,
        ScannerCycleStateStartup::Blocked
    ));
    assert_eq!(scanner_cycle_recovery_status().classification.as_deref(), Some("future_schema"));
}

#[tokio::test]
#[serial]
async fn concurrent_leaders_cannot_quarantine_newer_cycle_state() {
    let store = Arc::new(MemoryConfigStore::default());
    let state_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    store.objects.lock().await.insert(state_key.clone(), vec![1]);
    store.revisions.lock().await.insert(state_key, 4);

    let (first, second) = tokio::join!(
        load_scanner_cycle_state_for_startup(store.clone()),
        load_scanner_cycle_state_for_startup(store.clone()),
    );
    assert!(matches!(first, ScannerCycleStateStartup::Blocked));
    assert!(matches!(second, ScannerCycleStateStartup::Blocked));

    let marker_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str());
    let marker_data = store
        .objects
        .lock()
        .await
        .get(&marker_key)
        .cloned()
        .expect("one contender must publish the recovery marker");
    let marker: ScannerCycleRecoveryMarker = serde_json::from_slice(&marker_data).expect("marker should decode");
    assert_eq!(marker.primary_revision, "memory-4");
}

#[tokio::test]
#[serial]
async fn cleanup_pending_marker_blocks_a_rewritten_primary_after_restart() {
    let store = Arc::new(MemoryConfigStore::default());
    let state_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    let marker_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str());
    let encoded = encode_scanner_cycle_state(
        &CurrentCycle {
            next: 12,
            ..Default::default()
        },
        8,
    )
    .expect("valid state should encode");
    store.objects.lock().await.insert(state_key.clone(), encoded);
    store.revisions.lock().await.insert(state_key, 22);
    let marker = ScannerCycleRecoveryMarker {
        schema_version: 1,
        primary_revision: "memory-21".to_string(),
        generation: 11,
        leader_epoch: 7,
        classification: "corrupt".to_string(),
        first_detected_at_unix_secs: 1,
        last_attempt_at_unix_secs: 2,
        retry_count: 1,
        reason: "reset in progress".to_string(),
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: DATA_USAGE_BLOOM_RECOVERY_PATH.clone(),
        state: "cleanup-pending".to_string(),
    };
    store
        .objects
        .lock()
        .await
        .insert(marker_key.clone(), serde_json::to_vec(&marker).expect("marker should encode"));
    store.revisions.lock().await.insert(marker_key, 3);

    assert!(matches!(
        load_scanner_cycle_state_for_startup(store).await,
        ScannerCycleStateStartup::Blocked
    ));
    assert_eq!(scanner_cycle_recovery_status().state, "cleanup-pending");
}

#[test]
fn full_rescan_reset_accepts_unknown_marker_fields_without_trusting_cursor() {
    let marker = br#"{
        "schema_version": 99,
        "primary_revision": "memory-7",
        "generation": 9000,
        "leader_epoch": 9000,
        "classification": "new-future-classification",
        "first_detected_at_unix_secs": 1,
        "last_attempt_at_unix_secs": 2,
        "retry_count": 9,
        "reason": "future marker",
        "path": "buckets/.bloomcycle.bin",
        "quarantine_path": "buckets/.bloomcycle.bin.recovery-required.json",
        "future_field": {"cursor": "untrusted"}
    }"#;
    let decoded =
        super::cycle_state::decode_recovery_marker_for_reset(marker, &DataUsageCacheRevision::Etag("memory-3".to_string()))
            .expect("full-rescan compatibility decoder should accept additive fields");
    assert_eq!(decoded.primary_revision, "memory-7");
    assert_eq!(decoded.classification, "future_schema");
    assert_eq!(decoded.generation, 0);
    assert_eq!(decoded.leader_epoch, 0);
    assert_eq!(decoded.state, "blocked");

    let malformed =
        super::cycle_state::decode_recovery_marker_for_reset(b"{not-json", &DataUsageCacheRevision::Etag("memory-4".to_string()))
            .expect("a full-rescan reset must recover even when the marker is malformed");
    assert!(malformed.primary_revision.is_empty());
    assert_eq!(malformed.classification, "future_schema");
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_rebuilds_after_malformed_marker_without_trusting_cursor() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), vec![0xff, 0x00, 0x01])
        .await
        .expect("corrupt cycle state should be persisted");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), br#"{not-json"#.to_vec())
        .await
        .expect("malformed marker should be persisted");

    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("full-rescan reset should recover malformed marker");

    let state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("rebuilt cycle state should remain durable");
    let (cycle, leader_epoch) = decode_scanner_cycle_state(&state).expect("rebuilt cycle state should decode");
    assert_eq!(cycle.next, 0, "reset must use the verified usage floor, not marker cursor");
    assert_eq!(leader_epoch, 1);
    assert!(matches!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_ignores_epoch_from_malformed_future_primary() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let mut future_primary = vec![0; 24];
    future_primary[8..16].copy_from_slice(b"RSCY9999");
    future_primary[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
    save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), future_primary)
        .await
        .expect("future cycle state should be persisted");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), br#"{not-json"#.to_vec())
        .await
        .expect("malformed marker should be persisted");

    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("full-rescan reset should recover malformed future state");

    let state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("rebuilt cycle state should remain durable");
    let (_, leader_epoch) = decode_scanner_cycle_state(&state).expect("rebuilt cycle state should decode");
    assert_eq!(leader_epoch, 1, "invalid persisted bytes must not raise the recovery epoch");
    assert!(matches!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
async fn ecstore_exact_recovery_marker_delete_honors_etag() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), b"marker-v1".to_vec())
        .await
        .expect("initial recovery marker should be persisted");
    let (_, stale_revision) = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
        .await
        .expect("initial marker revision should load");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), b"marker-v2".to_vec())
        .await
        .expect("replacement recovery marker should be persisted");

    let delete_result = store
        .delete_config_object(
            RUSTFS_META_BUCKET,
            DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
            ObjectOptions {
                http_preconditions: Some(stale_revision.preconditions()),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(delete_result, Err(EcstoreError::PreconditionFailed)));
    assert_eq!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
            .await
            .expect("replacement marker should remain durable"),
        b"marker-v2"
    );
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_rejects_corrupt_primary_under_stale_blocked_marker() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let corrupt_primary = vec![0xff, 0x00, 0x01];
    save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), corrupt_primary.clone())
        .await
        .expect("corrupt cycle state should be persisted");
    let (_, primary_revision) = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("primary revision should load");
    let marker = ScannerCycleRecoveryMarker {
        schema_version: 1,
        primary_revision: "memory-stale".to_string(),
        generation: 1,
        leader_epoch: 1,
        classification: "corrupt".to_string(),
        first_detected_at_unix_secs: 1,
        last_attempt_at_unix_secs: 2,
        retry_count: 1,
        reason: "blocked primary changed".to_string(),
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: DATA_USAGE_BLOOM_RECOVERY_PATH.clone(),
        state: "blocked".to_string(),
    };
    let marker_data = serde_json::to_vec(&marker).expect("blocked marker should encode");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), marker_data.clone())
        .await
        .expect("blocked marker should be persisted");

    assert!(
        reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
            .await
            .is_err(),
        "a strict marker must fail closed when its primary revision changed"
    );
    assert_eq!(
        read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
            .await
            .expect("primary should remain readable"),
        corrupt_primary
    );
    assert_eq!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
            .await
            .expect("blocked marker should remain durable"),
        marker_data
    );
    assert!(!matches!(primary_revision, DataUsageCacheRevision::Missing));
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_preserves_valid_primary_when_marker_is_malformed() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let primary = CurrentCycle {
        next: 42,
        ..Default::default()
    };
    let old_primary_data = encode_scanner_cycle_state(&primary, 7).expect("valid cycle state should encode");
    save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), old_primary_data.clone())
        .await
        .expect("valid cycle state should be persisted");
    let (_, old_primary_revision) = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("primary state revision should load");
    let old_usage = DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(41),
        ..complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0)
    };
    let old_usage_data = serde_json::to_vec(&old_usage).expect("usage snapshot should encode");
    save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), old_usage_data.clone())
        .await
        .expect("usage snapshot should be persisted");
    let (_, old_usage_revision) = read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("usage snapshot revision should load");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), b"{not-json".to_vec())
        .await
        .expect("malformed marker should be persisted");

    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("reset should clear a stale malformed marker");

    let state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("valid primary should remain durable");
    let (cycle, leader_epoch) = decode_scanner_cycle_state(&state).expect("primary cycle state should decode");
    assert_eq!(cycle.next, 42, "reset must not regress an independently fenced primary");
    assert_eq!(leader_epoch, 8, "reset must advance the preserved primary epoch");
    let stale_primary_save = save_config_with_preconditions(
        store.clone(),
        DATA_USAGE_BLOOM_NAME_PATH.as_str(),
        old_primary_data,
        old_primary_revision.preconditions(),
    )
    .await;
    assert!(matches!(stale_primary_save, Err(EcstoreError::PreconditionFailed)));
    let usage = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("usage epoch fence should remain durable");
    assert_eq!(
        serde_json::from_slice::<DataUsageInfo>(&usage)
            .expect("fenced usage should decode")
            .scanner_epoch,
        Some(8)
    );
    let stale_save = save_config_with_preconditions(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        old_usage_data,
        old_usage_revision.preconditions(),
    )
    .await;
    assert!(matches!(stale_save, Err(EcstoreError::PreconditionFailed)));
    assert!(matches!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_resumes_cleanup_pending_preserved_primary() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let completed_at = Utc::now();
    let primary = CurrentCycle {
        current: 3,
        next: 42,
        cycle_completed: vec![completed_at],
        started: completed_at,
    };
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_NAME_PATH.as_str(),
        encode_scanner_cycle_state(&primary, 7).expect("valid cycle state should encode"),
    )
    .await
    .expect("valid cycle state should be persisted");
    let usage = DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(41),
        ..complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0)
    };
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&usage).expect("usage snapshot should encode"),
    )
    .await
    .expect("usage snapshot should be persisted");
    let marker = ScannerCycleRecoveryMarker {
        schema_version: 1,
        primary_revision: "memory-old".to_string(),
        generation: 41,
        leader_epoch: 7,
        classification: "corrupt".to_string(),
        first_detected_at_unix_secs: 1,
        last_attempt_at_unix_secs: 2,
        retry_count: 1,
        reason: "reset in progress".to_string(),
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: DATA_USAGE_BLOOM_RECOVERY_PATH.clone(),
        state: "cleanup-pending".to_string(),
    };
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
        serde_json::to_vec(&marker).expect("marker should encode"),
    )
    .await
    .expect("cleanup marker should be persisted");

    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("reset should resume a cleanup-pending preserved primary");

    let state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("preserved cycle state should remain durable");
    let (cycle, leader_epoch) = decode_scanner_cycle_state(&state).expect("cycle state should decode");
    assert_eq!(cycle.current, 3, "cleanup retry must preserve the in-progress cursor");
    assert_eq!(cycle.next, 42);
    assert_eq!(cycle.cycle_completed, vec![completed_at]);
    assert_eq!(cycle.started, completed_at);
    assert_eq!(leader_epoch, 8);
    let usage = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("usage epoch fence should remain durable");
    assert_eq!(
        serde_json::from_slice::<DataUsageInfo>(&usage)
            .expect("usage should decode")
            .scanner_epoch,
        Some(8)
    );
    assert!(matches!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_rebuilds_oversized_regular_primary_with_malformed_marker() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), vec![0; 1024 * 1024 + 1])
        .await
        .expect("oversized cycle state should be persisted");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), b"{not-json".to_vec())
        .await
        .expect("malformed marker should be persisted");

    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("explicit full-rescan reset should replace an oversized regular primary");

    let state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("rebuilt cycle state should remain durable");
    let (cycle, leader_epoch) = decode_scanner_cycle_state(&state).expect("rebuilt cycle state should decode");
    assert_eq!(cycle.next, 0);
    assert_eq!(leader_epoch, 1);
    assert!(matches!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_rebuilds_oversized_primary_after_cleanup_marker() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), vec![0; 1024 * 1024 + 1])
        .await
        .expect("oversized cycle state should be persisted");
    let (_, primary_revision) = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("primary revision should load");
    let marker = ScannerCycleRecoveryMarker {
        schema_version: 1,
        primary_revision: match primary_revision {
            DataUsageCacheRevision::Etag(etag) => etag,
            DataUsageCacheRevision::Missing => panic!("primary revision should be present"),
        },
        generation: 1,
        leader_epoch: 1,
        classification: "corrupt".to_string(),
        first_detected_at_unix_secs: 1,
        last_attempt_at_unix_secs: 2,
        retry_count: 1,
        reason: "reset in progress".to_string(),
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: DATA_USAGE_BLOOM_RECOVERY_PATH.clone(),
        state: "cleanup-pending".to_string(),
    };
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
        serde_json::to_vec(&marker).expect("cleanup marker should encode"),
    )
    .await
    .expect("cleanup marker should be persisted");

    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("cleanup retry should rebuild an oversized primary");

    let state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("rebuilt cycle state should remain durable");
    let (cycle, leader_epoch) = decode_scanner_cycle_state(&state).expect("rebuilt cycle state should decode");
    assert_eq!(cycle.next, 0);
    assert_eq!(leader_epoch, 1);
    assert!(matches!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_rebuilds_with_oversized_marker() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), vec![0xff, 0x00, 0x01])
        .await
        .expect("corrupt cycle state should be persisted");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), vec![b'x'; 64 * 1024 + 1])
        .await
        .expect("oversized recovery marker should be persisted");

    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("full-rescan reset should recover an oversized marker");

    let state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("rebuilt cycle state should remain durable");
    let (_, leader_epoch) = decode_scanner_cycle_state(&state).expect("rebuilt cycle state should decode");
    assert_eq!(leader_epoch, 1);
    assert!(matches!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_rebuilds_with_empty_marker() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), vec![0xff, 0x00, 0x01])
        .await
        .expect("corrupt cycle state should be persisted");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), Vec::new())
        .await
        .expect("empty recovery marker should be persisted");

    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("full-rescan reset should recover an empty marker");

    let state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("rebuilt cycle state should remain durable");
    let (_, leader_epoch) = decode_scanner_cycle_state(&state).expect("rebuilt cycle state should decode");
    assert_eq!(leader_epoch, 1);
    assert!(matches!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_keeps_cleanup_marker_when_preserved_epoch_is_exhausted() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let primary = CurrentCycle {
        next: 42,
        ..Default::default()
    };
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_NAME_PATH.as_str(),
        encode_scanner_cycle_state(&primary, u64::MAX).expect("valid cycle state should encode"),
    )
    .await
    .expect("valid cycle state should be persisted");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), b"{not-json".to_vec())
        .await
        .expect("malformed marker should be persisted");

    assert!(
        reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
            .await
            .is_err()
    );

    let marker = read_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
        .await
        .expect("cleanup marker should remain durable");
    assert_eq!(
        serde_json::from_slice::<ScannerCycleRecoveryMarker>(&marker)
            .expect("cleanup marker should decode")
            .state,
        "cleanup-pending"
    );
    assert!(matches!(
        load_scanner_cycle_state_for_startup(store).await,
        ScannerCycleStateStartup::Blocked
    ));
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_rejects_preserved_epoch_that_would_be_terminal() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let primary = CurrentCycle {
        next: 42,
        ..Default::default()
    };
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_NAME_PATH.as_str(),
        encode_scanner_cycle_state(&primary, u64::MAX - 1).expect("valid cycle state should encode"),
    )
    .await
    .expect("valid cycle state should be persisted");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), b"{not-json".to_vec())
        .await
        .expect("malformed marker should be persisted");

    assert!(
        reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
            .await
            .is_err(),
        "reset must not persist the terminal leader epoch"
    );

    let marker = read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
        .await
        .expect("cleanup marker should remain durable");
    assert_eq!(
        serde_json::from_slice::<ScannerCycleRecoveryMarker>(&marker)
            .expect("cleanup marker should decode")
            .state,
        "cleanup-pending"
    );
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_rejects_usage_floor_that_would_be_terminal() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), vec![0xff, 0x00, 0x01])
        .await
        .expect("corrupt cycle state should be persisted");
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&DataUsageInfo {
            scanner_epoch: Some(u64::MAX - 1),
            ..complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0)
        })
        .expect("usage floor should encode"),
    )
    .await
    .expect("usage floor should be persisted");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), b"{not-json".to_vec())
        .await
        .expect("malformed marker should be persisted");

    assert!(
        reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
            .await
            .is_err(),
        "reset must not persist the terminal leader epoch"
    );
    assert_eq!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
            .await
            .expect("recovery marker should remain durable"),
        b"{not-json"
    );
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_rebuilds_empty_primary_with_malformed_marker() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), Vec::new())
        .await
        .expect("empty cycle state should be persisted");
    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), b"{not-json".to_vec())
        .await
        .expect("malformed marker should be persisted");

    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("explicit full-rescan reset should replace an empty primary");

    let state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("rebuilt cycle state should remain durable");
    let (cycle, leader_epoch) = decode_scanner_cycle_state(&state).expect("rebuilt cycle state should decode");
    assert_eq!(cycle.next, 0);
    assert_eq!(leader_epoch, 1);
    assert!(matches!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_rebuilds_when_primary_cycle_state_is_missing() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let marker = ScannerCycleRecoveryMarker {
        schema_version: 1,
        primary_revision: "memory-missing".to_string(),
        generation: u64::MAX,
        leader_epoch: u64::MAX,
        classification: "corrupt".to_string(),
        first_detected_at_unix_secs: 1,
        last_attempt_at_unix_secs: 2,
        retry_count: 0,
        reason: "missing primary".to_string(),
        path: DATA_USAGE_BLOOM_NAME_PATH.clone(),
        quarantine_path: DATA_USAGE_BLOOM_RECOVERY_PATH.clone(),
        state: "blocked".to_string(),
    };
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
        serde_json::to_vec(&marker).expect("marker should encode"),
    )
    .await
    .expect("marker should be persisted");

    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("full-rescan reset should recreate missing primary");
    let state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("missing primary should be rebuilt");
    let (cycle, leader_epoch) = decode_scanner_cycle_state(&state).expect("rebuilt cycle state should decode");
    assert_eq!(cycle.next, 0);
    assert_eq!(leader_epoch, 1);
    assert!(matches!(
        read_config(store, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
#[serial]
async fn corrupt_cycle_state_rename_or_marker_failure_stays_recovery_required() {
    let store = Arc::new(MemoryConfigStore::default());
    let state_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    let marker_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str());
    store.objects.lock().await.insert(state_key.clone(), vec![1]);
    store.revisions.lock().await.insert(state_key, 9);
    store.fail_put_number.lock().await.insert(marker_key, 1);

    assert!(matches!(
        load_scanner_cycle_state_for_startup(store.clone()).await,
        ScannerCycleStateStartup::Transient(_)
    ));
    let status = scanner_cycle_recovery_status();
    assert_eq!(status.state, "recovery-required");
    assert!(status.retryable);
    assert!(
        store
            .objects
            .lock()
            .await
            .contains_key(&memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str()))
    );
}

#[tokio::test]
#[serial]
async fn oversized_or_symlinked_cycle_state_is_rejected() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    store.objects.lock().await.insert(key.clone(), vec![0; 1024 * 1024 + 1]);
    store.revisions.lock().await.insert(key.clone(), 11);

    assert!(matches!(
        load_scanner_cycle_state_for_startup(store.clone()).await,
        ScannerCycleStateStartup::Blocked
    ));
    assert_eq!(scanner_cycle_recovery_status().classification.as_deref(), Some("corrupt"));
    assert!(
        scanner_cycle_recovery_status()
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("oversized"))
    );

    let marker_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_RECOVERY_PATH.as_str());
    store.objects.lock().await.remove(&marker_key);
    store.objects.lock().await.insert(key.clone(), vec![1]);
    store.revisions.lock().await.insert(key.clone(), 12);
    store.non_regular_objects.lock().await.insert(key);
    // The object contract exposes a non-regular object as `is_dir`; local
    // backends reject symlink/reparse entries before they become an object.
    assert!(matches!(
        load_scanner_cycle_state_for_startup(store).await,
        ScannerCycleStateStartup::Blocked
    ));
}

#[tokio::test]
async fn scanner_startup_uses_primary_and_backup_usage_floor() {
    let store = Arc::new(MemoryConfigStore::default());
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    for (path, epoch, cycle) in [(DATA_USAGE_OBJ_NAME_PATH.as_str(), 8, 100), (backup_path.as_str(), 11, 103)] {
        let mut usage = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
        usage.scanner_epoch = Some(epoch);
        usage.scanner_cycle = Some(cycle);
        store.objects.lock().await.insert(
            memory_config_key(RUSTFS_META_BUCKET, path),
            serde_json::to_vec(&usage).expect("usage snapshot should encode"),
        );
    }

    let floor = persisted_usage_floor(store).await.expect("usage floor should load");
    assert_eq!(
        floor,
        PersistedUsageFloor {
            next_cycle: 104,
            leader_epoch: 11,
        }
    );

    let mut cycle = CurrentCycle::default();
    let mut epoch = 0;
    apply_persisted_usage_floor(&mut cycle, &mut epoch, floor);
    assert_eq!(cycle.next, 104);
    assert_eq!(epoch, 11);
}

#[tokio::test]
async fn scanner_usage_floor_keeps_valid_primary_when_backup_has_no_identity() {
    let store = Arc::new(MemoryConfigStore::default());
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let mut primary = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    primary.scanner_epoch = Some(8);
    primary.scanner_cycle = Some(100);
    let backup = DataUsageInfo {
        scanner_epoch: Some(9),
        scanner_cycle: Some(101),
        usage_snapshot_complete: false,
        ..Default::default()
    };

    for (path, usage) in [(DATA_USAGE_OBJ_NAME_PATH.as_str(), primary), (backup_path.as_str(), backup)] {
        store.objects.lock().await.insert(
            memory_config_key(RUSTFS_META_BUCKET, path),
            serde_json::to_vec(&usage).expect("usage snapshot should encode"),
        );
    }

    assert_eq!(
        persisted_usage_floor(store)
            .await
            .expect("valid primary should remain authoritative"),
        PersistedUsageFloor {
            next_cycle: 101,
            leader_epoch: 8,
        }
    );
}

fn rc3_legacy_empty_usage_fence(epoch: Option<u64>) -> Vec<u8> {
    // Pinned field set emitted by rc.3 after DeleteBucket synthesized a
    // default v2 usage primary. Leadership added scanner_epoch separately.
    const RC3_EMPTY_USAGE_FENCE: &str = r#"{
        "total_capacity":0,
        "total_used_capacity":0,
        "total_free_capacity":0,
        "last_update":{"secs_since_epoch":1,"nanos_since_epoch":0},
        "objects_total_count":0,
        "versions_total_count":0,
        "delete_markers_total_count":0,
        "objects_total_size":0,
        "replication_info":{},
        "buckets_count":0,
        "buckets_usage":{},
        "usage_snapshot_complete":false,
        "bucket_sizes":{},
        "disk_usage_status":[]
    }"#;
    let mut value =
        serde_json::from_str::<serde_json::Value>(RC3_EMPTY_USAGE_FENCE).expect("pinned rc.3 empty usage fence should decode");
    let fields = value
        .as_object_mut()
        .expect("legacy empty usage fence should be a JSON object");
    if let Some(epoch) = epoch {
        fields.insert("scanner_epoch".to_string(), serde_json::Value::from(epoch));
    }
    serde_json::to_vec(&value).expect("rc.3 legacy empty usage fence fixture should encode")
}

fn rc3_legacy_non_empty_usage_fence(epoch: Option<u64>) -> Vec<u8> {
    // Pinned rc.3 field set. Leadership preserved this data and added only
    // scanner_epoch when the producing scanner cycle had not completed.
    const RC3_NON_EMPTY_USAGE_FENCE: &str = r#"{
        "total_capacity":2000000000,
        "total_used_capacity":1000000000,
        "total_free_capacity":1000000000,
        "last_update":{"secs_since_epoch":1,"nanos_since_epoch":0},
        "objects_total_count":156382067,
        "versions_total_count":156382070,
        "delete_markers_total_count":3,
        "objects_total_size":987654321,
        "replication_info":{},
        "buckets_count":1,
        "buckets_usage":{
            "photos":{
                "size":987654321,
                "replication_pending_size_v1":0,
                "replication_failed_size_v1":0,
                "replicated_size_v1":0,
                "replication_pending_count_v1":0,
                "replication_failed_count_v1":0,
                "objects_count":156382067,
                "object_size_histogram":{},
                "object_versions_histogram":{},
                "versions_count":156382070,
                "delete_markers_count":3,
                "replica_size":0,
                "replica_count":0,
                "replication_info":{}
            }
        },
        "usage_snapshot_complete":false,
        "bucket_sizes":{"photos":987654321},
        "disk_usage_status":[]
    }"#;
    let mut value = serde_json::from_str::<serde_json::Value>(RC3_NON_EMPTY_USAGE_FENCE)
        .expect("pinned rc.3 non-empty usage fence should decode");
    if let Some(epoch) = epoch {
        value
            .as_object_mut()
            .expect("legacy non-empty usage fence should be a JSON object")
            .insert("scanner_epoch".to_string(), serde_json::Value::from(epoch));
    }
    serde_json::to_vec(&value).expect("rc.3 legacy non-empty usage fence fixture should encode")
}

#[tokio::test]
async fn scanner_usage_floor_recovers_rc3_empty_fences_and_preserves_cycle_number() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let backup_key = memory_config_key(RUSTFS_META_BUCKET, &backup_path);
    store
        .objects
        .lock()
        .await
        .insert(primary_key.clone(), rc3_legacy_empty_usage_fence(Some(7)));
    store
        .objects
        .lock()
        .await
        .insert(backup_key, rc3_legacy_empty_usage_fence(None));
    store.revisions.lock().await.insert(primary_key, 1);

    let (floor, startup) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("rc.3 empty usage fences should enter recovery");
    assert_eq!(
        floor,
        PersistedUsageFloor {
            next_cycle: 0,
            leader_epoch: 7,
        }
    );
    assert_eq!(startup, PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence);

    let primary = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("recovered usage bootstrap should be persisted");
    let pending = serde_json::from_slice::<DataUsageInfo>(&primary).expect("recovered usage bootstrap should decode");
    assert!(data_usage_info_is_bootstrap_pending(&pending));
    assert_eq!(pending.scanner_epoch, Some(7));
    assert!(read_config(store.clone(), DATA_USAGE_RECOVERY_PATH.as_str()).await.is_ok());

    let (restart_floor, restart_state) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("recovery marker should survive a restart before leadership claim");
    assert_eq!(restart_floor.leader_epoch, 7);
    assert_eq!(restart_state, PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence);
    let mut cycle = CurrentCycle {
        current: 17_117,
        next: 17_118,
        cycle_completed: vec![Utc::now()],
        started: Utc::now(),
    };
    assert_eq!(
        prepare_cycle_for_usage_floor_bootstrap(&mut cycle, restart_floor, restart_state),
        (true, ScannerCycleResetPolicy::ResetCoveragePreservingNext)
    );
    assert_eq!(cycle.current, 0);
    assert_eq!(cycle.next, 17_118);
    assert!(cycle.cycle_completed.is_empty());

    let mut revision = DataUsageCacheRevision::Missing;
    let mut leader_epoch = restart_floor.leader_epoch;
    assert!(
        claim_scanner_leadership(
            &CancellationToken::new(),
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut leader_epoch,
            true,
            ScannerCycleResetPolicy::ResetCoveragePreservingNext,
        )
        .await
    );
    assert_eq!(leader_epoch, 8);
    assert_eq!(cycle.next, 17_118);
    assert_eq!(cycle.current, 0);
    assert!(cycle.cycle_completed.is_empty());

    let source = DataUsageCacheSource::new(0, 0);
    let scan_plan_digest = DataUsageScanPlanDigest([7; 32]);
    for (cache_path, name) in [
        (DATA_USAGE_CACHE_NAME.to_string(), DATA_USAGE_ROOT),
        (format!("photos/{DATA_USAGE_CACHE_NAME}"), "photos"),
    ] {
        let mut historical = DataUsageCache::default();
        historical.info.name = name.to_string();
        historical.info.next_cycle = 17_118;
        historical.info.leader_epoch = 7;
        historical.info.source = Some(source);
        historical.info.scan_plan_digest = Some(scan_plan_digest);
        historical.info.cache_key_format = DATA_USAGE_CACHE_KEY_FORMAT;
        historical.info.snapshot_complete = true;
        historical.replace(name, "", DataUsageEntry::default());
        historical
            .save(store.clone(), &cache_path)
            .await
            .expect("historical scanner cache should persist through the storage path");

        let mut recovered = DataUsageCache::default();
        let revisions = recovered
            .load_with_revisions(store.clone(), &cache_path)
            .await
            .expect("historical scanner cache should reload with CAS revisions");
        assert_eq!(recovered.info.name, name);
        assert_eq!(recovered.info.next_cycle, 17_118);
        assert_eq!(recovered.info.leader_epoch, 7);
        assert!(!recovered.cache.is_empty());

        assert_eq!(
            recovered.prepare_for_scan(name, cycle.next, leader_epoch, source, scan_plan_digest, true),
            DataUsageCachePrepareOutcome::Reset,
            "recovered cache should reset without a cycle regression: {cache_path}"
        );
        assert_eq!(recovered.info.next_cycle, 17_118);
        assert_eq!(recovered.info.leader_epoch, 8);
        assert!(!recovered.info.snapshot_complete);
        assert!(recovered.cache.is_empty());

        recovered
            .save_with_revisions(store.clone(), &cache_path, &revisions)
            .await
            .expect("reset scanner cache should persist with its loaded revisions");
        let mut persisted_reset = DataUsageCache::default();
        persisted_reset
            .load(store.clone(), &cache_path)
            .await
            .expect("persisted reset scanner cache should reload");
        assert_eq!(persisted_reset.info.name, name);
        assert_eq!(persisted_reset.info.next_cycle, 17_118);
        assert_eq!(persisted_reset.info.leader_epoch, 8);
        assert!(!persisted_reset.info.snapshot_complete);
        assert!(persisted_reset.cache.is_empty());
    }
    complete_legacy_incomplete_usage_floor_recovery(store.clone(), leader_epoch)
        .await
        .expect("leadership claim should retire the recovery marker");
    assert!(matches!(
        read_config(store.clone(), DATA_USAGE_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
    let (claimed_floor, claimed_state) = persisted_usage_floor_for_startup(store, true)
        .await
        .expect("claimed bootstrap should remain restartable");
    assert_eq!(claimed_floor.leader_epoch, 8);
    assert_eq!(claimed_state, PersistedUsageFloorStartup::BootstrapPending);
}

#[tokio::test]
async fn scanner_usage_floor_recovers_rc3_non_empty_incomplete_fence() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    store
        .objects
        .lock()
        .await
        .insert(primary_key.clone(), rc3_legacy_non_empty_usage_fence(Some(13)));
    store.revisions.lock().await.insert(primary_key, 1);

    save_config(
        store.clone(),
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str(),
        rc3_legacy_non_empty_usage_fence(None),
    )
    .await
    .expect("legacy usage should persist");

    let (floor, startup) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("rc.3 non-empty incomplete fence should enter recovery");
    assert_eq!(
        floor,
        PersistedUsageFloor {
            next_cycle: 0,
            leader_epoch: 13,
        }
    );
    assert_eq!(startup, PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence);

    let primary = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("recovered usage bootstrap should replace the old floor");
    let pending = serde_json::from_slice::<DataUsageInfo>(&primary).expect("recovered usage bootstrap should decode");
    assert!(data_usage_info_is_bootstrap_pending(&pending));
    assert!(!data_usage_info_has_persisted_baseline_identity(&pending));
    assert_eq!(pending.scanner_epoch, Some(13));
    assert!(read_config(store, DATA_USAGE_RECOVERY_PATH.as_str()).await.is_ok());
}

#[tokio::test]
async fn scanner_usage_floor_prefers_newer_backup_over_rc3_non_empty_incomplete_fence() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    store
        .objects
        .lock()
        .await
        .insert(primary_key, rc3_legacy_non_empty_usage_fence(Some(13)));

    let mut backup = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 2);
    backup.scanner_epoch = Some(14);
    backup.scanner_cycle = Some(9845);
    save_config(
        store.clone(),
        &format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
        serde_json::to_vec(&backup).expect("newer backup should encode"),
    )
    .await
    .expect("newer backup should persist");

    let (floor, startup) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("newer authoritative backup should win over the old incomplete floor");
    assert_eq!(
        floor,
        PersistedUsageFloor {
            next_cycle: 9846,
            leader_epoch: 14,
        }
    );
    assert_eq!(startup, PersistedUsageFloorStartup::Authoritative);
    assert!(matches!(
        read_config(store, DATA_USAGE_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
async fn scanner_usage_floor_rejects_noncanonical_non_empty_incomplete_fences() {
    let base = serde_json::from_slice::<serde_json::Value>(&rc3_legacy_non_empty_usage_fence(Some(13)))
        .expect("pinned rc.3 usage fence should decode");
    let mut cases = Vec::new();

    let mut unknown_top_level = base.clone();
    unknown_top_level["future_field"] = serde_json::Value::Bool(true);
    cases.push(("unknown top-level field", unknown_top_level));

    let mut unknown_bucket_field = base.clone();
    unknown_bucket_field["buckets_usage"]["photos"]["future_field"] = serde_json::Value::Bool(true);
    cases.push(("unknown bucket field", unknown_bucket_field));

    let mut wrong_bucket_size = base.clone();
    wrong_bucket_size["bucket_sizes"]["photos"] = serde_json::Value::from(987_654_320_u64);
    cases.push(("bucket size mismatch", wrong_bucket_size));

    let mut wrong_total = base.clone();
    wrong_total["objects_total_count"] = serde_json::Value::from(156_382_068_u64);
    cases.push(("object total mismatch", wrong_total));

    let mut wrong_versions = base.clone();
    wrong_versions["versions_total_count"] = serde_json::Value::from(156_382_071_u64);
    cases.push(("version total mismatch", wrong_versions));

    let mut wrong_delete_markers = base.clone();
    wrong_delete_markers["delete_markers_total_count"] = serde_json::Value::from(4_u64);
    cases.push(("delete marker total mismatch", wrong_delete_markers));

    let mut wrong_total_size = base.clone();
    wrong_total_size["objects_total_size"] = serde_json::Value::from(987_654_320_u64);
    cases.push(("object size total mismatch", wrong_total_size));

    let mut wrong_cardinality = base.clone();
    wrong_cardinality["buckets_count"] = serde_json::Value::from(2_u64);
    cases.push(("bucket cardinality mismatch", wrong_cardinality));

    let mut overflow = base.clone();
    let mut overflow_bucket = overflow["buckets_usage"]["photos"].clone();
    overflow_bucket["objects_count"] = serde_json::Value::from(u64::MAX);
    overflow_bucket["size"] = serde_json::Value::from(0_u64);
    overflow["buckets_usage"]["overflow"] = overflow_bucket;
    overflow["bucket_sizes"]["overflow"] = serde_json::Value::from(0_u64);
    overflow["buckets_count"] = serde_json::Value::from(2_u64);
    cases.push(("checked total overflow", overflow));

    let mut invalid_epoch = base;
    invalid_epoch["scanner_epoch"] = serde_json::Value::from(0_u64);
    cases.push(("invalid epoch", invalid_epoch));

    for (case, value) in cases {
        let store = Arc::new(MemoryConfigStore::default());
        let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let original = serde_json::to_vec(&value).expect("noncanonical usage fixture should encode");
        store.objects.lock().await.insert(primary_key.clone(), original.clone());
        store.revisions.lock().await.insert(primary_key, 1);

        let err = persisted_usage_floor_for_startup(store.clone(), true)
            .await
            .expect_err("noncanonical incomplete usage must remain fail-closed");
        assert!(err.to_string().contains("usage-state/reset"), "unexpected error for {case}: {err}");
        assert_eq!(
            read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
                .await
                .expect("rejected usage primary should remain"),
            original,
            "rejected primary changed for {case}"
        );
        assert!(
            matches!(
                read_config(store, DATA_USAGE_RECOVERY_PATH.as_str()).await,
                Err(EcstoreError::ConfigNotFound)
            ),
            "recovery marker should not be written for {case}"
        );
    }
}

#[tokio::test]
async fn scanner_usage_floor_rejects_duplicate_legacy_fields() {
    let base = String::from_utf8(rc3_legacy_non_empty_usage_fence(Some(13))).expect("pinned rc.3 usage fence should be UTF-8");
    let base_value = serde_json::from_str::<serde_json::Value>(&base).expect("pinned rc.3 usage fence should decode");
    let duplicate_top_level = base.replacen(
        "\"objects_total_count\":156382067",
        "\"objects_total_count\":156382067,\"objects_total_count\":156382067",
        1,
    );
    let duplicate_bucket = base.replacen("\"size\":987654321", "\"size\":987654321,\"size\":987654321", 1);
    let bucket = serde_json::to_string(&base_value["buckets_usage"]["photos"]).expect("pinned rc.3 bucket usage should encode");
    let bucket_map = format!("\"buckets_usage\":{{\"photos\":{bucket}}}");
    let duplicate_bucket_key =
        base.replacen(&bucket_map, &format!("\"buckets_usage\":{{\"photos\":{bucket},\"photos\":{bucket}}}"), 1);
    let duplicate_bucket_size_key = base.replacen(
        "\"bucket_sizes\":{\"photos\":987654321}",
        "\"bucket_sizes\":{\"photos\":987654321,\"photos\":987654321}",
        1,
    );
    let duplicate_histogram_key =
        base.replacen("\"object_size_histogram\":{}", "\"object_size_histogram\":{\"small\":1,\"small\":1}", 1);
    let mut duplicate_target_field = base.clone();
    let target_map = "\"replication_info\":{\"target\":{\"replication_pending_size\":0,\"replication_failed_size\":0,\"replicated_size\":0,\"replica_size\":0,\"replication_pending_count\":0,\"replication_failed_count\":0,\"replicated_count\":0,\"replicated_count\":0}}";
    let target_offset = duplicate_target_field
        .rfind("\"replication_info\":{}")
        .expect("pinned fixture should contain bucket replication info");
    duplicate_target_field.replace_range(target_offset..target_offset + "\"replication_info\":{}".len(), target_map);
    let target = "{\"replication_pending_size\":0,\"replication_failed_size\":0,\"replicated_size\":0,\"replica_size\":0,\"replication_pending_count\":0,\"replication_failed_count\":0,\"replicated_count\":0}";
    let mut duplicate_target_key = base.clone();
    let target_offset = duplicate_target_key
        .rfind("\"replication_info\":{}")
        .expect("pinned fixture should contain bucket replication info");
    let target_map = format!("\"replication_info\":{{\"target\":{target},\"target\":{target}}}");
    duplicate_target_key.replace_range(target_offset..target_offset + "\"replication_info\":{}".len(), &target_map);

    let tier = "{\"total_size\":0,\"num_versions\":0,\"num_objects\":0}";
    let duplicate_tier_key = base.replacen(
        '{',
        &format!("{{\"tier_stats\":{{\"tiers\":{{\"STANDARD\":{tier},\"STANDARD\":{tier}}}}},"),
        1,
    );

    for (case, original, expected_error) in [
        ("top-level field", duplicate_top_level.into_bytes(), "duplicate field"),
        ("bucket field", duplicate_bucket.into_bytes(), "duplicate field"),
        ("replication target field", duplicate_target_field.into_bytes(), "duplicate field"),
        ("bucket map key", duplicate_bucket_key.into_bytes(), "usage-state/reset"),
        ("bucket size map key", duplicate_bucket_size_key.into_bytes(), "usage-state/reset"),
        ("histogram map key", duplicate_histogram_key.into_bytes(), "usage-state/reset"),
        ("replication target map key", duplicate_target_key.into_bytes(), "usage-state/reset"),
        ("tier map key", duplicate_tier_key.into_bytes(), "usage-state/reset"),
    ] {
        let store = Arc::new(MemoryConfigStore::default());
        let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        store.objects.lock().await.insert(primary_key.clone(), original.clone());
        store.revisions.lock().await.insert(primary_key, 1);

        let err = match persisted_usage_floor_for_startup(store.clone(), true).await {
            Err(err) => err,
            Ok(result) => panic!("duplicate {case} must remain fail-closed: {result:?}"),
        };
        assert!(err.to_string().contains(expected_error), "unexpected error for {case}: {err}");
        assert_eq!(
            read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
                .await
                .expect("rejected duplicate-field primary should remain"),
            original,
            "rejected primary changed for {case}"
        );
        assert!(
            matches!(
                read_config(store, DATA_USAGE_RECOVERY_PATH.as_str()).await,
                Err(EcstoreError::ConfigNotFound)
            ),
            "recovery marker should not be written for {case}"
        );
    }
}

#[tokio::test]
async fn scanner_usage_floor_rejects_current_schema_incomplete_snapshot() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let mut current = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 2);
    current.usage_snapshot_complete = false;
    current.scanner_epoch = Some(13);
    current.scanner_cycle = None;
    let original = serde_json::to_vec(&current).expect("current incomplete usage should encode");
    assert!(
        serde_json::from_slice::<serde_json::Value>(&original)
            .expect("current incomplete usage should decode")
            .get("usage_snapshot_partial")
            .is_some()
    );
    store.objects.lock().await.insert(primary_key.clone(), original.clone());
    store.revisions.lock().await.insert(primary_key, 1);

    let err = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect_err("current schema incomplete usage must remain fail-closed");
    assert!(err.to_string().contains("usage-state/reset"), "unexpected error: {err}");
    assert_eq!(
        read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("rejected current usage primary should remain"),
        original
    );
    assert!(matches!(
        read_config(store, DATA_USAGE_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
async fn scanner_usage_floor_recovery_preserves_newer_authoritative_companion_floor() {
    for companion_path in [
        format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.clone(),
    ] {
        let store = Arc::new(MemoryConfigStore::default());
        let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        store
            .objects
            .lock()
            .await
            .insert(primary_key.clone(), rc3_legacy_empty_usage_fence(Some(7)));
        store.revisions.lock().await.insert(primary_key, 1);
        persisted_usage_floor_for_startup(store.clone(), true)
            .await
            .expect("legacy empty primary should enter recovery");

        let mut companion = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
        companion.scanner_epoch = Some(8);
        companion.scanner_cycle = Some(11);
        store.objects.lock().await.insert(
            memory_config_key(RUSTFS_META_BUCKET, &companion_path),
            serde_json::to_vec(&companion).expect("authoritative companion should encode"),
        );
        if companion_path.ends_with(".bkp") {
            let mut stale_legacy = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
            stale_legacy.scanner_epoch = Some(6);
            stale_legacy.scanner_cycle = Some(10);
            store.objects.lock().await.insert(
                memory_config_key(RUSTFS_META_BUCKET, LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
                serde_json::to_vec(&stale_legacy).expect("stale legacy companion should encode"),
            );
        } else {
            let mut stale_backup = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
            stale_backup.scanner_epoch = Some(6);
            stale_backup.scanner_cycle = Some(10);
            store.objects.lock().await.insert(
                memory_config_key(RUSTFS_META_BUCKET, &format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str())),
                serde_json::to_vec(&stale_backup).expect("stale legacy backup should encode"),
            );
        }

        let (floor, state) = persisted_usage_floor_for_startup(store, true)
            .await
            .expect("a newer authoritative companion should advance the recovery floor");
        assert_eq!(floor.leader_epoch, 8, "unexpected companion path: {companion_path}");
        assert_eq!(floor.next_cycle, 12, "unexpected companion path: {companion_path}");
        assert_eq!(state, PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence);
    }
}

#[tokio::test]
async fn scanner_usage_floor_recovery_fences_non_authoritative_legacy_backup() {
    for partial_backup in [false, true] {
        let store = Arc::new(MemoryConfigStore::default());
        let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        store
            .objects
            .lock()
            .await
            .insert(primary_key.clone(), rc3_legacy_empty_usage_fence(Some(7)));
        store.revisions.lock().await.insert(primary_key, 1);
        persisted_usage_floor_for_startup(store.clone(), true)
            .await
            .expect("legacy empty primary should enter recovery");

        let mut legacy_primary = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
        legacy_primary.scanner_epoch = Some(8);
        legacy_primary.scanner_cycle = Some(11);
        store.objects.lock().await.insert(
            memory_config_key(RUSTFS_META_BUCKET, LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
            serde_json::to_vec(&legacy_primary).expect("legacy primary should encode"),
        );
        let backup_path = format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str());
        let backup = if partial_backup {
            serde_json::to_vec(&DataUsageInfo {
                last_update: Some(std::time::SystemTime::UNIX_EPOCH),
                scanner_epoch: Some(9),
                buckets_count: 1,
                ..Default::default()
            })
            .expect("partial legacy backup should encode")
        } else {
            rc3_legacy_empty_usage_fence(Some(9))
        };
        store
            .objects
            .lock()
            .await
            .insert(memory_config_key(RUSTFS_META_BUCKET, &backup_path), backup);

        if partial_backup {
            let err = persisted_usage_floor_for_startup(store, true)
                .await
                .expect_err("a partial noncanonical backup must remain fail-closed");
            assert!(err.to_string().contains("conflicts with persisted usage state"));
        } else {
            let (floor, state) = persisted_usage_floor_for_startup(store, true)
                .await
                .expect("an exact empty backup should contribute its epoch fence");
            assert_eq!(floor.leader_epoch, 9);
            assert_eq!(floor.next_cycle, 12);
            assert_eq!(state, PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence);
        }
    }
}

#[tokio::test]
async fn scanner_usage_floor_recovery_resumes_after_marker_only_crash_point() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let original = rc3_legacy_empty_usage_fence(Some(7));
    store.objects.lock().await.insert(primary_key.clone(), original.clone());
    store.revisions.lock().await.insert(primary_key.clone(), 1);
    store.fail_put_number.lock().await.insert(primary_key, 1);

    persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect_err("injected primary CAS failure should leave recovery pending");
    assert!(read_config(store.clone(), DATA_USAGE_RECOVERY_PATH.as_str()).await.is_ok());
    assert_eq!(
        read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("legacy primary should remain after the failed CAS"),
        original
    );

    let (floor, state) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("the durable marker should resume the primary conversion");
    assert_eq!(floor.leader_epoch, 7);
    assert_eq!(state, PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence);
    let recovered = read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("recovered bootstrap should replace the legacy primary");
    assert!(data_usage_info_is_bootstrap_pending(
        &serde_json::from_slice(&recovered).expect("recovered bootstrap should decode")
    ));
}

#[tokio::test]
async fn scanner_usage_floor_recovery_reconciles_marker_post_commit_error() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let marker_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_RECOVERY_PATH.as_str());
    store
        .objects
        .lock()
        .await
        .insert(primary_key.clone(), rc3_legacy_empty_usage_fence(Some(7)));
    store.revisions.lock().await.insert(primary_key, 1);
    store.error_after_commit_put_number.lock().await.insert(marker_key, 1);

    let (floor, state) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("a committed recovery marker should reconcile after an ambiguous error");
    assert_eq!(floor.leader_epoch, 7);
    assert_eq!(state, PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence);
    assert!(read_config(store, DATA_USAGE_RECOVERY_PATH.as_str()).await.is_ok());
}

#[tokio::test]
async fn scanner_usage_floor_recovery_reconciles_marker_delete_post_commit_error() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    store
        .objects
        .lock()
        .await
        .insert(primary_key.clone(), rc3_legacy_empty_usage_fence(Some(7)));
    store.revisions.lock().await.insert(primary_key, 1);
    let (floor, state) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("legacy empty primary should enter recovery");
    let mut cycle = CurrentCycle::default();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut leader_epoch = floor.leader_epoch;
    let (allow_pending, cycle_reset_policy) = prepare_cycle_for_usage_floor_bootstrap(&mut cycle, floor, state);
    assert!(
        claim_scanner_leadership(
            &CancellationToken::new(),
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut leader_epoch,
            allow_pending,
            cycle_reset_policy,
        )
        .await
    );
    store
        .error_after_commit_deletes
        .lock()
        .await
        .insert(memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_RECOVERY_PATH.as_str()));

    complete_legacy_incomplete_usage_floor_recovery(store.clone(), leader_epoch)
        .await
        .expect("a committed marker delete should reconcile after an ambiguous error");
    assert!(matches!(
        read_config(store, DATA_USAGE_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
    assert_eq!(scanner_cycle_recovery_status().state, "healthy");
}

#[tokio::test]
#[serial]
async fn scanner_usage_floor_recovery_retry_budget_uses_marker_epoch_identity() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    store
        .objects
        .lock()
        .await
        .insert(primary_key.clone(), rc3_legacy_empty_usage_fence(Some(7)));
    store.revisions.lock().await.insert(primary_key.clone(), 1);
    persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("legacy empty primary should enter recovery");
    assert!(record_scanner_cycle_recovery_retry(3));
    let first_detected = scanner_cycle_recovery_status().first_detected_at_unix_secs;

    let primary = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("recovered bootstrap should exist");
    let mut pending = serde_json::from_slice::<DataUsageInfo>(&primary).expect("recovered bootstrap should decode");
    pending.scanner_epoch = Some(8);
    store.objects.lock().await.insert(
        primary_key.clone(),
        serde_json::to_vec(&pending).expect("claimed bootstrap should encode"),
    );
    *store.revisions.lock().await.entry(primary_key).or_insert(1) += 1;

    let (floor, state) = persisted_usage_floor_for_startup(store, true)
        .await
        .expect("claimed bootstrap should retain its recovery identity");
    assert_eq!(floor.leader_epoch, 8);
    assert_eq!(state, PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence);
    let status = scanner_cycle_recovery_status();
    assert_eq!(status.leader_epoch, Some(7));
    assert_eq!(status.retry_count, 3);
    assert_eq!(status.first_detected_at_unix_secs, first_detected);
    clear_legacy_incomplete_usage_floor_recovery_status();
}

#[tokio::test]
async fn scanner_usage_floor_rejects_noncanonical_empty_fence() {
    let store = Arc::new(MemoryConfigStore::default());
    let mut value = serde_json::from_slice::<serde_json::Value>(&rc3_legacy_empty_usage_fence(Some(7)))
        .expect("legacy fixture should decode");
    value
        .as_object_mut()
        .expect("legacy fixture should be an object")
        .insert("future_field".to_string(), serde_json::Value::Bool(true));
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        serde_json::to_vec(&value).expect("noncanonical fixture should encode"),
    );

    let err = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect_err("unknown legacy fields must not be recovered as an empty baseline");
    assert!(err.to_string().contains("no authoritative baseline"));
    assert!(matches!(
        read_config(store, DATA_USAGE_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
async fn scanner_usage_floor_rejects_zero_or_exhausted_empty_fence_epoch() {
    for epoch in [0, u64::MAX - 1, u64::MAX] {
        let store = Arc::new(MemoryConfigStore::default());
        let primary = rc3_legacy_empty_usage_fence(Some(epoch));
        store
            .objects
            .lock()
            .await
            .insert(memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()), primary.clone());

        persisted_usage_floor_for_startup(store.clone(), true)
            .await
            .expect_err("an unclaimable legacy epoch must remain fail-closed");
        assert_eq!(
            read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
                .await
                .expect("rejected legacy floor should remain unchanged"),
            primary
        );
        assert!(matches!(
            read_config(store, DATA_USAGE_RECOVERY_PATH.as_str()).await,
            Err(EcstoreError::ConfigNotFound)
        ));
    }
}

#[tokio::test]
async fn scanner_usage_floor_recovery_does_not_overwrite_concurrent_authoritative_snapshot() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    store
        .objects
        .lock()
        .await
        .insert(primary_key.clone(), rc3_legacy_empty_usage_fence(Some(7)));
    store.revisions.lock().await.insert(primary_key.clone(), 1);
    let mut authoritative = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    authoritative.scanner_epoch = Some(8);
    authoritative.scanner_cycle = Some(11);
    let authoritative = serde_json::to_vec(&authoritative).expect("authoritative usage should encode");
    store
        .interleaving_puts
        .lock()
        .await
        .insert(primary_key, (1, authoritative.clone()));

    persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect_err("recovery CAS must lose to a concurrent authoritative snapshot");
    assert_eq!(
        read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("concurrent authoritative usage should remain"),
        authoritative
    );

    let (floor, state) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("the concurrent authoritative snapshot should win on retry");
    assert_eq!(floor.leader_epoch, 8);
    assert_eq!(floor.next_cycle, 12);
    assert_eq!(state, PersistedUsageFloorStartup::Authoritative);
    assert!(matches!(
        read_config(store, DATA_USAGE_RECOVERY_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}

#[tokio::test]
async fn scanner_usage_floor_recovery_rejects_concurrent_authoritative_epoch_regression() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    store
        .objects
        .lock()
        .await
        .insert(primary_key.clone(), rc3_legacy_empty_usage_fence(Some(7)));
    store.revisions.lock().await.insert(primary_key.clone(), 1);
    let mut stale = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    stale.scanner_epoch = Some(6);
    stale.scanner_cycle = Some(11);
    let stale = serde_json::to_vec(&stale).expect("stale authoritative usage should encode");
    store.interleaving_puts.lock().await.insert(primary_key, (1, stale.clone()));

    persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect_err("recovery CAS must lose to the concurrent writer");
    let retry_error = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect_err("the recovery marker must fence an older authoritative winner");
    assert!(retry_error.to_string().contains("older than the required recovery fence"));
    assert_eq!(
        read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("stale concurrent snapshot should not be rewritten without a new scan"),
        stale
    );
    assert!(read_config(store, DATA_USAGE_RECOVERY_PATH.as_str()).await.is_ok());
}

#[test]
#[serial]
fn scanner_usage_floor_failure_is_exposed_and_cleared() {
    record_scanner_usage_floor_failure("persisted usage floor is invalid".to_string());
    let blocked = scanner_cycle_recovery_status();
    assert_eq!(blocked.path, DATA_USAGE_OBJ_NAME_PATH.as_str());
    assert_eq!(blocked.state, "usage_floor_load_failed");
    assert_eq!(blocked.classification.as_deref(), Some("usage_floor_load_failed"));
    assert!(blocked.retryable);
    assert_eq!(blocked.reason.as_deref(), Some("persisted usage floor is invalid"));
    let first_detected = blocked.first_detected_at_unix_secs;

    assert!(record_scanner_cycle_recovery_retry(2));
    record_scanner_usage_floor_failure("persisted usage floor remains invalid".to_string());
    let retried = scanner_cycle_recovery_status();
    assert_eq!(retried.retry_count, 2);
    assert_eq!(retried.first_detected_at_unix_secs, first_detected);

    clear_scanner_usage_floor_failure();
    let healthy = scanner_cycle_recovery_status();
    assert_eq!(healthy.state, "healthy");
    assert_eq!(healthy.path, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    assert_eq!(healthy.classification, None);
}

#[test]
#[serial]
fn scanner_usage_floor_recovery_stays_retryable_until_claim_cleanup() {
    record_legacy_incomplete_usage_floor_recovery_pending(7);
    let pending = scanner_cycle_recovery_status();
    assert_eq!(pending.state, "usage_floor_recovery_pending");
    assert_eq!(pending.classification.as_deref(), Some("legacy_empty_usage_floor"));
    assert_eq!(pending.leader_epoch, Some(7));
    assert!(pending.retryable);
    assert_eq!(pending.quarantine_path.as_deref(), Some(DATA_USAGE_RECOVERY_PATH.as_str()));
    let first_detected = pending.first_detected_at_unix_secs;

    assert!(record_scanner_cycle_recovery_retry(3));
    record_legacy_incomplete_usage_floor_recovery_pending(7);
    let retried = scanner_cycle_recovery_status();
    assert_eq!(retried.retry_count, 3);
    assert_eq!(retried.first_detected_at_unix_secs, first_detected);

    clear_legacy_incomplete_usage_floor_recovery_status();
    assert_eq!(scanner_cycle_recovery_status().state, "healthy");
}

#[test]
#[serial]
fn scanner_cache_cycle_ahead_is_visible_until_a_later_scan_clears_it() {
    record_scanner_cache_cycle_ahead(0, 17_118, 8);
    let pending = scanner_cycle_recovery_status();
    assert_eq!(pending.state, "cache_cycle_ahead");
    assert_eq!(pending.classification.as_deref(), Some("cache_cycle_ahead"));
    assert_eq!(pending.generation, Some(17_118));
    assert_eq!(pending.leader_epoch, Some(8));
    assert!(pending.retryable);
    assert_eq!(pending.max_retries, 0);
    assert_eq!(
        pending.reason.as_deref(),
        Some("persisted scanner cache cycle 17118 is ahead of requested cycle 0")
    );
    let first_detected = pending.first_detected_at_unix_secs;

    record_scanner_cache_cycle_ahead(0, 17_118, 8);
    let observed_again = scanner_cycle_recovery_status();
    assert_eq!(observed_again.retry_count, 0);
    assert_eq!(observed_again.first_detected_at_unix_secs, first_detected);

    assert!(record_scanner_cycle_recovery_retry(4));
    assert_eq!(scanner_cycle_recovery_status().retry_count, 0);

    record_scanner_cache_cycle_recovery_attempt();
    record_scanner_cache_cycle_recovery_attempt();
    let retried = scanner_cycle_recovery_status();
    assert_eq!(retried.retry_count, 2);
    assert!(retried.retryable);

    update_scanner_cache_cycle_recovery_status(
        0,
        8,
        None,
        Some(ScannerCyclePreCommitOutcome::Deferred(ScannerCycleDeferReason::DataMovement)),
        false,
    );
    assert_eq!(scanner_cycle_recovery_status().classification.as_deref(), Some("cache_cycle_ahead"));

    update_scanner_cache_cycle_recovery_status(17_118, 8, None, None, false);
    assert_eq!(scanner_cycle_recovery_status().classification.as_deref(), Some("cache_cycle_ahead"));

    update_scanner_cache_cycle_recovery_status(17_118, 8, None, None, true);
    assert_eq!(scanner_cycle_recovery_status().state, "healthy");
}

#[tokio::test]
#[serial]
async fn scanner_usage_floor_failure_clears_stale_leader_liveness() {
    record_scanner_cycle_schedule_role("leader");
    global_metrics().record_scanner_leader_liveness("acquired", true, "").await;

    finish_scanner_leader_iteration(false, "usage_floor_load_failed", "invalid floor".to_string()).await;

    assert_eq!(scanner_cycle_schedule_status().execution_role, "unknown");
    let report = global_metrics().report().await;
    assert_eq!(report.leader_lock_state, "usage_floor_load_failed");
    assert!(!report.leader_lock_held_by_this_process);
    assert_eq!(report.leader_lock_last_error, "invalid floor");

    global_metrics().record_scanner_leader_liveness("acquired", true, "").await;
    finish_scanner_leader_iteration(true, "stopped", "lock lost before classification".to_string()).await;
    let report = global_metrics().report().await;
    assert_eq!(report.leader_lock_state, "stopped");
    assert!(!report.leader_lock_held_by_this_process);
    assert_eq!(report.leader_lock_last_error, "lock lost before classification");
}

#[tokio::test]
async fn scanner_usage_floor_recovers_from_incomplete_v2_primary_using_fenced_backup() {
    let store = Arc::new(MemoryConfigStore::default());
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());

    // This shape is valid JSON from an interrupted v2 publication, but it is
    // not a durable baseline because the snapshot is incomplete. It must not
    // be converted into an empty floor.
    let primary = DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(100),
        usage_snapshot_complete: false,
        ..Default::default()
    };
    let mut backup = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    backup.scanner_epoch = Some(7);
    backup.scanner_cycle = Some(103);

    for (path, usage) in [(DATA_USAGE_OBJ_NAME_PATH.as_str(), primary), (backup_path.as_str(), backup)] {
        store.objects.lock().await.insert(
            memory_config_key(RUSTFS_META_BUCKET, path),
            serde_json::to_vec(&usage).expect("usage snapshot should encode"),
        );
    }

    assert_eq!(
        persisted_usage_floor(store)
            .await
            .expect("valid backup should recover the usage floor"),
        PersistedUsageFloor {
            next_cycle: 104,
            leader_epoch: 7,
        }
    );
}

async fn seed_legacy_primary_read_error_with_backup(store: &Arc<MemoryConfigStore>, error: EcstoreError, epoch: u64, cycle: u64) {
    let legacy_primary = LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str();
    let legacy_backup = format!("{legacy_primary}.bkp");
    let mut backup = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    backup.scanner_epoch = Some(epoch);
    backup.scanner_cycle = Some(cycle);

    store
        .read_errors
        .lock()
        .await
        .insert(memory_config_key(RUSTFS_META_BUCKET, legacy_primary), error);
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, &legacy_backup),
        serde_json::to_vec(&backup).expect("legacy backup usage snapshot should encode"),
    );
}

#[tokio::test]
async fn scanner_usage_floor_recovers_legacy_backup_after_primary_decode_error() {
    let store = Arc::new(MemoryConfigStore::default());
    seed_legacy_primary_read_error_with_backup(&store, EcstoreError::other("InlineData value out of range"), 19, 41).await;

    let (floor, state) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("valid legacy backup should recover the startup floor");
    assert_eq!(state, PersistedUsageFloorStartup::Authoritative);
    assert_eq!(
        floor,
        PersistedUsageFloor {
            next_cycle: 42,
            leader_epoch: 19,
        }
    );
    assert_eq!(
        persisted_usage_floor(store)
            .await
            .expect("valid legacy backup should recover the authoritative floor"),
        floor
    );
}

#[tokio::test]
async fn scanner_usage_floor_does_not_bootstrap_over_corrupt_legacy_primary_without_backup() {
    let store = Arc::new(MemoryConfigStore::default());
    let legacy_primary = LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str();
    store
        .read_errors
        .lock()
        .await
        .insert(memory_config_key(RUSTFS_META_BUCKET, legacy_primary), EcstoreError::FileCorrupt);

    let err = persisted_usage_floor_for_startup(store, true)
        .await
        .expect_err("corrupt legacy primary without a valid backup must remain fail-closed");
    assert!(err.to_string().contains("no valid scanner usage floor backup"), "unexpected error: {err}");
}

#[tokio::test]
async fn scanner_usage_floor_does_not_fallback_to_legacy_after_corrupt_v2_primary() {
    let store = Arc::new(MemoryConfigStore::default());
    let mut v2_backup = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    v2_backup.scanner_epoch = Some(8);
    v2_backup.scanner_cycle = Some(11);
    let mut legacy = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    legacy.scanner_epoch = Some(3);
    legacy.scanner_cycle = Some(7);
    store.read_errors.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        EcstoreError::FileCorrupt,
    );
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, &format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str())),
        serde_json::to_vec(&v2_backup).expect("v2 backup usage snapshot should encode"),
    );
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
        serde_json::to_vec(&legacy).expect("legacy usage snapshot should encode"),
    );

    let err = persisted_usage_floor_for_startup(store, true)
        .await
        .expect_err("corrupt v2 primary must not recover without a primary revision");
    assert!(
        err.to_string().contains(&format!(
            "failed to read scanner usage epoch floor from {}",
            DATA_USAGE_OBJ_NAME_PATH.as_str()
        )),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn scanner_usage_floor_keeps_transient_primary_read_error_fail_closed() {
    let store = Arc::new(MemoryConfigStore::default());
    let legacy_primary = LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str();
    seed_legacy_primary_read_error_with_backup(
        &store,
        EcstoreError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset while reading usage primary",
        )),
        19,
        41,
    )
    .await;

    let err = persisted_usage_floor_for_startup(store, true)
        .await
        .expect_err("transient primary errors must not be converted into backup recovery");
    assert!(
        err.to_string()
            .contains(&format!("failed to read scanner usage epoch floor from {legacy_primary}")),
        "unexpected error: {err}"
    );
    assert!(
        !err.to_string().contains("no valid scanner usage floor backup"),
        "transient error should not enter corrupt-primary fallback: {err}"
    );
}

#[tokio::test]
async fn scanner_usage_floor_keeps_outdated_primary_metadata_fail_closed() {
    let store = Arc::new(MemoryConfigStore::default());
    let legacy_primary = LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str();
    seed_legacy_primary_read_error_with_backup(&store, EcstoreError::OutdatedXLMeta, 19, 41).await;

    let err = persisted_usage_floor_for_startup(store, true)
        .await
        .expect_err("outdated primary metadata must not be converted into backup recovery");
    assert!(
        err.to_string()
            .contains(&format!("failed to read scanner usage epoch floor from {legacy_primary}")),
        "unexpected error: {err}"
    );
    assert!(
        !err.to_string().contains("no valid scanner usage floor backup"),
        "outdated metadata should not enter corrupt-primary fallback: {err}"
    );
}

#[tokio::test]
async fn scanner_usage_floor_does_not_bootstrap_over_incomplete_v2_primary() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary = DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(100),
        usage_snapshot_complete: false,
        ..Default::default()
    };
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        serde_json::to_vec(&primary).expect("usage snapshot should encode"),
    );

    let err = persisted_usage_floor_for_startup(store, true)
        .await
        .expect_err("an existing incomplete primary must remain fail-closed");
    assert!(err.to_string().contains("no authoritative baseline"));
}

#[tokio::test]
async fn scanner_usage_floor_rejects_backup_older_than_incomplete_v2_primary() {
    let store = Arc::new(MemoryConfigStore::default());
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let primary = DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(100),
        usage_snapshot_complete: false,
        ..Default::default()
    };
    let mut backup = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    backup.scanner_epoch = Some(6);
    backup.scanner_cycle = Some(10_000);

    for (path, usage) in [(DATA_USAGE_OBJ_NAME_PATH.as_str(), primary), (backup_path.as_str(), backup)] {
        store.objects.lock().await.insert(
            memory_config_key(RUSTFS_META_BUCKET, path),
            serde_json::to_vec(&usage).expect("usage snapshot should encode"),
        );
    }

    let err = persisted_usage_floor_for_startup(store, true)
        .await
        .expect_err("an older backup must not cross the incomplete primary epoch fence");
    assert!(err.to_string().contains("older than the required recovery fence"));
}

#[tokio::test]
async fn scanner_usage_floor_rejects_older_legacy_primary_after_incomplete_v2_primary() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary = DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(100),
        usage_snapshot_complete: false,
        ..Default::default()
    };
    let mut legacy = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    legacy.scanner_epoch = Some(6);
    legacy.scanner_cycle = Some(103);
    for (path, usage) in [
        (DATA_USAGE_OBJ_NAME_PATH.as_str(), primary),
        (LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str(), legacy),
    ] {
        store.objects.lock().await.insert(
            memory_config_key(RUSTFS_META_BUCKET, path),
            serde_json::to_vec(&usage).expect("usage snapshot should encode"),
        );
    }

    let err = persisted_usage_floor_for_startup(store, true)
        .await
        .expect_err("an older legacy baseline must not cross the incomplete v2 epoch fence");
    assert!(err.to_string().contains("older than the required recovery fence"));
}

#[tokio::test]
async fn scanner_leadership_fencing_recovers_incomplete_v2_primary_from_backup() {
    let store = Arc::new(MemoryConfigStore::default());
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let primary = serde_json::to_vec(&DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(100),
        usage_snapshot_complete: false,
        ..Default::default()
    })
    .expect("incomplete usage snapshot should encode");
    let mut backup = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    backup.scanner_epoch = Some(7);
    backup.scanner_cycle = Some(103);
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, &backup_path),
        serde_json::to_vec(&backup).expect("backup usage snapshot should encode"),
    );

    let recovered = usage_snapshot_for_epoch_fence(store, Some(&primary), false)
        .await
        .expect("a valid backup should provide the fencing baseline")
        .expect("the fencing baseline should be present");
    assert_eq!(recovered.scanner_epoch, Some(7));
    assert_eq!(recovered.scanner_cycle, Some(103));
}

#[tokio::test]
async fn scanner_usage_floor_leadership_fencing_recovers_legacy_backup_after_primary_decode_error() {
    let store = Arc::new(MemoryConfigStore::default());
    seed_legacy_primary_read_error_with_backup(&store, EcstoreError::other("InlineData value out of range"), 19, 41).await;

    let recovered = usage_snapshot_for_epoch_fence(store, None, false)
        .await
        .expect("a valid legacy backup should provide the fencing baseline")
        .expect("the fencing baseline should be present");
    assert_eq!(recovered.scanner_epoch, Some(19));
    assert_eq!(recovered.scanner_cycle, Some(41));
}

#[tokio::test]
async fn scanner_usage_floor_ignores_older_backup_after_primary_epoch_fence() {
    let store = Arc::new(MemoryConfigStore::default());
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    for (path, epoch, cycle) in [(DATA_USAGE_OBJ_NAME_PATH.as_str(), 8, 100), (backup_path.as_str(), 7, 10_000)] {
        let mut usage = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
        usage.scanner_epoch = Some(epoch);
        usage.scanner_cycle = Some(cycle);
        store.objects.lock().await.insert(
            memory_config_key(RUSTFS_META_BUCKET, path),
            serde_json::to_vec(&usage).expect("usage snapshot should encode"),
        );
    }

    assert_eq!(
        persisted_usage_floor(store).await.expect("usage floor should load"),
        PersistedUsageFloor {
            next_cycle: 101,
            leader_epoch: 8,
        }
    );
}

#[test]
fn scanner_startup_treats_incomplete_usage_snapshot_as_cold() {
    let mut legacy = complete_usage_with_bucket_count(Some(std::time::SystemTime::now()), 1);
    legacy.usage_snapshot_complete = false;

    assert!(data_usage_info_is_cold(&legacy));
    assert!(!data_usage_info_is_cold(&complete_usage_with_bucket_count(
        Some(std::time::SystemTime::now()),
        1,
    )));
    assert!(!data_usage_info_is_cold(&DataUsageInfo {
        last_update: Some(std::time::SystemTime::now()),
        usage_snapshot_complete: true,
        ..Default::default()
    }));
}

#[test]
fn scanner_baseline_identity_requires_complete_or_strict_legacy_shape() {
    assert!(!data_usage_info_has_persisted_baseline_identity(&DataUsageInfo {
        scanner_epoch: Some(3),
        scanner_cycle: Some(7),
        ..Default::default()
    }));

    let mut legacy = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    legacy.usage_snapshot_complete = false;
    legacy.scanner_cycle = Some(7);
    assert!(data_usage_info_has_persisted_baseline_identity(&legacy));

    legacy.scanner_epoch = Some(3);
    assert!(!data_usage_info_has_persisted_baseline_identity(&legacy));
}

#[test]
fn scanner_startup_prompts_only_for_a_newer_valid_observation() {
    let authoritative = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH),
        scanner_epoch: Some(4),
        scanner_cycle: Some(10),
        ..complete_usage_with_bucket_count(None, 0)
    };
    let observed = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
        scanner_epoch: Some(4),
        scanner_cycle: Some(11),
        usage_snapshot_converged: Some(false),
        usage_snapshot_authoritative_baseline: Some(authoritative.snapshot_identity()),
        ..complete_usage_with_bucket_count(None, 0)
    };

    assert!(usage_cache_needs_prompt_scan(&authoritative, Some(&observed)));
    assert!(!usage_cache_needs_prompt_scan(&authoritative, None));

    let mut converged = observed.clone();
    converged.usage_snapshot_converged = Some(true);
    assert!(!usage_cache_needs_prompt_scan(&authoritative, Some(&converged)));

    let mut legacy_observation = observed;
    legacy_observation.usage_snapshot_converged = None;
    assert!(!usage_cache_needs_prompt_scan(&authoritative, Some(&legacy_observation)));
}

#[tokio::test]
async fn scanner_startup_prefers_v2_over_legacy_usage() {
    let store = Arc::new(MemoryConfigStore::default());
    let mut legacy = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    legacy.scanner_epoch = Some(19);
    legacy.scanner_cycle = Some(41);
    let legacy_data = serde_json::to_vec(&legacy).expect("legacy usage snapshot should encode");
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
        legacy_data.clone(),
    );

    assert_eq!(
        read_data_usage_config_for_startup(&store)
            .await
            .expect("legacy startup usage should load"),
        Some(legacy_data)
    );
    assert_eq!(
        persisted_usage_floor(store.clone())
            .await
            .expect("legacy usage floor should seed the upgrade"),
        PersistedUsageFloor {
            next_cycle: 42,
            leader_epoch: 19,
        }
    );

    let mut authoritative = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    authoritative.scanner_epoch = Some(23);
    authoritative.scanner_cycle = Some(51);
    let authoritative_data = serde_json::to_vec(&authoritative).expect("v2 usage snapshot should encode");
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        authoritative_data.clone(),
    );

    assert_eq!(
        read_data_usage_config_for_startup(&store)
            .await
            .expect("v2 startup usage should load"),
        Some(authoritative_data)
    );
    assert_eq!(
        persisted_usage_floor(store.clone())
            .await
            .expect("v2 usage floor should be authoritative"),
        PersistedUsageFloor {
            next_cycle: 52,
            leader_epoch: 23,
        }
    );

    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        b"corrupt-v2".to_vec(),
    );
    assert_eq!(
        read_data_usage_config_for_startup(&store)
            .await
            .expect("startup inspection should preserve authoritative bytes"),
        Some(b"corrupt-v2".to_vec())
    );
    assert!(
        persisted_usage_floor(store).await.is_err(),
        "corrupt v2 state must not fall back to a legacy writer"
    );
}

#[tokio::test]
async fn scanner_usage_floor_fails_closed_on_corrupt_or_exhausted_usage_state() {
    let store = Arc::new(MemoryConfigStore::default());
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        b"not-json".to_vec(),
    );

    assert!(persisted_usage_floor(store.clone()).await.is_err());

    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        br#"{}"#.to_vec(),
    );
    assert!(
        persisted_usage_floor(store.clone()).await.is_err(),
        "a structurally incomplete usage snapshot must not be treated as an empty floor"
    );

    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        serde_json::to_vec(&DataUsageInfo {
            last_update: Some(std::time::SystemTime::now()),
            scanner_cycle: Some(1),
            usage_snapshot_bootstrap_pending: true,
            ..Default::default()
        })
        .expect("pending usage marker should encode"),
    );
    assert!(
        persisted_usage_floor(store.clone()).await.is_err(),
        "a pending marker must never pass the legacy authoritative fallback"
    );

    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        serde_json::to_vec(&DataUsageInfo {
            scanner_cycle: Some(u64::MAX - 1),
            ..Default::default()
        })
        .expect("usage snapshot should encode"),
    );
    assert!(persisted_usage_floor(store).await.is_err());
}

#[tokio::test]
async fn scanner_usage_floor_allows_only_explicit_missing_state_bootstrap() {
    let store = Arc::new(MemoryConfigStore::default());
    let (floor, state) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("a verified missing state should use the empty floor");
    assert_eq!(floor, PersistedUsageFloor::default());
    assert_eq!(state, PersistedUsageFloorStartup::Missing);
    assert!(persisted_usage_floor_for_startup(store.clone(), false).await.is_err());

    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        b"not-json".to_vec(),
    );
    assert!(
        persisted_usage_floor_for_startup(store, true).await.is_err(),
        "usage bootstrap must not hide corrupt persisted state"
    );
}

#[tokio::test]
async fn scanner_usage_floor_fails_closed_on_zero_byte_usage_objects() {
    for path in [
        DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
        format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
        format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
    ] {
        let key = memory_config_key(RUSTFS_META_BUCKET, &path);
        let existing = Arc::new(MemoryConfigStore::default());
        existing.objects.lock().await.insert(key.clone(), Vec::new());

        let err = persisted_usage_floor(existing)
            .await
            .expect_err("an empty usage object must not be treated as missing");
        assert!(
            err.to_string()
                .contains(&format!("failed to decode scanner usage floor from {path}:")),
            "unexpected error for {path}: {err}"
        );

        let appearing = Arc::new(MemoryConfigStore::default());
        appearing.insert_after_gets.lock().await.insert(key, Vec::new());

        let err = persisted_usage_floor_for_startup(appearing, true)
            .await
            .expect_err("an empty usage object appearing during confirmation must prevent usage bootstrap");
        assert!(
            err.to_string().contains("changed while confirming missing state"),
            "unexpected confirmation error for {path}: {err}"
        );
    }
}

#[tokio::test]
async fn scanner_usage_floor_requires_publication_admission_for_bootstrap() {
    let store = Arc::new(MemoryConfigStore::default());
    store.publication_admission_blocked.store(true, Ordering::Release);

    assert!(persisted_usage_floor(store).await.is_err());
}

#[tokio::test]
async fn scanner_usage_floor_fails_closed_when_usage_appears_during_missing_confirmation() {
    let store = Arc::new(MemoryConfigStore::default());
    insert_usage_after_first_legacy_backup_read(store.as_ref()).await;

    let err = persisted_usage_floor_for_startup(store, true)
        .await
        .expect_err("an appearing usage snapshot must prevent usage bootstrap");
    assert!(err.to_string().contains("changed while confirming missing state"));
}

#[tokio::test]
async fn scanner_usage_floor_rejects_publication_change_during_missing_confirmation() {
    let store = Arc::new(MemoryConfigStore::default());
    store.block_publication_after_admissions.store(2, Ordering::Release);

    assert!(persisted_usage_floor(store).await.is_err());
}

#[test]
fn missing_usage_floor_discards_unfenced_cycle_progress() {
    let mut cycle = CurrentCycle {
        current: 11,
        next: 12,
        cycle_completed: vec![Utc::now()],
        started: Utc::now(),
    };

    assert_eq!(
        prepare_cycle_for_usage_floor_bootstrap(&mut cycle, PersistedUsageFloor::default(), PersistedUsageFloorStartup::Missing,),
        (true, ScannerCycleResetPolicy::ResetAll)
    );
    assert_eq!(cycle.next, 0);
    assert_eq!(cycle.current, 0);
    assert!(cycle.cycle_completed.is_empty());

    cycle.next = 12;
    assert_eq!(
        prepare_cycle_for_usage_floor_bootstrap(
            &mut cycle,
            PersistedUsageFloor::default(),
            PersistedUsageFloorStartup::BootstrapPending,
        ),
        (true, ScannerCycleResetPolicy::ResetAll)
    );
    assert_eq!(cycle.next, 0);
}

#[test]
fn fenced_usage_bootstrap_retains_partial_cycle_progress() {
    let mut cycle = CurrentCycle {
        next: 12,
        ..Default::default()
    };

    assert_eq!(
        prepare_cycle_for_usage_floor_bootstrap(
            &mut cycle,
            PersistedUsageFloor {
                next_cycle: 0,
                leader_epoch: 7,
            },
            PersistedUsageFloorStartup::BootstrapPending,
        ),
        (true, ScannerCycleResetPolicy::None)
    );
    assert_eq!(cycle.next, 12);

    assert_eq!(
        prepare_cycle_for_usage_floor_bootstrap(
            &mut cycle,
            PersistedUsageFloor {
                next_cycle: 13,
                leader_epoch: 7,
            },
            PersistedUsageFloorStartup::Authoritative,
        ),
        (false, ScannerCycleResetPolicy::None)
    );
    assert_eq!(cycle.next, 12);
}

#[tokio::test]
#[serial]
async fn missing_usage_floor_rebuilds_persisted_cycle_before_leadership_claim() {
    let store = Arc::new(MemoryConfigStore::default());
    let state_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    let stale_cycle = CurrentCycle {
        next: 12,
        ..Default::default()
    };
    store.objects.lock().await.insert(
        state_key.clone(),
        encode_scanner_cycle_state(&stale_cycle, 4).expect("stale cycle state should encode"),
    );
    store.revisions.lock().await.insert(state_key, 7);

    let ScannerCycleStateStartup::Ready {
        cycle: mut cycle_info,
        leader_epoch: mut persisted_epoch,
        revision: mut cycle_revision,
    } = load_scanner_cycle_state_for_startup(store.clone()).await
    else {
        panic!("valid persisted cycle state should load");
    };
    let (usage_floor, startup) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("stably missing usage floor should admit a bootstrap marker");
    assert_eq!(startup, PersistedUsageFloorStartup::Missing);
    let (allow_bootstrap_pending, cycle_reset_policy) =
        prepare_cycle_for_usage_floor_bootstrap(&mut cycle_info, usage_floor, startup);
    apply_persisted_usage_floor(&mut cycle_info, &mut persisted_epoch, usage_floor);
    initialize_usage_baseline_bootstrap(store.clone())
        .await
        .expect("missing usage floor should publish a pending marker");

    assert!(
        claim_scanner_leadership(
            &CancellationToken::new(),
            store.clone(),
            &mut cycle_info,
            &mut cycle_revision,
            &mut persisted_epoch,
            allow_bootstrap_pending,
            cycle_reset_policy,
        )
        .await
    );
    let persisted_cycle = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("rebuilt cycle state should be persisted");
    let (persisted_cycle, persisted_cycle_epoch) =
        decode_scanner_cycle_state(&persisted_cycle).expect("rebuilt cycle state should decode");
    assert_eq!(persisted_cycle.next, 0);
    assert_eq!(persisted_cycle_epoch, 5);

    let pending = read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("fenced bootstrap marker should remain persisted");
    let pending = serde_json::from_slice::<DataUsageInfo>(&pending).expect("bootstrap marker should decode");
    assert!(data_usage_info_is_bootstrap_pending(&pending));
    assert_eq!(pending.scanner_epoch, Some(5));
    assert!(!data_usage_info_has_persisted_baseline_identity(&pending));
}

#[tokio::test]
async fn scanner_usage_bootstrap_allows_first_bucket_to_win_startup() {
    let (_temp_dir, store) = setup_scanner_cycle_store_with_usage_baseline(false).await;

    store
        .make_bucket("first-user-bucket", &crate::storage_api::scan::MakeBucketOptions::default())
        .await
        .expect("test bucket should be created");

    assert_eq!(
        persisted_usage_floor_for_startup(store.clone(), true)
            .await
            .expect("first startup should still admit a non-authoritative bootstrap marker")
            .1,
        PersistedUsageFloorStartup::Missing
    );
    initialize_usage_baseline_bootstrap(store.clone())
        .await
        .expect("first startup should persist its pending marker");
    let pending = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("pending marker should be stored");
    let pending = serde_json::from_slice::<DataUsageInfo>(&pending).expect("pending marker should decode");
    assert!(data_usage_info_is_bootstrap_pending(&pending));
    assert!(!data_usage_info_has_persisted_baseline_identity(&pending));
    assert_eq!(
        persisted_usage_floor_for_startup(store.clone(), false)
            .await
            .expect("the pending marker should be resumable after restart")
            .1,
        PersistedUsageFloorStartup::BootstrapPending
    );

    store
        .delete_bucket("first-user-bucket", &crate::storage_api::scan::DeleteBucketOptions::default())
        .await
        .expect("first user bucket should be deleted");
    assert!(
        read_config(store.clone(), &format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()))
            .await
            .is_err(),
        "bucket deletion must not copy the pending marker into the backup slot"
    );
    assert_eq!(
        persisted_usage_floor_for_startup(store, false)
            .await
            .expect("the pending marker should remain resumable after bucket deletion")
            .1,
        PersistedUsageFloorStartup::BootstrapPending
    );
}

#[tokio::test]
#[serial]
async fn scanner_usage_backup_uses_durable_cycle_cadence_across_tasks() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();

    for cycle in [9, 10] {
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(DataUsageInfo {
                scanner_epoch: Some(1),
                scanner_cycle: Some(cycle),
                last_update: Some(std::time::SystemTime::now()),
                ..complete_usage_with_bucket_count(None, 0)
            })
            .await
            .expect("usage update should queue");
        drop(sender);

        assert_eq!(
            store_data_usage_in_backend_with_outcome(ctx.clone(), store.clone(), receiver).await,
            DataUsagePersistOutcome::Saved
        );
        let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
        let backup = read_config(store.clone(), &backup_path).await;
        if cycle == 9 {
            assert!(matches!(backup, Err(EcstoreError::ConfigNotFound)));
        } else {
            let saved =
                serde_json::from_slice::<DataUsageInfo>(&backup.expect("the tenth durable scanner cycle should create a backup"))
                    .expect("backup usage snapshot should decode");
            assert_eq!(saved.scanner_cycle, Some(10));
            assert_eq!(saved.scanner_epoch, Some(1));
        }
    }
}

#[tokio::test]
async fn scanner_backup_sync_distinguishes_movement_from_missing_or_corrupt_primary() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let primary = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    store.objects.lock().await.insert(
        primary_key.clone(),
        serde_json::to_vec(&primary).expect("primary usage snapshot should encode"),
    );
    store.revisions.lock().await.insert(primary_key.clone(), 1);

    store.publication_admission_blocked.store(true, Ordering::Release);
    let movement_error = sync_data_usage_backup_from_primary(&CancellationToken::new(), store.clone())
        .await
        .expect_err("movement admission loss should fail backup synchronization");
    assert!(scanner_publication_epoch_changed(&movement_error));

    store.publication_admission_blocked.store(false, Ordering::Release);
    store.objects.lock().await.remove(&primary_key);
    store.revisions.lock().await.remove(&primary_key);
    assert!(matches!(
        sync_data_usage_backup_from_primary(&CancellationToken::new(), store.clone()).await,
        Err(EcstoreError::ConfigNotFound)
    ));

    store.objects.lock().await.insert(primary_key.clone(), b"not-json".to_vec());
    store.revisions.lock().await.insert(primary_key, 1);
    let corrupt_error = sync_data_usage_backup_from_primary(&CancellationToken::new(), store)
        .await
        .expect_err("corrupt primary should fail backup synchronization");
    assert!(!scanner_publication_epoch_changed(&corrupt_error));
}

#[async_trait::async_trait]
impl crate::ScannerConfigObjectDelete for MemoryConfigStore {
    async fn delete_config_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> EcstoreResult<ObjectInfo> {
        let key = memory_config_key(bucket, object);
        let mut objects = self.objects.lock().await;
        if !objects.contains_key(&key) {
            return Err(EcstoreError::FileNotFound);
        }
        let mut revisions = self.revisions.lock().await;
        if let Some(expected) = opts
            .http_preconditions
            .as_ref()
            .and_then(|preconditions| preconditions.if_match.as_deref())
        {
            let actual = revisions.get(&key).map(|revision| format!("memory-{revision}"));
            if actual.as_deref() != Some(expected.trim_matches('"')) {
                return Err(EcstoreError::PreconditionFailed);
            }
        }
        objects.remove(&key);
        revisions.remove(&key);
        drop(revisions);
        drop(objects);
        if let Some(token) = self.cancel_after_deletes.lock().await.remove(&key) {
            token.cancel();
        }
        if self.error_after_commit_deletes.lock().await.remove(&key) {
            return Err(EcstoreError::other("injected delete error after commit"));
        }
        Ok(ObjectInfo::default())
    }

    async fn scanner_data_usage_publication_admission(&self) -> Option<crate::ScannerDataUsagePublicationAdmission> {
        let pause = self.pause_next_publication_admission.lock().await.take();
        if let Some((entered, resume)) = pause {
            entered.notify_one();
            resume.notified().await;
        }
        if self.publication_admission_blocked.load(Ordering::Acquire) {
            return None;
        }
        if self
            .block_publication_after_admissions
            .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| remaining.checked_sub(1))
            == Ok(1)
        {
            self.publication_admission_blocked.store(true, Ordering::Release);
        }
        Some(crate::ScannerDataUsagePublicationAdmission::unfenced())
    }
}

#[test]
fn scanner_cycle_advance_fails_before_reserved_exhausted_value() {
    let mut cycle = CurrentCycle {
        next: u64::MAX - 2,
        ..Default::default()
    };
    advance_scanner_cycle(&mut cycle).expect("last persistable scanner cycle should remain valid");
    assert_eq!(cycle.next, u64::MAX - 1);
    assert!(advance_scanner_cycle(&mut cycle).is_err());
    assert_eq!(cycle.next, u64::MAX - 1);
}

#[tokio::test]
#[serial]
async fn test_finalize_partial_scan_cycle_reports_persist_failure() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    store.fail_put_number.lock().await.insert(key, 1);
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle_info = CurrentCycle {
        current: 12,
        next: 12,
        cycle_completed: vec![],
        started: Utc::now(),
    };
    let mut cycle_metrics_guard = ScannerCycleMetricsGuard::new(cycle_info.clone()).await;

    assert!(!finalize_partial_scan_cycle(&ctx, store, &mut cycle_info, &mut revision, 1, &mut cycle_metrics_guard,).await);
    assert_eq!(cycle_info.next, 13);
    assert_eq!(cycle_info.current, 0);
    assert_eq!(revision, DataUsageCacheRevision::Missing);

    global_metrics().set_cycle(None).await;
}

#[tokio::test]
#[serial]
async fn test_persist_scanner_cycle_state_reconciles_newer_winner() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut initial_revision = DataUsageCacheRevision::Missing;
    let mut initial = CurrentCycle {
        current: 0,
        next: 12,
        cycle_completed: vec![],
        started: Utc::now(),
    };
    assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut initial, &mut initial_revision, 1).await);

    let mut current_revision = initial_revision.clone();
    let mut stale_revision = initial_revision;
    let mut current = CurrentCycle {
        next: 14,
        ..initial.clone()
    };
    let mut stale = CurrentCycle { next: 13, ..initial };

    assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut current, &mut current_revision, 1).await);
    assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut stale, &mut stale_revision, 1).await);

    let buf = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("new leader cycle state should remain persisted");
    let (decoded, epoch) = decode_scanner_cycle_state(&buf).expect("persisted cycle state should decode");
    assert_eq!(decoded.next, 14);
    assert_eq!(epoch, 1);
    assert_eq!(stale.next, 14);
    assert!(matches!(current_revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-2"));
    assert!(matches!(stale_revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-2"));

    global_metrics().set_cycle(None).await;
}

#[tokio::test]
async fn test_persist_scanner_cycle_state_retries_after_stale_winner() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut initial_revision = DataUsageCacheRevision::Missing;
    let mut initial = CurrentCycle {
        current: 0,
        next: 12,
        cycle_completed: vec![],
        started: Utc::now(),
    };
    assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut initial, &mut initial_revision, 1).await);

    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    let stale = CurrentCycle {
        next: 13,
        ..initial.clone()
    };
    let stale_buf = encode_scanner_cycle_state(&stale, 1).expect("stale cycle state should encode");
    store.interleaving_puts.lock().await.insert(key, (2, stale_buf));

    let mut current = CurrentCycle { next: 14, ..initial };
    assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut current, &mut initial_revision, 1).await);

    let buf = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("newer cycle state should replace the stale conflict winner");
    let (decoded, epoch) = decode_scanner_cycle_state(&buf).expect("persisted cycle state should decode");
    assert_eq!(decoded.next, 14);
    assert_eq!(epoch, 1);
    assert_eq!(current.next, 14);
    assert!(matches!(initial_revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-3"));
}

#[tokio::test]
async fn test_persist_scanner_cycle_state_stops_retry_after_leader_fence() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut initial = CurrentCycle {
        current: 0,
        next: 12,
        cycle_completed: vec![],
        started: Utc::now(),
    };
    assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut initial, &mut revision, 1).await);

    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    let replacement = CurrentCycle {
        next: 13,
        ..initial.clone()
    };
    let replacement_buf = encode_scanner_cycle_state(&replacement, 2).expect("replacement cycle state should encode");
    store.interleaving_puts.lock().await.insert(key.clone(), (2, replacement_buf));
    store
        .cancel_after_interleaving_puts
        .lock()
        .await
        .insert(key.clone(), ctx.clone());

    let mut stale_leader = CurrentCycle { next: 14, ..initial };
    assert!(!persist_scanner_cycle_state(&ctx, store.clone(), &mut stale_leader, &mut revision, 1).await);

    let buf = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("replacement leader cycle state should remain persisted");
    let (decoded, epoch) = decode_scanner_cycle_state(&buf).expect("persisted cycle state should decode");
    assert_eq!(decoded.next, 13);
    assert_eq!(epoch, 2);
    assert_eq!(stale_leader.next, 14);
    assert!(matches!(revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-2"));
    assert_eq!(store.put_counts.lock().await.get(&key), Some(&2));
}

#[tokio::test]
async fn test_leadership_claim_preserves_usage_epoch_floor_across_old_epoch_conflict() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        current: 0,
        next: 12,
        cycle_completed: vec![],
        started: Utc::now(),
    };
    assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 1).await);
    seed_usage_snapshot_for_leadership_claim(&store).await;

    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    let old_epoch_commit = CurrentCycle {
        next: 14,
        ..cycle.clone()
    };
    store.interleaving_puts.lock().await.insert(
        key.clone(),
        (
            2,
            encode_scanner_cycle_state(&old_epoch_commit, 1).expect("old-epoch cycle state should encode"),
        ),
    );

    let mut persisted_epoch = 8;
    assert!(
        claim_scanner_leadership(
            &ctx,
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut persisted_epoch,
            false,
            ScannerCycleResetPolicy::None,
        )
        .await
    );

    let state = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("new leadership claim should remain persisted");
    let (claimed_cycle, claimed_epoch) = decode_scanner_cycle_state(&state).expect("claimed cycle state should decode");
    assert_eq!(claimed_cycle.next, 14);
    assert_eq!(claimed_epoch, 9);
    assert_eq!(persisted_epoch, 9);
    assert_eq!(store.put_counts.lock().await.get(&key), Some(&3));
}

#[tokio::test]
async fn unfenced_usage_bootstrap_discards_old_epoch_conflict_progress() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        next: 12,
        ..Default::default()
    };
    assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 1).await);
    initialize_usage_baseline_bootstrap(store.clone())
        .await
        .expect("missing usage floor should publish a pending marker");

    cycle = CurrentCycle::default();
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    let stale_cycle = CurrentCycle {
        next: 14,
        ..Default::default()
    };
    store.interleaving_puts.lock().await.insert(
        key,
        (2, encode_scanner_cycle_state(&stale_cycle, 1).expect("stale cycle state should encode")),
    );

    let mut persisted_epoch = 1;
    assert!(
        claim_scanner_leadership(
            &ctx,
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut persisted_epoch,
            true,
            ScannerCycleResetPolicy::ResetAll,
        )
        .await
    );

    let state = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("rebuilt leadership claim should remain persisted");
    let (claimed_cycle, claimed_epoch) = decode_scanner_cycle_state(&state).expect("claimed cycle state should decode");
    assert_eq!(claimed_cycle.next, 0);
    assert_eq!(claimed_epoch, 2);
}

#[tokio::test]
async fn recovered_usage_bootstrap_claim_conflicts_preserve_the_highest_cycle_number() {
    for winner_next in [42_u64, 20_000] {
        let store = Arc::new(MemoryConfigStore::default());
        let ctx = CancellationToken::new();
        let mut revision = DataUsageCacheRevision::Missing;
        let mut cycle = CurrentCycle {
            next: 12,
            ..Default::default()
        };
        assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 1).await);
        seed_usage_snapshot_for_leadership_claim(&store).await;

        cycle = CurrentCycle {
            current: 17_117,
            next: 17_118,
            cycle_completed: vec![Utc::now()],
            started: Utc::now(),
        };
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
        let winner = CurrentCycle {
            current: winner_next.saturating_sub(1),
            next: winner_next,
            cycle_completed: vec![Utc::now()],
            started: Utc::now(),
        };
        store
            .interleaving_puts
            .lock()
            .await
            .insert(key, (2, encode_scanner_cycle_state(&winner, 7).expect("conflict winner should encode")));

        let mut persisted_epoch = 7;
        assert!(
            claim_scanner_leadership(
                &ctx,
                store.clone(),
                &mut cycle,
                &mut revision,
                &mut persisted_epoch,
                true,
                ScannerCycleResetPolicy::ResetCoveragePreservingNext,
            )
            .await
        );

        let persisted = read_config(store, DATA_USAGE_BLOOM_NAME_PATH.as_str())
            .await
            .expect("recovered leadership claim should remain durable");
        let (persisted_cycle, claimed_epoch) =
            decode_scanner_cycle_state(&persisted).expect("recovered leadership claim should decode");
        assert_eq!(persisted_cycle.next, 17_118_u64.max(winner_next));
        assert_eq!(persisted_cycle.current, 0);
        assert!(persisted_cycle.cycle_completed.is_empty());
        assert_eq!(claimed_epoch, 8);
    }
}

#[test]
fn recovered_usage_cache_reset_keeps_cycle_and_leader_regression_guards() {
    let source = DataUsageCacheSource::new(0, 0);
    let digest = DataUsageScanPlanDigest([9; 32]);
    let mut newer_cycle = DataUsageCache::default();
    newer_cycle.info.next_cycle = 17_119;
    assert_eq!(
        newer_cycle.prepare_for_scan(DATA_USAGE_ROOT, 17_118, 8, source, digest, true),
        DataUsageCachePrepareOutcome::RejectedNewerCycle
    );

    let mut newer_leader = DataUsageCache::default();
    newer_leader.info.next_cycle = 17_118;
    newer_leader.info.leader_epoch = 9;
    assert_eq!(
        newer_leader.prepare_for_scan(DATA_USAGE_ROOT, 17_118, 8, source, digest, true),
        DataUsageCachePrepareOutcome::RejectedNewerLeader
    );
}

#[tokio::test]
async fn test_leadership_claim_rejects_terminal_epoch() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        next: 12,
        ..Default::default()
    };
    let mut persisted_epoch = u64::MAX - 1;

    assert!(
        !claim_scanner_leadership(
            &ctx,
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut persisted_epoch,
            false,
            ScannerCycleResetPolicy::None,
        )
        .await
    );
    assert_eq!(persisted_epoch, u64::MAX - 1);
    assert!(read_config(store, &DATA_USAGE_BLOOM_NAME_PATH).await.is_err());
}

#[tokio::test]
async fn scanner_defers_leadership_when_usage_snapshots_are_stably_absent() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        next: 12,
        ..Default::default()
    };
    let mut persisted_epoch = 0;

    assert!(
        !claim_scanner_leadership(
            &ctx,
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut persisted_epoch,
            false,
            ScannerCycleResetPolicy::None,
        )
        .await
    );
    assert!(read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH).await.is_err());
    assert!(read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str()).await.is_err());
}

#[tokio::test]
async fn scanner_usage_floor_leadership_claim_recovers_legacy_backup_after_primary_decode_error() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    seed_legacy_primary_read_error_with_backup(&store, EcstoreError::other("InlineData value out of range"), 19, 41).await;

    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle::default();
    let mut persisted_epoch = 19;
    assert!(
        claim_scanner_leadership(
            &ctx,
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut persisted_epoch,
            false,
            ScannerCycleResetPolicy::None,
        )
        .await
    );

    let state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("leadership claim should persist after legacy backup recovery");
    let (_, claimed_epoch) = decode_scanner_cycle_state(&state).expect("leadership claim should decode");
    assert_eq!(claimed_epoch, 20);
    assert_eq!(persisted_epoch, 20);

    let usage = read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("legacy backup recovery should publish a fenced v2 usage primary");
    let usage = serde_json::from_slice::<DataUsageInfo>(&usage).expect("fenced v2 usage primary should decode");
    assert_eq!(usage.scanner_epoch, Some(20));
    assert_eq!(usage.scanner_cycle, Some(41));
}

#[tokio::test]
#[serial_test::serial]
async fn scanner_legacy_usage_backup_survives_fencing_and_restart_after_real_metadata_truncation() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let (temp_dir, store) = setup_scanner_cycle_store_with_usage_baseline(false).await;
    let mut usage = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    usage.usage_snapshot_complete = false;
    usage.scanner_cycle = Some(41);
    let mut data = serde_json::to_vec(&usage).expect("legacy usage should encode");
    data.resize(data.len() + 16 * 1024, b' ');
    let legacy_path = LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str();
    let backup_path = format!("{legacy_path}.bkp");
    for path in [legacy_path, backup_path.as_str()] {
        save_config(store.clone(), path, data.clone())
            .await
            .expect("legacy usage fixture should persist");
    }
    let mut truncated_files = Vec::new();
    for disk_index in 0..4 {
        let path = temp_dir
            .path()
            .join(format!("pool0/disk{disk_index}"))
            .join(RUSTFS_META_BUCKET)
            .join(legacy_path)
            .join("xl.meta");
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .await
            .expect("legacy inline metadata should exist");
        assert!(file.metadata().await.expect("metadata should be readable").len() > 4096);
        file.set_len(4096).await.expect("fixture should truncate at a page boundary");
        truncated_files.push((
            path.clone(),
            tokio::fs::read(&path)
                .await
                .expect("truncated evidence should remain readable"),
        ));
    }

    let store = restart_scanner_cycle_store_from(&store).await;
    let error = read_config_with_revision(store.clone(), legacy_path)
        .await
        .expect_err("truncated primary must fail in the real object reader");
    assert!(
        error.to_string().contains("InlineData value out of range"),
        "unexpected truncated-primary error: {error}"
    );
    assert_eq!(
        read_config_with_revision(store.clone(), &backup_path)
            .await
            .expect("backup should remain readable")
            .0,
        Some(data.clone()),
    );
    let (floor, state) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("intact legacy backup must recover startup despite truncated primary");
    assert_eq!(
        floor,
        PersistedUsageFloor {
            next_cycle: 42,
            leader_epoch: 0
        }
    );
    assert_eq!(state, PersistedUsageFloorStartup::Authoritative);

    let baseline = read_data_usage_persist_baseline(store.clone())
        .await
        .expect("publication must also read the intact backup");
    assert_eq!(baseline.data.as_deref(), Some(data.as_slice()));
    assert_eq!(baseline.revision, DataUsageCacheRevision::Missing);
    fence_scanner_usage_epoch_with_expected_epoch(&CancellationToken::new(), store.clone(), 7, None, false, || true)
        .await
        .expect("legacy backup must be fenced into v2");
    let fenced = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("fencing must publish a v2 usage primary");
    let fenced = serde_json::from_slice::<DataUsageInfo>(&fenced).expect("fenced v2 usage primary should decode");
    assert!(
        fenced.usage_snapshot_complete,
        "the fenced pre-marker baseline must become a complete v2 identity"
    );

    let store = restart_scanner_cycle_store_from(&store).await;
    let (floor, state) = persisted_usage_floor_for_startup(store.clone(), true)
        .await
        .expect("a restart after fencing must preserve the recovered floor");
    assert_eq!(
        floor,
        PersistedUsageFloor {
            next_cycle: 42,
            leader_epoch: 7
        }
    );
    assert_eq!(state, PersistedUsageFloorStartup::Authoritative);
    let restarted = restart_scanner_cycle_store_from(&store).await;
    assert_eq!(
        persisted_usage_floor(restarted)
            .await
            .expect("fenced floor must survive another restart"),
        PersistedUsageFloor {
            next_cycle: 42,
            leader_epoch: 7
        }
    );
    for (path, bytes) in truncated_files {
        assert_eq!(tokio::fs::read(path).await.expect("legacy evidence must not be removed"), bytes);
    }
    assert_eq!(
        read_config(store, &backup_path)
            .await
            .expect("legacy backup must remain intact"),
        data
    );
    global_metrics().set_cycle(None).await;
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
async fn usage_bootstrap_pending_unblocks_first_leadership_claim() {
    let store = Arc::new(MemoryConfigStore::default());
    initialize_usage_baseline_bootstrap(store.clone())
        .await
        .expect("verified missing state should publish its pending marker");

    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle::default();
    let mut persisted_epoch = 0;
    assert!(
        !claim_scanner_leadership(
            &ctx,
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut persisted_epoch,
            false,
            ScannerCycleResetPolicy::None,
        )
        .await
    );
    assert!(read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH).await.is_err());
    assert!(
        claim_scanner_leadership(
            &ctx,
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut persisted_epoch,
            true,
            ScannerCycleResetPolicy::ResetAll,
        )
        .await
    );

    let usage = read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("leadership claim should fence the bootstrap marker");
    let usage = serde_json::from_slice::<DataUsageInfo>(&usage).expect("bootstrap marker should remain valid");
    assert!(data_usage_info_is_bootstrap_pending(&usage));
    assert!(!data_usage_info_has_persisted_baseline_identity(&usage));
    assert_eq!(usage.scanner_epoch, Some(1));
}

#[tokio::test]
async fn existing_usage_bootstrap_is_resumed_after_restart() {
    let store = Arc::new(MemoryConfigStore::default());
    initialize_usage_baseline_bootstrap(store.clone())
        .await
        .expect("verified missing state should publish its pending marker");

    let (floor, state) = persisted_usage_floor_for_startup(store.clone(), false)
        .await
        .expect("restart should recognize the pending usage bootstrap");
    assert_eq!(floor, PersistedUsageFloor::default());
    assert_eq!(state, PersistedUsageFloorStartup::BootstrapPending);
    assert!(persisted_usage_floor(store).await.is_err());
}

#[tokio::test]
async fn usage_bootstrap_reconciles_post_commit_error() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    store.error_after_commit_put_number.lock().await.insert(key, 1);

    initialize_usage_baseline_bootstrap(store.clone())
        .await
        .expect("a committed pending marker should reconcile after a lost response");
    let usage = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("the reconciled pending marker should remain");
    let usage = serde_json::from_slice::<DataUsageInfo>(&usage).expect("pending marker should decode");
    assert!(data_usage_info_is_bootstrap_pending(&usage));
    assert!(!data_usage_info_has_persisted_baseline_identity(&usage));
    assert_eq!(
        persisted_usage_floor_for_startup(store.clone(), false)
            .await
            .expect("restart should resume a committed pending marker")
            .1,
        PersistedUsageFloorStartup::BootstrapPending
    );
    assert!(persisted_usage_floor(store).await.is_err());
}

#[tokio::test]
async fn usage_bootstrap_does_not_overwrite_concurrent_replacement() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let replacement = serde_json::to_vec(&complete_usage_with_bucket_count(None, 1)).expect("replacement should encode");
    store
        .replace_after_successful_puts
        .lock()
        .await
        .insert(key, (1, replacement.clone()));

    initialize_usage_baseline_bootstrap(store.clone())
        .await
        .expect("the bootstrap write completed before the replacement");
    assert_eq!(
        read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("newer usage snapshot must remain"),
        replacement
    );
}

#[tokio::test]
#[serial]
async fn scanner_usage_state_reset_publishes_fenced_bootstrap_marker() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    let quota_ledger_path = "config/quota-ledger/reserved-bucket.json";
    let quota_ledger = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "bucket_incarnation": "00000000-0000-0000-0000-000000000001",
        "quota_revision_unix_nanos": 1,
        "accounted_usage": 100,
        "reservations": {
            "00000000-0000-0000-0000-000000000002": {
                "object": "pending-object",
                "old_size": 0,
                "new_size": 64,
                "created_at": 1,
                "pool_index": 0,
                "set_index": 0,
                "commit_started": true
            }
        }
    }))
    .expect("quota ledger fixture should encode");
    save_config(store.clone(), quota_ledger_path, quota_ledger.clone())
        .await
        .expect("independent quota reservations should persist");
    let cycle = CurrentCycle {
        current: 41,
        next: 42,
        cycle_completed: vec![Utc::now()],
        started: Utc::now(),
    };
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_NAME_PATH.as_str(),
        encode_scanner_cycle_state(&cycle, 7).expect("cycle state should encode"),
    )
    .await
    .expect("cycle state should persist");

    let usage_backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let legacy_backup_path = format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str());
    for (path, epoch, cycle) in [
        (usage_backup_path.as_str(), 6, 40),
        (LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str(), 5, 39),
        (legacy_backup_path.as_str(), 4, 38),
        (DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(), 8, 41),
    ] {
        let mut usage = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
        usage.scanner_epoch = Some(epoch);
        usage.scanner_cycle = Some(cycle);
        save_config(store.clone(), path, serde_json::to_vec(&usage).expect("usage slot should encode"))
            .await
            .expect("usage slot should persist");
    }

    let result = reset_scanner_usage_state_for_full_rebuild(CancellationToken::new(), store.clone())
        .await
        .expect("usage state reset should publish a fenced bootstrap marker");

    assert_eq!(result.status, "reset");
    assert_eq!(result.mode, "full-rebuild");
    assert_eq!(result.usage_state, "bootstrap-pending");
    assert_eq!(result.leader_epoch, 9);
    assert_eq!(result.next_cycle, 42);
    assert_eq!(result.reset_paths.len(), 5);

    let cycle_state = read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("reset cycle state should remain");
    let (reset_cycle, reset_epoch) = decode_scanner_cycle_state(&cycle_state).expect("reset cycle state should decode");
    assert_eq!(reset_cycle.next, 42);
    assert_eq!(reset_cycle.current, 0);
    assert!(reset_cycle.cycle_completed.is_empty());
    assert_eq!(reset_epoch, 9);

    let usage = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("reset usage marker should remain");
    let usage = serde_json::from_slice::<DataUsageInfo>(&usage).expect("reset usage marker should decode");
    assert!(data_usage_info_is_bootstrap_pending(&usage));
    assert!(!data_usage_info_has_persisted_baseline_identity(&usage));
    assert_eq!(usage.scanner_epoch, Some(9));

    assert_eq!(
        read_config(store.clone(), quota_ledger_path)
            .await
            .expect("quota ledger must remain readable after scanner reset"),
        quota_ledger,
        "scanner reset must preserve incarnation and outstanding reserved bytes exactly"
    );

    for path in [
        usage_backup_path.as_str(),
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str(),
        legacy_backup_path.as_str(),
        DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
    ] {
        assert!(
            matches!(read_config(store.clone(), path).await, Err(EcstoreError::ConfigNotFound)),
            "reset should remove stale usage slot {path}"
        );
    }

    let cycle_before_retry = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("cycle should remain before retry");
    let marker_before_retry = read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("bootstrap should remain before retry");
    let retry = reset_scanner_usage_state_for_full_rebuild(CancellationToken::new(), store.clone())
        .await
        .expect("completed cleanup should be reentrant");
    assert_eq!(retry.leader_epoch, result.leader_epoch);
    assert_eq!(
        read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
            .await
            .expect("cycle should remain"),
        cycle_before_retry
    );
    assert_eq!(
        read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("bootstrap should remain"),
        marker_before_retry
    );
    let (floor, state) = persisted_usage_floor_for_startup(store, false)
        .await
        .expect("reset marker should be resumable");
    assert_eq!(floor.leader_epoch, 9);
    assert_eq!(state, PersistedUsageFloorStartup::BootstrapPending);
}

#[tokio::test]
async fn scanner_usage_state_reset_bootstrap_survives_stale_cleanup_slots_after_restart() {
    let store = Arc::new(MemoryConfigStore::default());
    let mut marker = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH),
        scanner_epoch: Some(9),
        usage_snapshot_converged: Some(false),
        usage_snapshot_bootstrap_pending: true,
        ..Default::default()
    };
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&marker).expect("usage reset marker should encode"),
    )
    .await
    .expect("usage reset marker should persist");
    save_config(
        store.clone(),
        format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()).as_str(),
        b"{not-json".to_vec(),
    )
    .await
    .expect("stale malformed backup should persist");

    marker.usage_snapshot_bootstrap_pending = false;
    marker.usage_snapshot_complete = true;
    marker.scanner_epoch = Some(8);
    marker.scanner_cycle = Some(41);
    for path in [
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
        format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
    ] {
        save_config(
            store.clone(),
            &path,
            serde_json::to_vec(&marker).expect("stale legacy usage should encode"),
        )
        .await
        .expect("stale legacy usage should persist");
    }

    let (floor, state) = persisted_usage_floor_for_startup(store.clone(), false)
        .await
        .expect("restart should resume reset bootstrap while stale cleanup slots remain");
    assert_eq!(
        floor,
        PersistedUsageFloor {
            next_cycle: 0,
            leader_epoch: 9,
        }
    );
    assert_eq!(state, PersistedUsageFloorStartup::BootstrapPending);
    assert!(
        persisted_usage_floor(store).await.is_err(),
        "bootstrap marker must still not become an authoritative usage floor"
    );
}

#[tokio::test]
async fn scanner_usage_state_reset_bootstrap_survives_malformed_legacy_primary_after_restart() {
    let store = Arc::new(MemoryConfigStore::default());
    let marker = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH),
        scanner_epoch: Some(9),
        usage_snapshot_converged: Some(false),
        usage_snapshot_bootstrap_pending: true,
        ..Default::default()
    };
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&marker).expect("usage reset marker should encode"),
    )
    .await
    .expect("usage reset marker should persist");
    save_config(store.clone(), LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str(), b"{not-json".to_vec())
        .await
        .expect("stale malformed legacy primary should persist");

    let (floor, state) = persisted_usage_floor_for_startup(store.clone(), false)
        .await
        .expect("restart should resume reset bootstrap when only stale malformed legacy primary remains");
    assert_eq!(
        floor,
        PersistedUsageFloor {
            next_cycle: 0,
            leader_epoch: 9,
        }
    );
    assert_eq!(state, PersistedUsageFloorStartup::BootstrapPending);
    assert!(persisted_usage_floor(store).await.is_err());
}

#[tokio::test]
async fn scanner_usage_state_reset_bootstrap_does_not_mask_newer_legacy_backup() {
    let store = Arc::new(MemoryConfigStore::default());
    let marker = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH),
        scanner_epoch: Some(9),
        usage_snapshot_converged: Some(false),
        usage_snapshot_bootstrap_pending: true,
        ..Default::default()
    };
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&marker).expect("usage reset marker should encode"),
    )
    .await
    .expect("usage reset marker should persist");
    save_config(store.clone(), LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str(), b"{not-json".to_vec())
        .await
        .expect("malformed legacy primary should persist");

    let mut newer_backup = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    newer_backup.scanner_epoch = Some(10);
    newer_backup.scanner_cycle = Some(43);
    save_config(
        store.clone(),
        format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()).as_str(),
        serde_json::to_vec(&newer_backup).expect("newer legacy backup should encode"),
    )
    .await
    .expect("newer legacy backup should persist");

    let err = persisted_usage_floor_for_startup(store, false)
        .await
        .expect_err("newer legacy backup must not be hidden by an older bootstrap marker");
    assert!(
        err.to_string()
            .contains("scanner usage bootstrap conflicts with a persisted backup"),
        "unexpected conflict error: {err}"
    );
}

#[tokio::test]
async fn scanner_usage_state_reset_slots_reject_primary_aba() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    store.objects.lock().await.insert(key.clone(), b"not-json".to_vec());
    store.revisions.lock().await.insert(key.clone(), 1);
    let slots = read_usage_state_reset_slots(store.clone())
        .await
        .expect("usage reset slots should be inspected");

    store.objects.lock().await.insert(key.clone(), b"newer-json".to_vec());
    store.revisions.lock().await.insert(key, 2);
    let err = reset_scanner_usage_state_slots_for_full_rebuild(store, &slots, 0, 3, || true)
        .await
        .expect_err("stale primary revision must not be overwritten");
    assert!(
        err.to_string()
            .contains("scanner usage reset primary slot changed before bootstrap publish"),
        "unexpected primary ABA error: {err}"
    );
}

#[tokio::test]
async fn scanner_usage_state_reset_resumes_every_cleanup_boundary_without_rewriting_intent() {
    for completed in 0..=4 {
        let store = Arc::new(MemoryConfigStore::default());
        let primary_path = DATA_USAGE_OBJ_NAME_PATH.as_str();
        let cleanup_paths = [
            format!("{primary_path}.bkp"),
            LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
            format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
            DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str().to_string(),
        ];
        for path in std::iter::once(primary_path).chain(cleanup_paths.iter().map(String::as_str)) {
            let mut usage = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
            usage.scanner_epoch = Some(1);
            save_config(store.clone(), path, serde_json::to_vec(&usage).expect("fixture should encode"))
                .await
                .expect("fixture should persist");
        }
        // These objects belong to other owners, even when reset cleanup resumes.
        for path in ["buckets/quota-reservations/ledger", "buckets/example/incarnation"] {
            save_config(store.clone(), path, b"retain".to_vec())
                .await
                .expect("unrelated state should persist");
        }
        let slots = read_usage_state_reset_slots(store.clone()).await.expect("slots should load");
        let cancelled = CancellationToken::new();
        if completed == 0 {
            store
                .cancel_after_successful_puts
                .lock()
                .await
                .insert(memory_config_key(RUSTFS_META_BUCKET, primary_path), (2, cancelled.clone()));
        } else {
            store
                .cancel_after_deletes
                .lock()
                .await
                .insert(memory_config_key(RUSTFS_META_BUCKET, &cleanup_paths[completed - 1]), cancelled.clone());
        }
        let err = reset_scanner_usage_state_slots_for_full_rebuild(store.clone(), &slots, 0, 3, || !cancelled.is_cancelled())
            .await
            .expect_err("interruption should stop cleanup");
        assert!(err.to_string().contains("ownership"), "boundary {completed}: {err}");
        for (index, path) in cleanup_paths.iter().enumerate() {
            assert_eq!(
                store
                    .objects
                    .lock()
                    .await
                    .contains_key(&memory_config_key(RUSTFS_META_BUCKET, path)),
                index >= completed,
                "boundary {completed}, slot {index}"
            );
        }
        let intent = read_config_with_revision(store.clone(), primary_path)
            .await
            .expect("intent should persist");
        let slots = read_usage_state_reset_slots(store.clone())
            .await
            .expect("restart should reload slots");
        reset_scanner_usage_state_slots_for_full_rebuild(store.clone(), &slots, 0, 3, || true)
            .await
            .expect("restart should complete the same intent");
        assert_eq!(
            read_config_with_revision(store.clone(), primary_path)
                .await
                .expect("intent should remain"),
            intent
        );
        assert_eq!(store.put_counts.lock().await[&memory_config_key(RUSTFS_META_BUCKET, primary_path)], 2);
        for path in cleanup_paths {
            assert!(
                !store
                    .objects
                    .lock()
                    .await
                    .contains_key(&memory_config_key(RUSTFS_META_BUCKET, &path))
            );
        }
        for path in ["buckets/quota-reservations/ledger", "buckets/example/incarnation"] {
            assert_eq!(read_config(store.clone(), path).await.expect("unrelated state should remain"), b"retain");
        }
    }
}

#[tokio::test]
#[serial]
async fn scanner_usage_state_reset_resumes_real_store_cleanup_boundaries_after_reopen() {
    let primary_path = DATA_USAGE_OBJ_NAME_PATH.as_str();
    let cleanup_paths = [
        format!("{primary_path}.bkp"),
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str().to_string(),
        format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
        DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str().to_string(),
    ];

    for completed in 0..=cleanup_paths.len() {
        let (_temp_dir, store) = setup_scanner_cycle_store().await;
        let cycle = CurrentCycle {
            current: 12,
            next: 42,
            cycle_completed: vec![Utc::now()],
            started: Utc::now(),
        };
        save_config(
            store.clone(),
            DATA_USAGE_BLOOM_NAME_PATH.as_str(),
            encode_scanner_cycle_state(&cycle, 3).expect("cycle state should encode"),
        )
        .await
        .expect("cycle state should persist");
        let marker = scanner_usage_bootstrap_marker(std::time::SystemTime::UNIX_EPOCH, Some(3));
        save_config(
            store.clone(),
            primary_path,
            serde_json::to_vec(&marker).expect("usage reset marker should encode"),
        )
        .await
        .expect("usage reset marker should persist");

        for path in cleanup_paths.iter().skip(completed) {
            let mut usage = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
            usage.scanner_epoch = Some(1);
            usage.scanner_cycle = Some(12);
            save_config(store.clone(), path, serde_json::to_vec(&usage).expect("cleanup slot should encode"))
                .await
                .expect("cleanup slot should persist");
        }
        for path in ["buckets/quota-reservations/ledger", "buckets/example/incarnation"] {
            save_config(store.clone(), path, b"retain".to_vec())
                .await
                .expect("unrelated state should persist before reopen");
        }

        let restarted = restart_scanner_cycle_store_from(&store).await;
        let intent_before = read_config_with_revision(restarted.clone(), primary_path)
            .await
            .expect("reopened reset intent should be readable");

        let result = reset_scanner_usage_state_for_full_rebuild(CancellationToken::new(), restarted.clone())
            .await
            .expect("reopened usage reset should complete");

        assert_eq!(result.leader_epoch, 3, "boundary {completed}");
        assert_eq!(result.next_cycle, 42, "boundary {completed}");
        assert_eq!(result.reset_paths.len(), cleanup_paths.len() + 1 - completed, "boundary {completed}");
        assert_eq!(
            read_config_with_revision(restarted.clone(), primary_path)
                .await
                .expect("completed reset intent should remain readable"),
            intent_before,
            "boundary {completed}: resumed cleanup must not rewrite the reset intent"
        );

        let (floor, state) = persisted_usage_floor_for_startup(restarted.clone(), false)
            .await
            .expect("completed reset marker should remain resumable");
        assert_eq!(floor.leader_epoch, 3, "boundary {completed}");
        assert_eq!(state, PersistedUsageFloorStartup::BootstrapPending, "boundary {completed}");
        assert!(
            persisted_usage_floor(restarted.clone()).await.is_err(),
            "boundary {completed}: bootstrap marker must not become an authoritative floor"
        );

        for path in &cleanup_paths {
            assert!(
                matches!(read_config(restarted.clone(), path).await, Err(EcstoreError::ConfigNotFound)),
                "boundary {completed}: reset should remove stale usage slot {path}"
            );
        }
        for path in ["buckets/quota-reservations/ledger", "buckets/example/incarnation"] {
            assert_eq!(
                read_config(restarted.clone(), path)
                    .await
                    .expect("unrelated state should survive reopened reset"),
                b"retain",
                "boundary {completed}: reset must preserve non-scanner-state config"
            );
        }
    }
}

#[tokio::test]
async fn scanner_usage_state_reset_stops_usage_fence_after_owner_loss() {
    let store = Arc::new(MemoryConfigStore::default());
    let mut usage = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    usage.scanner_epoch = Some(1);
    let bytes = serde_json::to_vec(&usage).expect("baseline should encode");
    save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), bytes.clone())
        .await
        .expect("baseline should persist");
    let checks = AtomicUsize::new(0);
    let err = fence_scanner_usage_epoch_with_expected_epoch(&CancellationToken::new(), store.clone(), 3, Some(0), false, || {
        checks.fetch_add(1, Ordering::SeqCst) == 0
    })
    .await
    .expect_err("ownership lost during reads must prevent the write");
    assert!(err.to_string().contains("leadership was lost"), "{err}");
    assert_eq!(
        read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("baseline should remain"),
        bytes
    );
}

#[tokio::test]
async fn scanner_usage_state_reset_cancels_during_publication_admission() {
    for resuming in [false, true] {
        let store = Arc::new(MemoryConfigStore::default());
        let usage = if resuming {
            scanner_usage_bootstrap_marker(std::time::SystemTime::UNIX_EPOCH, Some(3))
        } else {
            complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0)
        };
        save_config(
            store.clone(),
            DATA_USAGE_OBJ_NAME_PATH.as_str(),
            serde_json::to_vec(&usage).expect("primary should encode"),
        )
        .await
        .expect("primary should persist");
        save_config(store.clone(), LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str(), b"corrupt".to_vec())
            .await
            .expect("cleanup target should persist");
        let slots = read_usage_state_reset_slots(store.clone()).await.expect("slots should load");
        let before = store.objects.lock().await.clone();
        let revisions_before = store.revisions.lock().await.clone();
        let entered = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        *store.pause_next_publication_admission.lock().await = Some((entered.clone(), resume.clone()));
        let cancelled = CancellationToken::new();
        let (result, ()) = tokio::join!(
            reset_scanner_usage_state_slots_for_full_rebuild(store.clone(), &slots, 0, 3, || !cancelled.is_cancelled()),
            async {
                entered.notified().await;
                cancelled.cancel();
                resume.notify_one();
            }
        );
        let err = result.expect_err("losing ownership during admission must prevent mutation");
        assert!(err.to_string().contains("ownership was lost"), "resuming={resuming}: {err}");
        assert_eq!(*store.objects.lock().await, before);
        assert_eq!(*store.revisions.lock().await, revisions_before);
    }
}

#[tokio::test]
#[serial]
async fn scanner_usage_state_reset_rejects_corruption_without_a_trusted_floor() {
    let (_temp_dir, store) = setup_scanner_cycle_store_with_usage_baseline(false).await;
    save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), b"{corrupt".to_vec())
        .await
        .expect("corrupt primary should persist");
    let before = read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("evidence should load");
    let err = reset_scanner_usage_state_for_full_rebuild(CancellationToken::new(), store.clone())
        .await
        .expect_err("corruption must not become a zero floor");
    assert!(err.to_string().contains("no trusted cycle or usage floor"), "{err}");
    assert_eq!(
        read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("evidence should remain"),
        before
    );
    assert!(matches!(
        read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
    let mut backup = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    backup.scanner_epoch = Some(7);
    backup.scanner_cycle = Some(40);
    save_config(
        store.clone(),
        &format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
        serde_json::to_vec(&backup).expect("backup should encode"),
    )
    .await
    .expect("valid backup should persist");
    let result = reset_scanner_usage_state_for_full_rebuild(CancellationToken::new(), store)
        .await
        .expect("valid backup should supply the recovery floor");
    assert_eq!(result.leader_epoch, 8);
    assert_eq!(result.next_cycle, 41);
}

#[tokio::test]
async fn scanner_usage_state_reset_rejects_replaced_intent_and_newer_cleanup_slot() {
    let store = Arc::new(MemoryConfigStore::default());
    let marker = scanner_usage_bootstrap_marker(std::time::SystemTime::UNIX_EPOCH, Some(3));
    let bytes = serde_json::to_vec(&marker).expect("marker should encode");
    save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), bytes.clone())
        .await
        .expect("intent should persist");
    let slots = read_usage_state_reset_slots(store.clone()).await.expect("slots should load");
    save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), bytes)
        .await
        .expect("another intent should persist");
    let err = reset_scanner_usage_state_slots_for_full_rebuild(store.clone(), &slots, 0, 3, || true)
        .await
        .expect_err("same epoch cannot replace an intent revision");
    assert!(err.to_string().contains("intent revision changed"), "{err}");

    let mut newer = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    newer.scanner_epoch = Some(3);
    let path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let bytes = serde_json::to_vec(&newer).expect("newer snapshot should encode");
    save_config(store.clone(), &path, bytes.clone())
        .await
        .expect("newer snapshot should persist");
    let slots = read_usage_state_reset_slots(store.clone())
        .await
        .expect("slots should reload");
    let err = reset_scanner_usage_state_slots_for_full_rebuild(store.clone(), &slots, 0, 3, || true)
        .await
        .expect_err("cleanup cannot delete same-epoch progress");
    assert!(err.to_string().contains("not older than its intent"), "{err}");
    assert_eq!(read_config(store, &path).await.expect("newer snapshot should remain"), bytes);
}

#[tokio::test]
#[serial]
async fn scanner_usage_state_reset_rejects_decodable_untrusted_floor() {
    let (_temp_dir, store) = setup_scanner_cycle_store_with_usage_baseline(false).await;
    let invalid_identity = DataUsageInfo {
        usage_snapshot_complete: true,
        buckets_count: 1,
        last_update: Some(std::time::SystemTime::UNIX_EPOCH),
        ..Default::default()
    };
    for usage in [DataUsageInfo::default(), invalid_identity] {
        save_config(
            store.clone(),
            DATA_USAGE_OBJ_NAME_PATH.as_str(),
            serde_json::to_vec(&usage).expect("fixture should encode"),
        )
        .await
        .expect("untrusted primary should persist");
        let before = read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("primary should load");
        let err = reset_scanner_usage_state_for_full_rebuild(CancellationToken::new(), store.clone())
            .await
            .expect_err("valid JSON alone cannot prove a usage floor");
        assert!(err.to_string().contains("no trusted cycle or usage floor"), "{err}");
        assert_eq!(
            read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
                .await
                .expect("evidence should remain"),
            before
        );
        assert!(matches!(
            read_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str()).await,
            Err(EcstoreError::ConfigNotFound)
        ));
    }
}

#[test]
fn full_rescan_reset_rejects_unknown_marker_phase_even_with_invalid_compat_fields() {
    for state in [serde_json::json!("rewrite-v2"), serde_json::json!(7), serde_json::Value::Null] {
        let marker = serde_json::json!({"state": state, "retry_count": "future-type", "schema_version": 99});
        let err = super::cycle_state::decode_recovery_marker_for_reset(
            &serde_json::to_vec(&marker).expect("future marker should encode"),
            &DataUsageCacheRevision::Etag("intent-1".to_string()),
        )
        .expect_err("unknown persistent phases must remain fenced");
        assert!(err.to_string().contains("state is unsupported"), "{err}");
    }
}

#[tokio::test]
#[serial]
async fn full_rescan_reset_preserves_unknown_phase_and_retries_completed_cleanup() {
    let (_temp_dir, store) = setup_scanner_cycle_store().await;
    save_config(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str(), b"corrupt".to_vec())
        .await
        .expect("corrupt primary should persist");
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(),
        br#"{"state":"future-rewrite"}"#.to_vec(),
    )
    .await
    .expect("future marker should persist");
    let primary_before = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("primary should load");
    let marker_before = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
        .await
        .expect("marker should load");
    let err = reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect_err("unknown phase must block explicit reset");
    assert!(err.to_string().contains("state is unsupported"), "{err}");
    assert_eq!(
        read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
            .await
            .expect("primary should remain"),
        primary_before
    );
    assert_eq!(
        read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str())
            .await
            .expect("marker should remain"),
        marker_before
    );

    save_config(store.clone(), DATA_USAGE_BLOOM_RECOVERY_PATH.as_str(), b"{malformed".to_vec())
        .await
        .expect("recoverable marker should persist");
    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("reset should complete");
    let primary = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("rebuilt primary should load");
    let usage = read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("fenced usage should load");
    reset_scanner_cycle_recovery(CancellationToken::new(), store.clone())
        .await
        .expect("retry after marker deletion should complete");
    assert_eq!(
        read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
            .await
            .expect("rebuilt primary should remain"),
        primary
    );
    assert_eq!(
        read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("fenced usage should remain"),
        usage
    );
}

#[tokio::test]
async fn scanner_usage_state_reset_slots_defer_when_publication_epoch_moves() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    store.objects.lock().await.insert(key, b"not-json".to_vec());
    let slots = read_usage_state_reset_slots(store.clone())
        .await
        .expect("usage reset slots should be inspected");
    store.publication_admission_blocked.store(true, Ordering::Release);

    let err = reset_scanner_usage_state_slots_for_full_rebuild(store, &slots, 0, 3, || true)
        .await
        .expect_err("movement admission loss must defer reset");
    assert!(
        err.to_string()
            .contains("scanner usage reset deferred by a movement epoch change"),
        "unexpected movement defer error: {err}"
    );
}

#[tokio::test]
async fn leadership_claim_defers_on_corrupt_usage_baseline_without_bloom_write() {
    let store = Arc::new(MemoryConfigStore::default());
    let usage_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    store.objects.lock().await.insert(usage_key.clone(), b"not-json".to_vec());
    store.revisions.lock().await.insert(usage_key, 1);

    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        next: 12,
        ..Default::default()
    };
    let mut persisted_epoch = 0;

    assert!(
        !claim_scanner_leadership(
            &ctx,
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut persisted_epoch,
            false,
            ScannerCycleResetPolicy::None,
        )
        .await
    );
    assert!(read_config(store, &DATA_USAGE_BLOOM_NAME_PATH).await.is_err());
}

#[tokio::test]
async fn leadership_claim_defers_on_unidentified_usage_baseline_without_bloom_write() {
    let store = Arc::new(MemoryConfigStore::default());
    let usage_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let data = serde_json::to_vec(&DataUsageInfo::default()).expect("default usage should encode");
    store.objects.lock().await.insert(usage_key.clone(), data);
    store.revisions.lock().await.insert(usage_key, 1);

    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        next: 12,
        ..Default::default()
    };
    let mut persisted_epoch = 0;

    assert!(
        !claim_scanner_leadership(
            &ctx,
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut persisted_epoch,
            false,
            ScannerCycleResetPolicy::None,
        )
        .await
    );
    assert!(read_config(store, &DATA_USAGE_BLOOM_NAME_PATH).await.is_err());
}

#[tokio::test]
async fn test_leadership_claim_confirms_commit_after_returned_error() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    let usage_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    store.error_after_commit_put_number.lock().await.insert(key.clone(), 1);
    store.error_after_commit_put_number.lock().await.insert(usage_key.clone(), 1);
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        current: 0,
        next: 12,
        cycle_completed: vec![],
        started: Utc::now(),
    };
    let mut persisted_epoch = 0;
    seed_usage_snapshot_for_leadership_claim(&store).await;

    assert!(
        claim_scanner_leadership(
            &ctx,
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut persisted_epoch,
            false,
            ScannerCycleResetPolicy::None,
        )
        .await
    );

    let state = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("ambiguous leadership claim should be durable");
    let (claimed_cycle, claimed_epoch) = decode_scanner_cycle_state(&state).expect("claimed cycle state should decode");
    assert_eq!(claimed_cycle.next, 12);
    assert_eq!(claimed_epoch, 1);
    assert_eq!(persisted_epoch, 1);
    assert!(matches!(revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-1"));
    assert_eq!(store.put_counts.lock().await.get(&key), Some(&1));
    let usage = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("ambiguous usage epoch fence should be durable");
    assert_eq!(
        serde_json::from_slice::<DataUsageInfo>(&usage)
            .expect("usage epoch fence should decode")
            .scanner_epoch,
        Some(1)
    );
    assert_eq!(store.put_counts.lock().await.get(&usage_key), Some(&1));
}

#[tokio::test]
async fn test_leadership_claim_usage_fence_rejects_old_inflight_writer() {
    let store = Arc::new(MemoryConfigStore::default());
    let usage_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let mut old_usage = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
        scanner_epoch: Some(4),
        scanner_cycle: Some(11),
        ..Default::default()
    };
    old_usage.buckets_usage.insert(
        "bucket-a".to_string(),
        rustfs_data_usage::BucketUsageInfo {
            objects_count: 2,
            size: 84,
            ..Default::default()
        },
    );
    old_usage.buckets_count = 1;
    old_usage.calculate_totals();
    old_usage.usage_snapshot_complete = true;
    let old_data = serde_json::to_vec(&old_usage).expect("old usage snapshot should encode");
    store.objects.lock().await.insert(usage_key.clone(), old_data.clone());
    store.revisions.lock().await.insert(usage_key, 1);

    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        next: 12,
        started: Utc::now(),
        ..Default::default()
    };
    let mut persisted_epoch = 4;
    assert!(
        claim_scanner_leadership(
            &ctx,
            store.clone(),
            &mut cycle,
            &mut revision,
            &mut persisted_epoch,
            false,
            ScannerCycleResetPolicy::None,
        )
        .await
    );

    let (fenced_data, fenced_revision) = read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("fenced usage snapshot should load");
    let fenced = serde_json::from_slice::<DataUsageInfo>(fenced_data.as_deref().expect("fenced usage snapshot should exist"))
        .expect("fenced usage snapshot should decode");
    assert_eq!(fenced.scanner_epoch, Some(5));
    assert_eq!(fenced.objects_total_count, 2);
    assert_eq!(fenced.buckets_usage.get("bucket-a").map(|usage| usage.size), Some(84));
    assert!(matches!(fenced_revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-2"));

    let stale_save = save_config_with_preconditions(
        store,
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        old_data,
        DataUsageCacheRevision::Etag("memory-1".to_string()).preconditions(),
    )
    .await;
    assert!(matches!(stale_save, Err(EcstoreError::PreconditionFailed)));
}

#[tokio::test]
async fn cycle_budget_lease_takeover_rejects_old_generation() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        current: 0,
        next: 12,
        cycle_completed: vec![],
        started: Utc::now(),
    };
    assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 1).await);
    seed_usage_snapshot_for_leadership_claim(&store).await;

    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
    store
        .cancel_after_successful_puts
        .lock()
        .await
        .insert(key.clone(), (2, ctx.clone()));
    cycle.next = 14;
    assert!(!persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 1).await);

    let (persisted, persisted_revision) = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .expect("committed old-epoch state should load");
    let mut replacement_cycle = decode_scanner_cycle_state(
        persisted
            .as_deref()
            .expect("old-epoch state should have committed before cancellation"),
    )
    .expect("old-epoch state should decode")
    .0;
    let mut replacement_revision = persisted_revision;
    let mut replacement_epoch = 1;
    let replacement_ctx = CancellationToken::new();
    assert!(
        claim_scanner_leadership(
            &replacement_ctx,
            store.clone(),
            &mut replacement_cycle,
            &mut replacement_revision,
            &mut replacement_epoch,
            false,
            ScannerCycleResetPolicy::None,
        )
        .await
    );

    let state = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .expect("replacement leadership claim should persist");
    let (claimed_cycle, claimed_epoch) = decode_scanner_cycle_state(&state).expect("replacement cycle state should decode");
    assert_eq!(claimed_cycle.next, 14);
    assert_eq!(claimed_epoch, 2);

    let mut stale_cycle = CurrentCycle { next: 15, ..cycle };
    let mut stale_revision = DataUsageCacheRevision::Etag("memory-2".to_string());
    let stale_ctx = CancellationToken::new();
    assert!(!persist_scanner_cycle_state(&stale_ctx, store, &mut stale_cycle, &mut stale_revision, 1,).await);
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_preserves_newer_snapshot() {
    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(2);
    let ctx = CancellationToken::new();

    let newer = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)), 2);
    let older = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10)), 1);

    sender.send(newer).await.expect("newer usage snapshot should enqueue");
    sender.send(older).await.expect("older usage snapshot should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

    let objects = store.objects.lock().await;
    let saved = objects
        .get(&memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()))
        .expect("data usage config should be saved");
    let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");

    assert_eq!(saved.buckets_count, 2);
    assert_eq!(saved.last_update, Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)));
    assert_eq!(outcome, DataUsagePersistOutcome::Current);
}

#[tokio::test]
#[serial]
async fn test_usage_save_object_not_found_defers_only_with_a_fresh_route_barrier() {
    for (route_blocked, expected) in [
        (true, DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement)),
        (false, DataUsagePersistOutcome::Failed),
    ] {
        let store = Arc::new(MemoryConfigStore::default());
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let baseline = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10)), 1);
        let baseline_data = serde_json::to_vec(&baseline).expect("baseline usage snapshot should encode");
        store.objects.lock().await.insert(key.clone(), baseline_data.clone());
        store.revisions.lock().await.insert(key.clone(), 1);
        store.object_not_found_put_number.lock().await.insert(key.clone(), 1);

        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(complete_usage_with_bucket_count(
                Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
                2,
            ))
            .await
            .expect("new usage snapshot should enqueue");
        drop(sender);
        let probe_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let route_probe_calls = probe_calls.clone();

        let outcome = store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe(
            CancellationToken::new(),
            store.clone(),
            receiver,
            None,
            Some(DataUsagePersistBaseline {
                data: Some(Bytes::from(baseline_data.clone())),
                revision: DataUsageCacheRevision::Etag("memory-1".to_string()),
            }),
            move || {
                let probe_calls = route_probe_calls.clone();
                async move {
                    let call = probe_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    (route_blocked && call > 1).then_some(ScannerCycleDeferReason::DataMovement)
                }
            },
        )
        .await;

        assert_eq!(outcome, expected);
        assert_eq!(
            probe_calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "ObjectNotFound must be followed by a fresh route-barrier probe"
        );
        assert_eq!(
            store.objects.lock().await.get(&key),
            Some(&baseline_data),
            "a route failure must not replace the authoritative baseline"
        );
    }
}

#[tokio::test]
#[serial]
async fn test_usage_save_route_barrier_prevents_missing_snapshot_creation() {
    for observational in [false, true] {
        let store = Arc::new(MemoryConfigStore::default());
        let target_path = if observational {
            DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str()
        } else {
            DATA_USAGE_OBJ_NAME_PATH.as_str()
        };
        let target_key = memory_config_key(RUSTFS_META_BUCKET, target_path);
        let mut incoming = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)), 1);
        incoming.usage_snapshot_converged = Some(!observational);
        let (sender, receiver) = mpsc::channel(1);
        sender.send(incoming).await.expect("usage snapshot should enqueue");
        drop(sender);

        let outcome = store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe(
            CancellationToken::new(),
            store.clone(),
            receiver,
            None,
            Some(DataUsagePersistBaseline {
                data: None,
                revision: DataUsageCacheRevision::Missing,
            }),
            || async { Some(ScannerCycleDeferReason::ActivityBaselineUnavailable) },
        )
        .await;

        assert_eq!(
            outcome,
            DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable)
        );
        assert!(!store.objects.lock().await.contains_key(&target_key));
        assert_eq!(
            store.put_counts.lock().await.get(&target_key),
            None,
            "the final publication fence must run before the first PUT"
        );
    }
}

#[tokio::test]
#[serial]
async fn test_observational_usage_defers_when_authoritative_baseline_is_missing() {
    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(1);
    let mut observation = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)), 1);
    observation.usage_snapshot_converged = Some(false);
    sender.send(observation).await.expect("observation should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe(
        CancellationToken::new(),
        store.clone(),
        receiver,
        None,
        None,
        || async { None },
    )
    .await;

    assert_eq!(outcome, DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement));
    assert!(
        !store
            .objects
            .lock()
            .await
            .contains_key(&memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str()))
    );
}

#[tokio::test]
#[serial]
async fn test_observational_usage_uses_fenced_backup_when_v2_primary_has_no_identity() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary = DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(100),
        usage_snapshot_complete: false,
        ..Default::default()
    };
    let mut backup = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    backup.scanner_epoch = Some(7);
    backup.scanner_cycle = Some(103);
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        serde_json::to_vec(&primary).expect("incomplete primary should encode"),
    );
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, &format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str())),
        serde_json::to_vec(&backup).expect("backup baseline should encode"),
    );

    let (sender, receiver) = mpsc::channel(1);
    let mut observation = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)), 1);
    observation.usage_snapshot_converged = Some(false);
    sender.send(observation).await.expect("observation should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe(
        CancellationToken::new(),
        store.clone(),
        receiver,
        None,
        None,
        || async { None },
    )
    .await;

    assert_eq!(outcome, DataUsagePersistOutcome::Saved);
    let observed = read_config(store, DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str())
        .await
        .expect("observational snapshot should be persisted");
    let observed = serde_json::from_slice::<DataUsageInfo>(&observed).expect("observational snapshot should decode");
    assert_eq!(observed.usage_snapshot_authoritative_baseline, Some(backup.snapshot_identity()));
}

#[tokio::test]
#[serial]
async fn test_observational_usage_uses_bootstrap_pending_primary_as_baseline() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH),
        scanner_epoch: Some(7),
        usage_snapshot_converged: Some(false),
        usage_snapshot_bootstrap_pending: true,
        ..Default::default()
    };
    assert!(data_usage_info_is_bootstrap_pending(&primary));
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        serde_json::to_vec(&primary).expect("bootstrap primary should encode"),
    );

    let (sender, receiver) = mpsc::channel(1);
    let mut observation = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)), 1);
    observation.usage_snapshot_converged = Some(false);
    sender.send(observation).await.expect("observation should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe(
        CancellationToken::new(),
        store.clone(),
        receiver,
        None,
        None,
        || async { None },
    )
    .await;

    assert_eq!(outcome, DataUsagePersistOutcome::Saved);
    let observed = read_config(store, DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str())
        .await
        .expect("observational snapshot should be persisted");
    let observed = serde_json::from_slice::<DataUsageInfo>(&observed).expect("observational snapshot should decode");
    assert_eq!(observed.usage_snapshot_authoritative_baseline, Some(primary.snapshot_identity()));
}

#[tokio::test]
async fn usage_baseline_does_not_fall_back_to_older_legacy_snapshot() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary = DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(100),
        usage_snapshot_complete: false,
        ..Default::default()
    };
    let mut legacy = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    legacy.scanner_epoch = Some(6);
    legacy.scanner_cycle = Some(103);
    let primary_data = serde_json::to_vec(&primary).expect("incomplete primary should encode");
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
        primary_data.clone(),
    );
    store.objects.lock().await.insert(
        memory_config_key(RUSTFS_META_BUCKET, LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
        serde_json::to_vec(&legacy).expect("legacy baseline should encode"),
    );

    let baseline = read_data_usage_persist_baseline(store)
        .await
        .expect("baseline inspection should complete");
    assert_eq!(baseline.data.as_deref(), Some(primary_data.as_slice()));
}

#[tokio::test]
#[serial]
async fn test_usage_route_barrier_precedes_durable_reconciliation() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let snapshot = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)), 1);
    let snapshot_data = serde_json::to_vec(&snapshot).expect("usage snapshot should encode");
    let (sender, receiver) = mpsc::channel(1);
    sender.send(snapshot).await.expect("usage snapshot should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe(
        CancellationToken::new(),
        store.clone(),
        receiver,
        None,
        Some(DataUsagePersistBaseline {
            data: Some(Bytes::from(snapshot_data)),
            revision: DataUsageCacheRevision::Etag("memory-1".to_string()),
        }),
        || async { Some(ScannerCycleDeferReason::DataMovement) },
    )
    .await;

    assert_eq!(outcome, DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement));
    assert_eq!(store.put_counts.lock().await.get(&key), None);
}

#[tokio::test]
#[serial]
async fn coordinator_does_not_put_after_remote_generation_flip() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(complete_usage_with_bucket_count(
            Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            1,
        ))
        .await
        .expect("usage snapshot should enqueue");
    drop(sender);

    let route_store = store.clone();
    let outcome = store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch(
        CancellationToken::new(),
        store.clone(),
        receiver,
        None,
        Some(DataUsagePersistBaseline {
            data: None,
            revision: DataUsageCacheRevision::Missing,
        }),
        ScannerPublicationFence::new(Some(0), None, None),
        move || {
            let route_store = route_store.clone();
            async move {
                // Model the remote lease holder flipping its movement generation
                // after the activity probe but before the coordinator's PUT.
                route_store.publication_admission_blocked.store(true, Ordering::Release);
                None
            }
        },
    )
    .await;

    assert_eq!(outcome, DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement));
    assert_eq!(store.put_counts.lock().await.get(&key), None);
}

#[tokio::test]
#[serial]
async fn coordinator_classifies_an_expired_publication_lease() {
    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(complete_usage_with_bucket_count(
            Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            1,
        ))
        .await
        .expect("usage snapshot should enqueue");
    drop(sender);

    let expired = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(1))
        .expect("test instant should support a one-second subtraction");
    let outcome =
        store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch_and_lease_fence(
            CancellationToken::new(),
            store.clone(),
            receiver,
            None,
            Some(DataUsagePersistBaseline {
                data: None,
                revision: DataUsageCacheRevision::Missing,
            }),
            ScannerPublicationFence::new(None, Some(expired), None),
            || async { None },
        )
        .await;

    assert_eq!(
        outcome.outcome(),
        DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::PublicationLeaseDeadlineExceeded)
    );
    assert!(store.put_counts.lock().await.is_empty(), "expired lease must prevent a PUT");
}

#[tokio::test]
async fn backup_sync_checks_the_lease_deadline_after_a_slow_backup_read() {
    let store = Arc::new(MemoryConfigStore::default());
    let primary_path = DATA_USAGE_OBJ_NAME_PATH.as_str();
    let backup_path = format!("{primary_path}.bkp");
    let primary_key = memory_config_key(RUSTFS_META_BUCKET, primary_path);
    let backup_key = memory_config_key(RUSTFS_META_BUCKET, &backup_path);
    let primary = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0);
    store
        .objects
        .lock()
        .await
        .insert(primary_key, serde_json::to_vec(&primary).expect("primary usage snapshot should encode"));
    store
        .delayed_gets
        .lock()
        .await
        .insert(backup_key.clone(), Duration::from_millis(20));

    // The primary read is allowed to start, but the backup read consumes the
    // remaining lease window. The second deadline check must prevent a stale
    // backup PUT after that window has elapsed.
    let deadline = std::time::Instant::now()
        .checked_add(std::time::Duration::from_millis(5))
        .expect("test deadline should support a five-millisecond window");
    let result = sync_data_usage_backup_from_primary_for_epoch_and_lease_and_fence(
        &CancellationToken::new(),
        store.clone(),
        None,
        Some(deadline),
        None,
    )
    .await;

    assert!(scanner_publication_epoch_changed(
        &result.expect_err("an expired backup lease must defer publication")
    ));
    assert!(!store.objects.lock().await.contains_key(&backup_key));
    assert_eq!(store.put_counts.lock().await.get(&backup_key), None);
}

#[tokio::test]
#[serial]
async fn test_deferred_usage_save_keeps_last_real_save_metric() {
    let metrics = global_metrics();
    metrics.record_scanner_usage_save_result(ScannerUsageSaveResult::Success);
    let before = metrics.report().await.usage_freshness;

    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(complete_usage_with_bucket_count(
            Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            1,
        ))
        .await
        .expect("usage snapshot should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe(
        CancellationToken::new(),
        store,
        receiver,
        None,
        Some(DataUsagePersistBaseline {
            data: None,
            revision: DataUsageCacheRevision::Missing,
        }),
        || async { Some(ScannerCycleDeferReason::DataMovement) },
    )
    .await;

    assert_eq!(outcome, DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement));
    let after = metrics.report().await.usage_freshness;
    assert_eq!(after.last_usage_save_result, before.last_usage_save_result);
    assert_eq!(after.last_usage_save_result_code, before.last_usage_save_result_code);
    assert_eq!(after.last_usage_save_unix_secs, before.last_usage_save_unix_secs);
    assert_eq!(after.deferred_total, before.deferred_total.saturating_add(1));
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_fences_interleaving_newer_writer() {
    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(1);
    let ctx = CancellationToken::new();
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let newer = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)), 2);
    let stale = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10)), 1);
    store
        .interleaving_puts
        .lock()
        .await
        .insert(key.clone(), (1, serde_json::to_vec(&newer).expect("newer usage snapshot should encode")));

    sender.send(stale).await.expect("stale usage snapshot should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

    let objects = store.objects.lock().await;
    let saved = objects
        .get(&key)
        .expect("interleaving newer usage snapshot should remain saved");
    let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");
    assert_eq!(saved.buckets_count, 2);
    assert_eq!(saved.last_update, newer.last_update);
    assert_eq!(outcome, DataUsagePersistOutcome::Current);
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_does_not_resurrect_deleted_bucket_after_conflict() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let mut initial = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
        scanner_epoch: Some(8),
        scanner_cycle: Some(12),
        ..Default::default()
    };
    initial.buckets_usage.insert(
        "bucket-a".to_string(),
        rustfs_data_usage::BucketUsageInfo {
            objects_count: 2,
            size: 84,
            ..Default::default()
        },
    );
    initial.bucket_sizes.insert("bucket-a".to_string(), 84);
    initial.buckets_count = 1;
    initial.calculate_totals();
    mark_usage_snapshot_complete(&mut initial);
    let initial_data = serde_json::to_vec(&initial).expect("initial usage snapshot should encode");
    store.objects.lock().await.insert(key.clone(), initial_data.clone());
    store.revisions.lock().await.insert(key.clone(), 1);

    let mut deleted = initial.clone();
    deleted.buckets_usage.clear();
    deleted.bucket_sizes.clear();
    deleted.buckets_count = 0;
    deleted.calculate_totals();
    mark_usage_snapshot_complete(&mut deleted);
    store
        .interleaving_puts
        .lock()
        .await
        .insert(key.clone(), (1, serde_json::to_vec(&deleted).expect("deleted snapshot should encode")));

    let mut incoming = initial;
    incoming.last_update = Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(30));
    incoming.scanner_cycle = Some(13);
    let (sender, receiver) = mpsc::channel(1);
    sender.send(incoming).await.expect("stale scanner snapshot should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome_for_epoch_and_baseline(
        CancellationToken::new(),
        store.clone(),
        receiver,
        Some(8),
        Some(DataUsagePersistBaseline {
            data: Some(Bytes::from(initial_data)),
            revision: DataUsageCacheRevision::Etag("memory-1".to_string()),
        }),
    )
    .await;

    assert_eq!(outcome, DataUsagePersistOutcome::Current);
    let saved = store
        .objects
        .lock()
        .await
        .get(&key)
        .cloned()
        .expect("deleted usage snapshot should remain");
    let saved = serde_json::from_slice::<DataUsageInfo>(&saved).expect("deleted usage snapshot should decode");
    assert!(!saved.buckets_usage.contains_key("bucket-a"));
    assert!(!saved.bucket_sizes.contains_key("bucket-a"));
    assert_eq!(store.put_counts.lock().await.get(&key), Some(&1));
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_updates_backup_with_new_bucket() {
    let store = Arc::new(MemoryConfigStore::default());
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let backup_key = memory_config_key(RUSTFS_META_BUCKET, &backup_path);
    let deleted = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
        scanner_epoch: Some(8),
        scanner_cycle: Some(1),
        ..complete_usage_with_bucket_count(None, 0)
    };
    store.objects.lock().await.insert(
        backup_key.clone(),
        serde_json::to_vec(&deleted).expect("deleted backup snapshot should encode"),
    );
    store.revisions.lock().await.insert(backup_key.clone(), 1);

    let (sender, receiver) = mpsc::channel(11);
    for cycle in 2_u64..=12 {
        let mut incoming = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20 + cycle)),
            scanner_epoch: Some(8),
            scanner_cycle: Some(cycle),
            ..Default::default()
        };
        incoming.buckets_usage.insert(
            "bucket-a".to_string(),
            rustfs_data_usage::BucketUsageInfo {
                objects_count: 2,
                size: 84,
                ..Default::default()
            },
        );
        incoming.bucket_sizes.insert("bucket-a".to_string(), 84);
        incoming.buckets_count = 1;
        incoming.calculate_totals();
        mark_usage_snapshot_complete(&mut incoming);
        sender.send(incoming).await.expect("usage snapshot should enqueue");
    }
    drop(sender);

    assert_eq!(
        store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), receiver).await,
        DataUsagePersistOutcome::Saved
    );

    let saved = store
        .objects
        .lock()
        .await
        .get(&backup_key)
        .cloned()
        .expect("deleted backup snapshot should remain");
    let saved = serde_json::from_slice::<DataUsageInfo>(&saved).expect("backup snapshot should decode");
    assert!(saved.buckets_usage.contains_key("bucket-a"));
    assert!(saved.bucket_sizes.contains_key("bucket-a"));
    assert_eq!(saved.scanner_cycle, Some(10));
    assert_eq!(store.put_counts.lock().await.get(&backup_key), Some(&1));
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_repairs_backup_after_primary_only_commit() {
    let store = Arc::new(MemoryConfigStore::default());
    let main_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let backup_key = memory_config_key(RUSTFS_META_BUCKET, &backup_path);
    let durable = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(30)),
        scanner_epoch: Some(8),
        scanner_cycle: Some(10),
        ..complete_usage_with_bucket_count(None, 0)
    };
    let encoded = serde_json::to_vec(&durable).expect("usage snapshot should encode");
    store.objects.lock().await.insert(main_key.clone(), encoded.clone());
    store.revisions.lock().await.insert(main_key.clone(), 1);

    let (sender, receiver) = mpsc::channel(1);
    sender.send(durable).await.expect("usage snapshot should enqueue");
    drop(sender);

    assert_eq!(
        store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), receiver).await,
        DataUsagePersistOutcome::AlreadyDurable
    );
    assert_eq!(store.objects.lock().await.get(&backup_key), Some(&encoded));
    assert_eq!(store.put_counts.lock().await.get(&main_key), None);
    assert_eq!(store.put_counts.lock().await.get(&backup_key), Some(&1));
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_copies_concurrent_bucket_removal_to_backup() {
    let store = Arc::new(MemoryConfigStore::default());
    let main_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let backup_key = memory_config_key(RUSTFS_META_BUCKET, &backup_path);

    let mut incoming = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(30)),
        scanner_epoch: Some(8),
        scanner_cycle: Some(10),
        ..Default::default()
    };
    incoming.buckets_usage.insert(
        "bucket-a".to_string(),
        rustfs_data_usage::BucketUsageInfo {
            objects_count: 2,
            size: 84,
            ..Default::default()
        },
    );
    incoming.bucket_sizes.insert("bucket-a".to_string(), 84);
    incoming.buckets_count = 1;
    incoming.calculate_totals();
    mark_usage_snapshot_complete(&mut incoming);

    let mut deleted = incoming.clone();
    deleted.last_update = Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(31));
    deleted.buckets_usage.clear();
    deleted.bucket_sizes.clear();
    deleted.buckets_count = 0;
    deleted.calculate_totals();
    mark_usage_snapshot_complete(&mut deleted);
    store.replace_after_successful_puts.lock().await.insert(
        main_key.clone(),
        (1, serde_json::to_vec(&deleted).expect("deleted primary snapshot should encode")),
    );
    store.objects.lock().await.insert(
        backup_key.clone(),
        serde_json::to_vec(&incoming).expect("existing backup snapshot should encode"),
    );
    store.revisions.lock().await.insert(backup_key.clone(), 1);

    let (sender, receiver) = mpsc::channel(1);
    sender.send(incoming).await.expect("usage snapshot should enqueue");
    drop(sender);

    assert_eq!(
        store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), receiver).await,
        DataUsagePersistOutcome::Saved
    );

    for key in [main_key, backup_key] {
        let saved = store
            .objects
            .lock()
            .await
            .get(&key)
            .cloned()
            .expect("usage snapshot should remain");
        let saved = serde_json::from_slice::<DataUsageInfo>(&saved).expect("usage snapshot should decode");
        assert!(!saved.buckets_usage.contains_key("bucket-a"));
        assert!(!saved.bucket_sizes.contains_key("bucket-a"));
    }
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_retries_after_stale_interleaving_writer() {
    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(1);
    let ctx = CancellationToken::new();
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let initial = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10)), 3);
    let stale_winner = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)), 3);
    let current = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(30)), 3);
    store
        .objects
        .lock()
        .await
        .insert(key.clone(), serde_json::to_vec(&initial).expect("initial usage snapshot should encode"));
    store.revisions.lock().await.insert(key.clone(), 1);
    store.interleaving_puts.lock().await.insert(
        key.clone(),
        (1, serde_json::to_vec(&stale_winner).expect("stale usage snapshot should encode")),
    );

    sender
        .send(current.clone())
        .await
        .expect("current usage snapshot should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

    let objects = store.objects.lock().await;
    let saved = objects
        .get(&key)
        .expect("current usage snapshot should replace the stale conflict winner");
    let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");
    assert_eq!(saved.buckets_count, 3);
    assert_eq!(saved.last_update, current.last_update);
    assert_eq!(outcome, DataUsagePersistOutcome::Saved);
    drop(objects);
    assert_eq!(store.put_counts.lock().await.get(&key), Some(&2));
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_rejects_untimestamped_complete_snapshot() {
    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(2);
    let ctx = CancellationToken::new();

    let timestamped = complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)), 2);
    let untimestamped = complete_usage_with_bucket_count(None, 1);

    sender
        .send(timestamped)
        .await
        .expect("timestamped usage snapshot should enqueue");
    sender
        .send(untimestamped)
        .await
        .expect("untimestamped usage snapshot should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

    let objects = store.objects.lock().await;
    let saved = objects
        .get(&memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()))
        .expect("data usage config should be saved");
    let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");

    assert_eq!(saved.buckets_count, 2);
    assert_eq!(saved.last_update, Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)));
    assert_eq!(outcome, DataUsagePersistOutcome::Failed);
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_recognizes_already_durable_snapshot() {
    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(1);
    let ctx = CancellationToken::new();
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let snapshot = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
        scanner_cycle: Some(12),
        ..complete_usage_with_bucket_count(None, 2)
    };
    store
        .objects
        .lock()
        .await
        .insert(key.clone(), serde_json::to_vec(&snapshot).expect("durable usage snapshot should encode"));
    store.revisions.lock().await.insert(key.clone(), 1);

    sender
        .send(snapshot)
        .await
        .expect("ambiguous committed snapshot should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

    assert_eq!(outcome, DataUsagePersistOutcome::AlreadyDurable);
    assert_eq!(store.put_counts.lock().await.get(&key), None);
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_advances_past_changed_same_epoch_cycle() {
    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(1);
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let durable = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
        scanner_epoch: Some(8),
        scanner_cycle: Some(12),
        ..complete_usage_with_bucket_count(None, 2)
    };
    store
        .objects
        .lock()
        .await
        .insert(key.clone(), serde_json::to_vec(&durable).expect("durable usage snapshot should encode"));
    store.revisions.lock().await.insert(key.clone(), 1);

    sender
        .send(DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(30)),
            scanner_epoch: Some(8),
            scanner_cycle: Some(12),
            ..complete_usage_with_bucket_count(None, 3)
        })
        .await
        .expect("changed retry snapshot should enqueue");
    drop(sender);

    let outcome =
        store_data_usage_in_backend_with_outcome_for_epoch(CancellationToken::new(), store.clone(), receiver, Some(8)).await;

    assert_eq!(outcome, DataUsagePersistOutcome::PriorCycleDurable);
    assert_eq!(store.put_counts.lock().await.get(&key), None);
    let saved = store
        .objects
        .lock()
        .await
        .get(&key)
        .cloned()
        .expect("first snapshot should remain durable");
    assert_eq!(
        serde_json::from_slice::<DataUsageInfo>(&saved)
            .expect("durable usage snapshot should decode")
            .buckets_count,
        2
    );
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_orders_scanner_cycles_before_wall_clock() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let existing = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(200)),
        scanner_cycle: Some(12),
        ..complete_usage_with_bucket_count(None, 2)
    };
    store
        .objects
        .lock()
        .await
        .insert(key.clone(), serde_json::to_vec(&existing).expect("existing usage snapshot should encode"));
    store.revisions.lock().await.insert(key.clone(), 1);

    let (older_sender, older_receiver) = mpsc::channel(1);
    let older = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(300)),
        scanner_cycle: Some(11),
        ..complete_usage_with_bucket_count(None, 1)
    };
    older_sender.send(older).await.expect("older-cycle snapshot should enqueue");
    drop(older_sender);
    assert_eq!(
        store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), older_receiver).await,
        DataUsagePersistOutcome::Current
    );

    let (newer_sender, newer_receiver) = mpsc::channel(1);
    let newer = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(100)),
        scanner_cycle: Some(13),
        ..complete_usage_with_bucket_count(None, 3)
    };
    newer_sender
        .send(newer.clone())
        .await
        .expect("newer-cycle snapshot should enqueue");
    drop(newer_sender);
    assert_eq!(
        store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), newer_receiver).await,
        DataUsagePersistOutcome::Saved
    );

    let saved = store
        .objects
        .lock()
        .await
        .get(&key)
        .cloned()
        .expect("newer scanner cycle should be persisted");
    assert_eq!(
        serde_json::from_slice::<DataUsageInfo>(&saved)
            .expect("persisted usage snapshot should decode")
            .scanner_cycle,
        Some(13)
    );
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_orders_leader_epochs_before_cycles() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let existing = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(200)),
        scanner_epoch: Some(8),
        scanner_cycle: Some(12),
        ..complete_usage_with_bucket_count(None, 2)
    };
    store
        .objects
        .lock()
        .await
        .insert(key.clone(), serde_json::to_vec(&existing).expect("existing usage snapshot should encode"));
    store.revisions.lock().await.insert(key.clone(), 1);

    let (older_sender, older_receiver) = mpsc::channel(1);
    older_sender
        .send(DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(300)),
            scanner_epoch: Some(7),
            scanner_cycle: Some(99),
            ..complete_usage_with_bucket_count(None, 1)
        })
        .await
        .expect("old-epoch snapshot should enqueue");
    drop(older_sender);
    assert_eq!(
        store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), older_receiver).await,
        DataUsagePersistOutcome::Current
    );

    let (newer_sender, newer_receiver) = mpsc::channel(1);
    newer_sender
        .send(DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(100)),
            scanner_epoch: None,
            scanner_cycle: Some(1),
            ..complete_usage_with_bucket_count(None, 3)
        })
        .await
        .expect("replacement-epoch snapshot should enqueue");
    drop(newer_sender);
    assert_eq!(
        store_data_usage_in_backend_with_outcome_for_epoch(CancellationToken::new(), store.clone(), newer_receiver, Some(9),)
            .await,
        DataUsagePersistOutcome::Saved
    );

    let saved = store
        .objects
        .lock()
        .await
        .get(&key)
        .cloned()
        .expect("replacement leader snapshot should persist");
    let saved = serde_json::from_slice::<DataUsageInfo>(&saved).expect("persisted usage snapshot should decode");
    assert_eq!(saved.scanner_epoch, Some(9));
    assert_eq!(saved.scanner_cycle, Some(1));
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_keeps_first_same_cycle_snapshot() {
    let store = Arc::new(MemoryConfigStore::default());
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let existing = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(100)),
        scanner_cycle: Some(12),
        ..complete_usage_with_bucket_count(None, 2)
    };
    store
        .objects
        .lock()
        .await
        .insert(key.clone(), serde_json::to_vec(&existing).expect("existing usage snapshot should encode"));
    store.revisions.lock().await.insert(key.clone(), 1);

    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(300)),
            scanner_cycle: Some(12),
            ..complete_usage_with_bucket_count(None, 3)
        })
        .await
        .expect("conflicting same-cycle snapshot should enqueue");
    drop(sender);

    assert_eq!(
        store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), receiver).await,
        DataUsagePersistOutcome::Current
    );
    let saved = store
        .objects
        .lock()
        .await
        .get(&key)
        .cloned()
        .expect("first same-cycle snapshot should remain persisted");
    assert_eq!(
        serde_json::from_slice::<DataUsageInfo>(&saved)
            .expect("persisted usage snapshot should decode")
            .buckets_count,
        2
    );
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_rejects_incomplete_snapshot() {
    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(2);
    let complete_update = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10);

    sender
        .send(complete_usage_with_bucket_count(Some(complete_update), 1))
        .await
        .expect("complete usage snapshot should enqueue");
    sender
        .send(DataUsageInfo {
            last_update: Some(complete_update + Duration::from_secs(1)),
            buckets_count: 1,
            ..Default::default()
        })
        .await
        .expect("incomplete usage snapshot should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), receiver).await;

    let objects = store.objects.lock().await;
    let saved = objects
        .get(&memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()))
        .expect("complete data usage snapshot should remain saved");
    let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");
    assert_eq!(saved.last_update, Some(complete_update));
    assert!(saved.is_complete_bucket_usage_snapshot());
    assert_eq!(outcome, DataUsagePersistOutcome::Failed);
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_preserves_superseded_status() {
    let store = Arc::new(MemoryConfigStore::default());
    let authoritative_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let authoritative = DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(10),
        ..complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 1)
    };
    let authoritative_bytes = serde_json::to_vec(&authoritative).expect("authoritative snapshot should encode");
    store
        .objects
        .lock()
        .await
        .insert(authoritative_key.clone(), authoritative_bytes.clone());
    store.revisions.lock().await.insert(authoritative_key.clone(), 1);

    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
            scanner_epoch: Some(7),
            scanner_cycle: Some(11),
            usage_snapshot_converged: Some(false),
            ..complete_usage_with_bucket_count(None, 1)
        })
        .await
        .expect("superseded usage snapshot should enqueue");
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), receiver).await;
    let saved = store
        .objects
        .lock()
        .await
        .get(&memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str()))
        .cloned()
        .expect("superseded usage snapshot should persist");
    let saved = serde_json::from_slice::<DataUsageInfo>(&saved).expect("persisted usage snapshot should decode");

    assert_eq!(outcome, DataUsagePersistOutcome::Saved);
    assert!(saved.is_complete_bucket_usage_snapshot());
    assert_eq!(saved.usage_snapshot_converged, Some(false));
    assert_eq!(saved.usage_snapshot_authoritative_baseline, Some(authoritative.snapshot_identity()));
    assert_eq!(
        store.objects.lock().await.get(&authoritative_key),
        Some(&authoritative_bytes),
        "an observation must never lower the quota-authoritative snapshot"
    );
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_removes_observed_after_authoritative_save() {
    let store = Arc::new(MemoryConfigStore::default());
    let authoritative_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let authoritative = DataUsageInfo {
        scanner_epoch: Some(7),
        scanner_cycle: Some(10),
        ..complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 1)
    };
    store.objects.lock().await.insert(
        authoritative_key.clone(),
        serde_json::to_vec(&authoritative).expect("authoritative snapshot should encode"),
    );
    store.revisions.lock().await.insert(authoritative_key, 1);

    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
            scanner_epoch: Some(7),
            scanner_cycle: Some(11),
            usage_snapshot_converged: Some(false),
            ..complete_usage_with_bucket_count(None, 1)
        })
        .await
        .expect("superseded usage snapshot should enqueue");
    drop(sender);

    assert_eq!(
        store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), receiver).await,
        DataUsagePersistOutcome::Saved
    );
    let observed_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str());
    assert!(store.objects.lock().await.contains_key(&observed_key));

    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(2)),
            scanner_epoch: Some(7),
            scanner_cycle: Some(12),
            usage_snapshot_converged: Some(true),
            ..complete_usage_with_bucket_count(None, 1)
        })
        .await
        .expect("authoritative usage snapshot should enqueue");
    drop(sender);

    assert_eq!(
        store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), receiver).await,
        DataUsagePersistOutcome::Saved
    );
    assert!(
        !store.objects.lock().await.contains_key(&observed_key),
        "an authoritative snapshot should retire stale observations"
    );

    let next_authoritative = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(3)),
        scanner_epoch: Some(7),
        scanner_cycle: Some(13),
        usage_snapshot_converged: Some(true),
        ..complete_usage_with_bucket_count(None, 1)
    };
    let newer_observed = DataUsageInfo {
        last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(4)),
        scanner_epoch: Some(7),
        scanner_cycle: Some(14),
        usage_snapshot_converged: Some(false),
        usage_snapshot_authoritative_baseline: Some(next_authoritative.snapshot_identity()),
        ..complete_usage_with_bucket_count(None, 1)
    };
    store.objects.lock().await.insert(
        observed_key.clone(),
        serde_json::to_vec(&newer_observed).expect("newer observed snapshot should encode"),
    );
    store.revisions.lock().await.insert(observed_key.clone(), 3);

    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(next_authoritative)
        .await
        .expect("next authoritative usage snapshot should enqueue");
    drop(sender);

    assert_eq!(
        store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), receiver).await,
        DataUsagePersistOutcome::Saved
    );
    assert!(
        store.objects.lock().await.contains_key(&observed_key),
        "a newer observation must survive stale authoritative cleanup"
    );
}

fn mark_usage_snapshot_complete(info: &mut DataUsageInfo) {
    info.usage_snapshot_complete = true;
}

fn complete_usage_with_bucket_count(last_update: Option<std::time::SystemTime>, buckets_count: u64) -> DataUsageInfo {
    let mut info = DataUsageInfo {
        last_update,
        buckets_count,
        usage_snapshot_complete: true,
        ..Default::default()
    };
    for index in 0..buckets_count {
        let bucket = format!("bucket-{index}");
        info.buckets_usage.insert(bucket.clone(), Default::default());
        info.bucket_sizes.insert(bucket, 0);
    }
    info
}

async fn seed_usage_snapshot_for_leadership_claim(store: &Arc<MemoryConfigStore>) {
    let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let data = serde_json::to_vec(&complete_usage_with_bucket_count(Some(std::time::SystemTime::UNIX_EPOCH), 0))
        .expect("leadership usage baseline should encode");
    store.objects.lock().await.insert(key.clone(), data);
    store.revisions.lock().await.insert(key, 1);
}

fn usage_with_last_update(last_update: Option<std::time::SystemTime>) -> DataUsageInfo {
    complete_usage_with_bucket_count(last_update, 0)
}

#[test]
fn test_stale_data_usage_update_reason_allows_newer_incoming() {
    let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
    let incoming = usage_with_last_update(Some(now));
    let existing = usage_with_last_update(Some(now - Duration::from_secs(60)));
    assert_eq!(stale_data_usage_update_reason(&incoming, &existing, now), None);
}

#[test]
fn test_stale_data_usage_update_reason_skips_older_or_equal_incoming() {
    let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
    let existing = usage_with_last_update(Some(now - Duration::from_secs(60)));

    let older = usage_with_last_update(Some(now - Duration::from_secs(120)));
    assert_eq!(stale_data_usage_update_reason(&older, &existing, now), Some("older_or_equal_last_update"));

    let equal = usage_with_last_update(existing.last_update);
    assert_eq!(stale_data_usage_update_reason(&equal, &existing, now), Some("older_or_equal_last_update"));
}

#[test]
fn test_stale_data_usage_update_reason_allows_save_when_existing_is_future_dated() {
    // Existing snapshot timestamp beyond the clock tolerance is untrustworthy
    // (clock step-back / slower-clock leader): the save must be allowed even
    // though incoming <= existing, otherwise usage stats freeze forever.
    let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
    let existing =
        usage_with_last_update(Some(now + rustfs_data_usage::USAGE_LAST_UPDATE_FUTURE_TOLERANCE + Duration::from_secs(1)));
    let incoming = usage_with_last_update(Some(now));
    assert_eq!(stale_data_usage_update_reason(&incoming, &existing, now), None);
}

#[test]
fn test_stale_data_usage_update_reason_skips_at_exact_tolerance_boundary() {
    // Exactly at now + tolerance is still within the trusted window.
    let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
    let existing = usage_with_last_update(Some(now + rustfs_data_usage::USAGE_LAST_UPDATE_FUTURE_TOLERANCE));
    let incoming = usage_with_last_update(Some(now));
    assert_eq!(
        stale_data_usage_update_reason(&incoming, &existing, now),
        Some("older_or_equal_last_update")
    );
}

#[test]
fn test_stale_data_usage_update_reason_preserves_none_handling() {
    let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

    let incoming_none = usage_with_last_update(None);
    let existing_some = usage_with_last_update(Some(now - Duration::from_secs(60)));
    assert_eq!(
        stale_data_usage_update_reason(&incoming_none, &existing_some, now),
        Some("missing_incoming_last_update")
    );

    let incoming_some = usage_with_last_update(Some(now));
    let existing_none = usage_with_last_update(None);
    assert_eq!(stale_data_usage_update_reason(&incoming_some, &existing_none, now), None);

    let both_none = usage_with_last_update(None);
    assert_eq!(stale_data_usage_update_reason(&both_none, &usage_with_last_update(None), now), None);
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_keeps_backup_when_primary_save_fails() {
    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(11);
    let ctx = CancellationToken::new();

    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let main_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
    let backup_key = memory_config_key(RUSTFS_META_BUCKET, &backup_path);
    let old_backup = b"old-backup".to_vec();

    store.objects.lock().await.insert(backup_key.clone(), old_backup.clone());
    store.fail_put_number.lock().await.insert(main_key.clone(), 11);

    for idx in 1_u64..=11 {
        sender
            .send(complete_usage_with_bucket_count(
                Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(idx)),
                idx,
            ))
            .await
            .expect("usage snapshot should enqueue");
    }
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

    let objects = store.objects.lock().await;
    assert_eq!(
        objects.get(&backup_key),
        Some(&old_backup),
        "primary save failure must not overwrite the previous backup"
    );
    let saved = objects
        .get(&main_key)
        .expect("last successful primary usage snapshot should remain saved");
    let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");
    assert_eq!(saved.buckets_count, 10);
    assert_eq!(saved.last_update, Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10)));
    assert_eq!(outcome, DataUsagePersistOutcome::Failed);
}

#[tokio::test]
#[serial]
async fn test_store_data_usage_in_backend_reports_missing_snapshot() {
    let store = Arc::new(MemoryConfigStore::default());
    let (sender, receiver) = mpsc::channel(1);
    let ctx = CancellationToken::new();
    drop(sender);

    let outcome = store_data_usage_in_backend_with_outcome(ctx, store, receiver).await;

    assert_eq!(outcome, DataUsagePersistOutcome::NoUpdate);
}

#[test]
fn test_scanner_cycle_completion_prioritizes_persist_failure() {
    assert_eq!(
        scanner_cycle_completion_outcome(
            ScannerCycleStatus::Complete,
            DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement),
            true,
            false,
        ),
        ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement)
    );
    assert_eq!(
        scanner_cycle_completion_outcome(
            ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable),
            DataUsagePersistOutcome::NoUpdate,
            false,
            false,
        ),
        ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable)
    );
    assert_eq!(
        scanner_cycle_completion_outcome(
            ScannerCycleStatus::Deferred(ScannerCycleDeferReason::DataMovement),
            DataUsagePersistOutcome::Saved,
            false,
            false,
        ),
        ScannerCycleOutcome::Failed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(
            ScannerCycleStatus::Deferred(ScannerCycleDeferReason::DataMovement),
            DataUsagePersistOutcome::NoUpdate,
            true,
            false,
        ),
        ScannerCycleOutcome::Failed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(
            ScannerCycleStatus::Deferred(ScannerCycleDeferReason::DataMovement),
            DataUsagePersistOutcome::Failed,
            false,
            false,
        ),
        ScannerCycleOutcome::Failed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Incomplete, DataUsagePersistOutcome::NoUpdate, false, false),
        ScannerCycleOutcome::Failed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Incomplete, DataUsagePersistOutcome::Failed, true, true),
        ScannerCycleOutcome::Failed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Incomplete, DataUsagePersistOutcome::NoUpdate, true, true),
        ScannerCycleOutcome::Failed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Incomplete, DataUsagePersistOutcome::Saved, true, false),
        ScannerCycleOutcome::Partial
    );
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Incomplete, DataUsagePersistOutcome::Saved, true, true),
        ScannerCycleOutcome::Failed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Complete, DataUsagePersistOutcome::Saved, true, false),
        ScannerCycleOutcome::Completed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Complete, DataUsagePersistOutcome::AlreadyDurable, true, false,),
        ScannerCycleOutcome::Completed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Complete, DataUsagePersistOutcome::PriorCycleDurable, true, false,),
        ScannerCycleOutcome::Completed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Complete, DataUsagePersistOutcome::Current, false, false),
        ScannerCycleOutcome::Completed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Complete, DataUsagePersistOutcome::Current, true, false),
        ScannerCycleOutcome::Failed
    );
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Complete, DataUsagePersistOutcome::NoUpdate, false, false),
        ScannerCycleOutcome::Failed
    );
    for persist_outcome in [
        DataUsagePersistOutcome::NoUpdate,
        DataUsagePersistOutcome::Current,
        DataUsagePersistOutcome::Saved,
    ] {
        assert_eq!(
            scanner_cycle_completion_outcome(ScannerCycleStatus::Superseded, persist_outcome, true, false),
            ScannerCycleOutcome::Superseded
        );
    }
    assert_eq!(
        scanner_cycle_completion_outcome(ScannerCycleStatus::Superseded, DataUsagePersistOutcome::Saved, true, true),
        ScannerCycleOutcome::Failed
    );
}

#[test]
fn scanner_cycle_cache_floor_stays_pending_during_deferred_usage_publication() {
    for reason in [
        ScannerCycleDeferReason::DataMovement,
        ScannerCycleDeferReason::ActivityBaselineUnavailable,
        ScannerCycleDeferReason::PublicationLeaseDeadlineExceeded,
        ScannerCycleDeferReason::PublicationLeaseReleaseFailed,
    ] {
        let deferred = DataUsagePersistOutcome::Deferred(reason);
        assert_eq!(
            scanner_cycle_pre_commit_outcome(Some(19), &deferred),
            Some(ScannerCyclePreCommitOutcome::Deferred(reason)),
            "a blocked publication must not persist the routed scanner cycle floor"
        );
        assert_eq!(
            scanner_cycle_pre_commit_outcome(None, &deferred),
            Some(ScannerCyclePreCommitOutcome::Deferred(reason))
        );
    }
    assert_eq!(
        scanner_cycle_pre_commit_outcome(Some(19), &DataUsagePersistOutcome::Saved),
        Some(ScannerCyclePreCommitOutcome::RecoverCacheCycle(19))
    );
    assert_eq!(
        scanner_cycle_pre_commit_outcome(Some(19), &DataUsagePersistOutcome::Failed),
        Some(ScannerCyclePreCommitOutcome::RecoverCacheCycle(19))
    );
}

#[test]
#[serial]
fn finalizing_a_saved_enum_without_proof_keeps_dirty_pending() {
    crate::scanner_io::clear_dirty_usage_bucket("photos");
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

    let remote_acknowledgement = ScannerDirtyUsageAcknowledgement {
        host: "node-2".to_string(),
        instance_id: "0123456789abcdef0123456789abcdef".to_string(),
        kind: ScannerDirtyUsageAcknowledgementKind::Generation(11),
    };
    let unsaved = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot.clone()))
        .with_remote_dirty_usage_acknowledgements(vec![remote_acknowledgement.clone()]);
    let (outcome, _, acknowledgements) = finalize_scanner_cycle_result(unsaved, DataUsagePersistOutcome::NoUpdate.into());
    assert_eq!(outcome, ScannerCycleOutcome::Failed);
    assert!(acknowledgements.is_empty());
    assert!(crate::scanner_io::dirty_usage_buckets_pending());

    let saved = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot))
        .with_remote_dirty_usage_acknowledgements(vec![remote_acknowledgement]);
    let (outcome, pending, acknowledgements) = finalize_scanner_cycle_result(saved, DataUsagePersistOutcome::Saved.into());
    assert_eq!(outcome, ScannerCycleOutcome::Completed);
    assert!(acknowledgements.is_empty());
    assert!(pending);
    assert!(crate::scanner_io::dirty_usage_buckets_pending());
    crate::scanner_io::clear_dirty_usage_bucket("photos");
}

#[test]
#[serial]
fn finalizing_a_deferred_usage_save_keeps_dirty_work_pending() {
    crate::scanner_io::clear_dirty_usage_bucket("photos");
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();
    let deferred = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot));

    let (outcome, _, acknowledgements) =
        finalize_scanner_cycle_result(deferred, DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement).into());

    assert_eq!(outcome, ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement));
    assert!(acknowledgements.is_empty());
    assert!(crate::scanner_io::dirty_usage_buckets_pending());
    crate::scanner_io::clear_dirty_usage_bucket("photos");
}

#[test]
#[serial]
fn finalizing_post_scan_observation_advances_partially_without_dirty_ack() {
    crate::scanner_io::clear_dirty_usage_bucket("photos");
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();
    let observed = crate::scanner_io::ScannerCycleResult::new(
        ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable),
        Some(dirty_snapshot),
    )
    .with_observational_snapshot_published(true);

    let (outcome, _, acknowledgements) = finalize_scanner_cycle_result(observed, DataUsagePersistOutcome::Saved.into());

    assert_eq!(outcome, ScannerCycleOutcome::Partial);
    assert!(acknowledgements.is_empty());
    assert!(crate::scanner_io::dirty_usage_buckets_pending());
    crate::scanner_io::clear_dirty_usage_bucket("photos");
}

#[tokio::test]
async fn scanner_cycle_keeps_remote_pending_acknowledgement() {
    let pending = remote_dirty_usage_acknowledgement_pending(7, 1, std::future::ready(Ok::<bool, std::io::Error>(true))).await;
    assert_eq!(
        scanner_cycle_outcome_with_pending_maintenance(ScannerCycleOutcome::Completed, pending),
        ScannerCycleOutcome::CompletedWithPendingMaintenance
    );

    let cleared = remote_dirty_usage_acknowledgement_pending(7, 1, std::future::ready(Ok::<bool, std::io::Error>(false))).await;
    assert_eq!(
        scanner_cycle_outcome_with_pending_maintenance(ScannerCycleOutcome::Completed, cleared),
        ScannerCycleOutcome::Completed
    );

    let failed = remote_dirty_usage_acknowledgement_pending(
        7,
        1,
        std::future::ready(Err::<bool, _>(std::io::Error::other("injected acknowledgement failure"))),
    )
    .await;
    assert_eq!(
        scanner_cycle_outcome_with_pending_maintenance(ScannerCycleOutcome::Completed, failed),
        ScannerCycleOutcome::CompletedWithPendingMaintenance
    );
}

#[test]
#[serial]
fn finalizing_an_already_durable_enum_without_proof_keeps_dirty_pending() {
    crate::scanner_io::clear_dirty_usage_bucket("photos");
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

    let durable = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot));
    let (outcome, pending, acknowledgements) =
        finalize_scanner_cycle_result(durable, DataUsagePersistOutcome::AlreadyDurable.into());

    assert_eq!(outcome, ScannerCycleOutcome::Completed);
    assert!(acknowledgements.is_empty());
    assert!(pending);
    assert!(crate::scanner_io::dirty_usage_buckets_pending());
    crate::scanner_io::clear_dirty_usage_bucket("photos");
}

#[test]
#[serial]
fn finalizing_a_prior_same_cycle_snapshot_keeps_new_dirty_work_pending() {
    crate::scanner_io::clear_dirty_usage_bucket("photos");
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

    let durable = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot));
    let (outcome, _, acknowledgements) =
        finalize_scanner_cycle_result(durable, DataUsagePersistOutcome::PriorCycleDurable.into());

    assert_eq!(outcome, ScannerCycleOutcome::Completed);
    assert!(acknowledgements.is_empty());
    assert!(crate::scanner_io::dirty_usage_buckets_pending());
    crate::scanner_io::clear_dirty_usage_bucket("photos");
}

#[test]
#[serial]
fn finalizing_a_durable_superseded_snapshot_keeps_dirty_work_pending() {
    crate::scanner_io::clear_dirty_usage_bucket("photos");
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

    let superseded = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Superseded, Some(dirty_snapshot));
    let (outcome, _, acknowledgements) = finalize_scanner_cycle_result(superseded, DataUsagePersistOutcome::Saved.into());

    assert_eq!(outcome, ScannerCycleOutcome::Superseded);
    assert!(acknowledgements.is_empty());
    assert!(crate::scanner_io::dirty_usage_buckets_pending());
    crate::scanner_io::clear_dirty_usage_bucket("photos");
}

#[test]
#[serial]
fn data_usage_persist_wait_covers_cache_retries_and_backup() {
    with_var(rustfs_config::ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, Some("7"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(data_usage_persist_timeout(), Duration::from_millis(31_350));
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
#[serial]
fn default_data_usage_persist_wait_fits_publication_lease_window() {
    with_var_unset(rustfs_config::ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        let effective_publication_lease_window =
            Duration::from_millis(crate::storage_api::ECSTORE_SCANNER_PUBLICATION_LEASE_TTL_MS)
                .saturating_sub(Duration::from_secs(5));

        assert_eq!(data_usage_persist_timeout(), Duration::from_millis(52_350));
        assert!(data_usage_persist_timeout() < effective_publication_lease_window);
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[tokio::test]
async fn data_usage_persist_wait_aborts_when_scanner_is_cancelled() {
    let ctx = CancellationToken::new();
    let mut task = AbortOnDropHandle::new(tokio::spawn(async {
        std::future::pending::<()>().await;
        DataUsagePersistOutcome::Saved
    }));
    ctx.cancel();

    let result = wait_for_data_usage_persist_task(&ctx, &mut task, Duration::from_secs(60)).await;

    assert!(matches!(result, DataUsagePersistTaskResult::Cancelled));
    assert!(task.is_finished());
}

#[tokio::test(start_paused = true)]
async fn data_usage_persist_wait_aborts_after_timeout() {
    let ctx = CancellationToken::new();
    let mut task = AbortOnDropHandle::new(tokio::spawn(async {
        std::future::pending::<()>().await;
        DataUsagePersistOutcome::Saved
    }));

    let result = wait_for_data_usage_persist_task(&ctx, &mut task, Duration::from_secs(30)).await;

    assert!(matches!(result, DataUsagePersistTaskResult::TimedOut));
    assert!(task.is_finished());
}

#[tokio::test(start_paused = true)]
async fn data_usage_persist_timeout_drops_owned_task_without_a_late_commit() {
    let ctx = CancellationToken::new();
    let commit_started = Arc::new(AtomicBool::new(false));
    let commit_started_by_task = commit_started.clone();
    let task_ready = Arc::new(tokio::sync::Notify::new());
    let task_ready_by_task = task_ready.clone();
    let mut task = AbortOnDropHandle::new(tokio::spawn(async move {
        task_ready_by_task.notify_one();
        std::future::pending::<()>().await;
        commit_started_by_task.store(true, Ordering::Release);
        DataUsagePersistOutcome::Saved
    }));
    task_ready.notified().await;

    let result = wait_for_data_usage_persist_task(&ctx, &mut task, Duration::from_secs(1)).await;

    assert!(matches!(result, DataUsagePersistTaskResult::TimedOut));
    assert!(task.is_finished(), "the timed-out persistence task must be drained before return");
    tokio::task::yield_now().await;
    assert!(!commit_started.load(Ordering::Acquire), "an owned task must not commit after its timeout");
}

#[tokio::test(start_paused = true)]
async fn maintenance_feature_inspection_preserves_base_cycle_after_timeout() {
    let ctx = CancellationToken::new();

    let result = wait_for_maintenance_feature_inspection(
        &ctx,
        std::future::pending::<ScannerMaintenanceFeatures>(),
        Duration::from_secs(30),
    )
    .await;

    assert_eq!(result, MaintenanceInspectionAttempt::TimedOut);
}

#[tokio::test(start_paused = true)]
#[serial]
async fn stable_maintenance_detection_preserves_base_cycle_after_timeout() {
    let ctx = CancellationToken::new();

    let (features, generation) = detect_stable_scanner_maintenance_features_with(
        &ctx,
        std::future::pending::<ScannerMaintenanceFeatures>,
        Duration::from_secs(30),
    )
    .await
    .expect("timeout should preserve the scanner rather than stop it");

    assert!(features.inspection_failed);
    assert_eq!(generation, scanner_maintenance_generation());
    assert!(!scanner_clean_idle_backoff_enabled(
        true,
        true,
        features,
        &ScannerRuntimeConfig::default()
    ));
}

#[tokio::test(start_paused = true)]
async fn failed_maintenance_inspection_uses_bounded_retry_backoff() {
    let failed = ScannerMaintenanceFeatures {
        inspection_failed: true,
        ..Default::default()
    };
    let mut retry = ScannerMaintenanceInspectionRetry::from_features(failed, Instant::now());

    assert_eq!(retry.retry_interval(), Some(MAINTENANCE_FEATURE_INSPECTION_RETRY_BASE_INTERVAL));
    assert!(!retry.retry_due(failed, ScannerCycleWakeReason::Timer, Instant::now()));
    tokio::time::advance(MAINTENANCE_FEATURE_INSPECTION_RETRY_BASE_INTERVAL).await;
    assert!(retry.retry_due(failed, ScannerCycleWakeReason::Timer, Instant::now()));
    assert!(!retry.retry_due(failed, ScannerCycleWakeReason::DirtyUsage, Instant::now()));

    retry.record_inspection(failed, Instant::now());
    assert_eq!(
        retry.retry_interval(),
        Some(MAINTENANCE_FEATURE_INSPECTION_RETRY_BASE_INTERVAL.saturating_mul(2))
    );
    for _ in 0..8 {
        retry.record_inspection(failed, Instant::now());
    }
    assert_eq!(retry.retry_interval(), Some(MAINTENANCE_FEATURE_INSPECTION_RETRY_MAX_INTERVAL));

    retry.record_inspection(ScannerMaintenanceFeatures::default(), Instant::now());
    assert_eq!(retry, ScannerMaintenanceInspectionRetry::default());
}

#[tokio::test]
async fn maintenance_feature_inspection_stops_on_cancellation() {
    let ctx = CancellationToken::new();
    ctx.cancel();

    let result = wait_for_maintenance_feature_inspection(
        &ctx,
        std::future::pending::<ScannerMaintenanceFeatures>(),
        Duration::from_secs(30),
    )
    .await;

    assert_eq!(result, MaintenanceInspectionAttempt::Cancelled);
}

#[test]
#[serial]
fn test_cycle_interval_prefers_explicit_cycle_override() {
    with_var(ENV_SCANNER_SPEED, Some("slowest"), || {
        with_var(ENV_SCANNER_CYCLE, Some("42"), || {
            assert_eq!(cycle_interval(), Duration::from_secs(42));
        });
    });
}

#[test]
#[serial]
fn test_cycle_interval_prefers_explicit_cycle_over_default_cycle() {
    let _guard = ScannerDefaultCycleGuard::set(TEST_DEFAULT_SCANNER_CYCLE_SECS);

    with_var(ENV_SCANNER_CYCLE, Some("42"), || {
        assert_eq!(cycle_interval(), Duration::from_secs(42));
    });
}

#[test]
#[serial]
fn test_cycle_interval_uses_scanner_default_speed_override_when_unconfigured() {
    let _guard = ScannerDefaultSpeedGuard::set(ScannerSpeed::Slowest);

    with_unset_scanner_timing_env(|| {
        assert_eq!(cycle_interval(), Duration::from_secs(30 * 60));
    });
}

#[test]
#[serial]
fn test_cycle_interval_prefers_explicit_speed_over_default_speed_override() {
    let _guard = ScannerDefaultSpeedGuard::set(ScannerSpeed::Slowest);

    with_var_unset(ENV_SCANNER_CYCLE, || {
        with_var_unset("MINIO_SCANNER_CYCLE", || {
            with_var_unset(ENV_SCANNER_START_DELAY_SECS, || {
                with_var_unset(ENV_SCANNER_START_DELAY_SECS_DEPRECATED, || {
                    with_var(ENV_SCANNER_SPEED, Some("fastest"), || {
                        assert_eq!(cycle_interval(), Duration::from_secs(1));
                    });
                });
            });
        });
    });
}

#[test]
#[serial]
fn test_cycle_interval_uses_default_cycle_override_when_unconfigured() {
    let _guard = ScannerDefaultCycleGuard::set(TEST_DEFAULT_SCANNER_CYCLE_SECS);

    with_unset_scanner_timing_env(|| {
        assert_eq!(cycle_interval(), Duration::from_secs(TEST_DEFAULT_SCANNER_CYCLE_SECS));
    });
}

#[test]
fn test_single_disk_default_speed_uses_regular_scanner_default() {
    assert_eq!(single_disk_default_speed(), ScannerSpeed::Default);
}

#[test]
fn test_maintenance_feature_inspection_is_bounded_and_conservative() {
    assert_eq!(maintenance_inspection_decision(1, 1, 1), MaintenanceInspectionDecision::Accept);
    assert_eq!(maintenance_inspection_decision(1, 2, 1), MaintenanceInspectionDecision::Retry);
    assert_eq!(
        maintenance_inspection_decision(1, 2, MAX_MAINTENANCE_FEATURE_INSPECTION_ATTEMPTS),
        MaintenanceInspectionDecision::PreserveBaseCycle
    );
}

#[test]
fn clean_idle_backoff_grows_to_cap() {
    let base_interval = Duration::from_secs(60);
    let max_interval = CLEAN_IDLE_MAX_INTERVAL;
    let mut backoff = ScannerCleanIdleBackoff::default();

    assert_eq!(backoff.effective_interval(base_interval, max_interval, true), Duration::from_secs(60));
    for expected_secs in [
        120, 240, 480, 960, 1_920, 3_840, 7_680, 15_360, 30_720, 61_440, 86_400, 86_400,
    ] {
        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
        assert_eq!(
            backoff.effective_interval(base_interval, max_interval, true),
            Duration::from_secs(expected_secs)
        );
    }
}

#[test]
fn superseded_retry_backoff_grows_caps_and_resets_after_convergence() {
    let mut backoff = ScannerRetryBackoff::default();
    assert_eq!(backoff.retry_interval(Duration::from_secs(24 * 60 * 60)), None);

    for expected in [5, 10, 20, 40, 80, 160, 320] {
        backoff.record_retryable_cycle(true);
        assert_eq!(
            backoff.retry_interval(Duration::from_secs(24 * 60 * 60)),
            Some(Duration::from_secs(expected))
        );
    }
    for _ in 0..20 {
        backoff.record_retryable_cycle(true);
    }
    assert_eq!(
        backoff.retry_interval(Duration::from_secs(24 * 60 * 60)),
        Some(Duration::from_secs(24 * 60 * 60))
    );

    backoff.record_retryable_cycle(false);
    assert_eq!(backoff.retry_interval(Duration::from_secs(24 * 60 * 60)), None);
}

#[test]
fn superseded_retry_backoff_respects_a_faster_configured_cycle() {
    let mut backoff = ScannerRetryBackoff::default();
    backoff.record_retryable_cycle(true);

    // A configured cycle shorter than the base still wins: retrying sooner
    // than the operator's own cadence buys nothing.
    assert_eq!(backoff.retry_interval(Duration::from_secs(3)), Some(Duration::from_secs(3)));
    backoff.record_retryable_cycle(true);
    assert_eq!(backoff.retry_interval(Duration::from_secs(3)), Some(Duration::from_secs(6)));
}

#[test]
fn superseded_retry_backoff_grows_from_the_default_cycle() {
    let mut backoff = ScannerRetryBackoff::default();
    // The first race after a write burst retries in seconds, not a whole
    // cycle, while repeated supersedes still climb toward the cap.
    for expected in [5, 10, 20, 40] {
        backoff.record_retryable_cycle(true);
        assert_eq!(backoff.retry_interval(Duration::from_secs(60)), Some(Duration::from_secs(expected)));
    }
}

#[test]
fn superseded_retry_preserves_explicit_cycle_cadence() {
    let mut backoff = ScannerRetryBackoff::default();
    backoff.record_retryable_cycle(true);

    let default_config = ScannerRuntimeConfig {
        cycle_interval: Duration::from_secs(6 * 60 * 60),
        ..Default::default()
    };
    assert_eq!(
        scanner_superseded_retry_interval(backoff, &default_config),
        Some(SCANNER_RETRY_BASE_INTERVAL),
        "the default adaptive cadence must retain fast convergence"
    );

    for source in [
        ScannerRuntimeConfigSource::Env,
        ScannerRuntimeConfigSource::Config,
        ScannerRuntimeConfigSource::ScannerCompatConfig,
    ] {
        let runtime_config = ScannerRuntimeConfig {
            cycle_interval: Duration::from_secs(6 * 60 * 60),
            cycle_interval_source: source,
            ..Default::default()
        };

        assert_eq!(
            scanner_superseded_retry_interval(backoff, &runtime_config),
            Some(runtime_config.cycle_interval),
            "{source:?} cycle cadence must not be shortened by convergence retries"
        );
    }
}

#[test]
fn publication_proof_retry_backoff_reaches_its_short_cap() {
    for (failures, expected) in [(1, 5), (2, 10), (3, 20), (4, 30), (20, 30)] {
        assert_eq!(scanner_publication_proof_retry_delay(failures), Duration::from_secs(expected));
    }
}

#[test]
fn publication_proof_retry_classifies_availability_without_masking_protocol_errors() {
    for error in [
        "peer node3 is temporarily offline",
        "scanner activity peer node3 timed out after 5s",
        "transport error: connection refused",
    ] {
        assert!(scanner_publication_activity_error_is_retryable(error), "{error}");
    }

    for error in [
        "scanner activity peer node3 uses protocol 6, expected 7",
        "scanner activity peer node3 has a different storage topology",
        "scanner activity peer node3 omitted its movement generation",
        "duplicate scanner activity peer: node3",
        "scanner activity peer[2] is unreachable",
        "scanner publication lease peer node3 is unavailable",
    ] {
        assert!(!scanner_publication_activity_error_is_retryable(error), "{error}");
    }
}

#[test]
fn publication_lease_retry_preserves_only_recoverable_candidates() {
    for error in [
        "scanner publication lease acquisition failed: scanner publication lease capacity is exhausted",
        "scanner publication lease acquisition failed: scanner publication lease response arrived after its safety window",
        "scanner publication lease acquisition failed: peer node3 is temporarily offline",
    ] {
        assert!(scanner_publication_lease_error_is_retryable(error), "{error}");
    }

    for error in [
        "scanner publication lease acquisition failed: scanner publication lease generation is stale",
        "scanner publication lease acquisition failed: peer returned a different scanner publication lease session",
        "scanner publication lease acquisition failed: scanner publication lease is blocked by data movement",
        "scanner publication lease acquisition failed: peer returned an invalid scanner publication lease proof",
    ] {
        assert!(!scanner_publication_lease_error_is_retryable(error), "{error}");
    }
}

#[tokio::test(start_paused = true)]
async fn publication_proof_retains_candidate_until_activity_recovers() {
    let ctx = CancellationToken::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let probe_attempts = attempts.clone();
    let started_at = Instant::now();

    let snapshot = await_scanner_publication_activity(&ctx, 17, "postscan", move || {
        let attempt = probe_attempts.fetch_add(1, Ordering::SeqCst);
        async move {
            if attempt == 0 {
                Err("peer temporarily offline".to_string())
            } else {
                Ok(ScannerActivitySnapshot::new())
            }
        }
    })
    .await
    .expect("a retained publication candidate should survive one transient probe failure");

    assert!(snapshot.is_empty());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(started_at.elapsed(), Duration::from_secs(5));
}

#[tokio::test(start_paused = true)]
async fn publication_proof_does_not_retry_a_protocol_mismatch() {
    let ctx = CancellationToken::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let probe_attempts = attempts.clone();

    let err = await_scanner_publication_activity(&ctx, 17, "postscan", move || {
        probe_attempts.fetch_add(1, Ordering::SeqCst);
        async { Err("scanner activity peer node3 uses protocol 6, expected 7".to_string()) }
    })
    .await
    .expect_err("a protocol mismatch must not be hidden behind availability retries");

    assert!(err.contains("uses protocol"));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn publication_proof_stops_waiting_when_the_cycle_is_cancelled() {
    let ctx = CancellationToken::new();
    ctx.cancel();
    let attempts = Arc::new(AtomicUsize::new(0));
    let probe_attempts = attempts.clone();

    let err = await_scanner_publication_activity(&ctx, 17, "postscan", move || {
        probe_attempts.fetch_add(1, Ordering::SeqCst);
        async { Ok(ScannerActivitySnapshot::new()) }
    })
    .await
    .expect_err("a cancelled cycle must release its retained publication candidate");

    assert!(err.contains("cancelled"));
    assert_eq!(attempts.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn publication_proof_releases_candidate_when_cancelled_during_backoff() {
    let ctx = CancellationToken::new();
    let cancel_ctx = ctx.clone();
    let attempts = Arc::new(AtomicUsize::new(0));
    let probe_attempts = attempts.clone();
    let started_at = Instant::now();

    let cancel = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        cancel_ctx.cancel();
    });
    let err = await_scanner_publication_activity(&ctx, 17, "postscan", move || {
        probe_attempts.fetch_add(1, Ordering::SeqCst);
        async { Err("peer temporarily offline".to_string()) }
    })
    .await
    .expect_err("cycle cancellation must release a candidate waiting to retry publication proof");
    cancel.await.expect("cancellation task should complete");

    assert!(err.contains("cancelled"));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(started_at.elapsed(), Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn corrupt_cycle_state_backoff_uses_virtual_clock() {
    let mut backoff = ScannerRetryBackoff::default();
    backoff.record_retryable_cycle(true);
    let first_delay = backoff
        .retry_interval(Duration::from_secs(60))
        .expect("the first recovery retry should be scheduled");
    assert_eq!(first_delay, Duration::from_secs(5));

    let deadline = Instant::now() + first_delay;
    assert!(Instant::now() < deadline);
    tokio::time::advance(first_delay).await;
    assert!(Instant::now() >= deadline);

    backoff.record_retryable_cycle(true);
    assert_eq!(backoff.retry_interval(Duration::from_secs(60)), Some(Duration::from_secs(10)));
}

#[test]
fn scanner_cycle_wait_plan_drives_growth_resets_and_bitrot_cap() {
    let runtime_config = ScannerRuntimeConfig {
        cycle_interval: Duration::from_secs(60),
        bitrot_cycle: None,
        ..Default::default()
    };
    let mut clean_idle_backoff = ScannerCleanIdleBackoff::default();

    let plan = scanner_cycle_wait_plan(&runtime_config, clean_idle_backoff, true, std::convert::identity);
    assert_eq!(plan.delay, Duration::from_secs(60));

    for expected in [120, 240] {
        record_scanner_cycle_result(
            &mut clean_idle_backoff,
            &runtime_config,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
        let plan = scanner_cycle_wait_plan(&runtime_config, clean_idle_backoff, true, std::convert::identity);
        assert_eq!(plan.delay, Duration::from_secs(expected));
    }

    for (wake_reason, outcome, dirty_work_observed) in [
        (ScannerCycleWakeReason::Timer, ScannerCycleOutcome::Completed, true),
        (ScannerCycleWakeReason::Timer, ScannerCycleOutcome::Partial, false),
        (ScannerCycleWakeReason::Timer, ScannerCycleOutcome::Failed, false),
        (ScannerCycleWakeReason::Timer, ScannerCycleOutcome::CompletedWithPendingMaintenance, false),
        (ScannerCycleWakeReason::DirtyUsage, ScannerCycleOutcome::Completed, false),
    ] {
        record_scanner_cycle_result(&mut clean_idle_backoff, &runtime_config, true, wake_reason, outcome, dirty_work_observed);
        let plan = scanner_cycle_wait_plan(&runtime_config, clean_idle_backoff, true, std::convert::identity);
        assert_eq!(plan.effective_interval, Duration::from_secs(60));
        assert_eq!(plan.delay, Duration::from_secs(60));

        record_scanner_cycle_result(
            &mut clean_idle_backoff,
            &runtime_config,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
    }

    clean_idle_backoff.reset();
    for _ in 0..32 {
        record_scanner_cycle_result(
            &mut clean_idle_backoff,
            &runtime_config,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
    }
    let plan = scanner_cycle_wait_plan(&runtime_config, clean_idle_backoff, true, |interval| interval.mul_f64(1.1));
    assert_eq!(plan.effective_interval, CLEAN_IDLE_MAX_INTERVAL);
    assert!(plan.delay < CLEAN_IDLE_MAX_INTERVAL);
    assert_eq!(
        plan.delay,
        CLEAN_IDLE_MAX_INTERVAL.saturating_sub(CLEAN_IDLE_MAX_INTERVAL.mul_f64(1.1) - CLEAN_IDLE_MAX_INTERVAL)
    );
}

#[test]
#[serial]
fn scanner_cycle_schedule_status_reports_effective_backoff() {
    record_scanner_cycle_schedule(Duration::from_millis(86_400_001), true, 2_048, true, 7);

    let status = scanner_cycle_schedule_status();

    assert_eq!(status.execution_role, "leader");
    assert!(status.effective_interval_available);
    assert_eq!(status.effective_interval_seconds, 86_401);
    assert!(status.clean_idle_backoff_enabled);
    assert_eq!(status.clean_idle_backoff_multiplier, 2_048);
    assert!(status.superseded_retry_backoff_enabled);
    assert_eq!(status.superseded_cycles, 7);

    record_scanner_cycle_schedule_role("follower");
    let status = scanner_cycle_schedule_status();
    assert_eq!(status.execution_role, "follower");
    assert!(!status.effective_interval_available);
    assert_eq!(status.effective_interval_seconds, 0);

    reset_scanner_cycle_schedule();
    let status = scanner_cycle_schedule_status();
    assert_eq!(status.execution_role, "unknown");
    assert!(!status.effective_interval_available);
    assert_eq!(status.effective_interval_seconds, 0);
    assert!(!status.clean_idle_backoff_enabled);
    assert_eq!(status.clean_idle_backoff_multiplier, 1);
    assert!(!status.superseded_retry_backoff_enabled);
    assert_eq!(status.superseded_cycles, 0);
}

#[test]
fn scanner_leader_lock_failure_classifies_only_timeout_as_expected_contention() {
    let timeout = LockError::timeout(".rustfs.sys/leader.lock@latest", Duration::from_secs(5));
    assert!(matches!(
        classify_scanner_leader_lock_failure(&timeout),
        ScannerLeaderLockFailure::Contended
    ));

    let failures = [
        LockError::internal("lock service unavailable"),
        LockError::network(
            "leader lock transport unavailable",
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused"),
        ),
        LockError::QuorumNotReached {
            required: 3,
            achieved: 1,
        },
    ];
    for failure in &failures {
        assert!(matches!(
            classify_scanner_leader_lock_failure(failure),
            ScannerLeaderLockFailure::Failed(_)
        ));
    }
}

#[test]
fn clean_idle_backoff_resets_for_non_idle_work() {
    let base_interval = Duration::from_secs(60);
    let max_interval = CLEAN_IDLE_MAX_INTERVAL;
    let mut backoff = ScannerCleanIdleBackoff::default();

    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        false,
    );
    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        false,
    );
    assert_eq!(backoff.effective_interval(base_interval, max_interval, true), Duration::from_secs(240));

    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::DirtyUsage,
        ScannerCycleOutcome::Completed,
        false,
    );
    assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);

    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        false,
    );
    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Partial,
        false,
    );
    assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);

    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        false,
    );
    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Failed,
        false,
    );
    assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);

    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        false,
    );
    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        true,
    );
    assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);

    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        false,
    );
    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::CompletedWithPendingMaintenance,
        false,
    );
    assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);
}

#[test]
fn test_dirty_work_is_observed_across_cycle_waits() {
    assert!(scanner_cycle_observed_dirty_work(true, 7, 7));
    assert!(scanner_cycle_observed_dirty_work(false, 7, 8));
    assert!(!scanner_cycle_observed_dirty_work(false, 7, 7));
}

#[test]
fn clean_idle_backoff_never_shortens_base_interval() {
    let base_interval = Duration::from_secs(48 * 60 * 60);
    let mut backoff = ScannerCleanIdleBackoff::default();

    backoff.record_cycle(
        base_interval,
        CLEAN_IDLE_MAX_INTERVAL,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        false,
    );

    assert_eq!(backoff.effective_interval(base_interval, CLEAN_IDLE_MAX_INTERVAL, true), base_interval);
}

#[test]
fn clean_idle_backoff_resets_while_disabled() {
    let base_interval = Duration::from_secs(60);
    let max_interval = CLEAN_IDLE_MAX_INTERVAL;
    let mut backoff = ScannerCleanIdleBackoff::default();

    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        false,
    );
    backoff.record_cycle(
        base_interval,
        max_interval,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        false,
    );
    assert_eq!(backoff.effective_interval(base_interval, max_interval, true), Duration::from_secs(240));

    backoff.record_cycle(
        base_interval,
        max_interval,
        false,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        false,
    );

    assert_eq!(backoff.effective_interval(base_interval, max_interval, false), base_interval);
    assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);
}

#[test]
fn clean_idle_backoff_policy_preserves_explicit_and_maintenance_cycles() {
    let no_features = ScannerMaintenanceFeatures::default();
    let default_config = ScannerRuntimeConfig::default();
    assert!(scanner_clean_idle_backoff_enabled(true, true, no_features, &default_config));
    assert!(!scanner_clean_idle_backoff_enabled(false, true, no_features, &default_config));
    assert!(!scanner_clean_idle_backoff_enabled(true, false, no_features, &default_config));

    for source in [ScannerRuntimeConfigSource::Env, ScannerRuntimeConfigSource::Config] {
        let mut config = default_config.clone();
        config.cycle_interval_source = source;
        assert!(!scanner_clean_idle_backoff_enabled(true, true, no_features, &config));
    }

    for source in [
        ScannerRuntimeConfigSource::Env,
        ScannerRuntimeConfigSource::Config,
        ScannerRuntimeConfigSource::ScannerCompatConfig,
    ] {
        let mut explicit_bitrot_config = default_config.clone();
        explicit_bitrot_config.bitrot_cycle = Some(Duration::from_secs(60 * 60));
        explicit_bitrot_config.bitrot_cycle_source = source;
        assert!(!scanner_clean_idle_backoff_enabled(true, true, no_features, &explicit_bitrot_config));

        explicit_bitrot_config.bitrot_cycle = None;
        assert!(scanner_clean_idle_backoff_enabled(true, true, no_features, &explicit_bitrot_config));
    }

    for features in [
        ScannerMaintenanceFeatures {
            lifecycle: true,
            ..Default::default()
        },
        ScannerMaintenanceFeatures {
            replication: true,
            ..Default::default()
        },
        ScannerMaintenanceFeatures {
            inspection_failed: true,
            ..Default::default()
        },
    ] {
        assert!(!scanner_clean_idle_backoff_enabled(true, true, features, &default_config));
    }
}

#[tokio::test]
#[serial]
async fn scoped_scan_explicit_bitrot_keeps_dirty_planning_without_idle_backoff() {
    temp_env::async_with_vars([(ENV_SCANNER_CYCLE, None), (ENV_SCANNER_BITROT_CYCLE_SECS, Some("3600"))], async {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let (_temp_dir, store) = setup_scanner_cycle_store_with_pool_count(true, 2).await;
        let ctx = CancellationToken::new();
        let (features, generation) = configure_scanner_defaults(&ctx, &store).await;
        let config = resolve_scanner_runtime_config();
        assert_eq!(config.cycle_interval_source, ScannerRuntimeConfigSource::Default);
        assert_eq!(config.bitrot_cycle_source, ScannerRuntimeConfigSource::Env);
        assert!(!scanner_clean_idle_backoff_configured(&config));
        assert!(!features.needs_regular_cycle());
        assert_eq!(
            generation,
            Some(scanner_maintenance_generation()),
            "multi-disk startup must inspect maintenance independently"
        );
        let observed = ScannerCycleObservedGenerations::for_wait(&config, None, 7, 0, scanner_maintenance_generation());
        assert_eq!(observed.dirty_usage, Some(7), "explicit bitrot still permits ordinary dirty wakeups");
        for (wake, full) in [
            (ScannerCycleWakeReason::DirtyUsage, false),
            (ScannerCycleWakeReason::ClusterActivity, false),
            (ScannerCycleWakeReason::Timer, true),
            (ScannerCycleWakeReason::ClusterMaintenance, true),
        ] {
            assert_eq!(
                features.requires_full_scan(generation, scanner_maintenance_generation(), wake),
                full,
                "{wake:?}"
            );
        }
        assert!(features.requires_full_scan(None, scanner_maintenance_generation(), ScannerCycleWakeReason::DirtyUsage));
        for unsafe_features in [
            ScannerMaintenanceFeatures {
                lifecycle: true,
                ..Default::default()
            },
            ScannerMaintenanceFeatures {
                replication: true,
                ..Default::default()
            },
            ScannerMaintenanceFeatures {
                inspection_failed: true,
                ..Default::default()
            },
        ] {
            assert!(unsafe_features.requires_full_scan(
                generation,
                scanner_maintenance_generation(),
                ScannerCycleWakeReason::DirtyUsage
            ));
        }
        crate::scanner_io::record_scanner_maintenance_change("maintenance-proof-change");
        assert!(features.requires_full_scan(generation, scanner_maintenance_generation(), ScannerCycleWakeReason::DirtyUsage));
        let (refreshed, refreshed_generation) = detect_stable_scanner_maintenance_features(&ctx, &store)
            .await
            .expect("changed maintenance generation should be inspected");
        assert!(!refreshed.requires_full_scan(
            Some(refreshed_generation),
            scanner_maintenance_generation(),
            ScannerCycleWakeReason::DirtyUsage
        ));
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    })
    .await;
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
fn clean_idle_backoff_requires_activity_probes() {
    let default_config = ScannerRuntimeConfig::default();
    let no_features = ScannerMaintenanceFeatures::default();
    assert!(scanner_activity_probe_required(true, false, no_features, &default_config));
    assert!(!scanner_activity_probe_required(false, false, no_features, &default_config));
    assert!(!scanner_activity_probe_required(true, true, no_features, &default_config));

    let mut explicit_cycle = default_config.clone();
    explicit_cycle.cycle_interval_source = ScannerRuntimeConfigSource::Env;
    assert!(!scanner_activity_probe_required(true, false, no_features, &explicit_cycle));

    let lifecycle = ScannerMaintenanceFeatures {
        lifecycle: true,
        ..Default::default()
    };
    assert!(!scanner_activity_probe_required(true, false, lifecycle, &default_config));
}

#[test]
fn dirty_usage_wakes_are_disabled_for_explicit_cycle_policy() {
    let default_config = ScannerRuntimeConfig::default();

    let default_observed = ScannerCycleObservedGenerations::for_wait(&default_config, None, 7, 11, 13);
    assert_eq!(default_observed.dirty_usage, Some(7));
    assert_eq!(default_observed.runtime_config, 11);
    assert_eq!(default_observed.maintenance, 13);
    assert!(!default_observed.defer_cluster_activity);

    let retry_observed = ScannerCycleObservedGenerations::for_wait(&default_config, Some(Duration::from_secs(11)), 7, 11, 13);
    assert_eq!(retry_observed.dirty_usage, None);
    assert!(retry_observed.defer_cluster_activity);

    for source in [ScannerRuntimeConfigSource::Env, ScannerRuntimeConfigSource::Config] {
        let explicit_cycle = ScannerRuntimeConfig {
            cycle_interval_source: source,
            ..default_config.clone()
        };
        let explicit_observed = ScannerCycleObservedGenerations::for_wait(&explicit_cycle, None, 7, 11, 13);
        assert_eq!(explicit_observed.dirty_usage, None);
        assert!(!explicit_observed.defer_cluster_activity);
    }
}

#[test]
#[serial]
fn clean_idle_cap_preserves_default_bitrot_coverage_window() {
    let config = ScannerRuntimeConfig {
        bitrot_cycle: Some(Duration::from_secs(30 * 24 * 60 * 60)),
        bitrot_cycle_source: ScannerRuntimeConfigSource::Default,
        ..Default::default()
    };

    with_var("RUSTFS_HEAL_OBJECT_SELECT_PROB", Some("1024"), || {
        let max_interval = scanner_clean_idle_max_interval(Duration::from_secs(60), &config);
        assert_eq!(max_interval, Duration::from_millis(2_531_250));
        let positive_jitter = max_interval.mul_f64(1.1);
        let actual_delay = cap_clean_idle_cycle_delay(positive_jitter, max_interval, true);
        assert!(actual_delay < max_interval);
        assert_eq!(actual_delay, max_interval.saturating_sub(positive_jitter - max_interval));
        assert!(actual_delay.saturating_mul(1024) <= config.bitrot_cycle.expect("bitrot cycle should be configured"));
    });
}

#[test]
fn clean_idle_cap_allows_policy_max_when_bitrot_is_disabled() {
    let config = ScannerRuntimeConfig {
        bitrot_cycle: None,
        ..Default::default()
    };

    assert_eq!(scanner_clean_idle_max_interval(Duration::from_secs(60), &config), CLEAN_IDLE_MAX_INTERVAL);
}

#[test]
#[serial]
fn clean_idle_cap_never_shortens_the_base_cycle() {
    let config = ScannerRuntimeConfig {
        bitrot_cycle: Some(Duration::from_secs(60)),
        bitrot_cycle_source: ScannerRuntimeConfigSource::Default,
        ..Default::default()
    };

    with_var("RUSTFS_HEAL_OBJECT_SELECT_PROB", Some("1024"), || {
        assert_eq!(scanner_clean_idle_max_interval(Duration::from_secs(60), &config), Duration::from_secs(60));
    });
}

#[test]
#[serial]
fn test_cycle_interval_keeps_default_cycle_with_explicit_speed() {
    let _guard = ScannerDefaultCycleGuard::set(TEST_DEFAULT_SCANNER_CYCLE_SECS);

    with_var_unset(ENV_SCANNER_CYCLE, || {
        with_var_unset("MINIO_SCANNER_CYCLE", || {
            with_var_unset(ENV_SCANNER_START_DELAY_SECS, || {
                with_var_unset(ENV_SCANNER_START_DELAY_SECS_DEPRECATED, || {
                    with_var(ENV_SCANNER_SPEED, Some("slowest"), || {
                        assert_eq!(cycle_interval(), Duration::from_secs(TEST_DEFAULT_SCANNER_CYCLE_SECS));
                    });
                });
            });
        });
    });
}

#[test]
#[serial]
fn test_cycle_interval_prefers_explicit_start_delay_over_default_cycle() {
    let _guard = ScannerDefaultCycleGuard::set(TEST_DEFAULT_SCANNER_CYCLE_SECS);

    with_var_unset(ENV_SCANNER_CYCLE, || {
        with_var_unset("MINIO_SCANNER_CYCLE", || {
            with_var(ENV_SCANNER_START_DELAY_SECS, Some("120"), || {
                assert_eq!(cycle_interval(), Duration::from_secs(120));
            });
        });
    });
}

#[test]
#[serial]
fn test_cycle_interval_supports_minio_speed_alias() {
    with_var_unset(ENV_SCANNER_SPEED, || {
        with_var_unset(ENV_SCANNER_CYCLE, || {
            with_var_unset(ENV_SCANNER_START_DELAY_SECS, || {
                with_var("MINIO_SCANNER_SPEED", Some("slowest"), || {
                    assert_eq!(cycle_interval(), Duration::from_secs(30 * 60));
                });
            });
        });
    });
}

#[test]
#[serial]
fn test_cycle_interval_supports_minio_cycle_alias() {
    with_var_unset(ENV_SCANNER_CYCLE, || {
        with_var_unset(ENV_SCANNER_START_DELAY_SECS, || {
            with_var("MINIO_SCANNER_CYCLE", Some("90"), || {
                assert_eq!(cycle_interval(), Duration::from_secs(90));
            });
        });
    });
}

#[test]
fn test_randomized_cycle_delay_handles_small_start_delay() {
    // 0 is treated as minimum 1 second before jitter, with lower bound preserved.
    let delay = randomized_cycle_delay_for(Duration::from_secs(0));
    assert!(delay >= Duration::from_secs(1), "expected delay >= 1s");
    assert!(delay < Duration::from_secs(2), "expected delay < 2s");
}

#[tokio::test]
#[serial]
async fn test_wait_for_next_scanner_cycle_wakes_for_dirty_usage() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();

    let ctx = CancellationToken::new();
    let dirty_generation = crate::scanner_io::dirty_usage_generation();
    let mut wait = Box::pin(wait_for_next_scanner_cycle(
        &ctx,
        Duration::from_secs(60),
        Some(dirty_generation),
        crate::runtime_config::scanner_runtime_config_generation(),
        crate::scanner_io::scanner_maintenance_generation(),
        || false,
    ));
    assert!(matches!(futures::poll!(&mut wait), Poll::Pending));

    crate::scanner_io::record_dirty_usage_bucket("photos");
    let reason = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("dirty usage should wake scanner before timer");

    assert_eq!(reason, ScannerCycleWakeReason::DirtyUsage);
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test(start_paused = true)]
#[serial]
async fn service_cohort_aging_preserves_explicit_cycle_wait() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let config = ScannerRuntimeConfig {
        cycle_interval: Duration::from_secs(3600),
        cycle_interval_source: ScannerRuntimeConfigSource::Env,
        ..Default::default()
    };
    let observed = ScannerCycleObservedGenerations::for_wait(
        &config,
        None,
        crate::scanner_io::dirty_usage_generation(),
        crate::runtime_config::scanner_runtime_config_generation(),
        crate::scanner_io::scanner_maintenance_generation(),
    );
    assert_eq!(observed.dirty_usage, None);
    let inventory = HashMap::from([(
        crate::data_usage_define::DataUsageCacheSource::new(0, 0),
        vec![crate::storage_api::scanner_io::BucketInfo {
            name: "waiting-bootstrap".to_string(),
            ..Default::default()
        }],
    )]);
    let mut cohort = crate::scanner_io::ScannerServiceCohort::default();
    cohort.refresh(&inventory);
    let ctx = CancellationToken::new();
    let mut wait = Box::pin(wait_for_next_scanner_cycle(
        &ctx,
        config.cycle_interval,
        observed.dirty_usage,
        observed.runtime_config,
        observed.maintenance,
        || false,
    ));
    assert!(matches!(futures::poll!(&mut wait), Poll::Pending));
    for _ in 0..59 {
        tokio::time::advance(Duration::from_secs(60)).await;
        cohort.refresh(&inventory);
        crate::scanner_io::record_dirty_usage_bucket("hot");
        assert!(
            matches!(futures::poll!(&mut wait), Poll::Pending),
            "aging/dirty must not shorten the explicit hour"
        );
    }
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(wait.await, ScannerCycleWakeReason::Timer);
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn test_wait_for_next_scanner_cycle_sees_unattempted_dirty_usage() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let dirty_generation = crate::scanner_io::dirty_usage_generation();
    crate::scanner_io::record_dirty_usage_bucket("photos");

    let ctx = CancellationToken::new();
    let reason = wait_for_next_scanner_cycle(
        &ctx,
        Duration::from_secs(60),
        Some(dirty_generation),
        crate::runtime_config::scanner_runtime_config_generation(),
        crate::scanner_io::scanner_maintenance_generation(),
        || false,
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::DirtyUsage);
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test(start_paused = true)]
#[serial]
async fn test_wait_for_next_scanner_cycle_retries_stable_dirty_usage_on_timer() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_generation = crate::scanner_io::dirty_usage_generation();
    let ctx = CancellationToken::new();
    let wait = wait_for_next_scanner_cycle(
        &ctx,
        Duration::from_secs(60),
        Some(dirty_generation),
        crate::runtime_config::scanner_runtime_config_generation(),
        crate::scanner_io::scanner_maintenance_generation(),
        || false,
    );

    let reason = wait.await;

    assert_eq!(reason, ScannerCycleWakeReason::Timer);
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test(start_paused = true)]
#[serial]
async fn test_wait_for_next_scanner_cycle_can_defer_dirty_wakes_until_timer() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let ctx = CancellationToken::new();
    let wait = wait_for_next_scanner_cycle(
        &ctx,
        Duration::from_secs(60),
        None,
        crate::runtime_config::scanner_runtime_config_generation(),
        crate::scanner_io::scanner_maintenance_generation(),
        || false,
    );

    crate::scanner_io::record_dirty_usage_bucket("photos");
    assert_eq!(wait.await, ScannerCycleWakeReason::Timer);
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn test_wait_for_next_scanner_cycle_wakes_for_repeated_dirty_bucket() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_generation = crate::scanner_io::dirty_usage_generation();
    let ctx = CancellationToken::new();
    let mut wait = Box::pin(wait_for_next_scanner_cycle(
        &ctx,
        Duration::from_secs(60),
        Some(dirty_generation),
        crate::runtime_config::scanner_runtime_config_generation(),
        crate::scanner_io::scanner_maintenance_generation(),
        || false,
    ));
    assert!(matches!(futures::poll!(&mut wait), Poll::Pending));

    crate::scanner_io::record_dirty_usage_bucket("photos");
    let reason = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("a newer mutation of an already-dirty bucket should wake scanner");

    assert_eq!(reason, ScannerCycleWakeReason::DirtyUsage);
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn test_wait_for_next_scanner_cycle_reschedules_for_runtime_config() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let observed_generation = crate::runtime_config::scanner_runtime_config_generation();
    let ctx = CancellationToken::new();
    let mut wait = Box::pin(wait_for_next_scanner_cycle(
        &ctx,
        Duration::from_secs(60),
        Some(crate::scanner_io::dirty_usage_generation()),
        observed_generation,
        crate::scanner_io::scanner_maintenance_generation(),
        || false,
    ));
    assert!(matches!(futures::poll!(&mut wait), Poll::Pending));

    let mut config = rustfs_config::server_config::Config::new();
    config.set_defaults();
    crate::runtime_config::apply_scanner_runtime_config(&config).expect("default scanner config should apply");
    let reason = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("runtime config should wake scanner before timer");

    assert_eq!(reason, ScannerCycleWakeReason::RuntimeConfig);
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn test_wait_for_next_scanner_cycle_reschedules_for_maintenance_change() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let observed_generation = crate::scanner_io::scanner_maintenance_generation();
    let ctx = CancellationToken::new();
    let mut wait = Box::pin(wait_for_next_scanner_cycle(
        &ctx,
        Duration::from_secs(60),
        Some(crate::scanner_io::dirty_usage_generation()),
        crate::runtime_config::scanner_runtime_config_generation(),
        observed_generation,
        || false,
    ));
    assert!(matches!(futures::poll!(&mut wait), Poll::Pending));

    crate::scanner_io::record_scanner_maintenance_change("photos");
    let reason = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("maintenance change should wake scanner before timer");

    assert_eq!(reason, ScannerCycleWakeReason::MaintenanceConfig);
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
async fn test_wait_for_next_scanner_cycle_stops_after_leader_lock_loss() {
    let ctx = CancellationToken::new();
    let reason = wait_for_next_scanner_cycle(
        &ctx,
        Duration::from_secs(60),
        Some(crate::scanner_io::dirty_usage_generation()),
        crate::runtime_config::scanner_runtime_config_generation(),
        crate::scanner_io::scanner_maintenance_generation(),
        || true,
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::LeaderLockLost);
}

#[tokio::test]
async fn movement_generation_wakes_deferred_wait_without_dirty_bucket() {
    let ctx = CancellationToken::new();
    let movement_generation = Arc::new(AtomicU64::new(7));
    let movement_changed = Arc::new(Notify::new());
    let next_generation = Arc::clone(&movement_generation);
    let next_changed = Arc::clone(&movement_changed);
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        next_generation.store(8, Ordering::Release);
        next_changed.notify_waiters();
    });

    let movement = ScannerMovementWaitContext {
        movement_generation_seen: Some(7),
        movement_changed,
        current_movement_generation: move || movement_generation.load(Ordering::Acquire),
        is_lock_lost: || false,
    };
    let reason = wait_for_next_scanner_cycle_with_movement(
        &ctx,
        Duration::from_secs(60),
        ScannerCycleObservedGenerations {
            dirty_usage: None,
            runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
            maintenance: crate::scanner_io::scanner_maintenance_generation(),
            defer_cluster_activity: false,
        },
        &movement,
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::MovementGeneration);
}

fn scanner_node_activity(epoch: &str, namespace_generation: u64, maintenance_generation: u64) -> ScannerNodeActivity {
    ScannerNodeActivity {
        instance_id: epoch.to_string(),
        namespace_generation,
        maintenance_generation,
        protocol_version: SCANNER_ACTIVITY_PROTOCOL_VERSION,
        topology_digest: [3; 32],
        data_movement_active: false,
        dirty_usage_generation: 5,
        dirty_usage_pending: false,
        movement_generation: 9,
        publication_blocked: false,
    }
}

#[test]
fn scoped_scan_remote_dirty_coverage_invalidates_local_bucket_current() {
    let before = BTreeMap::from([("remote".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
    let mut after = before.clone();
    let remote = after.get_mut("remote").expect("remote activity should exist");
    remote.dirty_usage_generation += 1;
    remote.dirty_usage_pending = true;
    assert_eq!(scanner_activity_structural_digest(&before), scanner_activity_structural_digest(&after));
    let old_plan = crate::scanner_io::checkpoint_fixture_bucket_digest(
        DataUsageScanPlanDigest(scanner_activity_snapshot_digest(&before)),
        None,
    );
    let new_plan = crate::scanner_io::checkpoint_fixture_bucket_digest(
        DataUsageScanPlanDigest(scanner_activity_snapshot_digest(&after)),
        None,
    );
    assert_ne!(
        old_plan, new_plan,
        "remote dirty changes must fence Current even without a local bucket hint"
    );
    let source = DataUsageCacheSource::new(0, 0);
    let mut cache = DataUsageCache {
        info: crate::DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            leader_epoch: 11,
            source: Some(source),
            last_update: Some(std::time::SystemTime::UNIX_EPOCH),
            snapshot_complete: true,
            scan_plan_digest: Some(old_plan),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace("bucket", "", DataUsageEntry::default());
    assert!(matches!(
        crate::scanner_io::current_cache_root_or_prepare_with_generation(
            &mut cache,
            "bucket",
            source,
            7,
            11,
            new_plan,
            crate::scanner_io::DataUsageCacheReuseOptions {
                require_source: true,
                tier_registry_generation: None,
                checkpoint_identity: None,
            },
        ),
        crate::scanner_io::DataUsageCacheScanState::Prepared {
            outcome: DataUsageCachePrepareOutcome::Reset,
            ..
        }
    ));
}

#[test]
fn post_lease_activity_proof_rejects_a_put_tail_that_finished_before_lease_acquisition() {
    let before = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
    let expected_digest = Some(scanner_activity_snapshot_digest(&before));
    assert_eq!(scanner_post_lease_activity_defer_reason(expected_digest, Ok(before.clone())), None);

    let mut after = before.clone();
    after
        .get_mut("node-2")
        .expect("writer should be present")
        .namespace_generation += 1;
    assert_eq!(
        before["node-2"].movement_generation, after["node-2"].movement_generation,
        "the existing movement-only lease remains valid after a PUT tail drains"
    );
    assert!(scanner_activity_allows_usage_publication(&after));
    let reason = scanner_post_lease_activity_defer_reason(expected_digest, Ok(after));
    assert_eq!(reason, Some(ScannerCycleDeferReason::ActivityBaselineUnavailable));

    let result = ScannerCycleResult::new(ScannerCycleStatus::Complete, None).with_remote_dirty_usage_acknowledgements(vec![
        ScannerDirtyUsageAcknowledgement {
            host: "node-2".to_string(),
            instance_id: "epoch-a".to_string(),
            kind: ScannerDirtyUsageAcknowledgementKind::Generation(5),
        },
    ]);
    let (outcome, _, acknowledgements) = finalize_scanner_cycle_result(
        result,
        DataUsagePersistOutcome::Deferred(reason.expect("changed namespace should defer publication")).into(),
    );
    assert_eq!(
        outcome,
        ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable)
    );
    assert!(
        acknowledgements.is_empty(),
        "a rejected publication must not acknowledge the peer's dirty usage"
    );
}

#[test]
fn post_lease_activity_proof_requires_a_complete_matching_baseline() {
    let before = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
    let digest = scanner_activity_snapshot_digest(&before);
    let mut blocked = before.clone();
    blocked.get_mut("node-2").expect("peer should be present").publication_blocked = true;
    let blocked_digest = scanner_activity_snapshot_digest(&blocked);
    for (expected, observed) in [
        (None, Ok(before)),
        (Some(digest), Err("peer is unavailable".to_string())),
        (Some(digest), Ok(BTreeMap::new())),
        (Some(blocked_digest), Ok(blocked)),
    ] {
        assert_eq!(
            scanner_post_lease_activity_defer_reason(expected, observed),
            Some(ScannerCycleDeferReason::ActivityBaselineUnavailable)
        );
    }
}

#[test]
fn scanner_activity_snapshot_digest_fences_storage_topology() {
    let first = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
    let mut changed = first.clone();
    changed.get_mut("node-2").expect("node should exist").topology_digest = [4; 32];

    assert_ne!(scanner_activity_snapshot_digest(&first), scanner_activity_snapshot_digest(&changed));
}

#[test]
fn scanner_activity_snapshot_digest_fences_peer_protocol_upgrades() {
    let legacy = BTreeMap::from([(
        "node-2".to_string(),
        ScannerNodeActivity {
            protocol_version: SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION,
            ..scanner_node_activity("epoch-a", 7, 3)
        },
    )]);
    let previous = BTreeMap::from([(
        "node-2".to_string(),
        ScannerNodeActivity {
            protocol_version: SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION,
            ..scanner_node_activity("epoch-a", 7, 3)
        },
    )]);
    let current = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);

    assert_ne!(scanner_activity_snapshot_digest(&legacy), scanner_activity_snapshot_digest(&current));
    assert_ne!(scanner_activity_snapshot_digest(&previous), scanner_activity_snapshot_digest(&current));
}

#[test]
fn scanner_activity_snapshot_fences_data_movement() {
    let idle = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
    let mut moving = idle.clone();
    moving.get_mut("node-2").expect("node should exist").data_movement_active = true;

    assert!(!scanner_activity_allows_usage_publication(&BTreeMap::new()));
    assert!(scanner_activity_allows_usage_publication(&idle));
    assert!(!scanner_activity_allows_usage_publication(&moving));
    assert_ne!(scanner_activity_snapshot_digest(&idle), scanner_activity_snapshot_digest(&moving));
}

#[test]
fn scanner_activity_snapshot_digest_fences_dirty_usage_state() {
    let clean = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
    let pending = BTreeMap::from([(
        "node-2".to_string(),
        ScannerNodeActivity {
            dirty_usage_generation: 6,
            dirty_usage_pending: true,
            ..scanner_node_activity("epoch-a", 7, 3)
        },
    )]);

    assert_ne!(scanner_activity_snapshot_digest(&clean), scanner_activity_snapshot_digest(&pending));
}

#[test]
fn scanner_activity_structural_digest_ignores_regular_bucket_writes() {
    let baseline = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
    let mut written = baseline.clone();
    let activity = written.get_mut("node-2").expect("node should exist");
    activity.namespace_generation = 8;
    activity.dirty_usage_generation = 6;
    activity.dirty_usage_pending = true;

    assert_ne!(scanner_activity_snapshot_digest(&baseline), scanner_activity_snapshot_digest(&written));
    assert_eq!(
        scanner_activity_structural_digest(&baseline),
        scanner_activity_structural_digest(&written),
        "bucket writes are refreshed through the dirty-bucket scope rather than invalidating every cache"
    );
}

#[test]
fn scanner_activity_structural_digest_fences_restart_and_maintenance() {
    let baseline = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
    let mut restarted = baseline.clone();
    restarted.get_mut("node-2").expect("node should exist").instance_id = "epoch-b".to_string();
    let mut maintained = baseline.clone();
    maintained
        .get_mut("node-2")
        .expect("node should exist")
        .maintenance_generation = 4;

    assert_ne!(
        scanner_activity_structural_digest(&baseline),
        scanner_activity_structural_digest(&restarted)
    );
    assert_ne!(
        scanner_activity_structural_digest(&baseline),
        scanner_activity_structural_digest(&maintained)
    );
}

#[test]
fn scanner_dirty_usage_acknowledgements_exclude_local_and_clean_nodes() {
    let snapshot = BTreeMap::from([
        (
            LOCAL_SCANNER_ACTIVITY_NODE.to_string(),
            ScannerNodeActivity {
                dirty_usage_generation: 7,
                dirty_usage_pending: true,
                ..scanner_node_activity("epoch-local", 7, 3)
            },
        ),
        ("node-2".to_string(), scanner_node_activity("epoch-clean", 7, 3)),
        (
            "node-3".to_string(),
            ScannerNodeActivity {
                dirty_usage_generation: 11,
                dirty_usage_pending: true,
                ..scanner_node_activity("epoch-dirty", 7, 3)
            },
        ),
    ]);

    assert_eq!(
        scanner_dirty_usage_acknowledgements(&snapshot),
        vec![ScannerDirtyUsageAcknowledgement {
            host: "node-3".to_string(),
            instance_id: "epoch-dirty".to_string(),
            kind: ScannerDirtyUsageAcknowledgementKind::Generation(11),
        }]
    );
}

#[test]
fn scanner_activity_rejects_one_process_claimed_by_multiple_hosts() {
    let mut instances = BTreeMap::new();
    record_scanner_activity_instance(&mut instances, "node-1", "0123456789abcdef0123456789abcdef")
        .expect("first host should establish the instance identity");
    let err = record_scanner_activity_instance(&mut instances, "node-2", "0123456789abcdef0123456789abcdef")
        .expect_err("a process identity must not represent two cluster nodes");

    assert!(err.contains("node-1 and node-2"));
}

#[test]
fn scanner_activity_observation_requires_a_complete_baseline() {
    let mut seen = None;
    let first = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);

    let (observation, error) = apply_scanner_activity_probe_result(&mut seen, Ok(first.clone()));
    assert_eq!(observation, ScannerActivityObservation::Unverified);
    assert!(error.is_none());

    let (observation, error) = apply_scanner_activity_probe_result(&mut seen, Ok(first));
    assert_eq!(observation, ScannerActivityObservation::Unchanged);
    assert!(error.is_none());

    let changed = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 8, 3))]);
    let (observation, error) = apply_scanner_activity_probe_result(&mut seen, Ok(changed));
    assert_eq!(observation, ScannerActivityObservation::Changed);
    assert!(error.is_none());

    let restarted = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-b", 8, 0))]);
    let (observation, error) = apply_scanner_activity_probe_result(&mut seen, Ok(restarted));
    assert_eq!(observation, ScannerActivityObservation::RemoteRestarted);
    assert!(error.is_none());

    let (observation, error) =
        apply_scanner_activity_probe_result(&mut seen, Err("peer does not support activity probes".to_string()));
    assert_eq!(observation, ScannerActivityObservation::Unverified);
    assert_eq!(error.as_deref(), Some("peer does not support activity probes"));
    assert!(seen.is_none());
}

#[test]
fn remote_maintenance_change_is_distinct_from_namespace_activity() {
    let previous = BTreeMap::from([
        (LOCAL_SCANNER_ACTIVITY_NODE.to_string(), scanner_node_activity("local", 5, 2)),
        ("node-2".to_string(), scanner_node_activity("remote", 7, 3)),
    ]);
    let remote_maintenance_changed = BTreeMap::from([
        (LOCAL_SCANNER_ACTIVITY_NODE.to_string(), scanner_node_activity("local", 5, 2)),
        ("node-2".to_string(), scanner_node_activity("remote", 7, 4)),
    ]);
    assert_eq!(
        compare_scanner_activity(&previous, &remote_maintenance_changed),
        ScannerActivityObservation::MaintenanceChanged
    );

    let local_maintenance_changed = BTreeMap::from([
        (LOCAL_SCANNER_ACTIVITY_NODE.to_string(), scanner_node_activity("local", 5, 3)),
        ("node-2".to_string(), scanner_node_activity("remote", 7, 3)),
    ]);
    assert_eq!(
        compare_scanner_activity(&previous, &local_maintenance_changed),
        ScannerActivityObservation::Changed
    );
}

#[test]
fn remote_movement_generation_change_is_distinct_from_cluster_activity() {
    let previous = BTreeMap::from([("node-2".to_string(), scanner_node_activity("remote", 7, 3))]);
    let movement_changed = BTreeMap::from([(
        "node-2".to_string(),
        ScannerNodeActivity {
            movement_generation: 10,
            ..scanner_node_activity("remote", 7, 3)
        },
    )]);

    assert_eq!(
        compare_scanner_activity(&previous, &movement_changed),
        ScannerActivityObservation::MovementChanged
    );
    assert!(scanner_activity_observed_work(ScannerActivityObservation::MovementChanged));
}

#[test]
fn remote_restart_is_distinct_from_deferred_cluster_activity() {
    let previous = BTreeMap::from([("node-2".to_string(), scanner_node_activity("remote-a", 7, 3))]);
    let restarted = BTreeMap::from([("node-2".to_string(), scanner_node_activity("remote-b", 7, 3))]);

    assert_eq!(
        compare_scanner_activity(&previous, &restarted),
        ScannerActivityObservation::RemoteRestarted
    );
    assert!(scanner_activity_observed_work(ScannerActivityObservation::RemoteRestarted));
}

#[test]
fn local_maintenance_wakeup_releases_a_remote_maintenance_block() {
    let blocked = scanner_activity_backoff_blocked_after_wake(false, ScannerCycleWakeReason::ClusterMaintenance);
    assert!(blocked);

    let unblocked = scanner_activity_backoff_blocked_after_wake(blocked, ScannerCycleWakeReason::MaintenanceConfig);
    assert!(!unblocked);
    assert!(scanner_activity_backoff_blocked_after_wake(
        blocked,
        ScannerCycleWakeReason::ClusterActivity
    ));
}

#[test]
fn scanner_activity_after_a_cycle_restores_the_base_interval() {
    let runtime_config = ScannerRuntimeConfig {
        cycle_interval: Duration::from_secs(60),
        ..Default::default()
    };
    let mut backoff = ScannerCleanIdleBackoff { interval_multiplier: 8 };

    record_scanner_cycle_result(
        &mut backoff,
        &runtime_config,
        true,
        ScannerCycleWakeReason::Timer,
        ScannerCycleOutcome::Completed,
        scanner_activity_observed_work(ScannerActivityObservation::Changed),
    );

    let plan = scanner_cycle_wait_plan(&runtime_config, backoff, true, std::convert::identity);
    assert_eq!(plan.effective_interval, Duration::from_secs(60));
    assert_eq!(plan.delay, Duration::from_secs(60));
}

#[tokio::test(start_paused = true)]
#[serial]
async fn distributed_clean_idle_wait_wakes_at_base_interval_for_remote_activity() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let ctx = CancellationToken::new();
    let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]));
    let changed = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 8, 3))]);

    let reason = wait_for_next_scanner_cycle_with_activity(
        &ctx,
        Duration::from_secs(120),
        Some(Duration::from_secs(60)),
        &mut seen,
        ScannerCycleObservedGenerations {
            dirty_usage: Some(crate::scanner_io::dirty_usage_generation()),
            runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
            maintenance: crate::scanner_io::scanner_maintenance_generation(),
            defer_cluster_activity: false,
        },
        || false,
        || std::future::ready(Ok(changed.clone())),
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::ClusterActivity);
    assert_eq!(seen, Some(changed));
}

#[tokio::test(start_paused = true)]
#[serial]
async fn superseded_retry_wait_defers_dirty_cluster_activity_until_timer() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let ctx = CancellationToken::new();
    let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]));
    let changed = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 8, 3))]);

    let reason = wait_for_next_scanner_cycle_with_activity(
        &ctx,
        Duration::from_secs(120),
        Some(Duration::from_secs(60)),
        &mut seen,
        ScannerCycleObservedGenerations {
            dirty_usage: None,
            runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
            maintenance: crate::scanner_io::scanner_maintenance_generation(),
            defer_cluster_activity: true,
        },
        || false,
        || std::future::ready(Ok(changed.clone())),
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::Timer);
    assert_eq!(seen, Some(changed));
}

#[tokio::test(start_paused = true)]
async fn superseded_retry_wait_wakes_for_remote_movement_generation() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let ctx = CancellationToken::new();
    let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("remote", 7, 3))]));
    let changed = BTreeMap::from([(
        "node-2".to_string(),
        ScannerNodeActivity {
            movement_generation: 10,
            ..scanner_node_activity("remote", 7, 3)
        },
    )]);

    let reason = wait_for_next_scanner_cycle_with_activity(
        &ctx,
        Duration::from_secs(120),
        Some(Duration::from_secs(60)),
        &mut seen,
        ScannerCycleObservedGenerations {
            dirty_usage: None,
            runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
            maintenance: crate::scanner_io::scanner_maintenance_generation(),
            defer_cluster_activity: true,
        },
        || false,
        || std::future::ready(Ok(changed.clone())),
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::ClusterActivity);
    assert_eq!(seen, Some(changed));
}

#[tokio::test(start_paused = true)]
async fn superseded_retry_wait_wakes_when_remote_restart_clears_movement_state() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let ctx = CancellationToken::new();
    let blocked = BTreeMap::from([(
        "node-2".to_string(),
        ScannerNodeActivity {
            data_movement_active: true,
            publication_blocked: true,
            ..scanner_node_activity("remote-a", 7, 3)
        },
    )]);
    let restarted = BTreeMap::from([("node-2".to_string(), scanner_node_activity("remote-b", 7, 3))]);
    let mut seen = Some(blocked);

    let reason = wait_for_next_scanner_cycle_with_activity(
        &ctx,
        Duration::from_secs(120),
        Some(Duration::from_secs(60)),
        &mut seen,
        ScannerCycleObservedGenerations {
            dirty_usage: None,
            runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
            maintenance: crate::scanner_io::scanner_maintenance_generation(),
            defer_cluster_activity: true,
        },
        || false,
        || std::future::ready(Ok(restarted.clone())),
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::ClusterActivity);
    assert_eq!(seen, Some(restarted));
}

#[tokio::test(start_paused = true)]
#[serial]
async fn distributed_clean_idle_wait_blocks_backoff_for_unpropagated_maintenance() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let ctx = CancellationToken::new();
    let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]));
    let changed = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 4))]);

    let reason = wait_for_next_scanner_cycle_with_activity(
        &ctx,
        Duration::from_secs(120),
        Some(Duration::from_secs(60)),
        &mut seen,
        ScannerCycleObservedGenerations {
            dirty_usage: Some(crate::scanner_io::dirty_usage_generation()),
            runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
            maintenance: crate::scanner_io::scanner_maintenance_generation(),
            defer_cluster_activity: false,
        },
        || false,
        || std::future::ready(Ok(changed.clone())),
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::ClusterMaintenance);
}

#[tokio::test(start_paused = true)]
#[serial]
async fn distributed_clean_idle_wait_fails_closed_when_a_peer_is_unverifiable() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let ctx = CancellationToken::new();
    let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]));

    let reason = wait_for_next_scanner_cycle_with_activity(
        &ctx,
        Duration::from_secs(120),
        Some(Duration::from_secs(60)),
        &mut seen,
        ScannerCycleObservedGenerations {
            dirty_usage: Some(crate::scanner_io::dirty_usage_generation()),
            runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
            maintenance: crate::scanner_io::scanner_maintenance_generation(),
            defer_cluster_activity: false,
        },
        || false,
        || std::future::ready(Err("node-2 is unreachable".to_string())),
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::ClusterActivityUnavailable);
    assert!(seen.is_none());
}

#[tokio::test(start_paused = true)]
#[serial]
async fn distributed_clean_idle_wait_keeps_the_extended_deadline_when_peers_are_clean() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let ctx = CancellationToken::new();
    let expected = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
    let mut seen = Some(expected.clone());

    let reason = wait_for_next_scanner_cycle_with_activity(
        &ctx,
        Duration::from_secs(120),
        Some(Duration::from_secs(60)),
        &mut seen,
        ScannerCycleObservedGenerations {
            dirty_usage: Some(crate::scanner_io::dirty_usage_generation()),
            runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
            maintenance: crate::scanner_io::scanner_maintenance_generation(),
            defer_cluster_activity: false,
        },
        || false,
        || std::future::ready(Ok(expected.clone())),
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::Timer);
    assert_eq!(seen, Some(expected));
}

#[tokio::test(start_paused = true)]
#[serial]
async fn scanner_activity_probe_wait_is_cancellation_aware() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let ctx = CancellationToken::new();
    let cancel = ctx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(61)).await;
        cancel.cancel();
    });
    let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]));

    let reason = wait_for_next_scanner_cycle_with_activity(
        &ctx,
        Duration::from_secs(120),
        Some(Duration::from_secs(60)),
        &mut seen,
        ScannerCycleObservedGenerations {
            dirty_usage: Some(crate::scanner_io::dirty_usage_generation()),
            runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
            maintenance: crate::scanner_io::scanner_maintenance_generation(),
            defer_cluster_activity: false,
        },
        || false,
        std::future::pending::<Result<ScannerActivitySnapshot, String>>,
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::Cancelled);
}

#[tokio::test(start_paused = true)]
#[serial]
async fn scanner_activity_probe_wait_stops_after_leader_lock_loss() {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let ctx = CancellationToken::new();
    let lock_lost = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let lose_lock = Arc::clone(&lock_lost);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(61)).await;
        lose_lock.store(true, std::sync::atomic::Ordering::Release);
    });
    let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]));

    let reason = wait_for_next_scanner_cycle_with_activity(
        &ctx,
        Duration::from_secs(120),
        Some(Duration::from_secs(60)),
        &mut seen,
        ScannerCycleObservedGenerations {
            dirty_usage: Some(crate::scanner_io::dirty_usage_generation()),
            runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
            maintenance: crate::scanner_io::scanner_maintenance_generation(),
            defer_cluster_activity: false,
        },
        || lock_lost.load(std::sync::atomic::Ordering::Acquire),
        std::future::pending::<Result<ScannerActivitySnapshot, String>>,
    )
    .await;

    assert_eq!(reason, ScannerCycleWakeReason::LeaderLockLost);
}

#[test]
#[serial]
fn test_get_cycle_scan_mode_runs_deep_until_selection_window_completes() {
    with_var(ENV_SCANNER_BITROT_CYCLE_SECS, Some("3600"), || {
        let mode = get_cycle_scan_mode(10, 0, Some(Utc::now()), bitrot_scan_cycle());
        assert_eq!(mode, HealScanMode::Deep);
    });
}

#[test]
#[serial]
fn test_get_cycle_scan_mode_respects_elapsed_bitrot_cycle() {
    with_var(ENV_SCANNER_BITROT_CYCLE_SECS, Some("3600"), || {
        let recent = Utc::now() - chrono::Duration::minutes(30);
        let old = Utc::now() - chrono::Duration::hours(2);

        assert_eq!(get_cycle_scan_mode(2048, 0, Some(recent), bitrot_scan_cycle()), HealScanMode::Normal);
        assert_eq!(get_cycle_scan_mode(2048, 0, Some(old), bitrot_scan_cycle()), HealScanMode::Deep);
    });
}

#[test]
#[serial]
fn test_get_cycle_scan_mode_can_disable_periodic_deep_scan() {
    with_var(ENV_SCANNER_BITROT_CYCLE_SECS, Some("off"), || {
        assert_eq!(get_cycle_scan_mode(1, 0, None, bitrot_scan_cycle()), HealScanMode::Normal);
    });
}

#[test]
#[serial]
fn test_background_heal_info_for_scan_start_marks_deep_active() {
    let now = Utc::now();
    let info =
        background_heal_info_for_scan_start(BackgroundHealInfo::default(), 7, HealScanMode::Deep, now, bitrot_scan_cycle())
            .expect("deep scan should update background heal info");

    assert_eq!(info.current_scan_mode, HealScanMode::Deep);
    assert_eq!(info.bitrot_start_cycle, 7);
    assert_eq!(info.bitrot_start_time, Some(now));
}

#[test]
fn background_heal_read_failures_never_become_initializable_defaults() {
    assert_eq!(
        classify_background_heal_read_error(&EcstoreError::ConfigNotFound),
        BackgroundHealInfoReadStatus::Missing
    );
    assert_eq!(
        classify_background_heal_read_error(&EcstoreError::SlowDown),
        BackgroundHealInfoReadStatus::Transient
    );
    assert!(decode_background_heal_info(b"not-json").is_err());
}

#[test]
#[serial]
fn test_background_heal_info_for_scan_start_keeps_deep_window_start() {
    with_var_unset(ENV_SCANNER_BITROT_CYCLE_SECS, || {
        let started_at = Utc::now();
        let info = BackgroundHealInfo {
            bitrot_start_time: Some(started_at),
            bitrot_start_cycle: 7,
            current_scan_mode: HealScanMode::Normal,
        };

        let info = background_heal_info_for_scan_start(info, 8, HealScanMode::Deep, Utc::now(), bitrot_scan_cycle())
            .expect("deep scan should mark active status");

        assert_eq!(info.current_scan_mode, HealScanMode::Deep);
        assert_eq!(info.bitrot_start_cycle, 7);
        assert_eq!(info.bitrot_start_time, Some(started_at));
    });
}

#[test]
fn test_background_heal_info_for_scan_complete_marks_deep_idle() {
    let started_at = Utc::now();
    let info = BackgroundHealInfo {
        bitrot_start_time: Some(started_at),
        bitrot_start_cycle: 7,
        current_scan_mode: HealScanMode::Deep,
    };

    let info = background_heal_info_for_scan_complete(info, HealScanMode::Deep)
        .expect("completed deep scan should update background heal info");

    assert_eq!(info.current_scan_mode, HealScanMode::Normal);
    assert_eq!(info.bitrot_start_cycle, 7);
    assert_eq!(info.bitrot_start_time, Some(started_at));
}

#[test]
fn test_background_heal_info_for_scan_complete_leaves_normal_scan_unchanged() {
    let info = BackgroundHealInfo {
        bitrot_start_time: Some(Utc::now()),
        bitrot_start_cycle: 7,
        current_scan_mode: HealScanMode::Normal,
    };

    assert!(background_heal_info_for_scan_complete(info, HealScanMode::Normal).is_none());
}

#[test]
fn test_background_heal_info_for_failed_scan_preserves_deep_mode() {
    let info = BackgroundHealInfo {
        bitrot_start_time: Some(Utc::now()),
        bitrot_start_cycle: 7,
        current_scan_mode: HealScanMode::Deep,
    };

    assert!(background_heal_info_for_scan_result(info, HealScanMode::Deep, false).is_none());
}

#[test]
fn test_retain_recent_cycle_completions_keeps_last_entries() {
    let base = Utc::now();
    let keep = data_usage_update_dir_cycles() as usize;
    let mut completed: Vec<_> = (0..keep + 2).map(|i| base + chrono::Duration::seconds(i as i64)).collect();

    retain_recent_cycle_completions(&mut completed);

    assert_eq!(completed.len(), keep);
    assert_eq!(completed.first().copied(), Some(base + chrono::Duration::seconds(2)));
    assert_eq!(completed.last().copied(), Some(base + chrono::Duration::seconds((keep + 1) as i64)));
}
