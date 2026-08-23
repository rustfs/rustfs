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
use crate::{
    DATA_USAGE_BLOOM_RECOVERY_PATH, Endpoint, EndpointServerPools, Endpoints, InstanceContext, PoolEndpoints,
    ScannerGetObjectReader as GetObjectReader, ScannerObjectInfo as ObjectInfo, ScannerObjectOptions as ObjectOptions,
    ScannerPutObjReader as PutObjReader, init_bucket_metadata_sys_for_scanner_tests, init_ecstore_config_for_scanner_tests,
    init_local_disks_with_instance_ctx,
};
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use temp_env::{with_var, with_var_unset};
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use tokio::time::{Duration, advance};

const TEST_DEFAULT_SCANNER_CYCLE_SECS: u64 = 24 * 60 * 60;

async fn setup_scanner_cycle_store() -> (tempfile::TempDir, Arc<ECStore>) {
    init_ecstore_config_for_scanner_tests();
    let temp_dir = tempfile::tempdir().expect("scanner cycle test directory should be created");
    let mut endpoints = Vec::new();
    for disk_index in 0..4 {
        let disk_path = temp_dir.path().join(format!("disk{disk_index}"));
        tokio::fs::create_dir_all(&disk_path)
            .await
            .expect("scanner cycle test disk should be created");
        let mut endpoint =
            Endpoint::try_from(disk_path.to_str().expect("disk path should be utf8")).expect("endpoint should parse");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(disk_index);
        endpoints.push(endpoint);
    }
    let endpoint_pools = EndpointServerPools::from(vec![PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: 4,
        endpoints: Endpoints::from(endpoints),
        cmd_line: "scanner-cycle-metrics".to_string(),
        platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
    }]);
    let instance_ctx = Arc::new(InstanceContext::new());
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

    (temp_dir, store)
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
    non_regular_objects: Mutex<HashSet<String>>,
    fail_put_number: Mutex<HashMap<String, usize>>,
    object_not_found_put_number: Mutex<HashMap<String, usize>>,
    error_after_commit_put_number: Mutex<HashMap<String, usize>>,
    interleaving_puts: Mutex<HashMap<String, (usize, Vec<u8>)>>,
    cancel_after_interleaving_puts: Mutex<HashMap<String, CancellationToken>>,
    cancel_after_successful_puts: Mutex<HashMap<String, (usize, CancellationToken)>>,
    replace_after_successful_puts: Mutex<HashMap<String, (usize, Vec<u8>)>>,
    put_counts: Mutex<HashMap<String, usize>>,
    publication_admission_blocked: AtomicBool,
}

fn memory_config_key(bucket: &str, object: &str) -> String {
    format!("{bucket}/{object}")
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
        let data = self
            .objects
            .lock()
            .await
            .get(&key)
            .cloned()
            .ok_or(EcstoreError::FileNotFound)?;
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
    assert!(persisted_usage_floor(store.clone()).await.is_err());

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
            scanner_cycle: Some(u64::MAX - 1),
            ..Default::default()
        })
        .expect("usage snapshot should encode"),
    );
    assert!(persisted_usage_floor(store).await.is_err());
}

#[tokio::test]
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
        Ok(ObjectInfo::default())
    }

    async fn scanner_data_usage_publication_admission(&self) -> Option<crate::ScannerDataUsagePublicationAdmission> {
        (!self.publication_admission_blocked.load(Ordering::Acquire)).then(crate::ScannerDataUsagePublicationAdmission::unfenced)
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
    assert!(claim_scanner_leadership(&ctx, store.clone(), &mut cycle, &mut revision, &mut persisted_epoch,).await);

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
async fn test_leadership_claim_rejects_terminal_epoch() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        next: 12,
        ..Default::default()
    };
    let mut persisted_epoch = u64::MAX - 1;

    assert!(!claim_scanner_leadership(&ctx, store.clone(), &mut cycle, &mut revision, &mut persisted_epoch).await);
    assert_eq!(persisted_epoch, u64::MAX - 1);
    assert!(read_config(store, &DATA_USAGE_BLOOM_NAME_PATH).await.is_err());
}

#[tokio::test]
async fn leadership_claim_defers_without_usage_baseline_before_bloom_write() {
    let store = Arc::new(MemoryConfigStore::default());
    let ctx = CancellationToken::new();
    let mut revision = DataUsageCacheRevision::Missing;
    let mut cycle = CurrentCycle {
        next: 12,
        ..Default::default()
    };
    let mut persisted_epoch = 0;

    assert!(!claim_scanner_leadership(&ctx, store.clone(), &mut cycle, &mut revision, &mut persisted_epoch,).await);
    assert!(read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH).await.is_err());
    assert!(read_config(store, DATA_USAGE_OBJ_NAME_PATH.as_str()).await.is_err());
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

    assert!(!claim_scanner_leadership(&ctx, store.clone(), &mut cycle, &mut revision, &mut persisted_epoch,).await);
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

    assert!(!claim_scanner_leadership(&ctx, store.clone(), &mut cycle, &mut revision, &mut persisted_epoch,).await);
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

    assert!(claim_scanner_leadership(&ctx, store.clone(), &mut cycle, &mut revision, &mut persisted_epoch).await);

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
    assert!(claim_scanner_leadership(&ctx, store.clone(), &mut cycle, &mut revision, &mut persisted_epoch).await);

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
                    route_blocked && call > 1
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
            || async { true },
        )
        .await;

        assert_eq!(outcome, DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement));
        assert!(!store.objects.lock().await.contains_key(&target_key));
        assert_eq!(
            store.put_counts.lock().await.get(&target_key),
            None,
            "the final pool-state fence must run before the first PUT"
        );
    }
}

#[tokio::test]
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
        || async { false },
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
        || async { true },
    )
    .await;

    assert_eq!(outcome, DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement));
    assert_eq!(store.put_counts.lock().await.get(&key), None);
}

#[tokio::test]
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
        || async { true },
    )
    .await;

    assert_eq!(outcome, DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement));
    let after = metrics.report().await.usage_freshness;
    assert_eq!(after.last_usage_save_result, before.last_usage_save_result);
    assert_eq!(after.last_usage_save_result_code, before.last_usage_save_result_code);
    assert_eq!(after.last_usage_save_unix_secs, before.last_usage_save_unix_secs);
}

#[tokio::test]
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
fn finalizing_a_saved_cycle_acknowledges_its_exact_dirty_snapshot() {
    crate::scanner_io::clear_dirty_usage_bucket("photos");
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

    let remote_acknowledgement = ScannerDirtyUsageAcknowledgement {
        host: "node-2".to_string(),
        instance_id: "0123456789abcdef0123456789abcdef".to_string(),
        generation: 11,
    };
    let unsaved = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot.clone()))
        .with_remote_dirty_usage_acknowledgements(vec![remote_acknowledgement.clone()]);
    let (outcome, _, acknowledgements) = finalize_scanner_cycle_result(unsaved, DataUsagePersistOutcome::NoUpdate);
    assert_eq!(outcome, ScannerCycleOutcome::Failed);
    assert!(acknowledgements.is_empty());
    assert!(crate::scanner_io::dirty_usage_buckets_pending());

    let saved = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot))
        .with_remote_dirty_usage_acknowledgements(vec![remote_acknowledgement.clone()]);
    let (outcome, _, acknowledgements) = finalize_scanner_cycle_result(saved, DataUsagePersistOutcome::Saved);
    assert_eq!(outcome, ScannerCycleOutcome::Completed);
    assert_eq!(acknowledgements, vec![remote_acknowledgement]);
    assert!(!crate::scanner_io::dirty_usage_buckets_pending());
}

#[test]
fn finalizing_a_deferred_usage_save_keeps_dirty_work_pending() {
    crate::scanner_io::clear_dirty_usage_bucket("photos");
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();
    let deferred = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot));

    let (outcome, _, acknowledgements) =
        finalize_scanner_cycle_result(deferred, DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::DataMovement));

    assert_eq!(outcome, ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement));
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
fn finalizing_an_already_durable_cycle_acknowledges_its_exact_dirty_snapshot() {
    crate::scanner_io::clear_dirty_usage_bucket("photos");
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

    let durable = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot));
    let (outcome, _, acknowledgements) = finalize_scanner_cycle_result(durable, DataUsagePersistOutcome::AlreadyDurable);

    assert_eq!(outcome, ScannerCycleOutcome::Completed);
    assert!(acknowledgements.is_empty());
    assert!(!crate::scanner_io::dirty_usage_buckets_pending());
}

#[test]
fn finalizing_a_prior_same_cycle_snapshot_keeps_new_dirty_work_pending() {
    crate::scanner_io::clear_dirty_usage_bucket("photos");
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

    let durable = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot));
    let (outcome, _, acknowledgements) = finalize_scanner_cycle_result(durable, DataUsagePersistOutcome::PriorCycleDurable);

    assert_eq!(outcome, ScannerCycleOutcome::Completed);
    assert!(acknowledgements.is_empty());
    assert!(crate::scanner_io::dirty_usage_buckets_pending());
    crate::scanner_io::clear_dirty_usage_bucket("photos");
}

#[test]
fn finalizing_a_durable_superseded_snapshot_keeps_dirty_work_pending() {
    crate::scanner_io::clear_dirty_usage_bucket("photos");
    crate::scanner_io::record_dirty_usage_bucket("photos");
    let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

    let superseded = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Superseded, Some(dirty_snapshot));
    let (outcome, _, acknowledgements) = finalize_scanner_cycle_result(superseded, DataUsagePersistOutcome::Saved);

    assert_eq!(outcome, ScannerCycleOutcome::Superseded);
    assert!(acknowledgements.is_empty());
    assert!(crate::scanner_io::dirty_usage_buckets_pending());
    crate::scanner_io::clear_dirty_usage_bucket("photos");
}

#[test]
fn data_usage_persist_wait_covers_cache_retries_and_backup() {
    with_var(rustfs_config::ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, Some("7"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(data_usage_persist_timeout(), Duration::from_millis(31_350));
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
fn test_cycle_interval_prefers_explicit_cycle_override() {
    with_var(ENV_SCANNER_SPEED, Some("slowest"), || {
        with_var(ENV_SCANNER_CYCLE, Some("42"), || {
            assert_eq!(cycle_interval(), Duration::from_secs(42));
        });
    });
}

#[test]
fn test_cycle_interval_prefers_explicit_cycle_over_default_cycle() {
    let _guard = ScannerDefaultCycleGuard::set(TEST_DEFAULT_SCANNER_CYCLE_SECS);

    with_var(ENV_SCANNER_CYCLE, Some("42"), || {
        assert_eq!(cycle_interval(), Duration::from_secs(42));
    });
}

#[test]
fn test_cycle_interval_uses_scanner_default_speed_override_when_unconfigured() {
    let _guard = ScannerDefaultSpeedGuard::set(ScannerSpeed::Slowest);

    with_unset_scanner_timing_env(|| {
        assert_eq!(cycle_interval(), Duration::from_secs(30 * 60));
    });
}

#[test]
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
fn scanner_cycle_schedule_status_reports_effective_backoff() {
    record_scanner_cycle_schedule(Duration::from_millis(86_400_001), true, 2_048, true, 7);

    let status = scanner_cycle_schedule_status();

    assert_eq!(status.effective_interval_seconds, 86_401);
    assert!(status.clean_idle_backoff_enabled);
    assert_eq!(status.clean_idle_backoff_multiplier, 2_048);
    assert!(status.superseded_retry_backoff_enabled);
    assert_eq!(status.superseded_cycles, 7);

    reset_scanner_cycle_schedule();
    let status = scanner_cycle_schedule_status();
    assert_eq!(status.effective_interval_seconds, 0);
    assert!(!status.clean_idle_backoff_enabled);
    assert_eq!(status.clean_idle_backoff_multiplier, 1);
    assert!(!status.superseded_retry_backoff_enabled);
    assert_eq!(status.superseded_cycles, 0);
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

#[tokio::test]
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
            generation: 11,
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
    assert_eq!(observation, ScannerActivityObservation::Changed);
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
fn test_get_cycle_scan_mode_runs_deep_until_selection_window_completes() {
    with_var(ENV_SCANNER_BITROT_CYCLE_SECS, Some("3600"), || {
        let mode = get_cycle_scan_mode(10, 0, Some(Utc::now()), bitrot_scan_cycle());
        assert_eq!(mode, HealScanMode::Deep);
    });
}

#[test]
fn test_get_cycle_scan_mode_respects_elapsed_bitrot_cycle() {
    with_var(ENV_SCANNER_BITROT_CYCLE_SECS, Some("3600"), || {
        let recent = Utc::now() - chrono::Duration::minutes(30);
        let old = Utc::now() - chrono::Duration::hours(2);

        assert_eq!(get_cycle_scan_mode(2048, 0, Some(recent), bitrot_scan_cycle()), HealScanMode::Normal);
        assert_eq!(get_cycle_scan_mode(2048, 0, Some(old), bitrot_scan_cycle()), HealScanMode::Deep);
    });
}

#[test]
fn test_get_cycle_scan_mode_can_disable_periodic_deep_scan() {
    with_var(ENV_SCANNER_BITROT_CYCLE_SECS, Some("off"), || {
        assert_eq!(get_cycle_scan_mode(1, 0, None, bitrot_scan_cycle()), HealScanMode::Normal);
    });
}

#[test]
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
