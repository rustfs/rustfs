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

use super::periodic::PeriodicMaintenanceScheduler;
use crate::storage_api::startup::services::ServerContextSlot;
use crate::storage_api::table::check_valid_bucket_name_strict;
use crate::table_catalog::{
    ConfiguredTableCatalogStore, EcStoreTableCatalogObjectBackend, ObjectTableCatalogStore, TableCatalogBackingMode,
    TableCatalogStoreError, TableCatalogStoreResult,
};
use std::{collections::BTreeSet, future::Future, sync::Arc, time::Duration};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const ENV_ENABLED: &str = "RUSTFS_TABLE_MAINTENANCE_SCHEDULER_ENABLED";
const ENV_BUCKETS: &str = "RUSTFS_TABLE_MAINTENANCE_SCHEDULER_BUCKETS";
const ENV_INTERVAL: &str = "RUSTFS_TABLE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS";
const ADMISSION_BUDGET: Duration = Duration::from_secs(30);
const LOG_COMPONENT: &str = "table_catalog";
const LOG_SUBSYSTEM: &str = "maintenance";
const EVENT_PERIODIC_SCHEDULER: &str = "table_maintenance_periodic_scheduler";

pub(crate) struct PeriodicMaintenanceConfig {
    buckets: Vec<String>,
    interval: Duration,
    backing: TableCatalogBackingMode,
}

impl PeriodicMaintenanceConfig {
    pub(crate) fn from_env() -> TableCatalogStoreResult<Option<Self>> {
        Self::parse(
            |key| match std::env::var(key) {
                Ok(value) => Ok(Some(value)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(std::env::VarError::NotUnicode(_)) => {
                    Err(TableCatalogStoreError::Invalid(format!("{key} must be valid UTF-8")))
                }
            },
            TableCatalogBackingMode::from_env,
        )
    }

    fn parse(
        read: impl Fn(&str) -> TableCatalogStoreResult<Option<String>>,
        backing: impl FnOnce() -> TableCatalogStoreResult<TableCatalogBackingMode>,
    ) -> TableCatalogStoreResult<Option<Self>> {
        match read(ENV_ENABLED)?.as_deref().map(str::trim) {
            None | Some("false") => return Ok(None),
            Some("true") => {}
            _ => return Err(TableCatalogStoreError::Invalid(format!("{ENV_ENABLED} must be true or false"))),
        }
        let raw = read(ENV_BUCKETS)?
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{ENV_BUCKETS} is required when scheduling is enabled")))?;
        if raw.len() > 2048 {
            return Err(TableCatalogStoreError::Invalid(format!("{ENV_BUCKETS} exceeds the allowlist size limit")));
        }
        let mut buckets = BTreeSet::new();
        for bucket in raw.split(',').map(str::trim) {
            check_valid_bucket_name_strict(bucket)
                .map_err(|_| TableCatalogStoreError::Invalid(format!("{ENV_BUCKETS} contains an invalid bucket name")))?;
            if !buckets.insert(bucket.to_string()) || buckets.len() > 32 {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "{ENV_BUCKETS} requires 1 to 32 distinct bucket names"
                )));
            }
        }
        let interval = read(ENV_INTERVAL)?.unwrap_or_else(|| "300".to_string());
        let seconds = interval
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|seconds| (30..=86_400).contains(seconds))
            .ok_or_else(|| TableCatalogStoreError::Invalid(format!("{ENV_INTERVAL} must be between 30 and 86400")))?;
        Ok(Some(Self {
            buckets: buckets.into_iter().collect(),
            interval: Duration::from_secs(seconds),
            backing: backing()?,
        }))
    }
}

pub(crate) type PeriodicMaintenanceRuntime = crate::table_catalog::CatalogBackgroundTask;

impl PeriodicMaintenanceRuntime {
    pub(crate) fn spawn(config: PeriodicMaintenanceConfig, server: Arc<ServerContextSlot>, shutdown: &CancellationToken) -> Self {
        Self::start(shutdown, move |cancellation| run(config, server, cancellation))
    }
}

fn tick_delay(interval: Duration) -> Duration {
    // Delay after completion avoids catch-up bursts; jitter spreads node scans.
    interval + Duration::from_secs(rand::random_range(0..=interval.as_secs() / 10))
}

async fn wait_for_tick(cancellation: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

async fn finish_owned_pass<F: Future>(pass: F, cancellation: &CancellationToken, deadline: Instant) -> F::Output {
    finish_owned_catalog_pass(pass, cancellation, deadline, EVENT_PERIODIC_SCHEDULER).await
}

async fn finish_owned_catalog_pass<F: Future>(
    pass: F,
    cancellation: &CancellationToken,
    deadline: Instant,
    event: &'static str,
) -> F::Output {
    tokio::pin!(pass);
    tokio::select! {
        biased;
        result = &mut pass => result,
        _ = cancellation.cancelled() => pass.await,
        _ = tokio::time::sleep_until(deadline) => {
            warn!(
                event, component = LOG_COMPONENT, subsystem = LOG_SUBSYSTEM,
                state = "draining", reason = "admission_deadline",
                "Maintenance scheduler stopped admitting work while its store call drains"
            );
            pass.await
        }
    }
}

async fn run(config: PeriodicMaintenanceConfig, server: Arc<ServerContextSlot>, cancellation: CancellationToken) {
    let mut scheduler = None;
    let mut degraded = false;
    while wait_for_tick(&cancellation, tick_delay(config.interval)).await {
        if scheduler.is_none() {
            // Deferred IAM may not have installed this server yet. Do not use
            // another embedded server's ambient context as a fallback.
            let Some(context) = server.installed_app_context() else {
                continue;
            };
            let backend = EcStoreTableCatalogObjectBackend::new(context.object_store());
            let store = match config.backing {
                TableCatalogBackingMode::ObjectBacked => {
                    ConfiguredTableCatalogStore::ObjectBacked(ObjectTableCatalogStore::new(backend))
                }
            };
            scheduler = Some(PeriodicMaintenanceScheduler::new(store, config.buckets.clone(), config.interval));
        }
        let deadline = Instant::now() + ADMISSION_BUDGET;
        let Some(scheduler) = scheduler.as_mut() else {
            continue;
        };
        let result = finish_owned_pass(scheduler.run_pass(&cancellation, deadline), &cancellation, deadline).await;
        match result {
            Ok(()) if degraded => {
                info!(
                    event = EVENT_PERIODIC_SCHEDULER,
                    component = LOG_COMPONENT,
                    subsystem = LOG_SUBSYSTEM,
                    state = "recovered",
                    "Maintenance scheduler pass recovered"
                );
                degraded = false;
            }
            Err(_) if !degraded => {
                warn!(
                    event = EVENT_PERIODIC_SCHEDULER,
                    component = LOG_COMPONENT,
                    subsystem = LOG_SUBSYSTEM,
                    state = "degraded",
                    reason = "queue_pass_failed",
                    "Maintenance scheduler could not complete a queueing pass"
                );
                degraded = true;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn config(values: &[(&str, &str)]) -> TableCatalogStoreResult<Option<PeriodicMaintenanceConfig>> {
        PeriodicMaintenanceConfig::parse(
            |key| {
                Ok(values
                    .iter()
                    .find(|(name, _)| *name == key)
                    .map(|(_, value)| value.to_string()))
            },
            || Ok(TableCatalogBackingMode::ObjectBacked),
        )
    }

    #[test]
    fn periodic_config_is_opt_in_and_validates_limits() {
        assert!(config(&[]).unwrap().is_none());
        assert!(
            config(&[(ENV_ENABLED, "false"), (ENV_BUCKETS, "invalid/path")])
                .unwrap()
                .is_none()
        );
        for value in ["", "1", "yes", "TRUE"] {
            assert!(config(&[(ENV_ENABLED, value)]).is_err());
        }
        assert!(config(&[(ENV_ENABLED, "true")]).is_err());
        for buckets in ["", "foo,", "foo,foo", "../foo", "127.0.0.1", "Upper", ".minio.sys"] {
            assert!(config(&[(ENV_ENABLED, "true"), (ENV_BUCKETS, buckets)]).is_err(), "{buckets}");
        }
        let too_many = (0..33).map(|n| format!("bucket-{n}")).collect::<Vec<_>>().join(",");
        assert!(config(&[(ENV_ENABLED, "true"), (ENV_BUCKETS, &too_many)]).is_err());
        for interval in ["0", "29", "86401", "-1", "NaN", "18446744073709551616"] {
            assert!(config(&[(ENV_ENABLED, "true"), (ENV_BUCKETS, "foo"), (ENV_INTERVAL, interval)]).is_err());
        }
        let parsed = config(&[(ENV_ENABLED, "true"), (ENV_BUCKETS, "bbb, aaa")]).unwrap().unwrap();
        assert_eq!(parsed.buckets, ["aaa", "bbb"]);
        assert_eq!(parsed.interval, Duration::from_secs(300));
        for _ in 0..100 {
            assert!((Duration::from_secs(300)..=Duration::from_secs(330)).contains(&tick_delay(parsed.interval)));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_tick_waits_and_cancels_without_work() {
        let cancellation = CancellationToken::new();
        let start = Instant::now();
        assert!(wait_for_tick(&cancellation, Duration::from_secs(300)).await);
        assert_eq!(Instant::now() - start, Duration::from_secs(300));
        cancellation.cancel();
        assert!(!wait_for_tick(&cancellation, Duration::from_secs(300)).await);
        assert_eq!(Instant::now() - start, Duration::from_secs(300));
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_deadline_and_shutdown_retain_owned_work() {
        for cancel in [false, true] {
            let cancellation = CancellationToken::new();
            let completed = Arc::new(AtomicBool::new(false));
            let (entered, started) = tokio::sync::oneshot::channel();
            let (release, resume) = tokio::sync::oneshot::channel();
            let done = completed.clone();
            let token = cancellation.clone();
            let task = tokio::spawn(async move {
                finish_owned_pass(
                    async move {
                        entered.send(()).unwrap();
                        resume.await.unwrap();
                        done.store(true, Ordering::SeqCst);
                    },
                    &token,
                    Instant::now() + Duration::from_secs(30),
                )
                .await;
            });
            started.await.unwrap();
            if cancel {
                cancellation.cancel();
            } else {
                tokio::time::advance(Duration::from_secs(31)).await;
            }
            tokio::task::yield_now().await;
            assert!(!task.is_finished());
            assert!(!completed.load(Ordering::SeqCst));
            release.send(()).unwrap();
            task.await.unwrap();
            assert!(completed.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn periodic_join_cancelled_waiter_retains_original_task() {
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            entered.send(()).expect("task started");
            resume.await.expect("same task released");
        });
        let task_id = task.id();
        let mut runtime = PeriodicMaintenanceRuntime::from_test_task(CancellationToken::new(), Some(task));
        started.await.expect("actual task is held");
        runtime.request_stop();
        {
            let mut join = Box::pin(runtime.join());
            tokio::select! {
                biased;
                result = join.as_mut() => panic!("held task joined before release: {result:?}"),
                _ = std::future::ready(()) => {}
            }
        }
        let task = runtime.task.as_ref().expect("cancelled waiter must retain its handle");
        assert_eq!(task.id(), task_id);
        assert!(!task.is_finished());
        release.send(()).expect("same task is still owned");
        runtime
            .join()
            .await
            .expect("original task observation")
            .expect("actual task joined");
        assert!(runtime.task.is_none(), "a completed handle must not be polled again");
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_shutdown_timeout_joins_owned_pass_and_stays_failed() {
        let cancellation = CancellationToken::new();
        let witness = cancellation.clone();
        let token = cancellation.clone();
        let completed = Arc::new(AtomicBool::new(false));
        let done = completed.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            finish_owned_pass(
                async move {
                    entered.send(()).expect("owned pass started");
                    resume.await.expect("owned pass released");
                    done.store(true, Ordering::SeqCst);
                },
                &token,
                Instant::now() + Duration::from_secs(30),
            )
            .await;
        });
        let runtime = PeriodicMaintenanceRuntime::from_test_task(cancellation, Some(task));
        started.await.expect("real owned pass must be held");
        let mut stopping = Box::pin(crate::startup_shutdown::stop_catalog_background_task(Some(runtime)));
        tokio::select! {
            biased;
            result = stopping.as_mut() => panic!("held pass returned before shutdown was observed: {result:?}"),
            _ = witness.cancelled() => {}
        }
        tokio::time::advance(crate::server::SHUTDOWN_TIMEOUT + Duration::from_millis(1)).await;
        tokio::select! {
            biased;
            result = stopping.as_mut() => panic!("original timeout lost the held pass owner: {result:?}"),
            _ = std::future::ready(()) => {}
        }
        assert!(!completed.load(Ordering::SeqCst));
        release.send(()).expect("accepted work must not be aborted");
        let error = stopping.await.expect_err("late actual join must retain the original timeout");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert!(completed.load(Ordering::SeqCst), "shutdown cannot return before the owned pass completes");
        assert!(error.to_string().contains("owned task joined"));
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_shutdown_preserves_join_error_before_and_after_deadline() {
        for late in [false, true] {
            let cancellation = CancellationToken::new();
            let witness = cancellation.clone();
            let (entered, started) = tokio::sync::oneshot::channel();
            let (release, resume) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                entered.send(()).expect("task started");
                resume.await.expect("actual task released");
                panic!("expected maintenance task failure");
            });
            let task_id = task.id();
            let runtime = PeriodicMaintenanceRuntime::from_test_task(cancellation, Some(task));
            started.await.expect("actual task is held");
            let mut stopping = Box::pin(crate::startup_shutdown::stop_catalog_background_task(Some(runtime)));
            tokio::select! {
                biased;
                result = stopping.as_mut() => panic!("held task returned before stop: {result:?}"),
                _ = witness.cancelled() => {}
            }
            if late {
                tokio::time::advance(crate::server::SHUTDOWN_TIMEOUT + Duration::from_millis(1)).await;
                tokio::select! {
                    biased;
                    result = stopping.as_mut() => panic!("timeout detached the actual task: {result:?}"),
                    _ = std::future::ready(()) => {}
                }
            }
            release.send(()).expect("task must remain owned");
            let error = stopping.await.expect_err("actual task failure must survive shutdown");
            assert_eq!(
                error.kind(),
                if late {
                    std::io::ErrorKind::TimedOut
                } else {
                    std::io::ErrorKind::Other
                }
            );
            let failure = error.get_ref().expect("typed maintenance failure");
            let source = std::error::Error::source(failure).expect("background failure source");
            let source = source
                .downcast_ref::<tokio::task::JoinError>()
                .expect("original JoinError retained");
            assert!(source.is_panic());
            assert_eq!(source.id(), task_id, "failure must belong to the original task");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_shutdown_late_ready_observation_keeps_deadline_failure_and_original_join() {
        for panics in [false, true] {
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                let _ = entered_tx.send(());
                if release_rx.await.is_ok() {
                    let _ = completed_tx.send(());
                    assert!(!panics, "expected late maintenance task failure");
                }
            });
            let task_id = task.id();
            let task_finished = task.abort_handle();
            let runtime = PeriodicMaintenanceRuntime::from_test_task(CancellationToken::new(), Some(task));
            entered_rx.await.expect("the original task must start before observation");
            let mut stopping = Box::pin(crate::startup_shutdown::stop_catalog_background_task(Some(runtime)));
            let first = std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(stopping.as_mut(), cx))).await;
            let initially_pending = first.is_pending();
            // Do not poll the owner again until the deadline and actual task exit.
            tokio::time::advance(crate::server::SHUTDOWN_TIMEOUT + Duration::from_millis(1)).await;
            let released = release_tx.send(()).is_ok();
            let completed = completed_rx.await.is_ok();
            while !task_finished.is_finished() {
                tokio::task::yield_now().await;
            }
            let result = match first {
                std::task::Poll::Ready(result) => result,
                std::task::Poll::Pending => stopping.await,
            };
            let error = result.expect_err("late Ready must not restore timely maintenance success");
            // All real work is released, exited and observed before assertions.
            assert!(initially_pending && released && completed);
            assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
            if panics {
                let failure = error.get_ref().expect("typed maintenance failure");
                let source = std::error::Error::source(failure).expect("background failure source");
                let source = source
                    .downcast_ref::<tokio::task::JoinError>()
                    .expect("original late JoinError retained");
                assert!(source.is_panic());
                assert_eq!(source.id(), task_id, "late error must identify the original task");
            } else {
                assert!(error.to_string().contains("owned task joined"));
            }
        }
    }

    #[tokio::test]
    async fn periodic_shutdown_accepts_ready_and_disabled_but_rejects_missing_join() {
        crate::startup_shutdown::stop_catalog_background_task(None)
            .await
            .expect("disabled maintenance has no owner to drain");
        let task = tokio::spawn(async {});
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        let runtime = PeriodicMaintenanceRuntime::from_test_task(CancellationToken::new(), Some(task));
        crate::startup_shutdown::stop_catalog_background_task(Some(runtime))
            .await
            .expect("actual promptly completed task must join");
        let runtime = PeriodicMaintenanceRuntime::from_test_task(CancellationToken::new(), None);
        let error = crate::startup_shutdown::stop_catalog_background_task(Some(runtime))
            .await
            .expect_err("configured runtime with no join observation must fail closed");
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert!(error.to_string().contains("actual join unconfirmed"));
    }
}

use super::config::VectorMaintenanceRuntimeConfig;
use super::periodic::{VectorMaintenancePassReport, VectorMaintenanceScheduler};
use super::worker::{VectorMaintenanceExecutionContext, VectorMaintenanceRuntimeFailure};
use crate::table_catalog::{CatalogBackgroundTask, TableCatalogObjectBackend};

const EVENT_VECTOR_SCHEDULER: &str = "vector_maintenance_runtime";

/// The caller supplies the selected identity after this server's IAM has become ready.
/// The returned owner must be stopped and joined through the existing shutdown path.
pub(crate) fn spawn_vector_maintenance<B: TableCatalogObjectBackend>(
    config: VectorMaintenanceRuntimeConfig,
    server: Arc<ServerContextSlot>,
    backend: B,
    context: VectorMaintenanceExecutionContext<B>,
    node_admission: Option<Arc<rustfs_concurrency::adaptive::NodeAdmission>>,
    shutdown: &CancellationToken,
) -> TableCatalogStoreResult<Option<CatalogBackgroundTask>> {
    config.validate()?;
    if !config.enabled {
        return Ok(None);
    }
    let interval = config.discovery_interval;
    let pass_budget = config.pass_admission_budget;
    let VectorMaintenanceExecutionContext {
        identity,
        source,
        executor,
    } = context;
    let scheduler = VectorMaintenanceScheduler::new(config, backend, source, executor, identity, node_admission)?;
    Ok(Some(CatalogBackgroundTask::start_fallible(shutdown, move |cancellation| {
        run_vector(scheduler, server, interval, pass_budget, cancellation)
    })))
}

pub(super) async fn run_vector<B: TableCatalogObjectBackend>(
    mut scheduler: VectorMaintenanceScheduler<B>,
    server: Arc<ServerContextSlot>,
    interval: Duration,
    pass_budget: Duration,
    cancellation: CancellationToken,
) -> Result<(), VectorMaintenanceRuntimeFailure> {
    let mut next_discovery = Instant::now() + tick_delay(interval);
    let mut degraded = false;
    loop {
        if scheduler.has_fatal_failure() {
            cancellation.cancel();
            break;
        }
        let retry_delay = scheduler.next_retry_delay();
        let retry_deadline = Instant::now() + retry_delay.unwrap_or(interval);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            result = scheduler.join_next(), if scheduler.has_in_flight() => {
                if let Some(report) = result {
                    observe_vector_pass(Ok(report), &mut degraded);
                }
                if scheduler.start_ready(&cancellation).await.is_err() {
                    observe_vector_pass(Err(()), &mut degraded);
                }
            }
            _ = tokio::time::sleep_until(next_discovery) => {
                if server.installed_app_context().is_some() {
                    let deadline = Instant::now() + pass_budget;
                    let result = finish_owned_catalog_pass(scheduler.run_pass(&cancellation, deadline), &cancellation, deadline, EVENT_VECTOR_SCHEDULER).await;
                    observe_vector_pass(result.map_err(|_| ()), &mut degraded);
                }
                next_discovery = Instant::now() + tick_delay(interval);
            }
            _ = tokio::time::sleep_until(retry_deadline), if retry_delay.is_some() => {
                if scheduler.start_ready(&cancellation).await.is_err() {
                    observe_vector_pass(Err(()), &mut degraded);
                }
            }
        }
    }
    // JoinHandle owners are retained until every worker has drained its original operations.
    let report = scheduler.drain().await;
    if !scheduler.has_failure() || report.failed > 0 {
        observe_vector_pass(Ok(report), &mut degraded);
    }
    match scheduler.take_failure() {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn observe_vector_pass(result: Result<VectorMaintenancePassReport, ()>, degraded: &mut bool) {
    match result {
        Ok(report) if report.failed == 0 && report.first_error.is_none() => {
            if *degraded {
                info!(
                    event = EVENT_VECTOR_SCHEDULER,
                    component = LOG_COMPONENT,
                    subsystem = LOG_SUBSYSTEM,
                    state = "recovered",
                    "Vector maintenance resumed"
                );
                *degraded = false;
            }
            tracing::debug!(
                event = EVENT_VECTOR_SCHEDULER,
                component = LOG_COMPONENT,
                subsystem = LOG_SUBSYSTEM,
                result = "complete",
                discovered = report.discovered,
                queued = report.queued,
                started = report.started,
                completed = report.completed,
                skipped = report.skipped,
                retry_pending = report.retry_pending,
                manual_review = report.manual_review,
                "Vector maintenance pass completed"
            );
        }
        Ok(report) => {
            tracing::error!(
                event = EVENT_VECTOR_SCHEDULER,
                component = LOG_COMPONENT,
                subsystem = LOG_SUBSYSTEM,
                result = "failed",
                failed = report.failed,
                "Vector maintenance work failed"
            );
            *degraded = true;
        }
        Err(()) if !*degraded => {
            warn!(
                event = EVENT_VECTOR_SCHEDULER,
                component = LOG_COMPONENT,
                subsystem = LOG_SUBSYSTEM,
                state = "degraded",
                reason = "admission_or_discovery_failed",
                "Vector maintenance admission failed"
            );
            *degraded = true;
        }
        Err(()) => {}
    }
}

#[cfg(test)]
mod vector_runtime_tests {
    use super::*;
    use crate::table_catalog::test_support::TestCatalogObjectBackend;
    use crate::table_catalog::*;
    use std::collections::VecDeque;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use tokio::time::Instant;

    struct Source {
        calls: AtomicUsize,
        pages: Mutex<VecDeque<VectorMaintenanceTargetPage>>,
        entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    }
    impl Source {
        fn new(pages: Vec<VectorMaintenanceTargetPage>) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                pages: Mutex::new(pages.into()),
                entered: Mutex::new(None),
                release: Mutex::new(None),
            })
        }
    }
    #[async_trait::async_trait]
    impl VectorMaintenanceSource for Source {
        async fn check_admission(&self, _: &VectorMaintenanceExecutionIdentityV1) -> TableCatalogStoreResult<()> {
            Ok(())
        }
        async fn authorize_preparation(
            &self,
            _: &VectorMaintenanceExecutionIdentityV1,
            _: &VectorMaintenanceResourceV1,
        ) -> TableCatalogStoreResult<()> {
            Ok(())
        }
        async fn next_targets(
            &self,
            cursor: Option<&VectorMaintenanceDiscoveryCursor>,
            _: std::num::NonZeroUsize,
            _: &VectorMaintenanceDiscoveryBudget,
        ) -> TableCatalogStoreResult<VectorMaintenanceTargetPage> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call > 0 {
                assert!(cursor.is_some());
            }
            let entered = self.entered.lock().unwrap().take();
            let release = self.release.lock().unwrap().take();
            if let Some(entered) = entered {
                entered.send(()).unwrap();
            }
            if let Some(release) = release {
                release.await.unwrap();
            }
            Ok(self.pages.lock().unwrap().pop_front().unwrap_or(VectorMaintenanceTargetPage {
                targets: vec![],
                failures: vec![],
                next_cursor: None,
            }))
        }
    }
    struct Executor;
    #[async_trait::async_trait]
    impl<B: TableCatalogObjectBackend> VectorMaintenanceExecutor<B> for Executor {
        async fn recover_native(
            &self,
            _: Arc<ObjectTableCatalogStore<B>>,
            _: VectorMaintenanceWork,
            _: VectorMaintenanceControl,
        ) -> Result<(), VectorMaintenanceExecutionError> {
            panic!("empty discovery cannot recover native work");
        }

        async fn execute(
            &self,
            _: Arc<ObjectTableCatalogStore<B>>,
            _: VectorMaintenanceWork,
            _: VectorMaintenanceControl,
        ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
            panic!("empty discovery cannot execute work");
        }
        async fn prepare(
            &self,
            _: Arc<ObjectTableCatalogStore<B>>,
            _: VectorMaintenancePreparationAction,
            _: VectorMaintenanceControl,
        ) -> Result<(), VectorMaintenanceExecutionError> {
            panic!("empty discovery cannot prepare resources");
        }
    }
    fn identity() -> VectorMaintenanceExecutionIdentityV1 {
        VectorMaintenanceExecutionIdentityV1::try_new("maintenance-user".into(), [1; 16]).unwrap()
    }
    fn config() -> VectorMaintenanceRuntimeConfig {
        VectorMaintenanceRuntimeConfig {
            enabled: true,
            execution_account: "maintenance-user".into(),
            ..Default::default()
        }
    }
    fn page(more: bool) -> VectorMaintenanceTargetPage {
        VectorMaintenanceTargetPage {
            targets: vec![],
            failures: vec![],
            next_cursor: more.then(VectorMaintenanceDiscoveryCursor::default),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn vector_nested_runtime_preserves_worker_failure_and_original_shutdown_deadline() {
        use std::sync::atomic::AtomicBool;
        struct Owner(Arc<AtomicBool>);
        impl Drop for Owner {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        struct FailingExecutor {
            panics: bool,
            entered: Mutex<Option<tokio::sync::oneshot::Sender<tokio::task::Id>>>,
            release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
            completed: Arc<AtomicBool>,
        }
        #[async_trait::async_trait]
        impl<B: TableCatalogObjectBackend> VectorMaintenanceExecutor<B> for FailingExecutor {
            async fn recover_native(
                &self,
                _: Arc<ObjectTableCatalogStore<B>>,
                _: VectorMaintenanceWork,
                _: VectorMaintenanceControl,
            ) -> Result<(), VectorMaintenanceExecutionError> {
                panic!("preparation does not claim a job");
            }
            async fn execute(
                &self,
                _: Arc<ObjectTableCatalogStore<B>>,
                _: VectorMaintenanceWork,
                _: VectorMaintenanceControl,
            ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
                panic!("preparation uses its dedicated entry point");
            }
            async fn prepare(
                &self,
                _: Arc<ObjectTableCatalogStore<B>>,
                _: VectorMaintenancePreparationAction,
                control: VectorMaintenanceControl,
            ) -> Result<(), VectorMaintenanceExecutionError> {
                let _owner = Owner(self.completed.clone());
                let _io = control.acquire_io().await?;
                self.entered.lock().unwrap().take().unwrap().send(tokio::task::id()).unwrap();
                let release = self.release.lock().unwrap().take().unwrap();
                release.await.unwrap();
                assert!(!self.panics, "controlled execution panic");
                Err(VectorMaintenanceExecutionError::Executor {
                    category: VectorMaintenanceFailureCode::StorageUnavailable,
                    source: Box::new(std::io::Error::from_raw_os_error(5)),
                })
            }
        }
        for (panics, late) in [(false, false), (true, false), (false, true), (true, true)] {
            let fixture = crate::table_catalog::tests::prepared_commit::prepared_v3_fixture().await;
            let namespace = Namespace::parse("indexes").unwrap();
            let name = CatalogName::parse(fixture.entry.table.clone()).unwrap();
            let resource = CatalogHead::from_vector_entry(&fixture.entry)
                .unwrap()
                .resource_identity()
                .clone();
            let source = Source::new(vec![VectorMaintenanceTargetPage {
                targets: vec![VectorMaintenanceTarget::PrepareGc {
                    resource: VectorMaintenanceResourceV1::try_new(&resource, &namespace, &name).unwrap(),
                    kind: VectorMaintenanceKind::HistoryGc,
                    expected_head: Box::new(VectorMaintenanceHeadV1::from_catalog_entry(&fixture.entry).unwrap()),
                }],
                failures: vec![],
                next_cursor: None,
            }]);
            let (entered, started) = tokio::sync::oneshot::channel();
            let (release, resume) = tokio::sync::oneshot::channel();
            let completed = Arc::new(AtomicBool::new(false));
            let mut scheduler = VectorMaintenanceScheduler::new(
                config(),
                fixture.backend,
                source,
                Arc::new(FailingExecutor {
                    panics,
                    entered: Mutex::new(Some(entered)),
                    release: Mutex::new(Some(resume)),
                    completed: completed.clone(),
                }),
                identity(),
                None,
            )
            .unwrap();
            let (inner_joined, observed_inner_join) = tokio::sync::oneshot::channel();
            let outer = CatalogBackgroundTask::start_fallible(&CancellationToken::new(), move |cancellation| async move {
                let mut inner = CatalogBackgroundTask::start_fallible(&cancellation, move |cancellation| async move {
                    scheduler
                        .run_pass(&cancellation, Instant::now() + Duration::from_secs(30))
                        .await
                        .expect("initial discovery succeeds");
                    run_vector(
                        scheduler,
                        ServerContextSlot::new(),
                        Duration::from_secs(300),
                        Duration::from_secs(30),
                        cancellation,
                    )
                    .await
                });
                let result = inner.join_result().await.expect("original inner runtime must be joined");
                inner_joined.send(result.is_err()).expect("inner result observer retained");
                result
            });
            let original_worker = started.await.unwrap();
            let outer_completion = outer.task.as_ref().unwrap().abort_handle();
            let error = if late {
                let mut stopping = Box::pin(crate::startup_shutdown::stop_catalog_background_task(Some(outer)));
                tokio::select! { biased; result = stopping.as_mut() => panic!("accepted worker was detached: {result:?}"), _ = tokio::task::yield_now() => {} }
                tokio::time::advance(crate::server::SHUTDOWN_TIMEOUT + Duration::from_millis(1)).await;
                tokio::select! { biased; result = stopping.as_mut() => panic!("timeout dropped the original owner: {result:?}"), _ = tokio::task::yield_now() => {} }
                assert!(!completed.load(Ordering::SeqCst));
                assert!(!outer_completion.is_finished());
                release.send(()).unwrap();
                let error = stopping.await.unwrap_err();
                assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
                error
            } else {
                release.send(()).unwrap();
                if !panics {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while !completed.load(Ordering::SeqCst) {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    assert!(!outer_completion.is_finished(), "ordinary task failure keeps the runtime available");
                    outer.request_stop();
                }
                tokio::time::timeout(Duration::from_secs(5), async {
                    while !outer_completion.is_finished() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                let error = crate::startup_shutdown::stop_catalog_background_task(Some(outer))
                    .await
                    .unwrap_err();
                assert_eq!(error.kind(), std::io::ErrorKind::Other);
                error
            };
            assert!(completed.load(Ordering::SeqCst));
            assert!(outer_completion.is_finished());
            assert!(observed_inner_join.await.unwrap(), "actual inner owner must propagate its failed result");
            let mut source = error.get_ref().map(|source| source as &(dyn std::error::Error + 'static));
            let mut found_original = false;
            while let Some(current) = source {
                if panics {
                    if let Some(join) = current.downcast_ref::<tokio::task::JoinError>() {
                        assert!(join.is_panic());
                        assert_eq!(join.id(), original_worker);
                        found_original = true;
                    }
                } else if let Some(io) = current.downcast_ref::<std::io::Error>() {
                    assert_eq!(io.raw_os_error(), Some(5));
                    found_original = true;
                }
                source = current.source();
            }
            assert!(found_original, "original typed worker failure must survive both runtime owners");
        }
    }

    #[tokio::test]
    async fn vector_disabled_spawn_does_not_start_discovery() {
        let source = Source::new(vec![]);
        let runtime = spawn_vector_maintenance(
            VectorMaintenanceRuntimeConfig::default(),
            ServerContextSlot::new(),
            TestCatalogObjectBackend::default(),
            VectorMaintenanceExecutionContext {
                identity: identity(),
                source: source.clone(),
                executor: Arc::new(Executor),
            },
            None,
            &CancellationToken::new(),
        )
        .unwrap();
        assert!(runtime.is_none());
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn vector_discovery_keeps_empty_page_continuation() {
        let source = Source::new(vec![page(true), page(false)]);
        let mut scheduler = VectorMaintenanceScheduler::new(
            config(),
            TestCatalogObjectBackend::default(),
            source.clone(),
            Arc::new(Executor),
            identity(),
            None,
        )
        .unwrap();
        let token = CancellationToken::new();
        for _ in 0..2 {
            let report = scheduler
                .run_pass(&token, Instant::now() + Duration::from_secs(30))
                .await
                .unwrap();
            assert_eq!(report.started, 0);
        }
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn vector_shutdown_drains_owned_discovery_and_preserves_timeout() {
        let source = Source::new(vec![page(false)]);
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        *source.entered.lock().unwrap() = Some(entered);
        *source.release.lock().unwrap() = Some(resume);
        let mut scheduler = VectorMaintenanceScheduler::new(
            config(),
            TestCatalogObjectBackend::default(),
            source,
            Arc::new(Executor),
            identity(),
            None,
        )
        .unwrap();
        let token = CancellationToken::new();
        let runtime = CatalogBackgroundTask::start(&token, move |cancellation| async move {
            finish_owned_catalog_pass(
                scheduler.run_pass(&cancellation, Instant::now() + Duration::from_secs(30)),
                &cancellation,
                Instant::now() + Duration::from_secs(30),
                EVENT_VECTOR_SCHEDULER,
            )
            .await
            .unwrap();
            assert!(!scheduler.has_in_flight());
        });
        started.await.unwrap();
        let mut stop = Box::pin(crate::startup_shutdown::stop_catalog_background_task(Some(runtime)));
        tokio::select! { biased; result = stop.as_mut() => panic!("held discovery stopped: {result:?}"), _ = tokio::task::yield_now() => {} }
        tokio::time::advance(crate::server::SHUTDOWN_TIMEOUT + Duration::from_millis(1)).await;
        tokio::select! { biased; result = stop.as_mut() => panic!("held discovery lost owner: {result:?}"), _ = tokio::task::yield_now() => {} }
        release.send(()).unwrap();
        assert_eq!(stop.await.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
    }
}
