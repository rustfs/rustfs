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
    ConfiguredTableCatalogStore, EcStoreTableCatalogObjectBackend, ObjectTableCatalogStore, StrongTableCatalogStore,
    TableCatalogBackingMode, TableCatalogStoreError, TableCatalogStoreResult,
};
use std::{collections::BTreeSet, future::Future, sync::Arc, time::Duration};
use tokio::{task::JoinHandle, time::Instant};
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

/// Owns queueing until completion; never runs a worker or deletes table objects.
pub(crate) struct PeriodicMaintenanceRuntime {
    cancellation: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl Drop for PeriodicMaintenanceRuntime {
    fn drop(&mut self) {
        // Detach, never abort: in-flight sidecar writes retain their publication owner.
        self.cancellation.cancel();
    }
}

impl PeriodicMaintenanceRuntime {
    pub(crate) fn spawn(config: PeriodicMaintenanceConfig, server: Arc<ServerContextSlot>, shutdown: &CancellationToken) -> Self {
        let cancellation = shutdown.child_token();
        let task = tokio::spawn(run(config, server, cancellation.clone()));
        Self {
            cancellation,
            task: Some(task),
        }
    }

    pub(crate) fn request_stop(&self) {
        self.cancellation.cancel();
    }

    pub(crate) async fn join(&mut self) -> Option<Result<(), tokio::task::JoinError>> {
        // A cancelled waiter leaves the actual task in this owner.
        let result = self.task.as_mut()?.await;
        self.task = None;
        Some(result)
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
    tokio::pin!(pass);
    tokio::select! {
        biased;
        result = &mut pass => result,
        _ = cancellation.cancelled() => pass.await,
        _ = tokio::time::sleep_until(deadline) => {
            warn!(
                event = EVENT_PERIODIC_SCHEDULER, component = LOG_COMPONENT, subsystem = LOG_SUBSYSTEM,
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
            let backend = EcStoreTableCatalogObjectBackend::new_with_strong_runtime(
                context.object_store(),
                context.table_catalog_strong_runtime(),
            );
            let store = match config.backing {
                TableCatalogBackingMode::ObjectBacked => {
                    ConfiguredTableCatalogStore::ObjectBacked(ObjectTableCatalogStore::new(backend))
                }
                TableCatalogBackingMode::DurableStrong => {
                    ConfiguredTableCatalogStore::DurableStrong(StrongTableCatalogStore::new_requiring_snapshot(backend))
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
            || Ok(TableCatalogBackingMode::DurableStrong),
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
        let mut runtime = PeriodicMaintenanceRuntime {
            cancellation: CancellationToken::new(),
            task: Some(task),
        };
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
        let runtime = PeriodicMaintenanceRuntime {
            cancellation,
            task: Some(task),
        };
        started.await.expect("real owned pass must be held");
        let mut stopping = Box::pin(crate::startup_shutdown::stop_table_maintenance(Some(runtime)));
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
            let runtime = PeriodicMaintenanceRuntime {
                cancellation,
                task: Some(task),
            };
            started.await.expect("actual task is held");
            let mut stopping = Box::pin(crate::startup_shutdown::stop_table_maintenance(Some(runtime)));
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
            let source = std::error::Error::source(failure).expect("original JoinError source");
            let source = source
                .downcast_ref::<tokio::task::JoinError>()
                .expect("JoinError type retained");
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
            let runtime = PeriodicMaintenanceRuntime {
                cancellation: CancellationToken::new(),
                task: Some(task),
            };
            entered_rx.await.expect("the original task must start before observation");
            let mut stopping = Box::pin(crate::startup_shutdown::stop_table_maintenance(Some(runtime)));
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
                let source = std::error::Error::source(failure).expect("actual late JoinError source");
                let source = source
                    .downcast_ref::<tokio::task::JoinError>()
                    .expect("original JoinError type");
                assert!(source.is_panic());
                assert_eq!(source.id(), task_id, "late error must identify the original task");
            } else {
                assert!(error.to_string().contains("owned task joined"));
            }
        }
    }

    #[tokio::test]
    async fn periodic_shutdown_accepts_ready_and_disabled_but_rejects_missing_join() {
        crate::startup_shutdown::stop_table_maintenance(None)
            .await
            .expect("disabled maintenance has no owner to drain");
        let task = tokio::spawn(async {});
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        let runtime = PeriodicMaintenanceRuntime {
            cancellation: CancellationToken::new(),
            task: Some(task),
        };
        crate::startup_shutdown::stop_table_maintenance(Some(runtime))
            .await
            .expect("actual promptly completed task must join");
        let runtime = PeriodicMaintenanceRuntime {
            cancellation: CancellationToken::new(),
            task: None,
        };
        let error = crate::startup_shutdown::stop_table_maintenance(Some(runtime))
            .await
            .expect_err("configured runtime with no join observation must fail closed");
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert!(error.to_string().contains("actual join unconfirmed"));
    }
}
