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

    pub(crate) async fn shutdown(mut self, grace: Duration) -> bool {
        self.cancellation.cancel();
        let Some(task) = self.task.take() else {
            return true;
        };
        let joined = matches!(tokio::time::timeout(grace, task).await, Ok(Ok(())));
        if !joined {
            warn!(
                event = EVENT_PERIODIC_SCHEDULER,
                component = LOG_COMPONENT,
                subsystem = LOG_SUBSYSTEM,
                state = "drain_unconfirmed",
                "Maintenance scheduler completion is unconfirmed; owned work was not aborted"
            );
        }
        joined
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

    #[tokio::test(start_paused = true)]
    async fn periodic_shutdown_reports_incomplete_drain_without_aborting() {
        let completed = Arc::new(AtomicBool::new(false));
        let done = completed.clone();
        let (release, resume) = tokio::sync::oneshot::channel();
        let (finished, observed) = tokio::sync::oneshot::channel();
        let runtime = PeriodicMaintenanceRuntime {
            cancellation: CancellationToken::new(),
            task: Some(tokio::spawn(async move {
                resume.await.unwrap();
                done.store(true, Ordering::SeqCst);
                finished.send(()).unwrap();
            })),
        };
        assert!(!runtime.shutdown(Duration::from_secs(30)).await);
        assert!(!completed.load(Ordering::SeqCst));
        release.send(()).unwrap();
        observed.await.unwrap();
        assert!(completed.load(Ordering::SeqCst));
        let runtime = PeriodicMaintenanceRuntime {
            cancellation: CancellationToken::new(),
            task: Some(tokio::spawn(async {})),
        };
        assert!(runtime.shutdown(Duration::from_secs(30)).await);
    }
}
