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

use crate::app::object::OnDemandMigrationWriteBack;
use crate::module_switches::{on_demand_migration_enabled_from_env, set_on_demand_migration_module_enabled};
use crate::on_demand_migration::OnDemandMigrationSys;
use crate::storage_api::startup::bucket_metadata::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage_api::startup::bucket_metadata::{
    ECStore, Error as StorageError, Result as StorageResult, get_global_replication_pool, init_bucket_metadata_sys,
    reconcile_bucket_resync_target_intents, try_migrate_bucket_metadata, try_migrate_iam_config,
};
use std::{
    future::Future,
    io::{Error as IoError, Result as IoResult},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const EVENT_ON_DEMAND_MIGRATION_RUNTIME_INITIALIZED: &str = "on_demand_migration_runtime_initialized";
const EVENT_REPLICATION_RESYNC_STARTUP_BACKGROUND_CANCELED: &str = "replication_resync_startup_background_canceled";
const EVENT_REPLICATION_RESYNC_STARTUP_BACKGROUND_COMPLETED: &str = "replication_resync_startup_background_completed";
const EVENT_REPLICATION_RESYNC_STARTUP_BACKGROUND_FAILED: &str = "replication_resync_startup_background_failed";
const EVENT_REPLICATION_RESYNC_STARTUP_BACKGROUND_STARTED: &str = "replication_resync_startup_background_started";
const LOG_COMPONENT_STARTUP_BUCKET_METADATA: &str = "startup_bucket_metadata";
const LOG_SUBSYSTEM_ON_DEMAND_MIGRATION: &str = "on_demand_migration";
const LOG_SUBSYSTEM_REPLICATION: &str = "replication";
const IAM_MIGRATION_MAX_RETRIES: usize = 15;
const IAM_MIGRATION_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const METRIC_REPLICATION_RESYNC_STARTUP_BACKGROUND_DURATION_SECONDS: &str =
    "rustfs_replication_resync_startup_background_duration_seconds";
const METRIC_REPLICATION_RESYNC_STARTUP_BACKGROUND_EVENTS_TOTAL: &str =
    "rustfs_replication_resync_startup_background_events_total";
const METRIC_REPLICATION_RESYNC_STARTUP_BACKGROUND_STATUS: &str = "rustfs_replication_resync_startup_background_status";
const STARTUP_BACKGROUND_MODE_EMBEDDED: &str = "embedded";
const STARTUP_BACKGROUND_MODE_SERVER: &str = "server";
const STARTUP_BACKGROUND_OUTCOME_CANCELED: &str = "canceled";
const STARTUP_BACKGROUND_OUTCOME_FAILED: &str = "failed";
const STARTUP_BACKGROUND_OUTCOME_STARTED: &str = "started";
const STARTUP_BACKGROUND_OUTCOME_SUCCEEDED: &str = "succeeded";
const STARTUP_BACKGROUND_STATUS_FAILED: f64 = 0.0;
const STARTUP_BACKGROUND_STATUS_SUCCEEDED: f64 = 1.0;
const STARTUP_BACKGROUND_STATUS_RUNNING: f64 = 2.0;
const STARTUP_BACKGROUND_STATUS_CANCELED: f64 = 3.0;

pub(crate) async fn init_embedded_bucket_metadata_runtime(store: Arc<ECStore>, ctx: &CancellationToken) -> IoResult<Vec<String>> {
    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(|err| IoError::other(format!("list_bucket: {err}")))?;

    let buckets: Vec<String> = buckets_list.into_iter().map(|v| v.name).collect();

    try_migrate_bucket_metadata(store.clone()).await?;
    init_on_demand_migration_runtime();
    init_bucket_metadata_sys(store.clone(), buckets.clone()).await;
    try_migrate_iam_config(store).await?;
    spawn_bucket_resync_startup_reconcile(buckets.clone(), ctx.clone(), false);

    Ok(buckets)
}

pub(crate) async fn init_bucket_metadata_runtime(store: Arc<ECStore>, ctx: CancellationToken) -> IoResult<Vec<String>> {
    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(IoError::other)?;

    let buckets: Vec<String> = buckets_list.into_iter().map(|v| v.name).collect();

    try_migrate_bucket_metadata(store.clone()).await?;

    retry_iam_config_migration(|| try_migrate_iam_config(store.clone())).await?;
    init_on_demand_migration_runtime();
    init_bucket_metadata_sys(store, buckets.clone()).await;
    spawn_bucket_resync_startup_reconcile(buckets.clone(), ctx, true);

    Ok(buckets)
}

async fn retry_iam_config_migration<Operation, OperationFuture>(mut operation: Operation) -> IoResult<()>
where
    Operation: FnMut() -> OperationFuture,
    OperationFuture: Future<Output = IoResult<()>>,
{
    retry_iam_config_migration_with(&mut operation, IAM_MIGRATION_MAX_RETRIES, IAM_MIGRATION_RETRY_INTERVAL).await
}

async fn retry_iam_config_migration_with<Operation, OperationFuture>(
    operation: &mut Operation,
    max_retries: usize,
    retry_interval: Duration,
) -> IoResult<()>
where
    Operation: FnMut() -> OperationFuture,
    OperationFuture: Future<Output = IoResult<()>>,
{
    let mut retries = 0;
    loop {
        match operation().await {
            Ok(()) => return Ok(()),
            Err(error) if iam_migration_error_is_retryable(&error) && retries < max_retries => {
                retries += 1;
                tracing::warn!(
                    component = LOG_COMPONENT_STARTUP_BUCKET_METADATA,
                    subsystem = "iam_migration",
                    retry_count = retries,
                    max_retries,
                    error = %error,
                    "IAM config migration hit a transient quorum error; retrying"
                );
                tokio::time::sleep(retry_interval).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn iam_migration_error_is_retryable(error: &IoError) -> bool {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<StorageError>())
        .is_some_and(StorageError::is_quorum_error)
}

/// Publishes the on-demand migration module switch, installs the app-layer
/// write-back the pull pipeline stores objects with (rustfs/backlog#2153),
/// and registers the runtime's config hook before bucket metadata is
/// loaded, so every cache install path (initial load included) reaches
/// `OnDemandMigrationSys` with a usable write-back (rustfs/backlog#2152).
/// Idempotent across embedded and server startups.
fn init_on_demand_migration_runtime() {
    crate::on_demand_migration::register_metrics();
    let enabled = on_demand_migration_enabled_from_env();
    set_on_demand_migration_module_enabled(enabled);
    let sys = OnDemandMigrationSys::get();
    sys.set_module_enabled(enabled);
    sys.set_write_back(Arc::new(OnDemandMigrationWriteBack::new()));
    let hook_registered = sys.register_config_hook();
    tracing::info!(
        event = EVENT_ON_DEMAND_MIGRATION_RUNTIME_INITIALIZED,
        component = LOG_COMPONENT_STARTUP_BUCKET_METADATA,
        subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
        state = if enabled { "enabled" } else { "disabled" },
        hook_registered,
        "On-demand migration runtime initialized"
    );
}

fn spawn_bucket_resync_startup_reconcile(buckets: Vec<String>, ctx: CancellationToken, init_resync_after_reconcile: bool) {
    tokio::spawn(async move {
        describe_bucket_resync_startup_background_metrics();
        let bucket_count = buckets.len();
        let mode = bucket_resync_startup_background_mode(init_resync_after_reconcile);
        let started = Instant::now();

        record_bucket_resync_startup_background_started(mode);
        tracing::info!(
            event = EVENT_REPLICATION_RESYNC_STARTUP_BACKGROUND_STARTED,
            component = LOG_COMPONENT_STARTUP_BUCKET_METADATA,
            subsystem = LOG_SUBSYSTEM_REPLICATION,
            state = STARTUP_BACKGROUND_OUTCOME_STARTED,
            mode,
            bucket_count,
            init_resync_after_reconcile,
            "Bucket metadata startup resync reconcile started in background"
        );

        if let Err(error) = run_bucket_resync_startup_reconcile(buckets, ctx, init_resync_after_reconcile).await {
            if !report_bucket_resync_startup_background_error(&error) {
                record_bucket_resync_startup_background_finished(mode, STARTUP_BACKGROUND_OUTCOME_CANCELED, started.elapsed());
                tracing::debug!(
                    event = EVENT_REPLICATION_RESYNC_STARTUP_BACKGROUND_CANCELED,
                    component = LOG_COMPONENT_STARTUP_BUCKET_METADATA,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    result = STARTUP_BACKGROUND_OUTCOME_CANCELED,
                    mode,
                    bucket_count,
                    init_resync_after_reconcile,
                    duration_ms = started.elapsed().as_millis() as u64,
                    "Bucket metadata startup resync reconcile canceled during shutdown"
                );
                return;
            }
            record_bucket_resync_startup_background_finished(mode, STARTUP_BACKGROUND_OUTCOME_FAILED, started.elapsed());
            tracing::error!(
                event = EVENT_REPLICATION_RESYNC_STARTUP_BACKGROUND_FAILED,
                component = LOG_COMPONENT_STARTUP_BUCKET_METADATA,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                result = "failed",
                mode,
                bucket_count,
                init_resync_after_reconcile,
                duration_ms = started.elapsed().as_millis() as u64,
                error = %error,
                "Bucket metadata startup resync reconcile failed in background"
            );
            return;
        }

        record_bucket_resync_startup_background_finished(mode, STARTUP_BACKGROUND_OUTCOME_SUCCEEDED, started.elapsed());
        tracing::info!(
            event = EVENT_REPLICATION_RESYNC_STARTUP_BACKGROUND_COMPLETED,
            component = LOG_COMPONENT_STARTUP_BUCKET_METADATA,
            subsystem = LOG_SUBSYSTEM_REPLICATION,
            result = "ok",
            mode,
            bucket_count,
            init_resync_after_reconcile,
            duration_ms = started.elapsed().as_millis() as u64,
            "Bucket metadata startup resync reconcile completed in background"
        );
    });
}

fn describe_bucket_resync_startup_background_metrics() {
    static DESCRIBE: std::sync::Once = std::sync::Once::new();
    DESCRIBE.call_once(|| {
        metrics::describe_counter!(
            METRIC_REPLICATION_RESYNC_STARTUP_BACKGROUND_EVENTS_TOTAL,
            "Bucket metadata startup resync background task events, by fixed mode and outcome"
        );
        metrics::describe_histogram!(
            METRIC_REPLICATION_RESYNC_STARTUP_BACKGROUND_DURATION_SECONDS,
            "Bucket metadata startup resync background task duration in seconds, by fixed mode and outcome"
        );
        metrics::describe_gauge!(
            METRIC_REPLICATION_RESYNC_STARTUP_BACKGROUND_STATUS,
            "Latest bucket metadata startup resync background task status by fixed mode: 0=failed, 1=succeeded, 2=running, 3=canceled"
        );
    });
}

fn bucket_resync_startup_background_mode(init_resync_after_reconcile: bool) -> &'static str {
    if init_resync_after_reconcile {
        STARTUP_BACKGROUND_MODE_SERVER
    } else {
        STARTUP_BACKGROUND_MODE_EMBEDDED
    }
}

fn record_bucket_resync_startup_background_started(mode: &'static str) {
    metrics::counter!(
        METRIC_REPLICATION_RESYNC_STARTUP_BACKGROUND_EVENTS_TOTAL,
        "mode" => mode,
        "outcome" => STARTUP_BACKGROUND_OUTCOME_STARTED
    )
    .increment(1);
    metrics::gauge!(METRIC_REPLICATION_RESYNC_STARTUP_BACKGROUND_STATUS, "mode" => mode).set(STARTUP_BACKGROUND_STATUS_RUNNING);
}

fn record_bucket_resync_startup_background_finished(mode: &'static str, outcome: &'static str, duration: Duration) {
    let status = match outcome {
        STARTUP_BACKGROUND_OUTCOME_SUCCEEDED => STARTUP_BACKGROUND_STATUS_SUCCEEDED,
        STARTUP_BACKGROUND_OUTCOME_CANCELED => STARTUP_BACKGROUND_STATUS_CANCELED,
        _ => STARTUP_BACKGROUND_STATUS_FAILED,
    };
    metrics::counter!(
        METRIC_REPLICATION_RESYNC_STARTUP_BACKGROUND_EVENTS_TOTAL,
        "mode" => mode,
        "outcome" => outcome
    )
    .increment(1);
    metrics::histogram!(
        METRIC_REPLICATION_RESYNC_STARTUP_BACKGROUND_DURATION_SECONDS,
        "mode" => mode,
        "outcome" => outcome
    )
    .record(duration.as_secs_f64());
    metrics::gauge!(METRIC_REPLICATION_RESYNC_STARTUP_BACKGROUND_STATUS, "mode" => mode).set(status);
}

async fn run_bucket_resync_startup_reconcile(
    buckets: Vec<String>,
    ctx: CancellationToken,
    init_resync_after_reconcile: bool,
) -> StorageResult<()> {
    reconcile_bucket_resync_target_intents(&buckets, &ctx).await?;

    if init_resync_after_reconcile {
        let Some(pool) = get_global_replication_pool() else {
            return Err(StorageError::other("replication pool is not initialized"));
        };
        pool.init_resync(ctx, buckets).await?;
    }

    Ok(())
}

fn report_bucket_resync_startup_background_error(error: &StorageError) -> bool {
    !matches!(error, StorageError::OperationCanceled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_resync_background_error_reporting_skips_shutdown() {
        assert!(!report_bucket_resync_startup_background_error(&StorageError::OperationCanceled));
        assert!(report_bucket_resync_startup_background_error(&StorageError::other(
            "replication pool is not initialized"
        )));
    }

    #[test]
    fn startup_resync_background_observability_uses_fixed_modes_and_statuses() {
        assert_eq!(bucket_resync_startup_background_mode(true), STARTUP_BACKGROUND_MODE_SERVER);
        assert_eq!(bucket_resync_startup_background_mode(false), STARTUP_BACKGROUND_MODE_EMBEDDED);
        assert_eq!(STARTUP_BACKGROUND_STATUS_FAILED, 0.0);
        assert_eq!(STARTUP_BACKGROUND_STATUS_SUCCEEDED, 1.0);
        assert_eq!(STARTUP_BACKGROUND_STATUS_RUNNING, 2.0);
        assert_eq!(STARTUP_BACKGROUND_STATUS_CANCELED, 3.0);
    }

    #[tokio::test]
    async fn iam_migration_retries_only_quorum_errors() {
        let mut attempts = 0;
        retry_iam_config_migration_with(
            &mut || {
                attempts += 1;
                std::future::ready(if attempts == 1 {
                    Err(IoError::other(StorageError::InsufficientReadQuorum(
                        ".minio.sys".into(),
                        "config/iam/".into(),
                    )))
                } else {
                    Ok(())
                })
            },
            1,
            Duration::ZERO,
        )
        .await
        .expect("quorum recovery must allow startup to continue");
        assert_eq!(attempts, 2);

        let mut deterministic_attempts = 0;
        let error = retry_iam_config_migration_with(
            &mut || {
                deterministic_attempts += 1;
                std::future::ready(Err(IoError::other("incompatible IAM metadata")))
            },
            1,
            Duration::ZERO,
        )
        .await
        .expect_err("deterministic migration errors must fail immediately");
        assert_eq!(error.to_string(), "incompatible IAM metadata");
        assert_eq!(deterministic_attempts, 1);
    }
}
