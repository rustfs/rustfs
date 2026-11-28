// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Startup and shutdown ownership for one server's Vector maintenance worker.

use super::*;
use crate::runtime_sources::ServerContextSlot;
use crate::table_catalog::{
    CatalogBackgroundTask, CatalogBackgroundTaskError, VectorMaintenanceExecutionIdentityV1, VectorMaintenancePolicyV1,
    VectorMaintenanceRuntimeConfig, spawn_vector_maintenance,
};
use tokio_util::sync::CancellationToken;

const LOG_COMPONENT: &str = "vector";
const LOG_SUBSYSTEM: &str = "maintenance";
const EVENT_VECTOR_MAINTENANCE_ADMISSION: &str = "vector_maintenance_admission";

pub(crate) fn spawn(
    server: Arc<ServerContextSlot>,
    node_admission: Option<Arc<rustfs_concurrency::adaptive::NodeAdmission>>,
    shutdown: &CancellationToken,
) -> CatalogBackgroundTask {
    CatalogBackgroundTask::start_fallible(shutdown, move |cancellation| async move {
        let app = loop {
            if cancellation.is_cancelled() {
                return Ok::<(), CatalogBackgroundTaskError>(());
            }
            if let Some(app) = server.installed_app_context()
                && app.iam().is_ready()
            {
                break app;
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
        };
        let result = admitted_runtime(app, server, node_admission, &cancellation).await;
        let mut worker = match result {
            Ok(Some(worker)) => worker,
            Ok(None) => return Ok(()),
            Err(error) => {
                tracing::error!(
                    event = EVENT_VECTOR_MAINTENANCE_ADMISSION,
                    component = LOG_COMPONENT, subsystem = LOG_SUBSYSTEM, error = %error,
                    "Vector maintenance startup admission failed"
                );
                return Err(CatalogBackgroundTaskError::Worker(Box::new(error)));
            }
        };
        // The outer task keeps the original owner until every admitted operation joins.
        // Its caller retains the original shutdown deadline when this takes longer.
        match worker.join_result().await {
            Some(Ok(())) => Ok(()),
            Some(Err(error)) => {
                tracing::error!(
                    event = EVENT_VECTOR_MAINTENANCE_ADMISSION,
                    component = LOG_COMPONENT, subsystem = LOG_SUBSYSTEM, error = %error,
                    "Vector maintenance worker terminated unexpectedly"
                );
                Err(error)
            }
            None => Err(CatalogBackgroundTaskError::Worker(Box::new(std::io::Error::other(
                "Vector maintenance worker completion was not observed",
            )))),
        }
    })
}

async fn admitted_runtime(
    app: Arc<AppContext>,
    server: Arc<ServerContextSlot>,
    node_admission: Option<Arc<rustfs_concurrency::adaptive::NodeAdmission>>,
    cancellation: &CancellationToken,
) -> Result<Option<CatalogBackgroundTask>, VectorMaintenanceExecutionError> {
    let snapshot = app.server_config().get().unwrap_or_default();
    let config = VectorMaintenanceRuntimeConfig::from_server_config(&snapshot)?;
    if !config.enabled {
        return Ok(None);
    }
    let (identity, _) = resolve_identity(&app, &config.execution_account)
        .await
        .map_err(maintenance_execution_error)?;
    let execution_identity =
        VectorMaintenanceExecutionIdentityV1::try_new(identity.account().into(), *identity.user_generation())?;
    let backend = super::super::Backend::new_with_managed_candidate_reader(
        app.object_store(),
        Arc::new(super::super::dataset::CandidateReader::default()),
    );
    let policy = VectorMaintenancePolicyV1::try_new(
        config.lease_duration.as_secs(),
        config.heartbeat_interval.as_secs(),
        config.max_attempts.get(),
        config.retry_initial_backoff.as_secs(),
        config.retry_max_backoff.as_secs(),
    )?;
    let source = Arc::new(source::MaintenanceSource {
        app: app.clone(),
        backend: backend.clone(),
        identity: execution_identity.clone(),
        policy,
        gc_scan: parking_lot::Mutex::new(None),
    });
    let limits = ManifestIoLimits {
        max_manifest_bytes: std::num::NonZeroU64::new(64 * 1024 * 1024).expect("positive manifest read limit"),
        max_concurrent_reads: config.io_concurrency,
    };
    let executor = Arc::new(MaintenanceExecutor::new(app, identity, limits, source.clone()));
    spawn_vector_maintenance(
        config,
        server,
        backend,
        crate::table_catalog::VectorMaintenanceExecutionContext {
            identity: execution_identity,
            source,
            executor,
        },
        node_admission,
        cancellation,
    )
    .map_err(Into::into)
}
