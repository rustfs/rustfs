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

use crate::startup_fs_guard::enforce_unsupported_fs_policy;
use crate::storage_api::startup::storage::{
    ECStore, EndpointServerPools, init_background_replication, init_ecstore_config, init_global_config_sys, init_local_disks,
    init_lock_clients, prewarm_local_disk_id_map, set_global_endpoints, try_migrate_server_config, update_erasure_type,
};
use rustfs_common::{GlobalReadiness, SystemStage};
use std::{
    io::{Error, Result},
    net::SocketAddr,
    sync::Arc,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_COMPONENT_EMBEDDED: &str = "embedded";
const LOG_SUBSYSTEM_STORAGE: &str = "storage";
const LOG_SUBSYSTEM_EMBEDDED: &str = "embedded";
const GLOBAL_CONFIG_INIT_MAX_RETRIES: usize = 15;
const EVENT_ENDPOINT_PARSING_STARTED: &str = "endpoint_parsing_started";
const EVENT_STARTUP_STORAGE_STAGE: &str = "startup_storage_stage";
const EVENT_STORAGE_POOL_FORMATTING: &str = "storage_pool_formatting";
const EVENT_STORAGE_POOL_HOST_RISK: &str = "storage_pool_host_risk";
const EVENT_EMBEDDED_STORAGE_INIT_FAILED: &str = "embedded_storage_init_failed";
const EVENT_EMBEDDED_STORAGE_INIT_RETRY: &str = "embedded_storage_init_retry";

pub(crate) struct StartupStorageRuntime {
    pub(crate) store: Arc<ECStore>,
    pub(crate) shutdown_token: CancellationToken,
}

pub(crate) async fn init_startup_storage_foundation(server_address: &str, volumes: &[String]) -> Result<EndpointServerPools> {
    info!(
        target: "rustfs::main::run",
        event = EVENT_ENDPOINT_PARSING_STARTED,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STORAGE,
        server_address = %server_address,
        volume_count = volumes.len(),
        "Starting endpoint parsing"
    );
    let (endpoint_pools, setup_type) = EndpointServerPools::from_volumes(server_address, volumes.to_vec())
        .await
        .inspect_err(|err| {
            error!(
                target: "rustfs::main::run",
                event = EVENT_STARTUP_STORAGE_STAGE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                stage = "endpoint_parsing",
                state = "failed",
                error = ?err,
                "Endpoint parsing failed"
            );
        })
        .map_err(Error::other)?;
    enforce_unsupported_fs_policy(&endpoint_pools)?;

    set_global_endpoints(endpoint_pools.as_ref().clone());
    update_erasure_type(setup_type).await;

    debug!(
        target: "rustfs::main::run",
        event = EVENT_STARTUP_STORAGE_STAGE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STORAGE,
        stage = "local_disk_initialization",
        state = "starting",
        "starting local disk initialization"
    );
    init_local_disks(endpoint_pools.clone())
        .await
        .inspect_err(|err| {
            error!(
                target: "rustfs::main::run",
                event = EVENT_STARTUP_STORAGE_STAGE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                stage = "local_disk_initialization",
                state = "failed",
                error = ?err,
                "Local disk initialization failed"
            );
        })
        .map_err(Error::other)?;
    prewarm_local_disk_id_map().await;
    init_lock_clients(endpoint_pools.clone());

    log_storage_pool_layout(&endpoint_pools);

    Ok(endpoint_pools)
}

pub(crate) async fn init_embedded_startup_storage_foundation(
    server_address: &str,
    volumes: &[String],
) -> Result<EndpointServerPools> {
    let (endpoint_pools, setup_type) = EndpointServerPools::from_volumes(server_address, volumes.to_vec())
        .await
        .map_err(|err| Error::other(format!("endpoints: {err}")))?;
    enforce_unsupported_fs_policy(&endpoint_pools).map_err(|err| Error::other(format!("unsupported fs guard: {err}")))?;

    set_global_endpoints(endpoint_pools.as_ref().clone());
    update_erasure_type(setup_type).await;

    init_local_disks(endpoint_pools.clone())
        .await
        .map_err(|err| Error::other(format!("local disks: {err}")))?;
    init_lock_clients(endpoint_pools.clone());

    Ok(endpoint_pools)
}

pub(crate) async fn init_startup_storage_runtime(
    server_addr: SocketAddr,
    endpoint_pools: &EndpointServerPools,
    readiness: Arc<GlobalReadiness>,
) -> Result<StartupStorageRuntime> {
    let ctx = CancellationToken::new();

    debug!(
        target: "rustfs::main::run",
        event = EVENT_STARTUP_STORAGE_STAGE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STORAGE,
        stage = "ecstore_initialization",
        state = "starting",
        "starting ECStore initialization"
    );
    let store = ECStore::new(server_addr, endpoint_pools.clone(), ctx.clone())
        .await
        .inspect_err(|err| {
            error!(
                target: "rustfs::main::run",
                event = EVENT_STARTUP_STORAGE_STAGE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                stage = "ecstore_initialization",
                state = "failed",
                error = ?err,
                "ECStore initialization failed"
            );
        })?;

    init_startup_storage_global_config(store.clone()).await?;
    readiness.mark_stage(SystemStage::StorageReady);
    init_background_replication(store.clone()).await;

    Ok(StartupStorageRuntime {
        store,
        shutdown_token: ctx,
    })
}

pub(crate) async fn init_embedded_startup_storage_runtime(
    server_addr: SocketAddr,
    endpoint_pools: &EndpointServerPools,
    readiness: Arc<GlobalReadiness>,
    shutdown_token: CancellationToken,
) -> Result<StartupStorageRuntime> {
    let store = match ECStore::new(server_addr, endpoint_pools.clone(), shutdown_token.clone()).await {
        Ok(store) => store,
        Err(err) => {
            error!(
                component = LOG_COMPONENT_EMBEDDED,
                subsystem = LOG_SUBSYSTEM_EMBEDDED,
                event = EVENT_EMBEDDED_STORAGE_INIT_FAILED,
                stage = "ecstore_new",
                error = ?err,
                "Embedded storage initialization failed"
            );
            return Err(Error::other(format!("ECStore: {err}")));
        }
    };

    init_embedded_startup_storage_global_config(store.clone()).await?;
    readiness.mark_stage(SystemStage::StorageReady);
    init_background_replication(store.clone()).await;

    Ok(StartupStorageRuntime { store, shutdown_token })
}

async fn init_startup_storage_global_config(store: Arc<ECStore>) -> Result<()> {
    init_ecstore_config();
    try_migrate_server_config(store.clone()).await;

    let mut retry_count = 0;
    while let Err(e) = init_global_config_sys(store.clone()).await {
        let next_retry_count = retry_count + 1;
        error!(
            target: "rustfs::main::run",
            event = EVENT_STARTUP_STORAGE_STAGE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            stage = "global_config_initialization",
            state = "retrying",
            retry_count = next_retry_count,
            error = ?e,
            "Global config initialization retry failed"
        );
        // TODO: check error type
        retry_count = next_retry_count;
        if global_config_retry_exhausted(retry_count) {
            return Err(Error::other("ecconfig::init_global_config_sys failed"));
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

async fn init_embedded_startup_storage_global_config(store: Arc<ECStore>) -> Result<()> {
    init_ecstore_config();
    try_migrate_server_config(store.clone()).await;

    let mut retry = 0;
    while let Err(err) = init_global_config_sys(store.clone()).await {
        retry += 1;
        if retry > GLOBAL_CONFIG_INIT_MAX_RETRIES {
            return Err(Error::other(format!(
                "init_global_config_sys failed after {GLOBAL_CONFIG_INIT_MAX_RETRIES} retries: {err}"
            )));
        }
        debug!(
            component = LOG_COMPONENT_EMBEDDED,
            subsystem = LOG_SUBSYSTEM_EMBEDDED,
            event = EVENT_EMBEDDED_STORAGE_INIT_RETRY,
            stage = "global_config_sys",
            retry,
            error = %err,
            "Embedded storage initialization retry scheduled"
        );
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

fn log_storage_pool_layout(endpoint_pools: &EndpointServerPools) {
    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        info!(
            target: "rustfs::main::run",
            event = EVENT_STORAGE_POOL_FORMATTING,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            pool_id = i + 1,
            set_count = eps.set_count,
            drives_per_set = eps.drives_per_set,
            "Formatting storage pool"
        );

        if storage_pool_has_host_failure_risk(eps.drives_per_set) {
            warn!(
                target: "rustfs::main::run",
                event = EVENT_STORAGE_POOL_HOST_RISK,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                pool_id = i + 1,
                drives_per_set = eps.drives_per_set,
                risk = "host_failure_data_unavailable",
                "Detected multi-drive local set host failure risk"
            );
        }
    }

    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        debug!(
            target: "rustfs::main::run",
            id = i,
            set_count = eps.set_count,
            drives_per_set = eps.drives_per_set,
            cmd = ?eps.cmd_line,
            "created endpoints {}, set_count:{}, drives_per_set: {}, cmd: {:?}",
            i, eps.set_count, eps.drives_per_set, eps.cmd_line
        );

        for ep in eps.endpoints.as_ref().iter() {
            debug!(
                target: "rustfs::main::run",
                "  - endpoint: {}", ep
            );
        }
    }
}

fn storage_pool_has_host_failure_risk(drives_per_set: usize) -> bool {
    drives_per_set > 1
}

fn global_config_retry_exhausted(retry_count: usize) -> bool {
    retry_count > GLOBAL_CONFIG_INIT_MAX_RETRIES
}

#[cfg(test)]
mod tests {
    use super::{global_config_retry_exhausted, storage_pool_has_host_failure_risk};

    #[test]
    fn reports_host_failure_risk_only_for_multi_drive_sets() {
        assert!(!storage_pool_has_host_failure_risk(0));
        assert!(!storage_pool_has_host_failure_risk(1));
        assert!(storage_pool_has_host_failure_risk(2));
    }

    #[test]
    fn global_config_retry_limit_matches_startup_policy() {
        assert!(!global_config_retry_exhausted(15));
        assert!(global_config_retry_exhausted(16));
    }
}
