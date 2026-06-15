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
use rustfs_ecstore::{
    endpoints::EndpointServerPools,
    set_global_endpoints,
    store::{init_local_disks, init_lock_clients, prewarm_local_disk_id_map},
    update_erasure_type,
};
use std::io::{Error, Result};
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STORAGE: &str = "storage";
const EVENT_ENDPOINT_PARSING_STARTED: &str = "endpoint_parsing_started";
const EVENT_STARTUP_STORAGE_STAGE: &str = "startup_storage_stage";
const EVENT_STORAGE_POOL_FORMATTING: &str = "storage_pool_formatting";
const EVENT_STORAGE_POOL_HOST_RISK: &str = "storage_pool_host_risk";

pub async fn init_startup_storage_foundation(server_address: &str, volumes: &[String]) -> Result<EndpointServerPools> {
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

#[cfg(test)]
mod tests {
    use super::storage_pool_has_host_failure_risk;

    #[test]
    fn reports_host_failure_risk_only_for_multi_drive_sets() {
        assert!(!storage_pool_has_host_failure_risk(0));
        assert!(!storage_pool_has_host_failure_risk(1));
        assert!(storage_pool_has_host_failure_risk(2));
    }
}
