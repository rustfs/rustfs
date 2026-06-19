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

use crate::{
    server::{ServiceStateManager, ShutdownHandle, wait_for_shutdown},
    startup_iam::publish_ready_for_iam_bootstrap,
    startup_services::StartupServiceRuntime,
    startup_shutdown::run_startup_shutdown_sequence,
    storage_compat::ECStore,
};
use rustfs_common::GlobalReadiness;
use rustfs_scanner::init_data_scanner;
use std::{io::Result, sync::Arc};
use tokio_util::sync::CancellationToken;
use tracing::info;

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_SERVER_READY: &str = "server_ready";
const EVENT_SERVER_SHUTDOWN_STATE: &str = "server_shutdown_state";

pub struct StartupRuntimeLifecycle {
    pub server_address: String,
    pub state_manager: Arc<ServiceStateManager>,
    pub s3_shutdown_tx: Option<ShutdownHandle>,
    pub console_shutdown_tx: Option<ShutdownHandle>,
    pub service_runtime: StartupServiceRuntime,
    pub store: Arc<ECStore>,
    pub shutdown_token: CancellationToken,
    pub readiness: Arc<GlobalReadiness>,
}

pub async fn run_startup_runtime_lifecycle(lifecycle: StartupRuntimeLifecycle) -> Result<()> {
    let StartupRuntimeLifecycle {
        server_address,
        state_manager,
        s3_shutdown_tx,
        console_shutdown_tx,
        service_runtime,
        store,
        shutdown_token,
        readiness,
    } = lifecycle;
    let StartupServiceRuntime {
        optional_runtimes,
        iam_bootstrap,
        enable_scanner,
    } = service_runtime;

    info!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_READY,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        version = %crate::version::get_version(),
        server_address = %server_address,
        started_at = %jiff::Zoned::now(),
        iam_bootstrap = ?iam_bootstrap,
        "RustFS server ready"
    );
    publish_ready_for_iam_bootstrap(iam_bootstrap, readiness.as_ref(), Some(state_manager.as_ref())).await?;
    rustfs_common::set_global_init_time_now().await;

    if enable_scanner {
        init_data_scanner(shutdown_token.clone(), store).await;
    }

    let shutdown_signal = wait_for_shutdown().await;
    run_startup_shutdown_sequence(
        &state_manager,
        shutdown_signal,
        s3_shutdown_tx,
        console_shutdown_tx,
        optional_runtimes,
        shutdown_token,
    )
    .await;

    info!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_SHUTDOWN_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = ?state_manager.current_state(),
        result = "stopped",
        "RustFS server stopped"
    );
    Ok(())
}
