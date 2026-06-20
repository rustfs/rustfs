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
    server::{ServiceState, ServiceStateManager, ShutdownHandle, ShutdownSignal, shutdown_event_notifier, stop_audit_system},
    startup_optional_runtime_sidecars::{
        OptionalRuntimeServices, prepare_optional_runtime_shutdowns, shutdown_optional_runtime_services,
    },
    storage_compat::shutdown_background_services,
};
use rustfs_heal::shutdown_ahm_services;
use rustfs_utils::get_env_bool_with_aliases;
use std::path::Path;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const ENV_SCANNER_ENABLED: &str = "RUSTFS_SCANNER_ENABLED";
const ENV_SCANNER_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_SCANNER";
const ENV_HEAL_ENABLED: &str = "RUSTFS_HEAL_ENABLED";
const ENV_HEAL_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_HEAL";
const LOG_COMPONENT_MAIN: &str = "main";
const LOG_COMPONENT_EMBEDDED: &str = "embedded";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const LOG_SUBSYSTEM_EMBEDDED: &str = "embedded";
const EVENT_AUDIT_SYSTEM_STATE: &str = "audit_system_state";
const EVENT_EMBEDDED_SERVER_STATE: &str = "embedded_server_state";
const EVENT_EMBEDDED_SHUTDOWN_CLEANUP_FAILED: &str = "embedded_shutdown_cleanup_failed";
const EVENT_SHUTDOWN_SIGNAL_RECEIVED: &str = "shutdown_signal_received";
const EVENT_BACKGROUND_SERVICE_SHUTDOWN: &str = "background_service_shutdown";
const EVENT_EVENT_NOTIFIER_SHUTDOWN: &str = "event_notifier_shutdown";
const EVENT_PROFILING_SHUTDOWN: &str = "profiling_shutdown";
const EVENT_SERVER_SHUTDOWN_STATE: &str = "server_shutdown_state";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackgroundShutdownStep {
    DataScanner,
    Ahm,
}

fn background_shutdown_steps(enable_scanner: bool, enable_heal: bool) -> Vec<BackgroundShutdownStep> {
    let mut steps = Vec::with_capacity(2);
    if enable_scanner {
        steps.push(BackgroundShutdownStep::DataScanner);
    }
    if enable_heal || enable_scanner {
        steps.push(BackgroundShutdownStep::Ahm);
    }
    steps
}

pub async fn run_startup_shutdown_sequence(
    state_manager: &ServiceStateManager,
    shutdown_signal: ShutdownSignal,
    s3_shutdown_handle: Option<ShutdownHandle>,
    console_shutdown_handle: Option<ShutdownHandle>,
    optional_runtimes: OptionalRuntimeServices,
    ctx: CancellationToken,
) {
    ctx.cancel();

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_SHUTDOWN_SIGNAL_RECEIVED,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        signal = shutdown_signal.log_label(),
        "Shutdown signal received"
    );
    state_manager.update(ServiceState::Stopping);

    let enable_scanner = get_env_bool_with_aliases(ENV_SCANNER_ENABLED, &[ENV_SCANNER_ENABLED_DEPRECATED], true);
    let enable_heal = get_env_bool_with_aliases(ENV_HEAL_ENABLED, &[ENV_HEAL_ENABLED_DEPRECATED], true);

    let background_steps = background_shutdown_steps(enable_scanner, enable_heal);
    for step in &background_steps {
        match step {
            BackgroundShutdownStep::DataScanner => {
                info!(
                    target: "rustfs::main::handle_shutdown",
                    event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    service = "data_scanner",
                    state = "stopping",
                    "Background service shutdown started"
                );
                shutdown_background_services();
            }
            BackgroundShutdownStep::Ahm => {
                info!(
                    target: "rustfs::main::handle_shutdown",
                    event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    service = "ahm",
                    state = "stopping",
                    "Background service shutdown started"
                );
                shutdown_ahm_services();
            }
        }
    }

    if background_steps.is_empty() {
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            service = "ahm",
            state = "skipped",
            reason = "disabled",
            "Background service shutdown skipped"
        );
    }

    let optional_runtime_shutdowns = prepare_optional_runtime_shutdowns(optional_runtimes);

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_EVENT_NOTIFIER_SHUTDOWN,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Event notifier shutdown started"
    );
    shutdown_event_notifier().await;

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_AUDIT_SYSTEM_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Audit runtime stopping"
    );
    match stop_audit_system().await {
        Ok(_) => info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "stopped",
            "Audit runtime stopped"
        ),
        Err(e) => error!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "stop_failed",
            error = %e,
            "Audit runtime failed to stop"
        ),
    }

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_PROFILING_SHUTDOWN,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Profiling shutdown started"
    );
    crate::startup_profiling::shutdown_profiling_runtime();

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_SERVER_SHUTDOWN_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "RustFS server stopping"
    );
    if let Some(s3_shutdown_handle) = s3_shutdown_handle {
        s3_shutdown_handle.shutdown().await;
    }
    if let Some(console_shutdown_handle) = console_shutdown_handle {
        console_shutdown_handle.shutdown().await;
    }
    shutdown_optional_runtime_services(optional_runtime_shutdowns).await;
    state_manager.update(ServiceState::Stopped);
    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_SERVER_SHUTDOWN_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopped",
        "RustFS server stopped"
    );
}

pub async fn run_embedded_shutdown_cleanup() {
    shutdown_event_notifier().await;

    if let Err(err) = stop_audit_system().await {
        warn!(
            component = LOG_COMPONENT_EMBEDDED,
            subsystem = LOG_SUBSYSTEM_EMBEDDED,
            event = EVENT_EMBEDDED_SHUTDOWN_CLEANUP_FAILED,
            service = "audit",
            error = %err,
            "Embedded shutdown cleanup failed"
        );
    }
}

pub(crate) fn signal_embedded_startup_shutdown(shutdown_handle: &ShutdownHandle, ctx: &CancellationToken) {
    shutdown_handle.signal();
    ctx.cancel();
}

pub async fn run_embedded_server_shutdown(
    ctx: &CancellationToken,
    shutdown_handle: &mut Option<ShutdownHandle>,
    temp_dir: Option<&Path>,
) {
    info!(
        target: "rustfs::embedded",
        component = LOG_COMPONENT_EMBEDDED,
        subsystem = LOG_SUBSYSTEM_EMBEDDED,
        event = EVENT_EMBEDDED_SERVER_STATE,
        state = "stopping",
        "Embedded server state changed"
    );

    ctx.cancel();

    run_embedded_shutdown_cleanup().await;

    if let Some(shutdown_handle) = shutdown_handle.take() {
        shutdown_handle.shutdown().await;
    }

    if let Some(dir) = temp_dir
        && let Err(err) = tokio::fs::remove_dir_all(dir).await
    {
        warn!(
            component = LOG_COMPONENT_EMBEDDED,
            subsystem = LOG_SUBSYSTEM_EMBEDDED,
            event = EVENT_EMBEDDED_SHUTDOWN_CLEANUP_FAILED,
            service = "temp_dir",
            path = %dir.display(),
            error = %err,
            "Embedded shutdown cleanup failed"
        );
    }

    info!(
        target: "rustfs::embedded",
        component = LOG_COMPONENT_EMBEDDED,
        subsystem = LOG_SUBSYSTEM_EMBEDDED,
        event = EVENT_EMBEDDED_SERVER_STATE,
        state = "stopped",
        "Embedded server state changed"
    );
}

#[cfg(test)]
mod tests {
    use super::{BackgroundShutdownStep, background_shutdown_steps, signal_embedded_startup_shutdown};
    use crate::server::ShutdownHandle;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn background_shutdown_plan_keeps_scanner_before_ahm() {
        assert_eq!(
            background_shutdown_steps(true, true),
            vec![BackgroundShutdownStep::DataScanner, BackgroundShutdownStep::Ahm]
        );
        assert_eq!(
            background_shutdown_steps(true, false),
            vec![BackgroundShutdownStep::DataScanner, BackgroundShutdownStep::Ahm]
        );
        assert_eq!(background_shutdown_steps(false, true), vec![BackgroundShutdownStep::Ahm]);
        assert!(background_shutdown_steps(false, false).is_empty());
    }

    #[tokio::test]
    async fn signal_embedded_startup_shutdown_signals_handle_and_cancels_token() {
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
        let shutdown_task = tokio::spawn(async move {
            let _ = shutdown_rx.recv().await;
        });
        let shutdown_handle = ShutdownHandle::new(shutdown_tx, shutdown_task);
        let cancel_token = CancellationToken::new();

        signal_embedded_startup_shutdown(&shutdown_handle, &cancel_token);

        tokio::time::timeout(Duration::from_secs(1), shutdown_handle.wait())
            .await
            .expect("shutdown task should observe startup signal");
        assert!(cancel_token.is_cancelled());
    }
}
