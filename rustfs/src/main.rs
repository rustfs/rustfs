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

// Ensure the correct path for parse_license is imported
use futures_util::future::join_all;
use rustfs::server::{
    ServiceState, ServiceStateManager, ShutdownHandle, ShutdownSignal, shutdown_event_notifier, stop_audit_system,
    wait_for_shutdown,
};
use rustfs::startup_iam::publish_ready_for_iam_bootstrap;
use rustfs::startup_preflight::{StartupServerPreflightError, bootstrap_external_prefix_compat, init_startup_server_preflight};
use rustfs::startup_protocols::ProtocolShutdownSenders;
use rustfs::startup_server::{StartupHttpServers, StartupListenContext, init_startup_http_servers, init_startup_listen_context};
use rustfs::startup_services::{StartupServiceRuntime, init_startup_runtime_services};
use rustfs::startup_storage::{StartupStorageRuntime, init_startup_storage_foundation, init_startup_storage_runtime};
use rustfs_ecstore::global::shutdown_background_services;
use rustfs_heal::shutdown_ahm_services;
use rustfs_scanner::init_data_scanner;
use rustfs_utils::get_env_bool_with_aliases;
use std::io::{Error, Result};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};

const ENV_SCANNER_ENABLED: &str = "RUSTFS_SCANNER_ENABLED";
const ENV_SCANNER_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_SCANNER";
const ENV_HEAL_ENABLED: &str = "RUSTFS_HEAL_ENABLED";
const ENV_HEAL_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_HEAL";
const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_SERVER_RUNTIME_FAILED: &str = "server_runtime_failed";
const EVENT_PROTOCOL_SYSTEM_STATE: &str = "protocol_system_state";
const EVENT_AUDIT_SYSTEM_STATE: &str = "audit_system_state";
const EVENT_SERVER_READY: &str = "server_ready";
const EVENT_SHUTDOWN_SIGNAL_RECEIVED: &str = "shutdown_signal_received";
const EVENT_BACKGROUND_SERVICE_SHUTDOWN: &str = "background_service_shutdown";
const EVENT_EVENT_NOTIFIER_SHUTDOWN: &str = "event_notifier_shutdown";
const EVENT_PROFILING_SHUTDOWN: &str = "profiling_shutdown";
const EVENT_SERVER_SHUTDOWN_STATE: &str = "server_shutdown_state";
const OBSERVABILITY_INIT_FATAL_ALREADY_REPORTED: &str = "observability initialization failure already reported";
#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn format_fatal_stderr_message(context: &str, error: impl std::fmt::Display) -> String {
    format!("[FATAL] {context}: {error}")
}

fn emit_fatal_stderr(context: &str, error: impl std::fmt::Display) {
    eprintln!("{}", format_fatal_stderr_message(context, error));
}

fn main() {
    // Build Tokio runtime with optional dial9 telemetry support
    let runtime = rustfs::server::build_tokio_runtime().expect("Failed to build Tokio runtime");
    let result = runtime.block_on(async_main());
    if let Err(ref e) = result {
        if e.to_string() != OBSERVABILITY_INIT_FATAL_ALREADY_REPORTED {
            // Tracing may not be initialized when startup fails this early.
            emit_fatal_stderr("Server runtime failed", e);
        }
        std::process::exit(1);
    }
}

async fn async_main() -> Result<()> {
    let env_compat_report = bootstrap_external_prefix_compat()?;

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let command_result = match rustfs::config::Opt::parse_command(args) {
        Ok(result) => result,
        Err(e) => {
            emit_fatal_stderr("Command parse failed", e);
            std::process::exit(1);
        }
    };

    // Execute subcommand, or prepare config for `server` subcommand
    let config = match command_result {
        rustfs::config::CommandResult::Info(opts) => {
            rustfs::config::execute_info(&opts);
            return Ok(());
        }
        rustfs::config::CommandResult::Tls(opts) => return rustfs::tls::execute_tls(&opts),
        rustfs::config::CommandResult::Server(config) => config,
    };

    match init_startup_server_preflight(&config, &env_compat_report).await {
        Ok(()) => {}
        Err(StartupServerPreflightError::ObservabilityInit(err)) => {
            // Structured logging is unavailable until observability initializes.
            emit_fatal_stderr("Observability initialization failed", err);
            return Err(Error::other(OBSERVABILITY_INIT_FATAL_ALREADY_REPORTED));
        }
        Err(StartupServerPreflightError::Other(err)) => return Err(err),
    }

    // Run parameters
    match run(*config).await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!(
                target: "rustfs::main",
                event = EVENT_SERVER_RUNTIME_FAILED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                error = %e,
                "Server runtime failed"
            );
            Err(e)
        }
    }
}

#[instrument(skip(config))]
async fn run(config: rustfs::config::Config) -> Result<()> {
    let StartupListenContext {
        readiness,
        server_addr,
        server_address,
    } = init_startup_listen_context(&config).await?;

    let endpoint_pools = init_startup_storage_foundation(&server_address, &config.volumes).await?;
    let StartupHttpServers {
        state_manager,
        s3_shutdown_tx,
        console_shutdown_tx,
    } = init_startup_http_servers(&config, readiness.clone()).await?;

    let StartupStorageRuntime {
        store,
        shutdown_token: ctx,
    } = init_startup_storage_runtime(server_addr, &endpoint_pools, readiness.clone()).await?;

    let StartupServiceRuntime {
        protocol_shutdowns,
        iam_bootstrap,
        enable_scanner,
    } = init_startup_runtime_services(
        &config,
        endpoint_pools,
        store.clone(),
        ctx.clone(),
        readiness.clone(),
        state_manager.clone(),
    )
    .await?;

    info!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_READY,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        version = %rustfs::version::get_version(),
        server_address = %server_address,
        started_at = %jiff::Zoned::now(),
        iam_bootstrap = ?iam_bootstrap,
        "RustFS server ready"
    );
    publish_ready_for_iam_bootstrap(iam_bootstrap, readiness.as_ref(), Some(state_manager.as_ref())).await?;
    // Set the global RustFS initialization time to now
    rustfs_common::set_global_init_time_now().await;

    if enable_scanner {
        init_data_scanner(ctx.clone(), store.clone()).await;
    }

    // listen to the shutdown signal
    let shutdown_signal = wait_for_shutdown().await;
    handle_shutdown(
        &state_manager,
        shutdown_signal,
        s3_shutdown_tx,
        console_shutdown_tx,
        protocol_shutdowns,
        ctx.clone(),
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

/// Handles the shutdown process of the server
async fn handle_shutdown(
    state_manager: &ServiceStateManager,
    shutdown_signal: ShutdownSignal,
    s3_shutdown_handle: Option<ShutdownHandle>,
    console_shutdown_handle: Option<ShutdownHandle>,
    protocols: ProtocolShutdownSenders,
    ctx: CancellationToken,
) {
    let ProtocolShutdownSenders {
        ftp: ftp_shutdown_tx,
        ftps: ftps_shutdown_tx,
        webdav: webdav_shutdown_tx,
        sftp: sftp_shutdown_tx,
    } = protocols;
    ctx.cancel();

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_SHUTDOWN_SIGNAL_RECEIVED,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        signal = shutdown_signal.log_label(),
        "Shutdown signal received"
    );
    // update the status to stopping first
    state_manager.update(ServiceState::Stopping);

    // Check environment variables to determine what services need to be stopped
    let enable_scanner = get_env_bool_with_aliases(ENV_SCANNER_ENABLED, &[ENV_SCANNER_ENABLED_DEPRECATED], true);
    let enable_heal = get_env_bool_with_aliases(ENV_HEAL_ENABLED, &[ENV_HEAL_ENABLED_DEPRECATED], true);

    // Stop background services based on what was enabled.
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

    // Shutdown FTP and FTPS servers
    let mut protocol_shutdowns = Vec::new();
    if let Some(ftp_shutdown_tx) = ftp_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_PROTOCOL_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            protocol = "ftp",
            state = "stopping",
            "Protocol runtime stopping"
        );
        protocol_shutdowns.push(ftp_shutdown_tx.shutdown());
    }

    if let Some(ftps_shutdown_tx) = ftps_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_PROTOCOL_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            protocol = "ftps",
            state = "stopping",
            "Protocol runtime stopping"
        );
        protocol_shutdowns.push(ftps_shutdown_tx.shutdown());
    }

    // Shutdown WebDAV server
    if let Some(webdav_shutdown_tx) = webdav_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_PROTOCOL_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            protocol = "webdav",
            state = "stopping",
            "Protocol runtime stopping"
        );
        protocol_shutdowns.push(webdav_shutdown_tx.shutdown());
    }

    // Shutdown SFTP server
    if let Some(sftp_shutdown_tx) = sftp_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_PROTOCOL_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            protocol = "sftp",
            state = "stopping",
            "Protocol runtime stopping"
        );
        protocol_shutdowns.push(sftp_shutdown_tx.shutdown());
    }

    // Stop the notification system
    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_EVENT_NOTIFIER_SHUTDOWN,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Event notifier shutdown started"
    );
    shutdown_event_notifier().await;

    // Stop the audit system
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

    // Stop profiling tasks
    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_PROFILING_SHUTDOWN,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Profiling shutdown started"
    );
    rustfs::profiling::shutdown_profiling();

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
    join_all(protocol_shutdowns).await;

    // the last updated status is stopped
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_stderr_message_uses_consistent_prefix_and_context() {
        assert_eq!(
            format_fatal_stderr_message("Observability initialization failed", "collector unavailable"),
            "[FATAL] Observability initialization failed: collector unavailable"
        );
    }

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
}
