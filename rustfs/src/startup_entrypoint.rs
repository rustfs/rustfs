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
    config::{CommandResult, Config, Opt},
    startup_lifecycle::{StartupRuntimeLifecycle, run_startup_runtime_lifecycle},
    startup_preflight::{StartupServerPreflightError, bootstrap_external_prefix_compat, init_startup_server_preflight},
    startup_server::{StartupHttpServers, StartupListenContext, init_startup_http_servers, init_startup_listen_context},
    startup_services::init_startup_runtime_services,
    startup_storage::{StartupStorageRuntime, init_startup_storage_foundation, init_startup_storage_runtime},
    storage_api::server::http::ServerContextSlot,
    storage_api::startup::storage::bootstrap_instance_ctx,
};
use std::io::{Error, Result};
use tracing::{error, instrument};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_SERVER_RUNTIME_FAILED: &str = "server_runtime_failed";
const OBSERVABILITY_INIT_FATAL_ALREADY_REPORTED: &str = "observability initialization failure already reported";

pub fn run_process() {
    // Building the process runtime is a startup fatal boundary.
    let (runtime, dial9_guard) = crate::server::build_tokio_runtime().expect("Failed to build Tokio runtime");
    let result = runtime.block_on(async_main());

    // Flush and seal the trace segment before any exit path. `process::exit`
    // below does not run destructors, and neither does returning from `main`
    // for a guard held in a `static`. Under feature sets where the dial9 guard
    // carries no Drop impl (e.g. `--features sftp`), this is an intentional no-op.
    #[allow(clippy::drop_non_drop)]
    drop(dial9_guard);

    if let Err(ref e) = result {
        if e.to_string() != OBSERVABILITY_INIT_FATAL_ALREADY_REPORTED {
            // Tracing may not be initialized when startup fails this early.
            emit_fatal_stderr("Server runtime failed", e);
        }
        let _ = crate::startup_runtime_sources::shutdown_observability_guard();
        std::process::exit(1);
    }
}

fn format_fatal_stderr_message(context: &str, error: impl std::fmt::Display) -> String {
    format!("[FATAL] {context}: {error}")
}

fn emit_fatal_stderr(context: &str, error: impl std::fmt::Display) {
    // Pre-observability startup failures cannot rely on tracing.
    eprintln!("{}", format_fatal_stderr_message(context, error));
}

async fn async_main() -> Result<()> {
    #[cfg(feature = "e2e-test-hooks")]
    if let Ok(nonce) = std::env::var("RUSTFS_E2E_STARTUP_CAS_PROBE") {
        let nonce = uuid::Uuid::parse_str(&nonce).map_err(Error::other)?;
        // This precedes CLI parsing and observability, including `--help`.
        println!(
            "RUSTFS_E2E_STARTUP_CAS {}",
            serde_json::json!({
                "kind": "capability", "schema": "fresh-startup-cas/v1", "nonce": nonce,
            })
        );
        return Ok(());
    }
    #[cfg(feature = "e2e-test-hooks")]
    if let Ok(nonce) = std::env::var("RUSTFS_E2E_STARTUP_CAS_NONCE") {
        let nonce = uuid::Uuid::parse_str(&nonce).map_err(Error::other)?;
        let line = format!(
            "RUSTFS_E2E_STARTUP_CAS {}\n",
            serde_json::json!({
                "kind": "observer-ready", "nonce": nonce, "pid": std::process::id(),
            })
        );
        let _ = std::io::Write::write_all(&mut std::io::stderr().lock(), line.as_bytes());
    }
    hotpath::tokio_runtime!();

    // Log container resource detection early in startup
    // This helps operators verify that RustFS correctly detected cgroup limits
    crate::cgroup_resources::log_container_resources();

    let env_compat_report = bootstrap_external_prefix_compat()?;

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let command_result = match Opt::parse_command(args) {
        Ok(result) => result,
        Err(e) => {
            emit_fatal_stderr("Command parse failed", e);
            let _ = crate::startup_runtime_sources::shutdown_observability_guard();
            std::process::exit(1);
        }
    };

    // Execute subcommand, or prepare config for `server` subcommand
    let config = match command_result {
        CommandResult::Info(opts) => {
            crate::config::execute_info(&opts);
            return Ok(());
        }
        CommandResult::Tls(opts) => return crate::tls::execute_tls(&opts),
        // Diagnose short-circuits before observability init on purpose:
        // the report goes to stdout and must not be wrapped by the JSON logger.
        CommandResult::Diagnose(opts) => return crate::diagnose::execute_diagnose(&opts),
        // Inspect is offline like diagnose: read-only against drive paths, output
        // to stdout/--out, and must run before any observability/storage init.
        CommandResult::Inspect(opts) => return crate::inspect::execute_inspect(&opts).await,
        CommandResult::ConnectRegister(opts) => {
            let registered = crate::connect::register_from_protected_input(
                &opts.endpoint,
                &opts.ca_file,
                &opts.state_dir,
                opts.token_file.as_deref(),
            )
            .await
            .map_err(Error::other)?;
            println!("device={} cluster={}", registered.device_uid, registered.cluster_name);
            return Ok(());
        }
        CommandResult::Server(config) => config,
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
async fn run(config: Config) -> Result<()> {
    // Single-instance startup threads the process bootstrap context through
    // the storage path explicitly (Phase 5 follow-up, backlog#1052); a future
    // multi-instance server constructs its own context here instead.
    let instance_ctx = bootstrap_instance_ctx();
    let StartupListenContext {
        readiness,
        server_addr,
        server_address,
    } = init_startup_listen_context(&config, &instance_ctx).await?;

    let endpoint_pools = init_startup_storage_foundation(&server_address, &config.volumes, &instance_ctx).await?;
    let server_ctx = ServerContextSlot::with_instance_context(instance_ctx.clone());
    let StartupHttpServers {
        state_manager,
        s3_shutdown_tx,
        console_shutdown_tx,
    } = init_startup_http_servers(&config, readiness.clone(), server_ctx.clone()).await?;

    let StartupStorageRuntime {
        store,
        shutdown_token: ctx,
    } = init_startup_storage_runtime(server_addr, &endpoint_pools, readiness.clone(), instance_ctx).await?;

    #[cfg(feature = "e2e-test-hooks")]
    if let Ok(nonce) = std::env::var("RUSTFS_E2E_STARTUP_CAS_NONCE") {
        let nonce = uuid::Uuid::parse_str(&nonce).map_err(Error::other)?;
        let release = std::path::PathBuf::from(
            std::env::var_os("RUSTFS_E2E_STARTUP_CAS_RELEASE")
                .ok_or_else(|| Error::other("startup CAS fixture requires a release path"))?,
        );
        if server_ctx.installed_object_store().is_some() {
            return Err(Error::other("startup CAS gate reached an installed slot"));
        }
        let line = format!(
            "RUSTFS_E2E_STARTUP_CAS {}\n",
            serde_json::json!({
                "kind": "gate", "nonce": nonce, "pid": std::process::id(), "slot_installed": false,
            })
        );
        let _ = std::io::Write::write_all(&mut std::io::stderr().lock(), line.as_bytes());
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
            while !tokio::fs::try_exists(&release).await? {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            Ok::<_, Error>(())
        })
        .await
        .map_err(|_| Error::other("startup CAS gate release timed out"))??;
    }

    let capacity_tasks = crate::capacity::capacity_integration::init_capacity_management_managed().await;

    let service_runtime = init_startup_runtime_services(
        &config,
        endpoint_pools,
        store.clone(),
        ctx.clone(),
        readiness.clone(),
        state_manager.clone(),
        server_ctx,
    )
    .await?;

    run_startup_runtime_lifecycle(StartupRuntimeLifecycle {
        server_address,
        state_manager,
        s3_shutdown_tx,
        console_shutdown_tx,
        capacity_tasks,
        service_runtime,
        store,
        shutdown_token: ctx,
        readiness,
    })
    .await
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
}
