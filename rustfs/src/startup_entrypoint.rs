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
};
use std::io::{Error, Result};
use tracing::{error, instrument};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_SERVER_RUNTIME_FAILED: &str = "server_runtime_failed";
const OBSERVABILITY_INIT_FATAL_ALREADY_REPORTED: &str = "observability initialization failure already reported";

pub fn run_process() {
    // Building the process runtime is a startup fatal boundary.
    let runtime = crate::server::build_tokio_runtime().expect("Failed to build Tokio runtime");
    let result = runtime.block_on(async_main());
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

    let service_runtime = init_startup_runtime_services(
        &config,
        endpoint_pools,
        store.clone(),
        ctx.clone(),
        readiness.clone(),
        state_manager.clone(),
    )
    .await?;

    run_startup_runtime_lifecycle(StartupRuntimeLifecycle {
        server_address,
        state_manager,
        s3_shutdown_tx,
        console_shutdown_tx,
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
