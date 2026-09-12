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
    config::{
        CommandResult, Config, ConnectLicenseCommands, ConnectLicenseScopeOpts, ConnectProfileOpts, ConnectProfileTool,
        ConnectThreadProfileScope, Opt,
    },
    startup_lifecycle::{StartupRuntimeLifecycle, run_startup_runtime_lifecycle},
    startup_preflight::{StartupServerPreflightError, bootstrap_external_prefix_compat, init_startup_server_preflight},
    startup_server::{StartupHttpServers, StartupListenContext, init_startup_http_servers, init_startup_listen_context},
    startup_services::init_startup_runtime_services,
    startup_storage::{StartupStorageRuntime, init_startup_storage_foundation, init_startup_storage_runtime},
    storage_api::server::http::ServerContextSlot,
    storage_api::startup::storage::bootstrap_instance_ctx,
};
use std::io::{Error, Read as _, Result};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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
        CommandResult::ConnectLicense(command) => return execute_connect_license(command),
        CommandResult::ConnectProfile(options) => return execute_connect_profile(options).await,
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

async fn execute_connect_profile(options: ConnectProfileOpts) -> Result<()> {
    use crate::connect::{
        IdentityStore, LocalProfileConsent, ProfileCaptureRequest, ProfileProvenance, ThreadProfileScope, export_cpu_profile,
        export_memory_profile, export_thread_profile, save_signed_profile_export,
    };
    use rand::{TryRng as _, rngs::SysRng};

    let key = IdentityStore::new(options.state_dir.join("identity"))
        .load()
        .map_err(Error::other)?
        .ok_or_else(|| Error::other("connect profile requires an enrolled device identity"))?;
    let executable_sha256 = hash_current_executable()?;
    let produced_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(Error::other)
        .and_then(|duration| i64::try_from(duration.as_secs()).map_err(Error::other))?;
    let mut nonce = [0_u8; 32];
    SysRng.try_fill_bytes(&mut nonce).map_err(Error::other)?;
    let request = ProfileCaptureRequest {
        organization_name: options.organization,
        cluster_name: options.cluster,
        device_name: options.device,
        run_uid: options.run_uid,
        artifact_uid: options.artifact_uid,
        schema_version: options.schema_version,
        capability: options.capability,
        consent: LocalProfileConsent {
            consent_uid: options.consent_uid,
            policy_revision: options.policy_revision,
            expires_at_unix: options.consent_expires_at_unix,
            confirmed: options.acknowledge_l3,
        },
        produced_at_unix,
        expires_at_unix: options.expires_at_unix,
        nonce,
        duration: Duration::from_millis(options.duration_millis),
        sample_period: Duration::from_micros(options.sample_period_micros),
        provenance: ProfileProvenance::new(
            crate::version::build::COMMIT_HASH,
            executable_sha256,
            env!("CARGO_PKG_VERSION"),
            enabled_build_features(),
        ),
    };
    let cancel = tokio_util::sync::CancellationToken::new();
    let capture = async {
        match options.tool {
            ConnectProfileTool::Cpu => {
                if options.thread_scope.is_some() {
                    return Err(Error::other("--thread-scope is valid only for the threads profile"));
                }
                export_cpu_profile(&request, &key, &cancel).map_err(Error::other)
            }
            ConnectProfileTool::Memory => {
                if options.thread_scope.is_some() {
                    return Err(Error::other("--thread-scope is valid only for the threads profile"));
                }
                export_memory_profile(&request, &key, &cancel).await.map_err(Error::other)
            }
            ConnectProfileTool::Threads => {
                let scope = match options.thread_scope {
                    Some(ConnectThreadProfileScope::TokioRuntime) => ThreadProfileScope::TokioRuntime,
                    Some(ConnectThreadProfileScope::NativeThreads) => ThreadProfileScope::NativeThreads,
                    None => return Err(Error::other("--thread-scope is required for the threads profile")),
                };
                export_thread_profile(&request, scope, &key, &cancel).map_err(Error::other)
            }
        }
    };
    tokio::pin!(capture);
    let export = tokio::select! {
        biased;
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(Error::other)?;
            cancel.cancel();
            return Err(Error::other("profile collection cancelled"));
        }
        result = capture.as_mut() => result?,
    };
    drop(capture);
    let tool = export.tool;
    let outcome = export.outcome;
    let reason_code = export.reason_code;
    let output = options.output;
    let writer_cancel = cancel.clone();
    let mut writer = tokio::task::spawn_blocking(move || save_signed_profile_export(&output, &export, &writer_cancel));
    let receipt = tokio::select! {
        biased;
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(Error::other)?;
            cancel.cancel();
            writer.await.map_err(Error::other)?.map_err(Error::other)?
        }
        result = &mut writer => result.map_err(Error::other)?.map_err(Error::other)?,
    };

    println!("tool={} outcome={} reason={}", tool.id(), outcome.as_str(), reason_code.as_str());
    println!(
        "artifact={} bytes={} sha256={}",
        receipt.artifact_uid, receipt.archive_size_bytes, receipt.archive_sha256
    );
    println!("upload=not-performed");
    Ok(())
}

fn hash_current_executable() -> Result<String> {
    use sha2::{Digest as _, Sha256};

    const MAX_EXECUTABLE_BYTES: u64 = 1_073_741_824;
    let path = std::env::current_exe().map_err(Error::other)?;
    let mut file = std::fs::File::open(path).map_err(Error::other)?;
    let metadata = file.metadata().map_err(Error::other)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_EXECUTABLE_BYTES {
        return Err(Error::other("current executable is outside the profile provenance limit"));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut read_bytes = 0_u64;
    loop {
        let count = file.read(&mut buffer).map_err(Error::other)?;
        if count == 0 {
            break;
        }
        read_bytes = read_bytes
            .checked_add(u64::try_from(count).map_err(Error::other)?)
            .ok_or_else(|| Error::other("current executable is outside the profile provenance limit"))?;
        if read_bytes > MAX_EXECUTABLE_BYTES {
            return Err(Error::other("current executable is outside the profile provenance limit"));
        }
        hasher.update(&buffer[..count]);
    }
    if read_bytes != metadata.len() {
        return Err(Error::other("current executable changed while hashing profile provenance"));
    }
    Ok(hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower))
}

fn enabled_build_features() -> Vec<String> {
    let mut features = Vec::new();
    for (enabled, name) in [
        (cfg!(feature = "connect-e2e-short-credentials"), "connect-e2e-short-credentials"),
        (cfg!(feature = "dial9"), "dial9"),
        (cfg!(feature = "e2e-test-hooks"), "e2e-test-hooks"),
        (cfg!(feature = "ftps"), "ftps"),
        (cfg!(feature = "full"), "full"),
        (cfg!(feature = "gcs"), "gcs"),
        (cfg!(feature = "hotpath"), "hotpath"),
        (cfg!(feature = "hotpath-alloc"), "hotpath-alloc"),
        (cfg!(feature = "hotpath-cpu"), "hotpath-cpu"),
        (cfg!(feature = "io-scheduler-debug"), "io-scheduler-debug"),
        (cfg!(feature = "license"), "license"),
        (cfg!(feature = "metrics-gpu"), "metrics-gpu"),
        (cfg!(feature = "offline-enrollment-e2e-root"), "offline-enrollment-e2e-root"),
        (cfg!(feature = "pyroscope"), "pyroscope"),
        (cfg!(feature = "rio-v2"), "rio-v2"),
        (cfg!(feature = "sftp"), "sftp"),
        (cfg!(feature = "swift"), "swift"),
        (cfg!(feature = "tracing-chunk-debug"), "tracing-chunk-debug"),
        (cfg!(feature = "webdav"), "webdav"),
    ] {
        if enabled {
            features.push(name.to_owned());
        }
    }
    features
}

fn execute_connect_license(command: ConnectLicenseCommands) -> Result<()> {
    use crate::connect::{apply_license_artifact, inspect_installed_license, verify_license_artifact};

    let scope = match &command {
        ConnectLicenseCommands::Import(options) | ConnectLicenseCommands::Verify(options) => &options.scope,
        ConnectLicenseCommands::Show(options) => options,
    };
    let context = license_context(scope);
    let report = match context {
        Ok(context) => match &command {
            ConnectLicenseCommands::Import(options) => apply_license_artifact(&options.artifact, &scope.state_dir, &context),
            ConnectLicenseCommands::Verify(options) => verify_license_artifact(&options.artifact, &scope.state_dir, &context),
            ConnectLicenseCommands::Show(_) => inspect_installed_license(&scope.state_dir, &context),
        }
        .unwrap_or_else(|error| {
            let installed = matches!(&command, ConnectLicenseCommands::Show(_))
                && error.status != crate::connect::LicenseArtifactStatus::Missing;
            error.report(installed)
        }),
        Err(error) => error.report(false),
    };
    print_license_report(&report)?;
    if report.is_valid() {
        Ok(())
    } else {
        Err(Error::other(format!("Connect service license status is {}", report.status)))
    }
}

fn license_context(
    scope: &ConnectLicenseScopeOpts,
) -> std::result::Result<crate::connect::LicenseVerificationContext, crate::connect::LicenseArtifactError> {
    crate::connect::LicenseVerificationContext::from_public_key_file(
        &scope.public_key_file,
        scope.key_id.clone(),
        scope.issuer.clone(),
        scope.audience.clone(),
        scope.organization.clone(),
        scope.deployment.clone(),
        scope.service_code.clone(),
    )
}

fn print_license_report(report: &crate::connect::LicenseReport) -> Result<()> {
    let output = serde_json::to_string(report).map_err(Error::other)?;
    println!("{output}");
    Ok(())
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
