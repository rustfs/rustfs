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

use crate::{config::Config, license::init_license, startup_runtime::init_startup_runtime_foundation, startup_runtime_sources};
use rustfs_utils::{ExternalEnvCompatReport, apply_external_env_compat};
use std::{
    fmt,
    io::{Error, Result},
};
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_EXTERNAL_ENV_COMPAT_CONFLICT: &str = "external_env_compat_conflict";
const EVENT_EXTERNAL_ENV_COMPAT_APPLIED: &str = "external_env_compat_applied";
const EVENT_OBSERVABILITY_GUARD_SET: &str = "observability_guard_set";
const EVENT_OBSERVABILITY_GUARD_SET_FAILED: &str = "observability_guard_set_failed";

#[derive(Debug)]
pub(crate) enum StartupServerPreflightError {
    ObservabilityInit(Error),
    Other(Error),
}

impl StartupServerPreflightError {
    fn as_io_error(&self) -> &Error {
        match self {
            Self::ObservabilityInit(err) | Self::Other(err) => err,
        }
    }
}

impl fmt::Display for StartupServerPreflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_io_error().fmt(formatter)
    }
}

impl std::error::Error for StartupServerPreflightError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.as_io_error())
    }
}

pub(crate) fn bootstrap_external_prefix_compat() -> Result<ExternalEnvCompatReport> {
    let env_compat_report = apply_external_env_compat();
    Ok(env_compat_report)
}

pub(crate) async fn init_startup_server_preflight(
    config: &Config,
    env_compat_report: &ExternalEnvCompatReport,
) -> std::result::Result<(), StartupServerPreflightError> {
    crate::config::init_config_snapshot(config);
    init_license(config.license.clone());
    init_startup_observability(config.obs_endpoint.clone()).await?;
    log_external_prefix_compat_report(env_compat_report);
    init_startup_runtime_foundation(config)
        .await
        .map_err(StartupServerPreflightError::Other)
}

async fn init_startup_observability(obs_endpoint: String) -> std::result::Result<(), StartupServerPreflightError> {
    let guard = startup_runtime_sources::init_observability_guard(obs_endpoint)
        .await
        .map_err(|err| StartupServerPreflightError::ObservabilityInit(Error::other(err)))?;

    match startup_runtime_sources::set_observability_guard(guard).map_err(Error::other) {
        Ok(_) => {
            debug!(
                target: "rustfs::main",
                event = EVENT_OBSERVABILITY_GUARD_SET,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                result = "ok",
                "Stored global observability guard"
            );
            Ok(())
        }
        Err(err) => {
            error!(
                target: "rustfs::main",
                event = EVENT_OBSERVABILITY_GUARD_SET_FAILED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                error = %err,
                "Failed to store global observability guard"
            );
            Err(StartupServerPreflightError::Other(err))
        }
    }
}

fn format_external_prefix_mappings(report: &ExternalEnvCompatReport) -> String {
    report
        .mapped_pairs
        .iter()
        .map(|(source_key, rustfs_key)| format!("{source_key}->{rustfs_key}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn log_external_prefix_compat_report(report: &ExternalEnvCompatReport) {
    if report.conflict_count() > 0 {
        warn!(
            target: "rustfs::main",
            event = EVENT_EXTERNAL_ENV_COMPAT_CONFLICT,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            conflict_count = report.conflict_count(),
            conflict_keys = %report.conflict_keys.join(", "),
            "Detected external-prefix compatibility conflicts; keeping RUSTFS_ values"
        );
    }

    if report.mapped_count() > 0 {
        info!(
            target: "rustfs::main",
            event = EVENT_EXTERNAL_ENV_COMPAT_APPLIED,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            mapped_count = report.mapped_count(),
            mapped_pairs = %format_external_prefix_mappings(report),
            "Applied external-prefix compatibility mappings"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_external_prefix_mappings_lists_mapped_pairs() {
        let report = ExternalEnvCompatReport {
            mapped_pairs: vec![
                ("MINIO_ROOT_USER".to_string(), "RUSTFS_ROOT_USER".to_string()),
                (
                    "MINIO_NOTIFY_WEBHOOK_ENABLE_PRIMARY".to_string(),
                    "RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY".to_string(),
                ),
            ],
            conflict_keys: Vec::new(),
        };

        let formatted = format_external_prefix_mappings(&report);

        assert_eq!(
            formatted,
            "MINIO_ROOT_USER->RUSTFS_ROOT_USER, MINIO_NOTIFY_WEBHOOK_ENABLE_PRIMARY->RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY"
        );
    }

    #[test]
    fn preflight_error_preserves_observability_init_classification() {
        let err = StartupServerPreflightError::ObservabilityInit(Error::other("collector unavailable"));

        assert!(matches!(&err, StartupServerPreflightError::ObservabilityInit(_)));
        assert_eq!(err.to_string(), "collector unavailable");
        assert!(std::error::Error::source(&err).is_some());
    }
}
