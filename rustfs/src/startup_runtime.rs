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

use crate::{config::Config, license::license_status};
use rustls::crypto::aws_lc_rs::default_provider;
use std::io::{Error, Result};
use tracing::{debug, error, info};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_LICENSE: &str = "license";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_CRYPTO_PROVIDER_STATE: &str = "crypto_provider_state";
const EVENT_DIAL9_RUNTIME_STATUS: &str = "dial9_runtime_status";
const EVENT_RUNTIME_LICENSE_STATUS: &str = "runtime_license_status";
const EVENT_TLS_OUTBOUND_INITIALIZED: &str = "tls_outbound_initialized";
const EVENT_TLS_OUTBOUND_INITIALIZATION_FAILED: &str = "tls_outbound_initialization_failed";
const TLS_STARTUP_GENERATION_CONSUMER: &str = "rustfs_server_startup";

pub async fn init_startup_runtime_foundation(config: &Config) -> Result<()> {
    log_dial9_runtime_status();
    log_runtime_license_status();

    debug!("{}", crate::server::LOGO);

    crate::profiling::init_from_env().await;
    rustfs_trusted_proxies::init();
    install_default_crypto_provider();
    init_outbound_tls_material(config).await
}

fn log_dial9_runtime_status() {
    if rustfs_obs::dial9::is_enabled() {
        info!(
            target: "rustfs::main",
            event = EVENT_DIAL9_RUNTIME_STATUS,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            enabled = true,
            "Dial9 Tokio runtime telemetry is enabled"
        );
    } else {
        debug!(
            target: "rustfs::main",
            event = EVENT_DIAL9_RUNTIME_STATUS,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            enabled = false,
            "Dial9 Tokio runtime telemetry is disabled"
        );
    }
}

fn log_runtime_license_status() {
    info!(
        target: "rustfs::main",
        event = EVENT_RUNTIME_LICENSE_STATUS,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_LICENSE,
        license_status = %license_status(),
        "Initialized runtime license state"
    );
}

fn install_default_crypto_provider() {
    if default_provider().install_default().is_err() {
        debug!(
            target: "rustfs::main",
            event = EVENT_CRYPTO_PROVIDER_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            provider = "aws_lc_rs",
            state = "already_installed",
            "Rustls crypto provider state checked"
        );
    }
}

async fn init_outbound_tls_material(config: &Config) -> Result<()> {
    if let Some(tls_path) = normalized_tls_path(config.tls_path.as_deref()) {
        match crate::server::tls_material::load_tls_material(tls_path).await {
            Ok(snapshot) => {
                use rustfs_tls_runtime::{publish_global_outbound_tls_state, record_tls_generation};

                let generation = next_tls_generation(rustfs_common::get_global_outbound_tls_generation());
                publish_global_outbound_tls_state(generation, &snapshot.outbound).await;
                record_tls_generation(TLS_STARTUP_GENERATION_CONSUMER, generation.0);
                info!(
                    target: "rustfs::main",
                    event = EVENT_TLS_OUTBOUND_INITIALIZED,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    tls_path,
                    generation = generation.0,
                    "Initialized TLS outbound material"
                );
            }
            Err(err) => {
                error!(
                    target: "rustfs::main",
                    event = EVENT_TLS_OUTBOUND_INITIALIZATION_FAILED,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    tls_path,
                    error = %err,
                    "Failed to initialize TLS outbound material"
                );
                return Err(Error::other(err.to_string()));
            }
        }
        if rustfs_obs::observability_metric_enabled() {
            rustfs_tls_runtime::init_tls_metrics();
        }
    }

    Ok(())
}

fn normalized_tls_path(path: Option<&str>) -> Option<&str> {
    path.map(str::trim).filter(|value| !value.is_empty())
}

fn next_tls_generation(current: u64) -> rustfs_tls_runtime::TlsGeneration {
    rustfs_tls_runtime::TlsGeneration(current.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_tls_path_ignores_empty_values() {
        assert_eq!(normalized_tls_path(None), None);
        assert_eq!(normalized_tls_path(Some("")), None);
        assert_eq!(normalized_tls_path(Some("   ")), None);
    }

    #[test]
    fn normalized_tls_path_trims_configured_path() {
        assert_eq!(normalized_tls_path(Some(" /tmp/rustfs-tls ")), Some("/tmp/rustfs-tls"));
    }

    #[test]
    fn next_tls_generation_saturates() {
        assert_eq!(next_tls_generation(0).0, 1);
        assert_eq!(next_tls_generation(u64::MAX).0, u64::MAX);
    }
}
