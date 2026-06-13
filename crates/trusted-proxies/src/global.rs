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

//! Global entry point for the Trusted Proxies module.
//!
//! This module provides a unified interface for initializing and using the
//! trusted proxy functionality within the RustFS server.

use crate::{AppConfig, ConfigLoader, LegacyTrustedProxyLayer, ProxyMetrics, default_proxy_metrics};
use rustfs_config::{DEFAULT_TRUSTED_PROXY_ENABLED, ENV_TRUSTED_PROXY_ENABLED};
use std::sync::Arc;
use std::sync::OnceLock;

/// Global instance of the application configuration.
static CONFIG: OnceLock<Arc<AppConfig>> = OnceLock::new();

/// Global instance of the metrics collector.
static METRICS: OnceLock<Option<ProxyMetrics>> = OnceLock::new();

/// Global instance of the trusted proxy layer.
static PROXY_LAYER: OnceLock<LegacyTrustedProxyLayer> = OnceLock::new();

/// Global flag indicating if the trusted proxy middleware is enabled.
static ENABLED: OnceLock<bool> = OnceLock::new();

/// Initializes the global trusted proxy system.
///
/// This function should be called once at the start of the application.
/// It loads the configuration, initializes metrics, and sets up the proxy layer.
pub fn init() {
    let enabled = is_enabled();
    ENABLED.get_or_init(|| enabled);

    if !enabled {
        tracing::info!(
            event = "trusted_proxies.lifecycle",
            component = "trusted_proxies",
            subsystem = "global",
            state = "disabled",
            enabled,
            "trusted proxies state changed"
        );
        return;
    }

    let config = CONFIG.get_or_init(|| Arc::new(ConfigLoader::from_env_or_default())).clone();

    METRICS.get_or_init(|| {
        if config.monitoring.metrics_enabled {
            Some(default_proxy_metrics(enabled))
        } else {
            None
        }
    });

    PROXY_LAYER.get_or_init(|| {
        LegacyTrustedProxyLayer::with_cache_config(
            config.proxy.clone(),
            config.cache.clone(),
            METRICS.get().and_then(|m| m.clone()),
            enabled,
        )
    });

    tracing::info!(
        event = "trusted_proxies.lifecycle",
        component = "trusted_proxies",
        subsystem = "global",
        state = "initialized",
        enabled,
        metrics_enabled = config.monitoring.metrics_enabled,
        trusted_proxy_count = config.proxy.proxies.len(),
        validation_mode = config.proxy.validation_mode.as_str(),
        "trusted proxies state changed"
    );
    ConfigLoader::print_summary(&config);
}

/// Returns a reference to the global trusted proxy layer.
///
/// This layer can be used to wrap Axum services or other Tower-compatible services.
///
/// # Panics
///
/// Panics if `init()` has not been called.
pub fn layer() -> &'static LegacyTrustedProxyLayer {
    PROXY_LAYER
        .get()
        .expect("Trusted proxy system not initialized. Call init() first.")
}

/// Returns a reference to the global configuration.
///
/// # Panics
///
/// Panics if `init()` has not been called.
pub fn config() -> &'static AppConfig {
    CONFIG
        .get()
        .expect("Trusted proxy system not initialized. Call init() first.")
}

/// Returns a reference to the global metrics collector, if enabled.
pub fn metrics() -> Option<&'static ProxyMetrics> {
    METRICS.get().and_then(|m| m.as_ref())
}

/// Returns true if the trusted proxy system is enabled.
pub fn is_enabled() -> bool {
    *ENABLED.get_or_init(|| rustfs_utils::get_env_bool(ENV_TRUSTED_PROXY_ENABLED, DEFAULT_TRUSTED_PROXY_ENABLED))
}
