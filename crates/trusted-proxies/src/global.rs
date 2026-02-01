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

use std::sync::Arc;
use std::sync::OnceLock;

use crate::config::{AppConfig, ConfigLoader};
use crate::middleware::TrustedProxyLayer;
use crate::proxy::default_proxy_metrics;
use crate::proxy::ProxyMetrics;

/// Global instance of the application configuration.
static CONFIG: OnceLock<Arc<AppConfig>> = OnceLock::new();

/// Global instance of the metrics collector.
static METRICS: OnceLock<Option<ProxyMetrics>> = OnceLock::new();

/// Global instance of the trusted proxy layer.
static PROXY_LAYER: OnceLock<TrustedProxyLayer> = OnceLock::new();

/// Initializes the global trusted proxy system.
///
/// This function should be called once at the start of the application.
/// It loads the configuration, initializes metrics, and sets up the proxy layer.
pub fn init() {
    // Load configuration from environment variables.
    let config = Arc::new(ConfigLoader::from_env_or_default());
    CONFIG.set(config.clone()).expect("Trusted proxy config already initialized");

    // Initialize metrics if enabled.
    let metrics = if config.monitoring.metrics_enabled {
        let m = default_proxy_metrics(true);
        Some(m)
    } else {
        None
    };
    METRICS.set(metrics.clone()).expect("Trusted proxy metrics already initialized");

    // Initialize the trusted proxy layer.
    let layer = TrustedProxyLayer::enabled(config.proxy.clone(), metrics);
    PROXY_LAYER.set(layer).expect("Trusted proxy layer already initialized");

    tracing::info!("Trusted Proxies module initialized");
    ConfigLoader::print_summary(&config);
}

/// Returns a reference to the global trusted proxy layer.
///
/// This layer can be used to wrap Axum services or other Tower-compatible services.
///
/// # Panics
///
/// Panics if `init()` has not been called.
pub fn layer() -> &'static TrustedProxyLayer {
    PROXY_LAYER.get().expect("Trusted proxy system not initialized. Call init() first.")
}

/// Returns a reference to the global configuration.
///
/// # Panics
///
/// Panics if `init()` has not been called.
pub fn config() -> &'static AppConfig {
    CONFIG.get().expect("Trusted proxy system not initialized. Call init() first.")
}

/// Returns a reference to the global metrics collector, if enabled.
pub fn metrics() -> Option<&'static ProxyMetrics> {
    METRICS.get().and_then(|m| m.as_ref())
}
