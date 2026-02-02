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

//! Metrics and monitoring for proxy validation performance and results.

use crate::{ProxyError, ValidationMode};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::time::Duration;
use tracing::info;

/// Collector for proxy validation metrics.
#[derive(Debug, Clone)]
pub struct ProxyMetrics {
    /// Whether metrics collection is enabled.
    enabled: bool,
    /// Application name used as a label for metrics.
    app_name: String,
}

impl ProxyMetrics {
    /// Creates a new `ProxyMetrics` collector.
    pub fn new(app_name: &str, enabled: bool) -> Self {
        let metrics = Self {
            enabled,
            app_name: app_name.to_string(),
        };

        // Register metric descriptions for Prometheus.
        metrics.register_descriptions();

        metrics
    }

    /// Registers descriptions for all metrics.
    fn register_descriptions(&self) {
        if !self.enabled {
            return;
        }

        describe_counter!(
            "rustfs_trusted_proxy_validation_attempts_total",
            "Total number of proxy validation attempts"
        );
        describe_counter!(
            "rustfs_trusted_proxy_validation_success_total",
            "Total number of successful proxy validations"
        );
        describe_counter!(
            "rustfs_trusted_proxy_validation_failure_total",
            "Total number of failed proxy validations"
        );
        describe_counter!(
            "rustfs_trusted_proxy_validation_failure_by_type_total",
            "Total number of failed proxy validations categorized by error type"
        );
        describe_gauge!("rustfs_trusted_proxy_chain_length", "Current length of proxy chains being validated");
        describe_histogram!(
            "rustfs_trusted_proxy_validation_duration_seconds",
            "Time taken to validate a proxy chain in seconds"
        );
        describe_gauge!(
            "rustfs_trusted_proxy_cache_size",
            "Current number of entries in the proxy validation cache"
        );
        describe_counter!("rustfs_trusted_proxy_cache_hits_total", "Total number of cache hits for proxy validation");
        describe_counter!(
            "rustfs_trusted_proxy_cache_misses_total",
            "Total number of cache misses for proxy validation"
        );
    }

    /// Increments the total number of validation attempts.
    pub fn increment_validation_attempts(&self) {
        if !self.enabled {
            return;
        }

        counter!(
            "rustfs_trusted_proxy_validation_attempts_total",
            "app" => self.app_name.clone()
        )
        .increment(1);
    }

    /// Records a successful validation.
    pub fn record_validation_success(&self, from_trusted_proxy: bool, proxy_hops: usize, duration: Duration) {
        if !self.enabled {
            return;
        }

        counter!(
            "rustfs_trusted_proxy_validation_success_total",
            "app" => self.app_name.clone(),
            "trusted" => from_trusted_proxy.to_string()
        )
        .increment(1);

        gauge!(
            "rustfs_trusted_proxy_chain_length",
            "app" => self.app_name.clone()
        )
        .set(proxy_hops as f64);

        histogram!(
            "rustfs_trusted_proxy_validation_duration_seconds",
            "app" => self.app_name.clone()
        )
        .record(duration.as_secs_f64());
    }

    /// Records a failed validation with the specific error type.
    pub fn record_validation_failure(&self, error: &ProxyError, duration: Duration) {
        if !self.enabled {
            return;
        }

        let error_type = match error {
            ProxyError::InvalidXForwardedFor(_) => "invalid_x_forwarded_for",
            ProxyError::InvalidForwardedHeader(_) => "invalid_forwarded_header",
            ProxyError::ChainValidationFailed(_) => "chain_validation_failed",
            ProxyError::ChainTooLong(_, _) => "chain_too_long",
            ProxyError::UntrustedProxy(_) => "untrusted_proxy",
            ProxyError::ChainNotContinuous => "chain_not_continuous",
            ProxyError::IpParseError(_) => "ip_parse_error",
            ProxyError::HeaderParseError(_) => "header_parse_error",
            ProxyError::Timeout => "timeout",
            ProxyError::Internal(_) => "internal",
        };

        counter!(
            "rustfs_trusted_proxy_validation_failure_total",
            "app" => self.app_name.clone(),
            "error_type" => error_type
        )
        .increment(1);

        counter!(
            "rustfs_trusted_proxy_validation_failure_by_type_total",
            "app" => self.app_name.clone(),
            "error_type" => error_type
        )
        .increment(1);

        histogram!(
            "rustfs_trusted_proxy_validation_duration_seconds",
            "app" => self.app_name.clone(),
            "error_type" => error_type
        )
        .record(duration.as_secs_f64());
    }

    /// Records the validation mode currently in use.
    pub fn record_validation_mode(&self, mode: ValidationMode) {
        if !self.enabled {
            return;
        }

        gauge!(
            "rustfs_trusted_proxy_validation_mode",
            "app" => self.app_name.clone(),
            "mode" => mode.as_str()
        )
        .set(match mode {
            ValidationMode::Lenient => 0.0,
            ValidationMode::Strict => 1.0,
            ValidationMode::HopByHop => 2.0,
        });
    }

    /// Records cache performance metrics.
    pub fn record_cache_metrics(&self, hits: u64, misses: u64, size: usize) {
        if !self.enabled {
            return;
        }

        counter!("rustfs_trusted_proxy_cache_hits_total", "app" => self.app_name.clone()).increment(hits);
        counter!("rustfs_trusted_proxy_cache_misses_total", "app" => self.app_name.clone()).increment(misses);
        gauge!("rustfs_trusted_proxy_cache_size", "app" => self.app_name.clone()).set(size as f64);
    }

    /// Prints a summary of enabled metrics to the log.
    pub fn print_summary(&self) {
        if !self.enabled {
            info!("Metrics collection is disabled");
            return;
        }

        info!("Proxy metrics enabled for application: {}", self.app_name);
        info!("Available metrics:");
        info!("  - rustfs_trusted_proxy_validation_attempts_total");
        info!("  - rustfs_trusted_proxy_validation_success_total");
        info!("  - rustfs_trusted_proxy_validation_failure_total");
        info!("  - rustfs_trusted_proxy_validation_failure_by_type_total");
        info!("  - rustfs_trusted_proxy_chain_length");
        info!("  - rustfs_trusted_proxy_validation_duration_seconds");
        info!("  - rustfs_trusted_proxy_cache_size");
        info!("  - rustfs_trusted_proxy_cache_hits_total");
        info!("  - rustfs_trusted_proxy_cache_misses_total");
    }
}

/// Default application name for metrics.
const DEFAULT_APP_NAME: &str = "trusted-proxy";

/// Creates a default `ProxyMetrics` collector.
pub fn default_proxy_metrics(enabled: bool) -> ProxyMetrics {
    ProxyMetrics::new(DEFAULT_APP_NAME, enabled)
}
