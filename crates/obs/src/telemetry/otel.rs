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

//! OpenTelemetry HTTP exporter initialisation.
//!
//! This module sets up full OTLP/HTTP pipelines for:
//! - **Traces** via [`opentelemetry_otlp::SpanExporter`]
//! - **Metrics** via [`opentelemetry_otlp::MetricExporter`]
//! - **Logs** via [`opentelemetry_otlp::LogExporter`]
//!
//! Each signal has a dedicated endpoint field in [`OtelConfig`].  When a
//! per-signal endpoint is absent, the function falls back to appending the
//! standard OTLP path suffix to the root `endpoint` field:
//!
//! | Signal  | Fallback path   |
//! |---------|-----------------|
//! | Traces  | `/v1/traces`    |
//! | Metrics | `/v1/metrics`   |
//! | Logs    | `/v1/logs`      |
//!
//! All exporters use **HTTP binary** (Protobuf) encoding with **gzip**
//! compression for efficiency over the wire.
//!
//! If log export is not configured, this module deliberately falls back to the
//! same rolling-file logging path used by the local backend so applications can
//! combine OTLP traces/metrics with on-disk logs.

use crate::cleaner::types::FileMatchMode;
use crate::config::OtelConfig;
use crate::global::set_observability_metric_enabled;
use crate::telemetry::filter::build_env_filter;
use crate::telemetry::guard::OtelGuard;
use crate::telemetry::local::spawn_cleanup_task;
use crate::telemetry::recorder::Recorder;
use crate::telemetry::resource::build_resource;
use crate::telemetry::rolling::{RollingAppender, Rotation};
// Import helper functions from local.rs (sibling module)
use crate::TelemetryError;
use metrics::counter;
use opentelemetry::{global, propagation::TextMapCompositePropagator, trace::TracerProvider};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Compression, Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use percent_encoding::percent_decode_str;
use rustfs_config::observability::{DEFAULT_OBS_LOG_MATCH_MODE, DEFAULT_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES};
use rustfs_config::{
    APP_NAME, DEFAULT_LOG_KEEP_FILES, DEFAULT_LOG_ROTATION_TIME, DEFAULT_OBS_LOG_STDOUT_ENABLED, DEFAULT_OBS_LOGS_EXPORT_ENABLED,
    DEFAULT_OBS_METRICS_EXPORT_ENABLED, DEFAULT_OBS_TRACES_EXPORT_ENABLED, METER_INTERVAL, SAMPLE_RATIO,
};
use std::collections::HashMap;
use std::{fs, io::IsTerminal, time::Duration};
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{
    Layer,
    fmt::{format::FmtSpan, time::LocalTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

/// Initialize the full OpenTelemetry HTTP pipeline (traces + metrics + logs).
///
/// This function is invoked when at least one OTLP endpoint has been
/// configured.  It creates exporters, wires them into SDK providers, installs
/// a global tracer/meter, and builds a `tracing_subscriber` registry that
/// bridges Rust's `tracing` macros to the OTLP pipelines.
///
/// # Arguments
/// * `config`        - Fully populated observability configuration.
/// * `logger_level`  - Effective log level string (e.g., `"info"`).
/// * `is_production` - Controls span verbosity and stdout layer defaults.
///
/// # Returns
/// An [`OtelGuard`] owning all created providers.  Dropping it triggers an
/// ordered shutdown and flushes all pending telemetry data.
///
/// # Errors
/// Returns [`TelemetryError`] if any exporter or provider fails to build.
///
/// # Note
/// This function is intentionally kept unchanged from the pre-refactor
/// implementation to preserve existing OTLP behavior.
pub(super) fn init_observability_http(
    config: &OtelConfig,
    logger_level: &str,
    is_production: bool,
) -> Result<OtelGuard, TelemetryError> {
    // ── Resource & sampling ──────────────────────────────────────────────────
    // Build the common resource once so all enabled signals report the same
    // service identity and deployment metadata.
    let res = build_resource(config);
    let service_name = config.service_name.as_deref().unwrap_or(APP_NAME).to_owned();
    let use_stdout = config.use_stdout.unwrap_or(!is_production);
    let sample_ratio = config.sample_ratio.unwrap_or(SAMPLE_RATIO);
    let sampler = build_tracer_sampler(sample_ratio);

    // ── Endpoint resolution ───────────────────────────────────────────────────
    // Each signal may have a dedicated endpoint; if absent, fall back to the
    // root endpoint with the standard OTLP path suffix appended.
    let root_ep = config.endpoint.clone();

    let trace_ep: String = config
        .trace_endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            if !root_ep.is_empty() {
                format!("{root_ep}/v1/traces")
            } else {
                String::new()
            }
        });

    let metric_ep: String = config
        .metric_endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            if !root_ep.is_empty() {
                format!("{root_ep}/v1/metrics")
            } else {
                String::new()
            }
        });

    // If `log_endpoint` is not explicitly set, fall back to `root_ep/v1/logs`
    // only when a root endpoint exists. An empty result intentionally triggers
    // the file-logging path below instead of silently disabling application logs.
    let log_ep: String = config
        .log_endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            if !root_ep.is_empty() {
                format!("{root_ep}/v1/logs")
            } else {
                String::new()
            }
        });

    // ── Tracer provider (HTTP) ────────────────────────────────────────────────
    let tracer_provider = build_tracer_provider(&trace_ep, config, res.clone(), sampler, use_stdout)?;

    // ── Meter provider (HTTP) ─────────────────────────────────────────────────
    let meter_provider = build_meter_provider(&metric_ep, config, res.clone(), &service_name, use_stdout)?;

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let profiling_agent = init_profiler(config);

    // ── Logger Logic ──────────────────────────────────────────────────────────
    // Logging is the only signal that may intentionally route to either OTLP
    // or local files depending on configuration completeness.
    let mut logger_provider: Option<SdkLoggerProvider> = None;
    let mut otel_bridge = None;
    let mut file_layer_opt = None; // File layer (File mode)
    let mut stdout_layer_opt = None; // Stdout layer (File mode)
    let mut cleanup_handle = None;
    let mut tracing_guard = None; // Guard for file writer
    let mut stdout_guard = None; // Guard for stdout writer (File mode)
    let mut force_stdout_logging = false;

    // ── Case 1: OTLP Logging
    if !log_ep.is_empty() {
        // Init OTLP logger logic.
        // We initialize the OTLP collector and honor the configured stdout setting
        // (e.g. via RUSTFS_OBS_USE_STDOUT / config.use_stdout) when building the provider.
        logger_provider = build_logger_provider(&log_ep, config, res, false)?;

        // Build bridge to capture `tracing` events.
        otel_bridge = logger_provider
            .as_ref()
            .map(|p| OpenTelemetryTracingBridge::new(p).with_filter(build_env_filter(logger_level, None)));

        // No separate formatting layer is added here; when OTLP logging is
        // active, the OpenTelemetry bridge is the authoritative sink for
        // `tracing` events unless local file logging is needed as a fallback.
    }
    let span_events = if is_production { FmtSpan::CLOSE } else { FmtSpan::FULL };
    // ── Case 2: File Logging
    // If a log directory is configured and OTLP log export is unavailable, use
    // the same rolling-file behavior as the local-only telemetry backend.
    if let Some(log_directory) = config.log_directory.as_deref().filter(|s| !s.is_empty())
        && logger_provider.is_none()
    {
        let log_filename = config.log_filename.as_deref().unwrap_or(&service_name);
        let keep_files = config.log_keep_files.unwrap_or(DEFAULT_LOG_KEEP_FILES);
        let file_logging_result = (|| -> Result<_, TelemetryError> {
            fs::create_dir_all(log_directory).map_err(|e| TelemetryError::Io(e.to_string()))?;

            #[cfg(any(target_os = "linux", target_os = "macos"))]
            crate::telemetry::local::ensure_dir_permissions(log_directory)?;

            let rotation_str = config
                .log_rotation_time
                .as_deref()
                .unwrap_or(DEFAULT_LOG_ROTATION_TIME)
                .to_lowercase();
            let match_mode =
                FileMatchMode::from_config_str(config.log_match_mode.as_deref().unwrap_or(DEFAULT_OBS_LOG_MATCH_MODE));
            let rotation = match rotation_str.as_str() {
                "minutely" => Rotation::Minutely,
                "hourly" => Rotation::Hourly,
                "daily" => Rotation::Daily,
                _ => Rotation::Daily,
            };
            let max_single_file_size = config
                .log_max_single_file_size_bytes
                .unwrap_or(DEFAULT_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES);

            let file_appender =
                RollingAppender::new(log_directory, log_filename.to_string(), rotation, max_single_file_size, match_mode)?;

            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            let file_layer = tracing_subscriber::fmt::layer()
                .with_timer(LocalTime::rfc_3339())
                .with_target(true)
                .with_ansi(false)
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_writer(non_blocking)
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .with_span_events(span_events.clone())
                .with_filter(build_env_filter(logger_level, None));
            let cleanup_handle = spawn_cleanup_task(config, log_directory, log_filename, keep_files);
            Ok((file_layer, guard, cleanup_handle, rotation_str))
        })();

        match file_logging_result {
            Ok((file_layer, guard, new_cleanup_handle, rotation_str)) => {
                tracing_guard = Some(guard);
                file_layer_opt = Some(file_layer);
                cleanup_handle = Some(new_cleanup_handle);

                info!(
                    "Init file logging at '{}', rotation: {}, keep {} files",
                    log_directory, rotation_str, keep_files
                );
            }
            Err(error) if crate::telemetry::local::should_fallback_to_stdout(&error) => {
                crate::telemetry::local::emit_file_logging_fallback_warning(log_directory, &error);
                force_stdout_logging = true;
            }
            Err(error) => return Err(error),
        }
    }

    // ── Tracing subscriber registry ───────────────────────────────────────────
    let tracer_layer = tracer_provider
        .as_ref()
        .map(|p| OpenTelemetryLayer::new(p.tracer(service_name.to_string())));
    let metrics_layer = meter_provider.as_ref().map(|p| MetricsLayer::new(p.clone()));

    // Optional stdout mirror (matching init_file_logging_internal logic)
    // This is separate from OTLP stdout logic. If file logging is enabled, we honor its stdout rules.
    if force_stdout_logging || config.log_stdout_enabled.unwrap_or(DEFAULT_OBS_LOG_STDOUT_ENABLED) || !is_production {
        let (stdout_nb, stdout_g) = tracing_appender::non_blocking(std::io::stdout());
        stdout_guard = Some(stdout_g);
        stdout_layer_opt = Some(
            tracing_subscriber::fmt::layer()
                .with_timer(LocalTime::rfc_3339())
                .with_target(true)
                .with_ansi(std::io::stdout().is_terminal())
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_writer(stdout_nb)
                .with_span_events(span_events)
                .with_filter(build_env_filter(logger_level, None)),
        );
    }
    let filter = build_env_filter(logger_level, None);
    tracing_subscriber::registry()
        .with(filter)
        .with(ErrorLayer::default())
        .with(file_layer_opt) // File
        .with(stdout_layer_opt) // Stdout (only if file logging enabled it)
        .with(tracer_layer)
        .with(otel_bridge)
        .with(metrics_layer)
        .init();

    counter!("rustfs.start.total").increment(1);
    info!(
        "Init observability (HTTP): trace='{}', metric='{}', log='{}'",
        trace_ep, metric_ep, log_ep
    );

    Ok(OtelGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        profiling_agent,
        tracing_guard,
        stdout_guard,
        cleanup_handle,
    })
}

// ─── Private builder helpers ──────────────────────────────────────────────────

/// Build an optional [`SdkTracerProvider`] for the given trace endpoint.
///
/// Returns `None` when the endpoint is empty or trace export is disabled.
/// When enabled, the provider is also registered as the global tracer provider
/// and installs a composite propagator supporting both W3C TraceContext
/// (traceparent header) and W3C Baggage (baggage header) propagation.
fn build_tracer_provider(
    trace_ep: &str,
    config: &OtelConfig,
    res: opentelemetry_sdk::Resource,
    sampler: Sampler,
    use_stdout: bool,
) -> Result<Option<SdkTracerProvider>, TelemetryError> {
    if trace_ep.is_empty() || !config.traces_export_enabled.unwrap_or(DEFAULT_OBS_TRACES_EXPORT_ENABLED) {
        return Ok(None);
    }

    let mut exporter_builder = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(trace_ep)
        .with_protocol(Protocol::HttpBinary)
        .with_compression(Compression::Gzip);
    let trace_headers = resolve_signal_headers(config.endpoint_headers.as_deref(), config.trace_headers.as_deref());
    if !trace_headers.is_empty() {
        exporter_builder = exporter_builder.with_headers(trace_headers);
    }
    if let Some(timeout) = resolve_signal_timeout(config.endpoint_timeout_millis, config.trace_timeout_millis) {
        exporter_builder = exporter_builder.with_timeout(timeout);
    }
    let exporter = exporter_builder
        .build()
        .map_err(|e| TelemetryError::BuildSpanExporter(e.to_string()))?;

    let mut builder = SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(res)
        .with_batch_exporter(exporter);

    if use_stdout {
        builder = builder.with_batch_exporter(opentelemetry_stdout::SpanExporter::default());
    }

    let provider = builder.build();
    global::set_tracer_provider(provider.clone());

    // Configure composite propagator to support multiple trace context formats:
    // - W3C TraceContext (traceparent header) - standard format for distributed tracing
    // - W3C Baggage (baggage header) - for propagating user-defined key-value pairs
    let propagator =
        TextMapCompositePropagator::new(vec![Box::new(TraceContextPropagator::new()), Box::new(BaggagePropagator::new())]);
    global::set_text_map_propagator(propagator);

    Ok(Some(provider))
}

/// Convert a configured sample ratio into the SDK sampler strategy.
///
/// Invalid or non-finite ratios fall back to `AlwaysOn` so telemetry does not
/// disappear due to configuration mistakes.
fn build_tracer_sampler(sample_ratio: f64) -> Sampler {
    if sample_ratio.is_finite() && (0.0..=1.0).contains(&sample_ratio) {
        Sampler::TraceIdRatioBased(sample_ratio)
    } else {
        Sampler::AlwaysOn
    }
}

/// Build an optional [`SdkMeterProvider`] for the given metrics endpoint.
///
/// Returns `None` when the endpoint is empty or metric export is disabled.
/// The provider is paired with the crate's metrics recorder so `metrics` crate
/// instruments flow into OpenTelemetry readers.
fn build_meter_provider(
    metric_ep: &str,
    config: &OtelConfig,
    res: opentelemetry_sdk::Resource,
    service_name: &str,
    use_stdout: bool,
) -> Result<Option<SdkMeterProvider>, TelemetryError> {
    if metric_ep.is_empty() || !config.metrics_export_enabled.unwrap_or(DEFAULT_OBS_METRICS_EXPORT_ENABLED) {
        return Ok(None);
    }

    let mut exporter_builder = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(metric_ep)
        .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
        .with_protocol(Protocol::HttpBinary)
        .with_compression(Compression::Gzip);
    let metric_headers = resolve_signal_headers(config.endpoint_headers.as_deref(), config.metric_headers.as_deref());
    if !metric_headers.is_empty() {
        exporter_builder = exporter_builder.with_headers(metric_headers);
    }
    if let Some(timeout) = resolve_signal_timeout(config.endpoint_timeout_millis, config.metric_timeout_millis) {
        exporter_builder = exporter_builder.with_timeout(timeout);
    }
    let exporter = exporter_builder
        .build()
        .map_err(|e| TelemetryError::BuildMetricExporter(e.to_string()))?;

    let meter_interval = config.meter_interval.unwrap_or(METER_INTERVAL);

    let (provider, recorder) = Recorder::builder(service_name.to_string())
        .with_meter_provider(|b: opentelemetry_sdk::metrics::MeterProviderBuilder| {
            let b = b.with_resource(res).with_reader(
                PeriodicReader::builder(exporter)
                    .with_interval(Duration::from_secs(meter_interval))
                    .build(),
            );
            if use_stdout {
                b.with_reader(create_periodic_reader(meter_interval))
            } else {
                b
            }
        })
        .build();

    global::set_meter_provider(provider.clone());
    metrics::set_global_recorder(recorder).map_err(|e| TelemetryError::InstallMetricsRecorder(e.to_string()))?;
    set_observability_metric_enabled(true);
    Ok(Some(provider))
}

/// Build an optional [`SdkLoggerProvider`] for the given log endpoint.
///
/// Returns `None` when the endpoint is empty or log export is disabled.
/// The caller wraps the resulting provider in an OpenTelemetry tracing bridge.
fn build_logger_provider(
    log_ep: &str,
    config: &OtelConfig,
    res: opentelemetry_sdk::Resource,
    use_stdout: bool,
) -> Result<Option<SdkLoggerProvider>, TelemetryError> {
    if log_ep.is_empty() || !config.logs_export_enabled.unwrap_or(DEFAULT_OBS_LOGS_EXPORT_ENABLED) {
        return Ok(None);
    }

    let mut exporter_builder = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(log_ep)
        .with_protocol(Protocol::HttpBinary)
        .with_compression(Compression::Gzip);
    let log_headers = resolve_signal_headers(config.endpoint_headers.as_deref(), config.log_headers.as_deref());
    if !log_headers.is_empty() {
        exporter_builder = exporter_builder.with_headers(log_headers);
    }
    if let Some(timeout) = resolve_signal_timeout(config.endpoint_timeout_millis, config.log_timeout_millis) {
        exporter_builder = exporter_builder.with_timeout(timeout);
    }
    let exporter = exporter_builder
        .build()
        .map_err(|e| TelemetryError::BuildLogExporter(e.to_string()))?;

    let mut builder = SdkLoggerProvider::builder().with_resource(res);
    builder = builder.with_batch_exporter(exporter);
    if use_stdout {
        builder = builder.with_batch_exporter(opentelemetry_stdout::LogExporter::default());
    }
    Ok(Some(builder.build()))
}

/// Start the Pyroscope continuous profiling agent when profiling is enabled.
///
/// Returns `None` when profiling export is disabled, when no usable
/// profiling endpoint is configured, or when building or starting the agent
/// fails.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn init_profiler(config: &OtelConfig) -> Option<pyroscope::PyroscopeAgent<pyroscope::pyroscope::PyroscopeAgentRunning>> {
    use pyroscope::backend::{BackendConfig, PprofConfig, pprof_backend};
    use pyroscope::pyroscope::PyroscopeAgentBuilder;
    use rustfs_config::VERSION;

    if !config
        .profiling_export_enabled
        .unwrap_or(rustfs_config::DEFAULT_OBS_PROFILING_EXPORT_ENABLED)
    {
        return None;
    }

    let endpoint = config.profiling_endpoint.as_ref()?.as_str();
    if endpoint.is_empty() {
        return None;
    }

    // Configure Pyroscope Agent
    let backend = pprof_backend(PprofConfig::default(), BackendConfig::default());
    let service_name = config.service_name.as_deref().unwrap_or(APP_NAME);
    let version = config.service_version.as_deref().unwrap_or(VERSION);
    let sample_rate = 100; // 100 Hz

    let agent = PyroscopeAgentBuilder::new(endpoint, service_name, sample_rate, "pyroscope-rs", "1.0.1", backend)
        .tags(vec![("version", version)]) // TODO: add git commit tag
        .build()
        .ok()?;

    match agent.start() {
        Ok(agent) => Some(agent),
        Err(err) => {
            eprintln!("Pyroscope agent start error: {err:?}");
            None
        }
    }
}

/// Create a stdout periodic metrics reader for the given interval.
///
/// This helper is primarily used for local development and diagnostics when
/// operators want to see exported metric points without an OTLP collector.
fn create_periodic_reader(interval: u64) -> PeriodicReader<opentelemetry_stdout::MetricExporter> {
    PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default())
        .with_interval(Duration::from_secs(interval))
        .build()
}

fn resolve_signal_headers(common_headers: Option<&str>, signal_headers: Option<&str>) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    if let Some(raw_headers) = common_headers {
        headers.extend(parse_otlp_headers(raw_headers));
    }
    if let Some(raw_headers) = signal_headers {
        headers.extend(parse_otlp_headers(raw_headers));
    }
    headers
}

fn parse_otlp_headers(raw_headers: &str) -> HashMap<String, String> {
    raw_headers
        .split(',')
        .filter_map(|entry| {
            let (key, value) = entry.split_once('=')?;
            let key = key.trim();
            if key.is_empty() {
                return None;
            }
            let value = percent_decode_str(value.trim()).decode_utf8().ok()?;
            Some((key.to_string(), value.into_owned()))
        })
        .collect()
}

fn resolve_signal_timeout(common_timeout_millis: Option<u64>, signal_timeout_millis: Option<u64>) -> Option<Duration> {
    signal_timeout_millis
        .or(common_timeout_millis)
        .filter(|timeout_millis| *timeout_millis > 0)
        .map(Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// Valid ratios should produce trace-id-ratio sampling.
    fn test_build_tracer_sampler_uses_trace_ratio_for_valid_values() {
        let sampler = build_tracer_sampler(0.0);
        assert!(format!("{sampler:?}").contains("TraceIdRatioBased"));

        let sampler = build_tracer_sampler(1.0);
        assert!(format!("{sampler:?}").contains("TraceIdRatioBased"));

        let sampler = build_tracer_sampler(0.5);
        assert!(format!("{sampler:?}").contains("TraceIdRatioBased"));
    }

    #[test]
    /// Invalid ratios should degrade to the safest non-dropping sampler.
    fn test_build_tracer_sampler_rejects_invalid_ratio_with_always_on() {
        let sampler = build_tracer_sampler(-0.1);
        assert!(format!("{sampler:?}").contains("AlwaysOn"));

        let sampler = build_tracer_sampler(1.2);
        assert!(format!("{sampler:?}").contains("AlwaysOn"));
    }

    #[test]
    fn test_parse_otlp_headers_ignores_invalid_entries() {
        let headers = parse_otlp_headers("Authorization=Bearer%20abc,empty=,missing, =ignored,key=value,bad=%FF");
        assert_eq!(headers.len(), 3);
        assert_eq!(headers.get("Authorization"), Some(&"Bearer abc".to_string()));
        assert_eq!(headers.get("empty"), Some(&"".to_string()));
        assert_eq!(headers.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_resolve_signal_headers_signal_overrides_common() {
        let headers = resolve_signal_headers(Some("k1=v1,k2=common"), Some("k2=signal,k3=v3"));
        assert_eq!(headers.get("k1"), Some(&"v1".to_string()));
        assert_eq!(headers.get("k2"), Some(&"signal".to_string()));
        assert_eq!(headers.get("k3"), Some(&"v3".to_string()));
    }

    #[test]
    fn test_resolve_signal_timeout_prefers_signal_value() {
        assert_eq!(resolve_signal_timeout(Some(2_000), Some(5_000)), Some(Duration::from_millis(5_000)));
    }

    #[test]
    fn test_resolve_signal_timeout_falls_back_to_common() {
        assert_eq!(resolve_signal_timeout(Some(3_000), None), Some(Duration::from_millis(3_000)));
        assert_eq!(resolve_signal_timeout(None, None), None);
        assert_eq!(resolve_signal_timeout(Some(0), None), None);
        assert_eq!(resolve_signal_timeout(None, Some(0)), None);
    }
}
