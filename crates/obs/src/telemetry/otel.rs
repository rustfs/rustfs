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
use crate::telemetry::filter::{build_env_filter, pyroscope_log_filter};
use crate::telemetry::guard::{OtelGuard, ProfilingAgent};
use crate::telemetry::local::{build_json_log_layer, spawn_cleanup_task};
use crate::telemetry::recorder::{Recorder, install_process_global_recorder};
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
    metrics::{Aggregation, Instrument, PeriodicReader, SdkMeterProvider, Stream},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use percent_encoding::percent_decode_str;
use rustfs_config::observability::{DEFAULT_OBS_LOG_MATCH_MODE, DEFAULT_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES};
use rustfs_config::{
    APP_NAME, DEFAULT_LOG_KEEP_FILES, DEFAULT_LOG_ROTATION_TIME, DEFAULT_OBS_LOGS_EXPORT_ENABLED,
    DEFAULT_OBS_METRICS_EXPORT_ENABLED, DEFAULT_OBS_TRACES_EXPORT_ENABLED, METER_INTERVAL, SAMPLE_RATIO,
};
use std::collections::HashMap;
use std::{fs, io::IsTerminal, path::Path, time::Duration};
use tracing::{info, warn};
use tracing_error::ErrorLayer;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const GET_OBJECT_DURATION_HISTOGRAM_METRICS: &[&str] = &[
    "rustfs_io_get_object_request_duration_seconds",
    "rustfs_io_get_object_total_duration_seconds",
    "rustfs_io_get_object_total_duration_seconds_with_path",
    "rustfs_io_get_object_stage_duration_seconds",
    "rustfs_io_get_object_stage_duration_seconds_by_size",
];

const GET_OBJECT_DURATION_HISTOGRAM_BUCKETS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.00075, 0.001, 0.0015, 0.002, 0.003, 0.004, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.03, 0.05, 0.075,
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

const DEFAULT_OTLP_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const OTEL_EXPORTER_OTLP_TIMEOUT: &str = "OTEL_EXPORTER_OTLP_TIMEOUT";
const OTEL_EXPORTER_OTLP_TRACES_TIMEOUT: &str = "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT";
const OTEL_EXPORTER_OTLP_METRICS_TIMEOUT: &str = "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT";
const OTEL_EXPORTER_OTLP_LOGS_TIMEOUT: &str = "OTEL_EXPORTER_OTLP_LOGS_TIMEOUT";

#[cfg(all(
    feature = "pyroscope",
    any(target_os = "macos", all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))
))]
const REDACTED_PROFILING_ENDPOINT: &str = "[redacted]";

#[cfg(all(
    feature = "pyroscope",
    any(target_os = "macos", all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))
))]
fn log_profiler_failure(result: &'static str, error_kind: &'static str) {
    warn!(
        backend = "pyroscope",
        endpoint = REDACTED_PROFILING_ENDPOINT,
        result,
        error_kind,
        "Profiling export agent initialization failed"
    );
}

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
    let otlp_tls_ca_bundle = OtlpTlsCaBundle::load(config)?;

    // ── Resource & sampling ──────────────────────────────────────────────────
    // Build the common resource once so all enabled signals report the same
    // service identity and deployment metadata.
    let res = build_resource(config);
    let service_name = config.service_name.as_deref().unwrap_or(APP_NAME).to_owned();
    let use_stdout = resolve_otlp_use_stdout(config.use_stdout, is_production);
    let sample_ratio = config.sample_ratio.unwrap_or(SAMPLE_RATIO);
    let sampler = build_tracer_sampler(sample_ratio);

    // ── Endpoint resolution ───────────────────────────────────────────────────
    // Each signal may have a dedicated endpoint; if absent, fall back to the
    // root endpoint with the standard OTLP path suffix appended.
    let root_ep = normalize_otlp_root_endpoint(&config.endpoint);

    let trace_ep = resolve_signal_endpoint(config.trace_endpoint.as_deref(), root_ep, "/v1/traces");
    let metric_ep = resolve_signal_endpoint(config.metric_endpoint.as_deref(), root_ep, "/v1/metrics");

    // If `log_endpoint` is not explicitly set, fall back to `root_ep/v1/logs`
    // only when a root endpoint exists. An empty result intentionally triggers
    // the file-logging path below instead of silently disabling application logs.
    let log_ep = resolve_signal_endpoint(config.log_endpoint.as_deref(), root_ep, "/v1/logs");

    // ── Tracer provider (HTTP) ────────────────────────────────────────────────
    let tracer_provider =
        build_tracer_provider(&trace_ep, config, otlp_tls_ca_bundle.as_ref(), res.clone(), sampler, use_stdout)?;

    // ── Meter provider (HTTP) ─────────────────────────────────────────────────
    let meter_provider =
        build_meter_provider(&metric_ep, config, otlp_tls_ca_bundle.as_ref(), res.clone(), &service_name, use_stdout)?;

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
        logger_provider = build_logger_provider(&log_ep, config, otlp_tls_ca_bundle.as_ref(), res, use_stdout)?;

        // Build bridge to capture `tracing` events.
        otel_bridge = logger_provider.as_ref().map(OpenTelemetryTracingBridge::new);

        // No separate formatting layer is added here; when OTLP logging is
        // active, the OpenTelemetry bridge is the authoritative sink for
        // `tracing` events unless local file logging is needed as a fallback.
    }
    let span_events = crate::telemetry::local::resolve_span_events();
    let span_list = crate::telemetry::local::resolve_span_list(logger_level);
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

            #[cfg(unix)]
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
            crate::telemetry::local::validate_stdout_sink(&file_appender)?;

            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            let file_layer = build_json_log_layer(non_blocking, false, span_events.clone(), span_list);
            let cleanup_handle = spawn_cleanup_task(config, log_directory, log_filename, keep_files);
            Ok((file_layer, guard, cleanup_handle, rotation_str))
        })();

        match file_logging_result {
            Ok((file_layer, guard, new_cleanup_handle, rotation_str)) => {
                tracing_guard = Some(guard);
                file_layer_opt = Some(file_layer);
                cleanup_handle = Some(new_cleanup_handle);

                info!(
                    backend = "local",
                    sink = "file",
                    output_format = "json",
                    log_directory,
                    rotation = %rotation_str,
                    keep_files,
                    stdout_mirror_enabled = crate::telemetry::local::resolve_file_stdout_mirror(
                        config.log_stdout_enabled,
                        is_production,
                    ),
                    logger_level,
                    is_production,
                    "Initialized local logging fallback for observability"
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
    if force_stdout_logging || crate::telemetry::local::resolve_file_stdout_mirror(config.log_stdout_enabled, is_production) {
        let (stdout_nb, stdout_g) = tracing_appender::non_blocking(std::io::stdout());
        stdout_guard = Some(stdout_g);
        stdout_layer_opt = Some(build_json_log_layer(stdout_nb, std::io::stdout().is_terminal(), span_events, span_list));
    }
    let local_file_fallback_enabled = file_layer_opt.is_some();
    let stdout_mirror_enabled = stdout_guard.is_some();
    let filter = build_env_filter(logger_level, None);
    tracing_subscriber::registry()
        .with(filter)
        .with(pyroscope_log_filter())
        .with(ErrorLayer::default())
        .with(file_layer_opt)
        .with(stdout_layer_opt)
        .with(tracer_layer)
        .with(otel_bridge)
        .with(metrics_layer)
        .try_init()
        .map_err(|err| TelemetryError::SubscriberInit(err.to_string()))?;

    counter!("rustfs_start_total").increment(1);
    info!(
        backend = "otlp_http",
        trace_endpoint = %trace_ep,
        metric_endpoint = %metric_ep,
        log_endpoint = %log_ep,
        local_file_fallback_enabled,
        stdout_mirror_enabled,
        output_format = "json",
        logger_level,
        is_production,
        "Initialized observability"
    );

    Ok(OtelGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
        profiling_agent: None,
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
    otlp_tls_ca_bundle: Option<&OtlpTlsCaBundle>,
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
    let timeout = resolve_signal_timeout(config.endpoint_timeout_millis, config.trace_timeout_millis);
    if let Some(timeout) = timeout {
        exporter_builder = exporter_builder.with_timeout(timeout);
    }
    if let Some(http_client) = build_otlp_http_client(otlp_tls_ca_bundle, timeout, OTEL_EXPORTER_OTLP_TRACES_TIMEOUT)? {
        exporter_builder = exporter_builder.with_http_client(http_client);
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
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(sample_ratio)))
    } else {
        Sampler::ParentBased(Box::new(Sampler::AlwaysOn))
    }
}

fn resolve_otlp_use_stdout(config_use_stdout: Option<bool>, is_production: bool) -> bool {
    config_use_stdout.unwrap_or(!is_production)
}

fn normalize_otlp_root_endpoint(root_ep: &str) -> &str {
    root_ep.trim_end_matches('/')
}

fn resolve_signal_endpoint(signal_endpoint: Option<&str>, root_ep: &str, suffix: &str) -> String {
    signal_endpoint
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if root_ep.is_empty() {
                String::new()
            } else {
                format!("{root_ep}{suffix}")
            }
        })
}

/// Build an optional [`SdkMeterProvider`] for the given metrics endpoint.
///
/// Returns `None` when the endpoint is empty or metric export is disabled.
/// The provider is paired with the crate's metrics recorder so `metrics` crate
/// instruments flow into OpenTelemetry readers.
fn build_meter_provider(
    metric_ep: &str,
    config: &OtelConfig,
    otlp_tls_ca_bundle: Option<&OtlpTlsCaBundle>,
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
    let timeout = resolve_signal_timeout(config.endpoint_timeout_millis, config.metric_timeout_millis);
    if let Some(timeout) = timeout {
        exporter_builder = exporter_builder.with_timeout(timeout);
    }
    if let Some(http_client) = build_otlp_http_client(otlp_tls_ca_bundle, timeout, OTEL_EXPORTER_OTLP_METRICS_TIMEOUT)? {
        exporter_builder = exporter_builder.with_http_client(http_client);
    }
    let exporter = exporter_builder
        .build()
        .map_err(|e| TelemetryError::BuildMetricExporter(e.to_string()))?;

    let meter_interval = resolve_meter_interval(config);

    let (provider, recorder) = Recorder::builder(service_name.to_string())
        .with_meter_provider(|b: opentelemetry_sdk::metrics::MeterProviderBuilder| {
            let b = b
                .with_resource(res)
                .with_reader(
                    PeriodicReader::builder(exporter)
                        .with_interval(Duration::from_secs(meter_interval))
                        .build(),
                )
                .with_view(get_object_duration_histogram_view);
            if use_stdout {
                b.with_reader(create_periodic_reader(meter_interval))
            } else {
                b
            }
        })
        .build();

    global::set_meter_provider(provider.clone());
    install_process_global_recorder(recorder).map_err(|e| TelemetryError::InstallMetricsRecorder(e.to_string()))?;
    set_observability_metric_enabled(true);
    Ok(Some(provider))
}

fn get_object_duration_histogram_view(instrument: &Instrument) -> Option<Stream> {
    if !is_get_object_duration_histogram_metric(instrument.name()) {
        return None;
    }

    Stream::builder()
        .with_aggregation(Aggregation::ExplicitBucketHistogram {
            boundaries: GET_OBJECT_DURATION_HISTOGRAM_BUCKETS.to_vec(),
            record_min_max: true,
        })
        .build()
        .ok()
}

fn is_get_object_duration_histogram_metric(name: &str) -> bool {
    GET_OBJECT_DURATION_HISTOGRAM_METRICS.contains(&name)
}

/// Build an optional [`SdkLoggerProvider`] for the given log endpoint.
///
/// Returns `None` when the endpoint is empty or log export is disabled.
/// The caller wraps the resulting provider in an OpenTelemetry tracing bridge.
fn build_logger_provider(
    log_ep: &str,
    config: &OtelConfig,
    otlp_tls_ca_bundle: Option<&OtlpTlsCaBundle>,
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
    let timeout = resolve_signal_timeout(config.endpoint_timeout_millis, config.log_timeout_millis);
    if let Some(timeout) = timeout {
        exporter_builder = exporter_builder.with_timeout(timeout);
    }
    if let Some(http_client) = build_otlp_http_client(otlp_tls_ca_bundle, timeout, OTEL_EXPORTER_OTLP_LOGS_TIMEOUT)? {
        exporter_builder = exporter_builder.with_http_client(http_client);
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
#[cfg(all(
    feature = "pyroscope",
    any(target_os = "macos", all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))
))]
pub(super) fn init_profiler(config: &OtelConfig) -> Option<ProfilingAgent> {
    use pyroscope::backend::{BackendConfig, PprofConfig, pprof_backend};
    use pyroscope::pyroscope::PyroscopeAgentBuilder;
    use rustfs_config::VERSION;

    if !config
        .profiling_export_enabled
        .unwrap_or(rustfs_config::DEFAULT_OBS_PROFILING_EXPORT_ENABLED)
    {
        return None;
    }

    let Some(endpoint) = config
        .profiling_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
    else {
        warn!(
            backend = "pyroscope",
            result = "profiling_endpoint_missing",
            "Profiling export is enabled but no profiling endpoint was configured"
        );
        return None;
    };

    if url::Url::parse(endpoint).is_err() {
        log_profiler_failure("profiling_endpoint_invalid", "invalid_endpoint");
        return None;
    }

    // Configure Pyroscope Agent
    let backend = pprof_backend(PprofConfig::default(), BackendConfig::default());
    let service_name = config.service_name.as_deref().unwrap_or(APP_NAME);
    let version = config.service_version.as_deref().unwrap_or(VERSION);
    let sample_rate = 100; // 100 Hz

    let agent = match PyroscopeAgentBuilder::new(endpoint, service_name, sample_rate, "pyroscope-rs", "1.0.1", backend)
        .tags(vec![("version", version), ("profile_type", "cpu")])
        .build()
    {
        Ok(agent) => agent,
        Err(_) => {
            log_profiler_failure("profiling_agent_build_failed", "build");
            return None;
        }
    };

    match agent.start() {
        Ok(agent) => Some(agent),
        Err(_) => {
            log_profiler_failure("profiling_agent_start_failed", "start");
            None
        }
    }
}

#[cfg(not(all(
    feature = "pyroscope",
    any(target_os = "macos", all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))
)))]
pub(super) fn init_profiler(config: &OtelConfig) -> Option<ProfilingAgent> {
    if config
        .profiling_export_enabled
        .unwrap_or(rustfs_config::DEFAULT_OBS_PROFILING_EXPORT_ENABLED)
    {
        warn!(
            backend = "pyroscope",
            result = "profiling_feature_not_compiled",
            required_feature = "pyroscope",
            "Profiling export is enabled but this binary was built without Pyroscope support"
        );
    }
    None
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

fn resolve_meter_interval(config: &OtelConfig) -> u64 {
    match config.meter_interval {
        Some(0) => {
            warn!(
                result = "invalid_meter_interval",
                configured_seconds = 0_u64,
                fallback_seconds = METER_INTERVAL,
                "Metrics export interval is invalid; using default interval"
            );
            METER_INTERVAL
        }
        Some(interval) => interval,
        None => METER_INTERVAL,
    }
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

struct OtlpTlsCaBundle {
    certificates: Vec<reqwest::Certificate>,
}

impl OtlpTlsCaBundle {
    fn load(config: &OtelConfig) -> Result<Option<Self>, TelemetryError> {
        let Some(ca_file) = config.tls_ca_file.as_deref() else {
            return Ok(None);
        };
        let path = Path::new(ca_file);
        if !path.is_absolute() {
            return Err(TelemetryError::OtlpTlsCaPathNotAbsolute);
        }

        let pem_bundle = fs::read(path).map_err(TelemetryError::OtlpTlsCaRead)?;
        if pem_bundle.iter().all(u8::is_ascii_whitespace) {
            return Err(TelemetryError::OtlpTlsCaEmpty);
        }
        let certificates = reqwest::Certificate::from_pem_bundle(&pem_bundle).map_err(TelemetryError::OtlpTlsCaParse)?;
        if certificates.is_empty() {
            return Err(TelemetryError::OtlpTlsCaInvalid);
        }
        Ok(Some(Self { certificates }))
    }

    fn build_http_client(
        &self,
        configured_timeout: Option<Duration>,
        otel_signal_timeout_env: &str,
    ) -> Result<reqwest::Client, TelemetryError> {
        let timeout = configured_timeout.unwrap_or_else(|| resolve_otlp_http_timeout(otel_signal_timeout_env));
        reqwest::Client::builder()
            .timeout(timeout)
            .tls_certs_merge(self.certificates.clone())
            .build()
            .map_err(TelemetryError::BuildOtlpHttpClient)
    }
}

fn build_otlp_http_client(
    otlp_tls_ca_bundle: Option<&OtlpTlsCaBundle>,
    configured_timeout: Option<Duration>,
    otel_signal_timeout_env: &str,
) -> Result<Option<reqwest::Client>, TelemetryError> {
    otlp_tls_ca_bundle
        .map(|bundle| bundle.build_http_client(configured_timeout, otel_signal_timeout_env))
        .transpose()
}

fn resolve_otlp_http_timeout(signal_timeout_env: &str) -> Duration {
    [signal_timeout_env, OTEL_EXPORTER_OTLP_TIMEOUT]
        .into_iter()
        .find_map(|name| std::env::var(name).ok().and_then(|value| value.parse::<u64>().ok()))
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_OTLP_HTTP_TIMEOUT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::io::{self, Write};
    use std::path::Path;
    use std::process::Command;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const LOG_TRACER_CHILD_ENV: &str = "RUSTFS_OBS_LOG_TRACER_CHILD";

    fn config_with_tls_ca_file(path: &Path) -> OtelConfig {
        OtelConfig {
            tls_ca_file: Some(path.display().to_string()),
            ..OtelConfig::default()
        }
    }

    fn load_tls_ca_bundle(path: &Path) -> OtlpTlsCaBundle {
        OtlpTlsCaBundle::load(&config_with_tls_ca_file(path))
            .expect("load CA bundle")
            .expect("configured CA file should produce a bundle")
    }

    async fn spawn_test_tls_server() -> (String, String, tokio::task::JoinHandle<bool>) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let certified =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).expect("generate TLS server certificate");
        let ca_pem = certified.cert.pem();
        let private_key = rustls_pki_types::PrivateKeyDer::try_from(certified.signing_key.serialize_der())
            .expect("convert TLS server private key");
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certified.cert.der().clone()], private_key)
            .expect("build TLS server config");
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind TLS test server");
        let endpoint = format!("https://{}", listener.local_addr().expect("read TLS test server address"));
        let server = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return false;
            };
            let Ok(mut stream) = acceptor.accept(stream).await else {
                return false;
            };
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1_024];
            loop {
                let Ok(read) = stream.read(&mut buffer).await else {
                    return false;
                };
                if read == 0 {
                    return false;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
                .await
                .is_ok()
                && stream.shutdown().await.is_ok()
        });
        (endpoint, ca_pem, server)
    }

    #[tokio::test]
    async fn otlp_http_client_trusts_every_certificate_in_a_pem_bundle() {
        let (first_endpoint, first_ca_pem, first_server) = spawn_test_tls_server().await;
        let (second_endpoint, second_ca_pem, second_server) = spawn_test_tls_server().await;
        let file = tempfile::NamedTempFile::new().expect("create CA bundle file");
        std::fs::write(file.path(), format!("{first_ca_pem}\n{second_ca_pem}")).expect("write CA bundle");

        let bundle = load_tls_ca_bundle(file.path());
        let client = build_otlp_http_client(Some(&bundle), Some(Duration::from_millis(250)), OTEL_EXPORTER_OTLP_TRACES_TIMEOUT)
            .expect("build client with CA bundle");
        let client = client.expect("custom CA should build a client");
        assert_eq!(
            client
                .get(first_endpoint)
                .send()
                .await
                .expect("first bundled CA should complete TLS handshake")
                .status(),
            reqwest::StatusCode::OK
        );
        assert_eq!(
            client
                .get(second_endpoint)
                .send()
                .await
                .expect("second bundled CA should complete TLS handshake")
                .status(),
            reqwest::StatusCode::OK
        );
        assert!(first_server.await.expect("first bundled TLS server task"));
        assert!(second_server.await.expect("second bundled TLS server task"));
    }

    #[test]
    fn otlp_http_client_rejects_relative_missing_empty_and_invalid_ca_files() {
        let relative = OtelConfig {
            tls_ca_file: Some("otlp-ca.pem".to_string()),
            ..OtelConfig::default()
        };
        assert!(matches!(OtlpTlsCaBundle::load(&relative), Err(TelemetryError::OtlpTlsCaPathNotAbsolute)));

        let missing_directory = tempfile::tempdir().expect("create missing CA directory");
        let missing = OtelConfig {
            tls_ca_file: Some(missing_directory.path().join("otlp-ca.pem").display().to_string()),
            ..OtelConfig::default()
        };
        assert!(matches!(OtlpTlsCaBundle::load(&missing), Err(TelemetryError::OtlpTlsCaRead(_))));

        let empty = tempfile::NamedTempFile::new().expect("create empty CA file");
        assert!(matches!(
            OtlpTlsCaBundle::load(&config_with_tls_ca_file(empty.path())),
            Err(TelemetryError::OtlpTlsCaEmpty)
        ));

        let invalid = tempfile::NamedTempFile::new().expect("create invalid CA file");
        std::fs::write(invalid.path(), b"not a PEM certificate").expect("write invalid CA file");
        assert!(matches!(
            OtlpTlsCaBundle::load(&config_with_tls_ca_file(invalid.path())),
            Err(TelemetryError::OtlpTlsCaInvalid)
        ));
    }

    #[tokio::test]
    async fn otlp_http_client_trusts_the_configured_ca_and_rejects_another_ca() {
        let (endpoint, ca_pem, trusted_server) = spawn_test_tls_server().await;
        let trusted_file = tempfile::NamedTempFile::new().expect("create trusted CA file");
        std::fs::write(trusted_file.path(), ca_pem).expect("write trusted CA file");
        let trusted_bundle = load_tls_ca_bundle(trusted_file.path());
        let trusted_client =
            build_otlp_http_client(Some(&trusted_bundle), Some(Duration::from_secs(1)), OTEL_EXPORTER_OTLP_TRACES_TIMEOUT)
                .expect("build trusted client")
                .expect("custom CA should build a client");
        assert_eq!(
            trusted_client
                .get(&endpoint)
                .send()
                .await
                .expect("configured CA should complete TLS handshake")
                .status(),
            reqwest::StatusCode::OK
        );
        assert!(trusted_server.await.expect("trusted TLS server task"));

        let (endpoint, _ca_pem, untrusted_server) = spawn_test_tls_server().await;
        let wrong_ca =
            rcgen::generate_simple_self_signed(vec!["wrong.test".to_string()]).expect("generate unrelated CA certificate");
        let untrusted_file = tempfile::NamedTempFile::new().expect("create unrelated CA file");
        std::fs::write(untrusted_file.path(), wrong_ca.cert.pem()).expect("write unrelated CA file");
        let untrusted_bundle = load_tls_ca_bundle(untrusted_file.path());
        let untrusted_client =
            build_otlp_http_client(Some(&untrusted_bundle), Some(Duration::from_secs(1)), OTEL_EXPORTER_OTLP_TRACES_TIMEOUT)
                .expect("build untrusted client")
                .expect("custom CA should build a client");
        assert!(untrusted_client.get(&endpoint).send().await.is_err());
        assert!(!untrusted_server.await.expect("untrusted TLS server task"));
    }

    #[test]
    fn otlp_http_client_is_not_built_without_a_custom_ca_file() {
        assert!(
            build_otlp_http_client(None, None, OTEL_EXPORTER_OTLP_TRACES_TIMEOUT)
                .expect("no CA file keeps the default client")
                .is_none()
        );
    }

    #[derive(Clone)]
    struct TestWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for TestWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("lock test log buffer").extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TestWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    /// Valid ratios should produce trace-id-ratio sampling.
    fn test_build_tracer_sampler_uses_trace_ratio_for_valid_values() {
        let sampler = build_tracer_sampler(0.0);
        let rendered = format!("{sampler:?}");
        assert!(rendered.contains("ParentBased"));
        assert!(rendered.contains("TraceIdRatioBased"));

        let sampler = build_tracer_sampler(1.0);
        let rendered = format!("{sampler:?}");
        assert!(rendered.contains("ParentBased"));
        assert!(rendered.contains("TraceIdRatioBased"));

        let sampler = build_tracer_sampler(0.5);
        let rendered = format!("{sampler:?}");
        assert!(rendered.contains("ParentBased"));
        assert!(rendered.contains("TraceIdRatioBased"));
    }

    #[test]
    /// Invalid ratios should degrade to the safest non-dropping sampler.
    fn test_build_tracer_sampler_rejects_invalid_ratio_with_always_on() {
        let sampler = build_tracer_sampler(-0.1);
        let rendered = format!("{sampler:?}");
        assert!(rendered.contains("ParentBased"));
        assert!(rendered.contains("AlwaysOn"));

        let sampler = build_tracer_sampler(1.2);
        let rendered = format!("{sampler:?}");
        assert!(rendered.contains("ParentBased"));
        assert!(rendered.contains("AlwaysOn"));
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

    #[test]
    fn test_normalize_otlp_root_endpoint_trims_trailing_slashes() {
        assert_eq!(normalize_otlp_root_endpoint("http://collector:4318/"), "http://collector:4318");
        assert_eq!(normalize_otlp_root_endpoint("http://collector:4318///"), "http://collector:4318");
        assert_eq!(normalize_otlp_root_endpoint("http://collector:4318"), "http://collector:4318");
    }

    #[test]
    fn test_resolve_signal_endpoint_avoids_double_slashes() {
        assert_eq!(
            resolve_signal_endpoint(None, normalize_otlp_root_endpoint("http://collector:4318/"), "/v1/traces"),
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            resolve_signal_endpoint(Some("http://custom:4318/v1/custom"), "http://collector:4318", "/v1/traces"),
            "http://custom:4318/v1/custom"
        );
        assert_eq!(resolve_signal_endpoint(None, "", "/v1/traces"), "");
    }

    #[test]
    fn test_resolve_otlp_use_stdout_honors_config_or_environment_default() {
        assert!(resolve_otlp_use_stdout(Some(true), true));
        assert!(!resolve_otlp_use_stdout(Some(false), false));
        assert!(resolve_otlp_use_stdout(None, false));
        assert!(!resolve_otlp_use_stdout(None, true));
    }

    #[test]
    fn test_resolve_meter_interval_rejects_zero() {
        let config = OtelConfig {
            meter_interval: Some(0),
            ..OtelConfig::default()
        };

        assert_eq!(resolve_meter_interval(&config), METER_INTERVAL);
    }

    #[test]
    fn test_get_object_duration_histogram_metric_match_is_scoped() {
        assert!(is_get_object_duration_histogram_metric("rustfs_io_get_object_stage_duration_seconds"));
        assert!(is_get_object_duration_histogram_metric("rustfs_io_get_object_request_duration_seconds"));
        assert!(!is_get_object_duration_histogram_metric("rustfs_io_put_object_request_duration_seconds"));
        assert!(!is_get_object_duration_histogram_metric("rustfs_io_get_object_response_size_bytes"));
    }

    #[test]
    fn test_get_object_duration_histogram_buckets_are_sorted() {
        assert!(GET_OBJECT_DURATION_HISTOGRAM_BUCKETS.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(GET_OBJECT_DURATION_HISTOGRAM_BUCKETS.first(), Some(&0.0001));
        assert_eq!(GET_OBJECT_DURATION_HISTOGRAM_BUCKETS.last(), Some(&10.0));
    }

    #[cfg(all(
        feature = "pyroscope",
        any(target_os = "macos", all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))
    ))]
    #[test]
    fn test_init_profiler_invalid_endpoint_redacts_sensitive_components() {
        let endpoint = "https://profile-user:profile-token@10.24.0.5:invalid/private/profiles?access_token=query-secret";
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = TestWriter(Arc::clone(&buffer));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(writer)
            .finish();

        let config = OtelConfig {
            profiling_export_enabled: Some(true),
            profiling_endpoint: Some(endpoint.to_string()),
            ..OtelConfig::default()
        };

        tracing::subscriber::with_default(subscriber, || assert!(init_profiler(&config).is_none()));

        let rendered = String::from_utf8(buffer.lock().expect("lock test log buffer").clone()).expect("decode test log");
        assert!(rendered.contains(REDACTED_PROFILING_ENDPOINT));
        assert!(rendered.contains("error_kind=\"invalid_endpoint\""));
        for leaked in [
            "profile-user",
            "profile-token",
            "10.24.0.5",
            "private/profiles",
            "query-secret",
        ] {
            assert!(!rendered.contains(leaked), "profiling log leaked {leaked}: {rendered}");
        }
    }

    #[test]
    fn test_pyroscope_upload_failure_targets_stay_filtered_when_env_filter_enables_them() {
        let endpoint = "https://profile-user:profile-token@10.24.0.5/private/profiles?access_token=query-secret";
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = TestWriter(Arc::clone(&buffer));
        let env_filter = tracing_subscriber::EnvFilter::new("pyroscope=trace")
            .add_directive("trace".parse().expect("parse trace filter directive"))
            .add_directive("Pyroscope::Session=trace".parse().expect("parse Pyroscope filter directive"));
        let subscriber = tracing_subscriber::registry()
            .with(env_filter)
            .with(pyroscope_log_filter())
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .without_time()
                    .with_writer(writer),
            );

        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(target: "pyroscope::session", "SessionManager - Failed to send session: {endpoint}");
            tracing::error!(target: "Pyroscope::Session", "SessionManager - Failed to send session: {endpoint}");
        });

        let rendered = String::from_utf8(buffer.lock().expect("lock test log buffer").clone()).expect("decode test log");
        assert!(rendered.is_empty(), "filtered Pyroscope upload failure reached a log sink: {rendered}");
    }

    #[test]
    fn test_pyroscope_log_facade_upload_failure_is_filtered() {
        let endpoint = "https://profile-user:profile-token@10.24.0.5/private/profiles?access_token=query-secret";
        if env::var_os(LOG_TRACER_CHILD_ENV).is_some() {
            tracing_subscriber::registry()
                .with(build_env_filter("info", None))
                .with(pyroscope_log_filter())
                .with(tracing_subscriber::fmt::layer().with_ansi(false).without_time())
                .try_init()
                .expect("install isolated LogTracer subscriber");
            log::error!(target: "pyroscope::session", "SessionManager - Failed to send session: {endpoint}");
            log::error!(target: "Pyroscope::Session", "SessionManager - Failed to send session: {endpoint}");
            return;
        }

        let output = Command::new(env::current_exe().expect("resolve test executable"))
            .args([
                "--exact",
                "telemetry::otel::tests::test_pyroscope_log_facade_upload_failure_is_filtered",
                "--nocapture",
            ])
            .env(LOG_TRACER_CHILD_ENV, "1")
            .env("RUST_LOG", "trace,pyroscope=trace,Pyroscope::Session=trace")
            .output()
            .expect("run isolated LogTracer test process");
        assert!(output.status.success(), "isolated LogTracer test failed: {output:?}");

        let rendered = format!("{}{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        for leaked in [
            "profile-user",
            "profile-token",
            "10.24.0.5",
            "private/profiles",
            "query-secret",
        ] {
            assert!(!rendered.contains(leaked), "LogTracer path leaked {leaked}: {rendered}");
        }
    }
}
