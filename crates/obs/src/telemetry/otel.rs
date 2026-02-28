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

use crate::TelemetryError;
use crate::config::OtelConfig;
use crate::global::OBSERVABILITY_METRIC_ENABLED;
use crate::telemetry::filter::build_env_filter;
use crate::telemetry::guard::OtelGuard;
use crate::telemetry::recorder::Recorder;
use crate::telemetry::resource::build_resource;
use metrics::counter;
use opentelemetry::{global, trace::TracerProvider};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Compression, Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use rustfs_config::{
    APP_NAME, DEFAULT_OBS_LOG_STDOUT_ENABLED, DEFAULT_OBS_LOGS_EXPORT_ENABLED, DEFAULT_OBS_METRICS_EXPORT_ENABLED,
    DEFAULT_OBS_TRACES_EXPORT_ENABLED, METER_INTERVAL, SAMPLE_RATIO,
};
use std::{io::IsTerminal, time::Duration};
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
/// implementation to preserve existing OTLP behaviour.
pub(super) fn init_observability_http(
    config: &OtelConfig,
    logger_level: &str,
    is_production: bool,
) -> Result<OtelGuard, TelemetryError> {
    // ── Resource & sampling ──────────────────────────────────────────────────
    let res = build_resource(config);
    let service_name = config.service_name.as_deref().unwrap_or(APP_NAME).to_owned();
    let use_stdout = config.use_stdout.unwrap_or(!is_production);
    let sample_ratio = config.sample_ratio.unwrap_or(SAMPLE_RATIO);
    let sampler = if (0.0..1.0).contains(&sample_ratio) {
        Sampler::TraceIdRatioBased(sample_ratio)
    } else {
        Sampler::AlwaysOn
    };

    // ── Endpoint resolution ───────────────────────────────────────────────────
    // Each signal may have a dedicated endpoint; if absent, fall back to the
    // root endpoint with the standard OTLP path suffix appended.
    let root_ep = config.endpoint.clone();

    let trace_ep: String = config
        .trace_endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{root_ep}/v1/traces"));

    let metric_ep: String = config
        .metric_endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{root_ep}/v1/metrics"));

    let log_ep: String = config
        .log_endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{root_ep}/v1/logs"));

    // ── Tracer provider (HTTP) ────────────────────────────────────────────────
    let tracer_provider = build_tracer_provider(&trace_ep, config, res.clone(), sampler, use_stdout)?;

    // ── Meter provider (HTTP) ─────────────────────────────────────────────────
    let meter_provider = build_meter_provider(&metric_ep, config, res.clone(), &service_name, use_stdout)?;

    // ── Logger provider (HTTP) ────────────────────────────────────────────────
    let logger_provider = build_logger_provider(&log_ep, config, res, use_stdout)?;

    // ── Tracing subscriber registry ───────────────────────────────────────────
    // Build an optional stdout formatting layer. When `log_stdout_enabled` is
    // false the field is `None` and tracing-subscriber will skip it.
    let fmt_layer_opt = if config.log_stdout_enabled.unwrap_or(DEFAULT_OBS_LOG_STDOUT_ENABLED) {
        let enable_color = std::io::stdout().is_terminal();
        let span_event = if is_production { FmtSpan::CLOSE } else { FmtSpan::FULL };
        let layer = tracing_subscriber::fmt::layer()
            .with_timer(LocalTime::rfc_3339())
            .with_target(true)
            .with_ansi(enable_color)
            .with_thread_names(true)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .json()
            .with_current_span(true)
            .with_span_list(true)
            .with_span_events(span_event)
            .with_filter(build_env_filter(logger_level, None));
        Some(layer)
    } else {
        None
    };
    let filter = build_env_filter(logger_level, None);
    let otel_bridge = logger_provider
        .as_ref()
        .map(|p| OpenTelemetryTracingBridge::new(p).with_filter(build_env_filter(logger_level, None)));
    let tracer_layer = tracer_provider
        .as_ref()
        .map(|p| OpenTelemetryLayer::new(p.tracer(service_name.to_string())));
    let metrics_layer = meter_provider.as_ref().map(|p| MetricsLayer::new(p.clone()));

    tracing_subscriber::registry()
        .with(filter)
        .with(ErrorLayer::default())
        .with(fmt_layer_opt)
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
        tracing_guard: None,
        stdout_guard: None,
        cleanup_handle: None,
    })
}

// ─── Private builder helpers ──────────────────────────────────────────────────

/// Build an optional [`SdkTracerProvider`] for the given trace endpoint.
///
/// Returns `None` when the endpoint is empty or trace export is disabled.
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

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(trace_ep)
        .with_protocol(Protocol::HttpBinary)
        .with_compression(Compression::Gzip)
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
    global::set_text_map_propagator(TraceContextPropagator::new());
    Ok(Some(provider))
}

/// Build an optional [`SdkMeterProvider`] for the given metrics endpoint.
///
/// Returns `None` when the endpoint is empty or metric export is disabled.
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

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(metric_ep)
        .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
        .with_protocol(Protocol::HttpBinary)
        .with_compression(Compression::Gzip)
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

    global::set_meter_provider(provider.clone() as SdkMeterProvider);
    metrics::set_global_recorder(recorder).map_err(|e| TelemetryError::InstallMetricsRecorder(e.to_string()))?;
    OBSERVABILITY_METRIC_ENABLED.set(true).ok();
    Ok(Some(provider))
}

/// Build an optional [`SdkLoggerProvider`] for the given log endpoint.
///
/// Returns `None` when the endpoint is empty or log export is disabled.
fn build_logger_provider(
    log_ep: &str,
    config: &OtelConfig,
    res: opentelemetry_sdk::Resource,
    use_stdout: bool,
) -> Result<Option<SdkLoggerProvider>, TelemetryError> {
    if log_ep.is_empty() || !config.logs_export_enabled.unwrap_or(DEFAULT_OBS_LOGS_EXPORT_ENABLED) {
        return Ok(None);
    }

    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(log_ep)
        .with_protocol(Protocol::HttpBinary)
        .with_compression(Compression::Gzip)
        .build()
        .map_err(|e| TelemetryError::BuildLogExporter(e.to_string()))?;

    let mut builder = SdkLoggerProvider::builder().with_resource(res);
    builder = builder.with_batch_exporter(exporter);
    if use_stdout {
        builder = builder.with_batch_exporter(opentelemetry_stdout::LogExporter::default());
    }
    Ok(Some(builder.build()))
}

/// Create a stdout periodic metrics reader for the given interval.
fn create_periodic_reader(interval: u64) -> PeriodicReader<opentelemetry_stdout::MetricExporter> {
    PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default())
        .with_interval(Duration::from_secs(interval))
        .build()
}
