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

//! RAII guard for OpenTelemetry provider lifecycle management.
//!
//! [`OtelGuard`] holds all runtime resources created during telemetry
//! initialisation. Dropping it triggers an ordered shutdown:
//!
//! 1. Tracer provider — flushes pending spans.
//! 2. Meter provider — flushes pending metrics.
//! 3. Logger provider — flushes pending log records.
//! 4. Cleanup task — aborted to prevent lingering background work.
//! 5. Tracing worker guard — flushes buffered log lines written by
//!    `tracing_appender`.

use opentelemetry_sdk::{logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider};

/// RAII guard that owns all active OpenTelemetry providers and the
/// `tracing_appender` worker guard.
///
/// Construct this via the `init_*` functions in [`crate::telemetry`] rather
/// than directly.  The guard must be kept alive for the entire duration of the
/// application — once dropped, all telemetry pipelines are shut down.
pub struct OtelGuard {
    /// Optional tracer provider for distributed tracing.
    pub(crate) tracer_provider: Option<SdkTracerProvider>,
    /// Optional meter provider for metrics collection.
    pub(crate) meter_provider: Option<SdkMeterProvider>,
    /// Optional logger provider for OTLP log export.
    pub(crate) logger_provider: Option<SdkLoggerProvider>,
    /// Worker guard that keeps the non-blocking `tracing_appender` thread
    /// alive.  Dropping it blocks until all buffered records are flushed.
    pub(crate) tracing_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
    /// Optional guard for stdout logging; kept separate to allow independent flushing and shutdown.
    pub(crate) stdout_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
    /// Handle to the background log-cleanup task; aborted on drop.
    pub(crate) cleanup_handle: Option<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for OtelGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelGuard")
            .field("tracer_provider", &self.tracer_provider.is_some())
            .field("meter_provider", &self.meter_provider.is_some())
            .field("logger_provider", &self.logger_provider.is_some())
            .field("tracing_guard", &self.tracing_guard.is_some())
            .field("stdout_guard", &self.stdout_guard.is_some())
            .field("cleanup_handle", &self.cleanup_handle.is_some())
            .finish()
    }
}

impl Drop for OtelGuard {
    /// Shut down all telemetry providers in order.
    ///
    /// Errors during shutdown are printed to `stderr` so they are visible even
    /// after the tracing subscriber has been torn down.
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take()
            && let Err(err) = provider.shutdown()
        {
            eprintln!("Tracer shutdown error: {err:?}");
        }

        if let Some(provider) = self.meter_provider.take()
            && let Err(err) = provider.shutdown()
        {
            eprintln!("Meter shutdown error: {err:?}");
        }

        if let Some(provider) = self.logger_provider.take()
            && let Err(err) = provider.shutdown()
        {
            eprintln!("Logger shutdown error: {err:?}");
        }

        if let Some(handle) = self.cleanup_handle.take() {
            handle.abort();
            eprintln!("Log cleanup task stopped");
        }

        if let Some(guard) = self.tracing_guard.take() {
            drop(guard);
            eprintln!("Tracing guard dropped, flushing logs.");
        }

        if let Some(guard) = self.stdout_guard.take() {
            drop(guard);
            eprintln!("Stdout guard dropped, flushing logs.");
        }
    }
}
