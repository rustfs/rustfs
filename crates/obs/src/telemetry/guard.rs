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
//! 4. Profiling agent — flushes pending profiles.
//! 5. Cleanup task — aborted to prevent lingering background work.
//! 6. Tracing worker guard — flushes buffered log lines written by
//!    `tracing_appender`.
//! 7. Stdout worker guard — flushes buffered log lines written to stdout.

use opentelemetry_sdk::{logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use tracing::{debug, error};

#[cfg(all(
    feature = "pyroscope",
    any(target_os = "macos", all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))
))]
pub(crate) type ProfilingAgent = pyroscope::PyroscopeAgent<pyroscope::pyroscope::PyroscopeAgentRunning>;
#[cfg(not(all(
    feature = "pyroscope",
    any(target_os = "macos", all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))
)))]
pub(crate) type ProfilingAgent = ();
#[cfg(all(feature = "pyroscope", target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
pub(crate) type MemoryProfilingAgent = pyroscope::PyroscopeAgent<pyroscope::pyroscope::PyroscopeAgentRunning>;
#[cfg(not(all(feature = "pyroscope", target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
pub(crate) type MemoryProfilingAgent = ();

const LOG_COMPONENT_OBS: &str = "obs";
const LOG_SUBSYSTEM_GUARD: &str = "guard";
const EVENT_OBS_GUARD_SHUTDOWN: &str = "obs_guard_shutdown";
const STDERR_ERROR_PREFIX: &str = "[ERROR]";

fn format_guard_shutdown_stderr_message(resource: &str, error: impl std::fmt::Display) -> String {
    format!("{STDERR_ERROR_PREFIX} observability guard shutdown failed: resource={resource} error={error}")
}

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
    pub(crate) profiling_agent: Option<ProfilingAgent>,
    pub(crate) memory_profiling_agent: Option<MemoryProfilingAgent>,
    /// Handle to the background log-cleanup task; aborted on drop.
    pub(crate) cleanup_handle: Option<tokio::task::JoinHandle<()>>,
    /// Worker guard that keeps the non-blocking `tracing_appender` thread
    /// alive.  Dropping it blocks until all buffered records are flushed.
    pub(crate) tracing_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
    /// Optional guard for stdout logging; kept separate to allow independent flushing and shutdown.
    pub(crate) stdout_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

impl std::fmt::Debug for OtelGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("OtelGuard");
        s.field("tracer_provider", &self.tracer_provider.is_some())
            .field("meter_provider", &self.meter_provider.is_some())
            .field("logger_provider", &self.logger_provider.is_some())
            .field("profiling_agent", &self.profiling_agent.is_some())
            .field("memory_profiling_agent", &self.memory_profiling_agent.is_some())
            .field("cleanup_handle", &self.cleanup_handle.is_some())
            .field("tracing_guard", &self.tracing_guard.is_some())
            .field("stdout_guard", &self.stdout_guard.is_some())
            .finish()
    }
}

impl Drop for OtelGuard {
    /// Shut down all telemetry providers in order.
    ///
    /// Errors are emitted before tracing resources are dropped so shutdown
    /// diagnostics remain structured and low-noise.
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take()
            && let Err(err) = provider.shutdown()
        {
            if tracing::dispatcher::has_been_set() {
                error!(
                    event = EVENT_OBS_GUARD_SHUTDOWN,
                    component = LOG_COMPONENT_OBS,
                    subsystem = LOG_SUBSYSTEM_GUARD,
                    resource = "tracer_provider",
                    result = "shutdown_failed",
                    error = %err,
                    "observability guard shutdown failed"
                );
            } else {
                eprintln!("{}", format_guard_shutdown_stderr_message("tracer_provider", err));
            }
        }

        if let Some(provider) = self.meter_provider.take()
            && let Err(err) = provider.shutdown()
        {
            if tracing::dispatcher::has_been_set() {
                error!(
                    event = EVENT_OBS_GUARD_SHUTDOWN,
                    component = LOG_COMPONENT_OBS,
                    subsystem = LOG_SUBSYSTEM_GUARD,
                    resource = "meter_provider",
                    result = "shutdown_failed",
                    error = %err,
                    "observability guard shutdown failed"
                );
            } else {
                eprintln!("{}", format_guard_shutdown_stderr_message("meter_provider", err));
            }
        }

        #[cfg(all(
            feature = "pyroscope",
            any(target_os = "macos", all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))
        ))]
        if let Some(agent) = self.profiling_agent.take() {
            match agent.stop() {
                Err(err) => {
                    if tracing::dispatcher::has_been_set() {
                        error!(
                            event = EVENT_OBS_GUARD_SHUTDOWN,
                            component = LOG_COMPONENT_OBS,
                            subsystem = LOG_SUBSYSTEM_GUARD,
                            resource = "profiling_agent",
                            result = "shutdown_failed",
                            error = %err,
                            "observability guard shutdown failed"
                        );
                    } else {
                        eprintln!("{}", format_guard_shutdown_stderr_message("profiling_agent", err));
                    }
                }
                Ok(stopped) => {
                    stopped.shutdown();
                }
            }
        }

        #[cfg(all(feature = "pyroscope", target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
        if let Some(agent) = self.memory_profiling_agent.take() {
            match agent.stop() {
                Err(err) => {
                    if tracing::dispatcher::has_been_set() {
                        error!(
                            event = EVENT_OBS_GUARD_SHUTDOWN,
                            component = LOG_COMPONENT_OBS,
                            subsystem = LOG_SUBSYSTEM_GUARD,
                            resource = "memory_profiling_agent",
                            result = "shutdown_failed",
                            error = %err,
                            "observability guard shutdown failed"
                        );
                    } else {
                        eprintln!("{}", format_guard_shutdown_stderr_message("memory_profiling_agent", err));
                    }
                }
                Ok(stopped) => {
                    stopped.shutdown();
                }
            }
        }

        if let Some(handle) = self.cleanup_handle.take() {
            debug!(
                event = EVENT_OBS_GUARD_SHUTDOWN,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_GUARD,
                resource = "log_cleaner",
                result = "abort_requested",
                "observability guard resource shutdown requested"
            );
            handle.abort();
        }

        if let Some(provider) = self.logger_provider.take()
            && let Err(err) = provider.shutdown()
        {
            // After logger shutdown, the OTLP log bridge is no longer a reliable
            // sink for its own failure path, so fall back to stderr.
            // resource = "logger_provider"
            eprintln!("{}", format_guard_shutdown_stderr_message("logger_provider", err));
        }

        if let Some(guard) = self.tracing_guard.take() {
            debug!(
                event = EVENT_OBS_GUARD_SHUTDOWN,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_GUARD,
                resource = "tracing_guard",
                result = "flush_requested",
                "observability guard resource flush requested"
            );
            drop(guard);
        }

        if let Some(guard) = self.stdout_guard.take() {
            debug!(
                event = EVENT_OBS_GUARD_SHUTDOWN,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_GUARD,
                resource = "stdout_guard",
                result = "flush_requested",
                "observability guard resource flush requested"
            );
            drop(guard);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guard_shutdown_stderr_message_is_actionable() {
        let message = format_guard_shutdown_stderr_message("logger_provider", "flush failed");

        assert!(message.starts_with(STDERR_ERROR_PREFIX));
        assert!(message.contains("resource=logger_provider"));
        assert!(message.contains("error=flush failed"));
    }
}
