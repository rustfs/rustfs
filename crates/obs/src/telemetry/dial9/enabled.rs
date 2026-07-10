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

//! `dial9-tokio-telemetry` integration, compiled when the `dial9` feature is on.
//!
//! Captures Tokio runtime-level events (poll start/end, worker park/unpark,
//! task spawn/terminate) into rotating binary trace segments. This is an
//! on-demand profiler rather than always-on telemetry: it is disabled by
//! default and requires a `--cfg tokio_unstable` build.

use super::config::Dial9Config;
use super::state::{dial9_runtime_state, measure_disk_usage_bytes};
use super::{EVENT_DIAL9_STATE, LOG_COMPONENT_OBS, LOG_SUBSYSTEM_DIAL9};
use crate::TelemetryError;
use dial9_tokio_telemetry::telemetry::{ProcessResourceUsageConfig, RotatingWriter, TaskDumpConfig, TracedRuntime};
use std::time::Duration;
use tracing::{info, warn};

pub use dial9_tokio_telemetry::telemetry::TelemetryGuard;

/// How often the background refresher restates trace-file disk usage.
const DISK_USAGE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Name recorded in segment metadata so the trace viewer can label workers.
const RUNTIME_NAME: &str = "rustfs-worker";

/// Owns a live dial9 telemetry session.
///
/// Dropping this guard flushes buffered events and seals the active segment.
/// It must be dropped before the process exits: a guard parked in a `static` is
/// never dropped, and the final buffered events — usually the interesting ones —
/// are lost.
pub struct Dial9SessionGuard {
    guard: TelemetryGuard,
    config: Dial9Config,
}

impl Dial9SessionGuard {
    /// Whether the underlying telemetry session is recording.
    pub fn is_active(&self) -> bool {
        self.guard.is_enabled()
    }

    /// The configuration this session was built from.
    pub fn config(&self) -> &Dial9Config {
        &self.config
    }
}

impl std::fmt::Debug for Dial9SessionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dial9SessionGuard")
            .field("active", &self.is_active())
            .field("output_dir", &self.config.output_dir)
            .finish()
    }
}

impl Drop for Dial9SessionGuard {
    fn drop(&mut self) {
        dial9_runtime_state().record_runtime_stopped();
        info!(
            event = EVENT_DIAL9_STATE,
            component = LOG_COMPONENT_OBS,
            subsystem = LOG_SUBSYSTEM_DIAL9,
            state = "flushed",
            "dial9 state changed"
        );
        // `TelemetryGuard`'s own `Drop` flushes buffered events and seals the
        // active segment; it runs immediately after this body.
    }
}

/// Build a Tokio runtime with dial9 telemetry attached and recording started.
///
/// # Errors
///
/// Returns an error when dial9 is not enabled, when the output directory cannot
/// be created, or when the traced runtime fails to build. Callers are expected
/// to fall back to a standard runtime.
pub fn build_traced_runtime(
    builder: tokio::runtime::Builder,
) -> Result<(tokio::runtime::Runtime, Dial9SessionGuard), TelemetryError> {
    let config = Dial9Config::from_env();
    if !config.enabled {
        return Err(TelemetryError::Io("dial9 telemetry is not enabled".to_string()));
    }

    std::fs::create_dir_all(&config.output_dir).map_err(|e| {
        dial9_runtime_state().record_runtime_error(&config);
        TelemetryError::Io(format!("Failed to create dial9 output directory '{}': {e}", config.output_dir))
    })?;

    let writer = RotatingWriter::new(config.base_path(), config.max_file_size, config.total_disk_budget()).map_err(|e| {
        dial9_runtime_state().record_runtime_error(&config);
        TelemetryError::Io(format!("Failed to create dial9 RotatingWriter: {e}"))
    })?;

    // `with_trace_path` transitions the builder into the state that spawns the
    // background worker, which drives the segment pipeline.
    let traced = TracedRuntime::builder()
        .with_trace_path(&config.output_dir)
        .with_task_tracking(true)
        .with_runtime_name(RUNTIME_NAME)
        .with_process_resource_usage(ProcessResourceUsageConfig::default());

    let traced = if config.task_dump_enabled {
        traced.with_task_dumps(
            TaskDumpConfig::builder()
                .idle_threshold(config.task_dump_idle_threshold)
                .build(),
        )
    } else {
        traced
    };

    // `build_and_start` rather than `build`: `build` returns a live guard that
    // never records, writing segments that contain only a header.
    //
    // No `with_s3_uploader` here: dial9's `worker-s3` feature carries a
    // vulnerable TLS stack. See the note in `crates/obs/Cargo.toml`.
    finish_traced_runtime(traced.build_and_start(builder, writer), config)
}

/// Publish the outcome of a traced-runtime build and start the background
/// disk-usage refresher.
fn finish_traced_runtime(
    started: std::io::Result<(tokio::runtime::Runtime, TelemetryGuard)>,
    config: Dial9Config,
) -> Result<(tokio::runtime::Runtime, Dial9SessionGuard), TelemetryError> {
    let (runtime, guard) = started.map_err(|e| {
        dial9_runtime_state().record_runtime_error(&config);
        TelemetryError::Io(format!("Failed to build dial9 TracedRuntime: {e}"))
    })?;

    // `is_enabled` distinguishes a live guard from the inert one a lenient
    // config produces after a build failure. It does NOT mean recording has
    // started — a guard from `build` (rather than `build_and_start`) reports
    // `true` while writing segments that contain only a header. Recording is
    // guaranteed by the `build_and_start` call above, not by this check.
    if !guard.is_enabled() {
        dial9_runtime_state().record_runtime_error(&config);
        return Err(TelemetryError::Io("dial9 TracedRuntime built with telemetry disabled".to_string()));
    }

    dial9_runtime_state().record_runtime_started(&config);
    runtime.spawn(refresh_disk_usage_loop());

    info!(
        event = EVENT_DIAL9_STATE,
        component = LOG_COMPONENT_OBS,
        subsystem = LOG_SUBSYSTEM_DIAL9,
        state = "recording",
        output_dir = %config.output_dir,
        file_prefix = %config.file_prefix,
        disk_budget_bytes = config.total_disk_budget(),
        task_dumps = config.task_dump_enabled,
        "dial9 state changed"
    );

    Ok((runtime, Dial9SessionGuard { guard, config }))
}

/// Periodically restate trace-file disk usage so the metrics collector can read
/// it from an atomic instead of walking the directory on its own thread.
async fn refresh_disk_usage_loop() {
    let mut ticker = tokio::time::interval(DISK_USAGE_REFRESH_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        let Some(location) = dial9_runtime_state().trace_location() else {
            continue;
        };

        // `measure_disk_usage_bytes` walks a directory and stats every entry,
        // so it must stay off the async workers.
        let measured =
            tokio::task::spawn_blocking(move || measure_disk_usage_bytes(&location.output_dir, &location.file_prefix)).await;

        match measured {
            Ok(bytes) => dial9_runtime_state().set_disk_usage_bytes(bytes),
            Err(e) => warn!(
                event = EVENT_DIAL9_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_DIAL9,
                result = "disk_usage_refresh_failed",
                error = %e,
                "dial9 state changed"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::state::{reset_for_test, runtime_stats_snapshot};
    use super::*;

    #[test]
    fn build_traced_runtime_refuses_when_disabled() {
        reset_for_test();
        // dial9 is not configured in the unit-test environment, so
        // `Dial9Config::from_env()` yields a disabled config.
        let result = build_traced_runtime(tokio::runtime::Builder::new_current_thread());
        assert!(result.is_err(), "disabled dial9 must not build a traced runtime");
    }

    #[test]
    fn recording_session_reports_active_until_stopped() {
        reset_for_test();
        let config = Dial9Config {
            enabled: true,
            ..Dial9Config::default()
        };

        dial9_runtime_state().record_runtime_started(&config);
        assert_eq!(runtime_stats_snapshot().active_sessions, 1);

        // Mirrors what `Dial9SessionGuard::drop` publishes.
        dial9_runtime_state().record_runtime_stopped();
        assert_eq!(runtime_stats_snapshot().active_sessions, 0);
    }
}
