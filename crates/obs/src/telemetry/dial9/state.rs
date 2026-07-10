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

//! Process-global observable state for dial9 telemetry.
//!
//! Every field is an atomic so that [`runtime_stats_snapshot`] is allocation
//! free and performs no I/O: it runs on the metrics collection path, which must
//! never block. Disk usage is sampled by a background refresher (see
//! `enabled::refresh_disk_usage_loop`) and published here.

use super::config::Dial9Config;
#[cfg(any(feature = "dial9", test))]
use std::path::Path;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Point-in-time view of dial9 runtime state.
#[derive(Debug, Clone, Default)]
pub(crate) struct Dial9RuntimeSnapshot {
    /// 1 while a traced runtime is installed, 0 otherwise.
    pub active_sessions: u64,
    /// Cumulative count of telemetry setup failures.
    pub errors_total: u64,
    /// Bytes on disk for this trace family, as of the last refresh.
    pub disk_usage_bytes: u64,
}

#[derive(Debug)]
pub(super) struct Dial9RuntimeState {
    active_sessions: AtomicU64,
    errors_total: AtomicU64,
    disk_usage_bytes: AtomicU64,
    trace_dir: RwLock<Option<TraceLocation>>,
}

#[derive(Debug, Clone)]
pub(super) struct TraceLocation {
    #[cfg_attr(not(feature = "dial9"), allow(dead_code))]
    pub output_dir: PathBuf,
    #[cfg_attr(not(feature = "dial9"), allow(dead_code))]
    pub file_prefix: String,
}

impl Dial9RuntimeState {
    fn new() -> Self {
        Self {
            active_sessions: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            disk_usage_bytes: AtomicU64::new(0),
            trace_dir: RwLock::new(None),
        }
    }

    pub(super) fn record_config(&self, config: &Dial9Config) {
        *self.trace_dir.write().expect("dial9 trace_dir lock should not be poisoned") = Some(TraceLocation {
            output_dir: PathBuf::from(&config.output_dir),
            file_prefix: config.file_prefix.clone(),
        });
        if !config.enabled {
            self.active_sessions.store(0, Ordering::Relaxed);
        }
    }

    #[cfg(any(feature = "dial9", test))]
    pub(super) fn record_runtime_started(&self, config: &Dial9Config) {
        self.record_config(config);
        self.active_sessions.store(1, Ordering::Relaxed);
    }

    #[cfg(any(feature = "dial9", test))]
    pub(super) fn record_runtime_error(&self, config: &Dial9Config) {
        self.record_config(config);
        self.active_sessions.store(0, Ordering::Relaxed);
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(feature = "dial9")]
    pub(super) fn record_runtime_stopped(&self) {
        self.active_sessions.store(0, Ordering::Relaxed);
    }

    #[cfg(any(feature = "dial9", test))]
    pub(super) fn set_disk_usage_bytes(&self, bytes: u64) {
        self.disk_usage_bytes.store(bytes, Ordering::Relaxed);
    }

    #[cfg(feature = "dial9")]
    pub(super) fn trace_location(&self) -> Option<TraceLocation> {
        self.trace_dir
            .read()
            .expect("dial9 trace_dir lock should not be poisoned")
            .clone()
    }

    fn snapshot(&self) -> Dial9RuntimeSnapshot {
        Dial9RuntimeSnapshot {
            active_sessions: self.active_sessions.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            disk_usage_bytes: self.disk_usage_bytes.load(Ordering::Relaxed),
        }
    }
}

pub(super) fn dial9_runtime_state() -> &'static Dial9RuntimeState {
    static STATE: OnceLock<Dial9RuntimeState> = OnceLock::new();
    STATE.get_or_init(Dial9RuntimeState::new)
}

/// Reset the process-global state. Tests share one process, so any test that
/// mutates the state must call this first.
#[cfg(test)]
pub(super) fn reset_for_test() {
    let state = dial9_runtime_state();
    state.active_sessions.store(0, Ordering::Relaxed);
    state.errors_total.store(0, Ordering::Relaxed);
    state.disk_usage_bytes.store(0, Ordering::Relaxed);
    *state.trace_dir.write().expect("dial9 trace_dir lock should not be poisoned") = None;
}

/// Sum the on-disk size of every segment belonging to this trace family.
///
/// This walks a directory and stats each entry, so it must never run on an
/// async worker or on the metrics collection path. Callers are responsible for
/// scheduling it on a blocking thread.
#[cfg(any(feature = "dial9", test))]
pub(super) fn measure_disk_usage_bytes(output_dir: &Path, file_prefix: &str) -> u64 {
    let Ok(entries) = std::fs::read_dir(output_dir) else {
        return 0;
    };

    entries
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|filename| filename.starts_with(file_prefix))
        })
        .filter_map(|entry| entry.metadata().ok())
        .map(|meta| meta.len())
        .sum()
}

/// Snapshot dial9 runtime state. Lock-free apart from a read lock on the trace
/// location, and performs no I/O.
pub(crate) fn runtime_stats_snapshot() -> Dial9RuntimeSnapshot {
    dial9_runtime_state().snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn measure_disk_usage_sums_only_matching_prefix() {
        let dir = tempdir().expect("create temp dir");
        std::fs::write(dir.path().join("rustfs-tokio.0.bin"), vec![0_u8; 128]).expect("write segment");
        std::fs::write(dir.path().join("rustfs-tokio.1.bin"), vec![0_u8; 64]).expect("write segment");
        std::fs::write(dir.path().join("unrelated.log"), vec![0_u8; 4096]).expect("write unrelated");

        assert_eq!(measure_disk_usage_bytes(dir.path(), "rustfs-tokio"), 192);
    }

    #[test]
    fn measure_disk_usage_of_missing_dir_is_zero() {
        assert_eq!(measure_disk_usage_bytes(Path::new("/nonexistent/rustfs/dial9"), "rustfs-tokio"), 0);
    }

    #[test]
    fn snapshot_reports_cached_disk_usage_without_touching_disk() {
        reset_for_test();
        let config = Dial9Config {
            enabled: true,
            ..Dial9Config::default()
        };
        dial9_runtime_state().record_runtime_started(&config);
        dial9_runtime_state().set_disk_usage_bytes(4096);

        let snapshot = runtime_stats_snapshot();
        assert_eq!(snapshot.active_sessions, 1);
        assert_eq!(snapshot.disk_usage_bytes, 4096);
    }

    #[test]
    fn runtime_error_clears_session_and_counts_the_failure() {
        reset_for_test();
        let config = Dial9Config {
            enabled: true,
            ..Dial9Config::default()
        };
        dial9_runtime_state().record_runtime_started(&config);
        dial9_runtime_state().record_runtime_error(&config);

        let snapshot = runtime_stats_snapshot();
        assert_eq!(snapshot.active_sessions, 0);
        assert_eq!(snapshot.errors_total, 1);
    }
}
