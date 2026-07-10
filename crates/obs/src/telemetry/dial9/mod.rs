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

//! Tokio runtime-level telemetry via `dial9-tokio-telemetry`.
//!
//! This is an on-demand profiler for executor-level faults — long polls that
//! stall a worker, park/unpark storms, tasks that never yield — which are
//! invisible to request-level metrics and spans.
//!
//! # Compile-time support
//!
//! Telemetry requires the `dial9` cargo feature *and* a `--cfg tokio_unstable`
//! build; `build.rs` enforces that pairing. Without the feature this module
//! still exposes its full API, backed by [`disabled`] stubs, so no caller needs
//! a `#[cfg]`. Use [`is_supported`] to test compile-time support,
//! [`is_configured`] to test operator intent, and [`is_enabled`] for the
//! conjunction that decides whether telemetry actually runs.
//!
//! # Cost
//!
//! Trace segments are written to disk continuously and evicted oldest-first
//! once `RUSTFS_RUNTIME_DIAL9_MAX_FILE_SIZE * RUSTFS_RUNTIME_DIAL9_ROTATION_COUNT`
//! bytes are retained. Under a high poll rate the budget can wrap in minutes,
//! so size it against the window you need to capture.
//!
//! # Known observability gap
//!
//! `dial9`'s `RotatingWriter` stops accepting writes (its internal `Finished`
//! state) when the output directory disappears or a segment cannot be sealed,
//! and it exposes no way to observe that from outside. `TelemetryGuard::is_enabled`
//! reports how the session was *built*, not whether it is still writing. There
//! is therefore no `writer_healthy` metric: it could only ever be hard-coded to
//! `1`. Watch `rustfs_dial9_disk_usage_bytes` — a session that is recording but
//! whose disk usage stops growing has most likely hit this state.

mod config;
mod state;

#[cfg(not(feature = "dial9"))]
mod disabled;
#[cfg(feature = "dial9")]
mod enabled;

pub(super) const LOG_COMPONENT_OBS: &str = "obs";
pub(super) const LOG_SUBSYSTEM_DIAL9: &str = "dial9";
pub(super) const EVENT_DIAL9_STATE: &str = "dial9_state";

pub use config::{Dial9Config, is_configured, is_enabled, is_supported};
pub(crate) use state::runtime_stats_snapshot;

#[cfg(feature = "dial9")]
pub use enabled::{Dial9SessionGuard, TelemetryGuard, build_traced_runtime};

#[cfg(not(feature = "dial9"))]
pub use disabled::{Dial9SessionGuard, TelemetryGuard, build_traced_runtime};
