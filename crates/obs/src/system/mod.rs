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

//! System monitoring module (DEPRECATED).
//!
//! **Deprecation Notice**: This module has been migrated to `rustfs-metrics`.
//! Please use `rustfs_metrics::init_metrics_system()` instead.
//!
//! This module will be removed in a future version (planned for v0.3.0).
//!
//! # Migration Guide
//!
//! Before:
//! ```ignore
//! use rustfs_obs::SystemObserver;
//! SystemObserver::init_process_observer().await?;
//! ```
//!
//! After:
//! ```ignore
//! use tokio_util::sync::CancellationToken;
//! use rustfs_metrics::init_metrics_system;
//!
//! let token = CancellationToken::new();
//! init_metrics_system(token.clone());
//! ```

use crate::{GlobalError, observability_metric_enabled};
use opentelemetry::{global::meter, metrics::Meter};
use sysinfo::Pid;

mod attributes;
mod collector;
#[cfg(feature = "gpu")]
mod gpu;
mod metrics;

/// System observer for process monitoring (DEPRECATED).
///
/// **Deprecated**: Use `rustfs_metrics::init_metrics_system()` instead.
/// This struct will be removed in a future version.
#[deprecated(
    since = "0.2.0",
    note = "Use rustfs_metrics::init_metrics_system() instead. This module will be removed in v0.3.0."
)]
pub struct SystemObserver {}

#[allow(deprecated)]
impl SystemObserver {
    /// Initialize the indicator collector for the current process (DEPRECATED).
    ///
    /// **Deprecated**: Use `rustfs_metrics::init_metrics_system()` instead.
    ///
    /// This function will create a new `Collector` instance and start collecting metrics.
    /// It will run indefinitely until the process is terminated.
    #[deprecated(
        since = "0.2.0",
        note = "Use rustfs_metrics::init_metrics_system() instead. See module documentation for migration guide."
    )]
    pub async fn init_process_observer() -> Result<(), GlobalError> {
        if observability_metric_enabled() {
            let meter = meter("system");
            let pid = sysinfo::get_current_pid().map_err(|e| GlobalError::PidError(e.to_string()))?;
            return SystemObserver::init_process_observer_for_pid(meter, pid).await;
        }
        Ok(())
    }

    /// Initialize the metric collector for the specified PID process (DEPRECATED).
    ///
    /// **Deprecated**: Use `rustfs_metrics` collectors directly instead.
    ///
    /// This function will create a new `Collector` instance and start collecting metrics.
    /// It will run indefinitely until the process is terminated.
    #[deprecated(
        since = "0.2.0",
        note = "Use rustfs_metrics collectors directly instead. This function will be removed in v0.3.0."
    )]
    pub async fn init_process_observer_for_pid(meter: Meter, pid: Pid) -> Result<(), GlobalError> {
        let interval_ms = rustfs_utils::get_env_u64(
            rustfs_config::observability::ENV_OBS_METRICS_SYSTEM_INTERVAL_MS,
            rustfs_config::observability::DEFAULT_METRICS_SYSTEM_INTERVAL_MS,
        );
        let mut collector = collector::Collector::new(pid, meter, interval_ms)?;
        collector.run().await
    }
}
