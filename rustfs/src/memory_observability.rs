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

use rustfs_io_metrics::{
    record_cgroup_memory_split, record_cpu_usage, record_memory_usage, record_process_memory_split,
    snapshot_process_resource_and_system,
};
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use sysinfo::System;
use tokio_util::sync::CancellationToken;
use tracing::debug;

static MEMORY_SYSTEM: OnceLock<Mutex<System>> = OnceLock::new();

const ENV_MEMORY_OBSERVABILITY_INTERVAL_SECS: &str = "RUSTFS_MEMORY_OBSERVABILITY_INTERVAL_SECS";
const DEFAULT_MEMORY_OBSERVABILITY_INTERVAL_SECS: u64 = 15;
const MEMORY_OBSERVABILITY_SERVICE_NAME: &str = "memory_observability";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryObservabilityServiceState {
    Disabled,
    Running,
    Stopping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryObservabilityCancellationSource {
    RuntimeToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryObservabilityShutdownHandle {
    RuntimeTokenOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MemoryObservabilityStatusSnapshot {
    pub service: &'static str,
    pub state: MemoryObservabilityServiceState,
    pub metrics_enabled: bool,
    pub interval_secs: u64,
    pub cancellation_source: MemoryObservabilityCancellationSource,
    pub shutdown_handle: MemoryObservabilityShutdownHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryObservabilityDesiredState {
    Disabled,
    Enabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MemoryObservabilityDesiredSnapshot {
    pub state: MemoryObservabilityDesiredState,
    pub interval_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MemoryObservabilityControllerSnapshot {
    pub desired: MemoryObservabilityDesiredSnapshot,
    pub status: MemoryObservabilityStatusSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryObservabilityWorkerMutation {
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MemoryObservabilityReconcilePlan {
    pub service: &'static str,
    pub desired: MemoryObservabilityDesiredSnapshot,
    pub current_state: MemoryObservabilityServiceState,
    pub worker_mutation: MemoryObservabilityWorkerMutation,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MemoryObservabilityController;

impl MemoryObservabilityController {
    pub fn snapshot(&self, ctx: &CancellationToken) -> MemoryObservabilityControllerSnapshot {
        memory_observability_controller_snapshot(ctx)
    }

    pub fn reconcile(&self, ctx: &CancellationToken) -> MemoryObservabilityReconcilePlan {
        let snapshot = self.snapshot(ctx);
        self.reconcile_snapshot(snapshot)
    }

    pub fn reconcile_snapshot(&self, snapshot: MemoryObservabilityControllerSnapshot) -> MemoryObservabilityReconcilePlan {
        MemoryObservabilityReconcilePlan {
            service: MEMORY_OBSERVABILITY_SERVICE_NAME,
            desired: snapshot.desired,
            current_state: snapshot.status.state,
            worker_mutation: MemoryObservabilityWorkerMutation::None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CgroupMemorySnapshot {
    current_bytes: Option<u64>,
    limit_bytes: Option<u64>,
    anon_bytes: Option<u64>,
    file_bytes: Option<u64>,
    active_file_bytes: Option<u64>,
    inactive_file_bytes: Option<u64>,
}

fn memory_system() -> &'static Mutex<System> {
    MEMORY_SYSTEM.get_or_init(|| Mutex::new(System::new()))
}

fn refresh_total_memory() -> u64 {
    let mut system = memory_system().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    system.refresh_memory();
    system.total_memory()
}

fn read_optional_u64(path: &Path) -> Option<u64> {
    let content = std::fs::read_to_string(path).ok()?;
    let trimmed = content.trim();
    if trimmed.is_empty() || trimmed == "max" {
        return None;
    }
    trimmed.parse::<u64>().ok()
}

fn parse_kv_stats(content: &str) -> HashMap<String, u64> {
    content
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let key = parts.next()?;
            let value = parts.next()?.parse::<u64>().ok()?;
            Some((key.to_string(), value))
        })
        .collect()
}

fn read_cgroup_v2() -> Option<CgroupMemorySnapshot> {
    let root = Path::new("/sys/fs/cgroup");
    let stat_path = root.join("memory.stat");
    if !stat_path.exists() {
        return None;
    }

    let stats = parse_kv_stats(&std::fs::read_to_string(&stat_path).ok()?);
    Some(CgroupMemorySnapshot {
        current_bytes: read_optional_u64(&root.join("memory.current")),
        limit_bytes: read_optional_u64(&root.join("memory.max")),
        anon_bytes: stats.get("anon").copied(),
        file_bytes: stats.get("file").copied(),
        active_file_bytes: stats.get("active_file").copied(),
        inactive_file_bytes: stats.get("inactive_file").copied(),
    })
}

fn read_cgroup_v1() -> Option<CgroupMemorySnapshot> {
    let root = Path::new("/sys/fs/cgroup/memory");
    let stat_path = root.join("memory.stat");
    if !stat_path.exists() {
        return None;
    }

    let stats = parse_kv_stats(&std::fs::read_to_string(&stat_path).ok()?);
    Some(CgroupMemorySnapshot {
        current_bytes: read_optional_u64(&root.join("memory.usage_in_bytes")),
        limit_bytes: read_optional_u64(&root.join("memory.limit_in_bytes")),
        anon_bytes: stats.get("total_rss").copied().or_else(|| stats.get("rss").copied()),
        file_bytes: stats.get("total_cache").copied().or_else(|| stats.get("cache").copied()),
        active_file_bytes: stats
            .get("total_active_file")
            .copied()
            .or_else(|| stats.get("active_file").copied()),
        inactive_file_bytes: stats
            .get("total_inactive_file")
            .copied()
            .or_else(|| stats.get("inactive_file").copied()),
    })
}

fn read_cgroup_memory_snapshot() -> Option<CgroupMemorySnapshot> {
    read_cgroup_v2().or_else(read_cgroup_v1)
}

fn configured_memory_observability_interval_secs() -> u64 {
    rustfs_utils::get_env_u64(ENV_MEMORY_OBSERVABILITY_INTERVAL_SECS, DEFAULT_MEMORY_OBSERVABILITY_INTERVAL_SECS).max(1)
}

fn build_memory_observability_desired_snapshot(metrics_enabled: bool, interval_secs: u64) -> MemoryObservabilityDesiredSnapshot {
    let state = if metrics_enabled {
        MemoryObservabilityDesiredState::Enabled
    } else {
        MemoryObservabilityDesiredState::Disabled
    };

    MemoryObservabilityDesiredSnapshot {
        state,
        interval_secs: interval_secs.max(1),
    }
}

fn build_memory_observability_status_snapshot(
    metrics_enabled: bool,
    interval_secs: u64,
    cancellation_requested: bool,
) -> MemoryObservabilityStatusSnapshot {
    let state = if !metrics_enabled {
        MemoryObservabilityServiceState::Disabled
    } else if cancellation_requested {
        MemoryObservabilityServiceState::Stopping
    } else {
        MemoryObservabilityServiceState::Running
    };

    MemoryObservabilityStatusSnapshot {
        service: MEMORY_OBSERVABILITY_SERVICE_NAME,
        state,
        metrics_enabled,
        interval_secs: interval_secs.max(1),
        cancellation_source: MemoryObservabilityCancellationSource::RuntimeToken,
        shutdown_handle: MemoryObservabilityShutdownHandle::RuntimeTokenOnly,
    }
}

pub fn memory_observability_status_snapshot(ctx: &CancellationToken) -> MemoryObservabilityStatusSnapshot {
    build_memory_observability_status_snapshot(
        rustfs_obs::observability_metric_enabled(),
        configured_memory_observability_interval_secs(),
        ctx.is_cancelled(),
    )
}

fn build_memory_observability_controller_snapshot(
    metrics_enabled: bool,
    interval_secs: u64,
    cancellation_requested: bool,
) -> MemoryObservabilityControllerSnapshot {
    MemoryObservabilityControllerSnapshot {
        desired: build_memory_observability_desired_snapshot(metrics_enabled, interval_secs),
        status: build_memory_observability_status_snapshot(metrics_enabled, interval_secs, cancellation_requested),
    }
}

pub fn memory_observability_controller_snapshot(ctx: &CancellationToken) -> MemoryObservabilityControllerSnapshot {
    build_memory_observability_controller_snapshot(
        rustfs_obs::observability_metric_enabled(),
        configured_memory_observability_interval_secs(),
        ctx.is_cancelled(),
    )
}

async fn record_memory_snapshot() {
    match tokio::task::spawn_blocking(|| {
        let (resource, process) = snapshot_process_resource_and_system();
        let total_memory = refresh_total_memory();
        let cgroup = read_cgroup_memory_snapshot();
        (resource, process, total_memory, cgroup)
    })
    .await
    {
        Ok((resource, process, total_memory, cgroup)) => {
            record_memory_usage(process.resident_memory_bytes, total_memory);
            record_cpu_usage(resource.cpu_percent);
            record_process_memory_split(process.resident_memory_bytes, process.virtual_memory_bytes);

            if let Some(cgroup) = cgroup {
                record_cgroup_memory_split(
                    cgroup.current_bytes,
                    cgroup.limit_bytes,
                    cgroup.anon_bytes,
                    cgroup.file_bytes,
                    cgroup.active_file_bytes,
                    cgroup.inactive_file_bytes,
                );
            }
        }
        Err(err) => {
            debug!(error = ?err, "memory observability sampler task failed");
        }
    }
}

pub fn init_memory_observability(ctx: CancellationToken) {
    let interval_secs = configured_memory_observability_interval_secs();
    let interval = Duration::from_secs(interval_secs.max(1));

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ctx.cancelled() => {
                    debug!("memory observability sampler cancelled");
                    break;
                }
                _ = ticker.tick() => {
                    record_memory_snapshot().await;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{
        CgroupMemorySnapshot, MemoryObservabilityCancellationSource, MemoryObservabilityController,
        MemoryObservabilityDesiredState, MemoryObservabilityServiceState, MemoryObservabilityShutdownHandle,
        MemoryObservabilityWorkerMutation, build_memory_observability_controller_snapshot,
        build_memory_observability_status_snapshot, parse_kv_stats, read_optional_u64,
    };
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn parse_kv_stats_extracts_numeric_pairs() {
        let parsed = parse_kv_stats("anon 12\nfile 34\nactive_file 56\n");
        assert_eq!(parsed.get("anon").copied(), Some(12));
        assert_eq!(parsed.get("file").copied(), Some(34));
        assert_eq!(parsed.get("active_file").copied(), Some(56));
    }

    #[test]
    fn read_optional_u64_parses_numeric_and_max_values() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let value_path: PathBuf = tempdir.path().join("value");
        let max_path: PathBuf = tempdir.path().join("max");
        fs::write(&value_path, "123\n").expect("write numeric");
        fs::write(&max_path, "max\n").expect("write max");

        assert_eq!(read_optional_u64(&value_path), Some(123));
        assert_eq!(read_optional_u64(&max_path), None);
    }

    #[test]
    fn cgroup_memory_snapshot_defaults_are_empty() {
        let snapshot = CgroupMemorySnapshot::default();
        assert_eq!(snapshot.current_bytes, None);
        assert_eq!(snapshot.limit_bytes, None);
        assert_eq!(snapshot.anon_bytes, None);
        assert_eq!(snapshot.file_bytes, None);
        assert_eq!(snapshot.active_file_bytes, None);
        assert_eq!(snapshot.inactive_file_bytes, None);
    }

    #[test]
    fn memory_observability_snapshot_reports_disabled_when_metrics_are_disabled() {
        let snapshot = build_memory_observability_status_snapshot(false, 15, false);

        assert_eq!(snapshot.service, "memory_observability");
        assert_eq!(snapshot.state, MemoryObservabilityServiceState::Disabled);
        assert!(!snapshot.metrics_enabled);
        assert_eq!(snapshot.interval_secs, 15);
        assert_eq!(snapshot.cancellation_source, MemoryObservabilityCancellationSource::RuntimeToken);
        assert_eq!(snapshot.shutdown_handle, MemoryObservabilityShutdownHandle::RuntimeTokenOnly);
    }

    #[test]
    fn memory_observability_snapshot_reports_running_and_stopping_states() {
        let running = build_memory_observability_status_snapshot(true, 0, false);
        let stopping = build_memory_observability_status_snapshot(true, 30, true);

        assert_eq!(running.state, MemoryObservabilityServiceState::Running);
        assert_eq!(running.interval_secs, 1);
        assert_eq!(stopping.state, MemoryObservabilityServiceState::Stopping);
        assert_eq!(stopping.interval_secs, 30);
    }

    #[test]
    fn memory_observability_controller_reconcile_is_idempotent() {
        let controller = MemoryObservabilityController;
        let snapshot = build_memory_observability_controller_snapshot(true, 15, false);

        let first = controller.reconcile_snapshot(snapshot);
        let second = controller.reconcile_snapshot(snapshot);

        assert_eq!(first, second);
        assert_eq!(first.desired.state, MemoryObservabilityDesiredState::Enabled);
        assert_eq!(first.current_state, MemoryObservabilityServiceState::Running);
        assert_eq!(first.worker_mutation, MemoryObservabilityWorkerMutation::None);
    }

    #[test]
    fn memory_observability_controller_preserves_cancellation_state_without_worker_mutation() {
        let controller = MemoryObservabilityController;
        let snapshot = build_memory_observability_controller_snapshot(true, 30, true);
        let plan = controller.reconcile_snapshot(snapshot);

        assert_eq!(snapshot.status.state, MemoryObservabilityServiceState::Stopping);
        assert_eq!(plan.current_state, MemoryObservabilityServiceState::Stopping);
        assert_eq!(plan.worker_mutation, MemoryObservabilityWorkerMutation::None);
    }

    #[test]
    fn memory_observability_controller_reports_disabled_desired_state_without_starting_worker() {
        let controller = MemoryObservabilityController;
        let snapshot = build_memory_observability_controller_snapshot(false, 0, false);
        let plan = controller.reconcile_snapshot(snapshot);

        assert_eq!(snapshot.desired.state, MemoryObservabilityDesiredState::Disabled);
        assert_eq!(snapshot.desired.interval_secs, 1);
        assert_eq!(plan.current_state, MemoryObservabilityServiceState::Disabled);
        assert_eq!(plan.worker_mutation, MemoryObservabilityWorkerMutation::None);
    }
}
