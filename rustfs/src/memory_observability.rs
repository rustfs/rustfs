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
    AllocatorMemoryObservation, ProcessSampler, record_allocator_memory_observation, record_cgroup_memory_split,
    record_cpu_usage, record_memory_usage, record_process_memory_split,
};
use serde::Serialize;
use serde_json::Value;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use sysinfo::System;
use tokio_util::sync::CancellationToken;
use tracing::debug;

static MEMORY_SYSTEM: OnceLock<Mutex<System>> = OnceLock::new();

const ENV_MEMORY_OBSERVABILITY_INTERVAL_SECS: &str = "RUSTFS_MEMORY_OBSERVABILITY_INTERVAL_SECS";
const DEFAULT_MEMORY_OBSERVABILITY_INTERVAL_SECS: u64 = 15;
const MEMORY_OBSERVABILITY_SERVICE_NAME: &str = "memory_observability";
const CGROUP_V2_MEMORY_STAT_PATH: &str = "/sys/fs/cgroup/memory.stat";
const CGROUP_V2_MEMORY_CURRENT_PATH: &str = "/sys/fs/cgroup/memory.current";
const CGROUP_V2_MEMORY_MAX_PATH: &str = "/sys/fs/cgroup/memory.max";
const CGROUP_V1_MEMORY_STAT_PATH: &str = "/sys/fs/cgroup/memory/memory.stat";
const CGROUP_V1_MEMORY_USAGE_PATH: &str = "/sys/fs/cgroup/memory/memory.usage_in_bytes";
const CGROUP_V1_MEMORY_LIMIT_PATH: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";

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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CgroupMemoryStatFields {
    anon: Option<u64>,
    file: Option<u64>,
    active_file: Option<u64>,
    inactive_file: Option<u64>,
    rss: Option<u64>,
    cache: Option<u64>,
    total_rss: Option<u64>,
    total_cache: Option<u64>,
    total_active_file: Option<u64>,
    total_inactive_file: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct AllocatorMemorySnapshot {
    backend: &'static str,
    observation: AllocatorMemoryObservation,
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

fn parse_cgroup_memory_stat(content: &str) -> CgroupMemoryStatFields {
    let mut fields = CgroupMemoryStatFields::default();
    for line in content.lines() {
        let mut parts = line.split_whitespace();
        let Some(key) = parts.next() else {
            continue;
        };
        let Some(value) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
            continue;
        };

        match key {
            "anon" => fields.anon = Some(value),
            "file" => fields.file = Some(value),
            "active_file" => fields.active_file = Some(value),
            "inactive_file" => fields.inactive_file = Some(value),
            "rss" => fields.rss = Some(value),
            "cache" => fields.cache = Some(value),
            "total_rss" => fields.total_rss = Some(value),
            "total_cache" => fields.total_cache = Some(value),
            "total_active_file" => fields.total_active_file = Some(value),
            "total_inactive_file" => fields.total_inactive_file = Some(value),
            _ => {}
        }
    }
    fields
}

fn read_cgroup_v2() -> Option<CgroupMemorySnapshot> {
    let stats = parse_cgroup_memory_stat(&std::fs::read_to_string(CGROUP_V2_MEMORY_STAT_PATH).ok()?);
    Some(CgroupMemorySnapshot {
        current_bytes: read_optional_u64(Path::new(CGROUP_V2_MEMORY_CURRENT_PATH)),
        limit_bytes: read_optional_u64(Path::new(CGROUP_V2_MEMORY_MAX_PATH)),
        anon_bytes: stats.anon,
        file_bytes: stats.file,
        active_file_bytes: stats.active_file,
        inactive_file_bytes: stats.inactive_file,
    })
}

fn read_cgroup_v1() -> Option<CgroupMemorySnapshot> {
    let stats = parse_cgroup_memory_stat(&std::fs::read_to_string(CGROUP_V1_MEMORY_STAT_PATH).ok()?);
    Some(CgroupMemorySnapshot {
        current_bytes: read_optional_u64(Path::new(CGROUP_V1_MEMORY_USAGE_PATH)),
        limit_bytes: read_optional_u64(Path::new(CGROUP_V1_MEMORY_LIMIT_PATH)),
        anon_bytes: stats.total_rss.or(stats.rss),
        file_bytes: stats.total_cache.or(stats.cache),
        active_file_bytes: stats.total_active_file.or(stats.active_file),
        inactive_file_bytes: stats.total_inactive_file.or(stats.inactive_file),
    })
}

fn read_cgroup_memory_snapshot() -> Option<CgroupMemorySnapshot> {
    read_cgroup_v2().or_else(read_cgroup_v1)
}

fn read_allocator_memory_snapshot() -> Option<AllocatorMemorySnapshot> {
    let json = rustfs_mimalloc::MiMalloc::stats_json();
    if json.is_empty() {
        return None;
    }
    let observation = parse_mimalloc_stats_json(&json)?;
    Some(AllocatorMemorySnapshot {
        backend: crate::allocator_reclaim::allocator_backend(),
        observation,
    })
}

fn numeric_json_value(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_i64().and_then(|value| u64::try_from(value).ok())),
        Value::String(value) => value.parse::<u64>().ok(),
        _ => None,
    }
}

fn numeric_json_field(value: &Value, field: &str) -> Option<u64> {
    match value {
        Value::Object(fields) => fields
            .get(field)
            .and_then(numeric_json_value)
            .or_else(|| fields.values().find_map(|value| numeric_json_field(value, field))),
        Value::Array(values) => values.iter().find_map(|value| numeric_json_field(value, field)),
        _ => None,
    }
}

fn mimalloc_stat_field(value: &Value, metric: &str, field: &str) -> Option<u64> {
    match value {
        Value::Object(fields) => {
            if let Some(metric_value) = fields.get(metric)
                && let Some(value) = numeric_json_value(metric_value).or_else(|| numeric_json_field(metric_value, field))
            {
                return Some(value);
            }

            fields.values().find_map(|value| mimalloc_stat_field(value, metric, field))
        }
        Value::Array(values) => values.iter().find_map(|value| mimalloc_stat_field(value, metric, field)),
        _ => None,
    }
}

fn mimalloc_stat_current(value: &Value, metric: &str) -> Option<u64> {
    mimalloc_stat_field(value, metric, "current")
}

fn mimalloc_stat_sum(value: &Value, metrics: &[&str], field: &str) -> Option<u64> {
    metrics
        .iter()
        .map(|metric| mimalloc_stat_field(value, metric, field))
        .try_fold(0_u64, |sum, value| value.map(|value| sum.saturating_add(value)))
        .filter(|value| *value > 0)
}

fn parse_mimalloc_stats_json(stats_json: &str) -> Option<AllocatorMemoryObservation> {
    let value = serde_json::from_str::<Value>(stats_json).ok()?;
    let malloc_metrics = ["malloc_normal", "malloc_huge"];
    let observation = AllocatorMemoryObservation {
        reserved_bytes: mimalloc_stat_current(&value, "reserved"),
        committed_bytes: mimalloc_stat_current(&value, "committed"),
        page_committed_bytes: mimalloc_stat_current(&value, "page_committed"),
        malloc_requested_bytes: mimalloc_stat_current(&value, "malloc_requested")
            .filter(|value| *value > 0)
            .or_else(|| mimalloc_stat_sum(&value, &malloc_metrics, "current")),
        malloc_requested_peak_bytes: mimalloc_stat_field(&value, "malloc_requested", "peak")
            .filter(|value| *value > 0)
            .or_else(|| mimalloc_stat_sum(&value, &malloc_metrics, "peak")),
        malloc_requested_total_bytes: mimalloc_stat_field(&value, "malloc_requested", "total")
            .filter(|value| *value > 0)
            .or_else(|| mimalloc_stat_sum(&value, &malloc_metrics, "total")),
        heap_count: mimalloc_stat_current(&value, "heaps").or_else(|| mimalloc_stat_current(&value, "heap_count")),
    };

    if observation == AllocatorMemoryObservation::default() {
        None
    } else {
        Some(observation)
    }
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

async fn record_memory_snapshot(process_sampler: Arc<Mutex<ProcessSampler>>) {
    match tokio::task::spawn_blocking(move || {
        let mut sampler = process_sampler.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (resource, process) = sampler.snapshot_resource_and_system();
        let total_memory = refresh_total_memory();
        let cgroup = read_cgroup_memory_snapshot();
        let allocator = read_allocator_memory_snapshot();
        (resource, process, total_memory, cgroup, allocator)
    })
    .await
    {
        Ok((resource, process, total_memory, cgroup, allocator)) => {
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

            if let Some(allocator) = allocator {
                record_allocator_memory_observation(allocator.backend, allocator.observation);
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
    let process_sampler = Arc::new(Mutex::new(ProcessSampler::new()));

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
                    record_memory_snapshot(Arc::clone(&process_sampler)).await;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{
        CgroupMemorySnapshot, MEMORY_OBSERVABILITY_SERVICE_NAME, MemoryObservabilityCancellationSource,
        MemoryObservabilityController, MemoryObservabilityDesiredState, MemoryObservabilityServiceState,
        MemoryObservabilityShutdownHandle, MemoryObservabilityWorkerMutation, build_memory_observability_controller_snapshot,
        build_memory_observability_status_snapshot, parse_cgroup_memory_stat, parse_mimalloc_stats_json, read_optional_u64,
    };
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn parse_cgroup_memory_stat_extracts_tracked_numeric_fields() {
        let parsed = parse_cgroup_memory_stat("anon 12\nfile 34\nactive_file 56\nignored 78\nmalformed nope\n");
        assert_eq!(parsed.anon, Some(12));
        assert_eq!(parsed.file, Some(34));
        assert_eq!(parsed.active_file, Some(56));
        assert_eq!(parsed.inactive_file, None);
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
    fn parse_mimalloc_stats_json_extracts_allocator_attribution() {
        let parsed = parse_mimalloc_stats_json(
            r#"{
                "process": {
                    "reserved": { "current": 1048576 },
                    "committed": { "current": "524288" },
                    "page_committed": { "current": 262144 },
                    "malloc_requested": {
                        "current": 131072,
                        "peak": 196608,
                        "total": 10485760
                    },
                    "heaps": { "current": 8 }
                }
            }"#,
        )
        .expect("mimalloc stats should parse");

        assert_eq!(parsed.reserved_bytes, Some(1_048_576));
        assert_eq!(parsed.committed_bytes, Some(524_288));
        assert_eq!(parsed.page_committed_bytes, Some(262_144));
        assert_eq!(parsed.malloc_requested_bytes, Some(131_072));
        assert_eq!(parsed.malloc_requested_peak_bytes, Some(196_608));
        assert_eq!(parsed.malloc_requested_total_bytes, Some(10_485_760));
        assert_eq!(parsed.heap_count, Some(8));
    }

    #[test]
    fn parse_mimalloc_stats_json_falls_back_to_allocated_bytes_when_requested_is_zero() {
        let parsed = parse_mimalloc_stats_json(
            r#"{
                "stat_version": 1,
                "mimalloc_version": 300,
                "reserved": { "total": 1048576, "peak": 1048576, "current": 1048576 },
                "committed": { "total": 524288, "peak": 524288, "current": 524288 },
                "malloc_normal": { "total": 7340032, "peak": 262144, "current": 196608 },
                "malloc_huge": { "total": 3145728, "peak": 131072, "current": 65536 },
                "malloc_requested": { "total": 0, "peak": 0, "current": 0 },
                "heaps": { "total": 1, "peak": 1, "current": 1 }
            }"#,
        )
        .expect("mimalloc v3 stats should parse");

        assert_eq!(parsed.reserved_bytes, Some(1_048_576));
        assert_eq!(parsed.committed_bytes, Some(524_288));
        assert_eq!(parsed.malloc_requested_bytes, Some(262_144));
        assert_eq!(parsed.malloc_requested_peak_bytes, Some(393_216));
        assert_eq!(parsed.malloc_requested_total_bytes, Some(10_485_760));
        assert_eq!(parsed.heap_count, Some(1));
    }

    #[test]
    fn parse_mimalloc_stats_json_rejects_unrecognized_payload() {
        assert_eq!(parse_mimalloc_stats_json(r#"{ "allocator": "unknown" }"#), None);
    }

    #[test]
    fn read_allocator_memory_snapshot_uses_mimalloc_stats_json() {
        let snapshot = super::read_allocator_memory_snapshot();
        #[cfg(not(target_os = "windows"))]
        assert!(snapshot.is_some(), "allocator snapshot should be available on non-Windows");
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

    #[test]
    fn memory_observability_controller_harness_never_mutates_workers() {
        let controller = MemoryObservabilityController;
        let snapshots = [
            build_memory_observability_controller_snapshot(true, 15, false),
            build_memory_observability_controller_snapshot(true, 15, true),
            build_memory_observability_controller_snapshot(false, 15, false),
        ];

        for snapshot in snapshots {
            let plan = controller.reconcile_snapshot(snapshot);

            assert_eq!(plan.service, MEMORY_OBSERVABILITY_SERVICE_NAME);
            assert_eq!(plan.current_state, snapshot.status.state);
            assert_eq!(plan.worker_mutation, MemoryObservabilityWorkerMutation::None);
        }
    }
}
