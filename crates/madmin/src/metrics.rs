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

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::health::MemInfo;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimedAction {
    #[serde(rename = "count")]
    pub count: u64,
    #[serde(rename = "acc_time_ns")]
    pub acc_time: u64,
    #[serde(rename = "bytes")]
    pub bytes: u64,
}

impl TimedAction {
    pub fn merge(&mut self, other: &TimedAction) {
        self.count += other.count;
        self.acc_time += other.acc_time;
        self.bytes += other.bytes;
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DiskIOStats {
    #[serde(rename = "read_ios")]
    pub read_ios: u64,
    #[serde(rename = "read_merges")]
    pub read_merges: u64,
    #[serde(rename = "read_sectors")]
    pub read_sectors: u64,
    #[serde(rename = "read_ticks")]
    pub read_ticks: u64,
    #[serde(rename = "write_ios")]
    pub write_ios: u64,
    #[serde(rename = "write_merges")]
    pub write_merges: u64,
    #[serde(rename = "write_sectors")]
    pub write_sectors: u64,
    #[serde(rename = "write_ticks")]
    pub write_ticks: u64,
    #[serde(rename = "current_ios")]
    pub current_ios: u64,
    #[serde(rename = "total_ticks")]
    pub total_ticks: u64,
    #[serde(rename = "req_ticks")]
    pub req_ticks: u64,
    #[serde(rename = "discard_ios")]
    pub discard_ios: u64,
    #[serde(rename = "discard_merges")]
    pub discard_merges: u64,
    #[serde(rename = "discard_secotrs")]
    pub discard_sectors: u64,
    #[serde(rename = "discard_ticks")]
    pub discard_ticks: u64,
    #[serde(rename = "flush_ios")]
    pub flush_ios: u64,
    #[serde(rename = "flush_ticks")]
    pub flush_ticks: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DiskMetric {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "n_disks")]
    pub n_disks: usize,
    #[serde(rename = "offline")]
    pub offline: usize,
    #[serde(rename = "healing")]
    pub healing: usize,
    #[serde(rename = "life_time_ops")]
    pub life_time_ops: HashMap<String, u64>,
    #[serde(rename = "last_minute")]
    pub last_minute: Operations,
    #[serde(rename = "iostats")]
    pub io_stats: DiskIOStats,
}

impl DiskMetric {
    pub fn merge(&mut self, other: &DiskMetric) {
        if self.collected_at < other.collected_at {
            self.collected_at = other.collected_at;
        }
        self.n_disks += other.n_disks;
        self.offline += other.offline;
        self.healing += other.healing;

        for (k, v) in other.life_time_ops.iter() {
            *self.life_time_ops.entry(k.clone()).or_insert(0) += v;
        }

        for (k, v) in other.last_minute.operations.iter() {
            self.last_minute.operations.entry(k.clone()).or_default().merge(v);
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LastMinute {
    #[serde(rename = "actions")]
    pub actions: HashMap<String, TimedAction>,
    #[serde(rename = "ilm")]
    pub ilm: HashMap<String, TimedAction>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerSourceCycleSnapshot {
    #[serde(rename = "source", default)]
    pub source: String,
    #[serde(rename = "cycles", default)]
    pub cycles: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerCheckpointReport {
    #[serde(rename = "version", default)]
    pub version: u16,
    #[serde(rename = "resume_after", default)]
    pub resume_after: String,
    #[serde(rename = "reason", default)]
    pub reason: String,
    #[serde(rename = "last_event", default)]
    pub last_event: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerSourceWorkSnapshot {
    #[serde(rename = "source", default)]
    pub source: String,
    #[serde(rename = "checked", default)]
    pub checked: u64,
    #[serde(rename = "queued", default)]
    pub queued: u64,
    #[serde(rename = "executed", default)]
    pub executed: u64,
    #[serde(rename = "failed", default)]
    pub failed: u64,
    #[serde(rename = "skipped", default)]
    pub skipped: u64,
    #[serde(rename = "missed", default)]
    pub missed: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerMaintenanceSourceSnapshot {
    #[serde(rename = "source", default)]
    pub source: String,
    #[serde(rename = "state", default)]
    pub state: String,
    #[serde(rename = "reason", default)]
    pub reason: String,
    #[serde(rename = "backlog", default)]
    pub backlog: u64,
    #[serde(rename = "current_checked", default)]
    pub current_checked: u64,
    #[serde(rename = "current_queued", default)]
    pub current_queued: u64,
    #[serde(rename = "current_missed", default)]
    pub current_missed: u64,
    #[serde(rename = "lifetime_missed", default)]
    pub lifetime_missed: u64,
    #[serde(rename = "partial_cycles", default)]
    pub partial_cycles: u64,
}

impl ScannerMaintenanceSourceSnapshot {
    fn merge(&mut self, other: &Self) {
        if scanner_maintenance_state_priority(&other.state) > scanner_maintenance_state_priority(&self.state) {
            self.state = other.state.clone();
            self.reason = other.reason.clone();
        } else if self.reason.is_empty() || self.reason == SCANNER_MAINTENANCE_REASON_IDLE {
            self.reason = other.reason.clone();
        }

        self.backlog = self.backlog.saturating_add(other.backlog);
        self.current_checked = self.current_checked.saturating_add(other.current_checked);
        self.current_queued = self.current_queued.saturating_add(other.current_queued);
        self.current_missed = self.current_missed.saturating_add(other.current_missed);
        self.lifetime_missed = self.lifetime_missed.saturating_add(other.lifetime_missed);
        self.partial_cycles = self.partial_cycles.saturating_add(other.partial_cycles);
    }
}

const SCANNER_MAINTENANCE_CONTROL_NONE: &str = "none";
const SCANNER_MAINTENANCE_REASON_IDLE: &str = "idle";

fn scanner_maintenance_state_priority(state: &str) -> u8 {
    match state {
        "blocked" => 3,
        "deferred" => 2,
        "active" => 1,
        _ => 0,
    }
}

fn scanner_maintenance_control_priority(control: &str) -> u8 {
    match control {
        "blocked_source" => 4,
        "deferred_source" => 3,
        "active_source" => 2,
        "pacing_pressure" => 1,
        _ => 0,
    }
}

fn default_scanner_maintenance_control() -> String {
    SCANNER_MAINTENANCE_CONTROL_NONE.to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerMaintenanceControlSnapshot {
    #[serde(rename = "primary_control", default = "default_scanner_maintenance_control")]
    pub primary_control: String,
    #[serde(rename = "sources", default)]
    pub sources: Vec<ScannerMaintenanceSourceSnapshot>,
}

impl Default for ScannerMaintenanceControlSnapshot {
    fn default() -> Self {
        Self {
            primary_control: default_scanner_maintenance_control(),
            sources: Vec::new(),
        }
    }
}

impl ScannerMaintenanceControlSnapshot {
    fn merge(&mut self, other: &Self) {
        if scanner_maintenance_control_priority(&other.primary_control)
            > scanner_maintenance_control_priority(&self.primary_control)
        {
            self.primary_control = other.primary_control.clone();
        }

        for source in other.sources.iter() {
            if let Some(existing) = self.sources.iter_mut().find(|existing| existing.source == source.source) {
                existing.merge(source);
            } else {
                self.sources.push(source.clone());
            }
        }
        self.sources.sort_by(|left, right| left.source.cmp(&right.source));
    }
}

impl ScannerSourceWorkSnapshot {
    fn merge(&mut self, other: &Self) {
        self.checked = self.checked.saturating_add(other.checked);
        self.queued = self.queued.saturating_add(other.queued);
        self.executed = self.executed.saturating_add(other.executed);
        self.failed = self.failed.saturating_add(other.failed);
        self.skipped = self.skipped.saturating_add(other.skipped);
        self.missed = self.missed.saturating_add(other.missed);
    }
}

fn merge_source_work_snapshots(target: &mut Vec<ScannerSourceWorkSnapshot>, source: &[ScannerSourceWorkSnapshot]) {
    for work in source {
        if let Some(existing) = target.iter_mut().find(|existing| existing.source == work.source) {
            existing.merge(work);
        } else {
            target.push(work.clone());
        }
    }
    target.sort_by(|left, right| left.source.cmp(&right.source));
}

const SCANNER_PRIMARY_PRESSURE_NONE: &str = "none";

fn default_scanner_primary_pressure() -> String {
    SCANNER_PRIMARY_PRESSURE_NONE.to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ScannerPacingPressureSnapshot {
    #[serde(rename = "primary_pressure", default = "default_scanner_primary_pressure")]
    pub primary_pressure: String,
    #[serde(rename = "current_queued_scans", default)]
    pub current_queued_scans: u64,
    #[serde(rename = "current_active_scans", default)]
    pub current_active_scans: u64,
    #[serde(rename = "last_cycle_budget_limited", default)]
    pub last_cycle_budget_limited: bool,
    #[serde(rename = "last_cycle_pause_observed", default)]
    pub last_cycle_pause_observed: bool,
    #[serde(rename = "last_cycle_throttle_sleep_ratio", default)]
    pub last_cycle_throttle_sleep_ratio: f64,
    #[serde(rename = "last_cycle_yield_ratio", default)]
    pub last_cycle_yield_ratio: f64,
    #[serde(rename = "last_cycle_total_pause_ratio", default)]
    pub last_cycle_total_pause_ratio: f64,
}

impl Default for ScannerPacingPressureSnapshot {
    fn default() -> Self {
        Self {
            primary_pressure: default_scanner_primary_pressure(),
            current_queued_scans: 0,
            current_active_scans: 0,
            last_cycle_budget_limited: false,
            last_cycle_pause_observed: false,
            last_cycle_throttle_sleep_ratio: 0.0,
            last_cycle_yield_ratio: 0.0,
            last_cycle_total_pause_ratio: 0.0,
        }
    }
}

impl ScannerPacingPressureSnapshot {
    fn refresh_primary_pressure(&mut self) {
        self.primary_pressure = if self.current_queued_scans > 0 {
            "queued_scans"
        } else if self.last_cycle_budget_limited {
            "cycle_budget"
        } else if self.last_cycle_pause_observed {
            "throttle_pause"
        } else if self.current_active_scans > 0 {
            "active_scans"
        } else {
            SCANNER_PRIMARY_PRESSURE_NONE
        }
        .to_string();
    }

    fn merge(&mut self, other: &Self) {
        let pause_observed = self.last_cycle_pause_observed
            || self.primary_pressure == "throttle_pause"
            || other.last_cycle_pause_observed
            || other.primary_pressure == "throttle_pause";
        self.current_queued_scans = self.current_queued_scans.saturating_add(other.current_queued_scans);
        self.current_active_scans = self.current_active_scans.saturating_add(other.current_active_scans);
        self.last_cycle_budget_limited |= other.last_cycle_budget_limited;
        self.last_cycle_pause_observed = pause_observed;
        self.last_cycle_throttle_sleep_ratio = self
            .last_cycle_throttle_sleep_ratio
            .max(other.last_cycle_throttle_sleep_ratio);
        self.last_cycle_yield_ratio = self.last_cycle_yield_ratio.max(other.last_cycle_yield_ratio);
        self.last_cycle_total_pause_ratio = self.last_cycle_total_pause_ratio.max(other.last_cycle_total_pause_ratio);
        self.refresh_primary_pressure();
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerLifecycleExpirySnapshot {
    #[serde(rename = "current_queue_capacity", default)]
    pub current_queue_capacity: u64,
    #[serde(rename = "current_queued", default)]
    pub current_queued: u64,
    #[serde(rename = "current_active", default)]
    pub current_active: u64,
    #[serde(rename = "current_workers", default)]
    pub current_workers: u64,
    #[serde(rename = "queue_missed", default)]
    pub queue_missed: u64,
    #[serde(rename = "scanner_queued", default)]
    pub scanner_queued: u64,
    #[serde(rename = "scanner_missed", default)]
    pub scanner_missed: u64,
}

impl ScannerLifecycleExpirySnapshot {
    fn merge(&mut self, other: &Self) {
        self.current_queue_capacity = self.current_queue_capacity.saturating_add(other.current_queue_capacity);
        self.current_queued = self.current_queued.saturating_add(other.current_queued);
        self.current_active = self.current_active.saturating_add(other.current_active);
        self.current_workers = self.current_workers.saturating_add(other.current_workers);
        self.queue_missed = self.queue_missed.saturating_add(other.queue_missed);
        self.scanner_queued = self.scanner_queued.saturating_add(other.scanner_queued);
        self.scanner_missed = self.scanner_missed.saturating_add(other.scanner_missed);
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScannerLifecycleTransitionSnapshot {
    #[serde(rename = "current_queue_capacity", default)]
    pub current_queue_capacity: u64,
    #[serde(rename = "current_queued", default)]
    pub current_queued: u64,
    #[serde(rename = "current_active", default)]
    pub current_active: u64,
    #[serde(rename = "current_workers", default)]
    pub current_workers: u64,
    #[serde(rename = "queue_full", default)]
    pub queue_full: u64,
    #[serde(rename = "queue_send_timeout", default)]
    pub queue_send_timeout: u64,
    #[serde(rename = "compensation_scheduled", default)]
    pub compensation_scheduled: u64,
    #[serde(rename = "compensation_pending", default)]
    pub compensation_pending: u64,
    #[serde(rename = "compensation_running", default)]
    pub compensation_running: u64,
    #[serde(rename = "scanner_queued", default)]
    pub scanner_queued: u64,
    #[serde(rename = "scanner_missed", default)]
    pub scanner_missed: u64,
    #[serde(rename = "completed", default)]
    pub completed: u64,
    #[serde(rename = "failed", default)]
    pub failed: u64,
}

impl ScannerLifecycleTransitionSnapshot {
    fn merge(&mut self, other: &Self) {
        self.current_queue_capacity = self.current_queue_capacity.saturating_add(other.current_queue_capacity);
        self.current_queued = self.current_queued.saturating_add(other.current_queued);
        self.current_active = self.current_active.saturating_add(other.current_active);
        self.current_workers = self.current_workers.saturating_add(other.current_workers);
        self.queue_full = self.queue_full.saturating_add(other.queue_full);
        self.queue_send_timeout = self.queue_send_timeout.saturating_add(other.queue_send_timeout);
        self.compensation_scheduled = self.compensation_scheduled.saturating_add(other.compensation_scheduled);
        self.compensation_pending = self.compensation_pending.saturating_add(other.compensation_pending);
        self.compensation_running = self.compensation_running.saturating_add(other.compensation_running);
        self.scanner_queued = self.scanner_queued.saturating_add(other.scanner_queued);
        self.scanner_missed = self.scanner_missed.saturating_add(other.scanner_missed);
        self.completed = self.completed.saturating_add(other.completed);
        self.failed = self.failed.saturating_add(other.failed);
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScannerMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "current_cycle")]
    pub current_cycle: u64,
    #[serde(rename = "current_started")]
    pub current_started: DateTime<Utc>,
    #[serde(rename = "cycle_complete_times")]
    pub cycles_completed_at: Vec<DateTime<Utc>>,
    #[serde(rename = "ongoing_buckets")]
    pub ongoing_buckets: usize,
    #[serde(rename = "active_scan_paths", default)]
    pub active_scan_paths: usize,
    #[serde(rename = "oldest_active_path_age_seconds", default)]
    pub oldest_active_path_age_seconds: u64,
    #[serde(rename = "life_time_ops")]
    pub life_time_ops: HashMap<String, u64>,
    #[serde(rename = "ilm_ops")]
    pub life_time_ilm: HashMap<String, u64>,
    #[serde(rename = "last_minute")]
    pub last_minute: LastMinute,
    #[serde(rename = "active")]
    pub active_paths: Vec<String>,
    #[serde(rename = "current_scan_mode", default)]
    pub current_scan_mode: String,
    #[serde(rename = "current_set_scan_concurrency_limit", default)]
    pub current_set_scan_concurrency_limit: u64,
    #[serde(rename = "current_set_scans_queued", default)]
    pub current_set_scans_queued: u64,
    #[serde(rename = "current_set_scans_active", default)]
    pub current_set_scans_active: u64,
    #[serde(rename = "current_disk_scan_concurrency_limit", default)]
    pub current_disk_scan_concurrency_limit: u64,
    #[serde(rename = "current_disk_bucket_scans_queued", default)]
    pub current_disk_bucket_scans_queued: u64,
    #[serde(rename = "current_disk_bucket_scans_active", default)]
    pub current_disk_bucket_scans_active: u64,
    #[serde(rename = "current_cycle_objects_scanned", default)]
    pub current_cycle_objects_scanned: u64,
    #[serde(rename = "current_cycle_directories_scanned", default)]
    pub current_cycle_directories_scanned: u64,
    #[serde(rename = "current_cycle_bucket_drive_scans", default)]
    pub current_cycle_bucket_drive_scans: u64,
    #[serde(rename = "current_cycle_bucket_drive_failures", default)]
    pub current_cycle_bucket_drive_failures: u64,
    #[serde(rename = "current_cycle_yield_events", default)]
    pub current_cycle_yield_events: u64,
    #[serde(rename = "current_cycle_yield_duration_seconds", default)]
    pub current_cycle_yield_duration_seconds: f64,
    #[serde(rename = "current_cycle_throttle_sleep_events", default)]
    pub current_cycle_throttle_sleep_events: u64,
    #[serde(rename = "current_cycle_throttle_sleep_duration_seconds", default)]
    pub current_cycle_throttle_sleep_duration_seconds: f64,
    #[serde(rename = "current_cycle_ilm_actions", default)]
    pub current_cycle_ilm_actions: u64,
    #[serde(rename = "current_cycle_lifecycle_expiry_actions", default)]
    pub current_cycle_lifecycle_expiry_actions: u64,
    #[serde(rename = "current_cycle_lifecycle_transition_actions", default)]
    pub current_cycle_lifecycle_transition_actions: u64,
    #[serde(rename = "current_cycle_heal_objects", default)]
    pub current_cycle_heal_objects: u64,
    #[serde(rename = "current_cycle_replication_checks", default)]
    pub current_cycle_replication_checks: u64,
    #[serde(rename = "current_cycle_usage_saves", default)]
    pub current_cycle_usage_saves: u64,
    #[serde(rename = "last_cycle_result", default)]
    pub last_cycle_result: String,
    #[serde(rename = "last_cycle_result_code", default)]
    pub last_cycle_result_code: u64,
    #[serde(rename = "last_cycle_partial_reason", default)]
    pub last_cycle_partial_reason: String,
    #[serde(rename = "last_cycle_partial_reason_code", default)]
    pub last_cycle_partial_reason_code: u64,
    #[serde(rename = "last_cycle_lifecycle_expiry_actions", default)]
    pub last_cycle_lifecycle_expiry_actions: u64,
    #[serde(rename = "last_cycle_lifecycle_transition_actions", default)]
    pub last_cycle_lifecycle_transition_actions: u64,
    #[serde(rename = "last_cycle_partial_source", default)]
    pub last_cycle_partial_source: String,
    #[serde(rename = "last_cycle_partial_source_code", default)]
    pub last_cycle_partial_source_code: u64,
    #[serde(rename = "last_cycle_duration_seconds", default)]
    pub last_cycle_duration_seconds: f64,
    #[serde(rename = "last_cycle_objects_scanned", default)]
    pub last_cycle_objects_scanned: u64,
    #[serde(rename = "last_cycle_directories_scanned", default)]
    pub last_cycle_directories_scanned: u64,
    #[serde(rename = "last_cycle_bucket_drive_scans", default)]
    pub last_cycle_bucket_drive_scans: u64,
    #[serde(rename = "last_cycle_bucket_drive_failures", default)]
    pub last_cycle_bucket_drive_failures: u64,
    #[serde(rename = "last_cycle_yield_events", default)]
    pub last_cycle_yield_events: u64,
    #[serde(rename = "last_cycle_yield_duration_seconds", default)]
    pub last_cycle_yield_duration_seconds: f64,
    #[serde(rename = "last_cycle_throttle_sleep_events", default)]
    pub last_cycle_throttle_sleep_events: u64,
    #[serde(rename = "last_cycle_throttle_sleep_duration_seconds", default)]
    pub last_cycle_throttle_sleep_duration_seconds: f64,
    #[serde(rename = "last_cycle_ilm_actions", default)]
    pub last_cycle_ilm_actions: u64,
    #[serde(rename = "last_cycle_heal_objects", default)]
    pub last_cycle_heal_objects: u64,
    #[serde(rename = "last_cycle_replication_checks", default)]
    pub last_cycle_replication_checks: u64,
    #[serde(rename = "last_cycle_usage_saves", default)]
    pub last_cycle_usage_saves: u64,
    #[serde(rename = "failed_cycles", default)]
    pub failed_cycles: u64,
    #[serde(rename = "partial_cycles_unknown", default)]
    pub partial_cycles_unknown: u64,
    #[serde(rename = "partial_cycles_runtime", default)]
    pub partial_cycles_runtime: u64,
    #[serde(rename = "partial_cycles_objects", default)]
    pub partial_cycles_objects: u64,
    #[serde(rename = "partial_cycles_directories", default)]
    pub partial_cycles_directories: u64,
    #[serde(rename = "partial_cycles_by_source", default)]
    pub partial_cycles_by_source: Vec<ScannerSourceCycleSnapshot>,
    #[serde(rename = "pacing_pressure", default)]
    pub pacing_pressure: ScannerPacingPressureSnapshot,
    #[serde(rename = "lifecycle_expiry", default)]
    pub lifecycle_expiry: ScannerLifecycleExpirySnapshot,
    #[serde(rename = "lifecycle_transition", default)]
    pub lifecycle_transition: ScannerLifecycleTransitionSnapshot,
    #[serde(rename = "maintenance_control", default)]
    pub maintenance_control: ScannerMaintenanceControlSnapshot,
    #[serde(rename = "throttle_idle_mode_enabled", default)]
    pub throttle_idle_mode_enabled: bool,
    #[serde(rename = "throttle_sleep_factor", default)]
    pub throttle_sleep_factor: f64,
    #[serde(rename = "throttle_max_sleep_seconds", default)]
    pub throttle_max_sleep_seconds: f64,
    #[serde(rename = "yield_every_n_objects", default)]
    pub yield_every_n_objects: u64,
    #[serde(rename = "cycle_interval_seconds", default)]
    pub cycle_interval_seconds: f64,
    #[serde(rename = "cycle_max_duration_seconds", default)]
    pub cycle_max_duration_seconds: f64,
    #[serde(rename = "cycle_max_objects", default)]
    pub cycle_max_objects: u64,
    #[serde(rename = "cycle_max_directories", default)]
    pub cycle_max_directories: u64,
    #[serde(rename = "bitrot_cycle_enabled", default)]
    pub bitrot_cycle_enabled: bool,
    #[serde(rename = "bitrot_cycle_seconds", default)]
    pub bitrot_cycle_seconds: f64,
    #[serde(rename = "scan_checkpoint", default, skip_serializing_if = "Option::is_none")]
    pub scan_checkpoint: Option<ScannerCheckpointReport>,
    #[serde(rename = "scan_checkpoint_used", default)]
    pub scan_checkpoint_used: u64,
    #[serde(rename = "scan_checkpoint_cleared", default)]
    pub scan_checkpoint_cleared: u64,
    #[serde(rename = "scan_checkpoint_ignored", default)]
    pub scan_checkpoint_ignored: u64,
    #[serde(rename = "scan_checkpoint_stale", default)]
    pub scan_checkpoint_stale: u64,
    #[serde(rename = "source_work", default)]
    pub source_work: Vec<ScannerSourceWorkSnapshot>,
    #[serde(rename = "current_cycle_source_work", default)]
    pub current_cycle_source_work: Vec<ScannerSourceWorkSnapshot>,
    #[serde(rename = "last_cycle_source_work", default)]
    pub last_cycle_source_work: Vec<ScannerSourceWorkSnapshot>,
    #[serde(rename = "partial_cycles", default)]
    pub partial_cycles: u64,
}

impl ScannerMetrics {
    pub fn merge(&mut self, other: &Self) {
        let other_is_newer = self.collected_at < other.collected_at;
        if other_is_newer {
            self.collected_at = other.collected_at;
            self.current_scan_mode = other.current_scan_mode.clone();
            self.last_cycle_result = other.last_cycle_result.clone();
            self.last_cycle_result_code = other.last_cycle_result_code;
            self.last_cycle_partial_reason = other.last_cycle_partial_reason.clone();
            self.last_cycle_partial_reason_code = other.last_cycle_partial_reason_code;
            self.last_cycle_partial_source = other.last_cycle_partial_source.clone();
            self.last_cycle_partial_source_code = other.last_cycle_partial_source_code;
            self.last_cycle_duration_seconds = other.last_cycle_duration_seconds;
            self.throttle_idle_mode_enabled = other.throttle_idle_mode_enabled;
            self.throttle_sleep_factor = other.throttle_sleep_factor;
            self.throttle_max_sleep_seconds = other.throttle_max_sleep_seconds;
            self.yield_every_n_objects = other.yield_every_n_objects;
            self.cycle_interval_seconds = other.cycle_interval_seconds;
            self.cycle_max_duration_seconds = other.cycle_max_duration_seconds;
            self.cycle_max_objects = other.cycle_max_objects;
            self.cycle_max_directories = other.cycle_max_directories;
            self.bitrot_cycle_enabled = other.bitrot_cycle_enabled;
            self.bitrot_cycle_seconds = other.bitrot_cycle_seconds;
        }
        if other.scan_checkpoint.is_some() && (other_is_newer || self.scan_checkpoint.is_none()) {
            self.scan_checkpoint = other.scan_checkpoint.clone();
        }

        self.active_scan_paths = self.active_scan_paths.saturating_add(other.active_scan_paths);
        self.oldest_active_path_age_seconds = self.oldest_active_path_age_seconds.max(other.oldest_active_path_age_seconds);
        self.pacing_pressure.merge(&other.pacing_pressure);
        self.lifecycle_expiry.merge(&other.lifecycle_expiry);
        self.lifecycle_transition.merge(&other.lifecycle_transition);
        self.maintenance_control.merge(&other.maintenance_control);
        self.current_set_scan_concurrency_limit = self
            .current_set_scan_concurrency_limit
            .saturating_add(other.current_set_scan_concurrency_limit);
        self.current_set_scans_queued = self.current_set_scans_queued.saturating_add(other.current_set_scans_queued);
        self.current_set_scans_active = self.current_set_scans_active.saturating_add(other.current_set_scans_active);
        self.current_disk_scan_concurrency_limit = self
            .current_disk_scan_concurrency_limit
            .saturating_add(other.current_disk_scan_concurrency_limit);
        self.current_disk_bucket_scans_queued = self
            .current_disk_bucket_scans_queued
            .saturating_add(other.current_disk_bucket_scans_queued);
        self.current_disk_bucket_scans_active = self
            .current_disk_bucket_scans_active
            .saturating_add(other.current_disk_bucket_scans_active);
        self.current_cycle_objects_scanned = self
            .current_cycle_objects_scanned
            .saturating_add(other.current_cycle_objects_scanned);
        self.current_cycle_directories_scanned = self
            .current_cycle_directories_scanned
            .saturating_add(other.current_cycle_directories_scanned);
        self.current_cycle_bucket_drive_scans = self
            .current_cycle_bucket_drive_scans
            .saturating_add(other.current_cycle_bucket_drive_scans);
        self.current_cycle_bucket_drive_failures = self
            .current_cycle_bucket_drive_failures
            .saturating_add(other.current_cycle_bucket_drive_failures);
        self.current_cycle_yield_events = self
            .current_cycle_yield_events
            .saturating_add(other.current_cycle_yield_events);
        self.current_cycle_yield_duration_seconds += other.current_cycle_yield_duration_seconds;
        self.current_cycle_throttle_sleep_events = self
            .current_cycle_throttle_sleep_events
            .saturating_add(other.current_cycle_throttle_sleep_events);
        self.current_cycle_throttle_sleep_duration_seconds += other.current_cycle_throttle_sleep_duration_seconds;
        self.current_cycle_ilm_actions = self.current_cycle_ilm_actions.saturating_add(other.current_cycle_ilm_actions);
        self.current_cycle_lifecycle_expiry_actions = self
            .current_cycle_lifecycle_expiry_actions
            .saturating_add(other.current_cycle_lifecycle_expiry_actions);
        self.current_cycle_lifecycle_transition_actions = self
            .current_cycle_lifecycle_transition_actions
            .saturating_add(other.current_cycle_lifecycle_transition_actions);
        self.current_cycle_heal_objects = self
            .current_cycle_heal_objects
            .saturating_add(other.current_cycle_heal_objects);
        self.current_cycle_replication_checks = self
            .current_cycle_replication_checks
            .saturating_add(other.current_cycle_replication_checks);
        self.current_cycle_usage_saves = self.current_cycle_usage_saves.saturating_add(other.current_cycle_usage_saves);
        self.last_cycle_objects_scanned = self
            .last_cycle_objects_scanned
            .saturating_add(other.last_cycle_objects_scanned);
        self.last_cycle_directories_scanned = self
            .last_cycle_directories_scanned
            .saturating_add(other.last_cycle_directories_scanned);
        self.last_cycle_bucket_drive_scans = self
            .last_cycle_bucket_drive_scans
            .saturating_add(other.last_cycle_bucket_drive_scans);
        self.last_cycle_bucket_drive_failures = self
            .last_cycle_bucket_drive_failures
            .saturating_add(other.last_cycle_bucket_drive_failures);
        self.last_cycle_yield_events = self.last_cycle_yield_events.saturating_add(other.last_cycle_yield_events);
        self.last_cycle_yield_duration_seconds += other.last_cycle_yield_duration_seconds;
        self.last_cycle_throttle_sleep_events = self
            .last_cycle_throttle_sleep_events
            .saturating_add(other.last_cycle_throttle_sleep_events);
        self.last_cycle_throttle_sleep_duration_seconds += other.last_cycle_throttle_sleep_duration_seconds;
        self.last_cycle_ilm_actions = self.last_cycle_ilm_actions.saturating_add(other.last_cycle_ilm_actions);
        self.last_cycle_lifecycle_expiry_actions = self
            .last_cycle_lifecycle_expiry_actions
            .saturating_add(other.last_cycle_lifecycle_expiry_actions);
        self.last_cycle_lifecycle_transition_actions = self
            .last_cycle_lifecycle_transition_actions
            .saturating_add(other.last_cycle_lifecycle_transition_actions);
        self.last_cycle_heal_objects = self.last_cycle_heal_objects.saturating_add(other.last_cycle_heal_objects);
        self.last_cycle_replication_checks = self
            .last_cycle_replication_checks
            .saturating_add(other.last_cycle_replication_checks);
        self.last_cycle_usage_saves = self.last_cycle_usage_saves.saturating_add(other.last_cycle_usage_saves);
        self.failed_cycles = self.failed_cycles.saturating_add(other.failed_cycles);
        self.partial_cycles_unknown = self.partial_cycles_unknown.saturating_add(other.partial_cycles_unknown);
        self.partial_cycles_runtime = self.partial_cycles_runtime.saturating_add(other.partial_cycles_runtime);
        self.partial_cycles_objects = self.partial_cycles_objects.saturating_add(other.partial_cycles_objects);
        self.partial_cycles_directories = self
            .partial_cycles_directories
            .saturating_add(other.partial_cycles_directories);
        self.scan_checkpoint_used = self.scan_checkpoint_used.saturating_add(other.scan_checkpoint_used);
        self.scan_checkpoint_cleared = self.scan_checkpoint_cleared.saturating_add(other.scan_checkpoint_cleared);
        self.scan_checkpoint_ignored = self.scan_checkpoint_ignored.saturating_add(other.scan_checkpoint_ignored);
        self.scan_checkpoint_stale = self.scan_checkpoint_stale.saturating_add(other.scan_checkpoint_stale);
        self.partial_cycles = self.partial_cycles.saturating_add(other.partial_cycles);
        merge_source_work_snapshots(&mut self.source_work, &other.source_work);
        merge_source_work_snapshots(&mut self.current_cycle_source_work, &other.current_cycle_source_work);
        merge_source_work_snapshots(&mut self.last_cycle_source_work, &other.last_cycle_source_work);

        if self.ongoing_buckets < other.ongoing_buckets {
            self.ongoing_buckets = other.ongoing_buckets;
        }

        if self.current_cycle < other.current_cycle {
            self.current_cycle = other.current_cycle;
            self.cycles_completed_at = other.cycles_completed_at.clone();
            self.current_started = other.current_started;
        }

        if other.cycles_completed_at.len() > self.cycles_completed_at.len() {
            self.cycles_completed_at = other.cycles_completed_at.clone();
        }

        if !other.life_time_ops.is_empty() && self.life_time_ops.is_empty() {
            self.life_time_ops = other.life_time_ops.clone();
        }

        for (k, v) in other.life_time_ops.iter() {
            *self.life_time_ops.entry(k.clone()).or_default() += v;
        }

        for (k, v) in other.last_minute.actions.iter() {
            self.last_minute.actions.entry(k.clone()).or_default().merge(v);
        }

        for (k, v) in other.life_time_ilm.iter() {
            *self.life_time_ilm.entry(k.clone()).or_default() += v;
        }

        for (k, v) in other.last_minute.ilm.iter() {
            self.last_minute.ilm.entry(k.clone()).or_default().merge(v);
        }

        for source in other.partial_cycles_by_source.iter() {
            if let Some(existing) = self
                .partial_cycles_by_source
                .iter_mut()
                .find(|existing| existing.source == source.source)
            {
                existing.cycles += source.cycles;
            } else {
                self.partial_cycles_by_source.push(source.clone());
            }
        }
        self.partial_cycles_by_source
            .sort_by(|left, right| left.source.cmp(&right.source));

        self.active_paths.extend(other.active_paths.clone());

        self.active_paths.sort();
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metrics {
    #[serde(rename = "scanner", skip_serializing_if = "Option::is_none")]
    pub scanner: Option<ScannerMetrics>,
    #[serde(rename = "disk", skip_serializing_if = "Option::is_none")]
    pub disk: Option<DiskMetric>,
    #[serde(rename = "os", skip_serializing_if = "Option::is_none")]
    pub os: Option<OsMetrics>,
    #[serde(rename = "batchJobs", skip_serializing_if = "Option::is_none")]
    pub batch_jobs: Option<BatchJobMetrics>,
    #[serde(rename = "siteResync", skip_serializing_if = "Option::is_none")]
    pub site_resync: Option<SiteResyncMetrics>,
    #[serde(rename = "net", skip_serializing_if = "Option::is_none")]
    pub net: Option<NetMetrics>,
    #[serde(rename = "mem", skip_serializing_if = "Option::is_none")]
    pub mem: Option<MemMetrics>,
    #[serde(rename = "cpu", skip_serializing_if = "Option::is_none")]
    pub cpu: Option<CPUMetrics>,
    #[serde(rename = "rpc", skip_serializing_if = "Option::is_none")]
    pub rpc: Option<RPCMetrics>,
}

impl Metrics {
    pub fn merge(&mut self, other: &Self) {
        if let Some(scanner) = other.scanner.as_ref() {
            match self.scanner {
                Some(ref mut s_scanner) => s_scanner.merge(scanner),
                None => self.scanner = Some(scanner.clone()),
            }
        }

        if let Some(disk) = other.disk.as_ref() {
            match self.disk {
                Some(ref mut s_disk) => s_disk.merge(disk),
                None => self.disk = Some(disk.clone()),
            }
        }

        if let Some(os) = other.os.as_ref() {
            match self.os {
                Some(ref mut s_os) => s_os.merge(os),
                None => self.os = Some(os.clone()),
            }
        }

        if let Some(batch_jobs) = other.batch_jobs.as_ref() {
            match self.batch_jobs {
                Some(ref mut s_batch_jobs) => s_batch_jobs.merge(batch_jobs),
                None => self.batch_jobs = Some(batch_jobs.clone()),
            }
        }

        if let Some(site_resync) = other.site_resync.as_ref() {
            match self.site_resync {
                Some(ref mut s_site_resync) => s_site_resync.merge(site_resync),
                None => self.site_resync = Some(site_resync.clone()),
            }
        }

        if let Some(net) = other.net.as_ref() {
            match self.net {
                Some(ref mut s_net) => s_net.merge(net),
                None => self.net = Some(net.clone()),
            }
        }

        if let Some(rpc) = other.rpc.as_ref() {
            match self.rpc {
                Some(ref mut s_rpc) => s_rpc.merge(rpc),
                None => self.rpc = Some(rpc.clone()),
            }
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RPCMetrics {
    #[serde(rename = "collectedAt")]
    pub collected_at: DateTime<Utc>,

    pub connected: i32,

    #[serde(rename = "reconnectCount")]
    pub reconnect_count: i32,

    pub disconnected: i32,

    #[serde(rename = "outgoingStreams")]
    pub outgoing_streams: i32,

    #[serde(rename = "incomingStreams")]
    pub incoming_streams: i32,

    #[serde(rename = "outgoingBytes")]
    pub outgoing_bytes: i64,

    #[serde(rename = "incomingBytes")]
    pub incoming_bytes: i64,

    #[serde(rename = "outgoingMessages")]
    pub outgoing_messages: i64,

    #[serde(rename = "incomingMessages")]
    pub incoming_messages: i64,

    pub out_queue: i32,

    #[serde(rename = "lastPongTime")]
    pub last_pong_time: DateTime<Utc>,

    #[serde(rename = "lastPingMS")]
    pub last_ping_ms: f64,

    #[serde(rename = "maxPingDurMS")]
    pub max_ping_dur_ms: f64, // Maximum across all merged entries.

    #[serde(rename = "lastConnectTime")]
    pub last_connect_time: DateTime<Utc>,

    #[serde(rename = "byDestination", skip_serializing_if = "Option::is_none")]
    pub by_destination: Option<HashMap<String, RPCMetrics>>,

    #[serde(rename = "byCaller", skip_serializing_if = "Option::is_none")]
    pub by_caller: Option<HashMap<String, RPCMetrics>>,
}

impl RPCMetrics {
    pub fn merge(&mut self, other: &Self) {
        if self.collected_at < other.collected_at {
            self.collected_at = other.collected_at;
        }

        if self.last_connect_time < other.last_connect_time {
            self.last_connect_time = other.last_connect_time;
        }

        self.connected += other.connected;
        self.disconnected += other.disconnected;
        self.reconnect_count += other.reconnect_count;
        self.outgoing_streams += other.outgoing_streams;
        self.incoming_streams += other.incoming_streams;
        self.outgoing_bytes += other.outgoing_bytes;
        self.incoming_bytes += other.incoming_bytes;
        self.outgoing_messages += other.outgoing_messages;
        self.incoming_messages += other.incoming_messages;
        self.out_queue += other.out_queue;

        if self.last_pong_time < other.last_pong_time {
            self.last_pong_time = other.last_pong_time;
            self.last_ping_ms = other.last_ping_ms;
        }

        if self.max_ping_dur_ms < other.max_ping_dur_ms {
            self.max_ping_dur_ms = other.max_ping_dur_ms;
        }

        if let Some(by_destination) = other.by_destination.as_ref() {
            match self.by_destination.as_mut() {
                Some(s_by_de) => {
                    for (key, value) in by_destination {
                        s_by_de
                            .entry(key.to_string())
                            .and_modify(|v| v.merge(value))
                            .or_insert_with(|| value.clone());
                    }
                }
                None => self.by_destination = Some(by_destination.clone()),
            }
        }

        if let Some(by_caller) = other.by_caller.as_ref() {
            match self.by_caller.as_mut() {
                Some(s_by_caller) => {
                    for (key, value) in by_caller {
                        s_by_caller
                            .entry(key.to_string())
                            .and_modify(|v| v.merge(value))
                            .or_insert_with(|| value.clone());
                    }
                }
                None => self.by_caller = Some(by_caller.clone()),
            }
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CPUMetrics {}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NetMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "interfaceName")]
    pub interface_name: String,
    #[serde(rename = "netstats")]
    pub net_stats: NetDevLine,
}

impl NetMetrics {
    pub fn merge(&mut self, other: &Self) {
        if self.collected_at < other.collected_at {
            self.collected_at = other.collected_at;
        }

        self.net_stats.rx_bytes += other.net_stats.rx_bytes;
        self.net_stats.rx_packets += other.net_stats.rx_packets;
        self.net_stats.rx_errors += other.net_stats.rx_errors;
        self.net_stats.rx_dropped += other.net_stats.rx_dropped;
        self.net_stats.rx_fifo += other.net_stats.rx_fifo;
        self.net_stats.rx_frame += other.net_stats.rx_frame;
        self.net_stats.rx_compressed += other.net_stats.rx_compressed;
        self.net_stats.rx_multicast += other.net_stats.rx_multicast;
        self.net_stats.tx_bytes += other.net_stats.tx_bytes;
        self.net_stats.tx_packets += other.net_stats.tx_packets;
        self.net_stats.tx_errors += other.net_stats.tx_errors;
        self.net_stats.tx_dropped += other.net_stats.tx_dropped;
        self.net_stats.tx_fifo += other.net_stats.tx_fifo;
        self.net_stats.tx_collisions += other.net_stats.tx_collisions;
        self.net_stats.tx_carrier += other.net_stats.tx_carrier;
        self.net_stats.tx_compressed += other.net_stats.tx_compressed;
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NetDevLine {
    #[serde(rename = "name")]
    pub name: String, // The name of the interface.

    #[serde(rename = "rx_bytes")]
    pub rx_bytes: u64, // Cumulative count of bytes received.

    #[serde(rename = "rx_packets")]
    pub rx_packets: u64, // Cumulative count of packets received.

    #[serde(rename = "rx_errors")]
    pub rx_errors: u64, // Cumulative count of receive errors encountered.

    #[serde(rename = "rx_dropped")]
    pub rx_dropped: u64, // Cumulative count of packets dropped while receiving.

    #[serde(rename = "rx_fifo")]
    pub rx_fifo: u64, // Cumulative count of FIFO buffer errors.

    #[serde(rename = "rx_frame")]
    pub rx_frame: u64, // Cumulative count of packet framing errors.

    #[serde(rename = "rx_compressed")]
    pub rx_compressed: u64, // Cumulative count of compressed packets received by the device driver.

    #[serde(rename = "rx_multicast")]
    pub rx_multicast: u64, // Cumulative count of multicast frames received by the device driver.

    #[serde(rename = "tx_bytes")]
    pub tx_bytes: u64, // Cumulative count of bytes transmitted.

    #[serde(rename = "tx_packets")]
    pub tx_packets: u64, // Cumulative count of packets transmitted.

    #[serde(rename = "tx_errors")]
    pub tx_errors: u64, // Cumulative count of transmit errors encountered.

    #[serde(rename = "tx_dropped")]
    pub tx_dropped: u64, // Cumulative count of packets dropped while transmitting.

    #[serde(rename = "tx_fifo")]
    pub tx_fifo: u64, // Cumulative count of FIFO buffer errors.

    #[serde(rename = "tx_collisions")]
    pub tx_collisions: u64, // Cumulative count of collisions detected on the interface.

    #[serde(rename = "tx_carrier")]
    pub tx_carrier: u64, // Cumulative count of carrier losses detected by the device driver.

    #[serde(rename = "tx_compressed")]
    pub tx_compressed: u64, // Cumulative count of compressed packets transmitted by the device driver.
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "memInfo")]
    pub info: MemInfo,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SiteResyncMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "resyncStatus", skip_serializing_if = "Option::is_none")]
    pub resync_status: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: DateTime<Utc>,
    #[serde(rename = "lastUpdate")]
    pub last_update: DateTime<Utc>,
    #[serde(rename = "numBuckets")]
    pub num_buckets: i64,
    #[serde(rename = "resyncID")]
    pub resync_id: String,
    #[serde(rename = "deplID")]
    pub depl_id: String,
    #[serde(rename = "completedReplicationSize")]
    pub replicated_size: i64,
    #[serde(rename = "replicationCount")]
    pub replicated_count: i64,
    #[serde(rename = "failedReplicationSize")]
    pub failed_size: i64,
    #[serde(rename = "failedReplicationCount")]
    pub failed_count: i64,
    #[serde(rename = "failedBuckets")]
    pub failed_buckets: Vec<String>,
    #[serde(rename = "bucket", skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(rename = "object", skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
}

impl SiteResyncMetrics {
    pub fn merge(&mut self, other: &Self) {
        if self.collected_at < other.collected_at {
            *self = other.clone();
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BatchJobMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "Jobs")]
    pub jobs: HashMap<String, JobMetric>,
}

impl BatchJobMetrics {
    pub fn merge(&mut self, other: &BatchJobMetrics) {
        if other.jobs.is_empty() {
            return;
        }

        if self.collected_at < other.collected_at {
            self.collected_at = other.collected_at;
        }

        for (k, v) in other.jobs.clone().into_iter() {
            self.jobs.insert(k, v);
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct JobMetric {
    #[serde(rename = "jobID")]
    pub job_id: String,
    #[serde(rename = "jobType")]
    pub job_type: String,
    #[serde(rename = "startTime")]
    pub start_time: DateTime<Utc>,
    #[serde(rename = "lastUpdate")]
    pub last_update: DateTime<Utc>,
    #[serde(rename = "retryAttempts")]
    pub retry_attempts: i32,
    pub complete: bool,
    pub failed: bool,
    // Specific job type data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicate: Option<ReplicateInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_rotate: Option<KeyRotationInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expired: Option<ExpirationInfo>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicateInfo {
    #[serde(rename = "lastBucket")]
    pub bucket: String,
    #[serde(rename = "lastObject")]
    pub object: String,
    #[serde(rename = "objects")]
    pub objects: i64,
    #[serde(rename = "objectsFailed")]
    pub objects_failed: i64,
    #[serde(rename = "bytesTransferred")]
    pub bytes_transferred: i64,
    #[serde(rename = "bytesFailed")]
    pub bytes_failed: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExpirationInfo {
    #[serde(rename = "lastBucket")]
    pub bucket: String,
    #[serde(rename = "lastObject")]
    pub object: String,
    #[serde(rename = "objects")]
    pub objects: i64,
    #[serde(rename = "objectsFailed")]
    pub objects_failed: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct KeyRotationInfo {
    #[serde(rename = "lastBucket")]
    pub bucket: String,
    #[serde(rename = "lastObject")]
    pub object: String,
    #[serde(rename = "objects")]
    pub objects: i64,
    #[serde(rename = "objectsFailed")]
    pub objects_failed: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RealtimeMetrics {
    #[serde(rename = "errors")]
    pub errors: Vec<String>,
    #[serde(rename = "hosts")]
    pub hosts: Vec<String>,
    #[serde(rename = "aggregated")]
    pub aggregated: Metrics,
    #[serde(rename = "by_host")]
    pub by_host: HashMap<String, Metrics>,
    #[serde(rename = "by_disk")]
    pub by_disk: HashMap<String, DiskMetric>,
    #[serde(rename = "final")]
    pub finally: bool,
}

impl RealtimeMetrics {
    pub fn merge(&mut self, other: Self) {
        if !other.errors.is_empty() {
            self.errors.extend(other.errors);
        }

        for (k, v) in other.by_host.into_iter() {
            *self.by_host.entry(k).or_default() = v;
        }

        self.hosts.extend(other.hosts);
        self.aggregated.merge(&other.aggregated);
        self.hosts.sort();

        for (k, v) in other.by_disk.into_iter() {
            self.by_disk.entry(k.to_string()).and_modify(|h| *h = v.clone()).or_insert(v);
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OsMetrics {
    #[serde(rename = "collected")]
    pub collected_at: DateTime<Utc>,
    #[serde(rename = "life_time_ops")]
    pub life_time_ops: HashMap<String, u64>,
    #[serde(rename = "last_minute")]
    pub last_minute: Operations,
}

impl OsMetrics {
    pub fn merge(&mut self, other: &Self) {
        if self.collected_at < other.collected_at {
            self.collected_at = other.collected_at;
        }

        for (k, v) in other.life_time_ops.iter() {
            *self.life_time_ops.entry(k.clone()).or_default() += v;
        }

        for (k, v) in other.last_minute.operations.iter() {
            self.last_minute.operations.entry(k.clone()).or_default().merge(v);
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Operations {
    #[serde(rename = "operations")]
    pub operations: HashMap<String, TimedAction>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scanner_metrics_merge_aggregates_partial_cycles_by_source() {
        let collected_at = Utc::now();
        let mut scanner = ScannerMetrics {
            collected_at,
            last_cycle_partial_source: "usage".to_string(),
            last_cycle_partial_source_code: 1,
            pacing_pressure: ScannerPacingPressureSnapshot {
                primary_pressure: "cycle_budget".to_string(),
                current_active_scans: 1,
                last_cycle_budget_limited: true,
                last_cycle_pause_observed: true,
                last_cycle_throttle_sleep_ratio: 0.1,
                last_cycle_yield_ratio: 0.2,
                last_cycle_total_pause_ratio: 0.3,
                ..Default::default()
            },
            partial_cycles_by_source: vec![ScannerSourceCycleSnapshot {
                source: "usage".to_string(),
                cycles: 1,
            }],
            ..Default::default()
        };

        scanner.merge(&ScannerMetrics {
            collected_at: collected_at + chrono::Duration::seconds(1),
            last_cycle_partial_source: "lifecycle".to_string(),
            last_cycle_partial_source_code: 2,
            pacing_pressure: ScannerPacingPressureSnapshot {
                primary_pressure: "queued_scans".to_string(),
                current_queued_scans: 4,
                current_active_scans: 2,
                last_cycle_throttle_sleep_ratio: 0.4,
                last_cycle_yield_ratio: 0.1,
                last_cycle_total_pause_ratio: 0.5,
                ..Default::default()
            },
            partial_cycles_by_source: vec![
                ScannerSourceCycleSnapshot {
                    source: "usage".to_string(),
                    cycles: 2,
                },
                ScannerSourceCycleSnapshot {
                    source: "lifecycle".to_string(),
                    cycles: 1,
                },
            ],
            ..Default::default()
        });

        assert_eq!(scanner.last_cycle_partial_source, "lifecycle");
        assert_eq!(scanner.last_cycle_partial_source_code, 2);
        assert_eq!(scanner.pacing_pressure.primary_pressure, "queued_scans");
        assert_eq!(scanner.pacing_pressure.current_queued_scans, 4);
        assert_eq!(scanner.pacing_pressure.current_active_scans, 3);
        assert!(scanner.pacing_pressure.last_cycle_budget_limited);
        assert!(scanner.pacing_pressure.last_cycle_pause_observed);
        assert_eq!(scanner.pacing_pressure.last_cycle_throttle_sleep_ratio, 0.4);
        assert_eq!(scanner.pacing_pressure.last_cycle_yield_ratio, 0.2);
        assert_eq!(scanner.pacing_pressure.last_cycle_total_pause_ratio, 0.5);
        assert_eq!(
            scanner.partial_cycles_by_source,
            vec![
                ScannerSourceCycleSnapshot {
                    source: "lifecycle".to_string(),
                    cycles: 1,
                },
                ScannerSourceCycleSnapshot {
                    source: "usage".to_string(),
                    cycles: 3,
                },
            ]
        );
    }

    #[test]
    fn scanner_metrics_merge_preserves_pause_pressure_without_duration() {
        let collected_at = Utc::now();
        let mut scanner = ScannerMetrics {
            collected_at,
            pacing_pressure: ScannerPacingPressureSnapshot {
                primary_pressure: "throttle_pause".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        scanner.merge(&ScannerMetrics {
            collected_at: collected_at + chrono::Duration::seconds(1),
            pacing_pressure: ScannerPacingPressureSnapshot::default(),
            ..Default::default()
        });

        assert_eq!(scanner.pacing_pressure.primary_pressure, "throttle_pause");
        assert!(scanner.pacing_pressure.last_cycle_pause_observed);
        assert_eq!(scanner.pacing_pressure.last_cycle_total_pause_ratio, 0.0);
    }

    #[test]
    fn scanner_metrics_deserializes_missing_primary_pressure_as_none() {
        let pacing_pressure: ScannerPacingPressureSnapshot = serde_json::from_str(
            r#"{
                "current_active_scans": 1
            }"#,
        )
        .expect("deserialize partial scanner pacing pressure");

        assert_eq!(pacing_pressure.primary_pressure, "none");
        assert_eq!(pacing_pressure.current_active_scans, 1);
    }

    #[test]
    fn scanner_metrics_merge_aggregates_lifecycle_transition_status() {
        let collected_at = Utc::now();
        let mut scanner = ScannerMetrics {
            collected_at,
            current_cycle_lifecycle_expiry_actions: 2,
            current_cycle_lifecycle_transition_actions: 3,
            last_cycle_lifecycle_expiry_actions: 5,
            last_cycle_lifecycle_transition_actions: 7,
            lifecycle_expiry: ScannerLifecycleExpirySnapshot {
                current_queue_capacity: 8,
                current_queued: 2,
                current_active: 1,
                current_workers: 2,
                queue_missed: 3,
                scanner_queued: 5,
                scanner_missed: 2,
            },
            lifecycle_transition: ScannerLifecycleTransitionSnapshot {
                current_queue_capacity: 8,
                current_queued: 2,
                current_active: 1,
                current_workers: 2,
                queue_full: 3,
                queue_send_timeout: 1,
                compensation_scheduled: 1,
                compensation_pending: 2,
                compensation_running: 1,
                scanner_queued: 5,
                scanner_missed: 2,
                completed: 7,
                failed: 1,
            },
            ..Default::default()
        };

        scanner.merge(&ScannerMetrics {
            collected_at: collected_at + chrono::Duration::seconds(1),
            current_cycle_lifecycle_expiry_actions: 11,
            current_cycle_lifecycle_transition_actions: 13,
            last_cycle_lifecycle_expiry_actions: 17,
            last_cycle_lifecycle_transition_actions: 19,
            lifecycle_expiry: ScannerLifecycleExpirySnapshot {
                current_queue_capacity: 4,
                current_queued: 3,
                current_active: 2,
                current_workers: 1,
                queue_missed: 2,
                scanner_queued: 6,
                scanner_missed: 4,
            },
            lifecycle_transition: ScannerLifecycleTransitionSnapshot {
                current_queue_capacity: 4,
                current_queued: 3,
                current_active: 2,
                current_workers: 1,
                queue_full: 2,
                queue_send_timeout: 3,
                compensation_scheduled: 4,
                compensation_pending: 3,
                compensation_running: 0,
                scanner_queued: 6,
                scanner_missed: 4,
                completed: 8,
                failed: 2,
            },
            ..Default::default()
        });

        assert_eq!(scanner.lifecycle_expiry.current_queue_capacity, 12);
        assert_eq!(scanner.lifecycle_expiry.current_queued, 5);
        assert_eq!(scanner.lifecycle_expiry.current_active, 3);
        assert_eq!(scanner.lifecycle_expiry.current_workers, 3);
        assert_eq!(scanner.lifecycle_expiry.queue_missed, 5);
        assert_eq!(scanner.lifecycle_expiry.scanner_queued, 11);
        assert_eq!(scanner.lifecycle_expiry.scanner_missed, 6);
        assert_eq!(scanner.lifecycle_transition.current_queue_capacity, 12);
        assert_eq!(scanner.lifecycle_transition.current_queued, 5);
        assert_eq!(scanner.lifecycle_transition.current_active, 3);
        assert_eq!(scanner.lifecycle_transition.current_workers, 3);
        assert_eq!(scanner.lifecycle_transition.queue_full, 5);
        assert_eq!(scanner.lifecycle_transition.queue_send_timeout, 4);
        assert_eq!(scanner.lifecycle_transition.compensation_scheduled, 5);
        assert_eq!(scanner.lifecycle_transition.compensation_pending, 5);
        assert_eq!(scanner.lifecycle_transition.compensation_running, 1);
        assert_eq!(scanner.lifecycle_transition.scanner_queued, 11);
        assert_eq!(scanner.lifecycle_transition.scanner_missed, 6);
        assert_eq!(scanner.lifecycle_transition.completed, 15);
        assert_eq!(scanner.lifecycle_transition.failed, 3);
        assert_eq!(scanner.current_cycle_lifecycle_expiry_actions, 13);
        assert_eq!(scanner.current_cycle_lifecycle_transition_actions, 16);
        assert_eq!(scanner.last_cycle_lifecycle_expiry_actions, 22);
        assert_eq!(scanner.last_cycle_lifecycle_transition_actions, 26);
    }

    #[test]
    fn scanner_metrics_merge_aggregates_maintenance_control_status() {
        let collected_at = Utc::now();
        let mut scanner = ScannerMetrics {
            collected_at,
            maintenance_control: ScannerMaintenanceControlSnapshot {
                primary_control: "active_source".to_string(),
                sources: vec![ScannerMaintenanceSourceSnapshot {
                    source: "usage".to_string(),
                    state: "active".to_string(),
                    reason: "active_work".to_string(),
                    backlog: 1,
                    current_checked: 2,
                    current_queued: 0,
                    current_missed: 0,
                    lifetime_missed: 0,
                    partial_cycles: 0,
                }],
            },
            ..Default::default()
        };

        scanner.merge(&ScannerMetrics {
            collected_at: collected_at + chrono::Duration::seconds(1),
            maintenance_control: ScannerMaintenanceControlSnapshot {
                primary_control: "blocked_source".to_string(),
                sources: vec![
                    ScannerMaintenanceSourceSnapshot {
                        source: "usage".to_string(),
                        state: "deferred".to_string(),
                        reason: "partial_cycle".to_string(),
                        backlog: 3,
                        current_checked: 5,
                        current_queued: 1,
                        current_missed: 0,
                        lifetime_missed: 0,
                        partial_cycles: 2,
                    },
                    ScannerMaintenanceSourceSnapshot {
                        source: "lifecycle".to_string(),
                        state: "blocked".to_string(),
                        reason: "missed_work".to_string(),
                        backlog: 4,
                        current_checked: 0,
                        current_queued: 0,
                        current_missed: 4,
                        lifetime_missed: 9,
                        partial_cycles: 1,
                    },
                ],
            },
            ..Default::default()
        });

        assert_eq!(scanner.maintenance_control.primary_control, "blocked_source");
        let lifecycle = scanner
            .maintenance_control
            .sources
            .iter()
            .find(|source| source.source == "lifecycle")
            .expect("lifecycle maintenance control should be present");
        assert_eq!(lifecycle.state, "blocked");
        assert_eq!(lifecycle.reason, "missed_work");
        assert_eq!(lifecycle.backlog, 4);
        assert_eq!(lifecycle.current_missed, 4);
        assert_eq!(lifecycle.lifetime_missed, 9);

        let usage = scanner
            .maintenance_control
            .sources
            .iter()
            .find(|source| source.source == "usage")
            .expect("usage maintenance control should be present");
        assert_eq!(usage.state, "deferred");
        assert_eq!(usage.reason, "partial_cycle");
        assert_eq!(usage.backlog, 4);
        assert_eq!(usage.current_checked, 7);
        assert_eq!(usage.current_queued, 1);
        assert_eq!(usage.partial_cycles, 2);
    }

    #[test]
    fn scanner_metrics_merge_preserves_distributed_status_fields() {
        let collected_at = Utc::now();
        let mut scanner = ScannerMetrics {
            collected_at,
            active_scan_paths: 1,
            oldest_active_path_age_seconds: 15,
            active_paths: vec!["node-a/disk-a/bucket-a".to_string()],
            current_set_scan_concurrency_limit: 1,
            current_set_scans_queued: 2,
            current_set_scans_active: 3,
            current_disk_scan_concurrency_limit: 4,
            current_disk_bucket_scans_queued: 5,
            current_disk_bucket_scans_active: 6,
            current_cycle_objects_scanned: 7,
            current_cycle_directories_scanned: 8,
            current_cycle_bucket_drive_scans: 9,
            current_cycle_bucket_drive_failures: 1,
            current_cycle_yield_events: 2,
            current_cycle_yield_duration_seconds: 0.5,
            current_cycle_throttle_sleep_events: 3,
            current_cycle_throttle_sleep_duration_seconds: 1.5,
            current_cycle_ilm_actions: 4,
            current_cycle_heal_objects: 5,
            current_cycle_replication_checks: 6,
            current_cycle_usage_saves: 7,
            last_cycle_objects_scanned: 8,
            last_cycle_directories_scanned: 9,
            last_cycle_bucket_drive_scans: 10,
            last_cycle_bucket_drive_failures: 2,
            last_cycle_yield_events: 3,
            last_cycle_yield_duration_seconds: 0.75,
            last_cycle_throttle_sleep_events: 4,
            last_cycle_throttle_sleep_duration_seconds: 1.75,
            last_cycle_ilm_actions: 5,
            last_cycle_heal_objects: 6,
            last_cycle_replication_checks: 7,
            last_cycle_usage_saves: 8,
            failed_cycles: 1,
            partial_cycles_unknown: 2,
            partial_cycles_runtime: 3,
            partial_cycles_objects: 4,
            partial_cycles_directories: 5,
            scan_checkpoint: Some(ScannerCheckpointReport {
                version: 1,
                resume_after: "bucket-a/prefix-a".to_string(),
                reason: "directories".to_string(),
                last_event: "used".to_string(),
            }),
            scan_checkpoint_used: 1,
            scan_checkpoint_cleared: 2,
            scan_checkpoint_ignored: 3,
            scan_checkpoint_stale: 4,
            source_work: vec![ScannerSourceWorkSnapshot {
                source: "usage".to_string(),
                checked: 10,
                executed: 1,
                ..Default::default()
            }],
            current_cycle_source_work: vec![ScannerSourceWorkSnapshot {
                source: "lifecycle".to_string(),
                queued: 2,
                missed: 1,
                ..Default::default()
            }],
            last_cycle_source_work: vec![ScannerSourceWorkSnapshot {
                source: "heal".to_string(),
                skipped: 3,
                ..Default::default()
            }],
            partial_cycles: 6,
            ..Default::default()
        };

        scanner.merge(&ScannerMetrics {
            collected_at: collected_at + chrono::Duration::seconds(1),
            active_scan_paths: 2,
            oldest_active_path_age_seconds: 45,
            active_paths: vec!["node-b/disk-b/bucket-b".to_string()],
            current_set_scan_concurrency_limit: 10,
            current_set_scans_queued: 20,
            current_set_scans_active: 30,
            current_disk_scan_concurrency_limit: 40,
            current_disk_bucket_scans_queued: 50,
            current_disk_bucket_scans_active: 60,
            current_cycle_objects_scanned: 70,
            current_cycle_directories_scanned: 80,
            current_cycle_bucket_drive_scans: 90,
            current_cycle_bucket_drive_failures: 10,
            current_cycle_yield_events: 20,
            current_cycle_yield_duration_seconds: 2.5,
            current_cycle_throttle_sleep_events: 30,
            current_cycle_throttle_sleep_duration_seconds: 3.5,
            current_cycle_ilm_actions: 40,
            current_cycle_heal_objects: 50,
            current_cycle_replication_checks: 60,
            current_cycle_usage_saves: 70,
            last_cycle_objects_scanned: 80,
            last_cycle_directories_scanned: 90,
            last_cycle_bucket_drive_scans: 100,
            last_cycle_bucket_drive_failures: 20,
            last_cycle_yield_events: 30,
            last_cycle_yield_duration_seconds: 2.75,
            last_cycle_throttle_sleep_events: 40,
            last_cycle_throttle_sleep_duration_seconds: 3.75,
            last_cycle_ilm_actions: 50,
            last_cycle_heal_objects: 60,
            last_cycle_replication_checks: 70,
            last_cycle_usage_saves: 80,
            failed_cycles: 10,
            partial_cycles_unknown: 20,
            partial_cycles_runtime: 30,
            partial_cycles_objects: 40,
            partial_cycles_directories: 50,
            scan_checkpoint: Some(ScannerCheckpointReport {
                version: 1,
                resume_after: "bucket-b/prefix-b".to_string(),
                reason: "objects".to_string(),
                last_event: "set".to_string(),
            }),
            scan_checkpoint_used: 10,
            scan_checkpoint_cleared: 20,
            scan_checkpoint_ignored: 30,
            scan_checkpoint_stale: 40,
            source_work: vec![
                ScannerSourceWorkSnapshot {
                    source: "usage".to_string(),
                    checked: 5,
                    executed: 2,
                    ..Default::default()
                },
                ScannerSourceWorkSnapshot {
                    source: "replication".to_string(),
                    queued: 7,
                    missed: 2,
                    ..Default::default()
                },
            ],
            current_cycle_source_work: vec![ScannerSourceWorkSnapshot {
                source: "lifecycle".to_string(),
                queued: 3,
                missed: 4,
                ..Default::default()
            }],
            last_cycle_source_work: vec![ScannerSourceWorkSnapshot {
                source: "heal".to_string(),
                skipped: 4,
                missed: 5,
                ..Default::default()
            }],
            partial_cycles: 60,
            ..Default::default()
        });

        assert_eq!(scanner.active_scan_paths, 3);
        assert_eq!(scanner.oldest_active_path_age_seconds, 45);
        assert_eq!(
            scanner.active_paths,
            vec!["node-a/disk-a/bucket-a".to_string(), "node-b/disk-b/bucket-b".to_string()]
        );
        assert_eq!(scanner.current_set_scan_concurrency_limit, 11);
        assert_eq!(scanner.current_set_scans_queued, 22);
        assert_eq!(scanner.current_set_scans_active, 33);
        assert_eq!(scanner.current_disk_scan_concurrency_limit, 44);
        assert_eq!(scanner.current_disk_bucket_scans_queued, 55);
        assert_eq!(scanner.current_disk_bucket_scans_active, 66);
        assert_eq!(scanner.current_cycle_objects_scanned, 77);
        assert_eq!(scanner.current_cycle_directories_scanned, 88);
        assert_eq!(scanner.current_cycle_bucket_drive_scans, 99);
        assert_eq!(scanner.current_cycle_bucket_drive_failures, 11);
        assert_eq!(scanner.current_cycle_yield_events, 22);
        assert_eq!(scanner.current_cycle_yield_duration_seconds, 3.0);
        assert_eq!(scanner.current_cycle_throttle_sleep_events, 33);
        assert_eq!(scanner.current_cycle_throttle_sleep_duration_seconds, 5.0);
        assert_eq!(scanner.current_cycle_ilm_actions, 44);
        assert_eq!(scanner.current_cycle_heal_objects, 55);
        assert_eq!(scanner.current_cycle_replication_checks, 66);
        assert_eq!(scanner.current_cycle_usage_saves, 77);
        assert_eq!(scanner.last_cycle_objects_scanned, 88);
        assert_eq!(scanner.last_cycle_directories_scanned, 99);
        assert_eq!(scanner.last_cycle_bucket_drive_scans, 110);
        assert_eq!(scanner.last_cycle_bucket_drive_failures, 22);
        assert_eq!(scanner.last_cycle_yield_events, 33);
        assert_eq!(scanner.last_cycle_yield_duration_seconds, 3.5);
        assert_eq!(scanner.last_cycle_throttle_sleep_events, 44);
        assert_eq!(scanner.last_cycle_throttle_sleep_duration_seconds, 5.5);
        assert_eq!(scanner.last_cycle_ilm_actions, 55);
        assert_eq!(scanner.last_cycle_heal_objects, 66);
        assert_eq!(scanner.last_cycle_replication_checks, 77);
        assert_eq!(scanner.last_cycle_usage_saves, 88);
        assert_eq!(scanner.failed_cycles, 11);
        assert_eq!(scanner.partial_cycles_unknown, 22);
        assert_eq!(scanner.partial_cycles_runtime, 33);
        assert_eq!(scanner.partial_cycles_objects, 44);
        assert_eq!(scanner.partial_cycles_directories, 55);
        let checkpoint = scanner.scan_checkpoint.expect("newest checkpoint should be preserved");
        assert_eq!(checkpoint.resume_after, "bucket-b/prefix-b");
        assert_eq!(scanner.scan_checkpoint_used, 11);
        assert_eq!(scanner.scan_checkpoint_cleared, 22);
        assert_eq!(scanner.scan_checkpoint_ignored, 33);
        assert_eq!(scanner.scan_checkpoint_stale, 44);
        assert_eq!(scanner.partial_cycles, 66);

        let usage = scanner
            .source_work
            .iter()
            .find(|work| work.source == "usage")
            .expect("usage work should be merged");
        assert_eq!(usage.checked, 15);
        assert_eq!(usage.executed, 3);
        let replication = scanner
            .source_work
            .iter()
            .find(|work| work.source == "replication")
            .expect("replication work should be preserved");
        assert_eq!(replication.queued, 7);
        assert_eq!(replication.missed, 2);
        let lifecycle = scanner
            .current_cycle_source_work
            .iter()
            .find(|work| work.source == "lifecycle")
            .expect("current cycle lifecycle work should be merged");
        assert_eq!(lifecycle.queued, 5);
        assert_eq!(lifecycle.missed, 5);
        let heal = scanner
            .last_cycle_source_work
            .iter()
            .find(|work| work.source == "heal")
            .expect("last cycle heal work should be merged");
        assert_eq!(heal.skipped, 7);
        assert_eq!(heal.missed, 5);
    }
}
