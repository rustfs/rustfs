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

use crate::admin_server_info::get_local_server_property;
use crate::global::resolve_object_store_handle;
use chrono::Utc;
use rustfs_common::{GLOBAL_LOCAL_NODE_NAME, GLOBAL_RUSTFS_ADDR, heal_channel::DriveState, metrics::global_metrics};
use rustfs_io_metrics::internode_metrics::global_internode_metrics;
use rustfs_madmin::metrics::{
    DiskIOStats, DiskMetric, LastMinute as MadminLastMinute, NetDevLine, NetMetrics, RPCMetrics, RealtimeMetrics,
    ScannerCheckpointReport as MadminScannerCheckpointReport,
    ScannerLifecycleExpirySnapshot as MadminScannerLifecycleExpirySnapshot,
    ScannerLifecycleTransitionSnapshot as MadminScannerLifecycleTransitionSnapshot,
    ScannerMaintenanceControlSnapshot as MadminScannerMaintenanceControlSnapshot,
    ScannerMaintenanceSourceSnapshot as MadminScannerMaintenanceSourceSnapshot, ScannerMetrics as MadminScannerMetrics,
    ScannerPacingPressureSnapshot as MadminScannerPacingPressureSnapshot,
    ScannerReplicationRepairSnapshot as MadminScannerReplicationRepairSnapshot,
    ScannerSourceCycleSnapshot as MadminScannerSourceCycleSnapshot, ScannerSourceWorkSnapshot as MadminScannerSourceWorkSnapshot,
    ScannerUsageFreshnessSnapshot as MadminScannerUsageFreshnessSnapshot, TimedAction as MadminTimedAction,
};
use rustfs_storage_api::StorageAdminApi;
use rustfs_utils::os::get_drive_stats;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CollectMetricsOpts {
    pub hosts: HashSet<String>,
    pub disks: HashSet<String>,
    pub job_id: String,
    pub dep_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MetricType(u32);

impl MetricType {
    // Define some constants
    pub const NONE: MetricType = MetricType(0);
    pub const SCANNER: MetricType = MetricType(1 << 0);
    pub const DISK: MetricType = MetricType(1 << 1);
    pub const OS: MetricType = MetricType(1 << 2);
    pub const BATCH_JOBS: MetricType = MetricType(1 << 3);
    pub const SITE_RESYNC: MetricType = MetricType(1 << 4);
    pub const NET: MetricType = MetricType(1 << 5);
    pub const MEM: MetricType = MetricType(1 << 6);
    pub const CPU: MetricType = MetricType(1 << 7);
    pub const RPC: MetricType = MetricType(1 << 8);

    // MetricsAll must be last.
    pub const ALL: MetricType = MetricType((1 << 9) - 1);

    pub fn new(t: u32) -> Self {
        Self(t)
    }
}

fn to_madmin_scanner_metrics(metrics: rustfs_common::metrics::ScannerMetricsReport) -> MadminScannerMetrics {
    MadminScannerMetrics {
        collected_at: metrics.collected_at,
        current_cycle: metrics.current_cycle,
        current_started: metrics.current_started,
        cycles_completed_at: metrics.cycles_completed_at,
        ongoing_buckets: metrics.ongoing_buckets,
        active_scan_paths: metrics.active_scan_paths,
        oldest_active_path_age_seconds: metrics.oldest_active_path_age_seconds,
        life_time_ops: metrics.life_time_ops,
        life_time_ilm: metrics.life_time_ilm,
        last_minute: MadminLastMinute {
            actions: metrics
                .last_minute
                .actions
                .into_iter()
                .map(|(key, value)| {
                    (
                        key,
                        MadminTimedAction {
                            count: value.count,
                            acc_time: value.acc_time,
                            bytes: value.bytes,
                        },
                    )
                })
                .collect(),
            ilm: metrics
                .last_minute
                .ilm
                .into_iter()
                .map(|(key, value)| {
                    (
                        key,
                        MadminTimedAction {
                            count: value.count,
                            acc_time: value.acc_time,
                            bytes: value.bytes,
                        },
                    )
                })
                .collect(),
        },
        active_paths: metrics.active_paths,
        current_scan_mode: metrics.current_scan_mode,
        current_set_scan_concurrency_limit: metrics.current_set_scan_concurrency_limit,
        current_set_scans_queued: metrics.current_set_scans_queued,
        current_set_scans_active: metrics.current_set_scans_active,
        current_disk_scan_concurrency_limit: metrics.current_disk_scan_concurrency_limit,
        current_disk_bucket_scans_queued: metrics.current_disk_bucket_scans_queued,
        current_disk_bucket_scans_active: metrics.current_disk_bucket_scans_active,
        current_cycle_objects_scanned: metrics.current_cycle_objects_scanned,
        current_cycle_directories_scanned: metrics.current_cycle_directories_scanned,
        current_cycle_bucket_drive_scans: metrics.current_cycle_bucket_drive_scans,
        current_cycle_bucket_drive_failures: metrics.current_cycle_bucket_drive_failures,
        current_cycle_yield_events: metrics.current_cycle_yield_events,
        current_cycle_yield_duration_seconds: metrics.current_cycle_yield_duration_seconds,
        current_cycle_throttle_sleep_events: metrics.current_cycle_throttle_sleep_events,
        current_cycle_throttle_sleep_duration_seconds: metrics.current_cycle_throttle_sleep_duration_seconds,
        current_cycle_ilm_actions: metrics.current_cycle_ilm_actions,
        current_cycle_lifecycle_expiry_actions: metrics.current_cycle_lifecycle_expiry_actions,
        current_cycle_lifecycle_transition_actions: metrics.current_cycle_lifecycle_transition_actions,
        current_cycle_heal_objects: metrics.current_cycle_heal_objects,
        current_cycle_replication_checks: metrics.current_cycle_replication_checks,
        current_cycle_usage_saves: metrics.current_cycle_usage_saves,
        last_cycle_result: metrics.last_cycle_result,
        last_cycle_result_code: metrics.last_cycle_result_code,
        last_cycle_partial_reason: metrics.last_cycle_partial_reason,
        last_cycle_partial_reason_code: metrics.last_cycle_partial_reason_code,
        last_cycle_lifecycle_expiry_actions: metrics.last_cycle_lifecycle_expiry_actions,
        last_cycle_lifecycle_transition_actions: metrics.last_cycle_lifecycle_transition_actions,
        last_cycle_partial_source: metrics.last_cycle_partial_source,
        last_cycle_partial_source_code: metrics.last_cycle_partial_source_code,
        last_cycle_duration_seconds: metrics.last_cycle_duration_seconds,
        last_cycle_objects_scanned: metrics.last_cycle_objects_scanned,
        last_cycle_directories_scanned: metrics.last_cycle_directories_scanned,
        last_cycle_bucket_drive_scans: metrics.last_cycle_bucket_drive_scans,
        last_cycle_bucket_drive_failures: metrics.last_cycle_bucket_drive_failures,
        last_cycle_yield_events: metrics.last_cycle_yield_events,
        last_cycle_yield_duration_seconds: metrics.last_cycle_yield_duration_seconds,
        last_cycle_throttle_sleep_events: metrics.last_cycle_throttle_sleep_events,
        last_cycle_throttle_sleep_duration_seconds: metrics.last_cycle_throttle_sleep_duration_seconds,
        last_cycle_ilm_actions: metrics.last_cycle_ilm_actions,
        last_cycle_heal_objects: metrics.last_cycle_heal_objects,
        last_cycle_replication_checks: metrics.last_cycle_replication_checks,
        last_cycle_usage_saves: metrics.last_cycle_usage_saves,
        failed_cycles: metrics.failed_cycles,
        partial_cycles_unknown: metrics.partial_cycles_unknown,
        partial_cycles_runtime: metrics.partial_cycles_runtime,
        partial_cycles_objects: metrics.partial_cycles_objects,
        partial_cycles_directories: metrics.partial_cycles_directories,
        pacing_pressure: MadminScannerPacingPressureSnapshot {
            primary_pressure: metrics.pacing_pressure.primary_pressure,
            current_queued_scans: metrics.pacing_pressure.current_queued_scans,
            current_active_scans: metrics.pacing_pressure.current_active_scans,
            last_cycle_budget_limited: metrics.pacing_pressure.last_cycle_budget_limited,
            last_cycle_pause_observed: metrics.pacing_pressure.last_cycle_pause_observed,
            last_cycle_throttle_sleep_ratio: metrics.pacing_pressure.last_cycle_throttle_sleep_ratio,
            last_cycle_yield_ratio: metrics.pacing_pressure.last_cycle_yield_ratio,
            last_cycle_total_pause_ratio: metrics.pacing_pressure.last_cycle_total_pause_ratio,
        },
        lifecycle_transition: MadminScannerLifecycleTransitionSnapshot {
            current_queue_capacity: metrics.lifecycle_transition.current_queue_capacity,
            current_queued: metrics.lifecycle_transition.current_queued,
            current_active: metrics.lifecycle_transition.current_active,
            current_workers: metrics.lifecycle_transition.current_workers,
            queue_full: metrics.lifecycle_transition.queue_full,
            queue_send_timeout: metrics.lifecycle_transition.queue_send_timeout,
            compensation_scheduled: metrics.lifecycle_transition.compensation_scheduled,
            compensation_pending: metrics.lifecycle_transition.compensation_pending,
            compensation_running: metrics.lifecycle_transition.compensation_running,
            scanner_queued: metrics.lifecycle_transition.scanner_queued,
            scanner_missed: metrics.lifecycle_transition.scanner_missed,
            completed: metrics.lifecycle_transition.completed,
            failed: metrics.lifecycle_transition.failed,
        },
        lifecycle_expiry: MadminScannerLifecycleExpirySnapshot {
            current_queue_capacity: metrics.lifecycle_expiry.current_queue_capacity,
            current_queued: metrics.lifecycle_expiry.current_queued,
            current_active: metrics.lifecycle_expiry.current_active,
            current_workers: metrics.lifecycle_expiry.current_workers,
            queue_missed: metrics.lifecycle_expiry.queue_missed,
            scanner_queued: metrics.lifecycle_expiry.scanner_queued,
            scanner_missed: metrics.lifecycle_expiry.scanner_missed,
        },
        usage_freshness: MadminScannerUsageFreshnessSnapshot {
            dirty_pending_buckets: metrics.usage_freshness.dirty_pending_buckets,
            last_dirty_mark_unix_secs: metrics.usage_freshness.last_dirty_mark_unix_secs,
            last_dirty_clear_unix_secs: metrics.usage_freshness.last_dirty_clear_unix_secs,
            last_cycle_dirty_buckets: metrics.usage_freshness.last_cycle_dirty_buckets,
            last_cycle_cleared_dirty_buckets: metrics.usage_freshness.last_cycle_cleared_dirty_buckets,
            last_usage_save_unix_secs: metrics.usage_freshness.last_usage_save_unix_secs,
            last_usage_save_result: metrics.usage_freshness.last_usage_save_result,
            last_usage_save_result_code: metrics.usage_freshness.last_usage_save_result_code,
        },
        maintenance_control: MadminScannerMaintenanceControlSnapshot {
            primary_control: metrics.maintenance_control.primary_control,
            sources: metrics
                .maintenance_control
                .sources
                .into_iter()
                .map(|source| MadminScannerMaintenanceSourceSnapshot {
                    source: source.source,
                    state: source.state,
                    reason: source.reason,
                    backlog: source.backlog,
                    current_checked: source.current_checked,
                    current_queued: source.current_queued,
                    current_missed: source.current_missed,
                    lifetime_missed: source.lifetime_missed,
                    partial_cycles: source.partial_cycles,
                })
                .collect(),
        },
        partial_cycles_by_source: metrics
            .partial_cycles_by_source
            .into_iter()
            .map(|source| MadminScannerSourceCycleSnapshot {
                source: source.source,
                cycles: source.cycles,
            })
            .collect(),
        throttle_idle_mode_enabled: metrics.throttle_idle_mode_enabled,
        throttle_sleep_factor: metrics.throttle_sleep_factor,
        throttle_max_sleep_seconds: metrics.throttle_max_sleep_seconds,
        yield_every_n_objects: metrics.yield_every_n_objects,
        cycle_interval_seconds: metrics.cycle_interval_seconds,
        cycle_max_duration_seconds: metrics.cycle_max_duration_seconds,
        cycle_max_objects: metrics.cycle_max_objects,
        cycle_max_directories: metrics.cycle_max_directories,
        bitrot_cycle_enabled: metrics.bitrot_cycle_enabled,
        bitrot_cycle_seconds: metrics.bitrot_cycle_seconds,
        scan_checkpoint: metrics.scan_checkpoint.map(|checkpoint| MadminScannerCheckpointReport {
            version: checkpoint.version,
            resume_after: checkpoint.resume_after,
            reason: checkpoint.reason,
            last_event: checkpoint.last_event,
        }),
        scan_checkpoint_used: metrics.scan_checkpoint_used,
        scan_checkpoint_cleared: metrics.scan_checkpoint_cleared,
        scan_checkpoint_ignored: metrics.scan_checkpoint_ignored,
        scan_checkpoint_stale: metrics.scan_checkpoint_stale,
        source_work: metrics
            .source_work
            .into_iter()
            .map(|work| MadminScannerSourceWorkSnapshot {
                source: work.source,
                checked: work.checked,
                queued: work.queued,
                executed: work.executed,
                failed: work.failed,
                skipped: work.skipped,
                missed: work.missed,
            })
            .collect(),
        current_cycle_source_work: metrics
            .current_cycle_source_work
            .into_iter()
            .map(|work| MadminScannerSourceWorkSnapshot {
                source: work.source,
                checked: work.checked,
                queued: work.queued,
                executed: work.executed,
                failed: work.failed,
                skipped: work.skipped,
                missed: work.missed,
            })
            .collect(),
        last_cycle_source_work: metrics
            .last_cycle_source_work
            .into_iter()
            .map(|work| MadminScannerSourceWorkSnapshot {
                source: work.source,
                checked: work.checked,
                queued: work.queued,
                executed: work.executed,
                failed: work.failed,
                skipped: work.skipped,
                missed: work.missed,
            })
            .collect(),
        replication_repair: metrics
            .replication_repair
            .into_iter()
            .map(|repair| MadminScannerReplicationRepairSnapshot {
                source: repair.source,
                kind: repair.kind,
                checked: repair.checked,
                queued: repair.queued,
                executed: repair.executed,
                failed: repair.failed,
                skipped: repair.skipped,
                missed: repair.missed,
            })
            .collect(),
        current_cycle_replication_repair: metrics
            .current_cycle_replication_repair
            .into_iter()
            .map(|repair| MadminScannerReplicationRepairSnapshot {
                source: repair.source,
                kind: repair.kind,
                checked: repair.checked,
                queued: repair.queued,
                executed: repair.executed,
                failed: repair.failed,
                skipped: repair.skipped,
                missed: repair.missed,
            })
            .collect(),
        last_cycle_replication_repair: metrics
            .last_cycle_replication_repair
            .into_iter()
            .map(|repair| MadminScannerReplicationRepairSnapshot {
                source: repair.source,
                kind: repair.kind,
                checked: repair.checked,
                queued: repair.queued,
                executed: repair.executed,
                failed: repair.failed,
                skipped: repair.skipped,
                missed: repair.missed,
            })
            .collect(),
        partial_cycles: metrics.partial_cycles,
    }
}

impl MetricType {
    fn contains(&self, x: &MetricType) -> bool {
        (self.0 & x.0) == x.0
    }
}

/// Collect local metrics based on the specified types and options.
///
/// # Arguments
///
/// * `types` - A `MetricType` specifying which types of metrics to collect.
/// * `opts` - A reference to `CollectMetricsOpts` containing additional options for metric collection.
///
/// # Returns
/// * A `RealtimeMetrics` struct containing the collected metrics.
///
pub async fn collect_local_metrics(types: MetricType, opts: &CollectMetricsOpts) -> RealtimeMetrics {
    debug!("collect_local_metrics");
    let mut real_time_metrics = RealtimeMetrics::default();
    if types.0 == MetricType::NONE.0 {
        info!("types is None, return");
        return real_time_metrics;
    }

    let mut by_host_name = GLOBAL_RUSTFS_ADDR.read().await.clone();
    if !opts.hosts.is_empty() {
        let server = get_local_server_property().await;
        if opts.hosts.contains(&server.endpoint) {
            by_host_name = server.endpoint;
        } else {
            return real_time_metrics;
        }
    }
    let local_node_name = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
    if by_host_name.starts_with(":") && !local_node_name.starts_with(":") {
        by_host_name = local_node_name;
    }

    if types.contains(&MetricType::DISK) {
        debug!("start get disk metrics");
        let mut aggr = DiskMetric {
            collected_at: Utc::now(),
            ..Default::default()
        };
        for (name, disk) in collect_local_disks_metrics(&opts.disks).await.into_iter() {
            debug!("got disk metric, name: {name}, metric: {disk:?}");
            real_time_metrics.by_disk.insert(name, disk.clone());
            aggr.merge(&disk);
        }
        real_time_metrics.aggregated.disk = Some(aggr);
    }

    if types.contains(&MetricType::SCANNER) {
        debug!("start get scanner metrics");
        let mut metrics = global_metrics().report().await;
        if let Some(init_time) = rustfs_common::get_global_init_time().await {
            metrics.current_started = init_time;
        }
        real_time_metrics.aggregated.scanner = Some(to_madmin_scanner_metrics(metrics));
    }

    // if types.contains(&MetricType::OS) {}

    // if types.contains(&MetricType::BATCH_JOBS) {}

    // if types.contains(&MetricType::SITE_RESYNC) {}

    if types.contains(&MetricType::NET) {
        let snapshot = global_internode_metrics().snapshot();
        real_time_metrics.aggregated.net = Some(NetMetrics {
            collected_at: Utc::now(),
            interface_name: "internode".to_string(),
            net_stats: NetDevLine {
                name: "internode".to_string(),
                rx_bytes: snapshot.recv_bytes_total,
                tx_bytes: snapshot.sent_bytes_total,
                ..Default::default()
            },
        });
    }

    // if types.contains(&MetricType::MEM) {}

    // if types.contains(&MetricType::CPU) {}

    if types.contains(&MetricType::RPC) {
        let collected_at = Utc::now();
        let snapshot = global_internode_metrics().snapshot();
        let last_connect_time =
            chrono::DateTime::<Utc>::from_timestamp_millis(snapshot.last_dial_unix_millis as i64).unwrap_or(collected_at);

        real_time_metrics.aggregated.rpc = Some(RPCMetrics {
            collected_at,
            connected: i32::from(snapshot.last_dial_unix_millis > 0),
            reconnect_count: snapshot.dial_errors_total.min(i32::MAX as u64) as i32,
            disconnected: 0,
            outgoing_streams: 0,
            incoming_streams: 0,
            outgoing_bytes: snapshot.sent_bytes_total.min(i64::MAX as u64) as i64,
            incoming_bytes: snapshot.recv_bytes_total.min(i64::MAX as u64) as i64,
            outgoing_messages: snapshot.outgoing_requests_total.min(i64::MAX as u64) as i64,
            incoming_messages: snapshot.incoming_requests_total.min(i64::MAX as u64) as i64,
            out_queue: 0,
            last_pong_time: collected_at,
            last_ping_ms: snapshot.dial_avg_time_nanos as f64 / 1_000_000.0,
            max_ping_dur_ms: snapshot.dial_avg_time_nanos as f64 / 1_000_000.0,
            last_connect_time,
            by_destination: None,
            by_caller: None,
        });
    }

    real_time_metrics
        .by_host
        .insert(by_host_name.clone(), real_time_metrics.aggregated.clone());
    real_time_metrics.hosts.push(by_host_name);

    real_time_metrics
}

async fn collect_local_disks_metrics(disks: &HashSet<String>) -> HashMap<String, DiskMetric> {
    let store = match resolve_object_store_handle() {
        Some(store) => store,
        None => return HashMap::new(),
    };

    let mut metrics = HashMap::new();
    let storage_info = StorageAdminApi::local_storage_info(store.as_ref()).await;
    for d in storage_info.disks.iter() {
        if !disks.is_empty() && !disks.contains(&d.endpoint) {
            continue;
        }

        if d.state != DriveState::Ok.to_string() && d.state != DriveState::Unformatted.to_string() {
            metrics.insert(
                d.endpoint.clone(),
                DiskMetric {
                    n_disks: 1,
                    offline: 1,
                    ..Default::default()
                },
            );
            continue;
        }

        let mut dm = DiskMetric {
            n_disks: 1,
            ..Default::default()
        };
        if d.healing {
            dm.healing += 1;
        }

        if let Some(m) = &d.metrics {
            for (k, v) in m.api_calls.iter() {
                if *v != 0 {
                    dm.life_time_ops.insert(k.clone(), *v);
                }
            }
            for (k, v) in m.last_minute.iter() {
                if v.count != 0 {
                    dm.last_minute.operations.insert(k.clone(), v.clone());
                }
            }
        }

        if let Ok(st) = get_drive_stats(d.major, d.minor) {
            dm.io_stats = DiskIOStats {
                read_ios: st.read_ios,
                read_merges: st.read_merges,
                read_sectors: st.read_sectors,
                read_ticks: st.read_ticks,
                write_ios: st.write_ios,
                write_merges: st.write_merges,
                write_sectors: st.write_sectors,
                write_ticks: st.write_ticks,
                current_ios: st.current_ios,
                total_ticks: st.total_ticks,
                req_ticks: st.req_ticks,
                discard_ios: st.discard_ios,
                discard_merges: st.discard_merges,
                discard_sectors: st.discard_sectors,
                discard_ticks: st.discard_ticks,
                flush_ios: st.flush_ios,
                flush_ticks: st.flush_ticks,
            };
        }
        metrics.insert(d.endpoint.clone(), dm);
    }

    metrics
}

#[cfg(test)]
mod test {
    use super::*;
    use rustfs_io_metrics::internode_metrics::global_internode_metrics;
    use std::time::Duration;

    #[test]
    fn tes_types() {
        let t = MetricType::ALL;
        assert!(t.contains(&MetricType::NONE));
        assert!(t.contains(&MetricType::DISK));
        assert!(t.contains(&MetricType::OS));
        assert!(t.contains(&MetricType::BATCH_JOBS));
        assert!(t.contains(&MetricType::SITE_RESYNC));
        assert!(t.contains(&MetricType::NET));
        assert!(t.contains(&MetricType::MEM));
        assert!(t.contains(&MetricType::CPU));
        assert!(t.contains(&MetricType::RPC));

        let disk = MetricType::new(1 << 1);
        assert!(disk.contains(&MetricType::DISK));
    }

    #[tokio::test]
    async fn collect_local_metrics_reports_internode_net_and_rpc() {
        let metrics = global_internode_metrics();
        metrics.reset_for_test();
        metrics.record_sent_bytes(128);
        metrics.record_recv_bytes(64);
        metrics.record_outgoing_request();
        metrics.record_incoming_request();
        metrics.record_dial_result(Duration::from_millis(4), true);

        let realtime = collect_local_metrics(MetricType::NET, &CollectMetricsOpts::default()).await;
        let net = realtime.aggregated.net.expect("net metrics");
        assert_eq!(net.net_stats.tx_bytes, 128);
        assert_eq!(net.net_stats.rx_bytes, 64);

        let realtime = collect_local_metrics(MetricType::RPC, &CollectMetricsOpts::default()).await;
        let rpc = realtime.aggregated.rpc.expect("rpc metrics");
        assert_eq!(rpc.outgoing_bytes, 128);
        assert_eq!(rpc.incoming_bytes, 64);
        assert_eq!(rpc.outgoing_messages, 1);
        assert_eq!(rpc.incoming_messages, 1);
        assert!(rpc.last_ping_ms > 0.0);

        metrics.reset_for_test();
    }

    #[test]
    fn scanner_metrics_mapping_preserves_partial_source_status() {
        let scanner = to_madmin_scanner_metrics(rustfs_common::metrics::ScannerMetricsReport {
            last_cycle_partial_source: "usage".to_string(),
            last_cycle_partial_source_code: 1,
            partial_cycles_by_source: vec![rustfs_common::metrics::ScannerSourceCycleSnapshot {
                source: "usage".to_string(),
                cycles: 2,
            }],
            ..Default::default()
        });

        assert_eq!(scanner.last_cycle_partial_source, "usage");
        assert_eq!(scanner.last_cycle_partial_source_code, 1);
        let usage = scanner
            .partial_cycles_by_source
            .iter()
            .find(|source| source.source == "usage")
            .expect("usage partial source should be mapped");
        assert_eq!(usage.cycles, 2);
    }

    #[test]
    fn scanner_metrics_mapping_preserves_pacing_pressure() {
        let scanner = to_madmin_scanner_metrics(rustfs_common::metrics::ScannerMetricsReport {
            pacing_pressure: rustfs_common::metrics::ScannerPacingPressureSnapshot {
                primary_pressure: "cycle_budget".to_string(),
                current_queued_scans: 4,
                current_active_scans: 2,
                last_cycle_budget_limited: true,
                last_cycle_pause_observed: true,
                last_cycle_throttle_sleep_ratio: 0.25,
                last_cycle_yield_ratio: 0.05,
                last_cycle_total_pause_ratio: 0.3,
            },
            ..Default::default()
        });

        assert_eq!(scanner.pacing_pressure.primary_pressure, "cycle_budget");
        assert_eq!(scanner.pacing_pressure.current_queued_scans, 4);
        assert_eq!(scanner.pacing_pressure.current_active_scans, 2);
        assert!(scanner.pacing_pressure.last_cycle_budget_limited);
        assert!(scanner.pacing_pressure.last_cycle_pause_observed);
        assert_eq!(scanner.pacing_pressure.last_cycle_throttle_sleep_ratio, 0.25);
        assert_eq!(scanner.pacing_pressure.last_cycle_yield_ratio, 0.05);
        assert_eq!(scanner.pacing_pressure.last_cycle_total_pause_ratio, 0.3);
    }

    #[test]
    fn scanner_metrics_mapping_preserves_lifecycle_transition_status() {
        let scanner = to_madmin_scanner_metrics(rustfs_common::metrics::ScannerMetricsReport {
            current_cycle_lifecycle_expiry_actions: 2,
            current_cycle_lifecycle_transition_actions: 3,
            last_cycle_lifecycle_expiry_actions: 5,
            last_cycle_lifecycle_transition_actions: 7,
            lifecycle_expiry: rustfs_common::metrics::ScannerLifecycleExpirySnapshot {
                current_queue_capacity: 16,
                current_queued: 5,
                current_active: 2,
                current_workers: 4,
                queue_missed: 3,
                scanner_queued: 6,
                scanner_missed: 2,
            },
            lifecycle_transition: rustfs_common::metrics::ScannerLifecycleTransitionSnapshot {
                current_queue_capacity: 16,
                current_queued: 5,
                current_active: 2,
                current_workers: 4,
                queue_full: 3,
                queue_send_timeout: 1,
                compensation_scheduled: 2,
                compensation_pending: 3,
                compensation_running: 1,
                scanner_queued: 6,
                scanner_missed: 2,
                completed: 4,
                failed: 1,
            },
            ..Default::default()
        });

        assert_eq!(scanner.lifecycle_expiry.current_queue_capacity, 16);
        assert_eq!(scanner.lifecycle_expiry.current_queued, 5);
        assert_eq!(scanner.lifecycle_expiry.current_active, 2);
        assert_eq!(scanner.lifecycle_expiry.current_workers, 4);
        assert_eq!(scanner.lifecycle_expiry.queue_missed, 3);
        assert_eq!(scanner.lifecycle_expiry.scanner_queued, 6);
        assert_eq!(scanner.lifecycle_expiry.scanner_missed, 2);
        assert_eq!(scanner.lifecycle_transition.current_queue_capacity, 16);
        assert_eq!(scanner.lifecycle_transition.current_queued, 5);
        assert_eq!(scanner.lifecycle_transition.current_active, 2);
        assert_eq!(scanner.lifecycle_transition.current_workers, 4);
        assert_eq!(scanner.lifecycle_transition.queue_full, 3);
        assert_eq!(scanner.lifecycle_transition.queue_send_timeout, 1);
        assert_eq!(scanner.lifecycle_transition.compensation_scheduled, 2);
        assert_eq!(scanner.lifecycle_transition.compensation_pending, 3);
        assert_eq!(scanner.lifecycle_transition.compensation_running, 1);
        assert_eq!(scanner.lifecycle_transition.scanner_queued, 6);
        assert_eq!(scanner.lifecycle_transition.scanner_missed, 2);
        assert_eq!(scanner.lifecycle_transition.completed, 4);
        assert_eq!(scanner.lifecycle_transition.failed, 1);
        assert_eq!(scanner.current_cycle_lifecycle_expiry_actions, 2);
        assert_eq!(scanner.current_cycle_lifecycle_transition_actions, 3);
        assert_eq!(scanner.last_cycle_lifecycle_expiry_actions, 5);
        assert_eq!(scanner.last_cycle_lifecycle_transition_actions, 7);
    }

    #[test]
    fn scanner_metrics_mapping_preserves_maintenance_control_status() {
        let scanner = to_madmin_scanner_metrics(rustfs_common::metrics::ScannerMetricsReport {
            maintenance_control: rustfs_common::metrics::ScannerMaintenanceControlSnapshot {
                primary_control: "blocked_source".to_string(),
                sources: vec![rustfs_common::metrics::ScannerMaintenanceSourceSnapshot {
                    source: "lifecycle".to_string(),
                    state: "blocked".to_string(),
                    reason: "missed_work".to_string(),
                    backlog: 4,
                    current_checked: 2,
                    current_queued: 1,
                    current_missed: 3,
                    lifetime_missed: 8,
                    partial_cycles: 5,
                }],
            },
            ..Default::default()
        });

        assert_eq!(scanner.maintenance_control.primary_control, "blocked_source");
        let lifecycle = scanner
            .maintenance_control
            .sources
            .iter()
            .find(|source| source.source == "lifecycle")
            .expect("lifecycle maintenance control should be mapped");
        assert_eq!(lifecycle.state, "blocked");
        assert_eq!(lifecycle.reason, "missed_work");
        assert_eq!(lifecycle.backlog, 4);
        assert_eq!(lifecycle.current_checked, 2);
        assert_eq!(lifecycle.current_queued, 1);
        assert_eq!(lifecycle.current_missed, 3);
        assert_eq!(lifecycle.lifetime_missed, 8);
        assert_eq!(lifecycle.partial_cycles, 5);
    }

    #[test]
    fn scanner_metrics_mapping_preserves_usage_freshness_status() {
        let scanner = to_madmin_scanner_metrics(rustfs_common::metrics::ScannerMetricsReport {
            usage_freshness: rustfs_common::metrics::ScannerUsageFreshnessSnapshot {
                dirty_pending_buckets: 3,
                last_dirty_mark_unix_secs: 10,
                last_dirty_clear_unix_secs: 11,
                last_cycle_dirty_buckets: 2,
                last_cycle_cleared_dirty_buckets: 1,
                last_usage_save_unix_secs: 12,
                last_usage_save_result: "success".to_string(),
                last_usage_save_result_code: 1,
            },
            ..Default::default()
        });

        assert_eq!(scanner.usage_freshness.dirty_pending_buckets, 3);
        assert_eq!(scanner.usage_freshness.last_dirty_mark_unix_secs, 10);
        assert_eq!(scanner.usage_freshness.last_dirty_clear_unix_secs, 11);
        assert_eq!(scanner.usage_freshness.last_cycle_dirty_buckets, 2);
        assert_eq!(scanner.usage_freshness.last_cycle_cleared_dirty_buckets, 1);
        assert_eq!(scanner.usage_freshness.last_usage_save_unix_secs, 12);
        assert_eq!(scanner.usage_freshness.last_usage_save_result, "success");
        assert_eq!(scanner.usage_freshness.last_usage_save_result_code, 1);
    }

    #[test]
    fn scanner_metrics_mapping_preserves_distributed_status_fields() {
        let scanner = to_madmin_scanner_metrics(rustfs_common::metrics::ScannerMetricsReport {
            active_scan_paths: 2,
            oldest_active_path_age_seconds: 45,
            active_paths: vec!["disk-a/bucket-a".to_string(), "disk-b/bucket-b".to_string()],
            current_scan_mode: "normal".to_string(),
            current_set_scan_concurrency_limit: 3,
            current_set_scans_queued: 4,
            current_set_scans_active: 5,
            current_disk_scan_concurrency_limit: 6,
            current_disk_bucket_scans_queued: 7,
            current_disk_bucket_scans_active: 8,
            current_cycle_objects_scanned: 9,
            current_cycle_directories_scanned: 10,
            current_cycle_bucket_drive_scans: 11,
            current_cycle_bucket_drive_failures: 12,
            current_cycle_yield_events: 13,
            current_cycle_yield_duration_seconds: 1.5,
            current_cycle_throttle_sleep_events: 14,
            current_cycle_throttle_sleep_duration_seconds: 2.5,
            current_cycle_ilm_actions: 15,
            current_cycle_heal_objects: 16,
            current_cycle_replication_checks: 17,
            current_cycle_usage_saves: 18,
            last_cycle_result: "partial".to_string(),
            last_cycle_result_code: 2,
            last_cycle_partial_reason: "objects".to_string(),
            last_cycle_partial_reason_code: 3,
            last_cycle_duration_seconds: 4.5,
            last_cycle_objects_scanned: 19,
            last_cycle_directories_scanned: 20,
            last_cycle_bucket_drive_scans: 21,
            last_cycle_bucket_drive_failures: 22,
            last_cycle_yield_events: 23,
            last_cycle_yield_duration_seconds: 5.5,
            last_cycle_throttle_sleep_events: 24,
            last_cycle_throttle_sleep_duration_seconds: 6.5,
            last_cycle_ilm_actions: 25,
            last_cycle_heal_objects: 26,
            last_cycle_replication_checks: 27,
            last_cycle_usage_saves: 28,
            failed_cycles: 29,
            partial_cycles_unknown: 30,
            partial_cycles_runtime: 31,
            partial_cycles_objects: 32,
            partial_cycles_directories: 33,
            throttle_idle_mode_enabled: true,
            throttle_sleep_factor: 2.0,
            throttle_max_sleep_seconds: 3.0,
            yield_every_n_objects: 34,
            cycle_interval_seconds: 35.0,
            cycle_max_duration_seconds: 36.0,
            cycle_max_objects: 37,
            cycle_max_directories: 38,
            bitrot_cycle_enabled: true,
            bitrot_cycle_seconds: 39.0,
            scan_checkpoint: Some(rustfs_common::metrics::ScannerCheckpointReport {
                version: 1,
                resume_after: "bucket-a/prefix-a".to_string(),
                reason: "directories".to_string(),
                last_event: "used".to_string(),
            }),
            scan_checkpoint_used: 40,
            scan_checkpoint_cleared: 41,
            scan_checkpoint_ignored: 42,
            scan_checkpoint_stale: 43,
            source_work: vec![rustfs_common::metrics::ScannerSourceWorkSnapshot {
                source: "usage".to_string(),
                checked: 44,
                queued: 45,
                executed: 46,
                failed: 47,
                skipped: 48,
                missed: 49,
            }],
            current_cycle_source_work: vec![rustfs_common::metrics::ScannerSourceWorkSnapshot {
                source: "lifecycle".to_string(),
                checked: 50,
                queued: 51,
                executed: 52,
                failed: 53,
                skipped: 54,
                missed: 55,
            }],
            last_cycle_source_work: vec![rustfs_common::metrics::ScannerSourceWorkSnapshot {
                source: "heal".to_string(),
                checked: 56,
                queued: 57,
                executed: 58,
                failed: 59,
                skipped: 60,
                missed: 61,
            }],
            replication_repair: vec![rustfs_common::metrics::ScannerReplicationRepairSnapshot {
                source: "bucket_replication".to_string(),
                kind: "object".to_string(),
                checked: 62,
                queued: 63,
                executed: 64,
                failed: 65,
                skipped: 66,
                missed: 67,
            }],
            current_cycle_replication_repair: vec![rustfs_common::metrics::ScannerReplicationRepairSnapshot {
                source: "bucket_replication".to_string(),
                kind: "delete_marker".to_string(),
                checked: 68,
                queued: 69,
                executed: 70,
                failed: 71,
                skipped: 72,
                missed: 73,
            }],
            last_cycle_replication_repair: vec![rustfs_common::metrics::ScannerReplicationRepairSnapshot {
                source: "site_replication".to_string(),
                kind: "active_resync".to_string(),
                checked: 74,
                queued: 75,
                executed: 76,
                failed: 77,
                skipped: 78,
                missed: 79,
            }],
            partial_cycles: 80,
            ..Default::default()
        });

        assert_eq!(scanner.active_scan_paths, 2);
        assert_eq!(scanner.oldest_active_path_age_seconds, 45);
        assert_eq!(scanner.active_paths, vec!["disk-a/bucket-a".to_string(), "disk-b/bucket-b".to_string()]);
        assert_eq!(scanner.current_scan_mode, "normal");
        assert_eq!(scanner.current_set_scan_concurrency_limit, 3);
        assert_eq!(scanner.current_set_scans_queued, 4);
        assert_eq!(scanner.current_set_scans_active, 5);
        assert_eq!(scanner.current_disk_scan_concurrency_limit, 6);
        assert_eq!(scanner.current_disk_bucket_scans_queued, 7);
        assert_eq!(scanner.current_disk_bucket_scans_active, 8);
        assert_eq!(scanner.current_cycle_objects_scanned, 9);
        assert_eq!(scanner.current_cycle_directories_scanned, 10);
        assert_eq!(scanner.current_cycle_bucket_drive_scans, 11);
        assert_eq!(scanner.current_cycle_bucket_drive_failures, 12);
        assert_eq!(scanner.current_cycle_yield_events, 13);
        assert_eq!(scanner.current_cycle_yield_duration_seconds, 1.5);
        assert_eq!(scanner.current_cycle_throttle_sleep_events, 14);
        assert_eq!(scanner.current_cycle_throttle_sleep_duration_seconds, 2.5);
        assert_eq!(scanner.current_cycle_ilm_actions, 15);
        assert_eq!(scanner.current_cycle_heal_objects, 16);
        assert_eq!(scanner.current_cycle_replication_checks, 17);
        assert_eq!(scanner.current_cycle_usage_saves, 18);
        assert_eq!(scanner.last_cycle_result, "partial");
        assert_eq!(scanner.last_cycle_result_code, 2);
        assert_eq!(scanner.last_cycle_partial_reason, "objects");
        assert_eq!(scanner.last_cycle_partial_reason_code, 3);
        assert_eq!(scanner.last_cycle_duration_seconds, 4.5);
        assert_eq!(scanner.last_cycle_objects_scanned, 19);
        assert_eq!(scanner.last_cycle_directories_scanned, 20);
        assert_eq!(scanner.last_cycle_bucket_drive_scans, 21);
        assert_eq!(scanner.last_cycle_bucket_drive_failures, 22);
        assert_eq!(scanner.last_cycle_yield_events, 23);
        assert_eq!(scanner.last_cycle_yield_duration_seconds, 5.5);
        assert_eq!(scanner.last_cycle_throttle_sleep_events, 24);
        assert_eq!(scanner.last_cycle_throttle_sleep_duration_seconds, 6.5);
        assert_eq!(scanner.last_cycle_ilm_actions, 25);
        assert_eq!(scanner.last_cycle_heal_objects, 26);
        assert_eq!(scanner.last_cycle_replication_checks, 27);
        assert_eq!(scanner.last_cycle_usage_saves, 28);
        assert_eq!(scanner.failed_cycles, 29);
        assert_eq!(scanner.partial_cycles_unknown, 30);
        assert_eq!(scanner.partial_cycles_runtime, 31);
        assert_eq!(scanner.partial_cycles_objects, 32);
        assert_eq!(scanner.partial_cycles_directories, 33);
        assert!(scanner.throttle_idle_mode_enabled);
        assert_eq!(scanner.throttle_sleep_factor, 2.0);
        assert_eq!(scanner.throttle_max_sleep_seconds, 3.0);
        assert_eq!(scanner.yield_every_n_objects, 34);
        assert_eq!(scanner.cycle_interval_seconds, 35.0);
        assert_eq!(scanner.cycle_max_duration_seconds, 36.0);
        assert_eq!(scanner.cycle_max_objects, 37);
        assert_eq!(scanner.cycle_max_directories, 38);
        assert!(scanner.bitrot_cycle_enabled);
        assert_eq!(scanner.bitrot_cycle_seconds, 39.0);
        let checkpoint = scanner.scan_checkpoint.expect("checkpoint should be mapped");
        assert_eq!(checkpoint.resume_after, "bucket-a/prefix-a");
        assert_eq!(scanner.scan_checkpoint_used, 40);
        assert_eq!(scanner.scan_checkpoint_cleared, 41);
        assert_eq!(scanner.scan_checkpoint_ignored, 42);
        assert_eq!(scanner.scan_checkpoint_stale, 43);
        assert_eq!(scanner.source_work[0].source, "usage");
        assert_eq!(scanner.source_work[0].missed, 49);
        assert_eq!(scanner.current_cycle_source_work[0].source, "lifecycle");
        assert_eq!(scanner.current_cycle_source_work[0].queued, 51);
        assert_eq!(scanner.last_cycle_source_work[0].source, "heal");
        assert_eq!(scanner.last_cycle_source_work[0].skipped, 60);
        assert_eq!(scanner.replication_repair[0].source, "bucket_replication");
        assert_eq!(scanner.replication_repair[0].kind, "object");
        assert_eq!(scanner.replication_repair[0].missed, 67);
        assert_eq!(scanner.current_cycle_replication_repair[0].kind, "delete_marker");
        assert_eq!(scanner.current_cycle_replication_repair[0].queued, 69);
        assert_eq!(scanner.last_cycle_replication_repair[0].source, "site_replication");
        assert_eq!(scanner.last_cycle_replication_repair[0].kind, "active_resync");
        assert_eq!(scanner.last_cycle_replication_repair[0].skipped, 78);
        assert_eq!(scanner.partial_cycles, 80);
    }
}
