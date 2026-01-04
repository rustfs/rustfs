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

use crate::{
    admin_server_info::get_local_server_property,
    new_object_layer_fn,
    store_api::StorageAPI,
    // utils::os::get_drive_stats,
};
use chrono::Utc;
use rustfs_common::{GLOBAL_LOCAL_NODE_NAME, GLOBAL_RUSTFS_ADDR, heal_channel::DriveState, metrics::global_metrics};
use rustfs_madmin::metrics::{DiskIOStats, DiskMetric, RealtimeMetrics};
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
        real_time_metrics.aggregated.scanner = Some(metrics);
    }

    // if types.contains(&MetricType::OS) {}

    // if types.contains(&MetricType::BATCH_JOBS) {}

    // if types.contains(&MetricType::SITE_RESYNC) {}

    // if types.contains(&MetricType::NET) {}

    // if types.contains(&MetricType::MEM) {}

    // if types.contains(&MetricType::CPU) {}

    // if types.contains(&MetricType::RPC) {}

    real_time_metrics
        .by_host
        .insert(by_host_name.clone(), real_time_metrics.aggregated.clone());
    real_time_metrics.hosts.push(by_host_name);

    real_time_metrics
}

async fn collect_local_disks_metrics(disks: &HashSet<String>) -> HashMap<String, DiskMetric> {
    let store = match new_object_layer_fn() {
        Some(store) => store,
        None => return HashMap::new(),
    };

    let mut metrics = HashMap::new();
    let storage_info = store.local_storage_info().await;
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
    use super::MetricType;

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
}
