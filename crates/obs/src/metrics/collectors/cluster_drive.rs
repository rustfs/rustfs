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

//! One observer's global drive inventory. Runtime counters belong to local drives only.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::MetricType;

#[derive(Debug, Clone, Default)]
pub(crate) struct ClusterDriveStats {
    pub server: String,
    pub drive: String,
    pub pool_index: String,
    pub set_index: String,
    pub drive_index: String,
    pub disk_id: String,
    pub runtime_state: String,
    pub offline_duration_seconds: Option<u64>,
    pub capacity_state: &'static str,
    pub capacity_age_seconds: u64,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
}

pub(crate) fn collect_cluster_drive_metrics(stats: &[ClusterDriveStats]) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::with_capacity(stats.len() * 13);
    for drive in stats {
        let metric = |name, help, value| {
            PrometheusMetric::new(name, MetricType::Gauge, help, value)
                .with_label_owned("server", drive.server.clone())
                .with_label_owned("drive", drive.drive.clone())
                .with_label_owned("pool_index", drive.pool_index.clone())
                .with_label_owned("set_index", drive.set_index.clone())
                .with_label_owned("drive_index", drive.drive_index.clone())
                .with_label_owned("disk_id", drive.disk_id.clone())
        };
        metrics.push(metric(
            "rustfs_cluster_drive_present",
            "Configured drive slot in this observer's inventory",
            1.0,
        ));
        for state in ["online", "offline", "returning", "suspect", "unknown"] {
            let observed = match drive.runtime_state.as_str() {
                "online" | "offline" | "returning" | "suspect" => drive.runtime_state.as_str(),
                _ => "unknown",
            };
            metrics.push(
                metric(
                    "rustfs_cluster_drive_runtime_state",
                    "Observed drive runtime state (one active state)",
                    f64::from(state == observed),
                )
                .with_label("state", state),
            );
        }
        if let Some(seconds) = drive.offline_duration_seconds {
            metrics.push(metric(
                "rustfs_cluster_drive_offline_duration_seconds",
                "Observed duration in seconds the drive has been offline",
                seconds as f64,
            ));
        }
        for state in ["live", "stale", "missing"] {
            metrics.push(
                metric(
                    "rustfs_cluster_drive_capacity_observation_state",
                    "Provenance of the observed drive capacity",
                    f64::from(state == drive.capacity_state),
                )
                .with_label("state", state),
            );
        }
        metrics.push(metric(
            "rustfs_cluster_drive_capacity_observation_age_seconds",
            "Age in seconds of the drive capacity observation at collection",
            drive.capacity_age_seconds as f64,
        ));
        // A missing capacity observation is unknown, not a zero-capacity drive.
        if drive.capacity_state != "missing" {
            metrics.push(metric(
                "rustfs_cluster_drive_total_bytes",
                "Observed total drive capacity in bytes",
                drive.total_bytes as f64,
            ));
            metrics.push(metric(
                "rustfs_cluster_drive_used_bytes",
                "Observed used drive capacity in bytes",
                drive.used_bytes as f64,
            ));
            metrics.push(metric(
                "rustfs_cluster_drive_free_bytes",
                "Observed free drive capacity in bytes",
                drive.free_bytes as f64,
            ));
        }
    }
    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_slots_remain_visible_without_inventing_capacity_or_counters() {
        let metrics = collect_cluster_drive_metrics(&[ClusterDriveStats {
            server: "unreachable:9000".into(),
            drive: "/data".into(),
            pool_index: "1".into(),
            set_index: "0".into(),
            drive_index: "3".into(),
            capacity_state: "missing",
            ..Default::default()
        }]);
        assert!(
            metrics
                .iter()
                .any(|metric| metric.name == "rustfs_cluster_drive_present" && metric.value == 1.0)
        );
        assert!(
            metrics
                .iter()
                .any(|metric| metric.name == "rustfs_cluster_drive_runtime_state"
                    && metric.value == 1.0
                    && metric.labels.iter().any(|(key, value)| *key == "state" && value == "unknown"))
        );
        assert!(
            !metrics
                .iter()
                .any(|metric| metric.name.ends_with("_bytes") || metric.metric_type == MetricType::Counter)
        );
        assert!(
            metrics
                .iter()
                .all(|metric| metric.labels.iter().any(|(key, value)| *key == "pool_index" && value == "1"))
        );
    }
}
