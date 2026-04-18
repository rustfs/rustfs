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

#![allow(dead_code)]

//! Cluster erasure set metrics collector.
//!
//! Collects erasure coding set metrics including parity, quorum,
//! drive counts, and health status.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::cluster_erasure_set::*;

/// Erasure set statistics.
#[derive(Debug, Clone, Default)]
pub struct ErasureSetStats {
    /// Pool ID
    pub pool_id: u32,
    /// Set ID within the pool
    pub set_id: u32,
    /// Total number of drives in the set
    pub size: u32,
    /// Number of parity drives
    pub parity: u32,
    /// Number of data shards
    pub data_shards: u32,
    /// Read quorum
    pub read_quorum: u32,
    /// Write quorum
    pub write_quorum: u32,
    /// Number of online drives
    pub online_drives_count: u32,
    /// Number of healing drives
    pub healing_drives_count: u32,
    /// Health status (1=healthy, 0=unhealthy)
    pub health: u8,
    /// Read tolerance (number of drive failures tolerated for reads)
    pub read_tolerance: u32,
    /// Write tolerance (number of drive failures tolerated for writes)
    pub write_tolerance: u32,
    /// Read health status (1=healthy, 0=unhealthy)
    pub read_health: u8,
    /// Write health status (1=healthy, 0=unhealthy)
    pub write_health: u8,
}

/// Collects erasure set metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for erasure sets.
pub fn collect_erasure_set_metrics(stats: &[ErasureSetStats]) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::with_capacity(stats.len() * 12);

    for stat in stats {
        let pool_id_label = stat.pool_id.to_string();
        let set_id_label = stat.set_id.to_string();

        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_SIZE_MD, stat.size as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_PARITY_MD, stat.parity as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_DATA_SHARDS_MD, stat.data_shards as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_READ_QUORUM_MD, stat.read_quorum as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_WRITE_QUORUM_MD, stat.write_quorum as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_ONLINE_DRIVES_COUNT_MD, stat.online_drives_count as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_HEALING_DRIVES_COUNT_MD, stat.healing_drives_count as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_HEALTH_MD, stat.health as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_READ_TOLERANCE_MD, stat.read_tolerance as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_WRITE_TOLERANCE_MD, stat.write_tolerance as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_READ_HEALTH_MD, stat.read_health as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ERASURE_SET_WRITE_HEALTH_MD, stat.write_health as f64)
                .with_label_owned(POOL_ID_L, pool_id_label.clone())
                .with_label_owned(SET_ID_L, set_id_label.clone()),
        );
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_erasure_set_metrics() {
        let stats = vec![ErasureSetStats {
            pool_id: 1,
            set_id: 0,
            size: 16,
            parity: 4,
            data_shards: 12,
            read_quorum: 13,
            write_quorum: 13,
            online_drives_count: 16,
            healing_drives_count: 0,
            health: 1,
            read_tolerance: 4,
            write_tolerance: 4,
            read_health: 1,
            write_health: 1,
        }];

        let metrics = collect_erasure_set_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 12);

        let size_name = ERASURE_SET_SIZE_MD.get_full_metric_name();
        let size = metrics.iter().find(|m| m.name == size_name);
        assert!(size.is_some());
        assert_eq!(size.map(|m| m.value), Some(16.0));

        let health_name = ERASURE_SET_HEALTH_MD.get_full_metric_name();
        let health = metrics.iter().find(|m| m.name == health_name);
        assert!(health.is_some());
        assert_eq!(health.map(|m| m.value), Some(1.0));
    }

    #[test]
    fn test_collect_erasure_set_metrics_empty() {
        let stats: Vec<ErasureSetStats> = vec![];
        let metrics = collect_erasure_set_metrics(&stats);
        assert!(metrics.is_empty());
    }
}
