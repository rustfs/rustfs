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

#[cfg(test)]
pub(crate) use rustfs_replication::FailStats;
pub(crate) use rustfs_replication::{
    ActiveWorkerStat, ProxyMetric, ProxyStatsCache, QueueCache, ReplicationMetricScope, SRMetricsSummary,
};
// Public so the admin wire DTOs (rustfs/src/admin/replication_metrics_wire.rs)
// can project the internal stats onto the minio-go response shapes through
// the storage_api facade chain.
pub use rustfs_replication::{BucketReplicationStat, BucketReplicationStats, BucketStats, InQueueMetric, XferStats};
