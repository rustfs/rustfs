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

pub mod collectors;
pub mod config;
mod ecstore_compat;
pub mod report;
pub mod scheduler;
pub mod schema;
pub mod stats_collector;

pub(crate) use ecstore_compat::{
    OBS_GLOBAL_EXPIRY_STATE, OBS_GLOBAL_REPLICATION_STATS, OBS_GLOBAL_TRANSITION_STATE, ObsEcstoreResult, ObsStore,
    obs_get_global_bucket_monitor, obs_get_quota_config, obs_get_total_usable_capacity, obs_get_total_usable_capacity_free,
    obs_load_data_usage_from_backend, obs_resolve_object_store_handle,
};

pub use collectors::*;
pub use config::*;
pub use report::{PrometheusMetric, report_metrics};
pub use scheduler::{
    MetricsRuntimeCancellationSource, MetricsRuntimeController, MetricsRuntimeControllerSnapshot, MetricsRuntimeDesiredSnapshot,
    MetricsRuntimeDesiredState, MetricsRuntimeIntervalsSnapshot, MetricsRuntimeReconcilePlan, MetricsRuntimeServiceState,
    MetricsRuntimeShutdownHandle, MetricsRuntimeStatusSnapshot, MetricsRuntimeWorkerMutation, init_metrics_collectors,
    init_metrics_runtime, metrics_runtime_controller_snapshot, metrics_runtime_status_snapshot,
};
