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

pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::{
    GLOBAL_ExpiryState as OBS_GLOBAL_EXPIRY_STATE, GLOBAL_TransitionState as OBS_GLOBAL_TRANSITION_STATE,
};
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::get_quota_config as obs_get_quota_config;
pub(crate) use rustfs_ecstore::api::bucket::replication::GLOBAL_REPLICATION_STATS as OBS_GLOBAL_REPLICATION_STATS;
pub(crate) use rustfs_ecstore::api::capacity::{
    get_total_usable_capacity as obs_get_total_usable_capacity,
    get_total_usable_capacity_free as obs_get_total_usable_capacity_free,
};
pub(crate) use rustfs_ecstore::api::data_usage::load_data_usage_from_backend as obs_load_data_usage_from_backend;
pub(crate) use rustfs_ecstore::api::error::Result as ObsEcstoreResult;
pub(crate) use rustfs_ecstore::api::global::{
    get_global_bucket_monitor as obs_get_global_bucket_monitor, resolve_object_store_handle as obs_resolve_object_store_handle,
};
pub(crate) use rustfs_ecstore::api::storage::ECStore as ObsStore;

pub mod collectors;
pub mod config;
pub mod report;
pub mod scheduler;
pub mod schema;
pub mod stats_collector;

pub use collectors::*;
pub use config::*;
pub use report::{PrometheusMetric, report_metrics};
pub use scheduler::{
    MetricsRuntimeCancellationSource, MetricsRuntimeController, MetricsRuntimeControllerSnapshot, MetricsRuntimeDesiredSnapshot,
    MetricsRuntimeDesiredState, MetricsRuntimeIntervalsSnapshot, MetricsRuntimeReconcilePlan, MetricsRuntimeServiceState,
    MetricsRuntimeShutdownHandle, MetricsRuntimeStatusSnapshot, MetricsRuntimeWorkerMutation, init_metrics_collectors,
    init_metrics_runtime, metrics_runtime_controller_snapshot, metrics_runtime_status_snapshot,
};
