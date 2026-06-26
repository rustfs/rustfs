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

pub(crate) use rustfs_ecstore::api::bucket::bandwidth::monitor::Monitor as ObsBucketBandwidthMonitor;
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::{
    GLOBAL_ExpiryState as OBS_GLOBAL_EXPIRY_STATE, GLOBAL_TransitionState as OBS_GLOBAL_TRANSITION_STATE,
};
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::get_quota_config as obs_get_quota_config;
pub(crate) use rustfs_ecstore::api::bucket::replication::{
    GLOBAL_REPLICATION_STATS as OBS_GLOBAL_REPLICATION_STATS, ReplicationStats as ObsReplicationStats,
};
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
pub(crate) use rustfs_storage_api::{BucketOperations, BucketOptions, StorageAdminApi};

pub(crate) mod metrics {
    pub(crate) use super::{
        BucketOperations, BucketOptions, OBS_GLOBAL_EXPIRY_STATE, OBS_GLOBAL_REPLICATION_STATS, OBS_GLOBAL_TRANSITION_STATE,
        ObsBucketBandwidthMonitor, ObsEcstoreResult, ObsReplicationStats, ObsStore, StorageAdminApi,
        obs_get_global_bucket_monitor, obs_get_quota_config, obs_get_total_usable_capacity, obs_get_total_usable_capacity_free,
        obs_load_data_usage_from_backend, obs_resolve_object_store_handle,
    };
}
