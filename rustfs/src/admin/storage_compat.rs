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

pub(crate) use rustfs_ecstore::api::bucket::lifecycle::tier_last_day_stats::DailyAllTierStats;
pub(crate) use rustfs_ecstore::api::bucket::{
    bandwidth, bucket_target_sys, lifecycle, metadata, metadata_sys, quota, replication, target, utils, versioning,
    versioning_sys,
};
pub(crate) use rustfs_ecstore::api::capacity::is_reserved_or_invalid_bucket;
pub(crate) use rustfs_ecstore::api::client::admin_handler_utils::AdminError;
pub(crate) use rustfs_ecstore::api::config::com::{
    STORAGE_CLASS_SUB_SYS, delete_config as delete_admin_config, read_config as read_admin_config,
    read_config_without_migrate as read_admin_config_without_migrate, save_config as save_admin_config,
    save_server_config as save_admin_server_config,
};
pub(crate) use rustfs_ecstore::api::config::{init as init_admin_config_defaults, set_global_storage_class, storageclass};
pub(crate) use rustfs_ecstore::api::data_usage::load_data_usage_from_backend;
pub(crate) use rustfs_ecstore::api::disk::RUSTFS_META_BUCKET;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::disk::endpoint::Endpoint;
pub(crate) use rustfs_ecstore::api::error::{Error, StorageError};
pub(crate) use rustfs_ecstore::api::global::{
    GLOBAL_BOOT_TIME, get_global_bucket_monitor, get_global_deployment_id, get_global_endpoints_opt, get_global_region,
    global_rustfs_port,
};
pub(crate) use rustfs_ecstore::api::layout::EndpointServerPools;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::layout::{Endpoints, PoolEndpoints};
pub(crate) use rustfs_ecstore::api::metrics::{CollectMetricsOpts, MetricType, collect_local_metrics};
pub(crate) use rustfs_ecstore::api::notification::get_global_notification_sys;
pub(crate) use rustfs_ecstore::api::rebalance::{
    DiskStat, RebalSaveOpt, RebalanceCleanupWarnings, RebalanceMeta, RebalanceStats,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::rebalance::{RebalStatus, RebalanceInfo};
pub(crate) use rustfs_ecstore::api::rpc::PeerRestClient;
pub(crate) use rustfs_ecstore::api::storage::ECStore;
pub(crate) use rustfs_ecstore::api::tier::tier::{
    ERR_TIER_BACKEND_IN_USE, ERR_TIER_BACKEND_NOT_EMPTY, ERR_TIER_MISSING_CREDENTIALS,
};
pub(crate) use rustfs_ecstore::api::tier::tier_admin::TierCreds;
pub(crate) use rustfs_ecstore::api::tier::tier_config::{TierConfig, TierType};
pub(crate) use rustfs_ecstore::api::tier::tier_handlers::{
    ERR_TIER_ALREADY_EXISTS, ERR_TIER_CONNECT_ERR, ERR_TIER_INVALID_CREDENTIALS, ERR_TIER_NAME_NOT_UPPERCASE, ERR_TIER_NOT_FOUND,
};
