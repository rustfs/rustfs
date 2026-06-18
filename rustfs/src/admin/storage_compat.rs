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

pub(crate) mod bucket {
    pub(crate) use rustfs_ecstore::bucket::{
        bandwidth, bucket_target_sys, lifecycle, metadata, metadata_sys, quota, replication, target, utils, versioning,
        versioning_sys,
    };
}

pub(crate) mod client {
    pub(crate) use rustfs_ecstore::client::admin_handler_utils;
}

pub(crate) mod config {
    pub(crate) use rustfs_ecstore::config::{com, init, set_global_storage_class, storageclass};
}

pub(crate) mod data_usage {
    pub(crate) use rustfs_ecstore::data_usage::load_data_usage_from_backend;
}

pub(crate) mod disk {
    pub(crate) use rustfs_ecstore::disk::RUSTFS_META_BUCKET;
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::disk::endpoint;
}

pub(crate) mod endpoints {
    pub(crate) use rustfs_ecstore::endpoints::EndpointServerPools;
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::endpoints::{Endpoints, PoolEndpoints};
}

pub(crate) mod error {
    pub(crate) use rustfs_ecstore::error::{Error, StorageError};
}

pub(crate) mod global {
    pub(crate) use rustfs_ecstore::global::{
        GLOBAL_BOOT_TIME, get_global_bucket_monitor, get_global_deployment_id, get_global_endpoints_opt, get_global_region,
        global_rustfs_port,
    };
}

pub(crate) mod metrics_realtime {
    pub(crate) use rustfs_ecstore::metrics_realtime::{CollectMetricsOpts, MetricType, collect_local_metrics};
}

pub(crate) mod notification_sys {
    pub(crate) use rustfs_ecstore::notification_sys::get_global_notification_sys;
}

pub(crate) mod rebalance {
    pub(crate) use rustfs_ecstore::rebalance::{DiskStat, RebalSaveOpt, RebalanceCleanupWarnings, RebalanceMeta, RebalanceStats};
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::rebalance::{RebalStatus, RebalanceInfo};
}

pub(crate) mod rpc {
    pub(crate) use rustfs_ecstore::rpc::PeerRestClient;
}

pub(crate) mod store {
    pub(crate) use rustfs_ecstore::store::ECStore;
}

pub(crate) mod store_utils {
    pub(crate) use rustfs_ecstore::store_utils::is_reserved_or_invalid_bucket;
}

pub(crate) mod tier {
    pub(crate) use rustfs_ecstore::tier::{tier, tier_admin, tier_config, tier_handlers};
}
