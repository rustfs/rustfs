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

pub(crate) mod bandwidth {
    pub(crate) mod monitor {
        pub(crate) use rustfs_ecstore::api::bucket::bandwidth::monitor::BandwidthDetails;
    }
}
pub(crate) mod bucket_target_sys {
    pub(crate) use rustfs_ecstore::api::bucket::bucket_target_sys::{
        AdvancedPutOptions, BucketTargetError, BucketTargetSys, PutObjectOptions, RemoveObjectOptions, S3ClientError,
        TargetClient,
    };
}
pub(crate) mod lifecycle {
    pub(crate) mod bucket_lifecycle_ops {
        pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_TransitionState;
    }

    pub(crate) mod tier_last_day_stats {
        #[cfg(test)]
        pub(crate) use rustfs_ecstore::api::bucket::lifecycle::tier_last_day_stats::LastDayTierStats;
    }
}
pub(crate) mod metadata {
    pub(crate) use rustfs_ecstore::api::bucket::metadata::{
        BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG, BUCKET_QUOTA_CONFIG_FILE,
        BUCKET_REPLICATION_CONFIG, BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG, BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG,
        BucketMetadata, OBJECT_LOCK_CONFIG, table_catalog_path_hash,
    };
}
pub(crate) mod metadata_sys {
    pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::{
        BucketMetadataSys, delete, get, get_bucket_policy, get_bucket_targets_config, get_config_from_disk, get_lifecycle_config,
        get_notification_config, get_object_lock_config, get_quota_config, get_replication_config, get_sse_config,
        get_tagging_config, get_versioning_config, list_bucket_targets, update,
    };
}
pub(crate) mod quota {
    pub(crate) mod checker {
        pub(crate) use rustfs_ecstore::api::bucket::quota::checker::QuotaChecker;
    }

    pub(crate) use rustfs_ecstore::api::bucket::quota::{BucketQuota, QuotaError, QuotaOperation};
}
pub(crate) mod replication {
    pub(crate) use rustfs_ecstore::api::bucket::replication::{
        BucketReplicationResyncStatus, BucketStats, GLOBAL_REPLICATION_STATS, ObjectOpts, ReplicationConfigurationExt,
        ResyncOpts, get_global_replication_pool,
    };
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::bucket::replication::{ResyncStatusType, TargetReplicationResyncStatus};
}
pub(crate) mod target {
    pub(crate) use rustfs_ecstore::api::bucket::target::{ARN, BucketTarget, BucketTargetType, BucketTargets, Credentials};
}
pub(crate) mod utils {
    pub(crate) use rustfs_ecstore::api::bucket::utils::{deserialize, is_valid_object_prefix, serialize};
}
pub(crate) mod versioning {
    pub(crate) use rustfs_ecstore::api::bucket::versioning::VersioningApi;
}
pub(crate) mod versioning_sys {
    pub(crate) use rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys;
}
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::tier_last_day_stats::DailyAllTierStats;
pub(crate) use rustfs_ecstore::api::capacity::is_reserved_or_invalid_bucket;
pub(crate) use rustfs_ecstore::api::client::admin_handler_utils::AdminError;
pub(crate) use rustfs_ecstore::api::config::com::{
    STORAGE_CLASS_SUB_SYS, delete_config as delete_admin_config, read_config as read_admin_config,
    read_config_without_migrate as read_admin_config_without_migrate, save_config as save_admin_config,
    save_server_config as save_admin_server_config,
};
pub(crate) mod storageclass {
    pub(crate) use rustfs_ecstore::api::config::storageclass::{
        INLINE_BLOCK_ENV, OPTIMIZE_ENV, RRS, RRS_ENV, STANDARD, STANDARD_ENV, lookup_config,
    };
}
pub(crate) use rustfs_ecstore::api::config::{init as init_admin_config_defaults, set_global_storage_class};
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
