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

pub(crate) use crate::admin::storage_compat::{
    AdminError, AdminReplicationConfigExt, AdminVersioningConfigExt, CollectMetricsOpts, DailyAllTierStats, DiskStat, ECStore,
    ERR_TIER_ALREADY_EXISTS, ERR_TIER_BACKEND_IN_USE, ERR_TIER_BACKEND_NOT_EMPTY, ERR_TIER_CONNECT_ERR,
    ERR_TIER_INVALID_CREDENTIALS, ERR_TIER_MISSING_CREDENTIALS, ERR_TIER_NAME_NOT_UPPERCASE, ERR_TIER_NOT_FOUND,
    EndpointServerPools, Error, MetricType, NotificationSys, PeerRestClient, RUSTFS_META_BUCKET, RebalSaveOpt,
    RebalanceCleanupWarnings, RebalanceMeta, RebalanceStats, RebalanceStopPropagationRecord, STORAGE_CLASS_SUB_SYS, StorageError,
    TierConfig, TierCreds, TierType, collect_local_metrics, decode_rebalance_stop_propagation_record, delete_admin_config,
    get_global_deployment_id, get_global_endpoints_opt, get_global_notification_sys, get_global_region, global_rustfs_port,
    init_admin_config_defaults, is_reserved_or_invalid_bucket, load_data_usage_from_backend, read_admin_config,
    read_admin_config_without_migrate, save_admin_config, save_admin_server_config,
};

#[cfg(test)]
pub(crate) use crate::admin::storage_compat::{
    Endpoint, Endpoints, PoolEndpoints, RebalStatus, RebalanceCleanupWarningEntry, RebalanceInfo,
    encode_rebalance_stop_propagation_record,
};

pub(crate) mod bucket_target_sys {
    pub(crate) use crate::admin::storage_compat::bucket_target_sys::{BucketTargetError, BucketTargetSys};
}

pub(crate) mod lifecycle {
    pub(crate) mod bucket_lifecycle_ops {
        pub(crate) use crate::admin::storage_compat::lifecycle::bucket_lifecycle_ops::GLOBAL_TransitionState;
    }

    pub(crate) mod tier_last_day_stats {
        #[cfg(test)]
        pub(crate) use crate::admin::storage_compat::lifecycle::tier_last_day_stats::LastDayTierStats;
    }
}

pub(crate) mod metadata {
    pub(crate) use crate::admin::storage_compat::metadata::{
        BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG, BUCKET_QUOTA_CONFIG_FILE,
        BUCKET_REPLICATION_CONFIG, BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG, BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG,
        BucketMetadata, OBJECT_LOCK_CONFIG, table_catalog_path_hash,
    };
}

pub(crate) mod metadata_sys {
    pub(crate) use crate::admin::storage_compat::metadata_sys::{
        BucketMetadataSys, delete, get, get_bucket_policy, get_bucket_targets_config, get_config_from_disk, get_lifecycle_config,
        get_notification_config, get_object_lock_config, get_quota_config, get_replication_config, get_sse_config,
        get_tagging_config, get_versioning_config, list_bucket_targets, update,
    };
}

pub(crate) mod quota {
    pub(crate) use crate::admin::storage_compat::quota::{BucketQuota, QuotaError, QuotaOperation};

    pub(crate) mod checker {
        pub(crate) use crate::admin::storage_compat::quota::checker::QuotaChecker;
    }
}

pub(crate) mod replication {
    pub(crate) use crate::admin::storage_compat::replication::{
        BucketStats, GLOBAL_REPLICATION_STATS, ResyncOpts, get_global_replication_pool,
    };
}

pub(crate) mod storageclass {
    pub(crate) use crate::admin::storage_compat::storageclass::{
        INLINE_BLOCK_ENV, OPTIMIZE_ENV, RRS, RRS_ENV, STANDARD, STANDARD_ENV,
    };
}

pub(crate) mod target {
    pub(crate) use crate::admin::storage_compat::target::{ARN, BucketTarget, BucketTargetType, BucketTargets, Credentials};
}

pub(crate) mod utils {
    pub(crate) use crate::admin::storage_compat::utils::{deserialize, is_valid_object_prefix, serialize};
}

pub(crate) mod versioning_sys {
    pub(crate) use crate::admin::storage_compat::versioning_sys::BucketVersioningSys;
}
