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

use std::ops::Deref;
use std::sync::Arc;

pub(crate) const RUSTFS_META_BUCKET: &str = rustfs_ecstore::api::disk::RUSTFS_META_BUCKET;
pub(crate) const STORAGE_CLASS_SUB_SYS: &str = rustfs_ecstore::api::config::com::STORAGE_CLASS_SUB_SYS;

pub(crate) type AdminError = rustfs_ecstore::api::client::admin_handler_utils::AdminError;
pub(crate) type CollectMetricsOpts = rustfs_ecstore::api::metrics::CollectMetricsOpts;
pub(crate) type DiskStat = rustfs_ecstore::api::rebalance::DiskStat;
pub(crate) type ECStore = rustfs_ecstore::api::storage::ECStore;
pub(crate) type EndpointServerPools = rustfs_ecstore::api::layout::EndpointServerPools;
pub(crate) type MetricType = rustfs_ecstore::api::metrics::MetricType;
pub(crate) type NotificationSys = rustfs_ecstore::api::notification::NotificationSys;
pub(crate) type PeerRestClient = rustfs_ecstore::api::rpc::PeerRestClient;
pub(crate) type RebalSaveOpt = rustfs_ecstore::api::rebalance::RebalSaveOpt;
pub(crate) type RebalanceCleanupWarnings = rustfs_ecstore::api::rebalance::RebalanceCleanupWarnings;
pub(crate) type RebalanceMeta = rustfs_ecstore::api::rebalance::RebalanceMeta;
pub(crate) type RebalanceStats = rustfs_ecstore::api::rebalance::RebalanceStats;
pub(crate) type StorageError = rustfs_ecstore::api::error::StorageError;
pub(crate) type Error = StorageError;
pub(crate) type Result<T> = core::result::Result<T, Error>;
pub(crate) type TierConfig = rustfs_ecstore::api::tier::tier_config::TierConfig;
pub(crate) type TierCreds = rustfs_ecstore::api::tier::tier_admin::TierCreds;
pub(crate) type TierType = rustfs_ecstore::api::tier::tier_config::TierType;

#[cfg(test)]
pub(crate) type Endpoint = rustfs_ecstore::api::disk::endpoint::Endpoint;
#[cfg(test)]
pub(crate) type Endpoints = rustfs_ecstore::api::layout::Endpoints;
#[cfg(test)]
pub(crate) type PoolEndpoints = rustfs_ecstore::api::layout::PoolEndpoints;
#[cfg(test)]
pub(crate) type RebalStatus = rustfs_ecstore::api::rebalance::RebalStatus;
#[cfg(test)]
pub(crate) type RebalanceInfo = rustfs_ecstore::api::rebalance::RebalanceInfo;

pub(crate) mod bandwidth {
    pub(crate) mod monitor {
        pub(crate) type BandwidthDetails = rustfs_ecstore::api::bucket::bandwidth::monitor::BandwidthDetails;
        pub(crate) type Monitor = rustfs_ecstore::api::bucket::bandwidth::monitor::Monitor;
    }
}

pub(crate) mod bucket_target_sys {
    pub(crate) type AdvancedPutOptions = rustfs_ecstore::api::bucket::bucket_target_sys::AdvancedPutOptions;
    pub(crate) type BucketTargetError = rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetError;
    pub(crate) type BucketTargetSys = rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetSys;
    pub(crate) type PutObjectOptions = rustfs_ecstore::api::bucket::bucket_target_sys::PutObjectOptions;
    pub(crate) type RemoveObjectOptions = rustfs_ecstore::api::bucket::bucket_target_sys::RemoveObjectOptions;
    pub(crate) type S3ClientError = rustfs_ecstore::api::bucket::bucket_target_sys::S3ClientError;
    pub(crate) type TargetClient = rustfs_ecstore::api::bucket::bucket_target_sys::TargetClient;
}

pub(crate) mod lifecycle {
    pub(crate) mod bucket_lifecycle_ops {
        use super::super::DailyAllTierStats;

        pub(crate) struct GlobalTransitionStateCompat;

        #[allow(non_upper_case_globals)]
        pub(crate) static GLOBAL_TransitionState: GlobalTransitionStateCompat = GlobalTransitionStateCompat;

        impl GlobalTransitionStateCompat {
            pub(crate) fn get_daily_all_tier_stats(&self) -> DailyAllTierStats {
                rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_TransitionState.get_daily_all_tier_stats()
            }
        }
    }

    pub(crate) mod tier_last_day_stats {
        #[cfg(test)]
        pub(crate) type LastDayTierStats = rustfs_ecstore::api::bucket::lifecycle::tier_last_day_stats::LastDayTierStats;
    }
}

pub(crate) mod metadata {
    pub(crate) const BUCKET_CORS_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_CORS_CONFIG;
    pub(crate) const BUCKET_LIFECYCLE_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
    pub(crate) const BUCKET_NOTIFICATION_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_NOTIFICATION_CONFIG;
    pub(crate) const BUCKET_POLICY_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_POLICY_CONFIG;
    pub(crate) const BUCKET_QUOTA_CONFIG_FILE: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_QUOTA_CONFIG_FILE;
    pub(crate) const BUCKET_REPLICATION_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_REPLICATION_CONFIG;
    pub(crate) const BUCKET_SSECONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_SSECONFIG;
    pub(crate) const BUCKET_TAGGING_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_TAGGING_CONFIG;
    pub(crate) const BUCKET_TARGETS_FILE: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_TARGETS_FILE;
    pub(crate) const BUCKET_VERSIONING_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_VERSIONING_CONFIG;
    pub(crate) const OBJECT_LOCK_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::OBJECT_LOCK_CONFIG;

    pub(crate) type BucketMetadata = rustfs_ecstore::api::bucket::metadata::BucketMetadata;

    pub(crate) fn table_catalog_path_hash(value: &str) -> String {
        rustfs_ecstore::api::bucket::metadata::table_catalog_path_hash(value)
    }
}

pub(crate) mod metadata_sys {
    use std::sync::Arc;

    use rustfs_policy::policy::BucketPolicy;
    use s3s::dto::{
        BucketLifecycleConfiguration, NotificationConfiguration, ObjectLockConfiguration, ServerSideEncryptionConfiguration,
        Tagging, VersioningConfiguration,
    };
    use time::OffsetDateTime;

    use super::Result;
    use super::metadata::BucketMetadata;
    use super::quota::BucketQuota;
    use super::target::BucketTargets;

    pub(crate) type BucketMetadataSys = rustfs_ecstore::api::bucket::metadata_sys::BucketMetadataSys;

    pub(crate) async fn get(bucket: &str) -> Result<Arc<BucketMetadata>> {
        rustfs_ecstore::api::bucket::metadata_sys::get(bucket).await
    }

    pub(crate) async fn update(bucket: &str, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime> {
        rustfs_ecstore::api::bucket::metadata_sys::update(bucket, config_file, data).await
    }

    pub(crate) async fn delete(bucket: &str, config_file: &str) -> Result<OffsetDateTime> {
        rustfs_ecstore::api::bucket::metadata_sys::delete(bucket, config_file).await
    }

    pub(crate) async fn get_bucket_policy(bucket: &str) -> Result<(BucketPolicy, OffsetDateTime)> {
        rustfs_ecstore::api::bucket::metadata_sys::get_bucket_policy(bucket).await
    }

    pub(crate) async fn get_bucket_targets_config(bucket: &str) -> Result<BucketTargets> {
        rustfs_ecstore::api::bucket::metadata_sys::get_bucket_targets_config(bucket).await
    }

    pub(crate) async fn get_config_from_disk(bucket: &str) -> Result<BucketMetadata> {
        rustfs_ecstore::api::bucket::metadata_sys::get_config_from_disk(bucket).await
    }

    pub(crate) async fn get_lifecycle_config(bucket: &str) -> Result<(BucketLifecycleConfiguration, OffsetDateTime)> {
        rustfs_ecstore::api::bucket::metadata_sys::get_lifecycle_config(bucket).await
    }

    pub(crate) async fn get_notification_config(bucket: &str) -> Result<Option<NotificationConfiguration>> {
        rustfs_ecstore::api::bucket::metadata_sys::get_notification_config(bucket).await
    }

    pub(crate) async fn get_object_lock_config(bucket: &str) -> Result<(ObjectLockConfiguration, OffsetDateTime)> {
        rustfs_ecstore::api::bucket::metadata_sys::get_object_lock_config(bucket).await
    }

    pub(crate) async fn get_quota_config(bucket: &str) -> Result<(BucketQuota, OffsetDateTime)> {
        rustfs_ecstore::api::bucket::metadata_sys::get_quota_config(bucket).await
    }

    pub(crate) async fn get_replication_config(bucket: &str) -> Result<(s3s::dto::ReplicationConfiguration, OffsetDateTime)> {
        rustfs_ecstore::api::bucket::metadata_sys::get_replication_config(bucket).await
    }

    pub(crate) async fn get_sse_config(bucket: &str) -> Result<(ServerSideEncryptionConfiguration, OffsetDateTime)> {
        rustfs_ecstore::api::bucket::metadata_sys::get_sse_config(bucket).await
    }

    pub(crate) async fn get_tagging_config(bucket: &str) -> Result<(Tagging, OffsetDateTime)> {
        rustfs_ecstore::api::bucket::metadata_sys::get_tagging_config(bucket).await
    }

    pub(crate) async fn get_versioning_config(bucket: &str) -> Result<(VersioningConfiguration, OffsetDateTime)> {
        rustfs_ecstore::api::bucket::metadata_sys::get_versioning_config(bucket).await
    }

    pub(crate) async fn list_bucket_targets(bucket: &str) -> Result<BucketTargets> {
        rustfs_ecstore::api::bucket::metadata_sys::list_bucket_targets(bucket).await
    }
}

pub(crate) mod quota {
    pub(crate) mod checker {
        pub(crate) type QuotaChecker = rustfs_ecstore::api::bucket::quota::checker::QuotaChecker;
    }

    pub(crate) type BucketQuota = rustfs_ecstore::api::bucket::quota::BucketQuota;
    pub(crate) type QuotaError = rustfs_ecstore::api::bucket::quota::QuotaError;
    pub(crate) type QuotaOperation = rustfs_ecstore::api::bucket::quota::QuotaOperation;
}

pub(crate) mod replication {
    use std::sync::Arc;

    pub(crate) type BucketReplicationResyncStatus = rustfs_ecstore::api::bucket::replication::BucketReplicationResyncStatus;
    pub(crate) type BucketStats = rustfs_ecstore::api::bucket::replication::BucketStats;
    pub(crate) type DynReplicationPool = rustfs_ecstore::api::bucket::replication::DynReplicationPool;
    pub(crate) type ObjectOpts = rustfs_ecstore::api::bucket::replication::ObjectOpts;
    pub(crate) type ReplicationStats = rustfs_ecstore::api::bucket::replication::ReplicationStats;
    pub(crate) type ResyncOpts = rustfs_ecstore::api::bucket::replication::ResyncOpts;
    #[cfg(test)]
    pub(crate) type ResyncStatusType = rustfs_ecstore::api::bucket::replication::ResyncStatusType;
    #[cfg(test)]
    pub(crate) type TargetReplicationResyncStatus = rustfs_ecstore::api::bucket::replication::TargetReplicationResyncStatus;

    pub(crate) struct GlobalReplicationStatsCompat;

    pub(crate) static GLOBAL_REPLICATION_STATS: GlobalReplicationStatsCompat = GlobalReplicationStatsCompat;

    impl GlobalReplicationStatsCompat {
        pub(crate) fn get(&self) -> Option<&'static Arc<ReplicationStats>> {
            rustfs_ecstore::api::bucket::replication::GLOBAL_REPLICATION_STATS.get()
        }
    }

    pub(crate) fn get_global_replication_pool() -> Option<Arc<DynReplicationPool>> {
        rustfs_ecstore::api::bucket::replication::get_global_replication_pool()
    }
}

pub(crate) mod target {
    #[allow(clippy::upper_case_acronyms)]
    pub(crate) type ARN = rustfs_ecstore::api::bucket::target::ARN;
    pub(crate) type BucketTarget = rustfs_ecstore::api::bucket::target::BucketTarget;
    pub(crate) type BucketTargetType = rustfs_ecstore::api::bucket::target::BucketTargetType;
    pub(crate) type BucketTargets = rustfs_ecstore::api::bucket::target::BucketTargets;
    pub(crate) type Credentials = rustfs_ecstore::api::bucket::target::Credentials;
}

pub(crate) mod utils {
    pub(crate) fn deserialize<T>(input: &[u8]) -> s3s::xml::DeResult<T>
    where
        T: for<'xml> s3s::xml::Deserialize<'xml>,
    {
        rustfs_ecstore::api::bucket::utils::deserialize(input)
    }

    pub(crate) fn is_valid_object_prefix(object: &str) -> bool {
        rustfs_ecstore::api::bucket::utils::is_valid_object_prefix(object)
    }

    pub(crate) fn serialize<T: s3s::xml::Serialize>(val: &T) -> s3s::xml::SerResult<Vec<u8>> {
        rustfs_ecstore::api::bucket::utils::serialize(val)
    }
}

pub(crate) mod versioning {}

pub(crate) mod versioning_sys {
    pub(crate) type BucketVersioningSys = rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys;
}

pub(crate) mod storageclass {
    pub(crate) const INLINE_BLOCK_ENV: &str = rustfs_ecstore::api::config::storageclass::INLINE_BLOCK_ENV;
    pub(crate) const OPTIMIZE_ENV: &str = rustfs_ecstore::api::config::storageclass::OPTIMIZE_ENV;
    pub(crate) const RRS: &str = rustfs_ecstore::api::config::storageclass::RRS;
    pub(crate) const RRS_ENV: &str = rustfs_ecstore::api::config::storageclass::RRS_ENV;
    pub(crate) const STANDARD: &str = rustfs_ecstore::api::config::storageclass::STANDARD;
    pub(crate) const STANDARD_ENV: &str = rustfs_ecstore::api::config::storageclass::STANDARD_ENV;

    pub(crate) type Config = rustfs_ecstore::api::config::storageclass::Config;

    pub(crate) fn lookup_config(kvs: &rustfs_config::server_config::KVS, set_drive_count: usize) -> super::Result<Config> {
        rustfs_ecstore::api::config::storageclass::lookup_config(kvs, set_drive_count)
    }
}

pub(crate) type DailyAllTierStats = rustfs_ecstore::api::bucket::lifecycle::tier_last_day_stats::DailyAllTierStats;

pub(crate) fn is_reserved_or_invalid_bucket(bucket_entry: &str, strict: bool) -> bool {
    rustfs_ecstore::api::capacity::is_reserved_or_invalid_bucket(bucket_entry, strict)
}

pub(crate) async fn read_admin_config(api: Arc<ECStore>, file: &str) -> Result<Vec<u8>> {
    rustfs_ecstore::api::config::com::read_config(api, file).await
}

pub(crate) async fn read_admin_config_without_migrate(api: Arc<ECStore>) -> Result<rustfs_config::server_config::Config> {
    rustfs_ecstore::api::config::com::read_config_without_migrate(api).await
}

pub(crate) async fn save_admin_config(api: Arc<ECStore>, file: &str, data: Vec<u8>) -> Result<()> {
    rustfs_ecstore::api::config::com::save_config(api, file, data).await
}

pub(crate) async fn delete_admin_config(api: Arc<ECStore>, file: &str) -> Result<()> {
    rustfs_ecstore::api::config::com::delete_config(api, file).await
}

pub(crate) async fn save_admin_server_config(api: Arc<ECStore>, cfg: &rustfs_config::server_config::Config) -> Result<()> {
    rustfs_ecstore::api::config::com::save_server_config(api, cfg).await
}

pub(crate) fn init_admin_config_defaults() {
    rustfs_ecstore::api::config::init();
}

pub(crate) fn set_global_storage_class(cfg: storageclass::Config) {
    rustfs_ecstore::api::config::set_global_storage_class(cfg);
}

pub(crate) async fn load_data_usage_from_backend(
    store: Arc<ECStore>,
) -> std::result::Result<rustfs_data_usage::DataUsageInfo, Error> {
    rustfs_ecstore::api::data_usage::load_data_usage_from_backend(store).await
}

pub(crate) fn get_global_bucket_monitor() -> Option<Arc<bandwidth::monitor::Monitor>> {
    rustfs_ecstore::api::global::get_global_bucket_monitor()
}

pub(crate) fn get_global_deployment_id() -> Option<String> {
    rustfs_ecstore::api::global::get_global_deployment_id()
}

pub(crate) fn get_global_endpoints_opt() -> Option<EndpointServerPools> {
    rustfs_ecstore::api::global::get_global_endpoints_opt()
}

pub(crate) fn get_global_region() -> Option<s3s::region::Region> {
    rustfs_ecstore::api::global::get_global_region()
}

pub(crate) fn global_rustfs_port() -> u16 {
    rustfs_ecstore::api::global::global_rustfs_port()
}

pub(crate) async fn collect_local_metrics(
    types: MetricType,
    opts: &CollectMetricsOpts,
) -> rustfs_madmin::metrics::RealtimeMetrics {
    rustfs_ecstore::api::metrics::collect_local_metrics(types, opts).await
}

pub(crate) fn get_global_notification_sys() -> Option<&'static NotificationSys> {
    rustfs_ecstore::api::notification::get_global_notification_sys()
}

pub(crate) struct BootTimeCompat;

pub(crate) static GLOBAL_BOOT_TIME: BootTimeCompat = BootTimeCompat;

impl BootTimeCompat {
    pub(crate) fn get(&self) -> Option<&'static std::time::SystemTime> {
        rustfs_ecstore::api::global::GLOBAL_BOOT_TIME.get()
    }
}

pub(crate) struct AdminErrorRef(fn() -> &'static AdminError);

impl Deref for AdminErrorRef {
    type Target = AdminError;

    fn deref(&self) -> &Self::Target {
        (self.0)()
    }
}

pub(crate) static ERR_TIER_BACKEND_IN_USE: AdminErrorRef =
    AdminErrorRef(|| &rustfs_ecstore::api::tier::tier::ERR_TIER_BACKEND_IN_USE);
pub(crate) static ERR_TIER_BACKEND_NOT_EMPTY: AdminErrorRef =
    AdminErrorRef(|| &rustfs_ecstore::api::tier::tier::ERR_TIER_BACKEND_NOT_EMPTY);
pub(crate) static ERR_TIER_MISSING_CREDENTIALS: AdminErrorRef =
    AdminErrorRef(|| &rustfs_ecstore::api::tier::tier::ERR_TIER_MISSING_CREDENTIALS);
pub(crate) static ERR_TIER_ALREADY_EXISTS: AdminErrorRef =
    AdminErrorRef(|| &rustfs_ecstore::api::tier::tier_handlers::ERR_TIER_ALREADY_EXISTS);
pub(crate) static ERR_TIER_CONNECT_ERR: AdminErrorRef =
    AdminErrorRef(|| &rustfs_ecstore::api::tier::tier_handlers::ERR_TIER_CONNECT_ERR);
pub(crate) static ERR_TIER_INVALID_CREDENTIALS: AdminErrorRef =
    AdminErrorRef(|| &rustfs_ecstore::api::tier::tier_handlers::ERR_TIER_INVALID_CREDENTIALS);
pub(crate) static ERR_TIER_NAME_NOT_UPPERCASE: AdminErrorRef =
    AdminErrorRef(|| &rustfs_ecstore::api::tier::tier_handlers::ERR_TIER_NAME_NOT_UPPERCASE);
pub(crate) static ERR_TIER_NOT_FOUND: AdminErrorRef =
    AdminErrorRef(|| &rustfs_ecstore::api::tier::tier_handlers::ERR_TIER_NOT_FOUND);
