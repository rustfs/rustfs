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

pub(crate) mod access_key_identity;
mod auth;
pub mod console;
pub mod handlers;
mod plugin_contract;
// Contract inventory is validated by tests before later runtime integration.
#[allow(dead_code)]
pub(crate) mod route_policy;
pub mod router;
pub(crate) mod runtime_sources;
pub mod service;
pub mod site_replication_identity;
pub(crate) mod storage_api;
pub mod utils;

#[cfg(test)]
mod console_test;
#[cfg(test)]
mod route_registration_test;

use handlers::{
    audit, bucket_meta, cluster_snapshot, config_admin, extensions, heal, health, kms, module_switch, object_zip_download, oidc,
    plugins_catalog, plugins_instances, pools, profile_admin, quota as quota_handler, rebalance,
    replication as replication_handler, scanner, site_replication, sts, system, table_catalog, tier, tls_debug, user,
};
use router::{AdminOperation, S3Router};
use s3s::route::S3Route;

/// Create admin router
///
/// # Arguments
/// * `console_enabled` - Whether the console is enabled
///
/// # Returns
/// An instance of S3Route for admin operations
pub fn make_admin_route(console_enabled: bool) -> std::io::Result<impl S3Route> {
    let mut r: S3Router<AdminOperation> = S3Router::new(console_enabled);
    register_admin_routes(&mut r)?;
    Ok(r)
}

fn register_admin_routes(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    health::register_health_route(r)?;
    sts::register_admin_auth_route(r)?;

    user::register_user_route(r)?;
    system::register_system_route(r)?;
    pools::register_pool_route(r)?;
    rebalance::register_rebalance_route(r)?;

    heal::register_heal_route(r)?;

    tier::register_tier_route(r)?;

    quota_handler::register_quota_route(r)?;
    bucket_meta::register_bucket_meta_route(r)?;
    config_admin::register_config_route(r)?;
    scanner::register_scanner_route(r)?;
    audit::register_audit_target_route(r)?;
    module_switch::register_module_switch_route(r)?;
    cluster_snapshot::register_cluster_snapshot_route(r)?;
    extensions::register_extension_route(r)?;
    object_zip_download::register_object_zip_download_route(r)?;
    plugins_catalog::register_plugin_catalog_route(r)?;
    plugins_instances::register_plugin_instance_route(r)?;

    replication_handler::register_replication_route(r)?;
    site_replication::register_site_replication_route(r)?;
    profile_admin::register_profiling_route(r)?;
    tls_debug::register_tls_debug_route(r)?;
    kms::register_kms_route(r)?;
    oidc::register_oidc_route(r)?;
    table_catalog::register_table_catalog_route(r)?;

    Ok(())
}

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

mod ecstore_bucket {
    pub(crate) use crate::storage::ecstore_bucket::{
        bandwidth, bucket_target_sys, lifecycle, metadata, metadata_sys, quota, replication, target, utils, versioning,
        versioning_sys,
    };
}

mod ecstore_capacity {
    pub(crate) use crate::storage::ecstore_capacity::is_reserved_or_invalid_bucket;
}

mod ecstore_client {
    pub(crate) use crate::storage::ecstore_client::admin_handler_utils;
}

mod ecstore_cluster {
    pub(crate) use crate::storage::ecstore_cluster::{
        ClusterDriveMembership, ClusterEndpointType, ClusterLocalNodeStorage, ClusterLocalNodeStorageSnapshot,
        ClusterMembershipSnapshot, ClusterNodeMembership, ClusterPeerHealth, ClusterPeerHealthSnapshot, ClusterPoolState,
        ClusterPoolStateSnapshot,
    };
}

mod ecstore_config {
    pub(crate) use crate::storage::ecstore_config::{com, init, storageclass};
}

#[allow(unused_imports)]
mod ecstore_disk {
    pub(crate) use crate::storage::ecstore_disk::{RUSTFS_META_BUCKET, endpoint};
}

mod ecstore_error {
    pub(crate) use crate::storage::ecstore_error::StorageError;
}

#[allow(unused_imports)]
mod ecstore_layout {
    pub(crate) use crate::storage::ecstore_layout::{EndpointServerPools, Endpoints, PoolEndpoints};
}

mod ecstore_metrics {
    pub(crate) use crate::storage::ecstore_metrics::{CollectMetricsOpts, MetricType, collect_local_metrics};
}

mod ecstore_notification {
    pub(crate) use crate::storage::ecstore_notification::NotificationSys;
}

#[allow(unused_imports)]
mod ecstore_rebalance {
    pub(crate) use crate::storage::ecstore_rebalance::{
        DiskStat, RebalSaveOpt, RebalStatus, RebalanceCleanupWarningEntry, RebalanceCleanupWarnings, RebalanceInfo,
        RebalanceMeta, RebalanceStats, RebalanceStopPropagationRecord, decode_rebalance_stop_propagation_record,
        encode_rebalance_stop_propagation_record,
    };
}

mod ecstore_rpc {
    pub(crate) use crate::storage::ecstore_rpc::PeerRestClient;
}

mod ecstore_storage {
    pub(crate) use crate::storage::ecstore_storage::ECStore;
}

mod ecstore_tier {
    pub(crate) use crate::storage::ecstore_tier::{tier, tier_admin, tier_config, tier_handlers};
}

pub(crate) const RUSTFS_META_BUCKET: &str = ecstore_disk::RUSTFS_META_BUCKET;
pub(crate) const STORAGE_CLASS_SUB_SYS: &str = ecstore_config::com::STORAGE_CLASS_SUB_SYS;

pub(crate) type AdminError = ecstore_client::admin_handler_utils::AdminError;
pub(crate) type CollectMetricsOpts = ecstore_metrics::CollectMetricsOpts;
pub(crate) type DiskStat = ecstore_rebalance::DiskStat;
pub(crate) type ECStore = ecstore_storage::ECStore;
pub(crate) type EndpointServerPools = ecstore_layout::EndpointServerPools;
pub(crate) type MetricType = ecstore_metrics::MetricType;
pub(crate) type NotificationSys = ecstore_notification::NotificationSys;
pub(crate) type PeerRestClient = ecstore_rpc::PeerRestClient;
pub(crate) type RebalSaveOpt = ecstore_rebalance::RebalSaveOpt;
pub(crate) type RebalanceCleanupWarnings = ecstore_rebalance::RebalanceCleanupWarnings;
pub(crate) type RebalanceMeta = ecstore_rebalance::RebalanceMeta;
pub(crate) type RebalanceStats = ecstore_rebalance::RebalanceStats;
pub(crate) type RebalanceStopPropagationRecord = ecstore_rebalance::RebalanceStopPropagationRecord;
pub(crate) type StorageError = ecstore_error::StorageError;
pub(crate) type Error = StorageError;
pub(crate) type Result<T> = core::result::Result<T, Error>;
pub(crate) type TierConfig = ecstore_tier::tier_config::TierConfig;
pub(crate) type TierCreds = ecstore_tier::tier_admin::TierCreds;
pub(crate) type TierType = ecstore_tier::tier_config::TierType;

#[cfg(test)]
pub(crate) type Endpoint = ecstore_disk::endpoint::Endpoint;
#[cfg(test)]
pub(crate) type Endpoints = ecstore_layout::Endpoints;
#[cfg(test)]
pub(crate) type PoolEndpoints = ecstore_layout::PoolEndpoints;
#[cfg(test)]
pub(crate) type RebalStatus = ecstore_rebalance::RebalStatus;
#[cfg(test)]
pub(crate) type RebalanceCleanupWarningEntry = ecstore_rebalance::RebalanceCleanupWarningEntry;
#[cfg(test)]
pub(crate) type RebalanceInfo = ecstore_rebalance::RebalanceInfo;

pub(crate) fn decode_rebalance_stop_propagation_record(message: &str) -> Option<RebalanceStopPropagationRecord> {
    ecstore_rebalance::decode_rebalance_stop_propagation_record(message)
}

#[cfg(test)]
pub(crate) fn encode_rebalance_stop_propagation_record(record: &RebalanceStopPropagationRecord) -> String {
    ecstore_rebalance::encode_rebalance_stop_propagation_record(record)
}

pub(crate) trait AdminReplicationConfigExt {
    fn filter_target_arns(&self, obj: &replication::ObjectOpts) -> Vec<String>;
    fn has_existing_object_replication(&self, arn: &str) -> (bool, bool);
}

impl AdminReplicationConfigExt for s3s::dto::ReplicationConfiguration {
    fn filter_target_arns(&self, obj: &replication::ObjectOpts) -> Vec<String> {
        <s3s::dto::ReplicationConfiguration as ecstore_bucket::replication::ReplicationConfigurationExt>::filter_target_arns(
            self, obj,
        )
    }

    fn has_existing_object_replication(&self, arn: &str) -> (bool, bool) {
        <s3s::dto::ReplicationConfiguration as ecstore_bucket::replication::ReplicationConfigurationExt>::has_existing_object_replication(
            self, arn,
        )
    }
}

pub(crate) trait AdminVersioningConfigExt {
    fn enabled(&self) -> bool;
}

impl AdminVersioningConfigExt for s3s::dto::VersioningConfiguration {
    fn enabled(&self) -> bool {
        <s3s::dto::VersioningConfiguration as ecstore_bucket::versioning::VersioningApi>::enabled(self)
    }
}

pub(crate) mod bandwidth {
    pub(crate) mod monitor {
        pub(crate) type BandwidthDetails = super::super::ecstore_bucket::bandwidth::monitor::BandwidthDetails;
    }
}

pub(crate) mod bucket_target_sys {
    pub(crate) type AdvancedPutOptions = super::ecstore_bucket::bucket_target_sys::AdvancedPutOptions;
    pub(crate) type BucketTargetError = super::ecstore_bucket::bucket_target_sys::BucketTargetError;
    pub(crate) type BucketTargetSys = super::ecstore_bucket::bucket_target_sys::BucketTargetSys;
    pub(crate) type PutObjectOptions = super::ecstore_bucket::bucket_target_sys::PutObjectOptions;
    pub(crate) type RemoveObjectOptions = super::ecstore_bucket::bucket_target_sys::RemoveObjectOptions;
    pub(crate) type S3ClientError = super::ecstore_bucket::bucket_target_sys::S3ClientError;
    pub(crate) type TargetClient = super::ecstore_bucket::bucket_target_sys::TargetClient;
}

pub(crate) mod lifecycle {
    pub(crate) mod tier_last_day_stats {
        #[cfg(test)]
        pub(crate) type LastDayTierStats = super::super::ecstore_bucket::lifecycle::tier_last_day_stats::LastDayTierStats;
    }
}

pub(crate) mod metadata {
    pub(crate) const BUCKET_CORS_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_CORS_CONFIG;
    pub(crate) const BUCKET_LIFECYCLE_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
    pub(crate) const BUCKET_NOTIFICATION_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_NOTIFICATION_CONFIG;
    pub(crate) const BUCKET_POLICY_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG;
    pub(crate) const BUCKET_QUOTA_CONFIG_FILE: &str = super::ecstore_bucket::metadata::BUCKET_QUOTA_CONFIG_FILE;
    pub(crate) const BUCKET_REPLICATION_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_REPLICATION_CONFIG;
    pub(crate) const BUCKET_SSECONFIG: &str = super::ecstore_bucket::metadata::BUCKET_SSECONFIG;
    pub(crate) const BUCKET_TAGGING_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_TAGGING_CONFIG;
    pub(crate) const BUCKET_TARGETS_FILE: &str = super::ecstore_bucket::metadata::BUCKET_TARGETS_FILE;
    pub(crate) const BUCKET_VERSIONING_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_VERSIONING_CONFIG;
    pub(crate) const OBJECT_LOCK_CONFIG: &str = super::ecstore_bucket::metadata::OBJECT_LOCK_CONFIG;

    pub(crate) type BucketMetadata = super::ecstore_bucket::metadata::BucketMetadata;

    pub(crate) fn table_catalog_path_hash(value: &str) -> String {
        super::ecstore_bucket::metadata::table_catalog_path_hash(value)
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

    pub(crate) type BucketMetadataSys = super::ecstore_bucket::metadata_sys::BucketMetadataSys;

    pub(crate) async fn get(bucket: &str) -> Result<Arc<BucketMetadata>> {
        super::ecstore_bucket::metadata_sys::get(bucket).await
    }

    pub(crate) async fn update(bucket: &str, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime> {
        super::ecstore_bucket::metadata_sys::update(bucket, config_file, data).await
    }

    pub(crate) async fn delete(bucket: &str, config_file: &str) -> Result<OffsetDateTime> {
        super::ecstore_bucket::metadata_sys::delete(bucket, config_file).await
    }

    pub(crate) async fn get_bucket_policy(bucket: &str) -> Result<(BucketPolicy, OffsetDateTime)> {
        super::ecstore_bucket::metadata_sys::get_bucket_policy(bucket).await
    }

    pub(crate) async fn get_bucket_targets_config(bucket: &str) -> Result<BucketTargets> {
        super::ecstore_bucket::metadata_sys::get_bucket_targets_config(bucket).await
    }

    pub(crate) async fn get_config_from_disk(bucket: &str) -> Result<BucketMetadata> {
        super::ecstore_bucket::metadata_sys::get_config_from_disk(bucket).await
    }

    pub(crate) async fn get_lifecycle_config(bucket: &str) -> Result<(BucketLifecycleConfiguration, OffsetDateTime)> {
        super::ecstore_bucket::metadata_sys::get_lifecycle_config(bucket).await
    }

    pub(crate) async fn get_notification_config(bucket: &str) -> Result<Option<NotificationConfiguration>> {
        super::ecstore_bucket::metadata_sys::get_notification_config(bucket).await
    }

    pub(crate) async fn get_object_lock_config(bucket: &str) -> Result<(ObjectLockConfiguration, OffsetDateTime)> {
        super::ecstore_bucket::metadata_sys::get_object_lock_config(bucket).await
    }

    pub(crate) async fn get_quota_config(bucket: &str) -> Result<(BucketQuota, OffsetDateTime)> {
        super::ecstore_bucket::metadata_sys::get_quota_config(bucket).await
    }

    pub(crate) async fn get_replication_config(bucket: &str) -> Result<(s3s::dto::ReplicationConfiguration, OffsetDateTime)> {
        super::ecstore_bucket::metadata_sys::get_replication_config(bucket).await
    }

    pub(crate) async fn get_sse_config(bucket: &str) -> Result<(ServerSideEncryptionConfiguration, OffsetDateTime)> {
        super::ecstore_bucket::metadata_sys::get_sse_config(bucket).await
    }

    pub(crate) async fn get_tagging_config(bucket: &str) -> Result<(Tagging, OffsetDateTime)> {
        super::ecstore_bucket::metadata_sys::get_tagging_config(bucket).await
    }

    pub(crate) async fn get_versioning_config(bucket: &str) -> Result<(VersioningConfiguration, OffsetDateTime)> {
        super::ecstore_bucket::metadata_sys::get_versioning_config(bucket).await
    }

    pub(crate) async fn list_bucket_targets(bucket: &str) -> Result<BucketTargets> {
        super::ecstore_bucket::metadata_sys::list_bucket_targets(bucket).await
    }
}

pub(crate) mod quota {
    pub(crate) mod checker {
        pub(crate) type QuotaChecker = super::super::ecstore_bucket::quota::checker::QuotaChecker;
    }

    pub(crate) type BucketQuota = super::ecstore_bucket::quota::BucketQuota;
    pub(crate) type QuotaError = super::ecstore_bucket::quota::QuotaError;
    pub(crate) type QuotaOperation = super::ecstore_bucket::quota::QuotaOperation;
}

pub(crate) mod replication {
    pub(crate) type BucketReplicationResyncStatus = super::ecstore_bucket::replication::BucketReplicationResyncStatus;
    pub(crate) type BucketStats = super::ecstore_bucket::replication::BucketStats;
    pub(crate) type ObjectOpts = super::ecstore_bucket::replication::ObjectOpts;
    pub(crate) type ResyncOpts = super::ecstore_bucket::replication::ResyncOpts;
    #[cfg(test)]
    pub(crate) type ResyncStatusType = super::ecstore_bucket::replication::ResyncStatusType;
    #[cfg(test)]
    pub(crate) type TargetReplicationResyncStatus = super::ecstore_bucket::replication::TargetReplicationResyncStatus;
}

pub(crate) mod target {
    #[allow(clippy::upper_case_acronyms)]
    pub(crate) type ARN = super::ecstore_bucket::target::ARN;
    pub(crate) type BucketTarget = super::ecstore_bucket::target::BucketTarget;
    pub(crate) type BucketTargetType = super::ecstore_bucket::target::BucketTargetType;
    pub(crate) type BucketTargets = super::ecstore_bucket::target::BucketTargets;
    pub(crate) type Credentials = super::ecstore_bucket::target::Credentials;
}

pub(crate) mod ecstore_utils {
    pub(crate) fn deserialize<T>(input: &[u8]) -> s3s::xml::DeResult<T>
    where
        T: for<'xml> s3s::xml::Deserialize<'xml>,
    {
        super::ecstore_bucket::utils::deserialize(input)
    }

    pub(crate) fn is_valid_object_prefix(object: &str) -> bool {
        super::ecstore_bucket::utils::is_valid_object_prefix(object)
    }

    pub(crate) fn serialize<T: s3s::xml::Serialize>(val: &T) -> s3s::xml::SerResult<Vec<u8>> {
        super::ecstore_bucket::utils::serialize(val)
    }
}

pub(crate) mod versioning {}

pub(crate) mod versioning_sys {
    pub(crate) type BucketVersioningSys = super::ecstore_bucket::versioning_sys::BucketVersioningSys;
}

pub(crate) mod storageclass {
    pub(crate) const INLINE_BLOCK_ENV: &str = super::ecstore_config::storageclass::INLINE_BLOCK_ENV;
    pub(crate) const OPTIMIZE_ENV: &str = super::ecstore_config::storageclass::OPTIMIZE_ENV;
    pub(crate) const RRS: &str = super::ecstore_config::storageclass::RRS;
    pub(crate) const RRS_ENV: &str = super::ecstore_config::storageclass::RRS_ENV;
    pub(crate) const STANDARD: &str = super::ecstore_config::storageclass::STANDARD;
    pub(crate) const STANDARD_ENV: &str = super::ecstore_config::storageclass::STANDARD_ENV;

    pub(crate) type Config = super::ecstore_config::storageclass::Config;

    pub(crate) fn lookup_config(kvs: &rustfs_config::server_config::KVS, set_drive_count: usize) -> super::Result<Config> {
        super::ecstore_config::storageclass::lookup_config(kvs, set_drive_count)
    }
}

pub(crate) type DailyAllTierStats = ecstore_bucket::lifecycle::tier_last_day_stats::DailyAllTierStats;

pub(crate) fn is_reserved_or_invalid_bucket(bucket_entry: &str, strict: bool) -> bool {
    ecstore_capacity::is_reserved_or_invalid_bucket(bucket_entry, strict)
}

pub(crate) async fn read_admin_config(api: Arc<ECStore>, file: &str) -> Result<Vec<u8>> {
    ecstore_config::com::read_config(api, file).await
}

pub(crate) async fn read_admin_config_without_migrate(api: Arc<ECStore>) -> Result<rustfs_config::server_config::Config> {
    ecstore_config::com::read_config_without_migrate(api).await
}

pub(crate) async fn save_admin_config(api: Arc<ECStore>, file: &str, data: Vec<u8>) -> Result<()> {
    ecstore_config::com::save_config(api, file, data).await
}

pub(crate) async fn delete_admin_config(api: Arc<ECStore>, file: &str) -> Result<()> {
    ecstore_config::com::delete_config(api, file).await
}

pub(crate) async fn save_admin_server_config(api: Arc<ECStore>, cfg: &rustfs_config::server_config::Config) -> Result<()> {
    ecstore_config::com::save_server_config(api, cfg).await
}

pub(crate) fn init_admin_config_defaults() {
    ecstore_config::init();
}

pub(crate) async fn collect_local_metrics(
    types: MetricType,
    opts: &CollectMetricsOpts,
) -> rustfs_madmin::metrics::RealtimeMetrics {
    ecstore_metrics::collect_local_metrics(types, opts).await
}

pub(crate) struct AdminErrorRef(fn() -> &'static AdminError);

impl Deref for AdminErrorRef {
    type Target = AdminError;

    fn deref(&self) -> &Self::Target {
        (self.0)()
    }
}

pub(crate) static ERR_TIER_BACKEND_IN_USE: AdminErrorRef = AdminErrorRef(|| &ecstore_tier::tier::ERR_TIER_BACKEND_IN_USE);
pub(crate) static ERR_TIER_BACKEND_NOT_EMPTY: AdminErrorRef = AdminErrorRef(|| &ecstore_tier::tier::ERR_TIER_BACKEND_NOT_EMPTY);
pub(crate) static ERR_TIER_MISSING_CREDENTIALS: AdminErrorRef =
    AdminErrorRef(|| &ecstore_tier::tier::ERR_TIER_MISSING_CREDENTIALS);
pub(crate) static ERR_TIER_ALREADY_EXISTS: AdminErrorRef =
    AdminErrorRef(|| &ecstore_tier::tier_handlers::ERR_TIER_ALREADY_EXISTS);
pub(crate) static ERR_TIER_CONNECT_ERR: AdminErrorRef = AdminErrorRef(|| &ecstore_tier::tier_handlers::ERR_TIER_CONNECT_ERR);
pub(crate) static ERR_TIER_INVALID_CREDENTIALS: AdminErrorRef =
    AdminErrorRef(|| &ecstore_tier::tier_handlers::ERR_TIER_INVALID_CREDENTIALS);
pub(crate) static ERR_TIER_NAME_NOT_UPPERCASE: AdminErrorRef =
    AdminErrorRef(|| &ecstore_tier::tier_handlers::ERR_TIER_NAME_NOT_UPPERCASE);
pub(crate) static ERR_TIER_NOT_FOUND: AdminErrorRef = AdminErrorRef(|| &ecstore_tier::tier_handlers::ERR_TIER_NOT_FOUND);
