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

//! App-local boundary for storage-layer helper APIs used by S3 use cases.

use rustfs_storage_api as storage_contracts;

#[cfg(test)]
#[allow(non_snake_case)]
pub(crate) fn EndpointServerPools(pools: Vec<crate::storage::PoolEndpoints>) -> crate::storage::EndpointServerPools {
    crate::storage::EndpointServerPools::from(pools)
}

pub(crate) mod admin {
    pub(crate) async fn get_server_info(get_pools: bool) -> rustfs_madmin::InfoMessage {
        crate::storage::ecstore_admin::get_server_info(get_pools).await
    }
}

pub(crate) mod capacity {
    pub(crate) type PoolDecommissionInfo = crate::storage::ecstore_capacity::PoolDecommissionInfo;
    pub(crate) type PoolStatus = crate::storage::ecstore_capacity::PoolStatus;
    pub(crate) type RebalStatus = crate::storage::ecstore_rebalance::RebalStatus;

    pub(crate) fn get_total_usable_capacity(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
        crate::storage::ecstore_capacity::get_total_usable_capacity(disks, info)
    }

    pub(crate) fn get_total_usable_capacity_free(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
        crate::storage::ecstore_capacity::get_total_usable_capacity_free(disks, info)
    }
}

pub(crate) mod data_usage {
    use std::sync::Arc;

    pub(crate) async fn apply_bucket_usage_memory_overlay(data_usage_info: &mut rustfs_data_usage::DataUsageInfo) {
        crate::storage::ecstore_data_usage::apply_bucket_usage_memory_overlay(data_usage_info).await;
    }

    pub(crate) async fn load_data_usage_from_backend(
        store: Arc<crate::storage::ECStore>,
    ) -> Result<rustfs_data_usage::DataUsageInfo, crate::storage::StorageError> {
        crate::storage::ecstore_data_usage::load_data_usage_from_backend(store).await
    }

    pub(crate) async fn refresh_versioned_bucket_usage_from_object_layer(
        store: Arc<crate::storage::ECStore>,
        data_usage_info: &mut rustfs_data_usage::DataUsageInfo,
    ) {
        crate::storage::ecstore_data_usage::refresh_versioned_bucket_usage_from_object_layer(store, data_usage_info).await;
    }

    pub(crate) async fn replace_bucket_usage_memory_from_info(data_usage_info: &rustfs_data_usage::DataUsageInfo) {
        crate::storage::ecstore_data_usage::replace_bucket_usage_memory_from_info(data_usage_info).await;
    }

    pub(crate) async fn record_bucket_object_delete_memory(bucket: &str, deleted_size: u64, removed_current_object: bool) {
        crate::storage::ecstore_data_usage::record_bucket_object_delete_memory(bucket, deleted_size, removed_current_object)
            .await;
    }

    pub(crate) async fn record_bucket_delete_marker_memory(bucket: &str) {
        crate::storage::ecstore_data_usage::record_bucket_delete_marker_memory(bucket).await;
    }

    pub(crate) async fn record_bucket_object_version_write_memory(
        bucket: &str,
        previous_current_size: Option<u64>,
        new_size: u64,
    ) {
        crate::storage::ecstore_data_usage::record_bucket_object_version_write_memory(bucket, previous_current_size, new_size)
            .await;
    }

    pub(crate) async fn record_bucket_object_write_memory(bucket: &str, previous_current_size: Option<u64>, new_size: u64) {
        crate::storage::ecstore_data_usage::record_bucket_object_write_memory(bucket, previous_current_size, new_size).await;
    }

    pub(crate) async fn remove_bucket_usage_from_backend(
        store: Arc<crate::storage::ECStore>,
        bucket: &str,
    ) -> Result<(), crate::storage::StorageError> {
        crate::storage::ecstore_data_usage::remove_bucket_usage_from_backend(store, bucket).await
    }
}

pub(crate) mod runtime {
    use std::{collections::HashMap, sync::Arc};

    pub(crate) type BucketBandwidthMonitor = crate::storage::BucketBandwidthMonitor;
    pub(crate) type DailyAllTierStats = crate::storage::DailyAllTierStats;
    pub(crate) type DynReplicationPool = crate::storage::DynReplicationPool;
    pub(crate) type ExpiryState = crate::storage::ExpiryState;
    pub(crate) type NotificationSys = crate::storage::NotificationSys;
    pub(crate) type ObjectStoreResolver = crate::storage::ObjectStoreResolver;
    pub(crate) type ReplicationStats = crate::storage::ReplicationStats;
    pub(crate) type ScannerMetricsReport = rustfs_common::metrics::ScannerMetricsReport;
    pub(crate) type StorageClassConfig = crate::storage::ecstore_config::storageclass::Config;
    pub(crate) type TierConfigMgr = crate::storage::TierConfigMgr;

    #[cfg(test)]
    pub(crate) type TierConfig = crate::storage::ecstore_tier::tier_config::TierConfig;
    #[cfg(test)]
    pub(crate) type TierType = crate::storage::ecstore_tier::tier_config::TierType;
    #[cfg(test)]
    pub(crate) use crate::storage::ecstore_tier::warm_backend::WarmBackend as AppWarmBackend;
    #[cfg(test)]
    pub(crate) type WarmBackendGetOpts = crate::storage::ecstore_tier::warm_backend::WarmBackendGetOpts;

    pub(crate) fn set_global_storage_class(cfg: StorageClassConfig) {
        crate::storage::ecstore_config::set_global_storage_class(cfg);
    }

    pub(crate) fn get_global_endpoints_opt() -> Option<crate::storage::EndpointServerPools> {
        crate::storage::get_global_endpoints_opt()
    }

    pub(crate) fn get_global_deployment_id() -> Option<String> {
        crate::storage::get_global_deployment_id()
    }

    pub(crate) fn get_global_lock_client() -> Option<Arc<dyn rustfs_lock::client::LockClient>> {
        crate::storage::get_global_lock_client()
    }

    pub(crate) fn get_global_lock_clients() -> Option<&'static HashMap<String, Arc<dyn rustfs_lock::client::LockClient>>> {
        crate::storage::get_global_lock_clients()
    }

    pub(crate) fn get_global_region() -> Option<s3s::region::Region> {
        crate::storage::get_global_region()
    }

    pub(crate) fn global_rustfs_port() -> u16 {
        crate::storage::global_rustfs_port()
    }

    pub(crate) fn get_global_tier_config_mgr() -> Arc<tokio::sync::RwLock<TierConfigMgr>> {
        crate::storage::get_global_tier_config_mgr()
    }

    pub(crate) fn get_global_expiry_state() -> Arc<tokio::sync::RwLock<ExpiryState>> {
        crate::storage::get_global_expiry_state()
    }

    pub(crate) fn new_object_layer_fn() -> Option<Arc<crate::storage::ECStore>> {
        crate::storage::new_object_layer_fn()
    }

    pub(crate) fn set_object_store_resolver(resolver: Arc<ObjectStoreResolver>) -> bool {
        crate::storage::set_object_store_resolver(resolver)
    }

    pub(crate) fn get_global_notification_sys() -> Option<&'static NotificationSys> {
        crate::storage::get_global_notification_sys()
    }

    pub(crate) fn get_global_bucket_monitor() -> Option<Arc<BucketBandwidthMonitor>> {
        crate::storage::get_global_bucket_monitor()
    }

    pub(crate) fn get_global_replication_pool() -> Option<Arc<DynReplicationPool>> {
        crate::storage::get_global_replication_pool()
    }

    pub(crate) fn get_global_replication_stats() -> Option<Arc<ReplicationStats>> {
        crate::storage::get_global_replication_stats()
    }

    pub(crate) fn get_global_boot_time() -> Option<std::time::SystemTime> {
        crate::storage::get_global_boot_time()
    }

    pub(crate) fn get_daily_all_tier_stats() -> DailyAllTierStats {
        crate::storage::get_daily_all_tier_stats()
    }

    pub(crate) async fn collect_scanner_metrics_report() -> ScannerMetricsReport {
        rustfs_common::metrics::global_metrics().report().await
    }

    #[cfg(test)]
    pub(crate) async fn init_local_disks(
        endpoint_pools: crate::storage::EndpointServerPools,
    ) -> Result<(), crate::storage::StorageError> {
        crate::storage::init_local_disks(endpoint_pools).await
    }
}

pub(crate) mod access {
    pub(crate) use crate::storage::access::{
        PostObjectRequestMarker, ReqInfo, authorize_request, has_bypass_governance_header, req_info_mut, req_info_ref,
    };
}

pub(crate) mod bucket {
    use std::sync::Arc;

    pub(crate) trait ObjectLockConfigExt {
        fn enabled(&self) -> bool;
    }

    impl ObjectLockConfigExt for s3s::dto::ObjectLockConfiguration {
        fn enabled(&self) -> bool {
            <s3s::dto::ObjectLockConfiguration as crate::storage::ecstore_bucket::object_lock::ObjectLockApi>::enabled(self)
        }
    }

    pub(crate) trait ReplicationConfigExt {
        fn filter_target_arns(&self, obj: &replication::ObjectOpts) -> Vec<String>;
        fn replicate(&self, opts: &replication::ObjectOpts) -> bool;
    }

    impl ReplicationConfigExt for s3s::dto::ReplicationConfiguration {
        fn filter_target_arns(&self, obj: &replication::ObjectOpts) -> Vec<String> {
            <s3s::dto::ReplicationConfiguration as crate::storage::ecstore_bucket::replication::ReplicationConfigurationExt>::filter_target_arns(
                self, obj,
            )
        }

        fn replicate(&self, opts: &replication::ObjectOpts) -> bool {
            <s3s::dto::ReplicationConfiguration as crate::storage::ecstore_bucket::replication::ReplicationConfigurationExt>::replicate(
                self, opts,
            )
        }
    }

    pub(crate) trait VersioningConfigExt {
        fn enabled(&self) -> bool;
        fn prefix_enabled(&self, prefix: &str) -> bool;
        fn suspended(&self) -> bool;
    }

    impl VersioningConfigExt for s3s::dto::VersioningConfiguration {
        fn enabled(&self) -> bool {
            <s3s::dto::VersioningConfiguration as crate::storage::ecstore_bucket::versioning::VersioningApi>::enabled(self)
        }

        fn prefix_enabled(&self, prefix: &str) -> bool {
            <s3s::dto::VersioningConfiguration as crate::storage::ecstore_bucket::versioning::VersioningApi>::prefix_enabled(
                self, prefix,
            )
        }

        fn suspended(&self) -> bool {
            <s3s::dto::VersioningConfiguration as crate::storage::ecstore_bucket::versioning::VersioningApi>::suspended(self)
        }
    }

    pub(crate) async fn predict_lifecycle_expiration(
        lifecycle: &s3s::dto::BucketLifecycleConfiguration,
        obj: &lifecycle::lifecycle::ObjectOpts,
    ) -> lifecycle::lifecycle::Event {
        crate::storage::ecstore_bucket::lifecycle::lifecycle::Lifecycle::predict_expiration(lifecycle, obj).await
    }

    pub(crate) fn validate_restore_request(
        request: &s3s::dto::RestoreRequest,
        api: Arc<crate::storage::ECStore>,
    ) -> std::io::Result<()> {
        <s3s::dto::RestoreRequest as crate::storage::ecstore_bucket::lifecycle::bucket_lifecycle_ops::RestoreRequestOps>::validate(
            request, api,
        )
    }

    pub(crate) mod bucket_target_sys {
        pub(crate) type BucketTargetSys = crate::storage::ecstore_bucket::bucket_target_sys::BucketTargetSys;
    }

    pub(crate) mod lifecycle {
        pub(crate) mod bucket_lifecycle_audit {
            pub(crate) type LcEventSrc = crate::storage::ecstore_bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
        }

        pub(crate) mod bucket_lifecycle_ops {
            use std::sync::Arc;

            use super::bucket_lifecycle_audit::LcEventSrc;

            #[cfg(test)]
            pub(crate) async fn init_background_expiry(api: Arc<crate::storage::ECStore>) {
                crate::storage::ecstore_bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry(api).await;
            }

            pub(crate) async fn enqueue_expiry_for_existing_objects(
                api: Arc<crate::storage::ECStore>,
                bucket: &str,
            ) -> Result<(), crate::storage::StorageError> {
                crate::storage::ecstore_bucket::lifecycle::bucket_lifecycle_ops::enqueue_expiry_for_existing_objects(api, bucket)
                    .await
            }

            pub(crate) async fn enqueue_transition_for_existing_objects(
                api: Arc<crate::storage::ECStore>,
                bucket: &str,
            ) -> Result<(), crate::storage::StorageError> {
                crate::storage::ecstore_bucket::lifecycle::bucket_lifecycle_ops::enqueue_transition_for_existing_objects(
                    api, bucket,
                )
                .await
            }

            pub(crate) async fn enqueue_transition_immediate(oi: &crate::storage::StorageObjectInfo, src: LcEventSrc) {
                crate::storage::ecstore_bucket::lifecycle::bucket_lifecycle_ops::enqueue_transition_immediate(oi, src).await;
            }

            pub(crate) async fn post_restore_opts(
                version_id: &str,
                bucket: &str,
                object: &str,
            ) -> Result<crate::storage::StorageObjectOptions, std::io::Error> {
                crate::storage::ecstore_bucket::lifecycle::bucket_lifecycle_ops::post_restore_opts(version_id, bucket, object)
                    .await
            }

            pub(crate) async fn validate_transition_tier(
                lc: &s3s::dto::BucketLifecycleConfiguration,
            ) -> Result<(), std::io::Error> {
                crate::storage::ecstore_bucket::lifecycle::bucket_lifecycle_ops::validate_transition_tier(lc).await
            }

            pub(crate) async fn validate_lifecycle_config(
                lc: &s3s::dto::BucketLifecycleConfiguration,
                lock_config: &s3s::dto::ObjectLockConfiguration,
            ) -> Result<(), std::io::Error> {
                use crate::storage::ecstore_bucket::lifecycle::lifecycle::Lifecycle as _;

                lc.validate(lock_config).await
            }
        }

        pub(crate) mod lifecycle_contract {
            #[cfg(test)]
            pub(crate) type IlmAction = crate::storage::ecstore_bucket::lifecycle::lifecycle::IlmAction;
            pub(crate) type Event = crate::storage::ecstore_bucket::lifecycle::lifecycle::Event;
            pub(crate) type ObjectOpts = crate::storage::ecstore_bucket::lifecycle::lifecycle::ObjectOpts;
            pub(crate) type TransitionOptions = crate::storage::ecstore_bucket::lifecycle::lifecycle::TransitionOptions;

            pub(crate) const TRANSITION_COMPLETE: &str =
                crate::storage::ecstore_bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;

            pub(crate) fn expected_expiry_time(mod_time: time::OffsetDateTime, days: i32) -> time::OffsetDateTime {
                crate::storage::ecstore_bucket::lifecycle::lifecycle::expected_expiry_time(mod_time, days)
            }
        }
        pub(crate) use lifecycle_contract as lifecycle;

        pub(crate) mod tier_delete_journal {
            use std::sync::Arc;

            pub(crate) async fn persist_tier_delete_journal_entry(
                api: Arc<crate::storage::ECStore>,
                je: &super::tier_sweeper::Jentry,
            ) -> std::io::Result<()> {
                crate::storage::ecstore_bucket::lifecycle::tier_delete_journal::persist_tier_delete_journal_entry(api, je).await
            }
        }

        pub(crate) mod tier_sweeper {
            pub(crate) type Jentry = crate::storage::ecstore_bucket::lifecycle::tier_sweeper::Jentry;

            pub(crate) fn transitioned_delete_journal_entry(
                version_id: Option<uuid::Uuid>,
                versioned: bool,
                suspended: bool,
                transitioned: &super::super::super::storage_contracts::TransitionedObject,
            ) -> Option<Jentry> {
                crate::storage::ecstore_bucket::lifecycle::tier_sweeper::transitioned_delete_journal_entry(
                    version_id,
                    versioned,
                    suspended,
                    transitioned,
                )
            }

            pub(crate) fn transitioned_force_delete_journal_entry(
                transitioned: &super::super::super::storage_contracts::TransitionedObject,
            ) -> Option<Jentry> {
                crate::storage::ecstore_bucket::lifecycle::tier_sweeper::transitioned_force_delete_journal_entry(transitioned)
            }
        }
    }

    pub(crate) mod metadata {
        pub(crate) const BUCKET_CORS_CONFIG: &str = crate::storage::ecstore_bucket::metadata::BUCKET_CORS_CONFIG;
        pub(crate) const BUCKET_LIFECYCLE_CONFIG: &str = crate::storage::ecstore_bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
        pub(crate) const BUCKET_NOTIFICATION_CONFIG: &str = crate::storage::ecstore_bucket::metadata::BUCKET_NOTIFICATION_CONFIG;
        pub(crate) const BUCKET_POLICY_CONFIG: &str = crate::storage::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG;
        pub(crate) const BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG: &str =
            crate::storage::ecstore_bucket::metadata::BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG;
        pub(crate) const BUCKET_REPLICATION_CONFIG: &str = crate::storage::ecstore_bucket::metadata::BUCKET_REPLICATION_CONFIG;
        pub(crate) const BUCKET_SSECONFIG: &str = crate::storage::ecstore_bucket::metadata::BUCKET_SSECONFIG;
        pub(crate) const BUCKET_TAGGING_CONFIG: &str = crate::storage::ecstore_bucket::metadata::BUCKET_TAGGING_CONFIG;
        pub(crate) const BUCKET_TARGETS_FILE: &str = crate::storage::ecstore_bucket::metadata::BUCKET_TARGETS_FILE;
        pub(crate) const BUCKET_VERSIONING_CONFIG: &str = crate::storage::ecstore_bucket::metadata::BUCKET_VERSIONING_CONFIG;
        #[cfg(test)]
        pub(crate) const OBJECT_LOCK_CONFIG: &str = crate::storage::ecstore_bucket::metadata::OBJECT_LOCK_CONFIG;
    }

    pub(crate) mod metadata_sys {
        use std::sync::Arc;

        use rustfs_policy::policy::BucketPolicy;
        use s3s::dto::{
            BucketLifecycleConfiguration, CORSConfiguration, NotificationConfiguration, ObjectLockConfiguration,
            PublicAccessBlockConfiguration, ServerSideEncryptionConfiguration, Tagging,
        };
        use time::OffsetDateTime;
        use tokio::sync::RwLock;

        use super::target::BucketTargets;

        pub(crate) type BucketMetadataSys = crate::storage::ecstore_bucket::metadata_sys::BucketMetadataSys;

        #[cfg(test)]
        pub(crate) async fn init_bucket_metadata_sys(api: Arc<crate::storage::ECStore>, buckets: Vec<String>) {
            crate::storage::ecstore_bucket::metadata_sys::init_bucket_metadata_sys(api, buckets).await;
        }

        pub(crate) async fn delete(bucket: &str, config_file: &str) -> Result<OffsetDateTime, crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::delete(bucket, config_file).await
        }

        pub(crate) async fn get_bucket_policy(
            bucket: &str,
        ) -> Result<(BucketPolicy, OffsetDateTime), crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::get_bucket_policy(bucket).await
        }

        pub(crate) async fn get_bucket_policy_raw(
            bucket: &str,
        ) -> Result<(String, OffsetDateTime), crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::get_bucket_policy_raw(bucket).await
        }

        pub(crate) async fn get_bucket_targets_config(bucket: &str) -> Result<BucketTargets, crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::get_bucket_targets_config(bucket).await
        }

        pub(crate) async fn get_cors_config(
            bucket: &str,
        ) -> Result<(CORSConfiguration, OffsetDateTime), crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::get_cors_config(bucket).await
        }

        pub(crate) fn get_global_bucket_metadata_sys() -> Option<Arc<RwLock<BucketMetadataSys>>> {
            crate::storage::ecstore_bucket::metadata_sys::get_global_bucket_metadata_sys()
        }

        pub(crate) async fn get_lifecycle_config(
            bucket: &str,
        ) -> Result<(BucketLifecycleConfiguration, OffsetDateTime), crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::get_lifecycle_config(bucket).await
        }

        pub(crate) async fn get_notification_config(
            bucket: &str,
        ) -> Result<Option<NotificationConfiguration>, crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::get_notification_config(bucket).await
        }

        pub(crate) async fn get_object_lock_config(
            bucket: &str,
        ) -> Result<(ObjectLockConfiguration, OffsetDateTime), crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::get_object_lock_config(bucket).await
        }

        pub(crate) async fn get_public_access_block_config(
            bucket: &str,
        ) -> Result<(PublicAccessBlockConfiguration, OffsetDateTime), crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::get_public_access_block_config(bucket).await
        }

        pub(crate) async fn get_replication_config(
            bucket: &str,
        ) -> Result<(s3s::dto::ReplicationConfiguration, OffsetDateTime), crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::get_replication_config(bucket).await
        }

        pub(crate) async fn get_sse_config(
            bucket: &str,
        ) -> Result<(ServerSideEncryptionConfiguration, OffsetDateTime), crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::get_sse_config(bucket).await
        }

        pub(crate) async fn get_tagging_config(bucket: &str) -> Result<(Tagging, OffsetDateTime), crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::get_tagging_config(bucket).await
        }

        pub(crate) async fn update(
            bucket: &str,
            config_file: &str,
            data: Vec<u8>,
        ) -> Result<OffsetDateTime, crate::storage::StorageError> {
            crate::storage::ecstore_bucket::metadata_sys::update(bucket, config_file, data).await
        }
    }

    pub(crate) mod object_lock {
        pub(crate) mod objectlock {
            pub(crate) fn get_object_legalhold_meta(
                meta: &std::collections::HashMap<String, String>,
            ) -> s3s::dto::ObjectLockLegalHold {
                crate::storage::ecstore_bucket::object_lock::objectlock::get_object_legalhold_meta(meta)
            }

            pub(crate) fn get_object_retention_meta(
                meta: &std::collections::HashMap<String, String>,
            ) -> s3s::dto::ObjectLockRetention {
                crate::storage::ecstore_bucket::object_lock::objectlock::get_object_retention_meta(meta)
            }
        }

        pub(crate) mod objectlock_sys {
            pub(crate) type BucketObjectLockSys =
                crate::storage::ecstore_bucket::object_lock::objectlock_sys::BucketObjectLockSys;
            pub(crate) type ObjectLockBlockReason =
                crate::storage::ecstore_bucket::object_lock::objectlock_sys::ObjectLockBlockReason;

            pub(crate) async fn check_object_lock_for_deletion(
                bucket: &str,
                obj_info: &crate::storage::StorageObjectInfo,
                bypass_governance: bool,
            ) -> Option<ObjectLockBlockReason> {
                crate::storage::ecstore_bucket::object_lock::objectlock_sys::check_object_lock_for_deletion(
                    bucket,
                    obj_info,
                    bypass_governance,
                )
                .await
            }

            pub(crate) fn is_retention_active(mode: &str, retain_until_date: Option<&s3s::dto::Date>) -> bool {
                crate::storage::ecstore_bucket::object_lock::objectlock_sys::is_retention_active(mode, retain_until_date)
            }
        }
    }

    pub(crate) mod policy_sys {
        pub(crate) type PolicySys = crate::storage::ecstore_bucket::policy_sys::PolicySys;
    }

    pub(crate) mod quota {
        pub(crate) mod checker {
            pub(crate) type QuotaChecker = crate::storage::ecstore_bucket::quota::checker::QuotaChecker;
        }

        pub(crate) type QuotaOperation = crate::storage::ecstore_bucket::quota::QuotaOperation;
    }

    pub(crate) mod replication {
        use std::collections::HashMap;
        use std::sync::Arc;

        pub(crate) type DeletedObjectReplicationInfo = crate::storage::ecstore_bucket::replication::DeletedObjectReplicationInfo;
        pub(crate) type MustReplicateOptions = crate::storage::ecstore_bucket::replication::MustReplicateOptions;
        pub(crate) type ObjectOpts = crate::storage::ecstore_bucket::replication::ObjectOpts;
        pub(crate) type ReplicateDecision = rustfs_filemeta::ReplicateDecision;

        pub(crate) async fn check_replicate_delete(
            bucket: &str,
            dobj: &super::super::storage_contracts::ObjectToDelete,
            oi: &crate::storage::StorageObjectInfo,
            del_opts: &crate::storage::StorageObjectOptions,
            gerr: Option<String>,
        ) -> ReplicateDecision {
            crate::storage::ecstore_bucket::replication::check_replicate_delete(bucket, dobj, oi, del_opts, gerr).await
        }

        pub(crate) fn get_must_replicate_options(
            user_defined: &HashMap<String, String>,
            user_tags: String,
            status: rustfs_filemeta::ReplicationStatusType,
            op_type: rustfs_filemeta::ReplicationType,
            opts: crate::storage::StorageObjectOptions,
        ) -> MustReplicateOptions {
            crate::storage::ecstore_bucket::replication::get_must_replicate_options(
                user_defined,
                user_tags,
                status,
                op_type,
                opts,
            )
        }

        pub(crate) async fn must_replicate(bucket: &str, object: &str, mopts: MustReplicateOptions) -> ReplicateDecision {
            crate::storage::ecstore_bucket::replication::must_replicate(bucket, object, mopts).await
        }

        pub(crate) async fn schedule_replication(
            oi: crate::storage::StorageObjectInfo,
            store: Arc<crate::storage::ECStore>,
            dsc: ReplicateDecision,
            op_type: rustfs_filemeta::ReplicationType,
        ) {
            crate::storage::ecstore_bucket::replication::schedule_replication(oi, store, dsc, op_type).await;
        }

        pub(crate) async fn schedule_replication_delete(dv: DeletedObjectReplicationInfo) {
            crate::storage::ecstore_bucket::replication::schedule_replication_delete(dv).await;
        }
    }

    pub(crate) mod tagging {
        pub(crate) fn decode_tags(tags: &str) -> Vec<s3s::dto::Tag> {
            crate::storage::ecstore_bucket::tagging::decode_tags(tags)
        }
    }

    pub(crate) mod target {
        #[cfg(test)]
        pub(crate) type BucketTarget = crate::storage::ecstore_bucket::target::BucketTarget;
        pub(crate) type BucketTargetType = crate::storage::ecstore_bucket::target::BucketTargetType;
        pub(crate) type BucketTargets = crate::storage::ecstore_bucket::target::BucketTargets;
    }

    pub(crate) mod utils {
        pub(crate) fn serialize<T: s3s::xml::Serialize>(val: &T) -> s3s::xml::SerResult<Vec<u8>> {
            crate::storage::ecstore_bucket::utils::serialize(val)
        }
    }

    #[cfg(test)]
    pub(crate) mod transition_api {
        pub(crate) type ReadCloser = crate::storage::ecstore_client::transition_api::ReadCloser;
        pub(crate) type ReaderImpl = crate::storage::ecstore_client::transition_api::ReaderImpl;
    }

    pub(crate) mod versioning_sys {
        pub(crate) type BucketVersioningSys = crate::storage::ecstore_bucket::versioning_sys::BucketVersioningSys;
    }
}

pub(crate) mod concurrency {
    pub(crate) use crate::storage::concurrency::{
        ConcurrencyManager, GetObjectGuard, IoQueueStatus, IoStrategy, PutObjectGuard, get_concurrency_aware_buffer_size,
        get_concurrency_manager, get_put_concurrency_aware_buffer_size,
    };
}

pub(crate) mod compression {
    pub(crate) use crate::storage::ecstore_compression::{MIN_DISK_COMPRESSIBLE_SIZE, is_disk_compressible};
}

pub(crate) mod deadlock_detector {
    #[cfg(test)]
    pub(crate) use crate::storage::deadlock_detector::RequestHangDetectionPolicy;
    pub(crate) use crate::storage::deadlock_detector::{DeadlockDetector, get_deadlock_detector};
}

pub(crate) mod ecfs {
    pub(crate) use crate::storage::ecfs::FS;
}

pub(crate) mod error {
    pub(crate) use crate::storage::{
        DiskError, StorageError, is_all_buckets_not_found, is_err_bucket_not_found, is_err_object_not_found,
        is_err_version_not_found,
    };

    pub(crate) type Error = StorageError;
}

pub(crate) mod head_prefix {
    pub(crate) use crate::storage::head_prefix::{head_prefix_not_found_message, probe_prefix_has_children};
}

pub(crate) mod helper {
    pub(crate) use crate::storage::helper::{OperationHelper, spawn_background_with_context};
}

pub(crate) mod object_utils {
    pub(crate) use crate::storage::to_s3s_etag;
}

pub(crate) mod io {
    #[cfg(test)]
    pub(crate) use crate::storage::{DecryptReader, EncryptReader, HardLimitReader, boxed_reader};
    pub(crate) use crate::storage::{DynReader, HashReader, WriteEncryption, WritePlan, compression_metadata_value, wrap_reader};
}

pub(crate) mod options {
    pub(crate) use crate::storage::options::{
        copy_dst_opts, copy_src_opts, del_opts, extract_metadata, extract_metadata_from_mime,
        extract_metadata_from_mime_with_object_name, filter_object_metadata, get_complete_multipart_upload_opts,
        get_content_sha256_with_query, get_opts, normalize_content_encoding_for_storage, parse_copy_source_range, put_opts,
        validate_archive_content_encoding,
    };
}

pub(crate) mod request_context {
    pub(crate) use crate::storage::request_context::{RequestContext, spawn_traced};
}

pub(crate) mod sse {
    pub(crate) use crate::storage::sse::{
        EncryptionKeyKind, SSEType, build_ssec_read_headers, encryption_material_to_metadata, extract_ssec_params_from_headers,
        extract_ssekms_context_from_headers, map_get_object_reader_error, mark_encrypted_multipart_metadata,
    };
    pub(crate) use crate::storage::{
        DecryptionRequest, EncryptionRequest, PrepareEncryptionRequest, apply_bucket_default_lock_retention,
        extract_server_side_encryption_from_headers, get_buffer_size_opt_in, sse_decryption, sse_encryption,
        sse_prepare_encryption,
    };
}

pub(crate) mod set_disk {
    pub(crate) use crate::storage::{get_lock_acquire_timeout, is_valid_storage_class};
}

pub(crate) mod storage_class {
    pub(crate) use crate::storage::ecstore_config::storageclass::STANDARD;
    #[cfg(test)]
    pub(crate) use crate::storage::ecstore_config::storageclass::STANDARD_IA;
}

pub(crate) mod timeout_wrapper {
    pub(crate) use crate::storage::timeout_wrapper::{GetObjectTimeoutPolicy, RequestTimeoutWrapper};
}

pub(crate) mod s3_api {
    pub(crate) mod bucket {
        pub(crate) use crate::storage::s3_api::bucket::{
            ListObjectVersionsParams, ListObjectsV2Params, build_list_buckets_output, build_list_object_versions_output,
            build_list_objects_output, build_list_objects_v2_output, parse_list_object_versions_params,
            parse_list_objects_v2_params,
        };
        pub(crate) use crate::storage::s3_api::common::rustfs_owner;
    }

    pub(crate) mod multipart {
        pub(crate) use crate::storage::s3_api::multipart::{
            ListMultipartUploadsParams, build_list_multipart_uploads_output, build_list_parts_output,
            parse_list_multipart_uploads_params, parse_list_parts_params, parse_upload_part_number,
        };
    }
}

pub(crate) mod admin_usecase {
    pub(crate) mod contract {
        pub(crate) use super::super::storage_contracts::StorageAdminApi;
    }

    pub(crate) use super::{admin, capacity, data_usage};
    pub(crate) use crate::storage::{ECStore, EndpointServerPools};
}

pub(crate) mod bucket_usecase {
    pub(crate) mod contract {
        pub(crate) mod bucket {
            pub(crate) use super::super::super::storage_contracts::{
                BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions,
            };
        }

        pub(crate) mod list {
            pub(crate) use super::super::super::storage_contracts::{ListObjectVersionsInfo, ListObjectsV2Info, ListOperations};
        }
    }

    pub(crate) use super::{access, bucket, data_usage, error, helper, object_utils, request_context, s3_api};
    pub(crate) use crate::storage::{
        ECStore, StorageObjectInfo, get_validated_store, process_lambda_configurations, process_queue_configurations,
        process_topic_configurations, validate_list_object_unordered_with_delimiter,
    };
}

pub(crate) mod object_usecase {
    pub(crate) mod contract {
        #[cfg(test)]
        pub(crate) mod http {
            pub(crate) use super::super::super::storage_contracts::HTTPPreconditions;
        }

        pub(crate) mod namespace {
            pub(crate) use super::super::super::storage_contracts::NamespaceLocking;
        }

        pub(crate) mod object {
            pub(crate) use super::super::super::storage_contracts::{ObjectIO, ObjectOperations};
        }

        pub(crate) mod range {
            pub(crate) use super::super::super::storage_contracts::HTTPRangeSpec;
        }
    }

    pub(crate) use super::{
        access, bucket, compression, concurrency, data_usage, deadlock_detector, ecfs, error, head_prefix, helper, io,
        object_utils, options, request_context, s3_api, set_disk, sse, storage_class, timeout_wrapper,
    };
    pub(crate) use crate::storage::{
        ECStore, RFC1123, StorageDeletedObject, StorageObjectInfo, StorageObjectOptions, StorageObjectToDelete,
        StoragePutObjReader, check_preconditions, get_validated_store, has_replication_rules, parse_object_lock_legal_hold,
        parse_object_lock_retention, parse_part_number_i32_to_usize, remove_object_lock_metadata_for_copy,
        strip_managed_encryption_metadata, validate_bucket_object_lock_enabled, validate_object_key,
        validate_sse_headers_for_read, validate_sse_headers_for_write, validate_ssec_for_read, wrap_response_with_cors,
    };
}

pub(crate) mod multipart_usecase {
    pub(crate) mod contract {
        #[cfg(test)]
        pub(crate) mod http {
            pub(crate) use super::super::super::storage_contracts::HTTPPreconditions;
        }

        pub(crate) mod multipart {
            pub(crate) use super::super::super::storage_contracts::{CompletePart, MultipartOperations, MultipartUploadResult};
        }

        pub(crate) mod object {
            pub(crate) use super::super::super::storage_contracts::{ObjectIO, ObjectOperations};
        }

        pub(crate) mod range {
            pub(crate) use super::super::super::storage_contracts::HTTPRangeSpec;
        }
    }

    pub(crate) use super::{
        access, bucket, compression, data_usage, error, helper, io, object_utils, options, s3_api, set_disk, sse,
    };
    pub(crate) use crate::storage::{ECStore, StorageObjectOptions, StoragePutObjReader};
}

pub(crate) mod select_object {
    pub(crate) mod contract {
        pub(crate) mod object {
            pub(crate) use super::super::super::storage_contracts::ObjectOperations;
        }
    }

    pub(crate) use super::{options, request_context};
    pub(crate) use crate::storage::{get_validated_store, validate_sse_headers_for_read, validate_ssec_for_read};
}

pub(crate) mod context {
    #[cfg(test)]
    pub(crate) use super::EndpointServerPools;
    pub(crate) use super::bucket;
    pub(crate) use super::runtime;
    pub(crate) use crate::storage::{ECStore, EndpointServerPools};
    #[cfg(test)]
    pub(crate) use crate::storage::{Endpoint, Endpoints, PoolEndpoints};
}

#[cfg(test)]
pub(crate) mod test {
    pub(crate) use super::EndpointServerPools;
    pub(crate) mod contract {
        pub(crate) mod bucket {
            pub(crate) use super::super::super::storage_contracts::{BucketOperations, BucketOptions, MakeBucketOptions};
        }

        pub(crate) mod heal {
            pub(crate) use super::super::super::storage_contracts::HealOperations;
        }

        pub(crate) mod list {
            pub(crate) use super::super::super::storage_contracts::ListOperations;
        }

        pub(crate) mod multipart {
            pub(crate) use super::super::super::storage_contracts::MultipartOperations;
        }

        pub(crate) mod object {
            pub(crate) use super::super::super::storage_contracts::{ObjectIO, ObjectOperations};
        }
    }

    pub(crate) use super::{bucket, ecfs, object_utils, runtime};
    pub(crate) use crate::storage::{
        ECStore, Endpoint, Endpoints, PoolEndpoints, StorageObjectInfo, StorageObjectOptions, StoragePutObjReader,
    };
}
