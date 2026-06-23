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

//! Application layer module entry.
//! Concrete use-case modules will be introduced incrementally in Phase 3.

pub mod admin_usecase;
pub mod bucket_usecase;
pub mod context;
pub mod multipart_usecase;
pub mod object_usecase;
mod select_object;

#[cfg(test)]
mod capacity_dirty_scope_test;
#[cfg(test)]
mod lifecycle_transition_api_test;

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

use std::sync::Arc;

mod ecstore_admin {
    pub(crate) use crate::storage::ecstore_admin::get_server_info;
}

mod ecstore_bucket {
    pub(crate) use crate::storage::ecstore_bucket::{
        bucket_target_sys, lifecycle, metadata, metadata_sys, object_lock, policy_sys, quota, replication, tagging, target,
        utils, versioning, versioning_sys,
    };
}

mod ecstore_capacity {
    pub(crate) use crate::storage::ecstore_capacity::{
        PoolDecommissionInfo, PoolStatus, get_total_usable_capacity, get_total_usable_capacity_free,
    };
}

#[allow(unused_imports)]
mod ecstore_client {
    pub(crate) use crate::storage::ecstore_client::{object_api_utils, transition_api};
}

mod ecstore_compression {
    pub(crate) use crate::storage::ecstore_compression::{MIN_DISK_COMPRESSIBLE_SIZE, is_disk_compressible};
}

mod ecstore_config {
    pub(crate) use crate::storage::ecstore_config::storageclass;
}

mod ecstore_data_usage {
    pub(crate) use crate::storage::ecstore_data_usage::{
        apply_bucket_usage_memory_overlay, load_data_usage_from_backend, record_bucket_object_delete_memory,
        record_bucket_object_write_memory,
    };
}

#[cfg(test)]
mod ecstore_global {
    pub(crate) use crate::storage::ecstore_global::GLOBAL_TierConfigMgr;
}

#[allow(unused_imports)]
mod ecstore_tier {
    pub(crate) use crate::storage::ecstore_tier::{tier, tier_config, warm_backend};
}

pub(crate) const MIN_DISK_COMPRESSIBLE_SIZE: usize = ecstore_compression::MIN_DISK_COMPRESSIBLE_SIZE;

pub(crate) type DiskError = crate::storage::DiskError;
pub(crate) type DynReader = crate::storage::DynReader;
pub(crate) type DynReplicationPool = crate::storage::DynReplicationPool;
pub(crate) type ECStore = crate::storage::ECStore;
pub(crate) type EndpointServerPools = crate::storage::EndpointServerPools;
pub(crate) type HashReader = crate::storage::HashReader;
pub(crate) type NotificationSys = crate::storage::NotificationSys;
pub(crate) type BucketBandwidthMonitor = crate::storage::BucketBandwidthMonitor;
pub(crate) type ObjectStoreResolver = crate::storage::ObjectStoreResolver;
pub(crate) type ObjectInfo = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub(crate) type ObjectOptions = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub(crate) type PoolDecommissionInfo = ecstore_capacity::PoolDecommissionInfo;
pub(crate) type PoolStatus = ecstore_capacity::PoolStatus;
pub(crate) type StorageError = crate::storage::StorageError;
pub(crate) type Error = StorageError;
pub(crate) type TierConfigMgr = crate::storage::TierConfigMgr;
pub(crate) type WriteEncryption = crate::storage::WriteEncryption;
pub(crate) type WritePlan = crate::storage::WritePlan;

#[cfg(test)]
pub(crate) type DecryptReader<R> = crate::storage::DecryptReader<R>;
#[cfg(test)]
pub(crate) type EncryptReader<R> = crate::storage::EncryptReader<R>;
#[cfg(test)]
pub(crate) type Endpoint = crate::storage::Endpoint;
#[cfg(test)]
pub(crate) type Endpoints = crate::storage::Endpoints;
#[cfg(test)]
pub(crate) type HardLimitReader<R> = crate::storage::HardLimitReader<R>;
#[cfg(test)]
pub(crate) type PoolEndpoints = crate::storage::PoolEndpoints;
#[cfg(test)]
pub(crate) type TierConfig = ecstore_tier::tier_config::TierConfig;
#[cfg(test)]
pub(crate) type TierType = ecstore_tier::tier_config::TierType;
#[cfg(test)]
pub(crate) use ecstore_tier::warm_backend::WarmBackend as AppWarmBackend;
#[cfg(test)]
pub(crate) type WarmBackendGetOpts = ecstore_tier::warm_backend::WarmBackendGetOpts;

#[cfg(test)]
#[allow(non_snake_case)]
pub(crate) fn EndpointServerPools(pools: Vec<PoolEndpoints>) -> EndpointServerPools {
    crate::storage::EndpointServerPools::from(pools)
}

pub(crate) trait AppObjectLockConfigExt {
    fn enabled(&self) -> bool;
}

impl AppObjectLockConfigExt for s3s::dto::ObjectLockConfiguration {
    fn enabled(&self) -> bool {
        <s3s::dto::ObjectLockConfiguration as ecstore_bucket::object_lock::ObjectLockApi>::enabled(self)
    }
}

pub(crate) trait AppReplicationConfigExt {
    fn filter_target_arns(&self, obj: &replication::ObjectOpts) -> Vec<String>;
    fn replicate(&self, opts: &replication::ObjectOpts) -> bool;
}

impl AppReplicationConfigExt for s3s::dto::ReplicationConfiguration {
    fn filter_target_arns(&self, obj: &replication::ObjectOpts) -> Vec<String> {
        <s3s::dto::ReplicationConfiguration as ecstore_bucket::replication::ReplicationConfigurationExt>::filter_target_arns(
            self, obj,
        )
    }

    fn replicate(&self, opts: &replication::ObjectOpts) -> bool {
        <s3s::dto::ReplicationConfiguration as ecstore_bucket::replication::ReplicationConfigurationExt>::replicate(self, opts)
    }
}

pub(crate) trait AppVersioningConfigExt {
    fn prefix_enabled(&self, prefix: &str) -> bool;
    fn suspended(&self) -> bool;
}

impl AppVersioningConfigExt for s3s::dto::VersioningConfiguration {
    fn prefix_enabled(&self, prefix: &str) -> bool {
        <s3s::dto::VersioningConfiguration as ecstore_bucket::versioning::VersioningApi>::prefix_enabled(self, prefix)
    }

    fn suspended(&self) -> bool {
        <s3s::dto::VersioningConfiguration as ecstore_bucket::versioning::VersioningApi>::suspended(self)
    }
}

pub(crate) async fn predict_lifecycle_expiration(
    lifecycle: &s3s::dto::BucketLifecycleConfiguration,
    obj: &lifecycle::lifecycle::ObjectOpts,
) -> lifecycle::lifecycle::Event {
    ecstore_bucket::lifecycle::lifecycle::Lifecycle::predict_expiration(lifecycle, obj).await
}

pub(crate) fn validate_restore_request(request: &s3s::dto::RestoreRequest, api: Arc<ECStore>) -> std::io::Result<()> {
    <s3s::dto::RestoreRequest as ecstore_bucket::lifecycle::bucket_lifecycle_ops::RestoreRequestOps>::validate(request, api)
}

pub(crate) async fn get_server_info(get_pools: bool) -> rustfs_madmin::InfoMessage {
    ecstore_admin::get_server_info(get_pools).await
}

pub(crate) mod bucket_target_sys {
    pub(crate) type BucketTargetSys = super::ecstore_bucket::bucket_target_sys::BucketTargetSys;
}

pub(crate) mod lifecycle {
    use super::ECStore;

    pub(crate) mod bucket_lifecycle_audit {
        pub(crate) type LcEventSrc = super::super::ecstore_bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
    }

    pub(crate) mod bucket_lifecycle_ops {
        use std::ops::Deref;
        use std::sync::Arc;

        use super::ECStore;
        use super::bucket_lifecycle_audit::LcEventSrc;

        pub(crate) type ExpiryState = super::super::ecstore_bucket::lifecycle::bucket_lifecycle_ops::ExpiryState;

        pub(crate) struct GlobalExpiryStateCompat;

        #[allow(non_upper_case_globals)]
        pub(crate) static GLOBAL_ExpiryState: GlobalExpiryStateCompat = GlobalExpiryStateCompat;

        impl Deref for GlobalExpiryStateCompat {
            type Target = Arc<tokio::sync::RwLock<ExpiryState>>;

            fn deref(&self) -> &Self::Target {
                &super::super::ecstore_bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_ExpiryState
            }
        }

        #[cfg(test)]
        pub(crate) async fn init_background_expiry(api: Arc<ECStore>) {
            super::super::ecstore_bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry(api).await;
        }

        pub(crate) async fn enqueue_expiry_for_existing_objects(
            api: Arc<ECStore>,
            bucket: &str,
        ) -> Result<(), super::super::Error> {
            super::super::ecstore_bucket::lifecycle::bucket_lifecycle_ops::enqueue_expiry_for_existing_objects(api, bucket).await
        }

        pub(crate) async fn enqueue_transition_for_existing_objects(
            api: Arc<ECStore>,
            bucket: &str,
        ) -> Result<(), super::super::Error> {
            super::super::ecstore_bucket::lifecycle::bucket_lifecycle_ops::enqueue_transition_for_existing_objects(api, bucket)
                .await
        }

        pub(crate) async fn enqueue_transition_immediate(oi: &super::super::ObjectInfo, src: LcEventSrc) {
            super::super::ecstore_bucket::lifecycle::bucket_lifecycle_ops::enqueue_transition_immediate(oi, src).await;
        }

        pub(crate) async fn post_restore_opts(
            version_id: &str,
            bucket: &str,
            object: &str,
        ) -> Result<super::super::ObjectOptions, std::io::Error> {
            super::super::ecstore_bucket::lifecycle::bucket_lifecycle_ops::post_restore_opts(version_id, bucket, object).await
        }

        pub(crate) async fn validate_transition_tier(lc: &s3s::dto::BucketLifecycleConfiguration) -> Result<(), std::io::Error> {
            super::super::ecstore_bucket::lifecycle::bucket_lifecycle_ops::validate_transition_tier(lc).await
        }

        pub(crate) async fn validate_lifecycle_config(
            lc: &s3s::dto::BucketLifecycleConfiguration,
            lock_config: &s3s::dto::ObjectLockConfiguration,
        ) -> Result<(), std::io::Error> {
            use super::super::ecstore_bucket::lifecycle::lifecycle::Lifecycle as _;

            lc.validate(lock_config).await
        }
    }

    pub(crate) mod lifecycle_contract {
        #[cfg(test)]
        pub(crate) type IlmAction = super::super::ecstore_bucket::lifecycle::lifecycle::IlmAction;
        pub(crate) type Event = super::super::ecstore_bucket::lifecycle::lifecycle::Event;
        pub(crate) type ObjectOpts = super::super::ecstore_bucket::lifecycle::lifecycle::ObjectOpts;
        pub(crate) type TransitionOptions = super::super::ecstore_bucket::lifecycle::lifecycle::TransitionOptions;

        pub(crate) const TRANSITION_COMPLETE: &str = super::super::ecstore_bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;

        pub(crate) fn expected_expiry_time(mod_time: time::OffsetDateTime, days: i32) -> time::OffsetDateTime {
            super::super::ecstore_bucket::lifecycle::lifecycle::expected_expiry_time(mod_time, days)
        }
    }
    pub(crate) use lifecycle_contract as lifecycle;

    pub(crate) mod tier_delete_journal {
        use std::sync::Arc;

        use super::ECStore;

        pub(crate) async fn persist_tier_delete_journal_entry(
            api: Arc<ECStore>,
            je: &super::tier_sweeper::Jentry,
        ) -> std::io::Result<()> {
            super::super::ecstore_bucket::lifecycle::tier_delete_journal::persist_tier_delete_journal_entry(api, je).await
        }
    }

    pub(crate) mod tier_sweeper {
        pub(crate) type Jentry = super::super::ecstore_bucket::lifecycle::tier_sweeper::Jentry;

        pub(crate) fn transitioned_delete_journal_entry(
            version_id: Option<uuid::Uuid>,
            versioned: bool,
            suspended: bool,
            transitioned: &rustfs_storage_api::TransitionedObject,
        ) -> Option<Jentry> {
            super::super::ecstore_bucket::lifecycle::tier_sweeper::transitioned_delete_journal_entry(
                version_id,
                versioned,
                suspended,
                transitioned,
            )
        }

        pub(crate) fn transitioned_force_delete_journal_entry(
            transitioned: &rustfs_storage_api::TransitionedObject,
        ) -> Option<Jentry> {
            super::super::ecstore_bucket::lifecycle::tier_sweeper::transitioned_force_delete_journal_entry(transitioned)
        }
    }
}

pub(crate) mod metadata {
    pub(crate) const BUCKET_CORS_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_CORS_CONFIG;
    pub(crate) const BUCKET_LIFECYCLE_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
    pub(crate) const BUCKET_NOTIFICATION_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_NOTIFICATION_CONFIG;
    pub(crate) const BUCKET_POLICY_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG;
    pub(crate) const BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG;
    pub(crate) const BUCKET_REPLICATION_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_REPLICATION_CONFIG;
    pub(crate) const BUCKET_SSECONFIG: &str = super::ecstore_bucket::metadata::BUCKET_SSECONFIG;
    pub(crate) const BUCKET_TAGGING_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_TAGGING_CONFIG;
    pub(crate) const BUCKET_TARGETS_FILE: &str = super::ecstore_bucket::metadata::BUCKET_TARGETS_FILE;
    pub(crate) const BUCKET_VERSIONING_CONFIG: &str = super::ecstore_bucket::metadata::BUCKET_VERSIONING_CONFIG;
    #[cfg(test)]
    pub(crate) const OBJECT_LOCK_CONFIG: &str = super::ecstore_bucket::metadata::OBJECT_LOCK_CONFIG;
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

    #[cfg(test)]
    use super::ECStore;
    use super::Error;
    use super::target::BucketTargets;

    pub(crate) type BucketMetadataSys = super::ecstore_bucket::metadata_sys::BucketMetadataSys;

    #[cfg(test)]
    pub(crate) async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
        super::ecstore_bucket::metadata_sys::init_bucket_metadata_sys(api, buckets).await;
    }

    pub(crate) async fn delete(bucket: &str, config_file: &str) -> Result<OffsetDateTime, Error> {
        super::ecstore_bucket::metadata_sys::delete(bucket, config_file).await
    }

    pub(crate) async fn get_bucket_policy(bucket: &str) -> Result<(BucketPolicy, OffsetDateTime), Error> {
        super::ecstore_bucket::metadata_sys::get_bucket_policy(bucket).await
    }

    pub(crate) async fn get_bucket_policy_raw(bucket: &str) -> Result<(String, OffsetDateTime), Error> {
        super::ecstore_bucket::metadata_sys::get_bucket_policy_raw(bucket).await
    }

    pub(crate) async fn get_bucket_targets_config(bucket: &str) -> Result<BucketTargets, Error> {
        super::ecstore_bucket::metadata_sys::get_bucket_targets_config(bucket).await
    }

    pub(crate) async fn get_cors_config(bucket: &str) -> Result<(CORSConfiguration, OffsetDateTime), Error> {
        super::ecstore_bucket::metadata_sys::get_cors_config(bucket).await
    }

    pub(crate) fn get_global_bucket_metadata_sys() -> Option<Arc<RwLock<BucketMetadataSys>>> {
        super::ecstore_bucket::metadata_sys::get_global_bucket_metadata_sys()
    }

    pub(crate) async fn get_lifecycle_config(bucket: &str) -> Result<(BucketLifecycleConfiguration, OffsetDateTime), Error> {
        super::ecstore_bucket::metadata_sys::get_lifecycle_config(bucket).await
    }

    pub(crate) async fn get_notification_config(bucket: &str) -> Result<Option<NotificationConfiguration>, Error> {
        super::ecstore_bucket::metadata_sys::get_notification_config(bucket).await
    }

    pub(crate) async fn get_object_lock_config(bucket: &str) -> Result<(ObjectLockConfiguration, OffsetDateTime), Error> {
        super::ecstore_bucket::metadata_sys::get_object_lock_config(bucket).await
    }

    pub(crate) async fn get_public_access_block_config(
        bucket: &str,
    ) -> Result<(PublicAccessBlockConfiguration, OffsetDateTime), Error> {
        super::ecstore_bucket::metadata_sys::get_public_access_block_config(bucket).await
    }

    pub(crate) async fn get_replication_config(
        bucket: &str,
    ) -> Result<(s3s::dto::ReplicationConfiguration, OffsetDateTime), Error> {
        super::ecstore_bucket::metadata_sys::get_replication_config(bucket).await
    }

    pub(crate) async fn get_sse_config(bucket: &str) -> Result<(ServerSideEncryptionConfiguration, OffsetDateTime), Error> {
        super::ecstore_bucket::metadata_sys::get_sse_config(bucket).await
    }

    pub(crate) async fn get_tagging_config(bucket: &str) -> Result<(Tagging, OffsetDateTime), Error> {
        super::ecstore_bucket::metadata_sys::get_tagging_config(bucket).await
    }

    pub(crate) async fn update(bucket: &str, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime, Error> {
        super::ecstore_bucket::metadata_sys::update(bucket, config_file, data).await
    }
}

pub(crate) mod object_api_utils {
    pub(crate) fn to_s3s_etag(etag: &str) -> s3s::dto::ETag {
        crate::storage::to_s3s_etag(etag)
    }
}

pub(crate) mod object_lock {
    pub(crate) mod objectlock {
        pub(crate) fn get_object_legalhold_meta(
            meta: &std::collections::HashMap<String, String>,
        ) -> s3s::dto::ObjectLockLegalHold {
            super::super::ecstore_bucket::object_lock::objectlock::get_object_legalhold_meta(meta)
        }

        pub(crate) fn get_object_retention_meta(
            meta: &std::collections::HashMap<String, String>,
        ) -> s3s::dto::ObjectLockRetention {
            super::super::ecstore_bucket::object_lock::objectlock::get_object_retention_meta(meta)
        }
    }

    pub(crate) mod objectlock_sys {
        pub(crate) type BucketObjectLockSys = super::super::ecstore_bucket::object_lock::objectlock_sys::BucketObjectLockSys;
        pub(crate) type ObjectLockBlockReason = super::super::ecstore_bucket::object_lock::objectlock_sys::ObjectLockBlockReason;

        pub(crate) async fn check_object_lock_for_deletion(
            bucket: &str,
            obj_info: &super::super::ObjectInfo,
            bypass_governance: bool,
        ) -> Option<ObjectLockBlockReason> {
            super::super::ecstore_bucket::object_lock::objectlock_sys::check_object_lock_for_deletion(
                bucket,
                obj_info,
                bypass_governance,
            )
            .await
        }

        pub(crate) fn is_retention_active(mode: &str, retain_until_date: Option<&s3s::dto::Date>) -> bool {
            super::super::ecstore_bucket::object_lock::objectlock_sys::is_retention_active(mode, retain_until_date)
        }
    }
}

pub(crate) mod policy_sys {
    pub(crate) type PolicySys = super::ecstore_bucket::policy_sys::PolicySys;
}

pub(crate) mod quota {
    pub(crate) mod checker {
        pub(crate) type QuotaChecker = super::super::ecstore_bucket::quota::checker::QuotaChecker;
    }

    pub(crate) type QuotaOperation = super::ecstore_bucket::quota::QuotaOperation;
}

pub(crate) mod replication {
    use std::collections::HashMap;
    use std::sync::Arc;

    pub(crate) type DeletedObjectReplicationInfo = super::ecstore_bucket::replication::DeletedObjectReplicationInfo;
    pub(crate) type MustReplicateOptions = super::ecstore_bucket::replication::MustReplicateOptions;
    pub(crate) type ObjectOpts = super::ecstore_bucket::replication::ObjectOpts;
    pub(crate) type ReplicateDecision = rustfs_filemeta::ReplicateDecision;

    pub(crate) async fn check_replicate_delete(
        bucket: &str,
        dobj: &rustfs_storage_api::ObjectToDelete,
        oi: &super::ObjectInfo,
        del_opts: &super::ObjectOptions,
        gerr: Option<String>,
    ) -> ReplicateDecision {
        super::ecstore_bucket::replication::check_replicate_delete(bucket, dobj, oi, del_opts, gerr).await
    }

    pub(crate) fn get_must_replicate_options(
        user_defined: &HashMap<String, String>,
        user_tags: String,
        status: rustfs_filemeta::ReplicationStatusType,
        op_type: rustfs_filemeta::ReplicationType,
        opts: super::ObjectOptions,
    ) -> MustReplicateOptions {
        super::ecstore_bucket::replication::get_must_replicate_options(user_defined, user_tags, status, op_type, opts)
    }

    pub(crate) async fn must_replicate(bucket: &str, object: &str, mopts: MustReplicateOptions) -> ReplicateDecision {
        super::ecstore_bucket::replication::must_replicate(bucket, object, mopts).await
    }

    pub(crate) async fn schedule_replication(
        oi: super::ObjectInfo,
        store: Arc<super::ECStore>,
        dsc: ReplicateDecision,
        op_type: rustfs_filemeta::ReplicationType,
    ) {
        super::ecstore_bucket::replication::schedule_replication(oi, store, dsc, op_type).await;
    }

    pub(crate) async fn schedule_replication_delete(dv: DeletedObjectReplicationInfo) {
        super::ecstore_bucket::replication::schedule_replication_delete(dv).await;
    }
}

pub(crate) mod tagging {
    pub(crate) fn decode_tags(tags: &str) -> Vec<s3s::dto::Tag> {
        super::ecstore_bucket::tagging::decode_tags(tags)
    }
}

pub(crate) mod target {
    #[cfg(test)]
    pub(crate) type BucketTarget = super::ecstore_bucket::target::BucketTarget;
    pub(crate) type BucketTargetType = super::ecstore_bucket::target::BucketTargetType;
    pub(crate) type BucketTargets = super::ecstore_bucket::target::BucketTargets;
}

pub(crate) mod utils {
    pub(crate) fn serialize<T: s3s::xml::Serialize>(val: &T) -> s3s::xml::SerResult<Vec<u8>> {
        super::ecstore_bucket::utils::serialize(val)
    }
}

pub(crate) mod versioning {}

pub(crate) mod versioning_sys {
    pub(crate) type BucketVersioningSys = super::ecstore_bucket::versioning_sys::BucketVersioningSys;
}

#[cfg(test)]
pub(crate) mod transition_api {
    pub(crate) type ReadCloser = super::ecstore_client::transition_api::ReadCloser;
    pub(crate) type ReaderImpl = super::ecstore_client::transition_api::ReaderImpl;
}

pub(crate) mod storageclass {
    pub(crate) const STANDARD: &str = super::ecstore_config::storageclass::STANDARD;
    #[cfg(test)]
    pub(crate) const STANDARD_IA: &str = super::ecstore_config::storageclass::STANDARD_IA;
}

pub(crate) fn get_total_usable_capacity(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
    ecstore_capacity::get_total_usable_capacity(disks, info)
}

pub(crate) fn get_total_usable_capacity_free(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
    ecstore_capacity::get_total_usable_capacity_free(disks, info)
}

pub(crate) fn is_disk_compressible(headers: &http::HeaderMap, object_name: &str) -> bool {
    ecstore_compression::is_disk_compressible(headers, object_name)
}

pub(crate) async fn apply_bucket_usage_memory_overlay(data_usage_info: &mut rustfs_data_usage::DataUsageInfo) {
    ecstore_data_usage::apply_bucket_usage_memory_overlay(data_usage_info).await;
}

pub(crate) async fn load_data_usage_from_backend(
    store: Arc<ECStore>,
) -> std::result::Result<rustfs_data_usage::DataUsageInfo, Error> {
    ecstore_data_usage::load_data_usage_from_backend(store).await
}

pub(crate) async fn record_bucket_object_delete_memory(bucket: &str, deleted_size: u64, removed_current_object: bool) {
    ecstore_data_usage::record_bucket_object_delete_memory(bucket, deleted_size, removed_current_object).await;
}

pub(crate) async fn record_bucket_object_write_memory(bucket: &str, previous_current_size: Option<u64>, new_size: u64) {
    ecstore_data_usage::record_bucket_object_write_memory(bucket, previous_current_size, new_size).await;
}

pub(crate) fn is_all_buckets_not_found(errs: &[Option<DiskError>]) -> bool {
    crate::storage::is_all_buckets_not_found(errs)
}

pub(crate) fn is_err_bucket_not_found(err: &Error) -> bool {
    crate::storage::is_err_bucket_not_found(err)
}

pub(crate) fn is_err_object_not_found(err: &Error) -> bool {
    crate::storage::is_err_object_not_found(err)
}

pub(crate) fn is_err_version_not_found(err: &Error) -> bool {
    crate::storage::is_err_version_not_found(err)
}

#[cfg(test)]
pub(crate) struct GlobalTierConfigMgrCompat;

#[cfg(test)]
#[allow(non_upper_case_globals)]
pub(crate) static GLOBAL_TierConfigMgr: GlobalTierConfigMgrCompat = GlobalTierConfigMgrCompat;

#[cfg(test)]
impl std::ops::Deref for GlobalTierConfigMgrCompat {
    type Target = Arc<tokio::sync::RwLock<TierConfigMgr>>;

    fn deref(&self) -> &Self::Target {
        &ecstore_global::GLOBAL_TierConfigMgr
    }
}

pub(crate) fn get_global_endpoints_opt() -> Option<EndpointServerPools> {
    crate::storage::get_global_endpoints_opt()
}

pub(crate) fn get_global_deployment_id() -> Option<String> {
    crate::storage::get_global_deployment_id()
}

pub(crate) fn get_global_lock_client() -> Option<Arc<dyn rustfs_lock::client::LockClient>> {
    crate::storage::get_global_lock_client()
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

pub(crate) fn new_object_layer_fn() -> Option<Arc<ECStore>> {
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

pub(crate) type ReplicationStats = crate::storage::ReplicationStats;

pub(crate) fn get_global_replication_stats() -> Option<Arc<ReplicationStats>> {
    crate::storage::get_global_replication_stats()
}

#[cfg(test)]
pub(crate) fn boxed_reader<R>(reader: R) -> DynReader
where
    R: crate::storage::ecstore_rio::Reader + 'static,
{
    crate::storage::boxed_reader(reader)
}

pub(crate) fn compression_metadata_value(algorithm: rustfs_utils::CompressionAlgorithm) -> String {
    crate::storage::compression_metadata_value(algorithm)
}

pub(crate) fn wrap_reader<R>(reader: R) -> DynReader
where
    R: crate::storage::ecstore_rio::ReadStream + 'static,
{
    crate::storage::wrap_reader(reader)
}

pub(crate) fn get_lock_acquire_timeout() -> tokio::time::Duration {
    crate::storage::get_lock_acquire_timeout()
}

pub(crate) fn is_valid_storage_class(storage_class: &str) -> bool {
    crate::storage::is_valid_storage_class(storage_class)
}

#[cfg(test)]
pub(crate) async fn init_local_disks(endpoint_pools: EndpointServerPools) -> Result<(), Error> {
    crate::storage::init_local_disks(endpoint_pools).await
}
