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

pub(crate) const MIN_DISK_COMPRESSIBLE_SIZE: usize = rustfs_ecstore::api::compression::MIN_DISK_COMPRESSIBLE_SIZE;

pub(crate) type DiskError = rustfs_ecstore::api::disk::error::DiskError;
pub(crate) type DynReader = rustfs_ecstore::api::rio::DynReader;
pub(crate) type ECStore = rustfs_ecstore::api::storage::ECStore;
pub(crate) type EndpointServerPools = rustfs_ecstore::api::layout::EndpointServerPools;
pub(crate) type Error = rustfs_ecstore::api::error::Error;
pub(crate) type HashReader = rustfs_ecstore::api::rio::HashReader;
pub(crate) type ObjectStoreResolver = dyn Fn() -> Option<Arc<ECStore>> + Send + Sync + 'static;
pub(crate) type PoolDecommissionInfo = rustfs_ecstore::api::capacity::PoolDecommissionInfo;
pub(crate) type PoolStatus = rustfs_ecstore::api::capacity::PoolStatus;
pub(crate) type StorageError = rustfs_ecstore::api::error::StorageError;
pub(crate) type TierConfigMgr = rustfs_ecstore::api::tier::tier::TierConfigMgr;
pub(crate) type WriteEncryption = rustfs_ecstore::api::rio::WriteEncryption;
pub(crate) type WritePlan = rustfs_ecstore::api::rio::WritePlan;

#[cfg(test)]
pub(crate) type DecryptReader<R> = rustfs_ecstore::api::rio::DecryptReader<R>;
#[cfg(test)]
pub(crate) type EncryptReader<R> = rustfs_ecstore::api::rio::EncryptReader<R>;
#[cfg(test)]
pub(crate) type Endpoint = rustfs_ecstore::api::disk::endpoint::Endpoint;
#[cfg(test)]
pub(crate) type Endpoints = rustfs_ecstore::api::layout::Endpoints;
#[cfg(test)]
pub(crate) type HardLimitReader<R> = rustfs_ecstore::api::rio::HardLimitReader<R>;
#[cfg(test)]
pub(crate) type PoolEndpoints = rustfs_ecstore::api::layout::PoolEndpoints;
#[cfg(test)]
pub(crate) type TierConfig = rustfs_ecstore::api::tier::tier_config::TierConfig;
#[cfg(test)]
pub(crate) type TierType = rustfs_ecstore::api::tier::tier_config::TierType;
#[cfg(test)]
pub(crate) type WarmBackendGetOpts = rustfs_ecstore::api::tier::warm_backend::WarmBackendGetOpts;

#[cfg(test)]
#[allow(non_snake_case)]
pub(crate) fn EndpointServerPools(pools: Vec<PoolEndpoints>) -> EndpointServerPools {
    rustfs_ecstore::api::layout::EndpointServerPools::from(pools)
}

pub(crate) async fn get_server_info(get_pools: bool) -> rustfs_madmin::InfoMessage {
    rustfs_ecstore::api::admin::get_server_info(get_pools).await
}

pub(crate) mod bucket_target_sys {
    pub(crate) type BucketTargetSys = rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetSys;
}

pub(crate) mod lifecycle {
    use super::ECStore;

    pub(crate) mod bucket_lifecycle_audit {
        pub(crate) type LcEventSrc = rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
    }

    pub(crate) mod bucket_lifecycle_ops {
        use std::ops::Deref;
        use std::sync::Arc;

        use super::ECStore;
        use super::bucket_lifecycle_audit::LcEventSrc;

        pub(crate) type ExpiryState = rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::ExpiryState;

        pub(crate) struct GlobalExpiryStateCompat;

        #[allow(non_upper_case_globals)]
        pub(crate) static GLOBAL_ExpiryState: GlobalExpiryStateCompat = GlobalExpiryStateCompat;

        impl Deref for GlobalExpiryStateCompat {
            type Target = Arc<tokio::sync::RwLock<ExpiryState>>;

            fn deref(&self) -> &Self::Target {
                &rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_ExpiryState
            }
        }

        #[cfg(test)]
        pub(crate) async fn init_background_expiry(api: Arc<ECStore>) {
            rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry(api).await;
        }

        pub(crate) async fn enqueue_expiry_for_existing_objects(
            api: Arc<ECStore>,
            bucket: &str,
        ) -> Result<(), super::super::Error> {
            rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::enqueue_expiry_for_existing_objects(api, bucket).await
        }

        pub(crate) async fn enqueue_transition_for_existing_objects(
            api: Arc<ECStore>,
            bucket: &str,
        ) -> Result<(), super::super::Error> {
            rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::enqueue_transition_for_existing_objects(api, bucket)
                .await
        }

        pub(crate) async fn enqueue_transition_immediate(oi: &rustfs_ecstore::api::object::ObjectInfo, src: LcEventSrc) {
            rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::enqueue_transition_immediate(oi, src).await;
        }

        pub(crate) async fn post_restore_opts(
            version_id: &str,
            bucket: &str,
            object: &str,
        ) -> Result<rustfs_ecstore::api::object::ObjectOptions, std::io::Error> {
            rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::post_restore_opts(version_id, bucket, object).await
        }

        pub(crate) async fn validate_transition_tier(lc: &s3s::dto::BucketLifecycleConfiguration) -> Result<(), std::io::Error> {
            rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::validate_transition_tier(lc).await
        }

        pub(crate) async fn validate_lifecycle_config(
            lc: &s3s::dto::BucketLifecycleConfiguration,
            lock_config: &s3s::dto::ObjectLockConfiguration,
        ) -> Result<(), std::io::Error> {
            use rustfs_ecstore::api::bucket::lifecycle::lifecycle::Lifecycle as _;

            lc.validate(lock_config).await
        }
    }

    pub(crate) mod lifecycle_contract {
        #[cfg(test)]
        pub(crate) type IlmAction = rustfs_ecstore::api::bucket::lifecycle::lifecycle::IlmAction;
        pub(crate) type Event = rustfs_ecstore::api::bucket::lifecycle::lifecycle::Event;
        pub(crate) type ObjectOpts = rustfs_ecstore::api::bucket::lifecycle::lifecycle::ObjectOpts;
        pub(crate) type TransitionOptions = rustfs_ecstore::api::bucket::lifecycle::lifecycle::TransitionOptions;

        pub(crate) const TRANSITION_COMPLETE: &str = rustfs_ecstore::api::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;

        pub(crate) fn expected_expiry_time(mod_time: time::OffsetDateTime, days: i32) -> time::OffsetDateTime {
            rustfs_ecstore::api::bucket::lifecycle::lifecycle::expected_expiry_time(mod_time, days)
        }
    }
    pub(crate) use lifecycle_contract as lifecycle;

    pub(crate) mod tier_delete_journal {
        use std::sync::Arc;

        use super::ECStore;

        pub(crate) async fn persist_tier_delete_journal_entry(
            api: Arc<ECStore>,
            je: &rustfs_ecstore::api::bucket::lifecycle::tier_sweeper::Jentry,
        ) -> std::io::Result<()> {
            rustfs_ecstore::api::bucket::lifecycle::tier_delete_journal::persist_tier_delete_journal_entry(api, je).await
        }
    }

    pub(crate) mod tier_sweeper {
        pub(crate) type Jentry = rustfs_ecstore::api::bucket::lifecycle::tier_sweeper::Jentry;

        pub(crate) fn transitioned_delete_journal_entry(
            version_id: Option<uuid::Uuid>,
            versioned: bool,
            suspended: bool,
            transitioned: &rustfs_storage_api::TransitionedObject,
        ) -> Option<Jentry> {
            rustfs_ecstore::api::bucket::lifecycle::tier_sweeper::transitioned_delete_journal_entry(
                version_id,
                versioned,
                suspended,
                transitioned,
            )
        }

        pub(crate) fn transitioned_force_delete_journal_entry(
            transitioned: &rustfs_storage_api::TransitionedObject,
        ) -> Option<Jentry> {
            rustfs_ecstore::api::bucket::lifecycle::tier_sweeper::transitioned_force_delete_journal_entry(transitioned)
        }
    }
}

pub(crate) mod metadata {
    pub(crate) const BUCKET_CORS_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_CORS_CONFIG;
    pub(crate) const BUCKET_LIFECYCLE_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
    pub(crate) const BUCKET_NOTIFICATION_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_NOTIFICATION_CONFIG;
    pub(crate) const BUCKET_POLICY_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_POLICY_CONFIG;
    pub(crate) const BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG: &str =
        rustfs_ecstore::api::bucket::metadata::BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG;
    pub(crate) const BUCKET_REPLICATION_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_REPLICATION_CONFIG;
    pub(crate) const BUCKET_SSECONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_SSECONFIG;
    pub(crate) const BUCKET_TAGGING_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_TAGGING_CONFIG;
    pub(crate) const BUCKET_TARGETS_FILE: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_TARGETS_FILE;
    pub(crate) const BUCKET_VERSIONING_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::BUCKET_VERSIONING_CONFIG;
    #[cfg(test)]
    pub(crate) const OBJECT_LOCK_CONFIG: &str = rustfs_ecstore::api::bucket::metadata::OBJECT_LOCK_CONFIG;
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

    pub(crate) type BucketMetadataSys = rustfs_ecstore::api::bucket::metadata_sys::BucketMetadataSys;

    #[cfg(test)]
    pub(crate) async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
        rustfs_ecstore::api::bucket::metadata_sys::init_bucket_metadata_sys(api, buckets).await;
    }

    pub(crate) async fn delete(bucket: &str, config_file: &str) -> Result<OffsetDateTime, Error> {
        rustfs_ecstore::api::bucket::metadata_sys::delete(bucket, config_file).await
    }

    pub(crate) async fn get_bucket_policy(bucket: &str) -> Result<(BucketPolicy, OffsetDateTime), Error> {
        rustfs_ecstore::api::bucket::metadata_sys::get_bucket_policy(bucket).await
    }

    pub(crate) async fn get_bucket_policy_raw(bucket: &str) -> Result<(String, OffsetDateTime), Error> {
        rustfs_ecstore::api::bucket::metadata_sys::get_bucket_policy_raw(bucket).await
    }

    pub(crate) async fn get_bucket_targets_config(bucket: &str) -> Result<BucketTargets, Error> {
        rustfs_ecstore::api::bucket::metadata_sys::get_bucket_targets_config(bucket).await
    }

    pub(crate) async fn get_cors_config(bucket: &str) -> Result<(CORSConfiguration, OffsetDateTime), Error> {
        rustfs_ecstore::api::bucket::metadata_sys::get_cors_config(bucket).await
    }

    pub(crate) fn get_global_bucket_metadata_sys() -> Option<Arc<RwLock<BucketMetadataSys>>> {
        rustfs_ecstore::api::bucket::metadata_sys::get_global_bucket_metadata_sys()
    }

    pub(crate) async fn get_lifecycle_config(bucket: &str) -> Result<(BucketLifecycleConfiguration, OffsetDateTime), Error> {
        rustfs_ecstore::api::bucket::metadata_sys::get_lifecycle_config(bucket).await
    }

    pub(crate) async fn get_notification_config(bucket: &str) -> Result<Option<NotificationConfiguration>, Error> {
        rustfs_ecstore::api::bucket::metadata_sys::get_notification_config(bucket).await
    }

    pub(crate) async fn get_object_lock_config(bucket: &str) -> Result<(ObjectLockConfiguration, OffsetDateTime), Error> {
        rustfs_ecstore::api::bucket::metadata_sys::get_object_lock_config(bucket).await
    }

    pub(crate) async fn get_public_access_block_config(
        bucket: &str,
    ) -> Result<(PublicAccessBlockConfiguration, OffsetDateTime), Error> {
        rustfs_ecstore::api::bucket::metadata_sys::get_public_access_block_config(bucket).await
    }

    pub(crate) async fn get_replication_config(
        bucket: &str,
    ) -> Result<(s3s::dto::ReplicationConfiguration, OffsetDateTime), Error> {
        rustfs_ecstore::api::bucket::metadata_sys::get_replication_config(bucket).await
    }

    pub(crate) async fn get_sse_config(bucket: &str) -> Result<(ServerSideEncryptionConfiguration, OffsetDateTime), Error> {
        rustfs_ecstore::api::bucket::metadata_sys::get_sse_config(bucket).await
    }

    pub(crate) async fn get_tagging_config(bucket: &str) -> Result<(Tagging, OffsetDateTime), Error> {
        rustfs_ecstore::api::bucket::metadata_sys::get_tagging_config(bucket).await
    }

    pub(crate) async fn update(bucket: &str, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime, Error> {
        rustfs_ecstore::api::bucket::metadata_sys::update(bucket, config_file, data).await
    }
}

pub(crate) mod object_api_utils {
    pub(crate) fn to_s3s_etag(etag: &str) -> s3s::dto::ETag {
        rustfs_ecstore::api::client::object_api_utils::to_s3s_etag(etag)
    }
}

pub(crate) mod object_lock {
    pub(crate) mod objectlock {
        pub(crate) fn get_object_legalhold_meta(
            meta: &std::collections::HashMap<String, String>,
        ) -> s3s::dto::ObjectLockLegalHold {
            rustfs_ecstore::api::bucket::object_lock::objectlock::get_object_legalhold_meta(meta)
        }

        pub(crate) fn get_object_retention_meta(
            meta: &std::collections::HashMap<String, String>,
        ) -> s3s::dto::ObjectLockRetention {
            rustfs_ecstore::api::bucket::object_lock::objectlock::get_object_retention_meta(meta)
        }
    }

    pub(crate) mod objectlock_sys {
        pub(crate) type BucketObjectLockSys = rustfs_ecstore::api::bucket::object_lock::objectlock_sys::BucketObjectLockSys;
        pub(crate) type ObjectLockBlockReason = rustfs_ecstore::api::bucket::object_lock::objectlock_sys::ObjectLockBlockReason;

        pub(crate) async fn check_object_lock_for_deletion(
            bucket: &str,
            obj_info: &rustfs_ecstore::api::object::ObjectInfo,
            bypass_governance: bool,
        ) -> Option<ObjectLockBlockReason> {
            rustfs_ecstore::api::bucket::object_lock::objectlock_sys::check_object_lock_for_deletion(
                bucket,
                obj_info,
                bypass_governance,
            )
            .await
        }

        pub(crate) fn is_retention_active(mode: &str, retain_until_date: Option<&s3s::dto::Date>) -> bool {
            rustfs_ecstore::api::bucket::object_lock::objectlock_sys::is_retention_active(mode, retain_until_date)
        }
    }
}

pub(crate) mod policy_sys {
    pub(crate) type PolicySys = rustfs_ecstore::api::bucket::policy_sys::PolicySys;
}

pub(crate) mod quota {
    pub(crate) mod checker {
        pub(crate) type QuotaChecker = rustfs_ecstore::api::bucket::quota::checker::QuotaChecker;
    }

    pub(crate) type QuotaOperation = rustfs_ecstore::api::bucket::quota::QuotaOperation;
}

pub(crate) mod replication {
    use std::collections::HashMap;
    use std::sync::Arc;

    pub(crate) type DeletedObjectReplicationInfo = rustfs_ecstore::api::bucket::replication::DeletedObjectReplicationInfo;
    pub(crate) type MustReplicateOptions = rustfs_ecstore::api::bucket::replication::MustReplicateOptions;
    pub(crate) type ObjectOpts = rustfs_ecstore::api::bucket::replication::ObjectOpts;
    pub(crate) type ReplicateDecision = rustfs_filemeta::ReplicateDecision;

    pub(crate) async fn check_replicate_delete(
        bucket: &str,
        dobj: &rustfs_storage_api::ObjectToDelete,
        oi: &rustfs_ecstore::api::object::ObjectInfo,
        del_opts: &rustfs_ecstore::api::object::ObjectOptions,
        gerr: Option<String>,
    ) -> ReplicateDecision {
        rustfs_ecstore::api::bucket::replication::check_replicate_delete(bucket, dobj, oi, del_opts, gerr).await
    }

    pub(crate) fn get_must_replicate_options(
        user_defined: &HashMap<String, String>,
        user_tags: String,
        status: rustfs_filemeta::ReplicationStatusType,
        op_type: rustfs_filemeta::ReplicationType,
        opts: rustfs_ecstore::api::object::ObjectOptions,
    ) -> MustReplicateOptions {
        rustfs_ecstore::api::bucket::replication::get_must_replicate_options(user_defined, user_tags, status, op_type, opts)
    }

    pub(crate) async fn must_replicate(bucket: &str, object: &str, mopts: MustReplicateOptions) -> ReplicateDecision {
        rustfs_ecstore::api::bucket::replication::must_replicate(bucket, object, mopts).await
    }

    pub(crate) async fn schedule_replication(
        oi: rustfs_ecstore::api::object::ObjectInfo,
        store: Arc<super::ECStore>,
        dsc: ReplicateDecision,
        op_type: rustfs_filemeta::ReplicationType,
    ) {
        rustfs_ecstore::api::bucket::replication::schedule_replication(oi, store, dsc, op_type).await;
    }

    pub(crate) async fn schedule_replication_delete(dv: DeletedObjectReplicationInfo) {
        rustfs_ecstore::api::bucket::replication::schedule_replication_delete(dv).await;
    }
}

pub(crate) mod tagging {
    pub(crate) fn decode_tags(tags: &str) -> Vec<s3s::dto::Tag> {
        rustfs_ecstore::api::bucket::tagging::decode_tags(tags)
    }
}

pub(crate) mod target {
    #[cfg(test)]
    pub(crate) type BucketTarget = rustfs_ecstore::api::bucket::target::BucketTarget;
    pub(crate) type BucketTargetType = rustfs_ecstore::api::bucket::target::BucketTargetType;
    pub(crate) type BucketTargets = rustfs_ecstore::api::bucket::target::BucketTargets;
}

pub(crate) mod utils {
    pub(crate) fn serialize<T: s3s::xml::Serialize>(val: &T) -> s3s::xml::SerResult<Vec<u8>> {
        rustfs_ecstore::api::bucket::utils::serialize(val)
    }
}

pub(crate) mod versioning {}

pub(crate) mod versioning_sys {
    pub(crate) type BucketVersioningSys = rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys;
}

#[cfg(test)]
pub(crate) mod transition_api {
    pub(crate) type ReadCloser = rustfs_ecstore::api::client::transition_api::ReadCloser;
    pub(crate) type ReaderImpl = rustfs_ecstore::api::client::transition_api::ReaderImpl;
}

pub(crate) mod storageclass {
    pub(crate) const STANDARD: &str = rustfs_ecstore::api::config::storageclass::STANDARD;
    #[cfg(test)]
    pub(crate) const STANDARD_IA: &str = rustfs_ecstore::api::config::storageclass::STANDARD_IA;
}

pub(crate) fn get_total_usable_capacity(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
    rustfs_ecstore::api::capacity::get_total_usable_capacity(disks, info)
}

pub(crate) fn get_total_usable_capacity_free(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
    rustfs_ecstore::api::capacity::get_total_usable_capacity_free(disks, info)
}

pub(crate) fn is_disk_compressible(headers: &http::HeaderMap, object_name: &str) -> bool {
    rustfs_ecstore::api::compression::is_disk_compressible(headers, object_name)
}

pub(crate) async fn apply_bucket_usage_memory_overlay(data_usage_info: &mut rustfs_data_usage::DataUsageInfo) {
    rustfs_ecstore::api::data_usage::apply_bucket_usage_memory_overlay(data_usage_info).await;
}

pub(crate) async fn load_data_usage_from_backend(
    store: Arc<ECStore>,
) -> std::result::Result<rustfs_data_usage::DataUsageInfo, Error> {
    rustfs_ecstore::api::data_usage::load_data_usage_from_backend(store).await
}

pub(crate) async fn record_bucket_object_delete_memory(bucket: &str, deleted_size: u64, removed_current_object: bool) {
    rustfs_ecstore::api::data_usage::record_bucket_object_delete_memory(bucket, deleted_size, removed_current_object).await;
}

pub(crate) async fn record_bucket_object_write_memory(bucket: &str, previous_current_size: Option<u64>, new_size: u64) {
    rustfs_ecstore::api::data_usage::record_bucket_object_write_memory(bucket, previous_current_size, new_size).await;
}

pub(crate) fn is_all_buckets_not_found(errs: &[Option<DiskError>]) -> bool {
    rustfs_ecstore::api::disk::error_reduce::is_all_buckets_not_found(errs)
}

pub(crate) fn is_err_bucket_not_found(err: &Error) -> bool {
    rustfs_ecstore::api::error::is_err_bucket_not_found(err)
}

pub(crate) fn is_err_object_not_found(err: &Error) -> bool {
    rustfs_ecstore::api::error::is_err_object_not_found(err)
}

pub(crate) fn is_err_version_not_found(err: &Error) -> bool {
    rustfs_ecstore::api::error::is_err_version_not_found(err)
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
        &rustfs_ecstore::api::global::GLOBAL_TierConfigMgr
    }
}

pub(crate) fn get_global_endpoints_opt() -> Option<EndpointServerPools> {
    rustfs_ecstore::api::global::get_global_endpoints_opt()
}

pub(crate) fn get_global_region() -> Option<s3s::region::Region> {
    rustfs_ecstore::api::global::get_global_region()
}

pub(crate) fn get_global_tier_config_mgr() -> Arc<tokio::sync::RwLock<TierConfigMgr>> {
    rustfs_ecstore::api::global::get_global_tier_config_mgr()
}

pub(crate) fn new_object_layer_fn() -> Option<Arc<ECStore>> {
    rustfs_ecstore::api::global::new_object_layer_fn()
}

pub(crate) fn set_object_store_resolver(resolver: Arc<ObjectStoreResolver>) -> bool {
    rustfs_ecstore::api::global::set_object_store_resolver(resolver)
}

pub(crate) fn get_global_notification_sys() -> Option<&'static rustfs_ecstore::api::notification::NotificationSys> {
    rustfs_ecstore::api::notification::get_global_notification_sys()
}

#[cfg(test)]
pub(crate) fn boxed_reader<R>(reader: R) -> DynReader
where
    R: rustfs_ecstore::api::rio::Reader + 'static,
{
    rustfs_ecstore::api::rio::boxed_reader(reader)
}

pub(crate) fn compression_metadata_value(algorithm: rustfs_utils::CompressionAlgorithm) -> String {
    rustfs_ecstore::api::rio::compression_metadata_value(algorithm)
}

pub(crate) fn wrap_reader<R>(reader: R) -> DynReader
where
    R: rustfs_ecstore::api::rio::ReadStream + 'static,
{
    rustfs_ecstore::api::rio::wrap_reader(reader)
}

pub(crate) fn get_lock_acquire_timeout() -> tokio::time::Duration {
    rustfs_ecstore::api::set_disk::get_lock_acquire_timeout()
}

pub(crate) fn is_valid_storage_class(storage_class: &str) -> bool {
    rustfs_ecstore::api::set_disk::is_valid_storage_class(storage_class)
}

#[cfg(test)]
pub(crate) async fn init_local_disks(endpoint_pools: EndpointServerPools) -> Result<(), Error> {
    rustfs_ecstore::api::storage::init_local_disks(endpoint_pools).await
}
