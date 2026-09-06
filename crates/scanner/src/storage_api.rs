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

use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
use http::HeaderMap;
use rustfs_lock::NamespaceLockWrapper;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

pub(crate) use s3s::dto::{
    BucketLifecycleConfiguration as EcstoreBucketLifecycleConfiguration, LifecycleRuleFilter as EcstoreLifecycleRuleFilter,
    ObjectLockConfiguration as EcstoreObjectLockConfiguration, VersioningConfiguration as EcstoreVersioningConfiguration,
};
#[cfg(test)]
pub(crate) use s3s::dto::{ExpirationStatus as EcstoreExpirationStatus, LifecycleRule as EcstoreLifecycleRule};

pub(crate) use rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetSys as EcstoreBucketTargetSys;
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc as EcstoreLcEventSrc;
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::{
    apply_expiry_rule as ecstore_apply_expiry_rule, apply_transition_rule as ecstore_apply_transition_rule,
    lifecycle_version_delete_target as ecstore_lifecycle_version_delete_target,
};
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::lifecycle::object_opts_from_object_info as ecstore_object_opts_from_object_info;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::init_bucket_metadata_sys as ecstore_init_bucket_metadata_sys;
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::{
    BucketMetadataMutationGuard as EcstoreBucketMetadataMutationGuard, get_lifecycle_config as ecstore_get_lifecycle_config,
    get_object_lock_config as ecstore_get_object_lock_config, get_replication_config as ecstore_get_replication_config,
};
pub(crate) use rustfs_ecstore::api::bucket::replication::{
    ReplicateObjectInfo, ReplicationConfig as EcstoreReplicationConfig,
    ReplicationConfigurationExt as EcstoreReplicationConfigurationExt,
    ReplicationHealQueueResult as EcstoreReplicationHealQueueResult,
    ReplicationQueueAdmission as EcstoreReplicationQueueAdmission, ReplicationScannerBridge as EcstoreReplicationScannerBridge,
    ReplicationType,
};
pub(crate) use rustfs_ecstore::api::bucket::replication::{ReplicationStatusType, VersionPurgeStatusType};
pub(crate) use rustfs_ecstore::api::bucket::target::BucketTargets as EcstoreBucketTargets;
pub(crate) use rustfs_ecstore::api::bucket::versioning::VersioningApi as EcstoreVersioningApi;
pub(crate) use rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys as EcstoreBucketVersioningSys;
pub(crate) use rustfs_ecstore::api::cache::{
    ListPathRawOptions as EcstoreListPathRawOptions, list_path_raw as ecstore_list_path_raw,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::capacity::PoolDecommissionInfo as EcstorePoolDecommissionInfo;
pub(crate) use rustfs_ecstore::api::capacity::{
    is_reserved_or_invalid_bucket as ecstore_is_reserved_or_invalid_bucket, path2_bucket_object as ecstore_path2_bucket_object,
    path2_bucket_object_with_base_path as ecstore_path2_bucket_object_with_base_path,
};
pub(crate) use rustfs_ecstore::api::config::com::{read_config as ecstore_read_config, save_config as ecstore_save_config};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::config::init as ecstore_config_init;
pub(crate) use rustfs_ecstore::api::config::storageclass::{
    RRS as ECSTORE_STORAGECLASS_RRS, STANDARD as ECSTORE_STORAGECLASS_STANDARD,
};
pub(crate) use rustfs_ecstore::api::data_usage::{
    invalidate_admin_data_usage_snapshot_cache as ecstore_invalidate_admin_data_usage_snapshot_cache,
    invalidate_data_usage_snapshot_cache as ecstore_invalidate_data_usage_snapshot_cache,
    replace_bucket_usage_memory_from_info as ecstore_replace_bucket_usage_memory_from_info,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::disk::endpoint::Endpoint as EcstoreEndpoint;
pub(crate) use rustfs_ecstore::api::disk::error::{DiskError as EcstoreDiskError, Result as EcstoreDiskResult};
pub(crate) use rustfs_ecstore::api::disk::{
    BUCKET_META_PREFIX as ECSTORE_BUCKET_META_PREFIX, Bytes as EcstoreDiskBytes, Disk as EcstoreDisk, DiskAPI as EcstoreDiskAPI,
    DiskInfo as EcstoreDiskInfo, DiskInfoOptions as EcstoreDiskInfoOptions, DiskLocation as EcstoreDiskLocation,
    NsScannerOpenRequest as EcstoreNsScannerOpenRequest, RUSTFS_META_BUCKET as ECSTORE_RUSTFS_META_BUCKET,
    STORAGE_FORMAT_FILE as ECSTORE_STORAGE_FORMAT_FILE, ScanGuard as EcstoreScanGuard,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::disk::{
    DiskOption as EcstoreDiskOption, DiskStore as EcstoreDiskStore, new_disk as ecstore_new_disk,
};
pub(crate) use rustfs_ecstore::api::error::{
    Error as EcstoreErrorType, Result as EcstoreResultType, StorageError as EcstoreStorageError,
};
pub(crate) use rustfs_ecstore::api::event::{EventArgs as EcstoreEventArgs, send_event as ecstore_send_event};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::layout::{
    EndpointServerPools as EcstoreEndpointServerPools, Endpoints as EcstoreEndpoints, PoolEndpoints as EcstorePoolEndpoints,
};
pub(crate) use rustfs_ecstore::api::notification::NotificationSys as EcstoreNotificationSys;
pub(crate) use rustfs_ecstore::api::notification::scanner_peer_transport_error_message_is_retryable;
pub(crate) use rustfs_ecstore::api::object::{
    SCANNER_PUBLICATION_LEASE_FENCE_METADATA_KEY, ScannerPublicationCommitScope, ScannerPublicationCommitState,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::rebalance::{
    RebalStatus as EcstoreRebalStatus, RebalanceInfo as EcstoreRebalanceInfo, RebalanceMeta as EcstoreRebalanceMeta,
    RebalanceStats as EcstoreRebalanceStats,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::rpc::ScannerPeerDirtyUsageBucket as EcstoreScannerPeerDirtyUsageBucket;
pub(crate) use rustfs_ecstore::api::rpc::{
    ScannerBucketListing as EcstoreScannerBucketListing,
    ScannerDirtyUsageAcknowledgement as EcstoreScannerDirtyUsageAcknowledgement,
    ScannerPeerDirtyUsageSnapshot as EcstoreScannerPeerDirtyUsageSnapshot,
    ScannerScopedDirtyUsageAckEntry as EcstoreScannerScopedDirtyUsageAckEntry,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::runtime::InstanceContext as EcstoreInstanceContext;
pub(crate) use rustfs_ecstore::api::runtime::{
    expiry_state_handle as ecstore_expiry_state_handle, global_tier_config_mgr as ecstore_get_global_tier_config_mgr,
    object_store_handle as ecstore_resolve_object_store_handle, setup_is_erasure as ecstore_is_erasure,
};
pub(crate) use rustfs_ecstore::api::set_disk::SetDisks as EcstoreSetDisks;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::storage::SCANNER_PUBLICATION_LEASE_TTL_MS as ECSTORE_SCANNER_PUBLICATION_LEASE_TTL_MS;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::storage::init_local_disks_with_instance_ctx as ecstore_init_local_disks_with_instance_ctx;
pub(crate) use rustfs_ecstore::api::storage::{
    ECStore as EcstoreStore, ScannerDataMovementPauseStatus as EcstoreScannerDataMovementPauseStatus,
};
pub(crate) use rustfs_lifecycle::{
    Evaluator as EcstoreEvaluator, Event as EcstoreEvent, Lifecycle as EcstoreLifecycle, ObjectOpts as EcstoreObjectOpts,
    TRANSITION_COMPLETE as ECSTORE_TRANSITION_COMPLETE,
};
use rustfs_storage_api as storage_contracts;

#[cfg(test)]
pub(crate) type EcstoreHealResultItem = <EcstoreStore as storage_contracts::HealOperations>::HealResultItem;

pub(crate) mod owner {
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::set_disk::test_util::hold_namespace_commit as ecstore_hold_namespace_commit;

    pub(crate) use super::storage_contracts::{
        HTTPPreconditions, HTTPRangeSpec, NS_SCANNER_PROTOCOL_VERSION, ObjectIO, ObjectOperations, ObjectToDelete,
    };

    pub(crate) use super::{
        ECSTORE_BUCKET_META_PREFIX, ECSTORE_RUSTFS_META_BUCKET, ECSTORE_STORAGE_FORMAT_FILE, ECSTORE_STORAGECLASS_RRS,
        ECSTORE_STORAGECLASS_STANDARD, ECSTORE_TRANSITION_COMPLETE, EcstoreBucketLifecycleConfiguration, EcstoreBucketTargetSys,
        EcstoreBucketVersioningSys, EcstoreDisk, EcstoreDiskAPI, EcstoreDiskBytes, EcstoreDiskError, EcstoreDiskInfo,
        EcstoreDiskInfoOptions, EcstoreDiskLocation, EcstoreDiskResult, EcstoreErrorType, EcstoreEvaluator, EcstoreEvent,
        EcstoreEventArgs, EcstoreLcEventSrc, EcstoreLifecycle, EcstoreLifecycleRuleFilter, EcstoreListPathRawOptions,
        EcstoreNsScannerOpenRequest, EcstoreObjectLockConfiguration, EcstoreObjectOpts, EcstoreReplicationConfigurationExt,
        EcstoreReplicationScannerBridge, EcstoreResultType, EcstoreScanGuard, EcstoreSetDisks, EcstoreStorageError, EcstoreStore,
        EcstoreVersioningApi, EcstoreVersioningConfiguration, SCANNER_PUBLICATION_LEASE_FENCE_METADATA_KEY,
        ScannerPublicationCommitScope, ScannerPublicationCommitState, ScannerReplicationHealObject, ScannerReplicationHealResult,
        ScannerReplicationQueueAdmission, ecstore_apply_expiry_rule, ecstore_apply_transition_rule, ecstore_expiry_state_handle,
        ecstore_get_global_tier_config_mgr, ecstore_get_lifecycle_config, ecstore_get_object_lock_config,
        ecstore_get_replication_config, ecstore_invalidate_admin_data_usage_snapshot_cache,
        ecstore_invalidate_data_usage_snapshot_cache, ecstore_is_erasure, ecstore_is_reserved_or_invalid_bucket,
        ecstore_lifecycle_version_delete_target, ecstore_list_path_raw, ecstore_object_opts_from_object_info,
        ecstore_path2_bucket_object, ecstore_path2_bucket_object_with_base_path, ecstore_read_config,
        ecstore_replace_bucket_usage_memory_from_info, ecstore_resolve_object_store_handle, ecstore_save_config,
        ecstore_send_event, scanner_replication_config_for_lifecycle_eval,
    };

    #[cfg(test)]
    pub(crate) use super::{
        EcstoreDiskOption, EcstoreDiskStore, EcstoreEndpoint, EcstoreEndpointServerPools, EcstoreEndpoints,
        EcstoreExpirationStatus, EcstoreInstanceContext, EcstoreLifecycleRule, EcstorePoolDecommissionInfo, EcstorePoolEndpoints,
        EcstoreRebalStatus, EcstoreRebalanceInfo, EcstoreRebalanceMeta, EcstoreRebalanceStats, ecstore_config_init,
        ecstore_init_bucket_metadata_sys, ecstore_init_local_disks_with_instance_ctx, ecstore_new_disk,
    };
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScannerReplicationConfig(EcstoreReplicationConfig);

impl ScannerReplicationConfig {
    pub(crate) fn new(config: Option<s3s::dto::ReplicationConfiguration>, remotes: Option<EcstoreBucketTargets>) -> Self {
        Self(EcstoreReplicationConfig::new(config, remotes))
    }

    pub(crate) fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool {
        !self.0.is_empty()
            && self
                .0
                .config
                .as_ref()
                .is_some_and(|config| EcstoreReplicationConfigurationExt::has_active_rules(config, prefix, recursive))
    }

    pub(crate) fn into_ecstore(self) -> EcstoreReplicationConfig {
        self.0
    }
}

pub(crate) fn scanner_replication_config_for_lifecycle_eval(
    config: Option<Arc<ScannerReplicationConfig>>,
) -> Option<Arc<EcstoreReplicationConfig>> {
    config.map(|config| match Arc::try_unwrap(config) {
        Ok(config) => Arc::new(config.into_ecstore()),
        Err(config) => Arc::new(config.0.clone()),
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ScannerReplicationQueueAdmission {
    #[default]
    Skipped,
    Queued,
    Missed,
}

impl From<EcstoreReplicationQueueAdmission> for ScannerReplicationQueueAdmission {
    fn from(admission: EcstoreReplicationQueueAdmission) -> Self {
        match admission {
            EcstoreReplicationQueueAdmission::Skipped => Self::Skipped,
            EcstoreReplicationQueueAdmission::Queued => Self::Queued,
            EcstoreReplicationQueueAdmission::Missed => Self::Missed,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ScannerReplicationHealObject {
    pub(crate) bucket: String,
    pub(crate) name: String,
    pub(crate) size: i64,
    pub(crate) delete_marker: bool,
    pub(crate) target_statuses: HashMap<String, ReplicationStatusType>,
    version_purge_status: VersionPurgeStatusType,
    existing_object: bool,
    existing_object_resync: bool,
}

impl ScannerReplicationHealObject {
    #[cfg(test)]
    pub(crate) fn new(bucket: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            name: name.into(),
            ..Default::default()
        }
    }

    pub(crate) fn is_empty_identity(&self) -> bool {
        self.bucket.is_empty() && self.name.is_empty()
    }

    pub(crate) fn is_existing_object_repair(&self) -> bool {
        self.existing_object || self.existing_object_resync
    }

    pub(crate) fn has_version_purge_status(&self) -> bool {
        !self.version_purge_status.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn with_delete_marker(mut self) -> Self {
        self.delete_marker = true;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_pending_version_purge(mut self) -> Self {
        self.version_purge_status = VersionPurgeStatusType::Pending;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_existing_object(mut self) -> Self {
        self.existing_object = true;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_existing_object_resync(mut self) -> Self {
        self.existing_object_resync = true;
        self
    }
}

impl From<ReplicateObjectInfo> for ScannerReplicationHealObject {
    fn from(object: ReplicateObjectInfo) -> Self {
        Self {
            bucket: object.bucket,
            name: object.name,
            size: object.size,
            delete_marker: object.delete_marker,
            target_statuses: object.target_statuses,
            version_purge_status: object.version_purge_status,
            existing_object: object.op_type == ReplicationType::ExistingObject,
            existing_object_resync: object.existing_obj_resync.must_resync(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ScannerReplicationHealResult {
    pub(crate) object_info: ScannerReplicationHealObject,
    pub(crate) admission: ScannerReplicationQueueAdmission,
}

impl From<EcstoreReplicationHealQueueResult> for ScannerReplicationHealResult {
    fn from(result: EcstoreReplicationHealQueueResult) -> Self {
        Self {
            object_info: result.object_info.into(),
            admission: result.admission.into(),
        }
    }
}

pub(crate) mod scan {
    #[cfg(test)]
    pub(crate) use super::storage_contracts::BucketOperations;
    pub(crate) use super::storage_contracts::{
        BucketOptions, NamespaceLocking, SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION, SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION,
    };
    #[cfg(test)]
    pub(crate) use super::storage_contracts::{DeleteBucketOptions, MakeBucketOptions, ObjectIO};
    pub use super::storage_contracts::{
        SCANNER_ACTIVITY_PROTOCOL_VERSION, SCANNER_ACTIVITY_V6_PROTOCOL_VERSION, SCANNER_DIRTY_USAGE_SNAPSHOT_MAX_ENTRIES,
        SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION, SCANNER_DIRTY_USAGE_SNAPSHOT_RPC_MAX_MESSAGE_SIZE,
        SCANNER_SCOPED_DIRTY_USAGE_ACK_MAX_ENTRIES,
    };
}

pub(crate) mod scanner_io {
    pub(crate) use super::storage_contracts::{BucketInfo, BucketOptions};
    #[cfg(test)]
    pub(crate) use super::storage_contracts::{HTTPRangeSpec, ObjectIO};
}

pub(crate) type ScannerBucketListing = EcstoreScannerBucketListing;
pub(crate) type ScannerDataMovementPauseStatus = EcstoreScannerDataMovementPauseStatus;
pub(crate) type ScannerNotificationSys = EcstoreNotificationSys;

#[async_trait::async_trait]
pub(crate) trait ScannerStorage:
    crate::ScannerObjectIO
    + crate::ScannerConfigObjectDelete
    + storage_contracts::BucketOperations<Error = EcstoreErrorType>
    + storage_contracts::NamespaceLocking<Error = EcstoreErrorType, NamespaceLock = NamespaceLockWrapper>
{
    fn scanner_topology_digest(&self) -> [u8; 32];
    fn scanner_namespace_mutation_generation(&self) -> u64;
    async fn scanner_data_movement_activity(&self) -> (bool, bool, u64);
    async fn scanner_data_usage_publication_blocked(&self) -> bool;
    async fn scanner_data_movement_pause_status(&self) -> ScannerDataMovementPauseStatus;
    fn scanner_data_movement_generation(&self) -> u64;
    fn scanner_data_movement_changed(&self) -> Arc<Notify>;
    fn scanner_notification_system(&self) -> Option<Arc<ScannerNotificationSys>>;
    async fn setup_is_erasure(&self) -> bool;
    async fn setup_is_dist_erasure(&self) -> bool;
    async fn setup_is_erasure_sd(&self) -> bool;
    async fn list_bucket_for_scanner(&self, opts: &storage_contracts::BucketOptions) -> EcstoreResultType<ScannerBucketListing>;
    fn all_set_disks(&self) -> Vec<Arc<EcstoreSetDisks>>;
    async fn scanner_pause_backlog_writable_set_disks(&self) -> Vec<Arc<EcstoreSetDisks>>;
    #[cfg(test)]
    fn scanner_observed_probe_store_key(&self) -> usize;
}

#[async_trait::async_trait]
impl ScannerStorage for EcstoreStore {
    fn scanner_topology_digest(&self) -> [u8; 32] {
        crate::scanner::scanner_topology_digest(self)
    }

    fn scanner_namespace_mutation_generation(&self) -> u64 {
        EcstoreStore::scanner_namespace_mutation_generation(self)
    }

    async fn scanner_data_movement_activity(&self) -> (bool, bool, u64) {
        EcstoreStore::scanner_data_movement_activity(self).await
    }

    async fn scanner_data_usage_publication_blocked(&self) -> bool {
        EcstoreStore::scanner_data_usage_publication_blocked(self).await
    }

    async fn scanner_data_movement_pause_status(&self) -> ScannerDataMovementPauseStatus {
        EcstoreStore::scanner_data_movement_pause_status(self).await
    }

    fn scanner_data_movement_generation(&self) -> u64 {
        EcstoreStore::scanner_data_movement_generation(self)
    }

    fn scanner_data_movement_changed(&self) -> Arc<Notify> {
        EcstoreStore::scanner_data_movement_changed(self)
    }

    fn scanner_notification_system(&self) -> Option<Arc<ScannerNotificationSys>> {
        EcstoreStore::notification_system(self)
    }

    async fn setup_is_erasure(&self) -> bool {
        EcstoreStore::setup_is_erasure(self).await
    }

    async fn setup_is_dist_erasure(&self) -> bool {
        EcstoreStore::setup_is_dist_erasure(self).await
    }

    async fn setup_is_erasure_sd(&self) -> bool {
        EcstoreStore::setup_is_erasure_sd(self).await
    }

    async fn list_bucket_for_scanner(&self, opts: &storage_contracts::BucketOptions) -> EcstoreResultType<ScannerBucketListing> {
        EcstoreStore::list_bucket_for_scanner(self, opts).await
    }

    fn all_set_disks(&self) -> Vec<Arc<EcstoreSetDisks>> {
        EcstoreStore::all_set_disks(self)
    }

    async fn scanner_pause_backlog_writable_set_disks(&self) -> Vec<Arc<EcstoreSetDisks>> {
        EcstoreStore::scanner_pause_backlog_writable_set_disks(self).await
    }

    #[cfg(test)]
    fn scanner_observed_probe_store_key(&self) -> usize {
        std::ptr::from_ref(self).cast::<()>() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct FakeScannerStorage;

    #[async_trait::async_trait]
    impl storage_contracts::ObjectIO for FakeScannerStorage {
        type Error = EcstoreErrorType;
        type RangeSpec = storage_contracts::HTTPRangeSpec;
        type HeaderMap = HeaderMap;
        type ObjectOptions = <EcstoreStore as storage_contracts::ObjectIO>::ObjectOptions;
        type ObjectInfo = <EcstoreStore as storage_contracts::ObjectIO>::ObjectInfo;
        type GetObjectReader = <EcstoreStore as storage_contracts::ObjectIO>::GetObjectReader;
        type PutObjectReader = <EcstoreStore as storage_contracts::ObjectIO>::PutObjectReader;

        async fn get_object_reader(
            &self,
            _bucket: &str,
            _object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::GetObjectReader, Self::Error> {
            Err(EcstoreErrorType::other("fake scanner storage has no object reader"))
        }

        async fn put_object(
            &self,
            _bucket: &str,
            _object: &str,
            _data: &mut Self::PutObjectReader,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreErrorType::other("fake scanner storage has no object writer"))
        }
    }

    #[async_trait::async_trait]
    impl storage_contracts::BucketOperations for FakeScannerStorage {
        type Error = EcstoreErrorType;

        async fn make_bucket(&self, _bucket: &str, _opts: &storage_contracts::MakeBucketOptions) -> Result<(), Self::Error> {
            Err(EcstoreErrorType::other("fake scanner storage cannot make buckets"))
        }

        async fn get_bucket_info(
            &self,
            _bucket: &str,
            _opts: &storage_contracts::BucketOptions,
        ) -> Result<storage_contracts::BucketInfo, Self::Error> {
            Err(EcstoreErrorType::other("fake scanner storage cannot read buckets"))
        }

        async fn list_bucket(
            &self,
            _opts: &storage_contracts::BucketOptions,
        ) -> Result<Vec<storage_contracts::BucketInfo>, Self::Error> {
            Ok(Vec::new())
        }

        async fn delete_bucket(&self, _bucket: &str, _opts: &storage_contracts::DeleteBucketOptions) -> Result<(), Self::Error> {
            Err(EcstoreErrorType::other("fake scanner storage cannot delete buckets"))
        }
    }

    #[async_trait::async_trait]
    impl storage_contracts::NamespaceLocking for FakeScannerStorage {
        type Error = EcstoreErrorType;
        type NamespaceLock = NamespaceLockWrapper;

        async fn new_ns_lock(&self, _bucket: &str, _object: &str) -> Result<Self::NamespaceLock, Self::Error> {
            Err(EcstoreErrorType::other("fake scanner storage has no namespace lock"))
        }
    }

    #[async_trait::async_trait]
    impl crate::ScannerConfigObjectDelete for FakeScannerStorage {
        async fn delete_config_object(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: <EcstoreStore as storage_contracts::ObjectIO>::ObjectOptions,
        ) -> EcstoreResultType<<EcstoreStore as storage_contracts::ObjectIO>::ObjectInfo> {
            Err(EcstoreErrorType::other("fake scanner storage cannot delete objects"))
        }

        async fn scanner_data_usage_publication_admission(&self) -> Option<crate::ScannerDataUsagePublicationAdmission> {
            Some(crate::ScannerDataUsagePublicationAdmission::unfenced())
        }
    }

    #[async_trait::async_trait]
    impl ScannerStorage for FakeScannerStorage {
        fn scanner_topology_digest(&self) -> [u8; 32] {
            [0; 32]
        }

        fn scanner_namespace_mutation_generation(&self) -> u64 {
            0
        }

        async fn scanner_data_movement_activity(&self) -> (bool, bool, u64) {
            (false, false, 0)
        }

        async fn scanner_data_usage_publication_blocked(&self) -> bool {
            false
        }

        async fn scanner_data_movement_pause_status(&self) -> ScannerDataMovementPauseStatus {
            ScannerDataMovementPauseStatus::default()
        }

        fn scanner_data_movement_generation(&self) -> u64 {
            0
        }

        fn scanner_data_movement_changed(&self) -> Arc<Notify> {
            Arc::new(Notify::new())
        }

        fn scanner_notification_system(&self) -> Option<Arc<ScannerNotificationSys>> {
            None
        }

        async fn setup_is_erasure(&self) -> bool {
            false
        }

        async fn setup_is_dist_erasure(&self) -> bool {
            false
        }

        async fn setup_is_erasure_sd(&self) -> bool {
            true
        }

        async fn list_bucket_for_scanner(
            &self,
            _opts: &storage_contracts::BucketOptions,
        ) -> EcstoreResultType<ScannerBucketListing> {
            Err(EcstoreErrorType::other("fake scanner storage cannot list scanner buckets"))
        }

        fn all_set_disks(&self) -> Vec<Arc<EcstoreSetDisks>> {
            Vec::new()
        }

        async fn scanner_pause_backlog_writable_set_disks(&self) -> Vec<Arc<EcstoreSetDisks>> {
            Vec::new()
        }

        fn scanner_observed_probe_store_key(&self) -> usize {
            0
        }
    }

    fn assert_scanner_storage<S: ScannerStorage>() {}

    #[test]
    fn scanner_storage_contract_accepts_fake_without_ecstore() {
        assert_scanner_storage::<FakeScannerStorage>();
    }
}
