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

pub mod access;
pub mod backpressure;
pub mod concurrency;
pub mod deadlock_detector;
pub mod ecfs;
pub(crate) mod helper;
pub mod lock_optimizer;
pub mod options;
pub mod request_context;
pub mod rpc;
pub(crate) mod s3_api;
pub(crate) mod sse;
pub mod timeout_wrapper;
pub mod tonic_service;

pub(crate) type StorageDeletedObject = rustfs_storage_api::DeletedObject;
pub(crate) type StorageGetObjectReader = self::GetObjectReader;
pub(crate) type StorageObjectInfo = self::ObjectInfo;
pub(crate) type StorageObjectOptions = self::ObjectOptions;
pub(crate) type StorageObjectToDelete = rustfs_storage_api::ObjectToDelete;
pub(crate) type StoragePutObjReader = self::PutObjReader;

#[cfg(test)]
mod concurrent_fix_test;
#[cfg(test)]
mod concurrent_get_object_test;
mod ecfs_extend;
#[cfg(test)]
mod ecfs_test;
pub(crate) mod head_prefix;
#[cfg(test)]
mod multi_factor_scheduler_integration_test;
#[cfg(test)]
mod sse_test;

pub(crate) use ecfs_extend::*;
pub(crate) use sse::{
    DecryptionRequest, EncryptionRequest, PrepareEncryptionRequest, extract_server_side_encryption_from_headers,
    extract_ssec_params_from_headers, sse_decryption, sse_encryption, sse_prepare_encryption, strip_managed_encryption_metadata,
    validate_sse_headers_for_read, validate_sse_headers_for_write, validate_ssec_for_read,
};

use std::sync::Arc;

pub(crate) mod ecstore_admin {
    pub(crate) use rustfs_ecstore::api::admin::{get_local_server_property, get_server_info};
}

pub(crate) mod ecstore_bucket {
    pub(crate) use rustfs_ecstore::api::bucket::{
        bandwidth, bucket_target_sys, lifecycle, metadata, metadata_sys, migration, object_lock, policy_sys, replication,
        tagging, target, utils,
    };
    pub(crate) use rustfs_ecstore::api::bucket::{quota, versioning, versioning_sys};
}

pub(crate) mod ecstore_capacity {
    pub(crate) use rustfs_ecstore::api::capacity::{
        PoolDecommissionInfo, PoolStatus, get_total_usable_capacity, get_total_usable_capacity_free,
        is_reserved_or_invalid_bucket,
    };
}

pub(crate) mod ecstore_client {
    pub(crate) use rustfs_ecstore::api::client::{admin_handler_utils, object_api_utils, transition_api};
}

pub(crate) mod ecstore_compression {
    pub(crate) use rustfs_ecstore::api::compression::{MIN_DISK_COMPRESSIBLE_SIZE, is_disk_compressible};
}

pub(crate) mod ecstore_cluster {
    pub(crate) use rustfs_ecstore::api::cluster::topology_snapshot_from_endpoint_pools_with_capabilities;
}

pub(crate) mod ecstore_config {
    pub(crate) use rustfs_ecstore::api::config::{
        com, init, init_global_config_sys, set_global_storage_class, storageclass, try_migrate_server_config,
    };
}

pub(crate) mod ecstore_data_usage {
    pub(crate) use rustfs_ecstore::api::data_usage::{
        apply_bucket_usage_memory_overlay, load_data_usage_from_backend, record_bucket_object_delete_memory,
        record_bucket_object_write_memory,
    };
}

#[allow(unused_imports)]
pub(crate) mod ecstore_disk {
    pub(crate) use rustfs_ecstore::api::disk::{
        CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskStore, FileInfoVersions, FileReader, FileWriter,
        RUSTFS_META_BUCKET, ReadMultipleReq, ReadMultipleResp, ReadOptions, RenameDataResp, UpdateMetadataOpts, VolumeInfo,
        WalkDirOptions,
    };
    pub(crate) use rustfs_ecstore::api::disk::{endpoint, error, error_reduce};
}

pub(crate) mod ecstore_error {
    pub(crate) use rustfs_ecstore::api::error::{
        Error, Result, StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found,
    };
}

pub(crate) mod ecstore_event {
    pub(crate) use rustfs_ecstore::api::event::{EventArgs, register_event_dispatch_hook};
}

pub(crate) mod ecstore_global {
    pub(crate) use rustfs_ecstore::api::global::{
        GLOBAL_BOOT_TIME, GLOBAL_TierConfigMgr, get_global_bucket_monitor, get_global_deployment_id, get_global_endpoints_opt,
        get_global_lock_client, get_global_lock_clients, get_global_region, get_global_tier_config_mgr, global_rustfs_port,
        is_dist_erasure, new_object_layer_fn, resolve_object_store_handle, set_global_endpoints, set_global_region,
        set_global_rustfs_port, set_object_store_resolver, shutdown_background_services, update_erasure_type,
    };
}

#[allow(unused_imports)]
pub(crate) mod ecstore_layout {
    pub(crate) use rustfs_ecstore::api::layout::{DisksLayout, EndpointServerPools, Endpoints, PoolEndpoints, SetupType};
}

pub(crate) mod ecstore_metrics {
    pub(crate) use rustfs_ecstore::api::metrics::{CollectMetricsOpts, MetricType, collect_local_metrics};
}

#[allow(unused_imports)]
pub(crate) mod ecstore_notification {
    pub(crate) use rustfs_ecstore::api::notification::{
        NotificationSys, get_global_notification_sys, new_global_notification_sys,
    };
}

#[allow(unused_imports)]
pub(crate) mod ecstore_rebalance {
    pub(crate) use rustfs_ecstore::api::rebalance::{
        DiskStat, RebalSaveOpt, RebalStatus, RebalanceCleanupWarningEntry, RebalanceCleanupWarnings, RebalanceInfo,
        RebalanceMeta, RebalanceStats, RebalanceStopPropagationRecord, decode_rebalance_stop_propagation_record,
        encode_rebalance_stop_propagation_record,
    };
}

pub(crate) mod ecstore_rio {
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::rio::{DecryptReader, EncryptReader, HardLimitReader, Reader, boxed_reader};
    pub(crate) use rustfs_ecstore::api::rio::{
        DynReader, HashReader, ReadStream, WriteEncryption, WritePlan, compression_metadata_value, wrap_reader,
    };
}

pub(crate) mod ecstore_rpc {
    pub(crate) use rustfs_ecstore::api::rpc::{
        LocalPeerS3Client, PEER_RESTSIGNAL, PEER_RESTSUB_SYS, PeerRestClient, PeerS3Client, SERVICE_SIGNAL_REFRESH_CONFIG,
        SERVICE_SIGNAL_RELOAD_DYNAMIC, TONIC_RPC_PREFIX, verify_rpc_signature,
    };
}

pub(crate) mod ecstore_set_disk {
    pub(crate) use rustfs_ecstore::api::set_disk::{DEFAULT_READ_BUFFER_SIZE, get_lock_acquire_timeout, is_valid_storage_class};
}

pub(crate) mod ecstore_storage {
    pub(crate) use rustfs_ecstore::api::storage::{
        ECStore, all_local_disk, all_local_disk_path, find_local_disk_by_ref, init_local_disks, init_lock_clients,
        prewarm_local_disk_id_map,
    };
}

pub(crate) mod ecstore_tier {
    pub(crate) use rustfs_ecstore::api::tier::tier::TierConfigMgr;
    pub(crate) use rustfs_ecstore::api::tier::{tier, tier_admin, tier_config, tier_handlers, warm_backend};
}

pub(crate) const BUCKET_ACCELERATE_CONFIG: &str = ecstore_bucket::metadata::BUCKET_ACCELERATE_CONFIG;
pub(crate) const BUCKET_LOGGING_CONFIG: &str = ecstore_bucket::metadata::BUCKET_LOGGING_CONFIG;
pub(crate) const BUCKET_REQUEST_PAYMENT_CONFIG: &str = ecstore_bucket::metadata::BUCKET_REQUEST_PAYMENT_CONFIG;
pub(crate) const BUCKET_TABLE_CATALOG_META_PREFIX: &str = ecstore_bucket::metadata::BUCKET_TABLE_CATALOG_META_PREFIX;
pub(crate) const BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX: &str =
    ecstore_bucket::metadata::BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX;
pub(crate) const BUCKET_TABLE_CONFIG: &str = ecstore_bucket::metadata::BUCKET_TABLE_CONFIG;
pub(crate) const BUCKET_TABLE_RESERVED_PREFIX: &str = ecstore_bucket::metadata::BUCKET_TABLE_RESERVED_PREFIX;
pub(crate) const BUCKET_VERSIONING_CONFIG: &str = ecstore_bucket::metadata::BUCKET_VERSIONING_CONFIG;
pub(crate) const BUCKET_WEBSITE_CONFIG: &str = ecstore_bucket::metadata::BUCKET_WEBSITE_CONFIG;
pub(crate) const DEFAULT_READ_BUFFER_SIZE: usize = ecstore_set_disk::DEFAULT_READ_BUFFER_SIZE;
pub(crate) const OBJECT_LOCK_CONFIG: &str = ecstore_bucket::metadata::OBJECT_LOCK_CONFIG;
pub(crate) const PEER_RESTSIGNAL: &str = ecstore_rpc::PEER_RESTSIGNAL;
pub(crate) const PEER_RESTSUB_SYS: &str = ecstore_rpc::PEER_RESTSUB_SYS;
pub(crate) const SERVICE_SIGNAL_REFRESH_CONFIG: u64 = ecstore_rpc::SERVICE_SIGNAL_REFRESH_CONFIG;
pub(crate) const SERVICE_SIGNAL_RELOAD_DYNAMIC: u64 = ecstore_rpc::SERVICE_SIGNAL_RELOAD_DYNAMIC;
pub(crate) const RUSTFS_META_BUCKET: &str = ecstore_disk::RUSTFS_META_BUCKET;
pub(crate) const TONIC_RPC_PREFIX: &str = ecstore_rpc::TONIC_RPC_PREFIX;
#[cfg(test)]
pub(crate) const STORAGE_CLASS_SUB_SYS: &str = ecstore_config::com::STORAGE_CLASS_SUB_SYS;

pub(crate) type BucketMetadata = ecstore_bucket::metadata::BucketMetadata;
#[cfg(test)]
pub(crate) type BucketMetadataSys = ecstore_bucket::metadata_sys::BucketMetadataSys;
pub(crate) type BucketVersioningSys = ecstore_bucket::versioning_sys::BucketVersioningSys;
pub(crate) type BucketBandwidthMonitor = ecstore_bucket::bandwidth::monitor::Monitor;
pub(crate) type CheckPartsResp = ecstore_disk::CheckPartsResp;
pub(crate) type CollectMetricsOpts = ecstore_metrics::CollectMetricsOpts;
pub(crate) type DailyAllTierStats = ecstore_bucket::lifecycle::tier_last_day_stats::DailyAllTierStats;
pub(crate) type DeleteOptions = ecstore_disk::DeleteOptions;
pub(crate) type DiskError = ecstore_disk::error::DiskError;
pub(crate) type DiskInfo = ecstore_disk::DiskInfo;
pub(crate) type DiskInfoOptions = ecstore_disk::DiskInfoOptions;
pub(crate) type DiskResult<T> = ecstore_disk::error::Result<T>;
pub(crate) type DiskStore = ecstore_disk::DiskStore;
#[cfg(test)]
pub(crate) type DisksLayout = ecstore_layout::DisksLayout;
pub(crate) type DynReplicationPool = ecstore_bucket::replication::DynReplicationPool;
pub(crate) type DynReader = ecstore_rio::DynReader;
pub(crate) type ECStore = ecstore_storage::ECStore;
pub(crate) type Endpoint = ecstore_disk::endpoint::Endpoint;
#[cfg(test)]
pub(crate) type Endpoints = ecstore_layout::Endpoints;
pub(crate) type EndpointServerPools = ecstore_layout::EndpointServerPools;
pub(crate) type EventArgs = ecstore_event::EventArgs;
pub(crate) type FileInfoVersions = ecstore_disk::FileInfoVersions;
pub(crate) type FileReader = ecstore_disk::FileReader;
pub(crate) type FileWriter = ecstore_disk::FileWriter;
pub(crate) type HashReader = ecstore_rio::HashReader;
pub(crate) type LocalPeerS3Client = ecstore_rpc::LocalPeerS3Client;
pub(crate) type MetricType = ecstore_metrics::MetricType;
pub(crate) type ObjectPartInfo = rustfs_filemeta::ObjectPartInfo;
pub(crate) type ObjectLockBlockReason = ecstore_bucket::object_lock::objectlock_sys::ObjectLockBlockReason;
pub(crate) type ObjectStoreResolver = dyn Fn() -> Option<Arc<ECStore>> + Send + Sync + 'static;
pub(crate) type PolicySys = ecstore_bucket::policy_sys::PolicySys;
pub(crate) type PoolEndpoints = ecstore_layout::PoolEndpoints;
pub(crate) type QuotaError = ecstore_bucket::quota::QuotaError;
pub(crate) type RawFileInfo = rustfs_filemeta::RawFileInfo;
pub(crate) type ReadMultipleReq = ecstore_disk::ReadMultipleReq;
pub(crate) type ReadMultipleResp = ecstore_disk::ReadMultipleResp;
pub(crate) type ReadOptions = ecstore_disk::ReadOptions;
pub(crate) type RenameDataResp = ecstore_disk::RenameDataResp;
pub(crate) type ReplicationStats = ecstore_bucket::replication::ReplicationStats;
pub(crate) type SetupType = ecstore_layout::SetupType;
pub(crate) type StorageError = ecstore_error::StorageError;
pub(crate) type TierConfigMgr = ecstore_tier::TierConfigMgr;
pub(crate) type Error = ecstore_error::Error;
pub(crate) type Result<T> = ecstore_error::Result<T>;
pub(crate) type UpdateMetadataOpts = ecstore_disk::UpdateMetadataOpts;
pub(crate) type VolumeInfo = ecstore_disk::VolumeInfo;
pub(crate) type WalkDirOptions = ecstore_disk::WalkDirOptions;
pub(crate) type WriteEncryption = ecstore_rio::WriteEncryption;
pub(crate) type WritePlan = ecstore_rio::WritePlan;
#[cfg(test)]
pub(crate) type DecryptReader<R> = ecstore_rio::DecryptReader<R>;
#[cfg(test)]
pub(crate) type EncryptReader<R> = ecstore_rio::EncryptReader<R>;
#[cfg(test)]
pub(crate) type HardLimitReader<R> = ecstore_rio::HardLimitReader<R>;
pub(crate) type NotificationSys = ecstore_notification::NotificationSys;

pub(crate) async fn get_local_server_property() -> rustfs_madmin::ServerProperties {
    ecstore_admin::get_local_server_property().await
}

pub(crate) async fn init_background_replication(store: Arc<ECStore>) {
    ecstore_bucket::replication::init_background_replication(store).await;
}

pub(crate) async fn all_local_disk() -> Vec<DiskStore> {
    ecstore_storage::all_local_disk().await
}

pub(crate) async fn get_bucket_notification_config(bucket: &str) -> Result<Option<s3s::dto::NotificationConfiguration>> {
    ecstore_bucket::metadata_sys::get_notification_config(bucket).await
}

pub(crate) async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
    ecstore_bucket::metadata_sys::init_bucket_metadata_sys(api, buckets).await;
}

pub(crate) fn bucket_metadata_runtime_initialized() -> bool {
    ecstore_bucket::metadata_sys::get_global_bucket_metadata_sys().is_some()
}

pub(crate) fn disk_drive_path(disk: &DiskStore) -> String {
    ecstore_disk::DiskAPI::to_string(disk.as_ref())
}

pub(crate) fn disk_endpoint(disk: &DiskStore) -> String {
    ecstore_disk::DiskAPI::endpoint(disk.as_ref()).to_string()
}

pub(crate) fn get_global_replication_pool() -> Option<Arc<DynReplicationPool>> {
    ecstore_bucket::replication::get_global_replication_pool()
}

pub(crate) fn get_global_replication_stats() -> Option<Arc<ReplicationStats>> {
    ecstore_bucket::replication::GLOBAL_REPLICATION_STATS.get().cloned()
}

pub(crate) fn get_global_boot_time() -> Option<std::time::SystemTime> {
    ecstore_global::GLOBAL_BOOT_TIME.get().cloned()
}

pub(crate) fn get_daily_all_tier_stats() -> DailyAllTierStats {
    ecstore_bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_TransitionState.get_daily_all_tier_stats()
}

pub(crate) async fn try_migrate_bucket_metadata(store: Arc<ECStore>) {
    ecstore_bucket::migration::try_migrate_bucket_metadata(store).await;
}

pub(crate) async fn try_migrate_iam_config(store: Arc<ECStore>) {
    ecstore_bucket::migration::try_migrate_iam_config(store).await;
}

pub(crate) fn init_ecstore_config() {
    ecstore_config::init();
}

pub(crate) async fn init_global_config_sys(store: Arc<ECStore>) -> Result<()> {
    ecstore_config::init_global_config_sys(store).await
}

pub(crate) async fn init_local_disks(endpoint_pools: EndpointServerPools) -> Result<()> {
    ecstore_storage::init_local_disks(endpoint_pools).await
}

pub(crate) fn init_lock_clients(endpoint_pools: EndpointServerPools) {
    ecstore_storage::init_lock_clients(endpoint_pools);
}

pub(crate) async fn new_global_notification_sys(endpoint_pools: EndpointServerPools) -> Result<()> {
    ecstore_notification::new_global_notification_sys(endpoint_pools).await
}

pub(crate) async fn read_config(api: Arc<ECStore>, file: &str) -> Result<Vec<u8>> {
    ecstore_config::com::read_config(api, file).await
}

pub(crate) async fn prewarm_local_disk_id_map() {
    ecstore_storage::prewarm_local_disk_id_map().await;
}

pub(crate) fn replication_queue_current_count() -> Option<i64> {
    ecstore_bucket::replication::GLOBAL_REPLICATION_STATS.get().and_then(|stats| {
        stats
            .q_cache
            .try_lock()
            .ok()
            .map(|cache| cache.sr_queue_stats.curr.get_current_count())
    })
}

pub(crate) async fn save_config(api: Arc<ECStore>, file: &str, data: Vec<u8>) -> Result<()> {
    ecstore_config::com::save_config(api, file, data).await
}

pub(crate) fn shutdown_background_services() {
    ecstore_global::shutdown_background_services();
}

pub(crate) fn set_global_endpoints(endpoints: Vec<PoolEndpoints>) {
    ecstore_global::set_global_endpoints(endpoints);
}

pub(crate) fn set_global_region(region: s3s::region::Region) {
    ecstore_global::set_global_region(region);
}

pub(crate) fn set_global_rustfs_port(value: u16) {
    ecstore_global::set_global_rustfs_port(value);
}

pub(crate) async fn try_migrate_server_config(store: Arc<ECStore>) {
    ecstore_config::try_migrate_server_config(store).await;
}

pub(crate) async fn update_erasure_type(setup_type: SetupType) {
    ecstore_global::update_erasure_type(setup_type).await;
}

pub(crate) trait StorageDiskRpcExt {
    async fn disk_info(&self, opts: &DiskInfoOptions) -> DiskResult<DiskInfo>;
    async fn delete_volume(&self, volume: &str) -> DiskResult<()>;
    async fn read_multiple(&self, req: ReadMultipleReq) -> DiskResult<Vec<ReadMultipleResp>>;
    async fn delete_versions(&self, volume: &str, versions: Vec<FileInfoVersions>, opts: DeleteOptions)
    -> Vec<Option<DiskError>>;
    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        file_info: rustfs_filemeta::FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> DiskResult<()>;
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> DiskResult<RawFileInfo>;
    async fn read_version(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> DiskResult<rustfs_filemeta::FileInfo>;
    async fn write_metadata(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        file_info: rustfs_filemeta::FileInfo,
    ) -> DiskResult<()>;
    async fn update_metadata(
        &self,
        volume: &str,
        path: &str,
        file_info: rustfs_filemeta::FileInfo,
        opts: &UpdateMetadataOpts,
    ) -> DiskResult<()>;
    async fn read_metadata(&self, volume: &str, path: &str) -> DiskResult<bytes::Bytes>;
    async fn delete_paths(&self, volume: &str, paths: &[String]) -> DiskResult<()>;
    async fn stat_volume(&self, volume: &str) -> DiskResult<VolumeInfo>;
    async fn list_volumes(&self) -> DiskResult<Vec<VolumeInfo>>;
    async fn make_volume(&self, volume: &str) -> DiskResult<()>;
    async fn make_volumes(&self, volume: Vec<&str>) -> DiskResult<()>;
    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        file_info: rustfs_filemeta::FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> DiskResult<RenameDataResp>;
    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> DiskResult<Vec<String>>;
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> DiskResult<FileReader>;
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> DiskResult<()>;
    async fn rename_part(
        &self,
        src_volume: &str,
        src_path: &str,
        dst_volume: &str,
        dst_path: &str,
        meta: bytes::Bytes,
    ) -> DiskResult<()>;
    async fn delete(&self, volume: &str, path: &str, options: DeleteOptions) -> DiskResult<()>;
    async fn verify_file(&self, volume: &str, path: &str, file_info: &rustfs_filemeta::FileInfo) -> DiskResult<CheckPartsResp>;
    async fn check_parts(&self, volume: &str, path: &str, file_info: &rustfs_filemeta::FileInfo) -> DiskResult<CheckPartsResp>;
    async fn read_parts(&self, bucket: &str, paths: &[String]) -> DiskResult<Vec<ObjectPartInfo>>;
    async fn walk_dir<W: tokio::io::AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> DiskResult<()>;
    async fn write_all(&self, volume: &str, path: &str, data: bytes::Bytes) -> DiskResult<()>;
    async fn read_all(&self, volume: &str, path: &str) -> DiskResult<bytes::Bytes>;
    async fn append_file(&self, volume: &str, path: &str) -> DiskResult<FileWriter>;
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, file_size: i64) -> DiskResult<FileWriter>;
}

impl<T> StorageDiskRpcExt for T
where
    T: ecstore_disk::DiskAPI,
{
    async fn disk_info(&self, opts: &DiskInfoOptions) -> DiskResult<DiskInfo> {
        ecstore_disk::DiskAPI::disk_info(self, opts).await
    }

    async fn delete_volume(&self, volume: &str) -> DiskResult<()> {
        ecstore_disk::DiskAPI::delete_volume(self, volume).await
    }

    async fn read_multiple(&self, req: ReadMultipleReq) -> DiskResult<Vec<ReadMultipleResp>> {
        ecstore_disk::DiskAPI::read_multiple(self, req).await
    }

    async fn delete_versions(
        &self,
        volume: &str,
        versions: Vec<FileInfoVersions>,
        opts: DeleteOptions,
    ) -> Vec<Option<DiskError>> {
        ecstore_disk::DiskAPI::delete_versions(self, volume, versions, opts).await
    }

    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        file_info: rustfs_filemeta::FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> DiskResult<()> {
        ecstore_disk::DiskAPI::delete_version(self, volume, path, file_info, force_del_marker, opts).await
    }

    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> DiskResult<RawFileInfo> {
        ecstore_disk::DiskAPI::read_xl(self, volume, path, read_data).await
    }

    async fn read_version(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> DiskResult<rustfs_filemeta::FileInfo> {
        ecstore_disk::DiskAPI::read_version(self, org_volume, volume, path, version_id, opts).await
    }

    async fn write_metadata(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        file_info: rustfs_filemeta::FileInfo,
    ) -> DiskResult<()> {
        ecstore_disk::DiskAPI::write_metadata(self, org_volume, volume, path, file_info).await
    }

    async fn update_metadata(
        &self,
        volume: &str,
        path: &str,
        file_info: rustfs_filemeta::FileInfo,
        opts: &UpdateMetadataOpts,
    ) -> DiskResult<()> {
        ecstore_disk::DiskAPI::update_metadata(self, volume, path, file_info, opts).await
    }

    async fn read_metadata(&self, volume: &str, path: &str) -> DiskResult<bytes::Bytes> {
        ecstore_disk::DiskAPI::read_metadata(self, volume, path).await
    }

    async fn delete_paths(&self, volume: &str, paths: &[String]) -> DiskResult<()> {
        ecstore_disk::DiskAPI::delete_paths(self, volume, paths).await
    }

    async fn stat_volume(&self, volume: &str) -> DiskResult<VolumeInfo> {
        ecstore_disk::DiskAPI::stat_volume(self, volume).await
    }

    async fn list_volumes(&self) -> DiskResult<Vec<VolumeInfo>> {
        ecstore_disk::DiskAPI::list_volumes(self).await
    }

    async fn make_volume(&self, volume: &str) -> DiskResult<()> {
        ecstore_disk::DiskAPI::make_volume(self, volume).await
    }

    async fn make_volumes(&self, volume: Vec<&str>) -> DiskResult<()> {
        ecstore_disk::DiskAPI::make_volumes(self, volume).await
    }

    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        file_info: rustfs_filemeta::FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> DiskResult<RenameDataResp> {
        ecstore_disk::DiskAPI::rename_data(self, src_volume, src_path, file_info, dst_volume, dst_path).await
    }

    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> DiskResult<Vec<String>> {
        ecstore_disk::DiskAPI::list_dir(self, origvolume, volume, dir_path, count).await
    }

    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> DiskResult<FileReader> {
        ecstore_disk::DiskAPI::read_file_stream(self, volume, path, offset, length).await
    }

    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> DiskResult<()> {
        ecstore_disk::DiskAPI::rename_file(self, src_volume, src_path, dst_volume, dst_path).await
    }

    async fn rename_part(
        &self,
        src_volume: &str,
        src_path: &str,
        dst_volume: &str,
        dst_path: &str,
        meta: bytes::Bytes,
    ) -> DiskResult<()> {
        ecstore_disk::DiskAPI::rename_part(self, src_volume, src_path, dst_volume, dst_path, meta).await
    }

    async fn delete(&self, volume: &str, path: &str, options: DeleteOptions) -> DiskResult<()> {
        ecstore_disk::DiskAPI::delete(self, volume, path, options).await
    }

    async fn verify_file(&self, volume: &str, path: &str, file_info: &rustfs_filemeta::FileInfo) -> DiskResult<CheckPartsResp> {
        ecstore_disk::DiskAPI::verify_file(self, volume, path, file_info).await
    }

    async fn check_parts(&self, volume: &str, path: &str, file_info: &rustfs_filemeta::FileInfo) -> DiskResult<CheckPartsResp> {
        ecstore_disk::DiskAPI::check_parts(self, volume, path, file_info).await
    }

    async fn read_parts(&self, bucket: &str, paths: &[String]) -> DiskResult<Vec<ObjectPartInfo>> {
        ecstore_disk::DiskAPI::read_parts(self, bucket, paths).await
    }

    async fn walk_dir<W: tokio::io::AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> DiskResult<()> {
        ecstore_disk::DiskAPI::walk_dir(self, opts, wr).await
    }

    async fn write_all(&self, volume: &str, path: &str, data: bytes::Bytes) -> DiskResult<()> {
        ecstore_disk::DiskAPI::write_all(self, volume, path, data).await
    }

    async fn read_all(&self, volume: &str, path: &str) -> DiskResult<bytes::Bytes> {
        ecstore_disk::DiskAPI::read_all(self, volume, path).await
    }

    async fn append_file(&self, volume: &str, path: &str) -> DiskResult<FileWriter> {
        ecstore_disk::DiskAPI::append_file(self, volume, path).await
    }

    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, file_size: i64) -> DiskResult<FileWriter> {
        ecstore_disk::DiskAPI::create_file(self, origvolume, volume, path, file_size).await
    }
}

pub(crate) trait StoragePeerS3ClientExt {
    async fn heal_bucket(
        &self,
        bucket: &str,
        opts: &rustfs_common::heal_channel::HealOpts,
    ) -> DiskResult<rustfs_madmin::heal_commands::HealResultItem>;
    async fn make_bucket(&self, bucket: &str, opts: &rustfs_storage_api::MakeBucketOptions) -> DiskResult<()>;
    async fn list_bucket(&self, opts: &rustfs_storage_api::BucketOptions) -> DiskResult<Vec<rustfs_storage_api::BucketInfo>>;
    async fn delete_bucket(&self, bucket: &str, opts: &rustfs_storage_api::DeleteBucketOptions) -> DiskResult<()>;
    async fn get_bucket_info(
        &self,
        bucket: &str,
        opts: &rustfs_storage_api::BucketOptions,
    ) -> DiskResult<rustfs_storage_api::BucketInfo>;
}

impl StoragePeerS3ClientExt for LocalPeerS3Client {
    async fn heal_bucket(
        &self,
        bucket: &str,
        opts: &rustfs_common::heal_channel::HealOpts,
    ) -> DiskResult<rustfs_madmin::heal_commands::HealResultItem> {
        ecstore_rpc::PeerS3Client::heal_bucket(self, bucket, opts).await
    }

    async fn make_bucket(&self, bucket: &str, opts: &rustfs_storage_api::MakeBucketOptions) -> DiskResult<()> {
        ecstore_rpc::PeerS3Client::make_bucket(self, bucket, opts).await
    }

    async fn list_bucket(&self, opts: &rustfs_storage_api::BucketOptions) -> DiskResult<Vec<rustfs_storage_api::BucketInfo>> {
        ecstore_rpc::PeerS3Client::list_bucket(self, opts).await
    }

    async fn delete_bucket(&self, bucket: &str, opts: &rustfs_storage_api::DeleteBucketOptions) -> DiskResult<()> {
        ecstore_rpc::PeerS3Client::delete_bucket(self, bucket, opts).await
    }

    async fn get_bucket_info(
        &self,
        bucket: &str,
        opts: &rustfs_storage_api::BucketOptions,
    ) -> DiskResult<rustfs_storage_api::BucketInfo> {
        ecstore_rpc::PeerS3Client::get_bucket_info(self, bucket, opts).await
    }
}

pub(crate) async fn load_bucket_metadata(api: Arc<ECStore>, bucket: &str) -> Result<BucketMetadata> {
    ecstore_bucket::metadata::load_bucket_metadata(api, bucket).await
}

#[cfg(test)]
pub(crate) fn bucket_metadata_sys_initialized() -> bool {
    ecstore_bucket::metadata_sys::GLOBAL_BucketMetadataSys.get().is_some()
}

#[cfg(test)]
pub(crate) fn get_global_bucket_metadata_sys() -> Option<Arc<tokio::sync::RwLock<BucketMetadataSys>>> {
    ecstore_bucket::metadata_sys::get_global_bucket_metadata_sys()
}

pub(crate) async fn delete_bucket_metadata_config(bucket: &str, config_file: &str) -> Result<time::OffsetDateTime> {
    ecstore_bucket::metadata_sys::delete(bucket, config_file).await
}

pub(crate) async fn get_bucket_metadata(bucket: &str) -> Result<Arc<BucketMetadata>> {
    ecstore_bucket::metadata_sys::get(bucket).await
}

pub(crate) async fn get_bucket_accelerate_config(
    bucket: &str,
) -> Result<(s3s::dto::AccelerateConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_accelerate_config(bucket).await
}

pub(crate) async fn get_bucket_policy_raw(bucket: &str) -> Result<(String, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_bucket_policy_raw(bucket).await
}

pub(crate) async fn get_bucket_cors_config(bucket: &str) -> Result<(s3s::dto::CORSConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_cors_config(bucket).await
}

pub(crate) async fn get_bucket_logging_config(bucket: &str) -> Result<(s3s::dto::BucketLoggingStatus, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_logging_config(bucket).await
}

pub(crate) async fn get_bucket_object_lock_config(
    bucket: &str,
) -> Result<(s3s::dto::ObjectLockConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_object_lock_config(bucket).await
}

pub(crate) async fn get_public_access_block_config(
    bucket: &str,
) -> Result<(s3s::dto::PublicAccessBlockConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_public_access_block_config(bucket).await
}

pub(crate) async fn get_bucket_replication_config(
    bucket: &str,
) -> Result<(s3s::dto::ReplicationConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_replication_config(bucket).await
}

pub(crate) async fn get_bucket_request_payment_config(
    bucket: &str,
) -> Result<(s3s::dto::RequestPaymentConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_request_payment_config(bucket).await
}

pub(crate) async fn get_bucket_sse_config(
    bucket: &str,
) -> Result<(s3s::dto::ServerSideEncryptionConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_sse_config(bucket).await
}

pub(crate) async fn get_bucket_website_config(bucket: &str) -> Result<(s3s::dto::WebsiteConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_website_config(bucket).await
}

pub(crate) async fn set_bucket_metadata(bucket: String, bm: BucketMetadata) -> Result<()> {
    ecstore_bucket::metadata_sys::set_bucket_metadata(bucket, bm).await
}

pub(crate) async fn update_bucket_metadata_config(
    bucket: &str,
    config_file: &str,
    data: Vec<u8>,
) -> Result<time::OffsetDateTime> {
    ecstore_bucket::metadata_sys::update(bucket, config_file, data).await
}

pub(crate) fn add_object_lock_years(dt: time::OffsetDateTime, years: i32) -> time::OffsetDateTime {
    ecstore_bucket::object_lock::objectlock_sys::add_years(dt, years)
}

pub(crate) fn check_retention_for_modification(
    user_defined: &std::collections::HashMap<String, String>,
    new_mode: Option<&str>,
    new_retain_until: Option<time::OffsetDateTime>,
    bypass_governance: bool,
) -> Option<ObjectLockBlockReason> {
    ecstore_bucket::object_lock::objectlock_sys::check_retention_for_modification(
        user_defined,
        new_mode,
        new_retain_until,
        bypass_governance,
    )
}

pub(crate) async fn record_replication_proxy(bucket: &str, api: &str, is_err: bool) {
    if let Some(stats) = ecstore_bucket::replication::GLOBAL_REPLICATION_STATS.get() {
        stats.inc_proxy(bucket, api, is_err).await;
    }
}

pub(crate) fn decode_tags(tags: &str) -> Vec<s3s::dto::Tag> {
    ecstore_bucket::tagging::decode_tags(tags)
}

pub(crate) fn decode_tags_to_map(tags: &str) -> std::collections::HashMap<String, String> {
    ecstore_bucket::tagging::decode_tags_to_map(tags)
}

pub(crate) fn encode_tags(tags: Vec<s3s::dto::Tag>) -> String {
    ecstore_bucket::tagging::encode_tags(tags)
}

pub(crate) fn serialize<T: s3s::xml::Serialize>(val: &T) -> s3s::xml::SerResult<Vec<u8>> {
    ecstore_bucket::utils::serialize(val)
}

pub(crate) fn is_err_bucket_not_found(err: &Error) -> bool {
    ecstore_error::is_err_bucket_not_found(err)
}

pub(crate) fn is_err_object_not_found(err: &Error) -> bool {
    ecstore_error::is_err_object_not_found(err)
}

pub(crate) fn is_err_version_not_found(err: &Error) -> bool {
    ecstore_error::is_err_version_not_found(err)
}

pub(crate) fn is_all_buckets_not_found(errs: &[Option<DiskError>]) -> bool {
    ecstore_disk::error_reduce::is_all_buckets_not_found(errs)
}

pub(crate) fn get_global_lock_client() -> Option<Arc<dyn rustfs_lock::client::LockClient>> {
    ecstore_global::get_global_lock_client()
}

pub(crate) fn get_global_lock_clients()
-> Option<&'static std::collections::HashMap<String, Arc<dyn rustfs_lock::client::LockClient>>> {
    ecstore_global::get_global_lock_clients()
}

pub(crate) fn get_global_bucket_monitor() -> Option<Arc<BucketBandwidthMonitor>> {
    ecstore_global::get_global_bucket_monitor()
}

pub(crate) fn get_global_endpoints_opt() -> Option<EndpointServerPools> {
    ecstore_global::get_global_endpoints_opt()
}

pub(crate) fn get_global_deployment_id() -> Option<String> {
    ecstore_global::get_global_deployment_id()
}

pub(crate) fn get_global_region() -> Option<s3s::region::Region> {
    ecstore_global::get_global_region()
}

pub(crate) fn global_rustfs_port() -> u16 {
    ecstore_global::global_rustfs_port()
}

pub(crate) fn get_global_tier_config_mgr() -> Arc<tokio::sync::RwLock<TierConfigMgr>> {
    ecstore_global::get_global_tier_config_mgr()
}

pub(crate) fn new_object_layer_fn() -> Option<Arc<ECStore>> {
    ecstore_global::new_object_layer_fn()
}

pub(crate) fn set_object_store_resolver(resolver: Arc<ObjectStoreResolver>) -> bool {
    ecstore_global::set_object_store_resolver(resolver)
}

pub(crate) fn get_global_notification_sys() -> Option<&'static NotificationSys> {
    ecstore_notification::get_global_notification_sys()
}

pub(crate) async fn is_dist_erasure() -> bool {
    ecstore_global::is_dist_erasure().await
}

pub(crate) fn resolve_object_store_handle() -> Option<Arc<ECStore>> {
    ecstore_global::resolve_object_store_handle()
}

pub(crate) async fn collect_local_metrics(
    types: MetricType,
    opts: &CollectMetricsOpts,
) -> rustfs_madmin::metrics::RealtimeMetrics {
    ecstore_metrics::collect_local_metrics(types, opts).await
}

pub(crate) fn verify_rpc_signature(url: &str, method: &http::Method, headers: &http::HeaderMap) -> std::io::Result<()> {
    ecstore_rpc::verify_rpc_signature(url, method, headers)
}

pub(crate) fn to_s3s_etag(etag: &str) -> s3s::dto::ETag {
    ecstore_client::object_api_utils::to_s3s_etag(etag)
}

pub(crate) fn table_catalog_path_hash(value: &str) -> String {
    ecstore_bucket::metadata::table_catalog_path_hash(value)
}

pub(crate) fn get_lock_acquire_timeout() -> std::time::Duration {
    ecstore_set_disk::get_lock_acquire_timeout()
}

#[cfg(test)]
pub(crate) fn boxed_reader<R>(reader: R) -> DynReader
where
    R: ecstore_rio::Reader + 'static,
{
    ecstore_rio::boxed_reader(reader)
}

pub(crate) fn compression_metadata_value(algorithm: rustfs_utils::CompressionAlgorithm) -> String {
    ecstore_rio::compression_metadata_value(algorithm)
}

pub(crate) fn wrap_reader<R>(reader: R) -> DynReader
where
    R: ecstore_rio::ReadStream + 'static,
{
    ecstore_rio::wrap_reader(reader)
}

pub(crate) fn is_valid_storage_class(storage_class: &str) -> bool {
    ecstore_set_disk::is_valid_storage_class(storage_class)
}

pub(crate) fn register_event_dispatch_hook<F>(hook: F) -> bool
where
    F: Fn(EventArgs) + Send + Sync + 'static,
{
    ecstore_event::register_event_dispatch_hook(hook)
}

pub(crate) fn topology_snapshot_from_endpoint_pools_with_capabilities(
    endpoint_pools: &EndpointServerPools,
    capabilities: rustfs_storage_api::TopologyCapabilities,
    disk_capabilities: rustfs_storage_api::DiskCapabilities,
) -> rustfs_storage_api::TopologySnapshot {
    ecstore_cluster::topology_snapshot_from_endpoint_pools_with_capabilities(endpoint_pools, capabilities, disk_capabilities)
}

pub(crate) async fn reload_transition_tier_config(api: Arc<ECStore>) -> std::io::Result<()> {
    ecstore_global::GLOBAL_TierConfigMgr.write().await.reload(api).await
}

pub(crate) async fn all_local_disk_path() -> Vec<String> {
    ecstore_storage::all_local_disk_path().await
}

pub(crate) async fn find_local_disk_by_ref(disk_ref: &str) -> Option<DiskStore> {
    ecstore_storage::find_local_disk_by_ref(disk_ref).await
}

pub(crate) trait StorageReplicationConfigExt {
    fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool;
}

impl StorageReplicationConfigExt for s3s::dto::ReplicationConfiguration {
    fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool {
        <s3s::dto::ReplicationConfiguration as ecstore_bucket::replication::ReplicationConfigurationExt>::has_active_rules(
            self, prefix, recursive,
        )
    }
}

pub(crate) trait StorageVersioningConfigExt {
    fn enabled(&self) -> bool;
}

impl StorageVersioningConfigExt for s3s::dto::VersioningConfiguration {
    fn enabled(&self) -> bool {
        <s3s::dto::VersioningConfiguration as ecstore_bucket::versioning::VersioningApi>::enabled(self)
    }
}

pub(crate) type GetObjectReader = <ECStore as rustfs_storage_api::ObjectIO>::GetObjectReader;
pub(crate) type ObjectInfo = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub(crate) type ObjectOptions = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub(crate) type PutObjReader = <ECStore as rustfs_storage_api::ObjectIO>::PutObjectReader;
