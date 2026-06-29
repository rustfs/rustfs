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
pub(crate) mod runtime_sources;
#[cfg(test)]
mod sse_test;
pub(crate) mod storage_api;

pub(crate) use ecfs_extend::*;
#[allow(unused_imports)]
pub(crate) use storage_api::{
    BUCKET_ACCELERATE_CONFIG, BUCKET_LOGGING_CONFIG, BUCKET_REQUEST_PAYMENT_CONFIG, BUCKET_TABLE_CATALOG_META_PREFIX,
    BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX, BUCKET_TABLE_CONFIG, BUCKET_TABLE_RESERVED_PREFIX, BUCKET_VERSIONING_CONFIG,
    BUCKET_WEBSITE_CONFIG, BucketBandwidthMonitor, BucketMetadata, BucketVersioningSys, CheckPartsResp, CollectMetricsOpts,
    DEFAULT_READ_BUFFER_SIZE, DailyAllTierStats, DeleteOptions, DiskError, DiskInfo, DiskInfoOptions, DiskResult, DiskStore,
    DynReader, DynReplicationPool, ECStore, Endpoint, EndpointServerPools, Error, EventArgs, ExpiryState, FS, FileInfoVersions,
    FileReader, FileWriter, GetObjectReader, HashReader, LocalPeerS3Client, MetricType, NotificationSys, OBJECT_LOCK_CONFIG,
    ObjectInfo, ObjectLockBlockReason, ObjectOptions, ObjectPartInfo, PEER_RESTSIGNAL, PEER_RESTSUB_SYS, PolicySys,
    PoolEndpoints, PutObjReader, QuotaError, RUSTFS_META_BUCKET, RawFileInfo, ReadMultipleReq, ReadMultipleResp, ReadOptions,
    RenameDataResp, ReplicationStats, Result, SERVICE_SIGNAL_REFRESH_CONFIG, SERVICE_SIGNAL_RELOAD_DYNAMIC, SetupType,
    StorageDeletedObject, StorageDiskRpcExt, StorageError, StorageGetObjectReader, StorageObjectInfo, StorageObjectOptions,
    StorageObjectToDelete, StoragePeerS3ClientExt, StoragePutObjReader, StorageReplicationConfigExt, StorageVersioningConfigExt,
    TONIC_RPC_PREFIX, TierConfigMgr, UpdateMetadataOpts, VolumeInfo, WalkDirOptions, WorkloadAdmissionSnapshotProviderRef,
    WriteEncryption, WritePlan, access_consumer, add_object_lock_years, all_local_disk, all_local_disk_path,
    bucket_metadata_runtime_initialized, check_retention_for_modification, collect_local_metrics, compression_metadata_value,
    contract, decode_tags, decode_tags_to_map, delete_bucket_metadata_config, disk_drive_path, disk_endpoint, ecfs_consumer,
    ecfs_extend_consumer, ecstore_admin, ecstore_bucket, ecstore_capacity, ecstore_client, ecstore_cluster, ecstore_compression,
    ecstore_config, ecstore_data_usage, ecstore_disk, ecstore_error, ecstore_event, ecstore_layout, ecstore_metrics,
    ecstore_notification, ecstore_rebalance, ecstore_rio, ecstore_rpc, ecstore_set_disk, ecstore_storage, ecstore_tier,
    encode_tags, find_local_disk_by_ref, get_bucket_accelerate_config, get_bucket_cors_config, get_bucket_logging_config,
    get_bucket_metadata, get_bucket_notification_config, get_bucket_object_lock_config, get_bucket_policy_raw,
    get_bucket_replication_config, get_bucket_request_payment_config, get_bucket_sse_config, get_bucket_website_config,
    get_global_boot_time, get_global_bucket_monitor, get_global_deployment_id, get_global_endpoints_opt, get_global_expiry_state,
    get_global_lock_client, get_global_lock_clients, get_global_notification_sys, get_global_region, get_global_replication_pool,
    get_global_replication_stats, get_global_tier_config_mgr, get_local_server_property, get_lock_acquire_timeout,
    get_public_access_block_config, global_rustfs_port, head_prefix_consumer, helper_consumer, init_background_replication,
    init_bucket_metadata_sys, init_ecstore_config, init_global_config_sys, init_local_disks, init_lock_clients,
    is_all_buckets_not_found, is_dist_erasure, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found,
    is_valid_storage_class, load_bucket_metadata, new_global_notification_sys, options_consumer, prewarm_local_disk_id_map,
    read_config, record_replication_proxy, register_event_dispatch_hook, reload_transition_tier_config,
    replication_queue_current_count, rpc_consumer, runtime_sources_consumer, s3_api_consumer, save_config, serialize,
    set_bucket_metadata, set_global_endpoints, set_global_region, set_global_rustfs_port,
    set_workload_admission_snapshot_provider, shutdown_background_services, table_catalog_path_hash, to_s3s_etag,
    topology_snapshot_from_endpoint_pools_with_capabilities, try_migrate_bucket_metadata, try_migrate_iam_config,
    try_migrate_server_config, update_bucket_metadata_config, update_erasure_type, verify_rpc_signature, wrap_reader,
};

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use storage_api::{
    BucketMetadataSys, DecryptReader, DisksLayout, EncryptReader, Endpoints, HardLimitReader, STORAGE_CLASS_SUB_SYS,
    boxed_reader, get_global_bucket_metadata_sys, test_consumer,
};
