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

//! Explicit ECStore public facades for outer crate compatibility boundaries.

pub mod admin {
    pub use crate::diagnostics::admin_server_info::{get_local_server_property, get_server_info};
}

pub mod bitrot {
    pub use crate::io_support::bitrot::{create_bitrot_reader, create_bitrot_writer};
}

pub mod bucket {
    pub use crate::bucket::{
        bandwidth, bucket_target_sys, lifecycle, metadata, metadata_sys, migration, object_lock, policy_sys, quota, replication,
        tagging, target, utils, versioning, versioning_sys,
    };
}

pub mod cache {
    pub use crate::cache_value::metacache_set::{ListPathRawOptions, list_path_raw};
}

pub mod capacity {
    pub use crate::core::pools::{
        PoolDecommissionInfo, PoolStatus, get_total_usable_capacity, get_total_usable_capacity_free, path2_bucket_object,
        path2_bucket_object_with_base_path,
    };
    pub use crate::store::utils::is_reserved_or_invalid_bucket;
}

pub mod client {
    pub use crate::client::{admin_handler_utils, api_put_object, object_api_utils, transition_api};
}

pub mod cluster {
    pub use crate::cluster::{
        ClusterControlPlane, ClusterControlPlaneSnapshot, ClusterDriveMembership, ClusterEndpointType, ClusterLocalNodeStorage,
        ClusterLocalNodeStorageSnapshot, ClusterMembershipSnapshot, ClusterNodeMembership, ClusterPeerHealth,
        ClusterPeerHealthSnapshot, ClusterPoolState, ClusterPoolStateSnapshot, ClusterRpcBoundarySnapshot,
        ClusterRpcChannelSnapshot, ClusterRpcPlane, ClusterRpcTransport, local_node_storage_snapshot_from_membership,
        membership_snapshot_from_endpoint_pools, peer_health_snapshot_from_membership, pool_state_snapshot_from_endpoint_pools,
        rpc_boundary_snapshot, topology_snapshot_from_endpoint_pools, topology_snapshot_from_endpoint_pools_with_capabilities,
    };
}

pub mod compression {
    pub use crate::io_support::compress::{MIN_DISK_COMPRESSIBLE_SIZE, is_disk_compressible};
}

pub mod config {
    pub use crate::config::{
        RUSTFS_CONFIG_PREFIX, com, init, init_global_config_sys, set_global_storage_class, storageclass,
        try_migrate_server_config,
    };
}

pub mod data_usage {
    pub use crate::data_usage::{
        DATA_USAGE_CACHE_NAME, apply_bucket_usage_memory_overlay, load_data_usage_from_backend,
        record_bucket_delete_marker_memory, record_bucket_object_delete_memory, record_bucket_object_version_write_memory,
        record_bucket_object_write_memory, refresh_versioned_bucket_usage_from_object_layer, remove_bucket_usage_from_backend,
        replace_bucket_usage_memory_from_info,
    };
}

pub mod disk {
    pub use crate::disk::endpoint::Endpoint;
    pub use crate::disk::error::DiskError;
    pub use crate::disk::error_reduce::is_all_buckets_not_found;
    pub use crate::disk::local::ScanGuard;
    pub use crate::disk::{
        BUCKET_META_PREFIX, CheckPartsResp, DeleteOptions, Disk, DiskAPI, DiskInfo, DiskInfoOptions, DiskLocation, DiskOption,
        DiskStore, FileInfoVersions, FileReader, FileWriter, RUSTFS_META_BUCKET, ReadMultipleReq, ReadMultipleResp, ReadOptions,
        RenameDataResp, STORAGE_FORMAT_FILE, UpdateMetadataOpts, VolumeInfo, WalkDirOptions, new_disk,
    };
    pub use crate::disk::{endpoint, error, error_reduce};
    pub use bytes::Bytes;
}

pub mod error {
    pub use crate::error::{
        Error, Result, StorageError, classify_system_path_failure_reason, is_err_bucket_not_found, is_err_object_not_found,
        is_err_version_not_found,
    };
}

pub mod erasure {
    pub use crate::erasure::coding::{
        BitrotReader, BitrotWriter, BitrotWriterWrapper, CustomWriter, Erasure, ReedSolomonEncoder, calc_shard_size,
        calc_shard_size_legacy,
    };
}

pub mod event {
    pub use crate::event::name::EventName;
    pub use crate::services::event_notification::{EventArgs, register_event_dispatch_hook};
}

pub mod global {
    pub use crate::runtime::global::{
        GLOBAL_BOOT_TIME, GLOBAL_LOCAL_DISK_MAP, GLOBAL_TierConfigMgr, get_global_bucket_monitor, get_global_deployment_id,
        get_global_endpoints_opt, get_global_lock_client, get_global_lock_clients, get_global_region, get_global_tier_config_mgr,
        global_rustfs_port, is_dist_erasure, is_erasure, is_erasure_sd, is_first_cluster_node_local, new_object_layer_fn,
        resolve_object_store_handle, set_global_endpoints, set_global_region, set_global_rustfs_port, set_object_store_resolver,
        shutdown_background_services, update_erasure_type,
    };
}

pub mod layout {
    pub use crate::layout::disks_layout::DisksLayout;
    pub use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints, SetupType};
}

pub mod metrics {
    pub use crate::services::metrics_realtime::{CollectMetricsOpts, MetricType, collect_local_metrics};
}

pub mod notification {
    pub use crate::services::notification_sys::{
        NotificationPeerErr, NotificationSys, get_global_notification_sys, new_global_notification_sys,
    };
}

pub mod object {
    pub use crate::object_api::{
        BLOCK_SIZE_V2, ERASURE_ALGORITHM, GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader, RangedDecompressReader,
        StreamConsumer,
    };
}

pub mod rebalance {
    pub use crate::services::rebalance::{
        DiskStat, RebalSaveOpt, RebalStatus, RebalanceCleanupWarningEntry, RebalanceCleanupWarnings, RebalanceInfo,
        RebalanceMeta, RebalanceStats, RebalanceStopPropagationRecord, decode_rebalance_stop_propagation_record,
        encode_rebalance_stop_propagation_record,
    };
}

pub mod rio {
    pub use crate::io_support::rio::{
        DecryptReader, DynReader, EncryptReader, HardLimitReader, HashReader, ReadStream, Reader, WriteEncryption, WritePlan,
        boxed_reader, compression_metadata_value, wrap_reader,
    };
}

pub mod rpc {
    pub use crate::cluster::rpc::{
        LocalPeerS3Client, PEER_RESTSIGNAL, PEER_RESTSUB_SYS, PeerRestClient, PeerS3Client, SERVICE_SIGNAL_REFRESH_CONFIG,
        SERVICE_SIGNAL_RELOAD_DYNAMIC, TONIC_RPC_PREFIX, TonicInterceptor, gen_tonic_signature_interceptor,
        node_service_time_out_client, node_service_time_out_client_no_auth, verify_rpc_signature,
    };
}

pub mod set_disk {
    pub use crate::set_disk::{DEFAULT_READ_BUFFER_SIZE, SetDisks, get_lock_acquire_timeout, is_valid_storage_class};
}

pub mod store_list {
    pub use crate::store::list_objects::{ListPathOptions, max_keys_plus_one};
}

pub mod storage {
    pub use crate::store::{
        ECStore, all_local_disk, all_local_disk_path, find_local_disk_by_ref, init_local_disks, init_lock_clients,
        prewarm_local_disk_id_map,
    };
}

pub mod tier {
    pub use crate::services::tier::{tier, tier_admin, tier_config, tier_handlers, warm_backend};
}
