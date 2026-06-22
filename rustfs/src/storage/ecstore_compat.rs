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
