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

//! Root-local boundary for storage owner APIs used by startup, server, and
//! other RustFS outer runtime modules.

pub(crate) use crate::storage::{
    BUCKET_TABLE_CATALOG_META_PREFIX, BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX, BUCKET_TABLE_CONFIG,
    BUCKET_TABLE_RESERVED_PREFIX, DynReplicationPool, ECStore, Endpoint, EndpointServerPools, Error, EventArgs, QuotaError,
    RUSTFS_META_BUCKET, Result, StorageDeletedObject, StorageError, StorageGetObjectReader, StorageObjectInfo,
    StorageObjectOptions, StorageObjectToDelete, StoragePutObjReader, TONIC_RPC_PREFIX, all_local_disk, apply_cors_headers,
    bucket_metadata_runtime_initialized, disk_drive_path, disk_endpoint, get_bucket_metadata, get_bucket_notification_config,
    get_lock_acquire_timeout, init_background_replication, init_bucket_metadata_sys, init_ecstore_config, init_global_config_sys,
    init_local_disks, init_lock_clients, is_dist_erasure, new_global_notification_sys, prewarm_local_disk_id_map,
    process_lambda_configurations, process_queue_configurations, process_topic_configurations, read_config,
    register_event_dispatch_hook, replication_queue_current_count, save_config, set_global_endpoints, set_global_region,
    set_global_rustfs_port, shutdown_background_services, table_catalog_path_hash,
    topology_snapshot_from_endpoint_pools_with_capabilities, try_migrate_bucket_metadata, try_migrate_iam_config,
    try_migrate_server_config, update_erasure_type, verify_rpc_signature,
};
pub(crate) use rustfs_storage_api::{
    BucketOperations, BucketOptions, CapabilitySnapshotError, CapabilityStatus, DiskCapabilities, HTTPPreconditions,
    HTTPRangeError, HTTPRangeSpec, ListObjectVersionsInfo, ListObjectsV2Info, ListOperations, MemorySamplingState,
    NamespaceLocking, ObjectIO, ObjectInfoOrErr, ObjectOperations, ObservabilitySnapshot, ObservabilitySnapshotProvider,
    PlatformSupport, StorageAdminApi, TopologyCapabilities, TopologySnapshot, TopologySnapshotProvider,
    UserspaceProfilingCapability, WalkOptions,
};

#[cfg(test)]
pub(crate) use crate::storage::{DisksLayout, Endpoints, PoolEndpoints};
#[cfg(test)]
pub(crate) use rustfs_storage_api::{CapabilityState, TransitionedObject};

pub(crate) mod access {
    pub(crate) use crate::storage::access::ReqInfo;
}

pub(crate) mod concurrency {
    pub(crate) use crate::storage::concurrency::get_concurrency_manager;
}

pub(crate) mod deadlock_detector {
    pub(crate) use crate::storage::deadlock_detector::get_deadlock_detector;
}

pub(crate) mod ecfs {
    pub(crate) type FS = crate::storage::FS;
}

pub(crate) mod ecstore_cluster {
    pub(crate) use crate::storage::ecstore_cluster::{
        ClusterControlPlane, ClusterControlPlaneSnapshot, ClusterLocalNodeStorageSnapshot, ClusterMembershipSnapshot,
        ClusterPeerHealthSnapshot, ClusterPoolStateSnapshot,
    };
}

pub(crate) mod request_context {
    pub(crate) use crate::storage::request_context::{
        RequestContext, extract_request_id_from_headers, extract_trace_context_ids_from_headers, spawn_traced,
    };
}

pub(crate) mod rpc {
    pub(crate) use crate::storage::rpc::InternodeRpcService;
}

pub(crate) mod tonic_service {
    pub(crate) use crate::storage::tonic_service::make_server;
}
