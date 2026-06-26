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

//! Root-local domain boundaries for storage owner APIs used by RustFS outer
//! runtime modules.

use rustfs_storage_api as storage_contracts;

pub(crate) mod capacity {
    pub(crate) use crate::storage::{all_local_disk, disk_drive_path, disk_endpoint};
}

pub(crate) mod cluster {
    pub(crate) use super::storage_contracts::{
        CapabilitySnapshotError, CapabilityStatus, DiskCapabilities, MemorySamplingState, ObservabilitySnapshot,
        ObservabilitySnapshotProvider, PlatformSupport, TopologyCapabilities, TopologySnapshot, TopologySnapshotProvider,
        UserspaceProfilingCapability,
    };
    pub(crate) use crate::storage::{EndpointServerPools, topology_snapshot_from_endpoint_pools_with_capabilities};

    #[cfg(test)]
    pub(crate) use super::storage_contracts::CapabilityState;
    #[cfg(test)]
    pub(crate) use crate::storage::{Endpoint, Endpoints, PoolEndpoints};

    pub(crate) mod control_plane {
        pub(crate) use crate::storage::ecstore_cluster::{
            ClusterControlPlane, ClusterControlPlaneSnapshot, ClusterLocalNodeStorageSnapshot, ClusterMembershipSnapshot,
            ClusterPeerHealthSnapshot, ClusterPoolStateSnapshot,
        };
    }
}

#[cfg(test)]
pub(crate) mod config_test {
    pub(crate) use crate::storage::DisksLayout;
}

pub(crate) mod error {
    pub(crate) use super::storage_contracts::HTTPRangeError;
    pub(crate) use crate::storage::{QuotaError, StorageError};
}

pub(crate) mod protocols {
    pub(crate) mod access {
        pub(crate) use crate::storage::access::ReqInfo;
    }

    pub(crate) mod ecfs {
        pub(crate) type FS = crate::storage::FS;
    }

    pub(crate) mod request_context {
        pub(crate) use crate::storage::request_context::RequestContext;
    }
}

pub(crate) mod server {
    pub(crate) use super::storage_contracts::StorageAdminApi;
    pub(crate) use crate::storage::{
        ECStore, Endpoint, EndpointServerPools, Error, EventArgs, StorageObjectInfo, TONIC_RPC_PREFIX, apply_cors_headers,
        is_dist_erasure, read_config, register_event_dispatch_hook, save_config, verify_rpc_signature,
    };

    #[cfg(test)]
    pub(crate) use super::storage_contracts::TransitionedObject;
    #[cfg(test)]
    pub(crate) use crate::storage::{Endpoints, PoolEndpoints};

    pub(crate) mod ecfs {
        pub(crate) type FS = crate::storage::FS;
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
}

pub(crate) mod startup {
    pub(crate) use super::storage_contracts::{BucketOperations, BucketOptions};
    pub(crate) use crate::storage::{
        DynReplicationPool, ECStore, EndpointServerPools, Result, get_bucket_notification_config, init_background_replication,
        init_bucket_metadata_sys, init_ecstore_config, init_global_config_sys, init_local_disks, init_lock_clients,
        new_global_notification_sys, prewarm_local_disk_id_map, process_lambda_configurations, process_queue_configurations,
        process_topic_configurations, set_global_endpoints, set_global_region, set_global_rustfs_port,
        shutdown_background_services, try_migrate_bucket_metadata, try_migrate_iam_config, try_migrate_server_config,
        update_erasure_type,
    };

    #[cfg(test)]
    pub(crate) use crate::storage::Error;

    pub(crate) mod concurrency {
        pub(crate) use crate::storage::concurrency::get_concurrency_manager;
    }

    pub(crate) mod deadlock_detector {
        pub(crate) use crate::storage::deadlock_detector::get_deadlock_detector;
    }

    pub(crate) mod ecfs {
        pub(crate) type FS = crate::storage::FS;
    }
}

pub(crate) mod table {
    pub(crate) use super::storage_contracts::{
        HTTPPreconditions, HTTPRangeSpec, ListObjectVersionsInfo, ListObjectsV2Info, ListOperations, NamespaceLocking, ObjectIO,
        ObjectInfoOrErr, ObjectOperations, WalkOptions,
    };
    pub(crate) use crate::storage::{
        BUCKET_TABLE_CATALOG_META_PREFIX, BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX, BUCKET_TABLE_CONFIG,
        BUCKET_TABLE_RESERVED_PREFIX, Error, RUSTFS_META_BUCKET, StorageDeletedObject, StorageError, StorageGetObjectReader,
        StorageObjectInfo, StorageObjectOptions, StorageObjectToDelete, StoragePutObjReader, get_bucket_metadata,
        get_lock_acquire_timeout, table_catalog_path_hash,
    };
}

pub(crate) mod workload {
    pub(crate) use crate::storage::{bucket_metadata_runtime_initialized, replication_queue_current_count};

    pub(crate) mod concurrency {
        pub(crate) use crate::storage::concurrency::get_concurrency_manager;
    }
}
