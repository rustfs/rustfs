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
    pub(crate) mod service {
        pub(crate) use crate::storage::storage_api::{all_local_disk, disk_drive_path, disk_endpoint};
    }
}

pub(crate) mod cluster {
    pub(crate) mod contract {
        pub(crate) mod capability {
            #[cfg(test)]
            pub(crate) use super::super::super::storage_contracts::CapabilityState;
            pub(crate) use super::super::super::storage_contracts::{
                CapabilitySnapshotError, CapabilityStatus, MemorySamplingState, PlatformSupport, UserspaceProfilingCapability,
            };
        }

        pub(crate) mod observability {
            pub(crate) use super::super::super::storage_contracts::{ObservabilitySnapshot, ObservabilitySnapshotProvider};
        }

        pub(crate) mod topology {
            pub(crate) use super::super::super::storage_contracts::{
                DiskCapabilities, TopologyCapabilities, TopologySnapshot, TopologySnapshotProvider,
            };
        }
    }

    pub(crate) use crate::storage::storage_api::{EndpointServerPools, topology_snapshot_from_endpoint_pools_with_capabilities};

    #[cfg(test)]
    pub(crate) use crate::storage::storage_api::{Endpoint, Endpoints, PoolEndpoints};

    pub(crate) mod control_plane {
        pub(crate) use crate::storage::storage_api::ecstore_cluster::{
            ClusterControlPlane, ClusterControlPlaneSnapshot, ClusterLocalNodeStorageSnapshot, ClusterMembershipSnapshot,
            ClusterPeerHealthSnapshot, ClusterPoolStateSnapshot, ClusterRpcBoundarySnapshot,
        };
    }
}

#[cfg(test)]
pub(crate) mod config_test {
    pub(crate) use crate::storage::storage_api::DisksLayout;
}

pub(crate) mod error {
    pub(crate) mod contract {
        pub(crate) mod range {
            pub(crate) use super::super::super::storage_contracts::HTTPRangeError;
        }
    }

    pub(crate) use crate::storage::storage_api::{QuotaError, StorageError};
}

pub(crate) mod protocols {
    pub(crate) mod client {
        pub(crate) use crate::storage::storage_api::access_consumer::ReqInfo;
        pub(crate) type FS = crate::storage::storage_api::FS;
        pub(crate) use crate::storage::storage_api::request_context_consumer::RequestContext;
    }
}

pub(crate) mod server {
    pub(crate) mod event {
        pub(crate) mod contract {
            #[cfg(test)]
            pub(crate) mod lifecycle {
                pub(crate) use super::super::super::super::storage_contracts::TransitionedObject;
            }
        }

        pub(crate) use crate::storage::storage_api::{EventArgs, StorageObjectInfo, register_event_dispatch_hook};
    }

    pub(crate) mod http {
        pub(crate) use crate::storage::storage_api::{TONIC_RPC_PREFIX, verify_rpc_signature};

        pub(crate) mod ecfs {
            pub(crate) type FS = crate::storage::storage_api::FS;
        }

        pub(crate) mod metadata_route {
            pub(crate) fn with_metadata_route<A>(admin: A, host: Option<s3s::host::MultiDomain>) -> impl s3s::route::S3Route
            where
                A: s3s::route::S3Route,
            {
                crate::app::metadata_route::with_metadata_route(admin, host)
            }
        }

        pub(crate) mod request_context {
            pub(crate) use crate::storage::storage_api::request_context_consumer::{
                RequestContext, extract_request_id_from_headers,
            };
        }

        pub(crate) mod rpc {
            pub(crate) use crate::storage::storage_api::rpc_consumer::InternodeRpcService;
        }

        pub(crate) mod tonic_service {
            pub(crate) use crate::storage::storage_api::tonic_service_consumer::make_server;
        }
    }

    pub(crate) mod layer {
        pub(crate) use crate::storage::storage_api::apply_cors_headers;

        pub(crate) mod request_context {
            pub(crate) use crate::storage::storage_api::request_context_consumer::{
                RequestContext, extract_request_id_from_headers, extract_trace_context_ids_from_headers, spawn_traced,
            };
        }
    }

    pub(crate) mod module_switch {
        pub(crate) use crate::storage::storage_api::{Error, read_config, save_config};
    }

    pub(crate) mod readiness {
        pub(crate) mod contract {
            pub(crate) mod admin {
                pub(crate) use super::super::super::super::storage_contracts::StorageAdminApi;
            }
        }

        pub(crate) use crate::storage::storage_api::{Endpoint, EndpointServerPools, is_dist_erasure};

        #[cfg(test)]
        pub(crate) use crate::storage::storage_api::{Endpoints, PoolEndpoints};
    }

    pub(crate) mod runtime_sources {
        pub(crate) use crate::storage::storage_api::{ECStore, EndpointServerPools};
    }
}

pub(crate) mod startup {
    pub(crate) mod background {
        pub(crate) use crate::storage::storage_api::{ECStore, set_workload_admission_snapshot_provider};
    }

    pub(crate) mod bucket_metadata {
        pub(crate) mod contract {
            pub(crate) mod bucket {
                pub(crate) use super::super::super::super::storage_contracts::{BucketOperations, BucketOptions};
            }
        }

        pub(crate) use crate::storage::storage_api::{
            ECStore, init_bucket_metadata_sys, try_migrate_bucket_metadata, try_migrate_iam_config,
        };
    }

    pub(crate) mod deadlock {
        pub(crate) use crate::storage::storage_api::deadlock_detector_consumer::get_deadlock_detector;
    }

    pub(crate) mod fs_guard {
        pub(crate) use crate::storage::storage_api::EndpointServerPools;
    }

    pub(crate) mod iam {
        pub(crate) use crate::storage::storage_api::ECStore;
    }

    pub(crate) mod init {
        pub(crate) use crate::storage::storage_api::{
            get_bucket_notification_config, process_lambda_configurations, process_queue_configurations,
            process_topic_configurations,
        };

        pub(crate) mod concurrency {
            pub(crate) use crate::storage::storage_api::concurrency_consumer::get_concurrency_manager;
        }

        pub(crate) mod ecfs {
            pub(crate) type FS = crate::storage::storage_api::FS;
        }
    }

    pub(crate) mod lifecycle {
        pub(crate) use crate::storage::storage_api::ECStore;
    }

    pub(crate) mod notification {
        pub(crate) use crate::storage::storage_api::{EndpointServerPools, Result, new_global_notification_sys};

        #[cfg(test)]
        pub(crate) use crate::storage::storage_api::Error;
    }

    pub(crate) mod runtime_sources {
        pub(crate) use crate::storage::storage_api::{DynReplicationPool, set_global_region, set_global_rustfs_port};
    }

    pub(crate) mod services {
        pub(crate) use crate::storage::storage_api::{ECStore, EndpointServerPools};
    }

    pub(crate) mod shutdown {
        pub(crate) use crate::storage::storage_api::{
            shutdown_background_monitors, shutdown_background_services, store_compression_total_in_backend,
        };
    }

    pub(crate) mod storage {
        pub(crate) use crate::storage::storage_api::{
            ECStore, EndpointServerPools, global_config_init_error_is_deterministic, init_background_replication,
            init_compression_total_memory_from_backend, init_ecstore_config, init_global_config_sys, init_local_disks,
            init_lock_clients, prewarm_local_disk_id_map, set_global_endpoints, try_migrate_server_config, update_erasure_type,
        };
    }
}

pub(crate) mod workload {
    pub(crate) mod admission {
        pub(crate) use crate::storage::storage_api::{bucket_metadata_runtime_initialized, replication_queue_current_count};
    }

    pub(crate) mod concurrency {
        pub(crate) use crate::storage::storage_api::concurrency_consumer::get_concurrency_manager;
    }
}

pub(crate) mod table {
    pub(crate) mod contract {
        pub(crate) mod http {
            pub(crate) use super::super::super::storage_contracts::HTTPPreconditions;
        }

        pub(crate) mod list {
            pub(crate) use super::super::super::storage_contracts::{
                ListObjectVersionsInfo, ListObjectsV2Info, ListOperations, ObjectInfoOrErr, WalkOptions,
            };
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

    pub(crate) use crate::storage::storage_api::{
        BUCKET_TABLE_CATALOG_META_PREFIX, BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX, BUCKET_TABLE_CONFIG,
        BUCKET_TABLE_RESERVED_PREFIX, Error, RUSTFS_META_BUCKET, StorageDeletedObject, StorageError, StorageGetObjectReader,
        StorageObjectInfo, StorageObjectOptions, StorageObjectToDelete, StoragePutObjReader, get_bucket_metadata,
        get_lock_acquire_timeout, table_catalog_path_hash,
    };
}
