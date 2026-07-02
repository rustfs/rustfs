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

//! Storage owner-local boundary for ECStore facade and storage contract symbols.

use std::sync::Arc;

use rustfs_storage_api as storage_contracts;
use tokio_util::sync::CancellationToken;

pub(crate) mod contract {
    pub(crate) mod admin {
        pub(crate) use super::super::storage_contracts::StorageAdminApi;
    }

    pub(crate) mod bucket {
        pub(crate) use super::super::storage_contracts::{
            BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions,
        };
    }

    pub(crate) mod list {
        pub(crate) use super::super::storage_contracts::{ListObjectVersionsInfo, ListObjectsV2Info, ListOperations};
    }

    pub(crate) mod multipart {
        pub(crate) use super::super::storage_contracts::{ListMultipartsInfo, ListPartsInfo};
        #[cfg(test)]
        pub(crate) use super::super::storage_contracts::{MultipartInfo, PartInfo};
    }

    pub(crate) mod object {
        pub(crate) use super::super::storage_contracts::{
            DeletedObject, HTTPPreconditions, ObjectIO, ObjectLockRetentionOptions, ObjectOperations, ObjectToDelete,
        };
    }

    pub(crate) mod range {
        pub(crate) use super::super::storage_contracts::HTTPRangeSpec;
    }

    pub(crate) mod topology {
        pub(crate) use super::super::storage_contracts::{DiskCapabilities, TopologyCapabilities, TopologySnapshot};
    }
}

pub(crate) type StorageDeletedObject = contract::object::DeletedObject;
pub(crate) type StorageGetObjectReader = super::GetObjectReader;
pub(crate) type StorageObjectInfo = super::ObjectInfo;
pub(crate) type StorageObjectOptions = super::ObjectOptions;
pub(crate) type StorageObjectToDelete = contract::object::ObjectToDelete;
pub(crate) type StoragePutObjReader = super::PutObjReader;
pub(crate) use super::ecfs_extend::{
    RFC1123, apply_bucket_default_lock_retention, apply_cors_headers, check_preconditions, get_buffer_size_opt_in,
    get_validated_store, has_replication_rules, parse_object_lock_legal_hold, parse_object_lock_retention,
    parse_part_number_i32_to_usize, process_lambda_configurations, process_queue_configurations, process_topic_configurations,
    remove_object_lock_metadata_for_copy, validate_bucket_object_lock_enabled, validate_list_object_unordered_with_delimiter,
    validate_object_key, wrap_response_with_cors,
};
pub(crate) use super::sse::{
    DecryptionRequest, EncryptionRequest, PrepareEncryptionRequest, extract_server_side_encryption_from_headers, sse_decryption,
    sse_encryption, sse_prepare_encryption, strip_managed_encryption_metadata, validate_sse_headers_for_read,
    validate_sse_headers_for_write, validate_ssec_for_read,
};

pub(crate) mod access_consumer {
    pub(crate) use super::super::access::{
        PostObjectRequestMarker, ReqInfo, authorize_request, has_bypass_governance_header, req_info_mut, req_info_ref,
    };

    pub(crate) mod contract {
        pub(crate) mod bucket {
            pub(crate) use super::super::super::contract::bucket::BucketOperations;
        }
    }
}

pub(crate) mod concurrency_consumer {
    pub(crate) use super::super::concurrency::{
        ConcurrencyManager, GetObjectGuard, IoQueueStatus, IoStrategy, PutObjectGuard, get_concurrency_aware_buffer_size,
        get_concurrency_manager, get_put_concurrency_aware_buffer_size,
    };
}

pub(crate) mod deadlock_detector_consumer {
    #[cfg(test)]
    pub(crate) use super::super::deadlock_detector::RequestHangDetectionPolicy;
    pub(crate) use super::super::deadlock_detector::{DeadlockDetector, get_deadlock_detector};
}

pub(crate) mod ecfs_consumer {
    pub(crate) mod contract {
        pub(crate) mod bucket {
            pub(crate) use super::super::super::contract::bucket::{BucketOperations, BucketOptions};
        }

        pub(crate) mod object {
            pub(crate) use super::super::super::contract::object::{ObjectLockRetentionOptions, ObjectOperations};
        }
    }

    pub(crate) mod object_lock {
        pub(crate) use super::super::super::{
            parse_object_lock_legal_hold, parse_object_lock_retention, validate_bucket_object_lock_enabled,
        };
    }

    pub(crate) type StorageObjectOptions = super::StorageObjectOptions;
}

pub(crate) mod ecfs_extend_consumer {
    pub(crate) mod contract {
        pub(crate) mod bucket {
            pub(crate) use super::super::super::contract::bucket::{BucketOperations, BucketOptions};
        }

        pub(crate) mod object {
            pub(crate) use super::super::super::contract::object::ObjectToDelete;
        }
    }

    pub(crate) type StorageObjectInfo = super::StorageObjectInfo;
}

pub(crate) mod head_prefix_consumer {
    pub(crate) use super::super::head_prefix::{head_prefix_not_found_message, probe_prefix_has_children};

    pub(crate) mod contract {
        pub(crate) mod list {
            pub(crate) use super::super::super::contract::list::ListOperations;
        }
    }
}

pub(crate) mod helper_consumer {
    pub(crate) use super::super::helper::{OperationHelper, spawn_background_with_context};

    pub(crate) type StorageObjectInfo = super::StorageObjectInfo;
}

pub(crate) mod options_consumer {
    pub(crate) use super::super::options::{
        copy_dst_opts, copy_src_opts, del_opts, extract_metadata, extract_metadata_from_mime,
        extract_metadata_from_mime_with_object_name, filter_object_metadata, get_complete_multipart_upload_opts,
        get_content_sha256_with_query, get_opts, normalize_content_encoding_for_storage, parse_copy_source_range, put_opts,
        validate_archive_content_encoding,
    };

    pub(crate) mod contract {
        pub(crate) mod object {
            pub(crate) use super::super::super::contract::object::HTTPPreconditions;
        }

        pub(crate) mod range {
            pub(crate) use super::super::super::contract::range::HTTPRangeSpec;
        }
    }

    pub(crate) type StorageObjectOptions = super::StorageObjectOptions;
}

pub(crate) mod request_context_consumer {
    pub(crate) use super::super::request_context::{
        RequestContext, extract_request_id_from_headers, extract_trace_context_ids_from_headers, spawn_traced,
    };
}

pub(crate) mod rpc_consumer {
    pub(crate) use super::super::rpc::InternodeRpcService;

    pub(crate) mod http_service {
        pub(crate) const DEFAULT_READ_BUFFER_SIZE: usize = super::super::DEFAULT_READ_BUFFER_SIZE;
        pub(crate) use super::super::{StorageDiskRpcExt, WalkDirOptions, find_local_disk_by_ref, verify_rpc_signature};
    }

    pub(crate) mod node_service {
        pub(crate) use super::super::{
            BatchReadVersionReq, BatchReadVersionResp, CollectMetricsOpts, DeleteOptions, DiskError, DiskInfoOptions, DiskStore,
            ECStore, Error, FileInfoVersions, LocalPeerS3Client, MetricType, PEER_RESTSIGNAL, PEER_RESTSUB_SYS, ReadMultipleReq,
            ReadMultipleResp, ReadOptions, SERVICE_SIGNAL_REFRESH_CONFIG, SERVICE_SIGNAL_RELOAD_DYNAMIC, StorageDiskRpcExt,
            StoragePeerS3ClientExt, UpdateMetadataOpts, all_local_disk_path, collect_local_metrics, find_local_disk_by_ref,
            get_local_server_property, load_bucket_metadata, reload_transition_tier_config, set_bucket_metadata,
            validate_batch_read_version_item_count,
        };
        pub(crate) type StorageResult<T> = super::super::Result<T>;

        #[cfg(test)]
        pub(crate) const STORAGE_CLASS_SUB_SYS: &str = super::super::STORAGE_CLASS_SUB_SYS;

        pub(crate) mod contract {
            pub(crate) mod admin {
                pub(crate) use super::super::super::super::contract::admin::StorageAdminApi;
            }

            pub(crate) mod bucket {
                pub(crate) use super::super::super::super::contract::bucket::{
                    BucketOptions, DeleteBucketOptions, MakeBucketOptions,
                };
            }
        }
    }
}

pub(crate) mod runtime_sources_consumer {
    pub(crate) type ECStore = super::ECStore;
    pub(crate) use crate::storage::runtime_sources;
}

pub(crate) mod s3_api_consumer {
    pub(crate) mod bucket {
        pub(crate) use super::super::super::s3_api::bucket::{
            ListObjectVersionsParams, ListObjectsV2Params, build_list_buckets_output, build_list_object_versions_output,
            build_list_objects_output, build_list_objects_v2_output, parse_list_object_versions_params,
            parse_list_objects_v2_params,
        };

        pub(crate) mod contract {
            pub(crate) mod bucket {
                pub(crate) use super::super::super::super::contract::bucket::BucketInfo;
            }

            pub(crate) mod list {
                pub(crate) use super::super::super::super::contract::list::{ListObjectVersionsInfo, ListObjectsV2Info};
            }
        }

        pub(crate) type StorageObjectInfo = super::super::StorageObjectInfo;

        pub(crate) fn to_s3s_etag(etag: &str) -> s3s::dto::ETag {
            super::super::to_s3s_etag(etag)
        }
    }

    pub(crate) mod common {
        pub(crate) use super::super::super::s3_api::common::rustfs_owner;
    }

    pub(crate) mod multipart {
        pub(crate) use super::super::super::s3_api::multipart::{
            ListMultipartUploadsParams, build_list_multipart_uploads_output, build_list_parts_output,
            parse_list_multipart_uploads_params, parse_list_parts_params, parse_upload_part_number,
        };

        pub(crate) mod contract {
            pub(crate) mod multipart {
                pub(crate) use super::super::super::super::contract::multipart::{ListMultipartsInfo, ListPartsInfo};

                #[cfg(test)]
                pub(crate) use super::super::super::super::contract::multipart::{MultipartInfo, PartInfo};
            }
        }

        pub(crate) fn to_s3s_etag(etag: &str) -> s3s::dto::ETag {
            super::super::to_s3s_etag(etag)
        }
    }
}

pub(crate) mod sse_consumer {
    pub(crate) use super::super::sse::{
        EncryptionKeyKind, SSEType, build_ssec_read_headers, encryption_material_to_metadata, extract_ssec_params_from_headers,
        extract_ssekms_context_from_headers, map_get_object_reader_error, mark_encrypted_multipart_metadata,
    };
    pub(crate) use super::{
        DecryptionRequest, EncryptionRequest, PrepareEncryptionRequest, apply_bucket_default_lock_retention,
        extract_server_side_encryption_from_headers, get_buffer_size_opt_in, sse_decryption, sse_encryption,
        sse_prepare_encryption,
    };
}

pub(crate) mod timeout_wrapper_consumer {
    pub(crate) use super::super::timeout_wrapper::{GetObjectTimeoutPolicy, RequestTimeoutWrapper};
}

pub(crate) mod tonic_service_consumer {
    pub(crate) use super::super::tonic_service::make_server;
}

#[cfg(test)]
pub(crate) mod test_consumer {
    pub(crate) use super::super::{
        apply_cors_headers, apply_default_lock_retention_metadata, check_preconditions, decode_tags_to_map,
        get_adaptive_buffer_size_with_profile, get_buffer_size_opt_in, is_etag_equal, matches_origin_pattern, parse_etag,
        parse_object_lock_legal_hold, parse_object_lock_retention, process_lambda_configurations, process_queue_configurations,
        process_topic_configurations, remove_object_lock_metadata_for_copy, remove_object_lock_retention_metadata,
        validate_bucket_object_lock_enabled, validate_list_object_unordered_with_delimiter,
    };
    pub(crate) use super::{
        BucketMetadata, DEFAULT_READ_BUFFER_SIZE, bucket_metadata_sys_initialized, get_global_bucket_metadata_sys,
        set_bucket_metadata,
    };
    pub(crate) type StorageObjectInfo = super::StorageObjectInfo;
}

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
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::client::transition_api;
    pub(crate) use rustfs_ecstore::api::client::{admin_handler_utils, object_api_utils};
}

pub(crate) mod ecstore_compression {
    pub(crate) use rustfs_ecstore::api::compression::{MIN_DISK_COMPRESSIBLE_SIZE, is_disk_compressible};
}

pub(crate) mod ecstore_cluster {
    pub(crate) use rustfs_ecstore::api::cluster::{
        ClusterControlPlane, ClusterControlPlaneSnapshot, ClusterDriveMembership, ClusterEndpointType, ClusterLocalNodeStorage,
        ClusterLocalNodeStorageSnapshot, ClusterMembershipSnapshot, ClusterNodeMembership, ClusterPeerHealth,
        ClusterPeerHealthSnapshot, ClusterPoolState, ClusterPoolStateSnapshot, ClusterRpcBoundarySnapshot,
        ClusterRpcChannelSnapshot, ClusterRpcPlane, ClusterRpcTransport, topology_snapshot_from_endpoint_pools_with_capabilities,
    };
}

pub(crate) mod ecstore_config {
    pub(crate) use rustfs_ecstore::api::config::{
        com, init, init_global_config_sys, set_global_storage_class, storageclass, try_migrate_server_config,
    };
}

pub(crate) mod ecstore_data_usage {
    pub(crate) use rustfs_ecstore::api::data_usage::{
        apply_bucket_usage_memory_overlay, init_compression_total_memory_from_backend, load_data_usage_from_backend,
        record_bucket_delete_marker_memory, record_bucket_object_delete_memory, record_bucket_object_version_write_memory,
        record_bucket_object_write_memory, refresh_versioned_bucket_usage_from_object_layer, remove_bucket_usage_from_backend,
        replace_bucket_usage_memory_from_info, store_compression_total_in_backend,
    };
}

#[allow(unused_imports)]
pub(crate) mod ecstore_disk {
    pub(crate) use rustfs_ecstore::api::disk::{
        BatchReadVersionReq, BatchReadVersionResp, CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskStore,
        FileInfoVersions, FileReader, FileWriter, RUSTFS_META_BUCKET, ReadMultipleReq, ReadMultipleResp, ReadOptions,
        RenameDataResp, UpdateMetadataOpts, VolumeInfo, WalkDirOptions, get_object_disk_read_timeout,
        validate_batch_read_version_item_count,
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
        set_global_endpoints, set_global_region, set_global_rustfs_port, set_object_store_resolver, shutdown_background_services,
        update_erasure_type,
    };
}

pub(crate) mod ecstore_runtime {
    pub(crate) use rustfs_ecstore::api::runtime::{
        boot_time, bucket_monitor, deployment_id, endpoint_pools, global_lock_client, global_lock_clients,
        global_tier_config_mgr, region, rustfs_port, setup_is_dist_erasure,
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
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::tier::warm_backend;
    pub(crate) use rustfs_ecstore::api::tier::{tier, tier_admin, tier_config, tier_handlers};
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
type EcstoreDynReplicationPool = ecstore_bucket::replication::DynReplicationPool;
type EcstoreReplicationStats = ecstore_bucket::replication::ReplicationStats;
pub(crate) type DynReplicationPool = StorageReplicationPoolHandle;
pub(crate) type DynReader = ecstore_rio::DynReader;
pub(crate) type ECStore = ecstore_storage::ECStore;
pub(crate) type Endpoint = ecstore_disk::endpoint::Endpoint;
#[cfg(test)]
pub(crate) type Endpoints = ecstore_layout::Endpoints;
pub(crate) type EndpointServerPools = ecstore_layout::EndpointServerPools;
pub(crate) type EventArgs = ecstore_event::EventArgs;
pub(crate) type ExpiryState = ecstore_bucket::lifecycle::bucket_lifecycle_ops::ExpiryState;
pub(crate) type FileInfoVersions = ecstore_disk::FileInfoVersions;
pub(crate) type FileReader = ecstore_disk::FileReader;
pub(crate) type FileWriter = ecstore_disk::FileWriter;
pub(crate) type FS = super::ecfs::FS;
pub(crate) type HashReader = ecstore_rio::HashReader;
pub(crate) type LocalPeerS3Client = ecstore_rpc::LocalPeerS3Client;
pub(crate) type MetricType = ecstore_metrics::MetricType;
pub(crate) type ObjectPartInfo = rustfs_filemeta::ObjectPartInfo;
pub(crate) type ObjectLockBlockReason = ecstore_bucket::object_lock::objectlock_sys::ObjectLockBlockReason;
pub(crate) type ObjectStoreResolver = dyn Fn() -> Option<Arc<ECStore>> + Send + Sync + 'static;
pub(crate) type PolicySys = ecstore_bucket::policy_sys::PolicySys;
pub(crate) type PoolEndpoints = ecstore_layout::PoolEndpoints;
pub(crate) type WorkloadAdmissionSnapshotProviderRef = rustfs_ecstore::WorkloadAdmissionSnapshotProviderRef;
pub(crate) type QuotaError = ecstore_bucket::quota::QuotaError;
pub(crate) type RawFileInfo = rustfs_filemeta::RawFileInfo;
pub(crate) type BatchReadVersionReq = ecstore_disk::BatchReadVersionReq;
pub(crate) type BatchReadVersionResp = ecstore_disk::BatchReadVersionResp;
pub(crate) type ReadMultipleReq = ecstore_disk::ReadMultipleReq;
pub(crate) type ReadMultipleResp = ecstore_disk::ReadMultipleResp;
pub(crate) type ReadOptions = ecstore_disk::ReadOptions;
pub(crate) type RenameDataResp = ecstore_disk::RenameDataResp;
pub(crate) type ReplicationStats = StorageReplicationStatsHandle;
pub(crate) type SetupType = ecstore_layout::SetupType;
pub(crate) type StorageError = ecstore_error::StorageError;
pub(crate) type TierConfigMgr = ecstore_tier::TierConfigMgr;
pub(crate) use ecstore_disk::validate_batch_read_version_item_count;
pub(crate) type TransitionState = ecstore_bucket::lifecycle::bucket_lifecycle_ops::TransitionState;
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

#[derive(Debug, Clone)]
pub(crate) struct StorageReplicationPoolHandle {
    inner: Arc<EcstoreDynReplicationPool>,
}

impl StorageReplicationPoolHandle {
    fn new(inner: Arc<EcstoreDynReplicationPool>) -> Arc<Self> {
        Arc::new(Self { inner })
    }

    pub(crate) fn active_workers(&self) -> i32 {
        self.inner.active_workers()
    }

    pub(crate) fn active_mrf_workers(&self) -> i32 {
        self.inner.active_mrf_workers()
    }

    pub(crate) fn active_lrg_workers(&self) -> i32 {
        self.inner.active_lrg_workers()
    }

    pub(crate) async fn get_bucket_resync_status(
        &self,
        bucket: &str,
    ) -> Result<ecstore_bucket::replication::BucketReplicationResyncStatus> {
        self.inner.get_bucket_resync_status(bucket).await
    }

    pub(crate) async fn cancel_bucket_resync(&self, opts: ecstore_bucket::replication::ResyncOpts) -> Result<()> {
        self.inner.clone().cancel_bucket_resync(opts).await
    }

    pub(crate) async fn start_bucket_resync(&self, opts: ecstore_bucket::replication::ResyncOpts) -> Result<()> {
        self.inner.clone().start_bucket_resync(opts).await
    }

    pub(crate) async fn init_resync(self: Arc<Self>, ctx: CancellationToken, buckets: Vec<String>) -> Result<()> {
        self.inner.clone().init_resync(ctx, buckets).await
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StorageReplicationStatsHandle {
    inner: Arc<EcstoreReplicationStats>,
}

impl StorageReplicationStatsHandle {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(EcstoreReplicationStats::new()),
        }
    }

    fn from_ecstore(inner: Arc<EcstoreReplicationStats>) -> Arc<Self> {
        Arc::new(Self { inner })
    }

    pub(crate) async fn get_latest_replication_stats(&self, bucket: &str) -> ecstore_bucket::replication::BucketStats {
        self.inner.get_latest_replication_stats(bucket).await
    }

    pub(crate) async fn site_metrics_snapshot(&self) -> ReplicationSiteMetricsSnapshot {
        let metrics = self.inner.get_sr_metrics_for_node().await;
        ReplicationSiteMetricsSnapshot {
            uptime: metrics.uptime,
            queued_curr_count: metrics.queued.curr.count,
            queued_curr_bytes: metrics.queued.curr.bytes,
            queued_avg_count: metrics.queued.avg.count,
            queued_avg_bytes: metrics.queued.avg.bytes,
            queued_max_count: metrics.queued.max.count,
            queued_max_bytes: metrics.queued.max.bytes,
            active_workers_curr: metrics.active_workers.curr,
            active_workers_avg: metrics.active_workers.avg,
            active_workers_max: metrics.active_workers.max,
            proxy_get_total: metrics.proxied.get_total,
            proxy_head_total: metrics.proxied.head_total,
            proxy_get_failed: metrics.proxied.get_failed,
            proxy_head_failed: metrics.proxied.head_failed,
            proxy_put_tag_total: metrics.proxied.put_tag_total,
            proxy_put_tag_failed: metrics.proxied.put_tag_failed,
            replica_size: metrics.replica_size,
            replica_count: metrics.replica_count,
        }
    }

    fn queue_current_count(&self) -> Option<i64> {
        self.inner
            .q_cache
            .try_lock()
            .ok()
            .map(|cache| cache.sr_queue_stats.curr.get_current_count())
    }

    async fn record_proxy(&self, bucket: &str, api: &str, is_err: bool) {
        self.inner.inc_proxy(bucket, api, is_err).await;
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ReplicationSiteMetricsSnapshot {
    pub(crate) uptime: i64,
    pub(crate) queued_curr_count: i64,
    pub(crate) queued_curr_bytes: i64,
    pub(crate) queued_avg_count: i64,
    pub(crate) queued_avg_bytes: i64,
    pub(crate) queued_max_count: i64,
    pub(crate) queued_max_bytes: i64,
    pub(crate) active_workers_curr: i32,
    pub(crate) active_workers_avg: f64,
    pub(crate) active_workers_max: i32,
    pub(crate) proxy_get_total: i64,
    pub(crate) proxy_head_total: i64,
    pub(crate) proxy_get_failed: i64,
    pub(crate) proxy_head_failed: i64,
    pub(crate) proxy_put_tag_total: i64,
    pub(crate) proxy_put_tag_failed: i64,
    pub(crate) replica_size: i64,
    pub(crate) replica_count: i64,
}

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
    ecstore_bucket::replication::get_global_replication_pool().map(StorageReplicationPoolHandle::new)
}

pub(crate) fn get_global_replication_stats() -> Option<Arc<ReplicationStats>> {
    ecstore_bucket::replication::get_global_replication_stats().map(StorageReplicationStatsHandle::from_ecstore)
}

pub(crate) fn get_global_boot_time() -> Option<std::time::SystemTime> {
    ecstore_runtime::boot_time()
}

pub(crate) fn get_global_expiry_state() -> Arc<tokio::sync::RwLock<ExpiryState>> {
    ecstore_bucket::lifecycle::bucket_lifecycle_ops::get_global_expiry_state()
}

pub(crate) fn get_global_transition_state() -> Arc<TransitionState> {
    ecstore_bucket::lifecycle::bucket_lifecycle_ops::get_global_transition_state()
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
    get_global_replication_stats().and_then(|stats| stats.queue_current_count())
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
    async fn batch_read_version(&self, req: BatchReadVersionReq) -> DiskResult<Vec<BatchReadVersionResp>>;
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

    async fn batch_read_version(&self, req: BatchReadVersionReq) -> DiskResult<Vec<BatchReadVersionResp>> {
        ecstore_disk::DiskAPI::batch_read_version(self, req).await
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
    async fn make_bucket(&self, bucket: &str, opts: &contract::bucket::MakeBucketOptions) -> DiskResult<()>;
    async fn list_bucket(&self, opts: &contract::bucket::BucketOptions) -> DiskResult<Vec<contract::bucket::BucketInfo>>;
    async fn delete_bucket(&self, bucket: &str, opts: &contract::bucket::DeleteBucketOptions) -> DiskResult<()>;
    async fn get_bucket_info(
        &self,
        bucket: &str,
        opts: &contract::bucket::BucketOptions,
    ) -> DiskResult<contract::bucket::BucketInfo>;
}

impl StoragePeerS3ClientExt for LocalPeerS3Client {
    async fn heal_bucket(
        &self,
        bucket: &str,
        opts: &rustfs_common::heal_channel::HealOpts,
    ) -> DiskResult<rustfs_madmin::heal_commands::HealResultItem> {
        ecstore_rpc::PeerS3Client::heal_bucket(self, bucket, opts).await
    }

    async fn make_bucket(&self, bucket: &str, opts: &contract::bucket::MakeBucketOptions) -> DiskResult<()> {
        ecstore_rpc::PeerS3Client::make_bucket(self, bucket, opts).await
    }

    async fn list_bucket(&self, opts: &contract::bucket::BucketOptions) -> DiskResult<Vec<contract::bucket::BucketInfo>> {
        ecstore_rpc::PeerS3Client::list_bucket(self, opts).await
    }

    async fn delete_bucket(&self, bucket: &str, opts: &contract::bucket::DeleteBucketOptions) -> DiskResult<()> {
        ecstore_rpc::PeerS3Client::delete_bucket(self, bucket, opts).await
    }

    async fn get_bucket_info(
        &self,
        bucket: &str,
        opts: &contract::bucket::BucketOptions,
    ) -> DiskResult<contract::bucket::BucketInfo> {
        ecstore_rpc::PeerS3Client::get_bucket_info(self, bucket, opts).await
    }
}

pub(crate) async fn load_bucket_metadata(api: Arc<ECStore>, bucket: &str) -> Result<BucketMetadata> {
    ecstore_bucket::metadata::load_bucket_metadata(api, bucket).await
}

#[cfg(test)]
pub(crate) fn bucket_metadata_sys_initialized() -> bool {
    ecstore_bucket::metadata_sys::get_global_bucket_metadata_sys().is_some()
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
    if let Some(stats) = get_global_replication_stats() {
        stats.record_proxy(bucket, api, is_err).await;
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
    ecstore_runtime::global_lock_client()
}

pub(crate) fn get_global_lock_clients()
-> Option<&'static std::collections::HashMap<String, Arc<dyn rustfs_lock::client::LockClient>>> {
    ecstore_runtime::global_lock_clients()
}

pub(crate) fn get_global_bucket_monitor() -> Option<Arc<BucketBandwidthMonitor>> {
    ecstore_runtime::bucket_monitor()
}

pub(crate) fn get_global_endpoints_opt() -> Option<EndpointServerPools> {
    ecstore_runtime::endpoint_pools()
}

pub(crate) fn get_global_deployment_id() -> Option<String> {
    ecstore_runtime::deployment_id()
}

pub(crate) fn get_global_region() -> Option<s3s::region::Region> {
    ecstore_runtime::region()
}

pub(crate) fn global_rustfs_port() -> u16 {
    ecstore_runtime::rustfs_port()
}

pub(crate) fn get_global_tier_config_mgr() -> Arc<tokio::sync::RwLock<TierConfigMgr>> {
    ecstore_runtime::global_tier_config_mgr()
}

pub(crate) fn set_object_store_resolver(resolver: Arc<ObjectStoreResolver>) -> bool {
    ecstore_global::set_object_store_resolver(resolver)
}

pub(crate) fn set_workload_admission_snapshot_provider(
    provider: WorkloadAdmissionSnapshotProviderRef,
) -> std::result::Result<(), WorkloadAdmissionSnapshotProviderRef> {
    rustfs_ecstore::set_workload_admission_snapshot_provider(provider)
}

pub(crate) fn get_global_notification_sys() -> Option<&'static NotificationSys> {
    ecstore_notification::get_global_notification_sys()
}

pub(crate) async fn is_dist_erasure() -> bool {
    ecstore_runtime::setup_is_dist_erasure().await
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

pub(crate) fn get_object_disk_read_timeout() -> std::time::Duration {
    ecstore_disk::get_object_disk_read_timeout()
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
    capabilities: contract::topology::TopologyCapabilities,
    disk_capabilities: contract::topology::DiskCapabilities,
) -> contract::topology::TopologySnapshot {
    ecstore_cluster::topology_snapshot_from_endpoint_pools_with_capabilities(endpoint_pools, capabilities, disk_capabilities)
}

pub(crate) async fn reload_transition_tier_config(api: Arc<ECStore>) -> std::io::Result<()> {
    ecstore_runtime::global_tier_config_mgr().write().await.reload(api).await
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

pub(crate) type GetObjectReader = <ECStore as contract::object::ObjectIO>::GetObjectReader;
pub(crate) type ObjectInfo = <ECStore as contract::object::ObjectOperations>::ObjectInfo;
pub(crate) type ObjectOptions = <ECStore as contract::object::ObjectOperations>::ObjectOptions;
pub(crate) type PutObjReader = <ECStore as contract::object::ObjectIO>::PutObjectReader;

pub(crate) async fn store_compression_total_in_backend() {
    ecstore_data_usage::store_compression_total_in_backend().await;
}

pub(crate) async fn init_compression_total_memory_from_backend(store: Arc<ECStore>) {
    ecstore_data_usage::init_compression_total_memory_from_backend(store).await
}
