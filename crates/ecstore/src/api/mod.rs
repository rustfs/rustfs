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
    pub mod bandwidth {
        pub mod monitor {
            pub use crate::bucket::bandwidth::monitor::{BandwidthDetails, Monitor};
        }
    }

    pub mod bucket_target_sys {
        pub use crate::bucket::bucket_target_sys::{
            AdvancedPutOptions, BucketTargetError, BucketTargetSys, PutObjectOptions, RemoveObjectOptions, S3ClientError,
            TargetClient,
        };
    }

    pub mod lifecycle {
        pub mod bucket_lifecycle_audit {
            pub use crate::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
        }

        pub mod bucket_lifecycle_ops {
            pub use crate::bucket::lifecycle::bucket_lifecycle_ops::{
                ExpiryState, LifecycleOps, RestoreRequestOps, TransitionState, TransitionedObject, apply_expiry_rule,
                apply_transition_rule, enqueue_expiry_for_existing_objects, enqueue_transition_for_existing_objects,
                enqueue_transition_immediate, get_global_expiry_state, get_global_transition_state, init_background_expiry,
                post_restore_opts, run_stale_multipart_upload_cleanup_once, validate_transition_tier,
            };
        }

        pub mod evaluator {
            pub use crate::bucket::lifecycle::evaluator::Evaluator;
        }

        #[allow(clippy::module_inception)]
        pub mod lifecycle {
            pub use crate::bucket::lifecycle::lifecycle::{
                Event, ExpirationOptions, IlmAction, Lifecycle, LifecycleCalculate, ObjectOpts, RuleValidate,
                TRANSITION_COMPLETE, TRANSITION_PENDING, TransitionOptions, expected_expiry_time, object_opts_from_object_info,
            };
        }

        pub mod rule {
            pub use crate::bucket::lifecycle::rule::{Filter, NoncurrentVersionTransitionOps, TransitionOps};
        }

        pub mod tier_delete_journal {
            pub use crate::bucket::lifecycle::tier_delete_journal::persist_tier_delete_journal_entry;
        }

        pub mod tier_last_day_stats {
            pub use crate::bucket::lifecycle::tier_last_day_stats::{DailyAllTierStats, LastDayTierStats};
        }

        pub mod tier_sweeper {
            pub use crate::bucket::lifecycle::tier_sweeper::{
                Jentry, transitioned_delete_journal_entry, transitioned_force_delete_journal_entry,
            };
        }
    }

    pub mod metadata {
        pub use crate::bucket::metadata::{
            BUCKET_ACCELERATE_CONFIG, BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_LOGGING_CONFIG,
            BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG, BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, BUCKET_QUOTA_CONFIG_FILE,
            BUCKET_REPLICATION_CONFIG, BUCKET_REQUEST_PAYMENT_CONFIG, BUCKET_SSECONFIG, BUCKET_TABLE_CATALOG_META_PREFIX,
            BUCKET_TABLE_CATALOG_TABLE_BUCKETS_PREFIX, BUCKET_TABLE_CONFIG, BUCKET_TABLE_RESERVED_PREFIX, BUCKET_TAGGING_CONFIG,
            BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG, BUCKET_WEBSITE_CONFIG, BucketMetadata, OBJECT_LOCK_CONFIG,
            load_bucket_metadata, table_catalog_path_hash,
        };
    }

    pub mod metadata_sys {
        pub use crate::bucket::metadata_sys::{
            BucketMetadataSys, delete, get, get_accelerate_config, get_bucket_policy, get_bucket_policy_raw,
            get_bucket_targets_config, get_config_from_disk, get_cors_config, get_global_bucket_metadata_sys,
            get_lifecycle_config, get_logging_config, get_notification_config, get_object_lock_config,
            get_public_access_block_config, get_quota_config, get_replication_config, get_request_payment_config, get_sse_config,
            get_tagging_config, get_versioning_config, get_website_config, init_bucket_metadata_sys, list_bucket_targets,
            set_bucket_metadata, update,
        };
    }

    pub mod migration {
        pub use crate::bucket::migration::{try_migrate_bucket_metadata, try_migrate_iam_config};
    }

    pub mod object_lock {
        pub use crate::bucket::object_lock::{ObjectLockApi, ObjectLockStatusExt};

        pub mod objectlock {
            pub use crate::bucket::object_lock::objectlock::{get_object_legalhold_meta, get_object_retention_meta};
        }

        pub mod objectlock_sys {
            pub use crate::bucket::object_lock::objectlock_sys::{
                BucketObjectLockSys, ObjectLockBlockReason, add_years, check_object_lock_for_deletion,
                check_retention_for_modification, is_retention_active,
            };
        }
    }

    pub mod policy_sys {
        pub use crate::bucket::policy_sys::PolicySys;
    }

    pub mod quota {
        pub use crate::bucket::quota::{BucketQuota, QuotaError, QuotaOperation};

        pub mod checker {
            pub use crate::bucket::quota::checker::QuotaChecker;
        }
    }

    pub mod replication {
        pub use crate::bucket::replication::{
            BucketReplicationResyncStatus, BucketStats, DeletedObjectReplicationInfo, DynReplicationPool, MustReplicateOptions,
            ObjectOpts, REPLICATE_INCOMING_DELETE, ReplicateDecision, ReplicateObjectInfo, ReplicationConfig,
            ReplicationConfigurationExt, ReplicationDeleteScheduleInput, ReplicationDeleteStateSource,
            ReplicationHealQueueResult, ReplicationObjectBridge, ReplicationObjectIO, ReplicationOperation, ReplicationPoolTrait,
            ReplicationPriority, ReplicationQueueAdmission, ReplicationScannerBridge, ReplicationState, ReplicationStats,
            ReplicationStatusType, ReplicationStorage, ReplicationTargetValidationError, ReplicationType, ResyncOpts,
            ResyncStatusType, TargetReplicationResyncStatus, VersionPurgeStatusType, delete_replication_state_from_config,
            delete_replication_version_id, get_global_replication_pool, get_global_replication_stats,
            init_background_replication, replication_state_to_filemeta, replication_status_to_filemeta, replication_statuses_map,
            replication_target_arns, should_remove_replication_target, should_schedule_delete_replication,
            should_use_existing_delete_replication_info, should_use_existing_delete_replication_source,
            validate_replication_config_target_arns, version_purge_status_to_filemeta,
        };
    }

    pub mod tagging {
        pub use crate::bucket::tagging::{decode_tags, decode_tags_to_map, encode_tags};
    }

    pub mod target {
        pub use crate::bucket::target::{ARN, BucketTarget, BucketTargetType, BucketTargets, Credentials, LatencyStat};
    }

    pub mod utils {
        pub use crate::bucket::utils::{
            check_bucket_and_object_names, check_list_objs_args, check_object_name_for_length_and_slash,
            check_valid_bucket_name_strict, deserialize, has_bad_path_component, is_meta_bucketname, is_valid_object_prefix,
            serialize,
        };
    }

    pub mod versioning {
        pub use crate::bucket::versioning::VersioningApi;
    }

    pub mod versioning_sys {
        pub use crate::bucket::versioning_sys::BucketVersioningSys;
    }
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
    pub use crate::io_support::compress::{MIN_DISK_COMPRESSIBLE_SIZE, is_disk_compressible, is_disk_compression_enabled};
}

pub mod config {
    pub use crate::config::{
        RUSTFS_CONFIG_PREFIX, com, init, init_global_config_sys, set_global_storage_class, storageclass,
        try_migrate_server_config,
    };
}

pub mod data_usage {
    pub use crate::data_usage::{
        DATA_USAGE_CACHE_NAME, apply_bucket_usage_memory_overlay, init_compression_total_memory_from_backend,
        load_compression_total_from_memory, load_data_usage_from_backend, record_bucket_delete_marker_memory,
        record_bucket_object_delete_memory, record_bucket_object_version_write_memory, record_bucket_object_write_memory,
        record_compression_total_memory, refresh_versioned_bucket_usage_from_object_layer, remove_bucket_usage_from_backend,
        replace_bucket_usage_memory_from_info, store_compression_total_in_backend,
    };
}

pub mod disk {
    pub use crate::disk::disk_store::get_object_disk_read_timeout;
    pub use crate::disk::endpoint::Endpoint;
    pub use crate::disk::error::DiskError;
    pub use crate::disk::error_reduce::is_all_buckets_not_found;
    pub use crate::disk::local::ScanGuard;
    pub use crate::disk::{
        BATCH_READ_VERSION_MAX_ITEMS, BUCKET_META_PREFIX, BatchReadVersionItem, BatchReadVersionReq, BatchReadVersionResp,
        CheckPartsResp, DeleteOptions, Disk, DiskAPI, DiskInfo, DiskInfoOptions, DiskLocation, DiskOption, DiskStore,
        FileInfoVersions, FileReader, FileWriter, HEALING_MARKER_PATH, RUSTFS_META_BUCKET, ReadMultipleReq, ReadMultipleResp,
        ReadOptions, RenameDataResp, STORAGE_FORMAT_FILE, UpdateMetadataOpts, VolumeInfo, WalkDirOptions, new_disk,
        validate_batch_read_version_item_count,
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
        set_global_endpoints, set_global_region, set_global_rustfs_port, set_object_store_resolver, shutdown_background_services,
        update_erasure_type,
    };
}

pub mod runtime {
    pub use crate::runtime::sources::{
        boot_time, bucket_monitor, deployment_id, endpoint_pools, expiry_state_handle, first_cluster_node_is_local,
        global_lock_client, global_lock_clients, global_tier_config_mgr, local_disk_map_read, object_store_handle, region,
        rustfs_port, setup_is_dist_erasure, setup_is_erasure, setup_is_erasure_sd, transition_state_handle,
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
        BLOCK_SIZE_V2, ERASURE_ALGORITHM, GetObjectBodyCacheHook, GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader,
        RangedDecompressReader, StreamConsumer, register_get_object_body_cache_hook,
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
