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

pub(crate) use rustfs_ecstore::api::admin::get_server_info;
pub(crate) mod bucket_target_sys {
    pub(crate) use rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetSys;
}
pub(crate) mod lifecycle {
    pub(crate) mod bucket_lifecycle_audit {
        pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
    }

    pub(crate) mod bucket_lifecycle_ops {
        #[cfg(test)]
        pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry;
        pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::{
            GLOBAL_ExpiryState, RestoreRequestOps, enqueue_expiry_for_existing_objects, enqueue_transition_for_existing_objects,
            enqueue_transition_immediate, post_restore_opts, validate_transition_tier,
        };
    }

    pub(crate) mod lifecycle_contract {
        #[cfg(test)]
        pub(crate) use rustfs_ecstore::api::bucket::lifecycle::lifecycle::IlmAction;
        pub(crate) use rustfs_ecstore::api::bucket::lifecycle::lifecycle::{
            Event, Lifecycle, ObjectOpts, TRANSITION_COMPLETE, TransitionOptions, expected_expiry_time,
        };
    }
    pub(crate) use lifecycle_contract as lifecycle;

    pub(crate) mod tier_delete_journal {
        pub(crate) use rustfs_ecstore::api::bucket::lifecycle::tier_delete_journal::persist_tier_delete_journal_entry;
    }

    pub(crate) mod tier_sweeper {
        pub(crate) use rustfs_ecstore::api::bucket::lifecycle::tier_sweeper::{
            transitioned_delete_journal_entry, transitioned_force_delete_journal_entry,
        };
    }
}
pub(crate) mod metadata {
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::bucket::metadata::OBJECT_LOCK_CONFIG;
    pub(crate) use rustfs_ecstore::api::bucket::metadata::{
        BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG,
        BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, BUCKET_REPLICATION_CONFIG, BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG,
        BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG,
    };
}
pub(crate) mod metadata_sys {
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::init_bucket_metadata_sys;
    pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::{
        BucketMetadataSys, delete, get_bucket_policy, get_bucket_policy_raw, get_bucket_targets_config, get_cors_config,
        get_global_bucket_metadata_sys, get_lifecycle_config, get_notification_config, get_object_lock_config,
        get_public_access_block_config, get_replication_config, get_sse_config, get_tagging_config, update,
    };
}
pub(crate) mod object_api_utils {
    pub(crate) use rustfs_ecstore::api::client::object_api_utils::to_s3s_etag;
}
pub(crate) mod object_lock {
    pub(crate) mod objectlock {
        pub(crate) use rustfs_ecstore::api::bucket::object_lock::objectlock::{
            get_object_legalhold_meta, get_object_retention_meta,
        };
    }

    pub(crate) mod objectlock_sys {
        pub(crate) use rustfs_ecstore::api::bucket::object_lock::objectlock_sys::{
            BucketObjectLockSys, check_object_lock_for_deletion, is_retention_active,
        };
    }

    pub(crate) use rustfs_ecstore::api::bucket::object_lock::ObjectLockApi;
}
pub(crate) mod policy_sys {
    pub(crate) use rustfs_ecstore::api::bucket::policy_sys::PolicySys;
}
pub(crate) mod quota {
    pub(crate) mod checker {
        pub(crate) use rustfs_ecstore::api::bucket::quota::checker::QuotaChecker;
    }

    pub(crate) use rustfs_ecstore::api::bucket::quota::QuotaOperation;
}
pub(crate) mod replication {
    pub(crate) use rustfs_ecstore::api::bucket::replication::{
        DeletedObjectReplicationInfo, ObjectOpts, ReplicationConfigurationExt, check_replicate_delete,
        get_must_replicate_options, must_replicate, schedule_replication, schedule_replication_delete,
    };
}
pub(crate) mod tagging {
    pub(crate) use rustfs_ecstore::api::bucket::tagging::decode_tags;
}
pub(crate) mod target {
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::bucket::target::BucketTarget;
    pub(crate) use rustfs_ecstore::api::bucket::target::{BucketTargetType, BucketTargets};
}
pub(crate) mod utils {
    pub(crate) use rustfs_ecstore::api::bucket::utils::serialize;
}
pub(crate) mod versioning {
    pub(crate) use rustfs_ecstore::api::bucket::versioning::VersioningApi;
}
pub(crate) mod versioning_sys {
    pub(crate) use rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys;
}
pub(crate) use rustfs_ecstore::api::capacity::{
    PoolDecommissionInfo, PoolStatus, get_total_usable_capacity, get_total_usable_capacity_free,
};
#[cfg(test)]
pub(crate) mod transition_api {
    pub(crate) use rustfs_ecstore::api::client::transition_api::{ReadCloser, ReaderImpl};
}
pub(crate) use rustfs_ecstore::api::compression::{MIN_DISK_COMPRESSIBLE_SIZE, is_disk_compressible};
pub(crate) mod storageclass {
    pub(crate) use rustfs_ecstore::api::config::storageclass::STANDARD;
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::config::storageclass::STANDARD_IA;
}
pub(crate) use rustfs_ecstore::api::data_usage::{
    apply_bucket_usage_memory_overlay, load_data_usage_from_backend, record_bucket_object_delete_memory,
    record_bucket_object_write_memory,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::disk::endpoint::Endpoint;
pub(crate) use rustfs_ecstore::api::disk::error::DiskError;
pub(crate) use rustfs_ecstore::api::disk::error_reduce::is_all_buckets_not_found;
pub(crate) use rustfs_ecstore::api::error::{
    Error, StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::global::GLOBAL_TierConfigMgr;
pub(crate) use rustfs_ecstore::api::global::{
    get_global_endpoints_opt, get_global_region, get_global_tier_config_mgr, new_object_layer_fn, set_object_store_resolver,
};
pub(crate) use rustfs_ecstore::api::layout::EndpointServerPools;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::layout::{Endpoints, PoolEndpoints};
pub(crate) use rustfs_ecstore::api::notification::get_global_notification_sys;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::rio::{DecryptReader, EncryptReader, HardLimitReader, boxed_reader};
pub(crate) use rustfs_ecstore::api::rio::{
    DynReader, HashReader, WriteEncryption, WritePlan, compression_metadata_value, wrap_reader,
};
pub(crate) use rustfs_ecstore::api::set_disk::{get_lock_acquire_timeout, is_valid_storage_class};
pub(crate) use rustfs_ecstore::api::storage::ECStore;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::storage::init_local_disks;
pub(crate) use rustfs_ecstore::api::tier::tier::TierConfigMgr;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::tier::tier_config::{TierConfig, TierType};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::tier::warm_backend::{WarmBackend, WarmBackendGetOpts};
