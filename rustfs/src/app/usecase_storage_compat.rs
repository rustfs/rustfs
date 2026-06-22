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

pub(crate) use super::storage_compat::{
    AppObjectLockConfigExt, AppReplicationConfigExt, AppVersioningConfigExt, DiskError, DynReader, ECStore, EndpointServerPools,
    Error, HashReader, MIN_DISK_COMPRESSIBLE_SIZE, PoolDecommissionInfo, PoolStatus, StorageError, WriteEncryption, WritePlan,
    apply_bucket_usage_memory_overlay, compression_metadata_value, get_global_notification_sys, get_lock_acquire_timeout,
    get_server_info, get_total_usable_capacity, get_total_usable_capacity_free, is_all_buckets_not_found, is_disk_compressible,
    is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found, is_valid_storage_class,
    load_data_usage_from_backend, predict_lifecycle_expiration, record_bucket_object_delete_memory,
    record_bucket_object_write_memory, validate_restore_request, wrap_reader,
};

#[cfg(test)]
pub(crate) use super::storage_compat::{
    AppWarmBackend, DecryptReader, EncryptReader, Endpoint, Endpoints, GLOBAL_TierConfigMgr, HardLimitReader, PoolEndpoints,
    TierConfig, TierType, WarmBackendGetOpts, boxed_reader, init_local_disks,
};

pub(crate) mod bucket_target_sys {
    pub(crate) use super::super::storage_compat::bucket_target_sys::BucketTargetSys;
}

pub(crate) mod lifecycle {
    pub(crate) mod bucket_lifecycle_audit {
        pub(crate) use super::super::super::storage_compat::lifecycle::bucket_lifecycle_audit::LcEventSrc;
    }

    pub(crate) mod bucket_lifecycle_ops {
        pub(crate) use super::super::super::storage_compat::lifecycle::bucket_lifecycle_ops::{
            GLOBAL_ExpiryState, enqueue_expiry_for_existing_objects, enqueue_transition_for_existing_objects,
            enqueue_transition_immediate, post_restore_opts, validate_lifecycle_config, validate_transition_tier,
        };

        #[cfg(test)]
        pub(crate) use super::super::super::storage_compat::lifecycle::bucket_lifecycle_ops::init_background_expiry;
    }

    pub(crate) mod lifecycle_contract {
        pub(crate) use super::super::super::storage_compat::lifecycle::lifecycle::{
            Event, ObjectOpts, TRANSITION_COMPLETE, TransitionOptions, expected_expiry_time,
        };

        #[cfg(test)]
        pub(crate) use super::super::super::storage_compat::lifecycle::lifecycle::IlmAction;
    }
    pub(crate) use lifecycle_contract as lifecycle;

    pub(crate) mod tier_delete_journal {
        pub(crate) use super::super::super::storage_compat::lifecycle::tier_delete_journal::persist_tier_delete_journal_entry;
    }

    pub(crate) mod tier_sweeper {
        pub(crate) use super::super::super::storage_compat::lifecycle::tier_sweeper::{
            transitioned_delete_journal_entry, transitioned_force_delete_journal_entry,
        };
    }
}

pub(crate) mod metadata {
    pub(crate) use super::super::storage_compat::metadata::{
        BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG,
        BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, BUCKET_REPLICATION_CONFIG, BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG,
        BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG,
    };

    #[cfg(test)]
    pub(crate) use super::super::storage_compat::metadata::OBJECT_LOCK_CONFIG;
}

pub(crate) mod metadata_sys {
    pub(crate) use super::super::storage_compat::metadata_sys::{
        BucketMetadataSys, delete, get_bucket_policy, get_bucket_policy_raw, get_bucket_targets_config, get_cors_config,
        get_lifecycle_config, get_notification_config, get_object_lock_config, get_public_access_block_config,
        get_replication_config, get_sse_config, get_tagging_config, update,
    };

    #[cfg(test)]
    pub(crate) use super::super::storage_compat::metadata_sys::init_bucket_metadata_sys;
}

pub(crate) mod object_api_utils {
    pub(crate) use super::super::storage_compat::object_api_utils::to_s3s_etag;
}

pub(crate) mod object_lock {
    pub(crate) mod objectlock {
        pub(crate) use super::super::super::storage_compat::object_lock::objectlock::{
            get_object_legalhold_meta, get_object_retention_meta,
        };
    }

    pub(crate) mod objectlock_sys {
        pub(crate) use super::super::super::storage_compat::object_lock::objectlock_sys::{
            BucketObjectLockSys, check_object_lock_for_deletion, is_retention_active,
        };
    }
}

pub(crate) mod policy_sys {
    pub(crate) use super::super::storage_compat::policy_sys::PolicySys;
}

pub(crate) mod quota {
    pub(crate) use super::super::storage_compat::quota::QuotaOperation;

    pub(crate) mod checker {
        pub(crate) use super::super::super::storage_compat::quota::checker::QuotaChecker;
    }
}

pub(crate) mod replication {
    pub(crate) use super::super::storage_compat::replication::{
        DeletedObjectReplicationInfo, ObjectOpts, check_replicate_delete, get_must_replicate_options, must_replicate,
        schedule_replication, schedule_replication_delete,
    };
}

pub(crate) mod storageclass {
    pub(crate) use super::super::storage_compat::storageclass::STANDARD;

    #[cfg(test)]
    pub(crate) use super::super::storage_compat::storageclass::STANDARD_IA;
}

pub(crate) mod tagging {
    pub(crate) use super::super::storage_compat::tagging::decode_tags;
}

pub(crate) mod target {
    pub(crate) use super::super::storage_compat::target::{BucketTargetType, BucketTargets};

    #[cfg(test)]
    pub(crate) use super::super::storage_compat::target::BucketTarget;
}

#[cfg(test)]
pub(crate) mod transition_api {
    pub(crate) use super::super::storage_compat::transition_api::{ReadCloser, ReaderImpl};
}

pub(crate) mod utils {
    pub(crate) use super::super::storage_compat::utils::serialize;
}

pub(crate) mod versioning_sys {
    pub(crate) use super::super::storage_compat::versioning_sys::BucketVersioningSys;
}
