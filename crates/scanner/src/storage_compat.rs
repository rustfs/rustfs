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

use http::HeaderMap;
pub(crate) mod ecstore {
    pub(crate) use rustfs_ecstore::{
        bucket, cache_value, config, data_usage, disk, error, global, pools, resolve_object_store_handle, set_disk, store,
        store_api, store_utils,
    };
}
pub(crate) use self::ecstore::{
    bucket::{
        bucket_target_sys::BucketTargetSys,
        lifecycle::{
            bucket_lifecycle_audit::LcEventSrc,
            bucket_lifecycle_ops::{GLOBAL_ExpiryState, apply_expiry_rule, apply_transition_rule},
            evaluator::Evaluator,
            lifecycle::{Event, Lifecycle, ObjectOpts, TRANSITION_COMPLETE},
        },
        metadata_sys::{get_lifecycle_config, get_object_lock_config, get_replication_config},
        replication::{
            ReplicationConfig, ReplicationConfigurationExt, ReplicationQueueAdmission, queue_replication_heal_internal,
        },
        versioning::VersioningApi,
        versioning_sys::BucketVersioningSys,
    },
    cache_value::metacache_set::{ListPathRawOptions, list_path_raw},
    config::{
        com::{read_config, save_config},
        storageclass,
    },
    data_usage::replace_bucket_usage_memory_from_info,
    disk::{BUCKET_META_PREFIX, Disk, DiskAPI, DiskInfoOptions, RUSTFS_META_BUCKET, STORAGE_FORMAT_FILE, error::DiskError},
    error::{Error as EcstoreError, Result as EcstoreResult, StorageError},
    global::{GLOBAL_TierConfigMgr, is_erasure, is_erasure_sd},
    pools::{path2_bucket_object, path2_bucket_object_with_base_path},
    set_disk::SetDisks,
    store::ECStore,
    store_api::{
        GetObjectReader as EcstoreGetObjectReader, ObjectInfo as EcstoreObjectInfo, ObjectOptions as EcstoreObjectOptions,
        PutObjReader as EcstorePutObjReader,
    },
    store_utils::is_reserved_or_invalid_bucket,
};
use rustfs_storage_api::{HTTPRangeSpec, ObjectIO, ObjectToDelete};
use std::sync::Arc;

#[cfg(test)]
pub(crate) use self::ecstore::{
    config::init as init_ecstore_config_for_scanner_tests,
    disk::{DiskOption, endpoint::Endpoint, new_disk},
};

pub type ScannerGetObjectReader = EcstoreGetObjectReader;
pub type ScannerObjectInfo = EcstoreObjectInfo;
pub type ScannerObjectOptions = EcstoreObjectOptions;
pub type ScannerObjectToDelete = ObjectToDelete;
pub type ScannerPutObjReader = EcstorePutObjReader;

pub(crate) fn resolve_scanner_object_store_handle() -> Option<Arc<ECStore>> {
    ecstore::resolve_object_store_handle()
}

pub trait ScannerObjectIO:
    ObjectIO<
        Error = EcstoreError,
        RangeSpec = HTTPRangeSpec,
        HeaderMap = HeaderMap,
        ObjectOptions = ScannerObjectOptions,
        ObjectInfo = ScannerObjectInfo,
        GetObjectReader = ScannerGetObjectReader,
        PutObjectReader = ScannerPutObjReader,
    >
{
}

impl<T> ScannerObjectIO for T where
    T: ObjectIO<
            Error = EcstoreError,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ScannerObjectOptions,
            ObjectInfo = ScannerObjectInfo,
            GetObjectReader = ScannerGetObjectReader,
            PutObjectReader = ScannerPutObjReader,
        >
{
}
