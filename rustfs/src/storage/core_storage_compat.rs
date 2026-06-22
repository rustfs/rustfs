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

pub(crate) use crate::storage::storage_compat::{
    BUCKET_ACCELERATE_CONFIG, BUCKET_LOGGING_CONFIG, BUCKET_REQUEST_PAYMENT_CONFIG, BUCKET_VERSIONING_CONFIG,
    BUCKET_WEBSITE_CONFIG, BucketVersioningSys, ECStore, Error, GetObjectReader, OBJECT_LOCK_CONFIG, ObjectInfo, ObjectOptions,
    PolicySys, PutObjReader, Result, StorageError, StorageReplicationConfigExt, StorageVersioningConfigExt, WriteEncryption,
    add_object_lock_years, check_retention_for_modification, decode_tags, decode_tags_to_map, delete_bucket_metadata_config,
    encode_tags, get_bucket_accelerate_config, get_bucket_cors_config, get_bucket_logging_config, get_bucket_metadata,
    get_bucket_object_lock_config, get_bucket_policy_raw, get_bucket_replication_config, get_bucket_request_payment_config,
    get_bucket_sse_config, get_bucket_website_config, get_global_region, get_public_access_block_config, is_err_bucket_not_found,
    is_err_object_not_found, is_err_version_not_found, record_replication_proxy, resolve_object_store_handle, serialize,
    update_bucket_metadata_config,
};

#[cfg(test)]
pub(crate) use crate::storage::storage_compat::{
    BucketMetadata, DEFAULT_READ_BUFFER_SIZE, bucket_metadata_sys_initialized, get_global_bucket_metadata_sys,
    set_bucket_metadata,
};
