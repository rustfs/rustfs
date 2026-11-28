// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::HeaderMap;
pub(crate) use rustfs_ecstore::api::bucket::metadata::BucketMetadata as SwiftBucketMetadata;
use rustfs_ecstore::api::bucket::metadata::SWIFT_TAGGING_MARKER;
use rustfs_ecstore::api::bucket::metadata::{BUCKET_TAGGING_CONFIG, is_unreadable_config_error};
use rustfs_ecstore::api::bucket::metadata_sys::{
    get as get_swift_bucket_metadata_from_backend, get_sse_config as get_swift_bucket_sse_config_from_backend, update_config_with,
};
use rustfs_ecstore::api::bucket::utils::serialize as serialize_bucket_config;
pub(crate) use rustfs_ecstore::api::error::Error as SwiftStorageError;
pub(crate) use rustfs_ecstore::api::error::Result as SwiftStorageResult;
use rustfs_ecstore::api::notification::get_global_notification_sys;
pub(crate) use rustfs_ecstore::api::object::{
    PreparedGetObjectReader as SwiftPreparedGetObjectReader, WriteCommitGuard as SwiftWriteCommitGuard,
    WriteCompletion as SwiftWriteCompletion,
};
pub(crate) use rustfs_ecstore::api::rio::{WriteEncryption as SwiftWriteEncryption, WritePlan as SwiftWritePlan};
pub(crate) use rustfs_ecstore::api::runtime::object_store_handle as resolve_swift_object_store_handle;
use rustfs_ecstore::api::storage::ECStore as SwiftStore;
use rustfs_gateway_types::cors_tagging::{PersistedTag as Tag, PersistedTagging as Tagging};
pub(crate) use rustfs_gateway_types::persistence::PersistedBucketEncryptionConfiguration as SwiftBucketEncryptionConfig;
use rustfs_storage_api as storage_contracts;

use super::{SwiftError, SwiftResult};

pub(crate) mod account {
    pub(crate) use super::storage_contracts::{BucketOperations, MakeBucketOptions};
}

pub(crate) mod container {
    pub(crate) use super::storage_contracts::{
        BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, ListOperations, MakeBucketOptions,
    };
}

pub(crate) mod large_object {
    pub(crate) use super::storage_contracts::HTTPRangeSpec;
}

pub(crate) mod object {
    pub(crate) use super::storage_contracts::{BucketOperations, BucketOptions, HTTPRangeSpec, ObjectIO, ObjectOperations};
}

pub(crate) mod public_api {
    pub use super::{SwiftGetObjectReader, SwiftObjectInfo, SwiftObjectOptions, SwiftPutObjReader};
    pub(crate) use super::{
        SwiftWriteCommitGuard, SwiftWriteCompletion, SwiftWritePlan, acquire_swift_self_copy_lock, get_swift_bucket_metadata,
        get_swift_bucket_sse_config, get_swift_bucket_usage, prepare_swift_get_object_reader,
        record_swift_bucket_object_delete_memory, record_swift_bucket_object_write_memory, resolve_swift_object_store_handle,
        update_swift_bucket_tagging,
    };
}

pub(crate) mod tagging {
    pub(crate) use rustfs_ecstore::api::bucket::metadata::{
        MAX_SWIFT_METADATA_COUNT, MAX_SWIFT_METADATA_VALUE_SIZE, SWIFT_ACCOUNT_META_TAG_PREFIX, SWIFT_CONTAINER_META_TAG_PREFIX,
    };
}

pub(crate) mod versioning {
    pub(crate) use super::storage_contracts::{ListOperations, ObjectOperations};
}

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_SWIFT_STORAGE: &str = "swift_storage";
const EVENT_SWIFT_BUCKET_TAGGING_UPDATE: &str = "swift_bucket_tagging_update";

pub type SwiftGetObjectReader = <SwiftStore as storage_contracts::ObjectIO>::GetObjectReader;
pub type SwiftObjectInfo = <SwiftStore as storage_contracts::ObjectOperations>::ObjectInfo;
pub type SwiftObjectOptions = <SwiftStore as storage_contracts::ObjectOperations>::ObjectOptions;
pub type SwiftPutObjReader = <SwiftStore as storage_contracts::ObjectIO>::PutObjectReader;

pub(crate) async fn get_swift_bucket_metadata(bucket: &str) -> SwiftStorageResult<Arc<SwiftBucketMetadata>> {
    get_swift_bucket_metadata_from_backend(bucket).await
}

/// Rewrite the bucket's tagging config through the persisting
/// bucket-metadata path.
///
/// `rewrite` sees the tag set currently persisted on disk (`None` when the
/// bucket has none) and returns the full replacement; an empty tag set
/// clears the config. The read-modify-write runs under the bucket metadata
/// system's write guard — serialized against every other config update —
/// and the result is written to the bucket metadata file before the cache
/// is refreshed, so a Swift metadata POST survives process restarts and
/// disk-truth reloads. Peers are then told to reload, matching what the S3
/// handlers do after a config write.
///
/// A rewrite may also refuse the update outright, for limits that can only be
/// judged against the state being merged into. That verdict is the client's
/// answer, so it is returned as-is rather than folded into a storage error.
///
/// Storage failures are logged in full and reported to the client as a
/// generic error: these now carry real disk and quorum detail, which does not
/// belong in a Swift response body. The one exception is an unreadable
/// persisted config, which is reported specifically because the operator has
/// to act on it.
pub(crate) async fn update_swift_bucket_tagging<F>(bucket: String, rewrite: F) -> SwiftResult<()>
where
    F: FnOnce(Option<&Tagging>) -> SwiftResult<Tagging> + Send,
{
    // Carries a rewrite's refusal back out past the storage error the config
    // write has to fail with to abort the transaction.
    let mut rejected = None;

    // `update_config_with` refuses to run this closure over an unparseable
    // tag set: merging onto it would silently drop every tag the bucket has —
    // including the container ACL and versioning tags — because the rewrite
    // closures treat "no parsed tags" as "no tags".
    let result = update_config_with(&bucket, BUCKET_TAGGING_CONFIG, |bm| {
        let mut tagging = match rewrite(bm.tagging_config.as_ref()) {
            Ok(tagging) => tagging,
            Err(err) => {
                rejected = Some(err);
                return Err(SwiftStorageError::other("swift: tagging rewrite rejected the update"));
            }
        };

        if tagging.tag_set.is_empty() {
            Ok(Vec::new())
        } else {
            if !tagging
                .tag_set
                .iter()
                .any(|tag| tag.key.as_deref() == Some(SWIFT_TAGGING_MARKER))
            {
                tagging.tag_set.push(Tag {
                    key: Some(SWIFT_TAGGING_MARKER.to_owned()),
                    value: Some("1".to_owned()),
                });
            }
            // The S3 XML serializer, not quick_xml: the metadata loader's
            // parse step must be able to round-trip what we persist.
            serialize_bucket_config(&tagging)
                .map_err(|e| SwiftStorageError::other(format!("failed to serialize bucket tagging: {e}")))
        }
    })
    .await;

    // Nothing was written, but that is the rewrite's own decision about the
    // request rather than a storage fault, so it is not logged as one.
    if let Some(err) = rejected {
        return Err(err);
    }

    if let Err(err) = result {
        let unreadable = is_unreadable_config_error(&err);
        tracing::error!(
            event = EVENT_SWIFT_BUCKET_TAGGING_UPDATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_SWIFT_STORAGE,
            bucket = %bucket,
            error = %err,
            reason = if unreadable { "unreadable_persisted_config" } else { "storage_failure" },
            result = "failed",
            "swift bucket tagging update failed"
        );
        // A Swift-only client has no way to repair this itself, so say what
        // happened and name the remedy instead of a bare storage error.
        return Err(if unreadable {
            SwiftError::Conflict(format!(
                "The persisted tagging configuration for container store '{bucket}' cannot be parsed, so metadata cannot be updated without discarding it. Reset it with the S3 DeleteBucketTagging API."
            ))
        } else {
            SwiftError::InternalServerError("Metadata update operation failed".to_string())
        });
    }

    if let Some(notification_sys) = get_global_notification_sys() {
        tokio::spawn(async move {
            if let Err(err) = notification_sys.load_bucket_metadata(&bucket).await {
                tracing::warn!(bucket = %bucket, error = %err, "failed to notify peers after swift bucket tagging update");
            }
        });
    }

    Ok(())
}

pub(crate) async fn get_swift_bucket_usage() -> SwiftStorageResult<Option<HashMap<String, (u64, u64)>>> {
    let Some(store) = resolve_swift_object_store_handle() else {
        return Ok(None);
    };
    let mut data_usage = rustfs_ecstore::api::data_usage::load_data_usage_from_backend_cached(store).await?;
    rustfs_ecstore::api::data_usage::apply_bucket_usage_memory_overlay(&mut data_usage).await;
    Ok(Some(
        data_usage
            .buckets_usage
            .into_iter()
            .map(|(bucket, usage)| (bucket, (usage.objects_count, usage.size)))
            .collect(),
    ))
}

pub(crate) async fn get_swift_bucket_sse_config(bucket: &str) -> SwiftStorageResult<Option<SwiftBucketEncryptionConfig>> {
    match get_swift_bucket_sse_config_from_backend(bucket).await {
        Ok((config, _updated_at)) => Ok(Some(config)),
        Err(SwiftStorageError::ConfigNotFound) => Ok(None),
        Err(err) => Err(err),
    }
}

pub(crate) async fn prepare_swift_get_object_reader(
    store: &SwiftStore,
    bucket: &str,
    key: &str,
    range: Option<storage_contracts::HTTPRangeSpec>,
    headers: HeaderMap,
    opts: &SwiftObjectOptions,
) -> SwiftStorageResult<SwiftPreparedGetObjectReader> {
    store.prepare_get_object_reader(bucket, key, range, headers, opts).await
}

pub(crate) async fn acquire_swift_self_copy_lock(
    store: &SwiftStore,
    bucket: &str,
    key: &str,
) -> SwiftStorageResult<SwiftWriteCommitGuard> {
    use storage_contracts::NamespaceLocking as _;

    let encoded = rustfs_utils::path::encode_dir_object(key);
    let lock = store.new_ns_lock(bucket, &encoded).await?;
    let timeout = rustfs_ecstore::api::set_disk::get_lock_acquire_timeout();
    SwiftWriteCommitGuard::acquire(&lock, timeout)
        .await
        .map_err(|err| SwiftStorageError::other(format!("failed to acquire self-copy write lock: {err}")))
}

pub(crate) async fn record_swift_bucket_object_write_memory(bucket: &str, previous_current_size: Option<u64>, new_size: u64) {
    rustfs_ecstore::api::data_usage::record_bucket_object_write_memory(bucket, previous_current_size, new_size).await;
}

pub(crate) async fn record_swift_bucket_object_delete_memory(bucket: &str, deleted_size: u64, removed_current_object: bool) {
    rustfs_ecstore::api::data_usage::record_bucket_object_delete_memory(bucket, deleted_size, removed_current_object).await;
}
