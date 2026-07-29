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

use rustfs_ecstore::api::bucket::metadata::BUCKET_TAGGING_CONFIG;
pub(crate) use rustfs_ecstore::api::bucket::metadata::BucketMetadata as SwiftBucketMetadata;
use rustfs_ecstore::api::bucket::metadata_sys::{get as get_swift_bucket_metadata_from_backend, update_config_with};
use rustfs_ecstore::api::bucket::utils::serialize as serialize_bucket_config;
use rustfs_ecstore::api::error::Error as SwiftStorageError;
pub(crate) use rustfs_ecstore::api::error::Result as SwiftStorageResult;
use rustfs_ecstore::api::notification::get_global_notification_sys;
pub(crate) use rustfs_ecstore::api::runtime::object_store_handle as resolve_swift_object_store_handle;
use rustfs_ecstore::api::storage::ECStore as SwiftStore;
use rustfs_storage_api as storage_contracts;
use s3s::dto::Tagging;

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
        get_swift_bucket_metadata, get_swift_bucket_usage, resolve_swift_object_store_handle, update_swift_bucket_tagging,
    };
}

pub(crate) mod versioning {
    pub(crate) use super::storage_contracts::{ListOperations, ObjectOperations};
}

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_SWIFT_STORAGE: &str = "swift_storage";
const EVENT_SWIFT_BUCKET_TAGGING_UPDATE: &str = "swift_bucket_tagging_update";

/// Marks the refusal to rewrite an unreadable persisted tagging config, so the
/// caller can turn it into an actionable client error rather than a generic
/// storage failure. Carried through the ecstore error, which is a string type.
const UNREADABLE_TAGGING_SENTINEL: &str = "swift: persisted tagging config could not be parsed";

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

    let result = update_config_with(&bucket, BUCKET_TAGGING_CONFIG, |bm| {
        // Merging onto an unparseable tag set would silently drop every tag
        // the bucket has — including the container ACL and versioning tags —
        // because the rewrite closures treat "no parsed tags" as "no tags".
        // Refuse instead: the persisted config is intact, just unreadable.
        if !bm.tagging_config_xml.is_empty() && bm.tagging_config.is_none() {
            return Err(SwiftStorageError::other(UNREADABLE_TAGGING_SENTINEL));
        }

        let tagging = match rewrite(bm.tagging_config.as_ref()) {
            Ok(tagging) => tagging,
            Err(err) => {
                rejected = Some(err);
                return Err(SwiftStorageError::other("swift: tagging rewrite rejected the update"));
            }
        };

        if tagging.tag_set.is_empty() {
            Ok(Vec::new())
        } else {
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
        let unreadable = err.to_string().contains(UNREADABLE_TAGGING_SENTINEL);
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
