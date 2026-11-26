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

//! Use cases of the object metadata operations, migration batch B1 (rustfs/backlog#2766): object
//! ACL, tagging, legal hold, retention and torrent, one file per operation family.
//!
//! Responsible for: what each operation answers, in the gateway's types.
//! NOT responsible for: authorization (the legacy access hook and `Served::authorize` both call
//! one owned function before a use case runs), or counting the operation (each edge does).
//! Upstream: the legacy edge `storage/legacy_edge/object_meta.rs` and the gateway edge
//! `server/gateway_handlers/object_meta.rs`. Downstream: the object store.

pub(crate) mod acl;
pub(crate) mod legal_hold;
pub(crate) mod retention;
pub(crate) mod tagging;
pub(crate) mod tagging_proxy;
pub(crate) mod torrent;

use crate::app::RequestEnvelope;
use crate::app::object::shared::reject_veeam_sosapi_mutation;
use crate::app::runtime_sources::{AppContext, current_object_store_handle_for_context};
use crate::app::storage_api::object_meta_usecase::contract::ObjectOperations as _;
use crate::app::storage_api::object_meta_usecase::validate_bucket_object_lock_enabled_for_read;
use crate::app::storage_api::object_usecase::error::{StorageError, is_err_object_not_found, is_err_version_not_found};
use crate::app::storage_api::object_usecase::options::get_opts;
use crate::app::storage_api::object_usecase::{ECStore, StorageObjectInfo as ObjectInfo};
use crate::error::ApiError;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::sync::Arc;
use tracing::error;

/// The use cases of this batch, bound to one application context.
pub struct ObjectMetaUsecase {
    context: Option<Arc<AppContext>>,
}

impl ObjectMetaUsecase {
    pub fn with_context(context: Option<Arc<AppContext>>) -> Self {
        Self { context }
    }

    /// The store, or `InternalError` before the server has one.
    fn object_store(&self) -> S3Result<Arc<ECStore>> {
        current_object_store_handle_for_context(self.context.as_deref())
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))
    }

    /// The store, once `key` is known not to be a read-only Veeam SOSAPI system object.
    async fn store_outside_veeam_sosapi(&self, bucket: &str, key: &str) -> S3Result<Arc<ECStore>> {
        reject_veeam_sosapi_mutation(|| self.object_store().ok(), bucket, key).await?;
        self.object_store()
    }
}

/// Looks up an object's metadata in a bucket with Object Lock enabled.
async fn locked_object_info(
    store: &ECStore,
    envelope: &RequestEnvelope,
    bucket: &str,
    key: &str,
    version_id: Option<String>,
) -> S3Result<ObjectInfo> {
    validate_bucket_object_lock_enabled_for_read(bucket).await?;
    let explicit_version = version_id.is_some();
    let opts = get_opts(bucket, key, version_id, None, envelope.headers())
        .await
        .map_err(ApiError::from)?;
    let code = match store.get_object_info(bucket, key, &opts).await {
        Ok(info) if !info.delete_marker => return Ok(info),
        // A single pool can return the marker snapshot, while multi-pool lookup can reject it.
        Ok(_) | Err(StorageError::MethodNotAllowed) => {
            if explicit_version {
                S3ErrorCode::MethodNotAllowed
            } else {
                S3ErrorCode::NoSuchKey
            }
        }
        Err(error) if is_err_object_not_found(&error) || is_err_version_not_found(&error) => {
            if explicit_version {
                S3ErrorCode::NoSuchVersion
            } else {
                S3ErrorCode::NoSuchKey
            }
        }
        Err(StorageError::BucketNotFound(_) | StorageError::VolumeNotFound) => S3ErrorCode::NoSuchBucket,
        Err(error) => {
            error!(
                component = "storage",
                subsystem = "object",
                event = "object_info_load_failed",
                bucket = %bucket,
                object = %key,
                error = %error,
                "Failed to load object info"
            );
            return Err(s3_error!(InternalError, "{}", error.to_string()));
        }
    };
    let message = ApiError::error_code_to_message(&code);
    Err(S3Error::with_message(code, message))
}

#[cfg(test)]
mod tests {
    use super::{ObjectMetaUsecase, RequestEnvelope, S3ErrorCode};
    use crate::app::RequestHead;
    use http::{Extensions, HeaderMap, Method, Uri};
    use rustfs_gateway_types::dto::{
        DeleteObjectTaggingInput, GetObjectAclInput, GetObjectLegalHoldInput, GetObjectRetentionInput, GetObjectTaggingInput,
        PutObjectAclInput, Tag,
    };
    use rustfs_gateway_types::{BucketName, ObjectKey};

    fn envelope() -> RequestEnvelope {
        let head = RequestHead {
            method: Method::GET,
            uri: Uri::from_static("/bucket/key"),
            headers: HeaderMap::new(),
            credentials: None,
            region: None,
            service: None,
        };
        RequestEnvelope::from_parts(head, Extensions::new())
    }

    fn object() -> (BucketName, ObjectKey) {
        (
            BucketName::new("test-bucket").expect("a bucket name"),
            ObjectKey::new("test-key").expect("a key"),
        )
    }

    /// Every use case answers `InternalError` before the server has a store.
    #[tokio::test]
    async fn every_use_case_refuses_before_the_store_is_up() {
        let usecase = ObjectMetaUsecase::with_context(None);
        let (bucket, key) = object();
        let errors = [
            usecase
                .execute_get_object_acl(
                    envelope(),
                    GetObjectAclInput {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        ..Default::default()
                    },
                )
                .await
                .map(drop),
            usecase
                .execute_put_object_acl(
                    envelope(),
                    PutObjectAclInput {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        ..Default::default()
                    },
                )
                .await
                .map(drop),
            usecase
                .execute_get_object_legal_hold(
                    envelope(),
                    GetObjectLegalHoldInput {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        ..Default::default()
                    },
                )
                .await
                .map(drop),
            usecase
                .execute_get_object_retention(
                    envelope(),
                    GetObjectRetentionInput {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        ..Default::default()
                    },
                )
                .await
                .map(drop),
            usecase
                .execute_get_object_tagging(
                    envelope(),
                    GetObjectTaggingInput {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        ..Default::default()
                    },
                )
                .await
                .map(drop),
            usecase
                .put_object_retention_fields(envelope(), bucket.as_str(), key.as_str(), None, None)
                .await
                .map(drop),
            usecase
                .put_object_legal_hold_fields(envelope(), bucket.as_str(), key.as_str(), None, None)
                .await
                .map(drop),
            usecase
                .put_object_tagging_fields(
                    envelope(),
                    bucket.as_str(),
                    key.as_str(),
                    None,
                    vec![Tag {
                        key: Some("project".into()),
                        value: Some("test".into()),
                    }],
                )
                .await
                .map(drop),
            usecase
                .execute_delete_object_tagging(
                    envelope(),
                    DeleteObjectTaggingInput {
                        bucket,
                        key,
                        ..Default::default()
                    },
                )
                .await
                .map(drop),
        ];

        for (index, result) in errors.into_iter().enumerate() {
            let error = result.expect_err("no store");
            assert_eq!(error.code(), &S3ErrorCode::InternalError, "use case {index}");
        }
    }
    #[tokio::test]
    async fn invalid_put_tags_are_rejected_before_version_and_store_lookup() {
        let usecase = ObjectMetaUsecase::with_context(None);
        let tag = |key: Option<&str>, value: Option<&str>| Tag {
            key: key.map(str::to_owned),
            value: value.map(str::to_owned),
        };
        let too_many = (0..11).map(|index| tag(Some(&format!("k{index}")), Some("value"))).collect();
        for (tags, expected) in [
            (too_many, "Cannot have more than 10 tags per object"),
            (vec![tag(Some(""), Some("v1"))], "Tag key cannot be empty"),
            (vec![tag(None, Some("value"))], "Tag key cannot be empty"),
            (vec![tag(Some("key"), None)], "Tag value cannot be null"),
        ] {
            let error = usecase
                .put_object_tagging_fields(envelope(), "test-bucket", "test-key", Some("invalid".into()), tags)
                .await
                .unwrap_err();
            assert_eq!(error.code(), &S3ErrorCode::InvalidTag);
            assert_eq!(error.message(), Some(expected));
        }
    }
}
