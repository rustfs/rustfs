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

//! Bucket application use-case contracts.
#![allow(dead_code)]

use crate::app::context::{AppContext, get_global_app_context};
use crate::error::ApiError;
use crate::storage::access::authorize_request;
use crate::storage::ecfs::{default_owner, serialize_acl, stored_acl_from_canned_bucket, stored_acl_from_grant_headers};
use crate::storage::helper::OperationHelper;
use crate::storage::*;
use metrics::counter;
use rustfs_ecstore::bucket::{metadata::BUCKET_ACL_CONFIG, metadata_sys};
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::{BucketOptions, DeleteBucketOptions, MakeBucketOptions, StorageAPI};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_targets::EventName;
use rustfs_utils::http::RUSTFS_FORCE_DELETE;
use rustfs_utils::string::parse_bool;
use s3s::dto::*;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::sync::Arc;
use tracing::{debug, instrument};
use urlencoding::encode;

pub type BucketUsecaseResult<T> = Result<T, ApiError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBucketRequest {
    pub bucket: String,
    pub object_lock_enabled: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateBucketResponse;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteBucketRequest {
    pub bucket: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteBucketResponse;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadBucketRequest {
    pub bucket: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeadBucketResponse;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsV2Request {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub continuation_token: Option<String>,
    pub max_keys: Option<i32>,
    pub fetch_owner: Option<bool>,
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsV2Item {
    pub key: String,
    pub etag: Option<String>,
    pub size: i64,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListObjectsV2Response {
    pub objects: Vec<ListObjectsV2Item>,
    pub common_prefixes: Vec<String>,
    pub key_count: i32,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

#[async_trait::async_trait]
pub trait BucketUsecase: Send + Sync {
    async fn create_bucket(&self, req: CreateBucketRequest) -> BucketUsecaseResult<CreateBucketResponse>;

    async fn delete_bucket(&self, req: DeleteBucketRequest) -> BucketUsecaseResult<DeleteBucketResponse>;

    async fn head_bucket(&self, req: HeadBucketRequest) -> BucketUsecaseResult<HeadBucketResponse>;

    async fn list_objects_v2(&self, req: ListObjectsV2Request) -> BucketUsecaseResult<ListObjectsV2Response>;
}

#[derive(Clone, Default)]
pub struct DefaultBucketUsecase {
    context: Option<Arc<AppContext>>,
}

impl DefaultBucketUsecase {
    pub fn new(context: Arc<AppContext>) -> Self {
        Self { context: Some(context) }
    }

    pub fn without_context() -> Self {
        Self { context: None }
    }

    pub fn from_global() -> Self {
        Self {
            context: get_global_app_context(),
        }
    }

    pub fn context(&self) -> Option<Arc<AppContext>> {
        self.context.clone()
    }

    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    pub async fn execute_create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let helper = OperationHelper::new(&req, EventName::BucketCreated, "s3:CreateBucket");
        let CreateBucketInput {
            bucket,
            acl,
            grant_full_control,
            grant_read,
            grant_read_acp,
            grant_write,
            grant_write_acp,
            object_lock_enabled_for_bucket,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        counter!("rustfs_create_bucket_total").increment(1);

        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    force_create: false, // TODO: force support
                    lock_enabled: object_lock_enabled_for_bucket.is_some_and(|v| v),
                    ..Default::default()
                },
            )
            .await
            .map_err(ApiError::from)?;

        let owner = default_owner();
        let mut stored_acl = stored_acl_from_grant_headers(
            &owner,
            grant_read.map(|v| v.to_string()),
            grant_write.map(|v| v.to_string()),
            grant_read_acp.map(|v| v.to_string()),
            grant_write_acp.map(|v| v.to_string()),
            grant_full_control.map(|v| v.to_string()),
        )?;

        if stored_acl.is_none()
            && let Some(canned) = acl
        {
            stored_acl = Some(stored_acl_from_canned_bucket(canned.as_str(), &owner));
        }

        let stored_acl = stored_acl.unwrap_or_else(|| stored_acl_from_canned_bucket(BucketCannedACL::PRIVATE, &owner));
        let data = serialize_acl(&stored_acl)?;
        metadata_sys::update(&bucket, BUCKET_ACL_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        let output = CreateBucketOutput::default();

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_bucket(&self, mut req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let helper = OperationHelper::new(&req, EventName::BucketRemoved, "s3:DeleteBucket");
        let input = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // get value from header, support mc style
        let force_str = req
            .headers
            .get(RUSTFS_FORCE_DELETE)
            .map(|v| v.to_str().unwrap_or_default())
            .unwrap_or(
                req.headers
                    .get("x-minio-force-delete")
                    .map(|v| v.to_str().unwrap_or_default())
                    .unwrap_or_default(),
            );

        let force = parse_bool(force_str).unwrap_or_default();

        if force {
            authorize_request(&mut req, Action::S3Action(S3Action::ForceDeleteBucketAction)).await?;
        }

        store
            .delete_bucket(
                &input.bucket,
                &DeleteBucketOptions {
                    force,
                    ..Default::default()
                },
            )
            .await
            .map_err(ApiError::from)?;

        let result = Ok(S3Response::new(DeleteBucketOutput {}));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let input = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        // warn!("list_objects_v2 req {:?}", &req.input);
        let ListObjectsV2Input {
            bucket,
            continuation_token,
            delimiter,
            encoding_type,
            fetch_owner,
            max_keys,
            prefix,
            start_after,
            ..
        } = req.input;

        let prefix = prefix.unwrap_or_default();

        // Log debug info for prefixes with special characters to help diagnose encoding issues
        if prefix.contains([' ', '+', '%', '\n', '\r', '\0']) {
            debug!("LIST objects with special characters in prefix: {:?}", prefix);
        }

        let max_keys = max_keys.unwrap_or(1000);
        if max_keys < 0 {
            return Err(S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid max keys".to_string()));
        }

        let delimiter = delimiter.filter(|v| !v.is_empty());

        validate_list_object_unordered_with_delimiter(delimiter.as_ref(), req.uri.query())?;

        // Save original start_after for response (per S3 API spec, must echo back if provided)
        let response_start_after = start_after.clone();
        let start_after_for_query = start_after.filter(|v| !v.is_empty());

        // Save original continuation_token for response (per S3 API spec, must echo back if provided)
        // Note: empty string should still be echoed back in the response
        let response_continuation_token = continuation_token.clone();
        let continuation_token_for_query = continuation_token.filter(|v| !v.is_empty());

        // Decode continuation_token from base64 for internal use
        let decoded_continuation_token = continuation_token_for_query
            .map(|token| {
                base64_simd::STANDARD
                    .decode_to_vec(token.as_bytes())
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid continuation token"))
                    .and_then(|bytes| {
                        String::from_utf8(bytes).map_err(|_| s3_error!(InvalidArgument, "Invalid continuation token"))
                    })
            })
            .transpose()?;

        let store = get_validated_store(&bucket).await?;

        let incl_deleted = req
            .headers
            .get(rustfs_utils::http::headers::RUSTFS_INCLUDE_DELETED)
            .is_some_and(|v| v.to_str().unwrap_or_default() == "true");

        let object_infos = store
            .list_objects_v2(
                &bucket,
                &prefix,
                decoded_continuation_token,
                delimiter.clone(),
                max_keys,
                fetch_owner.unwrap_or_default(),
                start_after_for_query,
                incl_deleted,
            )
            .await
            .map_err(ApiError::from)?;

        // warn!("object_infos objects {:?}", object_infos.objects);

        // Apply URL encoding if encoding_type is "url"
        // Note: S3 URL encoding should encode special characters but preserve path separators (/)
        let should_encode = encoding_type.as_ref().map(|e| e.as_str() == "url").unwrap_or(false);

        // Helper function to encode S3 keys/prefixes (preserving /)
        // S3 URL encoding encodes special characters but keeps '/' unencoded
        let encode_s3_name = |name: &str| -> String {
            name.split('/')
                .map(|part| encode(part).to_string())
                .collect::<Vec<_>>()
                .join("/")
        };

        let objects: Vec<Object> = object_infos
            .objects
            .iter()
            .filter(|v| !v.name.is_empty())
            .map(|v| {
                let key = if should_encode {
                    encode_s3_name(&v.name)
                } else {
                    v.name.to_owned()
                };
                let mut obj = Object {
                    key: Some(key),
                    last_modified: v.mod_time.map(Timestamp::from),
                    size: Some(v.get_actual_size().unwrap_or_default()),
                    e_tag: v.etag.clone().map(|etag| to_s3s_etag(&etag)),
                    storage_class: v.storage_class.clone().map(ObjectStorageClass::from),
                    ..Default::default()
                };

                if fetch_owner.is_some_and(|v| v) {
                    obj.owner = Some(Owner {
                        display_name: Some("rustfs".to_owned()),
                        id: Some("v0.1".to_owned()),
                    });
                }
                obj
            })
            .collect();

        let common_prefixes: Vec<CommonPrefix> = object_infos
            .prefixes
            .into_iter()
            .map(|v| {
                let prefix = if should_encode { encode_s3_name(&v) } else { v };
                CommonPrefix { prefix: Some(prefix) }
            })
            .collect();

        // KeyCount should include both objects and common prefixes per S3 API spec
        let key_count = (objects.len() + common_prefixes.len()) as i32;

        // Encode next_continuation_token to base64
        let next_continuation_token = object_infos
            .next_continuation_token
            .map(|token| base64_simd::STANDARD.encode_to_string(token.as_bytes()));

        let output = ListObjectsV2Output {
            is_truncated: Some(object_infos.is_truncated),
            continuation_token: response_continuation_token,
            next_continuation_token,
            start_after: response_start_after,
            key_count: Some(key_count),
            max_keys: Some(max_keys),
            contents: Some(objects),
            delimiter,
            encoding_type: encoding_type.clone(),
            name: Some(bucket),
            prefix: Some(prefix),
            common_prefixes: Some(common_prefixes),
            ..Default::default()
        };

        // let output = ListObjectsV2Output { ..Default::default() };
        Ok(S3Response::new(output))
    }
}

#[async_trait::async_trait]
impl BucketUsecase for DefaultBucketUsecase {
    async fn create_bucket(&self, req: CreateBucketRequest) -> BucketUsecaseResult<CreateBucketResponse> {
        let _ = req;
        Err(ApiError::from(StorageError::other(
            "DefaultBucketUsecase::create_bucket DTO path is not implemented yet",
        )))
    }

    async fn delete_bucket(&self, req: DeleteBucketRequest) -> BucketUsecaseResult<DeleteBucketResponse> {
        let _ = req;
        Err(ApiError::from(StorageError::other(
            "DefaultBucketUsecase::delete_bucket DTO path is not implemented yet",
        )))
    }

    async fn head_bucket(&self, req: HeadBucketRequest) -> BucketUsecaseResult<HeadBucketResponse> {
        let _ = req;
        Err(ApiError::from(StorageError::other(
            "DefaultBucketUsecase::head_bucket DTO path is not implemented yet",
        )))
    }

    async fn list_objects_v2(&self, req: ListObjectsV2Request) -> BucketUsecaseResult<ListObjectsV2Response> {
        let _ = req;
        Err(ApiError::from(StorageError::other(
            "DefaultBucketUsecase::list_objects_v2 DTO path is not implemented yet",
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Extensions, HeaderMap, Method, Uri};

    fn build_request<T>(input: T, method: Method) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn execute_create_bucket_returns_internal_error_when_store_uninitialized() {
        let input = CreateBucketInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_create_bucket(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_delete_bucket_returns_internal_error_when_store_uninitialized() {
        let input = DeleteBucketInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_delete_bucket(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_head_bucket_returns_internal_error_when_store_uninitialized() {
        let input = HeadBucketInput::builder().bucket("test-bucket".to_string()).build().unwrap();

        let req = build_request(input, Method::HEAD);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_head_bucket(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_objects_v2_rejects_negative_max_keys() {
        let input = ListObjectsV2Input::builder()
            .bucket("test-bucket".to_string())
            .max_keys(Some(-1))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects_v2(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }
}
