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
use crate::auth::get_condition_values;
use crate::error::ApiError;
use crate::server::RemoteAddr;
use crate::storage::access::{ReqInfo, authorize_request};
use crate::storage::ecfs::{
    RUSTFS_OWNER, default_owner, is_public_grant, parse_acl_json_or_canned_bucket, serialize_acl, stored_acl_from_canned_bucket,
    stored_acl_from_grant_headers, stored_acl_from_policy, stored_grant_to_dto, stored_owner_to_dto,
};
use crate::storage::helper::OperationHelper;
use crate::storage::s3_api::{encryption, replication, tagging};
use crate::storage::*;
use futures::StreamExt;
use http::StatusCode;
use metrics::counter;
use rustfs_ecstore::bucket::{
    lifecycle::bucket_lifecycle_ops::validate_transition_tier,
    metadata::{
        BUCKET_ACL_CONFIG, BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG,
        BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, BUCKET_REPLICATION_CONFIG, BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG,
        BUCKET_VERSIONING_CONFIG,
    },
    metadata_sys,
    policy_sys::PolicySys,
    utils::serialize,
    versioning_sys::BucketVersioningSys,
};
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::{BucketOptions, DeleteBucketOptions, MakeBucketOptions, StorageAPI};
use rustfs_notify::notifier_global;
use rustfs_policy::policy::{
    action::{Action, S3Action},
    {BucketPolicy, BucketPolicyArgs, Effect, Validator},
};
use rustfs_targets::{
    EventName,
    arn::{ARN, TargetIDError},
};
use rustfs_utils::http::RUSTFS_FORCE_DELETE;
use rustfs_utils::string::parse_bool;
use s3s::dto::*;
use s3s::xml;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::{fmt::Display, sync::Arc};
use tracing::{debug, error, info, instrument, warn};
use urlencoding::encode;

pub type BucketUsecaseResult<T> = Result<T, ApiError>;

fn serialize_config<T: xml::Serialize>(value: &T) -> S3Result<Vec<u8>> {
    serialize(value).map_err(to_internal_error)
}

fn to_internal_error(err: impl Display) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, format!("{err}"))
}

fn resolve_notification_region(global_region: Option<String>, request_region: Option<String>) -> String {
    global_region.unwrap_or_else(|| request_region.unwrap_or_default())
}

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

    pub async fn execute_put_bucket_acl(&self, req: S3Request<PutBucketAclInput>) -> S3Result<S3Response<PutBucketAclOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutBucketAclInput {
            bucket,
            acl,
            access_control_policy,
            grant_full_control,
            grant_read,
            grant_read_acp,
            grant_write,
            grant_write_acp,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let owner = default_owner();
        let mut stored_acl = access_control_policy
            .as_ref()
            .map(|policy| stored_acl_from_policy(policy, &owner))
            .transpose()?;

        if stored_acl.is_none() {
            stored_acl = stored_acl_from_grant_headers(
                &owner,
                grant_read.map(|v| v.to_string()),
                grant_write.map(|v| v.to_string()),
                grant_read_acp.map(|v| v.to_string()),
                grant_write_acp.map(|v| v.to_string()),
                grant_full_control.map(|v| v.to_string()),
            )?;
        }

        if stored_acl.is_none()
            && let Some(canned) = acl
        {
            stored_acl = Some(stored_acl_from_canned_bucket(canned.as_str(), &owner));
        }

        let stored_acl = stored_acl.unwrap_or_else(|| stored_acl_from_canned_bucket(BucketCannedACL::PRIVATE, &owner));

        if let Ok((config, _)) = metadata_sys::get_public_access_block_config(&bucket).await
            && config.block_public_acls.unwrap_or(false)
            && stored_acl.grants.iter().any(is_public_grant)
        {
            return Err(s3_error!(AccessDenied, "Access Denied"));
        }

        let data = serialize_acl(&stored_acl)?;
        metadata_sys::update(&bucket, BUCKET_ACL_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(PutBucketAclOutput::default()))
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

    pub async fn execute_get_bucket_acl(&self, req: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetBucketAclInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let owner = default_owner();
        let stored_acl = match metadata_sys::get_bucket_acl_config(&bucket).await {
            Ok((acl, _)) => parse_acl_json_or_canned_bucket(&acl, &owner),
            Err(err) => {
                if err != StorageError::ConfigNotFound {
                    return Err(ApiError::from(err).into());
                }
                stored_acl_from_canned_bucket(BucketCannedACL::PRIVATE, &owner)
            }
        };

        let mut sorted_grants = stored_acl.grants.clone();
        sorted_grants.sort_by_key(|grant| grant.grantee.grantee_type != "Group");
        let grants = sorted_grants.iter().map(stored_grant_to_dto).collect();

        Ok(S3Response::new(GetBucketAclOutput {
            grants: Some(grants),
            owner: Some(stored_owner_to_dto(&stored_acl.owner)),
        }))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
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

        if let Some(region) = rustfs_ecstore::global::get_global_region() {
            return Ok(S3Response::new(GetBucketLocationOutput {
                location_constraint: Some(BucketLocationConstraint::from(region)),
            }));
        }

        Ok(S3Response::new(GetBucketLocationOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mut req = req;

        if req.credentials.as_ref().is_none_or(|cred| cred.access_key.is_empty()) {
            return Err(S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied"));
        }

        let bucket_infos = if let Err(e) = authorize_request(&mut req, Action::S3Action(S3Action::ListAllMyBucketsAction)).await {
            if e.code() != &S3ErrorCode::AccessDenied {
                return Err(e);
            }

            let mut list_bucket_infos = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;

            list_bucket_infos = futures::stream::iter(list_bucket_infos)
                .filter_map(|info| async {
                    let mut req_clone = req.clone();
                    let req_info = req_clone.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
                    req_info.bucket = Some(info.name.clone());

                    if authorize_request(&mut req_clone, Action::S3Action(S3Action::ListBucketAction))
                        .await
                        .is_ok()
                        || authorize_request(&mut req_clone, Action::S3Action(S3Action::GetBucketLocationAction))
                            .await
                            .is_ok()
                    {
                        Some(info)
                    } else {
                        None
                    }
                })
                .collect()
                .await;

            if list_bucket_infos.is_empty() {
                return Err(S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied"));
            }
            list_bucket_infos
        } else {
            store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?
        };

        let buckets: Vec<Bucket> = bucket_infos
            .iter()
            .map(|v| Bucket {
                creation_date: v.created.map(Timestamp::from),
                name: Some(v.name.clone()),
                ..Default::default()
            })
            .collect();

        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(RUSTFS_OWNER.to_owned()),
            ..Default::default()
        }))
    }

    pub async fn execute_delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let DeleteBucketEncryptionInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_SSECONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(DeleteBucketEncryptionOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_delete_bucket_cors(
        &self,
        req: S3Request<DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let DeleteBucketCorsInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_CORS_CONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(DeleteBucketCorsOutput {}))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let DeleteBucketLifecycleInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_LIFECYCLE_CONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(DeleteBucketLifecycleOutput::default()))
    }

    pub async fn execute_delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let DeleteBucketPolicyInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_POLICY_CONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(DeleteBucketPolicyOutput {}))
    }

    pub async fn execute_delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let DeleteBucketReplicationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;
        metadata_sys::delete(&bucket, BUCKET_REPLICATION_CONFIG)
            .await
            .map_err(ApiError::from)?;

        // TODO: remove targets
        info!(bucket = %bucket, "deleted bucket replication config");

        Ok(S3Response::new(DeleteBucketReplicationOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let DeleteBucketTaggingInput { bucket, .. } = req.input;

        metadata_sys::delete(&bucket, BUCKET_TAGGING_CONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(tagging::build_delete_bucket_tagging_output()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_delete_public_access_block(
        &self,
        req: S3Request<DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<DeletePublicAccessBlockOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let DeletePublicAccessBlockInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::with_status(DeletePublicAccessBlockOutput::default(), StatusCode::NO_CONTENT))
    }

    pub async fn execute_get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetBucketEncryptionInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let server_side_encryption_configuration = match metadata_sys::get_sse_config(&bucket).await {
            Ok((cfg, _)) => Some(cfg),
            Err(err) => {
                warn!("get_sse_config err {:?}", err);
                None
            }
        };

        Ok(S3Response::new(encryption::build_get_bucket_encryption_output(
            server_side_encryption_configuration,
        )))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_bucket_cors(&self, req: S3Request<GetBucketCorsInput>) -> S3Result<S3Response<GetBucketCorsOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetBucketCorsInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let cors_configuration = match metadata_sys::get_cors_config(&bucket).await {
            Ok((config, _)) => config,
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::NoSuchCORSConfiguration,
                        "The CORS configuration does not exist".to_string(),
                    ));
                }
                warn!("get_cors_config err {:?}", &err);
                return Err(ApiError::from(err).into());
            }
        };

        Ok(S3Response::new(GetBucketCorsOutput {
            cors_rules: Some(cors_configuration.cors_rules),
        }))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetBucketLifecycleConfigurationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let rules = match metadata_sys::get_lifecycle_config(&bucket).await {
            Ok((cfg, _)) => cfg.rules,
            Err(_) => {
                return Err(s3_error!(NoSuchLifecycleConfiguration));
            }
        };

        Ok(S3Response::new(GetBucketLifecycleConfigurationOutput {
            rules: Some(rules),
            ..Default::default()
        }))
    }

    pub async fn execute_get_bucket_notification_configuration(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetBucketNotificationConfigurationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let has_notification_config = metadata_sys::get_notification_config(&bucket).await.unwrap_or_else(|err| {
            warn!("get_notification_config err {:?}", err);
            None
        });

        if let Some(NotificationConfiguration {
            event_bridge_configuration,
            lambda_function_configurations,
            queue_configurations,
            topic_configurations,
        }) = has_notification_config
        {
            Ok(S3Response::new(GetBucketNotificationConfigurationOutput {
                event_bridge_configuration,
                lambda_function_configurations,
                queue_configurations,
                topic_configurations,
            }))
        } else {
            Ok(S3Response::new(GetBucketNotificationConfigurationOutput::default()))
        }
    }

    pub async fn execute_get_bucket_policy(
        &self,
        req: S3Request<GetBucketPolicyInput>,
    ) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetBucketPolicyInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let (policy_str, _) = match metadata_sys::get_bucket_policy_raw(&bucket).await {
            Ok(res) => res,
            Err(err) => {
                if StorageError::ConfigNotFound == err {
                    return Err(s3_error!(NoSuchBucketPolicy));
                }
                return Err(S3Error::with_message(S3ErrorCode::InternalError, err.to_string()));
            }
        };

        Ok(S3Response::new(GetBucketPolicyOutput {
            policy: Some(policy_str),
        }))
    }

    pub async fn execute_get_bucket_policy_status(
        &self,
        req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetBucketPolicyStatusInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        let conditions = get_condition_values(&req.headers, &rustfs_credentials::Credentials::default(), None, None, remote_addr);

        let read_allowed = PolicySys::is_allowed(&BucketPolicyArgs {
            bucket: &bucket,
            action: Action::S3Action(S3Action::ListBucketAction),
            is_owner: false,
            account: "",
            groups: &None,
            conditions: &conditions,
            object: "",
        })
        .await;

        let write_allowed = PolicySys::is_allowed(&BucketPolicyArgs {
            bucket: &bucket,
            action: Action::S3Action(S3Action::PutObjectAction),
            is_owner: false,
            account: "",
            groups: &None,
            conditions: &conditions,
            object: "",
        })
        .await;

        let mut is_public = read_allowed || write_allowed;
        let ignore_public_acls = match metadata_sys::get_public_access_block_config(&bucket).await {
            Ok((config, _)) => config.ignore_public_acls.unwrap_or(false),
            Err(_) => false,
        };

        let owner = default_owner();
        let acl_public = match metadata_sys::get_bucket_acl_config(&bucket).await {
            Ok((acl, _)) => {
                let stored_acl = parse_acl_json_or_canned_bucket(&acl, &owner);
                stored_acl
                    .grants
                    .iter()
                    .any(|grant| is_public_grant(grant) && !ignore_public_acls)
            }
            Err(_) => false,
        };

        if acl_public {
            is_public = true;
        }

        let policy_public = match metadata_sys::get_bucket_policy(&bucket).await {
            Ok((cfg, _)) => cfg.statements.iter().any(|statement| {
                matches!(statement.effect, Effect::Allow)
                    && statement.principal.is_match("*")
                    && statement.conditions.is_empty()
                    && statement.actions.is_match(&Action::S3Action(S3Action::ListBucketAction))
            }),
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    false
                } else {
                    return Err(ApiError::from(err).into());
                }
            }
        };

        if policy_public {
            is_public = true;
        }

        let output = GetBucketPolicyStatusOutput {
            policy_status: Some(PolicyStatus {
                is_public: Some(is_public),
            }),
        };

        Ok(S3Response::new(output))
    }

    pub async fn execute_get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetBucketReplicationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let replication_configuration = match metadata_sys::get_replication_config(&bucket).await {
            Ok((cfg, _created)) => cfg,
            Err(err) => {
                error!("get_replication_config err {:?}", err);
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::ReplicationConfigurationNotFoundError,
                        "replication not found".to_string(),
                    ));
                }
                return Err(ApiError::from(err).into());
            }
        };

        Ok(S3Response::new(replication::build_get_bucket_replication_output(
            replication_configuration,
        )))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_bucket_tagging(
        &self,
        req: S3Request<GetBucketTaggingInput>,
    ) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetBucketTaggingInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let Tagging { tag_set } = match metadata_sys::get_tagging_config(&bucket).await {
            Ok((tags, _)) => tags,
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(S3ErrorCode::NoSuchTagSet, "The TagSet does not exist".to_string()));
                }
                warn!("get_tagging_config err {:?}", &err);
                return Err(ApiError::from(err).into());
            }
        };

        Ok(S3Response::new(tagging::build_get_bucket_tagging_output(tag_set)))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_public_access_block(
        &self,
        req: S3Request<GetPublicAccessBlockInput>,
    ) -> S3Result<S3Response<GetPublicAccessBlockOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetPublicAccessBlockInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let config = match metadata_sys::get_public_access_block_config(&bucket).await {
            Ok((config, _)) => config,
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::Custom("NoSuchPublicAccessBlockConfiguration".into()),
                        "Public access block configuration does not exist".to_string(),
                    ));
                }
                return Err(ApiError::from(err).into());
            }
        };

        Ok(S3Response::new(GetPublicAccessBlockOutput {
            public_access_block_configuration: Some(config),
        }))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetBucketVersioningInput { bucket, .. } = req.input;
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let VersioningConfiguration { status, .. } = BucketVersioningSys::get(&bucket).await.map_err(ApiError::from)?;

        Ok(S3Response::new(GetBucketVersioningOutput {
            status,
            ..Default::default()
        }))
    }

    pub async fn execute_put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutBucketEncryptionInput {
            bucket,
            server_side_encryption_configuration,
            ..
        } = req.input;

        info!("sse_config {:?}", &server_side_encryption_configuration);

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = serialize_config(&server_side_encryption_configuration)?;
        metadata_sys::update(&bucket, BUCKET_SSECONFIG, data)
            .await
            .map_err(ApiError::from)?;
        Ok(S3Response::new(encryption::build_put_bucket_encryption_output()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutBucketLifecycleConfigurationInput {
            bucket,
            lifecycle_configuration,
            ..
        } = req.input;

        let Some(input_cfg) = lifecycle_configuration else { return Err(s3_error!(InvalidArgument)) };

        let rcfg = metadata_sys::get_object_lock_config(&bucket).await;
        if let Ok(rcfg) = rcfg
            && let Err(err) = rustfs_ecstore::bucket::lifecycle::lifecycle::Lifecycle::validate(&input_cfg, &rcfg.0).await
        {
            return Err(S3Error::with_message(S3ErrorCode::Custom("ValidateFailed".into()), err.to_string()));
        }

        if let Err(err) = validate_transition_tier(&input_cfg).await {
            return Err(S3Error::with_message(S3ErrorCode::Custom("CustomError".into()), err.to_string()));
        }

        let data = serialize_config(&input_cfg)?;
        metadata_sys::update(&bucket, BUCKET_LIFECYCLE_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(PutBucketLifecycleConfigurationOutput::default()))
    }

    pub async fn execute_put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }
        let request_region = req.region.clone();

        let PutBucketNotificationConfigurationInput {
            bucket,
            notification_configuration,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = serialize_config(&notification_configuration)?;
        metadata_sys::update(&bucket, BUCKET_NOTIFICATION_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        let region = resolve_notification_region(rustfs_ecstore::global::get_global_region(), request_region);
        let clear_rules = notifier_global::clear_bucket_notification_rules(&bucket);
        let parse_rules = async {
            let mut event_rules = Vec::new();

            process_queue_configurations(&mut event_rules, notification_configuration.queue_configurations.clone(), |arn_str| {
                ARN::parse(arn_str)
                    .map(|arn| arn.target_id)
                    .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
            })?;
            process_topic_configurations(&mut event_rules, notification_configuration.topic_configurations.clone(), |arn_str| {
                ARN::parse(arn_str)
                    .map(|arn| arn.target_id)
                    .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
            })?;
            process_lambda_configurations(
                &mut event_rules,
                notification_configuration.lambda_function_configurations.clone(),
                |arn_str| {
                    ARN::parse(arn_str)
                        .map(|arn| arn.target_id)
                        .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
                },
            )?;

            Ok::<_, TargetIDError>(event_rules)
        };

        let (clear_result, event_rules_result) = tokio::join!(clear_rules, parse_rules);

        clear_result.map_err(|e| s3_error!(InternalError, "Failed to clear rules: {e}"))?;
        let event_rules =
            event_rules_result.map_err(|e| s3_error!(InvalidArgument, "Invalid ARN in notification configuration: {e}"))?;
        warn!("notify event rules: {:?}", &event_rules);

        notifier_global::add_event_specific_rules(&bucket, &region, &event_rules)
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to add rules: {e}"))?;

        Ok(S3Response::new(PutBucketNotificationConfigurationOutput {}))
    }

    pub async fn execute_put_bucket_policy(
        &self,
        req: S3Request<PutBucketPolicyInput>,
    ) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutBucketPolicyInput { bucket, policy, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let cfg: BucketPolicy =
            serde_json::from_str(&policy).map_err(|e| s3_error!(InvalidArgument, "parse policy failed {:?}", e))?;

        if let Err(err) = cfg.is_valid() {
            warn!("put_bucket_policy err input {:?}, {:?}", &policy, err);
            return Err(s3_error!(MalformedPolicy));
        }

        let is_public_policy = cfg
            .statements
            .iter()
            .any(|statement| matches!(statement.effect, Effect::Allow) && statement.principal.is_match("*"));

        if is_public_policy {
            match metadata_sys::get_public_access_block_config(&bucket).await {
                Ok((config, _)) => {
                    if config.block_public_policy.unwrap_or(false) {
                        return Err(s3_error!(AccessDenied, "Access Denied"));
                    }
                }
                Err(err) => {
                    if err != StorageError::ConfigNotFound {
                        warn!("get_public_access_block_config err {:?}", &err);
                        return Err(ApiError::from(err).into());
                    }
                }
            }
        }

        let data = policy.as_bytes().to_vec();

        metadata_sys::update(&bucket, BUCKET_POLICY_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(PutBucketPolicyOutput {}))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_bucket_cors(&self, req: S3Request<PutBucketCorsInput>) -> S3Result<S3Response<PutBucketCorsOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutBucketCorsInput {
            bucket,
            cors_configuration,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = serialize_config(&cors_configuration)?;
        metadata_sys::update(&bucket, BUCKET_CORS_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(PutBucketCorsOutput::default()))
    }

    pub async fn execute_put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutBucketReplicationInput {
            bucket,
            replication_configuration,
            ..
        } = req.input;
        info!(bucket = %bucket, "updating bucket replication config");

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // TODO: check enable, versioning enable
        let data = serialize_config(&replication_configuration)?;
        metadata_sys::update(&bucket, BUCKET_REPLICATION_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(replication::build_put_bucket_replication_output()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_public_access_block(
        &self,
        req: S3Request<PutPublicAccessBlockInput>,
    ) -> S3Result<S3Response<PutPublicAccessBlockOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutPublicAccessBlockInput {
            bucket,
            public_access_block_configuration,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = serialize_config(&public_access_block_configuration)?;
        metadata_sys::update(&bucket, BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(PutPublicAccessBlockOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_bucket_tagging(
        &self,
        req: S3Request<PutBucketTaggingInput>,
    ) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutBucketTaggingInput { bucket, tagging, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = serialize_config(&tagging)?;

        metadata_sys::update(&bucket, BUCKET_TAGGING_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(tagging::build_put_bucket_tagging_output()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutBucketVersioningInput {
            bucket,
            versioning_configuration,
            ..
        } = req.input;

        let data = serialize_config(&versioning_configuration)?;

        metadata_sys::update(&bucket, BUCKET_VERSIONING_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(PutBucketVersioningOutput {}))
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

    pub async fn execute_list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let ListObjectVersionsInput {
            bucket,
            delimiter,
            key_marker,
            version_id_marker,
            max_keys,
            prefix,
            ..
        } = req.input;

        let prefix = prefix.unwrap_or_default();
        let max_keys = max_keys.unwrap_or(1000);

        let key_marker = key_marker.filter(|v| !v.is_empty());
        let version_id_marker = version_id_marker.filter(|v| !v.is_empty());
        let delimiter = delimiter.filter(|v| !v.is_empty());

        let store = get_validated_store(&bucket).await?;

        let object_infos = store
            .list_object_versions(&bucket, &prefix, key_marker, version_id_marker, delimiter.clone(), max_keys)
            .await
            .map_err(ApiError::from)?;

        let objects: Vec<ObjectVersion> = object_infos
            .objects
            .iter()
            .filter(|v| !v.name.is_empty() && !v.delete_marker)
            .map(|v| ObjectVersion {
                key: Some(v.name.to_owned()),
                last_modified: v.mod_time.map(Timestamp::from),
                size: Some(v.size),
                version_id: Some(v.version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string())),
                is_latest: Some(v.is_latest),
                e_tag: v.etag.clone().map(|etag| to_s3s_etag(&etag)),
                storage_class: v.storage_class.clone().map(ObjectVersionStorageClass::from),
                ..Default::default()
            })
            .collect();

        let common_prefixes = object_infos
            .prefixes
            .into_iter()
            .map(|v| CommonPrefix { prefix: Some(v) })
            .collect();

        let delete_markers = object_infos
            .objects
            .iter()
            .filter(|o| o.delete_marker)
            .map(|o| DeleteMarkerEntry {
                key: Some(o.name.clone()),
                version_id: Some(o.version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string())),
                is_latest: Some(o.is_latest),
                last_modified: o.mod_time.map(Timestamp::from),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let next_key_marker = object_infos.next_marker.filter(|v| !v.is_empty());
        let next_version_id_marker = object_infos.next_version_idmarker.filter(|v| !v.is_empty());

        let output = ListObjectVersionsOutput {
            is_truncated: Some(object_infos.is_truncated),
            max_keys: Some(max_keys),
            delimiter,
            name: Some(bucket),
            prefix: Some(prefix),
            common_prefixes: Some(common_prefixes),
            versions: Some(objects),
            delete_markers: Some(delete_markers),
            next_key_marker,
            next_version_id_marker,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let request_marker = req.input.marker.clone();
        let v2_resp = self.execute_list_objects_v2(req.map_input(Into::into)).await?;

        Ok(v2_resp.map_output(|v2| {
            let next_marker = if v2.is_truncated.unwrap_or(false) {
                let last_key = v2
                    .contents
                    .as_ref()
                    .and_then(|contents| contents.last())
                    .and_then(|obj| obj.key.as_ref())
                    .cloned();

                let last_prefix = v2
                    .common_prefixes
                    .as_ref()
                    .and_then(|prefixes| prefixes.last())
                    .and_then(|prefix| prefix.prefix.as_ref())
                    .cloned();

                match (last_key, last_prefix) {
                    (Some(k), Some(p)) => {
                        if k > p {
                            Some(k)
                        } else {
                            Some(p)
                        }
                    }
                    (Some(k), None) => Some(k),
                    (None, Some(p)) => Some(p),
                    (None, None) => None,
                }
            } else {
                None
            };

            let marker = Some(request_marker.unwrap_or_default());

            ListObjectsOutput {
                contents: v2.contents,
                delimiter: v2.delimiter,
                encoding_type: v2.encoding_type,
                name: v2.name,
                prefix: v2.prefix,
                max_keys: v2.max_keys,
                common_prefixes: v2.common_prefixes,
                is_truncated: v2.is_truncated,
                marker,
                next_marker,
                ..Default::default()
            }
        }))
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

    #[test]
    fn resolve_notification_region_prefers_global_region() {
        let region = resolve_notification_region(Some("us-east-1".to_string()), Some("ap-southeast-1".to_string()));
        assert_eq!(region, "us-east-1");
    }

    #[test]
    fn resolve_notification_region_falls_back_to_request_region() {
        let region = resolve_notification_region(None, Some("ap-southeast-1".to_string()));
        assert_eq!(region, "ap-southeast-1");
    }

    #[test]
    fn resolve_notification_region_defaults_to_empty() {
        let region = resolve_notification_region(None, None);
        assert!(region.is_empty());
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
    async fn execute_delete_bucket_cors_returns_internal_error_when_store_uninitialized() {
        let input = DeleteBucketCorsInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_delete_bucket_cors(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_delete_bucket_replication_returns_internal_error_when_store_uninitialized() {
        let input = DeleteBucketReplicationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_delete_bucket_replication(req).await.unwrap_err();
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
    async fn execute_get_bucket_policy_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketPolicyInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_policy(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_acl_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketAclInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_acl(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_location_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketLocationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_location(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_cors_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketCorsInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_cors(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_delete_public_access_block_returns_internal_error_when_store_uninitialized() {
        let input = DeletePublicAccessBlockInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_delete_public_access_block(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_replication_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketReplicationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_replication(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_public_access_block_returns_internal_error_when_store_uninitialized() {
        let input = GetPublicAccessBlockInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_public_access_block(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_versioning_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketVersioningInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_versioning(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_buckets_returns_internal_error_when_store_uninitialized() {
        let input = ListBucketsInput::builder().build().unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_buckets(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_object_versions_returns_internal_error_when_store_uninitialized() {
        let input = ListObjectVersionsInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_object_versions(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_objects_returns_internal_error_when_store_uninitialized() {
        let input = ListObjectsInput::builder().bucket("test-bucket".to_string()).build().unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_lifecycle_configuration_rejects_missing_configuration() {
        let input = PutBucketLifecycleConfigurationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_lifecycle_configuration(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_put_bucket_policy_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketPolicyInput::builder()
            .bucket("test-bucket".to_string())
            .policy("{}".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_policy(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_cors_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketCorsInput::builder()
            .bucket("test-bucket".to_string())
            .cors_configuration(CORSConfiguration::default())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_cors(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_replication_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketReplicationInput::builder()
            .bucket("test-bucket".to_string())
            .replication_configuration(ReplicationConfiguration {
                role: "arn:aws:iam::123456789012:role/test".to_string(),
                rules: vec![],
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_replication(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_acl_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketAclInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_acl(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_public_access_block_returns_internal_error_when_store_uninitialized() {
        let input = PutPublicAccessBlockInput::builder()
            .bucket("test-bucket".to_string())
            .public_access_block_configuration(PublicAccessBlockConfiguration::default())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_public_access_block(req).await.unwrap_err();
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
