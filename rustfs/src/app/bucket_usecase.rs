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
use crate::storage::access::authorize_request;
use crate::storage::ecfs::{
    default_owner, is_public_grant, parse_acl_json_or_canned_bucket, serialize_acl, stored_acl_from_canned_bucket,
    stored_acl_from_grant_headers,
};
use crate::storage::helper::OperationHelper;
use crate::storage::*;
use metrics::counter;
use rustfs_ecstore::bucket::{
    lifecycle::{bucket_lifecycle_ops::validate_transition_tier, lifecycle::Lifecycle},
    metadata::{
        BUCKET_ACL_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG, BUCKET_SSECONFIG,
        BUCKET_TAGGING_CONFIG, BUCKET_VERSIONING_CONFIG,
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
use tracing::{debug, info, instrument, warn};
use urlencoding::encode;

pub type BucketUsecaseResult<T> = Result<T, ApiError>;

fn serialize_config<T: xml::Serialize>(value: &T) -> S3Result<Vec<u8>> {
    serialize(value).map_err(to_internal_error)
}

fn to_internal_error(err: impl Display) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, format!("{err}"))
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

        Ok(S3Response::new(DeleteBucketTaggingOutput {}))
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

        Ok(S3Response::new(GetBucketEncryptionOutput {
            server_side_encryption_configuration,
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

        Ok(S3Response::new(GetBucketTaggingOutput { tag_set }))
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
        Ok(S3Response::new(PutBucketEncryptionOutput::default()))
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
            && let Err(err) = input_cfg.validate(&rcfg.0).await
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

        let region = rustfs_ecstore::global::get_global_region().unwrap_or_default();
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

        Ok(S3Response::new(Default::default()))
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
