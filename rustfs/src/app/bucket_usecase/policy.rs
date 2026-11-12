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

//! Bucket policy and policy status use-case operations.

use super::*;
use rustfs_policy::policy::Validator;

impl DefaultBucketUsecase {
    pub async fn execute_delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        record_s3_op(S3Operation::DeleteBucketPolicy);
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let DeleteBucketPolicyInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        delete_bucket_config_for_incarnation(&bucket, BUCKET_POLICY_CONFIG, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "delete bucket policy", request_context, false).await;

        let item = sr_bucket_meta_item(bucket.clone(), "policy");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket policy delete hook failed");
        }

        Ok(S3Response::new(DeleteBucketPolicyOutput {}))
    }

    pub async fn execute_get_bucket_policy(
        &self,
        req: S3Request<GetBucketPolicyInput>,
    ) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        let GetBucketPolicyInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
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
        let GetBucketPolicyStatusInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        let client_info = req.extensions.get::<ClientInfo>();
        let conditions = get_condition_values_with_client_info(
            &req.headers,
            &rustfs_credentials::Credentials::default(),
            None,
            None,
            remote_addr,
            client_info,
        );

        let read_allowed = PolicySys::try_is_allowed(&BucketPolicyArgs {
            bucket: &bucket,
            action: Action::S3Action(S3Action::ListBucketAction),
            is_owner: false,
            account: "",
            groups: &None,
            conditions: &conditions,
            object: "",
        })
        .await
        .map_err(ApiError::from)?;

        let write_allowed = PolicySys::try_is_allowed(&BucketPolicyArgs {
            bucket: &bucket,
            action: Action::S3Action(S3Action::PutObjectAction),
            is_owner: false,
            account: "",
            groups: &None,
            conditions: &conditions,
            object: "",
        })
        .await
        .map_err(ApiError::from)?;

        let mut is_public = read_allowed || write_allowed;
        let ignore_public_acls = match metadata_sys::get_public_access_block_config(&bucket).await {
            Ok((config, _)) => config.ignore_public_acls.unwrap_or(false),
            Err(_) => false,
        };

        let _ = ignore_public_acls;

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

    pub async fn execute_put_bucket_policy(
        &self,
        req: S3Request<PutBucketPolicyInput>,
    ) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        record_s3_op(S3Operation::PutBucketPolicy);
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let PutBucketPolicyInput { bucket, policy, .. } = req.input;

        let Some(store) = self.object_store() else {
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
                        warn!(
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_BUCKET,
                            event = "bucket_public_access_block_load_failed",
                            bucket = %bucket,
                            error = ?err,
                            "Failed to load bucket public access block configuration"
                        );
                        return Err(ApiError::from(err).into());
                    }
                }
            }
        }

        let data = policy.as_bytes().to_vec();

        update_bucket_config_for_incarnation(&bucket, BUCKET_POLICY_CONFIG, data, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "put bucket policy", request_context, false).await;

        let mut item = sr_bucket_meta_item(bucket.clone(), "policy");
        item.policy = Some(serde_json::from_str(&policy).map_err(|e| s3_error!(InvalidArgument, "parse policy failed {:?}", e))?);
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket policy hook failed");
        }

        Ok(S3Response::new(PutBucketPolicyOutput {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::{build_request, s3_op_total};
    use http::Method;

    #[tokio::test]
    async fn execute_delete_bucket_policy_records_s3_operation_before_store_lookup() {
        let input = DeleteBucketPolicyInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultBucketUsecase::without_context();
        let before = s3_op_total(S3Operation::DeleteBucketPolicy);

        let err = usecase.execute_delete_bucket_policy(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(s3_op_total(S3Operation::DeleteBucketPolicy), before + 1);
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
    async fn execute_put_bucket_policy_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketPolicyInput::builder()
            .bucket("test-bucket".to_string())
            .policy("{}".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();
        let before = s3_op_total(S3Operation::PutBucketPolicy);

        let err = usecase.execute_put_bucket_policy(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(s3_op_total(S3Operation::PutBucketPolicy), before + 1);
    }
}
