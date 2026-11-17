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
use rustfs_s3_types::s3_error;

impl DefaultBucketUsecase {
    pub(crate) async fn execute_delete_bucket_policy(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
    ) -> rustfs_s3_types::S3Result<()> {
        RequestEnvelope::audit_only_operation(envelope, S3Operation::DeleteBucketPolicy, StatusCode::OK, |envelope| async move {
            let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
            let request_context = envelope.request_context().cloned();

            let Some(store) = self.object_store() else {
                return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
            };

            store
                .get_bucket_info(bucket, &BucketOptions::default())
                .await
                .map_err(ApiError::from)?;

            delete_bucket_config_for_incarnation(bucket, BUCKET_POLICY_CONFIG, expected_incarnation_id)
                .await
                .map_err(ApiError::from)?;

            notify_bucket_metadata_reload(bucket.to_owned(), "delete bucket policy", request_context, false).await;

            let item = sr_bucket_meta_item(bucket.to_owned(), "policy");
            if let Err(err) = site_replication_bucket_meta_hook(item).await {
                warn!(bucket = %bucket, error = ?err, "site replication bucket policy delete hook failed");
            }

            Ok(())
        })
        .await
    }

    /// The stored policy document, verbatim.
    pub async fn execute_get_bucket_policy(
        &self,
        input: gateway_dto::GetBucketPolicyInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::GetBucketPolicyOutput> {
        let Some(store) = self.object_store() else {
            return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(input.bucket.as_str(), &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let (policy, _) = match metadata_sys::get_bucket_policy_raw(input.bucket.as_str()).await {
            Ok(stored) => stored,
            Err(StorageError::ConfigNotFound) => return Err(rustfs_s3_types::s3_error!(NoSuchBucketPolicy)),
            Err(err) => {
                return Err(rustfs_s3_types::S3Error::with_message(
                    rustfs_s3_types::S3ErrorCode::InternalError,
                    err.to_string(),
                ));
            }
        };

        Ok(gateway_dto::GetBucketPolicyOutput { policy: Some(policy) })
    }

    pub async fn execute_get_bucket_policy_status(
        &self,
        envelope: RequestEnvelope,
        input: gateway_dto::GetBucketPolicyStatusInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::GetBucketPolicyStatusOutput> {
        let bucket = input.bucket.as_str();

        let Some(store) = self.object_store() else {
            return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let conditions = get_condition_values_with_client_info(
            envelope.headers(),
            &rustfs_credentials::Credentials::default(),
            None,
            None,
            envelope.remote_addr(),
            envelope.client_info(),
        );

        let read_allowed = PolicySys::try_is_allowed(&BucketPolicyArgs {
            bucket,
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
            bucket,
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
        let ignore_public_acls = match metadata_sys::get_public_access_block_config(bucket).await {
            Ok((config, _)) => config.ignore_public_acls.unwrap_or(false),
            Err(_) => false,
        };

        let _ = ignore_public_acls;

        let policy_public = match metadata_sys::get_bucket_policy(bucket).await {
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

        Ok(gateway_dto::GetBucketPolicyStatusOutput {
            policy_status: Some(gateway_dto::PolicyStatus {
                is_public: Some(is_public),
            }),
        })
    }

    pub(crate) async fn execute_put_bucket_policy(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        policy: &str,
    ) -> rustfs_s3_types::S3Result<()> {
        RequestEnvelope::audit_only_operation(envelope, S3Operation::PutBucketPolicy, StatusCode::OK, |envelope| async move {
            let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
            let request_context = envelope.request_context().cloned();

            let Some(store) = self.object_store() else {
                return Err(s3_error!(InternalError, "Not init"));
            };

            store
                .get_bucket_info(bucket, &BucketOptions::default())
                .await
                .map_err(ApiError::from)?;

            let cfg: BucketPolicy =
                serde_json::from_str(policy).map_err(|e| s3_error!(InvalidArgument, "parse policy failed {:?}", e))?;

            if cfg.is_valid().is_err() {
                return Err(s3_error!(MalformedPolicy));
            }

            let is_public_policy = cfg
                .statements
                .iter()
                .any(|statement| matches!(statement.effect, Effect::Allow) && statement.principal.is_match("*"));

            if is_public_policy {
                match metadata_sys::get_public_access_block_config(bucket).await {
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

            update_bucket_config_for_incarnation(bucket, BUCKET_POLICY_CONFIG, data, expected_incarnation_id)
                .await
                .map_err(ApiError::from)?;

            notify_bucket_metadata_reload(bucket.to_owned(), "put bucket policy", request_context, false).await;

            let mut item = sr_bucket_meta_item(bucket.to_owned(), "policy");
            item.policy =
                Some(serde_json::from_str(policy).map_err(|e| s3_error!(InvalidArgument, "parse policy failed {:?}", e))?);
            if let Err(err) = site_replication_bucket_meta_hook(item).await {
                warn!(bucket = %bucket, error = ?err, "site replication bucket policy hook failed");
            }

            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::{build_request, s3_op_total};
    use crate::app::storage_api::s3::S3ErrorCode;
    use crate::app::storage_api::s3::dto::{DeleteBucketPolicyInput, PutBucketPolicyInput};
    use http::Method;

    #[tokio::test]
    async fn execute_delete_bucket_policy_records_s3_operation_before_store_lookup() {
        let input = DeleteBucketPolicyInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let fs = crate::storage::ecfs::FS::new();
        let before = s3_op_total(S3Operation::DeleteBucketPolicy);

        let err = crate::app::storage_api::s3::S3::delete_bucket_policy(&fs, req)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(s3_op_total(S3Operation::DeleteBucketPolicy), before + 1);
    }

    fn bucket(name: &str) -> rustfs_gateway_types::BucketName {
        rustfs_gateway_types::BucketName::new(name).expect("valid bucket name")
    }

    #[tokio::test]
    async fn execute_get_bucket_policy_returns_internal_error_when_store_uninitialized() {
        let input = gateway_dto::GetBucketPolicyInput {
            bucket: bucket("test-bucket"),
            ..Default::default()
        };
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_policy(input).await.unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_policy_status_returns_internal_error_when_store_uninitialized() {
        let input = gateway_dto::GetBucketPolicyStatusInput {
            bucket: bucket("test-bucket"),
            ..Default::default()
        };
        let req = build_request((), Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_get_bucket_policy_status(RequestEnvelope::from_s3s(&req), input)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_policy_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketPolicyInput::builder()
            .bucket("test-bucket".to_string())
            .policy("{}".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let fs = crate::storage::ecfs::FS::new();
        let before = s3_op_total(S3Operation::PutBucketPolicy);

        let err = crate::app::storage_api::s3::S3::put_bucket_policy(&fs, req)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(s3_op_total(S3Operation::PutBucketPolicy), before + 1);
    }
}
