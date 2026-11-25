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
                    && statement.principal.is_public()
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

            let cfg: BucketPolicy = serde_json::from_str(policy).map_err(|_| s3_error!(MalformedPolicy))?;

            if cfg.is_valid().is_err() {
                return Err(s3_error!(MalformedPolicy));
            }

            let replication_policy = serde_json::from_str(policy)
                .map(serde_json::Value::Object)
                .map_err(|_| s3_error!(MalformedPolicy))?;

            let is_public_policy = cfg
                .statements
                .iter()
                .any(|statement| matches!(statement.effect, Effect::Allow) && statement.principal.is_public());

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
            item.policy = Some(replication_policy);
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
    use crate::app::bucket_usecase::test_support::owned_envelope;
    use http::Method;

    #[tokio::test]
    async fn execute_delete_bucket_policy_returns_internal_error_when_store_uninitialized() {
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_delete_bucket_policy(owned_envelope(Method::DELETE), "test-bucket")
            .await
            .unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
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
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_get_bucket_policy_status(owned_envelope(Method::GET), input)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_policy_returns_internal_error_when_store_uninitialized() {
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_put_bucket_policy(owned_envelope(Method::PUT), "test-bucket", "{}")
            .await
            .unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }

    #[test]
    #[serial_test::serial]
    fn block_public_policy_rejects_anonymous_matching_principals() {
        use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient};
        use crate::app::runtime_sources::ServerContextSlot;
        use crate::app::storage_api::owned_parity::authorize_bucket_config_mutation;
        use rustfs_credentials::Credentials;
        use rustfs_policy::policy::action::{Action, S3Action};

        run_large_stack_test("bucket-policy-block-public-principals", || async {
            let context = shared_gating_ambient().await;
            let store = context.object_store();
            let bucket_name = format!("block-public-policy-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket_name, &MakeBucketOptions::default())
                .await
                .expect("create bucket");

            metadata_sys::update(
                &bucket_name,
                BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG,
                b"<PublicAccessBlockConfiguration><BlockPublicPolicy>true</BlockPublicPolicy></PublicAccessBlockConfiguration>"
                    .to_vec(),
            )
            .await
            .expect("seed public access block");

            let server = ServerContextSlot::new();
            assert!(server.install(Arc::clone(&context)));
            let usecase = DefaultBucketUsecase::with_context(Some(Arc::clone(&context)));

            let authorized_put_envelope = || async {
                let mut extensions = http::Extensions::new();
                extensions.insert(Arc::clone(&server));
                extensions.insert(ReqInfo {
                    cred: Some(Credentials {
                        access_key: "policy-test-owner".to_owned(),
                        ..Default::default()
                    }),
                    is_owner: true,
                    ..Default::default()
                });
                let mut envelope = RequestEnvelope::from_parts(
                    crate::app::RequestHead {
                        method: Method::PUT,
                        uri: format!("/{bucket_name}?policy").parse().expect("request URI"),
                        headers: http::HeaderMap::new(),
                        credentials: None,
                        region: None,
                        service: None,
                    },
                    extensions,
                );
                authorize_bucket_config_mutation(
                    &server,
                    &mut envelope,
                    &bucket_name,
                    Action::S3Action(S3Action::PutBucketPolicyAction),
                )
                .await
                .expect("owner authorization installs incarnation snapshot");
                envelope
            };

            for (label, principal_json) in [
                ("wildcard", r#"{"AWS":["*"]}"#),
                ("empty_array", r#"{"AWS":[""]}"#),
                ("empty_string", r#"{"AWS":""}"#),
                ("question_mark", r#"{"AWS":"?"}"#),
                ("leading_question_mark", r#"{"AWS":["?nobody"]}"#),
                ("mixed_named_and_empty", r#"{"AWS":["alice",""]}"#),
            ] {
                let policy = format!(
                    r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{principal_json},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::{bucket_name}/*"]}}]}}"#
                );
                let Err(err) = usecase
                    .execute_put_bucket_policy(authorized_put_envelope().await, &bucket_name, &policy)
                    .await
                else {
                    panic!("BlockPublicPolicy must reject {label} principal");
                };
                assert_eq!(
                    err.code(),
                    &rustfs_s3_types::S3ErrorCode::AccessDenied,
                    "expected AccessDenied for {label}"
                );
            }

            // A named principal Allow remains accepted under BlockPublicPolicy.
            let named_policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":["alice"]}},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::{bucket_name}/*"]}}]}}"#
            );
            usecase
                .execute_put_bucket_policy(authorized_put_envelope().await, &bucket_name, &named_policy)
                .await
                .expect("named principal policy should be accepted");

            // A Deny statement with an empty principal is not a public grant.
            let deny_policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":{{"AWS":[""]}},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::{bucket_name}/*"]}}]}}"#
            );
            usecase
                .execute_put_bucket_policy(authorized_put_envelope().await, &bucket_name, &deny_policy)
                .await
                .expect("Deny statement should not be rejected as a public grant");

            // GetBucketPolicyStatus also classifies empty and `?` Allow principals as public.
            let status_bucket = format!("policy-status-public-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&status_bucket, &MakeBucketOptions::default())
                .await
                .expect("create status bucket");
            for (label, principal_json, expected_public) in [
                ("empty_principal", r#"{"AWS":[""]}"#, true),
                ("leading_question_mark", r#"{"AWS":["?nobody"]}"#, true),
                ("named_principal", r#"{"AWS":["alice"]}"#, false),
            ] {
                let policy = format!(
                    r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{principal_json},"Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::{status_bucket}"]}}]}}"#
                );
                metadata_sys::update(&status_bucket, BUCKET_POLICY_CONFIG, policy.into_bytes())
                    .await
                    .expect("seed bucket policy");
                let output = usecase
                    .execute_get_bucket_policy_status(
                        owned_envelope(Method::GET),
                        gateway_dto::GetBucketPolicyStatusInput {
                            bucket: bucket(&status_bucket),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("get bucket policy status");
                assert_eq!(
                    output.policy_status.and_then(|status| status.is_public),
                    Some(expected_public),
                    "unexpected policy status for {label}"
                );
            }
        });
    }
}
