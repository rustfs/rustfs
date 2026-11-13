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

//! Bucket default-encryption use-case operations.

use super::*;
use crate::app::storage_api::s3::dto::{
    DeleteBucketEncryptionInput, DeleteBucketEncryptionOutput, GetBucketEncryptionInput, PutBucketEncryptionInput,
    PutBucketEncryptionOutput, ServerSideEncryption,
};
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

/// Refuse a default-encryption configuration the write path could not honour
/// as written. `bucket_default_write_sse` falls back to AES256 for any
/// algorithm it does not know, so storing one would make GetBucketEncryption
/// advertise a scheme no object is encrypted under. s3s parses `SSEAlgorithm`
/// as an open string, so the schema check has to happen here.
fn validate_bucket_encryption_configuration(config: &PersistedBucketEncryptionConfiguration) -> S3Result<()> {
    if config.rules.is_empty() {
        return Err(S3Error::with_message(
            S3ErrorCode::MalformedXML,
            "ServerSideEncryptionConfiguration must contain at least one Rule".to_string(),
        ));
    }
    for rule in &config.rules {
        let Some(by_default) = rule.apply_server_side_encryption_by_default.as_ref() else {
            return Err(S3Error::with_message(
                S3ErrorCode::MalformedXML,
                "Rule must contain ApplyServerSideEncryptionByDefault".to_string(),
            ));
        };
        let names_kms_key = by_default.kms_master_key_id.as_deref().is_some_and(|id| !id.is_empty());
        match by_default.sse_algorithm.as_str() {
            ServerSideEncryption::AWS_KMS => {}
            ServerSideEncryption::AES256 if names_kms_key => {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidArgument,
                    "KMSMasterKeyID can only be specified when SSEAlgorithm is aws:kms".to_string(),
                ));
            }
            ServerSideEncryption::AES256 => {}
            other => {
                return Err(S3Error::with_message(
                    S3ErrorCode::MalformedXML,
                    format!("SSEAlgorithm {other} is not supported; expected AES256 or aws:kms"),
                ));
            }
        }
    }
    Ok(())
}

impl DefaultBucketUsecase {
    pub async fn execute_delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let DeleteBucketEncryptionInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        delete_bucket_config_for_incarnation(&bucket, BUCKET_SSECONFIG, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "delete bucket encryption", request_context, false).await;

        let item = sr_bucket_meta_item(bucket.clone(), "sse-config");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket encryption delete hook failed");
        }

        Ok(S3Response::with_status(DeleteBucketEncryptionOutput::default(), StatusCode::NO_CONTENT))
    }

    pub async fn execute_get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<Option<PersistedBucketEncryptionConfiguration>> {
        let GetBucketEncryptionInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let server_side_encryption_configuration = match metadata_sys::get_sse_config(&bucket).await {
            Ok((cfg, _)) => Some(cfg),
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(s3_error!(ServerSideEncryptionConfigurationNotFoundError));
                }
                warn!(
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_BUCKET,
                    event = "bucket_sse_config_load_failed",
                    bucket = %bucket,
                    error = ?err,
                    "Failed to load bucket SSE configuration"
                );
                return Err(ApiError::from(err).into());
            }
        };

        Ok(server_side_encryption_configuration)
    }

    pub async fn execute_put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
        prepared: S3Result<PersistedBucketEncryptionConfiguration>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let PutBucketEncryptionInput { bucket, .. } = req.input;

        let mut server_side_encryption_configuration = prepared?;
        validate_bucket_encryption_configuration(&server_side_encryption_configuration)?;

        // When SSE-KMS is set without a specific key ID, populate the default
        // KMS key so that GetBucketEncryption responses include it. Clients like
        // mc rely on the presence of KMSMasterKeyID to distinguish SSE-KMS from
        // SSE-S3 in their display logic.
        if let Some(rule) = server_side_encryption_configuration.rules.first_mut()
            && let Some(ref mut by_default) = rule.apply_server_side_encryption_by_default
            && by_default.sse_algorithm.as_str() == ServerSideEncryption::AWS_KMS
            && by_default.kms_master_key_id.as_deref().is_none_or(str::is_empty)
        {
            let service = current_encryption_service()
                .await
                .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "KMS service not initialized".to_string()))?;
            let default_key = service
                .get_default_key_id()
                .filter(|key| !key.is_empty())
                .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "KMS default key not configured".to_string()))?;
            by_default.kms_master_key_id = Some(default_key.clone());
        }

        info!("sse_config {:?}", &server_side_encryption_configuration);

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = serialize_config(&server_side_encryption_configuration)?;
        update_bucket_config_for_incarnation(&bucket, BUCKET_SSECONFIG, data, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "put bucket encryption", request_context, false).await;

        let mut item = sr_bucket_meta_item(bucket.clone(), "sse-config");
        item.sse_config = Some(
            serialize_config(&server_side_encryption_configuration)
                .and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?,
        );
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket encryption hook failed");
        }
        Ok(S3Response::new(PutBucketEncryptionOutput::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use crate::app::storage_api::s3::dto;
    use http::Method;
    use rustfs_gateway_types::persistence::{
        PersistedBucketEncryptionConfiguration as ServerSideEncryptionConfiguration,
        PersistedBucketEncryptionRule as ServerSideEncryptionRule, PersistedEncryptionByDefault as ServerSideEncryptionByDefault,
    };

    fn sse_config(rules: Vec<ServerSideEncryptionRule>) -> ServerSideEncryptionConfiguration {
        ServerSideEncryptionConfiguration { rules }
    }

    fn sse_rule(algorithm: &str, kms_key_id: Option<&str>) -> ServerSideEncryptionRule {
        ServerSideEncryptionRule {
            apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                sse_algorithm: algorithm.to_string(),
                kms_master_key_id: kms_key_id.map(|id| id.to_string()),
            }),
            blocked_encryption_types: None,
            bucket_key_enabled: None,
        }
    }

    /// The stored configuration must be one the write path honours as written:
    /// only AES256 and aws:kms exist, and a key id belongs to aws:kms alone.

    #[test]
    fn put_bucket_encryption_refuses_configurations_the_write_path_cannot_honour() {
        validate_bucket_encryption_configuration(&sse_config(vec![sse_rule("AES256", None)])).expect("AES256 is valid");
        validate_bucket_encryption_configuration(&sse_config(vec![sse_rule("aws:kms", Some("bucket-key"))]))
            .expect("aws:kms with a key is valid");
        validate_bucket_encryption_configuration(&sse_config(vec![sse_rule("aws:kms", None)]))
            .expect("aws:kms without a key is valid (the default key is filled in)");
        validate_bucket_encryption_configuration(&sse_config(vec![sse_rule("AES256", Some(""))]))
            .expect("an empty key id on AES256 is how some clients spell 'none'");

        let unknown = validate_bucket_encryption_configuration(&sse_config(vec![sse_rule("AES128", None)]))
            .expect_err("AES128 is not an algorithm this server encrypts with");
        assert_eq!(*unknown.code(), S3ErrorCode::MalformedXML);

        let misplaced = validate_bucket_encryption_configuration(&sse_config(vec![sse_rule("AES256", Some("bucket-key"))]))
            .expect_err("a key id only makes sense for aws:kms");
        assert_eq!(*misplaced.code(), S3ErrorCode::InvalidArgument);

        let empty = validate_bucket_encryption_configuration(&sse_config(Vec::new())).expect_err("no rule, no default");
        assert_eq!(*empty.code(), S3ErrorCode::MalformedXML);

        let bare_rule = validate_bucket_encryption_configuration(&sse_config(vec![ServerSideEncryptionRule {
            apply_server_side_encryption_by_default: None,
            blocked_encryption_types: None,
            bucket_key_enabled: None,
        }]))
        .expect_err("a rule without ApplyServerSideEncryptionByDefault configures nothing");
        assert_eq!(*bare_rule.code(), S3ErrorCode::MalformedXML);

        // A second rule is checked too, so a malformed one cannot hide behind a valid first rule.
        let second_bad =
            validate_bucket_encryption_configuration(&sse_config(vec![sse_rule("AES256", None), sse_rule("garbage", None)]))
                .expect_err("every rule is validated");
        assert_eq!(*second_bad.code(), S3ErrorCode::MalformedXML);
    }

    #[tokio::test]
    async fn execute_get_bucket_encryption_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketEncryptionInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_encryption(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_encryption_returns_internal_error_when_store_uninitialized() {
        // A well-formed rule, so the request reaches the store lookup instead
        // of being refused by configuration validation first.
        let input = PutBucketEncryptionInput::builder()
            .bucket("test-bucket".to_string())
            .server_side_encryption_configuration(dto::ServerSideEncryptionConfiguration {
                rules: vec![dto::ServerSideEncryptionRule {
                    apply_server_side_encryption_by_default: Some(dto::ServerSideEncryptionByDefault {
                        sse_algorithm: ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                        kms_master_key_id: None,
                    }),
                    blocked_encryption_types: None,
                    bucket_key_enabled: None,
                }],
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let prepared = crate::storage::persisted_from_s3s(&req.input.server_side_encryption_configuration);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_encryption(req, prepared).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }
}
