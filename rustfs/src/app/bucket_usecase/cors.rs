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

//! Bucket CORS configuration use-case operations.

use super::*;
use crate::app::storage_api::s3::dto::GetBucketCorsInput;
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Result};
use rustfs_gateway_types::cors_tagging::parse_cors;
use rustfs_gateway_types::persistence::serialize_cors_dto;

impl DefaultBucketUsecase {
    /// Removes the CORS document of `input.bucket`; audited as an audit-only operation.
    ///
    /// # Errors
    ///
    /// `InternalError` when the store is not initialized or the access phase left no mutation
    /// snapshot; the storage error of the bucket lookup or of the write.
    #[instrument(level = "debug", skip(self, envelope, input))]
    pub async fn execute_delete_bucket_cors(
        &self,
        envelope: RequestEnvelope,
        input: gateway_dto::DeleteBucketCorsInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::DeleteBucketCorsOutput> {
        // The audit entry records 200, as on main; the wire answer is the operation's 204.
        RequestEnvelope::audit_only_operation(envelope, S3Operation::DeleteBucketCors, StatusCode::OK, |envelope| async move {
            let bucket = input.bucket.as_str();
            let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
            let request_context = envelope.request_context().cloned();

            let Some(store) = self.object_store() else {
                return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
            };

            store
                .get_bucket_info(bucket, &BucketOptions::default())
                .await
                .map_err(ApiError::from)?;

            delete_bucket_config_for_incarnation(bucket, BUCKET_CORS_CONFIG, expected_incarnation_id)
                .await
                .map_err(ApiError::from)?;

            notify_bucket_metadata_reload(bucket.to_owned(), "delete bucket cors", request_context, false).await;

            let item = sr_bucket_meta_item(bucket.to_owned(), "cors-config");
            if let Err(err) = site_replication_bucket_meta_hook(item).await {
                warn!(bucket = %bucket, error = ?err, "site replication bucket cors delete hook failed");
            }

            Ok(gateway_dto::DeleteBucketCorsOutput::default())
        })
        .await
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_get_bucket_cors(&self, req: S3Request<GetBucketCorsInput>) -> S3Result<PersistedCorsConfiguration> {
        let GetBucketCorsInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
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
                warn!(
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_BUCKET,
                    event = "bucket_cors_config_load_failed",
                    bucket = %bucket,
                    error = ?err,
                    "Failed to load bucket CORS configuration"
                );
                return Err(ApiError::from(err).into());
            }
        };

        Ok(cors_configuration)
    }

    /// Stores the CORS document of `input.bucket` with the persisted writer; audited as an
    /// audit-only operation.
    ///
    /// # Errors
    ///
    /// `InternalError` when the store is not initialized or the access phase left no mutation
    /// snapshot; the storage error of the bucket lookup; `MalformedXML` when the persisted reader
    /// refuses the document, after the bucket lookup as before.
    #[instrument(level = "debug", skip(self, envelope, input))]
    pub async fn execute_put_bucket_cors(
        &self,
        envelope: RequestEnvelope,
        input: gateway_dto::PutBucketCorsInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::PutBucketCorsOutput> {
        RequestEnvelope::audit_only_operation(envelope, S3Operation::PutBucketCors, StatusCode::OK, |envelope| async move {
            let bucket = input.bucket.as_str();
            let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
            let request_context = envelope.request_context().cloned();

            let Some(store) = self.object_store() else {
                return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
            };

            store
                .get_bucket_info(bucket, &BucketOptions::default())
                .await
                .map_err(ApiError::from)?;

            // The persisted reader decides what may be stored, on both edges.
            let cors_configuration = parse_cors(&serialize_cors_dto(&input.cors_configuration))
                .map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"))?;
            let data = serialize_config(&cors_configuration)?;
            update_bucket_config_for_incarnation(bucket, BUCKET_CORS_CONFIG, data.clone(), expected_incarnation_id)
                .await
                .map_err(ApiError::from)?;

            notify_bucket_metadata_reload(bucket.to_owned(), "put bucket cors", request_context, false).await;

            let mut item = sr_bucket_meta_item(bucket.to_owned(), "cors-config");
            item.cors = Some(String::from_utf8(data).map_err(to_internal_error)?);
            if let Err(err) = site_replication_bucket_meta_hook(item).await {
                warn!(bucket = %bucket, error = ?err, "site replication bucket cors hook failed");
            }

            Ok(gateway_dto::PutBucketCorsOutput::default())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::{build_request, gateway_bucket, owned_envelope};
    use http::Method;

    #[tokio::test]
    async fn execute_delete_bucket_cors_returns_internal_error_when_store_uninitialized() {
        let input = gateway_dto::DeleteBucketCorsInput {
            bucket: gateway_bucket("test-bucket"),
            ..Default::default()
        };
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_delete_bucket_cors(owned_envelope(Method::DELETE), input)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
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
    async fn execute_put_bucket_cors_returns_internal_error_when_store_uninitialized() {
        let input = gateway_dto::PutBucketCorsInput {
            bucket: gateway_bucket("test-bucket"),
            ..Default::default()
        };
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_put_bucket_cors(owned_envelope(Method::PUT), input)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }
}
