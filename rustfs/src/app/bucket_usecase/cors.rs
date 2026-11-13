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
use crate::app::storage_api::s3::dto::{
    DeleteBucketCorsInput, DeleteBucketCorsOutput, GetBucketCorsInput, PutBucketCorsInput, PutBucketCorsOutput,
};
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result};

impl DefaultBucketUsecase {
    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_bucket_cors(
        &self,
        req: S3Request<DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let DeleteBucketCorsInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        delete_bucket_config_for_incarnation(&bucket, BUCKET_CORS_CONFIG, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "delete bucket cors", request_context, false).await;

        let item = sr_bucket_meta_item(bucket.clone(), "cors-config");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket cors delete hook failed");
        }

        Ok(S3Response::new(DeleteBucketCorsOutput {}))
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

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_put_bucket_cors(
        &self,
        req: S3Request<PutBucketCorsInput>,
        prepared: S3Result<PersistedCorsConfiguration>,
    ) -> S3Result<S3Response<PutBucketCorsOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let PutBucketCorsInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let cors_configuration = prepared?;
        let data = serialize_config(&cors_configuration)?;
        update_bucket_config_for_incarnation(&bucket, BUCKET_CORS_CONFIG, data, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "put bucket cors", request_context, false).await;

        let mut item = sr_bucket_meta_item(bucket.clone(), "cors-config");
        item.cors =
            Some(serialize_config(&cors_configuration).and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?);
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket cors hook failed");
        }

        Ok(S3Response::new(PutBucketCorsOutput::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use crate::app::storage_api::s3::dto::CORSConfiguration;
    use http::Method;

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
        let input = PutBucketCorsInput::builder()
            .bucket("test-bucket".to_string())
            .cors_configuration(CORSConfiguration::default())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_put_bucket_cors(req, Ok(PersistedCorsConfiguration::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }
}
