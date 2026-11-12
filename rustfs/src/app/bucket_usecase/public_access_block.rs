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

//! Bucket public access block use-case operations.

use super::*;

impl DefaultBucketUsecase {
    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_public_access_block(
        &self,
        req: S3Request<DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<DeletePublicAccessBlockOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let DeletePublicAccessBlockInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        delete_bucket_config_for_incarnation(&bucket, BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "delete public access block", request_context, false).await;

        Ok(S3Response::with_status(DeletePublicAccessBlockOutput::default(), StatusCode::NO_CONTENT))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_get_public_access_block(
        &self,
        req: S3Request<GetPublicAccessBlockInput>,
    ) -> S3Result<PersistedPublicAccessBlockConfiguration> {
        let GetPublicAccessBlockInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
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

        Ok(config)
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_put_public_access_block(
        &self,
        req: S3Request<PutPublicAccessBlockInput>,
        prepared: S3Result<PersistedPublicAccessBlockConfiguration>,
    ) -> S3Result<S3Response<PutPublicAccessBlockOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let PutPublicAccessBlockInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let public_access_block_configuration = prepared?;
        let data = serialize_config(&public_access_block_configuration)?;
        update_bucket_config_for_incarnation(&bucket, BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, data, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "put public access block", request_context, false).await;

        Ok(S3Response::new(PutPublicAccessBlockOutput::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use http::Method;

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
    async fn execute_put_public_access_block_returns_internal_error_when_store_uninitialized() {
        let input = PutPublicAccessBlockInput::builder()
            .bucket("test-bucket".to_string())
            .public_access_block_configuration(PublicAccessBlockConfiguration::default())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_put_public_access_block(req, Ok(PersistedPublicAccessBlockConfiguration::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }
}
