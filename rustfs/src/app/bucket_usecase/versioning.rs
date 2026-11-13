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

//! Bucket versioning use-case operations.

use super::*;
use crate::app::storage_api::bucket_usecase::bucket::{ObjectLockConfigExt as _, VersioningConfigExt as _};
use crate::app::storage_api::s3::dto::{PutBucketVersioningInput, PutBucketVersioningOutput};
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result};

fn versioning_configuration_has_object_lock_incompatible_settings(config: &PersistedVersioningConfiguration) -> bool {
    config.suspended()
        || config.exclude_folders.unwrap_or(false)
        || config
            .excluded_prefixes
            .as_ref()
            .is_some_and(|excluded_prefixes| !excluded_prefixes.is_empty())
}

async fn validate_bucket_versioning_update(bucket: &str, config: &PersistedVersioningConfiguration) -> S3Result<()> {
    if config
        .status
        .as_ref()
        .is_some_and(|status| !matches!(status.as_str(), "Enabled" | "Suspended"))
    {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            "bucket versioning configuration has an invalid status",
        ));
    }
    match metadata_sys::get_object_lock_config(bucket).await {
        Ok((object_lock_config, _)) => {
            if object_lock_config.enabled() && versioning_configuration_has_object_lock_incompatible_settings(config) {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidBucketState,
                    "An Object Lock configuration is present on this bucket, versioning cannot be suspended.".to_string(),
                ));
            }
        }
        Err(StorageError::ConfigNotFound) => {}
        Err(err) => return Err(ApiError::from(err).into()),
    }
    // AWS S3 and MinIO both refuse to suspend versioning while a replication
    // configuration exists: suspension would start minting null versions that
    // the replication engine (versioned by contract) can never converge.
    if config.suspended() {
        match metadata_sys::get_replication_config(bucket).await {
            Ok(_) => {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidBucketState,
                    "A replication configuration is present on this bucket, bucket wide versioning cannot be suspended."
                        .to_string(),
                ));
            }
            Err(StorageError::ConfigNotFound) => {}
            Err(err) => return Err(ApiError::from(err).into()),
        }
    }

    Ok(())
}

impl DefaultBucketUsecase {
    /// The bucket's versioning status. A bucket that never configured versioning answers no status.
    #[instrument(level = "debug", skip(self, input))]
    pub async fn execute_get_bucket_versioning(
        &self,
        input: gateway_dto::GetBucketVersioningInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::GetBucketVersioningOutput> {
        let Some(store) = self.object_store() else {
            return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(input.bucket.as_str(), &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let config = BucketVersioningSys::get(input.bucket.as_str()).await.map_err(ApiError::from)?;
        Ok(gateway_dto::GetBucketVersioningOutput {
            status: config.status.map(gateway_dto::Status::custom),
            ..Default::default()
        })
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
        prepared: S3Result<PersistedVersioningConfiguration>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let PutBucketVersioningInput { bucket, .. } = req.input;

        let versioning_configuration = prepared?;
        validate_bucket_versioning_update(&bucket, &versioning_configuration).await?;

        let data = serialize_config(&versioning_configuration)?;

        update_bucket_config_for_incarnation(&bucket, BUCKET_VERSIONING_CONFIG, data, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "put bucket versioning", request_context, false).await;

        let mut item = sr_bucket_meta_item(bucket.clone(), "version-config");
        item.versioning = Some(
            serialize_config(&versioning_configuration).and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?,
        );
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket versioning hook failed");
        }

        rustfs_scanner::record_dirty_usage_bucket_from_producer(
            &bucket,
            rustfs_scanner::SegmentInvalidationProducerIdentity::BucketMetadata,
        );
        Ok(S3Response::new(PutBucketVersioningOutput {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn validate_bucket_versioning_update_rejects_invalid_status_before_metadata_lookup() {
        let config = PersistedVersioningConfiguration {
            status: Some("Invalid".to_owned()),
            ..Default::default()
        };

        let err = validate_bucket_versioning_update("unregistered-test-bucket", &config)
            .await
            .expect_err("an invalid string-backed versioning status must be rejected before persistence");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn versioning_configuration_has_object_lock_incompatible_settings_rejects_suspended() {
        let config = PersistedVersioningConfiguration {
            status: Some("Suspended".to_owned()),
            ..Default::default()
        };

        assert!(versioning_configuration_has_object_lock_incompatible_settings(&config));
    }

    #[test]
    fn versioning_configuration_has_object_lock_incompatible_settings_rejects_exclude_folders() {
        let config = PersistedVersioningConfiguration {
            exclude_folders: Some(true),
            ..Default::default()
        };

        assert!(versioning_configuration_has_object_lock_incompatible_settings(&config));
    }

    #[test]
    fn versioning_configuration_has_object_lock_incompatible_settings_rejects_excluded_prefixes() {
        let config = PersistedVersioningConfiguration {
            excluded_prefixes: Some(vec![Some("archive/".to_string())]),
            ..Default::default()
        };

        assert!(versioning_configuration_has_object_lock_incompatible_settings(&config));
    }

    #[tokio::test]
    async fn execute_get_bucket_versioning_returns_internal_error_when_store_uninitialized() {
        let input = gateway_dto::GetBucketVersioningInput {
            bucket: rustfs_gateway_types::BucketName::new("test-bucket").expect("valid bucket name"),
            ..Default::default()
        };
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_versioning(input).await.unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }
}
