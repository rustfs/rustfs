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

//! Bucket Object Lock configuration use-case operations.
//!
//! NOT responsible for: the lock validators, which object writes share (`storage::ecfs_extend`).

use super::*;
use rustfs_gateway_types::persistence::parse_object_lock_dto;

const LOG_SUBSYSTEM_OBJECT_LOCK: &str = "object_lock";

impl DefaultBucketUsecase {
    /// The stored Object Lock configuration as the gateway's answer holds it.
    ///
    /// # Errors
    ///
    /// `InternalError` when the store is not initialized, the stored configuration cannot be read
    /// or cannot be represented; the lookup's storage error, `NoSuchBucket` among them;
    /// `ObjectLockConfigurationNotFoundError` when none is stored.
    #[instrument(level = "debug", skip(self, input))]
    pub async fn execute_get_object_lock_configuration(
        &self,
        input: gateway_dto::GetObjectLockConfigurationInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::GetObjectLockConfigurationOutput> {
        let bucket = input.bucket.as_str();
        let Some(store) = self.object_store() else {
            return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let config = match metadata_sys::get_object_lock_config(bucket).await {
            Ok((cfg, _created)) => cfg,
            Err(StorageError::ConfigNotFound) => {
                return Err(rustfs_s3_types::s3_error!(
                    ObjectLockConfigurationNotFoundError,
                    "Object Lock configuration does not exist for this bucket"
                ));
            }
            Err(err) => {
                warn!(
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_OBJECT_LOCK,
                    event = "object_lock_config_load_failed",
                    bucket = %bucket,
                    error = ?err,
                    "Failed to load bucket object lock configuration"
                );
                return Err(rustfs_s3_types::s3_error!(InternalError, "Failed to load Object Lock configuration"));
            }
        };

        Ok(gateway_dto::GetObjectLockConfigurationOutput {
            object_lock_configuration: Some(dto_values::stored_config(&config, parse_object_lock_dto)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execute_get_object_lock_configuration_returns_internal_error_when_store_uninitialized() {
        let input = gateway_dto::GetObjectLockConfigurationInput {
            bucket: rustfs_gateway_types::BucketName::new("test-bucket").expect("valid bucket name"),
            ..Default::default()
        };
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_object_lock_configuration(input).await.unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }
}
