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

//! Bucket accelerate configuration shared by the S3 edges.

use super::BUCKET_ACCELERATE_CONFIG;
use super::{
    ApiError, DefaultBucketUsecase, S3Operation, StatusCode, StorageError, bucket_config_mutation_incarnation, metadata_sys,
    serialize, update_bucket_config_for_incarnation,
};
use crate::app::RequestEnvelope;
use rustfs_gateway_types::persistence::PersistedAccelerateConfiguration;
use rustfs_s3_types::S3Result;

impl DefaultBucketUsecase {
    pub(crate) async fn execute_get_bucket_accelerate_configuration(
        &self,
        bucket: &str,
    ) -> S3Result<PersistedAccelerateConfiguration> {
        self.ensure_config_bucket(bucket).await?;
        match metadata_sys::get_accelerate_config(bucket).await {
            Ok((config, _)) => Ok(config),
            Err(StorageError::ConfigNotFound) => Ok(PersistedAccelerateConfiguration::default()),
            Err(err) => Err(ApiError::from(err).into()),
        }
    }

    pub(crate) async fn execute_put_bucket_accelerate_configuration(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        configuration: impl FnOnce() -> S3Result<PersistedAccelerateConfiguration> + Send,
    ) -> S3Result<()> {
        RequestEnvelope::audit_only_operation(
            envelope,
            S3Operation::PutBucketAccelerateConfiguration,
            StatusCode::OK,
            |envelope| async move {
                let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
                self.ensure_config_bucket(bucket).await?;

                let config = configuration()?;
                let data = serialize(&config).map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"))?;
                update_bucket_config_for_incarnation(bucket, BUCKET_ACCELERATE_CONFIG, data, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;
                Ok(())
            },
        )
        .await
    }
}
