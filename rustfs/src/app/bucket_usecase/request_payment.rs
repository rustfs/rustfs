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

//! Bucket request-payment configuration reads shared by the S3 edges.

use super::{ApiError, DefaultBucketUsecase, StorageError, metadata_sys};
use rustfs_gateway_types::persistence::PersistedRequestPaymentConfiguration;
use rustfs_s3_types::S3Result;

impl DefaultBucketUsecase {
    pub(crate) async fn execute_get_bucket_request_payment(
        &self,
        bucket: &str,
    ) -> S3Result<PersistedRequestPaymentConfiguration> {
        self.ensure_config_read_bucket(bucket).await?;
        match metadata_sys::get_request_payment_config(bucket).await {
            Ok((config, _)) => Ok(config),
            Err(StorageError::ConfigNotFound) => Ok(PersistedRequestPaymentConfiguration {
                payer: "BucketOwner".to_owned(),
            }),
            Err(err) => Err(ApiError::from(err).into()),
        }
    }
}
