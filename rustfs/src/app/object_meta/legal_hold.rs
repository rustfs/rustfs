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

//! GetObjectLegalHold: the legal hold status an object's metadata stores.
//!
//! Responsible for: reading the stored status, `OFF` when none is stored, on a bucket with Object
//! Lock enabled. A stored status outside the model's set is answered as stored.
//! NOT responsible for: authorization, or setting a hold (PutObjectLegalHold).
//! Upstream: both edges. Downstream: the object store and the bucket's Object Lock configuration.

use super::{ObjectMetaUsecase, locked_object_info};
use crate::app::RequestEnvelope;
use crate::app::storage_api::object_meta_usecase::contract::{BucketOperations as _, BucketOptions};
use crate::app::storage_api::object_usecase::helper::OperationHelper;
use crate::error::ApiError;
#[cfg(any(feature = "gateway-stack", test))]
use rustfs_gateway_types::dto::GetObjectLegalHoldInput;
use rustfs_gateway_types::dto::{GetObjectLegalHoldOutput, ObjectLockLegalHold, Status};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{EventName, S3Result};
use rustfs_utils::http::headers::AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER;
use uuid::Uuid;

impl ObjectMetaUsecase {
    /// The legal hold of the object `input` names.
    ///
    /// # Errors
    ///
    /// `MethodNotAllowed` for a Veeam SOSAPI system object; `InternalError` before the store is up
    /// or when the object cannot be read; `NoSuchBucket`; the Object Lock refusal of a bucket
    /// without Object Lock.
    #[cfg(any(feature = "gateway-stack", test))]
    pub async fn execute_get_object_legal_hold(
        &self,
        envelope: RequestEnvelope,
        input: GetObjectLegalHoldInput,
    ) -> S3Result<GetObjectLegalHoldOutput> {
        self.get_object_legal_hold_fields(envelope, input.bucket.as_str(), input.key.as_str(), input.version_id)
            .await
    }

    pub(crate) async fn get_object_legal_hold_fields(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> S3Result<GetObjectLegalHoldOutput> {
        let mut helper = OperationHelper::from_view(
            &envelope.audit_view(),
            EventName::ObjectAccessedGetLegalHold,
            S3Operation::GetObjectLegalHold,
        )
        .suppress_event();
        let store = self.store_outside_veeam_sosapi(bucket, key).await?;
        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;
        let object_info = locked_object_info(&store, &envelope, bucket, key, version_id.clone()).await?;

        let status = object_info
            .user_defined
            .get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER)
            .map_or(Status::OFF, |status| Status::custom(status.clone()));
        let output = GetObjectLegalHoldOutput {
            legal_hold: Some(ObjectLockLegalHold { status: Some(status) }),
        };

        let version_id = version_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(output);
        let _ = helper.complete_owned(&result, http::StatusCode::OK, &http::HeaderMap::new());
        result
    }
}
