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
//! Also writes lock metadata to the requested version and schedules metadata replication.
//! NOT responsible for: authorization.
//! Upstream: both edges. Downstream: the object store and the bucket's Object Lock configuration.

use super::{ObjectMetaUsecase, locked_object_info};
use crate::app::RequestEnvelope;
use crate::app::object::shared::{reject_veeam_sosapi_mutation, validate_table_catalog_object_mutation};
use crate::app::storage_api::object_meta_usecase::contract::ObjectOperations as _;
use crate::app::storage_api::object_meta_usecase::contract::{BucketOperations as _, BucketOptions};
use crate::app::storage_api::object_meta_usecase::validate_bucket_object_lock_enabled;
use crate::app::storage_api::object_usecase::access::{RequestedObjectLegalHold, apply_bucket_generation_guard};
use crate::app::storage_api::object_usecase::bucket::replication::{
    ReplicateDecision, must_replicate_metadata, schedule_metadata_replication,
};
use crate::app::storage_api::object_usecase::helper::OperationHelper;
use crate::app::storage_api::object_usecase::options::get_opts;
use crate::app::storage_api::object_usecase::{StorageObjectOptions as ObjectOptions, parse_object_lock_legal_hold_native};
use crate::error::ApiError;
#[cfg(any(feature = "gateway-stack", test))]
use rustfs_gateway_types::dto::GetObjectLegalHoldInput;
use rustfs_gateway_types::dto::{GetObjectLegalHoldOutput, ObjectLockLegalHold, Status};
use rustfs_gateway_types::dto::{PutObjectLegalHoldOutput, RequestCharged};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{EventName, S3Error, S3ErrorCode, S3Result};
use rustfs_utils::http::headers::AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER;
use rustfs_utils::http::{SUFFIX_REPLICATION_GENERATION, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP, insert_str};
use tracing::error;
use uuid::Uuid;

impl ObjectMetaUsecase {
    /// Updates the lock metadata of the version named by the request.
    pub(crate) async fn put_object_legal_hold_fields(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
        legal_hold: Option<RequestedObjectLegalHold>,
    ) -> S3Result<PutObjectLegalHoldOutput> {
        let helper = OperationHelper::from_view(
            &envelope.audit_view(),
            EventName::ObjectCreatedPutLegalHold,
            S3Operation::PutObjectLegalHold,
        )
        .suppress_event();
        reject_veeam_sosapi_mutation(|| self.object_store().ok(), bucket, key).await?;
        validate_table_catalog_object_mutation(bucket, key).await?;
        let store = self.object_store()?;

        if let Err(err) = validate_bucket_object_lock_enabled(bucket).await {
            let _ = store
                .get_bucket_info(bucket, &BucketOptions::default())
                .await
                .map_err(ApiError::from)?;
            return Err(err.into());
        }

        let opts: ObjectOptions = get_opts(bucket, key, version_id.clone(), None, envelope.headers())
            .await
            .map_err(ApiError::from)?;

        let mut popts = ObjectOptions {
            mod_time: opts.mod_time,
            version_id: opts.version_id.clone(),
            ..Default::default()
        };
        apply_bucket_generation_guard(&envelope, bucket, &mut popts)?;

        // PutObjectLegalHold only rewrites metadata, so replication is not scheduled by the
        // object PUT path. Schedule it explicitly, otherwise the legal hold never reaches the
        // replica and the peer copy remains deletable.
        //
        // Mirror the PUT path ordering (see object_usecase::put_object): compute the decision
        // once BEFORE the commit and persist the pending marker with it, so a restart between
        // the commit and the worker write-back leaves a Pending status on disk for the scanner
        // to re-drive. Scheduling without that marker would lose the task silently.
        let dsc = if crate::app::storage_api::object_usecase::bucket::metadata_sys::get_replication_config(bucket)
            .await
            .is_ok()
            && let Ok(info) = store.get_object_info(bucket, key, &opts).await
        {
            must_replicate_metadata(
                bucket,
                key,
                &info.user_defined,
                info.user_tags.as_ref().clone(),
                popts.delete_marker_replication_status(),
                popts.clone(),
            )
            .await
        } else {
            ReplicateDecision::new()
        };

        let mut eval_metadata = parse_object_lock_legal_hold_native(legal_hold)?;
        if dsc.replicate_any() {
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_STATUS, dsc.pending_status().unwrap_or_default());
        }
        popts.eval_metadata = Some(eval_metadata);

        let info = store.put_object_metadata(bucket, key, &popts).await.map_err(|e| {
            error!(
                event = "object_metadata_write_failed",
                component = "storage",
                subsystem = "object",
                error = %e,
                "Failed to write object metadata"
            );
            S3Error::with_message(S3ErrorCode::InternalError, e.to_string())
        })?;

        // The current target transport carries the updated metadata in a full-object PUT.
        if dsc.replicate_any() {
            schedule_metadata_replication(info.clone(), store.clone(), dsc).await;
        }

        let output = PutObjectLegalHoldOutput {
            request_charged: Some(RequestCharged::REQUESTER),
        };
        let version_id = version_id.unwrap_or_default();
        let helper = helper.object(info).version_id(version_id);

        let result = Ok(output);
        let _ = helper.complete_owned(&result, http::StatusCode::OK, &http::HeaderMap::new());
        rustfs_scanner::record_dirty_usage_object_from_producer(
            bucket,
            key,
            rustfs_scanner::SegmentInvalidationProducerIdentity::ObjectMetadata,
        );
        result
    }

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
