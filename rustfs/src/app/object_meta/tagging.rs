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

//! GetObjectTagging and DeleteObjectTagging: the tag set an object version stores.
//!
//! Responsible for: reading the stored tags (as stored), deleting them with metadata replication,
//! and, for an object missing locally, serving the request from a replication target that has it.
//! NOT responsible for: authorization (and so not the version swap of its action), or
//! PutObjectTagging, which still runs on the legacy edge.
//! Upstream: both edges. Downstream: the object store, metadata replication and the tagging proxy.

use super::ObjectMetaUsecase;
use super::tagging_proxy::{proxy_delete_object_tagging, proxy_get_object_tagging};
use crate::app::RequestEnvelope;
use crate::app::object::shared::{reject_veeam_sosapi_mutation, s3_response_version_id, validate_table_catalog_object_mutation};
use crate::app::storage_api::object_meta_usecase::contract::ObjectOperations as _;
use crate::app::storage_api::object_meta_usecase::{VersioningApi as _, decode_stored_tags, parse_object_version_id};
use crate::app::storage_api::object_usecase::ECStore;
use crate::app::storage_api::object_usecase::StorageObjectOptions as ObjectOptions;
use crate::app::storage_api::object_usecase::bucket::replication::{must_replicate_metadata, schedule_metadata_replication};
use crate::app::storage_api::object_usecase::bucket::versioning_sys::BucketVersioningSys;
use crate::app::storage_api::object_usecase::error::{is_err_object_not_found, is_err_version_not_found};
use crate::app::storage_api::object_usecase::helper::OperationHelper;
use crate::app::storage_api::object_usecase::options::get_opts;
use crate::error::ApiError;
use http::{HeaderMap, StatusCode};
use metrics::{counter, histogram};
#[cfg(any(feature = "gateway-stack", test))]
use rustfs_gateway_types::dto::{DeleteObjectTaggingInput, GetObjectTaggingInput};
use rustfs_gateway_types::dto::{DeleteObjectTaggingOutput, GetObjectTaggingOutput, Tag};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{EventName, S3Error, S3ErrorCode, S3Result};
use rustfs_utils::http::{
    SUFFIX_REPLICATION_GENERATION, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP, SUFFIX_TAGGING_TIMESTAMP, insert_str,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tracing::{debug, error};
use uuid::Uuid;

const LOG_COMPONENT_STORAGE: &str = "storage";
const LOG_SUBSYSTEM_TAGGING: &str = "tagging";

impl ObjectMetaUsecase {
    /// The tag set of the object version `input` names. An object missing locally is read from a
    /// replication target that has it, unless a tag-conditioned authorization ran.
    ///
    /// # Errors
    ///
    /// `MethodNotAllowed` for a Veeam SOSAPI system object or a named delete marker; `NoSuchKey` for
    /// a missing object or a current delete marker; `NoSuchVersion` for a missing named version;
    /// `InvalidArgument` for a malformed version id; `InternalError` before the store is up.
    #[cfg(any(feature = "gateway-stack", test))]
    pub async fn execute_get_object_tagging(
        &self,
        envelope: RequestEnvelope,
        input: GetObjectTaggingInput,
    ) -> S3Result<GetObjectTaggingOutput> {
        self.get_object_tagging_fields(envelope, input.bucket.as_str(), input.key.as_str(), input.version_id)
            .await
    }

    pub(crate) async fn get_object_tagging_fields(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
    ) -> S3Result<GetObjectTaggingOutput> {
        let start_time = Instant::now();
        reject_veeam_sosapi_mutation(|| self.object_store().ok(), bucket, object).await?;
        let store = self.tagging_store("get", bucket, object)?;

        let parsed_version_id = parse_object_version_id(version_id.clone())?.map(Into::into);
        let versioning = BucketVersioningSys::get(bucket).await.map_err(ApiError::from)?;
        let opts = ObjectOptions {
            version_id: parsed_version_id,
            versioned: versioning.prefix_enabled(object),
            version_suspended: versioning.prefix_suspended(object),
            ..Default::default()
        };

        // Tags and response identity must come from the same locked metadata snapshot.
        let info = match store.get_object_info(bucket, object, &opts).await {
            Ok(info) if info.delete_marker => {
                return Err(S3Error::new(if opts.version_id.is_some() {
                    S3ErrorCode::MethodNotAllowed
                } else {
                    S3ErrorCode::NoSuchKey
                }));
            }
            Ok(info) => info,
            Err(e) => {
                if (is_err_object_not_found(&e) || is_err_version_not_found(&e))
                    && proxy_allowed(&envelope)
                    && let Some(output) = proxy_get_object_tagging(bucket, object, version_id.clone(), envelope.headers()).await
                {
                    counter!("rustfs_get_object_tagging_success").increment(1);
                    record_tagging_duration("get", start_time);
                    return Ok(output);
                }
                if is_err_object_not_found(&e) {
                    debug!(
                        component = LOG_COMPONENT_STORAGE,
                        subsystem = LOG_SUBSYSTEM_TAGGING,
                        event = "object_tagging_not_found",
                        bucket = %bucket,
                        object = %object,
                        error = %e,
                        "Object tags not found"
                    );
                    return Err(S3Error::new(if opts.version_id.is_some() {
                        S3ErrorCode::NoSuchVersion
                    } else {
                        S3ErrorCode::NoSuchKey
                    }));
                }
                error!(
                    component = LOG_COMPONENT_STORAGE,
                    subsystem = LOG_SUBSYSTEM_TAGGING,
                    event = "object_tagging_get_failed",
                    bucket = %bucket,
                    object = %object,
                    error = %e,
                    "Failed to load object tags"
                );
                return Err(ApiError::from(e).into());
            }
        };

        let tag_set = stored_tags(&info.user_tags);
        debug!(
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_TAGGING,
            event = "object_tagging_decoded",
            bucket = %bucket,
            object = %object,
            tag_count = tag_set.len(),
            "Decoded object tags"
        );

        counter!("rustfs_get_object_tagging_success").increment(1);
        record_tagging_duration("get", start_time);
        Ok(GetObjectTaggingOutput {
            tag_set,
            version_id: s3_response_version_id(info.version_id),
        })
    }

    /// Deletes the tag set of the object version `input` names, replicating the change where the
    /// bucket's rules ask for it. An object missing locally is deleted on a replication target that
    /// has it, without a local event.
    ///
    /// # Errors
    ///
    /// `MethodNotAllowed` for a Veeam SOSAPI system object; `InvalidRequest` for a reserved table
    /// catalog object; `InternalError` before the store is up; the store's refusal otherwise.
    #[cfg(any(feature = "gateway-stack", test))]
    pub async fn execute_delete_object_tagging(
        &self,
        envelope: RequestEnvelope,
        input: DeleteObjectTaggingInput,
    ) -> S3Result<DeleteObjectTaggingOutput> {
        self.delete_object_tagging_fields(envelope, input.bucket.as_str(), input.key.as_str(), input.version_id)
            .await
    }

    pub(crate) async fn delete_object_tagging_fields(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        object: &str,
        version_id: Option<String>,
    ) -> S3Result<DeleteObjectTaggingOutput> {
        let start_time = Instant::now();
        let mut helper =
            OperationHelper::from_view(&envelope.audit_view(), EventName::ObjectTaggingDelete, S3Operation::DeleteObjectTagging);
        // Owned, so the reserved-prefix call below reads exactly as the text guard of
        // `table_catalog::tests` counts it on every edge.
        let (bucket, object) = (bucket.to_owned(), object.to_owned());

        reject_veeam_sosapi_mutation(|| self.object_store().ok(), &bucket, &object).await?;
        validate_table_catalog_object_mutation(&bucket, &object).await?;
        let (bucket, object) = (bucket.as_str(), object.as_str());
        let store = self.tagging_store("delete", bucket, object)?;

        let mut opts = get_opts(bucket, object, version_id.clone(), None, envelope.headers())
            .await
            .map_err(ApiError::from)?;
        let existing_object_info = match store.get_object_info(bucket, object, &opts).await {
            Ok(info) => info,
            Err(e) => {
                // No local object exists, so no bucket notification event is emitted for the
                // proxied write.
                if (is_err_object_not_found(&e) || is_err_version_not_found(&e))
                    && proxy_allowed(&envelope)
                    && proxy_delete_object_tagging(bucket, object, version_id.clone(), envelope.headers())
                        .await
                        .is_some()
                {
                    counter!("rustfs_delete_object_tagging_success").increment(1);
                    record_tagging_duration("delete", start_time);
                    return Ok(DeleteObjectTaggingOutput { version_id });
                }
                return Err(ApiError::from(e).into());
            }
        };
        let dsc = must_replicate_metadata(
            bucket,
            object,
            &existing_object_info.user_defined,
            String::new(),
            existing_object_info.replication_status.clone(),
            opts.clone(),
        )
        .await;
        if dsc.replicate_any() {
            let mut eval_metadata = HashMap::new();
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_STATUS, dsc.pending_status().unwrap_or_default());
            insert_str(
                &mut eval_metadata,
                SUFFIX_TAGGING_TIMESTAMP,
                OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_default(),
            );
            opts.eval_metadata = Some(eval_metadata);
        }

        let object_info = store.delete_object_tags(bucket, object, &opts).await.map_err(|e| {
            error!(
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_TAGGING,
                event = "object_tagging_delete_failed",
                bucket = %bucket,
                object = %object,
                error = %e,
                "Failed to delete object tags"
            );
            ApiError::from(e)
        })?;

        let event_version_id = version_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| object_info.version_id.map(|version_id| version_id.to_string()))
            .unwrap_or_default();
        if dsc.replicate_any() {
            schedule_metadata_replication(object_info.clone(), Arc::clone(&store), dsc).await;
        }
        counter!("rustfs_delete_object_tagging_success").increment(1);
        helper = helper.object(object_info).version_id(event_version_id);

        let result = Ok(DeleteObjectTaggingOutput { version_id });
        let _ = helper.complete_owned(&result, StatusCode::OK, &HeaderMap::new());
        rustfs_scanner::record_dirty_usage_object_from_producer(
            bucket,
            object,
            rustfs_scanner::SegmentInvalidationProducerIdentity::ObjectMetadata,
        );
        record_tagging_duration("delete", start_time);
        result
    }

    /// The store for a tagging request on `object`, logging when the server has none yet.
    fn tagging_store(&self, operation: &'static str, bucket: &str, object: &str) -> S3Result<Arc<ECStore>> {
        self.object_store().inspect_err(|_| {
            error!(
                    component = LOG_COMPONENT_STORAGE,
                    subsystem = LOG_SUBSYSTEM_TAGGING,
                    event = "object_tagging_store_uninitialized",
                    operation,
                    bucket = %bucket,
                    object = %object,
                "Object tagging operation failed because storage is not initialized"
            );
        })
    }
}

/// A proxy may answer only when no tag-conditioned authorization ran: the remote's tags were never
/// shown to the policy that admitted the request.
fn proxy_allowed(envelope: &RequestEnvelope) -> bool {
    envelope.object_tag_conditions().is_none()
}

fn record_tagging_duration(operation: &'static str, start_time: Instant) {
    histogram!("rustfs_object_tagging_operation_duration_seconds", "operation" => operation)
        .record(start_time.elapsed().as_secs_f64());
}

/// The tags `encoded` stores, each key and value as stored.
fn stored_tags(encoded: &str) -> Vec<Tag> {
    decode_stored_tags(encoded)
        .into_iter()
        .map(|tag| Tag {
            key: tag.key,
            value: tag.value,
        })
        .collect()
}
