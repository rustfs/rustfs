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

//! GetObjectRetention: the retention an object's metadata stores.
//!
//! Responsible for: reading the stored mode and retain-until date on a bucket with Object Lock
//! enabled. A stored mode outside the model's set is answered as stored; a stored date is answered
//! at its instant in UTC, and an instant outside the years 0 to 9999 in UTC is `InternalError`.
//! Also writes lock metadata to the requested version and schedules metadata replication.
//! NOT responsible for: authorization.
//! Upstream: both edges. Downstream: the object store and the bucket's Object Lock configuration.

use super::{ObjectMetaUsecase, locked_object_info};
use crate::app::RequestEnvelope;
use crate::app::dto_values;
use crate::app::object::shared::{reject_veeam_sosapi_mutation, validate_table_catalog_object_mutation};
use crate::app::storage_api::object_meta_usecase::contract::{ObjectLockRetentionOptions, ObjectOperations as _};
use crate::app::storage_api::object_meta_usecase::{
    check_retention_for_modification, load_bucket_object_lock_config_state, validate_bucket_object_lock_enabled_state,
};
use crate::app::storage_api::object_usecase::access::{
    RequestedObjectLockRetention, apply_bucket_generation_guard, has_bypass_governance_header,
};
use crate::app::storage_api::object_usecase::bucket::replication::{
    ReplicateDecision, must_replicate_metadata, schedule_metadata_replication,
};
use crate::app::storage_api::object_usecase::helper::OperationHelper;
use crate::app::storage_api::object_usecase::options::get_opts;
use crate::app::storage_api::object_usecase::{StorageObjectOptions as ObjectOptions, parse_object_lock_retention_native};
use crate::error::ApiError;
use rustfs_gateway_types::dto::GetObjectRetentionInput;
use rustfs_gateway_types::dto::{GetObjectRetentionOutput, Mode, ObjectLockRetention};
use rustfs_gateway_types::dto::{PutObjectRetentionOutput, RequestCharged};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{EventName, S3Error, S3ErrorCode, S3Result};
use rustfs_utils::http::{AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER};
use rustfs_utils::http::{SUFFIX_REPLICATION_GENERATION, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP, insert_str};
use std::collections::HashMap;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing::error;
use uuid::Uuid;

impl ObjectMetaUsecase {
    /// Updates the lock metadata of the version named by the request.
    pub(crate) async fn put_object_retention_fields(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
        retention: Option<RequestedObjectLockRetention>,
    ) -> S3Result<PutObjectRetentionOutput> {
        let helper = OperationHelper::from_view(
            &envelope.audit_view(),
            EventName::ObjectCreatedPutRetention,
            S3Operation::PutObjectRetention,
        )
        .suppress_event();
        reject_veeam_sosapi_mutation(|| self.object_store().ok(), bucket, key).await?;
        validate_table_catalog_object_mutation(bucket, key).await?;
        let store = self.object_store()?;

        let object_lock_config_state = load_bucket_object_lock_config_state(bucket).await?;
        validate_bucket_object_lock_enabled_state(bucket, &object_lock_config_state)?;

        let new_retain_until = retention.as_ref().and_then(|retention| retention.retain_until);
        let new_mode = retention.as_ref().and_then(|retention| retention.mode.clone());

        let bypass_governance = has_bypass_governance_header(envelope.headers());
        // Keep the early check for existing response behavior; put_object_metadata
        // repeats the same check after taking the metadata write lock.
        let check_opts: ObjectOptions = get_opts(bucket, key, version_id.clone(), None, envelope.headers())
            .await
            .map_err(ApiError::from)?;

        let existing_obj_info = store.get_object_info(bucket, key, &check_opts).await.ok();

        if let Some(existing_obj_info) = existing_obj_info.as_ref()
            && let Some(block_reason) = check_retention_for_modification(
                &object_lock_config_state,
                existing_obj_info,
                new_mode.as_deref(),
                new_retain_until,
                bypass_governance,
            )
            .map_err(|_| {
                S3Error::with_message(S3ErrorCode::AccessDenied, "Object Lock state could not be verified.".to_string())
            })?
        {
            return Err(S3Error::with_message(S3ErrorCode::AccessDenied, block_reason.error_message()));
        }

        let mut opts: ObjectOptions = get_opts(bucket, key, version_id.clone(), None, envelope.headers())
            .await
            .map_err(ApiError::from)?;
        apply_bucket_generation_guard(&envelope, bucket, &mut opts)?;
        opts.object_lock_retention = Some(ObjectLockRetentionOptions {
            mode: new_mode,
            retain_until: new_retain_until,
            bypass_governance,
        });

        // PutObjectRetention only rewrites metadata, so the object PUT path that normally
        // computes a replication decision and schedules replication never runs for it. Without
        // scheduling here the peer keeps the previous, unprotected lock state and a
        // WORM-protected object stays deletable on the replica.
        //
        // Mirror the PUT path ordering (see object_usecase::put_object): compute the decision
        // once BEFORE the commit and persist the pending marker with it, so a restart between
        // the commit and the worker write-back leaves a Pending status on disk for the scanner
        // to re-drive. Scheduling without that marker would lose the task silently.
        let dsc = match existing_obj_info.as_ref() {
            Some(info) => {
                must_replicate_metadata(
                    bucket,
                    key,
                    &info.user_defined,
                    info.user_tags.as_ref().clone(),
                    opts.delete_marker_replication_status(),
                    opts.clone(),
                )
                .await
            }
            None => ReplicateDecision::new(),
        };

        let mut eval_metadata = parse_object_lock_retention_native(retention)?;
        if dsc.replicate_any() {
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_TIMESTAMP, jiff::ZonedDateTime::now().to_string());
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_STATUS, dsc.pending_status().unwrap_or_default());
        }
        opts.eval_metadata = Some(eval_metadata);

        let object_info = store.put_object_metadata(bucket, key, &opts).await.map_err(|e| {
            error!(
                event = "object_metadata_write_failed",
                component = "storage",
                subsystem = "object",
                error = %e,
                "Failed to write object metadata"
            );
            S3Error::from(ApiError::from(e))
        })?;

        // The current target transport carries the updated metadata in a full-object PUT.
        if dsc.replicate_any() {
            schedule_metadata_replication(object_info.clone(), store.clone(), dsc, opts.expected_bucket_incarnation_id).await;
        }

        let output = PutObjectRetentionOutput {
            request_charged: Some(RequestCharged::REQUESTER),
        };

        let version_id = version_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let helper = helper.object(object_info).version_id(version_id);

        let result = Ok(output);
        let _ = helper.complete_owned(&result, http::StatusCode::OK, &http::HeaderMap::new());
        rustfs_scanner::record_dirty_usage_object_from_producer(
            bucket,
            key,
            rustfs_scanner::SegmentInvalidationProducerIdentity::ObjectMetadata,
        );
        result
    }

    /// The retention of the object `input` names.
    ///
    /// # Errors
    ///
    /// `MethodNotAllowed` for a Veeam SOSAPI system object; `InternalError` before the store is up,
    /// when the object cannot be read, or for a stored date outside the answerable range; the
    /// Object Lock refusal of a bucket without Object Lock.
    pub async fn execute_get_object_retention(
        &self,
        envelope: RequestEnvelope,
        input: GetObjectRetentionInput,
    ) -> S3Result<GetObjectRetentionOutput> {
        self.get_object_retention_fields(envelope, input.bucket.as_str(), input.key.as_str(), input.version_id)
            .await
    }

    pub(crate) async fn get_object_retention_fields(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> S3Result<GetObjectRetentionOutput> {
        let mut helper = OperationHelper::from_view(
            &envelope.audit_view(),
            EventName::ObjectAccessedGetRetention,
            S3Operation::GetObjectRetention,
        )
        .suppress_event();
        let store = self.store_outside_veeam_sosapi(bucket, key).await?;
        let object_info = locked_object_info(&store, &envelope, bucket, key, version_id.clone()).await?;

        let output = GetObjectRetentionOutput {
            retention: Some(stored_retention(&object_info.user_defined)?),
        };
        helper = helper.object(object_info).version_id(version_id.unwrap_or_default());

        let result = Ok(output);
        let _ = helper.complete_owned(&result, http::StatusCode::OK, &http::HeaderMap::new());
        result
    }
}

/// The retention `metadata` stores. An unparseable date is answered as absent, as before.
fn stored_retention(metadata: &HashMap<String, String>) -> S3Result<ObjectLockRetention> {
    let mode = metadata
        .get(AMZ_OBJECT_LOCK_MODE_LOWER)
        .map(|mode| Mode::custom(mode.clone()));
    let retain_until_date = metadata
        .get(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER)
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
        .map(|date| {
            date.checked_to_offset(time::UtcOffset::UTC)
                .filter(|date| (0..=9999).contains(&date.year()))
                .map(dto_values::timestamp)
                .ok_or_else(|| {
                    S3Error::with_message(
                        S3ErrorCode::InternalError,
                        "Stored retention date is outside the supported UTC range".to_string(),
                    )
                })
        })
        .transpose()?;
    Ok(ObjectLockRetention {
        mode,
        retain_until_date,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, HashMap, Mode, OffsetDateTime, RequestEnvelope,
        Rfc3339, S3ErrorCode, stored_retention,
    };
    use rustfs_gateway::{Limits, MetaView, OperationCodec, ResponseBody, TargetKind, WireRequest, dto::GetObjectRetention};
    use rustfs_gateway_types::dto::GetObjectRetentionOutput;

    fn stored(retain_until: &str) -> HashMap<String, String> {
        HashMap::from([
            (AMZ_OBJECT_LOCK_MODE_LOWER.to_owned(), "COMPLIANCE".to_owned()),
            (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_owned(), retain_until.to_owned()),
        ])
    }

    #[test]
    fn retention_response_serialization_preserves_offset_metadata_instant() {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct Retention {
            mode: String,
            retain_until_date: String,
        }
        let request = http::Request::builder()
            .method("GET")
            .uri("/bucket/key?retention")
            .header("host", "localhost")
            .body(())
            .unwrap();
        let wire = WireRequest::accept(request, &Limits::default()).expect("retention request");
        let view = MetaView::of(&wire, TargetKind::Object)
            .expect("object view")
            .with_rustfs_response_layout();
        for value in [
            "2035-01-02T00:00:00+23:59",
            "2035-01-02T00:00:00-23:59",
            "2035-01-02T00:00:00Z",
        ] {
            let retention = stored_retention(&stored(value)).expect("representable retention instant");
            assert_eq!(retention.mode.as_ref().expect("mode").as_str(), "COMPLIANCE");
            // The response codec writes RetainUntilDate as ISO 8601.
            let serialized = retention
                .retain_until_date
                .expect("stored retention date")
                .render(rustfs_gateway_types::TimestampFormat::Iso8601)
                .expect("serialize the response timestamp");
            let returned = OffsetDateTime::parse(&serialized, &Rfc3339).expect("parse wire timestamp");
            let stored_at = OffsetDateTime::parse(value, &Rfc3339).expect("parse stored timestamp");
            assert_eq!(returned.unix_timestamp(), stored_at.unix_timestamp(), "stored date {value}");
            let response = GetObjectRetention::encode(
                GetObjectRetentionOutput {
                    retention: Some(retention),
                },
                &view,
                200,
            )
            .expect("encode the retention response");
            let ResponseBody::Complete(xml) = response.body else { panic!("expected retention XML") };
            let wire: Retention = quick_xml::de::from_reader(xml.as_slice()).expect("retention response XML");
            assert_eq!(wire.mode, "COMPLIANCE");
            let date = OffsetDateTime::parse(&wire.retain_until_date, &Rfc3339).expect("wire date");
            assert_eq!(date, stored_at, "stored date {value}");
        }
    }

    #[test]
    fn retention_response_rejects_instants_outside_the_utc_wire_range() {
        for value in ["9999-12-31T23:59:59-23:59", "0000-01-01T00:00:00+23:59"] {
            let error = stored_retention(&stored(value)).expect_err("unrepresentable UTC timestamp");
            assert_eq!(error.code(), &S3ErrorCode::InternalError);
        }
    }

    #[test]
    fn a_stored_mode_outside_the_model_and_an_unparseable_date_are_answered_as_before() {
        let metadata = HashMap::from([
            (AMZ_OBJECT_LOCK_MODE_LOWER.to_owned(), "governance".to_owned()),
            (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_owned(), "not a date".to_owned()),
        ]);

        let retention = stored_retention(&metadata).expect("a retention");

        assert_eq!(retention.mode.as_ref().map(Mode::as_str), Some("governance"));
        assert_eq!(retention.retain_until_date, None);
    }

    #[test]
    #[serial_test::serial]
    fn put_object_retention_blocks_downgrade_or_shorten_of_bucket_default_compliance() {
        crate::app::gating_test_env::run_large_stack_test("put-object-retention-default-compliance", || async {
            use crate::app::RequestHead;
            use crate::app::object_meta::ObjectMetaUsecase;
            use crate::app::storage_api::object_meta_usecase::contract::BucketOperations as _;
            use crate::app::storage_api::object_usecase::access::RequestedObjectLockRetention;
            use crate::app::storage_api::object_usecase::{StorageObjectOptions as ObjectOptions, StoragePutObjReader};
            use crate::app::storage_api::test::contract::bucket::MakeBucketOptions;
            use crate::app::storage_api::test::contract::object::ObjectIO as _;
            use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
            use std::sync::Arc;
            use time::Duration;
            use uuid::Uuid;

            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            if crate::app::runtime_sources::current_app_context().is_none() {
                crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
            }
            let bucket = format!("default-compliance-retention-{}", Uuid::new_v4().simple());
            store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        lock_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("create Object Lock bucket");

            // Upload a version BEFORE setting a default retention rule so it carries no explicit retention keys.
            let mut reader = StoragePutObjReader::from_vec(b"pre-rule ledger".to_vec());
            let put_info = store
                .put_object(
                    &bucket,
                    "old-ledger.csv",
                    &mut reader,
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("upload keyless version");
            let version_id = put_info.version_id.expect("versioned upload assigns version id").to_string();

            // Install a bucket DefaultRetention = COMPLIANCE / 30 days.
            let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
                .await
                .expect("load bucket metadata"))
            .clone();
            metadata.object_lock_config = Some(rustfs_gateway_types::persistence::PersistedObjectLockConfiguration {
                object_lock_enabled: Some("Enabled".to_owned()),
                rule: Some(rustfs_gateway_types::persistence::PersistedObjectLockRule {
                    default_retention: Some(rustfs_gateway_types::persistence::PersistedDefaultRetention {
                        mode: Some("COMPLIANCE".to_owned()),
                        days: Some(30),
                        years: None,
                    }),
                }),
            });
            crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
                .await
                .expect("publish default COMPLIANCE retention rule");

            let usecase = ObjectMetaUsecase::with_context(crate::app::runtime_sources::current_app_context());
            let build_envelope = |bypass: bool| {
                let mut headers = HeaderMap::new();
                if bypass {
                    headers.insert("x-amz-bypass-governance-retention", HeaderValue::from_static("true"));
                }
                let head = RequestHead {
                    method: Method::PUT,
                    uri: Uri::from_static("/bucket/old-ledger.csv?retention"),
                    headers,
                    credentials: None,
                    region: None,
                    service: None,
                };
                RequestEnvelope::from_parts(head, Extensions::new())
            };

            let now = OffsetDateTime::now_utc();
            // Downgrading default COMPLIANCE to GOVERNANCE must be rejected even with bypass.
            let err = usecase
                .put_object_retention_fields(
                    build_envelope(true),
                    &bucket,
                    "old-ledger.csv",
                    Some(version_id.clone()),
                    Some(RequestedObjectLockRetention {
                        mode: Some("GOVERNANCE".to_owned()),
                        retain_until: Some(now + Duration::days(60)),
                    }),
                )
                .await
                .expect_err("downgrading bucket default COMPLIANCE to GOVERNANCE must fail");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

            // Shortening default COMPLIANCE (30d -> 1d) must be rejected.
            let err = usecase
                .put_object_retention_fields(
                    build_envelope(true),
                    &bucket,
                    "old-ledger.csv",
                    Some(version_id.clone()),
                    Some(RequestedObjectLockRetention {
                        mode: Some("COMPLIANCE".to_owned()),
                        retain_until: Some(now + Duration::days(1)),
                    }),
                )
                .await
                .expect_err("shortening bucket default COMPLIANCE must fail");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

            // Extending default COMPLIANCE (30d -> 60d) must succeed.
            usecase
                .put_object_retention_fields(
                    build_envelope(false),
                    &bucket,
                    "old-ledger.csv",
                    Some(version_id),
                    Some(RequestedObjectLockRetention {
                        mode: Some("COMPLIANCE".to_owned()),
                        retain_until: Some(now + Duration::days(60)),
                    }),
                )
                .await
                .expect("extending bucket default COMPLIANCE must succeed");
        });
    }
}
