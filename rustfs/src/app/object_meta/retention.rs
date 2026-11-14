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
//! NOT responsible for: authorization, or setting a retention (PutObjectRetention).
//! Upstream: both edges. Downstream: the object store and the bucket's Object Lock configuration.

use super::{ObjectMetaUsecase, locked_object_info};
use crate::app::RequestEnvelope;
use crate::app::dto_values;
use crate::app::storage_api::object_usecase::helper::OperationHelper;
#[cfg(any(feature = "gateway-stack", test))]
use rustfs_gateway_types::dto::GetObjectRetentionInput;
use rustfs_gateway_types::dto::{GetObjectRetentionOutput, Mode, ObjectLockRetention};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{EventName, S3Error, S3ErrorCode, S3Result};
use rustfs_utils::http::{AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER};
use std::collections::HashMap;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

impl ObjectMetaUsecase {
    /// The retention of the object `input` names.
    ///
    /// # Errors
    ///
    /// `MethodNotAllowed` for a Veeam SOSAPI system object; `InternalError` before the store is up,
    /// when the object cannot be read, or for a stored date outside the answerable range; the
    /// Object Lock refusal of a bucket without Object Lock.
    #[cfg(any(feature = "gateway-stack", test))]
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
        AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, HashMap, Mode, OffsetDateTime, Rfc3339, S3ErrorCode,
        stored_retention,
    };
    use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::get_object_retention;
    use rustfs_gateway_types::dto::GetObjectRetentionOutput;

    fn stored(retain_until: &str) -> HashMap<String, String> {
        HashMap::from([
            (AMZ_OBJECT_LOCK_MODE_LOWER.to_owned(), "COMPLIANCE".to_owned()),
            (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_owned(), retain_until.to_owned()),
        ])
    }

    #[test]
    fn retention_response_serialization_preserves_offset_metadata_instant() {
        for value in [
            "2035-01-02T00:00:00+23:59",
            "2035-01-02T00:00:00-23:59",
            "2035-01-02T00:00:00Z",
        ] {
            let retention = stored_retention(&stored(value)).expect("representable retention instant");
            assert_eq!(retention.mode.as_ref().expect("mode").as_str(), "COMPLIANCE");
            let output = get_object_retention::output_to_s3s(GetObjectRetentionOutput {
                retention: Some(retention),
            })
            .expect("convert the response");
            let mut serialized = Vec::new();
            output
                .retention
                .expect("retention")
                .retain_until_date
                .expect("stored retention date")
                .format(s3s::dto::TimestampFormat::DateTime, &mut serialized)
                .expect("serialize the response timestamp");
            let serialized = String::from_utf8(serialized).expect("UTF-8 timestamp");
            let returned = OffsetDateTime::parse(&serialized, &Rfc3339).expect("parse wire timestamp");
            let stored = OffsetDateTime::parse(value, &Rfc3339).expect("parse stored timestamp");
            assert_eq!(returned.unix_timestamp(), stored.unix_timestamp(), "stored date {value}");
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
}
