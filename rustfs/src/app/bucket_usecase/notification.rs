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

//! Bucket notification configuration use-case operations.

use super::*;
use rustfs_gateway_types::persistence::parse_notification_dto;
#[cfg(test)]
use rustfs_s3_types::S3ErrorCode;
use rustfs_s3_types::{S3Result, s3_error};

/// Runtime rules input retained alongside the prepared persisted configuration.
pub(crate) struct NotificationDestination {
    pub(crate) kind: &'static str,
    pub(crate) id: Option<String>,
    pub(crate) events: Vec<String>,
    pub(crate) target_arn: String,
    pub(crate) filters: Vec<(Option<String>, Option<String>)>,
}

fn is_valid_notification_filter_value(value: &str) -> bool {
    if value.len() > 1024 || value.contains('\\') {
        return false;
    }
    !value.split('/').any(|segment| segment == "." || segment == "..")
}

fn invalid_filter_value_message(cfg_scope: &str, value: &str) -> String {
    format!("invalid notification filter value (len={}) ({cfg_scope})", value.len())
}

fn invalid_filter_name_message(cfg_scope: &str, name: &str) -> String {
    format!(
        "invalid notification filter name (len={}) (only 'prefix'/'suffix' are supported) ({cfg_scope})",
        name.len()
    )
}

fn validate_notification_filter_rules(destination: &NotificationDestination) -> S3Result<()> {
    if destination.filters.is_empty() {
        return Ok(());
    }

    let mut has_prefix = false;
    let mut has_suffix = false;
    let kind = destination.kind;
    let cfg_scope = destination
        .id
        .as_ref()
        .map_or_else(|| kind.to_owned(), |id| format!("{kind} id={id}"));

    for (name, value) in &destination.filters {
        let Some(name) = name.as_deref() else {
            return Err(s3_error!(InvalidArgument, "invalid notification filter rule: missing Name ({cfg_scope})"));
        };
        let Some(value) = value.as_deref() else {
            return Err(s3_error!(
                InvalidArgument,
                "invalid notification filter rule: missing Value ({cfg_scope})"
            ));
        };

        if !is_valid_notification_filter_value(value) {
            return Err(s3_error!(InvalidArgument, "{}", invalid_filter_value_message(&cfg_scope, value)));
        }

        if name.eq_ignore_ascii_case("prefix") {
            if has_prefix {
                return Err(s3_error!(InvalidArgument, "duplicate notification filter name 'prefix' ({cfg_scope})"));
            }
            has_prefix = true;
        } else if name.eq_ignore_ascii_case("suffix") {
            if has_suffix {
                return Err(s3_error!(InvalidArgument, "duplicate notification filter name 'suffix' ({cfg_scope})"));
            }
            has_suffix = true;
        } else {
            return Err(s3_error!(InvalidArgument, "{}", invalid_filter_name_message(&cfg_scope, name)));
        }
    }

    Ok(())
}

fn parse_notification_target_id(arn_str: &str) -> Result<TargetID, TargetIDError> {
    ARN::parse(arn_str)
        .map(|arn| arn.target_id)
        .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
}

type NotificationEventRule = (Vec<EventName>, String, String, Vec<TargetID>);

fn build_notification_event_rules(destinations: Vec<NotificationDestination>) -> S3Result<Vec<NotificationEventRule>> {
    destinations
        .into_iter()
        .map(|destination| {
            let events = destination
                .events
                .iter()
                .filter_map(|event| EventName::parse(event).ok())
                .collect();
            let (mut prefix, mut suffix) = (String::new(), String::new());
            for (name, value) in destination.filters {
                if let (Some(name), Some(value)) = (name, value) {
                    if name.eq_ignore_ascii_case("prefix") {
                        prefix = value;
                    } else if name.eq_ignore_ascii_case("suffix") {
                        suffix = value;
                    }
                }
            }
            let target = parse_notification_target_id(&destination.target_arn)
                .map_err(|err| s3_error!(InvalidArgument, "Invalid ARN in notification configuration: {err}"))?;
            Ok((events, prefix, suffix, vec![target]))
        })
        .collect()
}

fn resolve_notification_region(global_region: Option<String>, request_region: Option<&str>) -> String {
    global_region.unwrap_or_else(|| request_region.unwrap_or(RUSTFS_REGION).to_owned())
}

impl DefaultBucketUsecase {
    /// The stored notification configuration, or an empty answer when none is stored.
    pub async fn execute_get_bucket_notification_configuration(
        &self,
        input: gateway_dto::GetBucketNotificationConfigurationInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::GetBucketNotificationConfigurationOutput> {
        let bucket = input.bucket.as_str();
        let Some(store) = self.object_store() else {
            return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let stored = match metadata_sys::get_notification_config(bucket).await {
            Ok(config) => config,
            // An unreadable config is not "no notifications configured".
            Err(err) if crate::storage_api::error::is_unreadable_config_error(&err) => {
                return Err(ApiError::from(err).into());
            }
            Err(err) => {
                warn!(
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_BUCKET,
                    event = "bucket_notification_config_load_failed",
                    bucket = %bucket,
                    error = ?err,
                    "Failed to load bucket notification configuration"
                );
                None
            }
        };
        let Some(stored) = stored else {
            return Ok(gateway_dto::GetBucketNotificationConfigurationOutput::default());
        };

        let gateway_dto::NotificationConfiguration {
            event_bridge_configuration,
            lambda_function_configurations,
            queue_configurations,
            topic_configurations,
        } = dto_values::stored_config(&stored, parse_notification_dto)?;
        Ok(gateway_dto::GetBucketNotificationConfigurationOutput {
            event_bridge_configuration,
            lambda_function_configurations,
            queue_configurations,
            topic_configurations,
        })
    }

    /// Persists notification configuration, then updates the active rules for the bucket.
    pub(crate) async fn execute_put_bucket_notification_configuration(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        destinations: Vec<NotificationDestination>,
        prepared: S3Result<PersistedNotificationConfiguration>,
    ) -> S3Result<gateway_dto::PutBucketNotificationConfigurationOutput> {
        RequestEnvelope::audit_only_operation(
            envelope,
            S3Operation::PutBucketNotificationConfiguration,
            StatusCode::OK,
            |envelope| async move {
                let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
                let request_context = envelope.request_context().cloned();

                for destination in &destinations {
                    validate_notification_filter_rules(destination)?;
                }
                let persisted_notification_configuration = prepared?;

                let Some(store) = self.object_store() else {
                    return Err(s3_error!(InternalError, "Not init"));
                };
                store
                    .get_bucket_info(bucket, &BucketOptions::default())
                    .await
                    .map_err(ApiError::from)?;

                let region =
                    resolve_notification_region(self.global_region().map(|region| region.to_string()), envelope.region());
                let notify = current_notify_interface_for_context(self.context.as_deref());
                let event_rules = build_notification_event_rules(destinations)?;

                // Validate before the write. Clear and add still run after the durable update.
                notify
                    .validate_event_specific_rules(bucket, &region, &event_rules)
                    .await
                    .map_err(|e| s3_error!(InternalError, "Failed to add rules: {e}"))?;

                let data = serialize_config(&persisted_notification_configuration)?;
                update_bucket_config_for_incarnation(bucket, BUCKET_NOTIFICATION_CONFIG, data, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;

                notify_bucket_metadata_reload(bucket.to_owned(), "put bucket notification", request_context, false).await;

                notify
                    .clear_bucket_notification_rules(bucket)
                    .await
                    .map_err(|e| s3_error!(InternalError, "Failed to clear rules: {e}"))?;
                warn!("notify event rules: {:?}", &event_rules);
                notify
                    .add_event_specific_rules(bucket, &region, &event_rules)
                    .await
                    .map_err(|e| s3_error!(InternalError, "Failed to add rules: {e}"))?;

                Ok(gateway_dto::PutBucketNotificationConfigurationOutput::default())
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::{build_request, owned_envelope};
    use http::Method;

    #[tokio::test]
    async fn execute_get_bucket_notification_configuration_returns_internal_error_when_store_uninitialized() {
        let input = gateway_dto::GetBucketNotificationConfigurationInput {
            bucket: rustfs_gateway_types::BucketName::new("test-bucket").expect("valid bucket name"),
            ..Default::default()
        };
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_get_bucket_notification_configuration(input)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }

    #[test]
    fn resolve_notification_region_prefers_global_region() {
        assert_eq!(
            resolve_notification_region(Some("us-east-1".to_owned()), Some("ap-southeast-1")),
            "us-east-1"
        );
    }

    #[test]
    fn resolve_notification_region_falls_back_to_request_region() {
        assert_eq!(resolve_notification_region(None, Some("ap-southeast-1")), "ap-southeast-1");
    }

    #[test]
    fn resolve_notification_region_defaults_value() {
        assert_eq!(resolve_notification_region(None, None), RUSTFS_REGION);
    }

    fn destination(kind: &'static str, filters: &[(Option<&str>, Option<&str>)]) -> NotificationDestination {
        NotificationDestination {
            kind,
            id: Some("fixture".to_owned()),
            events: vec!["s3:ObjectCreated:*".to_owned()],
            target_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_owned(),
            filters: filters
                .iter()
                .map(|&(name, value)| (name.map(str::to_owned), value.map(str::to_owned)))
                .collect(),
        }
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_invalid_filter_name() {
        let raw_name = "unsupported".repeat(100);
        let cfg = destination("QueueConfiguration", &[(Some(&raw_name), Some("uploads/"))]);
        let err = validate_notification_filter_rules(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        let msg = err.message().unwrap_or_default();
        assert!(msg.contains("len="), "error message should include summarized length");
        assert!(!msg.contains(&raw_name), "error message should not echo full raw filter name");
    }

    #[test]
    fn validate_notification_configuration_filters_accepts_case_insensitive_filter_names() {
        let cfg = destination(
            "QueueConfiguration",
            &[(Some("Prefix"), Some("uploads/")), (Some("Suffix"), Some(".csv"))],
        );
        validate_notification_filter_rules(&cfg).expect("capitalized filter names should be accepted");
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_duplicate_prefix_rules() {
        for kind in ["QueueConfiguration"] {
            for duplicate in ["prefix", "PREFIX"] {
                let cfg = destination(kind, &[(Some("prefix"), Some("uploads/")), (Some(duplicate), Some("images/"))]);
                let err = validate_notification_filter_rules(&cfg).unwrap_err();
                assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
                assert_eq!(
                    err.message(),
                    Some(format!("duplicate notification filter name 'prefix' ({kind} id=fixture)").as_str())
                );
            }
        }
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_duplicate_suffix_rules() {
        for kind in ["QueueConfiguration", "TopicConfiguration"] {
            for duplicate in ["suffix", "SUFFIX"] {
                let cfg = destination(kind, &[(Some("suffix"), Some(".csv")), (Some(duplicate), Some(".log"))]);
                let err = validate_notification_filter_rules(&cfg).unwrap_err();
                assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
                assert_eq!(
                    err.message(),
                    Some(format!("duplicate notification filter name 'suffix' ({kind} id=fixture)").as_str())
                );
            }
        }
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_invalid_filter_value() {
        let cfg = destination("QueueConfiguration", &[(Some("suffix"), Some("../secret"))]);
        let err = validate_notification_filter_rules(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        let msg = err.message().unwrap_or_default();
        assert!(msg.contains("len="), "error message should include summarized length");
        assert!(!msg.contains("../secret"), "error message should not echo full raw filter value");
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_missing_filter_name() {
        for (kind, value) in [
            ("QueueConfiguration", Some("uploads/")),
            ("LambdaFunctionConfiguration", None),
        ] {
            let cfg = destination(kind, &[(None, value)]);
            let err = validate_notification_filter_rules(&cfg).unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
            assert_eq!(
                err.message(),
                Some(format!("invalid notification filter rule: missing Name ({kind} id=fixture)").as_str())
            );
        }
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_missing_filter_value() {
        let cfg = destination("QueueConfiguration", &[(Some("prefix"), None)]);
        let err = validate_notification_filter_rules(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(
            err.message(),
            Some("invalid notification filter rule: missing Value (QueueConfiguration id=fixture)")
        );
    }

    #[test]
    fn notification_filter_values_preserve_byte_and_path_boundaries() {
        let limit = "é".repeat(512);
        for value in ["", "a.b/..suffix/.prefix", &limit] {
            let cfg = destination("QueueConfiguration", &[(Some("prefix"), Some(value))]);
            validate_notification_filter_rules(&cfg).unwrap();
        }
        let too_long = format!("{limit}a");
        for value in ["a\\b", "a/./b", "a/../b", &too_long] {
            let cfg = destination("QueueConfiguration", &[(Some("unknown"), Some(value))]);
            let error = validate_notification_filter_rules(&cfg).unwrap_err();
            assert_eq!(error.code(), &S3ErrorCode::InvalidArgument);
            assert_eq!(
                error.message(),
                Some(format!("invalid notification filter value (len={}) (QueueConfiguration id=fixture)", value.len()).as_str())
            );
        }
    }

    #[tokio::test]
    async fn execute_put_bucket_notification_configuration_rejects_invalid_filter_before_store_lookup() {
        let usecase = DefaultBucketUsecase::without_context();
        let bucket = "test-bucket";
        let error = usecase
            .execute_put_bucket_notification_configuration(
                owned_envelope(Method::PUT),
                bucket,
                vec![destination("QueueConfiguration", &[(Some("unsupported"), Some("uploads/"))])],
                Ok(PersistedNotificationConfiguration::default()),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidArgument);

        let invalid = [
            destination("QueueConfiguration", &[(Some("unsupported"), Some("uploads/"))]),
            destination("TopicConfiguration", &[(None, None)]),
        ];
        let error = usecase
            .execute_put_bucket_notification_configuration(
                owned_envelope(Method::PUT),
                bucket,
                invalid.into(),
                Err(s3_error!(MalformedXML, "persistence conversion")),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(
            error.message(),
            Some(
                "invalid notification filter name (len=11) (only 'prefix'/'suffix' are supported) (QueueConfiguration id=fixture)"
            )
        );

        let error = usecase
            .execute_put_bucket_notification_configuration(
                owned_envelope(Method::PUT),
                bucket,
                Vec::new(),
                Err(s3_error!(MalformedXML, "persistence conversion")),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::MalformedXML);
        assert_eq!(error.message(), Some("persistence conversion"));

        let mut request = build_request((), Method::PUT);
        request.extensions.insert(crate::runtime_sources::ServerContextSlot::new());
        let error = usecase
            .execute_put_bucket_notification_configuration(
                RequestEnvelope::from_s3s(&request),
                bucket,
                vec![destination("QueueConfiguration", &[(None, None)])],
                Err(s3_error!(MalformedXML, "persistence conversion")),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InternalError);
        assert_eq!(error.message(), Some("bucket config mutation snapshot is missing"));

        let mut invalid_arn = destination("QueueConfiguration", &[]);
        invalid_arn.target_arn = "invalid-arn".to_owned();
        let error = usecase
            .execute_put_bucket_notification_configuration(
                owned_envelope(Method::PUT),
                bucket,
                vec![invalid_arn],
                Ok(PersistedNotificationConfiguration::default()),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InternalError);
        assert_eq!(error.message(), Some("Not init"));
    }
}

#[cfg(test)]
mod side_effect_tests;
