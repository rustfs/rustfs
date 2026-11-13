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
use crate::app::storage_api::s3::dto::{
    GetBucketNotificationConfigurationInput, NotificationConfiguration, NotificationConfigurationFilter,
    PutBucketNotificationConfigurationInput, PutBucketNotificationConfigurationOutput,
};
use crate::app::storage_api::s3::region::Region;
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

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

fn validate_notification_filter_rules(
    filter: Option<&NotificationConfigurationFilter>,
    cfg_kind: &str,
    cfg_id: Option<&str>,
) -> S3Result<()> {
    let Some(filter) = filter else {
        return Ok(());
    };
    let Some(s3key_filter) = filter.key.as_ref() else {
        return Ok(());
    };
    let Some(rules) = s3key_filter.filter_rules.as_ref() else {
        return Ok(());
    };

    let mut has_prefix = false;
    let mut has_suffix = false;
    let cfg_scope = cfg_id.map_or_else(|| cfg_kind.to_string(), |id| format!("{cfg_kind} id={id}"));

    for rule in rules {
        let Some(name) = rule.name.as_ref() else {
            return Err(s3_error!(InvalidArgument, "invalid notification filter rule: missing Name ({cfg_scope})"));
        };
        let Some(value) = rule.value.as_ref() else {
            return Err(s3_error!(
                InvalidArgument,
                "invalid notification filter rule: missing Value ({cfg_scope})"
            ));
        };

        if !is_valid_notification_filter_value(value) {
            return Err(s3_error!(InvalidArgument, "{}", invalid_filter_value_message(&cfg_scope, value)));
        }

        if name.as_str().eq_ignore_ascii_case("prefix") {
            if has_prefix {
                return Err(s3_error!(InvalidArgument, "duplicate notification filter name 'prefix' ({cfg_scope})"));
            }
            has_prefix = true;
        } else if name.as_str().eq_ignore_ascii_case("suffix") {
            if has_suffix {
                return Err(s3_error!(InvalidArgument, "duplicate notification filter name 'suffix' ({cfg_scope})"));
            }
            has_suffix = true;
        } else {
            return Err(s3_error!(InvalidArgument, "{}", invalid_filter_name_message(&cfg_scope, name.as_str())));
        }
    }

    Ok(())
}

fn validate_notification_configuration_filters(notification_configuration: &NotificationConfiguration) -> S3Result<()> {
    if let Some(queue_configs) = notification_configuration.queue_configurations.as_ref() {
        for cfg in queue_configs {
            validate_notification_filter_rules(cfg.filter.as_ref(), "QueueConfiguration", cfg.id.as_deref())?;
        }
    }
    if let Some(topic_configs) = notification_configuration.topic_configurations.as_ref() {
        for cfg in topic_configs {
            validate_notification_filter_rules(cfg.filter.as_ref(), "TopicConfiguration", cfg.id.as_deref())?;
        }
    }
    if let Some(lambda_configs) = notification_configuration.lambda_function_configurations.as_ref() {
        for cfg in lambda_configs {
            validate_notification_filter_rules(cfg.filter.as_ref(), "LambdaFunctionConfiguration", cfg.id.as_deref())?;
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

/// Builds the notify runtime rules for a bucket notification configuration
/// without touching the store or the runtime rule state.
fn build_notification_event_rules(
    notification_configuration: &NotificationConfiguration,
) -> S3Result<Vec<NotificationEventRule>> {
    let mut event_rules = Vec::new();
    let invalid_arn = |e: TargetIDError| {
        S3Error::with_message(S3ErrorCode::InvalidArgument, format!("Invalid ARN in notification configuration: {e}"))
    };

    process_queue_configurations(
        &mut event_rules,
        notification_configuration.queue_configurations.clone(),
        parse_notification_target_id,
    )
    .map_err(invalid_arn)?;
    process_topic_configurations(
        &mut event_rules,
        notification_configuration.topic_configurations.clone(),
        parse_notification_target_id,
    )
    .map_err(invalid_arn)?;
    process_lambda_configurations(
        &mut event_rules,
        notification_configuration.lambda_function_configurations.clone(),
        parse_notification_target_id,
    )
    .map_err(invalid_arn)?;

    Ok(event_rules)
}

fn resolve_notification_region(global_region: Option<Region>, request_region: Option<Region>) -> String {
    global_region
        .or(request_region)
        .map(|region| region.to_string())
        .unwrap_or_else(|| RUSTFS_REGION.to_string())
}

impl DefaultBucketUsecase {
    pub async fn execute_get_bucket_notification_configuration(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<Option<PersistedNotificationConfiguration>> {
        let GetBucketNotificationConfigurationInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let has_notification_config = match metadata_sys::get_notification_config(&bucket).await {
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

        Ok(has_notification_config)
    }

    pub async fn execute_put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
        prepared: S3Result<PersistedNotificationConfiguration>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_region = req.region.clone();
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();

        let PutBucketNotificationConfigurationInput {
            bucket,
            notification_configuration,
            ..
        } = req.input;

        validate_notification_configuration_filters(&notification_configuration)?;
        let persisted_notification_configuration = prepared?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let region = resolve_notification_region(self.global_region(), request_region);
        let notify = current_notify_interface_for_context(self.context.as_deref());
        let event_rules = build_notification_event_rules(&notification_configuration)?;

        // Reject the request before the store write so a failure cannot leave a
        // persisted configuration that the notify runtime refused to activate.
        notify
            .validate_event_specific_rules(&bucket, region.as_str(), &event_rules)
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to add rules: {e}"))?;

        let data = serialize_config(&persisted_notification_configuration)?;
        update_bucket_config_for_incarnation(&bucket, BUCKET_NOTIFICATION_CONFIG, data, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "put bucket notification", request_context, false).await;

        notify
            .clear_bucket_notification_rules(&bucket)
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to clear rules: {e}"))?;
        warn!("notify event rules: {:?}", &event_rules);
        notify
            .add_event_specific_rules(&bucket, region.as_str(), &event_rules)
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to add rules: {e}"))?;

        Ok(S3Response::new(PutBucketNotificationConfigurationOutput {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use crate::app::storage_api::s3::dto::{FilterRule, FilterRuleName, QueueConfiguration, S3KeyFilter};
    use http::Method;

    #[test]
    fn resolve_notification_region_prefers_global_region() {
        let binding = resolve_notification_region(Some("us-east-1".parse().unwrap()), Some("ap-southeast-1".parse().unwrap()));
        assert_eq!(binding, "us-east-1");
    }

    #[test]
    fn resolve_notification_region_falls_back_to_request_region() {
        let binding = resolve_notification_region(None, Some("ap-southeast-1".parse().unwrap()));
        assert_eq!(binding, "ap-southeast-1");
    }

    #[test]
    fn resolve_notification_region_defaults_value() {
        let binding = resolve_notification_region(None, None);
        assert_eq!(binding, RUSTFS_REGION);
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_invalid_filter_name() {
        let raw_name = "unsupported".repeat(100);
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![FilterRule {
                            name: Some(FilterRuleName::from(raw_name.clone())),
                            value: Some("uploads/".to_string()),
                        }]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        let msg = err.message().unwrap_or_default();
        assert!(msg.contains("len="), "error message should include summarized length");
        assert!(!msg.contains(&raw_name), "error message should not echo full raw filter name");
    }

    #[test]
    fn validate_notification_configuration_filters_accepts_case_insensitive_filter_names() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![
                            FilterRule {
                                name: Some(FilterRuleName::from("Prefix".to_string())),
                                value: Some("uploads/".to_string()),
                            },
                            FilterRule {
                                name: Some(FilterRuleName::from("Suffix".to_string())),
                                value: Some(".csv".to_string()),
                            },
                        ]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        validate_notification_configuration_filters(&cfg).expect("capitalized filter names should be accepted");
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_duplicate_prefix_rules() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![
                            FilterRule {
                                name: Some(FilterRuleName::from_static(FilterRuleName::PREFIX)),
                                value: Some("uploads/".to_string()),
                            },
                            FilterRule {
                                name: Some(FilterRuleName::from_static(FilterRuleName::PREFIX)),
                                value: Some("images/".to_string()),
                            },
                        ]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_invalid_filter_value() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![FilterRule {
                            name: Some(FilterRuleName::from_static(FilterRuleName::SUFFIX)),
                            value: Some("../secret".to_string()),
                        }]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        let msg = err.message().unwrap_or_default();
        assert!(msg.contains("len="), "error message should include summarized length");
        assert!(!msg.contains("../secret"), "error message should not echo full raw filter value");
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_missing_filter_name() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![FilterRule {
                            name: None,
                            value: Some("uploads/".to_string()),
                        }]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_missing_filter_value() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![FilterRule {
                            name: Some(FilterRuleName::from_static(FilterRuleName::PREFIX)),
                            value: None,
                        }]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_duplicate_suffix_rules() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![
                            FilterRule {
                                name: Some(FilterRuleName::from_static(FilterRuleName::SUFFIX)),
                                value: Some(".csv".to_string()),
                            },
                            FilterRule {
                                name: Some(FilterRuleName::from_static(FilterRuleName::SUFFIX)),
                                value: Some(".log".to_string()),
                            },
                        ]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_put_bucket_notification_configuration_rejects_invalid_filter_before_store_lookup() {
        let input = PutBucketNotificationConfigurationInput::builder()
            .bucket("test-bucket".to_string())
            .notification_configuration(NotificationConfiguration {
                queue_configurations: Some(vec![QueueConfiguration {
                    id: Some("q1".to_string()),
                    queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                    events: vec!["s3:ObjectCreated:*".to_string().into()],
                    filter: Some(NotificationConfigurationFilter {
                        key: Some(S3KeyFilter {
                            filter_rules: Some(vec![FilterRule {
                                name: Some(FilterRuleName::from("unsupported".to_string())),
                                value: Some("uploads/".to_string()),
                            }]),
                        }),
                    }),
                }]),
                ..Default::default()
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_put_bucket_notification_configuration(req, Ok(PersistedNotificationConfiguration::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }
}
