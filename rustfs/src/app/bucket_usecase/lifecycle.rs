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

//! Bucket lifecycle configuration use-case operations.

use super::*;
use crate::app::storage_api::s3::dto::{
    DeleteBucketLifecycleInput, DeleteBucketLifecycleOutput, ExpirationStatus, GetBucketLifecycleConfigurationInput,
    LifecycleRule, PutBucketLifecycleConfigurationInput, PutBucketLifecycleConfigurationOutput,
};
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

const ERR_LIFECYCLE_RULE_STATUS: &str = "Rule status must be either Enabled or Disabled";

fn assign_lifecycle_rule_ids(rules: &mut [LifecycleRule]) {
    let mut rule_ids: HashSet<String> = HashSet::new();

    for rule in rules.iter() {
        if let Some(id) = rule.id.as_ref() {
            rule_ids.insert(id.to_string());
        }
    }

    for (idx, rule) in rules.iter_mut().enumerate() {
        if rule.id.is_none() {
            let mut suffix = 0usize;
            let mut generated_id = format!("rule-{}", idx);

            while rule_ids.contains(&generated_id) {
                suffix += 1;
                generated_id = format!("rule-{idx}-{suffix}");
            }

            rule_ids.insert(generated_id.clone());
            rule.id = Some(generated_id);
        }
    }
}

fn validate_lifecycle_rule_status(rules: &[LifecycleRule]) -> std::result::Result<(), &'static str> {
    for rule in rules {
        if rule.status != ExpirationStatus::from_static(ExpirationStatus::ENABLED)
            && rule.status != ExpirationStatus::from_static(ExpirationStatus::DISABLED)
        {
            return Err(ERR_LIFECYCLE_RULE_STATUS);
        }
    }

    Ok(())
}

/// Map a lifecycle validation failure onto the S3 error the client should see.
///
/// The validator reports a schema-shape violation (a `Filter` with more than
/// one predicate, a one-member `And`) with
/// [`LIFECYCLE_MALFORMED_XML_ERROR_KIND`]; AWS answers those with
/// `MalformedXML`. Everything else is a value the schema allows but S3 refuses,
/// which stays `InvalidArgument` — the code this path has always returned
/// (backlog#2201).
fn lifecycle_validation_error(err: &std::io::Error) -> S3Error {
    if err.kind() == LIFECYCLE_MALFORMED_XML_ERROR_KIND {
        return S3Error::with_message(S3ErrorCode::MalformedXML, format!("Malformed XML: {err}"));
    }
    s3_error!(InvalidArgument, "{err}")
}

fn lifecycle_has_transition_rules(config: &PersistedLifecycleConfiguration) -> bool {
    config.rules.iter().any(|rule| {
        rule.status == "Enabled"
            && (rule.transitions.as_ref().is_some_and(|transitions| {
                transitions.iter().any(|transition| {
                    transition
                        .storage_class
                        .as_ref()
                        .is_some_and(|storage_class| !storage_class.as_str().is_empty())
                })
            }) || rule.noncurrent_version_transitions.as_ref().is_some_and(|transitions| {
                transitions.iter().any(|transition| {
                    transition
                        .storage_class
                        .as_ref()
                        .is_some_and(|storage_class| !storage_class.as_str().is_empty())
                })
            }))
    })
}

fn lifecycle_has_expiry_rules(config: &PersistedLifecycleConfiguration) -> bool {
    config.rules.iter().any(|rule| {
        rule.status == "Enabled"
            && (rule.expiration.is_some() || rule.del_marker_expiration.is_some() || rule.noncurrent_version_expiration.is_some())
    })
}

/// Status-independent presence of the expiry subset that site replication
/// propagates (`replicateILMExpiry`): expiration / noncurrent-version
/// expiration only. Distinct from [`lifecycle_has_expiry_rules`], which
/// filters on ENABLED for scanner scheduling — editing a Disabled expiry rule
/// must still advance the replication axis. Del-marker expiration and
/// abort-multipart are site-local and never travel.
fn lifecycle_rules_have_expiry(config: &PersistedLifecycleConfiguration) -> bool {
    config
        .rules
        .iter()
        .any(|rule| rule.expiration.is_some() || rule.noncurrent_version_expiration.is_some())
}

fn lifecycle_has_abort_multipart_rules(config: &PersistedLifecycleConfiguration) -> bool {
    config
        .rules
        .iter()
        .any(|rule| rule.status == "Enabled" && rule.abort_incomplete_multipart_upload.is_some())
}

impl DefaultBucketUsecase {
    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let DeleteBucketLifecycleInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        delete_bucket_config_for_incarnation(&bucket, BUCKET_LIFECYCLE_CONFIG, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "delete bucket lifecycle", request_context, true).await;

        let item = sr_bucket_meta_item(bucket.clone(), "lc-config");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket lifecycle delete hook failed");
        }

        Ok(S3Response::new(DeleteBucketLifecycleOutput::default()))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<PersistedLifecycleConfiguration> {
        let GetBucketLifecycleConfigurationInput { bucket, .. } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let config = match metadata_sys::get_lifecycle_config(&bucket).await {
            Ok((cfg, _)) => cfg,
            Err(StorageError::ConfigNotFound) => {
                return Err(s3_error!(NoSuchLifecycleConfiguration));
            }
            // An unreadable stored configuration is not an absent one.
            Err(err) => return Err(ApiError::from(err).into()),
        };

        Ok(config)
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
        prepared_configuration: std::io::Result<Option<PersistedLifecycleConfiguration>>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let PutBucketLifecycleConfigurationInput {
            bucket,
            lifecycle_configuration,
            ..
        } = req.input;

        let Some(mut wire_cfg) = lifecycle_configuration else { return Err(s3_error!(InvalidArgument)) };
        assign_lifecycle_rule_ids(&mut wire_cfg.rules);
        if let Err(err) = validate_lifecycle_rule_status(&wire_cfg.rules) {
            return Err(S3Error::with_message(S3ErrorCode::MalformedXML, format!("Malformed XML: {err}")));
        }

        let rcfg = match metadata_sys::get_object_lock_config(&bucket).await {
            Ok((cfg, _)) => cfg,
            Err(StorageError::ConfigNotFound) => ObjectLockConfiguration::default(),
            Err(err) => {
                warn!(
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_BUCKET,
                    event = "bucket_object_lock_config_load_failed",
                    bucket = %bucket,
                    error = ?err,
                    "Failed to load bucket object lock configuration"
                );
                return Err(ApiError::from(err).into());
            }
        };

        // Keep conversion failures behind the request-boundary status and Object Lock checks.
        let Some(mut input_cfg) = prepared_configuration.map_err(to_internal_error)? else {
            return Err(rustfs_s3_types::s3_error!(InvalidArgument).into());
        };
        if input_cfg.rules.len() != wire_cfg.rules.len() {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "lifecycle conversion changed the rule count",
            ));
        }
        for (rule, wire_rule) in input_cfg.rules.iter_mut().zip(&wire_cfg.rules) {
            rule.id.clone_from(&wire_rule.id);
        }

        if let Err(err) = validate_lifecycle_config(&input_cfg, &rcfg).await {
            return Err(lifecycle_validation_error(&err));
        }

        if let Err(err) = validate_transition_tier(&input_cfg).await {
            return Err(s3_error!(InvalidArgument, "{err}"));
        }

        // Stamp the expiry axis only when the expiry subset can have changed
        // (MinIO: HasExpiry() || expiryRuleRemoved). Site-replication peers
        // judge lc-config staleness on this axis; a transition-only edit that
        // advanced it would let this site's stale expiry subset shadow — and
        // roll back — a newer peer expiry edit fleet-wide.
        let previous_expiry_updated_at = match metadata_sys::get_lifecycle_config(&bucket).await {
            Ok((previous, _)) => {
                if lifecycle_rules_have_expiry(&input_cfg) || lifecycle_rules_have_expiry(&previous) {
                    Some(
                        time::OffsetDateTime::now_utc()
                            .format(&time::format_description::well_known::Rfc3339)
                            .map_err(to_internal_error)?,
                    )
                } else {
                    previous.expiry_updated_at
                }
            }
            // No previous config (or unreadable): stamping is the
            // conservative pre-existing behavior.
            Err(_) => {
                if lifecycle_rules_have_expiry(&input_cfg) {
                    Some(
                        time::OffsetDateTime::now_utc()
                            .format(&time::format_description::well_known::Rfc3339)
                            .map_err(to_internal_error)?,
                    )
                } else {
                    None
                }
            }
        };
        input_cfg.expiry_updated_at = previous_expiry_updated_at;
        let data = serialize_config(&input_cfg)?;
        update_bucket_config_for_incarnation(&bucket, BUCKET_LIFECYCLE_CONFIG, data, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "put bucket lifecycle", request_context, true).await;

        let mut item = sr_bucket_meta_item(bucket.clone(), "lc-config");
        item.expiry_lc_config =
            Some(serialize_config(&input_cfg).and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?);
        // The item travels with the expiry axis, not the wall clock: a site
        // whose expiry knowledge is old (or absent — UNIX_EPOCH) must not
        // out-rank newer peer expiry state at the receivers.
        item.expiry_updated_at = input_cfg
            .expiry_updated_at
            .as_deref()
            .map(|value| time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339))
            .transpose()
            .map_err(to_internal_error)?
            .or(Some(time::OffsetDateTime::UNIX_EPOCH));
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket lifecycle hook failed");
        }

        if lifecycle_has_transition_rules(&input_cfg)
            && let Some(store) = self.object_store()
        {
            let bucket_name = bucket.clone();
            let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
            spawn_background_with_context(request_context, async move {
                if let Err(err) = enqueue_transition_for_existing_objects(store, &bucket_name).await {
                    warn!(bucket = %bucket_name, error = ?err, "failed to enqueue transition for existing objects");
                }
            });
        }

        if lifecycle_has_expiry_rules(&input_cfg)
            && let Some(store) = self.object_store()
        {
            let bucket_name = bucket.clone();
            let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
            spawn_background_with_context(request_context, async move {
                if let Err(err) = enqueue_expiry_for_existing_objects(store, &bucket_name).await {
                    warn!(bucket = %bucket_name, error = ?err, "failed to enqueue expiry for existing objects");
                }
            });
        }

        if lifecycle_has_abort_multipart_rules(&input_cfg)
            && let Some(store) = self.object_store()
        {
            let bucket_name = bucket.clone();
            let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
            spawn_background_with_context(request_context, async move {
                let deleted = run_stale_multipart_upload_cleanup_once(store).await;
                debug!(bucket = %bucket_name, deleted, "completed lifecycle abort multipart cleanup trigger");
            });
        }

        Ok(S3Response::new(PutBucketLifecycleConfigurationOutput::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use crate::app::storage_api::s3 as s3s_crate;
    use crate::app::storage_api::s3::dto::{
        BucketLifecycleConfiguration, LifecycleExpiration, NoncurrentVersionTransition, Transition, TransitionStorageClass,
    };
    use http::Method;

    #[test]
    fn normalize_lifecycle_rules_generate_rule_ids_for_missing_values() {
        let mut rules = vec![
            LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: None,
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            },
            LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(60),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("rule-1".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            },
            LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(90),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: None,
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            },
        ];

        assign_lifecycle_rule_ids(&mut rules);

        assert_eq!(rules[0].id.as_deref(), Some("rule-0"));
        assert_eq!(rules[1].id.as_deref(), Some("rule-1"));
        assert_eq!(rules[2].id.as_deref(), Some("rule-2"));
    }

    #[tokio::test]
    async fn put_bucket_lifecycle_validation_errors_keep_their_s3_code() {
        // The PUT path answers a schema-shape violation with MalformedXML and a
        // rejected value with InvalidArgument. Both categories are produced by
        // the real validator here, so the mapping cannot drift from it
        // (backlog#2201).
        let malformed = validate_lifecycle_config(
            &rustfs_gateway_types::persistence::PersistedLifecycleConfiguration {
                expiry_updated_at: None,
                rules: vec![rustfs_gateway_types::persistence::PersistedLifecycleRule {
                    status: "Enabled".to_owned(),
                    expiration: Some(rustfs_gateway_types::persistence::PersistedLifecycleExpiration {
                        days: Some(1),
                        ..Default::default()
                    }),
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    filter: Some(rustfs_gateway_types::persistence::PersistedLifecycleFilter {
                        and: Some(rustfs_gateway_types::persistence::PersistedLifecycleAnd {
                            prefix: Some("logs/".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    id: Some("one-member-and".to_string()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                }],
            },
            &ObjectLockConfiguration::default(),
        )
        .await
        .expect_err("a Filter And with one predicate is a schema violation");
        assert_eq!(*lifecycle_validation_error(&malformed).code(), S3ErrorCode::MalformedXML);

        let invalid_value = validate_lifecycle_config(
            &rustfs_gateway_types::persistence::PersistedLifecycleConfiguration {
                expiry_updated_at: None,
                rules: vec![rustfs_gateway_types::persistence::PersistedLifecycleRule {
                    status: "Enabled".to_owned(),
                    expiration: None,
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    filter: None,
                    id: Some("negative-count".to_string()),
                    noncurrent_version_expiration: Some(
                        rustfs_gateway_types::persistence::PersistedNoncurrentVersionExpiration {
                            noncurrent_days: Some(30),
                            newer_noncurrent_versions: Some(-1),
                        },
                    ),
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: None,
                }],
            },
            &ObjectLockConfiguration::default(),
        )
        .await
        .expect_err("a negative retention count is rejected");
        assert_eq!(*lifecycle_validation_error(&invalid_value).code(), S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_lifecycle_rule_status_rejects_invalid_status() {
        let rules = vec![LifecycleRule {
            status: ExpirationStatus::from_static("enabled"),
            expiration: Some(LifecycleExpiration {
                days: Some(30),
                ..Default::default()
            }),
            abort_incomplete_multipart_upload: None,
            del_marker_expiration: None,
            filter: None,
            id: None,
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            transitions: None,
        }];

        assert_eq!(validate_lifecycle_rule_status(&rules).unwrap_err(), ERR_LIFECYCLE_RULE_STATUS);
    }

    #[test]
    fn lifecycle_has_transition_rules_ignores_disabled_rules() {
        let config = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::DISABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("disabled-transition".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: Some(vec![Transition {
                    days: Some(1),
                    date: None,
                    storage_class: Some(TransitionStorageClass::from_static("WARM")),
                }]),
            }],
        };

        assert!(!lifecycle_has_transition_rules(
            &crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s(config).unwrap()
        ));
    }

    #[test]
    fn lifecycle_has_transition_rules_accepts_enabled_noncurrent_transitions() {
        let config = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("enabled-noncurrent-transition".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: Some(vec![NoncurrentVersionTransition {
                    noncurrent_days: Some(1),
                    newer_noncurrent_versions: None,
                    storage_class: Some(TransitionStorageClass::from_static("WARM")),
                }]),
                prefix: None,
                transitions: None,
            }],
        };

        assert!(lifecycle_has_transition_rules(
            &crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s(config).unwrap()
        ));
    }

    #[test]
    fn lifecycle_has_abort_multipart_rules_accepts_enabled_abort_rule() {
        let config = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: Some(s3s_crate::dto::AbortIncompleteMultipartUpload {
                    days_after_initiation: Some(0),
                }),
                del_marker_expiration: None,
                filter: None,
                id: Some("enabled-abort".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        assert!(lifecycle_has_abort_multipart_rules(
            &crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s(config.clone()).unwrap()
        ));
        assert!(!lifecycle_has_expiry_rules(
            &crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s(config).unwrap()
        ));
    }

    #[tokio::test]
    async fn execute_put_bucket_lifecycle_configuration_rejects_missing_configuration() {
        let input = PutBucketLifecycleConfigurationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let prepared = req
            .input
            .lifecycle_configuration
            .clone()
            .map(crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s)
            .transpose();
        let err = usecase
            .execute_put_bucket_lifecycle_configuration(req, prepared)
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }
}
