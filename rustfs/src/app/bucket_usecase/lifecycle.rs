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
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, s3_error};
use rustfs_gateway_types::persistence::{PersistedLifecycleRule, parse_lifecycle_dto};

const ERR_LIFECYCLE_RULE_STATUS: &str = "Rule status must be either Enabled or Disabled";

pub(crate) enum PreparedLifecycleConfiguration {
    Missing,
    InvalidStatus,
    Prepared(std::io::Result<PersistedLifecycleConfiguration>),
}

fn assign_lifecycle_rule_ids(rules: &mut [PersistedLifecycleRule]) {
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

pub(crate) fn validate_lifecycle_rule_status<'a>(
    statuses: impl IntoIterator<Item = &'a str>,
) -> std::result::Result<(), &'static str> {
    for status in statuses {
        if status != "Enabled" && status != "Disabled" {
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
    #[instrument(level = "debug", skip(self, envelope, bucket))]
    pub(crate) async fn execute_delete_bucket_lifecycle(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
    ) -> rustfs_s3_types::S3Result<gateway_dto::DeleteBucketLifecycleOutput> {
        RequestEnvelope::audit_only_operation(
            envelope,
            S3Operation::DeleteBucketLifecycle,
            StatusCode::OK,
            |envelope| async move {
                let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
                let request_context = envelope.request_context().cloned();

                let Some(store) = self.object_store() else {
                    return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
                };

                store
                    .get_bucket_info(bucket, &BucketOptions::default())
                    .await
                    .map_err(ApiError::from)?;

                delete_bucket_config_for_incarnation(bucket, BUCKET_LIFECYCLE_CONFIG, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;

                notify_bucket_metadata_reload(bucket.to_owned(), "delete bucket lifecycle", request_context, true).await;

                let item = sr_bucket_meta_item(bucket.to_owned(), "lc-config");
                if let Err(err) = site_replication_bucket_meta_hook(item).await {
                    warn!(bucket = %bucket, error = ?err, "site replication bucket lifecycle delete hook failed");
                }

                Ok(gateway_dto::DeleteBucketLifecycleOutput::default())
            },
        )
        .await
    }

    /// The stored lifecycle configuration as the gateway's answer holds it.
    ///
    /// # Errors
    ///
    /// `InternalError` when the store is not initialized or the stored document cannot be
    /// represented; the lookup's storage error, `NoSuchBucket` among them;
    /// `NoSuchLifecycleConfiguration` when none is stored.
    #[instrument(level = "debug", skip(self, input))]
    pub async fn execute_get_bucket_lifecycle_configuration(
        &self,
        input: gateway_dto::GetBucketLifecycleConfigurationInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::GetBucketLifecycleConfigurationOutput> {
        let bucket = input.bucket.as_str();
        let Some(store) = self.object_store() else {
            return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let config = match metadata_sys::get_lifecycle_config(bucket).await {
            Ok((cfg, _)) => cfg,
            Err(StorageError::ConfigNotFound) => {
                return Err(rustfs_s3_types::s3_error!(NoSuchLifecycleConfiguration));
            }
            // An unreadable stored configuration is not an absent one.
            Err(err) => return Err(ApiError::from(err).into()),
        };

        Ok(gateway_dto::GetBucketLifecycleConfigurationOutput {
            rules: dto_values::stored_config(&config, parse_lifecycle_dto)?.rules,
            ..Default::default()
        })
    }

    /// Validates and persists a lifecycle configuration fenced to the authorized bucket incarnation.
    ///
    /// # Errors
    /// Returns the original configuration validation or storage error. Missing configuration is
    /// `InvalidArgument`; invalid status is `MalformedXML`; conversion errors are `InternalError`.
    #[instrument(level = "debug", skip(self, envelope, bucket, prepared_configuration))]
    pub(crate) async fn execute_put_bucket_lifecycle_configuration(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        prepared_configuration: PreparedLifecycleConfiguration,
    ) -> rustfs_s3_types::S3Result<gateway_dto::PutBucketLifecycleConfigurationOutput> {
        RequestEnvelope::audit_only_operation(
            envelope,
            S3Operation::PutBucketLifecycleConfiguration,
            StatusCode::OK,
            |envelope| async move {
                let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
                let request_context = envelope.request_context().cloned();
                let prepared_configuration = match prepared_configuration {
                    PreparedLifecycleConfiguration::Missing => return Err(rustfs_s3_types::s3_error!(InvalidArgument)),
                    PreparedLifecycleConfiguration::InvalidStatus => {
                        return Err(rustfs_s3_types::s3_error!(MalformedXML, "Malformed XML: {ERR_LIFECYCLE_RULE_STATUS}"));
                    }
                    PreparedLifecycleConfiguration::Prepared(result) => result,
                };

                let rcfg = match metadata_sys::get_object_lock_config(bucket).await {
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
                let mut input_cfg = prepared_configuration.map_err(to_internal_error)?;
                assign_lifecycle_rule_ids(&mut input_cfg.rules);
                if let Err(err) = validate_lifecycle_config(&input_cfg, &rcfg).await {
                    return Err(lifecycle_validation_error(&err).into());
                }

                if let Err(err) = validate_transition_tier(&input_cfg).await {
                    return Err(rustfs_s3_types::s3_error!(InvalidArgument, "{err}"));
                }

                // Stamp the expiry axis only when the expiry subset can have changed
                // (MinIO: HasExpiry() || expiryRuleRemoved). Site-replication peers
                // judge lc-config staleness on this axis; a transition-only edit that
                // advanced it would let this site's stale expiry subset shadow — and
                // roll back — a newer peer expiry edit fleet-wide.
                let previous_expiry_updated_at = match metadata_sys::get_lifecycle_config(bucket).await {
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
                update_bucket_config_for_incarnation(bucket, BUCKET_LIFECYCLE_CONFIG, data, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;

                notify_bucket_metadata_reload(bucket.to_owned(), "put bucket lifecycle", request_context, true).await;

                let mut item = sr_bucket_meta_item(bucket.to_owned(), "lc-config");
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
                    let bucket_name = bucket.to_owned();
                    let request_context = envelope.request_context().cloned();
                    spawn_background_with_context(request_context, async move {
                        if let Err(err) = enqueue_transition_for_existing_objects(store, &bucket_name).await {
                            warn!(bucket = %bucket_name, error = ?err, "failed to enqueue transition for existing objects");
                        }
                    });
                }

                if lifecycle_has_expiry_rules(&input_cfg)
                    && let Some(store) = self.object_store()
                {
                    let bucket_name = bucket.to_owned();
                    let request_context = envelope.request_context().cloned();
                    spawn_background_with_context(request_context, async move {
                        if let Err(err) = enqueue_expiry_for_existing_objects(store, &bucket_name).await {
                            warn!(bucket = %bucket_name, error = ?err, "failed to enqueue expiry for existing objects");
                        }
                    });
                }

                if lifecycle_has_abort_multipart_rules(&input_cfg)
                    && let Some(store) = self.object_store()
                {
                    let bucket_name = bucket.to_owned();
                    let request_context = envelope.request_context().cloned();
                    spawn_background_with_context(request_context, async move {
                        let deleted = run_stale_multipart_upload_cleanup_once(store).await;
                        debug!(bucket = %bucket_name, deleted, "completed lifecycle abort multipart cleanup trigger");
                    });
                }

                Ok(gateway_dto::PutBucketLifecycleConfigurationOutput::default())
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient};
    use crate::app::runtime_sources::ServerContextSlot;
    use crate::app::storage_api::bucket_usecase::helper::MutationCompletionProbe;
    use crate::app::storage_api::owned_parity::{ReqInfo, authorize_bucket_config_mutation};
    use crate::app::storage_api::s3::dto;
    use crate::app::storage_api::s3::dto::{
        BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule, NoncurrentVersionTransition,
        PutBucketLifecycleConfigurationInput, Transition, TransitionStorageClass,
    };
    use crate::site_replication::hooks::{BUCKET_META_HOOK_CALLS, BucketMetaHookCall};
    use http::Method;
    use rustfs_credentials::Credentials;
    use rustfs_policy::policy::action::{Action, S3Action};
    use serial_test::serial;
    use std::cell::RefCell;
    use std::sync::Arc;

    const DELETE_LIFECYCLE_XML: &[u8] = b"<LifecycleConfiguration><Rule><ID>kept</ID><Status>Disabled</Status>\
<Filter/><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>";
    const DELETE_REQUEST_ID: &str = "lifecycle-delete-context";

    fn delete_envelope(context: &Arc<AppContext>, bucket: &str) -> (Arc<ServerContextSlot>, RequestEnvelope) {
        let server = ServerContextSlot::new();
        assert!(server.install(Arc::clone(context)));
        let mut request = build_request((), Method::DELETE);
        request.uri = format!("/{bucket}?lifecycle").parse().expect("bucket request URI");
        request.extensions.insert(Arc::clone(&server));
        request.extensions.insert(ReqInfo {
            cred: Some(Credentials {
                access_key: "lifecycle-delete-owner".to_owned(),
                ..Default::default()
            }),
            is_owner: true,
            ..Default::default()
        });
        request.extensions.insert(request_context::RequestContext {
            request_id: DELETE_REQUEST_ID.to_owned(),
            x_amz_request_id: DELETE_REQUEST_ID.to_owned(),
            trace_id: None,
            span_id: None,
            start_time: std::time::Instant::now(),
        });
        (server, RequestEnvelope::from_s3s(&request))
    }

    async fn authorized_lifecycle_envelope(context: &Arc<AppContext>, bucket: &str) -> RequestEnvelope {
        let (server, mut envelope) = delete_envelope(context, bucket);
        authorize_bucket_config_mutation(&server, &mut envelope, bucket, Action::S3Action(S3Action::PutBucketLifecycleAction))
            .await
            .expect("bucket owner authorization installs the incarnation snapshot");
        envelope
    }

    async fn seed_lifecycle(bucket: &str) {
        metadata_sys::update(bucket, BUCKET_LIFECYCLE_CONFIG, DELETE_LIFECYCLE_XML.to_vec())
            .await
            .expect("store lifecycle configuration");
    }

    async fn persisted_lifecycle(bucket: &str) -> Vec<u8> {
        metadata_sys::get_global_bucket_metadata_sys()
            .expect("initialized metadata system")
            .read()
            .await
            .get_config_from_disk(bucket)
            .await
            .expect("persisted bucket metadata")
            .lifecycle_config_xml
    }

    async fn observed_delete(
        usecase: &DefaultBucketUsecase,
        envelope: RequestEnvelope,
        bucket: &str,
    ) -> (
        rustfs_s3_types::S3Result<gateway_dto::DeleteBucketLifecycleOutput>,
        Vec<BucketMetaHookCall>,
    ) {
        BUCKET_META_HOOK_CALLS
            .scope(RefCell::default(), async move {
                let result = usecase.execute_delete_bucket_lifecycle(envelope, bucket).await;
                (result, BUCKET_META_HOOK_CALLS.with(RefCell::take))
            })
            .await
    }

    #[test]
    #[serial]
    fn delete_lifecycle_rejects_a_stale_bucket_incarnation() {
        run_large_stack_test("delete-lifecycle-stale", || async {
            let ambient = shared_gating_ambient().await;
            let store = ambient.object_store();
            let bucket = format!("lifecycle-delete-stale-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create original bucket");
            let envelope = authorized_lifecycle_envelope(&ambient, &bucket).await;

            store
                .delete_bucket(&bucket, &DeleteBucketOptions::default())
                .await
                .expect("delete original bucket");
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("recreate bucket");
            seed_lifecycle(&bucket).await;

            let before = rustfs_scanner::scanner_dirty_usage_bucket_generation(&bucket);
            let usecase = DefaultBucketUsecase::with_context(Some(ambient));
            let (result, calls) = observed_delete(&usecase, envelope, &bucket).await;
            let error = result.expect_err("a stale bucket incarnation cannot delete replacement configuration");
            assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::NoSuchBucket);
            assert_eq!(persisted_lifecycle(&bucket).await, DELETE_LIFECYCLE_XML);
            assert!(calls.is_empty());
            assert_eq!(rustfs_scanner::scanner_dirty_usage_bucket_generation(&bucket), before);
        });
    }

    #[test]
    #[serial]
    fn delete_lifecycle_rejects_misbound_or_missing_bucket_snapshots() {
        run_large_stack_test("delete-lifecycle-unbound", || async {
            let ambient = shared_gating_ambient().await;
            let store = ambient.object_store();
            let authorized = format!("lifecycle-delete-authorized-{}", uuid::Uuid::new_v4().simple());
            let target = format!("lifecycle-delete-target-{}", uuid::Uuid::new_v4().simple());
            for bucket in [&authorized, &target] {
                store
                    .make_bucket(bucket, &MakeBucketOptions::default())
                    .await
                    .expect("create bucket");
            }
            seed_lifecycle(&target).await;
            let misbound = authorized_lifecycle_envelope(&ambient, &authorized).await;
            let (_, missing) = delete_envelope(&ambient, &target);
            let usecase = DefaultBucketUsecase::with_context(Some(ambient));

            for (envelope, message) in [
                (misbound, "bucket config mutation snapshot does not match request bucket"),
                (missing, "bucket config mutation snapshot is missing"),
            ] {
                let before = rustfs_scanner::scanner_dirty_usage_bucket_generation(&target);
                let (result, calls) = observed_delete(&usecase, envelope, &target).await;
                let error = result.expect_err("an unbound snapshot cannot delete lifecycle configuration");
                assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
                assert_eq!(error.message(), Some(message));
                assert_eq!(persisted_lifecycle(&target).await, DELETE_LIFECYCLE_XML);
                assert!(calls.is_empty());
                assert_eq!(rustfs_scanner::scanner_dirty_usage_bucket_generation(&target), before);
            }
        });
    }

    #[test]
    #[serial]
    fn delete_lifecycle_reloads_scanner_and_audits_success_once() {
        run_large_stack_test("delete-lifecycle-completion", || async {
            let ambient = shared_gating_ambient().await;
            let store = ambient.object_store();
            let bucket = format!("lifecycle-delete-audit-{}", uuid::Uuid::new_v4().simple());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create bucket");
            seed_lifecycle(&bucket).await;
            let envelope = authorized_lifecycle_envelope(&ambient, &bucket).await;
            let before = rustfs_scanner::scanner_dirty_usage_bucket_generation(&bucket);
            let maintenance_before = rustfs_scanner::scanner_maintenance_generation();
            let audit = MutationCompletionProbe::install(&bucket);

            let usecase = DefaultBucketUsecase::with_context(Some(ambient));
            let (result, calls) = observed_delete(&usecase, envelope, &bucket).await;
            result.expect("delete lifecycle configuration");

            assert!(persisted_lifecycle(&bucket).await.is_empty());
            let [call] = calls.as_slice() else { panic!("expected one site replication hook call") };
            assert_eq!(call.bucket, bucket);
            assert_eq!(call.item_type, "lc-config");
            // Persistence and the local lifecycle reload each advance the maintenance generation.
            assert_eq!(
                call.scanner_maintenance_generation,
                maintenance_before.saturating_add(2),
                "lifecycle reload advances maintenance before the site hook"
            );
            assert_eq!(rustfs_scanner::scanner_maintenance_generation(), call.scanner_maintenance_generation);
            assert!(call.scanner_generation > before, "scanner reload precedes site replication");
            assert_eq!(rustfs_scanner::scanner_dirty_usage_bucket_generation(&bucket), call.scanner_generation);
            let audits = audit.audits.lock().expect("audit entries");
            assert_eq!(audits.len(), 1, "one audit entry");
            assert_eq!(audits[0].request_id.as_deref(), Some(DELETE_REQUEST_ID));
            assert_eq!(audits[0].api.status_code, Some(200), "audit status uses 200");
        });
    }

    #[tokio::test]
    async fn execute_get_bucket_lifecycle_configuration_returns_internal_error_when_store_uninitialized() {
        let input = gateway_dto::GetBucketLifecycleConfigurationInput {
            bucket: rustfs_gateway_types::BucketName::new("test-bucket").expect("valid bucket name"),
            ..Default::default()
        };
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_lifecycle_configuration(input).await.unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }

    #[test]
    fn normalize_lifecycle_rules_generate_rule_ids_for_missing_values() {
        let rules = vec![
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

        let mut rules = crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s(BucketLifecycleConfiguration {
            rules,
            ..Default::default()
        })
        .unwrap()
        .rules;
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
    fn validate_lifecycle_rule_status_checks_exact_values() {
        assert!(validate_lifecycle_rule_status(["Enabled", "Disabled"]).is_ok());
        for invalid in ["enabled", "unknown"] {
            assert_eq!(
                validate_lifecycle_rule_status(["Enabled", invalid]),
                Err(ERR_LIFECYCLE_RULE_STATUS),
                "{invalid}"
            );
        }
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
                abort_incomplete_multipart_upload: Some(dto::AbortIncompleteMultipartUpload {
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

        let err = usecase
            .execute_put_bucket_lifecycle_configuration(
                RequestEnvelope::from_s3s(&req),
                &req.input.bucket,
                PreparedLifecycleConfiguration::Missing,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
    }
}
