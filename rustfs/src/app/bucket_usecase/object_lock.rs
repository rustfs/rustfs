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

//! Bucket Object Lock configuration use-case operations.
//!
//! Default-retention policy here applies to bucket configuration. Object writes enforce their
//! own retention rules.

use super::*;
use crate::app::storage_api::bucket_usecase::bucket::VersioningConfigExt as _;
use crate::app::storage_api::bucket_usecase::bucket::metadata::OBJECT_LOCK_CONFIG;
use rustfs_gateway_types::persistence::{PersistedDefaultRetention, parse_object_lock_dto};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};

const LOG_SUBSYSTEM_OBJECT_LOCK: &str = "object_lock";

const MAXIMUM_RETENTION_DAYS: i32 = 36_500;
const MAXIMUM_RETENTION_YEARS: i32 = 100;

fn invalid_object_lock_configuration(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::MalformedXML, message.into())
}

fn propagate_object_lock_peer_reload(result: std::result::Result<(), StorageError>) -> S3Result<()> {
    result.map_err(|err| {
        S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to publish Object Lock metadata: {err}"))
    })
}

fn invalid_retention_period(message: impl Into<String>) -> S3Error {
    let mut err = S3Error::with_message(S3ErrorCode::Custom("InvalidRetentionPeriod".into()), message.into());
    err.set_status_code(StatusCode::BAD_REQUEST);
    err
}

fn validate_default_retention_configuration(default_retention: &PersistedDefaultRetention) -> S3Result<()> {
    let Some(mode) = default_retention.mode.as_deref() else {
        return Err(invalid_object_lock_configuration("retention mode must be specified"));
    };

    match mode {
        "COMPLIANCE" | "GOVERNANCE" => {}
        _ => {
            return Err(invalid_object_lock_configuration(format!("unknown retention mode {mode}")));
        }
    }

    match (default_retention.days, default_retention.years) {
        (Some(days), None) => {
            if days <= 0 {
                return Err(invalid_retention_period(
                    "Default retention period must be a positive integer value for 'Days'",
                ));
            }
            if days > MAXIMUM_RETENTION_DAYS {
                return Err(invalid_retention_period(format!("Default retention period too large for 'Days' {days}",)));
            }
        }
        (None, Some(years)) => {
            if years <= 0 {
                return Err(invalid_retention_period(
                    "Default retention period must be a positive integer value for 'Years'",
                ));
            }
            if years > MAXIMUM_RETENTION_YEARS {
                return Err(invalid_retention_period(format!(
                    "Default retention period too large for 'Years' {years}",
                )));
            }
        }
        (Some(_), Some(_)) => {
            return Err(invalid_object_lock_configuration("either Days or Years must be specified, not both"));
        }
        (None, None) => {
            return Err(invalid_object_lock_configuration("either Days or Years must be specified"));
        }
    }

    Ok(())
}

fn validate_object_lock_configuration_input(input_cfg: &ObjectLockConfiguration) -> S3Result<()> {
    let enabled = input_cfg.object_lock_enabled.as_deref();
    if enabled != Some("Enabled") {
        return Err(invalid_object_lock_configuration(
            "only 'Enabled' value is allowed to ObjectLockEnabled element",
        ));
    }

    if let Some(rule) = input_cfg.rule.as_ref() {
        let Some(default_retention) = rule.default_retention.as_ref() else {
            return Err(invalid_object_lock_configuration("Rule must include DefaultRetention"));
        };
        validate_default_retention_configuration(default_retention)?;
    }

    Ok(())
}

impl DefaultBucketUsecase {
    /// Persists the lock configuration, then enables versioning under the same authorization.
    ///
    /// Each write is fenced independently. A versioning write or peer reload can fail after
    /// the lock configuration is durable.
    pub(crate) async fn execute_put_object_lock_configuration(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        config: Option<ObjectLockConfiguration>,
    ) -> S3Result<()> {
        RequestEnvelope::audit_only_operation(envelope, S3Operation::PutObjectLockConfiguration, StatusCode::OK, |envelope| {
            self.put_object_lock_configuration(envelope, bucket, config)
        })
        .await
    }

    async fn put_object_lock_configuration(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        config: Option<ObjectLockConfiguration>,
    ) -> S3Result<()> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&envelope, bucket)?;
        let Some(config) = config else {
            return Err(s3_error!(InvalidArgument, "Invalid argument."));
        };
        self.ensure_config_bucket(bucket).await?;
        validate_object_lock_configuration_input(&config)?;

        match metadata_sys::get_object_lock_config(bucket).await {
            Ok(_) => {}
            Err(StorageError::ConfigNotFound) => {
                if !BucketVersioningSys::enabled(bucket).await {
                    return Err(s3_error!(
                        InvalidBucketState,
                        "Object Lock configuration cannot be enabled on existing buckets"
                    ));
                }
            }
            Err(err) => {
                warn!(
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_OBJECT_LOCK,
                    event = "object_lock_config_load_failed",
                    bucket = %bucket,
                    error = ?err,
                    "Failed to load bucket object lock configuration"
                );
                return Err(s3_error!(InternalError, "Failed to get bucket ObjectLockConfiguration"));
            }
        };

        let data = serialize_config(&config)?;
        let object_lock_config = String::from_utf8(data.clone()).map_err(to_internal_error)?;
        let updated_at = update_bucket_config_for_incarnation(bucket, OBJECT_LOCK_CONFIG, data, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        #[cfg(test)]
        tests::after_lock_write(bucket).await;

        // Versioning is a separate write and must stay fenced to the original incarnation.
        let versioning_config = BucketVersioningSys::get(bucket).await.map_err(ApiError::from)?;
        if !versioning_config.enabled() {
            let versioning_config = PersistedVersioningConfiguration {
                status: Some("Enabled".to_owned()),
                ..Default::default()
            };
            let data = serialize_config(&versioning_config)?;
            update_bucket_config_for_incarnation(bucket, BUCKET_VERSIONING_CONFIG, data, expected_incarnation_id)
                .await
                .map_err(ApiError::from)?;
        }

        let peer_reload = match current_notification_system_for_context(self.context.as_deref()) {
            Some(notification_sys) => notification_sys.load_bucket_metadata(bucket).await,
            None => Ok(()),
        };
        #[cfg(test)]
        let peer_reload = peer_reload.and(tests::peer_reload_outcome(bucket).await);
        propagate_object_lock_peer_reload(peer_reload)?;

        let site_result = site_replication_bucket_meta_hook(SRBucketMeta {
            bucket: bucket.to_owned(),
            r#type: "object-lock-config".to_owned(),
            object_lock_config: Some(object_lock_config),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_owned()),
            ..Default::default()
        })
        .await;
        #[cfg(test)]
        let site_result = site_result.and_then(|()| tests::site_hook_outcome());
        if let Err(err) = site_result {
            warn!(
                component = LOG_COMPONENT_APP,
                subsystem = LOG_SUBSYSTEM_OBJECT_LOCK,
                event = "put_object_lock_configuration",
                bucket = %bucket,
                result = "site_replication_hook_failed",
                error = ?err,
                "storage object lock state"
            );
        }

        rustfs_scanner::record_dirty_usage_bucket_from_producer(
            bucket,
            rustfs_scanner::SegmentInvalidationProducerIdentity::BucketMetadata,
        );
        Ok(())
    }

    /// The stored Object Lock configuration as the gateway's answer holds it.
    ///
    /// # Errors
    ///
    /// `InternalError` when the store is not initialized, the stored configuration cannot be read
    /// or cannot be represented; the lookup's storage error, `NoSuchBucket` among them;
    /// `ObjectLockConfigurationNotFoundError` when none is stored.
    #[instrument(level = "debug", skip(self, input))]
    pub async fn execute_get_object_lock_configuration(
        &self,
        input: gateway_dto::GetObjectLockConfigurationInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::GetObjectLockConfigurationOutput> {
        let bucket = input.bucket.as_str();
        let Some(store) = self.object_store() else {
            return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let config = match metadata_sys::get_object_lock_config(bucket).await {
            Ok((cfg, _created)) => cfg,
            Err(StorageError::ConfigNotFound) => {
                return Err(rustfs_s3_types::s3_error!(
                    ObjectLockConfigurationNotFoundError,
                    "Object Lock configuration does not exist for this bucket"
                ));
            }
            Err(err) => {
                warn!(
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_OBJECT_LOCK,
                    event = "object_lock_config_load_failed",
                    bucket = %bucket,
                    error = ?err,
                    "Failed to load bucket object lock configuration"
                );
                return Err(rustfs_s3_types::s3_error!(InternalError, "Failed to load Object Lock configuration"));
            }
        };

        Ok(gateway_dto::GetObjectLockConfigurationOutput {
            object_lock_configuration: Some(dto_values::stored_config(&config, parse_object_lock_dto)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rustfs_gateway_types::persistence::PersistedObjectLockRule;

    use crate::app::bucket_usecase::test_support::build_request;
    use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient};
    use crate::app::runtime_sources::ServerContextSlot;
    use crate::app::storage_api::bucket_usecase::helper::MutationCompletionProbe;
    use crate::app::storage_api::owned_parity::authorize_bucket_config_mutation;
    use crate::site_replication::hooks::BUCKET_META_HOOK_CALLS;
    use http::Method;
    use rustfs_credentials::Credentials;
    use serial_test::serial;
    use std::cell::{Cell, RefCell};

    const ORIGINAL: &[u8] = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>";
    const CHANGED: &[u8] = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Days>3</Days><Mode>GOVERNANCE</Mode></DefaultRetention></Rule></ObjectLockConfiguration>";
    const SUSPENDED: &[u8] = b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
    const ENABLED: &[u8] = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Scenario {
        FirstFence,
        SecondFence,
        PeerFailure,
        SiteFailure,
        Success,
    }

    type MetadataBytes = (Vec<u8>, Vec<u8>);

    struct CompletionProbe {
        scenario: Scenario,
        store: Arc<ECStore>,
        observations: RefCell<Vec<MetadataBytes>>,
        recreated_scanner_generation: Cell<Option<u64>>,
    }

    tokio::task_local! {
        static STATE: CompletionProbe;
    }

    async fn stored_lock_and_versioning(bucket: &str) -> MetadataBytes {
        let metadata = metadata_sys::get_global_bucket_metadata_sys()
            .expect("metadata system")
            .read()
            .await
            .get_config_from_disk(bucket)
            .await
            .expect("read metadata from disk");
        (metadata.object_lock_config_xml, metadata.versioning_config_xml)
    }

    async fn recreate(store: &Arc<ECStore>, bucket: &str) {
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete original bucket");
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate bucket");
        seed(bucket).await;
    }

    async fn seed(bucket: &str) {
        metadata_sys::update(bucket, OBJECT_LOCK_CONFIG, ORIGINAL.to_vec())
            .await
            .expect("store original lock");
        metadata_sys::update(bucket, BUCKET_VERSIONING_CONFIG, SUSPENDED.to_vec())
            .await
            .expect("store suspended versioning");
    }

    pub(super) async fn after_lock_write(bucket: &str) {
        if let Ok((Scenario::SecondFence, store)) = STATE.try_with(|state| (state.scenario, Arc::clone(&state.store))) {
            let bytes = stored_lock_and_versioning(bucket).await;
            STATE.with(|state| state.observations.borrow_mut().push(bytes));
            recreate(&store, bucket).await;
            STATE.with(|state| {
                state
                    .recreated_scanner_generation
                    .set(rustfs_scanner::scanner_dirty_usage_bucket_generation(bucket));
            });
        }
    }

    pub(super) async fn peer_reload_outcome(bucket: &str) -> Result<(), StorageError> {
        let Ok(scenario) = STATE.try_with(|state| state.scenario) else {
            return Ok(());
        };
        let bytes = stored_lock_and_versioning(bucket).await;
        STATE.with(|state| state.observations.borrow_mut().push(bytes));
        if scenario == Scenario::PeerFailure {
            return Err(StorageError::other("test peer reload failure"));
        }
        Ok(())
    }

    pub(super) fn site_hook_outcome() -> S3Result<()> {
        if matches!(STATE.try_with(|state| state.scenario), Ok(Scenario::SiteFailure)) {
            return Err(s3_error!(InternalError, "test site replication failure"));
        }
        Ok(())
    }

    async fn put_lock_with_observed_completion(scenario: Scenario) {
        let context = shared_gating_ambient().await;
        let store = context.object_store();
        let bucket = format!(
            "lock-cfg-{}-{}",
            match scenario {
                Scenario::FirstFence => "first",
                Scenario::SecondFence => "second",
                Scenario::PeerFailure => "peer",
                Scenario::SiteFailure => "site",
                Scenario::Success => "success",
            },
            uuid::Uuid::new_v4().simple()
        );
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        seed(&bucket).await;

        let server = ServerContextSlot::new();
        assert!(server.install(Arc::clone(&context)));
        let mut request = build_request((), Method::PUT);
        request.uri = format!("/{bucket}?object-lock").parse().expect("request URI");
        request.extensions.insert(Arc::clone(&server));
        request.extensions.insert(ReqInfo {
            cred: Some(Credentials {
                access_key: "bucket-lock-owner".to_owned(),
                ..Default::default()
            }),
            is_owner: true,
            ..Default::default()
        });
        let mut envelope = RequestEnvelope::from_s3s(&request);
        authorize_bucket_config_mutation(
            &server,
            &mut envelope,
            &bucket,
            Action::S3Action(S3Action::PutBucketObjectLockConfigurationAction),
        )
        .await
        .expect("capture the authorized bucket incarnation");

        if scenario == Scenario::FirstFence {
            recreate(&store, &bucket).await;
        }
        let scanner_before = rustfs_scanner::scanner_dirty_usage_bucket_generation(&bucket);
        let audits = MutationCompletionProbe::install(&bucket);
        let usecase = DefaultBucketUsecase::with_context(Some(context));
        let config = rustfs_gateway_types::persistence::parse_object_lock(CHANGED).expect("fixture parses");
        let (result, hooks, observed, recreated_scanner_generation) = STATE
            .scope(
                CompletionProbe {
                    scenario,
                    store,
                    observations: RefCell::default(),
                    recreated_scanner_generation: Cell::default(),
                },
                async {
                    let (result, hooks) = BUCKET_META_HOOK_CALLS
                        .scope(RefCell::default(), async {
                            let result = usecase
                                .execute_put_object_lock_configuration(envelope, &bucket, Some(config))
                                .await;
                            (result, BUCKET_META_HOOK_CALLS.with(RefCell::take))
                        })
                        .await;
                    let (observed, recreated_scanner_generation) =
                        STATE.with(|state| (state.observations.take(), state.recreated_scanner_generation.get()));
                    (result, hooks, observed, recreated_scanner_generation)
                },
            )
            .await;
        let stored = stored_lock_and_versioning(&bucket).await;
        let scanner_after = rustfs_scanner::scanner_dirty_usage_bucket_generation(&bucket);
        let audits_seen = audits.audits.lock().expect("audit entries");
        let seen: Vec<_> = audits_seen
            .iter()
            .map(|entry| (entry.api.name.as_deref(), entry.api.status_code))
            .collect();
        let batch = |lock: &[u8], version: &[u8]| (lock.to_vec(), version.to_vec());
        let (error, versioning, callbacks, status) = match scenario {
            Scenario::FirstFence => (Some(S3ErrorCode::NoSuchBucket), batch(ORIGINAL, SUSPENDED), vec![], 404),
            Scenario::SecondFence => (
                Some(S3ErrorCode::NoSuchBucket),
                batch(ORIGINAL, SUSPENDED),
                vec![batch(CHANGED, SUSPENDED)],
                404,
            ),
            Scenario::PeerFailure => (
                Some(S3ErrorCode::InternalError),
                batch(CHANGED, ENABLED),
                vec![batch(CHANGED, ENABLED)],
                500,
            ),
            Scenario::SiteFailure | Scenario::Success => (None, batch(CHANGED, ENABLED), vec![batch(CHANGED, ENABLED)], 200),
        };
        assert_eq!(
            (result.err().map(|error| error.code().clone()), stored, observed, seen),
            (
                error,
                versioning,
                callbacks,
                vec![(Some(S3Operation::PutObjectLockConfiguration.as_str()), Some(status))]
            ),
            "{scenario:?}",
        );
        if scenario == Scenario::SiteFailure || scenario == Scenario::Success {
            let [hook] = hooks.as_slice() else { panic!("{scenario:?} must call site replication once") };
            assert_eq!(
                (&hook.bucket, hook.item_type.as_str(), hook.scanner_generation),
                (&bucket, "object-lock-config", scanner_before)
            );
            assert!(scanner_after > scanner_before, "{scenario:?} must invalidate scanner usage");
        } else {
            assert!(hooks.is_empty(), "{scenario:?} must stop before site replication");
            let expected_generation = if scenario == Scenario::SecondFence {
                recreated_scanner_generation
            } else {
                scanner_before
            };
            assert_eq!(expected_generation, scanner_after, "{scenario:?} must stop before scanner invalidation");
        }
        assert!(audits.events.lock().expect("event entries").is_empty());
    }

    #[test]
    #[serial]
    fn put_lock_configuration_fences_the_first_write() {
        run_large_stack_test("bucket-lock-first-write", || put_lock_with_observed_completion(Scenario::FirstFence));
    }

    #[test]
    #[serial]
    fn put_lock_configuration_fences_the_versioning_write() {
        run_large_stack_test("bucket-lock-version-write", || put_lock_with_observed_completion(Scenario::SecondFence));
    }

    #[test]
    #[serial]
    fn put_lock_configuration_propagates_peer_reload_failure() {
        run_large_stack_test("bucket-lock-peer-failure", || put_lock_with_observed_completion(Scenario::PeerFailure));
    }

    #[test]
    #[serial]
    fn put_lock_configuration_continues_after_site_hook_failure() {
        run_large_stack_test("bucket-lock-site-failure", || put_lock_with_observed_completion(Scenario::SiteFailure));
    }

    #[test]
    #[serial]
    fn put_lock_configuration_completes_durable_writes_before_callbacks() {
        run_large_stack_test("bucket-lock-callback-order", || put_lock_with_observed_completion(Scenario::Success));
    }

    #[test]
    fn test_validate_object_lock_configuration_rejects_disabled_status() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some("Disabled".to_owned()),
            rule: None,
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::MalformedXML);
    }

    #[test]
    fn test_validate_object_lock_configuration_rejects_invalid_default_retention_mode() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some("Enabled".to_owned()),
            rule: Some(PersistedObjectLockRule {
                default_retention: Some(PersistedDefaultRetention {
                    mode: Some("abc".to_owned()),
                    days: Some(1),
                    years: None,
                }),
            }),
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::MalformedXML);
    }

    #[test]
    fn test_validate_object_lock_configuration_rejects_days_and_years_together() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some("Enabled".to_owned()),
            rule: Some(PersistedObjectLockRule {
                default_retention: Some(PersistedDefaultRetention {
                    mode: Some("GOVERNANCE".to_owned()),
                    days: Some(1),
                    years: Some(1),
                }),
            }),
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::MalformedXML);
    }

    #[test]
    fn test_validate_object_lock_configuration_rejects_missing_default_retention() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some("Enabled".to_owned()),
            rule: Some(PersistedObjectLockRule { default_retention: None }),
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::MalformedXML);
    }

    #[test]
    fn test_validate_object_lock_configuration_rejects_zero_days() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some("Enabled".to_owned()),
            rule: Some(PersistedObjectLockRule {
                default_retention: Some(PersistedDefaultRetention {
                    mode: Some("GOVERNANCE".to_owned()),
                    days: Some(0),
                    years: None,
                }),
            }),
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::Custom("InvalidRetentionPeriod".into()));
    }

    #[test]
    fn test_validate_object_lock_configuration_rejects_too_many_years() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some("Enabled".to_owned()),
            rule: Some(PersistedObjectLockRule {
                default_retention: Some(PersistedDefaultRetention {
                    mode: Some("COMPLIANCE".to_owned()),
                    days: None,
                    years: Some(101),
                }),
            }),
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::Custom("InvalidRetentionPeriod".into()));
    }

    #[test]
    fn test_object_lock_peer_reload_failure_is_returned_to_the_client() {
        let err = propagate_object_lock_peer_reload(Err(StorageError::other("peer reload failed")))
            .expect_err("peer reload failure must prevent a successful Object Lock update response");

        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert!(err.message().is_some_and(|message| message.contains("peer reload failed")));
    }

    #[tokio::test]
    async fn execute_get_object_lock_configuration_returns_internal_error_when_store_uninitialized() {
        let input = gateway_dto::GetObjectLockConfigurationInput {
            bucket: rustfs_gateway_types::BucketName::new("test-bucket").expect("valid bucket name"),
            ..Default::default()
        };
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_object_lock_configuration(input).await.unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }
}
