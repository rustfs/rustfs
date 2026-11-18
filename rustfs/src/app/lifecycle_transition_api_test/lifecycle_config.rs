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

//! PutBucketLifecycleConfiguration tests (batch A3 owns the lifecycle operations).

use super::common::*;
use super::*;
use crate::app::storage_api::s3::S3 as _;

fn expiration_lifecycle_configuration(prefix: &str) -> BucketLifecycleConfiguration {
    BucketLifecycleConfiguration {
        expiry_updated_at: None,
        rules: vec![LifecycleRule {
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            abort_incomplete_multipart_upload: None,
            del_marker_expiration: None,
            expiration: Some(LifecycleExpiration {
                date: Some(Timestamp::from(
                    time::OffsetDateTime::now_utc()
                        .replace_time(time::Time::MIDNIGHT)
                        .saturating_sub(time::Duration::days(1)),
                )),
                days: None,
                expired_object_delete_marker: None,
                ..Default::default()
            }),
            filter: Some(LifecycleRuleFilter {
                and: None,
                object_size_greater_than: None,
                object_size_less_than: None,
                prefix: Some(prefix.to_string()),
                tag: None,
                ..Default::default()
            }),
            id: Some("expire-existing".to_string()),
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            transitions: None,
        }],
    }
}

fn expired_object_delete_marker_lifecycle_configuration() -> BucketLifecycleConfiguration {
    BucketLifecycleConfiguration {
        expiry_updated_at: None,
        rules: vec![LifecycleRule {
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            abort_incomplete_multipart_upload: None,
            del_marker_expiration: None,
            expiration: Some(LifecycleExpiration {
                expired_object_delete_marker: Some(true),
                ..Default::default()
            }),
            filter: Some(LifecycleRuleFilter {
                prefix: Some("test/".to_string()),
                ..Default::default()
            }),
            id: Some("expired-delete-marker".to_string()),
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            transitions: None,
        }],
    }
}

fn del_marker_expiration_lifecycle_configuration(days: i32) -> BucketLifecycleConfiguration {
    BucketLifecycleConfiguration {
        expiry_updated_at: None,
        rules: vec![LifecycleRule {
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            abort_incomplete_multipart_upload: None,
            del_marker_expiration: Some(DelMarkerExpiration { days: Some(days) }),
            expiration: Some(LifecycleExpiration {
                days: Some(30),
                ..Default::default()
            }),
            filter: Some(LifecycleRuleFilter {
                prefix: Some("test/".to_string()),
                ..Default::default()
            }),
            id: Some("del-marker-expiration".to_string()),
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            transitions: None,
        }],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn compensation_driven_delete_marker_still_honors_lifecycle_cleanup() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultObjectUsecase::from_global();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-comp-del-marker-cleanup-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/object.txt";
    let payload = b"delete marker lifecycle should still clean up after compensation-driven transition";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set transition lifecycle configuration");

    with_forced_immediate_enqueue_timeout(&backend, || async {
        let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;
    })
    .await;

    let transitioned = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("object should eventually transition after compensation backfill");
    assert_eq!(backend.put_count().await, 1, "compensation should upload the version once");
    let remote_object = transitioned.transitioned_object.name.clone();

    assert!(backend.contains(&remote_object).await);

    let req = build_request(
        DeleteObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .unwrap(),
        Method::DELETE,
    );

    Box::pin(usecase.execute_delete_object_from_s3s(req))
        .await
        .expect("Failed to issue versioned delete after compensation-driven transition");

    assert!(
        wait_for_delete_marker(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "versioned delete should create a delete marker before lifecycle cleanup"
    );
    assert!(
        backend.contains(&remote_object).await,
        "delete marker creation should keep the transitioned remote object version"
    );

    let req = build_request(
        PutBucketLifecycleConfigurationInput::builder()
            .bucket(bucket.clone())
            .lifecycle_configuration(Some(expiration_lifecycle_configuration("test/")))
            .build()
            .unwrap(),
        Method::PUT,
    );
    FS::new()
        .put_bucket_lifecycle_configuration(req)
        .await
        .expect("Failed to update lifecycle configuration for delete marker cleanup");

    assert!(
        wait_for_delete_marker(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "delete marker should remain visible after lifecycle update until cleanup completes"
    );
    assert!(
        backend.contains(&remote_object).await,
        "delete marker lifecycle cleanup should not remove the transitioned remote object version"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn put_bucket_lifecycle_configuration_expires_existing_objects() {
    let (_disk_paths, ecstore) = setup_test_env().await;

    let bucket = format!("test-api-expire-existing-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/existing.txt";
    let payload = b"expire existing object after lifecycle update";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    let _ = upload_test_object(&ecstore, bucket.as_str(), object, payload).await;

    let req = build_request(
        PutBucketLifecycleConfigurationInput::builder()
            .bucket(bucket.clone())
            .lifecycle_configuration(Some(expiration_lifecycle_configuration("test/")))
            .build()
            .unwrap(),
        Method::PUT,
    );

    FS::new()
        .put_bucket_lifecycle_configuration(req)
        .await
        .expect("Failed to update lifecycle configuration");

    assert!(
        wait_for_delete_marker(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT).await,
        "existing object should be lifecycle-deleted after lifecycle update"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn put_bucket_lifecycle_configuration_allows_expired_delete_marker_on_object_lock_bucket() {
    let (_disk_paths, ecstore) = setup_test_env().await;

    let bucket = format!("test-lock-expired-del-marker-{}", &Uuid::new_v4().simple().to_string()[..8]);
    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_object_lock_enabled(bucket.as_str())
        .await
        .expect("Failed to enable object lock for bucket");

    let req = build_request(
        PutBucketLifecycleConfigurationInput::builder()
            .bucket(bucket.clone())
            .lifecycle_configuration(Some(expired_object_delete_marker_lifecycle_configuration()))
            .build()
            .unwrap(),
        Method::PUT,
    );

    FS::new()
        .put_bucket_lifecycle_configuration(req)
        .await
        .expect("ExpiredObjectDeleteMarker should be accepted on object-lock bucket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn put_bucket_lifecycle_configuration_rejects_del_marker_expiration_on_object_lock_bucket() {
    let (_disk_paths, ecstore) = setup_test_env().await;

    let bucket = format!("test-lock-del-marker-exp-{}", &Uuid::new_v4().simple().to_string()[..8]);
    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_object_lock_enabled(bucket.as_str())
        .await
        .expect("Failed to enable object lock for bucket");

    let req = build_request(
        PutBucketLifecycleConfigurationInput::builder()
            .bucket(bucket.clone())
            .lifecycle_configuration(Some(del_marker_expiration_lifecycle_configuration(1)))
            .build()
            .unwrap(),
        Method::PUT,
    );

    let err: rustfs_s3_types::S3Error = FS::new()
        .put_bucket_lifecycle_configuration(req)
        .await
        .expect_err("DelMarkerExpiration must be rejected on object-lock bucket")
        .into();
    assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
    let message = err.message().unwrap_or_default();
    assert!(
        message.contains(
            "ExpiredObjectAllVersions element and DelMarkerExpiration action cannot be used on an object locked bucket"
        ),
        "unexpected error message: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn put_bucket_lifecycle_configuration_rejects_zero_day_del_marker_expiration_on_object_lock_bucket() {
    let (_disk_paths, ecstore) = setup_test_env().await;

    let bucket = format!("test-lock-del-marker-exp0-{}", &Uuid::new_v4().simple().to_string()[..8]);
    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_object_lock_enabled(bucket.as_str())
        .await
        .expect("Failed to enable object lock for bucket");

    let req = build_request(
        PutBucketLifecycleConfigurationInput::builder()
            .bucket(bucket.clone())
            .lifecycle_configuration(Some(del_marker_expiration_lifecycle_configuration(0)))
            .build()
            .unwrap(),
        Method::PUT,
    );

    let err: rustfs_s3_types::S3Error = FS::new()
        .put_bucket_lifecycle_configuration(req)
        .await
        .expect_err("DelMarkerExpiration with zero days must be rejected on object-lock bucket")
        .into();
    assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
    let message = err.message().unwrap_or_default();
    assert!(
        message.contains("Days must be a positive integer with DelMarkerExpiration"),
        "unexpected error message: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn put_bucket_lifecycle_configuration_rejects_zero_day_expiration() {
    // S3 compatibility (ceph s3-tests `test_lifecycle_expiration_days0`): a rule with
    // Expiration{Days:0} must be rejected with InvalidArgument through the real
    // PutBucketLifecycleConfiguration handler, not merely at the crates/lifecycle
    // io::Error layer. No object lock required.
    let (_disk_paths, ecstore) = setup_test_env().await;

    let bucket = format!("test-zero-day-exp-{}", &Uuid::new_v4().simple().to_string()[..8]);
    create_test_bucket(&ecstore, bucket.as_str()).await;

    let lifecycle = BucketLifecycleConfiguration {
        expiry_updated_at: None,
        rules: vec![LifecycleRule {
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            abort_incomplete_multipart_upload: None,
            del_marker_expiration: None,
            expiration: Some(LifecycleExpiration {
                days: Some(0),
                ..Default::default()
            }),
            filter: Some(LifecycleRuleFilter {
                prefix: Some("days0/".to_string()),
                ..Default::default()
            }),
            id: Some("expire-zero-days".to_string()),
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            transitions: None,
        }],
    };

    let req = build_request(
        PutBucketLifecycleConfigurationInput::builder()
            .bucket(bucket.clone())
            .lifecycle_configuration(Some(lifecycle))
            .build()
            .unwrap(),
        Method::PUT,
    );

    let err: rustfs_s3_types::S3Error = FS::new()
        .put_bucket_lifecycle_configuration(req)
        .await
        .expect_err("zero-day expiration must be rejected with InvalidArgument")
        .into();
    assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
    let message = err.message().unwrap_or_default();
    assert!(
        message.contains("'Days' for Expiration action must be a positive integer"),
        "unexpected error message: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
async fn put_bucket_lifecycle_configuration_preserves_error_priority() {
    use crate::app::storage_api::bucket_usecase::{error::StorageError, helper::MutationCompletionProbe};
    use crate::app::storage_api::owned_parity::{RequestContext, parity_test_support};
    use crate::app::storage_api::test::{get_global_bucket_metadata_sys, set_bucket_metadata};
    use crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s;
    use rustfs_s3_ops::S3Operation;

    let (_disk_paths, store) = setup_test_env().await;
    let suffix = Uuid::new_v4().simple().to_string();
    let readable = format!("lifecycle-readable-{suffix}");
    let unreadable = format!("lifecycle-unreadable-{suffix}");
    create_test_bucket(&store, &readable).await;
    create_test_bucket(&store, &unreadable).await;

    let sys = get_global_bucket_metadata_sys().unwrap();
    let cached = sys.read().await.get(&unreadable).await.unwrap();
    let mut metadata = (*cached).clone();
    metadata.object_lock_config_xml = b"<ObjectLockConfiguration>".to_vec();
    metadata.object_lock_config = None;
    set_bucket_metadata(unreadable.clone(), metadata).await.unwrap();
    let storage_error = metadata_sys::get_object_lock_config(&unreadable).await.unwrap_err();
    let lookup_error = crate::app::storage_api::s3::S3Error::from(crate::error::ApiError::from(storage_error));
    let lookup = lookup_error.message().unwrap();
    assert!(lookup.contains(OBJECT_LOCK_CONFIG) && lookup.contains(&unreadable), "{lookup}");
    assert!(matches!(
        metadata_sys::get_object_lock_config(&readable).await,
        Err(StorageError::ConfigNotFound)
    ));

    let configuration = |status, bad_date| {
        let mut configuration = expiration_lifecycle_configuration("dated/");
        let date = time::macros::datetime!(2099-01-01 00:00 UTC);
        let date = if bad_date {
            date.to_offset(time::UtcOffset::from_hms(0, 0, 1).unwrap())
        } else {
            date
        };
        configuration.rules[0].expiration.as_mut().unwrap().date = Some(Timestamp::from(date));
        configuration.rules[0].id = None;
        let mut days_rule = del_marker_expiration_lifecycle_configuration(30).rules.remove(0);
        days_rule.id = Some("rule-0".to_owned());
        days_rule.status = ExpirationStatus::from_static(status);
        configuration.rules.push(days_rule);
        configuration
    };
    let bad_date = lifecycle_configuration_from_s3s(configuration("enabled", true))
        .unwrap_err()
        .to_string();
    assert!(bad_date.contains("offset_second"), "{bad_date}");
    assert!(lifecycle_configuration_from_s3s(configuration("Disabled", false)).is_ok());

    let malformed = "Malformed XML: Rule status must be either Enabled or Disabled";
    let mismatch = "bucket config mutation snapshot does not match request bucket";
    let cases = [
        (
            "incarnation before status",
            &unreadable,
            Some(("enabled", true)),
            true,
            S3ErrorCode::InternalError,
            Some(mismatch),
        ),
        ("missing before lock lookup", &unreadable, None, false, S3ErrorCode::InvalidArgument, None),
        (
            "status before lock lookup",
            &unreadable,
            Some(("enabled", true)),
            false,
            S3ErrorCode::MalformedXML,
            Some(malformed),
        ),
        (
            "later status before first date",
            &readable,
            Some(("enabled", true)),
            false,
            S3ErrorCode::MalformedXML,
            Some(malformed),
        ),
        (
            "lock lookup before date",
            &unreadable,
            Some(("Disabled", true)),
            false,
            S3ErrorCode::ServiceUnavailable,
            Some(lookup),
        ),
        (
            "date after lock lookup",
            &readable,
            Some(("Disabled", true)),
            false,
            S3ErrorCode::InternalError,
            Some(bad_date.as_str()),
        ),
        (
            "real lock lookup failure",
            &unreadable,
            Some(("Disabled", false)),
            false,
            S3ErrorCode::ServiceUnavailable,
            Some(lookup),
        ),
    ];

    let fs = FS::new();
    let handled_count = || {
        rustfs_io_metrics::s3_op_metrics_snapshot()
            .into_iter()
            .find(|metric| metric.op == S3Operation::PutBucketLifecycleConfiguration.as_str())
            .map_or(0, |metric| metric.total)
    };
    let request = |bucket: &str, config: Option<(&'static str, bool)>| {
        let mut request = build_request(
            PutBucketLifecycleConfigurationInput::builder()
                .bucket(bucket.to_owned())
                .lifecycle_configuration(config.map(|(status, bad_date)| configuration(status, bad_date)))
                .build()
                .unwrap(),
            Method::PUT,
        );
        request.uri = format!("/{bucket}?lifecycle").parse().unwrap();
        request.extensions.insert(RequestContext {
            request_id: "lifecycle-priority-context".to_owned(),
            x_amz_request_id: "lifecycle-priority-context".to_owned(),
            trace_id: None,
            span_id: None,
            start_time: std::time::Instant::now(),
        });
        request
    };
    let assert_audit = |probe: &MutationCompletionProbe| {
        let audits = probe.audits.lock().unwrap();
        assert_eq!(audits.len(), 1, "each attempt is audited once");
        assert_eq!(audits[0].request_id.as_deref(), Some("lifecycle-priority-context"));
    };
    for (name, bucket, config, mismatched_snapshot, code, message) in cases {
        let before = handled_count();
        let probe = MutationCompletionProbe::install(bucket);
        let mut request = request(bucket, config);
        if mismatched_snapshot {
            request
                .extensions
                .insert(parity_test_support::bucket_config_mutation(&readable));
        }
        let error = fs.put_bucket_lifecycle_configuration(request).await.unwrap_err();
        assert_eq!(error.code(), &code, "{name}: {error}");
        if let Some(message) = message {
            assert_eq!(error.message(), Some(message), "{name}");
        }
        if code == S3ErrorCode::ServiceUnavailable {
            assert_eq!(error.status_code(), Some(http::StatusCode::SERVICE_UNAVAILABLE), "{name}");
            assert!(
                error.source().and_then(|err| err.downcast_ref::<StorageError>()).is_some(),
                "{name} keeps the storage cause"
            );
        }
        assert_audit(&probe);
        assert_eq!(handled_count() - before, 1, "{name} counts once");
    }
    assert!(matches!(
        metadata_sys::get_lifecycle_config(&readable).await,
        Err(StorageError::ConfigNotFound)
    ));

    let before = handled_count();
    let probe = MutationCompletionProbe::install(&readable);
    fs.put_bucket_lifecycle_configuration(request(&readable, Some(("Disabled", false))))
        .await
        .unwrap();
    assert_audit(&probe);
    assert_eq!(handled_count() - before, 1, "success counts once");

    let (stored, _) = metadata_sys::get_lifecycle_config(&readable).await.unwrap();
    let rules = stored.rules;
    assert_eq!(rules.len(), 2);
    assert_eq!((rules[0].id.as_deref(), rules[0].status.as_str()), (Some("rule-0-1"), "Enabled"));
    assert_eq!((rules[1].id.as_deref(), rules[1].status.as_str()), (Some("rule-0"), "Disabled"));
    assert_eq!(rules[0].expiration.as_ref().unwrap().date.as_deref(), Some("2099-01-01T00:00:00.000Z"));
    assert_eq!(rules[1].expiration.as_ref().unwrap().days, Some(30));
}
