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
    let bucket_usecase = DefaultBucketUsecase::from_global();

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

    Box::pin(usecase.execute_delete_object(req))
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
    let prepared = req
        .input
        .lifecycle_configuration
        .clone()
        .map(crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s)
        .transpose();
    bucket_usecase
        .execute_put_bucket_lifecycle_configuration(req, prepared)
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
    let usecase = DefaultBucketUsecase::from_global();

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

    let prepared = req
        .input
        .lifecycle_configuration
        .clone()
        .map(crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s)
        .transpose();
    usecase
        .execute_put_bucket_lifecycle_configuration(req, prepared)
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
    let usecase = DefaultBucketUsecase::from_global();

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

    let prepared = req
        .input
        .lifecycle_configuration
        .clone()
        .map(crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s)
        .transpose();
    usecase
        .execute_put_bucket_lifecycle_configuration(req, prepared)
        .await
        .expect("ExpiredObjectDeleteMarker should be accepted on object-lock bucket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn put_bucket_lifecycle_configuration_rejects_del_marker_expiration_on_object_lock_bucket() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultBucketUsecase::from_global();

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

    let prepared = req
        .input
        .lifecycle_configuration
        .clone()
        .map(crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s)
        .transpose();
    let err = usecase
        .execute_put_bucket_lifecycle_configuration(req, prepared)
        .await
        .expect_err("DelMarkerExpiration must be rejected on object-lock bucket");
    assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
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
    let usecase = DefaultBucketUsecase::from_global();

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

    let prepared = req
        .input
        .lifecycle_configuration
        .clone()
        .map(crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s)
        .transpose();
    let err = usecase
        .execute_put_bucket_lifecycle_configuration(req, prepared)
        .await
        .expect_err("DelMarkerExpiration with zero days must be rejected on object-lock bucket");
    assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
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
    let usecase = DefaultBucketUsecase::from_global();

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

    let prepared = req
        .input
        .lifecycle_configuration
        .clone()
        .map(crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s)
        .transpose();
    let err = usecase
        .execute_put_bucket_lifecycle_configuration(req, prepared)
        .await
        .expect_err("zero-day expiration must be rejected with InvalidArgument");
    assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    let message = err.message().unwrap_or_default();
    assert!(
        message.contains("'Days' for Expiration action must be a positive integer"),
        "unexpected error message: {message}"
    );
}
