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

#![cfg(test)]

//! Object Lock E2E Tests
//!
//! These tests verify the complete Object Lock workflow including:
//! - COMPLIANCE mode blocks deletion
//! - GOVERNANCE mode blocks deletion without bypass header
//! - GOVERNANCE mode allows deletion with bypass header
//! - Legal Hold blocks deletion
//! - PutObjectRetention modification restrictions
//! - Default bucket retention is applied to new objects

use super::common::*;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier, ObjectLockLegalHoldStatus, ObjectLockRetentionMode,
};
use serial_test::serial;
use tracing::info;

/// Initialize test logging
fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("e2e_test=debug,rustfs=info")
        .try_init();
}

async fn put_bucket_deny_policy(
    client: &Client,
    bucket: &str,
    sid: &str,
    action: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": sid,
            "Effect": "Deny",
            "Principal": "*",
            "Action": action,
            "Resource": format!("arn:aws:s3:::{}/*", bucket)
        }]
    })
    .to_string();

    client.put_bucket_policy().bucket(bucket).policy(policy).send().await?;
    Ok(())
}

fn retention_timestamp(days: i64) -> aws_sdk_s3::primitives::DateTime {
    let retain_until = future_retain_until(days).format("%Y-%m-%dT%H:%M:%SZ").to_string();
    aws_sdk_s3::primitives::DateTime::from_str(&retain_until, aws_sdk_s3::primitives::DateTimeFormat::DateTime)
        .expect("retention timestamp should parse")
}

fn assert_access_denied<T, E: std::fmt::Debug>(result: Result<T, E>, context: &str) {
    let err = match result {
        Ok(_) => panic!("{context}"),
        Err(err) => format!("{err:?}"),
    };
    assert!(
        err.contains("AccessDenied") || err.to_lowercase().contains("access denied"),
        "{context}: expected AccessDenied, got: {err}"
    );
}

// ============================================================================
// DeleteObject Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_delete_object_blocked_by_compliance_retention() {
    init_logging();
    info!("🧪 Test: DeleteObject blocked by COMPLIANCE retention");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-compliance-delete";
    let key = "locked-object";
    let data = b"test data for compliance mode";

    // Create bucket with Object Lock enabled
    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Put object with COMPLIANCE retention (30 days in future)
    let retain_until = future_retain_until(30);
    let version_id = put_object_with_retention(&client, bucket, key, data, ObjectLockRetentionMode::Compliance, retain_until)
        .await
        .unwrap();

    // Attempt to delete - should fail
    let delete_result = delete_object_with_bypass(&client, bucket, key, Some(&version_id), false).await;
    assert!(delete_result.is_err(), "Delete should fail for COMPLIANCE locked object");

    // Even with bypass header, COMPLIANCE should not allow deletion
    let delete_with_bypass_result = delete_object_with_bypass(&client, bucket, key, Some(&version_id), true).await;
    assert!(
        delete_with_bypass_result.is_err(),
        "Delete with bypass should still fail for COMPLIANCE mode"
    );

    info!("✅ Test passed: COMPLIANCE retention blocks deletion");
}

#[tokio::test]
#[serial]
async fn test_delete_object_blocked_by_governance_without_bypass() {
    init_logging();
    info!("🧪 Test: DeleteObject blocked by GOVERNANCE retention without bypass");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-governance-no-bypass";
    let key = "governance-locked-object";
    let data = b"test data for governance mode";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Put object with GOVERNANCE retention
    let retain_until = future_retain_until(30);
    let version_id = put_object_with_retention(&client, bucket, key, data, ObjectLockRetentionMode::Governance, retain_until)
        .await
        .unwrap();

    // Attempt to delete without bypass - should fail
    let delete_result = delete_object_with_bypass(&client, bucket, key, Some(&version_id), false).await;
    assert!(delete_result.is_err(), "Delete without bypass should fail for GOVERNANCE locked object");

    info!("✅ Test passed: GOVERNANCE retention blocks deletion without bypass");
}

#[tokio::test]
#[serial]
async fn test_delete_object_allowed_by_governance_with_bypass() {
    init_logging();
    info!("🧪 Test: DeleteObject allowed by GOVERNANCE retention with bypass");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-governance-with-bypass";
    let key = "governance-bypass-object";
    let data = b"test data for governance bypass";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Put object with GOVERNANCE retention
    let retain_until = future_retain_until(30);
    let version_id = put_object_with_retention(&client, bucket, key, data, ObjectLockRetentionMode::Governance, retain_until)
        .await
        .unwrap();

    // Delete with bypass header - should succeed
    let delete_result = delete_object_with_bypass(&client, bucket, key, Some(&version_id), true).await;
    assert!(delete_result.is_ok(), "Delete with bypass should succeed for GOVERNANCE mode");

    // Verify object is deleted
    let head_result = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await;
    assert!(head_result.is_err(), "Object should be deleted");

    info!("✅ Test passed: GOVERNANCE retention allows deletion with bypass");
}

#[tokio::test]
#[serial]
async fn test_delete_object_creates_delete_marker_for_retained_current_version() {
    init_logging();
    info!("🧪 Test: DeleteObject creates delete marker for retained current version");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-retention-delete-marker";
    let key = "retained-object";
    let data = b"test data for retained current version";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    let retain_until = future_retain_until(30);
    let retained_version_id =
        put_object_with_retention(&client, bucket, key, data, ObjectLockRetentionMode::Governance, retain_until)
            .await
            .unwrap();

    let delete_marker_output = client.delete_object().bucket(bucket).key(key).send().await.unwrap();
    assert_eq!(delete_marker_output.delete_marker(), Some(true));

    let delete_marker_version_id = delete_marker_output
        .version_id()
        .expect("delete marker should have a version id")
        .to_string();

    let protected_delete = delete_object_with_bypass(&client, bucket, key, Some(&retained_version_id), false).await;
    assert!(protected_delete.is_err(), "Retained version should still reject direct deletion");

    delete_object_with_bypass(&client, bucket, key, Some(&delete_marker_version_id), false)
        .await
        .unwrap();

    let still_protected = delete_object_with_bypass(&client, bucket, key, Some(&retained_version_id), false).await;
    assert!(
        still_protected.is_err(),
        "Retained version should remain protected after delete marker removal"
    );

    delete_object_with_bypass(&client, bucket, key, Some(&retained_version_id), true)
        .await
        .unwrap();

    info!("✅ Test passed: Delete marker is allowed while retained version stays protected");
}

#[tokio::test]
#[serial]
async fn test_delete_object_blocked_by_legal_hold() {
    init_logging();
    info!("🧪 Test: DeleteObject blocked by Legal Hold");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-legal-hold-delete";
    let key = "legal-hold-object";
    let data = b"test data for legal hold";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Put object with legal hold ON
    let version_id = put_object_with_legal_hold(&client, bucket, key, data, ObjectLockLegalHoldStatus::On)
        .await
        .unwrap();

    // Attempt to delete - should fail (legal hold cannot be bypassed)
    let delete_result = delete_object_with_bypass(&client, bucket, key, Some(&version_id), false).await;
    assert!(delete_result.is_err(), "Delete should fail for legal hold object");

    // Even with bypass header, legal hold should block deletion
    let delete_with_bypass_result = delete_object_with_bypass(&client, bucket, key, Some(&version_id), true).await;
    assert!(delete_with_bypass_result.is_err(), "Delete with bypass should still fail for legal hold");

    info!("✅ Test passed: Legal Hold blocks deletion");
}

#[tokio::test]
#[serial]
async fn test_delete_object_allowed_with_legal_hold_off() {
    init_logging();
    info!("🧪 Test: DeleteObject allowed with Legal Hold OFF");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-legal-hold-off-delete";
    let key = "legal-hold-off-object";
    let data = b"test data for legal hold off";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    let version_id = put_object_with_legal_hold(&client, bucket, key, data, ObjectLockLegalHoldStatus::Off)
        .await
        .unwrap();

    let delete_result = delete_object_with_bypass(&client, bucket, key, Some(&version_id), false).await;
    assert!(delete_result.is_ok(), "Delete should succeed when legal hold is OFF");

    let head_result = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await;
    assert!(head_result.is_err(), "Object should be deleted when legal hold is OFF");

    info!("✅ Test passed: Legal Hold OFF allows deletion");
}

#[tokio::test]
#[serial]
async fn test_delete_object_after_legal_hold_removed() {
    init_logging();
    info!("🧪 Test: DeleteObject succeeds after Legal Hold is removed");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-legal-hold-remove";
    let key = "legal-hold-remove-object";
    let data = b"test data for legal hold removal";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Put object with legal hold ON
    let version_id = put_object_with_legal_hold(&client, bucket, key, data, ObjectLockLegalHoldStatus::On)
        .await
        .unwrap();

    // Remove legal hold
    put_object_legal_hold(&client, bucket, key, Some(&version_id), ObjectLockLegalHoldStatus::Off)
        .await
        .unwrap();

    // Now deletion should succeed
    let delete_result = delete_object_with_bypass(&client, bucket, key, Some(&version_id), false).await;
    assert!(delete_result.is_ok(), "Delete should succeed after legal hold is removed");

    info!("✅ Test passed: Deletion succeeds after Legal Hold removal");
}

#[tokio::test]
#[serial]
async fn test_get_object_legal_hold_returns_updated_status() {
    init_logging();
    info!("🧪 Test: GetObjectLegalHold returns updated status");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-get-legal-hold";
    let key = "legal-hold-object";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    let version_id = put_object_with_legal_hold(&client, bucket, key, b"test data", ObjectLockLegalHoldStatus::On)
        .await
        .unwrap();

    let on_hold = client
        .get_object_legal_hold()
        .bucket(bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        on_hold
            .legal_hold()
            .and_then(|value| value.status())
            .map(|value| value.as_str()),
        Some("ON")
    );

    put_object_legal_hold(&client, bucket, key, Some(&version_id), ObjectLockLegalHoldStatus::Off)
        .await
        .unwrap();

    let off_hold = client
        .get_object_legal_hold()
        .bucket(bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        off_hold
            .legal_hold()
            .and_then(|value| value.status())
            .map(|value| value.as_str()),
        Some("OFF")
    );
}

#[tokio::test]
#[serial]
async fn test_get_object_retention_returns_configured_values() {
    init_logging();
    info!("🧪 Test: GetObjectRetention returns configured values");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-get-retention";
    let key = "retained-object";
    let retain_until = future_retain_until(30);
    let retain_until_expected = retain_until.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    let version_id =
        put_object_with_retention(&client, bucket, key, b"test data", ObjectLockRetentionMode::Governance, retain_until)
            .await
            .unwrap();

    let retention = client
        .get_object_retention()
        .bucket(bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .unwrap();
    let retention = retention.retention().expect("retention should be present");

    assert_eq!(retention.mode().map(|value| value.as_str()), Some("GOVERNANCE"));
    assert_eq!(
        retention
            .retain_until_date()
            .expect("retain_until_date should be present")
            .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
            .unwrap(),
        retain_until_expected
    );
}

// ============================================================================
// Put/Copy/Multipart Legal Hold Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_put_object_overwrite_blocked_by_legal_hold() {
    init_logging();
    info!("🧪 Test: PutObject overwrite blocked by Legal Hold");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-put-overwrite-legal-hold";
    let key = "overwrite-object";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    put_object_with_legal_hold(&client, bucket, key, b"locked-body", ObjectLockLegalHoldStatus::On)
        .await
        .unwrap();

    let overwrite_result = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(b"replacement-body".to_vec()))
        .send()
        .await;

    assert!(overwrite_result.is_err(), "PutObject overwrite should fail while legal hold is ON");

    let error_str = format!("{:?}", overwrite_result.unwrap_err());
    assert!(
        error_str.to_lowercase().contains("legal") || error_str.to_lowercase().contains("hold"),
        "overwrite error should mention legal hold, got: {error_str}"
    );
}

#[tokio::test]
#[serial]
async fn test_copy_object_applies_requested_legal_hold() {
    init_logging();
    info!("🧪 Test: CopyObject applies requested Legal Hold");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-copy-object-legal-hold";
    let src_key = "src-object";
    let dst_key = "dst-object";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    client
        .put_object()
        .bucket(bucket)
        .key(src_key)
        .body(ByteStream::from(b"copy-source".to_vec()))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .copy_source(format!("{bucket}/{src_key}"))
        .bucket(bucket)
        .key(dst_key)
        .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        .send()
        .await
        .unwrap();

    let legal_hold = client
        .get_object_legal_hold()
        .bucket(bucket)
        .key(dst_key)
        .send()
        .await
        .unwrap();

    assert_eq!(
        legal_hold
            .legal_hold()
            .and_then(|value| value.status())
            .map(|value| value.as_str()),
        Some("ON")
    );
}

#[tokio::test]
#[serial]
async fn test_copy_object_overwrite_blocked_by_legal_hold() {
    init_logging();
    info!("🧪 Test: CopyObject overwrite blocked by Legal Hold");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-copy-overwrite-legal-hold";
    let src_key = "src-object";
    let dst_key = "dst-object";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    client
        .put_object()
        .bucket(bucket)
        .key(src_key)
        .body(ByteStream::from(b"copy-source".to_vec()))
        .send()
        .await
        .unwrap();

    put_object_with_legal_hold(&client, bucket, dst_key, b"locked-destination", ObjectLockLegalHoldStatus::On)
        .await
        .unwrap();

    let copy_result = client
        .copy_object()
        .copy_source(format!("{bucket}/{src_key}"))
        .bucket(bucket)
        .key(dst_key)
        .send()
        .await;

    assert!(
        copy_result.is_err(),
        "CopyObject overwrite should fail while destination legal hold is ON"
    );

    let error_str = format!("{:?}", copy_result.unwrap_err());
    assert!(
        error_str.to_lowercase().contains("legal") || error_str.to_lowercase().contains("hold"),
        "copy overwrite error should mention legal hold, got: {error_str}"
    );
}

#[tokio::test]
#[serial]
async fn test_create_multipart_upload_applies_requested_legal_hold() {
    init_logging();
    info!("🧪 Test: CreateMultipartUpload applies requested Legal Hold");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-multipart-legal-hold";
    let key = "multipart-object";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    let create_output = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        .send()
        .await
        .unwrap();

    let upload_id = create_output.upload_id().unwrap();
    let upload_part_output = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(b"multipart-body".to_vec()))
        .send()
        .await
        .unwrap();

    let completed_upload = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(upload_part_output.e_tag().unwrap_or_default())
                .build(),
        )
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
        .unwrap();

    let legal_hold = client.get_object_legal_hold().bucket(bucket).key(key).send().await.unwrap();

    assert_eq!(
        legal_hold
            .legal_hold()
            .and_then(|value| value.status())
            .map(|value| value.as_str()),
        Some("ON")
    );
}

#[tokio::test]
#[serial]
async fn test_create_multipart_upload_blocked_by_compliance_retention() {
    init_logging();
    info!("🧪 Test: CreateMultipartUpload blocked by COMPLIANCE retention");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-multipart-create-compliance";
    let key = "protected-object";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    put_object_with_retention(
        &client,
        bucket,
        key,
        b"locked-destination",
        ObjectLockRetentionMode::Compliance,
        future_retain_until(30),
    )
    .await
    .unwrap();

    let create_result = client.create_multipart_upload().bucket(bucket).key(key).send().await;

    assert!(
        create_result.is_err(),
        "CreateMultipartUpload should fail while destination is under active COMPLIANCE retention"
    );

    let error_str = format!("{:?}", create_result.unwrap_err());
    assert!(
        error_str.to_lowercase().contains("retention") || error_str.to_lowercase().contains("compliance"),
        "multipart create error should mention retention, got: {error_str}"
    );
}

#[tokio::test]
#[serial]
async fn test_delete_completed_multipart_object_blocked_by_legal_hold() {
    init_logging();
    info!("🧪 Test: Delete completed multipart object blocked by Legal Hold");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-multipart-delete-legal-hold";
    let key = "multipart-object";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    let create_output = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        .send()
        .await
        .unwrap();

    let upload_id = create_output.upload_id().unwrap();
    let upload_part_output = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(b"multipart-body".to_vec()))
        .send()
        .await
        .unwrap();

    let completed_upload = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(upload_part_output.e_tag().unwrap_or_default())
                .build(),
        )
        .build();

    let complete_output = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
        .unwrap();

    let version_id = complete_output.version_id().expect("multipart object should be versioned");
    let delete_result = delete_object_with_bypass(&client, bucket, key, Some(version_id), false).await;
    assert!(delete_result.is_err(), "Delete should fail for multipart object protected by legal hold");
}

#[tokio::test]
#[serial]
async fn test_delete_completed_multipart_object_blocked_by_retention() {
    init_logging();
    info!("🧪 Test: Delete completed multipart object blocked by retention");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-multipart-delete-retention";
    let key = "multipart-object";
    let retain_until = retention_timestamp(30);

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    let create_output = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Compliance)
        .object_lock_retain_until_date(retain_until)
        .send()
        .await
        .unwrap();

    let upload_id = create_output.upload_id().unwrap();
    let upload_part_output = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(b"multipart-body".to_vec()))
        .send()
        .await
        .unwrap();

    let completed_upload = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(upload_part_output.e_tag().unwrap_or_default())
                .build(),
        )
        .build();

    let complete_output = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
        .unwrap();

    let version_id = complete_output.version_id().expect("multipart object should be versioned");
    let delete_result = delete_object_with_bypass(&client, bucket, key, Some(version_id), false).await;
    assert!(delete_result.is_err(), "Delete should fail for multipart object protected by retention");
}

#[tokio::test]
#[serial]
async fn test_complete_multipart_upload_blocked_when_legal_hold_added_after_create() {
    init_logging();
    info!("🧪 Test: CompleteMultipartUpload blocked when Legal Hold appears after MPU creation");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-complete-multipart-legal-hold";
    let key = "multipart-race-object";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    let create_output = client.create_multipart_upload().bucket(bucket).key(key).send().await.unwrap();

    let upload_id = create_output.upload_id().unwrap();
    let upload_part_output = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(b"multipart-body".to_vec()))
        .send()
        .await
        .unwrap();

    put_object_with_legal_hold(&client, bucket, key, b"locked-current-version", ObjectLockLegalHoldStatus::On)
        .await
        .unwrap();

    let completed_upload = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(upload_part_output.e_tag().unwrap_or_default())
                .build(),
        )
        .build();

    let complete_result = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await;

    assert!(complete_result.is_err(), "CompleteMultipartUpload should fail once legal hold is enabled");

    let error_str = format!("{:?}", complete_result.unwrap_err());
    assert!(
        error_str.to_lowercase().contains("legal") || error_str.to_lowercase().contains("hold"),
        "complete error should mention legal hold, got: {error_str}"
    );
}

#[tokio::test]
#[serial]
async fn test_complete_multipart_upload_blocked_when_compliance_retention_added_after_create() {
    init_logging();
    info!("🧪 Test: CompleteMultipartUpload blocked when COMPLIANCE retention appears after MPU creation");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-complete-multipart-compliance";
    let key = "multipart-race-object";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    let create_output = client.create_multipart_upload().bucket(bucket).key(key).send().await.unwrap();

    let upload_id = create_output.upload_id().unwrap();
    let upload_part_output = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(b"multipart-body".to_vec()))
        .send()
        .await
        .unwrap();

    put_object_with_retention(
        &client,
        bucket,
        key,
        b"locked-current-version",
        ObjectLockRetentionMode::Compliance,
        future_retain_until(30),
    )
    .await
    .unwrap();

    let completed_upload = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(upload_part_output.e_tag().unwrap_or_default())
                .build(),
        )
        .build();

    let complete_result = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await;

    assert!(
        complete_result.is_err(),
        "CompleteMultipartUpload should fail once COMPLIANCE retention is enabled"
    );

    let error_str = format!("{:?}", complete_result.unwrap_err());
    assert!(
        error_str.to_lowercase().contains("retention") || error_str.to_lowercase().contains("compliance"),
        "complete error should mention retention, got: {error_str}"
    );
}

#[tokio::test]
#[serial]
async fn test_write_paths_require_put_object_legal_hold_permission() {
    init_logging();
    info!("🧪 Test: write paths require PutObjectLegalHold permission");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-legal-hold-permissions";
    let src_key = "src-object";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    client
        .put_object()
        .bucket(bucket)
        .key(src_key)
        .body(ByteStream::from(b"copy-source".to_vec()))
        .send()
        .await
        .unwrap();

    put_bucket_deny_policy(&client, bucket, "DenyPutObjectLegalHold", "s3:PutObjectLegalHold")
        .await
        .unwrap();

    assert_access_denied(
        client
            .put_object()
            .bucket(bucket)
            .key("put-target")
            .body(ByteStream::from(b"put-body".to_vec()))
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await,
        "PutObject with legal hold should require s3:PutObjectLegalHold",
    );

    assert_access_denied(
        client
            .copy_object()
            .copy_source(format!("{bucket}/{src_key}"))
            .bucket(bucket)
            .key("copy-target")
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await,
        "CopyObject with legal hold should require s3:PutObjectLegalHold",
    );

    assert_access_denied(
        client
            .create_multipart_upload()
            .bucket(bucket)
            .key("multipart-target")
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await,
        "CreateMultipartUpload with legal hold should require s3:PutObjectLegalHold",
    );
}

#[tokio::test]
#[serial]
async fn test_write_paths_require_put_object_retention_permission() {
    init_logging();
    info!("🧪 Test: write paths require PutObjectRetention permission");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-retention-permissions";
    let src_key = "src-object";
    let retain_until = retention_timestamp(30);

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();
    client
        .put_object()
        .bucket(bucket)
        .key(src_key)
        .body(ByteStream::from(b"copy-source".to_vec()))
        .send()
        .await
        .unwrap();

    put_bucket_deny_policy(&client, bucket, "DenyPutObjectRetention", "s3:PutObjectRetention")
        .await
        .unwrap();

    assert_access_denied(
        client
            .put_object()
            .bucket(bucket)
            .key("put-target")
            .body(ByteStream::from(b"put-body".to_vec()))
            .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .send()
            .await,
        "PutObject with retention should require s3:PutObjectRetention",
    );

    assert_access_denied(
        client
            .copy_object()
            .copy_source(format!("{bucket}/{src_key}"))
            .bucket(bucket)
            .key("copy-target")
            .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .send()
            .await,
        "CopyObject with retention should require s3:PutObjectRetention",
    );

    assert_access_denied(
        client
            .create_multipart_upload()
            .bucket(bucket)
            .key("multipart-target")
            .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .send()
            .await,
        "CreateMultipartUpload with retention should require s3:PutObjectRetention",
    );
}

// ============================================================================
// DeleteObjects (Batch Delete) Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_delete_objects_mixed_locked_unlocked() {
    init_logging();
    info!("🧪 Test: DeleteObjects with mixed locked and unlocked objects");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-batch-delete-mixed";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Put unlocked object
    let unlocked_key = "unlocked-object";
    client
        .put_object()
        .bucket(bucket)
        .key(unlocked_key)
        .body(ByteStream::from(b"unlocked data".to_vec()))
        .send()
        .await
        .unwrap();

    // Put locked object with COMPLIANCE
    let locked_key = "locked-object";
    let retain_until = future_retain_until(30);
    let locked_version = put_object_with_retention(
        &client,
        bucket,
        locked_key,
        b"locked data",
        ObjectLockRetentionMode::Compliance,
        retain_until,
    )
    .await
    .unwrap();

    // Batch delete both objects
    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key(unlocked_key).build().unwrap())
        .objects(
            ObjectIdentifier::builder()
                .key(locked_key)
                .version_id(&locked_version)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    let result = client.delete_objects().bucket(bucket).delete(delete).send().await.unwrap();

    // Unlocked object should be deleted
    let deleted_count = result.deleted().len();
    let error_count = result.errors().len();

    info!("Deleted: {}, Errors: {}", deleted_count, error_count);

    // Should have 1 successful delete (unlocked) and 1 error (locked)
    assert_eq!(deleted_count, 1, "One object should be deleted");
    assert_eq!(error_count, 1, "One object should have error (locked)");

    // Verify locked object still exists
    let head_result = client
        .head_object()
        .bucket(bucket)
        .key(locked_key)
        .version_id(&locked_version)
        .send()
        .await;
    assert!(head_result.is_ok(), "Locked object should still exist");

    info!("✅ Test passed: Batch delete correctly handles mixed locked/unlocked objects");
}

// ============================================================================
// PutObjectRetention Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_put_retention_compliance_cannot_shorten() {
    init_logging();
    info!("🧪 Test: PutObjectRetention cannot shorten COMPLIANCE retention");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-retention-shorten";
    let key = "compliance-shorten-object";
    let data = b"test data";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Put object with COMPLIANCE retention for 60 days
    let retain_until_60 = future_retain_until(60);
    let version_id = put_object_with_retention(&client, bucket, key, data, ObjectLockRetentionMode::Compliance, retain_until_60)
        .await
        .unwrap();

    // Try to shorten to 30 days - should fail
    let retain_until_30 = future_retain_until(30);
    let shorten_result = put_object_retention(
        &client,
        bucket,
        key,
        Some(&version_id),
        ObjectLockRetentionMode::Compliance,
        retain_until_30,
        false,
    )
    .await;

    assert!(shorten_result.is_err(), "Shortening COMPLIANCE retention should fail");

    info!("✅ Test passed: Cannot shorten COMPLIANCE retention");
}

#[tokio::test]
#[serial]
async fn test_put_retention_compliance_can_extend() {
    init_logging();
    info!("🧪 Test: PutObjectRetention can extend COMPLIANCE retention");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-retention-extend";
    let key = "compliance-extend-object";
    let data = b"test data";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Put object with COMPLIANCE retention for 30 days
    let retain_until_30 = future_retain_until(30);
    let version_id = put_object_with_retention(&client, bucket, key, data, ObjectLockRetentionMode::Compliance, retain_until_30)
        .await
        .unwrap();

    // Extend to 60 days - should succeed
    let retain_until_60 = future_retain_until(60);
    let extend_result = put_object_retention(
        &client,
        bucket,
        key,
        Some(&version_id),
        ObjectLockRetentionMode::Compliance,
        retain_until_60,
        false,
    )
    .await;

    assert!(extend_result.is_ok(), "Extending COMPLIANCE retention should succeed");

    info!("✅ Test passed: Can extend COMPLIANCE retention");
}

#[tokio::test]
#[serial]
async fn test_put_retention_governance_extend_without_bypass() {
    init_logging();
    info!("🧪 Test: PutObjectRetention on GOVERNANCE can extend without bypass");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-governance-extend";
    let key = "governance-extend-object";
    let data = b"test data";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Put object with GOVERNANCE retention for 30 days
    let retain_until_30 = future_retain_until(30);
    let version_id = put_object_with_retention(&client, bucket, key, data, ObjectLockRetentionMode::Governance, retain_until_30)
        .await
        .unwrap();

    // Extend to 60 days without bypass - should succeed (AWS S3 behavior)
    let retain_until_60 = future_retain_until(60);
    let extend_without_bypass = put_object_retention(
        &client,
        bucket,
        key,
        Some(&version_id),
        ObjectLockRetentionMode::Governance,
        retain_until_60,
        false,
    )
    .await;

    assert!(
        extend_without_bypass.is_ok(),
        "Extending GOVERNANCE retention without bypass should succeed"
    );

    info!("✅ Test passed: GOVERNANCE retention can be extended without bypass");
}

#[tokio::test]
#[serial]
async fn test_put_retention_governance_shorten_requires_bypass() {
    init_logging();
    info!("🧪 Test: PutObjectRetention on GOVERNANCE requires bypass to shorten");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-governance-shorten";
    let key = "governance-shorten-object";
    let data = b"test data";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Put object with GOVERNANCE retention for 60 days
    let retain_until_60 = future_retain_until(60);
    let version_id = put_object_with_retention(&client, bucket, key, data, ObjectLockRetentionMode::Governance, retain_until_60)
        .await
        .unwrap();

    // Try to shorten to 30 days without bypass - should fail
    let retain_until_30 = future_retain_until(30);
    let shorten_without_bypass = put_object_retention(
        &client,
        bucket,
        key,
        Some(&version_id),
        ObjectLockRetentionMode::Governance,
        retain_until_30,
        false,
    )
    .await;

    assert!(
        shorten_without_bypass.is_err(),
        "Shortening GOVERNANCE retention without bypass should fail"
    );

    // Shorten with bypass - should succeed
    let shorten_with_bypass = put_object_retention(
        &client,
        bucket,
        key,
        Some(&version_id),
        ObjectLockRetentionMode::Governance,
        retain_until_30,
        true,
    )
    .await;

    assert!(shorten_with_bypass.is_ok(), "Shortening GOVERNANCE retention with bypass should succeed");

    info!("✅ Test passed: GOVERNANCE retention shortening requires bypass");
}

// ============================================================================
// Default Retention Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_default_retention_applied_to_new_objects() {
    init_logging();
    info!("🧪 Test: Default retention is applied to new objects");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-default-retention";
    let key = "object-with-default-retention";
    let data = b"test data";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Set default retention: GOVERNANCE for 30 days
    put_object_lock_configuration(&client, bucket, ObjectLockRetentionMode::Governance, Some(30), None)
        .await
        .unwrap();

    // Put object without explicit retention
    let response = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await
        .unwrap();

    let version_id = response.version_id().unwrap();

    // Try to delete without bypass - should fail due to default retention
    let delete_result = delete_object_with_bypass(&client, bucket, key, Some(version_id), false).await;
    assert!(delete_result.is_err(), "Delete should fail for object with default retention applied");

    info!("✅ Test passed: Default retention is applied to new objects");
}

// ============================================================================
// Versioning Auto-Enable Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_versioning_auto_enabled_with_object_lock() {
    init_logging();
    info!("🧪 Test: Versioning is auto-enabled when Object Lock is configured");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-versioning-auto-enable";

    // Create bucket with Object Lock enabled
    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Check versioning status - should be Enabled
    let versioning = client.get_bucket_versioning().bucket(bucket).send().await.unwrap();

    // Object Lock buckets must have versioning enabled
    // Note: Some S3 implementations may report MfaDelete status as well
    let status = versioning.status();
    info!("Versioning status: {:?}", status);

    // Put an object and verify it gets a version ID
    let key = "versioned-object";
    let response = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();

    let version1 = response.version_id();
    assert!(version1.is_some(), "Object should have a version ID");

    // Put another version
    let response2 = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(b"v2".to_vec()))
        .send()
        .await
        .unwrap();

    let version2 = response2.version_id();
    assert!(version2.is_some(), "Second object should have a version ID");
    assert_ne!(version1, version2, "Version IDs should be different");

    info!("✅ Test passed: Versioning is auto-enabled with Object Lock");
}

// ============================================================================
// Error Message Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_error_message_distinguishes_legal_hold_from_retention() {
    init_logging();
    info!("🧪 Test: Error messages distinguish Legal Hold from Retention");

    let mut env = ObjectLockTestEnvironment::new().await.unwrap();
    env.start_rustfs().await.unwrap();

    let bucket = "test-error-messages";

    env.create_object_lock_bucket(bucket).await.unwrap();

    let client = env.s3_client();

    // Put object with legal hold
    let legal_hold_key = "legal-hold-object";
    let lh_version = put_object_with_legal_hold(&client, bucket, legal_hold_key, b"data", ObjectLockLegalHoldStatus::On)
        .await
        .unwrap();

    // Put object with retention
    let retention_key = "retention-object";
    let retain_until = future_retain_until(30);
    let ret_version =
        put_object_with_retention(&client, bucket, retention_key, b"data", ObjectLockRetentionMode::Compliance, retain_until)
            .await
            .unwrap();

    // Delete legal hold object - check error
    let lh_delete_result = client
        .delete_object()
        .bucket(bucket)
        .key(legal_hold_key)
        .version_id(&lh_version)
        .send()
        .await;

    if let Err(e) = lh_delete_result {
        let error_str = format!("{:?}", e);
        info!("Legal hold delete error: {}", error_str);
        // Error should mention legal hold
        assert!(
            error_str.to_lowercase().contains("legal") || error_str.to_lowercase().contains("hold"),
            "Error should mention legal hold"
        );
    }

    // Delete retention object - check error
    let ret_delete_result = client
        .delete_object()
        .bucket(bucket)
        .key(retention_key)
        .version_id(&ret_version)
        .send()
        .await;

    if let Err(e) = ret_delete_result {
        let error_str = format!("{:?}", e);
        info!("Retention delete error: {}", error_str);
        // Error should mention retention
        assert!(
            error_str.to_lowercase().contains("retention") || error_str.to_lowercase().contains("compliance"),
            "Error should mention retention"
        );
    }

    info!("✅ Test passed: Error messages distinguish lock types");
}
