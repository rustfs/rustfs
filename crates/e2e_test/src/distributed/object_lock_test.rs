// Copyright 2026 RustFS Team
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

use super::harness::{DistCluster, DistLayout, TestResult, unique_bucket};
use crate::common::init_logging;
use crate::object_lock::common::{
    delete_object_with_bypass, put_object_lock_configuration, put_object_with_legal_hold, put_object_with_retention,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHoldStatus, ObjectLockRetentionMode,
    ObjectLockRule,
};
use chrono::{Duration as ChronoDuration, Utc};

fn delete_denied(error: &SdkError<DeleteObjectError>, context: &str) -> TestResult {
    let code = error.as_service_error().and_then(ProvideErrorMetadata::code);
    if code == Some("AccessDenied") {
        Ok(())
    } else {
        Err(format!("{context}: expected AccessDenied, got {error:?}").into())
    }
}

async fn expect_versioned_delete_denied(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: &str,
    bypass: bool,
    context: &str,
) -> TestResult {
    match delete_object_with_bypass(client, bucket, key, Some(version_id), bypass).await {
        Ok(_) => Err(format!("{context}: DeleteObject of retained version must be denied").into()),
        Err(error) => delete_denied(error.as_ref(), context),
    }
}

#[tokio::test]
async fn four_node_four_drive_object_lock_worm_blocks_delete() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let client = dist.client(0)?;
    let peer = dist.client(2)?;
    let bucket = unique_bucket("objlock");

    client
        .create_bucket()
        .bucket(&bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;

    let retain_until = Utc::now() + ChronoDuration::days(1);

    let compliance_key = "compliance.bin";
    let compliance_version = put_object_with_retention(
        &client,
        &bucket,
        compliance_key,
        b"locked-compliance",
        ObjectLockRetentionMode::Compliance,
        retain_until,
    )
    .await?;

    // Unversioned DELETE is allowed: it only creates a delete marker. WORM
    // applies to a specific version id.
    let marker = peer.delete_object().bucket(&bucket).key(compliance_key).send().await?;
    assert_eq!(
        marker.delete_marker(),
        Some(true),
        "unversioned DELETE on a locked object must create a delete marker"
    );

    expect_versioned_delete_denied(&peer, &bucket, compliance_key, &compliance_version, false, "COMPLIANCE without bypass")
        .await?;
    expect_versioned_delete_denied(&peer, &bucket, compliance_key, &compliance_version, true, "COMPLIANCE with bypass").await?;

    let governance_key = "governance.bin";
    let governance_version = put_object_with_retention(
        &client,
        &bucket,
        governance_key,
        b"locked-governance",
        ObjectLockRetentionMode::Governance,
        retain_until,
    )
    .await?;

    expect_versioned_delete_denied(&peer, &bucket, governance_key, &governance_version, false, "GOVERNANCE without bypass")
        .await?;
    delete_object_with_bypass(&peer, &bucket, governance_key, Some(&governance_version), true).await?;
    let deleted_governance = peer
        .head_object()
        .bucket(&bucket)
        .key(governance_key)
        .version_id(&governance_version)
        .send()
        .await
        .expect_err("GOVERNANCE bypass must remove the retained version");
    assert_eq!(
        deleted_governance.raw_response().map(|response| response.status().as_u16()),
        Some(404),
        "deleted GOVERNANCE version returned an unexpected HEAD result: {deleted_governance:?}"
    );

    let hold_key = "legal-hold.bin";
    let hold_version =
        put_object_with_legal_hold(&client, &bucket, hold_key, b"legal-hold", ObjectLockLegalHoldStatus::On).await?;
    expect_versioned_delete_denied(&peer, &bucket, hold_key, &hold_version, false, "legal hold without bypass").await?;
    expect_versioned_delete_denied(&peer, &bucket, hold_key, &hold_version, true, "legal hold with bypass").await?;

    Ok(())
}

#[tokio::test]
async fn four_node_default_retention_is_visible_and_non_lock_bucket_rejects_configuration() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let writer = dist.client(0)?;
    let reader = dist.client(3)?;
    let bucket = unique_bucket("default-lock");

    writer
        .create_bucket()
        .bucket(&bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    put_object_lock_configuration(&writer, &bucket, ObjectLockRetentionMode::Governance, Some(1), None).await?;

    let key = "default-governance.bin";
    let put = writer
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"default retention payload"))
        .send()
        .await?;
    let version_id = put.version_id().ok_or("default-retained PUT omitted version ID")?;

    let config = reader.get_object_lock_configuration().bucket(&bucket).send().await?;
    let default_retention = config
        .object_lock_configuration()
        .and_then(|configuration| configuration.rule())
        .and_then(|rule| rule.default_retention())
        .ok_or("GetObjectLockConfiguration omitted default retention")?;
    assert_eq!(default_retention.mode().map(|mode| mode.as_str()), Some("GOVERNANCE"));
    assert_eq!(default_retention.days(), Some(1));

    let retention = reader
        .get_object_retention()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .send()
        .await?;
    let retention = retention.retention().ok_or("GetObjectRetention omitted applied retention")?;
    assert_eq!(retention.mode().map(|mode| mode.as_str()), Some("GOVERNANCE"));
    let retain_until = retention
        .retain_until_date()
        .ok_or("default retention omitted retain-until date")?;
    assert!(retain_until.secs() > Utc::now().timestamp(), "default retention is not in the future");

    let versioning = reader.get_bucket_versioning().bucket(&bucket).send().await?;
    assert_eq!(versioning.status().map(|status| status.as_str()), Some("Enabled"));
    expect_versioned_delete_denied(&reader, &bucket, key, version_id, false, "default GOVERNANCE retention without bypass")
        .await?;

    let plain_bucket = unique_bucket("no-lock");
    dist.create_bucket(&plain_bucket).await?;
    let configuration = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .days(1)
                        .build(),
                )
                .build(),
        )
        .build();
    let error = writer
        .put_object_lock_configuration()
        .bucket(&plain_bucket)
        .object_lock_configuration(configuration)
        .send()
        .await
        .expect_err("an unversioned bucket must reject Object Lock enablement");
    let service_error = error
        .as_service_error()
        .ok_or("non-lock bucket rejection was not an S3 service error")?;
    assert_eq!(service_error.code(), Some("InvalidBucketState"), "unexpected error: {error:?}");
    assert_eq!(
        service_error.message(),
        Some("Object Lock configuration cannot be enabled on existing buckets"),
        "unexpected error: {error:?}"
    );

    Ok(())
}
