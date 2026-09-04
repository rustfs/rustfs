// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::harness::{DistCluster, DistLayout, TestResult, put_object, unique_bucket};
use crate::common::init_logging;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{
    DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold, ObjectLockLegalHoldStatus,
    ObjectLockRetention, ObjectLockRetentionMode, ObjectLockRule,
};
use chrono::{Duration as ChronoDuration, Utc};

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
    let retain_until_s3 = aws_sdk_s3::primitives::DateTime::from_secs(retain_until.timestamp());

    client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(
            ObjectLockConfiguration::builder()
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
                .build(),
        )
        .send()
        .await?;

    let compliance_key = "compliance.bin";
    put_object(&client, &bucket, compliance_key, b"locked-compliance".to_vec()).await?;
    client
        .put_object_retention()
        .bucket(&bucket)
        .key(compliance_key)
        .retention(
            ObjectLockRetention::builder()
                .mode(ObjectLockRetentionMode::Compliance)
                .retain_until_date(retain_until_s3)
                .build(),
        )
        .send()
        .await?;

    let compliance_delete = peer.delete_object().bucket(&bucket).key(compliance_key).send().await;
    match compliance_delete {
        Ok(_) => return Err("COMPLIANCE retention must block DeleteObject".into()),
        Err(error) => {
            let code = error.as_service_error().and_then(ProvideErrorMetadata::code);
            assert_eq!(code, Some("AccessDenied"), "unexpected COMPLIANCE delete error: {error:?}");
        }
    }

    let governance_key = "governance.bin";
    put_object(&client, &bucket, governance_key, b"locked-governance".to_vec()).await?;
    client
        .put_object_retention()
        .bucket(&bucket)
        .key(governance_key)
        .retention(
            ObjectLockRetention::builder()
                .mode(ObjectLockRetentionMode::Governance)
                .retain_until_date(retain_until_s3)
                .build(),
        )
        .send()
        .await?;

    let governance_blocked = peer.delete_object().bucket(&bucket).key(governance_key).send().await;
    match governance_blocked {
        Ok(_) => return Err("GOVERNANCE retention must block DeleteObject without bypass".into()),
        Err(error) => {
            let code = error.as_service_error().and_then(ProvideErrorMetadata::code);
            assert_eq!(code, Some("AccessDenied"), "unexpected GOVERNANCE delete error: {error:?}");
        }
    }

    peer.delete_object()
        .bucket(&bucket)
        .key(governance_key)
        .bypass_governance_retention(true)
        .send()
        .await?;

    let hold_key = "legal-hold.bin";
    put_object(&client, &bucket, hold_key, b"legal-hold".to_vec()).await?;
    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(hold_key)
        .legal_hold(ObjectLockLegalHold::builder().status(ObjectLockLegalHoldStatus::On).build())
        .send()
        .await?;
    let hold_delete = peer.delete_object().bucket(&bucket).key(hold_key).send().await;
    match hold_delete {
        Ok(_) => return Err("legal hold must block DeleteObject".into()),
        Err(error) => {
            let code = error.as_service_error().and_then(ProvideErrorMetadata::code);
            assert_eq!(code, Some("AccessDenied"), "unexpected legal-hold delete error: {error:?}");
        }
    }

    Ok(())
}
