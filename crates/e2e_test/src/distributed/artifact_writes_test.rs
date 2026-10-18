// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

#![cfg(unix)]

use super::artifact_delete_test::physical_snapshot;
use super::artifact_repair_test::{RepairObject, finish_heal, start_heal};
use super::artifact_seal_test::{ArtifactAdmin, assert_exact_version, confirm_seal, create_artifact_attempt};
use super::harness::{
    DistCluster, DistLayout, TestResult, enable_versioning, s3_probe_client, unique_bucket, wait_for_ready, wait_until,
};
use crate::common::init_logging;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{
    ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetention,
    ObjectLockRetentionMode,
};
use http::StatusCode;
use serde_json::{Value, json};
use std::path::Path;
use std::time::Duration;

const WRITES_ROUTE: &str = "/rustfs/admin/v3/plugin-artifacts/seal-writes";
const INTERNAL_MESSAGE: &str = "We encountered an internal error, please try again.";

async fn seal_writes(dist: &DistCluster, user: &ArtifactAdmin, target: &Value, confirmed: bool) -> TestResult {
    wait_until(Duration::from_secs(45), || async {
        let (status, response) = user.request(dist, 0, WRITES_ROUTE, json!({"version":1,"target":target}).to_string()).await?;
        if status == StatusCode::SERVICE_UNAVAILABLE && response.contains("artifact operation unconfirmed; retry the same identity") {
            return Ok(!confirmed);
        }
        assert!(confirmed, "offline configured receiver produced a complete receipt: {status} {response}");
        assert_eq!(status, StatusCode::OK, "combined writes seal: {response}");
        assert_eq!(serde_json::from_str::<Value>(&response)?, json!({"version":1,"target":target,"ordinary_put_sealed":true,"attempt_repairs_sealed":true,"all_writes_sealed":true}));
        Ok(true)
    }, "combined seal acknowledges both domains on every receiver").await
}

async fn assert_put_closed(dist: &DistCluster, bucket: &str, key: &str) -> TestResult {
    let before = physical_snapshot(dist, bucket)?;
    let client = s3_probe_client(&dist.client(2)?);
    let error = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(b"must-not-publish".to_vec().into())
        .send()
        .await
        .expect_err("combined seal admitted a new ordinary PUT");
    let service = error.as_service_error().ok_or("PUT was not a service rejection")?;
    assert_eq!(service.code(), Some("InternalError"));
    assert_eq!(service.message(), Some(INTERNAL_MESSAGE));
    assert!(
        physical_snapshot(dist, bucket)? == before,
        "rejected PUT changed the managed physical namespace"
    );
    Ok(())
}

async fn lock_metadata_entrypoints(dist: &DistCluster, managed: &Value, object: &RepairObject, ordinary: &str) -> TestResult {
    let bucket = managed["bucket"].as_str().ok_or("bucket identity")?;
    let client = s3_probe_client(&dist.client(0)?);
    for bucket in [bucket, ordinary] {
        client
            .put_object_lock_configuration()
            .bucket(bucket)
            .object_lock_configuration(
                ObjectLockConfiguration::builder()
                    .object_lock_enabled(ObjectLockEnabled::Enabled)
                    .build(),
            )
            .send()
            .await?;
        wait_until(
            Duration::from_secs(30),
            || async {
                for node in 0..4 {
                    let output = match dist.client(node)?.get_object_lock_configuration().bucket(bucket).send().await {
                        Ok(output) => output,
                        Err(error)
                            if error.as_service_error().and_then(ProvideErrorMetadata::code)
                                == Some("ObjectLockConfigurationNotFoundError") =>
                        {
                            return Ok(false);
                        }
                        Err(error) => return Err(error.into()),
                    };
                    if output
                        .object_lock_configuration()
                        .and_then(|configuration| configuration.object_lock_enabled())
                        != Some(&ObjectLockEnabled::Enabled)
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            },
            "actual Object Lock configuration is enabled on all request nodes",
        )
        .await?;
    }
    let control = RepairObject::put(dist, ordinary, "lock-control.bin".to_owned()).await?;
    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_sdk_s3::primitives::DateTime::from_secs(chrono::Utc::now().timestamp() + 86400))
        .build();
    let hold = ObjectLockLegalHold::builder().status(ObjectLockLegalHoldStatus::On).build();
    client
        .put_object_retention()
        .bucket(ordinary)
        .key(&control.key)
        .version_id(&control.version)
        .retention(retention.clone())
        .send()
        .await?;
    assert_eq!(
        client
            .get_object_retention()
            .bucket(ordinary)
            .key(&control.key)
            .version_id(&control.version)
            .send()
            .await?
            .retention()
            .and_then(|value| value.mode()),
        Some(&ObjectLockRetentionMode::Governance)
    );
    client
        .put_object_legal_hold()
        .bucket(ordinary)
        .key(&control.key)
        .version_id(&control.version)
        .legal_hold(hold.clone())
        .send()
        .await?;
    assert_eq!(
        client
            .get_object_legal_hold()
            .bucket(ordinary)
            .key(&control.key)
            .version_id(&control.version)
            .send()
            .await?
            .legal_hold()
            .and_then(|value| value.status()),
        Some(&ObjectLockLegalHoldStatus::On)
    );
    let before = physical_snapshot(dist, bucket)?;
    let error = client
        .put_object_retention()
        .bucket(bucket)
        .key(&object.key)
        .version_id(&object.version)
        .retention(retention)
        .send()
        .await
        .expect_err("managed Retention must reach and fail its unsupported metadata write");
    let service = error.as_service_error().ok_or("retention was not a service rejection")?;
    assert_eq!(service.code(), Some("InternalError"));
    assert_eq!(service.message(), Some(INTERNAL_MESSAGE));
    assert!(physical_snapshot(dist, bucket)? == before, "retention changed managed metadata");
    let error = client
        .put_object_legal_hold()
        .bucket(bucket)
        .key(&object.key)
        .version_id(&object.version)
        .legal_hold(hold)
        .send()
        .await
        .expect_err("managed LegalHold must reach and fail its unsupported metadata write");
    let service = error.as_service_error().ok_or("legal hold was not a service rejection")?;
    assert_eq!(service.code(), Some("InternalError"));
    assert_eq!(service.message(), Some("Io error: artifact bucket does not support this write operation"));
    assert!(physical_snapshot(dist, bucket)? == before, "legal hold changed managed metadata");
    Ok(())
}

#[tokio::test]
async fn four_node_artifact_writes_seal_first_partial_retry_offline_and_restart() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::FourNodeFourDisk).await?;
    let ordinary = unique_bucket("writes-control");
    dist.create_bucket(&ordinary).await?;
    enable_versioning(&dist.client(0)?, &ordinary).await?;
    let (fresh, fresh_user) = create_artifact_attempt(&dist).await?;
    let fresh_bucket = fresh["bucket"].as_str().ok_or("fresh bucket")?;
    let fresh_prefix = fresh["attempt_prefix"].as_str().ok_or("fresh prefix")?;
    let first = RepairObject::put(&dist, fresh_bucket, format!("{fresh_prefix}existing.bin")).await?;
    lock_metadata_entrypoints(&dist, &fresh, &first, &ordinary).await?;
    seal_writes(&dist, &fresh_user, &fresh, true).await?;
    seal_writes(&dist, &fresh_user, &fresh, true).await?;
    assert_put_closed(&dist, fresh_bucket, &format!("{fresh_prefix}closed.bin")).await?;

    let (partial, partial_user) = create_artifact_attempt(&dist).await?;
    let bucket = partial["bucket"].as_str().ok_or("partial bucket")?;
    let prefix = partial["attempt_prefix"].as_str().ok_or("partial prefix")?;
    let repaired = RepairObject::put(&dist, bucket, format!("{prefix}repair.bin")).await?;
    // Enter the combined operation with only its ordinary domain closed.
    confirm_seal(&dist, &partial_user, &json!({"version":1,"target":partial}).to_string(), &partial).await?;
    let part = repaired.part(Path::new(&dist.cluster.nodes[3].data_dirs[0]), bucket, 3)?;
    std::fs::remove_file(&part)?;
    let token = start_heal(&dist, bucket, &repaired.key, false).await?;
    repaired.assert_repaired(&dist, bucket).await?;
    finish_heal(&dist, &token, None).await?;
    for _ in 0..2 {
        dist.cluster.stop_node(3)?;
        seal_writes(&dist, &partial_user, &partial, false).await?;
        dist.cluster.start_node(3).await?;
        wait_for_ready(&dist.cluster).await?;
        seal_writes(&dist, &partial_user, &partial, true).await?;
    }
    let ordinary_object = RepairObject::put(&dist, &ordinary, "repair-control.bin".to_owned()).await?;
    for restarted in [false, true] {
        if restarted {
            dist.restart_with_current_binary().await?;
        }
        seal_writes(&dist, &partial_user, &partial, true).await?;
        assert_put_closed(&dist, bucket, &format!("{prefix}closed-{restarted}.bin")).await?;
        if part.exists() {
            std::fs::remove_file(&part)?;
        }
        let before = physical_snapshot(&dist, bucket)?;
        let token = start_heal(&dist, bucket, &repaired.key, false).await?;
        finish_heal(&dist, &token, Some(&repaired.key)).await?;
        assert!(!part.exists(), "combined seal admitted a remote repair publication");
        assert!(physical_snapshot(&dist, bucket)? == before, "rejected repair changed metadata or shards");
        assert_exact_version(&dist.client(2)?, bucket, &repaired.key, &repaired.version, &repaired.body).await?;
        let ordinary_part = ordinary_object.part(Path::new(&dist.cluster.nodes[3].data_dirs[0]), &ordinary, 3)?;
        std::fs::remove_file(ordinary_part)?;
        let token = start_heal(&dist, &ordinary, &ordinary_object.key, false).await?;
        ordinary_object.assert_repaired(&dist, &ordinary).await?;
        finish_heal(&dist, &token, None).await?;
    }
    Ok(())
}
