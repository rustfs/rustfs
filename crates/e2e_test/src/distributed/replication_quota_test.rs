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

use super::harness::{
    DistCluster, DistLayout, TestResult, enable_versioning, put_bucket_replication, put_object, retrying_put, set_bucket_quota,
    set_remote_target, unique_bucket, wait_for_replicated_bytes, wait_until,
};
use crate::common::{FAST_DATA_USAGE_SCANNER_ENV, init_logging};
use aws_sdk_s3::error::ProvideErrorMetadata;
use http::Method;
use std::time::Duration;

/// `Ok(true)` quota admission rejected the PUT, `Ok(false)` retry, `Err` not quota.
fn quota_over_limit_put_outcome(code: Option<&str>, message: Option<&str>) -> Result<bool, String> {
    let quota_message = message.is_some_and(|text| text.starts_with("Bucket quota exceeded"));
    match code {
        Some("InvalidRequest" | "QuotaExceeded") if quota_message => Ok(true),
        Some("SlowDown" | "ServiceUnavailable") => Ok(false),
        Some("AccessDenied") => Err("AccessDenied is not a quota admission rejection".to_string()),
        Some("InvalidRequest" | "QuotaExceeded") => {
            Err(format!("InvalidRequest/QuotaExceeded without quota admission message: {message:?}"))
        }
        other => Err(format!("unexpected over-quota error code {other:?} message {message:?}")),
    }
}

#[tokio::test]
async fn four_node_bucket_replication_converges_to_peer_cluster() -> TestResult {
    init_logging();
    let (source, target) = DistCluster::start_replication_pair().await?;
    let source_bucket = unique_bucket("replsrc");
    let target_bucket = unique_bucket("repldst");
    source.create_bucket(&source_bucket).await?;
    target.create_bucket(&target_bucket).await?;

    let source_client = source.client(0)?;
    let target_client = target.client(0)?;
    enable_versioning(&source_client, &source_bucket).await?;
    enable_versioning(&target_client, &target_bucket).await?;

    let arn = set_remote_target(&source.cluster, &source_bucket, &target.cluster, &target_bucket).await?;
    put_bucket_replication(&source.cluster, &source_bucket, &arn).await?;

    let key = "replicated.bin";
    let body = b"distributed-bucket-replication".to_vec();
    put_object(&source_client, &source_bucket, key, body.clone()).await?;
    wait_for_replicated_bytes(&target_client, &target_bucket, key, &body, Duration::from_secs(45)).await?;

    let peer_read = target.client(3)?;
    wait_for_replicated_bytes(&peer_read, &target_bucket, key, &body, Duration::from_secs(15)).await?;
    Ok(())
}

#[tokio::test]
async fn four_node_four_drive_hard_quota_rejects_over_limit_put() -> TestResult {
    init_logging();
    let dist = DistCluster::start_with_env(DistLayout::FourByFour, FAST_DATA_USAGE_SCANNER_ENV).await?;
    let bucket = unique_bucket("quota");
    dist.create_bucket(&bucket).await?;
    set_bucket_quota(&dist.cluster, &bucket, 8 * 1024).await?;

    let client = dist.client(1)?;
    retrying_put(&client, &bucket, "small.bin", vec![0u8; 1024], Duration::from_secs(30)).await?;
    wait_until(
        Duration::from_secs(30),
        || async {
            let (status, body) = super::harness::cluster_admin(
                &dist.cluster,
                Method::GET,
                &format!("/rustfs/admin/v3/quota-stats/{bucket}"),
                None,
            )
            .await?;
            if !status.is_success() {
                return Ok(false);
            }
            let stats: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            Ok(stats.get("current_usage").and_then(serde_json::Value::as_u64).unwrap_or(0) >= 1024)
        },
        "quota stats observe small object",
    )
    .await?;

    let mut oversized_attempt = 0u32;
    wait_until(
        Duration::from_secs(30),
        || {
            oversized_attempt += 1;
            let key = format!("too-big-{oversized_attempt}.bin");
            let client = client.clone();
            let bucket = bucket.clone();
            async move {
                match client
                    .put_object()
                    .bucket(&bucket)
                    .key(key)
                    .body(vec![0u8; 16 * 1024].into())
                    .send()
                    .await
                {
                    Ok(_) => Ok(false),
                    Err(error) => {
                        let code = error.as_service_error().and_then(ProvideErrorMetadata::code);
                        let message = error.as_service_error().and_then(ProvideErrorMetadata::message);
                        match quota_over_limit_put_outcome(code, message) {
                            Ok(done) => Ok(done),
                            Err(detail) => Err(format!("{detail}: {error:?}").into()),
                        }
                    }
                }
            }
        },
        "hard quota rejects oversized PUT",
    )
    .await?;
    Ok(())
}

#[test]
fn quota_over_limit_put_outcome_requires_quota_admission() {
    assert_eq!(
        quota_over_limit_put_outcome(Some("InvalidRequest"), Some("Bucket quota exceeded for bucket x")),
        Ok(true)
    );
    assert_eq!(
        quota_over_limit_put_outcome(Some("QuotaExceeded"), Some("Bucket quota exceeded")),
        Ok(true)
    );
    assert_eq!(quota_over_limit_put_outcome(Some("SlowDown"), Some("slow down")), Ok(false));
    assert_eq!(quota_over_limit_put_outcome(Some("ServiceUnavailable"), Some("unavailable")), Ok(false));
    assert!(quota_over_limit_put_outcome(Some("AccessDenied"), Some("Access Denied")).is_err());
    assert!(quota_over_limit_put_outcome(Some("InvalidRequest"), Some("invalid argument")).is_err());
}
