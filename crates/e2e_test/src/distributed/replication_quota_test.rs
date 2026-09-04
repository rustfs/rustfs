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
    DistCluster, DistLayout, TestResult, enable_versioning, put_bucket_replication, put_object, set_bucket_quota,
    set_remote_target, unique_bucket, wait_for_replicated_bytes,
};
use crate::common::{FAST_DATA_USAGE_SCANNER_ENV, init_logging};
use aws_sdk_s3::error::ProvideErrorMetadata;
use std::time::Duration;

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
    put_object(&client, &bucket, "small.bin", vec![0u8; 1024]).await?;

    let over_limit = client
        .put_object()
        .bucket(&bucket)
        .key("too-big.bin")
        .body(vec![0u8; 16 * 1024].into())
        .send()
        .await;
    match over_limit {
        Ok(_) => {
            // Scanner-backed quota can lag a cycle; a second over-quota PUT must fail.
            let second = client
                .put_object()
                .bucket(&bucket)
                .key("too-big-2.bin")
                .body(vec![0u8; 16 * 1024].into())
                .send()
                .await;
            match second {
                Ok(_) => return Err("hard quota admitted two oversized PUTs on a 4x4 cluster".into()),
                Err(error) => {
                    let code = error.as_service_error().and_then(ProvideErrorMetadata::code);
                    assert!(
                        matches!(code, Some("QuotaExceeded" | "SlowDown" | "AccessDenied" | "InvalidRequest")),
                        "unexpected over-quota error: {error:?}"
                    );
                }
            }
        }
        Err(error) => {
            let code = error.as_service_error().and_then(ProvideErrorMetadata::code);
            assert!(
                matches!(code, Some("QuotaExceeded" | "SlowDown" | "AccessDenied" | "InvalidRequest")),
                "unexpected over-quota error: {error:?}"
            );
        }
    }
    Ok(())
}
