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

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use rustfs_data_usage::DataUsageInfo;
use serial_test::serial;
use tokio::time::{Duration, sleep};

use crate::common::{RustFSTestEnvironment, TEST_BUCKET, awscurl_get, init_logging};

async fn get_data_usage_info(env: &RustFSTestEnvironment) -> Result<DataUsageInfo, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/datausageinfo", env.url);
    let resp = awscurl_get(&url, &env.access_key, &env.secret_key).await?;
    Ok(serde_json::from_str(&resp)?)
}

async fn wait_for_bucket_usage<F>(
    env: &RustFSTestEnvironment,
    bucket: &str,
    mut predicate: F,
) -> Result<DataUsageInfo, Box<dyn std::error::Error + Send + Sync>>
where
    F: FnMut(&DataUsageInfo) -> bool,
{
    let mut last_usage = DataUsageInfo::default();
    for _ in 0..45 {
        let usage = get_data_usage_info(env).await?;
        if usage.buckets_usage.contains_key(bucket) && predicate(&usage) {
            return Ok(usage);
        }
        last_usage = usage;
        sleep(Duration::from_secs(2)).await;
    }

    Err(format!("bucket usage did not converge for {bucket}; last usage: {last_usage:?}").into())
}

/// Regression test for data usage accuracy (issue #1012).
/// Launches rustfs, writes 1000 objects, then asserts admin data usage reports the full count.
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a rustfs server and requires awscurl; enable when running full E2E"]
async fn data_usage_reports_all_objects() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let client = env.create_s3_client();

    // Create bucket and upload objects
    client.create_bucket().bucket(TEST_BUCKET).send().await?;

    for i in 0..1000 {
        let key = format!("obj-{i:04}");
        client
            .put_object()
            .bucket(TEST_BUCKET)
            .key(key)
            .body(ByteStream::from_static(b"hello-world"))
            .send()
            .await?;
    }

    // Query admin data usage API
    let usage = get_data_usage_info(&env).await?;

    // Assert total object count and per-bucket count are not truncated
    let bucket_usage = usage
        .buckets_usage
        .get(TEST_BUCKET)
        .cloned()
        .expect("bucket usage should exist");

    assert!(
        usage.objects_total_count >= 1000,
        "total object count should be at least 1000, got {}",
        usage.objects_total_count
    );
    assert!(
        bucket_usage.objects_count >= 1000,
        "bucket object count should be at least 1000, got {}",
        bucket_usage.objects_count
    );

    env.stop_server();
    Ok(())
}

/// Regression test for issue #3898.
/// Versioned buckets should expose versions and delete markers through admin data usage.
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a rustfs server and requires awscurl; enable when running full E2E"]
async fn data_usage_reports_versioned_objects_and_delete_markers() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let client = env.create_s3_client();
    let bucket = "data-usage-versioned";

    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;

    client
        .put_object()
        .bucket(bucket)
        .key("alpha")
        .body(ByteStream::from_static(b"v1-alpha"))
        .send()
        .await?;
    client
        .put_object()
        .bucket(bucket)
        .key("alpha")
        .body(ByteStream::from_static(b"v2-alpha-larger"))
        .send()
        .await?;
    client
        .put_object()
        .bucket(bucket)
        .key("beta")
        .body(ByteStream::from_static(b"beta"))
        .send()
        .await?;
    client.delete_object().bucket(bucket).key("alpha").send().await?;

    let listed_versions = client.list_object_versions().bucket(bucket).send().await?;
    assert_eq!(listed_versions.versions().len(), 3, "S3 version listing should report stored versions");
    assert_eq!(
        listed_versions.delete_markers().len(),
        1,
        "S3 version listing should report the delete marker"
    );

    let usage = wait_for_bucket_usage(&env, bucket, |usage| {
        usage
            .buckets_usage
            .get(bucket)
            .map(|bucket_usage| {
                bucket_usage.objects_count == 2 && bucket_usage.versions_count == 3 && bucket_usage.delete_markers_count == 1
            })
            .unwrap_or(false)
    })
    .await?;
    let bucket_usage = usage
        .buckets_usage
        .get(bucket)
        .expect("bucket usage should exist after convergence");

    assert_eq!(
        bucket_usage.objects_count, 2,
        "current object count should exclude the deleted current alpha"
    );
    assert_eq!(bucket_usage.versions_count, 3, "version count should include all stored object versions");
    assert_eq!(
        bucket_usage.delete_markers_count, 1,
        "delete marker count should include the alpha marker"
    );
    assert_eq!(usage.objects_total_count, 2, "total current object count should match bucket usage");
    assert_eq!(usage.versions_total_count, 3, "total version count should match bucket usage");
    assert_eq!(usage.delete_markers_total_count, 1, "total delete marker count should match bucket usage");

    env.stop_server();
    env.start_rustfs_server(vec![]).await?;

    let restarted_usage = wait_for_bucket_usage(&env, bucket, |usage| {
        usage
            .buckets_usage
            .get(bucket)
            .map(|bucket_usage| {
                bucket_usage.objects_count == 2 && bucket_usage.versions_count == 3 && bucket_usage.delete_markers_count == 1
            })
            .unwrap_or(false)
    })
    .await?;
    let restarted_bucket_usage = restarted_usage
        .buckets_usage
        .get(bucket)
        .expect("bucket usage should exist after restart");
    assert_eq!(restarted_bucket_usage.objects_count, 2, "object count should persist after restart");
    assert_eq!(restarted_bucket_usage.versions_count, 3, "version count should persist after restart");
    assert_eq!(
        restarted_bucket_usage.delete_markers_count, 1,
        "delete marker count should persist after restart"
    );

    env.stop_server();
    Ok(())
}
