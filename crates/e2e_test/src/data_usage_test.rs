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
use rustfs_common::data_usage::DataUsageInfo;
use serial_test::serial;

use crate::common::{RustFSTestEnvironment, TEST_BUCKET, awscurl_get, init_logging};

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
    let url = format!("{}/rustfs/admin/v3/datausageinfo", env.url);
    let resp = awscurl_get(&url, &env.access_key, &env.secret_key).await?;
    let usage: DataUsageInfo = serde_json::from_str(&resp)?;

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
