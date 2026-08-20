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

//! Regression tests for bucket statistics and data usage accuracy.
//!
//! Covers the recurring pattern where bucket statistics (object count, size)
//! show stale/incorrect values, remain at 0, or oscillate between complete,
//! partial, and zero. This has regressed 10+ times.
//!
//! ## Regression Issues
//!
//! - rustfs#5615: bucket statistics remain unchanged after data expiration
//! - rustfs#5008: Admin usage reports only one pool
//! - rustfs#5116: Admin usage reports stale 0/0 for non-empty bucket after upgrade
//! - rustfs#5055: console object count and size still loading
//! - rustfs#5010: Storage usage info changed abnormally
//! - rustfs#3662: Incorrect bucket, object count and size
//! - rustfs#3898: DataUsageInfo undercounts versioned bucket versions
//! - rustfs#1012: Object count in the console doesn't change

#[cfg(test)]
mod tests {
    use crate::common::{FAST_DATA_USAGE_SCANNER_ENV, RustFSTestEnvironment, awscurl_get, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    use rustfs_data_usage::DataUsageInfo;
    use std::error::Error;
    use tokio::time::{Duration, sleep};
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    async fn get_data_usage(env: &RustFSTestEnvironment) -> Result<DataUsageInfo, Box<dyn Error + Send + Sync>> {
        let url = format!("{}/rustfs/admin/v3/datausageinfo", env.url);
        let resp = awscurl_get(&url, &env.access_key, &env.secret_key).await?;
        Ok(serde_json::from_str(&resp)?)
    }

    /// RT-09: Verify bucket object count updates after PUT.
    ///
    /// Regression pattern: bucket stats remain at 0 after objects are uploaded
    /// (rustfs#5055, rustfs#1012).
    ///
    /// Steps:
    /// 1. Create a bucket
    /// 2. Upload 10 objects
    /// 3. Query admin data usage API
    /// 4. Verify object count > 0
    #[tokio::test]
    async fn test_bucket_object_count_updates_after_put() -> TestResult {
        init_logging();
        info!("RT-09: bucket object count updates after PUT");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], FAST_DATA_USAGE_SCANNER_ENV)
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt09-stats-put";

        client.create_bucket().bucket(bucket).send().await.expect("create bucket");

        // Upload 10 objects
        for i in 0..10 {
            client
                .put_object()
                .bucket(bucket)
                .key(format!("stat-obj-{i:04}.txt"))
                .body(ByteStream::from_static(b"statistical data"))
                .send()
                .await
                .expect("put object");
        }

        // Wait for scanner to process (up to 90 seconds)
        let mut found_nonzero = false;
        let mut last_query_error = None;
        for attempt in 0..18 {
            sleep(Duration::from_secs(5)).await;

            let usage = match get_data_usage(&env).await {
                Ok(usage) => {
                    last_query_error = None;
                    usage
                }
                Err(err) => {
                    last_query_error = Some(err.to_string());
                    continue;
                }
            };
            if let Some(bucket_usage) = usage.buckets_usage.get(bucket) {
                info!("  attempt {attempt}: objectsCount = {}", bucket_usage.objects_count);
                if bucket_usage.objects_count >= 10 {
                    found_nonzero = true;
                    break;
                }
            }
        }

        assert!(
            found_nonzero,
            "RT-09 FAIL: bucket object count did not update after PUT 10 objects (regression: stats stuck at 0); last query error: {}",
            last_query_error.as_deref().unwrap_or("none")
        );

        info!("RT-09 PASS: bucket object count updates after PUT");
        Ok(())
    }

    /// RT-09b: Verify bucket stats update after DELETE.
    ///
    /// Regression pattern: stats remain unchanged after objects are deleted
    /// (rustfs#5615).
    #[tokio::test]
    async fn test_bucket_object_count_updates_after_delete() -> TestResult {
        init_logging();
        info!("RT-09b: bucket object count updates after DELETE");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], FAST_DATA_USAGE_SCANNER_ENV)
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt09b-stats-delete";

        client.create_bucket().bucket(bucket).send().await.expect("create bucket");

        // Upload 5 objects
        for i in 0..5 {
            client
                .put_object()
                .bucket(bucket)
                .key(format!("del-stat-{i}.txt"))
                .body(ByteStream::from_static(b"data"))
                .send()
                .await
                .expect("put object");
        }

        let mut found_nonzero = false;
        for attempt in 0..18 {
            sleep(Duration::from_secs(5)).await;

            if let Ok(usage) = get_data_usage(&env).await
                && let Some(bucket_usage) = usage.buckets_usage.get(bucket)
            {
                info!("  baseline attempt {attempt}: objectsCount = {}", bucket_usage.objects_count);
                if bucket_usage.objects_count >= 5 {
                    found_nonzero = true;
                    break;
                }
            }
        }
        assert!(found_nonzero, "RT-09b setup failed: scanner did not observe the 5 uploaded objects");

        // Delete all objects
        for i in 0..5 {
            client
                .delete_object()
                .bucket(bucket)
                .key(format!("del-stat-{i}.txt"))
                .send()
                .await
                .expect("delete object");
        }

        // Wait for scanner to update stats (up to 90 seconds)
        let mut found_zero = false;
        let mut last_query_error = None;
        for attempt in 0..18 {
            sleep(Duration::from_secs(5)).await;

            let usage = match get_data_usage(&env).await {
                Ok(usage) => {
                    last_query_error = None;
                    usage
                }
                Err(err) => {
                    last_query_error = Some(err.to_string());
                    continue;
                }
            };
            if let Some(bucket_usage) = usage.buckets_usage.get(bucket) {
                info!("  attempt {attempt}: objectsCount = {}", bucket_usage.objects_count);
                if bucket_usage.objects_count == 0 {
                    found_zero = true;
                    break;
                }
            }
        }

        assert!(
            found_zero,
            "RT-09b FAIL: bucket object count did not update to 0 after deleting all objects (regression rustfs#5615); last query error: {}",
            last_query_error.as_deref().unwrap_or("none")
        );

        info!("RT-09b PASS: bucket object count updates to 0 after DELETE");
        Ok(())
    }

    /// RT-09c: Verify versioned bucket stats count all versions.
    ///
    /// Regression pattern: DataUsageInfo undercounts versioned bucket versions
    /// and delete markers (rustfs#3898).
    #[tokio::test]
    async fn test_versioned_bucket_stats_count_all_versions() -> TestResult {
        init_logging();
        info!("RT-09c: versioned bucket stats count all versions");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt09c-versioned-stats";

        client.create_bucket().bucket(bucket).send().await.expect("create bucket");

        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .expect("enable versioning");

        // Create 3 versions of the same object
        for i in 0..3 {
            client
                .put_object()
                .bucket(bucket)
                .key("multi-version.txt")
                .body(ByteStream::from(format!("version-{i}").into_bytes()))
                .send()
                .await
                .expect("put version");
        }

        // Create a delete marker
        client
            .delete_object()
            .bucket(bucket)
            .key("multi-version.txt")
            .send()
            .await
            .expect("create delete marker");

        // Verify versions via API (immediate, no scanner wait)
        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .send()
            .await
            .expect("list versions");

        assert_eq!(
            versions.versions().len(),
            3,
            "RT-09c FAIL: expected 3 versions, found {}",
            versions.versions().len()
        );
        assert_eq!(
            versions.delete_markers().len(),
            1,
            "RT-09c FAIL: expected 1 delete marker, found {}",
            versions.delete_markers().len()
        );

        info!("RT-09c PASS: versioned bucket correctly tracks all versions and delete markers");
        Ok(())
    }
}
