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

//! End-to-end tests for ListObjectsV2 pagination
//!
//! This module tests the ListObjectsV2 pagination functionality to ensure that:
//! - `IsTruncated` is set correctly based on whether there are more results
//! - `NextContinuationToken` is returned when there are more results
//! - Pagination works correctly with `ContinuationToken`
//!
//! ## Bug Reference
//!
//! GitHub Issue #1596: ListObjectsV2 pagination fails due to missing NextContinuationToken
//! The server was incorrectly setting IsTruncated=true even when all objects fit within max_keys,
//! and was returning V1 NextMarker instead of V2 NextContinuationToken.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use serial_test::serial;
    use std::collections::HashSet;
    use tracing::info;

    /// Helper function to create an S3 client for testing
    fn create_s3_client(env: &RustFSTestEnvironment) -> Client {
        env.create_s3_client()
    }

    /// Helper function to create a test bucket
    async fn create_bucket(client: &Client, bucket: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match client.create_bucket().bucket(bucket).send().await {
            Ok(_) => {
                info!("Bucket {} created successfully", bucket);
                Ok(())
            }
            Err(e) => {
                // Ignore if bucket already exists
                if e.to_string().contains("BucketAlreadyOwnedByYou") || e.to_string().contains("BucketAlreadyExists") {
                    info!("Bucket {} already exists", bucket);
                    Ok(())
                } else {
                    Err(Box::new(e))
                }
            }
        }
    }

    /// Test for Issue #2775: continuation forwarding must not
    /// skip a child directory when the prefix component repeats in the key.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_repeated_prefix_continuation() {
        init_logging();
        info!("Starting test: ListObjectsV2 repeated-prefix continuation");

        const PAGE_SIZE: i32 = 2;

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-repeated-prefix-small";
        let prefix = "engineering/";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        let expected_keys = vec![
            format!("{prefix}alpha-000/artifact.txt"),
            format!("{prefix}engineering/engineering/project-000/artifact.txt"),
            format!("{prefix}engineering/engineering/project-001/artifact.txt"),
            format!("{prefix}engineering/project-000/artifact.txt"),
            format!("{prefix}engineering/project-001/artifact.txt"),
            format!("{prefix}engineering/project-002/artifact.txt"),
            format!("{prefix}zulu-000/artifact.txt"),
        ];
        let noise_keys = [
            "different/prefix/prefix/project-000/artifact.txt",
            "engineering-other/project-000/artifact.txt",
            "unrelated/engineering/project-000/artifact.txt",
        ];

        for key in &expected_keys {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"x"))
                .send()
                .await
                .expect("Failed to put object");
        }
        for key in noise_keys {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"x"))
                .send()
                .await
                .expect("Failed to put noise object");
        }

        let mut listed_keys = Vec::new();
        let mut continuation_token: Option<String> = None;
        let mut last_key: Option<String> = None;
        let mut page_count = 0;

        loop {
            let mut request = client.list_objects_v2().bucket(bucket).prefix(prefix).max_keys(PAGE_SIZE);

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let output = request.send().await.expect("Failed to list objects");

            for obj in output.contents() {
                if let Some(key) = obj.key() {
                    if let Some(previous) = &last_key {
                        assert!(
                            key > previous.as_str(),
                            "ListObjectsV2 did not preserve lexicographic order: {key} <= {previous}"
                        );
                    }

                    last_key = Some(key.to_string());
                    listed_keys.push(key.to_string());
                }
            }

            page_count += 1;

            if output.is_truncated().unwrap_or(false) {
                continuation_token = output.next_continuation_token().map(|s| s.to_string());
                assert!(
                    continuation_token.is_some(),
                    "BUG: NextContinuationToken must be present when IsTruncated is true"
                );
            } else {
                break;
            }

            if page_count > 10 {
                panic!("Too many pages, possible infinite loop due to pagination bug");
            }
        }

        let seen: HashSet<String> = listed_keys.iter().cloned().collect();

        assert_eq!(
            listed_keys, expected_keys,
            "Issue #2775 regression: repeated-prefix pagination must return exactly the expected keys in lexicographic order"
        );
        assert_eq!(
            listed_keys.len(),
            expected_keys.len(),
            "Issue #2775 regression: expected all {} repeated-prefix objects under {prefix}, got {}",
            expected_keys.len(),
            listed_keys.len()
        );
        assert_eq!(seen.len(), expected_keys.len(), "Listed keys must be unique");

        for key in &expected_keys {
            assert!(seen.contains(key), "Missing expected key after repeated-prefix pagination: {key}");
        }

        env.stop_server();
    }

    /// Test that IsTruncated is false when all objects fit within max_keys
    ///
    /// This is the core bug from issue #1596: the server was returning
    /// IsTruncated=true even when all objects fit within the requested max_keys.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_not_truncated_when_all_objects_returned() {
        init_logging();
        info!("Starting test: ListObjectsV2 should not be truncated when all objects fit within max_keys");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-list-pagination";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Create 3 test objects
        let test_objects = ["file1.txt", "file2.txt", "file3.txt"];
        for key in &test_objects {
            client
                .put_object()
                .bucket(bucket)
                .key(*key)
                .body(ByteStream::from_static(b"test content"))
                .send()
                .await
                .expect("Failed to put object");
            info!("Created object: {}", key);
        }

        // List objects with max_keys=10 (larger than the number of objects)
        let result = client.list_objects_v2().bucket(bucket).max_keys(10).send().await;

        assert!(result.is_ok(), "Failed to list objects: {:?}", result.err());

        let output = result.unwrap();

        // Verify we got all 3 objects
        let contents = output.contents();
        assert_eq!(contents.len(), 3, "Expected 3 objects, got {}", contents.len());

        // KEY ASSERTION: IsTruncated should be false because all objects fit within max_keys
        let is_truncated = output.is_truncated().unwrap_or(false);
        assert!(
            !is_truncated,
            "BUG: IsTruncated should be false when all objects ({}) fit within max_keys (10)",
            contents.len()
        );

        // NextContinuationToken should be None when not truncated
        assert!(
            output.next_continuation_token().is_none(),
            "NextContinuationToken should be None when IsTruncated is false"
        );

        info!("Test passed: IsTruncated is correctly false when all objects fit within max_keys");

        env.stop_server();
    }

    /// Test that pagination works correctly when there are more objects than max_keys
    ///
    /// This test verifies that:
    /// 1. IsTruncated is true when there are more objects
    /// 2. NextContinuationToken is returned (not NextMarker)
    /// 3. Using ContinuationToken fetches the remaining objects
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_pagination_with_continuation_token() {
        init_logging();
        info!("Starting test: ListObjectsV2 pagination with continuation token");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-pagination-token";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Create 10 test objects
        let object_count = 10;
        for i in 1..=object_count {
            let key = format!("object{:02}.txt", i);
            client
                .put_object()
                .bucket(bucket)
                .key(&key)
                .body(ByteStream::from_static(b"test content"))
                .send()
                .await
                .expect("Failed to put object");
            info!("Created object: {}", key);
        }

        // First request: List with max_keys=3 (should get first 3 objects)
        let result = client.list_objects_v2().bucket(bucket).max_keys(3).send().await;

        assert!(result.is_ok(), "Failed to list objects: {:?}", result.err());

        let output = result.unwrap();
        let contents = output.contents();

        // Verify we got 3 objects
        assert_eq!(contents.len(), 3, "Expected 3 objects in first page, got {}", contents.len());

        // IsTruncated should be true because there are more objects
        let is_truncated = output.is_truncated().unwrap_or(false);
        assert!(is_truncated, "IsTruncated should be true when there are more objects than max_keys");

        // NextContinuationToken MUST be present (this is the V2 API requirement)
        let next_token = output.next_continuation_token();
        assert!(
            next_token.is_some(),
            "BUG: NextContinuationToken must be present when IsTruncated is true (Issue #1596)"
        );

        info!(
            "First page: Got {} objects, IsTruncated={}, NextContinuationToken={:?}",
            contents.len(),
            is_truncated,
            next_token
        );

        // Second request: Use continuation token to get next page
        let result = client
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(3)
            .continuation_token(next_token.unwrap())
            .send()
            .await;

        assert!(result.is_ok(), "Failed to list objects with continuation token: {:?}", result.err());

        let output = result.unwrap();
        let contents = output.contents();

        // Verify we got another page of objects
        assert_eq!(contents.len(), 3, "Expected 3 objects in second page, got {}", contents.len());

        // IsTruncated should still be true (we have 10 objects, requested 6 so far)
        let is_truncated = output.is_truncated().unwrap_or(false);
        assert!(is_truncated, "IsTruncated should be true for second page (still more objects)");

        info!("Second page: Got {} objects, IsTruncated={}", contents.len(), is_truncated);

        // Collect all objects using pagination
        let mut all_objects: Vec<String> = Vec::new();
        let mut continuation_token: Option<String> = None;
        let mut page_count = 0;

        loop {
            let mut request = client.list_objects_v2().bucket(bucket).max_keys(3);

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let output = request.send().await.expect("Failed to list objects");

            for obj in output.contents() {
                if let Some(key) = obj.key() {
                    all_objects.push(key.to_string());
                }
            }

            page_count += 1;

            if output.is_truncated().unwrap_or(false) {
                continuation_token = output.next_continuation_token().map(|s| s.to_string());
                assert!(
                    continuation_token.is_some(),
                    "BUG: NextContinuationToken must be present when IsTruncated is true"
                );
            } else {
                break;
            }

            // Safety limit to prevent infinite loops
            if page_count > 10 {
                panic!("Too many pages, possible infinite loop due to pagination bug");
            }
        }

        // Verify we collected all 10 objects
        assert_eq!(
            all_objects.len(),
            object_count,
            "Expected {} total objects across all pages, got {}",
            object_count,
            all_objects.len()
        );

        info!(
            "Pagination test passed: Collected all {} objects in {} pages",
            all_objects.len(),
            page_count
        );

        env.stop_server();
    }

    /// Test ListObjectsV2 with max_keys equal to object count
    ///
    /// Edge case: when max_keys exactly equals the number of objects,
    /// IsTruncated should be false.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_max_keys_equals_object_count() {
        init_logging();
        info!("Starting test: ListObjectsV2 with max_keys equal to object count");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-exact-count";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Create exactly 5 objects
        let object_count = 5;
        for i in 1..=object_count {
            let key = format!("item{}.txt", i);
            client
                .put_object()
                .bucket(bucket)
                .key(&key)
                .body(ByteStream::from_static(b"test"))
                .send()
                .await
                .expect("Failed to put object");
        }

        // List with max_keys=5 (exactly the number of objects)
        let result = client.list_objects_v2().bucket(bucket).max_keys(5).send().await;

        assert!(result.is_ok(), "Failed to list objects: {:?}", result.err());

        let output = result.unwrap();
        let contents = output.contents();

        assert_eq!(contents.len(), 5, "Expected 5 objects, got {}", contents.len());

        // IsTruncated should be false when max_keys equals object count
        let is_truncated = output.is_truncated().unwrap_or(false);
        assert!(
            !is_truncated,
            "BUG: IsTruncated should be false when max_keys ({}) equals object count ({})",
            5,
            contents.len()
        );

        assert!(
            output.next_continuation_token().is_none(),
            "NextContinuationToken should be None when IsTruncated is false"
        );

        info!("Test passed: IsTruncated is correctly false when max_keys equals object count");

        env.stop_server();
    }

    /// Test ListObjectsV2 with empty bucket
    ///
    /// Edge case: IsTruncated should be false for empty bucket.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_empty_bucket() {
        init_logging();
        info!("Starting test: ListObjectsV2 with empty bucket");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-empty-bucket";

        // Create empty bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // List objects in empty bucket
        let result = client.list_objects_v2().bucket(bucket).max_keys(10).send().await;

        assert!(result.is_ok(), "Failed to list objects: {:?}", result.err());

        let output = result.unwrap();
        let contents = output.contents();

        assert!(contents.is_empty(), "Expected empty bucket, got {} objects", contents.len());

        // IsTruncated should be false for empty bucket
        let is_truncated = output.is_truncated().unwrap_or(false);
        assert!(!is_truncated, "IsTruncated should be false for empty bucket");

        assert!(
            output.next_continuation_token().is_none(),
            "NextContinuationToken should be None for empty bucket"
        );

        info!("Test passed: Empty bucket returns IsTruncated=false");

        env.stop_server();
    }

    /// Test ListObjectsV2 caps max_keys above the service limit and still paginates.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_max_keys_above_limit_returns_token() {
        init_logging();
        info!("Starting test: ListObjectsV2 with max_keys above limit");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-max-keys-above-limit";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        let object_count = 1002;
        for i in 0..object_count {
            let key = format!("object{:04}.txt", i);
            client
                .put_object()
                .bucket(bucket)
                .key(&key)
                .body(ByteStream::from_static(b"test content"))
                .send()
                .await
                .expect("Failed to put object");
        }

        let output = client
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(1001)
            .send()
            .await
            .expect("Failed to list objects");

        assert_eq!(output.contents().len(), 1000);
        assert_eq!(output.key_count(), Some(1000));
        assert_eq!(output.max_keys(), Some(1000));
        assert!(
            output.is_truncated().unwrap_or(false),
            "IsTruncated should be true when more objects remain after capped max_keys"
        );

        let next_token = output
            .next_continuation_token()
            .expect("NextContinuationToken should be present when capped response is truncated")
            .to_string();

        let output = client
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(1001)
            .continuation_token(next_token)
            .send()
            .await
            .expect("Failed to list objects with continuation token");

        assert_eq!(output.contents().len(), 2);
        assert!(!output.is_truncated().unwrap_or(false));
        assert!(output.next_continuation_token().is_none());

        env.stop_server();
    }

    /// Test ListObjectsV2 with max_keys=0
    ///
    /// S3 semantics: when max_keys is 0, the response should include no objects
    /// and IsTruncated should be false.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_max_keys_zero() {
        init_logging();
        info!("Starting test: ListObjectsV2 with max_keys=0");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-max-keys-zero";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Create 2 objects
        let test_objects = ["alpha.txt", "beta.txt"];
        for key in &test_objects {
            client
                .put_object()
                .bucket(bucket)
                .key(*key)
                .body(ByteStream::from_static(b"test content"))
                .send()
                .await
                .expect("Failed to put object");
        }

        // List with max_keys=0
        let result = client.list_objects_v2().bucket(bucket).max_keys(0).send().await;

        assert!(result.is_ok(), "Failed to list objects: {:?}", result.err());

        let output = result.unwrap();
        let contents = output.contents();

        assert!(contents.is_empty(), "Expected no objects when max_keys=0");

        let is_truncated = output.is_truncated().unwrap_or(false);
        assert!(!is_truncated, "IsTruncated should be false when max_keys=0");

        assert!(
            output.next_continuation_token().is_none(),
            "NextContinuationToken should be None when max_keys=0"
        );

        info!("Test passed: max_keys=0 returns no objects and IsTruncated=false");

        env.stop_server();
    }

    /// Regression test: delimiter listing must not produce false-positive truncation
    /// when many raw keys collapse into a small number of CommonPrefixes.
    ///
    /// Scenario: 30 objects across 3 directories + 2 direct files under prefix.
    /// With max_keys=1000, all 5 visible results (3 prefixes + 2 objects) fit in one
    /// page, so IsTruncated must be false even though raw entry count is much larger.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_delimiter_collapsed_prefix_no_false_truncation() {
        init_logging();
        info!("Starting test: ListObjectsV2 delimiter collapsed-prefix no false truncation");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-collapsed-prefix";
        let prefix = "data/";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Create objects under 3 subdirectories (10 each = 30 raw keys)
        let dirs = ["alpha/", "beta/", "gamma/"];
        let mut expected_keys = Vec::new();
        for dir in &dirs {
            for i in 0..10 {
                let key = format!("{prefix}{dir}file{i:02}.txt");
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(ByteStream::from_static(b"x"))
                    .send()
                    .await
                    .expect("Failed to put object");
                expected_keys.push(key);
            }
        }
        // Add 2 direct objects under prefix
        for name in ["readme.txt", "config.json"] {
            let key = format!("{prefix}{name}");
            client
                .put_object()
                .bucket(bucket)
                .key(&key)
                .body(ByteStream::from_static(b"x"))
                .send()
                .await
                .expect("Failed to put object");
            expected_keys.push(key);
        }

        // List with delimiter="/", max_keys large enough to cover all visible results
        // Visible: 3 common prefixes + 2 direct objects = 5
        let output = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .delimiter("/")
            .max_keys(1000)
            .send()
            .await
            .expect("Failed to list objects");

        let listed_keys: Vec<String> = output
            .contents()
            .iter()
            .filter_map(|obj| obj.key())
            .map(|k| k.to_string())
            .collect();
        let listed_prefixes: Vec<String> = output
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix())
            .map(|p| p.to_string())
            .collect();

        // KEY ASSERTION: IsTruncated must be false because all visible results fit within max_keys
        let is_truncated = output.is_truncated().unwrap_or(false);
        assert!(
            !is_truncated,
            "BUG: IsTruncated should be false when all visible results ({} objects + {} prefixes = {}) fit within max_keys (1000)",
            listed_keys.len(),
            listed_prefixes.len(),
            listed_keys.len() + listed_prefixes.len()
        );

        assert!(
            output.next_continuation_token().is_none(),
            "NextContinuationToken should be None when IsTruncated is false"
        );

        // All 32 objects must be covered (listed_keys + keys under listed_prefixes)
        let total_raw_count = expected_keys.len();
        let keys_under_prefixes: usize = listed_prefixes
            .iter()
            .map(|p| {
                let dir = p.trim_end_matches('/');
                expected_keys.iter().filter(|k| k.starts_with(&format!("{dir}/"))).count()
            })
            .sum();
        let covered = listed_keys.len() + keys_under_prefixes;

        assert_eq!(
            covered,
            total_raw_count,
            "Collapsed-prefix listing must cover all {} objects, got {} (keys={}, under_prefixes={})",
            total_raw_count,
            covered,
            listed_keys.len(),
            keys_under_prefixes
        );

        info!(
            "Collapsed-prefix test passed: {} objects covered ({} keys + {} prefixes), IsTruncated=false",
            covered,
            listed_keys.len(),
            listed_prefixes.len()
        );

        env.stop_server();
    }

    /// Regression test: delimiter listing pagination must traverse all keys
    /// even when the visible result count per page is much smaller than the
    /// raw entry count (due to CommonPrefix collapse).
    ///
    /// Scenario: 1000 objects across 100 directories, paginated with max_keys=50.
    /// Each page returns up to 50 CommonPrefixes. The server must correctly set
    /// IsTruncated and provide a valid continuation token across all pages.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_delimiter_small_page_traverses_all() {
        init_logging();
        info!("Starting test: ListObjectsV2 delimiter small page traverses all keys");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-delimiter-small-page";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Create 1000 objects: 100 directories with 10 files each
        let mut all_keys = Vec::new();
        let dirs: Vec<String> = (0..100).map(|i| format!("dir-{:03}/", i)).collect();
        for dir in &dirs {
            for i in 0..10 {
                let key = format!("{dir}file{:02}.txt", i);
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(ByteStream::from_static(b"x"))
                    .send()
                    .await
                    .expect("Failed to put object");
                all_keys.push(key);
            }
        }

        // Paginate with delimiter="/" and max_keys=50
        // Visible per page: up to 50 CommonPrefixes
        let mut listed_keys = Vec::new();
        let mut listed_prefixes = Vec::new();
        let mut continuation_token: Option<String> = None;
        let mut page_count = 0;
        let mut last_page_is_truncated: bool;

        loop {
            let mut request = client.list_objects_v2().bucket(bucket).delimiter("/").max_keys(50);

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let output = request.send().await.expect("Failed to list objects");
            last_page_is_truncated = output.is_truncated().unwrap_or(false);

            for obj in output.contents() {
                if let Some(key) = obj.key() {
                    listed_keys.push(key.to_string());
                }
            }
            for cp in output.common_prefixes() {
                if let Some(p) = cp.prefix() {
                    listed_prefixes.push(p.to_string());
                }
            }

            page_count += 1;

            if last_page_is_truncated {
                continuation_token = output.next_continuation_token().map(|s| s.to_string());
                assert!(
                    continuation_token.is_some(),
                    "BUG: NextContinuationToken must be present when IsTruncated is true"
                );
            } else {
                break;
            }

            if page_count > 20 {
                panic!("Too many pages, possible infinite loop in delimiter pagination");
            }
        }

        // Last page must have IsTruncated=false
        assert!(
            !last_page_is_truncated,
            "BUG: Last page must have IsTruncated=false after all results returned"
        );

        // Verify all objects are covered via listed prefixes
        let keys_under_prefixes: HashSet<String> = all_keys
            .iter()
            .filter(|k| {
                listed_prefixes
                    .iter()
                    .any(|p| k.starts_with(&format!("{}/", p.trim_end_matches('/'))))
            })
            .cloned()
            .collect();
        let listed_set: HashSet<String> = listed_keys.iter().cloned().collect();
        let covered: HashSet<String> = listed_set.union(&keys_under_prefixes).cloned().collect();
        let expected_set: HashSet<String> = all_keys.iter().cloned().collect();

        assert_eq!(
            covered,
            expected_set,
            "Delimiter pagination must cover all {} objects, missing: {:?}",
            expected_set.difference(&covered).count(),
            expected_set.difference(&covered)
        );

        info!(
            "Delimiter small-page test passed: {} objects covered in {} pages",
            covered.len(),
            page_count
        );

        env.stop_server();
    }

    /// Regression test: raw entries exceed MaxKeys but all collapse into fewer
    /// visible CommonPrefixes — IsTruncated must be false because there are no
    /// more visible results beyond the collapsed prefixes.
    ///
    /// Scenario: 10 directories with 200 files each = 2000 raw keys.
    /// With MaxKeys=1000, the raw entry count exceeds MaxKeys (disk_has_more=true),
    /// but after delimiter collapse only 10 CommonPrefixes are visible (10 < 1000).
    /// IsTruncated must be false since there are no additional visible results.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_raw_exceeds_maxkeys_but_visible_below() {
        init_logging();
        info!("Starting test: ListObjectsV2 raw > MaxKeys but visible < MaxKeys after collapse");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-raw-exceeds-visible-below";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Create 10 directories × 200 files = 2000 raw keys
        let dir_count = 10;
        let files_per_dir = 200;
        let mut all_keys = Vec::new();
        for d in 0..dir_count {
            let dir = format!("dir-{:02}/", d);
            for f in 0..files_per_dir {
                let key = format!("{}file{:03}.txt", dir, f);
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(ByteStream::from_static(b"x"))
                    .send()
                    .await
                    .expect("Failed to put object");
                all_keys.push(key);
            }
        }

        // List with delimiter="/", max_keys=1000
        // Visible: 10 CommonPrefixes (dir-00/ .. dir-09/) = 10 visible << 1000 MaxKeys
        // But raw entry count (2000+1001 requested) exceeds MaxKeys
        let output = client
            .list_objects_v2()
            .bucket(bucket)
            .delimiter("/")
            .max_keys(1000)
            .send()
            .await
            .expect("Failed to list objects");

        let listed_keys: Vec<String> = output
            .contents()
            .iter()
            .filter_map(|obj| obj.key())
            .map(|k| k.to_string())
            .collect();
        let listed_prefixes: Vec<String> = output
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix())
            .map(|p| p.to_string())
            .collect();

        // KEY ASSERTION: IsTruncated must be false because visible results (10)
        // are far below MaxKeys (1000), even though raw entries exceeded MaxKeys.
        let is_truncated = output.is_truncated().unwrap_or(false);
        assert!(
            !is_truncated,
            "BUG: IsTruncated should be false when visible results ({} objects + {} prefixes = {}) < MaxKeys (1000), even if raw entries exceeded MaxKeys",
            listed_keys.len(),
            listed_prefixes.len(),
            listed_keys.len() + listed_prefixes.len()
        );

        assert!(
            output.next_continuation_token().is_none(),
            "NextContinuationToken should be None when IsTruncated is false"
        );

        // All objects must be covered by listed prefixes
        assert_eq!(
            listed_prefixes.len(),
            dir_count,
            "Expected {} prefixes, got {}",
            dir_count,
            listed_prefixes.len()
        );
        for d in 0..dir_count {
            let prefix = format!("dir-{:02}/", d);
            assert!(listed_prefixes.contains(&prefix), "Missing prefix: {}", prefix);
        }

        info!(
            "Raw-exceeds-visible test passed: {} raw keys → {} prefixes, IsTruncated=false",
            all_keys.len(),
            listed_prefixes.len()
        );

        env.stop_server();
    }

    /// Regression test: pagination with MaxKeys exceeding S3 limit (1000) and delimiter.
    /// The server caps MaxKeys to 1000. With delimiter="/", many raw keys collapse into
    /// few CommonPrefixes. Visible results (prefixes) < capped MaxKeys → IsTruncated=false.
    ///
    /// This complements test_list_objects_v2_max_keys_above_limit_returns_token which
    /// tests the non-delimiter case.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_maxkeys_above_limit_with_delimiter() {
        init_logging();
        info!("Starting test: ListObjectsV2 MaxKeys above limit with delimiter");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-maxkeys-limit-delimiter";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // 12 dirs × 100 files = 1200 raw keys
        let dir_count = 12;
        let files_per_dir = 100;
        for d in 0..dir_count {
            for f in 0..files_per_dir {
                let key = format!("dir-{:02}/file{:03}.txt", d, f);
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(ByteStream::from_static(b"x"))
                    .send()
                    .await
                    .expect("Failed to put object");
            }
        }

        // With delimiter: 12 CommonPrefixes visible, all fit within capped 1000
        let output = client
            .list_objects_v2()
            .bucket(bucket)
            .delimiter("/")
            .max_keys(2000)
            .send()
            .await
            .expect("Failed to list objects");

        assert_eq!(
            output.common_prefixes().len(),
            dir_count,
            "Expected {} prefixes, got {}",
            dir_count,
            output.common_prefixes().len()
        );
        assert_eq!(output.max_keys(), Some(1000));
        // 12 visible < 1000 capped MaxKeys → not truncated
        assert!(
            !output.is_truncated().unwrap_or(false),
            "BUG: IsTruncated should be false when visible ({}) < capped MaxKeys (1000)",
            output.common_prefixes().len()
        );

        info!("MaxKeys above limit with delimiter test passed");

        env.stop_server();
    }
}
