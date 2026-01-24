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
}
