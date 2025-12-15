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

//! End-to-end tests for special characters in object paths
//!
//! This module tests the handling of various special characters in S3 object keys,
//! including spaces, plus signs, percent signs, and other URL-encoded characters.
//!
//! ## Test Scenarios
//!
//! 1. **Spaces in paths**: `a f+/b/c/README.md` (encoded as `a%20f+/b/c/README.md`)
//! 2. **Plus signs in paths**: `ES+net/file+name.txt`
//! 3. **Mixed special characters**: Combinations of spaces, plus, percent, etc.
//! 4. **Operations tested**: PUT, GET, LIST, DELETE

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use serial_test::serial;
    use tracing::{debug, info};

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

    /// Test PUT and GET with space character in path
    ///
    /// This reproduces Part A of the issue:
    /// ```
    /// mc cp README.md "local/dummy/a%20f+/b/c/3/README.md"
    /// ```
    #[tokio::test]
    #[serial]
    async fn test_object_with_space_in_path() {
        init_logging();
        info!("Starting test: object with space in path");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-special-chars";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Test key with space: "a f+/b/c/3/README.md"
        // When URL-encoded by client: "a%20f+/b/c/3/README.md"
        let key = "a f+/b/c/3/README.md";
        let content = b"Test content with space in path";

        info!("Testing PUT object with key: {}", key);

        // PUT object
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .send()
            .await;

        assert!(result.is_ok(), "Failed to PUT object with space in path: {:?}", result.err());
        info!("✅ PUT object with space in path succeeded");

        // GET object
        info!("Testing GET object with key: {}", key);
        let result = client.get_object().bucket(bucket).key(key).send().await;

        assert!(result.is_ok(), "Failed to GET object with space in path: {:?}", result.err());

        let output = result.unwrap();
        let body_bytes = output.body.collect().await.unwrap().into_bytes();
        assert_eq!(body_bytes.as_ref(), content, "Content mismatch");
        info!("✅ GET object with space in path succeeded");

        // LIST objects with prefix containing space
        info!("Testing LIST objects with prefix: a f+/");
        let result = client.list_objects_v2().bucket(bucket).prefix("a f+/").send().await;

        assert!(result.is_ok(), "Failed to LIST objects with space in prefix: {:?}", result.err());

        let output = result.unwrap();
        let contents = output.contents();
        assert!(!contents.is_empty(), "LIST returned no objects");
        assert!(
            contents.iter().any(|obj| obj.key().unwrap() == key),
            "Object with space not found in LIST results"
        );
        info!("✅ LIST objects with space in prefix succeeded");

        // LIST objects with deeper prefix
        info!("Testing LIST objects with prefix: a f+/b/c/");
        let result = client.list_objects_v2().bucket(bucket).prefix("a f+/b/c/").send().await;

        assert!(result.is_ok(), "Failed to LIST objects with deeper prefix: {:?}", result.err());

        let output = result.unwrap();
        let contents = output.contents();
        assert!(!contents.is_empty(), "LIST with deeper prefix returned no objects");
        info!("✅ LIST objects with deeper prefix succeeded");

        // Cleanup
        env.stop_server();
        info!("Test completed successfully");
    }

    /// Test PUT and GET with plus sign in path
    ///
    /// This reproduces Part B of the issue:
    /// ```
    /// /test/data/org_main-org/dashboards/ES+net/LHC+Data+Challenge/firefly-details.json
    /// ```
    #[tokio::test]
    #[serial]
    async fn test_object_with_plus_in_path() {
        init_logging();
        info!("Starting test: object with plus sign in path");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-plus-chars";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Test key with plus signs
        let key = "dashboards/ES+net/LHC+Data+Challenge/firefly-details.json";
        let content = b"Test content with plus signs in path";

        info!("Testing PUT object with key: {}", key);

        // PUT object
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .send()
            .await;

        assert!(result.is_ok(), "Failed to PUT object with plus in path: {:?}", result.err());
        info!("✅ PUT object with plus in path succeeded");

        // GET object
        info!("Testing GET object with key: {}", key);
        let result = client.get_object().bucket(bucket).key(key).send().await;

        assert!(result.is_ok(), "Failed to GET object with plus in path: {:?}", result.err());

        let output = result.unwrap();
        let body_bytes = output.body.collect().await.unwrap().into_bytes();
        assert_eq!(body_bytes.as_ref(), content, "Content mismatch");
        info!("✅ GET object with plus in path succeeded");

        // LIST objects with prefix containing plus
        info!("Testing LIST objects with prefix: dashboards/ES+net/");
        let result = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("dashboards/ES+net/")
            .send()
            .await;

        assert!(result.is_ok(), "Failed to LIST objects with plus in prefix: {:?}", result.err());

        let output = result.unwrap();
        let contents = output.contents();
        assert!(!contents.is_empty(), "LIST returned no objects");
        assert!(
            contents.iter().any(|obj| obj.key().unwrap() == key),
            "Object with plus not found in LIST results"
        );
        info!("✅ LIST objects with plus in prefix succeeded");

        // Cleanup
        env.stop_server();
        info!("Test completed successfully");
    }

    /// Test with mixed special characters
    #[tokio::test]
    #[serial]
    async fn test_object_with_mixed_special_chars() {
        init_logging();
        info!("Starting test: object with mixed special characters");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-mixed-chars";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Test various special characters
        let test_cases = vec![
            ("path/with spaces/file.txt", b"Content 1" as &[u8]),
            ("path/with+plus/file.txt", b"Content 2"),
            ("path/with spaces+and+plus/file.txt", b"Content 3"),
            ("ES+net/folder name/file.txt", b"Content 4"),
        ];

        for (key, content) in &test_cases {
            info!("Testing with key: {}", key);

            // PUT
            let result = client
                .put_object()
                .bucket(bucket)
                .key(*key)
                .body(ByteStream::from(content.to_vec()))
                .send()
                .await;
            assert!(result.is_ok(), "Failed to PUT object with key '{}': {:?}", key, result.err());

            // GET
            let result = client.get_object().bucket(bucket).key(*key).send().await;
            assert!(result.is_ok(), "Failed to GET object with key '{}': {:?}", key, result.err());

            let output = result.unwrap();
            let body_bytes = output.body.collect().await.unwrap().into_bytes();
            assert_eq!(body_bytes.as_ref(), *content, "Content mismatch for key '{key}'");

            info!("✅ PUT/GET succeeded for key: {}", key);
        }

        // LIST all objects
        let result = client.list_objects_v2().bucket(bucket).send().await;
        assert!(result.is_ok(), "Failed to LIST all objects");

        let output = result.unwrap();
        let contents = output.contents();
        assert_eq!(contents.len(), test_cases.len(), "Number of objects mismatch");

        // Cleanup
        env.stop_server();
        info!("Test completed successfully");
    }

    /// Test DELETE operation with special characters
    #[tokio::test]
    #[serial]
    async fn test_delete_object_with_special_chars() {
        init_logging();
        info!("Starting test: DELETE object with special characters");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-delete-special";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        let key = "folder with spaces/ES+net/file.txt";
        let content = b"Test content";

        // PUT object
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("Failed to PUT object");

        // Verify it exists
        let result = client.get_object().bucket(bucket).key(key).send().await;
        assert!(result.is_ok(), "Object should exist before DELETE");

        // DELETE object
        info!("Testing DELETE object with key: {}", key);
        let result = client.delete_object().bucket(bucket).key(key).send().await;
        assert!(result.is_ok(), "Failed to DELETE object with special chars: {:?}", result.err());
        info!("✅ DELETE object succeeded");

        // Verify it's deleted
        let result = client.get_object().bucket(bucket).key(key).send().await;
        assert!(result.is_err(), "Object should not exist after DELETE");

        // Cleanup
        env.stop_server();
        info!("Test completed successfully");
    }

    /// Test exact scenario from the issue
    #[tokio::test]
    #[serial]
    async fn test_issue_scenario_exact() {
        init_logging();
        info!("Starting test: Exact scenario from GitHub issue");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "dummy";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Exact key from issue: "a%20f+/b/c/3/README.md"
        // The decoded form should be: "a f+/b/c/3/README.md"
        let key = "a f+/b/c/3/README.md";
        let content = b"README content";

        info!("Reproducing exact issue scenario with key: {}", key);

        // Step 1: Upload file (like `mc cp README.md "local/dummy/a%20f+/b/c/3/README.md"`)
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .send()
            .await;
        assert!(result.is_ok(), "Failed to upload file: {:?}", result.err());
        info!("✅ File uploaded successfully");

        // Step 2: Navigate to folder (like navigating to "%20f+/" in UI)
        // This is equivalent to listing with prefix "a f+/"
        info!("Listing folder 'a f+/' (this should show subdirectories)");
        let result = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("a f+/")
            .delimiter("/")
            .send()
            .await;
        assert!(result.is_ok(), "Failed to list folder: {:?}", result.err());

        let output = result.unwrap();
        debug!("List result: {:?}", output);

        // Should show "b/" as a common prefix (subdirectory)
        let common_prefixes = output.common_prefixes();
        assert!(
            !common_prefixes.is_empty() || !output.contents().is_empty(),
            "Folder should show contents or subdirectories"
        );
        info!("✅ Folder listing succeeded");

        // Step 3: List deeper (like `mc ls "local/dummy/a%20f+/b/c/3/"`)
        info!("Listing deeper folder 'a f+/b/c/3/'");
        let result = client.list_objects_v2().bucket(bucket).prefix("a f+/b/c/3/").send().await;
        assert!(result.is_ok(), "Failed to list deep folder: {:?}", result.err());

        let output = result.unwrap();
        let contents = output.contents();
        assert!(!contents.is_empty(), "Deep folder should show the file");
        assert!(contents.iter().any(|obj| obj.key().unwrap() == key), "README.md should be in the list");
        info!("✅ Deep folder listing succeeded - file found");

        // Cleanup
        env.stop_server();
        info!("✅ Exact issue scenario test completed successfully");
    }

    /// Test HEAD object with special characters
    #[tokio::test]
    #[serial]
    async fn test_head_object_with_special_chars() {
        init_logging();
        info!("Starting test: HEAD object with special characters");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-head-special";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        let key = "folder with spaces/ES+net/file.txt";
        let content = b"Test content for HEAD";

        // PUT object
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("Failed to PUT object");

        info!("Testing HEAD object with key: {}", key);

        // HEAD object
        let result = client.head_object().bucket(bucket).key(key).send().await;
        assert!(result.is_ok(), "Failed to HEAD object with special chars: {:?}", result.err());

        let output = result.unwrap();
        assert_eq!(output.content_length().unwrap_or(0), content.len() as i64, "Content length mismatch");
        info!("✅ HEAD object with special characters succeeded");

        // Cleanup
        env.stop_server();
        info!("Test completed successfully");
    }

    /// Test COPY object with special characters in both source and destination
    #[tokio::test]
    #[serial]
    async fn test_copy_object_with_special_chars() {
        init_logging();
        info!("Starting test: COPY object with special characters");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-copy-special";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        let src_key = "source/folder with spaces/file.txt";
        let dest_key = "dest/ES+net/copied file.txt";
        let content = b"Test content for COPY";

        // PUT source object
        client
            .put_object()
            .bucket(bucket)
            .key(src_key)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("Failed to PUT source object");

        info!("Testing COPY from '{}' to '{}'", src_key, dest_key);

        // COPY object
        let copy_source = format!("{bucket}/{src_key}");
        let result = client
            .copy_object()
            .bucket(bucket)
            .key(dest_key)
            .copy_source(&copy_source)
            .send()
            .await;

        assert!(result.is_ok(), "Failed to COPY object with special chars: {:?}", result.err());
        info!("✅ COPY operation succeeded");

        // Verify destination exists
        let result = client.get_object().bucket(bucket).key(dest_key).send().await;
        assert!(result.is_ok(), "Failed to GET copied object");

        let output = result.unwrap();
        let body_bytes = output.body.collect().await.unwrap().into_bytes();
        assert_eq!(body_bytes.as_ref(), content, "Copied content mismatch");
        info!("✅ Copied object verified successfully");

        // Cleanup
        env.stop_server();
        info!("Test completed successfully");
    }

    /// Test Unicode characters in object keys
    #[tokio::test]
    #[serial]
    async fn test_unicode_characters_in_path() {
        init_logging();
        info!("Starting test: Unicode characters in object paths");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-unicode";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Test various Unicode characters
        let test_cases = vec![
            ("测试/文件.txt", b"Chinese characters" as &[u8]),
            ("テスト/ファイル.txt", b"Japanese characters"),
            ("테스트/파일.txt", b"Korean characters"),
            ("тест/файл.txt", b"Cyrillic characters"),
            ("emoji/😀/file.txt", b"Emoji in path"),
            ("mixed/测试 test/file.txt", b"Mixed languages"),
        ];

        for (key, content) in &test_cases {
            info!("Testing Unicode key: {}", key);

            // PUT
            let result = client
                .put_object()
                .bucket(bucket)
                .key(*key)
                .body(ByteStream::from(content.to_vec()))
                .send()
                .await;
            assert!(result.is_ok(), "Failed to PUT object with Unicode key '{}': {:?}", key, result.err());

            // GET
            let result = client.get_object().bucket(bucket).key(*key).send().await;
            assert!(result.is_ok(), "Failed to GET object with Unicode key '{}': {:?}", key, result.err());

            let output = result.unwrap();
            let body_bytes = output.body.collect().await.unwrap().into_bytes();
            assert_eq!(body_bytes.as_ref(), *content, "Content mismatch for Unicode key '{key}'");

            info!("✅ PUT/GET succeeded for Unicode key: {}", key);
        }

        // LIST to verify all objects
        let result = client.list_objects_v2().bucket(bucket).send().await;
        assert!(result.is_ok(), "Failed to LIST objects with Unicode keys");

        let output = result.unwrap();
        let contents = output.contents();
        assert_eq!(contents.len(), test_cases.len(), "Number of Unicode objects mismatch");
        info!("✅ All Unicode objects listed successfully");

        // Cleanup
        env.stop_server();
        info!("Test completed successfully");
    }

    /// Test special characters in different parts of the path
    #[tokio::test]
    #[serial]
    async fn test_special_chars_in_different_path_positions() {
        init_logging();
        info!("Starting test: Special characters in different path positions");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-path-positions";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Test special characters in different positions
        let test_cases = vec![
            ("start with space/file.txt", b"Space at start" as &[u8]),
            ("folder/end with space /file.txt", b"Space at end of folder"),
            ("multiple   spaces/file.txt", b"Multiple consecutive spaces"),
            ("folder/file with space.txt", b"Space in filename"),
            ("a+b/c+d/e+f.txt", b"Plus signs throughout"),
            ("a%b/c%d/e%f.txt", b"Percent signs throughout"),
            ("folder/!@#$%^&*()/file.txt", b"Multiple special chars"),
            ("(parentheses)/[brackets]/file.txt", b"Parentheses and brackets"),
            ("'quotes'/\"double\"/file.txt", b"Quote characters"),
        ];

        for (key, content) in &test_cases {
            info!("Testing key: {}", key);

            // PUT
            let result = client
                .put_object()
                .bucket(bucket)
                .key(*key)
                .body(ByteStream::from(content.to_vec()))
                .send()
                .await;
            assert!(result.is_ok(), "Failed to PUT object with key '{}': {:?}", key, result.err());

            // GET
            let result = client.get_object().bucket(bucket).key(*key).send().await;
            assert!(result.is_ok(), "Failed to GET object with key '{}': {:?}", key, result.err());

            let output = result.unwrap();
            let body_bytes = output.body.collect().await.unwrap().into_bytes();
            assert_eq!(body_bytes.as_ref(), *content, "Content mismatch for key '{key}'");

            info!("✅ PUT/GET succeeded for key: {}", key);
        }

        // Cleanup
        env.stop_server();
        info!("Test completed successfully");
    }

    /// Test that control characters are properly rejected
    #[tokio::test]
    #[serial]
    async fn test_control_characters_rejected() {
        init_logging();
        info!("Starting test: Control characters should be rejected");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-control-chars";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Test that control characters are rejected
        let invalid_keys = vec![
            "file\0with\0null.txt",
            "file\nwith\nnewline.txt",
            "file\rwith\rcarriage.txt",
            "file\twith\ttab.txt", // Tab might be allowed, but let's test
        ];

        for key in invalid_keys {
            info!("Testing rejection of control character in key: {:?}", key);

            let result = client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"test"))
                .send()
                .await;

            // Note: The validation happens on the server side, so we expect an error
            // For null byte, newline, and carriage return
            if key.contains('\0') || key.contains('\n') || key.contains('\r') {
                assert!(result.is_err(), "Control character should be rejected for key: {key:?}");
                if let Err(e) = result {
                    info!("✅ Control character correctly rejected: {:?}", e);
                }
            }
        }

        // Cleanup
        env.stop_server();
        info!("Test completed successfully");
    }

    /// Test LIST with various special character prefixes
    #[tokio::test]
    #[serial]
    async fn test_list_with_special_char_prefixes() {
        init_logging();
        info!("Starting test: LIST with special character prefixes");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-list-prefixes";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Create objects with various special characters
        let test_objects = vec![
            "prefix with spaces/file1.txt",
            "prefix with spaces/file2.txt",
            "prefix+plus/file1.txt",
            "prefix+plus/file2.txt",
            "prefix%percent/file1.txt",
            "prefix%percent/file2.txt",
        ];

        for key in &test_objects {
            client
                .put_object()
                .bucket(bucket)
                .key(*key)
                .body(ByteStream::from_static(b"test"))
                .send()
                .await
                .expect("Failed to PUT object");
        }

        // Test LIST with different prefixes
        let prefix_tests = vec![
            ("prefix with spaces/", 2),
            ("prefix+plus/", 2),
            ("prefix%percent/", 2),
            ("prefix", 6), // Should match all
        ];

        for (prefix, expected_count) in prefix_tests {
            info!("Testing LIST with prefix: '{}'", prefix);

            let result = client.list_objects_v2().bucket(bucket).prefix(prefix).send().await;
            assert!(result.is_ok(), "Failed to LIST with prefix '{}': {:?}", prefix, result.err());

            let output = result.unwrap();
            let contents = output.contents();
            assert_eq!(
                contents.len(),
                expected_count,
                "Expected {} objects with prefix '{}', got {}",
                expected_count,
                prefix,
                contents.len()
            );
            info!("✅ LIST with prefix '{}' returned {} objects", prefix, contents.len());
        }

        // Cleanup
        env.stop_server();
        info!("Test completed successfully");
    }

    /// Test delimiter-based listing with special characters
    #[tokio::test]
    #[serial]
    async fn test_list_with_delimiter_and_special_chars() {
        init_logging();
        info!("Starting test: LIST with delimiter and special characters");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-delimiter-special";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // Create hierarchical structure with special characters
        let test_objects = vec![
            "folder with spaces/subfolder1/file.txt",
            "folder with spaces/subfolder2/file.txt",
            "folder with spaces/file.txt",
            "folder+plus/subfolder1/file.txt",
            "folder+plus/file.txt",
        ];

        for key in &test_objects {
            client
                .put_object()
                .bucket(bucket)
                .key(*key)
                .body(ByteStream::from_static(b"test"))
                .send()
                .await
                .expect("Failed to PUT object");
        }

        // Test LIST with delimiter
        info!("Testing LIST with delimiter for 'folder with spaces/'");
        let result = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("folder with spaces/")
            .delimiter("/")
            .send()
            .await;

        assert!(result.is_ok(), "Failed to LIST with delimiter");

        let output = result.unwrap();
        let common_prefixes = output.common_prefixes();
        assert_eq!(common_prefixes.len(), 2, "Should have 2 common prefixes (subdirectories)");
        info!("✅ LIST with delimiter returned {} common prefixes", common_prefixes.len());

        // Cleanup
        env.stop_server();
        info!("Test completed successfully");
    }
}
