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
        let mut retries = 20;
        loop {
            match client.create_bucket().bucket(bucket).send().await {
                Ok(_) => {
                    info!("Bucket {} created successfully", bucket);
                    return Ok(());
                }
                Err(e) => {
                    // Ignore if bucket already exists
                    if e.to_string().contains("BucketAlreadyOwnedByYou") || e.to_string().contains("BucketAlreadyExists") {
                        info!("Bucket {} already exists", bucket);
                        return Ok(());
                    }
                    if retries > 0 {
                        retries -= 1;
                        info!("Bucket creation failed, retrying... ({})", retries);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                    return Err(Box::new(e));
                }
            }
        }
    }

    /// Test ensuring that ListObjectsV2 returns unique CommonPrefixes even if "folder" objects exist.
    ///
    /// Bug Reference: Issue #1797
    /// Veeam creates 0-byte objects ending in '/' (e.g. "folder/") to represent folders.
    /// If "folder/file.txt" also exists, "folder/" is a CommonPrefix.
    /// The bug was that "folder/" (the object) and "folder/" (derived prefix) were both added to CommonPrefixes
    /// when delimiter was "/" because the deduplication check was explicitly skipped for "/" delimiter.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_unique_common_prefixes() {
        init_logging();
        info!("Starting test: ListObjectsV2 should return unique CommonPrefixes");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-list-unique-prefixes";

        // Create bucket
        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        // 1. Create a file inside a folder
        client
            .put_object()
            .bucket(bucket)
            .key("folder/file.txt")
            .body(ByteStream::from_static(b"content"))
            .send()
            .await
            .expect("Failed to create file inside folder");

        // 2. Create the "folder" object itself (Veeam behavior)
        client
            .put_object()
            .bucket(bucket)
            .key("folder/")
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .expect("Failed to create folder object");

        // 3. List with delimiter="/"
        let result = client
            .list_objects_v2()
            .bucket(bucket)
            .delimiter("/")
            .send()
            .await
            .expect("Failed to list objects");

        // Verify prefixes
        let prefixes = result.common_prefixes();
        info!("CommonPrefixes: {:?}", prefixes);

        // Should contain "folder/" exactly once
        let folder_prefixes: Vec<_> = prefixes.iter().filter(|p| p.prefix() == Some("folder/")).collect();

        assert_eq!(
            folder_prefixes.len(),
            1,
            "Expected exactly 1 'folder/' prefix, found {}",
            folder_prefixes.len()
        );

        // Verify that "folder/" is NOT returned as an object in Contents.
        // For this regression test, we expect "folder/" to be represented only as a CommonPrefix
        // (rolled up from "folder/file.txt" and the explicit "folder/" object), and to appear there
        // exactly once. It must not appear in Contents at all.

        // Ensure "folder/" is NOT in contents (Contents)
        let folder_in_contents = result.contents().iter().any(|o| o.key() == Some("folder/"));
        assert!(
            !folder_in_contents,
            "Expected 'folder/' to be rolled up into CommonPrefixes, but found it in Contents"
        );

        // Stop the RustFS server to ensure proper cleanup
        env.stop_server();
    }

    /// Test ensuring that ListObjectsV2 returns unique keys when an explicit directory marker
    /// exists under the requested prefix and delimiter is not provided.
    ///
    /// Bug Reference: Issue #2439
    /// When both "marker/subdir/" and "marker/subdir/file.txt" exist, listing with
    /// Prefix="marker/" must not duplicate "marker/subdir/file.txt" in Contents.
    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_unique_contents_with_explicit_directory_markers() {
        init_logging();
        info!("Starting test: ListObjectsV2 should return unique keys with explicit directory markers");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-list-unique-contents";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");

        for (key, body) in [
            ("marker/", ByteStream::from_static(b"")),
            ("marker/subdir/", ByteStream::from_static(b"")),
            ("marker/file.txt", ByteStream::from_static(b"content")),
            ("marker/subdir/file.txt", ByteStream::from_static(b"nested")),
        ] {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(body)
                .send()
                .await
                .unwrap_or_else(|err| panic!("Failed to create test object {key}: {err}"));
        }

        let result = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("marker/")
            .send()
            .await
            .expect("Failed to list objects");

        let keys: Vec<String> = result
            .contents()
            .iter()
            .filter_map(|object| object.key().map(ToOwned::to_owned))
            .collect();

        info!("Contents: {:?}", keys);

        assert_eq!(
            keys,
            vec![
                "marker/".to_string(),
                "marker/file.txt".to_string(),
                "marker/subdir/".to_string(),
                "marker/subdir/file.txt".to_string(),
            ]
        );
        assert_eq!(result.key_count(), Some(4));

        env.stop_server();
    }
}
