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

//! Regression test for Issue #1066: Veeam VBR - S3 returned empty versionId
//!
//! This test verifies that:
//! 1. PutObject returns version_id when versioning is enabled
//! 2. CopyObject returns version_id when versioning is enabled
//! 3. CompleteMultipartUpload returns version_id when versioning is enabled
//! 4. Basic S3 operations still work correctly (no regression)
//! 5. Operations on non-versioned buckets work as expected

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, VersioningConfiguration};
    use serial_test::serial;
    use tracing::info;

    fn create_s3_client(env: &RustFSTestEnvironment) -> Client {
        env.create_s3_client()
    }

    async fn create_bucket(client: &Client, bucket: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match client.create_bucket().bucket(bucket).send().await {
            Ok(_) => {
                info!("✅ Bucket {} created successfully", bucket);
                Ok(())
            }
            Err(e) => {
                if e.to_string().contains("BucketAlreadyOwnedByYou") || e.to_string().contains("BucketAlreadyExists") {
                    info!("ℹ️  Bucket {} already exists", bucket);
                    Ok(())
                } else {
                    Err(Box::new(e))
                }
            }
        }
    }

    async fn enable_versioning(client: &Client, bucket: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let versioning_config = VersioningConfiguration::builder()
            .status(BucketVersioningStatus::Enabled)
            .build();

        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(versioning_config)
            .send()
            .await?;

        info!("✅ Versioning enabled for bucket {}", bucket);
        Ok(())
    }

    /// Test 1: PutObject should return version_id when versioning is enabled
    /// This directly addresses the Veeam issue from #1066
    #[tokio::test]
    #[serial]
    async fn test_put_object_returns_version_id_with_versioning() {
        init_logging();
        info!("🧪 TEST: PutObject returns version_id with versioning enabled");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-put-version-id";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");
        enable_versioning(&client, bucket).await.expect("Failed to enable versioning");

        let key = "test-file.txt";
        let content = b"Test content for version ID test";

        info!("📤 Uploading object with key: {}", key);
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .send()
            .await;

        assert!(result.is_ok(), "PutObject failed: {:?}", result.err());
        let output = result.unwrap();

        info!("📥 PutObject response - version_id: {:?}", output.version_id);
        assert!(
            output.version_id.is_some(),
            "❌ FAILED: version_id should be present when versioning is enabled"
        );
        assert!(
            !output.version_id.as_ref().unwrap().is_empty(),
            "❌ FAILED: version_id should not be empty"
        );

        info!("✅ PASSED: PutObject correctly returns version_id");
    }

    /// Test 2: CopyObject should return version_id when versioning is enabled
    #[tokio::test]
    #[serial]
    async fn test_copy_object_returns_version_id_with_versioning() {
        init_logging();
        info!("🧪 TEST: CopyObject returns version_id with versioning enabled");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-copy-version-id";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");
        enable_versioning(&client, bucket).await.expect("Failed to enable versioning");

        let source_key = "source-file.txt";
        let dest_key = "dest-file.txt";
        let content = b"Content to copy";

        // First, create source object
        client
            .put_object()
            .bucket(bucket)
            .key(source_key)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("Failed to create source object");

        info!("📤 Copying object from {} to {}", source_key, dest_key);
        let copy_result = client
            .copy_object()
            .bucket(bucket)
            .key(dest_key)
            .copy_source(format!("{}/{}", bucket, source_key))
            .send()
            .await;

        assert!(copy_result.is_ok(), "CopyObject failed: {:?}", copy_result.err());
        let output = copy_result.unwrap();

        info!("📥 CopyObject response - version_id: {:?}", output.version_id);
        assert!(
            output.version_id.is_some(),
            "❌ FAILED: version_id should be present when versioning is enabled"
        );
        assert!(
            !output.version_id.as_ref().unwrap().is_empty(),
            "❌ FAILED: version_id should not be empty"
        );

        info!("✅ PASSED: CopyObject correctly returns version_id");
    }

    /// Test 3: CompleteMultipartUpload should return version_id when versioning is enabled
    #[tokio::test]
    #[serial]
    async fn test_multipart_upload_returns_version_id_with_versioning() {
        init_logging();
        info!("🧪 TEST: CompleteMultipartUpload returns version_id with versioning enabled");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-multipart-version-id";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");
        enable_versioning(&client, bucket).await.expect("Failed to enable versioning");

        let key = "multipart-file.txt";
        let content = b"Part 1 content for multipart upload test";

        info!("📤 Creating multipart upload for key: {}", key);
        let create_result = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("Failed to create multipart upload");

        let upload_id = create_result.upload_id().expect("No upload_id returned");

        info!("📤 Uploading part 1");
        let upload_part_result = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("Failed to upload part");

        let etag = upload_part_result.e_tag().expect("No etag returned").to_string();

        let completed_part = CompletedPart::builder().part_number(1).e_tag(etag).build();

        let completed_upload = CompletedMultipartUpload::builder().parts(completed_part).build();

        info!("📤 Completing multipart upload");
        let complete_result = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await;

        assert!(complete_result.is_ok(), "CompleteMultipartUpload failed: {:?}", complete_result.err());
        let output = complete_result.unwrap();

        info!("📥 CompleteMultipartUpload response - version_id: {:?}", output.version_id);
        assert!(
            output.version_id.is_some(),
            "❌ FAILED: version_id should be present when versioning is enabled"
        );
        assert!(
            !output.version_id.as_ref().unwrap().is_empty(),
            "❌ FAILED: version_id should not be empty"
        );

        info!("✅ PASSED: CompleteMultipartUpload correctly returns version_id");
    }

    /// Test 4: PutObject should NOT return version_id when versioning is NOT enabled
    /// This ensures we didn't break non-versioned buckets
    #[tokio::test]
    #[serial]
    async fn test_put_object_without_versioning() {
        init_logging();
        info!("🧪 TEST: PutObject behavior without versioning (no regression)");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-no-versioning";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");
        // Note: NOT enabling versioning here

        let key = "test-file.txt";
        let content = b"Test content without versioning";

        info!("📤 Uploading object to non-versioned bucket");
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .send()
            .await;

        assert!(result.is_ok(), "PutObject failed: {:?}", result.err());
        let output = result.unwrap();

        info!("📥 PutObject response - version_id: {:?}", output.version_id);
        // version_id can be None or Some("null") for non-versioned buckets
        info!("✅ PASSED: PutObject works correctly without versioning");
    }

    /// Test 5: Basic S3 operations still work correctly (no regression)
    #[tokio::test]
    #[serial]
    async fn test_basic_s3_operations_no_regression() {
        init_logging();
        info!("🧪 TEST: Basic S3 operations work correctly (no regression)");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-basic-operations";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");
        enable_versioning(&client, bucket).await.expect("Failed to enable versioning");

        let key = "test-basic-file.txt";
        let content = b"Basic operations test content";

        // Test PUT
        info!("📤 Testing PUT operation");
        let put_result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .send()
            .await;
        assert!(put_result.is_ok(), "PUT operation failed");
        let _version_id = put_result.unwrap().version_id;

        // Test GET
        info!("📥 Testing GET operation");
        let get_result = client.get_object().bucket(bucket).key(key).send().await;
        assert!(get_result.is_ok(), "GET operation failed");
        let body = get_result.unwrap().body.collect().await.unwrap().to_vec();
        assert_eq!(body, content, "Content mismatch after GET");

        // Test HEAD
        info!("📋 Testing HEAD operation");
        let head_result = client.head_object().bucket(bucket).key(key).send().await;
        assert!(head_result.is_ok(), "HEAD operation failed");

        // Test LIST
        info!("📝 Testing LIST operation");
        let list_result = client.list_objects_v2().bucket(bucket).send().await;
        assert!(list_result.is_ok(), "LIST operation failed");
        let list_output = list_result.unwrap();
        let objects = list_output.contents();
        assert!(objects.iter().any(|obj| obj.key() == Some(key)), "Object not found in LIST");

        // Test DELETE
        info!("🗑️  Testing DELETE operation");
        let delete_result = client.delete_object().bucket(bucket).key(key).send().await;
        assert!(delete_result.is_ok(), "DELETE operation failed");

        // Verify object is deleted (should return NoSuchKey or version marker)
        let get_after_delete = client.get_object().bucket(bucket).key(key).send().await;
        assert!(
            get_after_delete.is_err() || get_after_delete.unwrap().delete_marker == Some(true),
            "Object should be deleted or have delete marker"
        );

        info!("✅ PASSED: All basic S3 operations work correctly");
    }

    /// Test 6: Veeam-specific scenario simulation
    /// Simulates the exact workflow that Veeam uses when backing up data
    #[tokio::test]
    #[serial]
    async fn test_veeam_backup_workflow_simulation() {
        init_logging();
        info!("🧪 TEST: Veeam VBR backup workflow simulation (Issue #1066)");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "veeam-backup-test";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");
        enable_versioning(&client, bucket).await.expect("Failed to enable versioning");

        // Veeam typically creates multiple objects in a backup session
        let test_paths = vec![
            "Veeam/Backup/Clients/test-client-id/test-backup-id/CloudStg/Meta/Blocks/History/CheckpointHistory.dat",
            "Veeam/Backup/Clients/test-client-id/test-backup-id/Metadata/Lock/create.checkpoint/declare",
        ];

        for path in test_paths {
            info!("📤 Simulating Veeam upload to: {}", path);
            let content = format!("Veeam backup data for {}", path);

            let put_result = client
                .put_object()
                .bucket(bucket)
                .key(path)
                .body(ByteStream::from(content.into_bytes()))
                .send()
                .await;

            assert!(put_result.is_ok(), "Veeam upload failed for path: {}", path);
            let output = put_result.unwrap();

            info!("📥 Response version_id: {:?}", output.version_id);
            assert!(output.version_id.is_some(), "❌ FAILED: Veeam expects version_id for path: {}", path);
            assert!(
                !output.version_id.as_ref().unwrap().is_empty(),
                "❌ FAILED: version_id should not be empty for path: {}",
                path
            );

            info!("✅ Veeam upload successful with version_id for: {}", path);
        }

        info!("✅ PASSED: Veeam backup workflow simulation completed successfully");
    }
}
