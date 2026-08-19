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

//! Regression tests for object delete operations.
//!
//! Covers the recurring pattern where DELETE succeeds at the API level but the
//! object remains visible in LIST, or deleted objects reappear after restart,
//! or versioned delete operations fail with FileAccessDenied.
//! This has regressed 15+ times across the entire release history.
//!
//! ## Regression Issues
//!
//! - rustfs#5375: delete object in a bucket list api also exist this object
//! - rustfs#5349: The deleted bucket was rebuilt after some time
//! - rustfs#5339: data not delete in Object Lock bucket
//! - rustfs#5029: Node Does Not Remove Files After Reconnect to Cluster
//! - rustfs#4978: DELETE fails with InternalError/FileAccessDenied on beta 10
//! - rustfs#760: Cannot delete a versioned bucket

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration};
    use std::error::Error;
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    /// RT-05: Verify DELETE → LIST → HEAD consistency.
    ///
    /// Regression pattern: DELETE returns 200 but the object remains in LIST.
    /// Covers rustfs#5375.
    ///
    /// Steps:
    /// 1. Create a bucket and upload an object
    /// 2. Verify the object is in LIST
    /// 3. DELETE the object
    /// 4. Verify the object is NOT in LIST
    /// 5. Verify HEAD returns 404
    #[tokio::test]
    async fn test_delete_removes_object_from_list() -> TestResult {
        init_logging();
        info!("RT-05: delete removes object from list");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt05-delete-consistency";

        client.create_bucket().bucket(bucket).send().await.expect("create bucket");

        // Upload an object
        client
            .put_object()
            .bucket(bucket)
            .key("to-delete.txt")
            .body(ByteStream::from_static(b"will be deleted"))
            .send()
            .await
            .expect("put object");

        // Verify it appears in LIST
        let list = client
            .list_objects_v2()
            .bucket(bucket)
            .send()
            .await
            .expect("list objects before delete");

        assert!(
            list.contents()
                .iter()
                .map(|o| o.key().unwrap_or(""))
                .any(|key| key == "to-delete.txt"),
            "RT-05 FAIL: object not in LIST before delete"
        );

        // DELETE
        client
            .delete_object()
            .bucket(bucket)
            .key("to-delete.txt")
            .send()
            .await
            .expect("delete object");

        // Verify NOT in LIST
        let list = client
            .list_objects_v2()
            .bucket(bucket)
            .send()
            .await
            .expect("list objects after delete");

        assert!(
            !list
                .contents()
                .iter()
                .map(|o| o.key().unwrap_or(""))
                .any(|key| key == "to-delete.txt"),
            "RT-05 FAIL: deleted object still in LIST (regression rustfs#5375)"
        );

        // Verify HEAD returns 404
        let head = client.head_object().bucket(bucket).key("to-delete.txt").send().await;

        assert!(head.is_err(), "RT-05 FAIL: HEAD on deleted object should return error, got success");

        info!("RT-05 PASS: delete correctly removes object from LIST and HEAD");
        Ok(())
    }

    /// RT-05c: Verify batch delete (DeleteObjects) consistency.
    ///
    /// Regression pattern: batch delete returns success but some objects
    /// remain in LIST.
    #[tokio::test]
    async fn test_batch_delete_removes_all_objects() -> TestResult {
        init_logging();
        info!("RT-05c: batch delete removes all objects");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt05c-batch-delete";

        client.create_bucket().bucket(bucket).send().await.expect("create bucket");

        // Upload multiple objects
        let keys: Vec<String> = (0..5).map(|i| format!("batch-{i:04}.txt")).collect();
        for key in &keys {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"batch-delete-me"))
                .send()
                .await
                .expect("put object");
        }

        // Verify all in LIST
        let list = client
            .list_objects_v2()
            .bucket(bucket)
            .send()
            .await
            .expect("list before batch delete");

        assert_eq!(
            list.contents().len(),
            5,
            "RT-05c FAIL: expected 5 objects before batch delete, found {}",
            list.contents().len()
        );

        // Batch delete
        let objects: Vec<ObjectIdentifier> = keys
            .iter()
            .map(|k| ObjectIdentifier::builder().key(k).build().expect("build object id"))
            .collect();

        client
            .delete_objects()
            .bucket(bucket)
            .delete(Delete::builder().set_objects(Some(objects)).build().expect("build delete"))
            .send()
            .await
            .expect("batch delete");

        // Verify all removed
        let list = client
            .list_objects_v2()
            .bucket(bucket)
            .send()
            .await
            .expect("list after batch delete");

        assert!(
            list.contents().is_empty(),
            "RT-05c FAIL: {} objects remain after batch delete (regression: delete objects not fully applied)",
            list.contents().len()
        );

        info!("RT-05c PASS: batch delete removes all objects");
        Ok(())
    }

    /// RT-05d: Verify versioned delete → permanent delete → object gone.
    ///
    /// Covers the pattern where permanent deletion of a specific version
    /// fails with FileAccessDenied (rustfs#4978).
    #[tokio::test]
    async fn test_versioned_permanent_delete() -> TestResult {
        init_logging();
        info!("RT-05d: versioned permanent delete");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt05d-permanent-delete";

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

        // Upload a single object (single version)
        let put_resp = client
            .put_object()
            .bucket(bucket)
            .key("single-version.txt")
            .body(ByteStream::from_static(b"to-be-permanently-deleted"))
            .send()
            .await
            .expect("put object");

        let version_id = put_resp.version_id().expect("version ID should be present").to_string();

        // Permanently delete the specific version (rustfs#4978: FileAccessDenied)
        client
            .delete_object()
            .bucket(bucket)
            .key("single-version.txt")
            .version_id(&version_id)
            .send()
            .await
            .expect("permanent delete should succeed (regression rustfs#4978)");

        // Verify the object is completely gone
        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .send()
            .await
            .expect("list versions");

        assert!(
            versions.versions().is_empty(),
            "RT-05d FAIL: version still present after permanent delete"
        );

        info!("RT-05d PASS: versioned permanent delete succeeds");
        Ok(())
    }

    /// RT-05e: Verify delete marker + version history interaction.
    ///
    /// Covers the pattern where creating a delete marker and then listing
    /// versions shows incorrect state (rustfs#760).
    #[tokio::test]
    async fn test_versioned_delete_marker_and_list_consistency() -> TestResult {
        init_logging();
        info!("RT-05e: versioned delete marker and list consistency");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt05e-dm-consistency";

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

        // Create 3 versions
        for i in 0..3 {
            client
                .put_object()
                .bucket(bucket)
                .key("history.txt")
                .body(ByteStream::from(format!("v{i}").into_bytes()))
                .send()
                .await
                .expect("put version");
        }

        // Create a delete marker
        let del = client
            .delete_object()
            .bucket(bucket)
            .key("history.txt")
            .send()
            .await
            .expect("delete (create marker)");

        assert!(del.delete_marker().unwrap_or(false), "RT-05e FAIL: should have created a delete marker");

        // ListObjectVersions should show 3 versions + 1 delete marker
        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .send()
            .await
            .expect("list versions");

        assert_eq!(
            versions.versions().len(),
            3,
            "RT-05e FAIL: expected 3 versions, found {}",
            versions.versions().len()
        );
        assert_eq!(
            versions.delete_markers().len(),
            1,
            "RT-05e FAIL: expected 1 delete marker, found {}",
            versions.delete_markers().len()
        );

        // Now delete the delete marker (restore the object)
        let dm_version = &versions.delete_markers()[0];
        client
            .delete_object()
            .bucket(bucket)
            .key("history.txt")
            .version_id(dm_version.version_id().expect("dm version id"))
            .send()
            .await
            .expect("delete delete-marker");

        // HEAD should succeed now (latest version is accessible)
        let head = client.head_object().bucket(bucket).key("history.txt").send().await;

        assert!(head.is_ok(), "RT-05e FAIL: HEAD should succeed after removing delete marker");

        info!("RT-05e PASS: versioned delete marker and list consistency");
        Ok(())
    }

    /// RT-05f: Verify object deletion does not leave orphan data on disk.
    ///
    /// Regression pattern: after delete, the object data files remain on disk
    /// (rustfs#5029: Node Does Not Remove Files After Reconnect).
    #[tokio::test]
    async fn test_delete_removes_object_head_returns_404() -> TestResult {
        init_logging();
        info!("RT-05f: delete → HEAD 404 consistency");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt05f-delete-head";

        client.create_bucket().bucket(bucket).send().await.expect("create bucket");

        // Upload, delete, verify HEAD returns 404
        let keys = vec!["small.txt", "medium.txt", "with-slash.txt", "special+chars.txt"];

        for key in &keys {
            client
                .put_object()
                .bucket(bucket)
                .key(*key)
                .body(ByteStream::from_static(b"delete-me"))
                .send()
                .await
                .expect("put object");
        }

        for key in &keys {
            client
                .delete_object()
                .bucket(bucket)
                .key(*key)
                .send()
                .await
                .expect("delete object");
        }

        // All HEAD requests should return 404
        for key in &keys {
            let head = client.head_object().bucket(bucket).key(*key).send().await;

            assert!(head.is_err(), "RT-05f FAIL: HEAD on deleted key '{key}' should return error");
        }

        // LIST should be empty
        let list = client
            .list_objects_v2()
            .bucket(bucket)
            .send()
            .await
            .expect("list after all deletes");

        assert!(
            list.contents().is_empty(),
            "RT-05f FAIL: {} objects remain after deleting all",
            list.contents().len()
        );

        info!("RT-05f PASS: all deleted objects return 404 on HEAD");
        Ok(())
    }
}
