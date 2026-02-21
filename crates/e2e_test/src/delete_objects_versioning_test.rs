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

//! Regression test for Issue #1878:
//! "In a versioned Bucket, DeleteMarkers are not appearing straight after
//! a delete_objects is called."
//!
//! Root cause: `delete_versions_internal` wrote new xl.meta to disk via
//! `write_all_private` without invalidating the `GlobalFileCache`. Subsequent
//! calls to `read_metadata` returned the stale cached xl.meta (without the
//! delete marker), making `list_object_versions` show the old version as
//! `IsLatest=true` rather than the new delete marker.
//!
//! Fix: `write_all_private` now calls `get_global_file_cache().invalidate()`
//! after every successful write, and `rename_data` also invalidates the cache
//! for the destination path after the atomic rename.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::types::{BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration};
    use serial_test::serial;
    use tracing::info;

    fn create_s3_client(env: &RustFSTestEnvironment) -> Client {
        env.create_s3_client()
    }

    /// Regression test for Issue #1878.
    ///
    /// Verifies that after calling `delete_objects` (the batch plural API) on
    /// a versioned bucket, calling `list_object_versions` **immediately** (with
    /// no sleep) returns the newly-created DeleteMarker with `is_latest = true`.
    #[tokio::test]
    #[serial]
    async fn test_delete_objects_delete_marker_immediately_visible() {
        init_logging();
        info!("🧪 TEST: DeleteMarker from delete_objects is immediately visible via list_object_versions");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-delete-marker-visibility";
        let key = "test-prefix/test-object.txt";

        // 1. Create bucket
        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("Failed to create bucket");
        info!("✅ Bucket created: {}", bucket);

        // 2. Enable versioning
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
            .expect("Failed to enable versioning");
        info!("✅ Versioning enabled");

        // 3. Put an object
        let put_resp = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello versioned world\n"))
            .send()
            .await
            .expect("Failed to put object");
        let original_version_id = put_resp.version_id().map(str::to_string);
        info!("✅ Object put, version_id: {:?}", original_version_id);
        assert!(
            original_version_id.is_some(),
            "PutObject should return a version_id in a versioned bucket"
        );

        // 4. List versions – should show exactly 1 Version with is_latest = true
        let list_before = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .expect("Failed to list object versions before delete");

        let versions_before = list_before.versions().to_vec();
        let markers_before = list_before.delete_markers().to_vec();
        assert_eq!(versions_before.len(), 1, "Should have 1 version before delete");
        assert!(markers_before.is_empty(), "Should have no delete markers before delete");
        assert_eq!(
            versions_before[0].is_latest(),
            Some(true),
            "Original version should be latest before delete"
        );
        info!("✅ Verified: 1 version, 0 delete markers before delete_objects");

        // 5. Call delete_objects (plural, no version_id → creates a delete marker)
        let del_resp = client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .objects(
                        ObjectIdentifier::builder()
                            .key(key)
                            .build()
                            .expect("Failed to build object identifier"),
                    )
                    .build()
                    .expect("Failed to build delete request"),
            )
            .send()
            .await
            .expect("Failed to call delete_objects");

        let deleted = del_resp.deleted();
        assert_eq!(deleted.len(), 1, "delete_objects should report 1 deleted object");
        let delete_marker_version_id = deleted[0].delete_marker_version_id().map(str::to_string);
        assert!(
            deleted[0].delete_marker() == Some(true),
            "delete_objects should report delete_marker=true for versioned delete"
        );
        assert!(
            delete_marker_version_id.is_some(),
            "delete_objects should return a delete_marker_version_id"
        );
        info!("✅ delete_objects succeeded, delete_marker_version_id: {:?}", delete_marker_version_id);

        // 6. List versions IMMEDIATELY (no sleep!) – this is the key regression assertion.
        //    Before the fix, the stale file cache caused the listing to return the original
        //    version as is_latest=true without any delete marker.
        let list_after = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .expect("Failed to list object versions immediately after delete_objects");

        let versions_after = list_after.versions().to_vec();
        let markers_after = list_after.delete_markers().to_vec();

        info!("Versions after delete_objects: {:?}", versions_after.len());
        info!("Delete markers after delete_objects: {:?}", markers_after.len());

        // The delete marker must be visible immediately – no delay required.
        assert_eq!(
            markers_after.len(),
            1,
            "list_object_versions should return exactly 1 delete marker IMMEDIATELY after delete_objects (regression #1878)"
        );
        assert_eq!(markers_after[0].is_latest(), Some(true), "The delete marker should be is_latest=true");
        assert_eq!(
            markers_after[0].version_id().map(str::to_string),
            delete_marker_version_id,
            "The delete marker version_id should match the one returned by delete_objects"
        );

        // The original version should still be present, but is_latest = false
        assert_eq!(versions_after.len(), 1, "The original version should still be present");
        assert_eq!(
            versions_after[0].is_latest(),
            Some(false),
            "Original version should be is_latest=false after delete marker is created"
        );

        info!("✅ TEST PASSED: Delete marker is immediately visible after delete_objects (no sleep needed)");
    }

    /// Additional regression check: verify that multiple objects deleted via
    /// a single `delete_objects` call all have their delete markers visible
    /// immediately afterwards.
    #[tokio::test]
    #[serial]
    async fn test_delete_objects_multiple_keys_delete_markers_immediately_visible() {
        init_logging();
        info!("🧪 TEST: Multiple delete markers from delete_objects are immediately visible");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-multi-delete-marker-visibility";
        let keys = ["obj1.txt", "obj2.txt", "obj3.txt"];

        // Create bucket and enable versioning
        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("Failed to create bucket");

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
            .expect("Failed to enable versioning");

        // Put all objects
        for key in &keys {
            client
                .put_object()
                .bucket(bucket)
                .key(*key)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"data"))
                .send()
                .await
                .unwrap_or_else(|_| panic!("Failed to put object {}", key));
        }
        info!("✅ Put {} objects", keys.len());

        // Delete all objects with a single delete_objects call
        let identifiers: Vec<ObjectIdentifier> = keys
            .iter()
            .map(|k| {
                ObjectIdentifier::builder()
                    .key(*k)
                    .build()
                    .expect("Failed to build ObjectIdentifier")
            })
            .collect();

        let del_resp = client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(identifiers))
                    .build()
                    .expect("Failed to build delete"),
            )
            .send()
            .await
            .expect("Failed to call delete_objects");

        assert_eq!(del_resp.deleted().len(), keys.len(), "All keys should be reported deleted");
        info!("✅ delete_objects deleted {} objects", keys.len());

        // Immediately list all versions and check each key has a delete marker
        let list_resp = client
            .list_object_versions()
            .bucket(bucket)
            .send()
            .await
            .expect("Failed to list object versions");

        let markers = list_resp.delete_markers().to_vec();
        assert_eq!(
            markers.len(),
            keys.len(),
            "Each deleted key should have an immediately-visible delete marker (regression #1878)"
        );

        for marker in &markers {
            assert_eq!(marker.is_latest(), Some(true), "Each delete marker should be is_latest=true");
        }

        info!("✅ TEST PASSED: All {} delete markers are immediately visible", keys.len());
    }
}
