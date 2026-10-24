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
//! Root cause: metadata updates could become temporarily invisible to
//! `list_object_versions`, so the old version was still reported as
//! `IsLatest=true` instead of the newly-created delete marker.
//!
//! Fix: metadata write, delete, and rename paths now make the updated
//! `xl.meta` immediately visible, and the old file-cache shortcut has been
//! removed from the read path.

use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::types::{BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration};
use std::collections::BTreeMap;

async fn delete_exact_marker_versions(
    client: &Client,
    bucket: &str,
    key: &str,
    versions: &[(String, bool)],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let objects = versions
        .iter()
        .map(|(version, _)| ObjectIdentifier::builder().key(key).version_id(version).build())
        .collect::<Result<Vec<_>, _>>()?;
    let response = client
        .delete_objects()
        .bucket(bucket)
        .delete(Delete::builder().set_objects(Some(objects)).build()?)
        .send()
        .await?;
    assert!(response.errors().is_empty(), "explicit batch returned errors: {response:?}");
    let mut actual = BTreeMap::new();
    for deleted in response.deleted() {
        assert_eq!(deleted.key(), Some(key));
        *actual
            .entry((
                deleted.version_id().ok_or("Deleted entry omitted version ID")?.to_string(),
                deleted.delete_marker().unwrap_or(false),
                deleted.delete_marker_version_id().map(str::to_string),
            ))
            .or_insert(0_usize) += 1;
    }
    let mut expected = BTreeMap::new();
    for (version, marker) in versions {
        *expected
            .entry((version.clone(), *marker, marker.then(|| version.clone())))
            .or_insert(0_usize) += 1;
    }
    assert_eq!(actual, expected, "every requested occurrence must report its own version identity");
    Ok(())
}

pub(crate) async fn assert_explicit_marker_batch_cleanup(
    clients: &[Client],
    bucket: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    assert!(!clients.is_empty());
    let clients: Vec<_> = clients
        .iter()
        .map(|client| {
            Client::from_conf(
                client
                    .config()
                    .to_builder()
                    .retry_config(RetryConfig::standard().with_max_attempts(1))
                    .build(),
            )
        })
        .collect();
    let writer = &clients[0];
    let marker_writer = &clients[1 % clients.len()];
    let deleter = &clients[2 % clients.len()];
    let key = "same-key-history";
    writer.create_bucket().bucket(bucket).send().await?;
    writer
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    let mut data = Vec::new();
    for byte in [41_u8, 73_u8] {
        let body = vec![byte; 1024 * 1024];
        let uploaded = writer
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.clone().into())
            .send()
            .await?;
        data.push((uploaded.version_id().ok_or("versioned PUT omitted ID")?.to_string(), body));
    }
    let mut markers = Vec::with_capacity(16);
    for _ in 0..16 {
        let deleted = marker_writer.delete_object().bucket(bucket).key(key).send().await?;
        assert_eq!(deleted.delete_marker(), Some(true));
        markers.push(deleted.version_id().ok_or("marker creation omitted ID")?.to_string());
    }
    let mut requested: Vec<_> = markers.iter().map(|id| (id.clone(), true)).collect();
    requested.push((markers[0].clone(), true));
    requested.push((markers[15].clone(), true));
    delete_exact_marker_versions(deleter, bucket, key, &requested).await?;
    for client in &clients {
        let versions = client.list_object_versions().bucket(bucket).send().await?;
        assert!(versions.delete_markers().is_empty(), "marker survived successful batch: {versions:?}");
        let mut actual: Vec<_> = versions
            .versions()
            .iter()
            .filter_map(|version| version.version_id())
            .collect();
        actual.sort_unstable();
        let mut expected: Vec<_> = data.iter().map(|(id, _)| id.as_str()).collect();
        expected.sort_unstable();
        assert_eq!(actual, expected);
        let current = client.get_object().bucket(bucket).key(key).send().await?;
        assert_eq!(current.body.collect().await?.into_bytes().as_ref(), data[1].1.as_slice());
        let historical = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(&data[0].0)
            .send()
            .await?;
        assert_eq!(historical.body.collect().await?.into_bytes().as_ref(), data[0].1.as_slice());
    }
    let uuid_marker = marker_writer.delete_object().bucket(bucket).key(key).send().await?;
    let uuid_marker = uuid_marker.version_id().ok_or("marker creation omitted UUID")?.to_string();
    writer
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await?;
    let null_marker = marker_writer.delete_object().bucket(bucket).key(key).send().await?;
    assert_eq!(null_marker.delete_marker(), Some(true));
    assert_eq!(null_marker.version_id(), Some("null"));
    let requested = [
        (uuid_marker.clone(), true),
        (data[1].0.clone(), false),
        ("null".to_string(), true),
        (data[0].0.clone(), false),
        (uuid_marker, true),
    ];
    delete_exact_marker_versions(deleter, bucket, key, &requested).await?;
    for client in &clients {
        let versions = client.list_object_versions().bucket(bucket).send().await?;
        assert!(versions.versions().is_empty() && versions.delete_markers().is_empty(), "{versions:?}");
    }
    clients
        .last()
        .expect("nonempty clients")
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::types::{BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration};
    use tracing::info;

    fn create_s3_client(env: &RustFSTestEnvironment) -> Client {
        env.create_s3_client()
    }

    #[tokio::test]
    async fn test_explicit_delete_marker_batch_removes_versions_without_client_retries()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await?;
        super::assert_explicit_marker_batch_cleanup(&[env.create_s3_client()], "explicit-marker-batch").await
    }

    /// Regression test for Issue #1878.
    ///
    /// Verifies that after calling `delete_objects` (the batch plural API) on
    /// a versioned bucket, calling `list_object_versions` **immediately** (with
    /// no sleep) returns the newly-created DeleteMarker with `is_latest = true`.
    #[tokio::test]
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
