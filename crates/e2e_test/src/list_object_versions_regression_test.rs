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

//! Regression test for Issue #2252:
//! "ListObjectVersions misses the newest version after create -> delete -> create."

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    use tracing::info;

    fn create_s3_client(env: &RustFSTestEnvironment) -> Client {
        env.create_s3_client()
    }

    #[tokio::test]
    async fn test_list_object_versions_immediately_returns_latest_put_after_delete_marker() {
        init_logging();
        info!("🧪 TEST: ListObjectVersions returns the newest version immediately after put -> delete -> put");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-list-object-versions-2252";
        let key = "test-prefix/test-object.txt";

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

        let first_put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"first version"))
            .send()
            .await
            .expect("Failed to put first object version");
        let first_version_id = first_put
            .version_id()
            .map(str::to_string)
            .expect("First put should return a version_id");

        let delete_resp = client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("Failed to create delete marker");
        let delete_marker_version_id = delete_resp
            .version_id()
            .map(str::to_string)
            .expect("DeleteObject should return a delete marker version_id");

        let second_put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"second version"))
            .send()
            .await
            .expect("Failed to put second object version");
        let second_version_id = second_put
            .version_id()
            .map(str::to_string)
            .expect("Second put should return a version_id");

        let first_listing = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .expect("Failed to list object versions immediately after second put");
        let second_listing = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .expect("Failed to list object versions a second time");

        let first_versions = first_listing.versions().to_vec();
        let first_delete_markers = first_listing.delete_markers().to_vec();
        let second_versions = second_listing.versions().to_vec();
        let second_delete_markers = second_listing.delete_markers().to_vec();

        info!(
            "First listing: {} versions, {} delete markers",
            first_versions.len(),
            first_delete_markers.len()
        );
        info!(
            "Second listing: {} versions, {} delete markers",
            second_versions.len(),
            second_delete_markers.len()
        );

        assert_eq!(
            first_versions.len(),
            2,
            "First ListObjectVersions call should return both object versions immediately (regression #2252)"
        );
        assert_eq!(
            first_delete_markers.len(),
            1,
            "First ListObjectVersions call should return the delete marker immediately (regression #2252)"
        );
        assert_eq!(
            second_versions.len(),
            2,
            "Second ListObjectVersions call should still return both object versions"
        );
        assert_eq!(
            second_delete_markers.len(),
            1,
            "Second ListObjectVersions call should still return the delete marker"
        );

        let first_latest_version = first_versions
            .iter()
            .find(|version| version.version_id() == Some(second_version_id.as_str()))
            .expect("First listing should include the newest object version");
        assert_eq!(
            first_latest_version.is_latest(),
            Some(true),
            "Newest object version should be latest on the first listing"
        );

        let first_original_version = first_versions
            .iter()
            .find(|version| version.version_id() == Some(first_version_id.as_str()))
            .expect("First listing should include the original object version");
        assert_eq!(
            first_original_version.is_latest(),
            Some(false),
            "Original object version should no longer be latest"
        );

        let first_delete_marker = first_delete_markers
            .iter()
            .find(|marker| marker.version_id() == Some(delete_marker_version_id.as_str()))
            .expect("First listing should include the delete marker");
        assert_eq!(
            first_delete_marker.is_latest(),
            Some(false),
            "Delete marker should no longer be latest after the second put"
        );

        let expected = [
            (second_version_id.as_str(), false),
            (delete_marker_version_id.as_str(), true),
            (first_version_id.as_str(), false),
        ];
        let mut key_marker = None;
        let mut version_marker = None;
        for (index, (version_id, is_delete_marker)) in expected.iter().copied().enumerate() {
            let page = client
                .list_object_versions()
                .bucket(bucket)
                .prefix(key)
                .max_keys(1)
                .set_key_marker(key_marker)
                .set_version_id_marker(version_marker)
                .send()
                .await
                .expect("Failed to list a page of object versions");
            let entry = match (page.versions(), page.delete_markers()) {
                ([version], []) => (version.key(), version.version_id(), false),
                ([], [marker]) => (marker.key(), marker.version_id(), true),
                _ => panic!("Expected exactly one version or delete marker on page {}", index + 1),
            };
            assert_eq!(entry, (Some(key), Some(version_id), is_delete_marker));
            let more = index + 1 < expected.len();
            assert_eq!(page.is_truncated(), Some(more));
            if more {
                assert!(page.next_key_marker().is_some_and(|marker| !marker.is_empty()));
                assert!(page.next_version_id_marker().is_some_and(|marker| !marker.is_empty()));
            } else {
                assert!(page.next_key_marker().is_none());
                assert!(page.next_version_id_marker().is_none());
            }
            key_marker = page.next_key_marker().map(str::to_owned);
            version_marker = page.next_version_id_marker().map(str::to_owned);
        }

        let error = client
            .list_object_versions()
            .bucket(bucket)
            .version_id_marker(&first_version_id)
            .send()
            .await
            .expect_err("A version ID marker requires a key marker");
        assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(400));
        assert_eq!(error.as_service_error().and_then(ProvideErrorMetadata::code), Some("InvalidArgument"));
    }

    #[tokio::test]
    async fn test_list_object_versions_prefix_with_marker_object_returns_children() {
        init_logging();
        info!("🧪 TEST: ListObjectVersions returns prefix children when a marker object also exists");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-list-versions-prefix-marker";
        let marker_key = "data01";
        let child_keys = [
            "data01/meta/dump-2026-04-08-053205.json.gz",
            "data01/meta/dump-2026-04-08-063209.json.gz",
        ];

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
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await
            .expect("Failed to suspend versioning");

        client
            .put_object()
            .bucket(bucket)
            .key(marker_key)
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .expect("Failed to put marker object");

        for key in child_keys {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"payload"))
                .send()
                .await
                .expect("Failed to put child object");
        }

        let listing = client
            .list_object_versions()
            .bucket(bucket)
            .prefix("data01/")
            .send()
            .await
            .expect("Failed to list object versions by prefix");

        let version_keys: Vec<_> = listing.versions().iter().filter_map(|version| version.key()).collect();

        assert_eq!(
            version_keys.len(),
            child_keys.len(),
            "ListObjectVersions with a trailing slash prefix should include child objects even when the marker object exists"
        );

        for key in child_keys {
            assert!(
                version_keys.contains(&key),
                "ListObjectVersions(prefix=data01/) should include child object {key}"
            );
        }
    }
}
