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
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    use serial_test::serial;

    async fn create_versioned_bucket(client: &Client, bucket: &str) {
        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("create versioning proof bucket");
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
            .expect("enable versioning for proof bucket");
    }

    async fn assert_current_get_is_delete_marker_not_found(client: &Client, bucket: &str, key: &str) {
        let err = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect_err("current get should fail when latest version is a delete marker");
        let service_err = err.into_service_error();
        let code = service_err.meta().code();
        assert!(
            matches!(code, Some("NoSuchKey") | Some("NotFound")),
            "current get should expose delete-marker not-found semantics, got {service_err:?}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_versioning_only_delete_marker_has_minio_compatible_visibility_for_migration_proof() {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server(vec![]).await.expect("start RustFS");
        let client = env.create_s3_client();
        let bucket = "delete-marker-migration-proof-a";
        let key = "only-delete-marker.txt";

        create_versioned_bucket(&client, bucket).await;

        let delete_marker = client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("create delete marker for absent object");
        let delete_marker_version_id = delete_marker
            .version_id()
            .expect("MinIO-compatible delete marker should have a version id");

        let listed = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .expect("list delete marker only object");
        let versions = listed.versions();
        let markers = listed.delete_markers();

        assert!(versions.is_empty(), "only-delete-marker case must not report data versions");
        assert_eq!(markers.len(), 1, "only-delete-marker case must report the delete marker");
        assert_eq!(markers[0].version_id(), Some(delete_marker_version_id));
        assert_eq!(markers[0].is_latest(), Some(true));
        assert_current_get_is_delete_marker_not_found(&client, bucket, key).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_versioning_delete_marker_plus_history_remains_visible_for_migration_proof() {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server(vec![]).await.expect("start RustFS");
        let client = env.create_s3_client();
        let bucket = "delete-marker-migration-proof-b";
        let key = "delete-marker-with-history.txt";
        let body = b"historical version body";

        create_versioned_bucket(&client, bucket).await;

        let put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await
            .expect("put historical version");
        let data_version_id = put.version_id().expect("put should return data version id");

        let delete_marker = client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("create delete marker over historical version");
        let delete_marker_version_id = delete_marker.version_id().expect("delete marker should have a version id");

        let listed = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .expect("list delete marker plus historical version");
        let versions = listed.versions();
        let markers = listed.delete_markers();

        assert_eq!(versions.len(), 1, "history case must report the historical data version");
        assert_eq!(markers.len(), 1, "history case must report the latest delete marker");
        assert_eq!(versions[0].version_id(), Some(data_version_id));
        assert_eq!(versions[0].is_latest(), Some(false));
        assert_eq!(markers[0].version_id(), Some(delete_marker_version_id));
        assert_eq!(markers[0].is_latest(), Some(true));
        assert_current_get_is_delete_marker_not_found(&client, bucket, key).await;

        let historical = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(data_version_id)
            .send()
            .await
            .expect("historical version get should succeed");
        let bytes = historical
            .body
            .collect()
            .await
            .expect("collect historical version body")
            .into_bytes();
        assert_eq!(bytes.as_ref(), body);
    }
}
