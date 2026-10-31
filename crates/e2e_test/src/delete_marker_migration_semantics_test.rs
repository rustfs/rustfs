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
    use aws_sdk_s3::config::http::HttpResponse;
    use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, VersioningConfiguration};

    fn assert_not_found<E: ProvideErrorMetadata + std::fmt::Debug>(error: &SdkError<E, HttpResponse>) {
        assert_eq!(error.raw_response().map(|r| r.status().as_u16()), Some(404), "{error:?}");
        assert_eq!(
            error.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("NoSuchKey"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn conditional_writes_cannot_replace_delete_marker_with_if_match() {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server(vec![]).await.expect("start RustFS");
        let client = env.create_s3_client();
        let bucket = "conditional-delete-marker-proof";
        create_versioned_bucket(&client, bucket).await;
        client
            .put_object()
            .bucket(bucket)
            .key("source")
            .body(ByteStream::from_static(b"source"))
            .send()
            .await
            .expect("write copy source");

        for operation in ["put", "copy", "multipart"] {
            let old = client
                .put_object()
                .bucket(bucket)
                .key(operation)
                .body(ByteStream::from_static(b"old"))
                .send()
                .await
                .expect("write historical version");
            let old_etag = old.e_tag().expect("historical ETag");
            let marker = client
                .delete_object()
                .bucket(bucket)
                .key(operation)
                .send()
                .await
                .expect("create delete marker");
            let marker_id = marker.version_id().expect("delete marker version");

            let (upload_id, completed) = if operation == "multipart" {
                let upload = client
                    .create_multipart_upload()
                    .bucket(bucket)
                    .key(operation)
                    .send()
                    .await
                    .expect("initiate multipart upload");
                let upload_id = upload.upload_id().expect("upload ID").to_owned();
                let part = client
                    .upload_part()
                    .bucket(bucket)
                    .key(operation)
                    .upload_id(&upload_id)
                    .part_number(1)
                    .body(ByteStream::from_static(b"new"))
                    .send()
                    .await
                    .expect("upload part");
                let completed = CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .set_e_tag(part.e_tag().map(str::to_owned))
                            .build(),
                    )
                    .build();
                (Some(upload_id), Some(completed))
            } else {
                (None, None)
            };

            for etag in [old_etag, "\"00000000000000000000000000000000\""] {
                match operation {
                    "put" => {
                        let err = client
                            .put_object()
                            .bucket(bucket)
                            .key(operation)
                            .if_match(etag)
                            .body(ByteStream::from_static(b"new"))
                            .send()
                            .await
                            .expect_err("If-Match must reject marker");
                        assert_not_found(&err);
                    }
                    "copy" => {
                        let err = client
                            .copy_object()
                            .bucket(bucket)
                            .key(operation)
                            .copy_source(format!("{bucket}/source"))
                            .if_match(etag)
                            .send()
                            .await
                            .expect_err("If-Match copy must reject marker");
                        assert_not_found(&err);
                    }
                    _ => {
                        let err = client
                            .complete_multipart_upload()
                            .bucket(bucket)
                            .key(operation)
                            .upload_id(upload_id.as_deref().expect("upload ID"))
                            .multipart_upload(completed.clone().expect("completed parts"))
                            .if_match(etag)
                            .send()
                            .await
                            .expect_err("If-Match completion must reject marker");
                        assert_not_found(&err);
                    }
                }
                let versions = client
                    .list_object_versions()
                    .bucket(bucket)
                    .prefix(operation)
                    .send()
                    .await
                    .expect("list versions after rejected write");
                assert_eq!(versions.versions().len(), 1, "{operation}: failed write created a version");
                assert_eq!(versions.delete_markers().len(), 1);
                assert_eq!(versions.delete_markers()[0].version_id(), Some(marker_id));
                assert_eq!(versions.delete_markers()[0].is_latest(), Some(true));
            }

            match operation {
                "put" => {
                    client
                        .put_object()
                        .bucket(bucket)
                        .key(operation)
                        .if_none_match("*")
                        .body(ByteStream::from_static(b"new"))
                        .send()
                        .await
                        .expect("If-None-Match PUT over marker");
                }
                "copy" => {
                    client
                        .copy_object()
                        .bucket(bucket)
                        .key(operation)
                        .copy_source(format!("{bucket}/source"))
                        .if_none_match("*")
                        .send()
                        .await
                        .expect("If-None-Match copy over marker");
                }
                _ => {
                    client
                        .complete_multipart_upload()
                        .bucket(bucket)
                        .key(operation)
                        .upload_id(upload_id.as_deref().expect("upload ID"))
                        .multipart_upload(completed.expect("completed parts"))
                        .if_none_match("*")
                        .send()
                        .await
                        .expect("If-None-Match completion over marker");
                }
            }
            let current = client
                .get_object()
                .bucket(bucket)
                .key(operation)
                .send()
                .await
                .expect("read successful conditional write")
                .body
                .collect()
                .await
                .expect("read object body")
                .into_bytes();
            let expected: &[u8] = if operation == "copy" { b"source" } else { b"new" };
            assert_eq!(current.as_ref(), expected, "{operation}: unexpected current object");
        }
    }

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

    async fn assert_current_list_hides_delete_marker(client: &Client, bucket: &str, key: &str) {
        let listed = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .expect("list current objects after delete marker");

        assert!(
            listed.contents().iter().all(|object| object.key() != Some(key)),
            "ListObjectsV2 must hide an object whose latest version is a delete marker"
        );
    }

    #[tokio::test]
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
        assert_current_list_hides_delete_marker(&client, bucket, key).await;
    }

    #[tokio::test]
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
        let listed_before_delete = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .expect("list current object before creating delete marker");
        assert!(
            listed_before_delete.contents().iter().any(|object| object.key() == Some(key)),
            "ListObjectsV2 must include the current object before it is deleted"
        );

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
        assert_current_list_hides_delete_marker(&client, bucket, key).await;

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
