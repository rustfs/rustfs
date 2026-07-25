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

//! CopyObject metadata replacement regression tests.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::{ByteStream, DateTime, DateTimeFormat};
    use aws_sdk_s3::types::{
        BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, MetadataDirective, StorageClass, VersioningConfiguration,
    };
    use serial_test::serial;
    use tracing::info;

    #[tokio::test]
    #[serial]
    async fn copy_object_standard_metadata_copy_replace_and_clear() {
        init_logging();
        info!("Issue #2789: self-copy metadata replacement must preserve object data");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let bucket = "self-copy-metadata-replace-test";
        let key = "assets/chunk-2F3R7JUG.js";
        let content = b"console.log('metadata replacement should keep object data readable');";
        let source_expires = DateTime::from_secs(1_893_456_000);
        let source_expires_http_date = source_expires
            .fmt(DateTimeFormat::HttpDate)
            .expect("Test timestamp should format as an HTTP date");
        let replacement_expires = DateTime::from_secs(1_924_992_000);
        let replacement_expires_http_date = replacement_expires
            .fmt(DateTimeFormat::HttpDate)
            .expect("Test timestamp should format as an HTTP date");

        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("Failed to create bucket");

        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .cache_control("max-age=60")
            .content_disposition("inline; filename=source.js")
            .content_encoding("br")
            .content_language("en-US")
            .content_type("text/javascript; charset=utf-8")
            .expires(source_expires)
            .website_redirect_location("/source.html")
            .storage_class(StorageClass::ReducedRedundancy)
            .metadata("mtime", "1777992333")
            .metadata("stale", "must-be-removed")
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("PUT failed");

        let copied_key = "assets/default-copy.js";
        client
            .copy_object()
            .bucket(bucket)
            .key(copied_key)
            .copy_source(format!("{bucket}/{key}"))
            .send()
            .await
            .expect("default CopyObject failed");

        let copied_head = client
            .head_object()
            .bucket(bucket)
            .key(copied_key)
            .send()
            .await
            .expect("HEAD failed after default copy");
        assert_eq!(copied_head.cache_control(), Some("max-age=60"));
        assert_eq!(copied_head.content_disposition(), Some("inline; filename=source.js"));
        assert_eq!(copied_head.content_encoding(), Some("br"));
        assert_eq!(copied_head.content_language(), Some("en-US"));
        assert_eq!(copied_head.content_type(), Some("text/javascript; charset=utf-8"));
        assert_eq!(copied_head.expires_string(), Some(source_expires_http_date.as_str()));
        assert_eq!(
            copied_head.storage_class(),
            None,
            "CopyObject without a storage class should write STANDARD"
        );
        assert_eq!(
            copied_head.website_redirect_location(),
            Some("/source.html"),
            "default CopyObject should preserve source metadata"
        );
        assert_eq!(
            copied_head.metadata().and_then(|metadata| metadata.get("stale")),
            Some(&"must-be-removed".to_string())
        );

        client
            .copy_object()
            .bucket(bucket)
            .key("assets/explicit-copy.js")
            .copy_source(format!("{bucket}/{key}"))
            .metadata_directive(MetadataDirective::Copy)
            .send()
            .await
            .expect("explicit COPY directive failed");
        let explicit_copy_head = client
            .head_object()
            .bucket(bucket)
            .key("assets/explicit-copy.js")
            .send()
            .await
            .expect("HEAD failed after explicit COPY");
        assert_eq!(explicit_copy_head.cache_control(), Some("max-age=60"));
        assert_eq!(
            explicit_copy_head.website_redirect_location(),
            None,
            "explicit COPY does not inherit website redirect metadata"
        );

        client
            .copy_object()
            .bucket(bucket)
            .key("assets/explicit-copy-redirect.js")
            .copy_source(format!("{bucket}/{key}"))
            .metadata_directive(MetadataDirective::Copy)
            .website_redirect_location("/explicit-copy.html")
            .send()
            .await
            .expect("explicit COPY with redirect failed");
        let explicit_redirect_head = client
            .head_object()
            .bucket(bucket)
            .key("assets/explicit-copy-redirect.js")
            .send()
            .await
            .expect("HEAD failed after explicit COPY with redirect");
        assert_eq!(explicit_redirect_head.website_redirect_location(), Some("/explicit-copy.html"));

        client
            .copy_object()
            .bucket(bucket)
            .key("assets/explicit-storage-class.js")
            .copy_source(format!("{bucket}/{key}"))
            .storage_class(StorageClass::ReducedRedundancy)
            .send()
            .await
            .expect("CopyObject with an explicit storage class failed");
        let explicit_storage_class_head = client
            .head_object()
            .bucket(bucket)
            .key("assets/explicit-storage-class.js")
            .send()
            .await
            .expect("HEAD failed after explicit storage class copy");
        assert_eq!(
            explicit_storage_class_head.storage_class().map(StorageClass::as_str),
            Some("REDUCED_REDUNDANCY")
        );

        client
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}"))
            .metadata_directive(MetadataDirective::Replace)
            .cache_control("no-cache")
            .content_disposition("attachment; filename=replaced.js")
            .content_encoding("gzip")
            .content_language("fr-FR")
            .content_type("application/javascript")
            .expires(replacement_expires)
            .website_redirect_location("/replaced.html")
            .metadata("mtime", "1777992348")
            .send()
            .await
            .expect("self CopyObject with metadata replacement failed");

        let head_resp = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("HEAD failed after self-copy");
        assert_eq!(head_resp.content_length(), Some(content.len() as i64));
        assert_eq!(
            head_resp.metadata().and_then(|metadata| metadata.get("mtime")),
            Some(&"1777992348".to_string()),
            "HEAD should return replaced metadata"
        );
        assert_eq!(
            head_resp.metadata().and_then(|metadata| metadata.get("stale")),
            None,
            "HEAD should not return metadata omitted by REPLACE"
        );
        assert_eq!(head_resp.cache_control(), Some("no-cache"));
        assert_eq!(head_resp.content_disposition(), Some("attachment; filename=replaced.js"));
        assert_eq!(head_resp.content_encoding(), Some("gzip"));
        assert_eq!(head_resp.content_language(), Some("fr-FR"));
        assert_eq!(head_resp.content_type(), Some("application/javascript"));
        assert_eq!(head_resp.expires_string(), Some(replacement_expires_http_date.as_str()));
        assert_eq!(head_resp.website_redirect_location(), Some("/replaced.html"));
        assert_eq!(head_resp.storage_class(), None, "REPLACE without a storage class should write STANDARD");

        let get_resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("GET failed after self-copy");
        let body = get_resp
            .body
            .collect()
            .await
            .expect("Failed to collect GET body")
            .into_bytes();
        assert_eq!(body.as_ref(), content, "self-copy metadata replacement must not drop object data");

        client
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}"))
            .metadata_directive(MetadataDirective::Replace)
            .send()
            .await
            .expect("self CopyObject with empty metadata replacement failed");

        let empty_head_resp = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("HEAD failed after empty metadata replacement");
        assert_eq!(
            empty_head_resp.metadata().and_then(|metadata| metadata.get("mtime")),
            None,
            "HEAD should not return metadata omitted by empty REPLACE"
        );
        assert_eq!(empty_head_resp.cache_control(), None);
        assert_eq!(empty_head_resp.content_disposition(), None);
        assert_eq!(empty_head_resp.content_encoding(), None);
        assert_eq!(empty_head_resp.content_language(), None);
        assert_eq!(empty_head_resp.content_type(), None);
        assert_eq!(empty_head_resp.expires_string(), None);
        assert_eq!(empty_head_resp.website_redirect_location(), None);

        let empty_get_resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("GET failed after empty metadata replacement");
        let empty_body = empty_get_resp
            .body
            .collect()
            .await
            .expect("Failed to collect GET body after empty metadata replacement")
            .into_bytes();
        assert_eq!(empty_body.as_ref(), content, "empty metadata replacement must not drop object data");

        env.stop_server();
    }

    #[tokio::test]
    #[serial]
    async fn copy_object_replace_accepts_each_standard_field_independently() {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let bucket = "copy-object-metadata-fields";
        let source = "source.txt";
        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("Failed to create bucket");
        client
            .put_object()
            .bucket(bucket)
            .key(source)
            .cache_control("source-cache")
            .content_disposition("inline")
            .content_encoding("br")
            .content_language("en")
            .content_type("text/source")
            .expires(DateTime::from_secs(1_893_456_000))
            .body(ByteStream::from_static(b"field-by-field"))
            .send()
            .await
            .expect("PUT failed");
        let replacement_expires = DateTime::from_secs(1_924_992_000);
        let replacement_expires_http_date = replacement_expires
            .fmt(DateTimeFormat::HttpDate)
            .expect("Test timestamp should format as an HTTP date");

        for field in [
            "cache-control",
            "content-disposition",
            "content-encoding",
            "content-language",
            "content-type",
            "expires",
            "website-redirect",
        ] {
            let destination = format!("{field}.txt");
            let request = client
                .copy_object()
                .bucket(bucket)
                .key(&destination)
                .copy_source(format!("{bucket}/{source}"))
                .metadata_directive(MetadataDirective::Replace);
            let request = match field {
                "cache-control" => request.cache_control("field-cache"),
                "content-disposition" => request.content_disposition("attachment"),
                "content-encoding" => request.content_encoding("gzip"),
                "content-language" => request.content_language("de"),
                "content-type" => request.content_type("text/field"),
                "expires" => request.expires(replacement_expires),
                "website-redirect" => request.website_redirect_location("/field.html"),
                _ => unreachable!("field table contains only supported entries"),
            };
            request.send().await.expect("field-specific CopyObject failed");

            let head = client
                .head_object()
                .bucket(bucket)
                .key(&destination)
                .send()
                .await
                .expect("HEAD failed");
            assert_eq!(head.cache_control(), (field == "cache-control").then_some("field-cache"));
            assert_eq!(head.content_disposition(), (field == "content-disposition").then_some("attachment"));
            assert_eq!(head.content_encoding(), (field == "content-encoding").then_some("gzip"));
            assert_eq!(head.content_language(), (field == "content-language").then_some("de"));
            assert_eq!(head.content_type(), (field == "content-type").then_some("text/field"));
            assert_eq!(
                head.expires_string(),
                (field == "expires").then_some(replacement_expires_http_date.as_str())
            );
            assert_eq!(head.website_redirect_location(), (field == "website-redirect").then_some("/field.html"));
        }

        client
            .copy_object()
            .bucket(bucket)
            .key("user-metadata-collision.txt")
            .copy_source(format!("{bucket}/{source}"))
            .metadata_directive(MetadataDirective::Replace)
            .metadata("content-type", "user-content-type")
            .metadata("content-encoding", "user-content-encoding")
            .send()
            .await
            .expect("CopyObject should preserve user metadata namespaces");
        let collision_head = client
            .head_object()
            .bucket(bucket)
            .key("user-metadata-collision.txt")
            .send()
            .await
            .expect("HEAD failed for metadata collision case");
        assert_eq!(collision_head.content_type(), None);
        assert_eq!(collision_head.content_encoding(), None);
        assert_eq!(
            collision_head.metadata().and_then(|metadata| metadata.get("content-type")),
            Some(&"user-content-type".to_string())
        );
        assert_eq!(
            collision_head
                .metadata()
                .and_then(|metadata| metadata.get("content-encoding")),
            Some(&"user-content-encoding".to_string())
        );

        env.stop_server();
    }

    #[tokio::test]
    #[serial]
    async fn copy_object_replace_handles_versioned_multipart_source() {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let bucket = "copy-object-metadata-multipart";
        let source = "source.bin";
        let multipart_body = b"multipart historical source";
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

        let upload = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(source)
            .content_type("application/source")
            .send()
            .await
            .expect("Failed to create multipart upload");
        let upload_id = upload.upload_id().expect("Multipart upload should return an ID");
        let part = client
            .upload_part()
            .bucket(bucket)
            .key(source)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(multipart_body))
            .send()
            .await
            .expect("Failed to upload multipart part");
        let completed = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(source)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .e_tag(part.e_tag().expect("Uploaded part should return an ETag"))
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .expect("Failed to complete multipart upload");
        let historical_version = completed
            .version_id()
            .expect("Versioned multipart upload should return a version ID")
            .to_string();

        client
            .put_object()
            .bucket(bucket)
            .key(source)
            .body(ByteStream::from_static(b"new current version"))
            .send()
            .await
            .expect("Failed to write current version");

        let copy = client
            .copy_object()
            .bucket(bucket)
            .key("restored.bin")
            .copy_source(format!("{bucket}/{source}?versionId={historical_version}"))
            .metadata_directive(MetadataDirective::Replace)
            .content_type("application/replaced")
            .send()
            .await
            .expect("Failed to copy historical multipart version");
        assert_eq!(copy.copy_source_version_id(), Some(historical_version.as_str()));

        let restored = client
            .get_object()
            .bucket(bucket)
            .key("restored.bin")
            .send()
            .await
            .expect("Failed to read copied multipart source");
        assert_eq!(restored.content_type(), Some("application/replaced"));
        assert_eq!(
            restored
                .body
                .collect()
                .await
                .expect("Failed to collect restored body")
                .into_bytes()
                .as_ref(),
            multipart_body
        );

        env.stop_server();
    }

    #[tokio::test]
    #[serial]
    async fn invalid_replacement_metadata_does_not_mutate_destination() {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING", "true")])
            .await
            .expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let bucket = "copy-object-invalid-metadata";
        let key = "destination.zip";
        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("Failed to create bucket");
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_type("application/zip")
            .metadata("state", "original")
            .body(ByteStream::from_static(b"original destination"))
            .send()
            .await
            .expect("Failed to write destination");

        let error = client
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}"))
            .metadata_directive(MetadataDirective::Replace)
            .content_type("application/zip")
            .content_encoding("gzip")
            .send()
            .await
            .expect_err("Invalid replacement metadata should be rejected");
        assert_eq!(error.as_service_error().and_then(|err| err.code()), Some("InvalidArgument"));

        let invalid_directive = client
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}"))
            .customize()
            .mutate_request(|request| {
                request.headers_mut().insert("x-amz-metadata-directive", "UNKNOWN");
            })
            .send()
            .await
            .expect_err("Unknown metadata directives should be rejected");
        assert_eq!(
            invalid_directive.as_service_error().and_then(|error| error.code()),
            Some("InvalidArgument")
        );

        let ignored_replacement = client
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}"))
            .content_type("application/ignored")
            .send()
            .await
            .expect_err("Replacement fields without REPLACE should be rejected");
        assert_eq!(
            ignored_replacement.as_service_error().and_then(|error| error.code()),
            Some("InvalidRequest")
        );

        let unchanged = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("Destination should remain readable");
        assert_eq!(unchanged.content_type(), Some("application/zip"));
        assert_eq!(
            unchanged.metadata().and_then(|metadata| metadata.get("state")),
            Some(&"original".to_string())
        );
        assert_eq!(
            unchanged
                .body
                .collect()
                .await
                .expect("Failed to collect destination body")
                .into_bytes()
                .as_ref(),
            b"original destination"
        );

        env.stop_server();
    }
}
