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

//! Regression test for Issue #4238: CopyObject must allow restoring a historical
//! object version onto the current key (same bucket/key + sourceVersionId) using the
//! default COPY metadata directive, preserving data, Content-Type and user metadata.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    use serial_test::serial;
    use tracing::info;

    #[tokio::test]
    #[serial]
    async fn test_self_copy_of_historical_version_restores_data_and_metadata() {
        init_logging();
        info!("Issue #4238: self-copy of a historical version must be allowed and preserve metadata");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let bucket = "copy-object-version-restore-test";
        let key = "docs/report.txt";

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

        // Version 1: the content we want to restore later.
        let v1_content = b"original report contents -- version one";
        let put_v1 = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_type("text/plain; charset=utf-8")
            .metadata("origin", "v1")
            .body(ByteStream::from_static(v1_content))
            .send()
            .await
            .expect("PUT v1 failed");
        let v1_id = put_v1.version_id().expect("v1 must have a version id").to_string();

        // Version 2: becomes the current version.
        let v2_content = b"updated report contents -- version two (current)";
        let put_v2 = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_type("application/octet-stream")
            .metadata("origin", "v2")
            .body(ByteStream::from_static(v2_content))
            .send()
            .await
            .expect("PUT v2 failed");
        let v2_id = put_v2.version_id().expect("v2 must have a version id").to_string();

        assert_ne!(v1_id, v2_id, "the two puts must produce distinct versions");

        // Restore v1 by copying it onto the current key with the DEFAULT (COPY) directive.
        // This is the exact operation issue #4238 says is wrongly rejected as a self-copy.
        let copy_out = client
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}?versionId={v1_id}"))
            .send()
            .await
            .expect("CopyObject restoring a historical version must succeed (issue #4238)");

        let restored_id = copy_out
            .copy_object_result()
            .and_then(|_| copy_out.version_id())
            .expect("restore copy must create a new version id")
            .to_string();
        assert_ne!(restored_id, v1_id, "restore must create a NEW version, not mutate v1");
        assert_ne!(restored_id, v2_id, "restore must create a NEW version, not mutate v2");

        // The current object must now serve v1's data and metadata (COPY directive preserves both).
        let head = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("HEAD failed after restore");
        assert_eq!(head.content_length(), Some(v1_content.len() as i64), "current size must equal v1");
        assert_eq!(
            head.content_type(),
            Some("text/plain; charset=utf-8"),
            "Content-Type must be preserved from v1 (issue #4238 reports it is lost)"
        );
        assert_eq!(
            head.metadata().and_then(|m| m.get("origin")),
            Some(&"v1".to_string()),
            "user metadata must be preserved from v1"
        );

        let get = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("GET failed after restore");
        let body = get.body.collect().await.expect("collect body").into_bytes();
        assert_eq!(body.as_ref(), v1_content, "current object data must equal v1 after restore");

        // The original versions must remain intact and independently readable.
        let get_v1 = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(&v1_id)
            .send()
            .await
            .expect("GET v1 failed");
        let v1_body = get_v1.body.collect().await.expect("collect v1 body").into_bytes();
        assert_eq!(v1_body.as_ref(), v1_content, "v1 must remain intact after restore");

        let get_v2 = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(&v2_id)
            .send()
            .await
            .expect("GET v2 failed");
        let v2_body = get_v2.body.collect().await.expect("collect v2 body").into_bytes();
        assert_eq!(v2_body.as_ref(), v2_content, "v2 must remain intact after restore");

        env.stop_server();
    }

    /// Regression test for Issue #4976: a versioned-source CopyObject must echo the exact source
    /// version copied via `x-amz-copy-source-version-id` (SDK `CopySourceVersionId`), kept distinct
    /// from the newly created destination `x-amz-version-id`.
    #[tokio::test]
    #[serial]
    async fn test_copy_of_non_latest_source_version_returns_copy_source_version_id() {
        init_logging();
        info!("Issue #4976: versioned CopyObject must return x-amz-copy-source-version-id for the exact source version");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let src_bucket = "copy-source-version-header-src";
        let dst_bucket = "copy-source-version-header-dst";
        let src_key = "reports/quarantined.bin";
        let dst_key = "reports/promoted.bin";

        for bucket in [src_bucket, dst_bucket] {
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
        }

        // Source version 1: the exact (non-latest) version we will copy.
        let v1_content = b"quarantined payload -- source version one (target of the copy)";
        let put_v1 = client
            .put_object()
            .bucket(src_bucket)
            .key(src_key)
            .body(ByteStream::from_static(v1_content))
            .send()
            .await
            .expect("PUT source v1 failed");
        let v1_id = put_v1.version_id().expect("source v1 must have a version id").to_string();

        // Source version 2: becomes the latest, so v1 is deliberately NOT the current version.
        let v2_content = b"quarantined payload -- source version two (now current, must be ignored)";
        let put_v2 = client
            .put_object()
            .bucket(src_bucket)
            .key(src_key)
            .body(ByteStream::from_static(v2_content))
            .send()
            .await
            .expect("PUT source v2 failed");
        let v2_id = put_v2.version_id().expect("source v2 must have a version id").to_string();
        assert_ne!(v1_id, v2_id, "the two source puts must produce distinct versions");

        // Copy the exact NON-LATEST source version into the destination bucket.
        let copy_out = client
            .copy_object()
            .bucket(dst_bucket)
            .key(dst_key)
            .copy_source(format!("{src_bucket}/{src_key}?versionId={v1_id}"))
            .send()
            .await
            .expect("CopyObject of a versioned source must succeed");

        // (3) The response must echo the exact source version copied.
        let copy_source_version_id = copy_out
            .copy_source_version_id()
            .expect("issue #4976: response must include x-amz-copy-source-version-id for a versioned source");
        assert_eq!(
            copy_source_version_id, v1_id,
            "x-amz-copy-source-version-id must equal the exact source version requested, not the latest source version"
        );
        assert_ne!(
            copy_source_version_id, v2_id,
            "x-amz-copy-source-version-id must not be the latest source version"
        );

        // (4) The destination header must identify a distinct, newly created version.
        let dst_version_id = copy_out
            .version_id()
            .expect("destination copy must create a new version id")
            .to_string();
        assert!(!dst_version_id.is_empty(), "destination version id must be present");
        assert_ne!(dst_version_id, v1_id, "destination version must be distinct from the source version");
        assert_ne!(
            dst_version_id, v2_id,
            "destination version must be distinct from the source latest version"
        );

        // (5) The destination must hold the exact bytes/size of the copied (v1) source version.
        let head = client
            .head_object()
            .bucket(dst_bucket)
            .key(dst_key)
            .send()
            .await
            .expect("HEAD destination failed");
        assert_eq!(
            head.content_length(),
            Some(v1_content.len() as i64),
            "destination size must equal the copied source version (v1)"
        );

        let get_dst = client
            .get_object()
            .bucket(dst_bucket)
            .key(dst_key)
            .version_id(&dst_version_id)
            .send()
            .await
            .expect("GET destination failed");
        let dst_body = get_dst.body.collect().await.expect("collect destination body").into_bytes();
        assert_eq!(
            dst_body.as_ref(),
            v1_content,
            "destination bytes must exactly equal the copied source version (v1), not the latest (v2)"
        );

        // (6) The source version copied from must remain present and independently readable.
        let get_src_v1 = client
            .get_object()
            .bucket(src_bucket)
            .key(src_key)
            .version_id(&v1_id)
            .send()
            .await
            .expect("GET source v1 failed after copy");
        let src_v1_body = get_src_v1.body.collect().await.expect("collect source v1 body").into_bytes();
        assert_eq!(src_v1_body.as_ref(), v1_content, "source v1 must remain intact after the copy");

        env.stop_server();
    }
}
