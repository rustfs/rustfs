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
}
