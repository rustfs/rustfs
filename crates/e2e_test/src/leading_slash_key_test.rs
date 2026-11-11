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

//! Regression tests for Issue #2427: object keys with a leading slash.
//!
//! AWS S3 accepts keys such as `/foo/bar` (request path `PUT /bucket//foo/bar`),
//! and some SDK wrappers produce such keys. RustFS normalizes these MinIO-style:
//! leading slashes are stripped and duplicate slashes after a leading slash are
//! collapsed, so `//foo/bar` is stored and served as `foo/bar`.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, admin_create_user, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration};
    use std::error::Error;
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    async fn create_bucket(client: &Client, bucket: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        client.create_bucket().bucket(bucket).send().await?;
        Ok(())
    }

    /// PUT with a leading-slash key must succeed and the object must be
    /// readable under the normalized key (leading slash stripped).
    #[tokio::test]
    async fn test_put_object_with_leading_slash_key() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        info!("Starting test: PUT object with leading slash in key (Issue #2427)");

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "test-leading-slash";
        create_bucket(&client, bucket).await?;

        // Key "/foo/bar" produces the request path "PUT /bucket//foo/bar".
        let content = b"leading slash content";
        client
            .put_object()
            .bucket(bucket)
            .key("/foo/bar")
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("PUT with leading-slash key must succeed");
        info!("PUT with leading-slash key succeeded");

        // The object is stored under the normalized key "foo/bar".
        let output = client
            .get_object()
            .bucket(bucket)
            .key("foo/bar")
            .send()
            .await
            .expect("GET with normalized key must succeed");
        let body = output.body.collect().await?.into_bytes();
        assert_eq!(body.as_ref(), content, "content mismatch for normalized key");

        // GET with the original leading-slash key is normalized to the same object.
        let output = client
            .get_object()
            .bucket(bucket)
            .key("/foo/bar")
            .send()
            .await
            .expect("GET with leading-slash key must succeed");
        let body = output.body.collect().await?.into_bytes();
        assert_eq!(body.as_ref(), content, "content mismatch for leading-slash key");

        // LIST must report the normalized key only.
        let output = client.list_objects_v2().bucket(bucket).send().await?;
        let keys: Vec<&str> = output.contents().iter().filter_map(|o| o.key()).collect();
        assert_eq!(keys, vec!["foo/bar"], "LIST must contain only the normalized key");

        env.stop_server();
        info!("Test completed successfully");
        Ok(())
    }

    /// Duplicate and repeated slashes after a leading slash collapse MinIO-style across single and bulk operations.
    #[tokio::test]
    async fn test_put_object_with_duplicate_slashes_normalized() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        info!("Starting test: duplicate slash normalization (Issue #2427)");

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "test-duplicate-slash";
        create_bucket(&client, bucket).await?;

        // (request key, normalized stored key)
        let cases = [("//keyname", "keyname"), ("/dir///sub//file", "dir/sub/file")];

        for (raw_key, normalized_key) in cases {
            let content = format!("content for {raw_key}");
            client
                .put_object()
                .bucket(bucket)
                .key(raw_key)
                .body(ByteStream::from(content.clone().into_bytes()))
                .send()
                .await
                .unwrap_or_else(|e| panic!("PUT with key '{raw_key}' must succeed: {e:?}"));

            let output = client
                .get_object()
                .bucket(bucket)
                .key(normalized_key)
                .send()
                .await
                .unwrap_or_else(|e| panic!("GET with normalized key '{normalized_key}' must succeed: {e:?}"));
            let body = output.body.collect().await?.into_bytes();
            assert_eq!(body.as_ref(), content.as_bytes(), "content mismatch for key '{raw_key}'");
            info!("PUT '{}' stored and readable as '{}'", raw_key, normalized_key);
        }

        // DELETE through the raw key removes the normalized object.
        client.delete_object().bucket(bucket).key("//keyname").send().await?;
        let error = client
            .get_object()
            .bucket(bucket)
            .key("keyname")
            .send()
            .await
            .expect_err("object must be gone after DELETE with raw key");
        assert_eq!(
            error.raw_response().map(|response| response.status().as_u16()),
            Some(404),
            "GET after DELETE with raw key must return HTTP 404, got {error:?}"
        );

        assert_bulk_delete_normalizes_keys(&client).await?;
        assert_bulk_delete_authorizes_normalized_keys(&env, &client).await?;
        assert_bulk_delete_preserves_version_identity(&client).await?;

        env.stop_server();
        info!("Test completed successfully");
        Ok(())
    }

    async fn assert_bulk_delete_normalizes_keys(client: &Client) -> TestResult {
        let bucket = "test-bulk-leading-slash";
        create_bucket(client, bucket).await?;
        let cases = [
            ("/foo/bar", "foo/bar"),
            ("//keyname", "keyname"),
            ("/dir///sub//file", "dir/sub/file"),
            ("/folder///", "folder/"),
            ("/literal%2F+名", "literal%2F+名"),
            ("plain", "plain"),
        ];
        let invalid = ["///", "/dir/../keep", "dir//keep"];
        client
            .put_object()
            .bucket(bucket)
            .key("keep")
            .body(ByteStream::from_static(b"keep"))
            .send()
            .await?;
        client
            .put_object()
            .bucket(bucket)
            .key("dir/keep")
            .body(ByteStream::from_static(b"keep"))
            .send()
            .await?;

        for quiet in [false, true] {
            for (raw, _) in cases {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(raw)
                    .body(ByteStream::from_static(b""))
                    .send()
                    .await?;
            }
            let listing = client.list_objects_v2().bucket(bucket).send().await?;
            let stored: Vec<_> = listing.contents().iter().filter_map(|object| object.key()).collect();
            let mut expected: Vec<_> = cases
                .iter()
                .map(|(_, normalized)| *normalized)
                .chain(["dir/keep", "keep"])
                .collect();
            expected.sort_unstable();
            assert_eq!(stored, expected, "PUT must store the exact normalized targets before bulk deletion");
            let objects = cases
                .iter()
                .map(|(raw, _)| *raw)
                .chain(invalid)
                .map(|key| ObjectIdentifier::builder().key(key).build())
                .collect::<Result<Vec<_>, _>>()?;
            let delete = Delete::builder().set_objects(Some(objects)).quiet(quiet).build()?;
            // A second identical batch also pins idempotent deletion of absent normalized keys.
            for _ in 0..2 {
                let output = client.delete_objects().bucket(bucket).delete(delete.clone()).send().await?;
                let errors: Vec<_> = output.errors().iter().map(|error| (error.key(), error.code())).collect();
                assert_eq!(
                    errors,
                    invalid.map(|key| (Some(key), Some("InvalidArgument"))),
                    "unexpected per-key errors: {output:?}"
                );
                let deleted: Vec<_> = output.deleted().iter().filter_map(|object| object.key()).collect();
                let expected = if quiet {
                    vec![]
                } else {
                    cases.iter().map(|(raw, _)| *raw).collect()
                };
                assert_eq!(deleted, expected, "responses must retain request keys, including directory slashes");
            }
            let listing = client.list_objects_v2().bucket(bucket).send().await?;
            let remaining: Vec<_> = listing.contents().iter().filter_map(|object| object.key()).collect();
            assert_eq!(
                remaining,
                ["dir/keep", "keep"],
                "bulk delete must remove normalized objects without resolving dot segments"
            );
        }
        Ok(())
    }

    async fn assert_bulk_delete_authorizes_normalized_keys(env: &RustFSTestEnvironment, admin: &Client) -> TestResult {
        let bucket = "test-bulk-slash-policy";
        let user = "bulk-slash-user";
        admin_create_user(env, user, "bulkSlashTestSecret123!").await?;
        create_bucket(admin, bucket).await?;
        for key in ["allowed/item", "protected/item"] {
            admin
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"keep"))
                .send()
                .await?;
        }
        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"AWS": [user]}, "Action": ["s3:DeleteObject"],
                 "Resource": [format!("arn:aws:s3:::{bucket}/*")]},
                {"Effect": "Deny", "Principal": {"AWS": [user]}, "Action": ["s3:DeleteObject"],
                 "Resource": [format!("arn:aws:s3:::{bucket}/protected/*")]}
            ]
        });
        admin
            .put_bucket_policy()
            .bucket(bucket)
            .policy(policy.to_string())
            .send()
            .await?;
        let client = env.create_s3_client_with_credentials(user, "bulkSlashTestSecret123!");
        let output = client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .objects(ObjectIdentifier::builder().key("/allowed///item").build()?)
                    .objects(ObjectIdentifier::builder().key("/protected///item").build()?)
                    .build()?,
            )
            .send()
            .await?;
        assert_eq!(output.deleted().len(), 1, "allowed normalized key must be deleted: {output:?}");
        assert_eq!(output.deleted()[0].key(), Some("/allowed///item"));
        assert_eq!(output.errors().len(), 1, "normalized resource denial must apply: {output:?}");
        assert_eq!(output.errors()[0].key(), Some("/protected///item"));
        assert_eq!(output.errors()[0].code(), Some("AccessDenied"));
        let listing = admin.list_objects_v2().bucket(bucket).send().await?;
        let remaining: Vec<_> = listing.contents().iter().filter_map(|object| object.key()).collect();
        assert_eq!(remaining, ["protected/item"], "only the authorized object may be removed");
        let protected = admin.get_object().bucket(bucket).key("protected/item").send().await?;
        assert_eq!(protected.body.collect().await?.into_bytes().as_ref(), b"keep");
        Ok(())
    }

    async fn assert_bulk_delete_preserves_version_identity(client: &Client) -> TestResult {
        let bucket = "test-bulk-slash-versions";
        let raw = "/history///file";
        let normalized = "history/file";
        create_bucket(client, bucket).await?;
        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await?;
        let first = client
            .put_object()
            .bucket(bucket)
            .key(raw)
            .body(ByteStream::from_static(b"old"))
            .send()
            .await?;
        let first_id = first.version_id().ok_or("first PUT omitted version ID")?;
        let latest = client
            .put_object()
            .bucket(bucket)
            .key(raw)
            .body(ByteStream::from_static(b"latest"))
            .send()
            .await?;
        let latest_id = latest.version_id().ok_or("latest PUT omitted version ID")?;
        let invalid = client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .objects(ObjectIdentifier::builder().key("///").build()?)
                    .build()?,
            )
            .send()
            .await?;
        assert!(invalid.deleted().is_empty(), "slash-only keys must not delete a versioned object");
        assert_eq!(
            invalid.errors().len(),
            1,
            "slash-only keys must remain invalid in versioned buckets: {invalid:?}"
        );
        assert_eq!(invalid.errors()[0].key(), Some("///"));
        assert_eq!(invalid.errors()[0].code(), Some("InvalidArgument"));
        let deleted = client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .objects(ObjectIdentifier::builder().key(raw).version_id(first_id).build()?)
                    .build()?,
            )
            .send()
            .await?;
        assert!(deleted.errors().is_empty(), "historical delete failed: {deleted:?}");
        assert_eq!(deleted.deleted().len(), 1);
        assert_eq!(deleted.deleted()[0].key(), Some(raw));
        assert_eq!(deleted.deleted()[0].version_id(), Some(first_id));
        let listing = client.list_object_versions().bucket(bucket).send().await?;
        assert_eq!(listing.versions().len(), 1, "only the latest data version should remain: {listing:?}");
        assert_eq!(listing.versions()[0].version_id(), Some(latest_id));

        let deleted = client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .objects(ObjectIdentifier::builder().key(raw).build()?)
                    .build()?,
            )
            .send()
            .await?;
        assert!(deleted.errors().is_empty(), "delete marker creation failed: {deleted:?}");
        assert_eq!(deleted.deleted().len(), 1);
        assert_eq!(deleted.deleted()[0].key(), Some(raw));
        assert_eq!(deleted.deleted()[0].delete_marker(), Some(true));
        let marker = deleted.deleted()[0]
            .delete_marker_version_id()
            .ok_or("missing delete marker ID")?;
        let listing = client.list_object_versions().bucket(bucket).send().await?;
        assert_eq!(listing.delete_markers().len(), 1);
        assert_eq!(listing.delete_markers()[0].key(), Some(normalized));
        assert_eq!(listing.delete_markers()[0].version_id(), Some(marker));

        let deleted = client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .objects(ObjectIdentifier::builder().key(raw).version_id(marker).build()?)
                    .build()?,
            )
            .send()
            .await?;
        assert!(deleted.errors().is_empty(), "explicit marker removal failed: {deleted:?}");
        assert_eq!(deleted.deleted().len(), 1);
        assert_eq!(deleted.deleted()[0].key(), Some(raw));
        assert_eq!(deleted.deleted()[0].version_id(), Some(marker));
        let listing = client.list_object_versions().bucket(bucket).send().await?;
        assert!(listing.delete_markers().is_empty());
        let restored = client.get_object().bucket(bucket).key(normalized).send().await?;
        assert_eq!(restored.version_id(), Some(latest_id));
        assert_eq!(restored.body.collect().await?.into_bytes().as_ref(), b"latest");
        Ok(())
    }
}
