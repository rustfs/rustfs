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
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use serial_test::serial;
    use std::error::Error;
    use tracing::info;

    async fn create_bucket(client: &Client, bucket: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        client.create_bucket().bucket(bucket).send().await?;
        Ok(())
    }

    /// PUT with a leading-slash key must succeed and the object must be
    /// readable under the normalized key (leading slash stripped).
    #[tokio::test]
    #[serial]
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

    /// Duplicate and repeated slashes after a leading slash collapse MinIO-style.
    #[tokio::test]
    #[serial]
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
        let result = client.get_object().bucket(bucket).key("keyname").send().await;
        assert!(result.is_err(), "object must be gone after DELETE with raw key");

        env.stop_server();
        info!("Test completed successfully");
        Ok(())
    }
}
