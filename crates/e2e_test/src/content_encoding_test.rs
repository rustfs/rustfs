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

//! End-to-end test for Content-Encoding header handling
//!
//! Tests that the Content-Encoding header is correctly stored during PUT
//! and returned in GET/HEAD responses. This is important for clients that
//! upload pre-compressed content and rely on the header for decompression.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use serial_test::serial;
    use tracing::info;

    /// Verify Content-Encoding header roundtrips through PUT, GET, and HEAD operations
    #[tokio::test]
    #[serial]
    async fn test_content_encoding_roundtrip() {
        init_logging();
        info!("Starting Content-Encoding roundtrip test");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let bucket = "content-encoding-test";
        let key = "logs/app.log.zst";
        let content = b"2024-01-15 10:23:45 INFO Application started\n2024-01-15 10:23:46 DEBUG Loading config\n";

        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("Failed to create bucket");

        info!("Uploading object with Content-Encoding: zstd");
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_type("text/plain")
            .content_encoding("zstd")
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("PUT failed");

        info!("Verifying GET response includes Content-Encoding");
        let get_resp = client.get_object().bucket(bucket).key(key).send().await.expect("GET failed");

        assert_eq!(get_resp.content_encoding(), Some("zstd"), "GET should return Content-Encoding: zstd");
        assert_eq!(get_resp.content_type(), Some("text/plain"), "GET should return correct Content-Type");

        let body = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), content, "Body content mismatch");

        info!("Verifying HEAD response includes Content-Encoding");
        let head_resp = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("HEAD failed");

        assert_eq!(head_resp.content_encoding(), Some("zstd"), "HEAD should return Content-Encoding: zstd");
        assert_eq!(head_resp.content_type(), Some("text/plain"), "HEAD should return correct Content-Type");

        env.stop_server();
    }
}
