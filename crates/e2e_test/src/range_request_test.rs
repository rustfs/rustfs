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

//! End-to-end regression test for invalid GET object ranges.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::primitives::ByteStream;
    use serial_test::serial;
    use tracing::info;

    fn create_s3_client(env: &RustFSTestEnvironment) -> Client {
        env.create_s3_client()
    }

    async fn create_bucket(client: &Client, bucket: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match client.create_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(()),
            Err(err) => {
                if err.to_string().contains("BucketAlreadyOwnedByYou") || err.to_string().contains("BucketAlreadyExists") {
                    Ok(())
                } else {
                    Err(Box::new(err))
                }
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_get_object_invalid_range_returns_416_issue_s3_implemented_tests() {
        init_logging();
        info!("TEST: GetObject invalid range should return InvalidRange/416");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = create_s3_client(&env);
        let bucket = "test-invalid-range";
        let key = "range.txt";
        let content = b"testcontent";

        create_bucket(&client, bucket).await.expect("Failed to create bucket");
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("PutObject should succeed");

        let result = client.get_object().bucket(bucket).key(key).range("bytes=40-50").send().await;

        let err = result.expect_err("GetObject with an unsatisfiable range should fail");
        match err {
            SdkError::ServiceError(service_err) => {
                assert_eq!(service_err.raw().status().as_u16(), 416, "invalid range should return HTTP 416");

                let s3_err = service_err.into_err();
                assert_eq!(s3_err.meta().code(), Some("InvalidRange"), "invalid range should map to InvalidRange");
            }
            other_err => panic!("Expected S3 service error, got: {other_err:?}"),
        }
    }
}
