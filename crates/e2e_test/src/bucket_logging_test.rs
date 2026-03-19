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

//! End-to-end tests for S3 dummy-compat bucket APIs.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use serial_test::serial;
    use tracing::info;

    #[tokio::test]
    #[serial]
    async fn test_dummy_bucket_compatibility_endpoints() {
        init_logging();
        info!("Starting test: dummy-compat bucket APIs should match S3-compatible behavior");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let bucket = "test-get-bucket-logging";

        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("Failed to create bucket");

        let result = client.get_bucket_logging().bucket(bucket).send().await;
        assert!(
            result.is_ok(),
            "GetBucketLogging should return success for existing bucket, got: {:?}",
            result.err()
        );

        let output = result.unwrap();
        assert!(
            output.logging_enabled().is_none(),
            "Default GetBucketLogging should return empty logging configuration"
        );

        let accelerate = client
            .get_bucket_accelerate_configuration()
            .bucket(bucket)
            .send()
            .await
            .expect("GetBucketAccelerateConfiguration should succeed");
        assert!(
            accelerate.status().is_none(),
            "Default GetBucketAccelerateConfiguration should return empty status"
        );

        let payment = client
            .get_bucket_request_payment()
            .bucket(bucket)
            .send()
            .await
            .expect("GetBucketRequestPayment should succeed");
        assert_eq!(
            payment.payer().map(|p| p.as_str()),
            Some("BucketOwner"),
            "GetBucketRequestPayment should return BucketOwner by default"
        );

        let website = client.get_bucket_website().bucket(bucket).send().await;
        assert!(website.is_err(), "GetBucketWebsite should return NoSuchWebsiteConfiguration when unset");
        let website_err = website.err().unwrap();
        let website_code = website_err.as_service_error().and_then(|e| e.code());
        assert!(
            matches!(website_code, Some("NoSuchWebsiteConfiguration")),
            "Unexpected GetBucketWebsite error code: {:?}, err: {:?}",
            website_code,
            website_err
        );

        client
            .delete_bucket_website()
            .bucket(bucket)
            .send()
            .await
            .expect("DeleteBucketWebsite should return success");

        env.stop_server();
    }
}
