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
    use std::path::PathBuf;
    use std::process::Command;
    use tracing::info;

    fn awscurl_binary_path() -> PathBuf {
        std::env::var_os("AWSCURL_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("awscurl"))
    }

    fn awscurl_available() -> bool {
        Command::new(awscurl_binary_path()).arg("--version").output().is_ok()
    }

    fn execute_s3_awscurl(
        method: &str,
        url: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let output = Command::new(awscurl_binary_path())
            .args([
                "--service",
                "s3",
                "--region",
                "us-east-1",
                "--access_key",
                access_key,
                "--secret_key",
                secret_key,
                "-i",
                "-X",
                method,
                url,
            ])
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("awscurl failed: stderr='{stderr}', stdout='{stdout}'").into());
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    fn parse_status(raw: &str) -> Option<u16> {
        raw.lines()
            .filter_map(|line| {
                if line.starts_with("HTTP/") {
                    line.split_whitespace().nth(1)?.parse::<u16>().ok()
                } else {
                    None
                }
            })
            .next_back()
    }

    fn parse_body(raw: &str) -> String {
        if let Some(pos) = raw.rfind("\r\n\r\n") {
            return raw[pos + 4..].to_string();
        }
        if let Some(pos) = raw.rfind("\n\n") {
            return raw[pos + 2..].to_string();
        }
        String::new()
    }

    fn parse_headers(raw: &str) -> String {
        let start = raw.rfind("HTTP/").unwrap_or(0);
        let tail = &raw[start..];
        if let Some(pos) = tail.find("\r\n\r\n") {
            return tail[..pos].to_string();
        }
        if let Some(pos) = tail.find("\n\n") {
            return tail[..pos].to_string();
        }
        tail.to_string()
    }

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

    #[tokio::test]
    #[serial]
    async fn test_dummy_bucket_endpoints_http_contracts() {
        init_logging();
        info!("Starting test: dummy-compat bucket API HTTP contracts");
        if !awscurl_available() {
            info!("Skipping test_dummy_bucket_endpoints_http_contracts: awscurl binary not found");
            return;
        }

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let bucket = "test-dummy-bucket-http-contracts";

        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("Failed to create bucket");

        let logging_raw = execute_s3_awscurl("GET", &format!("{}/{bucket}?logging=", env.url), &env.access_key, &env.secret_key)
            .expect("GetBucketLogging HTTP request failed");
        assert_eq!(parse_status(&logging_raw), Some(200), "GetBucketLogging should return 200");
        let logging_body = parse_body(&logging_raw);
        assert!(
            logging_body.contains("<BucketLoggingStatus"),
            "GetBucketLogging response should contain BucketLoggingStatus XML, got: {logging_body}"
        );

        let accel_raw = execute_s3_awscurl("GET", &format!("{}/{bucket}?accelerate=", env.url), &env.access_key, &env.secret_key)
            .expect("GetBucketAccelerateConfiguration HTTP request failed");
        assert_eq!(parse_status(&accel_raw), Some(200), "GetBucketAccelerateConfiguration should return 200");
        let accel_body = parse_body(&accel_raw);
        assert!(
            accel_body.contains("<AccelerateConfiguration"),
            "GetBucketAccelerateConfiguration response should contain AccelerateConfiguration XML, got: {accel_body}"
        );

        let payment_raw =
            execute_s3_awscurl("GET", &format!("{}/{bucket}?requestPayment=", env.url), &env.access_key, &env.secret_key)
                .expect("GetBucketRequestPayment HTTP request failed");
        assert_eq!(parse_status(&payment_raw), Some(200), "GetBucketRequestPayment should return 200");
        let payment_body = parse_body(&payment_raw);
        assert!(
            payment_body.contains("<Payer>BucketOwner</Payer>"),
            "GetBucketRequestPayment should return BucketOwner payer, got: {payment_body}"
        );

        let website_raw = execute_s3_awscurl("GET", &format!("{}/{bucket}?website=", env.url), &env.access_key, &env.secret_key)
            .expect("GetBucketWebsite HTTP request failed");
        assert_eq!(
            parse_status(&website_raw),
            Some(404),
            "GetBucketWebsite should return 404 when website config is absent"
        );
        let website_content_type = parse_headers(&website_raw).to_ascii_lowercase();
        assert!(
            website_content_type.contains("content-type:") && website_content_type.contains("xml"),
            "GetBucketWebsite error response should be XML, got content-type: {website_content_type}"
        );
        let website_body = parse_body(&website_raw);
        assert!(
            website_body.contains("<Code>NoSuchWebsiteConfiguration</Code>"),
            "GetBucketWebsite should return NoSuchWebsiteConfiguration code, got: {website_body}"
        );

        let delete_raw =
            execute_s3_awscurl("DELETE", &format!("{}/{bucket}?website=", env.url), &env.access_key, &env.secret_key)
                .expect("DeleteBucketWebsite HTTP request failed");
        assert_eq!(parse_status(&delete_raw), Some(204), "DeleteBucketWebsite should return 204");

        env.stop_server();
    }
}
