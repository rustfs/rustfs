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
    use std::borrow::Borrow;

    use crate::common::{RustFSTestEnvironment, init_logging, signed_s3_request};
    use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
    use aws_sdk_s3::types::{
        AccelerateConfiguration, BucketAccelerateStatus, BucketLoggingStatus, IndexDocument, LoggingEnabled, Payer,
        RequestPaymentConfiguration, WebsiteConfiguration,
    };
    use http::Method;
    use http::header::CONTENT_TYPE;
    use tracing::info;

    fn assert_s3_error<T, E, R>(result: Result<T, R>, expected_status: u16, expected_code: &str, context: &str)
    where
        T: std::fmt::Debug,
        E: ProvideErrorMetadata + std::fmt::Debug,
        R: Borrow<SdkError<E>> + std::fmt::Debug,
    {
        let error = result.expect_err(context);
        let sdk_error = error.borrow();
        assert_eq!(
            sdk_error.raw_response().map(|response| response.status().as_u16()),
            Some(expected_status),
            "{context}: expected HTTP {expected_status}, got: {error:?}"
        );
        assert_eq!(
            sdk_error.as_service_error().and_then(ProvideErrorMetadata::code),
            Some(expected_code),
            "{context}: expected {expected_code}, got: {error:?}"
        );
    }

    #[tokio::test]
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

        let put_logging = client
            .put_bucket_logging()
            .bucket(bucket)
            .bucket_logging_status(
                BucketLoggingStatus::builder()
                    .logging_enabled(
                        LoggingEnabled::builder()
                            .target_bucket(bucket)
                            .target_prefix("logs/")
                            .build()
                            .expect("failed to build LoggingEnabled"),
                    )
                    .build(),
            )
            .send()
            .await;
        assert!(
            put_logging.is_ok(),
            "PutBucketLogging should return success for existing bucket, got: {:?}",
            put_logging.err()
        );

        let output_after_put = client
            .get_bucket_logging()
            .bucket(bucket)
            .send()
            .await
            .expect("GetBucketLogging should succeed after PutBucketLogging");
        let logging_after_put = output_after_put
            .logging_enabled()
            .expect("GetBucketLogging should return persisted logging_enabled");
        assert_eq!(
            logging_after_put.target_bucket(),
            bucket,
            "GetBucketLogging should preserve target bucket"
        );
        assert_eq!(
            logging_after_put.target_prefix(),
            "logs/",
            "GetBucketLogging should preserve target prefix"
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

        let put_accelerate = client
            .put_bucket_accelerate_configuration()
            .bucket(bucket)
            .accelerate_configuration(
                AccelerateConfiguration::builder()
                    .status(BucketAccelerateStatus::Suspended)
                    .build(),
            )
            .send()
            .await;
        assert!(
            put_accelerate.is_ok(),
            "PutBucketAccelerateConfiguration should return success for existing bucket, got: {:?}",
            put_accelerate.err()
        );

        let put_request_payment = client
            .put_bucket_request_payment()
            .bucket(bucket)
            .request_payment_configuration(
                RequestPaymentConfiguration::builder()
                    .payer(Payer::Requester)
                    .build()
                    .expect("failed to build RequestPaymentConfiguration"),
            )
            .send()
            .await;
        assert!(
            put_request_payment.is_ok(),
            "PutBucketRequestPayment should return success for existing bucket, got: {:?}",
            put_request_payment.err()
        );

        let accelerate_after_put = client
            .get_bucket_accelerate_configuration()
            .bucket(bucket)
            .send()
            .await
            .expect("GetBucketAccelerateConfiguration should succeed after put");
        assert_eq!(
            accelerate_after_put.status().map(|s| s.as_str()),
            Some("Suspended"),
            "GetBucketAccelerateConfiguration should preserve put status"
        );

        let payment_after_put = client
            .get_bucket_request_payment()
            .bucket(bucket)
            .send()
            .await
            .expect("GetBucketRequestPayment should succeed after put");
        assert_eq!(
            payment_after_put.payer().map(|p| p.as_str()),
            Some("Requester"),
            "GetBucketRequestPayment should preserve put payer"
        );

        let put_website = client
            .put_bucket_website()
            .bucket(bucket)
            .website_configuration(
                WebsiteConfiguration::builder()
                    .index_document(
                        IndexDocument::builder()
                            .suffix("index.html")
                            .build()
                            .expect("failed to build IndexDocument"),
                    )
                    .build(),
            )
            .send()
            .await;
        assert!(
            put_website.is_ok(),
            "PutBucketWebsite should return success for existing bucket, got: {:?}",
            put_website.err()
        );

        let website = client.get_bucket_website().bucket(bucket).send().await;
        assert!(website.is_ok(), "GetBucketWebsite should return persisted website configuration");
        let website_output = website.unwrap();
        assert_eq!(
            website_output.index_document().map(|doc| doc.suffix()),
            Some("index.html"),
            "GetBucketWebsite should preserve index document suffix"
        );

        client
            .delete_bucket_website()
            .bucket(bucket)
            .send()
            .await
            .expect("DeleteBucketWebsite should return success");

        let website_after_delete = client.get_bucket_website().bucket(bucket).send().await;
        assert_s3_error(
            website_after_delete,
            404,
            "NoSuchWebsiteConfiguration",
            "GetBucketWebsite after deleting the website configuration",
        );

        env.stop_server();
    }

    #[tokio::test]
    async fn test_dummy_bucket_compatibility_endpoints_no_such_bucket() {
        init_logging();
        info!("Starting test: dummy-compat bucket APIs should return NoSuchBucket for missing bucket");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let missing_bucket = "test-dummy-bucket-missing";

        let get_logging = client.get_bucket_logging().bucket(missing_bucket).send().await;
        assert_s3_error(get_logging, 404, "NoSuchBucket", "GetBucketLogging for a missing bucket");

        let put_logging = client
            .put_bucket_logging()
            .bucket(missing_bucket)
            .bucket_logging_status(BucketLoggingStatus::builder().build())
            .send()
            .await;
        assert_s3_error(put_logging, 404, "NoSuchBucket", "PutBucketLogging for a missing bucket");

        let get_accelerate = client
            .get_bucket_accelerate_configuration()
            .bucket(missing_bucket)
            .send()
            .await;
        assert_s3_error(
            get_accelerate,
            404,
            "NoSuchBucket",
            "GetBucketAccelerateConfiguration for a missing bucket",
        );

        let get_request_payment = client.get_bucket_request_payment().bucket(missing_bucket).send().await;
        assert_s3_error(get_request_payment, 404, "NoSuchBucket", "GetBucketRequestPayment for a missing bucket");

        let put_accelerate = client
            .put_bucket_accelerate_configuration()
            .bucket(missing_bucket)
            .accelerate_configuration(
                AccelerateConfiguration::builder()
                    .status(BucketAccelerateStatus::Suspended)
                    .build(),
            )
            .send()
            .await;
        assert_s3_error(
            put_accelerate,
            404,
            "NoSuchBucket",
            "PutBucketAccelerateConfiguration for a missing bucket",
        );

        let put_request_payment = client
            .put_bucket_request_payment()
            .bucket(missing_bucket)
            .request_payment_configuration(
                RequestPaymentConfiguration::builder()
                    .payer(Payer::BucketOwner)
                    .build()
                    .expect("failed to build RequestPaymentConfiguration"),
            )
            .send()
            .await;
        assert_s3_error(put_request_payment, 404, "NoSuchBucket", "PutBucketRequestPayment for a missing bucket");

        let put_website = client
            .put_bucket_website()
            .bucket(missing_bucket)
            .website_configuration(
                WebsiteConfiguration::builder()
                    .index_document(
                        IndexDocument::builder()
                            .suffix("index.html")
                            .build()
                            .expect("failed to build IndexDocument"),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_s3_error(put_website, 404, "NoSuchBucket", "PutBucketWebsite for a missing bucket");

        let get_website = client.get_bucket_website().bucket(missing_bucket).send().await;
        assert_s3_error(get_website, 404, "NoSuchBucket", "GetBucketWebsite for a missing bucket");

        let delete_website = client.delete_bucket_website().bucket(missing_bucket).send().await;
        assert_s3_error(delete_website, 404, "NoSuchBucket", "DeleteBucketWebsite for a missing bucket");

        env.stop_server();
    }

    #[tokio::test]
    async fn test_dummy_bucket_endpoints_http_contracts() {
        init_logging();
        info!("Starting test: dummy-compat bucket API HTTP contracts");

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

        let logging_response = signed_s3_request(
            Method::GET,
            &format!("{}/{bucket}?logging=", env.url),
            None,
            None,
            &env.access_key,
            &env.secret_key,
        )
        .await
        .expect("GetBucketLogging HTTP request failed");
        assert_eq!(logging_response.status(), 200, "GetBucketLogging should return 200");
        let logging_body = logging_response
            .text()
            .await
            .expect("Failed to read GetBucketLogging response body");
        assert!(
            logging_body.contains("<BucketLoggingStatus"),
            "GetBucketLogging response should contain BucketLoggingStatus XML, got: {logging_body}"
        );

        let accel_response = signed_s3_request(
            Method::GET,
            &format!("{}/{bucket}?accelerate=", env.url),
            None,
            None,
            &env.access_key,
            &env.secret_key,
        )
        .await
        .expect("GetBucketAccelerateConfiguration HTTP request failed");
        assert_eq!(accel_response.status(), 200, "GetBucketAccelerateConfiguration should return 200");
        let accel_body = accel_response
            .text()
            .await
            .expect("Failed to read GetBucketAccelerateConfiguration response body");
        assert!(
            accel_body.contains("<AccelerateConfiguration"),
            "GetBucketAccelerateConfiguration response should contain AccelerateConfiguration XML, got: {accel_body}"
        );

        let payment_response = signed_s3_request(
            Method::GET,
            &format!("{}/{bucket}?requestPayment=", env.url),
            None,
            None,
            &env.access_key,
            &env.secret_key,
        )
        .await
        .expect("GetBucketRequestPayment HTTP request failed");
        assert_eq!(payment_response.status(), 200, "GetBucketRequestPayment should return 200");
        let payment_body = payment_response
            .text()
            .await
            .expect("Failed to read GetBucketRequestPayment response body");
        assert!(
            payment_body.contains("<Payer>BucketOwner</Payer>"),
            "GetBucketRequestPayment should return BucketOwner payer, got: {payment_body}"
        );

        let website_response = signed_s3_request(
            Method::GET,
            &format!("{}/{bucket}?website=", env.url),
            None,
            None,
            &env.access_key,
            &env.secret_key,
        )
        .await
        .expect("GetBucketWebsite HTTP request failed");
        assert_eq!(
            website_response.status(),
            404,
            "GetBucketWebsite should return 404 when website config is absent"
        );
        let website_content_type = website_response
            .headers()
            .get(CONTENT_TYPE)
            .expect("GetBucketWebsite response should include Content-Type")
            .to_str()
            .expect("GetBucketWebsite Content-Type should be valid ASCII")
            .to_ascii_lowercase();
        assert!(
            website_content_type.contains("xml"),
            "GetBucketWebsite error response should be XML, got content-type: {website_content_type}"
        );
        let website_body = website_response
            .text()
            .await
            .expect("Failed to read GetBucketWebsite response body");
        assert!(
            website_body.contains("<Code>NoSuchWebsiteConfiguration</Code>"),
            "GetBucketWebsite should return NoSuchWebsiteConfiguration code, got: {website_body}"
        );

        let delete_response = signed_s3_request(
            Method::DELETE,
            &format!("{}/{bucket}?website=", env.url),
            None,
            None,
            &env.access_key,
            &env.secret_key,
        )
        .await
        .expect("DeleteBucketWebsite HTTP request failed");
        assert_eq!(delete_response.status(), 204, "DeleteBucketWebsite should return 204");

        env.stop_server();
    }
}
