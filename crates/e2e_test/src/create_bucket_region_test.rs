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

//! Region-aware bucket creation and location regressions: backlog#629(b), rustfs#8729.
//!
//! `minio-go`'s `MakeBucket(bucket, "us-east-1")` was reported to fail SigV4
//! validation. The SigV4 signature is verified by `s3s` over the raw request
//! body bytes (compared against `x-amz-content-sha256`); a
//! `CreateBucketConfiguration`/`LocationConstraint` body therefore does not
//! change signature canonicalization. This test proves that both a
//! region-body `CreateBucket` and a plain `CreateBucket` succeed under SigV4.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::config::Region;
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
    use std::error::Error;

    /// `CreateBucket` with a `LocationConstraint` body must pass SigV4 validation
    /// and create the bucket, mirroring `minio-go` `MakeBucket(bucket, "us-east-1")`.
    #[tokio::test]
    async fn test_create_bucket_with_us_east_1_location_constraint() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec!["--region", "us-east-1"]).await?;
        let client = env.create_s3_client();

        client
            .create_bucket()
            .bucket("region-aware-bucket")
            .create_bucket_configuration(
                CreateBucketConfiguration::builder()
                    .location_constraint(BucketLocationConstraint::from("us-east-1"))
                    .build(),
            )
            .send()
            .await
            .expect("region-aware CreateBucket must pass SigV4 validation and succeed");

        let location = client.get_bucket_location().bucket("region-aware-bucket").send().await?;
        assert_eq!(
            location.location_constraint().map(|value| value.as_str()),
            Some(""),
            "us-east-1 must serialize an empty LocationConstraint element"
        );

        // The bucket exists and is listed.
        let listed = client.list_buckets().send().await?;
        let names: Vec<&str> = listed.buckets().iter().filter_map(|b| b.name()).collect();
        assert!(names.contains(&"region-aware-bucket"), "created bucket must be listed: {names:?}");

        env.stop_server();
        Ok(())
    }

    /// A plain `CreateBucket` (no body) must also succeed; guards against a
    /// regression where an empty body would be hashed incorrectly during SigV4.
    #[tokio::test]
    async fn test_create_bucket_without_location_constraint() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec!["--region", "us-east-1"]).await?;
        let client = env.create_s3_client();

        client
            .create_bucket()
            .bucket("plain-region-bucket")
            .send()
            .await
            .expect("plain CreateBucket must succeed");

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn test_create_bucket_rejects_unserved_location_constraints() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec!["--region", "us-east-1"]).await?;
        let client = env.create_s3_client();

        for (index, region) in ["eu-1", "not-a-real-region-42", "us-west-2", "EU"].iter().enumerate() {
            let bucket = format!("rejected-region-{index}");
            let error = client
                .create_bucket()
                .bucket(&bucket)
                .create_bucket_configuration(
                    CreateBucketConfiguration::builder()
                        .location_constraint(BucketLocationConstraint::from(*region))
                        .build(),
                )
                .send()
                .await
                .expect_err("a region not served by this instance must be rejected");
            assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(400));
            assert_eq!(
                error.as_service_error().and_then(ProvideErrorMetadata::code),
                Some("InvalidLocationConstraint")
            );

            let missing = client
                .head_bucket()
                .bucket(&bucket)
                .send()
                .await
                .expect_err("rejected bucket must not exist");
            assert_eq!(missing.raw_response().map(|response| response.status().as_u16()), Some(404));
        }

        let missing = client
            .get_bucket_location()
            .bucket("missing-region-bucket")
            .send()
            .await
            .expect_err("missing bucket must remain an error");
        assert_eq!(missing.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchBucket"));
        Ok(())
    }

    #[tokio::test]
    async fn test_create_bucket_uses_configured_region() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        for region in ["eu-1", "eu-west-1"] {
            let mut env = RustFSTestEnvironment::new().await?;
            env.start_rustfs_server(vec!["--region", region]).await?;
            let client = Client::from_conf(
                env.create_s3_client()
                    .config()
                    .to_builder()
                    .region(Region::new(region))
                    .build(),
            );
            let mut configurations = vec![None, Some(CreateBucketConfiguration::builder().build())];
            for constraint in ["", region].into_iter().chain((region == "eu-west-1").then_some("EU")) {
                configurations.push(Some(
                    CreateBucketConfiguration::builder()
                        .location_constraint(BucketLocationConstraint::from(constraint))
                        .build(),
                ));
            }

            // Omitted and empty constraints retain the S3-compatible server's site default.
            for (index, configuration) in configurations.into_iter().enumerate() {
                let bucket = format!("configured-region-{index}");
                client
                    .create_bucket()
                    .bucket(&bucket)
                    .set_create_bucket_configuration(configuration)
                    .send()
                    .await?;
                let location = client.get_bucket_location().bucket(&bucket).send().await?;
                assert_eq!(location.location_constraint().map(|value| value.as_str()), Some(region));
            }

            // A correctly signed request still cannot choose another bucket region.
            let error = client
                .create_bucket()
                .bucket("wrong-default-region")
                .create_bucket_configuration(
                    CreateBucketConfiguration::builder()
                        .location_constraint(BucketLocationConstraint::from("us-east-1"))
                        .build(),
                )
                .send()
                .await
                .expect_err("the default region must not bypass a custom site region");
            assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(400));
            assert_eq!(
                error.as_service_error().and_then(ProvideErrorMetadata::code),
                Some("InvalidLocationConstraint")
            );
            let missing = client
                .head_bucket()
                .bucket("wrong-default-region")
                .send()
                .await
                .expect_err("rejected bucket must not exist");
            assert_eq!(missing.raw_response().map(|response| response.status().as_u16()), Some(404));
        }
        Ok(())
    }
}
