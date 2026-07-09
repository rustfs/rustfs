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

//! Regression test for rustfs/backlog#629(b): region-aware `CreateBucket`.
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
    use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
    use serial_test::serial;
    use std::error::Error;

    /// `CreateBucket` with a `LocationConstraint` body must pass SigV4 validation
    /// and create the bucket, mirroring `minio-go` `MakeBucket(bucket, "us-east-1")`.
    #[tokio::test]
    #[serial]
    async fn test_create_bucket_with_us_east_1_location_constraint() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;
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
    #[serial]
    async fn test_create_bucket_without_location_constraint() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;
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
}
