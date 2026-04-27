#![cfg(test)]
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

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use bytes::Bytes;
use serial_test::serial;
use std::error::Error;

const ENDPOINT: &str = "http://localhost:9000";
const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const BUCKET: &str = "test-basic-bucket";

async fn create_aws_s3_client() -> Result<Client, Box<dyn Error>> {
    let region_provider = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "static"))
        .endpoint_url(ENDPOINT)
        .load()
        .await;

    let client = Client::from_conf(
        aws_sdk_s3::Config::from(&shared_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    );
    Ok(client)
}

async fn setup_test_bucket(client: &Client) -> Result<(), Box<dyn Error>> {
    match client.create_bucket().bucket(BUCKET).send().await {
        Ok(_) => {}
        Err(e) => {
            let error_str = e.to_string();
            if !error_str.contains("BucketAlreadyOwnedByYou") && !error_str.contains("BucketAlreadyExists") {
                return Err(e.into());
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_bucket_lifecycle_configuration() -> Result<(), Box<dyn std::error::Error>> {
    use aws_sdk_s3::types::{BucketLifecycleConfiguration, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter};
    use chrono::{Duration as ChronoDuration, Utc};
    use tokio::time::Duration;

    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;

    // Upload test object first
    let test_content = "Test object for lifecycle expiration";
    let lifecycle_object_key = "lifecycle-test-object.txt";
    let untouched_object_key = "keep-object.txt";
    client
        .put_object()
        .bucket(BUCKET)
        .key(lifecycle_object_key)
        .body(Bytes::from(test_content.as_bytes()).into())
        .send()
        .await?;
    client
        .put_object()
        .bucket(BUCKET)
        .key(untouched_object_key)
        .body(Bytes::from("should-stay".as_bytes()).into())
        .send()
        .await?;

    // Verify object exists initially
    let resp = client.get_object().bucket(BUCKET).key(lifecycle_object_key).send().await?;
    assert!(resp.content_length().unwrap_or(0) > 0);
    let untouched_resp = client.get_object().bucket(BUCKET).key(untouched_object_key).send().await?;
    assert!(untouched_resp.content_length().unwrap_or(0) > 0);

    // Use a past midnight UTC date to trigger immediate lifecycle expiry without requiring days=0.
    let yesterday_midnight_utc = Utc::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight should always be valid")
        - ChronoDuration::days(1);
    let expiration = LifecycleExpiration::builder()
        .date(aws_sdk_s3::primitives::DateTime::from_secs(yesterday_midnight_utc.and_utc().timestamp()))
        .build();
    let filter = LifecycleRuleFilter::builder().prefix(lifecycle_object_key).build();
    let rule = LifecycleRule::builder()
        .id("expire-test-object")
        .filter(filter)
        .expiration(expiration)
        .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
        .build()?;
    let lifecycle = BucketLifecycleConfiguration::builder().rules(rule).build()?;

    client
        .put_bucket_lifecycle_configuration()
        .bucket(BUCKET)
        .lifecycle_configuration(lifecycle)
        .send()
        .await?;

    // Verify lifecycle configuration was set
    let resp = client.get_bucket_lifecycle_configuration().bucket(BUCKET).send().await?;
    let rules = resp.rules();
    assert!(rules.iter().any(|r| r.id().unwrap_or("") == "expire-test-object"));

    // Poll for deletion instead of using a fixed sleep to keep the test deterministic.
    // Default scanner cycle interval is 60s with jitter, so allow enough time for one full cycle.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(150);
    loop {
        let get_result = client.get_object().bucket(BUCKET).key(lifecycle_object_key).send().await;
        match get_result {
            Ok(_) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("Expected object to be deleted by lifecycle rule within 150s, but it still exists");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => {
                if let Some(service_error) = e.as_service_error() {
                    if service_error.is_no_such_key() {
                        println!("Lifecycle configuration test completed - object was successfully deleted by lifecycle rule");
                        break;
                    }
                    panic!("Expected NoSuchKey error, but got: {e:?}");
                } else {
                    panic!("Expected service error, but got: {e:?}");
                }
            }
        }
    }

    println!("Lifecycle configuration test completed.");

    // Non-matching prefix object should remain available.
    let untouched_after = client.get_object().bucket(BUCKET).key(untouched_object_key).send().await?;
    assert!(untouched_after.content_length().unwrap_or(0) > 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_bucket_lifecycle_accepts_zero_days() -> Result<(), Box<dyn std::error::Error>> {
    use aws_sdk_s3::types::{BucketLifecycleConfiguration, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter};

    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;

    let expiration = LifecycleExpiration::builder().days(0).build();
    let filter = LifecycleRuleFilter::builder().prefix("zero-days/").build();
    let rule = LifecycleRule::builder()
        .id("expire-zero-days")
        .filter(filter)
        .expiration(expiration)
        .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
        .build()?;
    let lifecycle = BucketLifecycleConfiguration::builder().rules(rule).build()?;

    client
        .put_bucket_lifecycle_configuration()
        .bucket(BUCKET)
        .lifecycle_configuration(lifecycle)
        .send()
        .await?;

    Ok(())
}
