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
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::Client;
use serial_test::serial;
use uuid::Uuid;
use std::error::Error;

const ENDPOINT: &str = "http://localhost:9000";
const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";

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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_list_objects_v2_on_nonexistent_bucket() -> Result<(), Box<dyn Error>> {
    let client = create_aws_s3_client().await?;
    let bucket_name = format!("non-existent-bucket-{}", Uuid::new_v4());

    let result = client.list_objects_v2().bucket(&bucket_name).send().await;

    match result {
        Ok(_) => {
            panic!("Expected an error for non-existent bucket, but got Ok");
        }
        Err(e) => {
            let service_error = e.as_service_error().expect("Expected a service error");
            assert!(
                service_error.is_no_such_bucket(),
                "Expected NoSuchBucket error, but got: {:?}",
                service_error
            );
            println!(
                "Successfully received NoSuchBucket error for bucket '{}'.",
                bucket_name
            );
        }
    }

    Ok(())
}
