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

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
};
use serial_test::serial;
use std::env;
use std::time::Duration;
use uuid::Uuid;

struct Settings {
    oss_endpoint: String,
    oss_access_key: String,
    oss_secret_key: String,
    oss_bucket_name: String,
    oss_lifecycle_days: i32,
    #[allow(dead_code)]
    oss_secure: bool,
    oss_region: String,
}

impl Settings {
    fn new() -> Self {
        Self {
            oss_endpoint: env::var("TEST_RUSTFS_SERVER").unwrap_or_else(|_| "http://localhost:9000".to_string()),
            oss_access_key: "rustfsadmin".to_string(),
            oss_secret_key: "rustfsadmin".to_string(),
            oss_bucket_name: "mblock99".to_string(),
            oss_lifecycle_days: 1,
            oss_secure: false,
            oss_region: "us-east-1".to_string(),
        }
    }
}

struct Oss {
    client: Client,
    bucket_name: String,
    lifecycle_days: i32,
}

impl Oss {
    async fn new(settings: &Settings) -> Result<Self> {
        let credentials = Credentials::new(&settings.oss_access_key, &settings.oss_secret_key, None, None, "test");

        let config = aws_config::defaults(BehaviorVersion::latest())
            .credentials_provider(credentials)
            .region(Region::new(settings.oss_region.clone()))
            .endpoint_url(&settings.oss_endpoint)
            .load()
            .await;

        let client = Client::new(&config);

        Ok(Self {
            client,
            bucket_name: settings.oss_bucket_name.clone(),
            lifecycle_days: settings.oss_lifecycle_days,
        })
    }

    fn new_uuid(&self) -> String {
        Uuid::new_v4().to_string()
    }

    async fn create_bucket(&self) -> Result<()> {
        match self.client.head_bucket().bucket(&self.bucket_name).send().await {
            Ok(_) => {
                println!("Bucket {} already exists", self.bucket_name);
                Ok(())
            }
            Err(_) => match self.client.create_bucket().bucket(&self.bucket_name).send().await {
                Ok(_) => {
                    self.set_lifecycle_expiration(self.lifecycle_days, None).await?;
                    println!("Bucket {} created successfully", self.bucket_name);
                    Ok(())
                }
                Err(err) => {
                    let err_msg = err.to_string();
                    if err_msg.contains("BucketAlreadyOwnedByYou") {
                        println!("Bucket {} already owned by you; continuing", self.bucket_name);
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!("Failed to create bucket: {}", err))
                    }
                }
            },
        }
    }

    async fn set_lifecycle_expiration(&self, days: i32, prefix: Option<&str>) -> Result<()> {
        let prefix_str = prefix.unwrap_or("");
        let rule_id = format!("expire-{}-{}d", if prefix_str.is_empty() { "all" } else { prefix_str }, days);

        let filter = LifecycleRuleFilter::builder().prefix(prefix_str.to_string()).build();

        let expiration = LifecycleExpiration::builder().days(days).build();

        let rule = LifecycleRule::builder()
            .id(rule_id)
            .status(ExpirationStatus::Enabled)
            .filter(filter)
            .expiration(expiration)
            .build()
            .context("Failed to build lifecycle rule")?;

        let lifecycle_config = BucketLifecycleConfiguration::builder()
            .rules(rule)
            .build()
            .context("Failed to build bucket lifecycle configuration")?;

        self.client
            .put_bucket_lifecycle_configuration()
            .bucket(&self.bucket_name)
            .lifecycle_configuration(lifecycle_config)
            .send()
            .await
            .context("Failed to set bucket lifecycle")?;

        Ok(())
    }

    #[allow(dead_code)]
    async fn upload_file(&self, filename: &str, content: &[u8]) -> Result<String> {
        let ext = std::path::Path::new(filename)
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("");

        let uuid = self.new_uuid();
        let object_name = if ext.is_empty() { uuid } else { format!("{}.{}", uuid, ext) };

        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&object_name)
            .body(content.to_vec().into())
            .send()
            .await
            .context("Failed to upload file")?;

        Ok(object_name)
    }

    #[allow(dead_code)]
    async fn get_presigned_url(&self, filename: &str) -> Result<String> {
        let expires_in = Duration::from_secs((self.lifecycle_days * 24 * 60 * 60) as u64);
        let presigning_config = PresigningConfig::expires_in(expires_in)?;

        let presigned_req = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(filename)
            .presigned(presigning_config)
            .await
            .context("Failed to get presigned URL")?;

        Ok(presigned_req.uri().to_string())
    }
}

#[tokio::test]
#[serial]
#[ignore]
async fn test_lifecycle_minio_sdk() -> Result<()> {
    let settings = Settings::new();
    let oss = Oss::new(&settings).await?;
    oss.create_bucket().await?;
    Ok(())
}
