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
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::{Client, Config as S3Config};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::config::Config;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub creation_date: Option<String>,
}

#[derive(Debug, Clone)]
pub struct S3Client {
    client: Client,
}

impl S3Client {
    pub async fn new(config: &Config) -> Result<Self> {
        info!("Initializing S3 client from configuration");

        let access_key = config.access_key_id();
        let secret_key = config.secret_access_key();

        debug!("Using AWS region: {}", config.region);
        if let Some(ref endpoint) = config.endpoint_url {
            debug!("Using custom endpoint: {}", endpoint);
        }

        let credentials = Credentials::new(access_key, secret_key, None, None, "rustfs-mcp-server");

        let mut config_builder = S3Config::builder()
            .credentials_provider(credentials)
            .region(Region::new(config.region.clone()))
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest());

        // Set force path style if custom endpoint or explicitly requested
        let should_force_path_style = config.endpoint_url.is_some() || config.force_path_style;
        if should_force_path_style {
            config_builder = config_builder.force_path_style(true);
        }

        if let Some(endpoint) = &config.endpoint_url {
            config_builder = config_builder.endpoint_url(endpoint);
        }

        let s3_config = config_builder.build();
        let client = Client::from_conf(s3_config);

        info!("S3 client initialized successfully");

        Ok(Self { client })
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        debug!("Listing S3 buckets");

        let response = self.client.list_buckets().send().await.context("Failed to list S3 buckets")?;

        let buckets: Vec<BucketInfo> = response
            .buckets()
            .iter()
            .map(|bucket| {
                let name = bucket.name().unwrap_or("unknown").to_string();
                let creation_date = bucket
                    .creation_date()
                    .map(|dt| dt.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime).unwrap());

                BucketInfo { name, creation_date }
            })
            .collect();

        debug!("Found {} buckets", buckets.len());
        Ok(buckets)
    }

    pub async fn health_check(&self) -> Result<()> {
        debug!("Performing S3 health check");

        self.client.list_buckets().send().await.context("S3 health check failed")?;

        debug!("S3 health check passed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires AWS credentials
    async fn test_s3_client_creation() {
        let config = Config {
            access_key_id: Some("test_key".to_string()),
            secret_access_key: Some("test_secret".to_string()),
            region: "us-east-1".to_string(),
            ..Config::default()
        };

        let result = S3Client::new(&config).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_bucket_info_serialization() {
        let bucket = BucketInfo {
            name: "test-bucket".to_string(),
            creation_date: Some("2024-01-01T00:00:00Z".to_string()),
        };

        let json = serde_json::to_string(&bucket).unwrap();
        let deserialized: BucketInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(bucket.name, deserialized.name);
        assert_eq!(bucket.creation_date, deserialized.creation_date);
    }
}
