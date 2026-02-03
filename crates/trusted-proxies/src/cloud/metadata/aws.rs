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

//! AWS metadata fetching implementation for identifying trusted proxy ranges.

use async_trait::async_trait;
use reqwest::Client;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info};

use crate::AppError;
use crate::CloudMetadataFetcher;

/// Fetcher for AWS-specific metadata.
#[derive(Debug, Clone)]
pub struct AwsMetadataFetcher {
    client: Client,
    metadata_endpoint: String,
}

impl AwsMetadataFetcher {
    /// Creates a new `AwsMetadataFetcher`.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Duration to use for HTTP request timeouts.
    ///
    /// Returns a new instance of `AwsMetadataFetcher`.
    pub fn new(timeout: Duration) -> Self {
        let client = Client::builder().timeout(timeout).build().unwrap_or_else(|_| Client::new());

        Self {
            client,
            metadata_endpoint: "http://169.254.169.254".to_string(),
        }
    }

    /// Retrieves an IMDSv2 token for secure metadata access.
    #[allow(dead_code)]
    async fn get_metadata_token(&self) -> Result<String, AppError> {
        let url = format!("{}/latest/api/token", self.metadata_endpoint);

        match self
            .client
            .put(&url)
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    let token = response
                        .text()
                        .await
                        .map_err(|e| AppError::cloud(format!("Failed to read IMDSv2 token: {}", e)))?;
                    Ok(token)
                } else {
                    debug!("IMDSv2 token request failed with status: {}", response.status());
                    Err(AppError::cloud("Failed to obtain IMDSv2 token"))
                }
            }
            Err(e) => {
                debug!("IMDSv2 token request failed: {}", e);
                Err(AppError::cloud(format!("IMDSv2 request failed: {}", e)))
            }
        }
    }
}

#[async_trait]
impl CloudMetadataFetcher for AwsMetadataFetcher {
    fn provider_name(&self) -> &str {
        "aws"
    }

    async fn fetch_network_cidrs(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        // Simplified implementation: returns standard AWS VPC private ranges.
        let default_ranges = vec![
            "10.0.0.0/8",     // Large VPCs
            "172.16.0.0/12",  // Medium VPCs
            "192.168.0.0/16", // Small VPCs
        ];

        let networks: Result<Vec<_>, _> = default_ranges.into_iter().map(ipnetwork::IpNetwork::from_str).collect();

        match networks {
            Ok(networks) => {
                debug!("Using default AWS VPC network ranges");
                Ok(networks)
            }
            Err(e) => Err(AppError::cloud(format!("Failed to parse default AWS ranges: {}", e))),
        }
    }

    async fn fetch_public_ip_ranges(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        let url = "https://ip-ranges.amazonaws.com/ip-ranges.json";

        #[derive(Debug, serde::Deserialize)]
        struct AwsIpRanges {
            prefixes: Vec<AwsPrefix>,
        }

        #[derive(Debug, serde::Deserialize)]
        struct AwsPrefix {
            ip_prefix: String,
            service: String,
        }

        match self.client.get(url).timeout(Duration::from_secs(5)).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let ip_ranges: AwsIpRanges = response
                        .json()
                        .await
                        .map_err(|e| AppError::cloud(format!("Failed to parse AWS IP ranges JSON: {}", e)))?;

                    let mut networks = Vec::new();

                    for prefix in ip_ranges.prefixes {
                        // Include EC2 and CloudFront ranges as potential trusted proxies.
                        if (prefix.service == "EC2" || prefix.service == "CLOUDFRONT")
                            && let Ok(network) = ipnetwork::IpNetwork::from_str(&prefix.ip_prefix)
                        {
                            networks.push(network);
                        }
                    }

                    info!("Successfully fetched {} AWS public IP ranges", networks.len());
                    Ok(networks)
                } else {
                    debug!("Failed to fetch AWS IP ranges: HTTP {}", response.status());
                    Ok(Vec::new())
                }
            }
            Err(e) => {
                debug!("Failed to fetch AWS IP ranges: {}", e);
                Ok(Vec::new())
            }
        }
    }
}
