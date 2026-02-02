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

//! Cloud provider detection and metadata fetching.

use async_trait::async_trait;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::AppError;

/// Supported cloud providers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CloudProvider {
    /// Amazon Web Services
    Aws,
    /// Microsoft Azure
    Azure,
    /// Google Cloud Platform
    Gcp,
    /// DigitalOcean
    DigitalOcean,
    /// Cloudflare
    Cloudflare,
    /// Unknown or custom provider.
    Unknown(String),
}

impl FromStr for CloudProvider {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "aws" | "amazon" => Self::Aws,
            "azure" | "microsoft" => Self::Azure,
            "gcp" | "google" => Self::Gcp,
            "digitalocean" | "do" => Self::DigitalOcean,
            "cloudflare" | "cf" => Self::Cloudflare,
            _ => Self::Unknown(s.to_string()),
        })
    }
}

impl CloudProvider {
    /// Detects the cloud provider based on environment variables.
    pub fn detect_from_env() -> Option<Self> {
        // Check for AWS environment variables.
        if std::env::var("RUSTFS_AWS_EXECUTION_ENV").is_ok()
            || std::env::var("RUSTFS_AWS_REGION").is_ok()
            || std::env::var("RUSTFS_EC2_INSTANCE_ID").is_ok()
        {
            return Some(Self::Aws);
        }

        // Check for Azure environment variables.
        if std::env::var("RUSTFS_WEBSITE_SITE_NAME").is_ok()
            || std::env::var("RUSTFS_WEBSITE_INSTANCE_ID").is_ok()
            || std::env::var("RUSTFS_APPSETTING_WEBSITE_SITE_NAME").is_ok()
        {
            return Some(Self::Azure);
        }

        // Check for GCP environment variables.
        if std::env::var("RUSTFS_GCP_PROJECT").is_ok()
            || std::env::var("RUSTFS_GOOGLE_CLOUD_PROJECT").is_ok()
            || std::env::var("RUSTFS_GAE_INSTANCE").is_ok()
        {
            return Some(Self::Gcp);
        }

        // Check for DigitalOcean environment variables.
        if std::env::var("RUSTFS_DIGITALOCEAN_REGION").is_ok() {
            return Some(Self::DigitalOcean);
        }

        // Check for Cloudflare environment variables.
        if std::env::var("RUSTFS_CF_PAGES").is_ok() || std::env::var("RUSTFS_CF_WORKERS").is_ok() {
            return Some(Self::Cloudflare);
        }

        None
    }

    /// Returns the canonical name of the cloud provider.
    pub fn name(&self) -> &str {
        match self {
            Self::Aws => "aws",
            Self::Azure => "azure",
            Self::Gcp => "gcp",
            Self::DigitalOcean => "digitalocean",
            Self::Cloudflare => "cloudflare",
            Self::Unknown(name) => name,
        }
    }
}

/// Trait for fetching metadata from a specific cloud provider.
#[async_trait]
pub trait CloudMetadataFetcher: Send + Sync {
    /// Returns the name of the provider.
    fn provider_name(&self) -> &str;

    /// Fetches the network CIDR ranges for the current instance.
    async fn fetch_network_cidrs(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError>;

    /// Fetches the public IP ranges for the cloud provider.
    async fn fetch_public_ip_ranges(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError>;

    /// Fetches all IP ranges that should be considered trusted proxies.
    async fn fetch_trusted_proxy_ranges(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        let mut ranges = Vec::new();

        match self.fetch_network_cidrs().await {
            Ok(cidrs) => ranges.extend(cidrs),
            Err(e) => warn!("Failed to fetch network CIDRs from {}: {}", self.provider_name(), e),
        }

        match self.fetch_public_ip_ranges().await {
            Ok(public_ranges) => ranges.extend(public_ranges),
            Err(e) => warn!("Failed to fetch public IP ranges from {}: {}", self.provider_name(), e),
        }

        Ok(ranges)
    }
}

/// Detector for identifying the current cloud environment and fetching relevant metadata.
#[derive(Debug, Clone)]
pub struct CloudDetector {
    /// Whether cloud detection is enabled.
    enabled: bool,
    /// Timeout for metadata requests.
    timeout: Duration,
    /// Optionally force a specific provider.
    forced_provider: Option<CloudProvider>,
}

impl CloudDetector {
    /// Creates a new `CloudDetector`.
    pub fn new(enabled: bool, timeout: Duration, forced_provider: Option<String>) -> Self {
        let forced_provider = forced_provider.and_then(|s| CloudProvider::from_str(&s).ok());

        Self {
            enabled,
            timeout,
            forced_provider,
        }
    }

    /// Identifies the current cloud provider.
    pub fn detect_provider(&self) -> Option<CloudProvider> {
        if !self.enabled {
            return None;
        }

        if let Some(provider) = self.forced_provider.as_ref() {
            return Some(provider.clone());
        }

        CloudProvider::detect_from_env()
    }

    /// Fetches trusted IP ranges for the detected cloud provider.
    pub async fn fetch_trusted_ranges(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        if !self.enabled {
            debug!("Cloud metadata fetching is disabled");
            return Ok(Vec::new());
        }

        let provider = self.detect_provider();

        match provider {
            Some(CloudProvider::Aws) => {
                info!("Detected AWS environment, fetching metadata");
                let fetcher = crate::AwsMetadataFetcher::new(self.timeout);
                fetcher.fetch_trusted_proxy_ranges().await
            }
            Some(CloudProvider::Azure) => {
                info!("Detected Azure environment, fetching metadata");
                let fetcher = crate::AzureMetadataFetcher::new(self.timeout);
                fetcher.fetch_trusted_proxy_ranges().await
            }
            Some(CloudProvider::Gcp) => {
                info!("Detected GCP environment, fetching metadata");
                let fetcher = crate::GcpMetadataFetcher::new(self.timeout);
                fetcher.fetch_trusted_proxy_ranges().await
            }
            Some(CloudProvider::Cloudflare) => {
                info!("Detected Cloudflare environment");
                let ranges = crate::CloudflareIpRanges::fetch().await?;
                Ok(ranges)
            }
            Some(CloudProvider::DigitalOcean) => {
                info!("Detected DigitalOcean environment");
                let ranges = crate::DigitalOceanIpRanges::fetch().await?;
                Ok(ranges)
            }
            Some(CloudProvider::Unknown(name)) => {
                warn!("Unknown cloud provider detected: {}", name);
                Ok(Vec::new())
            }
            None => {
                debug!("No cloud provider detected");
                Ok(Vec::new())
            }
        }
    }

    /// Attempts to fetch metadata from all supported providers sequentially.
    pub async fn try_all_providers(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        if !self.enabled {
            return Ok(Vec::new());
        }

        let providers: Vec<Box<dyn CloudMetadataFetcher>> = vec![
            Box::new(crate::AwsMetadataFetcher::new(self.timeout)),
            Box::new(crate::AzureMetadataFetcher::new(self.timeout)),
            Box::new(crate::GcpMetadataFetcher::new(self.timeout)),
        ];

        for provider in providers {
            let provider_name = provider.provider_name();
            debug!("Trying to fetch metadata from {}", provider_name);

            match provider.fetch_trusted_proxy_ranges().await {
                Ok(ranges) => {
                    if !ranges.is_empty() {
                        info!("Fetched {} IP ranges from {}", ranges.len(), provider_name);
                        return Ok(ranges);
                    }
                }
                Err(e) => {
                    debug!("Failed to fetch metadata from {}: {}", provider_name, e);
                }
            }
        }

        Ok(Vec::new())
    }
}

/// Returns a default `CloudDetector` with detection disabled.
pub fn default_cloud_detector() -> CloudDetector {
    CloudDetector::new(false, Duration::from_secs(5), None)
}
