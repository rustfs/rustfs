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

// src/cloud/metadata.rs

use async_trait::async_trait;
use ipnetwork::IpNetwork;
use reqwest::Client;
use serde::Deserialize;
use std::net::Ipv4Addr;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, info, warn};

/// Error in obtaining cloud service provider metadata
#[derive(Error, Debug)]
pub enum CloudMetadataError {
    #[error("HTTP request failed: {0}")]
    HttpRequestFailed(#[from] reqwest::Error),

    #[error("JSON parsing fails: {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error("Metadata Service Unavailable: {0}")]
    MetadataUnavailable(String),

    #[error("IP address resolution failed: {0}")]
    IpParseError(String),

    #[error("Unsupported cloud service providers: {0}")]
    UnsupportedProvider(String),

    #[error("Misconfiguration: {0}")]
    ConfigurationError(String),
}

/// Cloud service provider type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudProvider {
    Aws,
    Azure,
    Gcp,
    DigitalOcean,
    Vultr,
    Linode,
    Oracle,
    Alibaba,
    Tencent,
    /// Unknown or customized
    Unknown(String),
}

impl CloudProvider {
    /// Automatically detect cloud service providers from environment variables
    pub fn detect_from_env() -> Option<Self> {
        // Check various cloud service provider-specific environment variables

        // AWS
        if std::env::var("AWS_EXECUTION_ENV").is_ok()
            || std::env::var("AWS_REGION").is_ok()
            || std::env::var("EC2_INSTANCE_ID").is_ok()
        {
            return Some(Self::Aws);
        }

        // Azure
        if std::env::var("WEBSITE_SITE_NAME").is_ok()
            || std::env::var("WEBSITE_INSTANCE_ID").is_ok()
            || std::env::var("APPSETTING_WEBSITE_SITE_NAME").is_ok()
        {
            return Some(Self::Azure);
        }

        // GCP
        if std::env::var("GCP_PROJECT").is_ok()
            || std::env::var("GOOGLE_CLOUD_PROJECT").is_ok()
            || std::env::var("GAE_INSTANCE").is_ok()
        {
            return Some(Self::Gcp);
        }

        // DigitalOcean
        if std::env::var("DIGITALOCEAN_REGION").is_ok() {
            return Some(Self::DigitalOcean);
        }

        // Vultr
        if std::env::var("VULTR_REGION").is_ok() {
            return Some(Self::Vultr);
        }

        // Linode
        if std::env::var("LINODE_REGION").is_ok() {
            return Some(Self::Linode);
        }

        // Oracle
        if std::env::var("OCI_REGION").is_ok() {
            return Some(Self::Oracle);
        }

        // Alibaba Cloud
        if std::env::var("ALIBABA_CLOUD_REGION").is_ok() {
            return Some(Self::Alibaba);
        }

        // Tencent Cloud
        if std::env::var("TENCENTCLOUD_REGION").is_ok() {
            return Some(Self::Tencent);
        }

        None
    }

    /// Get the cloud service provider name
    pub fn name(&self) -> &str {
        match self {
            Self::Aws => "aws",
            Self::Azure => "azure",
            Self::Gcp => "gcp",
            Self::DigitalOcean => "digitalocean",
            Self::Vultr => "vultr",
            Self::Linode => "linode",
            Self::Oracle => "oracle",
            Self::Alibaba => "alibaba",
            Self::Tencent => "tencent",
            Self::Unknown(name) => name,
        }
    }
}

/// Cloud metadata fetcher characteristics
#[async_trait]
pub trait CloudMetadataFetcher {
    /// Get the cloud service provider name
    fn provider_name(&self) -> &str;

    /// Gets the CIDR range of the network where the instance is located
    async fn fetch_network_cidrs(&self) -> Result<Vec<IpNetwork>, CloudMetadataError>;

    /// Get the cloud service provider's public IP range (e.g., load balancer, NAT gateway, etc.)
    async fn fetch_public_ip_ranges(&self) -> Result<Vec<IpNetwork>, CloudMetadataError>;

    /// Get the IP range of a trusted proxy
    async fn fetch_trusted_proxy_ranges(&self) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        // Default implementation: Merge network CIDR and public IP ranges
        let mut ranges = Vec::new();

        match self.fetch_network_cidrs().await {
            Ok(cidrs) => ranges.extend(cidrs),
            Err(e) => warn!("Get network CIDR failed: {}", e),
        }

        match self.fetch_public_ip_ranges().await {
            Ok(public_ranges) => ranges.extend(public_ranges),
            Err(e) => warn!("Failed to get public IP range: {}", e),
        }

        Ok(ranges)
    }
}

/// AWS Metadata Fetcher
pub struct AwsMetadataFetcher {
    client: Client,
    metadata_endpoint: String,
}

impl AwsMetadataFetcher {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            metadata_endpoint: "http://169.254.169.254".to_string(),
        }
    }

    /// Get an IMDSv2 token
    async fn get_metadata_token(&self) -> Result<String, CloudMetadataError> {
        let url = format!("{}/latest/api/token", self.metadata_endpoint);

        let response = self
            .client
            .put(&url)
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()
            .await
            .map_err(|e| {
                debug!("AWS IMDSv2 token acquisition failed, try IMDSv1: {}", e);
                CloudMetadataError::MetadataUnavailable(format!("IMDSv2 failed: {}", e))
            })?;

        if response.status().is_success() {
            let token = response.text().await?;
            Ok(token)
        } else {
            Err(CloudMetadataError::MetadataUnavailable("Unable to obtain IMDSv2 tokens".to_string()))
        }
    }

    /// Use tokens to get metadata
    async fn get_metadata_with_token(&self, path: &str, token: Option<&str>) -> Result<String, CloudMetadataError> {
        let url = format!("{}/latest/{}", self.metadata_endpoint, path);

        let mut request = self.client.get(&url);

        if let Some(t) = token {
            request = request.header("X-aws-ec2-metadata-token", t);
        }

        let response = request.send().await?;

        if response.status().is_success() {
            let text = response.text().await?;
            Ok(text)
        } else {
            Err(CloudMetadataError::MetadataUnavailable(format!(
                "Metadata path {} returns status:{}",
                path,
                response.status()
            )))
        }
    }

    /// Get a list of MAC addresses
    async fn get_mac_addresses(&self, token: Option<&str>) -> Result<Vec<String>, CloudMetadataError> {
        let text = self
            .get_metadata_with_token("meta-data/network/interfaces/macs/", token)
            .await?;

        let macs: Vec<String> = text
            .lines()
            .map(|line| line.trim().trim_end_matches('/'))
            .filter(|mac| !mac.is_empty())
            .map(String::from)
            .collect();

        Ok(macs)
    }

    /// Get the VPC CIDR block
    async fn get_vpc_cidr_blocks(&self, token: Option<&str>) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        let macs = self.get_mac_addresses(token).await?;
        let mut cidrs = Vec::new();

        for mac in macs {
            let path = format!("meta-data/network/interfaces/macs/{}/vpc-ipv4-cidr-block", mac);

            match self.get_metadata_with_token(&path, token).await {
                Ok(cidr_text) => {
                    let cidr_text = cidr_text.trim();
                    if let Ok(network) = cidr_text.parse::<IpNetwork>() {
                        cidrs.push(network);
                        debug!("To get a VPC CIDR: {}", network);
                    }
                }
                Err(e) => {
                    debug!("Unable to obtain VPC CIDR for MAC {}: {}", mac, e);
                }
            }
        }

        Ok(cidrs)
    }

    /// Get the subnet CIDR block
    async fn get_subnet_cidr_blocks(&self, token: Option<&str>) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        let macs = self.get_mac_addresses(token).await?;
        let mut cidrs = Vec::new();

        for mac in macs {
            let path = format!("meta-data/network/interfaces/macs/{}/subnet-ipv4-cidr-block", mac);

            match self.get_metadata_with_token(&path, token).await {
                Ok(cidr_text) => {
                    let cidr_text = cidr_text.trim();
                    if let Ok(network) = cidr_text.parse::<IpNetwork>() {
                        cidrs.push(network);
                        debug!("Get the subnet CIDR: {}", network);
                    }
                }
                Err(e) => {
                    debug!("The subnet CIDR for MAC cannot be obtained {}: {}", mac, e);
                }
            }
        }

        Ok(cidrs)
    }

    /// Get public IP ranges for AWS (from official sources)
    async fn get_aws_public_ip_ranges(&self) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        let url = "https://ip-ranges.amazonaws.com/ip-ranges.json";

        #[derive(Debug, Deserialize)]
        struct AwsIpRanges {
            prefixes: Vec<AwsPrefix>,
        }

        #[derive(Debug, Deserialize)]
        struct AwsPrefix {
            ip_prefix: String,
            region: String,
            service: String,
        }

        let response = self.client.get(url).timeout(Duration::from_secs(5)).send().await?;

        if response.status().is_success() {
            let ip_ranges: AwsIpRanges = response.json().await?;

            let mut ranges = Vec::new();
            for prefix in ip_ranges.prefixes {
                // Include only service-specific IP ranges (e.g., EC2, CLOUDFRONT, etc.)
                if matches!(prefix.service.as_str(), "EC2" | "CLOUDFRONT" | "ROUTE53" | "ROUTE53_HEALTHCHECKS") {
                    if let Ok(network) = prefix.ip_prefix.parse::<IpNetwork>() {
                        ranges.push(network);
                    }
                }
            }

            info!("{} public IP ranges are obtained from AWS officially", ranges.len());
            Ok(ranges)
        } else {
            Err(CloudMetadataError::MetadataUnavailable(format!(
                "AWS IP Ranges API returns status:{}",
                response.status()
            )))
        }
    }
}

#[async_trait]
impl CloudMetadataFetcher for AwsMetadataFetcher {
    fn provider_name(&self) -> &str {
        "aws"
    }

    async fn fetch_network_cidrs(&self) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        let mut cidrs = Vec::new();

        // 尝试获取 IMDSv2 令牌
        let token = match self.get_metadata_token().await {
            Ok(t) => Some(t),
            Err(_) => {
                debug!("Using IMDSv1 (no token)");
                None
            }
        };

        // 获取 VPC CIDR
        match self.get_vpc_cidr_blocks(token.as_deref()).await {
            Ok(vpc_cidrs) => cidrs.extend(vpc_cidrs),
            Err(e) => debug!("Failed to obtain VPC CIDR:{}", e),
        }

        // 获取子网 CIDR
        match self.get_subnet_cidr_blocks(token.as_deref()).await {
            Ok(subnet_cidrs) => cidrs.extend(subnet_cidrs),
            Err(e) => debug!("Failed to get subnet CIDR: {}", e),
        }

        if cidrs.is_empty() {
            Err(CloudMetadataError::MetadataUnavailable("No network CIDR can be obtained".to_string()))
        } else {
            info!("{} network CIDRs were obtained from AWS metadata", cidrs.len());
            Ok(cidrs)
        }
    }

    async fn fetch_public_ip_ranges(&self) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        self.get_aws_public_ip_ranges().await
    }
}

/// Azure Metadata Fetcher
pub struct AzureMetadataFetcher {
    client: Client,
    metadata_endpoint: String,
}

impl AzureMetadataFetcher {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            metadata_endpoint: "http://169.254.169.254".to_string(),
        }
    }

    /// Get Azure metadata
    async fn get_metadata(&self, path: &str) -> Result<String, CloudMetadataError> {
        let url = format!("{}/metadata/{}?api-version=2021-05-01", self.metadata_endpoint, path);

        let response = self.client.get(&url).header("Metadata", "true").send().await?;

        if response.status().is_success() {
            let text = response.text().await?;
            Ok(text)
        } else {
            Err(CloudMetadataError::MetadataUnavailable(format!(
                "Azure metadata path {} Return status: {}",
                path,
                response.status()
            )))
        }
    }

    /// Get Azure public IP ranges
    async fn get_azure_public_ip_ranges(&self) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        // Azure Public IP Range Download URL
        let urls = [
            "https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519",
            "https://download.microsoft.com/download/7/1/D/71D86715-5596-4529-9B13-DA13A5DE5B63/ServiceTags_Public_20231211.json",
        ];

        for url in urls.iter() {
            match self.fetch_azure_ip_ranges_from_url(url).await {
                Ok(ranges) => {
                    info!("{} public IP ranges are downloaded from Azure", ranges.len());
                    return Ok(ranges);
                }
                Err(e) => {
                    debug!("Failed to get Azure IP range from {}: {}", url, e);
                }
            }
        }

        Err(CloudMetadataError::MetadataUnavailable(
            "Azure public IP ranges cannot be obtained".to_string(),
        ))
    }

    async fn fetch_azure_ip_ranges_from_url(&self, url: &str) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        #[derive(Debug, Deserialize)]
        struct AzureServiceTags {
            values: Vec<AzureServiceTag>,
        }

        #[derive(Debug, Deserialize)]
        struct AzureServiceTag {
            properties: AzureServiceTagProperties,
        }

        #[derive(Debug, Deserialize)]
        struct AzureServiceTagProperties {
            address_prefixes: Vec<String>,
        }

        let response = self.client.get(url).timeout(Duration::from_secs(10)).send().await?;

        if response.status().is_success() {
            let service_tags: AzureServiceTags = response.json().await?;

            let mut ranges = Vec::new();
            for tag in service_tags.values {
                for prefix in tag.properties.address_prefixes {
                    if let Ok(network) = prefix.parse::<IpNetwork>() {
                        ranges.push(network);
                    }
                }
            }

            Ok(ranges)
        } else {
            Err(CloudMetadataError::MetadataUnavailable(format!(
                "Azure IP range URL returns status:{}",
                response.status()
            )))
        }
    }
}

#[async_trait]
impl CloudMetadataFetcher for AzureMetadataFetcher {
    fn provider_name(&self) -> &str {
        "azure"
    }

    async fn fetch_network_cidrs(&self) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        // Azure metadata provides network interface information
        let metadata = self.get_metadata("instance/network/interface").await?;

        #[derive(Debug, Deserialize)]
        struct AzureNetworkInterface {
            ipv4: AzureIpv4Info,
        }

        #[derive(Debug, Deserialize)]
        struct AzureIpv4Info {
            subnet: Vec<AzureSubnet>,
        }

        #[derive(Debug, Deserialize)]
        struct AzureSubnet {
            address: String,
            prefix: String,
        }

        let interfaces: Vec<AzureNetworkInterface> = serde_json::from_str(&metadata)?;

        let mut cidrs = Vec::new();
        for interface in interfaces {
            for subnet in interface.ipv4.subnet {
                let cidr = format!("{}/{}", subnet.address, subnet.prefix);
                if let Ok(network) = cidr.parse::<IpNetwork>() {
                    cidrs.push(network);
                }
            }
        }

        if cidrs.is_empty() {
            Err(CloudMetadataError::MetadataUnavailable(
                "Azure network CIDR can't be obtained".to_string(),
            ))
        } else {
            info!("{} network CIDRs were obtained from Azure metadata", cidrs.len());
            Ok(cidrs)
        }
    }

    async fn fetch_public_ip_ranges(&self) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        self.get_azure_public_ip_ranges().await
    }
}

/// GCP metadata fetcher
pub struct GcpMetadataFetcher {
    client: Client,
    metadata_endpoint: String,
}

impl GcpMetadataFetcher {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            metadata_endpoint: "http://metadata.google.internal".to_string(),
        }
    }

    /// Get GCP metadata
    async fn get_metadata(&self, path: &str) -> Result<String, CloudMetadataError> {
        let url = format!("{}/computeMetadata/v1/{}", self.metadata_endpoint, path);

        let response = self.client.get(&url).header("Metadata-Flavor", "Google").send().await?;

        if response.status().is_success() {
            let text = response.text().await?;
            Ok(text)
        } else {
            Err(CloudMetadataError::MetadataUnavailable(format!(
                "GCP metadata path {} returns status:{}",
                path,
                response.status()
            )))
        }
    }

    /// Get GCP public IP ranges
    async fn get_gcp_public_ip_ranges(&self) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        let url = "https://www.gstatic.com/ipranges/cloud.json";

        #[derive(Debug, Deserialize)]
        struct GcpIpRanges {
            prefixes: Vec<GcpPrefix>,
        }

        #[derive(Debug, Deserialize)]
        struct GcpPrefix {
            ipv4_prefix: Option<String>,
            ipv6_prefix: Option<String>,
        }

        let response = self.client.get(url).timeout(Duration::from_secs(5)).send().await?;

        if response.status().is_success() {
            let ip_ranges: GcpIpRanges = response.json().await?;

            let mut ranges = Vec::new();
            for prefix in ip_ranges.prefixes {
                if let Some(ipv4_prefix) = prefix.ipv4_prefix {
                    if let Ok(network) = ipv4_prefix.parse::<IpNetwork>() {
                        ranges.push(network);
                    }
                }
            }

            info!("{} public IP ranges were obtained from GCP officials", ranges.len());
            Ok(ranges)
        } else {
            Err(CloudMetadataError::MetadataUnavailable(format!(
                "GCP IP Range API returns status:{}",
                response.status()
            )))
        }
    }
}

#[async_trait]
impl CloudMetadataFetcher for GcpMetadataFetcher {
    fn provider_name(&self) -> &str {
        "gcp"
    }

    async fn fetch_network_cidrs(&self) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        // Get network interface information
        let metadata = self.get_metadata("instance/network-interfaces/").await?;

        let interface_indices: Vec<usize> = metadata
            .lines()
            .filter_map(|line| {
                let line = line.trim().trim_end_matches('/');
                if line.chars().all(|c| c.is_digit(10)) {
                    line.parse().ok()
                } else {
                    None
                }
            })
            .collect();

        let mut cidrs = Vec::new();

        for index in interface_indices {
            // Get the subnet range
            let subnet_path = format!("instance/network-interfaces/{}/subnetworks", index);
            match self.get_metadata(&subnet_path).await {
                Ok(_subnet_metadata) => {
                    // Subnet metadata may contain CIDR information
                    // Simplified processing: we get IP addresses and netmasks
                    let ip_path = format!("instance/network-interfaces/{}/ip", index);
                    let mask_path = format!("instance/network-interfaces/{}/subnetmask", index);

                    if let (Ok(ip), Ok(mask)) = tokio::join!(self.get_metadata(&ip_path), self.get_metadata(&mask_path)) {
                        if let (Ok(ip_addr), Ok(mask_len)) = (ip.trim().parse::<Ipv4Addr>(), mask_to_prefix_length(&mask)) {
                            if let Ok(network) = format!("{}/{}", ip_addr, mask_len).parse::<IpNetwork>() {
                                cidrs.push(network);
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("Failed to get GCP subnet information: {}", e);
                }
            }
        }

        if cidrs.is_empty() {
            Err(CloudMetadataError::MetadataUnavailable("GCP network CIDR is not available".to_string()))
        } else {
            info!("{} network CIDRs were obtained from GCP metadata", cidrs.len());
            Ok(cidrs)
        }
    }

    async fn fetch_public_ip_ranges(&self) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        self.get_gcp_public_ip_ranges().await
    }
}

/// Convert the subnet mask to the prefix length
pub fn mask_to_prefix_length(mask: &str) -> Result<u8, CloudMetadataError> {
    let mask_parts: Vec<&str> = mask.split('.').collect();
    if mask_parts.len() != 4 {
        return Err(CloudMetadataError::IpParseError(format!("Invalid subnet masks:{}", mask)));
    }

    let mut prefix_length = 0;
    for part in mask_parts {
        let octet: u8 = part
            .parse()
            .map_err(|_| CloudMetadataError::IpParseError(format!("Invalid mask octet:{}", part)))?;

        let mut remaining = octet;
        while remaining > 0 {
            if remaining & 0x80 == 0x80 {
                prefix_length += 1;
                remaining <<= 1;
            } else {
                break;
            }
        }

        if remaining != 0 {
            return Err(CloudMetadataError::IpParseError("Non-contiguous subnet masks".to_string()));
        }
    }

    Ok(prefix_length)
}

/// Universal Cloud Metadata Fetcher (Auto-Detect)
pub struct CloudMetadataDetector {
    client: Client,
    provider: Option<CloudProvider>,
}

impl CloudMetadataDetector {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap_or_else(|_| Client::new());

        let provider = CloudProvider::detect_from_env();

        if let Some(p) = &provider {
            info!("Cloud service provider detected:{}", p.name());
        } else {
            info!("The cloud service provider is not detected, and it may be running on-premises or in an unknown environment");
        }

        Self { client, provider }
    }

    /// Create a fetcher for a specific cloud service provider
    pub fn create_fetcher(&self) -> Option<Box<dyn CloudMetadataFetcher + Send + Sync>> {
        match self.provider {
            Some(CloudProvider::Aws) => Some(Box::new(AwsMetadataFetcher::new())),
            Some(CloudProvider::Azure) => Some(Box::new(AzureMetadataFetcher::new())),
            Some(CloudProvider::Gcp) => Some(Box::new(GcpMetadataFetcher::new())),
            Some(CloudProvider::DigitalOcean) => {
                // DigitalOcean has a similar implementation
                None
            }
            _ => None,
        }
    }

    /// Try metadata endpoints for all cloud providers
    pub async fn try_all_providers(&self) -> Result<Vec<IpNetwork>, CloudMetadataError> {
        let providers: Vec<Box<dyn CloudMetadataFetcher + Send + Sync>> = vec![
            Box::new(AwsMetadataFetcher::new()),
            Box::new(AzureMetadataFetcher::new()),
            Box::new(GcpMetadataFetcher::new()),
        ];

        for provider in providers {
            let provider_name = provider.provider_name();
            debug!("Try getting metadata from {}", provider_name);

            match provider.fetch_trusted_proxy_ranges().await {
                Ok(ranges) => {
                    if !ranges.is_empty() {
                        info!("{} IP ranges are obtained from {}", provider_name, ranges.len());
                        return Ok(ranges);
                    }
                }
                Err(e) => {
                    debug!("Failed to get metadata from {}: {}", provider_name, e);
                }
            }
        }

        Err(CloudMetadataError::MetadataUnavailable(
            "All cloud service provider metadata fetching fails".to_string(),
        ))
    }
}

/// Main export function - Get the IP range from the cloud service provider
pub async fn fetch_cloud_provider_ips() -> Result<Vec<String>, CloudMetadataError> {
    let detector = CloudMetadataDetector::new();

    let ip_ranges = if let Some(fetcher) = detector.create_fetcher() {
        // Use a detected cloud service provider
        fetcher.fetch_trusted_proxy_ranges().await?
    } else {
        // Try all cloud service providers
        detector.try_all_providers().await?
    };

    // Convert to a list of strings
    let result: Vec<String> = ip_ranges.into_iter().map(|network| network.to_string()).collect();

    Ok(result)
}

/// Asynchronously Obtaining CSP IP Ranges (with Timeout)
pub async fn fetch_cloud_provider_ips_with_timeout(timeout_secs: u64) -> Result<Vec<String>, CloudMetadataError> {
    tokio::time::timeout(Duration::from_secs(timeout_secs), fetch_cloud_provider_ips())
        .await
        .map_err(|_| CloudMetadataError::MetadataUnavailable("Metadata fetch timeout".to_string()))?
}

/// Synchronous version (used in a synchronous context)
pub fn fetch_cloud_provider_ips_sync(timeout_secs: u64) -> Result<Vec<String>, CloudMetadataError> {
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| CloudMetadataError::ConfigurationError(format!("Unable to create runtime: {}", e)))?;

    runtime.block_on(fetch_cloud_provider_ips_with_timeout(timeout_secs))
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_mask_to_prefix_length() {
        use crate::cloud::mask_to_prefix_length;

        assert_eq!(mask_to_prefix_length("255.255.255.0").unwrap(), 24);
        assert_eq!(mask_to_prefix_length("255.255.0.0").unwrap(), 16);
        assert_eq!(mask_to_prefix_length("255.0.0.0").unwrap(), 8);
        assert_eq!(mask_to_prefix_length("255.255.255.252").unwrap(), 30);

        // Invalid masks should fail
        assert!(mask_to_prefix_length("255.255.255.1").is_err());
        assert!(mask_to_prefix_length("invalid").is_err());
    }

    #[tokio::test]
    async fn test_cloud_metadata_fallback() {
        use crate::cloud::metadata::CloudMetadataDetector;

        // In a test environment, the metadata service should not be available
        let detector = CloudMetadataDetector::new();

        // Trying all providers should fail (unless running tests in a real cloud environment)
        let result = detector.try_all_providers().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_cloud_ip_parsing() {
        use ipnetwork::IpNetwork;

        // Test IP range resolution
        let cidr: IpNetwork = "10.0.0.0/8".parse().unwrap();
        assert_eq!(cidr.prefix(), 8);

        let cidr: IpNetwork = "192.168.1.0/24".parse().unwrap();
        assert_eq!(cidr.prefix(), 24);

        // IPv6
        let cidr: IpNetwork = "2001:db8::/32".parse().unwrap();
        assert_eq!(cidr.prefix(), 32);
    }
}
