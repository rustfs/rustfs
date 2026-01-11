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

//! Azure Cloud metadata fetching implementation

use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::cloud::detector::CloudMetadataFetcher;
use crate::error::AppError;

/// Azure 元数据获取器
#[derive(Debug, Clone)]
pub struct AzureMetadataFetcher {
    client: Client,
    metadata_endpoint: String,
}

impl AzureMetadataFetcher {
    /// 创建新的 Azure 元数据获取器
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

    /// 获取 Azure 元数据
    async fn get_metadata(&self, path: &str) -> Result<String, AppError> {
        let url = format!("{}/metadata/{}?api-version=2021-05-01", self.metadata_endpoint, path);

        debug!("Fetching Azure metadata from: {}", url);

        match self.client.get(&url).header("Metadata", "true").send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let text = response
                        .text()
                        .await
                        .map_err(|e| AppError::cloud(format!("Failed to read response: {}", e)))?;
                    Ok(text)
                } else {
                    debug!("Azure metadata request failed with status: {}", response.status());
                    Err(AppError::cloud(format!("Azure metadata API returned status: {}", response.status())))
                }
            }
            Err(e) => {
                debug!("Azure metadata request failed: {}", e);
                Err(AppError::cloud(format!("Azure metadata request failed: {}", e)))
            }
        }
    }

    /// 从 Microsoft 下载 IP 范围
    async fn fetch_azure_ip_ranges(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        // Azure 官方 IP 范围下载 URL
        let url =
            "https://download.microsoft.com/download/7/1/D/71D86715-5596-4529-9B13-DA13A5DE5B63/ServiceTags_Public_20231211.json";

        #[derive(Debug, Deserialize)]
        struct AzureServiceTags {
            values: Vec<AzureServiceTag>,
        }

        #[derive(Debug, Deserialize)]
        struct AzureServiceTag {
            id: String,
            name: String,
            properties: AzureServiceTagProperties,
        }

        #[derive(Debug, Deserialize)]
        struct AzureServiceTagProperties {
            address_prefixes: Vec<String>,
            region: Option<String>,
            system_service: Option<String>,
        }

        debug!("Fetching Azure IP ranges from: {}", url);

        match self.client.get(url).timeout(Duration::from_secs(10)).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let service_tags: AzureServiceTags = response
                        .json()
                        .await
                        .map_err(|e| AppError::cloud(format!("Failed to parse Azure IP ranges: {}", e)))?;

                    let mut networks = Vec::new();

                    for tag in service_tags.values {
                        // 只包含 Azure 数据中心和前端服务的 IP 范围
                        if tag.name.contains("Azure") && !tag.name.contains("ActiveDirectory") {
                            for prefix in tag.properties.address_prefixes {
                                if let Ok(network) = ipnetwork::IpNetwork::from_str(&prefix) {
                                    networks.push(network);
                                }
                            }
                        }
                    }

                    info!("Fetched {} Azure public IP ranges", networks.len());
                    Ok(networks)
                } else {
                    debug!("Failed to fetch Azure IP ranges: {}", response.status());
                    Ok(Vec::new())
                }
            }
            Err(e) => {
                debug!("Failed to fetch Azure IP ranges: {}", e);
                // 如果 API 失败，返回默认的 Azure IP 范围
                Self::default_azure_ranges()
            }
        }
    }

    /// 默认 Azure IP 范围（作为备选）
    fn default_azure_ranges() -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        let ranges = vec![
            // Azure 全球 IP 范围
            "13.64.0.0/11",
            "13.96.0.0/13",
            "13.104.0.0/14",
            "20.33.0.0/16",
            "20.34.0.0/15",
            "20.36.0.0/14",
            "20.40.0.0/13",
            "20.48.0.0/12",
            "20.64.0.0/10",
            "20.128.0.0/16",
            "20.135.0.0/16",
            "20.136.0.0/13",
            "20.150.0.0/15",
            "20.157.0.0/16",
            "20.184.0.0/13",
            "20.190.0.0/16",
            "20.192.0.0/10",
            "40.64.0.0/10",
            "40.80.0.0/12",
            "40.96.0.0/13",
            "40.112.0.0/13",
            "40.120.0.0/14",
            "40.124.0.0/16",
            "40.125.0.0/17",
            "51.12.0.0/15",
            "51.104.0.0/15",
            "51.120.0.0/16",
            "51.124.0.0/16",
            "51.132.0.0/16",
            "51.136.0.0/15",
            "51.138.0.0/16",
            "51.140.0.0/14",
            "51.144.0.0/15",
            "52.96.0.0/12",
            "52.112.0.0/14",
            "52.120.0.0/14",
            "52.124.0.0/16",
            "52.125.0.0/16",
            "52.126.0.0/15",
            "52.130.0.0/15",
            "52.136.0.0/13",
            "52.144.0.0/15",
            "52.146.0.0/15",
            "52.148.0.0/14",
            "52.152.0.0/13",
            "52.160.0.0/12",
            "52.176.0.0/13",
            "52.184.0.0/14",
            "52.188.0.0/14",
            "52.224.0.0/11",
            "65.52.0.0/14",
            "104.40.0.0/13",
            "104.208.0.0/13",
            "104.215.0.0/16",
            "137.116.0.0/15",
            "137.135.0.0/16",
            "138.91.0.0/16",
            "157.56.0.0/16",
            "168.61.0.0/16",
            "168.62.0.0/15",
            "191.233.0.0/18",
            "193.149.0.0/19",
            // IPv6 范围
            "2603:1000::/40",
            "2603:1010::/40",
            "2603:1020::/40",
            "2603:1030::/40",
            "2603:1040::/40",
            "2603:1050::/40",
            "2603:1060::/40",
            "2603:1070::/40",
            "2603:1080::/40",
            "2603:1090::/40",
            "2603:10a0::/40",
            "2603:10b0::/40",
            "2603:10c0::/40",
            "2603:10d0::/40",
            "2603:10e0::/40",
            "2603:10f0::/40",
            "2603:1100::/40",
        ];

        let networks: Result<Vec<_>, _> = ranges.into_iter().map(|s| ipnetwork::IpNetwork::from_str(s)).collect();

        match networks {
            Ok(networks) => {
                debug!("Using default Azure IP ranges");
                Ok(networks)
            }
            Err(e) => Err(AppError::cloud(format!("Failed to parse default Azure ranges: {}", e))),
        }
    }
}

#[async_trait]
impl CloudMetadataFetcher for AzureMetadataFetcher {
    fn provider_name(&self) -> &str {
        "azure"
    }

    async fn fetch_network_cidrs(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        // 尝试从 Azure 元数据获取网络信息
        match self.get_metadata("instance/network/interface").await {
            Ok(metadata) => {
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

                let interfaces: Vec<AzureNetworkInterface> = serde_json::from_str(&metadata)
                    .map_err(|e| AppError::cloud(format!("Failed to parse Azure network metadata: {}", e)))?;

                let mut cidrs = Vec::new();
                for interface in interfaces {
                    for subnet in interface.ipv4.subnet {
                        let cidr = format!("{}/{}", subnet.address, subnet.prefix);
                        if let Ok(network) = ipnetwork::IpNetwork::from_str(&cidr) {
                            cidrs.push(network);
                        }
                    }
                }

                if !cidrs.is_empty() {
                    info!("Fetched {} network CIDRs from Azure metadata", cidrs.len());
                    Ok(cidrs)
                } else {
                    // 如果元数据中没有网络信息，使用默认的 Azure VNet 范围
                    debug!("No network CIDRs found in Azure metadata, using defaults");
                    Self::default_azure_network_ranges()
                }
            }
            Err(e) => {
                warn!("Failed to fetch Azure network metadata: {}", e);
                // 元数据获取失败，使用默认范围
                Self::default_azure_network_ranges()
            }
        }
    }

    async fn fetch_public_ip_ranges(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        self.fetch_azure_ip_ranges().await
    }
}

impl AzureMetadataFetcher {
    /// 默认 Azure 网络范围
    fn default_azure_network_ranges() -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        // Azure 虚拟网络的常见 IP 范围
        let ranges = vec![
            "10.0.0.0/8",     // 大型虚拟网络
            "172.16.0.0/12",  // 中型虚拟网络
            "192.168.0.0/16", // 小型虚拟网络
            "100.64.0.0/10",  // Azure 保留范围
            "192.0.0.0/24",   // Azure 保留
        ];

        let networks: Result<Vec<_>, _> = ranges.into_iter().map(|s| ipnetwork::IpNetwork::from_str(s)).collect();

        match networks {
            Ok(networks) => {
                debug!("Using default Azure network ranges");
                Ok(networks)
            }
            Err(e) => Err(AppError::cloud(format!("Failed to parse default network ranges: {}", e))),
        }
    }
}
