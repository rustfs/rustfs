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

//! Cloud provider detection and metadata fetching

use async_trait::async_trait;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::error::AppError;

/// 云服务商类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    /// 未知或自定义
    Unknown(String),
}

impl CloudProvider {
    /// 从环境变量检测云服务商
    pub fn detect_from_env() -> Option<Self> {
        // 检查 AWS 环境变量
        if std::env::var("AWS_EXECUTION_ENV").is_ok()
            || std::env::var("AWS_REGION").is_ok()
            || std::env::var("EC2_INSTANCE_ID").is_ok()
        {
            return Some(Self::Aws);
        }

        // 检查 Azure 环境变量
        if std::env::var("WEBSITE_SITE_NAME").is_ok()
            || std::env::var("WEBSITE_INSTANCE_ID").is_ok()
            || std::env::var("APPSETTING_WEBSITE_SITE_NAME").is_ok()
        {
            return Some(Self::Azure);
        }

        // 检查 GCP 环境变量
        if std::env::var("GCP_PROJECT").is_ok()
            || std::env::var("GOOGLE_CLOUD_PROJECT").is_ok()
            || std::env::var("GAE_INSTANCE").is_ok()
        {
            return Some(Self::Gcp);
        }

        // 检查 DigitalOcean 环境变量
        if std::env::var("DIGITALOCEAN_REGION").is_ok() {
            return Some(Self::DigitalOcean);
        }

        // 检查 Cloudflare 环境变量
        if std::env::var("CF_PAGES").is_ok() || std::env::var("CF_WORKERS").is_ok() {
            return Some(Self::Cloudflare);
        }

        None
    }

    /// 获取云服务商名称
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

    /// 从字符串解析云服务商
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "aws" | "amazon" => Self::Aws,
            "azure" | "microsoft" => Self::Azure,
            "gcp" | "google" => Self::Gcp,
            "digitalocean" | "do" => Self::DigitalOcean,
            "cloudflare" | "cf" => Self::Cloudflare,
            _ => Self::Unknown(s.to_string()),
        }
    }
}

/// 云元数据获取器特征
#[async_trait]
pub trait CloudMetadataFetcher: Send + Sync {
    /// 获取云服务商名称
    fn provider_name(&self) -> &str;

    /// 获取实例所在的网络 CIDR 范围
    async fn fetch_network_cidrs(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError>;

    /// 获取云服务商的公共 IP 范围
    async fn fetch_public_ip_ranges(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError>;

    /// 获取可信代理的 IP 范围
    async fn fetch_trusted_proxy_ranges(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        let mut ranges = Vec::new();

        // 尝试获取网络 CIDR
        match self.fetch_network_cidrs().await {
            Ok(cidrs) => ranges.extend(cidrs),
            Err(e) => warn!("Failed to fetch network CIDRs from {}: {}", self.provider_name(), e),
        }

        // 尝试获取公共 IP 范围
        match self.fetch_public_ip_ranges().await {
            Ok(public_ranges) => ranges.extend(public_ranges),
            Err(e) => warn!("Failed to fetch public IP ranges from {}: {}", self.provider_name(), e),
        }

        Ok(ranges)
    }
}

/// 云服务检测器
#[derive(Debug, Clone)]
pub struct CloudDetector {
    /// 是否启用云检测
    enabled: bool,
    /// 超时时间
    timeout: Duration,
    /// 强制指定的云服务商
    forced_provider: Option<CloudProvider>,
}

impl CloudDetector {
    /// 创建新的云检测器
    pub fn new(enabled: bool, timeout: Duration, forced_provider: Option<String>) -> Self {
        let forced_provider = forced_provider.map(|s| CloudProvider::from_str(&s));

        Self {
            enabled,
            timeout,
            forced_provider,
        }
    }

    /// 检测云服务商
    pub fn detect_provider(&self) -> Option<CloudProvider> {
        if !self.enabled {
            return None;
        }

        // 如果强制指定了云服务商，直接返回
        if let Some(provider) = self.forced_provider {
            return Some(provider);
        }

        // 自动检测
        CloudProvider::detect_from_env()
    }

    /// 获取可信代理 IP 范围
    pub async fn fetch_trusted_ranges(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        if !self.enabled {
            debug!("Cloud metadata fetching is disabled");
            return Ok(Vec::new());
        }

        let provider = self.detect_provider();

        match provider {
            Some(CloudProvider::Aws) => {
                info!("Detected AWS environment, fetching metadata");
                let fetcher = crate::cloud::metadata::AwsMetadataFetcher::new();
                fetcher.fetch_trusted_proxy_ranges().await
            }
            Some(CloudProvider::Azure) => {
                info!("Detected Azure environment, fetching metadata");
                let fetcher = crate::cloud::metadata::AzureMetadataFetcher::new();
                fetcher.fetch_trusted_proxy_ranges().await
            }
            Some(CloudProvider::Gcp) => {
                info!("Detected GCP environment, fetching metadata");
                let fetcher = crate::cloud::metadata::GcpMetadataFetcher::new();
                fetcher.fetch_trusted_proxy_ranges().await
            }
            Some(CloudProvider::Cloudflare) => {
                info!("Detected Cloudflare environment");
                let ranges = crate::cloud::ranges::CloudflareIpRanges::fetch().await?;
                Ok(ranges)
            }
            Some(CloudProvider::DigitalOcean) => {
                info!("Detected DigitalOcean environment");
                let ranges = crate::cloud::ranges::DigitalOceanIpRanges::fetch().await?;
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

    /// 尝试所有云服务商获取元数据
    pub async fn try_all_providers(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        if !self.enabled {
            return Ok(Vec::new());
        }

        let providers: Vec<Box<dyn CloudMetadataFetcher>> = vec![
            Box::new(crate::cloud::metadata::AwsMetadataFetcher::new()),
            Box::new(crate::cloud::metadata::AzureMetadataFetcher::new()),
            Box::new(crate::cloud::metadata::GcpMetadataFetcher::new()),
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

/// 默认云检测器
pub fn default_cloud_detector() -> CloudDetector {
    CloudDetector::new(
        false, // 默认禁用
        Duration::from_secs(5),
        None,
    )
}
