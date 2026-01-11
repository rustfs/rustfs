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

//! Configuration type definitions

use ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use crate::error::ConfigError;

/// 代理验证模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationMode {
    /// 宽松模式：只要最后一个代理可信，就接受整个链
    Lenient,
    /// 严格模式：要求链中所有代理都可信
    Strict,
    /// 跳数验证模式：从右向左找到第一个不可信代理
    HopByHop,
}

impl ValidationMode {
    /// 从字符串解析验证模式
    pub fn from_str(s: &str) -> Result<Self, ConfigError> {
        match s.to_lowercase().as_str() {
            "lenient" => Ok(Self::Lenient),
            "strict" => Ok(Self::Strict),
            "hop_by_hop" => Ok(Self::HopByHop),
            _ => Err(ConfigError::InvalidConfig(format!(
                "Invalid validation mode: '{}'. Must be one of: lenient, strict, hop_by_hop",
                s
            ))),
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Lenient => "lenient",
            Self::Strict => "strict",
            Self::HopByHop => "hop_by_hop",
        }
    }
}

impl Default for ValidationMode {
    fn default() -> Self {
        Self::HopByHop
    }
}

/// 可信代理类型
#[derive(Debug, Clone)]
pub enum TrustedProxy {
    /// 单个 IP 地址
    Single(IpAddr),
    /// IP 地址段 (CIDR 表示法)
    Cidr(IpNetwork),
}

impl TrustedProxy {
    /// 检查 IP 是否匹配此代理配置
    pub fn contains(&self, ip: &IpAddr) -> bool {
        match self {
            Self::Single(proxy_ip) => ip == proxy_ip,
            Self::Cidr(network) => network.contains(*ip),
        }
    }

    /// 转换为字符串表示
    pub fn to_string(&self) -> String {
        match self {
            Self::Single(ip) => ip.to_string(),
            Self::Cidr(network) => network.to_string(),
        }
    }
}

/// 可信代理配置
#[derive(Debug, Clone)]
pub struct TrustedProxyConfig {
    /// 代理列表
    pub proxies: Vec<TrustedProxy>,
    /// 验证模式
    pub validation_mode: ValidationMode,
    /// 是否启用 RFC 7239 Forwarded 头部
    pub enable_rfc7239: bool,
    /// 最大代理跳数
    pub max_hops: usize,
    /// 是否启用链连续性检查
    pub enable_chain_continuity_check: bool,
    /// 私有网络范围
    pub private_networks: Vec<IpNetwork>,
}

impl TrustedProxyConfig {
    /// 创建新配置
    pub fn new(
        proxies: Vec<TrustedProxy>,
        validation_mode: ValidationMode,
        enable_rfc7239: bool,
        max_hops: usize,
        enable_chain_continuity_check: bool,
        private_networks: Vec<IpNetwork>,
    ) -> Self {
        Self {
            proxies,
            validation_mode,
            enable_rfc7239,
            max_hops,
            enable_chain_continuity_check,
            private_networks,
        }
    }

    /// 检查 SocketAddr 是否来自可信代理
    pub fn is_trusted(&self, addr: &SocketAddr) -> bool {
        let ip = addr.ip();
        self.proxies.iter().any(|proxy| proxy.contains(&ip))
    }

    /// 检查 IP 是否在私有网络范围内
    pub fn is_private_network(&self, ip: &IpAddr) -> bool {
        self.private_networks.iter().any(|network| network.contains(*ip))
    }

    /// 获取所有网络范围的字符串表示（用于调试）
    pub fn get_network_strings(&self) -> Vec<String> {
        self.proxies.iter().map(|p| p.to_string()).collect()
    }

    /// 获取配置摘要
    pub fn summary(&self) -> String {
        format!(
            "TrustedProxyConfig {{ proxies: {}, mode: {}, max_hops: {} }}",
            self.proxies.len(),
            self.validation_mode.as_str(),
            self.max_hops
        )
    }
}

/// 缓存配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// 缓存容量
    pub capacity: usize,
    /// 缓存 TTL（秒）
    pub ttl_seconds: u64,
    /// 缓存清理间隔（秒）
    pub cleanup_interval_seconds: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: 10_000,
            ttl_seconds: 300,
            cleanup_interval_seconds: 60,
        }
    }
}

impl CacheConfig {
    /// 获取缓存 TTL 时长
    pub fn ttl_duration(&self) -> Duration {
        Duration::from_secs(self.ttl_seconds)
    }

    /// 获取缓存清理间隔时长
    pub fn cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.cleanup_interval_seconds)
    }
}

/// 监控配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// 是否启用监控指标
    pub metrics_enabled: bool,
    /// 日志级别
    pub log_level: String,
    /// 是否启用结构化日志
    pub structured_logging: bool,
    /// 是否启用请求追踪
    pub tracing_enabled: bool,
    /// 是否记录验证失败的请求
    pub log_failed_validations: bool,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            log_level: "info".to_string(),
            structured_logging: false,
            tracing_enabled: true,
            log_failed_validations: true,
        }
    }
}

/// 云服务集成配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudConfig {
    /// 是否启用云元数据获取
    pub metadata_enabled: bool,
    /// 云元数据获取超时（秒）
    pub metadata_timeout_seconds: u64,
    /// 是否启用 Cloudflare IP 范围
    pub cloudflare_ips_enabled: bool,
    /// 强制指定的云服务商
    pub forced_provider: Option<String>,
}

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            metadata_enabled: false,
            metadata_timeout_seconds: 5,
            cloudflare_ips_enabled: false,
            forced_provider: None,
        }
    }
}

impl CloudConfig {
    /// 获取元数据获取超时时长
    pub fn metadata_timeout(&self) -> Duration {
        Duration::from_secs(self.metadata_timeout_seconds)
    }
}

/// 完整的应用配置
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// 代理配置
    pub proxy: TrustedProxyConfig,
    /// 缓存配置
    pub cache: CacheConfig,
    /// 监控配置
    pub monitoring: MonitoringConfig,
    /// 云服务配置
    pub cloud: CloudConfig,
    /// 服务器绑定地址
    pub server_addr: SocketAddr,
}

impl AppConfig {
    /// 创建应用配置
    pub fn new(
        proxy: TrustedProxyConfig,
        cache: CacheConfig,
        monitoring: MonitoringConfig,
        cloud: CloudConfig,
        server_addr: SocketAddr,
    ) -> Self {
        Self {
            proxy,
            cache,
            monitoring,
            cloud,
            server_addr,
        }
    }

    /// 获取配置摘要
    pub fn summary(&self) -> String {
        format!(
            "AppConfig {{\n\
            \x20\x20proxy: {},\n\
            \x20\x20cache_capacity: {},\n\
            \x20\x20metrics: {},\n\
            \x20\x20cloud_metadata: {}\n\
            }}",
            self.proxy.summary(),
            self.cache.capacity,
            self.monitoring.metrics_enabled,
            self.cloud.metadata_enabled
        )
    }
}
