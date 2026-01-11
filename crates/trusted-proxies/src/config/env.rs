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

//! Environment variable configuration constants and helpers

use crate::error::ConfigError;
use ipnetwork::IpNetwork;
use std::str::FromStr;

// ==================== 代理基础配置 ====================
/// 代理验证模式
pub const ENV_PROXY_VALIDATION_MODE: &str = "TRUSTED_PROXY_VALIDATION_MODE";
pub const DEFAULT_PROXY_VALIDATION_MODE: &str = "hop_by_hop";

/// 是否启用 RFC 7239 Forwarded 头部
pub const ENV_PROXY_ENABLE_RFC7239: &str = "TRUSTED_PROXY_ENABLE_RFC7239";
pub const DEFAULT_PROXY_ENABLE_RFC7239: bool = true;

/// 最大代理跳数
pub const ENV_PROXY_MAX_HOPS: &str = "TRUSTED_PROXY_MAX_HOPS";
pub const DEFAULT_PROXY_MAX_HOPS: usize = 10;

/// 是否启用链连续性检查
pub const ENV_PROXY_CHAIN_CONTINUITY_CHECK: &str = "TRUSTED_PROXY_CHAIN_CONTINUITY_CHECK";
pub const DEFAULT_PROXY_CHAIN_CONTINUITY_CHECK: bool = true;

/// 是否记录验证失败的请求
pub const ENV_PROXY_LOG_FAILED_VALIDATIONS: &str = "TRUSTED_PROXY_LOG_FAILED_VALIDATIONS";
pub const DEFAULT_PROXY_LOG_FAILED_VALIDATIONS: bool = true;

// ==================== 可信代理配置 ====================
/// 基础可信代理列表（逗号分隔的 IP/CIDR）
pub const ENV_TRUSTED_PROXIES: &str = "TRUSTED_PROXY_NETWORKS";
pub const DEFAULT_TRUSTED_PROXIES: &str = "127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fd00::/8";

/// 额外可信代理列表（生产环境专用，可覆盖）
pub const ENV_EXTRA_TRUSTED_PROXIES: &str = "TRUSTED_PROXY_EXTRA_NETWORKS";
pub const DEFAULT_EXTRA_TRUSTED_PROXIES: &str = "";

/// 私有网络范围（用于内部代理验证）
pub const ENV_PRIVATE_NETWORKS: &str = "TRUSTED_PROXY_PRIVATE_NETWORKS";
pub const DEFAULT_PRIVATE_NETWORKS: &str = "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fd00::/8";

// ==================== 缓存配置 ====================
/// 缓存容量
pub const ENV_CACHE_CAPACITY: &str = "TRUSTED_PROXY_CACHE_CAPACITY";
pub const DEFAULT_CACHE_CAPACITY: usize = 10_000;

/// 缓存 TTL（秒）
pub const ENV_CACHE_TTL_SECONDS: &str = "TRUSTED_PROXY_CACHE_TTL_SECONDS";
pub const DEFAULT_CACHE_TTL_SECONDS: u64 = 300;

/// 缓存清理间隔（秒）
pub const ENV_CACHE_CLEANUP_INTERVAL: &str = "TRUSTED_PROXY_CACHE_CLEANUP_INTERVAL";
pub const DEFAULT_CACHE_CLEANUP_INTERVAL: u64 = 60;

// ==================== 监控配置 ====================
/// 是否启用监控指标
pub const ENV_METRICS_ENABLED: &str = "TRUSTED_PROXY_METRICS_ENABLED";
pub const DEFAULT_METRICS_ENABLED: bool = true;

/// 日志级别
pub const ENV_LOG_LEVEL: &str = "TRUSTED_PROXY_LOG_LEVEL";
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// 是否启用结构化日志
pub const ENV_STRUCTURED_LOGGING: &str = "TRUSTED_PROXY_STRUCTURED_LOGGING";
pub const DEFAULT_STRUCTURED_LOGGING: bool = false;

/// 是否启用请求追踪
pub const ENV_TRACING_ENABLED: &str = "TRUSTED_PROXY_TRACING_ENABLED";
pub const DEFAULT_TRACING_ENABLED: bool = true;

// ==================== 云服务集成 ====================
/// 是否启用云元数据获取
pub const ENV_CLOUD_METADATA_ENABLED: &str = "TRUSTED_PROXY_CLOUD_METADATA_ENABLED";
pub const DEFAULT_CLOUD_METADATA_ENABLED: bool = false;

/// 云元数据获取超时（秒）
pub const ENV_CLOUD_METADATA_TIMEOUT: &str = "TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT";
pub const DEFAULT_CLOUD_METADATA_TIMEOUT: u64 = 5;

/// 是否启用 Cloudflare IP 范围
pub const ENV_CLOUDFLARE_IPS_ENABLED: &str = "TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED";
pub const DEFAULT_CLOUDFLARE_IPS_ENABLED: bool = false;

/// 强制指定的云服务商（覆盖自动检测）
pub const ENV_CLOUD_PROVIDER_FORCE: &str = "TRUSTED_PROXY_CLOUD_PROVIDER_FORCE";
pub const DEFAULT_CLOUD_PROVIDER_FORCE: &str = "";

// ==================== 辅助函数 ====================

/// 从环境变量解析逗号分隔的IP/CIDR列表
pub fn parse_ip_list_from_env(key: &str, default: &str) -> Result<Vec<IpNetwork>, ConfigError> {
    let value = std::env::var(key).unwrap_or_else(|_| default.to_string());

    if value.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut networks = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }

        match IpNetwork::from_str(item) {
            Ok(network) => networks.push(network),
            Err(e) => {
                tracing::warn!("Failed to parse network '{}' from {}: {}", item, key, e);
            }
        }
    }

    Ok(networks)
}

/// 从环境变量解析逗号分隔的字符串列表
pub fn parse_string_list_from_env(key: &str, default: &str) -> Vec<String> {
    let value = std::env::var(key).unwrap_or_else(|_| default.to_string());

    value
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// 从环境变量获取布尔值
pub fn get_bool_from_env(key: &str, default: bool) -> bool {
    std::env::var(key)
        .map(|v| match v.to_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => true,
            "false" | "0" | "no" | "off" => false,
            _ => default,
        })
        .unwrap_or(default)
}

/// 从环境变量获取整数值
pub fn get_usize_from_env(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// 从环境变量获取 u64 值
pub fn get_u64_from_env(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// 从环境变量获取字符串值
pub fn get_string_from_env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// 检查环境变量是否已设置
pub fn is_env_set(key: &str) -> bool {
    std::env::var(key).is_ok()
}

/// 获取所有与可信代理相关的环境变量（用于调试）
pub fn get_all_proxy_env_vars() -> Vec<(String, String)> {
    let vars = [
        ENV_PROXY_VALIDATION_MODE,
        ENV_PROXY_ENABLE_RFC7239,
        ENV_PROXY_MAX_HOPS,
        ENV_PROXY_CHAIN_CONTINUITY_CHECK,
        ENV_TRUSTED_PROXIES,
        ENV_EXTRA_TRUSTED_PROXIES,
        ENV_CLOUD_METADATA_ENABLED,
        ENV_CLOUD_METADATA_TIMEOUT,
        ENV_CLOUDFLARE_IPS_ENABLED,
    ];

    vars.iter()
        .filter_map(|&key| std::env::var(key).ok().map(|value| (key.to_string(), value)))
        .collect()
}
