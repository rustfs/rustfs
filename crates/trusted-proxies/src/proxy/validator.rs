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

//! Proxy validator for validating proxy chains and client information

use std::net::{IpAddr, SocketAddr};
use std::time::Instant;

use axum::http::HeaderMap;
use tracing::{debug, trace, warn};

use crate::config::{TrustedProxyConfig, ValidationMode};
use crate::error::ProxyError;
use crate::proxy::chain::ProxyChainAnalyzer;
use crate::proxy::metrics::ProxyMetrics;

/// 客户端信息验证结果
#[derive(Debug, Clone)]
pub struct ClientInfo {
    /// 真实客户端 IP 地址（已验证）
    pub real_ip: IpAddr,
    /// 原始请求主机名（如果来自可信代理）
    pub forwarded_host: Option<String>,
    /// 原始请求协议（如果来自可信代理）
    pub forwarded_proto: Option<String>,
    /// 请求是否来自可信代理
    pub is_from_trusted_proxy: bool,
    /// 直接连接的代理 IP（如果经过代理）
    pub proxy_ip: Option<IpAddr>,
    /// 代理链长度
    pub proxy_hops: usize,
    /// 验证模式
    pub validation_mode: ValidationMode,
    /// 验证警告信息
    pub warnings: Vec<String>,
}

impl ClientInfo {
    /// 创建直接连接的客户端信息（无代理）
    pub fn direct(addr: SocketAddr) -> Self {
        Self {
            real_ip: addr.ip(),
            forwarded_host: None,
            forwarded_proto: None,
            is_from_trusted_proxy: false,
            proxy_ip: None,
            proxy_hops: 0,
            validation_mode: ValidationMode::Lenient,
            warnings: Vec::new(),
        }
    }

    /// 从可信代理创建客户端信息
    pub fn from_trusted_proxy(
        real_ip: IpAddr,
        forwarded_host: Option<String>,
        forwarded_proto: Option<String>,
        proxy_ip: IpAddr,
        proxy_hops: usize,
        validation_mode: ValidationMode,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            real_ip,
            forwarded_host,
            forwarded_proto,
            is_from_trusted_proxy: true,
            proxy_ip: Some(proxy_ip),
            proxy_hops,
            validation_mode,
            warnings,
        }
    }

    /// 获取客户端信息的字符串表示（用于日志）
    pub fn to_log_string(&self) -> String {
        format!(
            "client_ip={}, proxy={:?}, hops={}, trusted={}, mode={:?}",
            self.real_ip, self.proxy_ip, self.proxy_hops, self.is_from_trusted_proxy, self.validation_mode
        )
    }
}

/// 代理验证器
#[derive(Debug, Clone)]
pub struct ProxyValidator {
    /// 代理配置
    config: TrustedProxyConfig,
    /// 代理链分析器
    chain_analyzer: ProxyChainAnalyzer,
    /// 监控指标
    metrics: Option<ProxyMetrics>,
}

impl ProxyValidator {
    /// 创建新的代理验证器
    pub fn new(config: TrustedProxyConfig, metrics: Option<ProxyMetrics>) -> Self {
        let chain_analyzer = ProxyChainAnalyzer::new(config.clone());

        Self {
            config,
            chain_analyzer,
            metrics,
        }
    }

    /// 验证请求并提取客户端信息
    pub fn validate_request(&self, peer_addr: Option<SocketAddr>, headers: &HeaderMap) -> Result<ClientInfo, ProxyError> {
        let start_time = Instant::now();

        // 记录验证开始
        self.record_metric_start();

        // 验证请求
        let result = self.validate_request_internal(peer_addr, headers);

        // 记录验证结果
        let duration = start_time.elapsed();
        self.record_metric_result(&result, duration);

        result
    }

    /// 内部验证逻辑
    fn validate_request_internal(&self, peer_addr: Option<SocketAddr>, headers: &HeaderMap) -> Result<ClientInfo, ProxyError> {
        // 如果没有对端地址，使用默认值
        let peer_addr = peer_addr.unwrap_or_else(|| SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 0));

        // 检查是否来自可信代理
        if self.config.is_trusted(&peer_addr) {
            debug!("Request from trusted proxy: {}", peer_addr.ip());

            // 来自可信代理，解析转发头部
            self.validate_trusted_proxy_request(&peer_addr, headers)
        } else {
            // 检查是否为私有网络地址
            if self.config.is_private_network(&peer_addr.ip()) {
                warn!(
                    "Request from private network but not trusted: {}. This might be a configuration issue.",
                    peer_addr.ip()
                );
            }

            // 来自不可信代理或直接连接
            Ok(ClientInfo::direct(peer_addr))
        }
    }

    /// 验证来自可信代理的请求
    fn validate_trusted_proxy_request(&self, proxy_addr: &SocketAddr, headers: &HeaderMap) -> Result<ClientInfo, ProxyError> {
        let proxy_ip = proxy_addr.ip();

        // 优先使用 RFC 7239 Forwarded 头部（如果启用）
        let client_info = if self.config.enable_rfc7239 {
            self.try_parse_rfc7239_headers(headers, proxy_ip)
                .unwrap_or_else(|| self.parse_legacy_headers(headers, proxy_ip))
        } else {
            self.parse_legacy_headers(headers, proxy_ip)
        };

        // 验证代理链
        let chain_analysis = self
            .chain_analyzer
            .analyze_chain(&client_info.proxy_chain, proxy_ip, headers)?;

        // 检查代理链长度
        if chain_analysis.hops > self.config.max_hops {
            return Err(ProxyError::ChainTooLong(chain_analysis.hops, self.config.max_hops));
        }

        // 检查链连续性（如果启用）
        if self.config.enable_chain_continuity_check && !chain_analysis.is_continuous {
            return Err(ProxyError::ChainNotContinuous);
        }

        // 创建客户端信息
        let warnings = if !chain_analysis.warnings.is_empty() {
            chain_analysis.warnings
        } else {
            Vec::new()
        };

        Ok(ClientInfo::from_trusted_proxy(
            chain_analysis.client_ip,
            client_info.forwarded_host,
            client_info.forwarded_proto,
            proxy_ip,
            chain_analysis.hops,
            self.config.validation_mode,
            warnings,
        ))
    }

    /// 尝试解析 RFC 7239 Forwarded 头部
    fn try_parse_rfc7239_headers(&self, headers: &HeaderMap, proxy_ip: IpAddr) -> Option<ParsedHeaders> {
        headers
            .get("forwarded")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| Self::parse_forwarded_header(s, proxy_ip))
    }

    /// 解析传统的代理头部
    fn parse_legacy_headers(&self, headers: &HeaderMap, proxy_ip: IpAddr) -> ParsedHeaders {
        let forwarded_host = headers
            .get("x-forwarded-host")
            .and_then(|h| h.to_str().ok())
            .map(String::from);

        let forwarded_proto = headers
            .get("x-forwarded-proto")
            .and_then(|h| h.to_str().ok())
            .map(String::from);

        let proxy_chain = headers
            .get("x-forwarded-for")
            .and_then(|h| h.to_str().ok())
            .map(|s| Self::parse_x_forwarded_for(s))
            .unwrap_or_else(Vec::new);

        ParsedHeaders {
            proxy_chain,
            forwarded_host,
            forwarded_proto,
        }
    }

    /// 解析 RFC 7239 Forwarded 头部
    fn parse_forwarded_header(header_value: &str, proxy_ip: IpAddr) -> Option<ParsedHeaders> {
        // 简化实现：只处理第一个值
        let first_part = header_value.split(',').next()?.trim();

        let mut proxy_chain = Vec::new();
        let mut forwarded_host = None;
        let mut forwarded_proto = None;

        // 解析键值对
        for part in first_part.split(';') {
            let part = part.trim();
            if let Some((key, value)) = part.split_once('=') {
                let key = key.trim().to_lowercase();
                let value = value.trim().trim_matches('"');

                match key.as_str() {
                    "for" => {
                        // 解析客户端 IP（可能包含端口）
                        if let Some(ip_part) = value.split(':').next() {
                            if let Ok(ip) = ip_part.parse::<IpAddr>() {
                                proxy_chain.push(ip);
                            }
                        }
                    }
                    "host" => {
                        forwarded_host = Some(value.to_string());
                    }
                    "proto" => {
                        forwarded_proto = Some(value.to_string());
                    }
                    _ => {}
                }
            }
        }

        // 如果没有找到客户端 IP，添加代理 IP 作为备选
        if proxy_chain.is_empty() {
            proxy_chain.push(proxy_ip);
        }

        Some(ParsedHeaders {
            proxy_chain,
            forwarded_host,
            forwarded_proto,
        })
    }

    /// 解析 X-Forwarded-For 头部
    fn parse_x_forwarded_for(header_value: &str) -> Vec<IpAddr> {
        header_value
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .filter_map(|s| {
                // 移除端口部分（如果存在）
                let ip_part = s.split(':').next().unwrap_or(s);
                ip_part.parse::<IpAddr>().ok()
            })
            .collect()
    }

    /// 记录验证开始指标
    fn record_metric_start(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.increment_validation_attempts();
        }
    }

    /// 记录验证结果指标
    fn record_metric_result(&self, result: &Result<ClientInfo, ProxyError>, duration: std::time::Duration) {
        if let Some(metrics) = &self.metrics {
            match result {
                Ok(client_info) => {
                    metrics.record_validation_success(client_info.is_from_trusted_proxy, client_info.proxy_hops, duration);
                }
                Err(err) => {
                    metrics.record_validation_failure(err, duration);

                    // 记录失败的验证（如果启用）
                    if self.config.log_failed_validations {
                        warn!("Proxy validation failed: {}", err);
                    }
                }
            }
        }
    }
}

/// 解析后的头部信息
#[derive(Debug, Clone)]
struct ParsedHeaders {
    /// 代理链（客户端 IP 在第一个位置）
    proxy_chain: Vec<IpAddr>,
    /// 转发的主机名
    forwarded_host: Option<String>,
    /// 转发的协议
    forwarded_proto: Option<String>,
}
