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

//! Proxy chain analysis and validation

use std::collections::HashSet;
use std::net::IpAddr;

use axum::http::HeaderMap;
use tracing::{debug, trace};

use crate::config::{TrustedProxyConfig, ValidationMode};
use crate::error::ProxyError;
use crate::utils::ip::is_valid_ip_address;

/// 代理链分析结果
#[derive(Debug, Clone)]
pub struct ChainAnalysis {
    /// 客户端真实 IP
    pub client_ip: IpAddr,
    /// 已验证的代理跳数
    pub hops: usize,
    /// 是否连续
    pub is_continuous: bool,
    /// 警告信息
    pub warnings: Vec<String>,
    /// 使用的验证模式
    pub validation_mode: ValidationMode,
    /// 可信代理部分
    pub trusted_chain: Vec<IpAddr>,
}

/// 代理链分析器
#[derive(Debug, Clone)]
pub struct ProxyChainAnalyzer {
    /// 代理配置
    config: TrustedProxyConfig,
    /// 已验证的可信代理 IP 缓存（用于快速查找）
    trusted_ip_cache: HashSet<IpAddr>,
}

impl ProxyChainAnalyzer {
    /// 创建新的代理链分析器
    pub fn new(config: TrustedProxyConfig) -> Self {
        // 构建可信 IP 缓存
        let mut trusted_ip_cache = HashSet::new();

        for proxy in &config.proxies {
            match proxy {
                crate::config::TrustedProxy::Single(ip) => {
                    trusted_ip_cache.insert(*ip);
                }
                crate::config::TrustedProxy::Cidr(network) => {
                    // 对于小网络，可以缓存所有 IP
                    // 这里我们只缓存/24及更小的网络的前几个IP作为示例
                    // 实际生产环境中可能需要更复杂的缓存策略
                    if network.prefix() >= 24 && network.prefix() <= 30 {
                        // 对于小网络，我们可以缓存网络地址和广播地址之间的几个 IP
                        // 这里简化处理，只缓存网络地址
                        if let Some(first_ip) = network.iter().next() {
                            trusted_ip_cache.insert(first_ip);
                        }
                    }
                }
            }
        }

        Self {
            config,
            trusted_ip_cache,
        }
    }

    /// 分析代理链
    pub fn analyze_chain(
        &self,
        proxy_chain: &[IpAddr],
        current_proxy_ip: IpAddr,
        headers: &HeaderMap,
    ) -> Result<ChainAnalysis, ProxyError> {
        trace!("Analyzing proxy chain: {:?} with current proxy: {}", proxy_chain, current_proxy_ip);

        // 验证 IP 地址
        self.validate_ip_addresses(proxy_chain)?;

        // 构建完整链（包括当前代理）
        let mut full_chain = proxy_chain.to_vec();
        full_chain.push(current_proxy_ip);

        // 根据验证模式分析链
        let (client_ip, trusted_chain, hops) = match self.config.validation_mode {
            ValidationMode::Lenient => self.analyze_lenient(&full_chain),
            ValidationMode::Strict => self.analyze_strict(&full_chain)?,
            ValidationMode::HopByHop => self.analyze_hop_by_hop(&full_chain),
        };

        // 检查链连续性
        let is_continuous = if self.config.enable_chain_continuity_check {
            self.check_chain_continuity(&full_chain, &trusted_chain)
        } else {
            true
        };

        // 收集警告
        let warnings = self.collect_warnings(&full_chain, &trusted_chain, headers);

        // 验证客户端 IP
        if !is_valid_ip_address(&client_ip) {
            return Err(ProxyError::internal(format!("Invalid client IP: {}", client_ip)));
        }

        Ok(ChainAnalysis {
            client_ip,
            hops,
            is_continuous,
            warnings,
            validation_mode: self.config.validation_mode,
            trusted_chain,
        })
    }

    /// 宽松模式分析：只要最后一个代理可信，就接受整个链
    fn analyze_lenient(&self, chain: &[IpAddr]) -> (IpAddr, Vec<IpAddr>, usize) {
        if chain.is_empty() {
            return (IpAddr::from([0, 0, 0, 0]), Vec::new(), 0);
        }

        // 检查最后一个代理是否可信
        if let Some(last_proxy) = chain.last() {
            if self.is_ip_trusted(last_proxy) {
                // 整个链都可信
                let client_ip = chain.first().copied().unwrap_or(*last_proxy);
                return (client_ip, chain.to_vec(), chain.len());
            }
        }

        // 如果最后一个代理不可信，使用链中第一个 IP 作为客户端
        let client_ip = chain.first().copied().unwrap_or(IpAddr::from([0, 0, 0, 0]));
        (client_ip, Vec::new(), 0)
    }

    /// 严格模式分析：要求链中所有代理都可信
    fn analyze_strict(&self, chain: &[IpAddr]) -> Result<(IpAddr, Vec<IpAddr>, usize), ProxyError> {
        if chain.is_empty() {
            return Ok((IpAddr::from([0, 0, 0, 0]), Vec::new(), 0));
        }

        // 检查每个代理是否都可信
        for (i, ip) in chain.iter().enumerate() {
            if !self.is_ip_trusted(ip) {
                return Err(ProxyError::chain_failed(format!("Proxy at position {} ({}) is not trusted", i, ip)));
            }
        }

        let client_ip = chain.first().copied().unwrap_or(IpAddr::from([0, 0, 0, 0]));
        Ok((client_ip, chain.to_vec(), chain.len()))
    }

    /// 跳数模式分析：从右向左找到第一个不可信代理
    fn analyze_hop_by_hop(&self, chain: &[IpAddr]) -> (IpAddr, Vec<IpAddr>, usize) {
        if chain.is_empty() {
            return (IpAddr::from([0, 0, 0, 0]), Vec::new(), 0);
        }

        let mut trusted_chain = Vec::new();
        let mut validated_hops = 0;

        // 从右向左遍历（从离我们最近的代理开始）
        for ip in chain.iter().rev() {
            if self.is_ip_trusted(ip) {
                trusted_chain.insert(0, *ip);
                validated_hops += 1;
            } else {
                // 找到第一个不可信代理，停止遍历
                break;
            }
        }

        if trusted_chain.is_empty() {
            // 没有可信代理，使用链的最后一个 IP
            let client_ip = *chain.last().unwrap();
            (client_ip, vec![client_ip], 0)
        } else {
            // 客户端 IP 是可信链的第一个 IP 之前的那个 IP
            let client_ip_index = chain.len().saturating_sub(trusted_chain.len());
            let client_ip = if client_ip_index > 0 {
                chain[client_ip_index - 1]
            } else {
                // 如果整个链都可信，使用第一个 IP
                chain[0]
            };

            (client_ip, trusted_chain, validated_hops)
        }
    }

    /// 检查链连续性
    fn check_chain_continuity(&self, full_chain: &[IpAddr], trusted_chain: &[IpAddr]) -> bool {
        if full_chain.len() <= 1 || trusted_chain.is_empty() {
            return true;
        }

        // 可信链应该是完整链的尾部连续部分
        if trusted_chain.len() > full_chain.len() {
            return false;
        }

        let expected_tail = &full_chain[full_chain.len() - trusted_chain.len()..];
        expected_tail == trusted_chain
    }

    /// 验证 IP 地址
    fn validate_ip_addresses(&self, chain: &[IpAddr]) -> Result<(), ProxyError> {
        for ip in chain {
            if !is_valid_ip_address(ip) {
                return Err(ProxyError::IpParseError(format!("Invalid IP address in chain: {}", ip)));
            }

            // 检查是否为特殊地址
            if ip.is_unspecified() {
                return Err(ProxyError::invalid_xff("IP address cannot be unspecified (0.0.0.0 or ::)"));
            }

            if ip.is_multicast() {
                return Err(ProxyError::invalid_xff("IP address cannot be multicast"));
            }
        }

        Ok(())
    }

    /// 检查 IP 是否可信
    fn is_ip_trusted(&self, ip: &IpAddr) -> bool {
        // 首先检查缓存
        if self.trusted_ip_cache.contains(ip) {
            return true;
        }

        // 然后检查配置中的代理
        self.config.proxies.iter().any(|proxy| proxy.contains(ip))
    }

    /// 收集警告信息
    fn collect_warnings(&self, full_chain: &[IpAddr], trusted_chain: &[IpAddr], headers: &HeaderMap) -> Vec<String> {
        let mut warnings = Vec::new();

        // 检查代理链长度
        if full_chain.len() > self.config.max_hops {
            warnings.push(format!(
                "Proxy chain length ({}) exceeds recommended maximum ({})",
                full_chain.len(),
                self.config.max_hops
            ));
        }

        // 检查是否缺少必要的头部
        if trusted_chain.len() > 0 {
            if !headers.contains_key("x-forwarded-for") && !headers.contains_key("forwarded") {
                warnings.push("No proxy headers found for trusted proxy request".to_string());
            }
        }

        // 检查是否有重复的 IP
        let mut seen_ips = HashSet::new();
        for ip in full_chain {
            if !seen_ips.insert(ip) {
                warnings.push(format!("Duplicate IP in proxy chain: {}", ip));
                break;
            }
        }

        warnings
    }
}
