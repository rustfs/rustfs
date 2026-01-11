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

//! Validation utility functions

use http::HeaderMap;
use lazy_static::lazy_static;
use regex::Regex;
use std::net::IpAddr;
use std::str::FromStr;

/// 验证工具函数集合
pub struct ValidationUtils;

impl ValidationUtils {
    /// 验证电子邮件地址
    pub fn is_valid_email(email: &str) -> bool {
        lazy_static! {
            static ref EMAIL_REGEX: Regex =
                Regex::new(r"^([a-z0-9_+]([a-z0-9_+.]*[a-z0-9_+])?)@([a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,6})").unwrap();
        }

        EMAIL_REGEX.is_match(email)
    }

    /// 验证 URL
    pub fn is_valid_url(url: &str) -> bool {
        lazy_static! {
            static ref URL_REGEX: Regex =
                Regex::new(r"^(https?://)?([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}(/.*)?$").unwrap();
        }

        URL_REGEX.is_match(url)
    }

    /// 验证 X-Forwarded-For 头部
    pub fn validate_x_forwarded_for(header_value: &str) -> bool {
        if header_value.is_empty() {
            return false;
        }

        // 分割 IP 地址
        let ips: Vec<&str> = header_value.split(',').map(|s| s.trim()).collect();

        // 检查每个 IP 地址
        for ip_str in ips {
            if ip_str.is_empty() {
                return false;
            }

            // 移除端口部分（如果存在）
            let ip_part = ip_str.split(':').next().unwrap_or(ip_str);

            if IpAddr::from_str(ip_part).is_err() {
                return false;
            }
        }

        true
    }

    /// 验证 Forwarded 头部（RFC 7239）
    pub fn validate_forwarded_header(header_value: &str) -> bool {
        if header_value.is_empty() {
            return false;
        }

        // 简化的验证：检查基本格式
        let parts: Vec<&str> = header_value.split(';').collect();

        if parts.is_empty() {
            return false;
        }

        // 检查每个部分是否包含等号
        for part in parts {
            let part = part.trim();
            if !part.contains('=') {
                return false;
            }
        }

        true
    }

    /// 验证 IP 地址是否在允许的范围内
    pub fn validate_ip_in_range(ip: &IpAddr, cidr_ranges: &[String]) -> bool {
        for cidr in cidr_ranges {
            if let Ok(network) = ipnetwork::IpNetwork::from_str(cidr) {
                if network.contains(*ip) {
                    return true;
                }
            }
        }

        false
    }

    /// 验证头部是否包含恶意内容
    pub fn validate_header_value(value: &str) -> bool {
        // 检查是否包含控制字符（除了水平制表符）
        for c in value.chars() {
            if c.is_control() && c != '\t' && c != '\n' && c != '\r' {
                return false;
            }
        }

        // 检查长度限制（防止头部过大攻击）
        if value.len() > 8192 {
            return false;
        }

        true
    }

    /// 验证整个头部映射
    pub fn validate_headers(headers: &HeaderMap) -> bool {
        for (name, value) in headers {
            // 检查头部名称
            let name_str = name.as_str();
            if name_str.len() > 256 {
                return false;
            }

            // 检查头部值
            if let Ok(value_str) = value.to_str() {
                if !Self::validate_header_value(value_str) {
                    return false;
                }
            } else {
                // 无法转换为字符串，可能包含二进制数据
                if value.len() > 8192 {
                    return false;
                }
            }
        }

        true
    }

    /// 验证端口号
    pub fn validate_port(port: u16) -> bool {
        port > 0 && port <= 65535
    }

    /// 验证 CIDR 表示法
    pub fn validate_cidr(cidr: &str) -> bool {
        ipnetwork::IpNetwork::from_str(cidr).is_ok()
    }

    /// 验证代理链长度
    pub fn validate_proxy_chain_length(chain: &[IpAddr], max_length: usize) -> bool {
        chain.len() <= max_length
    }

    /// 验证代理链是否连续
    pub fn validate_proxy_chain_continuity(chain: &[IpAddr]) -> bool {
        if chain.len() < 2 {
            return true;
        }

        // 检查是否有重复的相邻 IP
        for i in 1..chain.len() {
            if chain[i] == chain[i - 1] {
                return false;
            }
        }

        true
    }

    /// 验证字符串是否只包含安全字符
    pub fn is_safe_string(s: &str) -> bool {
        // 允许的字符：字母、数字、基本标点符号
        let safe_pattern = Regex::new(r"^[a-zA-Z0-9\-._~:/?#\[\]@!$&'()*+,;=]+$").unwrap();
        safe_pattern.is_match(s)
    }

    /// 验证速率限制参数
    pub fn validate_rate_limit_params(requests: u32, period_seconds: u64) -> bool {
        requests > 0 && requests <= 10000 && period_seconds > 0 && period_seconds <= 86400
    }

    /// 验证缓存参数
    pub fn validate_cache_params(capacity: usize, ttl_seconds: u64) -> bool {
        capacity > 0 && capacity <= 1000000 && ttl_seconds > 0 && ttl_seconds <= 86400
    }

    /// 脱敏敏感数据
    pub fn mask_sensitive_data(data: &str, sensitive_patterns: &[&str]) -> String {
        let mut result = data.to_string();

        for pattern in sensitive_patterns {
            let regex = Regex::new(&format!(r#"(?i){}[:=]\s*([^&\s]+)"#, pattern)).unwrap();
            result = regex
                .replace_all(&result, |caps: &regex::Captures| format!("{}:[REDACTED]", &caps[1]))
                .to_string();
        }

        result
    }
}
