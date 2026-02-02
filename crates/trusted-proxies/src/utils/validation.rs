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

//! Validation utility functions for various data types.

use http::HeaderMap;
use regex::Regex;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::OnceLock;

static EMAIL_REGEX: OnceLock<Regex> = OnceLock::new();
static URL_REGEX: OnceLock<Regex> = OnceLock::new();
static SAFE_REGEX: OnceLock<Regex> = OnceLock::new();

/// Collection of validation utility functions.
pub struct ValidationUtils;

impl ValidationUtils {
    /// Validates an email address format.
    pub fn is_valid_email(email: &str) -> bool {
        EMAIL_REGEX
            .get_or_init(|| Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").expect("Invalid email regex"))
            .is_match(email)
    }

    /// Validates a URL format.
    pub fn is_valid_url(url: &str) -> bool {
        URL_REGEX
            .get_or_init(|| {
                Regex::new(r"^(https?://)?([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}(/.*)?$")
                    .expect("Invalid URL regex")
            })
            .is_match(url)
    }

    /// Validates the format of an X-Forwarded-For header value.
    pub fn validate_x_forwarded_for(header_value: &str) -> bool {
        if header_value.is_empty() {
            return false;
        }

        let ips: Vec<&str> = header_value.split(',').map(|s| s.trim()).collect();

        for ip_str in ips {
            if ip_str.is_empty() {
                return false;
            }

            if let Some(ip_part) = Self::extract_ip_part(ip_str) {
                if IpAddr::from_str(ip_part).is_err() {
                    return false;
                }
            } else {
                continue;
            }
        }

        true
    }

    /// Extracts the IP part from a string, handling brackets for IPv6.
    pub fn extract_ip_part(ip_str: &str) -> Option<&str> {
        if ip_str.starts_with('[') {
            if let Some(end) = ip_str.find(']') {
                Some(&ip_str[1..end])
            } else {
                None
            }
        } else {
            // For IPv4 or IPv6 without brackets, take the part before the first colon.
            Some(ip_str.split(':').next().unwrap_or(ip_str))
        }
    }

    /// Validates the format of an RFC 7239 Forwarded header value.
    pub fn validate_forwarded_header(header_value: &str) -> bool {
        if header_value.is_empty() {
            return false;
        }

        let parts: Vec<&str> = header_value.split(';').collect();

        if parts.is_empty() {
            return false;
        }

        for part in parts {
            let part = part.trim();
            if !part.contains('=') {
                return false;
            }
        }

        true
    }

    /// Checks if an IP address is within any of the specified CIDR ranges.
    pub fn validate_ip_in_range(ip: &IpAddr, cidr_ranges: &[String]) -> bool {
        for cidr in cidr_ranges {
            if let Ok(network) = ipnetwork::IpNetwork::from_str(cidr)
                && network.contains(*ip)
            {
                return true;
            }
        }

        false
    }

    /// Validates a header value for security (length and control characters).
    pub fn validate_header_value(value: &str) -> bool {
        for c in value.chars() {
            if c.is_control() && c != '\t' {
                return false;
            }
        }

        if value.len() > 8192 {
            return false;
        }

        true
    }

    /// Validates an entire HeaderMap for security.
    pub fn validate_headers(headers: &HeaderMap) -> bool {
        for (name, value) in headers {
            let name_str = name.as_str();
            if name_str.len() > 256 {
                return false;
            }

            if let Ok(value_str) = value.to_str() {
                if !Self::validate_header_value(value_str) {
                    return false;
                }
            } else if value.len() > 8192 {
                return false;
            }
        }

        true
    }

    /// Validates a port number.
    pub fn validate_port(port: u16) -> bool {
        port > 0
    }

    /// Validates a CIDR notation string.
    pub fn validate_cidr(cidr: &str) -> bool {
        ipnetwork::IpNetwork::from_str(cidr).is_ok()
    }

    /// Validates the length of a proxy chain.
    pub fn validate_proxy_chain_length(chain: &[IpAddr], max_length: usize) -> bool {
        chain.len() <= max_length
    }

    /// Validates that a proxy chain does not contain duplicate adjacent IPs.
    pub fn validate_proxy_chain_continuity(chain: &[IpAddr]) -> bool {
        if chain.len() < 2 {
            return true;
        }

        for i in 1..chain.len() {
            if chain[i] == chain[i - 1] {
                return false;
            }
        }

        true
    }

    /// Checks if a string contains only safe characters for use in URLs or headers.
    pub fn is_safe_string(s: &str) -> bool {
        SAFE_REGEX
            .get_or_init(|| Regex::new(r"^[a-zA-Z0-9\-._~:/?#\[\]@!$&'()*+,;=]+$").expect("Invalid safe string regex"))
            .is_match(s)
    }

    /// Validates rate limiting parameters.
    pub fn validate_rate_limit_params(requests: u32, period_seconds: u64) -> bool {
        requests > 0 && requests <= 10000 && period_seconds > 0 && period_seconds <= 86400
    }

    /// Validates cache configuration parameters.
    pub fn validate_cache_params(capacity: usize, ttl_seconds: u64) -> bool {
        capacity > 0 && capacity <= 1000000 && ttl_seconds > 0 && ttl_seconds <= 86400
    }

    /// Redacts sensitive information from a string based on provided patterns.
    pub fn mask_sensitive_data(data: &str, sensitive_patterns: &[&str]) -> String {
        let mut result = data.to_string();

        for pattern in sensitive_patterns {
            match Regex::new(&format!(r#"(?i)({})[:=]\s*([^&\s]+)"#, pattern)) {
                Ok(regex) => {
                    result = regex
                        .replace_all(&result, |caps: &regex::Captures| format!("{}:[REDACTED]", &caps[1]))
                        .to_string();
                }
                Err(e) => {
                    tracing::warn!("Invalid sensitive pattern '{}': {}", pattern, e);
                }
            }
        }

        result
    }
}
