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

use http::HeaderMap;
use regex::Regex;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::LazyLock;

/// De-facto standard header keys.
const X_FORWARDED_FOR: &str = "x-forwarded-for";
const X_FORWARDED_PROTO: &str = "x-forwarded-proto";
const X_FORWARDED_SCHEME: &str = "x-forwarded-scheme";
const X_REAL_IP: &str = "x-real-ip";

/// RFC7239 defines a new "Forwarded: " header designed to replace the
/// existing use of X-Forwarded-* headers.
/// e.g. Forwarded: for=192.0.2.60;proto=https;by=203.0.113.43
const FORWARDED: &str = "forwarded";

static FOR_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)(?:for=)([^(;|,| )]+)(.*)").unwrap());
static PROTO_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^(;|,| )+(?:proto=)(https|http)").unwrap());

/// Used to disable all processing of the X-Forwarded-For header in source IP discovery.
///
/// # Returns
/// A `bool` indicating whether the X-Forwarded-For header is enabled
///
fn is_xff_header_enabled() -> bool {
    env::var("_RUSTFS_API_XFF_HEADER")
        .unwrap_or_else(|_| "on".to_string())
        .to_lowercase()
        == "on"
}

/// GetSourceScheme retrieves the scheme from the X-Forwarded-Proto and RFC7239
/// Forwarded headers (in that order).
///
/// # Arguments
/// * `headers` - HTTP headers from the request
///
/// # Returns
/// An `Option<String>` containing the source scheme if found
///
pub fn get_source_scheme(headers: &HeaderMap) -> Option<String> {
    // Retrieve the scheme from X-Forwarded-Proto.
    if let Some(proto) = headers.get(X_FORWARDED_PROTO) {
        if let Ok(proto_str) = proto.to_str() {
            return Some(proto_str.to_lowercase());
        }
    }

    if let Some(proto) = headers.get(X_FORWARDED_SCHEME) {
        if let Ok(proto_str) = proto.to_str() {
            return Some(proto_str.to_lowercase());
        }
    }

    if let Some(forwarded) = headers.get(FORWARDED) {
        if let Ok(forwarded_str) = forwarded.to_str() {
            // match should contain at least two elements if the protocol was
            // specified in the Forwarded header. The first element will always be
            // the 'for=', which we ignore, subsequently we proceed to look for
            // 'proto=' which should precede right after `for=` if not
            // we simply ignore the values and return empty. This is in line
            // with the approach we took for returning first ip from multiple
            // params.
            if let Some(for_match) = FOR_REGEX.captures(forwarded_str) {
                if for_match.len() > 1 {
                    let remaining = &for_match[2];
                    if let Some(proto_match) = PROTO_REGEX.captures(remaining) {
                        if proto_match.len() > 1 {
                            return Some(proto_match[2].to_lowercase());
                        }
                    }
                }
            }
        }
    }

    None
}

/// GetSourceIPFromHeaders retrieves the IP from the X-Forwarded-For, X-Real-IP
/// and RFC7239 Forwarded headers (in that order)
///
/// # Arguments
/// * `headers` - HTTP headers from the request
///
/// # Returns
/// An `Option<String>` containing the source IP address if found
///
pub fn get_source_ip_from_headers(headers: &HeaderMap) -> Option<String> {
    let mut addr = None;

    if is_xff_header_enabled() {
        if let Some(forwarded_for) = headers.get(X_FORWARDED_FOR) {
            if let Ok(forwarded_str) = forwarded_for.to_str() {
                // Only grab the first (client) address. Note that '192.168.0.1,
                // 10.1.1.1' is a valid key for X-Forwarded-For where addresses after
                // the first may represent forwarding proxies earlier in the chain.
                let first_comma = forwarded_str.find(", ");
                let end = first_comma.unwrap_or(forwarded_str.len());
                addr = Some(forwarded_str[..end].to_string());
            }
        }
    }

    if addr.is_none() {
        if let Some(real_ip) = headers.get(X_REAL_IP) {
            if let Ok(real_ip_str) = real_ip.to_str() {
                // X-Real-IP should only contain one IP address (the client making the
                // request).
                addr = Some(real_ip_str.to_string());
            }
        } else if let Some(forwarded) = headers.get(FORWARDED) {
            if let Ok(forwarded_str) = forwarded.to_str() {
                // match should contain at least two elements if the protocol was
                // specified in the Forwarded header. The first element will always be
                // the 'for=' capture, which we ignore. In the case of multiple IP
                // addresses (for=8.8.8.8, 8.8.4.4, 172.16.1.20 is valid) we only
                // extract the first, which should be the client IP.
                if let Some(for_match) = FOR_REGEX.captures(forwarded_str) {
                    if for_match.len() > 1 {
                        // IPv6 addresses in Forwarded headers are quoted-strings. We strip
                        // these quotes.
                        let ip = for_match[1].trim_matches('"');
                        addr = Some(ip.to_string());
                    }
                }
            }
        }
    }

    addr
}

/// GetSourceIPRaw retrieves the IP from the request headers
/// and falls back to remote_addr when necessary.
/// however returns without bracketing.
///
/// # Arguments
/// * `headers` - HTTP headers from the request
/// * `remote_addr` - Remote address as a string
///
/// # Returns
/// A `String` containing the source IP address
///
pub fn get_source_ip_raw(headers: &HeaderMap, remote_addr: &str) -> String {
    let addr = get_source_ip_from_headers(headers).unwrap_or_else(|| remote_addr.to_string());

    // Default to remote address if headers not set.
    if let Ok(socket_addr) = SocketAddr::from_str(&addr) {
        socket_addr.ip().to_string()
    } else {
        addr
    }
}

/// GetSourceIP retrieves the IP from the request headers
/// and falls back to remote_addr when necessary.
/// It brackets IPv6 addresses.
///
/// # Arguments
/// * `headers` - HTTP headers from the request
/// * `remote_addr` - Remote address as a string
///
/// # Returns
/// A `String` containing the source IP address, with IPv6 addresses bracketed
///
pub fn get_source_ip(headers: &HeaderMap, remote_addr: &str) -> String {
    let addr = get_source_ip_raw(headers, remote_addr);
    if addr.contains(':') { format!("[{addr}]") } else { addr }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn create_test_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static("192.168.1.1"));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        headers
    }

    #[test]
    fn test_get_source_scheme() {
        let headers = create_test_headers();
        assert_eq!(get_source_scheme(&headers), Some("https".to_string()));
    }

    #[test]
    fn test_get_source_ip_from_headers() {
        let headers = create_test_headers();
        assert_eq!(get_source_ip_from_headers(&headers), Some("192.168.1.1".to_string()));
    }

    #[test]
    fn test_get_source_ip_raw() {
        let headers = create_test_headers();
        let remote_addr = "127.0.0.1:8080";
        let result = get_source_ip_raw(&headers, remote_addr);
        assert_eq!(result, "192.168.1.1");
    }

    #[test]
    fn test_get_source_ip() {
        let headers = create_test_headers();
        let remote_addr = "127.0.0.1:8080";
        let result = get_source_ip(&headers, remote_addr);
        assert_eq!(result, "192.168.1.1");
    }

    #[test]
    fn test_get_source_ip_ipv6() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static("2001:db8::1"));
        let remote_addr = "127.0.0.1:8080";
        let result = get_source_ip(&headers, remote_addr);
        assert_eq!(result, "[2001:db8::1]");
    }
}
