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

use hashbrown::HashMap;
use hyper::HeaderMap;
use regex::Regex;
use s3s::{S3Request, S3Response};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::path::Path;
use std::sync::LazyLock;
use thiserror::Error;
use url::Url;

static HOST_LABEL_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$").expect("operation should succeed"));

/// NetError represents errors that can occur in network operations.
#[derive(Error, Debug)]
pub enum NetError {
    #[error("invalid argument")]
    InvalidArgument,
    #[error("invalid hostname")]
    InvalidHost,
    #[error("missing '[' in host")]
    MissingBracket,
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("unexpected scheme: {0}")]
    UnexpectedScheme(String),
    #[error("scheme appears with empty host")]
    SchemeWithEmptyHost,
}

/// Host represents a network host with IP/name and port.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Host {
    pub name: String,
    pub port: Option<u16>,
}

impl Host {
    pub fn is_empty(&self) -> bool {
        self.name.is_empty()
    }

    pub fn equal(&self, other: &Host) -> bool {
        self.to_string() == other.to_string()
    }
}

impl std::fmt::Display for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.port {
            Some(p) => write!(f, "{}:{}", self.name, p),
            None => write!(f, "{}", self.name),
        }
    }
}

/// Extract request parameters from S3Request, mainly header information.
pub fn extract_req_params<T>(req: &S3Request<T>) -> HashMap<String, String> {
    extract_params_header(&req.headers)
}

/// Extract request parameters from hyper::HeaderMap, mainly header information.
/// This function is useful when you have a raw HTTP request and need to extract parameters.
#[deprecated(since = "0.1.0", note = "Use extract_params_header instead")]
pub fn extract_req_params_header(head: &HeaderMap) -> HashMap<String, String> {
    extract_params_header(head)
}

/// Extract parameters from hyper::HeaderMap, mainly header information.
pub fn extract_params_header(head: &HeaderMap) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for (key, value) in head.iter() {
        if let Ok(val_str) = value.to_str() {
            params.insert(key.as_str().to_string(), val_str.to_string());
        }
    }
    params
}

/// Extract response elements from S3Response, mainly header information.
pub fn extract_resp_elements<T>(resp: &S3Response<T>) -> HashMap<String, String> {
    extract_params_header(&resp.headers)
}

/// Get host from header information.
pub fn get_request_host(headers: &HeaderMap) -> String {
    headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// Get Port from header information.
/// Priority:
/// 1. x-forwarded-port
/// 2. host header (parse port)
/// 3. x-forwarded-proto inferred default (http=80, https=443) when host has no explicit port
/// 4. port header
pub fn get_request_port(headers: &HeaderMap) -> u16 {
    if let Some(port) = headers
        .get("x-forwarded-port")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u16>().ok())
    {
        return port;
    }

    if let Some(host) = headers.get("host").and_then(|v| v.to_str().ok()) {
        if let Some(idx) = host.rfind(':') {
            let valid_colon = match host.rfind(']') {
                Some(close_bracket_idx) => idx > close_bracket_idx,
                None => true,
            };

            if valid_colon
                && let Ok(port) = host[idx + 1..].parse::<u16>()
                && port > 0
            {
                return port;
            }
        }

        if let Some(proto) = headers.get("x-forwarded-proto").and_then(|v| v.to_str().ok()) {
            match proto {
                "http" => return 80,
                "https" => return 443,
                _ => {}
            }
        }
    }

    headers
        .get("port")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0)
}

/// Get content-length from header information.
pub fn get_request_content_length(headers: &HeaderMap) -> u64 {
    headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Get referer from header information.
pub fn get_request_referer(headers: &HeaderMap) -> String {
    headers
        .get("referer")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// Get user-agent from header information.
pub fn get_request_user_agent(headers: &HeaderMap) -> String {
    headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// parse_host parses a string into a Host, with validation similar to Go's ParseHost.
pub fn parse_host(s: &str) -> Result<Host, NetError> {
    if s.is_empty() {
        return Err(NetError::InvalidArgument);
    }

    let is_valid_host = |host: &str| -> bool {
        if host.is_empty() {
            return true;
        }
        if host.parse::<IpAddr>().is_ok() {
            return true;
        }
        if !(1..=253).contains(&host.len()) {
            return false;
        }
        for (i, label) in host.split('.').enumerate() {
            if i + 1 == host.split('.').count() && label.is_empty() {
                continue;
            }
            if !(1..=63).contains(&label.len()) || !HOST_LABEL_REGEX.is_match(label) {
                return false;
            }
        }
        true
    };

    let (host, port) = if let Some(rest) = s.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return Err(NetError::MissingBracket);
        };
        let host = rest[..end].to_string();
        let port_str = &rest[end + 1..];
        let port = if let Some(port_str) = port_str.strip_prefix(':') {
            if port_str.is_empty() {
                None
            } else {
                Some(port_str.parse().map_err(|_| NetError::ParseError(port_str.to_string()))?)
            }
        } else if port_str.is_empty() {
            None
        } else {
            return Err(NetError::InvalidHost);
        };

        (host, port)
    } else {
        if s.contains(']') {
            return Err(NetError::MissingBracket);
        }

        let (host_str, port_str) = if s.matches(':').count() > 1 {
            (s, "")
        } else {
            s.rsplit_once(':').map_or((s, ""), |(h, p)| (h, p))
        };
        let port = if !port_str.is_empty() {
            Some(port_str.parse().map_err(|_| NetError::ParseError(port_str.to_string()))?)
        } else {
            None
        };

        (trim_ipv6(host_str)?, port)
    };

    let trimmed_host = host.split('%').next().unwrap_or(&host);

    if !is_valid_host(trimmed_host) {
        return Err(NetError::InvalidHost);
    }

    Ok(Host { name: host, port })
}

fn trim_ipv6(host: &str) -> Result<String, NetError> {
    if host.ends_with(']') {
        if !host.starts_with('[') {
            return Err(NetError::MissingBracket);
        }
        Ok(host[1..host.len() - 1].to_string())
    } else {
        Ok(host.to_string())
    }
}

/// URL is a wrapper around url::Url for custom handling.
#[derive(Debug, Clone)]
pub struct ParsedURL(pub Url);

impl ParsedURL {
    pub fn is_empty(&self) -> bool {
        self.0.as_str() == "" || (self.0.scheme() == "about" && self.0.path() == "blank")
    }

    pub fn hostname(&self) -> String {
        self.0.host_str().unwrap_or("").to_string()
    }

    pub fn port(&self) -> String {
        match self.0.port() {
            Some(p) => p.to_string(),
            None => match self.0.scheme() {
                "http" => "80".to_string(),
                "https" => "443".to_string(),
                _ => "".to_string(),
            },
        }
    }

    pub fn scheme(&self) -> &str {
        self.0.scheme()
    }

    pub fn url(&self) -> &Url {
        &self.0
    }
}

impl std::fmt::Display for ParsedURL {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut url = self.0.clone();
        if let Some(host) = url.host_str().map(|h| h.to_string())
            && let Some(port) = url.port()
            && ((url.scheme() == "http" && port == 80) || (url.scheme() == "https" && port == 443))
        {
            let _ = url.set_host(Some(&host));
            let _ = url.set_port(None);
        }
        let mut s = url.to_string();

        if s.ends_with('/') && url.path() == "/" {
            s.pop();
        }

        write!(f, "{s}")
    }
}

impl serde::Serialize for ParsedURL {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for ParsedURL {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: String = serde::Deserialize::deserialize(deserializer)?;
        if s.is_empty() {
            Ok(ParsedURL(Url::parse("about:blank").expect("operation should succeed")))
        } else {
            parse_url(&s).map_err(serde::de::Error::custom)
        }
    }
}

/// parse_url parses a string into a ParsedURL, with host validation and path cleaning.
pub fn parse_url(s: &str) -> Result<ParsedURL, NetError> {
    if let Some(scheme_end) = s.find("://")
        && s[scheme_end + 3..].starts_with('/')
    {
        let scheme = &s[..scheme_end];
        if !scheme.is_empty() {
            return Err(NetError::SchemeWithEmptyHost);
        }
    }

    let mut uu = Url::parse(s).map_err(|e| NetError::ParseError(e.to_string()))?;
    if uu.host_str().is_none_or(|h| h.is_empty()) {
        if uu.scheme() != "" {
            return Err(NetError::SchemeWithEmptyHost);
        }
    } else {
        let port_str = uu.port().map(|p| p.to_string()).unwrap_or_else(|| match uu.scheme() {
            "http" => "80".to_string(),
            "https" => "443".to_string(),
            _ => "".to_string(),
        });

        if !port_str.is_empty() {
            let host_port = format!("{}:{}", uu.host_str().expect("operation should succeed"), port_str);
            parse_host(&host_port)?;
        }
    }

    if !uu.path().is_empty() {
        let mut cleaned_path = String::new();
        for comp in Path::new(uu.path()).components() {
            use std::path::Component;
            match comp {
                Component::RootDir => cleaned_path.push('/'),
                Component::Normal(s) => {
                    if !cleaned_path.ends_with('/') {
                        cleaned_path.push('/');
                    }
                    cleaned_path.push_str(&s.to_string_lossy());
                }
                _ => {}
            }
        }
        if s.ends_with('/') && !cleaned_path.ends_with('/') {
            cleaned_path.push('/');
        }
        if cleaned_path.is_empty() {
            cleaned_path.push('/');
        }
        uu.set_path(&cleaned_path);
    }

    Ok(ParsedURL(uu))
}

#[allow(dead_code)]
pub fn parse_http_url(s: &str) -> Result<ParsedURL, NetError> {
    let u = parse_url(s)?;
    match u.0.scheme() {
        "http" | "https" => Ok(u),
        _ => Err(NetError::UnexpectedScheme(u.0.scheme().to_string())),
    }
}

#[allow(dead_code)]
pub fn is_network_or_host_down(err: &std::io::Error, expect_timeouts: bool) -> bool {
    if err.kind() == std::io::ErrorKind::TimedOut {
        return !expect_timeouts;
    }
    let err_str = err.to_string().to_lowercase();
    err_str.contains("connection reset by peer")
        || err_str.contains("connection timed out")
        || err_str.contains("broken pipe")
        || err_str.contains("use of closed network connection")
}

#[allow(dead_code)]
pub fn is_conn_reset_err(err: &std::io::Error) -> bool {
    err.to_string().contains("connection reset by peer") || matches!(err.raw_os_error(), Some(libc::ECONNRESET))
}

#[allow(dead_code)]
pub fn is_conn_refused_err(err: &std::io::Error) -> bool {
    err.to_string().contains("connection refused") || matches!(err.raw_os_error(), Some(libc::ECONNREFUSED))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;

    #[test]
    fn test_get_request_port() {
        let mut headers = HeaderMap::new();

        assert_eq!(get_request_port(&headers), 0);

        headers.insert("port", HeaderValue::from_static("8080"));
        assert_eq!(get_request_port(&headers), 8080);

        headers.remove("port");
        headers.insert("host", HeaderValue::from_static("example.com:9000"));
        assert_eq!(get_request_port(&headers), 9000);

        headers.insert("host", HeaderValue::from_static("example.com"));
        assert_eq!(get_request_port(&headers), 0);

        headers.insert("host", HeaderValue::from_static("[::1]:9001"));
        assert_eq!(get_request_port(&headers), 9001);

        headers.insert("host", HeaderValue::from_static("[::1]"));
        assert_eq!(get_request_port(&headers), 0);

        headers.insert("x-forwarded-port", HeaderValue::from_static("7000"));
        assert_eq!(get_request_port(&headers), 7000);

        headers.remove("x-forwarded-port");
        headers.insert("host", HeaderValue::from_static("example.com"));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("http"));
        assert_eq!(get_request_port(&headers), 80);

        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        assert_eq!(get_request_port(&headers), 443);

        headers.insert("x-forwarded-proto", HeaderValue::from_static("ftp"));
        assert_eq!(get_request_port(&headers), 0);

        headers.insert("host", HeaderValue::from_static("example.com:0"));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        assert_eq!(get_request_port(&headers), 443);

        headers.remove("x-forwarded-proto");
        assert_eq!(get_request_port(&headers), 0);
    }

    #[test]
    fn parse_host_with_empty_string_returns_error() {
        let result = parse_host("");
        assert!(matches!(result, Err(NetError::InvalidArgument)));
    }

    #[test]
    fn parse_host_with_valid_ipv4() {
        let result = parse_host("192.168.1.1:8080");
        assert!(result.is_ok());
        let host = result.expect("operation should succeed");
        assert_eq!(host.name, "192.168.1.1");
        assert_eq!(host.port, Some(8080));
    }

    #[test]
    fn parse_host_with_valid_hostname() {
        let result = parse_host("example.com:443");
        assert!(result.is_ok());
        let host = result.expect("operation should succeed");
        assert_eq!(host.name, "example.com");
        assert_eq!(host.port, Some(443));
    }

    #[test]
    fn parse_host_with_ipv6_brackets() {
        let result = parse_host("[::1]:8080");
        assert!(result.is_ok());
        let host = result.expect("operation should succeed");
        assert_eq!(host.name, "::1");
        assert_eq!(host.port, Some(8080));
    }

    #[test]
    fn parse_host_with_bare_ipv6_without_port() {
        let result = parse_host("::1");
        assert!(result.is_ok());
        let host = result.expect("operation should succeed");
        assert_eq!(host.name, "::1");
        assert_eq!(host.port, None);
    }

    #[test]
    fn parse_host_with_ipv6_zone_without_port() {
        let result = parse_host("fe80::1%eth0");
        assert!(result.is_ok());
        let host = result.expect("operation should succeed");
        assert_eq!(host.name, "fe80::1%eth0");
        assert_eq!(host.port, None);
    }

    #[test]
    fn parse_host_with_bracketed_ipv6_zone_and_port() {
        let result = parse_host("[fe80::1%eth0]:9000");
        assert!(result.is_ok());
        let host = result.expect("operation should succeed");
        assert_eq!(host.name, "fe80::1%eth0");
        assert_eq!(host.port, Some(9000));
    }

    #[test]
    fn parse_host_with_bracketed_ipv6_without_port() {
        let result = parse_host("[::1]");
        assert!(result.is_ok());
        let host = result.expect("operation should succeed");
        assert_eq!(host.name, "::1");
        assert_eq!(host.port, None);
    }

    #[test]
    fn parse_host_with_invalid_ipv6_missing_bracket() {
        let result = parse_host("::1]:8080");
        assert!(matches!(result, Err(NetError::MissingBracket)));
    }

    #[test]
    fn parse_host_with_invalid_hostname() {
        let result = parse_host("invalid..host:80");
        assert!(matches!(result, Err(NetError::InvalidHost)));
    }

    #[test]
    fn parse_host_without_port() {
        let result = parse_host("example.com");
        assert!(result.is_ok());
        let host = result.expect("operation should succeed");
        assert_eq!(host.name, "example.com");
        assert_eq!(host.port, None);
    }

    #[test]
    fn host_is_empty_when_name_is_empty() {
        let host = Host {
            name: "".to_string(),
            port: None,
        };
        assert!(host.is_empty());
    }

    #[test]
    fn host_is_not_empty_when_name_present() {
        let host = Host {
            name: "example.com".to_string(),
            port: Some(80),
        };
        assert!(!host.is_empty());
    }

    #[test]
    fn host_to_string_with_port() {
        let host = Host {
            name: "example.com".to_string(),
            port: Some(80),
        };
        assert_eq!(host.to_string(), "example.com:80");
    }

    #[test]
    fn host_to_string_without_port() {
        let host = Host {
            name: "example.com".to_string(),
            port: None,
        };
        assert_eq!(host.to_string(), "example.com");
    }

    #[test]
    fn parse_url_with_valid_http_url() {
        let result = parse_url("http://example.com/path");
        assert!(result.is_ok());
        let parsed = result.expect("operation should succeed");
        assert_eq!(parsed.hostname(), "example.com");
        assert_eq!(parsed.port(), "80");
        assert_eq!(parsed.scheme(), "http");
        assert_eq!(parsed.to_string(), "http://example.com/path");
    }

    #[test]
    fn parse_url_with_explicit_default_https_port() {
        let result = parse_url("https://example.com:443/path");
        assert!(result.is_ok());
        let parsed = result.expect("operation should succeed");
        assert_eq!(parsed.to_string(), "https://example.com/path");
    }

    #[test]
    fn parse_url_with_empty_host_returns_error() {
        let result = parse_url("http:///path");
        assert!(matches!(result, Err(NetError::SchemeWithEmptyHost)));
    }

    #[test]
    fn parse_url_with_invalid_host_returns_error() {
        let result = parse_url("http://invalid..host/path");
        assert!(matches!(result, Err(NetError::InvalidHost)));
    }

    #[test]
    fn parse_url_normalizes_path() {
        let result = parse_url("http://example.com//path/../path/");
        assert!(result.is_ok());
        let parsed = result.expect("operation should succeed");
        assert_eq!(parsed.to_string(), "http://example.com/path/");
    }
}
