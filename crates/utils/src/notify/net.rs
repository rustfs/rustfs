//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::path::Path;
use std::sync::LazyLock;
use thiserror::Error;
use url::Url;

// Lazy static for the host label regex.
static HOST_LABEL_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$").unwrap());

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

// Host represents a network host with IP/name and port.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Host {
    pub name: String,
    pub port: Option<u16>, // Using Option<u16> to represent if port is set, similar to IsPortSet.
}

// Implementation of Host methods.
impl Host {
    // is_empty returns true if the host name is empty.
    pub fn is_empty(&self) -> bool {
        self.name.is_empty()
    }

    // equal checks if two hosts are equal by comparing their string representations.
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

// parse_host parses a string into a Host, with validation similar to Go's ParseHost.
pub fn parse_host(s: &str) -> Result<Host, NetError> {
    if s.is_empty() {
        return Err(NetError::InvalidArgument);
    }

    // is_valid_host validates the host string, checking for IP or hostname validity.
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

    // Split host and port, similar to net.SplitHostPort.
    let (host_str, port_str) = s.rsplit_once(':').map_or((s, ""), |(h, p)| (h, p));
    let port = if !port_str.is_empty() {
        Some(port_str.parse().map_err(|_| NetError::ParseError(port_str.to_string()))?)
    } else {
        None
    };

    // Trim IPv6 brackets if present.
    let host = trim_ipv6(host_str)?;

    // Handle IPv6 zone identifier.
    let trimmed_host = host.split('%').next().unwrap_or(&host);

    if !is_valid_host(trimmed_host) {
        return Err(NetError::InvalidHost);
    }

    Ok(Host { name: host, port })
}

// trim_ipv6 removes square brackets from IPv6 addresses, similar to Go's trimIPv6.
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

// URL is a wrapper around url::Url for custom handling.
#[derive(Debug, Clone)]
pub struct ParsedURL(pub Url);

impl ParsedURL {
    /// is_empty returns true if the URL is empty or "about:blank".
    pub fn is_empty(&self) -> bool {
        self.0.as_str() == "" || (self.0.scheme() == "about" && self.0.path() == "blank")
    }

    /// hostname returns the hostname of the URL.
    pub fn hostname(&self) -> String {
        self.0.host_str().unwrap_or("").to_string()
    }

    /// port returns the port of the URL as a string, defaulting to "80" for http and "443" for https if not set.
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

    /// scheme returns the scheme of the URL.
    pub fn scheme(&self) -> &str {
        self.0.scheme()
    }

    /// url returns a reference to the underlying Url.
    pub fn url(&self) -> &Url {
        &self.0
    }
}

impl std::fmt::Display for ParsedURL {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut url = self.0.clone();
        if let Some(host) = url.host_str().map(|h| h.to_string()) {
            if let Some(port) = url.port() {
                if (url.scheme() == "http" && port == 80) || (url.scheme() == "https" && port == 443) {
                    url.set_host(Some(&host)).unwrap();
                    url.set_port(None).unwrap();
                }
            }
        }
        let mut s = url.to_string();

        // If the URL ends with a slash and the path is just "/", remove the trailing slash.
        if s.ends_with('/') && url.path() == "/" {
            s.pop();
        }

        write!(f, "{}", s)
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
            Ok(ParsedURL(Url::parse("about:blank").unwrap()))
        } else {
            parse_url(&s).map_err(serde::de::Error::custom)
        }
    }
}

// parse_url parses a string into a ParsedURL, with host validation and path cleaning.
pub fn parse_url(s: &str) -> Result<ParsedURL, NetError> {
    if let Some(scheme_end) = s.find("://") {
        if s[scheme_end + 3..].starts_with('/') {
            let scheme = &s[..scheme_end];
            if !scheme.is_empty() {
                return Err(NetError::SchemeWithEmptyHost);
            }
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
            let host_port = format!("{}:{}", uu.host_str().unwrap(), port_str);
            parse_host(&host_port)?; // Validate host.
        }
    }

    // Clean path: Use Url's path_segments to normalize.
    if !uu.path().is_empty() {
        // Url automatically cleans paths, but we ensure trailing slash if original had it.
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
/// parse_http_url parses a string into a ParsedURL, ensuring the scheme is http or https.
pub fn parse_http_url(s: &str) -> Result<ParsedURL, NetError> {
    let u = parse_url(s)?;
    match u.0.scheme() {
        "http" | "https" => Ok(u),
        _ => Err(NetError::UnexpectedScheme(u.0.scheme().to_string())),
    }
}

#[allow(dead_code)]
/// is_network_or_host_down checks if an error indicates network or host down, considering timeouts.
pub fn is_network_or_host_down(err: &std::io::Error, expect_timeouts: bool) -> bool {
    if err.kind() == std::io::ErrorKind::TimedOut {
        return !expect_timeouts;
    }
    // Simplified checks based on Go logic; adapt for Rust as needed
    let err_str = err.to_string().to_lowercase();
    err_str.contains("connection reset by peer")
        || err_str.contains("connection timed out")
        || err_str.contains("broken pipe")
        || err_str.contains("use of closed network connection")
}

#[allow(dead_code)]
/// is_conn_reset_err checks if an error indicates a connection reset by peer.
pub fn is_conn_reset_err(err: &std::io::Error) -> bool {
    err.to_string().contains("connection reset by peer") || matches!(err.raw_os_error(), Some(libc::ECONNRESET))
}

#[allow(dead_code)]
/// is_conn_refused_err checks if an error indicates a connection refused.
pub fn is_conn_refused_err(err: &std::io::Error) -> bool {
    err.to_string().contains("connection refused") || matches!(err.raw_os_error(), Some(libc::ECONNREFUSED))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_host_with_empty_string_returns_error() {
        let result = parse_host("");
        assert!(matches!(result, Err(NetError::InvalidArgument)));
    }

    #[test]
    fn parse_host_with_valid_ipv4() {
        let result = parse_host("192.168.1.1:8080");
        assert!(result.is_ok());
        let host = result.unwrap();
        assert_eq!(host.name, "192.168.1.1");
        assert_eq!(host.port, Some(8080));
    }

    #[test]
    fn parse_host_with_valid_hostname() {
        let result = parse_host("example.com:443");
        assert!(result.is_ok());
        let host = result.unwrap();
        assert_eq!(host.name, "example.com");
        assert_eq!(host.port, Some(443));
    }

    #[test]
    fn parse_host_with_ipv6_brackets() {
        let result = parse_host("[::1]:8080");
        assert!(result.is_ok());
        let host = result.unwrap();
        assert_eq!(host.name, "::1");
        assert_eq!(host.port, Some(8080));
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
        let host = result.unwrap();
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
    fn host_equal_when_same() {
        let host1 = Host {
            name: "example.com".to_string(),
            port: Some(80),
        };
        let host2 = Host {
            name: "example.com".to_string(),
            port: Some(80),
        };
        assert!(host1.equal(&host2));
    }

    #[test]
    fn host_not_equal_when_different() {
        let host1 = Host {
            name: "example.com".to_string(),
            port: Some(80),
        };
        let host2 = Host {
            name: "example.com".to_string(),
            port: Some(443),
        };
        assert!(!host1.equal(&host2));
    }

    #[test]
    fn parse_url_with_valid_http_url() {
        let result = parse_url("http://example.com/path");
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.hostname(), "example.com");
        assert_eq!(parsed.port(), "80");
    }

    #[test]
    fn parse_url_with_valid_https_url() {
        let result = parse_url("https://example.com:443/path");
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.hostname(), "example.com");
        assert_eq!(parsed.port(), "443");
    }

    #[test]
    fn parse_url_with_scheme_but_empty_host() {
        let result = parse_url("http:///path");
        assert!(matches!(result, Err(NetError::SchemeWithEmptyHost)));
    }

    #[test]
    fn parse_url_with_invalid_host() {
        let result = parse_url("http://invalid..host/path");
        assert!(matches!(result, Err(NetError::InvalidHost)));
    }

    #[test]
    fn parse_url_with_path_cleaning() {
        let result = parse_url("http://example.com//path/../path/");
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.0.path(), "/path/");
    }

    #[test]
    fn parse_http_url_with_http_scheme() {
        let result = parse_http_url("http://example.com");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_http_url_with_https_scheme() {
        let result = parse_http_url("https://example.com");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_http_url_with_invalid_scheme() {
        let result = parse_http_url("ftp://example.com");
        assert!(matches!(result, Err(NetError::UnexpectedScheme(_))));
    }

    #[test]
    fn parsed_url_is_empty_when_url_is_empty() {
        let url = ParsedURL(Url::parse("about:blank").unwrap());
        assert!(url.is_empty());
    }

    #[test]
    fn parsed_url_hostname() {
        let url = ParsedURL(Url::parse("http://example.com:8080").unwrap());
        assert_eq!(url.hostname(), "example.com");
    }

    #[test]
    fn parsed_url_port() {
        let url = ParsedURL(Url::parse("http://example.com:8080").unwrap());
        assert_eq!(url.port(), "8080");
    }

    #[test]
    fn parsed_url_to_string_removes_default_ports() {
        let url = ParsedURL(Url::parse("http://example.com:80").unwrap());
        assert_eq!(url.to_string(), "http://example.com");
    }

    #[test]
    fn is_network_or_host_down_with_timeout() {
        let err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout");
        assert!(is_network_or_host_down(&err, false));
    }

    #[test]
    fn is_network_or_host_down_with_expected_timeout() {
        let err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout");
        assert!(!is_network_or_host_down(&err, true));
    }

    #[test]
    fn is_conn_reset_err_with_reset_message() {
        let err = std::io::Error::other("connection reset by peer");
        assert!(is_conn_reset_err(&err));
    }

    #[test]
    fn is_conn_refused_err_with_refused_message() {
        let err = std::io::Error::other("connection refused");
        assert!(is_conn_refused_err(&err));
    }
}
