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

//! Proxy validator for verifying proxy chains and extracting client information.

use axum::http::HeaderMap;
use std::net::{IpAddr, SocketAddr};
use std::time::Instant;
use tracing::{debug, warn};

use crate::{ProxyChainAnalyzer, ProxyError, ProxyMetrics, TrustedProxyConfig, ValidationMode};

/// Information about the client extracted from the request and proxy headers.
#[derive(Debug, Clone)]
pub struct ClientInfo {
    /// The verified real IP address of the client.
    pub real_ip: IpAddr,
    /// The original host requested by the client (if provided by a trusted proxy).
    pub forwarded_host: Option<String>,
    /// The original protocol (http/https) used by the client (if provided by a trusted proxy).
    pub forwarded_proto: Option<String>,
    /// Whether the request was received from a trusted proxy.
    pub is_from_trusted_proxy: bool,
    /// The IP address of the proxy that directly connected to this server.
    pub proxy_ip: Option<IpAddr>,
    /// The number of proxy hops identified in the chain.
    pub proxy_hops: usize,
    /// The validation mode used for this request.
    pub validation_mode: ValidationMode,
    /// Any warnings generated during the validation process.
    pub warnings: Vec<String>,
}

impl ClientInfo {
    /// Creates a `ClientInfo` for a direct connection without any proxies.
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

    /// Creates a `ClientInfo` for a request received through a trusted proxy.
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

    /// Returns a string representation of the client info for logging.
    pub fn to_log_string(&self) -> String {
        format!(
            "client_ip={}, proxy={:?}, hops={}, trusted={}, mode={:?}",
            self.real_ip, self.proxy_ip, self.proxy_hops, self.is_from_trusted_proxy, self.validation_mode
        )
    }
}

/// Core validator that processes incoming requests to verify proxy chains.
#[derive(Debug, Clone)]
pub struct ProxyValidator {
    /// Configuration for trusted proxies.
    config: TrustedProxyConfig,
    /// Analyzer for verifying the integrity of the proxy chain.
    chain_analyzer: ProxyChainAnalyzer,
    /// Metrics collector for observability.
    metrics: Option<ProxyMetrics>,
}

impl ProxyValidator {
    /// Creates a new `ProxyValidator` with the given configuration and metrics.
    pub fn new(config: TrustedProxyConfig, metrics: Option<ProxyMetrics>) -> Self {
        let chain_analyzer = ProxyChainAnalyzer::new(config.clone());

        Self {
            config,
            chain_analyzer,
            metrics,
        }
    }

    /// Validates an incoming request and extracts client information.
    pub fn validate_request(&self, peer_addr: Option<SocketAddr>, headers: &HeaderMap) -> Result<ClientInfo, ProxyError> {
        let start_time = Instant::now();

        // Record the start of the validation attempt.
        self.record_metric_start();

        // Perform the internal validation logic.
        let result = self.validate_request_internal(peer_addr, headers);

        // Record the result and duration.
        let duration = start_time.elapsed();
        self.record_metric_result(&result, duration);

        result
    }

    /// Internal logic for request validation.
    fn validate_request_internal(&self, peer_addr: Option<SocketAddr>, headers: &HeaderMap) -> Result<ClientInfo, ProxyError> {
        // Fallback to unspecified address if peer address is missing.
        let peer_addr = peer_addr.unwrap_or_else(|| SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 0));

        // Check if the direct peer is a trusted proxy.
        if self.config.is_trusted(&peer_addr) {
            debug!("Request received from trusted proxy: {}", peer_addr.ip());

            // Parse and validate headers from the trusted proxy.
            self.validate_trusted_proxy_request(&peer_addr, headers)
        } else {
            // Log a warning if the request is from a private network but not trusted.
            if self.config.is_private_network(&peer_addr.ip()) {
                warn!(
                    "Request from private network but not trusted: {}. This might indicate a configuration issue.",
                    peer_addr.ip()
                );
            }

            // Treat as a direct connection if the peer is not trusted.
            Ok(ClientInfo::direct(peer_addr))
        }
    }

    /// Validates a request that originated from a trusted proxy.
    fn validate_trusted_proxy_request(&self, proxy_addr: &SocketAddr, headers: &HeaderMap) -> Result<ClientInfo, ProxyError> {
        let proxy_ip = proxy_addr.ip();

        // Prefer RFC 7239 "Forwarded" header if enabled, otherwise fallback to legacy headers.
        let client_info = if self.config.enable_rfc7239 {
            self.try_parse_rfc7239_headers(headers, proxy_ip)
                .unwrap_or_else(|| self.parse_legacy_headers(headers))
        } else {
            self.parse_legacy_headers(headers)
        };

        // Analyze the integrity and continuity of the proxy chain.
        let chain_analysis = self
            .chain_analyzer
            .analyze_chain(&client_info.proxy_chain, proxy_ip, headers)?;

        // Enforce maximum hop limit.
        if chain_analysis.hops > self.config.max_hops {
            return Err(ProxyError::ChainTooLong(chain_analysis.hops, self.config.max_hops));
        }

        // Enforce chain continuity if enabled.
        if self.config.enable_chain_continuity_check && !chain_analysis.is_continuous {
            return Err(ProxyError::ChainNotContinuous);
        }

        Ok(ClientInfo::from_trusted_proxy(
            chain_analysis.client_ip,
            client_info.forwarded_host,
            client_info.forwarded_proto,
            proxy_ip,
            chain_analysis.hops,
            self.config.validation_mode,
            chain_analysis.warnings,
        ))
    }

    /// Attempts to parse the RFC 7239 "Forwarded" header.
    fn try_parse_rfc7239_headers(&self, headers: &HeaderMap, proxy_ip: IpAddr) -> Option<ParsedHeaders> {
        headers
            .get("forwarded")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| Self::parse_forwarded_header(s, proxy_ip))
    }

    /// Parses legacy proxy headers (X-Forwarded-For, X-Forwarded-Host, X-Forwarded-Proto).
    fn parse_legacy_headers(&self, headers: &HeaderMap) -> ParsedHeaders {
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
            .map(Self::parse_x_forwarded_for)
            .unwrap_or_default();

        ParsedHeaders {
            proxy_chain,
            forwarded_host,
            forwarded_proto,
        }
    }

    /// Parses the RFC 7239 "Forwarded" header value.
    fn parse_forwarded_header(header_value: &str, proxy_ip: IpAddr) -> Option<ParsedHeaders> {
        // Simplified implementation: processes only the first entry in the header.
        let first_part = header_value.split(',').next()?.trim();

        let mut proxy_chain = Vec::new();
        let mut forwarded_host = None;
        let mut forwarded_proto = None;

        for part in first_part.split(';') {
            let part = part.trim();
            if let Some((key, value)) = part.split_once('=') {
                let key = key.trim().to_lowercase();
                let value = value.trim().trim_matches('"');

                match key.as_str() {
                    "for" => {
                        // Extract IP address, handling IPv6 addresses in brackets as per RFC 7239.
                        let ip_str = if value.starts_with('[') {
                            if let Some(end) = value.find(']') {
                                &value[1..end]
                            } else {
                                continue; // Invalid format, skip
                            }
                        } else {
                            // For IPv4 or IPv6 without brackets, take the part before the first colon.
                            value.split(':').next().unwrap_or(value)
                        };

                        if let Ok(ip) = ip_str.parse::<IpAddr>() {
                            proxy_chain.push(ip);
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

        // Fallback to the proxy IP if no client IP was found in the header.
        if proxy_chain.is_empty() {
            proxy_chain.push(proxy_ip);
        }

        Some(ParsedHeaders {
            proxy_chain,
            forwarded_host,
            forwarded_proto,
        })
    }

    /// Parses the X-Forwarded-For header into a list of IP addresses.
    pub fn parse_x_forwarded_for(header_value: &str) -> Vec<IpAddr> {
        header_value
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .filter_map(|s| {
                // Handle IPv6 addresses in brackets, e.g., [::1]:8080
                let ip_str = if s.starts_with('[') {
                    if let Some(end) = s.find(']') {
                        &s[1..end]
                    } else {
                        s // Invalid format, try parsing as is
                    }
                } else {
                    // For IPv4 or IPv6 without brackets, take the part before the first colon.
                    s.split(':').next().unwrap_or(s)
                };
                ip_str.parse::<IpAddr>().ok()
            })
            .collect()
    }

    /// Records the start of a validation attempt in metrics.
    fn record_metric_start(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.increment_validation_attempts();
        }
    }

    /// Records the result of a validation attempt in metrics.
    fn record_metric_result(&self, result: &Result<ClientInfo, ProxyError>, duration: std::time::Duration) {
        if let Some(metrics) = &self.metrics {
            match result {
                Ok(client_info) => {
                    metrics.record_validation_success(client_info.is_from_trusted_proxy, client_info.proxy_hops, duration);
                }
                Err(err) => {
                    metrics.record_validation_failure(err, duration);
                }
            }
        }
    }
}

/// Internal structure for holding parsed header information.
#[derive(Debug, Clone)]
struct ParsedHeaders {
    /// The chain of proxy IPs (client IP is typically the first).
    proxy_chain: Vec<IpAddr>,
    /// The original host requested.
    forwarded_host: Option<String>,
    /// The original protocol used.
    forwarded_proto: Option<String>,
}
