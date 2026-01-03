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

use crate::{ClientInfo, MultiProxyConfig, MultiProxyProcessor, TrustedProxiesConfig};
use axum::extract::Request;
use axum::response::Response;
use std::net::{IpAddr, SocketAddr};
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::{info_span, instrument};

/// Enhanced client information structure
#[derive(Debug, Clone)]
pub struct EnhancedClientInfo {
    pub real_ip: IpAddr,
    pub forwarded_host: Option<String>,
    pub forwarded_proto: Option<String>,
    pub is_from_trusted_proxy: bool,
    pub proxy_ip: Option<IpAddr>,
    pub proxy_chain_analysis: Option<crate::ProxyChainAnalysis>,
}

/// Trusted agent middleware layer
#[derive(Clone)]
pub struct TrustedProxiesLayer {
    config: TrustedProxiesConfig,
}

impl TrustedProxiesLayer {
    pub fn new(config: TrustedProxiesConfig) -> Self {
        Self { config }
    }
}

impl<S> Layer<S> for TrustedProxiesLayer {
    type Service = TrustedProxiesMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TrustedProxiesMiddleware {
            inner,
            config: self.config.clone(),
        }
    }
}

/// Trusted agent middleware service
#[derive(Clone)]
pub struct TrustedProxiesMiddleware<S> {
    inner: S,
    config: TrustedProxiesConfig,
}

impl<S> Service<Request> for TrustedProxiesMiddleware<S>
where
    S: Service<Request, Response = Response> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    /// Updated the call method of TrustedProxiesMiddleware
    #[instrument(name = "trusted_proxy_middleware", skip_all, fields(peer_addr, trusted))]
    fn call(&mut self, mut req: Request) -> Self::Future {
        let span = info_span!(
            "trusted_proxy_check",
            peer_addr = ?req.extensions().get::<SocketAddr>().map(|a| a.to_string())
        );

        let _guard = span.enter();

        // Use advanced processors
        let processor = MultiProxyProcessor::new(self.config.clone(), Some(MultiProxyConfig::default()));

        let peer_addr = req.extensions().get::<SocketAddr>().copied();
        let headers = req.headers();

        match peer_addr {
            Some(addr) => {
                match processor.process_request(&addr, headers) {
                    Ok(analysis) => {
                        tracing::debug!(
                            "Proxy chain analysis completed: client_ip={}, hops={}, integrity={}",
                            analysis.trusted_client_ip,
                            analysis.validated_hops,
                            analysis.chain_integrity
                        );

                        // 创建增强的客户端信息
                        let client_info = EnhancedClientInfo {
                            real_ip: analysis.trusted_client_ip,
                            forwarded_host: headers
                                .get("x-forwarded-host")
                                .and_then(|h| h.to_str().ok())
                                .map(String::from),
                            forwarded_proto: headers
                                .get("x-forwarded-proto")
                                .and_then(|h| h.to_str().ok())
                                .map(String::from),
                            is_from_trusted_proxy: analysis.validated_hops > 0,
                            proxy_ip: Some(addr.ip()),
                            proxy_chain_analysis: Some(analysis),
                        };

                        req.extensions_mut().insert(client_info.clone());

                        // Record safety warnings
                        if !client_info
                            .proxy_chain_analysis
                            .as_ref()
                            .unwrap()
                            .security_warnings
                            .is_empty()
                        {
                            tracing::warn!(
                                "Proxy Chain Security Warning: {:?}",
                                client_info.proxy_chain_analysis.as_ref().unwrap().security_warnings
                            );
                        }
                    }
                    Err(err) => {
                        tracing::warn!("Proxy chain validation failed: {}", err);

                        // Fallback to basic processing when validation fails
                        let client_info = if self.config.is_trusted(&addr) {
                            extract_client_info_from_headers(&req)
                        } else {
                            ClientInfo::direct(addr)
                        };

                        req.extensions_mut().insert(client_info);
                    }
                }
            }
            None => {
                tracing::warn!("The peer address cannot be obtained");
                let client_info = ClientInfo {
                    real_ip: std::net::Ipv4Addr::UNSPECIFIED.into(),
                    forwarded_host: None,
                    forwarded_proto: None,
                    is_from_trusted_proxy: false,
                    proxy_ip: None,
                };
                req.extensions_mut().insert(client_info);
            }
        }

        self.inner.call(req)
    }
}

/// Handle client-side information extraction logic
fn process_client_info(peer_addr: Option<SocketAddr>, config: &TrustedProxiesConfig, req: &Request) -> ClientInfo {
    match peer_addr {
        Some(addr) => {
            if config.is_trusted(&addr) {
                // From Trusted Agent: Parse the forwarding header
                extract_from_trusted_proxy(&addr, req)
            } else {
                // From an untrusted proxy or direct connection: Use the connection address
                tracing::debug!(
                    "Request from untrusted proxy or direct connection: {}, ignore x-forwarded-*header",
                    addr.ip()
                );
                ClientInfo::direct(addr)
            }
        }
        None => {
            // Unable to get peer address (shouldn't happen in theory)
            tracing::warn!("Unable to get the peer address of the request");
            ClientInfo::direct(SocketAddr::from(([0, 0, 0, 0], 0)))
        }
    }
}

/// Extract client information from HTTP headers
fn extract_client_info_from_headers(req: &Request) -> ClientInfo {
    let headers = req.headers();

    // Parsing X-Forwarded-For: Take the first IP (original client)
    let real_ip = headers
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.split(',').next())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or_else(|| {
            // Fallback to X-Real-IP or Connector IP
            headers
                .get("x-real-ip")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED.into())
        });

    let forwarded_host = headers
        .get("x-forwarded-host")
        .and_then(|h| h.to_str().ok())
        .map(String::from);

    let forwarded_proto = headers
        .get("x-forwarded-proto")
        .and_then(|h| h.to_str().ok())
        .map(String::from);

    ClientInfo {
        real_ip,
        forwarded_host,
        forwarded_proto,
        is_from_trusted_proxy: false,
        proxy_ip: None,
    }
}

/// Extract client information from requests from trusted agents
fn extract_from_trusted_proxy(proxy_addr: &SocketAddr, req: &Request) -> ClientInfo {
    let headers = req.headers();
    let proxy_ip = proxy_addr.ip();

    // Resolve X-Forwarded-For Link
    let real_ip = headers
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .and_then(|xff| {
            // Handle possible IP chains: client, proxy1, proxy2
            parse_x_forwarded_for(xff, proxy_ip)
        })
        .unwrap_or_else(|| {
            // Fall back to X-Real-IP or use a proxy IP
            headers
                .get("x-real-ip")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(proxy_ip) // Finally retreated
        });

    // Extract other forwarding heads
    let forwarded_host = headers
        .get("x-forwarded-host")
        .and_then(|h| h.to_str().ok())
        .map(String::from);

    let forwarded_proto = headers
        .get("x-forwarded-proto")
        .and_then(|h| h.to_str().ok())
        .map(String::from);

    tracing::debug!(
        "Extract client information from trusted proxy {}: real_ip={}, host={:?}, proto={:?}",
        proxy_ip,
        real_ip,
        forwarded_host,
        forwarded_proto
    );

    ClientInfo::from_trusted_proxy(real_ip, forwarded_host, forwarded_proto, proxy_ip)
}

/// Parse a comma-separated list of proxies/IPs
/// Processing format: "ip1, ip2, ip3"
///
/// #Arguments
/// * `proxies_str` - A string slice containing the comma-separated list of proxies/IPs
///
/// #Returns
/// A vector of string slices representing the individual proxies/IPs
pub fn parse_proxy_list(proxies_str: &str) -> Vec<&str> {
    proxies_str.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect()
}

/// Securely parses X-Forwarded-For heads
/// Processing format: "client, proxy1, proxy2" or "client"
fn parse_x_forwarded_for(xff: &str, _current_proxy_ip: IpAddr) -> Option<IpAddr> {
    // Split and clean up IP addresses
    let ips: Vec<&str> = parse_proxy_list(xff);

    if ips.is_empty() {
        return None;
    }

    // Take the first IP (original client)
    // Note: In a production environment, more complex logic may be required to handle multi-layer proxy chains
    ips.first().and_then(|ip_str| ip_str.parse().ok())
}
