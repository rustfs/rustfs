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

//! Simplified trusted proxy mode.
//!
//! The crate keeps both the simplified and legacy implementations. The default
//! runtime path uses the simplified rule set, and an environment variable can
//! switch the global entrypoints to the legacy chain validator.

use crate::{ClientInfo, LegacyTrustedProxyLayer, LegacyTrustedProxyMiddleware, ValidationMode, global};
use axum::http::{HeaderMap, Request};
use rustfs_config::{
    DEFAULT_TRUSTED_PROXY_ENABLED, DEFAULT_TRUSTED_PROXY_IMPLEMENTATION, ENV_TRUSTED_PROXY_ENABLED,
    ENV_TRUSTED_PROXY_IMPLEMENTATION,
};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::debug;

/// Constant switch for the crate's default integration path.
pub const SIMPLE_INTERNAL_ONLY_DEFAULT: bool = true;

const HEADER_FORWARDED: &str = "forwarded";
const HEADER_X_FORWARDED_FOR: &str = "x-forwarded-for";
const HEADER_X_REAL_IP: &str = "x-real-ip";

static ENABLED: OnceLock<bool> = OnceLock::new();
static IMPLEMENTATION: OnceLock<TrustedProxyImplementation> = OnceLock::new();
static LAYER: OnceLock<TrustedProxyLayer> = OnceLock::new();

/// Selects which implementation is used by the global entrypoints.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TrustedProxyImplementation {
    #[default]
    Simple,
    Legacy,
}

impl TrustedProxyImplementation {
    fn from_env() -> Self {
        parse_implementation(std::env::var(ENV_TRUSTED_PROXY_IMPLEMENTATION).ok().as_deref())
    }
}

impl fmt::Display for TrustedProxyImplementation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Simple => "simple",
            Self::Legacy => "legacy",
        })
    }
}

/// Initializes the default trusted proxy implementation.
pub fn init() {
    let enabled = is_enabled();
    let implementation = implementation();
    let _ = layer();

    tracing::info!(
        enabled,
        implementation = %implementation,
        simple_internal_only = SIMPLE_INTERNAL_ONLY_DEFAULT,
        "Trusted proxy middleware initialized"
    );
}

/// Returns whether the default trusted proxy implementation is enabled.
pub fn is_enabled() -> bool {
    *ENABLED.get_or_init(|| parse_env_bool(ENV_TRUSTED_PROXY_ENABLED, DEFAULT_TRUSTED_PROXY_ENABLED))
}

/// Returns the selected implementation.
pub fn implementation() -> TrustedProxyImplementation {
    *IMPLEMENTATION.get_or_init(TrustedProxyImplementation::from_env)
}

/// Returns the default trusted proxy layer.
pub fn layer() -> &'static TrustedProxyLayer {
    LAYER.get_or_init(build_layer)
}

fn build_layer() -> TrustedProxyLayer {
    if !is_enabled() {
        return TrustedProxyLayer::disabled();
    }

    match implementation() {
        TrustedProxyImplementation::Simple => TrustedProxyLayer::enabled(),
        TrustedProxyImplementation::Legacy => {
            global::init();
            TrustedProxyLayer::legacy(global::layer().clone())
        }
    }
}

/// Public layer wrapper for both implementations.
#[derive(Clone, Debug)]
pub enum TrustedProxyLayer {
    Simple(SimpleTrustedProxyLayer),
    Legacy(LegacyTrustedProxyLayer),
}

impl TrustedProxyLayer {
    pub fn enabled() -> Self {
        Self::Simple(SimpleTrustedProxyLayer::enabled())
    }

    pub fn disabled() -> Self {
        Self::Simple(SimpleTrustedProxyLayer::disabled())
    }

    pub fn legacy(layer: LegacyTrustedProxyLayer) -> Self {
        Self::Legacy(layer)
    }

    pub fn is_enabled(&self) -> bool {
        match self {
            Self::Simple(layer) => layer.is_enabled(),
            Self::Legacy(layer) => layer.is_enabled(),
        }
    }

    pub fn is_legacy(&self) -> bool {
        matches!(self, Self::Legacy(_))
    }
}

impl<S> Layer<S> for TrustedProxyLayer {
    type Service = TrustedProxyMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        match self {
            Self::Simple(layer) => TrustedProxyMiddleware::Simple(layer.layer(inner)),
            Self::Legacy(layer) => TrustedProxyMiddleware::Legacy(layer.layer(inner)),
        }
    }
}

/// Public middleware wrapper for both implementations.
#[derive(Clone)]
pub enum TrustedProxyMiddleware<S> {
    Simple(SimpleTrustedProxyMiddleware<S>),
    Legacy(LegacyTrustedProxyMiddleware<S>),
}

impl<S, ReqBody> Service<Request<ReqBody>> for TrustedProxyMiddleware<S>
where
    S: Service<Request<ReqBody>> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self {
            Self::Simple(service) => service.poll_ready(cx),
            Self::Legacy(service) => service.poll_ready(cx),
        }
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        match self {
            Self::Simple(service) => service.call(req),
            Self::Legacy(service) => service.call(req),
        }
    }
}

/// Minimal layer used by RustFS by default.
#[derive(Clone, Debug, Default)]
pub struct SimpleTrustedProxyLayer {
    enabled: bool,
}

impl SimpleTrustedProxyLayer {
    pub fn enabled() -> Self {
        Self { enabled: true }
    }

    pub fn disabled() -> Self {
        Self { enabled: false }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl<S> Layer<S> for SimpleTrustedProxyLayer {
    type Service = SimpleTrustedProxyMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SimpleTrustedProxyMiddleware {
            inner,
            enabled: self.enabled,
        }
    }
}

/// Minimal middleware used by RustFS by default.
#[derive(Clone)]
pub struct SimpleTrustedProxyMiddleware<S> {
    inner: S,
    enabled: bool,
}

impl<S, ReqBody> Service<Request<ReqBody>> for SimpleTrustedProxyMiddleware<S>
where
    S: Service<Request<ReqBody>> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        if self.enabled {
            let peer_addr = req.extensions().get::<SocketAddr>().copied();
            let client_info = resolve_client_info(peer_addr, req.headers());
            req.extensions_mut().insert(client_info);
        } else {
            debug!("Simple trusted proxy middleware is disabled");
        }

        self.inner.call(req)
    }
}

fn resolve_client_info(peer_addr: Option<SocketAddr>, headers: &HeaderMap) -> ClientInfo {
    let Some(peer_addr) = peer_addr else {
        return ClientInfo::direct(SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 0));
    };

    if !is_internal_ip(peer_addr.ip()) {
        return ClientInfo::direct(peer_addr);
    }

    match forwarded_client_ip(headers) {
        Some(real_ip) if is_usable_ip(real_ip) && real_ip != peer_addr.ip() => {
            ClientInfo::from_trusted_proxy(real_ip, None, None, peer_addr.ip(), 1, ValidationMode::Lenient, Vec::new())
        }
        _ => ClientInfo::direct(peer_addr),
    }
}

fn forwarded_client_ip(headers: &HeaderMap) -> Option<IpAddr> {
    parse_x_forwarded_for(headers)
        .or_else(|| parse_single_ip_header(headers, HEADER_X_REAL_IP))
        .or_else(|| parse_forwarded_header(headers))
}

fn parse_x_forwarded_for(headers: &HeaderMap) -> Option<IpAddr> {
    let value = headers.get(HEADER_X_FORWARDED_FOR)?.to_str().ok()?;
    let first = value.split(',').next()?.trim();
    parse_ip_token(first)
}

fn parse_single_ip_header(headers: &HeaderMap, name: &str) -> Option<IpAddr> {
    let value = headers.get(name)?.to_str().ok()?;
    parse_ip_token(value)
}

fn parse_forwarded_header(headers: &HeaderMap) -> Option<IpAddr> {
    let value = headers.get(HEADER_FORWARDED)?.to_str().ok()?;
    let first_entry = value.split(',').next()?.trim();

    for part in first_entry.split(';') {
        let (key, raw_value) = part.split_once('=')?;
        if key.trim().eq_ignore_ascii_case("for") {
            return parse_ip_token(raw_value.trim());
        }
    }

    None
}

fn parse_ip_token(value: &str) -> Option<IpAddr> {
    let value = value.trim().trim_matches('"');
    if value.is_empty() || value.eq_ignore_ascii_case("unknown") || value.starts_with('_') {
        return None;
    }

    if let Some(bracketed) = value.strip_prefix('[')
        && let Some(end) = bracketed.find(']')
    {
        return IpAddr::from_str(&bracketed[..end]).ok();
    }

    if let Ok(ip) = IpAddr::from_str(value) {
        return Some(ip);
    }

    if let Ok(socket_addr) = SocketAddr::from_str(value) {
        return Some(socket_addr.ip());
    }

    if let Some((ip, _port)) = value.rsplit_once(':')
        && ip.parse::<Ipv4Addr>().is_ok()
    {
        return IpAddr::from_str(ip).ok();
    }

    None
}

fn is_internal_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_private() || ip.is_loopback() || ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local(),
    }
}

fn is_usable_ip(ip: IpAddr) -> bool {
    !ip.is_unspecified() && !ip.is_multicast()
}

fn parse_env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

fn parse_implementation(value: Option<&str>) -> TrustedProxyImplementation {
    match value.map(|v| v.trim().to_ascii_lowercase()) {
        Some(mode) if mode == "legacy" || mode == "full" || mode == "full_legacy" => TrustedProxyImplementation::Legacy,
        Some(mode) if mode == "simple" || mode == "internal_only" || mode == "internal-only" => {
            TrustedProxyImplementation::Simple
        }
        Some(mode) if mode == DEFAULT_TRUSTED_PROXY_IMPLEMENTATION => TrustedProxyImplementation::Simple,
        _ => TrustedProxyImplementation::Simple,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ENV_TRUSTED_PROXY_IMPLEMENTATION, HEADER_FORWARDED, HEADER_X_FORWARDED_FOR, HEADER_X_REAL_IP, TrustedProxyImplementation, TrustedProxyLayer, forwarded_client_ip, is_internal_ip,
        parse_env_bool, parse_implementation, parse_ip_token, resolve_client_info,
    };
    use crate::ClientInfo;
    use axum::http::{HeaderMap, HeaderValue};
    use serial_test::serial;
    use std::net::{IpAddr, SocketAddr};

    #[test]
    fn test_simple_mode_is_default() {
        assert!(TrustedProxyLayer::enabled().is_enabled());
        assert!(!TrustedProxyLayer::disabled().is_enabled());
    }

    #[test]
    fn test_parse_implementation() {
        assert_eq!(parse_implementation(Some("simple")), TrustedProxyImplementation::Simple);
        assert_eq!(parse_implementation(Some("legacy")), TrustedProxyImplementation::Legacy);
        assert_eq!(parse_implementation(Some("full")), TrustedProxyImplementation::Legacy);
        assert_eq!(parse_implementation(Some("internal-only")), TrustedProxyImplementation::Simple);
        assert_eq!(parse_implementation(Some("unknown")), TrustedProxyImplementation::Simple);
    }

    #[test]
    fn test_parse_ip_token() {
        assert_eq!(parse_ip_token("203.0.113.10"), Some(IpAddr::from([203, 0, 113, 10])));
        assert_eq!(parse_ip_token("203.0.113.10:9000"), Some(IpAddr::from([203, 0, 113, 10])));
        assert_eq!(parse_ip_token("[2001:db8::10]:9000"), Some("2001:db8::10".parse().unwrap()));
        assert_eq!(parse_ip_token("unknown"), None);
    }

    #[test]
    fn test_forwarded_header_priority() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_X_FORWARDED_FOR, HeaderValue::from_static("203.0.113.10, 10.0.0.5"));
        headers.insert(HEADER_X_REAL_IP, HeaderValue::from_static("198.51.100.10"));
        headers.insert(HEADER_FORWARDED, HeaderValue::from_static("for=192.0.2.60;proto=https"));
        assert_eq!(forwarded_client_ip(&headers), Some(IpAddr::from([203, 0, 113, 10])));
    }

    #[test]
    fn test_forwarded_header_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_FORWARDED, HeaderValue::from_static("for=203.0.113.10;proto=https"));
        assert_eq!(forwarded_client_ip(&headers), Some(IpAddr::from([203, 0, 113, 10])));
    }

    #[test]
    fn test_internal_peer_can_override_real_ip() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_X_FORWARDED_FOR, HeaderValue::from_static("203.0.113.10"));

        let client_info = resolve_client_info(Some(SocketAddr::from(([10, 0, 0, 5], 9000))), &headers);
        assert_eq!(client_info.real_ip, IpAddr::from([203, 0, 113, 10]));
        assert!(client_info.is_from_trusted_proxy);
        assert_eq!(client_info.proxy_ip, Some(IpAddr::from([10, 0, 0, 5])));
    }

    #[test]
    fn test_public_peer_keeps_direct_ip() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_X_FORWARDED_FOR, HeaderValue::from_static("203.0.113.10"));

        let peer_addr = SocketAddr::from(([8, 8, 8, 8], 9000));
        let client_info = resolve_client_info(Some(peer_addr), &headers);
        assert_eq!(client_info.real_ip, peer_addr.ip());
        assert!(!client_info.is_from_trusted_proxy);
    }

    #[test]
    fn test_missing_headers_keep_direct_ip() {
        let peer_addr = SocketAddr::from(([192, 168, 1, 20], 9000));
        let client_info = resolve_client_info(Some(peer_addr), &HeaderMap::new());
        assert_eq!(client_info.real_ip, peer_addr.ip());
        assert!(!client_info.is_from_trusted_proxy);
    }

    #[test]
    fn test_missing_peer_addr_uses_direct_placeholder() {
        let client_info = resolve_client_info(None, &HeaderMap::new());
        assert_eq!(
            client_info.real_ip,
            ClientInfo::direct(SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 0)).real_ip
        );
    }

    #[test]
    fn test_internal_ip_detection() {
        assert!(is_internal_ip(IpAddr::from([10, 0, 0, 1])));
        assert!(is_internal_ip(IpAddr::from([127, 0, 0, 1])));
        assert!(is_internal_ip("fd00::1".parse().unwrap()));
        assert!(!is_internal_ip(IpAddr::from([203, 0, 113, 10])));
    }

    #[test]
    #[serial]
    fn test_parse_env_bool() {
        temp_env::with_vars(vec![("RUSTFS_TRUSTED_PROXY_TEST_BOOL", Some("on"))], || {
            assert!(parse_env_bool("RUSTFS_TRUSTED_PROXY_TEST_BOOL", false));
        });
        temp_env::with_vars(vec![("RUSTFS_TRUSTED_PROXY_TEST_BOOL", Some("off"))], || {
            assert!(!parse_env_bool("RUSTFS_TRUSTED_PROXY_TEST_BOOL", true));
        });
        temp_env::with_vars(vec![("RUSTFS_TRUSTED_PROXY_TEST_BOOL", Some("invalid"))], || {
            assert!(parse_env_bool("RUSTFS_TRUSTED_PROXY_TEST_BOOL", true));
        });
    }

    #[test]
    #[serial]
    fn test_implementation_from_env() {
        temp_env::with_vars(vec![(ENV_TRUSTED_PROXY_IMPLEMENTATION, Some("legacy"))], || {
            assert_eq!(TrustedProxyImplementation::from_env(), TrustedProxyImplementation::Legacy);
        });
    }
}
