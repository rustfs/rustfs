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

use crate::{
    ClientInfo, ConfigLoader, LegacyTrustedProxyLayer, LegacyTrustedProxyMiddleware, TrustedProxy, ValidationMode, global,
};
use axum::http::{HeaderMap, Request, Response, StatusCode};
use futures::future::{Either, Ready, ready};
use rustfs_config::{
    DEFAULT_TRUSTED_PROXY_ENABLED, DEFAULT_TRUSTED_PROXY_IMPLEMENTATION, ENV_TRUSTED_PROXY_ENABLED,
    ENV_TRUSTED_PROXY_IMPLEMENTATION,
};
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::debug;

/// Constant switch for the crate's default integration path.
///
/// Simple mode now honors the configured trusted-proxy allowlist
/// (`RUSTFS_TRUSTED_PROXY_NETWORKS`, defaulting to loopback-only) rather than
/// trusting every private/internal IP range.
pub const SIMPLE_INTERNAL_ONLY_DEFAULT: bool = false;

#[cfg(test)]
const HEADER_FORWARDED: &str = "forwarded";
const HEADER_X_FORWARDED_FOR: &str = "x-forwarded-for";
const HEADER_X_FORWARDED_HOST: &str = "x-forwarded-host";
const HEADER_X_FORWARDED_PROTO: &str = "x-forwarded-proto";
#[cfg(test)]
const HEADER_X_FORWARDED_SCHEME: &str = "x-forwarded-scheme";
#[cfg(test)]
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
    *ENABLED.get_or_init(|| rustfs_utils::get_env_bool(ENV_TRUSTED_PROXY_ENABLED, DEFAULT_TRUSTED_PROXY_ENABLED))
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

    pub fn with_trusted_proxies(trusted_proxies: Vec<TrustedProxy>) -> Self {
        Self::Simple(SimpleTrustedProxyLayer::with_trusted_proxies(trusted_proxies))
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

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for TrustedProxyMiddleware<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send,
    ResBody: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Either<S::Future, Ready<Result<S::Response, S::Error>>>;

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
    trusted_proxies: Arc<Vec<TrustedProxy>>,
}

impl SimpleTrustedProxyLayer {
    /// Creates an enabled simple layer using the operator-configured trusted proxy list
    /// (`RUSTFS_TRUSTED_PROXY_NETWORKS`, `RUSTFS_TRUSTED_PROXY_EXTRA_NETWORKS`,
    /// `RUSTFS_TRUSTED_PROXY_IPS`), falling back to loopback-only (`127.0.0.1`, `::1`).
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            trusted_proxies: Arc::new(ConfigLoader::from_env_or_default().proxy.proxies),
        }
    }

    /// Creates an enabled simple layer trusting only the explicitly supplied proxy entries.
    pub fn with_trusted_proxies(trusted_proxies: Vec<TrustedProxy>) -> Self {
        Self {
            enabled: true,
            trusted_proxies: Arc::new(trusted_proxies),
        }
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            trusted_proxies: Arc::new(Vec::new()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the configured trusted proxy entries enforced by this layer.
    pub fn trusted_proxies(&self) -> &[TrustedProxy] {
        &self.trusted_proxies
    }
}

impl<S> Layer<S> for SimpleTrustedProxyLayer {
    type Service = SimpleTrustedProxyMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SimpleTrustedProxyMiddleware {
            inner,
            enabled: self.enabled,
            trusted_proxies: Arc::clone(&self.trusted_proxies),
        }
    }
}

/// Minimal middleware used by RustFS by default.
#[derive(Clone)]
pub struct SimpleTrustedProxyMiddleware<S> {
    inner: S,
    enabled: bool,
    trusted_proxies: Arc<Vec<TrustedProxy>>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for SimpleTrustedProxyMiddleware<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send,
    ResBody: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Either<S::Future, Ready<Result<S::Response, S::Error>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        if self.enabled {
            let peer_addr = req.extensions().get::<SocketAddr>().copied();
            if peer_addr.is_some_and(|peer| is_trusted_proxy_ip(peer.ip(), &self.trusted_proxies))
                && (req.headers().contains_key(HEADER_X_FORWARDED_FOR)
                    || req.headers().contains_key("forwarded")
                    || req.headers().contains_key("x-real-ip"))
                && forwarded_client_ip(req.headers()).is_none_or(|ip| !is_usable_ip(ip))
            {
                let mut response = Response::new(ResBody::default());
                *response.status_mut() = StatusCode::BAD_REQUEST;
                return Either::Right(ready(Ok(response)));
            }
            let client_info = resolve_client_info_with_proxies(peer_addr, req.headers(), &self.trusted_proxies);
            req.extensions_mut().insert(client_info);
        } else {
            debug!("Simple trusted proxy middleware is disabled");
        }

        Either::Left(self.inner.call(req))
    }
}

fn resolve_client_info_with_proxies(
    peer_addr: Option<SocketAddr>,
    headers: &HeaderMap,
    trusted_proxies: &[TrustedProxy],
) -> ClientInfo {
    let Some(peer_addr) = peer_addr else {
        return ClientInfo::direct(SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 0));
    };

    if !is_trusted_proxy_ip(peer_addr.ip(), trusted_proxies) {
        return ClientInfo::direct(peer_addr);
    }

    match forwarded_client_ip(headers) {
        Some(real_ip) if is_usable_ip(real_ip) && real_ip != peer_addr.ip() => ClientInfo::from_trusted_proxy(
            real_ip,
            forwarded_host(headers),
            forwarded_proto(headers),
            peer_addr.ip(),
            1,
            ValidationMode::Lenient,
            Vec::new(),
        ),
        _ => ClientInfo::direct(peer_addr),
    }
}

fn forwarded_client_ip(headers: &HeaderMap) -> Option<IpAddr> {
    parse_x_forwarded_for(headers)
}

fn forwarded_host(headers: &HeaderMap) -> Option<String> {
    parse_single_value_header(headers, HEADER_X_FORWARDED_HOST).and_then(sanitize_forwarded_host)
}

fn forwarded_proto(headers: &HeaderMap) -> Option<String> {
    parse_single_value_header(headers, HEADER_X_FORWARDED_PROTO).and_then(sanitize_forwarded_proto)
}

fn parse_single_value_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(name)
        .iter()
        .next_back()?
        .to_str()
        .ok()?
        .rsplit(',')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn sanitize_forwarded_host(value: String) -> Option<String> {
    sanitize_forwarded_value(&value).filter(|value| !value.chars().any(char::is_whitespace))
}

fn sanitize_forwarded_proto(value: String) -> Option<String> {
    let value = sanitize_forwarded_value(&value)?;
    if value.eq_ignore_ascii_case("http") || value.eq_ignore_ascii_case("https") {
        return Some(value.to_ascii_lowercase());
    }

    None
}

fn sanitize_forwarded_value(value: &str) -> Option<String> {
    let value = value.trim().trim_matches('"');
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return None;
    }

    Some(value.to_string())
}

fn parse_x_forwarded_for(headers: &HeaderMap) -> Option<IpAddr> {
    // Simple mode trusts exactly one proxy. The rightmost value is the peer
    // that proxy observed, even when that peer is another private address.
    let value = headers.get_all(HEADER_X_FORWARDED_FOR).iter().next_back()?.to_str().ok()?;
    parse_ip_token(value.rsplit(',').next()?.trim())
}

fn parse_ip_token(value: &str) -> Option<IpAddr> {
    let value = value.trim().trim_matches('"');
    if value.is_empty() || value.eq_ignore_ascii_case("unknown") || value.starts_with('_') {
        return None;
    }

    if let Some(bracketed) = value.strip_prefix('[')
        && let Some(end) = bracketed.find(']')
    {
        return IpAddr::from_str(&bracketed[..end]).ok().map(|ip| ip.to_canonical());
    }

    if let Ok(ip) = IpAddr::from_str(value) {
        return Some(ip.to_canonical());
    }

    if let Ok(socket_addr) = SocketAddr::from_str(value) {
        return Some(socket_addr.ip().to_canonical());
    }

    None
}

fn is_trusted_proxy_ip(ip: IpAddr, trusted_proxies: &[TrustedProxy]) -> bool {
    let canonical = ip.to_canonical();
    trusted_proxies.iter().any(|proxy| proxy.contains(&canonical))
}

fn is_usable_ip(ip: IpAddr) -> bool {
    !ip.is_unspecified() && !ip.is_multicast()
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
        ENV_TRUSTED_PROXY_IMPLEMENTATION, HEADER_FORWARDED, HEADER_X_FORWARDED_FOR, HEADER_X_FORWARDED_HOST,
        HEADER_X_FORWARDED_PROTO, HEADER_X_FORWARDED_SCHEME, HEADER_X_REAL_IP, SimpleTrustedProxyLayer,
        TrustedProxyImplementation, TrustedProxyLayer, forwarded_client_ip, forwarded_host, forwarded_proto, is_trusted_proxy_ip,
        parse_implementation, parse_ip_token, resolve_client_info_with_proxies,
    };
    use crate::{ClientInfo, ConfigLoader, TrustedProxy};
    use axum::http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
    use ipnetwork::IpNetwork;
    use rustfs_config::{ENV_TRUSTED_PROXY_EXTRA_PROXIES, ENV_TRUSTED_PROXY_IPS, ENV_TRUSTED_PROXY_PROXIES};
    use serial_test::serial;
    use std::convert::Infallible;
    use std::net::{IpAddr, SocketAddr};
    use tower::{Layer, Service, service_fn};

    fn default_loopback_proxies() -> Vec<TrustedProxy> {
        ConfigLoader::default_config().proxy.proxies
    }

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
        assert_eq!(forwarded_client_ip(&headers), Some(IpAddr::from([10, 0, 0, 5])));
    }

    #[test]
    fn test_forwarded_header_does_not_select_another_address_family() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_FORWARDED, HeaderValue::from_static("for=203.0.113.10;proto=https"));
        assert_eq!(forwarded_client_ip(&headers), None);
    }

    #[test]
    fn test_default_loopback_peer_can_override_real_ip() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_X_FORWARDED_FOR, HeaderValue::from_static("203.0.113.10"));

        let proxies = default_loopback_proxies();
        let client_info = resolve_client_info_with_proxies(Some(SocketAddr::from(([127, 0, 0, 1], 9000))), &headers, &proxies);
        assert_eq!(client_info.real_ip, IpAddr::from([203, 0, 113, 10]));
        assert!(client_info.is_from_trusted_proxy);
        assert_eq!(client_info.proxy_ip, Some(IpAddr::from([127, 0, 0, 1])));

        let ipv6_loopback: SocketAddr = "[::1]:9000".parse().expect("IPv6 loopback should parse");
        let ipv6_info = resolve_client_info_with_proxies(Some(ipv6_loopback), &headers, &proxies);
        assert_eq!(ipv6_info.real_ip, IpAddr::from([203, 0, 113, 10]));
        assert!(ipv6_info.is_from_trusted_proxy);
    }

    #[test]
    fn test_untrusted_private_and_link_local_peers_cannot_spoof_forwarding_headers_by_default() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_X_FORWARDED_FOR, HeaderValue::from_static("203.0.113.10"));
        headers.insert(HEADER_X_FORWARDED_HOST, HeaderValue::from_static("spoofed.example.test"));
        headers.insert(HEADER_X_FORWARDED_PROTO, HeaderValue::from_static("https"));

        let proxies = default_loopback_proxies();
        for peer_ip in [
            IpAddr::from([10, 0, 0, 5]),
            IpAddr::from([172, 16, 0, 5]),
            IpAddr::from([192, 168, 1, 20]),
            IpAddr::from([169, 254, 1, 10]),
            "fd00::1".parse().expect("ULA IPv6"),
            "fe80::1".parse().expect("link-local IPv6"),
            "::ffff:10.0.0.5".parse().expect("IPv4-mapped RFC1918"),
        ] {
            let peer_addr = SocketAddr::new(peer_ip, 9000);
            let client_info = resolve_client_info_with_proxies(Some(peer_addr), &headers, &proxies);
            assert_eq!(
                client_info.real_ip,
                peer_ip.to_canonical(),
                "untrusted internal peer {peer_ip} must not spoof real_ip"
            );
            assert!(!client_info.is_from_trusted_proxy, "peer {peer_ip} must not be trusted by default");
            assert_eq!(client_info.forwarded_host, None, "peer {peer_ip} must not spoof forwarded_host");
            assert_eq!(client_info.forwarded_proto, None, "peer {peer_ip} must not spoof forwarded_proto");
        }
    }

    #[test]
    fn test_explicitly_configured_proxy_cidr_is_honored_and_bounded() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_X_FORWARDED_FOR, HeaderValue::from_static("198.51.100.7, 203.0.113.10"));
        headers.insert(HEADER_X_FORWARDED_HOST, HeaderValue::from_static("s3.example.test"));
        headers.insert(HEADER_X_FORWARDED_PROTO, HeaderValue::from_static("https"));

        let proxies = vec![TrustedProxy::Cidr("10.0.0.0/24".parse::<IpNetwork>().expect("CIDR"))];

        let allowed = resolve_client_info_with_proxies(Some(SocketAddr::from(([10, 0, 0, 5], 9000))), &headers, &proxies);
        assert_eq!(allowed.real_ip, IpAddr::from([203, 0, 113, 10]));
        assert!(allowed.is_from_trusted_proxy);
        assert_eq!(allowed.proxy_ip, Some(IpAddr::from([10, 0, 0, 5])));
        assert_eq!(allowed.forwarded_host.as_deref(), Some("s3.example.test"));
        assert_eq!(allowed.forwarded_proto.as_deref(), Some("https"));

        // An adjacent RFC1918 peer outside the configured CIDR must not be trusted.
        let outside = resolve_client_info_with_proxies(Some(SocketAddr::from(([10, 0, 1, 5], 9000))), &headers, &proxies);
        assert_eq!(outside.real_ip, IpAddr::from([10, 0, 1, 5]));
        assert!(!outside.is_from_trusted_proxy);
        assert_eq!(outside.forwarded_host, None);
        assert_eq!(outside.forwarded_proto, None);
    }

    #[test]
    fn test_trusted_peer_ignores_unselected_forwarded_scheme() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_X_FORWARDED_FOR, HeaderValue::from_static("203.0.113.10"));
        headers.insert(HEADER_X_FORWARDED_SCHEME, HeaderValue::from_static("https"));

        let proxies = vec![TrustedProxy::Single(IpAddr::from([10, 0, 0, 5]))];
        let client_info = resolve_client_info_with_proxies(Some(SocketAddr::from(([10, 0, 0, 5], 9000))), &headers, &proxies);

        assert_eq!(client_info.forwarded_proto, None);
    }

    #[test]
    fn test_forwarded_header_cannot_supply_proto_or_host() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HEADER_FORWARDED,
            HeaderValue::from_static("for=203.0.113.10;proto=https;host=s3.example.test"),
        );

        let proxies = vec![TrustedProxy::Single(IpAddr::from([10, 0, 0, 5]))];
        let client_info = resolve_client_info_with_proxies(Some(SocketAddr::from(([10, 0, 0, 5], 9000))), &headers, &proxies);

        assert_eq!(client_info.real_ip, IpAddr::from([10, 0, 0, 5]));
        assert_eq!(client_info.forwarded_host, None);
        assert_eq!(client_info.forwarded_proto, None);
    }

    #[test]
    fn test_invalid_forwarded_host_and_proto_are_ignored() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_X_FORWARDED_HOST, HeaderValue::from_static("bad host"));
        headers.insert(HEADER_X_FORWARDED_PROTO, HeaderValue::from_static("ftp"));

        assert_eq!(forwarded_host(&headers), None);
        assert_eq!(forwarded_proto(&headers), None);
    }

    #[test]
    fn test_public_peer_keeps_direct_ip() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_X_FORWARDED_FOR, HeaderValue::from_static("203.0.113.10"));

        let peer_addr = SocketAddr::from(([8, 8, 8, 8], 9000));
        let client_info = resolve_client_info_with_proxies(Some(peer_addr), &headers, &default_loopback_proxies());
        assert_eq!(client_info.real_ip, peer_addr.ip());
        assert!(!client_info.is_from_trusted_proxy);
    }

    #[test]
    fn test_missing_headers_keep_direct_ip() {
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 9000));
        let client_info = resolve_client_info_with_proxies(Some(peer_addr), &HeaderMap::new(), &default_loopback_proxies());
        assert_eq!(client_info.real_ip, peer_addr.ip());
        assert!(!client_info.is_from_trusted_proxy);
    }

    #[test]
    fn test_missing_peer_addr_uses_direct_placeholder() {
        let client_info = resolve_client_info_with_proxies(None, &HeaderMap::new(), &default_loopback_proxies());
        assert_eq!(
            client_info.real_ip,
            ClientInfo::direct(SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 0)).real_ip
        );
    }

    #[test]
    fn test_forwarded_header_segment_without_equals() {
        // Simple mode never accepts the alternate Forwarded family.
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_FORWARDED, HeaderValue::from_static("proto;for=203.0.113.10"));
        assert_eq!(forwarded_client_ip(&headers), None);
        headers.insert(HEADER_X_FORWARDED_FOR, HeaderValue::from_static("203.0.113.20"));
        assert_eq!(forwarded_client_ip(&headers), Some(IpAddr::from([203, 0, 113, 20])));
    }

    #[test]
    fn test_parse_ip_token_invalid_port_rejected() {
        // A bare "ip:notaport" token must not be accepted as a valid IP.
        assert_eq!(parse_ip_token("203.0.113.10:notaport"), None);
    }

    #[test]
    fn test_trusted_proxy_ip_matching() {
        let proxies = vec![
            TrustedProxy::Single(IpAddr::from([127, 0, 0, 1])),
            TrustedProxy::Cidr("10.20.0.0/24".parse::<IpNetwork>().expect("CIDR")),
        ];
        assert!(is_trusted_proxy_ip(IpAddr::from([127, 0, 0, 1]), &proxies));
        assert!(is_trusted_proxy_ip("::ffff:127.0.0.1".parse().expect("mapped loopback"), &proxies));
        assert!(is_trusted_proxy_ip(IpAddr::from([10, 20, 0, 42]), &proxies));
        assert!(!is_trusted_proxy_ip(IpAddr::from([10, 20, 1, 1]), &proxies));
        assert!(!is_trusted_proxy_ip(IpAddr::from([192, 168, 1, 1]), &proxies));
        assert!(!is_trusted_proxy_ip("fd00::1".parse().expect("ULA"), &proxies));
        assert!(!is_trusted_proxy_ip(IpAddr::from([203, 0, 113, 10]), &proxies));
    }

    #[tokio::test]
    async fn test_middleware_rejects_malformed_forwarded_from_trusted_proxy_but_ignores_untrusted_private_peer() {
        let layer = SimpleTrustedProxyLayer::with_trusted_proxies(default_loopback_proxies());
        let mut svc = layer.layer(service_fn(|req: Request<()>| async move {
            let info = req
                .extensions()
                .get::<ClientInfo>()
                .cloned()
                .expect("middleware must attach ClientInfo");
            let mut resp = Response::new(String::new());
            resp.headers_mut()
                .insert("x-observed-real-ip", HeaderValue::from_str(&info.real_ip.to_string()).expect("header"));
            resp.headers_mut().insert(
                "x-observed-trusted",
                HeaderValue::from_static(if info.is_from_trusted_proxy { "true" } else { "false" }),
            );
            Ok::<_, Infallible>(resp)
        }));

        // Untrusted private peer sending malformed X-Forwarded-For is treated as direct (not trusted, not 400).
        let mut untrusted_req = Request::builder()
            .header(HEADER_X_FORWARDED_FOR, "not-an-ip")
            .body(())
            .expect("request");
        untrusted_req.extensions_mut().insert(SocketAddr::from(([10, 0, 0, 9], 4321)));
        let untrusted_resp = svc.call(untrusted_req).await.expect("infallible");
        assert_eq!(untrusted_resp.status(), StatusCode::OK);
        assert_eq!(untrusted_resp.headers().get("x-observed-real-ip").unwrap(), "10.0.0.9");
        assert_eq!(untrusted_resp.headers().get("x-observed-trusted").unwrap(), "false");

        // Trusted loopback proxy sending malformed X-Forwarded-For fails closed with 400.
        let mut trusted_req = Request::builder()
            .header(HEADER_X_FORWARDED_FOR, "not-an-ip")
            .body(())
            .expect("request");
        trusted_req.extensions_mut().insert(SocketAddr::from(([127, 0, 0, 1], 4321)));
        let trusted_resp = svc.call(trusted_req).await.expect("infallible");
        assert_eq!(trusted_resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    #[serial]
    fn test_simple_layer_loads_configured_networks_from_env() {
        temp_env::with_vars(
            [
                (ENV_TRUSTED_PROXY_PROXIES, Some("10.42.0.0/24")),
                (ENV_TRUSTED_PROXY_EXTRA_PROXIES, Some("")),
                (ENV_TRUSTED_PROXY_IPS, Some("198.51.100.5")),
            ],
            || {
                let layer = SimpleTrustedProxyLayer::enabled();
                assert!(is_trusted_proxy_ip(IpAddr::from([10, 42, 0, 9]), layer.trusted_proxies()));
                assert!(is_trusted_proxy_ip(IpAddr::from([198, 51, 100, 5]), layer.trusted_proxies()));
                assert!(!is_trusted_proxy_ip(IpAddr::from([10, 42, 1, 9]), layer.trusted_proxies()));
                assert!(!is_trusted_proxy_ip(IpAddr::from([192, 168, 1, 1]), layer.trusted_proxies()));
            },
        );
    }

    #[test]
    #[serial]
    fn test_implementation_from_env() {
        temp_env::with_vars(vec![(ENV_TRUSTED_PROXY_IMPLEMENTATION, Some("legacy"))], || {
            assert_eq!(TrustedProxyImplementation::from_env(), TrustedProxyImplementation::Legacy);
        });
    }
}
