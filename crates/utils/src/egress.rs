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

use std::collections::HashSet;
#[cfg(test)]
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
#[cfg(test)]
use std::sync::Mutex;
use std::sync::{Arc, OnceLock};
use url::Url;

/// Comma-separated exact HTTP(S) origins that may resolve to otherwise restricted addresses.
///
/// This is an operator-owned process setting. Target configuration cannot extend it. Link-local,
/// metadata, and unspecified addresses remain forbidden even when their origin appears in the list.
pub const ENV_OUTBOUND_ALLOW_ORIGINS: &str = "RUSTFS_OUTBOUND_ALLOW_ORIGINS";

static OUTBOUND_POLICY: OnceLock<Result<OutboundPolicy, OutboundPolicyError>> = OnceLock::new();

/// Identifies which outbound network hop an egress check or DNS resolution applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboundPeerKind {
    /// Direct connection to the requested target origin.
    Target,
    /// Connection to an operator-configured forward or `CONNECT` proxy hop.
    Proxy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundUrlError {
    MissingHost,
    ForbiddenHost { host: String, reason: &'static str },
}

impl fmt::Display for OutboundUrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutboundUrlError::MissingHost => write!(f, "outbound URL is missing a host"),
            OutboundUrlError::ForbiddenHost { host, reason } => {
                write!(f, "outbound URL host '{host}' is not allowed: {reason}")
            }
        }
    }
}

impl std::error::Error for OutboundUrlError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundPolicyError {
    NonUnicodeEnvironment,
    InvalidAllowedOrigin { position: usize },
    MissingHost,
    UnsupportedScheme { scheme: String },
    UserInfoNotAllowed,
    ForbiddenHost { host: String, reason: &'static str },
}

impl OutboundPolicyError {
    /// Whether adding the destination's exact origin to [`ENV_OUTBOUND_ALLOW_ORIGINS`]
    /// could permit this request.
    pub fn allow_origin_can_recover(&self) -> bool {
        matches!(self, Self::ForbiddenHost { reason, .. } if restricted_reason_can_be_overridden(reason))
    }
}

impl fmt::Display for OutboundPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutboundPolicyError::NonUnicodeEnvironment => {
                write!(f, "{ENV_OUTBOUND_ALLOW_ORIGINS} must contain valid Unicode")
            }
            OutboundPolicyError::InvalidAllowedOrigin { position } => {
                write!(f, "invalid origin at position {position} in {ENV_OUTBOUND_ALLOW_ORIGINS}")
            }
            OutboundPolicyError::MissingHost => write!(f, "outbound URL is missing a host"),
            OutboundPolicyError::UnsupportedScheme { scheme } => {
                write!(f, "outbound URL scheme '{scheme}' is not allowed")
            }
            OutboundPolicyError::UserInfoNotAllowed => write!(f, "outbound URL userinfo is not allowed"),
            OutboundPolicyError::ForbiddenHost { host, reason } => {
                write!(f, "outbound URL host '{host}' is not allowed: {reason}")
            }
        }
    }
}

impl std::error::Error for OutboundPolicyError {}

/// Error returned when configuring an outbound HTTP client for a target URL and optional proxy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundClientError {
    Policy(OutboundPolicyError),
    InvalidProxyUrl,
    UnsupportedProxyScheme { scheme: String },
    ForbiddenProxyHost { host: String, reason: &'static str },
}

impl OutboundClientError {
    /// Whether this error rejected an outbound proxy configuration rather than the target URL.
    pub const fn is_proxy_error(&self) -> bool {
        matches!(
            self,
            Self::InvalidProxyUrl | Self::UnsupportedProxyScheme { .. } | Self::ForbiddenProxyHost { .. }
        )
    }

    /// Whether adding the destination's exact origin to [`ENV_OUTBOUND_ALLOW_ORIGINS`]
    /// could permit this request.
    pub fn allow_origin_can_recover(&self) -> bool {
        match self {
            Self::Policy(err) => err.allow_origin_can_recover(),
            Self::InvalidProxyUrl | Self::UnsupportedProxyScheme { .. } | Self::ForbiddenProxyHost { .. } => false,
        }
    }
}

impl From<OutboundPolicyError> for OutboundClientError {
    fn from(err: OutboundPolicyError) -> Self {
        Self::Policy(err)
    }
}

impl fmt::Display for OutboundClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy(err) => fmt::Display::fmt(err, f),
            Self::InvalidProxyUrl => write!(f, "outbound proxy URL is invalid"),
            Self::UnsupportedProxyScheme { scheme } => {
                write!(f, "outbound proxy URL scheme '{scheme}' is not allowed")
            }
            Self::ForbiddenProxyHost { host, reason } => {
                write!(f, "outbound proxy host '{host}' is not allowed: {reason}")
            }
        }
    }
}

impl std::error::Error for OutboundClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Policy(err) => Some(err),
            Self::InvalidProxyUrl | Self::UnsupportedProxyScheme { .. } | Self::ForbiddenProxyHost { .. } => None,
        }
    }
}

/// A DNS answer was rejected at the connection boundary because every resolved
/// address was forbidden by the outbound policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundDnsPolicyRejection {
    host: String,
    peer_kind: OutboundPeerKind,
    allow_origin_can_recover: bool,
}

impl OutboundDnsPolicyRejection {
    /// Records a rejected target host and whether an exact operator allowlist origin
    /// could permit at least one of its resolved addresses.
    pub fn new(host: String, allow_origin_can_recover: bool) -> Self {
        Self {
            host,
            peer_kind: OutboundPeerKind::Target,
            allow_origin_can_recover,
        }
    }

    /// Records a rejected outbound proxy hostname; allowlisting target origins cannot recover this.
    pub fn new_proxy(host: String) -> Self {
        Self {
            host,
            peer_kind: OutboundPeerKind::Proxy,
            allow_origin_can_recover: false,
        }
    }

    /// The normalized hostname whose DNS answers were rejected.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Whether the rejected DNS lookup was for the direct target host or an outbound proxy hop.
    pub fn peer_kind(&self) -> OutboundPeerKind {
        self.peer_kind
    }

    /// Whether an exact allowlist origin could permit at least one rejected address.
    pub fn allow_origin_can_recover(&self) -> bool {
        self.allow_origin_can_recover
    }
}

impl fmt::Display for OutboundDnsPolicyRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.peer_kind {
            OutboundPeerKind::Target => {
                write!(f, "outbound DNS resolution for '{}' returned no allowed addresses", self.host)
            }
            OutboundPeerKind::Proxy => {
                write!(
                    f,
                    "outbound DNS resolution for outbound proxy '{}' returned no allowed addresses",
                    self.host
                )
            }
        }
    }
}

impl std::error::Error for OutboundDnsPolicyRejection {}

/// Finds a typed DNS policy rejection through transport error wrappers.
pub fn find_outbound_dns_policy_rejection<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a OutboundDnsPolicyRejection> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(rejection) = error.downcast_ref::<OutboundDnsPolicyRejection>() {
            return Some(rejection);
        }
        if let Some(rejection) = error
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .and_then(|inner| inner.downcast_ref::<OutboundDnsPolicyRejection>())
        {
            return Some(rejection);
        }
        current = error.source();
    }
    None
}

/// Deterministic DNS lookup hook used by unit and cross-crate transport tests.
#[doc(hidden)]
pub type OutboundDnsLookup = Arc<dyn Fn(&str) -> Option<Vec<SocketAddr>> + Send + Sync>;

/// Evaluates `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` / `NO_PROXY` (and optional OS proxy
/// configuration) without exposing `hyper-util` types to downstream crates.
///
/// Proxy credentials are never formatted by [`fmt::Debug`] or error messages.
#[derive(Clone)]
pub struct OutboundProxyMatcher(Arc<hyper_util::client::proxy::matcher::Matcher>);

impl fmt::Debug for OutboundProxyMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundProxyMatcher").finish_non_exhaustive()
    }
}

impl Default for OutboundProxyMatcher {
    fn default() -> Self {
        Self::none()
    }
}

struct ValidatedProxy {
    canonical_url: Url,
    basic_auth: Option<http::HeaderValue>,
}

impl OutboundProxyMatcher {
    /// Returns a matcher that never routes requests through a proxy.
    pub fn none() -> Self {
        Self(Arc::new(hyper_util::client::proxy::matcher::Matcher::builder().build()))
    }

    /// Builds a matcher from process environment variables (`HTTP_PROXY`, `HTTPS_PROXY`,
    /// `ALL_PROXY`, `NO_PROXY` and lowercase counterparts) plus OS system proxy settings.
    pub fn from_system() -> Self {
        Self(Arc::new(hyper_util::client::proxy::matcher::Matcher::from_system()))
    }

    /// Builds a matcher from process environment variables only, ignoring OS dynamic stores.
    pub fn from_env() -> Self {
        Self(Arc::new(hyper_util::client::proxy::matcher::Matcher::from_env()))
    }

    /// Builds a hermetic matcher from explicit `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, and
    /// `NO_PROXY` values without reading process environment or OS proxy stores.
    pub fn from_env_values(
        http_proxy: Option<&str>,
        https_proxy: Option<&str>,
        all_proxy: Option<&str>,
        no_proxy: Option<&str>,
    ) -> Self {
        Self(Arc::new(
            hyper_util::client::proxy::matcher::Matcher::builder()
                .http(http_proxy.map(str::trim).unwrap_or_default())
                .https(https_proxy.map(str::trim).unwrap_or_default())
                .all(all_proxy.map(str::trim).unwrap_or_default())
                .no(no_proxy.map(str::trim).unwrap_or_default())
                .build(),
        ))
    }

    /// Builds a hermetic matcher routing all non-loopback HTTP(S) traffic through `proxy`.
    pub fn all(proxy: &str) -> Self {
        Self::from_env_values(None, None, Some(proxy), None)
    }

    fn match_target(&self, target: &ValidatedTarget, url: &Url) -> Result<Option<ValidatedProxy>, OutboundClientError> {
        let dst = target.proxy_target_uri(url)?;
        let Some(intercept) = self.0.intercept(&dst) else {
            return Ok(None);
        };
        let scheme = intercept.uri().scheme_str().unwrap_or_default();
        if !matches!(scheme, "http" | "https") {
            return Err(OutboundClientError::UnsupportedProxyScheme {
                scheme: scheme.to_ascii_lowercase(),
            });
        }
        if intercept.raw_auth().is_some() {
            return Err(OutboundClientError::InvalidProxyUrl);
        }
        let canonical_url = Url::parse(&intercept.uri().to_string()).map_err(|_| OutboundClientError::InvalidProxyUrl)?;
        validate_proxy_url(&canonical_url)?;
        Ok(Some(ValidatedProxy {
            canonical_url,
            basic_auth: intercept.basic_auth().cloned(),
        }))
    }
}

fn validate_proxy_url(url: &Url) -> Result<(), OutboundClientError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(OutboundClientError::UnsupportedProxyScheme {
            scheme: url.scheme().to_string(),
        });
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(OutboundClientError::InvalidProxyUrl);
    }
    let raw_host = url
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or(OutboundClientError::InvalidProxyUrl)?;
    let normalized = normalize_host_str(raw_host);
    if normalized.is_empty() {
        return Err(OutboundClientError::InvalidProxyUrl);
    }
    match classify_url_host(normalized) {
        Ok(()) => Ok(()),
        Err(reason) if proxy_restricted_reason_allowed(reason) => Ok(()),
        Err(reason) => Err(OutboundClientError::ForbiddenProxyHost {
            host: raw_host.to_string(),
            reason,
        }),
    }
}

fn is_loopback_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => v6.is_loopback() || embedded_ipv4(v6).is_some_and(|v4| v4.is_loopback()),
    }
}

/// Returns `true` when `url` targets `localhost` (including `localhost.`) or any IPv4/IPv6
/// loopback address (including IPv4-mapped/compatible IPv6 loopback) and must bypass proxies.
pub fn should_bypass_proxy_for_url(url: &Url) -> bool {
    let Some(raw_host) = url.host_str() else {
        return false;
    };
    let host = normalize_host_str(raw_host);
    host.eq_ignore_ascii_case("localhost") || host.parse::<IpAddr>().is_ok_and(is_loopback_ip)
}

struct ValidatedTarget {
    host: String,
    is_ipv6: bool,
    allow_restricted: bool,
    bypass_proxy: bool,
}

impl ValidatedTarget {
    fn proxy_target_uri(&self, url: &Url) -> Result<http::Uri, OutboundPolicyError> {
        let authority = match (self.is_ipv6, url.port()) {
            (true, Some(port)) => format!("[{}]:{port}", self.host),
            (true, None) => format!("[{}]", self.host),
            (false, Some(port)) => format!("{}:{port}", self.host),
            (false, None) => self.host.clone(),
        };
        http::Uri::builder()
            .scheme(url.scheme())
            .authority(authority.as_str())
            .path_and_query("/")
            .build()
            .map_err(|_| OutboundPolicyError::MissingHost)
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct OutboundPolicy {
    allowed_restricted_origins: HashSet<String>,
}

impl OutboundPolicy {
    pub fn from_env() -> Result<Self, OutboundPolicyError> {
        match std::env::var(ENV_OUTBOUND_ALLOW_ORIGINS) {
            Ok(value) => Self::from_allowed_origins(&value),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(std::env::VarError::NotUnicode(_)) => Err(OutboundPolicyError::NonUnicodeEnvironment),
        }
    }

    pub fn from_env_cached() -> Result<&'static Self, OutboundPolicyError> {
        match OUTBOUND_POLICY.get_or_init(Self::from_env) {
            Ok(policy) => Ok(policy),
            Err(err) => Err(err.clone()),
        }
    }

    pub fn from_allowed_origins(value: &str) -> Result<Self, OutboundPolicyError> {
        let mut allowed_restricted_origins = HashSet::new();
        if value.trim().is_empty() {
            return Ok(Self::default());
        }
        for (position, entry) in value.split(',').map(str::trim).enumerate() {
            let position = position + 1;
            if entry.is_empty() {
                return Err(OutboundPolicyError::InvalidAllowedOrigin { position });
            }
            let url = Url::parse(entry).map_err(|_| OutboundPolicyError::InvalidAllowedOrigin { position })?;
            let origin = normalized_origin(&url).ok_or(OutboundPolicyError::InvalidAllowedOrigin { position })?;
            if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
                return Err(OutboundPolicyError::InvalidAllowedOrigin { position });
            }
            allowed_restricted_origins.insert(origin);
        }
        Ok(Self {
            allowed_restricted_origins,
        })
    }

    pub fn validate_url(&self, url: &Url) -> Result<(), OutboundPolicyError> {
        self.validate_target(url).map(|_| ())
    }

    pub fn resolver_for(&self, url: &Url) -> Result<OutboundDnsResolver, OutboundPolicyError> {
        let target = self.validate_target(url)?;
        Ok(OutboundDnsResolver::new(target.host, target.allow_restricted))
    }

    /// Configures `builder` for a single outbound request to `url` using system proxy settings.
    ///
    /// # Errors
    ///
    /// Returns [`OutboundClientError`] if `url` violates the target outbound policy or if a
    /// matched outbound proxy URL is invalid, uses a non-HTTP(S) scheme, or targets a forbidden
    /// literal address (such as cloud metadata, link-local, unspecified, or reserved space).
    pub fn configure_reqwest_client(
        &self,
        url: &Url,
        builder: reqwest::ClientBuilder,
    ) -> Result<reqwest::ClientBuilder, OutboundClientError> {
        let proxy_matcher = OutboundProxyMatcher::from_system();
        self.configure_reqwest_client_with_proxy_matcher(url, &proxy_matcher, builder)
    }

    /// Configures `builder` for a single outbound request to `url` using an explicit
    /// [`OutboundProxyMatcher`].
    ///
    /// Target URL shape and IP-literal checks always run first. Loopback targets and targets
    /// matched by `NO_PROXY` connect directly using target-mode [`OutboundDnsResolver`]. Requests
    /// intercepted by an HTTP(S) proxy validate the canonicalized proxy URL, attach any proxy
    /// `Basic` credentials without putting secrets in the URL, and resolve the proxy hostname
    /// through [`OutboundDnsResolver::for_proxy`].
    ///
    /// # Errors
    ///
    /// Returns [`OutboundClientError`] when either `url` or the matched proxy URL is rejected.
    pub fn configure_reqwest_client_with_proxy_matcher(
        &self,
        url: &Url,
        proxy_matcher: &OutboundProxyMatcher,
        builder: reqwest::ClientBuilder,
    ) -> Result<reqwest::ClientBuilder, OutboundClientError> {
        self.configure_reqwest_client_for_test(url, proxy_matcher, None, builder)
    }

    /// Configures `builder` like [`Self::configure_reqwest_client_with_proxy_matcher`] while
    /// injecting an optional DNS lookup hook into whichever [`OutboundDnsResolver`] mode applies.
    #[doc(hidden)]
    pub fn configure_reqwest_client_for_test(
        &self,
        url: &Url,
        proxy_matcher: &OutboundProxyMatcher,
        dns_override: Option<OutboundDnsLookup>,
        builder: reqwest::ClientBuilder,
    ) -> Result<reqwest::ClientBuilder, OutboundClientError> {
        let target = self.validate_target(url)?;

        if !target.bypass_proxy
            && let Some(matched_proxy) = proxy_matcher.match_target(&target, url)?
        {
            let mut reqwest_proxy =
                reqwest::Proxy::all(matched_proxy.canonical_url).map_err(|_| OutboundClientError::InvalidProxyUrl)?;
            if let Some(mut basic_auth) = matched_proxy.basic_auth {
                basic_auth.set_sensitive(true);
                reqwest_proxy = reqwest_proxy.custom_http_auth(basic_auth);
            }
            let mut proxy_resolver = OutboundDnsResolver::for_proxy();
            if let Some(dns_override) = dns_override {
                proxy_resolver = proxy_resolver.with_lookup_override(dns_override);
            }
            return Ok(builder.no_proxy().proxy(reqwest_proxy).dns_resolver(proxy_resolver));
        }

        let mut target_resolver = OutboundDnsResolver::new(target.host, target.allow_restricted);
        if let Some(dns_override) = dns_override {
            target_resolver = target_resolver.with_lookup_override(dns_override);
        }
        Ok(builder.no_proxy().dns_resolver(target_resolver))
    }

    fn validate_target(&self, url: &Url) -> Result<ValidatedTarget, OutboundPolicyError> {
        validate_http_url_shape(url)?;
        let raw_host = url.host_str().ok_or(OutboundPolicyError::MissingHost)?;
        let normalized = normalize_host_str(raw_host);
        if normalized.is_empty() {
            return Err(OutboundPolicyError::MissingHost);
        }
        let host = normalized.to_ascii_lowercase();
        let parsed_ip = host.parse::<IpAddr>().ok();
        let is_ipv6 = parsed_ip.is_some_and(|ip| ip.is_ipv6());
        let bypass_proxy = host == "localhost" || parsed_ip.is_some_and(is_loopback_ip);
        let classification = if host == "localhost" {
            Err("loopback host")
        } else if let Some(ip) = parsed_ip {
            validate_policy_ip(ip)
        } else {
            Ok(())
        };

        let allow_restricted = !self.allowed_restricted_origins.is_empty()
            && url
                .port_or_known_default()
                .map(|port| format_normalized_origin(url.scheme(), &host, is_ipv6, port))
                .is_some_and(|origin| self.allowed_restricted_origins.contains(&origin));

        match classification {
            Ok(()) => {}
            Err(reason) if allow_restricted && restricted_reason_can_be_overridden(reason) => {}
            Err(reason) => {
                return Err(OutboundPolicyError::ForbiddenHost {
                    host: raw_host.to_string(),
                    reason,
                });
            }
        }

        Ok(ValidatedTarget {
            host,
            is_ipv6,
            allow_restricted,
            bypass_proxy,
        })
    }
}

#[derive(Clone)]
pub struct OutboundDnsResolver {
    peer_kind: OutboundPeerKind,
    allowed_restricted_host: Option<String>,
    lookup_override: Option<OutboundDnsLookup>,
}

impl fmt::Debug for OutboundDnsResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundDnsResolver")
            .field("peer_kind", &self.peer_kind)
            .field("allowed_restricted_host", &self.allowed_restricted_host)
            .field("has_lookup_override", &self.lookup_override.is_some())
            .finish()
    }
}

impl OutboundDnsResolver {
    fn new(host: String, allow_restricted: bool) -> Self {
        Self {
            peer_kind: OutboundPeerKind::Target,
            allowed_restricted_host: allow_restricted.then_some(host),
            lookup_override: None,
        }
    }

    /// Creates a connection-time DNS resolver for an operator-configured outbound proxy hop.
    ///
    /// Permits public, private (RFC 1918 / IPv6 ULA excluding cloud metadata), shared (CGNAT),
    /// and loopback addresses while rejecting metadata, link-local, unspecified, translation, and
    /// reserved addresses on every new connection.
    pub fn for_proxy() -> Self {
        Self {
            peer_kind: OutboundPeerKind::Proxy,
            allowed_restricted_host: None,
            lookup_override: None,
        }
    }

    /// Attaches a deterministic DNS lookup callback for tests while retaining full policy filtering.
    #[doc(hidden)]
    pub fn with_lookup_override(mut self, lookup: OutboundDnsLookup) -> Self {
        self.lookup_override = Some(lookup);
        self
    }

    #[cfg(test)]
    fn with_overrides(self, overrides: HashMap<String, Vec<IpAddr>>) -> Self {
        let overrides: HashMap<String, Vec<SocketAddr>> = overrides
            .into_iter()
            .map(|(host, ips)| {
                let addresses = ips.into_iter().map(|ip| SocketAddr::new(ip, 0)).collect();
                (host, addresses)
            })
            .collect();
        self.with_lookup_override(Arc::new(move |host| overrides.get(host).cloned()))
    }

    #[cfg(test)]
    fn with_override_sequence(self, host: &str, answers: Vec<Vec<SocketAddr>>) -> Self {
        let entries = Mutex::new(HashMap::from([(host.to_string(), VecDeque::from(answers))]));
        self.with_lookup_override(Arc::new(move |lookup_host| {
            let mut entries = entries.lock().expect("test DNS overrides must not be poisoned");
            let answers = entries.get_mut(lookup_host)?;
            if answers.len() > 1 {
                answers.pop_front()
            } else {
                answers.front().cloned()
            }
        }))
    }
}

impl reqwest::dns::Resolve for OutboundDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().trim_end_matches('.').to_ascii_lowercase();
        let peer_kind = self.peer_kind;
        let allow_restricted =
            peer_kind == OutboundPeerKind::Target && self.allowed_restricted_host.as_deref() == Some(host.as_str());
        let lookup_override = self.lookup_override.clone();
        Box::pin(async move {
            let overridden = lookup_override.as_ref().and_then(|lookup| lookup(&host));

            let addresses = if let Some(addresses) = overridden {
                addresses
            } else {
                // Do not use the cached resolver in `crate::net`: every new connection must
                // classify the addresses returned at that boundary so DNS rebinding fails closed.
                tokio::net::lookup_host((host.as_str(), 0))
                    .await
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::NotFound, err))?
                    .collect()
            };
            if addresses.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("DNS resolution for '{host}' returned no addresses"),
                )
                .into());
            }
            let mut allow_origin_can_recover = false;
            let addrs = addresses
                .into_iter()
                .filter(|address| match validate_policy_ip(address.ip()) {
                    Ok(()) => true,
                    Err(reason) => match peer_kind {
                        OutboundPeerKind::Target => {
                            let recoverable = restricted_reason_can_be_overridden(reason);
                            allow_origin_can_recover |= recoverable;
                            allow_restricted && recoverable
                        }
                        OutboundPeerKind::Proxy => proxy_restricted_reason_allowed(reason),
                    },
                })
                .collect::<Vec<_>>();
            if addrs.is_empty() {
                let rejection = match peer_kind {
                    OutboundPeerKind::Target => OutboundDnsPolicyRejection::new(host, allow_origin_can_recover),
                    OutboundPeerKind::Proxy => OutboundDnsPolicyRejection::new_proxy(host),
                };
                return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, rejection).into());
            }
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

pub fn validate_outbound_url(url: &Url) -> Result<(), OutboundUrlError> {
    let Some(raw_host) = url.host_str() else {
        return Err(OutboundUrlError::MissingHost);
    };
    let normalized_host = normalize_host_str(raw_host);

    if normalized_host.eq_ignore_ascii_case("localhost") {
        return Err(OutboundUrlError::ForbiddenHost {
            host: raw_host.to_string(),
            reason: "loopback host",
        });
    }

    let Ok(ip) = normalized_host.parse::<IpAddr>() else {
        return Ok(());
    };

    validate_outbound_ip(ip).map_err(|reason| OutboundUrlError::ForbiddenHost {
        host: raw_host.to_string(),
        reason,
    })
}

/// Apply the literal URL guard while allowing private storage endpoints.
///
/// Known metadata services in IPv6 ULA space remain forbidden. This does not
/// resolve DNS or consult either outbound-origin or replication-loopback settings.
pub fn validate_outbound_url_allow_private(url: &Url) -> Result<(), OutboundUrlError> {
    match validate_outbound_url(url) {
        Err(
            err @ OutboundUrlError::ForbiddenHost {
                reason: "private address",
                ..
            },
        ) => {
            let ip = normalized_host(url).and_then(|host| host.parse::<IpAddr>().ok());
            if ip.is_some_and(|ip| !is_metadata_endpoint(ip)) {
                Ok(())
            } else {
                Err(err)
            }
        }
        result => result,
    }
}

fn validate_http_url_shape(url: &Url) -> Result<(), OutboundPolicyError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(OutboundPolicyError::UnsupportedScheme {
            scheme: url.scheme().to_string(),
        });
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(OutboundPolicyError::UserInfoNotAllowed);
    }
    Ok(())
}

fn normalize_host_str(host: &str) -> &str {
    host.trim_end_matches('.').trim_matches(['[', ']'])
}

fn classify_url_host(normalized_host: &str) -> Result<(), &'static str> {
    if normalized_host.eq_ignore_ascii_case("localhost") {
        Err("loopback host")
    } else if let Ok(ip) = normalized_host.parse::<IpAddr>() {
        validate_policy_ip(ip)
    } else {
        Ok(())
    }
}

fn normalized_host(url: &Url) -> Option<String> {
    url.host_str().map(|host| normalize_host_str(host).to_ascii_lowercase())
}

fn format_normalized_origin(scheme: &str, host: &str, is_ipv6: bool, port: u16) -> String {
    // `Url::origin()` preserves a trailing DNS dot, but the resolver treats dotted and undotted
    // names as the same host. Normalize it here so policy matching follows connection semantics.
    if is_ipv6 {
        format!("{scheme}://[{host}]:{port}")
    } else {
        format!("{scheme}://{host}:{port}")
    }
}

fn normalized_origin(url: &Url) -> Option<String> {
    validate_http_url_shape(url).ok()?;
    let host = normalized_host(url).filter(|host| !host.is_empty())?;
    let port = url.port_or_known_default()?;
    Some(format_normalized_origin(url.scheme(), &host, host.parse::<Ipv6Addr>().is_ok(), port))
}

fn restricted_reason_can_be_overridden(reason: &str) -> bool {
    matches!(
        reason,
        "loopback host" | "loopback address" | "private address" | "shared address" | "reserved address"
    )
}

fn proxy_restricted_reason_allowed(reason: &str) -> bool {
    matches!(reason, "loopback host" | "loopback address" | "private address" | "shared address")
}

fn validate_policy_ip(ip: IpAddr) -> Result<(), &'static str> {
    if is_metadata_endpoint(ip) {
        return Err("metadata endpoint");
    }

    let original_v6 = match ip {
        IpAddr::V6(v6) => Some(v6),
        IpAddr::V4(_) => None,
    };
    let ip = match original_v6 {
        Some(v6) => match policy_embedded_ipv4(v6) {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        None => ip,
    };

    if is_metadata_endpoint(ip) {
        return Err("metadata endpoint");
    }
    validate_outbound_ip(ip)?;
    if original_v6.is_some_and(|v6| v6.segments()[0] == 0x2002) {
        return Err("reserved address");
    }

    match ip {
        IpAddr::V4(ipv4) => {
            let octets = ipv4.octets();
            if octets[0] == 100 && (64..=127).contains(&octets[1]) {
                return Err("shared address");
            }
            if octets[0] == 0
                || octets[0] >= 224
                || (octets[0] == 192 && ((octets[1] == 0 && matches!(octets[2], 0 | 2)) || (octets[1] == 88 && octets[2] == 99)))
                || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
                || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
                || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
            {
                return Err("reserved address");
            }
        }
        IpAddr::V6(ipv6) => {
            let segments = ipv6.segments();
            if segments[0] == 0x0064 && segments[1] == 0xff9b && segments[2] == 1 {
                return Err("translation address");
            }
            if is_special_use_ipv6(ipv6) {
                return Err("reserved address");
            }
        }
    }

    Ok(())
}

fn is_special_use_ipv6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    let bits = u128::from_be_bytes(ip.octets());
    let ietf_protocol_assignment = segments[0] == 0x2001
        && segments[1] < 0x0200
        && bits != 0x2001_0001_0000_0000_0000_0000_0000_0001
        && bits != 0x2001_0001_0000_0000_0000_0000_0000_0002
        && segments[1] != 0x0003
        && !(segments[1] == 0x0004 && segments[2] == 0x0112)
        && !(0x0020..=0x003f).contains(&segments[1]);

    (segments[0] & 0xff00) == 0xff00
        || (segments[0] & 0xffc0) == 0xfec0
        || (segments[0] == 0x0100 && segments[1..4] == [0, 0, 0])
        || ietf_protocol_assignment
        || segments[0] == 0x2002
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || (segments[0] == 0x3fff && segments[1] <= 0x0fff)
        || segments[0] == 0x5f00
}

fn is_metadata_endpoint(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => matches!(
            ipv4.octets(),
            [169, 254, 169, 254]
                | [169, 254, 170, 2]
                | [169, 254, 170, 23]
                | [169, 254, 0, 23]
                | [100, 100, 100, 200]
                | [168, 63, 129, 16]
        ),
        IpAddr::V6(ipv6) => matches!(
            ipv6.segments(),
            [0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254]
                | [0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0023]
                | [0xfd20, 0x00ce, 0, 0, 0, 0, 0, 0x0254]
        ),
    }
}

fn policy_embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = embedded_ipv4(v6) {
        return Some(v4);
    }
    let segments = v6.segments();
    if segments[..6] == [0, 0, 0, 0, 0xffff, 0] {
        let hi = segments[6].to_be_bytes();
        let lo = segments[7].to_be_bytes();
        return Some(Ipv4Addr::new(hi[0], hi[1], lo[0], lo[1]));
    }
    let octets = v6.octets();
    if octets[..12] == [0x00, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0] {
        return Some(Ipv4Addr::new(octets[12], octets[13], octets[14], octets[15]));
    }
    if octets[0..2] == [0x20, 0x02] {
        return Some(Ipv4Addr::new(octets[2], octets[3], octets[4], octets[5]));
    }

    None
}

fn validate_outbound_ip(ip: IpAddr) -> Result<(), &'static str> {
    if let IpAddr::V6(v6) = ip {
        if v6.is_loopback() {
            return Err("loopback address");
        }
        if v6.is_unspecified() {
            return Err("unspecified address");
        }
        if v6.is_unicast_link_local() {
            return Err("link-local address");
        }
        if v6.is_unique_local() {
            return Err("private address");
        }
    }

    let ip = match ip {
        IpAddr::V6(v6) => match embedded_ipv4(v6) {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        other => other,
    };

    if ip.is_unspecified() {
        return Err("unspecified address");
    }
    if ip == IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)) {
        return Err("metadata endpoint");
    }
    if let IpAddr::V4(ipv4) = ip {
        if ipv4.is_loopback() {
            return Err("loopback address");
        }
        if ipv4.is_link_local() {
            return Err("link-local address");
        }
        if ipv4.is_private() {
            return Err("private address");
        }
    }
    Ok(())
}

fn embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = v6.to_ipv4_mapped() {
        return Some(v4);
    }
    let segments = v6.segments();
    if segments[0..6] == [0, 0, 0, 0, 0, 0] {
        let hi = segments[6].to_be_bytes();
        let lo = segments[7].to_be_bytes();
        let v4 = Ipv4Addr::new(hi[0], hi[1], lo[0], lo[1]);
        if !v4.is_unspecified() && v4 != Ipv4Addr::new(0, 0, 0, 1) {
            return Some(v4);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{
        OutboundClientError, OutboundDnsLookup, OutboundDnsResolver, OutboundPeerKind, OutboundPolicy, OutboundPolicyError,
        OutboundProxyMatcher, OutboundUrlError, find_outbound_dns_policy_rejection, should_bypass_proxy_for_url,
        validate_outbound_url, validate_outbound_url_allow_private,
    };
    use std::collections::{HashMap, VecDeque};
    use std::net::{IpAddr, SocketAddr};
    use std::sync::{Arc, Mutex};
    use url::Url;

    #[test]
    fn validate_outbound_url_allows_public_hostname() {
        let url = Url::parse("https://example.com/webhook").expect("public URL should parse");
        assert!(validate_outbound_url(&url).is_ok());
    }

    #[test]
    fn validate_outbound_url_preserves_unicode_host_policy() {
        for (endpoint, expected_host, allowed) in [
            ("https://bücher.example/webhook", "xn--bcher-kva.example", true),
            ("https://bu\u{0308}cher.example/webhook", "xn--bcher-kva.example", true),
            ("https://ｌｏｃａｌｈｏｓｔ/webhook", "localhost", false),
            ("http://１２７。０。０。１/hook", "127.0.0.1", false),
        ] {
            let url = Url::parse(endpoint).expect("Unicode hostname URL should parse");
            assert_eq!(url.host_str(), Some(expected_host), "{endpoint}");
            if allowed {
                validate_outbound_url(&url).expect("public IDN hostname should be allowed");
            } else {
                assert!(
                    matches!(validate_outbound_url(&url), Err(OutboundUrlError::ForbiddenHost { .. })),
                    "normalized loopback hostname should be rejected: {endpoint}"
                );
            }
        }
    }

    #[test]
    fn private_storage_endpoints_are_allowed_without_relaxing_the_global_guard() {
        for endpoint in [
            "http://10.0.0.1:9000",
            "http://172.16.0.1:9000",
            "https://192.168.10.200:9443",
            "http://[fd12:3456::1]:9000",
            "http://[::ffff:192.168.10.200]:9000",
            "http://[::192.168.10.200]:9000",
        ] {
            let url = Url::parse(endpoint).expect("private endpoint should parse");
            assert!(validate_outbound_url_allow_private(&url).is_ok(), "{endpoint}");
            assert!(validate_outbound_url(&url).is_err(), "global guard must still reject {endpoint}");
        }
    }

    #[test]
    fn private_storage_exception_preserves_forbidden_endpoints() {
        for endpoint in [
            "http://localhost:9000",
            "http://localhost.:9000",
            "http://127.0.0.1:9000",
            "http://[::1]:9000",
            "http://[::ffff:127.0.0.1]:9000",
            "http://169.254.1.1:9000",
            "http://[fe80::1]:9000",
            "http://0.0.0.0:9000",
            "http://[::]:9000",
            "http://169.254.169.254",
            "http://[::ffff:169.254.169.254]",
            "http://[::169.254.169.254]",
            "http://[fd00:ec2::254]",
            "http://[fd00:ec2::23]",
            "http://[fd20:ce::254]",
            "rustfs://",
        ] {
            let url = Url::parse(endpoint).expect("forbidden endpoint should parse");
            let original = validate_outbound_url(&url).expect_err("original guard must reject the fixture");
            assert_eq!(
                validate_outbound_url_allow_private(&url).expect_err("private exception must retain the rejection"),
                original,
                "{endpoint}"
            );
        }
    }

    #[test]
    fn private_storage_exception_preserves_public_endpoints() {
        for endpoint in [
            "https://tier.example.com:9443",
            "https://8.8.8.8",
            "https://[2606:4700:4700::1111]",
        ] {
            let url = Url::parse(endpoint).expect("public endpoint should parse");
            assert!(validate_outbound_url_allow_private(&url).is_ok(), "{endpoint}");
        }
    }

    #[test]
    fn validate_outbound_url_rejects_localhost() {
        let url = Url::parse("https://localhost/webhook").expect("localhost URL should parse");
        let err = validate_outbound_url(&url).expect_err("localhost should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback host",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_loopback_ip() {
        let url = Url::parse("https://127.0.0.1/webhook").expect("loopback URL should parse");
        let err = validate_outbound_url(&url).expect_err("loopback IP should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback address",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_alternate_ipv4_loopback_syntax() {
        for endpoint in ["http://2130706433/hook", "http://0177.0.0.1/hook", "http://0x7f000001/hook"] {
            let url = Url::parse(endpoint).expect("alternate IPv4 URL should parse");
            assert!(
                validate_outbound_url(&url).is_err(),
                "alternate loopback syntax must be rejected: {endpoint}"
            );
        }
    }

    #[test]
    fn validate_outbound_url_rejects_private_ip() {
        let url = Url::parse("https://10.0.0.5/webhook").expect("private URL should parse");
        let err = validate_outbound_url(&url).expect_err("private IP should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "private address",
                ..
            }
        ));
    }

    #[test]
    fn outbound_policy_rejects_special_use_addresses() {
        let policy = OutboundPolicy::default();
        for endpoint in [
            "http://0.0.0.1/hook",
            "http://192.0.2.1/hook",
            "http://198.18.0.1/hook",
            "http://198.51.100.1/hook",
            "http://203.0.113.1/hook",
            "http://224.0.0.1/hook",
            "http://[100::1]/hook",
            "http://[2001:100::1]/hook",
            "http://[2001:db8::1]/hook",
            "http://[3fff::1]/hook",
            "http://[5f00::1]/hook",
            "http://[ff02::1]/hook",
        ] {
            let url = Url::parse(endpoint).expect("special-use URL should parse");
            assert!(policy.validate_url(&url).is_err(), "special-use address must be rejected: {endpoint}");
        }
    }

    #[test]
    fn validate_outbound_url_preserves_legacy_shared_address_behavior() {
        let url = Url::parse("http://100.64.0.5/hook").expect("shared URL should parse");
        assert!(validate_outbound_url(&url).is_ok());
        assert!(OutboundPolicy::default().validate_url(&url).is_err());
    }

    #[test]
    fn validate_outbound_url_rejects_metadata_endpoint() {
        let url = Url::parse("http://169.254.169.254/latest/meta-data").expect("metadata URL should parse");
        let err = validate_outbound_url(&url).expect_err("metadata endpoint should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "metadata endpoint",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_link_local_ipv6() {
        let url = Url::parse("https://[fe80::1]/hook").expect("IPv6 URL should parse");
        let err = validate_outbound_url(&url).expect_err("link-local IPv6 should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "link-local address",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_ipv4_mapped_loopback() {
        let url = Url::parse("http://[::ffff:127.0.0.1]/webhook").expect("mapped loopback URL should parse");
        let err = validate_outbound_url(&url).expect_err("IPv4-mapped loopback should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback address",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_ipv4_mapped_private() {
        let url = Url::parse("http://[::ffff:10.0.0.5]/webhook").expect("mapped private URL should parse");
        let err = validate_outbound_url(&url).expect_err("IPv4-mapped private should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "private address",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_ipv4_mapped_metadata_endpoint() {
        let url = Url::parse("http://[::ffff:169.254.169.254]/latest/meta-data").expect("mapped metadata URL should parse");
        let err = validate_outbound_url(&url).expect_err("IPv4-mapped metadata endpoint should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "metadata endpoint",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_still_allows_public_ipv6() {
        // Pure public IPv6 (Google DNS) must remain allowed after normalization.
        let url = Url::parse("https://[2001:4860:4860::8888]/webhook").expect("public IPv6 URL should parse");
        assert!(validate_outbound_url(&url).is_ok());
    }

    #[test]
    fn validate_outbound_url_rejects_ipv4_compatible_loopback() {
        // IPv4-compatible form ::a.b.c.d (deprecated but still routable) must also be caught.
        let url = Url::parse("http://[::127.0.0.1]/webhook").expect("compatible loopback URL should parse");
        let err = validate_outbound_url(&url).expect_err("IPv4-compatible loopback should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback address",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_ipv4_compatible_metadata_endpoint() {
        let url = Url::parse("http://[::169.254.169.254]/latest/meta-data").expect("compatible metadata URL should parse");
        let err = validate_outbound_url(&url).expect_err("IPv4-compatible metadata endpoint should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "metadata endpoint",
                ..
            }
        ));
    }

    #[test]
    fn outbound_policy_rejects_embedded_private_destinations() {
        let policy = OutboundPolicy::default();
        for endpoint in [
            "http://[64:ff9b::7f00:1]/hook",
            "http://[64:ff9b::a00:1]/hook",
            "http://[64:ff9b::a9fe:a9fe]/latest/meta-data",
            "http://[2002:7f00:1::]/hook",
            "http://[2002:a00:1::]/hook",
            "http://[::ffff:0:7f00:1]/hook",
            "http://[::ffff:0:a9fe:a9fe]/latest/meta-data",
        ] {
            let url = Url::parse(endpoint).expect("embedded IPv4 URL should parse");
            assert!(
                policy.validate_url(&url).is_err(),
                "embedded private destination must be rejected: {endpoint}"
            );
        }
    }

    #[test]
    fn validate_outbound_url_rejects_ipv6_loopback_and_unspecified() {
        // ::1 / :: must stay rejected even though they look like IPv4-compatible forms.
        let err = validate_outbound_url(&Url::parse("http://[::1]/x").unwrap()).expect_err("::1 rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback address",
                ..
            }
        ));
        let err = validate_outbound_url(&Url::parse("http://[::]/x").unwrap()).expect_err(":: rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "unspecified address",
                ..
            }
        ));
    }

    #[test]
    fn outbound_policy_allows_only_the_exact_operator_origin() {
        let policy = OutboundPolicy::from_allowed_origins("http://127.0.0.1:9443").expect("operator origin should parse");

        assert!(
            policy
                .validate_url(&Url::parse("http://127.0.0.1:9443/hook").expect("allowed URL"))
                .is_ok()
        );
        assert!(
            policy
                .validate_url(&Url::parse("http://127.0.0.1:9444/hook").expect("wrong-port URL"))
                .is_err(),
            "an allowlisted origin must not authorize another port"
        );
        assert!(
            policy
                .validate_url(&Url::parse("http://[::1]:9443/hook").expect("different-host URL"))
                .is_err(),
            "an allowlisted origin must not authorize a different host"
        );

        let shared_policy =
            OutboundPolicy::from_allowed_origins("http://100.64.0.5:9443").expect("shared operator origin should parse");
        assert!(
            shared_policy
                .validate_url(&Url::parse("http://100.64.0.5:9443/hook").expect("shared URL"))
                .is_ok(),
            "an exact operator origin may opt into an internal shared address"
        );
    }

    #[test]
    fn outbound_policy_never_allows_metadata_or_unspecified_addresses() {
        let policy = OutboundPolicy::from_allowed_origins(
            "http://169.254.1.10,http://169.254.169.254,http://169.254.170.2,http://169.254.170.23,http://169.254.0.23,http://100.100.100.200,http://168.63.129.16,http://[fd00:ec2::254],http://[fd00:ec2::23],http://[fd20:ce::254],http://[64:ff9b:1::1],http://0.0.0.0",
        )
        .expect("syntactically valid origins should parse");

        for endpoint in [
            "http://169.254.1.10/hook",
            "http://169.254.169.254/latest",
            "http://169.254.170.2/credentials",
            "http://169.254.170.23/v1/credentials",
            "http://169.254.0.23/latest/meta-data",
            "http://100.100.100.200/latest/meta-data",
            "http://168.63.129.16/machine?comp=goalstate",
            "http://[fd00:ec2::254]/latest",
            "http://[fd00:ec2::23]/v1/credentials",
            "http://[fd20:ce::254]/computeMetadata/v1",
            "http://[64:ff9b:1::1]/hook",
            "http://0.0.0.0/hook",
        ] {
            assert!(
                policy
                    .validate_url(&Url::parse(endpoint).expect("test endpoint should parse"))
                    .is_err(),
                "endpoint must remain forbidden: {endpoint}"
            );
        }
    }

    #[test]
    fn outbound_policy_rejects_non_http_and_userinfo_origins() {
        for origin in [
            "ftp://webhook.internal",
            "https://user@webhook.internal",
            "https://webhook.internal/path",
        ] {
            assert!(
                OutboundPolicy::from_allowed_origins(origin).is_err(),
                "invalid operator origin must fail closed: {origin}"
            );
        }
        assert!(OutboundPolicy::from_allowed_origins("https://one.example,,https://two.example").is_err());
    }

    #[test]
    fn outbound_policy_rejects_non_http_and_userinfo_targets() {
        let policy = OutboundPolicy::default();
        for endpoint in ["ftp://webhook.test/hook", "https://user:secret@webhook.test/hook"] {
            assert!(
                policy
                    .validate_url(&Url::parse(endpoint).expect("target URL should parse"))
                    .is_err(),
                "target must be rejected: {endpoint}"
            );
        }
    }

    #[tokio::test]
    async fn outbound_dns_resolver_filters_rebinding_and_mixed_answers() {
        let policy = OutboundPolicy::default();
        let endpoint = Url::parse("https://webhook.test/hook").expect("endpoint should parse");
        let resolver = policy
            .resolver_for(&endpoint)
            .expect("public hostname should be accepted")
            .with_overrides(HashMap::from([
                (
                    "webhook.test".to_string(),
                    vec![
                        "10.0.0.5".parse().expect("private IP"),
                        "100.64.0.5".parse().expect("shared IP"),
                        "100.100.100.200".parse().expect("metadata IP"),
                        "8.8.8.8".parse().expect("public IP"),
                        "::ffff:127.0.0.1".parse().expect("mapped loopback IP"),
                    ],
                ),
                ("rebound.test".to_string(), vec!["127.0.0.1".parse().expect("loopback IP")]),
            ]));

        let addrs = reqwest::dns::Resolve::resolve(&resolver, "webhook.test".parse().expect("resolver hostname"))
            .await
            .expect("one public address should remain")
            .map(|addr| addr.ip())
            .collect::<Vec<_>>();
        assert_eq!(addrs, vec!["8.8.8.8".parse::<std::net::IpAddr>().expect("public IP")]);
        let error = match reqwest::dns::Resolve::resolve(&resolver, "rebound.test".parse().expect("resolver hostname")).await {
            Ok(_) => panic!("a rebound answer containing only restricted addresses must fail closed"),
            Err(error) => error,
        };
        let io_error = error
            .downcast_ref::<std::io::Error>()
            .expect("resolver must preserve its PermissionDenied root error");
        assert_eq!(io_error.kind(), std::io::ErrorKind::PermissionDenied);
        let rejection =
            find_outbound_dns_policy_rejection(&*error).expect("typed policy rejection must remain in the error chain");
        assert_eq!(rejection.host, "rebound.test");
        assert!(rejection.allow_origin_can_recover());
    }

    #[tokio::test]
    async fn outbound_dns_resolver_does_not_classify_empty_answers_as_policy_rejections() {
        let endpoint = Url::parse("https://empty.test/hook").expect("endpoint should parse");
        let resolver = OutboundPolicy::default()
            .resolver_for(&endpoint)
            .expect("public hostname should be accepted")
            .with_overrides(HashMap::from([("empty.test".to_string(), Vec::new())]));

        let error = match reqwest::dns::Resolve::resolve(&resolver, "empty.test".parse().expect("resolver hostname")).await {
            Ok(_) => panic!("an empty DNS answer must fail"),
            Err(error) => error,
        };
        let io_error = error
            .downcast_ref::<std::io::Error>()
            .expect("resolver errors must remain I/O errors");

        assert_eq!(io_error.kind(), std::io::ErrorKind::NotFound);
        assert!(find_outbound_dns_policy_rejection(&*error).is_none());
    }

    #[tokio::test]
    async fn exact_operator_origin_allows_private_dns_answers_for_that_host_only() {
        let policy = OutboundPolicy::from_allowed_origins("https://webhook.test:9443").expect("operator origin should parse");
        let resolver = policy
            .resolver_for(&Url::parse("https://webhook.test:9443/hook").expect("endpoint should parse"))
            .expect("allowlisted endpoint should be accepted")
            .with_overrides(HashMap::from([
                ("webhook.test".to_string(), vec!["10.0.0.5".parse().expect("private IP")]),
                ("other.test".to_string(), vec!["10.0.0.6".parse().expect("private IP")]),
            ]));

        let allowed = reqwest::dns::Resolve::resolve(&resolver, "webhook.test".parse().expect("resolver hostname"))
            .await
            .expect("allowlisted host should resolve")
            .count();
        assert_eq!(allowed, 1);
        assert!(
            reqwest::dns::Resolve::resolve(&resolver, "other.test".parse().expect("resolver hostname"))
                .await
                .is_err(),
            "the allowlist must not authorize another private hostname"
        );
    }

    #[tokio::test]
    async fn exact_operator_origin_never_allows_metadata_dns_answers() {
        let policy = OutboundPolicy::from_allowed_origins("https://webhook.test:9443").expect("operator origin should parse");
        let resolver = policy
            .resolver_for(&Url::parse("https://webhook.test:9443/hook").expect("endpoint should parse"))
            .expect("allowlisted endpoint should be accepted")
            .with_overrides(HashMap::from([(
                "webhook.test".to_string(),
                vec![
                    "169.254.170.23".parse().expect("EKS credential IP"),
                    "169.254.0.23".parse().expect("Tencent metadata IP"),
                    "fd00:ec2::23".parse().expect("EKS credential IPv6"),
                    "fd20:ce::254".parse().expect("GCP metadata IPv6"),
                    "0.0.0.0".parse().expect("unspecified IP"),
                ],
            )]));

        assert!(
            reqwest::dns::Resolve::resolve(&resolver, "webhook.test".parse().expect("resolver hostname"))
                .await
                .is_err(),
            "metadata and unspecified answers must remain forbidden for an allowlisted origin"
        );
    }

    #[tokio::test]
    async fn outbound_dns_resolver_preserves_ipv6_scope_id() {
        use std::net::SocketAddrV6;

        let policy = OutboundPolicy::default();
        let scoped = SocketAddr::V6(SocketAddrV6::new("2001:4860:4860::8888".parse().expect("public IPv6"), 0, 0, 7));
        let resolver = policy
            .resolver_for(&Url::parse("https://webhook.test:9443/hook").expect("endpoint should parse"))
            .expect("public endpoint should be accepted")
            .with_override_sequence("webhook.test", vec![vec![scoped]]);

        let resolved = reqwest::dns::Resolve::resolve(&resolver, "webhook.test".parse().expect("resolver hostname"))
            .await
            .expect("public scoped address should resolve")
            .collect::<Vec<_>>();
        assert_eq!(resolved, vec![scoped]);
    }

    #[tokio::test]
    async fn reqwest_rechecks_dns_policy_for_each_new_connection() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind first connection listener");
        let address = listener.local_addr().expect("first connection address");
        let endpoint = Url::parse(&format!("http://rebind.test:{}/hook", address.port())).expect("endpoint should parse");
        let policy = OutboundPolicy::from_allowed_origins(&endpoint.origin().ascii_serialization())
            .expect("loopback test origin should be explicitly allowed");
        let resolver = policy
            .resolver_for(&endpoint)
            .expect("allowlisted endpoint should be accepted")
            .with_override_sequence(
                "rebind.test",
                vec![
                    vec![SocketAddr::new(address.ip(), 0)],
                    vec![SocketAddr::new("169.254.169.254".parse().expect("metadata IP"), 0)],
                ],
            );
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept first connection");
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await.expect("read first request");
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("write first response");
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(resolver)
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .expect("build test client");

        assert_eq!(
            client
                .get(endpoint.clone())
                .send()
                .await
                .expect("first request should connect")
                .status(),
            reqwest::StatusCode::NO_CONTENT
        );
        server.await.expect("first connection server should finish");
        let error = client
            .get(endpoint)
            .send()
            .await
            .expect_err("the second connection must reject a metadata DNS answer");
        let mut error_chain = vec![error.to_string()];
        let mut source = std::error::Error::source(&error);
        while let Some(error) = source {
            error_chain.push(error.to_string());
            source = error.source();
        }
        assert!(
            error_chain
                .iter()
                .any(|message| message.contains("returned no allowed addresses")),
            "request must fail in the DNS policy layer: {error_chain:?}"
        );
    }

    #[tokio::test]
    async fn reqwest_preserves_recoverable_private_dns_policy_rejection() {
        let endpoint = Url::parse("http://keycloak.internal:8080/realms/rustfs").expect("endpoint should parse");
        let resolver = OutboundPolicy::default()
            .resolver_for(&endpoint)
            .expect("hostname should pass the URL-shape check")
            .with_overrides(HashMap::from([(
                "keycloak.internal".to_string(),
                vec![
                    "10.96.0.20".parse().expect("private IP"),
                    "169.254.169.254".parse().expect("metadata IP"),
                ],
            )]));
        let client = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(resolver)
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .expect("test client should build");

        let error = client
            .get(endpoint)
            .send()
            .await
            .expect_err("private DNS answer should be rejected before connecting");
        let rejection = find_outbound_dns_policy_rejection(&error).expect("typed DNS policy rejection should be preserved");
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        let mut io_kind = None;
        while let Some(source) = current {
            if let Some(io_error) = source.downcast_ref::<std::io::Error>() {
                io_kind = Some(io_error.kind());
                break;
            }
            current = source.source();
        }

        assert_eq!(rejection.host, "keycloak.internal");
        assert!(rejection.allow_origin_can_recover());
        assert_eq!(io_kind, Some(std::io::ErrorKind::PermissionDenied));
    }

    #[tokio::test]
    async fn reqwest_preserves_nonrecoverable_metadata_dns_policy_rejection() {
        let endpoint = Url::parse("http://metadata.internal/latest").expect("endpoint should parse");
        let resolver = OutboundPolicy::default()
            .resolver_for(&endpoint)
            .expect("hostname should pass the URL-shape check")
            .with_overrides(HashMap::from([(
                "metadata.internal".to_string(),
                vec!["169.254.169.254".parse().expect("metadata IP")],
            )]));
        let client = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(resolver)
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .expect("test client should build");

        let error = client
            .get(endpoint)
            .send()
            .await
            .expect_err("metadata DNS answer should be rejected before connecting");
        let rejection = find_outbound_dns_policy_rejection(&error).expect("typed DNS policy rejection should be preserved");

        assert_eq!(rejection.host(), "metadata.internal");
        assert_eq!(rejection.peer_kind(), OutboundPeerKind::Target);
        assert!(!rejection.allow_origin_can_recover());
    }

    #[test]
    fn should_bypass_proxy_for_url_matches_loopback_hosts_and_literals() {
        for loopback_url in [
            "http://localhost:9000/.well-known/openid-configuration",
            "https://LOCALHOST.:9000/.well-known/openid-configuration",
            "http://127.0.0.1:9000/.well-known/openid-configuration",
            "http://127.0.0.42:9000/.well-known/openid-configuration",
            "http://0x7f000001:9000/.well-known/openid-configuration",
            "http://[::1]:9000/.well-known/openid-configuration",
            "http://[::ffff:127.0.0.1]:9000/.well-known/openid-configuration",
            "http://[::127.0.0.1]:9000/.well-known/openid-configuration",
        ] {
            let url = Url::parse(loopback_url).expect("loopback URL should parse");
            assert!(should_bypass_proxy_for_url(&url), "expected proxy bypass for {loopback_url}");
        }

        for non_loopback_url in [
            "https://sso-sit.intra.kommunale.it/.well-known/openid-configuration",
            "http://sub.localhost:9000/.well-known/openid-configuration",
            "http://10.0.0.1:9000/.well-known/openid-configuration",
            "http://100.64.0.1:9000/.well-known/openid-configuration",
            "http://169.254.169.254/latest/meta-data",
            "http://[::]:9000/",
        ] {
            let url = Url::parse(non_loopback_url).expect("non-loopback URL should parse");
            assert!(!should_bypass_proxy_for_url(&url), "unexpected proxy bypass for {non_loopback_url}");
        }
    }

    #[test]
    fn proxy_policy_allows_private_shared_and_loopback_hops_while_still_validating_target_url() {
        let policy = OutboundPolicy::default();
        let target =
            Url::parse("https://sso-sit.intra.kommunale.it/.well-known/openid-configuration").expect("target URL should parse");

        for proxy_url in [
            "http://bb-proxy.intra.kommunale.it:8080",
            "http://bb-proxy.intra.kommunale.it:8080/",
            "bb-proxy.intra.kommunale.it:8080",
            "https://bb-proxy.intra.kommunale.it:8443",
            "http://10.0.0.1:8080",
            "http://172.16.0.1:8080",
            "http://192.168.1.10:8080",
            "http://100.64.0.10:8080",
            "http://[fd12:3456::1]:8080",
            "http://127.0.0.1:8080",
            "http://[::1]:8080",
            "http://localhost:8080",
            "http://localhost.:8080",
            "http://8.8.8.8:8080",
        ] {
            let matcher = OutboundProxyMatcher::all(proxy_url);
            policy
                .configure_reqwest_client_with_proxy_matcher(&target, &matcher, reqwest::Client::builder())
                .unwrap_or_else(|err| panic!("proxy {proxy_url} should be accepted: {err}"))
                .build()
                .expect("accepted proxy client should build");
        }

        let valid_proxy = OutboundProxyMatcher::all("http://bb-proxy.intra.kommunale.it:8080");
        for (forbidden_target, expected_recoverable) in [
            ("http://169.254.169.254/latest/meta-data", false),
            ("http://127.0.0.1:8080/.well-known/openid-configuration", true),
            ("http://10.0.0.5:8080/.well-known/openid-configuration", true),
            ("ftp://sso-sit.intra.kommunale.it/resource", false),
            ("https://user:secret@sso-sit.intra.kommunale.it/.well-known/openid-configuration", false),
        ] {
            let url = Url::parse(forbidden_target).expect("forbidden target URL should parse");
            let policy_err = policy.validate_url(&url).expect_err("target URL must fail policy");
            assert_eq!(
                policy_err.allow_origin_can_recover(),
                expected_recoverable,
                "unexpected allow_origin_can_recover for {forbidden_target}: {policy_err:?}"
            );

            let err = policy
                .configure_reqwest_client_with_proxy_matcher(&url, &valid_proxy, reqwest::Client::builder())
                .expect_err("target URL policy must run before proxy configuration");
            assert!(!err.is_proxy_error(), "target rejection misclassified as proxy error: {err:?}");
            assert_eq!(err.allow_origin_can_recover(), expected_recoverable);
            assert_eq!(err, OutboundClientError::Policy(policy_err.clone()));
            assert_eq!(
                std::error::Error::source(&err).and_then(|src| src.downcast_ref::<OutboundPolicyError>()),
                Some(&policy_err)
            );
        }
    }

    #[test]
    fn proxy_policy_rejects_metadata_link_local_unspecified_reserved_and_invalid_proxy_urls() {
        let policy = OutboundPolicy::default();
        let target =
            Url::parse("https://sso-sit.intra.kommunale.it/.well-known/openid-configuration").expect("target URL should parse");

        for (proxy_url, expected_reason) in [
            ("http://169.254.169.254:8080", "metadata endpoint"),
            ("http://0xa9fea9fe:8080", "metadata endpoint"),
            ("http://0251.0376.0251.0376:8080", "metadata endpoint"),
            ("http://2852039166:8080", "metadata endpoint"),
            ("http://[::ffff:169.254.169.254]:8080", "metadata endpoint"),
            ("http://[::169.254.169.254]:8080", "metadata endpoint"),
            ("http://100.100.100.200:8080", "metadata endpoint"),
            ("http://168.63.129.16:8080", "metadata endpoint"),
            ("http://[fd00:ec2::254]:8080", "metadata endpoint"),
            ("http://[fd00:ec2::23]:8080", "metadata endpoint"),
            ("http://[fd20:ce::254]:8080", "metadata endpoint"),
            ("http://169.254.1.1:8080", "link-local address"),
            ("http://[fe80::1]:8080", "link-local address"),
            ("http://0.0.0.0:8080", "unspecified address"),
            ("http://[::]:8080", "unspecified address"),
            ("http://[64:ff9b:1::1]:8080", "translation address"),
            ("http://224.0.0.1:8080", "reserved address"),
            ("http://[ff02::1]:8080", "reserved address"),
            ("http://192.0.2.1:8080", "reserved address"),
            ("http://[2001:db8::1]:8080", "reserved address"),
            ("http://0.0.0.1:8080", "reserved address"),
        ] {
            let matcher = OutboundProxyMatcher::all(proxy_url);
            let err = policy
                .configure_reqwest_client_with_proxy_matcher(&target, &matcher, reqwest::Client::builder())
                .expect_err("forbidden proxy literal must be rejected");
            assert!(err.is_proxy_error(), "expected proxy error for {proxy_url}: {err:?}");
            assert!(!err.allow_origin_can_recover());
            assert!(std::error::Error::source(&err).is_none());
            assert!(
                matches!(
                    err,
                    OutboundClientError::ForbiddenProxyHost { reason, .. } if reason == expected_reason
                ),
                "unexpected error for {proxy_url}: {err:?}"
            );
            assert!(
                err.to_string().starts_with("outbound proxy host '"),
                "unexpected display for {proxy_url}: {err}"
            );
        }

        let unescaped_at = OutboundProxyMatcher::all("http://user:p@ss@proxy.internal:8080");
        let err = policy
            .configure_reqwest_client_with_proxy_matcher(&target, &unescaped_at, reqwest::Client::builder())
            .expect_err("proxy URL leaving residual userinfo must be rejected");
        assert_eq!(err, OutboundClientError::InvalidProxyUrl);
        assert!(err.is_proxy_error());
        assert!(!err.allow_origin_can_recover());
        assert!(std::error::Error::source(&err).is_none());
    }

    #[test]
    fn proxy_matcher_normalizes_http_paths_ignores_noise_and_rejects_matched_socks() {
        let policy = OutboundPolicy::default();
        let target =
            Url::parse("https://sso-sit.intra.kommunale.it/.well-known/openid-configuration").expect("target URL should parse");

        // HTTP(S) proxy URLs with trailing paths/queries are normalized by hyper-util like curl/reqwest.
        for normalized_proxy in ["http://10.0.0.1:8080/ignored-path", "http://10.0.0.1:8080/ignored-path?pac=1"] {
            let matcher = OutboundProxyMatcher::from_env_values(Some(normalized_proxy), Some(normalized_proxy), None, None);
            policy
                .configure_reqwest_client_with_proxy_matcher(&target, &matcher, reqwest::Client::builder())
                .unwrap_or_else(|err| panic!("proxy with path {normalized_proxy} should be normalized: {err}"))
                .build()
                .expect("normalized proxy client should build");
        }

        // Unparseable or non-proxy-scheme env noise is ignored like standard reqwest rather than crashing requests.
        for ignored_noise in [":::invalid", "://bad", "http://[::1", "ftp://proxy.internal:21"] {
            let matcher = OutboundProxyMatcher::all(ignored_noise);
            policy
                .configure_reqwest_client_with_proxy_matcher(&target, &matcher, reqwest::Client::builder())
                .unwrap_or_else(|err| panic!("noise proxy {ignored_noise} should fall back to direct mode: {err}"))
                .build()
                .expect("direct fallback client should build");
        }

        // SOCKS proxies fail closed when matched, and are ignored when bypassed by NO_PROXY.
        for (socks_proxy, expected_scheme) in [
            ("socks4://proxy.internal:1080", "socks4"),
            ("socks4a://proxy.internal:1080", "socks4a"),
            ("socks5://proxy.internal:1080", "socks5"),
            ("socks5h://proxy.internal:1080", "socks5h"),
        ] {
            let matched = OutboundProxyMatcher::all(socks_proxy);
            let err = policy
                .configure_reqwest_client_with_proxy_matcher(&target, &matched, reqwest::Client::builder())
                .expect_err("matched SOCKS proxy must fail closed");
            assert_eq!(
                err,
                OutboundClientError::UnsupportedProxyScheme {
                    scheme: expected_scheme.to_string(),
                }
            );
            assert!(err.is_proxy_error());

            let bypassed = OutboundProxyMatcher::from_env_values(None, None, Some(socks_proxy), Some(".intra.kommunale.it"));
            policy
                .configure_reqwest_client_with_proxy_matcher(&target, &bypassed, reqwest::Client::builder())
                .expect("SOCKS proxy bypassed by NO_PROXY should connect directly")
                .build()
                .expect("bypassed direct client should build");
        }
    }

    #[tokio::test]
    async fn proxy_dns_resolver_allows_private_and_rejects_metadata_or_rebound_answers() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let mixed_resolver = OutboundDnsResolver::for_proxy().with_overrides(HashMap::from([(
            "bb-proxy.intra.kommunale.it".to_string(),
            vec![
                "10.20.30.40".parse().expect("private IP"),
                "100.64.1.2".parse().expect("CGNAT IP"),
                "127.0.0.1".parse().expect("loopback IP"),
                "169.254.169.254".parse().expect("metadata IP"),
                "100.100.100.200".parse().expect("Alibaba metadata IP"),
                "fd00:ec2::254".parse().expect("AWS IPv6 metadata IP"),
                "fe80::1".parse().expect("link-local IPv6"),
                "0.0.0.0".parse().expect("unspecified IP"),
                "224.0.0.1".parse().expect("multicast IP"),
            ],
        )]));

        let resolved =
            reqwest::dns::Resolve::resolve(&mixed_resolver, "bb-proxy.intra.kommunale.it".parse().expect("proxy hostname"))
                .await
                .expect("private, shared, and loopback proxy addresses should remain")
                .map(|addr| addr.ip())
                .collect::<Vec<_>>();
        assert_eq!(
            resolved,
            vec![
                "10.20.30.40".parse::<IpAddr>().expect("private IP"),
                "100.64.1.2".parse::<IpAddr>().expect("CGNAT IP"),
                "127.0.0.1".parse::<IpAddr>().expect("loopback IP"),
            ]
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock proxy listener");
        let proxy_addr = listener.local_addr().expect("mock proxy local addr");
        let proxy_server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept first proxied connection");
            let mut buf = [0_u8; 1024];
            let _ = stream.read(&mut buf).await.expect("read first proxied request");
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("write first proxied response");
        });

        let sequence = Mutex::new(VecDeque::from([
            vec![SocketAddr::new(proxy_addr.ip(), 0)],
            vec![SocketAddr::new("169.254.169.254".parse().expect("metadata IP"), 0)],
        ]));
        let dns_override: OutboundDnsLookup = Arc::new(move |host| {
            if host != "bb-proxy.intra.kommunale.it" {
                return None;
            }
            let mut guard = sequence.lock().expect("DNS sequence lock");
            if guard.len() > 1 {
                guard.pop_front()
            } else {
                guard.front().cloned()
            }
        });

        let target = Url::parse("http://sso-sit.intra.kommunale.it/realms/rustfs").expect("target URL should parse");
        let matcher = OutboundProxyMatcher::all(&format!("http://bb-proxy.intra.kommunale.it:{}", proxy_addr.port()));
        let client = OutboundPolicy::default()
            .configure_reqwest_client_for_test(
                &target,
                &matcher,
                Some(dns_override),
                reqwest::Client::builder().timeout(std::time::Duration::from_secs(2)),
            )
            .expect("proxy client configuration should succeed")
            .build()
            .expect("proxy client should build");

        let first = client
            .get(target.clone())
            .send()
            .await
            .expect("first request through private/loopback proxy should succeed");
        assert_eq!(first.status(), reqwest::StatusCode::NO_CONTENT);
        proxy_server.await.expect("mock proxy task should finish");

        let err = client
            .get(target)
            .send()
            .await
            .expect_err("rebound metadata DNS answer for proxy must fail closed");
        let rejection = find_outbound_dns_policy_rejection(&err).expect("typed proxy DNS rejection must be preserved");
        assert_eq!(rejection.host(), "bb-proxy.intra.kommunale.it");
        assert_eq!(rejection.peer_kind(), OutboundPeerKind::Proxy);
        assert!(!rejection.allow_origin_can_recover());
        assert_eq!(
            rejection.to_string(),
            "outbound DNS resolution for outbound proxy 'bb-proxy.intra.kommunale.it' returned no allowed addresses"
        );
    }

    #[tokio::test]
    async fn no_proxy_falls_back_to_direct_mode_and_enforces_target_dns_policy() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let matcher = OutboundProxyMatcher::from_env_values(
            None,
            None,
            Some("http://bb-proxy.intra.kommunale.it:8080"),
            Some(".intra.kommunale.it"),
        );
        let private_dns: OutboundDnsLookup = Arc::new(|host| match host {
            "sso-sit.intra.kommunale.it" => Some(vec![SocketAddr::new("10.96.0.20".parse().expect("private IP"), 0)]),
            _ => None,
        });

        for raw_target in [
            "https://sso-sit.intra.kommunale.it/.well-known/openid-configuration",
            "https://sso-sit.intra.kommunale.it./.well-known/openid-configuration",
        ] {
            let target = Url::parse(raw_target).expect("target URL should parse");
            let client = OutboundPolicy::default()
                .configure_reqwest_client_for_test(
                    &target,
                    &matcher,
                    Some(private_dns.clone()),
                    reqwest::Client::builder().timeout(std::time::Duration::from_secs(2)),
                )
                .expect("NO_PROXY target should configure direct resolver")
                .build()
                .expect("direct client should build");

            let err = client
                .get(target)
                .send()
                .await
                .expect_err("direct NO_PROXY fallback must reject unallowlisted private target DNS");
            let rejection = find_outbound_dns_policy_rejection(&err).expect("target DNS rejection should be preserved");
            assert_eq!(rejection.host(), "sso-sit.intra.kommunale.it");
            assert_eq!(rejection.peer_kind(), OutboundPeerKind::Target);
            assert!(rejection.allow_origin_can_recover());
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind direct target listener");
        let target_addr = listener.local_addr().expect("direct target local addr");
        let direct_server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept direct connection");
            let mut buf = [0_u8; 1024];
            let _ = stream.read(&mut buf).await.expect("read direct request");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .expect("write direct response");
        });

        let allowlisted_target = Url::parse(&format!(
            "http://sso-sit.intra.kommunale.it:{}/.well-known/openid-configuration",
            target_addr.port()
        ))
        .expect("allowlisted target URL should parse");
        let allowlisted_policy =
            OutboundPolicy::from_allowed_origins(&format!("http://sso-sit.intra.kommunale.it:{}", target_addr.port()))
                .expect("allowlisted policy should parse");
        let direct_dns: OutboundDnsLookup = Arc::new(move |host| match host {
            "sso-sit.intra.kommunale.it" => Some(vec![SocketAddr::new(target_addr.ip(), 0)]),
            _ => None,
        });
        let client = allowlisted_policy
            .configure_reqwest_client_for_test(
                &allowlisted_target,
                &matcher,
                Some(direct_dns),
                reqwest::Client::builder().timeout(std::time::Duration::from_secs(2)),
            )
            .expect("allowlisted NO_PROXY target should configure")
            .build()
            .expect("allowlisted client should build");

        let response = client
            .get(allowlisted_target)
            .send()
            .await
            .expect("allowlisted NO_PROXY target should connect directly");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        direct_server.await.expect("direct target task should finish");
    }

    #[tokio::test]
    async fn loopback_target_always_bypasses_configured_proxy() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback target listener");
        let addr = listener.local_addr().expect("loopback target local addr");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept loopback connection");
            let mut buf = [0_u8; 1024];
            let _ = stream.read(&mut buf).await.expect("read loopback request");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .expect("write loopback response");
        });

        let target = Url::parse(&format!("http://127.0.0.1:{}/.well-known/openid-configuration", addr.port()))
            .expect("loopback target URL should parse");
        let policy =
            OutboundPolicy::from_allowed_origins(&format!("http://127.0.0.1:{}", addr.port())).expect("allow loopback origin");
        // Even with a metadata proxy configured, loopback targets must bypass the proxy completely.
        let matcher = OutboundProxyMatcher::all("http://169.254.169.254:8080");
        let client = policy
            .configure_reqwest_client_with_proxy_matcher(
                &target,
                &matcher,
                reqwest::Client::builder().timeout(std::time::Duration::from_secs(2)),
            )
            .expect("loopback target should bypass forbidden proxy")
            .build()
            .expect("loopback client should build");

        let response = client
            .get(target)
            .send()
            .await
            .expect("loopback request should connect directly");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        server.await.expect("loopback server should finish");
    }

    #[tokio::test]
    async fn proxy_basic_auth_forwards_proxy_authorization_on_connect_and_http_without_leaking_secret() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        const SECRET_PASS: &str = "top-secret-pass-9611";
        // base64("proxy-user:top-secret-pass-9611")
        const EXPECTED_BASIC_TOKEN: &str = "cHJveHktdXNlcjp0b3Atc2VjcmV0LXBhc3MtOTYxMQ==";

        async fn read_http_head(stream: &mut tokio::net::TcpStream) -> String {
            let mut raw = Vec::new();
            let mut chunk = [0_u8; 512];
            while !raw.windows(4).any(|window| window == b"\r\n\r\n") {
                let n = stream.read(&mut chunk).await.expect("read proxy request bytes");
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&chunk[..n]);
            }
            String::from_utf8_lossy(&raw).into_owned()
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind auth proxy listener");
        let proxy_addr = listener.local_addr().expect("auth proxy local addr");
        let proxy_server = tokio::spawn(async move {
            let (mut connect_stream, _) = listener.accept().await.expect("accept CONNECT request");
            let connect_head = read_http_head(&mut connect_stream).await;
            connect_stream
                .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("write CONNECT response");

            let (mut forward_stream, _) = listener.accept().await.expect("accept HTTP forward request");
            let forward_head = read_http_head(&mut forward_stream).await;
            forward_stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .expect("write HTTP forward response");

            (connect_head, forward_head)
        });

        let proxy_url = format!("http://proxy-user:{SECRET_PASS}@bb-proxy.intra.kommunale.it:{}", proxy_addr.port());
        let matcher = OutboundProxyMatcher::all(&proxy_url);
        let matcher_debug = format!("{matcher:?}");
        assert!(!matcher_debug.contains(SECRET_PASS), "matcher Debug leaked secret: {matcher_debug}");
        assert!(
            !matcher_debug.contains(EXPECTED_BASIC_TOKEN),
            "matcher Debug leaked encoded token: {matcher_debug}"
        );

        let dns_override: OutboundDnsLookup = Arc::new(move |host| match host {
            "bb-proxy.intra.kommunale.it" => Some(vec![SocketAddr::new(proxy_addr.ip(), 0)]),
            _ => None,
        });

        let https_target = Url::parse("https://sso-sit.intra.kommunale.it:8443/.well-known/openid-configuration")
            .expect("HTTPS target should parse");
        let https_builder = OutboundPolicy::default()
            .configure_reqwest_client_for_test(
                &https_target,
                &matcher,
                Some(dns_override.clone()),
                reqwest::Client::builder().timeout(std::time::Duration::from_secs(2)),
            )
            .expect("HTTPS proxied client builder should configure");
        let builder_debug = format!("{https_builder:?}");
        assert!(!builder_debug.contains(SECRET_PASS), "builder Debug leaked secret: {builder_debug}");
        assert!(
            !builder_debug.contains(EXPECTED_BASIC_TOKEN),
            "builder Debug leaked encoded token: {builder_debug}"
        );

        let https_client = https_builder.build().expect("HTTPS proxied client should build");
        let client_debug = format!("{https_client:?}");
        assert!(!client_debug.contains(SECRET_PASS), "client Debug leaked secret: {client_debug}");
        assert!(
            !client_debug.contains(EXPECTED_BASIC_TOKEN),
            "client Debug leaked encoded token: {client_debug}"
        );

        let connect_err = https_client
            .get(https_target.clone())
            .send()
            .await
            .expect_err("mock 502 CONNECT response should fail tunnel setup");
        let connect_err_text = format!("{connect_err} | {connect_err:?}");
        assert!(
            !connect_err_text.contains(SECRET_PASS) && !connect_err_text.contains(EXPECTED_BASIC_TOKEN),
            "CONNECT error leaked proxy secret: {connect_err_text}"
        );

        let http_target = Url::parse("http://sso-sit.intra.kommunale.it:8080/.well-known/openid-configuration")
            .expect("HTTP target should parse");
        let http_client = OutboundPolicy::default()
            .configure_reqwest_client_for_test(
                &http_target,
                &matcher,
                Some(dns_override),
                reqwest::Client::builder().timeout(std::time::Duration::from_secs(2)),
            )
            .expect("HTTP proxied client builder should configure")
            .build()
            .expect("HTTP proxied client should build");
        let forward_resp = http_client
            .get(http_target)
            .send()
            .await
            .expect("HTTP forward proxy request should succeed");
        assert_eq!(forward_resp.status(), reqwest::StatusCode::OK);

        let (connect_head, forward_head) = proxy_server.await.expect("auth proxy server should finish");
        assert!(
            connect_head.starts_with("CONNECT sso-sit.intra.kommunale.it:8443 HTTP/1.1\r\n"),
            "unexpected CONNECT request line: {connect_head}"
        );
        assert!(
            connect_head
                .to_ascii_lowercase()
                .contains(&format!("proxy-authorization: basic {}", EXPECTED_BASIC_TOKEN.to_ascii_lowercase())),
            "CONNECT request missing Proxy-Authorization header: {connect_head}"
        );
        assert!(
            forward_head.starts_with("GET http://sso-sit.intra.kommunale.it:8080/.well-known/openid-configuration HTTP/1.1\r\n"),
            "unexpected HTTP forward request line: {forward_head}"
        );
        assert!(
            forward_head
                .to_ascii_lowercase()
                .contains(&format!("proxy-authorization: basic {}", EXPECTED_BASIC_TOKEN.to_ascii_lowercase())),
            "HTTP forward request missing Proxy-Authorization header: {forward_head}"
        );

        for rejected_proxy in [
            format!("http://proxy-user:{SECRET_PASS}@169.254.169.254:8080"),
            format!("socks5://proxy-user:{SECRET_PASS}@proxy.internal:1080"),
            format!("http://proxy-user:p@{SECRET_PASS}@proxy.internal:8080"),
        ] {
            let bad_matcher = OutboundProxyMatcher::all(&rejected_proxy);
            let bad_matcher_debug = format!("{bad_matcher:?}");
            assert!(!bad_matcher_debug.contains(SECRET_PASS) && !bad_matcher_debug.contains(EXPECTED_BASIC_TOKEN));
            let err = OutboundPolicy::default()
                .configure_reqwest_client_with_proxy_matcher(&https_target, &bad_matcher, reqwest::Client::builder())
                .expect_err("credentialed invalid/forbidden proxy must fail");
            let err_text = format!("{err} | {err:?}");
            assert!(
                !err_text.contains(SECRET_PASS) && !err_text.contains(EXPECTED_BASIC_TOKEN),
                "proxy policy error leaked secret: {err_text}"
            );
        }
    }
}
