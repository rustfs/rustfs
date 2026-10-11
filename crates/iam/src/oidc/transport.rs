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
use crate::{LOG_COMPONENT_IAM, LOG_SUBSYSTEM_OIDC};
use openidconnect::AsyncHttpClient;
use reqwest::{Certificate, Client};
use rustfs_config::MAX_OIDC_RESPONSE_SIZE;
use rustfs_utils::egress::{ENV_OUTBOUND_ALLOW_ORIGINS, OutboundPolicy, find_outbound_dns_policy_rejection};
use std::collections::VecDeque;
use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, RwLock};
use std::time::{Duration as StdDuration, Instant};
use tracing::{debug, error, warn};
use url::Url;

pub(super) const EVENT_OIDC_HTTP: &str = "oidc_http";
const OIDC_HTTP_REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const OIDC_HTTP_CONNECT_TIMEOUT: StdDuration = StdDuration::from_secs(3);
const OIDC_PLUGIN_AUTHN_WINDOW: StdDuration = StdDuration::from_secs(60);

#[derive(Debug, Clone, Copy, Default)]
pub struct OidcPluginAuthnMetricsSnapshot {
    pub failed_requests_minute: u64,
    pub last_fail_seconds: u64,
    pub last_succ_seconds: u64,
    pub succ_avg_rtt_ms_minute: u64,
    pub succ_max_rtt_ms_minute: u64,
    pub total_requests_minute: u64,
}

#[derive(Debug, Clone)]
struct OidcPluginAuthnSample {
    observed_at: Instant,
    succeeded: bool,
    rtt_ms: u64,
}

#[derive(Debug, Default)]
struct OidcPluginAuthnMetrics {
    samples: Mutex<VecDeque<OidcPluginAuthnSample>>,
    last_fail_at: Mutex<Option<Instant>>,
    last_succ_at: Mutex<Option<Instant>>,
}

fn lock_oidc_plugin_authn_metrics<'a, T>(mutex: &'a Mutex<T>, metric: &'static str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(err) => {
            warn!(metric, "Recovering poisoned OIDC authn metrics lock");
            err.into_inner()
        }
    }
}

fn seconds_since(now: Instant, observed_at: Option<Instant>) -> u64 {
    observed_at
        .map(|instant| now.duration_since(instant).as_secs())
        .unwrap_or_default()
}

impl OidcPluginAuthnMetrics {
    fn record(&self, rtt_ms: u64, succeeded: bool) {
        let now = Instant::now();
        let mut samples = lock_oidc_plugin_authn_metrics(&self.samples, "samples");
        samples.push_back(OidcPluginAuthnSample {
            observed_at: now,
            succeeded,
            rtt_ms,
        });
        while samples
            .front()
            .is_some_and(|sample| now.duration_since(sample.observed_at) > OIDC_PLUGIN_AUTHN_WINDOW)
        {
            samples.pop_front();
        }
        drop(samples);

        if succeeded {
            *lock_oidc_plugin_authn_metrics(&self.last_succ_at, "last_succ_at") = Some(now);
        } else {
            *lock_oidc_plugin_authn_metrics(&self.last_fail_at, "last_fail_at") = Some(now);
        }
    }

    fn snapshot(&self) -> OidcPluginAuthnMetricsSnapshot {
        let now = Instant::now();
        let (total_requests_minute, failed_requests_minute, succ_avg_rtt_ms_minute, succ_max_rtt_ms_minute) = {
            let mut samples = lock_oidc_plugin_authn_metrics(&self.samples, "samples");
            while samples
                .front()
                .is_some_and(|sample| now.duration_since(sample.observed_at) > OIDC_PLUGIN_AUTHN_WINDOW)
            {
                samples.pop_front();
            }

            let mut failed_requests_minute = 0u64;
            let mut successful_requests = 0u64;
            let mut successful_rtt_sum = 0u64;
            let mut succ_max_rtt_ms_minute = 0u64;

            for sample in samples.iter() {
                if sample.succeeded {
                    successful_requests += 1;
                    successful_rtt_sum += sample.rtt_ms;
                    succ_max_rtt_ms_minute = succ_max_rtt_ms_minute.max(sample.rtt_ms);
                } else {
                    failed_requests_minute += 1;
                }
            }

            let succ_avg_rtt_ms_minute = successful_rtt_sum.checked_div(successful_requests).unwrap_or_default();

            (
                samples.len() as u64,
                failed_requests_minute,
                succ_avg_rtt_ms_minute,
                succ_max_rtt_ms_minute,
            )
        };

        let last_fail_seconds = seconds_since(now, *lock_oidc_plugin_authn_metrics(&self.last_fail_at, "last_fail_at"));
        let last_succ_seconds = seconds_since(now, *lock_oidc_plugin_authn_metrics(&self.last_succ_at, "last_succ_at"));

        OidcPluginAuthnMetricsSnapshot {
            failed_requests_minute,
            last_fail_seconds,
            last_succ_seconds,
            succ_avg_rtt_ms_minute,
            succ_max_rtt_ms_minute,
            total_requests_minute,
        }
    }
}

static OIDC_PLUGIN_AUTHN_METRICS: LazyLock<OidcPluginAuthnMetrics> = LazyLock::new(OidcPluginAuthnMetrics::default);

pub fn oidc_plugin_authn_metrics_snapshot() -> OidcPluginAuthnMetricsSnapshot {
    OIDC_PLUGIN_AUTHN_METRICS.snapshot()
}

/// Header names whose values may carry OIDC secrets (client credentials, cookies,
/// bearer tokens). Their values are never emitted to logs, only their byte length.
const SENSITIVE_HEADER_NAMES: [&str; 4] = ["authorization", "proxy-authorization", "cookie", "set-cookie"];

pub(super) fn is_sensitive_header(name: &str) -> bool {
    SENSITIVE_HEADER_NAMES
        .iter()
        .any(|candidate| name.eq_ignore_ascii_case(candidate))
}

pub(super) fn format_http_headers(headers: &http::HeaderMap) -> String {
    headers
        .iter()
        .map(|(name, value)| {
            if is_sensitive_header(name.as_str()) {
                format!("{}=<redacted len={}>", name.as_str(), value.as_bytes().len())
            } else {
                let value = value.to_str().unwrap_or("<non-utf8>");
                format!("{}={}", name.as_str(), value)
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

#[derive(Debug, Default)]
pub(super) struct TokenResponseBodyShape {
    pub(super) json_object: bool,
    pub(super) json_keys: String,
    pub(super) has_access_token: bool,
    pub(super) has_id_token: bool,
    pub(super) has_token_type: bool,
    pub(super) has_expires_in: bool,
    pub(super) has_error: bool,
    pub(super) has_error_description: bool,
    pub(super) looks_like_html: bool,
}

pub(super) fn inspect_token_response_body(body: &[u8]) -> TokenResponseBodyShape {
    let mut shape = TokenResponseBodyShape {
        looks_like_html: body
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            .is_some_and(|byte| byte == b'<'),
        ..Default::default()
    };

    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return shape;
    };
    let Some(object) = value.as_object() else {
        return shape;
    };

    shape.json_object = true;
    shape.has_access_token = object.contains_key("access_token");
    shape.has_id_token = object.contains_key("id_token");
    shape.has_token_type = object.contains_key("token_type");
    shape.has_expires_in = object.contains_key("expires_in");
    shape.has_error = object.contains_key("error");
    shape.has_error_description = object.contains_key("error_description");

    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    keys.truncate(16);
    shape.json_keys = keys.join(",");

    shape
}

pub(super) fn oidc_http_error_diagnostics(error: &OidcHttpError) -> (&'static str, String) {
    match error {
        OidcHttpError::Reqwest(err) if err.is_timeout() => ("timeout", String::new()),
        OidcHttpError::Reqwest(err) if err.is_connect() => ("connect", String::new()),
        OidcHttpError::Reqwest(err) if err.status().is_some() => {
            ("http_status", err.status().map(|status| status.as_u16().to_string()).unwrap_or_default())
        }
        OidcHttpError::Reqwest(_) => ("request", String::new()),
        OidcHttpError::Http(_) => ("http_build", String::new()),
        OidcHttpError::ExtraRootCa(_) => ("extra_root_ca", String::new()),
        OidcHttpError::ForbiddenOutbound(_) => ("forbidden_outbound", String::new()),
        OidcHttpError::ResponseTooLarge(limit) => ("response_too_large", limit.to_string()),
    }
}

// ---- HTTP Client Adapter ----

/// Error type for the OIDC HTTP client adapter.
#[derive(Debug)]
pub(super) enum OidcHttpError {
    Reqwest(reqwest::Error),
    Http(http::Error),
    ExtraRootCa(String),
    /// The outbound destination was rejected by the shared egress policy before any
    /// connection was attempted (invalid URL, loopback/link-local/metadata/private IP,
    /// or a malformed allow-origins configuration).
    ForbiddenOutbound(String),
    /// The provider response body exceeded [`MAX_OIDC_RESPONSE_SIZE`] and was abandoned
    /// instead of being buffered in full.
    ResponseTooLarge(usize),
}

impl std::fmt::Display for OidcHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reqwest(e) => write!(f, "{e}"),
            Self::Http(e) => write!(f, "{e}"),
            Self::ExtraRootCa(reason) => write!(f, "failed to load OIDC extra root CA bundle: {reason}"),
            Self::ForbiddenOutbound(reason) => write!(f, "outbound request rejected: {reason}"),
            Self::ResponseTooLarge(limit) => write!(f, "oidc response body exceeds {limit} bytes"),
        }
    }
}

impl std::error::Error for OidcHttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Reqwest(e) => Some(e),
            Self::Http(e) => Some(e),
            Self::ExtraRootCa(_) | Self::ForbiddenOutbound(_) | Self::ResponseTooLarge(_) => None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct OidcExtraRootCaMaterial {
    pub generation: u64,
    pub root_ca_pem: Option<Vec<u8>>,
}

type OidcExtraRootCaFuture = Pin<Box<dyn Future<Output = Result<OidcExtraRootCaMaterial, String>> + Send>>;
type OidcExtraRootCaLoader = dyn Fn() -> OidcExtraRootCaFuture + Send + Sync;

#[derive(Clone)]
pub struct OidcExtraRootCaProvider {
    loader: Arc<OidcExtraRootCaLoader>,
}

impl OidcExtraRootCaProvider {
    pub fn new<F, Fut>(loader: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<OidcExtraRootCaMaterial, String>> + Send + 'static,
    {
        Self {
            loader: Arc::new(move || Box::pin(loader())),
        }
    }

    async fn load(&self) -> Result<OidcExtraRootCaMaterial, String> {
        (self.loader)().await
    }
}

#[derive(Clone, Default)]
struct CachedOidcExtraRootCerts {
    generation: u64,
    initialized: bool,
    certs: Vec<Certificate>,
}

/// HTTP client adapter bridging reqwest 0.13 to the `openidconnect` `AsyncHttpClient` trait.
///
/// A fresh client is built for every request so the destination is re-validated and the
/// resolved IP is re-classified at connection time. This closes the SSRF / DNS-rebinding
/// gap where a one-shot URL string check is bypassed by a hostname that resolves to an
/// internal address only at connection time, and it also covers endpoints discovered from
/// the provider metadata (JWKS, token) rather than only the operator-configured `config_url`.
pub(super) struct ReqwestHttpClient {
    /// `None` in production: the process-cached outbound policy from the environment is used.
    /// `Some(..)` only in tests, to explicitly allow a loopback mock endpoint.
    policy_override: Option<OutboundPolicy>,
    extra_root_certs: Arc<RwLock<CachedOidcExtraRootCerts>>,
    extra_root_ca_provider: Option<OidcExtraRootCaProvider>,
    #[cfg(test)]
    dns_resolver_override: Option<Arc<dyn reqwest::dns::Resolve>>,
}

pub(super) fn parse_oidc_extra_root_certs(source: &str, pem: &[u8]) -> Result<Vec<Certificate>, String> {
    if pem.iter().all(|byte| byte.is_ascii_whitespace()) {
        return Ok(Vec::new());
    }
    Certificate::from_pem_bundle(pem).map_err(|err| format!("failed to parse OIDC extra root CA bundle from {source}: {err}"))
}

pub(super) fn oidc_extra_root_certs(root_ca_pem: Option<&[u8]>) -> Result<Vec<Certificate>, String> {
    match root_ca_pem {
        Some(pem) => parse_oidc_extra_root_certs("RustFS outbound TLS material", pem),
        None => Ok(Vec::new()),
    }
}

/// Build a reqwest client pinned to the shared outbound egress policy for a single request.
///
/// [`OutboundPolicy::resolver_for`] validates the URL shape and rejects loopback,
/// link-local, metadata, multicast and unauthorized private addresses up front, and the
/// returned `OutboundDnsResolver` re-resolves and re-classifies the host on every new
/// connection so DNS rebinding fails closed. Redirects are not followed: a redirect target
/// would otherwise skip URL-shape re-validation. The timeouts bound how long a slow or
/// stalled provider can pin the calling task.
pub(super) fn build_oidc_http_client(
    uri: &str,
    policy_override: Option<&OutboundPolicy>,
    extra_root_certs: &[Certificate],
    #[cfg(test)] dns_resolver_override: Option<Arc<dyn reqwest::dns::Resolve>>,
) -> Result<(Client, Url), OidcHttpError> {
    let url = Url::parse(uri).map_err(|_| OidcHttpError::ForbiddenOutbound("invalid outbound OIDC URL".to_string()))?;
    let resolver = match policy_override {
        Some(policy) => policy.resolver_for(&url),
        None => OutboundPolicy::from_env_cached()
            .map_err(|err| OidcHttpError::ForbiddenOutbound(err.to_string()))?
            .resolver_for(&url),
    }
    .map_err(|err| {
        let base = err.to_string();
        let origin = url.origin().ascii_serialization();
        let can_allow_origin =
            OutboundPolicy::from_allowed_origins(&origin).is_ok_and(|allowlisted| allowlisted.validate_url(&url).is_ok());
        oidc_forbidden_outbound_error(&url, base, can_allow_origin)
    })?;
    let bypass_proxy = should_bypass_proxy_for_oidc_uri(uri);
    #[cfg(test)]
    let bypass_proxy = bypass_proxy || dns_resolver_override.is_some();
    #[cfg(test)]
    let resolver: Arc<dyn reqwest::dns::Resolve> = dns_resolver_override.unwrap_or_else(|| Arc::new(resolver));

    let mut builder = reqwest::Client::builder()
        .dns_resolver(resolver)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(OIDC_HTTP_REQUEST_TIMEOUT)
        .connect_timeout(OIDC_HTTP_CONNECT_TIMEOUT);
    if bypass_proxy {
        builder = builder.no_proxy();
    }
    if !extra_root_certs.is_empty() {
        builder = builder.tls_certs_merge(extra_root_certs.iter().cloned());
    }
    builder.build().map(|client| (client, url)).map_err(OidcHttpError::Reqwest)
}

fn oidc_forbidden_outbound_error(url: &Url, base: String, can_allow_origin: bool) -> OidcHttpError {
    let reason = if can_allow_origin {
        let origin = url.origin().ascii_serialization();
        format!(
            "{base}; add {origin} to {ENV_OUTBOUND_ALLOW_ORIGINS} (comma-separated) and restart RustFS to allow this operator-owned OIDC provider (origin only, no path)"
        )
    } else {
        base
    };
    OidcHttpError::ForbiddenOutbound(reason)
}

fn oidc_http_error_from_reqwest(url: &Url, error: reqwest::Error) -> OidcHttpError {
    if let Some(rejection) = find_outbound_dns_policy_rejection(&error) {
        let base = rejection.to_string();
        return oidc_forbidden_outbound_error(url, base, rejection.allow_origin_can_recover());
    }
    OidcHttpError::Reqwest(error)
}

/// Buffer a provider response body, failing closed once `limit` bytes have been seen.
///
/// `Response::bytes` would buffer the whole body unconditionally, so a hostile or compromised
/// provider endpoint could stream an arbitrarily large (or endless) body into memory.
async fn read_bounded_response_body(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, OidcHttpError> {
    if response.content_length().is_some_and(|len| len > limit as u64) {
        return Err(OidcHttpError::ResponseTooLarge(limit));
    }

    let mut response = response;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(OidcHttpError::Reqwest)? {
        if body.len() + chunk.len() > limit {
            return Err(OidcHttpError::ResponseTooLarge(limit));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub(super) fn should_bypass_proxy_for_oidc_uri(uri: &str) -> bool {
    let Some(host) = Url::parse(uri).ok().and_then(|url| url.host_str().map(str::to_owned)) else {
        return false;
    };
    let host = host.trim_matches(['[', ']']);

    host.eq_ignore_ascii_case("localhost") || host.parse::<IpAddr>().is_ok_and(|addr| addr.is_loopback())
}

impl ReqwestHttpClient {
    pub(super) fn new() -> Result<Self, String> {
        Self::new_with_extra_root_certs(Vec::new())
    }

    fn extra_root_cert_cache(certs: Vec<Certificate>) -> Arc<RwLock<CachedOidcExtraRootCerts>> {
        Arc::new(RwLock::new(CachedOidcExtraRootCerts {
            generation: 0,
            initialized: true,
            certs,
        }))
    }

    pub(super) fn new_with_extra_root_certs(extra_root_certs: Vec<Certificate>) -> Result<Self, String> {
        Ok(Self {
            policy_override: None,
            extra_root_certs: Self::extra_root_cert_cache(extra_root_certs),
            extra_root_ca_provider: None,
            #[cfg(test)]
            dns_resolver_override: None,
        })
    }

    pub(super) fn new_with_extra_root_ca_provider(extra_root_ca_provider: OidcExtraRootCaProvider) -> Result<Self, String> {
        Ok(Self {
            policy_override: None,
            extra_root_certs: Arc::new(RwLock::new(CachedOidcExtraRootCerts::default())),
            extra_root_ca_provider: Some(extra_root_ca_provider),
            #[cfg(test)]
            dns_resolver_override: None,
        })
    }

    pub(super) async fn current_extra_root_certs(&self) -> Result<Vec<Certificate>, OidcHttpError> {
        let Some(provider) = self.extra_root_ca_provider.as_ref() else {
            return self
                .extra_root_certs
                .read()
                .map(|cache| cache.certs.clone())
                .map_err(|e| OidcHttpError::ExtraRootCa(format!("extra root certificate cache lock poisoned: {e}")));
        };

        let material = provider.load().await.map_err(OidcHttpError::ExtraRootCa)?;
        if let Ok(cache) = self.extra_root_certs.read()
            && cache.initialized
            && cache.generation == material.generation
        {
            return Ok(cache.certs.clone());
        }

        let certs = oidc_extra_root_certs(material.root_ca_pem.as_deref()).map_err(OidcHttpError::ExtraRootCa)?;
        let mut cache = self
            .extra_root_certs
            .write()
            .map_err(|e| OidcHttpError::ExtraRootCa(format!("extra root certificate cache lock poisoned: {e}")))?;
        cache.generation = material.generation;
        cache.initialized = true;
        cache.certs = certs.clone();
        Ok(certs)
    }

    /// Test-only constructor that pins outbound requests to an explicit policy, so a
    /// loopback mock server can be reached without depending on process-wide environment.
    #[cfg(test)]
    pub(super) fn with_policy(policy: OutboundPolicy) -> Self {
        Self {
            policy_override: Some(policy),
            extra_root_certs: Self::extra_root_cert_cache(Vec::new()),
            extra_root_ca_provider: None,
            dns_resolver_override: None,
        }
    }

    #[cfg(test)]
    pub(super) fn with_policy_and_extra_root_certs(policy: OutboundPolicy, extra_root_certs: Vec<Certificate>) -> Self {
        Self {
            policy_override: Some(policy),
            extra_root_certs: Self::extra_root_cert_cache(extra_root_certs),
            extra_root_ca_provider: None,
            dns_resolver_override: None,
        }
    }

    #[cfg(test)]
    pub(super) fn with_policy_and_extra_root_ca_provider(
        policy: OutboundPolicy,
        extra_root_ca_provider: OidcExtraRootCaProvider,
    ) -> Self {
        Self {
            policy_override: Some(policy),
            extra_root_certs: Arc::new(RwLock::new(CachedOidcExtraRootCerts::default())),
            extra_root_ca_provider: Some(extra_root_ca_provider),
            dns_resolver_override: None,
        }
    }

    #[cfg(test)]
    pub(super) fn with_policy_and_dns_resolver(policy: OutboundPolicy, resolver: Arc<dyn reqwest::dns::Resolve>) -> Self {
        Self {
            policy_override: Some(policy),
            extra_root_certs: Self::extra_root_cert_cache(Vec::new()),
            extra_root_ca_provider: None,
            dns_resolver_override: Some(resolver),
        }
    }
}

impl<'c> AsyncHttpClient<'c> for ReqwestHttpClient {
    type Error = OidcHttpError;
    type Future = Pin<Box<dyn Future<Output = Result<http::Response<Vec<u8>>, Self::Error>> + Send + 'c>>;

    fn call(&'c self, request: http::Request<Vec<u8>>) -> Self::Future {
        Box::pin(async move {
            let started_at = Instant::now();
            let (parts, body) = request.into_parts();
            let method = parts.method.clone();
            let uri = parts.uri.to_string();
            if tracing::enabled!(tracing::Level::DEBUG) {
                let request_headers = format_http_headers(&parts.headers);
                debug!(
                    event = EVENT_OIDC_HTTP,
                    component = LOG_COMPONENT_IAM,
                    subsystem = LOG_SUBSYSTEM_OIDC,
                    result = "request",
                    method = %method,
                    uri = %uri,
                    request_headers = %request_headers,
                    request_body_len = body.len(),
                    "oidc outbound http"
                );
            }

            let extra_root_certs = self.current_extra_root_certs().await?;
            let (client, url) = build_oidc_http_client(
                &uri,
                self.policy_override.as_ref(),
                &extra_root_certs,
                #[cfg(test)]
                self.dns_resolver_override.clone(),
            )?;
            let response = client
                .request(parts.method, uri.clone())
                .headers(parts.headers)
                .body(body)
                .send()
                .await;

            let elapsed_ms = started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            let succeeded = response.as_ref().is_ok_and(|resp| resp.status().is_success());
            OIDC_PLUGIN_AUTHN_METRICS.record(elapsed_ms, succeeded);

            let response = response.map_err(|err| {
                let error = oidc_http_error_from_reqwest(&url, err);
                error!(
                    event = EVENT_OIDC_HTTP,
                    component = LOG_COMPONENT_IAM,
                    subsystem = LOG_SUBSYSTEM_OIDC,
                    result = "request_failed",
                    method = %method,
                    uri = %uri,
                    elapsed_ms,
                    error = %error,
                    "oidc outbound http"
                );
                error
            })?;

            let status = response.status();
            let headers = response.headers().clone();
            let body_bytes = read_bounded_response_body(response, MAX_OIDC_RESPONSE_SIZE)
                .await
                .map_err(|err| {
                    error!(
                        event = EVENT_OIDC_HTTP,
                        component = LOG_COMPONENT_IAM,
                        subsystem = LOG_SUBSYSTEM_OIDC,
                        result = "response_body_failed",
                        method = %method,
                        uri = %uri,
                        status = status.as_u16(),
                        elapsed_ms,
                        error = %err,
                        "oidc outbound http"
                    );
                    err
                })?;
            if tracing::enabled!(tracing::Level::DEBUG) {
                let response_headers = format_http_headers(&headers);
                debug!(
                    event = EVENT_OIDC_HTTP,
                    component = LOG_COMPONENT_IAM,
                    subsystem = LOG_SUBSYSTEM_OIDC,
                    result = "response",
                    method = %method,
                    uri = %uri,
                    status = status.as_u16(),
                    status_success = status.is_success(),
                    elapsed_ms,
                    response_headers = %response_headers,
                    response_body_len = body_bytes.len(),
                    "oidc outbound http"
                );
            }

            let mut http_response = http::Response::builder()
                .status(status)
                .body(body_bytes)
                .map_err(OidcHttpError::Http)?;
            *http_response.headers_mut() = headers;

            Ok(http_response)
        })
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
