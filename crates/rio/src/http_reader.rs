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

use crate::{EtagResolvable, HashReaderDetector, HashReaderMut};
use bytes::{Bytes, BytesMut};
use futures::{Stream, TryStreamExt as _};
use http::{HeaderMap, Version};
use pin_project_lite::pin_project;
use reqwest::{Certificate, Client, Identity, Method, RequestBuilder};
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_OPERATION_PUT_FILE_STREAM, INTERNODE_OPERATION_READ_FILE_STREAM, INTERNODE_OPERATION_WALK_DIR,
};
use rustfs_tls_runtime::load_cert_bundle_der_bytes;
use rustfs_utils::{get_env_bool, get_env_opt_str, get_env_opt_u64, get_env_opt_usize};
use rustls_pki_types::pem::PemObject;
use std::io::IoSlice;
use std::io::{self, Error};
use std::net::IpAddr;
use std::ops::Not as _;
use std::pin::Pin;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio::time::{self, Sleep};
use tokio_util::io::StreamReader;
use tokio_util::sync::PollSender;
use tracing::{error, warn};

const READ_FILE_STREAM_PATH: &str = "/rustfs/rpc/read_file_stream";
const PUT_FILE_STREAM_PATH: &str = "/rustfs/rpc/put_file_stream";
const WALK_DIR_PATH: &str = "/rustfs/rpc/walk_dir";
const HTTP_VERSION_09_LABEL: &str = "http/0.9";
const HTTP_VERSION_10_LABEL: &str = "http/1.0";
const HTTP_VERSION_11_LABEL: &str = "http/1.1";
const HTTP_VERSION_2_LABEL: &str = "h2";
const HTTP_VERSION_UNKNOWN_LABEL: &str = "unknown";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InternodeHttpErrorKind {
    ConnectTimeout,
    ConnectionRefused,
    DnsResolutionFailed,
    ConnectionReset,
    BodyStreamAborted,
    HttpStatus(reqwest::StatusCode),
    Unknown,
}

impl InternodeHttpErrorKind {
    pub fn is_retryable(self) -> bool {
        match self {
            // `DnsResolutionFailed` is retryable: MinIO's `IsNetworkOrHostDown`
            // treats `*net.DNSError` as network-or-host-down (=> retryable), and
            // RustFS already retries transient DNS failures at
            // `crates/ecstore/src/layout/endpoints.rs:580` (`is_retryable_dns_error`).
            // Consumers apply bounded retries, so a permanent NXDOMAIN costs at
            // most one extra attempt while transient resolution failures recover.
            Self::ConnectTimeout
            | Self::ConnectionRefused
            | Self::ConnectionReset
            | Self::BodyStreamAborted
            | Self::DnsResolutionFailed => true,
            Self::HttpStatus(status) => matches!(
                status,
                reqwest::StatusCode::TOO_MANY_REQUESTS
                    | reqwest::StatusCode::BAD_GATEWAY
                    | reqwest::StatusCode::SERVICE_UNAVAILABLE
                    | reqwest::StatusCode::GATEWAY_TIMEOUT
            ),
            Self::Unknown => false,
        }
    }

    fn io_error_kind(self) -> io::ErrorKind {
        match self {
            Self::ConnectTimeout => io::ErrorKind::TimedOut,
            Self::ConnectionRefused => io::ErrorKind::ConnectionRefused,
            Self::DnsResolutionFailed => io::ErrorKind::AddrNotAvailable,
            Self::ConnectionReset => io::ErrorKind::ConnectionReset,
            Self::BodyStreamAborted => io::ErrorKind::BrokenPipe,
            Self::HttpStatus(_) | Self::Unknown => io::ErrorKind::Other,
        }
    }

    pub fn metric_label(self) -> &'static str {
        match self {
            Self::ConnectTimeout => "connect_timeout",
            Self::ConnectionRefused => "connection_refused",
            Self::DnsResolutionFailed => "dns_resolution_failed",
            Self::ConnectionReset => "connection_reset",
            Self::BodyStreamAborted => "body_stream_aborted",
            Self::HttpStatus(status) => match status.as_u16() {
                429 => "http_429",
                502 => "http_502",
                503 => "http_503",
                504 => "http_504",
                _ => "http_status_other",
            },
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for InternodeHttpErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectTimeout => write!(f, "internode connect timeout"),
            Self::ConnectionRefused => write!(f, "internode connection refused"),
            Self::DnsResolutionFailed => write!(f, "internode dns resolution failed"),
            Self::ConnectionReset => write!(f, "internode connection reset"),
            Self::BodyStreamAborted => write!(f, "internode body stream aborted"),
            Self::HttpStatus(status) => write!(f, "internode http status {status}"),
            Self::Unknown => write!(f, "internode request failed"),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InternodeHttpRequestContext {
    method: String,
    target: String,
    operation: Option<&'static str>,
}

impl InternodeHttpRequestContext {
    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn operation(&self) -> Option<&'static str> {
        self.operation
    }
}

impl std::fmt::Display for InternodeHttpRequestContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.method, self.target)
    }
}

#[derive(thiserror::Error)]
#[error("{kind}: {context}")]
pub struct InternodeHttpError {
    kind: InternodeHttpErrorKind,
    context: InternodeHttpRequestContext,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Debug for InternodeHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InternodeHttpError")
            .field("kind", &self.kind)
            .field("context", &self.context)
            .field("source_present", &self.source.is_some())
            .finish()
    }
}

impl InternodeHttpError {
    pub fn kind(&self) -> InternodeHttpErrorKind {
        self.kind
    }

    pub fn context(&self) -> &InternodeHttpRequestContext {
        &self.context
    }

    fn new(kind: InternodeHttpErrorKind, context: InternodeHttpRequestContext) -> Self {
        Self {
            kind,
            context,
            source: None,
        }
    }

    #[doc(hidden)]
    pub fn new_for_test(kind: InternodeHttpErrorKind) -> Self {
        Self::new(
            kind,
            InternodeHttpRequestContext {
                method: "PUT".to_string(),
                target: "/rustfs/rpc/put_file_stream".to_string(),
                operation: Some(INTERNODE_OPERATION_PUT_FILE_STREAM),
            },
        )
    }

    fn with_source<E>(kind: InternodeHttpErrorKind, context: InternodeHttpRequestContext, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            kind,
            context,
            source: Some(Box::new(source)),
        }
    }

    fn into_io_error(self) -> io::Error {
        io::Error::new(self.kind.io_error_kind(), self)
    }
}

#[doc(hidden)]
pub fn new_test_internode_http_io_error(kind: InternodeHttpErrorKind) -> io::Error {
    InternodeHttpError::new_for_test(kind).into_io_error()
}

fn add_root_certificates_from_der(builder: reqwest::ClientBuilder, certs_der: &[Vec<u8>]) -> reqwest::ClientBuilder {
    let mut b = builder;
    for der in certs_der {
        if let Ok(cert) = Certificate::from_der(der) {
            b = b.add_root_certificate(cert);
        }
    }
    b
}

#[derive(Clone)]
struct CachedClients {
    generation: u64,
    client: Client,
    no_proxy_client: Client,
}

impl CachedClients {
    fn client_for(&self, disable_proxy: bool) -> Client {
        if disable_proxy {
            self.no_proxy_client.clone()
        } else {
            self.client.clone()
        }
    }
}

// Lock-free reads: this cache is hit once per stream open (data_shards times per
// GET), so the hot path must not serialize on a mutex. Writes only happen on
// outbound-TLS generation bumps (cert rotation), which are rare.
static CLIENT_CACHE: arc_swap::ArcSwapOption<CachedClients> = arc_swap::ArcSwapOption::const_empty();

const INTERNODE_HTTP_PROFILE_LEGACY: &str = "legacy";
const INTERNODE_HTTP_PROFILE_BALANCED: &str = "balanced";
const INTERNODE_HTTP_PROFILE_THROUGHPUT: &str = "throughput";
const INTERNODE_HTTP_PROXY_LEGACY: &str = "legacy";
const INTERNODE_HTTP_PROXY_OFF: &str = "off";
const INTERNODE_HTTP_PROXY_SYSTEM: &str = "system";
const INTERNODE_HTTP2_WINDOW_MIN: u32 = 65_535;
const INTERNODE_HTTP2_WINDOW_MAX: u32 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InternodeHttpTuningProfile {
    Legacy,
    Balanced,
    Throughput,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InternodeHttpProxyMode {
    Legacy,
    NoProxy,
    System,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InternodeHttpClientTuning {
    profile: InternodeHttpTuningProfile,
    pool_max_idle_per_host: Option<usize>,
    pool_idle_timeout_secs: Option<u64>,
    http2_initial_stream_window_size: Option<u32>,
    http2_initial_connection_window_size: Option<u32>,
    http2_adaptive_window: bool,
    proxy_mode: InternodeHttpProxyMode,
    /// True when the operator explicitly configured any HTTP/2 tuning knob
    /// (a window size, the keepalive timeout, or a non-default tuning profile).
    /// Used to warn once when that tuning is inert because the internode
    /// connection negotiated HTTP/1.1 over plaintext (backlog#805-C3).
    h2_tuning_explicit: bool,
}

fn internode_http_client_tuning() -> InternodeHttpClientTuning {
    // The tuning env vars cannot change at runtime; parse them once instead of
    // re-reading the environment on every internode request.
    static TUNING: LazyLock<InternodeHttpClientTuning> = LazyLock::new(InternodeHttpClientTuning::from_env);
    *TUNING
}

impl InternodeHttpClientTuning {
    fn from_env() -> Self {
        let profile =
            parse_internode_http_tuning_profile(get_env_opt_str(rustfs_config::ENV_INTERNODE_HTTP_TUNING_PROFILE).as_deref());
        let mut tuning = Self::from_values(
            profile,
            get_env_opt_usize(rustfs_config::ENV_INTERNODE_HTTP_POOL_MAX_IDLE_PER_HOST),
            get_env_opt_u64(rustfs_config::ENV_INTERNODE_HTTP_POOL_IDLE_TIMEOUT_SECS),
            get_env_opt_u64(rustfs_config::ENV_INTERNODE_HTTP2_INITIAL_STREAM_WINDOW_SIZE),
            get_env_opt_u64(rustfs_config::ENV_INTERNODE_HTTP2_INITIAL_CONNECTION_WINDOW_SIZE),
            get_env_bool(
                rustfs_config::ENV_INTERNODE_HTTP2_ADAPTIVE_WINDOW,
                profile.default_http2_adaptive_window(),
            ),
            get_env_opt_str(rustfs_config::ENV_INTERNODE_HTTP_PROXY).as_deref(),
        );
        // The keepalive timeout env is read in build_http_client, not passed to
        // from_values; fold it into the explicit-tuning signal here so the
        // inert-h2 warning also fires when only the keepalive knob is set.
        if get_env_opt_u64(rustfs_config::ENV_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS).is_some() {
            tuning.h2_tuning_explicit = true;
        }
        tuning
    }

    fn from_values(
        profile: InternodeHttpTuningProfile,
        pool_max_idle_per_host: Option<usize>,
        pool_idle_timeout_secs: Option<u64>,
        stream_window_size: Option<u64>,
        connection_window_size: Option<u64>,
        http2_adaptive_window: bool,
        proxy_mode: Option<&str>,
    ) -> Self {
        // Explicit h2 tuning = an operator-set window size (raw arg, before the
        // profile fallback) or a non-default (non-Legacy) tuning profile. The
        // keepalive-env contribution is OR-ed in by from_env.
        let h2_tuning_explicit =
            stream_window_size.is_some() || connection_window_size.is_some() || profile != InternodeHttpTuningProfile::Legacy;
        Self {
            profile,
            pool_max_idle_per_host: pool_max_idle_per_host.or_else(|| profile.default_pool_max_idle_per_host()),
            pool_idle_timeout_secs: pool_idle_timeout_secs.or_else(|| profile.default_pool_idle_timeout_secs()),
            http2_initial_stream_window_size: clamp_http2_window(
                stream_window_size.or_else(|| profile.default_stream_window_size()),
            ),
            http2_initial_connection_window_size: clamp_http2_window(
                connection_window_size.or_else(|| profile.default_connection_window_size()),
            ),
            http2_adaptive_window,
            proxy_mode: parse_internode_http_proxy_mode(proxy_mode, profile),
            h2_tuning_explicit,
        }
    }
}

impl InternodeHttpTuningProfile {
    fn default_pool_max_idle_per_host(self) -> Option<usize> {
        match self {
            Self::Legacy => None,
            Self::Balanced => Some(64),
            Self::Throughput => Some(256),
        }
    }

    fn default_pool_idle_timeout_secs(self) -> Option<u64> {
        match self {
            Self::Legacy => None,
            Self::Balanced => Some(120),
            Self::Throughput => Some(300),
        }
    }

    fn default_stream_window_size(self) -> Option<u64> {
        match self {
            Self::Legacy => None,
            Self::Balanced => Some(1024 * 1024),
            Self::Throughput => Some(4 * 1024 * 1024),
        }
    }

    fn default_connection_window_size(self) -> Option<u64> {
        match self {
            Self::Legacy => None,
            Self::Balanced => Some(4 * 1024 * 1024),
            Self::Throughput => Some(16 * 1024 * 1024),
        }
    }

    fn default_http2_adaptive_window(self) -> bool {
        matches!(self, Self::Throughput)
    }
}

fn parse_internode_http_tuning_profile(value: Option<&str>) -> InternodeHttpTuningProfile {
    match value.map(|value| value.trim().to_ascii_lowercase()) {
        Some(value) if value == INTERNODE_HTTP_PROFILE_LEGACY => InternodeHttpTuningProfile::Legacy,
        Some(value) if value == INTERNODE_HTTP_PROFILE_BALANCED => InternodeHttpTuningProfile::Balanced,
        Some(value) if value == INTERNODE_HTTP_PROFILE_THROUGHPUT => InternodeHttpTuningProfile::Throughput,
        _ => InternodeHttpTuningProfile::Legacy,
    }
}

fn parse_internode_http_proxy_mode(value: Option<&str>, profile: InternodeHttpTuningProfile) -> InternodeHttpProxyMode {
    match value.map(|value| value.trim().to_ascii_lowercase()) {
        Some(value) if value == INTERNODE_HTTP_PROXY_SYSTEM => InternodeHttpProxyMode::System,
        Some(value) if value == INTERNODE_HTTP_PROXY_LEGACY => InternodeHttpProxyMode::Legacy,
        Some(value) if matches!(value.as_str(), INTERNODE_HTTP_PROXY_OFF | "none" | "no_proxy" | "disabled") => {
            InternodeHttpProxyMode::NoProxy
        }
        Some(_) | None if matches!(profile, InternodeHttpTuningProfile::Legacy) => InternodeHttpProxyMode::Legacy,
        Some(_) | None => InternodeHttpProxyMode::NoProxy,
    }
}

fn clamp_http2_window(value: Option<u64>) -> Option<u32> {
    let value = value?;
    let value = value.clamp(u64::from(INTERNODE_HTTP2_WINDOW_MIN), u64::from(INTERNODE_HTTP2_WINDOW_MAX));
    u32::try_from(value).ok()
}

/// Applies internode HTTP client tuning (connection pool + HTTP/2 window
/// sizes) to a reqwest client builder.
///
/// IMPORTANT (backlog#805-C3): the HTTP/2 window and keepalive settings only
/// take effect when the internode connection actually negotiates HTTP/2, which
/// happens via TLS-ALPN. Over plaintext internode transport the connection is
/// HTTP/1.1 and every `http2_*` knob here (and the keepalive settings in
/// `build_http_client`) is silently inert. Enable internode TLS to use them.
/// `should_warn_h2_inert` emits a one-time warning when explicitly-configured
/// h2 tuning is observed to be inert on a live connection.
fn apply_http_client_tuning(mut builder: reqwest::ClientBuilder, tuning: InternodeHttpClientTuning) -> reqwest::ClientBuilder {
    if let Some(pool_max_idle_per_host) = tuning.pool_max_idle_per_host {
        builder = builder.pool_max_idle_per_host(pool_max_idle_per_host);
    }
    if let Some(pool_idle_timeout_secs) = tuning.pool_idle_timeout_secs {
        builder = builder.pool_idle_timeout(std::time::Duration::from_secs(pool_idle_timeout_secs));
    }
    if tuning.http2_adaptive_window {
        builder = builder.http2_adaptive_window(true);
    } else {
        if let Some(stream_window_size) = tuning.http2_initial_stream_window_size {
            builder = builder.http2_initial_stream_window_size(stream_window_size);
        }
        if let Some(connection_window_size) = tuning.http2_initial_connection_window_size {
            builder = builder.http2_initial_connection_window_size(connection_window_size);
        }
    }
    builder
}

async fn build_http_client(
    disable_proxy: bool,
    tuning: InternodeHttpClientTuning,
    outbound_tls: &rustfs_tls_runtime::GlobalPublishedOutboundTlsState,
) -> io::Result<Client> {
    // Keep the data-plane HTTP/2 keepalive timeout consistent with the control-plane
    // gRPC channel and env-configurable. A too-aggressive value (e.g. 3s) tears down a
    // busy data connection when a PING ACK is delayed under load, aborting in-flight
    // shard read streams and truncating large-object GETs mid-stream (backlog#832).
    let http2_keepalive_timeout_secs = get_env_opt_u64(rustfs_config::ENV_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS)
        .unwrap_or(rustfs_config::DEFAULT_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS);
    let mut builder = Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .tcp_keepalive(std::time::Duration::from_secs(10))
        .http2_keep_alive_interval(std::time::Duration::from_secs(5))
        .http2_keep_alive_timeout(std::time::Duration::from_secs(http2_keepalive_timeout_secs))
        .http2_keep_alive_while_idle(true);
    builder = apply_http_client_tuning(builder, tuning);

    if disable_proxy {
        builder = builder.no_proxy();
    }

    if let Some(root_ca_pem) = outbound_tls.root_ca_pem.as_ref() {
        let mut reader = std::io::BufReader::new(root_ca_pem.as_slice());
        match rustls_pki_types::CertificateDer::pem_reader_iter(&mut reader).collect::<Result<Vec<_>, _>>() {
            Ok(certs_der) => {
                let certs_der = certs_der.into_iter().map(|cert| cert.to_vec()).collect::<Vec<_>>();
                builder = add_root_certificates_from_der(builder, &certs_der);
            }
            Err(err) => {
                warn!("Failed to parse published outbound root CA PEM; falling back to default trust roots: {err}");
            }
        }
    } else if let Some(tp) = get_env_opt_str(rustfs_config::ENV_RUSTFS_TLS_PATH).and_then(|s| {
        if s.is_empty() {
            None
        } else {
            Some(std::path::PathBuf::from(s))
        }
    }) {
        let ca_path = tp.join(rustfs_config::RUSTFS_CA_CERT);
        if ca_path.exists()
            && let Some(ca_path_str) = ca_path.to_str()
        {
            match load_cert_bundle_der_bytes(ca_path_str) {
                Ok(certs_der) => {
                    builder = add_root_certificates_from_der(builder, &certs_der);
                }
                Err(err) => {
                    warn!("Failed to parse fallback root CA bundle '{}': {}", ca_path.display(), err);
                }
            }
        }
    }

    if let Some(identity) = outbound_tls.mtls_identity.as_ref() {
        let mut pem = Vec::with_capacity(identity.cert_pem.len() + identity.key_pem.len() + 1);
        pem.extend_from_slice(&identity.cert_pem);
        if !pem.ends_with(b"\n") {
            pem.push(b'\n');
        }
        pem.extend_from_slice(&identity.key_pem);

        match Identity::from_pem(&pem) {
            Ok(id) => builder = builder.identity(id),
            Err(e) => error!("Failed to load mTLS identity from PEM: {e}"),
        }
    }

    // This runs lazily on the first internode request and again on every TLS
    // generation bump, so a build failure must surface as an error, not a panic.
    builder
        .build()
        .map_err(|err| Error::other(format!("failed to build internode HTTP client: {err}")))
}

fn should_bypass_proxy_for_url(url: &str) -> bool {
    let Some(host) = reqwest::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
    else {
        return false;
    };
    let host = host.trim_matches(['[', ']']);

    host.eq_ignore_ascii_case("localhost") || host.parse::<IpAddr>().is_ok_and(|addr| addr.is_loopback())
}

fn should_disable_proxy_for_url(url: &str, tuning: InternodeHttpClientTuning) -> bool {
    match tuning.proxy_mode {
        InternodeHttpProxyMode::System => false,
        InternodeHttpProxyMode::NoProxy => true,
        InternodeHttpProxyMode::Legacy => should_bypass_proxy_for_url(url),
    }
}

async fn get_http_client(url: &str) -> io::Result<Client> {
    let tuning = internode_http_client_tuning();
    // Reuse HTTP connection pools while honoring the configured internode proxy
    // policy. The legacy profile only bypasses loopback URLs to preserve defaults.
    let disable_proxy = should_disable_proxy_for_url(url, tuning);

    // Fast path: check generation first (cheap atomic read) to avoid cloning
    // the full PEM + identity bytes when the TLS state hasn't changed.
    let generation = crate::http_runtime_sources::outbound_tls_generation();

    let previous = CLIENT_CACHE.load_full();
    if let Some(cached) = previous.as_ref() {
        if cached.generation == generation {
            return Ok(cached.client_for(disable_proxy));
        }
        crate::http_runtime_sources::record_stale_outbound_tls_generation("rio_http_reader");
    }

    // Cache miss or stale generation — load full outbound TLS state.
    let outbound_tls = crate::http_runtime_sources::outbound_tls_state().await;

    let built = match build_http_client(false, tuning, &outbound_tls).await {
        Ok(client) => match build_http_client(true, tuning, &outbound_tls).await {
            Ok(no_proxy_client) => Ok((client, no_proxy_client)),
            Err(err) => Err(err),
        },
        Err(err) => Err(err),
    };
    let (client, no_proxy_client) = match built {
        Ok(pair) => pair,
        Err(err) => {
            // Prefer serving with the previous TLS generation over failing the
            // request outright; the stale-generation metric already fired above.
            if let Some(cached) = previous {
                warn!(
                    error = %err,
                    stale_generation = cached.generation,
                    target_generation = generation,
                    "failed to rebuild internode HTTP client; falling back to previous TLS generation"
                );
                return Ok(cached.client_for(disable_proxy));
            }
            return Err(err);
        }
    };
    let cached = std::sync::Arc::new(CachedClients {
        generation,
        client,
        no_proxy_client,
    });

    // Guard against races: only overwrite the cache if it is empty or contains an
    // older generation, so a slower task cannot regress the TLS state after a
    // faster task already cached a newer generation.
    CLIENT_CACHE.rcu(|current| match current {
        Some(existing) if existing.generation > generation => Some(existing.clone()),
        _ => Some(cached.clone()),
    });
    Ok(cached.client_for(disable_proxy))
}

fn internode_request_context(method: &Method, url: &str, operation: Option<&'static str>) -> InternodeHttpRequestContext {
    let target = reqwest::Url::parse(url)
        .ok()
        .map(|parsed| parsed.path().to_string())
        .unwrap_or_else(|| "<invalid-internode-url>".to_string());
    InternodeHttpRequestContext {
        method: method.to_string(),
        target,
        operation,
    }
}

/// Classify a transport-level error into an [`InternodeHttpErrorKind`].
///
/// Strategy is "typed-first, string-only-for-DNS", mirroring MinIO's
/// `xnet.IsNetworkOrHostDown`: we trust structured signals (reqwest flags and
/// the `io::Error` chain) and only fall back to fragile message matching for
/// DNS detection, which reqwest/hyper do not expose as a typed error kind.
///
/// Ordering matters:
/// 1. A caller-reported timeout wins outright (`ConnectTimeout`).
/// 2. A typed `io::ErrorKind` anywhere in the source chain wins over any
///    string or body signal — a real `ConnectionRefused` must never be
///    mislabeled `DnsResolutionFailed` just because "dns" appears in the text.
/// 3. Only for connect-phase failures with no typed kind do we consult the
///    DNS string heuristic, then default to `ConnectionRefused`.
/// 4. Body-phase failures map to `BodyStreamAborted`.
/// 5. Anything else is `Unknown`.
fn classify_transport_error(
    err: &(dyn std::error::Error + 'static),
    is_timeout: bool,
    is_connect: bool,
    is_body: bool,
) -> InternodeHttpErrorKind {
    if is_timeout {
        return InternodeHttpErrorKind::ConnectTimeout;
    }

    if let Some(kind) = find_io_error_kind_in_chain(err) {
        return kind;
    }

    if is_connect {
        if chain_contains_dns_marker(err) {
            return InternodeHttpErrorKind::DnsResolutionFailed;
        }
        return InternodeHttpErrorKind::ConnectionRefused;
    }

    if is_body {
        return InternodeHttpErrorKind::BodyStreamAborted;
    }

    InternodeHttpErrorKind::Unknown
}

/// Walk the error source chain looking for a `std::io::Error` and map its
/// [`io::ErrorKind`] onto our typed classification. Returns `None` when no
/// `io::Error` is present or its kind carries no actionable signal.
fn find_io_error_kind_in_chain(err: &(dyn std::error::Error + 'static)) -> Option<InternodeHttpErrorKind> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(current) = source {
        if let Some(io_err) = current.downcast_ref::<io::Error>() {
            match io_err.kind() {
                io::ErrorKind::ConnectionRefused => return Some(InternodeHttpErrorKind::ConnectionRefused),
                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionAborted => {
                    return Some(InternodeHttpErrorKind::ConnectionReset);
                }
                io::ErrorKind::TimedOut => return Some(InternodeHttpErrorKind::ConnectTimeout),
                io::ErrorKind::HostUnreachable | io::ErrorKind::NetworkUnreachable => {
                    return Some(InternodeHttpErrorKind::ConnectionRefused);
                }
                _ => {}
            }
        }
        source = current.source();
    }
    None
}

/// The SOLE remaining string heuristic, gated behind `is_connect`: reqwest and
/// hyper surface DNS resolution failures only as opaque messages, so we walk the
/// source chain and match well-known DNS wordings across OS locales/resolvers.
fn chain_contains_dns_marker(err: &(dyn std::error::Error + 'static)) -> bool {
    const DNS_MARKERS: [&str; 6] = [
        "dns",
        "failed to lookup address",
        "name or service not known",
        "nodename nor servname",
        "no such host",
        "temporary failure in name resolution",
    ];

    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(current) = source {
        let message = current.to_string().to_ascii_lowercase();
        if DNS_MARKERS.iter().any(|marker| message.contains(marker)) {
            return true;
        }
        source = current.source();
    }
    false
}

fn classify_reqwest_error(err: &reqwest::Error) -> InternodeHttpErrorKind {
    classify_transport_error(err, err.is_timeout(), err.is_connect(), err.is_body())
}

fn classify_http_status(status: reqwest::StatusCode) -> InternodeHttpErrorKind {
    InternodeHttpErrorKind::HttpStatus(status)
}

fn internode_reqwest_error(method: &Method, url: &str, operation: Option<&'static str>, err: reqwest::Error) -> io::Error {
    let context = internode_request_context(method, url, operation);
    let classified = classify_reqwest_error(&err);
    InternodeHttpError::with_source(classified, context, err).into_io_error()
}

fn internode_reqwest_body_error(method: &Method, url: &str, operation: Option<&'static str>, err: reqwest::Error) -> io::Error {
    let context = internode_request_context(method, url, operation);
    let classified = classify_transport_error(&err, err.is_timeout(), err.is_connect(), true);
    InternodeHttpError::with_source(classified, context, err).into_io_error()
}

fn internode_classified_error(
    method: &Method,
    url: &str,
    operation: Option<&'static str>,
    kind: InternodeHttpErrorKind,
) -> io::Error {
    let context = internode_request_context(method, url, operation);
    InternodeHttpError::new(kind, context).into_io_error()
}

fn internode_status_error(method: &Method, url: &str, operation: Option<&'static str>, status: reqwest::StatusCode) -> io::Error {
    let context = internode_request_context(method, url, operation);
    let classified = classify_http_status(status);
    InternodeHttpError::new(classified, context).into_io_error()
}

pin_project! {
    pub struct HttpReader {
        url:String,
        method: Method,
        headers: HeaderMap,
        track_internode_metrics: bool,
        internode_operation: Option<&'static str>,
        stall_timeout: Option<Duration>,
        stall_timer: Option<Pin<Box<Sleep>>>,
        request_started: Instant,
        duration_recorded: bool,
        #[pin]
        inner: StreamReader<Pin<Box<dyn Stream<Item=std::io::Result<Bytes>>+Send+Sync>>, Bytes>,
    }
}

impl HttpReader {
    pub async fn new(url: String, method: Method, headers: HeaderMap, body: Option<Vec<u8>>) -> io::Result<Self> {
        // http_log!("[HttpReader::new] url: {url}, method: {method:?}, headers: {headers:?}");
        Self::with_capacity_and_stall_timeout(url, method, headers, body, 0, None).await
    }

    pub async fn new_with_stall_timeout(
        url: String,
        method: Method,
        headers: HeaderMap,
        body: Option<Vec<u8>>,
        stall_timeout: Option<Duration>,
    ) -> io::Result<Self> {
        Self::with_capacity_and_stall_timeout(url, method, headers, body, 0, stall_timeout).await
    }

    /// Create a new HttpReader from a URL. The request is performed immediately.
    pub async fn with_capacity(
        url: String,
        method: Method,
        headers: HeaderMap,
        body: Option<Vec<u8>>,
        _read_buf_size: usize,
    ) -> io::Result<Self> {
        Self::with_capacity_and_stall_timeout(url, method, headers, body, _read_buf_size, None).await
    }

    async fn with_capacity_and_stall_timeout(
        url: String,
        method: Method,
        headers: HeaderMap,
        body: Option<Vec<u8>>,
        _read_buf_size: usize,
        stall_timeout: Option<Duration>,
    ) -> io::Result<Self> {
        let track_internode_metrics = is_internode_rpc_url(&url);
        let internode_operation = internode_rpc_operation(&url);
        let client = get_http_client(&url).await.inspect_err(|_| {
            record_internode_error(track_internode_metrics, internode_operation);
        })?;
        let mut request: RequestBuilder = client.request(method.clone(), url.clone()).headers(headers.clone());
        if let Some(body) = body {
            request = request.body(body);
        }

        let request_started = Instant::now();
        let resp = request.send().await.map_err(|e| {
            record_internode_operation_duration(track_internode_metrics, internode_operation, request_started.elapsed());
            record_internode_error(track_internode_metrics, internode_operation);
            record_internode_classified_error(track_internode_metrics, internode_operation, classify_reqwest_error(&e));
            internode_reqwest_error(&method, &url, internode_operation, e)
        })?;

        record_internode_http_version(track_internode_metrics, internode_operation, http_version_metric_label(resp.version()));
        maybe_warn_h2_inert(resp.version());
        if resp.status().is_success().not() {
            record_internode_operation_duration(track_internode_metrics, internode_operation, request_started.elapsed());
            record_internode_error(track_internode_metrics, internode_operation);
            record_internode_classified_error(track_internode_metrics, internode_operation, classify_http_status(resp.status()));
            return Err(internode_status_error(&method, &url, internode_operation, resp.status()));
        }

        record_internode_outgoing_request(track_internode_metrics, internode_operation);

        let stream_error_url = url.clone();
        let stream_error_method = method.clone();
        let stream = resp.bytes_stream().map_err(move |e| {
            record_internode_error(track_internode_metrics, internode_operation);
            let classified = classify_transport_error(&e, e.is_timeout(), e.is_connect(), true);
            record_internode_classified_error(track_internode_metrics, internode_operation, classified);
            internode_reqwest_body_error(&stream_error_method, &stream_error_url, internode_operation, e)
        });

        Ok(Self {
            inner: StreamReader::new(Box::pin(stream)),
            url,
            method,
            headers,
            track_internode_metrics,
            internode_operation,
            stall_timer: None,
            stall_timeout,
            request_started,
            duration_recorded: false,
        })
    }
    pub fn url(&self) -> &str {
        &self.url
    }
    pub fn method(&self) -> &Method {
        &self.method
    }
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

impl AsyncRead for HttpReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();

        let filled_before = buf.filled().len();
        match this.inner.as_mut().poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let bytes_read = buf.filled().len().saturating_sub(filled_before);
                if bytes_read > 0 {
                    record_internode_recv_bytes(*this.track_internode_metrics, *this.internode_operation, bytes_read);
                } else {
                    record_internode_operation_duration_once(
                        *this.track_internode_metrics,
                        *this.internode_operation,
                        *this.request_started,
                        this.duration_recorded,
                    );
                }
                *this.stall_timer = None;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                let Some(stall_timeout) = *this.stall_timeout else {
                    return Poll::Pending;
                };
                let timer = this.stall_timer.get_or_insert_with(|| Box::pin(time::sleep(stall_timeout)));
                if timer.as_mut().poll(cx).is_ready() {
                    record_internode_operation_duration_once(
                        *this.track_internode_metrics,
                        *this.internode_operation,
                        *this.request_started,
                        this.duration_recorded,
                    );
                    record_internode_stall_timeout(*this.track_internode_metrics, *this.internode_operation);
                    record_internode_error(*this.track_internode_metrics, *this.internode_operation);
                    Poll::Ready(Err(Error::new(
                        io::ErrorKind::TimedOut,
                        "HttpReader stall timeout: no data received before deadline",
                    )))
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Err(err)) => {
                record_internode_operation_duration_once(
                    *this.track_internode_metrics,
                    *this.internode_operation,
                    *this.request_started,
                    this.duration_recorded,
                );
                Poll::Ready(Err(err))
            }
        }
    }
}

impl EtagResolvable for HttpReader {
    fn is_etag_reader(&self) -> bool {
        false
    }
    fn try_resolve_etag(&mut self) -> Option<String> {
        None
    }
}

impl HashReaderDetector for HttpReader {
    fn is_hash_reader(&self) -> bool {
        false
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        None
    }
}

struct ReceiverStream {
    receiver: mpsc::Receiver<Option<Bytes>>,
    track_internode_metrics: bool,
    internode_operation: Option<&'static str>,
}

impl Stream for ReceiverStream {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = Pin::new(&mut self.receiver).poll_recv(cx);
        // match &poll {
        //     Poll::Ready(Some(Some(bytes))) => {
        //         // http_log!("[ReceiverStream] poll_next: got {} bytes", bytes.len());
        //     }
        //     Poll::Ready(Some(None)) => {
        //         // http_log!("[ReceiverStream] poll_next: sender shutdown");
        //     }
        //     Poll::Ready(None) => {
        //         // http_log!("[ReceiverStream] poll_next: channel closed");
        //     }
        //     Poll::Pending => {
        //         // http_log!("[ReceiverStream] poll_next: pending");
        //     }
        // }
        match poll {
            Poll::Ready(Some(Some(bytes))) => {
                record_internode_sent_bytes(self.track_internode_metrics, self.internode_operation, bytes.len());
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(None)) => Poll::Ready(None), // Sender shutdown
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Not pin-projected: every field is `Unpin` and the `AsyncWrite` impl accesses
// them through `get_mut()`, so a plain struct lets us add a manual `Drop`
// (pin-project forbids one) to abort the background HTTP task — see below.
pub struct HttpWriter {
    url: String,
    method: Method,
    headers: HeaderMap,
    err_rx: tokio::sync::oneshot::Receiver<std::io::Error>,
    start_tx: Option<tokio::sync::oneshot::Sender<()>>,
    sender: PollSender<Option<Bytes>>,
    handle: tokio::task::JoinHandle<std::io::Result<()>>,
    pending_chunk: BytesMut,
    finish: bool,
    track_internode_metrics: bool,
    internode_operation: Option<&'static str>,
}

const HTTP_WRITER_CHANNEL_CAPACITY: usize = 8;
const HTTP_WRITER_BUFFER_SIZE: usize = 1024 * 1024;

impl HttpWriter {
    /// Create a new HttpWriter for the given URL. The HTTP request is performed in the background.
    pub async fn new(url: String, method: Method, headers: HeaderMap) -> io::Result<Self> {
        // http_log!("[HttpWriter::new] url: {url}, method: {method:?}, headers: {headers:?}");
        let url_clone = url.clone();
        let method_clone = method.clone();
        let headers_clone = headers.clone();
        let track_internode_metrics = is_internode_rpc_url(&url);
        let internode_operation = internode_rpc_operation(&url);

        let (sender, receiver) = tokio::sync::mpsc::channel::<Option<Bytes>>(HTTP_WRITER_CHANNEL_CAPACITY);
        let (err_tx, err_rx) = tokio::sync::oneshot::channel::<io::Error>();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel::<()>();

        let handle = tokio::spawn(async move {
            if start_rx.await.is_err() {
                return Ok(());
            }
            record_internode_outgoing_request(track_internode_metrics, internode_operation);

            let stream = ReceiverStream {
                receiver,
                track_internode_metrics,
                internode_operation,
            };
            let body = reqwest::Body::wrap_stream(stream);
            // http_log!(
            //     "[HttpWriter::spawn] sending HTTP request: url={url_clone}, method={method_clone:?}, headers={headers_clone:?}"
            // );

            let client = match get_http_client(&url_clone).await {
                Ok(client) => client,
                Err(err) => {
                    record_internode_error(track_internode_metrics, internode_operation);
                    let _ = err_tx.send(Error::new(err.kind(), err.to_string()));
                    return Err(err);
                }
            };
            let request = client
                .request(method_clone.clone(), url_clone.clone())
                .headers(headers_clone.clone())
                .body(body);

            // Hold the request until the shutdown signal is received
            let request_started = Instant::now();
            let response = request.send().await;

            match response {
                Ok(resp) => {
                    record_internode_operation_duration(track_internode_metrics, internode_operation, request_started.elapsed());
                    record_internode_http_version(
                        track_internode_metrics,
                        internode_operation,
                        http_version_metric_label(resp.version()),
                    );
                    maybe_warn_h2_inert(resp.version());
                    // http_log!("[HttpWriter::spawn] got response: status={}", resp.status());
                    if !resp.status().is_success() {
                        record_internode_error(track_internode_metrics, internode_operation);
                        record_internode_classified_error(
                            track_internode_metrics,
                            internode_operation,
                            classify_http_status(resp.status()),
                        );
                        let status = resp.status();
                        let io_err = internode_status_error(&method_clone, &url_clone, internode_operation, status);
                        let _ = err_tx.send(internode_status_error(&method_clone, &url_clone, internode_operation, status));
                        return Err(io_err);
                    }
                }
                Err(e) => {
                    record_internode_operation_duration(track_internode_metrics, internode_operation, request_started.elapsed());
                    record_internode_error(track_internode_metrics, internode_operation);
                    let classified = classify_reqwest_error(&e);
                    record_internode_classified_error(track_internode_metrics, internode_operation, classified);
                    let _ = err_tx.send(internode_classified_error(&method_clone, &url_clone, internode_operation, classified));
                    let io_err = internode_reqwest_error(&method_clone, &url_clone, internode_operation, e);
                    return Err(io_err);
                }
            }

            // http_log!("[HttpWriter::spawn] HTTP request completed, exiting");
            Ok(())
        });

        // http_log!("[HttpWriter::new] connection established successfully");
        Ok(Self {
            url,
            method,
            headers,
            err_rx,
            start_tx: Some(start_tx),
            sender: PollSender::new(sender),
            handle,
            pending_chunk: BytesMut::with_capacity(HTTP_WRITER_BUFFER_SIZE),
            finish: false,
            track_internode_metrics,
            internode_operation,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

fn is_internode_rpc_url(url: &str) -> bool {
    url.contains("/rustfs/rpc/")
}

fn internode_rpc_operation(url: &str) -> Option<&'static str> {
    let url = reqwest::Url::parse(url).ok()?;
    match url.path() {
        READ_FILE_STREAM_PATH => Some(INTERNODE_OPERATION_READ_FILE_STREAM),
        PUT_FILE_STREAM_PATH => Some(INTERNODE_OPERATION_PUT_FILE_STREAM),
        WALK_DIR_PATH => Some(INTERNODE_OPERATION_WALK_DIR),
        _ => None,
    }
}

fn http_version_metric_label(version: Version) -> &'static str {
    if version == Version::HTTP_09 {
        HTTP_VERSION_09_LABEL
    } else if version == Version::HTTP_10 {
        HTTP_VERSION_10_LABEL
    } else if version == Version::HTTP_11 {
        HTTP_VERSION_11_LABEL
    } else if version == Version::HTTP_2 {
        HTTP_VERSION_2_LABEL
    } else {
        HTTP_VERSION_UNKNOWN_LABEL
    }
}

fn record_internode_outgoing_request(track: bool, operation: Option<&'static str>) {
    if !track {
        return;
    }

    crate::http_runtime_sources::record_outgoing_request(operation);
}

fn record_internode_sent_bytes(track: bool, operation: Option<&'static str>, bytes: usize) {
    if !track {
        return;
    }

    crate::http_runtime_sources::record_sent_bytes(operation, bytes);
}

fn record_internode_recv_bytes(track: bool, operation: Option<&'static str>, bytes: usize) {
    if !track {
        return;
    }

    crate::http_runtime_sources::record_recv_bytes(operation, bytes);
}

fn record_internode_error(track: bool, operation: Option<&'static str>) {
    if !track {
        return;
    }

    crate::http_runtime_sources::record_error(operation);
}

fn record_internode_classified_error(track: bool, operation: Option<&'static str>, classification: InternodeHttpErrorKind) {
    if !track {
        return;
    }

    if let Some(operation) = operation {
        crate::http_runtime_sources::record_classified_error(operation, classification.metric_label());
    }
}

fn record_internode_operation_duration(track: bool, operation: Option<&'static str>, duration: Duration) {
    if !track {
        return;
    }

    if let Some(operation) = operation {
        crate::http_runtime_sources::record_duration(operation, duration);
    }
}

fn record_internode_operation_duration_once(
    track: bool,
    operation: Option<&'static str>,
    request_started: Instant,
    duration_recorded: &mut bool,
) {
    if *duration_recorded {
        return;
    }

    *duration_recorded = true;
    record_internode_operation_duration(track, operation, request_started.elapsed());
}

fn record_internode_http_version(track: bool, operation: Option<&'static str>, http_version: &'static str) {
    if !track {
        return;
    }

    if let Some(operation) = operation {
        crate::http_runtime_sources::record_http_version(operation, http_version);
    }
}

/// Set once the inert-HTTP/2-tuning warning has been emitted, so it fires at
/// most once for the process lifetime (backlog#805-C3).
static H2_INERT_WARNED: AtomicBool = AtomicBool::new(false);

/// Pure predicate deciding whether to warn that configured HTTP/2 tuning is
/// inert. Warn only when the negotiated protocol is not HTTP/2, the operator
/// explicitly configured h2 tuning, and we have not warned before. HTTP/2 for
/// internode is negotiated via TLS-ALPN; over plaintext the connection is
/// HTTP/1.1 and the h2 window/keepalive knobs are silently ignored.
fn should_warn_h2_inert(negotiated_is_http2: bool, h2_tuning_explicit: bool, already_warned: bool) -> bool {
    !negotiated_is_http2 && h2_tuning_explicit && !already_warned
}

/// Emit a single process-lifetime warning if explicitly-configured HTTP/2
/// tuning is inert because the observed internode connection negotiated a
/// non-HTTP/2 protocol (i.e. HTTP/1.1 over plaintext). See backlog#805-C3.
fn maybe_warn_h2_inert(negotiated_version: Version) {
    let negotiated_is_http2 = negotiated_version == Version::HTTP_2;
    let tuning = internode_http_client_tuning();
    if should_warn_h2_inert(negotiated_is_http2, tuning.h2_tuning_explicit, H2_INERT_WARNED.load(Ordering::Relaxed))
        && H2_INERT_WARNED
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        warn!(
            "internode connection negotiated HTTP/1.1 while HTTP/2 tuning is configured; the HTTP/2 window/keepalive settings apply only over internode TLS (h2 is negotiated via ALPN) — enable internode TLS to use them."
        );
    }
}

fn record_internode_stall_timeout(track: bool, operation: Option<&'static str>) {
    if !track {
        return;
    }

    if let Some(operation) = operation {
        crate::http_runtime_sources::record_stall_timeout(operation);
    }
}

fn record_internode_write_shutdown_error(track: bool, operation: Option<&'static str>) {
    if !track {
        return;
    }

    if let Some(operation) = operation {
        crate::http_runtime_sources::record_write_shutdown_error(operation);
    }
}

fn poll_send_error_to_io<T>(err: tokio_util::sync::PollSendError<T>, context: &str) -> io::Error {
    Error::other(format!("{context}: {err}"))
}

fn send_error_to_io<T>(err: tokio_util::sync::PollSendError<T>, context: &str) -> io::Error {
    Error::other(format!("{context}: {err}"))
}

impl HttpWriter {
    fn start_request(&mut self) {
        if let Some(start_tx) = self.start_tx.take() {
            let _ = start_tx.send(());
        }
    }

    fn take_background_error(&mut self) -> io::Result<()> {
        match self.err_rx.try_recv() {
            Ok(err) => Err(err),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty | tokio::sync::oneshot::error::TryRecvError::Closed) => Ok(()),
        }
    }

    fn poll_send_pending_chunk(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.pending_chunk.is_empty() {
            return Poll::Ready(Ok(()));
        }

        match self.sender.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let chunk = self.pending_chunk.split().freeze();
                self.sender
                    .send_item(Some(chunk))
                    .map_err(|e| send_error_to_io(e, "HttpWriter send error"))?;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(poll_send_error_to_io(e, "HttpWriter send error"))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for HttpWriter {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        // http_log!(
        //     "[HttpWriter::poll_write] url: {}, method: {:?}, buf.len: {}",
        //     self.url,
        //     self.method,
        //     buf.len()
        // );
        let this = self.as_mut().get_mut();
        if let Err(err) = this.take_background_error() {
            return Poll::Ready(Err(err));
        }

        if this.pending_chunk.len() >= HTTP_WRITER_BUFFER_SIZE {
            match this.poll_send_pending_chunk(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            }
        }

        if buf.len() >= HTTP_WRITER_BUFFER_SIZE && this.pending_chunk.is_empty() {
            match this.sender.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {
                    this.sender
                        .send_item(Some(Bytes::copy_from_slice(buf)))
                        .map_err(|e| send_error_to_io(e, "HttpWriter send error"))?;
                    this.start_request();
                    return Poll::Ready(Ok(buf.len()));
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(poll_send_error_to_io(err, "HttpWriter send error"))),
                Poll::Pending => return Poll::Pending,
            }
        }

        this.pending_chunk.extend_from_slice(buf);
        if !buf.is_empty() {
            this.start_request();
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.as_mut().get_mut();
        if let Err(err) = this.take_background_error() {
            return Poll::Ready(Err(err));
        }

        this.poll_send_pending_chunk(cx)
    }

    fn poll_write_vectored(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bufs: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();
        if let Err(err) = this.take_background_error() {
            return Poll::Ready(Err(err));
        }

        if this.pending_chunk.len() >= HTTP_WRITER_BUFFER_SIZE {
            match this.poll_send_pending_chunk(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            }
        }

        let total_len = bufs.iter().map(|buf| buf.len()).sum::<usize>();
        if total_len == 0 {
            return Poll::Ready(Ok(0));
        }

        if bufs.len() == 1 && this.pending_chunk.is_empty() && total_len >= HTTP_WRITER_BUFFER_SIZE {
            match this.sender.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {
                    this.sender
                        .send_item(Some(Bytes::copy_from_slice(bufs[0].as_ref())))
                        .map_err(|e| send_error_to_io(e, "HttpWriter send error"))?;
                    this.start_request();
                    return Poll::Ready(Ok(total_len));
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(poll_send_error_to_io(err, "HttpWriter send error"))),
                Poll::Pending => return Poll::Pending,
            }
        }

        for buf in bufs {
            this.pending_chunk.extend_from_slice(buf);
        }
        if total_len > 0 {
            this.start_request();
        }

        Poll::Ready(Ok(total_len))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // let url = self.url.clone();
        // let method = self.method.clone();

        self.as_mut().get_mut().start_request();
        if let Err(err) = self.as_mut().get_mut().take_background_error() {
            record_internode_write_shutdown_error(self.track_internode_metrics, self.internode_operation);
            return Poll::Ready(Err(err));
        }

        match self.as_mut().get_mut().poll_send_pending_chunk(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => {
                record_internode_write_shutdown_error(self.track_internode_metrics, self.internode_operation);
                return Poll::Ready(Err(err));
            }
            Poll::Pending => return Poll::Pending,
        }

        if !self.finish {
            // http_log!("[HttpWriter::poll_shutdown] url: {}, method: {:?}", url, method);
            let this = self.as_mut().get_mut();
            match this.sender.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {
                    this.sender.send_item(None).map_err(|e| {
                        record_internode_write_shutdown_error(this.track_internode_metrics, this.internode_operation);
                        send_error_to_io(e, "HttpWriter shutdown error")
                    })?;
                }
                Poll::Ready(Err(err)) => {
                    record_internode_write_shutdown_error(this.track_internode_metrics, this.internode_operation);
                    return Poll::Ready(Err(poll_send_error_to_io(err, "HttpWriter shutdown error")));
                }
                Poll::Pending => return Poll::Pending,
            }
            // http_log!(
            //     "[HttpWriter::poll_shutdown] sent shutdown signal to HTTP request, url: {}, method: {:?}",
            //     url,
            //     method
            // );

            self.finish = true;
        }
        // Wait for the HTTP request to complete
        use futures::FutureExt;
        match Pin::new(&mut self.as_mut().get_mut().handle).poll_unpin(cx) {
            Poll::Ready(Ok(Ok(()))) => {
                // http_log!(
                //     "[HttpWriter::poll_shutdown] HTTP request finished successfully, url: {}, method: {:?}",
                //     url,
                //     method
                // );
            }
            Poll::Ready(Ok(Err(err))) => {
                record_internode_write_shutdown_error(self.track_internode_metrics, self.internode_operation);
                return Poll::Ready(Err(err));
            }
            Poll::Ready(Err(e)) => {
                record_internode_write_shutdown_error(self.track_internode_metrics, self.internode_operation);
                // http_log!("[HttpWriter::poll_shutdown] HTTP request failed: {e}, url: {}, method: {:?}", url, method);
                return Poll::Ready(Err(Error::other(format!("HTTP request failed: {e}"))));
            }
            Poll::Pending => {
                // http_log!("[HttpWriter::poll_shutdown] HTTP request pending, url: {}, method: {:?}", url, method);
                return Poll::Pending;
            }
        }

        Poll::Ready(Ok(()))
    }
}

impl Drop for HttpWriter {
    /// Abort the background HTTP request when the writer is dropped without a
    /// clean shutdown. On a stall timeout the caller drops this writer to fail
    /// its shard (rustfs/backlog#1319); a black-hole peer would otherwise leave
    /// the spawned task holding the connection and its buffered body alive. A
    /// cleanly shut-down writer has already joined the task, so aborting a
    /// finished handle is a no-op. This does not — and cannot — unsend bytes
    /// already handed to the transport: those land only in the upload's unique
    /// tmp path and are reclaimed by tmp GC, never touching a committed object.
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, extract::State, http::StatusCode, response::IntoResponse, routing::get};
    use futures::stream::{self, StreamExt as _};
    use http_body_util::BodyExt as _;
    use std::io::{self, IoSlice};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{Mutex, Notify},
    };

    /// Minimal error wrapper whose `source()` returns a boxed inner error, used
    /// to exercise `classify_transport_error` (reqwest::Error cannot be built by
    /// hand). The inner error may itself be an `io::Error` or another wrapper.
    #[derive(Debug)]
    struct WrapperError {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    }

    impl WrapperError {
        fn new(message: &str, source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>) -> Self {
            Self {
                message: message.to_string(),
                source,
            }
        }
    }

    impl std::fmt::Display for WrapperError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.message)
        }
    }

    impl std::error::Error for WrapperError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.source.as_ref().map(|s| s.as_ref() as &(dyn std::error::Error + 'static))
        }
    }

    fn io_source(kind: io::ErrorKind) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        Box::new(io::Error::new(kind, "io"))
    }

    #[test]
    fn classify_io_connection_refused() {
        let err = WrapperError::new("transport", Some(io_source(io::ErrorKind::ConnectionRefused)));
        let kind = classify_transport_error(&err, false, true, false);
        assert_eq!(kind, InternodeHttpErrorKind::ConnectionRefused);
        assert!(kind.is_retryable());
    }

    #[test]
    fn classify_io_reset_variants() {
        for io_kind in [
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::ConnectionAborted,
        ] {
            let err = WrapperError::new("transport", Some(io_source(io_kind)));
            assert_eq!(
                classify_transport_error(&err, false, false, false),
                InternodeHttpErrorKind::ConnectionReset,
                "{io_kind:?} should map to ConnectionReset"
            );
        }
    }

    #[test]
    fn classify_io_unreachable_variants() {
        for io_kind in [io::ErrorKind::HostUnreachable, io::ErrorKind::NetworkUnreachable] {
            let err = WrapperError::new("transport", Some(io_source(io_kind)));
            assert_eq!(
                classify_transport_error(&err, false, true, false),
                InternodeHttpErrorKind::ConnectionRefused,
                "{io_kind:?} should map to ConnectionRefused"
            );
        }
    }

    #[test]
    fn classify_io_timed_out_without_timeout_flag() {
        let err = WrapperError::new("transport", Some(io_source(io::ErrorKind::TimedOut)));
        assert_eq!(
            classify_transport_error(&err, false, false, false),
            InternodeHttpErrorKind::ConnectTimeout
        );
    }

    #[test]
    fn classify_timeout_flag_short_circuits() {
        // Even with an io ConnectionRefused in the chain, is_timeout wins.
        let err = WrapperError::new("transport", Some(io_source(io::ErrorKind::ConnectionRefused)));
        assert_eq!(classify_transport_error(&err, true, true, false), InternodeHttpErrorKind::ConnectTimeout);
    }

    #[test]
    fn classify_connect_dns_marker_without_io_kind() {
        let err = WrapperError::new("connect error: nodename nor servname provided", None);
        assert_eq!(
            classify_transport_error(&err, false, true, false),
            InternodeHttpErrorKind::DnsResolutionFailed
        );
    }

    #[test]
    fn classify_connect_no_dns_marker_defaults_refused() {
        let err = WrapperError::new("connect error", None);
        assert_eq!(
            classify_transport_error(&err, false, true, false),
            InternodeHttpErrorKind::ConnectionRefused
        );
    }

    #[test]
    fn classify_body_only() {
        let err = WrapperError::new("body error", None);
        assert_eq!(
            classify_transport_error(&err, false, false, true),
            InternodeHttpErrorKind::BodyStreamAborted
        );
    }

    #[test]
    fn classify_nothing_is_unknown() {
        let err = WrapperError::new("mystery", None);
        assert_eq!(classify_transport_error(&err, false, false, false), InternodeHttpErrorKind::Unknown);
    }

    #[test]
    fn classify_typed_wins_over_body() {
        // io ConnectionReset present AND is_body: typed classification wins.
        let err = WrapperError::new("stream body error", Some(io_source(io::ErrorKind::ConnectionReset)));
        assert_eq!(
            classify_transport_error(&err, false, false, true),
            InternodeHttpErrorKind::ConnectionReset
        );
    }

    #[test]
    fn classify_typed_wins_over_dns_message() {
        // Message says "dns" but a real io ConnectionRefused is in the chain and
        // is_connect is set: typed signal wins, never mislabeled as DNS.
        let err = WrapperError::new("dns lookup failure", Some(io_source(io::ErrorKind::ConnectionRefused)));
        assert_eq!(
            classify_transport_error(&err, false, true, false),
            InternodeHttpErrorKind::ConnectionRefused
        );
    }

    #[test]
    fn is_retryable_table() {
        use InternodeHttpErrorKind::*;
        for kind in [
            ConnectTimeout,
            ConnectionRefused,
            ConnectionReset,
            BodyStreamAborted,
            DnsResolutionFailed,
            HttpStatus(reqwest::StatusCode::TOO_MANY_REQUESTS),
            HttpStatus(reqwest::StatusCode::BAD_GATEWAY),
            HttpStatus(reqwest::StatusCode::SERVICE_UNAVAILABLE),
            HttpStatus(reqwest::StatusCode::GATEWAY_TIMEOUT),
        ] {
            assert!(kind.is_retryable(), "{kind:?} should be retryable");
        }
        for kind in [
            Unknown,
            HttpStatus(reqwest::StatusCode::NOT_FOUND),
            HttpStatus(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
            HttpStatus(reqwest::StatusCode::BAD_REQUEST),
        ] {
            assert!(!kind.is_retryable(), "{kind:?} should not be retryable");
        }
    }

    #[derive(Clone, Default)]
    struct TestState {
        head_count: Arc<AtomicUsize>,
        get_count: Arc<AtomicUsize>,
        put_count: Arc<AtomicUsize>,
        put_bodies: Arc<Mutex<Vec<Vec<u8>>>>,
        delayed_body: Arc<Notify>,
    }

    async fn get_stream(State(state): State<TestState>) -> impl IntoResponse {
        state.get_count.fetch_add(1, Ordering::SeqCst);
        (StatusCode::OK, Body::from("hello"))
    }

    async fn get_stalling_stream(State(state): State<TestState>) -> impl IntoResponse {
        state.get_count.fetch_add(1, Ordering::SeqCst);
        let body_stream = stream::once(async { Ok::<Bytes, io::Error>(Bytes::from_static(b"hello")) }).chain(stream::pending());
        (StatusCode::OK, Body::from_stream(body_stream))
    }

    async fn get_delayed_first_chunk(State(state): State<TestState>) -> impl IntoResponse {
        state.get_count.fetch_add(1, Ordering::SeqCst);
        let delayed_body = state.delayed_body;
        let body_stream = stream::once(async move {
            delayed_body.notified().await;
            Ok::<Bytes, io::Error>(Bytes::from_static(b"hello"))
        });
        (StatusCode::OK, Body::from_stream(body_stream))
    }

    async fn get_failing_stream(State(state): State<TestState>) -> impl IntoResponse {
        state.get_count.fetch_add(1, Ordering::SeqCst);
        let body_stream =
            stream::once(async { Ok::<Bytes, io::Error>(Bytes::from_static(b"partial")) }).chain(stream::once(async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Err(io::Error::other("stream failed"))
            }));
        (StatusCode::OK, Body::from_stream(body_stream))
    }

    async fn reject_head(State(state): State<TestState>) -> impl IntoResponse {
        state.head_count.fetch_add(1, Ordering::SeqCst);
        StatusCode::METHOD_NOT_ALLOWED
    }

    async fn accept_put(State(state): State<TestState>, body: Body) -> impl IntoResponse {
        state.put_count.fetch_add(1, Ordering::SeqCst);
        let bytes = body.collect().await.unwrap().to_bytes();
        state.put_bodies.lock().await.push(bytes.to_vec());
        StatusCode::OK
    }

    async fn reject_put(State(state): State<TestState>, body: Body) -> impl IntoResponse {
        state.put_count.fetch_add(1, Ordering::SeqCst);
        let bytes = body.collect().await.unwrap().to_bytes();
        state.put_bodies.lock().await.push(bytes.to_vec());
        StatusCode::INTERNAL_SERVER_ERROR
    }

    async fn start_test_server(state: TestState) -> Option<(String, tokio::task::JoinHandle<()>)> {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");
        let app = Router::new()
            .route("/stream", get(get_stream).head(reject_head).put(accept_put))
            .route("/reject-put", get(get_stream).put(reject_put))
            .route("/stall", get(get_stalling_stream))
            .route("/delayed-first", get(get_delayed_first_chunk))
            .route("/fail-after-partial", get(get_failing_stream))
            .with_state(state);

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Some((format!("http://{addr}/stream"), handle))
    }

    #[test]
    fn internode_rpc_operation_maps_known_routes() {
        assert_eq!(
            internode_rpc_operation(&format!("http://node:9000{READ_FILE_STREAM_PATH}?disk=d")),
            Some(INTERNODE_OPERATION_READ_FILE_STREAM)
        );
        assert_eq!(
            internode_rpc_operation(&format!("http://node:9000{PUT_FILE_STREAM_PATH}?disk=d")),
            Some(INTERNODE_OPERATION_PUT_FILE_STREAM)
        );
        assert_eq!(
            internode_rpc_operation(&format!("http://node:9000{WALK_DIR_PATH}?disk=d")),
            Some(INTERNODE_OPERATION_WALK_DIR)
        );
        assert_eq!(internode_rpc_operation("http://node:9000/rustfs/rpc/unknown"), None);
        assert_eq!(
            internode_rpc_operation("http://node:9000/rustfs/rpc/unknown?next=/rustfs/rpc/read_file_stream"),
            None
        );
    }

    #[test]
    fn http_version_metrics_labels_are_low_cardinality() {
        assert_eq!(http_version_metric_label(Version::HTTP_09), HTTP_VERSION_09_LABEL);
        assert_eq!(http_version_metric_label(Version::HTTP_10), HTTP_VERSION_10_LABEL);
        assert_eq!(http_version_metric_label(Version::HTTP_11), HTTP_VERSION_11_LABEL);
        assert_eq!(http_version_metric_label(Version::HTTP_2), HTTP_VERSION_2_LABEL);
    }

    #[tokio::test]
    async fn http_reader_does_not_send_preflight_head() {
        let state = TestState::default();
        let Some((url, handle)) = start_test_server(state.clone()).await else {
            return;
        };

        let mut reader = HttpReader::new(url, Method::GET, HeaderMap::new(), None).await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, b"hello");
        assert_eq!(state.head_count.load(Ordering::SeqCst), 0);
        assert_eq!(state.get_count.load(Ordering::SeqCst), 1);

        handle.abort();
    }

    #[tokio::test]
    async fn http_reader_stall_timeout_triggers_after_progress_stops() {
        let state = TestState::default();
        let Some((base_url, handle)) = start_test_server(state.clone()).await else {
            return;
        };
        let url = base_url.replace("/stream", "/stall");

        let mut reader =
            HttpReader::new_with_stall_timeout(url, Method::GET, HeaderMap::new(), None, Some(Duration::from_millis(20)))
                .await
                .unwrap();

        let mut first = [0u8; 5];
        reader.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, b"hello");

        let mut next = [0u8; 1];
        let read_result = tokio::time::timeout(Duration::from_secs(1), reader.read(&mut next))
            .await
            .expect("stall timeout should wake reader");
        let err = match read_result {
            Ok(_) => panic!("reader should return a timeout error"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);

        handle.abort();
    }

    #[tokio::test]
    async fn http_reader_stall_timeout_starts_when_read_is_polled() {
        let state = TestState::default();
        let Some((base_url, handle)) = start_test_server(state.clone()).await else {
            return;
        };
        let url = base_url.replace("/stream", "/delayed-first");

        let mut reader =
            HttpReader::new_with_stall_timeout(url, Method::GET, HeaderMap::new(), None, Some(Duration::from_millis(30)))
                .await
                .expect("reader should be created before the body is ready");

        time::sleep(Duration::from_millis(60)).await;

        let delayed_body = state.delayed_body.clone();
        let read_result = tokio::time::timeout(Duration::from_secs(1), async move {
            let mut first = [0u8; 5];
            let read = tokio::spawn(async move {
                reader.read_exact(&mut first).await?;
                Ok::<_, io::Error>(first)
            });
            time::sleep(Duration::from_millis(5)).await;
            delayed_body.notify_waiters();
            read.await.expect("read task should not panic")
        })
        .await
        .expect("delayed body should arrive before the active stall timeout")
        .expect("reader should not time out before its first active poll");

        assert_eq!(&read_result, b"hello");

        handle.abort();
    }

    #[tokio::test]
    async fn http_writer_does_not_send_empty_preflight_put() {
        let state = TestState::default();
        let Some((url, handle)) = start_test_server(state.clone()).await else {
            return;
        };

        let mut writer = HttpWriter::new(url, Method::PUT, HeaderMap::new()).await.unwrap();
        writer.write_all(b"payload").await.unwrap();
        writer.shutdown().await.unwrap();

        assert_eq!(state.put_count.load(Ordering::SeqCst), 1);
        assert_eq!(state.put_bodies.lock().await.as_slice(), &[b"payload".to_vec()]);

        handle.abort();
    }

    #[tokio::test]
    async fn http_writer_drop_before_first_write_does_not_send_put() {
        let state = TestState::default();
        let Some((url, server_handle)) = start_test_server(state.clone()).await else {
            return;
        };

        let writer = HttpWriter::new(url, Method::PUT, HeaderMap::new()).await.unwrap();
        // Dropping an unstarted writer aborts its parked background task
        // (rustfs/backlog#1319) before it can ever send a PUT.
        drop(writer);
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(state.put_count.load(Ordering::SeqCst), 0);
        assert!(state.put_bodies.lock().await.is_empty());

        server_handle.abort();
    }

    /// A PUT handler that registers the request, then parks forever without ever
    /// sending a response — modeling a black-hole peer that accepts the
    /// connection but never completes the request, so the writer's background
    /// task stays parked at `request.send().await` until it is aborted.
    async fn hanging_put(State(state): State<TestState>, _body: Body) -> impl IntoResponse {
        state.put_count.fetch_add(1, Ordering::SeqCst);
        std::future::pending::<()>().await;
        StatusCode::OK
    }

    // rustfs/backlog#1319: when a stalled remote writer is dropped (the encode
    // path drops it to fail its shard), the background HTTP task must be aborted
    // so it stops holding the connection and buffered body — it must not linger.
    #[tokio::test]
    async fn http_writer_drop_aborts_background_request_to_hanging_peer() {
        let state = TestState::default();
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");
        let app = Router::new()
            .route("/hang", axum::routing::put(hanging_put))
            .with_state(state.clone());
        let server_handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/hang");
        let mut writer = HttpWriter::new(url, Method::PUT, HeaderMap::new()).await.unwrap();
        // A >1MiB write starts the request and hands the body to the background
        // task, which then parks in the handler that never responds.
        writer.write_all(&vec![0xa5u8; HTTP_WRITER_BUFFER_SIZE + 1]).await.unwrap();
        // Let the server register the request, so the background task is parked
        // (alive) at send() before we drop the writer.
        for _ in 0..100 {
            if state.put_count.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(state.put_count.load(Ordering::SeqCst), 1, "server should have accepted the request");

        // Observe the background task via its abort handle; it must still be
        // running before the drop, then be aborted (finished) after it.
        let task = writer.handle.abort_handle();
        assert!(!task.is_finished(), "background task should still be running before drop");

        drop(writer);

        let mut aborted = false;
        for _ in 0..500 {
            if task.is_finished() {
                aborted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(aborted, "dropping the writer must abort the background HTTP task");

        server_handle.abort();
    }

    #[tokio::test]
    async fn http_writer_shutdown_without_write_sends_empty_put() {
        let state = TestState::default();
        let Some((url, handle)) = start_test_server(state.clone()).await else {
            return;
        };

        let mut writer = HttpWriter::new(url, Method::PUT, HeaderMap::new()).await.unwrap();
        writer.shutdown().await.unwrap();

        assert_eq!(state.put_count.load(Ordering::SeqCst), 1);
        assert_eq!(state.put_bodies.lock().await.as_slice(), &[Vec::<u8>::new()]);

        handle.abort();
    }

    #[tokio::test]
    async fn http_writer_handles_many_small_writes() {
        let state = TestState::default();
        let Some((url, handle)) = start_test_server(state.clone()).await else {
            return;
        };

        let mut writer = HttpWriter::new(url, Method::PUT, HeaderMap::new()).await.unwrap();
        let chunk = b"0123456789abcdef";
        let mut expected = Vec::new();
        for _ in 0..256 {
            writer.write_all(chunk).await.unwrap();
            expected.extend_from_slice(chunk);
        }
        writer.shutdown().await.unwrap();

        assert_eq!(state.put_count.load(Ordering::SeqCst), 1);
        assert_eq!(state.put_bodies.lock().await.as_slice(), &[expected]);

        handle.abort();
    }

    #[tokio::test]
    async fn http_writer_supports_vectored_writes() {
        let state = TestState::default();
        let Some((url, handle)) = start_test_server(state.clone()).await else {
            return;
        };

        let mut writer = HttpWriter::new(url, Method::PUT, HeaderMap::new()).await.unwrap();
        let bufs = [IoSlice::new(b"hello "), IoSlice::new(b"world")];
        let written = writer.write_vectored(&bufs).await.unwrap();
        assert_eq!(written, 11);
        writer.shutdown().await.unwrap();

        assert_eq!(state.put_count.load(Ordering::SeqCst), 1);
        assert_eq!(state.put_bodies.lock().await.as_slice(), &[b"hello world".to_vec()]);

        handle.abort();
    }

    #[tokio::test]
    async fn http_writer_shutdown_reports_http_status_error() {
        let state = TestState::default();
        let Some((base_url, handle)) = start_test_server(state.clone()).await else {
            return;
        };
        let url = base_url.replace("/stream", "/reject-put");

        let mut writer = HttpWriter::new(url, Method::PUT, HeaderMap::new()).await.unwrap();
        writer.write_all(b"payload").await.unwrap();
        let err = writer
            .shutdown()
            .await
            .expect_err("shutdown should report the HTTP response failure");

        let source = err
            .get_ref()
            .and_then(|source| source.downcast_ref::<InternodeHttpError>())
            .expect("expected shutdown error to carry InternodeHttpError source");
        assert_eq!(
            source.kind(),
            InternodeHttpErrorKind::HttpStatus(reqwest::StatusCode::INTERNAL_SERVER_ERROR)
        );
        assert_eq!(source.context().method(), "PUT");
        assert!(source.context().target().contains("/reject-put"));
        assert_eq!(state.put_count.load(Ordering::SeqCst), 1);
        assert_eq!(state.put_bodies.lock().await.as_slice(), &[b"payload".to_vec()]);

        handle.abort();
    }

    #[tokio::test]
    async fn http_reader_request_error_includes_method_and_url() {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");
        drop(listener);

        let url = format!("http://{addr}/stream");
        let err = match HttpReader::new(url.clone(), Method::GET, HeaderMap::new(), None).await {
            Ok(_) => panic!("closed listener should trigger request error"),
            Err(err) => err,
        };

        let source = err
            .get_ref()
            .and_then(|source| source.downcast_ref::<InternodeHttpError>())
            .expect("expected InternodeHttpError source");
        assert_eq!(source.context().method(), "GET");
        assert!(source.context().target().contains("/stream"));
    }

    #[tokio::test]
    async fn http_reader_surfaces_body_error_after_partial_data() {
        let state = TestState::default();
        let Some((base_url, handle)) = start_test_server(state).await else {
            return;
        };
        let url = base_url.replace("/stream", "/fail-after-partial");
        let mut reader = HttpReader::new(url, Method::GET, HeaderMap::new(), None)
            .await
            .expect("reader should accept the successful response headers");
        let mut partial = [0_u8; 7];

        reader
            .read_exact(&mut partial)
            .await
            .expect("partial response bytes should arrive before the terminal error");
        let err = reader
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("terminal body errors must not become clean EOF");

        assert_eq!(&partial, b"partial");
        let source = err
            .get_ref()
            .and_then(|source| source.downcast_ref::<InternodeHttpError>())
            .expect("body error should retain internode classification");
        assert_eq!(source.kind(), InternodeHttpErrorKind::BodyStreamAborted);

        handle.abort();
    }

    #[test]
    fn classify_http_status_marks_retryable_gateway_errors() {
        let unavailable = classify_http_status(reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let bad_gateway = classify_http_status(reqwest::StatusCode::BAD_GATEWAY);
        let bad_request = classify_http_status(reqwest::StatusCode::BAD_REQUEST);

        assert!(unavailable.is_retryable());
        assert!(bad_gateway.is_retryable());
        assert!(!bad_request.is_retryable());
    }

    #[test]
    fn dns_resolution_error_uses_network_io_kind() {
        let err = internode_classified_error(
            &Method::GET,
            "http://missing.invalid/rustfs/rpc/read_file_stream",
            Some(INTERNODE_OPERATION_READ_FILE_STREAM),
            InternodeHttpErrorKind::DnsResolutionFailed,
        );

        assert_eq!(err.kind(), io::ErrorKind::AddrNotAvailable);
        assert!(err.to_string().contains("internode dns resolution failed"));
        assert!(err.to_string().contains("GET /rustfs/rpc/read_file_stream"));
    }

    #[test]
    fn internode_status_error_preserves_classification_source_and_context() {
        let err = internode_status_error(
            &Method::PUT,
            "http://node:9000/rustfs/rpc/put_file_stream?disk=disk-a",
            Some(INTERNODE_OPERATION_PUT_FILE_STREAM),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
        );

        let source = err
            .get_ref()
            .and_then(|source| source.downcast_ref::<InternodeHttpError>())
            .expect("expected status error to carry InternodeHttpError source");
        assert_eq!(
            source.kind(),
            InternodeHttpErrorKind::HttpStatus(reqwest::StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(source.context().method(), "PUT");
        assert_eq!(source.context().target(), PUT_FILE_STREAM_PATH);
        assert!(!err.to_string().contains("disk-a"));
    }

    #[test]
    fn internode_request_context_redacts_malformed_targets() {
        let context =
            internode_request_context(&Method::GET, "not a url?disk=/sensitive/path", Some(INTERNODE_OPERATION_READ_FILE_STREAM));

        assert_eq!(context.target(), "<invalid-internode-url>");
        assert!(!context.to_string().contains("sensitive"));
    }

    #[test]
    fn internode_http_error_debug_redacts_source_details() {
        let context = InternodeHttpRequestContext {
            method: "GET".to_string(),
            target: WALK_DIR_PATH.to_string(),
            operation: Some(INTERNODE_OPERATION_WALK_DIR),
        };
        let error = InternodeHttpError::with_source(
            InternodeHttpErrorKind::BodyStreamAborted,
            context,
            io::Error::other("http://node/rustfs/rpc/walk_dir?disk=/sensitive/path&token=private"),
        );
        let debug = format!("{error:?}");

        assert!(debug.contains("source_present: true"));
        assert!(!debug.contains("sensitive"));
        assert!(!debug.contains("private"));
    }

    #[test]
    fn test_internode_http_error_test_helper_is_retryable() {
        let err = new_test_internode_http_io_error(InternodeHttpErrorKind::ConnectionReset);
        let source = err
            .get_ref()
            .and_then(|source| source.downcast_ref::<InternodeHttpError>())
            .expect("expected test helper to carry InternodeHttpError source");

        assert_eq!(source.kind(), InternodeHttpErrorKind::ConnectionReset);
        assert!(source.kind().is_retryable());
        assert_eq!(source.context().method(), "PUT");
        assert!(source.context().target().contains(PUT_FILE_STREAM_PATH));
    }

    #[test]
    fn loopback_urls_bypass_proxy_selection() {
        assert!(should_bypass_proxy_for_url("http://127.0.0.1:9000/stream"));
        assert!(should_bypass_proxy_for_url("http://localhost:9000/stream"));
        assert!(should_bypass_proxy_for_url("http://[::1]:9000/stream"));
        assert!(!should_bypass_proxy_for_url("http://192.168.1.10:9000/stream"));
        assert!(!should_bypass_proxy_for_url("http://example.com/stream"));
        assert!(!should_bypass_proxy_for_url("not-a-url"));
    }

    #[test]
    fn internode_http_tuning_profile_parses_known_values_and_falls_back_to_legacy() {
        assert_eq!(parse_internode_http_tuning_profile(None), InternodeHttpTuningProfile::Legacy);
        assert_eq!(
            parse_internode_http_tuning_profile(Some("balanced")),
            InternodeHttpTuningProfile::Balanced
        );
        assert_eq!(
            parse_internode_http_tuning_profile(Some(" Throughput ")),
            InternodeHttpTuningProfile::Throughput
        );
        assert_eq!(
            parse_internode_http_tuning_profile(Some("aggressive")),
            InternodeHttpTuningProfile::Legacy
        );
    }

    #[test]
    fn legacy_internode_http_tuning_keeps_existing_defaults() {
        let tuning = InternodeHttpClientTuning::from_values(
            InternodeHttpTuningProfile::Legacy,
            None,
            None,
            None,
            None,
            InternodeHttpTuningProfile::Legacy.default_http2_adaptive_window(),
            None,
        );

        assert_eq!(tuning.pool_max_idle_per_host, None);
        assert_eq!(tuning.pool_idle_timeout_secs, None);
        assert_eq!(tuning.http2_initial_stream_window_size, None);
        assert_eq!(tuning.http2_initial_connection_window_size, None);
        assert!(!tuning.http2_adaptive_window);
        assert_eq!(tuning.proxy_mode, InternodeHttpProxyMode::Legacy);
    }

    #[test]
    fn balanced_and_throughput_profiles_apply_conservative_defaults() {
        let balanced = InternodeHttpClientTuning::from_values(
            InternodeHttpTuningProfile::Balanced,
            None,
            None,
            None,
            None,
            InternodeHttpTuningProfile::Balanced.default_http2_adaptive_window(),
            None,
        );
        let throughput = InternodeHttpClientTuning::from_values(
            InternodeHttpTuningProfile::Throughput,
            None,
            None,
            None,
            None,
            InternodeHttpTuningProfile::Throughput.default_http2_adaptive_window(),
            None,
        );

        assert_eq!(balanced.pool_max_idle_per_host, Some(64));
        assert_eq!(balanced.pool_idle_timeout_secs, Some(120));
        assert_eq!(balanced.http2_initial_stream_window_size, Some(1024 * 1024));
        assert_eq!(balanced.http2_initial_connection_window_size, Some(4 * 1024 * 1024));
        assert!(!balanced.http2_adaptive_window);
        assert_eq!(balanced.proxy_mode, InternodeHttpProxyMode::NoProxy);

        assert_eq!(throughput.pool_max_idle_per_host, Some(256));
        assert_eq!(throughput.pool_idle_timeout_secs, Some(300));
        assert!(throughput.http2_adaptive_window);
        assert_eq!(throughput.proxy_mode, InternodeHttpProxyMode::NoProxy);
    }

    #[test]
    fn internode_http_tuning_overrides_are_clamped() {
        let tuning = InternodeHttpClientTuning::from_values(
            InternodeHttpTuningProfile::Balanced,
            Some(8),
            Some(30),
            Some(1),
            Some(u64::MAX),
            false,
            Some("system"),
        );

        assert_eq!(tuning.pool_max_idle_per_host, Some(8));
        assert_eq!(tuning.pool_idle_timeout_secs, Some(30));
        assert_eq!(tuning.http2_initial_stream_window_size, Some(INTERNODE_HTTP2_WINDOW_MIN));
        assert_eq!(tuning.http2_initial_connection_window_size, Some(INTERNODE_HTTP2_WINDOW_MAX));
        assert_eq!(tuning.proxy_mode, InternodeHttpProxyMode::System);
    }

    #[test]
    fn internode_http_proxy_policy_matches_profile_and_overrides() {
        let legacy =
            InternodeHttpClientTuning::from_values(InternodeHttpTuningProfile::Legacy, None, None, None, None, false, None);
        let balanced =
            InternodeHttpClientTuning::from_values(InternodeHttpTuningProfile::Balanced, None, None, None, None, false, None);
        let system_proxy = InternodeHttpClientTuning::from_values(
            InternodeHttpTuningProfile::Throughput,
            None,
            None,
            None,
            None,
            true,
            Some("system"),
        );
        let no_proxy = InternodeHttpClientTuning::from_values(
            InternodeHttpTuningProfile::Legacy,
            None,
            None,
            None,
            None,
            false,
            Some("off"),
        );

        assert!(should_disable_proxy_for_url("http://127.0.0.1:9000/stream", legacy));
        assert!(!should_disable_proxy_for_url("http://192.168.1.10:9000/stream", legacy));
        assert!(should_disable_proxy_for_url("http://192.168.1.10:9000/stream", balanced));
        assert!(!should_disable_proxy_for_url("http://127.0.0.1:9000/stream", system_proxy));
        assert!(should_disable_proxy_for_url("http://example.com/stream", no_proxy));
    }

    // backlog#805-C3: warn once when configured HTTP/2 tuning is inert over
    // plaintext internode (HTTP/1.1) transport.

    #[test]
    fn should_warn_h2_inert_truth_table() {
        // HTTP/1.1 + explicit tuning + not yet warned -> warn.
        assert!(should_warn_h2_inert(false, true, false));
        // Negotiated HTTP/2 -> tuning is actually active, never warn.
        assert!(!should_warn_h2_inert(true, true, false));
        // No explicit tuning -> nothing to warn about.
        assert!(!should_warn_h2_inert(false, false, false));
        // Already warned -> never warn again (Once semantics).
        assert!(!should_warn_h2_inert(false, true, true));
        // HTTP/2 and already warned combinations stay false too.
        assert!(!should_warn_h2_inert(true, false, false));
        assert!(!should_warn_h2_inert(true, true, true));
    }

    #[test]
    fn h2_tuning_explicit_reflects_windows_and_profile() {
        // Legacy profile with no explicit windows -> not explicit.
        let legacy =
            InternodeHttpClientTuning::from_values(InternodeHttpTuningProfile::Legacy, None, None, None, None, false, None);
        assert!(!legacy.h2_tuning_explicit);

        // Explicit stream window on the Legacy profile -> explicit.
        let explicit_stream = InternodeHttpClientTuning::from_values(
            InternodeHttpTuningProfile::Legacy,
            None,
            None,
            Some(1024 * 1024),
            None,
            false,
            None,
        );
        assert!(explicit_stream.h2_tuning_explicit);

        // Explicit connection window on the Legacy profile -> explicit.
        let explicit_conn = InternodeHttpClientTuning::from_values(
            InternodeHttpTuningProfile::Legacy,
            None,
            None,
            None,
            Some(4 * 1024 * 1024),
            false,
            None,
        );
        assert!(explicit_conn.h2_tuning_explicit);

        // A non-default (Balanced) profile implies explicit h2 tuning.
        let balanced =
            InternodeHttpClientTuning::from_values(InternodeHttpTuningProfile::Balanced, None, None, None, None, false, None);
        assert!(balanced.h2_tuning_explicit);
    }
}
