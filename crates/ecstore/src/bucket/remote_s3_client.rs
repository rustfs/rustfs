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

//! Shared builder for outbound `aws_sdk_s3::Client`s.
//!
//! Replication targets (`bucket_target_sys`) and the on-demand migration
//! source client build their remote clients from one neutral
//! [`RemoteS3EndpointSpec`]: endpoint assembly, credential handling, path-style
//! selection, custom CA / skip-TLS transports, the SDK retry policy and the
//! outbound SSRF gate all live here so both callers share exactly one policy.
//! The retry policy is the one knob the two consumers deliberately disagree
//! on, so [`RemoteS3EndpointSpec::retry`] is a required field rather than an
//! inherited SDK default. The gate keeps the
//! relaxed replication semantics documented in
//! `docs/operations/outbound-connection-policy.md`: private addresses are
//! always allowed, loopback only behind `RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET`.

use aws_credential_types::Credentials as SdkCredentials;
use aws_credential_types::provider::{ProvideCredentials, error::CredentialsError, future};
use aws_sdk_s3::config::Region as SdkRegion;
use aws_sdk_s3::config::RequestChecksumCalculation;
use aws_sdk_s3::config::SharedCredentialsProvider;
use aws_sdk_s3::config::SharedHttpClient;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::{Client as S3Client, Config as S3Config};
use aws_smithy_http_client::{Builder as SmithyHttpClientBuilder, tls as smithy_tls};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::http::{
    HttpConnector as SmithyHttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::config_bag::ConfigBag;
use aws_smithy_types::timeout::TimeoutConfig;
use http::Uri;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use rustfs_config::{DEFAULT_TRUST_LEAF_CERT_AS_CA, ENV_TRUST_LEAF_CERT_AS_CA, RUSTFS_CA_CERT, RUSTFS_TLS_CERT};
use rustfs_utils::egress::{OutboundUrlError, validate_outbound_url};
use rustls_pki_types::pem::PemObject;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tower::Service;
use tracing::warn;
use url::Url;

const REDACTED_CREDENTIAL: &str = "<redacted>";
pub(crate) const EXPIRED_REMOTE_TARGET_CREDENTIALS: &str = "remote target credentials have expired";

/// Request addressing style for a remote S3-compatible endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathStyle {
    /// Caller did not choose; the builder defaults to path-style because that
    /// is what custom S3-compatible endpoints accept most reliably.
    Auto,
    /// `https://endpoint/bucket/key`.
    Path,
    /// `https://bucket.endpoint/key`.
    VirtualHost,
}

impl PathStyle {
    /// Resolves the style to the SDK `force_path_style` flag. `Auto` keeps
    /// the historical replication default (path-style).
    pub fn force_path_style(self) -> bool {
        !matches!(self, PathStyle::VirtualHost)
    }
}

/// SDK-level retry policy for a remote client. Retries are invisible to the
/// caller — one logical call becomes several wire requests — so every consumer
/// states its own instead of inheriting the SDK default: a caller that already
/// owns a retry budget would otherwise multiply it against an endpoint that is
/// by definition already failing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteS3RetryPolicy {
    /// One logical call is exactly one wire request; the caller owns the
    /// retry budget.
    Disabled,
    /// Smithy's standard strategy, capped at `max_attempts` attempts in total
    /// (the initial request included). Values below 1 are clamped to 1.
    Standard { max_attempts: u32 },
}

/// The SDK default replication targets have always run with, written out so a
/// change to it is a change to this line rather than to a dependency default.
pub const REPLICATION_TARGET_RETRY_POLICY: RemoteS3RetryPolicy = RemoteS3RetryPolicy::Standard { max_attempts: 3 };

impl RemoteS3RetryPolicy {
    fn retry_config(self) -> RetryConfig {
        match self {
            RemoteS3RetryPolicy::Disabled => RetryConfig::disabled(),
            RemoteS3RetryPolicy::Standard { max_attempts } => RetryConfig::standard().with_max_attempts(max_attempts.max(1)),
        }
    }
}

/// Static or temporary credentials for a remote endpoint. `expiration` without
/// a `session_token` is rejected at build time: only STS-style temporary
/// credentials expire, so that combination is a corrupted configuration
/// rather than a static key.
#[derive(Clone)]
pub struct RemoteCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub expiration: Option<SystemTime>,
    /// SDK credential `account_id`; replication targets pass their reset id.
    pub account_id: String,
}

impl fmt::Debug for RemoteCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteCredentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &REDACTED_CREDENTIAL)
            .field("session_token", &self.session_token.as_ref().map(|_| REDACTED_CREDENTIAL))
            .field("expiration", &self.expiration)
            .field("account_id", &self.account_id)
            .finish()
    }
}

/// Neutral description of a remote S3 endpoint from which an
/// `aws_sdk_s3::Client` is built.
#[derive(Clone, Debug)]
pub struct RemoteS3EndpointSpec {
    /// `host[:port]` without a scheme; `secure` selects `https` or `http`.
    pub endpoint: String,
    pub secure: bool,
    pub region: String,
    pub path_style: PathStyle,
    pub credentials: Option<RemoteCredentials>,
    /// Accept any server certificate. Takes priority over `ca_cert_pem`.
    pub skip_tls_verify: bool,
    /// Extra PEM bundle trusted alongside the platform roots and the
    /// `RUSTFS_TLS_PATH` bundle. `None` and whitespace-only mean "not set".
    pub ca_cert_pem: Option<String>,
    pub connect_timeout: Option<Duration>,
    pub read_timeout: Option<Duration>,
    /// How many wire requests one logical call may cost. Every consumer
    /// declares it; see [`RemoteS3RetryPolicy`].
    pub retry: RemoteS3RetryPolicy,
    /// Appended to the SDK `User-Agent` (space separated) so the remote side
    /// can identify the caller; empty means no suffix.
    pub user_agent_suffix: &'static str,
}

impl RemoteS3EndpointSpec {
    /// Full endpoint URL (`scheme://host[:port]`) as handed to the SDK.
    pub fn endpoint_url(&self) -> String {
        if self.secure {
            format!("https://{}", self.endpoint)
        } else {
            format!("http://{}", self.endpoint)
        }
    }

    fn custom_ca_pem(&self) -> Option<&str> {
        self.ca_cert_pem.as_deref().filter(|pem| !pem.trim().is_empty())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RemoteS3ClientError {
    #[error("the {0} backend is not included in this build")]
    BackendNotCompiled(&'static str),
    #[error("remote endpoint requires credentials")]
    MissingCredentials,
    #[error("{0}")]
    Credentials(&'static str),
    #[error("invalid target endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("target endpoint is not allowed: {0}")]
    EndpointNotAllowed(#[source] OutboundUrlError),
    #[error("invalid target CA PEM: {0}")]
    InvalidCaPem(String),
}

#[derive(Clone)]
pub(crate) struct RemoteTargetCredentialsProvider {
    pub(crate) credentials: SdkCredentials,
}

impl RemoteTargetCredentialsProvider {
    pub(crate) fn resolve_at(&self, now: SystemTime) -> aws_credential_types::provider::Result {
        if self.credentials.expiry().is_some_and(|expiration| expiration <= now) {
            return Err(CredentialsError::provider_error(std::io::Error::other(EXPIRED_REMOTE_TARGET_CREDENTIALS)));
        }
        Ok(self.credentials.clone())
    }
}

impl fmt::Debug for RemoteTargetCredentialsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteTargetCredentialsProvider")
            .field("temporary", &self.credentials.session_token().is_some())
            .field("expiration", &self.credentials.expiry())
            .finish()
    }
}

impl ProvideCredentials for RemoteTargetCredentialsProvider {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::ready(self.resolve_at(SystemTime::now()))
    }

    fn fallback_on_interrupt(&self) -> Option<SdkCredentials> {
        self.resolve_at(SystemTime::now()).ok()
    }
}

pub(crate) fn remote_sdk_credentials(credentials: &RemoteCredentials, now: SystemTime) -> Result<SdkCredentials, &'static str> {
    if credentials.expiration.is_some() && credentials.session_token.is_none() {
        return Err("remote target credential expiration requires a session token");
    }
    if credentials.expiration.is_some_and(|expiration| expiration <= now) {
        return Err(EXPIRED_REMOTE_TARGET_CREDENTIALS);
    }

    let mut builder = SdkCredentials::builder()
        .access_key_id(credentials.access_key.clone())
        .secret_access_key(credentials.secret_key.clone())
        .account_id(credentials.account_id.clone())
        .provider_name("bucket_target_sys");
    if let Some(session_token) = &credentials.session_token {
        builder = builder.session_token(session_token.clone());
    }
    if let Some(expiration) = credentials.expiration {
        builder = builder.expiry(expiration);
    }
    Ok(builder.build())
}

/// Appends a caller-identifying token to the SDK `User-Agent`. Runs after
/// signing: SigV4 excludes `user-agent` from the canonical request, so the
/// signature stays valid.
#[derive(Debug)]
struct UserAgentSuffixInterceptor {
    suffix: &'static str,
}

impl Intercept for UserAgentSuffixInterceptor {
    fn name(&self) -> &'static str {
        "RustfsUserAgentSuffix"
    }

    fn modify_before_transmit(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context.request_mut().headers_mut();
        let user_agent = match headers.get(http::header::USER_AGENT.as_str()) {
            Some(existing) => format!("{existing} {}", self.suffix),
            None => self.suffix.to_string(),
        };
        headers.try_insert(http::header::USER_AGENT.as_str(), user_agent)?;
        Ok(())
    }
}

/// Builds the SDK config for `spec` without finalizing it, so callers can add
/// interceptors or (in tests) swap the HTTP client before `build()`.
pub(crate) async fn build_remote_s3_config(
    spec: &RemoteS3EndpointSpec,
) -> Result<aws_sdk_s3::config::Builder, RemoteS3ClientError> {
    let Some(credentials) = &spec.credentials else {
        return Err(RemoteS3ClientError::MissingCredentials);
    };
    let creds = remote_sdk_credentials(credentials, SystemTime::now()).map_err(RemoteS3ClientError::Credentials)?;

    let endpoint = spec.endpoint_url();
    let parsed_endpoint = Url::parse(&endpoint).map_err(|err| RemoteS3ClientError::InvalidEndpoint(err.to_string()))?;
    validate_remote_endpoint(&parsed_endpoint).map_err(RemoteS3ClientError::EndpointNotAllowed)?;

    let mut config_builder = S3Config::builder()
        .endpoint_url(endpoint)
        .credentials_provider(SharedCredentialsProvider::new(RemoteTargetCredentialsProvider { credentials: creds }))
        .region(SdkRegion::new(spec.region.clone()))
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .request_checksum_calculation(replication_request_checksum_calculation())
        .retry_config(spec.retry.retry_config());

    if spec.path_style.force_path_style() {
        config_builder = config_builder.force_path_style(true);
    }

    if let Some(http_client) = build_aws_s3_http_client_for_spec(spec).await? {
        config_builder = config_builder.http_client(http_client);
    }

    if spec.connect_timeout.is_some() || spec.read_timeout.is_some() {
        let mut timeouts = TimeoutConfig::builder();
        if let Some(connect_timeout) = spec.connect_timeout {
            timeouts = timeouts.connect_timeout(connect_timeout);
        }
        if let Some(read_timeout) = spec.read_timeout {
            timeouts = timeouts.read_timeout(read_timeout);
        }
        config_builder = config_builder.timeout_config(timeouts.build());
    }

    if !spec.user_agent_suffix.is_empty() {
        config_builder = config_builder.interceptor(UserAgentSuffixInterceptor {
            suffix: spec.user_agent_suffix,
        });
    }

    Ok(config_builder)
}

/// Builds an `aws_sdk_s3::Client` for `spec`, applying the outbound endpoint
/// gate, credential validation and the TLS transport selection.
pub async fn build_remote_s3_client(spec: &RemoteS3EndpointSpec) -> Result<S3Client, RemoteS3ClientError> {
    Ok(S3Client::from_conf(build_remote_s3_config(spec).await?.build()))
}

#[derive(Debug)]
struct AcceptAnyServerCertVerifier;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Clone)]
struct TargetHyperHttpConnector<C> {
    client: HyperClient<C, SdkBody>,
}

impl<C> fmt::Debug for TargetHyperHttpConnector<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TargetHyperHttpConnector")
            .field("client", &"** hyper client **")
            .finish()
    }
}

impl<C> SmithyHttpConnector for TargetHyperHttpConnector<C>
where
    C: Clone + Send + Sync + 'static,
    C: Service<Uri>,
    C::Response:
        hyper::rt::Read + hyper::rt::Write + hyper_util::client::legacy::connect::Connection + Send + Sync + Unpin + 'static,
    C::Future: Unpin + Send + 'static,
    C::Error: Into<BoxError>,
{
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let request = match request.try_into_http1x() {
            Ok(request) => request,
            Err(err) => return HttpConnectorFuture::ready(Err(ConnectorError::user(err.into()))),
        };

        let mut client = self.client.clone();
        let fut = client.call(request);
        HttpConnectorFuture::new(async move {
            let response = fut
                .await
                .map_err(|err| ConnectorError::io(err.into()))?
                .map(SdkBody::from_body_1_x);
            HttpResponse::try_from(response).map_err(|err| ConnectorError::other(err.into(), None))
        })
    }
}

pub(crate) fn ensure_rustls_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }
}

/// Env opt-in that re-enables loopback replication targets. Loopback (`127.0.0.1`,
/// `::1`, `localhost`) is a classic SSRF vector and stays rejected by default, but
/// single-host multi-instance dev setups and the e2e harness legitimately replicate
/// over loopback. Never set this in production.
const ALLOW_LOOPBACK_REPLICATION_TARGET_ENV: &str = "RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET";

fn loopback_replication_targets_allowed() -> bool {
    std::env::var(ALLOW_LOOPBACK_REPLICATION_TARGET_ENV)
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
}

const REPLICATION_STREAMING_CHECKSUMS_ENV: &str = "RUSTFS_REPLICATION_STREAMING_CHECKSUMS";

/// Streaming trailer checksums make the SDK frame request bodies as
/// `aws-chunked`; a target that does not decode that framing stores the frames
/// verbatim, silently corrupting every replica while the transfer itself
/// succeeds (#6853). Plain signed payloads are the compatible default; the env
/// knob restores trailer checksums for fleets whose targets are all known to
/// decode them.
pub(crate) fn replication_request_checksum_calculation() -> RequestChecksumCalculation {
    if std::env::var(REPLICATION_STREAMING_CHECKSUMS_ENV)
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
    {
        RequestChecksumCalculation::WhenSupported
    } else {
        RequestChecksumCalculation::WhenRequired
    }
}

/// Outbound gate for operator-configured remote endpoints (replication
/// targets, on-demand migration sources). See
/// `docs/operations/outbound-connection-policy.md`.
pub fn validate_remote_endpoint(url: &Url) -> Result<(), OutboundUrlError> {
    validate_remote_endpoint_inner(url, loopback_replication_targets_allowed())
}

pub(crate) fn validate_remote_endpoint_inner(url: &Url, allow_loopback: bool) -> Result<(), OutboundUrlError> {
    match validate_outbound_url(url) {
        Ok(()) => Ok(()),
        // Replication targets are trusted infrastructure the operator configures, and
        // legitimately live on private networks, so private addresses are always allowed.
        Err(OutboundUrlError::ForbiddenHost {
            reason: "private address",
            ..
        }) => Ok(()),
        // Loopback is far higher SSRF risk, so it is allowed only under the explicit,
        // off-by-default opt-in above (single-host multi-instance / the e2e harness).
        Err(OutboundUrlError::ForbiddenHost {
            reason: "loopback address" | "loopback host",
            ..
        }) if allow_loopback => Ok(()),
        Err(err) => Err(err),
    }
}

pub(crate) fn build_insecure_aws_s3_http_client() -> SharedHttpClient {
    ensure_rustls_crypto_provider();

    let tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCertVerifier))
        .with_no_client_auth();

    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let mut client_builder = HyperClient::builder(TokioExecutor::new());
    client_builder.pool_timer(TokioTimer::new());
    let client = client_builder.build(https);
    let connector = SharedHttpConnector::new(TargetHyperHttpConnector { client });

    http_client_fn(move |_settings, _components| connector.clone())
}

fn validate_ca_pem_bundle(ca_cert_pem: &[u8]) -> Result<(), String> {
    let certs = rustls_pki_types::CertificateDer::pem_slice_iter(ca_cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("invalid PEM encoding: {err}"))?;

    if certs.is_empty() {
        return Err("no certificates found".to_string());
    }

    // Smithy's rustls adapter defers parsing custom certificates and assumes
    // they are valid when the HTTPS connector is built. Validate every DER
    // certificate first so malformed configuration is reported rather than
    // reaching an `expect` in the dependency.
    let mut validation_store = rustls::RootCertStore::empty();
    for cert in certs {
        validation_store
            .add(cert)
            .map_err(|err| format!("invalid X.509 certificate: {err}"))?;
    }

    Ok(())
}

pub(crate) fn validate_target_ca_pem(ca_cert_pem: &str) -> Result<(), RemoteS3ClientError> {
    validate_ca_pem_bundle(ca_cert_pem.as_bytes()).map_err(RemoteS3ClientError::InvalidCaPem)
}

pub(crate) fn compose_replication_trust_store(
    certificate_bundles: impl IntoIterator<Item = Vec<u8>>,
) -> (smithy_tls::TrustStore, usize) {
    // `TrustStore::default()` keeps the platform-native roots enabled. Target
    // and RUSTFS_TLS_PATH certificates extend that baseline instead of
    // replacing it with a target-specific trust island.
    let mut trust_store = smithy_tls::TrustStore::default();
    let mut custom_bundle_count = 0;
    for pem in certificate_bundles {
        trust_store.add_pem_certificate(pem);
        custom_bundle_count += 1;
    }

    (trust_store, custom_bundle_count)
}

pub(crate) fn build_aws_s3_http_client_with_trust_store(
    trust_store: smithy_tls::TrustStore,
) -> Result<SharedHttpClient, RemoteS3ClientError> {
    let tls_context = smithy_tls::TlsContext::builder()
        .with_trust_store(trust_store)
        .build()
        .map_err(|err| RemoteS3ClientError::InvalidCaPem(err.to_string()))?;

    Ok(SmithyHttpClientBuilder::new()
        .tls_provider(smithy_tls::Provider::rustls(smithy_tls::rustls_provider::CryptoMode::AwsLc))
        .tls_context(tls_context)
        .build_https())
}

pub(crate) async fn load_tls_path_ca_bundles(tls_dir: &Path, trust_leaf_cert_as_ca: bool) -> Vec<Vec<u8>> {
    let mut certificate_bundles = Vec::new();

    let ca_path = tls_dir.join(RUSTFS_CA_CERT);
    match tokio::fs::read(&ca_path).await {
        Ok(pem) => match validate_ca_pem_bundle(&pem) {
            Ok(()) => certificate_bundles.push(pem),
            Err(err) => warn!("ignoring invalid custom CA bundle {:?} for replication client: {}", ca_path, err),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!("failed to read custom CA bundle {:?} for replication client: {}", ca_path, e),
    }

    if trust_leaf_cert_as_ca {
        let leaf_cert_path = tls_dir.join(RUSTFS_TLS_CERT);
        match tokio::fs::read(&leaf_cert_path).await {
            Ok(pem) => match validate_ca_pem_bundle(&pem) {
                Ok(()) => certificate_bundles.push(pem),
                Err(err) => warn!(
                    "ignoring invalid leaf certificate {:?} for replication client trust store: {}",
                    leaf_cert_path, err
                ),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => warn!("failed to read leaf cert {:?} for replication client trust store: {}", leaf_cert_path, e),
        }
    }

    certificate_bundles
}

async fn load_configured_tls_ca_bundles() -> Vec<Vec<u8>> {
    let tls_path = rustfs_utils::get_env_str(rustfs_config::ENV_RUSTFS_TLS_PATH, rustfs_config::DEFAULT_RUSTFS_TLS_PATH);
    if tls_path.is_empty() {
        return Vec::new();
    }

    load_tls_path_ca_bundles(
        Path::new(&tls_path),
        rustfs_utils::get_env_bool(ENV_TRUST_LEAF_CERT_AS_CA, DEFAULT_TRUST_LEAF_CERT_AS_CA),
    )
    .await
}

pub(crate) async fn build_aws_s3_http_client_from_target_ca_pem(
    ca_cert_pem: &str,
) -> Result<SharedHttpClient, RemoteS3ClientError> {
    validate_target_ca_pem(ca_cert_pem)?;

    let mut certificate_bundles = load_configured_tls_ca_bundles().await;
    certificate_bundles.push(ca_cert_pem.as_bytes().to_vec());
    let (trust_store, _) = compose_replication_trust_store(certificate_bundles);

    build_aws_s3_http_client_with_trust_store(trust_store)
}

/// Selects the HTTP client for `spec`: `None` keeps the SDK default (plain
/// HTTP, or HTTPS with platform roots when no custom trust is configured).
pub(crate) async fn build_aws_s3_http_client_for_spec(
    spec: &RemoteS3EndpointSpec,
) -> Result<Option<SharedHttpClient>, RemoteS3ClientError> {
    if !spec.secure {
        return Ok(None);
    }

    if spec.skip_tls_verify {
        return Ok(Some(build_insecure_aws_s3_http_client()));
    }

    if let Some(ca_cert_pem) = spec.custom_ca_pem() {
        return build_aws_s3_http_client_from_target_ca_pem(ca_cert_pem).await.map(Some);
    }

    Ok(build_aws_s3_http_client_from_tls_path().await)
}

async fn build_aws_s3_http_client_from_tls_path() -> Option<SharedHttpClient> {
    let certificate_bundles = load_configured_tls_ca_bundles().await;
    if certificate_bundles.is_empty() {
        return None;
    }

    let (trust_store, _) = compose_replication_trust_store(certificate_bundles);
    match build_aws_s3_http_client_with_trust_store(trust_store) {
        Ok(client) => Some(client),
        Err(e) => {
            warn!("failed to build AWS SDK TLS context for replication client: {}", e);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_async::time::TimeSource;
    use aws_smithy_runtime_api::http::StatusCode as SmithyStatusCode;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    fn spec(endpoint: &str, secure: bool) -> RemoteS3EndpointSpec {
        RemoteS3EndpointSpec {
            endpoint: endpoint.to_string(),
            secure,
            region: "us-east-1".to_string(),
            path_style: PathStyle::Auto,
            credentials: Some(RemoteCredentials {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
                session_token: None,
                expiration: None,
                account_id: String::new(),
            }),
            skip_tls_verify: false,
            ca_cert_pem: None,
            connect_timeout: None,
            read_timeout: None,
            retry: RemoteS3RetryPolicy::Disabled,
            user_agent_suffix: "",
        }
    }

    type RecordedHeaders = Arc<Mutex<Vec<Vec<(String, String)>>>>;

    #[derive(Clone, Debug)]
    struct RecordingHeaderConnector {
        request_headers: RecordedHeaders,
    }

    impl SmithyHttpConnector for RecordingHeaderConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            self.request_headers
                .lock()
                .expect("recorded header lock should not be poisoned")
                .push(
                    request
                        .headers()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                );
            HttpConnectorFuture::ready(Ok(HttpResponse::new(
                SmithyStatusCode::try_from(200_u16).expect("200 should be a valid response status"),
                SdkBody::empty(),
            )))
        }
    }

    #[tokio::test]
    async fn build_remote_s3_client_rejects_loopback_and_metadata_endpoints() {
        // Default (no loopback opt-in): loopback in IPv4, IPv6 and hostname
        // forms plus the metadata endpoint all return the typed gate error.
        for endpoint in ["127.0.0.1:9000", "[::1]:9000", "localhost:9000", "169.254.169.254"] {
            let err = build_remote_s3_client(&spec(endpoint, false))
                .await
                .err()
                .unwrap_or_else(|| panic!("{endpoint} must be rejected by the outbound gate"));
            assert!(
                matches!(err, RemoteS3ClientError::EndpointNotAllowed(OutboundUrlError::ForbiddenHost { .. })),
                "{endpoint}: unexpected error {err:?}"
            );
            assert!(err.to_string().contains("not allowed"), "{endpoint}: {err}");
        }
    }

    #[tokio::test]
    async fn build_remote_s3_client_allows_private_and_public_endpoints() {
        for endpoint in ["10.0.0.1:9000", "192.168.1.20", "s3.example.com"] {
            build_remote_s3_client(&spec(endpoint, false))
                .await
                .unwrap_or_else(|err| panic!("{endpoint} should be allowed: {err}"));
        }
    }

    #[tokio::test]
    async fn build_remote_s3_client_requires_credentials() {
        let mut spec = spec("s3.example.com", true);
        spec.credentials = None;
        let err = build_remote_s3_client(&spec)
            .await
            .expect_err("missing credentials must be a typed error");
        assert!(matches!(err, RemoteS3ClientError::MissingCredentials));
    }

    #[tokio::test]
    async fn build_remote_s3_client_rejects_expiration_without_session_token() {
        let mut spec = spec("s3.example.com", true);
        spec.credentials
            .as_mut()
            .expect("spec fixture carries credentials")
            .expiration = Some(SystemTime::now() + Duration::from_secs(3_600));
        let err = build_remote_s3_client(&spec)
            .await
            .expect_err("expiration without session token must be rejected");
        assert_eq!(err.to_string(), "remote target credential expiration requires a session token");
    }

    #[tokio::test]
    async fn build_remote_s3_client_rejects_invalid_custom_ca_pem() {
        let mut spec = spec("192.168.1.10:9000", true);
        spec.ca_cert_pem = Some("not a pem".to_string());
        let err = build_remote_s3_client(&spec)
            .await
            .expect_err("invalid custom CA PEM must be rejected");
        assert!(matches!(err, RemoteS3ClientError::InvalidCaPem(_)));
        assert!(err.to_string().contains("invalid target CA PEM"));
    }

    /// Answers every request with a retryable 503 and counts the wire
    /// requests one logical call produced.
    #[derive(Clone, Debug)]
    struct CountingUnavailableConnector {
        wire_requests: Arc<AtomicUsize>,
    }

    impl SmithyHttpConnector for CountingUnavailableConnector {
        fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
            self.wire_requests.fetch_add(1, Ordering::SeqCst);
            HttpConnectorFuture::ready(Ok(HttpResponse::new(
                SmithyStatusCode::try_from(503_u16).expect("503 should be a valid response status"),
                SdkBody::empty(),
            )))
        }
    }

    async fn wire_requests_for_one_failed_call(retry: RemoteS3RetryPolicy) -> usize {
        let wire_requests = Arc::new(AtomicUsize::new(0));
        let connector = SharedHttpConnector::new(CountingUnavailableConnector {
            wire_requests: Arc::clone(&wire_requests),
        });
        let http_client = http_client_fn(move |_settings, _components| connector.clone());

        let mut spec = spec("s3.example.com", true);
        spec.retry = retry;
        let config = build_remote_s3_config(&spec)
            .await
            .expect("spec should build")
            .http_client(http_client)
            .build();
        S3Client::from_conf(config)
            .head_bucket()
            .bucket("bucket")
            .send()
            .await
            .expect_err("a 503 must fail the call");

        wire_requests.load(Ordering::SeqCst)
    }

    #[tokio::test(start_paused = true)]
    async fn retry_policy_decides_how_many_wire_requests_one_call_costs() {
        assert_eq!(
            wire_requests_for_one_failed_call(RemoteS3RetryPolicy::Disabled).await,
            1,
            "a disabled policy must not amplify one logical call"
        );
        assert_eq!(
            wire_requests_for_one_failed_call(REPLICATION_TARGET_RETRY_POLICY).await,
            3,
            "replication targets keep the three-attempt SDK default"
        );
        assert_eq!(
            wire_requests_for_one_failed_call(RemoteS3RetryPolicy::Standard { max_attempts: 0 }).await,
            1,
            "a zero attempt budget is clamped to the initial request"
        );
    }

    #[derive(Clone, Debug)]
    struct ClockSkewTimeSource(Arc<AtomicU64>);

    impl TimeSource for ClockSkewTimeSource {
        fn now(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_secs(self.0.load(Ordering::SeqCst))
        }
    }

    #[derive(Clone, Debug)]
    struct ClockSkewConnector {
        request_headers: RecordedHeaders,
        error_code: &'static str,
        skew_seconds: i64,
        clock: ClockSkewTimeSource,
    }

    fn recorded_header<'a>(headers: &'a [(String, String)], name: &str) -> &'a str {
        headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
            .unwrap_or_else(|| panic!("signed request must contain {name}"))
    }

    fn signing_time(headers: &[(String, String)]) -> chrono::NaiveDateTime {
        chrono::NaiveDateTime::parse_from_str(recorded_header(headers, "x-amz-date"), "%Y%m%dT%H%M%SZ")
            .expect("SDK signing timestamp must use the SigV4 format")
    }

    impl SmithyHttpConnector for ClockSkewConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let mut headers = self.request_headers.lock().expect("clock skew request capture lock");
            assert!(headers.len() < 3, "clock skew fixture must not exceed two GET attempts and one HEAD");
            headers.push(
                request
                    .headers()
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            );
            let server_time = chrono::DateTime::<chrono::Utc>::from(self.clock.now()).naive_utc()
                + chrono::Duration::seconds(self.skew_seconds);
            let (status, body) = if headers.len() == 1 {
                (
                    403,
                    format!("<Error><Code>{}</Code><Message>Clock skew fixture</Message></Error>", self.error_code),
                )
            } else {
                (200, String::new())
            };
            let response = http::Response::builder()
                .status(status)
                .header("date", server_time.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
                .header("content-type", "application/xml")
                .header("content-length", body.len())
                .body(SdkBody::from(body))
                .expect("clock skew fixture response");
            HttpConnectorFuture::ready(Ok(HttpResponse::try_from(response).expect("Smithy fixture response")))
        }
    }

    async fn clock_skew_client(
        error_code: &'static str,
        skew_seconds: i64,
        retry: RemoteS3RetryPolicy,
    ) -> (S3Client, RecordedHeaders, ClockSkewTimeSource) {
        let headers: RecordedHeaders = Arc::new(Mutex::new(Vec::new()));
        let clock = ClockSkewTimeSource(Arc::new(AtomicU64::new(1_700_000_000)));
        let connector = SharedHttpConnector::new(ClockSkewConnector {
            request_headers: Arc::clone(&headers),
            error_code,
            skew_seconds,
            clock: clock.clone(),
        });
        let mut spec = spec("s3.example.com", true);
        spec.retry = retry;
        let config = build_remote_s3_config(&spec)
            .await
            .expect("clock skew fixture uses the production outbound configuration")
            .http_client(http_client_fn(move |_settings, _components| connector.clone()))
            .time_source(clock.clone())
            .build();
        (S3Client::from_conf(config), headers, clock)
    }

    #[tokio::test(start_paused = true)]
    async fn remote_s3_clock_skew_retries_resign_and_seed_next_operation() {
        for error_code in ["RequestTimeTooSkewed", "SignatureDoesNotMatch"] {
            for skew_seconds in [-600, 600] {
                let (client, headers, clock) = clock_skew_client(error_code, skew_seconds, REPLICATION_TARGET_RETRY_POLICY).await;
                let initial = chrono::DateTime::<chrono::Utc>::from(clock.now()).naive_utc();
                client
                    .get_object()
                    .bucket("bucket")
                    .key("object")
                    .send()
                    .await
                    .expect("clock skew GET must retry successfully");
                assert_eq!(
                    headers.lock().expect("captured requests").len(),
                    2,
                    "{error_code}: GET needs exactly one retry"
                );
                clock.0.fetch_add(17, Ordering::SeqCst);
                // SDK signing time is independent of Tokio's retry/scheduler clock.
                tokio::time::advance(Duration::from_secs(61)).await;
                client
                    .head_bucket()
                    .bucket("bucket")
                    .send()
                    .await
                    .expect("subsequent HEAD must use the client's cached skew");
                let headers = headers.lock().expect("captured signed requests");
                assert_eq!(headers.len(), 3, "subsequent operation must succeed on its first attempt");
                assert_eq!(signing_time(&headers[0]), initial, "the first attempt must use the injected clock");
                assert_eq!(
                    signing_time(&headers[1]),
                    initial + chrono::Duration::seconds(skew_seconds),
                    "{error_code}: retry must apply the measured offset exactly"
                );
                assert_eq!(
                    signing_time(&headers[2]),
                    initial + chrono::Duration::seconds(skew_seconds + 17),
                    "{error_code}: the next operation must apply cached skew to the advanced signing clock"
                );
                let signature = |index: usize| {
                    recorded_header(&headers[index], "authorization")
                        .rsplit_once("Signature=")
                        .expect("SigV4 authorization contains a signature")
                        .1
                };
                assert_ne!(
                    signature(0),
                    signature(1),
                    "{error_code}: retry must be signed again after adjusting its date"
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn remote_s3_clock_skew_respects_one_attempt_policy() {
        use aws_smithy_types::error::metadata::ProvideErrorMetadata;

        for error_code in ["RequestTimeTooSkewed", "SignatureDoesNotMatch"] {
            for retry in [
                RemoteS3RetryPolicy::Disabled,
                RemoteS3RetryPolicy::Standard { max_attempts: 1 },
            ] {
                let (client, headers, _clock) = clock_skew_client(error_code, 600, retry).await;
                let error = client
                    .get_object()
                    .bucket("bucket")
                    .key("object")
                    .send()
                    .await
                    .expect_err("clock skew must not override the caller's one-attempt budget");
                assert_eq!(error.as_service_error().and_then(ProvideErrorMetadata::code), Some(error_code));
                assert_eq!(
                    headers.lock().expect("captured requests").len(),
                    1,
                    "{error_code}: {retry:?} must send exactly one request"
                );
            }
        }
    }

    #[test]
    fn path_style_auto_and_path_force_path_style() {
        assert!(PathStyle::Auto.force_path_style());
        assert!(PathStyle::Path.force_path_style());
        assert!(!PathStyle::VirtualHost.force_path_style());
    }

    #[test]
    fn remote_credentials_debug_redacts_secrets() {
        let credentials = RemoteCredentials {
            access_key: "access".to_string(),
            secret_key: "very-secret".to_string(),
            session_token: Some("session-token".to_string()),
            expiration: None,
            account_id: String::new(),
        };
        let rendered = format!("{credentials:?}");
        assert!(rendered.contains("access"));
        assert!(!rendered.contains("very-secret"));
        assert!(!rendered.contains("session-token"));
    }

    #[tokio::test]
    async fn user_agent_suffix_is_appended_after_signing() {
        let request_headers: RecordedHeaders = Arc::new(Mutex::new(Vec::new()));
        let connector = SharedHttpConnector::new(RecordingHeaderConnector {
            request_headers: Arc::clone(&request_headers),
        });
        let http_client = http_client_fn(move |_settings, _components| connector.clone());

        let mut spec = spec("s3.example.com", true);
        spec.user_agent_suffix = "RustFS-Test/0.0";
        spec.connect_timeout = Some(Duration::from_secs(5));
        spec.read_timeout = Some(Duration::from_secs(5));
        let config = build_remote_s3_config(&spec)
            .await
            .expect("spec should build")
            .http_client(http_client)
            .build();
        S3Client::from_conf(config)
            .head_bucket()
            .bucket("bucket")
            .send()
            .await
            .expect("recording connector should accept the request");

        let recorded = request_headers.lock().expect("recorded header lock should not be poisoned");
        let headers = &recorded[0];
        let user_agent = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("user-agent"))
            .map(|(_, v)| v.as_str())
            .expect("SDK request must carry a user-agent");
        assert!(user_agent.ends_with(" RustFS-Test/0.0"), "user-agent was {user_agent}");
        assert!(user_agent.starts_with("aws-sdk-rust/"), "SDK identity must be preserved: {user_agent}");
        assert!(
            headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("authorization")),
            "request must still be signed"
        );
    }
}
