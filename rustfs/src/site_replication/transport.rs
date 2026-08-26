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

use super::*;

pub(crate) const SITE_REPLICATION_PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) const SITE_REPLICATION_PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) const SITE_REPLICATION_PEER_ERROR_DETAIL_LIMIT: usize = 256;

pub(crate) const MAX_PEER_CA_CERT_PEM_SIZE: usize = 256 * 1024;

pub(crate) const ALLOW_LOOPBACK_REPLICATION_TARGET_ENV: &str = "RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET";

pub(crate) const SITE_REPLICATION_PEER_DERIVED_RULE_CONTRACT_CAPABILITY_PATH: &str =
    "/rustfs/admin/v3/site-replication/peer/edit-capabilities?capability=derived-rule-contract";

pub(crate) const RUSTFS_ADMIN_V3_PREFIX: &str = "/rustfs/admin/v3";

pub(crate) const MINIO_ADMIN_V3_PREFIX: &str = "/minio/admin/v3";

pub(crate) const MINIO_SITE_REPLICATION_PEER_JOIN_PATH: &str = "/minio/admin/v3/site-replication/peer/join";

#[derive(Clone)]
pub(crate) enum SiteReplicationPeerClientCacheEntry {
    Ready(reqwest::Client),
    Failed(String),
}

#[derive(Clone)]
pub(crate) struct SiteReplicationPeerClientCache {
    pub(crate) generation: u64,
    pub(crate) entry: SiteReplicationPeerClientCacheEntry,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PeerConnection {
    pub(crate) endpoint: Url,
    pub(crate) skip_tls_verify: bool,
    pub(crate) ca_cert_pem: String,
}

#[derive(Deserialize, Default)]
pub(crate) struct PeerTlsFieldPresence {
    #[serde(rename = "skipTlsVerify")]
    pub(crate) skip_tls_verify: Option<IgnoredAny>,
    #[serde(rename = "caCertPem")]
    pub(crate) ca_cert_pem: Option<IgnoredAny>,
}

impl PeerTlsFieldPresence {
    pub(crate) fn has_skip_tls_verify(&self) -> bool {
        self.skip_tls_verify.is_some()
    }

    pub(crate) fn has_ca_cert_pem(&self) -> bool {
        self.ca_cert_pem.is_some()
    }
}

#[derive(Clone)]
pub(crate) struct PeerDnsResolver {
    pub(crate) allow_loopback: bool,
    #[cfg(test)]
    pub(crate) overrides: Option<Arc<HashMap<String, Vec<IpAddr>>>>,
}

impl PeerDnsResolver {
    pub(crate) fn new(allow_loopback: bool) -> Self {
        Self {
            allow_loopback,
            #[cfg(test)]
            overrides: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_overrides(allow_loopback: bool, overrides: HashMap<String, Vec<IpAddr>>) -> Self {
        Self {
            allow_loopback,
            overrides: Some(Arc::new(overrides)),
        }
    }
}

impl reqwest::dns::Resolve for PeerDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        let allow_loopback = self.allow_loopback;
        #[cfg(test)]
        let overrides = self.overrides.clone();
        Box::pin(async move {
            #[cfg(test)]
            let overridden = overrides.as_ref().and_then(|entries| entries.get(&host)).cloned();
            #[cfg(not(test))]
            let overridden: Option<Vec<IpAddr>> = None;

            let ips = if let Some(ips) = overridden {
                ips
            } else {
                tokio::net::lookup_host((host.as_str(), 0))
                    .await?
                    .map(|addr| addr.ip())
                    .collect()
            };
            let addrs = ips
                .into_iter()
                .filter(|ip| resolved_peer_ip_allowed(&host, *ip, allow_loopback))
                .map(|ip| SocketAddr::new(ip, 0))
                .collect::<Vec<_>>();
            if addrs.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("site replication DNS resolution for `{host}` returned no allowed addresses"),
                )
                .into());
            }
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

impl PeerConnection {
    pub(crate) fn new(endpoint: &str, skip_tls_verify: bool, ca_cert_pem: &str) -> S3Result<Self> {
        validate_peer_connection_inner(endpoint, skip_tls_verify, ca_cert_pem, loopback_replication_targets_allowed())
    }

    pub(crate) fn endpoint(&self) -> &str {
        self.endpoint.as_str().trim_end_matches('/')
    }

    pub(crate) fn uses_default_tls(&self) -> bool {
        !self.skip_tls_verify && self.ca_cert_pem.is_empty()
    }
}

impl TryFrom<&PeerInfo> for PeerConnection {
    type Error = S3Error;

    fn try_from(peer: &PeerInfo) -> Result<Self, Self::Error> {
        Self::new(&peer.endpoint, peer.skip_tls_verify, &peer.ca_cert_pem)
    }
}

impl TryFrom<&PeerSite> for PeerConnection {
    type Error = S3Error;

    fn try_from(site: &PeerSite) -> Result<Self, Self::Error> {
        Self::new(&site.endpoint, site.skip_tls_verify, &site.ca_cert_pem)
    }
}

static SITE_REPLICATION_PEER_CLIENT: LazyLock<Mutex<Option<SiteReplicationPeerClientCache>>> = LazyLock::new(|| Mutex::new(None));

pub(crate) fn site_replication_peer_client_cache_hit(
    cache: &Option<SiteReplicationPeerClientCache>,
    generation: u64,
) -> Option<S3Result<reqwest::Client>> {
    let cached = cache.as_ref()?;
    if cached.generation != generation {
        return None;
    }
    Some(match &cached.entry {
        SiteReplicationPeerClientCacheEntry::Ready(client) => Ok(client.clone()),
        SiteReplicationPeerClientCacheEntry::Failed(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("initialize site replication peer client failed: {err}"),
        )),
    })
}

pub(crate) struct SiteReplicationRuntime {
    pub(crate) state: SiteReplicationState,
    pub(crate) local_peer: PeerInfo,
    pub(crate) service_account_secret_key: String,
}

pub(crate) fn build_site_replication_peer_client(outbound_tls: &GlobalPublishedOutboundTlsState) -> S3Result<reqwest::Client> {
    build_site_replication_peer_client_with_resolver(outbound_tls, PeerDnsResolver::new(loopback_replication_targets_allowed()))
}

pub(crate) fn build_site_replication_peer_client_with_resolver(
    outbound_tls: &GlobalPublishedOutboundTlsState,
    resolver: PeerDnsResolver,
) -> S3Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(SITE_REPLICATION_PEER_REQUEST_TIMEOUT)
        .connect_timeout(SITE_REPLICATION_PEER_CONNECT_TIMEOUT)
        .pool_idle_timeout(Some(Duration::from_secs(60)))
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(resolver);

    if let Some(root_ca_pem) = outbound_tls.root_ca_pem.as_ref() {
        let mut reader = std::io::BufReader::new(root_ca_pem.as_slice());
        let certs_der = rustls_pki_types::CertificateDer::pem_reader_iter(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("failed to parse published site-replication CA certs: {e}"),
                )
            })?;

        for cert_der in certs_der {
            let cert = reqwest::Certificate::from_der(cert_der.as_ref()).map_err(|e| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("failed to load published site-replication CA cert: {e}"),
                )
            })?;
            builder = builder.add_root_certificate(cert);
        }
    }

    builder
        .build()
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("build site replication peer client failed: {e}")))
}

pub(crate) fn build_custom_site_replication_peer_client(
    outbound_tls: &GlobalPublishedOutboundTlsState,
    connection: &PeerConnection,
) -> S3Result<reqwest::Client> {
    build_custom_site_replication_peer_client_with_resolver(
        outbound_tls,
        connection,
        PeerDnsResolver::new(loopback_replication_targets_allowed()),
    )
}

pub(crate) fn build_custom_site_replication_peer_client_with_resolver(
    outbound_tls: &GlobalPublishedOutboundTlsState,
    connection: &PeerConnection,
    resolver: PeerDnsResolver,
) -> S3Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(SITE_REPLICATION_PEER_REQUEST_TIMEOUT)
        .connect_timeout(SITE_REPLICATION_PEER_CONNECT_TIMEOUT)
        .pool_idle_timeout(Some(Duration::from_secs(60)))
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(resolver)
        .danger_accept_invalid_certs(connection.skip_tls_verify);

    if let Some(root_ca_pem) = outbound_tls.root_ca_pem.as_ref() {
        let mut reader = std::io::BufReader::new(root_ca_pem.as_slice());
        let certs_der = rustls_pki_types::CertificateDer::pem_reader_iter(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("failed to parse published site-replication CA certs: {e}"),
                )
            })?;
        for cert_der in certs_der {
            let cert = reqwest::Certificate::from_der(cert_der.as_ref()).map_err(|e| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("failed to load published site-replication CA cert: {e}"),
                )
            })?;
            builder = builder.add_root_certificate(cert);
        }
    }
    if !connection.ca_cert_pem.is_empty() {
        for cert in parse_peer_ca_certificates(&connection.ca_cert_pem)? {
            builder = builder.add_root_certificate(cert);
        }
    }

    builder
        .build()
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("build site replication peer client failed: {e}")))
}

pub(crate) async fn site_replication_peer_client() -> S3Result<reqwest::Client> {
    let generation = current_outbound_tls_generation().0;
    let cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
    if let Some(hit) = site_replication_peer_client_cache_hit(&cache, generation) {
        return hit;
    }
    drop(cache);

    let outbound_tls = current_outbound_tls_state().await;
    let built = build_site_replication_peer_client(&outbound_tls);
    let cache_entry = match &built {
        Ok(client) => SiteReplicationPeerClientCacheEntry::Ready(client.clone()),
        Err(err) => SiteReplicationPeerClientCacheEntry::Failed(err.to_string()),
    };

    let mut cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
    if cache.as_ref().is_none_or(|cached| cached.generation <= generation) {
        *cache = Some(SiteReplicationPeerClientCache {
            generation,
            entry: cache_entry,
        });
    }

    built
}

pub(crate) async fn site_replication_client_for(connection: &PeerConnection) -> S3Result<reqwest::Client> {
    // Revalidate at the client boundary so callers cannot bypass endpoint/TLS policy.
    let connection = PeerConnection::new(connection.endpoint(), connection.skip_tls_verify, &connection.ca_cert_pem)?;
    if connection.uses_default_tls() {
        return site_replication_peer_client().await;
    }
    let outbound_tls = current_outbound_tls_state().await;
    build_custom_site_replication_peer_client(&outbound_tls, &connection)
}

pub(crate) fn runtime_peer_connection(peer: &PeerInfo) -> S3Result<PeerConnection> {
    PeerConnection::try_from(peer).map_err(|err| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("invalid persisted site replication peer `{}`: {err}", peer.endpoint),
        )
    })
}

pub(crate) struct PeerTransport {
    pub(crate) connection: PeerConnection,
    pub(crate) client: reqwest::Client,
}

impl PeerTransport {
    pub(crate) async fn for_runtime_peer(peer: &PeerInfo) -> S3Result<Self> {
        let connection = runtime_peer_connection(peer)?;
        let client = site_replication_client_for(&connection).await.map_err(|err| {
            S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("initialize persisted site replication peer `{}` transport failed: {err}", peer.endpoint),
            )
        })?;
        Ok(Self { connection, client })
    }
}

pub(crate) fn runtime_tls_enabled_with(endpoints: Option<&crate::storage_api::site_replication::EndpointServerPools>) -> bool {
    if !rustfs_utils::get_env_str(ENV_RUSTFS_TLS_PATH, DEFAULT_RUSTFS_TLS_PATH).is_empty() {
        return true;
    }

    if let Some(tls_enabled) = endpoints.and_then(|endpoints| {
        endpoints
            .as_ref()
            .iter()
            .flat_map(|pool| pool.endpoints.as_ref().iter())
            .find(|endpoint| endpoint.is_local)
            .map(|endpoint| endpoint.url.scheme().eq_ignore_ascii_case("https"))
    }) {
        return tls_enabled;
    }

    false
}

pub(crate) fn runtime_tls_enabled() -> bool {
    let endpoints = current_endpoints_handle();
    runtime_tls_enabled_with(endpoints.as_ref())
}

pub(crate) fn hash_client_secret(secret: Option<&str>) -> String {
    let Some(secret) = secret.filter(|secret| !secret.is_empty()) else {
        return String::new();
    };

    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    URL_SAFE_NO_PAD.encode_to_string(hasher.finalize())
}

pub(crate) fn loopback_replication_targets_allowed() -> bool {
    std::env::var(ALLOW_LOOPBACK_REPLICATION_TARGET_ENV)
        .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
        .unwrap_or(false)
}

pub(crate) fn validate_peer_egress(url: &Url, allow_loopback: bool) -> Result<(), OutboundUrlError> {
    match validate_outbound_url(url) {
        Ok(()) => Ok(()),
        Err(OutboundUrlError::ForbiddenHost {
            reason: "private address",
            ..
        }) => Ok(()),
        Err(OutboundUrlError::ForbiddenHost {
            reason: "loopback address" | "loopback host",
            ..
        }) if allow_loopback && peer_url_has_canonical_loopback_host(url) => Ok(()),
        Err(err) => Err(err),
    }
}

pub(crate) fn peer_url_has_canonical_loopback_host(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip == std::net::Ipv4Addr::LOCALHOST,
        Some(url::Host::Ipv6(ip)) => ip == std::net::Ipv6Addr::LOCALHOST,
        None => false,
    }
}

pub(crate) fn resolved_peer_ip_allowed(host: &str, ip: IpAddr, allow_loopback: bool) -> bool {
    let Ok(ip_url) = (match ip {
        IpAddr::V4(ip) => Url::parse(&format!("http://{ip}")),
        IpAddr::V6(ip) => Url::parse(&format!("http://[{ip}]")),
    }) else {
        return false;
    };
    match validate_outbound_url(&ip_url) {
        Ok(()) => true,
        Err(OutboundUrlError::ForbiddenHost {
            reason: "private address",
            ..
        }) => true,
        Err(OutboundUrlError::ForbiddenHost {
            reason: "loopback address",
            ..
        }) => {
            allow_loopback
                && host.eq_ignore_ascii_case("localhost")
                && matches!(ip, IpAddr::V4(std::net::Ipv4Addr::LOCALHOST) | IpAddr::V6(std::net::Ipv6Addr::LOCALHOST))
        }
        Err(_) => false,
    }
}

pub(crate) fn parse_peer_ca_certificates(ca_cert_pem: &str) -> S3Result<Vec<reqwest::Certificate>> {
    if ca_cert_pem.len() > MAX_PEER_CA_CERT_PEM_SIZE {
        return Err(s3_error!(InvalidRequest, "site replication CA certificate exceeds 256 KiB"));
    }
    if ca_cert_pem.contains("PRIVATE KEY-----") {
        return Err(s3_error!(
            InvalidRequest,
            "site replication CA certificate must not contain a private key"
        ));
    }

    let mut reader = std::io::BufReader::new(ca_cert_pem.as_bytes());
    let certs_der = rustls_pki_types::CertificateDer::pem_reader_iter(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site replication CA certificate: {e}"))
        })?;
    if certs_der.is_empty() {
        return Err(s3_error!(
            InvalidRequest,
            "site replication CA certificate must contain at least one certificate"
        ));
    }

    let mut root_store = rustls::RootCertStore::empty();
    certs_der
        .into_iter()
        .map(|cert| {
            root_store.add(cert.clone()).map_err(|e| {
                S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site replication CA certificate: {e}"))
            })?;
            reqwest::Certificate::from_der(cert.as_ref()).map_err(|e| {
                S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site replication CA certificate: {e}"))
            })
        })
        .collect()
}

pub(crate) fn validate_peer_connection_inner(
    endpoint: &str,
    skip_tls_verify: bool,
    ca_cert_pem: &str,
    allow_loopback: bool,
) -> S3Result<PeerConnection> {
    let parsed = Url::parse(endpoint)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site endpoint `{endpoint}`: {e}")))?;
    match parsed.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                format!("invalid site endpoint `{endpoint}`: unsupported scheme `{scheme}`"),
            ));
        }
    }
    if parsed.host_str().is_none() {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("invalid site endpoint `{endpoint}`: missing host"),
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(s3_error!(InvalidRequest, "invalid site endpoint `{endpoint}`: userinfo is not allowed"));
    }
    if parsed.path() != "/" || parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(s3_error!(
            InvalidRequest,
            "invalid site endpoint `{endpoint}`: endpoint must be an origin"
        ));
    }
    validate_peer_egress(&parsed, allow_loopback)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site endpoint `{endpoint}`: {e}")))?;

    if ca_cert_pem.len() > MAX_PEER_CA_CERT_PEM_SIZE {
        return Err(s3_error!(InvalidRequest, "site replication CA certificate exceeds 256 KiB"));
    }
    let ca_cert_pem = ca_cert_pem.trim();
    if parsed.scheme() != "https" && (skip_tls_verify || !ca_cert_pem.is_empty()) {
        return Err(s3_error!(InvalidRequest, "site replication TLS settings require an HTTPS endpoint"));
    }
    if skip_tls_verify && !ca_cert_pem.is_empty() {
        return Err(s3_error!(InvalidRequest, "skipTLSVerify and caCertPem are mutually exclusive"));
    }
    if !ca_cert_pem.is_empty() {
        parse_peer_ca_certificates(ca_cert_pem)?;
    }

    Ok(PeerConnection {
        endpoint: parsed,
        skip_tls_verify,
        ca_cert_pem: ca_cert_pem.to_string(),
    })
}

pub(crate) fn site_replication_peer_wire_path(path: &str) -> String {
    let (path_only, query) = path
        .split_once('?')
        .map(|(path, query)| (path, Some(query)))
        .unwrap_or((path, None));
    let wire_path = if let Some(suffix) = path_only.strip_prefix(RUSTFS_ADMIN_V3_PREFIX) {
        format!("{MINIO_ADMIN_V3_PREFIX}{suffix}")
    } else {
        path_only.to_string()
    };

    match query {
        Some(query) => format!("{wire_path}?{query}"),
        None => wire_path,
    }
}

pub(crate) fn site_replication_peer_payload_encrypted(wire_path: &str) -> bool {
    // MinIO's SRPeerJoin handler force-decrypts the request body, so the
    // peer/join payload must always travel encrypted.
    wire_path.split_once('?').map(|(path, _)| path).unwrap_or(wire_path) == MINIO_SITE_REPLICATION_PEER_JOIN_PATH
}

pub(crate) fn site_replication_peer_payload(path: &str, secret_key: &str, payload: Vec<u8>) -> S3Result<(Vec<u8>, &'static str)> {
    if site_replication_peer_payload_encrypted(path) {
        // The encrypted branch fires only for the `/minio/admin/...` peer-join
        // wire path, where `crate::admin::utils::encode_compatible_admin_payload`
        // unconditionally takes its compat-encryption arm — inlined here so
        // this module does not import the interface layer.
        let encrypted = rustfs_crypto::encrypt_stream_io(secret_key.as_bytes(), &payload)
            .map_err(|e| s3_error!(InternalError, "failed to encrypt MinIO admin payload: {}", e))?;
        Ok((encrypted, "application/octet-stream"))
    } else {
        Ok((payload, "application/json"))
    }
}

pub(crate) fn site_replication_peer_url(connection: &PeerConnection, wire_path: &str) -> S3Result<Url> {
    let path = wire_path.split_once('?').map_or(wire_path, |(path, _)| path);
    if !path.starts_with('/') || path.starts_with("//") {
        return Err(s3_error!(InvalidRequest, "invalid site replication peer path"));
    }
    connection
        .endpoint
        .join(wire_path)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site replication peer path: {e}")))
}

/// One peer admin request, collapsing the formerly-duplicated PUT/GET
/// dispatch bodies (backlog#1840 PR2): wire-path mapping, URL/authority
/// derivation, payload wire-encoding, SigV4 signing, transport-error
/// classification, and the response read live here once. The option axes are
/// the method, an optional pre-resolved client, the service-account secret
/// candidates, and the retry-event bookkeeping.
pub(crate) struct PeerAdminRequest<'a> {
    connection: &'a PeerConnection,
    path: &'a str,
    access_key: &'a str,
    method: Method,
    client: Option<&'a reqwest::Client>,
}

impl<'a> PeerAdminRequest<'a> {
    pub(crate) fn put(connection: &'a PeerConnection, path: &'a str, access_key: &'a str) -> Self {
        Self {
            connection,
            path,
            access_key,
            method: Method::PUT,
            client: None,
        }
    }

    pub(crate) fn get(connection: &'a PeerConnection, path: &'a str, access_key: &'a str) -> Self {
        Self {
            method: Method::GET,
            ..Self::put(connection, path, access_key)
        }
    }

    pub(crate) fn with_client(mut self, client: &'a reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    async fn resolved_client(&self) -> S3Result<reqwest::Client> {
        match self.client {
            Some(client) => Ok(client.clone()),
            None => site_replication_client_for(self.connection).await,
        }
    }

    /// Send and return the raw status/body without a success check. `body` is
    /// the JSON payload of a `PUT` (serialized and wire-encoded here, which
    /// is where the peer-join encryption applies); `None` sends a bodiless
    /// request (the `GET` flavor).
    pub(crate) async fn send_raw<T: Serialize>(&self, secret_key: &str, body: Option<&T>) -> S3Result<(StatusCode, Vec<u8>)> {
        let client = self.resolved_client().await?;
        let path = site_replication_peer_wire_path(self.path);
        let url = site_replication_peer_url(self.connection, &path)?;
        let uri = url
            .as_str()
            .parse::<Uri>()
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid peer endpoint: {e}")))?;
        let authority = uri
            .authority()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "peer endpoint missing authority".to_string()))?
            .to_string();
        let payload = body
            .map(|body| {
                let payload = serde_json::to_vec(body).map_err(|e| {
                    S3Error::with_message(S3ErrorCode::InternalError, format!("serialize peer request failed: {e}"))
                })?;
                site_replication_peer_payload(&path, secret_key, payload)
            })
            .transpose()?;

        let mut request = http::Request::builder()
            .method(self.method.clone())
            .uri(uri)
            .header(HOST, authority)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
        if let Some((_, content_type)) = &payload {
            request = request.header(CONTENT_TYPE, *content_type);
        }
        let signed = sign_v4(
            request
                .body(Body::empty())
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("build peer request failed: {e}")))?,
            payload.as_ref().map(|(payload, _)| payload.len() as i64).unwrap_or(0),
            self.access_key,
            secret_key,
            "",
            current_region()
                .map(|region| region.to_string())
                .as_deref()
                .unwrap_or("us-east-1"),
        );

        let mut req = client.request(self.method.clone(), url.clone());
        for (name, value) in signed.headers() {
            req = req.header(name, value);
        }
        if let Some((payload, _)) = payload {
            req = req.body(payload);
        }

        let response = req.send().await.map_err(|e| {
            let classify = if e.is_timeout() {
                "timeout"
            } else if e.is_connect() && e.to_string().to_ascii_lowercase().contains("dns") {
                "dns resolution"
            } else if e.to_string().to_ascii_lowercase().contains("certificate")
                || e.to_string().to_ascii_lowercase().contains("tls")
            {
                "tls handshake"
            } else if e.is_connect() {
                "connect"
            } else {
                "request"
            };
            S3Error::with_message(S3ErrorCode::InternalError, format!("peer request to {url} failed ({classify}): {e}"))
        })?;

        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("read peer response failed: {e}")))?;

        Ok((status, body.to_vec()))
    }

    /// `PUT` with a success check; a non-success status becomes an error
    /// naming the peer endpoint and the caller's path.
    pub(crate) async fn send<T: Serialize>(&self, secret_key: &str, body: &T) -> S3Result<Vec<u8>> {
        let (status, body) = self.send_raw(secret_key, Some(body)).await?;
        if status.is_success() {
            return Ok(body);
        }

        let detail = String::from_utf8_lossy(&body).into_owned();
        Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!(
                "peer request to {}{} failed with {status}: {detail}",
                self.connection.endpoint(),
                self.path
            ),
        ))
    }

    /// Bodiless `GET` with a success check; a non-success status becomes an
    /// error naming the resolved wire URL.
    pub(crate) async fn send_get(&self, secret_key: &str) -> S3Result<Vec<u8>> {
        let (status, body) = self.send_raw::<()>(secret_key, None).await?;
        if !status.is_success() {
            let url = site_replication_peer_url(self.connection, &site_replication_peer_wire_path(self.path))?;
            let detail = String::from_utf8_lossy(&body).into_owned();
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("peer request to {url} failed with {status}: {detail}"),
            ));
        }

        Ok(body)
    }

    /// Try each distinct non-empty service-account secret until one is
    /// accepted; stop early on an error that cannot be a secret mismatch.
    pub(crate) async fn send_with_secret_candidates<T: Serialize>(
        &self,
        secret_candidates: &[String],
        body: &T,
    ) -> S3Result<Vec<u8>> {
        let client = self.resolved_client().await?;
        let request = PeerAdminRequest {
            connection: self.connection,
            path: self.path,
            access_key: self.access_key,
            method: self.method.clone(),
            client: Some(&client),
        };
        let mut tried = HashSet::new();
        let mut errors = Vec::new();

        for secret_key in secret_candidates.iter().filter(|secret_key| !secret_key.is_empty()) {
            if !tried.insert(secret_key.as_str()) {
                continue;
            }

            match request.send(secret_key, body).await {
                Ok(body) => return Ok(body),
                Err(err) => {
                    let detail = format!("{err}");
                    let may_retry_with_next_secret = peer_error_may_be_secret_mismatch(&detail);
                    errors.push(summarize_peer_error_detail(&detail));
                    if !may_retry_with_next_secret {
                        break;
                    }
                }
            }
        }

        Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!(
                "peer request to {}{} failed with all service-account secrets: {}",
                self.connection.endpoint(),
                self.path,
                errors.join("; ")
            ),
        ))
    }

    /// [`Self::send`] plus the retry-queue bookkeeping: a success settles the
    /// peer/path's queued event, a failure enqueues one.
    pub(crate) async fn send_with_retry_event<T: Serialize>(
        &self,
        peer: &PeerInfo,
        secret_key: &str,
        body: &T,
    ) -> S3Result<Vec<u8>> {
        match self.send(secret_key, body).await {
            Ok(body) => {
                dequeue_site_replication_retry_event(peer, self.path).await;
                Ok(body)
            }
            Err(err) => {
                enqueue_site_replication_retry_event(peer, self.path, &err).await;
                Err(err)
            }
        }
    }
}

pub(crate) fn peer_error_may_be_secret_mismatch(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    detail.contains("signaturedoesnotmatch")
        || detail.contains("accessdenied")
        || detail.contains("forbidden")
        || detail.contains("401")
        || detail.contains("403")
}

pub(crate) async fn runtime_site_replication_targets() -> S3Result<Option<SiteReplicationRuntime>> {
    let state = load_site_replication_state().await?;
    if !state.enabled() || state.service_account_access_key.is_empty() {
        return Ok(None);
    }

    let service_account_secret_key = match site_replicator_service_account_secret(&state.service_account_access_key).await {
        Ok(secret) => secret,
        Err(err) => {
            let Some(secret) = legacy_site_replicator_state_secret(&state) else {
                return Err(err);
            };
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                result = "legacy_state_service_account_secret_fallback",
                error = ?err,
                "admin site replication state"
            );
            secret
        }
    };
    let local_peer = current_local_runtime_peer(&state);
    Ok(Some(SiteReplicationRuntime {
        state,
        local_peer,
        service_account_secret_key,
    }))
}

pub(crate) async fn broadcast_site_replication_json<T: Serialize>(path: &str, body: &T) -> S3Result<()> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(());
    };
    broadcast_site_replication_json_with_runtime(&runtime, path, body).await
}

pub(crate) async fn broadcast_site_replication_json_with_runtime<T: Serialize>(
    runtime: &SiteReplicationRuntime,
    path: &str,
    body: &T,
) -> S3Result<()> {
    let state = &runtime.state;
    let local_peer = &runtime.local_peer;

    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }

        let transport = PeerTransport::for_runtime_peer(peer).await?;
        PeerAdminRequest::put(&transport.connection, path, &state.service_account_access_key)
            .with_client(&transport.client)
            .send_with_retry_event(peer, &runtime.service_account_secret_key, body)
            .await?;
    }

    Ok(())
}

pub(crate) fn parse_endpoint_refresh_status(peer: &PeerInfo, body: &[u8]) -> S3Result<()> {
    let status: ReplicateEditStatus = serde_json::from_slice(body).map_err(|_| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("peer {} does not support endpoint target refresh", peer.endpoint),
        )
    })?;
    if status.success {
        Ok(())
    } else {
        Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("peer {} failed endpoint target refresh: {}", peer.endpoint, status.err_detail),
        ))
    }
}

pub(crate) fn peer_capability_response_supported(peer: &PeerInfo, status: StatusCode, body: &[u8]) -> S3Result<bool> {
    if status.is_success() {
        return Ok(parse_endpoint_refresh_status(peer, body).is_ok());
    }
    if matches!(status, StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED) {
        return Ok(false);
    }

    Err(S3Error::with_message(
        S3ErrorCode::InternalError,
        format!("probe site replication capability on peer {} failed with {status}", peer.endpoint),
    ))
}

pub(crate) fn summarize_peer_error_detail(detail: &str) -> String {
    let detail = detail.trim();
    let detail_chars = detail.chars().count();
    if detail_chars <= SITE_REPLICATION_PEER_ERROR_DETAIL_LIMIT {
        return detail.to_string();
    }

    let suffix = "... (truncated)";
    let take_chars = SITE_REPLICATION_PEER_ERROR_DETAIL_LIMIT.saturating_sub(suffix.chars().count());
    let mut summary: String = detail.chars().take(take_chars).collect();
    summary.push_str(suffix);
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_site_replication_peer_client_rebuilds_when_generation_changes() {
        let previous_generation = current_outbound_tls_generation().0;
        let previous_cache = {
            let mut cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
            let snapshot = cache.clone();
            *cache = None;
            snapshot
        };

        set_test_outbound_tls_generation(101);
        site_replication_peer_client()
            .await
            .expect("initial client build should succeed");
        let cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
        let cached = cache.as_ref().expect("cache should be populated");
        assert_eq!(cached.generation, 101);
        assert!(matches!(cached.entry, SiteReplicationPeerClientCacheEntry::Ready(_)));
        drop(cache);

        set_test_outbound_tls_generation(102);
        site_replication_peer_client()
            .await
            .expect("new generation should rebuild client");
        let cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
        let cached = cache.as_ref().expect("cache should be populated");
        assert_eq!(cached.generation, 102);
        assert!(matches!(cached.entry, SiteReplicationPeerClientCacheEntry::Ready(_)));

        drop(cache);
        set_test_outbound_tls_generation(previous_generation);
        let mut cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
        *cache = previous_cache;
    }
}
