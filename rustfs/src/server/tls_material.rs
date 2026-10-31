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

//! TLS material loading, enrichment, and acceptor construction.
//!
//! Single-load architecture: `rustfs_tls_runtime::TlsMaterialSnapshot` loads all
//! TLS materials (server certs + outbound CAs + mTLS identity) from disk in one
//! pass. This module enriches the outbound material with platform-specific CAs
//! (system roots, leaf-as-CA, mTLS env-var path overrides) and builds the server
//! TLS acceptor directly from the pre-loaded server material — no double reads.

use crate::startup_runtime_sources;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use rustfs_common::MtlsIdentityPem;
use rustfs_config::{
    DEFAULT_SERVER_MTLS_ENABLE, DEFAULT_TLS_KEYLOG, DEFAULT_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_INTERVAL,
    DEFAULT_TRUST_LEAF_CERT_AS_CA, DEFAULT_TRUST_SYSTEM_CA, ENV_MTLS_CLIENT_CERT, ENV_MTLS_CLIENT_KEY, ENV_RUSTFS_EXTRA_CA_CERT,
    ENV_SERVER_MTLS_ENABLE, ENV_TLS_KEYLOG, ENV_TLS_RELOAD_ENABLE, ENV_TLS_RELOAD_INTERVAL, ENV_TRUST_LEAF_CERT_AS_CA,
    ENV_TRUST_SYSTEM_CA, RUSTFS_CLIENT_CERT_FILENAME, RUSTFS_CLIENT_KEY_FILENAME,
};
#[cfg(test)]
use rustfs_config::{RUSTFS_CA_CERT, RUSTFS_CLIENT_CA_CERT_FILENAME};
use rustfs_tls_runtime::{ServerTlsMaterial as RuntimeServerTlsMaterial, TlsGeneration, TlsSource, create_multi_cert_resolver};
#[cfg(test)]
use rustfs_tls_runtime::{WebPkiClientVerifierOptions, build_webpki_client_verifier};
use rustfs_utils::{get_env_bool, get_env_opt_str};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Duration;
use tokio_rustls::TlsAcceptor;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

const LOG_COMPONENT_TLS: &str = "tls";
const LOG_SUBSYSTEM_TLS: &str = "tls_material";

/// System CA certificate search paths (platform-specific).
const SYSTEM_CA_PATHS: &[&str] = &[
    "/etc/ssl/certs/ca-certificates.crt",                  // Debian/Ubuntu/Alpine
    "/etc/pki/tls/certs/ca-bundle.crt",                    // Fedora/RHEL/CentOS
    "/etc/ssl/ca-bundle.pem",                              // OpenSUSE
    "/etc/pki/tls/cacert.pem",                             // OpenELEC
    "/etc/ssl/cert.pem",                                   // macOS/FreeBSD
    "/usr/local/etc/openssl/cert.pem",                     // macOS/Homebrew OpenSSL
    "/usr/local/share/certs/ca-root-nss.crt",              // FreeBSD
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",   // RHEL
    "/usr/share/pki/ca-trust-legacy/ca-bundle.legacy.crt", // RHEL legacy
];

// ── Public API ──

/// Load all TLS materials from the given directory in a single pass.
///
/// Uses `rustfs_tls_runtime::TlsMaterialSnapshot` as the single disk-read point,
/// then enriches the outbound material with platform-specific CAs that the runtime
/// crate does not handle (system roots, leaf-as-CA, mTLS env-var path overrides).
///
/// Returns the fully enriched snapshot ready for both outbound publishing and
/// acceptor construction.
pub async fn load_tls_material(tls_path: &str) -> Result<rustfs_tls_runtime::TlsMaterialSnapshot, TlsMaterialError> {
    load_tls_material_observed(
        tls_path,
        #[cfg(test)]
        None,
    )
    .await
}

async fn load_tls_material_observed(
    tls_path: &str,
    #[cfg(test)] read_control: Option<Arc<ReloadReadTestControl>>,
) -> Result<rustfs_tls_runtime::TlsMaterialSnapshot, TlsMaterialError> {
    if tls_path.is_empty() {
        return Err(TlsMaterialError::Io("TLS path is empty".into()));
    }

    let tls_source = TlsSource::from_directory(tls_path);
    let mut snapshot = tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        if let Some(control) = &read_control {
            control.entered.cancel();
            let _ = control.release.lock().unwrap_or_else(|e| e.into_inner()).recv();
        }
        let result = (|| {
            let pinned_source = tls_source.snapshot().map_err(map_runtime_tls_error)?;
            if let Some(snapshot) =
                super::tls_publish::configured_server_snapshot(&tls_source.base_dir.to_string_lossy(), &pinned_source)
                    .map_err(|_| TlsMaterialError::Io("invalid certificate publication source".into()))?
            {
                return Ok(snapshot);
            }
            rustfs_tls_runtime::TlsMaterialSnapshot::load_from_snapshot(&pinned_source).map_err(map_runtime_tls_error)
        })();
        #[cfg(test)]
        if let Some(control) = &read_control {
            control
                .result
                .store(if result.is_ok() { 1 } else { 2 }, std::sync::atomic::Ordering::Release);
            control.finished.cancel();
        }
        result
    })
    .await
    .map_err(|_| TlsMaterialError::Io("TLS material loader failed".into()))??;

    enrich_outbound(&mut snapshot).await?;

    Ok(snapshot)
}

/// Build a TLS acceptor from pre-loaded server material.
///
/// Takes the server material already loaded by `load_tls_material` and constructs
/// the `rustls::ServerConfig` without any additional disk reads (except the mTLS
/// client CA verifier, which reads the client CA cert from disk).
#[cfg(test)]
pub(crate) async fn build_acceptor_from_loaded(
    server: Option<RuntimeServerTlsMaterial>,
    tls_dir: &Path,
) -> Result<Option<Arc<TlsAcceptorHolder>>, TlsMaterialError> {
    let mtls_verifier = build_webpki_client_verifier(
        WebPkiClientVerifierOptions::builder(tls_dir, RUSTFS_CLIENT_CA_CERT_FILENAME, RUSTFS_CA_CERT)
            .enabled(get_env_bool(ENV_SERVER_MTLS_ENABLE, DEFAULT_SERVER_MTLS_ENABLE))
            .build(),
    )
    .map_err(|e| TlsMaterialError::Io(format!("build mTLS verifier: {e}")))?;

    build_acceptor_with_verifier(server, mtls_verifier)
}

fn snapshot_client_verifier(
    snapshot: &rustfs_tls_runtime::TlsMaterialSnapshot,
) -> Result<Option<Arc<dyn rustls::server::danger::ClientCertVerifier>>, TlsMaterialError> {
    if !get_env_bool(ENV_SERVER_MTLS_ENABLE, DEFAULT_SERVER_MTLS_ENABLE) {
        return Ok(None);
    }
    let pem = snapshot
        .server_client_ca_pem
        .as_deref()
        .ok_or_else(|| TlsMaterialError::Parse("mTLS client trust is missing".into()))?;
    let mut reader = Cursor::new(pem);
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_reader_iter(&mut reader) {
        roots
            .add(cert.map_err(|e| TlsMaterialError::Parse(e.to_string()))?)
            .map_err(|e| TlsMaterialError::Parse(e.to_string()))?;
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| TlsMaterialError::Parse(e.to_string()))?;
    #[cfg(test)]
    TLS_PREPARE_COUNTS.with(|counts| {
        let (roots, keys) = counts.get();
        counts.set((roots + 1, keys));
    });
    Ok(Some(verifier))
}

#[cfg(test)]
thread_local! {
    static TLS_PREPARE_COUNTS: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

/// Expensive certificate signing keys and client trust are prepared once per generation.
/// TCP and QUIC retain this shared resolver while owning their protocol settings.
#[derive(Clone)]
pub(crate) struct PreparedServerTls {
    resolver: Arc<dyn rustls::server::ResolvesServerCert>,
    verifier: Option<Arc<dyn rustls::server::danger::ClientCertVerifier>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl PreparedServerTls {
    pub(crate) fn from_snapshot(snapshot: &rustfs_tls_runtime::TlsMaterialSnapshot) -> Result<Option<Self>, TlsMaterialError> {
        Self::from_material(snapshot.server.as_ref(), snapshot_client_verifier(snapshot)?)
    }

    fn from_material(
        server: Option<&RuntimeServerTlsMaterial>,
        verifier: Option<Arc<dyn rustls::server::danger::ClientCertVerifier>>,
    ) -> Result<Option<Self>, TlsMaterialError> {
        let Some(server) = server else {
            return Ok(None);
        };
        let provider = rustls::ServerConfig::builder().crypto_provider().clone();
        let (resolver, validity): (Arc<dyn rustls::server::ResolvesServerCert>, _) = match server {
            RuntimeServerTlsMaterial::SingleCert { certs, key } => {
                let validity = certificate_validities(std::iter::once(certs.as_slice()))?;
                let certified = rustls::sign::CertifiedKey::from_der(certs.clone(), key.clone_key(), &provider)
                    .map_err(|e| TlsMaterialError::Io(format!("configure single cert: {e}")))?;
                (Arc::new(rustls::sign::SingleCertAndKey::from(certified)), validity)
            }
            RuntimeServerTlsMaterial::MultiCert { cert_key_pairs } => {
                let validity = certificate_validities(cert_key_pairs.values().map(|(certs, _)| certs.as_slice()))?;
                let pairs = cert_key_pairs
                    .iter()
                    .map(|(name, (certs, key))| (name.clone(), (certs.clone(), key.clone_key())))
                    .collect();
                let resolver = create_multi_cert_resolver(pairs)
                    .map_err(|e| TlsMaterialError::Parse(format!("build multi-cert resolver: {e}")))?;
                (Arc::new(resolver), validity)
            }
        };
        #[cfg(test)]
        TLS_PREPARE_COUNTS.with(|counts| {
            let (roots, keys) = counts.get();
            counts.set((roots, keys + 1));
        });
        Ok(Some(Self {
            resolver: Arc::new(ValidityCheckedResolver {
                inner: resolver,
                validity,
            }),
            verifier,
            provider,
        }))
    }

    pub(crate) fn tcp_acceptor(&self) -> Result<Arc<TlsAcceptorHolder>, TlsMaterialError> {
        let config = self.server_config(ServerProtocol::Tcp)?;
        Ok(Arc::new(TlsAcceptorHolder::new(Arc::new(TlsAcceptor::from(Arc::new(config))))))
    }

    #[cfg(feature = "http3")]
    pub(crate) fn http3_config(&self) -> Result<quinn::ServerConfig, TlsMaterialError> {
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(self.server_config(ServerProtocol::Http3)?)
            .map_err(|e| TlsMaterialError::Io(format!("configure QUIC TLS: {e}")))?;
        Ok(quinn::ServerConfig::with_crypto(Arc::new(crypto)))
    }

    fn server_config(&self, protocol: ServerProtocol) -> Result<rustls::ServerConfig, TlsMaterialError> {
        let versions = match protocol {
            ServerProtocol::Tcp => rustls::DEFAULT_VERSIONS,
            #[cfg(feature = "http3")]
            ServerProtocol::Http3 => &[&rustls::version::TLS13],
        };
        let builder = rustls::ServerConfig::builder_with_provider(self.provider.clone())
            .with_protocol_versions(versions)
            .map_err(|e| TlsMaterialError::Io(format!("configure TLS protocol: {e}")))?;
        let mut config = if let Some(verifier) = &self.verifier {
            builder
                .with_client_cert_verifier(verifier.clone())
                .with_cert_resolver(self.resolver.clone())
        } else {
            builder.with_no_client_auth().with_cert_resolver(self.resolver.clone())
        };
        config.alpn_protocols = match protocol {
            ServerProtocol::Tcp => vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()],
            #[cfg(feature = "http3")]
            ServerProtocol::Http3 => vec![b"h3".to_vec()],
        };
        #[cfg(feature = "http3")]
        if matches!(protocol, ServerProtocol::Http3) {
            config.max_early_data_size = 0;
        }
        config.session_storage = rustls::server::ServerSessionMemoryCache::new(10000);
        if tls_key_log() {
            config.key_log = Arc::new(rustls::KeyLogFile::new());
        }
        Ok(config)
    }
}

#[cfg(test)]
fn build_acceptor_with_verifier(
    server: Option<RuntimeServerTlsMaterial>,
    mtls_verifier: Option<Arc<dyn rustls::server::danger::ClientCertVerifier>>,
) -> Result<Option<Arc<TlsAcceptorHolder>>, TlsMaterialError> {
    PreparedServerTls::from_material(server.as_ref(), mtls_verifier)?
        .map(|prepared| prepared.tcp_acceptor())
        .transpose()
}

#[cfg(all(test, feature = "http3"))]
pub(crate) async fn build_http3_server_config_from_loaded(
    server: Option<&RuntimeServerTlsMaterial>,
    tls_dir: &Path,
) -> Result<Option<quinn::ServerConfig>, TlsMaterialError> {
    let mtls_verifier = build_webpki_client_verifier(
        WebPkiClientVerifierOptions::builder(tls_dir, RUSTFS_CLIENT_CA_CERT_FILENAME, RUSTFS_CA_CERT)
            .enabled(get_env_bool(ENV_SERVER_MTLS_ENABLE, DEFAULT_SERVER_MTLS_ENABLE))
            .build(),
    )
    .map_err(|e| TlsMaterialError::Io(format!("build mTLS verifier: {e}")))?;
    PreparedServerTls::from_material(server, mtls_verifier)?
        .map(|prepared| prepared.http3_config())
        .transpose()
}

// ── Outbound Enrichment ──

/// Enrich the outbound TLS material with platform-specific additions that the
/// tls-runtime crate does not handle:
/// 1. Optional: server leaf cert as root CA (`RUSTFS_TRUST_LEAF_CERT_AS_CA`)
/// 2. Optional: system root CAs from platform paths (`RUSTFS_TRUST_SYSTEM_CA`)
/// 3. Optional: mTLS identity from env-var-overridden paths
async fn enrich_outbound(snapshot: &mut rustfs_tls_runtime::TlsMaterialSnapshot) -> Result<(), TlsMaterialError> {
    let tls_dir = &snapshot.source.base_dir;
    let outbound = &mut snapshot.outbound;
    if get_env_bool(ENV_TRUST_LEAF_CERT_AS_CA, DEFAULT_TRUST_LEAF_CERT_AS_CA) {
        let chains: Vec<_> = match &snapshot.server {
            Some(RuntimeServerTlsMaterial::SingleCert { certs, .. }) => vec![certs],
            Some(RuntimeServerTlsMaterial::MultiCert { cert_key_pairs }) => {
                cert_key_pairs.values().map(|(certs, _)| certs).collect()
            }
            None => Vec::new(),
        };
        let mut certificates = BTreeSet::new();
        for chain in chains {
            certificates.extend(chain.iter().map(|cert| cert.as_ref().to_vec()));
        }
        for certificate in certificates {
            outbound.root_ca_pem.extend_from_slice(
                format!(
                    "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
                    base64_simd::STANDARD.encode_to_string(&certificate),
                )
                .as_bytes(),
            );
        }
    }

    // 2. Optional: load system root CAs
    if get_env_bool(ENV_TRUST_SYSTEM_CA, DEFAULT_TRUST_SYSTEM_CA) {
        let mut system_loaded = false;
        for path in SYSTEM_CA_PATHS {
            if load_cert_file(Path::new(path), &mut outbound.root_ca_pem, "system root certificates").await {
                system_loaded = true;
                info!(
                    component = LOG_COMPONENT_TLS,
                    subsystem = LOG_SUBSYSTEM_TLS,
                    event = "tls_root_ca_enriched",
                    source = "system_roots",
                    path,
                    "TLS root CA set enriched"
                );
                break;
            }
        }
        if !system_loaded {
            debug!(
                component = LOG_COMPONENT_TLS,
                subsystem = LOG_SUBSYSTEM_TLS,
                event = "tls_root_ca_enriched",
                source = "system_roots",
                state = "not_found",
                "No system root certificates were loaded"
            );
        }
    } else {
        info!(
            component = LOG_COMPONENT_TLS,
            subsystem = LOG_SUBSYSTEM_TLS,
            event = "tls_root_ca_enriched",
            source = "system_roots",
            state = "disabled",
            "System root CA loading disabled"
        );
    }

    // 3. Optional: override mTLS identity from env-var paths
    let env_cert = get_env_opt_str(ENV_MTLS_CLIENT_CERT);
    let env_key = get_env_opt_str(ENV_MTLS_CLIENT_KEY);
    if (env_cert.is_some() || env_key.is_some())
        && let Some(identity) = load_mtls_identity_from_overridden_paths(env_cert.as_deref(), env_key.as_deref(), tls_dir).await?
    {
        outbound.mtls_identity = Some(identity);
    }

    Ok(())
}

/// Load mTLS client identity when env-var path overrides are set.
/// If neither env var is set, returns Ok(None) (the tls-runtime already loaded
/// identity from the default path).
async fn load_mtls_identity_from_overridden_paths(
    env_cert: Option<&str>,
    env_key: Option<&str>,
    tls_dir: &Path,
) -> Result<Option<MtlsIdentityPem>, TlsMaterialError> {
    let client_cert_path = match env_cert {
        Some(p) => PathBuf::from(p),
        None => tls_dir.join(RUSTFS_CLIENT_CERT_FILENAME),
    };
    let client_key_path = match env_key {
        Some(p) => PathBuf::from(p),
        None => tls_dir.join(RUSTFS_CLIENT_KEY_FILENAME),
    };

    let cert_pem = tokio::fs::read(&client_cert_path)
        .await
        .map_err(|e| TlsMaterialError::Io(format!("read client cert {client_cert_path:?}: {e}")))?;
    let key_pem = tokio::fs::read(&client_key_path)
        .await
        .map_err(|e| TlsMaterialError::Io(format!("read client key {client_key_path:?}: {e}")))?;

    let mut reader = std::io::Cursor::new(&cert_pem);
    let certs = CertificateDer::pem_reader_iter(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsMaterialError::Parse(format!("invalid client certificate: {e}")))?;
    let mut reader = std::io::Cursor::new(&key_pem);
    let key =
        PrivateKeyDer::from_pem_reader(&mut reader).map_err(|e| TlsMaterialError::Parse(format!("invalid client key: {e}")))?;
    rustfs_tls_runtime::validate_certified_key(&certs, &key, "client identity override", false).map_err(map_runtime_tls_error)?;

    info!(
        component = LOG_COMPONENT_TLS,
        subsystem = LOG_SUBSYSTEM_TLS,
        event = "mtls_identity_state",
        state = "loaded",
        cert_path = ?client_cert_path,
        key_path = ?client_key_path,
        "mTLS client identity loaded"
    );
    Ok(Some(MtlsIdentityPem { cert_pem, key_pem }))
}

// ── Helpers ──

fn map_runtime_tls_error(err: rustfs_tls_runtime::TlsRuntimeError) -> TlsMaterialError {
    match &err {
        rustfs_tls_runtime::TlsRuntimeError::Material(msg) | rustfs_tls_runtime::TlsRuntimeError::Publication(msg) => {
            TlsMaterialError::Parse(msg.clone())
        }
        _ => TlsMaterialError::Io(err.to_string()),
    }
}

pub(crate) async fn validate_configured_oidc_extra_ca_cert() -> Result<(), TlsMaterialError> {
    if let Some(path) = configured_oidc_extra_ca_cert_path() {
        let _ = load_configured_oidc_extra_ca_cert().await?;
        info!(
            component = LOG_COMPONENT_TLS,
            subsystem = LOG_SUBSYSTEM_TLS,
            event = "oidc_extra_ca_validated",
            source = "oidc_extra_ca_bundle",
            env_var = ENV_RUSTFS_EXTRA_CA_CERT,
            path = ?path,
            "OIDC extra root CA bundle validated"
        );
    }
    Ok(())
}

pub(crate) async fn load_configured_oidc_extra_ca_cert() -> Result<Option<Vec<u8>>, TlsMaterialError> {
    let Some(path) = configured_oidc_extra_ca_cert_path() else {
        return Ok(None);
    };

    let data = tokio::fs::read(&path)
        .await
        .map_err(|e| TlsMaterialError::Io(format!("read extra CA bundle {path:?}: {e}")))?;
    validate_cert_bundle(&data, &path)?;
    Ok(Some(data))
}

fn configured_oidc_extra_ca_cert_path() -> Option<PathBuf> {
    let path = get_env_opt_str(ENV_RUSTFS_EXTRA_CA_CERT)?;
    let path = path.trim();
    if path.is_empty() {
        return None;
    }
    Some(PathBuf::from(path))
}

fn validate_cert_bundle(data: &[u8], path: &Path) -> Result<(), TlsMaterialError> {
    let mut reader = Cursor::new(data);
    let mut found = false;
    let mut store = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_reader_iter(&mut reader) {
        let cert = cert.map_err(|e| TlsMaterialError::Parse(format!("invalid extra CA bundle {path:?}: {e}")))?;
        store
            .add(cert)
            .map_err(|e| TlsMaterialError::Parse(format!("invalid extra CA bundle {path:?}: {e}")))?;
        found = true;
    }
    if !found {
        return Err(TlsMaterialError::Parse(format!("no certificate found in extra CA bundle {path:?}")));
    }
    Ok(())
}

/// Load a single certificate file and append PEM data.
/// Returns true if the file was successfully loaded.
async fn load_cert_file(path: &Path, pem_data: &mut Vec<u8>, desc: &str) -> bool {
    if tokio::fs::metadata(path).await.is_err() {
        debug!(
            component = LOG_COMPONENT_TLS,
            subsystem = LOG_SUBSYSTEM_TLS,
            event = "tls_material_file_state",
            description = desc,
            path = ?path,
            state = "missing",
            "TLS material file state changed"
        );
        return false;
    }
    match tokio::fs::read(path).await {
        Ok(data) => {
            pem_data.extend_from_slice(&data);
            pem_data.push(b'\n');
            info!(
                component = LOG_COMPONENT_TLS,
                subsystem = LOG_SUBSYSTEM_TLS,
                event = "tls_material_file_state",
                description = desc,
                path = ?path,
                state = "loaded",
                "TLS material file state changed"
            );
            true
        }
        Err(e) => {
            debug!(
                component = LOG_COMPONENT_TLS,
                subsystem = LOG_SUBSYSTEM_TLS,
                event = "tls_material_file_state",
                description = desc,
                path = ?path,
                state = "read_failed",
                error = %e,
                "TLS material file state changed"
            );
            false
        }
    }
}

/// Validity windows indexed by leaf certificate DER.
type CertificateValidities = HashMap<Vec<u8>, (i64, i64)>;

fn certificate_validities<'a>(
    chains: impl Iterator<Item = &'a [CertificateDer<'static>]>,
) -> Result<CertificateValidities, TlsMaterialError> {
    let mut validity = HashMap::new();
    for chain in chains {
        let leaf = chain
            .first()
            .ok_or_else(|| TlsMaterialError::Parse("empty certificate chain".into()))?;
        let (_, parsed) = x509_parser::parse_x509_certificate(leaf.as_ref())
            .map_err(|e| TlsMaterialError::Parse(format!("invalid certificate: {e}")))?;
        validity.insert(
            leaf.as_ref().to_vec(),
            (parsed.validity().not_before.timestamp(), parsed.validity().not_after.timestamp()),
        );
    }
    Ok(validity)
}

#[derive(Debug)]
struct ValidityCheckedResolver {
    inner: Arc<dyn rustls::server::ResolvesServerCert>,
    validity: CertificateValidities,
}

impl rustls::server::ResolvesServerCert for ValidityCheckedResolver {
    fn resolve(&self, hello: rustls::server::ClientHello<'_>) -> Option<Arc<rustls::sign::CertifiedKey>> {
        let certificate = self.inner.resolve(hello)?;
        let leaf = certificate.cert.first()?;
        let (not_before, not_after) = self.validity.get(leaf.as_ref())?;
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_secs(),
        )
        .ok()?;
        (*not_before <= now && now < *not_after).then_some(certificate)
    }
}

enum ServerProtocol {
    Tcp,
    #[cfg(feature = "http3")]
    Http3,
}

fn tls_key_log() -> bool {
    get_env_bool(ENV_TLS_KEYLOG, DEFAULT_TLS_KEYLOG)
}

// ── Errors ──

#[derive(Debug)]
pub enum TlsMaterialError {
    Io(String),
    Parse(String),
    ReloadStopping,
    ReloadTaskFailed(Arc<tokio::task::JoinError>),
    #[cfg(feature = "http3")]
    ReloadStartupCleanup {
        reload: Box<TlsMaterialError>,
        http3_join_error: Arc<tokio::task::JoinError>,
    },
}

impl std::fmt::Display for TlsMaterialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsMaterialError::Io(msg) => write!(f, "TLS material I/O error: {msg}"),
            TlsMaterialError::Parse(msg) => write!(f, "TLS material parse error: {msg}"),
            TlsMaterialError::ReloadStopping => write!(f, "TLS reload group is stopping"),
            TlsMaterialError::ReloadTaskFailed(error) => {
                write!(f, "TLS reload group {}", if error.is_cancelled() { "cancelled" } else { "panicked" })
            }
            #[cfg(feature = "http3")]
            TlsMaterialError::ReloadStartupCleanup { http3_join_error, .. } => write!(
                f,
                "TLS reload startup failed; HTTP/3 outer join {}",
                if http3_join_error.is_cancelled() {
                    "cancelled"
                } else {
                    "panicked"
                }
            ),
        }
    }
}

impl std::error::Error for TlsMaterialError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ReloadTaskFailed(error) => Some(error.as_ref()),
            #[cfg(feature = "http3")]
            Self::ReloadStartupCleanup { reload, .. } => Some(reload.as_ref()),
            _ => None,
        }
    }
}

// ── TLS Handshake Error Classification ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TlsHandshakeFailureKind {
    UnexpectedEof,
    ProtocolVersion,
    Certificate,
    Alert,
    Timeout,
    Unknown,
}

impl TlsHandshakeFailureKind {
    pub(crate) fn classify(err_msg: &str) -> Self {
        if err_msg.contains("unexpected EOF") || err_msg.contains("handshake eof") {
            Self::UnexpectedEof
        } else if err_msg.contains("protocol version") {
            Self::ProtocolVersion
        } else if err_msg.contains("certificate") || err_msg.contains("invalid peer certificate") {
            Self::Certificate
        } else if err_msg.contains("alert") {
            Self::Alert
        } else {
            Self::Unknown
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::UnexpectedEof => "UNEXPECTED_EOF",
            Self::ProtocolVersion => "PROTOCOL_VERSION",
            Self::Certificate => "CERTIFICATE",
            Self::Alert => "ALERT",
            Self::Timeout => "TIMEOUT",
            Self::Unknown => "UNKNOWN",
        }
    }
}

/// Why a TLS handshake did not produce a usable stream.
pub(crate) enum TlsAcceptFailure {
    Handshake(std::io::Error),
    Timeout,
}

/// Run the server-side TLS handshake under `handshake_timeout`.
///
/// Security invariant: the peer is unauthenticated at this point and the connection cap is
/// unlimited by default, so this deadline is the only control that sheds a client which opens a
/// socket and then stalls the handshake forever.
pub(crate) async fn accept_tls_with_deadline<IO>(
    acceptor: &TlsAcceptor,
    socket: IO,
    handshake_timeout: Duration,
) -> Result<tokio_rustls::server::TlsStream<IO>, TlsAcceptFailure>
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match tokio::time::timeout(handshake_timeout, acceptor.accept(socket)).await {
        Ok(Ok(tls_socket)) => Ok(tls_socket),
        Ok(Err(err)) => Err(TlsAcceptFailure::Handshake(err)),
        Err(_) => Err(TlsAcceptFailure::Timeout),
    }
}

// ── TLS Acceptor Holder (for hot reload) ──

pub(crate) struct TlsAcceptorHolder {
    current: RwLock<Arc<TlsAcceptor>>,
}

impl TlsAcceptorHolder {
    pub(crate) fn new(acceptor: Arc<TlsAcceptor>) -> Self {
        Self {
            current: RwLock::new(acceptor),
        }
    }

    #[inline]
    pub(crate) fn get(&self) -> Arc<TlsAcceptor> {
        match self.current.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn swap(&self, new_holder: &TlsAcceptorHolder) {
        let new_acceptor = new_holder.get();
        match self.current.write() {
            Ok(mut guard) => *guard = new_acceptor,
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                *guard = new_acceptor;
            }
        }
    }
}

// Build both configurations before publishing either listener's new certificate.
#[cfg(all(test, feature = "http3"))]
async fn reload_acceptors(
    server: Option<RuntimeServerTlsMaterial>,
    tls_dir: &Path,
    holder: &TlsAcceptorHolder,
    #[cfg(feature = "http3")] http3_endpoint: Option<&quinn::Endpoint>,
) -> Result<bool, TlsMaterialError> {
    let verifier = build_webpki_client_verifier(
        WebPkiClientVerifierOptions::builder(tls_dir, RUSTFS_CLIENT_CA_CERT_FILENAME, RUSTFS_CA_CERT)
            .enabled(get_env_bool(ENV_SERVER_MTLS_ENABLE, DEFAULT_SERVER_MTLS_ENABLE))
            .build(),
    )
    .map_err(|e| TlsMaterialError::Io(format!("build mTLS verifier: {e}")))?;
    let Some(prepared) = PreparedServerTls::from_material(server.as_ref(), verifier)? else {
        return Ok(false);
    };
    let http3_config = if http3_endpoint.is_some() {
        let mut config = prepared.http3_config()?;
        config.migration(false);
        Some(config)
    } else {
        None
    };
    let new_holder = prepared.tcp_acceptor()?;
    #[cfg(feature = "http3")]
    if let (Some(endpoint), Some(config)) = (http3_endpoint, http3_config) {
        endpoint.set_server_config(Some(config));
    }
    holder.swap(&new_holder);
    Ok(true)
}

// ── Reload Loop ──

/// Spawn a background task that periodically checks for TLS certificate changes.
/// Single load per tick: loads once via tls-runtime, enriches, publishes outbound,
/// and builds acceptor — no double reads.
#[derive(Clone)]
struct ReloadTarget {
    holder: Arc<TlsAcceptorHolder>,
    rounds: TaskTracker,
    #[cfg(feature = "http3")]
    http3_endpoint: Option<quinn::Endpoint>,
}

struct ReloadState {
    closing: bool,
    round_tokens: Option<Vec<tokio_util::task::task_tracker::TaskTrackerToken>>,
    targets: HashMap<u64, ReloadTarget>,
    accepted: Arc<rustfs_tls_runtime::TlsMaterialSnapshot>,
    tcp: Arc<TlsAcceptor>,
    prepared: PreparedServerTls,
    #[cfg(feature = "http3")]
    quic: Option<quinn::ServerConfig>,
}

struct ReloadGroup {
    required_startup_files: BTreeSet<String>,
    state: Mutex<ReloadState>,
    changed: tokio::sync::Notify,
    next_id: std::sync::atomic::AtomicU64,
    driver: std::sync::OnceLock<ReloadJoin>,
    #[cfg(test)]
    read_control: Mutex<Option<Arc<ReloadReadTestControl>>>,
    #[cfg(test)]
    panic_before_read: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    failure_entered: tokio_util::sync::CancellationToken,
    #[cfg(test)]
    driver_abort: std::sync::OnceLock<tokio::task::AbortHandle>,
    #[cfg(test)]
    pass_completed: tokio::sync::Notify,
}

// Lock order is registry, then group state, then acceptor. Neither guard crosses an await.
static RELOAD_GROUPS: LazyLock<Mutex<HashMap<String, Weak<ReloadGroup>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) fn observed_server_snapshot(tls_path: &str) -> Option<Arc<rustfs_tls_runtime::TlsMaterialSnapshot>> {
    let registry = RELOAD_GROUPS.lock().unwrap_or_else(|e| e.into_inner());
    let group = registry.get(tls_path)?.upgrade()?;
    let state = group.state.lock().unwrap_or_else(|e| e.into_inner());
    (!state.targets.is_empty()).then(|| state.accepted.clone())
}

/// Resolve the source actually registered by listener configuration, including CLI overrides.
pub(crate) fn active_server_tls_path() -> Option<String> {
    let registry = RELOAD_GROUPS.lock().unwrap_or_else(|e| e.into_inner());
    let mut paths = registry.iter().filter_map(|(path, group)| {
        let group = group.upgrade()?;
        let state = group.state.lock().unwrap_or_else(|e| e.into_inner());
        (!state.targets.is_empty()).then(|| path.clone())
    });
    let path = paths.next()?;
    paths.next().is_none().then_some(path)
}

pub(crate) fn notify_server_publication(tls_path: &str) {
    let registry = RELOAD_GROUPS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(group) = registry.get(tls_path).and_then(Weak::upgrade) {
        group.changed.notify_one();
    }
}

#[cfg(test)]
struct ReloadReadTestControl {
    entered: tokio_util::sync::CancellationToken,
    finished: tokio_util::sync::CancellationToken,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    result: std::sync::atomic::AtomicU8,
}

struct ReloadRegistration {
    group: Arc<ReloadGroup>,
    id: u64,
}

impl Drop for ReloadRegistration {
    fn drop(&mut self) {
        {
            let _registry = RELOAD_GROUPS.lock().unwrap_or_else(|e| e.into_inner());
            let mut state = self.group.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(target) = state.targets.remove(&self.id) {
                // Round tokens are captured under this same lock before a read starts.
                target.rounds.close();
            }
            if state.targets.is_empty() {
                state.closing = true;
            }
        }
        self.group.changed.notify_one();
    }
}

struct ReloadRound {
    group: Arc<ReloadGroup>,
}

impl Drop for ReloadRound {
    fn drop(&mut self) {
        let tokens = self.group.state.lock().unwrap_or_else(|e| e.into_inner()).round_tokens.take();
        drop(tokens);
    }
}

type ReloadJoin = Shared<BoxFuture<'static, Result<(), Arc<tokio::task::JoinError>>>>;

/// A stopped registration has drained every round captured before its stop fence.
/// The shared result records the original group task's eventual join, including failures.
#[derive(Clone)]
pub(crate) struct TlsReloadRetirement {
    joined: ReloadJoin,
}

impl std::fmt::Debug for TlsReloadRetirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsReloadRetirement")
            .field("group_joined", &self.group_joined())
            .finish()
    }
}

impl TlsReloadRetirement {
    pub(crate) fn group_joined(&self) -> bool {
        matches!(self.joined.peek(), Some(Ok(())))
    }

    pub(crate) fn join_error(&self) -> Option<&Arc<tokio::task::JoinError>> {
        self.joined.peek().and_then(|result| result.as_ref().err())
    }
}

#[cfg(test)]
pub(crate) fn test_retirement_from_task(task: tokio::task::JoinHandle<()>) -> TlsReloadRetirement {
    TlsReloadRetirement {
        joined: task.map(|result| result.map_err(Arc::new)).boxed().shared(),
    }
}

#[cfg(test)]
impl TlsReloadRetirement {
    pub(crate) async fn test_observe_join(&self) {
        let _ = self.joined.clone().await;
    }
}

pub(crate) struct TlsReloadTask {
    registration: Mutex<Option<ReloadRegistration>>,
    group: Arc<ReloadGroup>,
    rounds: TaskTracker,
    joined: ReloadJoin,
}

impl TlsReloadTask {
    pub(crate) fn request_stop(&self) {
        // Handle registration -> registry -> group state; no inverse acquisition or await.
        self.registration.lock().unwrap_or_else(|e| e.into_inner()).take();
    }

    pub(crate) async fn wait_stopped(&self) -> TlsReloadRetirement {
        self.request_stop();
        self.rounds.wait().await;
        let _ = self.joined.clone().now_or_never();
        let empty = self.group.state.lock().unwrap_or_else(|e| e.into_inner()).targets.is_empty();
        if empty {
            // The same-lock last-stop fence marks this generation closing.
            // New listeners cannot revive it; they receive ReloadStopping until it exits.
            let _ = self.joined.clone().await;
        }
        TlsReloadRetirement {
            joined: self.joined.clone(),
        }
    }

    #[cfg(test)]
    pub(super) fn abort(&self) {
        self.request_stop();
    }
}

impl Drop for TlsReloadTask {
    fn drop(&mut self) {
        self.request_stop();
    }
}

// Existing module tests request stop with abort(), then await the same real group.
#[cfg(test)]
impl std::future::Future for TlsReloadTask {
    type Output = Result<(), Arc<tokio::task::JoinError>>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.get_mut().joined.poll_unpin(cx)
    }
}

fn server_names(server: &Option<RuntimeServerTlsMaterial>) -> Result<BTreeMap<String, BTreeSet<String>>, TlsMaterialError> {
    let chains: Vec<_> = match server {
        Some(RuntimeServerTlsMaterial::SingleCert { certs, .. }) => vec![("default", certs)],
        Some(RuntimeServerTlsMaterial::MultiCert { cert_key_pairs }) => cert_key_pairs
            .iter()
            .map(|(selector, (certs, _))| (selector.as_str(), certs))
            .collect(),
        None => Vec::new(),
    };
    let mut selectors = BTreeMap::new();
    for (selector, chain) in chains {
        let leaf = chain
            .first()
            .ok_or_else(|| TlsMaterialError::Parse("empty certificate chain".into()))?;
        selectors.insert(selector.to_string(), certificate_names(leaf)?);
    }
    Ok(selectors)
}

pub(crate) fn certificate_names(leaf: &CertificateDer<'_>) -> Result<BTreeSet<String>, TlsMaterialError> {
    let mut names = BTreeSet::new();
    let (_, parsed) = x509_parser::parse_x509_certificate(leaf.as_ref())
        .map_err(|e| TlsMaterialError::Parse(format!("invalid certificate: {e}")))?;
    if let Some(san) = parsed
        .subject_alternative_name()
        .map_err(|e| TlsMaterialError::Parse(format!("invalid SAN: {e}")))?
    {
        for name in &san.value.general_names {
            match name {
                x509_parser::extensions::GeneralName::DNSName(name) => {
                    names.insert(format!("dns:{}", name.to_ascii_lowercase()));
                }
                x509_parser::extensions::GeneralName::IPAddress(bytes) => {
                    names.insert(format!("ip:{bytes:?}"));
                }
                _ => {}
            }
        }
    }
    Ok(names)
}

pub(crate) fn validate_required_material_files(
    required: &BTreeSet<String>,
    candidate: &rustfs_tls_runtime::TlsMaterialSnapshot,
) -> Result<(), TlsMaterialError> {
    if !required.is_subset(&candidate.material_files) {
        return Err(TlsMaterialError::Parse("required TLS source components disappeared".into()));
    }
    Ok(())
}

pub(crate) fn validate_candidate_replacement(
    accepted: &rustfs_tls_runtime::TlsMaterialSnapshot,
    candidate: &rustfs_tls_runtime::TlsMaterialSnapshot,
) -> Result<(), TlsMaterialError> {
    validate_required_material_files(&accepted.material_files, candidate)?;
    let old_names = server_names(&accepted.server)?;
    let new_names = server_names(&candidate.server)?;
    for (selector, names) in old_names {
        if !new_names.get(&selector).is_some_and(|new| names.is_subset(new)) {
            return Err(TlsMaterialError::Parse("required server certificate names disappeared".into()));
        }
    }
    Ok(())
}

fn outbound_material_matches(a: &rustfs_tls_runtime::OutboundTlsMaterial, b: &rustfs_tls_runtime::OutboundTlsMaterial) -> bool {
    a.root_ca_pem == b.root_ca_pem && a.mtls_identity == b.mtls_identity
}

pub(crate) fn spawn_reload_loop(
    tls_path: String,
    holder: Arc<TlsAcceptorHolder>,
    initial: rustfs_tls_runtime::TlsMaterialSnapshot,
    prepared: PreparedServerTls,
    #[cfg(feature = "http3")] http3_endpoint: Option<quinn::Endpoint>,
) -> Result<Option<TlsReloadTask>, TlsMaterialError> {
    if !get_env_bool(ENV_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_ENABLE) {
        return Ok(None);
    }
    let required_startup_files = crate::startup_tls_material::required_startup_material_files(&tls_path);
    let initial = Arc::new(initial);
    let mut registry = RELOAD_GROUPS.lock().unwrap_or_else(|e| e.into_inner());
    registry.retain(|_, group| group.strong_count() != 0);
    let existing = registry.get(&tls_path).and_then(Weak::upgrade);
    if let Some(group) = &existing {
        if group.state.lock().unwrap_or_else(|e| e.into_inner()).closing {
            return Err(TlsMaterialError::ReloadStopping);
        }
        if let Some(result) = group.driver.get().and_then(|joined| joined.clone().now_or_never()) {
            return Err(match result {
                Ok(()) => TlsMaterialError::ReloadStopping,
                Err(error) => TlsMaterialError::ReloadTaskFailed(error),
            });
        }
    }
    let first = existing.is_none();
    let group = existing.unwrap_or_else(|| {
        Arc::new(ReloadGroup {
            required_startup_files,
            state: Mutex::new(ReloadState {
                closing: false,
                round_tokens: None,
                targets: HashMap::new(),
                accepted: initial.clone(),
                tcp: holder.get(),
                prepared,
                #[cfg(feature = "http3")]
                quic: None,
            }),
            changed: tokio::sync::Notify::new(),
            next_id: std::sync::atomic::AtomicU64::new(1),
            driver: std::sync::OnceLock::new(),
            #[cfg(test)]
            read_control: Mutex::new(None),
            #[cfg(test)]
            panic_before_read: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            failure_entered: tokio_util::sync::CancellationToken::new(),
            #[cfg(test)]
            driver_abort: std::sync::OnceLock::new(),
            #[cfg(test)]
            pass_completed: tokio::sync::Notify::new(),
        })
    });
    let id = group.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let rounds = TaskTracker::new();
    {
        let mut state = group.state.lock().unwrap_or_else(|e| e.into_inner());
        #[cfg(feature = "http3")]
        if let Some(endpoint) = &http3_endpoint {
            if state.quic.is_none() {
                let mut config = state.prepared.http3_config()?;
                config.migration(false);
                state.quic = Some(config);
            }
            endpoint.set_server_config(state.quic.clone());
        }
        holder.swap(&TlsAcceptorHolder::new(state.tcp.clone()));
        if let Some(tokens) = &mut state.round_tokens {
            // A listener joining mid-round can receive this publication too.
            tokens.push(rounds.token());
        }
        state.targets.insert(
            id,
            ReloadTarget {
                holder,
                rounds: rounds.clone(),
                #[cfg(feature = "http3")]
                http3_endpoint,
            },
        );
    }
    registry.insert(tls_path.clone(), Arc::downgrade(&group));
    if first {
        let driver = tokio::spawn(run_reload_group(tls_path, group.clone()));
        #[cfg(test)]
        let _ = group.driver_abort.set(driver.abort_handle());
        let joined = driver.map(|result| result.map_err(Arc::new)).boxed().shared();
        let _ = group.driver.set(joined);
    }
    let joined = group
        .driver
        .get()
        .expect("registered group retains its original driver")
        .clone();
    drop(registry);
    group.changed.notify_one();
    let registration = ReloadRegistration {
        group: group.clone(),
        id,
    };
    Ok(Some(TlsReloadTask {
        registration: Mutex::new(Some(registration)),
        group,
        rounds,
        joined,
    }))
}

async fn run_reload_group(tls_path: String, group: Arc<ReloadGroup>) {
    let interval_secs = rustfs_utils::get_env_u64(ENV_TLS_RELOAD_INTERVAL, DEFAULT_TLS_RELOAD_INTERVAL).max(5);
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut generation = 1_u64;
    loop {
        tokio::select! {
            _ = interval.tick() => {},
            _ = group.changed.notified() => {},
        }
        let (accepted, empty) = {
            let mut state = group.state.lock().unwrap_or_else(|e| e.into_inner());
            state.round_tokens = Some(state.targets.values().map(|target| target.rounds.token()).collect());
            (state.accepted.clone(), state.targets.is_empty())
        };
        let round = ReloadRound { group: group.clone() };
        if empty {
            let mut registry = RELOAD_GROUPS.lock().unwrap_or_else(|e| e.into_inner());
            if group.state.lock().unwrap_or_else(|e| e.into_inner()).targets.is_empty() {
                if registry
                    .get(&tls_path)
                    .and_then(Weak::upgrade)
                    .is_some_and(|current| Arc::ptr_eq(&current, &group))
                {
                    registry.remove(&tls_path);
                }
                return;
            }
            continue;
        }
        #[cfg(test)]
        if group.panic_before_read.swap(false, std::sync::atomic::Ordering::AcqRel) {
            group.failure_entered.cancel();
            panic!("test TLS group driver failure");
        }
        let result = async {
            #[cfg(test)]
            let read_control = group.read_control.lock().unwrap_or_else(|e| e.into_inner()).take();
            let candidate = load_tls_material_observed(
                &tls_path,
                #[cfg(test)]
                read_control,
            )
            .await?;
            validate_candidate_replacement(&accepted, &candidate)?;
            validate_required_material_files(&group.required_startup_files, &candidate)?;
            // Listener initialization can load a newer bundle than startup published.
            let published = startup_runtime_sources::current_outbound_tls_state().await;
            let required_roots =
                !accepted.outbound.root_ca_pem.is_empty() || published.root_ca_pem.as_ref().is_some_and(|pem| !pem.is_empty());
            let required_identity = accepted.outbound.mtls_identity.is_some() || published.mtls_identity.is_some();
            if (required_roots && candidate.outbound.root_ca_pem.is_empty())
                || (required_identity && candidate.outbound.mtls_identity.is_none())
            {
                return Err(TlsMaterialError::Parse("required outbound TLS material disappeared".into()));
            }
            let outbound_changed = !outbound_material_matches(&accepted.outbound, &candidate.outbound)
                || published.root_ca_pem.as_deref().unwrap_or_default() != candidate.outbound.root_ca_pem.as_slice()
                || published.mtls_identity != candidate.outbound.mtls_identity;
            if accepted.fingerprint == candidate.fingerprint && !outbound_changed {
                startup_runtime_sources::record_tls_reload_skipped("rustfs_server_reload_loop", "unchanged");
                return Ok(None);
            }
            let prepared = PreparedServerTls::from_snapshot(&candidate)?
                .ok_or_else(|| TlsMaterialError::Parse("server certificate is missing".into()))?;
            let tcp = prepared.tcp_acceptor()?;
            #[cfg(feature = "http3")]
            let quic = {
                let mut config = prepared.http3_config()?;
                config.migration(false);
                config
            };
            let candidate = Arc::new(candidate);
            let listeners = {
                let mut state = group.state.lock().unwrap_or_else(|e| e.into_inner());
                // Newly registered listeners join the same prepared generation under this lock.
                for target in state.targets.values() {
                    #[cfg(feature = "http3")]
                    if let Some(endpoint) = &target.http3_endpoint {
                        endpoint.set_server_config(Some(quic.clone()));
                    }
                    target.holder.swap(&tcp);
                }
                state.tcp = tcp.get();
                state.prepared = prepared;
                state.accepted = candidate.clone();
                #[cfg(feature = "http3")]
                {
                    state.quic = Some(quic);
                }
                state.targets.len()
            };
            if outbound_changed {
                let next = startup_runtime_sources::current_outbound_tls_generation().saturating_add(1);
                startup_runtime_sources::publish_outbound_tls_state(TlsGeneration(next), &candidate.outbound).await;
            }
            Ok(Some(listeners))
        }
        .await;
        match result {
            Ok(Some(listeners)) => {
                generation = generation.saturating_add(1);
                startup_runtime_sources::record_tls_generation("rustfs_server_reload_loop", generation);
                startup_runtime_sources::record_tls_reload_result("rustfs_server_reload_loop", "ok", None, Some(generation));
                info!(
                    event = "tls_reload_state",
                    component = LOG_COMPONENT_TLS,
                    subsystem = LOG_SUBSYSTEM_TLS,
                    state = "reloaded",
                    generation,
                    listeners,
                    "TLS material updated"
                );
            }
            Ok(None) => {}
            Err(error) => {
                startup_runtime_sources::record_tls_reload_result("rustfs_server_reload_loop", "rejected", None, None);
                warn!(event = "tls_reload_failed", component = LOG_COMPONENT_TLS, subsystem = LOG_SUBSYSTEM_TLS,
                    result = "rejected", error = %error, "TLS material update rejected");
            }
        }
        // Complete reads, parsing, publication and result processing before retiring this round.
        drop(round);
        #[cfg(test)]
        group.pass_completed.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::CertifiedKey;
    use rustfs_config::RUSTFS_TLS_CERT;
    use std::fs;
    use std::sync::Once;
    use tempfile::TempDir;

    fn ensure_rustls_crypto_provider() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            if rustls::crypto::CryptoProvider::get_default().is_none() {
                let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            }
        });
    }

    fn write_test_cert_pair(dir: &Path, subject: &str) {
        let CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed(vec![subject.to_string()]).unwrap();
        fs::write(dir.join(RUSTFS_TLS_CERT), cert.pem()).unwrap();
        fs::write(dir.join(rustfs_config::RUSTFS_TLS_KEY), signing_key.serialize_pem()).unwrap();
    }

    fn register_test_reload(path: &str, initial: rustfs_tls_runtime::TlsMaterialSnapshot) -> TlsReloadTask {
        let prepared = PreparedServerTls::from_snapshot(&initial)
            .expect("prepare fixture")
            .expect("server material");
        let holder = prepared.tcp_acceptor().expect("fixture acceptor");
        spawn_reload_loop(
            path.into(),
            holder,
            initial,
            prepared,
            #[cfg(feature = "http3")]
            None,
        )
        .expect("register actual reloader")
        .expect("reload enabled")
    }

    async fn assert_owned_reload_stop(failed_read: bool, cancel_observer: bool, sharing: &str) {
        ensure_rustls_crypto_provider();
        temp_env::async_with_vars(
            [
                (ENV_TLS_RELOAD_ENABLE, Some("1")),
                (ENV_SERVER_MTLS_ENABLE, Some("0")),
                (ENV_TRUST_SYSTEM_CA, Some("0")),
                (ENV_TRUST_LEAF_CERT_AS_CA, Some("0")),
            ],
            async {
                let dir = TempDir::new().expect("owned TLS input");
                write_test_cert_pair(dir.path(), "owned-stop.example");
                let path = dir.path().to_str().expect("fixture path");
                let initial = load_tls_material(path).await.expect("initial real material");
                // Load the second owned input before any reload task or native pause exists.
                let mut registration_snapshot = Some(load_tls_material(path).await.expect("second real registration snapshot"));
                let task = register_test_reload(path, initial);
                let existing_peer = (sharing == "existing")
                    .then(|| register_test_reload(path, registration_snapshot.take().expect("preloaded existing registration")));
                let group = task.group.clone();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let control = Arc::new(ReloadReadTestControl {
                    entered: tokio_util::sync::CancellationToken::new(),
                    finished: tokio_util::sync::CancellationToken::new(),
                    release: Mutex::new(release_rx),
                    result: std::sync::atomic::AtomicU8::new(0),
                });
                *group.read_control.lock().unwrap_or_else(|e| e.into_inner()) = Some(control.clone());
                let entered = tokio::time::timeout(Duration::from_secs(5), control.entered.cancelled())
                    .await
                    .is_ok();
                let mid = (sharing == "mid-round")
                    .then(|| register_test_reload(path, registration_snapshot.take().expect("preloaded mid-round registration")));
                let input_changed =
                    !failed_read || fs::write(dir.path().join(RUSTFS_TLS_CERT), b"invalid certificate fixture").is_ok();
                let first = mid.as_ref().unwrap_or(&task);
                first.request_stop();
                let mut observer = Box::pin(first.wait_stopped());
                let pending = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(std::future::Future::poll(observer.as_mut(), cx).is_pending())
                })
                .await;
                let native_still_running = !control.finished.is_cancelled();
                let closing_rejection = if sharing.is_empty() {
                    let initial = registration_snapshot.take().expect("preloaded closing registration");
                    let prepared = PreparedServerTls::from_snapshot(&initial)
                        .expect("same prepared input")
                        .expect("server");
                    let holder = prepared.tcp_acceptor().expect("same acceptor");
                    Some(spawn_reload_loop(
                        path.into(),
                        holder,
                        initial,
                        prepared,
                        #[cfg(feature = "http3")]
                        None,
                    ))
                } else {
                    None
                };
                // Disconnecting this sender also releases native work during assertion unwinding.
                let first_receipt = if cancel_observer {
                    drop(observer);
                    drop(release_tx);
                    tokio::time::timeout(Duration::from_secs(5), first.wait_stopped()).await
                } else {
                    drop(release_tx);
                    tokio::time::timeout(Duration::from_secs(5), observer).await
                };
                let first_was_group_joined = first_receipt.as_ref().is_ok_and(|receipt| receipt.group_joined());
                let closing_was_rejected =
                    matches!(&closing_rejection, Some(Err(TlsMaterialError::ReloadStopping))) || !sharing.is_empty();
                if let Some(Ok(Some(unexpected))) = closing_rejection {
                    unexpected.request_stop();
                    let _ = tokio::time::timeout(Duration::from_secs(5), unexpected.wait_stopped()).await;
                }
                let remaining = if sharing == "mid-round" {
                    Some(&task)
                } else {
                    existing_peer.as_ref()
                };
                let later_receipt = if let Some(remaining) = remaining {
                    remaining.request_stop();
                    Some(tokio::time::timeout(Duration::from_secs(5), remaining.wait_stopped()).await)
                } else {
                    None
                };
                // Cleanup observes the same actual driver even when a bounded test observation failed.
                let final_join = group.driver.get().expect("same original driver").clone().await;
                // The original selected native closure is joined by the original group, not a noop barrier.
                let finished = control.finished.is_cancelled();
                let result = control.result.load(std::sync::atomic::Ordering::Acquire);
                let receipt = first_receipt.expect("original stopped registration drains");
                if let Some(receipt) = later_receipt {
                    assert!(receipt.expect("last group drains").group_joined());
                }
                assert!(final_join.is_ok(), "original group joined after all registrations stopped");
                assert!(entered, "selected original native read entered");
                assert!(pending && native_still_running, "stop cannot discard an entered native read");
                assert!(finished);
                assert!(input_changed, "actual parser failure input was installed");
                assert!(closing_was_rejected, "a closing old group explicitly rejects new registrations");
                assert_eq!(result, if failed_read { 2 } else { 1 }, "actual native result retained through stop");
                assert!(receipt.group_joined(), "earlier receipt observes the same eventual real group join");
                assert_eq!(
                    first_was_group_joined,
                    sharing.is_empty(),
                    "an independent live registration remains active"
                );
                if cancel_observer {
                    assert!(
                        group
                            .driver
                            .get()
                            .expect("same original driver")
                            .peek()
                            .is_some_and(Result::is_ok)
                    );
                }
                assert!(!RELOAD_GROUPS.lock().unwrap_or_else(|e| e.into_inner()).contains_key(path));
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn owned_reload_stop_waits_for_original_native_success() {
        assert_owned_reload_stop(false, false, "").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn owned_reload_stop_waits_for_original_native_error() {
        assert_owned_reload_stop(true, false, "").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn owned_reload_cancelled_observer_retains_original_native_owner() {
        assert_owned_reload_stop(false, true, "").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn owned_reload_mid_round_listener_waits_for_its_publication_round() {
        assert_owned_reload_stop(false, false, "mid-round").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn owned_reload_existing_shared_listener_waits_for_its_captured_round() {
        assert_owned_reload_stop(false, false, "existing").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn owned_reload_late_ready_keeps_original_http_deadline_failed() {
        ensure_rustls_crypto_provider();
        temp_env::async_with_vars(
            [
                (ENV_TLS_RELOAD_ENABLE, Some("1")),
                (ENV_SERVER_MTLS_ENABLE, Some("0")),
                (ENV_TRUST_SYSTEM_CA, Some("0")),
                (ENV_TRUST_LEAF_CERT_AS_CA, Some("0")),
            ],
            async {
                let dir = TempDir::new().expect("owned late observation input");
                write_test_cert_pair(dir.path(), "late-stop.example");
                let path = dir.path().to_str().expect("fixture path");
                let initial = load_tls_material(path).await.expect("initial real material");
                let task = register_test_reload(path, initial);
                let group = task.group.clone();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let control = Arc::new(ReloadReadTestControl {
                    entered: tokio_util::sync::CancellationToken::new(),
                    finished: tokio_util::sync::CancellationToken::new(),
                    release: Mutex::new(release_rx),
                    result: std::sync::atomic::AtomicU8::new(0),
                });
                *group.read_control.lock().unwrap_or_else(|e| e.into_inner()) = Some(control.clone());
                let entered = tokio::time::timeout(Duration::from_secs(5), control.entered.cancelled())
                    .await
                    .is_ok();
                task.request_stop();
                tokio::time::pause();
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                let mut outcome = super::super::http::test_retire_http12_scope(std::future::ready(()), deadline).await;
                let pending;
                let joined;
                {
                    let mut observer = Box::pin(super::super::http::retire_tls_reloader(&task, &mut outcome, deadline));
                    pending = std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(std::future::Future::poll(observer.as_mut(), cx).is_pending())
                    })
                    .await;
                    tokio::time::advance(Duration::from_secs(10)).await;
                    drop(release_tx);
                    joined = group.driver.get().expect("same actual driver").clone().await;
                    // The owned result is already ready before the original observer is polled again.
                    observer.await;
                }
                tokio::time::resume();
                assert!(entered && pending);
                assert!(joined.is_ok() && control.finished.is_cancelled());
                assert_eq!(control.result.load(std::sync::atomic::Ordering::Acquire), 1);
                assert!(
                    outcome.deadline_missed(),
                    "late Ready cannot erase the original absolute 10-second observation failure"
                );
                assert!(outcome.verified_scope_complete(), "cleanup genuinely joined the TLS group");
            },
        )
        .await;
    }

    async fn assert_reload_driver_failure(cancel: bool) {
        ensure_rustls_crypto_provider();
        temp_env::async_with_vars(
            [
                (ENV_TLS_RELOAD_ENABLE, Some("1")),
                (ENV_SERVER_MTLS_ENABLE, Some("0")),
                (ENV_TRUST_SYSTEM_CA, Some("0")),
                (ENV_TRUST_LEAF_CERT_AS_CA, Some("0")),
            ],
            async {
                let dir = TempDir::new().expect("owned driver fixture");
                write_test_cert_pair(dir.path(), "driver-stop.example");
                let path = dir.path().to_str().expect("path");
                let initial = load_tls_material(path).await.expect("initial material");
                let task = register_test_reload(path, initial);
                let group = task.group.clone();
                let entered = if cancel {
                    // This task has not been polled; no native read is detached by this error injection.
                    group.driver_abort.get().expect("actual original task").abort();
                    true
                } else {
                    group.panic_before_read.store(true, std::sync::atomic::Ordering::Release);
                    tokio::time::timeout(Duration::from_secs(5), group.failure_entered.cancelled())
                        .await
                        .is_ok()
                };
                task.request_stop();
                let receipt = tokio::time::timeout(Duration::from_secs(5), task.wait_stopped())
                    .await
                    .expect("actual failed driver joins");
                let error = receipt.join_error().expect("actual typed JoinError retained");
                assert!(entered, "actual driver reached the failure boundary");
                assert_eq!(error.is_cancelled(), cancel);
                assert_eq!(error.is_panic(), !cancel);
                assert!(!receipt.group_joined());
                let typed = TlsMaterialError::ReloadTaskFailed(error.clone());
                assert_eq!(
                    typed.to_string(),
                    if cancel {
                        "TLS reload group cancelled"
                    } else {
                        "TLS reload group panicked"
                    }
                );
                let source = std::error::Error::source(&typed).expect("actual join source");
                assert!(std::ptr::eq(
                    source.downcast_ref::<tokio::task::JoinError>().expect("typed source"),
                    error.as_ref()
                ));
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn owned_reload_original_driver_panic_remains_failure() {
        assert_reload_driver_failure(false).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn owned_reload_original_driver_cancel_remains_failure() {
        assert_reload_driver_failure(true).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn prepared_generation_shares_crypto_and_serves_sni_with_mtls() {
        ensure_rustls_crypto_provider();
        temp_env::async_with_vars([(ENV_SERVER_MTLS_ENABLE, Some("1"))], async {
            let dir = TempDir::new().expect("TLS directory");
            write_test_cert_pair(dir.path(), "default.local");
            let sni_dir = dir.path().join("sni.example");
            fs::create_dir(&sni_dir).expect("SNI directory");
            write_test_cert_pair(&sni_dir, "sni.example");
            let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
            ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let ca_key = rcgen::KeyPair::generate().expect("CA key");
            let ca = ca_params.self_signed(&ca_key).expect("CA certificate");
            fs::write(dir.path().join(RUSTFS_CLIENT_CA_CERT_FILENAME), ca.pem()).expect("client trust");
            let mut client_params = rcgen::CertificateParams::new(vec!["client.example".into()]).expect("client parameters");
            client_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
            let client_key = rcgen::KeyPair::generate().expect("client key");
            let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
            let client_cert = client_params.signed_by(&client_key, &issuer).expect("client identity");
            let snapshot = rustfs_tls_runtime::TlsMaterialSnapshot::load(&TlsSource::from_directory(dir.path()))
                .await
                .expect("validated TLS snapshot");
            let before = TLS_PREPARE_COUNTS.with(std::cell::Cell::get);
            let prepared = PreparedServerTls::from_snapshot(&snapshot)
                .expect("prepare generation")
                .expect("server TLS");
            let tcp = prepared.server_config(ServerProtocol::Tcp).expect("TCP config");
            let another_tcp = prepared
                .server_config(ServerProtocol::Tcp)
                .expect("additional listener config");
            assert!(Arc::ptr_eq(&tcp.cert_resolver, &another_tcp.cert_resolver));
            #[cfg(feature = "http3")]
            {
                let quic_tls = prepared.server_config(ServerProtocol::Http3).expect("QUIC TLS config");
                assert!(Arc::ptr_eq(&tcp.cert_resolver, &quic_tls.cert_resolver));
            }
            let after = TLS_PREPARE_COUNTS.with(std::cell::Cell::get);
            assert_eq!(
                (after.0 - before.0, after.1 - before.1),
                (1, 1),
                "client verifier and signing resolver must each be prepared once for all protocols and listeners"
            );

            let RuntimeServerTlsMaterial::MultiCert { cert_key_pairs } = snapshot.server.as_ref().expect("SNI material") else {
                panic!("multiple selectors expected");
            };
            let mut roots = rustls::RootCertStore::empty();
            for (certificates, _) in cert_key_pairs.values() {
                roots.add(certificates[0].clone()).expect("server trust");
            }
            for (name, selector) in [("default.local", "default"), ("sni.example", "sni.example")] {
                let expected = cert_key_pairs[selector].0[0].clone();
                let client_tls = rustls::ClientConfig::builder()
                    .with_root_certificates(roots.clone())
                    .with_client_auth_cert(
                        vec![client_cert.der().clone()],
                        PrivateKeyDer::try_from(client_key.serialize_der()).expect("client private key"),
                    )
                    .expect("mTLS client");
                let (server_io, client_io) = tokio::io::duplex(65536);
                let acceptor = prepared.tcp_acceptor().expect("acceptor").get();
                let connector = tokio_rustls::TlsConnector::from(Arc::new(client_tls.clone()));
                let (server, client) = tokio::join!(
                    acceptor.accept(server_io),
                    connector.connect(rustls::pki_types::ServerName::try_from(name.to_owned()).expect("server name"), client_io)
                );
                let server = server.expect("authenticated server handshake");
                let client = client.expect("authenticated client handshake");
                assert_eq!(client.get_ref().1.peer_certificates().expect("server identity")[0], expected);
                assert_eq!(server.get_ref().1.peer_certificates().expect("client identity")[0], *client_cert.der());

                #[cfg(feature = "http3")]
                {
                    let endpoint = quinn::Endpoint::server(
                        prepared.http3_config().expect("QUIC config"),
                        "127.0.0.1:0".parse().expect("address"),
                    )
                    .expect("QUIC server");
                    let mut client_tls = client_tls;
                    client_tls.alpn_protocols = vec![b"h3".to_vec()];
                    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(client_tls).expect("QUIC client TLS");
                    let mut client_endpoint =
                        quinn::Endpoint::client("127.0.0.1:0".parse().expect("address")).expect("QUIC client");
                    client_endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
                    let connection = client_endpoint
                        .connect(endpoint.local_addr().expect("server address"), name)
                        .expect("connect");
                    let (server, client) = tokio::time::timeout(Duration::from_secs(5), async {
                        tokio::join!(async { endpoint.accept().await.expect("incoming").await }, connection)
                    })
                    .await
                    .expect("QUIC handshake deadline");
                    let server = server.expect("QUIC mTLS server handshake");
                    let client = client.expect("QUIC mTLS client handshake");
                    let identity = client
                        .peer_identity()
                        .expect("QUIC server identity")
                        .downcast::<Vec<CertificateDer<'static>>>()
                        .expect("certificate identity");
                    assert_eq!(identity[0], expected);
                    let identity = server
                        .peer_identity()
                        .expect("QUIC client identity")
                        .downcast::<Vec<CertificateDer<'static>>>()
                        .expect("certificate identity");
                    assert_eq!(identity[0], *client_cert.der());
                    client.close(0u32.into(), b"done");
                    server.close(0u32.into(), b"done");
                    endpoint.close(0u32.into(), b"done");
                    client_endpoint.close(0u32.into(), b"done");
                }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn candidate_replacement_preserves_ca_components_and_default_names() {
        let dir = TempDir::new().expect("TLS directory");
        write_test_cert_pair(dir.path(), "s3.example.com");
        let ca_a = rcgen::generate_simple_self_signed(vec!["ca-a.example".into()]).expect("CA A");
        let ca_b = rcgen::generate_simple_self_signed(vec!["ca-b.example".into()]).expect("CA B");
        fs::write(dir.path().join(RUSTFS_CLIENT_CA_CERT_FILENAME), ca_a.cert.pem()).expect("primary client CA");
        fs::write(dir.path().join(RUSTFS_CA_CERT), ca_b.cert.pem()).expect("fallback CA");
        let source = TlsSource::from_directory(dir.path());
        let accepted = rustfs_tls_runtime::TlsMaterialSnapshot::load(&source)
            .await
            .expect("accepted generation");
        fs::remove_file(dir.path().join(RUSTFS_CLIENT_CA_CERT_FILENAME)).expect("remove primary client CA");
        let candidate = rustfs_tls_runtime::TlsMaterialSnapshot::load(&source)
            .await
            .expect("fallback generation parses");
        assert!(
            validate_candidate_replacement(&accepted, &candidate).is_err(),
            "a fallback must not replace accepted client trust"
        );

        fs::write(dir.path().join(RUSTFS_CLIENT_CA_CERT_FILENAME), ca_a.cert.pem()).expect("restore primary CA");
        write_test_cert_pair(dir.path(), "wrong.example.com");
        let candidate = rustfs_tls_runtime::TlsMaterialSnapshot::load(&source)
            .await
            .expect("valid but wrong SAN");
        assert!(
            validate_candidate_replacement(&accepted, &candidate).is_err(),
            "default SAN coverage must survive renewal"
        );

        write_test_cert_pair(dir.path(), "s3.example.com");
        let renewed = rustfs_tls_runtime::TlsMaterialSnapshot::load(&source)
            .await
            .expect("valid renewal");
        validate_candidate_replacement(&accepted, &renewed).expect("renewal preserving names and trust sources");
    }

    #[tokio::test]
    async fn candidate_default_names_cannot_move_into_another_sni_certificate() {
        let dir = TempDir::new().expect("TLS directory");
        let sni = dir.path().join("example.com");
        fs::create_dir(&sni).expect("SNI directory");
        write_test_cert_pair(dir.path(), "default.local");
        write_test_cert_pair(&sni, "example.com");
        let source = TlsSource::from_directory(dir.path());
        let accepted = rustfs_tls_runtime::TlsMaterialSnapshot::load(&source)
            .await
            .expect("accepted selectors");
        write_test_cert_pair(dir.path(), "wrong.local");
        let relocated = rcgen::generate_simple_self_signed(vec!["example.com".into(), "default.local".into()])
            .expect("certificate with relocated SAN");
        fs::write(sni.join(RUSTFS_TLS_CERT), relocated.cert.pem()).expect("SNI certificate");
        fs::write(sni.join(rustfs_config::RUSTFS_TLS_KEY), relocated.signing_key.serialize_pem()).expect("SNI key");
        let candidate = rustfs_tls_runtime::TlsMaterialSnapshot::load(&source)
            .await
            .expect("individually valid candidate");
        assert!(
            validate_candidate_replacement(&accepted, &candidate).is_err(),
            "another selector's SAN does not cover the default resolver's hostname"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn snapshot_leaf_trust_is_encoded_in_canonical_der_order() {
        let dir = TempDir::new().expect("TLS directory");
        let a = rcgen::generate_simple_self_signed(vec!["a.example".into()]).expect("certificate A");
        let b = rcgen::generate_simple_self_signed(vec!["b.example".into()]).expect("certificate B");
        let (first, second) = if a.cert.der().as_ref() > b.cert.der().as_ref() {
            (&a, &b)
        } else {
            (&b, &a)
        };
        fs::write(dir.path().join(RUSTFS_TLS_CERT), format!("{}{}", first.cert.pem(), second.cert.pem()))
            .expect("descending DER chain");
        fs::write(dir.path().join(rustfs_config::RUSTFS_TLS_KEY), first.signing_key.serialize_pem()).expect("leaf key");
        let mut snapshot = rustfs_tls_runtime::TlsMaterialSnapshot::load(&TlsSource::from_directory(dir.path()))
            .await
            .expect("validated material");
        temp_env::async_with_vars(
            [
                (ENV_TRUST_LEAF_CERT_AS_CA, Some("1")),
                (ENV_TRUST_SYSTEM_CA, Some("0")),
                (ENV_MTLS_CLIENT_CERT, None),
                (ENV_MTLS_CLIENT_KEY, None),
            ],
            async {
                enrich_outbound(&mut snapshot).await.expect("derive leaf trust");
                let mut reader = Cursor::new(&snapshot.outbound.root_ca_pem);
                let certificates = CertificateDer::pem_reader_iter(&mut reader)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("derived certificate PEM");
                assert_eq!(
                    certificates,
                    vec![second.cert.der().clone(), first.cert.der().clone()],
                    "leaf trust must not depend on source enumeration order"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn client_identity_override_rejects_mismatch_and_missing_files() {
        let dir = TempDir::new().expect("identity directory");
        let a = rcgen::generate_simple_self_signed(vec!["client-a.example".into()]).expect("identity A");
        let b = rcgen::generate_simple_self_signed(vec!["client-b.example".into()]).expect("identity B");
        let cert = dir.path().join(RUSTFS_CLIENT_CERT_FILENAME);
        let key = dir.path().join(RUSTFS_CLIENT_KEY_FILENAME);
        fs::write(&cert, a.cert.pem()).expect("client cert");
        fs::write(&key, b.signing_key.serialize_pem()).expect("mismatched key");
        assert!(
            load_mtls_identity_from_overridden_paths(cert.to_str(), key.to_str(), dir.path())
                .await
                .is_err()
        );
        fs::remove_file(&key).expect("remove override key");
        assert!(
            load_mtls_identity_from_overridden_paths(cert.to_str(), key.to_str(), dir.path())
                .await
                .is_err()
        );
        fs::write(&key, a.signing_key.serialize_pem()).expect("matching key");
        assert!(
            load_mtls_identity_from_overridden_paths(cert.to_str(), key.to_str(), dir.path())
                .await
                .expect("valid override")
                .is_some()
        );
    }

    enum StartupRotation {
        Outbound,
        RootsOnly,
        IdentityOnly,
        ServerOnly,
        MissingRoots,
        PartialRoots,
        MissingIdentity,
    }

    async fn assert_initial_outbound_reconciliation(rotation: StartupRotation) {
        ensure_rustls_crypto_provider();
        temp_env::async_with_vars(
            [
                (ENV_TLS_RELOAD_ENABLE, Some("1")),
                (ENV_SERVER_MTLS_ENABLE, Some("0")),
                (ENV_TRUST_LEAF_CERT_AS_CA, Some("0")),
                (ENV_TRUST_SYSTEM_CA, Some("0")),
                (ENV_MTLS_CLIENT_CERT, None),
                (ENV_MTLS_CLIENT_KEY, None),
                (ENV_RUSTFS_EXTRA_CA_CERT, None),
                (super::super::tls_publish::ENV_TLS_PUBLISH_DIR, None),
            ],
            async {
                let previous = startup_runtime_sources::current_outbound_tls_state().await;
                let previous_files = crate::startup_tls_material::replace_startup_material_files(None);
                let dir = TempDir::new().expect("TLS directory");
                write_test_cert_pair(dir.path(), "server.example");
                let identity_a = rcgen::generate_simple_self_signed(vec!["client-a.example".into()]).expect("identity A");
                fs::write(dir.path().join(RUSTFS_CA_CERT), identity_a.cert.pem()).expect("trust A");
                fs::write(dir.path().join(RUSTFS_CLIENT_CERT_FILENAME), identity_a.cert.pem()).expect("client certificate A");
                fs::write(dir.path().join(RUSTFS_CLIENT_KEY_FILENAME), identity_a.signing_key.serialize_pem())
                    .expect("client key A");
                if matches!(rotation, StartupRotation::PartialRoots) {
                    let public_ca =
                        rcgen::generate_simple_self_signed(vec!["public-ca.example".into()]).expect("distinct public CA");
                    fs::write(dir.path().join(rustfs_config::RUSTFS_PUBLIC_CERT), public_ca.cert.pem())
                        .expect("additional public trust");
                }
                let path = dir.path().to_str().expect("UTF8 path").to_string();
                let mut config = crate::config::Config::new("127.0.0.1:0", Vec::new());
                config.tls_path = Some(path.clone());
                crate::startup_tls_material::init_outbound_tls_material(&config)
                    .await
                    .expect("real startup publication");
                let startup = startup_runtime_sources::current_outbound_tls_state().await;

                write_test_cert_pair(dir.path(), "server.example");
                match rotation {
                    StartupRotation::Outbound | StartupRotation::RootsOnly | StartupRotation::IdentityOnly => {
                        let identity_b = rcgen::generate_simple_self_signed(vec!["client-b.example".into()]).expect("identity B");
                        if !matches!(rotation, StartupRotation::IdentityOnly) {
                            fs::write(dir.path().join(RUSTFS_CA_CERT), identity_b.cert.pem()).expect("trust B");
                        }
                        if !matches!(rotation, StartupRotation::RootsOnly) {
                            fs::write(dir.path().join(RUSTFS_CLIENT_CERT_FILENAME), identity_b.cert.pem())
                                .expect("client certificate B");
                            fs::write(dir.path().join(RUSTFS_CLIENT_KEY_FILENAME), identity_b.signing_key.serialize_pem())
                                .expect("client key B");
                        }
                    }
                    StartupRotation::ServerOnly => {}
                    StartupRotation::MissingRoots | StartupRotation::PartialRoots => {
                        fs::remove_file(dir.path().join(RUSTFS_CA_CERT)).expect("remove startup roots");
                    }
                    StartupRotation::MissingIdentity => {
                        fs::remove_file(dir.path().join(RUSTFS_CLIENT_CERT_FILENAME)).expect("remove startup client certificate");
                        fs::remove_file(dir.path().join(RUSTFS_CLIENT_KEY_FILENAME)).expect("remove startup client key");
                    }
                }
                let initial = load_tls_material(&path).await.expect("HTTP initialization snapshot");
                let incomplete = matches!(
                    rotation,
                    StartupRotation::MissingRoots | StartupRotation::PartialRoots | StartupRotation::MissingIdentity
                );
                let expected_roots = if incomplete {
                    startup.root_ca_pem.clone().expect("startup trust")
                } else {
                    initial.outbound.root_ca_pem.clone()
                };
                let expected_identity = if incomplete {
                    startup.mtls_identity.clone()
                } else {
                    initial.outbound.mtls_identity.clone()
                };
                let prepared = PreparedServerTls::from_snapshot(&initial).expect("config B").expect("TLS B");
                let holder = prepared.tcp_acceptor().expect("TLS B acceptor");
                let initial_acceptor = holder.get();
                let task = spawn_reload_loop(
                    path.clone(),
                    holder.clone(),
                    initial,
                    prepared,
                    #[cfg(feature = "http3")]
                    None,
                )
                .expect("register HTTP initialization snapshot")
                .expect("reload enabled");
                let group = RELOAD_GROUPS
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&path)
                    .and_then(Weak::upgrade)
                    .expect("registered reload group");
                notify_server_publication(&path);
                tokio::time::timeout(Duration::from_secs(10), group.pass_completed.notified())
                    .await
                    .expect("first reload pass completed");
                let first = startup_runtime_sources::current_outbound_tls_state().await;
                notify_server_publication(&path);
                tokio::time::timeout(Duration::from_secs(10), group.pass_completed.notified())
                    .await
                    .expect("second reload pass completed");
                let second = startup_runtime_sources::current_outbound_tls_state().await;

                task.abort();
                let _ = task.await;
                tokio::time::timeout(Duration::from_secs(10), async {
                    while RELOAD_GROUPS.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&path) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("reload group exits before restoring global state");
                startup_runtime_sources::publish_outbound_tls_state(
                    TlsGeneration(second.generation.0.saturating_add(1)),
                    &rustfs_tls_runtime::OutboundTlsMaterial {
                        root_ca_pem: previous.root_ca_pem.unwrap_or_default(),
                        mtls_identity: previous.mtls_identity,
                    },
                )
                .await;
                crate::startup_tls_material::replace_startup_material_files(previous_files);

                assert_eq!(first.root_ca_pem.as_deref(), Some(expected_roots.as_slice()));
                assert!(
                    first.mtls_identity == expected_identity,
                    "published identity must match the initialized listener snapshot"
                );
                let rotate_outbound = matches!(
                    rotation,
                    StartupRotation::Outbound | StartupRotation::RootsOnly | StartupRotation::IdentityOnly
                );
                let expected_generation = startup.generation.0.saturating_add(u64::from(rotate_outbound));
                assert_eq!(first.generation.0, expected_generation);
                assert_eq!(second.generation, first.generation, "unchanged material must not churn outbound clients");
                assert!(
                    rotate_outbound || Arc::ptr_eq(&holder.get(), &initial_acceptor),
                    "non-publications must retain the initial server configuration"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn tls_reload_reconciles_initial_outbound_after_startup_rotation() {
        for rotation in [
            StartupRotation::RootsOnly,
            StartupRotation::IdentityOnly,
            StartupRotation::Outbound,
        ] {
            assert_initial_outbound_reconciliation(rotation).await;
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn tls_reload_initial_server_rotation_preserves_matching_outbound_generation() {
        assert_initial_outbound_reconciliation(StartupRotation::ServerOnly).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn tls_reload_initial_missing_roots_retains_startup_material() {
        assert_initial_outbound_reconciliation(StartupRotation::MissingRoots).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn tls_reload_initial_partial_missing_roots_retains_startup_material() {
        assert_initial_outbound_reconciliation(StartupRotation::PartialRoots).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn tls_reload_initial_missing_identity_retains_startup_material() {
        assert_initial_outbound_reconciliation(StartupRotation::MissingIdentity).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn late_listener_joins_accepted_generation_when_source_is_invalid() {
        ensure_rustls_crypto_provider();
        temp_env::async_with_vars([(ENV_TLS_RELOAD_ENABLE, Some("1")), (ENV_SERVER_MTLS_ENABLE, Some("0"))], async {
            let dir = TempDir::new().expect("TLS directory");
            write_test_cert_pair(dir.path(), "server.example");
            let source = TlsSource::from_directory(dir.path());
            let a = rustfs_tls_runtime::TlsMaterialSnapshot::load(&source)
                .await
                .expect("generation A");
            let prepared_a = PreparedServerTls::from_snapshot(&a).expect("prepare A").expect("TLS A");
            let first = prepared_a.tcp_acceptor().expect("config A");
            let path = dir.path().to_str().expect("UTF8 path").to_string();
            let task_a = spawn_reload_loop(
                path.clone(),
                first.clone(),
                a,
                prepared_a,
                #[cfg(feature = "http3")]
                None,
            )
            .expect("register A")
            .expect("reload enabled");
            // No async yield between producing B and registration: the actor cannot accept B.
            write_test_cert_pair(dir.path(), "server.example");
            let b = rustfs_tls_runtime::TlsMaterialSnapshot::load_from_snapshot(&source.snapshot().expect("pin B"))
                .expect("generation B");
            let prepared_b = PreparedServerTls::from_snapshot(&b).expect("prepare B").expect("TLS B");
            let second = prepared_b.tcp_acceptor().expect("config B");
            assert!(!Arc::ptr_eq(&first.get(), &second.get()));
            fs::write(dir.path().join(RUSTFS_TLS_CERT), "invalid certificate").expect("break source");
            let task_b = spawn_reload_loop(
                path.clone(),
                second.clone(),
                b,
                prepared_b,
                #[cfg(feature = "http3")]
                None,
            )
            .expect("register B")
            .expect("reload enabled");
            assert!(
                Arc::ptr_eq(&first.get(), &second.get()),
                "late listener must use the group's accepted config"
            );
            task_a.abort();
            task_b.abort();
            let _ = task_a.await;
            let _ = task_b.await;
            tokio::time::timeout(Duration::from_secs(5), async {
                while RELOAD_GROUPS.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&path) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("group exits when all registrations are dropped");
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn oidc_extra_ca_cert_loads_configured_bundle() {
        let CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["extra-ca.example".to_string()]).expect("generate extra CA cert");
        let temp_file = tempfile::NamedTempFile::new().expect("create extra CA file");
        fs::write(temp_file.path(), cert.pem()).expect("write extra CA file");
        let path = temp_file.path().to_string_lossy().to_string();

        temp_env::async_with_vars([(ENV_RUSTFS_EXTRA_CA_CERT, Some(path.as_str()))], async {
            let extra_ca = load_configured_oidc_extra_ca_cert()
                .await
                .expect("OIDC extra CA should load")
                .expect("configured OIDC extra CA should be present");

            assert!(extra_ca.starts_with(cert.pem().as_bytes()));
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn oidc_extra_ca_cert_rejects_invalid_pem() {
        let temp_file = tempfile::NamedTempFile::new().expect("create invalid extra CA file");
        fs::write(temp_file.path(), b"not a certificate").expect("write invalid extra CA file");
        let path = temp_file.path().to_string_lossy().to_string();

        temp_env::async_with_vars([(ENV_RUSTFS_EXTRA_CA_CERT, Some(path.as_str()))], async {
            let err = load_configured_oidc_extra_ca_cert()
                .await
                .expect_err("invalid extra CA should fail");

            assert!(err.to_string().contains("no certificate found in extra CA bundle"));
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn oidc_extra_ca_cert_rejects_malformed_der_certificate() {
        let temp_file = tempfile::NamedTempFile::new().expect("create malformed extra CA file");
        fs::write(
            temp_file.path(),
            b"-----BEGIN CERTIFICATE-----\nbm90IGEgY2VydA==\n-----END CERTIFICATE-----\n",
        )
        .expect("write malformed extra CA file");
        let path = temp_file.path().to_string_lossy().to_string();

        temp_env::async_with_vars([(ENV_RUSTFS_EXTRA_CA_CERT, Some(path.as_str()))], async {
            let err = load_configured_oidc_extra_ca_cert()
                .await
                .expect_err("malformed DER in PEM framing should fail");

            assert!(err.to_string().contains("invalid extra CA bundle"));
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn load_tls_material_does_not_append_oidc_extra_ca_cert() {
        let temp_dir = TempDir::new().expect("create TLS material dir");
        write_test_cert_pair(temp_dir.path(), "server.example");
        let CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["extra-ca.example".to_string()]).expect("generate extra CA cert");
        let temp_file = tempfile::NamedTempFile::new().expect("create extra CA file");
        fs::write(temp_file.path(), cert.pem()).expect("write extra CA file");
        let path = temp_file.path().to_string_lossy().to_string();

        temp_env::async_with_vars([(ENV_RUSTFS_EXTRA_CA_CERT, Some(path.as_str()))], async {
            let snapshot = load_tls_material(temp_dir.path().to_str().expect("TLS material dir should be utf-8"))
                .await
                .expect("TLS material should load");

            assert!(snapshot.outbound.root_ca_pem.is_empty());
            assert!(snapshot.server.is_some());
        })
        .await;
    }

    #[tokio::test]
    async fn build_acceptor_accepts_root_single_cert_with_trailing_slash() {
        ensure_rustls_crypto_provider();
        let temp_dir = TempDir::new().unwrap();
        write_test_cert_pair(temp_dir.path(), "localhost");

        let snapshot = load_tls_material(&format!("{}/", temp_dir.path().display()))
            .await
            .expect("TLS material load should succeed");
        let acceptor = build_acceptor_from_loaded(snapshot.server, temp_dir.path())
            .await
            .expect("root single-cert TLS acceptor should build");

        assert!(acceptor.is_some());
    }

    #[tokio::test]
    async fn tls_handshake_deadline_sheds_a_peer_that_never_sends_client_hello() {
        ensure_rustls_crypto_provider();
        let temp_dir = TempDir::new().expect("temp dir should create");
        write_test_cert_pair(temp_dir.path(), "localhost");

        let snapshot = load_tls_material(temp_dir.path().to_str().expect("temp dir path should be utf-8"))
            .await
            .expect("TLS material load should succeed");
        let holder = build_acceptor_from_loaded(snapshot.server, temp_dir.path())
            .await
            .expect("TLS acceptor should build")
            .expect("TLS acceptor should be present");

        // The client half stays open and silent, so no ClientHello ever arrives.
        let (_client, server) = tokio::io::duplex(1024);
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            accept_tls_with_deadline(&holder.get(), server, Duration::from_millis(100)),
        )
        .await
        .expect("a stalled TLS handshake must be shed by the handshake deadline");

        assert!(
            matches!(outcome, Err(TlsAcceptFailure::Timeout)),
            "a silent peer must fail the handshake with a timeout"
        );
    }

    #[tokio::test]
    async fn build_acceptor_accepts_symlinked_root_single_cert_directory() {
        ensure_rustls_crypto_provider();
        let temp_dir = TempDir::new().unwrap();
        let real_dir = temp_dir.path().join("tls-real");
        fs::create_dir(&real_dir).unwrap();
        write_test_cert_pair(&real_dir, "localhost");

        let symlink_dir = temp_dir.path().join("tls-link");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real_dir, &symlink_dir).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&real_dir, &symlink_dir).unwrap();

        let snapshot = load_tls_material(symlink_dir.to_str().unwrap())
            .await
            .expect("TLS material load through symlink should succeed");
        let acceptor = build_acceptor_from_loaded(snapshot.server, &symlink_dir)
            .await
            .expect("TLS acceptor should build through symlink");

        assert!(acceptor.is_some());
    }

    #[tokio::test]
    async fn build_acceptor_rejects_missing_tls_directory() {
        let temp_dir = TempDir::new().unwrap();
        let missing_dir = temp_dir.path().join("missing");

        let err = load_tls_material(missing_dir.to_str().unwrap())
            .await
            .expect_err("missing TLS directory should fail");

        assert!(err.to_string().contains("TLS directory does not exist"));
    }

    #[tokio::test]
    async fn build_acceptor_rejects_tls_path_that_is_not_directory() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("tls-file");
        fs::write(&file_path, "not-a-directory").unwrap();

        let err = load_tls_material(file_path.to_str().unwrap())
            .await
            .expect_err("regular file TLS path should fail");

        assert!(err.to_string().contains("TLS path is not a directory"));
    }

    #[tokio::test]
    async fn build_acceptor_returns_none_for_empty_tls_directory() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot = load_tls_material(temp_dir.path().to_str().unwrap())
            .await
            .expect("TLS material load for empty dir should succeed");

        let acceptor = build_acceptor_from_loaded(snapshot.server, temp_dir.path())
            .await
            .expect("empty TLS directory should be treated as no acceptor");
        assert!(acceptor.is_none());
    }

    #[tokio::test]
    async fn build_acceptor_still_supports_multi_cert_directories() {
        ensure_rustls_crypto_provider();
        let temp_dir = TempDir::new().unwrap();
        let domain_dir = temp_dir.path().join("example.com");
        fs::create_dir(&domain_dir).unwrap();
        write_test_cert_pair(&domain_dir, "example.com");

        let snapshot = load_tls_material(temp_dir.path().to_str().unwrap())
            .await
            .expect("TLS material load for multi-cert dir should succeed");
        let acceptor = build_acceptor_from_loaded(snapshot.server, temp_dir.path())
            .await
            .expect("multi-cert TLS acceptor should build");

        assert!(acceptor.is_some());
    }

    #[tokio::test]
    async fn build_acceptor_prefers_multi_cert_when_root_and_domain_pairs_both_exist() {
        ensure_rustls_crypto_provider();
        let temp_dir = TempDir::new().unwrap();
        write_test_cert_pair(temp_dir.path(), "default.local");

        let domain_dir = temp_dir.path().join("example.com");
        fs::create_dir(&domain_dir).unwrap();
        write_test_cert_pair(&domain_dir, "example.com");

        let snapshot = load_tls_material(temp_dir.path().to_str().unwrap())
            .await
            .expect("TLS material load for mixed root/domain layout should succeed");
        let acceptor = build_acceptor_from_loaded(snapshot.server, temp_dir.path())
            .await
            .expect("multi-cert TLS acceptor should still build when root pair also exists");

        assert!(acceptor.is_some());
    }

    #[cfg(feature = "http3")]
    #[test]
    fn http3_server_config_uses_h3_and_disables_early_data() {
        ensure_rustls_crypto_provider();

        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("certificate should generate");
        let config = PreparedServerTls::from_material(
            Some(&RuntimeServerTlsMaterial::SingleCert {
                certs: vec![cert.cert.der().clone()],
                key: rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der())
                    .expect("private key should convert"),
            }),
            None,
        )
        .expect("prepare TLS")
        .expect("server certificate")
        .server_config(ServerProtocol::Http3)
        .expect("HTTP/3 TLS config should build");

        assert_eq!(config.alpn_protocols, vec![b"h3".to_vec()]);
        assert_eq!(config.max_early_data_size, 0);
    }
    #[cfg(feature = "http3")]
    #[tokio::test]
    #[serial_test::serial]
    async fn http3_certificate_reload_updates_handshakes_and_retains_valid_config() {
        use crate::server::http3::tests::{client_endpoint, connect};
        let dir = TempDir::new().expect("TLS directory");
        write_test_cert_pair(dir.path(), "localhost");
        let snapshot = load_tls_material(dir.path().to_str().expect("TLS path"))
            .await
            .expect("load first cert");
        let holder = build_acceptor_from_loaded(
            load_tls_material(dir.path().to_str().expect("TLS path"))
                .await
                .expect("load TCP cert")
                .server,
            dir.path(),
        )
        .await
        .expect("TCP config")
        .expect("TCP holder");
        let config = build_http3_server_config_from_loaded(snapshot.server.as_ref(), dir.path())
            .await
            .expect("QUIC config")
            .expect("server cert");
        let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().expect("address")).expect("QUIC endpoint");
        let first_cert = match snapshot.server.expect("cert") {
            RuntimeServerTlsMaterial::SingleCert { certs, .. } => certs[0].clone(),
            _ => panic!("single certificate expected"),
        };
        write_test_cert_pair(dir.path(), "localhost");
        let snapshot = load_tls_material(dir.path().to_str().expect("TLS path"))
            .await
            .expect("load rotated cert");
        let second_cert = match snapshot.server.as_ref().expect("cert") {
            RuntimeServerTlsMaterial::SingleCert { certs, .. } => certs[0].clone(),
            _ => panic!("single certificate expected"),
        };
        assert_ne!(first_cert, second_cert);
        let old_tcp = holder.get();
        assert!(
            reload_acceptors(snapshot.server, dir.path(), &holder, Some(&endpoint))
                .await
                .expect("reload both listeners")
        );
        assert!(!Arc::ptr_eq(&old_tcp, &holder.get()), "TCP acceptor must also rotate");
        let valid_tcp = holder.get();
        assert!(
            !reload_acceptors(None, dir.path(), &holder, Some(&endpoint))
                .await
                .expect("empty snapshot")
        );
        assert!(Arc::ptr_eq(&valid_tcp, &holder.get()), "failed reload must retain the valid TCP acceptor");
        let client = client_endpoint(vec![second_cert.clone()]).expect("trust rotated cert");
        let addr = endpoint.local_addr().expect("server address");
        let handshake = tokio::spawn(async move { connect(&client, addr).await });
        let incoming = endpoint.accept().await.expect("new connection");
        let server = incoming.await.expect("rotated TLS handshake");
        let (client, _, driver) = handshake.await.expect("client task").expect("client trusts new certificate");
        let identity = client
            .peer_identity()
            .expect("peer certs")
            .downcast::<Vec<CertificateDer<'static>>>()
            .expect("rustls identity");
        assert_eq!(
            identity[0], second_cert,
            "QUIC must serve the rotated certificate even after a rejected reload"
        );
        client.close(0u32.into(), b"test complete");
        server.close(0u32.into(), b"test complete");
        driver.abort();
        endpoint.close(0u32.into(), b"test complete");
    }
}
