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

//! Unified TLS Material Snapshot
//!
//! Provides a single loading point for all TLS materials, eliminating duplicate
//! directory scanning and PEM parsing between outbound and inbound paths.
//!
//! Usage:
//! 1. Call `TlsMaterialSnapshot::load(tls_path)` once at startup.
//! 2. Call `snapshot.apply_outbound()` to set global root CAs and mTLS identity.
//! 3. TLS acceptor construction is handled internally during server startup.

use rustfs_common::{MtlsIdentityPem, get_global_outbound_tls_generation};
use rustfs_config::{
    DEFAULT_SERVER_MTLS_ENABLE, DEFAULT_TLS_KEYLOG, DEFAULT_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_INTERVAL,
    DEFAULT_TRUST_LEAF_CERT_AS_CA, DEFAULT_TRUST_SYSTEM_CA, ENV_MTLS_CLIENT_CERT, ENV_MTLS_CLIENT_KEY, ENV_SERVER_MTLS_ENABLE,
    ENV_TLS_KEYLOG, ENV_TLS_RELOAD_ENABLE, ENV_TLS_RELOAD_INTERVAL, ENV_TRUST_LEAF_CERT_AS_CA, ENV_TRUST_SYSTEM_CA,
    RUSTFS_CA_CERT, RUSTFS_CLIENT_CA_CERT_FILENAME, RUSTFS_CLIENT_CERT_FILENAME, RUSTFS_CLIENT_KEY_FILENAME, RUSTFS_PUBLIC_CERT,
    RUSTFS_TLS_CERT,
};
use rustfs_tls_runtime::{
    OutboundTlsMaterial as RuntimeOutboundTlsMaterial, ServerTlsMaterial as RuntimeServerTlsMaterial, TlsGeneration, TlsSource,
    WebPkiClientVerifierOptions, build_webpki_client_verifier, create_multi_cert_resolver, publish_global_outbound_tls_state,
    record_tls_generation, record_tls_publication_fail, record_tls_reload_result, record_tls_reload_skipped,
};
use rustfs_utils::{get_env_bool, get_env_opt_str};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

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

/// Outbound TLS material for client connections (inter-node RPC).
#[derive(Debug, Clone)]
pub struct OutboundTlsMaterial {
    /// Concatenated PEM-encoded root CA certificates.
    pub root_ca_pem: Vec<u8>,
    /// Optional mTLS client identity.
    pub mtls_identity: Option<MtlsIdentityPem>,
}

/// Complete TLS material snapshot loaded once at startup.
#[derive(Debug)]
pub struct TlsMaterialSnapshot {
    /// Material for outbound client connections.
    pub outbound: OutboundTlsMaterial,
}

impl TlsMaterialSnapshot {
    /// Load all TLS materials from the given directory.
    ///
    /// This is the single entry point that replaces both the old
    /// `cert.rs::init_cert()` and `http.rs::setup_tls_acceptor()` loading logic.
    pub async fn load(tls_path: &str) -> Result<Self, TlsMaterialError> {
        if tls_path.is_empty() {
            info!("No TLS path configured; skipping TLS material loading");
            return Ok(Self::empty());
        }

        let tls_dir = PathBuf::from(tls_path);

        // Load outbound material (root CAs + mTLS identity)
        let outbound = load_outbound_material(&tls_dir).await?;

        Ok(Self { outbound })
    }

    /// Apply outbound material to global state (root CAs, mTLS identity).
    pub async fn apply_outbound(&self) {
        let next_generation = TlsGeneration(get_global_outbound_tls_generation().saturating_add(1));
        self.apply_outbound_with_generation(next_generation).await;
    }

    pub async fn apply_outbound_with_generation(&self, generation: TlsGeneration) {
        let outbound = RuntimeOutboundTlsMaterial {
            root_ca_pem: self.outbound.root_ca_pem.clone(),
            mtls_identity: self.outbound.mtls_identity.clone(),
        };
        publish_global_outbound_tls_state(generation, &outbound).await;
        record_tls_generation("rustfs_server_reload_loop", generation.0);
        if !self.outbound.root_ca_pem.is_empty() {
            info!("Configured custom root certificates for inter-node communication");
        }
    }

    /// Build a `TlsAcceptorHolder` from the loaded snapshot.
    ///
    /// This is the single place that constructs the server `ServerConfig`,
    /// handling both multi-cert (SNI resolver) and single-cert fallback.
    /// Returns `None` if no TLS certificates are available.
    pub(crate) async fn build_tls_acceptor(&self, tls_path: &str) -> Result<Option<Arc<TlsAcceptorHolder>>, TlsMaterialError> {
        if tls_path.is_empty() {
            return Ok(None);
        }

        let tls_source = TlsSource::from_directory(tls_path);
        let tls_dir = tls_source.validate_directory().map_err(map_runtime_tls_error)?.to_path_buf();
        let runtime_snapshot = rustfs_tls_runtime::TlsMaterialSnapshot::load(&tls_source)
            .await
            .map_err(map_runtime_tls_error)?;

        let mtls_verifier = build_webpki_client_verifier(
            WebPkiClientVerifierOptions::builder(&tls_dir, RUSTFS_CLIENT_CA_CERT_FILENAME, RUSTFS_CA_CERT)
                .enabled(get_env_bool(ENV_SERVER_MTLS_ENABLE, DEFAULT_SERVER_MTLS_ENABLE))
                .build(),
        )
        .map_err(|e| TlsMaterialError::Io(format!("build mTLS verifier: {e}")))?;

        match runtime_snapshot.server {
            Some(RuntimeServerTlsMaterial::SingleCert { certs, key }) => {
                let config = build_server_config(ServerCertSource::SingleCert { certs, key }, mtls_verifier)?;
                info!("Created TLS acceptor with root single certificate");
                let acceptor = Arc::new(TlsAcceptor::from(Arc::new(config)));
                Ok(Some(Arc::new(TlsAcceptorHolder::new(acceptor))))
            }
            Some(RuntimeServerTlsMaterial::MultiCert { cert_key_pairs }) => {
                let resolver = create_multi_cert_resolver(cert_key_pairs)
                    .map_err(|e| TlsMaterialError::Parse(format!("build multi-cert resolver: {e}")))?;
                let config = build_server_config(ServerCertSource::Resolver(Arc::new(resolver)), mtls_verifier)?;
                info!("Created TLS acceptor with SNI resolver");
                let acceptor = Arc::new(TlsAcceptor::from(Arc::new(config)));
                Ok(Some(Arc::new(TlsAcceptorHolder::new(acceptor))))
            }
            None => Ok(None),
        }
    }

    fn empty() -> Self {
        Self {
            outbound: OutboundTlsMaterial {
                root_ca_pem: Vec::new(),
                mtls_identity: None,
            },
        }
    }
}

fn map_runtime_tls_error(err: rustfs_tls_runtime::TlsRuntimeError) -> TlsMaterialError {
    match &err {
        rustfs_tls_runtime::TlsRuntimeError::Material(msg) | rustfs_tls_runtime::TlsRuntimeError::Publication(msg) => {
            TlsMaterialError::Parse(msg.clone())
        }
        _ => TlsMaterialError::Io(err.to_string()),
    }
}

// ── Server Config Construction ──

/// Certificate source for building a `ServerConfig`.
enum ServerCertSource {
    /// Pre-built SNI resolver from multi-cert directory.
    Resolver(Arc<dyn rustls::server::ResolvesServerCert + Send + Sync>),
    /// Single certificate/key pair.
    SingleCert {
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    },
}

/// Build a `ServerConfig` with standardized ALPN, session cache, and key log settings.
///
/// This is the single place for `ServerConfig` construction, used by both
/// initial startup and hot-reload.
fn build_server_config(
    cert_source: ServerCertSource,
    mtls_verifier: Option<Arc<dyn rustls::server::danger::ClientCertVerifier>>,
) -> Result<rustls::ServerConfig, TlsMaterialError> {
    let mut config = match cert_source {
        ServerCertSource::Resolver(resolver) => {
            if let Some(verifier) = mtls_verifier {
                rustls::ServerConfig::builder()
                    .with_client_cert_verifier(verifier)
                    .with_cert_resolver(resolver)
            } else {
                rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_cert_resolver(resolver)
            }
        }
        ServerCertSource::SingleCert { certs, key } => {
            if let Some(verifier) = mtls_verifier {
                rustls::ServerConfig::builder()
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(certs, key)
                    .map_err(|e| TlsMaterialError::Io(format!("configure single cert with mTLS: {e}")))?
            } else {
                rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(certs, key)
                    .map_err(|e| TlsMaterialError::Io(format!("configure single cert: {e}")))?
            }
        }
    };

    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];
    config.session_storage = rustls::server::ServerSessionMemoryCache::new(10000);

    if tls_key_log() {
        config.key_log = Arc::new(rustls::KeyLogFile::new());
    }

    Ok(config)
}

/// Checks if TLS key logging is enabled.
///
/// # Returns
/// * A boolean indicating whether TLS key logging is enabled based on the `RUSTFS_TLS_KEYLOG` environment variable.
///
fn tls_key_log() -> bool {
    get_env_bool(ENV_TLS_KEYLOG, DEFAULT_TLS_KEYLOG)
}

// ── Outbound Material Loading ──

/// Load root CA certificates and mTLS identity for outbound connections.
async fn load_outbound_material(tls_dir: &Path) -> Result<OutboundTlsMaterial, TlsMaterialError> {
    let mut root_ca_pem = Vec::new();

    // 1. Optional: load leaf certs as root CAs
    if get_env_bool(ENV_TRUST_LEAF_CERT_AS_CA, DEFAULT_TRUST_LEAF_CERT_AS_CA)
        && load_cert_file_by_name(tls_dir, RUSTFS_TLS_CERT, &mut root_ca_pem).await
    {
        info!("Loaded leaf certificate(s) as root CA as per RUSTFS_TRUST_LEAF_CERT_AS_CA");
    }

    // 2. Load public.crt and ca.crt
    load_cert_file(&tls_dir.join(RUSTFS_PUBLIC_CERT), &mut root_ca_pem, "CA certificate").await;
    load_cert_file(&tls_dir.join(RUSTFS_CA_CERT), &mut root_ca_pem, "CA certificate").await;

    // 3. Optional: load system root CAs
    if get_env_bool(ENV_TRUST_SYSTEM_CA, DEFAULT_TRUST_SYSTEM_CA) {
        let mut system_loaded = false;
        for path in SYSTEM_CA_PATHS {
            if load_cert_file(Path::new(path), &mut root_ca_pem, "system root certificates").await {
                system_loaded = true;
                info!("Loaded system root certificates from {}", path);
                break;
            }
        }
        if !system_loaded {
            debug!("Could not find system root certificates in common locations.");
        }
    } else {
        info!("Loading system root certificates disabled via RUSTFS_TRUST_SYSTEM_CA");
    }

    // 4. Load optional mTLS identity
    let mtls_identity = load_mtls_identity(tls_dir).await?;

    Ok(OutboundTlsMaterial {
        root_ca_pem,
        mtls_identity,
    })
}

/// Load mTLS client identity from the TLS directory.
async fn load_mtls_identity(tls_dir: &Path) -> Result<Option<MtlsIdentityPem>, TlsMaterialError> {
    let client_cert_path = match get_env_opt_str(ENV_MTLS_CLIENT_CERT) {
        Some(p) => PathBuf::from(p),
        None => tls_dir.join(RUSTFS_CLIENT_CERT_FILENAME),
    };

    let client_key_path = match get_env_opt_str(ENV_MTLS_CLIENT_KEY) {
        Some(p) => PathBuf::from(p),
        None => tls_dir.join(RUSTFS_CLIENT_KEY_FILENAME),
    };

    if !client_cert_path.exists() || !client_key_path.exists() {
        info!(
            "mTLS client identity not configured (missing {:?} and/or {:?}); proceeding with server-only TLS",
            client_cert_path, client_key_path
        );
        return Ok(None);
    }

    let cert_pem = tokio::fs::read(&client_cert_path)
        .await
        .map_err(|e| TlsMaterialError::Io(format!("read client cert {client_cert_path:?}: {e}")))?;
    let key_pem = tokio::fs::read(&client_key_path)
        .await
        .map_err(|e| TlsMaterialError::Io(format!("read client key {client_key_path:?}: {e}")))?;

    // Validate parse-ability
    let mut reader = std::io::Cursor::new(&cert_pem);
    if CertificateDer::pem_reader_iter(&mut reader).next().is_none() {
        return Err(TlsMaterialError::Parse("no valid certificate in client cert PEM".into()));
    }
    let mut reader = std::io::Cursor::new(&key_pem);
    PrivateKeyDer::from_pem_reader(&mut reader).map_err(|e| TlsMaterialError::Parse(format!("invalid client key PEM: {e}")))?;

    info!("Loaded mTLS client identity cert={:?} key={:?}", client_cert_path, client_key_path);
    Ok(Some(MtlsIdentityPem { cert_pem, key_pem }))
}

/// Load a single certificate file and append PEM data.
/// Returns true if the file was successfully loaded.
async fn load_cert_file(path: &Path, pem_data: &mut Vec<u8>, desc: &str) -> bool {
    if tokio::fs::metadata(path).await.is_err() {
        debug!("{} file not found at {:?}", desc, path);
        return false;
    }
    match tokio::fs::read(path).await {
        Ok(data) => {
            pem_data.extend_from_slice(&data);
            pem_data.push(b'\n');
            info!("Loaded {} from {:?}", desc, path);
            true
        }
        Err(e) => {
            debug!("Failed to read {} from {:?}: {}", desc, path, e);
            false
        }
    }
}

/// Search for and load certificate files matching `cert_name` in the directory
/// and one level of subdirectories.
/// Returns `true` if at least one matching file was loaded.
async fn load_cert_file_by_name(dir: &Path, cert_name: &str, pem_data: &mut Vec<u8>) -> bool {
    let Ok(mut rd) = tokio::fs::read_dir(dir).await else {
        debug!("Certificate directory not found: {}", dir.display());
        return false;
    };

    let mut loaded = false;
    while let Ok(Some(entry)) = rd.next_entry().await {
        let Ok(ft) = entry.file_type().await else { continue };

        if ft.is_file() {
            let fname = entry.file_name().to_string_lossy().to_string();
            if fname == cert_name && load_cert_file(&entry.path(), pem_data, "certificate").await {
                loaded = true;
            }
        } else if ft.is_dir() {
            // Only check direct subdirectories (one level deep)
            if let Ok(mut sub_rd) = tokio::fs::read_dir(&entry.path()).await {
                while let Ok(Some(sub_entry)) = sub_rd.next_entry().await {
                    if let Ok(sub_ft) = sub_entry.file_type().await
                        && sub_ft.is_file()
                    {
                        let fname = sub_entry.file_name().to_string_lossy().to_string();
                        if fname == cert_name && load_cert_file(&sub_entry.path(), pem_data, "certificate").await {
                            loaded = true;
                        }
                    }
                }
            }
        }
    }
    loaded
}

/// Errors that can occur during TLS material loading.
#[derive(Debug)]
pub enum TlsMaterialError {
    /// I/O error (file read, directory access).
    Io(String),
    /// PEM parsing error.
    Parse(String),
}

impl std::fmt::Display for TlsMaterialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsMaterialError::Io(msg) => write!(f, "TLS material I/O error: {msg}"),
            TlsMaterialError::Parse(msg) => write!(f, "TLS material parse error: {msg}"),
        }
    }
}

impl std::error::Error for TlsMaterialError {}

// ── TLS Handshake Error Classification ──

/// Structured classification of TLS handshake failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TlsHandshakeFailureKind {
    UnexpectedEof,
    ProtocolVersion,
    Certificate,
    Alert,
    Unknown,
}

impl TlsHandshakeFailureKind {
    /// Classify a TLS accept error into a structured failure kind.
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

    /// Metric label string for Prometheus.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::UnexpectedEof => "UNEXPECTED_EOF",
            Self::ProtocolVersion => "PROTOCOL_VERSION",
            Self::Certificate => "CERTIFICATE",
            Self::Alert => "ALERT",
            Self::Unknown => "UNKNOWN",
        }
    }
}

// ── TLS Acceptor Holder (for hot reload) ──

/// Holds the current TLS acceptor and supports atomic swap for certificate rotation.
///
/// Uses `RwLock` so that multiple readers (per-connection `get()` calls)
/// do not block each other. The write lock is held only briefly during swap.
pub(crate) struct TlsAcceptorHolder {
    current: RwLock<Arc<TlsAcceptor>>,
}

impl TlsAcceptorHolder {
    pub(crate) fn new(acceptor: Arc<TlsAcceptor>) -> Self {
        Self {
            current: RwLock::new(acceptor),
        }
    }

    /// Get the current TLS acceptor for handling a new connection.
    #[inline]
    pub(crate) fn get(&self) -> Arc<TlsAcceptor> {
        match self.current.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Atomically replace the TLS acceptor with a new one.
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

/// Spawn a background task that periodically checks for TLS certificate changes.
pub(crate) fn spawn_reload_loop(tls_path: String, holder: Arc<TlsAcceptorHolder>) {
    let enabled = get_env_bool(ENV_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_ENABLE);
    if !enabled {
        debug!("TLS certificate hot reload is disabled (set {}=1 to enable)", ENV_TLS_RELOAD_ENABLE);
        return;
    }

    let interval_secs = rustfs_utils::get_env_u64(ENV_TLS_RELOAD_INTERVAL, DEFAULT_TLS_RELOAD_INTERVAL).max(5);

    info!("TLS certificate hot reload enabled, checking every {}s", interval_secs);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;

            match TlsMaterialSnapshot::load(&tls_path).await {
                Ok(snapshot) => {
                    // Derive generation from the global counter on each tick so it
                    // stays monotonic even if other code paths publish outbound TLS.
                    let generation = get_global_outbound_tls_generation().saturating_add(1);
                    snapshot.apply_outbound_with_generation(TlsGeneration(generation)).await;

                    match snapshot.build_tls_acceptor(&tls_path).await {
                        Ok(Some(new_holder)) => {
                            info!("TLS certificates reloaded successfully");
                            holder.swap(&new_holder);
                            record_tls_reload_result("rustfs_server_reload_loop", "ok", None, Some(generation));
                        }
                        Ok(None) => {
                            record_tls_reload_skipped("rustfs_server_reload_loop", "no_acceptor");
                            warn!("TLS reload returned no acceptor despite configured TLS path; keeping previous acceptor")
                        }
                        Err(e) => {
                            record_tls_publication_fail("rustfs_server_reload_loop");
                            warn!("TLS certificate reload failed (will retry): {}", e)
                        }
                    }
                }
                Err(e) => {
                    record_tls_publication_fail("rustfs_server_reload_loop");
                    warn!("TLS material reload failed (will retry): {}", e);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::CertifiedKey;
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

    #[tokio::test]
    async fn build_tls_acceptor_accepts_root_single_cert_with_trailing_slash() {
        ensure_rustls_crypto_provider();
        let temp_dir = TempDir::new().unwrap();
        write_test_cert_pair(temp_dir.path(), "localhost");

        let snapshot = TlsMaterialSnapshot::load(&format!("{}/", temp_dir.path().display()))
            .await
            .expect("TLS material load should succeed");
        let acceptor = snapshot
            .build_tls_acceptor(&format!("{}/", temp_dir.path().display()))
            .await
            .expect("root single-cert TLS acceptor should build");

        assert!(acceptor.is_some());
    }

    #[tokio::test]
    async fn build_tls_acceptor_accepts_symlinked_root_single_cert_directory() {
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

        let snapshot = TlsMaterialSnapshot::load(symlink_dir.to_str().unwrap())
            .await
            .expect("TLS material load through symlink should succeed");
        let acceptor = snapshot
            .build_tls_acceptor(symlink_dir.to_str().unwrap())
            .await
            .expect("TLS acceptor should build through symlink");

        assert!(acceptor.is_some());
    }

    #[tokio::test]
    async fn build_tls_acceptor_rejects_missing_tls_directory() {
        let temp_dir = TempDir::new().unwrap();
        let missing_dir = temp_dir.path().join("missing");
        let snapshot = TlsMaterialSnapshot::empty();

        let err = match snapshot.build_tls_acceptor(missing_dir.to_str().unwrap()).await {
            Ok(_) => panic!("missing TLS directory should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("TLS directory does not exist"));
    }

    #[tokio::test]
    async fn build_tls_acceptor_rejects_tls_path_that_is_not_directory() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("tls-file");
        fs::write(&file_path, "not-a-directory").unwrap();
        let snapshot = TlsMaterialSnapshot::empty();

        let err = match snapshot.build_tls_acceptor(file_path.to_str().unwrap()).await {
            Ok(_) => panic!("regular file TLS path should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("TLS path is not a directory"));
    }

    #[tokio::test]
    async fn build_tls_acceptor_returns_none_for_empty_tls_directory() {
        let temp_dir = TempDir::new().unwrap();
        let snapshot = TlsMaterialSnapshot::load(temp_dir.path().to_str().unwrap())
            .await
            .expect("outbound TLS load for empty dir should succeed");

        let acceptor = snapshot
            .build_tls_acceptor(temp_dir.path().to_str().unwrap())
            .await
            .expect("empty TLS directory should be treated as no acceptor");
        assert!(acceptor.is_none());
    }

    #[tokio::test]
    async fn build_tls_acceptor_still_supports_multi_cert_directories() {
        ensure_rustls_crypto_provider();
        let temp_dir = TempDir::new().unwrap();
        let domain_dir = temp_dir.path().join("example.com");
        fs::create_dir(&domain_dir).unwrap();
        write_test_cert_pair(&domain_dir, "example.com");

        let snapshot = TlsMaterialSnapshot::load(temp_dir.path().to_str().unwrap())
            .await
            .expect("TLS material load for multi-cert dir should succeed");
        let acceptor = snapshot
            .build_tls_acceptor(temp_dir.path().to_str().unwrap())
            .await
            .expect("multi-cert TLS acceptor should build");

        assert!(acceptor.is_some());
    }

    #[tokio::test]
    async fn build_tls_acceptor_prefers_multi_cert_when_root_and_domain_pairs_both_exist() {
        ensure_rustls_crypto_provider();
        let temp_dir = TempDir::new().unwrap();
        write_test_cert_pair(temp_dir.path(), "default.local");

        let domain_dir = temp_dir.path().join("example.com");
        fs::create_dir(&domain_dir).unwrap();
        write_test_cert_pair(&domain_dir, "example.com");

        let snapshot = TlsMaterialSnapshot::load(temp_dir.path().to_str().unwrap())
            .await
            .expect("TLS material load for mixed root/domain layout should succeed");
        let acceptor = snapshot
            .build_tls_acceptor(temp_dir.path().to_str().unwrap())
            .await
            .expect("multi-cert TLS acceptor should still build when root pair also exists");

        assert!(acceptor.is_some());
    }
}
