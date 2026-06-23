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

use crate::app::context::resolve_outbound_tls_generation;
use rustfs_common::MtlsIdentityPem;
use rustfs_config::{
    DEFAULT_SERVER_MTLS_ENABLE, DEFAULT_TLS_KEYLOG, DEFAULT_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_INTERVAL,
    DEFAULT_TRUST_LEAF_CERT_AS_CA, DEFAULT_TRUST_SYSTEM_CA, ENV_MTLS_CLIENT_CERT, ENV_MTLS_CLIENT_KEY, ENV_SERVER_MTLS_ENABLE,
    ENV_TLS_KEYLOG, ENV_TLS_RELOAD_ENABLE, ENV_TLS_RELOAD_INTERVAL, ENV_TRUST_LEAF_CERT_AS_CA, ENV_TRUST_SYSTEM_CA,
    RUSTFS_CA_CERT, RUSTFS_CLIENT_CA_CERT_FILENAME, RUSTFS_CLIENT_CERT_FILENAME, RUSTFS_CLIENT_KEY_FILENAME, RUSTFS_TLS_CERT,
};
use rustfs_tls_runtime::{
    ServerTlsMaterial as RuntimeServerTlsMaterial, TlsGeneration, TlsSource, WebPkiClientVerifierOptions,
    build_webpki_client_verifier, create_multi_cert_resolver, publish_global_outbound_tls_state, record_tls_generation,
    record_tls_reload_result, record_tls_reload_skipped,
};
use rustfs_utils::{get_env_bool, get_env_opt_str};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio_rustls::TlsAcceptor;
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
    if tls_path.is_empty() {
        return Err(TlsMaterialError::Io("TLS path is empty".into()));
    }

    let tls_source = TlsSource::from_directory(tls_path);
    let mut snapshot = rustfs_tls_runtime::TlsMaterialSnapshot::load(&tls_source)
        .await
        .map_err(map_runtime_tls_error)?;

    enrich_outbound(&mut snapshot.outbound, &PathBuf::from(tls_path)).await?;

    Ok(snapshot)
}

/// Build a TLS acceptor from pre-loaded server material.
///
/// Takes the server material already loaded by `load_tls_material` and constructs
/// the `rustls::ServerConfig` without any additional disk reads (except the mTLS
/// client CA verifier, which reads the client CA cert from disk).
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

    match server {
        Some(RuntimeServerTlsMaterial::SingleCert { certs, key }) => {
            let config = build_server_config(ServerCertSource::SingleCert { certs, key }, mtls_verifier)?;
            info!(
                component = LOG_COMPONENT_TLS,
                subsystem = LOG_SUBSYSTEM_TLS,
                event = "tls_acceptor_created",
                mode = "single_cert",
                "TLS acceptor created"
            );
            let acceptor = Arc::new(TlsAcceptor::from(Arc::new(config)));
            Ok(Some(Arc::new(TlsAcceptorHolder::new(acceptor))))
        }
        Some(RuntimeServerTlsMaterial::MultiCert { cert_key_pairs }) => {
            let resolver = create_multi_cert_resolver(cert_key_pairs)
                .map_err(|e| TlsMaterialError::Parse(format!("build multi-cert resolver: {e}")))?;
            let config = build_server_config(ServerCertSource::Resolver(Arc::new(resolver)), mtls_verifier)?;
            info!(
                component = LOG_COMPONENT_TLS,
                subsystem = LOG_SUBSYSTEM_TLS,
                event = "tls_acceptor_created",
                mode = "sni_resolver",
                "TLS acceptor created"
            );
            let acceptor = Arc::new(TlsAcceptor::from(Arc::new(config)));
            Ok(Some(Arc::new(TlsAcceptorHolder::new(acceptor))))
        }
        None => Ok(None),
    }
}

// ── Outbound Enrichment ──

/// Enrich the outbound TLS material with platform-specific additions that the
/// tls-runtime crate does not handle:
/// 1. Optional: server leaf cert as root CA (`RUSTFS_TRUST_LEAF_CERT_AS_CA`)
/// 2. Optional: system root CAs from platform paths (`RUSTFS_TRUST_SYSTEM_CA`)
/// 3. Optional: mTLS identity from env-var-overridden paths
async fn enrich_outbound(outbound: &mut rustfs_tls_runtime::OutboundTlsMaterial, tls_dir: &Path) -> Result<(), TlsMaterialError> {
    // 1. Optional: load leaf certs as root CAs
    if get_env_bool(ENV_TRUST_LEAF_CERT_AS_CA, DEFAULT_TRUST_LEAF_CERT_AS_CA)
        && load_cert_file_by_name(tls_dir, RUSTFS_TLS_CERT, &mut outbound.root_ca_pem).await
    {
        info!(
            component = LOG_COMPONENT_TLS,
            subsystem = LOG_SUBSYSTEM_TLS,
            event = "tls_root_ca_enriched",
            source = "leaf_certificate",
            "TLS root CA set enriched"
        );
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

    if !client_cert_path.exists() || !client_key_path.exists() {
        info!(
            component = LOG_COMPONENT_TLS,
            subsystem = LOG_SUBSYSTEM_TLS,
            event = "mtls_identity_state",
            state = "not_configured",
            cert_path = ?client_cert_path,
            key_path = ?client_key_path,
            "mTLS client identity not configured"
        );
        return Ok(None);
    }

    let cert_pem = tokio::fs::read(&client_cert_path)
        .await
        .map_err(|e| TlsMaterialError::Io(format!("read client cert {client_cert_path:?}: {e}")))?;
    let key_pem = tokio::fs::read(&client_key_path)
        .await
        .map_err(|e| TlsMaterialError::Io(format!("read client key {client_key_path:?}: {e}")))?;

    let mut reader = std::io::Cursor::new(&cert_pem);
    if CertificateDer::pem_reader_iter(&mut reader).next().is_none() {
        return Err(TlsMaterialError::Parse("no valid certificate in client cert PEM".into()));
    }
    let mut reader = std::io::Cursor::new(&key_pem);
    PrivateKeyDer::from_pem_reader(&mut reader).map_err(|e| TlsMaterialError::Parse(format!("invalid client key PEM: {e}")))?;

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

/// Search for and load certificate files matching `cert_name` in the directory
/// and one level of subdirectories.
/// Returns `true` if at least one matching file was loaded.
async fn load_cert_file_by_name(dir: &Path, cert_name: &str, pem_data: &mut Vec<u8>) -> bool {
    let Ok(mut rd) = tokio::fs::read_dir(dir).await else {
        debug!(
            component = LOG_COMPONENT_TLS,
            subsystem = LOG_SUBSYSTEM_TLS,
            event = "tls_material_directory_state",
            path = %dir.display(),
            state = "missing",
            "TLS material directory state changed"
        );
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
        } else if ft.is_dir()
            && let Ok(mut sub_rd) = tokio::fs::read_dir(&entry.path()).await
        {
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
    loaded
}

// ── Server Config Construction ──

enum ServerCertSource {
    Resolver(Arc<dyn rustls::server::ResolvesServerCert + Send + Sync>),
    SingleCert {
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    },
}

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

fn tls_key_log() -> bool {
    get_env_bool(ENV_TLS_KEYLOG, DEFAULT_TLS_KEYLOG)
}

// ── Errors ──

#[derive(Debug)]
pub enum TlsMaterialError {
    Io(String),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TlsHandshakeFailureKind {
    UnexpectedEof,
    ProtocolVersion,
    Certificate,
    Alert,
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
            Self::Unknown => "UNKNOWN",
        }
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

// ── Reload Loop ──

/// Spawn a background task that periodically checks for TLS certificate changes.
/// Single load per tick: loads once via tls-runtime, enriches, publishes outbound,
/// and builds acceptor — no double reads.
pub(crate) fn spawn_reload_loop(tls_path: String, holder: Arc<TlsAcceptorHolder>) {
    let enabled = get_env_bool(ENV_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_ENABLE);
    if !enabled {
        debug!(
            component = LOG_COMPONENT_TLS,
            subsystem = LOG_SUBSYSTEM_TLS,
            event = "tls_reload_state",
            state = "disabled",
            env_var = ENV_TLS_RELOAD_ENABLE,
            "TLS reload state changed"
        );
        return;
    }

    let interval_secs = rustfs_utils::get_env_u64(ENV_TLS_RELOAD_INTERVAL, DEFAULT_TLS_RELOAD_INTERVAL).max(5);
    let tls_source = TlsSource::from_directory(&tls_path);

    info!(
        component = LOG_COMPONENT_TLS,
        subsystem = LOG_SUBSYSTEM_TLS,
        event = "tls_reload_state",
        state = "enabled",
        interval_secs,
        "TLS reload state changed"
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        let tls_dir = PathBuf::from(&tls_path);
        loop {
            interval.tick().await;

            match rustfs_tls_runtime::TlsMaterialSnapshot::load(&tls_source).await {
                Ok(mut snapshot) => {
                    if let Err(e) = enrich_outbound(&mut snapshot.outbound, &tls_dir).await {
                        record_tls_reload_result("rustfs_server_reload_loop", "enrich_err", None, None);
                        warn!(
                            component = LOG_COMPONENT_TLS,
                            subsystem = LOG_SUBSYSTEM_TLS,
                            event = "tls_reload_failed",
                            stage = "enrich_outbound",
                            error = %e,
                            "TLS reload failed"
                        );
                        continue;
                    }

                    let generation = resolve_outbound_tls_generation().0.saturating_add(1);
                    publish_global_outbound_tls_state(TlsGeneration(generation), &snapshot.outbound).await;
                    record_tls_generation("rustfs_server_reload_loop", generation);
                    if !snapshot.outbound.root_ca_pem.is_empty() {
                        info!(
                            component = LOG_COMPONENT_TLS,
                            subsystem = LOG_SUBSYSTEM_TLS,
                            event = "tls_root_ca_enriched",
                            source = "reload_snapshot",
                            generation,
                            "TLS root CA set updated"
                        );
                    }

                    match build_acceptor_from_loaded(snapshot.server, &tls_dir).await {
                        Ok(Some(new_holder)) => {
                            info!(
                                component = LOG_COMPONENT_TLS,
                                subsystem = LOG_SUBSYSTEM_TLS,
                                event = "tls_reload_state",
                                state = "reloaded",
                                generation,
                                "TLS reload state changed"
                            );
                            holder.swap(&new_holder);
                            record_tls_reload_result("rustfs_server_reload_loop", "ok", None, Some(generation));
                        }
                        Ok(None) => {
                            record_tls_reload_skipped("rustfs_server_reload_loop", "no_acceptor");
                            warn!(
                                component = LOG_COMPONENT_TLS,
                                subsystem = LOG_SUBSYSTEM_TLS,
                                event = "tls_reload_failed",
                                stage = "build_acceptor",
                                reason = "no_acceptor",
                                "TLS reload returned no acceptor"
                            )
                        }
                        Err(e) => {
                            record_tls_reload_result("rustfs_server_reload_loop", "acceptor_err", None, Some(generation));
                            warn!(
                                component = LOG_COMPONENT_TLS,
                                subsystem = LOG_SUBSYSTEM_TLS,
                                event = "tls_reload_failed",
                                stage = "build_acceptor",
                                error = %e,
                                generation,
                                "TLS reload failed"
                            )
                        }
                    }
                }
                Err(e) => {
                    record_tls_reload_result("rustfs_server_reload_loop", "load_err", None, None);
                    warn!(
                        component = LOG_COMPONENT_TLS,
                        subsystem = LOG_SUBSYSTEM_TLS,
                        event = "tls_reload_failed",
                        stage = "load_snapshot",
                        error = %e,
                        "TLS reload failed"
                    );
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
}
