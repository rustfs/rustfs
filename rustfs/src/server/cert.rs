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

use rustfs_common::{MtlsIdentityPem, set_global_mtls_identity, set_global_root_cert};
use rustfs_config::{RUSTFS_CA_CERT, RUSTFS_PUBLIC_CERT, RUSTFS_TLS_CERT};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::path::{Path, PathBuf};
use tracing::{debug, info};

#[derive(Debug)]
pub enum RustFSError {
    Cert(String),
}

impl std::fmt::Display for RustFSError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RustFSError::Cert(msg) => write!(f, "Certificate error: {}", msg),
        }
    }
}

impl std::error::Error for RustFSError {}

/// Parse PEM-encoded certificates into DER format.
/// Returns a vector of DER-encoded certificates.
///
/// # Arguments
/// * `pem` - A byte slice containing the PEM-encoded certificates.
///
/// # Returns
/// A vector of `CertificateDer` containing the DER-encoded certificates.
///
/// # Errors
/// Returns `RustFSError` if parsing fails.
fn parse_pem_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, RustFSError> {
    let mut out = Vec::new();
    let mut reader = std::io::Cursor::new(pem);
    for item in rustls_pemfile::certs(&mut reader) {
        let c = item.map_err(|e| RustFSError::Cert(format!("parse cert pem: {e}")))?;
        out.push(c);
    }
    Ok(out)
}

/// Parse a PEM-encoded private key into DER format.
/// Supports PKCS#8 and RSA private keys.
///
/// # Arguments
/// * `pem` - A byte slice containing the PEM-encoded private key.
///
/// # Returns
/// A `PrivateKeyDer` containing the DER-encoded private key.
///
/// # Errors
/// Returns `RustFSError` if parsing fails or no key is found.
fn parse_pem_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, RustFSError> {
    let mut reader = std::io::Cursor::new(pem);
    let key = rustls_pemfile::private_key(&mut reader).map_err(|e| RustFSError::Cert(format!("parse private key pem: {e}")))?;
    key.ok_or_else(|| RustFSError::Cert("no private key found in PEM".into()))
}

/// Helper function to read a file and return its contents.
/// Returns the file contents as a vector of bytes.
/// # Errors
/// Returns `RustFSError` if reading fails.
async fn read_file(path: &PathBuf, desc: &str) -> Result<Vec<u8>, RustFSError> {
    tokio::fs::read(path)
        .await
        .map_err(|e| RustFSError::Cert(format!("read {} {:?}: {e}", desc, path)))
}

/// Initialize TLS material for both server and outbound client connections.
///
/// Loads roots from:
/// - `${RUSTFS_TLS_PATH}/ca.crt` (or `tls/ca.crt`)
/// - `${RUSTFS_TLS_PATH}/public.crt` (optional additional root bundle)
/// - system roots if `RUSTFS_TRUST_SYSTEM_CA=true` (default: false)
/// - if `RUSTFS_TRUST_LEAF_CERT_AS_CA=true`, also loads leaf cert(s) from
///   `${RUSTFS_TLS_PATH}/rustfs_cert.pem` into the root store.
///
/// Loads mTLS client identity (optional) from:
/// - `${RUSTFS_TLS_PATH}/client_cert.pem`
/// - `${RUSTFS_TLS_PATH}/client_key.pem`
///
/// Environment overrides:
/// - RUSTFS_TLS_PATH
/// - RUSTFS_MTLS_CLIENT_CERT
/// - RUSTFS_MTLS_CLIENT_KEY
pub(crate) async fn init_cert(tls_path: &str) -> Result<(), RustFSError> {
    if tls_path.is_empty() {
        info!("No TLS path configured; skipping certificate initialization");
        return Ok(());
    }
    let tls_dir = PathBuf::from(tls_path);

    // Load root certificates
    load_root_certs(&tls_dir).await?;

    // Load optional mTLS identity
    load_mtls_identity(&tls_dir).await?;

    Ok(())
}

/// Load root certificates from various sources.
async fn load_root_certs(tls_dir: &Path) -> Result<(), RustFSError> {
    let mut cert_data = Vec::new();

    let trust_leaf_as_ca =
        rustfs_utils::get_env_bool(rustfs_config::ENV_TRUST_LEAF_CERT_AS_CA, rustfs_config::DEFAULT_TRUST_LEAF_CERT_AS_CA);
    if trust_leaf_as_ca {
        walk_dir(tls_dir.to_path_buf(), RUSTFS_TLS_CERT, &mut cert_data).await;
        info!("Loaded leaf certificate(s) as root CA as per RUSTFS_TRUST_LEAF_CERT_AS_CA");
    }

    // Try public.crt and ca.crt
    let public_cert_path = tls_dir.join(RUSTFS_PUBLIC_CERT);
    load_cert_file(public_cert_path.to_str().unwrap_or_default(), &mut cert_data, "CA certificate").await;

    let ca_cert_path = tls_dir.join(RUSTFS_CA_CERT);
    load_cert_file(ca_cert_path.to_str().unwrap_or_default(), &mut cert_data, "CA certificate").await;

    // Load system root certificates if enabled
    let trust_system_ca = rustfs_utils::get_env_bool(rustfs_config::ENV_TRUST_SYSTEM_CA, rustfs_config::DEFAULT_TRUST_SYSTEM_CA);
    if trust_system_ca {
        let system_ca_paths = [
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

        let mut system_cert_loaded = false;
        for path in system_ca_paths {
            if load_cert_file(path, &mut cert_data, "system root certificates").await {
                system_cert_loaded = true;
                info!("Loaded system root certificates from {}", path);
                break;
            }
        }

        if !system_cert_loaded {
            debug!("Could not find system root certificates in common locations.");
        }
    } else {
        info!("Loading system root certificates disabled via RUSTFS_TRUST_SYSTEM_CA");
    }

    if !cert_data.is_empty() {
        set_global_root_cert(cert_data).await;
        info!("Configured custom root certificates for inter-node communication");
    }

    Ok(())
}

/// Load optional mTLS identity.
async fn load_mtls_identity(tls_dir: &Path) -> Result<(), RustFSError> {
    let client_cert_path = match rustfs_utils::get_env_opt_str(rustfs_config::ENV_MTLS_CLIENT_CERT) {
        Some(p) => PathBuf::from(p),
        None => tls_dir.join(rustfs_config::RUSTFS_CLIENT_CERT_FILENAME),
    };

    let client_key_path = match rustfs_utils::get_env_opt_str(rustfs_config::ENV_MTLS_CLIENT_KEY) {
        Some(p) => PathBuf::from(p),
        None => tls_dir.join(rustfs_config::RUSTFS_CLIENT_KEY_FILENAME),
    };

    if client_cert_path.exists() && client_key_path.exists() {
        let cert_bytes = read_file(&client_cert_path, "client cert").await?;
        let key_bytes = read_file(&client_key_path, "client key").await?;

        // Validate parse-ability early; store as PEM bytes for tonic.
        parse_pem_certs(&cert_bytes)?;
        parse_pem_private_key(&key_bytes)?;

        let identity_pem = MtlsIdentityPem {
            cert_pem: cert_bytes,
            key_pem: key_bytes,
        };

        set_global_mtls_identity(Some(identity_pem)).await;
        info!("Loaded mTLS client identity cert={:?} key={:?}", client_cert_path, client_key_path);
    } else {
        set_global_mtls_identity(None).await;
        info!(
            "mTLS client identity not configured (missing {:?} and/or {:?}); proceeding with server-only TLS",
            client_cert_path, client_key_path
        );
    }

    Ok(())
}

/// Helper function to load a certificate file and append to cert_data.
/// Returns true if the file was successfully loaded.
async fn load_cert_file(path: &str, cert_data: &mut Vec<u8>, desc: &str) -> bool {
    if tokio::fs::metadata(path).await.is_ok() {
        if let Ok(data) = tokio::fs::read(path).await {
            cert_data.extend(data);
            cert_data.push(b'\n');
            info!("Loaded {} from {}", desc, path);
            true
        } else {
            debug!("Failed to read {} from {}", desc, path);
            false
        }
    } else {
        debug!("{} file not found at {}", desc, path);
        false
    }
}

/// Load the certificate file if its name matches `cert_name`.
/// If it matches, the certificate data is appended to `cert_data`.
///
/// # Parameters
/// - `entry`: The directory entry to check.
/// - `cert_name`: The name of the certificate file to match.
/// - `cert_data`: A mutable vector to append loaded certificate data.
async fn load_if_matches(entry: &tokio::fs::DirEntry, cert_name: &str, cert_data: &mut Vec<u8>) {
    let fname = entry.file_name().to_string_lossy().to_string();
    if fname == cert_name {
        let p = entry.path();
        load_cert_file(&p.to_string_lossy(), cert_data, "certificate").await;
    }
}

/// Search the directory at `path` and one level of subdirectories to find and load
/// certificates matching `cert_name`. Loaded certificate data is appended to
/// `cert_data`.
/// # Parameters
/// - `path`: The starting directory path to search for certificates.
/// - `cert_name`: The name of the certificate file to look for.
/// - `cert_data`: A mutable vector to append loaded certificate data.
async fn walk_dir(path: PathBuf, cert_name: &str, cert_data: &mut Vec<u8>) {
    if let Ok(mut rd) = tokio::fs::read_dir(&path).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            if let Ok(ft) = entry.file_type().await {
                if ft.is_file() {
                    load_if_matches(&entry, cert_name, cert_data).await;
                } else if ft.is_dir() {
                    // Only check direct subdirectories, no deeper recursion
                    if let Ok(mut sub_rd) = tokio::fs::read_dir(&entry.path()).await {
                        while let Ok(Some(sub_entry)) = sub_rd.next_entry().await {
                            if let Ok(sub_ft) = sub_entry.file_type().await {
                                if sub_ft.is_file() {
                                    load_if_matches(&sub_entry, cert_name, cert_data).await;
                                }
                                // Ignore subdirectories and symlinks in subdirs to limit to one level
                            }
                        }
                    }
                } else if ft.is_symlink() {
                    // Follow symlink and treat target as file or directory, but limit to one level
                    if let Ok(meta) = tokio::fs::metadata(&entry.path()).await {
                        if meta.is_file() {
                            load_if_matches(&entry, cert_name, cert_data).await;
                        } else if meta.is_dir() {
                            // Treat as directory but only check its direct contents
                            if let Ok(mut sub_rd) = tokio::fs::read_dir(&entry.path()).await {
                                while let Ok(Some(sub_entry)) = sub_rd.next_entry().await {
                                    if let Ok(sub_ft) = sub_entry.file_type().await {
                                        if sub_ft.is_file() {
                                            load_if_matches(&sub_entry, cert_name, cert_data).await;
                                        }
                                        // Ignore deeper levels
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    } else {
        debug!("Certificate directory not found: {}", path.display());
    }
}
