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

use rustfs_common::globals::set_global_root_cert;
use rustfs_config::{RUSTFS_CA_CERT, RUSTFS_PUBLIC_CERT, RUSTFS_TLS_CERT};
use tracing::{debug, info};

/// Initialize TLS certificates for inter-node communication.
/// This function attempts to load certificates from the specified `tls_path`.
/// It looks for `rustfs_cert.pem`, `public.crt`, and `ca.crt` files.
/// Additionally, it tries to load system root certificates from common locations
/// to ensure trust for public CAs when mixing self-signed and public certificates.
/// If any certificates are found, they are set as the global root certificates.
pub(crate) async fn init_cert(tls_path: &str) {
    let mut cert_data = Vec::new();

    // Try rustfs_cert.pem (custom cert name)
    walk_dir(std::path::PathBuf::from(tls_path), RUSTFS_TLS_CERT, &mut cert_data).await;

    // Try public.crt (common CA name)
    let public_cert_path = std::path::Path::new(tls_path).join(RUSTFS_PUBLIC_CERT);
    load_cert_file(public_cert_path.to_str().unwrap_or_default(), &mut cert_data, "CA certificate").await;

    // Try ca.crt (common CA name)
    let ca_cert_path = std::path::Path::new(tls_path).join(RUSTFS_CA_CERT);
    load_cert_file(ca_cert_path.to_str().unwrap_or_default(), &mut cert_data, "CA certificate").await;

    let trust_system_ca = rustfs_utils::get_env_bool(rustfs_config::ENV_TRUST_SYSTEM_CA, rustfs_config::DEFAULT_TRUST_SYSTEM_CA);
    if !trust_system_ca {
        // Attempt to load system root certificates to maintain trust for public CAs
        // This is important when mixing self-signed internal certs with public external certs
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
                break; // Stop after finding the first valid bundle
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
async fn walk_dir(path: std::path::PathBuf, cert_name: &str, cert_data: &mut Vec<u8>) {
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
