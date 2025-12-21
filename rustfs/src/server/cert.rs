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
pub(crate) async fn init_cert(tls_path: &String) {
    let mut cert_data = Vec::new();

    // Try rustfs_cert.pem (custom cert name)
    walk_dir(std::path::PathBuf::from(tls_path), RUSTFS_TLS_CERT, &mut cert_data).await;

    // Try public.crt (common CA name)
    load_cert_file(&format!("{tls_path}/{RUSTFS_PUBLIC_CERT}"), &mut cert_data, "CA certificate").await;

    // Try ca.crt (common CA name)
    load_cert_file(&format!("{tls_path}/{RUSTFS_CA_CERT}"), &mut cert_data, "CA certificate").await;

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
            break; // Stop after finding the first valid bundle
        }
    }

    if !system_cert_loaded {
        debug!("Could not find system root certificates in common locations.");
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
            info!("Failed to read {} from {}", desc, path);
            false
        }
    } else {
        info!("{} file not found at {}", desc, path);
        false
    }
}

/// Recursively walk through the directory at `path` to find and load certificates
/// matching `cert_name`. Loaded certificate data is appended to `cert_data`.
/// This function uses an iterative approach with a stack to avoid deep recursion.
/// # Parameters
/// - `path`: The starting directory path to search for certificates.
/// - `cert_name`: The name of the certificate file to look for.
/// - `cert_data`: A mutable vector to append loaded certificate data.
async fn walk_dir(path: std::path::PathBuf, cert_name: &str, cert_data: &mut Vec<u8>) {
    let mut stack = vec![path];

    while let Some(curr) = stack.pop() {
        if let Ok(mut rd) = tokio::fs::read_dir(&curr).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                if let Ok(ft) = entry.file_type().await {
                    let p = entry.path();
                    if ft.is_file() {
                        let fname = entry.file_name().to_string_lossy().to_string();
                        if fname == cert_name {
                            load_cert_file(&p.to_string_lossy(), cert_data, "certificate").await;
                        }
                    } else if ft.is_dir() {
                        stack.push(p);
                    }
                }
            }
        } else {
            debug!("Certificate directory not found: {}", curr.display());
        }
    }
}
