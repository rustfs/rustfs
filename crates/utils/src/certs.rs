use rustfs_config::{RUSTFS_TLS_CERT, RUSTFS_TLS_KEY};
use rustls::server::{ClientHello, ResolvesServerCert, ResolvesServerCertUsingSni};
use rustls::sign::CertifiedKey;
use rustls_pemfile::{certs, private_key};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::HashMap;
use std::io::Error;
use std::path::Path;
use std::sync::Arc;
use std::{fs, io};
use tracing::{debug, warn};

/// Load public certificate from file.
/// This function loads a public certificate from the specified file.
pub fn load_certs(filename: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    // Open certificate file.
    let cert_file = fs::File::open(filename).map_err(|e| certs_error(format!("failed to open {}: {}", filename, e)))?;
    let mut reader = io::BufReader::new(cert_file);

    // Load and return certificate.
    let certs = certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| certs_error(format!("certificate file {} format error:{:?}", filename, e)))?;
    if certs.is_empty() {
        return Err(certs_error(format!(
            "No valid certificate was found in the certificate file {}",
            filename
        )));
    }
    Ok(certs)
}

/// Load private key from file.
/// This function loads a private key from the specified file.
pub fn load_private_key(filename: &str) -> io::Result<PrivateKeyDer<'static>> {
    // Open keyfile.
    let keyfile = fs::File::open(filename).map_err(|e| certs_error(format!("failed to open {}: {}", filename, e)))?;
    let mut reader = io::BufReader::new(keyfile);

    // Load and return a single private key.
    private_key(&mut reader)?.ok_or_else(|| certs_error(format!("no private key found in {}", filename)))
}

/// error function
pub fn certs_error(err: String) -> Error {
    Error::other(err)
}

/// Load all certificates and private keys in the directory
/// This function loads all certificate and private key pairs from the specified directory.
/// It looks for files named `rustfs_cert.pem` and `rustfs_key.pem` in each subdirectory.
/// The root directory can also contain a default certificate/private key pair.
pub fn load_all_certs_from_directory(
    dir_path: &str,
) -> io::Result<HashMap<String, (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    let mut cert_key_pairs = HashMap::new();
    let dir = Path::new(dir_path);

    if !dir.exists() || !dir.is_dir() {
        return Err(certs_error(format!(
            "The certificate directory does not exist or is not a directory: {}",
            dir_path
        )));
    }

    // 1. First check whether there is a certificate/private key pair in the root directory
    let root_cert_path = dir.join(RUSTFS_TLS_CERT);
    let root_key_path = dir.join(RUSTFS_TLS_KEY);

    if root_cert_path.exists() && root_key_path.exists() {
        debug!("find the root directory certificate: {:?}", root_cert_path);
        let root_cert_str = root_cert_path
            .to_str()
            .ok_or_else(|| certs_error(format!("Invalid UTF-8 in root certificate path: {:?}", root_cert_path)))?;
        let root_key_str = root_key_path
            .to_str()
            .ok_or_else(|| certs_error(format!("Invalid UTF-8 in root key path: {:?}", root_key_path)))?;
        match load_cert_key_pair(root_cert_str, root_key_str) {
            Ok((certs, key)) => {
                // The root directory certificate is used as the default certificate and is stored using special keys.
                cert_key_pairs.insert("default".to_string(), (certs, key));
            }
            Err(e) => {
                warn!("unable to load root directory certificate: {}", e);
            }
        }
    }

    // 2.iterate through all folders in the directory
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            let domain_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| certs_error(format!("invalid domain name directory:{:?}", path)))?;

            // find certificate and private key files
            let cert_path = path.join(RUSTFS_TLS_CERT); // e.g., rustfs_cert.pem
            let key_path = path.join(RUSTFS_TLS_KEY); // e.g., rustfs_key.pem

            if cert_path.exists() && key_path.exists() {
                debug!("find the domain name certificate: {} in {:?}", domain_name, cert_path);
                match load_cert_key_pair(cert_path.to_str().unwrap(), key_path.to_str().unwrap()) {
                    Ok((certs, key)) => {
                        cert_key_pairs.insert(domain_name.to_string(), (certs, key));
                    }
                    Err(e) => {
                        warn!("unable to load the certificate for {} domain name: {}", domain_name, e);
                    }
                }
            }
        }
    }

    if cert_key_pairs.is_empty() {
        return Err(certs_error(format!(
            "No valid certificate/private key pair found in directory {}",
            dir_path
        )));
    }

    Ok(cert_key_pairs)
}

/// loading a single certificate private key pair
/// This function loads a certificate and private key from the specified paths.
/// It returns a tuple containing the certificate and private key.
fn load_cert_key_pair(cert_path: &str, key_path: &str) -> io::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    Ok((certs, key))
}

/// Create a multi-cert resolver
/// This function loads all certificates and private keys from the specified directory.
/// It uses the first certificate/private key pair found in the root directory as the default certificate.
/// The rest of the certificates/private keys are used for SNI resolution.
///
pub fn create_multi_cert_resolver(
    cert_key_pairs: HashMap<String, (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
) -> io::Result<impl ResolvesServerCert> {
    #[derive(Debug)]
    struct MultiCertResolver {
        cert_resolver: ResolvesServerCertUsingSni,
        default_cert: Option<Arc<CertifiedKey>>,
    }
    impl ResolvesServerCert for MultiCertResolver {
        fn resolve(&self, client_hello: ClientHello) -> Option<Arc<CertifiedKey>> {
            // try matching certificates with sni
            if let Some(cert) = self.cert_resolver.resolve(client_hello) {
                return Some(cert);
            }

            // If there is no matching SNI certificate, use the default certificate
            self.default_cert.clone()
        }
    }

    let mut resolver = ResolvesServerCertUsingSni::new();
    let mut default_cert = None;

    for (domain, (certs, key)) in cert_key_pairs {
        // create a signature
        let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
            .map_err(|e| certs_error(format!("unsupported private key types:{}, err:{:?}", domain, e)))?;

        // create a CertifiedKey
        let certified_key = CertifiedKey::new(certs, signing_key);
        if domain == "default" {
            default_cert = Some(Arc::new(certified_key.clone()));
        } else {
            // add certificate to resolver
            resolver
                .add(&domain, certified_key)
                .map_err(|e| certs_error(format!("failed to add a domain name certificate:{},err: {:?}", domain, e)))?;
        }
    }

    Ok(MultiCertResolver {
        cert_resolver: resolver,
        default_cert,
    })
}
