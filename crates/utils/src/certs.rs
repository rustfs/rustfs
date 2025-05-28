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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_certs_error_function() {
        let error_msg = "Test error message";
        let error = certs_error(error_msg.to_string());

        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(error.to_string(), error_msg);
    }

    #[test]
    fn test_load_certs_file_not_found() {
        let result = load_certs("non_existent_file.pem");
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert!(error.to_string().contains("failed to open"));
    }

    #[test]
    fn test_load_private_key_file_not_found() {
        let result = load_private_key("non_existent_key.pem");
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert!(error.to_string().contains("failed to open"));
    }

    #[test]
    fn test_load_certs_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("empty.pem");
        fs::write(&cert_path, "").unwrap();

        let result = load_certs(cert_path.to_str().unwrap());
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("No valid certificate was found"));
    }

    #[test]
    fn test_load_certs_invalid_format() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("invalid.pem");
        fs::write(&cert_path, "invalid certificate content").unwrap();

        let result = load_certs(cert_path.to_str().unwrap());
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("No valid certificate was found"));
    }

    #[test]
    fn test_load_private_key_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let key_path = temp_dir.path().join("empty_key.pem");
        fs::write(&key_path, "").unwrap();

        let result = load_private_key(key_path.to_str().unwrap());
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("no private key found"));
    }

    #[test]
    fn test_load_private_key_invalid_format() {
        let temp_dir = TempDir::new().unwrap();
        let key_path = temp_dir.path().join("invalid_key.pem");
        fs::write(&key_path, "invalid private key content").unwrap();

        let result = load_private_key(key_path.to_str().unwrap());
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("no private key found"));
    }

    #[test]
    fn test_load_all_certs_from_directory_not_exists() {
        let result = load_all_certs_from_directory("/non/existent/directory");
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("does not exist or is not a directory"));
    }

    #[test]
    fn test_load_all_certs_from_directory_empty() {
        let temp_dir = TempDir::new().unwrap();

        let result = load_all_certs_from_directory(temp_dir.path().to_str().unwrap());
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("No valid certificate/private key pair found"));
    }

    #[test]
    fn test_load_all_certs_from_directory_file_instead_of_dir() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("not_a_directory.txt");
        fs::write(&file_path, "content").unwrap();

        let result = load_all_certs_from_directory(file_path.to_str().unwrap());
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("does not exist or is not a directory"));
    }

    #[test]
    fn test_load_cert_key_pair_missing_cert() {
        let temp_dir = TempDir::new().unwrap();
        let key_path = temp_dir.path().join("test_key.pem");
        fs::write(&key_path, "dummy key content").unwrap();

        let result = load_cert_key_pair("non_existent_cert.pem", key_path.to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_load_cert_key_pair_missing_key() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("test_cert.pem");
        fs::write(&cert_path, "dummy cert content").unwrap();

        let result = load_cert_key_pair(cert_path.to_str().unwrap(), "non_existent_key.pem");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_multi_cert_resolver_empty_map() {
        let empty_map = HashMap::new();
        let result = create_multi_cert_resolver(empty_map);

        // Should succeed even with empty map
        assert!(result.is_ok());
    }

    #[test]
    fn test_error_message_formatting() {
        let test_cases = vec![
            ("file not found", "failed to open test.pem: file not found"),
            ("permission denied", "failed to open key.pem: permission denied"),
            ("invalid format", "certificate file cert.pem format error:invalid format"),
        ];

        for (input, _expected_pattern) in test_cases {
            let error1 = certs_error(format!("failed to open test.pem: {}", input));
            assert!(error1.to_string().contains(input));

            let error2 = certs_error(format!("failed to open key.pem: {}", input));
            assert!(error2.to_string().contains(input));
        }
    }

    #[test]
    fn test_path_handling_edge_cases() {
        // Test with various path formats
        let path_cases = vec![
            "",               // Empty path
            ".",              // Current directory
            "..",             // Parent directory
            "/",              // Root directory (Unix)
            "relative/path",  // Relative path
            "/absolute/path", // Absolute path
        ];

        for path in path_cases {
            let result = load_all_certs_from_directory(path);
            // All should fail since these are not valid cert directories
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_filename_constants_consistency() {
        // Test that the constants match expected values
        assert_eq!(RUSTFS_TLS_CERT, "rustfs_cert.pem");
        assert_eq!(RUSTFS_TLS_KEY, "rustfs_key.pem");

        // Test that constants are not empty
        assert!(!RUSTFS_TLS_CERT.is_empty());
        assert!(!RUSTFS_TLS_KEY.is_empty());

        // Test that constants have proper extensions
        assert!(RUSTFS_TLS_CERT.ends_with(".pem"));
        assert!(RUSTFS_TLS_KEY.ends_with(".pem"));
    }

    #[test]
    fn test_directory_structure_validation() {
        let temp_dir = TempDir::new().unwrap();

        // Create a subdirectory without certificates
        let sub_dir = temp_dir.path().join("example.com");
        fs::create_dir(&sub_dir).unwrap();

        // Should fail because no certificates found
        let result = load_all_certs_from_directory(temp_dir.path().to_str().unwrap());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No valid certificate/private key pair found"));
    }

    #[test]
    fn test_unicode_path_handling() {
        let temp_dir = TempDir::new().unwrap();

        // Create directory with Unicode characters
        let unicode_dir = temp_dir.path().join("测试目录");
        fs::create_dir(&unicode_dir).unwrap();

        let result = load_all_certs_from_directory(unicode_dir.to_str().unwrap());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No valid certificate/private key pair found"));
    }

    #[test]
    fn test_concurrent_access_safety() {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let dir_path = Arc::new(temp_dir.path().to_string_lossy().to_string());

        let handles: Vec<_> = (0..5)
            .map(|_| {
                let path = Arc::clone(&dir_path);
                thread::spawn(move || {
                    let result = load_all_certs_from_directory(&path);
                    // All should fail since directory is empty
                    assert!(result.is_err());
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread should complete successfully");
        }
    }

    #[test]
    fn test_memory_efficiency() {
        // Test that error types are reasonably sized
        use std::mem;

        let error = certs_error("test".to_string());
        let error_size = mem::size_of_val(&error);

        // Error should not be excessively large
        assert!(error_size < 1024, "Error size should be reasonable, got {} bytes", error_size);
    }
}
