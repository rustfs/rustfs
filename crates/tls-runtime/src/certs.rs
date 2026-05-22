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

use rustls::RootCertStore;
use rustls::server::{
    ClientHello, ResolvesServerCert, ResolvesServerCertUsingSni, WebPkiClientVerifier, danger::ClientCertVerifier,
};
use rustls::sign::CertifiedKey;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::collections::HashMap;
use std::io::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs, io};
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct CertDirectoryLoadOptions {
    dir_path: PathBuf,
    cert_filename: String,
    key_filename: String,
}

impl CertDirectoryLoadOptions {
    pub fn builder(
        dir_path: impl Into<PathBuf>,
        cert_filename: impl Into<String>,
        key_filename: impl Into<String>,
    ) -> CertDirectoryLoadOptionsBuilder {
        CertDirectoryLoadOptionsBuilder {
            dir_path: dir_path.into(),
            cert_filename: cert_filename.into(),
            key_filename: key_filename.into(),
        }
    }

    fn validate(&self) -> io::Result<()> {
        if self.cert_filename.is_empty() {
            return Err(certs_error("certificate filename cannot be empty".to_string()));
        }
        if self.key_filename.is_empty() {
            return Err(certs_error("private key filename cannot be empty".to_string()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CertDirectoryLoadOptionsBuilder {
    dir_path: PathBuf,
    cert_filename: String,
    key_filename: String,
}

impl CertDirectoryLoadOptionsBuilder {
    pub fn cert_filename(mut self, cert_filename: impl Into<String>) -> Self {
        self.cert_filename = cert_filename.into();
        self
    }

    pub fn key_filename(mut self, key_filename: impl Into<String>) -> Self {
        self.key_filename = key_filename.into();
        self
    }

    pub fn build(self) -> CertDirectoryLoadOptions {
        CertDirectoryLoadOptions {
            dir_path: self.dir_path,
            cert_filename: self.cert_filename,
            key_filename: self.key_filename,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WebPkiClientVerifierOptions {
    tls_path: PathBuf,
    enabled: bool,
    client_ca_cert_filename: String,
    fallback_ca_cert_filename: String,
}

impl WebPkiClientVerifierOptions {
    pub fn builder(
        tls_path: impl Into<PathBuf>,
        client_ca_cert_filename: impl Into<String>,
        fallback_ca_cert_filename: impl Into<String>,
    ) -> WebPkiClientVerifierOptionsBuilder {
        WebPkiClientVerifierOptionsBuilder {
            tls_path: tls_path.into(),
            enabled: false,
            client_ca_cert_filename: client_ca_cert_filename.into(),
            fallback_ca_cert_filename: fallback_ca_cert_filename.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WebPkiClientVerifierOptionsBuilder {
    tls_path: PathBuf,
    enabled: bool,
    client_ca_cert_filename: String,
    fallback_ca_cert_filename: String,
}

impl WebPkiClientVerifierOptionsBuilder {
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn client_ca_cert_filename(mut self, client_ca_cert_filename: impl Into<String>) -> Self {
        self.client_ca_cert_filename = client_ca_cert_filename.into();
        self
    }

    pub fn fallback_ca_cert_filename(mut self, fallback_ca_cert_filename: impl Into<String>) -> Self {
        self.fallback_ca_cert_filename = fallback_ca_cert_filename.into();
        self
    }

    pub fn build(self) -> WebPkiClientVerifierOptions {
        WebPkiClientVerifierOptions {
            tls_path: self.tls_path,
            enabled: self.enabled,
            client_ca_cert_filename: self.client_ca_cert_filename,
            fallback_ca_cert_filename: self.fallback_ca_cert_filename,
        }
    }
}

pub fn load_certs(filename: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let cert_file = fs::File::open(filename).map_err(|e| certs_error(format!("failed to open {filename}: {e}")))?;
    let mut reader = io::BufReader::new(cert_file);

    let certs = CertificateDer::pem_reader_iter(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| certs_error(format!("certificate file {filename} format error:{e:?}")))?;
    if certs.is_empty() {
        return Err(certs_error(format!("No valid certificate was found in the certificate file {filename}")));
    }
    Ok(certs)
}

pub fn load_cert_bundle_der_bytes(path: &str) -> io::Result<Vec<Vec<u8>>> {
    let pem = fs::read(path)?;
    let mut reader = io::BufReader::new(&pem[..]);

    let certs = CertificateDer::pem_reader_iter(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| certs_error(format!("Failed to parse PEM certs from {path}: {e}")))?;

    Ok(certs.into_iter().map(|c| c.to_vec()).collect())
}

pub fn build_webpki_client_verifier(options: WebPkiClientVerifierOptions) -> io::Result<Option<Arc<dyn ClientCertVerifier>>> {
    if !options.enabled {
        return Ok(None);
    }

    let tls_path = &options.tls_path;
    let ca_path = mtls_ca_bundle_path(&options).ok_or_else(|| {
        Error::other(format!(
            "mTLS is enabled but missing {}/{} (or fallback {}/{})",
            tls_path.display(),
            options.client_ca_cert_filename,
            tls_path.display(),
            options.fallback_ca_cert_filename
        ))
    })?;

    let ca_path = ca_path
        .to_str()
        .ok_or_else(|| Error::other(format!("Invalid UTF-8 in mTLS CA path: {ca_path:?}")))?;

    let der_list = load_cert_bundle_der_bytes(ca_path)?;

    let mut store = RootCertStore::empty();
    for der in der_list {
        store
            .add(der.into())
            .map_err(|e| Error::other(format!("Invalid client CA cert: {e}")))?;
    }

    let verifier = WebPkiClientVerifier::builder(Arc::new(store))
        .build()
        .map_err(|e| Error::other(format!("Build client cert verifier failed: {e}")))?;

    Ok(Some(verifier))
}

fn mtls_ca_bundle_path(options: &WebPkiClientVerifierOptions) -> Option<PathBuf> {
    let p1 = options.tls_path.join(&options.client_ca_cert_filename);
    if p1.exists() {
        return Some(p1);
    }
    let p2 = options.tls_path.join(&options.fallback_ca_cert_filename);
    if p2.exists() {
        return Some(p2);
    }
    None
}

pub fn load_private_key(filename: &str) -> io::Result<PrivateKeyDer<'static>> {
    let keyfile = fs::File::open(filename).map_err(|e| certs_error(format!("failed to open {filename}: {e}")))?;
    let mut reader = io::BufReader::new(keyfile);

    PrivateKeyDer::from_pem_reader(&mut reader)
        .map_err(|e| certs_error(format!("failed to parse private key in {filename}: {e}")))
}

pub fn certs_error(err: String) -> Error {
    Error::other(err)
}

fn is_discoverable_cert_domain_dir(domain_name: &str) -> bool {
    !domain_name.starts_with('.')
}

pub fn load_all_certs_from_directory(
    options: CertDirectoryLoadOptions,
) -> io::Result<HashMap<String, (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    options.validate()?;

    let mut cert_key_pairs = HashMap::new();
    let dir = options.dir_path.as_path();

    if !dir.exists() || !dir.is_dir() {
        return Err(certs_error(format!(
            "The certificate directory does not exist or is not a directory: {}",
            dir.display()
        )));
    }

    let root_cert_path = dir.join(&options.cert_filename);
    let root_key_path = dir.join(&options.key_filename);

    if root_cert_path.exists() && root_key_path.exists() {
        debug!("find the root directory certificate: {:?}", root_cert_path);
        let root_cert_str = root_cert_path
            .to_str()
            .ok_or_else(|| certs_error(format!("Invalid UTF-8 in root certificate path: {root_cert_path:?}")))?;
        let root_key_str = root_key_path
            .to_str()
            .ok_or_else(|| certs_error(format!("Invalid UTF-8 in root key path: {root_key_path:?}")))?;
        match load_cert_key_pair(root_cert_str, root_key_str) {
            Ok((certs, key)) => {
                cert_key_pairs.insert("default".to_string(), (certs, key));
            }
            Err(e) => {
                warn!("unable to load root directory certificate: {}", e);
            }
        }
    }

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            let domain_name: &str = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| certs_error(format!("invalid domain name directory:{path:?}")))?;
            if !is_discoverable_cert_domain_dir(domain_name) {
                debug!("skip internal certificate directory: {:?}", path);
                continue;
            }

            let cert_path = path.join(&options.cert_filename);
            let key_path = path.join(&options.key_filename);

            if cert_path.exists() && key_path.exists() {
                debug!("find the domain name certificate: {} in {:?}", domain_name, cert_path);
                let cert_path = match cert_path.to_str() {
                    Some(path) => path,
                    None => {
                        warn!("skip domain certificate load, invalid UTF-8 path: {:?}", cert_path);
                        continue;
                    }
                };

                let key_path = match key_path.to_str() {
                    Some(path) => path,
                    None => {
                        warn!("skip domain key load, invalid UTF-8 path: {:?}", key_path);
                        continue;
                    }
                };

                match load_cert_key_pair(cert_path, key_path) {
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
            dir.display()
        )));
    }

    Ok(cert_key_pairs)
}

fn load_cert_key_pair(cert_path: &str, key_path: &str) -> io::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    Ok((certs, key))
}

pub fn create_multi_cert_resolver(
    cert_key_pairs: HashMap<String, (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
) -> io::Result<impl ResolvesServerCert> {
    #[derive(Debug)]
    struct MultiCertResolver {
        cert_resolver: ResolvesServerCertUsingSni,
        default_cert: Option<Arc<CertifiedKey>>,
    }

    impl ResolvesServerCert for MultiCertResolver {
        fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
            if let Some(cert) = self.cert_resolver.resolve(client_hello) {
                return Some(cert);
            }

            self.default_cert.clone()
        }
    }

    let mut resolver = ResolvesServerCertUsingSni::new();
    let mut default_cert = None;

    for (domain, (certs, key)) in cert_key_pairs {
        let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
            .map_err(|e| certs_error(format!("unsupported private key types:{domain}, err:{e:?}")))?;

        let certified_key = CertifiedKey::new(certs, signing_key);
        if domain == "default" {
            default_cert = Some(Arc::new(certified_key.clone()));
        } else {
            resolver
                .add(&domain, certified_key)
                .map_err(|e| certs_error(format!("failed to add a domain name certificate:{domain},err: {e:?}")))?;
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
    use std::io::ErrorKind;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn default_load_options(path: impl Into<PathBuf>) -> CertDirectoryLoadOptions {
        CertDirectoryLoadOptions::builder(path, "rustfs_cert.pem", "rustfs_key.pem").build()
    }

    fn write_test_cert_pair(dir: &std::path::Path) {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["example.com".to_string()]).expect("cert should generate");
        fs::write(dir.join("rustfs_cert.pem"), cert.pem()).expect("cert should write");
        fs::write(dir.join("rustfs_key.pem"), signing_key.serialize_pem()).expect("key should write");
    }

    #[test]
    fn test_certs_error_function() {
        let error_msg = "Test error message";
        let error = certs_error(error_msg.to_string());

        assert_eq!(error.kind(), ErrorKind::Other);
        assert_eq!(error.to_string(), error_msg);
    }

    #[test]
    fn test_load_certs_file_not_found() {
        let result = load_certs("non_existent_file.pem");
        assert!(result.is_err());

        let error = result.expect_err("missing cert should error");
        assert_eq!(error.kind(), ErrorKind::Other);
        assert!(error.to_string().contains("failed to open"));
    }

    #[test]
    fn test_load_private_key_file_not_found() {
        let result = load_private_key("non_existent_key.pem");
        assert!(result.is_err());

        let error = result.expect_err("missing key should error");
        assert_eq!(error.kind(), ErrorKind::Other);
        assert!(error.to_string().contains("failed to open"));
    }

    #[test]
    fn test_load_all_certs_from_directory_empty() {
        let temp_dir = TempDir::new().expect("tempdir should create");
        let result = load_all_certs_from_directory(default_load_options(temp_dir.path()));
        assert!(result.is_err());
        let error = result.expect_err("empty directory should error");
        assert!(error.to_string().contains("No valid certificate/private key pair found"));
    }

    #[test]
    fn test_load_all_certs_skips_kubernetes_secret_projection_dirs() {
        let temp_dir = TempDir::new().expect("tempdir should create");
        write_test_cert_pair(temp_dir.path());

        let domain_dir = temp_dir.path().join("example.com");
        fs::create_dir(&domain_dir).expect("domain dir should create");
        write_test_cert_pair(&domain_dir);

        for internal_dir_name in ["..data", "..2026_04_28_18_33_53.4209048473"] {
            let internal_dir = temp_dir.path().join(internal_dir_name);
            fs::create_dir(&internal_dir).expect("internal dir should create");
            write_test_cert_pair(&internal_dir);
        }

        let certs = load_all_certs_from_directory(default_load_options(temp_dir.path())).expect("certs should load");
        assert!(certs.contains_key("default"));
        assert!(certs.contains_key("example.com"));
        assert!(!certs.contains_key("..data"));
        assert_eq!(certs.len(), 2);
    }
}
