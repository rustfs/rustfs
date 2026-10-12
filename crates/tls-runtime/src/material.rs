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

use crate::certs::{
    CertDirectoryLoadOptions, load_all_certs_from_directory_strict, parse_certificate_pem, parse_private_key_pem,
    read_optional_tls_file, validate_certified_key,
};
use crate::error::TlsRuntimeError;
use crate::fingerprint::TlsFingerprint;
use crate::source::{TlsSource, TlsSourceSnapshot};
use rustfs_common::MtlsIdentityPem;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct OutboundTlsMaterial {
    pub root_ca_pem: Vec<u8>,
    pub mtls_identity: Option<MtlsIdentityPem>,
}

#[derive(Debug)]
pub enum ServerTlsMaterial {
    SingleCert {
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    },
    MultiCert {
        cert_key_pairs: HashMap<String, (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    },
}

impl ServerTlsMaterial {
    /// Revalidate an accepted generation without reading the filesystem.
    pub fn validate(&self) -> Result<(), TlsRuntimeError> {
        match self {
            Self::SingleCert { certs, key } => {
                crate::certs::validate_certified_key(certs, key, "default", true)?;
            }
            Self::MultiCert { cert_key_pairs } => {
                for (name, (certs, key)) in cert_key_pairs {
                    crate::certs::validate_certified_key(certs, key, name, true)?;
                }
            }
        }
        Ok(())
    }
}

impl Clone for ServerTlsMaterial {
    fn clone(&self) -> Self {
        match self {
            Self::SingleCert { certs, key } => Self::SingleCert {
                certs: certs.clone(),
                key: key.clone_key(),
            },
            Self::MultiCert { cert_key_pairs } => Self::MultiCert {
                cert_key_pairs: cert_key_pairs
                    .iter()
                    .map(|(name, (certs, key))| (name.clone(), (certs.clone(), key.clone_key())))
                    .collect(),
            },
        }
    }
}

#[derive(Debug)]
pub struct TlsMaterialSnapshot {
    /// The resolved directory generation used by every consumer of this snapshot.
    pub source: TlsSource,
    pub server: Option<ServerTlsMaterial>,
    pub outbound: OutboundTlsMaterial,
    pub server_client_ca_pem: Option<Vec<u8>>,
    pub material_files: BTreeSet<String>,
    pub fingerprint: TlsFingerprint,
}

impl TlsMaterialSnapshot {
    pub async fn load(source: &TlsSource) -> Result<Self, TlsRuntimeError> {
        let source = source.clone();
        tokio::task::spawn_blocking(move || Self::load_from_snapshot(&source.snapshot()?)).await?
    }

    /// Load all consumers' material from one previously pinned directory generation.
    pub fn load_from_snapshot(snapshot: &TlsSourceSnapshot) -> Result<Self, TlsRuntimeError> {
        Self::load_from_snapshot_with_server_directory(snapshot, None)
    }

    /// Load a server-only publication without replacing the pinned trust or client identity source.
    pub fn load_from_snapshot_with_server_directory(
        snapshot: &TlsSourceSnapshot,
        server_directory: Option<&Path>,
    ) -> Result<Self, TlsRuntimeError> {
        snapshot.validate_available()?;
        let source = snapshot.resolved_source();
        let base_dir = snapshot.base_dir();
        let server = load_server_material(source, server_directory.unwrap_or(base_dir))?;
        Self::load_from_snapshot_with_loaded_server(snapshot, server)
    }

    /// Validate an already decoded default server chain while reading trust and client identity from the pinned source.
    /// Serving validity, EKU and key matching are checked here even when the publication ledger was verified earlier.
    pub fn load_from_snapshot_with_server_identity(
        snapshot: &TlsSourceSnapshot,
        certificates: Vec<CertificateDer<'static>>,
        private_key_pem: &[u8],
    ) -> Result<Self, TlsRuntimeError> {
        snapshot.validate_available()?;
        let key = parse_private_key_pem(private_key_pem, "default")?;
        validate_certified_key(&certificates, &key, "default", true)?;
        Self::load_from_snapshot_with_loaded_server(
            snapshot,
            Some(ServerTlsMaterial::SingleCert {
                certs: certificates,
                key,
            }),
        )
    }

    fn load_from_snapshot_with_loaded_server(
        snapshot: &TlsSourceSnapshot,
        server: Option<ServerTlsMaterial>,
    ) -> Result<Self, TlsRuntimeError> {
        snapshot.validate_available()?;
        let source = snapshot.resolved_source();
        let base_dir = snapshot.base_dir();
        let public_ca_pem = read_optional_tls_file(&base_dir.join(&source.layout.public_ca_filename))?;
        let fallback_ca_pem = read_optional_tls_file(&base_dir.join(&source.fallback_ca_filename))?;
        let client_ca_pem = read_optional_tls_file(&base_dir.join(&source.layout.client_ca_filename))?;
        let mut material_files = BTreeSet::new();
        for (name, present) in [
            (&source.layout.public_ca_filename, public_ca_pem.is_some()),
            (&source.fallback_ca_filename, fallback_ca_pem.is_some()),
            (&source.layout.client_ca_filename, client_ca_pem.is_some()),
        ] {
            if present {
                material_files.insert(name.clone());
            }
        }
        for (name, pem) in [
            (&source.layout.public_ca_filename, &public_ca_pem),
            (&source.fallback_ca_filename, &fallback_ca_pem),
            (&source.layout.client_ca_filename, &client_ca_pem),
        ] {
            if let Some(pem) = pem {
                let certs = parse_certificate_pem(pem, name)?;
                let mut roots = rustls::RootCertStore::empty();
                for cert in certs {
                    roots.add(cert).map_err(|source| TlsRuntimeError::InvalidTrustAnchor {
                        name: name.clone(),
                        source,
                    })?;
                }
            }
        }
        let server_client_ca_pem = client_ca_pem.or_else(|| fallback_ca_pem.clone());
        let root_ca_pem = combine_optional_pem(public_ca_pem.as_deref(), fallback_ca_pem.as_deref());
        let client_cert_pem = read_optional_tls_file(&base_dir.join(&source.layout.client_cert_filename))?;
        let client_key_pem = read_optional_tls_file(&base_dir.join(&source.layout.client_key_filename))?;
        if client_cert_pem.is_some() {
            material_files.insert(source.layout.client_cert_filename.clone());
        }
        if client_key_pem.is_some() {
            material_files.insert(source.layout.client_key_filename.clone());
        }
        let mtls_identity = match (client_cert_pem, client_key_pem) {
            (Some(cert_pem), Some(key_pem)) => {
                let certs = parse_certificate_pem(&cert_pem, "client identity")?;
                let key = parse_private_key_pem(&key_pem, "client identity")?;
                validate_certified_key(&certs, &key, "client identity", false)?;
                Some(MtlsIdentityPem { cert_pem, key_pem })
            }
            (None, None) => None,
            _ => {
                return Err(TlsRuntimeError::IncompleteIdentity {
                    name: "client identity".to_string(),
                });
            }
        };
        snapshot.validate_available()?;
        let outbound = OutboundTlsMaterial {
            root_ca_pem,
            mtls_identity: mtls_identity.clone(),
        };
        let mut server_fingerprint_bytes = server.as_ref().map(serialize_server_material_for_fingerprint);
        if let Some(pem) = &server_client_ca_pem {
            let bytes = server_fingerprint_bytes.get_or_insert_with(Vec::new);
            bytes.extend_from_slice(b"\0server-client-ca\0");
            bytes.extend_from_slice(pem);
        }
        let fingerprint = TlsFingerprint::from_optional_bytes(
            server_fingerprint_bytes.as_deref(),
            public_ca_pem.as_deref(),
            fallback_ca_pem.as_deref(),
            mtls_identity.as_ref().map(|identity| identity.cert_pem.as_slice()),
            mtls_identity.as_ref().map(|identity| identity.key_pem.as_slice()),
        );
        Ok(Self {
            source: snapshot.resolved_source().clone(),
            server,
            outbound,
            server_client_ca_pem,
            material_files,
            fingerprint,
        })
    }
}

fn load_server_material(source: &TlsSource, base_dir: &Path) -> Result<Option<ServerTlsMaterial>, TlsRuntimeError> {
    let mut cert_key_pairs = load_all_certs_from_directory_strict(
        CertDirectoryLoadOptions::builder(base_dir, &source.layout.server_cert_filename, &source.layout.server_key_filename)
            .build(),
    )?;
    if cert_key_pairs.is_empty() {
        return Ok(None);
    }
    if cert_key_pairs.len() == 1
        && let Some((certs, key)) = cert_key_pairs.remove("default")
    {
        return Ok(Some(ServerTlsMaterial::SingleCert { certs, key }));
    }
    Ok(Some(ServerTlsMaterial::MultiCert { cert_key_pairs }))
}

fn combine_optional_pem(primary: Option<&[u8]>, fallback: Option<&[u8]>) -> Vec<u8> {
    let mut combined = Vec::new();

    for pem in [primary, fallback].into_iter().flatten() {
        if pem.iter().all(|&b| b.is_ascii_whitespace()) {
            continue;
        }
        combined.extend_from_slice(pem);
        if !combined.ends_with(b"\n") {
            combined.push(b'\n');
        }
    }

    combined
}

fn serialize_server_material_for_fingerprint(material: &ServerTlsMaterial) -> Vec<u8> {
    match material {
        ServerTlsMaterial::SingleCert { certs, key } => {
            let mut bytes = Vec::new();
            for cert in certs {
                bytes.extend_from_slice(cert.as_ref());
            }
            bytes.extend_from_slice(key.secret_der());
            bytes
        }
        ServerTlsMaterial::MultiCert { cert_key_pairs } => {
            let mut entries = cert_key_pairs.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(left, _)| *left);

            let mut bytes = Vec::new();
            for (domain, (certs, key)) in entries {
                bytes.extend_from_slice(domain.as_bytes());
                for cert in certs {
                    bytes.extend_from_slice(cert.as_ref());
                }
                bytes.extend_from_slice(key.secret_der());
            }
            bytes
        }
    }
}

pub(crate) fn server_material_fingerprint(material: &ServerTlsMaterial) -> TlsFingerprint {
    let bytes = serialize_server_material_for_fingerprint(material);
    TlsFingerprint::from_optional_bytes(Some(&bytes), None, None, None, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair};
    use std::fs;
    use tempfile::TempDir;

    fn write_pair(dir: &Path, domain: &str) {
        let pair = rcgen::generate_simple_self_signed(vec![domain.to_string()]).expect("generate certificate");
        fs::write(dir.join("rustfs_cert.pem"), pair.cert.pem()).expect("write certificate");
        fs::write(dir.join("rustfs_key.pem"), pair.signing_key.serialize_pem()).expect("write private key");
    }

    #[test]
    fn provided_server_identity_does_not_reread_server_files_and_preserves_trust() {
        let base = TempDir::new().expect("base source");
        let serving = rcgen::generate_simple_self_signed(vec!["example.com".into()]).expect("server identity");
        let trust = rcgen::generate_simple_self_signed(vec!["trust.example".into()]).expect("trust source");
        fs::write(base.path().join("rustfs_cert.pem"), "unusable server certificate").expect("poison base certificate");
        fs::write(base.path().join("rustfs_key.pem"), "unusable server key").expect("poison base key");
        fs::write(base.path().join(rustfs_config::RUSTFS_CA_CERT), trust.cert.pem()).expect("trust file");
        let source = TlsSource::from_directory(base.path()).snapshot().expect("source");
        let loaded = TlsMaterialSnapshot::load_from_snapshot_with_server_identity(
            &source,
            vec![serving.cert.der().clone()],
            serving.signing_key.serialize_pem().as_bytes(),
        )
        .expect("provided identity");
        assert_eq!(loaded.outbound.root_ca_pem, trust.cert.pem().as_bytes());
        assert!(
            matches!(loaded.server, Some(ServerTlsMaterial::SingleCert { certs, .. }) if certs == vec![serving.cert.der().clone()])
        );
        fs::write(base.path().join(rustfs_config::RUSTFS_CA_CERT), "corrupt trust").expect("corrupt independent trust");
        assert!(
            TlsMaterialSnapshot::load_from_snapshot_with_server_identity(
                &source,
                vec![serving.cert.der().clone()],
                serving.signing_key.serialize_pem().as_bytes()
            )
            .is_err(),
            "providing server material must not bypass trust validation"
        );
    }

    #[test]
    fn provided_server_identity_preserves_key_time_and_eku_validation() {
        let base = TempDir::new().expect("base source");
        let source = TlsSource::from_directory(base.path()).snapshot().expect("source");
        let serving = rcgen::generate_simple_self_signed(vec!["example.com".into()]).expect("server identity");
        let wrong_key = KeyPair::generate().expect("wrong key");
        assert!(
            TlsMaterialSnapshot::load_from_snapshot_with_server_identity(
                &source,
                vec![serving.cert.der().clone()],
                wrong_key.serialize_pem().as_bytes()
            )
            .is_err(),
            "key matching remains mandatory"
        );
        let mut expired = CertificateParams::new(vec!["example.com".into()]).expect("parameters");
        expired.not_before = rcgen::date_time_ymd(2000, 1, 1);
        expired.not_after = rcgen::date_time_ymd(2001, 1, 1);
        let cert = expired.self_signed(&wrong_key).expect("expired certificate");
        assert!(
            TlsMaterialSnapshot::load_from_snapshot_with_server_identity(
                &source,
                vec![cert.der().clone()],
                wrong_key.serialize_pem().as_bytes()
            )
            .is_err(),
            "serving validity remains mandatory"
        );
        let mut client = CertificateParams::new(vec!["example.com".into()]).expect("parameters");
        client.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let cert = client.self_signed(&wrong_key).expect("client certificate");
        assert!(
            TlsMaterialSnapshot::load_from_snapshot_with_server_identity(
                &source,
                vec![cert.der().clone()],
                wrong_key.serialize_pem().as_bytes()
            )
            .is_err(),
            "server EKU remains mandatory"
        );
    }

    #[tokio::test]
    async fn server_overlay_survives_expired_bootstrap_without_replacing_trust() {
        let base = TempDir::new().expect("base source");
        let overlay = TempDir::new().expect("server overlay");
        let key = KeyPair::generate().expect("bootstrap key");
        let mut params = CertificateParams::new(vec!["example.com".into()]).expect("bootstrap params");
        params.not_before = rcgen::date_time_ymd(2000, 1, 1);
        params.not_after = rcgen::date_time_ymd(2001, 1, 1);
        let expired = params.self_signed(&key).expect("expired bootstrap");
        fs::write(base.path().join("rustfs_cert.pem"), expired.pem()).expect("bootstrap certificate");
        fs::write(base.path().join("rustfs_key.pem"), key.serialize_pem()).expect("bootstrap key");
        let ca = rcgen::generate_simple_self_signed(vec!["trust.example".into()]).expect("trust source");
        fs::write(base.path().join(rustfs_config::RUSTFS_CA_CERT), ca.cert.pem()).expect("trust file");
        write_pair(overlay.path(), "example.com");
        let source = TlsSource::from_directory(base.path());
        assert!(
            TlsMaterialSnapshot::load(&source).await.is_err(),
            "the expired base alone must remain invalid"
        );
        let loaded = TlsMaterialSnapshot::load_from_snapshot_with_server_directory(
            &source.snapshot().expect("pinned base"),
            Some(overlay.path()),
        )
        .expect("valid overlay");
        assert_eq!(loaded.outbound.root_ca_pem, ca.cert.pem().as_bytes());
        assert!(matches!(loaded.server, Some(ServerTlsMaterial::SingleCert { .. })));
    }

    #[tokio::test]
    async fn snapshot_rejects_mismatched_default_private_key() {
        let dir = TempDir::new().expect("create temp directory");
        write_pair(dir.path(), "example.com");
        let other = KeyPair::generate().expect("generate other key");
        fs::write(dir.path().join("rustfs_key.pem"), other.serialize_pem()).expect("replace key");
        assert!(
            TlsMaterialSnapshot::load(&TlsSource::from_directory(dir.path()))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn snapshot_rejects_partial_sni_pair_even_with_valid_default() {
        let dir = TempDir::new().expect("create temp directory");
        write_pair(dir.path(), "example.com");
        let domain = dir.path().join("sni.example.com");
        fs::create_dir(&domain).expect("create SNI directory");
        fs::write(domain.join("rustfs_cert.pem"), "invalid certificate").expect("write partial SNI pair");
        assert!(
            TlsMaterialSnapshot::load(&TlsSource::from_directory(dir.path()))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn snapshot_rejects_invalid_existing_ca() {
        let dir = TempDir::new().expect("create temp directory");
        write_pair(dir.path(), "example.com");
        let source = TlsSource::from_directory(dir.path());
        fs::write(dir.path().join(&source.layout.public_ca_filename), "invalid CA").expect("write invalid CA");
        assert!(TlsMaterialSnapshot::load(&source).await.is_err());
    }

    #[tokio::test]
    async fn snapshot_rejects_incomplete_client_identity() {
        let dir = TempDir::new().expect("create temp directory");
        write_pair(dir.path(), "example.com");
        let source = TlsSource::from_directory(dir.path());
        fs::write(dir.path().join(&source.layout.client_cert_filename), "invalid client certificate").expect("write client cert");
        assert!(TlsMaterialSnapshot::load(&source).await.is_err());
    }

    #[tokio::test]
    async fn snapshot_rejects_expired_server_certificate() {
        let dir = TempDir::new().expect("create temp directory");
        let key = KeyPair::generate().expect("generate key");
        let mut params = CertificateParams::new(vec!["example.com".to_string()]).expect("certificate params");
        params.not_before = rcgen::date_time_ymd(2000, 1, 1);
        params.not_after = rcgen::date_time_ymd(2001, 1, 1);
        let cert = params.self_signed(&key).expect("generate expired certificate");
        fs::write(dir.path().join("rustfs_cert.pem"), cert.pem()).expect("write certificate");
        fs::write(dir.path().join("rustfs_key.pem"), key.serialize_pem()).expect("write key");
        assert!(
            TlsMaterialSnapshot::load(&TlsSource::from_directory(dir.path()))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn snapshot_rejects_client_only_server_eku() {
        let dir = TempDir::new().expect("create temp directory");
        let key = KeyPair::generate().expect("generate key");
        let mut params = CertificateParams::new(vec!["example.com".to_string()]).expect("certificate params");
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let cert = params.self_signed(&key).expect("generate client-only certificate");
        fs::write(dir.path().join("rustfs_cert.pem"), cert.pem()).expect("write certificate");
        fs::write(dir.path().join("rustfs_key.pem"), key.serialize_pem()).expect("write key");
        assert!(
            TlsMaterialSnapshot::load(&TlsSource::from_directory(dir.path()))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn snapshot_rejects_server_only_client_eku() {
        let dir = TempDir::new().expect("client identity directory");
        write_pair(dir.path(), "server.example");
        let key = KeyPair::generate().expect("client key");
        let mut params = CertificateParams::new(vec!["client.example".to_string()]).expect("client params");
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let cert = params.self_signed(&key).expect("server-only certificate");
        fs::write(dir.path().join("client_cert.pem"), cert.pem()).expect("client certificate");
        fs::write(dir.path().join("client_key.pem"), key.serialize_pem()).expect("client key");
        assert!(matches!(
            TlsMaterialSnapshot::load(&TlsSource::from_directory(dir.path())).await,
            Err(TlsRuntimeError::InvalidClientEku { .. })
        ));
    }
    #[tokio::test]
    async fn snapshot_rejects_future_server_certificate() {
        let dir = TempDir::new().expect("create directory");
        let key = KeyPair::generate().expect("generate key");
        let mut params = CertificateParams::new(vec!["example.com".to_string()]).expect("certificate params");
        params.not_before = rcgen::date_time_ymd(2100, 1, 1);
        params.not_after = rcgen::date_time_ymd(2101, 1, 1);
        let cert = params.self_signed(&key).expect("generate future certificate");
        fs::write(dir.path().join("rustfs_cert.pem"), cert.pem()).expect("write certificate");
        fs::write(dir.path().join("rustfs_key.pem"), key.serialize_pem()).expect("write key");
        assert!(matches!(
            TlsMaterialSnapshot::load(&TlsSource::from_directory(dir.path())).await,
            Err(TlsRuntimeError::CertificateValidity { .. })
        ));
    }

    #[tokio::test]
    async fn snapshot_rejects_wrong_sni_certificate_name() {
        let dir = TempDir::new().expect("create directory");
        write_pair(dir.path(), "example.com");
        let domain = dir.path().join("other.example.com");
        fs::create_dir(&domain).expect("create domain directory");
        write_pair(&domain, "wrong.example.com");
        assert!(
            TlsMaterialSnapshot::load(&TlsSource::from_directory(dir.path()))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn snapshot_rejects_malformed_complete_sni_pair() {
        let dir = TempDir::new().expect("create directory");
        write_pair(dir.path(), "example.com");
        let domain = dir.path().join("sni.example.com");
        fs::create_dir(&domain).expect("create domain directory");
        write_pair(&domain, "sni.example.com");
        fs::write(domain.join("rustfs_cert.pem"), "invalid certificate").expect("corrupt domain certificate");
        assert!(matches!(
            TlsMaterialSnapshot::load(&TlsSource::from_directory(dir.path())).await,
            Err(TlsRuntimeError::InvalidCertificate { .. })
        ));
    }

    #[tokio::test]
    async fn snapshot_rejects_cert_only_root() {
        let dir = TempDir::new().expect("create directory");
        write_pair(dir.path(), "example.com");
        fs::remove_file(dir.path().join("rustfs_key.pem")).expect("remove key");
        assert!(matches!(
            TlsMaterialSnapshot::load(&TlsSource::from_directory(dir.path())).await,
            Err(TlsRuntimeError::IncompleteIdentity { .. })
        ));
    }

    #[tokio::test]
    async fn snapshot_fingerprint_is_stable_and_certificate_only_renewal_changes_it() {
        let dir = TempDir::new().expect("create directory");
        write_pair(dir.path(), "example.com");
        let source = TlsSource::from_directory(dir.path());
        let initial = TlsMaterialSnapshot::load(&source)
            .await
            .expect("load valid initial generation");
        let unchanged = TlsMaterialSnapshot::load(&source).await.expect("load unchanged generation");
        assert_eq!(initial.fingerprint, unchanged.fingerprint);
        let key = KeyPair::from_pem(&fs::read_to_string(dir.path().join("rustfs_key.pem")).expect("read key"))
            .expect("parse existing key");
        let mut params = CertificateParams::new(vec!["example.com".to_string()]).expect("certificate params");
        params.serial_number = Some(123_u64.into());
        let renewed = params.self_signed(&key).expect("renew certificate with existing key");
        fs::write(dir.path().join("rustfs_cert.pem"), renewed.pem()).expect("write renewed certificate");
        let changed = TlsMaterialSnapshot::load(&source).await.expect("load renewed generation");
        assert_ne!(initial.fingerprint, changed.fingerprint);
    }

    #[tokio::test]
    async fn snapshot_server_client_ca_rotation_changes_fingerprint() {
        let dir = TempDir::new().expect("create directory");
        write_pair(dir.path(), "example.com");
        let source = TlsSource::from_directory(dir.path());
        let first_ca = rcgen::generate_simple_self_signed(vec!["first-ca".to_string()]).expect("first CA");
        fs::write(dir.path().join(&source.layout.client_ca_filename), first_ca.cert.pem()).expect("write first CA");
        let initial = TlsMaterialSnapshot::load(&source).await.expect("load first CA");
        let second_ca = rcgen::generate_simple_self_signed(vec!["second-ca".to_string()]).expect("second CA");
        fs::write(dir.path().join(&source.layout.client_ca_filename), second_ca.cert.pem()).expect("write second CA");
        let changed = TlsMaterialSnapshot::load(&source).await.expect("load second CA");
        assert_ne!(initial.fingerprint, changed.fingerprint);
        assert_eq!(changed.server_client_ca_pem.as_deref(), Some(second_ca.cert.pem().as_bytes()));
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_kubernetes_switch_loads_all_material_from_pinned_generation() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new().expect("create projection");
        let source = TlsSource::from_directory(dir.path());
        for generation in ["..a", "..b"] {
            let path = dir.path().join(generation);
            fs::create_dir(&path).expect("create generation");
            write_pair(&path, "example.com");
            let cert = fs::read(path.join(&source.layout.server_cert_filename)).expect("read generated certificate");
            let key = fs::read(path.join(&source.layout.server_key_filename)).expect("read generated key");
            for filename in [
                &source.layout.public_ca_filename,
                &source.fallback_ca_filename,
                &source.layout.client_ca_filename,
                &source.layout.client_cert_filename,
            ] {
                fs::write(path.join(filename), &cert).expect("write generation CA/client certificate");
            }
            fs::write(path.join(&source.layout.client_key_filename), key).expect("write generation client key");
        }
        symlink("..a", dir.path().join("..data")).expect("link initial generation");
        for filename in [
            &source.layout.server_cert_filename,
            &source.layout.server_key_filename,
            &source.layout.public_ca_filename,
            &source.fallback_ca_filename,
            &source.layout.client_ca_filename,
            &source.layout.client_cert_filename,
            &source.layout.client_key_filename,
        ] {
            symlink(format!("..data/{filename}"), dir.path().join(filename)).expect("link projected file");
        }
        let pinned = source.snapshot().expect("pin initial generation");
        let expected = TlsMaterialSnapshot::load_from_snapshot(&pinned).expect("load initial generation");
        symlink("..b", dir.path().join("..next")).expect("link next generation");
        fs::rename(dir.path().join("..next"), dir.path().join("..data")).expect("switch projection");
        let actual = TlsMaterialSnapshot::load_from_snapshot(&pinned).expect("load pinned generation after switch");
        assert_eq!(actual.fingerprint, expected.fingerprint);
        assert_eq!(actual.source.base_dir, pinned.base_dir());
        assert_eq!(actual.outbound.root_ca_pem, expected.outbound.root_ca_pem);
        assert_eq!(actual.server_client_ca_pem, expected.server_client_ca_pem);
        assert_eq!(
            actual.outbound.mtls_identity.expect("client identity").key_pem,
            expected.outbound.mtls_identity.expect("expected identity").key_pem
        );
        let current = TlsMaterialSnapshot::load_from_snapshot(&source.snapshot().expect("pin new generation"))
            .expect("load new generation");
        assert_ne!(actual.fingerprint, current.fingerprint);
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_rejects_disappeared_projection_source_and_dangling_optional_ca() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new().expect("create directory");
        let source = TlsSource::from_directory(dir.path());
        write_pair(dir.path(), "example.com");
        symlink("missing-ca", dir.path().join(&source.layout.public_ca_filename)).expect("link missing CA");
        let pinned = source.snapshot().expect("pin directory");
        assert!(matches!(
            TlsMaterialSnapshot::load_from_snapshot(&pinned),
            Err(TlsRuntimeError::SourceIo { .. })
        ));
        fs::remove_file(dir.path().join(&source.layout.public_ca_filename)).expect("remove CA link");
        fs::remove_dir_all(dir.path()).expect("remove pinned source");
        assert!(matches!(
            TlsMaterialSnapshot::load_from_snapshot(&pinned),
            Err(TlsRuntimeError::SourceIo { .. })
        ));
    }
    #[tokio::test]
    async fn snapshot_rejects_mismatched_sni_key_and_malformed_chain_member() {
        let dir = TempDir::new().expect("create directory");
        write_pair(dir.path(), "example.com");
        let domain = dir.path().join("sni.example.com");
        fs::create_dir(&domain).expect("create SNI directory");
        write_pair(&domain, "sni.example.com");
        let source = TlsSource::from_directory(dir.path());
        let valid = TlsMaterialSnapshot::load(&source)
            .await
            .expect("load valid multi certificate material");
        assert!(matches!(valid.server, Some(ServerTlsMaterial::MultiCert { ref cert_key_pairs }) if cert_key_pairs.len() == 2));
        fs::write(
            domain.join("rustfs_key.pem"),
            KeyPair::generate().expect("generate different key").serialize_pem(),
        )
        .expect("replace SNI key");
        assert!(matches!(
            TlsMaterialSnapshot::load(&source).await,
            Err(TlsRuntimeError::InvalidCertifiedKey { .. })
        ));
        write_pair(&domain, "sni.example.com");
        let cert_path = dir.path().join("rustfs_cert.pem");
        let mut cert = fs::read_to_string(&cert_path).expect("read leaf certificate");
        cert.push_str("-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n");
        fs::write(cert_path, cert).expect("append malformed chain member");
        assert!(matches!(
            TlsMaterialSnapshot::load(&source).await,
            Err(TlsRuntimeError::InvalidCertificate { .. })
        ));
    }

    #[tokio::test]
    async fn snapshot_rejects_invalid_private_key_pem_and_invalid_primary_client_ca() {
        let dir = TempDir::new().expect("create directory");
        write_pair(dir.path(), "example.com");
        let source = TlsSource::from_directory(dir.path());
        fs::write(dir.path().join("rustfs_key.pem"), "invalid key").expect("corrupt key");
        assert!(matches!(
            TlsMaterialSnapshot::load(&source).await,
            Err(TlsRuntimeError::InvalidPrivateKey { .. })
        ));
        write_pair(dir.path(), "example.com");
        let valid_ca = fs::read(dir.path().join("rustfs_cert.pem")).expect("read valid certificate");
        fs::write(dir.path().join(&source.fallback_ca_filename), valid_ca).expect("write valid fallback CA");
        fs::write(dir.path().join(&source.layout.client_ca_filename), "invalid primary CA").expect("write invalid primary CA");
        assert!(matches!(
            TlsMaterialSnapshot::load(&source).await,
            Err(TlsRuntimeError::InvalidCertificate { .. })
        ));
    }
}
