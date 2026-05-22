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

use crate::certs::{CertDirectoryLoadOptions, load_all_certs_from_directory, load_certs, load_private_key};
use crate::error::TlsRuntimeError;
use crate::fingerprint::TlsFingerprint;
use crate::source::TlsSource;
use rustfs_common::MtlsIdentityPem;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::collections::HashMap;
use std::io::Cursor;
use std::io::ErrorKind;
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

#[derive(Debug)]
pub struct TlsMaterialSnapshot {
    pub source: TlsSource,
    pub server: Option<ServerTlsMaterial>,
    pub outbound: OutboundTlsMaterial,
    pub fingerprint: TlsFingerprint,
}

impl TlsMaterialSnapshot {
    pub async fn load(source: &TlsSource) -> Result<Self, TlsRuntimeError> {
        let base_dir = source.validate_directory()?.to_path_buf();

        let server = load_server_material(source, &base_dir)?;

        let public_ca_path = base_dir.join(&source.layout.public_ca_filename);
        let client_ca_path = base_dir.join(&source.fallback_ca_filename);
        let client_cert_path = base_dir.join(&source.layout.client_cert_filename);
        let client_key_path = base_dir.join(&source.layout.client_key_filename);

        let public_ca_pem = tokio::fs::read(&public_ca_path).await.ok();
        let client_ca_pem = tokio::fs::read(&client_ca_path).await.ok();
        let root_ca_pem = combine_optional_pem(public_ca_pem.as_deref(), client_ca_pem.as_deref());

        let mtls_identity = match (
            tokio::fs::read(&client_cert_path).await.ok(),
            tokio::fs::read(&client_key_path).await.ok(),
        ) {
            (Some(cert_pem), Some(key_pem)) => {
                let mut cert_reader = Cursor::new(&cert_pem);
                if CertificateDer::pem_reader_iter(&mut cert_reader).next().is_none() {
                    return Err(TlsRuntimeError::Material("no valid certificate in client cert PEM".to_string()));
                }

                let mut key_reader = Cursor::new(&key_pem);
                PrivateKeyDer::from_pem_reader(&mut key_reader)
                    .map_err(|e| TlsRuntimeError::Material(format!("invalid client key PEM: {e}")))?;

                Some(MtlsIdentityPem { cert_pem, key_pem })
            }
            _ => None,
        };

        let outbound = OutboundTlsMaterial {
            root_ca_pem: root_ca_pem.clone(),
            mtls_identity: mtls_identity.clone(),
        };

        let server_fingerprint_bytes = server.as_ref().map(serialize_server_material_for_fingerprint);
        let fingerprint = TlsFingerprint::from_optional_bytes(
            server_fingerprint_bytes.as_deref(),
            public_ca_pem.as_deref(),
            client_ca_pem.as_deref(),
            mtls_identity.as_ref().map(|identity| identity.cert_pem.as_slice()),
            mtls_identity.as_ref().map(|identity| identity.key_pem.as_slice()),
        );

        Ok(Self {
            source: source.clone(),
            server,
            outbound,
            fingerprint,
        })
    }
}

fn load_server_material(source: &TlsSource, base_dir: &Path) -> Result<Option<ServerTlsMaterial>, TlsRuntimeError> {
    let root_cert = base_dir.join(&source.layout.server_cert_filename);
    let root_key = base_dir.join(&source.layout.server_key_filename);

    let has_root_pair = root_cert.exists() && root_key.exists();
    let cert_key_pairs = load_all_certs_from_directory(
        CertDirectoryLoadOptions::builder(base_dir, &source.layout.server_cert_filename, &source.layout.server_key_filename)
            .build(),
    );

    match cert_key_pairs {
        Ok(cert_key_pairs) if cert_key_pairs.len() > 1 || cert_key_pairs.keys().any(|key| key != "default") => {
            Ok(Some(ServerTlsMaterial::MultiCert { cert_key_pairs }))
        }
        Ok(cert_key_pairs) if !cert_key_pairs.is_empty() => {
            if let Some((certs, key)) = cert_key_pairs.get("default") {
                return Ok(Some(ServerTlsMaterial::SingleCert {
                    certs: certs.clone(),
                    key: key.clone_key(),
                }));
            }
            Ok(Some(ServerTlsMaterial::MultiCert { cert_key_pairs }))
        }
        Ok(_) => Ok(None),
        Err(_err) if has_root_pair => {
            let root_cert_path = path_to_utf8_str(&root_cert, "root TLS certificate")?;
            let root_key_path = path_to_utf8_str(&root_key, "root TLS private key")?;
            let certs = load_certs(root_cert_path)
                .map_err(|e| TlsRuntimeError::Material(format!("load root TLS certificate {}: {e}", root_cert.display())))?;
            let key = load_private_key(root_key_path)
                .map_err(|e| TlsRuntimeError::Material(format!("load root TLS private key {}: {e}", root_key.display())))?;
            Ok(Some(ServerTlsMaterial::SingleCert { certs, key }))
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(TlsRuntimeError::Material(format!(
            "discover server TLS certificates under '{}': {err}",
            base_dir.display()
        ))),
    }
}

fn path_to_utf8_str<'a>(path: &'a Path, description: &str) -> Result<&'a str, TlsRuntimeError> {
    path.to_str()
        .ok_or_else(|| TlsRuntimeError::Material(format!("{description} path '{}' is not valid UTF-8", path.display())))
}

fn combine_optional_pem(primary: Option<&[u8]>, fallback: Option<&[u8]>) -> Vec<u8> {
    let mut combined = Vec::new();

    if let Some(primary) = primary {
        combined.extend_from_slice(primary);
        if !combined.ends_with(b"\n") {
            combined.push(b'\n');
        }
    }

    if let Some(fallback) = fallback {
        combined.extend_from_slice(fallback);
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
