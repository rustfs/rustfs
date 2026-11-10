// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use rustls::{ServerConfig, server::ResolvesServerCert};
use std::{io, path::Path, sync::Arc};

/// Shared listener TLS policy: a configured client CA always requires client authentication.
pub(crate) fn server_config(resolver: Arc<dyn ResolvesServerCert>, ca_file: Option<&str>) -> io::Result<ServerConfig> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let builder = ServerConfig::builder();
    let builder = if let Some(ca_file) = ca_file {
        builder.with_client_cert_verifier(rustfs_tls_runtime::build_webpki_client_verifier_from_ca_file(Path::new(ca_file))?)
    } else {
        builder.with_no_client_auth()
    };
    Ok(builder.with_cert_resolver(resolver))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose};
    use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConnection, pki_types::PrivatePkcs8KeyDer};

    fn authority(name: &str) -> CertifiedIssuer<'static, KeyPair> {
        let mut params = CertificateParams::new(vec![name.to_owned()]).expect("CA names");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        CertifiedIssuer::self_signed(params, KeyPair::generate().expect("CA signing key")).expect("self-signed CA")
    }

    fn handshake(server: ServerConfig, client: ClientConfig) -> io::Result<()> {
        let mut server = ServerConnection::new(Arc::new(server)).map_err(io::Error::other)?;
        let mut client =
            ClientConnection::new(Arc::new(client), "localhost".try_into().expect("server name")).map_err(io::Error::other)?;
        for _ in 0..16 {
            let mut wire = Vec::new();
            client.write_tls(&mut wire)?;
            if !wire.is_empty() {
                server.read_tls(&mut std::io::Cursor::new(wire))?;
                server.process_new_packets().map_err(io::Error::other)?;
            }
            let mut wire = Vec::new();
            server.write_tls(&mut wire)?;
            if !wire.is_empty() {
                client.read_tls(&mut std::io::Cursor::new(wire))?;
                client.process_new_packets().map_err(io::Error::other)?;
            }
            if !server.is_handshaking() && !client.is_handshaking() {
                return Ok(());
            }
        }
        Err(io::Error::other("TLS handshake did not finish"))
    }

    #[test]
    fn protocol_listener_tls_requires_the_configured_client_ca() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let directory = tempfile::tempdir().expect("TLS directory");
        let trusted = authority("trusted-ca");
        let unrelated = authority("unrelated-ca");
        let server_key = KeyPair::generate().expect("server key");
        let mut params = CertificateParams::new(vec!["localhost".to_owned()]).expect("server names");
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_cert = params.signed_by(&server_key, &trusted).expect("server certificate");
        std::fs::write(directory.path().join("rustfs_cert.pem"), server_cert.pem()).expect("write server certificate");
        std::fs::write(directory.path().join("rustfs_key.pem"), server_key.serialize_pem()).expect("write server key");
        let ca_file = directory.path().join("clients.pem");
        std::fs::write(&ca_file, trusted.pem()).expect("write client CA");
        let resolver =
            rustfs_tls_runtime::ReloadableServerCertResolver::load_from_directory(directory.path().to_str().expect("directory"))
                .expect("production certificate resolver");
        for (client_issuer, allowed) in [(None, false), (Some(&unrelated), false), (Some(&trusted), true)] {
            let mut roots = RootCertStore::empty();
            roots.add(trusted.der().clone()).expect("trust server CA");
            let builder = ClientConfig::builder().with_root_certificates(roots);
            let client = if let Some(issuer) = client_issuer {
                let key = KeyPair::generate().expect("client key");
                let mut params = CertificateParams::new(Vec::new()).expect("client names");
                params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
                let cert = params.signed_by(&key, issuer).expect("client certificate");
                builder
                    .with_client_auth_cert(vec![cert.der().clone()], PrivatePkcs8KeyDer::from(key.serialize_der()).into())
                    .expect("client identity")
            } else {
                builder.with_no_client_auth()
            };
            let server = server_config(resolver.clone(), ca_file.to_str()).expect("production listener TLS policy");
            assert_eq!(handshake(server, client).is_ok(), allowed);
        }
        let mut roots = RootCertStore::empty();
        roots.add(trusted.der().clone()).expect("trust server CA");
        handshake(
            server_config(resolver.clone(), None).expect("ordinary TLS"),
            ClientConfig::builder().with_root_certificates(roots).with_no_client_auth(),
        )
        .expect("no CA preserves ordinary TLS");
        for bad in [
            "",
            "not a certificate",
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----",
        ] {
            std::fs::write(&ca_file, bad).expect("bad CA fixture");
            assert!(server_config(resolver.clone(), ca_file.to_str()).is_err(), "bad CA must fail startup");
        }
        std::fs::remove_file(&ca_file).expect("remove CA fixture");
        assert!(server_config(resolver, ca_file.to_str()).is_err());
    }

    #[tokio::test]
    async fn protocol_listeners_refuse_startup_with_an_invalid_client_ca() {
        let directory = tempfile::tempdir().expect("TLS directory");
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("server identity");
        std::fs::write(directory.path().join("rustfs_cert.pem"), certified.cert.pem()).expect("server certificate");
        std::fs::write(directory.path().join("rustfs_key.pem"), certified.signing_key.serialize_pem()).expect("server key");
        let ca_file = directory.path().join("clients.pem");
        std::fs::write(&ca_file, "not a CA certificate").expect("invalid client CA fixture");
        let (_shutdown, shutdown) = tokio::sync::broadcast::channel(1);
        #[cfg(feature = "webdav")]
        {
            let config = crate::webdav::config::WebDavConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind"),
                tls_enabled: true,
                cert_dir: Some(directory.path().to_string_lossy().into_owned()),
                ca_file: Some(ca_file.to_string_lossy().into_owned()),
                ..Default::default()
            };
            let server = crate::webdav::server::WebDavServer::new(config, crate::common::dummy_storage::DummyBackend::default())
                .await
                .expect("path checks");
            let error = tokio::time::timeout(std::time::Duration::from_secs(5), server.start(shutdown.resubscribe()))
                .await
                .expect("startup deadline")
                .expect_err("invalid CA must not serve WebDAV");
            assert!(matches!(error, crate::webdav::config::WebDavInitError::Tls(_)));
        }
        #[cfg(feature = "ftps")]
        {
            let config = crate::ftps::config::FtpsConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind"),
                tls_enabled: true,
                cert_dir: Some(directory.path().to_string_lossy().into_owned()),
                ca_file: Some(ca_file.to_string_lossy().into_owned()),
                ..Default::default()
            };
            let server = crate::ftps::server::FtpsServer::new(config, crate::common::dummy_storage::DummyBackend::default())
                .await
                .expect("path checks");
            let error = tokio::time::timeout(std::time::Duration::from_secs(5), server.start(shutdown.resubscribe()))
                .await
                .expect("startup deadline")
                .expect_err("invalid CA must not serve FTPS");
            assert!(matches!(error, crate::ftps::config::FtpsInitError::InvalidConfig(_)));
        }
    }

    #[tokio::test]
    async fn protocol_client_ca_configuration_cannot_allow_cleartext() {
        let directory = tempfile::tempdir().expect("config directory");
        let ca_file = directory.path().join("clients.pem");
        std::fs::write(&ca_file, "test").expect("existing CA path");
        #[cfg(feature = "webdav")]
        {
            let config = crate::webdav::config::WebDavConfig {
                tls_enabled: false,
                cert_dir: Some(directory.path().to_string_lossy().into_owned()),
                ca_file: Some(ca_file.to_string_lossy().into_owned()),
                ..Default::default()
            };
            assert!(config.validate().await.is_err());
        }
        #[cfg(feature = "ftps")]
        {
            let config = crate::ftps::config::FtpsConfig {
                tls_enabled: false,
                cert_dir: Some(directory.path().to_string_lossy().into_owned()),
                ca_file: Some(ca_file.to_string_lossy().into_owned()),
                ..Default::default()
            };
            assert!(config.validate().await.is_err());
        }
    }
}
