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

//! Core FTPS tests
//!
//! # Security regression coverage
//!
//! GHSA-3p3x-734c-h5vx (constant-time secret comparison on the WebDAV/FTPS
//! password login path, fixed in rustfs/rustfs#4403) is anchored end-to-end
//! by [`assert_ftps_ghsa_3p3x_wrong_credentials_rejected`], invoked from
//! [`test_ftps_core_operations`]. It exercises the `ct_eq` rejection branch in
//! `FtpsAuthenticator::authenticate` (`crates/protocols/src/ftps/server.rs`),
//! proving that a wrong password and an unknown user are both rejected and are
//! indistinguishable at the protocol layer (both return FTP `530 Not logged
//! in`). The sibling WebDAV assertion lives in `webdav_core.rs`; the internode
//! RPC fail-closed advisory (GHSA-r5qv-rc46-hv8q, rustfs/rustfs#4402) is
//! anchored by the `ghsa_r5qv_*` unit tests in
//! `crates/ecstore/src/cluster/rpc/http_auth.rs`. See
//! `docs/testing/security-regressions.md` for the full advisory -> test map.
//!
//! Advisory: <https://github.com/rustfs/rustfs/security/advisories/GHSA-3p3x-734c-h5vx>

use crate::common::rustfs_binary_path_with_features;
use crate::protocols::test_env::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, ProtocolTestEnvironment};
use anyhow::Result;
use rcgen::generate_simple_self_signed;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use suppaftp::types::Response;
use suppaftp::{FtpError, RustlsConnector, RustlsFtpStream, Status};
use tokio::process::Command;
use tracing::info;

// Fixed FTPS port for testing
const FTPS_PORT: u16 = 9021;
const FTPS_ADDRESS: &str = "127.0.0.1:9021";

#[derive(Debug)]
struct AcceptAnyServerCertVerifier;

impl ServerCertVerifier for AcceptAnyServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Open a fresh, unauthenticated FTPS control connection to the test server
/// and upgrade it to TLS. The caller is responsible for logging in.
///
/// Assumes the process-wide rustls crypto provider has already been installed
/// (done once at the top of [`test_ftps_core_operations`]).
fn ftps_connect_secure() -> Result<RustlsFtpStream> {
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCertVerifier))
        .with_no_client_auth();

    let tls_connector = RustlsConnector::from(Arc::new(config));

    let ftp_stream = RustlsFtpStream::connect(FTPS_ADDRESS).map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?;

    ftp_stream
        .into_secure(tls_connector, "127.0.0.1")
        .map_err(|e| anyhow::anyhow!("Failed to upgrade to TLS: {}", e))
}

/// Extract the FTP reply status from a failed login, asserting the failure is a
/// protocol-level rejection (`FtpError::UnexpectedResponse`) rather than a
/// transport error.
fn login_rejection_status(user: &str, password: &str) -> Result<Status> {
    let mut ftp_stream = ftps_connect_secure()?;
    match ftp_stream.login(user, password) {
        Ok(()) => anyhow::bail!("login unexpectedly succeeded for user '{user}'"),
        Err(FtpError::UnexpectedResponse(Response { status, .. })) => Ok(status),
        Err(other) => anyhow::bail!("expected a protocol rejection, got transport error: {other}"),
    }
}

/// Security regression for GHSA-3p3x-734c-h5vx.
///
/// FTPS password verification must reject invalid credentials via the
/// constant-time `ct_eq` branch in `FtpsAuthenticator::authenticate`
/// (`crates/protocols/src/ftps/server.rs`, fixed in rustfs/rustfs#4403). This
/// asserts, purely on behavior (no timing assertions):
///
/// 1. A valid user with a wrong password is rejected (`530`), exercising the
///    secret-mismatch branch that the advisory hardened.
/// 2. An unknown user is rejected (`530`).
/// 3. The two failures are indistinguishable at the protocol layer — both
///    return the same FTP status, so an attacker cannot use the reply code to
///    tell "user exists, wrong password" from "no such user".
async fn assert_ftps_ghsa_3p3x_wrong_credentials_rejected() -> Result<()> {
    info!("Testing FTPS (GHSA-3p3x): wrong password is rejected");
    let wrong_password_status = login_rejection_status(DEFAULT_ACCESS_KEY, "definitely-not-the-secret")?;
    assert_eq!(
        wrong_password_status,
        Status::NotLoggedIn,
        "valid user + wrong password must be rejected with 530, got {wrong_password_status:?}"
    );
    info!("PASS: FTPS wrong password rejected with 530");

    info!("Testing FTPS (GHSA-3p3x): unknown user is rejected");
    let unknown_user_status = login_rejection_status("no-such-access-key", DEFAULT_SECRET_KEY)?;
    assert_eq!(
        unknown_user_status,
        Status::NotLoggedIn,
        "unknown user must be rejected with 530, got {unknown_user_status:?}"
    );
    info!("PASS: FTPS unknown user rejected with 530");

    assert_eq!(
        wrong_password_status, unknown_user_status,
        "invalid-user and invalid-password failures must be indistinguishable at the protocol layer"
    );
    info!("PASS: FTPS invalid-user and invalid-password failures are indistinguishable");
    Ok(())
}

/// Test FTPS: put, ls, mkdir, rmdir, delete operations
pub async fn test_ftps_core_operations() -> Result<()> {
    let env = ProtocolTestEnvironment::new().map_err(|e| anyhow::anyhow!("{}", e))?;

    let cert_dir = PathBuf::from(&env.temp_dir).join("ftps_certs");
    tokio::fs::create_dir_all(&cert_dir).await?;

    // Generate default certificate for root directory
    let default_cert = generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
    let default_cert_path = cert_dir.join("rustfs_cert.pem");
    let default_key_path = cert_dir.join("rustfs_key.pem");
    tokio::fs::write(&default_cert_path, default_cert.cert.pem()).await?;
    tokio::fs::write(&default_key_path, default_cert.signing_key.serialize_pem()).await?;

    // Create subdirectory for domain-specific certificate
    let example_domain_dir = cert_dir.join("example1.com");
    tokio::fs::create_dir_all(&example_domain_dir).await?;
    let domain_cert = generate_simple_self_signed(vec!["example1.com".to_string()])?;
    let domain_cert_path = example_domain_dir.join("rustfs_cert.pem");
    let domain_key_path = example_domain_dir.join("rustfs_key.pem");
    tokio::fs::write(&domain_cert_path, domain_cert.cert.pem()).await?;
    tokio::fs::write(&domain_key_path, domain_cert.signing_key.serialize_pem()).await?;

    info!("Generated 2 certificates in {:?}", cert_dir);

    // Start server manually
    info!("Starting FTPS server on {}", FTPS_ADDRESS);
    let binary_path = rustfs_binary_path_with_features(Some("ftps,webdav"));
    let mut server_process = Command::new(&binary_path)
        .env("RUSTFS_CONSOLE_ENABLE", "false")
        .env("RUSTFS_FTPS_ENABLE", "true")
        .env("RUSTFS_FTPS_ADDRESS", FTPS_ADDRESS)
        .env("RUSTFS_FTPS_CERTS_DIR", cert_dir.to_str().unwrap())
        .arg(&env.temp_dir)
        .spawn()?;

    // Ensure server is cleaned up even on failure
    let result = async {
        // Wait for server to be ready
        ProtocolTestEnvironment::wait_for_port_ready(FTPS_PORT, 30)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Install the default crypto provider once for this process before any
        // TLS handshake. Subsequent connections reuse it via `ftps_connect_secure`.
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .map_err(|e| anyhow::anyhow!("Failed to install crypto provider: {:?}", e))?;

        // Security regression (GHSA-3p3x-734c-h5vx): wrong credentials must be
        // rejected before any successful login is attempted below.
        assert_ftps_ghsa_3p3x_wrong_credentials_rejected().await?;

        // Connect and log in with valid credentials for the functional flow.
        let mut ftp_stream = ftps_connect_secure()?;
        ftp_stream.login(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)?;

        info!("Testing FTPS: mkdir bucket");
        let bucket_name = "testbucket";
        ftp_stream.mkdir(bucket_name)?;
        info!("PASS: mkdir bucket '{}' successful", bucket_name);

        info!("Testing FTPS: cd to bucket");
        ftp_stream.cwd(bucket_name)?;
        info!("PASS: cd to bucket '{}' successful", bucket_name);

        info!("Testing FTPS: put file");
        let filename = "test.txt";
        let content = "Hello, FTPS!";
        ftp_stream.put_file(filename, &mut Cursor::new(content.as_bytes()))?;
        info!("PASS: put file '{}' ({} bytes) successful", filename, content.len());

        info!("Testing FTPS: download file");
        let downloaded_content = ftp_stream.retr(filename, |stream| {
            let mut buffer = Vec::new();
            stream.read_to_end(&mut buffer).map_err(suppaftp::FtpError::ConnectionError)?;
            Ok(buffer)
        })?;
        let downloaded_str = String::from_utf8(downloaded_content)?;
        assert_eq!(downloaded_str, content, "Downloaded content should match uploaded content");
        info!("PASS: download file '{}' successful, content matches", filename);

        info!("Testing FTPS: ls list objects in bucket");
        let list = ftp_stream.list(None)?;
        assert!(list.iter().any(|line| line.contains(filename)), "File should appear in list");
        info!("PASS: ls command successful, file '{}' found in bucket", filename);

        info!("Testing FTPS: ls . (list current directory)");
        let list_dot = ftp_stream.list(Some(".")).unwrap_or_else(|_| ftp_stream.list(None).unwrap());
        assert!(list_dot.iter().any(|line| line.contains(filename)), "File should appear in ls .");
        info!("PASS: ls . successful, file '{}' found", filename);

        info!("Testing FTPS: ls / (list root directory)");
        let list_root = ftp_stream.list(Some("/")).unwrap();
        assert!(list_root.iter().any(|line| line.contains(bucket_name)), "Bucket should appear in ls /");
        assert!(!list_root.iter().any(|line| line.contains(filename)), "File should not appear in ls /");
        info!(
            "PASS: ls / successful, bucket '{}' found, file '{}' not found in root",
            bucket_name, filename
        );

        info!("Testing FTPS: ls /. (list root directory with /.)");
        let list_root_dot = ftp_stream
            .list(Some("/."))
            .unwrap_or_else(|_| ftp_stream.list(Some("/")).unwrap());
        assert!(
            list_root_dot.iter().any(|line| line.contains(bucket_name)),
            "Bucket should appear in ls /."
        );
        info!("PASS: ls /. successful, bucket '{}' found", bucket_name);

        info!("Testing FTPS: ls /bucket (list bucket by absolute path)");
        let list_bucket = ftp_stream.list(Some(&format!("/{}", bucket_name))).unwrap();
        assert!(list_bucket.iter().any(|line| line.contains(filename)), "File should appear in ls /bucket");
        info!("PASS: ls /{} successful, file '{}' found", bucket_name, filename);

        info!("Testing FTPS: cd . (stay in current directory)");
        ftp_stream.cwd(".")?;
        info!("PASS: cd . successful (stays in current directory)");

        info!("Testing FTPS: ls after cd . (should still see file)");
        let list_after_dot = ftp_stream.list(None)?;
        assert!(
            list_after_dot.iter().any(|line| line.contains(filename)),
            "File should still appear in list after cd ."
        );
        info!("PASS: ls after cd . successful, file '{}' still found in bucket", filename);

        info!("Testing FTPS: cd / (go to root directory)");
        ftp_stream.cwd("/")?;
        info!("PASS: cd / successful (back to root directory)");

        info!("Testing FTPS: ls after cd / (should see bucket only)");
        let root_list_after = ftp_stream.list(None)?;
        assert!(
            !root_list_after.iter().any(|line| line.contains(filename)),
            "File should not appear in root ls"
        );
        assert!(
            root_list_after.iter().any(|line| line.contains(bucket_name)),
            "Bucket should appear in root ls"
        );
        info!("PASS: ls after cd / successful, file not in root, bucket '{}' found in root", bucket_name);

        info!("Testing FTPS: cd back to bucket");
        ftp_stream.cwd(bucket_name)?;
        info!("PASS: cd back to bucket '{}' successful", bucket_name);

        info!("Testing FTPS: delete object");
        ftp_stream.rm(filename)?;
        info!("PASS: delete object '{}' successful", filename);

        info!("Testing FTPS: ls verify object deleted");
        let list_after = ftp_stream.list(None)?;
        assert!(!list_after.iter().any(|line| line.contains(filename)), "File should be deleted");
        info!("PASS: ls after delete successful, file '{}' is not found", filename);

        info!("Testing FTPS: cd up to root directory");
        ftp_stream.cdup()?;
        info!("PASS: cd up to root directory successful");

        info!("Testing FTPS: cd to nonexistent bucket (should fail)");
        let nonexistent_bucket = "nonexistent-bucket";
        let cd_result = ftp_stream.cwd(nonexistent_bucket);
        assert!(cd_result.is_err(), "cd to nonexistent bucket should fail");
        info!("PASS: cd to nonexistent bucket '{}' failed as expected", nonexistent_bucket);

        info!("Testing FTPS: ls verify bucket exists in root");
        let root_list = ftp_stream.list(None)?;
        assert!(root_list.iter().any(|line| line.contains(bucket_name)), "Bucket should exist in root");
        info!("PASS: ls root successful, bucket '{}' found in root", bucket_name);

        info!("Testing FTPS: rmdir delete bucket");
        ftp_stream.rmdir(bucket_name)?;
        info!("PASS: rmdir bucket '{}' successful", bucket_name);

        info!("Testing FTPS: ls verify bucket deleted");
        let root_list_after = ftp_stream.list(None)?;
        assert!(!root_list_after.iter().any(|line| line.contains(bucket_name)), "Bucket should be deleted");
        info!("PASS: ls root after delete successful, bucket '{}' is not found", bucket_name);

        ftp_stream.quit()?;

        info!("FTPS core tests passed");
        Ok(())
    }
    .await;

    // Always cleanup server process
    let _ = server_process.kill().await;
    let _ = server_process.wait().await;

    result
}
