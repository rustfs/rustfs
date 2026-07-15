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

//! Live-listener TLS certificate hot-reload e2e (backlog#1154 peri-5).
//!
//! `crates/tls-runtime` had ~20 unit tests but no test ever rotated a
//! certificate under a listening HTTPS server, so the operational promise
//! "swap certificates without a restart" (RUSTFS_TLS_RELOAD_ENABLE) had no
//! automated proof. This suite pins, against a real binary over real TLS
//! handshakes:
//!
//!   * the server starts with certificate A and serves its fingerprint;
//!   * after the on-disk material is replaced with certificate B, new
//!     connections receive B within the reload interval — no restart;
//!   * a connection established under A keeps working across the swap
//!     (existing sessions are not torn down);
//!   * replacing the material with garbage does not take down the listener or
//!     the previously loaded certificate (fail-safe, no fail-open) and leaves
//!     an assertable `tls_reload_failed` log event.

use crate::common::{RustFSTestEnvironment, init_logging};
use rcgen::generate_simple_self_signed;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ClientConnection, DigitallySignedStruct, Error as RustlsError, SignatureScheme, StreamOwned};
use serial_test::serial;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;
type BoxError = Box<dyn Error + Send + Sync>;

const CERT_FILE: &str = "rustfs_cert.pem";
const KEY_FILE: &str = "rustfs_key.pem";
/// Minimum value the server accepts for RUSTFS_TLS_RELOAD_INTERVAL (seconds).
const RELOAD_INTERVAL_SECS: u64 = 5;

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

fn tls_client_config() -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCertVerifier))
            .with_no_client_auth(),
    )
}

/// A live TLS session over a blocking TCP stream, kept open across reloads.
struct TlsSession {
    stream: StreamOwned<ClientConnection, TcpStream>,
    fingerprint: String,
    host: String,
}

/// Completes a TLS handshake with `addr` and returns the session together with
/// the SHA-256 fingerprint of the served leaf certificate.
fn tls_connect(addr: &str) -> Result<TlsSession, BoxError> {
    let host = addr.split(':').next().unwrap_or("127.0.0.1").to_string();
    let server_name = ServerName::try_from(host.clone())?;
    let mut conn = ClientConnection::new(tls_client_config(), server_name)?;
    let mut tcp = TcpStream::connect(addr)?;
    tcp.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
    tcp.set_write_timeout(Some(std::time::Duration::from_secs(10)))?;

    // Drive the handshake to completion so peer_certificates() is populated.
    while conn.is_handshaking() {
        conn.complete_io(&mut tcp)?;
    }

    let leaf = conn
        .peer_certificates()
        .and_then(|certs| certs.first())
        .ok_or("server presented no certificate")?;
    let fingerprint = Sha256::digest(leaf.as_ref())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();

    Ok(TlsSession {
        stream: StreamOwned::new(conn, tcp),
        fingerprint,
        host,
    })
}

/// Sends a keep-alive HTTP request on the session and reads the response head,
/// proving the TLS session is still fully functional.
fn http_roundtrip(session: &mut TlsSession) -> Result<String, BoxError> {
    let request = format!("GET / HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\n\r\n", session.host);
    session.stream.write_all(request.as_bytes())?;
    session.stream.flush()?;

    // Read up to the end of the headers plus a body chunk; anonymous ListBuckets
    // answers a small error payload, which is fine — we only need a valid
    // HTTP status line over the existing TLS session.
    let mut buf = vec![0_u8; 8192];
    let read = session.stream.read(&mut buf)?;
    if read == 0 {
        return Err("connection closed by server".into());
    }
    let head = String::from_utf8_lossy(&buf[..read]).to_string();
    if !head.starts_with("HTTP/1.1") {
        return Err(format!("unexpected response head: {head}").into());
    }
    Ok(head)
}

async fn write_cert_pair(tls_dir: &std::path::Path, sans: Vec<String>) -> Result<(), BoxError> {
    let cert = generate_simple_self_signed(sans)?;
    tokio::fs::write(tls_dir.join(CERT_FILE), cert.cert.pem()).await?;
    tokio::fs::write(tls_dir.join(KEY_FILE), cert.signing_key.serialize_pem()).await?;
    Ok(())
}

/// Polls new TLS connections until the served fingerprint changes away from
/// `old_fingerprint`, returning the new one.
async fn wait_for_new_fingerprint(addr: &str, old_fingerprint: &str, within: Duration) -> Result<String, BoxError> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let addr_owned = addr.to_string();
        let observed = tokio::task::spawn_blocking(move || tls_connect(&addr_owned).map(|s| s.fingerprint)).await?;
        if let Ok(fp) = observed
            && fp != old_fingerprint
        {
            return Ok(fp);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("served certificate never rotated away from {old_fingerprint} within {within:?}").into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

/// Starts the rustfs binary with HTTPS + hot reload enabled, capturing its
/// stdout/stderr to `log_path`. The harness's own start path is unusable here:
/// its readiness probe drives the AWS SDK over the environment URL, and the SDK
/// rejects the self-signed test certificate.
async fn start_https_server_with_reload(
    env: &mut RustFSTestEnvironment,
    tls_dir: &std::path::Path,
    log_path: &str,
) -> TestResult {
    let log_file = std::fs::OpenOptions::new().create(true).append(true).open(log_path)?;
    let log_file_err = log_file.try_clone()?;
    let process = std::process::Command::new(crate::common::rustfs_binary_path())
        .env("RUST_LOG", "rustfs=info")
        .env("RUSTFS_CONSOLE_ENABLE", "false")
        .env("RUSTFS_TLS_PATH", tls_dir)
        .env("RUSTFS_TLS_RELOAD_ENABLE", "true")
        .env("RUSTFS_TLS_RELOAD_INTERVAL", RELOAD_INTERVAL_SECS.to_string())
        .stdout(std::process::Stdio::from(log_file))
        .stderr(std::process::Stdio::from(log_file_err))
        .args([
            "--address",
            &env.address,
            "--access-key",
            &env.access_key,
            "--secret-key",
            &env.secret_key,
            &env.temp_dir,
        ])
        .spawn()?;
    env.process = Some(process);

    // Readiness = a TLS handshake completes on the listener.
    let addr = env.address.clone();
    for attempt in 0..60 {
        let addr_clone = addr.clone();
        if tokio::task::spawn_blocking(move || tls_connect(&addr_clone)).await?.is_ok() {
            return Ok(());
        }
        if attempt == 59 {
            return Err("HTTPS server never completed a TLS handshake within 60s".into());
        }
        sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

/// Runs `http_roundtrip` on a session inside the blocking pool, handing the
/// session back for later reuse (the point: the same TLS session survives).
async fn roundtrip_and_return(mut session: TlsSession) -> Result<TlsSession, BoxError> {
    tokio::task::spawn_blocking(move || {
        http_roundtrip(&mut session)?;
        Ok::<_, BoxError>(session)
    })
    .await?
}

#[tokio::test]
#[serial]
async fn test_tls_certificate_hot_reload_live_listener() -> TestResult {
    init_logging();
    // Install the process-wide rustls crypto provider (idempotent).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let mut env = RustFSTestEnvironment::new().await?;
    let tls_dir = std::path::PathBuf::from(format!("{}/tls", env.temp_dir));
    tokio::fs::create_dir_all(&tls_dir).await?;
    write_cert_pair(&tls_dir, vec!["localhost".into(), "127.0.0.1".into()]).await?;

    let log_path = format!("{}/server.log", env.temp_dir);
    start_https_server_with_reload(&mut env, &tls_dir, &log_path).await?;

    let addr = env.address.clone();

    // --- certificate A is served, and the session stays usable ---------------
    let session_a = {
        let addr = addr.clone();
        tokio::task::spawn_blocking(move || tls_connect(&addr)).await??
    };
    let fingerprint_a = session_a.fingerprint.clone();
    let session_a = roundtrip_and_return(session_a).await?;

    // --- swap to certificate B: new connections pick it up without restart ---
    write_cert_pair(&tls_dir, vec!["localhost".into(), "127.0.0.1".into()]).await?;
    let reload_budget = Duration::from_secs(RELOAD_INTERVAL_SECS * 4 + 10);
    let fingerprint_b = wait_for_new_fingerprint(&addr, &fingerprint_a, reload_budget).await?;
    assert_ne!(fingerprint_a, fingerprint_b);

    // The connection opened under certificate A must survive the rotation.
    let _session_a = roundtrip_and_return(session_a).await?;

    // --- garbage material: fail-safe, keep serving B, log the failure --------
    tokio::fs::write(tls_dir.join(CERT_FILE), b"not a certificate").await?;
    tokio::fs::write(tls_dir.join(KEY_FILE), b"not a key").await?;

    // Give the reload loop at least two ticks to observe the bad material.
    sleep(Duration::from_secs(RELOAD_INTERVAL_SECS * 2 + 2)).await;

    let after_bad = {
        let addr = addr.clone();
        tokio::task::spawn_blocking(move || tls_connect(&addr)).await??
    };
    assert_eq!(
        after_bad.fingerprint, fingerprint_b,
        "bad on-disk material must not change or drop the served certificate"
    );

    let log = tokio::fs::read_to_string(&log_path).await.unwrap_or_default();
    assert!(
        log.contains("tls_reload_failed") || log.contains("TLS reload failed"),
        "a failed reload must leave an assertable log event; captured log did not contain one"
    );

    // The process must still be alive and serving.
    let final_session = tokio::task::spawn_blocking(move || tls_connect(&addr)).await??;
    let _ = roundtrip_and_return(final_session).await?;

    env.stop_server();
    Ok(())
}
