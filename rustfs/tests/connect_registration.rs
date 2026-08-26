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

use std::collections::VecDeque;
use std::fs;
use std::io::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64_simd::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD as BASE64_URL_NO_PAD};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use p256::ecdsa::signature::Verifier as _;
use p256::ecdsa::{Signature, VerifyingKey};
use p256::pkcs8::DecodePublicKey as _;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType, SerialNumber,
};
use rustfs::connect::{
    ClientError, CoarseNodeSummary, ConnectClient, ConnectConfig, CredentialStore, DeviceIdentity, HeartbeatConfig,
    HeartbeatError, HeartbeatSchedule, HeartbeatStatus, IdentityStore, RegistrationToken, TokenError, spawn_heartbeat_runtime,
};
#[cfg(target_os = "linux")]
use rustfs::connect::{InventorySchedule, InventorySnapshot, InventoryStatus, spawn_inventory_runtime};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, pem::PemObject as _};
use rustls::server::WebPkiClientVerifier;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::net::TcpListener;
use tokio::sync::{Notify, watch};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "connect-e2e-short-credentials")]
const EXPECTED_CERTIFICATE_LIFETIME_SECONDS: i64 = 300;
#[cfg(not(feature = "connect-e2e-short-credentials"))]
const EXPECTED_CERTIFICATE_LIFETIME_SECONDS: i64 = 86_400;
#[cfg(feature = "connect-e2e-short-credentials")]
const EXPECTED_ROTATION_THRESHOLD_SECONDS: i64 = 120;
#[cfg(not(feature = "connect-e2e-short-credentials"))]
const EXPECTED_ROTATION_THRESHOLD_SECONDS: i64 = 8 * 60 * 60;
const ORGANIZATION_UID: &str = "0198f4b0-1a00-7c10-8d21-2e3f4a5b6c70";
const CLUSTER_UID: &str = "0198f4b0-2b00-7d20-9e31-3f4a5b6c7d81";
const DEVICE_UID: &str = "0198f4b0-3c00-7e30-8f41-4a5b6c7d8e92";
const TOKEN_UID: &str = "0198f4b0-6f00-7b60-9271-7d8e9fa0b1c5";
const FRESH_TOKEN_UID: &str = "0198f4b0-7f00-7c70-a381-8e9fa0b1c2d6";
const SECOND_TOKEN_UID: &str = "0198f4b0-8f00-7d80-b491-9fa0b1c2d3e7";
const UNRELATED_TOKEN_UID: &str = "0198f4b0-9f00-7e90-85a1-afb1c2d3e4f8";
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const INVENTORY_UID: &str = "0198f4b0-af00-7fa0-96b1-bfc1d2e3f509";

struct TestPki {
    root_params: CertificateParams,
    root_key: KeyPair,
    root_der: CertificateDer<'static>,
    root_pem: String,
    server_der: CertificateDer<'static>,
    server_key: PrivatePkcs8KeyDer<'static>,
}

impl TestPki {
    fn new() -> Self {
        let now = OffsetDateTime::now_utc();
        let root_key = KeyPair::generate().expect("generate root key");
        let mut root_params = CertificateParams::default();
        root_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        root_params.not_before = now - time::Duration::days(1);
        root_params.not_after = now + time::Duration::days(30);
        root_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::DigitalSignature];
        root_params.distinguished_name.push(DnType::CommonName, "Connect test root");
        let root = root_params.self_signed(&root_key).expect("sign root");

        let server_key = KeyPair::generate().expect("generate server key");
        let mut server_params = CertificateParams::default();
        server_params.not_before = now - time::Duration::hours(1);
        server_params.not_after = now + time::Duration::days(2);
        server_params
            .subject_alt_names
            .push(SanType::DnsName("localhost".try_into().expect("valid DNS name")));
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let issuer = Issuer::from_params(&root_params, &root_key);
        let server = server_params
            .signed_by(&server_key, &issuer)
            .expect("sign server certificate");

        Self {
            root_params,
            root_key,
            root_der: root.der().clone(),
            root_pem: root.pem(),
            server_der: server.der().clone(),
            server_key: PrivatePkcs8KeyDer::from(server_key.serialize_der()),
        }
    }

    fn credential(&self, identity: &rustfs::connect::DeviceIdentity, uri: &str, serial_byte: u8) -> Value {
        let now = OffsetDateTime::now_utc().replace_nanosecond(0).expect("whole second");
        self.credential_window(
            identity,
            uri,
            serial_byte,
            now,
            now + time::Duration::seconds(EXPECTED_CERTIFICATE_LIFETIME_SECONDS),
        )
    }

    fn credential_window(
        &self,
        identity: &rustfs::connect::DeviceIdentity,
        uri: &str,
        serial_byte: u8,
        not_before: OffsetDateTime,
        not_after: OffsetDateTime,
    ) -> Value {
        let mut params = CertificateParams::default();
        params.not_before = not_before;
        params.not_after = not_after;
        params.serial_number = Some(SerialNumber::from(vec![serial_byte; 16]));
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, DEVICE_UID);
        params
            .subject_alt_names
            .push(SanType::URI(uri.try_into().expect("valid URI SAN")));
        let private_key = identity.to_pkcs8_der().expect("serialize device key");
        let private_key = PrivatePkcs8KeyDer::from(private_key.to_vec());
        let device_key =
            KeyPair::from_pkcs8_der_and_sign_algo(&private_key, &rcgen::PKCS_ECDSA_P256_SHA256).expect("parse device key");
        let issuer = Issuer::from_params(&self.root_params, &self.root_key);
        let certificate = params.signed_by(&device_key, &issuer).expect("sign device certificate");
        let serial = format!("{serial_byte:02x}").repeat(16);
        let cluster = format!("organizations/{ORGANIZATION_UID}/clusters/{CLUSTER_UID}");

        json!({
            "name": format!("{cluster}/clusterDevices/{DEVICE_UID}"),
            "uid": DEVICE_UID,
            "cluster": cluster,
            "protocolVersion": "v1",
            "keyId": format!("x509-{serial}"),
            "certificateSerial": serial,
            "certificate": certificate.pem(),
            "certificateChain": certificate.pem(),
            "notBefore": not_before.format(&Rfc3339).expect("format notBefore"),
            "notAfter": not_after.format(&Rfc3339).expect("format notAfter"),
        })
    }

    fn server_config(&self, require_client: bool) -> rustls::ServerConfig {
        let mut roots = RootCertStore::empty();
        roots.add(self.root_der.clone()).expect("add client root");
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots));
        let verifier = if require_client {
            verifier.build()
        } else {
            verifier.allow_unauthenticated().build()
        }
        .expect("build client verifier");
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![self.server_der.clone()], PrivateKeyDer::Pkcs8(self.server_key.clone_key()))
            .expect("build server TLS")
    }
}

#[derive(Clone)]
enum Reply {
    Json(StatusCode, Value),
    DelayedClose(Duration),
    RateLimited(&'static str),
    UnauthorizedAfterCredential {
        path: std::path::PathBuf,
        certificate_serial: String,
    },
    VerifiedRotation {
        response: Value,
        current_public_key: Vec<u8>,
        current_certificate_fingerprint: String,
        device_name: String,
    },
}

struct TestServer {
    endpoint: String,
    seen: Arc<Mutex<Vec<Value>>>,
    paths: Arc<Mutex<Vec<String>>>,
    client_certificates: Arc<Mutex<Vec<Option<String>>>>,
    request_notify: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn server(pki: &TestPki, replies: Vec<Reply>) -> TestServer {
    server_with_client_auth(pki, replies, false).await
}

async fn server_with_client_auth(pki: &TestPki, replies: Vec<Reply>, require_client: bool) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test server");
    let address = listener.local_addr().expect("server address");
    let acceptor = TlsAcceptor::from(Arc::new(pki.server_config(require_client)));
    let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let paths = Arc::new(Mutex::new(Vec::new()));
    let client_certificates = Arc::new(Mutex::new(Vec::new()));
    let request_notify = Arc::new(Notify::new());
    let captured = seen.clone();
    let captured_paths = paths.clone();
    let captured_certificates = client_certificates.clone();
    let captured_notify = request_notify.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            let replies = replies.clone();
            let seen = captured.clone();
            let paths = captured_paths.clone();
            let client_certificates = captured_certificates.clone();
            let request_notify = captured_notify.clone();
            tokio::spawn(async move {
                let Ok(stream) = acceptor.accept(stream).await else {
                    return;
                };
                let client_certificate = stream
                    .get_ref()
                    .1
                    .peer_certificates()
                    .and_then(|certificates| certificates.first())
                    .map(|certificate| certificate_der_fingerprint(certificate.as_ref()));
                let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                    let replies = replies.clone();
                    let seen = seen.clone();
                    let paths = paths.clone();
                    let client_certificates = client_certificates.clone();
                    let request_notify = request_notify.clone();
                    let client_certificate = client_certificate.clone();
                    async move {
                        let path = request.uri().path().to_owned();
                        let body = request.into_body().collect().await.expect("read request body").to_bytes();
                        let value: Value = serde_json::from_slice(&body).expect("request JSON");
                        let reply = replies.lock().expect("reply lock").pop_front().expect("planned reply");
                        client_certificates
                            .lock()
                            .expect("client certificates lock")
                            .push(client_certificate);
                        paths.lock().expect("paths lock").push(path);
                        seen.lock().expect("seen lock").push(value.clone());
                        request_notify.notify_waiters();
                        match reply {
                            Reply::Json(status, value) => Ok::<_, hyper::Error>(
                                Response::builder()
                                    .status(status)
                                    .header("content-type", "application/json")
                                    .body(Full::new(Bytes::from(serde_json::to_vec(&value).expect("reply JSON"))))
                                    .expect("response"),
                            ),
                            Reply::DelayedClose(delay) => {
                                tokio::time::sleep(delay).await;
                                Ok(Response::builder()
                                    .status(StatusCode::SERVICE_UNAVAILABLE)
                                    .body(Full::new(Bytes::new()))
                                    .expect("response"))
                            }
                            Reply::RateLimited(retry_after) => Ok(Response::builder()
                                .status(StatusCode::TOO_MANY_REQUESTS)
                                .header("content-type", "application/json")
                                .header("retry-after", retry_after)
                                .body(Full::new(Bytes::from_static(br#"{"details":[]}"#)))
                                .expect("response")),
                            Reply::UnauthorizedAfterCredential {
                                path,
                                certificate_serial,
                            } => {
                                tokio::time::timeout(Duration::from_secs(3), async {
                                    loop {
                                        let changed = fs::read(&path)
                                            .ok()
                                            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                                            .is_some_and(|credential| {
                                                credential["certificateSerial"].as_str() == Some(certificate_serial.as_str())
                                            });
                                        if changed {
                                            break;
                                        }
                                        tokio::time::sleep(Duration::from_millis(5)).await;
                                    }
                                })
                                .await
                                .expect("rotated credential commit");
                                Ok(Response::builder()
                                    .status(StatusCode::UNAUTHORIZED)
                                    .header("content-type", "application/json")
                                    .body(Full::new(Bytes::from_static(br#"{"details":[{"reason":"CREDENTIAL_REVOKED"}]}"#)))
                                    .expect("response"))
                            }
                            Reply::VerifiedRotation {
                                response,
                                current_public_key,
                                current_certificate_fingerprint,
                                device_name,
                            } => {
                                verify_rotation_request(
                                    &value,
                                    &current_public_key,
                                    &current_certificate_fingerprint,
                                    &device_name,
                                );
                                Ok(Response::builder()
                                    .status(StatusCode::OK)
                                    .header("content-type", "application/json")
                                    .body(Full::new(Bytes::from(serde_json::to_vec(&response).expect("reply JSON"))))
                                    .expect("response"))
                            }
                        }
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    TestServer {
        endpoint: format!("https://localhost:{}/agent/", address.port()),
        seen,
        paths,
        client_certificates,
        request_notify,
        task,
    }
}

fn verify_rotation_request(request: &Value, current_public_key: &[u8], fingerprint: &str, device_name: &str) {
    assert_eq!(request["protocolVersion"], "v1");
    assert_eq!(request["proof"]["algorithm"], "ES256");
    let csr = BASE64_STANDARD
        .decode_to_vec(request["certificateRequest"].as_str().expect("certificateRequest"))
        .expect("CSR base64");
    let csr_digest = BASE64_URL_NO_PAD.encode_to_string(Sha256::digest(&csr));
    let request_id = request["requestId"].as_str().expect("requestId");
    let transcript = rebuilt_rotation_transcript(
        b"RUSTFS-CONNECT-CREDENTIAL-ROTATION-V1",
        [fingerprint, device_name, request_id, &csr_digest],
    );
    let encoded = request["proof"]["value"].as_str().expect("proof value");
    assert_eq!(encoded.len(), 86);
    let raw = BASE64_URL_NO_PAD.decode_to_vec(encoded).expect("proof base64url");
    let signature = Signature::from_slice(&raw).expect("fixed-width signature");
    assert_eq!(signature.normalize_s(), signature, "rotation proof must be low-S");
    let verifying = VerifyingKey::from_public_key_der(current_public_key).expect("current public key");
    verifying.verify(&transcript, &signature).expect("rotation proof verifies");

    let wrong_domain = rebuilt_rotation_transcript(
        b"RUSTFS-CONNECT-CREDENTIAL-ROTATION-V2",
        [fingerprint, device_name, request_id, &csr_digest],
    );
    assert!(verifying.verify(&wrong_domain, &signature).is_err());
    let wrong_order = rebuilt_rotation_transcript(
        b"RUSTFS-CONNECT-CREDENTIAL-ROTATION-V1",
        [device_name, fingerprint, request_id, &csr_digest],
    );
    assert!(verifying.verify(&wrong_order, &signature).is_err());
}

fn rebuilt_rotation_transcript(domain: &[u8], fields: [&str; 4]) -> Vec<u8> {
    let mut transcript = Vec::new();
    transcript.extend_from_slice(domain);
    transcript.push(b'\n');
    for field in fields {
        transcript.extend_from_slice(field.len().to_string().as_bytes());
        transcript.push(b':');
        transcript.extend_from_slice(field.as_bytes());
        transcript.push(b'\n');
    }
    transcript
}

fn certificate_fingerprint(pem: &str) -> String {
    let certificate = CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .expect("leaf certificate")
        .expect("certificate PEM");
    certificate_der_fingerprint(certificate.as_ref())
}

fn certificate_der_fingerprint(certificate: &[u8]) -> String {
    Sha256::digest(certificate).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn token_document() -> Value {
    json!({
        "registrationTokenUid": TOKEN_UID,
        "registrationTokenSecret": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "organizationUid": ORGANIZATION_UID,
        "clusterUid": CLUSTER_UID,
        "challengeNonce": "a3f1c07d9b2e4856af0c1d3b5e7f9012c4a6b8d0e2f4061738495a6b7c8d9e0f",
        "expiresUnix": OffsetDateTime::now_utc().unix_timestamp() + 3600,
    })
}

fn token() -> RegistrationToken {
    token_with_uid(TOKEN_UID)
}

fn token_with_uid(uid: &str) -> RegistrationToken {
    let document = token_document();
    let mut document = document;
    document["registrationTokenUid"] = json!(uid);
    RegistrationToken::from_reader(serde_json::to_vec(&document).expect("token JSON").as_slice()).expect("token parses")
}

fn stores(temp: &tempfile::TempDir) -> (IdentityStore, CredentialStore) {
    (
        IdentityStore::new(temp.path().join("identity")),
        CredentialStore::new(temp.path().join("credential")),
    )
}

#[cfg(target_os = "linux")]
fn secure_tempdir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt as _;

    let home = fs::canonicalize(std::env::var_os("HOME").expect("test requires a protected home directory"))
        .expect("test home directory must resolve without symlink components");
    fs::set_permissions(&home, fs::Permissions::from_mode(0o700))
        .expect("test home directory permissions must be restricted to the process owner");
    let temp = tempfile::Builder::new()
        .prefix(".connect-registration-")
        .tempdir_in(home)
        .expect("temporary directory inside the protected home directory");
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700))
        .expect("temporary state root permissions must be restricted to the process owner");
    temp
}

fn stage_next_identity(temp: &tempfile::TempDir) -> DeviceIdentity {
    let directory = temp.path().join("identity");
    fs::create_dir_all(&directory).expect("identity directory");
    let identity = DeviceIdentity::generate();
    let der = identity.to_pkcs8_der().expect("encode next identity");
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(directory.join("device.key.next")).expect("create next identity");
    file.write_all(&der).expect("write next identity");
    file.sync_all().expect("sync next identity");
    #[cfg(unix)]
    fs::File::open(&directory)
        .expect("open identity directory")
        .sync_all()
        .expect("sync identity directory");
    identity
}

fn client(server: &TestServer, pki: &TestPki, timeout: Duration) -> ConnectClient {
    ConnectClient::new(ConnectConfig {
        endpoint: &server.endpoint,
        root_ca_pem: pki.root_pem.as_bytes(),
        timeout,
    })
    .expect("build Connect client")
}

fn due_runtime_config(
    temp: &tempfile::TempDir,
    pki: &TestPki,
    endpoint: &str,
) -> (HeartbeatConfig, Value, rustfs::connect::DeviceIdentity) {
    runtime_config(
        temp,
        pki,
        endpoint,
        EXPECTED_CERTIFICATE_LIFETIME_SECONDS - EXPECTED_ROTATION_THRESHOLD_SECONDS,
    )
}

fn runtime_config(
    temp: &tempfile::TempDir,
    pki: &TestPki,
    endpoint: &str,
    elapsed_seconds: i64,
) -> (HeartbeatConfig, Value, rustfs::connect::DeviceIdentity) {
    let (identity_store, credential_store) = stores(temp);
    let identity = identity_store.load_or_create().expect("create current identity");
    let now = OffsetDateTime::now_utc().replace_nanosecond(0).expect("whole second");
    let credential = pki.credential_window(
        &identity,
        &format!("urn:rustfs:connect:device:{DEVICE_UID}"),
        0x20,
        now - time::Duration::seconds(elapsed_seconds),
        now + time::Duration::seconds(EXPECTED_CERTIFICATE_LIFETIME_SECONDS - elapsed_seconds),
    );
    let not_before =
        OffsetDateTime::parse(credential["notBefore"].as_str().expect("notBefore"), &Rfc3339).expect("parse notBefore");
    let not_after = OffsetDateTime::parse(credential["notAfter"].as_str().expect("notAfter"), &Rfc3339).expect("parse notAfter");
    assert_eq!(not_after - not_before, time::Duration::seconds(EXPECTED_CERTIFICATE_LIFETIME_SECONDS));
    fs::create_dir_all(temp.path().join("credential")).expect("credential directory");
    write_stored_credential(&temp.path().join("credential/device.crt.json"), &credential);
    let next = stage_next_identity(temp);
    let mut config = HeartbeatConfig::new(
        endpoint,
        pki.root_pem.as_bytes(),
        identity_store,
        credential_store,
        temp.path().join("heartbeat/state.json"),
    );
    config.schedule = HeartbeatSchedule {
        cadence: Duration::from_millis(100),
        jitter: Duration::ZERO,
        timeout: Duration::from_millis(250),
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_secs(2),
    };
    (config, credential, next)
}

fn heartbeat_response(server_time: &str) -> Value {
    json!({
        "serverTime": server_time,
        "acceptedVersion": "v1",
        "capabilityHints": [],
    })
}

async fn wait_for_requests(server: &TestServer, count: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while server.paths.lock().expect("paths lock").len() < count {
            tokio::select! {
                _ = server.request_notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }
    })
    .await
    .expect("planned requests");
}

async fn wait_for_heartbeat_status(
    status: &mut watch::Receiver<HeartbeatStatus>,
    predicate: impl Fn(&HeartbeatStatus) -> bool,
) -> HeartbeatStatus {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let current = status.borrow_and_update().clone();
            if predicate(&current) {
                return current;
            }
            status.changed().await.expect("heartbeat status channel");
        }
    })
    .await
    .expect("heartbeat status")
}

fn rotation_response(pki: &TestPki, identity: &rustfs::connect::DeviceIdentity, serial: u8) -> (Value, Value) {
    let stored = pki.credential(identity, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), serial);
    let mut wire = stored.clone();
    wire.as_object_mut().expect("response object").remove("uid");
    wire.as_object_mut().expect("response object").remove("cluster");
    (wire, stored)
}

fn write_stored_credential(path: &std::path::Path, response: &Value) {
    let not_before = OffsetDateTime::parse(response["notBefore"].as_str().expect("notBefore"), &Rfc3339)
        .expect("parse notBefore")
        .unix_timestamp();
    let not_after = OffsetDateTime::parse(response["notAfter"].as_str().expect("notAfter"), &Rfc3339)
        .expect("parse notAfter")
        .unix_timestamp();
    let stored = json!({
        "name": response["name"],
        "uid": DEVICE_UID,
        "protocolVersion": response["protocolVersion"],
        "keyId": response["keyId"],
        "certificateSerial": response["certificateSerial"],
        "certificate": response["certificate"],
        "certificateChain": response["certificateChain"],
        "notBeforeUnix": not_before,
        "notAfterUnix": not_after,
    });
    fs::write(path, serde_json::to_vec(&stored).expect("stored credential JSON")).expect("write credential");
    set_owner_only(path);
}

#[cfg(unix)]
fn set_owner_only(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("set owner-only mode");
}

#[cfg(not(unix))]
fn set_owner_only(_path: &std::path::Path) {}

#[tokio::test]
async fn registration_reuses_request_and_csr_after_timeout_and_restart() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (identity_store, credential_store) = stores(&temp);
    let identity = identity_store.load_or_create().expect("create identity");
    let pki = TestPki::new();
    let response = pki.credential(&identity, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 0x80);
    let server = server(
        &pki,
        vec![
            Reply::DelayedClose(Duration::from_millis(200)),
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
            Reply::Json(StatusCode::CREATED, response),
        ],
    )
    .await;
    let first = client(&server, &pki, Duration::from_millis(80));
    assert!(matches!(
        first.register(&identity_store, &credential_store, &token()).await,
        Err(ClientError::Unavailable { .. })
    ));

    let restarted = client(&server, &pki, Duration::from_secs(2));
    let credential = restarted
        .register(&identity_store, &credential_store, &token())
        .await
        .expect("restart replays completed exchange");
    assert_eq!(credential.uid, DEVICE_UID);
    assert_eq!(credential.certificate_serial, "80".repeat(16));

    let seen = server.seen.lock().expect("seen lock");
    assert_eq!(seen.len(), 4);
    for request in &seen[1..] {
        assert_eq!(request["requestId"], seen[0]["requestId"]);
        assert_eq!(request["certificateRequest"], seen[0]["certificateRequest"]);
    }
}

#[tokio::test]
async fn registration_rejects_untrusted_or_misbound_credentials() {
    for case in ["san", "chain", "key", "key_id", "cluster", "name"] {
        let temp = tempfile::tempdir().expect("temp dir");
        let (identity_store, credential_store) = stores(&temp);
        let identity = identity_store.load_or_create().expect("create identity");
        let pki = TestPki::new();
        let mut response = match case {
            "san" => pki.credential(&identity, "urn:rustfs:connect:device:0198f4b0-3c00-7e30-8f41-4a5b6c7d8e93", 2),
            "chain" => TestPki::new().credential(&identity, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 2),
            "key" => pki.credential(
                &rustfs::connect::DeviceIdentity::generate(),
                &format!("urn:rustfs:connect:device:{DEVICE_UID}"),
                2,
            ),
            _ => pki.credential(&identity, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 2),
        };
        match case {
            "key_id" => response["keyId"] = json!("x509-deadbeef"),
            "cluster" => response["cluster"] = json!(format!("organizations/{ORGANIZATION_UID}/clusters/other")),
            "name" => {
                response["name"] = json!(format!("organizations/{ORGANIZATION_UID}/clusters/other/clusterDevices/{DEVICE_UID}"))
            }
            _ => {}
        }
        let server = server(&pki, vec![Reply::Json(StatusCode::CREATED, response)]).await;
        let error = client(&server, &pki, Duration::from_secs(2))
            .register(&identity_store, &credential_store, &token())
            .await
            .expect_err("invalid returned identity must fail closed");
        assert!(matches!(error, ClientError::Credential(_)));
        assert!(!temp.path().join("credential/device.crt.json").exists());
    }
}

#[tokio::test]
async fn registration_enforces_build_profile_credential_lifetime() {
    let pki = TestPki::new();
    let accepted_temp = tempfile::tempdir().expect("temp dir");
    let (accepted_identity_store, accepted_credential_store) = stores(&accepted_temp);
    let accepted_identity = accepted_identity_store.load_or_create().expect("create identity");
    let accepted = pki.credential(&accepted_identity, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 4);
    let accepted_server = server(&pki, vec![Reply::Json(StatusCode::CREATED, accepted)]).await;
    let credential = client(&accepted_server, &pki, Duration::from_secs(2))
        .register(&accepted_identity_store, &accepted_credential_store, &token())
        .await
        .expect("configured lifetime must be accepted");
    assert_eq!(
        credential.not_after_unix - credential.not_before_unix,
        EXPECTED_CERTIFICATE_LIFETIME_SECONDS
    );

    let rejected_temp = tempfile::tempdir().expect("temp dir");
    let (rejected_identity_store, rejected_credential_store) = stores(&rejected_temp);
    let rejected_identity = rejected_identity_store.load_or_create().expect("create identity");
    let now = OffsetDateTime::now_utc().replace_nanosecond(0).expect("whole second");
    let other_lifetime = if cfg!(feature = "connect-e2e-short-credentials") {
        86_400
    } else {
        300
    };
    let rejected = pki.credential_window(
        &rejected_identity,
        &format!("urn:rustfs:connect:device:{DEVICE_UID}"),
        5,
        now,
        now + time::Duration::seconds(other_lifetime),
    );
    let rejected_server = server(&pki, vec![Reply::Json(StatusCode::CREATED, rejected)]).await;
    assert!(matches!(
        client(&rejected_server, &pki, Duration::from_secs(2))
            .register(&rejected_identity_store, &rejected_credential_store, &token())
            .await,
        Err(ClientError::Credential(_))
    ));
    assert!(!rejected_temp.path().join("credential/device.crt.json").exists());
}

#[tokio::test]
async fn stored_credential_is_revalidated_before_reuse() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (identity_store, credential_store) = stores(&temp);
    let identity = identity_store.load_or_create().expect("create identity");
    let pki = TestPki::new();
    let issued = pki.credential(&identity, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 4);
    let server = server(&pki, vec![Reply::Json(StatusCode::CREATED, issued)]).await;
    let client = client(&server, &pki, Duration::from_secs(2));
    client
        .register(&identity_store, &credential_store, &token())
        .await
        .expect("register");

    let path = temp.path().join("credential/device.crt.json");
    let mut stored: Value = serde_json::from_slice(&fs::read(&path).expect("read credential")).expect("credential JSON");
    stored["certificateSerial"] = json!("00".repeat(16));
    fs::write(&path, serde_json::to_vec(&stored).expect("credential JSON")).expect("tamper credential");
    let error = client
        .register(&identity_store, &credential_store, &token())
        .await
        .expect_err("tampered stored credential must fail closed");
    assert!(matches!(error, ClientError::Credential(_)));
}

#[tokio::test]
async fn register_rejects_expired_and_not_yet_valid_stored_credentials() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (identity_store, credential_store) = stores(&temp);
    let identity = identity_store.load_or_create().expect("create identity");
    let pki = TestPki::new();
    let issued = pki.credential(&identity, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 9);
    let registration = server(&pki, vec![Reply::Json(StatusCode::CREATED, issued)]).await;
    let client = client(&registration, &pki, Duration::from_secs(2));
    client
        .register(&identity_store, &credential_store, &token())
        .await
        .expect("register");

    let now = OffsetDateTime::now_utc().replace_nanosecond(0).expect("whole second");
    let path = temp.path().join("credential/device.crt.json");
    let expired = pki.credential_window(
        &identity,
        &format!("urn:rustfs:connect:device:{DEVICE_UID}"),
        10,
        now - time::Duration::seconds(EXPECTED_CERTIFICATE_LIFETIME_SECONDS + 60),
        now - time::Duration::seconds(60),
    );
    write_stored_credential(&path, &expired);
    assert!(matches!(
        client.register(&identity_store, &credential_store, &token()).await,
        Err(ClientError::CredentialExpired)
    ));

    let future = pki.credential_window(
        &identity,
        &format!("urn:rustfs:connect:device:{DEVICE_UID}"),
        11,
        now + time::Duration::hours(1),
        now + time::Duration::hours(1) + time::Duration::seconds(EXPECTED_CERTIFICATE_LIFETIME_SECONDS),
    );
    write_stored_credential(&path, &future);
    assert!(matches!(
        client.register(&identity_store, &credential_store, &token()).await,
        Err(ClientError::CredentialNotYetValid)
    ));
}

#[tokio::test]
async fn concurrent_rotation_retries_converge_and_promote_the_next_key() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (identity_store, credential_store) = stores(&temp);
    let current = identity_store.load_or_create().expect("create identity");
    let pki = TestPki::new();
    let mut issued = pki.credential(&current, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 5);
    issued["certificateChain"] = json!(
        issued["certificateChain"]
            .as_str()
            .expect("certificate chain")
            .trim_end_matches('\n')
    );
    let registration = server(&pki, vec![Reply::Json(StatusCode::CREATED, issued)]).await;
    let registered = client(&registration, &pki, Duration::from_secs(2))
        .register(&identity_store, &credential_store, &token())
        .await
        .expect("register");

    let retries = server(
        &pki,
        vec![
            Reply::DelayedClose(Duration::from_millis(200)),
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
        ],
    )
    .await;
    let retry_client = client(&retries, &pki, Duration::from_millis(80));
    let due = registered.not_after_unix - EXPECTED_ROTATION_THRESHOLD_SECONDS;
    let (first, second) = tokio::join!(
        retry_client.rotate_if_due(&identity_store, &credential_store, due),
        retry_client.rotate_if_due(&identity_store, &credential_store, due)
    );
    assert!(matches!(first, Err(ClientError::Unavailable { .. })));
    assert!(matches!(second, Err(ClientError::Unavailable { .. })));
    let (request_id, certificate_request) = {
        let seen = retries.seen.lock().expect("seen lock");
        assert_eq!(seen.len(), 6, "each public rotation call makes three attempts");
        for request in &seen[1..] {
            assert_eq!(request["requestId"], seen[0]["requestId"]);
            assert_eq!(request["certificateRequest"], seen[0]["certificateRequest"]);
        }
        (seen[0]["requestId"].clone(), seen[0]["certificateRequest"].clone())
    };

    let next_der = fs::read(temp.path().join("identity/device.key.next")).expect("read staged next key");
    let next = rustfs::connect::DeviceIdentity::from_pkcs8_der(&next_der).expect("parse next key");
    assert_ne!(current.public_key_der(), next.public_key_der());
    assert_eq!(
        identity_store
            .load()
            .expect("load current key")
            .expect("current key")
            .public_key_der(),
        current.public_key_der()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = fs::metadata(temp.path().join("identity/device.key.next"))
            .expect("next key metadata")
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(mode, 0o600);
    }
    let (rotated, _) = rotation_response(&pki, &next, 6);
    let success = server_with_client_auth(
        &pki,
        vec![Reply::VerifiedRotation {
            response: rotated,
            current_public_key: current.public_key_der(),
            current_certificate_fingerprint: certificate_fingerprint(&registered.certificate),
            device_name: registered.name.clone(),
        }],
        true,
    )
    .await;
    let success_client = client(&success, &pki, Duration::from_secs(2));
    let (due_result, current_result) = tokio::join!(
        success_client.rotate_if_due(&identity_store, &credential_store, due),
        success_client.rotate_if_due(&identity_store, &credential_store, OffsetDateTime::now_utc().unix_timestamp())
    );
    let credential = due_result
        .expect("retry rotation")
        .or(current_result.expect("concurrent current-state check"))
        .expect("exactly one rotation is due");
    assert_eq!(credential.certificate_serial, "06".repeat(16));
    let success_seen = success.seen.lock().expect("seen lock");
    assert_eq!(success_seen.len(), 1, "the post-commit actor must not publish stale state");
    assert_eq!(success_seen[0]["requestId"], request_id);
    assert_eq!(success_seen[0]["certificateRequest"], certificate_request);
    drop(success_seen);
    assert_eq!(
        identity_store
            .load()
            .expect("load key")
            .expect("current key")
            .public_key_der(),
        next.public_key_der()
    );
    assert!(!temp.path().join("identity/device.key.next").exists());
    assert!(!temp.path().join("credential/rotation.pending.json").exists());
}

#[tokio::test]
async fn heartbeat_runtime_retries_rotation_and_reloads_the_rotated_credential() {
    let temp = tempfile::tempdir().expect("temp dir");
    let pki = TestPki::new();
    let (mut config, current, next) = due_runtime_config(&temp, &pki, "https://localhost/agent/");
    let (rotated, rotated_stored) = rotation_response(&pki, &next, 0x21);
    let server = server_with_client_auth(
        &pki,
        vec![
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
            Reply::Json(StatusCode::OK, heartbeat_response("2026-08-25T01:02:03Z")),
            Reply::VerifiedRotation {
                response: rotated,
                current_public_key: config
                    .identity_store
                    .load()
                    .expect("load current identity")
                    .expect("current identity")
                    .public_key_der(),
                current_certificate_fingerprint: certificate_fingerprint(
                    current["certificate"].as_str().expect("current certificate"),
                ),
                device_name: current["name"].as_str().expect("device name").to_owned(),
            },
            Reply::Json(StatusCode::OK, heartbeat_response("2026-08-25T01:02:04Z")),
        ],
        true,
    )
    .await;
    config.endpoint = server.endpoint.clone();
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config), &shutdown, || CoarseNodeSummary::new(1, 1, 0).expect("node summary"))
        .expect("start heartbeat runtime")
        .expect("configured runtime");

    wait_for_requests(&server, 4).await;
    runtime.shutdown().await;

    let rotation_path = format!("/agent/clusterDevices/{DEVICE_UID}:rotateCredential");
    let heartbeat_path = format!("/agent/clusters/{CLUSTER_UID}/heartbeats");
    assert_eq!(
        &server.paths.lock().expect("paths lock")[..4],
        [rotation_path.clone(), heartbeat_path.clone(), rotation_path, heartbeat_path,]
    );
    let current_fingerprint = certificate_fingerprint(current["certificate"].as_str().expect("current certificate"));
    let rotated_fingerprint = certificate_fingerprint(rotated_stored["certificate"].as_str().expect("rotated certificate"));
    let certificates = server.client_certificates.lock().expect("client certificates lock");
    assert_eq!(certificates[1].as_deref(), Some(current_fingerprint.as_str()));
    assert_eq!(certificates[3].as_deref(), Some(rotated_fingerprint.as_str()));
    drop(certificates);
    let stored: Value =
        serde_json::from_slice(&fs::read(temp.path().join("credential/device.crt.json")).expect("stored rotated credential"))
            .expect("stored credential JSON");
    assert_eq!(stored["name"], current["name"]);
    assert_eq!(stored["uid"], current["uid"]);
    assert_eq!(stored["certificateSerial"], rotated_stored["certificateSerial"]);
}

#[tokio::test]
async fn heartbeat_runtime_respects_rotation_retry_after_without_blocking_heartbeats() {
    let temp = tempfile::tempdir().expect("temp dir");
    let pki = TestPki::new();
    let server = server_with_client_auth(
        &pki,
        std::iter::once(Reply::RateLimited("5"))
            .chain((0..8).map(|_| Reply::Json(StatusCode::OK, heartbeat_response("2026-08-25T01:02:03Z"))))
            .collect(),
        true,
    )
    .await;
    let (mut config, _, _) = due_runtime_config(&temp, &pki, &server.endpoint);
    config.schedule.max_backoff = Duration::from_secs(5);
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config), &shutdown, || CoarseNodeSummary::new(1, 1, 0).expect("node summary"))
        .expect("start heartbeat runtime")
        .expect("configured runtime");
    let mut status = runtime.status();

    wait_for_heartbeat_status(&mut status, |status| matches!(status, HeartbeatStatus::Online { .. })).await;
    wait_for_requests(&server, 4).await;
    runtime.shutdown().await;

    let paths = server.paths.lock().expect("paths lock");
    assert_eq!(paths.iter().filter(|path| path.ends_with(":rotateCredential")).count(), 1);
    assert!(paths.iter().filter(|path| path.ends_with("/heartbeats")).count() >= 3);
}

#[tokio::test]
async fn heartbeat_runtime_skips_only_valid_pending_reenrollment() {
    let temp = tempfile::tempdir().expect("temp dir");
    let pki = TestPki::new();
    let (mut config, _, next) = due_runtime_config(&temp, &pki, "https://localhost/agent/");
    let enrolled = pki.credential(&next, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 0x24);
    let server = server_with_client_auth(
        &pki,
        vec![
            Reply::Json(StatusCode::CREATED, enrolled),
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
            Reply::Json(StatusCode::OK, heartbeat_response("2026-08-25T01:02:03Z")),
        ],
        false,
    )
    .await;
    config.endpoint = server.endpoint.clone();
    let validation_config = config.clone();
    client(&server, &pki, Duration::from_millis(250))
        .reenroll(&config.identity_store, &config.credential_store, &token_with_uid(FRESH_TOKEN_UID))
        .await
        .expect("complete prior reenrollment");
    assert!(matches!(
        client(&server, &pki, Duration::from_millis(250))
            .reenroll(&config.identity_store, &config.credential_store, &token_with_uid(SECOND_TOKEN_UID))
            .await,
        Err(ClientError::Unavailable { .. })
    ));
    let pending_path = temp.path().join("credential/registration.pending.json");
    let next_path = temp.path().join("identity/device.key.next");
    let credential_path = temp.path().join("credential/device.crt.json");
    let pending = fs::read(&pending_path).expect("pending reenrollment");
    let next = fs::read(&next_path).expect("pending reenrollment key");
    let credential = fs::read(&credential_path).expect("current credential");
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config), &shutdown, || CoarseNodeSummary::new(1, 1, 0).expect("node summary"))
        .expect("start heartbeat runtime")
        .expect("configured runtime");
    let mut status = runtime.status();

    assert!(matches!(
        wait_for_heartbeat_status(&mut status, |status| matches!(status, HeartbeatStatus::Online { .. })).await,
        HeartbeatStatus::Online { .. }
    ));
    runtime.shutdown().await;

    assert_eq!(fs::read(&pending_path).expect("preserved pending reenrollment"), pending);
    assert_eq!(fs::read(&next_path).expect("preserved pending reenrollment key"), next);
    assert_eq!(fs::read(&credential_path).expect("preserved current credential"), credential);
    {
        let paths = server.paths.lock().expect("paths lock");
        assert_eq!(
            paths
                .iter()
                .filter(|path| path.ends_with("registrationTokens:exchange"))
                .count(),
            4
        );
        assert_eq!(paths.iter().filter(|path| path.ends_with(":rotateCredential")).count(), 0);
        assert_eq!(paths.iter().filter(|path| path.ends_with("/heartbeats")).count(), 1);
    }

    for (field, value) in [
        ("requestId", "not-a-request-id"),
        ("tokenUid", ""),
        ("tokenUid", "not-a-token-uid"),
        ("tokenUid", FRESH_TOKEN_UID),
        ("tokenUid", UNRELATED_TOKEN_UID),
    ] {
        let mut corrupted_pending: Value = serde_json::from_slice(&pending).expect("pending reenrollment JSON");
        corrupted_pending[field] = json!(value);
        let corrupted_pending = serde_json::to_vec(&corrupted_pending).expect("corrupted pending reenrollment JSON");
        fs::write(&pending_path, &corrupted_pending).expect("corrupt pending reenrollment");
        set_owner_only(&pending_path);
        let corrupted_runtime = spawn_heartbeat_runtime(Some(validation_config.clone()), &shutdown, || {
            CoarseNodeSummary::new(1, 1, 0).expect("node summary")
        })
        .expect("start heartbeat runtime with corrupt pending state")
        .expect("configured runtime");
        let mut corrupted_status = corrupted_runtime.status();
        assert_eq!(
            wait_for_heartbeat_status(&mut corrupted_status, |status| matches!(status, HeartbeatStatus::Failed { .. })).await,
            HeartbeatStatus::Failed {
                reason: HeartbeatError::StateConflict.to_string(),
            }
        );
        corrupted_runtime.shutdown().await;
        assert_eq!(fs::read(&pending_path).expect("preserved corrupt pending state"), corrupted_pending);
    }
    assert_eq!(fs::read(&next_path).expect("preserved pending reenrollment key"), next);
    assert_eq!(fs::read(&credential_path).expect("preserved current credential"), credential);
    let paths = server.paths.lock().expect("paths lock");
    assert_eq!(paths.iter().filter(|path| path.ends_with(":rotateCredential")).count(), 0);
    assert_eq!(paths.iter().filter(|path| path.ends_with("/heartbeats")).count(), 1);
}

#[tokio::test]
async fn heartbeat_retries_with_the_new_credential_after_concurrent_rotation() {
    let temp = tempfile::tempdir().expect("temp dir");
    let pki = TestPki::new();
    let (mut config, current, next) = runtime_config(&temp, &pki, "https://localhost/agent/", 1);
    let (rotated, rotated_stored) = rotation_response(&pki, &next, 0x22);
    let server = server_with_client_auth(
        &pki,
        vec![
            Reply::UnauthorizedAfterCredential {
                path: temp.path().join("credential/device.crt.json"),
                certificate_serial: rotated_stored["certificateSerial"]
                    .as_str()
                    .expect("rotated serial")
                    .to_owned(),
            },
            Reply::VerifiedRotation {
                response: rotated,
                current_public_key: config
                    .identity_store
                    .load()
                    .expect("load current identity")
                    .expect("current identity")
                    .public_key_der(),
                current_certificate_fingerprint: certificate_fingerprint(
                    current["certificate"].as_str().expect("current certificate"),
                ),
                device_name: current["name"].as_str().expect("device name").to_owned(),
            },
            Reply::Json(StatusCode::OK, heartbeat_response("2026-08-25T01:02:03Z")),
        ],
        true,
    )
    .await;
    config.endpoint = server.endpoint.clone();
    let identity_store = config.identity_store.clone();
    let credential_store = config.credential_store.clone();
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config), &shutdown, || CoarseNodeSummary::new(1, 1, 0).expect("node summary"))
        .expect("start heartbeat runtime")
        .expect("configured runtime");
    let mut status = runtime.status();

    wait_for_requests(&server, 1).await;
    let due = OffsetDateTime::parse(current["notAfter"].as_str().expect("notAfter"), &Rfc3339)
        .expect("parse notAfter")
        .unix_timestamp()
        - EXPECTED_ROTATION_THRESHOLD_SECONDS;
    client(&server, &pki, Duration::from_millis(250))
        .rotate_if_due(&identity_store, &credential_store, due)
        .await
        .expect("concurrent rotation")
        .expect("rotation due");
    assert!(matches!(
        wait_for_heartbeat_status(&mut status, |status| matches!(status, HeartbeatStatus::Online { .. })).await,
        HeartbeatStatus::Online { .. }
    ));
    runtime.shutdown().await;

    let heartbeat_path = format!("/agent/clusters/{CLUSTER_UID}/heartbeats");
    assert_eq!(
        server.paths.lock().expect("paths lock").as_slice(),
        [
            heartbeat_path.clone(),
            format!("/agent/clusterDevices/{DEVICE_UID}:rotateCredential"),
            heartbeat_path,
        ]
    );
    let current_fingerprint = certificate_fingerprint(current["certificate"].as_str().expect("current certificate"));
    let rotated_fingerprint = certificate_fingerprint(rotated_stored["certificate"].as_str().expect("rotated certificate"));
    let certificates = server.client_certificates.lock().expect("client certificates lock");
    assert_eq!(certificates[0].as_deref(), Some(current_fingerprint.as_str()));
    assert_eq!(certificates[2].as_deref(), Some(rotated_fingerprint.as_str()));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn inventory_retries_with_the_new_credential_after_concurrent_rotation() {
    let temp = secure_tempdir();
    let pki = TestPki::new();
    let (mut config, current, next) = runtime_config(&temp, &pki, "https://localhost/agent/", 1);
    let (rotated, rotated_stored) = rotation_response(&pki, &next, 0x23);
    let server = server_with_client_auth(
        &pki,
        vec![
            Reply::UnauthorizedAfterCredential {
                path: temp.path().join("credential/device.crt.json"),
                certificate_serial: rotated_stored["certificateSerial"]
                    .as_str()
                    .expect("rotated serial")
                    .to_owned(),
            },
            Reply::VerifiedRotation {
                response: rotated,
                current_public_key: config
                    .identity_store
                    .load()
                    .expect("load current identity")
                    .expect("current identity")
                    .public_key_der(),
                current_certificate_fingerprint: certificate_fingerprint(
                    current["certificate"].as_str().expect("current certificate"),
                ),
                device_name: current["name"].as_str().expect("device name").to_owned(),
            },
            Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})),
        ],
        true,
    )
    .await;
    config.endpoint = server.endpoint.clone();
    let identity_store = config.identity_store.clone();
    let credential_store = config.credential_store.clone();
    let snapshot = InventorySnapshot::current(1, 1, 100, 50, []).expect("inventory snapshot");
    let shutdown = CancellationToken::new();
    let runtime = spawn_inventory_runtime(
        Some(config),
        InventorySchedule {
            cadence: Duration::from_millis(100),
            jitter: Duration::ZERO,
        },
        &shutdown,
        move || std::future::ready(Ok(snapshot.clone())),
    )
    .expect("start inventory runtime")
    .expect("configured runtime");
    let mut status = runtime.status();

    wait_for_requests(&server, 1).await;
    let due = OffsetDateTime::parse(current["notAfter"].as_str().expect("notAfter"), &Rfc3339)
        .expect("parse notAfter")
        .unix_timestamp()
        - EXPECTED_ROTATION_THRESHOLD_SECONDS;
    client(&server, &pki, Duration::from_millis(250))
        .rotate_if_due(&identity_store, &credential_store, due)
        .await
        .expect("concurrent rotation")
        .expect("rotation due");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let current = status.borrow_and_update().clone();
                if matches!(current, InventoryStatus::BackingOff { .. }) {
                    break current;
                }
                status.changed().await.expect("inventory status channel");
            }
        })
        .await
        .expect("inventory retry status"),
        InventoryStatus::BackingOff { .. }
    ));
    wait_for_requests(&server, 3).await;
    runtime.shutdown().await;

    let inventory_path = format!("/agent/clusters/{CLUSTER_UID}/inventorySnapshots");
    assert_eq!(
        server.paths.lock().expect("paths lock").as_slice(),
        [
            inventory_path.clone(),
            format!("/agent/clusterDevices/{DEVICE_UID}:rotateCredential"),
            inventory_path,
        ]
    );
    let current_fingerprint = certificate_fingerprint(current["certificate"].as_str().expect("current certificate"));
    let rotated_fingerprint = certificate_fingerprint(rotated_stored["certificate"].as_str().expect("rotated certificate"));
    let certificates = server.client_certificates.lock().expect("client certificates lock");
    assert_eq!(certificates[0].as_deref(), Some(current_fingerprint.as_str()));
    assert_eq!(certificates[2].as_deref(), Some(rotated_fingerprint.as_str()));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn inventory_first_recovers_a_saved_reenrollment_before_telemetry() {
    let temp = secure_tempdir();
    let pki = TestPki::new();
    let failed = server(&pki, vec![Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})); 3]).await;
    let (mut config, _, next) = runtime_config(&temp, &pki, &failed.endpoint, 1);
    assert!(matches!(
        client(&failed, &pki, Duration::from_millis(250))
            .reenroll(&config.identity_store, &config.credential_store, &token_with_uid(SECOND_TOKEN_UID))
            .await,
        Err(ClientError::Unavailable { .. })
    ));
    let enrolled = pki.credential(&next, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 0x25);
    write_stored_credential(&temp.path().join("credential/device.crt.json"), &enrolled);
    assert!(temp.path().join("credential/registration.pending.json").exists());
    assert!(temp.path().join("identity/device.key.next").exists());
    assert_ne!(
        config
            .identity_store
            .load()
            .expect("load pre-recovery identity")
            .expect("pre-recovery identity")
            .public_key_der(),
        next.public_key_der()
    );

    let snapshot = InventorySnapshot::current(1, 1, 100, 50, []).expect("inventory snapshot");
    let content_hash = snapshot.content_hash().expect("inventory content hash");
    let telemetry = server_with_client_auth(
        &pki,
        vec![
            Reply::Json(
                StatusCode::OK,
                json!({
                    "name": format!("organizations/{ORGANIZATION_UID}/clusters/{CLUSTER_UID}/inventorySnapshots/{INVENTORY_UID}"),
                    "uid": INVENTORY_UID,
                    "contentHash": content_hash,
                    "receivedAt": "2026-08-25T01:02:03Z",
                }),
            ),
            Reply::Json(StatusCode::OK, heartbeat_response("2026-08-25T01:02:04Z")),
        ],
        true,
    )
    .await;
    config.endpoint = telemetry.endpoint.clone();
    config.schedule.cadence = Duration::from_secs(60);
    let heartbeat_config = config.clone();
    let shutdown = CancellationToken::new();
    let inventory_snapshot = snapshot.clone();
    let inventory = spawn_inventory_runtime(
        Some(config),
        InventorySchedule {
            cadence: Duration::from_secs(60),
            jitter: Duration::ZERO,
        },
        &shutdown,
        move || std::future::ready(Ok(inventory_snapshot.clone())),
    )
    .expect("start inventory runtime")
    .expect("configured inventory runtime");
    let mut inventory_status = inventory.status();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let current = inventory_status.borrow_and_update().clone();
                if matches!(current, InventoryStatus::Online { .. }) {
                    break current;
                }
                inventory_status.changed().await.expect("inventory status channel");
            }
        })
        .await
        .expect("inventory online status"),
        InventoryStatus::Online { .. }
    ));

    let pending_path = temp.path().join("credential/registration.pending.json");
    assert!(!pending_path.exists());
    assert!(!temp.path().join("identity/device.key.next").exists());
    assert_eq!(
        heartbeat_config
            .identity_store
            .load()
            .expect("load recovered identity")
            .expect("recovered identity")
            .public_key_der(),
        next.public_key_der()
    );
    let completed: Value = serde_json::from_slice(
        &fs::read(temp.path().join("credential/registration.completed.json")).expect("completed reenrollment receipt"),
    )
    .expect("completed reenrollment JSON");
    assert_eq!(completed["tokenUid"], SECOND_TOKEN_UID);

    let heartbeat = spawn_heartbeat_runtime(Some(heartbeat_config), &shutdown, || {
        CoarseNodeSummary::new(1, 1, 0).expect("node summary")
    })
    .expect("start heartbeat runtime")
    .expect("configured heartbeat runtime");
    let mut heartbeat_status = heartbeat.status();
    assert!(matches!(
        wait_for_heartbeat_status(&mut heartbeat_status, |status| matches!(status, HeartbeatStatus::Online { .. })).await,
        HeartbeatStatus::Online { .. }
    ));
    heartbeat.shutdown().await;
    inventory.shutdown().await;

    assert_eq!(failed.paths.lock().expect("registration paths").len(), 3);
    assert_eq!(
        telemetry.paths.lock().expect("telemetry paths").as_slice(),
        [
            format!("/agent/clusters/{CLUSTER_UID}/inventorySnapshots"),
            format!("/agent/clusters/{CLUSTER_UID}/heartbeats"),
        ]
    );
    let enrolled_fingerprint = certificate_fingerprint(enrolled["certificate"].as_str().expect("enrolled certificate"));
    assert_eq!(
        telemetry.client_certificates.lock().expect("client certificates").as_slice(),
        [Some(enrolled_fingerprint.clone()), Some(enrolled_fingerprint)]
    );
}

#[tokio::test]
async fn heartbeat_runtime_cancels_an_in_flight_rotation() {
    let temp = tempfile::tempdir().expect("temp dir");
    let pki = TestPki::new();
    let server = server_with_client_auth(&pki, vec![Reply::DelayedClose(Duration::from_secs(5))], true).await;
    let (config, _, _) = due_runtime_config(&temp, &pki, &server.endpoint);
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config), &shutdown, || CoarseNodeSummary::new(1, 1, 0).expect("node summary"))
        .expect("start heartbeat runtime")
        .expect("configured runtime");

    wait_for_requests(&server, 1).await;
    tokio::time::timeout(Duration::from_millis(250), runtime.shutdown())
        .await
        .expect("rotation cancellation");
    assert_eq!(
        server.paths.lock().expect("paths lock").as_slice(),
        [format!("/agent/clusterDevices/{DEVICE_UID}:rotateCredential")]
    );
}

#[tokio::test]
async fn heartbeat_runtime_stops_when_rotation_reports_revocation() {
    let temp = tempfile::tempdir().expect("temp dir");
    let pki = TestPki::new();
    let server = server_with_client_auth(
        &pki,
        vec![Reply::Json(
            StatusCode::UNAUTHORIZED,
            json!({"details": [{"reason": "DEVICE_REVOKED"}]}),
        )],
        true,
    )
    .await;
    let (config, _, _) = due_runtime_config(&temp, &pki, &server.endpoint);
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config), &shutdown, || CoarseNodeSummary::new(1, 1, 0).expect("node summary"))
        .expect("start heartbeat runtime")
        .expect("configured runtime");
    let mut status = runtime.status();

    assert_eq!(
        wait_for_heartbeat_status(&mut status, |status| matches!(status, HeartbeatStatus::AuthenticationStopped { .. })).await,
        HeartbeatStatus::AuthenticationStopped {
            status: 401,
            reason: Some("DEVICE_REVOKED".to_owned()),
        }
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        server.paths.lock().expect("paths lock").as_slice(),
        [format!("/agent/clusterDevices/{DEVICE_UID}:rotateCredential")]
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn rotation_commit_recovers_after_each_durable_step() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (identity_store, credential_store) = stores(&temp);
    let current = identity_store.load_or_create().expect("create identity");
    let pki = TestPki::new();
    let issued = pki.credential(&current, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 7);
    let registration = server(&pki, vec![Reply::Json(StatusCode::CREATED, issued)]).await;
    let registered = client(&registration, &pki, Duration::from_secs(2))
        .register(&identity_store, &credential_store, &token())
        .await
        .expect("register");
    let failed = server(&pki, vec![Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})); 3]).await;
    let due = registered.not_after_unix - EXPECTED_ROTATION_THRESHOLD_SECONDS;
    assert!(matches!(
        client(&failed, &pki, Duration::from_secs(2))
            .rotate_if_due(&identity_store, &credential_store, due)
            .await,
        Err(ClientError::Unavailable { .. })
    ));
    {
        let failed_seen = failed.seen.lock().expect("seen lock");
        assert_eq!(failed_seen.len(), 3, "public rotation keeps its bounded retry contract");
        for request in &failed_seen[1..] {
            assert_eq!(request["requestId"], failed_seen[0]["requestId"]);
            assert_eq!(request["certificateRequest"], failed_seen[0]["certificateRequest"]);
        }
    }

    let pending_path = temp.path().join("credential/rotation.pending.json");
    let pending = fs::read(&pending_path).expect("read pending state");
    let next_der = fs::read(temp.path().join("identity/device.key.next")).expect("read next key");
    let next = rustfs::connect::DeviceIdentity::from_pkcs8_der(&next_der).expect("parse next key");
    let (_, stored) = rotation_response(&pki, &next, 8);
    write_stored_credential(&temp.path().join("credential/device.crt.json"), &stored);

    let idle = server(&pki, vec![]).await;
    assert!(
        client(&idle, &pki, Duration::from_secs(2))
            .rotate_if_due(&identity_store, &credential_store, OffsetDateTime::now_utc().unix_timestamp())
            .await
            .expect("recover after credential save")
            .is_none()
    );
    assert_eq!(
        identity_store
            .load()
            .expect("load key")
            .expect("current key")
            .public_key_der(),
        next.public_key_der()
    );

    fs::write(&pending_path, pending).expect("restore pending after key commit");
    set_owner_only(&pending_path);
    assert!(
        client(&idle, &pki, Duration::from_secs(2))
            .rotate_if_due(&identity_store, &credential_store, OffsetDateTime::now_utc().unix_timestamp())
            .await
            .expect("recover after key commit")
            .is_none()
    );
    assert!(!pending_path.exists());
}

#[tokio::test]
async fn pending_reenrollment_blocks_rotation_and_resumes_original_exchange() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (identity_store, credential_store) = stores(&temp);
    let current = identity_store.load_or_create().expect("create identity");
    let pki = TestPki::new();
    let issued = pki.credential(&current, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 13);
    let registration = server(&pki, vec![Reply::Json(StatusCode::CREATED, issued)]).await;
    let registered = client(&registration, &pki, Duration::from_secs(2))
        .register(&identity_store, &credential_store, &token())
        .await
        .expect("register");

    let failed = server(&pki, vec![Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})); 3]).await;
    assert!(matches!(
        client(&failed, &pki, Duration::from_secs(2))
            .reenroll(&identity_store, &credential_store, &token_with_uid(FRESH_TOKEN_UID))
            .await,
        Err(ClientError::Unavailable { .. })
    ));

    let pending_path = temp.path().join("credential/registration.pending.json");
    let pending = fs::read(&pending_path).expect("read pending reenrollment");
    let pending_document: Value = serde_json::from_slice(&pending).expect("pending JSON");
    let next_der = fs::read(temp.path().join("identity/device.key.next")).expect("read next key");
    let next = rustfs::connect::DeviceIdentity::from_pkcs8_der(&next_der).expect("parse next key");
    let enrolled = pki.credential(&next, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 14);

    let rotation = server(&pki, vec![]).await;
    let error = client(&rotation, &pki, Duration::from_secs(2))
        .rotate_if_due(
            &identity_store,
            &credential_store,
            registered.not_after_unix - EXPECTED_ROTATION_THRESHOLD_SECONDS,
        )
        .await
        .expect_err("pending reenrollment blocks rotation");
    assert!(matches!(error, ClientError::PendingRegistration));
    assert!(rotation.seen.lock().expect("seen lock").is_empty());

    let resumed = server(&pki, vec![Reply::Json(StatusCode::CREATED, enrolled)]).await;
    let credential = client(&resumed, &pki, Duration::from_secs(2))
        .reenroll(&identity_store, &credential_store, &token_with_uid(FRESH_TOKEN_UID))
        .await
        .expect("resume reenrollment");
    assert_eq!(credential.certificate_serial, "0e".repeat(16));
    let resumed_seen = resumed.seen.lock().expect("seen lock");
    assert_eq!(resumed_seen.len(), 1);
    assert_eq!(resumed_seen[0]["requestId"], pending_document["requestId"]);
    assert_eq!(resumed_seen[0]["certificateRequest"], pending_document["certificateRequest"]);
}

#[tokio::test]
async fn reenrollment_commit_recovers_after_each_durable_step() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (identity_store, credential_store) = stores(&temp);
    let initial = identity_store.load_or_create().expect("create identity");
    let pki = TestPki::new();
    let issued = pki.credential(&initial, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 13);
    let registration = server(&pki, vec![Reply::Json(StatusCode::CREATED, issued)]).await;
    client(&registration, &pki, Duration::from_secs(2))
        .register(&identity_store, &credential_store, &token())
        .await
        .expect("register");

    let first_next = stage_next_identity(&temp);
    let first_enrolled = pki.credential(&first_next, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 14);
    let first_reenrollment = server(&pki, vec![Reply::Json(StatusCode::CREATED, first_enrolled)]).await;
    client(&first_reenrollment, &pki, Duration::from_secs(2))
        .reenroll(&identity_store, &credential_store, &token_with_uid(FRESH_TOKEN_UID))
        .await
        .expect("complete prior reenrollment");
    let completed_path = temp.path().join("credential/registration.completed.json");
    let previous_completed = fs::read(&completed_path).expect("read prior completed receipt");

    let failed = server(&pki, vec![Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})); 3]).await;
    assert!(matches!(
        client(&failed, &pki, Duration::from_secs(2))
            .reenroll(&identity_store, &credential_store, &token_with_uid(SECOND_TOKEN_UID))
            .await,
        Err(ClientError::Unavailable { .. })
    ));

    let pending_path = temp.path().join("credential/registration.pending.json");
    let pending = fs::read(&pending_path).expect("read pending reenrollment");
    let next_der = fs::read(temp.path().join("identity/device.key.next")).expect("read next key");
    let next = rustfs::connect::DeviceIdentity::from_pkcs8_der(&next_der).expect("parse next key");
    let enrolled = pki.credential(&next, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 15);
    write_stored_credential(&temp.path().join("credential/device.crt.json"), &enrolled);

    let idle = server(&pki, vec![]).await;
    let idle_client = client(&idle, &pki, Duration::from_secs(2));
    for token_uid in [UNRELATED_TOKEN_UID, FRESH_TOKEN_UID] {
        let mut document: Value = serde_json::from_slice(&pending).expect("pending reenrollment JSON");
        document["tokenUid"] = json!(token_uid);
        let tampered_pending = serde_json::to_vec(&document).expect("tampered pending reenrollment JSON");
        fs::write(&pending_path, &tampered_pending).expect("tamper pending after credential save");
        set_owner_only(&pending_path);
        assert!(matches!(
            idle_client
                .reenroll(&identity_store, &credential_store, &token_with_uid(SECOND_TOKEN_UID))
                .await,
            Err(ClientError::PendingRegistration)
        ));
        assert_eq!(fs::read(&pending_path).expect("preserved tampered pending"), tampered_pending);
        assert_eq!(fs::read(&completed_path).expect("preserved prior completed receipt"), previous_completed);
        assert_eq!(
            identity_store
                .load()
                .expect("load current key")
                .expect("current key")
                .public_key_der(),
            first_next.public_key_der()
        );
    }
    fs::write(&pending_path, &pending).expect("restore bound pending after credential save");
    set_owner_only(&pending_path);
    let recovered = idle_client
        .reenroll(&identity_store, &credential_store, &token_with_uid(SECOND_TOKEN_UID))
        .await
        .expect("recover after reenrollment credential save");
    assert_eq!(recovered.certificate_serial, "0f".repeat(16));
    assert_eq!(
        identity_store
            .load()
            .expect("load key")
            .expect("current key")
            .public_key_der(),
        next.public_key_der()
    );

    fs::write(&completed_path, &previous_completed).expect("restore prior completed receipt after key commit");
    set_owner_only(&completed_path);
    for token_uid in [UNRELATED_TOKEN_UID, FRESH_TOKEN_UID] {
        let mut document: Value = serde_json::from_slice(&pending).expect("pending reenrollment JSON");
        document["tokenUid"] = json!(token_uid);
        let tampered_pending = serde_json::to_vec(&document).expect("tampered pending reenrollment JSON");
        fs::write(&pending_path, &tampered_pending).expect("tamper pending after key commit");
        set_owner_only(&pending_path);
        assert!(matches!(
            idle_client
                .reenroll(&identity_store, &credential_store, &token_with_uid(SECOND_TOKEN_UID))
                .await,
            Err(ClientError::PendingRegistration)
        ));
        assert_eq!(fs::read(&pending_path).expect("preserved tampered pending"), tampered_pending);
        assert_eq!(fs::read(&completed_path).expect("preserved prior completed receipt"), previous_completed);
        assert_eq!(
            identity_store
                .load()
                .expect("load current key")
                .expect("current key")
                .public_key_der(),
            next.public_key_der()
        );
    }
    fs::write(&pending_path, pending).expect("restore bound pending after key commit");
    set_owner_only(&pending_path);
    let recovered = idle_client
        .reenroll(&identity_store, &credential_store, &token_with_uid(SECOND_TOKEN_UID))
        .await
        .expect("recover after reenrollment key commit");
    assert_eq!(recovered.certificate_serial, "0f".repeat(16));
    assert!(!pending_path.exists());
    let recovered = idle_client
        .reenroll(&identity_store, &credential_store, &token_with_uid(SECOND_TOKEN_UID))
        .await
        .expect("completed reenrollment is idempotent after pending cleanup");
    assert_eq!(recovered.certificate_serial, "0f".repeat(16));
    assert!(idle.seen.lock().expect("seen lock").is_empty());

    let different = server(&pki, vec![Reply::Json(StatusCode::SERVICE_UNAVAILABLE, json!({})); 3]).await;
    assert!(matches!(
        client(&different, &pki, Duration::from_secs(2))
            .reenroll(&identity_store, &credential_store, &token_with_uid(TOKEN_UID))
            .await,
        Err(ClientError::Unavailable { .. })
    ));
    assert_eq!(different.seen.lock().expect("seen lock").len(), 3);
}

#[test]
fn registration_token_schema_is_strict_and_bounded() {
    let mut document = serde_json::to_value(token_document()).expect("token document");
    document["unexpected"] = json!(true);
    assert!(matches!(
        RegistrationToken::from_reader(serde_json::to_vec(&document).expect("token JSON").as_slice()),
        Err(TokenError::Invalid(_))
    ));
    assert!(matches!(
        RegistrationToken::from_reader(vec![b' '; 16 * 1024 + 1].as_slice()),
        Err(TokenError::TooLarge)
    ));
    let mut malformed = token_document();
    malformed["challengeNonce"] = json!("A".repeat(64));
    assert!(matches!(
        RegistrationToken::from_reader(serde_json::to_vec(&malformed).expect("token JSON").as_slice()),
        Err(TokenError::Shape)
    ));
}

#[tokio::test]
async fn rotation_waits_for_threshold_and_stops_on_revocation() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (identity_store, credential_store) = stores(&temp);
    let identity = identity_store.load_or_create().expect("create identity");
    let pki = TestPki::new();
    let issued = pki.credential(&identity, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 3);
    let rotation_server = server(
        &pki,
        vec![
            Reply::Json(StatusCode::CREATED, issued),
            Reply::Json(StatusCode::UNAUTHORIZED, json!({"details": [{"reason": "DEVICE_REVOKED"}]})),
        ],
    )
    .await;
    let connect = client(&rotation_server, &pki, Duration::from_secs(2));
    let current = connect
        .register(&identity_store, &credential_store, &token())
        .await
        .expect("register");
    assert_eq!(current.not_after_unix - current.not_before_unix, EXPECTED_CERTIFICATE_LIFETIME_SECONDS);
    assert!(
        connect
            .rotate_if_due(
                &identity_store,
                &credential_store,
                current.not_after_unix - EXPECTED_ROTATION_THRESHOLD_SECONDS - 1,
            )
            .await
            .expect("not due")
            .is_none()
    );
    assert_eq!(rotation_server.seen.lock().expect("seen lock").len(), 1);
    let error = connect
        .rotate_if_due(
            &identity_store,
            &credential_store,
            current.not_after_unix - EXPECTED_ROTATION_THRESHOLD_SECONDS,
        )
        .await
        .expect_err("revocation must stop rotation");
    assert!(matches!(error, ClientError::AccessRevoked { .. }));
    assert!(error.to_string().contains("ConnectClient::reenroll"));
    assert_eq!(rotation_server.seen.lock().expect("seen lock").len(), 2);
    let stored: Value =
        serde_json::from_slice(&fs::read(temp.path().join("credential/device.crt.json")).expect("read stored credential"))
            .expect("stored credential JSON");
    assert_eq!(stored["certificateSerial"], current.certificate_serial);

    let next_der = fs::read(temp.path().join("identity/device.key.next")).expect("read staged next key");
    let next = rustfs::connect::DeviceIdentity::from_pkcs8_der(&next_der).expect("parse staged next key");
    let enrolled = pki.credential(&next, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 12);
    let reenrollment = server(&pki, vec![Reply::Json(StatusCode::CREATED, enrolled)]).await;
    let fresh = client(&reenrollment, &pki, Duration::from_secs(2))
        .reenroll(&identity_store, &credential_store, &token_with_uid(FRESH_TOKEN_UID))
        .await
        .expect("fresh token reenrolls revoked credential");
    assert_eq!(fresh.certificate_serial, "0c".repeat(16));
    assert_eq!(
        identity_store
            .load()
            .expect("load identity")
            .expect("identity")
            .public_key_der(),
        next.public_key_der()
    );
}

#[test]
fn unconfigured_connect_has_no_side_effects() {
    let temp = tempfile::tempdir().expect("temp dir");
    let directory = temp.path().join("connect");
    assert!(
        ConnectClient::from_optional_config(None)
            .expect("unconfigured is valid")
            .is_none()
    );
    assert!(!directory.exists());
}
