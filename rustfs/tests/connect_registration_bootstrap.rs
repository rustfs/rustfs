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

#![cfg(unix)]

use std::collections::VecDeque;
use std::fs;
use std::io::{self, Write as _};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType, SerialNumber,
};
use rustfs::connect::{IdentityStore, RegistrationBootstrapError, register_from_protected_input};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

const ORGANIZATION_UID: &str = "0198f4b0-1a00-7c10-8d21-2e3f4a5b6c70";
const CLUSTER_UID: &str = "0198f4b0-2b00-7d20-9e31-3f4a5b6c7d81";
const DEVICE_UID: &str = "0198f4b0-3c00-7e30-8f41-4a5b6c7d8e92";
const TOKEN_UID: &str = "0198f4b0-6f00-7b60-9271-7d8e9fa0b1c5";
const TOKEN_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const REMOTE_REASON: &str = "remote-reason-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

struct TestPki {
    root_params: CertificateParams,
    root_key: KeyPair,
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
        root_params
            .distinguished_name
            .push(DnType::CommonName, "Connect bootstrap test root");
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
            root_pem: root.pem(),
            server_der: server.der().clone(),
            server_key: PrivatePkcs8KeyDer::from(server_key.serialize_der()),
        }
    }

    fn server_config(&self) -> rustls::ServerConfig {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![self.server_der.clone()], PrivateKeyDer::Pkcs8(self.server_key.clone_key()))
            .expect("build server TLS")
    }

    fn credential(&self, identity: &rustfs::connect::DeviceIdentity) -> Value {
        let now = OffsetDateTime::now_utc()
            .replace_nanosecond(0)
            .expect("whole-second test time");
        let mut params = CertificateParams::default();
        params.not_before = now;
        params.not_after = now + time::Duration::days(1);
        params.serial_number = Some(SerialNumber::from(vec![7; 16]));
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, DEVICE_UID);
        params.subject_alt_names = vec![SanType::URI(
            format!("urn:rustfs:connect:device:{DEVICE_UID}")
                .try_into()
                .expect("valid URI SAN"),
        )];
        let private_key = identity.to_pkcs8_der().expect("serialize device key");
        let private_key = PrivatePkcs8KeyDer::from(private_key.to_vec());
        let device_key =
            KeyPair::from_pkcs8_der_and_sign_algo(&private_key, &rcgen::PKCS_ECDSA_P256_SHA256).expect("parse device key");
        let issuer = Issuer::from_params(&self.root_params, &self.root_key);
        let certificate = params.signed_by(&device_key, &issuer).expect("sign device certificate");
        let serial = "07".repeat(16);
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
            "notBefore": now.format(&Rfc3339).expect("format notBefore"),
            "notAfter": (now + time::Duration::days(1)).format(&Rfc3339).expect("format notAfter"),
        })
    }
}

#[derive(Clone)]
enum Reply {
    Register,
    RegisterAfter(Duration),
    Reject(StatusCode, &'static str),
    DropConnection,
}

struct TestServer {
    endpoint: String,
    root_pem: String,
    seen: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn assert_reused_pending_requests(server: &TestServer) {
    let requests = server.seen.lock().expect("seen lock");
    assert_eq!(requests.len(), 4);
    for request in &requests[1..] {
        assert_eq!(request["requestId"], requests[0]["requestId"]);
        assert_eq!(request["certificateRequest"], requests[0]["certificateRequest"]);
    }
}

async fn server(state_directory: &std::path::Path, replies: Vec<Reply>) -> TestServer {
    let pki = Arc::new(TestPki::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test server");
    let address = listener.local_addr().expect("server address");
    let acceptor = TlsAcceptor::from(Arc::new(pki.server_config()));
    let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let root_pem = pki.root_pem.clone();
    let identity_directory = state_directory.join("identity");
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            let replies = replies.clone();
            let seen = captured.clone();
            let pki = pki.clone();
            let identity_directory = identity_directory.clone();
            tokio::spawn(async move {
                let Ok(stream) = acceptor.accept(stream).await else {
                    return;
                };
                let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                    let replies = replies.clone();
                    let seen = seen.clone();
                    let pki = pki.clone();
                    let identity_directory = identity_directory.clone();
                    async move {
                        let body = request.into_body().collect().await.expect("read request body").to_bytes();
                        let request: Value = serde_json::from_slice(&body).expect("request JSON");
                        seen.lock().expect("seen lock").push(request.clone());
                        let reply = replies.lock().expect("reply lock").pop_front().expect("planned reply");
                        let (status, response) = match reply {
                            Reply::Register => {
                                let identity = IdentityStore::new(&identity_directory)
                                    .load()
                                    .expect("load bootstrap identity")
                                    .expect("bootstrap identity exists before exchange");
                                (StatusCode::CREATED, pki.credential(&identity))
                            }
                            Reply::RegisterAfter(delay) => {
                                tokio::time::sleep(delay).await;
                                let identity = IdentityStore::new(&identity_directory)
                                    .load()
                                    .expect("load bootstrap identity")
                                    .expect("bootstrap identity exists before exchange");
                                (StatusCode::CREATED, pki.credential(&identity))
                            }
                            Reply::Reject(status, reason) => (status, json!({"details": [{"reason": reason}]})),
                            Reply::DropConnection => {
                                return Err::<Response<Full<Bytes>>, io::Error>(io::Error::new(
                                    io::ErrorKind::ConnectionAborted,
                                    "planned response loss",
                                ));
                            }
                        };
                        Ok(Response::builder()
                            .status(status)
                            .header("content-type", "application/json")
                            .body(Full::new(Bytes::from(serde_json::to_vec(&response).expect("reply JSON"))))
                            .expect("response"))
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
        root_pem,
        seen,
        task,
    }
}

fn token_document(expires_unix: i64) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "registrationTokenUid": TOKEN_UID,
        "registrationTokenSecret": TOKEN_SECRET,
        "organizationUid": ORGANIZATION_UID,
        "clusterUid": CLUSTER_UID,
        "challengeNonce": "a3f1c07d9b2e4856af0c1d3b5e7f9012c4a6b8d0e2f4061738495a6b7c8d9e0f",
        "expiresUnix": expires_unix,
    }))
    .expect("token JSON")
}

fn write_file(path: &std::path::Path, bytes: &[u8], mode: u32) {
    fs::write(path, bytes).expect("write test file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("set test mode");
    }
    let _ = mode;
}

fn prepare_inputs(temp: &tempfile::TempDir, root_pem: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let root = temp.path().join("root.pem");
    let token = temp.path().join("token.json");
    write_file(&root, root_pem.as_bytes(), 0o644);
    write_file(&token, &token_document(OffsetDateTime::now_utc().unix_timestamp() + 3600), 0o600);
    (root, token)
}

fn secure_tempdir() -> tempfile::TempDir {
    let home = std::fs::canonicalize(std::env::var_os("HOME").expect("test requires a protected home directory"))
        .expect("test home directory must resolve without symlink components");
    tempfile::Builder::new()
        .prefix(".connect-registration-")
        .tempdir_in(home)
        .expect("temporary directory inside the protected home directory")
}

fn run_binary(endpoint: String, root: std::path::PathBuf, state: std::path::PathBuf, token: Vec<u8>) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustfs"))
        .args(["connect", "register", "--endpoint"])
        .arg(endpoint)
        .arg("--ca-file")
        .arg(root)
        .arg("--state-dir")
        .arg(state)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start production rustfs binary");
    child
        .stdin
        .take()
        .expect("command stdin")
        .write_all(&token)
        .expect("write protected token to stdin");
    child.wait_with_output().expect("wait for registration command")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_command_registers_once_and_emits_only_stable_identifiers() {
    let temp = secure_tempdir();
    let state = temp.path().join("state");
    let server = server(&state, vec![Reply::Register]).await;
    let (root, _) = prepare_inputs(&temp, &server.root_pem);
    let token = token_document(OffsetDateTime::now_utc().unix_timestamp() + 3600);
    let output = tokio::task::spawn_blocking({
        let endpoint = server.endpoint.clone();
        let state = state.clone();
        move || run_binary(endpoint, root, state, token)
    })
    .await
    .expect("registration process task");

    assert!(output.status.success(), "stderr: {}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(
        String::from_utf8(output.stdout).expect("UTF-8 stdout"),
        format!("device={DEVICE_UID} cluster=organizations/{ORGANIZATION_UID}/clusters/{CLUSTER_UID}\n")
    );
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
    assert!(stderr.is_empty());
    assert!(!stderr.contains(TOKEN_SECRET));
    assert_eq!(server.seen.lock().expect("seen lock").len(), 1);
    assert!(
        IdentityStore::new(state.join("identity"))
            .load()
            .expect("load identity")
            .is_some()
    );
    assert!(state.join("credential/device.crt.json").is_file());
    assert!(!state.join("credential/registration.pending.json").exists());
    assert_no_staging_files(&state);
    #[cfg(unix)]
    assert_owner_only_files(&state);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_command_never_echoes_a_remote_reason() {
    let temp = secure_tempdir();
    let state = temp.path().join("state");
    let server = server(
        &state,
        vec![
            Reply::Reject(StatusCode::BAD_REQUEST, REMOTE_REASON),
            Reply::Reject(StatusCode::BAD_REQUEST, REMOTE_REASON),
        ],
    )
    .await;
    let (root, token_file) = prepare_inputs(&temp, &server.root_pem);
    let error = register_from_protected_input(&server.endpoint, &root, &temp.path().join("direct-state"), Some(&token_file))
        .await
        .expect_err("remote rejection must fail");
    assert_sanitized_error(&error, &[TOKEN_SECRET, REMOTE_REASON]);

    let token = token_document(OffsetDateTime::now_utc().unix_timestamp() + 3600);
    let output = tokio::task::spawn_blocking({
        let endpoint = server.endpoint.clone();
        let state = state.clone();
        move || run_binary(endpoint, root, state, token)
    })
    .await
    .expect("registration process task");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
    assert_eq!(stderr, "[FATAL] Server runtime failed: Connect registration exchange failed\n");
    for forbidden in [TOKEN_SECRET, REMOTE_REASON] {
        assert!(!stderr.contains(forbidden));
    }
    assert_eq!(server.seen.lock().expect("seen lock").len(), 2);
}

#[tokio::test]
async fn response_loss_reuses_the_pending_request_and_existing_credential_is_idempotent() {
    let temp = secure_tempdir();
    let state = temp.path().join("state");
    let flaky_server = server(
        &state,
        vec![
            Reply::DropConnection,
            Reply::DropConnection,
            Reply::DropConnection,
            Reply::Register,
        ],
    )
    .await;
    let (root, token) = prepare_inputs(&temp, &flaky_server.root_pem);

    while flaky_server.seen.lock().expect("seen lock").len() < 3 {
        let error = register_from_protected_input(&flaky_server.endpoint, &root, &state, Some(&token))
            .await
            .expect_err("lost response must leave a retryable failure");
        assert!(!error.to_string().contains(TOKEN_SECRET));
        assert!(state.join("credential/registration.pending.json").is_file());
    }

    let registered = register_from_protected_input(&flaky_server.endpoint, &root, &state, Some(&token))
        .await
        .expect("retry registration");
    assert_eq!(registered.device_uid, DEVICE_UID);
    assert_reused_pending_requests(&flaky_server);

    let idle = server(&state, vec![]).await;
    let idempotent = register_from_protected_input(&idle.endpoint, &root, &state, Some(&token))
        .await
        .expect("valid stored credential is idempotent");
    assert_eq!(idempotent, registered);
    assert!(idle.seen.lock().expect("seen lock").is_empty());
    assert!(!state.join("credential/registration.pending.json").exists());
    assert_no_staging_files(&state);
}

#[tokio::test]
async fn concurrent_bootstraps_share_one_identity_and_one_exchange() {
    let temp = secure_tempdir();
    let state = temp.path().join("state");
    let server = server(&state, vec![Reply::RegisterAfter(Duration::from_millis(150))]).await;
    let (root, token) = prepare_inputs(&temp, &server.root_pem);

    let (first, second) = tokio::join!(
        register_from_protected_input(&server.endpoint, &root, &state, Some(&token)),
        register_from_protected_input(&server.endpoint, &root, &state, Some(&token)),
    );
    assert_eq!(first.expect("first registration"), second.expect("second registration"));
    assert_eq!(server.seen.lock().expect("seen lock").len(), 1);
    assert_no_staging_files(&state);
}

#[tokio::test]
async fn endpoint_ca_token_state_and_service_failures_are_closed_and_sanitized() {
    let temp = secure_tempdir();
    let server = server(temp.path(), vec![Reply::Reject(StatusCode::BAD_REQUEST, "REGISTRATION_TOKEN_EXPIRED")]).await;
    let (root, token) = prepare_inputs(&temp, &server.root_pem);

    let http_state = temp.path().join("http-state");
    let error = register_from_protected_input("http://localhost/agent/", &root, &http_state, Some(&token))
        .await
        .expect_err("HTTP endpoint must fail");
    assert!(matches!(error, RegistrationBootstrapError::Configuration));
    assert!(!http_state.exists());

    let malformed = temp.path().join("malformed-token.json");
    write_file(&malformed, b"not a token", 0o600);
    let malformed_state = temp.path().join("malformed-state");
    let error = register_from_protected_input(&server.endpoint, &root, &malformed_state, Some(&malformed))
        .await
        .expect_err("malformed token must fail");
    assert!(matches!(error, RegistrationBootstrapError::Token(_)));
    assert_eq!(
        fs::read(malformed_state.join(".bootstrap-ready")).expect("read durable state marker"),
        b"v1\n"
    );
    assert_no_staging_files(&malformed_state);

    let expired = temp.path().join("expired-token.json");
    write_file(&expired, &token_document(OffsetDateTime::now_utc().unix_timestamp() - 1), 0o600);
    let rejected_state = temp.path().join("rejected-state");
    let error = register_from_protected_input(&server.endpoint, &root, &rejected_state, Some(&expired))
        .await
        .expect_err("expired token must be refused by Connect");
    assert!(matches!(&error, RegistrationBootstrapError::Exchange));
    assert_eq!(error.to_string(), "Connect registration exchange failed");
    assert_sanitized_error(&error, &[TOKEN_SECRET, "REGISTRATION_TOKEN_EXPIRED"]);
    assert!(!rejected_state.join("credential/device.crt.json").exists());
    assert!(!rejected_state.join("credential/registration.pending.json").exists());

    let other_pki = TestPki::new();
    let wrong_root = temp.path().join("wrong-root.pem");
    write_file(&wrong_root, other_pki.root_pem.as_bytes(), 0o644);
    let wrong_ca_state = temp.path().join("wrong-ca-state");
    let error = register_from_protected_input(&server.endpoint, &wrong_root, &wrong_ca_state, Some(&token))
        .await
        .expect_err("wrong CA must fail TLS");
    assert!(matches!(error, RegistrationBootstrapError::Exchange));
    assert!(!wrong_ca_state.join("credential/device.crt.json").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn token_ca_and_state_paths_reject_sharing_symlinks_and_non_files() {
    use std::os::unix::fs::{PermissionsExt as _, symlink};

    let pki = TestPki::new();
    let temp = secure_tempdir();
    let (root, token) = prepare_inputs(&temp, &pki.root_pem);
    let endpoint = "https://localhost:1/agent/";

    fs::set_permissions(&token, fs::Permissions::from_mode(0o640)).expect("share token mode");
    let error = register_from_protected_input(endpoint, &root, &temp.path().join("shared"), Some(&token))
        .await
        .expect_err("group-readable token must fail");
    assert!(matches!(error, RegistrationBootstrapError::TokenFileSecurity));

    fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).expect("restore token mode");
    let token_link = temp.path().join("token-link");
    symlink(&token, &token_link).expect("token symlink");
    let error = register_from_protected_input(endpoint, &root, &temp.path().join("token-link-state"), Some(&token_link))
        .await
        .expect_err("token symlink must fail");
    assert!(matches!(error, RegistrationBootstrapError::TokenFileSecurity));

    let root_link = temp.path().join("root-link");
    symlink(&root, &root_link).expect("root symlink");
    let error = register_from_protected_input(endpoint, &root_link, &temp.path().join("root-link-state"), Some(&token))
        .await
        .expect_err("CA symlink must fail");
    assert!(matches!(error, RegistrationBootstrapError::RootCaFileSecurity));

    for mode in [0o664, 0o646] {
        fs::set_permissions(&root, fs::Permissions::from_mode(mode)).expect("make CA writable by another user");
        let error =
            register_from_protected_input(endpoint, &root, &temp.path().join(format!("writable-ca-{mode:o}")), Some(&token))
                .await
                .expect_err("group/world-writable CA must fail");
        assert!(matches!(error, RegistrationBootstrapError::RootCaFileSecurity));
    }
    fs::set_permissions(&root, fs::Permissions::from_mode(0o644)).expect("restore CA mode");

    let missing_parent = temp.path().join("missing-parent");
    fs::set_permissions(&token, fs::Permissions::from_mode(0o640)).expect("make token unsafe behind parent gate");
    let error = register_from_protected_input(endpoint, &root, &missing_parent.join("state"), Some(&token))
        .await
        .expect_err("missing state parent must fail before token access or network");
    assert!(matches!(error, RegistrationBootstrapError::StateParentRequired));
    assert!(!missing_parent.exists(), "bootstrap must not create the durable parent");
    fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).expect("restore token after parent gate");

    let parent_state = temp.path().join("secure/connect/..");
    fs::set_permissions(&token, fs::Permissions::from_mode(0o640)).expect("make token unsafe behind state gate");
    let error = register_from_protected_input(endpoint, &root, &parent_state, Some(&token))
        .await
        .expect_err("parent state component must fail before token access or network");
    assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));
    assert!(!temp.path().join("secure").exists());
    assert!(!parent_state.join(".bootstrap-ready").exists());
    fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).expect("restore token after state gate");

    let marker_state = temp.path().join("unsafe-marker-state");
    for directory in [
        marker_state.clone(),
        marker_state.join("identity"),
        marker_state.join("credential"),
    ] {
        fs::create_dir_all(&directory).expect("create marker state directory");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).expect("secure marker state directory");
    }
    let marker_target = temp.path().join("unsafe-marker-target");
    write_file(&marker_target, b"v1\n", 0o600);
    symlink(marker_target, marker_state.join(".bootstrap-ready")).expect("ready marker symlink");
    fs::set_permissions(&token, fs::Permissions::from_mode(0o640)).expect("make token unsafe behind marker gate");
    let error = register_from_protected_input(endpoint, &root, &marker_state, Some(&token))
        .await
        .expect_err("unsafe marker must fail before token access or network");
    assert!(matches!(error, RegistrationBootstrapError::StateMarkerSecurity));
    fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).expect("restore token after marker gate");

    let shared_state = temp.path().join("shared-state");
    fs::create_dir(&shared_state).expect("shared state directory");
    fs::set_permissions(&shared_state, fs::Permissions::from_mode(0o770)).expect("make state group-writable");
    let error = register_from_protected_input(endpoint, &root, &shared_state, Some(&token))
        .await
        .expect_err("shared-writable state must fail");
    assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));

    let shared_ancestor = temp.path().join("shared-ancestor");
    fs::create_dir(&shared_ancestor).expect("shared ancestor directory");
    fs::set_permissions(&shared_ancestor, fs::Permissions::from_mode(0o770)).expect("make ancestor group-writable");
    let error = register_from_protected_input(endpoint, &root, &shared_ancestor.join("state"), Some(&token))
        .await
        .expect_err("shared-writable state ancestor must fail");
    assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));

    let ancestor_target = temp.path().join("ancestor-target");
    fs::create_dir(&ancestor_target).expect("ancestor target");
    let ancestor_link = temp.path().join("ancestor-link");
    symlink(&ancestor_target, &ancestor_link).expect("ancestor symlink");
    let error = register_from_protected_input(endpoint, &root, &ancestor_link.join("state"), Some(&token))
        .await
        .expect_err("state ancestor symlink must fail");
    assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));

    for nested in ["identity", "credential"] {
        let state = temp.path().join(format!("nested-{nested}"));
        fs::create_dir(&state).expect("state directory");
        let target = temp.path().join(format!("{nested}-target"));
        fs::create_dir(&target).expect("nested target");
        symlink(&target, state.join(nested)).expect("nested store symlink");
        let error = register_from_protected_input(endpoint, &root, &state, Some(&token))
            .await
            .expect_err("nested store symlink must fail");
        assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));
    }

    for (nested, mode) in [("identity", 0o720), ("credential", 0o702)] {
        let state = temp.path().join(format!("writable-{nested}"));
        fs::create_dir(&state).expect("state directory");
        let nested = state.join(nested);
        fs::create_dir(&nested).expect("nested store directory");
        fs::set_permissions(&nested, fs::Permissions::from_mode(mode)).expect("make nested store writable");
        let error = register_from_protected_input(endpoint, &root, &state, Some(&token))
            .await
            .expect_err("writable nested store must fail");
        assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));
    }
}

fn assert_sanitized_error(error: &(dyn std::error::Error + 'static), forbidden: &[&str]) {
    let mut current = Some(error);
    while let Some(candidate) = current {
        let display = candidate.to_string();
        let debug = format!("{candidate:?}");
        for forbidden in forbidden {
            assert!(!display.contains(forbidden), "error Display leaked forbidden text");
            assert!(!debug.contains(forbidden), "error Debug leaked forbidden text");
        }
        current = candidate.source();
    }
}

fn assert_no_staging_files(root: &std::path::Path) {
    fn visit(path: &std::path::Path, found: &mut Vec<String>) {
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        for entry in entries {
            let entry = entry.expect("directory entry");
            let path = entry.path();
            if path.is_dir() {
                visit(&path, found);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with('.') && name.ends_with(".tmp"))
            {
                found.push(path.display().to_string());
            }
        }
    }

    let mut found = Vec::new();
    visit(root, &mut found);
    assert!(found.is_empty(), "staging files remained: {found:?}");
}

#[cfg(unix)]
fn assert_owner_only_files(state: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt as _;

    for path in [
        state.to_path_buf(),
        state.join(".bootstrap-ready"),
        state.join("identity"),
        state.join("credential"),
        state.join("identity/device.key"),
        state.join("credential/device.crt.json"),
        state.join("credential/.state.lock"),
    ] {
        let mode = fs::metadata(&path).expect("stored file metadata").permissions().mode() & 0o777;
        let expected = if path.is_dir() { 0o700 } else { 0o600 };
        assert_eq!(mode, expected, "unexpected mode for {}", path.display());
    }
}
