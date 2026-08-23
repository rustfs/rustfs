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
    let temp = tempfile::tempdir().expect("temp directory");
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

#[tokio::test]
async fn response_loss_reuses_the_pending_request_and_existing_credential_is_idempotent() {
    let temp = tempfile::tempdir().expect("temp directory");
    let state = temp.path().join("state");
    let server = server(
        &state,
        vec![
            Reply::DropConnection,
            Reply::DropConnection,
            Reply::DropConnection,
            Reply::Register,
        ],
    )
    .await;
    let (root, token) = prepare_inputs(&temp, &server.root_pem);

    while server.seen.lock().expect("seen lock").len() < 3 {
        let error = register_from_protected_input(&server.endpoint, &root, &state, Some(&token))
            .await
            .expect_err("lost response must leave a retryable failure");
        assert!(!error.to_string().contains(TOKEN_SECRET));
        assert!(state.join("credential/registration.pending.json").is_file());
    }

    let registered = register_from_protected_input(&server.endpoint, &root, &state, Some(&token))
        .await
        .expect("retry registration");
    assert_eq!(registered.device_uid, DEVICE_UID);
    let requests = server.seen.lock().expect("seen lock");
    assert_eq!(requests.len(), 4);
    for request in &requests[1..] {
        assert_eq!(request["requestId"], requests[0]["requestId"]);
        assert_eq!(request["certificateRequest"], requests[0]["certificateRequest"]);
    }
    drop(requests);

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
    let temp = tempfile::tempdir().expect("temp directory");
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
    let temp = tempfile::tempdir().expect("temp directory");
    let server = server(temp.path(), vec![Reply::Reject(StatusCode::BAD_REQUEST, "REGISTRATION_TOKEN_EXPIRED")]).await;
    let (root, token) = prepare_inputs(&temp, &server.root_pem);

    let http_state = temp.path().join("http-state");
    let error = register_from_protected_input("http://localhost/agent/", &root, &http_state, Some(&token))
        .await
        .expect_err("HTTP endpoint must fail");
    assert!(matches!(error, RegistrationBootstrapError::Client(_)));
    assert!(!http_state.exists());

    let malformed = temp.path().join("malformed-token.json");
    write_file(&malformed, b"not a token", 0o600);
    let malformed_state = temp.path().join("malformed-state");
    let error = register_from_protected_input(&server.endpoint, &root, &malformed_state, Some(&malformed))
        .await
        .expect_err("malformed token must fail");
    assert!(matches!(error, RegistrationBootstrapError::Token(_)));
    assert!(!malformed_state.exists());

    let expired = temp.path().join("expired-token.json");
    write_file(&expired, &token_document(OffsetDateTime::now_utc().unix_timestamp() - 1), 0o600);
    let rejected_state = temp.path().join("rejected-state");
    let error = register_from_protected_input(&server.endpoint, &root, &rejected_state, Some(&expired))
        .await
        .expect_err("expired token must be refused by Connect");
    let display = error.to_string();
    assert!(display.contains("REGISTRATION_TOKEN_EXPIRED"));
    assert!(!display.contains(TOKEN_SECRET));
    assert!(!rejected_state.join("credential/device.crt.json").exists());
    assert!(!rejected_state.join("credential/registration.pending.json").exists());

    let other_pki = TestPki::new();
    let wrong_root = temp.path().join("wrong-root.pem");
    write_file(&wrong_root, other_pki.root_pem.as_bytes(), 0o644);
    let wrong_ca_state = temp.path().join("wrong-ca-state");
    let error = register_from_protected_input(&server.endpoint, &wrong_root, &wrong_ca_state, Some(&token))
        .await
        .expect_err("wrong CA must fail TLS");
    assert!(!error.to_string().contains(TOKEN_SECRET));
    assert!(!wrong_ca_state.join("credential/device.crt.json").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn token_ca_and_state_paths_reject_sharing_symlinks_and_non_files() {
    use std::os::unix::fs::{PermissionsExt as _, symlink};

    let pki = TestPki::new();
    let temp = tempfile::tempdir().expect("temp directory");
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
    register_from_protected_input(endpoint, &root, &temp.path().join("token-link-state"), Some(&token_link))
        .await
        .expect_err("token symlink must fail");

    let root_link = temp.path().join("root-link");
    symlink(&root, &root_link).expect("root symlink");
    register_from_protected_input(endpoint, &root_link, &temp.path().join("root-link-state"), Some(&token))
        .await
        .expect_err("CA symlink must fail");

    let state_target = temp.path().join("state-target");
    fs::create_dir(&state_target).expect("state target");
    let state_link = temp.path().join("state-link");
    symlink(&state_target, &state_link).expect("state symlink");
    let error = register_from_protected_input(endpoint, &root, &state_link, Some(&token))
        .await
        .expect_err("state symlink must fail");
    assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));
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
        state.join("identity/device.key"),
        state.join("credential/device.crt.json"),
        state.join("credential/.state.lock"),
    ] {
        let mode = fs::metadata(&path).expect("stored file metadata").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "unexpected mode for {}", path.display());
    }
}
