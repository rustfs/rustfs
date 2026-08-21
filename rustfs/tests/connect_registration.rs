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
use rustfs::connect::{ClientError, ConnectClient, ConnectConfig, CredentialStore, IdentityStore, RegistrationToken, TokenError};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

const ORGANIZATION_UID: &str = "0198f4b0-1a00-7c10-8d21-2e3f4a5b6c70";
const CLUSTER_UID: &str = "0198f4b0-2b00-7d20-9e31-3f4a5b6c7d81";
const DEVICE_UID: &str = "0198f4b0-3c00-7e30-8f41-4a5b6c7d8e92";
const TOKEN_UID: &str = "0198f4b0-6f00-7b60-9271-7d8e9fa0b1c5";

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
        let mut params = CertificateParams::default();
        params.not_before = now;
        params.not_after = now + time::Duration::days(1);
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
            "notBefore": now.format(&Rfc3339).expect("format notBefore"),
            "notAfter": (now + time::Duration::days(1)).format(&Rfc3339).expect("format notAfter"),
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
}

struct TestServer {
    endpoint: String,
    seen: Arc<Mutex<Vec<Value>>>,
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
    let captured = seen.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            let replies = replies.clone();
            let seen = captured.clone();
            tokio::spawn(async move {
                let Ok(stream) = acceptor.accept(stream).await else {
                    return;
                };
                let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                    let replies = replies.clone();
                    let seen = seen.clone();
                    async move {
                        let body = request.into_body().collect().await.expect("read request body").to_bytes();
                        let value = serde_json::from_slice(&body).expect("request JSON");
                        seen.lock().expect("seen lock").push(value);
                        let reply = replies.lock().expect("reply lock").pop_front().expect("planned reply");
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
        task,
    }
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
    let document = token_document();
    RegistrationToken::from_reader(serde_json::to_vec(&document).expect("token JSON").as_slice()).expect("token parses")
}

fn stores(temp: &tempfile::TempDir) -> (IdentityStore, CredentialStore) {
    (
        IdentityStore::new(temp.path().join("identity")),
        CredentialStore::new(temp.path().join("credential")),
    )
}

fn client(server: &TestServer, pki: &TestPki, timeout: Duration) -> ConnectClient {
    ConnectClient::new(ConnectConfig {
        endpoint: &server.endpoint,
        root_ca_pem: pki.root_pem.as_bytes(),
        timeout,
    })
    .expect("build Connect client")
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
        assert!(credential_store.load().expect("load store").is_none());
    }
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
async fn concurrent_rotation_retries_converge_and_promote_the_next_key() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (identity_store, credential_store) = stores(&temp);
    let current = identity_store.load_or_create().expect("create identity");
    let pki = TestPki::new();
    let issued = pki.credential(&current, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 5);
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
    let due = registered.not_after_unix - 8 * 60 * 60;
    let (first, second) = tokio::join!(
        retry_client.rotate_if_due(&identity_store, &credential_store, due),
        retry_client.rotate_if_due(&identity_store, &credential_store, due)
    );
    assert!(matches!(first, Err(ClientError::Unavailable { .. })));
    assert!(matches!(second, Err(ClientError::Unavailable { .. })));
    let seen = retries.seen.lock().expect("seen lock");
    assert!(seen.len() >= 3, "bounded retries must reach the server");
    for request in &seen[1..] {
        assert_eq!(request["requestId"], seen[0]["requestId"]);
        assert_eq!(request["certificateRequest"], seen[0]["certificateRequest"]);
    }
    let request_id = seen[0]["requestId"].clone();
    let certificate_request = seen[0]["certificateRequest"].clone();
    drop(seen);

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
    let success = server_with_client_auth(&pki, vec![Reply::Json(StatusCode::OK, rotated)], true).await;
    let credential = client(&success, &pki, Duration::from_secs(2))
        .rotate_if_due(&identity_store, &credential_store, due)
        .await
        .expect("retry rotation")
        .expect("rotation due");
    assert_eq!(credential.certificate_serial, "06".repeat(16));
    let success_seen = success.seen.lock().expect("seen lock");
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
    let due = registered.not_after_unix - 8 * 60 * 60;
    assert!(matches!(
        client(&failed, &pki, Duration::from_secs(2))
            .rotate_if_due(&identity_store, &credential_store, due)
            .await,
        Err(ClientError::Unavailable { .. })
    ));

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
    let server = server(
        &pki,
        vec![
            Reply::Json(StatusCode::CREATED, issued),
            Reply::Json(StatusCode::UNAUTHORIZED, json!({"details": [{"reason": "DEVICE_REVOKED"}]})),
        ],
    )
    .await;
    let client = client(&server, &pki, Duration::from_secs(2));
    let current = client
        .register(&identity_store, &credential_store, &token())
        .await
        .expect("register");
    assert!(
        client
            .rotate_if_due(&identity_store, &credential_store, current.not_before_unix)
            .await
            .expect("not due")
            .is_none()
    );
    let error = client
        .rotate_if_due(&identity_store, &credential_store, current.not_after_unix - 8 * 60 * 60)
        .await
        .expect_err("revocation must stop rotation");
    assert!(matches!(error, ClientError::AccessRevoked { .. }));
    assert_eq!(server.seen.lock().expect("seen lock").len(), 2);
    assert_eq!(
        credential_store
            .load()
            .expect("load credential")
            .expect("stored credential")
            .certificate_serial,
        current.certificate_serial
    );
}

#[test]
fn unconfigured_connect_and_credential_reads_have_no_side_effects() {
    let temp = tempfile::tempdir().expect("temp dir");
    let directory = temp.path().join("connect");
    assert!(
        ConnectClient::from_optional_config(None)
            .expect("unconfigured is valid")
            .is_none()
    );
    assert!(CredentialStore::new(&directory).load().expect("read empty store").is_none());
    assert!(!directory.exists());
}
