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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType, SerialNumber, SubjectPublicKeyInfo,
};
use rustfs::connect::{
    ClientError, ConnectClient, ConnectConfig, CredentialStore, DeviceCredential, IdentityStore, RegistrationToken,
};
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
        let public_key = SubjectPublicKeyInfo::from_der(&identity.public_key_der()).expect("parse device SPKI");
        let issuer = Issuer::from_params(&self.root_params, &self.root_key);
        let certificate = params.signed_by(&public_key, &issuer).expect("sign device certificate");
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

    fn server_config(&self) -> rustls::ServerConfig {
        let mut roots = RootCertStore::empty();
        roots.add(self.root_der.clone()).expect("add client root");
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .allow_unauthenticated()
            .build()
            .expect("build optional client verifier");
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
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test server");
    let address = listener.local_addr().expect("server address");
    let acceptor = TlsAcceptor::from(Arc::new(pki.server_config()));
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

fn token() -> RegistrationToken {
    let document = json!({
        "registrationTokenUid": TOKEN_UID,
        "registrationTokenSecret": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "organizationUid": ORGANIZATION_UID,
        "clusterUid": CLUSTER_UID,
        "challengeNonce": "a3f1c07d9b2e4856af0c1d3b5e7f9012c4a6b8d0e2f4061738495a6b7c8d9e0f",
        "expiresUnix": OffsetDateTime::now_utc().unix_timestamp() + 3600,
    });
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

#[tokio::test]
async fn registration_reuses_request_and_csr_after_timeout_and_restart() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (identity_store, credential_store) = stores(&temp);
    let identity = identity_store.load_or_create().expect("create identity");
    let pki = TestPki::new();
    let response = pki.credential(&identity, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 1);
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

    let seen = server.seen.lock().expect("seen lock");
    assert_eq!(seen.len(), 4);
    for request in &seen[1..] {
        assert_eq!(request["requestId"], seen[0]["requestId"]);
        assert_eq!(request["certificateRequest"], seen[0]["certificateRequest"]);
    }
}

#[tokio::test]
async fn registration_rejects_untrusted_or_misbound_credentials() {
    for case in ["san", "chain", "key_id", "cluster", "name"] {
        let temp = tempfile::tempdir().expect("temp dir");
        let (identity_store, credential_store) = stores(&temp);
        let identity = identity_store.load_or_create().expect("create identity");
        let pki = TestPki::new();
        let mut response = match case {
            "san" => pki.credential(&identity, "urn:rustfs:connect:device:0198f4b0-3c00-7e30-8f41-4a5b6c7d8e93", 2),
            "chain" => TestPki::new().credential(&identity, &format!("urn:rustfs:connect:device:{DEVICE_UID}"), 2),
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

#[test]
fn credential_store_atomically_replaces_one_owner_only_file() {
    let temp = tempfile::tempdir().expect("temp dir");
    let store = CredentialStore::new(temp.path());
    let credential = |serial: &str| DeviceCredential {
        name: format!("organizations/{ORGANIZATION_UID}/clusters/{CLUSTER_UID}/clusterDevices/{DEVICE_UID}"),
        uid: DEVICE_UID.to_string(),
        protocol_version: "v1".to_string(),
        key_id: format!("x509-{serial}"),
        certificate_serial: serial.to_string(),
        certificate: "leaf".to_string(),
        certificate_chain: "chain".to_string(),
        not_before_unix: 1,
        not_after_unix: 2,
    };
    store.save(&credential("01")).expect("first save");
    store.save(&credential("02")).expect("replacement save");
    assert_eq!(store.load().expect("load").expect("credential").certificate_serial, "02");
    assert_eq!(std::fs::read_dir(temp.path()).expect("read store").count(), 1);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let entry = std::fs::read_dir(temp.path()).expect("read store").next().unwrap().unwrap();
        assert_eq!(entry.metadata().expect("metadata").permissions().mode() & 0o7777, 0o600);
    }
}
