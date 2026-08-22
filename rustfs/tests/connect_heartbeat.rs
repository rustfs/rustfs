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
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use rustfs::connect::{
    CoarseNodeSummary, CredentialStore, DeviceCredential, HeartbeatConfig, HeartbeatSchedule, HeartbeatStatus, IdentityStore,
    spawn_heartbeat_runtime,
};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

const ORGANIZATION_UID: &str = "0198f4b0-1a00-7c10-8d21-2e3f4a5b6c70";
const CLUSTER_UID: &str = "0198f4b0-2b00-7d20-9e31-3f4a5b6c7d81";
const DEVICE_UID: &str = "0198f4b0-3c00-7e30-8f41-4a5b6c7d8e92";

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
        root_params.not_before = now - time::Duration::days(30);
        root_params.not_after = now + time::Duration::days(30);
        root_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::DigitalSignature];
        let root = root_params.self_signed(&root_key).expect("sign root");

        let server_key = KeyPair::generate().expect("generate server key");
        let mut server_params = CertificateParams::default();
        server_params.not_before = now - time::Duration::hours(1);
        server_params.not_after = now + time::Duration::days(2);
        server_params
            .subject_alt_names
            .push(SanType::DnsName("localhost".try_into().expect("valid DNS name")));
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server = server_params
            .signed_by(&server_key, &Issuer::from_params(&root_params, &root_key))
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

    fn server_config(&self) -> rustls::ServerConfig {
        let mut roots = RootCertStore::empty();
        roots.add(self.root_der.clone()).expect("add client root");
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .expect("client verifier");
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![self.server_der.clone()], PrivateKeyDer::Pkcs8(self.server_key.clone_key()))
            .expect("server TLS")
    }

    fn stores(&self, temp: &tempfile::TempDir) -> (IdentityStore, CredentialStore) {
        let now = OffsetDateTime::now_utc();
        self.stores_with_certificate(temp, now - time::Duration::hours(1), now + time::Duration::hours(23), true)
    }

    fn stores_with_certificate(
        &self,
        temp: &tempfile::TempDir,
        not_before: OffsetDateTime,
        not_after: OffsetDateTime,
        bind_identity: bool,
    ) -> (IdentityStore, CredentialStore) {
        let identity_store = IdentityStore::new(temp.path().join("identity"));
        let identity = identity_store.load_or_create().expect("create identity");
        let private_key = PrivatePkcs8KeyDer::from(identity.to_pkcs8_der().expect("serialize key").to_vec());
        let device_key = if bind_identity {
            KeyPair::from_pkcs8_der_and_sign_algo(&private_key, &rcgen::PKCS_ECDSA_P256_SHA256).expect("device key")
        } else {
            KeyPair::generate().expect("mismatched device key")
        };
        let mut params = CertificateParams::default();
        params.not_before = not_before;
        params.not_after = not_after;
        params.serial_number = Some(vec![1; 16].into());
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, DEVICE_UID);
        params.subject_alt_names.push(SanType::URI(
            format!("urn:rustfs:connect:device:{DEVICE_UID}")
                .try_into()
                .expect("device URI"),
        ));
        let certificate = params
            .signed_by(&device_key, &Issuer::from_params(&self.root_params, &self.root_key))
            .expect("device certificate");
        let cluster = format!("organizations/{ORGANIZATION_UID}/clusters/{CLUSTER_UID}");
        let credential = DeviceCredential {
            name: format!("{cluster}/clusterDevices/{DEVICE_UID}"),
            uid: DEVICE_UID.to_owned(),
            protocol_version: "v1".to_owned(),
            key_id: format!("x509-{}", "01".repeat(16)),
            certificate_serial: "01".repeat(16),
            certificate: certificate.pem(),
            certificate_chain: certificate.pem(),
            not_before_unix: not_before.unix_timestamp(),
            not_after_unix: not_after.unix_timestamp(),
        };
        let directory = temp.path().join("credential");
        fs::create_dir_all(&directory).expect("credential directory");
        let path = directory.join("device.crt.json");
        fs::write(&path, serde_json::to_vec(&credential).expect("credential JSON")).expect("write credential");
        private_mode(&path);
        (identity_store, CredentialStore::new(directory))
    }
}

#[derive(Clone)]
struct Reply {
    status: StatusCode,
    body: Value,
    retry_after: Option<&'static str>,
    delay: Duration,
}

impl Reply {
    fn ok(time: &str) -> Self {
        Self {
            status: StatusCode::OK,
            body: json!({
                "serverTime": time,
                "acceptedVersion": "v1",
                "capabilityHints": [],
                "futureField": true
            }),
            retry_after: None,
            delay: Duration::ZERO,
        }
    }

    fn error(status: StatusCode) -> Self {
        Self {
            status,
            body: json!({"details": []}),
            retry_after: None,
            delay: Duration::ZERO,
        }
    }
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
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let address = listener.local_addr().expect("server address");
    let acceptor = TlsAcceptor::from(Arc::new(pki.server_config()));
    let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            let replies = replies.clone();
            let seen = captured.clone();
            tokio::spawn(async move {
                let Ok(stream) = acceptor.accept(stream).await else { return };
                let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                    let replies = replies.clone();
                    let seen = seen.clone();
                    async move {
                        assert_eq!(request.uri().path(), format!("/agent/clusters/{CLUSTER_UID}/heartbeats"));
                        let body = request.into_body().collect().await.expect("request body").to_bytes();
                        seen.lock()
                            .expect("seen lock")
                            .push(serde_json::from_slice(&body).expect("request JSON"));
                        let reply = replies
                            .lock()
                            .expect("reply lock")
                            .pop_front()
                            .unwrap_or_else(|| Reply::error(StatusCode::SERVICE_UNAVAILABLE));
                        if !reply.delay.is_zero() {
                            tokio::time::sleep(reply.delay).await;
                        }
                        let mut builder = Response::builder()
                            .status(reply.status)
                            .header("content-type", "application/json");
                        if let Some(value) = reply.retry_after {
                            builder = builder.header("retry-after", value);
                        }
                        Ok::<_, hyper::Error>(
                            builder
                                .body(Full::new(Bytes::from(serde_json::to_vec(&reply.body).expect("reply JSON"))))
                                .expect("reply"),
                        )
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

fn config(temp: &tempfile::TempDir, pki: &TestPki, server: &TestServer) -> HeartbeatConfig {
    let (identity_store, credential_store) = pki.stores(temp);
    config_with_stores(temp, pki, server, identity_store, credential_store)
}

fn config_with_stores(
    temp: &tempfile::TempDir,
    pki: &TestPki,
    server: &TestServer,
    identity_store: IdentityStore,
    credential_store: CredentialStore,
) -> HeartbeatConfig {
    HeartbeatConfig {
        endpoint: server.endpoint.clone(),
        root_ca_pem: pki.root_pem.as_bytes().to_vec(),
        identity_store,
        credential_store,
        state_path: temp.path().join("heartbeat/state.json"),
        schedule: HeartbeatSchedule {
            cadence: Duration::from_millis(40),
            jitter: Duration::ZERO,
            timeout: Duration::from_millis(200),
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(80),
        },
    }
}

fn rewrite_credential(temp: &tempfile::TempDir, update: impl FnOnce(&mut DeviceCredential)) {
    let path = temp.path().join("credential/device.crt.json");
    let mut credential: DeviceCredential =
        serde_json::from_slice(&fs::read(&path).expect("read credential")).expect("parse credential");
    update(&mut credential);
    fs::write(&path, serde_json::to_vec(&credential).expect("credential JSON")).expect("rewrite credential");
    private_mode(&path);
}

fn summary() -> CoarseNodeSummary {
    CoarseNodeSummary::new(8, 7, 1).expect("node summary")
}

async fn wait_for(
    status: &mut watch::Receiver<HeartbeatStatus>,
    predicate: impl Fn(&HeartbeatStatus) -> bool,
) -> HeartbeatStatus {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let current = status.borrow_and_update().clone();
            if predicate(&current) {
                return current;
            }
            status.changed().await.expect("status channel");
        }
    })
    .await
    .expect("heartbeat status timeout")
}

async fn assert_credential_failure(config: HeartbeatConfig, server: &TestServer, expected: &str) {
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config), &shutdown, summary)
        .expect("start runtime")
        .expect("configured runtime");
    let mut status = runtime.status();
    assert!(matches!(
        wait_for(&mut status, |status| matches!(status, HeartbeatStatus::Failed { .. })).await,
        HeartbeatStatus::Failed { reason } if reason.contains(expected)
    ));
    assert!(server.seen.lock().expect("seen lock").is_empty());
    runtime.shutdown().await;
}

#[tokio::test]
async fn connect_config_absent_starts_no_task() {
    let shutdown = CancellationToken::new();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sampled = calls.clone();
    let runtime = spawn_heartbeat_runtime(None, &shutdown, move || {
        sampled.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        summary()
    })
    .expect("absent config");

    assert!(runtime.is_none());
    tokio::task::yield_now().await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[tokio::test]
async fn duplicate_runtime_is_rejected_without_a_second_task() {
    let pki = TestPki::new();
    let mut reply = Reply::ok("2026-08-22T01:02:03Z");
    reply.delay = Duration::from_secs(5);
    let server = server(&pki, vec![reply]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let shutdown = CancellationToken::new();
    let config = config(&temp, &pki, &server);
    let runtime = spawn_heartbeat_runtime(Some(config.clone()), &shutdown, summary)
        .expect("first runtime")
        .expect("configured runtime");

    assert!(matches!(
        spawn_heartbeat_runtime(Some(config), &shutdown, summary),
        Err(rustfs::connect::HeartbeatError::AlreadyRunning)
    ));
    runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_runtime_keeps_the_lock_until_its_task_stops() {
    let pki = TestPki::new();
    let server = server(&pki, vec![Reply::ok("2026-08-22T01:02:03Z")]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let shutdown = CancellationToken::new();
    let config = config(&temp, &pki, &server);
    let runtime = spawn_heartbeat_runtime(Some(config.clone()), &shutdown, summary)
        .expect("first runtime")
        .expect("configured runtime");

    drop(runtime);
    assert!(matches!(
        spawn_heartbeat_runtime(Some(config.clone()), &shutdown, summary),
        Err(rustfs::connect::HeartbeatError::AlreadyRunning)
    ));

    let replacement = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match spawn_heartbeat_runtime(Some(config.clone()), &shutdown, summary) {
                Ok(Some(runtime)) => break runtime,
                Err(rustfs::connect::HeartbeatError::AlreadyRunning) => tokio::task::yield_now().await,
                Ok(None) => panic!("configured replacement returned no runtime"),
                Err(error) => panic!("unexpected replacement error: {error}"),
            }
        }
    })
    .await
    .expect("dropped runtime releases its lock after stopping");
    replacement.shutdown().await;
}

#[tokio::test]
async fn corrupt_persisted_state_is_rejected_before_network_delivery() {
    let pki = TestPki::new();
    let server = server(&pki, vec![Reply::ok("2026-08-22T01:02:03Z")]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let shutdown = CancellationToken::new();
    let config = config(&temp, &pki, &server);
    let directory = config.state_path.parent().expect("state directory");
    fs::create_dir_all(directory).expect("create state directory");
    fs::write(
        &config.state_path,
        br#"{"nextSequence":0,"pending":{"protocolVersion":"v1","requestId":"550e8400-e29b-41d4-a716-446655440000","agentVersion":"rustfs-agent/1.0.0-rc.3","capabilities":["heartbeat"],"sequence":0,"clientTime":"2026-08-22T01:02:03Z","coarseNodeSummary":{"total":0,"healthy":0,"degraded":0}}}"#,
    )
    .expect("write corrupt state");
    private_mode(&config.state_path);
    let runtime = spawn_heartbeat_runtime(Some(config), &shutdown, summary)
        .expect("start runtime")
        .expect("configured runtime");
    let mut status = runtime.status();

    assert!(matches!(
        wait_for(&mut status, |status| matches!(status, HeartbeatStatus::Failed { .. })).await,
        HeartbeatStatus::Failed { reason } if reason.contains("violates the protocol invariants")
    ));
    assert!(server.seen.lock().expect("seen lock").is_empty());
    runtime.shutdown().await;
}

#[tokio::test]
async fn invalid_stored_resource_name_is_rejected_before_network_delivery() {
    let pki = TestPki::new();
    let server = server(&pki, vec![]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let config = config(&temp, &pki, &server);
    rewrite_credential(&temp, |credential| {
        credential.name = format!("organizations/{ORGANIZATION_UID}/clusters/not-a-uuid/clusterDevices/{DEVICE_UID}");
    });

    assert_credential_failure(config, &server, "wrong device identity").await;
}

#[tokio::test]
async fn invalid_stored_protocol_is_rejected_before_network_delivery() {
    let pki = TestPki::new();
    let server = server(&pki, vec![]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let config = config(&temp, &pki, &server);
    rewrite_credential(&temp, |credential| credential.protocol_version = "v2".to_owned());

    assert_credential_failure(config, &server, "wrong device identity").await;
}

#[tokio::test]
async fn stored_certificate_key_mismatch_is_rejected_before_network_delivery() {
    let pki = TestPki::new();
    let server = server(&pki, vec![]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let now = OffsetDateTime::now_utc();
    let (identity_store, credential_store) =
        pki.stores_with_certificate(&temp, now - time::Duration::hours(1), now + time::Duration::hours(23), false);
    let config = config_with_stores(&temp, &pki, &server, identity_store, credential_store);

    assert_credential_failure(config, &server, "different device key").await;
}

#[tokio::test]
async fn expired_stored_certificate_is_rejected_before_network_delivery() {
    let pki = TestPki::new();
    let server = server(&pki, vec![]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let now = OffsetDateTime::now_utc();
    let (identity_store, credential_store) =
        pki.stores_with_certificate(&temp, now - time::Duration::days(2), now - time::Duration::days(1), true);
    let config = config_with_stores(&temp, &pki, &server, identity_store, credential_store);

    assert_credential_failure(config, &server, "not currently valid").await;
}

#[tokio::test]
async fn sends_only_l0_fields_and_accepts_additive_response_fields() {
    let pki = TestPki::new();
    let server = server(&pki, vec![Reply::ok("2038-01-19T03:14:07Z")]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config(&temp, &pki, &server)), &shutdown, summary)
        .expect("start runtime")
        .expect("configured runtime");
    let mut status = runtime.status();

    assert_eq!(
        wait_for(&mut status, |status| matches!(status, HeartbeatStatus::Online { .. })).await,
        HeartbeatStatus::Online {
            server_time: "2038-01-19T03:14:07Z".to_owned()
        }
    );
    runtime.shutdown().await;
    let seen = server.seen.lock().expect("seen lock");
    let request = &seen[0];
    let mut keys = request
        .as_object()
        .expect("heartbeat object")
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "agentVersion",
            "capabilities",
            "clientTime",
            "coarseNodeSummary",
            "protocolVersion",
            "requestId",
            "sequence"
        ]
    );
    assert_eq!(request["capabilities"], json!(["heartbeat"]));
    assert_eq!(request["coarseNodeSummary"], json!({"total": 8, "healthy": 7, "degraded": 1}));
    assert_ne!(request["clientTime"], "2038-01-19T03:14:07Z");
    assert!(request.get("authorization").is_none());
}

#[tokio::test]
async fn restart_replays_pending_request_then_advances_sequence() {
    let pki = TestPki::new();
    let first_server = server(&pki, vec![Reply::error(StatusCode::SERVICE_UNAVAILABLE)]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let shutdown = CancellationToken::new();
    let first_config = config(&temp, &pki, &first_server);
    let runtime = spawn_heartbeat_runtime(Some(first_config.clone()), &shutdown, summary)
        .expect("start runtime")
        .expect("configured runtime");
    let mut status = runtime.status();
    wait_for(&mut status, |status| matches!(status, HeartbeatStatus::BackingOff { .. })).await;
    runtime.shutdown().await;
    let first = first_server.seen.lock().expect("seen lock")[0].clone();
    drop(first_server);

    let second_server = server(&pki, vec![Reply::ok("2026-08-22T01:02:03Z"), Reply::ok("2026-08-22T01:02:04Z")]).await;
    let mut second_config = first_config;
    second_config.endpoint = second_server.endpoint.clone();
    let runtime = spawn_heartbeat_runtime(Some(second_config), &shutdown, summary)
        .expect("restart runtime")
        .expect("configured runtime");
    tokio::time::timeout(Duration::from_secs(3), async {
        while second_server.seen.lock().expect("seen lock").len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("two heartbeats");
    runtime.shutdown().await;

    let seen = second_server.seen.lock().expect("seen lock");
    assert_eq!(seen[0]["requestId"], first["requestId"]);
    assert_eq!(seen[0]["sequence"], first["sequence"]);
    assert_ne!(seen[1]["requestId"], seen[0]["requestId"]);
    assert_eq!(seen[1]["sequence"].as_u64(), seen[0]["sequence"].as_u64().map(|value| value + 1));
}

#[tokio::test]
async fn retry_after_is_respected_with_the_local_upper_bound() {
    let pki = TestPki::new();
    let mut reply = Reply::error(StatusCode::TOO_MANY_REQUESTS);
    reply.retry_after = Some("300");
    let server = server(&pki, vec![reply]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config(&temp, &pki, &server)), &shutdown, summary)
        .expect("start runtime")
        .expect("configured runtime");
    let mut status = runtime.status();
    assert_eq!(
        wait_for(&mut status, |status| matches!(status, HeartbeatStatus::BackingOff { .. })).await,
        HeartbeatStatus::BackingOff {
            delay: Duration::from_millis(80)
        }
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn disconnects_use_exponential_backoff_with_a_cap() {
    let pki = TestPki::new();
    let server = server(
        &pki,
        vec![
            Reply::error(StatusCode::SERVICE_UNAVAILABLE),
            Reply::error(StatusCode::SERVICE_UNAVAILABLE),
            Reply::error(StatusCode::SERVICE_UNAVAILABLE),
        ],
    )
    .await;
    let temp = tempfile::tempdir().expect("tempdir");
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config(&temp, &pki, &server)), &shutdown, summary)
        .expect("start runtime")
        .expect("configured runtime");
    let mut status = runtime.status();
    for delay in [20, 40, 80] {
        assert_eq!(
            wait_for(&mut status, |status| {
                matches!(status, HeartbeatStatus::BackingOff { delay: observed } if *observed == Duration::from_millis(delay))
            })
            .await,
            HeartbeatStatus::BackingOff {
                delay: Duration::from_millis(delay)
            }
        );
    }
    runtime.shutdown().await;
}

#[tokio::test]
async fn revoked_credential_stops_and_exposes_local_status() {
    let pki = TestPki::new();
    let mut reply = Reply::error(StatusCode::UNAUTHORIZED);
    reply.body = json!({"details": [{"reason": "CREDENTIAL_REVOKED"}]});
    let server = server(&pki, vec![reply]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config(&temp, &pki, &server)), &shutdown, summary)
        .expect("start runtime")
        .expect("configured runtime");
    let mut status = runtime.status();
    assert_eq!(
        wait_for(&mut status, |status| matches!(status, HeartbeatStatus::AuthenticationStopped { .. })).await,
        HeartbeatStatus::AuthenticationStopped {
            status: 401,
            reason: Some("CREDENTIAL_REVOKED".to_owned())
        }
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(server.seen.lock().expect("seen lock").len(), 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn shutdown_cancels_an_in_flight_request() {
    let pki = TestPki::new();
    let mut reply = Reply::ok("2026-08-22T01:02:03Z");
    reply.delay = Duration::from_secs(5);
    let server = server(&pki, vec![reply]).await;
    let temp = tempfile::tempdir().expect("tempdir");
    let shutdown = CancellationToken::new();
    let runtime = spawn_heartbeat_runtime(Some(config(&temp, &pki, &server)), &shutdown, summary)
        .expect("start runtime")
        .expect("configured runtime");
    tokio::time::timeout(Duration::from_secs(3), async {
        while server.seen.lock().expect("seen lock").is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request reached server");
    tokio::time::timeout(Duration::from_millis(250), runtime.shutdown())
        .await
        .expect("cancellable shutdown");
}

#[test]
fn consumes_the_frozen_heartbeat_fixtures() {
    let registry: Value =
        serde_json::from_str(include_str!("../../protocol/agent/v1/fixtures/fixture-sets.json")).expect("fixture registry");
    let heartbeat = registry["sets"]
        .as_array()
        .expect("fixture sets")
        .iter()
        .find(|set| set["name"] == "heartbeat")
        .expect("heartbeat fixture set");
    assert_eq!(heartbeat["status"], "populated");
    let valid: Value =
        serde_json::from_str(include_str!("../../protocol/agent/v1/fixtures/heartbeat/valid.json")).expect("valid fixture");
    assert_eq!(valid["request"]["protocolVersion"], "v1");
    let overflow: Value =
        serde_json::from_str(include_str!("../../protocol/agent/v1/fixtures/heartbeat/overflow.json")).expect("overflow fixture");
    assert_eq!(overflow["expected"]["httpStatus"], 422);
}

#[cfg(unix)]
fn private_mode(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("private mode");
}

#[cfg(not(unix))]
fn private_mode(_path: &Path) {}
