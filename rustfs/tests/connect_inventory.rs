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

#![cfg(target_os = "linux")]

use std::collections::VecDeque;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    CredentialStore, DeviceCredential, HeartbeatConfig, HeartbeatSchedule, IdentityStore, InventoryFlag, InventoryOsVersion,
    InventorySchedule, InventorySnapshot, InventoryStatus, OperatingSystemFamily, spawn_inventory_runtime,
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
const SNAPSHOT_UID: &str = "0198f4b0-4d00-7f40-9051-5b6c7d8e9fa3";

fn safe_tempdir() -> tempfile::TempDir {
    tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).expect("safe temporary directory")
}

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
        let identity_store = IdentityStore::new(temp.path().join("identity"));
        let identity = identity_store.load_or_create().expect("create identity");
        let private_key = PrivatePkcs8KeyDer::from(identity.to_pkcs8_der().expect("serialize key").to_vec());
        let device_key = KeyPair::from_pkcs8_der_and_sign_algo(&private_key, &rcgen::PKCS_ECDSA_P256_SHA256).expect("device key");
        let now = OffsetDateTime::now_utc();
        let mut params = CertificateParams::default();
        params.not_before = now - time::Duration::hours(1);
        params.not_after = now + time::Duration::hours(23);
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
            not_before_unix: (now - time::Duration::hours(1)).unix_timestamp(),
            not_after_unix: (now + time::Duration::hours(23)).unix_timestamp(),
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
}

impl Reply {
    fn ok(content_hash: &str) -> Self {
        Self {
            status: StatusCode::OK,
            body: json!({
                "name": format!("organizations/{ORGANIZATION_UID}/clusters/{CLUSTER_UID}/inventorySnapshots/{SNAPSHOT_UID}"),
                "uid": SNAPSHOT_UID,
                "contentHash": content_hash,
                "receivedAt": "2026-08-22T01:02:03Z",
                "futureField": true
            }),
            retry_after: None,
        }
    }

    fn error(status: StatusCode, reason: &str) -> Self {
        Self {
            status,
            body: json!({"details": [{"reason": reason}]}),
            retry_after: None,
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
                        assert_eq!(request.uri().path(), format!("/agent/clusters/{CLUSTER_UID}/inventorySnapshots"));
                        let body = request.into_body().collect().await.expect("request body").to_bytes();
                        seen.lock()
                            .expect("seen lock")
                            .push(serde_json::from_slice(&body).expect("request JSON"));
                        let reply = replies
                            .lock()
                            .expect("reply lock")
                            .pop_front()
                            .unwrap_or_else(|| Reply::error(StatusCode::SERVICE_UNAVAILABLE, "UNAVAILABLE"));
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
    if let Err(error) = fs::create_dir(temp.path().join("private-config-secret")) {
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists, "Connect state root");
    }
    private_directory_mode(&temp.path().join("private-config-secret"));
    HeartbeatConfig {
        endpoint: server.endpoint.clone(),
        root_ca_pem: pki.root_pem.as_bytes().to_vec(),
        identity_store,
        credential_store,
        state_path: temp.path().join("private-config-secret/heartbeat/state.json"),
        schedule: HeartbeatSchedule {
            cadence: Duration::from_secs(30),
            jitter: Duration::ZERO,
            timeout: Duration::from_millis(200),
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(80),
        },
    }
}

fn schedule() -> InventorySchedule {
    InventorySchedule {
        cadence: Duration::from_secs(60),
        jitter: Duration::ZERO,
    }
}

fn snapshot() -> InventorySnapshot {
    InventorySnapshot::new(
        "1.4.2",
        Some(InventoryOsVersion::new(OperatingSystemFamily::Linux, 6, 8).expect("valid operating-system version")),
        8,
        96,
        1_099_511_627_776,
        412_316_860_416,
        [InventoryFlag::ClusterDegraded, InventoryFlag::DriveOffline],
    )
    .expect("valid inventory")
}

fn collect_strings(value: &Value, strings: &mut Vec<String>) {
    match value {
        Value::String(value) => strings.push(value.clone()),
        Value::Array(values) => values.iter().for_each(|value| collect_strings(value, strings)),
        Value::Object(values) => values.values().for_each(|value| collect_strings(value, strings)),
        _ => {}
    }
}

async fn wait_for(
    status: &mut watch::Receiver<InventoryStatus>,
    predicate: impl Fn(&InventoryStatus) -> bool,
) -> InventoryStatus {
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
    .expect("inventory status timeout")
}

#[test]
fn connect_inventory_frozen_vector_has_the_exact_canonical_hash_and_no_open_ended_fields() {
    let fixtures: Value = serde_json::from_str(include_str!("../../protocol/agent/v1/fixtures/inventory/valid-vectors.json"))
        .expect("valid fixture JSON");
    let expected = &fixtures["vectors"][0]["expected"];
    let snapshot = snapshot();

    assert_eq!(snapshot.content_hash().expect("content hash"), expected["contentHash"]);
    assert_eq!(InventorySchedule::default().cadence, Duration::from_secs(6 * 60 * 60));
    assert_eq!(InventorySchedule::default().jitter, Duration::from_secs(30 * 60));
    let encoded = serde_json::to_value(snapshot).expect("snapshot JSON");
    assert_eq!(
        encoded,
        json!({
            "rustfsVersion": "1.4.2",
            "osVersion": {"family": "linux", "major": 6, "minor": 8},
            "nodeCount": 8,
            "driveCount": 96,
            "capacityTotalBytes": 1099511627776_u64,
            "capacityUsedBytes": 412316860416_u64,
            "coarseFlags": ["cluster.degraded", "drive.offline"]
        })
    );

    let fixtures: Value =
        serde_json::from_str(include_str!("../../protocol/agent/v1/fixtures/inventory/secret-like-vectors.json"))
            .expect("valid secret-like fixture JSON");
    let known_fields = [
        "protocolVersion",
        "rustfsVersion",
        "osVersion",
        "nodeCount",
        "driveCount",
        "capacityTotalBytes",
        "capacityUsedBytes",
        "coarseFlags",
    ];
    let known_flags = ["cluster.degraded", "drive.offline"];
    let mut excluded = Vec::new();
    for vector in fixtures["vectors"].as_array().expect("fixture vectors") {
        let input = vector["input"].as_object().expect("fixture input");
        for (name, value) in input {
            if !known_fields.contains(&name.as_str()) {
                collect_strings(value, &mut excluded);
            }
        }
        for (name, value) in input["osVersion"].as_object().expect("fixture OS version") {
            if !["family", "major", "minor"].contains(&name.as_str()) {
                collect_strings(value, &mut excluded);
            }
        }
        for flag in input["coarseFlags"].as_array().expect("fixture coarse flags") {
            let flag = flag.as_str().expect("fixture coarse flag");
            if !known_flags.contains(&flag) {
                excluded.push(flag.to_owned());
            }
        }
    }
    let encoded = serde_json::to_string(&encoded).expect("encoded snapshot");
    for value in excluded {
        assert!(!encoded.contains(&value), "snapshot exposed fixture value {value}");
    }
}

#[test]
fn connect_inventory_bounds_fail_instead_of_truncating_or_inventing_values() {
    assert!(matches!(
        InventorySnapshot::current(0, 0, 0, 0, []),
        Err(rustfs::connect::InventoryError::NodeCount)
    ));
    assert!(matches!(
        InventorySnapshot::current(1, 1_048_577, 0, 0, []),
        Err(rustfs::connect::InventoryError::DriveCount)
    ));
    assert!(matches!(
        InventorySnapshot::current(1, 0, 9_007_199_254_740_992, 0, []),
        Err(rustfs::connect::InventoryError::Capacity)
    ));
    assert!(matches!(
        InventorySnapshot::current(1, 0, 10, 11, []),
        Err(rustfs::connect::InventoryError::Capacity)
    ));
    assert!(matches!(
        InventorySnapshot::new("1.0.0-private.1", None, 1, 0, 0, 0, []),
        Err(rustfs::connect::InventoryError::RustfsVersion)
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn connect_inventory_state_only_persists_without_constructing_transport() {
    let temp = safe_tempdir();
    let state = temp.path().join("state");
    fs::create_dir(&state).expect("state root");
    private_directory_mode(&state);
    let config = HeartbeatConfig::new(
        "",
        Vec::new(),
        IdentityStore::new(state.join("identity")),
        CredentialStore::new(state.join("credential")),
        state.join("heartbeat/state.json"),
    );
    let shutdown = CancellationToken::new();
    let runtime = spawn_inventory_runtime(Some(config), schedule(), &shutdown, || std::future::ready(Ok(snapshot())))
        .expect("state-only inventory")
        .expect("configured inventory");
    let mut status = runtime.status();

    assert!(matches!(
        wait_for(&mut status, |status| matches!(status, InventoryStatus::Unchanged { .. })).await,
        InventoryStatus::Unchanged { .. }
    ));
    let envelope: Value = serde_json::from_slice(&fs::read(state.join("inventory/latest.json")).expect("latest inventory"))
        .expect("latest envelope");
    assert_eq!(envelope["formatVersion"], "v1");
    assert_eq!(envelope["snapshot"], serde_json::to_value(snapshot()).expect("snapshot JSON"));
    assert_eq!(envelope.as_object().expect("envelope object").len(), 4);
    runtime.shutdown().await;
}

#[tokio::test]
async fn connect_inventory_rejects_deserialized_invalid_snapshots_before_state_or_network() {
    let pki = TestPki::new();
    let server = server(&pki, Vec::new()).await;
    for rustfs_version in ["1.0".to_owned(), format!("1.{}.0", "1".repeat(4096))] {
        let invalid: InventorySnapshot = serde_json::from_value(json!({
            "rustfsVersion": rustfs_version,
            "osVersion": null,
            "nodeCount": 1,
            "driveCount": 1,
            "capacityTotalBytes": 100,
            "capacityUsedBytes": 60,
            "coarseFlags": []
        }))
        .expect("serde should not bypass the runtime validation boundary");
        let temp = safe_tempdir();
        let shutdown = CancellationToken::new();
        let runtime = spawn_inventory_runtime(Some(config(&temp, &pki, &server)), schedule(), &shutdown, move || {
            std::future::ready(Ok(invalid.clone()))
        })
        .expect("start inventory")
        .expect("configured inventory");
        let mut status = runtime.status();

        assert!(matches!(
            wait_for(&mut status, |status| matches!(status, InventoryStatus::Failed { .. })).await,
            InventoryStatus::Failed { reason } if reason == "connect_inventory_snapshot_version"
        ));
        assert!(!temp.path().join("private-config-secret/inventory/state.json").exists());
        runtime.shutdown().await;
    }
    assert!(server.seen.lock().expect("seen lock").is_empty());
}

#[tokio::test]
async fn connect_inventory_restart_replays_the_pending_request_and_then_skips_unchanged_inventory() {
    let pki = TestPki::new();
    let content_hash = snapshot().content_hash().expect("content hash");
    let first_server = server(&pki, vec![Reply::error(StatusCode::SERVICE_UNAVAILABLE, "UNAVAILABLE")]).await;
    let temp = safe_tempdir();
    let shutdown = CancellationToken::new();
    let samples = Arc::new(AtomicUsize::new(0));
    let sampled = samples.clone();
    let runtime = spawn_inventory_runtime(Some(config(&temp, &pki, &first_server)), schedule(), &shutdown, move || {
        sampled.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Ok(snapshot()))
    })
    .expect("start inventory")
    .expect("configured inventory");
    let mut status = runtime.status();

    assert!(matches!(
        wait_for(&mut status, |status| matches!(status, InventoryStatus::BackingOff { .. })).await,
        InventoryStatus::BackingOff { delay } if delay == Duration::from_millis(20)
    ));
    assert_eq!(samples.load(Ordering::Relaxed), 1);
    let original = first_server.seen.lock().expect("seen lock")[0].clone();
    let latest: Value = serde_json::from_slice(
        &fs::read(temp.path().join("private-config-secret/inventory/latest.json")).expect("latest inventory"),
    )
    .expect("latest envelope");
    assert_eq!(latest["snapshot"], serde_json::to_value(snapshot()).expect("snapshot JSON"));
    for field in [
        "rustfsVersion",
        "osVersion",
        "nodeCount",
        "driveCount",
        "capacityTotalBytes",
        "capacityUsedBytes",
        "coarseFlags",
    ] {
        assert_eq!(latest["snapshot"][field], original[field]);
    }
    runtime.shutdown().await;
    let state = temp.path().join("private-config-secret/inventory/state.json");
    let legacy_persisted_at = std::time::SystemTime::now() - Duration::from_secs(60 * 60);
    fs::File::options()
        .write(true)
        .open(&state)
        .expect("pending state")
        .set_times(std::fs::FileTimes::new().set_modified(legacy_persisted_at))
        .expect("legacy pending timestamp");
    let legacy_persisted_at = chrono::DateTime::<chrono::Utc>::from(
        fs::metadata(&state)
            .and_then(|metadata| metadata.modified())
            .expect("persisted pending timestamp"),
    )
    .format("%Y-%m-%dT%H:%M:%SZ")
    .to_string();
    fs::remove_file(temp.path().join("private-config-secret/inventory/latest.json"))
        .expect("simulate a pending snapshot created before local persistence");

    let mut limited = Reply::error(StatusCode::TOO_MANY_REQUESTS, "RATE_LIMITED");
    limited.retry_after = Some("0");
    let restart_server = server(&pki, vec![limited, Reply::ok(&content_hash)]).await;
    let restart_config = config(&temp, &pki, &restart_server);
    let restart_samples = Arc::new(AtomicUsize::new(0));
    let sampled = restart_samples.clone();
    let restart = spawn_inventory_runtime(Some(restart_config.clone()), schedule(), &shutdown, move || {
        sampled.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Ok(snapshot()))
    })
    .expect("restart inventory")
    .expect("configured inventory");
    let mut status = restart.status();

    assert!(matches!(
        wait_for(&mut status, |status| matches!(status, InventoryStatus::Online { .. })).await,
        InventoryStatus::Online { content_hash: accepted, received_at }
            if accepted == content_hash && received_at == "2026-08-22T01:02:03Z"
    ));
    assert_eq!(restart_samples.load(Ordering::Relaxed), 0);
    let restored_latest: Value = serde_json::from_slice(
        &fs::read(temp.path().join("private-config-secret/inventory/latest.json")).expect("restored latest inventory"),
    )
    .expect("restored latest envelope");
    assert_eq!(restored_latest["snapshot"], serde_json::to_value(snapshot()).expect("snapshot JSON"));
    assert_eq!(restored_latest["capturedAt"], legacy_persisted_at);
    let delivered = restart_server.seen.lock().expect("seen lock").clone();
    assert_eq!(delivered, vec![original.clone(), original.clone()]);
    assert_eq!(original["sequence"], 0);
    let encoded = serde_json::to_string(&original).expect("request JSON");
    let persisted = serde_json::to_string(&latest).expect("persisted JSON");
    for forbidden in [
        "private-config-secret",
        "BEGIN CERTIFICATE",
        "AKIAIOSFODNN7EXAMPLE",
        "bucket",
        "object",
        "path",
    ] {
        assert!(!encoded.contains(forbidden), "request exposed {forbidden}");
        assert!(!persisted.contains(forbidden), "persisted inventory exposed {forbidden}");
    }
    assert_eq!(original.as_object().expect("request object").len(), 10);
    restart.shutdown().await;
    #[cfg(target_os = "linux")]
    let latest_before_unchanged = {
        use std::os::unix::fs::MetadataExt as _;
        fs::metadata(temp.path().join("private-config-secret/inventory/latest.json"))
            .expect("latest metadata")
            .ino()
    };

    let unchanged_samples = Arc::new(AtomicUsize::new(0));
    let sampled = unchanged_samples.clone();
    let unchanged = spawn_inventory_runtime(Some(restart_config), schedule(), &shutdown, move || {
        sampled.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Ok(snapshot()))
    })
    .expect("restart inventory")
    .expect("configured inventory");
    let mut unchanged_status = unchanged.status();
    assert!(matches!(
        wait_for(&mut unchanged_status, |status| matches!(status, InventoryStatus::Unchanged { .. })).await,
        InventoryStatus::Unchanged { content_hash: unchanged } if unchanged == content_hash
    ));
    assert_eq!(unchanged_samples.load(Ordering::Relaxed), 1);
    assert_eq!(restart_server.seen.lock().expect("seen lock").len(), 2);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt as _;
        assert_ne!(
            fs::metadata(temp.path().join("private-config-secret/inventory/latest.json"))
                .expect("refreshed latest metadata")
                .ino(),
            latest_before_unchanged,
            "a complete unchanged sample must refresh the local envelope"
        );
    }
    unchanged.shutdown().await;
}

#[tokio::test]
async fn connect_inventory_disconnect_retries_without_resampling() {
    let pki = TestPki::new();
    let unavailable = server(&pki, Vec::new()).await;
    let temp = safe_tempdir();
    let config = config(&temp, &pki, &unavailable);
    drop(unavailable);
    let shutdown = CancellationToken::new();
    let samples = Arc::new(AtomicUsize::new(0));
    let sampled = samples.clone();
    let runtime = spawn_inventory_runtime(Some(config), schedule(), &shutdown, move || {
        sampled.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Ok(snapshot()))
    })
    .expect("start inventory")
    .expect("configured inventory");
    let mut status = runtime.status();

    assert!(matches!(
        wait_for(&mut status, |status| {
            matches!(status, InventoryStatus::BackingOff { delay } if *delay == Duration::from_millis(40))
        })
        .await,
        InventoryStatus::BackingOff { delay } if delay == Duration::from_millis(40)
    ));
    assert_eq!(samples.load(Ordering::Relaxed), 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn connect_inventory_retries_an_incomplete_sample_before_delivery() {
    let pki = TestPki::new();
    let content_hash = snapshot().content_hash().expect("content hash");
    let server = server(&pki, vec![Reply::ok(&content_hash)]).await;
    let temp = safe_tempdir();
    let shutdown = CancellationToken::new();
    let samples = Arc::new(AtomicUsize::new(0));
    let sampled = samples.clone();
    let runtime = spawn_inventory_runtime(Some(config(&temp, &pki, &server)), schedule(), &shutdown, move || {
        let attempt = sampled.fetch_add(1, Ordering::Relaxed);
        std::future::ready(if attempt == 0 {
            Err(rustfs::connect::InventoryError::SnapshotIncomplete {
                expected: 96,
                observed: 12,
            })
        } else {
            Ok(snapshot())
        })
    })
    .expect("start inventory")
    .expect("configured inventory");
    let mut status = runtime.status();

    assert!(matches!(
        wait_for(&mut status, |status| matches!(status, InventoryStatus::BackingOff { .. })).await,
        InventoryStatus::BackingOff { .. }
    ));
    assert!(!temp.path().join("private-config-secret/inventory/latest.json").exists());
    assert!(matches!(
        wait_for(&mut status, |status| matches!(status, InventoryStatus::Online { .. })).await,
        InventoryStatus::Online { content_hash: accepted, .. } if accepted == content_hash
    ));
    assert_eq!(samples.load(Ordering::Relaxed), 2);
    assert_eq!(server.seen.lock().expect("seen lock").len(), 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn connect_inventory_unchanged_sample_resets_incomplete_backoff() {
    let pki = TestPki::new();
    let content_hash = snapshot().content_hash().expect("content hash");
    let server = server(&pki, vec![Reply::ok(&content_hash)]).await;
    let temp = safe_tempdir();
    let shutdown = CancellationToken::new();
    let config = config(&temp, &pki, &server);
    let seed = spawn_inventory_runtime(Some(config.clone()), schedule(), &shutdown, || std::future::ready(Ok(snapshot())))
        .expect("start inventory")
        .expect("configured inventory");
    let mut seed_status = seed.status();

    assert!(matches!(
        wait_for(&mut seed_status, |status| matches!(status, InventoryStatus::Online { .. })).await,
        InventoryStatus::Online { content_hash: accepted, .. } if accepted == content_hash
    ));
    seed.shutdown().await;

    let samples = Arc::new(AtomicUsize::new(0));
    let sampled = samples.clone();
    let runtime = spawn_inventory_runtime(
        Some(config),
        InventorySchedule {
            cadence: Duration::from_millis(100),
            jitter: Duration::ZERO,
        },
        &shutdown,
        move || {
            let attempt = sampled.fetch_add(1, Ordering::Relaxed);
            std::future::ready(if matches!(attempt, 0 | 1 | 3) {
                Err(rustfs::connect::InventoryError::SnapshotIncomplete {
                    expected: 96,
                    observed: 12,
                })
            } else {
                Ok(snapshot())
            })
        },
    )
    .expect("restart inventory")
    .expect("configured inventory");
    let mut status = runtime.status();

    assert!(matches!(
        wait_for(&mut status, |status| {
            matches!(status, InventoryStatus::BackingOff { delay } if *delay == Duration::from_millis(20))
        })
        .await,
        InventoryStatus::BackingOff { delay } if delay == Duration::from_millis(20)
    ));
    assert!(matches!(
        wait_for(&mut status, |status| {
            matches!(status, InventoryStatus::BackingOff { delay } if *delay == Duration::from_millis(40))
        })
        .await,
        InventoryStatus::BackingOff { delay } if delay == Duration::from_millis(40)
    ));
    assert!(matches!(
        wait_for(&mut status, |status| matches!(status, InventoryStatus::Unchanged { .. })).await,
        InventoryStatus::Unchanged { content_hash: unchanged } if unchanged == content_hash
    ));
    assert!(matches!(
        wait_for(&mut status, |status| matches!(status, InventoryStatus::BackingOff { .. })).await,
        InventoryStatus::BackingOff { delay } if delay == Duration::from_millis(20)
    ));
    assert_eq!(samples.load(Ordering::Relaxed), 4);
    assert_eq!(server.seen.lock().expect("seen lock").len(), 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn connect_inventory_revoked_device_stops_without_retrying() {
    let pki = TestPki::new();
    let server = server(&pki, vec![Reply::error(StatusCode::UNAUTHORIZED, "DEVICE_REVOKED")]).await;
    let temp = safe_tempdir();
    let shutdown = CancellationToken::new();
    let runtime = spawn_inventory_runtime(Some(config(&temp, &pki, &server)), schedule(), &shutdown, || {
        std::future::ready(Ok(snapshot()))
    })
    .expect("start inventory")
    .expect("configured inventory");
    let mut status = runtime.status();

    assert!(matches!(
        wait_for(&mut status, |status| matches!(status, InventoryStatus::AuthenticationStopped { .. })).await,
        InventoryStatus::AuthenticationStopped {
            status: 401,
            reason: None
        }
    ));
    assert_eq!(server.seen.lock().expect("seen lock").len(), 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn connect_inventory_sequence_overflow_fails_before_sampling_or_network_delivery() {
    let pki = TestPki::new();
    let server = server(&pki, Vec::new()).await;
    let temp = safe_tempdir();
    let config = config(&temp, &pki, &server);
    let state = temp.path().join("private-config-secret/inventory/state.json");
    fs::create_dir_all(state.parent().expect("state directory")).expect("create state directory");
    private_directory_mode(state.parent().expect("state directory"));
    fs::write(
        &state,
        br#"{"nextSequence":9007199254740992,"pending":null,"lastAcceptedContentHash":null}"#,
    )
    .expect("write state");
    private_mode(&state);
    let samples = Arc::new(AtomicUsize::new(0));
    let sampled = samples.clone();
    let shutdown = CancellationToken::new();
    let runtime = spawn_inventory_runtime(Some(config), schedule(), &shutdown, move || {
        sampled.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Ok(snapshot()))
    })
    .expect("start inventory")
    .expect("configured inventory");
    let mut status = runtime.status();

    assert!(matches!(
        wait_for(&mut status, |status| matches!(status, InventoryStatus::Failed { .. })).await,
        InventoryStatus::Failed { reason } if reason == "connect_inventory_sequence_exhausted"
    ));
    assert_eq!(samples.load(Ordering::Relaxed), 0);
    assert!(server.seen.lock().expect("seen lock").is_empty());
    runtime.shutdown().await;
}

#[tokio::test]
async fn connect_inventory_rejects_noncanonical_persisted_snapshots_before_delivery() {
    let pki = TestPki::new();
    let server = server(&pki, Vec::new()).await;

    for invalid_case in ["flags", "os-version"] {
        let temp = safe_tempdir();
        let config = config(&temp, &pki, &server);
        let state = temp.path().join("private-config-secret/inventory/state.json");
        fs::create_dir_all(state.parent().expect("state directory")).expect("create state directory");
        private_directory_mode(state.parent().expect("state directory"));
        let mut pending = json!({
            "protocolVersion": "v1",
            "requestId": "00000000-0000-4000-8000-000000000001",
            "sequence": 0,
            "rustfsVersion": "1.4.2",
            "osVersion": {"family": "linux", "major": 6, "minor": 8},
            "nodeCount": 1,
            "driveCount": 1,
            "capacityTotalBytes": 1,
            "capacityUsedBytes": 0,
            "coarseFlags": ["cluster.degraded", "drive.offline"]
        });
        match invalid_case {
            "flags" => {
                pending["coarseFlags"] = json!(["drive.offline", "cluster.degraded", "drive.offline"]);
            }
            "os-version" => pending["osVersion"]["major"] = json!(10_000),
            _ => unreachable!(),
        }
        fs::write(
            &state,
            serde_json::to_vec(&json!({
                "nextSequence": 0,
                "pending": pending,
                "lastAcceptedContentHash": null
            }))
            .expect("state JSON"),
        )
        .expect("write state");
        private_mode(&state);

        let samples = Arc::new(AtomicUsize::new(0));
        let sampled = samples.clone();
        let shutdown = CancellationToken::new();
        let runtime = spawn_inventory_runtime(Some(config), schedule(), &shutdown, move || {
            sampled.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok(snapshot()))
        })
        .expect("start inventory")
        .expect("configured inventory");
        let mut status = runtime.status();

        assert!(matches!(
            wait_for(&mut status, |status| {
                matches!(status, InventoryStatus::Failed { .. } | InventoryStatus::BackingOff { .. })
            })
            .await,
            InventoryStatus::Failed { reason } if reason == "connect_inventory_state_corrupt"
        ));
        assert_eq!(samples.load(Ordering::Relaxed), 0);
        runtime.shutdown().await;
    }

    assert!(server.seen.lock().expect("seen lock").is_empty());
}

#[cfg(target_os = "linux")]
fn private_mode(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("private permissions");
}

#[cfg(target_os = "linux")]
fn private_directory_mode(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("private directory permissions");
}

#[cfg(not(target_os = "linux"))]
fn private_mode(_path: &std::path::Path) {}

#[cfg(not(target_os = "linux"))]
fn private_directory_mode(_path: &std::path::Path) {}
