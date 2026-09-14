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

use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use http_body_util::{BodyExt as _, Full};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use rustfs::connect::{
    CredentialStore, DeviceCredential, HeartbeatConfig, HeartbeatSchedule, IdentityStore, ReportUploadClient, ReportUploadError,
};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;
use tokio::net::TcpListener;
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

    fn agent_server_config(&self) -> rustls::ServerConfig {
        let mut roots = RootCertStore::empty();
        roots.add(self.root_der.clone()).expect("add client root");
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .expect("client verifier");
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![self.server_der.clone()], PrivateKeyDer::Pkcs8(self.server_key.clone_key()))
            .expect("agent TLS")
    }

    fn object_server_config(&self) -> rustls::ServerConfig {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![self.server_der.clone()], PrivateKeyDer::Pkcs8(self.server_key.clone_key()))
            .expect("object TLS")
    }

    fn stores(&self, temp: &tempfile::TempDir) -> (IdentityStore, CredentialStore) {
        let identity_store = IdentityStore::new(temp.path().join("identity"));
        let identity = identity_store.load_or_create().expect("create identity");
        let private_key = PrivatePkcs8KeyDer::from(identity.to_pkcs8_der().expect("serialize key").to_vec());
        let device_key = KeyPair::from_pkcs8_der_and_sign_algo(&private_key, &rcgen::PKCS_ECDSA_P256_SHA256).expect("device key");
        let now = OffsetDateTime::now_utc();
        let not_before = now - time::Duration::hours(1);
        let not_after = now + time::Duration::hours(23);
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
        let credential = DeviceCredential {
            name: format!("organizations/{ORGANIZATION_UID}/clusters/{CLUSTER_UID}/clusterDevices/{DEVICE_UID}"),
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

struct ObjectServer {
    endpoint: String,
    attempts: Arc<AtomicUsize>,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    client_certificates: Arc<Mutex<Vec<bool>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ObjectServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn object_server(pki: &TestPki) -> ObjectServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind object server");
    let address = listener.local_addr().expect("object server address");
    let acceptor = TlsAcceptor::from(Arc::new(pki.object_server_config()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed_attempts = attempts.clone();
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let observed_bodies = bodies.clone();
    let client_certificates = Arc::new(Mutex::new(Vec::new()));
    let observed_certificates = client_certificates.clone();
    let interrupt_first = Arc::new(AtomicBool::new(true));
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            let bodies = observed_bodies.clone();
            let certificates = observed_certificates.clone();
            let interrupt = interrupt_first.clone();
            observed_attempts.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let Ok(stream) = acceptor.accept(stream).await else { return };
                certificates.lock().expect("certificate observations").push(
                    stream
                        .get_ref()
                        .1
                        .peer_certificates()
                        .is_some_and(|certificates| !certificates.is_empty()),
                );
                if interrupt.swap(false, Ordering::SeqCst) {
                    return;
                }
                let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                    let bodies = bodies.clone();
                    async move {
                        assert_eq!(request.method(), hyper::Method::PUT);
                        assert_eq!(request.uri().path(), "/upload");
                        assert_eq!(request.headers().get("content-type").expect("content type"), "application/octet-stream");
                        assert_eq!(request.headers().get("if-none-match").expect("create only"), "*");
                        assert_eq!(request.headers().get("x-amz-server-side-encryption").expect("encryption"), "AES256");
                        let declared_length = request
                            .headers()
                            .get("content-length")
                            .expect("content length")
                            .to_str()
                            .expect("content length text")
                            .parse::<usize>()
                            .expect("content length number");
                        let checksum = request
                            .headers()
                            .get("x-amz-checksum-sha256")
                            .expect("checksum")
                            .to_str()
                            .expect("checksum text")
                            .to_owned();
                        let body = request.into_body().collect().await.expect("upload body").to_bytes().to_vec();
                        assert_eq!(declared_length, body.len());
                        assert_eq!(checksum, base64_simd::STANDARD.encode_to_string(Sha256::digest(&body)));
                        bodies.lock().expect("uploaded bodies").push(body);
                        Ok::<_, hyper::Error>(Response::new(Full::new(Bytes::new())))
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    ObjectServer {
        endpoint: format!("https://localhost:{}/upload?signature=hidden", address.port()),
        attempts,
        bodies,
        client_certificates,
        task,
    }
}

#[derive(Clone)]
struct BundleDeclaration {
    uid: String,
    size: u64,
    sha256: String,
}

struct AgentServer {
    endpoint: String,
    reserve_count: Arc<AtomicUsize>,
    complete_count: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for AgentServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn agent_server(pki: &TestPki, upload_url: String) -> AgentServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind agent server");
    let address = listener.local_addr().expect("agent server address");
    let acceptor = TlsAcceptor::from(Arc::new(pki.agent_server_config()));
    let declaration = Arc::new(Mutex::new(None::<BundleDeclaration>));
    let reserve_count = Arc::new(AtomicUsize::new(0));
    let observed_reserves = reserve_count.clone();
    let complete_count = Arc::new(AtomicUsize::new(0));
    let observed_completes = complete_count.clone();
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            let declaration = declaration.clone();
            let upload_url = upload_url.clone();
            let reserves = observed_reserves.clone();
            let completes = observed_completes.clone();
            tokio::spawn(async move {
                let Ok(stream) = acceptor.accept(stream).await else { return };
                let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                    let declaration = declaration.clone();
                    let upload_url = upload_url.clone();
                    let reserves = reserves.clone();
                    let completes = completes.clone();
                    async move {
                        let path = request.uri().path().to_owned();
                        let request_body = request.into_body().collect().await.expect("control body").to_bytes();
                        let request: Value = serde_json::from_slice(&request_body).expect("control JSON");
                        let (status, body) = if path == format!("/agent/clusters/{CLUSTER_UID}/supportBundles") {
                            reserves.fetch_add(1, Ordering::SeqCst);
                            assert_eq!(request["protocolVersion"], "v1");
                            assert_eq!(request["contentType"], "application/octet-stream");
                            let current = BundleDeclaration {
                                uid: request["bundleUid"].as_str().expect("bundle uid").to_owned(),
                                size: request["declaredSizeBytes"].as_u64().expect("declared size"),
                                sha256: request["declaredSha256"].as_str().expect("declared digest").to_owned(),
                            };
                            *declaration.lock().expect("declaration") = Some(current.clone());
                            let resource = bundle_resource(&current, "PENDING");
                            let mut digest = [0u8; 32];
                            faster_hex::hex_decode(current.sha256.as_bytes(), &mut digest).expect("digest hex");
                            (
                                StatusCode::CREATED,
                                json!({
                                    "supportBundle": resource,
                                    "uploadAuthorization": {
                                        "method": "PUT",
                                        "url": upload_url,
                                        "headers": {
                                            "Content-Type": "application/octet-stream",
                                            "Content-Length": current.size.to_string(),
                                            "If-None-Match": "*",
                                            "x-amz-checksum-sha256": base64_simd::STANDARD.encode_to_string(digest),
                                            "x-amz-server-side-encryption": "AES256"
                                        },
                                        "expireTime": instant(5)
                                    }
                                }),
                            )
                        } else {
                            let current = declaration.lock().expect("declaration").clone().expect("reserved bundle");
                            assert_eq!(
                                path,
                                format!("/agent/clusters/{CLUSTER_UID}/supportBundles/{}:completeUpload", current.uid)
                            );
                            assert_eq!(request["protocolVersion"], "v1");
                            completes.fetch_add(1, Ordering::SeqCst);
                            (StatusCode::OK, bundle_resource(&current, "UPLOADED"))
                        };
                        Ok::<_, hyper::Error>(
                            Response::builder()
                                .status(status)
                                .header("content-type", "application/json")
                                .body(Full::new(Bytes::from(serde_json::to_vec(&body).expect("response JSON"))))
                                .expect("control response"),
                        )
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    AgentServer {
        endpoint: format!("https://localhost:{}/agent/", address.port()),
        reserve_count,
        complete_count,
        task,
    }
}

fn bundle_resource(declaration: &BundleDeclaration, state: &str) -> Value {
    json!({
        "name": format!(
            "organizations/{ORGANIZATION_UID}/clusters/{CLUSTER_UID}/supportBundles/{}",
            declaration.uid
        ),
        "uid": &declaration.uid,
        "state": state,
        "declaredSizeBytes": declaration.size,
        "declaredSha256": &declaration.sha256,
        "expireTime": instant(60),
        "createTime": instant(0),
        "updateTime": instant(0)
    })
}

fn instant(minutes: i64) -> String {
    (Utc::now() + chrono::Duration::minutes(minutes)).to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn config(temp: &tempfile::TempDir, pki: &TestPki, endpoint: &str) -> HeartbeatConfig {
    let (identity_store, credential_store) = pki.stores(temp);
    HeartbeatConfig {
        endpoint: endpoint.to_owned(),
        root_ca_pem: pki.root_pem.as_bytes().to_vec(),
        identity_store,
        credential_store,
        state_path: temp.path().join("heartbeat/state.json"),
        schedule: HeartbeatSchedule {
            cadence: Duration::from_secs(60),
            jitter: Duration::ZERO,
            timeout: Duration::from_secs(2),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(20),
        },
        proxy: None,
        diagnostic_job_signer: None,
    }
}

#[tokio::test]
async fn interrupted_put_is_reauthorized_and_completed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let archive_path = temp.path().join("support-bundle.tar.zst");
    let archive = b"redacted support bundle evidence";
    fs::write(&archive_path, archive).expect("write archive");
    let pki = TestPki::new();
    let object = object_server(&pki).await;
    let agent = agent_server(&pki, object.endpoint.clone()).await;

    let receipt = ReportUploadClient::new(config(&temp, &pki, &agent.endpoint), Duration::from_secs(5))
        .expect("report client")
        .upload(&archive_path, &CancellationToken::new())
        .await
        .expect("upload report");

    assert_eq!(receipt.state, "UPLOADED");
    assert_eq!(receipt.declared_size_bytes, archive.len() as u64);
    assert_eq!(receipt.declared_sha256, faster_hex::hex_string(&Sha256::digest(archive)));
    assert_eq!(agent.reserve_count.load(Ordering::SeqCst), 2);
    assert_eq!(agent.complete_count.load(Ordering::SeqCst), 1);
    assert!(object.attempts.load(Ordering::SeqCst) >= 2);
    assert_eq!(object.bodies.lock().expect("uploaded bodies").as_slice(), &[archive.to_vec()]);
    let certificates = object.client_certificates.lock().expect("object certificates");
    assert!(certificates.len() >= 2);
    assert!(certificates.iter().all(|presented| !presented));
}

#[tokio::test]
async fn untrusted_object_store_certificate_stops_before_completion() {
    let temp = tempfile::tempdir().expect("tempdir");
    let archive_path = temp.path().join("support-bundle.tar.zst");
    fs::write(&archive_path, b"redacted support bundle evidence").expect("write archive");
    let agent_pki = TestPki::new();
    let object_pki = TestPki::new();
    let object = object_server(&object_pki).await;
    let agent = agent_server(&agent_pki, object.endpoint.clone()).await;

    let error = ReportUploadClient::new(config(&temp, &agent_pki, &agent.endpoint), Duration::from_secs(5))
        .expect("report client")
        .upload(&archive_path, &CancellationToken::new())
        .await
        .expect_err("untrusted object store must fail");

    assert!(matches!(error, ReportUploadError::TlsPeer));
    assert_eq!(agent.reserve_count.load(Ordering::SeqCst), 1);
    assert_eq!(agent.complete_count.load(Ordering::SeqCst), 0);
}

#[cfg(unix)]
fn private_mode(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("set private mode");
}

#[cfg(not(unix))]
fn private_mode(_path: &std::path::Path) {}
