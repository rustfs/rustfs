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

use std::fs::{self, File};
use std::io::{Cursor, Read as _};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier as _};
use p256::pkcs8::DecodePublicKey as _;
use rustfs::connect::{
    CLIENT_CAPABILITY, ClientOperation, ClientOutcome, ClientPerformanceError, ClientPerformanceRequest, ClientProbe,
    ClientProbeError, ClientProbeFuture, ClientProbeMeasurement, ClientProvenance, ClientReasonCode, ClientTargetReasonCode,
    DeviceIdentity, HttpClientProbe, LocalClientConsent, measure_client, save_signed_client_export, sign_client_export,
    validate_client_limits,
};
use rustfs::embedded::{RustFSServerBuilder, find_available_port};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("current time").as_secs() as i64
}

fn request(operation: ClientOperation) -> ClientPerformanceRequest {
    let now = now();
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000010";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000011");
    ClientPerformanceRequest {
        organization_name: organization.to_owned(),
        cluster_name: cluster.clone(),
        device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000012"),
        run_uid: "019e3ae0-0000-7000-8000-000000000013".to_owned(),
        artifact_uid: "019e3ae0-0000-7000-8000-000000000014".to_owned(),
        schema_version: 1,
        capability: CLIENT_CAPABILITY.to_owned(),
        consent: LocalClientConsent {
            consent_uid: "019e3ae0-0000-7000-8000-000000000015".to_owned(),
            policy_revision: 7,
            expires_at_unix: now + 120,
            confirmed: true,
        },
        produced_at_unix: now,
        expires_at_unix: now + 60,
        nonce: [0x6b; 32],
        duration: Duration::from_secs(1),
        operation,
        traffic_bytes: 65_536,
        target_alias: "deployment-1".to_owned(),
        provenance: ClientProvenance::new("c".repeat(40), "d".repeat(64), "1.0.0-rc.6", vec!["default".to_owned()]),
    }
}

struct SuccessfulProbe;

impl ClientProbe for SuccessfulProbe {
    fn probe<'a>(&'a self, request: &'a ClientPerformanceRequest, _cancel: &'a CancellationToken) -> ClientProbeFuture<'a> {
        Box::pin(async move {
            Ok(ClientProbeMeasurement {
                transferred_bytes: request.traffic_bytes,
                duration: Duration::from_millis(100),
                latency: Duration::from_millis(7),
            })
        })
    }
}

struct ErrorProbe(ClientProbeError);

impl ClientProbe for ErrorProbe {
    fn probe<'a>(&'a self, _request: &'a ClientPerformanceRequest, _cancel: &'a CancellationToken) -> ClientProbeFuture<'a> {
        Box::pin(async move { Err(self.0) })
    }
}

struct PendingProbe;

impl ClientProbe for PendingProbe {
    fn probe<'a>(&'a self, _request: &'a ClientPerformanceRequest, _cancel: &'a CancellationToken) -> ClientProbeFuture<'a> {
        Box::pin(std::future::pending())
    }
}

struct CountingProbe(AtomicUsize);

impl ClientProbe for CountingProbe {
    fn probe<'a>(&'a self, request: &'a ClientPerformanceRequest, _cancel: &'a CancellationToken) -> ClientProbeFuture<'a> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            Ok(ClientProbeMeasurement {
                transferred_bytes: request.traffic_bytes,
                duration: Duration::from_millis(100),
                latency: Duration::from_millis(1),
            })
        })
    }
}

#[tokio::test]
async fn typed_get_and_put_results_match_the_frozen_schema() {
    let _guard = TEST_LOCK.lock().await;
    for operation in [ClientOperation::GetObject, ClientOperation::PutObject] {
        let request = request(operation);
        let measurement = measure_client(&request, &SuccessfulProbe, &CancellationToken::new())
            .await
            .expect("client measurement");
        assert_eq!(measurement.result.outcome(), ClientOutcome::Succeeded);
        assert_eq!(measurement.result.reason_code(), ClientReasonCode::Complete);
        let data = measurement.result.data().expect("aggregate data");
        assert_eq!(data.operation, operation);
        assert_eq!(data.transferred_bytes, 65_536);
        assert_eq!(data.completed_operations, 1);
        assert_eq!(data.duration_millis, 100);
        assert_eq!(data.error_count, 0);
        assert_eq!(measurement.target.target_alias, "deployment-1");
        assert_eq!(measurement.target.parameters.operation, operation);
        assert_eq!(measurement.target.parameters.requested_bytes, 65_536);
        assert_eq!(measurement.target.parameters.concurrency, 1);
        assert_eq!(measurement.target.latency_micros, Some(7_000));

        let value = serde_json::to_value(&measurement.result).expect("result JSON");
        assert_eq!(value["toolId"], "performance.client");
        assert_eq!(value["capability"], "performance.client@1");
        assert_eq!(value["coverage"]["unit"], "WINDOW");
        assert_eq!(value["data"]["operation"], operation.as_str());
        assert!(value["data"].get("latencyMicros").is_none());
    }
}

#[tokio::test]
async fn consent_and_budget_fail_before_transport() {
    let _guard = TEST_LOCK.lock().await;
    let probe = CountingProbe(AtomicUsize::new(0));

    let mut unsupported_version = request(ClientOperation::PutObject);
    unsupported_version.schema_version = 2;
    assert!(matches!(
        measure_client(&unsupported_version, &probe, &CancellationToken::new()).await,
        Err(ClientPerformanceError::UnsupportedVersion)
    ));

    let mut unsupported_capability = request(ClientOperation::PutObject);
    unsupported_capability.capability = "performance.client@2".to_owned();
    assert!(matches!(
        measure_client(&unsupported_capability, &probe, &CancellationToken::new()).await,
        Err(ClientPerformanceError::UnsupportedCapability)
    ));

    let mut no_consent = request(ClientOperation::PutObject);
    no_consent.consent.confirmed = false;
    assert!(matches!(
        measure_client(&no_consent, &probe, &CancellationToken::new()).await,
        Err(ClientPerformanceError::ConsentRequired)
    ));

    let mut over_traffic = request(ClientOperation::PutObject);
    over_traffic.traffic_bytes = 1_048_577;
    assert!(matches!(
        measure_client(&over_traffic, &probe, &CancellationToken::new()).await,
        Err(ClientPerformanceError::LimitExceeded)
    ));

    let mut invalid_version = request(ClientOperation::PutObject);
    invalid_version.provenance =
        ClientProvenance::new("a".repeat(40), "b".repeat(64), "release_candidate", vec!["default".to_owned()]);
    assert!(matches!(
        measure_client(&invalid_version, &probe, &CancellationToken::new()).await,
        Err(ClientPerformanceError::InvalidRequest)
    ));

    let mut invalid_feature = request(ClientOperation::PutObject);
    invalid_feature.provenance = ClientProvenance::new("a".repeat(40), "b".repeat(64), "1.0.0-rc.6", vec!["Default".to_owned()]);
    assert!(matches!(
        measure_client(&invalid_feature, &probe, &CancellationToken::new()).await,
        Err(ClientPerformanceError::InvalidRequest)
    ));
    assert_eq!(probe.0.load(Ordering::Relaxed), 0);

    assert!(validate_client_limits(Duration::from_secs(1), 1_048_576).is_ok());
    assert!(matches!(
        validate_client_limits(Duration::from_secs(1), 1_048_577),
        Err(ClientPerformanceError::LimitExceeded)
    ));
    assert!(matches!(
        validate_client_limits(Duration::from_nanos(1), 1),
        Err(ClientPerformanceError::LimitExceeded)
    ));
    assert!(matches!(
        validate_client_limits(Duration::from_secs(30) + Duration::from_nanos(1), 1),
        Err(ClientPerformanceError::LimitExceeded)
    ));
}

#[tokio::test]
async fn endpoint_proxy_permission_timeout_and_cancel_are_explicit() {
    let _guard = TEST_LOCK.lock().await;
    for (error, expected_reason, expected_result_reason) in [
        (
            ClientProbeError::EndpointUnavailable,
            ClientTargetReasonCode::EndpointUnavailable,
            ClientReasonCode::SourceUnavailable,
        ),
        (
            ClientProbeError::ProxyFailure,
            ClientTargetReasonCode::ProxyFailure,
            ClientReasonCode::CollectionFailed,
        ),
        (
            ClientProbeError::PermissionDenied,
            ClientTargetReasonCode::PermissionDenied,
            ClientReasonCode::PermissionDenied,
        ),
        (
            ClientProbeError::TimedOut,
            ClientTargetReasonCode::TimedOut,
            ClientReasonCode::CollectionFailed,
        ),
    ] {
        let measurement = measure_client(&request(ClientOperation::PutObject), &ErrorProbe(error), &CancellationToken::new())
            .await
            .expect("typed failure");
        assert_eq!(measurement.result.outcome(), ClientOutcome::Failed);
        assert_eq!(measurement.result.reason_code(), expected_result_reason);
        assert_eq!(measurement.target.reason_code, expected_reason);
        assert!(measurement.result.data().is_none());
    }

    let cancel = CancellationToken::new();
    cancel.cancel();
    let measurement = measure_client(&request(ClientOperation::PutObject), &SuccessfulProbe, &cancel)
        .await
        .expect("typed cancellation");
    assert_eq!(measurement.result.outcome(), ClientOutcome::Cancelled);
    assert_eq!(measurement.target.reason_code, ClientTargetReasonCode::Cancelled);
}

#[tokio::test]
async fn deadline_and_in_flight_cancellation_stop_a_stalled_probe() {
    let _guard = TEST_LOCK.lock().await;
    let mut timed = request(ClientOperation::GetObject);
    timed.duration = Duration::from_millis(20);
    timed.traffic_bytes = 1_024;
    let measurement = measure_client(&timed, &PendingProbe, &CancellationToken::new())
        .await
        .expect("typed timeout");
    assert_eq!(measurement.result.outcome(), ClientOutcome::Failed);
    assert_eq!(measurement.target.reason_code, ClientTargetReasonCode::TimedOut);

    let cancel = CancellationToken::new();
    let cancellation = cancel.clone();
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        cancellation.cancel();
    });
    let measurement = measure_client(&request(ClientOperation::PutObject), &PendingProbe, &cancel)
        .await
        .expect("typed in-flight cancellation");
    assert_eq!(measurement.result.outcome(), ClientOutcome::Cancelled);
    assert_eq!(measurement.target.reason_code, ClientTargetReasonCode::Cancelled);
}

#[tokio::test]
async fn only_one_client_collector_can_run_at_a_time() {
    let _guard = TEST_LOCK.lock().await;
    let first_cancel = CancellationToken::new();
    let second_cancel = CancellationToken::new();
    let first_request = request(ClientOperation::GetObject);
    let second_request = request(ClientOperation::PutObject);
    let first = measure_client(&first_request, &PendingProbe, &first_cancel);
    let second = async {
        tokio::task::yield_now().await;
        measure_client(&second_request, &PendingProbe, &second_cancel).await
    };
    let cancellation = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        first_cancel.cancel();
    };
    let (first, second, ()) = tokio::join!(first, second, cancellation);
    assert_eq!(first.expect("typed cancellation").result.outcome(), ClientOutcome::Cancelled);
    assert!(matches!(second, Err(ClientPerformanceError::Busy)));
}

#[tokio::test]
async fn configured_proxy_and_direct_endpoint_failures_are_distinct() {
    let _guard = TEST_LOCK.lock().await;
    let proxy_probe = HttpClientProbe::new(
        "http://127.0.0.1:1",
        None,
        Some("http://127.0.0.1:9"),
        Zeroizing::new("access".to_owned()),
        Zeroizing::new("secret".to_owned()),
        Zeroizing::new(String::new()),
        Duration::from_millis(100),
    )
    .expect("proxy probe");
    let measurement = measure_client(&request(ClientOperation::GetObject), &proxy_probe, &CancellationToken::new())
        .await
        .expect("typed proxy failure");
    assert_eq!(measurement.target.reason_code, ClientTargetReasonCode::ProxyFailure);

    let direct_probe = HttpClientProbe::new(
        "http://127.0.0.1:9",
        None,
        None,
        Zeroizing::new("access".to_owned()),
        Zeroizing::new("secret".to_owned()),
        Zeroizing::new(String::new()),
        Duration::from_millis(100),
    )
    .expect("direct probe");
    let measurement = measure_client(&request(ClientOperation::GetObject), &direct_probe, &CancellationToken::new())
        .await
        .expect("typed endpoint failure");
    assert_eq!(measurement.target.reason_code, ClientTargetReasonCode::EndpointUnavailable);
}

#[tokio::test]
async fn oversized_response_is_rejected_without_buffering_it() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let _guard = TEST_LOCK.lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("response listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("client connection");
        let mut request = vec![0_u8; 16 * 1024];
        let _ = socket.read(&mut request).await.expect("request headers");
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 65537\r\nConnection: close\r\n\r\n")
            .await
            .expect("response headers");
    });
    let probe = HttpClientProbe::new(
        &format!("http://{address}"),
        None,
        None,
        Zeroizing::new("access".to_owned()),
        Zeroizing::new("secret".to_owned()),
        Zeroizing::new(String::new()),
        Duration::from_secs(1),
    )
    .expect("client probe");
    let measurement = measure_client(&request(ClientOperation::GetObject), &probe, &CancellationToken::new())
        .await
        .expect("typed protocol failure");
    assert_eq!(measurement.target.reason_code, ClientTargetReasonCode::ProtocolFailure);
    server.await.expect("response server");
}

#[tokio::test]
async fn signed_result_is_saved_without_overwrite() {
    let _guard = TEST_LOCK.lock().await;
    let request = request(ClientOperation::PutObject);
    let measurement = measure_client(&request, &SuccessfulProbe, &CancellationToken::new())
        .await
        .expect("client measurement");
    let export = sign_client_export(&request, &measurement, &DeviceIdentity::generate(), &CancellationToken::new())
        .expect("signed export");
    let directory = tempfile::tempdir().expect("output directory");
    let output = directory.path().join("client.zip");
    let saved = save_signed_client_export(&output, &export, &CancellationToken::new()).expect("save export");
    assert_eq!(saved.archive_sha256, export.archive_sha256);
    assert!(matches!(
        save_signed_client_export(&output, &export, &CancellationToken::new()),
        Err(ClientPerformanceError::AlreadyExists)
    ));

    let mut tampered = export.clone();
    tampered.archive_bytes[0] ^= 0xff;
    assert!(matches!(
        save_signed_client_export(&directory.path().join("tampered.zip"), &tampered, &CancellationToken::new()),
        Err(ClientPerformanceError::InvalidRequest)
    ));

    let mut oversized = export.clone();
    oversized.archive_bytes = vec![0; 524_289];
    oversized.archive_sha256 = hex_lower(&Sha256::digest(&oversized.archive_bytes));
    assert!(matches!(
        save_signed_client_export(&directory.path().join("oversized.zip"), &oversized, &CancellationToken::new()),
        Err(ClientPerformanceError::LimitExceeded)
    ));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        save_signed_client_export(&directory.path().join("cancelled.zip"), &export, &cancelled),
        Err(ClientPerformanceError::Cancelled)
    ));
}

#[tokio::test]
async fn real_rustfs_endpoint_supports_bounded_get_and_put() {
    let _guard = TEST_LOCK.lock().await;
    let port = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("find free port: {err}"),
    };
    let server = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port}"))
        .access_key("client-perf-access")
        .secret_key("client-perf-secret")
        .build()
        .await
        .expect("start embedded server");
    let probe = HttpClientProbe::new(
        &server.endpoint(),
        None,
        None,
        Zeroizing::new(server.access_key().to_owned()),
        Zeroizing::new(server.secret_key().to_owned()),
        Zeroizing::new(String::new()),
        Duration::from_secs(2),
    )
    .expect("client probe");

    for operation in [ClientOperation::GetObject, ClientOperation::PutObject] {
        let mut request = request(operation);
        request.duration = Duration::from_secs(2);
        let measurement = measure_client(&request, &probe, &CancellationToken::new())
            .await
            .expect("real client measurement");
        assert_eq!(measurement.result.outcome(), ClientOutcome::Succeeded);
        assert_eq!(measurement.result.data().expect("data").transferred_bytes, 65_536);
        assert_eq!(measurement.target.completed_operations, 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_cli_writes_a_verifiable_export_with_exact_binary_provenance() {
    let _guard = TEST_LOCK.lock().await;
    let port = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("find free port: {err}"),
    };
    let server = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port}"))
        .access_key("client-perf-access")
        .secret_key("client-perf-secret")
        .build()
        .await
        .expect("start embedded server");
    let temp = tempfile::tempdir().expect("CLI tempdir");
    let state = temp.path().join("state");
    let output = temp.path().join("client.zip");
    let access_key_file = temp.path().join("access-key");
    let secret_key_file = temp.path().join("secret-key");
    write_credential(&access_key_file, server.access_key());
    write_credential(&secret_key_file, server.secret_key());
    let identity = rustfs::connect::IdentityStore::new(state.join("identity"))
        .load_or_create()
        .expect("enrolled identity");

    let current = now();
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000010";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000011");
    let mut command = Command::new(env!("CARGO_BIN_EXE_rustfs"));
    command
        .args(["connect", "performance", "client", "--state-dir"])
        .arg(&state)
        .args(["--endpoint", &server.endpoint(), "--access-key-file"])
        .arg(&access_key_file)
        .arg("--secret-key-file")
        .arg(&secret_key_file)
        .arg("--output")
        .arg(&output)
        .args(["--organization", organization, "--cluster", &cluster, "--device"])
        .arg(format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000012"))
        .args([
            "--run-uid",
            "019e3ae0-0000-7000-8000-000000000013",
            "--artifact-uid",
            "019e3ae0-0000-7000-8000-000000000014",
            "--consent-uid",
            "019e3ae0-0000-7000-8000-000000000015",
            "--policy-revision",
            "7",
            "--consent-expires-at",
            &(current + 120).to_string(),
            "--expires-at",
            &(current + 60).to_string(),
            "--operation",
            "get",
            "--traffic-bytes",
            "65536",
            "--duration-millis",
            "1000",
            "--acknowledge-l1",
        ]);
    let result = tokio::task::spawn_blocking(move || command.output())
        .await
        .expect("CLI task")
        .expect("run production rustfs binary");

    assert!(result.status.success(), "stderr: {}", String::from_utf8_lossy(&result.stderr));
    let stdout = String::from_utf8(result.stdout).expect("UTF-8 stdout");
    assert!(stdout.contains("tool=performance.client outcome=SUCCEEDED reason=COMPLETE\n"));
    assert!(stdout.contains("upload=not-performed\n"));
    #[cfg(unix)]
    assert_eq!(fs::metadata(&output).expect("output metadata").permissions().mode(), 0o100600);

    let archive_bytes = fs::read(&output).expect("saved archive");
    let mut archive = zip::ZipArchive::new(Cursor::new(archive_bytes.as_slice())).expect("signed archive");
    let envelope_bytes = read_archive_member(&mut archive, "envelope.json");
    let signature_bytes = read_archive_member(&mut archive, "envelope.sig");
    let result_bytes = read_archive_member(&mut archive, "result.json");
    let envelope: serde_json::Value = serde_json::from_slice(&envelope_bytes).expect("envelope JSON");
    let signed_result: serde_json::Value = serde_json::from_slice(&result_bytes).expect("result JSON");
    assert_eq!(envelope["classification"], "L1");
    assert_eq!(envelope["payload"]["sha256"], hex_lower(&Sha256::digest(&result_bytes)));
    assert_eq!(signed_result["data"]["operation"], "GET_OBJECT");
    assert_eq!(signed_result["data"]["transferredBytes"], 65_536);
    assert_eq!(signed_result["provenance"]["sourceCommit"], rustfs::version::build::COMMIT_HASH);
    assert_eq!(
        signed_result["provenance"]["executableSha256"],
        sha256_file(Path::new(env!("CARGO_BIN_EXE_rustfs")))
    );

    let signature_document: serde_json::Value = serde_json::from_slice(&signature_bytes).expect("signature JSON");
    let raw = URL_SAFE_NO_PAD
        .decode_to_vec(signature_document["value"].as_str().expect("signature value"))
        .expect("base64url signature");
    let signature = Signature::from_slice(&raw).expect("P-256 signature");
    let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
    signed.extend_from_slice(&envelope_bytes);
    VerifyingKey::from_public_key_der(&identity.public_key_der())
        .expect("public key")
        .verify(&signed, &signature)
        .expect("valid ES256 signature");
}

fn write_credential(path: &Path, value: &str) {
    fs::write(path, value).expect("write credential");
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("protect credential");
}

fn read_archive_member(archive: &mut zip::ZipArchive<Cursor<&[u8]>>, name: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    archive
        .by_name(name)
        .expect("archive member")
        .read_to_end(&mut bytes)
        .expect("read archive member");
    bytes
}

fn sha256_file(path: &Path) -> String {
    let mut file = File::open(path).expect("open exact binary");
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).expect("hash exact binary");
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    hex_lower(&digest.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}
