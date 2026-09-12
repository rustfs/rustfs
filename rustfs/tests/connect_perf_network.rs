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

mod connect {
    pub use rustfs::connect::DeviceIdentity;
}

#[allow(dead_code)]
#[path = "../src/connect/diagnostics/perf_network.rs"]
mod perf_network;

use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier as _};
use p256::pkcs8::DecodePublicKey as _;
use perf_network::{
    LocalNetworkConsent, MAX_NETWORK_DURATION, MAX_TRAFFIC_BYTES, NETWORK_CAPABILITY, NetworkOutcome, NetworkPeerHarness,
    NetworkPerformanceError, NetworkPerformanceRequest, NetworkProvenance, NetworkReasonCode, PeerProbeError, PeerProbeFuture,
    PeerProbeMeasurement, PeerReasonCode, measure_network, measure_network_with_harness, save_signed_network_export,
    sign_network_export,
};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

static TEST_HARNESS_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("current time").as_secs() as i64
}

fn request(peer_count: usize) -> NetworkPerformanceRequest {
    let now = now();
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000010";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000011");
    NetworkPerformanceRequest {
        organization_name: organization.to_owned(),
        cluster_name: cluster.clone(),
        device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000012"),
        run_uid: "019e3ae0-0000-7000-8000-000000000013".to_owned(),
        artifact_uid: "019e3ae0-0000-7000-8000-000000000014".to_owned(),
        schema_version: 1,
        capability: NETWORK_CAPABILITY.to_owned(),
        consent: LocalNetworkConsent {
            consent_uid: "019e3ae0-0000-7000-8000-000000000015".to_owned(),
            policy_revision: 7,
            expires_at_unix: now + 120,
            confirmed: true,
        },
        produced_at_unix: now,
        expires_at_unix: now + 60,
        nonce: [0x6b; 32],
        duration: Duration::from_secs(1),
        peer_aliases: (1..=peer_count).map(|index| format!("peer-{index}")).collect(),
        traffic_bytes_per_peer: 4_096,
        provenance: NetworkProvenance::new("c".repeat(40), "d".repeat(64), "1.0.0-rc.6", vec!["default".to_owned()]),
    }
}

struct TcpPeerHarness {
    addresses: BTreeMap<String, SocketAddr>,
}

impl NetworkPeerHarness for TcpPeerHarness {
    fn probe<'a>(&'a self, peer_alias: &'a str, traffic_bytes: u64, cancel: &'a CancellationToken) -> PeerProbeFuture<'a> {
        Box::pin(async move {
            let address = *self.addresses.get(peer_alias).ok_or(PeerProbeError::ProtocolFailure)?;
            let started = Instant::now();
            let mut stream = tokio::select! {
                () = cancel.cancelled() => return Err(PeerProbeError::Cancelled),
                result = TcpStream::connect(address) => result.map_err(|_| PeerProbeError::Unreachable)?,
            };
            stream.write_all(&[0x51]).await.map_err(|_| PeerProbeError::ProtocolFailure)?;
            let mut pong = [0_u8; 1];
            stream
                .read_exact(&mut pong)
                .await
                .map_err(|_| PeerProbeError::ProtocolFailure)?;
            if pong != [0x52] {
                return Err(PeerProbeError::ProtocolFailure);
            }
            let latency = started.elapsed();
            let payload = vec![0x5a; usize::try_from(traffic_bytes).map_err(|_| PeerProbeError::ProtocolFailure)?];
            stream
                .write_all(&payload)
                .await
                .map_err(|_| PeerProbeError::ProtocolFailure)?;
            stream.shutdown().await.map_err(|_| PeerProbeError::ProtocolFailure)?;
            Ok(PeerProbeMeasurement {
                transferred_bytes: traffic_bytes,
                duration: started.elapsed(),
                latency,
            })
        })
    }
}

async fn echo_peer() -> (SocketAddr, tokio::task::JoinHandle<usize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind controlled peer");
    let address = listener.local_addr().expect("peer address");
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept controlled probe");
        let mut ping = [0_u8; 1];
        stream.read_exact(&mut ping).await.expect("read ping");
        assert_eq!(ping, [0x51]);
        stream.write_all(&[0x52]).await.expect("write pong");
        let mut transferred = 0;
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buffer).await.expect("read bounded payload");
            if read == 0 {
                return transferred;
            }
            transferred += read;
        }
    });
    (address, task)
}

async fn unused_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind unused port");
    let address = listener.local_addr().expect("unused address");
    drop(listener);
    address
}

#[test]
fn native_network_source_is_explicitly_unsupported() {
    let request = request(1);
    let measurement = measure_network(&request, &CancellationToken::new()).expect("typed unsupported result");
    assert_eq!(measurement.result.outcome(), NetworkOutcome::Unsupported);
    assert_eq!(measurement.result.reason_code(), NetworkReasonCode::SourceUnavailable);
    assert!(measurement.result.data().is_none());
    assert!(measurement.peers.is_empty());

    let value = serde_json::to_value(&measurement.result).expect("result JSON");
    assert_eq!(value["toolId"], "performance.network");
    assert_eq!(value["capability"], "performance.network@1");
    assert_eq!(value["outcome"], "UNSUPPORTED");
    assert_eq!(value["reasonCode"], "SOURCE_UNAVAILABLE");
    assert_eq!(value["data"], serde_json::Value::Null);
    assert_eq!(value["provenance"]["repository"], "rustfs/rustfs");
    assert_eq!(value["provenance"]["sourceCommit"], "c".repeat(40));
    assert_eq!(value["provenance"]["executableSha256"], "d".repeat(64));

    assert!(matches!(
        sign_network_export(&request, &measurement, &connect::DeviceIdentity::generate(), &CancellationToken::new()),
        Err(NetworkPerformanceError::InvalidRequest)
    ));
}

#[tokio::test]
async fn controlled_peer_reports_exact_bytes_duration_latency_and_attributed_failure() {
    let _guard = TEST_HARNESS_LOCK.lock().await;
    let (working_address, working_peer) = echo_peer().await;
    let unavailable_address = unused_address().await;
    let harness = TcpPeerHarness {
        addresses: BTreeMap::from([
            ("peer-1".to_owned(), working_address),
            ("peer-2".to_owned(), unavailable_address),
        ]),
    };
    let measurement = measure_network_with_harness(&request(2), &harness, &CancellationToken::new())
        .await
        .expect("controlled network measurement");
    assert_eq!(working_peer.await.expect("peer task"), 4_096);
    assert_eq!(measurement.result.outcome(), NetworkOutcome::Partial);
    assert_eq!(measurement.result.reason_code(), NetworkReasonCode::CollectionFailed);
    let data = measurement.result.data().expect("partial aggregate");
    assert_eq!(data.transferred_bytes, 4_096);
    assert_eq!(data.error_count, 1);
    assert_eq!(data.peer_count, 2);
    assert!(data.duration_millis >= 1);
    assert_eq!(measurement.peers[0].peer_alias, "peer-1");
    assert_eq!(measurement.peers[0].reason_code, PeerReasonCode::Complete);
    assert_eq!(measurement.peers[0].transferred_bytes, 4_096);
    assert!(measurement.peers[0].latency_micros.is_some());
    assert_eq!(measurement.peers[1].peer_alias, "peer-2");
    assert_eq!(measurement.peers[1].reason_code, PeerReasonCode::Unreachable);
    assert_eq!(measurement.peers[1].transferred_bytes, 0);

    let aggregate = serde_json::to_value(&measurement.result).expect("aggregate result JSON");
    assert_eq!(aggregate["data"]["transferredBytes"], 4_096);
    assert_eq!(aggregate["data"]["errorCount"], 1);
    assert_eq!(aggregate["data"]["peerCount"], 2);
    assert!(aggregate.get("peers").is_none(), "frozen aggregate schema has no per-peer field");
}

#[tokio::test]
async fn slow_peer_is_attributed_and_stops_at_the_wall_clock_limit() {
    let _guard = TEST_HARNESS_LOCK.lock().await;
    let mut request = request(1);
    request.duration = Duration::from_millis(20);
    request.traffic_bytes_per_peer = 1_000;
    let started = Instant::now();
    let measurement = measure_network_with_harness(&request, &BlockingHarness, &CancellationToken::new())
        .await
        .expect("typed timeout result");
    assert!(started.elapsed() < Duration::from_millis(100));
    assert_eq!(measurement.result.outcome(), NetworkOutcome::Failed);
    assert_eq!(measurement.result.reason_code(), NetworkReasonCode::CollectionFailed);
    assert!(measurement.result.data().is_none());
    assert_eq!(measurement.peers.len(), 1);
    assert_eq!(measurement.peers[0].peer_alias, "peer-1");
    assert_eq!(measurement.peers[0].reason_code, PeerReasonCode::TimedOut);
}

struct CountingHarness(AtomicUsize);

impl NetworkPeerHarness for CountingHarness {
    fn probe<'a>(&'a self, _peer_alias: &'a str, traffic_bytes: u64, _cancel: &'a CancellationToken) -> PeerProbeFuture<'a> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            Ok(PeerProbeMeasurement {
                transferred_bytes: traffic_bytes,
                duration: Duration::from_millis(10),
                latency: Duration::from_millis(1),
            })
        })
    }
}

struct ShortTransferHarness;

impl NetworkPeerHarness for ShortTransferHarness {
    fn probe<'a>(&'a self, _peer_alias: &'a str, traffic_bytes: u64, _cancel: &'a CancellationToken) -> PeerProbeFuture<'a> {
        Box::pin(async move {
            Ok(PeerProbeMeasurement {
                transferred_bytes: traffic_bytes - 1,
                duration: Duration::from_millis(10),
                latency: Duration::from_millis(1),
            })
        })
    }
}

#[tokio::test]
async fn short_transfer_cannot_be_reported_as_a_successful_benchmark() {
    let _guard = TEST_HARNESS_LOCK.lock().await;
    let measurement = measure_network_with_harness(&request(1), &ShortTransferHarness, &CancellationToken::new())
        .await
        .expect("typed failed measurement");
    assert_eq!(measurement.result.outcome(), NetworkOutcome::Failed);
    assert!(measurement.result.data().is_none());
    assert_eq!(measurement.peers[0].reason_code, PeerReasonCode::ProtocolFailure);
}

#[tokio::test]
async fn invalid_and_over_budget_requests_fail_before_peer_io() {
    let harness = CountingHarness(AtomicUsize::new(0));
    let mut over_traffic = request(1);
    over_traffic.traffic_bytes_per_peer = MAX_TRAFFIC_BYTES + 1;
    assert!(matches!(
        measure_network_with_harness(&over_traffic, &harness, &CancellationToken::new()).await,
        Err(NetworkPerformanceError::LimitExceeded)
    ));

    let mut over_duration = request(1);
    over_duration.duration = MAX_NETWORK_DURATION + Duration::from_millis(1);
    assert!(matches!(
        measure_network_with_harness(&over_duration, &harness, &CancellationToken::new()).await,
        Err(NetworkPerformanceError::LimitExceeded)
    ));

    let mut no_consent = request(1);
    no_consent.consent.confirmed = false;
    assert!(matches!(
        measure_network_with_harness(&no_consent, &harness, &CancellationToken::new()).await,
        Err(NetworkPerformanceError::ConsentRequired)
    ));

    let mut unknown_peer = request(1);
    unknown_peer.peer_aliases[0] = "node-raw-hostname".to_owned();
    assert!(matches!(
        measure_network_with_harness(&unknown_peer, &harness, &CancellationToken::new()).await,
        Err(NetworkPerformanceError::InvalidRequest)
    ));
    assert_eq!(harness.0.load(Ordering::Relaxed), 0);
}

struct BlockingHarness;

impl NetworkPeerHarness for BlockingHarness {
    fn probe<'a>(&'a self, _peer_alias: &'a str, traffic_bytes: u64, cancel: &'a CancellationToken) -> PeerProbeFuture<'a> {
        Box::pin(async move {
            tokio::select! {
                () = cancel.cancelled() => Err(PeerProbeError::Cancelled),
                () = tokio::time::sleep(Duration::from_millis(100)) => Ok(PeerProbeMeasurement {
                    transferred_bytes: traffic_bytes,
                    duration: Duration::from_millis(100),
                    latency: Duration::from_millis(5),
                }),
            }
        })
    }
}

#[tokio::test]
async fn cancellation_stops_collection_and_only_one_collector_runs() {
    let _guard = TEST_HARNESS_LOCK.lock().await;
    let first_cancel = CancellationToken::new();
    let second_cancel = CancellationToken::new();
    let request = request(1);
    let first = measure_network_with_harness(&request, &BlockingHarness, &first_cancel);
    let second = async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        measure_network_with_harness(&request, &BlockingHarness, &second_cancel).await
    };
    let cancellation = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        first_cancel.cancel();
    };
    let (first, second, ()) = tokio::join!(first, second, cancellation);
    let first = first.expect("typed cancelled result");
    assert_eq!(first.result.outcome(), NetworkOutcome::Cancelled);
    assert_eq!(first.result.reason_code(), NetworkReasonCode::Cancelled);
    assert!(first.result.data().is_none());
    assert!(matches!(second, Err(NetworkPerformanceError::Busy)));
}

#[tokio::test]
async fn successful_result_has_signed_bounded_private_offline_export() {
    let _guard = TEST_HARNESS_LOCK.lock().await;
    let request = request(1);
    let (address, peer) = echo_peer().await;
    let harness = TcpPeerHarness {
        addresses: BTreeMap::from([("peer-1".to_owned(), address)]),
    };
    let measurement = measure_network_with_harness(&request, &harness, &CancellationToken::new())
        .await
        .expect("successful controlled result");
    assert_eq!(peer.await.expect("peer task"), 4_096);
    assert_eq!(measurement.result.outcome(), NetworkOutcome::Succeeded);
    let identity = connect::DeviceIdentity::generate();
    let export = sign_network_export(&request, &measurement, &identity, &CancellationToken::new()).expect("signed export");
    assert!(export.result_json.len() <= perf_network::MAX_RESULT_BYTES);
    assert!(export.envelope_json.len() <= perf_network::MAX_ENVELOPE_BYTES);
    assert!(export.archive_bytes.len() <= perf_network::MAX_ARCHIVE_BYTES);
    assert_eq!(export.archive_sha256, hex(&Sha256::digest(&export.archive_bytes)));
    let mut archive = zip::ZipArchive::new(Cursor::new(&export.archive_bytes)).expect("three-file archive");
    assert_eq!(archive.len(), 3);
    assert_eq!(archive.by_index(0).expect("envelope entry").name(), "envelope.json");
    assert_eq!(archive.by_index(1).expect("signature entry").name(), "envelope.sig");
    assert_eq!(archive.by_index(2).expect("result entry").name(), "result.json");

    let envelope: serde_json::Value = serde_json::from_slice(&export.envelope_json).expect("envelope JSON");
    assert_eq!(envelope["toolId"], "performance.network");
    assert_eq!(envelope["classification"], "L1");
    assert_eq!(envelope["payload"]["path"], "result.json");
    assert_eq!(envelope["payload"]["sizeBytes"], export.result_json.len());
    assert_eq!(envelope["payload"]["sha256"], hex(&Sha256::digest(&export.result_json)));
    let result: serde_json::Value = serde_json::from_slice(&export.result_json).expect("result JSON");
    assert_eq!(result["outcome"], "SUCCEEDED");
    assert_eq!(result["data"]["transferredBytes"], 4_096);

    let signature_document: serde_json::Value = serde_json::from_slice(&export.envelope_signature).expect("signature document");
    assert_eq!(signature_document["algorithm"], "ES256");
    let raw_signature = URL_SAFE_NO_PAD
        .decode_to_vec(signature_document["value"].as_str().expect("signature value"))
        .expect("base64url signature");
    assert_eq!(raw_signature.len(), 64);
    let signature = Signature::from_slice(&raw_signature).expect("P-256 signature");
    assert_eq!(signature, signature.normalize_s());
    let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
    signed.extend_from_slice(&export.envelope_json);
    VerifyingKey::from_public_key_der(&identity.public_key_der())
        .expect("public key")
        .verify(&signed, &signature)
        .expect("signature over exact envelope bytes");

    let directory = tempfile::tempdir().expect("temporary directory");
    let output = directory.path().join("network-performance.zip");
    let receipt = save_signed_network_export(&output, &export, &CancellationToken::new()).expect("save offline export");
    assert_eq!(receipt.archive_sha256, export.archive_sha256);
    assert_eq!(fs::read(&output).expect("saved archive"), export.archive_bytes);
    #[cfg(unix)]
    assert_eq!(fs::metadata(&output).expect("metadata").permissions().mode() & 0o777, 0o600);
    assert!(matches!(
        save_signed_network_export(&output, &export, &CancellationToken::new()),
        Err(NetworkPerformanceError::AlreadyExists)
    ));

    let mut forged = export.clone();
    forged.artifact_uid = "../escape".to_owned();
    assert!(matches!(
        save_signed_network_export(&directory.path().join("forged.zip"), &forged, &CancellationToken::new()),
        Err(NetworkPerformanceError::InvalidRequest)
    ));

    let cancelled_output = directory.path().join("cancelled.zip");
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        save_signed_network_export(&cancelled_output, &export, &cancelled),
        Err(NetworkPerformanceError::Cancelled)
    ));
    assert!(!cancelled_output.exists());
}

fn hex(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}
