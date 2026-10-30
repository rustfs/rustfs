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

//! Full namespace POST coverage. The response tap retains an actual server-side
//! output owner; a network client's Bytes would not own that reservation.

use super::{Body, InternodeRpcService, NS_SCANNER_PATH, NS_SCANNER_PROTOCOL_VERSION, NS_SCANNER_SERVER_EPOCH};
use crate::storage::storage_api::ecstore_config::com::{read_config, save_config};
use crate::storage::storage_api::ecstore_rpc::gen_signature_headers;
use crate::storage::storage_api::rpc_consumer::http_service::DiskAPI as _;
use crate::storage::storage_api::{BackgroundMemoryBudget, set_object_store_resolver};
use bytes::Bytes;
use futures_util::{StreamExt as _, TryStreamExt as _};
use hmac::{Hmac, KeyInit as _, Mac as _};
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt as _, Full};
use hyper::{client::conn::http1 as client_http1, server::conn::http1 as server_http1};
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use rustfs_concurrency::adaptive::{AdaptiveAdmission, FeedbackController, FeedbackObservation, NodeAdmission, ResourcePressure};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::convert::Infallible;
use std::num::NonZeroU16;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tower::Service as _;
use uuid::Uuid;

const TEST_NAME: &str = "storage::rpc::http_service::namespace_memory_tests::namespace_post_memory_admission_and_output_owner";
const CHILD: &str = "RUSTFS_NAMESPACE_MEMORY_STREAM_CHILD";
const CYCLE: u64 = 7;
const EPOCH: u64 = 9;
const OUTPUT_CAPACITY: u64 = 64 * 1024;

#[derive(Serialize)]
struct RequestBudget {
    max_duration_ms: Option<u64>,
    max_objects: Option<u64>,
    max_directories: Option<u64>,
}

// Wire fixture encoded with the normal named MessagePack representation, then
// checked by the production decoder before it is sent over TCP.
#[derive(Serialize)]
struct NamespaceRequest {
    version: u16,
    request_id: Uuid,
    server_epoch: Uuid,
    session_id: Uuid,
    session_sequence: u64,
    bucket: String,
    next_cycle: u64,
    leader_epoch: u64,
    scan_plan_digest: rustfs_scanner::DataUsageScanPlanDigest,
    checkpoint_plan_digest: Option<rustfs_scanner::DataUsageScanPlanDigest>,
    skip_healing: bool,
    scan_mode: u8,
    budget: RequestBudget,
}

#[derive(Deserialize)]
struct FrameEnvelope {
    version: u16,
    sequence: u64,
    payload: Vec<u8>,
    mac: Vec<u8>,
}

#[test]
fn namespace_post_memory_admission_and_output_owner() {
    if let Ok(mode) = std::env::var(CHILD) {
        crate::app::gating_test_env::run_large_stack_test("namespace-memory-rpc", move || async move {
            tokio::time::timeout(Duration::from_secs(90), run_namespace_case(&mode))
                .await
                .expect("real namespace RPC fixture must finish");
        });
        println!("NAMESPACE_MEMORY_STREAM_OK");
        return;
    }
    for mode in ["zero", "tight", "funded", "disabled"] {
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
            .env(CHILD, mode)
            .env("RUSTFS_INTERNODE_PREWARM", "false")
            .env("RUSTFS_INTERNODE_IDEMPOTENT_READ_RETRIES", "0")
            .output()
            .expect("isolated namespace stream process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "namespace stream {mode}: {stdout}{stderr}");
        assert!(
            stdout.contains("NAMESPACE_MEMORY_STREAM_OK"),
            "child did not execute namespace stream: {stdout}"
        );
    }
}

async fn run_namespace_case(mode: &str) {
    let _ = rustfs_credentials::set_global_rpc_secret("namespace-memory-stream-test-secret".to_owned());
    let memory = BackgroundMemoryBudget::new();
    assert!(memory.install(), "isolated process owns its memory budget");
    memory.set_sampled_limit(16 * 1024 * 1024, Duration::from_secs(120));

    // Reuse the ordinary four-disk ECStore setup, including metadata and the
    // single-node publication proof. No scanner/auth/fence hook is installed.
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let resolver_store = store.clone();
    assert!(set_object_store_resolver(Arc::new(move || Some(resolver_store.clone()))));
    let state = rustfs_scanner_metrics::metrics::CurrentCycle {
        next: CYCLE,
        ..Default::default()
    };
    let mut fence = CYCLE.to_le_bytes().to_vec();
    fence.extend_from_slice(b"RSCYC001");
    fence.extend_from_slice(&EPOCH.to_le_bytes());
    fence.extend_from_slice(&rmp_serde::to_vec(&state).expect("real CurrentCycle payload"));
    save_config(store.clone(), rustfs_scanner::DATA_USAGE_BLOOM_NAME_PATH.as_str(), fence.clone())
        .await
        .expect("persist the real cycle fence through ECStore");
    assert_eq!(
        read_config(store.clone(), rustfs_scanner::DATA_USAGE_BLOOM_NAME_PATH.as_str())
            .await
            .expect("read back persisted fence"),
        fence
    );
    rustfs_scanner::validate_remote_scanner_request_fence(CYCLE, EPOCH)
        .await
        .expect("production fence validator accepts the persisted state");

    let disks: Vec<_> = store
        .disk_map
        .get(&0)
        .expect("fixture pool")
        .iter()
        .flatten()
        .cloned()
        .collect();
    assert_eq!(disks.len(), 4, "the registered receiver uses four real disks");
    let disk = disks[0].clone();
    let physical = AdaptiveAdmission::new(NonZeroU16::MIN, 1);
    for disk in &disks {
        disk.physical_admission()
            .expect("registered physical binding")
            .set_test_capacity(vec![physical.clone()])
            .expect("publish the actual receiver physical plan");
    }
    let node = NodeAdmission::new(NonZeroU16::MIN);
    node.update(
        &mut FeedbackController::new(),
        Some(FeedbackObservation {
            generation: 1,
            max_concurrency: 1,
            pressure: ResourcePressure::Unknown,
            completed_units: None,
        }),
    );
    if mode != "disabled" {
        crate::startup_background::install_background_node_admission_for_test(node.clone());
        assert!(rustfs_scanner::set_scanner_node_admission(node.clone()));
    }
    assert_eq!(memory.snapshot().used, 0, "fixture construction has no escaped payload owner");
    memory.set_sampled_limit(
        match mode {
            "zero" | "disabled" => 0,
            "tight" => OUTPUT_CAPACITY,
            "funded" => 16 * 1024 * 1024,
            _ => panic!("unknown namespace test mode"),
        },
        Duration::from_secs(120),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("namespace HTTP receiver");
    let address = listener.local_addr().expect("namespace receiver address");
    let retained: Arc<Mutex<Option<Bytes>>> = Arc::new(Mutex::new(None));
    let server_retained = retained.clone();
    let response_status = Arc::new(AtomicU16::new(0));
    let server_status = response_status.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("namespace connection");
        let service = tower::service_fn(move |request| {
            let retained = server_retained.clone();
            let status = server_status.clone();
            async move {
                let fallback = tower::service_fn(|_| async { Ok::<_, Infallible>(Response::new(Body::empty())) });
                let mut receiver = InternodeRpcService::new(fallback);
                let response = receiver.call(request).await?;
                status.store(response.status().as_u16(), Ordering::Release);
                let (parts, body) = response.into_parts();
                let tapped = body.into_data_stream().inspect_ok(move |bytes| {
                    if !bytes.is_empty() {
                        *retained.lock().expect("output owner") = Some(bytes.slice(bytes.len() - 1..));
                    }
                });
                Ok::<_, Infallible>(Response::from_parts(parts, Body::from_stream(tapped.map_err(std::io::Error::other))))
            }
        });
        // Body errors intentionally reset the connection in the denial cases.
        let _ = server_http1::Builder::new()
            .serve_connection(TokioIo::new(socket), TowerToHyperService::new(service))
            .await;
    });
    let request_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let body = rmp_serde::to_vec_named(&NamespaceRequest {
        version: NS_SCANNER_PROTOCOL_VERSION,
        request_id,
        server_epoch: *NS_SCANNER_SERVER_EPOCH,
        session_id,
        session_sequence: 0,
        bucket: "namespace-memory-absent".into(),
        next_cycle: CYCLE,
        leader_epoch: EPOCH,
        scan_plan_digest: rustfs_scanner::DataUsageScanPlanDigest([5; 32]),
        checkpoint_plan_digest: None,
        skip_healing: true,
        scan_mode: 2,
        budget: RequestBudget {
            max_duration_ms: Some(30_000),
            max_objects: None,
            max_directories: None,
        },
    })
    .expect("namespace request MessagePack");
    let decoded = rustfs_scanner::decode_remote_scanner_request(&body).expect("real request decoder");
    assert!(rustfs_scanner::remote_scanner_request_matches_envelope(
        &decoded,
        request_id,
        *NS_SCANNER_SERVER_EPOCH,
        session_id,
        0,
        CYCLE,
        EPOCH
    ));
    let query = serde_urlencoded::to_string([
        ("disk", disk.endpoint().to_string()),
        ("ns_scanner_request_id", request_id.to_string()),
        ("ns_scanner_server_epoch", NS_SCANNER_SERVER_EPOCH.to_string()),
        ("ns_scanner_session_id", session_id.to_string()),
        ("ns_scanner_session_sequence", "0".into()),
        ("ns_scanner_cycle", CYCLE.to_string()),
        ("ns_scanner_leader_epoch", EPOCH.to_string()),
        (
            "ns_scanner_body_sha256",
            hex_simd::encode_to_string(Sha256::digest(&body), hex_simd::AsciiCase::Lower),
        ),
    ])
    .expect("namespace query");
    let uri = format!("{NS_SCANNER_PATH}?{query}");
    let headers = gen_signature_headers(&uri, &Method::POST).expect("real namespace signature");
    assert!(!super::rpc_background_admission(&headers).expect("unmarked Scanner wire class"));
    super::verify_rpc_signature(&uri, &Method::POST, &headers).expect("wire signature verifies before transport");
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(&uri)
        .header(http::header::HOST, address.to_string())
        .body(Full::new(Bytes::from(body)))
        .expect("namespace POST");
    request.headers_mut().extend(headers);
    let (mut sender, connection) =
        client_http1::handshake(TokioIo::new(TcpStream::connect(address).await.expect("real namespace TCP sender")))
            .await
            .expect("namespace HTTP handshake");
    let client = tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut wire = Vec::new();
    let mut failed = false;
    match sender.send_request(request).await {
        Ok(response) => {
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "must pass disk, signature, digest, replay and persisted-fence gates"
            );
            let mut stream = response.into_body().into_data_stream();
            while let Some(item) = stream.next().await {
                match item {
                    Ok(bytes) => wire.extend_from_slice(&bytes),
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            }
        }
        Err(error) => {
            assert!(matches!(mode, "zero" | "tight"), "funded/default response headers failed: {error}");
            failed = true;
        }
    }
    drop(sender);
    client.await.expect("namespace client task");
    server.await.expect("namespace server task");
    // Hyper may report a first-poll body error before flushing its headers.
    // Observe the handler result to distinguish that from preflight failure.
    assert_eq!(
        response_status.load(Ordering::Acquire),
        200,
        "the real receiver accepted the validated namespace POST"
    );
    if matches!(mode, "zero" | "tight") {
        assert!(failed, "admission denial cannot become clean EOF");
        assert!(wire.is_empty(), "no unreserved namespace frame payload may escape");
        assert!(retained.lock().expect("output owner").is_none());
    } else {
        assert!(!failed, "funded/default namespace response must complete");
        assert_authenticated_terminal_frames(&wire, request_id);
        assert_eq!(
            retained
                .lock()
                .expect("output owner")
                .as_ref()
                .expect("actual receiver output owner")
                .len(),
            1
        );
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while node.snapshot().active != 0 || physical.snapshot().active != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("HTTP completion releases the node and physical units");
    let expected = if mode == "funded" { OUTPUT_CAPACITY } else { 0 };
    wait_for_used(&memory, expected).await;
    drop(retained.lock().expect("output owner").take());
    wait_for_used(&memory, 0).await;
    if let Some(cancel) = store.background_cancel_token() {
        cancel.cancel();
    }
}

async fn wait_for_used(memory: &Arc<BackgroundMemoryBudget>, expected: u64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while memory.snapshot().used != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("expected retained bytes {expected}, observed {}", memory.snapshot().used));
}

fn assert_authenticated_terminal_frames(wire: &[u8], request_id: Uuid) {
    let secret = rustfs_credentials::try_get_rpc_token().expect("RPC authentication secret");
    let mut remaining = wire;
    let mut sequence = 0_u64;
    let mut terminal = false;
    while !remaining.is_empty() {
        assert!(remaining.len() >= 4, "complete frame length prefix");
        let length = usize::try_from(u32::from_be_bytes(remaining[..4].try_into().expect("frame prefix"))).expect("frame length");
        remaining = &remaining[4..];
        assert!(length > 0 && remaining.len() >= length, "complete namespace envelope");
        let envelope: FrameEnvelope = rmp_serde::from_slice(&remaining[..length]).expect("namespace frame envelope");
        assert_eq!(envelope.version, NS_SCANNER_PROTOCOL_VERSION);
        assert_eq!(envelope.sequence, sequence);
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("frame MAC key");
        mac.update(b"rustfs-ns-scanner-frame-v3");
        mac.update(request_id.as_bytes());
        mac.update(&sequence.to_be_bytes());
        mac.update(&envelope.payload);
        mac.verify_slice(&envelope.mac).expect("actual response frame authenticates");
        let frame: serde_json::Value = rmp_serde::from_slice(&envelope.payload).expect("namespace frame payload");
        let result = &frame["result"];
        assert!(result.get("error").is_none(), "accepted RPC must not substitute an error frame: {frame}");
        terminal |= result == "namespace_not_found" || result.get("complete").is_some();
        sequence += 1;
        remaining = &remaining[length..];
    }
    assert!(terminal, "actual Scanner worker emits a successful terminal frame");
}
