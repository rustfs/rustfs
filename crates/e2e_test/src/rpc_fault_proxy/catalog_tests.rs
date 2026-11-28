// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use http_body_util::StreamBody;
use hyper::{Response, body::Frame};
use rustfs_lock::{LockId, LockRequest, LockType, ObjectKey};
use rustfs_protos::proto_gen::node_service::{GenerallyLockResponse, RenameDataRequest};
use sha2::{Digest as _, Sha256};
use std::convert::Infallible;

const HEAD: &str = "catalog/vectors/test-bucket/indexes/test-index/table.json";

#[derive(Clone)]
struct Received {
    body: Bytes,
    headers: http::HeaderMap,
    trailers: Option<http::HeaderMap>,
}

struct CatalogBackend {
    address: SocketAddr,
    received: Arc<Mutex<Vec<Received>>>,
    task: JoinHandle<()>,
}

impl Drop for CatalogBackend {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn catalog_backend(rename: RenameDataResponse) -> io::Result<CatalogBackend> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let received = Arc::new(Mutex::new(Vec::new()));
    let capture = received.clone();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((socket, _)) = accepted else { return };
                    let received = capture.clone();
                    let rename = rename.clone();
                    connections.spawn(async move {
                        let service = service_fn(move |request: Request<Incoming>| {
                            let received = received.clone();
                            let rename = rename.clone();
                            async move {
                                let is_rename = request.uri().path().ends_with("/RenameData");
                                let (parts, body) = request.into_parts();
                                let collected = body.collect().await?;
                                let trailers = collected.trailers().cloned();
                                received.lock().expect("backend evidence").push(Received {
                                    body: collected.to_bytes(), headers: parts.headers, trailers,
                                });
                                let reply = if is_rename { frame(&rename) } else {
                                    frame(&GenerallyLockResponse { success: true, ..Default::default() })
                                };
                                let mut trailers = http::HeaderMap::new();
                                trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                                trailers.insert("x-proof", http::HeaderValue::from_static("response-proof"));
                                let stream = futures::stream::iter([
                                    Ok::<_, Infallible>(Frame::data(reply)), Ok(Frame::trailers(trailers)),
                                ]);
                                Ok::<_, hyper::Error>(Response::new(StreamBody::new(stream)))
                            }
                        });
                        let _ = auto::Builder::new(TokioExecutor::new()).serve_connection(TokioIo::new(socket), service).await;
                    });
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    Ok(CatalogBackend { address, received, task })
}

fn frame(message: &impl prost::Message) -> Bytes {
    let payload = message.encode_to_vec();
    let mut bytes = vec![0];
    bytes.extend_from_slice(&u32::try_from(payload.len()).expect("bounded test frame").to_be_bytes());
    bytes.extend_from_slice(&payload);
    bytes.into()
}

fn rename_request(source: &str, volume: &str, key: &str) -> Bytes {
    frame(&RenameDataRequest {
        disk: "http://[::1]:9000/task-disk".into(),
        src_volume: ".rustfs.sys/tmp".into(),
        src_path: source.into(),
        dst_volume: volume.into(),
        dst_path: key.into(),
        file_info: "opaque signed metadata".into(),
        ..Default::default()
    })
}

fn lock_request(resource: ObjectKey, owner: &str) -> LockRequest {
    LockRequest::new(resource, LockType::Exclusive, owner)
}

fn lock_frame(request: &LockRequest) -> Bytes {
    frame(&GenerallyLockRequest {
        args: serde_json::to_string(request).expect("real lock wire shape"),
    })
}

fn scope(source: Option<&str>) -> CatalogRenameScope {
    CatalogRenameScope {
        dst_volume: ".rustfs.sys".into(),
        dst_path: HEAD.into(),
        src_path: source.map(str::to_owned),
    }
}

async fn send(address: SocketAddr, method: &str, body: Bytes) -> io::Result<Bytes> {
    let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-packet-proof", http::HeaderValue::from_static("original-trailers"));
    let body = Full::new(body)
        .map_err(|never| match never {})
        .with_trailers(async move { Some(Ok::<_, io::Error>(trailers)) })
        .boxed();
    let request = Request::builder()
        .method("POST")
        .uri(format!("http://{address}/node_service.NodeService/{method}"))
        .header(http::header::TE, "trailers")
        .header("x-rustfs-rpc-signature-v2", "original-signature")
        .header("x-rustfs-rpc-nonce", "original-nonce")
        .header("x-rustfs-content-sha256", "original-body-digest")
        .header("x-request-id", "catalog-test-request")
        .body(body)
        .map_err(io::Error::other)?;
    let response = client.request(request).await.map_err(io::Error::other)?;
    let collected = response.into_body().collect().await.map_err(io::Error::other)?;
    assert_eq!(collected.trailers().expect("response trailers")["x-proof"], "response-proof");
    Ok(collected.to_bytes())
}

async fn completed(gate: &CatalogRpcGate, responses: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while gate.snapshot().responses != responses {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("selected backend replies must be observed");
}

#[tokio::test]
async fn catalog_refresh_and_head_rules_are_independent_and_exact() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let backend = catalog_backend(RenameDataResponse {
            success: true,
            ..Default::default()
        })
        .await
        .expect("backend");
        let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
            .await
            .expect("proxy");
        let resource = ObjectKey::new(".rustfs.sys", HEAD);
        let mut old = lock_request(resource.clone(), "writer-a");
        let new = lock_request(resource.clone(), "writer-b");
        old.owner.clear();
        let refresh = proxy
            .pause_catalog_refresh(vec![old.lock_id.clone()])
            .expect("exact refresh gate");
        let rename = proxy
            .pause_catalog_rename(scope(Some("attempt-a")), 1)
            .expect("independent rename gate");
        let address = proxy.local_addr();
        let old_frame = lock_frame(&old);
        let a_refresh = tokio::spawn(send(address, "Refresh", old_frame.clone()));
        let original = rename_request("attempt-a", ".rustfs.sys", HEAD);
        let a_rename = tokio::spawn(send(address, "RenameData", original.clone()));
        refresh.wait_for_hits(1).await.expect("old renewal intercepted");
        rename.wait_for_hits(1).await.expect("head intercepted");
        assert_eq!(backend.received.lock().expect("backend evidence").len(), 0);
        send(address, "Refresh", lock_frame(&new))
            .await
            .expect("another lock id passes");
        let mut wrong_resource = old.clone();
        wrong_resource.resource = ObjectKey::new(".rustfs.sys", format!("{HEAD}-suffix"));
        send(address, "Refresh", lock_frame(&wrong_resource))
            .await
            .expect("inconsistent resource does not match");
        for packet in [
            rename_request("attempt-b", ".rustfs.sys", HEAD),
            rename_request("attempt-a", ".rustfs.sys", &format!("{HEAD}-suffix")),
            rename_request("attempt-a", "ordinary-s3", HEAD),
        ] {
            send(address, "RenameData", packet).await.expect("unrelated mutation passes");
        }
        assert_eq!(refresh.snapshot().hits, 1);
        assert_eq!(rename.snapshot().hits, 1);
        rename.release(0).expect("release only head");
        a_rename.await.expect("head caller").expect("head response");
        completed(&rename, 1).await;
        assert!(!a_refresh.is_finished(), "head release cannot release renewal");
        refresh.discard(0).expect("discard original renewal");
        assert!(a_refresh.await.expect("renewal caller").is_err());
        let evidence: CatalogRpcEvidence = rename.snapshot();
        assert_eq!(
            (
                evidence.hits,
                evidence.blocked,
                evidence.forwarded,
                evidence.responses,
                evidence.discarded
            ),
            (1, 0, 1, 1, 0)
        );
        let captured = base64_simd::STANDARD
            .decode_to_vec(&evidence.packets[0].body_base64)
            .expect("complete captured request body");
        assert_eq!(captured.as_slice(), original.as_ref());
        assert_eq!(evidence.packets[0].body_sha256.len(), 64);
        assert!(
            evidence.packets[0]
                .body_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        assert_eq!(
            evidence.packets[0].body_sha256,
            hex_simd::encode_to_string(Sha256::digest(&captured), hex_simd::AsciiCase::Lower)
        );
        assert_eq!(evidence.packets[0].response.as_ref().expect("rename reply").success, Some(true));
        assert_eq!(refresh.snapshot().packets[0].locks[0].lock_id, old.lock_id);
        let received = backend.received.lock().expect("backend evidence");
        let actual = received
            .iter()
            .find(|packet| packet.body == original)
            .expect("same original bytes at backend");
        assert_eq!(actual.body.as_ref(), captured.as_slice());
        assert_eq!(actual.headers["x-rustfs-rpc-signature-v2"], "original-signature");
        assert_eq!(actual.headers["x-rustfs-content-sha256"], "original-body-digest");
        assert_eq!(actual.headers["x-rustfs-rpc-nonce"], "original-nonce");
        assert_eq!(actual.trailers.as_ref().expect("request trailers")["x-packet-proof"], "original-trailers");
        assert!(
            !received.iter().any(|packet| packet.body == old_frame),
            "discarded renewal never reaches backend"
        );
    })
    .await
    .expect("dual rule deadline");
}

#[tokio::test]
async fn catalog_lock_observer_records_exact_members_without_changing_batches() {
    let backend = catalog_backend(RenameDataResponse::default()).await.expect("backend");
    let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
        .await
        .expect("proxy");
    let resource = ObjectKey::new(".rustfs.sys", HEAD);
    let observer = proxy.observe_catalog_locks(vec![resource.clone()]).expect("observer");
    let selected = lock_request(resource, "writer-a");
    let other = lock_request(ObjectKey::new(".rustfs.sys", format!("{HEAD}-suffix")), "writer-b");
    let batch = frame(&BatchGenerallyLockRequest {
        args: [&other, &selected]
            .into_iter()
            .map(|request| serde_json::to_string(request).expect("lock"))
            .collect(),
    });
    send(proxy.local_addr(), "LockBatch", batch.clone())
        .await
        .expect("whole batch forwarded");
    send(proxy.local_addr(), "Lock", lock_frame(&other))
        .await
        .expect("unrelated lock forwarded");
    let evidence = observer.snapshot();
    assert_eq!((evidence.hits, evidence.blocked, evidence.forwarded), (1, 0, 1));
    assert_eq!(evidence.packets[0].locks.len(), 1);
    assert_eq!(evidence.packets[0].locks[0].lock_id, selected.lock_id);
    assert_eq!(evidence.packets[0].locks[0].owner, "writer-a");
    assert_eq!(backend.received.lock().expect("backend evidence")[0].body, batch);
}

#[tokio::test]
async fn catalog_retained_packets_survive_disconnect_and_follow_explicit_order() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let backend = catalog_backend(RenameDataResponse {
            success: true,
            ..Default::default()
        })
        .await
        .expect("backend");
        let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
            .await
            .expect("proxy");
        let gate = proxy.pause_catalog_rename(scope(None), 2).expect("two retained attempts");
        let address = proxy.local_addr();
        let first = rename_request("old-attempt", ".rustfs.sys", HEAD);
        let socket = tokio::net::TcpStream::connect(address).await.expect("original connection");
        let socket = socket.into_std().expect("original socket control");
        let disconnect = socket.try_clone().expect("original socket shutdown handle");
        let socket = tokio::net::TcpStream::from_std(socket).expect("original async socket");
        let (mut sender, connection) = hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(socket))
            .await
            .expect("original HTTP/2 connection");
        let connection = tokio::spawn(connection);
        let request = Request::builder()
            .method("POST")
            .uri(format!("http://{address}/node_service.NodeService/RenameData"))
            .body(Full::new(first.clone()))
            .expect("original request");
        let caller = tokio::spawn(async move { sender.send_request(request).await });
        gate.wait_for_hits(1).await.expect("original request captured");
        // Hyper runs transport and response tasks separately from the connection future.
        // Shut down the actual socket so the pending caller observes the disconnect.
        disconnect
            .shutdown(std::net::Shutdown::Both)
            .expect("disconnect original TCP socket");
        assert!(
            tokio::time::timeout(Duration::from_secs(2), caller)
                .await
                .expect("original caller observes TCP shutdown")
                .expect("disconnected caller")
                .is_err()
        );
        let _ = tokio::time::timeout(Duration::from_secs(2), connection)
            .await
            .expect("original HTTP/2 connection terminates after TCP shutdown")
            .expect("original connection driver completes");
        let retained = gate.snapshot();
        assert_eq!((retained.hits, retained.blocked, retained.forwarded, retained.responses), (1, 1, 0, 0));
        assert!(retained.packets[0].transport_error.is_none());
        assert!(backend.received.lock().expect("backend evidence").is_empty());
        let second = rename_request("second-attempt", ".rustfs.sys", HEAD);
        let newer = tokio::spawn(send(address, "RenameData", second.clone()));
        gate.wait_for_hits(2).await.expect("second packet captured");
        send(address, "RenameData", rename_request("winner", ".rustfs.sys", HEAD))
            .await
            .expect("limit permits competitor");
        gate.release(1).expect("release second first");
        newer.await.expect("second caller").expect("second reply");
        gate.release(0).expect("deliver original after caller retirement");
        completed(&gate, 2).await;
        let evidence = gate.snapshot();
        assert_eq!((evidence.hits, evidence.blocked, evidence.forwarded, evidence.discarded), (2, 0, 2, 0));
        assert_eq!(evidence.packets[1].settle_sequence, Some(0));
        assert_eq!(evidence.packets[0].settle_sequence, Some(1));
        for (packet, expected) in evidence.packets.iter().zip([&first, &second]) {
            let captured = base64_simd::STANDARD
                .decode_to_vec(&packet.body_base64)
                .expect("retained original frame");
            assert_eq!(captured.as_slice(), expected.as_ref());
            assert_eq!(
                packet.body_sha256,
                hex_simd::encode_to_string(Sha256::digest(&captured), hex_simd::AsciiCase::Lower)
            );
        }
        assert!(gate.release(0).is_err(), "one original packet can only be delivered once");
        let received = backend.received.lock().expect("backend evidence");
        assert_eq!(received.len(), 3, "competitor and each retained packet arrive once");
        assert_eq!(received[1].body, second);
        assert_eq!(received[2].body, first);
    })
    .await
    .expect("late packet deadline");
}

#[tokio::test]
async fn catalog_discard_and_gate_drop_never_replay_packets() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let backend = catalog_backend(RenameDataResponse::default()).await.expect("backend");
        let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
            .await
            .expect("proxy");
        for explicit in [true, false] {
            let gate = proxy.pause_catalog_rename(scope(None), 1).expect("gate");
            let caller = tokio::spawn(send(proxy.local_addr(), "RenameData", rename_request("discard", ".rustfs.sys", HEAD)));
            gate.wait_for_hits(1).await.expect("packet captured");
            if explicit {
                gate.discard_all().expect("discard pending packet");
                assert_eq!((gate.snapshot().discarded, gate.snapshot().forwarded), (1, 0));
                assert!(gate.release(0).is_err(), "discard is terminal");
            }
            drop(gate);
            assert!(caller.await.expect("discarded caller").is_err());
            assert!(backend.received.lock().expect("backend evidence").is_empty());
        }
    })
    .await
    .expect("discard deadline");
}

#[tokio::test]
async fn catalog_response_evidence_distinguishes_backend_refusal_from_delivery() {
    let reply = RenameDataResponse {
        success: false,
        error: Some(rustfs_protos::proto_gen::node_service::Error {
            code: 42,
            error_info: "stale candidate".into(),
        }),
        ..Default::default()
    };
    let expected = frame(&reply);
    let backend = catalog_backend(reply).await.expect("backend");
    let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
        .await
        .expect("proxy");
    let gate = proxy.pause_catalog_rename(scope(None), 1).expect("gate");
    let caller = tokio::spawn(send(proxy.local_addr(), "RenameData", rename_request("rejected", ".rustfs.sys", HEAD)));
    gate.wait_for_hits(1).await.expect("real packet");
    gate.release_all().expect("forward original");
    assert_eq!(caller.await.expect("caller").expect("reply"), expected);
    let evidence = gate.snapshot();
    let response = evidence.packets[0].response.as_ref().expect("completed backend reply");
    assert_eq!(response.body, expected.to_vec());
    assert_eq!(
        response.body_sha256,
        hex_simd::encode_to_string(Sha256::digest(&expected), hex_simd::AsciiCase::Lower)
    );
    assert_eq!(
        (response.http_status, response.grpc_status.as_deref(), response.success),
        (200, Some("0"), Some(false))
    );
    assert!(response.error.as_deref().expect("typed refusal").contains("stale candidate"));
    assert_eq!((evidence.forwarded, evidence.responses, evidence.refused), (1, 1, 1));
}

#[tokio::test]
async fn catalog_rules_reject_ambiguous_or_unbounded_scopes() {
    let backend = catalog_backend(RenameDataResponse::default()).await.expect("backend");
    let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
        .await
        .expect("proxy");
    for candidate in [
        CatalogRenameScope {
            dst_volume: "ordinary".into(),
            ..scope(None)
        },
        CatalogRenameScope {
            dst_path: String::new(),
            ..scope(None)
        },
        CatalogRenameScope {
            src_path: Some(String::new()),
            ..scope(None)
        },
    ] {
        assert!(proxy.pause_catalog_rename(candidate, 1).is_err());
    }
    assert!(proxy.pause_catalog_rename(scope(None), 0).is_err());
    assert!(proxy.pause_catalog_rename(scope(None), 65).is_err());
    assert!(proxy.pause_catalog_refresh(Vec::<LockId>::new()).is_err());
    assert!(proxy.observe_catalog_locks(Vec::<ObjectKey>::new()).is_err());
}

#[tokio::test]
async fn catalog_large_unrelated_rename_preserves_stream_and_trailers() {
    let backend = catalog_backend(RenameDataResponse {
        success: true,
        ..Default::default()
    })
    .await
    .expect("backend");
    let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
        .await
        .expect("proxy");
    let gate = proxy.pause_catalog_rename(scope(None), 1).expect("head gate");
    let ordinary = frame(&RenameDataRequest {
        src_volume: ".rustfs.sys/tmp".into(),
        src_path: "ordinary-attempt".into(),
        dst_volume: "ordinary-s3".into(),
        dst_path: "large-inline".into(),
        file_info: "x".repeat(2 * 1024 * 1024),
        ..Default::default()
    });
    send(proxy.local_addr(), "RenameData", ordinary.clone())
        .await
        .expect("large ordinary request passes");
    assert_eq!(gate.snapshot().hits, 0);
    assert!(
        gate.snapshot().packets.is_empty(),
        "unrelated large body is neither captured nor truncated"
    );
    let received = backend.received.lock().expect("backend evidence");
    assert_eq!(received[0].body, ordinary);
    assert_eq!(
        received[0].trailers.as_ref().expect("original trailers")["x-packet-proof"],
        "original-trailers"
    );
}

#[tokio::test]
async fn catalog_proxy_drop_cancels_selected_original_without_replaying_it() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let backend = catalog_backend(RenameDataResponse::default()).await.expect("backend");
        let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
            .await
            .expect("proxy");
        let gate = proxy.pause_catalog_rename(scope(None), 1).expect("gate");
        let caller = tokio::spawn(send(
            proxy.local_addr(),
            "RenameData",
            rename_request("retired-proxy", ".rustfs.sys", HEAD),
        ));
        gate.wait_for_hits(1).await.expect("captured packet");
        drop(proxy);
        assert!(caller.await.expect("cancelled caller").is_err());
        while gate.snapshot().blocked != 0 {
            tokio::task::yield_now().await;
        }
        assert!(gate.release(0).is_err(), "an aborted proxy cannot release a retained packet");
        assert!(backend.received.lock().expect("backend evidence").is_empty());
        assert!(gate.snapshot().packets[0].transport_error.is_some());
    })
    .await
    .expect("proxy drop deadline");
}

#[tokio::test]
async fn catalog_acquisition_partition_preserves_exact_frames_and_other_methods() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let backend = catalog_backend(RenameDataResponse {
            success: true,
            ..Default::default()
        })
        .await
        .expect("backend");
        let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
            .await
            .expect("proxy");
        let resource = ObjectKey::new(".rustfs.sys", HEAD);
        let selected = lock_request(resource.clone(), "writer-a");
        let unrelated = lock_request(ObjectKey::new(".rustfs.sys", format!("{HEAD}-suffix")), "writer-b");
        let gate = proxy
            .pause_catalog_locks(vec![resource.clone()])
            .expect("acquisition partition");
        assert!(
            proxy.observe_catalog_locks(vec![resource]).is_err(),
            "observer must not bypass an acquisition partition"
        );
        let address = proxy.local_addr();
        let single = lock_frame(&selected);
        let batch = frame(&BatchGenerallyLockRequest {
            args: [&unrelated, &selected]
                .into_iter()
                .map(|request| serde_json::to_string(request).expect("typed lock"))
                .collect(),
        });
        let first = tokio::spawn(send(address, "Lock", single.clone()));
        gate.wait_for_hits(1).await.expect("initial Lock blocked");
        let second = tokio::spawn(send(address, "LockBatch", batch.clone()));
        gate.wait_for_hits(2).await.expect("initial batch blocked");
        assert_eq!(backend.received.lock().expect("backend evidence").len(), 0);
        send(address, "Refresh", single.clone())
            .await
            .expect("Refresh remains available");
        send(address, "Lock", lock_frame(&unrelated))
            .await
            .expect("neighboring resource remains available");
        let mut inconsistent = selected.clone();
        inconsistent.lock_id.resource = unrelated.resource.clone();
        send(address, "Lock", lock_frame(&inconsistent))
            .await
            .expect("inconsistent lock identity does not match");
        send(address, "RenameData", rename_request("candidate", ".rustfs.sys", HEAD))
            .await
            .expect("data lane remains available");
        assert_eq!(gate.snapshot().hits, 2);
        gate.release_all().expect("restore original acquisition packets");
        first.await.expect("single task").expect("single response");
        second.await.expect("batch task").expect("batch response");
        completed(&gate, 2).await;
        let evidence = gate.snapshot();
        assert_eq!(
            (evidence.blocked, evidence.forwarded, evidence.discarded, evidence.overflow),
            (0, 2, 0, 0)
        );
        for packet in &evidence.packets {
            assert_eq!(packet.locks.len(), 1);
            assert_eq!(packet.locks[0].lock_id, selected.lock_id);
            assert_eq!(packet.locks[0].owner, selected.owner);
        }
        let received = backend.received.lock().expect("backend evidence");
        assert_eq!(received.len(), 6);
        assert!(received[4..].iter().any(|request| request.body == single));
        assert!(received[4..].iter().any(|request| request.body == batch));
    })
    .await
    .expect("acquisition partition deadline");
}

#[tokio::test]
async fn catalog_acquisition_partition_drop_discards_pending_and_removes_rule() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let backend = catalog_backend(RenameDataResponse::default()).await.expect("backend");
        let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
            .await
            .expect("proxy");
        let resource = ObjectKey::new(".rustfs.sys", HEAD);
        let gate = proxy.pause_catalog_locks(vec![resource.clone()]).expect("partition");
        let request = lock_frame(&lock_request(resource.clone(), "writer-a"));
        let held = tokio::spawn(send(proxy.local_addr(), "Lock", request.clone()));
        gate.wait_for_hits(1).await.expect("blocked request");
        assert_eq!(gate.snapshot().overflow, 0);
        drop(gate);
        assert!(held.await.expect("request owner").is_err());
        assert!(backend.received.lock().expect("backend evidence").is_empty());
        let observer = proxy.observe_catalog_locks(vec![resource]).expect("partition slot released");
        send(proxy.local_addr(), "Lock", request.clone())
            .await
            .expect("restored request");
        completed(&observer, 1).await;
        assert_eq!(backend.received.lock().expect("backend evidence")[0].body, request);
    })
    .await
    .expect("partition owner drain deadline");
}
