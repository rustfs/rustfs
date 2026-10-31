// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Bounded retention of original scoped ACK frames, independent of caller cancellation.

use super::{ProxyBody, ProxyClient, forward_to_backend, unary_payload};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::{Request, Response};
use prost::Message as _;
use rustfs_protos::proto_gen::node_service::{ScannerScopedDirtyUsageAckRequest, ScannerScopedDirtyUsageAckResponse};
use rustfs_protos::scoped_dirty_usage::validate_scoped_dirty_usage_request;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeSet,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, oneshot, watch},
    task::AbortHandle,
    time::Instant,
};

const MAX_RETAINED: usize = 4;
const MAX_BYTES: usize = 1024 * 1024;
const RETENTION: Duration = Duration::from_secs(120);

pub(crate) struct ScopedAckScope {
    pub nonce: String,
    pub owner_id: String,
    pub instance_id: String,
    pub buckets: BTreeSet<String>,
}

impl ScopedAckScope {
    fn validate(&self) -> io::Result<()> {
        let valid_uuid = |value: &str| uuid::Uuid::parse_str(value).is_ok_and(|value| !value.is_nil());
        if !valid_uuid(&self.nonce)
            || !valid_uuid(&self.owner_id)
            || self.instance_id.is_empty()
            || self.instance_id.len() > 128
            || self.buckets.is_empty()
            || self.buckets.len() > 16
            || self.buckets.iter().any(|bucket| bucket.is_empty() || bucket.len() > 63)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scoped ACK queue needs a bounded session, receiver and bucket scope",
            ));
        }
        Ok(())
    }

    fn matches(&self, request: &ScannerScopedDirtyUsageAckRequest) -> bool {
        !request.probe_only
            && validate_scoped_dirty_usage_request(request).is_ok()
            && request.owner_id == self.owner_id
            && request.instance_id == self.instance_id
            && request.entries.iter().all(|entry| self.buckets.contains(&entry.bucket))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScopedAckFault {
    Forward,
    RequestLoss,
    ResponseLoss,
}

struct Packet {
    action: watch::Sender<Option<ScopedAckFault>>,
    receipt: Mutex<Value>,
    task: Mutex<Option<AbortHandle>>,
}

pub(super) struct QueueState {
    scope: ScopedAckScope,
    capacity: usize,
    started: Instant,
    deadline: Instant,
    closed: AtomicBool,
    // The packet inventory lock is released before any packet lock or await.
    packets: Mutex<Vec<Arc<Packet>>>,
    changed: Notify,
}

pub(super) type QueueSlot = Arc<Mutex<Option<Arc<QueueState>>>>;

pub(super) fn empty_slot() -> QueueSlot {
    Arc::new(Mutex::new(None))
}

pub(crate) struct ScopedAckQueue {
    state: Arc<QueueState>,
    slot: QueueSlot,
}

impl ScopedAckQueue {
    pub(super) fn arm(slot: QueueSlot, scope: ScopedAckScope, capacity: usize) -> io::Result<Self> {
        scope.validate()?;
        if !(1..=MAX_RETAINED).contains(&capacity) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "scoped ACK capacity must be 1..=4"));
        }
        let mut active = slot.lock().expect("scoped ACK queue slot");
        if active.is_some() {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "scoped ACK queue is already armed"));
        }
        let started = Instant::now();
        let state = Arc::new(QueueState {
            scope,
            capacity,
            started,
            deadline: started + RETENTION,
            closed: AtomicBool::new(false),
            packets: Mutex::new(Vec::new()),
            changed: Notify::new(),
        });
        *active = Some(state.clone());
        drop(active);
        Ok(Self { state, slot })
    }

    pub(crate) fn receipts(&self) -> Vec<Value> {
        let packets = self.state.packets.lock().expect("scoped ACK packets").clone();
        packets
            .iter()
            .map(|packet| packet.receipt.lock().expect("scoped ACK receipt").clone())
            .collect()
    }

    pub(crate) async fn wait_for_packets(&self, count: usize) -> io::Result<()> {
        self.wait_until(|| self.receipts().len() >= count).await
    }

    pub(crate) async fn wait_for_completion(&self, id: usize) -> io::Result<Value> {
        self.wait_until(|| {
            self.receipts()
                .get(id)
                .is_some_and(|receipt| receipt["completed"].as_bool() == Some(true))
        })
        .await?;
        self.receipts()
            .get(id)
            .cloned()
            .ok_or_else(|| io::Error::other("scoped ACK receipt disappeared"))
    }

    async fn wait_until(&self, ready: impl Fn() -> bool) -> io::Result<()> {
        tokio::time::timeout_at(self.state.deadline, async {
            loop {
                let changed = self.state.changed.notified();
                if ready() {
                    return;
                }
                changed.await;
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "scoped ACK queue lifetime elapsed"))
    }

    pub(crate) fn settle(&self, id: usize, fault: ScopedAckFault) -> io::Result<()> {
        if Instant::now() >= self.state.deadline {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "scoped ACK retention expired"));
        }
        let packet = self
            .state
            .packets
            .lock()
            .expect("scoped ACK packets")
            .get(id)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "scoped ACK packet is not retained"))?;
        if !packet.action.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(fault);
                true
            } else {
                false
            }
        }) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "scoped ACK packet was already settled"));
        }
        Ok(())
    }
}

impl Drop for ScopedAckQueue {
    fn drop(&mut self) {
        self.state.closed.store(true, Ordering::SeqCst);
        let mut slot = self.slot.lock().expect("scoped ACK queue slot");
        if slot.as_ref().is_some_and(|current| Arc::ptr_eq(current, &self.state)) {
            *slot = None;
        }
        drop(slot);
        let packets = self.state.packets.lock().expect("scoped ACK packets").clone();
        for packet in packets {
            if let Some(task) = packet.task.lock().expect("scoped ACK task").take() {
                task.abort();
            }
        }
    }
}

pub(super) enum Interception {
    Pass(Request<ProxyBody>),
    Handled(Response<ProxyBody>),
}

fn restored_body(bytes: Bytes, trailers: Option<http::HeaderMap>) -> ProxyBody {
    Full::new(bytes)
        .map_err(|never| match never {})
        .with_trailers(async move { trailers.map(Ok::<_, io::Error>) })
        .boxed()
}

fn hash(bytes: &[u8]) -> String {
    hex(Sha256::digest(bytes).as_ref())
}

fn hex(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}

fn trailer_receipt(trailers: Option<&http::HeaderMap>) -> Value {
    trailers.map_or(Value::Null, |headers| {
        json!(
            headers
                .iter()
                .map(|(name, value)| { json!({"name": name.as_str(), "value_hex": hex(value.as_bytes())}) })
                .collect::<Vec<_>>()
        )
    })
}

fn header_hash(headers: &http::HeaderMap) -> String {
    let mut hasher = Sha256::new();
    for (name, value) in headers {
        hasher.update(name.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    hex(hasher.finalize().as_ref())
}

fn millis(state: &QueueState) -> u64 {
    u64::try_from(state.started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

pub(super) async fn intercept(
    request: Request<ProxyBody>,
    client: ProxyClient,
    target: SocketAddr,
    state: Arc<QueueState>,
) -> io::Result<Interception> {
    if Instant::now() >= state.deadline || state.closed.load(Ordering::SeqCst) {
        return Ok(Interception::Pass(request));
    }
    let (parts, body) = request.into_parts();
    let collected = tokio::time::timeout_at(state.deadline, Limited::new(body, MAX_BYTES).collect())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "scoped ACK frame collection expired"))?
        .map_err(io::Error::other)?;
    let trailers = collected.trailers().cloned();
    let bytes = collected.to_bytes();
    let decoded = ScannerScopedDirtyUsageAckRequest::decode(unary_payload(&bytes)?).map_err(io::Error::other)?;
    let request = Request::from_parts(parts, restored_body(bytes.clone(), trailers.clone()));
    if Instant::now() >= state.deadline || !state.scope.matches(&decoded) {
        return Ok(Interception::Pass(request));
    }
    let header = |name| request.headers().get(name).and_then(|value| value.to_str().ok());
    let (action, mut selected) = watch::channel(None);
    let packet = {
        let mut packets = state.packets.lock().expect("scoped ACK packets");
        if packets.len() >= state.capacity || state.closed.load(Ordering::SeqCst) {
            return Ok(Interception::Pass(request));
        }
        let id = packets.len();
        let packet = Arc::new(Packet {
            action,
            task: Mutex::new(None),
            receipt: Mutex::new(json!({
                "id": id, "session_nonce": state.scope.nonce, "receiver": target.to_string(),
                "authority": request.uri().authority().map(|value| value.as_str()),
                "captured_at_ms": millis(&state), "completed": false, "forwarded": false,
                "backend_complete": false, "outcome": "held", "request_bytes": bytes.len(),
            "request_sha256": hash(&bytes), "request_frame_hex": hex(&bytes), "request_headers_sha256": header_hash(request.headers()),
                "request_trailers_sha256": trailers.as_ref().map(header_hash),
            "request_trailers": trailer_receipt(trailers.as_ref()),
                "auth_nonce": header("x-rustfs-rpc-nonce"), "replay_nonce": header("x-rustfs-rpc-replay-nonce"),
                "signature_sha256": header("x-rustfs-rpc-signature-v2").map(|value| hash(value.as_bytes())),
                "replay_signature_sha256": header("x-rustfs-rpc-signature-v3").map(|value| hash(value.as_bytes())),
                "request": {"owner_id": decoded.owner_id, "instance_id": decoded.instance_id,
                    "probe_only": decoded.probe_only, "entries": decoded.entries.iter().map(|entry| json!({
                        "bucket": entry.bucket, "bucket_incarnation": uuid::Uuid::from_slice(&entry.bucket_incarnation).ok().map(|value| value.to_string()),
                        "generation": entry.generation })).collect::<Vec<_>>() }
            })),
        });
        packets.push(packet.clone());
        packet
    };
    let (reply, response) = oneshot::channel();
    let worker_packet = packet.clone();
    let worker_state = state.clone();
    // This owner survives an HTTP/2 stream reset from the original 5 s caller.
    // Every request is settled once and is discarded at the session deadline.
    let task = tokio::spawn(async move {
        let result = tokio::time::timeout_at(worker_state.deadline, async {
            let fault = loop {
                if let Some(fault) = *selected.borrow_and_update() {
                    break fault;
                }
                selected
                    .changed()
                    .await
                    .map_err(|_| io::Error::other("scoped ACK queue closed"))?;
            };
            if fault == ScopedAckFault::RequestLoss {
                worker_packet.receipt.lock().expect("scoped ACK receipt")["outcome"] = json!("request_lost");
                return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "injected scoped ACK request loss"));
            }
            if worker_state.closed.load(Ordering::SeqCst) || Instant::now() >= worker_state.deadline {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "scoped ACK session ended or expired before forwarding",
                ));
            }
            {
                let mut receipt = worker_packet.receipt.lock().expect("scoped ACK receipt");
                receipt["forwarded"] = json!(true);
                receipt["forwarded_at_ms"] = json!(millis(&worker_state));
            }
            let backend = forward_to_backend(request, client, target).await?;
            let (parts, body) = backend.into_parts();
            let collected = Limited::new(body, MAX_BYTES).collect().await.map_err(io::Error::other)?;
            let trailers = collected.trailers().cloned();
            let bytes = collected.to_bytes();
            let grpc_status = trailers
                .as_ref()
                .and_then(|values| values.get("grpc-status"))
                .or_else(|| parts.headers.get("grpc-status"))
                .and_then(|value| value.to_str().ok());
            let response = unary_payload(&bytes)
                .ok()
                .and_then(|payload| ScannerScopedDirtyUsageAckResponse::decode(payload).ok())
                .map(|response| {
                    json!({"owner_id": response.owner_id, "instance_id": response.instance_id,
                    "supported": response.supported, "cleared": response.cleared,
                    "response_proof_bytes": response.response_proof.len()})
                });
            {
                let mut receipt = worker_packet.receipt.lock().expect("scoped ACK receipt");
                receipt["backend_complete"] = json!(true);
                receipt["backend_completed_at_ms"] = json!(millis(&worker_state));
                receipt["response"] = json!({"status": parts.status.as_u16(), "grpc_status": grpc_status,
                    "bytes": bytes.len(), "sha256": hash(&bytes), "frame_hex": hex(&bytes), "headers_sha256": header_hash(&parts.headers),
                    "trailers": trailer_receipt(trailers.as_ref()),
                    "trailers_sha256": trailers.as_ref().map(header_hash), "ack": response});
                receipt["outcome"] = json!(if fault == ScopedAckFault::ResponseLoss {
                    "response_lost"
                } else {
                    "forwarded"
                });
            }
            if fault == ScopedAckFault::ResponseLoss {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "injected scoped ACK response loss after backend trailers",
                ));
            }
            Ok(Response::from_parts(parts, restored_body(bytes, trailers)))
        })
        .await
        .unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "scoped ACK retention expired")));
        {
            let mut receipt = worker_packet.receipt.lock().expect("scoped ACK receipt");
            if result.is_err() && receipt["outcome"] == "held" {
                receipt["outcome"] = json!("transport_failed_or_expired");
            }
            receipt["completed"] = json!(true);
            receipt["completed_at_ms"] = json!(millis(&worker_state));
        }
        let _ = reply.send(result);
        worker_state.changed.notify_one();
    });
    *packet.task.lock().expect("scoped ACK task") = Some(task.abort_handle());
    if state.closed.load(Ordering::SeqCst) {
        task.abort();
    }
    drop(task);
    state.changed.notify_one();
    response
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::ConnectionAborted, "scoped ACK queue ended"))?
        .map(Interception::Handled)
}
