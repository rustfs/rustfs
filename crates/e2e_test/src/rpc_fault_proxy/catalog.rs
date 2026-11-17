// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Exact Catalog fault selection and bounded ownership of original RPC packets.

use super::{Disposition, ProxyBody, ProxyClient, RpcFaultProxy, forward_to_backend, unary_payload};
use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;
use http_body_util::{BodyExt as _, BodyStream, Full, Limited, StreamBody};
use hyper::{Request, Response};
use prost::Message as _;
use rustfs_lock::{LockId, LockRequest, ObjectKey};
use rustfs_protos::proto_gen::node_service::{
    BatchGenerallyLockRequest, GenerallyLockRequest, GenerallyLockResponse, RenameDataRequest, RenameDataResponse,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, watch};

const MAX_FRAME: usize = 1024 * 1024;
const MAX_PACKETS: usize = 256;
const HOLD_LIMIT: Duration = Duration::from_secs(120);

#[derive(Clone, Debug)]
pub(crate) struct CatalogRenameScope {
    pub dst_volume: String,
    pub dst_path: String,
    /// None selects the first `limit` attempts at this exact destination.
    pub src_path: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CatalogLockObservation {
    pub lock_id: LockId,
    pub resource: ObjectKey,
    pub owner: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CatalogRpcResponse {
    pub http_status: u16,
    pub grpc_status: Option<String>,
    pub body: Vec<u8>,
    pub body_sha256: String,
    pub success: Option<bool>,
    pub error: Option<String>,
    pub decode_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CatalogRpcPacket {
    pub sequence: usize,
    pub method: String,
    /// Complete bounded gRPC frame, excluding HTTP headers and authentication metadata.
    pub body_base64: String,
    pub body_sha256: String,
    pub request_id: Option<String>,
    pub locks: Vec<CatalogLockObservation>,
    pub disk: Option<String>,
    pub src_volume: Option<String>,
    pub src_path: Option<String>,
    pub dst_volume: Option<String>,
    pub dst_path: Option<String>,
    pub blocked: bool,
    pub forwarded: bool,
    pub discarded: bool,
    pub settle_sequence: Option<usize>,
    pub response: Option<CatalogRpcResponse>,
    pub transport_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CatalogRpcEvidence {
    pub hits: usize,
    pub blocked: usize,
    pub forwarded: usize,
    pub discarded: usize,
    pub responses: usize,
    pub refused: usize,
    pub overflow: usize,
    pub packets: Vec<CatalogRpcPacket>,
}

#[derive(Clone)]
enum Selection {
    Rename { scope: CatalogRenameScope, limit: usize },
    Refresh(Vec<LockId>),
    Observe(Vec<ObjectKey>),
}

#[derive(Default)]
struct GateData {
    packets: Vec<CatalogRpcPacket>,
    decisions: Vec<watch::Sender<Disposition>>,
    next_settle: usize,
    overflow: usize,
    closed: bool,
}

struct GateState {
    selection: Selection,
    data: Mutex<GateData>,
    changed: Notify,
}

#[derive(Default)]
struct Slots {
    rename: Option<Arc<GateState>>,
    refresh: Option<Arc<GateState>>,
    observe: Option<Arc<GateState>>,
}

#[derive(Default)]
pub(super) struct CatalogRules {
    // Clone a selected state and release this mutex before locking GateData.
    // No request, settlement, or drop holds either mutex across an await.
    slots: Mutex<Slots>,
}

pub(crate) struct CatalogRpcGate {
    state: Arc<GateState>,
    rules: Arc<CatalogRules>,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

impl CatalogRules {
    fn arm(self: &Arc<Self>, selection: Selection) -> io::Result<CatalogRpcGate> {
        let mut slots = self.slots.lock().expect("catalog proxy slots");
        let slot = match &selection {
            Selection::Rename { .. } => &mut slots.rename,
            Selection::Refresh(_) => &mut slots.refresh,
            Selection::Observe(_) => &mut slots.observe,
        };
        if slot.is_some() {
            return Err(invalid("catalog RPC rule already armed"));
        }
        let state = Arc::new(GateState {
            selection,
            data: Mutex::default(),
            changed: Notify::new(),
        });
        *slot = Some(state.clone());
        Ok(CatalogRpcGate {
            state,
            rules: self.clone(),
        })
    }

    fn select(&self, method: &str) -> Option<Arc<GateState>> {
        let slots = self.slots.lock().expect("catalog proxy slots");
        match method {
            "RenameData" | "RenameDataAtIncarnation" | "RenameDataWithQuotaFence" => slots.rename.clone(),
            "Refresh" => slots.refresh.clone(),
            "Lock" | "LockBatch" => slots.observe.clone(),
            _ => None,
        }
    }
}

impl RpcFaultProxy {
    pub(crate) fn pause_catalog_rename(&self, scope: CatalogRenameScope, limit: usize) -> io::Result<CatalogRpcGate> {
        if scope.dst_volume != ".rustfs.sys"
            || scope.dst_path.is_empty()
            || scope.dst_path.len() > 4096
            || scope
                .src_path
                .as_ref()
                .is_some_and(|source| source.is_empty() || source.len() > 4096)
            || !(1..=64).contains(&limit)
        {
            return Err(invalid("catalog rename requires an exact system object and 1..=64 packets"));
        }
        self.catalog.arm(Selection::Rename { scope, limit })
    }

    /// Refresh carries its original lock ID and resource; its owner is empty.
    pub(crate) fn pause_catalog_refresh(&self, lock_ids: Vec<LockId>) -> io::Result<CatalogRpcGate> {
        if lock_ids.is_empty()
            || lock_ids.len() > 64
            || lock_ids.iter().any(|id| id.uuid.is_empty() || id.resource.bucket.is_empty())
        {
            return Err(invalid("catalog refresh requires 1..=64 exact lock IDs"));
        }
        self.catalog.arm(Selection::Refresh(lock_ids))
    }

    /// Observe matching members while forwarding each original batch intact.
    pub(crate) fn observe_catalog_locks(&self, resources: Vec<ObjectKey>) -> io::Result<CatalogRpcGate> {
        if resources.is_empty() || resources.len() > 64 || resources.iter().any(|resource| resource.bucket.is_empty()) {
            return Err(invalid("catalog lock observation requires 1..=64 exact resources"));
        }
        self.catalog.arm(Selection::Observe(resources))
    }
}

impl CatalogRpcGate {
    /// Snapshot counters are derived under one mutex from the same packet records.
    pub(crate) fn snapshot(&self) -> CatalogRpcEvidence {
        let data = self.state.data.lock().expect("catalog packet evidence");
        CatalogRpcEvidence {
            hits: data.packets.len() + data.overflow,
            blocked: data.packets.iter().filter(|packet| packet.blocked).count(),
            forwarded: data.packets.iter().filter(|packet| packet.forwarded).count(),
            discarded: data.packets.iter().filter(|packet| packet.discarded).count(),
            responses: data.packets.iter().filter(|packet| packet.response.is_some()).count(),
            refused: data
                .packets
                .iter()
                .filter(|packet| packet.response.as_ref().is_some_and(|reply| reply.success == Some(false)))
                .count(),
            overflow: data.overflow,
            packets: data.packets.clone(),
        }
    }

    pub(crate) async fn wait_for_hits(&self, count: usize) -> io::Result<()> {
        tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                let changed = self.state.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.snapshot().hits >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "catalog RPC fault was not reached"))
    }

    pub(crate) fn release(&self, sequence: usize) -> io::Result<()> {
        self.state.settle(sequence, Disposition::Forward)
    }
    pub(crate) fn discard(&self, sequence: usize) -> io::Result<()> {
        self.state.settle(sequence, Disposition::Discard)
    }
    pub(crate) fn release_all(&self) -> io::Result<()> {
        self.state.settle_all(Disposition::Forward)
    }
    pub(crate) fn discard_all(&self) -> io::Result<()> {
        self.state.settle_all(Disposition::Discard)
    }
}

impl GateState {
    fn settle(&self, sequence: usize, disposition: Disposition) -> io::Result<()> {
        let mut data = self.data.lock().expect("catalog packet decisions");
        let Some(packet) = data.packets.get(sequence) else {
            return Err(invalid("unknown catalog packet sequence"));
        };
        if !packet.blocked {
            return Err(invalid("catalog packet already settled"));
        }
        let settle_sequence = data.next_settle;
        data.next_settle += 1;
        let packet = &mut data.packets[sequence];
        packet.blocked = false;
        packet.discarded = disposition == Disposition::Discard;
        packet.settle_sequence = Some(settle_sequence);
        data.decisions[sequence].send_replace(disposition);
        Ok(())
    }

    fn settle_all(&self, disposition: Disposition) -> io::Result<()> {
        if matches!(self.selection, Selection::Observe(_)) {
            return Err(invalid("a passive observer has no held packets"));
        }
        let mut data = self.data.lock().expect("catalog pending packets");
        for sequence in 0..data.packets.len() {
            if !data.packets[sequence].blocked {
                continue;
            }
            let settle_sequence = data.next_settle;
            data.next_settle += 1;
            let packet = &mut data.packets[sequence];
            packet.blocked = false;
            packet.discarded = disposition == Disposition::Discard;
            packet.settle_sequence = Some(settle_sequence);
            data.decisions[sequence].send_replace(disposition);
        }
        Ok(())
    }
}

impl Drop for CatalogRpcGate {
    fn drop(&mut self) {
        self.state.data.lock().expect("catalog close gate").closed = true;
        let _ = self.state.settle_all(Disposition::Discard);
        let mut slots = self.rules.slots.lock().expect("catalog proxy slots");
        let slot = match self.state.selection {
            Selection::Rename { .. } => &mut slots.rename,
            Selection::Refresh(_) => &mut slots.refresh,
            Selection::Observe(_) => &mut slots.observe,
        };
        if slot.as_ref().is_some_and(|active| Arc::ptr_eq(active, &self.state)) {
            *slot = None;
        }
    }
}

pub(super) enum Interception {
    Pass(Request<ProxyBody>),
    Handled(Response<ProxyBody>),
}

fn selected_packet(state: &GateState, method: &str, body: &[u8]) -> Option<CatalogRpcPacket> {
    let mut packet = CatalogRpcPacket {
        sequence: 0,
        method: method.into(),
        body_base64: String::new(),
        body_sha256: hex_simd::encode_to_string(Sha256::digest(body), hex_simd::AsciiCase::Lower),
        request_id: None,
        locks: Vec::new(),
        disk: None,
        src_volume: None,
        src_path: None,
        dst_volume: None,
        dst_path: None,
        blocked: !matches!(state.selection, Selection::Observe(_)),
        forwarded: false,
        discarded: false,
        settle_sequence: None,
        response: None,
        transport_error: None,
    };
    let payload = unary_payload(body).ok()?;
    match &state.selection {
        Selection::Rename { scope, .. } => {
            let request = RenameDataRequest::decode(payload).ok()?;
            if request.src_volume != ".rustfs.sys/tmp"
                || request.dst_volume != scope.dst_volume
                || request.dst_path != scope.dst_path
                || scope.src_path.as_ref().is_some_and(|source| source != &request.src_path)
            {
                return None;
            }
            packet.disk = Some(request.disk);
            packet.src_volume = Some(request.src_volume);
            packet.src_path = Some(request.src_path);
            packet.dst_volume = Some(request.dst_volume);
            packet.dst_path = Some(request.dst_path);
        }
        Selection::Refresh(ids) => {
            let wire = GenerallyLockRequest::decode(payload).ok()?;
            let request: LockRequest = serde_json::from_str(&wire.args).ok()?;
            if request.resource != request.lock_id.resource || !ids.contains(&request.lock_id) {
                return None;
            }
            packet.locks.push(CatalogLockObservation {
                lock_id: request.lock_id,
                resource: request.resource,
                owner: request.owner,
            });
        }
        Selection::Observe(resources) => {
            let requests = if method == "LockBatch" {
                BatchGenerallyLockRequest::decode(payload).ok()?.args
            } else {
                vec![GenerallyLockRequest::decode(payload).ok()?.args]
            };
            for args in requests {
                let request: LockRequest = serde_json::from_str(&args).ok()?;
                if request.resource == request.lock_id.resource && resources.contains(&request.resource) {
                    packet.locks.push(CatalogLockObservation {
                        lock_id: request.lock_id,
                        resource: request.resource,
                        owner: request.owner,
                    });
                }
            }
            if packet.locks.is_empty() {
                return None;
            }
        }
    }
    packet.body_base64 = base64_simd::STANDARD.encode_to_string(body);
    Some(packet)
}

fn restored_request(parts: http::request::Parts, body: Bytes, trailers: Option<http::HeaderMap>) -> Request<ProxyBody> {
    let body = Full::new(body)
        .map_err(|never| match never {})
        .with_trailers(async move { trailers.map(Ok::<_, io::Error>) })
        .boxed();
    Request::from_parts(parts, body)
}

enum InspectionBody {
    Bounded(Bytes, Option<http::HeaderMap>),
    Streaming(ProxyBody),
}

async fn inspect_body(mut body: ProxyBody) -> io::Result<InspectionBody> {
    let mut frames = Vec::new();
    let mut length = 0_usize;
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        length = length.saturating_add(frame.data_ref().map_or(0, Bytes::len));
        frames.push(frame);
        if length > MAX_FRAME {
            // A large unrelated mutation keeps its original frames and cancellation lifetime.
            let stream = futures::stream::iter(frames.into_iter().map(Ok::<_, io::Error>)).chain(BodyStream::new(body));
            return Ok(InspectionBody::Streaming(http_body_util::BodyExt::boxed(StreamBody::new(stream))));
        }
    }
    let mut bytes = BytesMut::with_capacity(length);
    let mut trailers = None;
    for frame in frames {
        match frame.into_data() {
            Ok(data) => bytes.extend_from_slice(&data),
            Err(frame) => {
                if let Ok(values) = frame.into_trailers() {
                    trailers = Some(values);
                }
            }
        }
    }
    Ok(InspectionBody::Bounded(bytes.freeze(), trailers))
}

pub(super) async fn intercept(
    request: Request<ProxyBody>,
    client: ProxyClient,
    target: SocketAddr,
    rules: Arc<CatalogRules>,
    late: super::LateRequests,
) -> io::Result<Interception> {
    let Some(method) = request
        .uri()
        .path()
        .strip_prefix("/node_service.NodeService/")
        .map(str::to_owned)
    else {
        return Ok(Interception::Pass(request));
    };
    let Some(state) = rules.select(&method) else {
        return Ok(Interception::Pass(request));
    };
    let (parts, body) = request.into_parts();
    let (bytes, trailers) = match inspect_body(body).await? {
        InspectionBody::Bounded(bytes, trailers) => (bytes, trailers),
        InspectionBody::Streaming(body) => return Ok(Interception::Pass(Request::from_parts(parts, body))),
    };
    let Some(mut packet) = selected_packet(&state, &method, &bytes) else {
        return Ok(Interception::Pass(restored_request(parts, bytes, trailers)));
    };
    packet.request_id = parts
        .headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let (sequence, decision, retain) = {
        let mut data = state.data.lock().expect("catalog packet registration");
        if data.closed {
            return Ok(Interception::Pass(restored_request(parts, bytes, trailers)));
        }
        if let Selection::Rename { limit, .. } = state.selection
            && data.packets.len() >= limit
        {
            return Ok(Interception::Pass(restored_request(parts, bytes, trailers)));
        }
        if data.packets.len() >= MAX_PACKETS {
            data.overflow += 1;
            state.changed.notify_waiters();
            if matches!(state.selection, Selection::Observe(_)) {
                return Ok(Interception::Pass(restored_request(parts, bytes, trailers)));
            }
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "catalog fault packet evidence capacity exceeded",
            ));
        }
        let sequence = data.packets.len();
        packet.sequence = sequence;
        let retain = packet.blocked;
        let (sender, receiver) = watch::channel(if retain { Disposition::Hold } else { Disposition::Forward });
        data.packets.push(packet);
        data.decisions.push(sender);
        (sequence, receiver, retain)
    };
    state.changed.notify_waiters();
    let request = restored_request(parts, bytes, trailers);
    let completion = PacketCompletion {
        state: state.clone(),
        sequence,
        complete: false,
    };
    let operation = process_packet(request, client, target, state, sequence, decision, completion);
    if !retain {
        return operation.await.map(Interception::Handled);
    }
    // Only an exactly selected packet changes cancellation ownership.
    let task = tokio::spawn(operation);
    {
        let mut requests = late.lock().expect("catalog retained RPC ownership");
        if let Some(requests) = requests.as_mut() {
            requests.retain(|request| !request.is_finished());
            requests.push(task.abort_handle());
        } else {
            task.abort();
        }
    }
    task.await.map_err(io::Error::other)?.map(Interception::Handled)
}

struct PacketCompletion {
    state: Arc<GateState>,
    sequence: usize,
    complete: bool,
}

impl Drop for PacketCompletion {
    fn drop(&mut self) {
        if !self.complete {
            let mut data = self.state.data.lock().expect("catalog cancelled RPC evidence");
            let packet = &mut data.packets[self.sequence];
            packet.blocked = false;
            packet
                .transport_error
                .get_or_insert_with(|| "proxy task cancelled before completion".into());
        }
    }
}

async fn process_packet(
    request: Request<ProxyBody>,
    client: ProxyClient,
    target: SocketAddr,
    state: Arc<GateState>,
    sequence: usize,
    mut decision: watch::Receiver<Disposition>,
    mut completion: PacketCompletion,
) -> io::Result<Response<ProxyBody>> {
    let wait = async {
        loop {
            let disposition = *decision.borrow_and_update();
            if disposition != Disposition::Hold {
                return disposition;
            }
            if decision.changed().await.is_err() {
                return Disposition::Discard;
            }
        }
    };
    let disposition = match tokio::time::timeout(HOLD_LIMIT, wait).await {
        Ok(disposition) => disposition,
        Err(_) => {
            let _ = state.settle(sequence, Disposition::Discard);
            state.data.lock().expect("catalog expired RPC evidence").packets[sequence].transport_error =
                Some("catalog packet hold expired".into());
            Disposition::Discard
        }
    };
    if disposition == Disposition::Discard {
        completion.complete = true;
        return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "injected Catalog RPC packet loss"));
    }
    state.data.lock().expect("catalog dispatch evidence").packets[sequence].forwarded = true;
    let result = async {
        let response = forward_to_backend(request, client, target).await?;
        let (parts, body) = response.into_parts();
        let collected = Limited::new(body, MAX_FRAME).collect().await.map_err(io::Error::other)?;
        let trailers = collected.trailers().cloned();
        let bytes = collected.to_bytes();
        let grpc_status = parts
            .headers
            .get("grpc-status")
            .or_else(|| trailers.as_ref()?.get("grpc-status"))
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut evidence = CatalogRpcResponse {
            http_status: parts.status.as_u16(),
            grpc_status,
            body: bytes.to_vec(),
            body_sha256: hex_simd::encode_to_string(Sha256::digest(&bytes), hex_simd::AsciiCase::Lower),
            success: None,
            error: None,
            decode_error: None,
        };
        if evidence.http_status == 200 && evidence.grpc_status.as_deref() == Some("0") {
            let decoded = match &state.selection {
                Selection::Rename { .. } => unary_payload(&bytes).and_then(|payload| {
                    RenameDataResponse::decode(payload)
                        .map(|reply| (reply.success, reply.error.map(|error| format!("{}: {}", error.code, error.error_info))))
                        .map_err(io::Error::other)
                }),
                Selection::Refresh(_) => unary_payload(&bytes).and_then(|payload| {
                    GenerallyLockResponse::decode(payload)
                        .map(|reply| (reply.success, reply.error_info))
                        .map_err(io::Error::other)
                }),
                Selection::Observe(_) => {
                    // LockBatch uses a different response; the original bytes are retained.
                    state.data.lock().expect("catalog response evidence").packets[sequence].response = Some(evidence);
                    let body = Full::new(bytes)
                        .map_err(|never| match never {})
                        .with_trailers(async move { trailers.map(Ok::<_, io::Error>) })
                        .boxed();
                    return Ok(Response::from_parts(parts, body));
                }
            };
            match decoded {
                Ok((success, error)) => {
                    evidence.success = Some(success);
                    evidence.error = error;
                }
                Err(error) => evidence.decode_error = Some(error.to_string()),
            }
        }
        state.data.lock().expect("catalog response evidence").packets[sequence].response = Some(evidence);
        let body = Full::new(bytes)
            .map_err(|never| match never {})
            .with_trailers(async move { trailers.map(Ok::<_, io::Error>) })
            .boxed();
        Ok(Response::from_parts(parts, body))
    };
    let result = tokio::time::timeout(Duration::from_secs(60), result)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "catalog backend response timed out"))
        .and_then(|result| result);
    if let Err(error) = &result {
        state.data.lock().expect("catalog transport evidence").packets[sequence].transport_error = Some(error.to_string());
    }
    completion.complete = true;
    result
}
