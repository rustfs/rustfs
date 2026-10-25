// Copyright 2026 RustFS Team
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

//! Method-selective faults without changing the server or gRPC body/trailers.

use crate::storage_api::multipart_source_rpc::{
    MULTIPART_SOURCE_PROTOCOL_VERSION, MultipartSourceOperation, MultipartSourceRequest as NativeSourceRequest,
};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full, Limited, combinators::BoxBody};
use hyper::{Request, body::Incoming, service::service_fn};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use prost::Message as _;
use rustfs_protos::proto_gen::node_service::{
    DeleteRequest, ReadVersionRequest, RecoverMultipartSourceRequest, RenameDataResponse,
};
use std::{
    collections::HashSet,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, watch},
    task::{JoinHandle, JoinSet},
};

type ProxyBody = BoxBody<Bytes, io::Error>;
type ProxyClient = Client<HttpConnector, ProxyBody>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Disposition {
    Hold,
    Forward,
    Discard,
}

struct PauseState {
    methods: Vec<&'static str>,
    scope: Option<Vec<u8>>,
    inspection_path: Option<Vec<u8>>,
    unsupported_inspection_routes: Mutex<HashSet<String>>,
    limit: usize,
    retain_after_disconnect: bool,
    refused_mutations: AtomicUsize,
    responses: AtomicUsize,
    matched: AtomicUsize,
    blocked: AtomicUsize,
    forwarded: AtomicUsize,
    forwarded_routes: Mutex<HashSet<String>>,
    reached: Notify,
    release: watch::Sender<Disposition>,
}

type PauseSlot = Arc<Mutex<Option<Arc<PauseState>>>>;
type LateRequests = Arc<Mutex<Option<Vec<tokio::task::AbortHandle>>>>;

pub(crate) struct RpcPause {
    state: Arc<PauseState>,
    slot: PauseSlot,
}

impl RpcPause {
    pub(crate) fn hits(&self) -> usize {
        self.state.blocked.load(Ordering::SeqCst)
    }

    /// Selected RPCs with successful HTTP/gRPC headers, excluding discarded packets.
    pub(crate) fn forwarded(&self) -> usize {
        self.state.forwarded.load(Ordering::SeqCst)
    }

    pub(crate) fn forwarded_routes(&self) -> usize {
        self.state.forwarded_routes.lock().expect("RPC route observations").len()
    }

    pub(crate) fn responses(&self) -> usize {
        self.state.responses.load(Ordering::SeqCst)
    }

    pub(crate) fn refused_mutations(&self) -> usize {
        self.state.refused_mutations.load(Ordering::SeqCst)
    }

    pub(crate) fn unsupported_source_inspection_routes(&self) -> HashSet<String> {
        self.state
            .unsupported_inspection_routes
            .lock()
            .expect("inspection response routes")
            .clone()
    }

    pub(crate) async fn wait_for_hits(&self, count: usize) -> io::Result<()> {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let notified = self.state.reached.notified();
                if self.hits() >= count {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "the selected RPC fault was not reached"))
    }

    pub(crate) fn release(&self) {
        self.settle(Disposition::Forward);
    }

    /// Retire packets that outlived the caller's operation budget. A later
    /// healthy request must not replay an expired lock grant or mutation.
    pub(crate) fn discard_pending(&self) {
        self.settle(Disposition::Discard);
    }

    fn settle(&self, disposition: Disposition) {
        self.state.release.send_if_modified(|current| {
            if *current == Disposition::Hold {
                *current = disposition;
                true
            } else {
                false
            }
        });
    }
}

impl Drop for RpcPause {
    fn drop(&mut self) {
        if self.state.retain_after_disconnect {
            self.discard_pending();
        } else {
            self.release();
        }
        let mut slot = self.slot.lock().expect("RPC pause slot");
        if slot.as_ref().is_some_and(|current| Arc::ptr_eq(current, &self.state)) {
            *slot = None;
        }
    }
}

pub(crate) struct RpcFaultProxy {
    address: SocketAddr,
    slot: PauseSlot,
    task: JoinHandle<()>,
    late_requests: LateRequests,
}

impl RpcFaultProxy {
    pub(crate) async fn start_on(address: SocketAddr, target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let slot: PauseSlot = Arc::new(Mutex::new(None));
        let active = slot.clone();
        let late_requests: LateRequests = Arc::new(Mutex::new(Some(Vec::new())));
        let late = late_requests.clone();
        let task = tokio::spawn(async move {
            let h1: ProxyClient = Client::builder(TokioExecutor::new()).build_http();
            let h2: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { break };
                        let h1 = h1.clone();
                        let h2 = h2.clone();
                        let active = active.clone();
                        let late = late.clone();
                        connections.spawn(async move {
                            let service = service_fn(move |request: Request<Incoming>| {
                                let client = if request.version() == http::Version::HTTP_2 { h2.clone() } else { h1.clone() };
                                let pause = active.lock().expect("RPC pause slot").clone();
                                let retain = pause.as_ref().is_some_and(|pause| {
                                    pause.retain_after_disconnect
                                        && request.uri().path().strip_prefix("/node_service.NodeService/")
                                            .is_some_and(|method| pause.methods.contains(&method))
                                });
                                let late = late.clone();
                                async move {
                                    if !retain {
                                        return forward_request(request, client, target, pause).await;
                                    }
                                    // This opt-in fault owns the packet after client cancellation.
                                    // Proxy teardown still aborts every retained request.
                                    let task = tokio::spawn(forward_request(request, client, target, pause));
                                    {
                                        let mut requests = late.lock().expect("late RPC ownership");
                                        if let Some(requests) = requests.as_mut() {
                                            requests.retain(|request| !request.is_finished());
                                            requests.push(task.abort_handle());
                                        } else {
                                            task.abort();
                                        }
                                    }
                                    task.await.map_err(io::Error::other)?
                                }
                            });
                            let _ = auto::Builder::new(TokioExecutor::new())
                                .serve_connection(TokioIo::new(socket), service).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Ok(Self {
            address,
            slot,
            task,
            late_requests,
        })
    }

    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// Pause at most limit requests to the named unary methods; other streams
    /// on the same HTTP/2 connection continue forwarding.
    pub(crate) fn pause(&self, methods: &[&'static str], limit: usize, scope: Option<&str>) -> io::Result<RpcPause> {
        self.pause_with_lifetime(methods, limit, scope, false, None)
    }

    /// Retain bounded control packets until explicit delivery after caller retirement.
    pub(crate) fn pause_late(&self, methods: &[&'static str], limit: usize, scope: &str) -> io::Result<RpcPause> {
        if limit > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "late RPC limit exceeds control-packet budget",
            ));
        }
        self.pause_with_lifetime(methods, limit, Some(scope), true, None)
    }

    /// Observe exact native Inspect rejections without consuming mutation hits
    /// or holding preparation. The signed request and response remain opaque.
    pub(crate) fn observe_source_inspections(
        &self,
        mutation_methods: &[&'static str],
        mutation_scope: &str,
        path: &str,
    ) -> io::Result<RpcPause> {
        let mut methods = mutation_methods.to_vec();
        if !methods.contains(&"RecoverMultipartSource") {
            methods.push("RecoverMultipartSource");
        }
        let observer = self.pause_with_lifetime(&methods, usize::MAX, Some(mutation_scope), false, Some(path))?;
        observer.release();
        Ok(observer)
    }

    fn pause_with_lifetime(
        &self,
        methods: &[&'static str],
        limit: usize,
        scope: Option<&str>,
        retain_after_disconnect: bool,
        inspection_path: Option<&str>,
    ) -> io::Result<RpcPause> {
        let mut slot = self.slot.lock().expect("RPC pause slot");
        if slot.is_some()
            || methods.is_empty()
            || limit == 0
            || scope.is_some_and(str::is_empty)
            || inspection_path.is_some_and(str::is_empty)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RPC pause needs an empty slot, methods and a positive limit",
            ));
        }
        let (release, _) = watch::channel(Disposition::Hold);
        let state = Arc::new(PauseState {
            methods: methods.to_vec(),
            scope: scope.map(|scope| scope.as_bytes().to_vec()),
            inspection_path: inspection_path.map(|path| path.as_bytes().to_vec()),
            unsupported_inspection_routes: Mutex::new(HashSet::new()),
            limit,
            retain_after_disconnect,
            refused_mutations: AtomicUsize::new(0),
            responses: AtomicUsize::new(0),
            matched: AtomicUsize::new(0),
            blocked: AtomicUsize::new(0),
            forwarded: AtomicUsize::new(0),
            forwarded_routes: Mutex::new(HashSet::new()),
            reached: Notify::new(),
            release,
        });
        *slot = Some(state.clone());
        Ok(RpcPause {
            state,
            slot: self.slot.clone(),
        })
    }
}

fn unary_payload(body: &[u8]) -> io::Result<&[u8]> {
    if body.len() < 5 || body[0] != 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "scoped RPC needs an uncompressed unary frame"));
    }
    let mut encoded_length = [0; 4];
    encoded_length.copy_from_slice(&body[1..5]);
    if u32::try_from(body.len() - 5).ok() != Some(u32::from_be_bytes(encoded_length)) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "scoped RPC frame length mismatch"));
    }
    Ok(&body[5..])
}

fn scope_matches(method: &str, body: &[u8], scope: &[u8]) -> io::Result<bool> {
    if method == "RecoverMultipartSource" {
        let request = RecoverMultipartSourceRequest::decode(unary_payload(body)?).map_err(io::Error::other)?;
        if request.protocol_version != u32::from(MULTIPART_SOURCE_PROTOCOL_VERSION) || request.path.as_bytes() != scope {
            return Ok(false);
        }
        let operation = rustfs_protos::decode_multipart_source_recovery_payload(&request.operation)?;
        let request = NativeSourceRequest {
            protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
            path: request.path,
            operation,
        };
        let MultipartSourceOperation::DeleteAborted { expected, certificate } = &request.operation else {
            return Ok(false);
        };
        if expected.lineage != certificate.round.lineage {
            return Ok(false);
        }
        request.validate().map_err(io::Error::other)?;
        // Authentication remains opaque; only storage can authorize deletion.
        // The proxy must not pause preparation or another upload's cleanup.
        return Ok(true);
    }
    if method != "Delete" {
        return Ok(body.windows(scope.len()).any(|window| window == scope));
    }
    let request = DeleteRequest::decode(unary_payload(body)?).map_err(io::Error::other)?;
    let options: serde_json::Value = serde_json::from_str(&request.options).map_err(io::Error::other)?;
    Ok(request.volume == ".rustfs.sys/multipart"
        && request.path.trim_end_matches('/').as_bytes() == scope
        && options.get("recursive").and_then(serde_json::Value::as_bool) == Some(true))
}

fn source_inspection_route(body: &[u8], path: &[u8]) -> io::Result<Option<String>> {
    let request = RecoverMultipartSourceRequest::decode(unary_payload(body)?).map_err(io::Error::other)?;
    if request.protocol_version != u32::from(MULTIPART_SOURCE_PROTOCOL_VERSION) || request.path.as_bytes() != path {
        return Ok(None);
    }
    let operation = rustfs_protos::decode_multipart_source_recovery_payload(&request.operation)?;
    if !matches!(operation, MultipartSourceOperation::Inspect { .. }) {
        return Ok(None);
    }
    NativeSourceRequest {
        protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
        path: request.path,
        operation,
    }
    .validate()
    .map_err(io::Error::other)?;
    Ok((!request.disk.is_empty()).then_some(request.disk))
}

async fn forward_request(
    request: Request<Incoming>,
    client: ProxyClient,
    target: SocketAddr,
    pause: Option<Arc<PauseState>>,
) -> io::Result<hyper::Response<ProxyBody>> {
    let (mut parts, body) = request.into_parts();
    let mut body = body.map_err(io::Error::other).boxed();
    let mut observed = None;
    let mut inspection = None;
    if let Some(pause) = pause {
        let method = parts.uri.path().strip_prefix("/node_service.NodeService/");
        if let Some(method) = method.filter(|name| pause.methods.contains(name)) {
            let mut route = None;
            let matches = if let Some(scope) = pause.scope.as_deref() {
                // Inspect bounded control routing while preserving the exact
                // signed bytes and trailers forwarded to the backend.
                let collected = Limited::new(body, 1024 * 1024).collect().await.map_err(io::Error::other)?;
                let trailers = collected.trailers().cloned();
                let bytes = collected.to_bytes();
                if method == "RecoverMultipartSource"
                    && let Some(path) = pause.inspection_path.as_deref()
                    && let Some(disk) = source_inspection_route(&bytes, path)?
                {
                    inspection = Some((pause.clone(), disk));
                }
                let matches = scope_matches(method, &bytes, scope)?;
                if matches && method == "ReadVersion" {
                    let disk = ReadVersionRequest::decode(unary_payload(&bytes)?)
                        .map_err(io::Error::other)?
                        .disk;
                    if !disk.is_empty() {
                        route = Some(disk);
                    }
                }
                body = Full::new(bytes)
                    .map_err(|never| match never {})
                    .with_trailers(async move { trailers.map(Ok::<_, io::Error>) })
                    .boxed();
                matches
            } else {
                true
            };
            if matches && pause.matched.fetch_add(1, Ordering::SeqCst) < pause.limit {
                let mut released = pause.release.subscribe();
                pause.blocked.fetch_add(1, Ordering::SeqCst);
                pause.reached.notify_one();
                let disposition = loop {
                    let disposition = *released.borrow_and_update();
                    if disposition != Disposition::Hold {
                        break disposition;
                    }
                    if released.changed().await.is_err() {
                        break Disposition::Discard;
                    }
                };
                if disposition == Disposition::Discard {
                    return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "injected RPC packet loss"));
                }
                observed = Some((
                    pause,
                    route,
                    matches!(
                        method,
                        "RenameData" | "RenameMultipartData" | "RenameDataAtIncarnation" | "RenameDataWithQuotaFence"
                    ),
                ));
            }
        }
    }
    if !parts.headers.contains_key(http::header::HOST)
        && let Some(authority) = parts.uri.authority()
    {
        parts.headers.insert(
            http::header::HOST,
            http::HeaderValue::from_str(authority.as_str()).map_err(io::Error::other)?,
        );
    }
    parts.uri = http::Uri::builder()
        .scheme("http")
        .authority(target.to_string())
        .path_and_query(parts.uri.path_and_query().map_or("/", |value| value.as_str()))
        .build()
        .map_err(io::Error::other)?;
    let response = client
        .request(Request::from_parts(parts, body))
        .await
        .map_err(io::Error::other)?;
    if let Some((pause, disk)) = inspection
        && response.status().is_success()
        && response.headers().get("grpc-status").is_some_and(|status| status == "12")
    {
        pause
            .unsupported_inspection_routes
            .lock()
            .expect("inspection response routes")
            .insert(disk);
    }
    if let Some((pause, route, rename)) = observed {
        pause.responses.fetch_add(1, Ordering::SeqCst);
        if response.status().is_success() && response.headers().get("grpc-status").is_none_or(|status| status == "0") {
            pause.forwarded.fetch_add(1, Ordering::SeqCst);
            if let Some(route) = route {
                pause.forwarded_routes.lock().expect("RPC route observations").insert(route);
            }
        }
        if pause.retain_after_disconnect {
            let (parts, body) = response.into_parts();
            let collected = Limited::new(body, 1024 * 1024).collect().await.map_err(io::Error::other)?;
            let trailers = collected.trailers().cloned();
            let bytes = collected.to_bytes();
            let status = parts
                .headers
                .get("grpc-status")
                .or_else(|| trailers.as_ref()?.get("grpc-status"));
            if rename && parts.status.is_success() && status.is_none_or(|status| status == "0") {
                let result = RenameDataResponse::decode(unary_payload(&bytes)?).map_err(io::Error::other)?;
                if !result.success && result.error.is_some() {
                    pause.refused_mutations.fetch_add(1, Ordering::SeqCst);
                }
            }
            let body = Full::new(bytes)
                .map_err(|never| match never {})
                .with_trailers(async move { trailers.map(Ok::<_, io::Error>) })
                .boxed();
            return Ok(hyper::Response::from_parts(parts, body));
        }
    }
    Ok(response.map(|body| body.map_err(io::Error::other).boxed()))
}

impl Drop for RpcFaultProxy {
    fn drop(&mut self) {
        if let Some(requests) = self.late_requests.lock().expect("late RPC ownership").take() {
            for request in requests {
                request.abort();
            }
        }
        self.task.abort();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{Full, StreamBody};
    use hyper::{Response, body::Frame};
    use std::convert::Infallible;

    struct Backend {
        received: Arc<AtomicUsize>,
        address: SocketAddr,
        task: JoinHandle<()>,
    }

    impl Drop for Backend {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn backend() -> io::Result<Backend> {
        backend_with_status(None).await
    }

    async fn backend_with_status(status: Option<&'static str>) -> io::Result<Backend> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let received = Arc::new(AtomicUsize::new(0));
        let received_task = received.clone();
        let task = tokio::spawn(async move {
            let mut children = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { break };
                        let received = received_task.clone();
                        children.spawn(async move {
                            let service = service_fn(move |request: Request<Incoming>| {
                                let received = received.clone();
                                async move {
                                received.fetch_add(1, Ordering::SeqCst);
                                let authority = request.headers().get(http::header::HOST).cloned().or_else(|| {
                                    request.uri().authority().map(|authority| {
                                        http::HeaderValue::from_str(authority.as_str()).expect("valid HTTP authority")
                                    })
                                });
                                let body = request.into_body().collect().await?.to_bytes();
                                let mut trailers = http::HeaderMap::new();
                                trailers.insert("grpc-status", http::HeaderValue::from_static(status.unwrap_or("0")));
                                trailers.insert("x-proof", http::HeaderValue::from_static("unchanged-proof"));
                                let stream = futures::stream::iter([
                                    Ok::<_, Infallible>(Frame::data(body)),
                                    Ok(Frame::trailers(trailers)),
                                ]);
                                let mut response = Response::new(StreamBody::new(stream));
                                if let Some(status) = status { response.headers_mut().insert("grpc-status", http::HeaderValue::from_static(status)); }
                                response.headers_mut().insert(http::header::TRAILER, http::HeaderValue::from_static("grpc-status, x-proof"));
                                if let Some(authority) = authority { response.headers_mut().insert("x-original-authority", authority); }
                                Ok::<_, hyper::Error>(response)
                            }});
                            let _ = auto::Builder::new(TokioExecutor::new()).serve_connection(TokioIo::new(socket), service).await;
                        });
                    }
                    _ = children.join_next(), if !children.is_empty() => {}
                }
            }
        });
        Ok(Backend { received, address, task })
    }

    async fn call(client: &ProxyClient, address: SocketAddr, method: &str) -> io::Result<hyper::Response<Incoming>> {
        call_body(client, address, method, Bytes::from_static(b"opaque signed protobuf")).await
    }

    async fn call_body(
        client: &ProxyClient,
        address: SocketAddr,
        method: &str,
        bytes: Bytes,
    ) -> io::Result<hyper::Response<Incoming>> {
        let body: ProxyBody = Full::new(bytes).map_err(|never| match never {}).boxed();
        let request = Request::builder()
            .method("POST")
            .uri(format!("http://{address}/node_service.NodeService/{method}"))
            .header(http::header::TE, "trailers")
            .body(body)
            .map_err(io::Error::other)?;
        client.request(request).await.map_err(io::Error::other)
    }

    fn unary_frame(message: &impl prost::Message) -> Bytes {
        let payload = message.encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&u32::try_from(payload.len()).expect("small test packet").to_be_bytes());
        frame.extend_from_slice(&payload);
        frame.into()
    }

    fn native_abort_request(path: &str) -> RecoverMultipartSourceRequest {
        use crate::storage_api::multipart_source_rpc::{
            MultipartAbortCertificate, MultipartAbortProof, MultipartAbortProofKind, MultipartAbortRound, MultipartSourcePhase,
            MultipartSourceSnapshot, SourceGeneration, SourceGeometry, SourceLineage,
        };
        use uuid::Uuid;

        let lineage = SourceLineage {
            bucket: "bucket".to_owned(),
            object: "object".to_owned(),
            path: path.to_owned(),
            original_data_dir: Uuid::new_v4(),
            original_version_id: None,
            created_unix_nanos: 1,
            original_identity: [1; 32],
            geometry: SourceGeometry {
                data_blocks: 2,
                parity_blocks: 2,
                block_size: 1024,
                distribution: vec![1, 2, 3, 4],
            },
        };
        let generation = SourceGeneration {
            epoch: 1,
            nonce: Uuid::new_v4(),
        };
        let disk_ids = (0..4).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
        let expected = MultipartSourceSnapshot {
            disk_id: disk_ids[0],
            physical_slot: 0,
            lineage: lineage.clone(),
            generation,
            revision: 1,
            phase: MultipartSourcePhase::Aborting,
            state_sha256: [2; 32],
            head_sha256: None,
            inventory_sha256: [3; 32],
            capability_sha256: None,
            publication: None,
        };
        let round = MultipartAbortRound {
            lineage,
            generation,
            round: Uuid::new_v4(),
        };
        let proofs = disk_ids
            .iter()
            .enumerate()
            .map(|(slot, disk_id)| {
                let mut snapshot = expected.clone();
                snapshot.disk_id = *disk_id;
                snapshot.physical_slot = slot;
                // Valid N-proof structure with opaque dummy authentication;
                // this fixture grants no native or cluster authority.
                serde_json::from_value::<MultipartAbortProof>(serde_json::json!({
                    "kind": MultipartAbortProofKind::Prepared,
                    "challenge": round.round,
                    "round": round,
                    "snapshot": snapshot,
                    "certificate_sha256": null,
                    "publication": null,
                    "authentication": { "class": "cluster_hmac", "mac": vec![7_u8; 32] },
                }))
                .expect("structural abort proof")
            })
            .collect();
        let certificate = MultipartAbortCertificate { round, disk_ids, proofs };
        RecoverMultipartSourceRequest {
            disk: "opaque-source-disk".to_owned(),
            path: path.to_owned(),
            protocol_version: u32::from(MULTIPART_SOURCE_PROTOCOL_VERSION),
            operation: rmp_serde::to_vec_named(&MultipartSourceOperation::DeleteAborted { expected, certificate })
                .expect("typed native deletion")
                .into(),
        }
    }

    #[test]
    fn native_abort_scope_matches_only_delete_aborted_at_the_exact_upload_root() {
        use crate::storage_api::multipart_source_rpc::PublicationIdentity;
        use uuid::Uuid;

        let root = b"hash/upload";
        let request = native_abort_request("hash/upload");
        assert!(scope_matches("RecoverMultipartSource", &unary_frame(&request), root).expect("native deletion"));
        for path in [
            "hash/upload/",
            "hash/upload/part.1",
            "hash/upload/.abort-authority",
            "hash/upload-other",
            "other/hash/upload",
        ] {
            assert!(
                !scope_matches("RecoverMultipartSource", &unary_frame(&native_abort_request(path)), root).expect("other scope")
            );
        }
        let MultipartSourceOperation::DeleteAborted { expected, certificate } =
            rustfs_protos::decode_multipart_source_recovery_payload(&request.operation).expect("native operation")
        else {
            panic!("native deletion fixture");
        };
        let publication = PublicationIdentity {
            lineage: expected.lineage.clone(),
            source_generation: expected.generation,
            attempt: Uuid::new_v4(),
            version_id: None,
            final_data_dir: Uuid::new_v4(),
            modified_unix_nanos: 2,
            completion_identity: [4; 32],
        };
        for operation in [
            MultipartSourceOperation::Inspect {
                lineage: Some(expected.lineage.clone()),
            },
            MultipartSourceOperation::PrepareAbort {
                expected: expected.clone(),
                round: certificate.round.clone(),
            },
            MultipartSourceOperation::CommitAbort {
                expected: expected.clone(),
                certificate: certificate.clone(),
            },
            MultipartSourceOperation::ConfirmCleanupTarget {
                expected: expected.clone(),
                publication: publication.clone(),
                challenge: Uuid::new_v4(),
            },
            MultipartSourceOperation::ConfirmTerminal {
                expected: Some(expected.clone()),
                publication,
                challenge: Uuid::new_v4(),
            },
        ] {
            let mut other = request.clone();
            other.operation = rmp_serde::to_vec_named(&operation).expect("other typed operation").into();
            assert!(!scope_matches("RecoverMultipartSource", &unary_frame(&other), root).expect("non-deleting operation"));
        }
        let mut other_certificate = certificate;
        other_certificate.round.lineage.path.push_str("/child");
        let mut other = request;
        other.operation = rmp_serde::to_vec_named(&MultipartSourceOperation::DeleteAborted {
            expected,
            certificate: other_certificate,
        })
        .expect("different certificate scope")
        .into();
        assert!(!scope_matches("RecoverMultipartSource", &unary_frame(&other), root).expect("certificate mismatch"));
    }

    #[test]
    fn native_abort_scope_rejects_unknown_protocol_and_malformed_operation_packets() {
        let root = b"hash/upload";
        let request = native_abort_request("hash/upload");
        let mut unsupported = request.clone();
        unsupported.protocol_version += 1;
        assert!(!scope_matches("RecoverMultipartSource", &unary_frame(&unsupported), root).expect("unsupported protocol"));
        let mut operation: MultipartSourceOperation =
            rustfs_protos::decode_multipart_source_recovery_payload(&request.operation).expect("typed operation fields");
        let mut unknown_scope = serde_json::to_value(&operation).expect("named operation fields");
        unknown_scope["volume"] = ".rustfs.sys/tmp".into();
        let MultipartSourceOperation::DeleteAborted { certificate, .. } = &mut operation else {
            panic!("native deletion fixture");
        };
        certificate.proofs.pop();
        for operation in [
            Vec::new(),
            vec![0xc1],
            rmp_serde::to_vec_named(&unknown_scope).expect("unknown scope field"),
            rmp_serde::to_vec_named(&operation).expect("incomplete N-proof certificate"),
            {
                let mut trailing = request.operation.to_vec();
                trailing.push(0xc0);
                trailing
            },
        ] {
            let mut malformed = request.clone();
            malformed.operation = operation.into();
            assert!(scope_matches("RecoverMultipartSource", &unary_frame(&malformed), root).is_err());
        }
        let mut compressed = unary_frame(&request).to_vec();
        compressed[0] = 1;
        assert!(scope_matches("RecoverMultipartSource", &compressed, root).is_err());
        assert!(scope_matches("RecoverMultipartSource", &[0, 0, 0, 0, 1], root).is_err());
    }

    #[test]
    fn source_inspection_observer_requires_exact_read_only_native_packet() {
        let path = b"hash/upload";
        let mut request = native_abort_request("hash/upload");
        assert!(
            source_inspection_route(&unary_frame(&request), path)
                .expect("valid mutation remains separate")
                .is_none()
        );
        request.operation = rmp_serde::to_vec_named(&MultipartSourceOperation::Inspect { lineage: None })
            .unwrap()
            .into();
        let body = unary_frame(&request);
        assert_eq!(source_inspection_route(&body, path).unwrap(), Some(request.disk.clone()));
        assert!(
            !scope_matches("RecoverMultipartSource", &body, path).unwrap(),
            "Inspect still cannot consume a DeleteAborted fault"
        );
        assert!(source_inspection_route(&body, b"hash/other").unwrap().is_none());
        request.protocol_version += 1;
        assert!(source_inspection_route(&unary_frame(&request), path).unwrap().is_none());
        request.protocol_version -= 1;
        request.operation = [request.operation.as_ref(), &[0]].concat().into();
        assert!(
            source_inspection_route(&unary_frame(&request), path).is_err(),
            "trailing operation bytes cannot prove a read-only route"
        );
    }

    #[tokio::test]
    async fn source_inspection_observer_proves_backend_rejection_without_consuming_mutation_hits() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend_with_status(Some("12")).await.expect("old unsupported endpoint");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().unwrap(), backend.address)
                .await
                .unwrap();
            let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            let observer = proxy
                .observe_source_inspections(&["RenameMultipartData"], "hash/upload", "hash/upload")
                .unwrap();
            let mut request = native_abort_request("hash/upload");
            request.operation = rmp_serde::to_vec_named(&MultipartSourceOperation::Inspect { lineage: None })
                .unwrap()
                .into();
            for slot in 0..4 {
                request.disk = format!("physical-disk-{slot}");
                let bytes = unary_frame(&request);
                let response = call_body(&client, proxy.local_addr(), "RecoverMultipartSource", bytes.clone())
                    .await
                    .unwrap();
                assert_eq!(response.headers()["grpc-status"], "12");
                let body = response.into_body().collect().await.unwrap();
                assert_eq!(body.trailers().unwrap()["x-proof"], "unchanged-proof");
                assert_eq!(body.to_bytes(), bytes, "inspection must not re-encode the opaque request");
            }
            let expected: HashSet<_> = (0..4).map(|slot| format!("physical-disk-{slot}")).collect();
            assert_eq!(observer.unsupported_source_inspection_routes(), expected);
            assert_eq!(observer.hits(), 0, "read-only rejections never count as publication mutations");
            request.path = "hash/other".to_owned();
            request.disk = "foreign-physical-route".to_owned();
            let response = call_body(&client, proxy.local_addr(), "RecoverMultipartSource", unary_frame(&request))
                .await
                .unwrap();
            response.into_body().collect().await.unwrap();
            assert_eq!(
                observer.unsupported_source_inspection_routes(),
                expected,
                "other upload responses add no route"
            );
            request.path = "hash/upload".to_owned();
            request.disk = "physical-disk-0".to_owned();
            call_body(&client, proxy.local_addr(), "RecoverMultipartSource", unary_frame(&request))
                .await
                .unwrap()
                .into_body()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                observer.unsupported_source_inspection_routes(),
                expected,
                "duplicate responses add no physical route"
            );
            request.disk = "nonconfigured-physical-route".to_owned();
            call_body(&client, proxy.local_addr(), "RecoverMultipartSource", unary_frame(&request))
                .await
                .unwrap()
                .into_body()
                .collect()
                .await
                .unwrap();
            assert_ne!(
                observer.unsupported_source_inspection_routes(),
                expected,
                "same-path foreign route evidence must fail the exact configured participant oracle"
            );
            assert!(
                observer
                    .unsupported_source_inspection_routes()
                    .contains("nonconfigured-physical-route")
            );
            call_body(&client, proxy.local_addr(), "RenameMultipartData", Bytes::from_static(b"hash/upload"))
                .await
                .unwrap()
                .into_body()
                .collect()
                .await
                .unwrap();
            assert_eq!(observer.hits(), 1, "a publication method remains independently observable");
            assert_eq!(backend.received.load(Ordering::SeqCst), 8);
        })
        .await
        .expect("read-only observation deadline");
    }

    #[tokio::test]
    async fn native_abort_scoped_pause_preserves_opaque_payload_and_grpc_trailers() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("backend listener");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback address"), backend.address)
                .await
                .expect("proxy listener");
            let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            let pause = proxy
                .pause(&["Delete", "RecoverMultipartSource"], 1, Some("hash/upload"))
                .expect("exact abort deletion barrier");
            let request = native_abort_request("hash/upload");
            let preparation = RecoverMultipartSourceRequest {
                operation: rmp_serde::to_vec_named(&MultipartSourceOperation::Inspect { lineage: None })
                    .expect("inspection operation")
                    .into(),
                ..request.clone()
            };
            let preparation = unary_frame(&preparation);
            let forwarded = call_body(&client, proxy.local_addr(), "RecoverMultipartSource", preparation.clone())
                .await
                .expect("preparation must reach storage before the deletion barrier")
                .into_body()
                .collect()
                .await
                .expect("preparation response");
            assert_eq!(forwarded.to_bytes(), preparation);
            assert_eq!(pause.hits(), 0, "preparation must not consume the deletion fault");

            let original = unary_frame(&request);
            let sent = original.clone();
            let task_client = client.clone();
            let address = proxy.local_addr();
            let blocked = tokio::spawn(async move { call_body(&task_client, address, "RecoverMultipartSource", sent).await });
            pause
                .wait_for_hits(1)
                .await
                .expect("native deletion reached its exact barrier");
            assert!(!blocked.is_finished());
            let child = unary_frame(&DeleteRequest {
                volume: ".rustfs.sys/multipart".to_owned(),
                path: "hash/upload/part.1".to_owned(),
                options: r#"{"recursive":true}"#.to_owned(),
                ..Default::default()
            });
            let forwarded = call_body(&client, address, "Delete", child.clone())
                .await
                .expect("child cleanup must progress")
                .into_body()
                .collect()
                .await
                .expect("child response");
            assert_eq!(forwarded.to_bytes(), child);
            assert_eq!(pause.hits(), 1);
            pause.release();
            let forwarded = blocked
                .await
                .expect("request task")
                .expect("released deletion")
                .into_body()
                .collect()
                .await
                .expect("native deletion body and trailers");
            assert_eq!(forwarded.trailers().expect("gRPC trailers")["grpc-status"], "0");
            assert_eq!(forwarded.trailers().expect("opaque proof trailer")["x-proof"], "unchanged-proof");
            assert_eq!(forwarded.to_bytes(), original, "routing inspection must never re-encode signed bytes");
            assert_eq!(backend.received.load(Ordering::SeqCst), 3);
        })
        .await
        .expect("native abort scope deadline");
    }

    #[tokio::test]
    async fn preserves_protocol_authority_payload_and_grpc_trailers() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("backend listener");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback address"), backend.address)
                .await
                .expect("proxy");
            for h2 in [false, true] {
                let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(h2).build_http();
                let response = call(&client, proxy.local_addr(), "ReadVersion")
                    .await
                    .expect("transparent request");
                assert_eq!(response.version(), if h2 { http::Version::HTTP_2 } else { http::Version::HTTP_11 });
                assert_eq!(response.headers()["x-original-authority"], proxy.local_addr().to_string());
                let collected = response.into_body().collect().await.expect("complete body and trailers");
                let trailers = collected.trailers().expect("gRPC trailers must survive both proxy hops");
                assert_eq!(trailers["grpc-status"], "0");
                assert_eq!(trailers["x-proof"], "unchanged-proof");
                assert_eq!(collected.to_bytes(), Bytes::from_static(b"opaque signed protobuf"));
            }
        })
        .await
        .expect("transparent proxy deadline");
    }

    #[tokio::test]
    async fn isolates_rpc_methods_limits_injection_and_releases_on_drop() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("backend listener");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback address"), backend.address)
                .await
                .expect("proxy");
            let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            let pause = proxy.pause(&["WriteMetadata"], 1, None).expect("arm one metadata request");
            let task_client = client.clone();
            let address = proxy.local_addr();
            let blocked = tokio::spawn(async move { call(&task_client, address, "WriteMetadata").await });
            pause.wait_for_hits(1).await.expect("metadata barrier reached");
            for method in ["Lock", "WriteMetadata"] {
                let response = call(&client, address, method)
                    .await
                    .expect("unselected or over-limit request must progress");
                assert_eq!(
                    response.into_body().collect().await.expect("forwarded body").to_bytes(),
                    Bytes::from_static(b"opaque signed protobuf")
                );
            }
            assert_eq!(pause.hits(), 1);
            assert!(!blocked.is_finished(), "the selected request must still be held");
            drop(pause);
            assert_eq!(
                blocked
                    .await
                    .expect("blocked request task")
                    .expect("released request")
                    .into_body()
                    .collect()
                    .await
                    .expect("released body")
                    .to_bytes(),
                Bytes::from_static(b"opaque signed protobuf")
            );
        })
        .await
        .expect("selective fault deadline");
    }
    #[tokio::test]
    async fn released_observer_requires_a_selected_backend_response() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("backend listener");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
                .await
                .expect("proxy listener");
            let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            let observer = proxy
                .pause(&["Lock"], usize::MAX, Some("recovery-bucket"))
                .expect("RPC observer");
            let unrelated = call_body(&client, proxy.local_addr(), "Lock", Bytes::from_static(b"other-bucket"))
                .await
                .expect("unrelated request");
            unrelated.into_body().collect().await.expect("unrelated body");
            assert_eq!(observer.forwarded(), 0, "unrelated traffic cannot prove recovery");
            let task_client = client.clone();
            let address = proxy.local_addr();
            let request =
                tokio::spawn(
                    async move { call_body(&task_client, address, "Lock", Bytes::from_static(b"recovery-bucket")).await },
                );
            observer.wait_for_hits(1).await.expect("selected request arrived");
            assert_eq!(observer.forwarded(), 0, "an intercepted request has not reached its backend");
            observer.release();
            let body = request
                .await
                .expect("request task")
                .expect("forwarded request")
                .into_body()
                .collect()
                .await
                .expect("forwarded body");
            assert_eq!(body.trailers().expect("preserved trailers")["x-proof"], "unchanged-proof");
            assert_eq!(body.to_bytes(), Bytes::from_static(b"recovery-bucket"));
            assert_eq!(observer.forwarded(), 1);
            let another = call_body(&client, address, "Lock", Bytes::from_static(b"recovery-bucket"))
                .await
                .expect("observer forwards without pausing");
            another.into_body().collect().await.expect("second body");
            assert_eq!(observer.forwarded(), 2);
            drop(observer);
            let routes = proxy
                .pause(&["ReadVersion"], usize::MAX, Some("recovery-route"))
                .expect("disk-route observer");
            routes.release();
            for (disk, expected) in [("drive-a", 1), ("drive-a", 1), ("drive-b", 2)] {
                let request = ReadVersionRequest {
                    disk: disk.to_owned(),
                    path: "recovery-route".to_owned(),
                    ..Default::default()
                }
                .encode_to_vec();
                let mut frame = vec![0];
                frame.extend_from_slice(&u32::try_from(request.len()).expect("small frame").to_be_bytes());
                frame.extend_from_slice(&request);
                call_body(&client, address, "ReadVersion", Bytes::from(frame))
                    .await
                    .expect("routed read")
                    .into_body()
                    .collect()
                    .await
                    .expect("routed response");
                assert_eq!(routes.forwarded_routes(), expected, "repeated traffic from one disk is only one voter");
            }
        })
        .await
        .expect("passive observer deadline");
    }

    #[tokio::test]
    async fn scoped_fault_discards_expired_packets_without_replaying_them() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("backend listener");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
                .await
                .expect("proxy listener");
            let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            assert!(proxy.pause(&["Lock"], 1, Some("")).is_err(), "empty scope must not match every body");
            let pause = proxy
                .pause(&["Lock"], usize::MAX, Some("specific-bucket"))
                .expect("bucket-scoped lock fault");
            let unrelated = call_body(&client, proxy.local_addr(), "Lock", Bytes::from_static(b"other-bucket"))
                .await
                .expect("unrelated bucket must progress");
            let unrelated = unrelated.into_body().collect().await.expect("unrelated response");
            assert_eq!(unrelated.trailers().expect("unchanged trailers")["x-proof"], "unchanged-proof");
            assert_eq!(unrelated.to_bytes(), Bytes::from_static(b"other-bucket"));
            assert_eq!(pause.hits(), 0);
            let task_client = client.clone();
            let address = proxy.local_addr();
            let blocked =
                tokio::spawn(
                    async move { call_body(&task_client, address, "Lock", Bytes::from_static(b"specific-bucket")).await },
                );
            pause.wait_for_hits(1).await.expect("selected bucket reached");
            assert!(!blocked.is_finished());
            pause.discard_pending();
            drop(pause); // Drop must not replace Discard with Forward.
            assert!(blocked.await.expect("request task").is_err());
            assert_eq!(backend.received.load(Ordering::SeqCst), 1, "discard must never execute at the peer");
            let recovered = call(&client, address, "Lock").await.expect("future requests recover");
            recovered.into_body().collect().await.expect("recovered response");
            assert_eq!(backend.received.load(Ordering::SeqCst), 2);
        })
        .await
        .expect("scoped discard deadline");
        assert_late_packet_ownership().await;
    }
    async fn assert_late_packet_ownership() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("backend listener");
            {
                let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
                    .await
                    .expect("live late-mode proxy");
                let pause = proxy
                    .pause_late(&["WriteMetadata"], 1, "late-upload")
                    .expect("retain live packet");
                let address = proxy.local_addr();
                let caller = tokio::spawn(async move {
                    let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
                    call_body(&client, address, "WriteMetadata", Bytes::from_static(b"signed late-upload control")).await
                });
                pause.wait_for_hits(1).await.expect("live caller reaches pause");
                pause.release();
                let response = caller.await.expect("live caller task").expect("live response");
                let collected = response.into_body().collect().await.expect("retained response body");
                let trailers = collected.trailers().expect("retained response trailers");
                assert_eq!(trailers["grpc-status"], "0");
                assert_eq!(trailers["x-proof"], "unchanged-proof");
                assert_eq!(collected.to_bytes(), Bytes::from_static(b"signed late-upload control"));
            }
            for disposition in ["deliver", "drop_proxy", "drop_pause"] {
                let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
                    .await
                    .expect("late proxy");
                let pause = proxy.pause_late(&["WriteMetadata"], 1, "late-upload").expect("retain packet");
                let address = proxy.local_addr();
                let caller = tokio::spawn(async move {
                    let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
                    call_body(&client, address, "WriteMetadata", Bytes::from_static(b"signed late-upload control")).await
                });
                pause.wait_for_hits(1).await.expect("packet retained before cancellation");
                caller.abort();
                assert!(caller.await.expect_err("caller cancelled").is_cancelled());
                let before = backend.received.load(Ordering::SeqCst);
                assert_eq!(pause.responses(), 0);
                let handles = proxy
                    .late_requests
                    .lock()
                    .expect("retained tasks")
                    .as_ref()
                    .expect("open registry")
                    .clone();
                assert_eq!(handles.len(), 1, "the intercepted packet must have supervised ownership");
                match disposition {
                    "deliver" => {
                        pause.release();
                        while handles.iter().any(|task| !task.is_finished()) {
                            tokio::task::yield_now().await;
                        }
                        assert_eq!(pause.responses(), 1);
                        assert_eq!(backend.received.load(Ordering::SeqCst), before + 1);
                    }
                    "drop_proxy" => {
                        drop(proxy);
                        while handles.iter().any(|task| !task.is_finished()) {
                            tokio::task::yield_now().await;
                        }
                        pause.release();
                        assert_eq!(backend.received.load(Ordering::SeqCst), before, "teardown must cancel retained packets");
                        assert_eq!(pause.responses(), 0);
                    }
                    "drop_pause" => {
                        drop(pause);
                        while handles.iter().any(|task| !task.is_finished()) {
                            tokio::task::yield_now().await;
                        }
                        assert_eq!(backend.received.load(Ordering::SeqCst), before, "late delivery must be explicit");
                    }
                    _ => unreachable!(),
                }
            }
        })
        .await
        .expect("late proxy ownership deadline");
    }

    #[test]
    fn delete_scope_matches_only_the_upload_root() {
        let frame = |volume: &str, path: &str| {
            let message = DeleteRequest {
                volume: volume.to_owned(),
                path: path.to_owned(),
                options: r#"{"recursive":true}"#.to_owned(),
                ..Default::default()
            };
            let payload = message.encode_to_vec();
            let length = u32::try_from(payload.len()).expect("small control packet");
            let mut bytes = vec![0];
            bytes.extend_from_slice(&length.to_be_bytes());
            bytes.extend_from_slice(&payload);
            bytes
        };
        let root = b"hash/upload";
        assert!(scope_matches("Delete", &frame(".rustfs.sys/multipart", "hash/upload"), root).expect("root frame"));
        assert!(scope_matches("Delete", &frame(".rustfs.sys/multipart", "hash/upload/"), root).expect("directory frame"));
        for path in ["hash/upload/part.1", "hash/upload/.abort-authority", "hash/upload-other"] {
            assert!(!scope_matches("Delete", &frame(".rustfs.sys/multipart", path), root).expect("other path"));
        }
        assert!(!scope_matches("Delete", &frame(".rustfs.sys/tmp", "hash/upload"), root).expect("other volume"));
        let non_recursive = DeleteRequest {
            volume: ".rustfs.sys/multipart".to_owned(),
            path: "hash/upload".to_owned(),
            options: r#"{"recursive":false}"#.to_owned(),
            ..Default::default()
        }
        .encode_to_vec();
        let mut non_recursive_frame = vec![0];
        non_recursive_frame.extend_from_slice(&u32::try_from(non_recursive.len()).expect("small frame").to_be_bytes());
        non_recursive_frame.extend_from_slice(&non_recursive);
        assert!(!scope_matches("Delete", &non_recursive_frame, root).expect("non-recursive frame"));
        assert!(
            scope_matches("Delete", &[0, 0, 0, 0, 1], root).is_err(),
            "truncation must not become a match"
        );
    }
}
