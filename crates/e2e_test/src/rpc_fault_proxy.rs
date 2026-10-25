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
    BatchGenerallyLockRequest, DeleteRequest, GenerallyLockRequest, ReadVersionRequest, RenameDataResponse,
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
    limit: usize,
    retain_after_disconnect: bool,
    refused_mutations: AtomicUsize,
    responses: AtomicUsize,
    matched: AtomicUsize,
    blocked: AtomicUsize,
    forwarded: AtomicUsize,
    forwarded_routes: Mutex<HashSet<String>>,
    record_lock_owners: bool,
    lock_owner: Option<String>,
    forwarded_lock_owners: Mutex<HashSet<String>>,
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

    pub(crate) fn forwarded_lock_owners(&self) -> usize {
        self.state
            .forwarded_lock_owners
            .lock()
            .expect("RPC lock owner observations")
            .len()
    }

    pub(crate) fn has_forwarded_lock_owner(&self, owner: &str) -> bool {
        self.state
            .forwarded_lock_owners
            .lock()
            .expect("RPC lock owner observations")
            .contains(owner)
    }

    pub(crate) fn responses(&self) -> usize {
        self.state.responses.load(Ordering::SeqCst)
    }

    pub(crate) fn refused_mutations(&self) -> usize {
        self.state.refused_mutations.load(Ordering::SeqCst)
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
    lock_slot: PauseSlot,
    task: JoinHandle<()>,
    late_requests: LateRequests,
    transport_passthrough: watch::Sender<bool>,
}

impl RpcFaultProxy {
    pub(crate) async fn start_on(address: SocketAddr, target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let slot: PauseSlot = Arc::new(Mutex::new(None));
        let active = slot.clone();
        let lock_slot: PauseSlot = Arc::new(Mutex::new(None));
        let active_lock = lock_slot.clone();
        let late_requests: LateRequests = Arc::new(Mutex::new(Some(Vec::new())));
        let late = late_requests.clone();
        let (transport_passthrough, transport_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let h1: ProxyClient = Client::builder(TokioExecutor::new()).build_http();
            let h2: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((mut socket, _)) = accepted else { break };
                        let h1 = h1.clone();
                        let h2 = h2.clone();
                        let active = active.clone();
                        let active_lock = active_lock.clone();
                        let late = late.clone();
                        let mut transport = transport_rx.clone();
                        connections.spawn(async move {
                            if *transport.borrow_and_update() {
                                // Seed legacy streaming traffic without HTTP adaptation.
                                // Switching to method faults retires every raw connection.
                                tokio::select! {
                                    _ = transport.changed() => {},
                                    _ = async {
                                        let mut server = tokio::net::TcpStream::connect(target).await?;
                                        tokio::io::copy_bidirectional(&mut socket, &mut server).await?;
                                        Ok::<_, io::Error>(())
                                    } => {},
                                }
                                return;
                            }
                            let service = service_fn(move |request: Request<Incoming>| {
                                let client = if request.version() == http::Version::HTTP_2 { h2.clone() } else { h1.clone() };
                                let lock_method = request.uri().path().strip_prefix("/node_service.NodeService/")
                                    .is_some_and(|method| matches!(method, "Lock" | "LockBatch"));
                                let owner_pause = if lock_method {
                                    let slot = active_lock.lock().expect("RPC owner lock slot");
                                    slot.clone()
                                } else {
                                    None
                                };
                                let pause = owner_pause.or_else(|| active.lock().expect("RPC pause slot").clone());
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
            lock_slot,
            task,
            late_requests,
            transport_passthrough,
        })
    }

    pub(crate) fn set_transport_passthrough(&self, enabled: bool) {
        self.transport_passthrough.send_replace(enabled);
    }

    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// Pause at most limit requests to the named unary methods; other streams
    /// on the same HTTP/2 connection continue forwarding.
    pub(crate) fn pause(&self, methods: &[&'static str], limit: usize, scope: Option<&str>) -> io::Result<RpcPause> {
        self.pause_with_lifetime(methods, limit, scope, false, false, None)
    }

    /// Distinguish contenders by owner rather than counting lock retries.
    pub(crate) fn pause_upload_locks(&self, scope: &str) -> io::Result<RpcPause> {
        self.pause_with_lifetime(&["Lock", "LockBatch"], usize::MAX, Some(scope), false, true, None)
    }

    /// Hold one coordinator's locks independently of the part-transaction gate.
    pub(crate) fn pause_upload_lock_owner(&self, scope: &str, owner: &str) -> io::Result<RpcPause> {
        self.pause_with_lifetime(&["Lock", "LockBatch"], usize::MAX, Some(scope), false, false, Some(owner))
    }

    /// Retain bounded control packets until explicit delivery after caller retirement.
    pub(crate) fn pause_late(&self, methods: &[&'static str], limit: usize, scope: &str) -> io::Result<RpcPause> {
        if limit > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "late RPC limit exceeds control-packet budget",
            ));
        }
        self.pause_with_lifetime(methods, limit, Some(scope), true, false, None)
    }

    fn pause_with_lifetime(
        &self,
        methods: &[&'static str],
        limit: usize,
        scope: Option<&str>,
        retain_after_disconnect: bool,
        record_lock_owners: bool,
        lock_owner: Option<&str>,
    ) -> io::Result<RpcPause> {
        // Always take the general slot before the owner slot. A typed lock
        // gate may coexist with storage faults, never another lock rule.
        let mut general_slot = self.slot.lock().expect("RPC pause slot");
        let mut owner_slot = self.lock_slot.lock().expect("RPC owner lock slot");
        if (lock_owner.is_some()
            && general_slot
                .as_ref()
                .is_some_and(|pause| pause.methods.iter().any(|method| matches!(*method, "Lock" | "LockBatch"))))
            || (lock_owner.is_none()
                && owner_slot.is_some()
                && methods.iter().any(|method| matches!(*method, "Lock" | "LockBatch")))
        {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "RPC lock gates must not overlap"));
        }
        let (slot, slot_arc) = if lock_owner.is_some() {
            (&mut *owner_slot, self.lock_slot.clone())
        } else {
            (&mut *general_slot, self.slot.clone())
        };
        if slot.is_some()
            || methods.is_empty()
            || limit == 0
            || scope.is_some_and(str::is_empty)
            || lock_owner.is_some_and(str::is_empty)
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
            limit,
            retain_after_disconnect,
            refused_mutations: AtomicUsize::new(0),
            responses: AtomicUsize::new(0),
            matched: AtomicUsize::new(0),
            blocked: AtomicUsize::new(0),
            forwarded: AtomicUsize::new(0),
            forwarded_routes: Mutex::new(HashSet::new()),
            record_lock_owners,
            lock_owner: lock_owner.map(str::to_owned),
            forwarded_lock_owners: Mutex::new(HashSet::new()),
            reached: Notify::new(),
            release,
        });
        *slot = Some(state.clone());
        Ok(RpcPause { state, slot: slot_arc })
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
    if method != "Delete" {
        return Ok(body.windows(scope.len()).any(|window| window == scope));
    }
    let request = DeleteRequest::decode(unary_payload(body)?).map_err(io::Error::other)?;
    let options: serde_json::Value = serde_json::from_str(&request.options).map_err(io::Error::other)?;
    Ok(request.volume == ".rustfs.sys/multipart"
        && request.path.trim_end_matches('/').as_bytes() == scope
        && options.get("recursive").and_then(serde_json::Value::as_bool) == Some(true))
}

fn scoped_lock_owners(method: &str, body: &[u8], scope: &[u8]) -> io::Result<Vec<String>> {
    let payload = unary_payload(body)?;
    let args = match method {
        "Lock" => vec![GenerallyLockRequest::decode(payload).map_err(io::Error::other)?.args],
        "LockBatch" => BatchGenerallyLockRequest::decode(payload).map_err(io::Error::other)?.args,
        _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "lock observer requires a lock method")),
    };
    args.into_iter()
        .filter(|args| args.as_bytes().windows(scope.len()).any(|window| window == scope))
        .map(|args| {
            let request: serde_json::Value = serde_json::from_str(&args).map_err(io::Error::other)?;
            request["owner"]
                .as_str()
                .filter(|owner| !owner.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "scoped lock needs an owner"))
        })
        .collect()
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
    if let Some(pause) = pause {
        let method = parts.uri.path().strip_prefix("/node_service.NodeService/");
        if let Some(method) = method.filter(|name| pause.methods.contains(name)) {
            let mut route = None;
            let mut lock_owners = Vec::new();
            let matches = if let Some(scope) = pause.scope.as_deref() {
                // Inspect bounded control routing while preserving the exact
                // signed bytes and trailers forwarded to the backend.
                let collected = Limited::new(body, 1024 * 1024).collect().await.map_err(io::Error::other)?;
                let trailers = collected.trailers().cloned();
                let bytes = collected.to_bytes();
                let mut matches = scope_matches(method, &bytes, scope)?;
                if matches && (pause.record_lock_owners || pause.lock_owner.is_some()) {
                    lock_owners = scoped_lock_owners(method, &bytes, scope)?;
                    if let Some(owner) = &pause.lock_owner {
                        matches = lock_owners.iter().any(|candidate| candidate == owner);
                    }
                }
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
                    lock_owners,
                    matches!(method, "RenameData" | "RenameDataAtIncarnation" | "RenameDataWithQuotaFence"),
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
    if let Some((pause, route, lock_owners, rename)) = observed {
        pause.responses.fetch_add(1, Ordering::SeqCst);
        if response.status().is_success() && response.headers().get("grpc-status").is_none_or(|status| status == "0") {
            pause.forwarded.fetch_add(1, Ordering::SeqCst);
            if let Some(route) = route {
                pause.forwarded_routes.lock().expect("RPC route observations").insert(route);
            }
            pause
                .forwarded_lock_owners
                .lock()
                .expect("RPC lock owner observations")
                .extend(lock_owners);
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
                                trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                                trailers.insert("x-proof", http::HeaderValue::from_static("unchanged-proof"));
                                let stream = futures::stream::iter([
                                    Ok::<_, Infallible>(Frame::data(body)),
                                    Ok(Frame::trailers(trailers)),
                                ]);
                                let mut response = Response::new(StreamBody::new(stream));
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

    #[tokio::test]
    async fn preserves_protocol_authority_payload_and_grpc_trailers() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("backend listener");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback address"), backend.address)
                .await
                .expect("proxy");
            for transport in [true, false] {
                proxy.set_transport_passthrough(transport);
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
        assert_upload_lock_owner_observations().await;
    }

    async fn assert_upload_lock_owner_observations() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("lock observation backend");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
                .await
                .expect("lock observation proxy");
            let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            let observer = proxy.pause_upload_locks("target-upload").expect("typed lock observer");
            observer.release();
            let args = |owner: &str, resource: &str| serde_json::json!({"owner": owner, "resource": resource}).to_string();
            let lock_frame = |method: &str, requests: Vec<String>| {
                let payload = if method == "Lock" {
                    GenerallyLockRequest {
                        args: requests[0].clone(),
                    }
                    .encode_to_vec()
                } else {
                    BatchGenerallyLockRequest { args: requests }.encode_to_vec()
                };
                let mut frame = vec![0];
                frame.extend_from_slice(&u32::try_from(payload.len()).expect("small lock frame").to_be_bytes());
                frame.extend_from_slice(&payload);
                Bytes::from(frame)
            };
            for (method, requests, expected) in [
                ("Lock", vec![args("a", "target-upload")], 1),
                ("Lock", vec![args("a", "target-upload")], 1),
                ("Lock", vec![args("b", "other-upload")], 1),
                ("LockBatch", vec![args("a", "target-upload"), args("b", "target-upload")], 2),
                ("LockBatch", vec![args("c", "other-upload"), args("b", "target-upload")], 2),
                ("Lock", vec![args("c", "target-upload")], 3),
            ] {
                let frame = lock_frame(method, requests);
                let body = call_body(&client, proxy.local_addr(), method, frame.clone())
                    .await
                    .expect("observed lock reaches backend")
                    .into_body()
                    .collect()
                    .await
                    .expect("observed lock response");
                assert_eq!(body.trailers().expect("preserved lock trailers")["x-proof"], "unchanged-proof");
                assert_eq!(body.to_bytes(), frame, "signed packet bytes must remain unchanged");
                assert_eq!(
                    observer.forwarded_lock_owners(),
                    expected,
                    "retries and other resources are not new contenders"
                );
            }
            assert!(observer.has_forwarded_lock_owner("b"));
            assert!(!observer.has_forwarded_lock_owner("unseen"));
            drop(observer);

            let storage = proxy
                .pause(&["PreparePartTransaction"], usize::MAX, Some("target-upload"))
                .expect("independent storage gate");
            let owner = proxy
                .pause_upload_lock_owner("target-upload", "held")
                .expect("scoped owner gate");
            assert!(proxy.pause_upload_lock_owner("target-upload", "another").is_err());
            let held_frame = lock_frame("Lock", vec![args("held", "target-upload")]);
            let address = proxy.local_addr();
            let held_client = client.clone();
            let packet = held_frame.clone();
            let held = tokio::spawn(async move { call_body(&held_client, address, "Lock", packet).await });
            owner.wait_for_hits(1).await.expect("selected coordinator reaches owner gate");
            let part_client = client.clone();
            let part = tokio::spawn(async move {
                call_body(&part_client, address, "PreparePartTransaction", Bytes::from_static(b"target-upload-part")).await
            });
            storage.wait_for_hits(1).await.expect("part reaches its separate gate");
            for (request_owner, resource) in [("allowed", "target-upload"), ("held", "other-upload")] {
                let packet = lock_frame("LockBatch", vec![args(request_owner, resource)]);
                let returned = call_body(&client, address, "LockBatch", packet.clone())
                    .await
                    .expect("other owner or upload must remain live")
                    .into_body()
                    .collect()
                    .await
                    .expect("bypass body");
                assert_eq!(returned.to_bytes(), packet);
            }
            assert_eq!(owner.hits(), 1, "unrelated owners and uploads cannot count as the held contender");
            storage.release();
            assert_eq!(
                part.await
                    .expect("part task")
                    .expect("released part")
                    .into_body()
                    .collect()
                    .await
                    .expect("part body")
                    .to_bytes(),
                Bytes::from_static(b"target-upload-part")
            );
            assert!(!held.is_finished(), "releasing part transactions must not release the losing lock owner");
            owner.release();
            let returned = held
                .await
                .expect("held owner task")
                .expect("released owner")
                .into_body()
                .collect()
                .await
                .expect("held packet response");
            assert_eq!(returned.trailers().expect("owner trailers")["x-proof"], "unchanged-proof");
            assert_eq!(returned.to_bytes(), held_frame);
            drop(storage);
            assert!(
                proxy.pause(&["Lock"], 1, Some("target-upload")).is_err(),
                "lock rules cannot shadow one another"
            );
            drop(owner);
            let general = proxy
                .pause(&["LockBatch"], 1, Some("target-upload"))
                .expect("general lock gate");
            assert!(proxy.pause_upload_lock_owner("target-upload", "held").is_err());
            drop(general);
            assert!(proxy.pause_upload_lock_owner("target-upload", "").is_err());

            let lost = proxy
                .pause_upload_lock_owner("target-upload", "held")
                .expect("discardable owner gate");
            let lost_client = client.clone();
            let packet = lock_frame("LockBatch", vec![args("held", "target-upload")]);
            let before = backend.received.load(Ordering::SeqCst);
            let lost_call = tokio::spawn(async move { call_body(&lost_client, address, "LockBatch", packet).await });
            lost.wait_for_hits(1).await.expect("batched owner reaches loss gate");
            lost.discard_pending();
            drop(lost);
            assert!(lost_call.await.expect("discarded owner task").is_err());
            assert_eq!(backend.received.load(Ordering::SeqCst), before, "discard must not replay a lock grant");
        })
        .await
        .expect("lock owner observation deadline");
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
