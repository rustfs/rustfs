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
use rustfs_protos::proto_gen::node_service::{DeleteRequest, ReadVersionRequest, ScannerScopedDirtyUsageAckRequest};
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

mod scoped_ack;
pub(crate) use scoped_ack::{ScopedAckFault, ScopedAckQueue, ScopedAckScope};

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
    matched: AtomicUsize,
    blocked: AtomicUsize,
    forwarded: AtomicUsize,
    forwarded_routes: Mutex<HashSet<String>>,
    reached: Notify,
    release: watch::Sender<Disposition>,
}

type PauseSlot = Arc<Mutex<Option<Arc<PauseState>>>>;

pub(crate) struct RpcPause {
    state: Arc<PauseState>,
    slot: PauseSlot,
}

impl RpcPause {
    pub(crate) fn hits(&self) -> usize {
        self.state.blocked.load(Ordering::SeqCst)
    }

    /// Selected RPCs whose backend returned response headers, excluding
    /// discarded packets. This does not prove a successful gRPC trailer or ACK apply.
    pub(crate) fn forwarded(&self) -> usize {
        self.state.forwarded.load(Ordering::SeqCst)
    }

    pub(crate) fn forwarded_routes(&self) -> usize {
        self.state.forwarded_routes.lock().expect("RPC route observations").len()
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
        self.release();
        let mut slot = self.slot.lock().expect("RPC pause slot");
        if slot.as_ref().is_some_and(|current| Arc::ptr_eq(current, &self.state)) {
            *slot = None;
        }
    }
}

pub(crate) struct RpcFaultProxy {
    address: SocketAddr,
    slot: PauseSlot,
    scoped_ack: scoped_ack::QueueSlot,
    task: JoinHandle<()>,
}

impl RpcFaultProxy {
    pub(crate) async fn start_on(address: SocketAddr, target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let slot: PauseSlot = Arc::new(Mutex::new(None));
        let active = slot.clone();
        let scoped_ack = scoped_ack::empty_slot();
        let active_ack = scoped_ack.clone();
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
                        let active_ack = active_ack.clone();
                        connections.spawn(async move {
                            let service = service_fn(move |request: Request<Incoming>| {
                                let client = if request.version() == http::Version::HTTP_2 { h2.clone() } else { h1.clone() };
                                let pause = active.lock().expect("RPC pause slot").clone();
                                let ack = active_ack.lock().expect("scoped ACK queue slot").clone();
                                forward_request(request, client, target, pause, ack)

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
            scoped_ack,
            task,
        })
    }

    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// Retain only genuine scoped ACKs for one receiver/session and bucket set.
    pub(crate) fn scoped_ack_queue(&self, scope: ScopedAckScope, capacity: usize) -> io::Result<ScopedAckQueue> {
        ScopedAckQueue::arm(self.scoped_ack.clone(), scope, capacity)
    }

    /// Pause at most limit requests to the named unary methods; other streams
    /// on the same HTTP/2 connection continue forwarding.
    pub(crate) fn pause(&self, methods: &[&'static str], limit: usize, scope: Option<&str>) -> io::Result<RpcPause> {
        let mut slot = self.slot.lock().expect("RPC pause slot");
        if slot.is_some() || methods.is_empty() || limit == 0 || scope.is_some_and(str::is_empty) {
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
    if method == "ScannerScopedDirtyUsageAck" {
        return scoped_ack_matches(body, Some(scope));
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

fn scoped_ack_matches(body: &[u8], scope: Option<&[u8]>) -> io::Result<bool> {
    let request = ScannerScopedDirtyUsageAckRequest::decode(unary_payload(body)?).map_err(io::Error::other)?;
    Ok(!request.probe_only && scope.is_none_or(|scope| request.entries.iter().any(|entry| entry.bucket.as_bytes() == scope)))
}

async fn forward_request(
    request: Request<Incoming>,
    client: ProxyClient,
    target: SocketAddr,
    pause: Option<Arc<PauseState>>,
    ack: Option<Arc<scoped_ack::QueueState>>,
) -> io::Result<hyper::Response<ProxyBody>> {
    let request = request.map(|body| body.map_err(io::Error::other).boxed());
    let request = if let Some(ack) = ack
        && request.uri().path() == "/node_service.ScannerControlService/ScannerScopedDirtyUsageAck"
    {
        match scoped_ack::intercept(request, client.clone(), target, ack).await? {
            scoped_ack::Interception::Pass(request) => request,
            scoped_ack::Interception::Handled(response) => return Ok(response),
        }
    } else {
        request
    };
    let (parts, body) = request.into_parts();
    let mut body = body;
    let mut observed = None;
    if let Some(pause) = pause {
        let method = parts
            .uri
            .path()
            .strip_prefix("/node_service.NodeService/")
            .or_else(|| parts.uri.path().strip_prefix("/node_service.ScannerControlService/"));
        if let Some(method) = method.filter(|name| pause.methods.contains(name)) {
            let mut route = None;
            let matches = if pause.scope.is_some() || method == "ScannerScopedDirtyUsageAck" {
                // Inspect bounded control routing while preserving the exact
                // signed bytes and trailers forwarded to the backend.
                let collected = Limited::new(body, 1024 * 1024).collect().await.map_err(io::Error::other)?;
                let trailers = collected.trailers().cloned();
                let bytes = collected.to_bytes();
                let matches = match pause.scope.as_deref() {
                    Some(scope) => scope_matches(method, &bytes, scope)?,
                    None => scoped_ack_matches(&bytes, None)?,
                };
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
                observed = Some((pause, route));
            }
        }
    }
    let response = forward_to_backend(Request::from_parts(parts, body), client, target).await?;
    if let Some((pause, route)) = observed
        && response.status().is_success()
        && response.headers().get("grpc-status").is_none_or(|status| status == "0")
    {
        pause.forwarded.fetch_add(1, Ordering::SeqCst);
        if let Some(route) = route {
            pause.forwarded_routes.lock().expect("RPC route observations").insert(route);
        }
    }
    Ok(response.map(|body| body.map_err(io::Error::other).boxed()))
}

async fn forward_to_backend(
    request: Request<ProxyBody>,
    client: ProxyClient,
    target: SocketAddr,
) -> io::Result<hyper::Response<Incoming>> {
    let (mut parts, body) = request.into_parts();
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
    client
        .request(Request::from_parts(parts, body))
        .await
        .map_err(io::Error::other)
}

impl Drop for RpcFaultProxy {
    fn drop(&mut self) {
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
        auth_headers: Arc<Mutex<Vec<(String, String)>>>,
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
        let auth_headers = Arc::new(Mutex::new(Vec::new()));
        let backend_auth = auth_headers.clone();
        let task = tokio::spawn(async move {
            let mut children = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { break };
                        let received = received_task.clone();
                        let backend_auth = backend_auth.clone();
                        children.spawn(async move {
                            let service = service_fn(move |request: Request<Incoming>| {
                                let received = received.clone();
                                let backend_auth = backend_auth.clone();
                                async move {
                                received.fetch_add(1, Ordering::SeqCst);
                                let authority = request.headers().get(http::header::HOST).cloned();
                                if let (Some(nonce), Some(signature)) = (request.headers().get("x-rustfs-rpc-nonce"), request.headers().get("x-rustfs-rpc-signature-v2")) {
                                    backend_auth.lock().expect("backend auth header observations").push((
                                        nonce.to_str().expect("test nonce").to_string(), signature.to_str().expect("test signature").to_string()));
                                }
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
        Ok(Backend {
            received,
            auth_headers,
            address,
            task,
        })
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
        let service = if method == "ScannerScopedDirtyUsageAck" {
            "ScannerControlService"
        } else {
            "NodeService"
        };
        let request = Request::builder()
            .method("POST")
            .uri(format!("http://{address}/node_service.{service}/{method}"))
            .header(http::header::TE, "trailers")
            .body(body)
            .map_err(io::Error::other)?;
        client.request(request).await.map_err(io::Error::other)
    }

    const ACK_OWNER: &str = "11111111-1111-1111-1111-111111111111";
    const ACK_INSTANCE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn ack_scope() -> ScopedAckScope {
        ScopedAckScope {
            nonce: uuid::Uuid::new_v4().to_string(),
            owner_id: ACK_OWNER.to_string(),
            instance_id: ACK_INSTANCE.to_string(),
            buckets: std::collections::BTreeSet::from(["cold-bucket".to_string()]),
        }
    }

    fn ack_request(bucket: &str, generation: u64) -> ScannerScopedDirtyUsageAckRequest {
        ScannerScopedDirtyUsageAckRequest {
            challenge: vec![7; 16].into(),
            protocol_version: 1,
            owner_id: ACK_OWNER.to_string(),
            instance_id: ACK_INSTANCE.to_string(),
            scope: 1,
            probe_only: false,
            entries: vec![rustfs_protos::proto_gen::node_service::ScannerScopedDirtyUsageEntry {
                bucket: bucket.to_string(),
                bucket_incarnation: vec![4; 16].into(),
                generation,
            }],
        }
    }

    fn ack_frame(request: &ScannerScopedDirtyUsageAckRequest) -> Bytes {
        let payload = request.encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&u32::try_from(payload.len()).expect("bounded test frame").to_be_bytes());
        frame.extend_from_slice(&payload);
        Bytes::from(frame)
    }

    fn ack_hash(bytes: &[u8]) -> String {
        use sha2::Digest as _;
        hex_simd::encode_to_string(&sha2::Sha256::digest(bytes)[..], hex_simd::AsciiCase::Lower)
    }

    // Transport-only fixtures: the distributed tests supply production signatures.
    async fn call_ack_frame(
        client: &ProxyClient,
        address: SocketAddr,
        frame: Bytes,
        nonce: &str,
    ) -> io::Result<hyper::Response<Incoming>> {
        let request = Request::builder()
            .method("POST")
            .uri(format!("http://{address}/node_service.ScannerControlService/ScannerScopedDirtyUsageAck"))
            .header(http::header::TE, "trailers")
            .header("x-rustfs-rpc-nonce", nonce)
            .header("x-rustfs-rpc-signature-v2", format!("opaque-signature-{nonce}"))
            .body(Full::new(frame).map_err(|never| match never {}).boxed())
            .map_err(io::Error::other)?;
        client.request(request).await.map_err(io::Error::other)
    }

    #[tokio::test]
    async fn scoped_ack_queue_loss_preserves_backend_boundary() {
        tokio::time::timeout(Duration::from_secs(10), async {
            for fault in [ScopedAckFault::RequestLoss, ScopedAckFault::ResponseLoss] {
                let backend = backend().await.expect("backend");
                let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
                    .await
                    .expect("proxy");
                let queue = proxy.scoped_ack_queue(ack_scope(), 1).expect("ACK queue");
                let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
                let frame = ack_frame(&ack_request("cold-bucket", 7));
                let address = proxy.local_addr();
                let body = frame.clone();
                let caller = tokio::spawn(async move { call_ack_frame(&client, address, body, "loss").await });
                queue.wait_for_packets(1).await.expect("captured ACK");
                assert_eq!(backend.received.load(Ordering::SeqCst), 0, "retention must precede backend dispatch");
                queue.settle(0, fault).expect("settle actual retained packet");
                assert!(
                    caller.await.expect("caller task").is_err(),
                    "the caller must observe the injected transport failure"
                );
                let receipt = queue.wait_for_completion(0).await.expect("complete fault receipt");
                let response_loss = fault == ScopedAckFault::ResponseLoss;
                assert_eq!(receipt["forwarded"], response_loss);
                assert_eq!(receipt["backend_complete"], response_loss);
                assert_eq!(backend.received.load(Ordering::SeqCst), usize::from(response_loss));
                if response_loss {
                    assert_eq!(receipt["outcome"], "response_lost");
                    assert_eq!(receipt["response"]["grpc_status"], "0", "status exists only in real backend trailers");
                    assert_eq!(receipt["response"]["bytes"], frame.len());
                    assert_eq!(receipt["response"]["sha256"], ack_hash(&frame));
                    assert!(
                        receipt["response"]["trailers_sha256"]
                            .as_str()
                            .is_some_and(|value| value.len() == 64)
                    );
                } else {
                    assert_eq!(receipt["outcome"], "request_lost");
                }
            }
        })
        .await
        .expect("bounded ACK loss checks");
    }

    #[tokio::test]
    async fn scoped_ack_queue_keeps_cancelled_packets_and_releases_out_of_order() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("backend");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
                .await
                .expect("proxy");
            let queue = proxy.scoped_ack_queue(ack_scope(), 2).expect("two-packet ACK queue");
            let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            let first = ack_frame(&ack_request("cold-bucket", 7));
            let second = ack_frame(&ack_request("cold-bucket", 9));
            let address = proxy.local_addr();
            let old_client = client.clone();
            let old_body = first.clone();
            let old_caller = tokio::spawn(async move { call_ack_frame(&old_client, address, old_body, "first").await });
            queue.wait_for_packets(1).await.expect("first retained packet");
            old_caller.abort();
            assert!(old_caller.await.expect_err("cancel original caller").is_cancelled());
            let new_body = second.clone();
            let new_caller = tokio::spawn(async move { call_ack_frame(&client, address, new_body, "second").await });
            queue.wait_for_packets(2).await.expect("second retained packet");
            assert_eq!(backend.received.load(Ordering::SeqCst), 0);
            queue.settle(1, ScopedAckFault::Forward).expect("release newer packet first");
            let response = new_caller
                .await
                .expect("new caller task")
                .expect("new response")
                .into_body()
                .collect()
                .await
                .expect("complete body");
            assert_eq!(response.trailers().expect("response trailers")["x-proof"], "unchanged-proof");
            assert_eq!(response.to_bytes(), second);
            queue.wait_for_completion(1).await.expect("new packet complete");
            queue
                .settle(0, ScopedAckFault::Forward)
                .expect("release old packet after its caller disappeared");
            let old = queue.wait_for_completion(0).await.expect("detached old packet complete");
            assert_eq!(old["backend_complete"], true);
            assert_eq!(old["request_sha256"], ack_hash(&first));
            assert_eq!(old["response"]["sha256"], ack_hash(&first));
            assert_eq!(backend.received.load(Ordering::SeqCst), 2);
            assert_eq!(
                *backend.auth_headers.lock().expect("backend header observations"),
                vec![
                    ("second".to_string(), "opaque-signature-second".to_string()),
                    ("first".to_string(), "opaque-signature-first".to_string())
                ]
            );
            assert!(
                queue.settle(0, ScopedAckFault::Forward).is_err(),
                "one original packet may only be forwarded once"
            );
        })
        .await
        .expect("bounded detached retention check");
    }

    #[tokio::test]
    async fn scoped_ack_queue_bounds_capacity_and_excludes_other_scopes() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("backend");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
                .await
                .expect("proxy");
            assert!(proxy.scoped_ack_queue(ack_scope(), 0).is_err());
            assert!(proxy.scoped_ack_queue(ack_scope(), 5).is_err());
            let mut invalid = ack_scope();
            invalid.nonce.clear();
            assert!(proxy.scoped_ack_queue(invalid, 1).is_err());
            let mut scope = ack_scope();
            scope.buckets.insert("cold-bucket-z".to_string());
            let queue = proxy.scoped_ack_queue(scope, 1).expect("bounded ACK queue");
            let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            let mut probe = ack_request("cold-bucket", 7);
            probe.probe_only = true;
            let mut other_peer = ack_request("cold-bucket", 7);
            other_peer.owner_id = uuid::Uuid::new_v4().to_string();
            let mut other_instance = ack_request("cold-bucket", 7);
            other_instance.instance_id = "b".repeat(32);
            let mut invalid_challenge = ack_request("cold-bucket", 7);
            invalid_challenge.challenge = vec![7; 15].into();
            let mut noncanonical_entries = ack_request("cold-bucket-z", 7);
            noncanonical_entries.entries.extend(ack_request("cold-bucket", 7).entries);
            let unselected = [
                probe,
                other_peer,
                other_instance,
                ack_request("cold-bucket-other", 7),
                ack_request("cold-bucket", u64::MAX),
                invalid_challenge,
                noncanonical_entries,
            ];
            let unselected_count = unselected.len();
            for (index, request) in unselected.into_iter().enumerate() {
                let body = ack_frame(&request);
                let response = call_ack_frame(&client, proxy.local_addr(), body.clone(), "unselected")
                    .await
                    .expect("unselected ACK progresses");
                assert_eq!(response.into_body().collect().await.expect("unselected body").to_bytes(), body);
                assert!(queue.receipts().is_empty(), "unselected or protocol-invalid ACK must not consume a slot");
                assert_eq!(backend.received.load(Ordering::SeqCst), index + 1);
            }
            assert!(queue.receipts().is_empty());
            let address = proxy.local_addr();
            let held_client = client.clone();
            let valid_frame = ack_frame(&ack_request("cold-bucket", 7));
            let held_body = valid_frame.clone();
            let held = tokio::spawn(async move { call_ack_frame(&held_client, address, held_body, "held").await });
            queue.wait_for_packets(1).await.expect("one matching packet");
            assert!(!held.is_finished(), "the subsequent valid ACK must be retained");
            assert_eq!(queue.receipts()[0]["request_sha256"], ack_hash(&valid_frame));
            assert_eq!(backend.received.load(Ordering::SeqCst), unselected_count);
            let extra = call_ack_frame(&client, address, ack_frame(&ack_request("cold-bucket", 8)), "beyond-capacity")
                .await
                .expect("capacity overflow passes without retention");
            extra.into_body().collect().await.expect("overflow response");
            assert_eq!(queue.receipts().len(), 1);
            assert_eq!(backend.received.load(Ordering::SeqCst), unselected_count + 1);
            drop(queue);
            assert!(
                held.await.expect("held caller").is_err(),
                "dropping the session must cancel its retained packet"
            );
            assert_eq!(
                backend.received.load(Ordering::SeqCst),
                unselected_count + 1,
                "session drop must never forward the held packet"
            );
        })
        .await
        .expect("bounded scope and lifecycle checks");
    }

    #[tokio::test]
    async fn scoped_ack_queue_expiry_never_forwards_retained_packet() {
        let (backend, _proxy, queue, caller) = tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("backend");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
                .await
                .expect("proxy");
            let queue = proxy.scoped_ack_queue(ack_scope(), 1).expect("ACK queue");
            let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            let address = proxy.local_addr();
            let caller = tokio::spawn(async move {
                call_ack_frame(&client, address, ack_frame(&ack_request("cold-bucket", 7)), "expires").await
            });
            queue.wait_for_packets(1).await.expect("retained packet");
            (backend, proxy, queue, caller)
        })
        .await
        .expect("bounded queue setup");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(120)).await;
        tokio::time::resume();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), caller)
                .await
                .expect("expiry completes caller")
                .expect("caller task")
                .is_err()
        );
        let receipt = queue.receipts().into_iter().next().expect("expired receipt");
        assert_eq!(receipt["completed"], true);
        assert_eq!(receipt["forwarded"], false);
        assert_eq!(receipt["backend_complete"], false);
        assert_eq!(backend.received.load(Ordering::SeqCst), 0);
        assert_eq!(
            queue
                .settle(0, ScopedAckFault::Forward)
                .expect_err("expired packet cannot be released")
                .kind(),
            io::ErrorKind::TimedOut
        );
    }

    #[tokio::test]
    async fn scanner_ack_fault_matches_exact_bucket_and_excludes_probes() {
        use rustfs_protos::proto_gen::node_service::ScannerScopedDirtyUsageEntry;

        tokio::time::timeout(Duration::from_secs(10), async {
            let backend = backend().await.expect("backend listener");
            let proxy = RpcFaultProxy::start_on("127.0.0.1:0".parse().expect("loopback"), backend.address)
                .await
                .expect("proxy listener");
            let client: ProxyClient = Client::builder(TokioExecutor::new()).http2_only(true).build_http();
            let pause = proxy
                .pause(&["ScannerScopedDirtyUsageAck"], 1, Some("cold-bucket"))
                .expect("scoped ACK fault");
            let frame = |bucket: &str, probe_only: bool| {
                let payload = ScannerScopedDirtyUsageAckRequest {
                    challenge: vec![3; 16].into(),
                    protocol_version: 1,
                    owner_id: "11111111-1111-1111-1111-111111111111".to_string(),
                    instance_id: "a".repeat(32),
                    scope: 1,
                    probe_only,
                    entries: vec![ScannerScopedDirtyUsageEntry {
                        bucket: bucket.to_string(),
                        bucket_incarnation: vec![4; 16].into(),
                        generation: 17,
                    }],
                }
                .encode_to_vec();
                let mut frame = vec![0];
                frame.extend_from_slice(&u32::try_from(payload.len()).expect("small ACK frame").to_be_bytes());
                frame.extend_from_slice(&payload);
                Bytes::from(frame)
            };
            for bytes in [frame("cold-bucket", true), frame("cold-bucket-other", false)] {
                let response = call_body(&client, proxy.local_addr(), "ScannerScopedDirtyUsageAck", bytes.clone())
                    .await
                    .expect("probe and other bucket must progress");
                assert_eq!(response.into_body().collect().await.expect("unselected response").to_bytes(), bytes);
            }
            assert_eq!(pause.hits(), 0);
            let signed_bytes = frame("cold-bucket", false);
            let task_bytes = signed_bytes.clone();
            let address = proxy.local_addr();
            let blocked =
                tokio::spawn(async move { call_body(&client, address, "ScannerScopedDirtyUsageAck", task_bytes).await });
            pause
                .wait_for_hits(1)
                .await
                .expect("real ScannerControlService method must reach the fault");
            assert!(!blocked.is_finished());
            assert_eq!(backend.received.load(Ordering::SeqCst), 2, "held ACK must not apply at the backend");
            pause.release();
            let collected = blocked
                .await
                .expect("ACK task")
                .expect("released ACK")
                .into_body()
                .collect()
                .await
                .expect("ACK body and trailers");
            assert_eq!(collected.trailers().expect("ACK trailers")["x-proof"], "unchanged-proof");
            assert_eq!(collected.to_bytes(), signed_bytes, "proxy must preserve the complete signed ACK frame");
        })
        .await
        .expect("scanner ACK fault deadline");
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
