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
use rustfs_protos::proto_gen::node_service::DeleteRequest;
use std::{
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
    matched: AtomicUsize,
    blocked: AtomicUsize,
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
    task: JoinHandle<()>,
}

impl RpcFaultProxy {
    pub(crate) async fn start_on(address: SocketAddr, target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let slot: PauseSlot = Arc::new(Mutex::new(None));
        let active = slot.clone();
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
                        connections.spawn(async move {
                            let service = service_fn(move |request: Request<Incoming>| {
                                let client = if request.version() == http::Version::HTTP_2 { h2.clone() } else { h1.clone() };
                                let pause = active.lock().expect("RPC pause slot").clone();
                                forward_request(request, client, target, pause)

                            });
                            let _ = auto::Builder::new(TokioExecutor::new())
                                .serve_connection(TokioIo::new(socket), service).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Ok(Self { address, slot, task })
    }

    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.address
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

fn scope_matches(method: &str, body: &[u8], scope: &[u8]) -> io::Result<bool> {
    if method != "Delete" {
        return Ok(body.windows(scope.len()).any(|window| window == scope));
    }
    // Abort's recursive root delete must not be confused with transaction or
    // part cleanup below that root. This is a unary, uncompressed gRPC frame.
    if body.len() < 5 || body[0] != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "scoped Delete needs an uncompressed unary frame",
        ));
    }
    let mut encoded_length = [0; 4];
    encoded_length.copy_from_slice(&body[1..5]);
    if u32::try_from(body.len() - 5).ok() != Some(u32::from_be_bytes(encoded_length)) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "scoped Delete frame length mismatch"));
    }
    let request = DeleteRequest::decode(&body[5..]).map_err(io::Error::other)?;
    let options: serde_json::Value = serde_json::from_str(&request.options).map_err(io::Error::other)?;
    Ok(request.volume == ".rustfs.sys/multipart"
        && request.path.trim_end_matches('/').as_bytes() == scope
        && options.get("recursive").and_then(serde_json::Value::as_bool) == Some(true))
}

async fn forward_request(
    request: Request<Incoming>,
    client: ProxyClient,
    target: SocketAddr,
    pause: Option<Arc<PauseState>>,
) -> io::Result<hyper::Response<Incoming>> {
    let (mut parts, body) = request.into_parts();
    let mut body = body.map_err(io::Error::other).boxed();
    if let Some(pause) = pause {
        let method = parts.uri.path().strip_prefix("/node_service.NodeService/");
        if let Some(method) = method.filter(|name| pause.methods.contains(name)) {
            let matches = if let Some(scope) = pause.scope.as_deref() {
                // Inspect bounded routing data while retaining the exact
                // original bytes covered by the RPC signature.
                let collected = Limited::new(body, 1024 * 1024).collect().await.map_err(io::Error::other)?;
                let trailers = collected.trailers().cloned();
                let bytes = collected.to_bytes();
                let matches = scope_matches(method, &bytes, scope)?;
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
                                let authority = request.headers().get(http::header::HOST).cloned();
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
        for path in ["hash/upload/part.1", "hash/upload-other"] {
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
