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

use super::*;
use axum::body::Body;
use futures::{StreamExt as _, stream};
use hyper::{
    Request, Response,
    body::Incoming,
    server::conn::{http1, http2},
    service::service_fn,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::{convert::Infallible, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::Notify,
    task::{JoinHandle, JoinSet},
};
use tokio_rustls::{TlsAcceptor, rustls};

const PAYLOAD_SIZE: usize = 512 * 1024;

struct TlsPeer {
    url: String,
    outbound_tls: rustfs_tls_runtime::GlobalPublishedOutboundTlsState,
    connections: Arc<std::sync::atomic::AtomicUsize>,
    held_body_started: Arc<Notify>,
    short_body_eof: Arc<Notify>,
    task: JoinHandle<()>,
}

impl Drop for TlsPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TlsPeer {
    async fn start() -> Self {
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("create test certificate");
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("TLS versions")
            .with_no_client_auth()
            .with_single_cert(
                vec![identity.cert.der().clone()],
                rustls_pki_types::PrivateKeyDer::try_from(identity.signing_key.serialize_der()).expect("test key"),
            )
            .expect("test TLS identity");
        tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let acceptor = TlsAcceptor::from(Arc::new(tls));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind TLS peer");
        let url = format!("https://localhost:{}", listener.local_addr().expect("peer address").port());
        let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted = connections.clone();
        let held_body_started = Arc::new(Notify::new());
        let held_started = held_body_started.clone();
        let short_body_eof = Arc::new(Notify::new());
        let short_eof = short_body_eof.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    incoming = listener.accept() => {
                        let (socket, _) = incoming.expect("accept peer connection");
                        let acceptor = acceptor.clone();
                        let accepted = accepted.clone();
                        let held_started = held_started.clone();
                        let short_eof = short_eof.clone();
                        tasks.spawn(async move {
                            let socket = acceptor.accept(socket).await.expect("TLS handshake");
                            accepted.fetch_add(1, Ordering::SeqCst);
                            let is_h2 = socket.get_ref().1.alpn_protocol() == Some(b"h2".as_slice());
                            let service = service_fn(move |request: Request<Incoming>| {
                                let held_started = held_started.clone();
                                let short_eof = short_eof.clone();
                                async move {
                                    let path = request.uri().path();
                                    let held = path == "/held";
                                    let bounded = path == "/bounded";
                                    let short = path == "/short";
                                    let delay_eof = bounded || path == "/unframed";
                                    let body_stream = stream::once(async move {
                                        if held {
                                            held_started.notify_one();
                                        }
                                        let length = if short { PAYLOAD_SIZE / 2 } else { PAYLOAD_SIZE };
                                        Ok::<_, Infallible>(Bytes::from(vec![0x5a; length]))
                                    });
                                    let body = if delay_eof {
                                        Body::from_stream(body_stream.chain(stream::pending()))
                                    } else if short {
                                        Body::from_stream(body_stream.chain(stream::once(async move {
                                            short_eof.notified().await;
                                            Ok::<_, Infallible>(Bytes::new())
                                        })))
                                    } else {
                                        Body::from_stream(body_stream)
                                    };
                                    let mut response = Response::new(body);
                                    if bounded || short {
                                        response.headers_mut().insert(http::header::CONTENT_LENGTH, http::HeaderValue::from(PAYLOAD_SIZE));
                                    }
                                    Ok::<_, Infallible>(response)
                                }
                            });
                            if is_h2 {
                                let _ = http2::Builder::new(TokioExecutor::new()).serve_connection(TokioIo::new(socket), service).await;
                            } else {
                                let _ = http1::Builder::new().serve_connection(TokioIo::new(socket), service).await;
                            }
                        });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {}
                }
            }
        });
        Self {
            url,
            outbound_tls: rustfs_tls_runtime::GlobalPublishedOutboundTlsState {
                generation: rustfs_tls_runtime::TlsGeneration(0),
                root_ca_pem: Some(identity.cert.pem().into_bytes()),
                mtls_identity: None,
            },
            connections,
            held_body_started,
            short_body_eof,
            task,
        }
    }

    async fn client(&self, protocol: InternodeHttpProtocol) -> Client {
        let mut tuning = InternodeHttpClientTuning::from_values(
            InternodeHttpTuningProfile::Legacy,
            None,
            None,
            Some(256 * 1024),
            Some(65_535),
            false,
            None,
        );
        tuning.protocol = protocol;
        build_http_client(true, tuning, &self.outbound_tls)
            .await
            .expect("build production TLS client")
    }
}

#[test]
fn protocol_defaults_to_http1_and_requires_explicit_auto() {
    for value in [None, Some("http1"), Some(" HTTP1 "), Some("h2"), Some("invalid"), Some("")] {
        assert_eq!(parse_internode_http_protocol(value), InternodeHttpProtocol::Http1, "input: {value:?}");
    }
    assert_eq!(parse_internode_http_protocol(Some(" AUTO ")), InternodeHttpProtocol::Auto);
    let tuning = InternodeHttpClientTuning::from_values(InternodeHttpTuningProfile::Legacy, None, None, None, None, false, None);
    assert_eq!(tuning.protocol, InternodeHttpProtocol::Http1);
}

#[tokio::test]
async fn http1_pool_isolates_paused_tls_bodies_and_reuses_connections() {
    time::timeout(Duration::from_secs(10), async {
        let peer = TlsPeer::start().await;
        let client = peer.client(parse_internode_http_protocol(None)).await;
        let held = client
            .get(format!("{}/held", peer.url))
            .send()
            .await
            .expect("held response headers");
        assert_eq!(held.version(), Version::HTTP_11);
        peer.held_body_started.notified().await;
        for _ in 0..3 {
            let response = client.get(&peer.url).send().await.expect("active response headers");
            assert_eq!(response.version(), Version::HTTP_11);
            assert_eq!(response.bytes().await.expect("complete active body"), vec![0x5a; PAYLOAD_SIZE]);
        }
        assert_eq!(
            peer.connections.load(Ordering::SeqCst),
            2,
            "one held socket plus one reused active socket"
        );
        drop(held);
    })
    .await
    .expect("paused body must not block independent HTTP/1.1 bodies");
}

#[tokio::test]
async fn auto_h2_shares_receive_credit_until_paused_body_is_dropped() {
    time::timeout(Duration::from_secs(10), async {
        let peer = TlsPeer::start().await;
        let client = peer.client(InternodeHttpProtocol::Auto).await;
        let held = client
            .get(format!("{}/held", peer.url))
            .send()
            .await
            .expect("held response headers");
        assert_eq!(held.version(), Version::HTTP_2);
        peer.held_body_started.notified().await;
        let mut active = client
            .get(&peer.url)
            .send()
            .await
            .expect("HTTP/2 headers need no DATA credit");
        assert_eq!(active.version(), Version::HTTP_2);
        let mut received = Vec::new();
        let blocked = time::timeout(Duration::from_secs(1), async {
            while let Some(chunk) = active.chunk().await.expect("active stream before release") {
                received.extend_from_slice(&chunk);
            }
        })
        .await;
        assert!(blocked.is_err(), "fixture must reproduce shared HTTP/2 receive-credit starvation");
        assert_eq!(
            peer.connections.load(Ordering::SeqCst),
            1,
            "HTTP/2 bodies must share one pooled connection"
        );
        drop(held);
        while let Some(chunk) = active.chunk().await.expect("active stream after release") {
            received.extend_from_slice(&chunk);
        }
        assert_eq!(received, vec![0x5a; PAYLOAD_SIZE], "dropping the held stream must restore exact delivery");
    })
    .await
    .expect("releasing held credit must restore HTTP/2 progress");
}

#[tokio::test]
async fn exact_length_framing_reuses_tls_connections_without_polling_eof() {
    time::timeout(Duration::from_secs(10), async {
        for (path, expected_connections) in [("unframed", 3), ("bounded", 1)] {
            let peer = TlsPeer::start().await;
            let client = peer.client(InternodeHttpProtocol::Http1).await;
            for _ in 0..3 {
                let mut response = client
                    .get(format!("{}/{path}", peer.url))
                    .send()
                    .await
                    .expect("bounded response headers");
                assert_eq!(response.version(), Version::HTTP_11);
                let mut received = Vec::new();
                while received.len() < PAYLOAD_SIZE {
                    let chunk = response
                        .chunk()
                        .await
                        .expect("bounded body chunk")
                        .expect("body must reach the requested length");
                    received.extend_from_slice(&chunk);
                }
                assert_eq!(received, vec![0x5a; PAYLOAD_SIZE]);
                // Match the EC consumer: stop at the known length without an EOF poll.
                drop(response);
            }
            assert_eq!(peer.connections.load(Ordering::SeqCst), expected_connections, "framing: {path}");
        }
    })
    .await
    .expect("exact framing must permit idle reuse even when stream EOF is pending");
}

#[tokio::test]
async fn exact_length_framing_rejects_short_tls_bodies() {
    time::timeout(Duration::from_secs(10), async {
        let peer = TlsPeer::start().await;
        let client = peer.client(InternodeHttpProtocol::Http1).await;
        let mut response = client
            .get(format!("{}/short", peer.url))
            .send()
            .await
            .expect("short response headers");
        assert_eq!(response.version(), Version::HTTP_11);
        assert_eq!(response.content_length(), Some(u64::try_from(PAYLOAD_SIZE).expect("test payload length")));
        let first = response
            .chunk()
            .await
            .expect("initial partial body")
            .expect("partial body must arrive");
        assert!(!first.is_empty());
        peer.short_body_eof.notify_one();
        assert!(
            response.bytes().await.is_err(),
            "a truncated fixed-length body must not become successful EOF"
        );
    })
    .await
    .expect("short fixed-length body must terminate with an error");
}
