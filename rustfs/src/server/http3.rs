// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2023-2026 The s3s Authors
// Copyright 2026 RustFS Team

use bytes::{Buf, Bytes};
use h3::error::Code;
use h3::server::{RequestResolver, RequestStream};
use http::{HeaderMap, HeaderName, Request, Response, Version, header};
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::BodyExt;
use quinn::{Endpoint, VarInt};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{Semaphore, broadcast};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) type RequestBody = http_body_util::combinators::UnsyncBoxBody<Bytes, io::Error>;

type Resolver = RequestResolver<h3_quinn::Connection, Bytes>;

pub(crate) fn spawn<M, S, B, E>(
    mut server_config: quinn::ServerConfig,
    bind_addr: SocketAddr,
    make_service: M,
    max_connections: usize,
    body_timeout: Duration,
    mut shutdown: broadcast::Receiver<()>,
) -> io::Result<(Endpoint, JoinHandle<()>)>
where
    M: Fn(SocketAddr) -> S + Clone + Send + 'static,
    S: Service<Request<RequestBody>, Response = Response<B>, Error = E> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Send,
    E: Send + 'static,
{
    // The service's peer address must remain valid for the connection lifetime.
    server_config.migration(false);
    let endpoint = Endpoint::server(server_config, bind_addr)?;
    let server_endpoint = endpoint.clone();
    let limiter = (max_connections > 0).then(|| Arc::new(Semaphore::new(max_connections.min(Semaphore::MAX_PERMITS))));
    let task = tokio::spawn(async move {
        let cancellation = CancellationToken::new();
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.recv() => break,
                _ = connections.join_next(), if !connections.is_empty() => {},
                incoming = server_endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    let permit = match &limiter {
                        Some(limiter) => match limiter.clone().try_acquire_owned() {
                            Ok(permit) => Some(permit),
                            Err(_) => { incoming.refuse(); continue; }
                        },
                        None => None,
                    };
                    let make_service = make_service.clone();
                    let cancellation = cancellation.child_token();
                    connections.spawn(async move {
                        let _permit = permit;
                        let connection = tokio::select! {
                            result = incoming => match result { Ok(connection) => connection, Err(_) => return },
                            _ = cancellation.cancelled() => return,
                        };
                        let service = make_service(connection.remote_address());
                        let _ = serve_connection(connection, service, body_timeout, cancellation).await;
                    });
                }
            }
        }
        server_endpoint.set_server_config(None);
        cancellation.cancel();
        let drain = async { while connections.join_next().await.is_some() {} };
        let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT, drain).await;
        server_endpoint.close(VarInt::from_u32(0), b"server shutdown");
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    });
    Ok((endpoint, task))
}

async fn serve_connection<S, B, E>(
    quic: quinn::Connection,
    service: S,
    body_timeout: Duration,
    cancellation: CancellationToken,
) -> std::result::Result<(), h3::error::ConnectionError>
where
    S: Service<Request<RequestBody>, Response = Response<B>, Error = E> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Send,
    E: Send + 'static,
{
    let mut connection = h3::server::builder()
        .max_field_section_size(u64::from(rustfs_config::DEFAULT_H2_MAX_HEADER_LIST_SIZE))
        .build(h3_quinn::Connection::new(quic.clone()))
        .await?;
    let mut requests = JoinSet::new();
    let mut shutting_down = false;
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled(), if !shutting_down => {
                shutting_down = true;
                connection.shutdown(0).await?;
            },
            _ = requests.join_next(), if !requests.is_empty() => {},
            result = connection.accept() => match result? {
                Some(resolver) => { requests.spawn(handle_request(resolver, service.clone(), body_timeout)); }
                None => break,
            },
        }
    }
    while requests.join_next().await.is_some() {}
    // finish() queues bytes; keep QUIC alive until the peer has consumed them.
    // The listener's shutdown deadline bounds peers that never close.
    let _ = quic.closed().await;
    Ok(())
}

async fn handle_request<S, B, E>(resolver: Resolver, service: S, body_timeout: Duration)
where
    S: Service<Request<RequestBody>, Response = Response<B>, Error = E> + Send,
    S::Future: Send,
    B: HttpBody<Data = Bytes> + Send,
    B::Error: Send,
{
    let Ok((request, stream)) = resolver.resolve_request().await else { return };
    let (mut send, recv) = stream.split();
    if disallowed_request_field(request.headers()).is_some() {
        send.stop_stream(Code::H3_MESSAGE_ERROR);
        return;
    }
    if super::has_path_prefix(request.uri().path(), super::RPC_PREFIX)
        || request
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value.as_bytes().starts_with(b"application/grpc"))
    {
        let response = Response::builder()
            .status(http::StatusCode::NOT_IMPLEMENTED)
            .body(())
            .expect("static response");
        if send.send_response(response).await.is_ok() {
            let _ = send
                .send_data(Bytes::from_static(b"Use the TCP listener for internode RPC"))
                .await;
            let _ = send.finish().await;
        }
        return;
    }
    let content_length = match request.headers().get(header::CONTENT_LENGTH) {
        Some(value) => match value.to_str().ok().and_then(|value| value.parse::<u64>().ok()) {
            Some(length) => Some(length),
            None => {
                send.stop_stream(Code::H3_MESSAGE_ERROR);
                return;
            }
        },
        None => None,
    };
    let request = request.map(|()| Body::new(recv, content_length, body_timeout).boxed_unsync());
    let Ok(response) = service.oneshot(request).await else {
        send.stop_stream(Code::H3_INTERNAL_ERROR);
        return;
    };
    let (mut parts, body) = response.into_parts();
    strip_hop_by_hop_headers(&mut parts.headers);
    parts.version = Version::HTTP_3;
    if send.send_response(Response::from_parts(parts, ())).await.is_err() {
        return;
    }
    let mut body = std::pin::pin!(body);
    while let Some(frame) = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        let Ok(frame) = frame else {
            send.stop_stream(Code::H3_INTERNAL_ERROR);
            return;
        };
        match frame.into_data() {
            Ok(data) => {
                if send.send_data(data).await.is_err() {
                    return;
                }
            }
            Err(frame) => {
                if let Ok(mut trailers) = frame.into_trailers() {
                    strip_hop_by_hop_headers(&mut trailers);
                    if send.send_trailers(trailers).await.is_err() {
                        return;
                    }
                    break;
                }
            }
        }
    }
    let _ = send.finish().await;
}

type RecvStream = RequestStream<h3_quinn::RecvStream, Bytes>;

enum State {
    Data,
    Trailers,
    Done,
}

// Dropping an unread body cancels the receive stream without a detached drain.
struct Body {
    stream: Option<RecvStream>,
    state: State,
    expected_length: Option<u64>,
    received_length: u64,
    timeout: Duration,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Body {
    fn new(stream: RecvStream, expected_length: Option<u64>, timeout: Duration) -> Self {
        Self {
            stream: Some(stream),
            state: State::Data,
            expected_length,
            received_length: 0,
            timeout,
            timer: None,
        }
    }

    fn pending(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Frame<Bytes>>>> {
        if self.timeout.is_zero() {
            return Poll::Pending;
        }
        let timer = self.timer.get_or_insert_with(|| Box::pin(tokio::time::sleep(self.timeout)));
        if std::future::Future::poll(timer.as_mut(), cx).is_pending() {
            return Poll::Pending;
        }
        // h3-quinn moves the receive stream into its pending read future.
        // Dropping it cancels that read and sends STOP_SENDING without panic.
        drop(self.stream.take());
        self.state = State::Done;
        Poll::Ready(Some(Err(io::Error::new(
            io::ErrorKind::TimedOut,
            crate::error::ClientBodyReadTimeout {
                timeout: self.timeout,
                raw_bytes_received: self.received_length,
            },
        ))))
    }
}

fn content_length_error(expected: u64, actual: u64) -> io::Error {
    io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("HTTP/3 request body length mismatch: expected {expected}, received {actual}"),
    )
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        loop {
            match this.state {
                State::Data => {
                    let stream = this.stream.as_mut().expect("data state owns the receive stream");

                    match stream.poll_recv_data(cx) {
                        Poll::Pending => return this.pending(cx),
                        Poll::Ready(Err(error)) => {
                            this.state = State::Done;
                            return Poll::Ready(Some(Err(io::Error::other(error))));
                        }
                        Poll::Ready(Ok(Some(mut data))) => {
                            let data = data.copy_to_bytes(data.remaining());
                            if !data.is_empty() {
                                this.timer = None;
                            }
                            let Some(received) = this
                                .received_length
                                .checked_add(u64::try_from(data.len()).expect("Bytes length fits in u64"))
                            else {
                                stream.stop_sending(Code::H3_MESSAGE_ERROR);
                                this.state = State::Done;
                                return Poll::Ready(Some(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "HTTP/3 body length overflow",
                                ))));
                            };
                            if let Some(expected) = this.expected_length
                                && received > expected
                            {
                                stream.stop_sending(Code::H3_MESSAGE_ERROR);
                                this.state = State::Done;
                                return Poll::Ready(Some(Err(content_length_error(expected, received))));
                            }
                            this.received_length = received;
                            return Poll::Ready(Some(Ok(Frame::data(data))));
                        }
                        Poll::Ready(Ok(None)) => {
                            if let Some(expected) = this.expected_length
                                && this.received_length != expected
                            {
                                stream.stop_sending(Code::H3_MESSAGE_ERROR);
                                this.state = State::Done;
                                return Poll::Ready(Some(Err(content_length_error(expected, this.received_length))));
                            }
                            this.timer = None;
                            this.state = State::Trailers;
                        }
                    }
                }
                State::Trailers => {
                    let stream = this.stream.as_mut().expect("trailers state owns the receive stream");

                    match stream.poll_recv_trailers(cx) {
                        Poll::Pending => return this.pending(cx),
                        Poll::Ready(Err(error)) => {
                            this.state = State::Done;
                            return Poll::Ready(Some(Err(io::Error::other(error))));
                        }
                        Poll::Ready(Ok(Some(trailers))) => {
                            this.state = State::Done;
                            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
                        }
                        Poll::Ready(Ok(None)) => {
                            this.state = State::Done;
                            return Poll::Ready(None);
                        }
                    }
                }
                State::Done => return Poll::Ready(None),
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self.state, State::Done)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

fn disallowed_request_field(headers: &HeaderMap) -> Option<&'static str> {
    for name in ["connection", "transfer-encoding", "upgrade", "keep-alive", "proxy-connection"] {
        if headers.contains_key(name) {
            return Some(name);
        }
    }

    if headers
        .get_all(header::TE)
        .iter()
        .any(|value| !trim_ows(value.as_bytes()).eq_ignore_ascii_case(b"trailers"))
    {
        return Some("te");
    }

    None
}

/// Removes the optional whitespace (SP / HTAB) around a field value, which
/// RFC 9110 §5.5 requires before the value is evaluated. Other whitespace
/// characters are left in place: CR, LF, and NUL make a field value invalid
/// rather than equivalent.
fn trim_ows(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !matches!(*byte, b' ' | b'\t'))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !matches!(*byte, b' ' | b'\t'))
        .map_or(start, |index| index + 1);

    value.get(start..end).unwrap_or_default()
}

fn strip_hop_by_hop_headers(headers: &mut HeaderMap) {
    let connection_headers = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect::<Vec<_>>();

    for name in connection_headers {
        headers.remove(name);
    }

    for name in [
        header::CONNECTION,
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
        HeaderName::from_static("keep-alive"),
    ] {
        headers.remove(name);
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::convert::Infallible;

    pub(crate) type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
    pub(crate) type Client = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

    pub(crate) fn configs() -> TestResult<(quinn::ServerConfig, CertificateDer<'static>)> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
        let der = cert.cert.der().clone();
        let mut tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![der.clone()], PrivateKeyDer::try_from(cert.signing_key.serialize_der())?)?;
        tls.alpn_protocols = vec![b"h3".to_vec()];
        Ok((
            quinn::ServerConfig::with_crypto(Arc::new(quinn::crypto::rustls::QuicServerConfig::try_from(tls)?)),
            der,
        ))
    }

    pub(crate) fn client_endpoint(certs: Vec<CertificateDer<'static>>) -> TestResult<Endpoint> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in certs {
            roots.add(cert)?;
        }
        let mut tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let mut endpoint = Endpoint::client("127.0.0.1:0".parse()?)?;
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls)?,
        )));
        Ok(endpoint)
    }

    pub(crate) async fn connect(
        endpoint: &Endpoint,
        addr: SocketAddr,
    ) -> TestResult<(quinn::Connection, Client, JoinHandle<()>)> {
        let quic = tokio::time::timeout(Duration::from_secs(5), endpoint.connect(addr, "localhost")?).await??;
        let (mut driver, client) = h3::client::builder().build(h3_quinn::Connection::new(quic.clone())).await?;
        let task = tokio::spawn(async move {
            let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        });
        Ok((quic, client, task))
    }

    pub(crate) async fn request(
        client: &mut Client,
        request: Request<()>,
        data: Bytes,
    ) -> TestResult<(http::StatusCode, HeaderMap, Bytes)> {
        let mut stream = client.send_request(request).await?;
        if !data.is_empty() {
            stream.send_data(data).await?;
        }
        stream.finish().await?;
        let response = tokio::time::timeout(Duration::from_secs(5), stream.recv_response()).await??;
        let mut body = Vec::new();
        while let Some(mut chunk) = stream.recv_data().await? {
            body.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
        }
        Ok((response.status(), response.headers().clone(), Bytes::from(body)))
    }

    #[tokio::test]
    async fn http3_bounds_connections_and_releases_permits() -> TestResult {
        let (config, cert) = configs()?;
        let (shutdown, receiver) = broadcast::channel(1);
        let (endpoint, task) = spawn(
            config,
            "127.0.0.1:0".parse()?,
            |_| {
                tower::service_fn(|_: Request<RequestBody>| async {
                    Ok::<_, Infallible>(Response::new(http_body_util::Full::new(Bytes::from_static(b"bounded"))))
                })
            },
            1,
            Duration::from_secs(1),
            receiver,
        )?;
        let client_endpoint = client_endpoint(vec![cert])?;
        let addr = endpoint.local_addr()?;
        let (first, mut client, driver) = connect(&client_endpoint, addr).await?;
        assert_eq!(
            request(&mut client, Request::builder().uri("https://localhost/").body(())?, Bytes::new())
                .await?
                .2,
            "bounded"
        );
        let rejected = tokio::time::timeout(Duration::from_secs(5), client_endpoint.connect(addr, "localhost")?).await?;
        assert!(rejected.is_err(), "a second QUIC connection must be refused at the configured cap");
        first.close(0u32.into(), b"release permit");
        drop(client);
        driver.abort();
        tokio::time::timeout(Duration::from_secs(5), async {
            while endpoint.open_connections() > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        // Quinn releases the connection before its serving task releases the permit.
        let (second, mut client, driver) = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(connection) = connect(&client_endpoint, addr).await {
                    break connection;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(
            request(&mut client, Request::builder().uri("https://localhost/").body(())?, Bytes::new())
                .await?
                .0,
            http::StatusCode::OK
        );
        shutdown.send(())?;
        drop(client);
        second.close(0u32.into(), b"test complete");
        tokio::time::timeout(Duration::from_secs(5), task).await??;
        assert!(
            !matches!(
                tokio::time::timeout(Duration::from_millis(250), client_endpoint.connect(addr, "localhost")?).await,
                Ok(Ok(_))
            ),
            "shutdown must stop new handshakes"
        );
        driver.abort();
        client_endpoint.close(0u32.into(), b"test complete");
        Ok(())
    }

    #[tokio::test]
    async fn http3_streams_body_and_times_out_stalled_upload() -> TestResult {
        let (config, cert) = configs()?;
        let (shutdown, receiver) = broadcast::channel(1);
        let (endpoint, task) = spawn(
            config,
            "127.0.0.1:0".parse()?,
            |_| {
                tower::service_fn(|request: Request<RequestBody>| async {
                    let mut response = match request.into_body().collect().await {
                        Ok(body) => Response::new(http_body_util::Full::new(body.to_bytes())),
                        Err(_) => {
                            let mut response = Response::new(http_body_util::Full::new(Bytes::new()));
                            *response.status_mut() = http::StatusCode::REQUEST_TIMEOUT;
                            response
                        }
                    };
                    response
                        .headers_mut()
                        .insert(header::CONNECTION, http::HeaderValue::from_static("close"));
                    Ok::<_, Infallible>(response)
                })
            },
            0,
            Duration::from_secs(1),
            receiver,
        )?;
        let client_endpoint = client_endpoint(vec![cert])?;
        let (quic, mut client, driver) = connect(&client_endpoint, endpoint.local_addr()?).await?;
        let (status, _, body) = request(
            &mut client,
            Request::builder()
                .method("POST")
                .uri("https://localhost/node_service.NodeService/Ping")
                .header(header::CONTENT_TYPE, "application/grpc")
                .body(())?,
            Bytes::new(),
        )
        .await?;
        assert_eq!(status, http::StatusCode::NOT_IMPLEMENTED);
        assert_eq!(body, "Use the TCP listener for internode RPC");
        let payload = Bytes::from(vec![b'x'; 256 * 1024]);
        let (status, headers, body) = request(
            &mut client,
            Request::builder()
                .method("PUT")
                .uri("https://localhost/bucket/key")
                .header(header::CONTENT_LENGTH, payload.len())
                .body(())?,
            payload.clone(),
        )
        .await?;
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(body, payload, "the full streamed body must survive transport adaptation");
        assert!(!headers.contains_key(header::CONNECTION));
        let mut stalled = client
            .send_request(
                Request::builder()
                    .method("PUT")
                    .uri("https://localhost/bucket/key")
                    .header(header::CONTENT_LENGTH, 10)
                    .body(())?,
            )
            .await?;
        stalled.send_data(Bytes::from_static(b"x")).await?;
        let response = tokio::time::timeout(Duration::from_secs(5), stalled.recv_response()).await??;
        assert_eq!(response.status(), http::StatusCode::REQUEST_TIMEOUT);
        drop(stalled);
        let (_, _, body) = request(
            &mut client,
            Request::builder()
                .method("PUT")
                .uri("https://localhost/bucket/key")
                .body(())?,
            Bytes::from_static(b"after timeout"),
        )
        .await?;
        assert_eq!(body, "after timeout", "a canceled upload must leave the QUIC connection usable");
        shutdown.send(())?;
        drop(client);
        quic.close(0u32.into(), b"test complete");
        tokio::time::timeout(Duration::from_secs(5), task).await??;
        driver.abort();
        Ok(())
    }
}
