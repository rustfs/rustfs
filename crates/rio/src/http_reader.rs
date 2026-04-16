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

use crate::{EtagResolvable, HashReaderDetector, HashReaderMut};
use bytes::{Bytes, BytesMut};
use futures::{Stream, TryStreamExt as _};
use http::HeaderMap;
use pin_project_lite::pin_project;
use reqwest::{Certificate, Client, Identity, Method, RequestBuilder};
use rustfs_common::internode_metrics::global_internode_metrics;
use rustfs_utils::get_env_opt_str;
use std::io::IoSlice;
use std::io::{self, Error};
use std::net::IpAddr;
use std::ops::Not as _;
use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio::time::{self, Sleep};
use tokio_util::io::StreamReader;
use tokio_util::sync::PollSender;
use tracing::error;

/// Get the TLS path from the RUSTFS_TLS_PATH environment variable.
/// If the variable is not set, return None.
fn tls_path() -> Option<&'static std::path::PathBuf> {
    static TLS_PATH: LazyLock<Option<std::path::PathBuf>> =
        LazyLock::new(|| get_env_opt_str("RUSTFS_TLS_PATH").and_then(|s| if s.is_empty() { None } else { Some(s.into()) }));
    TLS_PATH.as_ref()
}

/// Load CA root certificates from the RUSTFS_TLS_PATH directory.
/// The CA certificates should be in PEM format and stored in the file
/// specified by the RUSTFS_CA_CERT constant.
/// If the file does not exist or cannot be read, return the builder unchanged.
fn load_ca_roots_from_tls_path(builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    let Some(tp) = tls_path() else {
        return builder;
    };
    let ca_path = tp.join(rustfs_config::RUSTFS_CA_CERT);
    if !ca_path.exists() {
        return builder;
    }

    let Ok(certs_der) = rustfs_utils::load_cert_bundle_der_bytes(ca_path.to_str().unwrap_or_default()) else {
        return builder;
    };

    let mut b = builder;
    for der in certs_der {
        if let Ok(cert) = Certificate::from_der(&der) {
            b = b.add_root_certificate(cert);
        }
    }
    b
}

/// Load optional mTLS identity from the RUSTFS_TLS_PATH directory.
/// The client certificate and private key should be in PEM format and stored in the files
/// specified by RUSTFS_CLIENT_CERT_FILENAME and RUSTFS_CLIENT_KEY_FILENAME constants.
/// If the files do not exist or cannot be read, return None.
fn load_optional_mtls_identity_from_tls_path() -> Option<Identity> {
    let tp = tls_path()?;
    let cert = std::fs::read(tp.join(rustfs_config::RUSTFS_CLIENT_CERT_FILENAME)).ok()?;
    let key = std::fs::read(tp.join(rustfs_config::RUSTFS_CLIENT_KEY_FILENAME)).ok()?;

    let mut pem = Vec::with_capacity(cert.len() + key.len() + 1);
    pem.extend_from_slice(&cert);
    if !pem.ends_with(b"\n") {
        pem.push(b'\n');
    }
    pem.extend_from_slice(&key);

    match Identity::from_pem(&pem) {
        Ok(id) => Some(id),
        Err(e) => {
            error!("Failed to load mTLS identity from PEM: {e}");
            None
        }
    }
}

fn build_http_client(disable_proxy: bool) -> Client {
    let mut builder = Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .tcp_keepalive(std::time::Duration::from_secs(10))
        .http2_keep_alive_interval(std::time::Duration::from_secs(5))
        .http2_keep_alive_timeout(std::time::Duration::from_secs(3))
        .http2_keep_alive_while_idle(true);

    if disable_proxy {
        builder = builder.no_proxy();
    }

    builder = load_ca_roots_from_tls_path(builder);
    if let Some(id) = load_optional_mtls_identity_from_tls_path() {
        builder = builder.identity(id);
    }

    builder.build().expect("Failed to create global HTTP client")
}

fn should_bypass_proxy_for_url(url: &str) -> bool {
    let Some(host) = reqwest::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
    else {
        return false;
    };
    let host = host.trim_matches(['[', ']']);

    host.eq_ignore_ascii_case("localhost") || host.parse::<IpAddr>().is_ok_and(|addr| addr.is_loopback())
}

fn get_http_client(url: &str) -> Client {
    // Reuse HTTP connection pools while keeping loopback traffic away from
    // system proxies so local RPC/tests do not leak to proxy listeners.
    static CLIENT: LazyLock<Client> = LazyLock::new(|| build_http_client(false));
    static LOCAL_CLIENT: LazyLock<Client> = LazyLock::new(|| build_http_client(true));

    if should_bypass_proxy_for_url(url) {
        return LOCAL_CLIENT.clone();
    }

    CLIENT.clone()
}

pin_project! {
    pub struct HttpReader {
        url:String,
        method: Method,
        headers: HeaderMap,
        track_internode_metrics: bool,
        stall_timeout: Option<Duration>,
        stall_timer: Option<Pin<Box<Sleep>>>,
        #[pin]
        inner: StreamReader<Pin<Box<dyn Stream<Item=std::io::Result<Bytes>>+Send+Sync>>, Bytes>,
    }
}

impl HttpReader {
    pub async fn new(url: String, method: Method, headers: HeaderMap, body: Option<Vec<u8>>) -> io::Result<Self> {
        // http_log!("[HttpReader::new] url: {url}, method: {method:?}, headers: {headers:?}");
        Self::with_capacity_and_stall_timeout(url, method, headers, body, 0, None).await
    }

    pub async fn new_with_stall_timeout(
        url: String,
        method: Method,
        headers: HeaderMap,
        body: Option<Vec<u8>>,
        stall_timeout: Option<Duration>,
    ) -> io::Result<Self> {
        Self::with_capacity_and_stall_timeout(url, method, headers, body, 0, stall_timeout).await
    }

    /// Create a new HttpReader from a URL. The request is performed immediately.
    pub async fn with_capacity(
        url: String,
        method: Method,
        headers: HeaderMap,
        body: Option<Vec<u8>>,
        _read_buf_size: usize,
    ) -> io::Result<Self> {
        Self::with_capacity_and_stall_timeout(url, method, headers, body, _read_buf_size, None).await
    }

    async fn with_capacity_and_stall_timeout(
        url: String,
        method: Method,
        headers: HeaderMap,
        body: Option<Vec<u8>>,
        _read_buf_size: usize,
        stall_timeout: Option<Duration>,
    ) -> io::Result<Self> {
        let track_internode_metrics = is_internode_rpc_url(&url);
        let client = get_http_client(&url);
        let mut request: RequestBuilder = client.request(method.clone(), url.clone()).headers(headers.clone());
        if let Some(body) = body {
            request = request.body(body);
        }

        let resp = request.send().await.map_err(|e| {
            if track_internode_metrics {
                global_internode_metrics().record_error();
            }
            Error::other(format!("HttpReader HTTP request error: {e}"))
        })?;

        if resp.status().is_success().not() {
            if track_internode_metrics {
                global_internode_metrics().record_error();
            }
            return Err(Error::other(format!(
                "HttpReader HTTP request failed with non-200 status {}",
                resp.status()
            )));
        }

        if track_internode_metrics {
            global_internode_metrics().record_outgoing_request();
        }

        let stream = resp.bytes_stream().map_err(move |e| {
            if track_internode_metrics {
                global_internode_metrics().record_error();
            }
            Error::other(format!("HttpReader stream error: {e}"))
        });

        Ok(Self {
            inner: StreamReader::new(Box::pin(stream)),
            url,
            method,
            headers,
            track_internode_metrics,
            stall_timer: stall_timeout.map(|timeout| Box::pin(time::sleep(timeout))),
            stall_timeout,
        })
    }
    pub fn url(&self) -> &str {
        &self.url
    }
    pub fn method(&self) -> &Method {
        &self.method
    }
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

impl AsyncRead for HttpReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();

        let filled_before = buf.filled().len();
        match this.inner.as_mut().poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let bytes_read = buf.filled().len().saturating_sub(filled_before);
                if *this.track_internode_metrics && bytes_read > 0 {
                    global_internode_metrics().record_recv_bytes(bytes_read);
                }
                if bytes_read > 0 {
                    if let Some(stall_timeout) = *this.stall_timeout {
                        *this.stall_timer = Some(Box::pin(time::sleep(stall_timeout)));
                    }
                } else {
                    *this.stall_timer = None;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                if let Some(timer) = this.stall_timer.as_mut()
                    && timer.as_mut().poll(cx).is_ready()
                {
                    if *this.track_internode_metrics {
                        global_internode_metrics().record_error();
                    }
                    Poll::Ready(Err(Error::new(
                        io::ErrorKind::TimedOut,
                        "HttpReader stall timeout: no data received before deadline",
                    )))
                } else {
                    Poll::Pending
                }
            }
            other => other,
        }
    }
}

impl EtagResolvable for HttpReader {
    fn is_etag_reader(&self) -> bool {
        false
    }
    fn try_resolve_etag(&mut self) -> Option<String> {
        None
    }
}

impl HashReaderDetector for HttpReader {
    fn is_hash_reader(&self) -> bool {
        false
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        None
    }
}

struct ReceiverStream {
    receiver: mpsc::Receiver<Option<Bytes>>,
    track_internode_metrics: bool,
}

impl Stream for ReceiverStream {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = Pin::new(&mut self.receiver).poll_recv(cx);
        // match &poll {
        //     Poll::Ready(Some(Some(bytes))) => {
        //         // http_log!("[ReceiverStream] poll_next: got {} bytes", bytes.len());
        //     }
        //     Poll::Ready(Some(None)) => {
        //         // http_log!("[ReceiverStream] poll_next: sender shutdown");
        //     }
        //     Poll::Ready(None) => {
        //         // http_log!("[ReceiverStream] poll_next: channel closed");
        //     }
        //     Poll::Pending => {
        //         // http_log!("[ReceiverStream] poll_next: pending");
        //     }
        // }
        match poll {
            Poll::Ready(Some(Some(bytes))) => {
                if self.track_internode_metrics {
                    global_internode_metrics().record_sent_bytes(bytes.len());
                }
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(None)) => Poll::Ready(None), // Sender shutdown
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    pub struct HttpWriter {
        url:String,
        method: Method,
        headers: HeaderMap,
        err_rx: tokio::sync::oneshot::Receiver<std::io::Error>,
        sender: PollSender<Option<Bytes>>,
        handle: tokio::task::JoinHandle<std::io::Result<()>>,
        pending_chunk: BytesMut,
        finish:bool,

    }
}

const HTTP_WRITER_CHANNEL_CAPACITY: usize = 8;
const HTTP_WRITER_BUFFER_SIZE: usize = 1024 * 1024;

impl HttpWriter {
    /// Create a new HttpWriter for the given URL. The HTTP request is performed in the background.
    pub async fn new(url: String, method: Method, headers: HeaderMap) -> io::Result<Self> {
        // http_log!("[HttpWriter::new] url: {url}, method: {method:?}, headers: {headers:?}");
        let url_clone = url.clone();
        let method_clone = method.clone();
        let headers_clone = headers.clone();
        let track_internode_metrics = is_internode_rpc_url(&url);

        let (sender, receiver) = tokio::sync::mpsc::channel::<Option<Bytes>>(HTTP_WRITER_CHANNEL_CAPACITY);
        let (err_tx, err_rx) = tokio::sync::oneshot::channel::<io::Error>();

        let handle = tokio::spawn(async move {
            let stream = ReceiverStream {
                receiver,
                track_internode_metrics,
            };
            let body = reqwest::Body::wrap_stream(stream);
            // http_log!(
            //     "[HttpWriter::spawn] sending HTTP request: url={url_clone}, method={method_clone:?}, headers={headers_clone:?}"
            // );

            let client = get_http_client(&url_clone);
            let request = client
                .request(method_clone, url_clone.clone())
                .headers(headers_clone.clone())
                .body(body);

            // Hold the request until the shutdown signal is received
            let response = request.send().await;

            match response {
                Ok(resp) => {
                    // http_log!("[HttpWriter::spawn] got response: status={}", resp.status());
                    if !resp.status().is_success() {
                        if track_internode_metrics {
                            global_internode_metrics().record_error();
                        }
                        let _ = err_tx.send(Error::other(format!(
                            "HttpWriter HTTP request failed with non-200 status {}",
                            resp.status()
                        )));
                        return Err(Error::other(format!("HTTP request failed with non-200 status {}", resp.status())));
                    }
                }
                Err(e) => {
                    if track_internode_metrics {
                        global_internode_metrics().record_error();
                    }
                    // http_log!("[HttpWriter::spawn] HTTP request error: {e}");
                    let _ = err_tx.send(Error::other(format!("HTTP request failed: {e}")));
                    return Err(Error::other(format!("HTTP request failed: {e}")));
                }
            }

            // http_log!("[HttpWriter::spawn] HTTP request completed, exiting");
            Ok(())
        });

        // http_log!("[HttpWriter::new] connection established successfully");
        if track_internode_metrics {
            global_internode_metrics().record_outgoing_request();
        }
        Ok(Self {
            url,
            method,
            headers,
            err_rx,
            sender: PollSender::new(sender),
            handle,
            pending_chunk: BytesMut::with_capacity(HTTP_WRITER_BUFFER_SIZE),
            finish: false,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

fn is_internode_rpc_url(url: &str) -> bool {
    url.contains("/rustfs/rpc/")
}

fn poll_send_error_to_io<T>(err: tokio_util::sync::PollSendError<T>, context: &str) -> io::Error {
    Error::other(format!("{context}: {err}"))
}

fn send_error_to_io<T>(err: tokio_util::sync::PollSendError<T>, context: &str) -> io::Error {
    Error::other(format!("{context}: {err}"))
}

impl HttpWriter {
    fn poll_send_pending_chunk(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.pending_chunk.is_empty() {
            return Poll::Ready(Ok(()));
        }

        match self.sender.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let chunk = self.pending_chunk.split().freeze();
                self.sender
                    .send_item(Some(chunk))
                    .map_err(|e| send_error_to_io(e, "HttpWriter send error"))?;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(poll_send_error_to_io(e, "HttpWriter send error"))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for HttpWriter {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        // http_log!(
        //     "[HttpWriter::poll_write] url: {}, method: {:?}, buf.len: {}",
        //     self.url,
        //     self.method,
        //     buf.len()
        // );
        if let Ok(e) = Pin::new(&mut self.err_rx).try_recv() {
            return Poll::Ready(Err(e));
        }

        let this = self.as_mut().get_mut();

        if this.pending_chunk.len() >= HTTP_WRITER_BUFFER_SIZE {
            match this.poll_send_pending_chunk(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            }
        }

        if buf.len() >= HTTP_WRITER_BUFFER_SIZE && this.pending_chunk.is_empty() {
            match this.sender.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {
                    this.sender
                        .send_item(Some(Bytes::copy_from_slice(buf)))
                        .map_err(|e| send_error_to_io(e, "HttpWriter send error"))?;
                    return Poll::Ready(Ok(buf.len()));
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(poll_send_error_to_io(err, "HttpWriter send error"))),
                Poll::Pending => return Poll::Pending,
            }
        }

        this.pending_chunk.extend_from_slice(buf);

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.as_mut().get_mut().poll_send_pending_chunk(cx)
    }

    fn poll_write_vectored(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bufs: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        if let Ok(e) = Pin::new(&mut self.err_rx).try_recv() {
            return Poll::Ready(Err(e));
        }

        let this = self.as_mut().get_mut();

        if this.pending_chunk.len() >= HTTP_WRITER_BUFFER_SIZE {
            match this.poll_send_pending_chunk(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            }
        }

        let total_len = bufs.iter().map(|buf| buf.len()).sum::<usize>();
        if total_len == 0 {
            return Poll::Ready(Ok(0));
        }

        if bufs.len() == 1 && this.pending_chunk.is_empty() && total_len >= HTTP_WRITER_BUFFER_SIZE {
            match this.sender.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {
                    this.sender
                        .send_item(Some(Bytes::copy_from_slice(bufs[0].as_ref())))
                        .map_err(|e| send_error_to_io(e, "HttpWriter send error"))?;
                    return Poll::Ready(Ok(total_len));
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(poll_send_error_to_io(err, "HttpWriter send error"))),
                Poll::Pending => return Poll::Pending,
            }
        }

        for buf in bufs {
            this.pending_chunk.extend_from_slice(buf);
        }

        Poll::Ready(Ok(total_len))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // let url = self.url.clone();
        // let method = self.method.clone();

        match self.as_mut().get_mut().poll_send_pending_chunk(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }

        if !self.finish {
            // http_log!("[HttpWriter::poll_shutdown] url: {}, method: {:?}", url, method);
            let this = self.as_mut().get_mut();
            match this.sender.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {
                    this.sender
                        .send_item(None)
                        .map_err(|e| send_error_to_io(e, "HttpWriter shutdown error"))?;
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(poll_send_error_to_io(err, "HttpWriter shutdown error"))),
                Poll::Pending => return Poll::Pending,
            }
            // http_log!(
            //     "[HttpWriter::poll_shutdown] sent shutdown signal to HTTP request, url: {}, method: {:?}",
            //     url,
            //     method
            // );

            self.finish = true;
        }
        // Wait for the HTTP request to complete
        use futures::FutureExt;
        match Pin::new(&mut self.get_mut().handle).poll_unpin(cx) {
            Poll::Ready(Ok(_)) => {
                // http_log!(
                //     "[HttpWriter::poll_shutdown] HTTP request finished successfully, url: {}, method: {:?}",
                //     url,
                //     method
                // );
            }
            Poll::Ready(Err(e)) => {
                // http_log!("[HttpWriter::poll_shutdown] HTTP request failed: {e}, url: {}, method: {:?}", url, method);
                return Poll::Ready(Err(Error::other(format!("HTTP request failed: {e}"))));
            }
            Poll::Pending => {
                // http_log!("[HttpWriter::poll_shutdown] HTTP request pending, url: {}, method: {:?}", url, method);
                return Poll::Pending;
            }
        }

        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, extract::State, http::StatusCode, response::IntoResponse, routing::get};
    use futures::stream::{self, StreamExt as _};
    use http_body_util::BodyExt as _;
    use std::io::{self, IoSlice};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Mutex,
    };

    #[derive(Clone, Default)]
    struct TestState {
        head_count: Arc<AtomicUsize>,
        get_count: Arc<AtomicUsize>,
        put_count: Arc<AtomicUsize>,
        put_bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    async fn get_stream(State(state): State<TestState>) -> impl IntoResponse {
        state.get_count.fetch_add(1, Ordering::SeqCst);
        (StatusCode::OK, Body::from("hello"))
    }

    async fn get_stalling_stream(State(state): State<TestState>) -> impl IntoResponse {
        state.get_count.fetch_add(1, Ordering::SeqCst);
        let body_stream = stream::once(async { Ok::<Bytes, io::Error>(Bytes::from_static(b"hello")) }).chain(stream::pending());
        (StatusCode::OK, Body::from_stream(body_stream))
    }

    async fn reject_head(State(state): State<TestState>) -> impl IntoResponse {
        state.head_count.fetch_add(1, Ordering::SeqCst);
        StatusCode::METHOD_NOT_ALLOWED
    }

    async fn accept_put(State(state): State<TestState>, body: Body) -> impl IntoResponse {
        state.put_count.fetch_add(1, Ordering::SeqCst);
        let bytes = body.collect().await.unwrap().to_bytes();
        state.put_bodies.lock().await.push(bytes.to_vec());
        StatusCode::OK
    }

    async fn start_test_server(state: TestState) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/stream", get(get_stream).head(reject_head).put(accept_put))
            .route("/stall", get(get_stalling_stream))
            .with_state(state);

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://{addr}/stream"), handle)
    }

    #[tokio::test]
    async fn http_reader_does_not_send_preflight_head() {
        let state = TestState::default();
        let (url, handle) = start_test_server(state.clone()).await;

        let mut reader = HttpReader::new(url, Method::GET, HeaderMap::new(), None).await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, b"hello");
        assert_eq!(state.head_count.load(Ordering::SeqCst), 0);
        assert_eq!(state.get_count.load(Ordering::SeqCst), 1);

        handle.abort();
    }

    #[tokio::test]
    async fn http_reader_stall_timeout_triggers_after_progress_stops() {
        let state = TestState::default();
        let (base_url, handle) = start_test_server(state.clone()).await;
        let url = base_url.replace("/stream", "/stall");

        let mut reader =
            HttpReader::new_with_stall_timeout(url, Method::GET, HeaderMap::new(), None, Some(Duration::from_millis(20)))
                .await
                .unwrap();

        let mut first = [0u8; 5];
        reader.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, b"hello");

        let mut next = [0u8; 1];
        let err = tokio::time::timeout(Duration::from_secs(1), reader.read(&mut next))
            .await
            .expect("stall timeout should wake reader")
            .expect_err("reader should return a timeout error");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);

        handle.abort();
    }

    #[tokio::test]
    async fn http_writer_does_not_send_empty_preflight_put() {
        let state = TestState::default();
        let (url, handle) = start_test_server(state.clone()).await;

        let mut writer = HttpWriter::new(url, Method::PUT, HeaderMap::new()).await.unwrap();
        writer.write_all(b"payload").await.unwrap();
        writer.shutdown().await.unwrap();

        assert_eq!(state.put_count.load(Ordering::SeqCst), 1);
        assert_eq!(state.put_bodies.lock().await.as_slice(), &[b"payload".to_vec()]);

        handle.abort();
    }

    #[tokio::test]
    async fn http_writer_handles_many_small_writes() {
        let state = TestState::default();
        let (url, handle) = start_test_server(state.clone()).await;

        let mut writer = HttpWriter::new(url, Method::PUT, HeaderMap::new()).await.unwrap();
        let chunk = b"0123456789abcdef";
        let mut expected = Vec::new();
        for _ in 0..256 {
            writer.write_all(chunk).await.unwrap();
            expected.extend_from_slice(chunk);
        }
        writer.shutdown().await.unwrap();

        assert_eq!(state.put_count.load(Ordering::SeqCst), 1);
        assert_eq!(state.put_bodies.lock().await.as_slice(), &[expected]);

        handle.abort();
    }

    #[tokio::test]
    async fn http_writer_supports_vectored_writes() {
        let state = TestState::default();
        let (url, handle) = start_test_server(state.clone()).await;

        let mut writer = HttpWriter::new(url, Method::PUT, HeaderMap::new()).await.unwrap();
        let bufs = [IoSlice::new(b"hello "), IoSlice::new(b"world")];
        let written = writer.write_vectored(&bufs).await.unwrap();
        assert_eq!(written, 11);
        writer.shutdown().await.unwrap();

        assert_eq!(state.put_count.load(Ordering::SeqCst), 1);
        assert_eq!(state.put_bodies.lock().await.as_slice(), &[b"hello world".to_vec()]);

        handle.abort();
    }

    #[test]
    fn loopback_urls_bypass_proxy_selection() {
        assert!(should_bypass_proxy_for_url("http://127.0.0.1:9000/stream"));
        assert!(should_bypass_proxy_for_url("http://localhost:9000/stream"));
        assert!(should_bypass_proxy_for_url("http://[::1]:9000/stream"));
        assert!(!should_bypass_proxy_for_url("http://192.168.1.10:9000/stream"));
        assert!(!should_bypass_proxy_for_url("http://example.com/stream"));
        assert!(!should_bypass_proxy_for_url("not-a-url"));
    }
}
