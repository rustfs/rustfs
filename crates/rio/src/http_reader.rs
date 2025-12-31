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
use bytes::Bytes;
use futures::{Stream, TryStreamExt as _};
use http::HeaderMap;
use pin_project_lite::pin_project;
use reqwest::{Certificate, Client, Identity, Method, RequestBuilder};
use std::error::Error as _;
use std::io::{self, Error};
use std::ops::Not as _;
use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio_util::io::StreamReader;
use tracing::error;

/// Get the TLS path from the RUSTFS_TLS_PATH environment variable.
/// If the variable is not set, return None.
fn tls_path() -> Option<&'static std::path::PathBuf> {
    static TLS_PATH: LazyLock<Option<std::path::PathBuf>> = LazyLock::new(|| {
        std::env::var("RUSTFS_TLS_PATH")
            .ok()
            .and_then(|s| if s.is_empty() { None } else { Some(s.into()) })
    });
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

fn get_http_client() -> Client {
    // Reuse the HTTP connection pool in the global `reqwest::Client` instance
    // TODO: interact with load balancing?
    static CLIENT: LazyLock<Client> = LazyLock::new(|| {
        let mut builder = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .tcp_keepalive(std::time::Duration::from_secs(10))
            .http2_keep_alive_interval(std::time::Duration::from_secs(5))
            .http2_keep_alive_timeout(std::time::Duration::from_secs(3))
            .http2_keep_alive_while_idle(true);

        // HTTPS root trust + optional mTLS identity from RUSTFS_TLS_PATH
        builder = load_ca_roots_from_tls_path(builder);
        if let Some(id) = load_optional_mtls_identity_from_tls_path() {
            builder = builder.identity(id);
        }

        builder.build().expect("Failed to create global HTTP client")
    });
    CLIENT.clone()
}

static HTTP_DEBUG_LOG: bool = false;
#[inline(always)]
fn http_debug_log(args: std::fmt::Arguments) {
    if HTTP_DEBUG_LOG {
        println!("{args}");
    }
}
macro_rules! http_log {
    ($($arg:tt)*) => {
        http_debug_log(format_args!($($arg)*));
    };
}

pin_project! {
    pub struct HttpReader {
        url:String,
        method: Method,
        headers: HeaderMap,
        #[pin]
        inner: StreamReader<Pin<Box<dyn Stream<Item=std::io::Result<Bytes>>+Send+Sync>>, Bytes>,
    }
}

impl HttpReader {
    pub async fn new(url: String, method: Method, headers: HeaderMap, body: Option<Vec<u8>>) -> io::Result<Self> {
        // http_log!("[HttpReader::new] url: {url}, method: {method:?}, headers: {headers:?}");
        Self::with_capacity(url, method, headers, body, 0).await
    }
    /// Create a new HttpReader from a URL. The request is performed immediately.
    pub async fn with_capacity(
        url: String,
        method: Method,
        headers: HeaderMap,
        body: Option<Vec<u8>>,
        _read_buf_size: usize,
    ) -> io::Result<Self> {
        // http_log!(
        //     "[HttpReader::with_capacity] url: {url}, method: {method:?}, headers: {headers:?}, buf_size: {}",
        //     _read_buf_size
        // );
        // First, check if the connection is available (HEAD)
        let client = get_http_client();
        let head_resp = client.head(&url).headers(headers.clone()).send().await;
        match head_resp {
            Ok(resp) => {
                http_log!("[HttpReader::new] HEAD status: {}", resp.status());
                if !resp.status().is_success() {
                    return Err(Error::other(format!("HEAD failed: url: {}, status {}", url, resp.status())));
                }
            }
            Err(e) => {
                http_log!("[HttpReader::new] HEAD error: {e}");
                return Err(Error::other(e.source().map(|s| s.to_string()).unwrap_or_else(|| e.to_string())));
            }
        }

        let client = get_http_client();
        let mut request: RequestBuilder = client.request(method.clone(), url.clone()).headers(headers.clone());
        if let Some(body) = body {
            request = request.body(body);
        }

        let resp = request
            .send()
            .await
            .map_err(|e| Error::other(format!("HttpReader HTTP request error: {e}")))?;

        if resp.status().is_success().not() {
            return Err(Error::other(format!(
                "HttpReader HTTP request failed with non-200 status {}",
                resp.status()
            )));
        }

        let stream = resp
            .bytes_stream()
            .map_err(|e| Error::other(format!("HttpReader stream error: {e}")));

        Ok(Self {
            inner: StreamReader::new(Box::pin(stream)),
            url,
            method,
            headers,
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
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        // http_log!(
        //     "[HttpReader::poll_read] url: {}, method: {:?}, buf.remaining: {}",
        //     self.url,
        //     self.method,
        //     buf.remaining()
        // );
        // Read from the inner stream
        Pin::new(&mut self.inner).poll_read(cx, buf)
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
            Poll::Ready(Some(Some(bytes))) => Poll::Ready(Some(Ok(bytes))),
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
        sender: tokio::sync::mpsc::Sender<Option<Bytes>>,
        handle: tokio::task::JoinHandle<std::io::Result<()>>,
        finish:bool,

    }
}

impl HttpWriter {
    /// Create a new HttpWriter for the given URL. The HTTP request is performed in the background.
    pub async fn new(url: String, method: Method, headers: HeaderMap) -> io::Result<Self> {
        // http_log!("[HttpWriter::new] url: {url}, method: {method:?}, headers: {headers:?}");
        let url_clone = url.clone();
        let method_clone = method.clone();
        let headers_clone = headers.clone();

        // First, try to write empty data to check if writable
        let client = get_http_client();
        let resp = client.put(&url).headers(headers.clone()).body(Vec::new()).send().await;
        match resp {
            Ok(resp) => {
                // http_log!("[HttpWriter::new] empty PUT status: {}", resp.status());
                if !resp.status().is_success() {
                    return Err(Error::other(format!("Empty PUT failed: status {}", resp.status())));
                }
            }
            Err(e) => {
                // http_log!("[HttpWriter::new] empty PUT error: {e}");
                return Err(Error::other(format!("Empty PUT failed: {e}")));
            }
        }

        let (sender, receiver) = tokio::sync::mpsc::channel::<Option<Bytes>>(8);
        let (err_tx, err_rx) = tokio::sync::oneshot::channel::<io::Error>();

        let handle = tokio::spawn(async move {
            let stream = ReceiverStream { receiver };
            let body = reqwest::Body::wrap_stream(stream);
            // http_log!(
            //     "[HttpWriter::spawn] sending HTTP request: url={url_clone}, method={method_clone:?}, headers={headers_clone:?}"
            // );

            let client = get_http_client();
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
                        let _ = err_tx.send(Error::other(format!(
                            "HttpWriter HTTP request failed with non-200 status {}",
                            resp.status()
                        )));
                        return Err(Error::other(format!("HTTP request failed with non-200 status {}", resp.status())));
                    }
                }
                Err(e) => {
                    // http_log!("[HttpWriter::spawn] HTTP request error: {e}");
                    let _ = err_tx.send(Error::other(format!("HTTP request failed: {e}")));
                    return Err(Error::other(format!("HTTP request failed: {e}")));
                }
            }

            // http_log!("[HttpWriter::spawn] HTTP request completed, exiting");
            Ok(())
        });

        // http_log!("[HttpWriter::new] connection established successfully");
        Ok(Self {
            url,
            method,
            headers,
            err_rx,
            sender,
            handle,
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

impl AsyncWrite for HttpWriter {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        // http_log!(
        //     "[HttpWriter::poll_write] url: {}, method: {:?}, buf.len: {}",
        //     self.url,
        //     self.method,
        //     buf.len()
        // );
        if let Ok(e) = Pin::new(&mut self.err_rx).try_recv() {
            return Poll::Ready(Err(e));
        }

        self.sender
            .try_send(Some(Bytes::copy_from_slice(buf)))
            .map_err(|e| Error::other(format!("HttpWriter send error: {e}")))?;

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // let url = self.url.clone();
        // let method = self.method.clone();

        if !self.finish {
            // http_log!("[HttpWriter::poll_shutdown] url: {}, method: {:?}", url, method);
            self.sender
                .try_send(None)
                .map_err(|e| Error::other(format!("HttpWriter shutdown error: {e}")))?;
            // http_log!(
            //     "[HttpWriter::poll_shutdown] sent shutdown signal to HTTP request, url: {}, method: {:?}",
            //     url,
            //     method
            // );

            self.finish = true;
        }
        // Wait for the HTTP request to complete
        use futures::FutureExt;
        match Pin::new(&mut self.get_mut().handle).poll_unpin(_cx) {
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use reqwest::Method;
//     use std::vec;
//     use tokio::io::{AsyncReadExt, AsyncWriteExt};

//     #[tokio::test]
//     async fn test_http_writer_err() {
//         // Use a real local server for integration, or mockito for unit test
//         // Here, we use the Go test server at 127.0.0.1:8081 (scripts/testfile.go)
//         let url = "http://127.0.0.1:8081/testfile".to_string();
//         let data = vec![42u8; 8];

//         // Write
//         // Add header X-Deny-Write = 1 to simulate non-writable situation
//         let mut headers = HeaderMap::new();
//         headers.insert("X-Deny-Write", "1".parse().unwrap());
//         // Here we use PUT method
//         let writer_result = HttpWriter::new(url.clone(), Method::PUT, headers).await;
//         match writer_result {
//             Ok(mut writer) => {
//                 // If creation succeeds, write should fail
//                 let write_result = writer.write_all(&data).await;
//                 assert!(write_result.is_err(), "write_all should fail when server denies write");
//                 if let Err(e) = write_result {
//                     println!("write_all error: {e}");
//                 }
//                 let shutdown_result = writer.shutdown().await;
//                 if let Err(e) = shutdown_result {
//                     println!("shutdown error: {e}");
//                 }
//             }
//             Err(e) => {
//                 // Direct construction failure is also acceptable
//                 println!("HttpWriter::new error: {e}");
//                 assert!(
//                     e.to_string().contains("Empty PUT failed") || e.to_string().contains("Forbidden"),
//                     "unexpected error: {e}"
//                 );
//                 return;
//             }
//         }
//         // Should not reach here
//         panic!("HttpWriter should not allow writing when server denies write");
//     }

//     #[tokio::test]
//     async fn test_http_writer_and_reader_ok() {
//         // Use local Go test server
//         let url = "http://127.0.0.1:8081/testfile".to_string();
//         let data = vec![99u8; 512 * 1024]; // 512KB of data

//         // Write (without X-Deny-Write)
//         let headers = HeaderMap::new();
//         let mut writer = HttpWriter::new(url.clone(), Method::PUT, headers).await.unwrap();
//         writer.write_all(&data).await.unwrap();
//         writer.shutdown().await.unwrap();

//         http_log!("Wrote {} bytes to {} (ok case)", data.len(), url);

//         // Read back
//         let mut reader = HttpReader::with_capacity(url.clone(), Method::GET, HeaderMap::new(), 8192)
//             .await
//             .unwrap();
//         let mut buf = Vec::new();
//         reader.read_to_end(&mut buf).await.unwrap();
//         assert_eq!(buf, data);

//         // println!("Read {} bytes from {} (ok case)", buf.len(), url);
//         // tokio::time::sleep(std::time::Duration::from_secs(2)).await; // Wait for server to process
//         // println!("[test_http_writer_and_reader_ok] completed successfully");
//     }
// }
