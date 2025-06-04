use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::HeaderMap;
use pin_project_lite::pin_project;
use reqwest::{Client, Method, RequestBuilder};
use std::io::{self, Error};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::{mpsc, oneshot};

use crate::{EtagResolvable, HashReaderDetector, HashReaderMut};

static HTTP_DEBUG_LOG: bool = false;
#[inline(always)]
fn http_debug_log(args: std::fmt::Arguments) {
    if HTTP_DEBUG_LOG {
        println!("{}", args);
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
        inner: DuplexStream,
        err_rx: oneshot::Receiver<std::io::Error>,
    }
}

impl HttpReader {
    pub async fn new(url: String, method: Method, headers: HeaderMap) -> io::Result<Self> {
        http_log!("[HttpReader::new] url: {url}, method: {method:?}, headers: {headers:?}");
        Self::with_capacity(url, method, headers, 0).await
    }
    /// Create a new HttpReader from a URL. The request is performed immediately.
    pub async fn with_capacity(url: String, method: Method, headers: HeaderMap, mut read_buf_size: usize) -> io::Result<Self> {
        http_log!(
            "[HttpReader::with_capacity] url: {url}, method: {method:?}, headers: {headers:?}, buf_size: {}",
            read_buf_size
        );
        // First, check if the connection is available (HEAD)
        let client = Client::new();
        let head_resp = client.head(&url).headers(headers.clone()).send().await;
        match head_resp {
            Ok(resp) => {
                http_log!("[HttpReader::new] HEAD status: {}", resp.status());
                if !resp.status().is_success() {
                    return Err(Error::other(format!("HEAD failed: status {}", resp.status())));
                }
            }
            Err(e) => {
                http_log!("[HttpReader::new] HEAD error: {e}");
                return Err(Error::other(format!("HEAD request failed: {e}")));
            }
        }

        let url_clone = url.clone();
        let method_clone = method.clone();
        let headers_clone = headers.clone();

        if read_buf_size == 0 {
            read_buf_size = 8192; // Default buffer size
        }
        let (rd, mut wd) = tokio::io::duplex(read_buf_size);
        let (err_tx, err_rx) = oneshot::channel::<io::Error>();
        tokio::spawn(async move {
            let client = Client::new();
            let request: RequestBuilder = client.request(method_clone, url_clone).headers(headers_clone);

            let response = request.send().await;
            match response {
                Ok(resp) => {
                    if resp.status().is_success() {
                        let mut stream = resp.bytes_stream();
                        while let Some(chunk) = stream.next().await {
                            match chunk {
                                Ok(data) => {
                                    if let Err(e) = wd.write_all(&data).await {
                                        let _ = err_tx.send(Error::other(format!("HttpReader write error: {}", e)));
                                        break;
                                    }
                                }
                                Err(e) => {
                                    let _ = err_tx.send(Error::other(format!("HttpReader stream error: {}", e)));
                                    break;
                                }
                            }
                        }
                    } else {
                        http_log!("[HttpReader::spawn] HTTP request failed with status: {}", resp.status());
                        let _ = err_tx.send(Error::other(format!(
                            "HttpReader HTTP request failed with non-200 status {}",
                            resp.status()
                        )));
                    }
                }
                Err(e) => {
                    let _ = err_tx.send(Error::other(format!("HttpReader HTTP request error: {}", e)));
                }
            }

            http_log!("[HttpReader::spawn] HTTP request completed, exiting");
        });
        Ok(Self {
            inner: rd,
            err_rx,
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
        http_log!(
            "[HttpReader::poll_read] url: {}, method: {:?}, buf.remaining: {}",
            self.url,
            self.method,
            buf.remaining()
        );
        // Check for errors from the request
        match Pin::new(&mut self.err_rx).try_recv() {
            Ok(e) => return Poll::Ready(Err(e)),
            Err(oneshot::error::TryRecvError::Empty) => {}
            Err(oneshot::error::TryRecvError::Closed) => {
                // return Poll::Ready(Err(Error::new(ErrorKind::Other, "HTTP request closed")));
            }
        }
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
        match &poll {
            Poll::Ready(Some(Some(ref bytes))) => {
                http_log!("[ReceiverStream] poll_next: got {} bytes", bytes.len());
            }
            Poll::Ready(Some(None)) => {
                http_log!("[ReceiverStream] poll_next: sender shutdown");
            }
            Poll::Ready(None) => {
                http_log!("[ReceiverStream] poll_next: channel closed");
            }
            Poll::Pending => {
                // http_log!("[ReceiverStream] poll_next: pending");
            }
        }
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
        http_log!("[HttpWriter::new] url: {url}, method: {method:?}, headers: {headers:?}");
        let url_clone = url.clone();
        let method_clone = method.clone();
        let headers_clone = headers.clone();

        // First, try to write empty data to check if writable
        let client = Client::new();
        let resp = client.put(&url).headers(headers.clone()).body(Vec::new()).send().await;
        match resp {
            Ok(resp) => {
                http_log!("[HttpWriter::new] empty PUT status: {}", resp.status());
                if !resp.status().is_success() {
                    return Err(Error::other(format!("Empty PUT failed: status {}", resp.status())));
                }
            }
            Err(e) => {
                http_log!("[HttpWriter::new] empty PUT error: {e}");
                return Err(Error::other(format!("Empty PUT failed: {e}")));
            }
        }

        let (sender, receiver) = tokio::sync::mpsc::channel::<Option<Bytes>>(8);
        let (err_tx, err_rx) = tokio::sync::oneshot::channel::<io::Error>();

        let handle = tokio::spawn(async move {
            let stream = ReceiverStream { receiver };
            let body = reqwest::Body::wrap_stream(stream);
            http_log!(
                "[HttpWriter::spawn] sending HTTP request: url={url_clone}, method={method_clone:?}, headers={headers_clone:?}"
            );

            let client = Client::new();
            let request = client
                .request(method_clone, url_clone.clone())
                .headers(headers_clone.clone())
                .body(body);

            // Hold the request until the shutdown signal is received
            let response = request.send().await;

            match response {
                Ok(resp) => {
                    http_log!("[HttpWriter::spawn] got response: status={}", resp.status());
                    if !resp.status().is_success() {
                        let _ = err_tx.send(Error::other(format!(
                            "HttpWriter HTTP request failed with non-200 status {}",
                            resp.status()
                        )));
                        return Err(Error::other(format!("HTTP request failed with non-200 status {}", resp.status())));
                    }
                }
                Err(e) => {
                    http_log!("[HttpWriter::spawn] HTTP request error: {e}");
                    let _ = err_tx.send(Error::other(format!("HTTP request failed: {}", e)));
                    return Err(Error::other(format!("HTTP request failed: {}", e)));
                }
            }

            http_log!("[HttpWriter::spawn] HTTP request completed, exiting");
            Ok(())
        });

        http_log!("[HttpWriter::new] connection established successfully");
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
        http_log!(
            "[HttpWriter::poll_write] url: {}, method: {:?}, buf.len: {}",
            self.url,
            self.method,
            buf.len()
        );
        if let Ok(e) = Pin::new(&mut self.err_rx).try_recv() {
            return Poll::Ready(Err(e));
        }

        self.sender
            .try_send(Some(Bytes::copy_from_slice(buf)))
            .map_err(|e| Error::other(format!("HttpWriter send error: {}", e)))?;

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        if !self.finish {
            http_log!("[HttpWriter::poll_shutdown] url: {}, method: {:?}", self.url, self.method);
            self.sender
                .try_send(None)
                .map_err(|e| Error::other(format!("HttpWriter shutdown error: {}", e)))?;
            http_log!("[HttpWriter::poll_shutdown] sent shutdown signal to HTTP request");

            self.finish = true;
        }
        // Wait for the HTTP request to complete
        use futures::FutureExt;
        match Pin::new(&mut self.get_mut().handle).poll_unpin(_cx) {
            Poll::Ready(Ok(_)) => {
                http_log!("[HttpWriter::poll_shutdown] HTTP request finished successfully");
            }
            Poll::Ready(Err(e)) => {
                http_log!("[HttpWriter::poll_shutdown] HTTP request failed: {e}");
                return Poll::Ready(Err(Error::other(format!("HTTP request failed: {}", e))));
            }
            Poll::Pending => {
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
//         // 添加 header X-Deny-Write = 1 模拟不可写入的情况
//         let mut headers = HeaderMap::new();
//         headers.insert("X-Deny-Write", "1".parse().unwrap());
//         // 这里我们使用 PUT 方法
//         let writer_result = HttpWriter::new(url.clone(), Method::PUT, headers).await;
//         match writer_result {
//             Ok(mut writer) => {
//                 // 如果能创建成功，写入应该报错
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
//                 // 直接构造失败也可以
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
//         // 使用本地 Go 测试服务器
//         let url = "http://127.0.0.1:8081/testfile".to_string();
//         let data = vec![99u8; 512 * 1024]; // 512KB of data

//         // Write (不加 X-Deny-Write)
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
