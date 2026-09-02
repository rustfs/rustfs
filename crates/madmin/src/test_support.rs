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

//! Test-only HTTP server shared by the admin client modules: one canned
//! response per connection, every request recorded for assertions.

use std::sync::{Arc, Mutex};

/// One recorded request, parsed off the wire with the minimum needed for
/// assertions: method, path, query, headers, body.
#[derive(Debug, Clone)]
pub(crate) struct RecordedRequest {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) query: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: String,
}

impl RecordedRequest {
    pub(crate) fn header(&self, name: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
    }
}

/// Minimal HTTP/1.1 server: one canned response per connection, every
/// request recorded behind an `Arc<Mutex>`. Deliberately dependency-free —
/// the assertions only need the raw request bytes.
pub(crate) struct TestServer {
    pub(crate) addr: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl TestServer {
    pub(crate) async fn spawn(response_body: &'static str, status: u16) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::new(Mutex::new(Vec::new()));

        let recorded = requests.clone();
        tokio::spawn(async move {
            let reason = match status {
                200 => "OK",
                204 => "No Content",
                400 => "Bad Request",
                404 => "Not Found",
                _ => "Forbidden",
            };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response_body}",
                response_body.len()
            );
            // Each request is a fresh connection (connection: close); a
            // bounded loop serves every call a test makes while letting
            // the task exit instead of lingering for the whole process.
            for _ in 0..16 {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let mut buffer = Vec::with_capacity(2048);
                let mut chunk = [0u8; 2048];
                // Read headers plus content-length body, or stop on close.
                loop {
                    if let Some(end) = find_header_end(&buffer) {
                        let content_length = extract_content_length(&buffer[..end]);
                        if buffer.len() >= end + content_length {
                            break;
                        }
                    }
                    let n = match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    buffer.extend_from_slice(&chunk[..n]);
                    if buffer.len() > 64 * 1024 {
                        break;
                    }
                }
                if let Some(request) = parse_request(&buffer) {
                    recorded.lock().expect("recorded lock").push(request);
                }
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });

        Self { addr, requests }
    }

    pub(crate) fn recorded(&self) -> RecordedRequest {
        self.requests
            .lock()
            .expect("recorded lock")
            .last()
            .cloned()
            .expect("the client call must have produced one recorded request")
    }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n").map(|pos| pos + 4)
}

fn extract_content_length(headers: &[u8]) -> usize {
    let text = String::from_utf8_lossy(headers).to_ascii_lowercase();
    text.lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}

fn parse_request(raw: &[u8]) -> Option<RecordedRequest> {
    let end = find_header_end(raw)?;
    let head = String::from_utf8_lossy(&raw[..end]);
    let body = String::from_utf8_lossy(&raw[end..]).into_owned();
    let mut lines = head.lines();
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path.to_string(), query.to_string()),
        None => (target, String::new()),
    };
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect();
    Some(RecordedRequest {
        method,
        path,
        query,
        headers,
        body,
    })
}
