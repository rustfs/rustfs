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

//! Minimal scripted HTTP responder standing in for a Vault server.
//!
//! Wiring tests need to observe how many Vault requests a code path performs
//! (retries, read-confirm recovery) without a live Vault. The responder serves
//! one canned response per incoming request in order, closes the connection
//! after each response, and records the `METHOD /path` sequence for
//! assertions. It intentionally implements just enough HTTP/1.1 for the
//! `vaultrs` reqwest client: no keep-alive, no chunked bodies.

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// One canned HTTP response.
pub(crate) struct ScriptedResponse {
    status: u16,
    body: String,
}

impl ScriptedResponse {
    /// A 200 response carrying `data` inside the standard Vault envelope.
    pub(crate) fn ok(data: serde_json::Value) -> Self {
        Self {
            status: 200,
            body: serde_json::json!({
                "request_id": "scripted",
                "lease_id": "",
                "lease_duration": 0,
                "renewable": false,
                "data": data,
            })
            .to_string(),
        }
    }

    /// An error response in Vault's `{"errors": [...]}` format.
    pub(crate) fn error(status: u16, message: &str) -> Self {
        Self {
            status,
            body: serde_json::json!({ "errors": [message] }).to_string(),
        }
    }
}

/// A scripted stand-in Vault listening on a loopback port.
pub(crate) struct ScriptedVault {
    /// Base address (`http://127.0.0.1:port`) to point a Vault client at.
    pub(crate) address: String,
    requests: Arc<Mutex<Vec<String>>>,
}

impl ScriptedVault {
    /// Bind a loopback listener and serve `responses` one per request.
    ///
    /// Requests beyond the script get a 599 error so a test that under-scripts
    /// fails loudly instead of hanging.
    pub(crate) async fn serve(responses: Vec<ScriptedResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind scripted vault listener");
        let address = format!("http://{}", listener.local_addr().expect("scripted vault local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);

        tokio::spawn(async move {
            let mut responses = responses.into_iter();
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let Some(request_line) = read_request(&mut stream).await else {
                    continue;
                };
                recorded
                    .lock()
                    .expect("scripted vault request log poisoned")
                    .push(request_line);
                let response = responses
                    .next()
                    .unwrap_or_else(|| ScriptedResponse::error(599, "scripted vault: script exhausted"));
                let payload = format!(
                    "HTTP/1.1 {} Scripted\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    response.status,
                    response.body.len(),
                    response.body
                );
                let _ = stream.write_all(payload.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });

        Self { address, requests }
    }

    /// The `METHOD /path` lines of every request served so far, in order.
    pub(crate) fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("scripted vault request log poisoned").clone()
    }
}

/// Read one HTTP/1.1 request (head plus content-length body) and return its
/// `METHOD /path` line. Draining the body before responding keeps the client
/// from seeing a connection reset while it is still writing.
async fn read_request(stream: &mut TcpStream) -> Option<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    };

    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let mut lines = head.lines();
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    // rustify appends a lone "?" when an endpoint has no query parameters;
    // strip it so assertions can use the plain path.
    let path = path.strip_suffix('?').unwrap_or(path);

    let content_length: usize = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())?
        })
        .next()
        .unwrap_or(0);
    let mut remaining = content_length.saturating_sub(buffer.len() - head_end);
    while remaining > 0 {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            break;
        }
        remaining = remaining.saturating_sub(read);
    }

    Some(format!("{method} {path}"))
}
