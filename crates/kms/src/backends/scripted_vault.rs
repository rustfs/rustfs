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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// One scripted connection outcome.
pub(crate) enum ScriptedResponse {
    Http { status: u16, body: String },
    Close,
}

impl ScriptedResponse {
    /// A 200 response carrying `data` inside the standard Vault envelope.
    pub(crate) fn ok(data: serde_json::Value) -> Self {
        Self::Http {
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
        Self::Http {
            status,
            body: serde_json::json!({ "errors": [message] }).to_string(),
        }
    }

    /// The 404 Vault answers a LIST of an empty path with: something routed the
    /// request and found nothing under it, so the `errors` array comes back
    /// empty. [`ScriptedResponse::error`] cannot stand in — it always fills
    /// `errors`, which is what marks a 404 as an unrouted path instead.
    pub(crate) fn empty_list_404() -> Self {
        Self::Http {
            status: 404,
            body: serde_json::json!({ "errors": [] }).to_string(),
        }
    }

    /// Close the connection after consuming a request without sending an HTTP response.
    pub(crate) fn close() -> Self {
        Self::Close
    }
}

/// A scripted stand-in Vault listening on a loopback port.
pub(crate) struct ScriptedVault {
    /// Base address (`http://127.0.0.1:port`) to point a Vault client at.
    pub(crate) address: String,
    requests: Arc<Mutex<Vec<(String, String)>>>,
    kv2_state: Option<Arc<Mutex<Kv2State>>>,
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
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let Some((request_line, body, mut stream)) = read_request(stream).await else {
                    continue;
                };
                recorded
                    .lock()
                    .expect("scripted vault request log poisoned")
                    .push((request_line, body));
                let response = responses
                    .next()
                    .unwrap_or_else(|| ScriptedResponse::error(599, "scripted vault: script exhausted"));
                if let ScriptedResponse::Http { status, body } = response {
                    let payload = format!(
                        "HTTP/1.1 {status} Scripted\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len(),
                    );
                    let _ = stream.write_all(payload.as_bytes()).await;
                    let _ = stream.shutdown().await;
                }
            }
        });

        Self {
            address,
            requests,
            kv2_state: None,
        }
    }

    /// Bind a small stateful KV2 responder for concurrency tests.
    ///
    /// Unlike Self::serve, this responder evaluates CAS writes against a
    /// shared in-memory record and handles connections concurrently. It models
    /// only the KV2 data and metadata paths used by the rotation protocol; an
    /// unknown request receives a 599 response so a test cannot silently
    /// under-specify the Vault exchange.
    pub(crate) async fn serve_kv2(key_path: &str, key_data: serde_json::Value) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind scripted vault listener");
        let address = format!("http://{}", listener.local_addr().expect("scripted vault local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(Mutex::new(Kv2State::new(key_data)));
        let recorded = Arc::clone(&requests);
        let state_for_server = Arc::clone(&state);
        let key_path = key_path.to_string();

        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let recorded = Arc::clone(&recorded);
                let state = Arc::clone(&state_for_server);
                let key_path = key_path.clone();
                tokio::spawn(async move {
                    let Some((request_line, body, stream)) = read_request(stream).await else {
                        return;
                    };
                    recorded
                        .lock()
                        .expect("scripted vault request log poisoned")
                        .push((request_line.clone(), body.clone()));
                    let response = state
                        .lock()
                        .expect("scripted KV2 state poisoned")
                        .respond(&key_path, &request_line, &body);
                    write_response(stream, response).await;
                });
            }
        });

        Self {
            address,
            requests,
            kv2_state: Some(state),
        }
    }

    /// The `METHOD /path` lines of every request served so far, in order.
    pub(crate) fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("scripted vault request log poisoned")
            .iter()
            .map(|(line, _)| line.clone())
            .collect()
    }

    /// The request bodies, in the same order as [`Self::requests`]; empty for
    /// bodyless requests. Lets tests assert what a write actually persisted
    /// (record contents, check-and-set options), not just that a write happened.
    pub(crate) fn request_bodies(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("scripted vault request log poisoned")
            .iter()
            .map(|(_, body)| body.clone())
            .collect()
    }

    /// Snapshot the in-memory KV2 state used by Self::serve_kv2.
    pub(crate) fn kv2_snapshot(&self) -> Option<Kv2Snapshot> {
        self.kv2_state.as_ref().map(|state| {
            let state = state.lock().expect("scripted KV2 state poisoned");
            Kv2Snapshot {
                current_data: state.current_data.clone(),
                current_secret_version: state.current_secret_version,
                version_records: state.version_records.clone(),
            }
        })
    }
}

/// State captured by the stateful KV2 responder for assertions in wiring tests.
#[derive(Debug, Clone)]
pub(crate) struct Kv2Snapshot {
    pub(crate) current_data: serde_json::Value,
    pub(crate) current_secret_version: u64,
    pub(crate) version_records: BTreeMap<u32, serde_json::Value>,
}

#[derive(Debug)]
struct Kv2State {
    current_data: serde_json::Value,
    current_secret_version: u64,
    history: BTreeMap<u64, serde_json::Value>,
    version_records: BTreeMap<u32, serde_json::Value>,
}

impl Kv2State {
    fn new(current_data: serde_json::Value) -> Self {
        let mut version_records = BTreeMap::new();
        if current_data["baseline_version"].as_u64() == Some(1) {
            version_records.insert(1, current_data.clone());
        }
        Self {
            history: BTreeMap::from([(1, current_data.clone())]),
            current_data,
            current_secret_version: 1,
            version_records,
        }
    }

    fn respond(&mut self, key_path: &str, request_line: &str, body: &str) -> ScriptedResponse {
        let Some((method, path)) = request_line.split_once(' ') else {
            return ScriptedResponse::error(599, "scripted KV2: malformed request line");
        };
        let data_path = format!("/v1/secret/data/{key_path}");
        let metadata_path = format!("/v1/secret/metadata/{key_path}");
        let version_data_prefix = format!("{data_path}/versions/");
        let version_metadata_path = format!("{metadata_path}/versions");

        if method == "GET" && path == metadata_path {
            return ScriptedResponse::ok(metadata_data(self.current_secret_version));
        }

        if method == "LIST" && path == version_metadata_path {
            let keys = self.version_records.keys().map(u32::to_string).collect::<Vec<_>>();
            return ScriptedResponse::ok(serde_json::json!({ "keys": keys }));
        }

        if let Some(version) = path
            .strip_prefix(&version_data_prefix)
            .and_then(|value| value.parse::<u32>().ok())
        {
            return match method {
                "GET" => self
                    .version_records
                    .get(&version)
                    .cloned()
                    .map(read_data)
                    .unwrap_or_else(|| ScriptedResponse::error(404, "not found")),
                "POST" => self.create_version_record(version, body),
                _ => ScriptedResponse::error(599, "scripted KV2: unsupported version request"),
            };
        }

        if method == "GET" && path.strip_prefix(&data_path).is_some() {
            let version = path
                .split_once("?version=")
                .and_then(|(_, value)| value.parse::<u64>().ok())
                .unwrap_or(self.current_secret_version);
            return self
                .history
                .get(&version)
                .cloned()
                .map(read_data)
                .unwrap_or_else(|| ScriptedResponse::error(404, "not found"));
        }

        if method == "POST" && path == data_path {
            return self.write_current_record(body);
        }

        ScriptedResponse::error(599, "scripted KV2: unexpected request")
    }

    fn create_version_record(&mut self, version: u32, body: &str) -> ScriptedResponse {
        let body: serde_json::Value = match serde_json::from_str(body) {
            Ok(body) => body,
            Err(_) => return ScriptedResponse::error(400, "invalid JSON"),
        };
        if body["options"]["cas"].as_u64() != Some(0) {
            return ScriptedResponse::error(400, "version record requires create-only CAS");
        }
        if self.version_records.contains_key(&version) {
            return ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE);
        }
        let Some(data) = body.get("data").cloned() else {
            return ScriptedResponse::error(400, "missing data");
        };
        self.version_records.insert(version, data);
        write_ack(1)
    }

    fn write_current_record(&mut self, body: &str) -> ScriptedResponse {
        let body: serde_json::Value = match serde_json::from_str(body) {
            Ok(body) => body,
            Err(_) => return ScriptedResponse::error(400, "invalid JSON"),
        };
        if body["options"]["cas"].as_u64() != Some(self.current_secret_version) {
            return ScriptedResponse::error(400, CAS_CONFLICT_MESSAGE);
        }
        let Some(data) = body.get("data").cloned() else {
            return ScriptedResponse::error(400, "missing data");
        };
        self.current_secret_version += 1;
        self.current_data = data.clone();
        self.history.insert(self.current_secret_version, data);
        write_ack(self.current_secret_version)
    }
}

const CAS_CONFLICT_MESSAGE: &str = "check-and-set parameter did not match the current version";

fn metadata_data(current_version: u64) -> serde_json::Value {
    serde_json::json!({
        "cas_required": false,
        "created_time": "2026-01-01T00:00:00Z",
        "current_version": current_version,
        "delete_version_after": "0s",
        "max_versions": 0,
        "oldest_version": 0,
        "updated_time": "2026-01-01T00:00:00Z",
        "custom_metadata": null,
        "versions": {},
    })
}

fn read_data(data: serde_json::Value) -> ScriptedResponse {
    ScriptedResponse::ok(serde_json::json!({
        "data": data,
        "metadata": {
            "created_time": "2026-01-01T00:00:00Z",
            "deletion_time": "",
            "custom_metadata": null,
            "destroyed": false,
            "version": 1,
        },
    }))
}

fn write_ack(version: u64) -> ScriptedResponse {
    ScriptedResponse::ok(serde_json::json!({
        "created_time": "2026-01-01T00:00:00Z",
        "custom_metadata": null,
        "deletion_time": "",
        "destroyed": false,
        "version": version,
    }))
}

async fn write_response(mut stream: TcpStream, response: ScriptedResponse) {
    if let ScriptedResponse::Http { status, body } = response {
        let payload = format!(
            "HTTP/1.1 {status} Scripted\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len(),
        );
        let _ = stream.write_all(payload.as_bytes()).await;
        let _ = stream.shutdown().await;
    }
}

/// Read one HTTP/1.1 request (head plus content-length body) and return its
/// `METHOD /path` line together with the body. Draining the body before
/// responding keeps the client from seeing a connection reset while it is
/// still writing.
async fn read_request(mut stream: TcpStream) -> Option<(String, String, TcpStream)> {
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
    let mut body = buffer[head_end..].to_vec();
    let mut remaining = content_length.saturating_sub(body.len());
    while remaining > 0 {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
        remaining = remaining.saturating_sub(read);
    }
    body.truncate(content_length);

    Some((format!("{method} {path}"), String::from_utf8_lossy(&body).into_owned(), stream))
}
