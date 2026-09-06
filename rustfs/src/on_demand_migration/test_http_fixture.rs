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

//! Scripted HTTP server for the native source backends' tests.
//!
//! The S3 backend can be driven through the SDK's own connector; the native
//! backends talk to a real socket, so their tests need a server that answers a
//! fixed script and records what it was asked. Every response closes its
//! connection, which keeps one request on one socket and makes the script order
//! exactly the request order.

use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use url::Url;

pub(super) struct ScriptedResponse {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: String,
}

impl ScriptedResponse {
    pub(super) fn new(status: u16, headers: Vec<(&'static str, String)>, body: String) -> Self {
        Self { status, headers, body }
    }
}

#[derive(Clone, Debug)]
pub(super) struct RecordedRequest {
    pub(super) method: String,
    /// Request target as it appeared on the wire: path plus query.
    pub(super) target: String,
    pub(super) headers: Vec<(String, String)>,
}

impl RecordedRequest {
    pub(super) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

pub(super) type Recorder = Arc<Mutex<Vec<RecordedRequest>>>;

/// Checks the full request sequence, including the absence of extra probes.
pub(super) fn assert_requests(recorder: &Recorder, expected: &[(&str, &str)]) {
    let recorded = recorder.lock().expect("recorder lock");
    let actual: Vec<_> = recorded
        .iter()
        .map(|request| (request.method.as_str(), request.target.as_str()))
        .collect();
    assert_eq!(actual, expected, "unexpected native source request sequence");
}

/// Binds a loopback listener that answers `responses` in order and returns its
/// origin plus the recorder. The task ends once the script is exhausted.
pub(super) async fn scripted_server(responses: Vec<ScriptedResponse>) -> (Url, Recorder) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("fixture listener should bind");
    let port = listener.local_addr().expect("fixture address").port();
    let recorder: Recorder = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&recorder);

    tokio::spawn(async move {
        for response in responses {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut request = Vec::new();
            let mut buffer = [0_u8; 2048];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                match stream.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => request.extend_from_slice(&buffer[..read]),
                }
            }
            let text = String::from_utf8_lossy(&request).into_owned();
            let mut lines = text.lines();
            let start = lines.next().unwrap_or_default().to_string();
            let mut parts = start.split_whitespace();
            sink.lock().expect("recorder lock").push(RecordedRequest {
                method: parts.next().unwrap_or_default().to_string(),
                target: parts.next().unwrap_or_default().to_string(),
                headers: lines
                    .take_while(|line| !line.is_empty())
                    .filter_map(|line| line.split_once(':'))
                    .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
                    .collect(),
            });

            // A scripted HEAD declares the object size in its own headers while
            // carrying no body, so an explicit `Content-Length` wins over the
            // body length.
            let declares_length = response
                .headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("content-length"));
            let mut rendered = match declares_length {
                true => format!("HTTP/1.1 {} Scripted\r\nConnection: close\r\n", response.status),
                false => format!(
                    "HTTP/1.1 {} Scripted\r\nContent-Length: {}\r\nConnection: close\r\n",
                    response.status,
                    response.body.len()
                ),
            };
            for (name, value) in response.headers {
                rendered.push_str(&format!("{name}: {value}\r\n"));
            }
            rendered.push_str("\r\n");
            rendered.push_str(&response.body);
            let _ = stream.write_all(rendered.as_bytes()).await;
            let _ = stream.flush().await;
        }
    });

    (Url::parse(&format!("http://127.0.0.1:{port}")).expect("fixture endpoint"), recorder)
}
