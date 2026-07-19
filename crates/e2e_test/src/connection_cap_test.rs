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

//! E2E coverage for the opt-in global connection cap on the main API listener
//! (backlog#1191 follow-up, `RUSTFS_API_MAX_CONNECTIONS`): permits must be
//! released when connections close (no leak), and the cap must actually bound
//! concurrency — a queued connection is served only after a held one closes.

use crate::common::{RustFSTestEnvironment, init_logging};
use serial_test::serial;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Open a TCP connection and write one unauthenticated `GET /` (any response,
/// e.g. 403, proves the connection was accepted and served).
async fn open_and_request(addr: &str, connection: &str) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!("GET / HTTP/1.1\r\nHost: {addr}\r\nConnection: {connection}\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    Ok(stream)
}

/// Read until the response head is complete, or `None` on timeout/close —
/// a `None` on an open socket means the connection sits unaccepted in the
/// kernel backlog behind the cap.
async fn read_response_head(stream: &mut TcpStream, dur: Duration) -> Option<String> {
    let deadline = tokio::time::Instant::now() + dur;
    let mut buf = vec![0u8; 4096];
    let mut collected = String::new();
    loop {
        let remaining = deadline.checked_duration_since(tokio::time::Instant::now())?;
        match timeout(remaining, stream.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return None,
            Ok(Ok(n)) => {
                collected.push_str(&String::from_utf8_lossy(&buf[..n]));
                if collected.contains("\r\n\r\n") {
                    return Some(collected);
                }
            }
        }
    }
}

#[tokio::test]
#[serial]
async fn connection_cap_releases_permits_on_close() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_API_MAX_CONNECTIONS", "2")])
        .await?;

    // Ten sequential connections against cap 2: if permits leaked, the third
    // request would already hang in the backlog and time out.
    for i in 0..10 {
        let mut stream = open_and_request(&env.address, "close").await?;
        let head = read_response_head(&mut stream, Duration::from_secs(10))
            .await
            .unwrap_or_else(|| panic!("request {i} got no response — a connection permit leaked"));
        assert!(head.starts_with("HTTP/1.1"), "request {i} unexpected response: {head}");
    }

    Ok(())
}

/// Open a TCP connection and send an INCOMPLETE request head. Once accepted
/// it pins a connection permit: hyper waits for the rest of the head (75s
/// default header timeout) until we close the socket.
async fn open_and_stall(addr: &str) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    stream
        .write_all(format!("GET / HTTP/1.1\r\nHost: {addr}\r\n").as_bytes())
        .await?;
    Ok(stream)
}

#[tokio::test]
#[serial]
async fn connection_cap_blocks_excess_connections_until_permits_free() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_API_MAX_CONNECTIONS", "2")])
        .await?;
    // Let the readiness poller's pooled connection close and free its permit.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Two stalled connections saturate cap 2 (a served-and-closed connection
    // would release its permit immediately, so stalling is what makes the
    // occupancy deterministic).
    let stalled_a = open_and_stall(&env.address).await?;
    let stalled_b = open_and_stall(&env.address).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A complete request now sits in the kernel backlog: connect() succeeds
    // but no permit is available, so no response arrives.
    let mut blocked = open_and_request(&env.address, "close").await?;
    assert!(
        read_response_head(&mut blocked, Duration::from_secs(3)).await.is_none(),
        "cap 2 with two stalled connections must leave the third unserved"
    );

    // Dropping the stalled connections releases their permits (hyper sees
    // EOF while reading the head); the queued request's bytes already sit in
    // the socket buffer, so it must now be accepted and served.
    drop(stalled_a);
    drop(stalled_b);
    let head = read_response_head(&mut blocked, Duration::from_secs(10))
        .await
        .expect("queued connection must be served after permits are released");
    assert!(head.starts_with("HTTP/1.1"), "unexpected response: {head}");

    Ok(())
}
