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

use std::io;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tonic::codegen::{Service, http};

const CANCELLED_REQUESTS: usize = 64;

async fn write_frame(socket: &mut TcpStream, kind: u8, flags: u8, stream: u32, payload: &[u8]) -> io::Result<()> {
    let length = u32::try_from(payload.len()).expect("fixture frame length");
    let mut frame = Vec::with_capacity(9 + payload.len());
    frame.extend_from_slice(&length.to_be_bytes()[1..]);
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    socket.write_all(&frame).await
}

async fn read_frame(socket: &mut TcpStream) -> io::Result<(u8, u8, u32, Vec<u8>)> {
    let mut header = [0; 9];
    socket.read_exact(&mut header).await?;
    let length = u32::from_be_bytes([0, header[0], header[1], header[2]]);
    let stream = u32::from_be_bytes(header[5..9].try_into().expect("stream ID bytes"));
    let mut payload = vec![0; usize::try_from(length).expect("fixture frame length")];
    socket.read_exact(&mut payload).await?;
    Ok((header[3], header[4], stream, payload))
}

// A raw peer can release replies already in transit after observing CANCEL.
// An h2 server API would discard those replies upon receiving the reset.
async fn delayed_peer(
    listener: TcpListener,
    requests_seen: oneshot::Sender<()>,
    resets_seen: oneshot::Sender<usize>,
) -> io::Result<usize> {
    let (mut socket, _) = listener.accept().await?;
    socket.set_nodelay(true)?;
    let mut preface = [0; 24];
    socket.read_exact(&mut preface).await?;
    assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
    write_frame(&mut socket, 4, 0, 0, &[]).await?;
    let mut requests = Vec::new();
    while requests.len() < CANCELLED_REQUESTS {
        let (kind, flags, stream, _) = read_frame(&mut socket).await?;
        match (kind, flags) {
            (4, 0) => write_frame(&mut socket, 4, 1, 0, &[]).await?,
            (1, _) => requests.push(stream),
            _ => {}
        }
    }
    requests_seen.send(()).expect("caller waits for dispatched requests");
    let mut cancellations = 0;
    while cancellations < CANCELLED_REQUESTS {
        let (kind, _, stream, payload) = read_frame(&mut socket).await?;
        if kind == 3 {
            assert!(requests.contains(&stream), "reset belongs to a dispatched request");
            assert_eq!(payload, 8_u32.to_be_bytes(), "caller cancellation sends CANCEL");
            cancellations += 1;
        }
    }
    for stream in requests {
        // HPACK static index 8 is ':status: 200'; END_HEADERS | END_STREAM.
        write_frame(&mut socket, 1, 5, stream, &[0x88]).await?;
    }
    let ping = b"rst-test";
    write_frame(&mut socket, 6, 0, 0, ping).await?;
    let mut error_resets = 0;
    loop {
        let (kind, flags, _, payload) = read_frame(&mut socket).await?;
        match (kind, flags) {
            (3, _) => {
                assert_eq!(payload, 5_u32.to_be_bytes(), "late reply rejected as STREAM_CLOSED");
                error_resets += 1;
            }
            (6, 1) if payload == ping => break,
            (7, _) => panic!("late replies closed the shared connection: {payload:?}"),
            _ => {}
        }
    }
    // PING acknowledgment establishes that all preceding replies were processed.
    resets_seen.send(error_resets).expect("caller waits for reset accounting");
    loop {
        let (kind, _, stream, payload) = read_frame(&mut socket).await?;
        if kind == 3 {
            assert_eq!(payload, 5_u32.to_be_bytes());
            error_resets += 1;
        }
        if kind == 1 {
            write_frame(&mut socket, 1, 5, stream, &[0x88]).await?;
            return Ok(error_resets);
        }
    }
}

async fn cancelled_metadata_replies_keep_channel_usable(bulk: bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fixture peer");
        let address = format!("http://{}", listener.local_addr().expect("fixture address"));
        let key = if bulk {
            super::bulk_cache_key(&address, 0)
        } else {
            address.clone()
        };
        let (requests_tx, requests_rx) = oneshot::channel();
        let (resets_tx, resets_rx) = oneshot::channel();
        let server = tokio::spawn(delayed_peer(listener, requests_tx, resets_tx));
        let (channel, _) = super::dial_channel(&address, &key, Some(Duration::from_secs(10)))
            .await
            .expect("production channel connects");
        let mut calls = Vec::new();
        for _ in 0..CANCELLED_REQUESTS {
            let mut channel = channel.clone();
            let uri = format!("{address}/node_service.NodeService/ReadVersion");
            calls.push(tokio::spawn(async move {
                std::future::poll_fn(|cx| channel.poll_ready(cx))
                    .await
                    .expect("channel ready");
                channel
                    .call(
                        http::Request::builder()
                            .uri(uri)
                            .body(tonic::body::Body::empty())
                            .expect("read request"),
                    )
                    .await
            }));
        }
        requests_rx.await.expect("all metadata requests dispatched");
        for call in &calls {
            call.abort();
        }
        for call in calls {
            assert!(call.await.expect_err("request cancelled before response").is_cancelled());
        }
        let _ = resets_rx.await.expect("peer accounts for late response frames");
        let mut channel = channel;
        std::future::poll_fn(|cx| channel.poll_ready(cx))
            .await
            .expect("channel remains ready");
        let response = channel
            .call(
                http::Request::builder()
                    .uri(format!("{address}/node_service.NodeService/Ping"))
                    .body(tonic::body::Body::empty())
                    .expect("probe request"),
            )
            .await
            .expect("subsequent RPC shares the connection");
        assert_eq!(response.status(), http::StatusCode::OK);
        let error_resets = server
            .await
            .expect("fixture task completes")
            .expect("fixture transport succeeds");
        assert_eq!(
            error_resets, 0,
            "cancelled metadata replies must not spend the connection's protocol-error budget"
        );
    })
    .await
    .expect("bounded cancellation exchange");
}

#[tokio::test]
async fn control_channel_tolerates_late_cancelled_metadata_replies() {
    cancelled_metadata_replies_keep_channel_usable(false).await;
}

#[tokio::test]
async fn bulk_channel_tolerates_late_cancelled_metadata_replies() {
    cancelled_metadata_replies_keep_channel_usable(true).await;
}
