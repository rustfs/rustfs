// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{INTERNODE_RPC_CONCURRENCY_LIMIT, dial_channel, proto_gen::node_service};
use prost::Message;
use std::{
    collections::HashSet,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinSet,
};

const BATCHES: usize = 80;
const HEADERS: u8 = 1;
const RST_STREAM: u8 = 3;
const SETTINGS: u8 = 4;
const PING: u8 = 6;
const GOAWAY: u8 = 7;
const WINDOW_UPDATE: u8 = 8;
const END_HEADERS: u8 = 4;
const END_STREAM: u8 = 1;

struct Frame {
    kind: u8,
    flags: u8,
    stream: u32,
    payload: Vec<u8>,
}

async fn read_frame(socket: &mut TcpStream) -> io::Result<Frame> {
    let mut header = [0; 9];
    socket.read_exact(&mut header).await?;
    let length = usize::from(header[0]) * 65536 + usize::from(header[1]) * 256 + usize::from(header[2]);
    if length > 16384 {
        return Err(io::Error::other("test peer received an oversized frame"));
    }
    let mut payload = vec![0; length];
    socket.read_exact(&mut payload).await?;
    Ok(Frame {
        kind: header[3],
        flags: header[4],
        stream: u32::from_be_bytes(header[5..9].try_into().expect("four stream ID bytes")) & 0x7fff_ffff,
        payload,
    })
}

async fn write_frame(socket: &mut TcpStream, kind: u8, flags: u8, stream: u32, payload: &[u8]) -> io::Result<()> {
    let length = u32::try_from(payload.len()).expect("bounded test frame length").to_be_bytes();
    assert_eq!(length[0], 0, "HTTP/2 frame length fits 24 bits");
    let mut header = [0; 9];
    header[..3].copy_from_slice(&length[1..]);
    header[3] = kind;
    header[4] = flags;
    header[5..].copy_from_slice(&stream.to_be_bytes());
    socket.write_all(&header).await?;
    socket.write_all(payload).await
}

async fn write_probe_response(socket: &mut TcpStream, stream: u32) -> io::Result<()> {
    // HPACK's static :status=200 and literal content-type=application/grpc.
    write_frame(socket, HEADERS, END_HEADERS, stream, b"\x88\x0f\x10\x10application/grpc").await?;
    let response = node_service::ReadVersionResponse {
        success: true,
        ..Default::default()
    }
    .encode_to_vec();
    let mut body = vec![0];
    body.extend_from_slice(&u32::try_from(response.len()).expect("small probe response").to_be_bytes());
    body.extend(response);
    write_frame(socket, 0, 0, stream, &body).await?;
    // A literal grpc-status=0 trailer ends the live probe RPC.
    write_frame(socket, HEADERS, END_HEADERS | END_STREAM, stream, b"\x00\x0bgrpc-status\x010").await
}

async fn accept_peer(listener: TcpListener) -> io::Result<TcpStream> {
    let (mut socket, _) = listener.accept().await?;
    socket.set_nodelay(true)?;
    let mut preface = [0; 24];
    socket.read_exact(&mut preface).await?;
    assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
    write_frame(&mut socket, SETTINGS, 0, 0, &[]).await?;
    Ok(socket)
}

async fn delayed_response_peer(listener: TcpListener, ready: mpsc::Sender<()>, error_resets: Arc<AtomicUsize>) -> io::Result<()> {
    let mut socket = accept_peer(listener).await?;
    let mut cancelled = Vec::with_capacity(INTERNODE_RPC_CONCURRENCY_LIMIT);
    let mut resets = HashSet::new();
    let mut probe = None;
    loop {
        let frame = read_frame(&mut socket).await?;
        match frame.kind {
            SETTINGS | PING if frame.flags & 1 == 0 => {
                let payload = if frame.kind == PING { frame.payload.as_slice() } else { &[] };
                write_frame(&mut socket, frame.kind, 1, 0, payload).await?;
            }
            0 if !frame.payload.is_empty() => {
                let credit = u32::try_from(frame.payload.len()).expect("bounded DATA frame").to_be_bytes();
                write_frame(&mut socket, WINDOW_UPDATE, 0, 0, &credit).await?;
            }
            HEADERS if cancelled.len() < INTERNODE_RPC_CONCURRENCY_LIMIT => {
                cancelled.push(frame.stream);
                if cancelled.len() == INTERNODE_RPC_CONCURRENCY_LIMIT {
                    ready.send(()).await.map_err(|_| io::Error::other("test driver stopped"))?;
                }
            }
            HEADERS => {
                assert!(probe.replace(frame.stream).is_none(), "one live probe per batch");
            }
            RST_STREAM => {
                let reason = u32::from_be_bytes(frame.payload.as_slice().try_into().expect("RST_STREAM error code"));
                if reason == 5 {
                    error_resets.fetch_add(1, Ordering::Relaxed);
                }
                if cancelled.contains(&frame.stream) {
                    resets.insert(frame.stream);
                }
            }
            GOAWAY => return Err(io::Error::other(format!("client GOAWAY: {:?}", frame.payload))),
            _ => {}
        }
        if let Some(stream) = probe
            && resets.len() == INTERNODE_RPC_CONCURRENCY_LIMIT
        {
            // A raw peer controls bytes already in flight at cancellation. A normal
            // server would consume RST_STREAM and suppress these delayed responses.
            for cancelled_stream in &cancelled {
                write_frame(&mut socket, HEADERS, END_HEADERS | END_STREAM, *cancelled_stream, b"\x88").await?;
            }
            write_probe_response(&mut socket, stream).await?;
            cancelled.clear();
            resets.clear();
            probe = None;
        }
    }
}

async fn assert_cancelled_responses_are_ignored(bulk: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind isolated peer");
    let address = format!("http://{}", listener.local_addr().expect("peer address"));
    let (ready_tx, mut ready_rx) = mpsc::channel(1);
    let error_resets = Arc::new(AtomicUsize::new(0));
    let peer = tokio::spawn(delayed_response_peer(listener, ready_tx, error_resets.clone()));
    let cache_key = if bulk {
        super::bulk_cache_key(&address, 0)
    } else {
        address.clone()
    };
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let (channel, _) = dial_channel(&address, &cache_key, Duration::from_secs(10))
            .await
            .expect("dial real internode channel");
        let mut client = node_service::node_service_client::NodeServiceClient::new(channel);
        for batch in 0..BATCHES {
            let mut requests = JoinSet::new();
            for _ in 0..INTERNODE_RPC_CONCURRENCY_LIMIT {
                let mut pending_client = client.clone();
                requests.spawn(async move { pending_client.read_version(node_service::ReadVersionRequest::default()).await });
            }
            ready_rx.recv().await.expect("peer observed every pending request");
            requests.abort_all();
            while let Some(result) = requests.join_next().await {
                assert!(result.expect_err("pending RPC task was cancelled").is_cancelled());
            }
            let response = client
                .read_version(node_service::ReadVersionRequest::default())
                .await
                .map_err(|error| format!("live probe after cancellation batch {batch}: {error}"))?;
            assert!(response.into_inner().success);
        }
        Ok::<_, String>(())
    })
    .await;
    peer.abort();
    let peer_result = peer.await;
    assert!(matches!(result, Ok(Ok(()))), "shared channel failed: {result:?}; peer: {peer_result:?}");
    assert_eq!(
        error_resets.load(Ordering::Relaxed),
        0,
        "late cancelled responses must not spend the error budget"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_metadata_responses_keep_the_shared_channel_usable() {
    assert_cancelled_responses_are_ignored(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_metadata_responses_keep_the_bulk_channel_usable() {
    temp_env::async_with_vars(
        [(rustfs_config::ENV_INTERNODE_BULK_CONCURRENCY_LIMIT, Some("64"))],
        assert_cancelled_responses_are_ignored(true),
    )
    .await;
}

async fn unsolicited_response_peer(listener: TcpListener, attack: oneshot::Receiver<()>) -> io::Result<(usize, Vec<u8>)> {
    let mut socket = accept_peer(listener).await?;
    let mut completed = Vec::with_capacity(1025);
    while completed.len() < 1025 {
        let frame = read_frame(&mut socket).await?;
        match frame.kind {
            HEADERS => {
                write_probe_response(&mut socket, frame.stream).await?;
                completed.push(frame.stream);
            }
            SETTINGS if frame.flags & 1 == 0 => write_frame(&mut socket, SETTINGS, 1, 0, &[]).await?,
            PING if frame.flags & 1 == 0 => write_frame(&mut socket, PING, 1, 0, &frame.payload).await?,
            _ => {}
        }
    }
    attack
        .await
        .map_err(|_| io::Error::other("test driver stopped before attack"))?;
    // These streams completed normally, so duplicate responses are protocol errors,
    // not late responses to a cancellation that the grace period should absorb.
    let mut resets = 0;
    for stream in completed {
        write_frame(&mut socket, HEADERS, END_HEADERS | END_STREAM, stream, b"\x88").await?;
        // Drain each reset before provoking the next one: GOAWAY may discard
        // queued frames, so a burst's wire count is not the internal error count.
        loop {
            let frame = read_frame(&mut socket).await?;
            match frame.kind {
                RST_STREAM => {
                    assert_eq!(frame.stream, stream);
                    assert_eq!(frame.payload, 5_u32.to_be_bytes());
                    resets += 1;
                    break;
                }
                GOAWAY => return Ok((resets, frame.payload)),
                PING if frame.flags & 1 == 0 => write_frame(&mut socket, PING, 1, 0, &frame.payload).await?,
                _ => {}
            }
        }
    }
    Err(io::Error::other("peer accepted more than 1024 protocol errors"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_stream_retention_preserves_the_protocol_error_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind isolated peer");
    let address = format!("http://{}", listener.local_addr().expect("peer address"));
    let (attack_tx, attack_rx) = oneshot::channel();
    let mut peer = tokio::spawn(unsolicited_response_peer(listener, attack_rx));
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let (channel, _) = dial_channel(&address, &address, Duration::from_secs(10))
            .await
            .expect("dial real internode channel");
        let mut client = node_service::node_service_client::NodeServiceClient::new(channel);
        for _ in 0..1025 {
            assert!(
                client
                    .read_version(node_service::ReadVersionRequest::default())
                    .await
                    .expect("initial live probe")
                    .into_inner()
                    .success
            );
        }
        attack_tx
            .send(())
            .expect("release duplicate responses after the RPC completes");
        (&mut peer).await
    })
    .await;
    if result.is_err() {
        peer.abort();
        let _ = peer.await;
    }
    let (resets, payload) = result
        .expect("error budget is bounded")
        .expect("peer task")
        .expect("peer exchange");
    assert_eq!(resets, 1024, "the existing local protocol-error reset limit remains enabled");
    assert!(payload.len() >= 8);
    assert_eq!(u32::from_be_bytes(payload[4..8].try_into().expect("GOAWAY reason")), 11);
    assert_eq!(&payload[8..], b"too_many_internal_resets");
}
