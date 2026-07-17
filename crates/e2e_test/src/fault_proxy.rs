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

//! In-process, socket-level network fault-injection proxy for black-box
//! cluster E2E tests (backlog#1325 network fault-injection block).
//!
//! `FaultProxy` binds a TCP listener on a random local port, forwards every
//! accepted connection to a fixed target address, and lets a test flip the
//! forwarding behaviour at runtime. It is the black-box counterpart to the
//! in-process white-box hooks (rename barrier, disk call counters): those
//! `#[cfg(test)]` primitives cannot reach across the process boundary into a
//! spawned RustFS server, whereas this proxy sits on the wire between two
//! nodes and needs no cooperation from the peer.
//!
//! Service targets:
//! - **#1312 / #1319** lock-plane one-way partition and accept-then-blackhole
//!   peer: run a node's lock/RPC port through the proxy, then
//!   [`FaultMode::Partition`] one direction (data plane stays reachable via a
//!   separate direct port) or [`FaultMode::Blackhole`] the whole connection.
//! - **#1327** cross-process replay / tamper harness: the proxy is the wire
//!   seam a replay/tamper filter can later hook into.
//!
//! Supported fault modes:
//! - [`FaultMode::Pass`]: transparent byte forwarding (round-trip identical).
//! - [`FaultMode::Latency`]: delay every forwarded chunk by a fixed duration.
//! - [`FaultMode::Blackhole`]: accept the connection but forward nothing in
//!   either direction and never respond (models an accepted-then-dead peer);
//!   the client observes a read timeout, the proxy never panics.
//! - [`FaultMode::Partition`]: block exactly one direction while the other
//!   keeps flowing (models a lock-RPC one-way partition with a live data
//!   plane).
//!
//! Wiring this into the cluster harness ("expose node port N through a proxy")
//! is intentionally left as a follow-up: the harness multi-drive / 2-pool
//! changes land separately (rustfs#4937), so this block ships the proxy tool
//! plus its local self-tests against a loopback echo server and stays
//! independent of that harness work.
//!
//! # Example
//!
//! ```ignore
//! let proxy = FaultProxy::start(target_addr).await?;
//! let via = proxy.local_addr(); // point the client here instead of `target_addr`
//! proxy.set_mode(FaultMode::Latency(Duration::from_millis(50)));
//! // ... exercise the client ...
//! proxy.set_mode(FaultMode::Partition(Direction::ClientToServer));
//! // ... assert the blocked direction is dead ...
//! proxy.shutdown().await; // releases the listener port
//! ```

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// A single logical direction of a proxied connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Bytes travelling from the connecting client towards the target server
    /// (the forward / request path).
    ClientToServer,
    /// Bytes travelling from the target server back to the client (the return
    /// / response path).
    ServerToClient,
}

/// Runtime-switchable forwarding behaviour of a [`FaultProxy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultMode {
    /// Forward bytes transparently in both directions.
    Pass,
    /// Forward bytes in both directions, but delay every chunk by this
    /// duration before it is written onward.
    Latency(Duration),
    /// Accept connections but forward nothing in either direction and never
    /// respond. Bytes read from either side are drained and discarded so the
    /// proxy never blocks or panics; the peer simply never hears back.
    Blackhole,
    /// Block exactly one direction (drop its bytes) while the other direction
    /// keeps forwarding normally.
    Partition(Direction),
}

/// An async TCP proxy that forwards a random local port to a fixed target and
/// can inject network faults at runtime.
///
/// The proxy owns a background accept loop; each accepted connection is
/// handled by its own task pair (one per direction). [`FaultProxy::shutdown`]
/// stops the accept loop and drops the listener, releasing the bound port.
pub struct FaultProxy {
    listen_addr: SocketAddr,
    target: SocketAddr,
    mode_tx: watch::Sender<FaultMode>,
    shutdown_tx: watch::Sender<bool>,
    accept_task: JoinHandle<()>,
}

impl FaultProxy {
    /// Bind a listener on `127.0.0.1:0` and start forwarding accepted
    /// connections to `target`. Starts in [`FaultMode::Pass`].
    pub async fn start(target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let listen_addr = listener.local_addr()?;

        let (mode_tx, mode_rx) = watch::channel(FaultMode::Pass);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let accept_task = tokio::spawn(accept_loop(listener, target, mode_rx, shutdown_rx));

        Ok(Self {
            listen_addr,
            target,
            mode_tx,
            shutdown_tx,
            accept_task,
        })
    }

    /// Local address the proxy is listening on. Point clients here instead of
    /// the real target to route their traffic through the proxy.
    pub fn local_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// The fixed target address every connection is forwarded to.
    pub fn target_addr(&self) -> SocketAddr {
        self.target
    }

    /// The mode currently in effect.
    pub fn mode(&self) -> FaultMode {
        *self.mode_tx.borrow()
    }

    /// Switch the fault mode at runtime. Takes effect on the next chunk read by
    /// each direction of every live and future connection.
    pub fn set_mode(&self, mode: FaultMode) {
        // A send only fails if every receiver has dropped, i.e. the accept loop
        // and all connections have already ended; there is nothing to steer.
        let _ = self.mode_tx.send(mode);
    }

    /// Stop the accept loop, signal live connections to wind down, and drop the
    /// listener so the bound port is released. Awaits the accept loop so the
    /// port is free once this returns.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.accept_task.await;
    }
}

/// Accept loop: takes new connections until shutdown is signalled, then returns
/// (dropping `listener`, which releases the port).
async fn accept_loop(
    listener: TcpListener,
    target: SocketAddr,
    mode_rx: watch::Receiver<FaultMode>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((client, _peer)) => {
                        tokio::spawn(handle_connection(
                            client,
                            target,
                            mode_rx.clone(),
                            shutdown_rx.clone(),
                        ));
                    }
                    // A transient accept error should not tear the proxy down.
                    Err(_) => continue,
                }
            }
        }
    }
}

/// Bridge one accepted client connection to a fresh connection to the target,
/// running an independent pump per direction.
async fn handle_connection(
    client: TcpStream,
    target: SocketAddr,
    mode_rx: watch::Receiver<FaultMode>,
    shutdown_rx: watch::Receiver<bool>,
) {
    let server = match TcpStream::connect(target).await {
        Ok(s) => s,
        // Target unreachable: nothing to bridge; drop the client.
        Err(_) => return,
    };

    let (client_rd, client_wr) = client.into_split();
    let (server_rd, server_wr) = server.into_split();

    let up = tokio::spawn(pump(
        Direction::ClientToServer,
        client_rd,
        server_wr,
        mode_rx.clone(),
        shutdown_rx.clone(),
    ));
    let down = tokio::spawn(pump(Direction::ServerToClient, server_rd, client_wr, mode_rx, shutdown_rx));

    let _ = up.await;
    let _ = down.await;
}

/// Copy bytes from `from` to `to` for a single direction, applying the current
/// [`FaultMode`]. Returns when the source hits EOF/error, a write fails, or
/// shutdown is signalled.
async fn pump<R, W>(
    dir: Direction,
    mut from: R,
    mut to: W,
    mut mode_rx: watch::Receiver<FaultMode>,
    mut shutdown_rx: watch::Receiver<bool>,
) where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => break,
            read = from.read(&mut buf) => match read {
                Ok(0) => break,        // clean EOF
                Ok(n) => n,
                Err(_) => break,       // reset / error: end this direction
            },
        };

        // Snapshot the mode without holding the borrow across an await.
        let mode = *mode_rx.borrow_and_update();
        match mode {
            // Whole connection is a black hole: drain and drop.
            FaultMode::Blackhole => continue,
            // This direction is partitioned off: drop its bytes; the peer keeps
            // reading nothing while the other direction may still flow.
            FaultMode::Partition(blocked) if blocked == dir => continue,
            FaultMode::Latency(delay) => {
                tokio::time::sleep(delay).await;
                if to.write_all(&buf[..n]).await.is_err() {
                    break;
                }
                if to.flush().await.is_err() {
                    break;
                }
            }
            FaultMode::Pass | FaultMode::Partition(_) => {
                if to.write_all(&buf[..n]).await.is_err() {
                    break;
                }
                if to.flush().await.is_err() {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::Mutex;

    /// A loopback echo server that records every byte it receives, so tests can
    /// distinguish "the server never got it" (client->server blocked) from "the
    /// server got it but the reply never came back" (server->client blocked).
    struct EchoServer {
        addr: SocketAddr,
        received: Arc<Mutex<Vec<u8>>>,
        _task: JoinHandle<()>,
    }

    impl EchoServer {
        async fn start() -> io::Result<Self> {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let addr = listener.local_addr()?;
            let received = Arc::new(Mutex::new(Vec::new()));
            let received_task = received.clone();
            let task = tokio::spawn(async move {
                loop {
                    let Ok((mut sock, _)) = listener.accept().await else {
                        break;
                    };
                    let received_conn = received_task.clone();
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 16 * 1024];
                        loop {
                            match sock.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => {
                                    received_conn.lock().await.extend_from_slice(&buf[..n]);
                                    if sock.write_all(&buf[..n]).await.is_err() {
                                        break;
                                    }
                                    let _ = sock.flush().await;
                                }
                            }
                        }
                    });
                }
            });
            Ok(Self {
                addr,
                received,
                _task: task,
            })
        }

        async fn received(&self) -> Vec<u8> {
            self.received.lock().await.clone()
        }
    }

    /// Write `payload`, then try to read up to `want` bytes with a deadline.
    /// Returns the bytes actually received before the timeout (possibly empty).
    async fn write_then_read(stream: &mut TcpStream, payload: &[u8], want: usize, timeout: Duration) -> Vec<u8> {
        stream.write_all(payload).await.expect("client write");
        stream.flush().await.expect("client flush");

        let mut out = Vec::new();
        let mut buf = vec![0u8; want.max(1)];
        let deadline = tokio::time::Instant::now() + timeout;
        while out.len() < want {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, stream.read(&mut buf)).await {
                Ok(Ok(0)) => break, // peer closed
                Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
                Ok(Err(_)) => break, // connection error
                Err(_) => break,     // read timed out
            }
        }
        out
    }

    #[tokio::test]
    async fn pass_mode_round_trips_bytes() {
        let echo = EchoServer::start().await.unwrap();
        let proxy = FaultProxy::start(echo.addr).await.unwrap();
        assert_eq!(proxy.mode(), FaultMode::Pass);

        let mut client = TcpStream::connect(proxy.local_addr()).await.unwrap();
        let payload = b"hello-fault-proxy";
        let got = write_then_read(&mut client, payload, payload.len(), Duration::from_secs(5)).await;

        assert_eq!(got, payload, "pass mode must forward bytes unchanged");
        assert_eq!(echo.received().await, payload, "server must have received the bytes");

        proxy.shutdown().await;
    }

    #[tokio::test]
    async fn latency_mode_delays_forwarding() {
        // Loose lower bound only: sleep() guarantees *at least* the delay, so
        // the assertion cannot flake on a slow machine, and we never assert an
        // upper bound.
        let delay = Duration::from_millis(200);
        let echo = EchoServer::start().await.unwrap();
        let proxy = FaultProxy::start(echo.addr).await.unwrap();
        proxy.set_mode(FaultMode::Latency(delay));

        let mut client = TcpStream::connect(proxy.local_addr()).await.unwrap();
        let payload = b"delayed";
        let start = Instant::now();
        let got = write_then_read(&mut client, payload, payload.len(), Duration::from_secs(5)).await;
        let elapsed = start.elapsed();

        assert_eq!(got, payload, "latency mode must still deliver the bytes");
        // One delay on the request path plus one on the echo return path: the
        // round trip must exceed at least a single configured delay.
        assert!(elapsed >= delay, "round trip {elapsed:?} shorter than configured delay {delay:?}");

        proxy.shutdown().await;
    }

    #[tokio::test]
    async fn blackhole_mode_yields_no_response_and_no_panic() {
        let echo = EchoServer::start().await.unwrap();
        let proxy = FaultProxy::start(echo.addr).await.unwrap();
        proxy.set_mode(FaultMode::Blackhole);

        let mut client = TcpStream::connect(proxy.local_addr()).await.unwrap();
        let got = write_then_read(&mut client, b"into-the-void", 1, Duration::from_millis(300)).await;

        assert!(got.is_empty(), "blackhole mode must not return any bytes, got {got:?}");
        assert!(echo.received().await.is_empty(), "blackhole must not forward to the server");

        // Proxy is still alive and steerable after a blackholed connection.
        proxy.set_mode(FaultMode::Pass);
        proxy.shutdown().await;
    }

    #[tokio::test]
    async fn partition_client_to_server_blocks_request_path() {
        let echo = EchoServer::start().await.unwrap();
        let proxy = FaultProxy::start(echo.addr).await.unwrap();
        proxy.set_mode(FaultMode::Partition(Direction::ClientToServer));

        let mut client = TcpStream::connect(proxy.local_addr()).await.unwrap();
        let got = write_then_read(&mut client, b"blocked-request", 1, Duration::from_millis(300)).await;

        assert!(got.is_empty(), "client->server partition must yield no echo");
        assert!(
            echo.received().await.is_empty(),
            "client->server partition must stop bytes from reaching the server"
        );

        proxy.shutdown().await;
    }

    #[tokio::test]
    async fn partition_server_to_client_blocks_only_return_path() {
        let echo = EchoServer::start().await.unwrap();
        let proxy = FaultProxy::start(echo.addr).await.unwrap();
        proxy.set_mode(FaultMode::Partition(Direction::ServerToClient));

        let mut client = TcpStream::connect(proxy.local_addr()).await.unwrap();
        let payload = b"one-way";
        let got = write_then_read(&mut client, payload, 1, Duration::from_millis(300)).await;

        // Client hears nothing back...
        assert!(got.is_empty(), "server->client partition must block the reply");
        // ...but the request path is live, so the server did receive the bytes.
        // Poll briefly to avoid racing the forward direction.
        let mut server_saw = Vec::new();
        for _ in 0..30 {
            server_saw = echo.received().await;
            if !server_saw.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(server_saw, payload, "server->client partition must leave the request path intact");

        proxy.shutdown().await;
    }

    #[tokio::test]
    async fn set_mode_switches_behaviour_at_runtime() {
        let echo = EchoServer::start().await.unwrap();
        let proxy = FaultProxy::start(echo.addr).await.unwrap();

        // Start passing: first exchange round-trips.
        let mut client = TcpStream::connect(proxy.local_addr()).await.unwrap();
        let first = write_then_read(&mut client, b"first", 5, Duration::from_secs(5)).await;
        assert_eq!(first, b"first", "initial pass exchange must round-trip");

        // Flip to blackhole on the same live connection: the next write gets no reply.
        proxy.set_mode(FaultMode::Blackhole);
        let second = write_then_read(&mut client, b"second", 1, Duration::from_millis(300)).await;
        assert!(second.is_empty(), "after switching to blackhole the live connection must go silent");

        proxy.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_releases_the_listener_port() {
        let echo = EchoServer::start().await.unwrap();
        let proxy = FaultProxy::start(echo.addr).await.unwrap();
        let addr = proxy.local_addr();

        proxy.shutdown().await;

        // The port must be free to bind again once shutdown returns.
        let rebind = TcpListener::bind(addr).await;
        assert!(rebind.is_ok(), "shutdown must release the listener port {addr}");
    }
}
