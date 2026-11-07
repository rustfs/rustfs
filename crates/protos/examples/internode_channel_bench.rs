// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Local transport experiment using the production channel pool and two loopback tonic peers.
//! Arguments: bulk workers (16), requests per bulk worker (64), payload bytes (262144), control workers (0).

use rustfs_protos::proto_gen::node_service::{PingRequest, PingResponse, node_service_client::NodeServiceClient};
use rustfs_protos::{ChannelClass, get_channel_for_class};
use std::convert::Infallible;
use std::io::Error;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tonic::codegen::{
    Body, BoxFuture, Service, StdError, http,
    tokio_stream::{StreamExt, wrappers::TcpListenerStream},
};

#[derive(Clone)]
struct Echo;

impl tonic::server::NamedService for Echo {
    const NAME: &'static str = "node_service.NodeService";
}

impl tonic::server::UnaryService<PingRequest> for Echo {
    type Response = PingResponse;
    type Future = BoxFuture<tonic::Response<PingResponse>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<PingRequest>) -> Self::Future {
        Box::pin(async move {
            let request = request.into_inner();
            Ok(tonic::Response::new(PingResponse {
                version: request.version,
                body: request.body,
            }))
        })
    }
}

impl<B> Service<http::Request<B>> for Echo
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::<PingResponse, PingRequest>::default();
            let mut grpc = tonic::server::Grpc::new(codec)
                .max_decoding_message_size(rustfs_protos::DEFAULT_GRPC_SERVER_MESSAGE_LEN)
                .max_encoding_message_size(rustfs_protos::DEFAULT_GRPC_SERVER_MESSAGE_LEN);
            Ok(grpc.unary(Echo, request).await)
        })
    }
}

async fn roundtrip(address: &str, class: ChannelClass, payload: prost::bytes::Bytes) -> Result<(), StdError> {
    let channel = get_channel_for_class(address, class)
        .await
        .map_err(|error| Error::other(error.to_string()))?;
    let response = NodeServiceClient::new(channel)
        .max_decoding_message_size(rustfs_protos::DEFAULT_GRPC_SERVER_MESSAGE_LEN)
        .max_encoding_message_size(rustfs_protos::DEFAULT_GRPC_SERVER_MESSAGE_LEN)
        .ping(PingRequest {
            version: 1,
            body: payload.clone(),
        })
        .await?
        .into_inner();
    if response.version != 1 || response.body != payload {
        return Err(Error::other("echo response content mismatch").into());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), StdError> {
    let mut args = std::env::args().skip(1);
    let mut argument =
        |default| -> Result<usize, StdError> { Ok(args.next().map(|value| value.parse()).transpose()?.unwrap_or(default)) };
    let workers = argument(16)?;
    let requests = argument(64)?;
    let bytes = argument(262_144)?;
    let control_workers = argument(0)?;
    let total_workers = workers
        .checked_add(control_workers)
        .ok_or_else(|| Error::other("worker count overflow"))?;
    if args.next().is_some()
        || workers == 0
        || !(1..=256).contains(&total_workers)
        || requests == 0
        || bytes == 0
        || bytes > 32 * 1024 * 1024
        || total_workers.checked_mul(requests).is_none_or(|count| count > 1_000_000)
        || workers.checked_mul(bytes).is_none_or(|size| size > 512 * 1024 * 1024)
    {
        return Err(Error::other("usage: internode_channel_bench [bulk-workers] [requests-per-bulk-worker] [payload-bytes] [control-workers]; maximum 256 workers, 1M samples, 32 MiB/message and 512 MiB concurrent payload").into());
    }

    let mut addresses = Vec::new();
    let mut servers = Vec::new();
    let mut connection_counts = Vec::new();
    for _ in 0..2 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        addresses.push(format!("http://{}", listener.local_addr()?));
        let accepted = Arc::new(AtomicUsize::new(0));
        connection_counts.push(Arc::clone(&accepted));
        let incoming = TcpListenerStream::new(listener).map(move |socket| {
            if socket.is_ok() {
                accepted.fetch_add(1, Ordering::Relaxed);
            }
            socket
        });
        servers.push(tokio::spawn(async move {
            tonic::transport::Server::builder()
                .initial_stream_window_size(rustfs_config::DEFAULT_H2_INITIAL_STREAM_WINDOW_SIZE)
                .initial_connection_window_size(rustfs_config::DEFAULT_H2_INITIAL_CONN_WINDOW_SIZE)
                .max_concurrent_streams(Some(rustfs_config::DEFAULT_H2_MAX_CONCURRENT_STREAMS))
                .max_frame_size(rustfs_config::DEFAULT_H2_MAX_FRAME_SIZE)
                .http2_max_header_list_size(rustfs_config::DEFAULT_H2_MAX_HEADER_LIST_SIZE)
                .http2_keepalive_interval(Some(Duration::from_secs(rustfs_config::DEFAULT_H2_KEEP_ALIVE_INTERVAL)))
                .http2_keepalive_timeout(Some(Duration::from_secs(rustfs_config::DEFAULT_H2_KEEP_ALIVE_TIMEOUT)))
                .add_service(Echo)
                .serve_with_incoming(incoming)
                .await
        }));
    }
    let payload = prost::bytes::Bytes::from(vec![0x5a; bytes]);
    // Consecutive warmups fill both default bulk slots without depending on peer interleaving.
    for address in &addresses {
        for _ in 0..4 {
            roundtrip(address, ChannelClass::Bulk, payload.clone()).await?;
        }
        if control_workers > 0 {
            roundtrip(address, ChannelClass::Control, prost::bytes::Bytes::from_static(b"control")).await?;
        }
    }
    let addresses = Arc::new(addresses);
    let barrier = Arc::new(tokio::sync::Barrier::new(total_workers + 1));
    let mut tasks = tokio::task::JoinSet::new();
    let finished_bulk = Arc::new(AtomicUsize::new(0));
    let control_sample_limit = (1_000_000 - workers * requests) / control_workers.max(1);
    for worker in 0..total_workers {
        let addresses = Arc::clone(&addresses);
        let barrier = Arc::clone(&barrier);
        let control = worker >= workers;
        let class = if control { ChannelClass::Control } else { ChannelClass::Bulk };
        let payload = if control {
            prost::bytes::Bytes::from_static(b"control")
        } else {
            payload.clone()
        };
        let finished_bulk = Arc::clone(&finished_bulk);
        tasks.spawn(async move {
            let mut latencies = Vec::with_capacity(requests);
            barrier.wait().await;
            let started = Instant::now();
            loop {
                if control {
                    if !latencies.is_empty() && finished_bulk.load(Ordering::Acquire) == workers {
                        break;
                    }
                    if latencies.len() >= control_sample_limit {
                        return Err(
                            Error::other("control sample budget exhausted; reduce the workload or control workers").into()
                        );
                    }
                } else if latencies.len() == requests {
                    break;
                }
                let start = Instant::now();
                roundtrip(&addresses[(worker + latencies.len()) % addresses.len()], class, payload.clone()).await?;
                latencies.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            if !control {
                finished_bulk.fetch_add(1, Ordering::Release);
            }
            Ok::<_, StdError>((control, started, Instant::now(), latencies))
        });
    }
    barrier.wait().await;
    let mut bulk_latencies = Vec::with_capacity(workers * requests);
    let mut control_latencies = Vec::new();
    let mut first = None::<Instant>;
    let mut last = None::<Instant>;
    while let Some(result) = tasks.join_next().await {
        let (control, started, finished, samples) = result??;
        first = Some(first.map_or(started, |previous| previous.min(started)));
        last = Some(last.map_or(finished, |previous| previous.max(finished)));
        if control {
            control_latencies.extend(samples);
        } else {
            bulk_latencies.extend(samples);
        }
    }
    let first = first.ok_or_else(|| Error::other("no worker started"))?;
    let last = last.ok_or_else(|| Error::other("no worker completed"))?;
    let seconds = last.duration_since(first).as_secs_f64();
    let summary = |latencies: &mut [f64]| {
        latencies.sort_unstable_by(f64::total_cmp);
        let percentile =
            |percent: usize| (!latencies.is_empty()).then(|| latencies[(latencies.len() * percent).div_ceil(100) - 1]);
        serde_json::json!({"requests": latencies.len(), "p50_ms": percentile(50), "p95_ms": percentile(95), "p99_ms": percentile(99)})
    };
    let payload_mib = f64::from(u32::try_from(bytes)?) / (1024.0 * 1024.0);
    println!(
        "{}",
        serde_json::json!({
            "bulk_workers": workers, "control_workers": control_workers, "payload_bytes": bytes, "control_payload_bytes": 7,
            "elapsed_seconds": seconds,
            "roundtrip_payload_mib_per_second": payload_mib * f64::from(u32::try_from(bulk_latencies.len())?) / seconds,
            "quantile_method": "nearest_rank", "bulk": summary(&mut bulk_latencies), "control": summary(&mut control_latencies),
            "connections_per_peer": connection_counts.iter().map(|count| count.load(Ordering::Relaxed)).collect::<Vec<_>>()
        })
    );
    rustfs_common::clear_all_connections().await;
    for server in servers {
        server.abort();
        let _ = server.await;
    }
    Ok(())
}
