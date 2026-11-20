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

use super::proto_gen::node_service::{PingRequest, node_service_client::NodeServiceClient};
use super::{ChannelClass, INTERNODE_RPC_CONCURRENCY_LIMIT, bulk_cache_key, dial_channel};
use std::convert::Infallible;
use std::fmt::Debug;
use std::future::pending;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tonic::body::Body;
use tonic::codegen::{http::Response, tokio_stream::wrappers::TcpListenerStream};
use tracing::{Event, Subscriber, field::Field};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

// Tonic does not expose the applied h2 retention limit. Observe h2's local
// accounting on a normal cancellation batch, before the server replies.
struct ResetAccounting(mpsc::UnboundedSender<bool>);

impl<S: Subscriber> Layer<S> for ResetAccounting {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        event.record(&mut |field: &Field, value: &dyn Debug| {
            if field.name() == "message" {
                let message = format!("{value:?}");
                if message.starts_with("enqueue_reset_expiration; ") {
                    self.0
                        .send(message.starts_with("enqueue_reset_expiration; added "))
                        .expect("retention receiver");
                }
            }
        });
    }
}

async fn check_cancelled_batch(class: ChannelClass) {
    let (events, mut received) = mpsc::unbounded_channel();
    let observer = ResetAccounting(events).with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
        metadata.target() == "h2::proto::streams::recv"
    }));
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(observer));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test server");
    let address = format!("http://{}", listener.local_addr().expect("read listener address"));
    let (started, mut dispatches) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let service = tower::service_fn(move |_| {
            started.send(()).expect("record dispatch");
            pending::<Result<Response<Body>, Infallible>>()
        });
        tonic::transport::Server::builder()
            .serve_with_incoming(service, TcpListenerStream::new(listener))
            .await
    });
    let key = match class {
        ChannelClass::Control => address.clone(),
        ChannelClass::Bulk => bulk_cache_key(&address, 0),
    };
    let (channel, _) = dial_channel(&address, &key, Duration::from_secs(10))
        .await
        .expect("dial test server");
    let mut calls = JoinSet::new();
    for _ in 0..INTERNODE_RPC_CONCURRENCY_LIMIT {
        let mut client = NodeServiceClient::new(channel.clone());
        calls.spawn(async move { client.ping(PingRequest::default()).await });
    }
    for _ in 0..INTERNODE_RPC_CONCURRENCY_LIMIT {
        dispatches
            .recv()
            .await
            .expect("all RPCs must reach the service before cancellation");
    }
    calls.shutdown().await;
    let mut retained = 0;
    for _ in 0..INTERNODE_RPC_CONCURRENCY_LIMIT {
        retained += usize::from(received.recv().await.expect("observe accounting for each cancelled stream"));
    }
    assert_eq!(
        retained, INTERNODE_RPC_CONCURRENCY_LIMIT,
        "one complete RPC batch must fit in local reset retention"
    );
    server.abort();
}

#[tokio::test]
async fn control_channel_retains_cancelled_batch() {
    tokio::time::timeout(Duration::from_secs(15), check_cancelled_batch(ChannelClass::Control))
        .await
        .expect("control batch completes within the test deadline");
}

#[tokio::test]
async fn bulk_channel_retains_cancelled_batch() {
    tokio::time::timeout(Duration::from_secs(15), check_cancelled_batch(ChannelClass::Bulk))
        .await
        .expect("bulk batch completes within the test deadline");
}
