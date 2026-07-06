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

use crate::cluster::rpc::{TONIC_RPC_PREFIX, gen_signature_headers};
use crate::disk::error::{DiskError, Error as DiskErrorType};
use crate::runtime::sources as runtime_sources;
use http::Method;
use rustfs_protos::{create_new_channel, proto_gen::node_service::node_service_client::NodeServiceClient};
use std::{error::Error, io::ErrorKind};
use tonic::{service::interceptor::InterceptedService, transport::Channel};
use tracing::debug;

use super::context_propagation::{inject_request_id_into_metadata, inject_trace_context_into_metadata};

/// 3. Subsequent calls will attempt fresh connections
/// 4. If node is still down, connection will fail fast (3s timeout)
pub async fn node_service_time_out_client(
    addr: &String,
    interceptor: TonicInterceptor,
) -> Result<NodeServiceClient<InterceptedService<Channel, TonicInterceptor>>, Box<dyn Error>> {
    // Try to get cached channel
    let cached_channel = runtime_sources::cached_node_channel(addr).await;

    let channel = match cached_channel {
        Some(channel) => {
            debug!("Using cached gRPC channel for: {}", addr);
            channel
        }
        None => {
            // No cached connection, create new one
            create_new_channel(addr).await?
        }
    };

    let max_message_size = rustfs_protos::internode_rpc_max_message_size();
    Ok(NodeServiceClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size))
}

pub async fn node_service_time_out_client_no_auth(
    addr: &String,
) -> Result<NodeServiceClient<InterceptedService<Channel, TonicInterceptor>>, Box<dyn Error>> {
    node_service_time_out_client(addr, TonicInterceptor::NoOp(NoOpInterceptor)).await
}

pub(crate) fn is_network_like_disk_error(err: &DiskErrorType) -> bool {
    match err {
        DiskError::Timeout => true,
        DiskError::Io(io_err) => {
            if matches!(
                io_err.kind(),
                ErrorKind::TimedOut
                    | ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::BrokenPipe
                    | ErrorKind::NotConnected
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::UnexpectedEof
            ) {
                return true;
            }

            let message = io_err.to_string().to_ascii_lowercase();
            [
                "transport error",
                "unavailable",
                "error trying to connect",
                "connection refused",
                "connection reset",
                "broken pipe",
                "not connected",
                "unexpected eof",
                "timed out",
                "deadline has elapsed",
                "connection closed",
                "connection aborted",
                "tcp connect error",
            ]
            .iter()
            .any(|needle| message.contains(needle))
        }
        _ => false,
    }
}

pub struct TonicSignatureInterceptor;

impl tonic::service::Interceptor for TonicSignatureInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let headers = gen_signature_headers(TONIC_RPC_PREFIX, &Method::GET)
            .map_err(|_| tonic::Status::unauthenticated("No valid auth token"))?;
        req.metadata_mut().as_mut().extend(headers);
        inject_trace_context_into_metadata(req.metadata_mut());
        inject_request_id_into_metadata(req.metadata_mut());
        Ok(req)
    }
}

pub fn gen_tonic_signature_interceptor() -> TonicSignatureInterceptor {
    TonicSignatureInterceptor
}

pub struct NoOpInterceptor;

impl tonic::service::Interceptor for NoOpInterceptor {
    fn call(&mut self, req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        Ok(req)
    }
}

pub enum TonicInterceptor {
    Signature(TonicSignatureInterceptor),
    NoOp(NoOpInterceptor),
}

impl tonic::service::Interceptor for TonicInterceptor {
    fn call(&mut self, req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        match self {
            TonicInterceptor::Signature(interceptor) => interceptor.call(req),
            TonicInterceptor::NoOp(interceptor) => interceptor.call(req),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::global;
    use opentelemetry::trace::{SpanContext, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider as _};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tonic::service::Interceptor;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::{Registry, layer::SubscriberExt};

    fn ensure_test_rpc_secret() {
        runtime_sources::ensure_test_rpc_secret();
    }

    fn with_trace_parent<F>(trace_id_hex: &str, f: F)
    where
        F: FnOnce(),
    {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("rpc-client-tests");
        let subscriber = Registry::default().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("rpc-client-test-span");
            let trace_id = TraceId::from_hex(trace_id_hex).expect("trace id should be valid hex");
            let span_id = opentelemetry::trace::SpanId::from_hex("0102030405060708").expect("span id should be valid hex");
            let parent = SpanContext::new(trace_id, span_id, TraceFlags::SAMPLED, true, TraceState::default());
            span.set_parent(opentelemetry::Context::new().with_remote_span_context(parent))
                .expect("failed to set parent context");
            let _guard = span.enter();

            f();
        });
        let _ = provider.shutdown();
    }

    #[test]
    fn test_signature_interceptor_keeps_auth_headers() {
        ensure_test_rpc_secret();
        let mut interceptor = TonicSignatureInterceptor;
        let req = tonic::Request::new(());

        let req = interceptor.call(req).expect("interceptor call should succeed");

        assert!(req.metadata().contains_key("x-rustfs-signature"));
        assert!(req.metadata().contains_key("x-rustfs-timestamp"));
    }

    #[test]
    fn test_signature_interceptor_may_inject_request_id() {
        ensure_test_rpc_secret();
        let mut interceptor = TonicSignatureInterceptor;
        let req = tonic::Request::new(());

        let span = tracing::info_span!("grpc-rpc-test-span");
        let _guard = span.enter();
        let req = interceptor.call(req).expect("interceptor call should succeed");

        if let Some(v) = req.metadata().get("x-request-id") {
            assert!(!v.as_encoded_bytes().is_empty());
        }
    }

    #[test]
    fn test_signature_interceptor_injects_traceparent_metadata() {
        ensure_test_rpc_secret();
        let mut interceptor = TonicSignatureInterceptor;
        let req = tonic::Request::new(());

        with_trace_parent("4bf92f3577b34da6a3ce929d0e0e4736", || {
            let req = interceptor.call(req).expect("interceptor call should succeed");
            let traceparent = req
                .metadata()
                .get("traceparent")
                .and_then(|v| v.to_str().ok())
                .expect("traceparent metadata should be injected");
            assert!(traceparent.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"));
        });
    }
}
