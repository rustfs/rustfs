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

use crate::cluster::rpc::http_auth::RPC_CONTENT_SHA256_HEADER;
use crate::cluster::rpc::{gen_tonic_signature_headers, normalize_tonic_rpc_audience};
use crate::disk::error::{DiskError, Error as DiskErrorType};
use crate::runtime::sources as runtime_sources;
use http::Uri;
use rustfs_protos::{
    ChannelClass, create_new_channel, get_channel_for_class, proto_gen::node_service::node_service_client::NodeServiceClient,
};
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
    // Default to the latency-sensitive control channel; bulk `bytes` RPCs opt in via the
    // `_for_class` variant below (grpc-optimization P1).
    node_service_time_out_client_for_class(addr, interceptor, ChannelClass::Control).await
}

/// Build a `NodeServiceClient` bound to the [`ChannelClass`]-appropriate channel for `addr`.
///
/// Bulk `bytes`-carrying RPCs (ReadAll/WriteAll/ReadMultiple/BatchReadVersion) pass
/// [`ChannelClass::Bulk`] so, when channel isolation is enabled, they are physically isolated
/// from lock/health RPCs; everything else uses [`ChannelClass::Control`]. When isolation is
/// disabled the two classes resolve to the same cached channel, i.e. legacy behavior.
pub async fn node_service_time_out_client_for_class(
    addr: &String,
    interceptor: TonicInterceptor,
    class: ChannelClass,
) -> Result<NodeServiceClient<InterceptedService<Channel, TonicInterceptor>>, Box<dyn Error>> {
    let interceptor = interceptor.with_rpc_audience(addr)?;
    let channel = match class {
        ChannelClass::Control => match runtime_sources::cached_node_channel(addr).await {
            Some(channel) => {
                debug!("Using cached gRPC channel for: {}", addr);
                channel
            }
            // No cached connection, create new one.
            None => create_new_channel(addr).await?,
        },
        ChannelClass::Bulk => get_channel_for_class(addr, ChannelClass::Bulk).await?,
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

pub struct TonicSignatureInterceptor {
    audience: Option<String>,
}

impl tonic::service::Interceptor for TonicSignatureInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let method = req
            .extensions()
            .get::<tonic::GrpcMethod<'_>>()
            .ok_or_else(|| tonic::Status::unauthenticated("Missing gRPC method metadata"))?;
        let audience = self
            .audience
            .as_deref()
            .ok_or_else(|| tonic::Status::unauthenticated("Missing gRPC audience"))?;
        let content_sha256 = req
            .metadata()
            .get(RPC_CONTENT_SHA256_HEADER)
            .and_then(|value| value.to_str().ok());
        let headers = gen_tonic_signature_headers(audience, method.service(), method.method(), content_sha256)
            .map_err(|_| tonic::Status::unauthenticated("No valid auth token"))?;
        req.metadata_mut().as_mut().extend(headers);
        inject_trace_context_into_metadata(req.metadata_mut());
        inject_request_id_into_metadata(req.metadata_mut());
        Ok(req)
    }
}

pub fn gen_tonic_signature_interceptor() -> TonicSignatureInterceptor {
    TonicSignatureInterceptor { audience: None }
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

impl TonicInterceptor {
    fn with_rpc_audience(mut self, addr: &str) -> std::io::Result<Self> {
        if let Self::Signature(interceptor) = &mut self {
            let uri = addr
                .parse::<Uri>()
                .map_err(|_| std::io::Error::other("Invalid gRPC peer URI"))?;
            let audience = uri
                .authority()
                .map(|authority| normalize_tonic_rpc_audience(authority.as_str()))
                .ok_or_else(|| std::io::Error::other("Missing gRPC peer authority"))?;
            interceptor.audience = Some(audience?);
        }
        Ok(self)
    }
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

    fn test_request() -> tonic::Request<()> {
        let mut request = tonic::Request::new(());
        request
            .extensions_mut()
            .insert(tonic::GrpcMethod::new("node_service.NodeService", "Ping"));
        request
    }

    fn test_interceptor() -> TonicSignatureInterceptor {
        TonicSignatureInterceptor {
            audience: Some("node-a:9000".to_string()),
        }
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
        let mut interceptor = test_interceptor();
        let req = test_request();

        let req = interceptor.call(req).expect("interceptor call should succeed");

        assert!(req.metadata().contains_key("x-rustfs-signature"));
        assert!(req.metadata().contains_key("x-rustfs-timestamp"));
        assert!(req.metadata().contains_key("x-rustfs-rpc-signature-v2"));
        assert!(req.metadata().contains_key("x-rustfs-rpc-nonce"));
        assert!(
            crate::cluster::rpc::verify_tonic_rpc_signature(
                "node-a:9000",
                "/node_service.NodeService/Ping",
                req.metadata().as_ref(),
            )
            .is_ok(),
            "interceptor signature should bind the configured peer audience and generated method"
        );
    }

    #[test]
    fn test_signature_interceptor_binds_audience_from_peer_uri() {
        let interceptor = TonicInterceptor::Signature(gen_tonic_signature_interceptor())
            .with_rpc_audience("http://node-a:9000")
            .expect("peer URI should provide an audience");
        let TonicInterceptor::Signature(interceptor) = interceptor else {
            panic!("signature interceptor variant should be preserved");
        };

        assert_eq!(interceptor.audience.as_deref(), Some("node-a:9000"));
    }

    #[test]
    fn test_signature_interceptor_requires_generated_method_metadata() {
        ensure_test_rpc_secret();
        let mut interceptor = test_interceptor();
        let error = interceptor
            .call(tonic::Request::new(()))
            .expect_err("requests without an exact generated method must fail closed");

        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert_eq!(error.message(), "Missing gRPC method metadata");
    }

    #[test]
    fn test_signature_interceptor_may_inject_request_id() {
        ensure_test_rpc_secret();
        let mut interceptor = test_interceptor();
        let req = test_request();

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
        let mut interceptor = test_interceptor();
        let req = test_request();

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
