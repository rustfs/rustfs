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

use crate::rpc::{TONIC_RPC_PREFIX, gen_signature_headers};
use http::Method;
use opentelemetry::{global, propagation::Injector, trace::TraceContextExt};
use rustfs_common::GLOBAL_CONN_MAP;
use rustfs_protos::{create_new_channel, proto_gen::node_service::node_service_client::NodeServiceClient};
use std::error::Error;
use tonic::{service::interceptor::InterceptedService, transport::Channel};
use tracing::{Span, debug};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// 3. Subsequent calls will attempt fresh connections
/// 4. If node is still down, connection will fail fast (3s timeout)
pub async fn node_service_time_out_client(
    addr: &String,
    interceptor: TonicInterceptor,
) -> Result<NodeServiceClient<InterceptedService<Channel, TonicInterceptor>>, Box<dyn Error>> {
    // Try to get cached channel
    let cached_channel = { GLOBAL_CONN_MAP.read().await.get(addr).cloned() };

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

    Ok(NodeServiceClient::with_interceptor(channel, interceptor))
}

pub async fn node_service_time_out_client_no_auth(
    addr: &String,
) -> Result<NodeServiceClient<InterceptedService<Channel, TonicInterceptor>>, Box<dyn Error>> {
    node_service_time_out_client(addr, TonicInterceptor::NoOp(NoOpInterceptor)).await
}

pub struct TonicSignatureInterceptor;

struct MetadataInjector<'a> {
    metadata: &'a mut tonic::metadata::MetadataMap,
}

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(meta_key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) else {
            return;
        };
        let Ok(meta_value) = tonic::metadata::MetadataValue::try_from(value.as_str()) else {
            return;
        };
        self.metadata.insert(meta_key, meta_value);
    }
}

fn inject_trace_context_metadata(req: &mut tonic::Request<()>) {
    let current_context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        let mut injector = MetadataInjector {
            metadata: req.metadata_mut(),
        };
        propagator.inject_context(&current_context, &mut injector);
    });
}

fn inject_request_id_metadata(req: &mut tonic::Request<()>) {
    let request_id_key = tonic::metadata::MetadataKey::from_static("x-request-id");
    if req.metadata().contains_key(&request_id_key) {
        return;
    }

    let current_context = Span::current().context();
    let current_span = current_context.span();
    let span_context = current_span.span_context();
    if !span_context.is_valid() {
        return;
    }

    let trace_id = span_context.trace_id().to_string();
    let Ok(value) = tonic::metadata::MetadataValue::try_from(trace_id.as_str()) else {
        return;
    };
    req.metadata_mut().insert(request_id_key, value);
}

impl tonic::service::Interceptor for TonicSignatureInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let headers = gen_signature_headers(TONIC_RPC_PREFIX, &Method::GET);
        req.metadata_mut().as_mut().extend(headers);
        inject_trace_context_metadata(&mut req);
        inject_request_id_metadata(&mut req);
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
    use tonic::service::Interceptor;

    #[test]
    fn test_signature_interceptor_keeps_auth_headers() {
        let mut interceptor = TonicSignatureInterceptor;
        let req = tonic::Request::new(());

        let req = interceptor.call(req).expect("interceptor call should succeed");

        assert!(req.metadata().contains_key("x-rustfs-signature"));
        assert!(req.metadata().contains_key("x-rustfs-timestamp"));
    }

    #[test]
    fn test_signature_interceptor_may_inject_request_id() {
        let mut interceptor = TonicSignatureInterceptor;
        let req = tonic::Request::new(());

        let span = tracing::info_span!("grpc-rpc-test-span");
        let _guard = span.enter();
        let req = interceptor.call(req).expect("interceptor call should succeed");

        if let Some(v) = req.metadata().get("x-request-id") {
            assert!(!v.as_encoded_bytes().is_empty());
        }
    }
}
