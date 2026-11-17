// Copyright 2026 RustFS Team
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

use super::*;
use crate::vector::protocol::gateway::{Backend, VectorGatewayService, register};
use crate::vector::protocol::{ErrorCode, Request, Response};
use bytes::Bytes;
use http::{Method, Request as HttpRequest, StatusCode};
use http_body_util::BodyExt;
use rustfs_gateway::{
    BoxFuture, ConnectionIntent, DenyAllAuthorizer, ErrorCode as GatewayErrorCode, RegionSet, RequestContextView, S3Service,
    ServiceBuilder, SigService, SigV4Authenticator, StaticCredentials,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct DenyBusiness(Arc<AtomicUsize>);

impl VectorGatewayService for DenyBusiness {
    type Authorization = ();

    fn authorize<'a>(
        &'a self,
        _: &'a RequestContextView,
        _: &'a Request,
    ) -> BoxFuture<'a, Result<Self::Authorization, ProtocolError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(ProtocolError::new(ErrorCode::AccessDenied, "Access denied")) })
    }

    fn execute<'a>(&'a self, _: Self::Authorization, _: Request) -> BoxFuture<'a, Result<Response, ProtocolError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(ProtocolError::new(ErrorCode::AccessDenied, "Access denied")) })
    }
}

fn service(builder: ServiceBuilder, calls: Arc<AtomicUsize>) -> S3Service {
    let credentials = Arc::new(StaticCredentials::new());
    let authenticator = SigV4Authenticator::new(credentials, RegionSet::new(["us-east-1"]).expect("test region"));
    register(
        builder.authenticator(authenticator).authorizer(DenyAllAuthorizer),
        Arc::new(Backend::new(DenyBusiness(calls), "us-east-1".to_owned())),
    )
    .expect("Vector dialect")
    .build()
    .expect("independent service")
}

fn request(method: Method, path: &str) -> HttpRequest<http_body_util::Full<Bytes>> {
    HttpRequest::builder()
        .method(method)
        .uri(path)
        .header("host", "127.0.0.1")
        .body(http_body_util::Full::new(Bytes::new()))
        .expect("in-memory request")
}

#[tokio::test]
async fn authentication_refusal_exposes_typed_error_to_response_filter() {
    fn extension_bounds<T: Clone + Send + Sync + 'static>() {}
    extension_bounds::<S3Error>();
    let seen = Arc::new(AtomicUsize::new(0));
    let seen_filter = Arc::clone(&seen);
    let calls = Arc::new(AtomicUsize::new(0));
    let builder = ServiceBuilder::for_service(SigService::S3Vectors).stage_filter(response_filter(move |_, response| {
        let error = response.extensions().get::<S3Error>().expect("typed authentication refusal");
        assert_eq!(error.code(), Some(&GatewayErrorCode::ACCESS_DENIED));
        assert_eq!(error.status(), StatusCode::FORBIDDEN);
        seen_filter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));
    let response = service(builder, Arc::clone(&calls))
        .call(request(Method::POST, "/GetVectorBucket"))
        .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn route_and_method_refusals_retain_gateway_errors() {
    let calls = Arc::new(AtomicUsize::new(0));
    let service = service(ServiceBuilder::for_service(SigService::S3Vectors), Arc::clone(&calls));
    for (method, path) in [(Method::POST, "/UnknownAction"), (Method::GET, "/GetVectorBucket")] {
        let response = service.call(request(method, path)).await;
        let error = response.extensions().get::<S3Error>().expect("typed route refusal");
        assert_eq!(error.code(), Some(&GatewayErrorCode::NOT_IMPLEMENTED));
        assert_eq!(error.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(response.status(), error.status());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct TransportMarker {
    request_id: http::HeaderValue,
    connection: ConnectionIntent,
}

#[tokio::test]
async fn authentication_json_rewrite_preserves_original_error_and_transport_extensions() {
    let calls = Arc::new(AtomicUsize::new(0));
    let builder = ServiceBuilder::for_service(SigService::S3Vectors)
        .stage_filter(response_filter(|_, response| {
            let marker = TransportMarker {
                request_id: response.headers()["x-amz-request-id"].clone(),
                connection: *response.extensions().get::<ConnectionIntent>().expect("connection intent"),
            };
            response.extensions_mut().insert(marker);
            Ok(())
        }))
        .stage_filter(typed_error_filter(|_, error| {
            assert_eq!(error.code(), Some(&GatewayErrorCode::ACCESS_DENIED));
            Some(ProtocolError::new(ErrorCode::AccessDenied, "Access denied"))
        }));
    let response = service(builder, Arc::clone(&calls))
        .call(request(Method::POST, "/GetVectorBucket"))
        .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(response.headers()["x-amzn-errortype"], "AccessDeniedException");
    let original = response.extensions().get::<S3Error>().expect("original Gateway error");
    assert_eq!(original.code(), Some(&GatewayErrorCode::ACCESS_DENIED));
    let marker = response.extensions().get::<TransportMarker>().expect("transport marker");
    assert_eq!(response.headers()["x-amz-request-id"], marker.request_id);
    assert_eq!(response.extensions().get::<ConnectionIntent>(), Some(&marker.connection));
    assert_eq!(original.connection_intent(), marker.connection);
    let declared_len = response.headers()["content-length"]
        .to_str()
        .expect("length")
        .parse::<usize>()
        .expect("length number");
    let body = response.into_body().collect().await.expect("JSON body").to_bytes();
    assert_eq!(declared_len, body.len());
    let body: serde_json::Value = serde_json::from_slice(&body).expect("protocol JSON");
    assert_eq!(body["__type"], "AccessDeniedException");
    assert_eq!(body["message"], "Access denied");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn head_filter_reads_typed_refusal_before_body_rules_and_final_request_id() {
    let calls = Arc::new(AtomicUsize::new(0));
    let builder = ServiceBuilder::for_service(SigService::S3Vectors).stage_filter(response_filter(|view, response| {
        assert_eq!(view.method(), &Method::HEAD);
        let error = response.extensions().get::<S3Error>().expect("typed HEAD route refusal");
        assert_eq!(error.code(), Some(&GatewayErrorCode::NOT_IMPLEMENTED));
        assert_eq!(error.status(), StatusCode::NOT_IMPLEMENTED);
        assert!(error.message().is_some());
        let marker = TransportMarker {
            request_id: response.headers()["x-amz-request-id"].clone(),
            connection: *response.extensions().get::<ConnectionIntent>().expect("connection intent"),
        };
        response.extensions_mut().insert(marker);
        *response.body_mut() = rustfs_gateway::Body::from_bytes(Bytes::from_static(b"head-filter-body"));
        response
            .headers_mut()
            .insert("content-length", http::HeaderValue::from_static("16"));
        response
            .headers_mut()
            .insert("x-amz-request-id", http::HeaderValue::from_static("changed-by-filter"));
        Ok(())
    }));
    let response = service(builder, Arc::clone(&calls))
        .call(request(Method::HEAD, "/UnknownAction"))
        .await;
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let marker = response.extensions().get::<TransportMarker>().expect("filter observation");
    assert_eq!(response.headers()["x-amz-request-id"], marker.request_id);
    assert_eq!(response.extensions().get::<ConnectionIntent>(), Some(&marker.connection));
    assert!(response.extensions().get::<S3Error>().is_some());
    assert!(response.into_body().collect().await.expect("HEAD body").to_bytes().is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn typed_route_mapper_preserves_gateway_refusal_and_emits_model_json() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mapped = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&mapped);
    let builder = ServiceBuilder::for_service(SigService::S3Vectors).stage_filter(typed_error_filter(move |_, error| {
        assert_eq!(error.code(), Some(&GatewayErrorCode::NOT_IMPLEMENTED));
        observed.fetch_add(1, Ordering::SeqCst);
        // This test supplies the host's classification explicitly. It does not choose
        // the production mapping for an unrecognized method or resource.
        Some(ProtocolError::validation("request", "Unknown operation route"))
    }));
    let service = service(builder, Arc::clone(&calls));
    for (method, path) in [(Method::POST, "/UnknownAction"), (Method::GET, "/GetVectorBucket")] {
        let response = service.call(request(method, path)).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.headers()["x-amzn-errortype"], "ValidationException");
        let original = response.extensions().get::<S3Error>().expect("original refusal");
        assert_eq!(original.code(), Some(&GatewayErrorCode::NOT_IMPLEMENTED));
        assert_eq!(original.status(), StatusCode::NOT_IMPLEMENTED);
        let body = response.into_body().collect().await.expect("error body").to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
        assert_eq!(body["__type"], "ValidationException");
        assert_eq!(body["fieldList"][0]["path"], "request");
    }
    assert_eq!(mapped.load(Ordering::SeqCst), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn response_without_typed_refusal_is_not_classified_from_status() {
    let calls = Arc::new(AtomicUsize::new(0));
    let builder = ServiceBuilder::for_service(SigService::S3Vectors)
        .stage_filter(response_filter(|_, response| {
            response.extensions_mut().remove::<S3Error>();
            *response.status_mut() = StatusCode::BAD_REQUEST;
            *response.body_mut() = rustfs_gateway::Body::from_bytes(Bytes::from_static(b"unchanged body"));
            response.headers_mut().remove("content-length");
            Ok(())
        }))
        .stage_filter(typed_error_filter(|_, _| panic!("HTTP status does not supply a typed refusal")));
    let response = service(builder, Arc::clone(&calls))
        .call(request(Method::POST, "/GetVectorBucket"))
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.extensions().get::<S3Error>().is_none());
    assert_eq!(response.into_body().collect().await.expect("response body").to_bytes(), "unchanged body");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn established_error_mapping_uses_typed_codes_and_service_statuses() {
    for (code, status, name) in [
        (GatewayErrorCode::ACCESS_DENIED, 403, "AccessDeniedException"),
        (GatewayErrorCode::ENTITY_TOO_LARGE, 400, "ValidationException"),
        (GatewayErrorCode::REQUEST_TIMEOUT, 408, "RequestTimeoutException"),
        (GatewayErrorCode::INTERNAL_ERROR, 500, "InternalServerException"),
        (GatewayErrorCode::SERVICE_UNAVAILABLE, 503, "ServiceUnavailableException"),
        (GatewayErrorCode::SLOW_DOWN, 429, "TooManyRequestsException"),
    ] {
        let context =
            rustfs_gateway::ErrorContext::ordinary(rustfs_gateway::HandlerError::new(code.clone(), "Internal diagnostic text"))
                .expect("context-free test refusal");
        let original = S3Error::from(rustfs_gateway::resolve(context, rustfs_gateway::ResponseKind::Other));
        let injected = original.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let builder = ServiceBuilder::for_service(SigService::S3Vectors)
            .stage_filter(response_filter(move |_, response| {
                response.extensions_mut().insert(injected.clone());
                // The discriminator must come from the typed extension.
                *response.status_mut() = StatusCode::IM_A_TEAPOT;
                Ok(())
            }))
            .stage_filter(typed_error_filter(map_gateway_error));
        let response = service(builder, Arc::clone(&calls))
            .call(request(Method::POST, "/GetVectorBucket"))
            .await;
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers()["x-amzn-errortype"], name);
        let preserved = response.extensions().get::<S3Error>().expect("original typed error");
        assert_eq!(preserved.code(), Some(&code));
        assert_eq!(preserved.message(), original.message());
        assert_eq!(preserved.status(), original.status());
        let body = response.into_body().collect().await.expect("JSON error").to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).expect("service JSON");
        assert_eq!(body["__type"], name);
        assert_ne!(body["message"], "Internal diagnostic text");
        if name == "ValidationException" {
            assert_eq!(body["fieldList"][0]["path"], "request");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn unclassified_typed_error_preserves_original_response() {
    let calls = Arc::new(AtomicUsize::new(0));
    let builder = ServiceBuilder::for_service(SigService::S3Vectors).stage_filter(typed_error_filter(map_gateway_error));
    let response = service(builder, Arc::clone(&calls))
        .call(request(Method::POST, "/UnknownAction"))
        .await;
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(response.headers()["content-type"], "application/xml");
    assert!(response.headers().get("x-amzn-errortype").is_none());
    let original = response.extensions().get::<S3Error>().expect("unclassified refusal");
    assert_eq!(original.code(), Some(&GatewayErrorCode::NOT_IMPLEMENTED));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
