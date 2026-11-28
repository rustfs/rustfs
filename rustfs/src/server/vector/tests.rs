// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use http::{Method, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use std::num::NonZeroU64;
use tower::{Service, ServiceExt, service_fn};

fn gateway() -> VectorGateway {
    let slot = ServerContextSlot::new();
    assert!(slot.installed_app_context().is_none());
    assemble(
        VectorAssembly {
            server_ctx: slot,
            root_access_key: "vector-test-root".to_owned(),
            root_secret_key: "vector-test-root-secret".to_owned(),
            region: Some("us-east-1".to_owned()),
            body_read_timeout: Duration::from_secs(10),
        },
        ManifestIoLimits {
            max_manifest_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            max_concurrent_reads: NonZeroUsize::new(4).unwrap(),
        },
        NonZeroUsize::new(3).unwrap(),
    )
    .expect("HTTP assembly precedes IAM installation")
}

fn signed_hint(service: &str) -> String {
    format!("AWS4-HMAC-SHA256 Credential=test/20261121/us-east-1/{service}/aws4_request, SignedHeaders=host, Signature=00")
}

#[test]
fn tags_paths_require_vector_signing_scope() {
    for method in [Method::GET, Method::POST, Method::DELETE] {
        for path in [
            "/tags/object",
            "/tags?tagging",
            "/tags/arn%3Aaws%3As3vectors%3Aus-east-1%3A%3Abucket%2Fexample",
        ] {
            for service in [None, Some("s3"), Some("s3vectors")] {
                let mut request = http::Request::builder().method(method.clone()).uri(path);
                if let Some(service) = service {
                    request = request.header(http::header::AUTHORIZATION, signed_hint(service));
                }
                assert_eq!(claims_vector_service(&request.body(()).unwrap()), service == Some("s3vectors"));
            }
        }
    }
}

#[test]
fn dispatch_uses_signing_scope_without_claiming_action_named_s3_objects() {
    for path in ["/CreateVectorBucket", "/QueryVectors", "/ListVectors"] {
        let request = http::Request::builder().method(Method::POST).uri(path).body(()).unwrap();
        assert!(!claims_vector_service(&request));
        let request = http::Request::builder().method(Method::GET).uri(path).body(()).unwrap();
        assert!(!claims_vector_service(&request));
    }
    for path in ["/ListVectors?uploadId=upload", "/CreateIndex?uploadId=upload"] {
        for service in ["s3", "s3vectors"] {
            let request = http::Request::builder()
                .method(Method::POST)
                .uri(path)
                .header(http::header::AUTHORIZATION, signed_hint(service))
                .body(())
                .unwrap();
            assert_eq!(claims_vector_service(&request), service == "s3vectors");
            let uri = format!("{path}&X-Amz-Credential=test%2F20261121%2Fus-east-1%2F{service}%2Faws4_request");
            let request = http::Request::builder().method(Method::POST).uri(uri).body(()).unwrap();
            assert_eq!(claims_vector_service(&request), service == "s3vectors");
        }
    }
    for (scope, expected) in [("s3", false), ("s3vectors", true), ("s3vectors-extra", false)] {
        let uri = format!("/tags/object?X-Amz-Credential=test%2F20261121%2Fus-east-1%2F{scope}%2Faws4_request");
        let request = http::Request::builder().uri(uri).body(()).unwrap();
        assert_eq!(claims_vector_service(&request), expected);
    }
}

#[tokio::test]
async fn production_activation_gate_keeps_vector_operations_unavailable() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = calls.clone();
    let s3 = service_fn(move |_: http::Request<Empty<bytes::Bytes>>| {
        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async { Ok::<_, crate::server::EdgeError>(http::Response::new(crate::server::EdgeBody::default())) }
    });
    let request = http::Request::builder()
        .method(Method::POST)
        .uri("/ListVectorBuckets")
        .header(http::header::AUTHORIZATION, signed_hint("s3vectors"))
        .body(Empty::new())
        .unwrap();
    let response = VectorDispatch::new(s3, Ok(gateway())).oneshot(request).await.unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[http::header::CONTENT_TYPE], "application/json");
    assert_eq!(response.headers()["x-amzn-errortype"], "ServiceUnavailableException");
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["message"], "Vector service is unavailable");
}

#[tokio::test]
async fn ordinary_s3_request_preserves_method_uri_body_and_extensions() {
    #[derive(Clone, Debug, PartialEq)]
    struct Marker(u64);
    let s3 = service_fn(|request: http::Request<Full<bytes::Bytes>>| async move {
        assert_eq!(request.method(), Method::POST);
        assert_eq!(request.uri(), "/tags/object?uploads&partNumber=1");
        assert_eq!(request.headers()[http::header::AUTHORIZATION], signed_hint("s3"));
        assert_eq!(request.extensions().get::<Marker>(), Some(&Marker(42)));
        assert_eq!(request.into_body().collect().await.unwrap().to_bytes(), "ordinary S3 body");
        let mut response = http::Response::new(crate::server::EdgeBody::from(bytes::Bytes::from_static(b"S3 answer")));
        *response.status_mut() = StatusCode::ACCEPTED;
        Ok::<_, crate::server::EdgeError>(response)
    });
    let mut request = http::Request::builder()
        .method(Method::POST)
        .uri("/tags/object?uploads&partNumber=1")
        .header(http::header::AUTHORIZATION, signed_hint("s3"))
        .body(Full::new(bytes::Bytes::from_static(b"ordinary S3 body")))
        .unwrap();
    request.extensions_mut().insert(Marker(42));
    let mut dispatch = VectorDispatch::enabled_for_test(s3, gateway());
    let response = dispatch.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(response.into_body().collect().await.unwrap().to_bytes(), "S3 answer");
}
