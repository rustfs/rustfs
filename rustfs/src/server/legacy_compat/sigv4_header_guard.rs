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
//! `SigV4HeaderGuardLayer`: an s3s compatibility patch, installed by
//! `server::http::external_service_stack!` and pinned there by `stack_census`.
//!
//! Patches: s3s verifies the algorithm token before RustFS's access layer runs,
//! so the GHSA-xm99-m3gq-83g8 / GHSA-g8w9-qw9q-fghr rulings on unsigned
//! `x-amz-*` headers have to be applied in front of s3s. Introduced in
//! rustfs/rustfs#7796.
//! Replaced by: the gateway's own SigV4 verifier, whose signed-header
//! enforcement (`SignedHeaderSet::parse_and_enforce` in
//! `crates/sig/src/signed_headers.rs`) refuses any `x-amz-*` header absent
//! from `SignedHeaders` for header and query authentication alike (gateway
//! `docs/security-model.md`). This layer is not one of the nine rows of
//! `docs/middleware.md`.
//! Removed by: T2.12 (rustfs/backlog#2771).

use super::xml_escape;
use crate::server::hybrid::HybridBody;
use crate::storage_api::server::legacy_compat::S3Error;
use bytes::Bytes;
use http::{Request as HttpRequest, Response, StatusCode};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// GHSA-xm99-m3gq-83g8 / GHSA-g8w9-qw9q-fghr: enforce the SigV4 unsigned
/// `x-amz-*` header rules ahead of s3s dispatch.
///
/// s3s verifies the claimed algorithm as the first step of its own signature
/// flow and answers a swapped algorithm token with `501 NotImplemented` before
/// RustFS's access layer (`S3Access::check`) ever runs, so the `AccessDenied`
/// rulings of [`crate::auth::reject_unsigned_amz_headers_on_sigv4_request`]
/// must be applied here, in front of s3s. Rejections carry the same S3 error
/// document the access layer would have produced.
#[derive(Clone, Default)]
pub struct SigV4HeaderGuardLayer;

impl<S> Layer<S> for SigV4HeaderGuardLayer {
    type Service = SigV4HeaderGuardService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SigV4HeaderGuardService { inner }
    }
}

#[derive(Clone)]
pub struct SigV4HeaderGuardService<S> {
    inner: S,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for SigV4HeaderGuardService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    RestBody: From<Bytes> + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        match crate::auth::reject_unsigned_amz_headers_on_sigv4_request(req.headers(), req.uri().query()) {
            Ok(()) => {}
            Err(error) => {
                let version = req.version();
                return Box::pin(async move { Ok(sigv4_header_guard_rejection(version, error)) });
            }
        }
        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
    }
}

/// Serialize a header-guard rejection as the S3 error document the access
/// layer would have produced for the same rule violation.
fn sigv4_header_guard_rejection<RestBody, GrpcBody>(
    version: http::Version,
    error: S3Error,
) -> Response<HybridBody<RestBody, GrpcBody>>
where
    RestBody: From<Bytes>,
{
    let status = error.status_code().unwrap_or(StatusCode::FORBIDDEN);
    let message = error.message().unwrap_or_default().to_owned();
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>{code}</Code><Message>{message}</Message></Error>",
        code = xml_escape(error.code().as_str()),
        message = xml_escape(&message),
    );

    let mut builder = Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/xml");
    // This short-circuit path does not drain the request body. For HTTP/1.x, signal
    // connection close so an undrained body cannot disrupt keep-alive reuse. `Connection`
    // is a forbidden header in HTTP/2+, so it is only set for HTTP/1.x.
    if !matches!(version, http::Version::HTTP_2 | http::Version::HTTP_3) {
        builder = builder.header(http::header::CONNECTION, "close");
    }
    builder
        .body(HybridBody::Rest {
            rest_body: RestBody::from(Bytes::from(body)),
        })
        .expect("failed to build SigV4 header guard rejection response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::layer::tests::CountingHybridService;
    use http::Method;
    use http::Request;
    use http_body_util::{BodyExt, Full};
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn sigv4_header_guard_layer_rejects_swapped_algorithm_token_with_access_denied() {
        let inner = CountingHybridService::default();
        let calls = inner.calls();
        let mut service = SigV4HeaderGuardLayer.layer(inner);

        let response = service
            .call(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/xm99-private-source/target")
                    .header(
                        "authorization",
                        "OTHER Credential=rustfsadmin/20260914/us-east-1/s3/aws4_request, \
                         SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
                         Signature=00e997a1db4d3b6ee6c26d3f7d3f3fb3b6ee6c26d3f7d3f3fb3b6ee6c26d3f7d",
                    )
                    .header("x-amz-date", "20260914T000000Z")
                    .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                    .header("x-amz-copy-source", "/negative-sigv4-bucket/source")
                    .body(Full::<Bytes>::from(Bytes::new()))
                    .expect("request"),
            )
            .await
            .expect("guard response");

        // The swapped token must be answered with the access-layer ruling
        // instead of s3s's algorithm 501.
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let body = BodyExt::collect(response.into_body()).await.expect("body").to_bytes();
        let body = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(body.contains("<Code>AccessDenied</Code>"), "body: {body}");
        assert!(body.contains("Unsupported SigV4 authorization algorithm"), "body: {body}");
    }

    #[tokio::test]
    async fn sigv4_header_guard_layer_rejects_unsigned_copy_source_header() {
        let inner = CountingHybridService::default();
        let calls = inner.calls();
        let mut service = SigV4HeaderGuardLayer.layer(inner);

        let response = service
            .call(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/xm99-private-source/target")
                    .header(
                        "authorization",
                        "AWS4-HMAC-SHA256 Credential=rustfsadmin/20260914/us-east-1/s3/aws4_request, \
                         SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
                         Signature=00e997a1db4d3b6ee6c26d3f7d3f3fb3b6ee6c26d3f7d3f3fb3b6ee6c26d3f7d",
                    )
                    .header("x-amz-date", "20260914T000000Z")
                    .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                    .header("x-amz-copy-source", "/negative-sigv4-bucket/source")
                    .body(Full::<Bytes>::from(Bytes::new()))
                    .expect("request"),
            )
            .await
            .expect("guard response");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let body = BodyExt::collect(response.into_body()).await.expect("body").to_bytes();
        let body = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(body.contains("<Code>AccessDenied</Code>"), "body: {body}");
        assert!(
            body.contains("There were headers present in the request which were not signed"),
            "body: {body}"
        );
    }

    #[tokio::test]
    async fn sigv4_header_guard_layer_passes_unsigned_and_signed_envelope_requests_through() {
        let inner = CountingHybridService::default();
        let calls = inner.calls();
        let mut service = SigV4HeaderGuardLayer.layer(inner);

        // Anonymous request: no Authorization header, no presigned query.
        let response = service
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/bucket/key")
                    .body(Full::<Bytes>::from(Bytes::new()))
                    .expect("request"),
            )
            .await
            .expect("inner response");
        assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Header-signed request whose only x-amz-* headers are the signed envelope.
        let response = service
            .call(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/bucket/key")
                    .header(
                        "authorization",
                        "AWS4-HMAC-SHA256 Credential=rustfsadmin/20260914/us-east-1/s3/aws4_request, \
                         SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
                         Signature=00e997a1db4d3b6ee6c26d3f7d3f3fb3b6ee6c26d3f7d3f3fb3b6ee6c26d3f7d",
                    )
                    .header("x-amz-date", "20260914T000000Z")
                    .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                    .body(Full::<Bytes>::from(Bytes::new()))
                    .expect("request"),
            )
            .await
            .expect("inner response");
        assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
