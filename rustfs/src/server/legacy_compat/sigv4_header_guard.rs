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

    use crate::auth::{hoist_signed_query_params, reject_unsigned_amz_headers_on_presigned_request};
    use crate::storage_api::server::legacy_compat::S3ErrorCode;
    use http::header::HeaderName;
    use http::{HeaderMap, HeaderValue};

    /// A SigV4 presigned query: the presigner moved the request's `x-amz-*`
    /// headers into it and every parameter is covered by the signature.
    fn presigned_query(extra: &str) -> String {
        let mut query =
            String::from("X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20260101%2Fus-east-1%2Fs3%2Faws4_request");
        query.push_str("&X-Amz-Date=20260101T000000Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host&X-Amz-Signature=deadbeef");
        query.push('&');
        query.push_str(extra);
        query
    }

    fn hoist(uri_query: &str) -> Result<http::Request<()>, S3Error> {
        let mut req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/bucket/key?{uri_query}"))
            .body(())
            .expect("request");
        let hoisted_query = req.uri().query().map(str::to_owned);
        hoist_signed_query_params(req.headers_mut(), hoisted_query.as_deref())?;
        Ok(req)
    }

    #[test]
    fn hoist_restores_presigned_query_metadata_as_headers() {
        let req = hoist(&presigned_query("x-amz-meta-user-id=u1&x-amz-tagging=project%3Drustfs")).expect("hoist");
        assert_eq!(req.headers().get("x-amz-meta-user-id").and_then(|v| v.to_str().ok()), Some("u1"));
        assert!(req.headers().get("x-amz-tagging").is_none());
    }

    #[test]
    fn hoist_leaves_typed_request_properties_unchanged() {
        for name in [
            "x-amz-object-lock-legal-hold",
            "x-amz-object-lock-mode",
            "x-amz-object-lock-retain-until-date",
            "x-amz-server-side-encryption",
            "x-amz-storage-class",
            "x-amz-copy-source",
            "x-amz-content-sha256",
            "x-amz-tagging",
        ] {
            let req = hoist(&presigned_query(&format!("{name}=value"))).expect("metadata hoist");
            assert!(!req.headers().contains_key(name), "typed property must keep its original path: {name}");
        }
    }

    #[test]
    fn hoist_normalizes_parameter_names_like_header_names() {
        let req = hoist(&presigned_query("X-Amz-Meta-User-Id=u1")).expect("hoist");
        assert_eq!(req.headers().get("x-amz-meta-user-id").and_then(|v| v.to_str().ok()), Some("u1"));
        // `HeaderName` is lowercase, so the stored key is the normalized one.
        assert_eq!(req.headers().keys().map(|name| name.as_str()).collect::<Vec<_>>(), ["x-amz-meta-user-id"]);
    }

    #[test]
    fn hoist_never_overwrites_a_header_the_client_sent() {
        let mut req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/bucket/key?{}", presigned_query("x-amz-meta-user-id=from-query")))
            .header("x-amz-meta-user-id", "from-header")
            .body(())
            .expect("request");
        let hoisted_query = req.uri().query().map(str::to_owned);
        hoist_signed_query_params(req.headers_mut(), hoisted_query.as_deref()).expect("hoist");
        assert_eq!(req.headers().get("x-amz-meta-user-id").and_then(|v| v.to_str().ok()), Some("from-header"));
    }

    #[test]
    fn hoist_skips_sigv4_auth_params_and_non_amz_parameters() {
        let req = hoist(&presigned_query("versionId=3&x-amz-security-token=token")).expect("hoist");
        assert!(req.headers().get("x-amz-algorithm").is_none());
        assert!(req.headers().get("x-amz-credential").is_none());
        assert!(req.headers().get("x-amz-signature").is_none());
        assert!(req.headers().get("x-amz-security-token").is_none());
        assert!(req.headers().get("versionId").is_none());
    }

    #[test]
    fn hoist_ignores_a_sigv2_upload_that_only_claims_sigv4_markers() {
        // The SigV2 string-to-sign does not cover arbitrary query parameters,
        // so appending the SigV4 presign markers leaves the original signature
        // valid while making the query look signed. A URL holder must not be
        // able to turn on archive extraction that way.
        let mut query = String::from("AWSAccessKeyId=AKID&Expires=1893456000&Signature=deadbeef");
        query.push_str("&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID&X-Amz-Signature=deadbeef");
        query.push_str("&x-amz-meta-snowball-auto-extract=true&x-amz-meta-user-id=u1");
        let req = hoist(&query).expect("hoist");
        assert!(req.headers().get("x-amz-meta-snowball-auto-extract").is_none());
        assert!(req.headers().get("x-amz-meta-user-id").is_none());
    }

    #[test]
    fn hoist_requires_the_complete_presign_marker_set() {
        // A lone marker is not a presigned request; `X-Amz-Signature` alone is
        // what the unsigned-header guard keys on, not proof of presigning.
        let req = hoist("X-Amz-Signature=deadbeef&x-amz-meta-user-id=u1").expect("hoist");
        assert!(req.headers().get("x-amz-meta-user-id").is_none());
    }

    #[test]
    fn hoist_preserves_an_explicit_false_extraction_header() {
        // Header precedence: a client that says `false` in a header must not be
        // overruled by the hoisted form of the same key.
        let mut req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/bucket/key?{}", presigned_query("x-amz-meta-snowball-auto-extract=true")))
            .header("x-amz-meta-snowball-auto-extract", "false")
            .body(())
            .expect("request");
        let hoisted_query = req.uri().query().map(str::to_owned);
        hoist_signed_query_params(req.headers_mut(), hoisted_query.as_deref()).expect("hoist");
        assert_eq!(
            req.headers()
                .get("x-amz-meta-snowball-auto-extract")
                .and_then(|v| v.to_str().ok()),
            Some("false")
        );
    }

    #[test]
    fn hoist_ignores_a_query_the_signature_does_not_cover() {
        // SigV2 signs headers plus a fixed subresource list, not arbitrary
        // query parameters; honoring them would widen a presigned upload.
        let req = hoist("AWSAccessKeyId=AKID&Signature=deadbeef&x-amz-meta-user-id=u1").expect("hoist");
        assert!(req.headers().get("x-amz-meta-user-id").is_none());

        // Anonymous: no signature at all.
        let req = hoist("x-amz-meta-user-id=u1").expect("hoist");
        assert!(req.headers().get("x-amz-meta-user-id").is_none());
    }

    #[test]
    fn hoist_restores_params_on_a_header_signed_sigv4_request() {
        let mut req = Request::builder()
            .method(Method::PUT)
            .uri("/bucket/key?x-amz-meta-user-id=u1")
            .header(
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKID/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=deadbeef",
            )
            .body(())
            .expect("request");
        let hoisted_query = req.uri().query().map(str::to_owned);
        hoist_signed_query_params(req.headers_mut(), hoisted_query.as_deref()).expect("hoist");
        assert_eq!(req.headers().get("x-amz-meta-user-id").and_then(|v| v.to_str().ok()), Some("u1"));
    }

    #[test]
    fn hoist_rejects_names_and_values_that_could_not_be_headers() {
        // A stored control character would make the read path fail every
        // HeadObject of that object.
        let err = hoist(&presigned_query("x-amz-meta-user-id=a%0d%0ab")).expect_err("control char");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);

        let err = hoist(&presigned_query("x-amz-meta-bad%20key=v")).expect_err("bad name");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn hoist_rejects_duplicate_parameters_after_name_normalization() {
        let err = hoist(&presigned_query("x-amz-meta-User-Id=1&x-amz-meta-user-id=2")).expect_err("duplicate");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn hoisted_metadata_matches_the_header_path_exactly() {
        let req = hoist(&presigned_query("x-amz-meta-user-id=u1&x-amz-meta-content-type=custom")).expect("hoist");
        let mut from_query = HeaderMap::new();
        for name in ["x-amz-meta-user-id", "x-amz-meta-content-type"] {
            from_query.insert(HeaderName::from_static(name), req.headers().get(name).expect("hoisted header").clone());
        }

        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-user-id", HeaderValue::from_static("u1"));
        headers.insert("x-amz-meta-content-type", HeaderValue::from_static("custom"));
        assert_eq!(from_query, headers);
    }

    #[tokio::test]
    async fn hoist_then_guard_accepts_a_valid_metadata_bearing_presigned_put() {
        // The AWS SDK presigner emits `SignedHeaders=host` and moves
        // `x-amz-content-sha256` and `x-amz-meta-*` into the query. Only the
        // metadata is restored as headers; the digest keeps its dedicated
        // query verification path. The guard accepts the signed metadata.
        let inner = CountingHybridService::default();
        let calls = inner.calls();
        let mut service = SigV4HeaderGuardLayer.layer(inner);

        let mut query =
            String::from("X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20261103%2Fus-east-1%2Fs3%2Faws4_request");
        query.push_str("&X-Amz-Date=20261103T000000Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host&X-Amz-Signature=deadbeef");
        query.push_str("&x-amz-content-sha256=UNSIGNED-PAYLOAD&x-amz-meta-user-id=u1");

        let mut request = Request::builder()
            .method(Method::PUT)
            .uri(format!("/repro/bucket/meta-query.csv?{query}"))
            .header("content-type", "text/csv")
            .body(Full::<Bytes>::from(Bytes::new()))
            .expect("request");
        // The authorization boundary hoists the signed query properties before
        // the handler and before RustFS's own unsigned-header guard re-runs.
        let hoisted_query = request.uri().query().map(str::to_owned);
        hoist_signed_query_params(request.headers_mut(), hoisted_query.as_deref()).expect("hoist");

        let response = service.call(request).await.expect("inner response");

        let status = response.status();
        let body = BodyExt::collect(response.into_body()).await.expect("body").to_bytes();
        let body = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "status={status} body={body}");
        assert_eq!(status, StatusCode::IM_A_TEAPOT, "body={body}");
    }

    #[test]
    fn hoist_rejects_a_control_character_value_with_invalid_argument() {
        let err = hoist(&presigned_query("x-amz-meta-user-id=a%0d%0ab")).expect_err("control char");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn sigv4_header_guard_accepts_a_header_that_matches_a_signed_query_value() {
        // The hoist layer restores `?x-amz-meta-user-id=u1` as a header before
        // the access boundary re-runs this rule; the value it carries is the one
        // the signature covers.
        let uri = format!("/bucket/key?{}", presigned_query("x-amz-meta-user-id=u1"));
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-user-id", HeaderValue::from_static("u1"));
        assert!(reject_unsigned_amz_headers_on_presigned_request(&headers, Some(uri.split('?').nth(1).unwrap())).is_ok());
    }

    #[tokio::test]
    async fn sigv4_header_guard_still_rejects_an_unsigned_header() {
        let uri = format!("/bucket/key?{}", presigned_query("x-amz-meta-user-id=u1"));
        let query = uri.split('?').nth(1).unwrap();

        // Same name, different value: the signer did not authorize it.
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-user-id", HeaderValue::from_static("evil"));
        assert!(reject_unsigned_amz_headers_on_presigned_request(&headers, Some(query)).is_err());

        // No such parameter at all.
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-other", HeaderValue::from_static("v"));
        assert!(reject_unsigned_amz_headers_on_presigned_request(&headers, Some(query)).is_err());

        // Multi-valued: the hoist layer never produces one, and the extra value
        // is unsigned.
        let mut headers = HeaderMap::new();
        headers.append("x-amz-meta-user-id", HeaderValue::from_static("u1"));
        headers.append("x-amz-meta-user-id", HeaderValue::from_static("evil"));
        assert!(reject_unsigned_amz_headers_on_presigned_request(&headers, Some(query)).is_err());
    }
}
