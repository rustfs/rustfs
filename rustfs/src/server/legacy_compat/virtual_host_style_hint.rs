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
//! `VirtualHostStyleHintLayer`: an s3s compatibility patch, installed by
//! `server::http::external_service_stack!` when no server domain is
//! configured, and pinned there by `stack_census`.
//!
//! Patches: with no domain configured, s3s parses a virtual-hosted-style
//! `PUT /` as path-style with an empty bucket and answers an unreadable `501`.
//! Introduced in rustfs/rustfs#3442.
//! Replaced by: the gateway `HostResolver`'s diagnostic, which replaces the
//! message and nothing else (gateway `docs/middleware.md`, "The nine RustFS
//! tower patch layers", row 4).
//! Removed by: T2.12 (rustfs/backlog#2771).

use super::xml_escape;
use crate::server::hybrid::HybridBody;
use crate::server::layer::{LOG_COMPONENT_SERVER, LOG_SUBSYSTEM_HTTP};
use bytes::Bytes;
use http::{HeaderMap, Method, Request as HttpRequest, Response, StatusCode, Uri};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::debug;

/// Structured-log event emitted when a virtual-hosted-style request cannot be
/// routed because no server domain is configured.
const VIRTUAL_HOST_STYLE_UNROUTABLE_EVENT: &str = "virtual_host_style_unroutable";

/// Detects S3 requests that can only be virtual-hosted-style yet cannot be routed
/// because no server domain is configured (`RUSTFS_SERVER_DOMAINS` unset).
///
/// Without a configured domain, s3s parses every request as path-style, so a
/// virtual-hosted-style `PUT /` (the AWS SDK / Terraform default for `CreateBucket`)
/// arrives with an empty bucket and fails with an opaque `501 NotImplemented`.
///
/// Only the service root (`/`) with a method that has no path-style equivalent
/// (`PUT`/`DELETE`) and a DNS-style host (not an IP/socket address, and dotted like
/// `bucket.example.com`) is matched. The host is read from the `Host` header
/// (HTTP/1.1) or the request URI authority (HTTP/2 `:authority`). Such requests
/// already fail today, so returning a clearer error never changes the outcome of a
/// routable request. Returns the request host (without port) when matched.
fn unroutable_virtual_host_target(method: &Method, uri: &Uri, headers: &HeaderMap) -> Option<String> {
    if *method != Method::PUT && *method != Method::DELETE {
        return None;
    }
    if uri.path() != "/" {
        return None;
    }

    // `Host` is set for HTTP/1.1; HTTP/2 carries the host in the URI authority.
    let raw_authority = match headers.get(http::header::HOST).and_then(|value| value.to_str().ok()) {
        Some(value) => value,
        None => uri.authority().map(|authority| authority.as_str())?,
    };
    if raw_authority.is_empty() {
        return None;
    }
    // IP / socket-address hosts always use path-style addressing in s3s.
    if rustfs_utils::is_socket_addr(raw_authority) {
        return None;
    }
    // Parse as an Authority to split host/port robustly (e.g. bracketed IPv6) rather
    // than slicing on `:`.
    let authority = raw_authority.parse::<http::uri::Authority>().ok()?;
    let host = authority.host();
    // Require a dotted DNS name (the shape of a virtual-hosted bucket subdomain).
    if !host.contains('.') || host.starts_with('.') || host.ends_with('.') {
        return None;
    }

    Some(host.to_owned())
}

fn build_virtual_host_hint_response<RestBody, GrpcBody>(
    version: http::Version,
    host: &str,
    resource: &str,
) -> Response<HybridBody<RestBody, GrpcBody>>
where
    RestBody: From<Bytes>,
{
    let message = format!(
        "Virtual-hosted-style request for host '{host}' could not be routed because no server domain is \
         configured. Set RUSTFS_SERVER_DOMAINS to the S3 endpoint domain your buckets are addressed under \
         (for example RUSTFS_SERVER_DOMAINS=s3.example.com, so that bucket.s3.example.com routes to bucket \
         'bucket'), or configure your S3 client to use path-style addressing (AWS SDK: force_path_style=true; \
         Terraform aws provider: s3_use_path_style = true)."
    );
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>NotImplemented</Code><Message>{message}</Message><Resource>{resource}</Resource></Error>",
        message = xml_escape(&message),
        resource = xml_escape(resource),
    );

    let mut builder = Response::builder()
        .status(StatusCode::NOT_IMPLEMENTED)
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
        .expect("failed to build virtual-host hint response")
}

/// Returns an actionable error for virtual-hosted-style S3 requests that cannot be
/// routed because `RUSTFS_SERVER_DOMAINS` is not configured. See
/// [`unroutable_virtual_host_target`]. The layer is only installed when no server
/// domain is configured, so it is a pure pass-through for every routable request.
#[derive(Clone)]
pub struct VirtualHostStyleHintLayer;

impl<S> Layer<S> for VirtualHostStyleHintLayer {
    type Service = VirtualHostStyleHintService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        VirtualHostStyleHintService { inner }
    }
}

#[derive(Clone)]
pub struct VirtualHostStyleHintService<S> {
    inner: S,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for VirtualHostStyleHintService<S>
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
        if let Some(host) = unroutable_virtual_host_target(req.method(), req.uri(), req.headers()) {
            let resource = req.uri().path().to_owned();
            let version = req.version();
            debug!(
                event = VIRTUAL_HOST_STYLE_UNROUTABLE_EVENT,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_HTTP,
                host = %host,
                method = %req.method(),
                "Rejected virtual-hosted-style request: no server domain configured (set RUSTFS_SERVER_DOMAINS or use path-style)"
            );
            return Box::pin(async move { Ok(build_virtual_host_hint_response(version, &host, &resource)) });
        }

        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::layer::tests::CountingHybridService;
    use http::HeaderValue;
    use http::Request;
    use http_body_util::{BodyExt, Full};
    use std::sync::atomic::Ordering;

    #[test]
    fn unroutable_virtual_host_target_matches_service_root_writes() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, HeaderValue::from_static("my-bucket.s3.example.com"));
        assert_eq!(
            unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &headers),
            Some("my-bucket.s3.example.com".to_owned())
        );
        assert_eq!(
            unroutable_virtual_host_target(&Method::DELETE, &Uri::from_static("/"), &headers),
            Some("my-bucket.s3.example.com".to_owned())
        );
    }

    #[test]
    fn unroutable_virtual_host_target_strips_port() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, HeaderValue::from_static("bucket.localhost:9000"));
        assert_eq!(
            unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &headers),
            Some("bucket.localhost".to_owned())
        );
    }

    #[test]
    fn unroutable_virtual_host_target_uses_uri_authority_when_host_header_absent() {
        // HTTP/2 clients carry the host in the URI authority (`:authority`) rather than a Host header.
        let uri = Uri::from_static("https://my-bucket.s3.example.com/");
        assert_eq!(
            unroutable_virtual_host_target(&Method::PUT, &uri, &HeaderMap::new()),
            Some("my-bucket.s3.example.com".to_owned())
        );
    }

    #[test]
    fn unroutable_virtual_host_target_ignores_non_matching_requests() {
        let mut vhost = HeaderMap::new();
        vhost.insert(http::header::HOST, HeaderValue::from_static("bucket.s3.example.com"));
        // Path-style request carries the bucket in the path.
        assert_eq!(unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/bucket"), &vhost), None);
        // GET / is ListBuckets, a valid path-style service call.
        assert_eq!(unroutable_virtual_host_target(&Method::GET, &Uri::from_static("/"), &vhost), None);

        // IP / socket-address hosts always use path-style addressing.
        let mut ip = HeaderMap::new();
        ip.insert(http::header::HOST, HeaderValue::from_static("127.0.0.1:9000"));
        assert_eq!(unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &ip), None);

        // Bracketed IPv6 authority (with port) is a socket address, not a bucket subdomain.
        let mut ipv6 = HeaderMap::new();
        ipv6.insert(http::header::HOST, HeaderValue::from_static("[::1]:9000"));
        assert_eq!(unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &ipv6), None);

        // Bare single-label hosts are not virtual-hosted bucket subdomains.
        let mut bare = HeaderMap::new();
        bare.insert(http::header::HOST, HeaderValue::from_static("localhost:9000"));
        assert_eq!(unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &bare), None);

        // Trailing-dot FQDN is not matched (avoids suggesting a domain with a trailing dot).
        let mut fqdn = HeaderMap::new();
        fqdn.insert(http::header::HOST, HeaderValue::from_static("bucket.example.com."));
        assert_eq!(unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &fqdn), None);

        // Missing Host header and no URI authority.
        assert_eq!(
            unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &HeaderMap::new()),
            None
        );
    }

    #[test]
    fn build_virtual_host_hint_response_is_actionable() {
        let response: Response<HybridBody<Full<Bytes>, Full<Bytes>>> =
            build_virtual_host_hint_response(http::Version::HTTP_11, "my-bucket.s3.example.com", "/");
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(
            response
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/xml")
        );
        // HTTP/1.x error responses close the connection (the request body is not drained).
        assert_eq!(
            response
                .headers()
                .get(http::header::CONNECTION)
                .and_then(|value| value.to_str().ok()),
            Some("close")
        );

        // `Connection` is forbidden in HTTP/2, so it must not be set there.
        let h2_response: Response<HybridBody<Full<Bytes>, Full<Bytes>>> =
            build_virtual_host_hint_response(http::Version::HTTP_2, "my-bucket.s3.example.com", "/");
        assert!(h2_response.headers().get(http::header::CONNECTION).is_none());
    }

    #[tokio::test]
    async fn virtual_host_style_hint_layer_short_circuits_unroutable_put() {
        let inner = CountingHybridService::default();
        let calls = inner.calls();
        let mut service = VirtualHostStyleHintLayer.layer(inner);

        let response = service
            .call(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/")
                    .header(http::header::HOST, "my-bucket.s3.example.com")
                    .body(Full::<Bytes>::from(Bytes::new()))
                    .expect("request"),
            )
            .await
            .expect("hint response");

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            response
                .headers()
                .get(http::header::CONNECTION)
                .and_then(|value| value.to_str().ok()),
            Some("close")
        );

        let body = BodyExt::collect(response.into_body()).await.expect("body").to_bytes();
        let body = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(body.contains("RUSTFS_SERVER_DOMAINS=s3.example.com"));
        assert!(body.contains("s3_use_path_style"));
    }

    #[tokio::test]
    async fn virtual_host_style_hint_layer_passes_through_path_style() {
        let inner = CountingHybridService::default();
        let calls = inner.calls();
        let mut service = VirtualHostStyleHintLayer.layer(inner);

        let response = service
            .call(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/my-bucket")
                    .header(http::header::HOST, "s3.example.com")
                    .body(Full::<Bytes>::from(Bytes::new()))
                    .expect("request"),
            )
            .await
            .expect("inner response");

        // Path-style request reaches the inner service unchanged.
        assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
