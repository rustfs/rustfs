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

//! `RUSTFS_S3_STACK`: which HTTP boundary answers S3 requests (rustfs/backlog#1752).
//!
//! `legacy`, the default, serves every S3 request through the s3s service exactly as before the
//! switch existed: [`S3StackService::Legacy`] forwards each call to that service and does nothing
//! else. `gateway` puts the RustFS Gateway pipeline — wire acceptance, host resolution, routing,
//! SigV4 and the codecs — in front of the operations it is proven for, and hands every other
//! request to the same s3s service.
//!
//! # Fallback: route by operation before the body is read
//!
//! [`gateway_operation`] classifies a request from its method, path, query, `Host` and
//! authentication headers, before either stack has read a body byte. A request it does not
//! recognise exactly — another operation, a presigned or session-token request, a virtual-hosted
//! bucket, a Keystone token — goes to the legacy service untouched, so the fallback never replays
//! a body and every unrecognised shape keeps today's behaviour. A classified request is answered
//! by the gateway alone: a gateway refusal is never retried on s3s, which would let a request the
//! gateway rejected (a forged signature, say) be judged a second time by the other stack.
//!
//! The switch moves the wire boundary only. It selects no metadata writer: every persisted byte is
//! still produced by the unchanged s3s application layer, so going back to `legacy` is an
//! environment change and a restart.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::header::{AUTHORIZATION, HOST};
use http::{HeaderMap, Method, Request as HttpRequest, Uri};
use tower::Service;

use super::{is_admin_path, strip_valid_port_suffix};
use crate::config::S3Stack;
use crate::storage_api::server::http::gateway::{GatewayPipeline, HttpError, HttpResponse};

/// An operation the gateway stack serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GatewayOperation {
    GetBucketLocation,
}

impl GatewayOperation {
    /// The operation name both the s3s and the gateway router use.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::GetBucketLocation => "GetBucketLocation",
        }
    }
}

/// The operation the gateway stack answers `method uri` with, or `None` for the legacy stack.
///
/// Deliberately narrower than the gateway router: it recognises only the exact request shapes this
/// slice has proven, and everything else stays on s3s.
pub(crate) fn gateway_operation(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    vhost_domains: &[String],
) -> Option<GatewayOperation> {
    // `?location` and nothing else: a presigned query, `versionId` or a second subresource is
    // another request shape that has not been compared yet.
    if method != Method::GET || uri.query() != Some("location") {
        return None;
    }
    let path = uri.path();
    let bucket = path.strip_prefix('/')?;
    if bucket.is_empty() || bucket.contains('/') || is_admin_path(path) {
        return None;
    }
    if !is_path_style_host(uri, headers, vhost_domains) {
        return None;
    }
    // Session-token and Keystone requests authenticate through material the gateway credential
    // bridge does not carry yet; header SigV4 and anonymous are the two proven shapes.
    if headers.contains_key("x-amz-security-token") || headers.contains_key("x-auth-token") {
        return None;
    }
    match headers.get(AUTHORIZATION) {
        None => {}
        Some(value) if value.as_bytes().starts_with(b"AWS4-HMAC-SHA256 ") => {}
        Some(_) => return None,
    }
    Some(GatewayOperation::GetBucketLocation)
}

/// Whether the request addresses its bucket in the path.
///
/// The gateway stack resolves path-style only in this slice, so a host that is a label under a
/// configured `RUSTFS_SERVER_DOMAINS` domain — a virtual-hosted bucket — stays on s3s.
fn is_path_style_host(uri: &Uri, headers: &HeaderMap, vhost_domains: &[String]) -> bool {
    let host = match headers.get(HOST) {
        Some(value) => match value.to_str() {
            Ok(host) => host,
            Err(_) => return false,
        },
        None => match uri.authority() {
            Some(authority) => authority.as_str(),
            None => return false,
        },
    };
    if vhost_domains.is_empty() {
        return true;
    }
    let host = strip_valid_port_suffix(host).trim_end_matches('.').to_ascii_lowercase();
    !vhost_domains.iter().any(|domain| {
        let domain = strip_valid_port_suffix(domain).trim_end_matches('.').to_ascii_lowercase();
        host.strip_suffix(domain.as_str())
            .is_some_and(|label| label.len() > 1 && label.ends_with('.'))
    })
}

/// The future both stacks answer with: the s3s service's own future type.
pub(crate) type StackFuture = Pin<Box<dyn Future<Output = Result<HttpResponse, HttpError>> + Send>>;

/// The gateway half of a [`S3StackService::Gateway`] stack.
#[derive(Clone)]
pub(crate) struct GatewayFront {
    pipeline: GatewayPipeline,
    vhost_domains: Arc<[String]>,
}

impl GatewayFront {
    pub(crate) fn new(pipeline: GatewayPipeline, vhost_domains: Vec<String>) -> Self {
        Self {
            pipeline,
            vhost_domains: vhost_domains.into(),
        }
    }
}

/// The S3 entry service for one listener.
#[derive(Clone)]
pub(crate) enum S3StackService<L> {
    /// Every request goes to the legacy service.
    Legacy(L),
    /// Classified requests go to the gateway, every other request to the legacy service.
    Gateway { legacy: L, front: GatewayFront },
}

impl<L> S3StackService<L> {
    /// Builds the entry for `stack` around the already-built legacy service.
    ///
    /// `gateway` runs only when `stack` is [`S3Stack::Gateway`], so the legacy stack constructs
    /// nothing it did not construct before the switch.
    ///
    /// # Errors
    ///
    /// Whatever `gateway` returns: a gateway stack that cannot be assembled fails startup.
    pub(crate) fn assemble(stack: S3Stack, legacy: L, gateway: impl FnOnce() -> io::Result<GatewayFront>) -> io::Result<Self> {
        match stack {
            S3Stack::Legacy => Ok(Self::Legacy(legacy)),
            S3Stack::Gateway => Ok(Self::Gateway {
                legacy,
                front: gateway()?,
            }),
        }
    }
}

impl<L, B> Service<HttpRequest<B>> for S3StackService<L>
where
    L: Service<HttpRequest<B>, Response = HttpResponse, Error = HttpError, Future = StackFuture>,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    type Response = HttpResponse;
    type Error = HttpError;
    type Future = StackFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self {
            Self::Legacy(legacy) | Self::Gateway { legacy, .. } => legacy.poll_ready(cx),
        }
    }

    fn call(&mut self, request: HttpRequest<B>) -> Self::Future {
        match self {
            Self::Legacy(legacy) => legacy.call(request),
            Self::Gateway { legacy, front } => {
                match gateway_operation(request.method(), request.uri(), request.headers(), &front.vhost_domains) {
                    Some(operation) => {
                        let pipeline = front.pipeline.clone();
                        Box::pin(async move { Ok(pipeline.serve(operation.name(), request).await) })
                    }
                    None => legacy.call(request),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ENV_RUSTFS_S3_STACK;
    use crate::storage_api::server::http::gateway::Body;
    use std::sync::Mutex;

    fn classify(
        method: Method,
        target: &str,
        headers: &[(&'static str, &'static str)],
        domains: &[&str],
    ) -> Option<GatewayOperation> {
        let uri: Uri = target.parse().expect("test uri");
        let mut map = HeaderMap::new();
        map.insert(HOST, http::HeaderValue::from_static("127.0.0.1:9000"));
        for (name, value) in headers {
            map.insert(*name, http::HeaderValue::from_static(value));
        }
        let domains: Vec<String> = domains.iter().map(|domain| (*domain).to_string()).collect();
        gateway_operation(&method, &uri, &map, &domains)
    }

    #[test]
    fn unset_and_default_select_the_legacy_stack() {
        assert_eq!(S3Stack::default(), S3Stack::Legacy);
        temp_env::with_var_unset(ENV_RUSTFS_S3_STACK, || {
            assert_eq!(S3Stack::from_env().expect("unset is legacy"), S3Stack::Legacy);
        });
    }

    #[test]
    fn both_spellings_parse_and_round_trip() {
        for stack in [S3Stack::Legacy, S3Stack::Gateway] {
            assert_eq!(S3Stack::parse(stack.as_str()).expect("own spelling"), stack);
        }
        temp_env::with_var(ENV_RUSTFS_S3_STACK, Some("gateway"), || {
            assert_eq!(S3Stack::from_env().expect("gateway"), S3Stack::Gateway);
        });
    }

    #[test]
    fn an_invalid_or_empty_value_fails_instead_of_choosing_a_stack() {
        for value in ["", "s3s", "Gateway", "legacy ", "gateway,legacy"] {
            let error = S3Stack::parse(value).expect_err(value);
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{value:?}");
            assert!(error.to_string().contains(ENV_RUSTFS_S3_STACK), "{error}");
        }
        temp_env::with_var(ENV_RUSTFS_S3_STACK, Some(""), || {
            assert!(S3Stack::from_env().is_err(), "an empty value must fail startup");
        });
    }

    #[test]
    fn the_default_stack_builds_the_legacy_service_and_never_the_gateway() {
        let service = S3StackService::assemble(S3Stack::default(), "legacy-service", || {
            panic!("the legacy stack must not assemble the gateway")
        })
        .expect("legacy assembly cannot fail");
        assert!(matches!(service, S3StackService::Legacy("legacy-service")));
    }

    #[test]
    fn a_gateway_assembly_failure_fails_startup() {
        let result = S3StackService::assemble(S3Stack::Gateway, (), || Err(io::Error::other("assembly refused")));
        assert!(matches!(result, Err(error) if error.to_string() == "assembly refused"));
    }

    /// A legacy service that records what it was handed and answers a fixed response.
    #[derive(Clone, Default)]
    struct RecordingLegacy {
        seen: Arc<Mutex<Vec<(Method, Uri, HeaderMap)>>>,
    }

    impl Service<HttpRequest<http_body_util::Full<Bytes>>> for RecordingLegacy {
        type Response = HttpResponse;
        type Error = HttpError;
        type Future = StackFuture;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: HttpRequest<http_body_util::Full<Bytes>>) -> Self::Future {
            let (parts, _body) = request.into_parts();
            self.seen
                .lock()
                .expect("recording lock")
                .push((parts.method, parts.uri, parts.headers));
            Box::pin(async {
                let mut response = HttpResponse::new(Body::from(Bytes::from_static(b"legacy")));
                response.headers_mut().insert("x-legacy", http::HeaderValue::from_static("1"));
                Ok(response)
            })
        }
    }

    #[tokio::test]
    async fn the_legacy_stack_forwards_every_request_unchanged() {
        let legacy = RecordingLegacy::default();
        let mut service = S3StackService::assemble(S3Stack::Legacy, legacy.clone(), || unreachable!()).expect("legacy");

        // Even the request shape the gateway stack would take stays on the legacy service.
        let request = HttpRequest::get("/bucket?location")
            .header(HOST, "127.0.0.1:9000")
            .header("x-amz-date", "20260914T000000Z")
            .body(http_body_util::Full::new(Bytes::new()))
            .expect("request");
        let response = service.call(request).await.expect("legacy answers");

        assert_eq!(response.headers().get("x-legacy").map(|value| value.as_bytes()), Some(&b"1"[..]));
        let seen = legacy.seen.lock().expect("recording lock");
        assert_eq!(seen.len(), 1);
        let (method, uri, headers) = &seen[0];
        assert_eq!(method, Method::GET);
        assert_eq!(uri, "/bucket?location");
        assert_eq!(headers.get("x-amz-date").map(|value| value.as_bytes()), Some(&b"20260914T000000Z"[..]));
    }

    #[test]
    fn get_bucket_location_is_the_only_gateway_shape() {
        let signed = [("authorization", "AWS4-HMAC-SHA256 Credential=AK/20260914/us-east-1/s3/aws4_request")];
        assert_eq!(
            classify(Method::GET, "/bucket?location", &signed, &[]),
            Some(GatewayOperation::GetBucketLocation)
        );
        assert_eq!(
            classify(Method::GET, "/bucket?location", &[], &[]),
            Some(GatewayOperation::GetBucketLocation),
            "anonymous requests reach the gateway and RustFS access decides"
        );
    }

    #[test]
    fn every_other_shape_falls_back_to_legacy() {
        type Case = (Method, &'static str, &'static [(&'static str, &'static str)]);
        let cases: &[Case] = &[
            (Method::PUT, "/bucket/key", &[]),
            (Method::GET, "/bucket", &[]),
            (Method::HEAD, "/bucket?location", &[]),
            (Method::GET, "/bucket/?location", &[]),
            (Method::GET, "/bucket/key?location", &[]),
            (Method::GET, "/?location", &[]),
            (Method::GET, "/bucket?location=", &[]),
            (Method::GET, "/bucket?location&versionId=1", &[]),
            (Method::GET, "/bucket?location&X-Amz-Signature=00", &[]),
            (Method::GET, "/rustfs/admin?location", &[]),
            (Method::GET, "/bucket?location", &[("x-amz-security-token", "token")]),
            (Method::GET, "/bucket?location", &[("x-auth-token", "token")]),
            (Method::GET, "/bucket?location", &[("authorization", "AWS AK:signature")]),
        ];
        for (method, target, headers) in cases {
            assert_eq!(classify(method.clone(), target, headers, &[]), None, "{method} {target} {headers:?}");
        }
    }

    #[test]
    fn a_virtual_hosted_bucket_stays_on_legacy_but_the_base_domain_does_not() {
        let uri: Uri = "/bucket?location".parse().expect("uri");
        let domains = vec!["s3.example.com".to_string()];
        let mut headers = HeaderMap::new();
        headers.insert(HOST, http::HeaderValue::from_static("photos.S3.example.com:9000"));
        assert_eq!(gateway_operation(&Method::GET, &uri, &headers, &domains), None);
        headers.insert(HOST, http::HeaderValue::from_static("s3.example.com:9000"));
        assert_eq!(
            gateway_operation(&Method::GET, &uri, &headers, &domains),
            Some(GatewayOperation::GetBucketLocation)
        );
        headers.insert(HOST, http::HeaderValue::from_static("nots3.example.com"));
        assert_eq!(
            gateway_operation(&Method::GET, &uri, &headers, &domains),
            Some(GatewayOperation::GetBucketLocation),
            "a suffix without a label boundary is not a virtual host"
        );
        headers.remove(HOST);
        assert_eq!(gateway_operation(&Method::GET, &uri, &headers, &domains), None, "no host stays legacy");
    }
}
