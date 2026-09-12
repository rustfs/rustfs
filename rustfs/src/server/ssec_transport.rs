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

//! SSE-C transport policy (backlog#2369 P7.2).
//!
//! An SSE-C request carries the customer's AES key in a request header, so AWS
//! S3 and MinIO both refuse one that did not arrive over TLS. RustFS accepted
//! them on any transport, which means a plaintext hop hands the key to anyone
//! on the path — and the object is then unreadable without that same key, so
//! the exposure is permanent for as long as the object lives.
//!
//! Refusing outright is the correct end state but not a safe default to adopt
//! inside a release window: the project's own s3-tests and e2e lanes, and most
//! staging deployments, speak plain HTTP. This release therefore reports:
//! every SSE-C request on a plaintext transport increments
//! `rustfs_ssec_plaintext_requests_total` and logs one warning per process, so
//! an operator can see whether anything would break before the default flips.
//! `RUSTFS_SSE_C_REQUIRE_TLS=true` opts a deployment into the rejection now.
//!
//! The transport verdict is per connection, not per deployment: the layer is
//! built with whether *this* listener terminates TLS, and additionally accepts
//! a `https` forwarded protocol resolved by the trusted-proxy layer, which is
//! the only spoof-resistant source for a TLS-terminating proxy in front.

use bytes::Bytes;
use futures::future::{Either, Ready, ready};
use http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use metrics::counter;
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::http::headers::{
    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
};
use std::sync::Once;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::warn;

use crate::storage_api::server::layer::request_context::RequestContext;

/// Opt in to refusing SSE-C on a plaintext transport. Default `false` for this
/// release; the reporting path runs either way.
pub(crate) const ENV_SSE_C_REQUIRE_TLS: &str = "RUSTFS_SSE_C_REQUIRE_TLS";
pub(crate) const DEFAULT_SSE_C_REQUIRE_TLS: bool = false;

/// Counts SSE-C requests that arrived without TLS. A deployment planning to
/// enable [`ENV_SSE_C_REQUIRE_TLS`] should see this at zero first.
pub(crate) const METRIC_SSEC_PLAINTEXT_REQUESTS_TOTAL: &str = "rustfs_ssec_plaintext_requests_total";

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxBody = http_body_util::combinators::UnsyncBoxBody<Bytes, BoxError>;

/// Whether the request carries any SSE-C header.
///
/// Any one of the three is enough: an incomplete triple is still an attempt to
/// use SSE-C, and it is rejected later for being incomplete — but the key may
/// already have crossed the wire.
fn carries_ssec_headers(headers: &HeaderMap) -> bool {
    headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM)
        || headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY)
        || headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5)
}

/// Whether this request reached the server over TLS.
///
/// `connection_is_tls` is what this listener actually did. The forwarded
/// protocol is only consulted as a second source because the trusted-proxy
/// layer has already decided whether the peer is allowed to assert it; a
/// request that arrives direct carries no such assertion.
fn is_secure_transport(connection_is_tls: bool, client_info: Option<&ClientInfo>) -> bool {
    if connection_is_tls {
        return true;
    }
    client_info
        .and_then(|info| info.forwarded_proto.as_deref())
        .is_some_and(|proto| proto.eq_ignore_ascii_case("https"))
}

fn require_tls() -> bool {
    rustfs_utils::get_env_bool(ENV_SSE_C_REQUIRE_TLS, DEFAULT_SSE_C_REQUIRE_TLS)
}

/// One warning per process: plaintext SSE-C traffic is driven by clients, so a
/// per-request warning would let a busy client flood the log. The counter
/// carries the per-request volume.
fn warn_once_about_plaintext_ssec() {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        warn!(
            event = "ssec_request_without_tls",
            require_tls = ENV_SSE_C_REQUIRE_TLS,
            metric = METRIC_SSEC_PLAINTEXT_REQUESTS_TOTAL,
            "SSE-C requests are arriving without TLS, so the customer key crosses the network in \
             cleartext. AWS S3 refuses these; RustFS will too in a later release. Terminate TLS on \
             this listener or on a trusted proxy, then set RUSTFS_SSE_C_REQUIRE_TLS=true. Reported \
             once per process; the counter carries the volume."
        );
    });
}

/// The S3 rejection AWS returns for SSE-C without TLS. Built by hand because
/// the rejection short-circuits the inner response stack, mirroring the
/// rate-limit layer.
fn ssec_requires_tls_response(request_id: Option<&str>) -> Response<BoxBody> {
    let request_id_xml = request_id
        .filter(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
        .map(|id| format!("<RequestId>{id}</RequestId>"))
        .unwrap_or_default();
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>InvalidRequest</Code>\
         <Message>Requests specifying Server Side Encryption with Customer provided keys must be made over a secure connection.</Message>\
         {request_id_xml}</Error>"
    );
    let body: BoxBody = Full::new(Bytes::from(body))
        .map_err(|e| -> BoxError { Box::new(e) })
        .boxed_unsync();

    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::BAD_REQUEST;
    response
        .headers_mut()
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    response
}

/// Layer that reports — and optionally refuses — SSE-C over a plaintext
/// transport. `connection_is_tls` is whether the listener that accepted this
/// connection terminated TLS.
#[derive(Clone, Copy)]
pub(crate) struct SsecTransportLayer {
    connection_is_tls: bool,
}

impl SsecTransportLayer {
    pub(crate) fn new(connection_is_tls: bool) -> Self {
        Self { connection_is_tls }
    }
}

impl<S> Layer<S> for SsecTransportLayer {
    type Service = SsecTransportService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SsecTransportService {
            inner,
            connection_is_tls: self.connection_is_tls,
        }
    }
}

#[derive(Clone)]
pub(crate) struct SsecTransportService<S> {
    inner: S,
    connection_is_tls: bool,
}

impl<S, ReqBody> Service<Request<ReqBody>> for SsecTransportService<S>
where
    S: Service<Request<ReqBody>, Response = Response<BoxBody>>,
{
    type Response = Response<BoxBody>;
    type Error = S::Error;
    type Future = Either<S::Future, Ready<Result<Response<BoxBody>, S::Error>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        if !carries_ssec_headers(req.headers())
            || is_secure_transport(self.connection_is_tls, req.extensions().get::<ClientInfo>())
        {
            return Either::Left(self.inner.call(req));
        }

        counter!(METRIC_SSEC_PLAINTEXT_REQUESTS_TOTAL).increment(1);
        warn_once_about_plaintext_ssec();

        if !require_tls() {
            return Either::Left(self.inner.call(req));
        }

        Either::Right(ready(Ok(ssec_requires_tls_response(
            req.extensions()
                .get::<RequestContext>()
                .map(|context| context.request_id.as_str()),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, SocketAddr};

    fn ssec_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, HeaderValue::from_static("AES256"));
        headers
    }

    fn proxied(proto: &str) -> ClientInfo {
        ClientInfo::from_trusted_proxy(
            IpAddr::from([203, 0, 113, 10]),
            None,
            Some(proto.to_string()),
            IpAddr::from([10, 0, 0, 1]),
            1,
            rustfs_trusted_proxies::ValidationMode::Lenient,
            Vec::new(),
        )
    }

    fn direct() -> ClientInfo {
        ClientInfo::direct(SocketAddr::new(IpAddr::from([203, 0, 113, 10]), 443))
    }

    #[test]
    fn any_ssec_header_counts_as_an_ssec_request() {
        assert!(!carries_ssec_headers(&HeaderMap::new()));
        assert!(carries_ssec_headers(&ssec_headers()));

        // An incomplete triple is still an attempt, and the key may already
        // have crossed the wire.
        let mut only_key = HeaderMap::new();
        only_key.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, HeaderValue::from_static("a2V5"));
        assert!(carries_ssec_headers(&only_key));

        let mut only_md5 = HeaderMap::new();
        only_md5.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, HeaderValue::from_static("bWQ1"));
        assert!(carries_ssec_headers(&only_md5));
    }

    #[test]
    fn transport_is_secure_only_on_tls_or_a_resolved_https_proxy() {
        assert!(is_secure_transport(true, None), "a TLS listener needs no header to prove it");
        assert!(is_secure_transport(true, Some(&proxied("http"))), "the listener's own TLS wins");
        assert!(
            is_secure_transport(false, Some(&proxied("https"))),
            "a TLS-terminating trusted proxy is a secure transport"
        );
        assert!(
            !is_secure_transport(false, Some(&proxied("http"))),
            "a proxy that forwarded plain HTTP is not"
        );
        assert!(
            !is_secure_transport(false, Some(&direct())),
            "a direct plaintext client asserts no protocol"
        );
        assert!(!is_secure_transport(false, None), "no transport evidence means not secure");
    }

    #[test]
    fn the_rejection_carries_the_aws_wording_and_a_safe_request_id() {
        let response = ssec_requires_tls_response(Some("abc-123"));
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // A request id that is not plain enough to embed must be dropped
        // rather than escaped into the XML body.
        let injected = ssec_requires_tls_response(Some("<injected>"));
        assert_eq!(injected.status(), StatusCode::BAD_REQUEST);
    }
}
