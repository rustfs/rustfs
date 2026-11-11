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
//! Builds a [`RequestEnvelope`] from a request the gateway accepted, authenticated and routed.
//!
//! Responsible for: `RequestEnvelope::from_gateway`, the gateway edge's counterpart of the
//! legacy edge's `from_s3s` (`storage/s3_compat/envelope.rs`).
//! NOT responsible for: the facts the access phase attaches afterwards (`ReqInfo` and what the
//! operation hooks retain); `super::access` installs those.
//! Upstream: the gateway's handler request context. Downstream: every gateway handler.
//!
//! Every fact is copied from where the gateway or a RustFS layer in front of it put it: the
//! request line and the accepted header lines from the context, the caller from the verified
//! principal, and the payloads the connection and request layers attached from the transport
//! values the gateway carried through unchanged.

use crate::app::object::request_body::BodyReadControl;
use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
use crate::auth::{
    AuthType, VerifiedPresignedRequest, VerifiedSigV4Request, get_request_auth_type_with_query, get_session_token,
};
use crate::runtime_sources::ServerContextSlot;
use crate::shared_types::{NativeTlsTransport, RemoteAddr};
use crate::storage_api::server::http::request_context::RequestContext;
use http::{Extensions, HeaderMap, Uri};
use rustfs_gateway::{RequestContextView, TransportExtensions};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
use rustfs_trusted_proxies::ClientInfo;
use std::sync::Arc;

impl RequestEnvelope {
    /// Copies every request fact of `context` the app layer reads, for the server `server_ctx`.
    ///
    /// A payload the request does not carry leaves its field `None`. The caller's secret key is
    /// copied only when the gateway handed it to this operation; the signature markers are set
    /// only for a caller the gateway verified.
    ///
    /// # Errors
    ///
    /// `InternalError` when the accepted request target cannot be read back as a URI, or a
    /// handed-over secret key is not text. Neither is the caller's fault.
    pub(crate) fn from_gateway(context: &RequestContextView, server_ctx: &Arc<ServerContextSlot>) -> S3Result<Self> {
        let uri = request_uri(context)?;
        let mut headers = HeaderMap::new();
        for (name, value) in context.headers().iter_raw() {
            headers.append(name.clone(), value.clone());
        }
        let credentials = match context.principal() {
            Some(principal) => {
                let secret_key = principal
                    .secret_key_from_authenticator_lookup()
                    .map(|secret| secret.expose_secret());
                Some(request_credentials(
                    principal.access_key_id(),
                    secret_key,
                    get_session_token(&uri, &headers),
                )?)
            }
            None => None,
        };

        let mut extensions = Extensions::new();
        let transport = context.transport_extensions();
        copy_payload::<Option<RemoteAddr>>(transport, &mut extensions);
        copy_payload::<ClientInfo>(transport, &mut extensions);
        copy_payload::<NativeTlsTransport>(transport, &mut extensions);
        copy_payload::<RequestContext>(transport, &mut extensions);
        copy_payload::<BodyReadControl>(transport, &mut extensions);
        extensions.insert(Arc::clone(server_ctx));
        // The legacy access phase marks a request whose SigV4 signature its edge verified; here
        // the gateway verified it, and only a verified caller has a principal.
        if credentials.is_some() {
            match get_request_auth_type_with_query(&headers, uri.query()) {
                AuthType::Presigned => {
                    extensions.insert(VerifiedPresignedRequest);
                    extensions.insert(VerifiedSigV4Request);
                }
                AuthType::Signed => {
                    extensions.insert(VerifiedSigV4Request);
                }
                _ => {}
            }
        }

        let scope = context.verified_scope();
        let head = RequestHead {
            method: context.method().clone(),
            uri,
            headers,
            credentials,
            region: scope.map(|scope| scope.region().to_owned()),
            service: scope.map(|scope| scope.service().to_owned()),
        };
        Ok(Self::from_parts(head, extensions))
    }
}

/// The request target as it arrived: the raw path and query, with the scheme and authority when
/// the transport carried them (an absolute-form target, or HTTP/2).
fn request_uri(context: &RequestContextView) -> S3Result<Uri> {
    let path_and_query = match context.raw_query() {
        "" => context.raw_path().to_owned(),
        query => format!("{}?{query}", context.raw_path()),
    };
    let builder = match (context.target_scheme(), context.target_authority()) {
        (Some(scheme), Some(authority)) => Uri::builder().scheme(scheme).authority(authority),
        _ => Uri::builder(),
    };
    builder
        .path_and_query(path_and_query)
        .build()
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "the accepted request target is not a URI"))
}

/// The caller's credentials as the app layer holds them. `secret_key` is what the gateway handed
/// over, which is nothing unless the operation opted in.
fn request_credentials(access_key: &str, secret_key: Option<&[u8]>, session_token: Option<&str>) -> S3Result<RequestCredentials> {
    let session_token = session_token.map(str::to_owned);
    let Some(secret_key) = secret_key else {
        return Ok(RequestCredentials::without_secret_key(access_key.to_owned(), session_token));
    };
    let secret_key = std::str::from_utf8(secret_key)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "the caller's secret key is not text"))?;
    // The gateway refuses a credential whose secret is empty, so a handed-over secret is never
    // empty. Should one arrive, the request is refused rather than served with a secret that
    // every stored secret would have to be compared against.
    RequestCredentials::new(access_key.to_owned(), secret_key.to_owned(), session_token)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "the caller's secret key is empty"))
}

/// Copies the payload of type `T` a layer in front of the gateway attached, when one did.
fn copy_payload<T: Clone + Send + Sync + 'static>(transport: &TransportExtensions, extensions: &mut Extensions) {
    if let Some(payload) = transport.get::<T>() {
        extensions.insert(payload.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::gateway_handlers::error::handler_error;
    use bytes::Bytes;
    use http::{HeaderValue, Method, Request, StatusCode};
    use http_body_util::{BodyExt, Empty};
    use rustfs_gateway::sig::{AmzDate, PayloadMode, SigService, SigV4Signer, SigningCredentials, SigningRequest, SigningScope};
    use rustfs_gateway::{
        Credentials, Handler, HandlerResult, Limits, RegionSet, Req, Resp, ServiceBuilder, SigV4Authenticator, StaticCredentials,
        WireRequest, allow_when, dto,
    };
    use std::net::{IpAddr, SocketAddr};
    use std::sync::Mutex;
    use std::time::Instant;

    const PEER: ([u8; 4], u16) = ([10, 0, 0, 7], 41000);
    const CLIENT: [u8; 4] = [203, 0, 113, 9];
    const REQUEST_ID: &str = "envelope-request-id";
    const SIGNER_ACCESS_KEY: &str = "AKIAENVELOPESIGNER";
    const SIGNER_SECRET_KEY: &str = "envelope-signer-secret-key-0123456789";

    /// Records the envelope the gateway edge builds for the request it is handed.
    struct Probe {
        server_ctx: Arc<ServerContextSlot>,
        seen: Mutex<Option<RequestEnvelope>>,
    }

    impl Handler<dto::ListBuckets> for Probe {
        async fn call(&self, request: Req<dto::ListBuckets>) -> HandlerResult<dto::ListBuckets> {
            let envelope = RequestEnvelope::from_gateway(request.context(), &self.server_ctx).map_err(handler_error)?;
            *self.seen.lock().expect("probe lock") = Some(envelope);
            Ok(Resp::new(dto::ListBucketsOutput::default()))
        }
    }

    /// Sends `request` through a gateway whose only handler is the probe, and returns what the
    /// probe built. The test authorizer admits the one probed operation and nothing else.
    async fn envelope_of(request: Request<Empty<Bytes>>, server_ctx: &Arc<ServerContextSlot>) -> RequestEnvelope {
        let probe = Arc::new(Probe {
            server_ctx: Arc::clone(server_ctx),
            seen: Mutex::new(None),
        });
        let regions = RegionSet::new(["us-east-1"]).expect("a region");
        let service = ServiceBuilder::new()
            .rustfs_profile()
            .authenticator(SigV4Authenticator::new(Arc::new(signer_credentials()), regions).rustfs_profile())
            .authorizer(allow_when(|request| request.operation == "ListBuckets"))
            .register::<dto::ListBuckets, _>(Arc::clone(&probe))
            .build()
            .expect("the probe service assembles");

        let response = service.call(request).await;
        let status = response.status();
        if status != StatusCode::OK {
            let body = response
                .into_body()
                .collect()
                .await
                .map(|body| body.to_bytes())
                .unwrap_or_default();
            panic!("the probe must be reached: {status} {}", String::from_utf8_lossy(&body));
        }
        probe
            .seen
            .lock()
            .expect("probe lock")
            .take()
            .expect("the probe saw the request")
    }

    /// The one caller the probe service knows.
    fn signer_credentials() -> StaticCredentials {
        StaticCredentials::new().with(Credentials::new(SIGNER_ACCESS_KEY, SIGNER_SECRET_KEY.as_bytes()).expect("a valid key"))
    }

    /// A ListBuckets request header-signed now by the probe service's one caller.
    fn signed_list_buckets() -> Request<Empty<Bytes>> {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, HeaderValue::from_static("localhost"));
        let probe = list_buckets_request("/").body(Bytes::new()).expect("static request");
        let accepted = WireRequest::accept(probe, &Limits::default()).expect("an acceptable host");
        let stamp = AmzDate::parse(&chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()).expect("a stamp");
        let scope = SigningScope::new(stamp.day(), "us-east-1", SigService::S3).expect("a scope");
        let credentials = SigningCredentials::new(SIGNER_ACCESS_KEY, SIGNER_SECRET_KEY.as_bytes()).expect("credentials");
        let mut signer = SigV4Signer::new(credentials, scope);
        let method = Method::GET;
        let signing = SigningRequest::new(
            &method,
            "/",
            "",
            &headers,
            accepted.host().raw_for_signing(),
            PayloadMode::Unsigned,
            stamp,
        );
        let signed = signer.sign_headers(&signing).expect("a signable request");
        let mut builder = Request::builder().method(method).uri("/");
        for (name, value) in signed.headers() {
            builder = builder.header(name, value);
        }
        builder.body(Empty::new()).expect("a request")
    }

    fn list_buckets_request(target: &str) -> http::request::Builder {
        Request::builder().method(Method::GET).uri(target).header("host", "localhost")
    }

    fn request_context() -> RequestContext {
        RequestContext {
            request_id: REQUEST_ID.to_owned(),
            x_amz_request_id: REQUEST_ID.to_owned(),
            trace_id: None,
            span_id: None,
            start_time: Instant::now(),
        }
    }

    #[tokio::test]
    async fn from_gateway_copies_every_fact_of_the_accepted_request() {
        let server_ctx = ServerContextSlot::new();
        let request = list_buckets_request("/?prefix=probe")
            .header("x-envelope-probe", "probe")
            .extension(Some(RemoteAddr(SocketAddr::from(PEER))))
            .extension(ClientInfo::direct(SocketAddr::new(IpAddr::from(CLIENT), 443)))
            .extension(NativeTlsTransport(true))
            .extension(request_context())
            .extension(BodyReadControl::default())
            .body(Empty::new())
            .expect("static request");

        let envelope = envelope_of(request, &server_ctx).await;

        assert_eq!(envelope.method(), Method::GET);
        assert_eq!(envelope.uri().path(), "/");
        assert_eq!(envelope.uri().query(), Some("prefix=probe"));
        assert_eq!(envelope.headers().get("x-envelope-probe"), Some(&HeaderValue::from_static("probe")));
        assert_eq!(envelope.remote_addr(), Some(SocketAddr::from(PEER)));
        assert_eq!(envelope.client_info().map(|info| info.real_ip), Some(IpAddr::from(CLIENT)));
        assert_eq!(envelope.native_tls_transport(), Some(true));
        assert_eq!(envelope.request_context().map(|context| context.request_id.as_str()), Some(REQUEST_ID));
        assert!(envelope.body_read_control().is_some(), "body_read_control");
        assert!(
            envelope.server_context().is_some_and(|slot| Arc::ptr_eq(slot, &server_ctx)),
            "server_context"
        );
        assert!(envelope.residual_extensions().is_empty(), "every copied payload is a named field");
    }

    #[tokio::test]
    async fn from_gateway_leaves_absent_payloads_none_and_an_anonymous_caller_unverified() {
        let server_ctx = ServerContextSlot::new();
        let request = list_buckets_request("/").body(Empty::new()).expect("static request");

        let envelope = envelope_of(request, &server_ctx).await;

        assert!(envelope.credentials().is_none(), "credentials");
        assert_eq!(envelope.region(), None, "region");
        assert_eq!(envelope.service(), None, "service");
        assert_eq!(envelope.remote_addr(), None, "remote_addr");
        assert!(envelope.client_info().is_none(), "client_info");
        assert_eq!(envelope.native_tls_transport(), None, "native_tls_transport");
        assert!(envelope.request_context().is_none(), "request_context");
        assert!(envelope.body_read_control().is_none(), "body_read_control");
        assert!(envelope.verified_sigv4().is_none(), "verified_sigv4");
        assert!(envelope.verified_presigned().is_none(), "verified_presigned");
        assert!(envelope.req_info().is_none(), "the access phase installs req_info, not the edge");
    }

    #[tokio::test]
    async fn from_gateway_treats_a_peerless_remote_addr_as_absent() {
        let server_ctx = ServerContextSlot::new();
        let request = list_buckets_request("/")
            .extension(None::<RemoteAddr>)
            .body(Empty::new())
            .expect("static request");

        let envelope = envelope_of(request, &server_ctx).await;

        assert_eq!(envelope.remote_addr(), None);
        assert!(envelope.residual_extensions().is_empty(), "the peerless marker is consumed, not kept");
    }

    #[test]
    fn credentials_carry_the_secret_only_when_the_gateway_handed_it_over() {
        let kept = request_credentials("AKIAENVELOPE", None, Some("token")).expect("credentials");
        assert_eq!(kept.access_key(), "AKIAENVELOPE");
        assert!(kept.secret_key().is_none());
        assert_eq!(kept.session_token(), Some("token"));

        let handed = request_credentials("AKIAENVELOPE", Some(b"envelope-secret"), None).expect("credentials");
        assert_eq!(handed.secret_key(), Some("envelope-secret"));
        assert_eq!(handed.session_token(), None);
    }

    #[test]
    fn an_empty_handed_over_secret_is_refused_instead_of_kept() {
        let error = request_credentials("AKIAENVELOPE", Some(b""), None).expect_err("empty");

        assert_eq!(error.code(), &S3ErrorCode::InternalError);
    }

    /// ListBuckets does not opt in to the caller's secret, so the gateway keeps it: the handler
    /// sees who signed and no secret, and a reader that needs one is refused instead of being
    /// handed an empty string to compare with.
    #[tokio::test]
    async fn an_operation_that_did_not_opt_in_reaches_the_handler_without_the_caller_s_secret() {
        let server_ctx = ServerContextSlot::new();

        let envelope = envelope_of(signed_list_buckets(), &server_ctx).await;

        let credentials = envelope.credentials().expect("a signed request has a caller");
        assert_eq!(credentials.access_key(), SIGNER_ACCESS_KEY);
        assert!(credentials.secret_key().is_none(), "the secret stays with the gateway");
        assert!(credentials.require_secret_key().is_err(), "a reader of the secret fails closed");
        assert!(!credentials.secret_key_matches(""), "an absent secret equals no stored secret");
        assert!(!credentials.secret_key_matches(SIGNER_SECRET_KEY));
    }

    #[test]
    fn a_secret_that_is_not_text_is_refused_instead_of_dropped() {
        let error = request_credentials("AKIAENVELOPE", Some(&[0xff, 0xfe]), None).expect_err("not text");

        assert_eq!(error.code(), &S3ErrorCode::InternalError);
    }
}
