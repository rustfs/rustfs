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
//! NOT responsible for: the caller's `ReqInfo` and what an operation's authorization retains;
//! `super::access` and `Served::authorize` install those.
//! Upstream: `super::access::admit`. Downstream: every use case the gateway stack calls.
//!
//! Every fact is copied from where the gateway or a RustFS layer in front of it put it: the
//! request line and the accepted header lines from the context, the caller from the verified
//! principal, and the payloads the connection and request layers attached from the transport
//! values the gateway carried through unchanged.

use crate::app::object::request_body::BodyReadControl;
use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
use crate::auth::get_session_token;
use crate::runtime_sources::ServerContextSlot;
use crate::server::gateway_stack::OwnedKeystoneIdentity;
use crate::shared_types::{NativeTlsTransport, RemoteAddr};
use crate::storage_api::server::http::access::{AccessMarkers, VerifiedSignature};
use crate::storage_api::server::http::request_context::RequestContext;
use http::{Extensions, HeaderMap, HeaderValue, Uri};
use rustfs_gateway::sig::{SigFamily, SigLocation};
use rustfs_gateway::{IdentityOrigin, RequestContextView, RequestPrincipal, TransportExtensions};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
use rustfs_trusted_proxies::ClientInfo;
use std::sync::Arc;

impl RequestEnvelope {
    /// Copies every request fact of `context` the app layer reads, for the server `server_ctx`, with
    /// the access markers of `signature` and of the routed operation.
    ///
    /// A payload the request does not carry leaves its field `None`. The caller's secret key is
    /// copied only when the gateway handed it to this operation. A request that sent no `Host` line,
    /// which only HTTP/2 and HTTP/3 may, gets one from its authority, as the readers of `Host` expect.
    ///
    /// # Errors
    ///
    /// `InternalError` when the accepted request target cannot be read back as a URI, or a
    /// handed-over secret key is not text. Neither is the caller's fault.
    pub(crate) fn from_gateway(
        context: &RequestContextView,
        server_ctx: &Arc<ServerContextSlot>,
        signature: VerifiedSignature,
    ) -> S3Result<Self> {
        let uri = request_uri(context)?;
        let mut headers = HeaderMap::new();
        for (name, value) in context.headers().iter_raw() {
            headers.append(name.clone(), value.clone());
        }
        if !headers.contains_key(http::header::HOST)
            && let Some(authority) = context.target_authority()
        {
            let host =
                HeaderValue::from_str(authority).map_err(|_| unreadable("the accepted authority is not a header value"))?;
            headers.insert(http::header::HOST, host);
        }
        let credentials = match context.principal() {
            Some(principal) => {
                let secret_key = principal
                    .secret_key_from_authenticator_lookup()
                    .map(|secret| secret.expose_secret());
                Some(request_credentials(
                    principal.access_key_id(),
                    secret_key,
                    credential_token(context, principal, &uri, &headers)?,
                )?)
            }
            None => match context.header_token_principal() {
                Some(_) => {
                    let owned = keystone_identity(context)?;
                    Some(RequestCredentials::without_secret_key(owned.credentials().access_key.clone(), None))
                }
                None => None,
            },
        };

        let mut extensions = Extensions::new();
        let transport = context.transport_extensions();
        copy_payload::<Option<RemoteAddr>>(transport, &mut extensions);
        copy_payload::<ClientInfo>(transport, &mut extensions);
        copy_payload::<NativeTlsTransport>(transport, &mut extensions);
        copy_payload::<RequestContext>(transport, &mut extensions);
        copy_payload::<BodyReadControl>(transport, &mut extensions);
        extensions.insert(Arc::clone(server_ctx));
        AccessMarkers::new(context.operation(), signature).install(&mut extensions);

        let scope = context.verified_scope();
        let head = RequestHead {
            method: context.method().clone(),
            uri,
            headers,
            credentials,
            region: scope.map(|scope| scope.region().to_owned()),
            service: scope.map(|scope| scope.service().to_owned()),
        };
        let mut envelope = Self::from_parts(head, extensions);
        if context.operation() == "PostObject" {
            envelope.mark_gateway_post();
        }
        Ok(envelope)
    }
}

/// Binds the gateway's host-token verdict to this request's owned authorization identity.
pub(super) fn keystone_identity(context: &RequestContextView) -> S3Result<&OwnedKeystoneIdentity> {
    let denied = || rustfs_s3_types::s3_error!(AccessDenied, "Keystone identity is not established");
    let principal = context.header_token_principal().ok_or_else(denied)?;
    let owned = context
        .transport_extensions()
        .get::<OwnedKeystoneIdentity>()
        .ok_or_else(denied)?;
    if principal.origin() != IdentityOrigin::HostToken || principal.access_key_id() != owned.credentials().access_key {
        return Err(denied());
    }
    Ok(owned)
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
        .map_err(|_| unreadable("the accepted request target is not a URI"))
}

fn credential_token<'a>(
    context: &RequestContextView,
    principal: &'a RequestPrincipal,
    uri: &'a Uri,
    headers: &'a HeaderMap,
) -> S3Result<Option<&'a str>> {
    if context.operation() != "PostObject" {
        return Ok(get_session_token(uri, headers));
    }
    let scheme = principal.scheme();
    if !matches!(scheme.family(), SigFamily::V4 | SigFamily::V2) || scheme.location() != SigLocation::FormField {
        return Err(unreadable("the PostObject principal was not authenticated by a form signature"));
    }
    if !scheme.is_temporary() {
        return Ok(None);
    }
    let token = principal
        .verified_form_session_token()
        .ok_or_else(|| unreadable("the verified PostObject session token is missing"))?;
    let token =
        std::str::from_utf8(token.expose()).map_err(|_| unreadable("the verified PostObject session token is not text"))?;
    Ok(Some(token))
}

/// The caller's credentials as the app layer holds them. `secret_key` is what the gateway handed
/// over, which is nothing unless the operation opted in.
fn request_credentials(access_key: &str, secret_key: Option<&[u8]>, session_token: Option<&str>) -> S3Result<RequestCredentials> {
    let session_token = session_token.map(str::to_owned);
    let Some(secret_key) = secret_key else {
        return Ok(RequestCredentials::without_secret_key(access_key.to_owned(), session_token));
    };
    let secret_key = std::str::from_utf8(secret_key).map_err(|_| unreadable("the caller's secret key is not text"))?;
    // The gateway refuses a credential whose secret is empty, so a handed-over secret is never
    // empty. Should one arrive, the request is refused rather than served with a secret that
    // every stored secret would have to be compared against.
    RequestCredentials::new(access_key.to_owned(), secret_key.to_owned(), session_token)
        .map_err(|_| unreadable("the caller's secret key is empty"))
}

/// A request fact the edge cannot carry, which is never the caller's fault.
fn unreadable(message: &'static str) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, message)
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
    use crate::server::gateway_handlers::access::verified_signature;
    use crate::server::gateway_handlers::error::handler_error;
    use crate::server::gateway_handlers::test_support::{CLIENT, PEER, REQUEST_ID, Signed, request_context, signed_request};
    use bytes::Bytes;
    use http::{Method, Request, StatusCode, Version};
    use http_body_util::{BodyExt, Empty};
    use rustfs_gateway::{
        Credentials, Handler, HandlerResult, OperationCodec, RegionSet, Req, Resp, ServiceBuilder, SigV4Authenticator,
        StaticCredentials, allow_when, dto,
    };
    use std::net::{IpAddr, SocketAddr};
    use std::sync::Mutex;

    const SIGNER_ACCESS_KEY: &str = "AKIAENVELOPESIGNER";
    const SIGNER_SECRET_KEY: &str = "envelope-signer-secret-key-0123456789";

    /// Records the envelope the gateway edge builds for the request it is handed.
    struct Probe {
        server_ctx: Arc<ServerContextSlot>,
        seen: Mutex<Option<RequestEnvelope>>,
    }

    impl<O: OperationCodec> Handler<O> for Probe
    where
        O::Output: Default,
    {
        async fn call(&self, request: Req<O>) -> HandlerResult<O> {
            let context = request.context();
            let envelope =
                RequestEnvelope::from_gateway(context, &self.server_ctx, verified_signature(context)).map_err(handler_error)?;
            *self.seen.lock().expect("probe lock") = Some(envelope);
            Ok(Resp::new(O::Output::default()))
        }
    }

    /// Sends `request` through a gateway whose only handler is the probe, registered for `O`, and
    /// returns what the probe built.
    async fn envelope_of<O: OperationCodec>(
        request: Request<Empty<Bytes>>,
        server_ctx: &Arc<ServerContextSlot>,
    ) -> RequestEnvelope
    where
        O::Output: Default,
    {
        let probe = Arc::new(Probe {
            server_ctx: Arc::clone(server_ctx),
            seen: Mutex::new(None),
        });
        let regions = RegionSet::new(["us-east-1"]).expect("a region");
        let credentials = StaticCredentials::new()
            .with(Credentials::new(SIGNER_ACCESS_KEY, SIGNER_SECRET_KEY.as_bytes()).expect("a valid key"));
        let service = ServiceBuilder::new()
            .rustfs_profile()
            .authenticator(SigV4Authenticator::new(Arc::new(credentials), regions).rustfs_profile())
            .authorizer(allow_when(|request| request.operation == O::NAME))
            .register::<O, _>(Arc::clone(&probe))
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

    fn list_buckets_request(target: &str) -> http::request::Builder {
        Request::builder().method(Method::GET).uri(target).header("host", "localhost")
    }

    fn signed_list_buckets(form: Signed) -> Request<Empty<Bytes>> {
        signed_request(form, Method::GET, "/", "", SIGNER_ACCESS_KEY, SIGNER_SECRET_KEY)
    }

    #[test]
    fn from_gateway_marks_only_post_object_provenance() {
        let server_ctx = ServerContextSlot::new();
        for (operation, expected) in [("PostObject", true), ("PutObject", false)] {
            let context = RequestContextView::detached(operation);
            let envelope = RequestEnvelope::from_gateway(&context, &server_ctx, VerifiedSignature::default())
                .expect("detached gateway context");
            assert_eq!(envelope.is_gateway_post(), expected, "{operation}");
            assert_eq!(
                envelope.fork_for_authorization().is_gateway_post(),
                expected,
                "{operation} authorization fork"
            );
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
            .extension(request_context(REQUEST_ID))
            .extension(BodyReadControl::default())
            .body(Empty::new())
            .expect("static request");

        let envelope = envelope_of::<dto::ListBuckets>(request, &server_ctx).await;

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
    async fn from_gateway_leaves_absent_payloads_none_and_an_anonymous_caller_unmarked() {
        let server_ctx = ServerContextSlot::new();
        let request = list_buckets_request("/").body(Empty::new()).expect("static request");

        let envelope = envelope_of::<dto::ListBuckets>(request, &server_ctx).await;

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
        assert!(envelope.multipart_staging_operation().is_none(), "ListBuckets stages no upload");
        assert!(envelope.req_info().is_none(), "the access phase installs req_info, not the edge");
    }

    #[tokio::test]
    async fn from_gateway_treats_a_peerless_remote_addr_as_absent() {
        let server_ctx = ServerContextSlot::new();
        let request = list_buckets_request("/")
            .extension(None::<RemoteAddr>)
            .body(Empty::new())
            .expect("static request");

        let envelope = envelope_of::<dto::ListBuckets>(request, &server_ctx).await;

        assert_eq!(envelope.remote_addr(), None);
        assert!(envelope.residual_extensions().is_empty(), "the peerless marker is consumed, not kept");
    }

    #[tokio::test]
    async fn a_header_signature_marks_sigv4_and_not_presigned() {
        let envelope = envelope_of::<dto::ListBuckets>(signed_list_buckets(Signed::Header), &ServerContextSlot::new()).await;

        assert!(envelope.verified_sigv4().is_some(), "verified_sigv4");
        assert!(envelope.verified_presigned().is_none(), "verified_presigned");
    }

    #[tokio::test]
    async fn a_query_signature_marks_both_sigv4_and_presigned() {
        let envelope = envelope_of::<dto::ListBuckets>(signed_list_buckets(Signed::Query), &ServerContextSlot::new()).await;

        assert!(envelope.verified_sigv4().is_some(), "verified_sigv4");
        assert!(envelope.verified_presigned().is_some(), "verified_presigned");
    }

    #[tokio::test]
    async fn sigv4a_header_and_query_signatures_mark_verified_and_emit_v4a_conditions() {
        use crate::app::storage_api::owned_parity::parity_test_support::request_authz_facts;
        use crate::auth::facts::{AuthzPrincipal, AuthzTarget, build_conditions};
        use rustfs_gateway::sig::{AmzDate, PayloadMode, SigService, SigV4aSigner, SigningCredentials, SigningRequest};
        use rustfs_gateway::{Limits, WireRequest};
        use rustfs_policy::policy::action::{Action, S3Action};

        let sign_v4a = |form: Signed| {
            let mut headers = HeaderMap::new();
            headers.insert(http::header::HOST, HeaderValue::from_static("localhost"));
            let probe = Request::builder()
                .method(Method::GET)
                .uri("/")
                .header(http::header::HOST, "localhost")
                .body(Bytes::new())
                .expect("probe");
            let accepted = WireRequest::accept(probe, &Limits::default()).expect("host");
            let stamp = AmzDate::parse(&chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()).expect("stamp");
            let creds = SigningCredentials::new(SIGNER_ACCESS_KEY, SIGNER_SECRET_KEY.as_bytes()).expect("creds");
            let signer = SigV4aSigner::new(creds, stamp.day(), SigService::S3, "*").expect("signer");
            let req = SigningRequest::new(
                &Method::GET,
                "/",
                "",
                &headers,
                accepted.host().raw_for_signing(),
                PayloadMode::Unsigned,
                stamp,
            );
            let signed = match form {
                Signed::Header => signer.sign_headers(&req),
                Signed::Query => signer.presign(&req, 300),
            }
            .expect("signed SigV4a");
            let mut builder = Request::builder().method(Method::GET).uri(signed.target());
            for (name, value) in signed.headers() {
                builder = builder.header(name, value);
            }
            builder.body(Empty::new()).expect("request")
        };

        let cred = rustfs_credentials::Credentials {
            access_key: SIGNER_ACCESS_KEY.to_owned(),
            ..Default::default()
        };
        let target = || AuthzTarget {
            action: Action::S3Action(S3Action::ListAllMyBucketsAction),
            bucket: "",
            object: "",
            version_id: None,
            location_constraint: None,
        };

        let header_env = envelope_of::<dto::ListBuckets>(sign_v4a(Signed::Header), &ServerContextSlot::new()).await;
        assert!(header_env.verified_sigv4().is_some(), "header SigV4a verified_sigv4");
        assert!(header_env.verified_presigned().is_none(), "header SigV4a verified_presigned");
        let header_conditions = build_conditions(&AuthzPrincipal::new(&cred, ""), &request_authz_facts(&header_env, target()));
        assert_eq!(header_conditions.get("authType"), Some(&vec!["REST-HEADER".to_owned()]));
        assert_eq!(
            header_conditions.get("signatureversion"),
            Some(&vec!["AWS4-ECDSA-P256-SHA256".to_owned()])
        );

        let query_env = envelope_of::<dto::ListBuckets>(sign_v4a(Signed::Query), &ServerContextSlot::new()).await;
        assert!(query_env.verified_sigv4().is_some(), "presigned SigV4a verified_sigv4");
        assert!(query_env.verified_presigned().is_some(), "presigned SigV4a verified_presigned");
        let query_conditions = build_conditions(&AuthzPrincipal::new(&cred, ""), &request_authz_facts(&query_env, target()));
        assert_eq!(query_conditions.get("authType"), Some(&vec!["REST-QUERY-STRING".to_owned()]));
        assert_eq!(query_conditions.get("signatureversion"), Some(&vec!["AWS4-ECDSA-P256-SHA256".to_owned()]));
    }

    #[tokio::test]
    async fn presigned_and_anonymous_gateway_envelopes_ignore_unsigned_auth_type_headers() {
        use crate::app::storage_api::owned_parity::parity_test_support::request_authz_facts;
        use crate::auth::facts::{AuthzPrincipal, AuthzTarget, build_conditions};
        use rustfs_policy::policy::action::{Action, S3Action};

        let mut presigned = signed_list_buckets(Signed::Query);
        presigned
            .headers_mut()
            .insert("awsaccesskeyid", HeaderValue::from_static("x"));
        let envelope = envelope_of::<dto::ListBuckets>(presigned, &ServerContextSlot::new()).await;
        assert!(envelope.verified_sigv4().is_some(), "verified_sigv4");
        assert!(envelope.verified_presigned().is_some(), "verified_presigned");
        let cred = rustfs_credentials::Credentials {
            access_key: SIGNER_ACCESS_KEY.to_owned(),
            ..Default::default()
        };
        let facts = request_authz_facts(
            &envelope,
            AuthzTarget {
                action: Action::S3Action(S3Action::ListAllMyBucketsAction),
                bucket: "",
                object: "",
                version_id: None,
                location_constraint: None,
            },
        );
        let conditions = build_conditions(&AuthzPrincipal::new(&cred, ""), &facts);
        assert_eq!(conditions.get("authType"), Some(&vec!["REST-QUERY-STRING".to_owned()]));
        assert_eq!(conditions.get("signatureversion"), Some(&vec!["AWS4-HMAC-SHA256".to_owned()]));

        let anon_request = list_buckets_request("/")
            .header("awsaccesskeyid", "x")
            .header("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
            .header("content-type", "multipart/form-data; boundary=x")
            .header("action", "AssumeRole")
            .body(Empty::new())
            .expect("static request");
        let anon_envelope = envelope_of::<dto::ListBuckets>(anon_request, &ServerContextSlot::new()).await;
        assert!(anon_envelope.verified_sigv4().is_none(), "verified_sigv4");
        assert!(anon_envelope.verified_presigned().is_none(), "verified_presigned");
        let anon_cred = rustfs_credentials::Credentials::default();
        let anon_facts = request_authz_facts(
            &anon_envelope,
            AuthzTarget {
                action: Action::S3Action(S3Action::ListAllMyBucketsAction),
                bucket: "",
                object: "",
                version_id: None,
                location_constraint: None,
            },
        );
        let anon_conditions = build_conditions(&AuthzPrincipal::new(&anon_cred, ""), &anon_facts);
        assert_eq!(anon_conditions.get("authType"), Some(&vec!["Anonymous".to_owned()]));
        assert!(!anon_conditions.contains_key("signatureversion"));
    }

    #[tokio::test]
    async fn an_operation_that_stages_an_upload_is_marked_as_staging() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/bucket/key?uploads")
            .header("host", "localhost")
            .body(Empty::new())
            .expect("static request");

        let envelope = envelope_of::<dto::CreateMultipartUpload>(request, &ServerContextSlot::new()).await;

        assert!(envelope.multipart_staging_operation().is_some());
    }

    #[tokio::test]
    async fn an_http2_request_without_a_host_line_gets_one_from_its_authority() {
        let request = Request::builder()
            .version(Version::HTTP_2)
            .method(Method::GET)
            .uri("http://localhost/")
            .body(Empty::new())
            .expect("static request");

        let envelope = envelope_of::<dto::ListBuckets>(request, &ServerContextSlot::new()).await;

        let hosts: Vec<&HeaderValue> = envelope.headers().get_all(http::header::HOST).iter().collect();
        assert_eq!(hosts, [&HeaderValue::from_static("localhost")]);
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
        let envelope = envelope_of::<dto::ListBuckets>(signed_list_buckets(Signed::Header), &ServerContextSlot::new()).await;

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
