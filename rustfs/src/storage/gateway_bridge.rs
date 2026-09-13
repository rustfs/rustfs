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

//! The gateway half of `RUSTFS_S3_STACK=gateway` (rustfs/backlog#1752): a RustFS Gateway service
//! whose handlers run the unchanged s3s application layer.
//!
//! Responsible for: assembling the gateway pipeline — SigV4 through the same IAM credential chain
//! the legacy stack uses, the authorization entry, one handler per served operation — and running
//! each request through RustFS's own access check and `S3` implementation, so neither the app
//! bodies nor the storage layer change.
//! NOT responsible for: choosing which requests come here (`crate::server::stack`), or making a
//! policy decision of its own.
//!
//! # One request
//!
//! 1. [`GatewayPipeline::serve`] captures the request head — method, URI, headers, and the
//!    extensions RustFS's outer layers installed (`RequestContext`, `Option<RemoteAddr>`, …) —
//!    and runs the gateway service inside a request-scoped exchange.
//! 2. The gateway accepts the wire, resolves the host, routes, and verifies SigV4 against
//!    [`IamCredentials`], which asks `IAMAuth` exactly as the s3s auth hook does.
//! 3. [`ExchangeAuthorizer`] records the identity and credential scope the pipeline verified and
//!    admits the request. It decides nothing: the decision is step 4's.
//! 4. The handler rebuilds the s3s `S3Request` from the captured head and the recorded verdict,
//!    runs `FS::check_request_access` (the body of the legacy `S3Access::check`) and the
//!    operation's access hook, then calls the operation on `FS` — the order s3s uses.
//!
//! # Why the exchange is task-local
//!
//! A gateway handler receives `Req<O>`: the decoded input and authorization proofs, but no
//! identity, scope or header map at the pinned rev, and RustFS's access check needs all three.
//! The head and verdict therefore cross from step 1 and 3 to step 4 through a task-local set for
//! this one request. It is never shared and never global, and a handler that finds it missing,
//! unverified or contradictory fails closed. rustfs/gateway#771 (ADR-0022) adds the typed
//! `req.context()` that replaces it once this crate pins a rev that carries it.
// RUSTFS_COMPAT_TODO(backlog-1752-gateway-exchange): the pinned gateway rev hands handlers neither the accepted request head nor the verified identity, so both reach the s3s app layer through a request-scoped task-local, and the GetBucketLocation DTOs are mapped here because the gateway compat conversions target another s3s revision than RustFS links. Remove after the pinned gateway rev carries the typed handler request context (rustfs/gateway#771) and the handler builds the s3s request from it, and the compat conversions target the s3s revision RustFS links.

use std::cell::RefCell;
use std::io;
use std::sync::Arc;

use http::{Extensions, HeaderMap, Method, Request as HttpRequest, StatusCode, Uri};
use rustfs_gateway::dto::{GetBucketLocation, GetBucketLocationOutput, LocationConstraint};
use rustfs_gateway::{
    Authorizer, AuthzRequest, BoxFuture, CredentialGuardConfig, CredentialLookup, CredentialProvider,
    Credentials as GatewayCredentials, Decision, ErrorCode, Handler, HandlerError, HandlerErrorContext, HandlerResult,
    InputAuthzRequest, InputDecisions, ProviderError, RegionSet, Req, RequestContext, Resp, SecurityFloor, ServiceBuilder,
    SigV4Authenticator,
};

use super::access::AccessCheckContext;
use super::ecfs::FS;
use super::storage_api::gateway_bridge_consumer::{
    Body, Credentials, GetBucketLocationInput, HttpResponse, Region, S3, S3Access, S3Auth, S3Error, S3Request,
};
use crate::auth::IAMAuth;

tokio::task_local! {
    static EXCHANGE: RefCell<Exchange>;
}

/// What the s3s service would have handed the app layer, captured before the gateway reads the
/// request.
struct CapturedHead {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    extensions: Extensions,
}

/// Who a request runs as, as the gateway verified it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Principal {
    /// The access key the verified signature named; `None` for an anonymous request.
    access_key: Option<String>,
    /// The verified credential-scope region, the value s3s puts in `S3Request::region`.
    region: Option<String>,
    /// The verified credential-scope service, the value s3s puts in `S3Request::service`.
    service: Option<String>,
}

#[derive(Debug)]
enum VerdictSlot {
    Pending,
    Recorded(Principal),
    /// The authorizer saw another operation than the stack switch classified, or two verdicts.
    Conflict,
}

/// The per-request state shared by the stack entry, the authorizer and the handler.
struct Exchange {
    operation: &'static str,
    head: Option<CapturedHead>,
    verdict: VerdictSlot,
}

impl Exchange {
    fn new(operation: &'static str, head: CapturedHead) -> Self {
        Self {
            operation,
            head: Some(head),
            verdict: VerdictSlot::Pending,
        }
    }

    /// Records the verified principal. `false` — and a poisoned exchange — when the gateway routed
    /// another operation than the switch classified, or a second, different verdict arrives.
    fn record(&mut self, operation: &str, principal: Principal) -> bool {
        if operation != self.operation {
            self.verdict = VerdictSlot::Conflict;
            return false;
        }
        match &self.verdict {
            VerdictSlot::Pending => {
                self.verdict = VerdictSlot::Recorded(principal);
                true
            }
            VerdictSlot::Recorded(recorded) if *recorded == principal => true,
            VerdictSlot::Recorded(_) | VerdictSlot::Conflict => {
                self.verdict = VerdictSlot::Conflict;
                false
            }
        }
    }

    fn is_recorded(&self) -> bool {
        matches!(self.verdict, VerdictSlot::Recorded(_))
    }

    /// Hands the head and the verdict to the handler, once.
    fn take(&mut self, operation: &str) -> Result<(CapturedHead, Principal), &'static str> {
        if operation != self.operation {
            return Err("the gateway dispatched another operation than the stack switch classified");
        }
        let VerdictSlot::Recorded(principal) = &self.verdict else {
            return Err("the request reached a handler without a recorded authorization verdict");
        };
        let principal = principal.clone();
        let head = self.head.take().ok_or("the request head was already consumed")?;
        Ok((head, principal))
    }
}

/// The RustFS Gateway service the gateway stack routes classified requests to.
#[derive(Clone)]
pub(crate) struct GatewayPipeline {
    service: rustfs_gateway::S3Service,
}

impl GatewayPipeline {
    /// Assembles the pipeline over this server's `FS` and IAM credential chain.
    ///
    /// `regions` are the credential-scope regions a signature may name.
    ///
    /// # Errors
    ///
    /// An invalid region name, or a gateway assembly refusal.
    pub(crate) fn build(fs: FS, auth: IAMAuth, regions: &[String]) -> io::Result<Self> {
        let regions = RegionSet::new(regions).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("gateway S3 stack: invalid credential-scope region set {regions:?}: {error}"),
            )
        })?;
        // The legacy stack looks every key up afresh, so an IAM user created a moment ago signs
        // in at once; a negative cache would refuse that user until its entry aged out.
        let guard = CredentialGuardConfig {
            negative_entries: 0,
            ..CredentialGuardConfig::default()
        };
        let credentials: Arc<dyn CredentialProvider> = Arc::new(IamCredentials { auth: auth.clone() });
        let backend = Arc::new(GatewayBackend { fs, auth });
        let service = ServiceBuilder::new()
            .register::<GetBucketLocation, _>(backend)
            .authenticator(SigV4Authenticator::with_guard_config(credentials, regions, guard))
            .authorizer(ExchangeAuthorizer)
            // Anonymous requests reach the authorizer and then RustFS's access check, which
            // applies bucket policy to them exactly as the legacy stack does.
            .security_floor(SecurityFloor::new().delegate_anonymous_to_authorizer_after_listing_in_the_posture_report())
            .build()
            .map_err(|error| io::Error::other(format!("gateway S3 stack assembly failed: {error}")))?;
        Ok(Self { service })
    }

    /// Answers one request the stack switch classified as `operation`.
    pub(crate) async fn serve<B>(&self, operation: &'static str, request: HttpRequest<B>) -> HttpResponse
    where
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let (mut parts, body) = request.into_parts();
        let head = CapturedHead {
            method: parts.method.clone(),
            uri: parts.uri.clone(),
            headers: parts.headers.clone(),
            extensions: std::mem::take(&mut parts.extensions),
        };
        let exchange = RefCell::new(Exchange::new(operation, head));
        // Through the tower adapter, not the inherent `call`: the adapter also writes
        // `Connection: close` when the gateway refuses to reuse the connection (a refusal that
        // left request bytes unread), which hyper needs to see to stop reading from it.
        let mut service = self.service.clone();
        let response = EXCHANGE
            .scope(exchange, tower::Service::call(&mut service, HttpRequest::from_parts(parts, body)))
            .await;
        let response = match response {
            Ok(response) => response,
            Err(never) => match never {},
        };
        response.map(Body::http_body_unsync)
    }
}

/// SigV4 secrets from the credential chain the legacy s3s auth hook uses.
struct IamCredentials {
    auth: IAMAuth,
}

impl CredentialProvider for IamCredentials {
    fn lookup<'a>(&'a self, access_key_id: &'a str) -> BoxFuture<'a, Result<CredentialLookup, ProviderError>> {
        Box::pin(async move {
            match S3Auth::get_secret_key(&self.auth, access_key_id).await {
                // Keystone keys answer an empty secret: they authenticate by token, never by
                // signature, so no signature over them may verify.
                Ok(secret) if secret.expose().is_empty() => Ok(CredentialLookup::NotFound),
                Ok(secret) => GatewayCredentials::new(access_key_id, secret.expose().as_bytes())
                    .map(CredentialLookup::Found)
                    .map_err(|_| ProviderError::Backend),
                Err(error) if error.code().as_str() == "InvalidAccessKeyId" => Ok(CredentialLookup::NotFound),
                Err(_) => Err(ProviderError::Backend),
            }
        })
    }
}

/// Records the verified principal for the handler and admits the request.
///
/// The gateway requires an authorizer; RustFS's decision needs the headers and the typed s3s
/// input, which this stage does not see, so the decision is taken by RustFS's access check in the
/// handler, before any storage call. A request whose verdict cannot be recorded is denied here.
///
/// Because the input stage admits every derived resource, register an operation here only when
/// its RustFS access hook authorizes each resource the operation derives (a copy source, say), as
/// the legacy stack relies on that hook for the same decision.
struct ExchangeAuthorizer;

impl Authorizer for ExchangeAuthorizer {
    fn authorize_route<'a>(&'a self, context: &'a RequestContext<'a>, request: &'a AuthzRequest<'a>) -> BoxFuture<'a, Decision> {
        let scope = context.verified_scope();
        let principal = Principal {
            access_key: request.identity.map(|identity| identity.access_key_id().to_owned()),
            region: scope.map(|scope| scope.region().to_owned()),
            service: scope.map(|scope| scope.service().to_owned()),
        };
        let admitted = EXCHANGE
            .try_with(|exchange| exchange.borrow_mut().record(request.operation, principal))
            .unwrap_or(false);
        Box::pin(async move { if admitted { Decision::Allow } else { Decision::Deny } })
    }

    fn authorize_input<'a>(
        &'a self,
        _context: &'a RequestContext<'a>,
        request: &'a InputAuthzRequest<'a>,
    ) -> BoxFuture<'a, InputDecisions> {
        let admitted = EXCHANGE.try_with(|exchange| exchange.borrow().is_recorded()).unwrap_or(false);
        let stage = if admitted { Decision::Allow } else { Decision::Deny };
        let decisions = request.decide_all(stage, |_| stage);
        Box::pin(async move { decisions })
    }
}

/// The handlers: each runs one operation of the unchanged s3s app layer.
struct GatewayBackend {
    fs: FS,
    auth: IAMAuth,
}

impl GatewayBackend {
    /// The s3s request the legacy stack would have built for this request, around `input`.
    async fn s3_request<T>(&self, operation: &'static str, input: T) -> Result<S3Request<T>, HandlerError> {
        let (head, principal) = EXCHANGE
            .try_with(|exchange| exchange.borrow_mut().take(operation))
            .map_err(|_| HandlerError::internal_error("the gateway request exchange is missing"))?
            .map_err(HandlerError::internal_error)?;
        let credentials = match principal.access_key {
            Some(access_key) => {
                let secret_key = S3Auth::get_secret_key(&self.auth, &access_key).await.map_err(handler_error)?;
                Some(Credentials { access_key, secret_key })
            }
            None => None,
        };
        let region = principal
            .region
            .map(|region| Region::new(region.into_boxed_str()))
            .transpose()
            .map_err(|_| HandlerError::internal_error("the verified credential-scope region is not an s3s region"))?;
        Ok(S3Request {
            input,
            method: head.method,
            uri: head.uri,
            headers: head.headers,
            extensions: head.extensions,
            credentials,
            region,
            service: principal.service,
            trailing_headers: None,
        })
    }
}

impl Handler<GetBucketLocation> for GatewayBackend {
    async fn call(&self, request: Req<GetBucketLocation>) -> HandlerResult<GetBucketLocation> {
        let input = request.into_input();
        let input = GetBucketLocationInput {
            bucket: input.bucket.as_str().to_owned(),
            expected_bucket_owner: input.expected_bucket_owner,
        };
        let mut request = self.s3_request(GetBucketLocation::NAME, input).await?;
        self.fs
            .check_request_access(&mut BridgeAccessContext::new(&mut request, GetBucketLocation::NAME))
            .await
            .map_err(handler_error)?;
        S3Access::get_bucket_location(&self.fs, &mut request)
            .await
            .map_err(handler_error)?;
        let output = S3::get_bucket_location(&self.fs, request)
            .await
            .map_err(handler_error)?
            .output;
        Ok(Resp::new(GetBucketLocationOutput {
            location_constraint: output
                .location_constraint
                .map(|constraint| LocationConstraint::custom(constraint.as_str().to_owned())),
        }))
    }
}

/// The facts RustFS's access check reads, borrowed from the rebuilt s3s request.
struct BridgeAccessContext<'a> {
    credentials: Option<&'a Credentials>,
    uri: &'a Uri,
    headers: &'a HeaderMap,
    extensions: &'a mut Extensions,
    operation: &'static str,
}

impl<'a> BridgeAccessContext<'a> {
    fn new<T>(request: &'a mut S3Request<T>, operation: &'static str) -> Self {
        Self {
            credentials: request.credentials.as_ref(),
            uri: &request.uri,
            headers: &request.headers,
            extensions: &mut request.extensions,
            operation,
        }
    }
}

impl AccessCheckContext for BridgeAccessContext<'_> {
    fn credentials(&self) -> Option<&Credentials> {
        self.credentials
    }

    fn uri(&self) -> &Uri {
        self.uri
    }

    fn headers(&self) -> &HeaderMap {
        self.headers
    }

    fn extensions_mut(&mut self) -> &mut Extensions {
        self.extensions
    }

    fn operation_name(&self) -> &str {
        self.operation
    }
}

/// The gateway error for an s3s error: the same code, status and message.
///
/// A code the gateway declares with the same status keeps the declared row; any other pairing is
/// carried as a custom code with the status s3s would have rendered, so the answer on the wire does
/// not change with the stack.
///
/// The gateway renders a few codes only from typed context (its `is_contextual` set) and answers
/// 500 for a bare one. `NoSuchBucket` is the one GetBucketLocation can produce; map each further
/// contextual code here before registering an operation that can return it.
fn handler_error(error: S3Error) -> HandlerError {
    if error.code().as_str() == "NoSuchBucket" {
        return HandlerError::from(HandlerErrorContext::missing_bucket());
    }
    let name = error.code().as_str();
    let status = error
        .status_code()
        .or_else(|| error.code().status_code())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let code = match ErrorCode::known(name) {
        Some(known) if known.default_status() == status => known,
        _ => ErrorCode::custom(name.to_owned(), status),
    };
    HandlerError::new(code, error.message().unwrap_or_default().to_owned())
}

#[cfg(test)]
mod tests {
    use super::{CapturedHead, Exchange, Principal, handler_error};
    use crate::storage::storage_api::gateway_bridge_consumer::{S3Error, S3ErrorCode};
    use http::{Extensions, HeaderMap, Method, StatusCode};
    use rustfs_gateway::{ErrorCode, HandlerError, HandlerErrorContext};

    fn head() -> CapturedHead {
        CapturedHead {
            method: Method::GET,
            uri: "/bucket?location".parse().expect("uri"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
        }
    }

    fn principal(access_key: Option<&str>) -> Principal {
        Principal {
            access_key: access_key.map(str::to_owned),
            region: Some("us-east-1".to_owned()),
            service: Some("s3".to_owned()),
        }
    }

    #[test]
    fn a_recorded_verdict_is_handed_over_once() {
        let mut exchange = Exchange::new("GetBucketLocation", head());
        assert!(exchange.take("GetBucketLocation").is_err(), "no verdict yet");
        assert!(exchange.record("GetBucketLocation", principal(Some("AK"))));
        assert!(
            exchange.record("GetBucketLocation", principal(Some("AK"))),
            "the same verdict again is harmless"
        );
        let (head, verdict) = exchange.take("GetBucketLocation").expect("recorded");
        assert_eq!(head.uri, "/bucket?location");
        assert_eq!(verdict, principal(Some("AK")));
        assert!(exchange.take("GetBucketLocation").is_err(), "the head is consumed");
    }

    #[test]
    fn a_contradictory_or_misrouted_verdict_poisons_the_exchange() {
        let mut exchange = Exchange::new("GetBucketLocation", head());
        assert!(!exchange.record("PutObject", principal(None)), "another operation than classified");
        assert!(!exchange.is_recorded());
        assert!(exchange.take("GetBucketLocation").is_err());

        let mut exchange = Exchange::new("GetBucketLocation", head());
        assert!(exchange.record("GetBucketLocation", principal(Some("AK"))));
        assert!(!exchange.record("GetBucketLocation", principal(Some("OTHER"))));
        assert!(exchange.take("GetBucketLocation").is_err(), "a conflict never reaches a handler");

        let mut exchange = Exchange::new("GetBucketLocation", head());
        assert!(exchange.record("GetBucketLocation", principal(None)));
        assert!(exchange.take("PutObject").is_err(), "a handler for another operation is refused");
    }

    #[test]
    fn s3_errors_keep_their_code_status_and_message() {
        let error = handler_error(S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied."));
        assert_eq!(error.code(), &ErrorCode::ACCESS_DENIED);
        assert_eq!(error.message(), "Access Denied.");

        // A bare NoSuchBucket would resolve to 500 in the gateway; it must carry the typed
        // missing-bucket context, which renders 404 with the legacy message.
        let error = handler_error(S3Error::with_message(S3ErrorCode::NoSuchBucket, "gone"));
        assert_eq!(error, HandlerError::from(HandlerErrorContext::missing_bucket()));

        let mut overridden = S3Error::with_message(S3ErrorCode::AccessDenied, "denied");
        overridden.set_status_code(StatusCode::SERVICE_UNAVAILABLE);
        let error = handler_error(overridden);
        assert_eq!(error.code().as_str(), "AccessDenied");
        assert_eq!(error.code().default_status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
