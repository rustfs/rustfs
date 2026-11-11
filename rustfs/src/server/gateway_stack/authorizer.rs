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
//! The gateway `Authorizer` of the RustFS assembly: an early gate, never the authorization.
//!
//! Responsible for: [`AUTHORIZATION_TABLE`], one row per registered operation, and
//! [`RustfsAuthorizer`], which answers both gateway authorization stages from it.
//! NOT responsible for: the authoritative decision. That is the legacy access hook in its owned
//! form, run by the operation's handler immediately before its use case
//! (`server/gateway_handlers`), where the legacy stack runs it; decisions, error codes and their
//! order are therefore the same code on both stacks (rustfs/backlog#2761).
//! Upstream: the gateway's route and input authorization stages. Downstream: the shared
//! authorization core, through [`DecisionCore`].
//!
//! # What this gate may do
//!
//! It may refuse a request the hook would certainly refuse too, and nothing else. It is never
//! the only check, and it never allows by default:
//!
//! - an operation without a row is denied;
//! - a `RouteDecidable` row asks the shared decision core the hook asks, with the same facts;
//! - a `DeferToHandlerHook` row evaluates only its [`RouteGate`], which names exactly what the
//!   route stage can know, and leaves the rest to the hook;
//! - a `LegacyNoCheck` row is an operation whose legacy hook is empty, said so explicitly;
//! - a core that cannot answer is `Indeterminate`, which the gateway refuses.
//!
//! Plain rows therefore evaluate policy twice, once here and once in the hook. That is accepted
//! until it is measured.
//!
//! A row may be `RouteDecidable` only when its denial concerns the one routed resource and the
//! one routed action. An operation whose denial can concern a second resource (a copy source) or
//! a version-specific action (a versioned read) is a `DeferToHandlerHook` row: the hook writes
//! the denial record with the resource and action it actually refused, and an early refusal
//! here would record the wrong ones.
//!
//! # Deferred rows
//!
//! Every row that is not `RouteDecidable` is listed here, and the census below holds the table
//! to these two lists, so a deferral cannot be added without this section changing.
//!
//! `DeferToHandlerHook`:
//!
//! - `ListBuckets`: the legacy hook is empty and the use case decides per caller. It lists every
//!   bucket for a caller allowed `s3:ListAllMyBuckets`, otherwise the buckets the caller may list
//!   or locate, and refuses when there is none; that needs the bucket inventory. The route stage
//!   refuses an anonymous caller, which the use case refuses with the same answer.
//!
//! `LegacyNoCheck`: none.

use crate::app::{RequestCredentials, RequestEnvelope, RequestHead, authorize_owned_request};
use crate::auth::get_session_token;
use crate::runtime_sources::ServerContextSlot;
use crate::shared_types::RemoteAddr;
use crate::storage_api::server::http::access::resolve_req_info;
use crate::storage_api::server::http::request_context::RequestContext;
use http::{Extensions, HeaderMap, Method, Uri};
use rustfs_gateway::{
    Authorizer, AuthzRequest, BoxFuture, Decision, InputAuthzRequest, InputDecisions, RequestContext as GatewayRequestContext,
};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
use rustfs_trusted_proxies::ClientInfo;
use std::net::SocketAddr;
use std::sync::Arc;

/// The resource an operation's action is asked about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResourceForm {
    /// The service itself: no bucket and no key.
    Service,
    /// One bucket.
    #[cfg_attr(not(test), expect(dead_code, reason = "constructed by the first bucket-level row"))]
    Bucket,
    /// One object in one bucket.
    #[cfg_attr(not(test), expect(dead_code, reason = "constructed by the first object-level row"))]
    Object,
}

/// What a deferred row's route stage evaluates: only facts that do not depend on the body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteGate {
    /// Refuses a request that presented no credentials. A request with a verified caller passes
    /// to the hook, which decides.
    AuthenticatedCaller,
}

/// How much of an operation's decision the route stage can make.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteStage {
    /// The route stage asks the shared decision core the same question, with the same facts, as
    /// the hook.
    #[cfg_attr(not(test), expect(dead_code, reason = "constructed by the first plain operation row"))]
    RouteDecidable,
    /// The route stage evaluates `gate` and the hook in the handler decides the rest.
    DeferToHandlerHook {
        /// Exactly what the route stage evaluates.
        gate: RouteGate,
        /// Why the route stage cannot decide the whole of it.
        reason: &'static str,
    },
    /// The legacy hook for this operation is empty, so there is nothing to evaluate early.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "constructed by the row of an operation whose legacy hook is empty")
    )]
    LegacyNoCheck {
        /// Why legacy checks nothing.
        reason: &'static str,
    },
}

/// The checks an operation's legacy hook performs beyond its one action, which the shared
/// decision core does not perform. There is no default: every row states this column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExtraChecks {
    /// The hook asks its one action and nothing else.
    #[cfg_attr(not(test), expect(dead_code, reason = "constructed by the first plain operation row"))]
    None,
    /// The hook, or the use case it defers to, also performs these.
    Performed(&'static [&'static str]),
}

/// One operation's authorization, as the legacy access hook performs it.
///
/// A struct literal names every column, so a row cannot leave one unspecified.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the body-input and extra-check columns record what the handler hook does and are read by the census"
    )
)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct AuthorizationRow {
    /// The operation, by the name it is registered under.
    pub(crate) operation: &'static str,
    /// The action the legacy hook asks, mapped from the operation and not from the gateway's
    /// spelling of its action: the two differ for 22 operations, and legacy's is the baseline.
    pub(crate) action: Action,
    pub(crate) resource: ResourceForm,
    pub(crate) route_stage: RouteStage,
    /// The body-derived inputs the hook needs, which no route stage has.
    pub(crate) body_inputs: &'static [&'static str],
    pub(crate) extra_checks: ExtraChecks,
}

/// Every operation the gateway stack registers. Kept equal to the registered set by the census
/// in `server/gateway_stack.rs`.
pub(crate) static AUTHORIZATION_TABLE: &[AuthorizationRow] = &[AuthorizationRow {
    operation: "ListBuckets",
    action: Action::S3Action(S3Action::ListAllMyBucketsAction),
    resource: ResourceForm::Service,
    route_stage: RouteStage::DeferToHandlerHook {
        gate: RouteGate::AuthenticatedCaller,
        reason: "the use case decides per caller from the bucket inventory",
    },
    body_inputs: &[],
    extra_checks: ExtraChecks::Performed(&[
        "a caller without s3:ListAllMyBuckets sees the buckets it may list (s3:ListBucket) or locate (s3:GetBucketLocation), \
         judged by IAM alone, and is refused when there is none",
    ]),
}];

/// The request line as it reached RustFS, attached by the host before the gateway accepts the
/// request. The gateway hands an authorizer the headers and the query, but neither the method
/// nor the target, and the shared core is asked about the request as it arrived.
#[derive(Clone, Debug)]
pub(crate) struct RequestLine {
    pub(crate) method: Method,
    pub(crate) uri: Uri,
}

/// The request facts the shared decision core is asked with, each copied from where the gateway
/// or a RustFS layer in front of it put it.
pub(crate) struct RouteFacts<'a> {
    /// The access key the gateway verified; `None` for an anonymous request.
    pub(crate) access_key: Option<&'a str>,
    /// The bucket, key and version the gateway routed, when the request names them.
    pub(crate) bucket: Option<&'a str>,
    pub(crate) key: Option<&'a str>,
    pub(crate) version_id: Option<&'a str>,
    /// The accepted header lines; `None` when the gateway handed over none.
    pub(crate) headers: Option<HeaderMap>,
    pub(crate) request_line: Option<&'a RequestLine>,
    pub(crate) client_info: Option<&'a ClientInfo>,
    pub(crate) remote_addr: Option<SocketAddr>,
    pub(crate) request_context: Option<&'a RequestContext>,
}

/// The shared authorization decision core, as the route stage of a `RouteDecidable` row asks it.
pub(crate) trait DecisionCore: Send + Sync + 'static {
    /// Decides `row`'s action for the caller and resource of `facts`: `Ok` to allow,
    /// `AccessDenied` to deny, and any other error when the core could not answer.
    fn decide<'a>(&'a self, row: &'a AuthorizationRow, facts: &'a RouteFacts<'a>) -> BoxFuture<'a, S3Result<()>>;
}

/// The decision core both stacks share: the owned authorization entry point, asked about the
/// request the route stage sees.
///
/// The caller is resolved by the function the legacy access check resolves it with, and the
/// decision is `authorize_owned_request`, which the handler's access hook calls too. It is only
/// the policy decision: a row that needs a body-derived input or a composite check is not
/// `RouteDecidable`.
pub(crate) struct OwnedDecisionCore {
    server_ctx: Arc<ServerContextSlot>,
}

impl OwnedDecisionCore {
    pub(crate) fn new(server_ctx: Arc<ServerContextSlot>) -> Self {
        Self { server_ctx }
    }

    async fn authorize(&self, row: &AuthorizationRow, facts: &RouteFacts<'_>) -> S3Result<()> {
        let (Some(line), Some(headers)) = (facts.request_line, facts.headers.as_ref()) else {
            return Err(unanswerable("the route stage was handed no request line or headers"));
        };
        let session_token = get_session_token(&line.uri, headers).unwrap_or_default().to_owned();
        let mut req_info =
            resolve_req_info(&self.server_ctx, facts.access_key, &session_token, facts.request_context.cloned()).await?;
        match row.resource {
            ResourceForm::Service => {}
            ResourceForm::Bucket => req_info.bucket = Some(routed(facts.bucket)?),
            ResourceForm::Object => {
                req_info.bucket = Some(routed(facts.bucket)?);
                req_info.object = Some(routed(facts.key)?);
                req_info.version_id = facts.version_id.map(str::to_owned);
            }
        }

        let credentials = facts.access_key.map(|access_key| {
            let session_token = (!session_token.is_empty()).then(|| session_token.clone());
            RequestCredentials::without_secret_key(access_key.to_owned(), session_token)
        });
        let mut extensions = Extensions::new();
        extensions.insert(facts.remote_addr.map(RemoteAddr));
        if let Some(client_info) = facts.client_info {
            extensions.insert(client_info.clone());
        }
        if let Some(request_context) = facts.request_context {
            extensions.insert(request_context.clone());
        }
        extensions.insert(Arc::clone(&self.server_ctx));
        let head = RequestHead {
            method: line.method.clone(),
            uri: line.uri.clone(),
            headers: headers.clone(),
            credentials,
            region: None,
            service: None,
        };
        let mut envelope = RequestEnvelope::from_parts(head, extensions);
        envelope.install_req_info(req_info);
        authorize_owned_request(&mut envelope, row.action, None).await
    }
}

impl DecisionCore for OwnedDecisionCore {
    fn decide<'a>(&'a self, row: &'a AuthorizationRow, facts: &'a RouteFacts<'a>) -> BoxFuture<'a, S3Result<()>> {
        Box::pin(self.authorize(row, facts))
    }
}

/// The routed name a row's resource form requires. Its absence is a question the core cannot
/// answer, never a decision about some other resource.
fn routed(name: Option<&str>) -> S3Result<String> {
    name.map(str::to_owned)
        .ok_or_else(|| unanswerable("the route stage was handed no name for the row's resource"))
}

fn unanswerable(message: &'static str) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, message)
}

/// What the table decides about one request by itself, and what it leaves to the core.
enum Question<'a> {
    Decided(Decision),
    AskCore(&'static AuthorizationRow, Box<RouteFacts<'a>>),
}

/// The RustFS implementation of the gateway's two authorization stages.
pub(crate) struct RustfsAuthorizer {
    table: &'static [AuthorizationRow],
    core: Arc<dyn DecisionCore>,
}

impl RustfsAuthorizer {
    /// The authorizer over `table`, asking `core` for `RouteDecidable` rows.
    pub(crate) fn new(table: &'static [AuthorizationRow], core: Arc<dyn DecisionCore>) -> Self {
        Self { table, core }
    }

    /// Reads `operation`'s row. `facts` is built only for a row that asks the core.
    fn question<'f>(&self, operation: &str, anonymous: bool, facts: impl FnOnce() -> RouteFacts<'f>) -> Question<'f> {
        let table: &'static [AuthorizationRow] = self.table;
        let Some(row) = table.iter().find(|row| row.operation == operation) else {
            // No row, no answer to give: refused, never allowed.
            return Question::Decided(Decision::Allow);
        };
        match row.route_stage {
            RouteStage::RouteDecidable => Question::AskCore(row, Box::new(facts())),
            RouteStage::DeferToHandlerHook { gate, .. } => Question::Decided(match gate {
                RouteGate::AuthenticatedCaller if anonymous => Decision::Deny,
                RouteGate::AuthenticatedCaller => Decision::Allow,
            }),
            RouteStage::LegacyNoCheck { .. } => Question::Decided(Decision::Allow),
        }
    }

    async fn answer(&self, question: Question<'_>) -> Decision {
        match question {
            Question::Decided(decision) => decision,
            Question::AskCore(row, facts) => core_decision(self.core.decide(row, &facts).await),
        }
    }

    fn ask<'a>(&self, context: &GatewayRequestContext<'a>, request: &AuthzRequest<'a>) -> Question<'a> {
        self.question(request.operation, request.identity.is_none(), || route_facts(context, request))
    }
}

/// Reads the core's answer as a decision. Only an explicit denial is a `Deny`; a failure to
/// answer is `Indeterminate` and never an allowance.
fn core_decision(answer: S3Result<()>) -> Decision {
    match answer {
        Ok(()) => Decision::Allow,
        Err(error) if error.code() == &S3ErrorCode::AccessDenied => Decision::Deny,
        Err(_) => Decision::Allow,
    }
}

fn route_facts<'a>(context: &GatewayRequestContext<'a>, request: &AuthzRequest<'a>) -> RouteFacts<'a> {
    let host = context.server_extensions();
    RouteFacts {
        access_key: request.identity.map(|identity| identity.access_key_id()),
        bucket: request.bucket.map(|bucket| bucket.as_str()),
        key: request.key.map(|key| key.as_str()),
        version_id: request.version_id,
        headers: context.headers().map(|accepted| {
            let mut headers = HeaderMap::new();
            for (name, value) in accepted.iter_raw() {
                headers.append(name.clone(), value.clone());
            }
            headers
        }),
        request_line: host.get::<RequestLine>(),
        client_info: host.get::<ClientInfo>(),
        remote_addr: host
            .get::<Option<RemoteAddr>>()
            .and_then(|peer| peer.as_ref().map(|peer| peer.0)),
        request_context: host.get::<RequestContext>(),
    }
}

impl Authorizer for RustfsAuthorizer {
    fn authorize_route<'a>(
        &'a self,
        context: &'a GatewayRequestContext<'a>,
        request: &'a AuthzRequest<'a>,
    ) -> BoxFuture<'a, Decision> {
        let question = self.ask(context, request);
        Box::pin(self.answer(question))
    }

    fn authorize_input<'a>(
        &'a self,
        context: &'a GatewayRequestContext<'a>,
        request: &'a InputAuthzRequest<'a>,
    ) -> BoxFuture<'a, InputDecisions> {
        // `decide_all` visits every derived resource and then the visibility question, in that
        // order; each is read from the table now and answered below, where the core is awaited.
        let stage = self.ask(context, request.route());
        let mut derived: Vec<Question<'a>> = request
            .resources()
            .iter()
            .map(|resource| self.ask(context, resource))
            .collect();
        if let Some(visibility) = request.visibility() {
            derived.push(self.ask(context, visibility));
        }
        Box::pin(async move {
            let stage = self.answer(stage).await;
            let mut answers = Vec::with_capacity(derived.len());
            for question in derived {
                answers.push(self.answer(question).await);
            }
            let mut answers = answers.into_iter();
            request.decide_all(stage, |_| answers.next().unwrap_or(Decision::Deny))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    /// The rows documented as `DeferToHandlerHook` in this file's header.
    const DOCUMENTED_DEFERRED_ROWS: &[&str] = &["ListBuckets"];
    /// The rows documented as `LegacyNoCheck` in this file's header.
    const DOCUMENTED_NO_CHECK_ROWS: &[&str] = &[];

    static PLAIN_TABLE: &[AuthorizationRow] = &[
        AuthorizationRow {
            operation: "HeadBucket",
            action: Action::S3Action(S3Action::ListBucketAction),
            resource: ResourceForm::Bucket,
            route_stage: RouteStage::RouteDecidable,
            body_inputs: &[],
            extra_checks: ExtraChecks::None,
        },
        AuthorizationRow {
            operation: "GetObjectTorrent",
            action: Action::S3Action(S3Action::GetObjectAction),
            resource: ResourceForm::Object,
            route_stage: RouteStage::LegacyNoCheck {
                reason: "the legacy hook is empty",
            },
            body_inputs: &[],
            extra_checks: ExtraChecks::None,
        },
    ];

    /// A core that gives one scripted answer and records what it was asked.
    struct ScriptedCore {
        answer: fn() -> S3Result<()>,
        asked: Mutex<Vec<(&'static str, Action, Option<String>)>>,
    }

    impl ScriptedCore {
        fn answering(answer: fn() -> S3Result<()>) -> Arc<Self> {
            Arc::new(Self {
                answer,
                asked: Mutex::new(Vec::new()),
            })
        }
    }

    impl DecisionCore for ScriptedCore {
        fn decide<'a>(&'a self, row: &'a AuthorizationRow, facts: &'a RouteFacts<'a>) -> BoxFuture<'a, S3Result<()>> {
            self.asked
                .lock()
                .expect("core lock")
                .push((row.operation, row.action, facts.access_key.map(str::to_owned)));
            let answer = (self.answer)();
            Box::pin(async move { answer })
        }
    }

    fn denied() -> S3Result<()> {
        Err(S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied"))
    }

    fn unavailable() -> S3Result<()> {
        Err(S3Error::with_message(S3ErrorCode::InternalError, "the policy store is unavailable"))
    }

    /// One request as a test asks about it.
    struct Asked<'a> {
        operation: &'a str,
        facts: RouteFacts<'a>,
    }

    fn facts<'a>(operation: &'a str, access_key: Option<&'a str>) -> Asked<'a> {
        Asked {
            operation,
            facts: RouteFacts {
                access_key,
                bucket: None,
                key: None,
                version_id: None,
                headers: None,
                request_line: None,
                client_info: None,
                remote_addr: None,
                request_context: None,
            },
        }
    }

    async fn decision(table: &'static [AuthorizationRow], core: Arc<dyn DecisionCore>, asked: Asked<'_>) -> Decision {
        let authorizer = RustfsAuthorizer::new(table, core);
        let anonymous = asked.facts.access_key.is_none();
        let question = authorizer.question(asked.operation, anonymous, || asked.facts);
        authorizer.answer(question).await
    }

    /// The real decision core of a server that has no object store yet.
    fn owned_core() -> Arc<dyn DecisionCore> {
        Arc::new(OwnedDecisionCore::new(ServerContextSlot::new()))
    }

    fn head_bucket() -> RequestLine {
        RequestLine {
            method: Method::HEAD,
            uri: Uri::from_static("/bucket"),
        }
    }

    fn rows_where(predicate: fn(&RouteStage) -> bool) -> BTreeSet<&'static str> {
        AUTHORIZATION_TABLE
            .iter()
            .filter(|row| predicate(&row.route_stage))
            .map(|row| row.operation)
            .collect()
    }

    #[tokio::test]
    async fn an_operation_without_a_row_is_denied() {
        let core = ScriptedCore::answering(|| Ok(()));

        for operation in ["GetObject", "PutObject", "", "listbuckets"] {
            let decided = decision(AUTHORIZATION_TABLE, core.clone(), facts(operation, Some("AKIDUSER0001"))).await;
            assert_eq!(decided, Decision::Deny, "{operation:?}");
        }
        assert!(core.asked.lock().expect("core lock").is_empty(), "an unmapped operation asks nothing");
    }

    #[tokio::test]
    async fn a_core_that_cannot_answer_is_indeterminate_never_an_allowance() {
        let decided = decision(
            PLAIN_TABLE,
            ScriptedCore::answering(unavailable),
            facts("HeadBucket", Some("AKIDUSER0001")),
        )
        .await;

        assert_eq!(decided, Decision::Indeterminate);
    }

    #[tokio::test]
    async fn the_owned_core_without_a_store_cannot_answer_and_never_allows() {
        let line = head_bucket();
        let mut asked = facts("HeadBucket", None);
        asked.facts.bucket = Some("bucket");
        asked.facts.headers = Some(HeaderMap::new());
        asked.facts.request_line = Some(&line);

        assert_eq!(decision(PLAIN_TABLE, owned_core(), asked).await, Decision::Indeterminate);
    }

    #[tokio::test]
    async fn the_owned_core_refuses_a_request_the_host_did_not_describe() {
        let line = head_bucket();

        let mut without_a_line = facts("HeadBucket", None);
        without_a_line.facts.bucket = Some("bucket");
        without_a_line.facts.headers = Some(HeaderMap::new());
        assert_eq!(decision(PLAIN_TABLE, owned_core(), without_a_line).await, Decision::Indeterminate);

        let mut without_headers = facts("HeadBucket", None);
        without_headers.facts.bucket = Some("bucket");
        without_headers.facts.request_line = Some(&line);
        assert_eq!(decision(PLAIN_TABLE, owned_core(), without_headers).await, Decision::Indeterminate);
    }

    #[tokio::test]
    async fn the_owned_core_refuses_a_bucket_row_whose_request_names_no_bucket() {
        let line = head_bucket();
        let mut asked = facts("HeadBucket", None);
        asked.facts.headers = Some(HeaderMap::new());
        asked.facts.request_line = Some(&line);

        assert_eq!(decision(PLAIN_TABLE, owned_core(), asked).await, Decision::Indeterminate);
    }

    #[tokio::test]
    async fn a_core_denial_is_a_denial() {
        let decided = decision(PLAIN_TABLE, ScriptedCore::answering(denied), facts("HeadBucket", Some("AKIDUSER0001"))).await;

        assert_eq!(decided, Decision::Deny);
    }

    #[tokio::test]
    async fn a_route_decidable_row_asks_the_core_its_legacy_action_for_the_verified_caller() {
        let core = ScriptedCore::answering(|| Ok(()));

        let decided = decision(PLAIN_TABLE, core.clone(), facts("HeadBucket", Some("AKIDUSER0001"))).await;

        assert_eq!(decided, Decision::Allow);
        assert_eq!(
            *core.asked.lock().expect("core lock"),
            [(
                "HeadBucket",
                Action::S3Action(S3Action::ListBucketAction),
                Some("AKIDUSER0001".to_owned())
            )]
        );
    }

    #[tokio::test]
    async fn a_deferred_row_refuses_an_anonymous_caller_without_asking_the_core() {
        let core = ScriptedCore::answering(|| Ok(()));

        assert_eq!(
            decision(AUTHORIZATION_TABLE, core.clone(), facts("ListBuckets", None)).await,
            Decision::Deny
        );
        assert_eq!(
            decision(AUTHORIZATION_TABLE, core.clone(), facts("ListBuckets", Some("AKIDUSER0001"))).await,
            Decision::Allow,
            "a verified caller passes to the hook"
        );
        assert!(
            core.asked.lock().expect("core lock").is_empty(),
            "a deferred row decides nothing through the core"
        );
    }

    #[tokio::test]
    async fn a_no_check_row_is_allowed_only_because_its_row_says_so() {
        let core = ScriptedCore::answering(denied);

        assert_eq!(
            decision(PLAIN_TABLE, core.clone(), facts("GetObjectTorrent", None)).await,
            Decision::Allow
        );
        assert_eq!(decision(AUTHORIZATION_TABLE, core, facts("GetObjectTorrent", None)).await, Decision::Deny);
    }

    #[test]
    fn the_deferred_rows_are_exactly_the_documented_ones() {
        let deferred = rows_where(|stage| matches!(stage, RouteStage::DeferToHandlerHook { .. }));

        assert_eq!(deferred, DOCUMENTED_DEFERRED_ROWS.iter().copied().collect::<BTreeSet<_>>());
    }

    #[test]
    fn the_no_check_rows_are_exactly_the_documented_ones() {
        let unchecked = rows_where(|stage| matches!(stage, RouteStage::LegacyNoCheck { .. }));

        assert_eq!(unchecked, DOCUMENTED_NO_CHECK_ROWS.iter().copied().collect::<BTreeSet<_>>());
    }

    #[test]
    fn every_row_states_every_column_and_names_one_operation() {
        let mut seen = BTreeSet::new();
        for row in AUTHORIZATION_TABLE {
            assert!(seen.insert(row.operation), "{} has two rows", row.operation);
            assert!(!row.operation.is_empty());
            match row.route_stage {
                RouteStage::DeferToHandlerHook { reason, .. } | RouteStage::LegacyNoCheck { reason } => {
                    assert!(!reason.is_empty(), "{} defers without a reason", row.operation);
                }
                RouteStage::RouteDecidable => {}
            }
            match row.extra_checks {
                ExtraChecks::Performed(checks) => {
                    assert!(!checks.is_empty(), "{} lists no extra check; say `ExtraChecks::None`", row.operation);
                    assert!(checks.iter().all(|check| !check.is_empty()), "{}", row.operation);
                }
                ExtraChecks::None => {}
            }
            assert!(row.body_inputs.iter().all(|input| !input.is_empty()), "{}", row.operation);
        }
    }

    #[test]
    fn list_buckets_asks_the_legacy_action_on_the_service() {
        let row = AUTHORIZATION_TABLE
            .iter()
            .find(|row| row.operation == "ListBuckets")
            .expect("ListBuckets is registered");

        assert_eq!(row.action, Action::S3Action(S3Action::ListAllMyBucketsAction));
        assert_eq!(row.resource, ResourceForm::Service);
        assert_eq!(
            row.route_stage,
            RouteStage::DeferToHandlerHook {
                gate: RouteGate::AuthenticatedCaller,
                reason: "the use case decides per caller from the bucket inventory",
            }
        );
        assert!(row.body_inputs.is_empty());
        assert!(matches!(row.extra_checks, ExtraChecks::Performed(_)));
    }
}
