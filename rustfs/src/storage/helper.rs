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

use crate::module_switches::{is_audit_module_enabled, is_notify_module_enabled};
use crate::shared_types::convert_ecstore_object_info;
use crate::storage::req_info::ReqInfo;
use crate::storage::request_context::RequestContext;
use crate::storage::sse::KmsRequestAuditScope;
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use hashbrown::HashMap;
use http::{HeaderMap, StatusCode, Uri};
use metrics::counter;
use rustfs_audit::{
    ApiDetails, AuditEntry, ObjectVersion,
    entity::{ApiDetailsBuilder, AuditEntryBuilder},
    global::AuditLogger,
};
use rustfs_iam::sys::{OIDC_ISSUER_CLAIM, is_rustfs_oidc_claims};
use rustfs_notify::EventArgsBuilder;
use rustfs_policy::policy::action::Action;
use rustfs_s3_ops::{S3Operation, operation_matches_event_name};
use rustfs_s3_types::EventName;
use rustfs_targets::{extract_params_header, get_request_host, get_request_port, get_request_user_agent};
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::http::headers::AMZ_REQUEST_ID;
use serde_json::Value;
use std::future::Future;
use std::net::SocketAddr;
use tokio::runtime::{Builder, Handle};
use tracing::{Instrument, info_span, warn};

/// The decoded request fields used to build an audit entry.
pub(crate) struct AuditRequestView<'a> {
    bucket_generation: Option<&'a super::authorization::BucketGenerationGuard>,
    headers: &'a HeaderMap,
    uri: &'a Uri,
    pub(crate) req_info: Option<&'a ReqInfo>,
    client_info: Option<&'a ClientInfo>,
    remote_addr: Option<SocketAddr>,
    request_context: Option<&'a RequestContext>,
}

impl<'a> AuditRequestView<'a> {
    /// The view of a request whose facts an owned edge already decoded.
    pub(crate) fn from_facts(
        headers: &'a HeaderMap,
        uri: &'a Uri,
        req_info: Option<&'a ReqInfo>,
        client_info: Option<&'a ClientInfo>,
        remote_addr: Option<SocketAddr>,
        request_context: Option<&'a RequestContext>,
        bucket_generation: Option<&'a super::authorization::BucketGenerationGuard>,
    ) -> Self {
        Self {
            bucket_generation,
            headers,
            uri,
            req_info,
            client_info,
            remote_addr,
            request_context,
        }
    }

    pub(crate) fn request_context(&self) -> Option<RequestContext> {
        self.request_context
            .cloned()
            .or_else(|| self.req_info.and_then(|info| info.request_context.clone()))
    }
}

#[cfg(feature = "diagnostic-read-version-caller")]
use super::storage_api::ecstore_diagnostic;
use crate::storage::storage_api::helper_consumer::StorageObjectInfo as ObjectInfo;

/// Schedules an asynchronous task on the current runtime;
/// if there is no runtime, creates a minimal runtime execution on a new thread.
pub(crate) fn spawn_background<F>(fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    // Capture before handing work to either runtime; request task-locals are not inherited.
    #[cfg(feature = "diagnostic-read-version-caller")]
    let fut = ecstore_diagnostic::with_captured_read_version_context(
        ecstore_diagnostic::ReadVersionContextBoundary::BackgroundTask,
        fut,
    );
    #[cfg(not(feature = "diagnostic-read-version-caller"))]
    let fut = rustfs_io_metrics::s3_http_metrics::with_s3_http_operation_context(
        rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_context(),
        fut,
    );
    if let Ok(handle) = Handle::try_current() {
        drop(handle.spawn(fut));
    } else {
        std::thread::spawn(|| {
            if let Ok(rt) = Builder::new_current_thread().enable_all().build() {
                rt.block_on(fut);
            }
        });
    }
}

/// Spawn a background task with request context correlation.
/// Creates a child span with the request_id for tracing continuity,
/// ensuring audit/notify tasks can be traced back to the original request.
pub(crate) fn spawn_background_with_context<F>(request_context: Option<RequestContext>, fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    match request_context {
        Some(ctx) => {
            let request_id = ctx.request_id;
            let span = info_span!("background-task", request_id = %request_id);
            spawn_background(Instrument::instrument(fut, span));
        }
        None => spawn_background(fut),
    }
}

/// Own a parsed mutation through its storage and event/audit completion.
/// Dropping the HTTP waiter does not cancel the operation. Streaming bodies
/// must use their bounded upload owner instead of this entry point. Generic
/// over the result, so a legacy and an owned use case share it.
pub(crate) async fn run_owned_mutation<R, E, F>(operation: F) -> Result<R, E>
where
    R: Send + 'static,
    E: From<rustfs_s3_types::S3Error> + Send + 'static,
    F: Future<Output = Result<R, E>> + Send + 'static,
{
    let credentials = rustfs_keystone::KEYSTONE_CREDENTIALS.try_with(Clone::clone).ok().flatten();
    let operation = rustfs_keystone::KEYSTONE_CREDENTIALS.scope(credentials, operation);
    #[cfg(feature = "diagnostic-read-version-caller")]
    let operation = ecstore_diagnostic::with_captured_read_version_context(
        ecstore_diagnostic::ReadVersionContextBoundary::BackgroundTask,
        operation,
    );
    #[cfg(not(feature = "diagnostic-read-version-caller"))]
    let operation = rustfs_io_metrics::s3_http_metrics::with_s3_http_operation_context(
        rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_context(),
        operation,
    );
    super::request_context::spawn_traced_join(operation)
        .await
        .map_err(|_| E::from(rustfs_s3_types::s3_error!(InternalError, "object mutation owner task failed")))?
}

/// Builds S3-compatible notification response elements with the canonical request ID.
pub(crate) fn build_owned_event_resp_elements(response_headers: &HeaderMap, request_id: &str) -> HashMap<String, String> {
    let mut resp_elements = extract_params_header(response_headers);
    resp_elements.insert(AMZ_REQUEST_ID.to_string(), request_id.to_string());
    resp_elements
}

/// What an operation answered, as its audit entry and notification event record it.
#[derive(Clone, Copy)]
enum OperationOutcome<'a> {
    /// `headers` are the response headers the operation adds beyond its output.
    Success {
        status: StatusCode,
        headers: &'a HeaderMap,
    },
    Failure {
        status: StatusCode,
        message: Option<&'a str>,
    },
}

impl<'a> OperationOutcome<'a> {
    /// An owned error without a message is recorded under its code name, so every
    /// failure entry carries `error` (the legacy s3s error stores a default message).
    fn owned<T>(result: &'a rustfs_s3_types::S3Result<T>, status: StatusCode, headers: &'a HeaderMap) -> Self {
        match result {
            Ok(_) => Self::Success { status, headers },
            Err(err) => Self::failure(err.status_code(), Some(err.message().unwrap_or_else(|| err.code().as_str()))),
        }
    }

    /// An error without a status is audited as `400`.
    fn failure(status: Option<StatusCode>, message: Option<&'a str>) -> Self {
        Self::Failure {
            status: status.unwrap_or(StatusCode::BAD_REQUEST),
            message,
        }
    }
}

/// Builds the audit entry shared by every request-scoped audit source: caller identity,
/// client address and the request line, without any outcome.
fn request_audit_builder(
    req: &AuditRequestView<'_>,
    event: EventName,
    trigger: &str,
    api: ApiDetails,
    request_id: &str,
) -> AuditEntryBuilder {
    let remote_host = req
        .client_info
        .map(|info| info.real_ip.to_string())
        .or_else(|| req.remote_addr.map(|addr| addr.ip().to_string()))
        .unwrap_or_default();
    let mut builder = AuditEntryBuilder::new("1.0", event, trigger, api)
        .remote_host(remote_host)
        .user_agent(get_request_user_agent(req.headers))
        .req_host(get_request_host(req.headers))
        .req_path(req.uri.path().to_string())
        .req_query(extract_params_header(req.headers))
        .request_id(request_id);
    if let Some(cred) = req
        .req_info
        .and_then(|info| info.cred.as_ref())
        .filter(|cred| !cred.access_key.is_empty())
    {
        builder = builder.access_key(&cred.access_key);
        if !cred.parent_user.is_empty() {
            builder = builder.parent_user(&cred.parent_user);
        }
        if let Some(claims) = cred.claims.as_ref().filter(|claims| is_rustfs_oidc_claims(claims)) {
            let identity_claims = ["sub", "iss", OIDC_ISSUER_CLAIM, "oidc_provider"]
                .into_iter()
                .filter_map(|key| {
                    claims
                        .get(key)
                        .filter(|value| value.as_str().is_some_and(|value| !value.is_empty()))
                        .map(|value| (key.to_string(), value.clone()))
                })
                .collect();
            builder = builder.req_claims(identity_claims);
        }
    }
    builder
}

#[cfg(test)]
type MutationCompletionPause = (std::sync::Arc<tokio::sync::Barrier>, std::sync::Arc<tokio::sync::Barrier>);
#[cfg(test)]
type MutationCompletionProbes = HashMap<String, std::sync::Weak<MutationCompletionProbe>>;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct MutationCompletionProbe {
    pub(crate) audits: std::sync::Mutex<Vec<AuditEntry>>,
    pub(crate) events: std::sync::Mutex<Vec<rustfs_notify::EventArgs>>,
    changed: tokio::sync::Notify,
    pause: std::sync::Mutex<Option<MutationCompletionPause>>,
}

#[cfg(test)]
fn completion_probes() -> &'static std::sync::Mutex<MutationCompletionProbes> {
    static PROBES: std::sync::OnceLock<std::sync::Mutex<MutationCompletionProbes>> = std::sync::OnceLock::new();
    PROBES.get_or_init(Default::default)
}

#[cfg(test)]
fn completion_probe(bucket: &str) -> Option<std::sync::Arc<MutationCompletionProbe>> {
    completion_probes()
        .lock()
        .expect("completion probes")
        .get(bucket)
        .and_then(std::sync::Weak::upgrade)
}

#[cfg(test)]
impl MutationCompletionProbe {
    pub(crate) fn install(bucket: &str) -> std::sync::Arc<Self> {
        let probe = std::sync::Arc::new(Self::default());
        let mut probes = completion_probes().lock().expect("completion probes");
        probes.retain(|_, value| value.strong_count() > 0);
        probes.insert(bucket.to_owned(), std::sync::Arc::downgrade(&probe));
        probe
    }

    pub(crate) fn pause_after_commit(&self) -> MutationCompletionPause {
        let entered = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let resume = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        *self.pause.lock().expect("completion pause") = Some((entered.clone(), resume.clone()));
        (entered, resume)
    }

    pub(crate) async fn wait_for(&self, audits: usize, events: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let changed = self.changed.notified();
                if self.audits.lock().expect("audits").len() >= audits && self.events.lock().expect("events").len() >= events {
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("committed mutation must dispatch its audit and events");
    }
}

#[cfg(test)]
pub(crate) async fn pause_committed_mutation_for_test(bucket: &str) {
    let pause = completion_probe(bucket).and_then(|probe| probe.pause.lock().expect("completion pause").take());
    if let Some((entered, resume)) = pause {
        entered.wait().await;
        resume.wait().await;
    }
}

#[cfg(test)]
pub(crate) fn capture_mutation_event_for_test(event: &rustfs_notify::EventArgs) {
    if let Some(probe) = completion_probe(&event.bucket_name) {
        probe.events.lock().expect("events").push(event.clone());
        probe.changed.notify_one();
    }
}

/// A unified helper structure for building and distributing audit logs and event notifications via RAII mode at the end of an S3 operation scope.
pub enum OperationHelper {
    Disabled,
    Enabled(Box<EnabledOperationHelper>),
}

pub struct EnabledOperationHelper {
    audit_enabled: bool,
    notify_enabled: bool,
    audit_builder: Option<AuditEntryBuilder>,
    api_builder: ApiDetailsBuilder,
    event_builder: Option<EventArgsBuilder>,
    start_time: std::time::Instant,
    request_context: RequestContext,
    /// Open for the lifetime of the audit entry so the SSE data path can summarise
    /// its KMS work onto it. `None` when this request is not audited.
    kms_audit: Option<KmsRequestAuditScope>,
}

impl OperationHelper {
    /// Builds the helper from the audit facts decoded by the operation edge.
    /// The edge counts the operation before calling the use case.
    pub(crate) fn from_view(req: &AuditRequestView<'_>, event: EventName, op: S3Operation) -> Self {
        Self::build(req, event, op, false)
    }

    /// Starts an audit entry without building a notification event.
    pub(crate) fn audit_only_from_view(req: &AuditRequestView<'_>, op: S3Operation) -> Self {
        let event = op.to_event_name().unwrap_or(EventName::AuditS3Operation);
        Self::build(req, event, op, true)
    }

    /// Records an application operation using its server-selected API name.
    pub(crate) fn audit_only_named_from_view(req: &AuditRequestView<'_>, operation: &'static str) -> Self {
        Self::build_named(req, EventName::AuditS3Operation, operation, true)
    }

    fn build(req: &AuditRequestView<'_>, event: EventName, op: S3Operation, audit_only: bool) -> Self {
        // `audit_only` derives the event from the operation, so there is nothing to cross-check.
        let op_event_matches = audit_only || operation_matches_event_name(op, event);
        debug_assert!(op_event_matches, "operation/event mismatch: op={} event={}", op.as_str(), event.as_str());
        if !op_event_matches {
            counter!(
                "rustfs_log_chain_op_event_mismatch_total",
                "op" => op.as_str(),
                "event" => event.as_str().to_string()
            )
            .increment(1);
            warn!(
                op = op.as_str(),
                event = event.as_str(),
                "operation/event mismatch detected; check S3 semantic mapping"
            );
        }

        Self::build_named(req, event, op.as_str(), audit_only)
    }

    fn build_named(req: &AuditRequestView<'_>, event: EventName, trigger: &'static str, audit_only: bool) -> Self {
        #[cfg(test)]
        let probe_enabled = completion_probe(
            req.req_info
                .and_then(|info| info.bucket.as_deref())
                .unwrap_or_else(|| req.uri.path().trim_start_matches('/').split('/').next().unwrap_or_default()),
        )
        .is_some();
        let audit_enabled = is_audit_module_enabled();
        let notify_enabled = !audit_only && should_build_notification_event(is_notify_module_enabled());
        #[cfg(test)]
        let audit_enabled = audit_enabled || probe_enabled;
        #[cfg(test)]
        let notify_enabled = notify_enabled || (!audit_only && probe_enabled);
        if audit_only && !audit_enabled {
            // Nothing to record and no metric to bump: skip the path parsing below.
            return Self::Disabled;
        }

        let path = req.uri.path().trim_start_matches('/');
        let mut segs = path.splitn(2, '/');
        let path_bucket = segs.next().unwrap_or("").to_string();
        let path_object_key = segs.next().unwrap_or("").to_string();
        let req_info = req.req_info;
        let bucket = req_info
            .and_then(|info| info.bucket.clone())
            .filter(|value| !value.is_empty())
            .unwrap_or(path_bucket);

        // Fast path: when both chains are disabled, avoid audit/notify builder work.
        if !audit_enabled && !notify_enabled {
            return Self::Disabled;
        }

        if audit_enabled {
            counter!("rustfs_log_chain_audit_total").increment(1);
        }
        // Parse path -> bucket/object
        let object_key = req_info
            .and_then(|info| info.object.clone())
            .filter(|value| !value.is_empty())
            .unwrap_or(path_object_key);

        // Initialize audit builder
        let mut api_builder = ApiDetailsBuilder::new().name(trigger);
        if !bucket.is_empty() {
            api_builder = api_builder.bucket(&bucket);
        }
        if !object_key.is_empty() {
            api_builder = api_builder.object(&object_key);
        }
        // Audit builder
        // Resolve the canonical request context once for both output chains.
        let request_context = req.request_context();
        if request_context.is_none() {
            counter!("rustfs_log_chain_orphan_total", "component" => "operation_helper").increment(1);
        }
        let request_context = request_context.unwrap_or_else(|| RequestContext::from_external_headers(req.headers));
        let request_id = request_context.request_id.clone();

        let audit_builder =
            audit_enabled.then(|| request_audit_builder(req, event, trigger, api_builder.clone().build(), &request_id));

        let event_object = ObjectInfo {
            bucket: bucket.clone(),
            name: object_key,
            ..Default::default()
        };

        let mut req_params = extract_params_header(req.headers);
        if let Some(principal_id) = req_info
            .and_then(|info| info.cred.as_ref())
            .map(|cred| cred.access_key.clone())
            .filter(|value| !value.is_empty())
        {
            req_params.entry("principalId".to_string()).or_insert(principal_id);
        }

        // initialize event builder
        // object is a placeholder that must be set later using the `object()` method.
        let event_builder = if notify_enabled {
            let incarnation_id = req
                .bucket_generation
                .filter(|guard| guard.bucket == bucket)
                .map(|guard| guard.incarnation_id);
            let mut event_builder = EventArgsBuilder::new(event, bucket, convert_ecstore_object_info(event_object))
                .bucket_incarnation_id(incarnation_id)
                .host(get_request_host(req.headers))
                .port(get_request_port(req.headers))
                .user_agent(get_request_user_agent(req.headers))
                .req_params(req_params);
            if let Some(version_id) = req_info
                .and_then(|info| info.version_id.clone())
                .filter(|value| !value.is_empty())
            {
                event_builder = event_builder.version_id(version_id);
            }
            Some(event_builder)
        } else {
            None
        };

        let kms_audit = audit_enabled.then(|| KmsRequestAuditScope::register(&request_id));

        Self::Enabled(Box::new(EnabledOperationHelper {
            audit_enabled,
            notify_enabled,
            audit_builder,
            api_builder,
            event_builder,
            start_time: request_context.start_time,
            request_context,
            kms_audit,
        }))
    }

    /// True when a pending event notification still needs the final ObjectInfo.
    /// Callers can use this to avoid cloning ObjectInfo when the event chain is
    /// disabled or suppressed.
    pub fn wants_object_info(&self) -> bool {
        matches!(self, Self::Enabled(state) if state.event_builder.is_some())
    }

    /// True when the audit entry can include object details.
    pub fn wants_audit_object_info(&self) -> bool {
        matches!(self, Self::Enabled(state) if state.audit_builder.is_some())
    }

    #[cfg(test)]
    pub(crate) fn event_args(&self) -> Option<rustfs_notify::EventArgs> {
        match self {
            Self::Enabled(state) => state.event_builder.clone().map(|builder| builder.build()),
            Self::Disabled => None,
        }
    }

    pub(crate) fn with_bucket_incarnation_id(mut self, incarnation: Option<uuid::Uuid>) -> Self {
        if let Self::Enabled(helper) = &mut self
            && let Some(builder) = helper.event_builder.take()
        {
            helper.event_builder = Some(builder.bucket_incarnation_id(incarnation));
        }
        self
    }

    pub(crate) fn with_bucket_removed_targets(mut self, targets: rustfs_notify::rules::TargetIdSet) -> Self {
        if let Self::Enabled(state) = &mut self {
            state.event_builder = state
                .event_builder
                .take()
                .map(|builder| builder.bucket_removed_targets(targets));
        }
        self
    }

    /// Sets the ObjectInfo for event notification.
    pub fn object(mut self, object_info: ObjectInfo) -> Self {
        if let Self::Enabled(state) = &mut self
            && let Some(builder) = state.event_builder.take()
        {
            state.event_builder = Some(builder.object(convert_ecstore_object_info(object_info)));
        }
        self
    }

    /// Sets nonempty object details on the audit entry.
    pub fn audit_objects(mut self, objects: Vec<ObjectVersion>) -> Self {
        if !objects.is_empty()
            && let Self::Enabled(state) = &mut self
            && state.audit_builder.is_some()
        {
            state.api_builder = state.api_builder.clone().objects(objects);
        }
        self
    }

    /// Set the version ID for event notifications.
    pub fn version_id(mut self, version_id: impl Into<String>) -> Self {
        if let Self::Enabled(state) = &mut self
            && let Some(builder) = state.event_builder.take()
        {
            state.event_builder = Some(builder.version_id(version_id));
        }
        self
    }

    /// Set the event name for event notifications.
    pub fn event_name(mut self, event_name: EventName) -> Self {
        if let Self::Enabled(state) = &mut self {
            if let Some(builder) = state.event_builder.take() {
                state.event_builder = Some(builder.event_name(event_name));
            }

            if let Some(builder) = state.audit_builder.take() {
                state.audit_builder = Some(builder.event(event_name));
            }
        }

        self
    }

    /// Records the operation result with its success status and response headers.
    pub(crate) fn complete_owned<T>(
        self,
        result: &rustfs_s3_types::S3Result<T>,
        status: StatusCode,
        headers: &HeaderMap,
    ) -> Self {
        self.complete_with(OperationOutcome::owned(result, status, headers))
    }

    fn complete_with(mut self, outcome: OperationOutcome<'_>) -> Self {
        let Self::Enabled(state) = &mut self else {
            return self;
        };

        let (status, status_code, error_msg) = match outcome {
            OperationOutcome::Success { status, .. } => ("success".to_string(), status.as_u16() as i32, None),
            OperationOutcome::Failure { status, message } => {
                ("failure".to_string(), status.as_u16() as i32, message.map(str::to_string))
            }
        };
        state.api_builder = state.api_builder.clone().status(status.clone()).status_code(status_code);

        // Complete audit log
        if state.audit_enabled
            && let Some(builder) = state.audit_builder.take()
        {
            let ttr = state.start_time.elapsed();
            let api_details = state
                .api_builder
                .clone()
                .status(status)
                .status_code(status_code)
                .time_to_response(format!("{ttr:.2?}"))
                .time_to_response_in_ns(ttr.as_nanos().to_string())
                .build();

            let mut final_builder = builder.api(api_details.clone());
            if let OperationOutcome::Success { headers, .. } = outcome {
                final_builder = final_builder.resp_header(extract_params_header(headers));
            }
            if let Some(err) = error_msg {
                final_builder = final_builder.error(err);
            }

            // Inject OpenTelemetry trace context into audit tags for distributed tracing correlation
            let mut tags = HashMap::new();
            if let Some(ref tid) = state.request_context.trace_id {
                tags.insert("traceId".to_string(), Value::String(tid.clone()));
            }
            if let Some(ref sid) = state.request_context.span_id {
                tags.insert("spanId".to_string(), Value::String(sid.clone()));
            }
            // Summarise the KMS work the SSE data path did for this request, so the
            // KMS outcome inherits this entry's principal and request ID instead of
            // needing an event of its own.
            if let Some(scope) = state.kms_audit.as_ref() {
                tags.extend(scope.audit_tags().into_iter().map(|(key, value)| (key.to_string(), value)));
            }
            if !tags.is_empty() {
                final_builder = final_builder.tags(tags);
            }

            state.audit_builder = Some(final_builder);
            state.api_builder = ApiDetailsBuilder(api_details); // Store final details for Drop use
        }

        // Completion event notification (only on success)
        if state.notify_enabled
            && let (Some(builder), OperationOutcome::Success { headers, .. }) = (state.event_builder.take(), outcome)
        {
            state.event_builder =
                Some(builder.resp_elements(build_owned_event_resp_elements(headers, &state.request_context.request_id)));
        }

        self
    }

    /// Suppresses the automatic event notification on drop.
    pub fn suppress_event(mut self) -> Self {
        if let Self::Enabled(state) = &mut self {
            state.event_builder = None;
        }
        self
    }

    /// Returns the operation's canonical context, reading the ingress extension
    /// when the disabled fast path holds no request state.
    pub(crate) fn request_context_or_from_view(&self, req: &AuditRequestView<'_>) -> RequestContext {
        match self {
            Self::Enabled(state) => state.request_context.clone(),
            Self::Disabled => req
                .request_context()
                .unwrap_or_else(|| RequestContext::from_external_headers(req.headers)),
        }
    }
}

/// What an authorization denial entry describes.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DenialScope {
    /// One refused request for the request's own bucket and object.
    Request,
    /// A bulk request (DeleteObjects) in which this many keys were refused; no single object applies.
    Keys(u64),
}

pub(crate) fn authorization_denial_entry_for_view(
    req: &AuditRequestView<'_>,
    action: &Action,
    reason: &'static str,
    scope: DenialScope,
) -> AuditEntry {
    let req_info = req.req_info;
    let path = req.uri.path().trim_start_matches('/');
    let mut segs = path.splitn(2, '/');
    let path_bucket = segs.next().unwrap_or("");
    let path_object = segs.next().unwrap_or("");
    let bucket = req_info
        .and_then(|info| info.bucket.as_deref())
        .filter(|value| !value.is_empty())
        .unwrap_or(path_bucket);
    let object = match scope {
        DenialScope::Request => req_info
            .and_then(|info| info.object.as_deref())
            .filter(|value| !value.is_empty())
            .unwrap_or(path_object),
        DenialScope::Keys(_) => "",
    };

    let action_name: &str = action.into();
    let mut api = ApiDetailsBuilder::new()
        .name(action_name)
        .status("failure")
        .status_code(StatusCode::FORBIDDEN.as_u16() as i32);
    if !bucket.is_empty() {
        api = api.bucket(bucket);
    }
    if !object.is_empty() {
        api = api.object(object);
    }

    let request_context = req
        .request_context()
        .unwrap_or_else(|| RequestContext::from_external_headers(req.headers));
    let mut tags = HashMap::new();
    tags.insert("denialReason".to_string(), Value::String(reason.to_string()));
    if let DenialScope::Keys(denied) = scope {
        tags.insert("deniedKeys".to_string(), Value::from(denied));
    }
    request_audit_builder(req, EventName::AuditS3Operation, action_name, api.build(), &request_context.request_id)
        .error("Access Denied")
        .tags(tags)
        .build()
}

fn should_build_notification_event(notify_module_enabled: bool) -> bool {
    notify_module_enabled || rustfs_notify::notification_system().is_some_and(|system| system.has_live_listeners())
}

impl Drop for OperationHelper {
    fn drop(&mut self) {
        let Self::Enabled(state) = self else {
            return;
        };

        // An early error or panic must have an outcome, but is never proof
        // of a successful mutation. Committed owners complete explicitly.
        if state.api_builder.0.status.is_none() {
            let elapsed = state.start_time.elapsed();
            state.api_builder = state
                .api_builder
                .clone()
                .status("failure".to_string())
                .status_code(StatusCode::INTERNAL_SERVER_ERROR.as_u16() as i32)
                .time_to_response(format!("{elapsed:.2?}"))
                .time_to_response_in_ns(elapsed.as_nanos().to_string());
            if let Some(builder) = state.audit_builder.take() {
                state.audit_builder = Some(
                    builder
                        .api(state.api_builder.clone().build())
                        .error("Operation ended before reporting its outcome".to_string()),
                );
            }
        }

        // Distribute audit logs
        if state.audit_enabled
            && let Some(builder) = state.audit_builder.take()
        {
            let entry = builder.build();
            #[cfg(test)]
            if let Some(probe) = entry.api.bucket.as_deref().and_then(completion_probe) {
                probe.audits.lock().expect("audits").push(entry.clone());
                probe.changed.notify_one();
            }
            let ctx = Some(state.request_context.clone());
            spawn_background_with_context(ctx, async move {
                AuditLogger::log(entry).await;
            });
        }

        // Distribute event notification (only on success)
        if state.notify_enabled
            && state.api_builder.0.status.as_deref() == Some("success")
            && let Some(builder) = state.event_builder.take()
        {
            let event_args = builder.build();
            // Avoid generating notifications for copy requests
            if !event_args.is_replication_request() {
                #[cfg(test)]
                capture_mutation_event_for_test(&event_args);
                let ctx = Some(state.request_context.clone());
                spawn_background_with_context(ctx, async move {
                    runtime_sources::current_notify_interface().notify(event_args).await;
                });
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_imports)]
#[path = "helper_test.rs"]
mod tests;
