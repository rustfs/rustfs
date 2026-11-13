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
use crate::shared_types::{RemoteAddr, convert_ecstore_object_info};
use crate::storage::access::ReqInfo;
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
use rustfs_io_metrics::record_s3_op;
use rustfs_notify::EventArgsBuilder;
use rustfs_policy::policy::action::Action;
use rustfs_s3_ops::{S3Operation, operation_matches_event_name};
use rustfs_s3_types::EventName;
use rustfs_targets::{extract_params_header, get_request_host, get_request_port, get_request_user_agent};
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::http::headers::AMZ_REQUEST_ID;
use s3s::{S3Request, S3Response, S3Result};
use serde_json::Value;
use std::future::Future;
use std::net::SocketAddr;
use tokio::runtime::{Builder, Handle};

/// Request type seen by the denial auditor, so `denial_audit` stays off the direct s3s surface.
pub(crate) type DenialRequest<T> = S3Request<T>;
use tracing::{Instrument, info_span, warn};

/// The request fields used to build an audit entry, shared by both request edges.
pub(crate) struct AuditRequestView<'a> {
    headers: &'a HeaderMap,
    uri: &'a Uri,
    pub(crate) req_info: Option<&'a ReqInfo>,
    client_info: Option<&'a ClientInfo>,
    remote_addr: Option<SocketAddr>,
    request_context: Option<&'a RequestContext>,
}

impl<'a> AuditRequestView<'a> {
    pub(crate) fn legacy<T>(req: &'a S3Request<T>) -> Self {
        Self {
            headers: &req.headers,
            uri: &req.uri,
            req_info: req.extensions.get::<ReqInfo>(),
            client_info: req.extensions.get::<ClientInfo>(),
            remote_addr: req
                .extensions
                .get::<Option<RemoteAddr>>()
                .and_then(|addr| addr.map(|addr| addr.0)),
            request_context: req.extensions.get::<RequestContext>(),
        }
    }

    /// The view of a request whose facts an owned edge already decoded.
    pub(crate) fn from_facts(
        headers: &'a HeaderMap,
        uri: &'a Uri,
        req_info: Option<&'a ReqInfo>,
        client_info: Option<&'a ClientInfo>,
        remote_addr: Option<SocketAddr>,
        request_context: Option<&'a RequestContext>,
    ) -> Self {
        Self {
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
pub(crate) fn build_event_resp_elements<T>(response: &S3Response<T>, request_id: &str) -> HashMap<String, String> {
    event_resp_elements(&response.headers, request_id)
}

fn event_resp_elements(response_headers: &HeaderMap, request_id: &str) -> HashMap<String, String> {
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

    fn legacy<T>(result: &'a S3Result<S3Response<T>>) -> Self {
        match result {
            Ok(res) => Self::Success {
                status: res.status.unwrap_or(StatusCode::OK),
                headers: &res.headers,
            },
            Err(err) => Self::failure(err.status_code(), err.message()),
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
    /// Create a new OperationHelper for S3 requests.
    pub fn new(req: &S3Request<impl Send + Sync>, event: EventName, op: S3Operation) -> Self {
        record_s3_op(op);
        Self::from_view(&AuditRequestView::legacy(req), event, op)
    }

    /// [`Self::new`] for a request whose audit facts an owned edge decoded
    /// (`RequestEnvelope::audit_view`). It does not count the operation: the owned edge already
    /// did, once, before calling the use case.
    pub(crate) fn from_view(req: &AuditRequestView<'_>, event: EventName, op: S3Operation) -> Self {
        Self::build(req, event, op, false)
    }

    /// Create an OperationHelper that only emits an audit entry, never a bucket notification.
    ///
    /// For operations that have no notification event of their own (`UploadPart`, the
    /// bucket configuration writes) or whose event is internal (`AbortMultipartUpload`).
    /// Unlike `new` it does not call `record_s3_op`, so per-operation metrics stay as the
    /// handlers already report them.
    pub fn audit_only(req: &S3Request<impl Send + Sync>, op: S3Operation) -> Self {
        Self::audit_only_from_view(&AuditRequestView::legacy(req), op)
    }

    /// [`Self::audit_only`] for a request whose audit facts an owned edge decoded.
    pub(crate) fn audit_only_from_view(req: &AuditRequestView<'_>, op: S3Operation) -> Self {
        let event = op.to_event_name().unwrap_or(EventName::AuditS3Operation);
        Self::build(req, event, op, true)
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

        let trigger = op.as_str();

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
            let mut event_builder = EventArgsBuilder::new(event, bucket, convert_ecstore_object_info(event_object))
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

    /// Complete operational details from S3 results.
    /// This method should be called immediately before the function returns.
    /// It consumes and prepares auxiliary structures for use during `drop`.
    pub fn complete<T>(self, result: &S3Result<S3Response<T>>) -> Self {
        self.complete_with(OperationOutcome::legacy(result))
    }

    /// [`Self::complete`] for an owned operation, which answers success with
    /// `status` and adds `headers` beyond its output.
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
            state.event_builder = Some(builder.resp_elements(event_resp_elements(headers, &state.request_context.request_id)));
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
    pub(crate) fn request_context_or_from_request<T>(&self, req: &S3Request<T>) -> RequestContext {
        self.request_context_or_from_view(&AuditRequestView::legacy(req))
    }

    /// [`Self::request_context_or_from_request`] for a request whose audit facts an owned edge decoded.
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

/// Builds the audit entry for a request the authorization layer refused.
///
/// The entry carries the action that was denied and the internal denial reason, so an
/// audit consumer can tell a refused request from a handler that never ran.
pub(crate) fn authorization_denial_entry<T>(
    req: &S3Request<T>,
    action: &Action,
    reason: &'static str,
    scope: DenialScope,
) -> AuditEntry {
    authorization_denial_entry_for_view(&AuditRequestView::legacy(req), action, reason, scope)
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

/// Runs `handler` on `req` and records the outcome as an audit-only entry for `op`.
///
/// The helper is built before the request is handed over so it sees the original headers
/// and extensions, and it completes with the handler's result, so failed requests are audited too.
pub(crate) async fn audit_only_operation<I, O, F, Fut>(req: S3Request<I>, op: S3Operation, handler: F) -> S3Result<S3Response<O>>
where
    I: Send + Sync,
    F: FnOnce(S3Request<I>) -> Fut,
    Fut: Future<Output = S3Result<S3Response<O>>>,
{
    let helper = OperationHelper::audit_only(&req, op);
    let result = handler(req).await;
    let _helper = helper.complete(&result);
    result
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
mod tests {
    use super::OperationHelper;
    use crate::server::{refresh_audit_module_enabled, refresh_notify_module_enabled};
    use crate::storage::access::ReqInfo;
    use crate::storage::request_context::RequestContext;
    use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
    use metrics::{Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, SharedString, Unit};
    use rustfs_audit::ObjectVersion;
    use rustfs_credentials::Credentials;
    use rustfs_s3_ops::S3Operation;
    use rustfs_s3_types::EventName;
    use rustfs_utils::http::headers::{AMZ_REQUEST_ID, REQUEST_ID_HEADER};
    use s3s::dto::{DeleteObjectTaggingInput, DeleteObjectTaggingOutput, GetObjectInput, GetObjectOutput};
    use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result};
    use std::sync::{Arc, Mutex};
    use temp_env::{async_with_vars, with_vars};

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[tokio::test]
    async fn diagnostic_background_tasks_keep_existing_entry_across_nested_spawns() {
        use super::ecstore_diagnostic::{ReadVersionEntrySource, current_read_version_entry_source, with_read_version_entry};
        use rustfs_io_metrics::s3_http_metrics::{
            S3HttpRequestGuard, current_s3_http_operation_context, current_s3_http_operation_name, observe_s3_http_operation,
            with_s3_http_operation_context,
        };

        let mut request = S3HttpRequestGuard::new("GET");
        let operation = request.in_scope(|| {
            observe_s3_http_operation(S3Operation::GetObject);
            current_s3_http_operation_context().expect("captured GET operation")
        });
        for has_request_context in [false, true] {
            let context = has_request_context.then(|| RequestContext::from_headers(&HeaderMap::new()));
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let (tx, rx) = tokio::sync::oneshot::channel();
            with_s3_http_operation_context(Some(operation), async {
                with_read_version_entry(ReadVersionEntrySource::TierConfigReload, async {
                    super::spawn_background_with_context(context, async move {
                        release_rx.await.expect("parent releases background work");
                        with_read_version_entry(ReadVersionEntrySource::ConfigRead, async move {
                            super::spawn_background_with_context(None, async move {
                                tx.send((current_s3_http_operation_name(), current_read_version_entry_source()))
                                    .expect("context observer remains live");
                            });
                        })
                        .await;
                    });
                })
                .await;
            })
            .await;
            assert_eq!(current_read_version_entry_source(), None);
            release_tx.send(()).expect("detached background work remains alive");
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(5), rx)
                    .await
                    .expect("background completion deadline")
                    .expect("context result"),
                (Some("s3:GetObject"), Some(ReadVersionEntrySource::TierConfigReload))
            );
        }
        drop(request);
        let (tx, rx) = tokio::sync::oneshot::channel();
        super::spawn_background(async move {
            tx.send((current_s3_http_operation_name(), current_read_version_entry_source()))
                .expect("unscoped observer remains live");
        });
        assert_eq!(rx.await.expect("unscoped task completes"), (None, None));
    }

    #[tokio::test]
    async fn owned_mutation_keeps_keystone_context_and_audits_panics_as_failure() {
        let bucket = "owned-mutation-panic";
        let probe = super::MutationCompletionProbe::install(bucket);
        let credentials = Credentials {
            access_key: "keystone:test".to_owned(),
            ..Default::default()
        };
        let result: S3Result<S3Response<()>> = rustfs_keystone::KEYSTONE_CREDENTIALS
            .scope(Some(credentials), async {
                super::run_owned_mutation(async {
                    assert_eq!(
                        rustfs_keystone::KEYSTONE_CREDENTIALS
                            .with(|credentials| credentials.as_ref().map(|value| value.access_key.clone())),
                        Some("keystone:test".to_owned())
                    );
                    let req = S3Request {
                        input: (),
                        method: Method::DELETE,
                        uri: Uri::from_static("/owned-mutation-panic/object"),
                        headers: HeaderMap::new(),
                        extensions: Extensions::new(),
                        credentials: None,
                        region: None,
                        service: None,
                        trailing_headers: None,
                    };
                    let _helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, S3Operation::DeleteObject);
                    panic!("injected mutation panic before reporting a commit");
                })
                .await
            })
            .await;
        assert_eq!(result.expect_err("join failure").code(), &S3ErrorCode::InternalError);
        probe.wait_for(1, 0).await;
        let audits = probe.audits.lock().expect("audits");
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].api.status.as_deref(), Some("failure"));
        assert_eq!(audits[0].api.status_code, Some(500));
        assert!(probe.events.lock().expect("events").is_empty());
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[test]
    fn diagnostic_background_tasks_keep_existing_entry_on_fallback_thread() {
        use super::ecstore_diagnostic::{ReadVersionEntrySource, current_read_version_entry_source, with_read_version_entry};
        use rustfs_io_metrics::s3_http_metrics::{
            S3HttpRequestGuard, current_s3_http_operation_context, current_s3_http_operation_name, observe_s3_http_operation,
            with_s3_http_operation_context,
        };

        assert!(tokio::runtime::Handle::try_current().is_err());
        let mut request = S3HttpRequestGuard::new("DELETE");
        let operation = request.in_scope(|| {
            observe_s3_http_operation(S3Operation::DeleteObject);
            current_s3_http_operation_context().expect("captured DELETE operation")
        });
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (tx, rx) = std::sync::mpsc::channel();
        futures::executor::block_on(with_s3_http_operation_context(Some(operation), async {
            with_read_version_entry(ReadVersionEntrySource::TierConfigReload, async {
                super::spawn_background(async move {
                    release_rx.await.expect("parent releases fallback work");
                    tx.send((current_s3_http_operation_name(), current_read_version_entry_source()))
                        .expect("fallback observer remains live");
                });
            })
            .await;
        }));
        drop(request);
        assert_eq!(current_read_version_entry_source(), None);
        release_tx.send(()).expect("fallback work remains alive");
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_secs(5))
                .expect("fallback thread completes"),
            (Some("s3:DeleteObject"), Some(ReadVersionEntrySource::TierConfigReload))
        );
        assert_eq!(current_s3_http_operation_name(), None);
    }

    #[tokio::test]
    async fn background_tasks_keep_operation_after_parent_scope_ends() {
        use rustfs_io_metrics::s3_http_metrics::{S3HttpRequestGuard, current_s3_http_operation_name, observe_s3_http_operation};

        for context in [None, Some(RequestContext::from_headers(&HeaderMap::new()))] {
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
            let mut request = S3HttpRequestGuard::new("GET");
            request.in_scope(|| {
                observe_s3_http_operation(S3Operation::GetObject);
                super::spawn_background_with_context(context, async move {
                    release_rx.await.expect("parent releases detached task");
                    completed_tx
                        .send(current_s3_http_operation_name())
                        .expect("receiver remains live");
                });
            });
            drop(request);
            assert_eq!(current_s3_http_operation_name(), None);
            release_tx.send(()).expect("detached task remains live");
            let observed = tokio::time::timeout(std::time::Duration::from_secs(5), completed_rx)
                .await
                .expect("detached completion deadline")
                .expect("detached task sends operation");
            assert_eq!(observed, Some("s3:GetObject"));
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        super::spawn_background_with_context(None, async move {
            tx.send(current_s3_http_operation_name())
                .expect("internal receiver remains live");
        });
        assert_eq!(rx.await.expect("internal task completes"), None);
    }

    #[test]
    fn background_tasks_keep_operation_on_fallback_thread() {
        use rustfs_io_metrics::s3_http_metrics::{S3HttpRequestGuard, current_s3_http_operation_name, observe_s3_http_operation};

        assert!(tokio::runtime::Handle::try_current().is_err());
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (tx, rx) = std::sync::mpsc::channel();
        let mut request = S3HttpRequestGuard::new("DELETE");
        request.in_scope(|| {
            observe_s3_http_operation(S3Operation::DeleteObject);
            super::spawn_background_with_context(Some(RequestContext::from_headers(&HeaderMap::new())), async move {
                release_rx.await.expect("parent releases fallback thread");
                tx.send(current_s3_http_operation_name())
                    .expect("fallback receiver remains live");
            });
        });
        drop(request);
        release_tx.send(()).expect("fallback task remains live");
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_secs(5))
                .expect("fallback thread completes"),
            Some("s3:DeleteObject")
        );
        assert_eq!(current_s3_http_operation_name(), None);
    }

    fn build_request<T>(input: T, method: Method, uri: Uri) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri,
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[derive(Clone, Default)]
    struct SeenMetricsRecorder {
        counters: Arc<Mutex<Vec<Key>>>,
    }

    impl SeenMetricsRecorder {
        fn saw_counter_named(&self, name: &str) -> bool {
            self.counters.lock().unwrap().iter().any(|key| key.name() == name)
        }
    }

    impl metrics::Recorder for SeenMetricsRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            self.counters.lock().unwrap().push(key.clone());
            Counter::from_arc(Arc::new(NoopCounter))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            Gauge::from_arc(Arc::new(NoopGauge))
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::from_arc(Arc::new(NoopHistogram))
        }
    }

    struct NoopCounter;

    impl CounterFn for NoopCounter {
        fn increment(&self, _value: u64) {}

        fn absolute(&self, _value: u64) {}
    }

    struct NoopGauge;

    impl GaugeFn for NoopGauge {
        fn increment(&self, _value: f64) {}

        fn decrement(&self, _value: f64) {}

        fn set(&self, _value: f64) {}
    }

    struct NoopHistogram;

    impl HistogramFn for NoopHistogram {
        fn record(&self, _value: f64) {}
    }

    #[test]
    fn operation_helper_uses_req_info_for_notification_context() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();
                let input = DeleteObjectTaggingInput::builder()
                    .bucket("input-bucket".to_string())
                    .key("input-object".to_string())
                    .build()
                    .unwrap();
                let mut req = build_request(input, Method::DELETE, Uri::from_static("/from-uri/ignored"));
                req.headers.insert("host", HeaderValue::from_static("example.com"));
                req.headers.insert("user-agent", HeaderValue::from_static("rustfs-test"));
                req.extensions.insert(ReqInfo {
                    cred: Some(Credentials {
                        access_key: "notifyTag".to_string(),
                        ..Default::default()
                    }),
                    bucket: Some("issue-2292-bucket".to_string()),
                    object: Some("prefix/issue-2292.txt".to_string()),
                    version_id: Some("version-123".to_string()),
                    ..Default::default()
                });

                let helper = OperationHelper::new(&req, EventName::ObjectTaggingPut, S3Operation::PutObjectTagging);
                let OperationHelper::Enabled(state) = &helper else {
                    panic!("helper should be enabled when notify/audit switches are on");
                };
                let event_args = state.event_builder.clone().expect("event builder should exist").build();

                assert_eq!(event_args.bucket_name, "issue-2292-bucket");
                assert_eq!(event_args.object.bucket, "issue-2292-bucket");
                assert_eq!(event_args.object.name, "prefix/issue-2292.txt");
                assert_eq!(event_args.version_id, "version-123");
                assert_eq!(event_args.req_params.get("principalId").map(String::as_str), Some("notifyTag"));
            },
        );
    }

    /// Operations without a notification event still produce an audit entry, and the
    /// audit-only constructor never builds a notification event, even with notify on.
    #[test]
    fn audit_only_helper_audits_operations_without_a_notification_event() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                for (op, uri, expected_event) in [
                    (S3Operation::UploadPart, "/audit-bucket/object.txt", EventName::AuditS3Operation),
                    (S3Operation::UploadPartCopy, "/audit-bucket/object.txt", EventName::AuditS3Operation),
                    (S3Operation::PutBucketPolicy, "/audit-bucket", EventName::AuditS3Operation),
                    (S3Operation::PutBucketVersioning, "/audit-bucket", EventName::AuditS3Operation),
                    (
                        S3Operation::AbortMultipartUpload,
                        "/audit-bucket/object.txt",
                        EventName::ObjectRemovedAbortMultipartUpload,
                    ),
                ] {
                    let input = DeleteObjectTaggingInput::builder()
                        .bucket("audit-bucket".to_string())
                        .key("object.txt".to_string())
                        .build()
                        .unwrap();
                    let mut req = build_request(input, Method::PUT, Uri::from_static(uri));
                    req.extensions.insert(ReqInfo {
                        cred: Some(Credentials {
                            access_key: "auditor".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    });

                    let result = Ok(S3Response::new(DeleteObjectTaggingOutput::default()));
                    let mut helper = OperationHelper::audit_only(&req, op).complete(&result);
                    assert!(!helper.wants_object_info(), "{} must not build a notification event", op.as_str());
                    assert!(helper.event_args().is_none(), "{} must not build a notification event", op.as_str());
                    let OperationHelper::Enabled(state) = &mut helper else {
                        panic!("helper should be enabled when the audit switch is on");
                    };
                    let entry = state.audit_builder.take().expect("audit builder should exist").build();
                    assert_eq!(entry.event, expected_event, "{}", op.as_str());
                    assert_eq!(entry.api.name.as_deref(), Some(op.as_str()));
                    assert_eq!(entry.api.bucket.as_deref(), Some("audit-bucket"));
                    assert_eq!(entry.api.status.as_deref(), Some("success"));
                    assert_eq!(entry.access_key.as_deref(), Some("auditor"));
                }
            },
        );
    }

    /// With audit off, the audit-only helper does no work: it is the disabled helper.
    #[test]
    fn audit_only_helper_is_disabled_when_audit_is_off() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("false")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();
                let input = DeleteObjectTaggingInput::builder()
                    .bucket("audit-bucket".to_string())
                    .key("object.txt".to_string())
                    .build()
                    .unwrap();
                let req = build_request(input, Method::PUT, Uri::from_static("/audit-bucket/object.txt"));
                assert!(matches!(
                    OperationHelper::audit_only(&req, S3Operation::UploadPart),
                    OperationHelper::Disabled
                ));
            },
        );
    }

    /// A failed audit-only operation is audited with a failure status, the error message and
    /// the error's status code.
    #[test]
    fn audit_only_helper_records_failure_status_and_error() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let input = DeleteObjectTaggingInput::builder()
                    .bucket("audit-bucket".to_string())
                    .key("object.txt".to_string())
                    .build()
                    .unwrap();
                let req = build_request(input, Method::PUT, Uri::from_static("/audit-bucket"));
                let result: S3Result<S3Response<DeleteObjectTaggingOutput>> =
                    Err(S3Error::with_message(S3ErrorCode::AccessDenied, "policy refused".to_string()));
                let mut helper = OperationHelper::audit_only(&req, S3Operation::PutBucketPolicy).complete(&result);
                let OperationHelper::Enabled(state) = &mut helper else {
                    panic!("helper should be enabled when the audit switch is on");
                };
                let entry = state.audit_builder.take().expect("audit builder should exist").build();
                assert_eq!(entry.api.status.as_deref(), Some("failure"));
                assert_eq!(entry.api.status_code, Some(403));
                assert_eq!(entry.error.as_deref(), Some("policy refused"));
            },
        );
    }

    /// A failed request is audited with its failure status, and the wrapper hands the
    /// handler's result back unchanged.
    #[tokio::test]
    async fn audit_only_operation_returns_the_handler_result_unchanged() {
        async_with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            async {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let input = DeleteObjectTaggingInput::builder()
                    .bucket("audit-bucket".to_string())
                    .key("object.txt".to_string())
                    .build()
                    .unwrap();
                let req = build_request(input, Method::PUT, Uri::from_static("/audit-bucket"));
                let err = super::audit_only_operation(req, S3Operation::PutBucketPolicy, |req| async move {
                    assert_eq!(req.input.bucket, "audit-bucket");
                    Err::<S3Response<DeleteObjectTaggingOutput>, _>(S3Error::new(S3ErrorCode::AccessDenied))
                })
                .await
                .unwrap_err();
                assert_eq!(*err.code(), S3ErrorCode::AccessDenied);
            },
        )
        .await;
    }

    #[test]
    fn operation_helper_audits_the_authenticated_request_identity() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                for (access_key, parent_user) in [
                    (Some("testuser"), ""),
                    (Some("rustfsadmin"), ""),
                    (Some("service-account"), "testuser"),
                    (Some("sts-session"), "testuser"),
                    (None, ""),
                    (Some(""), ""),
                ] {
                    let expected_access_key = access_key.filter(|key| !key.is_empty());
                    for outcome in [None, Some(true), Some(false)] {
                        let input = GetObjectInput::builder()
                            .bucket("audit-bucket".to_string())
                            .key("object.txt".to_string())
                            .build()
                            .expect("get object input should build");
                        let mut req = build_request(input, Method::GET, Uri::from_static("/audit-bucket/object.txt"));
                        req.extensions.insert(ReqInfo {
                            cred: access_key.map(|key| Credentials {
                                access_key: key.to_string(),
                                secret_key: "audit-secret-must-not-leak".to_string(),
                                session_token: "audit-token-must-not-leak".to_string(),
                                parent_user: parent_user.to_string(),
                                claims: match key {
                                    "service-account" => Some(std::collections::HashMap::from([(
                                        rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA.to_string(),
                                        serde_json::json!(rustfs_credentials::INHERITED_POLICY_TYPE),
                                    )])),
                                    "sts-session" => Some(std::collections::HashMap::from([(
                                        "sessionPolicy".to_string(),
                                        serde_json::json!("private-session-policy"),
                                    )])),
                                    _ => None,
                                },
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                        let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject);
                        if let Some(success) = outcome {
                            let result = if success {
                                Ok(S3Response::new(GetObjectOutput::default()))
                            } else {
                                Err(S3Error::new(S3ErrorCode::NoSuchKey))
                            };
                            helper = helper.complete(&result);
                        }
                        let OperationHelper::Enabled(state) = &mut helper else {
                            panic!("helper should be enabled when the audit switch is on");
                        };
                        let entry = state.audit_builder.take().expect("audit builder should exist").build();
                        assert_eq!(entry.access_key.as_deref(), expected_access_key);
                        let json = serde_json::to_value(&entry).expect("audit entry should serialize");
                        assert_eq!(json.get("accessKey").and_then(|key| key.as_str()), expected_access_key);
                        let expected_parent_user = (!parent_user.is_empty()).then_some(parent_user);
                        assert_eq!(entry.parent_user.as_deref(), expected_parent_user);
                        assert_eq!(json.get("parentUser").and_then(|value| value.as_str()), expected_parent_user);
                        assert!(!json.as_object().unwrap().contains_key("requestClaims"));
                        let rendered = serde_json::to_string(&entry).expect("audit entry should serialize");
                        assert!(!rendered.contains("audit-secret-must-not-leak"));
                        assert!(!rendered.contains("audit-token-must-not-leak"));
                        assert!(!rendered.contains("private-session-policy"));
                    }
                }
            },
        );
    }

    fn oidc_audit_credentials(issuer: Option<&str>) -> Credentials {
        let mut claims = std::collections::HashMap::from([
            ("sub".to_string(), serde_json::json!(" subject-9170 ")),
            ("iss".to_string(), serde_json::json!("rustfs-oidc")),
            ("oidc_provider".to_string(), serde_json::json!("default")),
            ("email".to_string(), serde_json::json!("private@example.test")),
            ("preferred_username".to_string(), serde_json::json!("private-user")),
            ("groups".to_string(), serde_json::json!(["private-group"])),
            ("roles".to_string(), serde_json::json!(["private-role"])),
            ("policy".to_string(), serde_json::json!("private-policy")),
            ("sessionPolicy".to_string(), serde_json::json!("private-encoded-policy")),
            ("sessionPolicy-extracted".to_string(), serde_json::json!("private-decoded-policy")),
            ("custom".to_string(), serde_json::json!({"token": "private-custom-token"})),
        ]);
        if let Some(issuer) = issuer {
            claims.insert(rustfs_iam::sys::OIDC_ISSUER_CLAIM.to_string(), serde_json::json!(issuer));
        }
        Credentials {
            access_key: "oidc-session".to_string(),
            parent_user: "oidc-parent".to_string(),
            secret_key: "audit-secret-must-not-leak".to_string(),
            session_token: "audit-token-must-not-leak".to_string(),
            claims: Some(claims),
            ..Default::default()
        }
    }

    #[test]
    fn operation_helper_preserves_oidc_audit_identity_without_sensitive_claims() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                for issuer in [Some("https://idp-a.example.test"), Some("https://idp-b.example.test"), None] {
                    for service_account in [false, true] {
                        let mut credentials = oidc_audit_credentials(issuer);
                        if service_account {
                            credentials.access_key = "oidc-service-account".to_string();
                            credentials.claims.as_mut().unwrap().insert(
                                rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA.to_string(),
                                serde_json::json!(rustfs_credentials::INHERITED_POLICY_TYPE),
                            );
                        }
                        let before = serde_json::to_value(&credentials).expect("credentials should serialize");
                        let mut expected_claims = serde_json::json!({
                            "sub": " subject-9170 ",
                            "iss": "rustfs-oidc",
                            "oidc_provider": "default"
                        });
                        if let Some(issuer) = issuer {
                            expected_claims["x-rustfs-internal-oidc-issuer"] = serde_json::json!(issuer);
                        }

                        for outcome in [None, Some(true), Some(false)] {
                            let mut req = build_request((), Method::GET, Uri::from_static("/audit-bucket/object.txt"));
                            req.extensions.insert(ReqInfo {
                                cred: Some(credentials.clone()),
                                ..Default::default()
                            });
                            let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject);
                            if let Some(success) = outcome {
                                let result = if success {
                                    Ok(S3Response::new(GetObjectOutput::default()))
                                } else {
                                    Err(S3Error::new(S3ErrorCode::NoSuchKey))
                                };
                                helper = helper.complete(&result);
                            }
                            let OperationHelper::Enabled(state) = &mut helper else {
                                panic!("audit helper should be enabled");
                            };
                            let entry = state.audit_builder.take().expect("audit builder should exist").build();
                            let json = serde_json::to_value(&entry).expect("audit entry should serialize");
                            assert_eq!(json["accessKey"], credentials.access_key);
                            assert_eq!(json["parentUser"], "oidc-parent");
                            assert_eq!(json["requestClaims"], expected_claims);
                            let rendered = serde_json::to_string(&entry).expect("audit entry should serialize");
                            assert!(!rendered.contains("private-"));
                            assert!(!rendered.contains("private@example.test"));
                            assert!(!rendered.contains("audit-secret-must-not-leak"));
                            assert!(!rendered.contains("audit-token-must-not-leak"));
                            assert_eq!(
                                serde_json::to_value(req.extensions.get::<ReqInfo>().unwrap().cred.as_ref().unwrap()).unwrap(),
                                before
                            );
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn authorization_denial_preserves_oidc_audit_identity() {
        use rustfs_policy::policy::action::{Action, S3Action};

        let credentials = oidc_audit_credentials(Some("https://idp.example.test"));
        let before = serde_json::to_value(&credentials).unwrap();
        let mut req = build_request((), Method::GET, Uri::from_static("/audit-bucket/object.txt"));
        req.extensions.insert(ReqInfo {
            cred: Some(credentials),
            ..Default::default()
        });
        let entry = super::authorization_denial_entry(
            &req,
            &Action::S3Action(S3Action::GetObjectAction),
            "iam_explicit_deny",
            super::DenialScope::Request,
        );
        let json = serde_json::to_value(entry).expect("denial should serialize");
        assert_eq!(json["parentUser"], "oidc-parent");
        assert_eq!(
            json["requestClaims"],
            serde_json::json!({
                "sub": " subject-9170 ",
                "iss": "rustfs-oidc",
                "oidc_provider": "default",
                "x-rustfs-internal-oidc-issuer": "https://idp.example.test"
            })
        );
        assert_eq!(json["api"]["status_code"], 403);
        assert_eq!(json["tags"]["denialReason"], "iam_explicit_deny");
        let rendered = json.to_string();
        assert!(!rendered.contains("private"));
        assert!(!rendered.contains("audit-secret-must-not-leak"));
        assert!(!rendered.contains("audit-token-must-not-leak"));
        assert_eq!(
            serde_json::to_value(req.extensions.get::<ReqInfo>().unwrap().cred.as_ref().unwrap()).unwrap(),
            before
        );
    }

    #[test]
    fn request_audit_identity_omits_non_oidc_and_empty_claims() {
        for (key, value, expected_oidc) in [
            (rustfs_iam::sys::OIDC_ISSUER_CLAIM, serde_json::json!(""), true),
            (rustfs_iam::sys::OIDC_ISSUER_CLAIM, serde_json::json!(["https://idp.example.test"]), true),
            ("iss", serde_json::json!("other-session"), false),
            ("sub", serde_json::json!(""), false),
            ("oidc_provider", serde_json::Value::Null, false),
        ] {
            let mut credentials = oidc_audit_credentials(None);
            credentials.claims.as_mut().unwrap().insert(key.to_string(), value);
            let mut req = build_request((), Method::GET, Uri::from_static("/audit-bucket/object.txt"));
            req.extensions.insert(ReqInfo {
                cred: Some(credentials),
                ..Default::default()
            });
            let entry = super::request_audit_builder(
                &super::AuditRequestView::legacy(&req),
                EventName::ObjectAccessedGet,
                "GetObject",
                rustfs_audit::ApiDetails::default(),
                "audit-identity-boundary",
            )
            .build();
            let json = serde_json::to_value(entry).expect("audit entry should serialize");
            assert_eq!(json["parentUser"], "oidc-parent");
            if expected_oidc {
                assert_eq!(
                    json["requestClaims"],
                    serde_json::json!({"sub": " subject-9170 ", "iss": "rustfs-oidc", "oidc_provider": "default"})
                );
            } else {
                assert!(!json.as_object().unwrap().contains_key("requestClaims"));
            }
        }
    }

    #[test]
    fn operation_helper_audits_verified_client_addresses() {
        use crate::shared_types::RemoteAddr;
        use rustfs_trusted_proxies::{ClientInfo, ValidationMode};
        use std::net::SocketAddr;

        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let peer: SocketAddr = "192.0.2.10:54321".parse().unwrap();
                let ipv6_peer: SocketAddr = "[2001:db8::10]:54321".parse().unwrap();
                let proxied = ClientInfo::from_trusted_proxy(
                    "198.51.100.20".parse().unwrap(),
                    None,
                    None,
                    peer.ip(),
                    1,
                    ValidationMode::Strict,
                    Vec::new(),
                );
                for (remote_addr, client_info, expected) in [
                    (Some(RemoteAddr(peer)), None, "192.0.2.10"),
                    (Some(RemoteAddr(ipv6_peer)), None, "2001:db8::10"),
                    (Some(RemoteAddr(peer)), Some(proxied), "198.51.100.20"),
                    (Some(RemoteAddr(peer)), Some(ClientInfo::direct(peer)), "192.0.2.10"),
                    (None, None, ""),
                ] {
                    for spoofed in [false, true] {
                        let input = GetObjectInput::builder()
                            .bucket("audit-bucket".to_string())
                            .key("object.txt".to_string())
                            .build()
                            .expect("get object input should build");
                        let mut req = build_request(input, Method::GET, Uri::from_static("/audit-bucket/object.txt"));
                        req.extensions.insert(remote_addr);
                        if let Some(client_info) = client_info.as_ref() {
                            req.extensions.insert(client_info.clone());
                        }
                        if spoofed {
                            req.headers
                                .insert("x-forwarded-for", HeaderValue::from_static("203.0.113.99"));
                            req.headers.insert("x-real-ip", HeaderValue::from_static("203.0.113.98"));
                        }
                        let result = Ok(S3Response::new(GetObjectOutput::default()));
                        let mut helper =
                            OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject).complete(&result);
                        let OperationHelper::Enabled(state) = &mut helper else {
                            panic!("helper should be enabled when the audit switch is on");
                        };
                        let entry = state.audit_builder.take().expect("audit builder should exist").build();
                        assert_eq!(entry.remote_host.as_deref(), Some(expected));
                        let rendered = serde_json::to_value(&entry).expect("audit entry should serialize");
                        assert_eq!(rendered["remotehost"], expected);
                    }
                }
            },
        );
    }

    #[test]
    fn operation_helper_adds_objects_to_audit_details() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let input = DeleteObjectTaggingInput::builder()
                    .bucket("test-bucket".to_string())
                    .key("test-key".to_string())
                    .build()
                    .expect("delete object tagging input should build");
                let req = build_request(input, Method::DELETE, Uri::from_static("/test-bucket"));
                let objects = vec![
                    ObjectVersion::new("first-key".to_string(), None),
                    ObjectVersion::new("second-key".to_string(), Some("version-123".to_string())),
                ];
                let mut empty_helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, S3Operation::DeleteObjects)
                    .audit_objects(Vec::new());
                let OperationHelper::Enabled(empty_state) = &mut empty_helper else {
                    panic!("helper should be enabled when the audit switch is on");
                };
                assert!(empty_state.api_builder.0.objects.is_none());
                empty_state.audit_builder.take();

                let result = Ok(S3Response::new(DeleteObjectTaggingOutput::default()));
                let mut helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, S3Operation::DeleteObjects)
                    .audit_objects(objects.clone())
                    .complete(&result);

                let OperationHelper::Enabled(state) = &mut helper else {
                    panic!("helper should be enabled when the audit switch is on");
                };
                let audit_entry = state.audit_builder.take().expect("audit builder should exist").build();
                assert_eq!(audit_entry.api.objects, Some(objects));
            },
        );
    }

    #[test]
    fn operation_helper_initializes_audit_api_details_before_completion() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let input = GetObjectInput::builder()
                    .bucket("audit-bucket".to_string())
                    .key("missing/object.txt".to_string())
                    .build()
                    .expect("get object input should build");
                let req = build_request(input, Method::GET, Uri::from_static("/audit-bucket/missing/object.txt"));
                let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject);

                let OperationHelper::Enabled(state) = &mut helper else {
                    panic!("helper should be enabled when the audit switch is on");
                };
                let audit_entry = state.audit_builder.take().expect("audit builder should exist").build();

                assert_eq!(audit_entry.api.name.as_deref(), Some("s3:GetObject"));
                assert_eq!(audit_entry.api.bucket.as_deref(), Some("audit-bucket"));
                assert_eq!(audit_entry.api.object.as_deref(), Some("missing/object.txt"));
                assert!(audit_entry.api.status_code.is_none());
            },
        );
    }

    #[test]
    fn operation_helper_complete_records_failed_status_code() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let input = GetObjectInput::builder()
                    .bucket("audit-bucket".to_string())
                    .key("missing/object.txt".to_string())
                    .build()
                    .expect("get object input should build");
                let req = build_request(input, Method::GET, Uri::from_static("/audit-bucket/missing/object.txt"));
                let result: Result<S3Response<GetObjectOutput>, S3Error> = Err(S3Error::new(S3ErrorCode::NoSuchKey));
                let mut helper =
                    OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject).complete(&result);

                let OperationHelper::Enabled(state) = &mut helper else {
                    panic!("helper should be enabled when the audit switch is on");
                };
                let audit_entry = state.audit_builder.take().expect("audit builder should exist").build();

                assert_eq!(audit_entry.api.name.as_deref(), Some("s3:GetObject"));
                assert_eq!(audit_entry.api.status.as_deref(), Some("failure"));
                assert_eq!(audit_entry.api.status_code, Some(404));
            },
        );
    }

    #[test]
    fn operation_helper_prioritizes_request_context_for_request_id() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let input = DeleteObjectTaggingInput::builder()
                    .bucket("test-bucket".to_string())
                    .key("test-key".to_string())
                    .build()
                    .unwrap();
                let mut req = build_request(input, Method::DELETE, Uri::from_static("/test-bucket/test-key"));
                req.headers.insert("host", HeaderValue::from_static("example.com"));
                req.headers.insert("user-agent", HeaderValue::from_static("rustfs-test"));
                req.headers
                    .insert(REQUEST_ID_HEADER, HeaderValue::from_static("ingress-canonical-uuid"));
                req.headers
                    .insert("x-amz-request-id", HeaderValue::from_static("client-supplied-request-id"));

                req.extensions.insert(RequestContext {
                    request_id: "ingress-canonical-uuid".to_string(),
                    x_amz_request_id: "client-supplied-request-id".to_string(),
                    trace_id: None,
                    span_id: None,
                    start_time: std::time::Instant::now(),
                });

                req.extensions.insert(ReqInfo {
                    bucket: Some("test-bucket".to_string()),
                    object: Some("test-key".to_string()),
                    ..Default::default()
                });

                let result = Ok(S3Response::new(DeleteObjectTaggingOutput::default()));
                let mut helper =
                    OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject).complete(&result);

                // Verify the helper stored the RequestContext
                let OperationHelper::Enabled(state) = &mut helper else {
                    panic!("helper should be enabled when notify/audit switches are on");
                };
                assert_eq!(state.request_context.request_id, "ingress-canonical-uuid");
                let audit_entry = state.audit_builder.take().expect("audit builder should exist").build();
                assert_eq!(audit_entry.request_id.as_deref(), Some("ingress-canonical-uuid"));
                let event_args = state.event_builder.clone().expect("event builder should exist").build();
                assert_eq!(
                    event_args.req_params.get(AMZ_REQUEST_ID).map(String::as_str),
                    Some("client-supplied-request-id")
                );
                assert_eq!(
                    event_args.resp_elements.get(AMZ_REQUEST_ID).map(String::as_str),
                    Some("ingress-canonical-uuid")
                );
            },
        );
    }

    #[test]
    fn operation_helper_reuses_generated_context_when_headers_absent() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let input = DeleteObjectTaggingInput::builder()
                    .bucket("test-bucket".to_string())
                    .key("test-key".to_string())
                    .build()
                    .unwrap();
                let mut req = build_request(input, Method::DELETE, Uri::from_static("/test-bucket/test-key"));
                req.headers.insert("host", HeaderValue::from_static("example.com"));
                req.headers.insert("user-agent", HeaderValue::from_static("rustfs-test"));

                // No RequestContext inserted
                req.extensions.insert(ReqInfo {
                    bucket: Some("test-bucket".to_string()),
                    object: Some("test-key".to_string()),
                    ..Default::default()
                });

                let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject);
                let request_context = helper.request_context_or_from_request(&req);
                let request_id = request_context.request_id;
                assert!(uuid::Uuid::parse_str(&request_id).is_ok());
                let result = Ok(S3Response::new(DeleteObjectTaggingOutput::default()));
                let helper = helper.complete(&result);
                let OperationHelper::Enabled(state) = &helper else {
                    panic!("helper should be enabled when notify/audit switches are on");
                };
                assert_eq!(state.request_context.request_id, request_id);
                let event_args = state.event_builder.clone().expect("event builder should exist").build();
                assert!(!event_args.req_params.contains_key(AMZ_REQUEST_ID));
                assert_eq!(
                    event_args.resp_elements.get(AMZ_REQUEST_ID).map(String::as_str),
                    Some(request_id.as_str())
                );
            },
        );
    }

    #[test]
    fn operation_helper_returns_disabled_when_both_switches_off() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("false")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let input = DeleteObjectTaggingInput::builder()
                    .bucket("test-bucket".to_string())
                    .key("test-key".to_string())
                    .build()
                    .unwrap();
                let mut req = build_request(input, Method::DELETE, Uri::from_static("/test-bucket/test-key"));
                req.headers
                    .insert(REQUEST_ID_HEADER, HeaderValue::from_static("client-request-id"));
                req.extensions.insert(RequestContext {
                    request_id: "server-request-id".to_string(),
                    x_amz_request_id: "server-request-id".to_string(),
                    trace_id: None,
                    span_id: None,
                    start_time: std::time::Instant::now(),
                });
                let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject);

                assert!(matches!(&helper, OperationHelper::Disabled));
                assert_eq!(helper.request_context_or_from_request(&req).request_id, "server-request-id");
            },
        );
    }

    /// SSE-KMS object metadata whose IV is missing, so unwrapping fails inside the
    /// SSE layer without depending on a KMS service being reachable from a unit test.
    fn unreadable_sse_kms_metadata() -> std::collections::HashMap<String, String> {
        std::collections::HashMap::from([
            ("x-amz-server-side-encryption".to_string(), "aws:kms".to_string()),
            ("x-rustfs-encryption-key-id".to_string(), "finance-key".to_string()),
            ("x-rustfs-encryption-key".to_string(), base64_simd::STANDARD.encode_to_string([7u8; 48])),
            ("x-rustfs-encryption-algorithm".to_string(), "aws:kms".to_string()),
        ])
    }

    #[tokio::test]
    async fn operation_helper_attaches_the_sse_kms_summary_to_the_audit_entry() {
        async_with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            async {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let input = DeleteObjectTaggingInput::builder()
                    .bucket("finance".to_string())
                    .key("ledger.csv".to_string())
                    .build()
                    .unwrap();
                let mut req = build_request(input, Method::GET, Uri::from_static("/finance/ledger.csv"));
                req.extensions.insert(RequestContext {
                    request_id: "kms-tagged-request".to_string(),
                    x_amz_request_id: "kms-tagged-request".to_string(),
                    trace_id: None,
                    span_id: None,
                    start_time: std::time::Instant::now(),
                });
                req.extensions.insert(ReqInfo {
                    cred: Some(Credentials {
                        access_key: "analyst".to_string(),
                        ..Default::default()
                    }),
                    bucket: Some("finance".to_string()),
                    object: Some("ledger.csv".to_string()),
                    ..Default::default()
                });

                let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject);

                // Drive the real data path: the principal built from this request is
                // what carries the audit slot into the SSE code.
                let metadata = unreadable_sse_kms_metadata();
                let principal =
                    crate::storage::sse::SseKmsPrincipal::from_request(&req).expect("authenticated request has a principal");
                crate::storage::sse::sse_decryption(crate::storage::sse::DecryptionRequest {
                    bucket: "finance",
                    key: "ledger.csv",
                    metadata: &metadata,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    principal: Some(&principal),
                })
                .await
                .expect_err("metadata without an IV cannot be unwrapped");

                let result = Ok(S3Response::new(DeleteObjectTaggingOutput::default()));
                let mut helper = helper.complete(&result);
                let OperationHelper::Enabled(state) = &mut helper else {
                    panic!("helper should be enabled when the audit switch is on");
                };
                let entry = state.audit_builder.take().expect("audit builder should exist").build();
                let tags = entry.tags.expect("a request that used KMS must carry its summary");

                assert_eq!(tags.get("sseType").and_then(|value| value.as_str()), Some("SSE-KMS"));
                assert_eq!(tags.get("kmsKeyId").and_then(|value| value.as_str()), Some("finance-key"));
                assert_eq!(tags.get("kmsOutcome").and_then(|value| value.as_str()), Some("failure"));
                assert!(tags.contains_key("kmsErrorClass"), "a failed KMS interaction must be classified");

                let rendered = serde_json::to_string(&tags).expect("audit tags serialize");
                assert!(
                    !rendered.contains(&base64_simd::STANDARD.encode_to_string([7u8; 48])),
                    "the audit entry must not carry the wrapped data key: {rendered}"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn operation_helper_omits_kms_tags_for_requests_that_used_no_sse() {
        async_with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("true")),
            ],
            async {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let input = DeleteObjectTaggingInput::builder()
                    .bucket("finance".to_string())
                    .key("plain.txt".to_string())
                    .build()
                    .unwrap();
                let mut req = build_request(input, Method::GET, Uri::from_static("/finance/plain.txt"));
                req.extensions.insert(RequestContext {
                    request_id: "plain-request".to_string(),
                    x_amz_request_id: "plain-request".to_string(),
                    trace_id: None,
                    span_id: None,
                    start_time: std::time::Instant::now(),
                });

                let result = Ok(S3Response::new(DeleteObjectTaggingOutput::default()));
                let mut helper =
                    OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject).complete(&result);
                let OperationHelper::Enabled(state) = &mut helper else {
                    panic!("helper should be enabled when the audit switch is on");
                };
                let entry = state.audit_builder.take().expect("audit builder should exist").build();

                assert!(
                    entry.tags.is_none_or(|tags| tags.keys().all(|key| !key.starts_with("kms"))),
                    "a request that never reached KMS must not be tagged as if it had"
                );
            },
        )
        .await;
    }

    #[test]
    fn operation_helper_still_records_s3_ops_when_audit_and_notify_are_disabled() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("false")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("false")),
            ],
            || {
                refresh_notify_module_enabled();
                refresh_audit_module_enabled();

                let recorder = SeenMetricsRecorder::default();
                let input = DeleteObjectTaggingInput::builder()
                    .bucket("test-bucket".to_string())
                    .key("test-key".to_string())
                    .build()
                    .unwrap();
                let req = build_request(input, Method::DELETE, Uri::from_static("/test-bucket/test-key"));

                metrics::with_local_recorder(&recorder, || {
                    let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject);
                    assert!(matches!(helper, OperationHelper::Disabled));
                });

                assert!(
                    recorder.saw_counter_named("rustfs_s3_operations_total"),
                    "S3 operation metrics should still be recorded when audit/notify are disabled"
                );
            },
        );
    }
}
