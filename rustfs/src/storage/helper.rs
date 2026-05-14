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

use crate::server::{is_audit_module_enabled, is_notify_module_enabled};
use crate::storage::access::{ReqInfo, request_context_from_req};
use crate::storage::request_context::{RequestContext, extract_request_id_from_headers};
use hashbrown::HashMap;
use http::StatusCode;
use metrics::counter;
use rustfs_audit::{
    entity::{ApiDetails, ApiDetailsBuilder, AuditEntryBuilder},
    global::AuditLogger,
};
use rustfs_ecstore::store_api::ObjectInfo;
use rustfs_notify::{EventArgsBuilder, notifier_global};
use rustfs_s3_common::record_s3_op;
use rustfs_s3_common::{EventName, S3Operation};
use rustfs_utils::{
    extract_params_header, extract_req_params, extract_resp_elements, get_request_host, get_request_port, get_request_user_agent,
    http::headers::AMZ_REQUEST_ID,
};
use s3s::{S3Request, S3Response, S3Result};
use serde_json::Value;
use std::future::Future;
use tokio::runtime::{Builder, Handle};
use tracing::{Instrument, info_span};

/// Schedules an asynchronous task on the current runtime;
/// if there is no runtime, creates a minimal runtime execution on a new thread.
pub(crate) fn spawn_background<F>(fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
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
    request_context: Option<RequestContext>,
}

impl OperationHelper {
    /// Create a new OperationHelper for S3 requests.
    pub fn new(req: &S3Request<impl Send + Sync>, event: EventName, op: S3Operation) -> Self {
        let audit_enabled = is_audit_module_enabled();
        let notify_enabled = should_build_notification_event(is_notify_module_enabled());

        let path = req.uri.path().trim_start_matches('/');
        let mut segs = path.splitn(2, '/');
        let path_bucket = segs.next().unwrap_or("").to_string();
        let path_object_key = segs.next().unwrap_or("").to_string();
        let req_info = req.extensions.get::<ReqInfo>();
        let bucket = req_info
            .and_then(|info| info.bucket.clone())
            .filter(|value| !value.is_empty())
            .unwrap_or(path_bucket);

        let bucket_label = if bucket.is_empty() { "*" } else { &bucket };
        record_s3_op(op, bucket_label);

        // Fast path: when both chains are disabled, avoid all request parsing/builder work.
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

        // Infer remote address
        let remote_host = req
            .headers
            .get(rustfs_utils::http::X_FORWARDED_FOR)
            .and_then(|v| v.to_str().ok())
            .or_else(|| req.headers.get(rustfs_utils::http::X_REAL_IP).and_then(|v| v.to_str().ok()))
            .unwrap_or("")
            .to_string();

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
        // Resolve canonical request context and request_id in a single pass:
        //   RequestContext.request_id > extract_request_id_from_headers() > generated fallback id
        let request_context = request_context_from_req(req);
        if request_context.is_none() {
            counter!("rustfs_log_chain_orphan_total", "component" => "operation_helper").increment(1);
        }
        let request_id = request_context
            .as_ref()
            .map(|ctx| ctx.request_id.clone())
            .unwrap_or_else(|| extract_request_id_from_headers(&req.headers));

        let audit_builder = if audit_enabled {
            Some(
                AuditEntryBuilder::new("1.0", event, trigger, ApiDetails::default())
                    .remote_host(remote_host)
                    .user_agent(get_request_user_agent(&req.headers))
                    .req_host(get_request_host(&req.headers))
                    .req_path(req.uri.path().to_string())
                    .req_query(extract_req_params(req))
                    .request_id(&request_id),
            )
        } else {
            None
        };

        let event_object = ObjectInfo {
            bucket: bucket.clone(),
            name: object_key,
            ..Default::default()
        };

        let mut req_params = extract_params_header(&req.headers);
        // Inject x-amz-request-id from RequestContext into req_params for event correlation
        if let Some(ref ctx) = request_context {
            req_params
                .entry(AMZ_REQUEST_ID.to_string())
                .or_insert_with(|| ctx.x_amz_request_id.clone());
        }
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
            let mut event_builder = EventArgsBuilder::new(event, bucket, event_object)
                .host(get_request_host(&req.headers))
                .port(get_request_port(&req.headers))
                .user_agent(get_request_user_agent(&req.headers))
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

        Self::Enabled(Box::new(EnabledOperationHelper {
            audit_enabled,
            notify_enabled,
            audit_builder,
            api_builder,
            event_builder,
            start_time: request_context
                .as_ref()
                .map(|ctx| ctx.start_time)
                .unwrap_or_else(std::time::Instant::now),
            request_context,
        }))
    }

    /// Sets the ObjectInfo for event notification.
    pub fn object(mut self, object_info: ObjectInfo) -> Self {
        if let Self::Enabled(state) = &mut self
            && let Some(builder) = state.event_builder.take()
        {
            state.event_builder = Some(builder.object(object_info));
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
    pub fn complete<T>(mut self, result: &S3Result<S3Response<T>>) -> Self {
        let Self::Enabled(state) = &mut self else {
            return self;
        };

        let (status, status_code, error_msg) = match result {
            Ok(res) => ("success".to_string(), res.status.unwrap_or(StatusCode::OK).as_u16() as i32, None),
            Err(e) => (
                "failure".to_string(),
                e.status_code().unwrap_or(StatusCode::BAD_REQUEST).as_u16() as i32,
                e.message().map(|s| s.to_string()),
            ),
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
            if let Ok(res) = result {
                final_builder = final_builder.resp_header(extract_resp_elements(res));
            }
            if let Some(err) = error_msg {
                final_builder = final_builder.error(err);
            }

            if let Some(sk) = rustfs_credentials::get_global_access_key_opt() {
                final_builder = final_builder.access_key(&sk);
            }

            // Inject OpenTelemetry trace context into audit tags for distributed tracing correlation
            if let Some(ref ctx) = state.request_context
                && (ctx.trace_id.is_some() || ctx.span_id.is_some())
            {
                let mut tags = HashMap::new();
                if let Some(ref tid) = ctx.trace_id {
                    tags.insert("traceId".to_string(), Value::String(tid.clone()));
                }
                if let Some(ref sid) = ctx.span_id {
                    tags.insert("spanId".to_string(), Value::String(sid.clone()));
                }
                final_builder = final_builder.tags(tags);
            }

            state.audit_builder = Some(final_builder);
            state.api_builder = ApiDetailsBuilder(api_details); // Store final details for Drop use
        }

        // Completion event notification (only on success)
        if state.notify_enabled
            && let (Some(builder), Ok(res)) = (state.event_builder.take(), result)
        {
            state.event_builder = Some(builder.resp_elements(extract_resp_elements(res)));
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
}

fn should_build_notification_event(notify_module_enabled: bool) -> bool {
    notify_module_enabled || rustfs_notify::notification_system().is_some_and(|system| system.has_live_listeners())
}

impl Drop for OperationHelper {
    fn drop(&mut self) {
        let Self::Enabled(state) = self else {
            return;
        };

        // Distribute audit logs
        if state.audit_enabled
            && let Some(builder) = state.audit_builder.take()
        {
            let ctx = state.request_context.clone();
            spawn_background_with_context(ctx, async move {
                AuditLogger::log(builder.build()).await;
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
                let ctx = state.request_context.clone();
                spawn_background_with_context(ctx, async move {
                    notifier_global::notify(event_args).await;
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{refresh_audit_module_enabled, refresh_notify_module_enabled};
    use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
    use metrics::{Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, SharedString, Unit};
    use rustfs_credentials::Credentials;
    use s3s::dto::DeleteObjectTaggingInput;
    use std::sync::{Arc, Mutex};
    use temp_env::with_vars;

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
                let event_args = match &helper {
                    OperationHelper::Enabled(state) => state.event_builder.clone().expect("event builder should exist").build(),
                    OperationHelper::Disabled => panic!("helper should be enabled when notify/audit switches are on"),
                };

                assert_eq!(event_args.bucket_name, "issue-2292-bucket");
                assert_eq!(event_args.object.bucket, "issue-2292-bucket");
                assert_eq!(event_args.object.name, "prefix/issue-2292.txt");
                assert_eq!(event_args.version_id, "version-123");
                assert_eq!(event_args.req_params.get("principalId").map(String::as_str), Some("notifyTag"));
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

                // Insert RequestContext (set by ingress layer) with a specific request_id
                req.extensions.insert(RequestContext {
                    request_id: "ingress-canonical-uuid".to_string(),
                    x_amz_request_id: "ingress-canonical-uuid".to_string(),
                    trace_id: None,
                    span_id: None,
                    start_time: std::time::Instant::now(),
                });

                req.extensions.insert(ReqInfo {
                    bucket: Some("test-bucket".to_string()),
                    object: Some("test-key".to_string()),
                    ..Default::default()
                });

                let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject);

                // Verify the helper stored the RequestContext
                match &helper {
                    OperationHelper::Enabled(state) => {
                        assert!(state.request_context.is_some());
                        assert_eq!(state.request_context.as_ref().unwrap().request_id, "ingress-canonical-uuid");
                    }
                    OperationHelper::Disabled => panic!("helper should be enabled when notify/audit switches are on"),
                }
            },
        );
    }

    #[test]
    fn operation_helper_no_request_context_when_absent() {
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
                    .insert("x-amz-request-id", HeaderValue::from_static("amz-header-uuid"));

                // No RequestContext inserted
                req.extensions.insert(ReqInfo {
                    bucket: Some("test-bucket".to_string()),
                    object: Some("test-key".to_string()),
                    ..Default::default()
                });

                let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject);

                // Verify the helper has no RequestContext
                match &helper {
                    OperationHelper::Enabled(state) => assert!(state.request_context.is_none()),
                    OperationHelper::Disabled => panic!("helper should be enabled when notify/audit switches are on"),
                }
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
                let req = build_request(input, Method::DELETE, Uri::from_static("/test-bucket/test-key"));
                let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject);

                assert!(matches!(helper, OperationHelper::Disabled));
            },
        );
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
