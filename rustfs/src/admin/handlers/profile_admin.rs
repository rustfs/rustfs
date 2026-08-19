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

use super::profile::{authorize_profile_request, profile_not_implemented_response};
use crate::admin::auth::authorize_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::storage_api::access::spawn_traced;
use crate::server::ADMIN_PREFIX;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use regex::Regex;
use rustfs_common::trace_bus::{TraceEvent, TraceKind, TraceVal, subscribe_trace_events};
use rustfs_madmin::service_commands::ServiceTraceOpts;
use rustfs_madmin::trace::TraceType;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::stream::{ByteStream, DynByteStream};
use s3s::{Body, S3Request, S3Response, S3Result, StdError, s3_error};
use serde::Serialize;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::error;
use url::form_urlencoded;

#[derive(Serialize)]
struct ProfileStatus {
    enabled: &'static str,
    status: &'static str,
    platform: &'static str,
    message: &'static str,
}

pub fn register_profiling_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/debug/pprof/profile").as_str(),
        AdminOperation(&ProfileHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/debug/pprof/status").as_str(),
        AdminOperation(&ProfileStatusHandler {}),
    )?;

    // MinIO-compatible profiling / trace family (#606).
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/profiling/start").as_str(),
        AdminOperation(&ProfilingStartHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/profiling/download").as_str(),
        AdminOperation(&ProfilingDownloadHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/profile").as_str(),
        AdminOperation(&ProfileControlHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/trace").as_str(),
        AdminOperation(&TraceHandler {}),
    )?;

    Ok(())
}

/// Authorize a request against a single admin action (profiling or trace).
/// The pre-check keeps these endpoints' historical `AccessDenied` missing-credentials
/// response; the shared gate reports `InvalidRequest` "get cred failed".
async fn authorize_action(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    if req.credentials.is_none() {
        return Err(s3_error!(AccessDenied, "Signature is required"));
    }
    authorize_admin_request(req, vec![Action::AdminAction(action)]).await?;
    Ok(())
}

pub struct ProfileHandler {}

#[async_trait::async_trait]
impl Operation for ProfileHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_profile_request(&req).await?;

        let requested_url = req.uri.to_string();
        crate::profiling::log_cpu_pprof_dump_skipped();
        Ok(profile_not_implemented_response(format!(
            "{}; requested_url={requested_url}",
            crate::profiling::local_cpu_pprof_unsupported_message()
        )))
    }
}

pub struct ProfileStatusHandler {}

#[async_trait::async_trait]
impl Operation for ProfileStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_profile_request(&req).await?;

        let status = ProfileStatus {
            enabled: "false",
            status: "not_supported",
            platform: std::env::consts::OS,
            message: crate::profiling::LOCAL_CPU_PPROF_UNSUPPORTED_SUMMARY,
        };

        match serde_json::to_string(&status) {
            Ok(json) => {
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                Ok(S3Response::with_headers((StatusCode::OK, Body::from(json)), headers))
            }
            Err(e) => {
                error!("Failed to serialize status: {}", e);
                Ok(S3Response::new((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Body::from("Failed to serialize status".to_string()),
                )))
            }
        }
    }
}

/// `POST /v3/profiling/start` — begin a profiling session.
///
/// RustFS builds with the mimalloc allocator and ships no in-process pprof/CPU
/// sampler (see `crate::profiling`): profiling is exported out-of-process via
/// Pyroscope. We therefore honor the MinIO request shape but return
/// `501 Not Implemented` with a clear reason rather than pretending to have
/// started a capture that will never produce data.
pub struct ProfilingStartHandler {}

#[async_trait::async_trait]
impl Operation for ProfilingStartHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_action(&req, AdminAction::ProfilingAdminAction).await?;
        crate::profiling::log_cpu_pprof_dump_skipped();
        Ok(profile_not_implemented_response(format!(
            "profiling start is not supported: {}",
            crate::profiling::local_cpu_pprof_unsupported_message()
        )))
    }
}

/// `GET /v3/profiling/download` — download the captured profile archive.
///
/// No capture is ever produced (see `ProfilingStartHandler`), so there is
/// nothing to download.
pub struct ProfilingDownloadHandler {}

#[async_trait::async_trait]
impl Operation for ProfilingDownloadHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_action(&req, AdminAction::ProfilingAdminAction).await?;
        Ok(profile_not_implemented_response(format!(
            "profiling download is not supported: {}",
            crate::profiling::local_cpu_pprof_unsupported_message()
        )))
    }
}

/// `POST /v3/profile` — start/stop profiling in one call (legacy MinIO shape).
pub struct ProfileControlHandler {}

#[async_trait::async_trait]
impl Operation for ProfileControlHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_action(&req, AdminAction::ProfilingAdminAction).await?;
        crate::profiling::log_cpu_pprof_dump_skipped();
        Ok(profile_not_implemented_response(format!(
            "in-process profiling is not supported: {}",
            crate::profiling::local_cpu_pprof_unsupported_message()
        )))
    }
}

struct TraceStream {
    inner: ReceiverStream<Result<Bytes, StdError>>,
}

impl Stream for TraceStream {
    type Item = Result<Bytes, StdError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::into_inner(self).inner.poll_next_unpin(cx)
    }
}

impl ByteStream for TraceStream {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TraceKindFilter {
    heal: bool,
    scanner: bool,
}

impl TraceKindFilter {
    const ALL_SUPPORTED: Self = Self {
        heal: true,
        scanner: true,
    };

    fn from_request(uri: &hyper::Uri, trace_types: TraceType) -> S3Result<Self> {
        let mut has_kind = false;
        let mut filter = Self {
            heal: false,
            scanner: false,
        };

        for (key, value) in trace_query_pairs(uri) {
            if key != "kind" {
                continue;
            }
            has_kind = true;
            for item in value.split(',') {
                match item.trim().to_ascii_lowercase().as_str() {
                    "heal" | "healing" => filter.heal = true,
                    "scanner" => filter.scanner = true,
                    "all" => return Ok(Self::ALL_SUPPORTED),
                    _ => return Err(s3_error!(InvalidRequest, "invalid trace kind")),
                }
            }
        }

        if has_kind {
            return Ok(filter);
        }

        if trace_types.mask() == 0 || trace_query_flag(uri, "all") {
            return Ok(Self::ALL_SUPPORTED);
        }

        Ok(Self {
            heal: trace_types.overlaps(&TraceType::HEALING),
            scanner: trace_types.overlaps(&TraceType::SCANNER),
        })
    }

    const fn matches(self, kind: TraceKind) -> bool {
        match kind {
            TraceKind::Heal => self.heal,
            TraceKind::Scanner => self.scanner,
        }
    }
}

#[derive(Debug)]
struct TraceStreamFilter {
    kinds: TraceKindFilter,
    regex: Option<Regex>,
    threshold: Duration,
}

impl TraceStreamFilter {
    fn from_request(uri: &hyper::Uri, opts: &ServiceTraceOpts) -> S3Result<Self> {
        if opts.only_errors() {
            return Err(s3_error!(
                InvalidRequest,
                "trace error-only filter is not supported for heal/scanner trace"
            ));
        }

        Ok(Self {
            kinds: TraceKindFilter::from_request(uri, opts.trace_types())?,
            regex: trace_regex_filter(uri)?,
            threshold: opts.threshold(),
        })
    }

    fn matches_kind(&self, kind: TraceKind) -> bool {
        self.kinds.matches(kind)
    }

    fn matches_record(&self, record: &TraceWireRecord) -> bool {
        record.duration >= self.threshold && self.regex.as_ref().is_none_or(|regex| record.matches_regex(regex))
    }
}

#[derive(Serialize)]
struct TraceWireRecord {
    #[serde(rename = "type")]
    trace_type: u64,
    #[serde(rename = "nodename")]
    node_name: String,
    #[serde(rename = "funcname")]
    func_name: String,
    #[serde(rename = "time")]
    time: String,
    #[serde(rename = "path")]
    path: String,
    #[serde(rename = "dur")]
    duration: Duration,
    #[serde(rename = "bytes", skip_serializing_if = "Option::is_none")]
    bytes: Option<i64>,
    #[serde(rename = "msg", skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(rename = "custom", skip_serializing_if = "Option::is_none")]
    custom: Option<HashMap<String, String>>,
}

impl TraceWireRecord {
    fn from_event(node_name: &str, event: &TraceEvent) -> Self {
        Self {
            trace_type: trace_type_mask(event.kind),
            node_name: node_name.to_owned(),
            func_name: event.func.as_str().to_owned(),
            time: trace_time_string(event.time),
            path: trace_path(event),
            duration: event.duration,
            bytes: trace_bytes(event.bytes),
            message: None,
            custom: trace_custom_attrs(event),
        }
    }

    fn dropped(node_name: &str, dropped: u64) -> Self {
        let mut custom = HashMap::new();
        custom.insert("dropped_events".to_string(), dropped.to_string());

        Self {
            trace_type: 0,
            node_name: node_name.to_owned(),
            func_name: "trace.Dropped".to_string(),
            time: trace_time_string(SystemTime::now()),
            path: String::new(),
            duration: Duration::ZERO,
            bytes: None,
            message: Some("trace subscriber lagged".to_string()),
            custom: Some(custom),
        }
    }

    fn matches_regex(&self, regex: &Regex) -> bool {
        regex.is_match(&self.func_name)
            || regex.is_match(&self.path)
            || self.message.as_ref().is_some_and(|message| regex.is_match(message))
            || self
                .custom
                .as_ref()
                .is_some_and(|custom| custom.iter().any(|(key, value)| regex.is_match(key) || regex.is_match(value)))
    }
}

/// `GET /v3/trace` — stream real-time server trace events.
///
/// RustFS currently publishes heal and scanner diagnostics through the common
/// trace bus. The admin endpoint exposes those events as MinIO-shaped NDJSON
/// records while keeping unsupported trace classes filtered out.
pub struct TraceHandler {}

#[async_trait::async_trait]
impl Operation for TraceHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_action(&req, AdminAction::TraceAdminAction).await?;

        // Validate the trace options so malformed filters are rejected up front,
        // matching MinIO's behavior.
        let mut opts = ServiceTraceOpts::default();
        opts.parse_params(&req.uri)
            .map_err(|_| s3_error!(InvalidRequest, "invalid trace parameters"))?;
        let filter = TraceStreamFilter::from_request(&req.uri, &opts)?;

        let node_name = sysinfo::System::host_name().unwrap_or_else(|| "rustfs".to_string());
        let mut subscription = subscribe_trace_events();
        let (tx, rx) = mpsc::channel::<Result<Bytes, StdError>>(64);

        spawn_traced(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(15));
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = tx.closed() => break,
                    _ = ticker.tick() => {
                        if tx.send(Ok(Bytes::from_static(b" \n"))).await.is_err() {
                            break;
                        }
                    }
                    received = subscription.recv() => {
                        match received {
                            Ok(event) => {
                                if !filter.matches_kind(event.kind) {
                                    continue;
                                }
                                let record = TraceWireRecord::from_event(&node_name, &event);
                                if filter.matches_record(&record) && send_trace_record(&tx, &record).await.is_err() {
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                                let record = TraceWireRecord::dropped(&node_name, dropped);
                                if send_trace_record(&tx, &record).await.is_err() {
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
        });

        let stream: DynByteStream = Box::pin(TraceStream {
            inner: ReceiverStream::new(rx),
        });
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/x-ndjson"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(stream)), headers))
    }
}

async fn send_trace_record(tx: &mpsc::Sender<Result<Bytes, StdError>>, record: &TraceWireRecord) -> Result<(), ()> {
    let Some(encoded) = encode_ndjson(record) else {
        return Ok(());
    };
    tx.send(Ok(encoded)).await.map_err(|_| ())
}

fn encode_ndjson(value: &impl Serialize) -> Option<Bytes> {
    let mut encoded = serde_json::to_vec(value).ok()?;
    encoded.push(b'\n');
    Some(Bytes::from(encoded))
}

fn trace_query_pairs(uri: &hyper::Uri) -> impl Iterator<Item = (String, String)> + '_ {
    uri.query()
        .into_iter()
        .flat_map(|query| form_urlencoded::parse(query.as_bytes()))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
}

fn trace_query_flag(uri: &hyper::Uri, flag: &str) -> bool {
    trace_query_pairs(uri).any(|(key, value)| key == flag && value == "true")
}

fn trace_regex_filter(uri: &hyper::Uri) -> S3Result<Option<Regex>> {
    trace_query_pairs(uri)
        .find_map(|(key, value)| {
            if key == "filter" && !value.is_empty() {
                Some(value)
            } else {
                None
            }
        })
        .map(|pattern| Regex::new(&pattern).map_err(|_| s3_error!(InvalidRequest, "invalid trace filter")))
        .transpose()
}

fn trace_type_mask(kind: TraceKind) -> u64 {
    match kind {
        TraceKind::Heal => TraceType::HEALING.mask(),
        TraceKind::Scanner => TraceType::SCANNER.mask(),
    }
}

fn trace_time_string(time: SystemTime) -> String {
    match OffsetDateTime::from(time).format(&Rfc3339) {
        Ok(value) => value,
        Err(_) => "1970-01-01T00:00:00Z".to_string(),
    }
}

fn trace_path(event: &TraceEvent) -> String {
    match (event.bucket.as_deref(), event.object.as_deref()) {
        (Some(bucket), Some(object)) if !object.is_empty() => format!("{bucket}/{object}"),
        (Some(bucket), _) => bucket.to_owned(),
        (None, Some(object)) => object.to_owned(),
        (None, None) => String::new(),
    }
}

fn trace_bytes(bytes: u64) -> Option<i64> {
    if bytes == 0 {
        return None;
    }

    match i64::try_from(bytes) {
        Ok(value) => Some(value),
        Err(_) => Some(i64::MAX),
    }
}

fn trace_custom_attrs(event: &TraceEvent) -> Option<HashMap<String, String>> {
    if event.attrs.is_empty() {
        return None;
    }

    Some(
        event
            .attrs
            .iter()
            .map(|attr| (attr.key.to_string(), trace_value_string(&attr.value)))
            .collect(),
    )
}

fn trace_value_string(value: &TraceVal) -> String {
    match value {
        TraceVal::Bool(value) => value.to_string(),
        TraceVal::U64(value) => value.to_string(),
        TraceVal::I64(value) => value.to_string(),
        TraceVal::Str(value) => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ProfileControlHandler, ProfileHandler, ProfileStatusHandler, ProfilingDownloadHandler, ProfilingStartHandler,
        TraceHandler, TraceKindFilter, TraceStreamFilter, TraceWireRecord, authorize_action,
    };
    use crate::admin::router::Operation;
    use http::{Extensions, HeaderMap, Uri};
    use hyper::Method;
    use matchit::Params;
    use rustfs_common::trace_bus::{TraceEvent, TraceFunc, TraceKind};
    use rustfs_madmin::service_commands::ServiceTraceOpts;
    use rustfs_madmin::trace::TraceType;
    use rustfs_policy::policy::action::AdminAction;
    use s3s::{Body, S3ErrorCode, S3Request, S3Result};
    use std::time::{Duration, UNIX_EPOCH};

    fn build_profile_request(uri: &'static str) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: Uri::from_static(uri),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn build_trace_stream_filter(uri: &'static str) -> S3Result<TraceStreamFilter> {
        let uri = Uri::from_static(uri);
        let mut opts = ServiceTraceOpts::default();
        opts.parse_params(&uri).expect("test trace params should parse");
        TraceStreamFilter::from_request(&uri, &opts)
    }

    /// The profiling/trace endpoints authorize through the shared admin gate, which
    /// rejects a credential-less request with `InvalidRequest` "get cred failed". The
    /// pre-check keeps the `AccessDenied` response they have always returned
    /// (rustfs/backlog#1829).
    #[tokio::test]
    async fn profile_admin_gate_keeps_its_missing_credentials_response() {
        let err = authorize_action(
            &build_profile_request("/rustfs/admin/v3/profiling/start"),
            AdminAction::ProfilingAdminAction,
        )
        .await
        .expect_err("a request without credentials must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Signature is required"));
    }

    #[tokio::test]
    async fn profile_handler_rejects_missing_credentials() {
        let result = ProfileHandler {}
            .call(build_profile_request("/rustfs/admin/debug/pprof/profile?format=protobuf"), Params::new())
            .await;
        let err = match result {
            Ok(_) => panic!("profile handler must reject unauthenticated requests"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Signature is required"));
    }

    #[tokio::test]
    async fn profile_status_handler_rejects_missing_credentials() {
        let result = ProfileStatusHandler {}
            .call(build_profile_request("/rustfs/admin/debug/pprof/status"), Params::new())
            .await;
        let err = match result {
            Ok(_) => panic!("profile status handler must reject unauthenticated requests"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Signature is required"));
    }

    #[tokio::test]
    async fn profiling_start_rejects_missing_credentials() {
        let err = ProfilingStartHandler {}
            .call(build_profile_request("/rustfs/admin/v3/profiling/start"), Params::new())
            .await
            .expect_err("profiling start must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn profiling_download_rejects_missing_credentials() {
        let err = ProfilingDownloadHandler {}
            .call(build_profile_request("/rustfs/admin/v3/profiling/download"), Params::new())
            .await
            .expect_err("profiling download must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn profile_control_rejects_missing_credentials() {
        let err = ProfileControlHandler {}
            .call(build_profile_request("/rustfs/admin/v3/profile"), Params::new())
            .await
            .expect_err("profile control must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn trace_handler_rejects_missing_credentials() {
        let err = TraceHandler {}
            .call(build_profile_request("/rustfs/admin/v3/trace?s3=true"), Params::new())
            .await
            .expect_err("trace must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn trace_kind_filter_supports_kind_query() {
        let uri = Uri::from_static("/rustfs/admin/v3/trace?kind=heal");
        let filter = TraceKindFilter::from_request(&uri, TraceType::default()).expect("kind filter should parse");

        assert!(filter.matches(TraceKind::Heal));
        assert!(!filter.matches(TraceKind::Scanner));
    }

    #[test]
    fn trace_kind_filter_defaults_to_supported_events_without_type_flags() {
        let uri = Uri::from_static("/rustfs/admin/v3/trace");
        let filter = TraceKindFilter::from_request(&uri, TraceType::default()).expect("empty filter should parse");

        assert!(filter.matches(TraceKind::Heal));
        assert!(filter.matches(TraceKind::Scanner));
    }

    #[test]
    fn trace_kind_filter_rejects_unknown_kind() {
        let uri = Uri::from_static("/rustfs/admin/v3/trace?kind=s3");
        let err = TraceKindFilter::from_request(&uri, TraceType::default()).expect_err("unknown kind should fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn trace_stream_filter_matches_regex_against_path_and_attrs() {
        let filter = build_trace_stream_filter("/rustfs/admin/v3/trace?kind=heal&filter=data/.%2Bxl.meta")
            .expect("regex filter should parse");
        let event = TraceEvent::new(TraceKind::Heal, TraceFunc::HealObject)
            .with_bucket("data")
            .with_object("dir/xl.meta")
            .with_attr("dry_run", true);
        let record = TraceWireRecord::from_event("node-a", &event);

        assert!(filter.matches_kind(event.kind));
        assert!(filter.matches_record(&record));
    }

    #[test]
    fn trace_stream_filter_rejects_invalid_regex() {
        let err = build_trace_stream_filter("/rustfs/admin/v3/trace?kind=heal&filter=[").expect_err("invalid regex should fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn trace_stream_filter_applies_threshold() {
        let filter =
            build_trace_stream_filter("/rustfs/admin/v3/trace?kind=heal&threshold=10ms").expect("threshold should parse");
        let short = TraceWireRecord::from_event(
            "node-a",
            &TraceEvent::new(TraceKind::Heal, TraceFunc::HealObject).with_duration(Duration::from_millis(9)),
        );
        let long = TraceWireRecord::from_event(
            "node-a",
            &TraceEvent::new(TraceKind::Heal, TraceFunc::HealObject).with_duration(Duration::from_millis(10)),
        );

        assert!(!filter.matches_record(&short));
        assert!(filter.matches_record(&long));
    }

    #[test]
    fn trace_stream_filter_rejects_error_only_filter() {
        let err = build_trace_stream_filter("/rustfs/admin/v3/trace?kind=heal&err=true").expect_err("err filter should fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn trace_wire_record_contains_madmin_trace_fields() {
        let event = TraceEvent::new(TraceKind::Heal, TraceFunc::HealObject)
            .with_bucket("bucket")
            .with_object("object")
            .with_duration(Duration::from_millis(3))
            .with_bytes(17)
            .with_attr("dry", true);
        let mut record = TraceWireRecord::from_event("node-a", &event);
        record.time = "1970-01-01T00:00:00Z".to_string();

        let value = serde_json::to_value(&record).expect("trace record should serialize");

        assert_eq!(value["type"], TraceType::HEALING.mask());
        assert_eq!(value["nodename"], "node-a");
        assert_eq!(value["funcname"], "heal.Object");
        assert_eq!(value["time"], "1970-01-01T00:00:00Z");
        assert_eq!(value["path"], "bucket/object");
        assert_eq!(value["bytes"], 17);
        assert_eq!(value["custom"]["dry"], "true");
    }

    #[test]
    fn trace_wire_record_formats_epoch_time() {
        let event = TraceEvent {
            time: UNIX_EPOCH,
            ..TraceEvent::new(TraceKind::Scanner, TraceFunc::ScannerFolder)
        };
        let record = TraceWireRecord::from_event("node-a", &event);

        assert_eq!(record.time, "1970-01-01T00:00:00Z");
    }
}
