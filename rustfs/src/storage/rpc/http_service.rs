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

use crate::server::RPC_PREFIX;
use crate::storage::request_context::spawn_traced;
use crate::storage::storage_api::rpc_consumer::http_service::{
    DEFAULT_READ_BUFFER_SIZE, NS_SCANNER_PROTOCOL_VERSION, NsScannerCapabilityResponse, StorageDiskRpcExt as _,
    WALK_DIR_STREAM_COMPLETION_V1, WalkDirOptions, find_local_disk_by_ref, sign_ns_scanner_capability, verify_rpc_signature,
};
#[cfg(test)]
use crate::storage::storage_api::rpc_consumer::http_service::{
    NS_SCANNER_BODY_SHA256_QUERY, NS_SCANNER_CAPABILITY_CHALLENGE_QUERY, NS_SCANNER_CYCLE_QUERY, NS_SCANNER_LEADER_EPOCH_QUERY,
    NS_SCANNER_REQUEST_ID_QUERY, NS_SCANNER_SERVER_EPOCH_QUERY, NS_SCANNER_SESSION_ID_QUERY, NS_SCANNER_SESSION_SEQUENCE_QUERY,
    WALK_DIR_BODY_SHA256_QUERY,
};
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt, TryStreamExt, stream};
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_OPERATION_NS_SCANNER, INTERNODE_OPERATION_PUT_FILE_STREAM, INTERNODE_OPERATION_READ_FILE_STREAM,
    INTERNODE_OPERATION_WALK_DIR, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
};
use rustfs_utils::net::bytes_stream;
use s3s::Body;
use s3s::dto::StreamingBlob;
use serde::de::DeserializeOwned;
use serde_urlencoded::from_bytes;
use sha2::{Digest, Sha256};
use std::future::Future;
use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{self, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio_util::{io::ReaderStream, sync::CancellationToken};
use tower::Service;
use tracing::{error, warn};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
type RpcErrorResponse = Box<Response<Body>>;
const LOG_COMPONENT_INTERNODE_RPC: &str = "internode_rpc";
const LOG_SUBSYSTEM_FILE_TRANSFER: &str = "file_transfer";
const LOG_SUBSYSTEM_DIRECTORY_WALK: &str = "directory_walk";
const LOG_SUBSYSTEM_NAMESPACE_SCANNER: &str = "namespace_scanner";
const LOG_SUBSYSTEM_ROUTING: &str = "routing";
const EVENT_RPC_REQUEST_REJECTED: &str = "rpc_request_rejected";
const EVENT_RPC_REQUEST_FAILED: &str = "rpc_request_failed";
const EVENT_RPC_BACKGROUND_TASK_FAILED: &str = "rpc_background_task_failed";
const RPC_OPERATION_UNKNOWN: &str = "unknown";
const READ_FILE_STREAM_PATH: &str = "/rustfs/rpc/read_file_stream";
const PUT_FILE_STREAM_PATH: &str = "/rustfs/rpc/put_file_stream";
const WALK_DIR_PATH: &str = "/rustfs/rpc/walk_dir";
const NS_SCANNER_PATH: &str = "/rustfs/rpc/ns_scanner";
const NS_SCANNER_REQUEST_BODY_TIMEOUT: Duration = Duration::from_secs(15);
const NS_SCANNER_STREAM_BUFFER_SIZE: usize = 64 * 1024;
static NS_SCANNER_SERVER_EPOCH: LazyLock<uuid::Uuid> = LazyLock::new(uuid::Uuid::new_v4);

macro_rules! log_internode_rpc_response_failure {
    ($status:expr, $rpc_path:expr, $method:expr, $operation:expr, $reason:expr, $result:expr, Some(($context_key:expr, $context_value:expr)), Some($error_text:expr)) => {{
        let operation = $operation.unwrap_or(RPC_OPERATION_UNKNOWN);
        let subsystem = internode_rpc_subsystem(Some(operation));
        if $status.is_server_error() {
            error!(
                event = EVENT_RPC_REQUEST_FAILED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem,
                operation,
                result = $result,
                status_code = $status.as_u16(),
                rpc_path = $rpc_path,
                method = %$method,
                reason = $reason,
                $context_key = $context_value,
                error = %$error_text,
                "internode rpc request failed"
            );
        } else {
            warn!(
                event = EVENT_RPC_REQUEST_REJECTED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem,
                operation,
                result = $result,
                status_code = $status.as_u16(),
                rpc_path = $rpc_path,
                method = %$method,
                reason = $reason,
                $context_key = $context_value,
                error = %$error_text,
                "internode rpc request rejected"
            );
        }
    }};
    ($status:expr, $rpc_path:expr, $method:expr, $operation:expr, $reason:expr, $result:expr, Some(($context_key:expr, $context_value:expr)), None) => {{
        let operation = $operation.unwrap_or(RPC_OPERATION_UNKNOWN);
        let subsystem = internode_rpc_subsystem(Some(operation));
        if $status.is_server_error() {
            error!(
                event = EVENT_RPC_REQUEST_FAILED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem,
                operation,
                result = $result,
                status_code = $status.as_u16(),
                rpc_path = $rpc_path,
                method = %$method,
                reason = $reason,
                $context_key = $context_value,
                "internode rpc request failed"
            );
        } else {
            warn!(
                event = EVENT_RPC_REQUEST_REJECTED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem,
                operation,
                result = $result,
                status_code = $status.as_u16(),
                rpc_path = $rpc_path,
                method = %$method,
                reason = $reason,
                $context_key = $context_value,
                "internode rpc request rejected"
            );
        }
    }};
    ($status:expr, $rpc_path:expr, $method:expr, $operation:expr, $reason:expr, $result:expr, None, Some($error_text:expr)) => {{
        let operation = $operation.unwrap_or(RPC_OPERATION_UNKNOWN);
        let subsystem = internode_rpc_subsystem(Some(operation));
        if $status.is_server_error() {
            error!(
                event = EVENT_RPC_REQUEST_FAILED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem,
                operation,
                result = $result,
                status_code = $status.as_u16(),
                rpc_path = $rpc_path,
                method = %$method,
                reason = $reason,
                error = %$error_text,
                "internode rpc request failed"
            );
        } else {
            warn!(
                event = EVENT_RPC_REQUEST_REJECTED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem,
                operation,
                result = $result,
                status_code = $status.as_u16(),
                rpc_path = $rpc_path,
                method = %$method,
                reason = $reason,
                error = %$error_text,
                "internode rpc request rejected"
            );
        }
    }};
    ($status:expr, $rpc_path:expr, $method:expr, $operation:expr, $reason:expr, $result:expr, None, None) => {{
        let operation = $operation.unwrap_or(RPC_OPERATION_UNKNOWN);
        let subsystem = internode_rpc_subsystem(Some(operation));
        if $status.is_server_error() {
            error!(
                event = EVENT_RPC_REQUEST_FAILED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem,
                operation,
                result = $result,
                status_code = $status.as_u16(),
                rpc_path = $rpc_path,
                method = %$method,
                reason = $reason,
                "internode rpc request failed"
            );
        } else {
            warn!(
                event = EVENT_RPC_REQUEST_REJECTED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem,
                operation,
                result = $result,
                status_code = $status.as_u16(),
                rpc_path = $rpc_path,
                method = %$method,
                reason = $reason,
                "internode rpc request rejected"
            );
        }
    }};
}

macro_rules! log_internode_put_file_stage_failure {
    ($stage:expr, $query:expr, $err:expr) => {
        error!(
            event = EVENT_RPC_REQUEST_FAILED,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_FILE_TRANSFER,
            operation = INTERNODE_OPERATION_PUT_FILE_STREAM,
            result = "failed",
            status_code = StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            rpc_path = PUT_FILE_STREAM_PATH,
            method = %Method::PUT,
            reason = "put_file_stage_failed",
            stage = $stage,
            disk = %$query.disk,
            volume = %$query.volume,
            path = %$query.path,
            append = $query.append,
            size = $query.size,
            error = %$err,
            "internode rpc request failed"
        );
    };
}

#[derive(Clone)]
pub struct InternodeRpcService<S> {
    inner: S,
}

impl<S> InternodeRpcService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

#[derive(Debug, Default, serde::Deserialize)]
struct ReadFileQuery {
    disk: String,
    volume: String,
    path: String,
    offset: usize,
    length: usize,
}

#[derive(Debug, Default, serde::Deserialize)]
struct WalkDirQuery {
    disk: String,
    walk_dir_stream_completion: Option<String>,
    walk_dir_body_sha256: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NsScannerQuery {
    disk: String,
    ns_scanner_request_id: uuid::Uuid,
    ns_scanner_server_epoch: uuid::Uuid,
    ns_scanner_session_id: uuid::Uuid,
    ns_scanner_session_sequence: u64,
    ns_scanner_cycle: u64,
    ns_scanner_leader_epoch: u64,
    ns_scanner_body_sha256: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NsScannerCapabilityQuery {
    ns_scanner_protocol: Option<u16>,
    ns_scanner_challenge: Option<uuid::Uuid>,
}

fn verify_ns_scanner_body_digest(query: &NsScannerQuery, body: &[u8]) -> bool {
    let Some(expected) = query.ns_scanner_body_sha256.as_deref() else {
        return false;
    };
    let actual = hex_simd::encode_to_string(Sha256::digest(body), hex_simd::AsciiCase::Lower);
    expected == actual
}

fn supports_walk_dir_stream_completion(query: &WalkDirQuery) -> bool {
    query.walk_dir_stream_completion.as_deref() == Some(WALK_DIR_STREAM_COMPLETION_V1)
}

fn verify_walk_dir_body_digest(query: &WalkDirQuery, body: &[u8]) -> bool {
    if !supports_walk_dir_stream_completion(query) {
        return true;
    }

    let Some(expected) = query.walk_dir_body_sha256.as_deref() else {
        return false;
    };
    let actual = hex_simd::encode_to_string(Sha256::digest(body), hex_simd::AsciiCase::Lower);
    expected == actual
}

fn validate_walk_dir_completion_request(query: &WalkDirQuery, body: &[u8]) -> Option<bool> {
    let propagate_completion_errors = supports_walk_dir_stream_completion(query);
    if !verify_walk_dir_body_digest(query, body) {
        return None;
    }
    Some(propagate_completion_errors)
}

#[derive(Debug, Default, serde::Deserialize)]
struct PutFileQuery {
    disk: String,
    volume: String,
    path: String,
    append: bool,
    size: i64,
}

impl<S> Service<Request<Incoming>> for InternodeRpcService<S>
where
    S: Service<Request<Incoming>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        if !is_internode_rpc_path(req.uri().path()) {
            let mut inner = self.inner.clone();
            return Box::pin(async move { inner.call(req).await });
        }

        Box::pin(async move { Ok(handle_internode_rpc(req).await) })
    }
}

fn is_internode_rpc_path(path: &str) -> bool {
    path.starts_with(RPC_PREFIX)
}

async fn handle_internode_rpc(req: Request<Incoming>) -> Response<Body> {
    let operation = internode_http_operation(req.uri().path());
    let started_at = Instant::now();
    if let Err(response) = verify_internode_rpc_signature(req.uri(), req.method(), req.headers()) {
        record_internode_rpc_error(operation);
        return *response;
    }

    let method = req.method().clone();
    let path = req.uri().path();

    let response = match (method, path) {
        (Method::GET, READ_FILE_STREAM_PATH) | (Method::HEAD, READ_FILE_STREAM_PATH) => handle_read_file(req).await,
        (Method::GET, WALK_DIR_PATH) | (Method::HEAD, WALK_DIR_PATH) => handle_walk_dir(req).await,
        (Method::GET, NS_SCANNER_PATH) => match parse_query::<NsScannerCapabilityQuery>(&req) {
            Ok(query) if query.ns_scanner_protocol == Some(NS_SCANNER_PROTOCOL_VERSION) => match query.ns_scanner_challenge {
                Some(challenge) if !challenge.is_nil() => ns_scanner_capability_response(challenge),
                Some(_) | None => response_with_status(StatusCode::BAD_REQUEST, "namespace scanner challenge is invalid"),
            },
            Ok(_) => response_with_status(StatusCode::UPGRADE_REQUIRED, "namespace scanner protocol is unsupported"),
            Err(response) => *response,
        },
        (Method::POST, NS_SCANNER_PATH) => handle_ns_scanner(req).await,
        (Method::PUT, PUT_FILE_STREAM_PATH) => handle_put_file(req).await,
        _ => response_with_status(StatusCode::NOT_FOUND, "internode rpc route not found"),
    };

    if !response.status().is_success() {
        record_internode_rpc_error(operation);
    }

    if let Some(operation) = operation {
        runtime_sources::current_internode_metrics().record_duration_for_operation_and_backend(
            operation,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            started_at.elapsed(),
        );
    }

    response
}

fn internode_http_operation(path: &str) -> Option<&'static str> {
    match path {
        READ_FILE_STREAM_PATH => Some(INTERNODE_OPERATION_READ_FILE_STREAM),
        PUT_FILE_STREAM_PATH => Some(INTERNODE_OPERATION_PUT_FILE_STREAM),
        WALK_DIR_PATH => Some(INTERNODE_OPERATION_WALK_DIR),
        NS_SCANNER_PATH => Some(INTERNODE_OPERATION_NS_SCANNER),
        _ => None,
    }
}

fn record_internode_rpc_error(operation: Option<&'static str>) {
    let metrics = runtime_sources::current_internode_metrics();
    match operation {
        Some(operation) => metrics.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP),
        None => metrics.record_error(),
    }
}

fn ns_scanner_capability_response(challenge: uuid::Uuid) -> Response<Body> {
    let server_epoch = *NS_SCANNER_SERVER_EPOCH;
    let proof = match sign_ns_scanner_capability(challenge, server_epoch) {
        Ok(proof) => proof,
        Err(err) => {
            error!(
                event = EVENT_RPC_REQUEST_FAILED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
                operation = INTERNODE_OPERATION_NS_SCANNER,
                result = "failed",
                status_code = StatusCode::UPGRADE_REQUIRED.as_u16(),
                rpc_path = NS_SCANNER_PATH,
                method = %Method::GET,
                reason = "capability_authentication_unavailable",
                error = %err,
                "internode rpc request failed"
            );
            return response_with_status(StatusCode::UPGRADE_REQUIRED, "namespace scanner RPC authentication is unavailable");
        }
    };
    let body = match rmp_serde::to_vec_named(&NsScannerCapabilityResponse {
        version: NS_SCANNER_PROTOCOL_VERSION,
        server_epoch,
        proof,
    }) {
        Ok(body) => body,
        Err(err) => {
            error!(
                event = EVENT_RPC_REQUEST_FAILED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
                operation = INTERNODE_OPERATION_NS_SCANNER,
                result = "failed",
                status_code = StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                rpc_path = NS_SCANNER_PATH,
                method = %Method::GET,
                reason = "capability_response_encode_failed",
                error = %err,
                "internode rpc request failed"
            );
            return response_with_status(
                StatusCode::INTERNAL_SERVER_ERROR,
                "namespace scanner capability response encoding failed",
            );
        }
    };
    let mut response = Response::new(Body::from(Bytes::from(body)));
    response
        .headers_mut()
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/msgpack"));
    response
}

fn ns_scanner_server_epoch_matches(server_epoch: uuid::Uuid) -> bool {
    server_epoch == *NS_SCANNER_SERVER_EPOCH
}

fn verify_internode_rpc_signature(uri: &Uri, method: &Method, headers: &HeaderMap) -> Result<(), RpcErrorResponse> {
    if method == Method::HEAD {
        return Ok(());
    }

    verify_rpc_signature(&uri.to_string(), method, headers).map_err(|e| {
        let message = format!("rpc signature verification failed: {e}");
        log_internode_rpc_response_failure!(
            StatusCode::FORBIDDEN,
            uri.path(),
            method,
            internode_http_operation(uri.path()),
            "signature_verification_failed",
            "rejected",
            None,
            Some(&e)
        );
        Box::new(response_with_status(StatusCode::FORBIDDEN, message))
    })
}

async fn handle_read_file(req: Request<Incoming>) -> Response<Body> {
    if req.method() == Method::HEAD {
        return empty_ok();
    }

    let query = match parse_query::<ReadFileQuery>(&req) {
        Ok(query) => query,
        Err(response) => return *response,
    };

    let Some(disk) = find_local_disk_by_ref(&query.disk).await else {
        warn!(
            event = EVENT_RPC_REQUEST_REJECTED,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_FILE_TRANSFER,
            operation = INTERNODE_OPERATION_READ_FILE_STREAM,
            result = "rejected",
            status_code = StatusCode::BAD_REQUEST.as_u16(),
            rpc_path = req.uri().path(),
            method = %req.method(),
            reason = "disk_not_found",
            disk = %query.disk,
            volume = %query.volume,
            path = %query.path,
            offset = query.offset,
            length = query.length,
            "internode rpc request rejected"
        );
        return response_with_status(StatusCode::BAD_REQUEST, "disk not found");
    };

    let file = match disk
        .read_file_stream(&query.volume, &query.path, query.offset, query.length)
        .await
    {
        Ok(file) => file,
        Err(e) => {
            let message = format!("read file err {e}");
            error!(
                event = EVENT_RPC_REQUEST_FAILED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_FILE_TRANSFER,
                operation = INTERNODE_OPERATION_READ_FILE_STREAM,
                result = "failed",
                status_code = StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                rpc_path = req.uri().path(),
                method = %req.method(),
                reason = "read_file_failed",
                disk = %query.disk,
                volume = %query.volume,
                path = %query.path,
                offset = query.offset,
                length = query.length,
                error = %e,
                "internode rpc request failed"
            );
            return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, message);
        }
    };

    runtime_sources::current_internode_metrics().record_incoming_request_for_operation_and_backend(
        INTERNODE_OPERATION_READ_FILE_STREAM,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
    );
    let stream = read_file_body_stream(file, query.length, INTERNODE_OPERATION_READ_FILE_STREAM);

    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(StreamingBlob::wrap(stream)))
        .expect("failed to build read file stream response")
}

fn read_file_body_stream<R>(
    reader: R,
    length: usize,
    operation: &'static str,
) -> Pin<Box<dyn futures::Stream<Item = io::Result<Bytes>> + Send + Sync>>
where
    R: tokio::io::AsyncRead + Unpin + Send + Sync + 'static,
{
    let metrics = runtime_sources::current_internode_metrics();
    let stream = ReaderStream::with_capacity(reader, DEFAULT_READ_BUFFER_SIZE).map_ok(move |bytes| {
        metrics.record_sent_bytes_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP, bytes.len());
        bytes
    });

    if length == 0 {
        Box::pin(stream)
    } else {
        Box::pin(bytes_stream(stream, length))
    }
}

async fn handle_walk_dir(req: Request<Incoming>) -> Response<Body> {
    if req.method() == Method::HEAD {
        return empty_ok();
    }

    let query = match parse_query::<WalkDirQuery>(&req) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let Some(disk) = find_local_disk_by_ref(&query.disk).await else {
        warn!(
            event = EVENT_RPC_REQUEST_REJECTED,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_DIRECTORY_WALK,
            operation = INTERNODE_OPERATION_WALK_DIR,
            result = "rejected",
            status_code = StatusCode::BAD_REQUEST.as_u16(),
            rpc_path = req.uri().path(),
            method = %req.method(),
            reason = "disk_not_found",
            disk = %query.disk,
            "internode rpc request rejected"
        );
        return response_with_status(StatusCode::BAD_REQUEST, "disk not found");
    };

    let body = match Limited::new(req.into_body(), MAX_ADMIN_REQUEST_BODY_SIZE).collect().await {
        Ok(body) => body.to_bytes(),
        Err(e) => {
            let message = format!("read body err {e}");
            log_internode_rpc_response_failure!(
                StatusCode::PAYLOAD_TOO_LARGE,
                WALK_DIR_PATH,
                &Method::GET,
                Some(INTERNODE_OPERATION_WALK_DIR),
                "request_body_read_failed",
                "rejected",
                Some(("disk", query.disk.as_str())),
                Some(&e)
            );
            return response_with_status(StatusCode::PAYLOAD_TOO_LARGE, message);
        }
    };
    // RUSTFS_COMPAT_TODO(#4648): old clients retry terminal stream failures on an already-used writer.
    // Remove after every supported peer version advertises walk-dir stream completion v1.
    let propagate_completion_errors = match validate_walk_dir_completion_request(&query, &body) {
        Some(propagate_completion_errors) => propagate_completion_errors,
        None => {
            warn!(
                event = EVENT_RPC_REQUEST_REJECTED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_DIRECTORY_WALK,
                operation = INTERNODE_OPERATION_WALK_DIR,
                result = "rejected",
                status_code = StatusCode::FORBIDDEN.as_u16(),
                rpc_path = WALK_DIR_PATH,
                method = %Method::GET,
                reason = "request_body_digest_mismatch",
                "internode rpc request rejected"
            );
            return response_with_status(StatusCode::FORBIDDEN, "invalid request body digest");
        }
    };

    let args: WalkDirOptions = match serde_json::from_slice(&body) {
        Ok(args) => args,
        Err(e) => {
            let message = format!("unmarshal body err {e}");
            warn!(
                event = EVENT_RPC_REQUEST_REJECTED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_DIRECTORY_WALK,
                operation = INTERNODE_OPERATION_WALK_DIR,
                result = "rejected",
                status_code = StatusCode::BAD_REQUEST.as_u16(),
                rpc_path = WALK_DIR_PATH,
                method = %Method::GET,
                reason = "request_body_decode_failed",
                disk = %query.disk,
                error = %e,
                "internode rpc request rejected"
            );
            return response_with_status(StatusCode::BAD_REQUEST, message);
        }
    };

    let log_disk = query.disk.clone();
    let log_bucket = args.bucket.clone();
    let log_base_dir = args.base_dir.clone();
    let log_recursive = args.recursive;
    let log_report_notfound = args.report_notfound;
    let log_filter_prefix = args.filter_prefix.clone();
    let log_forward_to = args.forward_to.clone();
    let log_limit = args.limit;
    let log_disk_id = args.disk_id.clone();
    let log_skip_total_timeout = args.skip_total_timeout;
    let body = walk_dir_response_body(propagate_completion_errors, move |mut writer| async move {
        disk.walk_dir(args, &mut writer).await.map_err(|e| {
            warn!(
                event = EVENT_RPC_BACKGROUND_TASK_FAILED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_DIRECTORY_WALK,
                operation = INTERNODE_OPERATION_WALK_DIR,
                result = "failed",
                disk = %log_disk,
                bucket = %log_bucket,
                base_dir = %log_base_dir,
                recursive = log_recursive,
                report_notfound = log_report_notfound,
                filter_prefix = ?log_filter_prefix,
                forward_to = ?log_forward_to,
                limit = log_limit,
                disk_id = %log_disk_id,
                skip_total_timeout = log_skip_total_timeout,
                error = %e,
                "internode rpc background task failed"
            );
            io::Error::other("remote walk_dir failed")
        })
    });

    runtime_sources::current_internode_metrics()
        .record_incoming_request_for_operation_and_backend(INTERNODE_OPERATION_WALK_DIR, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP);

    Response::builder()
        .status(StatusCode::OK)
        .body(body)
        .expect("failed to build walk dir response")
}

async fn handle_ns_scanner(req: Request<Incoming>) -> Response<Body> {
    let query = match parse_query::<NsScannerQuery>(&req) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    if !ns_scanner_server_epoch_matches(query.ns_scanner_server_epoch) {
        warn!(
            event = EVENT_RPC_REQUEST_REJECTED,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
            operation = INTERNODE_OPERATION_NS_SCANNER,
            result = "rejected",
            status_code = StatusCode::CONFLICT.as_u16(),
            rpc_path = NS_SCANNER_PATH,
            method = %Method::POST,
            reason = "server_epoch_mismatch",
            "internode rpc request rejected"
        );
        return response_with_status(StatusCode::CONFLICT, "namespace scanner server epoch is stale");
    }
    let Some(disk) = find_local_disk_by_ref(&query.disk).await else {
        warn!(
            event = EVENT_RPC_REQUEST_REJECTED,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
            operation = INTERNODE_OPERATION_NS_SCANNER,
            result = "rejected",
            status_code = StatusCode::BAD_REQUEST.as_u16(),
            rpc_path = NS_SCANNER_PATH,
            method = %Method::POST,
            reason = "disk_not_found",
            disk = %query.disk,
            "internode rpc request rejected"
        );
        return response_with_status(StatusCode::BAD_REQUEST, "disk not found");
    };
    if let Err(err) = rustfs_scanner::preflight_remote_scanner_request(
        disk.as_ref(),
        query.ns_scanner_session_id,
        query.ns_scanner_cycle,
        query.ns_scanner_leader_epoch,
        query.ns_scanner_session_sequence,
    ) {
        let (status, reason, message) = remote_scanner_claim_rejection(&err);
        warn!(
            event = EVENT_RPC_REQUEST_REJECTED,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
            operation = INTERNODE_OPERATION_NS_SCANNER,
            result = "rejected",
            status_code = status.as_u16(),
            rpc_path = NS_SCANNER_PATH,
            method = %Method::POST,
            reason,
            disk = %query.disk,
            error = %err,
            "internode rpc request rejected"
        );
        return response_with_status(status, message);
    }
    if let Err(err) =
        rustfs_scanner::validate_remote_scanner_request_fence(query.ns_scanner_cycle, query.ns_scanner_leader_epoch).await
    {
        warn!(
            event = EVENT_RPC_REQUEST_REJECTED,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
            operation = INTERNODE_OPERATION_NS_SCANNER,
            result = "rejected",
            status_code = StatusCode::CONFLICT.as_u16(),
            rpc_path = NS_SCANNER_PATH,
            method = %Method::POST,
            reason = "scanner_cycle_mismatch",
            disk = %query.disk,
            error = %err,
            "internode rpc request rejected"
        );
        return response_with_status(StatusCode::CONFLICT, "namespace scanner cycle does not match persisted state");
    }
    let body = match tokio::time::timeout(
        NS_SCANNER_REQUEST_BODY_TIMEOUT,
        Limited::new(req.into_body(), rustfs_scanner::NS_SCANNER_MAX_REQUEST_BODY_SIZE).collect(),
    )
    .await
    {
        Ok(Ok(body)) => body.to_bytes(),
        Ok(Err(err)) => {
            log_internode_rpc_response_failure!(
                StatusCode::PAYLOAD_TOO_LARGE,
                NS_SCANNER_PATH,
                &Method::POST,
                Some(INTERNODE_OPERATION_NS_SCANNER),
                "request_body_read_failed",
                "rejected",
                Some(("disk", query.disk.as_str())),
                Some(&err)
            );
            return response_with_status(StatusCode::PAYLOAD_TOO_LARGE, "namespace scanner request body is too large");
        }
        Err(_) => {
            warn!(
                event = EVENT_RPC_REQUEST_REJECTED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
                operation = INTERNODE_OPERATION_NS_SCANNER,
                result = "rejected",
                status_code = StatusCode::REQUEST_TIMEOUT.as_u16(),
                rpc_path = NS_SCANNER_PATH,
                method = %Method::POST,
                reason = "request_body_timeout",
                disk = %query.disk,
                "internode rpc request rejected"
            );
            return response_with_status(StatusCode::REQUEST_TIMEOUT, "namespace scanner request body timed out");
        }
    };

    if !verify_ns_scanner_body_digest(&query, &body) {
        warn!(
            event = EVENT_RPC_REQUEST_REJECTED,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
            operation = INTERNODE_OPERATION_NS_SCANNER,
            result = "rejected",
            status_code = StatusCode::FORBIDDEN.as_u16(),
            rpc_path = NS_SCANNER_PATH,
            method = %Method::POST,
            reason = "request_body_digest_mismatch",
            disk = %query.disk,
            "internode rpc request rejected"
        );
        return response_with_status(StatusCode::FORBIDDEN, "invalid request body digest");
    }

    let request = match rustfs_scanner::decode_remote_scanner_request(&body) {
        Ok(request) => request,
        Err(err) => {
            warn!(
                event = EVENT_RPC_REQUEST_REJECTED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
                operation = INTERNODE_OPERATION_NS_SCANNER,
                result = "rejected",
                status_code = StatusCode::BAD_REQUEST.as_u16(),
                rpc_path = NS_SCANNER_PATH,
                method = %Method::POST,
                reason = "request_body_decode_failed",
                disk = %query.disk,
                error = %err,
                "internode rpc request rejected"
            );
            return response_with_status(StatusCode::BAD_REQUEST, "invalid namespace scanner request");
        }
    };
    if !rustfs_scanner::remote_scanner_request_matches_envelope(
        &request,
        query.ns_scanner_request_id,
        query.ns_scanner_server_epoch,
        query.ns_scanner_session_id,
        query.ns_scanner_session_sequence,
        query.ns_scanner_cycle,
        query.ns_scanner_leader_epoch,
    ) {
        warn!(
            event = EVENT_RPC_REQUEST_REJECTED,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
            operation = INTERNODE_OPERATION_NS_SCANNER,
            result = "rejected",
            status_code = StatusCode::FORBIDDEN.as_u16(),
            rpc_path = NS_SCANNER_PATH,
            method = %Method::POST,
            reason = "request_envelope_mismatch",
            disk = %query.disk,
            "internode rpc request rejected"
        );
        return response_with_status(StatusCode::FORBIDDEN, "namespace scanner request envelope mismatch");
    }
    let admission = match rustfs_scanner::admit_remote_scanner_request(disk.as_ref()) {
        Ok(admission) => admission,
        Err(rustfs_scanner::ScannerError::RemoteDiskBusy) => {
            warn!(
                event = EVENT_RPC_REQUEST_REJECTED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
                operation = INTERNODE_OPERATION_NS_SCANNER,
                result = "rejected",
                status_code = StatusCode::TOO_MANY_REQUESTS.as_u16(),
                rpc_path = NS_SCANNER_PATH,
                method = %Method::POST,
                reason = "disk_scan_already_active",
                disk = %query.disk,
                "internode rpc request rejected"
            );
            return response_with_status(StatusCode::TOO_MANY_REQUESTS, "namespace scanner disk is already active");
        }
        Err(err) => {
            let (status, reason, message) = remote_scanner_claim_rejection(&err);
            warn!(
                event = EVENT_RPC_REQUEST_REJECTED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
                operation = INTERNODE_OPERATION_NS_SCANNER,
                result = "rejected",
                status_code = status.as_u16(),
                rpc_path = NS_SCANNER_PATH,
                method = %Method::POST,
                reason,
                disk = %query.disk,
                error = %err,
                "internode rpc request rejected"
            );
            return response_with_status(status, message);
        }
    };
    if let Err(err) = rustfs_scanner::claim_remote_scanner_request(
        disk.as_ref(),
        query.ns_scanner_session_id,
        query.ns_scanner_cycle,
        query.ns_scanner_leader_epoch,
        query.ns_scanner_session_sequence,
    ) {
        let (status, reason, message) = remote_scanner_claim_rejection(&err);
        warn!(
            event = EVENT_RPC_REQUEST_REJECTED,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
            operation = INTERNODE_OPERATION_NS_SCANNER,
            result = "rejected",
            status_code = status.as_u16(),
            rpc_path = NS_SCANNER_PATH,
            method = %Method::POST,
            reason,
            disk = %query.disk,
            error = %err,
            "internode rpc request rejected"
        );
        return response_with_status(status, message);
    }

    let metrics = runtime_sources::current_internode_metrics();
    metrics
        .record_incoming_request_for_operation_and_backend(INTERNODE_OPERATION_NS_SCANNER, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP);
    metrics.record_recv_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_NS_SCANNER,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        body.len(),
    );

    let log_disk = query.disk;
    let response_body = ns_scanner_response_body(move |writer, disconnect| async move {
        let _admission = admission;
        rustfs_scanner::serve_remote_scanner_request(disk, request, writer, disconnect)
            .await
            .map_err(|err| {
                error!(
                    event = EVENT_RPC_BACKGROUND_TASK_FAILED,
                    component = LOG_COMPONENT_INTERNODE_RPC,
                    subsystem = LOG_SUBSYSTEM_NAMESPACE_SCANNER,
                    operation = INTERNODE_OPERATION_NS_SCANNER,
                    result = "failed",
                    disk = %log_disk,
                    error = %err,
                    "internode rpc background task failed"
                );
                io::Error::other("remote namespace scanner failed")
            })
    });

    let mut response = Response::new(response_body);
    response
        .headers_mut()
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/msgpack"));
    response
}

fn remote_scanner_claim_rejection(error: &rustfs_scanner::ScannerError) -> (StatusCode, &'static str, &'static str) {
    match error {
        rustfs_scanner::ScannerError::RemoteRequestReplay => {
            (StatusCode::CONFLICT, "request_replay", "namespace scanner request was already accepted")
        }
        rustfs_scanner::ScannerError::RemoteReplayCapacity => (
            StatusCode::TOO_MANY_REQUESTS,
            "replay_capacity",
            "namespace scanner request capacity is temporarily exhausted",
        ),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            "replay_state_unavailable",
            "namespace scanner replay state is unavailable",
        ),
    }
}

fn walk_dir_response_body<F, Fut>(propagate_completion_errors: bool, producer: F) -> Body
where
    F: FnOnce(tokio::io::DuplexStream) -> Fut + Send + 'static,
    Fut: Future<Output = io::Result<()>> + Send + 'static,
{
    let (reader, writer) = tokio::io::duplex(DEFAULT_READ_BUFFER_SIZE);
    let (mut completion_tx, completion_rx) = oneshot::channel();
    spawn_traced(async move {
        tokio::select! {
            biased;
            result = producer(writer) => {
                let _ = completion_tx.send(result);
            }
            _ = completion_tx.closed() => {}
        }
    });

    let metrics = runtime_sources::current_internode_metrics();
    let stream = ReaderStream::with_capacity(reader, DEFAULT_READ_BUFFER_SIZE).map_ok(move |bytes| {
        metrics.record_sent_bytes_for_operation_and_backend(
            INTERNODE_OPERATION_WALK_DIR,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            bytes.len(),
        );
        bytes
    });
    let stream = append_walk_dir_completion(stream, completion_rx, propagate_completion_errors);

    Body::from(StreamingBlob::wrap(stream))
}

fn append_walk_dir_completion<S>(
    stream: S,
    completion_rx: oneshot::Receiver<io::Result<()>>,
    propagate_completion_errors: bool,
) -> impl Stream<Item = io::Result<Bytes>>
where
    S: Stream<Item = io::Result<Bytes>>,
{
    stream.chain(
        stream::once(async move {
            match completion_rx.await {
                Ok(Ok(())) => None,
                Ok(Err(err)) if propagate_completion_errors => Some(Err(err)),
                Err(err) if propagate_completion_errors => {
                    Some(Err(io::Error::other(format!("remote walk_dir task ended without a result: {err}"))))
                }
                Ok(Err(_)) | Err(_) => None,
            }
        })
        .filter_map(std::future::ready),
    )
}

fn ns_scanner_response_body<F, Fut>(producer: F) -> Body
where
    F: FnOnce(tokio::io::DuplexStream, CancellationToken) -> Fut + Send + 'static,
    Fut: Future<Output = io::Result<()>> + Send + 'static,
{
    let (reader, writer) = tokio::io::duplex(NS_SCANNER_STREAM_BUFFER_SIZE);
    let (mut completion_tx, completion_rx) = oneshot::channel();
    let disconnect = CancellationToken::new();
    spawn_traced(async move {
        let producer = producer(writer, disconnect.clone());
        tokio::pin!(producer);
        tokio::select! {
            biased;
            result = &mut producer => {
                let _ = completion_tx.send(result);
            }
            _ = completion_tx.closed() => {
                disconnect.cancel();
                let _ = producer.await;
            }
        }
    });

    let metrics = runtime_sources::current_internode_metrics();
    let stream = ReaderStream::with_capacity(reader, NS_SCANNER_STREAM_BUFFER_SIZE).map_ok(move |bytes| {
        metrics.record_sent_bytes_for_operation_and_backend(
            INTERNODE_OPERATION_NS_SCANNER,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            bytes.len(),
        );
        bytes
    });
    let completion = stream::once(async move {
        match completion_rx.await {
            Ok(Ok(())) => None,
            Ok(Err(err)) => Some(Err(err)),
            Err(err) => Some(Err(io::Error::other(format!(
                "remote namespace scanner task ended without a result: {err}"
            )))),
        }
    })
    .filter_map(std::future::ready);

    Body::from(StreamingBlob::wrap(stream.chain(completion)))
}

async fn handle_put_file(req: Request<Incoming>) -> Response<Body> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = match parse_query::<PutFileQuery>(&req) {
        Ok(query) => query,
        Err(response) => return *response,
    };

    let Some(disk) = find_local_disk_by_ref(&query.disk).await else {
        log_internode_rpc_response_failure!(
            StatusCode::BAD_REQUEST,
            &path,
            &method,
            Some(INTERNODE_OPERATION_PUT_FILE_STREAM),
            "disk_not_found",
            "rejected",
            Some(("disk", query.disk.as_str())),
            None
        );
        return response_with_status(StatusCode::BAD_REQUEST, "disk not found");
    };

    let mut file = if query.append {
        match disk.append_file(&query.volume, &query.path).await {
            Ok(file) => file,
            Err(e) => {
                let message = put_file_stage_error_message("append", &query, &e);
                log_internode_put_file_stage_failure!("append", query, e);
                return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, message);
            }
        }
    } else {
        match disk.create_file("", &query.volume, &query.path, query.size).await {
            Ok(file) => file,
            Err(e) => {
                let message = put_file_stage_error_message("create", &query, &e);
                log_internode_put_file_stage_failure!("create", query, e);
                return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, message);
            }
        }
    };

    let copied = match write_body_chunks_to_writer(req.into_body().into_data_stream(), &mut file).await {
        Ok(copied) => copied,
        Err(e) => {
            let message = put_file_stage_error_message("write_body", &query, &e);
            log_internode_put_file_stage_failure!("write_body", query, e);
            return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, message);
        }
    };

    let metrics = runtime_sources::current_internode_metrics();
    metrics.record_incoming_request_for_operation_and_backend(
        INTERNODE_OPERATION_PUT_FILE_STREAM,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
    );
    metrics.record_recv_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_PUT_FILE_STREAM,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        usize::try_from(copied).unwrap_or(usize::MAX),
    );

    if put_body_size_mismatch(&query, copied) {
        let err = std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("body size mismatch: expected {} bytes, received {copied}", query.size),
        );
        let message = put_file_stage_error_message("verify_size", &query, &err);
        log_internode_put_file_stage_failure!("verify_size", query, err);
        return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, message);
    }

    if let Err(e) = file.flush().await {
        let message = put_file_stage_error_message("flush", &query, &e);
        log_internode_put_file_stage_failure!("flush", query, e);
        return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, message);
    }

    empty_ok()
}

async fn write_body_chunks_to_writer<S, E, W>(body: S, writer: &mut W) -> io::Result<u64>
where
    S: futures::TryStream<Ok = Bytes, Error = E> + Unpin,
    E: Into<BoxError>,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut body = body;
    let mut copied = 0_u64;
    let mut pending = BytesMut::with_capacity(DEFAULT_READ_BUFFER_SIZE);

    while let Some(bytes) = body.try_next().await.map_err(io::Error::other)? {
        copied = copied.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        pending.extend_from_slice(&bytes);

        if pending.len() >= DEFAULT_READ_BUFFER_SIZE {
            writer.write_all(&pending).await?;
            pending.clear();
        }
    }

    if !pending.is_empty() {
        writer.write_all(&pending).await?;
    }

    Ok(copied)
}

fn parse_query<T>(req: &Request<Incoming>) -> Result<T, RpcErrorResponse>
where
    T: DeserializeOwned + Default,
{
    match req.uri().query() {
        Some(query) => from_bytes(query.as_bytes()).map_err(|e| {
            let message = format!("get query failed {e}");
            log_internode_rpc_response_failure!(
                StatusCode::BAD_REQUEST,
                req.uri().path(),
                req.method(),
                internode_http_operation(req.uri().path()),
                "query_parse_failed",
                "rejected",
                None,
                Some(&e)
            );
            Box::new(response_with_status(StatusCode::BAD_REQUEST, message))
        }),
        None => Ok(T::default()),
    }
}

fn empty_ok() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .expect("failed to build empty ok response")
}

fn response_with_status(status: StatusCode, message: impl Into<String>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(Bytes::from(message.into())))
        .expect("failed to build rpc error response")
}

fn internode_rpc_subsystem(operation: Option<&'static str>) -> &'static str {
    match operation {
        Some(INTERNODE_OPERATION_WALK_DIR) => LOG_SUBSYSTEM_DIRECTORY_WALK,
        Some(INTERNODE_OPERATION_NS_SCANNER) => LOG_SUBSYSTEM_NAMESPACE_SCANNER,
        Some(INTERNODE_OPERATION_READ_FILE_STREAM | INTERNODE_OPERATION_PUT_FILE_STREAM) => LOG_SUBSYSTEM_FILE_TRANSFER,
        _ => LOG_SUBSYSTEM_ROUTING,
    }
}

/// A writer that is dropped mid-stream (cancelled sender task) terminates the chunked
/// body cleanly, indistinguishable from intentional EOF. When the client declared the
/// exact size up front (create path; append and unknown-size writes send `size <= 0`),
/// a byte-count mismatch means the body was truncated and must not be acknowledged.
fn put_body_size_mismatch(query: &PutFileQuery, copied: u64) -> bool {
    !query.append && query.size > 0 && copied != u64::try_from(query.size).unwrap_or(u64::MAX)
}

fn put_file_stage_error_message(stage: &str, query: &PutFileQuery, err: &dyn std::fmt::Display) -> String {
    format!(
        "{stage} file err {err} [disk={}, volume={}, path={}, append={}, size={}]",
        query.disk, query.volume, query.path, query.append, query.size
    )
}

#[cfg(test)]
mod tests {
    use super::{
        LOG_SUBSYSTEM_DIRECTORY_WALK, LOG_SUBSYSTEM_FILE_TRANSFER, LOG_SUBSYSTEM_NAMESPACE_SCANNER, LOG_SUBSYSTEM_ROUTING,
        NS_SCANNER_BODY_SHA256_QUERY, NS_SCANNER_CAPABILITY_CHALLENGE_QUERY, NS_SCANNER_CYCLE_QUERY,
        NS_SCANNER_LEADER_EPOCH_QUERY, NS_SCANNER_PATH, NS_SCANNER_REQUEST_ID_QUERY, NS_SCANNER_SERVER_EPOCH_QUERY,
        NS_SCANNER_SESSION_ID_QUERY, NS_SCANNER_SESSION_SEQUENCE_QUERY, NsScannerQuery, PUT_FILE_STREAM_PATH, PutFileQuery,
        READ_FILE_STREAM_PATH, WALK_DIR_BODY_SHA256_QUERY, WALK_DIR_PATH, WalkDirQuery, append_walk_dir_completion,
        internode_http_operation, internode_rpc_subsystem, is_internode_rpc_path, ns_scanner_response_body,
        ns_scanner_server_epoch_matches, put_body_size_mismatch, put_file_stage_error_message, read_file_body_stream,
        remote_scanner_claim_rejection, supports_walk_dir_stream_completion, validate_walk_dir_completion_request,
        verify_internode_rpc_signature, verify_ns_scanner_body_digest, verify_walk_dir_body_digest, walk_dir_response_body,
        write_body_chunks_to_writer,
    };
    use bytes::Bytes;
    use http::{HeaderMap, Method, StatusCode, Uri};
    use http_body_util::BodyExt;
    use rustfs_io_metrics::internode_metrics::{
        INTERNODE_OPERATION_NS_SCANNER, INTERNODE_OPERATION_PUT_FILE_STREAM, INTERNODE_OPERATION_READ_FILE_STREAM,
        INTERNODE_OPERATION_WALK_DIR, global_internode_metrics,
    };
    use sha2::Digest as _;
    use tokio::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_stream::StreamExt;
    use tokio_stream::iter;

    struct DropNotifier(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropNotifier {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[test]
    fn internode_rpc_path_matches_rpc_prefix() {
        assert!(is_internode_rpc_path("/rustfs/rpc/read_file_stream"));
        assert!(is_internode_rpc_path("/rustfs/rpc/walk_dir"));
        assert!(is_internode_rpc_path("/rustfs/rpc/ns_scanner"));
        assert!(!is_internode_rpc_path("/rustfs/admin/v3/info"));
    }

    #[test]
    fn internode_http_operation_maps_only_known_routes() {
        assert_eq!(
            internode_http_operation(READ_FILE_STREAM_PATH),
            Some(INTERNODE_OPERATION_READ_FILE_STREAM)
        );
        assert_eq!(internode_http_operation(PUT_FILE_STREAM_PATH), Some(INTERNODE_OPERATION_PUT_FILE_STREAM));
        assert_eq!(internode_http_operation(WALK_DIR_PATH), Some(INTERNODE_OPERATION_WALK_DIR));
        assert_eq!(internode_http_operation(NS_SCANNER_PATH), Some(INTERNODE_OPERATION_NS_SCANNER));
        assert_eq!(internode_http_operation("/rustfs/rpc/unknown"), None);
    }

    #[test]
    fn file_stream_head_signature_verification_is_skipped() {
        let uri: Uri = READ_FILE_STREAM_PATH.parse().expect("uri");
        let headers = HeaderMap::new();
        assert!(verify_internode_rpc_signature(&uri, &Method::HEAD, &headers).is_ok());
    }

    #[test]
    fn namespace_scanner_capability_get_requires_signature() {
        let challenge = uuid::Uuid::new_v4();
        let uri: Uri = format!(
            "{NS_SCANNER_PATH}?ns_scanner_protocol={}&{NS_SCANNER_CAPABILITY_CHALLENGE_QUERY}={challenge}",
            super::NS_SCANNER_PROTOCOL_VERSION
        )
        .parse()
        .expect("uri");
        let headers = HeaderMap::new();
        let response = verify_internode_rpc_signature(&uri, &Method::GET, &headers).expect_err("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn namespace_scanner_rejects_requests_from_a_prior_server_epoch() {
        assert!(ns_scanner_server_epoch_matches(*super::NS_SCANNER_SERVER_EPOCH));
        assert!(!ns_scanner_server_epoch_matches(uuid::Uuid::nil()));
    }

    #[test]
    fn rpc_get_request_requires_signature() {
        let uri: Uri = READ_FILE_STREAM_PATH.parse().expect("uri");
        let headers = HeaderMap::new();
        let response = verify_internode_rpc_signature(&uri, &Method::GET, &headers).expect_err("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn put_file_stage_error_message_includes_stage_and_request_context() {
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: ".rustfs.sys/tmp".to_string(),
            path: "tmp/object/part.1".to_string(),
            append: false,
            size: 1024,
        };

        let msg = put_file_stage_error_message("write_body", &query, &"connection reset");
        assert!(msg.contains("write_body"));
        assert!(msg.contains("disk=disk-a"));
        assert!(msg.contains("volume=.rustfs.sys/tmp"));
        assert!(msg.contains("path=tmp/object/part.1"));
        assert!(msg.contains("append=false"));
        assert!(msg.contains("size=1024"));
    }

    #[test]
    fn put_body_size_mismatch_rejects_truncated_create_only() {
        let query = |append: bool, size: i64| PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append,
            size,
        };

        // Truncated (or over-long) body on the create path is rejected.
        assert!(put_body_size_mismatch(&query(false, 1024), 512));
        assert!(put_body_size_mismatch(&query(false, 1024), 2048));
        assert!(!put_body_size_mismatch(&query(false, 1024), 1024));
        // Append streams send size=0; unknown-size creates send size<=0 — never rejected.
        assert!(!put_body_size_mismatch(&query(true, 0), 512));
        assert!(!put_body_size_mismatch(&query(false, 0), 512));
        assert!(!put_body_size_mismatch(&query(false, -1), 512));
    }

    #[test]
    fn internode_rpc_subsystem_matches_known_operations() {
        assert_eq!(
            internode_rpc_subsystem(Some(INTERNODE_OPERATION_READ_FILE_STREAM)),
            LOG_SUBSYSTEM_FILE_TRANSFER
        );
        assert_eq!(
            internode_rpc_subsystem(Some(INTERNODE_OPERATION_PUT_FILE_STREAM)),
            LOG_SUBSYSTEM_FILE_TRANSFER
        );
        assert_eq!(internode_rpc_subsystem(Some(INTERNODE_OPERATION_WALK_DIR)), LOG_SUBSYSTEM_DIRECTORY_WALK);
        assert_eq!(
            internode_rpc_subsystem(Some(INTERNODE_OPERATION_NS_SCANNER)),
            LOG_SUBSYSTEM_NAMESPACE_SCANNER
        );
        assert_eq!(internode_rpc_subsystem(None), LOG_SUBSYSTEM_ROUTING);
    }

    #[test]
    fn namespace_scanner_requires_matching_signed_body_digest() {
        let body = b"scanner-request";
        let digest = hex_simd::encode_to_string(sha2::Sha256::digest(body), hex_simd::AsciiCase::Lower);
        let request_id = uuid::Uuid::new_v4();
        let server_epoch = uuid::Uuid::new_v4();
        let session_id = uuid::Uuid::new_v4();
        let valid: NsScannerQuery = serde_urlencoded::from_str(&format!(
            "disk=disk-a&{NS_SCANNER_REQUEST_ID_QUERY}={request_id}&{NS_SCANNER_SERVER_EPOCH_QUERY}={server_epoch}&{NS_SCANNER_SESSION_ID_QUERY}={session_id}&{NS_SCANNER_SESSION_SEQUENCE_QUERY}=0&{NS_SCANNER_CYCLE_QUERY}=7&{NS_SCANNER_LEADER_EPOCH_QUERY}=9&{NS_SCANNER_BODY_SHA256_QUERY}={digest}"
        ))
        .expect("query should parse");
        let missing: NsScannerQuery = serde_urlencoded::from_str(&format!(
            "disk=disk-a&{NS_SCANNER_REQUEST_ID_QUERY}={request_id}&{NS_SCANNER_SERVER_EPOCH_QUERY}={server_epoch}&{NS_SCANNER_SESSION_ID_QUERY}={session_id}&{NS_SCANNER_SESSION_SEQUENCE_QUERY}=0&{NS_SCANNER_CYCLE_QUERY}=7&{NS_SCANNER_LEADER_EPOCH_QUERY}=9"
        ))
        .expect("query should parse");

        assert!(verify_ns_scanner_body_digest(&valid, body));
        assert!(!verify_ns_scanner_body_digest(&valid, b"tampered"));
        assert!(!verify_ns_scanner_body_digest(&missing, body));
    }

    #[test]
    fn namespace_scanner_queries_reject_unknown_fields() {
        let request_id = uuid::Uuid::new_v4();
        let server_epoch = uuid::Uuid::new_v4();
        let session_id = uuid::Uuid::new_v4();
        let query = format!(
            "disk=disk-a&{NS_SCANNER_REQUEST_ID_QUERY}={request_id}&{NS_SCANNER_SERVER_EPOCH_QUERY}={server_epoch}&{NS_SCANNER_SESSION_ID_QUERY}={session_id}&{NS_SCANNER_SESSION_SEQUENCE_QUERY}=0&{NS_SCANNER_CYCLE_QUERY}=7&{NS_SCANNER_LEADER_EPOCH_QUERY}=9&{NS_SCANNER_BODY_SHA256_QUERY}=digest&unexpected=true"
        );
        assert!(serde_urlencoded::from_str::<NsScannerQuery>(&query).is_err());
        assert!(serde_urlencoded::from_str::<super::NsScannerCapabilityQuery>("ns_scanner_protocol=1&unexpected=true").is_err());
    }

    #[test]
    fn namespace_scanner_replay_capacity_is_retryable_without_weakening_replay_rejection() {
        let (replay_status, replay_reason, _) =
            remote_scanner_claim_rejection(&rustfs_scanner::ScannerError::RemoteRequestReplay);
        let (capacity_status, capacity_reason, _) =
            remote_scanner_claim_rejection(&rustfs_scanner::ScannerError::RemoteReplayCapacity);

        assert_eq!(replay_status, StatusCode::CONFLICT);
        assert_eq!(replay_reason, "request_replay");
        assert_eq!(capacity_status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(capacity_reason, "replay_capacity");
    }

    #[tokio::test]
    async fn namespace_scanner_body_surfaces_background_failure() {
        let body = ns_scanner_response_body(|mut writer, _disconnect| async move {
            writer.write_all(b"partial scanner frame").await?;
            Err(io::Error::other("remote namespace scanner failed"))
        });
        let err = BodyExt::collect(body)
            .await
            .expect_err("failed completion must fail body collection");

        assert!(err.to_string().contains("remote namespace scanner failed"));
    }

    #[tokio::test]
    async fn dropping_namespace_scanner_body_cancels_producer() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let body = ns_scanner_response_body(move |_writer, disconnect| async move {
            let _drop_notifier = DropNotifier(Some(dropped_tx));
            let _ = started_tx.send(());
            disconnect.cancelled().await;
            Ok(())
        });

        started_rx.await.expect("namespace scanner producer should start");
        drop(body);

        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("dropping the response body should cancel the namespace scanner producer")
            .expect("drop notifier should send a cancellation signal");
    }

    #[tokio::test]
    async fn write_body_chunks_to_writer_streams_all_chunks() {
        let (mut reader, mut writer) = tokio::io::duplex(64);
        let body = iter(vec![
            Ok::<Bytes, io::Error>(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"world")),
        ]);

        let copied = write_body_chunks_to_writer(body, &mut writer).await.expect("copy succeeds");
        drop(writer);

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.expect("read succeeds");

        assert_eq!(copied, 11);
        assert_eq!(out, b"hello world");
    }

    #[tokio::test]
    async fn walk_dir_body_surfaces_background_failure_after_data() {
        let body = walk_dir_response_body(true, |mut writer| async move {
            writer.write_all(b"partial walk data").await?;
            Err(io::Error::other("remote walk_dir failed"))
        });
        let err = BodyExt::collect(body)
            .await
            .expect_err("failed completion must fail body collection");

        assert!(err.to_string().contains("remote walk_dir failed"));
    }

    #[tokio::test]
    async fn walk_dir_body_preserves_data_after_success() {
        let body = walk_dir_response_body(true, |mut writer| async move {
            writer.write_all(b"complete walk data").await?;
            Ok(())
        });
        let bytes = BodyExt::collect(body)
            .await
            .expect("successful completion should preserve the body")
            .to_bytes();

        assert_eq!(bytes, Bytes::from_static(b"complete walk data"));
    }

    #[tokio::test]
    async fn walk_dir_body_records_operation_sent_bytes() {
        let metrics = global_internode_metrics();
        let before = metrics.snapshot().sent_bytes_total;
        let payload = Bytes::from_static(b"metered walk data");
        let expected_len = u64::try_from(payload.len()).expect("test payload length should fit u64");
        let body = walk_dir_response_body(true, move |mut writer| async move {
            writer.write_all(&payload).await?;
            Ok(())
        });

        let bytes = BodyExt::collect(body)
            .await
            .expect("successful completion should preserve the metered body")
            .to_bytes();
        let after = metrics.snapshot().sent_bytes_total;

        assert_eq!(bytes, Bytes::from_static(b"metered walk data"));
        assert!(
            after >= before.saturating_add(expected_len),
            "walk_dir response body should record streamed bytes as internode sent bytes: before={before}, after={after}, expected_delta={expected_len}"
        );
    }

    #[tokio::test]
    async fn walk_dir_completion_stream_surfaces_cancelled_producer() {
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        drop(completion_tx);
        let stream = iter([Ok::<Bytes, io::Error>(Bytes::from_static(b"partial walk data"))]);
        let body = s3s::Body::from(s3s::dto::StreamingBlob::wrap(append_walk_dir_completion(stream, completion_rx, true)));

        let err = BodyExt::collect(body)
            .await
            .expect_err("a cancelled producer must fail body collection");

        assert!(err.to_string().contains("ended without a result"));
    }

    #[tokio::test]
    async fn dropping_walk_dir_body_cancels_blocked_producer() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let body = walk_dir_response_body(true, move |_writer| async move {
            let _drop_notifier = DropNotifier(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<io::Result<()>>().await
        });

        started_rx.await.expect("walk producer should start");
        drop(body);

        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("dropping the response body should cancel the walk producer")
            .expect("drop notifier should send a cancellation signal");
    }

    #[tokio::test]
    async fn legacy_walk_dir_client_keeps_clean_eof_compatibility() {
        let body = walk_dir_response_body(false, |mut writer| async move {
            writer.write_all(b"legacy partial data").await?;
            Err(io::Error::other("remote walk_dir failed"))
        });

        let bytes = BodyExt::collect(body)
            .await
            .expect("legacy clients must retain clean EOF until they advertise stream completion support")
            .to_bytes();

        assert_eq!(bytes, Bytes::from_static(b"legacy partial data"));
    }

    #[tokio::test]
    async fn legacy_walk_dir_client_keeps_clean_eof_when_producer_is_cancelled() {
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        drop(completion_tx);
        let stream = iter([Ok::<Bytes, io::Error>(Bytes::from_static(b"legacy partial data"))]);
        let body = s3s::Body::from(s3s::dto::StreamingBlob::wrap(append_walk_dir_completion(stream, completion_rx, false)));

        let bytes = BodyExt::collect(body)
            .await
            .expect("legacy clients must retain clean EOF after producer cancellation")
            .to_bytes();

        assert_eq!(bytes, Bytes::from_static(b"legacy partial data"));
    }

    #[test]
    fn walk_dir_completion_requires_the_exact_signed_query_capability() {
        let legacy: WalkDirQuery = serde_urlencoded::from_str("disk=disk-a").expect("legacy query should parse");
        let unknown: WalkDirQuery =
            serde_urlencoded::from_str("disk=disk-a&walk_dir_stream_completion=error-v2").expect("unknown query should parse");
        let capable: WalkDirQuery =
            serde_urlencoded::from_str("disk=disk-a&walk_dir_stream_completion=error-v1").expect("capable query should parse");

        assert!(!supports_walk_dir_stream_completion(&legacy));
        assert!(!supports_walk_dir_stream_completion(&unknown));
        assert!(supports_walk_dir_stream_completion(&capable));
    }

    #[test]
    fn walk_dir_completion_requires_matching_signed_body_digest() {
        let body = br#"{"bucket":"bucket-a"}"#;
        let digest = hex_simd::encode_to_string(sha2::Sha256::digest(body), hex_simd::AsciiCase::Lower);
        let capable: WalkDirQuery = serde_urlencoded::from_str(&format!(
            "disk=disk-a&walk_dir_stream_completion=error-v1&{WALK_DIR_BODY_SHA256_QUERY}={digest}"
        ))
        .expect("capable query should parse");
        let missing: WalkDirQuery =
            serde_urlencoded::from_str("disk=disk-a&walk_dir_stream_completion=error-v1").expect("query should parse");
        let legacy: WalkDirQuery = serde_urlencoded::from_str("disk=disk-a").expect("legacy query should parse");

        assert!(verify_walk_dir_body_digest(&capable, body));
        assert!(!verify_walk_dir_body_digest(&capable, b"tampered"));
        assert!(!verify_walk_dir_body_digest(&missing, body));
        assert!(verify_walk_dir_body_digest(&legacy, b"legacy body"));
        assert_eq!(validate_walk_dir_completion_request(&capable, body), Some(true));
        assert_eq!(validate_walk_dir_completion_request(&legacy, b"legacy body"), Some(false));
        assert_eq!(validate_walk_dir_completion_request(&capable, b"tampered"), None);
    }

    #[tokio::test]
    async fn read_file_body_stream_keeps_full_stream_when_length_is_zero() {
        let (reader, mut writer) = tokio::io::duplex(64);
        tokio::spawn(async move {
            writer.write_all(b"hello world").await.expect("write succeeds");
        });

        let mut stream = read_file_body_stream(reader, 0, INTERNODE_OPERATION_READ_FILE_STREAM);
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk.expect("chunk succeeds"));
        }

        assert_eq!(out, b"hello world");
    }

    #[tokio::test]
    async fn read_file_body_stream_truncates_to_requested_length() {
        let (reader, mut writer) = tokio::io::duplex(64);
        tokio::spawn(async move {
            writer.write_all(b"hello world").await.expect("write succeeds");
        });

        let mut stream = read_file_body_stream(reader, 5, INTERNODE_OPERATION_READ_FILE_STREAM);
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk.expect("chunk succeeds"));
        }

        assert_eq!(out, b"hello");
    }
}
