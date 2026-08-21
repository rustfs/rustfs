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
use crate::storage::storage_api::DiskError;
use crate::storage::storage_api::rpc_consumer::http_service::{
    DEFAULT_READ_BUFFER_SIZE, DeleteOptions, DiskStore, NS_SCANNER_PROTOCOL_VERSION, NsScannerCapabilityResponse,
    PUT_FILE_AUTH_TRAILER_LEN, PUT_FILE_AUTH_V1, PUT_FILE_CAPABILITY_VERSION, PutFileCapabilityResponse, StorageDiskRpcExt as _,
    WALK_DIR_STREAM_COMPLETION_V1, WalkDirOptions, check_and_record_signed_rpc_nonce, find_local_disk_by_ref,
    sign_ns_scanner_capability, sign_put_file_capability, verify_put_file_auth_trailer, verify_rpc_signature,
};
#[cfg(test)]
use crate::storage::storage_api::rpc_consumer::http_service::{
    NS_SCANNER_BODY_SHA256_QUERY, NS_SCANNER_CAPABILITY_CHALLENGE_QUERY, NS_SCANNER_CYCLE_QUERY, NS_SCANNER_LEADER_EPOCH_QUERY,
    NS_SCANNER_REQUEST_ID_QUERY, NS_SCANNER_SERVER_EPOCH_QUERY, NS_SCANNER_SESSION_ID_QUERY, NS_SCANNER_SESSION_SEQUENCE_QUERY,
    PUT_FILE_CAPABILITY_CHALLENGE_QUERY, PUT_FILE_CAPABILITY_QUERY, WALK_DIR_BODY_SHA256_QUERY,
};
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use crate::storage::storage_api::tonic_rpc_auth_failure_reason;
use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt, TryStreamExt, stream};
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_OPERATION_NS_SCANNER, INTERNODE_OPERATION_PUT_FILE_CAPABILITY, INTERNODE_OPERATION_PUT_FILE_STREAM,
    INTERNODE_OPERATION_READ_FILE_STREAM, INTERNODE_OPERATION_WALK_DIR, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
};
use s3s::Body;
use s3s::dto::StreamingBlob;
use serde::de::DeserializeOwned;
use serde_urlencoded::from_bytes;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, oneshot};
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
const PUT_FILE_AUTH_STREAM_PATH: &str = "/rustfs/rpc/put_file_stream_v1";
const PUT_FILE_CAPABILITY_PATH: &str = "/rustfs/rpc/put_file_capability";
const WALK_DIR_PATH: &str = "/rustfs/rpc/walk_dir";
const NS_SCANNER_PATH: &str = "/rustfs/rpc/ns_scanner";
const NS_SCANNER_REQUEST_BODY_TIMEOUT: Duration = Duration::from_secs(15);
const NS_SCANNER_STREAM_BUFFER_SIZE: usize = 64 * 1024;
static NS_SCANNER_SERVER_EPOCH: LazyLock<uuid::Uuid> = LazyLock::new(uuid::Uuid::new_v4);
static PUT_FILE_CAPABILITY_SERVER_EPOCH: LazyLock<uuid::Uuid> = LazyLock::new(uuid::Uuid::new_v4);
static PUT_FILE_AUTH_STRICT: LazyLock<bool> = LazyLock::new(|| {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_INTERNODE_RPC_BODY_DIGEST_STRICT,
        rustfs_config::DEFAULT_INTERNODE_RPC_BODY_DIGEST_STRICT,
    )
});
static PUT_FILE_TARGET_LOCKS: LazyLock<parking_lot::Mutex<HashMap<String, Weak<Mutex<()>>>>> =
    LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

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

#[derive(Clone, Debug, Default, serde::Deserialize)]
struct PutFileQuery {
    disk: String,
    volume: String,
    path: String,
    append: bool,
    size: i64,
    put_file_auth: Option<String>,
    put_file_nonce: Option<uuid::Uuid>,
    put_file_server_epoch: Option<uuid::Uuid>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PutFileCapabilityQuery {
    put_file_capability: Option<u16>,
    put_file_challenge: Option<uuid::Uuid>,
}

fn put_file_auth_nonce(query: &PutFileQuery) -> io::Result<Option<uuid::Uuid>> {
    match query.put_file_auth.as_deref() {
        None => {
            if *PUT_FILE_AUTH_STRICT {
                return Err(io::Error::other("put_file auth required"));
            }
            Ok(None)
        }
        Some(PUT_FILE_AUTH_V1) => {
            let nonce = query
                .put_file_nonce
                .filter(|nonce| !nonce.is_nil())
                .ok_or_else(|| io::Error::other("Invalid RPC nonce"))?;
            Ok(Some(nonce))
        }
        Some(_) => Err(io::Error::other("Unsupported put_file auth version")),
    }
}

fn put_file_server_epoch_matches(query: &PutFileQuery) -> bool {
    query.put_file_server_epoch == Some(*PUT_FILE_CAPABILITY_SERVER_EPOCH)
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
        (Method::GET, PUT_FILE_CAPABILITY_PATH) => match parse_query::<PutFileCapabilityQuery>(&req) {
            Ok(query) if query.put_file_capability == Some(PUT_FILE_CAPABILITY_VERSION) => {
                match query.put_file_challenge.filter(|challenge| !challenge.is_nil()) {
                    Some(challenge) => put_file_capability_response(challenge),
                    None => response_with_status(StatusCode::BAD_REQUEST, "put_file capability challenge is invalid"),
                }
            }
            Ok(_) => response_with_status(StatusCode::UPGRADE_REQUIRED, "put_file capability is unsupported"),
            Err(response) => *response,
        },
        (Method::PUT, PUT_FILE_STREAM_PATH) => handle_put_file(req, false).await,
        (Method::PUT, PUT_FILE_AUTH_STREAM_PATH) => handle_put_file(req, true).await,
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
        PUT_FILE_STREAM_PATH | PUT_FILE_AUTH_STREAM_PATH => Some(INTERNODE_OPERATION_PUT_FILE_STREAM),
        PUT_FILE_CAPABILITY_PATH => Some(INTERNODE_OPERATION_PUT_FILE_CAPABILITY),
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

fn put_file_capability_response(challenge: uuid::Uuid) -> Response<Body> {
    let server_epoch = *PUT_FILE_CAPABILITY_SERVER_EPOCH;
    let proof = match sign_put_file_capability(challenge, server_epoch, PUT_FILE_CAPABILITY_VERSION) {
        Ok(proof) => proof,
        Err(err) => {
            return response_with_status(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("put_file capability authentication is unavailable: {err}"),
            );
        }
    };
    match rmp_serde::to_vec_named(&PutFileCapabilityResponse {
        version: PUT_FILE_CAPABILITY_VERSION,
        server_epoch,
        proof,
    }) {
        Ok(body) => {
            let mut response = Response::new(Body::from(Bytes::from(body)));
            response
                .headers_mut()
                .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/msgpack"));
            response
        }
        Err(err) => response_with_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("put_file capability response encoding failed: {err}"),
        ),
    }
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
        let operation = internode_http_operation(uri.path());
        runtime_sources::current_internode_metrics().record_rpc_auth_failure_for_operation_and_backend(
            operation.unwrap_or(RPC_OPERATION_UNKNOWN),
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            tonic_rpc_auth_failure_reason(&e),
        );
        log_internode_rpc_response_failure!(
            StatusCode::FORBIDDEN,
            uri.path(),
            method,
            operation,
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
            return response_with_disk_error(&e, message);
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
    let read_buffer_size = read_file_stream_buffer_size(length);
    let read_limit = if length == 0 {
        u64::MAX
    } else {
        u64::try_from(length).unwrap_or(u64::MAX)
    };
    let stream = ReaderStream::with_capacity(reader.take(read_limit), read_buffer_size).map_ok(move |bytes| {
        metrics.record_sent_bytes_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP, bytes.len());
        bytes
    });
    Box::pin(stream)
}

fn read_file_stream_buffer_size(length: usize) -> usize {
    if length == 0 {
        DEFAULT_READ_BUFFER_SIZE
    } else {
        length.min(DEFAULT_READ_BUFFER_SIZE)
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
    // Acquire the per-disk permit before reading the request body. This bounds
    // slow or duplicated authenticated uploads to one request per physical disk;
    // dropping the request on timeout releases the permit without consuming its
    // replay sequence.
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

fn put_file_target_lock(disk: &DiskStore, query: &PutFileQuery) -> Arc<Mutex<()>> {
    let key = format!("{:p}\0{}\0{}", Arc::as_ptr(disk), query.volume, query.path);
    let mut locks = PUT_FILE_TARGET_LOCKS.lock();
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(Mutex::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

async fn remove_put_file_staging(disk: &DiskStore, volume: &str, path: &str) -> Result<(), BoxError> {
    disk.delete(
        volume,
        path,
        DeleteOptions {
            immediate: true,
            ..Default::default()
        },
    )
    .await
    .map_err(Into::into)
}

async fn write_authenticated_put_file<S, E>(
    disk: &DiskStore,
    body: S,
    query: &PutFileQuery,
    nonce: uuid::Uuid,
    url: &str,
) -> Result<u64, (&'static str, BoxError)>
where
    S: futures::TryStream<Ok = Bytes, Error = E> + Unpin,
    E: Into<BoxError>,
{
    let target_lock = put_file_target_lock(disk, query);
    let _target_guard = target_lock.lock_owned().await;
    let staging_name = format!(".rustfs-put-{}", uuid::Uuid::new_v4());
    let staging_path = match query.path.rsplit_once('/') {
        Some((parent, _)) => format!("{parent}/{staging_name}"),
        None => staging_name,
    };

    let result = async {
        let mut file = disk
            .create_file("", &query.volume, &staging_path, query.size)
            .await
            .map_err(|err| ("create_staging", Box::new(err) as BoxError))?;

        if query.append {
            match disk.read_file(&query.volume, &query.path).await {
                Ok(mut source) => {
                    tokio::io::copy(&mut source, &mut file)
                        .await
                        .map_err(|err| ("copy_existing", Box::new(err) as BoxError))?;
                }
                Err(DiskError::FileNotFound) => {}
                Err(err) => return Err(("read_existing", Box::new(err) as BoxError)),
            }
        }

        let copied = write_put_file_body_chunks_to_writer(body, &mut file, query, Some(nonce), url)
            .await
            .map_err(|err| ("write_body", Box::new(err) as BoxError))?;
        if put_body_size_mismatch(query, copied) {
            return Err((
                "verify_size",
                Box::new(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("body size mismatch: expected {} bytes, received {copied}", query.size),
                )) as BoxError,
            ));
        }
        file.shutdown().await.map_err(|err| ("shutdown", Box::new(err) as BoxError))?;
        drop(file);

        disk.rename_file(&query.volume, &staging_path, &query.volume, &query.path)
            .await
            .map_err(|err| ("publish", Box::new(err) as BoxError))?;
        Ok(copied)
    }
    .await;

    match result {
        Ok(copied) => Ok(copied),
        Err((stage, primary)) => {
            if let Err(cleanup) = remove_put_file_staging(disk, &query.volume, &staging_path).await {
                return Err((
                    stage,
                    Box::new(io::Error::other(format!("{primary}; staging cleanup failed: {cleanup}"))) as BoxError,
                ));
            }
            Err((stage, primary))
        }
    }
}

async fn handle_put_file(req: Request<Incoming>, require_auth: bool) -> Response<Body> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let url = req.uri().to_string();
    let query = match parse_query::<PutFileQuery>(&req) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let auth_nonce = match put_file_auth_nonce(&query) {
        Ok(nonce) => nonce,
        Err(e) => {
            log_internode_rpc_response_failure!(
                StatusCode::FORBIDDEN,
                &path,
                &method,
                Some(INTERNODE_OPERATION_PUT_FILE_STREAM),
                "put_file_auth_invalid",
                "rejected",
                Some(("disk", query.disk.as_str())),
                Some(&e)
            );
            return response_with_status(StatusCode::FORBIDDEN, format!("invalid put_file auth: {e}"));
        }
    };
    if require_auth && auth_nonce.is_none() {
        return response_with_status(StatusCode::FORBIDDEN, "invalid put_file auth: put_file auth required");
    }
    if require_auth && !put_file_server_epoch_matches(&query) {
        return response_with_status(StatusCode::CONFLICT, "put_file capability server epoch changed");
    }
    if let Some(nonce) = auth_nonce
        && let Err(e) = check_and_record_signed_rpc_nonce(
            req.headers(),
            nonce,
            &path,
            INTERNODE_OPERATION_PUT_FILE_STREAM,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        )
    {
        log_internode_rpc_response_failure!(
            StatusCode::FORBIDDEN,
            &path,
            &method,
            Some(INTERNODE_OPERATION_PUT_FILE_STREAM),
            "put_file_replay_rejected",
            "rejected",
            Some(("disk", query.disk.as_str())),
            Some(&e)
        );
        return response_with_status(StatusCode::FORBIDDEN, format!("invalid put_file auth: {e}"));
    }

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

    if let Some(nonce) = auth_nonce {
        let copied = match write_authenticated_put_file(&disk, req.into_body().into_data_stream(), &query, nonce, &url).await {
            Ok(copied) => copied,
            Err((stage, e)) => {
                let message = put_file_stage_error_message(stage, &query, e.as_ref());
                log_internode_put_file_stage_failure!(stage, query, e);
                return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, message);
            }
        };
        record_put_file_metrics(copied);
        return empty_ok();
    }

    let target_lock = put_file_target_lock(&disk, &query);
    let _target_guard = target_lock.lock_owned().await;

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

    let copied =
        match write_put_file_body_chunks_to_writer(req.into_body().into_data_stream(), &mut file, &query, auth_nonce, &url).await
        {
            Ok(copied) => copied,
            Err(e) => {
                let message = put_file_stage_error_message("write_body", &query, &e);
                log_internode_put_file_stage_failure!("write_body", query, e);
                return response_with_status(StatusCode::INTERNAL_SERVER_ERROR, message);
            }
        };

    record_put_file_metrics(copied);

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

fn record_put_file_metrics(copied: u64) {
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

async fn write_put_file_body_chunks_to_writer<S, E, W>(
    body: S,
    writer: &mut W,
    query: &PutFileQuery,
    auth_nonce: Option<uuid::Uuid>,
    url: &str,
) -> io::Result<u64>
where
    S: futures::TryStream<Ok = Bytes, Error = E> + Unpin,
    E: Into<BoxError>,
    W: tokio::io::AsyncWrite + Unpin,
{
    let Some(nonce) = auth_nonce else {
        return write_body_chunks_to_writer(body, writer).await;
    };

    let expected_size = (!query.append && query.size > 0)
        .then(|| {
            u64::try_from(query.size)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "put_file auth size cannot be represented"))
        })
        .transpose()?;
    let mut body = body;
    let mut remaining = expected_size;
    let mut copied = 0_u64;
    let mut trailer = Vec::with_capacity(PUT_FILE_AUTH_TRAILER_LEN);
    let mut hasher = Sha256::new();

    while let Some(bytes) = body.try_next().await.map_err(io::Error::other)? {
        if let Some(remaining) = remaining.as_mut() {
            let chunk_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let data_len = usize::try_from((*remaining).min(chunk_len))
                .map_err(|_| io::Error::other("put_file body length cannot be represented"))?;
            if data_len > 0 {
                hasher.update(&bytes[..data_len]);
                copied = copied
                    .checked_add(
                        u64::try_from(data_len).map_err(|_| io::Error::other("put_file body length cannot be represented"))?,
                    )
                    .ok_or_else(|| io::Error::other("put_file body length overflow"))?;
                *remaining -=
                    u64::try_from(data_len).map_err(|_| io::Error::other("put_file body length cannot be represented"))?;
                writer.write_all(&bytes[..data_len]).await?;
            }

            if data_len < bytes.len() {
                trailer.extend_from_slice(&bytes[data_len..]);
                if trailer.len() > PUT_FILE_AUTH_TRAILER_LEN {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "put_file auth trailer has trailing data"));
                }
            }
        } else {
            let write_len = trailer
                .len()
                .saturating_add(bytes.len())
                .saturating_sub(PUT_FILE_AUTH_TRAILER_LEN);
            if write_len > 0 {
                let buffered_write_len = write_len.min(trailer.len());
                if buffered_write_len > 0 {
                    writer.write_all(&trailer[..buffered_write_len]).await?;
                    hasher.update(&trailer[..buffered_write_len]);
                    copied = copied
                        .checked_add(
                            u64::try_from(buffered_write_len)
                                .map_err(|_| io::Error::other("put_file body length cannot be represented"))?,
                        )
                        .ok_or_else(|| io::Error::other("put_file body length overflow"))?;
                    trailer = trailer.split_off(buffered_write_len);
                }

                let chunk_write_len = write_len - buffered_write_len;
                if chunk_write_len > 0 {
                    writer.write_all(&bytes[..chunk_write_len]).await?;
                    hasher.update(&bytes[..chunk_write_len]);
                }
                copied = copied
                    .checked_add(
                        u64::try_from(chunk_write_len)
                            .map_err(|_| io::Error::other("put_file body length cannot be represented"))?,
                    )
                    .ok_or_else(|| io::Error::other("put_file body length overflow"))?;
                trailer.extend_from_slice(&bytes[chunk_write_len..]);
            } else {
                trailer.extend_from_slice(&bytes);
            }
        }
    }

    if remaining.is_some_and(|remaining| remaining != 0) {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("body size mismatch: expected {} bytes, received {copied}", query.size),
        ));
    }
    if trailer.len() != PUT_FILE_AUTH_TRAILER_LEN {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "put_file auth trailer is incomplete"));
    }

    let expected = verify_put_file_auth_trailer(url, &Method::PUT, nonce, &trailer)?;
    let actual = hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower);
    if actual != expected {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "put_file body digest mismatch"));
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

fn response_with_disk_error(error: &DiskError, message: impl Into<String>) -> Response<Body> {
    let missing = match error {
        DiskError::FileNotFound => Some(rustfs_rio::INTERNODE_FILE_NOT_FOUND),
        DiskError::VolumeNotFound => Some(rustfs_rio::INTERNODE_VOLUME_NOT_FOUND),
        _ => None,
    };
    let mut response = response_with_status(StatusCode::INTERNAL_SERVER_ERROR, message);
    if let Some(missing) = missing {
        response
            .headers_mut()
            .insert(rustfs_rio::INTERNODE_DISK_ERROR_HEADER, HeaderValue::from_static(missing));
    }
    response
}

fn internode_rpc_subsystem(operation: Option<&'static str>) -> &'static str {
    match operation {
        Some(INTERNODE_OPERATION_WALK_DIR) => LOG_SUBSYSTEM_DIRECTORY_WALK,
        Some(INTERNODE_OPERATION_NS_SCANNER) => LOG_SUBSYSTEM_NAMESPACE_SCANNER,
        Some(
            INTERNODE_OPERATION_READ_FILE_STREAM | INTERNODE_OPERATION_PUT_FILE_STREAM | INTERNODE_OPERATION_PUT_FILE_CAPABILITY,
        ) => LOG_SUBSYSTEM_FILE_TRANSFER,
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
        DEFAULT_READ_BUFFER_SIZE, DiskError, InternodeRpcService, LOG_SUBSYSTEM_DIRECTORY_WALK, LOG_SUBSYSTEM_FILE_TRANSFER,
        LOG_SUBSYSTEM_NAMESPACE_SCANNER, LOG_SUBSYSTEM_ROUTING, NS_SCANNER_BODY_SHA256_QUERY,
        NS_SCANNER_CAPABILITY_CHALLENGE_QUERY, NS_SCANNER_CYCLE_QUERY, NS_SCANNER_LEADER_EPOCH_QUERY, NS_SCANNER_PATH,
        NS_SCANNER_REQUEST_ID_QUERY, NS_SCANNER_SERVER_EPOCH_QUERY, NS_SCANNER_SESSION_ID_QUERY,
        NS_SCANNER_SESSION_SEQUENCE_QUERY, NsScannerQuery, PUT_FILE_AUTH_STREAM_PATH, PUT_FILE_CAPABILITY_PATH,
        PUT_FILE_STREAM_PATH, PutFileQuery, READ_FILE_STREAM_PATH, WALK_DIR_BODY_SHA256_QUERY, WALK_DIR_PATH, WalkDirQuery,
        append_walk_dir_completion, internode_http_operation, internode_rpc_subsystem, is_internode_rpc_path,
        ns_scanner_response_body, ns_scanner_server_epoch_matches, put_body_size_mismatch, put_file_auth_nonce,
        put_file_capability_response, put_file_server_epoch_matches, put_file_stage_error_message, put_file_target_lock,
        read_file_body_stream, read_file_stream_buffer_size, remote_scanner_claim_rejection, response_with_disk_error,
        supports_walk_dir_stream_completion, validate_walk_dir_completion_request, verify_internode_rpc_signature,
        verify_ns_scanner_body_digest, verify_walk_dir_body_digest, walk_dir_response_body, write_authenticated_put_file,
        write_body_chunks_to_writer, write_put_file_body_chunks_to_writer,
    };
    use crate::storage::storage_api::ecstore_rpc::{build_put_file_auth_trailer, gen_signature_headers};
    use crate::storage::storage_api::rpc_consumer::http_service::{DiskAPI as _, DiskOption, DiskStore, Endpoint, new_disk};
    use bytes::Bytes;
    use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Uri};
    use http_body_util::{BodyExt, Empty};
    use hyper::{client::conn::http1 as client_http1, server::conn::http1 as server_http1};
    use hyper_util::{rt::TokioIo, service::TowerToHyperService};
    use metrics::with_local_recorder;
    use metrics_util::debugging::DebuggingRecorder;
    use rustfs_io_metrics::internode_metrics::{
        INTERNODE_OPERATION_NS_SCANNER, INTERNODE_OPERATION_PUT_FILE_CAPABILITY, INTERNODE_OPERATION_PUT_FILE_STREAM,
        INTERNODE_OPERATION_READ_FILE_STREAM, INTERNODE_OPERATION_WALK_DIR, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        global_internode_metrics,
    };
    use sha2::Digest as _;
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::future::Future as _;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_stream::StreamExt;
    use tokio_stream::iter;

    struct RejectExtraPollReader {
        emitted: bool,
    }

    impl io::AsyncRead for RejectExtraPollReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut io::ReadBuf<'_>) -> Poll<io::Result<()>> {
            if self.emitted {
                return Poll::Ready(Err(io::Error::other("reader polled past the requested length")));
            }

            let bytes = b"hello world";
            buf.put_slice(&bytes[..buf.remaining().min(bytes.len())]);
            self.emitted = true;
            Poll::Ready(Ok(()))
        }
    }

    async fn new_put_file_test_disk() -> (DiskStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("temp directory should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp path should be utf8")).expect("disk endpoint should parse");
        let disk = new_disk(&endpoint, &DiskOption::default())
            .await
            .expect("local disk should be created");
        disk.make_volume("bucket").await.expect("test volume should be created");
        (disk, dir)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn authenticated_put_route_checks_server_epoch_before_disk_lookup() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-auth-body-test-secret".to_string());
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener address should be available");
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("test server should accept a connection");
            let fallback = tower::service_fn(|_| async { Ok::<_, Infallible>(Response::new(s3s::Body::empty())) });
            server_http1::Builder::new()
                .serve_connection(TokioIo::new(socket), TowerToHyperService::new(InternodeRpcService::new(fallback)))
                .await
                .expect("test connection should complete");
        });

        let stream = TcpStream::connect(addr).await.expect("test client should connect");
        let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream))
            .await
            .expect("HTTP/1 handshake should succeed");
        let client = tokio::spawn(async move {
            connection.await.expect("test client connection should complete");
        });

        let challenge = uuid::Uuid::new_v4();
        let capability_query = format!(
            "{}={}&{}={challenge}",
            super::PUT_FILE_CAPABILITY_QUERY,
            super::PUT_FILE_CAPABILITY_VERSION,
            super::PUT_FILE_CAPABILITY_CHALLENGE_QUERY
        );
        let capability_uri = format!("{PUT_FILE_CAPABILITY_PATH}?{capability_query}");
        let mut capability_request = Request::builder()
            .method(Method::GET)
            .uri(&capability_uri)
            .header(http::header::HOST, addr.to_string())
            .body(Empty::<Bytes>::new())
            .expect("capability request should build");
        capability_request
            .headers_mut()
            .extend(gen_signature_headers(&capability_uri, &Method::GET).expect("capability signature should build"));
        let response = sender
            .send_request(capability_request)
            .await
            .expect("capability request should complete");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !response
                .into_body()
                .collect()
                .await
                .expect("capability response should drain")
                .to_bytes()
                .is_empty()
        );

        let legacy_probe_uri = format!("{PUT_FILE_STREAM_PATH}?{capability_query}");
        let mut legacy_probe_request = Request::builder()
            .method(Method::GET)
            .uri(&legacy_probe_uri)
            .header(http::header::HOST, addr.to_string())
            .body(Empty::<Bytes>::new())
            .expect("legacy-path probe request should build");
        legacy_probe_request
            .headers_mut()
            .extend(gen_signature_headers(&legacy_probe_uri, &Method::GET).expect("legacy-path signature should build"));
        let response = sender
            .send_request(legacy_probe_request)
            .await
            .expect("legacy-path probe should complete");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        response
            .into_body()
            .collect()
            .await
            .expect("legacy-path response should drain");

        for (server_epoch, expected_status) in [
            (None, StatusCode::CONFLICT),
            (Some(uuid::Uuid::new_v4()), StatusCode::CONFLICT),
            (Some(*super::PUT_FILE_CAPABILITY_SERVER_EPOCH), StatusCode::BAD_REQUEST),
        ] {
            let nonce = uuid::Uuid::new_v4();
            let mut uri = format!(
                "{PUT_FILE_AUTH_STREAM_PATH}?disk=definitely-missing&volume=bucket&path=object&append=false&size=0&put_file_auth=digest-trailer-v1&put_file_nonce={nonce}"
            );
            if let Some(server_epoch) = server_epoch {
                uri.push_str(&format!("&put_file_server_epoch={server_epoch}"));
            }
            let headers = gen_signature_headers(&uri, &Method::PUT).expect("request signature should build");
            let mut request = Request::builder()
                .method(Method::PUT)
                .uri(&uri)
                .header(http::header::HOST, addr.to_string())
                .body(Empty::<Bytes>::new())
                .expect("test request should build");
            request.headers_mut().extend(headers);

            let response = sender.send_request(request).await.expect("test request should complete");
            assert_eq!(response.status(), expected_status, "unexpected status for server epoch {server_epoch:?}");
            response.into_body().collect().await.expect("response body should drain");
        }

        drop(sender);
        client.await.expect("test client task should join");
        server.await.expect("test server task should join");
    }

    struct DropNotifier(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropNotifier {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    struct GatedPutBody {
        started: Option<tokio::sync::oneshot::Sender<()>>,
        release: tokio::sync::oneshot::Receiver<()>,
        payload: Option<Bytes>,
    }

    impl futures_util::Stream for GatedPutBody {
        type Item = io::Result<Bytes>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(started) = self.started.take() {
                let _ = started.send(());
            }
            if self.payload.is_none() {
                return Poll::Ready(None);
            }
            match Pin::new(&mut self.release).poll(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(self.payload.take().map(Ok)),
                Poll::Ready(Err(_)) => Poll::Ready(Some(Err(io::Error::other("gated body release dropped")))),
                Poll::Pending => Poll::Pending,
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
        assert_eq!(
            internode_http_operation(PUT_FILE_AUTH_STREAM_PATH),
            Some(INTERNODE_OPERATION_PUT_FILE_STREAM)
        );
        assert_eq!(
            internode_http_operation(PUT_FILE_CAPABILITY_PATH),
            Some(INTERNODE_OPERATION_PUT_FILE_CAPABILITY)
        );
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
    fn put_file_capability_get_requires_signature() {
        let challenge = uuid::Uuid::new_v4();
        let uri: Uri = format!(
            "{PUT_FILE_CAPABILITY_PATH}?{}={}&{}={challenge}",
            super::PUT_FILE_CAPABILITY_QUERY,
            super::PUT_FILE_CAPABILITY_VERSION,
            super::PUT_FILE_CAPABILITY_CHALLENGE_QUERY
        )
        .parse()
        .expect("uri");
        let response = verify_internode_rpc_signature(&uri, &Method::GET, &HeaderMap::new()).expect_err("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn put_file_capability_response_is_authenticated() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-capability-server-test-secret".to_string());
        let challenge = uuid::Uuid::new_v4();
        let response = put_file_capability_response(challenge);
        assert_eq!(response.status(), StatusCode::OK);
        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .expect("response body should collect")
            .to_bytes();
        let capability: super::PutFileCapabilityResponse = rmp_serde::from_slice(&body).expect("capability should decode");
        assert_eq!(capability.version, super::PUT_FILE_CAPABILITY_VERSION);
        assert!(!capability.server_epoch.is_nil());
        crate::storage::storage_api::ecstore_rpc::verify_put_file_capability(
            challenge,
            capability.server_epoch,
            capability.version,
            &capability.proof,
        )
        .expect("capability proof should verify");
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
    fn rpc_get_request_auth_failure_records_failure_reason_metric() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let uri: Uri = READ_FILE_STREAM_PATH.parse().expect("uri");
        let headers = HeaderMap::new();

        with_local_recorder(&recorder, || {
            let response = verify_internode_rpc_signature(&uri, &Method::GET, &headers).expect_err("response");
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        });

        let entries: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _, _, _)| composite.key().name() == "rustfs_system_network_internode_rpc_auth_failures_total")
            .collect();
        assert_eq!(entries.len(), 1);
        let labels: HashMap<_, _> = entries[0]
            .0
            .key()
            .labels()
            .map(|label| (label.key().to_string(), label.value().to_string()))
            .collect();
        assert_eq!(labels.get("operation").map(String::as_str), Some(INTERNODE_OPERATION_READ_FILE_STREAM));
        assert_eq!(labels.get("backend").map(String::as_str), Some(INTERNODE_TRANSPORT_BACKEND_TCP_HTTP));
        assert_eq!(labels.get("failure_reason").map(String::as_str), Some("missing_v1_signature"));
        assert!(labels.get("server").is_some_and(|value| !value.is_empty()));
    }

    #[test]
    fn put_file_stage_error_message_includes_stage_and_request_context() {
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: ".rustfs.sys/tmp".to_string(),
            path: "tmp/object/part.1".to_string(),
            append: false,
            size: 1024,
            put_file_auth: None,
            put_file_nonce: None,
            put_file_server_epoch: None,
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
            put_file_auth: None,
            put_file_nonce: None,
            put_file_server_epoch: None,
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
        assert_eq!(
            internode_rpc_subsystem(Some(INTERNODE_OPERATION_PUT_FILE_CAPABILITY)),
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

    #[test]
    fn put_file_auth_nonce_accepts_v1_requests_with_non_nil_nonce() {
        let nonce = uuid::Uuid::new_v4();
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: false,
            size: 11,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(nonce),
            put_file_server_epoch: Some(*super::PUT_FILE_CAPABILITY_SERVER_EPOCH),
        };

        assert_eq!(put_file_auth_nonce(&query).expect("v1 auth should parse"), Some(nonce));
        assert!(put_file_server_epoch_matches(&query));

        let mut stale_epoch = query.clone();
        stale_epoch.put_file_server_epoch = Some(uuid::Uuid::new_v4());
        assert!(!put_file_server_epoch_matches(&stale_epoch));

        let mut missing_epoch = query.clone();
        missing_epoch.put_file_server_epoch = None;
        assert!(!put_file_server_epoch_matches(&missing_epoch));

        let mut append = query.clone();
        append.append = true;
        assert_eq!(put_file_auth_nonce(&append).expect("append auth should parse"), Some(nonce));

        let mut unknown_size = query.clone();
        unknown_size.size = -1;
        assert_eq!(put_file_auth_nonce(&unknown_size).expect("unknown-size auth should parse"), Some(nonce));

        let mut nil = query.clone();
        nil.put_file_nonce = Some(uuid::Uuid::nil());
        assert!(put_file_auth_nonce(&nil).is_err());

        let mut unknown = query;
        unknown.put_file_auth = Some("digest-trailer-v2".to_string());
        assert!(put_file_auth_nonce(&unknown).is_err());
    }

    #[tokio::test]
    async fn put_file_auth_body_writes_only_data_and_verifies_trailer() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-auth-body-test-secret".to_string());
        let nonce = uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("nonce");
        let url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=false&size=11&put_file_auth=digest-trailer-v1&put_file_nonce=11111111-2222-4333-8444-555555555555"
        );
        let digest = hex_simd::encode_to_string(sha2::Sha256::digest(b"hello world"), hex_simd::AsciiCase::Lower);
        let trailer = build_put_file_auth_trailer(url, &Method::PUT, nonce, &digest).expect("trailer should build");
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: false,
            size: 11,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(nonce),
            put_file_server_epoch: Some(*super::PUT_FILE_CAPABILITY_SERVER_EPOCH),
        };
        let mut second = b"world".to_vec();
        second.extend_from_slice(&trailer[..7]);
        let body = iter(vec![
            Ok::<Bytes, io::Error>(Bytes::from_static(b"hello ")),
            Ok(Bytes::from(second)),
            Ok(Bytes::copy_from_slice(&trailer[7..])),
        ]);
        let mut writer = Vec::new();

        let copied = write_put_file_body_chunks_to_writer(body, &mut writer, &query, Some(nonce), url)
            .await
            .expect("authenticated body should verify");

        assert_eq!(copied, 11);
        assert_eq!(writer, b"hello world");
    }

    #[tokio::test]
    async fn put_file_auth_body_rejects_tampered_data() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-auth-body-test-secret".to_string());
        let nonce = uuid::Uuid::parse_str("22222222-3333-4444-8555-666666666666").expect("nonce");
        let url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=false&size=11&put_file_auth=digest-trailer-v1&put_file_nonce=22222222-3333-4444-8555-666666666666"
        );
        let signed_digest = hex_simd::encode_to_string(sha2::Sha256::digest(b"hello world"), hex_simd::AsciiCase::Lower);
        let trailer = build_put_file_auth_trailer(url, &Method::PUT, nonce, &signed_digest).expect("trailer should build");
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: false,
            size: 11,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(nonce),
            put_file_server_epoch: Some(*super::PUT_FILE_CAPABILITY_SERVER_EPOCH),
        };
        let mut payload = b"hello worle".to_vec();
        payload.extend_from_slice(&trailer);
        let body = iter(vec![Ok::<Bytes, io::Error>(Bytes::from(payload))]);
        let mut writer = Vec::new();

        let err = write_put_file_body_chunks_to_writer(body, &mut writer, &query, Some(nonce), url)
            .await
            .expect_err("tampered body must fail digest verification");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "put_file body digest mismatch");
    }

    #[tokio::test]
    async fn put_file_auth_append_body_uses_trailing_auth_record() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-auth-body-test-secret".to_string());
        let nonce = uuid::Uuid::parse_str("33333333-4444-4555-8666-777777777777").expect("nonce");
        let url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=true&size=0&put_file_auth=digest-trailer-v1&put_file_nonce=33333333-4444-4555-8666-777777777777"
        );
        let digest = hex_simd::encode_to_string(sha2::Sha256::digest(b"append-data"), hex_simd::AsciiCase::Lower);
        let trailer = build_put_file_auth_trailer(url, &Method::PUT, nonce, &digest).expect("trailer should build");
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: true,
            size: 0,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(nonce),
            put_file_server_epoch: Some(*super::PUT_FILE_CAPABILITY_SERVER_EPOCH),
        };
        let mut payload = b"append-data".to_vec();
        payload.extend_from_slice(&trailer);
        let body = iter(vec![Ok::<Bytes, io::Error>(Bytes::from(payload))]);
        let mut writer = Vec::new();

        let copied = write_put_file_body_chunks_to_writer(body, &mut writer, &query, Some(nonce), url)
            .await
            .expect("append body should verify");

        assert_eq!(copied, 11);
        assert_eq!(writer, b"append-data");
    }

    #[tokio::test]
    async fn put_file_auth_zero_size_create_uses_trailing_auth_record() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-auth-body-test-secret".to_string());
        let nonce = uuid::Uuid::parse_str("43434343-4444-4555-8666-777777777777").expect("nonce");
        let url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=false&size=0&put_file_auth=digest-trailer-v1&put_file_nonce=43434343-4444-4555-8666-777777777777"
        );
        let digest = hex_simd::encode_to_string(sha2::Sha256::digest(b"unknown-size-data"), hex_simd::AsciiCase::Lower);
        let trailer = build_put_file_auth_trailer(url, &Method::PUT, nonce, &digest).expect("trailer should build");
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: false,
            size: 0,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(nonce),
            put_file_server_epoch: Some(*super::PUT_FILE_CAPABILITY_SERVER_EPOCH),
        };
        let mut payload = b"unknown-size-data".to_vec();
        payload.extend_from_slice(&trailer);
        let body = iter(vec![Ok::<Bytes, io::Error>(Bytes::from(payload))]);
        let mut writer = Vec::new();

        let copied = write_put_file_body_chunks_to_writer(body, &mut writer, &query, Some(nonce), url)
            .await
            .expect("zero-size create body should verify");

        assert_eq!(copied, 17);
        assert_eq!(writer, b"unknown-size-data");
    }

    #[tokio::test]
    async fn put_file_auth_append_body_rejects_missing_trailer() {
        let nonce = uuid::Uuid::parse_str("44444444-5555-4666-8777-888888888888").expect("nonce");
        let url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=true&size=0&put_file_auth=digest-trailer-v1&put_file_nonce=44444444-5555-4666-8777-888888888888"
        );
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: true,
            size: 0,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(nonce),
            put_file_server_epoch: Some(*super::PUT_FILE_CAPABILITY_SERVER_EPOCH),
        };
        let body = iter(vec![Ok::<Bytes, io::Error>(Bytes::from_static(b"append-data"))]);
        let mut writer = Vec::new();

        let err = write_put_file_body_chunks_to_writer(body, &mut writer, &query, Some(nonce), url)
            .await
            .expect_err("missing trailer must fail");

        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(err.to_string(), "put_file auth trailer is incomplete");
    }

    #[tokio::test]
    async fn authenticated_create_tamper_preserves_existing_local_file() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-auth-body-test-secret".to_string());
        let (disk, _dir) = new_put_file_test_disk().await;
        disk.write_all("bucket", "object/part.1", Bytes::from_static(b"old-data"))
            .await
            .expect("existing data should be written");
        let nonce = uuid::Uuid::parse_str("55555555-6666-4777-8888-999999999999").expect("nonce");
        let url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=false&size=11&put_file_auth=digest-trailer-v1&put_file_nonce=55555555-6666-4777-8888-999999999999"
        );
        let signed_digest = hex_simd::encode_to_string(sha2::Sha256::digest(b"hello world"), hex_simd::AsciiCase::Lower);
        let trailer = build_put_file_auth_trailer(url, &Method::PUT, nonce, &signed_digest).expect("trailer should build");
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: false,
            size: 11,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(nonce),
            put_file_server_epoch: None,
        };
        let mut payload = b"hello worle".to_vec();
        payload.extend_from_slice(&trailer);

        let err =
            write_authenticated_put_file(&disk, iter(vec![Ok::<Bytes, io::Error>(Bytes::from(payload))]), &query, nonce, url)
                .await
                .expect_err("tampered body must not publish");

        assert_eq!(err.0, "write_body");
        assert_eq!(
            disk.read_all("bucket", "object/part.1")
                .await
                .expect("existing data should remain"),
            Bytes::from_static(b"old-data")
        );
    }

    #[tokio::test]
    async fn authenticated_create_truncation_preserves_existing_local_file() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-auth-body-test-secret".to_string());
        let (disk, _dir) = new_put_file_test_disk().await;
        disk.write_all("bucket", "object/part.1", Bytes::from_static(b"old-data"))
            .await
            .expect("existing data should be written");
        let nonce = uuid::Uuid::parse_str("66666666-7777-4888-8999-aaaaaaaaaaaa").expect("nonce");
        let url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=false&size=11&put_file_auth=digest-trailer-v1&put_file_nonce=66666666-7777-4888-8999-aaaaaaaaaaaa"
        );
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: false,
            size: 11,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(nonce),
            put_file_server_epoch: None,
        };

        let err = write_authenticated_put_file(
            &disk,
            iter(vec![Ok::<Bytes, io::Error>(Bytes::from_static(b"short"))]),
            &query,
            nonce,
            url,
        )
        .await
        .expect_err("truncated body must not publish");

        assert_eq!(err.0, "write_body");
        assert_eq!(
            disk.read_all("bucket", "object/part.1")
                .await
                .expect("existing data should remain"),
            Bytes::from_static(b"old-data")
        );
    }

    #[tokio::test]
    async fn authenticated_append_missing_trailer_preserves_existing_local_file() {
        let (disk, _dir) = new_put_file_test_disk().await;
        disk.write_all("bucket", "object/part.1", Bytes::from_static(b"old-data"))
            .await
            .expect("existing data should be written");
        let nonce = uuid::Uuid::parse_str("77777777-8888-4999-8aaa-bbbbbbbbbbbb").expect("nonce");
        let url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=true&size=0&put_file_auth=digest-trailer-v1&put_file_nonce=77777777-8888-4999-8aaa-bbbbbbbbbbbb"
        );
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: true,
            size: 0,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(nonce),
            put_file_server_epoch: None,
        };

        let err = write_authenticated_put_file(
            &disk,
            iter(vec![Ok::<Bytes, io::Error>(Bytes::from_static(b"append-data"))]),
            &query,
            nonce,
            url,
        )
        .await
        .expect_err("missing trailer must not publish");

        assert_eq!(err.0, "write_body");
        assert_eq!(
            disk.read_all("bucket", "object/part.1")
                .await
                .expect("existing data should remain"),
            Bytes::from_static(b"old-data")
        );
    }

    #[tokio::test]
    async fn authenticated_append_publishes_existing_and_new_bytes() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-auth-body-test-secret".to_string());
        let (disk, _dir) = new_put_file_test_disk().await;
        disk.write_all("bucket", "object/part.1", Bytes::from_static(b"old-data"))
            .await
            .expect("existing data should be written");
        let nonce = uuid::Uuid::parse_str("88888888-9999-4aaa-8bbb-cccccccccccc").expect("nonce");
        let url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=true&size=0&put_file_auth=digest-trailer-v1&put_file_nonce=88888888-9999-4aaa-8bbb-cccccccccccc"
        );
        let digest = hex_simd::encode_to_string(sha2::Sha256::digest(b"append-data"), hex_simd::AsciiCase::Lower);
        let trailer = build_put_file_auth_trailer(url, &Method::PUT, nonce, &digest).expect("trailer should build");
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: true,
            size: 0,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(nonce),
            put_file_server_epoch: None,
        };
        let mut payload = b"append-data".to_vec();
        payload.extend_from_slice(&trailer);

        let copied =
            write_authenticated_put_file(&disk, iter(vec![Ok::<Bytes, io::Error>(Bytes::from(payload))]), &query, nonce, url)
                .await
                .expect("authenticated append should publish");

        assert_eq!(copied, 11);
        assert_eq!(
            disk.read_all("bucket", "object/part.1")
                .await
                .expect("published data should be readable"),
            Bytes::from_static(b"old-dataappend-data")
        );
    }

    #[tokio::test]
    async fn concurrent_authenticated_appends_preserve_both_payloads_without_staging() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-auth-body-test-secret".to_string());
        let (disk, dir) = new_put_file_test_disk().await;
        disk.write_all("bucket", "object/part.1", Bytes::from_static(b"old-data"))
            .await
            .expect("existing data should be written");

        let first_nonce = uuid::Uuid::parse_str("99999999-aaaa-4bbb-8ccc-dddddddddddd").expect("first nonce");
        let first_url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=true&size=0&put_file_auth=digest-trailer-v1&put_file_nonce=99999999-aaaa-4bbb-8ccc-dddddddddddd"
        );
        let first_digest = hex_simd::encode_to_string(sha2::Sha256::digest(b"first"), hex_simd::AsciiCase::Lower);
        let first_trailer =
            build_put_file_auth_trailer(first_url, &Method::PUT, first_nonce, &first_digest).expect("first trailer should build");
        let mut first_payload = b"first".to_vec();
        first_payload.extend_from_slice(&first_trailer);

        let second_nonce = uuid::Uuid::parse_str("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee").expect("second nonce");
        let second_url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=true&size=0&put_file_auth=digest-trailer-v1&put_file_nonce=aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        );
        let second_digest = hex_simd::encode_to_string(sha2::Sha256::digest(b"second"), hex_simd::AsciiCase::Lower);
        let second_trailer = build_put_file_auth_trailer(second_url, &Method::PUT, second_nonce, &second_digest)
            .expect("second trailer should build");
        let mut second_payload = b"second".to_vec();
        second_payload.extend_from_slice(&second_trailer);

        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: true,
            size: 0,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(first_nonce),
            put_file_server_epoch: None,
        };
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let first_disk = disk.clone();
        let first_query = query.clone();
        let first = tokio::spawn(async move {
            write_authenticated_put_file(
                &first_disk,
                GatedPutBody {
                    started: Some(started_tx),
                    release: release_rx,
                    payload: Some(Bytes::from(first_payload)),
                },
                &first_query,
                first_nonce,
                first_url,
            )
            .await
        });
        started_rx.await.expect("first append should start reading its body");

        let second_disk = disk.clone();
        let mut second_query = query;
        second_query.put_file_nonce = Some(second_nonce);
        let second = tokio::spawn(async move {
            write_authenticated_put_file(
                &second_disk,
                iter(vec![Ok::<Bytes, io::Error>(Bytes::from(second_payload))]),
                &second_query,
                second_nonce,
                second_url,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!second.is_finished(), "second append must wait while the first holds the target lock");

        release_tx.send(()).expect("first append should still be waiting");
        assert_eq!(
            first
                .await
                .expect("first append task should join")
                .expect("first append should succeed"),
            5
        );
        assert_eq!(
            second
                .await
                .expect("second append task should join")
                .expect("second append should succeed"),
            6
        );
        let final_data = disk
            .read_all("bucket", "object/part.1")
            .await
            .expect("final data should be readable");
        assert!(
            final_data == Bytes::from_static(b"old-datafirstsecond") || final_data == Bytes::from_static(b"old-datasecondfirst"),
            "both complete append payloads must be preserved: {final_data:?}"
        );

        let object_dir = dir.path().join("bucket/object");
        let entries = std::fs::read_dir(object_dir).expect("object directory should be readable");
        assert!(
            entries
                .map(|entry| entry.expect("directory entry should be readable").file_name())
                .all(|name| !name.to_string_lossy().starts_with(".rustfs-put-")),
            "successful concurrent appends must not leave staging files"
        );
    }

    #[tokio::test]
    async fn legacy_and_authenticated_appends_share_the_target_lock() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-auth-body-test-secret".to_string());
        let (disk, _dir) = new_put_file_test_disk().await;
        disk.write_all("bucket", "object/part.1", Bytes::from_static(b"old-data"))
            .await
            .expect("existing data should be written");
        let nonce = uuid::Uuid::parse_str("bbbbbbbb-cccc-4ddd-8eee-ffffffffffff").expect("nonce");
        let url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=true&size=0&put_file_auth=digest-trailer-v1&put_file_nonce=bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
        );
        let digest = hex_simd::encode_to_string(sha2::Sha256::digest(b"authenticated"), hex_simd::AsciiCase::Lower);
        let trailer = build_put_file_auth_trailer(url, &Method::PUT, nonce, &digest).expect("trailer should build");
        let mut payload = b"authenticated".to_vec();
        payload.extend_from_slice(&trailer);
        let query = PutFileQuery {
            disk: "disk-a".to_string(),
            volume: "bucket".to_string(),
            path: "object/part.1".to_string(),
            append: true,
            size: 0,
            put_file_auth: Some("digest-trailer-v1".to_string()),
            put_file_nonce: Some(nonce),
            put_file_server_epoch: None,
        };

        let legacy_lock = put_file_target_lock(&disk, &query);
        let (legacy_started_tx, legacy_started_rx) = tokio::sync::oneshot::channel();
        let (legacy_release_tx, legacy_release_rx) = tokio::sync::oneshot::channel();
        let legacy_disk = disk.clone();
        let legacy = tokio::spawn(async move {
            let _guard = legacy_lock.lock_owned().await;
            legacy_started_tx.send(()).expect("test should observe legacy lock");
            legacy_release_rx.await.expect("legacy append should be released");
            let mut file = legacy_disk
                .append_file("bucket", "object/part.1")
                .await
                .expect("legacy append should open");
            file.write_all(b"legacy").await.expect("legacy append should write");
            file.shutdown().await.expect("legacy append should finish");
        });
        legacy_started_rx.await.expect("legacy append should hold the target lock");

        let authenticated_disk = disk.clone();
        let authenticated_query = query.clone();
        let authenticated = tokio::spawn(async move {
            write_authenticated_put_file(
                &authenticated_disk,
                iter(vec![Ok::<Bytes, io::Error>(Bytes::from(payload))]),
                &authenticated_query,
                nonce,
                url,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!authenticated.is_finished(), "authenticated append must wait for legacy append");

        legacy_release_tx.send(()).expect("legacy append should still be waiting");
        legacy.await.expect("legacy append task should join");
        authenticated
            .await
            .expect("authenticated append task should join")
            .expect("authenticated append should succeed");
        assert_eq!(
            disk.read_all("bucket", "object/part.1")
                .await
                .expect("final data should be readable"),
            Bytes::from_static(b"old-datalegacyauthenticated")
        );
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
        let reader = RejectExtraPollReader { emitted: false };
        let mut stream = read_file_body_stream(reader, 5, INTERNODE_OPERATION_READ_FILE_STREAM);
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk.expect("chunk succeeds"));
        }

        assert_eq!(out, b"hello");
    }

    #[test]
    fn read_file_body_stream_sizes_buffer_to_requested_length() {
        for (length, expected_capacity) in [
            (0, DEFAULT_READ_BUFFER_SIZE),
            (40 * 1024, 40 * 1024),
            (DEFAULT_READ_BUFFER_SIZE, DEFAULT_READ_BUFFER_SIZE),
            (DEFAULT_READ_BUFFER_SIZE + 1, DEFAULT_READ_BUFFER_SIZE),
        ] {
            assert_eq!(read_file_stream_buffer_size(length), expected_capacity);
        }
    }

    #[test]
    fn read_file_error_response_marks_only_missing_disk_errors() {
        for (error, expected) in [
            (DiskError::FileNotFound, rustfs_rio::INTERNODE_FILE_NOT_FOUND),
            (DiskError::VolumeNotFound, rustfs_rio::INTERNODE_VOLUME_NOT_FOUND),
        ] {
            let response = response_with_disk_error(&error, error.to_string());
            assert_eq!(
                response.headers().get(rustfs_rio::INTERNODE_DISK_ERROR_HEADER),
                Some(&HeaderValue::from_static(expected))
            );
        }

        let response = response_with_disk_error(&DiskError::DiskAccessDenied, "permission denied");
        assert!(response.headers().get(rustfs_rio::INTERNODE_DISK_ERROR_HEADER).is_none());
    }
}
