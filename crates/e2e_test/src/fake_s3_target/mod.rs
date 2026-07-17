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

//! Programmable in-process S3 target for replication failure-path tests.
//!
//! See [`README.md`](README.md) for the supported protocol and fault surface.

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use http::header::{CONTENT_LENGTH, ETAG, LAST_MODIFIED};
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Uri};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::SimpleAuth;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, CompleteMultipartUploadInput, CompleteMultipartUploadOutput,
    CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteObjectInput, DeleteObjectOutput, ETag,
    GetBucketVersioningInput, GetBucketVersioningOutput, GetObjectInput, GetObjectOutput, HeadBucketInput, HeadBucketOutput,
    HeadObjectInput, HeadObjectOutput, PutObjectInput, PutObjectOutput, StreamingBlob, Timestamp, TimestampFormat,
    UploadPartInput, UploadPartOutput,
};
use s3s::service::{S3Service, S3ServiceBuilder};
use s3s::validation::{AwsNameValidation, NameValidation};
use s3s::{Body, S3, S3Request, S3Response, S3Result};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

type BoxError = Box<dyn Error + Send + Sync>;

const MAX_BUFFERED_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_TOTAL_STORED_BYTES: usize = 128 * 1024 * 1024;
const MAX_OBJECT_VERSIONS: usize = 4096;
const MAX_MULTIPART_UPLOADS: usize = 256;
const MAX_MULTIPART_PARTS: usize = 10_000;
const MAX_BUCKETS: usize = 256;
const MIN_MULTIPART_PART_BYTES: usize = 5 * 1024 * 1024;
const MAX_REQUEST_RECORDS: usize = 4096;
const MAX_SCRIPTED_FAULTS: usize = 4096;
const MAX_RETAINED_IDENTIFIER_BYTES: usize = 1024;
const MAX_STORED_METADATA_BYTES: usize = 2 * 1024;
const MAX_CONTENT_TYPE_BYTES: usize = 1024;
const MAX_CONNECTIONS: usize = 64;
const MAX_CONCURRENT_BODY_REQUESTS: usize = MAX_TOTAL_STORED_BYTES / MAX_BUFFERED_BODY_BYTES;
const MAX_FAULT_DURATION: Duration = Duration::from_secs(30);
const MAX_REQUEST_DURATION: Duration = Duration::from_secs(65);
const MAX_CONNECTION_DURATION: Duration = Duration::from_secs(100);
const DISCONNECT_HEADER: &str = "x-rustfs-fake-target-disconnect";
const WRONG_ETAG: &str = "\"fake-target-wrong-etag\"";
const SOURCE_VERSION_ID_HEADERS: [&str; 2] = ["x-rustfs-source-version-id", "x-minio-source-version-id"];
const SOURCE_MTIME_HEADERS: [&str; 2] = ["x-rustfs-source-mtime", "x-minio-source-mtime"];
const SOURCE_REPLICATION_REQUEST_HEADERS: [&str; 2] =
    ["x-rustfs-source-replication-request", "x-minio-source-replication-request"];
const SOURCE_ETAG_HEADERS: [&str; 2] = ["x-rustfs-source-etag", "x-minio-source-etag"];
const RESERVED_BUCKET_PREFIXES: [&str; 3] = ["xn--", "sthree-", "amzn-s3-demo-"];
const RESERVED_BUCKET_SUFFIXES: [&str; 6] = ["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3", "-an"];

pub const FAKE_ACCESS_KEY: &str = "fake-access";
pub const FAKE_SECRET_KEY: &str = "fake-secret";

/// S3 operations understood by the target and its request journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    HeadBucket,
    GetBucketVersioning,
    PutObject,
    GetObject,
    HeadObject,
    DeleteObject,
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    Unknown,
}

/// One fault consumed by the next matching operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultAction {
    /// Return the exact HTTP status without entering the S3 backend.
    Status(StatusCode),
    /// Wait before dispatching the request normally.
    Delay(Duration),
    /// Close once the body stream reaches this logical byte threshold. Hyper may
    /// already have buffered the rest of the current frame; the journal reports
    /// the threshold, and the backend never receives or stores the request.
    DisconnectAfterBytes(usize),
    /// Drain a request body in fixed-size slices, sleeping after every slice.
    SlowDrain { chunk_bytes: usize, delay: Duration },
    /// Store the request normally but replace the response ETag.
    WrongEtag,
}

/// Credential-free request metadata retained for deterministic assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestRecord {
    pub sequence: u64,
    pub operation: Operation,
    pub method: Method,
    pub bucket: String,
    pub key: Option<String>,
    pub version_id: Option<String>,
    pub upload_id: Option<String>,
    pub part_number: Option<i32>,
    pub content_length: Option<u64>,
    pub consumed_bytes: Option<usize>,
    pub fault: Option<FaultAction>,
}

#[derive(Default)]
struct ControlState {
    scripts: HashMap<Operation, VecDeque<FaultAction>>,
    requests: VecDeque<RequestRecord>,
    next_sequence: u64,
}

#[derive(Default)]
struct StoreState {
    buckets: HashMap<String, BucketState>,
    uploads: HashMap<String, MultipartState>,
    total_bytes: usize,
    total_versions: usize,
    total_parts: usize,
}

#[derive(Default)]
struct BucketState {
    objects: HashMap<String, Vec<ObjectVersion>>,
}

#[derive(Clone)]
struct ObjectVersion {
    version_id: String,
    body: Bytes,
    e_tag: String,
    last_modified: Timestamp,
    delete_marker: bool,
    content_type: Option<String>,
    metadata: Option<HashMap<String, String>>,
}

#[derive(Clone)]
struct MultipartState {
    bucket: String,
    key: String,
    version_id: String,
    content_type: Option<String>,
    metadata: Option<HashMap<String, String>>,
    parts: BTreeMap<i32, MultipartPart>,
}

#[derive(Clone)]
struct MultipartPart {
    body: Bytes,
    e_tag: String,
    digest: [u8; 16],
}

#[derive(Clone)]
struct FakeBackend {
    store: Arc<Mutex<StoreState>>,
    control: Arc<Mutex<ControlState>>,
    body_limit: Arc<Semaphore>,
}

#[derive(Clone)]
struct FaultAccess {
    control: Arc<Mutex<ControlState>>,
    body_limit: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
struct RequestFault {
    sequence: u64,
    action: FaultAction,
}

#[derive(Clone)]
struct PrebodyPermit(Arc<Mutex<Option<OwnedSemaphorePermit>>>);

impl PrebodyPermit {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self(Arc::new(Mutex::new(Some(permit))))
    }

    fn take(self) -> Option<OwnedSemaphorePermit> {
        lock(&self.0).take()
    }
}

struct ConnectionPermit {
    permit: Option<OwnedSemaphorePermit>,
    active_connections: watch::Sender<usize>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.active_connections.send_modify(|count| {
            *count = count.checked_sub(1).expect("active connection count underflow");
        });
    }
}

/// Running target plus its fault controls and request journal.
pub struct FakeS3Target {
    endpoint: String,
    address: SocketAddr,
    control: Arc<Mutex<ControlState>>,
    backend: FakeBackend,
    connection_limit: Arc<Semaphore>,
    active_connections: watch::Receiver<usize>,
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl FakeS3Target {
    /// Bind a new target on a random loopback port.
    pub async fn start() -> Result<Self, BoxError> {
        Self::start_with_connection_gate(MAX_CONNECTION_DURATION, None).await
    }

    async fn start_with_connection_gate(
        connection_duration: Duration,
        connection_gate: Option<watch::Receiver<bool>>,
    ) -> Result<Self, BoxError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let endpoint = format!("http://{address}");
        let control = Arc::new(Mutex::new(ControlState::default()));
        let body_limit = Arc::new(Semaphore::new(MAX_CONCURRENT_BODY_REQUESTS));
        let backend = FakeBackend {
            store: Arc::new(Mutex::new(StoreState::default())),
            control: Arc::clone(&control),
            body_limit: Arc::clone(&body_limit),
        };
        let service = {
            let mut builder = S3ServiceBuilder::new(backend.clone());
            builder.set_auth(SimpleAuth::from_single(FAKE_ACCESS_KEY, FAKE_SECRET_KEY));
            builder.set_access(FaultAccess {
                control: Arc::clone(&control),
                body_limit,
            });
            builder.build()
        };
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let connection_limit = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let (active_connections_tx, active_connections) = watch::channel(0usize);
        let task_connection_limit = Arc::clone(&connection_limit);

        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let Ok(permit) = Arc::clone(&task_connection_limit).try_acquire_owned() else {
                            drop(stream);
                            continue;
                        };
                        active_connections_tx.send_modify(|count| *count += 1);
                        let service = service.clone();
                        let active_connections = active_connections_tx.clone();
                        let mut connection_gate = connection_gate.clone();
                        connections.spawn(async move {
                            let _permit = ConnectionPermit {
                                permit: Some(permit),
                                active_connections,
                            };
                            if let Some(gate) = &mut connection_gate
                                && !*gate.borrow_and_update()
                                && gate.changed().await.is_err()
                            {
                                return;
                            }
                            let handler = service_fn(move |request| handle_request(request, service.clone()));
                            let mut connection = http1::Builder::new();
                            connection
                                .keep_alive(false)
                                .timer(TokioTimer::new())
                                .header_read_timeout(MAX_FAULT_DURATION);
                            let served = connection.serve_connection(TokioIo::new(stream), handler);
                            let _ = timeout(connection_duration, served).await;
                        });
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });

        Ok(Self {
            endpoint,
            address,
            control,
            backend,
            connection_limit,
            active_connections,
            shutdown,
            task: Some(task),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Host and port format expected by RustFS remote-target configuration.
    pub fn address(&self) -> String {
        self.address.to_string()
    }

    /// Pre-create a general-purpose bucket in the shared global namespace.
    /// Account-regional namespace buckets are intentionally not modeled.
    pub fn create_bucket(&self, bucket: impl Into<String>) {
        let bucket = bucket.into();
        assert!(valid_bucket_name(&bucket), "fake target bucket name must be S3-valid");
        let mut state = lock(&self.backend.store);
        if !state.buckets.contains_key(&bucket) {
            assert!(state.buckets.len() < MAX_BUCKETS, "fake target retains at most 256 buckets");
        }
        state.buckets.entry(bucket).or_default();
    }

    /// Queue `times` copies of a fault for one operation.
    pub fn inject(&self, operation: Operation, action: FaultAction, times: usize) {
        if times == 0 {
            return;
        }
        if let FaultAction::SlowDrain { chunk_bytes: 0, .. } = action {
            panic!("slow-drain chunk size must be non-zero");
        }
        match &action {
            FaultAction::Delay(duration) if *duration > MAX_FAULT_DURATION => {
                panic!("fault delay must not exceed 30 seconds");
            }
            FaultAction::SlowDrain { delay, .. } if *delay >= MAX_FAULT_DURATION => {
                panic!("slow-drain slice delay must be below 30 seconds");
            }
            _ => {}
        }
        let mut state = lock(&self.control);
        let queued = state.scripts.values().map(VecDeque::len).sum::<usize>();
        if queued.checked_add(times).is_none_or(|total| total > MAX_SCRIPTED_FAULTS) {
            panic!("fake target queues at most 4096 scripted faults");
        }
        state
            .scripts
            .entry(operation)
            .or_default()
            .extend(std::iter::repeat_n(action, times));
    }

    pub fn clear_faults(&self) {
        lock(&self.control).scripts.clear();
    }

    pub fn requests(&self) -> Vec<RequestRecord> {
        lock(&self.control).requests.iter().cloned().collect()
    }

    pub fn take_requests(&self) -> Vec<RequestRecord> {
        lock(&self.control).requests.drain(..).collect()
    }

    pub async fn shutdown(mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

fn valid_bucket_name(bucket: &str) -> bool {
    AwsNameValidation::new().validate_bucket_name(bucket)
        && !RESERVED_BUCKET_PREFIXES.iter().any(|prefix| bucket.starts_with(prefix))
        && !RESERVED_BUCKET_SUFFIXES.iter().any(|suffix| bucket.ends_with(suffix))
}

impl Drop for FakeS3Target {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[async_trait]
impl S3Access for FaultAccess {
    async fn check(&self, context: &mut S3AccessContext<'_>) -> S3Result<()> {
        if context
            .credentials()
            .is_none_or(|credentials| credentials.access_key != FAKE_ACCESS_KEY)
        {
            return Err(s3s::s3_error!(AccessDenied, "valid fixture credentials are required"));
        }

        let parsed = parse_request(context.method(), context.uri());
        let operation = operation_from_s3_name(context.s3_op().name());
        let content_length = context
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        let fault = record_request(&self.control, operation, context.method().clone(), parsed, content_length);
        if let Some(RequestFault {
            action: FaultAction::Status(status),
            ..
        }) = fault.as_ref()
        {
            return Err(scripted_status_error(*status));
        }
        let prebody_permit = if operation == Operation::CompleteMultipartUpload {
            Some(
                timeout(
                    MAX_FAULT_DURATION,
                    Arc::clone(&self.body_limit).acquire_many_owned(MAX_CONCURRENT_BODY_REQUESTS as u32),
                )
                .await
                .map_err(|_| s3s::s3_error!(RequestTimeout, "fake target body limiter wait exceeded 30 seconds"))?
                .map_err(|_| s3s::s3_error!(ServiceUnavailable, "fake target body limiter closed"))?,
            )
        } else {
            None
        };
        if let Some(fault) = fault {
            context.extensions_mut().insert(fault);
        }
        if let Some(permit) = prebody_permit {
            context.extensions_mut().insert(PrebodyPermit::new(permit));
        }
        Ok(())
    }
}

fn operation_from_s3_name(name: &str) -> Operation {
    match name {
        "HeadBucket" => Operation::HeadBucket,
        "GetBucketVersioning" => Operation::GetBucketVersioning,
        "PutObject" => Operation::PutObject,
        "GetObject" => Operation::GetObject,
        "HeadObject" => Operation::HeadObject,
        "DeleteObject" => Operation::DeleteObject,
        "CreateMultipartUpload" => Operation::CreateMultipartUpload,
        "UploadPart" => Operation::UploadPart,
        "CompleteMultipartUpload" => Operation::CompleteMultipartUpload,
        "AbortMultipartUpload" => Operation::AbortMultipartUpload,
        _ => Operation::Unknown,
    }
}

fn record_request(
    control: &Mutex<ControlState>,
    operation: Operation,
    method: Method,
    parsed: ParsedRequest,
    content_length: Option<u64>,
) -> Option<RequestFault> {
    let mut state = lock(control);
    let action = state.scripts.get_mut(&operation).and_then(VecDeque::pop_front);
    state.next_sequence += 1;
    let sequence = state.next_sequence;
    if state.requests.len() == MAX_REQUEST_RECORDS {
        state.requests.pop_front();
    }
    state.requests.push_back(RequestRecord {
        sequence,
        operation,
        method,
        bucket: bounded_journal_value(parsed.bucket),
        key: parsed.key.map(bounded_journal_value),
        version_id: parsed.version_id.map(bounded_journal_value),
        upload_id: parsed.upload_id.map(bounded_journal_value),
        part_number: parsed.part_number,
        content_length,
        consumed_bytes: None,
        fault: action.clone(),
    });
    action.map(|action| RequestFault { sequence, action })
}

fn bounded_journal_value(mut value: String) -> String {
    let mut end = value.len().min(MAX_RETAINED_IDENTIFIER_BYTES);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

async fn handle_request(request: Request<Incoming>, service: S3Service) -> Result<Response<Body>, BoxError> {
    let mut response = timeout(MAX_REQUEST_DURATION, call_s3(service, request.map(Body::from)))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "fake target request exceeded 65 seconds"))??;
    if response.headers_mut().remove(DISCONNECT_HEADER).is_some() {
        return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "fake target scripted disconnect").into());
    }
    Ok(response)
}

async fn call_s3(service: S3Service, request: Request<Body>) -> Result<Response<Body>, BoxError> {
    service.call(request).await.map_err(Into::into)
}

struct ParsedRequest {
    operation: Operation,
    bucket: String,
    key: Option<String>,
    version_id: Option<String>,
    upload_id: Option<String>,
    part_number: Option<i32>,
}

fn parse_request(method: &Method, uri: &Uri) -> ParsedRequest {
    let path = uri.path().trim_start_matches('/');
    let (bucket, key) = match path.split_once('/') {
        Some((bucket, "")) => (decode(bucket), None),
        Some((bucket, key)) => (decode(bucket), Some(decode(key))),
        None => (decode(path), None),
    };
    let query = query_values(uri.query());
    let version_id = query.get("versionId").cloned().flatten();
    let upload_id = query.get("uploadId").cloned().flatten();
    let part_number = query
        .get("partNumber")
        .and_then(Clone::clone)
        .and_then(|value| value.parse().ok());
    let only_query_keys = |allowed: &[&str]| {
        query
            .keys()
            .filter(|name| name.as_str() != "x-id")
            .all(|name| allowed.contains(&name.as_str()))
    };
    let operation = match (method, key.is_some()) {
        (&Method::HEAD, false) => Operation::HeadBucket,
        (&Method::GET, false) if query.contains_key("versioning") => Operation::GetBucketVersioning,
        (&Method::PUT, true) if upload_id.is_some() && part_number.is_some() => Operation::UploadPart,
        (&Method::PUT, true) if upload_id.is_some() || query.contains_key("partNumber") => Operation::Unknown,
        (&Method::POST, true) if query.contains_key("uploads") => Operation::CreateMultipartUpload,
        (&Method::POST, true) if upload_id.is_some() => Operation::CompleteMultipartUpload,
        (&Method::DELETE, true) if upload_id.is_some() => Operation::AbortMultipartUpload,
        (&Method::PUT, true) if only_query_keys(&[]) => Operation::PutObject,
        (&Method::GET, true) if only_query_keys(&["versionId"]) => Operation::GetObject,
        (&Method::HEAD, true) if only_query_keys(&["versionId"]) => Operation::HeadObject,
        (&Method::DELETE, true) if only_query_keys(&["versionId"]) => Operation::DeleteObject,
        _ => Operation::Unknown,
    };
    ParsedRequest {
        operation,
        bucket,
        key,
        version_id,
        upload_id,
        part_number,
    }
}

fn query_values(query: Option<&str>) -> HashMap<String, Option<String>> {
    query
        .unwrap_or_default()
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (name, value) = part
                .split_once('=')
                .map_or((part, None), |(name, value)| (name, Some(decode(value))));
            (decode(name), value)
        })
        .collect()
}

fn decode(value: &str) -> String {
    urlencoding::decode(value).map_or_else(|_| value.to_string(), |value| value.into_owned())
}

fn header_value(headers: &HeaderMap, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| headers.get(*name))
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn validate_retained_identifier(value: String, field: &str) -> S3Result<String> {
    if value.len() > MAX_RETAINED_IDENTIFIER_BYTES {
        Err(s3s::s3_error!(InvalidArgument, "{field} exceeds 1024 bytes"))
    } else {
        Ok(value)
    }
}

fn new_version_id(headers: &HeaderMap) -> S3Result<String> {
    let Some(value) = header_value(headers, &SOURCE_VERSION_ID_HEADERS) else {
        return Ok(Uuid::new_v4().to_string());
    };
    let value = validate_retained_identifier(value.trim().to_owned(), "source version ID")?;
    let version_id = Uuid::parse_str(&value).map_err(|_| s3s::s3_error!(InvalidArgument, "source version ID must be a UUID"))?;
    Ok(version_id.to_string())
}

fn source_etag(headers: &HeaderMap) -> S3Result<Option<String>> {
    header_value(headers, &SOURCE_ETAG_HEADERS)
        .map(|value| validate_retained_identifier(value, "source ETag").map(|value| normalize_etag(&value)))
        .transpose()
}

fn validate_stored_metadata(content_type: &Option<String>, metadata: &Option<HashMap<String, String>>) -> S3Result {
    let content_type_bytes = content_type.as_ref().map_or(0, String::len);
    if content_type_bytes > MAX_CONTENT_TYPE_BYTES {
        return Err(s3s::s3_error!(InvalidArgument, "content type exceeds 1024 bytes"));
    }
    let metadata_bytes = metadata
        .iter()
        .flat_map(|metadata| metadata.iter())
        .try_fold(0usize, |total, (key, value)| {
            total.checked_add(key.len()).and_then(|total| total.checked_add(value.len()))
        })
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "object metadata size overflow"))?;
    if metadata_bytes > MAX_STORED_METADATA_BYTES {
        Err(s3s::s3_error!(InvalidArgument, "fake target retains at most 2 KiB of user metadata"))
    } else {
        Ok(())
    }
}

fn source_mtime(headers: &HeaderMap) -> Timestamp {
    if header_value(headers, &SOURCE_REPLICATION_REQUEST_HEADERS).is_none_or(|value| value != "true") {
        return Timestamp::from(SystemTime::now());
    }
    header_value(headers, &SOURCE_MTIME_HEADERS)
        .and_then(|value| time::OffsetDateTime::parse(value.trim(), &time::format_description::well_known::Rfc3339).ok())
        .map(Timestamp::from)
        .unwrap_or_else(|| Timestamp::from(SystemTime::now()))
}

fn normalize_etag(value: &str) -> String {
    value.trim().trim_matches('"').to_string()
}

fn request_fault<T>(request: &S3Request<T>) -> Option<RequestFault> {
    request.extensions.get::<RequestFault>().cloned()
}

fn scripted_status_error(status: StatusCode) -> s3s::S3Error {
    let mut error = match status {
        StatusCode::UNAUTHORIZED => s3s::s3_error!(UnauthorizedAccess, "scripted fake target fault"),
        StatusCode::FORBIDDEN => s3s::s3_error!(AccessDenied, "scripted fake target fault"),
        StatusCode::SERVICE_UNAVAILABLE => s3s::s3_error!(ServiceUnavailable, "scripted fake target fault"),
        _ => s3s::s3_error!(InternalError, "scripted fake target fault"),
    };
    error.set_status_code(status);
    error
}

fn scripted_disconnect_error() -> s3s::S3Error {
    let mut error = s3s::s3_error!(InternalError, "scripted fake target disconnect");
    let mut headers = HeaderMap::new();
    headers.insert(DISCONNECT_HEADER, HeaderValue::from_static("true"));
    error.set_headers(headers);
    error
}

fn update_consumed(control: &Mutex<ControlState>, sequence: u64, consumed: usize) {
    if let Some(record) = lock(control).requests.iter_mut().find(|record| record.sequence == sequence) {
        record.consumed_bytes = Some(consumed);
    }
}

async fn apply_non_body_fault(fault: Option<&RequestFault>, control: &Mutex<ControlState>) -> S3Result<()> {
    match fault.map(|fault| &fault.action) {
        Some(FaultAction::Status(status)) => Err(scripted_status_error(*status)),
        Some(FaultAction::Delay(duration)) => {
            sleep(*duration).await;
            Ok(())
        }
        Some(FaultAction::DisconnectAfterBytes(_)) => {
            update_consumed(control, fault.expect("matched fault").sequence, 0);
            Err(scripted_disconnect_error())
        }
        Some(FaultAction::SlowDrain { .. }) | Some(FaultAction::WrongEtag) | None => Ok(()),
    }
}

async fn collect_stream(
    body: Option<StreamingBlob>,
    content_length: Option<i64>,
    fault: Option<&RequestFault>,
    control: &Mutex<ControlState>,
) -> S3Result<Bytes> {
    let mut body = body.unwrap_or_else(|| StreamingBlob::new(Body::empty()));
    let capacity = content_length
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default();
    if capacity > MAX_BUFFERED_BODY_BYTES {
        return Err(s3s::s3_error!(EntityTooLarge, "fake target buffers at most 64 MiB"));
    }
    if let Some(RequestFault {
        action: FaultAction::Delay(duration),
        ..
    }) = fault
    {
        sleep(*duration).await;
    }
    timeout(MAX_FAULT_DURATION, async {
        match fault.map(|fault| &fault.action) {
            Some(FaultAction::Status(status)) => return Err(scripted_status_error(*status)),
            Some(FaultAction::Delay(_)) => {}
            Some(FaultAction::DisconnectAfterBytes(limit)) => {
                let mut consumed = 0usize;
                while consumed < *limit {
                    let Some(chunk) = body.next().await else { break };
                    let chunk = chunk.map_err(|error| s3s::s3_error!(InternalError, "request body failed: {error}"))?;
                    consumed = consumed.saturating_add(chunk.len().min(*limit - consumed));
                }
                update_consumed(control, fault.expect("matched fault").sequence, consumed);
                return Err(scripted_disconnect_error());
            }
            Some(FaultAction::SlowDrain { chunk_bytes, delay }) => {
                return collect_stream_slow(body, capacity, *chunk_bytes, *delay).await;
            }
            Some(FaultAction::WrongEtag) | None => {}
        }

        let mut output = BytesMut::with_capacity(capacity);
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|error| s3s::s3_error!(InternalError, "request body failed: {error}"))?;
            ensure_body_growth(output.len(), chunk.len())?;
            output.extend_from_slice(&chunk);
        }
        Ok(output.freeze())
    })
    .await
    .map_err(|_| s3s::s3_error!(RequestTimeout, "fake target body drain exceeded 30 seconds"))?
}

async fn collect_stream_slow(mut body: StreamingBlob, capacity: usize, chunk_bytes: usize, delay: Duration) -> S3Result<Bytes> {
    let mut output = BytesMut::with_capacity(capacity);
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|error| s3s::s3_error!(InternalError, "request body failed: {error}"))?;
        for slice in chunk.chunks(chunk_bytes) {
            ensure_body_growth(output.len(), slice.len())?;
            output.extend_from_slice(slice);
            sleep(delay).await;
        }
    }
    Ok(output.freeze())
}

async fn assemble_multipart(
    parts: Vec<(Bytes, [u8; 16])>,
    total_len: usize,
    permit: OwnedSemaphorePermit,
) -> S3Result<(Bytes, Vec<u8>, OwnedSemaphorePermit)> {
    let assemble = move || {
        let mut body = BytesMut::with_capacity(total_len);
        let mut digests = Vec::with_capacity(parts.len() * 16);
        for (part, digest) in parts {
            body.extend_from_slice(&part);
            digests.extend_from_slice(&digest);
        }
        (body.freeze(), digests, permit)
    };
    if total_len < 1024 * 1024 {
        return Ok(assemble());
    }
    tokio::task::spawn_blocking(assemble)
        .await
        .map_err(|error| s3s::s3_error!(InternalError, "multipart assembly worker failed: {error}"))
}

fn maybe_wrong_etag(fault: Option<&RequestFault>, e_tag: String) -> String {
    if fault.is_some_and(|fault| fault.action == FaultAction::WrongEtag) {
        normalize_etag(WRONG_ETAG)
    } else {
        e_tag
    }
}

fn apply_response_fault<T>(mut response: S3Response<T>, fault: Option<&RequestFault>) -> S3Response<T> {
    if fault.is_some_and(|fault| fault.action == FaultAction::WrongEtag) {
        response.headers.insert(ETAG, HeaderValue::from_static(WRONG_ETAG));
    }
    response
}

fn ensure_body_growth(current: usize, added: usize) -> S3Result {
    if current.checked_add(added).is_none_or(|total| total > MAX_BUFFERED_BODY_BYTES) {
        Err(s3s::s3_error!(EntityTooLarge, "fake target buffers at most 64 MiB"))
    } else {
        Ok(())
    }
}

async fn md5_digest(body: Bytes, permit: OwnedSemaphorePermit) -> S3Result<([u8; 16], OwnedSemaphorePermit)> {
    if body.len() < 1024 * 1024 {
        return Ok((md5::compute(body).0, permit));
    }
    tokio::task::spawn_blocking(move || (md5::compute(body).0, permit))
        .await
        .map_err(|error| s3s::s3_error!(InternalError, "MD5 worker failed: {error}"))
}

fn ensure_store_budget(state: &StoreState, removed_bytes: usize, added_bytes: usize, adds_version: bool) -> S3Result {
    let total_bytes = state
        .total_bytes
        .checked_sub(removed_bytes)
        .and_then(|total| total.checked_add(added_bytes))
        .ok_or_else(|| s3s::s3_error!(InternalError, "fake target storage accounting overflow"))?;
    if total_bytes > MAX_TOTAL_STORED_BYTES {
        return Err(s3s::s3_error!(ServiceUnavailable, "fake target storage budget exhausted"));
    }
    if adds_version && state.total_versions >= MAX_OBJECT_VERSIONS {
        return Err(s3s::s3_error!(ServiceUnavailable, "fake target version budget exhausted"));
    }
    Ok(())
}

fn ensure_part_budget(state: &StoreState, adds_part: bool) -> S3Result {
    if adds_part && state.total_parts >= MAX_MULTIPART_PARTS {
        Err(s3s::s3_error!(ServiceUnavailable, "fake target multipart part budget exhausted"))
    } else {
        Ok(())
    }
}

fn ensure_upload_budget(state: &StoreState) -> S3Result {
    if state.uploads.len() >= MAX_MULTIPART_UPLOADS {
        Err(s3s::s3_error!(ServiceUnavailable, "fake target multipart budget exhausted"))
    } else {
        Ok(())
    }
}

fn upsert_version(state: &mut StoreState, bucket: &str, key: String, version: ObjectVersion) -> S3Result {
    let versions = state
        .buckets
        .get(bucket)
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket, "bucket does not exist"))?
        .objects
        .get(&key);
    let existing = versions.and_then(|versions| {
        versions
            .iter()
            .position(|candidate| candidate.version_id == version.version_id)
            .map(|position| (position, versions[position].body.len()))
    });
    ensure_store_budget(state, existing.map_or(0, |(_, bytes)| bytes), version.body.len(), existing.is_none())?;
    state.total_bytes = state.total_bytes - existing.map_or(0, |(_, bytes)| bytes) + version.body.len();
    if existing.is_none() {
        state.total_versions += 1;
    }
    let versions = state
        .buckets
        .get_mut(bucket)
        .expect("bucket existence checked above")
        .objects
        .entry(key)
        .or_default();
    if let Some((position, _)) = existing {
        versions.remove(position);
    }
    let insert_at = versions
        .binary_search_by(|candidate| {
            version
                .last_modified
                .cmp(&candidate.last_modified)
                .then_with(|| candidate.delete_marker.cmp(&version.delete_marker))
                .then_with(|| version.version_id.cmp(&candidate.version_id))
        })
        .unwrap_or_else(|position| position);
    versions.insert(insert_at, version);
    Ok(())
}

fn marker_error(version: &ObjectVersion, explicit_version: bool) -> s3s::S3Error {
    let mut error = if explicit_version {
        s3s::s3_error!(MethodNotAllowed, "requested version is a delete marker")
    } else {
        s3s::s3_error!(NoSuchKey, "current version is a delete marker")
    };
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-delete-marker", HeaderValue::from_static("true"));
    if let Ok(value) = HeaderValue::from_str(&version.version_id) {
        headers.insert("x-amz-version-id", value);
    }
    if explicit_version {
        let mut formatted = Vec::new();
        if version
            .last_modified
            .format(TimestampFormat::HttpDate, &mut formatted)
            .is_ok()
            && let Ok(value) = HeaderValue::from_bytes(&formatted)
        {
            headers.insert(LAST_MODIFIED, value);
        }
    }
    error.set_headers(headers);
    error
}

fn find_version(state: &StoreState, bucket: &str, key: &str, version_id: Option<&str>) -> S3Result<ObjectVersion> {
    let bucket = state
        .buckets
        .get(bucket)
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket, "bucket does not exist"))?;
    let versions = bucket.objects.get(key).ok_or_else(|| {
        if version_id.is_some() {
            s3s::s3_error!(NoSuchVersion, "object version does not exist")
        } else {
            s3s::s3_error!(NoSuchKey, "object does not exist")
        }
    })?;
    let version = match version_id {
        Some(version_id) => versions.iter().find(|version| version.version_id == version_id),
        None => versions.first(),
    }
    .ok_or_else(|| {
        if version_id.is_some() {
            s3s::s3_error!(NoSuchVersion, "object version does not exist")
        } else {
            s3s::s3_error!(NoSuchKey, "object does not exist")
        }
    })?;
    if version.delete_marker {
        return Err(marker_error(version, version_id.is_some()));
    }
    Ok(version.clone())
}

#[async_trait]
impl S3 for FakeBackend {
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        if !lock(&self.store).buckets.contains_key(&req.input.bucket) {
            return Err(s3s::s3_error!(NoSuchBucket, "bucket does not exist"));
        }
        Ok(apply_response_fault(S3Response::new(HeadBucketOutput::default()), fault.as_ref()))
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        if !lock(&self.store).buckets.contains_key(&req.input.bucket) {
            return Err(s3s::s3_error!(NoSuchBucket, "bucket does not exist"));
        }
        Ok(apply_response_fault(
            S3Response::new(GetBucketVersioningOutput {
                status: Some("Enabled".to_string().into()),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let fault = request_fault(&req);
        let _body_permit = timeout(MAX_FAULT_DURATION, Arc::clone(&self.body_limit).acquire_owned())
            .await
            .map_err(|_| s3s::s3_error!(RequestTimeout, "fake target body limiter wait exceeded 30 seconds"))?
            .map_err(|_| s3s::s3_error!(ServiceUnavailable, "fake target body limiter closed"))?;
        let headers = req.headers;
        let input = req.input;
        let body = collect_stream(input.body, input.content_length, fault.as_ref(), &self.control).await?;
        validate_stored_metadata(&input.content_type, &input.metadata)?;
        let version_id = new_version_id(&headers)?;
        let e_tag = match source_etag(&headers)? {
            Some(value) => value,
            None => {
                let (digest, _body_permit) = md5_digest(body.clone(), _body_permit).await?;
                format!("{:x}", md5::Digest(digest))
            }
        };
        let version = ObjectVersion {
            version_id: version_id.clone(),
            body,
            e_tag: e_tag.clone(),
            last_modified: source_mtime(&headers),
            delete_marker: false,
            content_type: input.content_type,
            metadata: input.metadata,
        };
        upsert_version(&mut lock(&self.store), &input.bucket, input.key, version)?;
        Ok(apply_response_fault(
            S3Response::new(PutObjectOutput {
                e_tag: Some(ETag::Strong(maybe_wrong_etag(fault.as_ref(), e_tag))),
                version_id: Some(version_id),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let version = {
            let state = lock(&self.store);
            find_version(&state, &input.bucket, &input.key, input.version_id.as_deref())?
        };
        Ok(apply_response_fault(
            S3Response::new(GetObjectOutput {
                body: Some(StreamingBlob::new(Body::from(version.body.clone()))),
                content_length: Some(version.body.len() as i64),
                content_type: version.content_type,
                metadata: version.metadata,
                e_tag: Some(ETag::Strong(version.e_tag)),
                last_modified: Some(version.last_modified),
                version_id: Some(version.version_id),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let version = {
            let state = lock(&self.store);
            find_version(&state, &input.bucket, &input.key, input.version_id.as_deref())?
        };
        Ok(apply_response_fault(
            S3Response::new(HeadObjectOutput {
                content_length: Some(version.body.len() as i64),
                content_type: version.content_type,
                metadata: version.metadata,
                e_tag: Some(ETag::Strong(version.e_tag)),
                last_modified: Some(version.last_modified),
                version_id: Some(version.version_id),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let headers = req.headers;
        let input = req.input;
        let mut state = lock(&self.store);
        if !state.buckets.contains_key(&input.bucket) {
            return Err(s3s::s3_error!(NoSuchBucket, "bucket does not exist"));
        }
        if let Some(version_id) = input.version_id {
            let (removed_bytes, removed_versions, delete_marker, remove_key) = {
                let Some(versions) = state
                    .buckets
                    .get_mut(&input.bucket)
                    .expect("bucket existence checked above")
                    .objects
                    .get_mut(&input.key)
                else {
                    return Ok(apply_response_fault(
                        S3Response::new(DeleteObjectOutput {
                            version_id: Some(version_id),
                            ..Default::default()
                        }),
                        fault.as_ref(),
                    ));
                };
                let mut removed_bytes = 0usize;
                let mut removed_versions = 0usize;
                let mut delete_marker = None;
                versions.retain(|version| {
                    if version.version_id == version_id {
                        removed_bytes += version.body.len();
                        removed_versions += 1;
                        delete_marker.get_or_insert(version.delete_marker);
                        false
                    } else {
                        true
                    }
                });
                (removed_bytes, removed_versions, delete_marker, versions.is_empty())
            };
            if remove_key {
                state
                    .buckets
                    .get_mut(&input.bucket)
                    .expect("bucket existence checked above")
                    .objects
                    .remove(&input.key);
            }
            state.total_bytes -= removed_bytes;
            state.total_versions -= removed_versions;
            return Ok(apply_response_fault(
                S3Response::new(DeleteObjectOutput {
                    delete_marker,
                    version_id: Some(version_id),
                    ..Default::default()
                }),
                fault.as_ref(),
            ));
        }

        let version_id = new_version_id(&headers)?;
        upsert_version(
            &mut state,
            &input.bucket,
            input.key,
            ObjectVersion {
                version_id: version_id.clone(),
                body: Bytes::new(),
                e_tag: String::new(),
                last_modified: source_mtime(&headers),
                delete_marker: true,
                content_type: None,
                metadata: None,
            },
        )?;
        Ok(apply_response_fault(
            S3Response::new(DeleteObjectOutput {
                delete_marker: Some(true),
                version_id: Some(version_id),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let headers = req.headers;
        let input = req.input;
        let mut state = lock(&self.store);
        if !state.buckets.contains_key(&input.bucket) {
            return Err(s3s::s3_error!(NoSuchBucket, "bucket does not exist"));
        }
        ensure_upload_budget(&state)?;
        validate_stored_metadata(&input.content_type, &input.metadata)?;
        let upload_id = Uuid::new_v4().to_string();
        state.uploads.insert(
            upload_id.clone(),
            MultipartState {
                bucket: input.bucket.clone(),
                key: input.key.clone(),
                version_id: new_version_id(&headers)?,
                content_type: input.content_type,
                metadata: input.metadata,
                parts: BTreeMap::new(),
            },
        );
        Ok(apply_response_fault(
            S3Response::new(CreateMultipartUploadOutput {
                bucket: Some(input.bucket),
                key: Some(input.key),
                upload_id: Some(upload_id),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        let fault = request_fault(&req);
        let _body_permit = timeout(MAX_FAULT_DURATION, Arc::clone(&self.body_limit).acquire_owned())
            .await
            .map_err(|_| s3s::s3_error!(RequestTimeout, "fake target body limiter wait exceeded 30 seconds"))?
            .map_err(|_| s3s::s3_error!(ServiceUnavailable, "fake target body limiter closed"))?;
        let input = req.input;
        if !(1..=10_000).contains(&input.part_number) {
            return Err(s3s::s3_error!(InvalidArgument, "part number must be in 1..=10000"));
        }
        let body = collect_stream(input.body, input.content_length, fault.as_ref(), &self.control).await?;
        let (digest, _body_permit) = md5_digest(body.clone(), _body_permit).await?;
        let e_tag = format!("{:x}", md5::Digest(digest));
        let mut state = lock(&self.store);
        let existing_bytes = state
            .uploads
            .get(&input.upload_id)
            .filter(|upload| upload.bucket == input.bucket && upload.key == input.key)
            .ok_or_else(|| s3s::s3_error!(NoSuchUpload, "multipart upload does not exist"))?
            .parts
            .get(&input.part_number)
            .map_or(0, |part| part.body.len());
        let adds_part = state
            .uploads
            .get(&input.upload_id)
            .is_some_and(|upload| !upload.parts.contains_key(&input.part_number));
        ensure_part_budget(&state, adds_part)?;
        ensure_store_budget(&state, existing_bytes, body.len(), false)?;
        state.total_bytes = state.total_bytes - existing_bytes + body.len();
        if adds_part {
            state.total_parts += 1;
        }
        let upload = state
            .uploads
            .get_mut(&input.upload_id)
            .expect("upload existence checked above");
        upload.parts.insert(
            input.part_number,
            MultipartPart {
                body,
                e_tag: e_tag.clone(),
                digest,
            },
        );
        Ok(apply_response_fault(
            S3Response::new(UploadPartOutput {
                e_tag: Some(ETag::Strong(maybe_wrong_etag(fault.as_ref(), e_tag))),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    async fn complete_multipart_upload(
        &self,
        mut req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let _body_permits = match req.extensions.remove::<PrebodyPermit>().and_then(PrebodyPermit::take) {
            Some(permit) => permit,
            None => timeout(
                MAX_FAULT_DURATION,
                Arc::clone(&self.body_limit).acquire_many_owned(MAX_CONCURRENT_BODY_REQUESTS as u32),
            )
            .await
            .map_err(|_| s3s::s3_error!(RequestTimeout, "fake target body limiter wait exceeded 30 seconds"))?
            .map_err(|_| s3s::s3_error!(ServiceUnavailable, "fake target body limiter closed"))?,
        };
        let headers = req.headers;
        let input = req.input;
        let requested = input
            .multipart_upload
            .and_then(|upload| upload.parts)
            .ok_or_else(|| s3s::s3_error!(InvalidPart, "completion omitted parts"))?;
        if requested.is_empty() {
            return Err(s3s::s3_error!(InvalidPart, "completion omitted parts"));
        }
        let mut requested_parts = Vec::with_capacity(requested.len());
        let mut previous = 0;
        for part in &requested {
            let number = part
                .part_number
                .ok_or_else(|| s3s::s3_error!(InvalidPart, "part number is missing"))?;
            if number <= previous {
                return Err(s3s::s3_error!(InvalidPartOrder, "parts are not strictly ordered"));
            }
            previous = number;
            let requested_etag = part
                .e_tag
                .as_ref()
                .ok_or_else(|| s3s::s3_error!(InvalidPart, "part ETag is missing"))?;
            requested_parts.push((number, normalize_etag(requested_etag.value())));
        }

        let (upload, selected_parts) = {
            let state = lock(&self.store);
            let upload = state
                .uploads
                .get(&input.upload_id)
                .filter(|upload| upload.bucket == input.bucket && upload.key == input.key)
                .ok_or_else(|| s3s::s3_error!(NoSuchUpload, "multipart upload does not exist"))?;
            let mut selected = Vec::with_capacity(requested_parts.len());
            for (index, (number, requested_etag)) in requested_parts.iter().enumerate() {
                let stored = upload
                    .parts
                    .get(number)
                    .ok_or_else(|| s3s::s3_error!(InvalidPart, "part was not uploaded"))?;
                if requested_etag != &stored.e_tag {
                    return Err(s3s::s3_error!(InvalidPart, "part ETag does not match"));
                }
                if index + 1 != requested_parts.len() && stored.body.len() < MIN_MULTIPART_PART_BYTES {
                    return Err(s3s::s3_error!(EntityTooSmall, "non-final multipart part is smaller than 5 MiB"));
                }
                selected.push((*number, stored.clone()));
            }
            (
                MultipartState {
                    bucket: upload.bucket.clone(),
                    key: upload.key.clone(),
                    version_id: upload.version_id.clone(),
                    content_type: upload.content_type.clone(),
                    metadata: upload.metadata.clone(),
                    parts: BTreeMap::new(),
                },
                selected,
            )
        };
        let total_len = selected_parts.iter().try_fold(0usize, |total, (_, part)| {
            total
                .checked_add(part.body.len())
                .filter(|total| *total <= MAX_TOTAL_STORED_BYTES)
                .ok_or_else(|| s3s::s3_error!(EntityTooLarge, "multipart object exceeds 128 MiB"))
        })?;
        let assembly_parts = selected_parts
            .iter()
            .map(|(_, stored)| (stored.body.clone(), stored.digest))
            .collect();
        let (body, digests, _body_permits) = assemble_multipart(assembly_parts, total_len, _body_permits).await?;
        let part_count = requested.len();
        let e_tag = source_etag(&headers)?.unwrap_or_else(|| format!("{:x}-{part_count}", md5::compute(digests)));
        let version = ObjectVersion {
            version_id: upload.version_id.clone(),
            body,
            e_tag: e_tag.clone(),
            last_modified: Timestamp::from(SystemTime::now()),
            delete_marker: false,
            content_type: upload.content_type,
            metadata: upload.metadata,
        };
        let mut state = lock(&self.store);
        let current = state
            .uploads
            .get(&input.upload_id)
            .filter(|current| current.bucket == input.bucket && current.key == input.key)
            .ok_or_else(|| s3s::s3_error!(NoSuchUpload, "multipart upload does not exist"))?;
        for (part, (selected_number, selected_part)) in requested.iter().zip(&selected_parts) {
            let number = part.part_number.expect("validated above");
            debug_assert_eq!(number, *selected_number);
            let current_part = current
                .parts
                .get(&number)
                .ok_or_else(|| s3s::s3_error!(InvalidPart, "part changed during completion"))?;
            if current_part.e_tag != selected_part.e_tag {
                return Err(s3s::s3_error!(InvalidPart, "part changed during completion"));
            }
        }
        let upload_bytes = current.parts.values().map(|part| part.body.len()).sum::<usize>();
        let upload_part_count = current.parts.len();
        let existing = state
            .buckets
            .get(&input.bucket)
            .ok_or_else(|| s3s::s3_error!(NoSuchBucket, "bucket does not exist"))?
            .objects
            .get(&input.key)
            .and_then(|versions| versions.iter().find(|candidate| candidate.version_id == version.version_id))
            .map(|version| version.body.len());
        ensure_store_budget(
            &state,
            upload_bytes + existing.unwrap_or_default(),
            version.body.len(),
            existing.is_none(),
        )?;
        state.total_bytes -= upload_bytes;
        state.total_parts -= upload_part_count;
        state.uploads.remove(&input.upload_id);
        upsert_version(&mut state, &input.bucket, input.key.clone(), version)?;
        Ok(apply_response_fault(
            S3Response::new(CompleteMultipartUploadOutput {
                bucket: Some(input.bucket),
                key: Some(input.key),
                e_tag: Some(ETag::Strong(maybe_wrong_etag(fault.as_ref(), e_tag))),
                version_id: Some(upload.version_id),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let mut state = lock(&self.store);
        let matches = state
            .uploads
            .get(&input.upload_id)
            .is_some_and(|upload| upload.bucket == input.bucket && upload.key == input.key);
        if !matches {
            return Err(s3s::s3_error!(NoSuchUpload, "multipart upload does not exist"));
        }
        let removed = state
            .uploads
            .remove(&input.upload_id)
            .expect("upload existence checked above");
        state.total_bytes -= removed.parts.values().map(|part| part.body.len()).sum::<usize>();
        state.total_parts -= removed.parts.len();
        Ok(apply_response_fault(
            S3Response::new(AbortMultipartUploadOutput::default()),
            fault.as_ref(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::retry::RetryConfig;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
    use std::time::Instant;

    fn client(target: &FakeS3Target) -> Client {
        client_with_credentials(target, FAKE_ACCESS_KEY, FAKE_SECRET_KEY)
    }

    fn client_with_credentials(target: &FakeS3Target, access_key: &'static str, secret_key: &'static str) -> Client {
        let credentials = Credentials::new(access_key, secret_key, None, None, "fake-target");
        Client::from_conf(
            aws_sdk_s3::Config::builder()
                .credentials_provider(credentials)
                .region(Region::new("us-east-1"))
                .endpoint_url(target.endpoint())
                .force_path_style(true)
                .behavior_version_latest()
                .retry_config(RetryConfig::standard().with_max_attempts(1))
                .http_client(SmithyHttpClientBuilder::new().build_http())
                .build(),
        )
    }

    async fn get_bytes(client: &Client, bucket: &str, key: &str, version_id: Option<String>) -> Result<Bytes, BoxError> {
        let output = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id)
            .send()
            .await?;
        Ok(output.body.collect().await?.into_bytes())
    }

    async fn wait_for_active_connections(target: &FakeS3Target, expected: usize) {
        let mut active_connections = target.active_connections.clone();
        timeout(Duration::from_secs(2), active_connections.wait_for(|count| *count == expected))
            .await
            .expect("active connection count did not converge")
            .expect("active connection counter closed unexpectedly");
        assert_eq!(MAX_CONNECTIONS - target.connection_limit.available_permits(), expected);
    }

    async fn put_source_version(
        client: &Client,
        bucket: &str,
        key: &str,
        source_version: &str,
        source_mtime: Option<&str>,
        body: &'static [u8],
    ) -> Result<aws_sdk_s3::operation::put_object::PutObjectOutput, BoxError> {
        let source_version = source_version.to_string();
        let source_mtime = source_mtime.map(ToOwned::to_owned);
        Ok(client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .customize()
            .map_request(move |mut request| {
                request
                    .headers_mut()
                    .insert("x-rustfs-source-version-id", source_version.clone());
                if let Some(source_mtime) = &source_mtime {
                    request.headers_mut().insert("x-rustfs-source-mtime", source_mtime.clone());
                    request.headers_mut().insert("x-rustfs-source-replication-request", "true");
                }
                Ok::<_, std::convert::Infallible>(request)
            })
            .send()
            .await?)
    }

    macro_rules! assert_sdk_error {
        ($error:expr, $status:expr, $code:expr) => {{
            let error = &$error;
            assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some($status));
            assert_eq!(error.as_service_error().and_then(|error| error.meta().code()), Some($code));
        }};
    }

    #[tokio::test]
    async fn versions_delete_markers_and_request_journal() -> Result<(), BoxError> {
        let target = FakeS3Target::start().await?;
        target.create_bucket("target-bucket");
        let client = client(&target);

        client.head_bucket().bucket("target-bucket").send().await?;
        let versioning = client.get_bucket_versioning().bucket("target-bucket").send().await?;
        assert_eq!(versioning.status.map(|status| status.as_str().to_string()).as_deref(), Some("Enabled"));

        let first = client
            .put_object()
            .bucket("target-bucket")
            .key("nested/key")
            .body(ByteStream::from_static(b"first"))
            .send()
            .await?;
        let second = client
            .put_object()
            .bucket("target-bucket")
            .key("nested/key")
            .body(ByteStream::from_static(b"second"))
            .send()
            .await?;
        let second_head = client.head_object().bucket("target-bucket").key("nested/key").send().await?;
        assert_eq!(second_head.content_length(), Some(6));
        assert_eq!(second_head.version_id(), second.version_id.as_deref());
        assert_eq!(second_head.e_tag(), second.e_tag.as_deref());
        let first_head = client
            .head_object()
            .bucket("target-bucket")
            .key("nested/key")
            .version_id(first.version_id.as_deref().expect("PUT must return a version"))
            .send()
            .await?;
        assert_eq!(first_head.content_length(), Some(5));
        assert_eq!(first_head.version_id(), first.version_id.as_deref());
        assert_eq!(
            get_bytes(&client, "target-bucket", "nested/key", None).await?,
            Bytes::from_static(b"second")
        );
        assert_eq!(
            get_bytes(&client, "target-bucket", "nested/key", first.version_id.clone()).await?,
            Bytes::from_static(b"first")
        );

        client
            .delete_object()
            .bucket("target-bucket")
            .key("nested/key")
            .version_id(first.version_id.as_deref().expect("PUT must return a version"))
            .send()
            .await?;
        let deleted = client
            .get_object()
            .bucket("target-bucket")
            .key("nested/key")
            .version_id(first.version_id.as_deref().expect("PUT must return a version"))
            .send()
            .await
            .expect_err("deleted version must not remain readable");
        assert_sdk_error!(deleted, 404, "NoSuchVersion");
        let marker = client
            .delete_object()
            .bucket("target-bucket")
            .key("nested/key")
            .send()
            .await?;
        assert_eq!(marker.delete_marker, Some(true));
        let current_marker = client
            .get_object()
            .bucket("target-bucket")
            .key("nested/key")
            .send()
            .await
            .expect_err("current delete marker must hide the object");
        assert_sdk_error!(current_marker, 404, "NoSuchKey");
        let current_marker_head = client
            .head_object()
            .bucket("target-bucket")
            .key("nested/key")
            .send()
            .await
            .expect_err("current delete marker must hide HEAD");
        let current_marker_head_response = current_marker_head.raw_response().expect("HEAD error must retain response");
        assert_eq!(current_marker_head_response.status().as_u16(), 404);
        assert_eq!(current_marker_head_response.headers().get("x-amz-delete-marker"), Some("true"));
        let marker_version = marker.version_id.as_deref().expect("DELETE must return marker version");
        let explicit_marker = client
            .head_object()
            .bucket("target-bucket")
            .key("nested/key")
            .version_id(marker_version)
            .send()
            .await
            .expect_err("explicit delete marker HEAD must fail");
        let marker_response = explicit_marker.raw_response().expect("HEAD error must retain raw response");
        assert_eq!(marker_response.status().as_u16(), 405);
        assert_eq!(marker_response.headers().get("x-amz-delete-marker"), Some("true"));
        assert_eq!(marker_response.headers().get("x-amz-version-id"), Some(marker_version));
        assert!(marker_response.headers().get("last-modified").is_some());
        let explicit_marker_get = client
            .get_object()
            .bucket("target-bucket")
            .key("nested/key")
            .version_id(marker_version)
            .send()
            .await
            .expect_err("explicit delete marker GET must fail");
        let explicit_marker_get_response = explicit_marker_get.raw_response().expect("GET error must retain response");
        assert_eq!(explicit_marker_get_response.status().as_u16(), 405);
        assert_eq!(explicit_marker_get_response.headers().get("x-amz-delete-marker"), Some("true"));
        assert_eq!(explicit_marker_get_response.headers().get("x-amz-version-id"), Some(marker_version));
        let purged_marker = client
            .delete_object()
            .bucket("target-bucket")
            .key("nested/key")
            .version_id(marker_version)
            .send()
            .await?;
        assert_eq!(purged_marker.delete_marker, Some(true));
        assert_eq!(purged_marker.version_id(), Some(marker_version));
        assert_eq!(
            get_bytes(&client, "target-bucket", "nested/key", None).await?,
            Bytes::from_static(b"second")
        );

        let operations = target
            .requests()
            .into_iter()
            .map(|request| request.operation)
            .collect::<Vec<_>>();
        assert_eq!(
            operations,
            [
                Operation::HeadBucket,
                Operation::GetBucketVersioning,
                Operation::PutObject,
                Operation::PutObject,
                Operation::HeadObject,
                Operation::HeadObject,
                Operation::GetObject,
                Operation::GetObject,
                Operation::DeleteObject,
                Operation::GetObject,
                Operation::DeleteObject,
                Operation::GetObject,
                Operation::HeadObject,
                Operation::HeadObject,
                Operation::GetObject,
                Operation::DeleteObject,
                Operation::GetObject,
            ]
        );
        assert!(
            target
                .requests()
                .windows(2)
                .all(|pair| pair[0].sequence + 1 == pair[1].sequence)
        );
        let version_delete = target
            .requests()
            .into_iter()
            .find(|request| request.operation == Operation::DeleteObject && request.version_id.is_some())
            .expect("journal must retain version delete");
        assert_eq!(version_delete.key.as_deref(), Some("nested/key"));
        assert_eq!(version_delete.content_length, None);

        let source_version = Uuid::from_u128(0xabcdef01_2345_6789_abcd_ef0123456789).to_string();
        let equivalent_source_version = source_version.to_uppercase();
        for supplied_version in [&equivalent_source_version, &source_version] {
            let output = put_source_version(
                &client,
                "target-bucket",
                "idempotent",
                supplied_version,
                Some("2025-01-02T00:00:00Z"),
                b"same-body",
            )
            .await?;
            assert_eq!(output.version_id(), Some(source_version.as_str()));
        }
        client
            .delete_object()
            .bucket("target-bucket")
            .key("idempotent")
            .version_id(&source_version)
            .send()
            .await?;
        let purged = client
            .get_object()
            .bucket("target-bucket")
            .key("idempotent")
            .version_id(&source_version)
            .send()
            .await
            .expect_err("idempotent retry must not leave a duplicate version");
        assert_sdk_error!(purged, 404, "NoSuchVersion");
        assert!(
            !lock(&target.backend.store)
                .buckets
                .get("target-bucket")
                .expect("bucket must exist")
                .objects
                .contains_key("idempotent"),
            "deleting the final version must remove the retained object key"
        );

        let invalid_source_version = "not-a-uuid".to_string();
        let invalid_source = client
            .put_object()
            .bucket("target-bucket")
            .key("invalid-source-version")
            .body(ByteStream::from_static(b"body"))
            .customize()
            .map_request(move |mut request| {
                request
                    .headers_mut()
                    .insert("x-rustfs-source-version-id", invalid_source_version.clone());
                Ok::<_, std::convert::Infallible>(request)
            })
            .send()
            .await
            .expect_err("source version IDs must be UUIDs");
        assert_sdk_error!(invalid_source, 400, "InvalidArgument");

        let fallback_base_version = Uuid::from_u128(9).to_string();
        put_source_version(
            &client,
            "target-bucket",
            "invalid-source-mtime",
            &fallback_base_version,
            Some("1970-01-02T00:00:00Z"),
            b"base",
        )
        .await?;
        let invalid_source_mtime = "not-rfc3339".to_string();
        let invalid_before = time::OffsetDateTime::from(SystemTime::now());
        client
            .put_object()
            .bucket("target-bucket")
            .key("invalid-source-mtime")
            .body(ByteStream::from_static(b"body"))
            .customize()
            .map_request(move |mut request| {
                request
                    .headers_mut()
                    .insert("x-rustfs-source-mtime", invalid_source_mtime.clone());
                request.headers_mut().insert("x-rustfs-source-replication-request", "true");
                Ok::<_, std::convert::Infallible>(request)
            })
            .send()
            .await?;
        let invalid_after = time::OffsetDateTime::from(SystemTime::now());
        let invalid_last_modified = {
            lock(&target.backend.store)
                .buckets
                .get("target-bucket")
                .expect("bucket")
                .objects
                .get("invalid-source-mtime")
                .expect("object")
                .first()
                .expect("version")
                .last_modified
                .clone()
        };
        let invalid_last_modified: time::OffsetDateTime = invalid_last_modified.into();
        assert!((invalid_before..=invalid_after).contains(&invalid_last_modified));
        assert_eq!(
            get_bytes(&client, "target-bucket", "invalid-source-mtime", None).await?,
            Bytes::from_static(b"body"),
            "invalid replication mtime must fall back to receipt time"
        );

        let unmarked_base_version = Uuid::from_u128(10).to_string();
        put_source_version(
            &client,
            "target-bucket",
            "unmarked-source-mtime",
            &unmarked_base_version,
            Some("1970-01-02T00:00:00Z"),
            b"base",
        )
        .await?;
        let unmarked_mtime = "1970-01-01T00:00:00Z".to_string();
        let unmarked_before = time::OffsetDateTime::from(SystemTime::now());
        client
            .put_object()
            .bucket("target-bucket")
            .key("unmarked-source-mtime")
            .body(ByteStream::from_static(b"body"))
            .customize()
            .map_request(move |mut request| {
                request.headers_mut().insert("x-rustfs-source-mtime", unmarked_mtime.clone());
                Ok::<_, std::convert::Infallible>(request)
            })
            .send()
            .await?;
        let unmarked_after = time::OffsetDateTime::from(SystemTime::now());
        let unmarked_last_modified = {
            lock(&target.backend.store)
                .buckets
                .get("target-bucket")
                .expect("bucket")
                .objects
                .get("unmarked-source-mtime")
                .expect("object")
                .first()
                .expect("version")
                .last_modified
                .clone()
        };
        let unmarked_last_modified: time::OffsetDateTime = unmarked_last_modified.into();
        assert!((unmarked_before..=unmarked_after).contains(&unmarked_last_modified));
        assert_eq!(
            get_bytes(&client, "target-bucket", "unmarked-source-mtime", None).await?,
            Bytes::from_static(b"body"),
            "source mtime without a replication marker must be ignored"
        );

        let newer_version = Uuid::from_u128(3).to_string();
        let older_version = Uuid::from_u128(2).to_string();
        put_source_version(
            &client,
            "target-bucket",
            "out-of-order",
            &newer_version,
            Some("2025-01-03T00:00:00Z"),
            b"newer",
        )
        .await?;
        put_source_version(
            &client,
            "target-bucket",
            "out-of-order",
            &older_version,
            Some("2025-01-01T00:00:00Z"),
            b"older",
        )
        .await?;
        assert_eq!(
            get_bytes(&client, "target-bucket", "out-of-order", None).await?,
            Bytes::from_static(b"newer")
        );
        assert_eq!(
            get_bytes(&client, "target-bucket", "out-of-order", Some(older_version)).await?,
            Bytes::from_static(b"older")
        );
        let old_marker_version = Uuid::from_u128(4).to_string();
        let old_marker_mtime = "2024-12-31T00:00:00Z".to_string();
        client
            .delete_object()
            .bucket("target-bucket")
            .key("out-of-order")
            .customize()
            .map_request(move |mut request| {
                request
                    .headers_mut()
                    .insert("x-rustfs-source-version-id", old_marker_version.clone());
                request
                    .headers_mut()
                    .insert("x-rustfs-source-mtime", old_marker_mtime.clone());
                request.headers_mut().insert("x-rustfs-source-replication-request", "true");
                Ok::<_, std::convert::Infallible>(request)
            })
            .send()
            .await?;
        assert_eq!(
            get_bytes(&client, "target-bucket", "out-of-order", None).await?,
            Bytes::from_static(b"newer"),
            "a late old delete marker must not hide a newer replica"
        );

        let equal_mtime = "2025-01-04T00:00:00Z";
        let lower_version = Uuid::from_u128(5).to_string();
        let higher_version = Uuid::from_u128(6).to_string();
        put_source_version(
            &client,
            "target-bucket",
            "equal-mtime-objects",
            &higher_version,
            Some(equal_mtime),
            b"higher",
        )
        .await?;
        put_source_version(
            &client,
            "target-bucket",
            "equal-mtime-objects",
            &lower_version,
            Some(equal_mtime),
            b"lower",
        )
        .await?;
        assert_eq!(
            get_bytes(&client, "target-bucket", "equal-mtime-objects", None).await?,
            Bytes::from_static(b"higher")
        );

        let equal_object_version = Uuid::from_u128(7).to_string();
        put_source_version(
            &client,
            "target-bucket",
            "equal-mtime-marker",
            &equal_object_version,
            Some(equal_mtime),
            b"object",
        )
        .await?;
        let equal_marker_version = Uuid::from_u128(8).to_string();
        let equal_marker_mtime = equal_mtime.to_string();
        client
            .delete_object()
            .bucket("target-bucket")
            .key("equal-mtime-marker")
            .customize()
            .map_request(move |mut request| {
                request
                    .headers_mut()
                    .insert("x-rustfs-source-version-id", equal_marker_version.clone());
                request
                    .headers_mut()
                    .insert("x-rustfs-source-mtime", equal_marker_mtime.clone());
                request.headers_mut().insert("x-rustfs-source-replication-request", "true");
                Ok::<_, std::convert::Infallible>(request)
            })
            .send()
            .await?;
        assert_eq!(
            get_bytes(&client, "target-bucket", "equal-mtime-marker", None).await?,
            Bytes::from_static(b"object"),
            "an equal-mtime delete marker must sort after an object"
        );
        target.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn multipart_complete_and_abort() -> Result<(), BoxError> {
        let target = FakeS3Target::start().await?;
        target.create_bucket("target-bucket");
        let client = client(&target);
        let created = client
            .create_multipart_upload()
            .bucket("target-bucket")
            .key("multipart")
            .send()
            .await?;
        let upload_id = created.upload_id.expect("create must return upload id");
        let first_body = Bytes::from(vec![b'a'; MIN_MULTIPART_PART_BYTES]);
        let first = client
            .upload_part()
            .bucket("target-bucket")
            .key("multipart")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(first_body.clone()))
            .send()
            .await?;
        let second = client
            .upload_part()
            .bucket("target-bucket")
            .key("multipart")
            .upload_id(&upload_id)
            .part_number(2)
            .body(ByteStream::from_static(b"world"))
            .send()
            .await?;
        let parts = [first.e_tag, second.e_tag]
            .into_iter()
            .enumerate()
            .map(|(index, e_tag)| {
                CompletedPart::builder()
                    .part_number(index as i32 + 1)
                    .set_e_tag(e_tag)
                    .build()
            })
            .collect::<Vec<_>>();
        let invalid_parts = vec![
            CompletedPart::builder().part_number(1).e_tag("\"wrong\"").build(),
            parts[1].clone(),
        ];
        let invalid_complete = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("multipart")
            .upload_id(&upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(invalid_parts)).build())
            .send()
            .await
            .expect_err("wrong part ETag must fail without consuming the upload");
        assert_sdk_error!(invalid_complete, 400, "InvalidPart");
        client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("multipart")
            .upload_id(&upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(parts)).build())
            .send()
            .await?;
        let completed = get_bytes(&client, "target-bucket", "multipart", None).await?;
        assert_eq!(completed.len(), MIN_MULTIPART_PART_BYTES + 5);
        assert_eq!(&completed[..MIN_MULTIPART_PART_BYTES], first_body.as_ref());
        assert_eq!(&completed[MIN_MULTIPART_PART_BYTES..], b"world");

        let missing_etag_upload = client
            .create_multipart_upload()
            .bucket("target-bucket")
            .key("missing-etag")
            .send()
            .await?
            .upload_id
            .expect("create must return upload id");
        client
            .upload_part()
            .bucket("target-bucket")
            .key("missing-etag")
            .upload_id(&missing_etag_upload)
            .part_number(1)
            .body(ByteStream::from_static(b"part"))
            .send()
            .await?;
        let missing_etag = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("missing-etag")
            .upload_id(&missing_etag_upload)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().part_number(1).build())
                    .build(),
            )
            .send()
            .await
            .expect_err("completion must require every part ETag");
        assert_sdk_error!(missing_etag, 400, "InvalidPart");
        client
            .abort_multipart_upload()
            .bucket("target-bucket")
            .key("missing-etag")
            .upload_id(&missing_etag_upload)
            .send()
            .await?;

        let invalid_upload = client
            .create_multipart_upload()
            .bucket("target-bucket")
            .key("invalid-parts")
            .send()
            .await?
            .upload_id
            .expect("create must return upload id");
        let invalid_one = client
            .upload_part()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .part_number(1)
            .body(ByteStream::from_static(b"one"))
            .send()
            .await?;
        let invalid_two = client
            .upload_part()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .part_number(2)
            .body(ByteStream::from_static(b"two"))
            .send()
            .await?;
        let zero_part = client
            .upload_part()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .part_number(0)
            .body(ByteStream::from_static(b"part"))
            .send()
            .await
            .expect_err("part number zero must fail");
        assert_sdk_error!(zero_part, 400, "InvalidArgument");
        client
            .upload_part()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .part_number(10_000)
            .body(ByteStream::from_static(b"part"))
            .send()
            .await?;
        let out_of_range = client
            .upload_part()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .part_number(10_001)
            .body(ByteStream::from_static(b"part"))
            .send()
            .await
            .expect_err("part number above 10000 must fail");
        assert_sdk_error!(out_of_range, 400, "InvalidArgument");
        let empty = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .multipart_upload(CompletedMultipartUpload::builder().build())
            .send()
            .await
            .expect_err("empty completion must fail");
        assert_sdk_error!(empty, 400, "InvalidPart");
        let missing_part = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().part_number(3).e_tag("\"missing\"").build())
                    .build(),
            )
            .send()
            .await
            .expect_err("unuploaded part must fail");
        assert_sdk_error!(missing_part, 400, "InvalidPart");
        let missing_number = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().set_e_tag(invalid_one.e_tag.clone()).build())
                    .build(),
            )
            .send()
            .await
            .expect_err("missing part number must fail");
        assert_sdk_error!(missing_number, 400, "InvalidPart");
        let duplicate = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .set_e_tag(invalid_one.e_tag.clone())
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .set_e_tag(invalid_one.e_tag.clone())
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .expect_err("duplicate completion part must fail");
        assert_sdk_error!(duplicate, 400, "InvalidPartOrder");
        let out_of_order = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(2)
                            .set_e_tag(invalid_two.e_tag.clone())
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .set_e_tag(invalid_one.e_tag.clone())
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .expect_err("out-of-order completion must fail");
        assert_sdk_error!(out_of_order, 400, "InvalidPartOrder");
        let too_small = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().part_number(1).set_e_tag(invalid_one.e_tag).build())
                    .parts(CompletedPart::builder().part_number(2).set_e_tag(invalid_two.e_tag).build())
                    .build(),
            )
            .send()
            .await
            .expect_err("non-final small part must fail");
        assert_sdk_error!(too_small, 400, "EntityTooSmall");
        client
            .abort_multipart_upload()
            .bucket("target-bucket")
            .key("invalid-parts")
            .upload_id(&invalid_upload)
            .send()
            .await?;

        let sparse_upload = client
            .create_multipart_upload()
            .bucket("target-bucket")
            .key("sparse")
            .send()
            .await?
            .upload_id
            .expect("create must return upload id");
        let sparse_first_body = Bytes::from(vec![b'b'; MIN_MULTIPART_PART_BYTES]);
        let sparse_first = client
            .upload_part()
            .bucket("target-bucket")
            .key("sparse")
            .upload_id(&sparse_upload)
            .part_number(1)
            .body(ByteStream::from(sparse_first_body.clone()))
            .send()
            .await?;
        let sparse_third = client
            .upload_part()
            .bucket("target-bucket")
            .key("sparse")
            .upload_id(&sparse_upload)
            .part_number(3)
            .body(ByteStream::from_static(b"three"))
            .send()
            .await?;
        let sparse = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("sparse")
            .upload_id(&sparse_upload)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().part_number(1).set_e_tag(sparse_first.e_tag).build())
                    .parts(CompletedPart::builder().part_number(3).set_e_tag(sparse_third.e_tag).build())
                    .build(),
            )
            .send()
            .await?;
        assert!(
            sparse.e_tag().is_some_and(|e_tag| e_tag.trim_matches('"').ends_with("-2")),
            "multipart ETag must use the number of completed parts"
        );
        let sparse_body = get_bytes(&client, "target-bucket", "sparse", None).await?;
        assert_eq!(sparse_body.len(), MIN_MULTIPART_PART_BYTES + 5);
        assert_eq!(&sparse_body[..MIN_MULTIPART_PART_BYTES], sparse_first_body.as_ref());
        assert_eq!(&sparse_body[MIN_MULTIPART_PART_BYTES..], b"three");

        let wrong_etag_upload = client
            .create_multipart_upload()
            .bucket("target-bucket")
            .key("wrong-complete-etag")
            .send()
            .await?
            .upload_id
            .expect("create must return upload id");
        let wrong_etag_part = client
            .upload_part()
            .bucket("target-bucket")
            .key("wrong-complete-etag")
            .upload_id(&wrong_etag_upload)
            .part_number(1)
            .body(ByteStream::from_static(b"part"))
            .send()
            .await?;
        target.inject(
            Operation::CompleteMultipartUpload,
            FaultAction::Status(StatusCode::SERVICE_UNAVAILABLE),
            1,
        );
        let complete_status = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("wrong-complete-etag")
            .upload_id(&wrong_etag_upload)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .set_e_tag(wrong_etag_part.e_tag.clone())
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .expect_err("scripted complete status must fail before consuming the upload");
        assert_sdk_error!(complete_status, 503, "ServiceUnavailable");
        target.inject(Operation::CompleteMultipartUpload, FaultAction::WrongEtag, 1);
        let wrong_etag_complete = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("wrong-complete-etag")
            .upload_id(&wrong_etag_upload)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .set_e_tag(wrong_etag_part.e_tag)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await?;
        assert_eq!(wrong_etag_complete.e_tag(), Some(WRONG_ETAG));

        let base_version = Uuid::from_u128(11).to_string();
        put_source_version(
            &client,
            "target-bucket",
            "multipart-receipt-time",
            &base_version,
            Some("1970-01-02T00:00:00Z"),
            b"base",
        )
        .await?;
        let completed_version = Uuid::from_u128(12).to_string();
        let source_version = completed_version.clone();
        let receipt_upload = client
            .create_multipart_upload()
            .bucket("target-bucket")
            .key("multipart-receipt-time")
            .customize()
            .map_request(move |mut request| {
                request
                    .headers_mut()
                    .insert("x-rustfs-source-version-id", source_version.clone());
                Ok::<_, std::convert::Infallible>(request)
            })
            .send()
            .await?
            .upload_id
            .expect("create must return upload id");
        let receipt_part = client
            .upload_part()
            .bucket("target-bucket")
            .key("multipart-receipt-time")
            .upload_id(&receipt_upload)
            .part_number(1)
            .body(ByteStream::from_static(b"completed"))
            .send()
            .await?;
        let forged_old_mtime = "1970-01-01T00:00:00Z".to_string();
        let completed = client
            .complete_multipart_upload()
            .bucket("target-bucket")
            .key("multipart-receipt-time")
            .upload_id(&receipt_upload)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().part_number(1).set_e_tag(receipt_part.e_tag).build())
                    .build(),
            )
            .customize()
            .map_request(move |mut request| {
                request
                    .headers_mut()
                    .insert("x-rustfs-source-mtime", forged_old_mtime.clone());
                request.headers_mut().insert("x-rustfs-source-replication-request", "true");
                Ok::<_, std::convert::Infallible>(request)
            })
            .send()
            .await?;
        assert_eq!(completed.version_id(), Some(completed_version.as_str()));
        assert_eq!(
            get_bytes(&client, "target-bucket", "multipart-receipt-time", None).await?,
            Bytes::from_static(b"completed"),
            "multipart completion must ignore a forged old source mtime"
        );

        let aborted = client
            .create_multipart_upload()
            .bucket("target-bucket")
            .key("aborted")
            .send()
            .await?
            .upload_id
            .expect("create must return upload id");
        let wrong_owner_abort = client
            .abort_multipart_upload()
            .bucket("target-bucket")
            .key("other-key")
            .upload_id(&aborted)
            .send()
            .await
            .expect_err("wrong key must not abort another upload");
        assert_sdk_error!(wrong_owner_abort, 404, "NoSuchUpload");
        let wrong_bucket_abort = client
            .abort_multipart_upload()
            .bucket("other-bucket")
            .key("aborted")
            .upload_id(&aborted)
            .send()
            .await
            .expect_err("wrong bucket must not abort another upload");
        assert_sdk_error!(wrong_bucket_abort, 404, "NoSuchUpload");
        client
            .abort_multipart_upload()
            .bucket("target-bucket")
            .key("aborted")
            .upload_id(&aborted)
            .send()
            .await?;
        let after_abort = client
            .upload_part()
            .bucket("target-bucket")
            .key("aborted")
            .upload_id(&aborted)
            .part_number(1)
            .body(ByteStream::from_static(b"part"))
            .send()
            .await
            .expect_err("aborted upload must no longer accept parts");
        assert_sdk_error!(after_abort, 404, "NoSuchUpload");
        assert!(
            target
                .requests()
                .iter()
                .any(|request| request.operation == Operation::AbortMultipartUpload)
        );
        let upload_record = target
            .requests()
            .into_iter()
            .find(|request| request.operation == Operation::UploadPart)
            .expect("journal must retain upload part");
        assert_eq!(upload_record.upload_id.as_deref(), Some(upload_id.as_str()));
        assert_eq!(upload_record.part_number, Some(1));
        assert_eq!(upload_record.content_length, Some(MIN_MULTIPART_PART_BYTES as u64));
        target.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn scripted_faults_are_consumed_per_operation() -> Result<(), BoxError> {
        let target = FakeS3Target::start().await?;
        target.create_bucket("target-bucket");
        let client = client(&target);

        target.inject(Operation::PutObject, FaultAction::Status(StatusCode::SERVICE_UNAVAILABLE), 3);
        let unsigned = crate::common::local_http_client()
            .put(format!("{}/target-bucket/unsigned", target.endpoint()))
            .body("unsigned")
            .send()
            .await?;
        assert!(unsigned.status().is_client_error());
        assert!(target.requests().is_empty(), "unauthenticated request consumed the fault script");
        let wrong_access = client_with_credentials(&target, "other-access", "other-secret")
            .put_object()
            .bucket("target-bucket")
            .key("wrong-access")
            .body(ByteStream::from_static(b"body"))
            .send()
            .await
            .expect_err("other access key must fail authentication");
        assert!(wrong_access.raw_response().is_some());
        let wrong_secret = client_with_credentials(&target, FAKE_ACCESS_KEY, "wrong-secret")
            .put_object()
            .bucket("target-bucket")
            .key("wrong-secret")
            .body(ByteStream::from_static(b"body"))
            .send()
            .await
            .expect_err("wrong signature must fail authentication");
        assert!(wrong_secret.raw_response().is_some());
        assert!(target.requests().is_empty(), "invalid signatures consumed the fault script");
        for key in ["fail-1", "fail-2", "fail-3"] {
            client.head_bucket().bucket("target-bucket").send().await?;
            let error = client
                .put_object()
                .bucket("target-bucket")
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .expect_err("scripted 503 must fail");
            assert_sdk_error!(error, 503, "ServiceUnavailable");
        }
        client
            .put_object()
            .bucket("target-bucket")
            .key("recovered")
            .body(ByteStream::from_static(b"body"))
            .send()
            .await?;
        assert_eq!(get_bytes(&client, "target-bucket", "recovered", None).await?, Bytes::from_static(b"body"));

        target.inject(Operation::PutObject, FaultAction::Delay(Duration::from_millis(40)), 1);
        target.inject(Operation::PutObject, FaultAction::Status(StatusCode::SERVICE_UNAVAILABLE), 1);
        let started = Instant::now();
        client
            .put_object()
            .bucket("target-bucket")
            .key("delayed")
            .body(ByteStream::from_static(b"body"))
            .send()
            .await?;
        assert!(started.elapsed() >= Duration::from_millis(35));
        let fifo_status = client
            .put_object()
            .bucket("target-bucket")
            .key("fifo-status")
            .body(ByteStream::from_static(b"body"))
            .send()
            .await
            .expect_err("second queued fault must run after the delay");
        assert_sdk_error!(fifo_status, 503, "ServiceUnavailable");
        target.inject(Operation::PutObject, FaultAction::Status(StatusCode::SERVICE_UNAVAILABLE), 1);
        target.clear_faults();
        client
            .put_object()
            .bucket("target-bucket")
            .key("cleared")
            .body(ByteStream::from_static(b"body"))
            .send()
            .await?;

        target.inject(
            Operation::PutObject,
            FaultAction::SlowDrain {
                chunk_bytes: 2,
                delay: Duration::from_millis(10),
            },
            1,
        );
        let started = Instant::now();
        client
            .put_object()
            .bucket("target-bucket")
            .key("slow")
            .body(ByteStream::from_static(b"123456"))
            .send()
            .await?;
        assert!(started.elapsed() >= Duration::from_millis(25));
        assert_eq!(get_bytes(&client, "target-bucket", "slow", None).await?, Bytes::from_static(b"123456"));

        target.inject(Operation::PutObject, FaultAction::WrongEtag, 1);
        let wrong = client
            .put_object()
            .bucket("target-bucket")
            .key("wrong-etag")
            .body(ByteStream::from_static(b"body"))
            .send()
            .await?;
        assert_eq!(wrong.e_tag.as_deref(), Some(WRONG_ETAG));

        target.inject(Operation::GetObject, FaultAction::Status(StatusCode::UNAUTHORIZED), 1);
        let unauthorized = client
            .get_object()
            .bucket("target-bucket")
            .key("credential-fault")
            .send()
            .await
            .expect_err("scripted 401 must fail");
        assert_sdk_error!(unauthorized, 401, "UnauthorizedAccess");
        target.inject(Operation::GetObject, FaultAction::Status(StatusCode::FORBIDDEN), 1);
        let forbidden = client
            .get_object()
            .bucket("target-bucket")
            .key("credential-fault")
            .send()
            .await
            .expect_err("scripted 403 must fail");
        assert_sdk_error!(forbidden, 403, "AccessDenied");

        target.inject(Operation::PutObject, FaultAction::DisconnectAfterBytes(1), 1);
        client
            .put_object()
            .bucket("target-bucket")
            .key("disconnected")
            .body(ByteStream::from_static(b"body"))
            .send()
            .await
            .expect_err("scripted disconnect must fail");
        let disconnected = client
            .head_object()
            .bucket("target-bucket")
            .key("disconnected")
            .send()
            .await
            .expect_err("disconnected request must not store an object");
        assert_sdk_error!(disconnected, 404, "NotFound");

        assert_eq!(
            parse_request(&Method::PUT, &"/target-bucket/key?uploadId=upload&partNumber=bad".parse::<Uri>()?).operation,
            Operation::Unknown
        );

        let requests = target.take_requests();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.fault == Some(FaultAction::Status(StatusCode::SERVICE_UNAVAILABLE)))
                .count(),
            4
        );
        assert!(
            requests
                .iter()
                .any(|request| request.fault == Some(FaultAction::DisconnectAfterBytes(1)) && request.consumed_bytes == Some(1))
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.operation == Operation::PutObject && request.fault.is_none())
                .filter(|request| request.key.as_deref() == Some("recovered"))
                .count(),
            1
        );
        assert!(target.requests().is_empty(), "take_requests must drain the journal");
        target.shutdown().await;

        let connection_duration = Duration::from_millis(200);
        let (connection_gate, gate_rx) = watch::channel(false);
        let bounded_target = FakeS3Target::start_with_connection_gate(connection_duration, Some(gate_rx)).await?;
        bounded_target.create_bucket("target-bucket");
        let bounded_client = client_with_credentials(&bounded_target, FAKE_ACCESS_KEY, FAKE_SECRET_KEY);
        let mut stalled_connections = Vec::with_capacity(MAX_CONNECTIONS);
        for _ in 0..MAX_CONNECTIONS {
            stalled_connections.push(tokio::net::TcpStream::connect(bounded_target.address()).await?);
        }
        wait_for_active_connections(&bounded_target, MAX_CONNECTIONS).await;
        bounded_client
            .head_bucket()
            .bucket("target-bucket")
            .send()
            .await
            .expect_err("idle connections must exhaust the bounded connection pool");
        let released_at = Instant::now();
        connection_gate.send(true)?;
        wait_for_active_connections(&bounded_target, 0).await;
        assert!(released_at.elapsed() >= Duration::from_millis(150));
        bounded_client.head_bucket().bucket("target-bucket").send().await?;
        drop(stalled_connections);
        bounded_target.shutdown().await;
        Ok(())
    }

    #[test]
    #[should_panic(expected = "slow-drain chunk size must be non-zero")]
    fn slow_drain_rejects_zero_chunk_size() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            assert!(ensure_body_growth(MAX_BUFFERED_BODY_BYTES, 0).is_ok());
            let too_large = ensure_body_growth(MAX_BUFFERED_BODY_BYTES, 1).expect_err("64 MiB + 1 must be rejected");
            assert_eq!(too_large.code().as_str(), "EntityTooLarge");
            assert!(validate_retained_identifier("v".repeat(MAX_RETAINED_IDENTIFIER_BYTES), "version").is_ok());
            let identifier_overflow = validate_retained_identifier("v".repeat(MAX_RETAINED_IDENTIFIER_BYTES + 1), "version")
                .expect_err("retained identifiers must be bounded");
            assert_eq!(identifier_overflow.code().as_str(), "InvalidArgument");
            let exact_metadata = HashMap::from([("k".to_string(), "v".repeat(MAX_STORED_METADATA_BYTES - 1))]);
            assert!(validate_stored_metadata(&None, &Some(exact_metadata)).is_ok());
            let oversized_metadata = HashMap::from([("k".to_string(), "v".repeat(MAX_STORED_METADATA_BYTES))]);
            let metadata_overflow =
                validate_stored_metadata(&None, &Some(oversized_metadata)).expect_err("stored metadata must be bounded");
            assert_eq!(metadata_overflow.code().as_str(), "InvalidArgument");
            assert!(validate_stored_metadata(&Some("c".repeat(MAX_CONTENT_TYPE_BYTES)), &None).is_ok());
            let content_type_overflow = validate_stored_metadata(&Some("c".repeat(MAX_CONTENT_TYPE_BYTES + 1)), &None)
                .expect_err("content type must be bounded");
            assert_eq!(content_type_overflow.code().as_str(), "InvalidArgument");
            let split_metadata = HashMap::from([
                ("a".to_string(), "x".repeat(1000)),
                ("b".to_string(), "y".repeat(MAX_STORED_METADATA_BYTES - 1002)),
            ]);
            assert!(validate_stored_metadata(&None, &Some(split_metadata)).is_ok());
            let split_metadata_overflow = HashMap::from([
                ("a".to_string(), "x".repeat(1000)),
                ("b".to_string(), "y".repeat(MAX_STORED_METADATA_BYTES - 1001)),
            ]);
            let split_overflow = validate_stored_metadata(&None, &Some(split_metadata_overflow))
                .expect_err("metadata field totals must be bounded");
            assert_eq!(split_overflow.code().as_str(), "InvalidArgument");
            let body_control = Mutex::new(ControlState::default());
            let max_body = Bytes::from(vec![0; MAX_BUFFERED_BODY_BYTES]);
            let collected = collect_stream(
                Some(StreamingBlob::new(Body::from(max_body))),
                Some(MAX_BUFFERED_BODY_BYTES as i64),
                None,
                &body_control,
            )
            .await
            .expect("exact body cap must be accepted");
            assert_eq!(collected.len(), MAX_BUFFERED_BODY_BYTES);
            drop(collected);
            let over_body = Bytes::from(vec![0; MAX_BUFFERED_BODY_BYTES + 1]);
            let normal_overflow =
                collect_stream(Some(StreamingBlob::new(Body::from(over_body.clone()))), None, None, &body_control)
                    .await
                    .expect_err("normal drain must enforce the body cap");
            assert_eq!(normal_overflow.code().as_str(), "EntityTooLarge");
            let slow_overflow = collect_stream_slow(StreamingBlob::new(Body::from(over_body)), 0, 1024 * 1024, Duration::ZERO)
                .await
                .expect_err("slow drain must enforce the body cap");
            assert_eq!(slow_overflow.code().as_str(), "EntityTooLarge");
            let full = StoreState {
                total_bytes: MAX_TOTAL_STORED_BYTES,
                ..Default::default()
            };
            let exhausted = ensure_store_budget(&full, 0, 1, false).expect_err("global byte budget must be enforced");
            assert_eq!(exhausted.code().as_str(), "ServiceUnavailable");
            assert!(ensure_store_budget(&StoreState::default(), 0, MAX_TOTAL_STORED_BYTES, false).is_ok());
            let almost_full_versions = StoreState {
                total_versions: MAX_OBJECT_VERSIONS - 1,
                ..Default::default()
            };
            assert!(ensure_store_budget(&almost_full_versions, 0, 0, true).is_ok());
            let full_versions = StoreState {
                total_versions: MAX_OBJECT_VERSIONS,
                ..Default::default()
            };
            let versions_exhausted =
                ensure_store_budget(&full_versions, 0, 0, true).expect_err("version cap must reject one more version");
            assert_eq!(versions_exhausted.code().as_str(), "ServiceUnavailable");
            let almost_full_parts = StoreState {
                total_parts: MAX_MULTIPART_PARTS - 1,
                ..Default::default()
            };
            assert!(ensure_part_budget(&almost_full_parts, true).is_ok());
            let full_parts = StoreState {
                total_parts: MAX_MULTIPART_PARTS,
                ..Default::default()
            };
            let parts_exhausted = ensure_part_budget(&full_parts, true).expect_err("global part budget must be enforced");
            assert_eq!(parts_exhausted.code().as_str(), "ServiceUnavailable");
            assert!(
                ensure_part_budget(&full_parts, false).is_ok(),
                "replacing a part must remain allowed at cap"
            );
            let mut full_uploads = StoreState::default();
            for index in 0..MAX_MULTIPART_UPLOADS {
                if index + 1 == MAX_MULTIPART_UPLOADS {
                    assert!(ensure_upload_budget(&full_uploads).is_ok());
                }
                full_uploads.uploads.insert(
                    index.to_string(),
                    MultipartState {
                        bucket: "bucket".to_string(),
                        key: "key".to_string(),
                        version_id: index.to_string(),
                        content_type: None,
                        metadata: None,
                        parts: BTreeMap::new(),
                    },
                );
            }
            let uploads_exhausted = ensure_upload_budget(&full_uploads).expect_err("upload cap must reject one more upload");
            assert_eq!(uploads_exhausted.code().as_str(), "ServiceUnavailable");
            let control = Mutex::new(ControlState::default());
            for index in 0..=MAX_REQUEST_RECORDS {
                record_request(
                    &control,
                    Operation::PutObject,
                    Method::PUT,
                    ParsedRequest {
                        operation: Operation::PutObject,
                        bucket: "bucket".to_string(),
                        key: Some(format!("key-{index}")),
                        version_id: None,
                        upload_id: None,
                        part_number: None,
                    },
                    Some(0),
                );
            }
            let records = lock(&control).requests.clone();
            assert_eq!(records.len(), MAX_REQUEST_RECORDS);
            assert_eq!(records.front().map(|record| record.sequence), Some(2));
            assert_eq!(records.back().map(|record| record.sequence), Some((MAX_REQUEST_RECORDS + 1) as u64));
            let bounded_control = Mutex::new(ControlState::default());
            let utf8_boundary = format!("{}é", "a".repeat(MAX_RETAINED_IDENTIFIER_BYTES - 1));
            record_request(
                &bounded_control,
                Operation::GetObject,
                Method::GET,
                ParsedRequest {
                    operation: Operation::GetObject,
                    bucket: "b".repeat(MAX_RETAINED_IDENTIFIER_BYTES + 1),
                    key: Some(utf8_boundary),
                    version_id: Some("v".repeat(MAX_RETAINED_IDENTIFIER_BYTES + 1)),
                    upload_id: Some("u".repeat(MAX_RETAINED_IDENTIFIER_BYTES + 1)),
                    part_number: None,
                },
                None,
            );
            {
                let bounded_records = lock(&bounded_control);
                let bounded = &bounded_records.requests[0];
                assert_eq!(bounded.bucket.len(), MAX_RETAINED_IDENTIFIER_BYTES);
                assert_eq!(bounded.key.as_ref().map(String::len), Some(MAX_RETAINED_IDENTIFIER_BYTES - 1));
                assert_eq!(bounded.version_id.as_ref().map(String::len), Some(MAX_RETAINED_IDENTIFIER_BYTES));
                assert_eq!(bounded.upload_id.as_ref().map(String::len), Some(MAX_RETAINED_IDENTIFIER_BYTES));
            }
            let target = FakeS3Target::start().await.expect("target");
            target.create_bucket("b".repeat(63));
            let empty_bucket = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                target.create_bucket("");
            }));
            assert!(empty_bucket.is_err());
            let short_bucket = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                target.create_bucket("ab");
            }));
            assert!(short_bucket.is_err());
            let malformed_bucket = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                target.create_bucket("Invalid_Bucket");
            }));
            assert!(malformed_bucket.is_err());
            for reserved in [
                "xn--bucket",
                "sthree-bucket",
                "amzn-s3-demo-bucket",
                "bucket-s3alias",
                "bucket--ol-s3",
                "bucket.mrap",
                "bucket--x-s3",
                "bucket--table-s3",
                "bucket-an",
            ] {
                let reserved_bucket = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    target.create_bucket(reserved);
                }));
                assert!(reserved_bucket.is_err(), "reserved bucket name was accepted: {reserved}");
            }
            let long_bucket = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                target.create_bucket("b".repeat(64));
            }));
            assert!(long_bucket.is_err());
            for index in 0..MAX_BUCKETS - 1 {
                target.create_bucket(format!("bucket-{index}"));
            }
            target.create_bucket("bucket-0");
            let bucket_overflow = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                target.create_bucket("one-bucket-too-many");
            }));
            assert!(bucket_overflow.is_err());
            target.inject(
                Operation::PutObject,
                FaultAction::Status(StatusCode::SERVICE_UNAVAILABLE),
                MAX_SCRIPTED_FAULTS,
            );
            target.clear_faults();
            target.inject(Operation::PutObject, FaultAction::Delay(MAX_FAULT_DURATION), 1);
            target.inject(
                Operation::PutObject,
                FaultAction::SlowDrain {
                    chunk_bytes: 1,
                    delay: MAX_FAULT_DURATION - Duration::from_millis(1),
                },
                1,
            );
            target.clear_faults();
            let fault_overflow = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                target.inject(
                    Operation::PutObject,
                    FaultAction::Status(StatusCode::SERVICE_UNAVAILABLE),
                    MAX_SCRIPTED_FAULTS + 1,
                );
            }));
            assert!(fault_overflow.is_err());
            let duration_overflow = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                target.inject(Operation::PutObject, FaultAction::Delay(MAX_FAULT_DURATION + Duration::from_millis(1)), 1);
            }));
            assert!(duration_overflow.is_err());
            let slow_duration_overflow = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                target.inject(
                    Operation::PutObject,
                    FaultAction::SlowDrain {
                        chunk_bytes: 1,
                        delay: MAX_FAULT_DURATION,
                    },
                    1,
                );
            }));
            assert!(slow_duration_overflow.is_err());
            target.inject(
                Operation::PutObject,
                FaultAction::SlowDrain {
                    chunk_bytes: 0,
                    delay: Duration::ZERO,
                },
                1,
            );
        });
    }
}
