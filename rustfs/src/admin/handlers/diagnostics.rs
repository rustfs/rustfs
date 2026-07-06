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

//! MinIO-compatible admin diagnostics endpoints.
//!
//! Implements the lock inspection / force-unlock, health-info, log, and
//! speedtest families of the admin `/v3` API surface. Each handler wires to a
//! real RustFS subsystem where one exists (namespace lock manager, `sysinfo`
//! host telemetry, `StorageInfo` per-drive throughput) and returns
//! MinIO-compatible request/response semantics — with an explicit,
//! honestly-labeled capability note — where RustFS does not yet carry the
//! backing infrastructure (in-process log ring buffer, cross-node object
//! speedtest harness).

use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::storage_api::access::spawn_traced;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_lock::{LockMode, ObjectKey, get_global_lock_manager};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::stream::{ByteStream, DynByteStream};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, StdError, s3_error};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

const CONTENT_TYPE_JSON: &str = "application/json";
const CONTENT_TYPE_NDJSON: &str = "application/x-ndjson";

/// Cap on how many locks a single `top/locks` response enumerates, matching the
/// MinIO default page size and bounding response size on busy clusters.
const TOP_LOCKS_DEFAULT_LIMIT: usize = 1000;
const TOP_LOCKS_MAX_LIMIT: usize = 10000;

// ---------------------------------------------------------------------------
// Route registration
// ---------------------------------------------------------------------------

pub fn register_diagnostics_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/top/locks").as_str(),
        AdminOperation(&TopLocksHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/force-unlock").as_str(),
        AdminOperation(&ForceUnlockHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/healthinfo").as_str(),
        AdminOperation(&HealthInfoHandler {}),
    )?;
    // MinIO exposes the same collector under the legacy `obdinfo` alias.
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/obdinfo").as_str(),
        AdminOperation(&HealthInfoHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/log").as_str(),
        AdminOperation(&ConsoleLogHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/speedtest").as_str(),
        AdminOperation(&SpeedtestHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/speedtest/object").as_str(),
        AdminOperation(&SpeedtestHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/speedtest/drive").as_str(),
        AdminOperation(&SpeedtestHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/speedtest/net").as_str(),
        AdminOperation(&SpeedtestHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/speedtest/site").as_str(),
        AdminOperation(&SpeedtestHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/speedtest/client/devnull").as_str(),
        AdminOperation(&SpeedtestClientDevnullHandler {}),
    )?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Shared auth helper
// ---------------------------------------------------------------------------

async fn authorize(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(AccessDenied, "Signature is required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));

    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(action)], remote_addr).await
}

fn json_response<T: Serialize>(status: StatusCode, value: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(value)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to serialize response: {e}")))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE_JSON));
    Ok(S3Response::with_headers((status, Body::from(data)), headers))
}

async fn read_body(input: Body) -> S3Result<Vec<u8>> {
    let mut input = input;
    let body = input
        .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
        .await
        .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;
    Ok(body.to_vec())
}

// ---------------------------------------------------------------------------
// #615 GET /v3/top/locks — real namespace-lock enumeration
// ---------------------------------------------------------------------------

/// A single held namespace lock, shaped for the admin "top locks" view.
#[derive(Debug, Clone, Serialize)]
pub struct LockEntry {
    /// `bucket/object` resource the lock is held on.
    pub resource: String,
    pub bucket: String,
    pub object: String,
    /// `None` means the latest version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// `"WRITE"` for exclusive, `"READ"` for shared.
    #[serde(rename = "type")]
    pub lock_type: &'static str,
    pub owner: String,
    pub priority: &'static str,
    /// Wall-clock RFC3339 timestamp when the lock was acquired, if representable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    /// How long the lock has been held, in seconds.
    pub elapsed_secs: u64,
    /// Seconds until the lock's timeout expires (0 if already past).
    pub ttl_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopLocksResponse {
    pub total: usize,
    pub truncated: bool,
    pub locks: Vec<LockEntry>,
    /// Present only when the lock subsystem is disabled by configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capability_note: Option<String>,
}

fn lock_priority_label(priority: rustfs_lock::fast_lock::LockPriority) -> &'static str {
    use rustfs_lock::fast_lock::LockPriority;
    match priority {
        LockPriority::Low => "LOW",
        LockPriority::Normal => "NORMAL",
        LockPriority::High => "HIGH",
        LockPriority::Critical => "CRITICAL",
    }
}

fn system_time_to_rfc3339(t: SystemTime) -> Option<String> {
    let dt: time::OffsetDateTime = t.into();
    dt.format(&time::format_description::well_known::Rfc3339).ok()
}

fn collect_top_locks(limit: usize) -> TopLocksResponse {
    let manager = get_global_lock_manager();
    let Some(fast) = manager.as_fast_lock_manager() else {
        return TopLocksResponse {
            total: 0,
            truncated: false,
            locks: Vec::new(),
            capability_note: Some(
                "namespace lock subsystem is disabled (RUSTFS_LOCK_ENABLED=false); no locks are tracked".to_string(),
            ),
        };
    };

    let now = SystemTime::now();
    let mut infos = fast.list_locks();
    // Longest-held first, matching MinIO's `top locks` ordering intent.
    infos.sort_by_key(|i| i.acquired_at);
    let total = infos.len();
    let truncated = total > limit;

    let locks = infos
        .into_iter()
        .take(limit)
        .map(|info| {
            let elapsed_secs = now.duration_since(info.acquired_at).unwrap_or(Duration::ZERO).as_secs();
            let ttl_secs = info.expires_at.duration_since(now).unwrap_or(Duration::ZERO).as_secs();
            LockEntry {
                resource: format!("{}/{}", info.key.bucket, info.key.object),
                bucket: info.key.bucket.to_string(),
                object: info.key.object.to_string(),
                version: info.key.version.as_ref().map(|v| v.to_string()),
                lock_type: match info.mode {
                    LockMode::Exclusive => "WRITE",
                    LockMode::Shared => "READ",
                },
                owner: info.owner.to_string(),
                priority: lock_priority_label(info.priority),
                since: system_time_to_rfc3339(info.acquired_at),
                elapsed_secs,
                ttl_secs,
            }
        })
        .collect();

    TopLocksResponse {
        total,
        truncated,
        locks,
        capability_note: None,
    }
}

fn parse_top_locks_limit(uri: &Uri) -> usize {
    query_value(uri, "count")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(TOP_LOCKS_DEFAULT_LIMIT)
        .min(TOP_LOCKS_MAX_LIMIT)
}

pub struct TopLocksHandler {}

#[async_trait::async_trait]
impl Operation for TopLocksHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize(&req, AdminAction::TopLocksAdminAction).await?;
        let limit = parse_top_locks_limit(&req.uri);
        let response = collect_top_locks(limit);
        json_response(StatusCode::OK, &response)
    }
}

// ---------------------------------------------------------------------------
// #615 POST /v3/force-unlock — real force-release
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ForceUnlockRequest {
    /// Resources to force-unlock. Each entry is a `bucket/object` path or a
    /// bucket-only name. Accepts either an explicit list or the MinIO-style
    /// single `resource` field.
    #[serde(default)]
    resources: Vec<String>,
    #[serde(default)]
    resource: Option<String>,
}

#[derive(Debug, Serialize)]
struct ForceUnlockResult {
    resource: String,
    released_owners: usize,
}

#[derive(Debug, Serialize)]
struct ForceUnlockResponse {
    results: Vec<ForceUnlockResult>,
    total_released: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    capability_note: Option<String>,
}

fn parse_resource(raw: &str) -> Option<ObjectKey> {
    let trimmed = raw.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    match trimmed.split_once('/') {
        Some((bucket, object)) if !bucket.is_empty() && !object.is_empty() => Some(ObjectKey::new(bucket, object)),
        // Bucket-only resource: MinIO treats the bucket name itself as the object key.
        _ => Some(ObjectKey::new(trimmed, trimmed)),
    }
}

pub struct ForceUnlockHandler {}

#[async_trait::async_trait]
impl Operation for ForceUnlockHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize(&req, AdminAction::ForceUnlockAdminAction).await?;

        // Resources may arrive as a JSON body or as repeated `resource` query params.
        let mut resources: Vec<String> = query_values(&req.uri, "resource");
        let body = read_body(req.input).await?;
        if !body.is_empty() {
            let parsed: ForceUnlockRequest = serde_json::from_slice(&body)
                .map_err(|e| s3_error!(InvalidRequest, "invalid force-unlock request body: {}", e))?;
            resources.extend(parsed.resources);
            resources.extend(parsed.resource);
        }

        if resources.is_empty() {
            return Err(s3_error!(InvalidRequest, "at least one resource is required"));
        }

        let manager = get_global_lock_manager();
        let Some(fast) = manager.as_fast_lock_manager() else {
            let response = ForceUnlockResponse {
                results: Vec::new(),
                total_released: 0,
                capability_note: Some(
                    "namespace lock subsystem is disabled (RUSTFS_LOCK_ENABLED=false); nothing to unlock".to_string(),
                ),
            };
            return json_response(StatusCode::OK, &response);
        };

        let mut results = Vec::with_capacity(resources.len());
        let mut total_released = 0usize;
        for raw in resources {
            let Some(key) = parse_resource(&raw) else {
                return Err(s3_error!(InvalidRequest, "invalid resource: {}", raw));
            };
            let released = fast.force_unlock(&key);
            total_released += released;
            results.push(ForceUnlockResult {
                resource: raw,
                released_owners: released,
            });
        }

        let response = ForceUnlockResponse {
            results,
            total_released,
            capability_note: None,
        };
        json_response(StatusCode::OK, &response)
    }
}

// ---------------------------------------------------------------------------
// #607 GET /v3/healthinfo and /v3/obdinfo — real host + storage telemetry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct HealthCpuInfo {
    logical_cores: usize,
    brand: String,
    frequency_mhz: u64,
    usage_percent: f64,
}

#[derive(Debug, Clone, Serialize)]
struct HealthMemInfo {
    total_bytes: u64,
    used_bytes: u64,
    available_bytes: u64,
    total_swap_bytes: u64,
    used_swap_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
struct HealthOsInfo {
    os: String,
    kernel_version: Option<String>,
    os_version: Option<String>,
    hostname: Option<String>,
    arch: String,
    uptime_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
struct HealthProcInfo {
    pid: u32,
    cpu_usage_percent: f32,
    memory_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
struct HealthDriveInfo {
    endpoint: String,
    drive_path: String,
    state: String,
    total_space: u64,
    used_space: u64,
    available_space: u64,
    read_throughput: f64,
    write_throughput: f64,
    read_latency: f64,
    write_latency: f64,
}

#[derive(Debug, Clone, Serialize)]
struct HealthInfoResponse {
    version: String,
    deployment_id: Option<String>,
    region: Option<String>,
    timestamp: Option<String>,
    cpu: HealthCpuInfo,
    memory: HealthMemInfo,
    os: HealthOsInfo,
    process: HealthProcInfo,
    drives: Vec<HealthDriveInfo>,
    /// Reserved MinIO health families (perf/net/config obd probes) that RustFS
    /// does not yet collect are enumerated here so clients can tell an
    /// unsupported probe apart from an empty result.
    unsupported_probes: Vec<&'static str>,
}

async fn collect_health_info() -> HealthInfoResponse {
    use sysinfo::{Pid, System};

    let mut sys = System::new_all();
    sys.refresh_cpu_all();
    // A second sample after a short interval yields meaningful CPU usage.
    tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;
    sys.refresh_cpu_all();
    sys.refresh_memory();

    let logical_cores = sys.cpus().len();
    let cpu = HealthCpuInfo {
        logical_cores,
        brand: sys.cpus().first().map(|c| c.brand().to_string()).unwrap_or_default(),
        frequency_mhz: sys.cpus().first().map(|c| c.frequency()).unwrap_or(0),
        usage_percent: if logical_cores > 0 {
            sys.cpus().iter().map(|c| c.cpu_usage() as f64).sum::<f64>() / logical_cores as f64
        } else {
            0.0
        },
    };

    let memory = HealthMemInfo {
        total_bytes: sys.total_memory(),
        used_bytes: sys.used_memory(),
        available_bytes: sys.available_memory(),
        total_swap_bytes: sys.total_swap(),
        used_swap_bytes: sys.used_swap(),
    };

    let os = HealthOsInfo {
        os: std::env::consts::OS.to_string(),
        kernel_version: System::kernel_version(),
        os_version: System::long_os_version(),
        hostname: System::host_name(),
        arch: std::env::consts::ARCH.to_string(),
        uptime_secs: System::uptime(),
    };

    let pid = std::process::id();
    let process = sys
        .process(Pid::from_u32(pid))
        .map(|p| HealthProcInfo {
            pid,
            cpu_usage_percent: p.cpu_usage(),
            memory_bytes: p.memory(),
        })
        .unwrap_or(HealthProcInfo {
            pid,
            cpu_usage_percent: 0.0,
            memory_bytes: 0,
        });

    let drives = collect_drive_info().await;

    HealthInfoResponse {
        version: crate::version::get_version(),
        deployment_id: crate::admin::runtime_sources::current_deployment_id(),
        region: crate::admin::runtime_sources::current_region().map(|r| r.as_str().to_string()),
        timestamp: system_time_to_rfc3339(SystemTime::now()),
        cpu,
        memory,
        os,
        process,
        drives,
        unsupported_probes: vec!["perf-net", "perf-drive-obd", "config-obd", "sys-services"],
    }
}

async fn collect_drive_info() -> Vec<HealthDriveInfo> {
    use crate::admin::runtime_sources::{DefaultAdminUsecase, default_admin_usecase};
    let usecase: DefaultAdminUsecase = default_admin_usecase();
    match usecase.execute_query_storage_info().await {
        Ok(info) => info
            .disks
            .into_iter()
            .map(|d| HealthDriveInfo {
                endpoint: d.endpoint,
                drive_path: d.drive_path,
                state: d.state,
                total_space: d.total_space,
                used_space: d.used_space,
                available_space: d.available_space,
                read_throughput: d.read_throughput,
                write_throughput: d.write_throughput,
                read_latency: d.read_latency,
                write_latency: d.write_latency,
            })
            .collect(),
        Err(err) => {
            warn!(error = %err, "healthinfo: storage info unavailable");
            Vec::new()
        }
    }
}

pub struct HealthInfoHandler {}

#[async_trait::async_trait]
impl Operation for HealthInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize(&req, AdminAction::HealthInfoAdminAction).await?;
        let response = collect_health_info().await;
        json_response(StatusCode::OK, &response)
    }
}

// ---------------------------------------------------------------------------
// #607 GET /v3/log — MinIO-compatible streaming log endpoint
// ---------------------------------------------------------------------------

struct ByteChannelStream {
    inner: ReceiverStream<Result<Bytes, StdError>>,
}

impl Stream for ByteChannelStream {
    type Item = Result<Bytes, StdError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::into_inner(self).inner.poll_next_unpin(cx)
    }
}

impl ByteStream for ByteChannelStream {}

/// A single console-log record, shaped after MinIO's `LogInfo`.
#[derive(Debug, Clone, Serialize)]
struct LogInfo {
    node_name: String,
    #[serde(rename = "consoleMsg")]
    console_msg: String,
    level: String,
    time: Option<String>,
    /// Distinguishes the honest keep-alive stream from real records.
    err: Option<String>,
}

pub struct ConsoleLogHandler {}

#[async_trait::async_trait]
impl Operation for ConsoleLogHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize(&req, AdminAction::ConsoleLogAdminAction).await?;

        // RustFS routes logs to the tracing pipeline (stdout / OTLP sinks) and
        // does NOT maintain an in-process ring buffer that could be replayed to
        // an admin client. Rather than fake historical log lines, this honors
        // the MinIO streaming contract (chunked NDJSON of `LogInfo`) and emits a
        // single capability record, then a keep-alive heartbeat, so `mc admin
        // logs` connects and stays open without being misled into believing it
        // received real buffered logs.
        let node_name = sysinfo::System::host_name().unwrap_or_else(|| "rustfs".to_string());
        let (tx, rx) = mpsc::channel::<Result<Bytes, StdError>>(8);

        spawn_traced(async move {
            let notice = LogInfo {
                node_name: node_name.clone(),
                console_msg: "RustFS does not expose an in-process console-log buffer; live log streaming is not yet \
                              available. Configure a tracing/OTLP sink to collect logs."
                    .to_string(),
                level: "INFO".to_string(),
                time: system_time_to_rfc3339(SystemTime::now()),
                err: Some("log_streaming_unsupported".to_string()),
            };
            if let Ok(mut encoded) = serde_json::to_vec(&notice) {
                encoded.push(b'\n');
                if tx.send(Ok(Bytes::from(encoded))).await.is_err() {
                    return;
                }
            }

            let mut ticker = tokio::time::interval(Duration::from_secs(15));
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = tx.closed() => break,
                    _ = ticker.tick() => {
                        // Whitespace keep-alive keeps the NDJSON stream open.
                        if tx.send(Ok(Bytes::from_static(b" \n"))).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        let stream: DynByteStream = Box::pin(ByteChannelStream {
            inner: ReceiverStream::new(rx),
        });
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE_NDJSON));
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(stream)), headers))
    }
}

// ---------------------------------------------------------------------------
// #615 POST /v3/speedtest family
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpeedtestKind {
    Object,
    Drive,
    Net,
    Site,
}

fn speedtest_kind_from_path(path: &str) -> SpeedtestKind {
    if path.ends_with("/speedtest/drive") {
        SpeedtestKind::Drive
    } else if path.ends_with("/speedtest/net") {
        SpeedtestKind::Net
    } else if path.ends_with("/speedtest/site") {
        SpeedtestKind::Site
    } else {
        // `/speedtest` and `/speedtest/object` both mean the object throughput test.
        SpeedtestKind::Object
    }
}

#[derive(Debug, Clone, Serialize)]
struct DriveSpeedtestEntry {
    endpoint: String,
    drive_path: String,
    state: String,
    read_throughput_bytes_per_sec: f64,
    write_throughput_bytes_per_sec: f64,
    read_latency_secs: f64,
    write_latency_secs: f64,
}

#[derive(Debug, Clone, Serialize)]
struct SpeedtestResponse {
    kind: &'static str,
    /// `true` when the reported numbers come from a real measurement/observation
    /// on this node; `false` when the endpoint returns MinIO-compatible
    /// structure only (see `capability_note`).
    measured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    capability_note: Option<String>,
    /// Aggregate throughput in bytes/sec across the sampled drives (drive test).
    #[serde(skip_serializing_if = "Option::is_none")]
    aggregate_read_throughput_bytes_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aggregate_write_throughput_bytes_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    drives: Vec<DriveSpeedtestEntry>,
    /// Bytes drained by the net/devnull probe and how long it took.
    #[serde(skip_serializing_if = "Option::is_none")]
    rx_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_secs: Option<f64>,
}

async fn run_drive_speedtest() -> S3Result<SpeedtestResponse> {
    use crate::admin::runtime_sources::default_admin_usecase;
    let usecase = default_admin_usecase();
    let info = usecase.execute_query_storage_info().await.map_err(S3Error::from)?;

    let mut drives = Vec::with_capacity(info.disks.len());
    let mut agg_read = 0.0f64;
    let mut agg_write = 0.0f64;
    for d in info.disks {
        agg_read += d.read_throughput;
        agg_write += d.write_throughput;
        drives.push(DriveSpeedtestEntry {
            endpoint: d.endpoint,
            drive_path: d.drive_path,
            state: d.state,
            read_throughput_bytes_per_sec: d.read_throughput,
            write_throughput_bytes_per_sec: d.write_throughput,
            read_latency_secs: d.read_latency,
            write_latency_secs: d.write_latency,
        });
    }

    Ok(SpeedtestResponse {
        kind: "drive",
        measured: true,
        capability_note: Some(
            "drive throughput/latency is reported from live per-drive StorageInfo observations rather than a \
             synthetic write/read benchmark"
                .to_string(),
        ),
        aggregate_read_throughput_bytes_per_sec: Some(agg_read),
        aggregate_write_throughput_bytes_per_sec: Some(agg_write),
        drives,
        rx_bytes: None,
        duration_secs: None,
    })
}

fn object_speedtest_unsupported() -> SpeedtestResponse {
    SpeedtestResponse {
        kind: "object",
        measured: false,
        capability_note: Some(
            "object PUT/GET speedtest requires a cross-node benchmark harness and a scratch bucket lifecycle that \
             RustFS does not yet expose from the admin layer; use the drive speedtest (/v3/speedtest/drive) for live \
             per-drive throughput"
                .to_string(),
        ),
        aggregate_read_throughput_bytes_per_sec: None,
        aggregate_write_throughput_bytes_per_sec: None,
        drives: Vec::new(),
        rx_bytes: None,
        duration_secs: None,
    }
}

fn net_speedtest_single_node() -> SpeedtestResponse {
    SpeedtestResponse {
        kind: "net",
        measured: false,
        capability_note: Some(
            "network speedtest measures inter-node bandwidth; a distributed peer-perf harness is not yet wired. See \
             /v3/site-replication/netperf for the site-to-site variant"
                .to_string(),
        ),
        aggregate_read_throughput_bytes_per_sec: None,
        aggregate_write_throughput_bytes_per_sec: None,
        drives: Vec::new(),
        rx_bytes: None,
        duration_secs: None,
    }
}

pub struct SpeedtestHandler {}

#[async_trait::async_trait]
impl Operation for SpeedtestHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // MinIO gates speedtest behind the health-info (OBD) admin action.
        authorize(&req, AdminAction::HealthInfoAdminAction).await?;
        let kind = speedtest_kind_from_path(req.uri.path());
        // Drain any request body so the connection is not left half-read.
        let _ = read_body(req.input).await?;

        let response = match kind {
            SpeedtestKind::Drive => run_drive_speedtest().await?,
            SpeedtestKind::Object => object_speedtest_unsupported(),
            SpeedtestKind::Net => net_speedtest_single_node(),
            SpeedtestKind::Site => SpeedtestResponse {
                kind: "site",
                measured: false,
                capability_note: Some(
                    "site speedtest aggregates peer results across a replicated deployment; not yet wired".to_string(),
                ),
                aggregate_read_throughput_bytes_per_sec: None,
                aggregate_write_throughput_bytes_per_sec: None,
                drives: Vec::new(),
                rx_bytes: None,
                duration_secs: None,
            },
        };
        json_response(StatusCode::OK, &response)
    }
}

/// `POST /v3/speedtest/client/devnull` — real client-to-server upload drain.
///
/// The client streams data; the server discards it and reports how much it
/// received and how long it took, giving a genuine one-way upload throughput
/// number (mirrors MinIO's `ClientDevNull`).
pub struct SpeedtestClientDevnullHandler {}

#[async_trait::async_trait]
impl Operation for SpeedtestClientDevnullHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize(&req, AdminAction::HealthInfoAdminAction).await?;

        let started = std::time::Instant::now();
        let mut input = req.input;
        let mut total: u64 = 0;
        while let Some(chunk) = input.next().await {
            let chunk = chunk.map_err(|e| s3_error!(InvalidRequest, "failed to read devnull stream: {}", e))?;
            total += chunk.len() as u64;
        }
        let elapsed = started.elapsed();

        let response = SpeedtestResponse {
            kind: "client-devnull",
            measured: true,
            capability_note: None,
            aggregate_read_throughput_bytes_per_sec: None,
            aggregate_write_throughput_bytes_per_sec: Some(if elapsed.as_secs_f64() > 0.0 {
                total as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            }),
            drives: Vec::new(),
            rx_bytes: Some(total),
            duration_secs: Some(elapsed.as_secs_f64()),
        };
        json_response(StatusCode::OK, &response)
    }
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

fn query_value(uri: &Uri, key: &str) -> Option<String> {
    uri.query().and_then(|q| {
        url::form_urlencoded::parse(q.as_bytes()).find_map(|(k, v)| if k == key { Some(v.into_owned()) } else { None })
    })
}

fn query_values(uri: &Uri, key: &str) -> Vec<String> {
    uri.query()
        .map(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .filter_map(|(k, v)| if k == key { Some(v.into_owned()) } else { None })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Extensions, Uri};

    fn build_request(method: Method, uri: &'static str) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method,
            uri: Uri::from_static(uri),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn top_locks_handler_rejects_missing_credentials() {
        let err = TopLocksHandler {}
            .call(build_request(Method::GET, "/rustfs/admin/v3/top/locks"), Params::new())
            .await
            .expect_err("must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn force_unlock_handler_rejects_missing_credentials() {
        let err = ForceUnlockHandler {}
            .call(build_request(Method::POST, "/rustfs/admin/v3/force-unlock"), Params::new())
            .await
            .expect_err("must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn health_info_handler_rejects_missing_credentials() {
        let err = HealthInfoHandler {}
            .call(build_request(Method::GET, "/rustfs/admin/v3/healthinfo"), Params::new())
            .await
            .expect_err("must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn speedtest_handler_rejects_missing_credentials() {
        let err = SpeedtestHandler {}
            .call(build_request(Method::POST, "/rustfs/admin/v3/speedtest/drive"), Params::new())
            .await
            .expect_err("must reject anonymous requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn speedtest_kind_routing() {
        assert_eq!(speedtest_kind_from_path("/rustfs/admin/v3/speedtest"), SpeedtestKind::Object);
        assert_eq!(speedtest_kind_from_path("/rustfs/admin/v3/speedtest/object"), SpeedtestKind::Object);
        assert_eq!(speedtest_kind_from_path("/rustfs/admin/v3/speedtest/drive"), SpeedtestKind::Drive);
        assert_eq!(speedtest_kind_from_path("/rustfs/admin/v3/speedtest/net"), SpeedtestKind::Net);
        assert_eq!(speedtest_kind_from_path("/rustfs/admin/v3/speedtest/site"), SpeedtestKind::Site);
    }

    #[test]
    fn parse_resource_variants() {
        let key = parse_resource("bucket/object").expect("bucket/object");
        assert_eq!(key.bucket.as_ref(), "bucket");
        assert_eq!(key.object.as_ref(), "object");

        let key = parse_resource("/leading/slash/object").expect("nested");
        assert_eq!(key.bucket.as_ref(), "leading");
        assert_eq!(key.object.as_ref(), "slash/object");

        let key = parse_resource("bucketonly").expect("bucket only");
        assert_eq!(key.bucket.as_ref(), "bucketonly");
        assert_eq!(key.object.as_ref(), "bucketonly");

        assert!(parse_resource("   ").is_none());
        assert!(parse_resource("/").is_none());
    }

    #[test]
    fn top_locks_limit_parsing() {
        assert_eq!(parse_top_locks_limit(&Uri::from_static("/x")), TOP_LOCKS_DEFAULT_LIMIT);
        assert_eq!(parse_top_locks_limit(&Uri::from_static("/x?count=5")), 5);
        assert_eq!(parse_top_locks_limit(&Uri::from_static("/x?count=0")), TOP_LOCKS_DEFAULT_LIMIT);
        assert_eq!(parse_top_locks_limit(&Uri::from_static("/x?count=999999")), TOP_LOCKS_MAX_LIMIT);
    }

    #[tokio::test]
    async fn collect_top_locks_reports_live_lock() {
        // Acquire a real lock through the global manager and confirm it surfaces.
        let manager = get_global_lock_manager();
        let key = ObjectKey::new("diag-bucket", "diag-object");
        // The fast-lock manager exposes the acquire API; if the lock subsystem is
        // disabled in this environment, the response must carry a capability note.
        let Some(fast) = manager.as_fast_lock_manager() else {
            let response = collect_top_locks(TOP_LOCKS_DEFAULT_LIMIT);
            assert!(response.capability_note.is_some() || response.locks.is_empty());
            return;
        };
        let guard = match fast.acquire_write_lock(key.clone(), "diag-owner").await {
            Ok(g) => g,
            Err(_) => {
                let response = collect_top_locks(TOP_LOCKS_DEFAULT_LIMIT);
                assert!(response.capability_note.is_some() || response.locks.is_empty());
                return;
            }
        };

        let response = collect_top_locks(TOP_LOCKS_DEFAULT_LIMIT);
        let found = response
            .locks
            .iter()
            .any(|l| l.bucket == "diag-bucket" && l.object == "diag-object");
        assert!(found, "expected the held lock to appear in top locks");
        let entry = response
            .locks
            .iter()
            .find(|l| l.bucket == "diag-bucket")
            .expect("entry present");
        assert_eq!(entry.lock_type, "WRITE");
        assert_eq!(entry.owner, "diag-owner");

        drop(guard);
    }
}
