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

use crate::admin::auth::authorize_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::storage_api::access::spawn_traced;
use crate::server::ADMIN_PREFIX;
use crate::storage::storage_api::get_global_lock_clients;
use bytes::Bytes;
use futures::{Stream, StreamExt, future::join_all};
use http::{HeaderMap, HeaderValue, Uri, header::CONTENT_LENGTH};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_lock::{LockLeaseInfo, LockMode, LockType, ObjectKey, get_global_lock_manager};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::stream::{ByteStream, DynByteStream};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, StdError, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::sync::{Semaphore, SemaphorePermit, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

const CONTENT_TYPE_JSON: &str = "application/json";
const CONTENT_TYPE_NDJSON: &str = "application/x-ndjson";
pub(crate) const CLIENT_DEVNULL_MAX_BYTES: u64 = 1024 * 1024 * 1024;
pub(crate) const CLIENT_DEVNULL_MAX_DURATION: Duration = Duration::from_secs(30);
pub(crate) const CLIENT_DEVNULL_MAX_CONCURRENCY: usize = 4;
static CLIENT_DEVNULL_ADMISSION: Semaphore = Semaphore::const_new(CLIENT_DEVNULL_MAX_CONCURRENCY);

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

/// The pre-check keeps these endpoints' historical `AccessDenied` missing-credentials
/// response; the shared gate reports `InvalidRequest` "get cred failed".
async fn authorize(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    if req.credentials.is_none() {
        return Err(s3_error!(AccessDenied, "Signature is required"));
    }

    authorize_admin_request(req, vec![Action::AdminAction(action)]).await?;
    Ok(())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TopLockMode {
    Read,
    Write,
}

impl TopLockMode {
    fn label(self) -> &'static str {
        match self {
            Self::Read => "READ",
            Self::Write => "WRITE",
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct LockHolderKey {
    resource: ObjectKey,
    mode: TopLockMode,
    owner: String,
}

#[derive(Debug)]
struct TopLockState {
    acquired_at: SystemTime,
    ttl_secs: u64,
    priority: &'static str,
}

#[derive(Debug)]
struct LeaseHolderState {
    acquired_at: SystemTime,
    ttl_secs: u64,
    holder_count: u32,
    guard_ids: Option<Vec<u64>>,
}

fn build_top_locks_response(
    limit: usize,
    now: SystemTime,
    lease_infos: Vec<LockLeaseInfo>,
    fast_infos: Vec<(rustfs_lock::ObjectLockInfo, u32, Option<Vec<u64>>)>,
) -> TopLocksResponse {
    let mut lease_holders = HashMap::with_capacity(lease_infos.len());

    for info in lease_infos {
        let mode = match info.lock_type {
            LockType::Shared => TopLockMode::Read,
            LockType::Exclusive => TopLockMode::Write,
        };
        let key = LockHolderKey {
            resource: info.resource,
            mode,
            owner: info.owner,
        };
        let ttl_secs = info.remaining_ttl.as_secs();
        lease_holders
            .entry(key)
            .and_modify(|state: &mut LeaseHolderState| {
                if info.acquired_at < state.acquired_at {
                    state.acquired_at = info.acquired_at;
                }
                state.ttl_secs = state.ttl_secs.max(ttl_secs);
                state.holder_count = state.holder_count.saturating_add(1);
                match (state.guard_ids.as_mut(), info.guard_id) {
                    (Some(guard_ids), Some(guard_id)) => guard_ids.push(guard_id),
                    _ => state.guard_ids = None,
                }
            })
            .or_insert(LeaseHolderState {
                acquired_at: info.acquired_at,
                ttl_secs,
                holder_count: 1,
                guard_ids: info.guard_id.map(|guard_id| vec![guard_id]),
            });
    }
    for state in lease_holders.values_mut() {
        if let Some(guard_ids) = &mut state.guard_ids {
            guard_ids.sort_unstable();
        }
    }

    let mut infos: Vec<_> = fast_infos
        .into_iter()
        .map(|(info, holder_count, guard_ids)| {
            let mode = match info.mode {
                LockMode::Shared => TopLockMode::Read,
                LockMode::Exclusive => TopLockMode::Write,
            };
            let key = LockHolderKey {
                resource: info.key,
                mode,
                owner: info.owner.to_string(),
            };
            let priority = lock_priority_label(info.priority);
            // Match the complete holder cohort so replacements cannot reuse stale lease data.
            let state = match lease_holders.remove(&key) {
                Some(lease)
                    if lease.holder_count == holder_count && lease.guard_ids.is_some() && lease.guard_ids == guard_ids =>
                {
                    TopLockState {
                        acquired_at: lease.acquired_at,
                        ttl_secs: lease.ttl_secs,
                        priority,
                    }
                }
                _ => TopLockState {
                    acquired_at: info.acquired_at,
                    ttl_secs: info.expires_at.duration_since(now).unwrap_or(Duration::ZERO).as_secs(),
                    priority,
                },
            };
            (key, state)
        })
        .collect();
    // Longest-held first, matching MinIO's `top locks` ordering intent.
    infos.sort_by_key(|(_, state)| state.acquired_at);
    let total = infos.len();
    let truncated = total > limit;

    let locks = infos
        .into_iter()
        .take(limit)
        .map(|(holder, state)| LockEntry {
            resource: format!("{}/{}", holder.resource.bucket, holder.resource.object),
            bucket: holder.resource.bucket.to_string(),
            object: holder.resource.object.to_string(),
            version: holder.resource.version.as_ref().map(|version| version.to_string()),
            lock_type: holder.mode.label(),
            owner: holder.owner,
            priority: state.priority,
            since: system_time_to_rfc3339(state.acquired_at),
            elapsed_secs: now.duration_since(state.acquired_at).unwrap_or(Duration::ZERO).as_secs(),
            ttl_secs: state.ttl_secs,
        })
        .collect();

    TopLocksResponse {
        total,
        truncated,
        locks,
        capability_note: None,
    }
}

async fn collect_top_locks_with_clients(
    limit: usize,
    manager: Arc<rustfs_lock::GlobalLockManager>,
    clients: Vec<Arc<dyn rustfs_lock::client::LockClient>>,
) -> TopLocksResponse {
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

    let lease_infos = if clients.is_empty() {
        Vec::new()
    } else {
        join_all(clients.iter().map(|client| client.list_lock_leases()))
            .await
            .into_iter()
            .flatten()
            .collect()
    };
    // Capture holders last so released or replaced lease guards fail the merge checks.
    let fast_infos = fast.list_locks_with_holder_generations();

    build_top_locks_response(limit, SystemTime::now(), lease_infos, fast_infos)
}

async fn collect_top_locks(limit: usize) -> TopLocksResponse {
    let manager = get_global_lock_manager();
    let clients = get_global_lock_clients()
        .map(|clients| clients.values().cloned().collect())
        .unwrap_or_default();
    collect_top_locks_with_clients(limit, manager, clients).await
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
        let response = collect_top_locks(limit).await;
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

fn validate_client_devnull_content_length(headers: &HeaderMap) -> S3Result<()> {
    let Some(content_length) = headers.get(CONTENT_LENGTH) else {
        return Ok(());
    };
    let content_length = content_length
        .to_str()
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| s3_error!(InvalidRequest, "invalid Content-Length for client devnull stream"))?;
    if content_length > CLIENT_DEVNULL_MAX_BYTES {
        return Err(s3_error!(
            EntityTooLarge,
            "client devnull stream exceeds the {}-byte limit",
            CLIENT_DEVNULL_MAX_BYTES
        ));
    }
    Ok(())
}

fn acquire_client_devnull_permit() -> S3Result<SemaphorePermit<'static>> {
    CLIENT_DEVNULL_ADMISSION.try_acquire().map_err(|_| {
        s3_error!(
            SlowDown,
            "client devnull concurrency limit of {} is exhausted",
            CLIENT_DEVNULL_MAX_CONCURRENCY
        )
    })
}

async fn drain_client_devnull(input: Body, max_bytes: u64, max_duration: Duration) -> S3Result<u64> {
    tokio::time::timeout(max_duration, async move {
        let mut input = input;
        let mut total = 0u64;
        while let Some(chunk) = input.next().await {
            let chunk = chunk.map_err(|e| s3_error!(InvalidRequest, "failed to read devnull stream: {}", e))?;
            let chunk_len = u64::try_from(chunk.len())
                .map_err(|_| s3_error!(InternalError, "devnull stream chunk length exceeds supported range"))?;
            total = total
                .checked_add(chunk_len)
                .ok_or_else(|| s3_error!(EntityTooLarge, "client devnull stream exceeds the configured byte limit"))?;
            if total > max_bytes {
                return Err(s3_error!(EntityTooLarge, "client devnull stream exceeds the {}-byte limit", max_bytes));
            }
        }
        Ok(total)
    })
    .await
    .map_err(|_| {
        s3_error!(
            RequestTimeout,
            "client devnull stream exceeded the {}-second limit",
            max_duration.as_secs()
        )
    })?
}

async fn drain_client_devnull_with_production_limits(input: Body) -> S3Result<u64> {
    drain_client_devnull(input, CLIENT_DEVNULL_MAX_BYTES, CLIENT_DEVNULL_MAX_DURATION).await
}

#[async_trait::async_trait]
impl Operation for SpeedtestClientDevnullHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize(&req, AdminAction::HealthInfoAdminAction).await?;
        validate_client_devnull_content_length(&req.headers)?;
        let _permit = acquire_client_devnull_permit()?;

        let started = std::time::Instant::now();
        let total = drain_client_devnull_with_production_limits(req.input).await?;
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

    /// These endpoints authorize through the shared admin gate, which rejects a
    /// credential-less request with `InvalidRequest` "get cred failed". The
    /// pre-check keeps the `AccessDenied` response they have always returned
    /// (rustfs/backlog#1829).
    #[tokio::test]
    async fn diagnostics_gate_keeps_its_missing_credentials_response() {
        let err = authorize(
            &build_request(Method::GET, "/rustfs/admin/v3/top/locks"),
            AdminAction::ServerInfoAdminAction,
        )
        .await
        .expect_err("a request without credentials must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Signature is required"));
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

    #[tokio::test]
    async fn client_devnull_enforces_byte_limit_while_streaming() {
        let err = drain_client_devnull(Body::from(vec![0u8; 5]), 4, Duration::from_secs(1))
            .await
            .expect_err("stream over the byte limit must fail");

        assert_eq!(err.code(), &S3ErrorCode::EntityTooLarge);
    }

    #[tokio::test]
    async fn client_devnull_enforces_duration_limit() {
        let (tx, rx) = mpsc::channel::<Result<Bytes, StdError>>(1);
        let stream: DynByteStream = Box::pin(ByteChannelStream {
            inner: ReceiverStream::new(rx),
        });
        let body = Body::from(stream);

        let err = drain_client_devnull(body, 4, Duration::ZERO)
            .await
            .expect_err("stalled stream must time out");
        assert_eq!(err.code(), &S3ErrorCode::RequestTimeout);
        assert!(
            tx.send(Ok(Bytes::from_static(b"late"))).await.is_err(),
            "timeout must drop the request body and cancel its upstream receiver"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn client_devnull_production_limits_enforce_advertised_timeout() {
        let (tx, rx) = mpsc::channel::<Result<Bytes, StdError>>(1);
        let stream: DynByteStream = Box::pin(ByteChannelStream {
            inner: ReceiverStream::new(rx),
        });
        let task = tokio::spawn(drain_client_devnull_with_production_limits(Body::from(stream)));

        tokio::task::yield_now().await;
        tokio::time::advance(CLIENT_DEVNULL_MAX_DURATION + Duration::from_millis(1)).await;

        let err = task
            .await
            .expect("production drain task must not panic")
            .expect_err("stalled production stream must time out at the advertised limit");
        assert_eq!(err.code(), &S3ErrorCode::RequestTimeout);
        assert!(
            tx.send(Ok(Bytes::from_static(b"late"))).await.is_err(),
            "production timeout must drop the request body and cancel its upstream receiver"
        );
    }

    #[tokio::test]
    async fn client_devnull_reports_exact_bounded_byte_count() {
        let total = drain_client_devnull(Body::from(vec![0u8; 4]), 4, Duration::from_secs(1))
            .await
            .expect("stream at the byte limit should succeed");

        assert_eq!(total, 4);
    }

    #[test]
    fn client_devnull_rejects_invalid_content_length() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("not-a-number"));

        let err = validate_client_devnull_content_length(&headers)
            .expect_err("malformed content length must fail before reading the stream");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn client_devnull_rejects_oversized_content_length() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&(CLIENT_DEVNULL_MAX_BYTES + 1).to_string()).expect("valid header"),
        );

        let err = validate_client_devnull_content_length(&headers)
            .expect_err("oversized content length must fail before reading the stream");
        assert_eq!(err.code(), &S3ErrorCode::EntityTooLarge);
    }

    #[test]
    fn client_devnull_admission_fails_fast_and_recovers() {
        let permits = (0..CLIENT_DEVNULL_MAX_CONCURRENCY)
            .map(|_| acquire_client_devnull_permit().expect("admission slot"))
            .collect::<Vec<_>>();
        let err = acquire_client_devnull_permit().expect_err("exhausted admission must fail");
        assert_eq!(err.code(), &S3ErrorCode::SlowDown);

        drop(permits);
        let _permit = acquire_client_devnull_permit().expect("released admission slots must be reusable");
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

    #[test]
    fn top_locks_prefers_renewable_lease_deadlines() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let leased_resource = ObjectKey::new("bucket", "shared-object");
        let exclusive_resource = ObjectKey::new("bucket", "write-object");
        let direct_resource = ObjectKey::new("bucket", "direct-object");
        let mixed_resource = ObjectKey::new("bucket", "mixed-object");
        let replaced_resource = ObjectKey::new("bucket", "replaced-object");
        let remaining_shared_resource = ObjectKey::new("bucket", "remaining-shared-object");
        let opaque_resource = ObjectKey::new("bucket", "opaque-object");

        let response = build_top_locks_response(
            TOP_LOCKS_DEFAULT_LIMIT,
            now,
            vec![
                LockLeaseInfo {
                    resource: leased_resource.clone(),
                    lock_type: LockType::Shared,
                    owner: "owner-a".to_string(),
                    acquired_at: now - Duration::from_secs(50),
                    guard_id: Some(11),
                    remaining_ttl: Duration::from_secs(5),
                },
                LockLeaseInfo {
                    resource: leased_resource.clone(),
                    lock_type: LockType::Shared,
                    owner: "owner-a".to_string(),
                    acquired_at: now - Duration::from_secs(40),
                    guard_id: Some(18),
                    remaining_ttl: Duration::from_secs(20),
                },
                LockLeaseInfo {
                    resource: mixed_resource.clone(),
                    lock_type: LockType::Shared,
                    owner: "owner-c".to_string(),
                    acquired_at: now - Duration::from_secs(30),
                    guard_id: Some(13),
                    remaining_ttl: Duration::from_secs(25),
                },
                LockLeaseInfo {
                    resource: exclusive_resource.clone(),
                    lock_type: LockType::Exclusive,
                    owner: "owner-d".to_string(),
                    acquired_at: now - Duration::from_secs(15),
                    guard_id: Some(12),
                    remaining_ttl: Duration::from_secs(18),
                },
                LockLeaseInfo {
                    resource: replaced_resource.clone(),
                    lock_type: LockType::Exclusive,
                    owner: "owner-e".to_string(),
                    acquired_at: now - Duration::from_secs(30),
                    guard_id: Some(14),
                    remaining_ttl: Duration::from_secs(25),
                },
                LockLeaseInfo {
                    resource: remaining_shared_resource.clone(),
                    lock_type: LockType::Shared,
                    owner: "owner-f".to_string(),
                    acquired_at: now - Duration::from_secs(30),
                    guard_id: Some(16),
                    remaining_ttl: Duration::from_secs(22),
                },
                LockLeaseInfo {
                    resource: opaque_resource.clone(),
                    lock_type: LockType::Exclusive,
                    owner: "owner-g".to_string(),
                    acquired_at: now - Duration::from_secs(30),
                    guard_id: None,
                    remaining_ttl: Duration::from_secs(30),
                },
            ],
            vec![
                (
                    rustfs_lock::ObjectLockInfo {
                        key: replaced_resource,
                        mode: LockMode::Exclusive,
                        owner: "owner-e".into(),
                        acquired_at: now - Duration::from_secs(5),
                        expires_at: now + Duration::from_secs(4),
                        priority: rustfs_lock::fast_lock::LockPriority::Normal,
                    },
                    1,
                    Some(vec![15]),
                ),
                (
                    rustfs_lock::ObjectLockInfo {
                        key: remaining_shared_resource,
                        mode: LockMode::Shared,
                        owner: "owner-f".into(),
                        acquired_at: now - Duration::from_secs(5),
                        expires_at: now + Duration::from_secs(3),
                        priority: rustfs_lock::fast_lock::LockPriority::Normal,
                    },
                    1,
                    Some(vec![16]),
                ),
                (
                    rustfs_lock::ObjectLockInfo {
                        key: exclusive_resource,
                        mode: LockMode::Exclusive,
                        owner: "owner-d".into(),
                        acquired_at: now - Duration::from_secs(15),
                        expires_at: now + Duration::from_secs(2),
                        priority: rustfs_lock::fast_lock::LockPriority::Normal,
                    },
                    1,
                    Some(vec![12]),
                ),
                (
                    rustfs_lock::ObjectLockInfo {
                        key: leased_resource,
                        mode: LockMode::Shared,
                        owner: "owner-a".into(),
                        acquired_at: now - Duration::from_secs(50),
                        expires_at: now + Duration::from_secs(1),
                        priority: rustfs_lock::fast_lock::LockPriority::Normal,
                    },
                    2,
                    Some(vec![11, 18]),
                ),
                (
                    rustfs_lock::ObjectLockInfo {
                        key: direct_resource,
                        mode: LockMode::Exclusive,
                        owner: "owner-b".into(),
                        acquired_at: now - Duration::from_secs(10),
                        expires_at: now + Duration::from_secs(7),
                        priority: rustfs_lock::fast_lock::LockPriority::Normal,
                    },
                    1,
                    Some(vec![17]),
                ),
                (
                    rustfs_lock::ObjectLockInfo {
                        key: mixed_resource,
                        mode: LockMode::Shared,
                        owner: "owner-c".into(),
                        acquired_at: now - Duration::from_secs(30),
                        expires_at: now + Duration::from_secs(9),
                        priority: rustfs_lock::fast_lock::LockPriority::Normal,
                    },
                    2,
                    None,
                ),
                (
                    rustfs_lock::ObjectLockInfo {
                        key: opaque_resource,
                        mode: LockMode::Exclusive,
                        owner: "owner-g".into(),
                        acquired_at: now - Duration::from_secs(4),
                        expires_at: now + Duration::from_secs(6),
                        priority: rustfs_lock::fast_lock::LockPriority::Normal,
                    },
                    1,
                    None,
                ),
            ],
        );

        assert_eq!(response.total, 7);
        let leased = response
            .locks
            .iter()
            .find(|entry| entry.object == "shared-object")
            .expect("lease-backed shared owner should be listed once");
        assert_eq!(leased.lock_type, "READ");
        assert_eq!(leased.elapsed_secs, 50);
        assert_eq!(leased.ttl_secs, 20);

        let exclusive = response
            .locks
            .iter()
            .find(|entry| entry.object == "write-object")
            .expect("lease-backed exclusive holder should be listed");
        assert_eq!(exclusive.lock_type, "WRITE");
        assert_eq!(exclusive.ttl_secs, 18);

        let direct = response
            .locks
            .iter()
            .find(|entry| entry.object == "direct-object")
            .expect("direct fast lock should remain visible");
        assert_eq!(direct.ttl_secs, 7);

        let mixed = response
            .locks
            .iter()
            .find(|entry| entry.object == "mixed-object")
            .expect("mixed direct and leased shared holders should remain visible");
        assert_eq!(mixed.ttl_secs, 9);

        let replaced = response
            .locks
            .iter()
            .find(|entry| entry.object == "replaced-object")
            .expect("a replaced lease holder should remain visible");
        assert_eq!(replaced.ttl_secs, 4);

        let remaining_shared = response
            .locks
            .iter()
            .find(|entry| entry.object == "remaining-shared-object")
            .expect("an older surviving shared lease should remain lease-backed");
        assert_eq!(remaining_shared.ttl_secs, 22);

        let opaque = response
            .locks
            .iter()
            .find(|entry| entry.object == "opaque-object")
            .expect("generation-less holder should remain visible");
        assert_eq!(opaque.ttl_secs, 6);
    }

    #[test]
    fn top_locks_rejects_replaced_shared_generation() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let resource = ObjectKey::new("bucket", "replaced-shared-object");
        let response = build_top_locks_response(
            TOP_LOCKS_DEFAULT_LIMIT,
            now,
            vec![LockLeaseInfo {
                resource: resource.clone(),
                lock_type: LockType::Shared,
                owner: "owner-a".to_string(),
                acquired_at: now - Duration::from_secs(30),
                guard_id: Some(1),
                remaining_ttl: Duration::from_secs(20),
            }],
            vec![(
                rustfs_lock::ObjectLockInfo {
                    key: resource,
                    mode: LockMode::Shared,
                    owner: "owner-a".into(),
                    acquired_at: now - Duration::from_secs(2),
                    expires_at: now + Duration::from_secs(4),
                    priority: rustfs_lock::fast_lock::LockPriority::Normal,
                },
                1,
                Some(vec![2]),
            )],
        );

        let entry = response.locks.first().expect("replacement remains visible");
        assert_eq!(entry.ttl_secs, 4);
        assert_eq!(entry.elapsed_secs, 2);
    }

    #[tokio::test]
    async fn collect_top_locks_reports_live_lock() {
        // Acquire a real lock through the global manager and confirm it surfaces.
        let manager = get_global_lock_manager();
        let key = ObjectKey::new("diag-bucket", "diag-object");
        // The fast-lock manager exposes the acquire API; if the lock subsystem is
        // disabled in this environment, the response must carry a capability note.
        let Some(fast) = manager.as_fast_lock_manager() else {
            let response = collect_top_locks(TOP_LOCKS_DEFAULT_LIMIT).await;
            assert!(response.capability_note.is_some() || response.locks.is_empty());
            return;
        };
        let guard = match fast.acquire_write_lock(key.clone(), "diag-owner").await {
            Ok(g) => g,
            Err(_) => {
                let response = collect_top_locks(TOP_LOCKS_DEFAULT_LIMIT).await;
                assert!(response.capability_note.is_some() || response.locks.is_empty());
                return;
            }
        };

        let response = collect_top_locks(TOP_LOCKS_DEFAULT_LIMIT).await;
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

    #[tokio::test(start_paused = true)]
    async fn collect_top_locks_uses_refreshed_local_lease() {
        use rustfs_lock::{FastObjectLockManager, GlobalLockManager, LocalClient, LockClient, LockRequest};

        let manager = Arc::new(GlobalLockManager::Enabled(Arc::new(FastObjectLockManager::new())));
        let client = Arc::new(LocalClient::with_manager(manager.clone()));
        let request = LockRequest::new(ObjectKey::new("diag-bucket", "renewed-object"), LockType::Exclusive, "diag-owner")
            .with_ttl(Duration::from_secs(30));
        let lock_id = request.lock_id.clone();
        assert!(
            client
                .acquire_lock(&request)
                .await
                .expect("local lock acquisition should succeed")
                .success
        );

        tokio::time::advance(Duration::from_secs(20)).await;
        assert!(client.refresh(&lock_id).await.expect("local lease refresh should succeed"));

        let clients: Vec<Arc<dyn rustfs_lock::client::LockClient>> = vec![client.clone()];
        let response = collect_top_locks_with_clients(TOP_LOCKS_DEFAULT_LIMIT, manager, clients).await;
        let entry = response
            .locks
            .iter()
            .find(|entry| entry.bucket == "diag-bucket" && entry.object == "renewed-object")
            .expect("refreshed local lock should be listed");
        assert!(entry.ttl_secs >= 29, "collector must use the refreshed lease deadline");

        assert!(client.release(&lock_id).await.expect("local lock release should succeed"));
    }
}
