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

use crate::cluster::rpc::client::{
    TonicInterceptor, gen_tonic_signature_interceptor, is_network_like_disk_error, node_service_time_out_client,
};
use crate::cluster::rpc::internode_data_transport::{
    InternodeDataTransport, ReadStreamRequest, WalkDirStreamRequest, WriteStreamRequest,
};
use crate::disk::error::{Error, Result};
use crate::disk::{
    BatchReadVersionReq, BatchReadVersionResp, CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskLocation,
    DiskOption, FileInfoVersions, FileReader, FileWriter, ReadMultipleReq, ReadMultipleResp, ReadOptions, RenameDataResp,
    UpdateMetadataOpts, VolumeInfo, WalkDirOptions, batch_read_version_one_by_one,
    disk_store::{
        DEFAULT_RUSTFS_DRIVE_ACTIVE_MONITORING, ENV_RUSTFS_DRIVE_ACTIVE_MONITORING, SKIP_IF_SUCCESS_BEFORE,
        get_drive_active_check_interval, get_drive_active_check_timeout, get_drive_disk_info_timeout, get_drive_list_dir_timeout,
        get_drive_metadata_timeout, get_drive_walkdir_stall_timeout, get_drive_walkdir_timeout, get_max_timeout_duration,
        get_object_disk_read_timeout,
    },
    endpoint::Endpoint,
    health_state::{RuntimeDriveHealthState, get_drive_returning_probe_interval, record_drive_runtime_state},
    validate_batch_read_version_item_count,
};
use crate::disk::{disk_store::DiskHealthTracker, error::DiskError, local::ScanGuard};
use crate::set_disk::DEFAULT_READ_BUFFER_SIZE;
use bytes::Bytes;
use futures::lock::Mutex;
use metrics::counter;
use rustfs_filemeta::{FileInfo, ObjectPartInfo, RawFileInfo};
use rustfs_protos::evict_failed_connection;
use rustfs_protos::proto_gen::node_service::RenamePartRequest;
use rustfs_protos::proto_gen::node_service::{
    BatchReadVersionRequest, BatchReadVersionResponse, CheckPartsRequest, DeletePathsRequest, DeleteRequest,
    DeleteVersionRequest, DeleteVersionsRequest, DeleteVolumeRequest, DiskInfoRequest, ListDirRequest, ListVolumesRequest,
    MakeVolumeRequest, MakeVolumesRequest, ReadAllRequest, ReadMetadataRequest, ReadMultipleRequest, ReadMultipleResponse,
    ReadPartsRequest, ReadVersionRequest, ReadXlRequest, RenameDataRequest, RenameFileRequest, StatVolumeRequest,
    UpdateMetadataRequest, VerifyFileRequest, WriteAllRequest, WriteMetadataRequest, node_service_client::NodeServiceClient,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    io::Cursor,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio::time;
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, service::interceptor::InterceptedService, transport::Channel};
use tracing::{Instrument, debug, warn};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailureHealthAction {
    MarkFailure,
    IgnoreFailure,
}

const REMOTE_DISK_OPEN_WRITE_MAX_ATTEMPTS: usize = 2;
const REMOTE_DISK_OPEN_WRITE_RETRY_BACKOFF: Duration = Duration::from_millis(20);
const ENV_RUSTFS_METADATA_BATCH_READ: &str = "RUSTFS_METADATA_BATCH_READ";
const LEGACY_ENV_RUSTFS_BATCH_METADATA_RPC: &str = "RUSTFS_BATCH_METADATA_RPC";
const BATCH_METADATA_RPC_OFF: &str = "off";
const BATCH_METADATA_RPC_AUTO: &str = "auto";
const BATCH_METADATA_RPC_ON: &str = "on";
const BATCH_READ_VERSION_GATE_ATTEMPT: &str = "attempt";
const BATCH_READ_VERSION_GATE_OFF_UNARY: &str = "off_unary";
const BATCH_READ_VERSION_GATE_FALLBACK_UNIMPLEMENTED: &str = "fallback_unimplemented";
const BATCH_READ_VERSION_GATE_UNSUPPORTED_NO_FALLBACK: &str = "unsupported_no_fallback";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_REMOTE_DISK: &str = "remote_disk";
const EVENT_REMOTE_DISK_HEALTH: &str = "remote_disk_health";
const EVENT_REMOTE_DISK_RPC: &str = "remote_disk_rpc";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchMetadataRpcMode {
    Off,
    Auto,
    On,
}

impl BatchMetadataRpcMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Off => BATCH_METADATA_RPC_OFF,
            Self::Auto => BATCH_METADATA_RPC_AUTO,
            Self::On => BATCH_METADATA_RPC_ON,
        }
    }

    fn should_attempt(self) -> bool {
        matches!(self, Self::Auto | Self::On)
    }

    fn should_fallback_on_unimplemented(self) -> bool {
        matches!(self, Self::Auto)
    }
}

fn parse_batch_metadata_rpc_mode(raw: &str) -> BatchMetadataRpcMode {
    match raw.trim() {
        value if value.eq_ignore_ascii_case(BATCH_METADATA_RPC_AUTO) => BatchMetadataRpcMode::Auto,
        value if value.eq_ignore_ascii_case(BATCH_METADATA_RPC_ON) => BatchMetadataRpcMode::On,
        value if value.eq_ignore_ascii_case(BATCH_METADATA_RPC_OFF) => BatchMetadataRpcMode::Off,
        _ => BatchMetadataRpcMode::Off,
    }
}

fn batch_metadata_rpc_mode_from_env() -> BatchMetadataRpcMode {
    rustfs_utils::get_env_opt_str(ENV_RUSTFS_METADATA_BATCH_READ)
        .or_else(|| rustfs_utils::get_env_opt_str(LEGACY_ENV_RUSTFS_BATCH_METADATA_RPC))
        .as_deref()
        .map(parse_batch_metadata_rpc_mode)
        .unwrap_or(BatchMetadataRpcMode::Off)
}

fn batch_metadata_rpc_mode() -> BatchMetadataRpcMode {
    // The gate cannot change at runtime; parse it once instead of re-reading
    // the environment on every batch RPC.
    static MODE: std::sync::LazyLock<BatchMetadataRpcMode> = std::sync::LazyLock::new(batch_metadata_rpc_mode_from_env);
    *MODE
}

fn record_batch_read_version_gate_decision(mode: BatchMetadataRpcMode, decision: &'static str) {
    counter!(
        "rustfs_remote_disk_batch_read_version_gate_total",
        "mode" => mode.as_str(),
        "decision" => decision
    )
    .increment(1);
}

async fn copy_stream_with_buffer<R, W>(reader: &mut R, writer: &mut W, buffer_size: usize) -> io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut copied = 0_u64;
    let mut buffer = vec![0_u8; buffer_size];

    loop {
        let bytes_read = reader.read(&mut buffer).await?;
        if bytes_read == 0 {
            writer.flush().await?;
            return Ok(copied);
        }

        writer.write_all(&buffer[..bytes_read]).await?;
        copied += bytes_read as u64;
    }
}

#[derive(Debug)]
pub struct RemoteDisk {
    pub id: Mutex<Option<Uuid>>,
    pub addr: String,
    endpoint: Endpoint,
    pub scanning: Arc<AtomicU32>,
    /// Whether health checking is enabled
    health_check: bool,
    /// Health tracker for connection monitoring
    health: Arc<DiskHealthTracker>,
    /// Cancellation token for monitoring tasks
    cancel_token: CancellationToken,
    data_transport: Arc<dyn InternodeDataTransport>,
}

impl RemoteDisk {
    fn recovery_monitor_span(addr: &str, endpoint: &Endpoint) -> tracing::Span {
        tracing::info_span!(
            "recovery-monitor",
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            kind = "remote_disk",
            endpoint = %endpoint,
            addr = %addr
        )
    }

    fn is_retryable_walk_dir_error(err: &DiskError) -> bool {
        if is_network_like_disk_error(err) {
            return true;
        }

        let err_text = err.to_string().to_ascii_lowercase();
        err_text.contains("httpreader stream error") || err_text.contains("error decoding response body")
    }

    fn is_retryable_open_write_error(err: &DiskError) -> bool {
        err.is_retryable_internode_write_failure()
    }

    pub(crate) async fn new(ep: &Endpoint, opt: &DiskOption, data_transport: Arc<dyn InternodeDataTransport>) -> Result<Self> {
        let addr = if let Some(port) = ep.url.port() {
            format!("{}://{}:{}", ep.url.scheme(), ep.url.host_str().expect("operation should succeed"), port)
        } else {
            format!("{}://{}", ep.url.scheme(), ep.url.host_str().expect("operation should succeed"))
        };

        let env_health_check =
            rustfs_utils::get_env_bool(ENV_RUSTFS_DRIVE_ACTIVE_MONITORING, DEFAULT_RUSTFS_DRIVE_ACTIVE_MONITORING);

        let disk = Self {
            id: Mutex::new(None),
            addr,
            endpoint: ep.clone(),
            scanning: Arc::new(AtomicU32::new(0)),
            health_check: opt.health_check && env_health_check,
            health: Arc::new(DiskHealthTracker::new()),
            cancel_token: CancellationToken::new(),
            data_transport,
        };
        record_drive_runtime_state(ep, RuntimeDriveHealthState::Online);

        Ok(disk)
    }

    pub fn runtime_state(&self) -> RuntimeDriveHealthState {
        self.health.runtime_state()
    }

    pub fn offline_duration_secs(&self) -> Option<u64> {
        self.health.offline_duration().map(|duration| duration.as_secs())
    }

    pub fn last_capacity_snapshot(&self) -> Option<(u64, u64, u64, u64)> {
        self.health.last_capacity_snapshot()
    }

    async fn open_write_with_retry(&self, request: WriteStreamRequest) -> Result<FileWriter> {
        let mut attempt = 1;
        let mut last_retry_classification = None;
        loop {
            match self.data_transport.open_write(request.clone()).await {
                Ok(writer) => {
                    if attempt > 1
                        && let Some(classification) = last_retry_classification
                    {
                        crate::cluster::rpc::runtime_sources::record_remote_disk_open_write_retry_success(classification);
                    }
                    return Ok(writer);
                }
                Err(err) if attempt < REMOTE_DISK_OPEN_WRITE_MAX_ATTEMPTS && Self::is_retryable_open_write_error(&err) => {
                    if let Some(classification) = err.internode_http_error_kind() {
                        let classification = classification.metric_label();
                        crate::cluster::rpc::runtime_sources::record_remote_disk_open_write_retry(classification);
                        last_retry_classification = Some(classification);
                    }
                    debug!(
                        endpoint = %request.endpoint,
                        volume = %request.volume,
                        path = %request.path,
                        append = request.append,
                        size = request.size,
                        attempt,
                        "retrying remote open_write after retryable transport error"
                    );
                    tokio::time::sleep(REMOTE_DISK_OPEN_WRITE_RETRY_BACKOFF).await;
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn record_capacity_probe(&self, total: u64, used: u64, free: u64) {
        self.health.record_capacity_probe(total, used, free);
    }

    #[cfg(test)]
    pub fn force_runtime_state_for_test(&self, state: RuntimeDriveHealthState) {
        self.health.force_runtime_state_for_test(state);
    }

    /// Same as [`DiskHealthTracker::reset_for_store_init_retry`]: undo a transient faulty mark before another format load attempt.
    pub fn reset_health_for_store_init_retry(&self) {
        self.health.reset_for_store_init_retry(&self.endpoint);
    }

    #[cfg(test)]
    pub fn health_check_enabled_for_test(&self) -> bool {
        self.health_check
    }

    fn spawn_recovery_monitor_if_needed(&self) {
        if !self.health_check {
            return;
        }

        let addr = self.addr.clone();
        let endpoint = self.endpoint.clone();
        let health = Arc::clone(&self.health);
        let cancel_token = self.cancel_token.clone();
        let span = Self::recovery_monitor_span(&addr, &endpoint);
        tokio::spawn(
            async move {
                Self::monitor_remote_disk_recovery(addr, endpoint, health, cancel_token).await;
            }
            .instrument(span),
        );
    }

    #[cfg(test)]
    fn spawn_recovery_monitor_log_probe_for_test(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let endpoint = self.endpoint.clone();
        let addr = self.addr.clone();
        let span = Self::recovery_monitor_span(&addr, &endpoint);
        tokio::spawn(
            async move {
                warn!(
                    event = EVENT_REMOTE_DISK_HEALTH,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                    endpoint = %endpoint,
                    addr,
                    state = "probe",
                    "remote disk recovery monitor log probe"
                );
                let _ = tx.send(());
            }
            .instrument(span),
        );
        rx
    }

    fn mark_suspect_or_offline(&self, reason: &'static str) -> bool {
        self.health.mark_failure(&self.endpoint, reason)
    }

    /// Enable health monitoring after disk creation.
    /// Used to defer health checks until after startup format loading completes,
    /// so that remote peers have time to come online.
    pub fn enable_health_check(&self) {
        if !self.health_check {
            return;
        }
        let health = Arc::clone(&self.health);
        let cancel_token = self.cancel_token.clone();
        let addr = self.addr.clone();
        let endpoint = self.endpoint.clone();

        tokio::spawn(async move {
            Self::monitor_remote_disk_health(addr, endpoint, health, cancel_token).await;
        });
    }

    /// Monitor remote disk health periodically
    async fn monitor_remote_disk_health(
        addr: String,
        endpoint: Endpoint,
        health: Arc<DiskHealthTracker>,
        cancel_token: CancellationToken,
    ) {
        let mut interval = time::interval(get_drive_active_check_interval());

        // Perform basic connectivity check
        let initial_probe_ok = Self::perform_connectivity_check(&addr).await.is_ok();
        if initial_probe_ok {
            health.record_operation_success(&endpoint, "connectivity_probe_success");
        } else if health.mark_failure(&endpoint, "connectivity_probe_failed") {
            warn!(
                event = EVENT_REMOTE_DISK_HEALTH,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %endpoint,
                addr,
                state = "initial_probe_failed",
                result = "mark_faulty",
                "Remote disk initial health probe failed"
            );

            // Start recovery monitoring
            let health_clone = Arc::clone(&health);
            let addr_clone = addr.clone();
            let endpoint_clone = endpoint.clone();
            let cancel_clone = cancel_token.clone();
            let span = Self::recovery_monitor_span(&addr_clone, &endpoint_clone);

            tokio::spawn(
                async move {
                    Self::monitor_remote_disk_recovery(addr_clone, endpoint_clone, health_clone, cancel_clone).await;
                }
                .instrument(span),
            );
        }

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!(
                        event = EVENT_REMOTE_DISK_HEALTH,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                        endpoint = %endpoint,
                        addr,
                        state = "monitor_cancelled",
                        "Remote disk health monitor cancelled"
                    );
                    return;
                }
                _ = interval.tick() => {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    // Skip health check if disk is already marked as faulty
                    if health.is_faulty() {
                        continue;
                    }

                    let last_success_nanos = health.last_success.load(Ordering::Relaxed);
                    let elapsed = Duration::from_nanos(
                        (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("operation should succeed")
                            .as_nanos() as i64 - last_success_nanos) as u64
                    );

                    if elapsed < SKIP_IF_SUCCESS_BEFORE {
                        continue;
                    }

                    // Perform basic connectivity check
                    if Self::perform_connectivity_check(&addr).await.is_ok() {
                        health.record_operation_success(&endpoint, "connectivity_probe_success");
                    } else if health.mark_failure(&endpoint, "connectivity_probe_failed") {
                        warn!(
                            event = EVENT_REMOTE_DISK_HEALTH,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                            endpoint = %endpoint,
                            addr,
                            state = "probe_failed",
                            result = "mark_faulty",
                            "Remote disk health probe failed"
                        );

                        // Start recovery monitoring
                        let health_clone = Arc::clone(&health);
                        let addr_clone = addr.clone();
                        let endpoint_clone = endpoint.clone();
                        let cancel_clone = cancel_token.clone();
                        let span = Self::recovery_monitor_span(&addr_clone, &endpoint_clone);

                        tokio::spawn(
                            async move {
                                Self::monitor_remote_disk_recovery(addr_clone, endpoint_clone, health_clone, cancel_clone).await;
                            }
                            .instrument(span),
                        );
                    }
                }
            }
        }
    }

    /// Monitor remote disk recovery and mark as healthy when recovered
    async fn monitor_remote_disk_recovery(
        addr: String,
        endpoint: Endpoint,
        health: Arc<DiskHealthTracker>,
        cancel_token: CancellationToken,
    ) {
        let mut interval = time::interval(get_drive_returning_probe_interval());
        debug!(
            event = EVENT_REMOTE_DISK_HEALTH,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %endpoint,
            addr,
            state = "recovery_monitor_started",
            "Remote disk recovery monitor started"
        );

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!(
                        event = EVENT_REMOTE_DISK_HEALTH,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                        endpoint = %endpoint,
                        addr,
                        state = "recovery_monitor_cancelled",
                        "Remote disk recovery monitor cancelled"
                    );
                    return;
                }
                _ = interval.tick() => {
                    if Self::perform_recovery_probe(&addr, &endpoint).await.is_ok() {
                        let became_online = health.mark_recovery_success(&endpoint, "disk_info_probe_success");
                        debug!(
                            event = EVENT_REMOTE_DISK_HEALTH,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                            endpoint = %endpoint,
                            addr,
                            state = "recovery_probe_succeeded",
                            "Remote disk recovery probe succeeded"
                        );
                        if became_online {
                            debug!(
                                event = EVENT_REMOTE_DISK_HEALTH,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                                endpoint = %endpoint,
                                addr,
                                state = "recovered",
                                "Remote disk recovered"
                            );
                            return;
                        }
                    } else {
                        health.mark_failure(&endpoint, "disk_info_probe_failed");
                    }
                }
            }
        }
    }

    async fn perform_recovery_probe(addr: &str, endpoint: &Endpoint) -> Result<()> {
        let mut evict_cached_connection = false;
        let result = match timeout(get_drive_active_check_timeout(), async {
            let opts = serde_json::to_string(&DiskInfoOptions {
                noop: true,
                ..Default::default()
            })
            .map_err(|err| (Error::other(format!("encode DiskInfoOptions failed: {err}")), false))?;
            let addr = addr.to_string();
            let mut client = node_service_time_out_client(&addr, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
                .await
                .map_err(|err| (Error::other(format!("can not get client, err: {err}")), true))?;
            let request = Request::new(DiskInfoRequest {
                disk: endpoint.to_string(),
                opts,
            });
            let response = client
                .disk_info(request)
                .await
                .map_err(|err| (Error::from(err), true))?
                .into_inner();
            if !response.success {
                return Err((response.error.unwrap_or_default().into(), false));
            }
            Ok(())
        })
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err((err, should_evict))) => {
                evict_cached_connection = should_evict;
                Err(err)
            }
            Err(_) => {
                evict_cached_connection = true;
                Err(DiskError::Timeout)
            }
        };

        if evict_cached_connection {
            evict_failed_connection(addr).await;
        }

        result
    }

    /// Perform basic connectivity check for remote disk
    async fn perform_connectivity_check(addr: &str) -> Result<()> {
        let url = url::Url::parse(addr).map_err(|e| Error::other(format!("Invalid URL: {e}")))?;

        let Some(host) = url.host_str() else {
            return Err(Error::other("No host in URL".to_string()));
        };

        let port = url.port_or_known_default().unwrap_or(80);

        // Try to establish TCP connection
        match timeout(get_drive_active_check_timeout(), TcpStream::connect((host, port))).await {
            Ok(Ok(stream)) => {
                drop(stream);
                Ok(())
            }
            _ => Err(Error::other(format!("Cannot connect to {host}:{port}"))),
        }
    }

    /// Execute operation with timeout and health tracking
    async fn execute_with_timeout<T, F, Fut>(&self, operation: F, timeout_duration: Duration) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.execute_with_timeout_for_op("unknown", operation, timeout_duration).await
    }

    async fn execute_with_timeout_for_op<T, F, Fut>(
        &self,
        op: &'static str,
        operation: F,
        timeout_duration: Duration,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.execute_with_timeout_for_op_and_health_action(op, operation, timeout_duration, FailureHealthAction::MarkFailure)
            .await
    }

    async fn execute_with_timeout_for_op_and_health_action<T, F, Fut>(
        &self,
        op: &'static str,
        operation: F,
        timeout_duration: Duration,
        failure_health_action: FailureHealthAction,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Check if disk is faulty
        if self.health.is_faulty() {
            debug!(
                event = EVENT_REMOTE_DISK_HEALTH,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %self.endpoint,
                addr = %self.addr,
                op,
                state = "faulty_short_circuit",
                "Remote disk operation short-circuited by faulty state"
            );
            return Err(DiskError::FaultyDisk);
        }

        // Record operation start
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("operation should succeed")
            .as_nanos() as i64;
        self.health.last_started.store(now, std::sync::atomic::Ordering::Relaxed);
        self.health.increment_waiting();

        if timeout_duration == Duration::ZERO {
            let operation_result = operation().await;
            if operation_result.is_ok() {
                self.health.log_success();
            }
            self.health.decrement_waiting();
            self.handle_network_like_error(op, timeout_duration, &operation_result, failure_health_action)
                .await;
            return operation_result;
        }

        // Execute operation with timeout
        let result = time::timeout(timeout_duration, operation()).await;

        match result {
            Ok(operation_result) => {
                // Log success and decrement waiting counter
                if operation_result.is_ok() {
                    self.health.log_success();
                }
                self.health.decrement_waiting();
                self.handle_network_like_error(op, timeout_duration, &operation_result, failure_health_action)
                    .await;
                operation_result
            }
            Err(_) => {
                // Timeout occurred, mark disk as potentially faulty
                self.health.decrement_waiting();
                counter!(
                    "rustfs_drive_op_timeout_total",
                    "endpoint" => self.endpoint.to_string(),
                    "op" => op.to_string()
                )
                .increment(1);
                if failure_health_action == FailureHealthAction::MarkFailure {
                    self.mark_faulty_and_evict("operation_timeout").await;
                }
                warn!(
                    event = EVENT_REMOTE_DISK_RPC,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                    endpoint = %self.endpoint,
                    addr = %self.addr,
                    op,
                    timeout_ms = timeout_duration.as_millis(),
                    state = "timeout",
                    "Remote disk operation timed out"
                );
                Err(DiskError::Timeout)
            }
        }
    }

    async fn handle_network_like_error<T>(
        &self,
        op: &'static str,
        timeout_duration: Duration,
        operation_result: &Result<T>,
        failure_health_action: FailureHealthAction,
    ) {
        if let Err(err) = operation_result
            && is_network_like_disk_error(err)
        {
            counter!(
                "rustfs_drive_op_network_error_total",
                "endpoint" => self.endpoint.to_string(),
                "op" => op.to_string()
            )
            .increment(1);
            warn!(
                event = EVENT_REMOTE_DISK_RPC,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %self.endpoint,
                addr = %self.addr,
                op,
                timeout_ms = timeout_duration.as_millis(),
                state = "network_like_error",
                "Remote disk operation returned network-like error"
            );
            if failure_health_action == FailureHealthAction::MarkFailure {
                self.mark_faulty_and_evict("operation_network_error").await;
            }
        }
    }

    async fn mark_faulty_and_evict(&self, reason: &'static str) {
        let previous_state = self.runtime_state();
        let transitioned_to_offline = self.mark_suspect_or_offline(reason);
        let state = self.runtime_state();

        if state != previous_state {
            self.spawn_recovery_monitor_if_needed();
            counter!(
                "rustfs_drive_faulty_mark_total",
                "endpoint" => self.endpoint.to_string(),
                "reason" => reason.to_string()
            )
            .increment(1);
            if transitioned_to_offline {
                warn!(
                    event = EVENT_REMOTE_DISK_HEALTH,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                    endpoint = %self.endpoint,
                    addr = %self.addr,
                    reason,
                    state = "marked_faulty",
                    "Remote disk marked faulty"
                );
            } else {
                warn!(
                    event = EVENT_REMOTE_DISK_HEALTH,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                    endpoint = %self.endpoint,
                    addr = %self.addr,
                    reason,
                    runtime_state = ?state,
                    state = "marked_suspect",
                    "Remote disk marked suspect"
                );
            }
            counter!(
                "rustfs_drive_connection_evict_total",
                "endpoint" => self.endpoint.to_string(),
                "reason" => reason.to_string()
            )
            .increment(1);
            debug!(
                event = EVENT_REMOTE_DISK_HEALTH,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %self.endpoint,
                addr = %self.addr,
                reason,
                state = "evict_cached_connection",
                "Remote disk cached connection evicted"
            );
            evict_failed_connection(&self.addr).await;
        }
    }

    async fn get_client(&self) -> Result<NodeServiceClient<InterceptedService<Channel, TonicInterceptor>>> {
        node_service_time_out_client(&self.addr, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))
    }

    async fn disk_ref(&self) -> String {
        (*self.id.lock().await)
            .map(|id| id.to_string())
            .unwrap_or_else(|| self.endpoint.to_string())
    }
}

fn encode_msgpack<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut serializer = rmp_serde::Serializer::new(Vec::new());
    value.serialize(&mut serializer)?;
    Ok(serializer.into_inner())
}

fn encode_msgpack_named<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut serializer = rmp_serde::Serializer::new(Vec::new()).with_struct_map();
    value.serialize(&mut serializer)?;
    Ok(serializer.into_inner())
}

fn decode_msgpack_or_json<T: DeserializeOwned>(binary: &[u8], json: &str) -> Result<T> {
    if !binary.is_empty() {
        let mut deserializer = rmp_serde::Deserializer::new(Cursor::new(binary));
        return T::deserialize(&mut deserializer).map_err(Error::from);
    }

    serde_json::from_str(json).map_err(Error::from)
}

fn decode_read_multiple_response_items(response: ReadMultipleResponse, endpoint: &Endpoint) -> Result<Vec<ReadMultipleResp>> {
    if !response.read_multiple_resps_bin.is_empty() {
        if !response.read_multiple_resps.is_empty()
            && response.read_multiple_resps.len() != response.read_multiple_resps_bin.len()
        {
            warn!(
                event = EVENT_REMOTE_DISK_RPC,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %endpoint,
                json_count = response.read_multiple_resps.len(),
                msgpack_count = response.read_multiple_resps_bin.len(),
                op = "read_multiple",
                state = "response_count_mismatch",
                "Remote disk ReadMultiple compatibility payload counts differ"
            );
        }

        let mut read_multiple_resps = Vec::with_capacity(response.read_multiple_resps_bin.len());
        for (index, buf) in response.read_multiple_resps_bin.iter().enumerate() {
            let resp = decode_msgpack_or_json::<ReadMultipleResp>(buf, "").map_err(|err| {
                Error::other(format!("decode ReadMultipleResp msgpack item {index} from {endpoint} failed: {err}"))
            })?;
            read_multiple_resps.push(resp);
        }
        return Ok(read_multiple_resps);
    }

    let mut read_multiple_resps = Vec::with_capacity(response.read_multiple_resps.len());
    for (index, json_str) in response.read_multiple_resps.iter().enumerate() {
        let resp = serde_json::from_str::<ReadMultipleResp>(json_str)
            .map_err(|err| Error::other(format!("decode ReadMultipleResp json item {index} from {endpoint} failed: {err}")))?;
        read_multiple_resps.push(resp);
    }

    Ok(read_multiple_resps)
}

fn decode_batch_read_version_response_items(
    response: BatchReadVersionResponse,
    endpoint: &Endpoint,
) -> Result<Vec<BatchReadVersionResp>> {
    if !response.batch_read_version_resps_bin.is_empty() {
        if !response.batch_read_version_resps.is_empty()
            && response.batch_read_version_resps.len() != response.batch_read_version_resps_bin.len()
        {
            warn!(
                event = EVENT_REMOTE_DISK_RPC,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %endpoint,
                json_count = response.batch_read_version_resps.len(),
                msgpack_count = response.batch_read_version_resps_bin.len(),
                op = "batch_read_version",
                state = "response_count_mismatch",
                "Remote disk BatchReadVersion compatibility payload counts differ"
            );
        }

        let mut batch_read_version_resps = Vec::with_capacity(response.batch_read_version_resps_bin.len());
        for (index, buf) in response.batch_read_version_resps_bin.iter().enumerate() {
            let resp = decode_msgpack_or_json::<BatchReadVersionResp>(buf, "").map_err(|err| {
                Error::other(format!("decode BatchReadVersionResp msgpack item {index} from {endpoint} failed: {err}"))
            })?;
            batch_read_version_resps.push(resp);
        }
        return Ok(batch_read_version_resps);
    }

    let mut batch_read_version_resps = Vec::with_capacity(response.batch_read_version_resps.len());
    for (index, json_str) in response.batch_read_version_resps.iter().enumerate() {
        let resp = serde_json::from_str::<BatchReadVersionResp>(json_str).map_err(|err| {
            Error::other(format!("decode BatchReadVersionResp json item {index} from {endpoint} failed: {err}"))
        })?;
        batch_read_version_resps.push(resp);
    }

    Ok(batch_read_version_resps)
}

#[async_trait::async_trait]
impl DiskAPI for RemoteDisk {
    #[tracing::instrument(skip(self))]
    fn to_string(&self) -> String {
        self.endpoint.to_string()
    }

    #[tracing::instrument(skip(self))]
    async fn is_online(&self) -> bool {
        // If disk is marked as faulty, consider it offline
        !self.health.is_faulty()
    }

    #[tracing::instrument(skip(self))]
    fn is_local(&self) -> bool {
        false
    }
    #[tracing::instrument(skip(self))]
    fn host_name(&self) -> String {
        self.endpoint.host_port()
    }
    #[tracing::instrument(skip(self))]
    fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }
    #[tracing::instrument(skip(self))]
    async fn close(&self) -> Result<()> {
        self.cancel_token.cancel();
        Ok(())
    }
    #[tracing::instrument(skip(self))]
    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        Ok(*self.id.lock().await)
    }

    #[tracing::instrument(skip(self))]
    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        let mut lock = self.id.lock().await;
        *lock = id;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn path(&self) -> PathBuf {
        PathBuf::from(self.endpoint.get_file_path())
    }

    #[tracing::instrument(skip(self))]
    fn get_disk_location(&self) -> DiskLocation {
        DiskLocation {
            pool_idx: {
                if self.endpoint.pool_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.pool_idx as usize)
                }
            },
            set_idx: {
                if self.endpoint.set_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.set_idx as usize)
                }
            },
            disk_idx: {
                if self.endpoint.disk_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.disk_idx as usize)
                }
            },
        }
    }

    #[tracing::instrument(skip(self))]
    async fn make_volume(&self, volume: &str) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            op = "make_volume",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(MakeVolumeRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                });

                let response = client.make_volume(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume_count = volumes.len(),
            op = "make_volumes",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(MakeVolumesRequest {
                    disk: self.endpoint.to_string(),
                    volumes: volumes.iter().map(|s| (*s).to_string()).collect(),
                });

                let response = client.make_volumes(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            op = "list_volumes",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ListVolumesRequest {
                    disk: self.endpoint.to_string(),
                });

                let response = client.list_volumes(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let infos = response
                    .volume_infos
                    .into_iter()
                    .filter_map(|json_str| serde_json::from_str::<VolumeInfo>(&json_str).ok())
                    .collect();

                Ok(infos)
            },
            Duration::ZERO,
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            op = "stat_volume",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(StatVolumeRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                });

                let response = client.stat_volume(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let volume_info = serde_json::from_str::<VolumeInfo>(&response.volume_info)?;

                Ok(volume_info)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn delete_volume(&self, volume: &str) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            op = "delete_volume",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(DeleteVolumeRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                });

                let response = client.delete_volume(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            Duration::ZERO,
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "delete_version",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let file_info = serde_json::to_string(&fi)?;
                let opts = serde_json::to_string(&opts)?;

                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(DeleteVersionRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    file_info,
                    force_del_marker,
                    opts,
                });

                let response = client.delete_version(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                // let raw_file_info = serde_json::from_str::<RawFileInfo>(&response.raw_file_info)?;

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn delete_versions(&self, volume: &str, versions: Vec<FileInfoVersions>, opts: DeleteOptions) -> Vec<Option<Error>> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            version_count = versions.len(),
            op = "delete_versions",
            state = "started",
            "Remote disk RPC started"
        );

        if self.health.is_faulty() {
            return vec![Some(DiskError::FaultyDisk); versions.len()];
        }

        let opts = match serde_json::to_string(&opts) {
            Ok(opts) => opts,
            Err(err) => {
                let mut errors = Vec::with_capacity(versions.len());
                for _ in 0..versions.len() {
                    errors.push(Some(Error::other(err.to_string())));
                }
                return errors;
            }
        };
        let mut versions_str = Vec::with_capacity(versions.len());
        for file_info_versions in versions.iter() {
            versions_str.push(match serde_json::to_string(file_info_versions) {
                Ok(versions_str) => versions_str,
                Err(err) => {
                    let mut errors = Vec::with_capacity(versions.len());
                    for _ in 0..versions.len() {
                        errors.push(Some(Error::other(err.to_string())));
                    }
                    return errors;
                }
            });
        }
        let mut client = match self.get_client().await {
            Ok(client) => client,
            Err(err) => {
                let mut errors = Vec::with_capacity(versions.len());
                for _ in 0..versions.len() {
                    errors.push(Some(Error::other(err.to_string())));
                }
                return errors;
            }
        };

        let request = Request::new(DeleteVersionsRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            versions: versions_str,
            opts,
        });

        // TODO: use Error not string

        let result = self
            .execute_with_timeout(
                || async {
                    client
                        .delete_versions(request)
                        .await
                        .map_err(|err| Error::other(format!("delete_versions failed: {err}")))
                },
                get_max_timeout_duration(),
            )
            .await;

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                let mut errors = Vec::with_capacity(versions.len());
                for _ in 0..versions.len() {
                    errors.push(Some(err.clone()));
                }
                return errors;
            }
        };

        let response = response.into_inner();
        if !response.success {
            let mut errors = Vec::with_capacity(versions.len());
            for _ in 0..versions.len() {
                errors.push(Some(Error::other(response.error.clone().map(|e| e.error_info).unwrap_or_default())));
            }
            return errors;
        }
        response
            .errors
            .iter()
            .map(|error| {
                if error.is_empty() {
                    None
                } else {
                    Some(Error::other(error.to_string()))
                }
            })
            .collect()
    }

    #[tracing::instrument(skip(self))]
    async fn delete_paths(&self, volume: &str, paths: &[String]) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path_count = paths.len(),
            op = "delete_paths",
            state = "started",
            "Remote disk RPC started"
        );
        let paths = paths.to_owned();

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(DeletePathsRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    paths: paths.clone(),
                });

                let response = client.delete_paths(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "write_metadata",
            state = "started",
            "Remote disk RPC started"
        );
        let file_info = serde_json::to_string(&fi)?;
        let file_info_bin = encode_msgpack(&fi)?;

        self.execute_with_timeout_for_op(
            "write_metadata",
            move || async move {
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(WriteMetadataRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                    file_info,
                    file_info_bin: file_info_bin.into(),
                });

                let response = client.write_metadata(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_metadata(&self, volume: &str, path: &str) -> Result<Bytes> {
        self.execute_with_timeout_for_op(
            "read_metadata",
            || async {
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ReadMetadataRequest {
                    volume: volume.to_string(),
                    path: path.to_string(),
                    disk,
                });

                let response = client.read_metadata(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(response.data)
            },
            get_drive_metadata_timeout(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "update_metadata",
            state = "started",
            "Remote disk RPC started"
        );
        let file_info = serde_json::to_string(&fi)?;
        let opts_str = serde_json::to_string(&opts)?;
        let file_info_bin = encode_msgpack(&fi)?;
        let opts_bin = encode_msgpack(opts)?;

        self.execute_with_timeout_for_op(
            "update_metadata",
            move || async move {
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(UpdateMetadataRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                    file_info,
                    opts: opts_str,
                    file_info_bin: file_info_bin.into(),
                    opts_bin: opts_bin.into(),
                });

                let response = client.update_metadata(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn read_version(
        &self,
        _org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<FileInfo> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            version_id,
            op = "read_version",
            state = "started",
            "Remote disk RPC started"
        );
        let opts_str = serde_json::to_string(opts)?;
        let opts_bin = encode_msgpack(opts)?;

        self.execute_with_timeout(
            move || async {
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ReadVersionRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                    version_id: version_id.to_string(),
                    opts: opts_str,
                    opts_bin: opts_bin.into(),
                });

                let response = client.read_version(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let file_info = decode_msgpack_or_json::<FileInfo>(&response.file_info_bin, &response.file_info)?;

                Ok(file_info)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self, req))]
    async fn batch_read_version(&self, req: BatchReadVersionReq) -> Result<Vec<BatchReadVersionResp>> {
        validate_batch_read_version_item_count(req.items.len())?;

        let mode = batch_metadata_rpc_mode();
        if !mode.should_attempt() {
            record_batch_read_version_gate_decision(mode, BATCH_READ_VERSION_GATE_OFF_UNARY);
            return batch_read_version_one_by_one(self, req).await;
        }
        record_batch_read_version_gate_decision(mode, BATCH_READ_VERSION_GATE_ATTEMPT);

        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            item_count = req.items.len(),
            batch_metadata_rpc_mode = mode.as_str(),
            op = "batch_read_version",
            state = "started",
            "Remote disk RPC started"
        );
        let batch_read_version_req = serde_json::to_string(&req)?;
        let batch_read_version_req_bin = encode_msgpack(&req)?;

        let batch_result = self
            .execute_with_timeout_for_op(
                "batch_read_version",
                move || async move {
                    let disk = self.disk_ref().await;
                    let mut client = self
                        .get_client()
                        .await
                        .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                    let request = Request::new(BatchReadVersionRequest {
                        disk,
                        batch_read_version_req,
                        batch_read_version_req_bin: batch_read_version_req_bin.into(),
                    });

                    let response = match client.batch_read_version(request).await {
                        Ok(response) => response.into_inner(),
                        Err(status) if status.code() == Code::Unimplemented => {
                            if mode.should_fallback_on_unimplemented() {
                                record_batch_read_version_gate_decision(mode, BATCH_READ_VERSION_GATE_FALLBACK_UNIMPLEMENTED);
                                warn!(
                                    event = EVENT_REMOTE_DISK_RPC,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                                    endpoint = %self.endpoint,
                                    batch_metadata_rpc_mode = mode.as_str(),
                                    op = "batch_read_version",
                                    state = "fallback_unimplemented",
                                    "Remote disk BatchReadVersion unsupported; falling back to unary read_version"
                                );
                                return Ok(None);
                            }

                            record_batch_read_version_gate_decision(mode, BATCH_READ_VERSION_GATE_UNSUPPORTED_NO_FALLBACK);
                            warn!(
                                event = EVENT_REMOTE_DISK_RPC,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                                endpoint = %self.endpoint,
                                batch_metadata_rpc_mode = mode.as_str(),
                                op = "batch_read_version",
                                state = "unsupported_no_fallback",
                                "Remote disk BatchReadVersion unsupported and explicit batch RPC mode forbids fallback"
                            );
                            return Err(Error::from(status));
                        }
                        Err(status) => return Err(Error::from(status)),
                    };

                    if !response.success {
                        return Err(response.error.unwrap_or_default().into());
                    }

                    decode_batch_read_version_response_items(response, &self.endpoint).map(Some)
                },
                get_max_timeout_duration(),
            )
            .await?;

        match batch_result {
            Some(batch_read_version_resps) => Ok(batch_read_version_resps),
            // Run the unary fallback outside the batch RPC deadline so each
            // read_version keeps its own per-op timeout and health accounting
            // instead of racing the whole batch against one drive timeout.
            None => batch_read_version_one_by_one(self, req).await,
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            read_data,
            op = "read_xl",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ReadXlRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                    read_data,
                });

                let response = client.read_xl(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let raw_file_info = decode_msgpack_or_json::<RawFileInfo>(&response.raw_file_info_bin, &response.raw_file_info)?;

                Ok(raw_file_info)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            src_volume,
            src_path,
            dst_volume,
            dst_path,
            op = "rename_data",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout_for_op(
            "rename_data",
            || async {
                let file_info = serde_json::to_string(&fi)?;
                let file_info_bin = encode_msgpack_named(&fi)?;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(RenameDataRequest {
                    disk: self.endpoint.to_string(),
                    src_volume: src_volume.to_string(),
                    src_path: src_path.to_string(),
                    file_info,
                    dst_volume: dst_volume.to_string(),
                    dst_path: dst_path.to_string(),
                    file_info_bin: file_info_bin.into(),
                });

                let response = client.rename_data(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let rename_data_resp =
                    decode_msgpack_or_json::<RenameDataResp>(&response.rename_data_resp_bin, &response.rename_data_resp)?;

                Ok(rename_data_resp)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn list_dir(&self, _origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>> {
        debug!("list_dir {}/{}", volume, dir_path);

        self.execute_with_timeout(
            || async {
                let disk = self.disk_ref().await;

                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ListDirRequest {
                    disk,
                    volume: volume.to_string(),
                    dir_path: dir_path.to_string(),
                    count,
                });

                let response = client.list_dir(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(response.volumes)
            },
            get_drive_list_dir_timeout(),
        )
        .await
    }

    #[tracing::instrument(skip(self, wr))]
    async fn walk_dir<W: AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            bucket = %opts.bucket,
            base_dir = %opts.base_dir,
            op = "walk_dir",
            state = "started",
            "Remote disk RPC started"
        );

        let disk = self.disk_ref().await;
        let body = serde_json::to_vec(&opts)?;
        let stall_timeout = get_drive_walkdir_stall_timeout();
        let bucket = opts.bucket.clone();
        let base_dir = opts.base_dir.clone();
        let disk_for_log = disk.clone();
        let timeout_duration = if opts.skip_total_timeout {
            Duration::ZERO
        } else {
            get_drive_walkdir_timeout()
        };

        self.execute_with_timeout_for_op_and_health_action(
            "walk_dir",
            || async {
                let mut last_err = None;

                for attempt in 1..=2 {
                    let mut reader = match self
                        .data_transport
                        .open_walk_dir(WalkDirStreamRequest {
                            endpoint: self.endpoint.grid_host(),
                            disk: disk.clone(),
                            body: body.clone(),
                            stall_timeout: Some(stall_timeout),
                        })
                        .await
                    {
                        Ok(reader) => reader,
                        Err(err) => {
                            if attempt == 1 && Self::is_retryable_walk_dir_error(&err) {
                                warn!(
                                    endpoint = %self.endpoint,
                                    addr = %self.addr,
                                    disk = %disk_for_log,
                                    bucket = %bucket,
                                    base_dir = %base_dir,
                                    attempt,
                                    stall_timeout_ms = stall_timeout.as_millis(),
                                    error = %err,
                                    "remote walk_dir returned retryable transport error; retrying"
                                );
                                last_err = Some(err);
                                continue;
                            }

                            return Err(err);
                        }
                    };

                    match copy_stream_with_buffer(&mut reader, wr, DEFAULT_READ_BUFFER_SIZE).await {
                        Ok(_) => return Ok(()),
                        Err(io_err) => return Err(DiskError::Io(io_err)),
                    }
                }

                Err(last_err.unwrap_or_else(|| DiskError::other("walk_dir retry exhausted without captured error")))
            },
            timeout_duration,
            FailureHealthAction::IgnoreFailure,
        )
        .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        self.read_file_stream(volume, path, 0, 0).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader> {
        // warn!(
        //     "disk remote read_file_stream {}/{}/{} offset={} length={}",
        //     self.endpoint.to_string(),
        //     volume,
        //     path,
        //     offset,
        //     length
        // );

        if self.health.is_faulty() {
            return Err(DiskError::FaultyDisk);
        }
        let disk = self.disk_ref().await;
        let stall_timeout = get_object_disk_read_timeout();
        self.data_transport
            .open_read(ReadStreamRequest {
                endpoint: self.endpoint.grid_host(),
                disk,
                volume: volume.to_string(),
                path: path.to_string(),
                offset,
                length,
                stall_timeout: (!stall_timeout.is_zero()).then_some(stall_timeout),
            })
            .await
    }

    /// Buffered read for remote disks.
    /// The transport stream is collected into owned Bytes for caller sharing.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file_mmap_copy(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<Bytes> {
        // For remote disks, use the regular reader and read into Bytes
        let reader = self.read_file_stream(volume, path, offset, length).await?;

        use tokio::io::AsyncReadExt;
        let mut reader = reader;

        // Read all data into Bytes (single allocation)
        let mut buffer = Vec::with_capacity(length);
        reader.read_to_end(&mut buffer).await?;

        Ok(Bytes::from(buffer))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "append_file",
            state = "started",
            "Remote disk RPC started"
        );

        if self.health.is_faulty() {
            return Err(DiskError::FaultyDisk);
        }
        let disk = self.disk_ref().await;
        self.open_write_with_retry(WriteStreamRequest {
            endpoint: self.endpoint.grid_host(),
            disk,
            volume: volume.to_string(),
            path: path.to_string(),
            append: true,
            size: 0,
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_file(&self, _origvolume: &str, volume: &str, path: &str, file_size: i64) -> Result<FileWriter> {
        // warn!(
        //     "disk remote create_file {}/{}/{} file_size={}",
        //     self.endpoint.to_string(),
        //     volume,
        //     path,
        //     file_size
        // );

        if self.health.is_faulty() {
            return Err(DiskError::FaultyDisk);
        }
        let disk = self.disk_ref().await;
        self.open_write_with_retry(WriteStreamRequest {
            endpoint: self.endpoint.grid_host(),
            disk,
            volume: volume.to_string(),
            path: path.to_string(),
            append: false,
            size: file_size,
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            src_volume,
            src_path,
            dst_volume,
            dst_path,
            op = "rename_file",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(RenameFileRequest {
                    disk: self.endpoint.to_string(),
                    src_volume: src_volume.to_string(),
                    src_path: src_path.to_string(),
                    dst_volume: dst_volume.to_string(),
                    dst_path: dst_path.to_string(),
                });

                let response = client.rename_file(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Bytes) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            src_volume,
            src_path,
            dst_volume,
            dst_path,
            op = "rename_part",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(RenamePartRequest {
                    disk: self.endpoint.to_string(),
                    src_volume: src_volume.to_string(),
                    src_path: src_path.to_string(),
                    dst_volume: dst_volume.to_string(),
                    dst_path: dst_path.to_string(),
                    meta,
                });

                let response = client.rename_part(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            recursive = opt.recursive,
            immediate = opt.immediate,
            op = "delete",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let options = serde_json::to_string(&opt)?;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(DeleteRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    options,
                });

                let response = client.delete(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "verify_file",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let file_info = serde_json::to_string(&fi)?;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(VerifyFileRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    file_info,
                });

                let response = client.verify_file(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let check_parts_resp = serde_json::from_str::<CheckPartsResp>(&response.check_parts_resp)?;

                Ok(check_parts_resp)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn read_parts(&self, bucket: &str, paths: &[String]) -> Result<Vec<ObjectPartInfo>> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            bucket,
            path_count = paths.len(),
            op = "read_parts",
            state = "started",
            "Remote disk RPC started"
        );
        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ReadPartsRequest {
                    disk: self.endpoint.to_string(),
                    bucket: bucket.to_string(),
                    paths: paths.to_vec(),
                });

                let response = client.read_parts(request).await?.into_inner();
                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let read_parts_resp = rmp_serde::from_slice::<Vec<ObjectPartInfo>>(&response.object_part_infos)?;

                Ok(read_parts_resp)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "check_parts",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let file_info = serde_json::to_string(&fi)?;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(CheckPartsRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    file_info,
                });

                let response = client.check_parts(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let check_parts_resp = serde_json::from_str::<CheckPartsResp>(&response.check_parts_resp)?;

                Ok(check_parts_resp)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            bucket = %req.bucket,
            prefix = %req.prefix,
            op = "read_multiple",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let read_multiple_req = serde_json::to_string(&req)?;
                let read_multiple_req_bin = encode_msgpack(&req)?;
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ReadMultipleRequest {
                    disk,
                    read_multiple_req,
                    read_multiple_req_bin: read_multiple_req_bin.into(),
                });

                let response = client.read_multiple(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let read_multiple_resps = decode_read_multiple_response_items(response, &self.endpoint)?;

                Ok(read_multiple_resps)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            bytes = data.len(),
            op = "write_all",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let data_len = data.len();
                let disk = self.disk_ref().await;
                let mut client = self.get_client().await.map_err(|err| {
                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_write_all_error();
                    Error::other(format!("can not get client, err: {err}"))
                })?;
                let request = Request::new(WriteAllRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                    data,
                });

                crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_write_all_request();
                let response = match client.write_all(request).await {
                    Ok(response) => response.into_inner(),
                    Err(err) => {
                        crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_write_all_error();
                        return Err(err.into());
                    }
                };

                crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_write_all_sent_bytes(data_len);

                if !response.success {
                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_write_all_error();
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        debug!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "read_all",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let disk = self.disk_ref().await;
                let mut client = self.get_client().await.map_err(|err| {
                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_all_error();
                    Error::other(format!("can not get client, err: {err}"))
                })?;
                let request = Request::new(ReadAllRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                });

                crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_all_request();
                let response = match client.read_all(request).await {
                    Ok(response) => response.into_inner(),
                    Err(err) => {
                        crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_all_error();
                        return Err(err.into());
                    }
                };

                if !response.success {
                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_all_error();
                    return Err(response.error.unwrap_or_default().into());
                }

                crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_all_recv_bytes(response.data.len());
                Ok(response.data)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn disk_info(&self, opts: &DiskInfoOptions) -> Result<DiskInfo> {
        self.execute_with_timeout_for_op(
            "disk_info",
            || async {
                let opts = serde_json::to_string(&opts)?;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(DiskInfoRequest {
                    disk: self.endpoint.to_string(),
                    opts,
                });

                let response = client.disk_info(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let disk_info = serde_json::from_str::<DiskInfo>(&response.disk_info)?;

                Ok(disk_info)
            },
            get_drive_disk_info_timeout(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    fn start_scan(&self) -> ScanGuard {
        self.scanning.fetch_add(1, Ordering::Relaxed);
        ScanGuard(Arc::clone(&self.scanning))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::rpc::internode_data_transport::{InternodeDataTransportCapabilities, TcpHttpInternodeDataTransport};
    use crate::runtime::sources as runtime_sources;
    use serde_json::Value;
    use std::io::{self as std_io, Write};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex, Mutex as StdMutex, Once};
    use std::task::{Context, Poll};
    use tokio::io::{ReadBuf, duplex};
    use tokio::net::TcpListener;
    use tonic::transport::Endpoint as TonicEndpoint;
    use tracing::Level;
    use tracing_subscriber::{Registry, fmt::MakeWriter, layer::SubscriberExt};
    use uuid::Uuid;

    static INIT: Once = Once::new();

    #[derive(Clone, Default)]
    struct CapturedLogs {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    struct CapturedLogWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        fn lines(&self) -> Vec<Value> {
            let buffer = self
                .buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .clone();
            String::from_utf8(buffer)
                .expect("captured logs should be valid UTF-8")
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).expect("captured log line should be valid JSON"))
                .collect()
        }
    }

    impl Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> std_io::Result<usize> {
            self.buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std_io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    #[derive(Debug, Clone)]
    enum RecordedTransportCall {
        Read(ReadStreamRequest),
        Write(WriteStreamRequest),
        WalkDir(WalkDirStreamRequest),
    }

    #[derive(Debug, Clone, Default)]
    struct RecordingInternodeDataTransport {
        calls: Arc<StdMutex<Vec<RecordedTransportCall>>>,
    }

    impl RecordingInternodeDataTransport {
        fn calls(&self) -> Vec<RecordedTransportCall> {
            self.calls.lock().expect("recorded transport calls lock poisoned").clone()
        }

        fn record(&self, call: RecordedTransportCall) {
            self.calls.lock().expect("recorded transport calls lock poisoned").push(call);
        }
    }

    #[derive(Debug)]
    enum WalkDirTestStep {
        Error(DiskError),
        Data(Vec<u8>),
        PartialDataThenError { data: Vec<u8>, error: io::Error },
    }

    #[derive(Debug, Clone, Default)]
    struct RetryingWalkDirInternodeDataTransport {
        calls: Arc<StdMutex<Vec<RecordedTransportCall>>>,
        steps: Arc<StdMutex<Vec<WalkDirTestStep>>>,
    }

    impl RetryingWalkDirInternodeDataTransport {
        fn with_steps(steps: Vec<WalkDirTestStep>) -> Self {
            Self {
                calls: Arc::new(StdMutex::new(Vec::new())),
                steps: Arc::new(StdMutex::new(steps)),
            }
        }

        fn calls(&self) -> Vec<RecordedTransportCall> {
            self.calls.lock().expect("recorded transport calls lock poisoned").clone()
        }

        fn record(&self, call: RecordedTransportCall) {
            self.calls.lock().expect("recorded transport calls lock poisoned").push(call);
        }
    }

    #[derive(Debug, Clone)]
    enum OpenWriteTestStep {
        Error(DiskError),
        Success,
    }

    #[derive(Debug, Clone, Default)]
    struct RetryingOpenWriteInternodeDataTransport {
        calls: Arc<StdMutex<Vec<RecordedTransportCall>>>,
        steps: Arc<StdMutex<Vec<OpenWriteTestStep>>>,
    }

    impl RetryingOpenWriteInternodeDataTransport {
        fn with_steps(steps: Vec<OpenWriteTestStep>) -> Self {
            Self {
                calls: Arc::new(StdMutex::new(Vec::new())),
                steps: Arc::new(StdMutex::new(steps)),
            }
        }

        fn calls(&self) -> Vec<RecordedTransportCall> {
            self.calls.lock().expect("recorded transport calls lock poisoned").clone()
        }

        fn record(&self, call: RecordedTransportCall) {
            self.calls.lock().expect("recorded transport calls lock poisoned").push(call);
        }
    }

    #[derive(Debug, Default)]
    struct EmptyTestReader;

    impl AsyncRead for EmptyTestReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn sample_rename_data_file_info() -> FileInfo {
        FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(Uuid::new_v4()),
            data_dir: Some(Uuid::new_v4()),
            size: 64 * 1024,
            mod_time: Some(::time::OffsetDateTime::UNIX_EPOCH + ::time::Duration::seconds(1)),
            metadata: [
                ("etag".to_string(), "etag-value".to_string()),
                ("content-type".to_string(), "application/octet-stream".to_string()),
            ]
            .into_iter()
            .collect(),
            erasure: rustfs_filemeta::ErasureInfo {
                algorithm: rustfs_filemeta::ERASURE_ALGORITHM.to_string(),
                data_blocks: 4,
                parity_blocks: 2,
                block_size: 1024 * 1024,
                index: 1,
                distribution: vec![1, 2, 3, 4, 5, 6],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn sample_read_multiple_resp(file: &str, data: &[u8]) -> ReadMultipleResp {
        ReadMultipleResp {
            bucket: "bucket".to_string(),
            prefix: "prefix".to_string(),
            file: file.to_string(),
            exists: true,
            data: data.to_vec(),
            ..Default::default()
        }
    }

    fn sample_remote_endpoint() -> Endpoint {
        Endpoint {
            url: url::Url::parse("http://server:9000/disk-a").expect("endpoint URL should parse"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        }
    }

    #[test]
    fn read_multiple_response_decode_prefers_msgpack_payloads() {
        let endpoint = sample_remote_endpoint();
        let msgpack_resp = sample_read_multiple_resp("msgpack", b"binary");
        let json_resp = sample_read_multiple_resp("json", b"fallback");
        let response = ReadMultipleResponse {
            success: true,
            read_multiple_resps: vec![serde_json::to_string(&json_resp).expect("json fallback should encode")],
            read_multiple_resps_bin: vec![encode_msgpack(&msgpack_resp).expect("msgpack response should encode").into()],
            error: None,
        };

        let decoded = decode_read_multiple_response_items(response, &endpoint).expect("msgpack response should decode");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].file, "msgpack");
        assert_eq!(decoded[0].data, b"binary");
    }

    #[test]
    fn read_multiple_response_decode_falls_back_to_json_payloads() {
        let endpoint = sample_remote_endpoint();
        let json_resp = sample_read_multiple_resp("json", b"fallback");
        let response = ReadMultipleResponse {
            success: true,
            read_multiple_resps: vec![serde_json::to_string(&json_resp).expect("json fallback should encode")],
            read_multiple_resps_bin: Vec::new(),
            error: None,
        };

        let decoded = decode_read_multiple_response_items(response, &endpoint).expect("json response should decode");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].file, "json");
        assert_eq!(decoded[0].data, b"fallback");
    }

    #[test]
    fn read_multiple_response_decode_reports_corrupt_msgpack_item() {
        let endpoint = sample_remote_endpoint();
        let response = ReadMultipleResponse {
            success: true,
            read_multiple_resps: Vec::new(),
            read_multiple_resps_bin: vec![
                encode_msgpack(&sample_read_multiple_resp("ok", b"data"))
                    .expect("msgpack response should encode")
                    .into(),
                bytes::Bytes::from_static(b"not-msgpack"),
            ],
            error: None,
        };

        let err = decode_read_multiple_response_items(response, &endpoint).expect_err("corrupt msgpack item should fail");
        let err = err.to_string();

        assert!(err.contains("ReadMultipleResp msgpack item 1"), "unexpected error: {err}");
        assert!(err.contains("server:9000"), "unexpected error: {err}");
    }

    fn sample_batch_read_version_resp(index: usize, path: &str, success: bool) -> BatchReadVersionResp {
        BatchReadVersionResp {
            index,
            path: path.to_string(),
            version_id: "version-a".to_string(),
            success,
            error: if success {
                String::new()
            } else {
                "file version not found".to_string()
            },
            ..Default::default()
        }
    }

    #[test]
    fn batch_read_version_response_decode_prefers_msgpack_payloads() {
        let endpoint = sample_remote_endpoint();
        let msgpack_resp = sample_batch_read_version_resp(7, "msgpack-object", true);
        let json_resp = sample_batch_read_version_resp(1, "json-object", false);
        let response = BatchReadVersionResponse {
            success: true,
            batch_read_version_resps: vec![serde_json::to_string(&json_resp).expect("json fallback should encode")],
            batch_read_version_resps_bin: vec![encode_msgpack(&msgpack_resp).expect("msgpack response should encode").into()],
            error: None,
        };

        let decoded = decode_batch_read_version_response_items(response, &endpoint).expect("msgpack response should decode");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].index, 7);
        assert_eq!(decoded[0].path, "msgpack-object");
        assert!(decoded[0].success);
    }

    #[test]
    fn batch_read_version_response_decode_reports_corrupt_msgpack_item() {
        let endpoint = sample_remote_endpoint();
        let response = BatchReadVersionResponse {
            success: true,
            batch_read_version_resps: Vec::new(),
            batch_read_version_resps_bin: vec![
                encode_msgpack(&sample_batch_read_version_resp(0, "ok", true))
                    .expect("msgpack response should encode")
                    .into(),
                bytes::Bytes::from_static(b"not-msgpack"),
            ],
            error: None,
        };

        let err = decode_batch_read_version_response_items(response, &endpoint)
            .expect_err("corrupt msgpack item should fail")
            .to_string();

        assert!(err.contains("BatchReadVersionResp msgpack item 1"), "unexpected error: {err}");
        assert!(err.contains("server:9000"), "unexpected error: {err}");
    }

    #[test]
    fn batch_metadata_rpc_mode_defaults_to_off_and_parses_supported_values() {
        assert_eq!(parse_batch_metadata_rpc_mode(""), BatchMetadataRpcMode::Off);
        assert_eq!(parse_batch_metadata_rpc_mode("off"), BatchMetadataRpcMode::Off);
        assert_eq!(parse_batch_metadata_rpc_mode("auto"), BatchMetadataRpcMode::Auto);
        assert_eq!(parse_batch_metadata_rpc_mode("on"), BatchMetadataRpcMode::On);
        assert_eq!(parse_batch_metadata_rpc_mode("unknown"), BatchMetadataRpcMode::Off);
        assert_eq!(BatchMetadataRpcMode::Off.as_str(), "off");
        assert_eq!(BatchMetadataRpcMode::Auto.as_str(), "auto");
        assert_eq!(BatchMetadataRpcMode::On.as_str(), "on");
        assert!(!BatchMetadataRpcMode::Off.should_attempt());
        assert!(BatchMetadataRpcMode::Auto.should_attempt());
        assert!(BatchMetadataRpcMode::On.should_attempt());
        assert_eq!(BATCH_READ_VERSION_GATE_ATTEMPT, "attempt");
        assert_eq!(BATCH_READ_VERSION_GATE_OFF_UNARY, "off_unary");
        assert_eq!(BATCH_READ_VERSION_GATE_FALLBACK_UNIMPLEMENTED, "fallback_unimplemented");
        assert_eq!(BATCH_READ_VERSION_GATE_UNSUPPORTED_NO_FALLBACK, "unsupported_no_fallback");
    }

    #[test]
    fn batch_metadata_rpc_mode_uses_documented_env_before_legacy_alias() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_METADATA_BATCH_READ, Some("auto")),
                (LEGACY_ENV_RUSTFS_BATCH_METADATA_RPC, Some("on")),
            ],
            || {
                assert_eq!(batch_metadata_rpc_mode_from_env(), BatchMetadataRpcMode::Auto);
            },
        );
    }

    #[test]
    fn batch_metadata_rpc_mode_falls_back_to_legacy_env_alias() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_METADATA_BATCH_READ, None::<&str>),
                (LEGACY_ENV_RUSTFS_BATCH_METADATA_RPC, Some("on")),
            ],
            || {
                assert_eq!(batch_metadata_rpc_mode_from_env(), BatchMetadataRpcMode::On);
            },
        );
    }

    #[test]
    fn batch_metadata_rpc_mode_only_auto_falls_back_on_unimplemented() {
        assert!(!BatchMetadataRpcMode::Off.should_fallback_on_unimplemented());
        assert!(BatchMetadataRpcMode::Auto.should_fallback_on_unimplemented());
        assert!(!BatchMetadataRpcMode::On.should_fallback_on_unimplemented());
    }

    #[test]
    fn rename_data_file_info_named_msgpack_is_smaller_than_json() {
        let file_info = sample_rename_data_file_info();
        let json = serde_json::to_vec(&file_info).expect("file info json should encode");
        let named_msgpack = encode_msgpack_named(&file_info).expect("file info named msgpack should encode");

        assert!(
            named_msgpack.len() < json.len(),
            "expected named msgpack payload to be smaller than json (msgpack={}, json={})",
            named_msgpack.len(),
            json.len()
        );
    }

    #[test]
    fn rename_data_resp_named_msgpack_is_smaller_than_json() {
        let response = RenameDataResp {
            old_data_dir: Some(Uuid::new_v4()),
            sign: Some(vec![1_u8; 32]),
        };
        let json = serde_json::to_vec(&response).expect("rename data response json should encode");
        let named_msgpack = encode_msgpack_named(&response).expect("rename data response named msgpack should encode");

        assert!(
            named_msgpack.len() < json.len(),
            "expected named msgpack payload to be smaller than json (msgpack={}, json={})",
            named_msgpack.len(),
            json.len()
        );
    }

    #[derive(Debug, Default)]
    struct SinkTestWriter;

    impl AsyncWrite for SinkTestWriter {
        fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[async_trait::async_trait]
    impl InternodeDataTransport for RecordingInternodeDataTransport {
        async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
            self.record(RecordedTransportCall::Read(request));
            Ok(Box::new(EmptyTestReader))
        }

        async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter> {
            self.record(RecordedTransportCall::Write(request));
            Ok(Box::new(SinkTestWriter))
        }

        async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader> {
            self.record(RecordedTransportCall::WalkDir(request));
            Ok(Box::new(EmptyTestReader))
        }

        fn name(&self) -> &'static str {
            "recording"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    #[async_trait::async_trait]
    impl InternodeDataTransport for RetryingWalkDirInternodeDataTransport {
        async fn open_read(&self, _request: ReadStreamRequest) -> Result<FileReader> {
            panic!("open_read should not be used in walk_dir retry test");
        }

        async fn open_write(&self, _request: WriteStreamRequest) -> Result<FileWriter> {
            panic!("open_write should not be used in walk_dir retry test");
        }

        async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader> {
            self.record(RecordedTransportCall::WalkDir(request));
            let step = self.steps.lock().expect("walk_dir retry steps lock poisoned").remove(0);
            match step {
                WalkDirTestStep::Error(err) => Err(err),
                WalkDirTestStep::Data(data) => Ok(Box::new(Cursor::new(data))),
                WalkDirTestStep::PartialDataThenError { data, error } => Ok(Box::new(PartialThenErrorReader {
                    cursor: Cursor::new(data),
                    error: Some(error),
                })),
            }
        }

        fn name(&self) -> &'static str {
            "retrying-walk-dir"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    #[async_trait::async_trait]
    impl InternodeDataTransport for RetryingOpenWriteInternodeDataTransport {
        async fn open_read(&self, _request: ReadStreamRequest) -> Result<FileReader> {
            panic!("open_read should not be used in open_write retry test");
        }

        async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter> {
            self.record(RecordedTransportCall::Write(request));
            let step = self.steps.lock().expect("open_write retry steps lock poisoned").remove(0);
            match step {
                OpenWriteTestStep::Error(err) => Err(err),
                OpenWriteTestStep::Success => Ok(Box::new(SinkTestWriter)),
            }
        }

        async fn open_walk_dir(&self, _request: WalkDirStreamRequest) -> Result<FileReader> {
            panic!("open_walk_dir should not be used in open_write retry test");
        }

        fn name(&self) -> &'static str {
            "retrying-open-write"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    async fn new_remote_disk_with_transport(data_transport: Arc<dyn InternodeDataTransport>) -> RemoteDisk {
        let endpoint = Endpoint {
            url: url::Url::parse("http://remote-node:9000/data/rustfs0").expect("operation should succeed"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        RemoteDisk::new(&endpoint, &disk_option, data_transport)
            .await
            .expect("operation should succeed")
    }

    #[derive(Debug)]
    struct PartialThenErrorReader {
        cursor: Cursor<Vec<u8>>,
        error: Option<io::Error>,
    }

    impl AsyncRead for PartialThenErrorReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let filled_before = buf.filled().len();
            match Pin::new(&mut self.cursor).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    if buf.filled().len() > filled_before {
                        return Poll::Ready(Ok(()));
                    }

                    if let Some(err) = self.error.take() {
                        return Poll::Ready(Err(err));
                    }

                    Poll::Ready(Ok(()))
                }
                other => other,
            }
        }
    }

    fn init_tracing(filter_level: Level) {
        INIT.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_max_level(filter_level)
                .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                .with_thread_names(true)
                .try_init();
        });
    }

    #[tokio::test]
    async fn test_remote_disk_creation() {
        let url = url::Url::parse("http://example.com:9000/path").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 1,
            disk_idx: 2,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");

        assert!(!remote_disk.is_local());
        assert_eq!(remote_disk.endpoint.url, url);
        assert_eq!(remote_disk.endpoint.pool_idx, 0);
        assert_eq!(remote_disk.endpoint.set_idx, 1);
        assert_eq!(remote_disk.endpoint.disk_idx, 2);
        assert_eq!(remote_disk.host_name(), "example.com:9000");
    }

    #[tokio::test]
    async fn test_remote_disk_basic_properties() {
        let url = url::Url::parse("http://remote-server:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: -1,
            set_idx: -1,
            disk_idx: -1,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");

        // Test basic properties
        assert!(!remote_disk.is_local());
        assert_eq!(remote_disk.host_name(), "remote-server:9000");
        assert!(remote_disk.to_string().contains("remote-server"));
        assert!(remote_disk.to_string().contains("9000"));

        // Test disk location
        let location = remote_disk.get_disk_location();
        assert_eq!(location.pool_idx, None);
        assert_eq!(location.set_idx, None);
        assert_eq!(location.disk_idx, None);
        assert!(!location.valid()); // None values make it invalid
    }

    #[tokio::test]
    async fn test_remote_disk_path() {
        let url = url::Url::parse("http://remote-server:9000/storage").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");
        let path = remote_disk.path();

        // Remote disk path should be based on the URL path
        assert!(path.to_string_lossy().contains("storage"));
    }

    #[tokio::test]
    async fn test_remote_disk_is_online_detects_active_listener() {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");

        let url =
            url::Url::parse(&format!("http://{}:{}/data/rustfs0", addr.ip(), addr.port())).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");
        assert!(remote_disk.is_online().await);

        drop(listener);
    }

    #[tokio::test]
    async fn test_remote_disk_is_online_detects_missing_listener() {
        init_tracing(Level::ERROR);

        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");
        let ip = addr.ip();
        let port = addr.port();

        drop(listener);

        let url = url::Url::parse(&format!("http://{ip}:{port}/data/rustfs0")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_DRIVE_ACTIVE_CHECK_INTERVAL_SECS, Some("1")),
                (rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS, Some("1")),
            ],
            async {
                let disk_option = DiskOption {
                    cleanup: false,
                    health_check: true,
                };

                let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
                    .await
                    .expect("operation should succeed");
                remote_disk.enable_health_check();

                // Wait out the initial success-grace window so the active probe loop
                // actually attempts a connectivity check. Under the new
                // suspect-first semantics we only need to prove that the drive
                // transitions away from a clean Online state at least once.
                tokio::time::sleep(SKIP_IF_SUCCESS_BEFORE + Duration::from_secs(2)).await;
                assert!(
                    remote_disk.offline_duration_secs().is_some(),
                    "missing listener should transition the drive through suspect/offline tracking"
                );
                assert_ne!(
                    remote_disk.runtime_state(),
                    RuntimeDriveHealthState::Online,
                    "missing listener should not remain in a clean Online state after probing"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_remote_disk_recovery_requires_disk_rpc_readiness() {
        init_tracing(Level::ERROR);
        runtime_sources::ensure_test_rpc_secret();

        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");
        let accept_task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });

        let base_addr = format!("http://{}:{}", addr.ip(), addr.port());
        let url = url::Url::parse(&format!("{base_addr}/data/rustfs0")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let health = Arc::new(DiskHealthTracker::new());
        health.mark_failure(&endpoint, "test_failure");
        health.mark_failure(&endpoint, "test_failure");
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Offline);
        let channel = TonicEndpoint::from_shared(base_addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(base_addr.clone(), channel).await;
        assert!(runtime_sources::test_node_channel_is_cached(&base_addr).await);

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_DRIVE_RETURNING_PROBE_INTERVAL_SECS, Some("1")),
                (rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS, Some("1")),
            ],
            async {
                let cancel_token = CancellationToken::new();
                let monitor = tokio::spawn(RemoteDisk::monitor_remote_disk_recovery(
                    base_addr.clone(),
                    endpoint,
                    Arc::clone(&health),
                    cancel_token.clone(),
                ));

                tokio::time::sleep(Duration::from_millis(2_500)).await;
                cancel_token.cancel();
                let _ = monitor.await;

                assert_ne!(
                    health.runtime_state(),
                    RuntimeDriveHealthState::Online,
                    "a plain TCP listener without disk_info RPC readiness must not restore the remote disk online"
                );
                assert!(
                    !runtime_sources::test_node_channel_is_cached(&base_addr).await,
                    "failed recovery probes should evict stale cached gRPC channels"
                );
            },
        )
        .await;

        accept_task.abort();
    }

    #[tokio::test]
    async fn test_copy_stream_with_buffer_copies_full_payload() {
        let payload = b"walk-dir-stream".repeat(1024);
        let expected = payload.clone();
        let (mut write_half, mut read_half) = duplex(128);

        let copy_task = tokio::spawn(async move {
            let mut cursor = Cursor::new(payload);
            copy_stream_with_buffer(&mut cursor, &mut write_half, 4 * 1024)
                .await
                .expect("operation should succeed");
        });

        let mut copied = Vec::new();
        read_half.read_to_end(&mut copied).await.expect("operation should succeed");
        copy_task.await.expect("operation should succeed");

        assert_eq!(copied, expected);
    }

    #[tokio::test]
    async fn test_remote_disk_disk_id() {
        let url = url::Url::parse("http://remote-server:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");

        // Initially, disk ID should be None
        let initial_id = remote_disk.get_disk_id().await.expect("operation should succeed");
        assert!(initial_id.is_none());

        // Set a disk ID
        let test_id = Uuid::new_v4();
        remote_disk
            .set_disk_id(Some(test_id))
            .await
            .expect("operation should succeed");

        // Verify the disk ID was set
        let retrieved_id = remote_disk.get_disk_id().await.expect("operation should succeed");
        assert_eq!(retrieved_id, Some(test_id));

        // Clear the disk ID
        remote_disk.set_disk_id(None).await.expect("operation should succeed");
        let cleared_id = remote_disk.get_disk_id().await.expect("operation should succeed");
        assert!(cleared_id.is_none());
    }

    #[tokio::test]
    async fn test_remote_disk_ref_prefers_disk_id() {
        let url = url::Url::parse("http://remote-server:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");
        assert_eq!(remote_disk.disk_ref().await, endpoint.to_string());

        let disk_id = Uuid::new_v4();
        remote_disk
            .set_disk_id(Some(disk_id))
            .await
            .expect("operation should succeed");

        assert_eq!(remote_disk.disk_ref().await, disk_id.to_string());
    }

    #[tokio::test]
    async fn test_remote_disk_read_file_stream_uses_configured_data_transport() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT, None::<&str>)], async {
            let transport = RecordingInternodeDataTransport::default();
            let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
            let expected_disk = remote_disk.disk_ref().await;

            let _reader = remote_disk
                .read_file_stream("bucket", "object/part.1", 7, 11)
                .await
                .expect("operation should succeed");

            let calls = transport.calls();
            assert_eq!(calls.len(), 1);
            match &calls[0] {
                RecordedTransportCall::Read(request) => {
                    assert_eq!(request.endpoint, "http://remote-node:9000");
                    assert_eq!(request.disk, expected_disk);
                    assert_eq!(request.volume, "bucket");
                    assert_eq!(request.path, "object/part.1");
                    assert_eq!(request.offset, 7);
                    assert_eq!(request.length, 11);
                    assert_eq!(request.stall_timeout, Some(get_object_disk_read_timeout()));
                }
                other => panic!("expected read transport call, got {other:?}"),
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_remote_disk_read_file_stream_disables_stall_timeout_when_configured_zero() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT, Some("0"))], async {
            let transport = RecordingInternodeDataTransport::default();
            let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;

            let _reader = remote_disk
                .read_file_stream("bucket", "object/part.1", 7, 11)
                .await
                .expect("operation should succeed");

            let calls = transport.calls();
            assert_eq!(calls.len(), 1);
            match &calls[0] {
                RecordedTransportCall::Read(request) => assert_eq!(request.stall_timeout, None),
                other => panic!("expected read transport call, got {other:?}"),
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_remote_disk_create_and_append_file_use_configured_data_transport() {
        let transport = RecordingInternodeDataTransport::default();
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let expected_disk = remote_disk.disk_ref().await;

        let _created = remote_disk
            .create_file("orig-bucket", "bucket", "object/part.1", 4096)
            .await
            .expect("operation should succeed");
        let _appended = remote_disk
            .append_file("bucket", "object/part.2")
            .await
            .expect("operation should succeed");

        let calls = transport.calls();
        assert_eq!(calls.len(), 2);

        match &calls[0] {
            RecordedTransportCall::Write(request) => {
                assert_eq!(request.endpoint, "http://remote-node:9000");
                assert_eq!(request.disk, expected_disk);
                assert_eq!(request.volume, "bucket");
                assert_eq!(request.path, "object/part.1");
                assert!(!request.append);
                assert_eq!(request.size, 4096);
            }
            other => panic!("expected create write transport call, got {other:?}"),
        }

        match &calls[1] {
            RecordedTransportCall::Write(request) => {
                assert_eq!(request.endpoint, "http://remote-node:9000");
                assert_eq!(request.disk, expected_disk);
                assert_eq!(request.volume, "bucket");
                assert_eq!(request.path, "object/part.2");
                assert!(request.append);
                assert_eq!(request.size, 0);
            }
            other => panic!("expected append write transport call, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_remote_disk_create_file_retries_once_on_retryable_open_write_error() {
        let transport = RetryingOpenWriteInternodeDataTransport::with_steps(vec![
            OpenWriteTestStep::Error(DiskError::from(rustfs_rio::new_test_internode_http_io_error(
                rustfs_rio::InternodeHttpErrorKind::ConnectionReset,
            ))),
            OpenWriteTestStep::Success,
        ]);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        crate::cluster::rpc::runtime_sources::reset_internode_metrics_for_test();

        let _created = remote_disk
            .create_file("orig-bucket", "bucket", "object/part.1", 4096)
            .await
            .expect("retryable open_write error should recover");

        let calls = transport.calls();
        assert_eq!(calls.len(), 2, "create_file should retry exactly once");
        let snapshot = crate::cluster::rpc::runtime_sources::internode_metrics_snapshot_for_test();
        assert_eq!(snapshot.outgoing_requests_total, 0);
    }

    #[tokio::test]
    async fn test_remote_disk_append_file_does_not_retry_non_retryable_open_write_error() {
        let transport = RetryingOpenWriteInternodeDataTransport::with_steps(vec![OpenWriteTestStep::Error(DiskError::from(
            rustfs_rio::new_test_internode_http_io_error(rustfs_rio::InternodeHttpErrorKind::DnsResolutionFailed),
        ))]);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;

        let err = match remote_disk.append_file("bucket", "object/part.2").await {
            Ok(_) => panic!("non-retryable open_write error should be returned directly"),
            Err(err) => err,
        };

        assert_eq!(
            err.internode_http_error_kind(),
            Some(rustfs_rio::InternodeHttpErrorKind::DnsResolutionFailed)
        );
        assert_eq!(transport.calls().len(), 1, "append_file should not retry non-retryable errors");
    }

    #[tokio::test]
    async fn test_remote_disk_walk_dir_uses_configured_data_transport() {
        let transport = RecordingInternodeDataTransport::default();
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let expected_disk = remote_disk.disk_ref().await;
        let opts = WalkDirOptions {
            bucket: "bucket".to_string(),
            base_dir: "prefix".to_string(),
            recursive: true,
            report_notfound: false,
            filter_prefix: Some("part".to_string()),
            forward_to: None,
            limit: 10,
            disk_id: String::new(),
            ..Default::default()
        };
        let expected_body = serde_json::to_vec(&opts).expect("operation should succeed");
        let mut writer = Vec::new();

        remote_disk
            .walk_dir(opts, &mut writer)
            .await
            .expect("operation should succeed");

        let calls = transport.calls();
        assert_eq!(calls.len(), 1);
        match &calls[0] {
            RecordedTransportCall::WalkDir(request) => {
                assert_eq!(request.endpoint, "http://remote-node:9000");
                assert_eq!(request.disk, expected_disk);
                assert_eq!(request.body, expected_body);
                assert_eq!(request.stall_timeout, Some(get_drive_walkdir_stall_timeout()));
            }
            other => panic!("expected walk-dir transport call, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_remote_disk_walk_dir_preserves_skip_total_timeout_option() {
        let transport = RecordingInternodeDataTransport::default();
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let opts = WalkDirOptions {
            bucket: "bucket".to_string(),
            base_dir: "prefix".to_string(),
            recursive: true,
            skip_total_timeout: true,
            ..Default::default()
        };
        let mut writer = Vec::new();

        remote_disk
            .walk_dir(opts, &mut writer)
            .await
            .expect("walk_dir should be sent through configured data transport");

        let calls = transport.calls();
        assert_eq!(calls.len(), 1);
        match &calls[0] {
            RecordedTransportCall::WalkDir(request) => {
                let sent_opts: WalkDirOptions =
                    serde_json::from_slice(&request.body).expect("walk_dir request body should deserialize");
                assert!(sent_opts.skip_total_timeout);
                assert_eq!(request.stall_timeout, Some(get_drive_walkdir_stall_timeout()));
            }
            other => panic!("expected walk-dir transport call, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_remote_disk_walk_dir_retries_once_on_retryable_transport_error() {
        let transport = RetryingWalkDirInternodeDataTransport::with_steps(vec![
            WalkDirTestStep::Error(DiskError::other("HttpReader stream error: error decoding response body")),
            WalkDirTestStep::Data(b"walk-dir-retry-ok".to_vec()),
        ]);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let opts = WalkDirOptions {
            bucket: "bucket".to_string(),
            base_dir: "config/iam".to_string(),
            recursive: true,
            report_notfound: false,
            filter_prefix: None,
            forward_to: None,
            limit: 10,
            disk_id: String::new(),
            ..Default::default()
        };
        let mut writer = Vec::new();

        remote_disk
            .walk_dir(opts, &mut writer)
            .await
            .expect("retryable walk_dir error should recover");

        assert_eq!(writer, b"walk-dir-retry-ok");
        assert_eq!(transport.calls().len(), 2, "walk_dir should retry exactly once");
    }

    #[tokio::test]
    async fn test_remote_disk_walk_dir_does_not_retry_after_partial_stream_failure() {
        let transport = RetryingWalkDirInternodeDataTransport::with_steps(vec![
            WalkDirTestStep::PartialDataThenError {
                data: b"partial-walk-dir".to_vec(),
                error: io::Error::new(io::ErrorKind::ConnectionReset, "connection reset"),
            },
            WalkDirTestStep::Data(b"walk-dir-retry-ok".to_vec()),
        ]);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let opts = WalkDirOptions {
            bucket: "bucket".to_string(),
            base_dir: "config/iam".to_string(),
            recursive: true,
            report_notfound: false,
            filter_prefix: None,
            forward_to: None,
            limit: 10,
            disk_id: String::new(),
            ..Default::default()
        };
        let mut writer = Vec::new();

        let err = remote_disk
            .walk_dir(opts, &mut writer)
            .await
            .expect_err("partial stream failure should be returned without retry");

        assert!(matches!(err, DiskError::Io(ref io_err) if io_err.kind() == io::ErrorKind::ConnectionReset));
        assert_eq!(writer, b"partial-walk-dir");
        assert_eq!(transport.calls().len(), 1, "walk_dir should not retry after writing partial bytes");
    }

    #[tokio::test]
    async fn test_remote_disk_endpoints_with_different_schemes() {
        let test_cases = vec![
            ("http://server:9000", "server:9000"),
            ("https://secure-server:443", "secure-server"), // Default HTTPS port is omitted
            ("http://192.168.1.100:8080", "192.168.1.100:8080"),
            ("https://secure-server", "secure-server"), // No port specified
        ];

        for (url_str, expected_hostname) in test_cases {
            let url = url::Url::parse(url_str).expect("operation should succeed");
            let endpoint = Endpoint {
                url: url.clone(),
                is_local: false,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 0,
            };

            let disk_option = DiskOption {
                cleanup: false,
                health_check: false,
            };

            let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
                .await
                .expect("operation should succeed");

            assert!(!remote_disk.is_local());
            assert_eq!(remote_disk.host_name(), expected_hostname);
            // Note: to_string() might not contain the exact hostname format
            assert!(!remote_disk.to_string().is_empty());
        }
    }

    #[tokio::test]
    async fn test_remote_disk_location_validation() {
        // Test valid location
        let url = url::Url::parse("http://server:9000").expect("operation should succeed");
        let valid_endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 1,
            disk_idx: 2,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&valid_endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");
        let location = remote_disk.get_disk_location();
        assert!(location.valid());
        assert_eq!(location.pool_idx, Some(0));
        assert_eq!(location.set_idx, Some(1));
        assert_eq!(location.disk_idx, Some(2));

        // Test invalid location (negative indices)
        let invalid_endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: -1,
            set_idx: -1,
            disk_idx: -1,
        };

        let remote_disk_invalid = RemoteDisk::new(&invalid_endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");
        let invalid_location = remote_disk_invalid.get_disk_location();
        assert!(!invalid_location.valid());
        assert_eq!(invalid_location.pool_idx, None);
        assert_eq!(invalid_location.set_idx, None);
        assert_eq!(invalid_location.disk_idx, None);
    }

    #[tokio::test]
    async fn test_remote_disk_close() {
        let url = url::Url::parse("http://server:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");

        // Test close operation (should succeed)
        let result = remote_disk.close().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_with_timeout_marks_remote_disk_faulty() {
        let url = url::Url::parse("http://remote-timeout:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let err = remote_disk
            .execute_with_timeout(
                || async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<(), Error>(())
                },
                Duration::from_millis(10),
            )
            .await
            .expect_err("timeout should fail");

        assert!(err.to_string().contains("timeout"));
        assert!(remote_disk.is_online().await, "first timeout should keep the remote disk online");
        assert_eq!(
            remote_disk.runtime_state(),
            RuntimeDriveHealthState::Suspect,
            "first timeout should move the remote disk into suspect state"
        );
    }

    #[tokio::test]
    async fn test_execute_with_timeout_can_ignore_remote_timeout_failure() {
        let url = url::Url::parse("http://remote-timeout-ignored:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let err = remote_disk
            .execute_with_timeout_for_op_and_health_action(
                "walk_dir",
                || async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<(), Error>(())
                },
                Duration::from_millis(10),
                FailureHealthAction::IgnoreFailure,
            )
            .await
            .expect_err("timeout should fail");

        assert!(err.to_string().contains("timeout"));
        assert!(remote_disk.is_online().await, "ignored timeout should not mark remote disk faulty");
    }

    #[tokio::test]
    async fn test_execute_with_timeout_zero_duration_waits_for_operation() {
        let url = url::Url::parse("http://remote-no-timeout:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        remote_disk
            .execute_with_timeout(
                || async {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok::<(), Error>(())
                },
                Duration::ZERO,
            )
            .await
            .expect("zero duration should disable the operation timeout");

        assert!(
            remote_disk.is_online().await,
            "successful no-timeout operation should keep remote disk online"
        );
    }

    #[tokio::test]
    async fn test_execute_with_timeout_evicts_cached_connection() {
        let addr = "http://127.0.0.1:59991".to_string();
        let url = url::Url::parse(&format!("{addr}/data")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let channel = TonicEndpoint::from_shared(addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(addr.clone(), channel).await;
        assert!(runtime_sources::test_node_channel_is_cached(&addr).await);

        let _ = remote_disk
            .execute_with_timeout(
                || async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<(), Error>(())
                },
                Duration::from_millis(10),
            )
            .await
            .expect_err("timeout should fail");

        assert!(
            !runtime_sources::test_node_channel_is_cached(&addr).await,
            "timeout should evict cached connection"
        );
    }

    #[tokio::test]
    async fn test_execute_with_timeout_marks_faulty_on_timeout_like_error() {
        let addr = "http://127.0.0.1:59992".to_string();
        let url = url::Url::parse(&format!("{addr}/data")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let channel = TonicEndpoint::from_shared(addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(addr.clone(), channel).await;

        let err = remote_disk
            .execute_with_timeout(
                || async { Err::<(), Error>(DiskError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "stall timeout"))) },
                Duration::from_secs(1),
            )
            .await
            .expect_err("timeout-like operation error should fail");

        assert_eq!(
            match &err {
                DiskError::Io(io_err) => io_err.kind(),
                other => panic!("expected io timeout error, got {other:?}"),
            },
            std::io::ErrorKind::TimedOut
        );
        assert!(
            remote_disk.is_online().await,
            "first timeout-like error should keep the remote disk online"
        );
        assert_eq!(
            remote_disk.runtime_state(),
            RuntimeDriveHealthState::Suspect,
            "first timeout-like error should move the remote disk into suspect state"
        );
        assert!(
            !runtime_sources::test_node_channel_is_cached(&addr).await,
            "timeout-like errors should evict cached connection"
        );
    }

    #[tokio::test]
    async fn test_execute_with_timeout_marks_faulty_on_network_like_error() {
        let addr = "http://127.0.0.1:59993".to_string();
        let url = url::Url::parse(&format!("{addr}/data")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let channel = TonicEndpoint::from_shared(addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(addr.clone(), channel).await;

        let err = remote_disk
            .execute_with_timeout(
                || async {
                    Err::<(), Error>(DiskError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "connection refused",
                    )))
                },
                Duration::from_secs(1),
            )
            .await
            .expect_err("network-like operation error should fail");

        assert_eq!(
            match &err {
                DiskError::Io(io_err) => io_err.kind(),
                other => panic!("expected io network error, got {other:?}"),
            },
            std::io::ErrorKind::ConnectionRefused
        );
        assert!(
            remote_disk.is_online().await,
            "first network-like error should keep the remote disk online"
        );
        assert_eq!(
            remote_disk.runtime_state(),
            RuntimeDriveHealthState::Suspect,
            "first network-like error should move the remote disk into suspect state"
        );
        assert!(
            !runtime_sources::test_node_channel_is_cached(&addr).await,
            "network-like errors should evict cached connection"
        );
    }

    #[tokio::test]
    async fn test_execute_with_timeout_can_ignore_network_like_error() {
        let addr = "http://127.0.0.1:59995".to_string();
        let url = url::Url::parse(&format!("{addr}/data")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let channel = TonicEndpoint::from_shared(addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(addr.clone(), channel).await;

        let err = remote_disk
            .execute_with_timeout_for_op_and_health_action(
                "walk_dir",
                || async { Err::<(), Error>(DiskError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "stall timeout"))) },
                Duration::from_secs(1),
                FailureHealthAction::IgnoreFailure,
            )
            .await
            .expect_err("timeout-like operation error should fail");

        assert_eq!(
            match &err {
                DiskError::Io(io_err) => io_err.kind(),
                other => panic!("expected io timeout error, got {other:?}"),
            },
            std::io::ErrorKind::TimedOut
        );
        assert!(
            remote_disk.is_online().await,
            "ignored network-like error should not mark remote disk faulty"
        );
        assert!(
            runtime_sources::test_node_channel_is_cached(&addr).await,
            "ignored network-like error should not evict cached connection"
        );
    }

    #[tokio::test]
    async fn test_execute_with_timeout_keeps_remote_disk_online_for_business_error() {
        let addr = "http://127.0.0.1:59994".to_string();
        let url = url::Url::parse(&format!("{addr}/data")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let channel = TonicEndpoint::from_shared(addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(addr.clone(), channel).await;

        let err = remote_disk
            .execute_with_timeout(|| async { Err::<(), Error>(DiskError::FileNotFound) }, Duration::from_secs(1))
            .await
            .expect_err("business error should still fail the operation");

        assert_eq!(err, DiskError::FileNotFound);
        assert!(remote_disk.is_online().await, "business errors should not mark remote disk faulty");
        assert!(
            runtime_sources::test_node_channel_is_cached(&addr).await,
            "business errors should not evict cached connection"
        );
    }

    #[test]
    fn test_remote_disk_sync_properties() {
        let url = url::Url::parse("https://secure-remote:9000/data").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 1,
            set_idx: 2,
            disk_idx: 3,
        };

        // Test endpoint method - we can't test this without creating RemoteDisk instance
        // but we can test that the endpoint contains expected values
        assert_eq!(endpoint.url, url);
        assert!(!endpoint.is_local);
        assert_eq!(endpoint.pool_idx, 1);
        assert_eq!(endpoint.set_idx, 2);
        assert_eq!(endpoint.disk_idx, 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remote_disk_recovery_probe_logs_keep_request_id_span_context() {
        let logs = CapturedLogs::default();
        let subscriber = Registry::default().with(
            tracing_subscriber::fmt::layer()
                .with_writer(logs.clone())
                .with_ansi(false)
                .without_time()
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(true),
        );
        let _guard = tracing::subscriber::set_default(subscriber);

        let endpoint = Endpoint {
            url: url::Url::parse("http://127.0.0.1:59996/data").expect("endpoint URL should parse"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: true,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("remote disk should construct");

        let span = tracing::info_span!("request-span", request_id = "req-remote-disk");
        let _entered = span.enter();
        let done = remote_disk.spawn_recovery_monitor_log_probe_for_test();
        done.await
            .expect("remote disk recovery monitor probe should signal completion");

        let log = logs
            .lines()
            .into_iter()
            .find(|value| value.get("message").and_then(Value::as_str) == Some("remote disk recovery monitor log probe"))
            .expect("expected remote disk recovery monitor probe log");

        assert_eq!(log["span"]["name"], Value::String("recovery-monitor".to_string()));
        assert_eq!(log["span"]["kind"], Value::String("remote_disk".to_string()));
        let spans = log["spans"].as_array().expect("spans should be present");
        assert!(spans.iter().any(|span| {
            span.get("name").and_then(Value::as_str) == Some("request-span")
                && span.get("request_id").and_then(Value::as_str) == Some("req-remote-disk")
        }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remote_disk_network_error_starts_recovery_monitor_with_request_context() {
        let logs = CapturedLogs::default();
        let subscriber = Registry::default().with(
            tracing_subscriber::fmt::layer()
                .with_writer(logs.clone())
                .with_ansi(false)
                .without_time()
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(true),
        );
        let _guard = tracing::subscriber::set_default(subscriber);

        let addr = "http://127.0.0.1:59997".to_string();
        let endpoint = Endpoint {
            url: url::Url::parse(&format!("{addr}/data")).expect("endpoint URL should parse"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: true,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("remote disk should construct");

        let span = tracing::info_span!("request-span", request_id = "req-remote-disk-e2e");
        let _entered = span.enter();

        let err = remote_disk
            .execute_with_timeout(
                || async {
                    Err::<(), Error>(DiskError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "connection refused",
                    )))
                },
                Duration::from_secs(1),
            )
            .await
            .expect_err("network-like operation error should fail");
        assert_eq!(
            match &err {
                DiskError::Io(io_err) => io_err.kind(),
                other => panic!("expected io network error, got {other:?}"),
            },
            std::io::ErrorKind::ConnectionRefused
        );

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        remote_disk.cancel_token.cancel();
        tokio::task::yield_now().await;

        let lines = logs.lines();
        let marked_suspect = lines
            .iter()
            .find(|value| value.get("state").and_then(Value::as_str) == Some("marked_suspect"))
            .expect("expected marked_suspect log");
        assert!(
            marked_suspect["spans"]
                .as_array()
                .expect("spans should be present")
                .iter()
                .any(|span| {
                    span.get("name").and_then(Value::as_str) == Some("request-span")
                        && span.get("request_id").and_then(Value::as_str) == Some("req-remote-disk-e2e")
                })
        );

        let recovery_started = lines
            .iter()
            .find(|value| value.get("state").and_then(Value::as_str) == Some("recovery_monitor_started"))
            .expect("expected recovery_monitor_started log");
        assert_eq!(recovery_started["span"]["name"], Value::String("recovery-monitor".to_string()));
        assert_eq!(recovery_started["span"]["kind"], Value::String("remote_disk".to_string()));
        assert!(
            recovery_started["spans"]
                .as_array()
                .expect("spans should be present")
                .iter()
                .any(|span| {
                    span.get("name").and_then(Value::as_str) == Some("request-span")
                        && span.get("request_id").and_then(Value::as_str) == Some("req-remote-disk-e2e")
                })
        );
    }
}
